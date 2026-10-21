import asyncio
import io
import json
import ssl
from pathlib import Path
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, MagicMock, patch
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlencode

import botfather_api as api
import check_names as app


SESSION_URL = api.ORIGIN + api.API_PATH + "?hash=test_session_123"


class SocksTransportTests(unittest.TestCase):
    def test_explicit_proxy_routes_https_despite_environment_and_keeps_credentials_separate(self):
        settings = app.parse_socks5_proxy("socks5://user:password@mihomo:7890")
        with patch.dict(app.os.environ, {"HTTPS_PROXY": "http://wrong:8080", "NO_PROXY": "*"}), \
             patch.object(api, "Proxy") as proxy_class, \
             patch.object(api, "SocksHTTPSConnection") as connection_class:
            client = api.BotFatherAPI({"api_url": SESSION_URL, "cookie": "session=test"},
                                      timeout=7, retry_delay=300, proxy=settings)
            connection = connection_class.return_value
            response = connection.getresponse.return_value
            response.status, response.code, response.reason = 200, 200, "OK"
            response.__enter__.return_value = response
            response.read.return_value = b'{"ok":true}'
            self.assertEqual(client.check_username("examplebot")[0], "available")
            proxy_class.assert_called_once_with(proxy_type=api.ProxyType.SOCKS5, host="mihomo", port=7890,
                                               username="user", password="password", rdns=True)
            self.assertEqual(connection_class.call_args.args, ("webappinternal.telegram.org",))
            self.assertEqual(connection_class.call_args.kwargs["timeout"], 7)
            self.assertIs(connection_class.call_args.kwargs["proxy"], proxy_class.return_value)
            headers = connection.request.call_args.args[3]
            self.assertEqual(headers["Cookie"], "session=test")
            self.assertNotIn("Proxy-Authorization", headers)

    def test_socks_socket_is_wrapped_with_verified_tls_and_closed_on_tls_failure(self):
        proxy = MagicMock()
        connection = api.SocksHTTPSConnection("webappinternal.telegram.org", proxy=proxy, timeout=7)
        self.assertTrue(connection._context.check_hostname)
        self.assertEqual(connection._context.verify_mode, ssl.CERT_REQUIRED)
        with patch.object(connection._context, "wrap_socket", side_effect=ssl.SSLError("test")) as wrap:
            with self.assertRaises(ssl.SSLError):
                connection.connect()
        proxy.connect.assert_called_once_with(dest_host="webappinternal.telegram.org", dest_port=443, timeout=7)
        wrap.assert_called_once_with(proxy.connect.return_value, server_hostname="webappinternal.telegram.org")
        proxy.connect.return_value.close.assert_called_once()

    def test_proxy_failure_is_redacted_and_never_falls_back_to_direct_connection(self):
        settings = app.parse_socks5_proxy("socks5://mihomo:7890")
        with patch.object(api.Proxy, "connect", side_effect=api.ProxyError("secret password")) as connect, \
             patch("socket.create_connection") as direct:
            client = api.BotFatherAPI({"api_url": SESSION_URL}, timeout=1, retry_delay=300, proxy=settings)
            with self.assertRaises(api.BotFatherError) as error:
                client.check_username("examplebot")
            self.assertNotIn("secret", str(error.exception))
            self.assertFalse(error.exception.fatal)
            connect.assert_called_once()
            direct.assert_not_called()


class APIClientTests(unittest.TestCase):
    def setUp(self):
        self.client = api.BotFatherAPI({"api_url": SESSION_URL}, timeout=7, retry_delay=300)
        self.client.opener = MagicMock()

    def response(self, value):
        self.client.opener.open.return_value.__enter__.return_value.read.return_value = json.dumps(value).encode()

    def test_captured_success_request_and_suffix_rejection(self):
        self.response({"ok": True})
        self.assertEqual(self.client.check_username("nvidia_gay")[0], "available")
        request = self.client.opener.open.call_args.args[0]
        self.assertEqual(request.full_url, SESSION_URL)
        self.assertEqual(request.get_method(), "POST")
        self.assertEqual(parse_qs(request.data.decode()), {"method": ["checkBotUsername"], "username": ["nvidia_gay"]})
        self.assertEqual(self.client.opener.open.call_args.kwargs["timeout"], 7)
        self.response({"error": "Sorry, the username must end in &#39;bot&#39;."})
        self.assertEqual(self.client.check_username("nvidia")[0], "unsupported_suffix")

    def test_unknown_and_malformed_results_never_claim_availability(self):
        for value in ({"ok": "true"}, {"ok": 1}, {"ok": False}, {}, [],
                      {"ok": True, "error": "something failed"}, {"error": "server secret: " + SESSION_URL}):
            with self.subTest(value=value):
                self.response(value)
                status, detail = self.client.check_username("examplebot")
                self.assertNotEqual(status, "available")
                self.assertNotIn("test_session_123", detail)
        self.client.opener.open.return_value.__enter__.return_value.read.return_value = b"<html>expired session</html>"
        with self.assertRaises(api.BotFatherError) as error:
            self.client.check_username("examplebot")
        self.assertTrue(error.exception.fatal)

    def test_known_negative_results(self):
        for message, expected in (("Sorry, this username is already taken.", "occupied"),
                                  ("USERNAME_INVALID", "invalid"),
                                  ("USERNAME_PURCHASE_AVAILABLE", "purchase_required"),
                                  ("OWNER_USERNAMES_LIMIT_EXCEEDED", "account_limited")):
            self.response({"error": message})
            self.assertEqual(self.client.check_username("examplebot")[0], expected)

    def test_http_rate_limit_and_json_cooldown(self):
        for headers, expected in (({"Retry-After": "1200"}, 1200), ({}, 300)):
            self.client.opener.open.side_effect = HTTPError(SESSION_URL, 429, "Too many", headers, io.BytesIO())
            with self.assertRaises(api.BotFatherError) as error:
                self.client.check_username("examplebot")
            self.assertEqual(error.exception.retry_after, expected)
            self.assertNotIn("test_session_123", str(error.exception))
        self.client.opener.open.side_effect = None
        for value, expected in (({"error": "FLOOD_WAIT_900"}, 900),
                                ({"error": "Too many requests", "retry_after": 600}, 600),
                                ({"error": "Too many requests"}, 300)):
            self.response(value)
            with self.assertRaises(api.BotFatherError) as error:
                self.client.check_username("examplebot")
            self.assertEqual(error.exception.retry_after, expected)

    def test_auth_redirect_and_network_errors_do_not_leak_session(self):
        for status in (302, 401, 403, 500):
            self.client.opener.open.side_effect = HTTPError(SESSION_URL, status, "secret", {}, io.BytesIO())
            with self.assertRaises(api.BotFatherError) as error:
                self.client.check_username("examplebot")
            self.assertEqual(error.exception.fatal, status != 500)
            self.assertNotIn("test_session_123", str(error.exception))
        self.client.opener.open.side_effect = URLError(SESSION_URL)
        with self.assertRaises(api.BotFatherError) as error:
            self.client.check_username("examplebot")
        self.assertFalse(error.exception.fatal)
        self.assertNotIn("test_session_123", str(error.exception))
        self.assertIsNone(api.NoRedirect().redirect_request(None, None, 302, "", {}, "https://example.com"))

    def test_captured_http_400_expired_mini_app_page_is_an_auth_error(self):
        body = json.dumps({"v": 575, "t": "BotFather", "h": '<div class="tm-auth-expired">Session expired.</div>',
                           "j": "TWebApp.init('/botfather')", "s": {"authPage": True}}).encode()
        self.client.opener.open.side_effect = HTTPError(SESSION_URL, 400, "Bad Request", {}, io.BytesIO(body))
        with self.assertRaises(api.BotFatherError) as error:
            self.client.check_username("nvidia_gay")
        self.assertTrue(error.exception.fatal)
        self.assertIn("истекла", str(error.exception))
        self.assertNotIn("test_session_123", str(error.exception))

    def test_session_cannot_target_another_host_or_inject_headers(self):
        for url in (None, 123, "http://webappinternal.telegram.org/botfather/api?hash=x",
                    SESSION_URL.replace("webappinternal.telegram.org", "example.com"),
                    SESSION_URL.replace("/botfather/api", "/other"), SESSION_URL + "&other=1",
                    SESSION_URL + "#fragment", SESSION_URL.replace("https://", "https://user@")):
            with self.subTest(url=url), self.assertRaises(ValueError):
                api.validate_api_url(url)
        with self.assertRaises(ValueError):
            api.BotFatherAPI({"api_url": SESSION_URL, "cookie": "a=b\r\nInjected: x"}, timeout=1, retry_delay=300)


class SessionImportTests(unittest.TestCase):
    def test_only_session_is_imported_and_catalog_is_untouched(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            har = root / "capture.har"
            target = root / "private" / "botfather.json"
            har.write_text(json.dumps({"log": {"entries": [
                {"request": {"method": "POST", "url": SESSION_URL,
                             "postData": {"text": urlencode({"method": "auth", "_auth": "PRIVATE_INIT_DATA"})}}},
                {"request": {"method": "POST", "url": SESSION_URL,
                             "headers": [{"name": "Cookie", "value": "session=private_cookie"}],
                             "postData": {"text": "method=checkBotUsername&username=examplebot"}}}
            ]}}), encoding="utf-8")
            with patch.object(app, "scan", AsyncMock()) as scan:
                self.assertEqual(app.main(["--import-botfather-har", str(har), "--botfather-session", str(target),
                                           "--out", str(root / "output")]), 0)
                scan.assert_not_awaited()
            session = json.loads(target.read_text())
            self.assertEqual(session, {"api_url": SESSION_URL, "cookie": "session=private_cookie"})
            self.assertNotIn("PRIVATE_INIT_DATA", target.read_text())
            self.assertFalse((root / "output").exists())
            previous = target.read_bytes()
            har.write_text('{"log":{"entries":[]}}')
            self.assertEqual(app.main(["--import-botfather-har", str(har), "--botfather-session", str(target)]), 2)
            self.assertEqual(target.read_bytes(), previous)


class BackendTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.path = self.root / "state.sqlite3"
        self.state = app.State(self.path)
        self.args = SimpleNamespace(mode="bot", backend="botfather", delay=3, jitter=0,
                                    timeout=1, max_flood_wait=300, recheck=False,
                                    retry_additional=False, cache_hours=72, retry_delay=300,
                                    botfather_session=self.root / "botfather.json", out=self.root,
                                    proxy=None, watch=False)

    def tearDown(self):
        self.state.close()
        self.temp.cleanup()

    async def test_saved_native_cooldown_does_not_block_http_or_get_erased(self):
        self.state.block(1200)
        native_deadline = self.state.blocked_until()
        self.state.close()
        self.state = app.State(self.path)
        transport = SimpleNamespace(check_username=MagicMock(return_value=("available", "test")))
        checker = app.BotFatherChecker(transport, self.state, self.args)
        self.assertEqual(await checker.check("examplebot"), ("available", "test"))
        transport.check_username.assert_called_once_with("examplebot")
        self.assertEqual(self.state.blocked_until(), native_deadline)
        self.assertEqual(self.state.blocked_until("botfather"), 0)
        native = app.RequestPacer(self.state, SimpleNamespace(max_flood_wait=300))
        with self.assertRaises(app.StopScan):
            await native.wait_for_cooldown()
        self.assertIsNone(self.state.get("bot", "examplebot"))

    async def test_http_rate_limit_is_persisted_and_no_name_result_is_written(self):
        transport = SimpleNamespace(check_username=MagicMock(side_effect=api.BotFatherError("wait", retry_after=1200)))
        checker = app.BotFatherChecker(transport, self.state, self.args)
        with self.assertRaises(app.StopScan):
            await checker.check("examplebot")
        self.assertGreater(self.state.blocked_until("botfather"), time.time() + 1190)
        self.assertEqual(self.state.blocked_until(), 0)
        self.assertIsNone(self.state.get("bot", "examplebot"))
        self.state.close()
        self.state = app.State(self.path)
        transport.check_username.reset_mock()
        checker = app.BotFatherChecker(transport, self.state, self.args)
        with self.assertRaises(app.StopScan):
            await checker.check("examplebot")
        transport.check_username.assert_not_called()
        native = app.RequestPacer(self.state, SimpleNamespace(max_flood_wait=300))
        await native.wait_for_cooldown()

    async def test_http_backend_exports_without_native_telegram_credentials(self):
        self.args.proxy = app.parse_socks5_proxy("socks5://mihomo:7890")
        self.args.botfather_session.write_text(json.dumps({"api_url": SESSION_URL}))
        candidates, _ = app.make_candidates([app.Company("Nvidia")], ["gay"], True)
        client = api.BotFatherAPI.from_file(self.args.botfather_session, timeout=1, retry_delay=300)
        client.opener = MagicMock()
        client.opener.open.return_value.__enter__.return_value.read.return_value = b'{"ok":true}'
        with patch.dict(app.os.environ, {"TG_API_ID": "", "TG_API_HASH": ""}), \
             patch("telethon.TelegramClient") as native, \
             patch.object(api.BotFatherAPI, "from_file", return_value=client) as load, \
             patch.object(app.asyncio, "sleep", AsyncMock()) as sleep:
            self.assertEqual(await app.scan_once(self.args, candidates, self.state), 0)
        native.assert_not_called()
        self.assertEqual(load.call_args.kwargs["proxy"], self.args.proxy)
        self.assertTrue(any(c.args[0] > 2.5 for c in sleep.await_args_list))
        self.assertEqual(client.opener.open.call_count, 2)
        self.assertEqual(self.state.get("bot", "nvidia_gay")[0], "available")
        app.export_results(self.root, candidates, self.state, "bot", 72)
        self.assertIn("@nvidia_gay", (self.root / "available.txt").read_text())

    async def test_short_http_cooldown_is_waited_before_retrying_same_name(self):
        transport = SimpleNamespace(check_username=MagicMock(side_effect=[
            api.BotFatherError("wait", retry_after=1), ("occupied", "test")]))
        checker = app.BotFatherChecker(transport, self.state, self.args)
        pauses = []
        async def pause(seconds):
            pauses.append(seconds)
            with self.state.db:
                self.state.db.execute("DELETE FROM metadata WHERE key='blocked_until:botfather'")
        with patch.object(app.asyncio, "sleep", side_effect=pause):
            self.assertEqual(await checker.check("examplebot"), ("occupied", "test"))
        self.assertEqual([c.args for c in transport.check_username.call_args_list], [("examplebot",), ("examplebot",)])
        self.assertTrue(any(seconds >= 1 for seconds in pauses))

    async def test_expired_auth_stops_instead_of_replacing_cached_result(self):
        self.state.put("bot", "examplebot", "occupied")
        transport = SimpleNamespace(check_username=MagicMock(side_effect=api.BotFatherError("expired", fatal=True)))
        checker = app.BotFatherChecker(transport, self.state, self.args)
        with self.assertRaises(app.StopScan):
            await checker.check("examplebot")
        self.assertEqual(self.state.get("bot", "examplebot")[0], "occupied")


if __name__ == "__main__":
    unittest.main()

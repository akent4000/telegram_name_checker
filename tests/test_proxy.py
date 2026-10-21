import asyncio
from collections import defaultdict
import io
import logging
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from telethon.network.connection import ConnectionTcpFull

import check_names as app


class ProxyConfigTests(unittest.TestCase):
    def test_optional_proxy_auth_ipv6_and_encoded_credentials(self):
        self.assertIsNone(app.parse_socks5_proxy(None))
        self.assertIsNone(app.parse_socks5_proxy(""))
        proxy = app.parse_socks5_proxy("socks5://proxy.example.com:1080")
        self.assertEqual(proxy, {"proxy_type": "socks5", "addr": "proxy.example.com",
                                 "port": 1080, "rdns": True, "username": None, "password": None})
        proxy = app.parse_socks5_proxy("socks5h://u%40ser:p%3Aa%23%25%24@[::1]:65535/")
        self.assertEqual((proxy["addr"], proxy["port"], proxy["username"], proxy["password"]),
                         ("::1", 65535, "u@ser", "p:a#%$"))

    def test_invalid_urls_fail_without_echoing_credentials(self):
        for url in ["http://host:1080", "host:1080", "socks5://:1080",
                    "socks5://host", "socks5://host:0", "socks5://host:65536",
                    "socks5://host:abc", "socks5://[::1:1080", "socks5://host:1080/path",
                    "socks5://host:1080?", "socks5://host:1080#", "socks5://ho st:1080",
                    "socks5://host:10\n80", "socks5://secret@host:1080",
                    "socks5://:secret@host:1080", "socks5://user:@host:1080",
                    "socks5://user:secret%GG@host:1080", "socks5://user:secret%FF@host:1080",
                    "socks5://user:" + "x" * 256 + "@host:1080"]:
            with self.subTest(url=url), self.assertRaises(ValueError) as error:
                app.parse_socks5_proxy(url)
            self.assertNotIn("secret", str(error.exception))

    def test_environment_cli_override_and_explicit_disable(self):
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory)
            for extra, expected in [([], "env.example"),
                                    (["--proxy", "socks5://cli.example:1081"], "cli.example"),
                                    (["--proxy", ""], None)]:
                with self.subTest(extra=extra), \
                     patch.dict(app.os.environ, {"TG_PROXY": "socks5://env.example:1080"}), \
                     patch.object(app, "scan", AsyncMock(return_value=0)) as scan:
                    result = app.main(["--login-only", "--out", directory,
                                       "--session", str(out / "session"), *extra])
                    self.assertEqual(result, 0)
                    proxy = scan.await_args.args[0].proxy
                    self.assertEqual(proxy["addr"] if proxy else None, expected)

    def test_bad_environment_is_rejected_before_scan_without_password_in_stderr(self):
        with patch.dict(app.os.environ, {"TG_PROXY": "socks5://user:secret@host:bad"}), \
             patch.object(app, "scan", AsyncMock()) as scan, \
             patch("sys.stderr", new_callable=io.StringIO) as stderr, \
             self.assertRaises(SystemExit) as error:
            app.main(["--login-only"])
        self.assertEqual(error.exception.code, 2)
        self.assertNotIn("secret", stderr.getvalue())
        scan.assert_not_awaited()


class ProxyConnectionTests(unittest.IsolatedAsyncioTestCase):
    async def test_proxy_reaches_client_for_login_checks_and_worker(self):
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory)
            state = app.State(out / "state.sqlite3")
            items, _ = app.make_candidates([app.Company("Apple")], ["bot"], False)
            try:
                for mode in ("login", "once", "watch", "direct"):
                    args = SimpleNamespace(mode="bot", recheck=True, retry_additional=False,
                                           cache_hours=72, session=out / "session", out=out,
                                           login_only=mode == "login", watch=mode == "watch",
                                           non_interactive=mode == "watch",
                                           proxy=None if mode == "direct" else app.parse_socks5_proxy(
                                               "socks5://user:password@proxy.example.com:1080"))
                    client = AsyncMock()
                    client.get_me.return_value = SimpleNamespace(bot=False, premium=True)
                    with self.subTest(mode=mode), \
                         patch.dict(app.os.environ, {"TG_API_ID": "123", "TG_API_HASH": "0" * 32}), \
                         patch("telethon.TelegramClient", return_value=client) as constructor, \
                         patch.object(app, "run_checks", AsyncMock(return_value=0)), \
                         patch.object(app, "run_watch", AsyncMock()):
                        self.assertEqual(await app.scan_once(args, items, state), 0)
                        self.assertEqual(constructor.call_args.kwargs["proxy"], args.proxy)
                        client.disconnect.assert_awaited_once()
            finally:
                state.close()

    async def test_real_telethon_socks5_handshake_and_tunnel(self):
        # A local SOCKS server verifies auth, remote DNS and actual data routing.
        for authenticated in (False, True):
            with self.subTest(authenticated=authenticated):
                received = asyncio.get_running_loop().create_future()

                async def handle(reader, writer):
                    try:
                        version, count = await reader.readexactly(2)
                        methods = await reader.readexactly(count)
                        method = 2 if authenticated else 0
                        self.assertEqual(version, 5)
                        self.assertIn(method, methods)
                        writer.write(bytes([5, method]))
                        await writer.drain()
                        if authenticated:
                            version, size = await reader.readexactly(2)
                            self.assertEqual(version, 1)
                            self.assertEqual(await reader.readexactly(size), b"user")
                            size = (await reader.readexactly(1))[0]
                            self.assertEqual(await reader.readexactly(size), b"p@ss")
                            writer.write(b"\x01\x00")
                            await writer.drain()
                        self.assertEqual(await reader.readexactly(4), b"\x05\x01\x00\x03")
                        size = (await reader.readexactly(1))[0]
                        self.assertEqual(await reader.readexactly(size), b"telegram.invalid")
                        self.assertEqual(await reader.readexactly(2), b"\x01\xbb")
                        writer.write(b"\x05\x00\x00\x01\x00\x00\x00\x00\x00\x00")
                        await writer.drain()
                        self.assertEqual(await reader.readexactly(4), b"ping")
                        writer.write(b"pong")
                        await writer.drain()
                        received.set_result(True)
                    except Exception as error:
                        received.set_exception(error)
                    finally:
                        writer.close()
                        await writer.wait_closed()

                server = await asyncio.start_server(handle, "127.0.0.1", 0)
                sock = None
                try:
                    port = server.sockets[0].getsockname()[1]
                    auth = "user:p%40ss@" if authenticated else ""
                    proxy = app.parse_socks5_proxy(f"socks5://{auth}127.0.0.1:{port}")
                    connection = ConnectionTcpFull("telegram.invalid", 443, 2,
                                                   loggers=defaultdict(lambda: logging.getLogger(__name__)),
                                                   proxy=proxy)
                    sock = await connection._proxy_connect(timeout=3)
                    loop = asyncio.get_running_loop()
                    await loop.sock_sendall(sock, b"ping")
                    self.assertEqual(await asyncio.wait_for(loop.sock_recv(sock, 4), 3), b"pong")
                    self.assertTrue(await asyncio.wait_for(received, 3))
                finally:
                    if sock:
                        sock.close()
                    server.close()
                    await server.wait_closed()


if __name__ == "__main__":
    unittest.main()

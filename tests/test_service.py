import asyncio
import csv
import io
import json
from pathlib import Path
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch
from urllib.error import HTTPError
from urllib.request import urlopen
from http.server import ThreadingHTTPServer

import check_names as checker
import web_server as web


class WatchTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.out = Path(self.temp.name)
        self.state = checker.State(self.out / "state.sqlite3")
        self.items, _ = checker.make_candidates([checker.Company("Apple"), checker.Company("Google"), checker.Company("Yandex")], ["bot"], False)
        self.args = SimpleNamespace(mode="bot", recheck_hours=0, out=self.out, cache_hours=72)

    def tearDown(self):
        self.state.close()
        self.temp.cleanup()

    async def run_n(self, n):
        calls = []
        async def check(name):
            if len(calls) == n:
                raise asyncio.CancelledError
            calls.append(name)
            return "occupied", "test"
        with self.assertRaises(asyncio.CancelledError):
            await checker.run_watch(SimpleNamespace(check=check), self.items, self.state, self.args)
        return calls

    async def test_unchecked_then_oldest_then_next_cycle(self):
        now = time.time()
        self.state.put("bot", "applebot", "available")
        self.state.put("bot", "googlebot", "occupied")
        with self.state.db:
            self.state.db.execute("UPDATE checks SET checked=? WHERE username='applebot'", (now-100,))
            self.state.db.execute("UPDATE checks SET checked=? WHERE username='googlebot'", (now-200,))
        self.assertEqual(await self.run_n(5), ["yandexbot", "googlebot", "applebot", "yandexbot", "googlebot"])

    async def test_restart_uses_persisted_order_and_updates_availability(self):
        self.assertEqual(await self.run_n(1), ["applebot"])
        self.state.close()
        self.state = checker.State(self.out / "state.sqlite3")
        self.assertEqual(await self.run_n(3), ["googlebot", "yandexbot", "applebot"])
        self.assertEqual(self.state.get("bot", "applebot")[0], "occupied")

    async def test_minimum_age_wait_and_flood_keep_same_name(self):
        self.items = self.items[:1]
        self.state.put("bot", "applebot", "available")
        self.args.recheck_hours = 1
        calls = AsyncMock(side_effect=[checker.StopScan("flood"), asyncio.CancelledError])
        self.state.block(1200)
        pauses = []
        async def pause(state, seconds, status, **fields):
            pauses.append((seconds, status))
            if status == "waiting":
                self.args.recheck_hours = 0
        with patch.object(checker, "worker_pause", side_effect=pause), self.assertRaises(asyncio.CancelledError):
            await checker.run_watch(SimpleNamespace(check=calls), self.items, self.state, self.args)
        self.assertEqual([call.args[0] for call in calls.await_args_list], ["applebot", "applebot"])
        self.assertEqual([status for _, status in pauses], ["waiting", "flood_wait"])
        self.assertGreater(pauses[0][0], 3500)
        self.assertEqual(self.state.get("bot", "applebot")[0], "available")

    async def test_noninteractive_session_requires_login_without_prompt(self):
        args = SimpleNamespace(mode="bot", recheck=False, retry_additional=False, cache_hours=72,
                               session=self.out / "session", non_interactive=True)
        client = AsyncMock()
        client.is_user_authorized.return_value = False
        with patch.dict(checker.os.environ, {"TG_API_ID":"123", "TG_API_HASH":"0"*32}), \
             patch("telethon.TelegramClient", return_value=client), self.assertRaises(checker.StopScan):
            await checker.scan_once(args, self.items, self.state)
        client.start.assert_not_awaited()
        client.disconnect.assert_awaited_once()

    def test_exclusive_lock_and_release_after_failure(self):
        path = self.out / "scan.lock"
        with checker.process_lock(path):
            with self.assertRaises(checker.StopScan):
                with checker.process_lock(path):
                    self.fail("A second owner acquired the lock")
        with checker.process_lock(path):
            pass
        self.assertEqual(checker.session_lock_path(self.out / "checker"),
                         checker.session_lock_path(self.out / "checker.session"))

    async def test_worker_reconnects_after_error_and_marks_stopped_on_shutdown(self):
        self.args.watch = True
        self.args.session = self.out / "session"
        self.args.retry_delay = 300
        scanner = AsyncMock(side_effect=[checker.StopScan("network"), asyncio.CancelledError])
        pause = AsyncMock()
        with patch.object(checker, "scan_once", scanner), patch.object(checker, "worker_pause", pause), \
             self.assertRaises(asyncio.CancelledError):
            await checker.scan(self.args, self.items, self.state)
        self.assertEqual(scanner.await_count, 2)
        self.assertEqual(pause.await_args.args[1:3], (300, "error"))
        status = json.loads(self.state.db.execute("SELECT value FROM metadata WHERE key='worker'").fetchone()[0])
        self.assertEqual(status["status"], "stopped")


class CommandTests(unittest.TestCase):
    def test_login_does_not_replace_catalog_or_exports(self):
        with tempfile.TemporaryDirectory() as name:
            out = Path(name)
            candidates = out / "candidates.csv"
            candidates.write_text("untouched", encoding="utf-8")
            with patch.object(checker, "scan", AsyncMock(return_value=0)) as scan:
                self.assertEqual(checker.main(["--login-only", "--out", str(out), "--session", str(out / "session")]), 0)
                self.assertEqual(scan.await_args.args[1], [])
            self.assertEqual(candidates.read_text(encoding="utf-8"), "untouched")
            self.assertFalse((out / "available.txt").exists())

    def test_second_process_cannot_overwrite_candidate_files(self):
        with tempfile.TemporaryDirectory() as name:
            out = Path(name)
            (out / "candidates.csv").write_text("untouched", encoding="utf-8")
            with checker.process_lock(out / ".scan.lock"):
                self.assertEqual(checker.main(["--dry-run", "--company", "Apple", "--out", str(out)]), 2)
            self.assertEqual((out / "candidates.csv").read_text(encoding="utf-8"), "untouched")


class DashboardTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.out = Path(self.temp.name)
        self.state = checker.State(self.out / "state.sqlite3")
        self.items, _ = checker.make_candidates([checker.Company("Apple"), checker.Company("Google"), checker.Company("Яндекс", ("yandex",))], ["bot", "ai"], True)
        self.state.sync_candidates(self.items)
        for username, status in [("applebot","available"), ("apple_bot","available"),
                                 ("googlebot","occupied"), ("google_bot","available"),
                                 ("appleai","account_limited")]:
            self.state.put("bot", username, status)
        self.state.put("namespace", "googleai", "namespace_available")
        with self.state.db:
            self.state.db.execute("UPDATE checks SET checked=? WHERE username='google_bot'", (time.time()-73*3600,))
        self.dashboard = web.Dashboard(self.out / "state.sqlite3")

    def tearDown(self):
        self.state.close()
        self.temp.cleanup()

    def test_only_fresh_bot_availability_and_live_updates(self):
        data = self.dashboard.query({})
        self.assertEqual({r["username"] for r in data["items"]}, {"applebot", "apple_bot"})
        self.assertEqual(data["stats"]["available"], 2)
        self.assertEqual(data["stats"]["stale"], 1)
        self.assertNotIn("namespace_available", data["stats"])
        self.state.put("bot", "applebot", "occupied")
        self.assertEqual([r["username"] for r in self.dashboard.query({})["items"]], ["apple_bot"])

    def test_filters_literal_underscore_unicode_paging_and_validation(self):
        self.assertEqual([r["username"] for r in self.dashboard.query({"q":"@apple_"})["items"]], ["apple_bot"])
        self.assertEqual(self.dashboard.query({"status":"all", "q":"ЯНДЕКС"})["total"], 8)
        self.assertEqual(self.dashboard.query({"status":"pending", "suffix":"ai"})["total"], 7)
        result = self.dashboard.query({"status":"all", "per_page":"3", "page":"2", "sort":"username"})
        names = sorted(c.username for c in self.items)
        self.assertEqual([r["username"] for r in result["items"]], names[3:6])
        for params in [{"sort":"username;DROP TABLE checks"}, {"status":"namespace_available"}, {"page":"0"}, {"per_page":"101"}, {"page":"999999999999999999999"}]:
            with self.assertRaises(ValueError):
                self.dashboard.query(params)

    def test_queue_worker_offline_and_mode_isolation(self):
        self.state.worker_status("running", username="applebot", mode="bot")
        worker = self.dashboard.query({})["worker"]
        worker["updated"] = time.time()-200
        with self.state.db:
            self.state.db.execute("UPDATE metadata SET value=? WHERE key='worker'", (json.dumps(worker),))
        data = self.dashboard.query({})
        self.assertEqual(data["worker"]["status"], "offline")
        self.assertEqual(data["queue"][0]["username"], "apple_ai")
        self.assertEqual(self.dashboard.query({"status":"pending", "q":"googleai"})["total"], 1)

    def test_initialize_imports_existing_csv_without_overwriting_worker_scope(self):
        with tempfile.TemporaryDirectory() as name:
            out = Path(name)
            checker.write_csv(out / "candidates.csv", ["username", "suffix", "base", "company", "source"], [
                {"username":"examplebot", "suffix":"bot", "base":"example", "company":"Example", "source":"custom"}])
            state = checker.State(out / "state.sqlite3")
            state.put("bot", "examplebot", "available")
            state.close()
            web.initialize(out)
            self.assertEqual(web.Dashboard(out / "state.sqlite3").query({})["total"], 1)
        web.initialize(self.out)
        self.assertEqual(self.dashboard.query({"status":"all"})["total"], len(self.items))

    def test_http_routes_export_pagination_and_private_file_protection(self):
        server = ThreadingHTTPServer(("127.0.0.1", 0), web.Handler)
        server.dashboard = self.dashboard
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        base = f"http://127.0.0.1:{server.server_port}"
        try:
            with urlopen(base + "/") as response:
                self.assertIn("Хорошее имя", response.read().decode())
                self.assertIn("default-src 'self'", response.headers["Content-Security-Policy"])
            for path in ["/styles.css", "/app.js", "/healthz", "/api/names"]:
                with urlopen(base + path) as response:
                    self.assertEqual(response.status, 200)
            with urlopen(base + "/api/export?format=txt&per_page=1") as response:
                self.assertEqual(set(response.read().decode().splitlines()), {"@applebot", "@apple_bot"})
            with urlopen(base + "/api/export?format=csv") as response:
                rows = list(csv.DictReader(io.StringIO(response.read().decode("utf-8-sig"))))
                self.assertEqual(len(rows), 2)
                self.assertIn("attachment", response.headers["Content-Disposition"])
            for path, status in [("/private/checker.session",404), ("/../check_names.py",404), ("/api/names?sort=bad",400)]:
                with self.assertRaises(HTTPError) as error:
                    urlopen(base + path)
                self.assertEqual(error.exception.code, status)
                error.exception.close()
        finally:
            server.shutdown()
            server.server_close()
            thread.join()


if __name__ == "__main__":
    unittest.main()

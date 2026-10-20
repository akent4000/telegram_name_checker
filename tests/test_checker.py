import asyncio
import csv
import io
from pathlib import Path
import struct
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

import check_names as app
from telethon import errors
from telethon.extensions import BinaryReader


class CandidateTests(unittest.TestCase):
    def test_names_aliases_and_collision_preserve_both_companies(self):
        companies = [app.Company("Alphabet Inc. (Class A)", ("Google",), "one"),
                     app.Company("Alphabet Inc. (Class C)", (), "two"),
                     app.Company("Яндекс", ("yandex",), "three")]
        items, skipped = app.make_candidates(companies, ["ai", "game"], False)
        by_name = {c.username: c for c in items}
        self.assertEqual(len(items), 8)  # alphabet, google, yandeks, yandex
        self.assertEqual(len(by_name["alphabetai"].companies), 2)
        self.assertEqual(by_name["alphabetai"].sources, {"one", "two"})
        self.assertIn("yandexai", by_name)
        self.assertFalse(skipped)

    def test_length_digit_start_and_underscore(self):
        items, skipped = app.make_candidates([app.Company("A"), app.Company("3M"), app.Company("x" * 31)], ["ai"], True)
        self.assertFalse(items)
        self.assertEqual(len(skipped), 6)
        items, _ = app.make_candidates([app.Company("Apple")], ["ai"], True)
        self.assertEqual([c.username for c in items], ["appleai", "apple_ai"])

    def test_csv_and_txt(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "companies.csv"
            path.write_text('company,aliases\nAlphabet,google;alphabet\nЯндекс,yandex\n', encoding="utf-8")
            loaded = app.load_companies(path)
            self.assertEqual(loaded[0].aliases, ("google", "alphabet"))
            path = Path(d) / "companies.txt"
            path.write_text("# comment\nApple\n\nMicrosoft\n", encoding="utf-8")
            self.assertEqual([c.name for c in app.load_companies(path)], ["Apple", "Microsoft"])

    def test_new_request_wire_format_and_bool_response(self):
        request = app.request_factory("bot")("appleai")
        self.assertEqual(bytes(request), struct.pack("<II", app.NEW_CHECK_ID, 1) + b"\x07appleai")
        with BinaryReader(struct.pack("<I", 0x997275B5)) as reader:
            self.assertIs(request.read_result(reader), True)
        from telethon.tl.functions.bots import CheckUsernameRequest
        for username in ("applebot", "apple_bot"):
            standard = app.request_factory("bot")(username)
            self.assertIsInstance(standard, CheckUsernameRequest)
            self.assertEqual(standard.username, username)
            self.assertEqual(bytes(standard), bytes(CheckUsernameRequest(username=username)))


class StateTests(unittest.TestCase):
    def test_resume_ttl_modes_and_interrupted_export(self):
        with tempfile.TemporaryDirectory() as d:
            out = Path(d)
            state = app.State(out / "state.sqlite3")
            candidates, _ = app.make_candidates([app.Company("Apple"), app.Company("Google")], ["ai"], False)
            state.put("bot", "appleai", "available")
            state.put("namespace", "googleai", "namespace_available")
            self.assertTrue(app.fresh(state.get("bot", "appleai"), 24))
            self.assertIsNone(state.get("bot", "googleai"))
            counts = app.export_results(out, candidates, state, "bot", 24)
            self.assertEqual(counts, {"available": 1, "pending": 1})
            self.assertEqual((out / "available.txt").read_text(), "@appleai\n")
            state.db.execute("UPDATE checks SET checked=? WHERE mode='bot'", (time.time() - 90000,))
            state.db.commit()
            app.export_results(out, candidates, state, "bot", 24)
            self.assertEqual((out / "available.txt").read_text(), "")
            with (out / "results.csv").open(encoding="utf-8-sig") as f:
                self.assertEqual(list(csv.DictReader(f))[0]["status"], "stale")
            state.block(100)
            deadline = state.blocked_until()
            state.close()
            state = app.State(out / "state.sqlite3")
            self.assertEqual(state.blocked_until(), deadline)
            self.assertFalse(app.fresh(state.get("bot", "appleai"), 24))
            state.close()

    def test_namespace_is_never_exported_as_free_bot_name(self):
        with tempfile.TemporaryDirectory() as d:
            out = Path(d)
            state = app.State(out / "state.sqlite3")
            candidates, _ = app.make_candidates([app.Company("Apple")], ["ai"], False)
            state.put("namespace", "appleai", "namespace_available")
            app.export_results(out, candidates, state, "namespace", 24)
            self.assertEqual((out / "available.txt").read_text(), "")
            with (out / "namespace_available.csv").open(encoding="utf-8-sig") as f:
                self.assertEqual(len(list(csv.DictReader(f))), 1)
            state.close()


class RpcTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.state = app.State(Path(self.temp.name) / "state.sqlite3")
        self.args = SimpleNamespace(delay=3, jitter=0, timeout=1, max_flood_wait=300, mode="bot")

    def tearDown(self):
        self.state.close()
        self.temp.cleanup()

    def checker(self, effects):
        client = AsyncMock(side_effect=effects)
        checker = app.Checker(client, lambda s: s, errors, self.state, self.args)
        return checker, client

    async def test_available_unavailable_and_purchase(self):
        for effect, expected in [(True, "available"), (False, "unavailable"),
                                 (errors.UsernamePurchaseAvailableError(None), "purchase_required"),
                                 (errors.UsernameOccupiedError(None), "occupied"),
                                 (errors.UsernameInvalidError(None), "invalid"),
                                 (errors.BadRequestError(None, "NEW_UNKNOWN_ERROR", code=400), "error")]:
            checker, _ = self.checker([effect])
            status, _ = await checker.check("appleai")
            self.assertEqual(status, expected)

    async def test_flood_is_persisted_and_long_wait_stops_before_next_request(self):
        checker, client = self.checker([errors.FloodWaitError(None, 1200), True])
        with self.assertRaises(app.StopScan):
            await checker.check("appleai")
        self.assertEqual(client.await_count, 1)
        self.assertGreater(self.state.blocked_until(), time.time() + 1190)
        checker2, client2 = self.checker([True])
        with self.assertRaises(app.StopScan):
            await checker2.check("googleai")
        client2.assert_not_awaited()

    async def test_short_flood_wait_then_retry(self):
        checker, client = self.checker([errors.FloodWaitError(None, 1), True])
        # Advance the saved cooldown when sleeping, avoiding wall-clock delays in tests.
        async def fake_sleep(delay):
            self.state.db.execute("DELETE FROM metadata")
            self.state.db.commit()
        with patch.object(app.asyncio, "sleep", side_effect=fake_sleep) as sleep:
            status, _ = await checker.check("appleai")
        self.assertEqual(status, "available")
        self.assertEqual(client.await_count, 2)
        self.assertTrue(any(c.args[0] >= 1 for c in sleep.call_args_list))

    async def test_api_rollout_and_premium_stop_without_false_positive(self):
        for error in [errors.BadRequestError(None, "INPUT_CONSTRUCTOR_INVALID", code=400),
                      errors.BadRequestError(None, "PREMIUM_ACCOUNT_REQUIRED", code=400)]:
            checker, client = self.checker([error])
            with self.assertRaises(app.StopScan):
                await checker.check("appleai")
            self.assertEqual(client.await_count, 1)
            self.assertIsNone(self.state.get("bot", "appleai"))

    async def test_interval_enforced_between_requests(self):
        checker, client = self.checker([True, False])
        with patch.object(app.asyncio, "sleep", new_callable=AsyncMock) as sleep:
            await checker.check("appleai")
            await checker.check("googleai")
        self.assertEqual(client.await_count, 2)
        self.assertTrue(any(c.args[0] > 2.5 for c in sleep.call_args_list))


class AccountLimitTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.out = Path(self.temp.name)
        self.path = self.out / "state.sqlite3"
        self.state = app.State(self.path)
        self.args = SimpleNamespace(mode="bot", out=self.out, cache_hours=72)
        self.candidates, _ = app.make_candidates([app.Company("Apple"), app.Company("Google")], ["ai", "bot"], True)
        self.state.db.executemany("INSERT INTO metadata VALUES (?,?)", [
            ("active_account", "123"),
            ("additional_limit:123", "OWNER_USERNAMES_LIMIT_EXCEEDED"),
        ])
        self.state.db.commit()

    def tearDown(self):
        self.state.close()
        self.temp.cleanup()

    async def test_quota_response_is_a_diagnostic_for_one_username(self):
        args = SimpleNamespace(delay=3, jitter=0, timeout=1, max_flood_wait=300, mode="bot")
        for username in ("appleai", "applebot"):
            client = AsyncMock(side_effect=[errors.BadRequestError(None, "OWNER_USERNAMES_LIMIT_EXCEEDED", code=400)])
            checker = app.Checker(client, lambda s: s, errors, self.state, args)
            self.assertEqual(await checker.check(username), ("account_limited", "OWNER_USERNAMES_LIMIT_EXCEEDED"))
            self.assertEqual(client.await_count, 1)

    async def test_one_rejection_continues_all_suffixes_and_is_cached_on_restart(self):
        self.state.put("bot", "appleai", "available")
        pending = [c for c in self.candidates if not app.fresh(self.state.get("bot", c.username), 72)]
        async def check(username):
            if username == "apple_ai":
                return "account_limited", "OWNER_USERNAMES_LIMIT_EXCEEDED"
            if username == "googleai":
                return "available", "BoolTrue"
            return "occupied", "USERNAME_OCCUPIED"
        checker = SimpleNamespace(check=AsyncMock(side_effect=check))
        self.assertEqual(await app.run_checks(checker, pending, self.state, self.args, self.candidates), 2)
        self.assertEqual([c.args[0] for c in checker.check.await_args_list], [c.username for c in pending])
        self.assertTrue(app.fresh(self.state.get("bot", "apple_ai"), 72))
        self.assertFalse(app.fresh(self.state.get("bot", "apple_ai"), 72, time.time() + 73 * 3600))
        self.assertEqual(self.state.get("bot", "googleai")[0], "available")
        unscanned, _ = app.make_candidates([app.Company("Microsoft")], ["ai"], False)
        app.export_results(self.out, self.candidates + unscanned, self.state, "bot", 72)
        self.assertEqual((self.out / "available.txt").read_text(), "@appleai\n@googleai\n")
        with (self.out / "results.csv").open(encoding="utf-8-sig") as f:
            rows = {r["username"]: r for r in csv.DictReader(f)}
        self.assertEqual(rows["apple_ai"]["status"], "account_limited")
        self.assertEqual(rows["apple_ai"]["detail"], "OWNER_USERNAMES_LIMIT_EXCEEDED")
        self.assertTrue(rows["apple_ai"]["checked_at_utc"])
        self.assertEqual(rows["microsoftai"]["status"], "pending")
        self.assertEqual(rows["microsoftai"]["checked_at_utc"], "")
        self.state.close()
        self.state = app.State(self.path)
        pending = [c for c in self.candidates if not app.fresh(self.state.get("bot", c.username), 72)]
        self.assertEqual(pending, [])
        checker.check.reset_mock()
        self.assertEqual(await app.run_checks(checker, pending, self.state, self.args, self.candidates), 0)
        checker.check.assert_not_awaited()

    async def test_consecutive_quota_rejections_do_not_skip_or_stop_other_names(self):
        checker = SimpleNamespace(check=AsyncMock(return_value=("account_limited", "OWNER_USERNAMES_LIMIT_EXCEEDED")))
        self.assertEqual(await app.run_checks(checker, self.candidates, self.state, self.args), 2)
        self.assertEqual(checker.check.await_count, len(self.candidates))
        self.assertEqual([c.args[0] for c in checker.check.await_args_list], [c.username for c in self.candidates])
        for item in self.candidates:
            row = self.state.get("bot", item.username)
            self.assertEqual(row[0], "account_limited")
            self.assertTrue(app.fresh(row, 72))
        app.export_results(self.out, self.candidates, self.state, "bot", 72)
        self.assertEqual((self.out / "available.txt").read_text(), "")

    async def test_legacy_group_flag_is_ignored_and_retry_flag_targets_only_rejected_names(self):
        self.state.put("bot", "appleai", "account_limited", "OWNER_USERNAMES_LIMIT_EXCEEDED")
        args = SimpleNamespace(mode="bot", out=self.out, cache_hours=72, recheck=False,
                               retry_additional=False, session=self.out / "session",
                               delay=3, jitter=0, timeout=1, max_flood_wait=300)
        client = AsyncMock(return_value=True)
        client.get_me.return_value = SimpleNamespace(id=123, bot=False, premium=True)
        with patch.dict(app.os.environ, {"TG_API_ID": "12345", "TG_API_HASH": "0" * 32}), \
             patch("telethon.TelegramClient", return_value=client), \
             patch.object(app.asyncio, "sleep", new_callable=AsyncMock):
            self.assertEqual(await app.scan(args, self.candidates, self.state), 0)
        self.assertEqual([c.args[0].username for c in client.await_args_list], [c.username for c in self.candidates if c.username != "appleai"])
        self.assertEqual(self.state.get("bot", "appleai")[0], "account_limited")
        args.retry_additional = True
        client.reset_mock()
        with patch.dict(app.os.environ, {"TG_API_ID": "12345", "TG_API_HASH": "0" * 32}), \
             patch("telethon.TelegramClient", return_value=client), \
             patch.object(app.asyncio, "sleep", new_callable=AsyncMock):
            self.assertEqual(await app.scan(args, self.candidates, self.state), 0)
        self.assertEqual([c.args[0].username for c in client.await_args_list], ["appleai"])
        self.assertEqual(self.state.get("bot", "appleai")[0], "available")

    async def test_namespace_does_not_use_legacy_group_flag_or_claim_bot_availability(self):
        self.args.mode = "namespace"
        checker = SimpleNamespace(check=AsyncMock(return_value=("namespace_available", "BoolTrue")))
        self.assertEqual(await app.run_checks(checker, self.candidates, self.state, self.args), 0)
        self.assertEqual(checker.check.await_count, len(self.candidates))
        app.export_results(self.out, self.candidates, self.state, "namespace", 72)
        self.assertEqual((self.out / "available.txt").read_text(), "")


if __name__ == "__main__":
    unittest.main()

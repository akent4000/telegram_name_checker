#!/usr/bin/env python3
"""Read-only Telegram bot username checker. Python 3.10+; see README.md."""
from __future__ import annotations

import argparse
import asyncio
import csv
import inspect
import io
import json
import math
import os
from pathlib import Path
import random
import re
import sqlite3
import struct
import sys
import time
import unicodedata
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from collections import deque

ROOT = Path(__file__).resolve().parent
SP500_URL = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv"
DEFAULT_SUFFIXES = ("agent", "ai", "app", "game", "gay", "bot")
NEW_CHECK_ID = 0xE3AC4C61
LEGAL = re.compile(
    r"(?:\s*[,.-]?\s+)(?:incorporated|corporation|limited|company|holdings|"
    r"inc|corp|ltd|plc|llc|ag|se|sa|nv|co)\.?$", re.I
)
CYRILLIC = dict(zip(
    "абвгдеёзийклмнопрстуфхцчыьэюя",
    ("a", "b", "v", "g", "d", "e", "yo", "z", "i", "y", "k", "l", "m", "n", "o", "p", "r", "s", "t", "u", "f", "kh", "ts", "ch", "y", "", "e", "yu", "ya"),
))
CYRILLIC.update({"ж": "zh", "ш": "sh", "щ": "shch", "ъ": "", "і": "i", "ї": "yi", "є": "ye", "ґ": "g"})


@dataclass(frozen=True)
class Company:
    name: str
    aliases: tuple[str, ...] = ()
    source: str = "user"


@dataclass
class Candidate:
    username: str
    suffix: str
    base: str
    companies: set[str]
    sources: set[str]


class StopScan(Exception):
    pass


def utc_stamp(epoch: float | None = None) -> str:
    return datetime.fromtimestamp(epoch if epoch is not None else time.time(), timezone.utc).isoformat(timespec="seconds")


def normalize(name: str) -> str:
    name = re.sub(r"\([^)]*\)", "", name).strip()
    while True:
        stripped = LEGAL.sub("", name).strip()
        if stripped == name:
            break
        name = stripped
    name = "".join(CYRILLIC.get(c, c) for c in name.lower())
    name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9_]", "", name).strip("_")


def load_companies(path: Path) -> list[Company]:
    text = path.read_text(encoding="utf-8-sig")
    if path.suffix.lower() != ".csv":
        return [Company(s.strip()) for s in text.splitlines() if s.strip() and not s.lstrip().startswith("#")]
    try:
        dialect = csv.Sniffer().sniff(text[:8192], delimiters=",;\t")
    except csv.Error:
        dialect = csv.excel
    reader = csv.DictReader(io.StringIO(text), dialect=dialect)
    headers = {s.strip().lower(): s for s in (reader.fieldnames or [])}
    key = next((headers[k] for k in ("company", "name", "security") if k in headers), None)
    if key is None:
        raise ValueError(f"{path}: нужен столбец company, name или Security")
    result = []
    for row in reader:
        name = (row.get(key) or "").strip()
        if name:
            aliases = tuple(a.strip() for a in (row.get(headers.get("aliases", "")) or "").split(";") if a.strip())
            source = (row.get(headers.get("source", "")) or path.stem).strip()
            result.append(Company(name, aliases, source))
    return result


def refresh_sp500() -> None:
    request = urllib.request.Request(SP500_URL, headers={"User-Agent": "TelegramNameChecker/1.0"})
    with urllib.request.urlopen(request, timeout=30) as response:
        raw = response.read(2_000_001)
    if len(raw) > 2_000_000:
        raise ValueError("Источник S&P 500 вернул слишком большой ответ")
    rows = list(csv.DictReader(io.StringIO(raw.decode("utf-8-sig"))))
    if not 450 <= len(rows) <= 600 or not all(r.get("Security") for r in rows):
        raise ValueError("Формат источника S&P 500 изменился; предыдущий файл сохранён")
    path = ROOT / "sources" / "sp500.csv"
    temp = path.with_suffix(".tmp")
    temp.write_bytes(raw)
    temp.replace(path)
    (ROOT / "sources" / "metadata.json").write_text(json.dumps({"sp500": {
        "url": SP500_URL, "retrieved_at": utc_stamp(), "rows": len(rows),
        "note": "Community-maintained dataset sourced from Wikipedia; not an official S&P feed."
    }}, ensure_ascii=False, indent=2), encoding="utf-8")


def make_candidates(companies: list[Company], suffixes: list[str], underscore: bool) -> tuple[list[Candidate], list[dict]]:
    found: dict[str, Candidate] = {}
    skipped = []
    for company in companies:
        bases = dict.fromkeys(normalize(n) for n in (company.name, *company.aliases))
        for base in bases:
            for suffix in suffixes:
                for separator in (("", "_") if underscore else ("",)):
                    username = f"{base}{separator}{suffix}"
                    if not base or not re.fullmatch(r"[a-z][a-z0-9_]{3,30}[a-z0-9]", username):
                        skipped.append({"company": company.name, "username": username, "reason": "local_format: 5–32 symbols, starts with a letter, ends with a letter/digit"})
                        continue
                    if username not in found:
                        found[username] = Candidate(username, suffix, base, set(), set())
                    found[username].companies.add(company.name)
                    found[username].sources.add(company.source)
    return list(found.values()), skipped


def write_csv(path: Path, fields: list[str], rows) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    temporary.replace(path)


class State:
    def __init__(self, path: Path):
        self.db = sqlite3.connect(path)
        self.db.execute("CREATE TABLE IF NOT EXISTS checks (mode TEXT, username TEXT, status TEXT, checked REAL, detail TEXT, PRIMARY KEY(mode,username))")
        self.db.execute("CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT)")
        self.db.commit()

    def get(self, mode: str, username: str):
        return self.db.execute("SELECT status, checked, detail FROM checks WHERE mode=? AND username=?", (mode, username)).fetchone()

    def put(self, mode: str, username: str, status: str, detail: str = ""):
        self.db.execute("INSERT OR REPLACE INTO checks VALUES (?,?,?,?,?)", (mode, username, status, time.time(), detail))
        self.db.commit()

    def blocked_until(self) -> float:
        row = self.db.execute("SELECT value FROM metadata WHERE key='blocked_until'").fetchone()
        return float(row[0]) if row else 0.0

    def block(self, seconds: int):
        deadline = max(self.blocked_until(), time.time() + seconds + 1)
        self.db.execute("INSERT OR REPLACE INTO metadata VALUES ('blocked_until',?)", (str(deadline),))
        self.db.commit()

    def close(self):
        self.db.close()


def fresh(row, cache_hours: float, now: float | None = None) -> bool:
    return bool(row and row[0] not in {"error", "unknown"} and (now if now is not None else time.time()) - row[1] < cache_hours * 3600)


def export_results(out: Path, candidates: list[Candidate], state: State, mode: str, cache_hours: float) -> dict[str, int]:
    rows, available, namespace = [], [], []
    counts: dict[str, int] = {}
    now = time.time()
    for item in candidates:
        stored = state.get(mode, item.username)
        status, checked, detail = stored if stored else ("pending", None, "")
        if stored and now - checked >= cache_hours * 3600:
            status, detail = "stale", f"previous_status={status}; {detail}"
        row = {"company": " | ".join(sorted(item.companies)), "username": item.username,
               "url": f"https://t.me/{item.username}", "suffix": item.suffix, "status": status,
               "checked_at_utc": utc_stamp(checked) if checked is not None else "",
               "mode": mode, "source": " | ".join(sorted(item.sources)), "detail": detail}
        rows.append(row)
        counts[status] = counts.get(status, 0) + 1
        if status == "available":
            available.append(row)
        if status == "namespace_available":
            namespace.append(row)
    fields = ["company", "username", "url", "suffix", "status", "checked_at_utc", "mode", "source", "detail"]
    write_csv(out / "results.csv", fields, rows)
    write_csv(out / "available.csv", fields, available)
    write_csv(out / "namespace_available.csv", fields, namespace)
    temporary = out / "available.txt.tmp"
    temporary.write_text("".join(f"@{row['username']}\n" for row in available), encoding="utf-8")
    temporary.replace(out / "available.txt")
    return counts


def request_factory(mode: str):
    from telethon.tl import functions
    from telethon.tl.tlobject import TLRequest
    if mode == "namespace":
        return functions.account.CheckUsernameRequest
    native = getattr(functions.bots, "CheckUsernameRequest", None)
    if native and "additional" in inspect.signature(native).parameters:
        return lambda username: native(username=username, additional=not username.lower().endswith("bot"))

    # New official TDLib schema; Telethon 1.45 only ships the older request.
    # bots.checkUsername#e3ac4c61 flags:# additional:flags.0?true username:string = Bool;
    class CheckAdditionalUsernameRequest(TLRequest):
        CONSTRUCTOR_ID = NEW_CHECK_ID
        SUBCLASS_OF_ID = 0xF5B399AC

        def __init__(self, username, additional=True):
            self.username = username
            self.additional = additional

        def _bytes(self):
            return struct.pack("<II", self.CONSTRUCTOR_ID, int(self.additional)) + self.serialize_bytes(self.username)

        def to_dict(self):
            return {"_": "bots.CheckAdditionalUsernameRequest", "username": self.username, "additional": self.additional}

    def make_request(username):
        if username.lower().endswith("bot") and native:
            return native(username=username)
        return CheckAdditionalUsernameRequest(username, additional=not username.lower().endswith("bot"))

    return make_request


def classify_rpc(message: str, code: int) -> tuple[str, bool]:
    """Return (status, stop_scan); never interpret an unknown error as availability."""
    message = message.upper()
    mapping = {"USERNAME_OCCUPIED": "occupied", "USERNAME_PURCHASE_AVAILABLE": "purchase_required",
               "USERNAME_INVALID": "invalid", "USERNAME_NOT_MODIFIED": "occupied",
               "USERNAME_RESERVED": "reserved", "USERNAME_SUFFIX_MISSING": "unsupported_suffix",
               "USERNAME_SUFFIX_INVALID": "unsupported_suffix",
               "OWNER_USERNAMES_LIMIT_EXCEEDED": "account_limited"}
    if message in mapping:
        return mapping[message], False
    if "PREMIUM" in message or message == "BOT_CREATE_LIMIT_EXCEEDED":
        return "account_requirement", True
    if code in {401, 403, 404, 406} or any(s in message for s in ("METHOD_INVALID", "CONSTRUCTOR_INVALID", "INPUT_LAYER_INVALID")):
        return "api_unavailable", True
    return "error", False


def rpc_name(error) -> str:
    # Known Telethon error classes often use English prose (or an empty string)
    # in .message, while unknown errors retain Telegram's raw RPC name.
    name = type(error).__name__.removesuffix("Error")
    message = getattr(error, "message", "") or ""
    if not type(error).__module__.endswith(".rpcerrorlist") and re.fullmatch(r"[A-Z][A-Z0-9_]*", message):
        return message
    return re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", name).upper()


class Checker:
    def __init__(self, client, factory, errors, state: State, args):
        self.client, self.factory, self.errors, self.state, self.args = client, factory, errors, state, args
        self.last_request = 0.0
        self.flood_errors = tuple(getattr(errors, name) for name in ("FloodWaitError", "FloodPremiumWaitError") if hasattr(errors, name))

    async def wait_for_cooldown(self):
        delay = max(0, self.state.blocked_until() - time.time())
        if delay > self.args.max_flood_wait:
            raise StopScan(f"Telegram требует паузу ещё {math.ceil(delay)} с. Повторите запуск позже: пауза сохранена.")
        if delay:
            print(f"Пауза Telegram: {math.ceil(delay)} с.", flush=True)
            await asyncio.sleep(delay)

    async def check(self, username: str) -> tuple[str, str]:
        floods = 0
        for attempt in range(4):
            await self.wait_for_cooldown()
            remaining = self.last_request + self.args.delay + random.uniform(0, self.args.jitter) - time.monotonic()
            if remaining > 0:
                await asyncio.sleep(remaining)
            self.last_request = time.monotonic()
            try:
                value = await asyncio.wait_for(self.client(self.factory(username)), timeout=self.args.timeout)
                if value is True:
                    kind = "standard bot username" if username.lower().endswith("bot") else "additional bot username"
                    return ("available" if self.args.mode == "bot" else "namespace_available"), f"Telegram returned BoolTrue ({kind if self.args.mode == 'bot' else 'account namespace'})"
                if value is False:
                    return "unavailable", "Telegram returned BoolFalse"
                return "unknown", f"Unexpected response type: {type(value).__name__}"
            except self.flood_errors as error:
                self.state.block(error.seconds)
                floods += 1
                print(f"FLOOD_WAIT: {error.seconds} с, @{username}", flush=True)
                if floods >= 3:
                    raise StopScan("Повторный FLOOD_WAIT; прогресс и обязательная пауза сохранены.")
                # Next iteration honours the full server cooldown, including after a restart.
            except self.errors.RPCError as error:
                name = rpc_name(error)
                code = getattr(error, "code", None) or 0
                status, fatal = classify_rpc(name, code)
                if fatal:
                    raise StopScan(f"Telegram: {name}. Проверка остановлена; имя не признано свободным.") from error
                if code >= 500 and attempt < 3:
                    await asyncio.sleep(2 ** (attempt + 1))
                    continue
                return status, name
            except (OSError, asyncio.TimeoutError) as error:
                if attempt == 3:
                    return "error", type(error).__name__
                await asyncio.sleep(2 ** (attempt + 1))
        return "error", "Retry budget exhausted"


async def run_checks(checker, pending: list[Candidate], state: State, args, candidates: list[Candidate] | None = None) -> int:
    queue = deque(pending)
    export_scope = candidates if candidates is not None else pending
    account_limited = False
    error_streak = 0
    unsupported_streak = 0
    checked = 0

    while queue:
        item = queue.popleft()
        status, detail = await checker.check(item.username)
        state.put(args.mode, item.username, status, detail)
        checked += 1
        if status == "account_limited":
            account_limited = True
            print(f"@{item.username}: {detail}. Отказ сохранён только для этого имени; продолжаю очередь.", flush=True)
        error_streak = error_streak + 1 if status in {"error", "unknown"} else 0
        unsupported_streak = unsupported_streak + 1 if status == "unsupported_suffix" else 0
        print(f"[новых: {checked}; в очереди: {len(queue)}] @{item.username}: {status}", flush=True)
        if checked % 25 == 0:
            export_results(args.out, export_scope, state, args.mode, args.cache_hours)
        if error_streak >= 5:
            raise StopScan("Пять ошибок подряд; прогресс сохранён. Проверьте подключение и совместимость API.")
        if unsupported_streak >= 5:
            raise StopScan("Telegram отклонил пять суффиксов подряд. Проверьте доступность новой функции для аккаунта.")
    return 2 if account_limited else 0


async def scan(args, candidates: list[Candidate], state: State) -> int:
    try:
        from telethon import TelegramClient, errors
    except ImportError as error:
        raise StopScan("Установите зависимость: python -m pip install -r requirements.txt") from error
    api_id = os.environ.get("TG_API_ID", "")
    api_hash = os.environ.get("TG_API_HASH", "")
    if not api_id.isdecimal() or int(api_id) <= 0 or not re.fullmatch(r"[0-9a-fA-F]{32}", api_hash):
        raise StopScan("Задайте TG_API_ID и TG_API_HASH приложения с https://my.telegram.org/apps (не токен бота).")
    pending = []
    for candidate in candidates:
        stored = state.get(args.mode, candidate.username)
        retry_limited = args.retry_additional and stored and stored[0] == "account_limited"
        if args.recheck or retry_limited or not fresh(stored, args.cache_hours):
            pending.append(candidate)
    print(f"Кандидатов: {len(candidates)}; из кэша: {len(candidates)-len(pending)}; к проверке: {len(pending)}.")
    if not pending:
        return 0
    factory = request_factory(args.mode)
    args.session.parent.mkdir(parents=True, exist_ok=True)
    client = TelegramClient(str(args.session), int(api_id), api_hash,
                            flood_sleep_threshold=0, request_retries=0, raise_last_call_error=True)
    try:
        await client.start()
        me = await client.get_me()
        if me.bot:
            raise StopScan("Нужна авторизация пользовательского аккаунта, а не бота.")
        if args.mode == "bot" and any(not c.username.lower().endswith("bot") for c in pending) and not getattr(me, "premium", False):
            print("У аккаунта нет Premium. Сервер может отклонить проверку дополнительных имён.", flush=True)
        checker = Checker(client, factory, errors, state, args)
        return await run_checks(checker, pending, state, args, candidates)
    except errors.RPCError as error:
        raise StopScan(f"Ошибка Telegram: {rpc_name(error)}. Прогресс сохранён.") from error
    finally:
        await client.disconnect()
    return 0


def positive_float(value):
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError("нужно положительное конечное число")
    return number


def nonnegative_float(value):
    number = float(value)
    if not math.isfinite(number) or number < 0:
        raise argparse.ArgumentTypeError("нужно неотрицательное конечное число")
    return number


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Проверка обычных и дополнительных юзернеймов ботов через Telegram MTProto.")
    parser.add_argument("--companies", type=Path, help="Свой CSV/TXT вместо встроенных списков")
    parser.add_argument("--company", action="append", default=[], help="Отдельная компания; флаг можно повторять")
    parser.add_argument("--suffixes", nargs="+", default=list(DEFAULT_SUFFIXES))
    parser.add_argument("--underscore", action=argparse.BooleanOptionalAction, default=True,
                        help="Также проверять company_ai помимо companyai (включено по умолчанию)")
    parser.add_argument("--refresh-sources", action="store_true", help="Обновить снимок S&P 500 перед запуском")
    parser.add_argument("--dry-run", action="store_true", help="Только сформировать кандидатов, без Telegram и авторизации")
    parser.add_argument("--export-only", action="store_true", help="Экспортировать сохранённые результаты без сети")
    parser.add_argument("--mode", choices=("bot", "namespace"), default="bot", help="namespace — только общая доступность, без подтверждения для бота")
    parser.add_argument("--out", type=Path, default=ROOT / "output")
    parser.add_argument("--session", type=Path, default=ROOT / "private" / "checker")
    parser.add_argument("--limit", type=int, help="Ограничить число кандидатов, например 100")
    parser.add_argument("--delay", type=positive_float, default=3.0, help="Минимальный интервал между запросами, с (не менее 3)")
    parser.add_argument("--jitter", type=nonnegative_float, default=0.5)
    parser.add_argument("--timeout", type=positive_float, default=30.0)
    parser.add_argument("--max-flood-wait", type=nonnegative_float, default=300.0, help="Более долгую паузу сохранить и выйти")
    parser.add_argument("--cache-hours", type=positive_float, default=72.0)
    parser.add_argument("--recheck", action="store_true", help="Повторно проверить даже свежие результаты")
    parser.add_argument("--retry-additional", action="store_true", help="Повторно проверить имена со статусом account_limited, даже если они есть в кэше")
    args = parser.parse_args(argv)
    if args.delay < 3:
        parser.error("--delay должен быть не менее 3 секунд")
    if args.limit is not None and args.limit <= 0:
        parser.error("--limit должен быть больше 0")
    args.suffixes = list(dict.fromkeys(s.lower() for s in args.suffixes))
    if not all(re.fullmatch(r"[a-z]+", s) for s in args.suffixes):
        parser.error("Суффиксы должны содержать только латинские буквы")
    if args.dry_run and args.export_only:
        parser.error("Выберите только один из --dry-run и --export-only")
    if args.export_only and args.refresh_sources:
        parser.error("--export-only не совместим с --refresh-sources")
    try:
        if args.refresh_sources:
            refresh_sp500()
        companies = load_companies(args.companies) if args.companies else []
        if not args.companies and not args.company:
            for filename in ("global_companies.csv", "sp500.csv", "russian_companies.csv", "moex_companies.csv"):
                companies.extend(load_companies(ROOT / "sources" / filename))
        companies += [Company(s) for s in args.company if s.strip()]
        candidates, skipped = make_candidates(companies, args.suffixes, args.underscore)
        if args.limit is not None:
            candidates = candidates[:args.limit]
        if not candidates:
            raise ValueError("Нет корректных кандидатов; проверьте входные названия и алиасы")
        args.out.mkdir(parents=True, exist_ok=True)
        write_csv(args.out / "candidates.csv", ["company", "username", "suffix", "base", "source"], (
            {"company": " | ".join(sorted(c.companies)), "username": c.username, "suffix": c.suffix,
             "base": c.base, "source": " | ".join(sorted(c.sources))} for c in candidates))
        write_csv(args.out / "skipped.csv", ["company", "username", "reason"], skipped)
        print(f"Входных строк компаний/брендов: {len(companies)}; уникальных кандидатов: {len(candidates)}.")
        if args.dry_run:
            hours = len(candidates) * (args.delay + args.jitter / 2) / 3600
            print(f"Без запросов к Telegram. Оценка полного прохода: от {hours:.1f} ч + задержки сети/FLOOD_WAIT.")
            print(f"Кандидаты: {args.out / 'candidates.csv'}")
            return 0
        state = State(args.out / "state.sqlite3")
        result = 0
        try:
            if not args.export_only:
                result = asyncio.run(scan(args, candidates, state))
        except StopScan as error:
            print(str(error), file=sys.stderr)
            result = 2
        except KeyboardInterrupt:
            print("Остановлено. Повторите команду, чтобы продолжить.", file=sys.stderr)
            result = 130
        finally:
            try:
                counts = export_results(args.out, candidates, state, args.mode, args.cache_hours)
                print("Результаты:", json.dumps(counts, ensure_ascii=False))
                print(f"Свободные имена ботов: {args.out / 'available.txt'}")
            finally:
                state.close()
        return result
    except (ValueError, OSError, csv.Error) as error:
        print(f"Ошибка: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    # Keep local session credentials private on POSIX systems.
    if os.name == "posix":
        os.umask(0o077)
    raise SystemExit(main())

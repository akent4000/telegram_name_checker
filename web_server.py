#!/usr/bin/env python3
"""Local dashboard and read-only API backed by the checker's SQLite database."""
from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import os
from pathlib import Path
import signal
import sqlite3
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

from check_names import ROOT, DEFAULT_SUFFIXES, Candidate, State, cooldown_key, load_companies, make_candidates, positive_float, utc_stamp

STATIC = ROOT / "web"
STATUSES = {"available", "occupied", "unavailable", "purchase_required", "reserved", "invalid",
            "unsupported_suffix", "account_limited", "error", "unknown", "pending", "stale"}
SORTS = {"newest": "checked DESC, username", "oldest": "COALESCE(checked, 0), position",
         "username": "username", "company": "company COLLATE NOCASE, username"}


def initialize(out: Path):
    """Import the existing candidate CSV once; subsequent worker runs own the scope."""
    out.mkdir(parents=True, exist_ok=True)
    state = State(out / "state.sqlite3")
    try:
        if state.db.execute("SELECT 1 FROM candidates LIMIT 1").fetchone():
            return
        candidates = []
        path = out / "candidates.csv"
        if path.exists():
            with path.open(encoding="utf-8-sig", newline="") as stream:
                for row in csv.DictReader(stream):
                    candidates.append(Candidate(row["username"], row["suffix"], row.get("base", ""),
                                                {row.get("company", "")}, {row.get("source", "")}))
        else:
            companies = []
            for filename in ("global_companies.csv", "sp500.csv", "russian_companies.csv", "moex_companies.csv"):
                companies.extend(load_companies(ROOT / "sources" / filename))
            candidates, _ = make_candidates(companies, list(DEFAULT_SUFFIXES), True)
        # A worker may have populated the table while the CSV was being read.
        with state.db:
            state.db.execute("BEGIN IMMEDIATE")
            if not state.db.execute("SELECT 1 FROM candidates LIMIT 1").fetchone():
                state.db.executemany("INSERT INTO candidates VALUES (?,?,?,?,?,?)", (
                    (c.username, c.suffix, c.base, " | ".join(sorted(c.companies)),
                     " | ".join(sorted(c.sources)), i) for i, c in enumerate(candidates)))
    finally:
        state.close()


class Dashboard:
    def __init__(self, path: Path, cache_hours: float = 72):
        self.path, self.cache_hours = path, cache_hours

    def connect(self):
        db = sqlite3.connect(self.path.resolve().as_uri() + "?mode=ro", uri=True, timeout=30)
        db.row_factory = sqlite3.Row
        db.create_function("casefold", 1, lambda s: (s or "").casefold(), deterministic=True)
        return db

    def query(self, params: dict, export: bool = False):
        status, suffix = params.get("status", "available"), params.get("suffix", "")
        sort, search = params.get("sort", "newest"), params.get("q", "").strip()
        if status != "all" and status not in STATUSES:
            raise ValueError("Неизвестный статус")
        if sort not in SORTS:
            raise ValueError("Неизвестная сортировка")
        if len(search) > 200 or len(suffix) > 32:
            raise ValueError("Слишком длинный фильтр")
        try:
            page, per_page = int(params.get("page", "1")), int(params.get("per_page", "50"))
        except ValueError:
            raise ValueError("Номер страницы и размер должны быть целыми числами") from None
        if not 1 <= page <= 1_000_000 or not 1 <= per_page <= 100:
            raise ValueError("Страница должна быть от 1 до 1000000, размер — от 1 до 100")
        conditions, values = [], []
        if status != "all":
            conditions.append("status=?")
            values.append(status)
        if suffix:
            conditions.append("suffix=?")
            values.append(suffix)
        if search:
            search = search.lstrip("@").casefold()
            pattern = "%" + search.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
            conditions.append("(casefold(username) LIKE ? ESCAPE '\\' OR casefold(company) LIKE ? ESCAPE '\\')")
            values.extend([pattern, pattern])
        where = " WHERE " + " AND ".join(conditions) if conditions else ""
        cutoff = time.time() - self.cache_hours * 3600
        cte = """WITH data AS (
            SELECT c.*, ch.checked, COALESCE(ch.detail,'') AS detail,
                   CASE WHEN ch.checked IS NULL THEN 'pending'
                        WHEN ch.checked<=? THEN 'stale' ELSE ch.status END AS status,
                   ch.status AS previous_status
            FROM candidates c LEFT JOIN checks ch ON ch.username=c.username AND ch.mode='bot'
        ) """
        db = self.connect()
        try:
            db.execute("BEGIN")  # Consistent counts, results and queue in a concurrent worker update.
            total = db.execute(cte + "SELECT COUNT(*) FROM data" + where, [cutoff, *values]).fetchone()[0]
            sql = cte + "SELECT * FROM data" + where + " ORDER BY " + SORTS[sort]
            args = [cutoff, *values]
            if not export:
                sql += " LIMIT ? OFFSET ?"
                args.extend([per_page, (page - 1) * per_page])
            rows = [self.serialize(row) for row in db.execute(sql, args)]
            if export:
                return rows
            stats = {row["status"]: row["n"] for row in db.execute(cte + "SELECT status, COUNT(*) n FROM data GROUP BY status", [cutoff])}
            queue = [self.serialize(row) for row in db.execute(cte + "SELECT * FROM data ORDER BY COALESCE(checked,0), position LIMIT 8", [cutoff])]
            suffixes = [row[0] for row in db.execute("SELECT DISTINCT suffix FROM candidates ORDER BY suffix")]
            metadata = dict(db.execute("SELECT key, value FROM metadata WHERE key IN ('worker', 'blocked_until', 'blocked_until:botfather')"))
            worker = json.loads(metadata.get("worker", "null"))
            backend = worker.get("backend", "mtproto") if worker else "mtproto"
            if worker and worker["status"] != "stopped" and time.time() - worker["updated"] > 180:
                worker["status"] = "offline"
            return {"items": rows, "total": total, "page": page, "per_page": per_page,
                    "stats": stats, "queue": queue, "suffixes": suffixes, "worker": worker,
                    "blocked_until": float(metadata.get(cooldown_key(backend), 0)),
                    "cache_hours": self.cache_hours, "updated_at": utc_stamp()}
        finally:
            db.close()

    @staticmethod
    def serialize(row):
        item = dict(row)
        item["checked_at_utc"] = utc_stamp(item["checked"]) if item["checked"] is not None else None
        item["url"] = "https://t.me/" + item["username"]
        return item


class Handler(BaseHTTPRequestHandler):
    server_version = "NameChecker"

    def send(self, content: bytes, kind: str, status: int = 200, filename: str | None = None):
        self.send_response(status)
        self.send_header("Content-Type", kind)
        self.send_header("Content-Length", str(len(content)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Content-Security-Policy", "default-src 'self'; style-src 'self'; script-src 'self'; connect-src 'self'; img-src 'self' data:; frame-ancestors 'none'; base-uri 'none'")
        if filename:
            self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.end_headers()
        self.wfile.write(content)

    def json(self, data, status=200):
        self.send(json.dumps(data, ensure_ascii=False).encode(), "application/json; charset=utf-8", status)

    def do_GET(self):
        path = urlsplit(self.path)
        params = {key: values[0] for key, values in parse_qs(path.query).items()}
        try:
            if path.path == "/healthz":
                db = self.server.dashboard.connect()
                try:
                    db.execute("SELECT 1 FROM candidates LIMIT 1").fetchone()
                finally:
                    db.close()
                self.json({"status": "ok"})
            elif path.path == "/api/names":
                self.json(self.server.dashboard.query(params))
            elif path.path == "/api/export":
                fmt = params.get("format", "csv")
                if fmt not in {"csv", "txt"}:
                    raise ValueError("Формат экспорта: csv или txt")
                rows = self.server.dashboard.query(params, export=True)
                if fmt == "txt":
                    self.send("".join("@" + r["username"] + "\n" for r in rows).encode(),
                              "text/plain; charset=utf-8", filename="names.txt")
                else:
                    stream = io.StringIO(newline="")
                    fields = ["username", "company", "suffix", "status", "checked_at_utc", "url", "source", "detail"]
                    writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
                    writer.writeheader()
                    for row in rows:
                        writer.writerow({k: "'" + v if isinstance(v, str) and v.startswith(("=", "+", "-", "@", "\t", "\r")) else v for k, v in row.items()})
                    self.send(stream.getvalue().encode("utf-8-sig"), "text/csv; charset=utf-8", filename="names.csv")
            elif path.path in {"/", "/app.js", "/styles.css"}:
                name, kind = {"/": ("index.html", "text/html"), "/app.js": ("app.js", "text/javascript"),
                              "/styles.css": ("styles.css", "text/css")}[path.path]
                self.send((STATIC / name).read_bytes(), kind + "; charset=utf-8")
            else:
                self.json({"error": "Страница не найдена"}, 404)
        except ValueError as error:
            self.json({"error": str(error)}, 400)
        except (OSError, sqlite3.Error):
            logging.exception("Dashboard request failed")
            self.json({"error": "База временно недоступна. Повторите запрос."}, 503)


def main():
    parser = argparse.ArgumentParser(description="Сайт для просмотра свободных Telegram-имён")
    parser.add_argument("--out", type=Path, default=Path(os.environ.get("CHECKER_OUT", ROOT / "output")))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--cache-hours", type=positive_float, default=72)
    args = parser.parse_args()
    initialize(args.out)
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    server.dashboard = Dashboard(args.out / "state.sqlite3", args.cache_hours)
    print(f"Сайт: http://{args.host}:{server.server_port}", flush=True)
    def stop_service(signum, frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, stop_service)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()

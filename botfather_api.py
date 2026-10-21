"""Read-only client for the BotFather Mini App API observed in an exported HAR.

No bot creation, username assignment, or messages are performed by this module.
The session URL is a credential; never include it in diagnostics.
"""
from __future__ import annotations

from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import html
from http.client import HTTPException, HTTPSConnection
import json
from pathlib import Path
import re
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlencode, urlsplit
from urllib.request import build_opener, HTTPRedirectHandler, HTTPSHandler, ProxyHandler, Request

from python_socks import ProxyError, ProxyType
from python_socks.sync import Proxy

ORIGIN = "https://webappinternal.telegram.org"
API_PATH = "/botfather/api"
MAX_RESPONSE = 65536


class BotFatherError(Exception):
    def __init__(self, message: str, *, fatal: bool = False, retry_after: float | None = None):
        super().__init__(message)
        self.fatal = fatal
        self.retry_after = retry_after


def validate_api_url(value: str) -> str:
    if not isinstance(value, str):
        raise ValueError("Неверный формат URL сессии BotFather.")
    try:
        url = urlsplit(value)
        query = parse_qs(url.query, strict_parsing=True)
        valid = (url.scheme == "https" and url.netloc == "webappinternal.telegram.org"
                 and url.path == API_PATH and not url.fragment
                 and set(query) == {"hash"} and len(query["hash"]) == 1
                 and re.fullmatch(r"[a-zA-Z0-9_-]{1,256}", query["hash"][0])
                 and not any(c.isspace() or ord(c) < 32 for c in value))
    except (TypeError, ValueError, KeyError):
        valid = False
    if not valid:
        raise ValueError("Неверный URL сессии BotFather: ожидается HTTPS /botfather/api с hash на официальном домене.")
    return value


def import_har(source: Path, destination: Path) -> None:
    """Copy only the latest observed username-check session, never the full HAR."""
    try:
        har = json.loads(source.read_text(encoding="utf-8-sig"))
        entries = har["log"]["entries"]
        if not isinstance(entries, list):
            raise ValueError
    except (OSError, ValueError, KeyError, TypeError):
        raise ValueError("Не удалось прочитать HAR-файл BotFather.") from None
    session = None
    for entry in reversed(entries):
        if not isinstance(entry, dict):
            continue
        request = entry.get("request", {})
        if not isinstance(request, dict):
            continue
        if request.get("method") != "POST":
            continue
        url = request.get("url", "")
        try:
            validate_api_url(url)
        except ValueError:
            continue
        post = request.get("postData", {})
        if not isinstance(post, dict):
            continue
        body = post.get("text", "")
        if not isinstance(body, str):
            continue
        fields = parse_qs(body)
        if not fields and post.get("params"):
            fields = {p["name"]: [p.get("value", "")] for p in post["params"]}
        if fields.get("method") != ["checkBotUsername"]:
            continue
        headers = {h["name"].lower(): h["value"] for h in request.get("headers", [])}
        cookie = headers.get("cookie", "")
        if not cookie:
            cookie = "; ".join(f"{c['name']}={c['value']}" for c in request.get("cookies", []))
        session = {"api_url": url, "cookie": cookie}
        break
    if session is None:
        raise ValueError("В HAR нет запроса checkBotUsername с hash. Откройте форму Mini App, проверьте имя и экспортируйте HAR заново.")
    # Validate before touching the previous credentials.
    BotFatherAPI(session, timeout=30, retry_delay=300)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(destination.name + ".tmp")
    try:
        with temporary.open("w", encoding="utf-8") as stream:
            temporary.chmod(0o600)
            json.dump(session, stream, indent=2)
            stream.write("\n")
        temporary.replace(destination)
    finally:
        temporary.unlink(missing_ok=True)


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class SocksHTTPSConnection(HTTPSConnection):
    def __init__(self, host, *, proxy, **kwargs):
        super().__init__(host, **kwargs)
        self.proxy = proxy

    def connect(self):
        sock = self.proxy.connect(dest_host=self.host, dest_port=self.port, timeout=self.timeout)
        try:
            self.sock = self._context.wrap_socket(sock, server_hostname=self.host)
        except BaseException:
            sock.close()
            raise


class SocksHTTPSHandler(HTTPSHandler):
    def __init__(self, settings):
        super().__init__()
        self.proxy = Proxy(proxy_type=ProxyType.SOCKS5, host=settings["addr"], port=settings["port"],
                           username=settings.get("username"), password=settings.get("password"), rdns=True)

    def https_open(self, request):
        def connection(host, **kwargs):
            return SocksHTTPSConnection(host, proxy=self.proxy, **kwargs)
        return self.do_open(connection, request, context=self._context)


def retry_seconds(value, fallback: float) -> float:
    try:
        seconds = float(value)
        if 0 <= seconds <= 31536000:
            return max(1, seconds)
    except (TypeError, ValueError, OverflowError):
        pass
    if isinstance(value, str):
        try:
            deadline = parsedate_to_datetime(value)
            if deadline.tzinfo is None:
                deadline = deadline.replace(tzinfo=timezone.utc)
            return max(1, min(31536000, (deadline - datetime.now(timezone.utc)).total_seconds()))
        except (TypeError, ValueError, OverflowError):
            pass
    return fallback


class BotFatherAPI:
    def __init__(self, session: dict, *, timeout: float, retry_delay: float, proxy: dict | None = None):
        self.api_url = validate_api_url(session.get("api_url", ""))
        self.cookie = session.get("cookie", "")
        if not isinstance(self.cookie, str) or len(self.cookie) > 16384 or any(ord(c) < 32 or ord(c) >= 127 for c in self.cookie):
            raise ValueError("Некорректная cookie сессии BotFather.")
        self.timeout = timeout
        self.retry_delay = retry_delay
        # Explicit SOCKS settings take precedence over environment proxies and NO_PROXY.
        self.opener = (build_opener(ProxyHandler({}), SocksHTTPSHandler(proxy), NoRedirect())
                       if proxy else build_opener(NoRedirect()))

    @classmethod
    def from_file(cls, path: Path, *, timeout: float, retry_delay: float, proxy: dict | None = None):
        try:
            session = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(session, dict):
                raise ValueError
            return cls(session, timeout=timeout, retry_delay=retry_delay, proxy=proxy)
        except (OSError, ValueError, TypeError):
            raise BotFatherError("Нет корректной HTTP-сессии BotFather. Выполните --import-botfather-har с новым HAR.", fatal=True) from None

    def check_username(self, username: str) -> tuple[str, str]:
        headers = {"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                   "Accept": "application/json", "Origin": ORIGIN,
                   "Referer": ORIGIN + "/botfather/create", "X-Requested-With": "XMLHttpRequest",
                   "User-Agent": "TelegramNameChecker/1.0"}
        if self.cookie:
            headers["Cookie"] = self.cookie
        request = Request(self.api_url, data=urlencode({"username": username, "method": "checkBotUsername"}).encode(),
                          headers=headers, method="POST")
        try:
            with self.opener.open(request, timeout=self.timeout) as response:
                raw = response.read(MAX_RESPONSE + 1)
        except HTTPError as error:
            try:
                if error.code == 429:
                    raise BotFatherError("BotFather: HTTP 429; обязательная пауза сохранена.",
                                         retry_after=retry_seconds(error.headers.get("Retry-After"), self.retry_delay)) from None
                if error.code in {301, 302, 303, 307, 308, 401, 403}:
                    raise BotFatherError("HTTP-сессия BotFather истекла или отклонена. Импортируйте новый HAR.", fatal=True) from None
                if error.code == 400:
                    # Expired Mini App sessions return the auth-page JSON with HTTP 400.
                    raw = error.read(MAX_RESPONSE + 1)
                else:
                    raise BotFatherError(f"BotFather: HTTP {error.code}.", fatal=error.code < 500) from None
            finally:
                error.close()
        except (URLError, OSError, HTTPException, ProxyError):
            raise BotFatherError("Не удалось подключиться к HTTP API BotFather.") from None
        if len(raw) > MAX_RESPONSE:
            raise BotFatherError("BotFather вернул слишком большой ответ.")
        try:
            result = json.loads(raw)
        except (ValueError, UnicodeError):
            raise BotFatherError("BotFather вернул не JSON. Проверьте срок действия HTTP-сессии.", fatal=True) from None
        if not isinstance(result, dict):
            return "unknown", "BotFather: unexpected response format"
        page_state = result.get("s")
        if isinstance(page_state, dict) and page_state.get("authPage") is True:
            raise BotFatherError("HTTP-сессия BotFather истекла. Откройте Mini App заново и импортируйте новый HAR.", fatal=True)
        # Positive results must match the boolean observed in the Mini App HAR.
        if result.get("ok") is True and not result.get("error"):
            return "available", "BotFather Mini App: checkBotUsername returned ok=true"
        message = result.get("error")
        if not isinstance(message, str):
            return "unknown", "BotFather: no explicit availability result"
        normalized = html.unescape(re.sub(r"<[^>]*>", "", message)).lower().strip()
        if ("flood" in normalized or "too many" in normalized
                or "try again in" in normalized or "retry_after" in result):
            match = re.search(r"flood[_ ]wait[_ :]*(\d+)|(?:wait|try again in)\s+(\d+)\s*(?:s\b|sec|second)", normalized)
            seconds = result.get("retry_after")
            if seconds is None and match:
                seconds = next(g for g in match.groups() if g is not None)
            raise BotFatherError("BotFather потребовал паузу; она сохранена.",
                                 retry_after=retry_seconds(seconds, self.retry_delay))
        if any(part in normalized for part in ("session expired", "unauthorized", "authentication required", "auth expired")):
            raise BotFatherError("HTTP-сессия BotFather истекла. Импортируйте новый HAR.", fatal=True)
        mapping = (
            (("username must end in 'bot'", "username must end with 'bot'"), "unsupported_suffix"),
            (("username is already taken", "username is already occupied", "username_occupied"), "occupied"),
            (("username is invalid", "username_invalid", "invalid username"), "invalid"),
            (("username_purchase_available",), "purchase_required"),
            (("username_reserved",), "reserved"),
            (("owner_usernames_limit_exceeded",), "account_limited"),
        )
        for phrases, status in mapping:
            if any(phrase in normalized for phrase in phrases):
                return status, f"BotFather Mini App: {status}"
        # Do not persist arbitrary server text: it can contain session credentials.
        return "error", "BotFather: unrecognized error response"

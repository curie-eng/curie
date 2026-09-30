"""Turn unacknowledged Slack Email alerts into source-thread SRE turns.

@spec SRE-EMAIL-1
@spec SRE-EMAIL-2
@spec SRE-EMAIL-3
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import math
import os
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from html.parser import HTMLParser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

LOG = logging.getLogger("sre-slack-email-intake")
PLACEHOLDER = "Investigating this alert..."
MAX_FILE_BYTES = 1_048_576
MAX_EMAIL_TEXT = 100_000
SLACK_API = "https://slack.com/api"


class RateLimited(RuntimeError):
    """An interrupted scan must wait without claiming a health success."""

    def __init__(self, retry_after: float) -> None:
        super().__init__("Slack rate limited the scan")
        self.retry_after = retry_after


def _retry_after(headers: Any, fallback: float) -> float:
    try:
        value = float(headers.get("Retry-After", ""))
    except (TypeError, ValueError):
        return fallback
    return value if math.isfinite(value) and value > 0 else fallback


def _is_slack_url(url: str) -> bool:
    parsed = urllib.parse.urlsplit(url)
    hostname = (parsed.hostname or "").lower()
    return parsed.scheme == "https" and (hostname == "slack.com" or hostname.endswith(".slack.com"))


class SlackRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Keep a Slack bearer token inside Slack's HTTPS origin family."""

    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> urllib.request.Request | None:
        if not _is_slack_url(newurl):
            raise urllib.error.HTTPError(
                newurl,
                code,
                "Slack redirect is outside slack.com",
                headers,
                fp,
            )
        return super().redirect_request(req, fp, code, msg, headers, newurl)


class Config:
    """Validated runtime configuration for one Slack Email source."""

    def __init__(
        self,
        *,
        slack_token: str,
        channel_id: str,
        source_user_id: str,
        source_bot_id: str | None,
        subject_prefixes: tuple[str, ...],
        scan_not_before: str,
        canary_thread_ts: str,
        hook_url: str,
        hook_secret: str,
        slack_adapter: str | None,
        poll_seconds: float,
        placeholder_stale_seconds: float,
        http_timeout_seconds: float,
    ) -> None:
        self.slack_token = slack_token
        self.channel_id = channel_id
        self.source_user_id = source_user_id
        self.source_bot_id = source_bot_id
        self.subject_prefixes = subject_prefixes
        self.scan_not_before = scan_not_before
        self.canary_thread_ts = canary_thread_ts
        self.hook_url = hook_url
        self.hook_secret = hook_secret
        self.slack_adapter = slack_adapter
        self.poll_seconds = poll_seconds
        self.placeholder_stale_seconds = placeholder_stale_seconds
        self.http_timeout_seconds = http_timeout_seconds

    @classmethod
    def from_env(cls) -> Config:
        """Read required values explicitly; no tenant defaults are valid."""

        def required(name: str) -> str:
            value = os.environ.get(name, "").strip()
            if not value:
                raise ValueError(f"{name} is required")
            return value

        prefixes = tuple(
            value.strip()
            for value in required("ALERT_SUBJECT_PREFIXES").split(",")
            if value.strip()
        )
        if not prefixes:
            raise ValueError("ALERT_SUBJECT_PREFIXES must contain a prefix")
        scan_not_before = required("SLACK_SCAN_NOT_BEFORE")
        canary_thread_ts = required("SLACK_CANARY_THREAD_TS")
        try:
            floor = float(scan_not_before)
            canary = float(canary_thread_ts)
        except ValueError as exc:
            raise ValueError(
                "SLACK_SCAN_NOT_BEFORE and SLACK_CANARY_THREAD_TS must be Slack timestamps"
            ) from exc
        for name, value in (("SLACK_SCAN_NOT_BEFORE", floor), ("SLACK_CANARY_THREAD_TS", canary)):
            if not math.isfinite(value) or value < 0:
                raise ValueError(f"{name} must be a finite nonnegative timestamp")
        if canary < floor:
            raise ValueError("SLACK_CANARY_THREAD_TS must be at or after SLACK_SCAN_NOT_BEFORE")
        poll = _positive_float("POLL_SECONDS", "60")
        stale = _positive_float("PLACEHOLDER_STALE_SECONDS", "900")
        if stale <= 2 * poll:
            raise ValueError("PLACEHOLDER_STALE_SECONDS must be greater than two poll intervals")
        return cls(
            slack_token=required("SLACK_BOT_TOKEN"),
            channel_id=required("SLACK_CHANNEL_ID"),
            source_user_id=required("SLACK_EMAIL_SOURCE_USER_ID"),
            source_bot_id=os.environ.get("SLACK_EMAIL_SOURCE_BOT_ID", "").strip() or None,
            subject_prefixes=prefixes,
            scan_not_before=scan_not_before,
            canary_thread_ts=canary_thread_ts,
            hook_url=required("CURIE_HOOK_URL"),
            hook_secret=required("CURIE_HOOK_SECRET"),
            slack_adapter=os.environ.get("CURIE_SLACK_ADAPTER", "").strip() or None,
            poll_seconds=poll,
            placeholder_stale_seconds=stale,
            http_timeout_seconds=_positive_float("HTTP_TIMEOUT_SECONDS", "30"),
        )


def _positive_float(name: str, default: str) -> float:
    try:
        value = float(os.environ.get(name, default))
    except ValueError as exc:
        raise ValueError(f"{name} must be a number") from exc
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be finite and greater than zero")
    return value


def _subject(message: dict[str, Any]) -> str:
    files = message.get("files") or []
    if len(files) != 1 or not isinstance(files[0], dict):
        return ""
    file = files[0]
    return str(file.get("title") or file.get("name") or "")


def is_candidate(message: dict[str, Any], config: Config) -> bool:
    """Return whether one history item is a configured Slack Email root."""

    ts = str(message.get("ts") or "")
    thread_ts = str(message.get("thread_ts") or "")
    if not ts or (thread_ts and thread_ts != ts):
        return False
    if message.get("user") != config.source_user_id:
        return False
    if config.source_bot_id and message.get("bot_id") != config.source_bot_id:
        return False
    files = message.get("files") or []
    if len(files) != 1 or files[0].get("mimetype") != "text/html":
        return False
    return _subject(message).startswith(config.subject_prefixes)


class _TextExtractor(HTMLParser):
    _BLOCKS = {
        "address",
        "article",
        "br",
        "div",
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
        "li",
        "p",
        "section",
        "table",
        "td",
        "th",
        "tr",
    }

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self.hidden = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        del attrs
        if tag in {"script", "style"}:
            self.hidden += 1
        elif tag in self._BLOCKS:
            self.parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if tag in {"script", "style"} and self.hidden:
            self.hidden -= 1
        elif tag in self._BLOCKS:
            self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        if not self.hidden:
            self.parts.append(data)


def html_to_text(raw: bytes, *, limit: int = MAX_EMAIL_TEXT) -> str:
    """Extract bounded human-readable evidence from an untrusted HTML file."""

    parser = _TextExtractor()
    parser.feed(raw.decode("utf-8", errors="replace"))
    parser.close()
    lines = [re.sub(r"\s+", " ", line).strip() for line in "".join(parser.parts).splitlines()]
    text = "\n\n".join(line for line in lines if line)
    return text[:limit]


class SlackClient:
    """Small bounded Slack Web API client for the four required operations."""

    def __init__(self, config: Config) -> None:
        self.config = config
        self.opener = urllib.request.build_opener(SlackRedirectHandler())

    def _api(self, method: str, fields: dict[str, str]) -> dict[str, Any]:
        body = urllib.parse.urlencode(fields).encode()
        request = urllib.request.Request(
            f"{SLACK_API}/{method}",
            data=body,
            headers={
                "Authorization": f"Bearer {self.config.slack_token}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            method="POST",
        )
        try:
            with self.opener.open(request, timeout=self.config.http_timeout_seconds) as response:
                payload = json.loads(response.read())
                retry_after = _retry_after(response.headers, self.config.poll_seconds)
        except urllib.error.HTTPError as exc:
            if exc.code == 429:
                raise RateLimited(_retry_after(exc.headers, self.config.poll_seconds)) from exc
            raise
        if not isinstance(payload, dict) or payload.get("ok") is not True:
            error = (
                payload.get("error", "invalid_response")
                if isinstance(payload, dict)
                else "invalid_response"
            )
            if error == "ratelimited":
                raise RateLimited(retry_after)
            raise RuntimeError(f"Slack {method} failed: {error}")
        return payload

    def bot_user_id(self) -> str:
        value = str(self._api("auth.test", {}).get("user_id") or "")
        if not value:
            raise RuntimeError("Slack auth.test returned no user_id")
        return value

    def history(self, channel: str, oldest: str) -> list[dict[str, Any]]:
        messages: list[dict[str, Any]] = []
        cursor = ""
        while True:
            fields = {"channel": channel, "oldest": oldest, "limit": "200", "inclusive": "true"}
            if cursor:
                fields["cursor"] = cursor
            payload = self._api("conversations.history", fields)
            page = payload.get("messages") or []
            if not isinstance(page, list):
                raise RuntimeError("Slack conversations.history returned invalid messages")
            messages.extend(item for item in page if isinstance(item, dict))
            cursor = str((payload.get("response_metadata") or {}).get("next_cursor") or "")
            if not cursor:
                return messages

    def root(self, channel: str, thread_ts: str) -> dict[str, Any] | None:
        """Look up the canary without anchoring discovery to its timestamp."""

        payload = self._api(
            "conversations.history",
            {
                "channel": channel,
                "oldest": thread_ts,
                "latest": thread_ts,
                "inclusive": "true",
                "limit": "1",
            },
        )
        messages = payload.get("messages") or []
        if not isinstance(messages, list):
            raise RuntimeError("Slack conversations.history returned invalid messages")
        return next(
            (item for item in messages if isinstance(item, dict) and item.get("ts") == thread_ts),
            None,
        )

    def replies(self, channel: str, thread_ts: str) -> list[dict[str, Any]]:
        messages: list[dict[str, Any]] = []
        cursor = ""
        while True:
            fields = {"channel": channel, "ts": thread_ts, "limit": "200"}
            if cursor:
                fields["cursor"] = cursor
            payload = self._api("conversations.replies", fields)
            page = payload.get("messages") or []
            if not isinstance(page, list):
                raise RuntimeError("Slack conversations.replies returned invalid messages")
            messages.extend(item for item in page if isinstance(item, dict))
            cursor = str((payload.get("response_metadata") or {}).get("next_cursor") or "")
            if not cursor:
                return messages

    def post_reply(self, channel: str, thread_ts: str, text: str) -> str:
        payload = self._api(
            "chat.postMessage", {"channel": channel, "thread_ts": thread_ts, "text": text}
        )
        ts = str(payload.get("ts") or "")
        if not ts:
            raise RuntimeError("Slack chat.postMessage returned no ts")
        return ts

    def download(self, url: str) -> bytes:
        if not _is_slack_url(url):
            raise RuntimeError("Slack file URL is outside slack.com")
        request = urllib.request.Request(
            url, headers={"Authorization": f"Bearer {self.config.slack_token}"}
        )
        try:
            with self.opener.open(request, timeout=self.config.http_timeout_seconds) as response:
                raw = bytes(response.read(MAX_FILE_BYTES + 1))
        except urllib.error.HTTPError as exc:
            if exc.code == 429:
                raise RateLimited(_retry_after(exc.headers, self.config.poll_seconds)) from exc
            raise
        if len(raw) > MAX_FILE_BYTES:
            raise RuntimeError("Slack Email HTML file exceeds the intake byte limit")
        return raw


Sender = Callable[[urllib.request.Request, float], tuple[int, bytes]]


class CapabilityRedirectHandler(urllib.request.HTTPRedirectHandler):
    """A schema from another URL cannot attest to the configured hook route."""

    def redirect_request(
        self, req: Any, fp: Any, code: int, msg: str, headers: Any, newurl: str
    ) -> None:
        return None


def _send(request: urllib.request.Request, timeout: float) -> tuple[int, bytes]:
    try:
        open_request = (
            urllib.request.build_opener(CapabilityRedirectHandler()).open
            if request.get_method() == "GET"
            else urllib.request.urlopen
        )
        with open_request(request, timeout=timeout) as response:
            return response.status, bytes(response.read())
    except urllib.error.HTTPError as exc:
        return exc.code, bytes(exc.read())


class CurieHookClient:
    """Signed hook client that requires the exact source-thread receipt."""

    def __init__(self, config: Config, *, send: Sender = _send) -> None:
        self.config = config
        self.send = send

    def verify_target_capability(self) -> None:
        """Refuse an older hook route before any Slack or hook side effect."""

        parsed = urllib.parse.urlsplit(self.config.hook_url)
        prefix, separator, _route = parsed.path.rpartition("/hooks/")
        if not separator:
            raise RuntimeError("Curie hook URL does not name a hooks route")
        url = urllib.parse.urlunsplit(
            (parsed.scheme, parsed.netloc, prefix + "/openapi.json", "", "")
        )
        # Capability discovery carries neither the hook secret nor a delivery id.
        request = urllib.request.Request(url, method="GET")
        status, body = self.send(request, self.config.http_timeout_seconds)
        if status != 200:
            raise RuntimeError(f"Curie API capability discovery returned HTTP {status}")
        try:
            document = json.loads(body)
            route = document["paths"]["/hooks/{agent_id}/{hook}"]
            operation = route["post"]
            parameters = route.get("parameters", []) + operation.get("parameters", [])
            names = {parameter["name"] for parameter in parameters if parameter["in"] == "query"}
        except (ValueError, KeyError, TypeError, AttributeError) as exc:
            raise RuntimeError("Curie API capability description is invalid") from exc
        if not {"conversation_id", "placeholder"}.issubset(names):
            raise RuntimeError("Curie API does not support explicit hook reply targets")

    def deliver(
        self,
        payload: dict[str, Any],
        *,
        conversation_id: str,
        placeholder: str,
        delivery_id: str,
    ) -> None:
        body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        signature = (
            "sha256=" + hmac.new(self.config.hook_secret.encode(), body, hashlib.sha256).hexdigest()
        )
        parsed = urllib.parse.urlsplit(self.config.hook_url)
        query = dict(urllib.parse.parse_qsl(parsed.query, keep_blank_values=True))
        query.update(
            {
                "kind": "slack",
                "address": self.config.channel_id,
                "conversation_id": conversation_id,
                "placeholder": placeholder,
            }
        )
        if self.config.slack_adapter:
            query["adapter"] = self.config.slack_adapter
        url = urllib.parse.urlunsplit(
            (parsed.scheme, parsed.netloc, parsed.path, urllib.parse.urlencode(query), "")
        )
        request = urllib.request.Request(
            url,
            data=body,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "X-Curie-Delivery-Id": delivery_id,
                "X-Curie-Signature-256": signature,
            },
        )
        status, response_body = self.send(request, self.config.http_timeout_seconds)
        if status not in {200, 202}:
            raise RuntimeError(f"Curie hook returned HTTP {status}")
        try:
            receipt = json.loads(response_body)
        except json.JSONDecodeError as exc:
            raise RuntimeError("Curie hook returned an invalid receipt") from exc
        if receipt.get("conversation_id") != conversation_id:
            raise RuntimeError("Curie hook returned a detached conversation")


def _own_replies(replies: list[dict[str, Any]], bot_user_id: str) -> list[dict[str, Any]]:
    return [
        reply
        for reply in replies
        if reply.get("user") == bot_user_id and str(reply.get("text") or "").strip()
    ]


def process_message(
    config: Config,
    slack: Any,
    hook: Any,
    message: dict[str, Any],
    *,
    bot_user_id: str,
    now: float,
    force_probe: bool = False,
) -> bool:
    """Acknowledge or dispatch one selected root."""

    root_ts = str(message["ts"])
    own = _own_replies(slack.replies(config.channel_id, root_ts), bot_user_id)
    completed = next(
        (reply for reply in own if str(reply.get("text") or "").strip() != PLACEHOLDER),
        None,
    )
    if completed is not None:
        if not force_probe:
            return True
        placeholder = str(completed["ts"])
        pending = None
    else:
        pending = next(
            (reply for reply in own if str(reply.get("text") or "").strip() == PLACEHOLDER),
            None,
        )
    if completed is None and pending is not None:
        placeholder = str(pending["ts"])
        if now - float(placeholder) > config.placeholder_stale_seconds:
            raise RuntimeError(f"stale placeholder in Slack thread {root_ts}")
    elif completed is None:
        placeholder = slack.post_reply(config.channel_id, root_ts, PLACEHOLDER)

    file = message["files"][0]
    file_url = str(file.get("url_private") or "")
    if not file_url:
        raise RuntimeError(f"Slack Email file in thread {root_ts} has no private URL")
    email_text = html_to_text(slack.download(file_url))
    if not email_text:
        raise RuntimeError(f"Slack Email file in thread {root_ts} has no readable text")
    hook.deliver(
        {
            "source": "slack-email",
            "channel_id": config.channel_id,
            "conversation_id": root_ts,
            "file_id": str(file.get("id") or ""),
            "subject": _subject(message),
            "email_text": email_text,
            "policy": "read-only investigation; never mutate or request approval",
        },
        conversation_id=root_ts,
        placeholder=placeholder,
        delivery_id=f"slack-email:{config.channel_id}:{root_ts}",
    )
    return completed is not None


class ScanState:
    """Disposable discovery progress; Slack replies remain the durable state."""

    def __init__(self) -> None:
        self.last_start: float | None = None
        self.pending: dict[str, dict[str, Any]] = {}
        self.acknowledged: set[str] = set()

    def oldest(self, config: Config) -> str:
        if self.last_start is None:
            return config.scan_not_before
        overlap = config.placeholder_stale_seconds + 2 * config.poll_seconds
        return f"{max(float(config.scan_not_before), self.last_start - overlap):.6f}"


def scan_once(
    config: Config, slack: Any, hook: Any, *, now: float, state: ScanState | None = None
) -> None:
    """Process every selected root oldest first; any broken item fails the scan."""

    state = state if state is not None else ScanState()
    bot_user_id = slack.bot_user_id()
    history = slack.history(config.channel_id, state.oldest(config))
    canary = slack.root(config.channel_id, config.canary_thread_ts)
    if canary is None:
        raise RuntimeError("configured Slack Email canary is missing from channel history")
    if not is_candidate(canary, config):
        raise RuntimeError("configured Slack Email canary no longer matches intake criteria")
    candidates = dict(state.pending)
    candidates.update(
        {
            str(message["ts"]): message
            for message in history
            if is_candidate(message, config)
            and float(message["ts"]) >= float(config.scan_not_before)
        }
    )
    candidates[config.canary_thread_ts] = canary
    for root_ts, message in sorted(candidates.items(), key=lambda item: float(item[0])):
        force_probe = root_ts == config.canary_thread_ts
        if root_ts in state.acknowledged and not force_probe:
            continue
        completed = process_message(
            config,
            slack,
            hook,
            message,
            bot_user_id=bot_user_id,
            now=now,
            force_probe=force_probe,
        )
        if completed:
            state.acknowledged.add(root_ts)
            state.pending.pop(root_ts, None)
        else:
            state.pending[root_ts] = message
    # Advance only after every pending item and the independent probe succeeded.
    state.last_start = now
    oldest = float(state.oldest(config))
    state.acknowledged = {ts for ts in state.acknowledged if float(ts) >= oldest}


class Health:
    """Thread-safe freshness state shared by the scanner and probe server."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.last_success: float | None = None
        self.scans = 0

    def mark_success(self, when: float) -> None:
        with self._lock:
            self.last_success = when
            self.scans += 1

    def snapshot(self) -> tuple[float | None, int]:
        with self._lock:
            return self.last_success, self.scans

    def ready(self, now: float, config: Config) -> bool:
        last_success, _scans = self.snapshot()
        return last_success is not None and now - last_success <= 2 * config.poll_seconds


def render_metrics(health: Health, now: float, config: Config) -> str:
    """Render the bounded metrics used for inspection and readiness alerting."""

    last_success, scans = health.snapshot()
    age = -1 if last_success is None else max(0.0, now - last_success)
    ready = 1 if health.ready(now, config) else 0
    return (
        "# TYPE sre_slack_email_intake_ready gauge\n"
        f"sre_slack_email_intake_ready {ready}\n"
        "# TYPE sre_slack_email_intake_last_success_age_seconds gauge\n"
        f"sre_slack_email_intake_last_success_age_seconds {age:.3f}\n"
        "# TYPE sre_slack_email_intake_scans_total counter\n"
        f"sre_slack_email_intake_scans_total {scans}\n"
    )


def handler(health: Health, config: Config) -> type[BaseHTTPRequestHandler]:
    """Build the process-local health and metrics handler."""

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: object) -> None:
            LOG.debug(format, *args)

        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/livez":
                self._reply(200, "ok\n")
            elif self.path == "/readyz":
                ready = health.ready(time.time(), config)
                self._reply(200 if ready else 503, "ready\n" if ready else "stale\n")
            elif self.path == "/metrics":
                self._reply(200, render_metrics(health, time.time(), config))
            else:
                self._reply(404, "not found\n")

        def _reply(self, status: int, body: str) -> None:
            raw = body.encode()
            self.send_response(status)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

    return Handler


def main() -> None:
    """Keep throttled scans alive; other failures restart and page."""

    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    config = Config.from_env()
    health = Health()
    server = ThreadingHTTPServer(("0.0.0.0", 8080), handler(health, config))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    slack = SlackClient(config)
    hook = CurieHookClient(config)
    hook.verify_target_capability()
    state = ScanState()
    while True:
        started = time.time()
        try:
            scan_once(config, slack, hook, now=started, state=state)
        except RateLimited as exc:
            LOG.warning("Slack Email scan throttled; waiting %.3f seconds", exc.retry_after)
            time.sleep(exc.retry_after)
            continue
        health.mark_success(time.time())
        LOG.info("Slack Email alert scan completed")
        time.sleep(max(0.0, config.poll_seconds - (time.time() - started)))


if __name__ == "__main__":
    main()

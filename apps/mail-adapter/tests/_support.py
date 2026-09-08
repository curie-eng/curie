"""The two fake external services, the clients that drive the adapter, and the
constants every test module shares.

Kept out of `conftest.py` because the root suite runs pytest with
``--import-mode=importlib``, under which a conftest is not importable by name.
`conftest.py` puts this directory on `sys.path` so `from _support import ...`
resolves in every test module, and holds the fixtures that wire what is here.

Only the two EXTERNAL dependencies are faked, both as real local
`ThreadingHTTPServer` instances: AgentMail's HTTP API and the platform's
channel-ingress endpoint. Everything inside `curie_mail_adapter` runs for real,
including its own egress HTTP server, the poll path, and both halves of the
completion dedupe. Nothing internal is patched.

The fake AgentMail server is not a stub that serves whatever it is handed: it
reproduces the filtering the provider documents, because a test built on a fake
that serves labeled mail the real provider would have withheld proves nothing
about production. See `MailState.visible` for the behavior and its sources.
"""

from __future__ import annotations

import http.client
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, cast

from curie_mail_adapter.egress import ADAPTER_SECRET_HEADER

INBOX = "sandbox@agentmail.to"
AGENTMAIL_API_KEY = "amk-tst"
CHANNEL_TOKEN = "chn-tst"
EGRESS_SECRET = "egr-tst"
ALLOWED_SENDER = "human@example.com"
STRANGER = "stranger@evil.example"

# The three categories AgentMail withholds from List Messages results unless the
# caller asks for them by name, and the three the API key can be denied outright:
#   "Spam, trash, blocked, and unauthenticated messages are excluded"
#     https://www.agentmail.to/docs/messages
#   "spam messages are filtered out unless you explicitly request them"
#     https://www.agentmail.to/docs/spam-virus-detection
#   include_spam / include_blocked / include_unauthenticated, each documented as
#   "Include <category> in results"
#     https://docs.agentmail.to/api-reference/inboxes/messages/list
#   "When a label visibility permission is denied, items with that label are
#   automatically excluded from list results"
#     https://docs.agentmail.to/permissions
# `trash` appears in the same documented sentence but is deliberately not modeled
# as a provider verdict: it is an operator action on their own mailbox, and the
# adapter deliberately does not reject on it.
WITHHELD_LABELS = ("spam", "blocked", "unauthenticated")
_PROCESS_STATE_ROOT = tempfile.TemporaryDirectory(prefix="curie-mail-tests-")


# --- the fake AgentMail API ---------------------------------------------------


class MailState:
    """The fake AgentMail inbox: listings, bodies, threads, and the replies sent."""

    def __init__(self) -> None:
        self.base_url = ""
        self.messages: list[dict[str, Any]] = []  # newest last, as seeded
        self.bodies: dict[str, dict[str, Any]] = {}
        self.threads: dict[str, list[dict[str, Any]]] = {}
        self.deleted_threads: set[str] = set()
        self.replies: list[tuple[str, str]] = []  # (in_reply_to_message_id, text)
        self.list_calls = 0
        # time.monotonic() per list call, so the retry CADENCE is observable at
        # the real external seam rather than inferred from the adapter's logs.
        self.list_times: list[float] = []
        self.list_queries: list[dict[str, str]] = []  # the parsed query of each list call
        self.list_authorization: list[str] = []
        self.thread_calls = 0
        # Test-only switch simulating the two futures the adapter's `labels` check
        # exists for: a provider that widens its default, or an API key that
        # carries the label-read permissions. Off by default, so a test that needs
        # labeled mail served has to name the unusual condition it assumes.
        self.leak_labeled = False
        # One-shot fault injection, per endpoint: an HTTP status to answer the
        # next call with, or 0 to drop the connection mid-request (a transport
        # failure). One-shot rather than sticky because the failures these model
        # are transient, and the recovery is half of what each test pins.
        self.fail_next_reply: int | None = None
        self.fail_next_list: int | None = None
        self.fail_next_body: int | None = None
        self.fail_next_thread: int | None = None
        # The body a NON-ZERO injected failure answers with, when a test needs
        # to choose it. Provider-authored failure bodies are exactly what the
        # adapter must never render into a log, and putting recognisable content
        # in one is the only way a test can prove it never does.
        self.injected_body: dict[str, Any] | None = None
        # A NON-JSON failure body, as an edge proxy, gateway or load balancer
        # serves. Distinct from `injected_body`, which is the provider's own
        # JSON: the adapter's terminal-vs-retryable split on a 404 turns on
        # exactly that difference, so a test cannot prove it without being able
        # to serve a body the provider would never have written.
        self.injected_raw_body: str | None = None
        # N CONSECUTIVE transport failures on List Messages before answering
        # normally, mirroring `IngressState.drop_next`. `fail_next_list` is
        # one-shot and so cannot express a repeated outage, which is the whole
        # subject of #2012: one dropped call is noise, a sustained one is the
        # condition the poller has to slow down for.
        self.drop_next_lists = 0
        # The index into `list_times` of every list call answered with a dropped
        # connection, so a test can judge the retry cadence by the gaps it KNOWS
        # were failures rather than inferring them from elapsed time on a shared
        # box, where a descheduled normal poll is indistinguishable from a
        # backed-off one.
        self.dropped_list_indexes: list[int] = []
        # Provider accepted the reply and exposed it in the thread, but the
        # HTTP response disappeared. Recovery must resolve this from the
        # provider-visible event witness rather than sending again.
        self.accept_then_drop_next_reply = False
        # Opaque pagination tokens that the fake provider refuses.
        self.invalid_page_tokens: set[str] = set()
        self.next_page_token_override_once: str | None = None
        # Sticky, per-message body failure: every Get Message for an id in this
        # set answers 500 until the test removes it. One-shot cannot express
        # either half of what the poller has to survive - a message the provider
        # will not serve at all (the poison case) or one it serves only after a
        # named number of passes (the transient case) - because the one-shot
        # fires on whichever body call happens to come first.
        self.fail_bodies: set[str] = set()
        # message_id -> how many times its body has been fetched. The retry
        # budget is only observable as a count that stops growing.
        self.body_calls: dict[str, int] = {}
        self.reply_gate = threading.Event()
        self.reply_gate.set()
        self.reply_entered = threading.Event()

    def add_inbound(
        self,
        message_id: str,
        thread_id: str = "thr-1",
        *,
        sender: str = ALLOWED_SENDER,
        subject: str = "Hello",
        text: str | None = "body text",
        labels: list[str] | None = None,
    ) -> dict[str, Any]:
        """Seed one inbound message.

        `text=None` seeds a body with no plain-text part at all, which is what
        Gmail and Outlook forwards look like: "Some email clients - particularly
        Gmail and Outlook - send forwarded emails as HTML-only, with no
        plain-text part. In these cases, `text` and `preview` will be absent."
        https://docs.agentmail.to/messages
        """
        summary = {
            "message_id": message_id,
            "thread_id": thread_id,
            "from": sender,
            "subject": subject,
            "labels": list(labels or []),
        }
        self.messages.append(summary)
        body = dict(summary)
        if text is not None:
            body["extracted_text"] = text
        self.bodies[message_id] = body
        self.threads.setdefault(thread_id, []).append(self.bodies[message_id])
        return summary

    def visible(self, query: dict[str, str]) -> list[dict[str, Any]]:
        """The listing the documented provider would return for this query.

        A message carrying a withheld label is served only when the matching
        `include_<category>=true` is present, mirroring the provider's default
        exclusion (sources on WITHHELD_LABELS above). `leak_labeled` overrides
        that to simulate a widened default or an over-permissioned key.
        """
        if self.leak_labeled:
            return list(self.messages)
        visible = []
        for message in self.messages:
            withheld = set(message.get("labels") or []) & set(WITHHELD_LABELS)
            if withheld and not all(query.get(f"include_{label}") == "true" for label in withheld):
                continue
            visible.append(message)
        return visible

    def list_page(self, query: dict[str, str]) -> dict[str, Any]:
        """One page of the listing, built the way the provider documents it.

        List Messages "Lists messages in the inbox, most recent first", the
        `messages` array is "Ordered by timestamp descending", the request takes
        `limit` ("Limit of number of items returned") and `page_token` ("Page
        token for pagination"), and the envelope carries `count` and
        `next_page_token`.
        https://docs.agentmail.to/api-reference/inboxes/messages/list

        The token is opaque to the caller; this fake makes it the index of the
        next item, which is the only property a caller may rely on. Honouring
        `limit` is what lets a test see a message pushed off the first page: a
        fake that serves the whole inbox on every call hides that entirely.
        """
        if query.get("page_token") in self.invalid_page_tokens:
            self.invalid_page_tokens.discard(query["page_token"])
            raise ValueError("invalid page token")
        newest_first = list(reversed(self.visible(query)))
        start = int(query.get("page_token") or 0)
        limit = int(query["limit"]) if query.get("limit") else len(newest_first)
        window = newest_first[start : start + limit]
        page: dict[str, Any] = {"messages": window, "count": len(window), "limit": limit}
        if start + len(window) < len(newest_first):
            page["next_page_token"] = str(start + len(window))
        if self.next_page_token_override_once is not None:
            page["next_page_token"] = self.next_page_token_override_once
            self.next_page_token_override_once = None
        return page

    def hold_replies(self) -> None:
        """Make the next reply block inside the provider until `release_replies`."""
        self.reply_entered.clear()
        self.reply_gate.clear()

    def release_replies(self) -> None:
        self.reply_gate.set()

    def replies_to(self, message_id: str) -> list[str]:
        return [text for mid, text in self.replies if mid == message_id]


class IngressState:
    """The fake platform ingress: what it received and what it answers."""

    def __init__(self) -> None:
        self.url = ""
        self.requests: list[tuple[Any, dict[str, Any]]] = []  # (headers, body)
        self.attempts = 0  # includes the ones dropped mid-flight
        self.attempt_times: list[float] = []
        self.drop_next = 0  # simulate N transport failures before answering
        self.response: tuple[int, dict[str, Any]] = (
            200,
            {"event_id": "chn-1-abc", "stream_id": "1-0", "duplicate": False},
        )
        self.responses: list[tuple[int, dict[str, Any], dict[str, str]]] = []

    def delivery_ids(self) -> list[str]:
        return [body["delivery_id"] for _headers, body in self.requests]


class _JsonHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.0"

    def log_message(self, *args: Any) -> None:
        pass

    def _read_body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length).decode()
        return json.loads(raw) if raw else {}

    def _send(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_raw(self, status: int, text: str) -> None:
        """Answer with a body that is not JSON, the way an edge 404 page is."""
        body = text.encode()
        self.send_response(status)
        self.send_header("Content-Type", "text/html")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class MailHandler(_JsonHandler):
    @property
    def state(self) -> MailState:
        return cast(MailState, self.server.state)  # type: ignore[attr-defined]

    def _parts(self) -> list[str]:
        path = urllib.parse.urlparse(self.path).path
        return [urllib.parse.unquote(part) for part in path.strip("/").split("/")]

    def _query(self) -> dict[str, str]:
        raw = urllib.parse.urlparse(self.path).query
        return {k: v[0] for k, v in urllib.parse.parse_qs(raw, keep_blank_values=True).items()}

    def _injected(self, failure: int | None) -> bool:
        """Answer with an armed one-shot fault, if there was one. Did it fire?"""
        if failure is None:
            return False
        if failure == 0:
            self.close_connection = True  # a real transport failure
            return True
        raw = self.state.injected_raw_body
        if raw is not None:
            self._send_raw(failure, raw)
            return True
        body = self.state.injected_body
        self._send(failure, body if body is not None else {"detail": "injected provider failure"})
        return True

    def do_GET(self) -> None:
        parts = self._parts()
        state = self.state
        # /v0/inboxes/{inbox}/messages
        if len(parts) == 4 and parts[3] == "messages":
            query = self._query()
            state.list_calls += 1
            state.list_times.append(time.monotonic())
            state.list_queries.append(query)
            state.list_authorization.append(self.headers.get("Authorization") or "")
            if state.drop_next_lists > 0:
                state.drop_next_lists -= 1
                # `list_times` was appended above, so this is that call's index.
                state.dropped_list_indexes.append(len(state.list_times) - 1)
                self.close_connection = True  # a real transport failure
                return
            failure, state.fail_next_list = state.fail_next_list, None
            if self._injected(failure):
                return
            return self._send(200, state.list_page(query))
        # /v0/inboxes/{inbox}/messages/{message_id}
        if len(parts) == 5 and parts[3] == "messages":
            state.body_calls[parts[4]] = state.body_calls.get(parts[4], 0) + 1
            failure, state.fail_next_body = state.fail_next_body, None
            if failure is None and parts[4] in state.fail_bodies:
                failure = 500
            if self._injected(failure):
                return
            body = state.bodies.get(parts[4])
            if body is None:
                return self._send(404, {"detail": "no such message"})
            return self._send(200, body)
        # /v0/inboxes/{inbox}/threads/{thread_id}
        if len(parts) == 5 and parts[3] == "threads":
            state.thread_calls += 1
            failure, state.fail_next_thread = state.fail_next_thread, None
            if self._injected(failure):
                return
            if parts[4] in state.deleted_threads:
                return self._send(404, {"detail": "no such thread"})
            return self._send(200, {"messages": state.threads.get(parts[4], [])})
        self._send(404, {"detail": "not found"})

    def do_POST(self) -> None:
        parts = self._parts()
        state = self.state
        # /v0/inboxes/{inbox}/messages/{message_id}/reply
        if len(parts) == 6 and parts[3] == "messages" and parts[5] == "reply":
            message_id = parts[4]
            text = self._read_body().get("text", "")
            state.reply_entered.set()
            state.reply_gate.wait(30)
            failure = state.fail_next_reply
            if failure is not None:
                state.fail_next_reply = None
                if failure == 0:
                    self.close_connection = True  # a real transport failure
                    return
                return self._send(failure, {"detail": "injected provider failure"})
            state.replies.append((message_id, text))
            thread_id = state.bodies[message_id]["thread_id"]
            reply_id = f"{message_id}-reply-{len(state.replies)}"
            state.threads.setdefault(thread_id, []).append(
                {
                    "message_id": reply_id,
                    "thread_id": thread_id,
                    "text": text,
                    "labels": ["sent"],
                }
            )
            if state.accept_then_drop_next_reply:
                state.accept_then_drop_next_reply = False
                self.close_connection = True
                return
            return self._send(200, {"message_id": reply_id, "thread_id": thread_id})
        self._send(404, {"detail": "not found"})


class IngressHandler(_JsonHandler):
    @property
    def state(self) -> IngressState:
        return cast(IngressState, self.server.state)  # type: ignore[attr-defined]

    def do_POST(self) -> None:
        state = self.state
        state.attempts += 1
        state.attempt_times.append(time.monotonic())
        if state.drop_next > 0:
            state.drop_next -= 1
            self.close_connection = True  # a real transport failure at the client
            return
        body = self._read_body()
        # Kept as the parsed message so header lookups stay case-insensitive,
        # exactly as HTTP (and Starlette on the platform side) treats them.
        state.requests.append((self.headers, body))
        if state.responses:
            status, payload, headers = state.responses.pop(0)
        else:
            status, payload = state.response
            headers = {}
        body_bytes = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body_bytes)))
        for name, value in headers.items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(body_bytes)


# --- a redirecting origin, and the origin it points at ------------------------
#
# Both are external HTTP endpoints standing in for a hostile or misconfigured
# provider, the same seam the two fakes above occupy. `RedirectState` answers
# every request with a 3xx naming `location`; `SinkState` records every request
# header that actually reaches the named origin, so a credential replayed by a
# followed redirect is visible as data rather than inferred.


class RedirectState:
    def __init__(self) -> None:
        self.url = ""
        self.location = ""
        self.status = 302
        self.hits = 0


class SinkState:
    def __init__(self) -> None:
        self.url = ""
        self.headers: list[Any] = []

    def credentials_seen(self) -> list[str]:
        """Every credential-bearing header value that reached this origin."""
        return [
            value
            for headers in self.headers
            for name in ("Authorization", "X-API-Key", ADAPTER_SECRET_HEADER)
            if (value := headers.get(name))
        ]


class RedirectHandler(_JsonHandler):
    def _redirect(self) -> None:
        state: RedirectState = self.server.state  # type: ignore[attr-defined]
        state.hits += 1
        length = int(self.headers.get("Content-Length") or 0)
        self.rfile.read(length)
        self.send_response(state.status)
        self.send_header("Location", state.location)
        self.send_header("Content-Length", "0")
        self.end_headers()

    do_GET = _redirect
    do_POST = _redirect


class SinkHandler(_JsonHandler):
    def _record(self) -> None:
        state: SinkState = self.server.state  # type: ignore[attr-defined]
        length = int(self.headers.get("Content-Length") or 0)
        self.rfile.read(length)
        state.headers.append(self.headers)
        self._send(200, {"messages": []})

    do_GET = _record
    do_POST = _record


def serve(handler: type[BaseHTTPRequestHandler], state: Any) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    server.state = state  # type: ignore[attr-defined]
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


# --- the neutral reply wire the platform speaks to the adapter ---------------


def target(conversation_id: str = "thr-1", reply_ref: str | None = "msg-1") -> dict[str, Any]:
    return {
        "kind": "email",
        "address": INBOX,
        "conversation_id": conversation_id,
        "reply_ref": reply_ref,
    }


def completed(
    event_id: str,
    conversation_id: str = "thr-1",
    reply_ref: str | None = "msg-1",
    outcome: str = "delivered",
) -> dict[str, Any]:
    return {
        "version": "1.0",
        "event": "turn.completed",
        "target": target(conversation_id, reply_ref),
        "event_id": event_id,
        "outcome": outcome,
    }


def update(
    text: str, conversation_id: str = "thr-1", reply_ref: str | None = "msg-1"
) -> dict[str, Any]:
    return {
        "version": "1.0",
        "event": "reply.update",
        "target": target(conversation_id, reply_ref),
        "text": text,
    }


def reply_post(text: str, conversation_id: str = "thr-1") -> dict[str, Any]:
    """A NEW platform-owned message in the conversation, the approval card being the one.

    It carries `message` (an `OutboundMessage`) and `requested_by` rather than a
    bare `text`, and its target names the conversation with a null `reply_ref`:
    `docs/guides/building-a-channel-adapter.md` section 5, and the `ReplyPost`
    the kernel emits in `apps/worker/src/curie_worker/kernel.py`.
    """
    return {
        "version": "1.0",
        "event": "reply.post",
        "target": target(conversation_id, reply_ref=None),
        "message": {"version": "1.0", "text": text},
        "requested_by": "U9",
    }


def turn_status(conversation_id: str = "thr-1", status: str = "thinking") -> dict[str, Any]:
    return {
        "version": "1.0",
        "event": "turn.status",
        "target": target(conversation_id),
        "status": status,
    }


def post_event(
    url: str, event: dict[str, Any], *, secret: str | None = EGRESS_SECRET
) -> tuple[int, Any]:
    """POST a neutral reply event the way the platform's HttpReplyAdapter does."""
    req = urllib.request.Request(url, data=json.dumps(event).encode(), method="POST")
    req.add_header("Content-Type", "application/json")
    if secret is not None:
        req.add_header(ADAPTER_SECRET_HEADER, secret)
    return _send(req)


def post_bytes(
    url: str,
    body: bytes,
    *,
    secret: str | None = EGRESS_SECRET,
    content_type: str = "application/json",
    headers: dict[str, str] | None = None,
) -> tuple[int, Any]:
    """POST exact bytes so framing and parser guards are exercised over HTTP."""
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", content_type)
    if secret is not None:
        req.add_header(ADAPTER_SECRET_HEADER, secret)
    for name, value in (headers or {}).items():
        req.add_header(name, value)
    return _send(req)


def post_raw(url: str, *, secret: str | None = None) -> tuple[int, Any]:
    """POST an empty body, for the paths that must refuse before reading one."""
    req = urllib.request.Request(url, data=b"{}", method="POST")
    req.add_header("Content-Type", "application/json")
    if secret is not None:
        req.add_header(ADAPTER_SECRET_HEADER, secret)
    return _send(req)


def get(url: str) -> tuple[int, Any]:
    return _send(urllib.request.Request(url, method="GET"))


def _send(req: urllib.request.Request) -> tuple[int, Any]:
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            return response.status, _decode(response.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, _decode(exc.read().decode())
    except (http.client.RemoteDisconnected, urllib.error.URLError, TimeoutError) as exc:
        return 0, {"transport_error": type(exc).__name__}


def _decode(raw: str) -> Any:
    try:
        return json.loads(raw or "{}")
    except ValueError:
        return raw


# --- the real process entry point --------------------------------------------


def free_port() -> int:
    """An ephemeral port, released immediately so the adapter process can bind it."""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def adapter_env(
    *,
    agentmail_base_url: str,
    api_url: str,
    port: int,
    ingress_enabled: str = "true",
    allowed_senders: str = ALLOWED_SENDER,
    poll_interval_seconds: str = "0.05",
    **overrides: str,
) -> dict[str, str]:
    """A closed-world environment for `python -m curie_mail_adapter`.

    Deliberately not `os.environ.copy()`: an ambient `CURIE_*` on the developer's
    box would silently change what the boot gates see.
    """
    env = {
        "PATH": os.environ.get("PATH", ""),
        "HOME": os.environ.get("HOME", "/tmp"),
        # Interpreter plumbing, not configuration: the child has to import the
        # package the same way this process did.
        "PYTHONPATH": os.environ.get("PYTHONPATH", ""),
        "PYTHONUNBUFFERED": "1",
        "AGENTMAIL_API_KEY": AGENTMAIL_API_KEY,
        "AGENTMAIL_INBOX": INBOX,
        "AGENTMAIL_BASE_URL": agentmail_base_url,
        "CURIE_API_URL": api_url,
        "CURIE_CHANNEL_TOKEN": CHANNEL_TOKEN,
        "CURIE_EGRESS_SECRET": EGRESS_SECRET,
        "ADAPTER_INGRESS_ENABLED": ingress_enabled,
        "CURIE_MAIL_POLL_INTERVAL_SECONDS": poll_interval_seconds,
        "CURIE_MAIL_INGRESS_RETRY_DELAY_SECONDS": "0.01",
        "CURIE_MAIL_PORT": str(port),
        "CURIE_MAIL_STATE_PATH": os.path.join(
            _PROCESS_STATE_ROOT.name, f"mail-state-{port}.sqlite3"
        ),
        "CURIE_MAIL_ALLOWED_SENDERS": allowed_senders,
    }
    env.update(overrides)
    return env


def spawn_adapter(env: dict[str, str]) -> subprocess.Popen[str]:
    return subprocess.Popen(
        [sys.executable, "-m", "curie_mail_adapter"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def exit_of(proc: subprocess.Popen[str], timeout: float = 30.0) -> tuple[int, str]:
    """The process's exit code and combined output, killing it if it never exits."""
    try:
        output, _ = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        proc.kill()
        output, _ = proc.communicate(timeout=10)
        raise AssertionError(
            f"the adapter did not exit within {timeout}s; output:\n{output}"
        ) from exc
    return proc.returncode, output


def stop(proc: subprocess.Popen[str]) -> str:
    proc.terminate()
    try:
        output, _ = proc.communicate(timeout=15)
    except subprocess.TimeoutExpired:
        proc.kill()
        output, _ = proc.communicate(timeout=10)
    return output


def wait_until(predicate: Callable[[], bool], timeout: float = 10.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return predicate()


def wait_for_healthz(port: int, proc: subprocess.Popen[str], timeout: float = 30.0) -> None:
    """Block until the adapter answers its health path, or fail with its output."""
    url = f"http://127.0.0.1:{port}/healthz"

    def answered() -> bool:
        if proc.poll() is not None:
            return True
        try:
            return get(url)[0] == 200
        except OSError:
            return False

    wait_until(answered, timeout)
    if proc.poll() is not None:
        raise AssertionError(f"the adapter exited during boot:\n{stop(proc)}")
    status, _ = get(url)
    assert status == 200, f"GET /healthz -> {status}"


def wait_for_readyz(port: int, proc: subprocess.Popen[str], timeout: float = 30.0) -> None:
    """Wait for completed durable startup, not merely a live HTTP thread."""
    url = f"http://127.0.0.1:{port}/readyz"

    def answered() -> bool:
        if proc.poll() is not None:
            return True
        try:
            return get(url)[0] == 200
        except OSError:
            return False

    wait_until(answered, timeout)
    if proc.poll() is not None:
        raise AssertionError(f"the adapter exited during boot:\n{stop(proc)}")
    status, _ = get(url)
    assert status == 200, f"GET /readyz -> {status}"

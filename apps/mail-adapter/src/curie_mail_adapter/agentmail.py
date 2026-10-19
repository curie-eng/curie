"""AgentMail: the built-in mail provider (ADR 0228) and its REST client.

``AgentMailClient`` is four calls, all stdlib ``urllib``. Every method returns
``(status, parsed)`` and never raises for a transport failure, which is
reported as status ``0`` so the caller can tell "the provider said no" from
"the provider was unreachable". Bearer auth is on every call
(https://docs.agentmail.to/api-reference/overview).

``AgentMailProvider`` puts that client behind the mail provider interface in
``provider.py``: it owns AgentMail's labels, paging, text fields, header map,
404 body rule and egress pinning, and maps every HTTP answer to a neutral
failure kind.
"""

from __future__ import annotations

import email.utils
import errno
import http.client
import ipaddress
import json
import socket
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from .config import MailAdapterConfig
from .provider import (
    ACCEPTED,
    FAILURE_CAUSE_MAX_CHARS,
    INTERFACE_VERSION,
    UNVERIFIABLE,
    Accepted,
    Failure,
    Message,
    MessagePage,
    MessageSummary,
    SenderVerdict,
    ThreadAnswer,
)

HTTP_TIMEOUT_SECONDS = 30.0
EGRESS_REFUSAL_ERROR = "connection_refused"

# The categories AgentMail withholds from List Messages results unless the caller
# asks for them by name. Sending them explicitly changes nothing about what a
# correct provider returns today, and that is the point: a provider that changes
# a default, or a key that carries the label-read permissions, cannot silently
# widen what reaches the agent. Parameter names and their "Include <category> in
# results" semantics are from
# https://docs.agentmail.to/api-reference/inboxes/messages/list ; the documented
# default exclusion they restate is from https://www.agentmail.to/docs/messages .
# These are constants, not parameters and not config: no caller can turn them on.
EXCLUDED_CATEGORIES = {
    "include_spam": "false",
    "include_blocked": "false",
    "include_unauthenticated": "false",
}


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Refuse every 3xx instead of following it.

    `docs/guides/building-a-channel-adapter.md` section 4: "Never redirect. A 3xx
    is treated as a delivery failure and is not followed, because following it
    would replay the egress secret at whatever origin the redirect named."
    `urlopen` does the opposite by default, rebuilding the request for the new URL
    with every header the caller added, so one 302 from a compromised or
    misconfigured origin hands out `AGENTMAIL_API_KEY` or `CURIE_CHANNEL_TOKEN`.
    Returning None here declines the redirect, and the opener chain then raises
    the 3xx as an `HTTPError`, which `request` reports as an ordinary failure.
    """

    def redirect_request(self, *args: Any, **kwargs: Any) -> None:
        return None


_OPENER = urllib.request.build_opener(_NoRedirectHandler)

EgressNetwork = ipaddress.IPv4Network | ipaddress.IPv6Network


def _admitted_candidates(host: str, port: int, cidrs: Sequence[EgressNetwork]) -> list[str]:
    """The addresses a pinned dial may use, in the order it tries them.

    Resolved addresses inside the admitted networks come first, in DNS order. When
    DNS returns none of them, the host address of every single-address network
    (/32, /128) is used in configured order. An empty result means nothing the
    egress policy admits can be dialed.
    """
    try:
        infos = socket.getaddrinfo(host, port, 0, socket.SOCK_STREAM)
    except OSError:
        infos = []
    resolved: list[str] = []
    for info in infos:
        try:
            address = ipaddress.ip_address(str(info[4][0]).split("%", 1)[0])
        except ValueError:
            continue
        if any(address in network for network in cidrs) and str(address) not in resolved:
            resolved.append(str(address))
    if resolved:
        return resolved
    return [str(network.network_address) for network in cidrs if network.num_addresses == 1]


def _pinned_create_connection(cidrs: Sequence[EgressNetwork]) -> Any:
    def create_connection(
        address: tuple[str, int],
        timeout: Any = socket._GLOBAL_DEFAULT_TIMEOUT,  # type: ignore[attr-defined]
        source_address: tuple[str, int] | None = None,
    ) -> socket.socket:
        host, port = address
        candidates = _admitted_candidates(host, port, cidrs)
        if not candidates:
            raise OSError(f"no address for {host} is admitted by the configured egress CIDRs")
        last_error: OSError | None = None
        for candidate in candidates:
            try:
                return socket.create_connection((candidate, port), timeout, source_address)
            except OSError as exc:
                last_error = exc
        assert last_error is not None
        raise last_error

    return create_connection


def _pinned_opener(cidrs: Sequence[EgressNetwork]) -> urllib.request.OpenerDirector:
    """An opener whose dials go only to addresses the egress policy admits (#2731).

    CloudFront rotates AgentMail's edge IPs, while the chart's NetworkPolicy is an
    IP snapshot; kube-router rejects a dial to a rotated edge with ICMP, which
    surfaces here as Errno 111 and silently kills discovery. Only the dialed IP
    changes: the connection keeps the URL hostname, so SNI and TLS certificate
    verification still run against it. Redirects stay refused.
    """
    create_connection = _pinned_create_connection(tuple(cidrs))

    class _PinnedHTTPConnection(http.client.HTTPConnection):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            self._create_connection = create_connection

    class _PinnedHTTPSConnection(http.client.HTTPSConnection):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            self._create_connection = create_connection

    class _PinnedHTTPHandler(urllib.request.HTTPHandler):
        def http_open(self, req: urllib.request.Request) -> http.client.HTTPResponse:
            return self.do_open(_PinnedHTTPConnection, req)

    class _PinnedHTTPSHandler(urllib.request.HTTPSHandler):
        def https_open(self, req: urllib.request.Request) -> http.client.HTTPResponse:
            return self.do_open(_PinnedHTTPSConnection, req, context=self._context)  # type: ignore[attr-defined]

    return urllib.request.build_opener(_NoRedirectHandler, _PinnedHTTPHandler, _PinnedHTTPSHandler)


@dataclass(frozen=True)
class HttpResult:
    """One bounded HTTP result, including headers needed for retry policy."""

    status: int
    body: Any
    headers: dict[str, str]


def request(
    method: str,
    url: str,
    body: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    *,
    max_response_bytes: int = 1_048_576,
    opener: urllib.request.OpenerDirector | None = None,
) -> HttpResult:
    """One bounded HTTP round trip; status 0 is transport failure.

    A 3xx is never followed; it comes back as its own status, which every caller
    already treats as a failure. ``opener`` defaults to the module's unpinned one.
    """
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    for key, value in (headers or {}).items():
        req.add_header(key, value)
    try:
        with (opener or _OPENER).open(req, timeout=HTTP_TIMEOUT_SECONDS) as response:
            raw_bytes = response.read(max_response_bytes + 1)
            status = int(response.status)
            response_headers = dict(response.headers.items())
    except urllib.error.HTTPError as exc:
        raw_bytes = exc.read(max_response_bytes + 1)
        status = int(exc.code)
        response_headers = dict(exc.headers.items())
    except OSError as exc:
        reason = exc.reason if isinstance(exc, urllib.error.URLError) else exc
        if isinstance(reason, OSError) and reason.errno == errno.ECONNREFUSED:
            return HttpResult(0, {"error": EGRESS_REFUSAL_ERROR}, {})
        return HttpResult(0, {"error": str(exc)}, {})
    if len(raw_bytes) > max_response_bytes:
        return HttpResult(
            0,
            {"error": "response body exceeds configured byte limit"},
            response_headers,
        )
    raw = raw_bytes.decode("utf-8", "replace")
    try:
        parsed: Any = json.loads(raw)
    except ValueError:
        parsed = raw
    return HttpResult(status, parsed, response_headers)


def _quoted(value: str) -> str:
    return urllib.parse.quote(value, safe="")


class AgentMailClient:
    """AgentMail's REST API. Holds the config; owns no state of its own."""

    def __init__(self, config: MailAdapterConfig) -> None:
        self.config = config
        cidrs = tuple(config.agentmail_egress_cidrs)
        self._opener = _pinned_opener(cidrs) if cidrs else _OPENER

    def _call(self, method: str, path: str, body: dict[str, Any] | None = None) -> tuple[int, Any]:
        result = request(
            method,
            f"{self.config.agentmail_base_url.rstrip('/')}{path}",
            body,
            {"Authorization": f"Bearer {self.config.agentmail_api_key}"},
            max_response_bytes=self.config.max_body_bytes,
            opener=self._opener,
        )
        return result.status, result.body

    @property
    def _inbox(self) -> str:
        return _quoted(self.config.agentmail_inbox)

    def list_messages(self, limit: int, page_token: str | None = None) -> tuple[int, Any]:
        """List the inbox, restating the provider's default exclusions explicitly.

        `page_token` walks the listing the provider pages with; the envelope's
        `next_page_token` is the value to pass back.
        https://docs.agentmail.to/api-reference/inboxes/messages/list
        """
        params: dict[str, Any] = {"limit": limit, **EXCLUDED_CATEGORIES}
        if page_token:
            params["page_token"] = page_token
        query = urllib.parse.urlencode(params)
        return self._call("GET", f"/inboxes/{self._inbox}/messages?{query}")

    def get_message(self, message_id: str) -> tuple[int, Any]:
        return self._call("GET", f"/inboxes/{self._inbox}/messages/{_quoted(message_id)}")

    def get_thread(self, thread_id: str) -> tuple[int, Any]:
        return self._call("GET", f"/inboxes/{self._inbox}/threads/{_quoted(thread_id)}")

    def reply(self, message_id: str, text: str, *, reply_all: bool = False) -> tuple[int, Any]:
        """Reply to one message: to its sender, or with ``reply_all`` to everyone on it.

        Reply To Message takes "reply_all (boolean, optional): Reply to all
        recipients of the original message"; without it the reply goes to the
        sender alone, which is every reply this adapter sends except the
        approval request and its outcome (ADR-0177 amendment A5).
        https://docs.agentmail.to/api-reference/inboxes/messages/reply
        """
        body: dict[str, Any] = {"text": text}
        if reply_all:
            body["reply_all"] = True
        return self._call(
            "POST",
            f"/inboxes/{self._inbox}/messages/{_quoted(message_id)}/reply",
            body,
        )


# -- the built-in mail provider (ADR 0228) -------------------------------------

OVERSIZE_ERROR = "response body exceeds configured byte limit"


class AgentMailProvider:
    """AgentMail behind the mail provider interface.

    AgentMail exposes labels and an arbitrary header map, without a trusted
    aligned authentication verdict or a guarantee of header provenance or
    stripping, and its inbound policy permits DMARC failure under ``p=none``
    (https://docs.agentmail.to/api-reference/inboxes/messages/get,
    https://docs.agentmail.to/knowledge-base/inbound-emails-missing). So this
    provider declares ``verifies_senders`` false and every read carries
    ``unverifiable``: an AgentMail install refuses every message from the
    listing alone, before any body fetch.

    A reply is visible in its thread as soon as Reply To Message returns, so
    ``sent_visible_within`` is zero.
    """

    name = "agentmail"
    interface_version = INTERFACE_VERSION
    verifies_senders = False
    sent_visible_within = 0.0

    def __init__(self, config: MailAdapterConfig, client: AgentMailClient | None = None) -> None:
        self.config = config
        self.client = client if client is not None else AgentMailClient(config)

    @property
    def inbox_address(self) -> str:
        """``AGENTMAIL_INBOX``, so every existing conversation keeps its identity."""
        return self.config.agentmail_inbox

    def config_problems(self) -> list[str]:
        """The AgentMail settings this provider cannot run without."""
        problems = []
        for env_name, value in (
            ("AGENTMAIL_INBOX", self.config.agentmail_inbox),
            ("AGENTMAIL_API_KEY", self.config.agentmail_api_key),
        ):
            if not value.strip():
                problems.append(f"{env_name} is required and is unset or empty")
        return problems

    def list_messages(self, cursor: str | None, limit: int) -> MessagePage | Failure:
        """One page of the inbox, with the three ``include_*=false`` constants."""
        status, page = self.client.list_messages(limit, cursor)
        if status != 200 or not isinstance(page, dict):
            return self._failure(status, page)
        summaries = []
        for item in page.get("messages", []):
            if not isinstance(item, dict):
                continue
            message_id = str(item.get("message_id") or "")
            summaries.append(
                MessageSummary(
                    message_id=message_id,
                    thread_id=str(item.get("thread_id") or message_id),
                    sent_by_inbox="sent" in _labels(item),
                    senders=_mailboxes(item.get("from")),
                )
            )
        next_cursor = page.get("next_page_token") or None
        return MessagePage(tuple(summaries), str(next_cursor) if next_cursor else None)

    def get_message(self, message_id: str) -> Message | Failure:
        """Get Message, normalized. ``extracted_text`` is the new text without quoted history."""
        status, full = self.client.get_message(message_id)
        if status != 200 or not isinstance(full, dict):
            return self._failure(status, full)
        headers = full.get("headers")
        return Message(
            message_id=str(full.get("message_id") or message_id),
            thread_id=str(full.get("thread_id") or message_id),
            subject=str(full.get("subject") or ""),
            senders=_mailboxes(full.get("from")),
            to=_mailboxes(full.get("to")),
            cc=_mailboxes(full.get("cc")),
            new_text=_text_or_none(full.get("extracted_text")),
            text=_text_or_none(full.get("text")),
            new_html=_text_or_none(full.get("extracted_html")),
            html=_text_or_none(full.get("html")),
            headers=(
                {str(key): str(value) for key, value in headers.items()}
                if isinstance(headers, dict)
                else None
            ),
            verdict=self._verdict(full),
        )

    def _verdict(self, full: dict[str, Any]) -> SenderVerdict:
        """Always ``unverifiable``: AgentMail supplies nothing a sender cannot write.

        Labels, the absence of a rejection label and every header in the map
        are provider or sender supplied, so none of them is a verdict.
        """
        return UNVERIFIABLE

    def send_reply(self, message_id: str, text: str, reply_all: bool) -> Accepted | Failure:
        """Reply To Message, sender-only unless ``reply_all``."""
        status, body = self.client.reply(message_id, text, reply_all=reply_all)
        if 200 <= status < 300:
            return ACCEPTED
        return self._failure(status, body)

    def thread_contains(self, thread_id: str, marker: str) -> ThreadAnswer | Failure:
        """Get Thread, scanning each message's text fields for the marker.

        Only AgentMail's own answer is deletion: a 404 carrying a JSON object.
        A 404 with any other body came from something between the adapter and
        AgentMail, such as an edge or gateway page, and is unreadable, because
        a deletion is a permanent tombstone after which no retry asks again.
        https://docs.agentmail.to/api-reference/inboxes/threads/get
        """
        status, thread = self.client.get_thread(thread_id)
        if status == 404:
            if isinstance(thread, dict):
                return "deleted"
            return Failure("server_error", "http_404_without_provider_body")
        if status != 200 or not isinstance(thread, dict):
            return self._failure(status, thread)
        for message in thread.get("messages", []):
            if not isinstance(message, dict):
                continue
            for field in ("extracted_text", "text", "preview"):
                if marker in str(message.get(field) or ""):
                    return "yes"
        return "no"

    def _failure(self, status: int, body: Any) -> Failure:
        """Map one AgentMail answer to a neutral failure with a bounded cause.

        Only a status 0 body is one this package wrote (``request`` builds it
        for a refused connection, another ``OSError`` or an oversize response),
        so only that body is read. Every other body is provider authored and may
        carry mail content, so it is reduced to its status token.
        """
        if status == 0:
            error = body.get("error") if isinstance(body, dict) else None
            if error == EGRESS_REFUSAL_ERROR:
                return Failure("egress_refused", EGRESS_REFUSAL_ERROR)
            if error == OVERSIZE_ERROR:
                return Failure("too_large", OVERSIZE_ERROR)
            return Failure("unreachable", self._local_cause(error))
        cause = f"http_{status}" if 100 <= status <= 599 else "http_invalid"
        if status == 429:
            return Failure("rate_limited", cause)
        if 400 <= status < 500:
            return Failure("refused", cause)
        # A 5xx, and anything else that is not an answer, neither arms nor
        # clears a poll backoff (the adapter's existing rule for a 5xx).
        return Failure("server_error", cause)

    def _local_cause(self, error: Any) -> str:
        """A locally written error string, redacted of the API key and bounded."""
        if not isinstance(error, str) or not error.strip():
            return "unavailable"
        cause = " ".join(error.split())
        if self.config.agentmail_api_key:
            cause = cause.replace(self.config.agentmail_api_key, "[redacted]")
        if len(cause) > FAILURE_CAUSE_MAX_CHARS:
            cause = cause[:FAILURE_CAUSE_MAX_CHARS] + "..."
        return cause


def _labels(message: dict[str, Any]) -> list[str]:
    return [str(label).strip().lower() for label in (message.get("labels") or [])]


def _mailboxes(value: Any) -> tuple[str, ...]:
    """Every mailbox in an address field, as listed, one string each.

    AgentMail serves addresses "In format `username@domain.com` or `Display Name
    <username@domain.com>`", as a string or a list of them.
    """
    entries = value if isinstance(value, list) else [value]
    found: list[str] = []
    for name, address in email.utils.getaddresses([str(entry) for entry in entries if entry]):
        if address:
            found.append(email.utils.formataddr((name, address)) if name else address)
    return tuple(found)


def _text_or_none(value: Any) -> str | None:
    return value if isinstance(value, str) else None

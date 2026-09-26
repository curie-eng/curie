"""The AgentMail REST client: the adapter's only outbound provider surface.

Four calls, all stdlib ``urllib``. Every method returns ``(status, parsed)`` and
never raises for a transport failure, which is reported as status ``0`` so the
caller can tell "the provider said no" from "the provider was unreachable".
Bearer auth is on every call (https://docs.agentmail.to/api-reference/overview).
"""

from __future__ import annotations

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
    """The provider seam. Holds the config; owns no state of its own."""

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

    def reply(self, message_id: str, text: str) -> tuple[int, Any]:
        return self._call(
            "POST",
            f"/inboxes/{self._inbox}/messages/{_quoted(message_id)}/reply",
            {"text": text},
        )

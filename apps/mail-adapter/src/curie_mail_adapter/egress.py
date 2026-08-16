"""The neutral reply wire: an HTTP server the platform posts turn events to.

Four events (`turn.status`, `reply.update`, `reply.post`, `turn.completed`) and
one health path. The platform is authenticated on `X-Curie-Adapter-Secret`
BEFORE a body is read or any state is touched: anyone who can reach this Service
could otherwise forge a completion and make the adapter send an arbitrary email.

The ack status is the platform's retry signal, so it is not always 200. A
`turn.completed` whose provider send failed acks 502 (a delivery failure the
platform retries and eventually dead-letters), and one whose outcome is not yet
known because a concurrent duplicate is mid-send acks 503 (come back later).
Acking 200 in either case would make the worker clear its durable completion
record and lose the email.
"""

from __future__ import annotations

import hmac
import json
import logging
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, cast

from .adapter import MailAdapter

logger = logging.getLogger(__name__)

ADAPTER_SECRET_HEADER = "X-Curie-Adapter-Secret"
HEALTH_PATH = "/healthz"


class EgressServer(ThreadingHTTPServer):
    """A `ThreadingHTTPServer` that carries the adapter its handler serves."""

    daemon_threads = True

    def __init__(
        self,
        server_address: tuple[str, int],
        handler_class: type[BaseHTTPRequestHandler],
        adapter: MailAdapter,
    ) -> None:
        self.adapter = adapter
        super().__init__(server_address, handler_class)


class EgressHandler(BaseHTTPRequestHandler):
    """One request. Authenticate, parse, dispatch, ack."""

    protocol_version = "HTTP/1.0"

    def log_message(self, format: str, *args: Any) -> None:
        """Silence the stdlib access log; this package logs through `logging`."""

    @property
    def adapter(self) -> MailAdapter:
        return cast(EgressServer, self.server).adapter

    def _respond(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload).encode()
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except OSError as exc:
            logger.warning("writing the response failed: %r", exc)

    def do_GET(self) -> None:
        """The liveness probe, and nothing else.

        The body is fixed and reveals nothing about the install: no config, no
        counts, no inbox address, no version.
        """
        if urllib.parse.urlparse(self.path).path == HEALTH_PATH:
            return self._respond(200, {"status": "ok"})
        self._respond(404, {"detail": "not found"})

    def do_POST(self) -> None:
        # Authenticate BEFORE reading a body or touching any state, and refuse
        # outright when no secret is configured, so an empty configured secret
        # can never become "any presented secret matches". The health path is
        # deliberately not special-cased: a probe endpoint must not become an
        # unauthenticated write.
        secret = self.adapter.config.egress_secret
        presented = (self.headers.get(ADAPTER_SECRET_HEADER) or "").encode("utf-8", "replace")
        if not secret or not hmac.compare_digest(presented, secret.encode()):
            logger.warning("%s 401: missing or invalid %s", self.path, ADAPTER_SECRET_HEADER)
            return self._respond(401, {"detail": "missing or invalid credential"})
        try:
            length = int(self.headers.get("Content-Length") or 0)
            event = json.loads(self.rfile.read(length).decode() or "{}")
        except (OSError, ValueError) as exc:
            logger.warning("malformed body: %r", exc)
            return self._respond(400, {"detail": "malformed body"})
        try:
            status = self.dispatch(event)
        except Exception:
            logger.exception("dispatching %s failed", event.get("event"))
            return self._respond(500, {"detail": "adapter error"})
        # Email has no editable handle to hand back, so it mints no `ref`.
        self._respond(status, {})

    def dispatch(self, event: dict[str, Any]) -> int:
        """Apply one neutral reply event. Returns the status to ack with."""
        name = event.get("event")
        target = event.get("target") or {}
        conversation_id = str(target.get("conversation_id") or "")
        if name == "reply.update":
            text = event.get("text") or (event.get("message") or {}).get("text")
            self.adapter.record_text(conversation_id, text)
        elif name == "reply.post":
            text = (event.get("message") or {}).get("text")
            self.adapter.record_text(conversation_id, text, append=True)
        elif name == "turn.completed":
            reply_ref = target.get("reply_ref")
            return self.adapter.send_reply(
                str(event.get("event_id") or ""),
                conversation_id,
                str(reply_ref) if reply_ref else None,
            )
        return 200


def make_server(adapter: MailAdapter, port: int) -> ThreadingHTTPServer:
    """Bind the egress server. Port 0 asks the OS for an ephemeral one."""
    return EgressServer(("0.0.0.0", port), EgressHandler, adapter)

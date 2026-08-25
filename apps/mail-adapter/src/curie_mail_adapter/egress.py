"""Authenticated, typed, and bounded HTTP receiver for channel reply events."""

from __future__ import annotations

import hmac
import json
import logging
import socket
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, cast

from channel_protocol import ReplyAck, ReplyEvent, ReplyPost, ReplyUpdate, TurnCompleted
from pydantic import TypeAdapter, ValidationError

from .adapter import CHANNEL_KIND, MailAdapter

logger = logging.getLogger(__name__)

ADAPTER_SECRET_HEADER = "X-Curie-Adapter-Secret"
HEALTH_PATH = "/healthz"
READY_PATH = "/readyz"
MAX_CONCURRENT_REQUESTS = 16
REJECT_DRAIN_SECONDS = 1.0
REQUEST_HEADER_SECONDS = 2.0
_REPLY_EVENT_ADAPTER: TypeAdapter[ReplyEvent] = TypeAdapter(ReplyEvent)


class EgressServer(ThreadingHTTPServer):
    """A bounded ``ThreadingHTTPServer`` carrying the adapter it serves."""

    daemon_threads = False

    def __init__(
        self,
        server_address: tuple[str, int],
        handler_class: type[BaseHTTPRequestHandler],
        adapter: MailAdapter,
    ) -> None:
        self.adapter = adapter
        self.request_slots = threading.BoundedSemaphore(MAX_CONCURRENT_REQUESTS)
        super().__init__(server_address, handler_class)

    def process_request(self, request: Any, client_address: Any) -> None:
        """Bound work before ``ThreadingMixIn`` allocates a handler thread."""
        if not self.request_slots.acquire(blocking=False):
            body = b'{"detail":"request concurrency limit reached"}'
            response = (
                b"HTTP/1.0 503 Service Unavailable\r\n"
                b"Content-Type: application/json\r\n"
                + f"Content-Length: {len(body)}\r\n\r\n".encode()
                + body
            )
            try:
                request.sendall(response)
            finally:
                self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except BaseException:
            self.request_slots.release()
            raise

    def process_request_thread(self, request: Any, client_address: Any) -> None:
        try:
            super().process_request_thread(request, client_address)
        finally:
            self.request_slots.release()


class EgressHandler(BaseHTTPRequestHandler):
    """Authenticate before reading, then validate one frozen reply-wire event."""

    protocol_version = "HTTP/1.0"

    def handle(self) -> None:
        """Close an incomplete request-line/header block at an absolute deadline."""
        self._headers_complete = threading.Event()
        self._header_timer = threading.Timer(REQUEST_HEADER_SECONDS, self._expire_headers)
        self._header_timer.daemon = True
        self._header_timer.start()
        try:
            super().handle()
        finally:
            self._headers_complete.set()
            self._header_timer.cancel()

    def parse_request(self) -> bool:
        try:
            return super().parse_request()
        finally:
            self._headers_complete.set()
            self._header_timer.cancel()

    def _expire_headers(self) -> None:
        if self._headers_complete.is_set():
            return
        try:
            self.connection.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass

    def log_message(self, format: str, *args: Any) -> None:
        """Silence the stdlib access log; this package logs through ``logging``."""

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
        except OSError:
            logger.warning("writing the response failed")

    def _drain_rejected_body(self, length: int) -> None:
        """Bound unread bytes so closing the socket does not erase a sent 413."""
        remaining = length
        deadline = time.monotonic() + REJECT_DRAIN_SECONDS
        try:
            while remaining > 0:
                timeout = deadline - time.monotonic()
                if timeout <= 0:
                    return
                self.connection.settimeout(timeout)
                chunk = self.rfile.read1(min(remaining, 65_536))
                if not chunk:
                    return
                remaining -= len(chunk)
        except OSError:
            return

    def do_GET(self) -> None:
        path = urllib.parse.urlparse(self.path).path
        if path == HEALTH_PATH:
            return self._respond(200, {"status": "ok"})
        if path == READY_PATH:
            ready = self.adapter.ready.is_set() and self.adapter.state.healthy()
            return self._respond(
                200 if ready else 503,
                {"status": "ready" if ready else "starting"},
            )
        self._respond(404, {"detail": "not found"})

    def do_POST(self) -> None:
        secret = self.adapter.config.egress_secret
        presented = (self.headers.get(ADAPTER_SECRET_HEADER) or "").encode("utf-8", "replace")
        if not secret or not hmac.compare_digest(presented, secret.encode()):
            logger.warning("request refused: invalid adapter credential")
            return self._respond(401, {"detail": "missing or invalid credential"})
        if self.headers.get("Transfer-Encoding"):
            return self._respond(400, {"detail": "transfer encoding is not supported"})
        content_length = self.headers.get("Content-Length")
        if content_length is None:
            return self._respond(411, {"detail": "Content-Length is required"})
        try:
            length = int(content_length)
        except ValueError:
            return self._respond(400, {"detail": "invalid Content-Length"})
        if length <= 0:
            return self._respond(400, {"detail": "request body is required"})
        if length > self.adapter.config.max_reply_bytes + 65_536:
            self._respond(413, {"detail": "request body is too large"})
            self._drain_rejected_body(length)
            return
        content_type = (
            (self.headers.get("Content-Type") or "").partition(";")[0].strip().lower()
        )
        if content_type != "application/json":
            return self._respond(415, {"detail": "Content-Type must be application/json"})
        try:
            raw = self.rfile.read(length)
            if len(raw) != length:
                raise ValueError("truncated body")
            parsed = json.loads(raw.decode("utf-8"))
            if not isinstance(parsed, dict):
                raise ValueError("event must be a JSON object")
            event = _REPLY_EVENT_ADAPTER.validate_python(parsed)
        except (OSError, UnicodeDecodeError, ValueError, ValidationError):
            logger.warning("invalid reply event")
            return self._respond(400, {"detail": "invalid reply event"})
        if (
            event.target.kind != CHANNEL_KIND
            or event.target.address != self.adapter.config.agentmail_inbox
        ):
            return self._respond(
                422,
                {"detail": "event target does not belong to this adapter"},
            )
        if isinstance(event, TurnCompleted) and not event.event_id:
            return self._respond(422, {"detail": "event_id must not be empty"})
        try:
            status = self.dispatch(event)
        except Exception:
            logger.error("dispatching event type=%s failed unexpectedly", event.event)
            return self._respond(500, {"detail": "adapter error"})
        self._respond(status, ReplyAck().model_dump())

    def dispatch(self, event: ReplyEvent) -> int:
        """Apply one validated neutral reply event."""
        conversation_id = event.target.conversation_id or ""
        if isinstance(event, ReplyUpdate):
            text = event.text or (event.message.text if event.message else None)
            return self.adapter.record_text(
                conversation_id,
                event.target.reply_ref,
                text,
            )
        if isinstance(event, ReplyPost):
            return self.adapter.record_text(
                conversation_id,
                event.target.reply_ref,
                event.message.text,
                append=True,
            )
        if isinstance(event, TurnCompleted):
            return self.adapter.send_reply(
                event.event_id,
                conversation_id,
                event.target.reply_ref,
            )
        return 200


def make_server(adapter: MailAdapter, port: int) -> ThreadingHTTPServer:
    """Bind the egress server. Port 0 asks the OS for an ephemeral one."""
    return EgressServer(("0.0.0.0", port), EgressHandler, adapter)

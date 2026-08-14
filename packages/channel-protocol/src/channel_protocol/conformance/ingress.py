"""A stand in for the platform's ingress, with the platform's real semantics.

This is PART OF THE KIT, not a mock of it. The ingress floor rules are decided
against what this server answers, so anything it answers differently from
``apps/api/src/curie_api/routers/channels.py`` is a rule decided against a
fiction. The three semantics that matter, and why each one is here:

* **The claim is keyed on the binding plus the delivery id, never the delivery
  id alone.** The real API derives an ``event_id`` from the pair, so two inboxes
  sharing an upstream id space cannot swallow each other's turns. Key on the id
  alone and an author testing two addresses reads a false duplicate.
* **An in flight claim answers 202 with a null ``stream_id``**, never the
  ``pending:`` sentinel the API keeps internally. Answering the sentinel would
  teach an author to parse a value production never sends.
* **Minting is platform key only.** The adapter holds no platform key, so a mint
  it attempts must be refused, exactly as production refuses it. A kit whose
  fake handed out a token on request would certify the trust boundary breach it
  exists to prevent: an adapter that can mint defeats both the token TTL and the
  binding generation.

Every request is recorded with the ``delivery_id`` that was ON THE WIRE. That
is the whole correlation mechanism: a post is attributed to a stimulus by
matching the declared ``UpstreamIdentity``, never by in process coordination, so
an adapter in another process or another language is decidable on the same
evidence.

The server binds 127.0.0.1 on an ephemeral port and is torn down by ``stop``.
"""

from __future__ import annotations

import hashlib
import json
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, cast

from pydantic import BaseModel, ConfigDict

TURNS_PATH = "/channels/turns"
TOKEN_PATH = "/channels/token"

_API_KEY_HEADER = "X-API-Key"


class ObservedRequest(BaseModel):
    """One request this ingress answered, in the terms the floor reasons in.

    Deliberately NOT the headers or the body: a recording is quoted into
    reports and issues, and both carry credentials. The ``delivery_id`` is the
    only body field any rule correlates on.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    path: str
    delivery_id: str | None
    status: int
    at: float


class _IngressServer(ThreadingHTTPServer):
    daemon_threads = True
    ingress: FakeIngress


class _IngressHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "curie-conformance-ingress/1"

    def log_message(self, fmt: str, *args: Any) -> None:
        return

    def do_POST(self) -> None:
        ingress = cast(_IngressServer, self.server).ingress
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else b""
        route = self.path.split("?", 1)[0]
        api_key = self.headers.get(_API_KEY_HEADER)
        if route == TURNS_PATH:
            status, payload = ingress.answer_turn(api_key, body)
        elif route == TOKEN_PATH:
            status, payload = ingress.answer_mint(api_key)
        else:
            status, payload = 404, {"detail": "no such route"}
        encoded = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)


class FakeIngress:
    """The platform's ingress, reduced to what the floor rules can observe.

    ``kind`` and ``address`` name the binding this stands in for. They are
    recorded and they form half the claim key; they are deliberately NOT
    enforced, because no floor rule turns on the API's 404 for an unbound pair
    and enforcing it would only add a way for a run to die early.
    """

    def __init__(self, *, kind: str, address: str) -> None:
        self.kind = kind
        self.address = address
        self.platform_key = f"platform_key_{uuid.uuid4().hex}"
        self.token = _new_token()
        self._valid_tokens = {self.token}
        self._claims: dict[tuple[str, str, str], str] = {}
        self._records: list[ObservedRequest] = []
        self._pending_202 = False
        self._lock = threading.Lock()
        self._server: _IngressServer | None = None
        self._thread: threading.Thread | None = None
        self._port = 0

    # -- lifecycle ---------------------------------------------------------

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self._port}"

    def start(self) -> None:
        server = _IngressServer(("127.0.0.1", 0), _IngressHandler)
        server.ingress = self
        self._port = int(server.server_address[1])
        self._server = server
        self._thread = threading.Thread(target=server.serve_forever, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        server, self._server = self._server, None
        thread, self._thread = self._thread, None
        if server is not None:
            server.shutdown()
            server.server_close()
        if thread is not None:
            thread.join(timeout=5)

    # -- arming ------------------------------------------------------------

    def arm_401(self) -> None:
        """Invalidate every outstanding token and issue a replacement.

        This is what a rebind does in production: every binding write bumps the
        generation unconditionally, so an adapter holding the old token starts
        getting 401 and an operator has to hand it a new one. Rule 7 is about
        what the adapter does in that window, so the fake reproduces the cause
        rather than a global "answer 401 to everything" switch.
        """

        with self._lock:
            self._valid_tokens.clear()
            self.token = _new_token()
            self._valid_tokens.add(self.token)

    def arm_202(self) -> None:
        """Answer the NEXT turn post as an in flight claim.

        One shot, and provoked rather than raced: rule 2 needs a 202 to exist,
        and waiting for a real concurrent claim race would make the rule flaky
        without making it stronger.
        """

        with self._lock:
            self._pending_202 = True

    # -- observation -------------------------------------------------------

    def records(self) -> tuple[ObservedRequest, ...]:
        with self._lock:
            return tuple(self._records)

    def mint_attempts(self) -> int:
        """How many times the adapter tried to mint its own token. Must be zero."""

        with self._lock:
            return sum(1 for record in self._records if record.path == TOKEN_PATH)

    # -- the two routes ----------------------------------------------------

    def answer_turn(self, api_key: str | None, body: bytes) -> tuple[int, dict[str, Any]]:
        delivery_id, kind, address = _delivery_fields(body)
        if api_key is None or api_key not in self._valid_tokens:
            self._record(TURNS_PATH, delivery_id, 401)
            return 401, {"detail": "missing, malformed, expired or stale credential"}
        event_id = _event_id(kind, address, delivery_id)
        with self._lock:
            if self._pending_202:
                self._pending_202 = False
                status: int = 202
                payload: dict[str, Any] = {
                    "event_id": event_id,
                    "stream_id": None,
                    "duplicate": True,
                }
            else:
                key = (kind, address, delivery_id or "")
                claimed = self._claims.get(key)
                if claimed is None:
                    claimed = f"{int(time.time() * 1000)}-{len(self._claims)}"
                    self._claims[key] = claimed
                    status, payload = 200, {
                        "event_id": event_id,
                        "stream_id": claimed,
                        "duplicate": False,
                    }
                else:
                    status, payload = 200, {
                        "event_id": event_id,
                        "stream_id": claimed,
                        "duplicate": True,
                    }
            self._records.append(
                ObservedRequest(
                    path=TURNS_PATH,
                    delivery_id=delivery_id,
                    status=status,
                    at=time.monotonic(),
                )
            )
        return status, payload

    def answer_mint(self, api_key: str | None) -> tuple[int, dict[str, Any]]:
        if api_key != self.platform_key:
            self._record(TOKEN_PATH, None, 401)
            return 401, {"detail": "minting a channel token requires the platform key"}
        token = _new_token()
        with self._lock:
            self._valid_tokens.add(token)
        self._record(TOKEN_PATH, None, 200)
        return 200, {"token": token}

    def _record(self, path: str, delivery_id: str | None, status: int) -> None:
        with self._lock:
            self._records.append(
                ObservedRequest(
                    path=path,
                    delivery_id=delivery_id,
                    status=status,
                    at=time.monotonic(),
                )
            )


def _new_token() -> str:
    return f"chn_{uuid.uuid4().hex}"


def _event_id(kind: str, address: str, delivery_id: str | None) -> str:
    """The claim identity, over the BINDING plus the delivery id.

    Never the delivery id alone: two adapters sharing an upstream id space would
    otherwise swallow each other's turns.
    """

    digest = hashlib.sha256(
        "\x00".join((kind, address, delivery_id or "")).encode()
    ).hexdigest()
    return f"evt_{digest[:32]}"


def _delivery_fields(body: bytes) -> tuple[str | None, str, str]:
    try:
        decoded = json.loads(body) if body else None
    except ValueError:
        decoded = None
    if not isinstance(decoded, dict):
        return None, "", ""
    raw = decoded.get("delivery_id")
    delivery_id = str(raw) if isinstance(raw, str) and raw else None
    return delivery_id, str(decoded.get("kind", "")), str(decoded.get("address", ""))

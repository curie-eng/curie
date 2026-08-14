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

**Every request is recorded, including the ones this server refuses to read.**
The adapter under test writes its own request headers, and two floor clauses
(rule 2's finality check and clause 7b's mint check) read the ABSENCE of a
record as conformance. So a request whose framing this server rejects is
recorded as unreadable rather than dropped: dropping it would let a hostile or
merely broken adapter suppress its own duplicate post, or its own attempt to
mint a platform credential, and certify clean on the trust boundary this kit
exists to enforce.

The server binds 127.0.0.1 on an ephemeral port and is torn down by ``stop``.
"""

from __future__ import annotations

import hashlib
import json
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, NamedTuple, cast

from pydantic import BaseModel, ConfigDict

TURNS_PATH = "/channels/turns"
TOKEN_PATH = "/channels/token"

# The most turn body this ingress will read, and the bound the real API puts on
# the same route. RE DECLARED here, never imported: ``channel_protocol`` is a
# contract package and ``curie_api`` imports IT, so importing back would invert
# the dependency. The copy is kept honest by
# ``tests/test_conformance_egress.py::test_platform_constants_match``, which
# reads the value back out of the API's settings and fails on any drift.
# Authoritative site: apps/api/src/curie_api/config.py, channel_turn_max_body_bytes.
MAX_TURN_BODY_BYTES = 256 * 1024

# How long one connection may hold a handler thread without making progress.
# The adapter under test is untrusted by construction, so a connection that
# declares a body and then goes quiet must not pin a thread for the whole run.
_HANDLER_TIMEOUT_S = 10.0

# The recorded status of a request this ingress deliberately did not answer.
# Not an HTTP status at all, because none of them mean "no response was sent":
# a transport failure is precisely the absence of one, and rule 1 turns on the
# difference between a failure the adapter must retry and a response it must
# treat as final.
NO_RESPONSE = 0

_API_KEY_HEADER = "X-API-Key"


class ObservedRequest(BaseModel):
    """One request this ingress answered, in the terms the floor reasons in.

    Deliberately NOT the headers or the body: a recording is quoted into
    reports and issues, and both carry credentials. The ``delivery_id`` is the
    only body field any rule correlates on.

    ``framing_error`` names why a request could not be read at all. It is the
    difference between "the adapter did not post" and "the adapter posted
    something this server could not attribute", and no floor rule is allowed to
    confuse the two.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    path: str
    delivery_id: str | None
    status: int
    at: float
    framing_error: str | None


class _Framing(NamedTuple):
    """A request body, or the reason it could not be framed and the refusal."""

    body: bytes
    error: str | None
    status: int


class _IngressServer(ThreadingHTTPServer):
    daemon_threads = True
    ingress: FakeIngress


class _IngressHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "curie-conformance-ingress/1"
    # ``socketserver`` applies this to the accepted connection, so a peer that
    # declares a body and never sends it cannot hold a handler thread for the
    # life of the run.
    timeout = _HANDLER_TIMEOUT_S

    def log_message(self, fmt: str, *args: Any) -> None:
        return

    def do_POST(self) -> None:
        ingress = cast(_IngressServer, self.server).ingress
        route = self.path.split("?", 1)[0]
        try:
            self._serve_post(ingress, route)
        except Exception as error:  # noqa: BLE001
            # Nothing this handler can raise is allowed to cost the kit an
            # observation. Every ingress rule decides on the presence or absence
            # of a record, and the adapter under test controls the request, so a
            # handler that raised its way out without recording would hand the
            # adapter a switch for what the kit is able to see.
            ingress.record_unframeable(
                route, f"the ingress handler failed ({type(error).__name__})", status=500
            )
            self.close_connection = True
            self._answer(500, {"detail": "the ingress could not read that request"})

    def _serve_post(self, ingress: FakeIngress, route: str) -> None:
        framing = self._frame_body()
        if framing.error is not None:
            ingress.record_unframeable(route, framing.error, status=framing.status)
            # The stream is out of sync with the declared framing, so anything
            # still on it would be read as the next request.
            self.close_connection = True
            self._answer(framing.status, {"detail": framing.error})
            return
        api_key = self.headers.get(_API_KEY_HEADER)
        if route == TURNS_PATH:
            status, payload = ingress.answer_turn(api_key, framing.body)
        elif route == TOKEN_PATH:
            status, payload = ingress.answer_mint(api_key)
        else:
            status, payload = 404, {"detail": "no such route"}
        if status == NO_RESPONSE:
            # The blackhole: the request was read and recorded, and the
            # connection closes with nothing on it. That is a transport failure
            # from the adapter's side, not a response, which is the only kind of
            # failure rule 1 licenses a retry for.
            self.close_connection = True
            return
        self._answer(status, payload)

    def _frame_body(self) -> _Framing:
        """The request body, read under the turn body cap.

        An exact ``Content-Length`` inside the cap is the only framing this
        server accepts. It cannot decode a chunked body, so tolerating one (or a
        missing, unparseable, negative or oversized length) would mean answering
        on a body it never read, which is the same evidence loss as dropping the
        request. Everything else is refused, recorded, and named.
        """

        if self.headers.get("Transfer-Encoding"):
            return _Framing(
                b"",
                "the request declared a Transfer-Encoding, and this ingress reads "
                "only an exact Content-Length",
                400,
            )
        declared = self.headers.get("Content-Length")
        if declared is None:
            return _Framing(b"", "the request declared no Content-Length", 400)
        try:
            length = int(declared)
        except ValueError:
            return _Framing(b"", "the request declared an unparseable Content-Length", 400)
        if length < 0:
            return _Framing(b"", "the request declared a negative Content-Length", 400)
        if length > MAX_TURN_BODY_BYTES:
            # 413 rather than 400, because this is the one framing refusal the
            # real API also makes, and it makes it with this status.
            return _Framing(
                b"",
                f"the request declared a Content-Length over the "
                f"{MAX_TURN_BODY_BYTES} byte turn body cap",
                413,
            )
        body = self.rfile.read(length) if length else b""
        if len(body) != length:
            return _Framing(
                b"", "the request sent fewer bytes than its Content-Length declared", 400
            )
        return _Framing(body, None, 0)

    def _answer(self, status: int, payload: dict[str, Any]) -> None:
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
        self._pending_202: str | None = None
        self._blackholed = False
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

    def arm_202(self, delivery_id: str) -> None:
        """Answer the next post of THIS delivery id as an in flight claim.

        Scoped to one identity, never to "the next post that arrives": rule 2
        asks whether the adapter treated the response to ITS declared delivery
        as final, so a one shot any other delivery could consume would let the
        rule pass with the declared delivery never having seen a 202 at all.
        Provoked rather than raced, because waiting for a real concurrent claim
        race would make the rule flaky without making it stronger.
        """

        with self._lock:
            self._pending_202 = delivery_id

    def armed_202(self) -> str | None:
        """The delivery id still waiting for its 202, if it never arrived."""

        with self._lock:
            return self._pending_202

    def arm_blackhole(self) -> None:
        """Read and record every turn post, and answer none of them.

        This is how the kit takes the transport down ITSELF, rather than asking
        the driver to take it down somewhere the kit cannot see. Rule 1 has to
        establish that the declared delivery actually met an unavailable
        transport before a retry can mean anything, and an outage the driver
        arranges privately leaves the kit sleeping for a fixed window and
        calling whatever arrives afterwards a retry. Here the failed attempt is
        on the wire, with its delivery id, like every other piece of evidence
        this kit decides on.
        """

        with self._lock:
            self._blackholed = True

    def disarm_blackhole(self) -> None:
        with self._lock:
            self._blackholed = False

    # -- observation -------------------------------------------------------

    def records(self) -> tuple[ObservedRequest, ...]:
        with self._lock:
            return tuple(self._records)

    def mint_attempts(self) -> int:
        """How many times the adapter tried to mint its own token. Must be zero.

        Counted over EVERY record on the mint route, including requests this
        server refused to read. A mint attempt the ingress could not frame is
        still a mint attempt, and clause 7b is the trust boundary clause: an
        attempt that went uncounted would certify an adapter that mints its own
        platform credential.
        """

        with self._lock:
            return sum(1 for record in self._records if record.path == TOKEN_PATH)

    def record_unframeable(self, path: str, reason: str, *, status: int) -> None:
        """Record a request this server could not read, and why.

        Recorded rather than dropped, and that is the entire point: the floor
        reads the absence of a record as conformance in two places, and the
        adapter under test writes its own request headers. An unreadable request
        is not an absent one.
        """

        self._record(path, None, status, framing_error=reason)

    # -- the two routes ----------------------------------------------------

    def answer_turn(self, api_key: str | None, body: bytes) -> tuple[int, dict[str, Any]]:
        delivery_id, kind, address = _delivery_fields(body)
        with self._lock:
            blackholed = self._blackholed
        if blackholed:
            # Ahead of the credential check, because a transport that is down is
            # down for an authorized request too, and an adapter that saw a 401
            # here would be exercising rule 7 rather than rule 1.
            self._record(TURNS_PATH, delivery_id, NO_RESPONSE, framing_error=None)
            return NO_RESPONSE, {}
        if api_key is None or api_key not in self._valid_tokens:
            self._record(TURNS_PATH, delivery_id, 401, framing_error=None)
            return 401, {"detail": "missing, malformed, expired or stale credential"}
        event_id = _event_id(kind, address, delivery_id)
        with self._lock:
            if delivery_id is not None and self._pending_202 == delivery_id:
                self._pending_202 = None
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
                    framing_error=None,
                )
            )
        return status, payload

    def answer_mint(self, api_key: str | None) -> tuple[int, dict[str, Any]]:
        if api_key != self.platform_key:
            self._record(TOKEN_PATH, None, 401, framing_error=None)
            return 401, {"detail": "minting a channel token requires the platform key"}
        token = _new_token()
        with self._lock:
            self._valid_tokens.add(token)
        self._record(TOKEN_PATH, None, 200, framing_error=None)
        return 200, {"token": token}

    def _record(
        self,
        path: str,
        delivery_id: str | None,
        status: int,
        *,
        framing_error: str | None,
    ) -> None:
        with self._lock:
            self._records.append(
                ObservedRequest(
                    path=path,
                    delivery_id=delivery_id,
                    status=status,
                    at=time.monotonic(),
                    framing_error=framing_error,
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

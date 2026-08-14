"""Real HTTP adapter stubs, and the ingress drivers over them (#1516).

These are TEST FIXTURES. They are never packaged, they are not adapters anyone
deploys, and they do not collide with the first party adapter directory #1515
owns. They exist to be the conformance kit's NEGATIVE CONTROL: for every
automatable floor clause there is a stub that breaks exactly that clause and
nothing else, so a kit that quietly asserts nothing goes green on the conformant
stub and stays green on the break, which reds the suite.

Everything here is a real HTTP server on 127.0.0.1 on an EPHEMERAL port, driven
over real loopback HTTP. Nothing internal is faked: an in process shortcut would
defeat the only thing a black box kit is for.

The stub deliberately re declares ``X-Curie-Adapter-Secret`` rather than
importing the kit's constant. A stub that read the header name out of the thing
under test could not witness a drift in it.
"""

from __future__ import annotations

import dataclasses
import hmac
import http.client
import json
import os
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import Callable, Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import EllipsisType
from typing import Any

from channel_protocol.conformance import ClauseResult, FloorReport, FloorResult, UpstreamIdentity

# The egress secret header, as an adapter author reads it out of the guide. Not
# imported from the kit on purpose: see the module docstring.
SECRET_HEADER = "X-Curie-Adapter-Secret"

# The stub retries a TRANSPORT failure for about ten seconds, which is long
# enough for the kit to restore a path it took down and short enough that a
# hung run still ends.
_MAX_ATTEMPTS = 200
_RETRY_DELAY = 0.05

# The keys each reply event models, so the stub can report an UNMODELLED extra
# key back to the test that asked whether the kit probes tolerance for one.
_MODELLED_KEYS: dict[str, frozenset[str]] = {
    "turn.status": frozenset({"version", "target", "event", "status"}),
    "reply.update": frozenset(
        {"version", "target", "event", "text", "message", "settled", "nav"}
    ),
    "reply.post": frozenset({"version", "target", "event", "message", "requested_by"}),
    "turn.completed": frozenset({"version", "target", "event", "event_id", "outcome"}),
}

STUB_MAIN = Path(__file__).with_name("stub_adapter_main.py")


@dataclasses.dataclass(frozen=True)
class StubBehavior:
    """One adapter's behavior. Every default is the CONFORMANT choice."""

    # Rule 3 (egress secret verification).
    skips_secret_check: bool = False
    side_effect_then_401: bool = False
    accepts_when_secret_unset: bool = False
    # Rule 4 (ack shape).
    redirects: bool = False
    empty_ack_body: bool = False
    ack_body_bytes: int | None = None
    ack_chunked_bytes: int | None = None
    # Rule 5 (handles all four events).
    rejects_turn_status: bool = False
    # Rule 6 (dedupe on event_id).
    double_sends_duplicate: bool = False
    # Rule 1 (stable delivery_id).
    fresh_delivery_id_per_retry: bool = False
    # Rule 2 (an ingress response is final).
    retries_after_202: bool = False
    posts_an_unrelated_delivery: bool = False
    # Rule 7 (stale credential handling).
    exits_on_401: bool = False
    drops_on_401: bool = False
    self_mints_on_401: bool = False


CONFORMANT = StubBehavior()

# One named break per automatable clause, which is what makes the kit
# falsifiable. Adding a clause to the kit without adding a break here leaves
# that clause unproven.
BREAKS: dict[str, StubBehavior] = {
    "skips_secret_check": StubBehavior(skips_secret_check=True),
    "side_effect_then_401": StubBehavior(side_effect_then_401=True),
    "accepts_when_secret_unset": StubBehavior(accepts_when_secret_unset=True),
    "redirects": StubBehavior(redirects=True),
    "empty_ack_body": StubBehavior(empty_ack_body=True),
    "rejects_turn_status": StubBehavior(rejects_turn_status=True),
    "double_sends_duplicate": StubBehavior(double_sends_duplicate=True),
    "fresh_delivery_id_per_retry": StubBehavior(fresh_delivery_id_per_retry=True),
    "retries_after_202": StubBehavior(retries_after_202=True),
    "posts_an_unrelated_delivery": StubBehavior(posts_an_unrelated_delivery=True),
    "exits_on_401": StubBehavior(exits_on_401=True),
    "drops_on_401": StubBehavior(drops_on_401=True),
    "self_mints_on_401": StubBehavior(self_mints_on_401=True),
}


def free_port() -> int:
    """An ephemeral port nothing holds. Never a hardcoded one: this box runs
    parallel jobs and a fixed port collides."""

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def padded_ack(size: int) -> bytes:
    """A valid JSON ack of EXACTLY ``size`` bytes."""

    skeleton = b'{"ref": "stub-ref", "pad": ""}'
    padding = size - len(skeleton)
    if padding < 0:
        raise ValueError(f"an ack cannot be shorter than {len(skeleton)} bytes")
    return b'{"ref": "stub-ref", "pad": "' + b"a" * padding + b'"}'


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "curie-conformance-stub/1"

    @property
    def stub(self) -> StubAdapter:
        return self.server.stub  # type: ignore[attr-defined]

    def log_message(self, fmt: str, *args: Any) -> None:
        return

    def _json(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    @property
    def route(self) -> str:
        """The path with any query stripped.

        An endpoint may legitimately carry a query (it can even BE part of the
        credential), so routing on the raw path would 404 a real adapter URL.
        """

        return self.path.split("?", 1)[0]

    def do_GET(self) -> None:
        if self.route == "/_probe":
            self._json(200, self.stub.probe())
            return
        self._json(404, {"error": "not found"})

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else b""
        if self.route.startswith("/_control/"):
            self._control(self.route, body)
            return
        if self.route == "/curie":
            self._egress(body)
            return
        self._json(404, {"error": "not found"})

    def _control(self, path: str, body: bytes) -> None:
        stub = self.stub
        payload: dict[str, Any] = json.loads(body) if body else {}
        if path == "/_control/configure":
            stub.configure(ingress_url=payload["ingress_url"], token=payload["token"])
            self._json(200, {"ok": True})
            return
        if path == "/_control/stimulate":
            delivery_id = stub.inject(str(payload["upstream_id"]))
            self._json(200, {"delivery_id": delivery_id})
            return
        if path == "/_control/transport":
            stub.set_reachable(bool(payload["reachable"]))
            self._json(200, {"ok": True})
            return
        self._json(404, {"error": "not found"})

    def _egress(self, body: bytes) -> None:
        stub = self.stub
        behavior = stub.behavior
        secret = stub.secret
        supplied = self.headers.get(SECRET_HEADER)

        if secret is None and not behavior.accepts_when_secret_unset:
            # Floor clause 3b: an adapter whose OWN secret is unset refuses
            # everything rather than serving unauthenticated.
            self._json(503, {"error": "egress secret is unset"})
            return

        authorized = behavior.skips_secret_check or (
            secret is not None
            and supplied is not None
            and hmac.compare_digest(supplied, secret)
        )
        if secret is None and behavior.accepts_when_secret_unset:
            authorized = True
        if not authorized:
            if behavior.side_effect_then_401:
                # The break codex-7 named, and the reason clause 3a needs a
                # probe: the side effect happens, THEN the rejection. The
                # status is identical to the conformant one.
                stub.handle_event(body)
            self._json(401, {"error": "unauthorized"})
            return

        if stub.handle_event(body) == "reject":
            self._json(500, {"error": "this adapter refuses that event"})
            return
        self._ack()

    def _ack(self) -> None:
        behavior = self.stub.behavior
        if behavior.redirects:
            self.send_response(302)
            self.send_header("Location", "http://127.0.0.1:1/somewhere-else")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if behavior.ack_chunked_bytes is not None:
            self._chunked_ack(behavior.ack_chunked_bytes)
            return
        if behavior.empty_ack_body:
            body = b""
        elif behavior.ack_body_bytes is not None:
            body = padded_ack(behavior.ack_body_bytes)
        else:
            body = b'{"ref": "stub-ref"}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body:
            self.wfile.write(body)

    def _chunked_ack(self, size: int) -> None:
        """An oversize ack with NO content length, sent as small chunks.

        A kit that trusts ``Content-Length``, or that does one sized read,
        passes this. The worker's ``_read_capped`` is a loop for exactly this
        reason, so the kit has to be one too.
        """

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()
        first = b'{"ref": "x", "pad": "'
        self.wfile.write(f"{len(first):x}\r\n".encode() + first + b"\r\n")
        remaining = size - len(first) - 2
        block = b"a" * 8192
        while remaining > 0:
            piece = block[: min(len(block), remaining)]
            self.wfile.write(f"{len(piece):x}\r\n".encode() + piece + b"\r\n")
            remaining -= len(piece)
        tail = b'"}'
        self.wfile.write(f"{len(tail):x}\r\n".encode() + tail + b"\r\n")
        self.wfile.write(b"0\r\n\r\n")


class StubAdapter:
    """One stub adapter: an egress endpoint plus an ingress sender.

    ``state_path`` is what a restart survives on. A real adapter re reads its
    upstream inbox after a restart; the stub writes the equivalent to a file, so
    "resume the same delivery after an operator supplies a replacement token" is
    a property of the stub rather than an artifact of shared memory.
    """

    def __init__(
        self,
        behavior: StubBehavior = CONFORMANT,
        *,
        secret: str | None,
        state_path: Path,
        port: int | None = None,
        standalone: bool = False,
    ) -> None:
        self.behavior = behavior
        self.secret = secret
        self.state_path = state_path
        self.port = port if port is not None else free_port()
        self._standalone = standalone
        self._lock = threading.Lock()
        self._httpd: ThreadingHTTPServer | None = None
        self._serve_thread: threading.Thread | None = None
        self._sender_thread: threading.Thread | None = None
        self._stopping = threading.Event()
        self._pending: list[dict[str, str]] = []
        self._dead_url = f"http://127.0.0.1:{free_port()}"
        self.ingress_url: str | None = None
        self.token: str | None = None
        self.reachable = True
        self.side_effects = 0
        self.dropped = 0
        self.stale_credential = False
        self.held: list[dict[str, str]] = []
        self.delivered: list[str] = []
        self.seen_event_ids: list[str] = []
        self.events: list[dict[str, Any]] = []
        self._posted_unrelated = False

    # -- lifecycle ---------------------------------------------------------

    @property
    def endpoint(self) -> str:
        return f"http://127.0.0.1:{self.port}/curie"

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def start(self) -> None:
        self._load_state()
        self._stopping = threading.Event()
        httpd = ThreadingHTTPServer(("127.0.0.1", self.port), _Handler)
        httpd.daemon_threads = True
        httpd.stub = self  # type: ignore[attr-defined]
        self.port = int(httpd.server_address[1])
        self._httpd = httpd
        self._serve_thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        self._serve_thread.start()
        self._sender_thread = threading.Thread(target=self._send_loop, daemon=True)
        self._sender_thread.start()

    def stop(self) -> None:
        self._stopping.set()
        httpd, self._httpd = self._httpd, None
        if httpd is not None:
            httpd.shutdown()
            httpd.server_close()
        current = threading.current_thread()
        for thread in (self._serve_thread, self._sender_thread):
            # A thread never joins itself: the sender thread is what calls stop
            # when the adapter simulates dying on a 401.
            if thread is not None and thread is not current:
                thread.join(timeout=5)
        self._serve_thread = None
        self._sender_thread = None

    def restart(
        self,
        *,
        secret: str | None | EllipsisType = ...,
        token: str | None | EllipsisType = ...,
    ) -> None:
        """Restart on the SAME port, reloading only what was persisted."""

        ingress_url = self.ingress_url
        self.stop()
        if not isinstance(secret, EllipsisType):
            self.secret = secret
        if not isinstance(token, EllipsisType):
            self.token = token
        self._pending = []
        self.start()
        self.ingress_url = ingress_url

    def configure(self, *, ingress_url: str, token: str) -> None:
        self.ingress_url = ingress_url
        self.token = token
        self.reachable = True

    def set_reachable(self, reachable: bool) -> None:
        self.reachable = reachable

    # -- egress ------------------------------------------------------------

    def handle_event(self, body: bytes) -> str:
        """Record one reply event and perform its side effect. ``ok`` or ``reject``."""

        try:
            decoded = json.loads(body)
        except ValueError:
            decoded = None
        with self._lock:
            if not isinstance(decoded, dict):
                self.events.append({"event": "<unparseable>", "extra_keys": []})
                return "ok"
            name = str(decoded.get("event"))
            modelled = _MODELLED_KEYS.get(name, frozenset())
            self.events.append(
                {"event": name, "extra_keys": sorted(set(decoded) - modelled)}
            )
            if name == "turn.status" and self.behavior.rejects_turn_status:
                return "reject"
            if name == "turn.completed":
                event_id = str(decoded.get("event_id"))
                duplicate = event_id in self.seen_event_ids
                if not duplicate:
                    self.seen_event_ids.append(event_id)
                if not duplicate or self.behavior.double_sends_duplicate:
                    self.side_effects += 1
                self._persist_locked()
                return "ok"
            self.side_effects += 1
            self._persist_locked()
            return "ok"

    def probe(self) -> dict[str, Any]:
        with self._lock:
            return {
                "side_effects": self.side_effects,
                "dropped": self.dropped,
                "stale_credential": self.stale_credential,
                "held": len(self.held),
                "delivered": list(self.delivered),
                "seen_event_ids": list(self.seen_event_ids),
                "events": list(self.events),
            }

    # -- ingress -----------------------------------------------------------

    def inject(self, upstream_id: str) -> str:
        """Deliver one upstream message. Returns the delivery_id it will send.

        The derivation is documented and deterministic, which is what lets a
        driver DECLARE the identity without reading anything back.
        """

        delivery_id = f"dlv-{upstream_id}"
        with self._lock:
            self._pending.append(
                {
                    "delivery_id": delivery_id,
                    "conversation_id": f"conv-{upstream_id}",
                    "author": "someone@example.test",
                    "text": "hello",
                }
            )
        return delivery_id

    def _send_loop(self) -> None:
        while not self._stopping.is_set():
            with self._lock:
                item = self._pending.pop(0) if self._pending else None
            if item is None:
                time.sleep(0.01)
                continue
            self._deliver(item)

    def _deliver(self, item: dict[str, str]) -> None:
        base = item["delivery_id"]
        attempt = 0
        while attempt < _MAX_ATTEMPTS and not self._stopping.is_set():
            attempt += 1
            if attempt == 1 or not self.behavior.fresh_delivery_id_per_retry:
                delivery_id = base
            else:
                delivery_id = f"{base}-r{attempt}"
            status = self._post_turn(delivery_id, item)
            if status is None:
                time.sleep(_RETRY_DELAY)
                continue
            self._on_status(status, delivery_id, item)
            return
        with self._lock:
            self.dropped += 1
            self._persist_locked()

    def _on_status(self, status: int, delivery_id: str, item: dict[str, str]) -> None:
        if status == 401:
            self._on_stale_credential(item)
            return
        if 200 <= status < 300:
            with self._lock:
                self.delivered.append(delivery_id)
                self._persist_locked()
            if status == 202 and self.behavior.retries_after_202:
                # Rule 2's break: a 202 is a RESPONSE, so it is final. Posting
                # again after one is the defect.
                time.sleep(_RETRY_DELAY)
                self._post_turn(delivery_id, item)
            if self.behavior.posts_an_unrelated_delivery and not self._posted_unrelated:
                # A legitimate upstream redelivery under an identity the kit
                # never declared. It must not red rule 2.
                self._posted_unrelated = True
                unrelated = f"dlv-unrelated-{uuid.uuid4().hex[:8]}"
                self._post_turn(unrelated, dict(item, conversation_id="conv-unrelated"))
            return
        with self._lock:
            self.dropped += 1
            self._persist_locked()

    def _on_stale_credential(self, item: dict[str, str]) -> None:
        if self.behavior.exits_on_401:
            self._die()
            return
        if self.behavior.drops_on_401:
            with self._lock:
                self.dropped += 1
                self._persist_locked()
            return
        if self.behavior.self_mints_on_401:
            # The trust boundary breach RULING 2 forbids: the adapter holds no
            # platform key, so it must never try to mint its own replacement.
            self._attempt_mint()
        with self._lock:
            self.stale_credential = True
            self.held.append(item)
            self._persist_locked()

    def _die(self) -> None:
        if self._standalone:
            os._exit(3)
        # In process, the equivalent observable is the endpoint going away with
        # the in flight delivery unpersisted.
        self.stop()

    def _target(self) -> str | None:
        if self.ingress_url is None:
            return None
        return self.ingress_url if self.reachable else self._dead_url

    def _post_turn(self, delivery_id: str, item: dict[str, str]) -> int | None:
        target = self._target()
        if target is None:
            return None
        body = json.dumps(
            {
                "kind": os.environ.get("STUB_KIND", "email"),
                "address": os.environ.get("STUB_ADDRESS", "agent@example.test"),
                "delivery_id": delivery_id,
                "conversation_id": item["conversation_id"],
                "author": item["author"],
                "text": item["text"],
                "reply_ref": delivery_id,
            }
        ).encode()
        request = urllib.request.Request(
            f"{target}/channels/turns",
            data=body,
            headers={"Content-Type": "application/json", "X-API-Key": self.token or ""},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return int(response.status)
        except urllib.error.HTTPError as error:
            return int(error.code)
        except (urllib.error.URLError, OSError, http.client.HTTPException):
            return None

    def _attempt_mint(self) -> None:
        target = self._target()
        if target is None:
            return
        request = urllib.request.Request(
            f"{target}/channels/token",
            data=json.dumps({"kind": "email", "address": "agent@example.test"}).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=5):
                return
        except (urllib.error.URLError, OSError, http.client.HTTPException):
            return

    # -- persistence -------------------------------------------------------

    def _persist_locked(self) -> None:
        self.state_path.write_text(
            json.dumps(
                {
                    "side_effects": self.side_effects,
                    "dropped": self.dropped,
                    "stale_credential": self.stale_credential,
                    "held": self.held,
                    "delivered": self.delivered,
                    "seen_event_ids": self.seen_event_ids,
                    "events": self.events,
                }
            ),
            encoding="utf-8",
        )

    def _load_state(self) -> None:
        if not self.state_path.exists():
            return
        state = json.loads(self.state_path.read_text(encoding="utf-8"))
        self.side_effects = int(state.get("side_effects", 0))
        self.dropped = int(state.get("dropped", 0))
        self.stale_credential = bool(state.get("stale_credential", False))
        self.delivered = list(state.get("delivered", []))
        self.seen_event_ids = list(state.get("seen_event_ids", []))
        self.events = list(state.get("events", []))
        # A held delivery is what "did not silently drop it" means: it comes
        # back out of the queue as soon as the adapter is running again.
        self._pending = list(state.get("held", []))
        self.held = []
        self._persist_locked()


def conformant_stub(
    *,
    secret: str | None,
    state_path: Path,
    port: int | None = None,
    ack_body_bytes: int | None = None,
    ack_chunked_bytes: int | None = None,
) -> StubAdapter:
    """A stub that satisfies every floor rule the kit can assert."""

    behavior = dataclasses.replace(
        CONFORMANT, ack_body_bytes=ack_body_bytes, ack_chunked_bytes=ack_chunked_bytes
    )
    return StubAdapter(behavior, secret=secret, state_path=state_path, port=port)


def non_conformant_stub(
    break_name: str,
    *,
    secret: str | None,
    state_path: Path,
    port: int | None = None,
) -> StubAdapter:
    """A stub that breaks exactly one floor clause."""

    return StubAdapter(BREAKS[break_name], secret=secret, state_path=state_path, port=port)


def serve_from_env() -> None:
    """Run one stub as a standalone process, configured purely by environment."""

    secret = os.environ.get("STUB_SECRET")
    behavior_name = os.environ.get("STUB_BEHAVIOR", "conformant")
    behavior = CONFORMANT if behavior_name == "conformant" else BREAKS[behavior_name]
    stub = StubAdapter(
        behavior,
        secret=secret or None,
        state_path=Path(os.environ["STUB_STATE"]),
        port=int(os.environ["STUB_PORT"]),
        standalone=True,
    )
    stub.start()
    sys.stdout.write("ready\n")
    sys.stdout.flush()
    threading.Event().wait()


# --- ingress drivers ---------------------------------------------------------


class StubIngressDriver:
    """The IN PROCESS driver: it holds the stub OBJECT.

    That reference is the temptation FIX A exists to rule out. Anything the kit
    correlates through this object rather than through the wire passes here and
    fails under ``SubprocessIngressDriver``, which is the whole point of running
    every ingress assertion under both.
    """

    def __init__(self, stub: StubAdapter) -> None:
        self.stub = stub
        self._stimuli = 0

    def start(self, *, ingress_url: str, token: str) -> None:
        self.stub.configure(ingress_url=ingress_url, token=token)

    def stimulate(self) -> UpstreamIdentity:
        self._stimuli += 1
        upstream_id = uuid.uuid4().hex[:10]
        delivery_id = self.stub.inject(upstream_id)
        return UpstreamIdentity(
            stimulus_id=f"in-process-{self._stimuli}", delivery_id=delivery_id
        )

    def set_transport(self, *, reachable: bool) -> None:
        self.stub.set_reachable(reachable)

    def restart(
        self,
        *,
        egress_secret: str | None | EllipsisType = ...,
        token: str | None | EllipsisType = ...,
    ) -> None:
        self.stub.restart(secret=egress_secret, token=token)

    def stop(self) -> None:
        self.stub.stop()


class SubprocessIngressDriver:
    """The OUT OF PROCESS driver: a ``subprocess.Popen`` and an HTTP surface.

    It shares no memory with the kit or with the stub, so the only thing that
    can correlate an observed post to a stimulus is the ``delivery_id`` this
    driver declares and the adapter puts on the wire.
    """

    def __init__(
        self,
        *,
        port: int,
        secret: str | None,
        state_path: Path,
        behavior_name: str = "conformant",
    ) -> None:
        self.port = port
        self.secret = secret
        self.state_path = state_path
        self.behavior_name = behavior_name
        self._process: Any = None
        self._token: str | None = None
        self._ingress_url: str | None = None
        self._stimuli = 0

    @property
    def endpoint(self) -> str:
        return f"http://127.0.0.1:{self.port}/curie"

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def boot(self) -> None:
        """Start the adapter process without pointing it at an ingress yet.

        The egress rules run against a live endpoint before any ingress rule
        does, so the fixture boots the adapter and ``start`` only configures it.
        """

        if self._process is None:
            self._spawn()

    def _spawn(self) -> None:
        env = dict(os.environ)
        env.update(
            {
                "STUB_PORT": str(self.port),
                "STUB_BEHAVIOR": self.behavior_name,
                "STUB_STATE": str(self.state_path),
            }
        )
        if self.secret is None:
            env.pop("STUB_SECRET", None)
        else:
            env["STUB_SECRET"] = self.secret
        self._process = subprocess.Popen(
            [sys.executable, str(STUB_MAIN)],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        wait_until_ready(self.base_url)

    def _kill(self) -> None:
        process, self._process = self._process, None
        if process is None:
            return
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)

    def start(self, *, ingress_url: str, token: str) -> None:
        self._ingress_url = ingress_url
        self._token = token
        if self._process is None:
            self._spawn()
        control(self.base_url, "configure", {"ingress_url": ingress_url, "token": token})

    def stimulate(self) -> UpstreamIdentity:
        self._stimuli += 1
        upstream_id = uuid.uuid4().hex[:10]
        control(self.base_url, "stimulate", {"upstream_id": upstream_id})
        # DECLARED, not read back: the derivation is the documented contract.
        return UpstreamIdentity(
            stimulus_id=f"subprocess-{self._stimuli}", delivery_id=f"dlv-{upstream_id}"
        )

    def set_transport(self, *, reachable: bool) -> None:
        control(self.base_url, "transport", {"reachable": reachable})

    def restart(
        self,
        *,
        egress_secret: str | None | EllipsisType = ...,
        token: str | None | EllipsisType = ...,
    ) -> None:
        if not isinstance(egress_secret, EllipsisType):
            self.secret = egress_secret
        if not isinstance(token, EllipsisType):
            self._token = token
        self._kill()
        self._spawn()
        if self._ingress_url is not None:
            control(
                self.base_url,
                "configure",
                {"ingress_url": self._ingress_url, "token": self._token or ""},
            )

    def stop(self) -> None:
        self._kill()


class LyingIngressDriver:
    """A driver that declares a delivery_id its adapter will never send.

    The honest verdict for it is a rule 1 FAILURE naming the mismatch. A kit
    that reads it as conformant would certify every broken vendor driver.
    """

    def __init__(self, inner: StubIngressDriver | SubprocessIngressDriver) -> None:
        self.inner = inner

    def start(self, *, ingress_url: str, token: str) -> None:
        self.inner.start(ingress_url=ingress_url, token=token)

    def stimulate(self) -> UpstreamIdentity:
        declared = self.inner.stimulate()
        return UpstreamIdentity(
            stimulus_id=declared.stimulus_id,
            delivery_id=f"never-sent-{uuid.uuid4().hex[:8]}",
        )

    def set_transport(self, *, reachable: bool) -> None:
        self.inner.set_transport(reachable=reachable)

    def restart(
        self,
        *,
        egress_secret: str | None | EllipsisType = ...,
        token: str | None | EllipsisType = ...,
    ) -> None:
        self.inner.restart(egress_secret=egress_secret, token=token)

    def stop(self) -> None:
        self.inner.stop()


def subprocess_driver_from_env() -> SubprocessIngressDriver:
    """A ``--driver module:attr`` target, so the CLI's driver loading is exercised."""

    driver = SubprocessIngressDriver(
        port=int(os.environ["STUB_PORT"]),
        secret=os.environ.get("STUB_SECRET"),
        state_path=Path(os.environ["STUB_STATE"]),
        behavior_name=os.environ.get("STUB_BEHAVIOR", "conformant"),
    )
    # Booted here rather than in ``start``, so the egress rules find a live
    # endpoint whichever order the kit runs its rules in.
    driver.boot()
    return driver


# --- helpers shared by the test modules --------------------------------------


def control(base_url: str, verb: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Drive a stub's control surface over HTTP, the way a vendor driver would."""

    request = urllib.request.Request(
        f"{base_url}/_control/{verb}",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        decoded: dict[str, Any] = json.loads(response.read())
        return decoded


def wait_until_ready(base_url: str, *, timeout_s: float = 20.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(f"{base_url}/_probe", timeout=1):
                return
        except (urllib.error.URLError, OSError, http.client.HTTPException):
            time.sleep(0.02)
    raise AssertionError(f"{base_url} never became ready")


def read_probe(base_url: str) -> dict[str, Any]:
    with urllib.request.urlopen(f"{base_url}/_probe", timeout=10) as response:
        decoded: dict[str, Any] = json.loads(response.read())
        return decoded


def side_effect_probe_for(base_url: str) -> Callable[[], int]:
    """The kit's side effect probe: an integer count, read over HTTP.

    Over HTTP rather than off an object, so the same probe works against a
    subprocess stub and against a vendor adapter in another language.
    """

    def probe() -> int:
        return int(read_probe(base_url)["side_effects"])

    return probe


def rule(report: FloorReport, number: int) -> FloorResult:
    for result in report.results:
        if result.rule == number:
            return result
    raise AssertionError(f"the report carries no rule {number}: {report.detail()}")


def rule_status(report: FloorReport, number: int) -> str:
    return rule(report, number).status


def clauses(report: FloorReport) -> Iterator[ClauseResult]:
    for result in report.results:
        yield from result.clauses


def clause(report: FloorReport, clause_id: str) -> ClauseResult:
    for candidate in clauses(report):
        if candidate.clause == clause_id:
            return candidate
    raise AssertionError(f"the report carries no clause {clause_id}: {report.detail()}")


def clause_status(report: FloorReport, clause_id: str) -> str:
    return clause(report, clause_id).status


def random_secret() -> str:
    return f"stub-secret-{uuid.uuid4().hex}"

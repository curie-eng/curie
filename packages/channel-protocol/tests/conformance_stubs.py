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
from urllib.parse import urlsplit

from channel_protocol.conformance import ClauseResult, FloorReport, FloorResult, UpstreamIdentity

# The egress secret header, as an adapter author reads it out of the guide. Not
# imported from the kit on purpose: see the module docstring.
SECRET_HEADER = "X-Curie-Adapter-Secret"

# The stub retries a TRANSPORT failure for about ten seconds, which is long
# enough for the kit to restore a path it took down and short enough that a
# hung run still ends.
_MAX_ATTEMPTS = 200
_RETRY_DELAY = 0.05

STUB_MAIN = Path(__file__).with_name("stub_adapter_main.py")


@dataclasses.dataclass(frozen=True)
class StubBehavior:
    """One adapter's behavior. Every default is the CONFORMANT choice."""

    # Rule 3 (egress secret verification).
    skips_secret_check: bool = False
    side_effect_then_401: bool = False
    accepts_when_secret_unset: bool = False
    # The side effect a refused request performs ANYWAY, scheduled after the
    # refusal has already been answered. The status is a conformant 401 and the
    # correspondent was answered, so only a count taken after the work settles
    # separates this from a correct adapter.
    side_effect_delay_s: float = 0.0
    # Clause 3b: with its OWN secret unset, refuse the request and serve it.
    side_effect_when_secret_unset: bool = False
    # Rule 4 (ack shape).
    redirects: bool = False
    empty_ack_body: bool = False
    ack_body_bytes: int | None = None
    ack_chunked_bytes: int | None = None
    # A valid probe payload padded past the ack cap, sent as small chunks. A kit
    # that reads the probe with one unbounded call buffers the whole thing.
    probe_chunked_bytes: int | None = None
    # Rule 5 (handles all four events).
    rejects_turn_status: bool = False
    # Rule 6 (dedupe on event_id, and tolerate a finished conversation).
    double_sends_duplicate: bool = False
    # Treats its own retirement of a conversation as final: the exact duplicate
    # is tolerated, and a NEW event_id for that conversation is refused. A
    # multi turn conversation and an outage sweeper both produce that traffic.
    rejects_finished_conversation: bool = False
    # Rule 1 (stable delivery_id).
    fresh_delivery_id_per_retry: bool = False
    # How long the adapter waits before its FIRST attempt. Long enough and a
    # fixed outage window is over before the adapter ever meets it.
    slow_start_s: float = 0.0
    drops_on_transport_failure: bool = False
    # Rule 2 (an ingress response is final).
    retries_after_202: bool = False
    # How long after the 202 the retry goes out. Longer than any fixed finality
    # window and the defect happens after the verdict was taken.
    retry_after_202_delay_s: float = _RETRY_DELAY
    posts_an_unrelated_delivery: bool = False
    # Rule 7: on restart, post something the kit never declared.
    posts_unrelated_after_restart: bool = False
    # Rule 7 (stale credential handling).
    exits_on_401: bool = False
    drops_on_401: bool = False
    self_mints_on_401: bool = False
    # Evidence suppression: the offending ingress post goes out under a
    # Content-Length the ingress cannot use. The adapter under test writes its
    # own request headers, so a kit that lets a header parse failure destroy the
    # observation lets the adapter choose what the kit is able to see.
    evades_with_bad_content_length: bool = False


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
    "rejects_finished_conversation": StubBehavior(rejects_finished_conversation=True),
    "fresh_delivery_id_per_retry": StubBehavior(fresh_delivery_id_per_retry=True),
    "retries_after_202": StubBehavior(retries_after_202=True),
    "posts_an_unrelated_delivery": StubBehavior(posts_an_unrelated_delivery=True),
    "exits_on_401": StubBehavior(exits_on_401=True),
    "drops_on_401": StubBehavior(drops_on_401=True),
    "self_mints_on_401": StubBehavior(self_mints_on_401=True),
    # The two evasions: the same break, with the offending post hidden behind a
    # Content-Length the ingress cannot parse. Each must reach the SAME verdict
    # as the honest break above it.
    "retries_after_202_evasively": StubBehavior(
        retries_after_202=True, evades_with_bad_content_length=True
    ),
    "self_mints_on_401_evasively": StubBehavior(
        self_mints_on_401=True, evades_with_bad_content_length=True
    ),
    # The timing evasions: the same defects, arranged so a kit deciding on a
    # fixed wall clock window records the conformant half and stops watching.
    "slow_start_and_drops_on_transport_failure": StubBehavior(
        slow_start_s=0.25, drops_on_transport_failure=True
    ),
    "retries_late_after_202": StubBehavior(
        retries_after_202=True, retry_after_202_delay_s=0.65
    ),
    "delayed_side_effect_then_401": StubBehavior(
        side_effect_then_401=True, side_effect_delay_s=0.1
    ),
    "side_effect_when_secret_unset": StubBehavior(side_effect_when_secret_unset=True),
    "drops_on_401_and_posts_unrelated_after_restart": StubBehavior(
        drops_on_401=True, posts_unrelated_after_restart=True
    ),
}


def free_port() -> int:
    """An ephemeral port nothing holds. Never a hardcoded one: this box runs
    parallel jobs and a fixed port collides."""

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def post_with_declared_length(
    url: str, body: bytes, headers: dict[str, str], *, declared_length: str
) -> int:
    """POST with a Content-Length the CALLER chooses, honest or not.

    ``urllib`` computes that header itself, so a raw connection is the only way
    to send the framing a hostile or broken adapter would send. Returns the
    status, and raises on a transport failure so a caller that expected an
    answer cannot mistake a dead connection for one.
    """

    parsed = urlsplit(url)
    connection = http.client.HTTPConnection(
        parsed.hostname or "127.0.0.1", parsed.port, timeout=10
    )
    try:
        connection.putrequest("POST", parsed.path)
        for name, value in headers.items():
            connection.putheader(name, value)
        connection.putheader("Content-Length", declared_length)
        connection.endheaders()
        connection.send(body)
        return int(connection.getresponse().status)
    finally:
        connection.close()


def _post_unparseable_length(
    url: str, body: bytes, headers: dict[str, str]
) -> int | None:
    """The evasion: a Content-Length no server can turn into a number."""

    try:
        return post_with_declared_length(
            url, body, headers, declared_length=f"{len(body)}x"
        )
    except (OSError, http.client.HTTPException):
        return None


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
            oversize = self.stub.behavior.probe_chunked_bytes
            if oversize is not None:
                # A VALID probe payload, just far too large. A kit that reads it
                # unbounded returns the count and buffers the padding, so this
                # break is only caught by a cap and never by a parse error.
                self._chunked_json(b'{"side_effects": 0, "pad": "', oversize)
                return
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
        if path == "/_control/retired":
            self._json(200, {"retired": stub.is_retired(str(payload["delivery_id"]))})
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
            if behavior.side_effect_when_secret_unset:
                # The 3b break the status cannot show: the correspondent is
                # answered and the response is a clean refusal.
                stub.perform_side_effect(body)
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
                # status is identical to the conformant one, and with a delay
                # the count is identical too at the instant of the response.
                stub.perform_side_effect(body)
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
            self._chunked_json(b'{"ref": "x", "pad": "', behavior.ack_chunked_bytes)
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

    def _chunked_json(self, head: bytes, size: int) -> None:
        """An oversize JSON body with NO content length, sent as small chunks.

        A kit that trusts ``Content-Length``, or that does one sized read,
        passes this. The worker's ``_read_capped`` is a loop for exactly this
        reason, so every read the kit makes has to be one too. ``head`` opens
        the object, so the same writer serves an acknowledgement and a side
        effect probe response.
        """

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()
        first = head
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
        self._retired: set[str] = set()
        self._timers: list[threading.Timer] = []
        self._starts = 0
        self.ingress_url: str | None = None
        self.token: str | None = None
        self.side_effects = 0
        self.dropped = 0
        self.stale_credential = False
        self.held: list[dict[str, str]] = []
        self.delivered: list[str] = []
        self.seen_event_ids: list[str] = []
        self.finished_conversations: list[str] = []
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
        self._starts += 1
        httpd = ThreadingHTTPServer(("127.0.0.1", self.port), _Handler)
        httpd.daemon_threads = True
        httpd.stub = self  # type: ignore[attr-defined]
        self.port = int(httpd.server_address[1])
        self._httpd = httpd
        self._serve_thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        self._serve_thread.start()
        self._sender_thread = threading.Thread(target=self._send_loop, daemon=True)
        self._sender_thread.start()
        if self.behavior.posts_unrelated_after_restart and self._starts > 1:
            # Rule 7's break that a bare "some 2xx arrived" predicate reads as a
            # resume: the held delivery is gone and something else takes its
            # place on the wire.
            self.inject(f"unrelated-after-restart-{uuid.uuid4().hex[:8]}")

    def stop(self) -> None:
        self._stopping.set()
        with self._lock:
            timers, self._timers = self._timers, []
        for timer in timers:
            timer.cancel()
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
        # Restored BEFORE the send loop exists, so a resumed delivery never
        # races the configuration it needs.
        self.ingress_url = ingress_url
        self.start()

    def configure(self, *, ingress_url: str, token: str) -> None:
        self.ingress_url = ingress_url
        self.token = token

    # -- egress ------------------------------------------------------------

    def handle_event(self, body: bytes) -> str:
        """Record one reply event and perform its side effect. ``ok`` or ``reject``."""

        try:
            decoded = json.loads(body)
        except ValueError:
            decoded = None
        with self._lock:
            if not isinstance(decoded, dict):
                self.events.append({"event": "<unparseable>"})
                return "ok"
            name = str(decoded.get("event"))
            self.events.append({"event": name})
            if name == "turn.status" and self.behavior.rejects_turn_status:
                return "reject"
            if name == "turn.completed":
                event_id = str(decoded.get("event_id"))
                duplicate = event_id in self.seen_event_ids
                target = decoded.get("target")
                conversation = (
                    str(target.get("conversation_id"))
                    if isinstance(target, dict)
                    else ""
                )
                if (
                    self.behavior.rejects_finished_conversation
                    and not duplicate
                    and conversation in self.finished_conversations
                ):
                    # Rule 6's second half, and it is invisible to the first: the
                    # exact duplicate is still tolerated, so the dedupe probe
                    # reads this adapter as conformant. Only a completion for a
                    # conversation it has already retired is refused.
                    return "reject"
                if conversation and conversation not in self.finished_conversations:
                    self.finished_conversations.append(conversation)
                if not duplicate:
                    self.seen_event_ids.append(event_id)
                if not duplicate or self.behavior.double_sends_duplicate:
                    self.side_effects += 1
                self._persist_locked()
                return "ok"
            self.side_effects += 1
            self._persist_locked()
            return "ok"

    def perform_side_effect(self, body: bytes) -> None:
        """Answer the correspondent, possibly AFTER the response has gone out.

        A real adapter that hands the work to a queue does exactly this, so a
        delay here is a plausible adapter and not a contrived one. It is also
        invisible to any check that reads a count the moment a response lands.
        """

        delay = self.behavior.side_effect_delay_s
        if not delay:
            self.handle_event(body)
            return
        timer = threading.Timer(delay, self.handle_event, args=(body,))
        timer.daemon = True
        with self._lock:
            self._timers.append(timer)
        timer.start()

    def probe(self) -> dict[str, Any]:
        with self._lock:
            return {
                "side_effects": self.side_effects,
                "dropped": self.dropped,
                "stale_credential": self.stale_credential,
                "held": len(self.held),
                "delivered": list(self.delivered),
                "seen_event_ids": list(self.seen_event_ids),
                "finished_conversations": list(self.finished_conversations),
                "events": list(self.events),
                "retired": sorted(self._retired),
            }

    # -- ingress -----------------------------------------------------------

    def inject(self, upstream_id: str) -> str:
        """Deliver one upstream message. Returns the delivery_id it will send.

        The derivation is documented and deterministic, which is what lets a
        driver DECLARE the identity before this is ever called.
        """

        delivery_id = f"dlv-{upstream_id}"
        with self._lock:
            self._retired.discard(delivery_id)
            self._pending.append(
                {
                    "delivery_id": delivery_id,
                    "conversation_id": f"conv-{upstream_id}",
                    "author": "someone@example.test",
                    "text": "hello",
                }
            )
        return delivery_id

    def is_retired(self, delivery_id: str) -> bool:
        """Whether this delivery is done: acknowledged, or given up on.

        The stub's answer to the driver contract's quiescence barrier. It is set
        only once ``_deliver`` has returned for that identity, so an adapter
        still inside a retry (however slow) is never reported retired.
        """

        with self._lock:
            return delivery_id in self._retired

    def _send_loop(self) -> None:
        while not self._stopping.is_set():
            with self._lock:
                item = self._pending.pop(0) if self._pending else None
            if item is None:
                time.sleep(0.01)
                continue
            try:
                self._deliver(item)
            finally:
                # Retired means "this adapter is done with the identity", pass
                # or fail. Set here rather than on the success path, so a
                # delivery that was dropped still stops the kit waiting on it.
                with self._lock:
                    self._retired.add(item["delivery_id"])
                    self._persist_locked()

    def _deliver(self, item: dict[str, str]) -> None:
        base = item["delivery_id"]
        if self.behavior.slow_start_s:
            # A first attempt late enough that a fixed outage window would be
            # over before it happens.
            time.sleep(self.behavior.slow_start_s)
        attempt = 0
        while attempt < _MAX_ATTEMPTS and not self._stopping.is_set():
            if self._target() is None:
                # Not yet pointed at an ingress. Waiting to be configured is not
                # a delivery attempt, and counting it as one would make a
                # restart consume an attempt and rename the delivery under
                # `fresh_delivery_id_per_retry` before it ever touched the wire.
                time.sleep(_RETRY_DELAY)
                continue
            attempt += 1
            if attempt == 1 or not self.behavior.fresh_delivery_id_per_retry:
                delivery_id = base
            else:
                delivery_id = f"{base}-r{attempt}"
            status = self._post_turn(delivery_id, item, evasive=False)
            if status is None:
                if self.behavior.drops_on_transport_failure:
                    with self._lock:
                        self.dropped += 1
                        self._persist_locked()
                    return
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
                time.sleep(self.behavior.retry_after_202_delay_s)
                self._post_turn(
                    delivery_id,
                    item,
                    evasive=self.behavior.evades_with_bad_content_length,
                )
            if self.behavior.posts_an_unrelated_delivery and not self._posted_unrelated:
                # A legitimate upstream redelivery under an identity the kit
                # never declared. It must not red rule 2.
                self._posted_unrelated = True
                unrelated = f"dlv-unrelated-{uuid.uuid4().hex[:8]}"
                self._post_turn(
                    unrelated, dict(item, conversation_id="conv-unrelated"), evasive=False
                )
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
        return self.ingress_url

    def _post_turn(
        self, delivery_id: str, item: dict[str, str], *, evasive: bool
    ) -> int | None:
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
        headers = {"Content-Type": "application/json", "X-API-Key": self.token or ""}
        if evasive:
            return _post_unparseable_length(f"{target}/channels/turns", body, headers)
        request = urllib.request.Request(
            f"{target}/channels/turns", data=body, headers=headers, method="POST"
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
        body = json.dumps({"kind": "email", "address": "agent@example.test"}).encode()
        headers = {"Content-Type": "application/json"}
        if self.behavior.evades_with_bad_content_length:
            _post_unparseable_length(f"{target}/channels/token", body, headers)
            return
        request = urllib.request.Request(
            f"{target}/channels/token", data=body, headers=headers, method="POST"
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
                    "finished_conversations": self.finished_conversations,
                    "events": self.events,
                    "retired": sorted(self._retired),
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
        self.finished_conversations = list(state.get("finished_conversations", []))
        self.events = list(state.get("events", []))
        self._retired = set(state.get("retired", []))
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
    probe_chunked_bytes: int | None = None,
) -> StubAdapter:
    """A stub that satisfies every floor rule the kit can assert."""

    behavior = dataclasses.replace(
        CONFORMANT,
        ack_body_bytes=ack_body_bytes,
        ack_chunked_bytes=ack_chunked_bytes,
        probe_chunked_bytes=probe_chunked_bytes,
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

    def reserve(self) -> UpstreamIdentity:
        self._stimuli += 1
        upstream_id = uuid.uuid4().hex[:10]
        # DECLARED from the documented derivation, and nothing is injected: the
        # kit arms its ingress for this identity before the message exists.
        return UpstreamIdentity(
            stimulus_id=f"in-process-{self._stimuli}", delivery_id=f"dlv-{upstream_id}"
        )

    def release(self, identity: UpstreamIdentity) -> None:
        self.stub.inject(identity.delivery_id.removeprefix("dlv-"))

    def settled(self, identity: UpstreamIdentity) -> bool:
        return self.stub.is_retired(identity.delivery_id)

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

    def reserve(self) -> UpstreamIdentity:
        self._stimuli += 1
        upstream_id = uuid.uuid4().hex[:10]
        # DECLARED, not read back: the derivation is the documented contract.
        return UpstreamIdentity(
            stimulus_id=f"subprocess-{self._stimuli}", delivery_id=f"dlv-{upstream_id}"
        )

    def release(self, identity: UpstreamIdentity) -> None:
        control(
            self.base_url,
            "stimulate",
            {"upstream_id": identity.delivery_id.removeprefix("dlv-")},
        )

    def settled(self, identity: UpstreamIdentity) -> bool:
        answer = control(
            self.base_url, "retired", {"delivery_id": identity.delivery_id}
        )
        return bool(answer["retired"])

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
        # The lie is only in what it DECLARES. The message it injects is the
        # honest one, so the adapter is exonerated and the driver is the finding.
        self._honest: dict[str, UpstreamIdentity] = {}

    def start(self, *, ingress_url: str, token: str) -> None:
        self.inner.start(ingress_url=ingress_url, token=token)

    def reserve(self) -> UpstreamIdentity:
        honest = self.inner.reserve()
        self._honest[f"never-sent-{uuid.uuid4().hex[:8]}"] = honest
        declared = list(self._honest)[-1]
        return UpstreamIdentity(
            stimulus_id=honest.stimulus_id, delivery_id=declared
        )

    def release(self, identity: UpstreamIdentity) -> None:
        self.inner.release(self._honest[identity.delivery_id])

    def settled(self, identity: UpstreamIdentity) -> bool:
        return self.inner.settled(self._honest[identity.delivery_id])

    def restart(
        self,
        *,
        egress_secret: str | None | EllipsisType = ...,
        token: str | None | EllipsisType = ...,
    ) -> None:
        self.inner.restart(egress_secret=egress_secret, token=token)

    def stop(self) -> None:
        self.inner.stop()


class RacingIngressDriver:
    """A driver whose adapter already has ANOTHER delivery in flight.

    Not a hostile driver: an upstream queue holding more than one message is the
    ordinary case, and this is what it looks like from the ingress. It is also
    the exact shape that drains a globally armed one shot 202 before the
    declared delivery can reach it, which is why rule 2 arms for one identity.
    The decoy is released once, on the first stimulus, so the later rules run
    against an adapter with nothing extra in flight.
    """

    def __init__(self, inner: StubIngressDriver | SubprocessIngressDriver) -> None:
        self.inner = inner
        self._raced = False

    def start(self, *, ingress_url: str, token: str) -> None:
        self.inner.start(ingress_url=ingress_url, token=token)

    def reserve(self) -> UpstreamIdentity:
        return self.inner.reserve()

    def release(self, identity: UpstreamIdentity) -> None:
        if not self._raced:
            self._raced = True
            decoy = self.inner.reserve()
            self.inner.release(decoy)
            deadline = time.monotonic() + 10.0
            while time.monotonic() < deadline and not self.inner.settled(decoy):
                time.sleep(0.02)
        self.inner.release(identity)

    def settled(self, identity: UpstreamIdentity) -> bool:
        return self.inner.settled(identity)

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

"""The kit's egress half: how it talks to the adapter under test (#1516).

Everything here is BLACK BOX. The kit holds an endpoint, a secret and the two
address fields, and it learns what the adapter does purely from what comes back
over HTTP. It never imports the adapter, so the same kit works on an adapter
written in Go.

Every event the kit sends is built from a real ``channel_protocol.reply`` model
and serialized with ``model_dump_json``, with no exceptions, so the kit can
never send a shape the worker could not send. A kit that hand built a payload
would be free to assert a requirement the published floor never states.

Every response the kit reads off the adapter is read in a LOOP with a running
total and refused past the cap, the acknowledgement and the side effect probe
alike. The adapter is untrusted by construction and both reads land on the
kit's own heap.

``ADAPTER_SECRET_HEADER`` and ``MAX_ACK_BODY_BYTES`` are RE DECLARED here, never
imported. ``apps/worker/src/curie_worker/reply_sink.py`` is the authoritative
site for both, and importing it would invert the dependency: this package is a
contract package and the worker imports IT. The copy is kept honest by
``tests/test_conformance_egress.py::test_platform_constants_match``, which reads
both values back out of the worker source and fails on any drift.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Callable
from typing import Any
from urllib.parse import urlsplit

import httpx
from pydantic import BaseModel, ConfigDict

from ..models import MESSAGE_VERSION, OutboundMessage
from ..reply import (
    REPLY_WIRE_VERSION,
    ReplyEvent,
    ReplyPost,
    ReplyTarget,
    ReplyUpdate,
    TurnCompleted,
    TurnStatus,
)

# The per adapter egress credential travels in this header, and only this one.
# Authoritative site: apps/worker/src/curie_worker/reply_sink.py.
ADAPTER_SECRET_HEADER = "X-Curie-Adapter-Secret"

# The most acknowledgement body the worker will read off an adapter. Oversize is
# a DELIVERY FAILURE there, not a truncation, so the kit's boundary is the same
# one: at the cap passes, one byte over fails.
# Authoritative site: apps/worker/src/curie_worker/reply_sink.py.
MAX_ACK_BODY_BYTES = 64 * 1024

# Where the kit reads the adapter's side effect count from. A count, not a log:
# the two clauses that need it (3a and rule 6) both ask "did the number move",
# and an integer over HTTP is the smallest thing an adapter in any language can
# expose. Without it those clauses are not_run, and not_run is nonconformant.
SIDE_EFFECT_PROBE_PATH = "/_probe"
SIDE_EFFECT_PROBE_FIELD = "side_effects"

# A secret the adapter cannot have been configured with. Clause 3a posts under
# it and the request must be refused.
WRONG_SECRET = "conformance_kit_deliberately_wrong_secret"

_STRICT = ConfigDict(frozen=True, extra="forbid")


class AdapterUnreachableError(RuntimeError):
    """The adapter endpoint did not answer at all.

    Distinct from a refusal: a response that arrived and said no is evidence,
    and an endpoint that never answered is the absence of it. The message names
    only the REDACTED endpoint and the exception class, because these strings
    reach a report a vendor pastes into a public issue and an endpoint's path or
    query can itself be a credential.
    """


def redacted(endpoint: str) -> str:
    """An endpoint reduced to scheme and host, for a string a human will read.

    Same shape, and the same reason, as the worker's own helper: an endpoint can
    carry a token in its path or its query, and every string this module puts in
    a report is quotable.
    """

    parsed = urlsplit(endpoint)
    if not parsed.scheme or not parsed.hostname:
        return "an unparseable endpoint"
    port = f":{parsed.port}" if parsed.port else ""
    return f"{parsed.scheme}://{parsed.hostname}{port}"


class AdapterResponse(BaseModel):
    """One answer from the adapter, read under the ack cap.

    ``oversize`` is a first class outcome rather than a truncated ``body``: the
    floor treats a body over the cap as a failure, and a caller handed a
    silently shortened body could not tell the two apart.
    """

    model_config = _STRICT

    status: int
    body: bytes
    oversize: bool


class AdapterUnderTest(BaseModel):
    """The adapter the kit is pointed at, and the only handle it has on it."""

    model_config = _STRICT

    endpoint: str
    secret: str
    kind: str
    address: str
    timeout_s: float

    @property
    def redacted_endpoint(self) -> str:
        return redacted(self.endpoint)

    @property
    def origin(self) -> str:
        """The endpoint's scheme and authority, with no path, query or fragment."""

        parsed = urlsplit(self.endpoint)
        return f"{parsed.scheme}://{parsed.netloc}"

    def post_body(self, body: bytes, *, secret: str | None) -> AdapterResponse:
        """POST one raw event body. ``secret=None`` sends no credential header.

        Redirects are never followed. Following one would replay the platform's
        egress credential at whatever origin the adapter named, which is exactly
        why the worker refuses them, so a kit that followed them would certify a
        credential exfiltration primitive.

        The body is read in a LOOP with a running total, never one sized read: a
        chunked response answers a short first buffer, so a single read lets a
        body of any size through behind it.
        """

        headers = {"Content-Type": "application/json"}
        if secret is not None:
            headers[ADAPTER_SECRET_HEADER] = secret
        try:
            with httpx.Client(timeout=self.timeout_s, follow_redirects=False) as client:
                with client.stream(
                    "POST", self.endpoint, content=body, headers=headers
                ) as response:
                    chunks: list[bytes] = []
                    total = 0
                    oversize = False
                    for chunk in response.iter_bytes():
                        total += len(chunk)
                        if total > MAX_ACK_BODY_BYTES:
                            oversize = True
                            break
                        chunks.append(chunk)
                    return AdapterResponse(
                        status=response.status_code,
                        body=b"".join(chunks),
                        oversize=oversize,
                    )
        except httpx.HTTPError as error:
            raise AdapterUnreachableError(
                f"the adapter endpoint {self.redacted_endpoint} did not answer "
                f"({type(error).__name__})"
            ) from None

    def post_event(self, event: ReplyEvent, *, secret: str | None) -> AdapterResponse:
        """POST one real reply event, serialized the way the worker serializes it."""

        return self.post_body(event.model_dump_json().encode(), secret=secret)


def new_conversation_id() -> str:
    """A conversation the adapter has never seen, so a probe cannot collide."""

    return f"conf-conv-{uuid.uuid4().hex}"


def new_event_id() -> str:
    return f"conf-event-{uuid.uuid4().hex}"


def reply_target(adapter: AdapterUnderTest, *, conversation_id: str) -> ReplyTarget:
    return ReplyTarget(
        kind=adapter.kind,
        address=adapter.address,
        conversation_id=conversation_id,
        reply_ref=f"conf-ref-{conversation_id}",
    )


def turn_status(adapter: AdapterUnderTest, *, conversation_id: str) -> TurnStatus:
    return TurnStatus(
        version=REPLY_WIRE_VERSION,
        target=reply_target(adapter, conversation_id=conversation_id),
        event="turn.status",
        status="the conformance kit is checking this adapter",
    )


def reply_update(adapter: AdapterUnderTest, *, conversation_id: str) -> ReplyUpdate:
    return ReplyUpdate(
        version=REPLY_WIRE_VERSION,
        target=reply_target(adapter, conversation_id=conversation_id),
        event="reply.update",
        text="a conformance kit reply",
    )


def reply_post(adapter: AdapterUnderTest, *, conversation_id: str) -> ReplyPost:
    return ReplyPost(
        version=REPLY_WIRE_VERSION,
        target=reply_target(adapter, conversation_id=conversation_id),
        event="reply.post",
        message=OutboundMessage(version=MESSAGE_VERSION, text="a conformance kit card"),
        requested_by="conformance_kit",
    )


def turn_completed(
    adapter: AdapterUnderTest, *, conversation_id: str, event_id: str
) -> TurnCompleted:
    return TurnCompleted(
        version=REPLY_WIRE_VERSION,
        target=reply_target(adapter, conversation_id=conversation_id),
        event="turn.completed",
        event_id=event_id,
        outcome="delivered",
    )


def side_effect_probe(adapter: AdapterUnderTest) -> Callable[[], int] | None:
    """The adapter's side effect count, if it exposes one, else None.

    Discovery is one real GET rather than a promise: an adapter that does not
    answer ``/_probe`` leaves clause 3a and rule 6 with no evidence, and the
    honest report for that is ``not_run``, which is nonconformant. Returning a
    callable that would fail later would turn a missing probe into a crash
    halfway through a run instead.

    The probe body is read under the SAME cap as an acknowledgement, streamed
    with a running total, and redirects are not followed. This GET lands on the
    same untrusted origin the acknowledgement comes from, it is issued once per
    probed clause rather than once per run, and a probe that answered an
    unbounded chunked body would OOM whatever CI box the vendor runs the kit on.
    The count itself is one integer, so a payload near the cap is already orders
    of magnitude past anything honest.
    """

    url = f"{adapter.origin}{SIDE_EFFECT_PROBE_PATH}"

    def read() -> int:
        with httpx.Client(timeout=adapter.timeout_s, follow_redirects=False) as client:
            with client.stream("GET", url) as response:
                response.raise_for_status()
                chunks: list[bytes] = []
                total = 0
                for chunk in response.iter_bytes():
                    total += len(chunk)
                    if total > MAX_ACK_BODY_BYTES:
                        raise ValueError(
                            "the side effect probe answered with more than "
                            f"{MAX_ACK_BODY_BYTES} bytes, which the kit refuses to buffer"
                        )
                    chunks.append(chunk)
        payload: dict[str, Any] = json.loads(b"".join(chunks))
        return int(payload[SIDE_EFFECT_PROBE_FIELD])

    try:
        read()
    except (httpx.HTTPError, KeyError, TypeError, ValueError):
        return None
    return read

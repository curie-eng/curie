"""The channel ingress API (ADR-0096 phase 2, #1459).

Two endpoints, and every request/response model they use lives here rather than
in ``schemas.py``:

- ``POST /channels/token`` (platform key only) mints a ``chn`` token over a
  binding ROW's id plus its current generation, so an ingress adapter holds a
  credential scoped to exactly one binding instead of the platform key.
- ``POST /channels/turns`` (platform key OR a ``chn`` token) enqueues a
  ``QueuedTurn`` for the binding named in the BODY.

**Kind and address ride in the BODY, not the path.** An address is an opaque
routing key matched on equality; it may contain ``@``, ``.``, ``/``, ``?`` or
``#``, and a path segment silently 404s on the ``/`` case instead of failing
loudly.

**``X-API-Key``, not ``Authorization: Bearer``.** Every authenticated router in
this API reads ``X-API-Key`` (``auth.py``); one header, one parser, and the
wrong one is simply absent.

**The platform mints the turn; the request never routes itself.** ``kind``,
``endpoint`` and ``adapter`` come from the binding row loaded during token
validation, never from the request body (plan D4.1). Accepting an endpoint from
the wire would let one token point the platform's AUTHENTICATED egress at any
URL, which is credential capture rather than merely SSRF.
"""

from __future__ import annotations

import logging
import secrets
import time
import uuid
from datetime import UTC, datetime
from typing import Annotated, Any

import redis.asyncio as redis
from aci_protocol import STREAM_PAYLOAD_FIELD, QueuedTurn, ReplyHandle, TurnSource
from fastapi import (
    APIRouter,
    Depends,
    Header,
    HTTPException,
    Request,
    Response,
    status,
)
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from sqlalchemy import select

from .. import channel_token
from ..auth import require_api_key, verify_platform_key
from ..channel_token import CHANNEL_ENQUEUE_SCOPE
from ..config import get_settings
from ..delivery import (
    claim_delivery,
    duplicate_stream_id,
    enqueue_owned,
    release_claim,
    sha16,
    take_backlog_slot,
)
from ..deps import SessionDep

# The API's Valkey client is built without `decode_responses`, so values come
# back as bytes; `_text` is the package's named, documented decode for exactly
# that, and this router is not the place for a seventh copy of the expression.
from ..graveyardwatcher import _text
from ..models import AgentChannel
from ..schemas import ChannelBinding
from ..wirebody import read_bounded_body

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/channels", tags=["channels"])

# The kind whose reply route is legitimately implicit: Slack replies go through
# the worker's CONFIGURED Slack origin (a wire-supplied endpoint would hand the
# bot token to whatever URL a turn named), so an unset endpoint/adapter pair is
# COMPLETE for `slack` and half-configured for anything else.
_IMPLICIT_ROUTE_KIND = "slack"

# The one detail string every ingress auth failure returns, identical for "no
# credential" and "wrong credential" -- the same string `require_state_access`
# uses. A caller that can tell the two apart (or spot a stale generation) can
# enumerate live bindings and probe rebinds.
_AUTH_DETAIL = "missing or invalid credential"

# The delivery claim key, per BINDING and delivery: `event_id` is derived from
# `(channel_id, delivery_id)` and never `delivery_id` alone, so two adapters
# sharing an upstream id space -- two AgentMail inboxes, two webhook sources --
# cannot swallow each other's turns (E16).
_CLAIM_PREFIX = "curie:channel"


# --- request/response models --------------------------------------------------


class ChannelTokenRequest(ChannelBinding):
    """Mint request: the binding pair to scope the token to, plus its lifetime.

    Subclasses `ChannelBinding` so the SAME `_validate_channel_binding` the
    agents API runs judges the pair here (E11). Two write paths cannot drift
    into two rules -- that drift is how #143 happened -- and an operator reads
    the identical message from either endpoint.
    """

    # An hour by default; a week is the ceiling. The TTL and the generation are
    # the only two things standing in for the revocation list phase 2
    # deliberately does not build, so an unbounded lifetime would remove one of
    # them.
    ttl_s: int = Field(default=3600, gt=0, le=604800)


class ChannelTokenOut(BaseModel):
    token: str


class TurnIn(ChannelBinding):
    """One inbound delivery, as the adapter presents it.

    `extra="ignore"`, unlike its parent: the body deliberately does NOT model
    `endpoint` or `adapter` (plan D4.1), and naming them must CHANGE NOTHING
    rather than 422 -- the fields are simply not part of what a caller gets a
    say in. The platform reads both from the binding row.

    `delivery_id` is the adapter's own stable upstream identifier (AgentMail's
    `message_id`, a webhook's delivery id). The platform derives `event_id` from
    it deterministically, so an adapter that never saw the response converges by
    retrying rather than by enqueuing a second turn.
    """

    model_config = ConfigDict(extra="ignore", from_attributes=True)

    delivery_id: str
    conversation_id: str
    author: str
    text: str
    reply_ref: str


class TurnAccepted(BaseModel):
    """The ingress receipt. `duplicate` says whether THIS request enqueued.

    `stream_id` is None only while another request holds the claim and has not
    enqueued yet (the 202 case): there is no id to hand back yet, and the
    adapter's retry loop already knows what "come back" means.
    """

    event_id: str
    stream_id: str | None
    duplicate: bool


# --- shared helpers -----------------------------------------------------------


def _parse_turn(raw: bytes) -> TurnIn:
    """Validate the raw body as a `TurnIn`, reporting failures as FastAPI would.

    The handler reads the body itself (the size bound has to run before anything
    else touches it), so this restores the 422 an ordinary body parameter would
    have produced -- same `loc`, same message text, which is what lets T-C7
    compare the ingress's validation messages against the agents API's.
    """

    try:
        return TurnIn.model_validate_json(raw)
    except ValidationError as exc:
        raise RequestValidationError(
            [
                {**error, "loc": ("body", *error.get("loc", ()))}
                for error in exc.errors(
                    include_url=False, include_context=False, include_input=False
                )
            ]
        ) from exc


async def _resolve_binding(
    session: Any, kind: str, address: str
) -> AgentChannel | None:
    """The binding row for one `(kind, address)` pair, or None.

    The PAIR, never the address alone: since migration 0023 one address can be
    bound under two kinds, and resolving on the address would let one kind's
    adapter reach the other kind's agent.
    """

    row: AgentChannel | None = await session.scalar(
        select(AgentChannel).where(
            AgentChannel.kind == kind, AgentChannel.address == address
        )
    )
    return row


def _route_is_configured(row: AgentChannel) -> bool:
    """Whether this binding can actually deliver a reply.

    `slack` needs no per-binding route (D4.4). Every other kind needs both
    halves, and the DB CHECK guarantees they are both-or-neither, so testing one
    of them would be enough -- both are tested because the guarantee is the
    database's, not this function's.
    """

    if row.kind == _IMPLICIT_ROUTE_KIND:
        return True
    return row.endpoint is not None and row.adapter is not None


def _unroutable(kind: str, address: str) -> HTTPException:
    """E17's refusal, in ONE place because BOTH endpoints owe it.

    Refusing at the mint closes only the token-bearing path: the platform key is
    accepted at the ingress by design, so a first-party caller could otherwise
    post a turn for a route-less non-`slack` binding and have it enqueued with
    `endpoint=None, adapter=None`, fail closed in the worker's reply sink, and
    escalate to a human far from the request that caused it -- the mid-turn
    failure E17 exists to prevent. One rule, one message, stated once.
    """

    return HTTPException(
        status.HTTP_409_CONFLICT,
        f"the binding for kind {kind!r} at address {address!r} has no reply "
        "route: set its endpoint and adapter before minting a token for it or "
        "posting turns to it, or its turns would be enqueued with nowhere to "
        "reply and no credential to reply with. Send them on "
        "PATCH /agents/{agent_id}/channels, selecting this binding with "
        f"?kind={kind}&address={address} -- or on POST /agents/{{agent_id}}"
        "/channels when adding the binding. The agent-level `channel` field is "
        "retired and rejects a binding write.",
    )


# --- POST /channels/token -----------------------------------------------------


@router.post(
    "/token", response_model=ChannelTokenOut, dependencies=[Depends(require_api_key)]
)
async def mint_channel_token(
    data: ChannelTokenRequest, session: SessionDep
) -> ChannelTokenOut:
    """Mint a `chn` token for one binding (platform key only).

    Platform-key-only on purpose: a `chn` token does exactly one thing, enqueue
    for the binding in its claims. If it could mint, a compromised adapter would
    defeat both the TTL and the generation -- the only two things standing in for
    a revocation list.

    The token claims the ROW's id and its CURRENT generation, so it dies both
    when the pair is deleted and recreated (a new row id) and when the binding is
    re-pointed or merely re-asserted (a bumped generation).
    """

    row = await _resolve_binding(session, data.kind, data.address)
    if row is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"no agent is bound to channel kind {data.kind!r} at address "
            f"{data.address!r}; bind one before minting a token for it",
        )
    if not _route_is_configured(row):
        # E17: refuse at MINT time, so the operator error surfaces at bind time
        # rather than as a fail-closed escalation mid-turn, in the worker, far
        # from the request that caused it. `ingest_turn` raises the same refusal.
        raise _unroutable(data.kind, data.address)
    token = channel_token.mint(
        get_settings().api_key,
        channel_id=str(row.id),
        generation=row.generation,
        scope=CHANNEL_ENQUEUE_SCOPE,
        exp=int(time.time()) + data.ttl_s,
    )
    return ChannelTokenOut(token=token)


# --- POST /channels/turns -----------------------------------------------------


# Both names below are built from the SAME digest of the same `delivery_id`, so
# the handler hashes once and hands the digest to each: they name one delivery,
# and re-deriving it per name is work paid on every ingress request.
def _event_id(channel_id: uuid.UUID, digest: str) -> str:
    return f"chn-{channel_id}-{digest}"


def _claim_key(channel_id: uuid.UUID, digest: str) -> str:
    return f"{_CLAIM_PREFIX}:delivery:{channel_id}:{digest}"


def _verify_credential(x_api_key: str | None) -> channel_token.ChannelClaims | None:
    """The STATELESS half of the ingress 401 matrix: no row, no query, no I/O.

    The platform key first (operator/first-party), which authenticates on its own
    and carries no binding claim -- `None` is its "authenticated, nothing
    claimed". Else the header, if present at all, is verified as a `chn` token
    against the shared key: signature, scope and expiry are decidable without
    touching the database, so a caller holding no valid credential is refused
    BEFORE it can make the API query anything. Without that ordering a network
    attacker floods small well-formed bodies, each costing a binding lookup, and
    exhausts the database pool through a route that answers 401.

    Every failure -- absent header, malformed token, an expired token, a foreign
    key's signature, the wrong scope, the right token in the wrong header --
    raises the identical 401. `_authorize` decides the row-dependent half.
    """

    if verify_platform_key(x_api_key):
        return None
    if x_api_key is not None:
        claims = channel_token.verify_claims(
            x_api_key, get_settings().api_key, scope=CHANNEL_ENQUEUE_SCOPE
        )
        if claims is not None:
            return claims
    raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail=_AUTH_DETAIL)


def _authorize(
    claims: channel_token.ChannelClaims | None, row: AgentChannel | None
) -> None:
    """The STATEFUL half: bind already-verified claims to the loaded row.

    A token names one binding ROW at one GENERATION, so a token for another
    binding, for a pair that resolves to nothing, or for a generation the row has
    moved past is refused here -- with the identical detail string the stateless
    half uses, because a caller that can tell "no credential" from "wrong
    binding" from "stale generation" can enumerate live bindings and probe
    rebinds. The platform key claims no binding and passes.
    """

    if claims is None:
        return
    if (
        row is None
        or str(row.id) != claims.channel_id
        or row.generation != claims.generation
    ):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail=_AUTH_DETAIL)


def _mint_turn(row: AgentChannel, body: TurnIn, event_id: str) -> QueuedTurn:
    """Build the `QueuedTurn` from the BINDING ROW plus the delivery's content.

    Three fields the request gets no say in: `kind` (the routing half),
    `endpoint` (where the reply goes) and `adapter` (whose credential
    authenticates it). This is the only mint site that produces non-Slack turns,
    so leaving `adapter` unset here would leave every production email turn
    without an egress-credential selector on the pre-resolution path.

    `event_id` is passed in rather than re-derived: the handler needs it before
    the claim (every duplicate answer carries it) and this turn must carry the
    identical one.
    """

    return QueuedTurn(
        event_id=event_id,
        conversation_id=body.conversation_id,
        author=body.author,
        text=body.text,
        # A channel-port turn is a PERSON writing on some transport (an email they
        # sent, a message they typed). The transport is `reply_handle.kind`; this
        # field is about who caused the turn, and the answer here is a human.
        # ADR-0079's hook ingress is a separate route and will say `WEBHOOK`.
        source=TurnSource.SLACK,
        reply_handle=ReplyHandle(
            kind=row.kind,
            channel=row.address,
            placeholder=body.reply_ref,
            endpoint=row.endpoint,
            adapter=row.adapter,
        ),
        received_at=datetime.now(UTC).isoformat(),
    )


def _duplicate(event_id: str, current: str, response: Response) -> TurnAccepted:
    """Answer a delivery someone else owns, from what the claim key holds.

    The status and the None-vs-id decision live in `delivery.duplicate_stream_id`
    so the hook ingress cannot drift from them; only this route's receipt SHAPE
    is built here.
    """

    stream_id = duplicate_stream_id(current, response)
    return TurnAccepted(event_id=event_id, stream_id=stream_id, duplicate=True)


@router.post("/turns", response_model=TurnAccepted)
async def ingest_turn(
    request: Request,
    response: Response,
    session: SessionDep,
    x_api_key: Annotated[str | None, Header()] = None,
) -> TurnAccepted:
    """Enqueue one inbound delivery for the binding the body names.

    Order is load-bearing, and every step of it earns its place:

    1. the size bound, before authentication and before JSON parsing, so an
       oversized body is refused without the server ever HMAC-verifying or
       deserializing it;
    2. the body, parsed next because the credential is scoped to the binding the
       body CLAIMS and a 422 costs no database work either;
    3. the credential's SIGNATURE, verified statelessly -- no row, no query. An
       attacker with no valid credential must not be able to make this route
       query the database, or a flood of small well-formed bodies exhausts the
       connection pool behind a 401;
    4. the binding row, loaded only for a caller that already authenticated;
    5. the claims bound to that row, and only then does an unknown pair earn a
       404 -- a caller with no credential learns nothing about which bindings
       exist.
    """

    settings = get_settings()
    raw = await read_bounded_body(
        request,
        settings.channel_turn_max_body_bytes,
        subject="channel turn body",
    )
    body = _parse_turn(raw)
    claims = _verify_credential(x_api_key)
    row = await _resolve_binding(session, body.kind, body.address)
    _authorize(claims, row)
    if row is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"no agent is bound to channel kind {body.kind!r} at address "
            f"{body.address!r}",
        )
    if not _route_is_configured(row):
        # The mint's refusal, on the ingress. Enqueuing here would hand the
        # worker a turn with nowhere to reply and no credential to reply with.
        raise _unroutable(body.kind, body.address)

    # One hash of the `delivery_id`, shared by the two names derived from it.
    digest = sha16(body.delivery_id)
    event_id = _event_id(row.id, digest)
    client: redis.Redis = request.app.state.valkey
    key = _claim_key(row.id, digest)
    owner = f"pending:{secrets.token_hex(16)}"

    # The generation, re-read at the LAST moment before the claim. The row above
    # was loaded at the top of the request, and `update_channel_binding` bumps the
    # generation on a rebind, so a credential revoked mid-request would otherwise
    # still enqueue against the binding it no longer names. Re-reading here
    # narrows that race from the whole request to the gap below; it does NOT
    # close it -- a rebind committing between this SELECT and the XADD still
    # lands, and closing that honestly needs a row lock held across the enqueue,
    # which phase 2 does not take (FU: revocation list).
    if claims is not None:
        current_generation = await session.scalar(
            select(AgentChannel.generation).where(AgentChannel.id == row.id)
        )
        if current_generation != claims.generation:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail=_AUTH_DETAIL)

    # Two attempts, not a loop: the second exists only for the narrow case where
    # the claim key expired between our failed `SET NX` and the `GET` that would
    # have named its owner. If that repeats, the honest answer is "someone is
    # mid-flight" -- never a second XADD.
    for _attempt in range(2):
        if await claim_delivery(
            client, key, owner, settings.channel_delivery_lease_s
        ):
            if not await take_backlog_slot(
                client,
                key_prefix=f"{_CLAIM_PREFIX}:backlog:{row.id}",
                limit=settings.channel_binding_backlog_limit,
                window_s=settings.channel_binding_backlog_window_s,
            ):
                # Over this binding's quota: give the claim back so the delivery
                # is not locked out for a lease, and tell the adapter to retry.
                # Metered per binding, so one compromised or runaway adapter
                # cannot fill the shared stream for every other tenant.
                await release_claim(client, key, owner)
                logger.warning(
                    "channel ingress refused event_id=%s kind=%s: binding backlog "
                    "quota of %d per %ds exceeded",
                    event_id,
                    row.kind,
                    settings.channel_binding_backlog_limit,
                    settings.channel_binding_backlog_window_s,
                )
                raise HTTPException(
                    status.HTTP_429_TOO_MANY_REQUESTS,
                    "too many new deliveries for this binding; retry later",
                    headers={
                        "Retry-After": str(settings.channel_binding_backlog_window_s)
                    },
                )
            # Minted and serialized only once this request holds both the claim
            # and a quota slot: an adapter retrying an already-enqueued delivery
            # is the steady state for an at-least-once ingress, and every one of
            # those requests answers from `event_id` alone and would have thrown
            # the payload away.
            turn = _mint_turn(row, body, event_id)
            enqueued, current = await enqueue_owned(
                client,
                key=key,
                stream=settings.runs_stream,
                owner=owner,
                payload=turn.model_dump_json(),
                payload_field=STREAM_PAYLOAD_FIELD,
                lease_s=settings.channel_delivery_lease_s,
            )
            if enqueued:
                logger.info(
                    "channel ingress enqueued event_id=%s stream_id=%s kind=%s",
                    event_id,
                    current,
                    row.kind,
                )
                return TurnAccepted(
                    event_id=event_id, stream_id=current, duplicate=False
                )
            # Branch (c): our lease was re-claimed while we were slow. The other
            # claimant owns the delivery; we do not enqueue on top of it.
            return _duplicate(event_id, current, response)
        held = await client.get(key)
        if held is not None:
            return _duplicate(event_id, _text(held), response)

    response.status_code = status.HTTP_202_ACCEPTED
    return TurnAccepted(event_id=event_id, stream_id=None, duplicate=True)

"""PROTOTYPE: agent-to-agent delegate calls (Draft ADR-0115, issue thread PR #1793).

**This is a demo prototype, not an implementation of ADR-0115.** The ADR is
Draft; per ADR-0085/ADR-0102 (`docs/adr/AGENTS.md`) a Draft authorizes no
implementation until a maintainer accepts it. This router exists on a
throw-away branch to produce a demo gif and deliberately cuts scope the real
ADR would need -- see `docs/demo/ADR-0115-PROTOTYPE-NOTES.md` for the full
list of documented deviations. Do not treat this as a template for the real
implementation without re-reading the ADR's Decision section end to end.

**What this reuses, on purpose.** The scoped-token pattern is ADR-0033's: a
new ``delegate`` scope, verified the same way ``routers/state.py`` verifies
``state``/``state.app`` (a third narrow exception to `apps/api/CLAUDE.md`'s
"every router keeps ``require_api_key``" rule, following the same precedent
the ``channels`` router already set as the second exception). Turn minting and
enqueue reuse ``delivery.py``'s claim/XADD helpers, the same machinery
``hooks.py``/``channels.py`` use -- this is not a new ingress mechanism, just a
third caller of an existing one.

**What this deliberately is NOT.** It does not add a message-lane
``TurnSource`` value (reuses ``WEBHOOK``), does not suspend the caller's
sandbox (the caller's turn ends normally; the reply arrives later as an
ordinary new turn on the same conversation), and does not carry a
bundle-declared allowlist or a call chain/depth on the wire (authorization is
a flat ``delegate_grants`` table, and depth is capped at 1 by refusing a call
whose OWN conversation id is already a delegate one).
"""

from __future__ import annotations

import logging
import secrets as pysecrets
import uuid
from datetime import UTC, datetime
from typing import Annotated

import redis.asyncio as redis
from aci_protocol import STREAM_PAYLOAD_FIELD, QueuedTurn, ReplyHandle, TurnSource
from fastapi import APIRouter, Depends, Header, HTTPException, Request, status
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from .. import crud, sandbox_token
from ..auth import require_api_key, verify_platform_key
from ..config import get_settings
from ..delivery import claim_delivery, enqueue_owned
from ..deps import SessionDep
from ..models import Agent, DelegationCallStatus
from ..schemas import (
    ChannelBindingWrite,
    DelegateCallDetailOut,
    DelegateCallIn,
    DelegateCallOut,
    DelegateCompleteIn,
    DelegateGrantIn,
    DelegateGrantOut,
    DelegateProgressIn,
)

logger = logging.getLogger(__name__)

# Mirrors STATE_SCOPE/STATE_APP_SCOPE in routers/state.py: a byte-identical
# string on both sides (this file and the worker's binding.py mint site) is the
# whole contract -- sandbox_token.py itself needs no change for a new scope.
DELEGATE_SCOPE = "delegate"

# The ReplyHandle.kind this prototype mints for a delegate-target turn. `kind`
# is an open vocabulary under ADR-0096 (no protocol change needed for a new
# value), and this is the one apps/worker/reply_sink.py's DelegationReplyAdapter
# is registered against.
DELEGATION_KIND = "delegation"

_CLAIM_PREFIX = "curie:delegate"


async def _load_agent(session: SessionDep, agent_id: uuid.UUID) -> Agent | None:
    # Annotated rather than returned bare: `session.scalar` is typed Any, and the
    # local annotation is how the rest of this package pins it (see `crud.py` and
    # `hooks._load_agent`).
    agent: Agent | None = await session.scalar(
        select(Agent).where(Agent.id == agent_id).options(selectinload(Agent.channel))
    )
    return agent


async def require_delegate_access(
    agent_id: uuid.UUID,
    x_api_key: Annotated[str | None, Header()] = None,
) -> None:
    """The caller-scoped route's auth: the platform key OR a ``delegate``-scoped
    sandbox token bound to this path's ``agent_id``. Modeled byte-for-byte on
    ``require_state_access`` (ADR-0033)."""

    if verify_platform_key(x_api_key):
        return
    if x_api_key is not None:
        api_key = get_settings().api_key
        if sandbox_token.verify(x_api_key, api_key, agent=str(agent_id), scope=DELEGATE_SCOPE):
            return
    raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="missing or invalid credential")


router = APIRouter(
    prefix="/agents",
    tags=["delegate"],
    dependencies=[Depends(require_delegate_access)],
)


@router.post("/{agent_id}/delegate/calls", response_model=DelegateCallOut, status_code=201)
async def create_call(
    request: Request,
    agent_id: uuid.UUID,
    body: DelegateCallIn,
    session: SessionDep,
) -> DelegateCallOut:
    """Mint a delegate call: caller ``agent_id`` asks ``body.target_agent`` to do
    something. Refuses (1) an unarmed pair -- default closed, ADR-0115 part 5 --
    and (2) a caller whose OWN conversation is already a delegate call, the
    prototype's stand-in for the real ADR's depth/cycle bound (part 6)."""

    caller = await _load_agent(session, agent_id)
    if caller is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "caller agent not found")

    target = await crud.get_agent_by_name(session, body.target_agent)
    if target is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"agent {body.target_agent!r} not found")

    grant = await crud.get_delegate_grant(
        session, caller_agent_id=caller.id, target_agent_id=target.id
    )
    if grant is None or not grant.armed:
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            f"{caller.name} is not armed to call {target.name}; an operator must "
            "arm this pair first (default closed, ADR-0115 part 5)",
        )

    if body.caller_conversation_id.startswith("delegate:"):
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            "a delegated turn may not itself delegate further (prototype depth "
            "cap of 1, standing in for ADR-0115 part 6's chain/depth bound)",
        )

    call = await crud.create_delegation_call(session, caller=caller, target=target, data=body)

    turn = QueuedTurn(
        event_id=f"delegate-{call.id}",
        conversation_id=f"delegate:{call.id}",
        # Not a person, and not the platform acting anonymously either: naming
        # the calling agent here is the one attribution fact this prototype
        # keeps (the ADR's fuller two-field caller/accountable-principal split
        # is cut -- see the prototype notes doc).
        author=f"agent:{caller.id}",
        text=body.message,
        # Reuses the job lane (a documented deviation -- ADR-0115 wants a
        # message-lane source, which needs a protocol codegen bump this
        # prototype skips). The target still runs as an ordinary turn on its
        # own conversation, credentials, and approval policy.
        source=TurnSource.WEBHOOK,
        reply_handle=ReplyHandle(
            kind=DELEGATION_KIND,
            channel=str(target.id),
            placeholder=None,
            endpoint=None,
            adapter=None,
        ),
        received_at=datetime.now(UTC).isoformat(),
    )

    settings = get_settings()
    key = f"{_CLAIM_PREFIX}:delivery:{call.id}"
    owner = f"pending:{pysecrets.token_hex(16)}"
    client: redis.Redis = request.app.state.valkey

    if not await claim_delivery(client, key, owner, settings.channel_delivery_lease_s):
        # call.id is a freshly minted uuid4, so a claim collision here would
        # mean the same call id was somehow enqueued twice; refusing loudly
        # beats silently answering "pending" for a turn that never mints.
        raise HTTPException(status.HTTP_409_CONFLICT, "delegate call id already claimed")
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
            "delegate call enqueued call_id=%s stream_id=%s caller=%s target=%s",
            call.id,
            current,
            caller.name,
            target.name,
        )
    return DelegateCallOut(id=call.id, status=call.status)


@router.get("/{agent_id}/delegate/calls", response_model=list[DelegateCallDetailOut])
async def list_calls(agent_id: uuid.UUID, session: SessionDep) -> list[DelegateCallDetailOut]:
    """Demo/ops convenience: every call this agent was caller or target of,
    newest first. Not part of the ADR's design."""

    calls = await crud.list_delegation_calls_for_agent(session, agent_id)
    return [DelegateCallDetailOut.model_validate(c) for c in calls]


@router.get("/{agent_id}/delegate/calls/{call_id}", response_model=DelegateCallDetailOut)
async def get_call(
    agent_id: uuid.UUID, call_id: uuid.UUID, session: SessionDep
) -> DelegateCallDetailOut:
    """Demo/ops convenience: inspect one call's current record, by either its
    caller or its target agent id. Not part of the ADR's design -- exists so
    the round trip can be checked without a direct DB connection."""

    call = await crud.get_delegation_call(session, call_id)
    if call is None or agent_id not in (call.caller_agent_id, call.target_agent_id):
        raise HTTPException(status.HTTP_404_NOT_FOUND, "delegate call not found")
    return DelegateCallDetailOut.model_validate(call)


async def _require_platform_key(x_api_key: Annotated[str | None, Header()] = None) -> None:
    """The worker calls back with the platform key, never a sandbox token --
    this is a platform-to-platform callback, not a sandbox-originated request."""
    if not verify_platform_key(x_api_key):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="missing or invalid API key")


@router.patch(
    "/{agent_id}/delegate/calls/{call_id}",
    status_code=204,
    dependencies=[Depends(_require_platform_key)],
)
async def progress_call(
    agent_id: uuid.UUID, call_id: uuid.UUID, body: DelegateProgressIn, session: SessionDep
) -> None:
    """Buffer the target's latest streamed reply text. Last-write-wins, no
    history -- this prototype only needs the FINAL text at completion time."""

    call = await crud.get_delegation_call(session, call_id)
    if call is None or call.target_agent_id != agent_id:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "delegate call not found")
    await crud.update_delegation_call_text(session, call, body.result_text)


@router.post(
    "/{agent_id}/delegate/calls/{call_id}/complete",
    dependencies=[Depends(_require_platform_key)],
)
async def complete_call(
    request: Request,
    agent_id: uuid.UUID,
    call_id: uuid.UUID,
    body: DelegateCompleteIn,
    session: SessionDep,
) -> dict[str, str]:
    """The target's turn settled. Mints the round-trip ``QueuedTurn`` back onto
    the caller's ORIGINAL conversation, using the reply route snapshotted at
    call time -- this is the one and only place a round-trip turn is minted,
    living in the API next to every other ingress mint site, never in the
    worker (the worker only ever calls back over HTTP with the platform key,
    the same pattern ``ApprovalClient`` already uses)."""

    call = await crud.get_delegation_call(session, call_id)
    if call is None or call.target_agent_id != agent_id:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "delegate call not found")
    if call.status != DelegationCallStatus.pending:
        return {"status": call.status}

    target = await crud.get_agent(session, agent_id)
    target_name = target.name if target is not None else str(agent_id)

    # TurnCompleted.outcome (channel_protocol.reply) is one of "delivered",
    # "dropped", "escalated", "awaiting-approval" -- NOT "completed". Only
    # "delivered" is treated as a real answer; "awaiting-approval" means the
    # target itself suspended on an approval gate, which this prototype does
    # not carry through (a documented cut -- see the prototype notes doc: the
    # demo's target agent is expected to have no approval gates of its own).
    delivered = body.outcome == "delivered"
    call = await crud.resolve_delegation_call(
        session,
        call,
        status=DelegationCallStatus.delivered if delivered else DelegationCallStatus.dropped,
    )
    if not delivered:
        logger.warning(
            "delegate call %s dropped: target turn outcome=%s (never delivered "
            "to the caller -- ADR-0099's treatment of a failed hook applies: a "
            "hard stop, not something the caller model can plan around)",
            call_id,
            body.outcome,
        )
        return {"status": call.status}

    turn = QueuedTurn(
        event_id=f"delegate-reply-{call.id}",
        conversation_id=call.caller_conversation_id,
        author=f"agent:{agent_id}",
        text=f"[reply from {target_name}] {call.result_text or ''}",
        source=TurnSource.WEBHOOK,
        reply_handle=ReplyHandle(
            kind=call.caller_reply_kind,
            channel=call.caller_reply_channel,
            placeholder=None,
            endpoint=call.caller_reply_endpoint,
            adapter=call.caller_reply_adapter,
        ),
        received_at=datetime.now(UTC).isoformat(),
    )
    settings = get_settings()
    key = f"{_CLAIM_PREFIX}:reply:{call.id}"
    owner = f"pending:{pysecrets.token_hex(16)}"
    client: redis.Redis = request.app.state.valkey
    if not await claim_delivery(client, key, owner, settings.channel_delivery_lease_s):
        raise HTTPException(status.HTTP_409_CONFLICT, "delegate reply already claimed")
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
        logger.info("delegate reply enqueued call_id=%s stream_id=%s", call.id, current)
    return {"status": call.status}


# --- operator arming, flat prefix, ordinary platform-key auth ----------------

grants_router = APIRouter(prefix="/delegate", tags=["delegate"])


@grants_router.post(
    "/grants", response_model=DelegateGrantOut, dependencies=[Depends(require_api_key)]
)
async def arm_grant(body: DelegateGrantIn, session: SessionDep) -> DelegateGrantOut:
    """Operator-only: arm (or disarm) ``caller_agent`` to call ``target_agent``.

    Arming a target for the first time also binds its channel to
    ``{kind: "delegation", address: <target agent id>}`` via the existing
    ``update_agent_binding`` -- no new binding code, since ``AgentChannel``
    already accepts any unregistered kind (ADR-0096's documented escape
    hatch). This REPLACES whatever channel the target held (an agent holds
    exactly one binding); the prototype's target agent is meant to be
    backend-only for this reason -- see the prototype notes doc.
    """

    caller = await crud.get_agent_by_name(session, body.caller_agent)
    if caller is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"agent {body.caller_agent!r} not found")
    target = await crud.get_agent_by_name(session, body.target_agent)
    if target is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"agent {body.target_agent!r} not found")

    if body.armed and target.channel.kind != DELEGATION_KIND:
        await crud.update_agent_binding(
            session,
            target,
            ChannelBindingWrite(
                kind=DELEGATION_KIND, address=str(target.id), endpoint=None, adapter=None
            ),
        )

    grant = await crud.upsert_delegate_grant(
        session, caller=caller, target=target, armed=body.armed
    )
    return DelegateGrantOut(
        caller_agent_id=grant.caller_agent_id,
        target_agent_id=grant.target_agent_id,
        armed=grant.armed,
    )

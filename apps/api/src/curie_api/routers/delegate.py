"""Agent-to-agent delegate calls (ADR-0115).

**What this reuses, on purpose.** The scoped-token pattern is ADR-0033's: a
``delegate`` scope, verified the same way ``routers/state.py`` verifies
``state``/``state.app`` -- a third narrow exception to `apps/api/CLAUDE.md`'s
"every router keeps ``require_api_key``" rule, following the same precedent
the ``channels`` router already set as the second exception. Turn minting and
enqueue reuse ``delivery.py``'s claim/XADD helpers, the same machinery
``hooks.py``/``channels.py`` use -- this is not a new ingress mechanism, just a
third caller of an existing one.

**The turn rides ``TurnSource.WEBHOOK``, deliberately, not a new value.**
``hooks.py``'s own docstring states the reason plainly: "``source=WEBHOOK``...
is what stops it steering a live conversation" (``source.is_job`` is the only
thing that predicate drives, per ``aci_protocol.TurnSource``). That is exactly
the safety property ADR-0115 part 2 needs -- a call must defer to an idle
target, never interrupt a live one -- and the kernel already gives it for free
to any job-lane turn via the existing bounded-retry reclaim (``kernel.py``'s
``ThreadBusyError`` path), no kernel change required. What WEBHOOK does NOT
give for free elsewhere -- a real caller identity, since ``hooks._mint_turn``
pins ``author`` to the platform -- is not a problem here, because this router
mints the turn directly and sets ``author``/``delegation.immediate_caller``
itself; it never goes through the hook ingress. A dedicated message-lane
source value (as ADR-0115's text originally sketched, pending ADR-0112) remains
a legitimate future taxonomy cleanup, not a safety gap this implementation is
missing.

**Depth is capped at 1.** A target may be CALLED, but may not itself delegate
further. See ``Settings.delegate_max_depth``'s docstring for why: tracing
``accountable_principal`` truthfully across a second hop needs the calling
agent's own sandbox to see its own inbound turn's attribution, which needs a
boot-env field threaded through the single-owner kernel claim path
(``apps/worker/CLAUDE.md``) that this change does not touch.
"""

from __future__ import annotations

import logging
import secrets as pysecrets
import uuid
from datetime import UTC, datetime
from typing import Annotated

import redis.asyncio as redis
from aci_protocol import (
    STREAM_PAYLOAD_FIELD,
    DelegationMeta,
    QueuedTurn,
    ReplyHandle,
    TurnSource,
)
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

# The ReplyHandle.kind minted for a delegate-target turn. `kind` is an open
# vocabulary under ADR-0096 (no protocol change needed for a new value), and
# this is the one apps/worker/reply_sink.py's DelegationReplyAdapter is
# registered against.
DELEGATION_KIND = "delegation"

_CLAIM_PREFIX = "curie:delegate"

# The conversation-id prefix this router mints for a target's own turn
# (`delegate:<call id>`), also read by runner/delegate.py's
# `is_delegate_target_boot` off CURIE_HISTORY_REF. Recognizing it here on an
# INCOMING call's `caller_conversation_id` is how the depth/cycle check finds
# the parent call to inherit chain/depth/accountable_principal from.
_DELEGATE_CONVERSATION_PREFIX = "delegate:"


def _parent_call_id(caller_conversation_id: str) -> uuid.UUID | None:
    """The parent call this request's caller is itself the target of, if any.

    ``None`` means an ordinary (non-nested) call: the caller's own conversation
    was not minted by this router, so there is no chain to inherit.
    """

    if not caller_conversation_id.startswith(_DELEGATE_CONVERSATION_PREFIX):
        return None
    raw = caller_conversation_id.removeprefix(_DELEGATE_CONVERSATION_PREFIX)
    try:
        return uuid.UUID(raw)
    except ValueError:
        return None


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
    something. Refuses (1) an unarmed pair -- default closed, ADR-0115 part 5
    -- and (2) a call that would revisit an agent already in the chain, or
    push the chain past ``Settings.delegate_max_depth`` (ADR-0115 part 6),
    recording that second kind of refusal as its own queryable call row."""

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

    immediate_caller = f"agent:{caller.id}"
    parent_chain: list[str] = []
    parent_depth = 0
    # v1 identity is payload-level agent identity (#1049 open): absent a
    # parent call to inherit from, the caller IS the accountable principal.
    accountable_principal = immediate_caller
    parent_call_id = _parent_call_id(body.caller_conversation_id)
    if parent_call_id is not None:
        parent = await crud.get_delegation_call(session, parent_call_id)
        if parent is not None:
            parent_chain = list(parent.chain)
            parent_depth = parent.depth
            accountable_principal = parent.accountable_principal

    chain = [*parent_chain, immediate_caller]
    depth = parent_depth + 1
    settings = get_settings()

    if f"agent:{target.id}" in parent_chain or immediate_caller in parent_chain:
        # A cycle: the target (or the caller itself, re-entering) already
        # appears earlier in this chain.
        await crud.create_delegation_call(
            session,
            caller=caller,
            target=target,
            data=body,
            immediate_caller=immediate_caller,
            accountable_principal=accountable_principal,
            chain=chain,
            depth=depth,
            status=DelegationCallStatus.refused,
        )
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            f"refused: {target.name} already appears in this call chain "
            "(ADR-0115 part 6 cycle bound)",
        )
    if depth > settings.delegate_max_depth:
        await crud.create_delegation_call(
            session,
            caller=caller,
            target=target,
            data=body,
            immediate_caller=immediate_caller,
            accountable_principal=accountable_principal,
            chain=chain,
            depth=depth,
            status=DelegationCallStatus.refused,
        )
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            f"refused: this call would reach depth {depth}, past the configured "
            f"maximum of {settings.delegate_max_depth} (ADR-0115 part 6)",
        )

    call = await crud.create_delegation_call(
        session,
        caller=caller,
        target=target,
        data=body,
        immediate_caller=immediate_caller,
        accountable_principal=accountable_principal,
        chain=chain,
        depth=depth,
    )

    turn = QueuedTurn(
        event_id=f"delegate-{call.id}",
        conversation_id=f"delegate:{call.id}",
        author=immediate_caller,
        text=body.message,
        # Job lane on purpose, not a cut corner -- see the module docstring:
        # `is_job` is what stops this from steering the target's live session.
        source=TurnSource.WEBHOOK,
        reply_handle=ReplyHandle(
            kind=DELEGATION_KIND,
            channel=str(target.id),
            placeholder=None,
            endpoint=None,
            adapter=None,
        ),
        received_at=datetime.now(UTC).isoformat(),
        delegation=DelegationMeta(
            immediate_caller=immediate_caller,
            accountable_principal=accountable_principal,
            chain=chain,
            depth=depth,
        ),
    )

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
    history -- only the FINAL text at completion time is delivered."""

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

    # TurnCompleted.outcome (channel_protocol.reply) is one of "delivered",
    # "dropped", "escalated", "awaiting-approval" -- NOT "completed".
    # "awaiting-approval" is NOT terminal: it means the target's turn hit one
    # of ITS OWN gated tools and suspended exactly as a user-started turn does
    # (ADR-0115 part 4 -- a delegation never satisfies an approval gate, so the
    # target's own policy runs unchanged). That turn resumes normally once a
    # human decides, through the ordinary approval-resume path
    # (`resumequeue.py`), and fires ANOTHER completion event when it truly
    # finishes. Resolving the call here would drop that eventual answer on the
    # floor, so this call is left `pending` and untouched.
    if body.outcome == "awaiting-approval":
        return {"status": call.status}

    target = await crud.get_agent(session, agent_id)
    target_name = target.name if target is not None else str(agent_id)

    # Only "delivered" is a real answer; "dropped" and "escalated" are
    # terminal failures with nothing more coming.
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
    hatch). This REPLACES whatever channel the target held: an agent holds
    exactly one binding today (issue #1525, not yet built), so a delegate
    target gives up its previous channel the moment it is armed. A
    delegate-callable agent is therefore backend-only for now, a real
    platform constraint rather than something ADR-0115 chose.

    This endpoint does not cross-check ``caller_agent``'s bundle-declared
    ``PluginManifest.delegatesTo`` before arming -- matching this codebase's
    existing precedent for ``Agent.approval_routes``, where an operator-set
    value is likewise not cross-validated against the bundle at write time
    (enforcement, where it exists at all, happens live at use time instead).
    Declaration and arming are independent today: arming a pair the bundle
    never declared succeeds. Tightening that is a follow-up, not a gap this
    change silently papers over.
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

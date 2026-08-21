"""Generic HMAC-verified inbound hooks (ADR-0079 decision 1, issue #269).

``POST /hooks/{agent_id}/{hook}`` turns an external event into a queued turn on
the same stream a Slack mention feeds. No new execution machinery: the turn walks
the identical consumer -> kernel -> claim path, and the only thing that marks it
out is ``source=WEBHOOK``, which is what stops it steering a live conversation.

**Why the API and not the dispatcher.** Settled by ADR-0079: the dispatcher is
Socket-Mode only with no inbound HTTP server, while this service already owns
HTTP ingress, already verifies an HMAC webhook and already produces to a Valkey
stream.

**Authentication is the signature, not the platform key**, exactly as the GitHub
webhook does, so this router sits outside the ``X-API-Key`` dependency. The
secret is per agent and derived rather than stored (see ``hook_secret``).

**One ordering difference from ``/channels/turns``, stated because it is a real
cost.** That route verifies its credential statelessly, before any query, so an
attacker with no credential cannot make it touch the database. This route cannot:
the secret is per agent, so the agent row must be read before the signature can
be checked at all. The read is a single primary-key lookup behind an already
enforced body bound, and it is the same exposure ``/github/webhook`` carries, but
it is not the stronger property its sibling has.

**A delivery id is required, not optional.** An at-least-once ingress without a
stable upstream id cannot deduplicate, and both alternatives are worse than
refusing: a per-request id disables idempotency silently, and a content digest
makes an identical payload undeliverable forever, because a delivery receipt
deliberately never expires. Refusing names the header and is fixed in the
upstream's configuration.
"""

from __future__ import annotations

import logging
import re
import secrets as pysecrets
import uuid
from datetime import UTC, datetime
from typing import Annotated

import redis.asyncio as redis
from aci_protocol import STREAM_PAYLOAD_FIELD, QueuedTurn, ReplyHandle, TurnSource
from fastapi import APIRouter, Header, HTTPException, Request, Response, status
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from .. import hook_signing
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
from ..graveyardwatcher import _text
from ..models import Agent, AgentChannel
from ..wirebody import read_bounded_body

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/hooks", tags=["hooks"])

# The claim namespace for hook deliveries. Deliberately NOT the channel ingress
# prefix: that one is keyed by binding row id and this one by agent id, and two
# different id spaces under one prefix could collide and swallow each other's
# turns.
_CLAIM_PREFIX = "curie:hook"

# The one detail string every hook auth failure returns. Identical for "no
# signature", "bad signature" and "no such agent", so a caller cannot use the
# route to discover which agent ids exist.
_AUTH_DETAIL = "missing or invalid signature"

# A hook name is an operator-chosen label that ends up inside Valkey key names
# and inside the conversation id, so it is constrained rather than trusted: no
# separators, no unbounded length, nothing that could make two distinct hooks
# build one key.
_HOOK_NAME = re.compile(r"^[a-z0-9][a-z0-9._-]{0,62}$")

# The header an upstream names its delivery with.
_DELIVERY_HEADER = "X-Curie-Delivery-Id"


class HookAccepted(BaseModel):
    """The hook receipt. ``duplicate`` says whether THIS request enqueued.

    ``stream_id`` is None only while another request holds the claim and has not
    enqueued yet (the 202 case).
    """

    event_id: str
    stream_id: str | None
    duplicate: bool


def _conversation_id(agent_id: uuid.UUID, hook: str) -> str:
    """The thread every firing of one hook shares.

    Per HOOK rather than per delivery, and the choice is load-bearing in two
    directions. Per delivery would claim a fresh sandbox for every event, and two
    rapid firings would run concurrently with no ordering at all. Sharing one
    thread instead means a hook reuses its session and a second firing arriving
    mid-run defers until the first finishes, which is exactly ADR-0079's "jobs
    are outputs, not steering inputs" applied to a hook competing with itself.

    It is also disjoint from the agent's Slack thread ids, so a hook can never
    land in the middle of a human conversation.

    Args:
        agent_id: The agent this hook belongs to.
        hook: The validated hook name.

    Returns:
        The conversation key.
    """

    return f"hook:{agent_id}:{hook}"


def _hook_text(hook: str, body: bytes) -> str:
    """The turn text a hook delivery becomes.

    An INTERIM shape, and named as one. ADR-0079 deliberately left the payload
    mapping open and Draft ADR-0099's trigger declarations are where a bundle
    gets to say how its own hook renders; until that lands, handing the model the
    raw document under a line naming the hook is the honest minimum. It invents
    no format for a bundle to depend on, so replacing it later breaks nothing.

    Args:
        hook: The validated hook name.
        body: The raw request body.

    Returns:
        The turn's text.
    """

    payload = body.decode("utf-8", errors="replace")
    return f"Inbound hook `{hook}` fired with this payload:\n\n{payload}"


async def _load_agent(session: SessionDep, agent_id: uuid.UUID) -> Agent | None:
    """Load the agent and every surface binding, or None."""

    # Annotated rather than returned bare: `session.scalar` is typed Any, and the
    # local annotation is how the rest of this package pins it (see `crud.py` and
    # `channels._resolve_binding`).
    agent: Agent | None = await session.scalar(
        select(Agent).where(Agent.id == agent_id).options(selectinload(Agent.channels))
    )
    return agent


def _mint_turn(
    agent: Agent,
    binding: AgentChannel,
    hook: str,
    event_id: str,
    body: bytes,
) -> QueuedTurn:
    """Build the ``QueuedTurn`` a verified hook delivery becomes.

    The reply route comes wholly from the agent's binding row, never from the
    request: an upstream that could name its own endpoint would be pointing the
    platform's authenticated egress wherever it liked.

    ``placeholder`` is None because nothing was preposted -- this is precisely the
    placeholder-less turn ADR-0079's kernel path exists for, so the first reply
    delivery creates its own message.

    Args:
        agent: The agent, with its channel binding loaded.
        hook: The validated hook name.
        event_id: This delivery's deterministic event id.
        body: The raw request body.

    Returns:
        The queued turn.
    """

    return QueuedTurn(
        event_id=event_id,
        conversation_id=_conversation_id(agent.id, hook),
        # The author is the platform, not a person: no human sent this, and
        # putting an upstream-supplied identity here would let a hook impersonate
        # one to anything downstream that reads the field.
        author=f"hook:{hook}",
        text=_hook_text(hook, body),
        source=TurnSource.WEBHOOK,
        reply_handle=ReplyHandle(
            kind=binding.kind,
            channel=binding.address,
            placeholder=None,
            endpoint=binding.endpoint,
            adapter=binding.adapter,
        ),
        received_at=datetime.now(UTC).isoformat(),
    )


@router.post("/{agent_id}/{hook}", response_model=HookAccepted)
async def ingest_hook(
    request: Request,
    response: Response,
    session: SessionDep,
    agent_id: uuid.UUID,
    hook: str,
    kind: str | None = None,
    address: str | None = None,
    x_curie_signature_256: Annotated[str | None, Header()] = None,
    x_curie_delivery_id: Annotated[str | None, Header()] = None,
) -> HookAccepted:
    """Verify one hook delivery and enqueue it as a turn.

    Order, and why each step sits where it does:

    1. the hook NAME, validated before anything else, because it is about to be
       used to build key names;
    2. the size bound, before the signature is computed, so an oversized body is
       refused without the server ever HMAC-ing it;
    3. the agent row, which unavoidably precedes authentication here (see the
       module docstring);
    4. the SIGNATURE over the raw body;
    5. the delivery id, checked after authentication so an unsigned caller learns
       nothing about what this route wants;
    6. routability, then the claim, quota and enqueue.
    """

    if not _HOOK_NAME.match(hook):
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            "hook name must be 1-63 characters of lowercase letters, digits, dot, "
            "dash or underscore",
        )

    settings = get_settings()
    raw = await read_bounded_body(
        request, settings.hook_max_body_bytes, subject="hook body"
    )

    agent = await _load_agent(session, agent_id)
    # One refusal for "no such agent" and for "bad signature": a caller that can
    # tell them apart can enumerate agent ids through a route that answers 401.
    if agent is None:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail=_AUTH_DETAIL)
    secret = hook_signing.derive(
        settings.api_key, agent_id=str(agent.id), generation=agent.hook_generation
    )
    if not hook_signing.verify(secret, raw, x_curie_signature_256):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail=_AUTH_DETAIL)

    if not x_curie_delivery_id:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            f"{_DELIVERY_HEADER} is required: this ingress is at-least-once, so a "
            "stable upstream id is what keeps a retried delivery from running the "
            "agent twice",
        )

    if (kind is None) != (address is None):
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            "hook reply surface requires both kind and address",
        )
    if kind is None:
        if len(agent.channels) != 1:
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                "this agent has multiple surfaces; select the hook reply surface "
                "with both kind and address query parameters",
            )
        binding = agent.channels[0]
    else:
        selected = next(
            (
                candidate
                for candidate in agent.channels
                if candidate.kind == kind and candidate.address == address
            ),
            None,
        )
        if selected is None:
            raise HTTPException(
                status.HTTP_404_NOT_FOUND,
                "this agent has no binding for the selected kind and address",
            )
        binding = selected

    # No unbound-agent branch, deliberately. `AgentCreate.channel` is required and
    # `crud.update_agent_binding` mutates the row in place rather than clearing
    # it, so an agent with no binding is not a reachable state and a branch for it
    # would be speculative code guarding nothing. `test_hooks.py` pins that
    # invariant, so if a future unbind path makes it reachable, that test fails
    # here rather than this route silently minting a turn with no reply route.
    digest = sha16(x_curie_delivery_id)
    # Namespaced by agent AND hook, so two hooks on one agent cannot swallow each
    # other's deliveries when an upstream reuses its id space across them.
    event_id = f"hook-{agent.id}-{hook}-{digest}"
    key = f"{_CLAIM_PREFIX}:delivery:{agent.id}:{hook}:{digest}"
    owner = f"pending:{pysecrets.token_hex(16)}"
    client: redis.Redis = request.app.state.valkey

    # Two attempts, not a loop: the second exists only for the narrow case where
    # the claim key expired between our failed `SET NX` and the `GET` that would
    # have named its owner.
    for _attempt in range(2):
        if await claim_delivery(client, key, owner, settings.channel_delivery_lease_s):
            if not await take_backlog_slot(
                client,
                key_prefix=f"{_CLAIM_PREFIX}:backlog:{agent.id}",
                limit=settings.hook_backlog_limit,
                window_s=settings.hook_backlog_window_s,
            ):
                # Metered per AGENT, not per hook: the thing worth bounding is how
                # much work one agent's upstreams can create, and per-hook quotas
                # would let a source multiply its allowance by inventing names.
                await release_claim(client, key, owner)
                logger.warning(
                    "hook ingress refused event_id=%s: agent backlog quota of %d "
                    "per %ds exceeded",
                    event_id,
                    settings.hook_backlog_limit,
                    settings.hook_backlog_window_s,
                )
                raise HTTPException(
                    status.HTTP_429_TOO_MANY_REQUESTS,
                    "too many new hook deliveries for this agent; retry later",
                    headers={"Retry-After": str(settings.hook_backlog_window_s)},
                )
            turn = _mint_turn(agent, binding, hook, event_id, raw)
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
                    "hook ingress enqueued event_id=%s stream_id=%s hook=%s",
                    event_id,
                    current,
                    hook,
                )
                return HookAccepted(
                    event_id=event_id, stream_id=current, duplicate=False
                )
            return HookAccepted(
                event_id=event_id,
                stream_id=duplicate_stream_id(current, response),
                duplicate=True,
            )
        held = await client.get(key)
        if held is not None:
            return HookAccepted(
                event_id=event_id,
                stream_id=duplicate_stream_id(_text(held), response),
                duplicate=True,
            )

    # Both attempts found the key absent after failing to claim it. Someone is
    # mid-flight; answering "come back" is honest and never a second XADD.
    response.status_code = status.HTTP_202_ACCEPTED
    return HookAccepted(event_id=event_id, stream_id=None, duplicate=True)

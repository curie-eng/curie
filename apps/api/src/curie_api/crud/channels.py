"""Database access for channels."""

import hashlib
import uuid

from aci_protocol.turn import SLACK_KIND, matching_routes, route_identity
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from curie_api.schemas.channels import ChannelBindingPatch, ChannelBindingWrite

from ..models import Agent, AgentChannel


class AmbiguousRoute(RuntimeError):
    """Raised when an omitted non-Slack adapter selects several routes on one
    pair, which migration 0070's triple key allows. Never resolved by picking
    one: every caller answers it.
    """


class RoutelessPairShared(RuntimeError):
    """A route-less non-Slack binding and another agent's route on one pair.

    Raised by `refuse_routeless_pair_sharing`; its message is the 409 detail.
    """


async def lock_agent_bindings(session: AsyncSession, agent_id: uuid.UUID) -> list[AgentChannel]:
    """`SELECT ... FOR UPDATE` the agent's WHOLE binding set, in route order.

    Ordered by the whole route `(kind, adapter, address)` (ADR-0168 decision
    3), so the lock order stays total when several identities share a pair.

    Every mutating binding handler opens with this, and then picks its target
    out of the returned list rather than issuing a second, unlocked query --
    which is what makes the lock load-bearing instead of decorative.

    Without it the last-binding guard is unsound: an agent with two bindings and
    two concurrent DELETEs of DIFFERENT pairs has both requests read count=2,
    both pass the guard, and the agent lands at ZERO bindings -- deployed,
    healthy-looking, answering nothing (#38). Under the lock the second delete
    re-reads count=1 and conflicts. The lock also serializes `generation += 1`
    into an increment instead of a lost update.

    `populate_existing` is load-bearing: the handler has already loaded the
    agent (for its 404), so its bindings are in the session's identity map, and
    a plain locking SELECT would hand those STALE objects back -- the row would
    be locked while the generation the caller compares against came from before
    the winner's commit.

    Known and accepted conservatism: `FOR UPDATE` locks rows that exist; it does
    not block a concurrent INSERT. A DELETE racing an ADD may therefore 409 as
    "last binding" even though a second binding commits moments later. That
    direction is safe (a retry succeeds) and is cheaper than the predicate lock
    that would close it -- it is accepted, not overlooked.
    """

    result = await session.scalars(
        select(AgentChannel)
        .where(AgentChannel.agent_id == agent_id)
        .order_by(AgentChannel.kind, AgentChannel.adapter, AgentChannel.address)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    return list(result)


async def agent_id_for_route(
    session: AsyncSession, kind: str, adapter: str | None, address: str
) -> uuid.UUID | None:
    """Which agent holds this ROUTE -- `(kind, adapter, address)` -- if any.

    Named rather than inlined at its call sites: it answers the question a
    binding write's 409 has to answer accurately -- is the duplicate THIS
    agent's or another's -- and an inline `select` there reads as an incidental
    query the next reader deletes. Replaces `agent_id_for_pair` (ADR-0168
    decision 3): the pair alone no longer names the row uniquely once several
    identities can share one `(kind, address)`.

    Selects the rows on `(kind, address)` and narrows to `adapter`'s RESOLVED
    identity through `route_identity`, the one rule every reader compares
    identities by. `agent_channels_route_key` holds at most one row per
    resolved route, so the first match is the only one.
    """

    wanted = route_identity(kind, adapter)
    result = await session.execute(
        select(AgentChannel.agent_id, AgentChannel.adapter).where(
            AgentChannel.kind == kind, AgentChannel.address == address
        )
    )
    for owner_id, stored_adapter in result.all():
        if route_identity(kind, stored_adapter) == wanted:
            owner: uuid.UUID = owner_id
            return owner
    return None


def _pair_lock_keys(kind: str, address: str) -> tuple[int, int]:
    digest = hashlib.sha256(f"curie-route-pair:{kind}:{address}".encode()).digest()
    return (
        int.from_bytes(digest[:4], "big", signed=True),
        int.from_bytes(digest[4:8], "big", signed=True),
    )


async def refuse_routeless_pair_sharing(
    session: AsyncSession,
    agent_id: uuid.UUID | None,
    kind: str,
    address: str,
    adapter: str | None,
) -> None:
    """Refuse a non-Slack route that would share its pair with a route-less row
    of ANOTHER agent (ADR-0168 decision 3).

    A route-less binding's turn names no adapter, and an omitted non-Slack
    adapter selects every route on the pair, so a route-less row beside another
    agent's route makes that turn's agent a guess, which is #38's misroute.
    `agent_channels_route_key` cannot say this: under NULLS NOT DISTINCT a
    NULL adapter and a named one are different keys. So the write paths keep
    0023's exclusivity for the route-less case, in both orders. Two named
    adapters on one pair stay legal, and `agent_id`'s own rows are not
    counted: one agent's rows are one deployment, and the per-agent readers
    already answer their ambiguity. Slack never stores a NULL adapter.

    Takes a transaction-scoped advisory lock on the pair first, so two writers
    racing onto one pair from opposite sides serialize and the second sees the
    first's committed row. The caller holds it to its commit. Raises
    `RoutelessPairShared`.
    """

    if kind == SLACK_KIND:
        return
    classid, objid = _pair_lock_keys(kind, address)
    await session.execute(
        text("SELECT pg_advisory_xact_lock(CAST(:classid AS integer), CAST(:objid AS integer))"),
        {"classid": classid, "objid": objid},
    )
    others = select(AgentChannel.id).where(
        AgentChannel.kind == kind, AgentChannel.address == address
    )
    if agent_id is not None:
        others = others.where(AgentChannel.agent_id != agent_id)
    if adapter is None:
        routed = await session.scalar(others.where(AgentChannel.adapter.is_not(None)).limit(1))
        if routed is not None:
            raise RoutelessPairShared(
                f"another agent holds a route on {kind}:{address}; a binding with no "
                "adapter answers every route on that pair, so it would take that agent's "
                "turns. Bind this one with its own endpoint and adapter, move or delete "
                "the other agent, or pick another address"
            )
        return
    routeless = await session.scalar(others.where(AgentChannel.adapter.is_(None)).limit(1))
    if routeless is not None:
        raise RoutelessPairShared(
            f"another agent is bound to {kind}:{address} with no adapter, which answers "
            "every route on that pair; give that binding its own endpoint and adapter "
            "first, move or delete the other agent, or pick another address"
        )


async def agent_holds_channel_pair(
    session: AsyncSession, agent_id: uuid.UUID, kind: str, address: str
) -> bool:
    """Does THIS agent hold a row on `(kind, address)`, under any identity?

    State is scoped to the agent across its identities, and
    `agent_channels_route_key` lets two agents hold one pair under two
    identities, so "who holds the pair" has no single answer. This asks the
    narrower thing every caller here needs, filtered on `agent_id` in the
    query itself.
    """

    held = await session.scalar(
        select(AgentChannel.id)
        .where(
            AgentChannel.agent_id == agent_id,
            AgentChannel.kind == kind,
            AgentChannel.address == address,
        )
        .limit(1)
    )
    return held is not None


def matching_bindings(
    bindings: list[AgentChannel], kind: str, address: str, adapter: str | None
) -> list[AgentChannel]:
    """The rows in ``bindings`` that `(kind, address, adapter)` selects.

    A thin, name-preserving wrapper over `aci_protocol.turn.matching_routes`
    (ADR-0168 decision 3), the one matching rule shared by every reader that
    has to answer "is this the same route" -- whether it already holds the
    candidate rows (`routers/hooks.py`'s preloaded `agent.channels`,
    `routers/agents.py`'s locked per-agent set) or fetches them fresh
    (`binding_for_route`, below).
    """

    return matching_routes(bindings, kind, address, adapter)


async def binding_for_route(
    session: AsyncSession,
    kind: str,
    adapter: str | None,
    address: str,
    *,
    agent_id: uuid.UUID | None = None,
    for_update: bool = False,
) -> AgentChannel | None:
    """The single binding row the route `(kind, adapter, address)` names, or None.

    Narrows in SQL to the resolved identity whenever there is one (every Slack
    route; a non-Slack route that names its adapter) and, with `agent_id`, to
    that agent -- so `for_update` locks only the named route. The shared rule
    (`matching_bindings`) still runs over the result, so this function and
    every in-memory caller of it agree on what counts as the same route.
    Raises `AmbiguousRoute` when an omitted non-Slack adapter still selects
    several rows; every caller answers that explicitly (ADR-0168 decision 3).

    `for_update` takes the same row lock `lock_agent_bindings` takes, with
    `populate_existing` for the same reason: a caller already holding this row
    in its identity map (loaded for an earlier check) must see the fresh,
    locked version rather than a stale one from before a concurrent winner's
    commit.
    """

    stmt = select(AgentChannel).where(AgentChannel.kind == kind, AgentChannel.address == address)
    wanted = route_identity(kind, adapter)
    if wanted is not None:
        stmt = stmt.where(AgentChannel.adapter == wanted)
    if agent_id is not None:
        stmt = stmt.where(AgentChannel.agent_id == agent_id)
    if for_update:
        stmt = stmt.with_for_update().execution_options(populate_existing=True)
    rows = list(await session.scalars(stmt))
    matches = matching_bindings(rows, kind, address, adapter)
    if len(matches) > 1:
        raise AmbiguousRoute(
            f"{len(matches)} routes are bound to {kind}:{address}; pass adapter to name one"
        )
    return matches[0] if matches else None


async def update_channel_binding(
    session: AsyncSession, binding: AgentChannel, channel: ChannelBindingPatch
) -> AgentChannel:
    """Move ONE binding row to a new kind/address (ADR-0096, #1459; ADR-0118).

    Mutated IN PLACE rather than replaced: assigning a fresh row would make the
    insert of the replacement race the delete of the original inside one flush,
    tripping `agent_channels_route_key` on a move that is perfectly legal.

    That in-place mutation is exactly why `generation` exists (ADR-0096 D5): the
    row id is a stable identity, so a credential minted against this binding
    before the move stays pointed at the row afterwards and would follow it to
    its NEW owner. The generation is what makes the rebind observable to that
    credential. It is bumped UNCONDITIONALLY on every write to the ROUTE through
    this function, including one whose values are identical -- an operator
    re-asserting a binding is the "I think something is wrong with this route"
    gesture that should invalidate outstanding credentials, and guarding the
    bump on a value change would leave that case silently valid. `POST
    /channels/token` is the sibling bump: a remint increments the same counter
    so rotation revokes (#2379). Not every binding write bumps it: editing the
    caller list (`set_allowed_callers`) deliberately does not (ADR 0175
    decision 4).

    FLUSHES rather than commits, so the caller can run it inside a SAVEPOINT:
    the unique violation this raises has to be recoverable without discarding
    the outer transaction's `FOR UPDATE` locks.
    """

    previous_kind = binding.kind
    binding.kind = channel.kind
    binding.address = channel.address
    # The reply route moves WITH the pair (ADR-0096 phase 2): a move that
    # re-points the pair and leaves the old endpoint/adapter behind would send
    # the new route's replies to the previous adapter, authenticated as it. This
    # is also the cutover's step 10 -- bind first, move the route in later.
    # Omitting both route fields preserves the stored route only within one
    # kind: `agent_channels_route_ck` gives Slack and every other kind different
    # route shapes, so a move across that line takes the new kind's.
    endpoint_sent = "endpoint" in channel.model_fields_set
    adapter_sent = "adapter" in channel.model_fields_set
    if endpoint_sent:
        binding.endpoint = channel.endpoint
        binding.adapter = channel.adapter
    elif channel.kind == SLACK_KIND and (adapter_sent or previous_kind != SLACK_KIND):
        # A Slack route is its identity with no endpoint (ADR-0168 decision 3):
        # naming one, or arriving from another kind, takes that shape.
        binding.adapter = channel.adapter
        binding.endpoint = None
    elif previous_kind == SLACK_KIND and channel.kind != SLACK_KIND:
        # A Slack identity is no route for another kind: route-less until set.
        binding.adapter = None
        binding.endpoint = None
    binding.generation += 1
    await session.flush()
    return binding


async def add_channel_binding(
    session: AsyncSession, agent_id: uuid.UUID, channel: ChannelBindingWrite
) -> AgentChannel:
    """Append a binding to an agent (ADR-0118). Appends; never moves.

    A new row, so its `generation` starts at 0 and no credential can exist for
    it yet. Flushes for the same savepoint reason as `update_channel_binding`.
    """

    binding = AgentChannel(
        agent_id=agent_id,
        # The agent's tenant, read inside the INSERT so there is no
        # read-then-write window (ADR 0166 decision 3).
        tenant_id=select(Agent.tenant_id).where(Agent.id == agent_id).scalar_subquery(),
        kind=channel.kind,
        address=channel.address,
        endpoint=channel.endpoint,
        adapter=channel.adapter,
    )
    session.add(binding)
    await session.flush()
    return binding


async def delete_channel_binding(session: AsyncSession, binding: AgentChannel) -> None:
    """Remove one binding row, after the caller proved under the lock that it is
    not the agent's last one.

    Deleting the row invalidates its outstanding channel tokens by construction:
    a `chn` claim names `channel_id`, and the id no longer resolves. The
    siblings' tokens are untouched, because the counters and ids are per-row.
    """

    await session.delete(binding)
    await session.flush()


async def any_binding_restricted(session: AsyncSession) -> bool:
    """Whether any binding on this install carries a caller list (ADR 0175).

    The install-wide half of the admission answer. One `EXISTS` over a
    nullable column: a binding table holds a handful of rows per agent, so the
    scan costs less than the round trip that carries it.
    """

    found = await session.scalar(
        select(AgentChannel.id).where(AgentChannel.allowed_callers.is_not(None)).limit(1)
    )
    return found is not None


async def set_allowed_callers(
    session: AsyncSession, binding: AgentChannel, allowed_callers: list[str] | None
) -> AgentChannel:
    """Replace ONE binding's caller list, leaving its generation alone (ADR 0175).

    The one writer of `allowed_callers`. Who may use a route is a separate
    question from the route itself (decision 4), so this does not bump
    `generation`: an adapter's `chn` token is issued for a generation (#2379),
    and revoking it on every list edit would take an inbox offline each time an
    operator adds a person. The caller has already validated the list against
    the binding's kind (`schemas.channels.validate_allowed_callers`) under the binding
    lock, so this only stores it.

    Flushes rather than commits, like the other binding writers, so the caller
    decides when the transaction ends.
    """

    binding.allowed_callers = allowed_callers
    await session.flush()
    return binding


async def existing_channel_binding_ids(
    session: AsyncSession, binding_ids: frozenset[uuid.UUID]
) -> frozenset[uuid.UUID]:
    """The subset of ``binding_ids`` that still name an ``agent_channels`` row."""

    if not binding_ids:
        return frozenset()
    result = await session.scalars(select(AgentChannel.id).where(AgentChannel.id.in_(binding_ids)))
    return frozenset(result)

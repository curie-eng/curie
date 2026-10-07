"""Database access for approvals."""

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from aci_protocol import ApprovalRequest
from aci_protocol.turn import route_identity
from sqlalchemy import (
    case,
    func,
    literal,
    or_,
    select,
    tuple_,
    update,
)
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from ..approvers import card_on_requesting_surface
from ..models import (
    Agent,
    AgentChannel,
    Approval,
    ApprovalAuditEntry,
    ApprovalStatus,
    ExecutionRequest,
    Publication,
)
from ..resumequeue import parse_resume_event_id
from .agents import get_agent
from .errors import PublicationSettlementConflict
from .publication_queries import get_publication_by_approval

# Purposes whose outcome no resumed model turn reports, so no wake is ever owed:
# ``publication`` (the worker reports it) and ``remediation`` (the platform
# executes the bound call, AUTOMATED-REMEDIATION-16). The resolution and expiry
# compare and sets mark them resumed, and resume reconciliation skips them.
NO_WAKE_PURPOSES = ("publication", "remediation")


def _no_wake_resumed_at() -> Any:
    """``resumed_at`` for a settling CAS: now for a no-wake purpose, else unchanged."""

    return case(
        (Approval.purpose.in_(NO_WAKE_PURPOSES), func.now()), else_=Approval.resumed_at
    )


async def create_approval(
    session: AsyncSession,
    data: "ApprovalRequest",
    *,
    traceparent: str | None = None,
) -> Approval:
    """Insert a pending approval. Raises IntegrityError on a dedupe_key replay;
    the router maps that to the existing record (idempotent creation)."""

    expires_at = None
    if data.expires_in_seconds is not None:
        # Naive UTC, matching the DateTime columns (server_default func.now()
        # stores naive timestamps in the session timezone, UTC in this stack).
        expires_at = datetime.now(UTC).replace(tzinfo=None) + timedelta(
            seconds=data.expires_in_seconds
        )
    approval = Approval(
        agent_id=data.agent_id,
        conversation_id=data.conversation_id,
        author=data.author,
        summary=data.summary,
        # The durable twin of the turn's routing pair and egress selector
        # (ADR-0096 phase 2). Persisted from the request, never re-derived from
        # `agent_channels`: an operator may re-bind the address between
        # suspension and resume, and these are facts about the original turn.
        reply_kind=data.reply_kind,
        reply_channel=data.reply_channel,
        reply_placeholder=data.reply_placeholder,
        reply_endpoint=data.reply_endpoint,
        reply_adapter=data.reply_adapter,
        dedupe_key=data.dedupe_key,
        traceparent=traceparent,
        route=data.route,
        card_channel=data.card_channel,
        gate_kind=data.gate_kind,
        granted_tool=data.granted_tool,
        granted_arguments=data.granted_arguments,
        expires_at=expires_at,
    )
    session.add(approval)
    await session.commit()
    await session.refresh(approval)
    return approval


async def get_approval(session: AsyncSession, approval_id: uuid.UUID) -> Approval | None:
    return await session.get(Approval, approval_id)


async def get_approval_route_binding(session: AsyncSession, approval: Approval) -> Any:
    """The route binding governing ``approval``, read fresh at resolve time
    (#420), or None when there is none to read.

    Read fresh rather than snapshotted at creation: approvals pend for hours to
    days, and evaluating against current policy means removing someone from the
    approver group revokes them immediately instead of leaving them able to
    resolve yesterday's stale request.

    None still covers every legitimate miss -- a generic approval with no agent
    (``agent_id`` is nullable by design), an approval with no route, an agent
    with no bindings, a route the map does not bind -- but it no longer means
    one thing. ADR-0123 makes the selector split a None on whether the approval
    NAMED a route: a routeless approval keeps the AC4 zero-setup channel
    membership, while a routed approval with no binding is refused outright,
    because a route the operator narrowed must not be readable as one they never
    narrowed.

    This function deliberately still returns a bare None and does not say which
    of the four misses happened. The selector needs only ``approval.route`` to
    make that split, and a richer return type is not something ADR-0123 asks
    for. Note the guard below is ``not approval.route``, so a ``route=""``
    approval is routeless here; the selector keys on the same truthiness so the
    two files cannot disagree about what "named a route" means.

    A present-but-non-dict value is NOT one of those misses, so it is returned
    raw (the JSONB value can be anything) rather than coerced to None: the
    selector fails a malformed binding closed, the same as a malformed
    ``approvers`` block, instead of widening it to card-channel membership.
    """

    if approval.agent_id is None or not approval.route:
        return None
    agent = await get_agent(session, approval.agent_id)
    if agent is None or not isinstance(agent.approval_routes, dict):
        return None
    return agent.approval_routes.get(approval.route)


async def get_approval_by_dedupe_key(session: AsyncSession, dedupe_key: str) -> Approval | None:
    result: Approval | None = await session.scalar(
        select(Approval).where(Approval.dedupe_key == dedupe_key)
    )
    return result


# How far back the re-raise guard walks a chain of platform-authored resume
# turns. A chain is one approval per hop, so this is far past any real run; it
# only bounds the walk against a corrupt row whose dedupe_key loops.
_RERAISE_CHAIN_LIMIT = 64


_DISPLAY_REQUESTER_CHAIN_LIMIT = 64


async def approval_display_requester(session: AsyncSession, approval: Approval) -> str | None:
    """Read the requester of a validated resume chain for display only.

    The current row's author remains the actor of its own turn. No result from
    this walk participates in resolution or grant authorization.
    """

    current = approval
    seen = {approval.id}
    reads = 0
    while True:
        prior_id = parse_resume_event_id(current.dedupe_key)
        if prior_id is None:
            # A malformed reserved resume id cannot prove a fresh human turn.
            if current.dedupe_key.startswith("approval-") and current.dedupe_key.endswith(
                "-resolved"
            ):
                return None
            return current.author
        if prior_id in seen or reads >= _DISPLAY_REQUESTER_CHAIN_LIMIT:
            return None
        seen.add(prior_id)
        reads += 1
        prior = await session.get(Approval, prior_id)
        if prior is None or (
            prior.agent_id != current.agent_id
            or prior.conversation_id != current.conversation_id
            or prior.reply_kind != current.reply_kind
            or prior.reply_channel != current.reply_channel
            or prior.reply_endpoint != current.reply_endpoint
            or route_identity(prior.reply_kind, prior.reply_adapter)
            != route_identity(current.reply_kind, current.reply_adapter)
        ):
            return None
        if prior.status == ApprovalStatus.expired:
            expected_actor = "system"
        elif prior.status in {ApprovalStatus.approved, ApprovalStatus.rejected}:
            if not prior.resolved_by:
                return None
            expected_actor = prior.resolved_by
        else:
            return None
        if current.author != expected_actor:
            return None
        current = prior


def _same_approval(prior: Approval, data: "ApprovalRequest") -> bool:
    """Whether ``data`` asks for the same human decision ``prior`` recorded (#2885).

    Same agent, same thread, same manifest route, and the same gate: a policy
    gate, or a permission gate on the same denied tool. The summary is left out
    on purpose. It is model-authored free text, so keying on it would let a
    reworded retry through, and a reworded retry is exactly the failure this
    guard exists for. ``route=""`` reads as routeless, matching
    ``get_approval_route_binding``.
    """

    return (
        prior.agent_id == data.agent_id
        and prior.conversation_id == data.conversation_id
        and (prior.route or None) == (data.route or None)
        and prior.gate_kind == data.gate_kind
        and prior.granted_tool == data.granted_tool
    )


async def find_rejected_reraise(session: AsyncSession, data: "ApprovalRequest") -> Approval | None:
    """The rejected approval ``data`` would re-raise with nobody asking, or None.

    The worker stamps each request with the event id of the turn that raised
    it, and a resume turn's event id is ``resume_event_id(<approval id>)``. So
    a request whose ``dedupe_key`` parses as a resume id was raised by a turn
    the platform authored, not one a person typed, and following those ids
    back walks every approval raised since the last turn a person started.
    If any of them was rejected and is the same approval (``_same_approval``),
    this request is the agent asking again on its own, and that rejected
    record is returned. A request raised from a person's turn (any other
    event id) ends the walk at once: a person asking is the explicit ask.

    Reads only; the caller decides the response and writes the audit row.
    """

    seen: set[uuid.UUID] = set()
    dedupe_key = data.dedupe_key
    for _ in range(_RERAISE_CHAIN_LIMIT):
        prior_id = parse_resume_event_id(dedupe_key)
        if prior_id is None or prior_id in seen:
            return None
        seen.add(prior_id)
        prior = await session.get(Approval, prior_id)
        if prior is None:
            return None
        if prior.status == ApprovalStatus.rejected and _same_approval(prior, data):
            return prior
        dedupe_key = prior.dedupe_key
    return None


# Per served agent: its approval route map, read fresh, and the adapter's
# bindings that belong to that agent, each as its (kind, address, identity)
# route (ADR-0168 decision 3), identity normalized by ``route_identity``.
_ServedTargets = dict[uuid.UUID, tuple[Any, frozenset[tuple[str, str, str | None]]]]


async def _adapter_served_targets(
    session: AsyncSession, bindings: frozenset[uuid.UUID]
) -> _ServedTargets:
    """What an adapter serving ``bindings`` can reach, keyed by agent id.

    Read fresh on every call, like ``get_approval_route_binding``: a binding
    deleted or a route re-pointed after the credential was issued narrows what
    the adapter sees immediately.

    Holds a binding row of ANY kind, Slack included: an adapter principal
    (ADR-0154) is scoped to the binding ROW id its token claims, not to a
    kind that can authenticate HTTP egress, so a principal may legitimately
    serve a Slack binding (``test_adapter_principal.py``'s own default
    fixture is one).
    """

    if not bindings:
        return {}
    rows = await session.execute(
        select(
            AgentChannel.agent_id,
            AgentChannel.kind,
            AgentChannel.address,
            AgentChannel.adapter,
            Agent.approval_routes,
        )
        .join(Agent, Agent.id == AgentChannel.agent_id)
        .where(AgentChannel.id.in_(bindings))
    )
    pairs: dict[uuid.UUID, set[tuple[str, str, str | None]]] = {}
    routes: dict[uuid.UUID, Any] = {}
    for agent_id, kind, address, adapter, approval_routes in rows:
        pairs.setdefault(agent_id, set()).add((kind, address, route_identity(kind, adapter)))
        routes[agent_id] = approval_routes
    return {agent_id: (routes[agent_id], frozenset(p)) for agent_id, p in pairs.items()}


def _approval_served(approval: Approval, targets: _ServedTargets) -> bool:
    """THE served predicate (ADR-0154, ADR-0177), shared by the list and the resolver.

    An approval is served when its card went to one of the adapter's bindings
    ON THE SAME AGENT, routed or not (ADR-0177 decision 4). Two ways a card
    gets there:

    - It was shown in the conversation that asked: a routeless approval, or a
      route in ``requesting_surface`` mode (``approvers.card_on_requesting_surface``).
      Then the asking route, ``(reply_kind, reply_channel, reply_adapter)``,
      must be one of the adapter's bindings. The adapter identity is part of
      the match: two adapters may bind one ``(kind, address)`` pair on the
      same agent (ADR-0168 decision 3), and only the one that showed the card
      may answer it. The record stores that route, and it is a fact about the
      original turn that no later rebinding rewrites.
    - Its route names a fixed target, the recorded card is at that target, and
      the target's ``(kind, address)`` is one of the adapter's bindings.

    An approval with no agent, or a route whose resolution is missing or
    malformed, is served by no adapter: fail closed.
    """

    if approval.agent_id is None:
        return False
    target = targets.get(approval.agent_id)
    if target is None:
        return False
    approval_routes, pairs = target
    binding = (
        approval_routes.get(approval.route)
        if approval.route and isinstance(approval_routes, dict)
        else None
    )
    if card_on_requesting_surface(approval, binding):
        asking = (
            approval.reply_kind,
            approval.reply_channel,
            route_identity(approval.reply_kind, approval.reply_adapter),
        )
        return asking in pairs
    if not approval.route or not isinstance(binding, dict):
        return False
    resolution = binding.get("resolution")
    if not isinstance(resolution, dict):
        return False
    kind, address = resolution.get("kind"), resolution.get("address")
    if not isinstance(kind, str) or not isinstance(address, str):
        return False
    # The card must actually be there: a route re-pointed after the ask does
    # not hand the pending approval to whoever serves the new target.
    if (approval.card_channel or approval.reply_channel) != address:
        return False
    # Fixed targets are Slack only, and no Slack approver set admits an
    # adapter, so this match grants listing, never an answer.
    return any((k, a) == (kind, address) for k, a, _ in pairs)


async def approval_served_by(
    session: AsyncSession, approval: Approval, bindings: frozenset[uuid.UUID]
) -> bool:
    """Whether an adapter serving ``bindings`` may see and resolve ``approval``."""

    return _approval_served(approval, await _adapter_served_targets(session, bindings))


# Rows read per round when an adapter's approval list is filtered in Python.
_SERVED_LIST_BATCH = 1000


async def list_approvals(
    session: AsyncSession,
    *,
    status: str | None = None,
    agent_id: uuid.UUID | None = None,
    conversation_id: str | None = None,
    limit: int = 50,
    served_by: frozenset[uuid.UUID] | None = None,
) -> list[Approval]:
    """Newest first. ``served_by`` (an adapter principal's bindings) narrows the
    result to approvals that adapter serves, BEFORE ``limit`` applies, so an
    adapter never gets a short page because unserved rows took the slots."""

    stmt = select(Approval).order_by(Approval.created_at.desc())
    if status is not None:
        stmt = stmt.where(Approval.status == status)
    if agent_id is not None:
        stmt = stmt.where(Approval.agent_id == agent_id)
    if conversation_id is not None:
        stmt = stmt.where(Approval.conversation_id == conversation_id)
    if served_by is None:
        result = await session.scalars(stmt.limit(limit))
        return list(result)
    targets = await _adapter_served_targets(session, served_by)
    if not targets:
        return []
    # Narrow in SQL to the served agents' rows, routed or not (ADR-0177). A
    # routeless row is served only through its asking pair, so rows asked on a
    # binding this adapter does not serve are dropped in SQL too. The rest of
    # the match (the adapter identity, the route map's resolution) stays in
    # Python, so read in keyset batches until the page is full: a batch of
    # unserved rows can never shorten the page or hide an older served row.
    asking_pairs = {(kind, address) for _, pairs in targets.values() for kind, address, _ in pairs}
    stmt = stmt.where(
        Approval.agent_id.in_(targets),
        or_(
            Approval.route.is_not(None),
            tuple_(Approval.reply_kind, Approval.reply_channel).in_(asking_pairs),
        ),
    ).order_by(Approval.id.desc())
    batch_size = max(limit, _SERVED_LIST_BATCH)
    served: list[Approval] = []
    cursor: tuple[datetime, uuid.UUID] | None = None
    while len(served) < limit:
        page = stmt
        if cursor is not None:
            page = page.where(
                tuple_(Approval.created_at, Approval.id)
                < tuple_(literal(cursor[0]), literal(cursor[1]))
            )
        batch = list(await session.scalars(page.limit(batch_size)))
        served.extend(a for a in batch if _approval_served(a, targets))
        if len(batch) < batch_size:
            break
        cursor = (batch[-1].created_at, batch[-1].id)
    return served[:limit]


async def pending_approval_inventory(
    session: AsyncSession,
) -> tuple[int, datetime | None]:
    """Fleet-wide pending count and oldest creation time, without pagination."""

    count, oldest = (
        await session.execute(
            select(func.count(Approval.id), func.min(Approval.created_at)).where(
                Approval.status == ApprovalStatus.pending
            )
        )
    ).one()
    return int(count), oldest


async def claim_approval_resolution(
    session: AsyncSession,
    approval_id: uuid.UUID,
    *,
    decision: str,
    resolved_by: str,
    note: str | None,
) -> Approval | None:
    """The resolve-once compare-and-set: exactly one resolver wins.

    A conditional UPDATE guarded on ``status = 'pending'`` claims the record;
    concurrent attempts see zero rows updated and get None back (the router
    tells them who won). This is the claim-race primitive of ADR-0010.
    """

    values: dict[str, Any] = {
        "status": decision,
        "resolved_by": resolved_by,
        "resolution_note": note,
        "resolved_at": func.now(),
    }
    # Publication outcomes are reported by the platform worker, never by a
    # resumed model turn. Mark the approval as owing no wake in the same CAS.
    publication = await get_publication_by_approval(session, approval_id)
    if (
        publication is not None
        and publication.execution_request_id is not None
        and decision == ApprovalStatus.approved
    ):
        owning = await session.get(ExecutionRequest, publication.execution_request_id)
        if owning is None or owning.status != "running":
            decision = ApprovalStatus.rejected
            values["status"] = decision
            values["resolution_note"] = "the factory run already ended"
    values["resumed_at"] = func.now() if publication is not None else _no_wake_resumed_at()

    result = await session.execute(
        update(Approval)
        .where(Approval.id == approval_id, Approval.status == ApprovalStatus.pending)
        .values(**values)
        .returning(Approval.id)
    )
    claimed = result.scalar_one_or_none()
    if claimed is not None and publication is not None:
        publication_status = "approved" if decision == ApprovalStatus.approved else "denied"
        publication_values: dict[str, Any] = {
            "status": publication_status,
            "version": Publication.version + 1,
            "updated_at": func.now(),
        }
        if publication_status == "denied":
            publication_values["terminal_at"] = func.now()
            publication_values["patch_bytes"] = None
        changed = await session.execute(
            update(Publication)
            .where(
                Publication.id == publication.id,
                Publication.status == "pending",
                Publication.version == publication.version,
            )
            .values(**publication_values)
            .returning(Publication.id)
        )
        if changed.scalar_one_or_none() is None:
            await session.rollback()
            return None
    await session.commit()
    if claimed is None:
        return None
    approval = await session.get(Approval, approval_id)
    if approval is not None:
        await session.refresh(approval)
    return approval


async def list_expired_pending_approvals(
    session: AsyncSession, *, now: datetime, limit: int = 100
) -> list[Approval]:
    """The pending approvals whose SLA has lapsed (#412), oldest-lapse-first.

    ``now`` is naive UTC, matching the DateTime columns and the router's
    ``_expired`` comparison. Ordering by ``expires_at`` drains the oldest
    lapses first, so a backlog larger than ``limit`` clears across successive
    sweep passes rather than starving the earliest-expired records. Records
    with a NULL ``expires_at`` (no SLA) are never selected.
    """

    result = await session.scalars(
        select(Approval)
        .where(
            Approval.status == ApprovalStatus.pending,
            Approval.expires_at.is_not(None),
            Approval.expires_at <= now,
        )
        .order_by(Approval.expires_at)
        .limit(limit)
    )
    return list(result)


async def expire_approval(session: AsyncSession, approval_id: uuid.UUID) -> Approval | None:
    """Flip a pending approval past its SLA to expired (same CAS guard, so an
    in-flight resolution that already won is never overwritten)."""

    publication = await get_publication_by_approval(session, approval_id)
    approval_values: dict[str, Any] = {
        "status": ApprovalStatus.expired,
        "resolved_at": func.now(),
    }
    approval_values["resumed_at"] = (
        func.now() if publication is not None else _no_wake_resumed_at()
    )
    result = await session.execute(
        update(Approval)
        .where(Approval.id == approval_id, Approval.status == ApprovalStatus.pending)
        .values(**approval_values)
        .returning(Approval.id)
    )
    claimed = result.scalar_one_or_none()
    if claimed is not None and publication is not None:
        await session.execute(
            update(Publication)
            .where(Publication.id == publication.id, Publication.status == "pending")
            .values(
                status="expired",
                patch_bytes=None,
                version=Publication.version + 1,
                updated_at=func.now(),
                terminal_at=func.now(),
            )
        )
    await session.commit()
    if claimed is None:
        return None
    return await session.get(Approval, approval_id)


async def mark_approval_resumed(session: AsyncSession, approval_id: uuid.UUID) -> None:
    """Record that the resume turn made it onto the stream (#411).

    Conditional UPDATE guarded on ``resumed_at IS NULL``, so a second call (a
    reconciler racing the inline path, another replica) matches zero rows and is
    a no-op. Mirrors the conditional-UPDATE style of ``claim_approval_resolution``.
    """

    await session.execute(
        update(Approval)
        .where(Approval.id == approval_id, Approval.resumed_at.is_(None))
        .values(resumed_at=func.now())
    )
    await session.commit()


async def reopen_dead_lettered_resume(
    session: AsyncSession, approval_id: uuid.UUID, *, dead_lettered_after: datetime
) -> bool:
    """Re-open an approval whose DELIVERED resume turn was dead-lettered (#532).

    A resume turn that reached the runs stream (so ``resumed_at`` was marked)
    can still die at the worker's delivery cap (#505) and be moved to the
    graveyard, acked off, and never woken -- a row the NULL-gated finder cannot
    re-select. Clearing ``resumed_at`` puts it back on the reconciler's owed-wake
    work-list so the standard reconcile pass re-enqueues it. Conditional UPDATE
    mirroring ``mark_approval_resumed``; returns whether a row was re-opened.

    The ``resumed_at < dead_lettered_after`` guard is LOAD-BEARING for
    idempotency: it fires only when the CURRENTLY-marked wake predates THIS
    dead-letter, so a graveyard row that persists across passes (the stream is
    only approximately trimmed) cannot repeatedly re-open a row that has since
    been re-enqueued -- its new ``resumed_at`` is newer than the row's
    dead-letter time. A genuinely new dead-letter carries a newer time and
    re-triggers. A row already re-opened (``resumed_at`` NULL) matches zero rows,
    so the standard NULL-gated reconciler owns it, never this path.

    The comparison is a CROSS-NODE clock comparison: ``resumed_at`` is stamped
    by Postgres (``func.now()`` on the inline mark path) or the API pod clock
    (``datetime.now(UTC)`` on the reconcile re-enqueue path), while
    ``dead_lettered_after`` is the worker pod's clock (``dl_dead_lettered_at``).
    It is safe because the gap between marking a wake and exhausting the
    delivery cap is minutes, dwarfing realistic NTP skew.
    """

    result = await session.execute(
        update(Approval)
        .where(
            Approval.id == approval_id,
            Approval.purpose.not_in(NO_WAKE_PURPOSES),
            Approval.status.in_(_RESUMABLE_STATUSES),
            Approval.resumed_at.is_not(None),
            Approval.resumed_at < dead_lettered_after,
        )
        .values(resumed_at=None)
        .returning(Approval.id)
    )
    reopened = result.scalar_one_or_none() is not None
    await session.commit()
    return reopened


# The statuses an owed-wake row can carry: a terminal outcome that must still
# reach its suspended session. ``expired`` belongs here since #412 gave both
# expiry paths (the sweeper and the resolve-path expiry branch) a resume turn of
# their own, so an expired record owes a wake exactly as a decided one does
# (#418). Only ``pending`` is excluded: it has neither been decided nor lapsed,
# so nothing is owed yet. Shared by the reconciler's candidate finder and its
# per-row claim so the two never desync.
_RESUMABLE_STATUSES = (
    ApprovalStatus.approved,
    ApprovalStatus.rejected,
    ApprovalStatus.expired,
)


async def claim_resume_row(session: AsyncSession, approval_id: uuid.UUID) -> Approval | None:
    """Atomically claim one owed-wake row for this reconcile pass (#411).

    ``SELECT ... FOR UPDATE SKIP LOCKED`` locks the row for the caller's
    transaction, or returns None if another replica already holds it OR it is
    already resumed (or no longer resolved). This is the per-row claim that keeps
    two API replicas' overlapping reconcile passes from both enqueuing the same
    resume turn -- the worker's done-marker is written only post-terminal, so it
    cannot dedupe a concurrent re-run; the row claim must. The caller owns the
    transaction (this does NOT commit); marking ``resumed_at`` on the returned
    ORM object and committing releases the lock.
    """

    approval: Approval | None = await session.scalar(
        select(Approval).where(*_owes_resume(approval_id)).with_for_update(skip_locked=True)
    )
    return approval


async def list_resolved_unresumed(
    session: AsyncSession, *, resolved_before: datetime, limit: int
) -> list[uuid.UUID]:
    """The reconciler's work-list: ids of settled approvals whose wake is owed.

    A row in any ``_RESUMABLE_STATUSES`` with ``resolved_at`` set and
    ``resumed_at`` NULL is an owed wake: every path that settles a record (the
    resolve endpoint, the expiry sweeper, and the resolve-path expiry branch)
    enqueues a resume turn and marks ``resumed_at`` only once that enqueue
    succeeded, so NULL means the wake never reached the stream. That now includes
    ``expired`` records (#418), whose expiry wake was previously unrecoverable
    because a flipped record is no longer ``pending`` and so is never re-selected
    by ``list_expired_pending_approvals``. ``resolved_before`` is naive UTC,
    matching the DateTime columns.

    Returns ids only (the unlocked candidate finder): each id is then claimed
    atomically by ``claim_resume_row`` in its own short transaction, which
    re-reads the row under lock, so the reconciler never holds a row lock across
    the Valkey enqueue of the batch and never needs the full row here.
    """

    result = await session.scalars(
        select(Approval.id)
        .where(
            Approval.purpose.not_in(NO_WAKE_PURPOSES),
            Approval.status.in_(_RESUMABLE_STATUSES),
            Approval.resolved_at.is_not(None),
            Approval.resumed_at.is_(None),
            Approval.resolved_at <= resolved_before,
        )
        .order_by(Approval.resolved_at)
        .limit(limit)
    )
    return list(result)


async def append_approval_audit(
    session: AsyncSession,
    *,
    approval_id: uuid.UUID,
    action: str,
    actor: str,
    actor_channel: str | None,
    decision: str,
    authorizer: str,
    authorized: bool,
    reason: str | None,
    evidence: dict[str, Any] | None = None,
    principal_kind: str | None = None,
    authenticated: bool = False,
    principal_subject: str | None = None,
) -> ApprovalAuditEntry:
    """Append one audit row (#247). Append-only by design; never updated.

    ``evidence`` (#420) is the membership snapshot the authorizer decided on;
    None for writers that made no membership decision. ``principal_subject``
    names the adapter that transported an ``adapter`` principal's decision
    (ADR-0154); None for every other kind.
    """

    entry = ApprovalAuditEntry(
        approval_id=approval_id,
        action=action,
        actor=actor,
        actor_channel=actor_channel,
        principal_kind=principal_kind,
        authenticated=authenticated,
        principal_subject=principal_subject,
        decision=decision,
        authorizer=authorizer,
        authorized=authorized,
        reason=reason,
        evidence=evidence,
    )
    session.add(entry)
    await session.commit()
    await session.refresh(entry)
    return entry


# --- break-glass recovery (#2753) --------------------------------------------
#
# ONE transaction, ONE commit, per operation. ``claim_approval_resolution``
# commits internally, and so does ``append_approval_audit``; composing the two
# leaves a crash window in which the status flipped and the audit row that
# explains it never existed. For a path whose whole justification is that every
# use is reviewable afterwards, that window is the failure, so recovery does
# the compare-and-set and the audit append inside a single ``session.begin()``
# block instead of calling either.
#
# Idempotency is keyed on that audit row, not on a column: the caller's
# ``recovery_key`` is recorded in the row's evidence, and because the row
# commits with the CAS, a key with no row means no effect landed.
#
# The audit row is built from the module-level ``ApprovalAuditEntry``, exactly
# as ``append_approval_audit`` does. That is deliberate and load-bearing: the
# seam between the CAS and the audit append has to be the same one the existing
# writer exposes, so a test can interrupt precisely there. A Core ``insert()``
# here would move the seam.

#: Everything the caller must supply about WHO acted. Recovery takes its actor
#: from the ADR-0106 operator principal for attribution only; no membership is
#: consulted and nothing widens.
_RECOVERY_AUTHORIZER = "approval_recovery"


async def reread_approval(session: AsyncSession, approval_id: uuid.UUID) -> Approval | None:
    """Read an approval back from the database, not from the identity map.

    An ORM-enabled Core UPDATE expires the columns it touched on any instance
    already in the session, so a plain ``session.get`` hands back an object
    whose next attribute access is a lazy load -- which under the async session
    is a ``MissingGreenlet``, not a refresh. ``claim_approval_resolution``
    refreshes for the same reason.
    """

    approval = await session.get(Approval, approval_id)
    if approval is not None:
        await session.refresh(approval)
    return approval


#: The audit model as the replay lookup reads it. A separate name on purpose:
#: ``recover_approval_atomic`` builds its row from the module-level
#: ``ApprovalAuditEntry`` so a test can interrupt exactly between the CAS and
#: the audit append, and the lookup must not be caught by that interruption.
_RecoveryAuditEntry = ApprovalAuditEntry


#: The audit action every administrative recovery writes. Its evidence carries
#: the caller's ``recovery_key``, which is what a replay is matched on.
RECOVERY_AUDIT_ACTION = "administratively_recovered"


async def find_recovery_audit(
    session: AsyncSession, recovery_key: str
) -> ApprovalAuditEntry | None:
    """The recovery audit row recorded under ``recovery_key``, on any approval.

    A key names one administrative act installation-wide, so the lookup is not
    scoped to an approval: the caller compares the row's ``approval_id`` to tell
    a replay from a key reused for a different approval.
    """

    result = await session.execute(
        select(_RecoveryAuditEntry)
        .where(
            _RecoveryAuditEntry.action == RECOVERY_AUDIT_ACTION,
            _RecoveryAuditEntry.evidence["recovery_key"].astext == recovery_key,
        )
        .order_by(_RecoveryAuditEntry.created_at)
        .limit(1)
    )
    return result.scalar_one_or_none()


async def recover_approval_atomic(
    session: AsyncSession,
    approval_id: uuid.UUID,
    *,
    reason: str,
    recovery_key: str,
    actor: str,
    actor_channel: str | None,
    principal_kind: str | None,
    facts: list[str],
) -> Approval | None:
    """Administratively settle a pending approval as ``rejected``, atomically.

    The CAS is guarded on ``status = 'pending'`` exactly as the ordinary
    resolve-once claim is, so an approval is settled at most once. Returns None
    when the CAS matched nothing; the caller looks the key up with
    ``find_recovery_audit`` to tell a replay (return the recorded outcome) from
    a genuine conflict.

    ``facts`` are the reporter's OBSERVATIONS, recorded as evidence. They state
    what was seen about the row. They never assert that the ordinary path was
    unavailable -- nothing here is in a position to know that.
    """

    recovered: uuid.UUID | None
    async with session.begin():
        # The associated publication, read inside the SAME transaction that
        # settles the approval. ``claim_approval_resolution`` settles it too,
        # but it commits internally, so it cannot be reused here: composing it
        # would put the publication's fate in a second transaction and reopen
        # the crash window this whole function exists to close.
        publication = await get_publication_by_approval(session, approval_id)
        values: dict[str, Any] = {
            "status": ApprovalStatus.rejected,
            "resolved_by": actor,
            "resolution_note": reason,
            "resolved_at": func.now(),
        }
        if publication is not None:
            # A publication outcome is reported by the platform worker through
            # the stored reply route, never by a resumed model turn. Mark the
            # wake as owing nothing in the same CAS, exactly as the ordinary
            # resolve path does, so the reconciler never picks the row up for a
            # resume the router deliberately does not enqueue.
            values["resumed_at"] = func.now()
        else:
            values["resumed_at"] = _no_wake_resumed_at()
        result = await session.execute(
            update(Approval)
            .where(
                Approval.id == approval_id,
                Approval.status == ApprovalStatus.pending,
            )
            .values(**values)
            .returning(Approval.id)
        )
        recovered = result.scalar_one_or_none()
        if recovered is None:
            return None
        if publication is not None:
            # The same denial the ordinary reject performs, under the same
            # version check: status denied, the patch dropped, the terminal
            # instant recorded. Without it the recovered approval is settled and
            # its publication waits forever -- the expiry sweeper no longer
            # selects a rejected approval, and no resume is enqueued to repair
            # it, so nothing else in the system would ever touch it again.
            changed = await session.execute(
                update(Publication)
                .where(
                    Publication.id == publication.id,
                    Publication.status == "pending",
                    Publication.version == publication.version,
                )
                .values(
                    status="denied",
                    version=Publication.version + 1,
                    updated_at=func.now(),
                    terminal_at=func.now(),
                    patch_bytes=None,
                )
                .returning(Publication.id)
            )
            if changed.scalar_one_or_none() is None:
                raise PublicationSettlementConflict(
                    "the approval's publication is no longer pending at the "
                    "version this recovery read; nothing was changed"
                )
        entry = ApprovalAuditEntry(
            approval_id=approval_id,
            action=RECOVERY_AUDIT_ACTION,
            actor=actor,
            actor_channel=actor_channel,
            principal_kind=principal_kind,
            authenticated=True,
            decision=ApprovalStatus.rejected,
            authorizer=_RECOVERY_AUTHORIZER,
            authorized=True,
            reason=reason,
            evidence={
                "kind": "administrative_recovery",
                "recovery_key": recovery_key,
                "facts": facts,
            },
        )
        session.add(entry)
    return await reread_approval(session, approval_id)


async def list_approval_audit(
    session: AsyncSession, approval_id: uuid.UUID
) -> list[ApprovalAuditEntry]:
    result = await session.scalars(
        select(ApprovalAuditEntry)
        .where(ApprovalAuditEntry.approval_id == approval_id)
        .order_by(ApprovalAuditEntry.created_at)
    )
    return list(result)


def _owes_resume(approval_id: uuid.UUID) -> tuple[ColumnElement[bool], ...]:
    """The owed-wake predicate for one row, shared by ``claim_resume_row`` and
    ``approval_owes_resume`` so the locked claim and the unlocked check never
    drift apart."""

    return (
        Approval.id == approval_id,
        Approval.purpose.not_in(NO_WAKE_PURPOSES),
        Approval.resumed_at.is_(None),
        Approval.status.in_(_RESUMABLE_STATUSES),
    )


async def approval_owes_resume(session: AsyncSession, approval_id: uuid.UUID) -> bool:
    """Whether this row still owes a wake, read WITHOUT a row lock.

    Same predicate as ``claim_resume_row``. A plain read is not blocked by a
    peer's ``FOR UPDATE``, so it tells "already resumed, gone, or no longer
    resumable" apart from "merely locked by another transaction", which
    ``SKIP LOCKED`` alone reports identically as None.
    """

    found = await session.scalar(select(Approval.id).where(*_owes_resume(approval_id)))
    return found is not None

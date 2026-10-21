"""Canonical memory records: the one transaction boundary (ADR 0239).

A canonical memory entry is a ``memory`` / ``fact-<32 lowercase hex>`` head in
``workflow_state_entries``, agent-wide (``binding_scope`` null) or in one
binding's scope. Its value is the typed shape ``{kind, title, statement,
author, stated_at, session_id, conversation, archived}``. Every mutation of
such a head, from the raw state routes and the typed ``_memory`` routes alike,
goes through this module, which in ONE Postgres transaction:

1. takes the scope's memory advisory lock (``lock_scope``);
2. re-reads the agent's ``memory_writes`` and ``agent_wide_learning`` after
   that lock, under a share lock on the agent row, and checks the caller with
   them (sandbox writes need ``memory_writes`` and, for a standing
   instruction, ``agent_wide_learning``), so a toggle turned off while the
   write waited for the lock is honored;
3. validates the value and stamps provenance (a sandbox write's author,
   conversation and time come from the signed claims and the server clock);
4. compares the expected version with the head, or for a deleted head with its
   latest revision, so a stale version never succeeds after delete/recreate;
5. checks the retained-head count and the byte caps;
6. writes the head and appends exactly one ``memory_revisions`` row.

A refusal raises before anything is written, and the transaction is rolled
back, so no partial head or revision survives it. This module never logs a
memory body, author or conversation.

Lock ordering. This module's memory scope lock is always the FIRST lock a
memory writer takes in its transaction. A mutation here then takes the agent
row's share lock (``FOR SHARE``), so an operator ``PATCH`` of the toggles either
commits before the re-read and is seen, or waits for this transaction. After
those, the existing order is unchanged: row locks (``FOR UPDATE``), and the
per-agent namespace-creation lock (``routers.state.enforce_caps``), which is
only requested when the namespace has no rows and so while no memory row lock
is held. Nothing requests the memory lock while holding any of the others (the
agent ``PATCH`` takes none), so no cycle is possible. Every writer of the
``memory`` namespace (the legacy log and the guidance included) takes the
memory lock first, so the count and the namespace byte cap are both serialized
per (agent, scope).
"""

import base64
import binascii
import hashlib
import json
import logging
import re
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal, NoReturn

from fastapi import HTTPException, status
from fastapi.responses import JSONResponse
from sqlalchemy import func, literal, or_, select, text, tuple_
from sqlalchemy.ext.asyncio import AsyncSession

from . import state_mutation
from .config import get_settings
from .memory_guidance import effective_guidance
from .models import Agent, MemoryRevision, WorkflowStateEntry
from .schemas.memory_records import (
    MemoryContextOut,
    MemoryDetailOut,
    MemoryIndexItemOut,
    MemoryOmittedOut,
    MemoryRecordOut,
    MemoryRevisionOut,
    MemorySearchOut,
    MemoryTotalsOut,
)
from .schemas.state import MemoryGuidanceOut

logger = logging.getLogger(__name__)

MEMORY_NAMESPACE = "memory"
GUIDANCE_KEY = "guidance"
# ``memory_revisions.scope`` for the agent-wide scope. A binding scope is always
# ``<kind>:<address>``, so it cannot collide. Mirrored in migration 0110.
AGENT_SCOPE = "agent"
# The canonical key. ``\Z``, not ``$``: ``$`` also matches before a trailing
# newline. Mirrors ``routers.state._FACT_KEY`` and the runner's fact id.
FACT_KEY = re.compile(r"^fact-[0-9a-f]{32}\Z")
# The same shape for Postgres (an ARE ``$`` matches only at the very end).
_FACT_KEY_SQL = "^fact-[0-9a-f]{32}$"

KINDS = ("fact", "instruction")
TITLE_MAX_CHARS = 80
STATEMENT_MAX_CHARS = 500
CONTEXT_MAX_INSTRUCTIONS = 20
CONTEXT_MAX_FACTS = 50
HISTORY_PAGE = 10
SEARCH_DEFAULT_LIMIT = 20
SEARCH_MAX_LIMIT = 50

OPERATOR = "operator"

# Advisory-lock class of the per-(agent, scope) memory lock, in the two-int4
# space like the namespace-creation lock (class 933, routers/state.py) and
# distinct from it. The number is the ADR.
_MEMORY_LOCK_CLASS = 239


@dataclass(frozen=True)
class Actor:
    """Who is mutating memory, as the server establishes it.

    A sandbox actor's ``name`` is the per-turn credential's signed ``sender``
    and ``conversation`` its signed ``conversation`` (the turn's thread key);
    neither ever comes from a request body. The platform key is the operator,
    with no conversation: a shared key cannot prove a particular person."""

    source: Literal["sandbox", "operator"]
    name: str
    conversation: str | None

    @classmethod
    def operator(cls) -> "Actor":
        return cls(source="operator", name=OPERATOR, conversation=None)

    @property
    def sandbox(self) -> bool:
        return self.source == "sandbox"


class VersionConflict(Exception):
    """The caller's expected version is not the entry's current version.

    ``current_version`` is the head's version, or for a deleted head its latest
    revision's (the deletion's), or 0 for an identity never created."""

    def __init__(self, current_version: int) -> None:
        super().__init__(current_version)
        self.current_version = current_version

    def response(self) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={
                "detail": f"version conflict: current version is {self.current_version}",
                "current_version": self.current_version,
            },
        )


def is_canonical_key(namespace: str, key: str) -> bool:
    return namespace == MEMORY_NAMESPACE and FACT_KEY.fullmatch(key) is not None


def scope_label(binding_scope: str | None) -> Literal["agent", "channel"]:
    return "agent" if binding_scope is None else "channel"


def _revision_scope(binding_scope: str | None) -> str:
    return AGENT_SCOPE if binding_scope is None else binding_scope


def _lock_key(agent_id: uuid.UUID, binding_scope: str | None) -> int:
    """A stable int4 key for one (agent, memory scope). hashlib, not ``hash()``,
    which is randomized per process. A collision only serializes two scopes
    against each other; every query under the lock still filters by scope."""

    material = agent_id.bytes + b"\0" + _revision_scope(binding_scope).encode() + b"\0memory"
    return int.from_bytes(hashlib.blake2b(material, digest_size=4).digest(), "big", signed=True)


async def lock_scope(session: AsyncSession, agent_id: uuid.UUID, binding_scope: str | None) -> None:
    """Serialize every writer of one (agent, scope)'s ``memory`` namespace.

    Transaction-scoped: COMMIT or ROLLBACK releases it. Take it before counting,
    before the byte caps and before any row lock (see the module docstring for
    the ordering with the namespace-creation lock)."""

    await session.execute(
        text("SELECT pg_advisory_xact_lock(:cls, :key)"),
        {"cls": _MEMORY_LOCK_CLASS, "key": _lock_key(agent_id, binding_scope)},
    )


@asynccontextmanager
async def _transaction(session: AsyncSession) -> AsyncIterator[None]:
    """Roll back on any refusal, so the memory lock is released at once and no
    partial head or revision is left in the session."""

    try:
        yield
    except BaseException:
        await session.rollback()
        raise


def _now() -> str:
    # The runner's ``memory_facts._now`` format: RFC3339 UTC with a ``Z``.
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _unprocessable(detail: str) -> NoReturn:
    raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, detail)


def _forbidden(agent_id: uuid.UUID, reason: str) -> NoReturn:
    logger.warning("memory: refused sandbox write for agent %s: %s", agent_id, reason)
    raise HTTPException(status.HTTP_403_FORBIDDEN, reason)


def _kind_of(value: Any) -> str | None:
    if isinstance(value, dict):
        kind = value.get("kind")
        if isinstance(kind, str) and kind in KINDS:
            return kind
    return None


def _archived_of(value: Any) -> bool:
    return isinstance(value, dict) and value.get("archived") is True


def _single_line(title: str) -> bool:
    return title.splitlines() == [title]


def canonical_value(value: Any, *, actor: Actor, archived: bool) -> dict[str, Any]:
    """Validate a written value and build the canonical stored shape.

    Only the eight canonical fields are kept. A sandbox write's author,
    stated_at and conversation are the server's; the platform key keeps the
    body's author and stated_at and names no conversation. ``archived`` is the
    server's in every case: false on create, the head's own on update."""

    if not isinstance(value, dict):
        _unprocessable("a memory entry must be a JSON object")
    kind = value.get("kind")
    if not isinstance(kind, str) or kind not in KINDS:
        _unprocessable("kind must be 'fact' or 'instruction'")
    title = value.get("title")
    if not isinstance(title, str) or not title.strip():
        _unprocessable("title must be a non-blank string")
    if not _single_line(title):
        _unprocessable("title must be a single line")
    if len(title) > TITLE_MAX_CHARS:
        _unprocessable(f"title is {len(title)} characters, over the {TITLE_MAX_CHARS} limit")
    statement = value.get("statement")
    if not isinstance(statement, str) or not statement.strip():
        _unprocessable("statement must be a non-blank string")
    if len(statement) > STATEMENT_MAX_CHARS:
        _unprocessable(
            f"statement is {len(statement)} characters, over the {STATEMENT_MAX_CHARS} limit"
        )
    session_id = value.get("session_id", "")
    if not isinstance(session_id, str):
        _unprocessable("session_id must be a string")
    author: Any
    stated_at: Any
    if actor.sandbox:
        author, stated_at, conversation = actor.name, _now(), actor.conversation
    else:
        author = value.get("author", OPERATOR)
        stated_at = value.get("stated_at", _now())
        conversation = None
        if not isinstance(author, str) or not isinstance(stated_at, str):
            _unprocessable("author and stated_at must be strings")
    return {
        "kind": kind,
        "title": title,
        "statement": statement,
        "author": author,
        "stated_at": stated_at,
        "session_id": session_id,
        "conversation": conversation,
        "archived": archived,
    }


def _valid_snapshot(value: Any) -> bool:
    return (
        _kind_of(value) is not None
        and isinstance(value.get("title"), str)
        and isinstance(value.get("statement"), str)
    )


def record_of(entry: WorkflowStateEntry) -> MemoryRecordOut | None:
    """A head as a typed record, or None for a malformed head (which stays
    inspectable and removable through the raw routes, never an instruction)."""

    value = entry.value
    if not _valid_snapshot(value):
        return None
    author = value.get("author")
    stated_at = value.get("stated_at")
    conversation = value.get("conversation")
    return MemoryRecordOut(
        id=entry.key,
        scope=scope_label(entry.binding_scope),
        kind=value["kind"],
        title=value["title"],
        statement=value["statement"],
        author=author if isinstance(author, str) else None,
        stated_at=stated_at if isinstance(stated_at, str) else None,
        conversation=conversation if isinstance(conversation, str) else None,
        archived=_archived_of(value),
        version=entry.version,
        updated_at=entry.updated_at,
    )


def _require_writes(agent: Agent, actor: Actor) -> None:
    if actor.sandbox and not agent.memory_writes:
        _forbidden(agent.id, "memory writes are turned off for this agent")


async def _admit(
    session: AsyncSession, agent: Agent, binding_scope: str | None, actor: Actor
) -> None:
    """Take the scope lock, then re-read the agent's toggles under a share lock
    on its row and check ``memory_writes``. The route loaded ``agent`` before
    the lock wait, so its toggles may be stale; after this, ``_require_learning``
    sees the values that hold for the rest of the transaction. The operator is
    not gated by the toggles, so its writes skip the re-read."""

    await lock_scope(session, agent.id, binding_scope)
    if not actor.sandbox:
        return
    await session.refresh(
        agent,
        attribute_names=["memory_writes", "agent_wide_learning"],
        with_for_update={"read": True},
    )
    _require_writes(agent, actor)


def _require_learning(agent: Agent, actor: Actor, *kinds: str | None) -> None:
    if actor.sandbox and "instruction" in kinds and not agent.agent_wide_learning:
        _forbidden(
            agent.id,
            "standing instructions need agent-wide learning, which is off for this agent",
        )


def _head_query(agent_id: uuid.UUID, binding_scope: str | None, fact_id: str) -> Any:
    return select(WorkflowStateEntry).where(
        WorkflowStateEntry.agent_id == agent_id,
        WorkflowStateEntry.binding_scope == binding_scope,
        WorkflowStateEntry.namespace == MEMORY_NAMESPACE,
        WorkflowStateEntry.key == fact_id,
    )


async def _locked_head(
    session: AsyncSession, agent_id: uuid.UUID, binding_scope: str | None, fact_id: str
) -> WorkflowStateEntry | None:
    head: WorkflowStateEntry | None = await session.scalar(
        _head_query(agent_id, binding_scope, fact_id).with_for_update()
    )
    return head


async def _latest_revision(
    session: AsyncSession, agent_id: uuid.UUID, binding_scope: str | None, fact_id: str
) -> int | None:
    latest: int | None = await session.scalar(
        select(func.max(MemoryRevision.version)).where(
            MemoryRevision.agent_id == agent_id,
            MemoryRevision.scope == _revision_scope(binding_scope),
            MemoryRevision.fact_id == fact_id,
        )
    )
    return latest


def _append_revision(
    session: AsyncSession,
    agent_id: uuid.UUID,
    binding_scope: str | None,
    fact_id: str,
    *,
    version: int,
    operation: str,
    snapshot: dict[str, Any] | None,
    actor: Actor,
    reverted_to_version: int | None = None,
) -> None:
    session.add(
        MemoryRevision(
            agent_id=agent_id,
            scope=_revision_scope(binding_scope),
            fact_id=fact_id,
            version=version,
            operation=operation,
            snapshot=None if snapshot is None else dict(snapshot),
            actor=actor.name,
            source=actor.source,
            conversation=actor.conversation,
            reverted_to_version=reverted_to_version,
        )
    )


def _record_mutation(
    agent_id: uuid.UUID,
    binding_scope: str | None,
    fact_id: str,
    actor: Actor,
    *,
    deleted: bool | None,
) -> None:
    """The safe operational record (#3673) of one committed typed mutation,
    the same line the raw state routes emit: ids and the credential kind only,
    never a body. ``deleted`` None is an edit (archive, restore, a revert to a
    snapshot); otherwise a removal, True when a head was actually removed.
    Called only after COMMIT, so refusals and no-ops emit nothing."""

    state_mutation.record(
        op="edit" if deleted is None else "delete",
        agent_id=agent_id,
        scope=binding_scope,
        namespace=MEMORY_NAMESPACE,
        key=fact_id,
        removed=deleted,
        principal="platform" if actor.source == "operator" else "state",
    )


async def _check_capacity(
    session: AsyncSession, agent_id: uuid.UUID, binding_scope: str | None
) -> None:
    """Refuse a new head once the scope retains its configured number of
    canonical heads, active, archived and malformed alike. Runs under the
    scope lock, so concurrent creates cannot both see the last free slot."""

    cap = get_settings().memory_max_entries_per_scope
    held = await session.scalar(
        select(func.count())
        .select_from(WorkflowStateEntry)
        .where(
            WorkflowStateEntry.agent_id == agent_id,
            WorkflowStateEntry.binding_scope == binding_scope,
            WorkflowStateEntry.namespace == MEMORY_NAMESPACE,
            WorkflowStateEntry.key.regexp_match(_FACT_KEY_SQL),
        )
    )
    if (held or 0) >= cap:
        raise HTTPException(
            status.HTTP_413_CONTENT_TOO_LARGE,
            f"this memory already holds {held} entries, its {cap}-entry cap; "
            "forget an entry before adding another (archived entries still count)",
        )


async def _enforce_bytes(
    session: AsyncSession,
    agent_id: uuid.UUID,
    binding_scope: str | None,
    fact_id: str,
    value: dict[str, Any],
) -> None:
    # Imported here: routers.state imports this module, and its byte caps are
    # the one implementation every state writer shares.
    from .routers.state import enforce_caps

    await enforce_caps(session, agent_id, binding_scope, MEMORY_NAMESPACE, fact_id, value)


async def put_canonical(
    session: AsyncSession,
    agent: Agent,
    binding_scope: str | None,
    fact_id: str,
    value: Any,
    expected_version: int | None,
    actor: Actor,
) -> WorkflowStateEntry:
    """Create or update one canonical head (raw ``PUT`` on a fact key).

    Create: no head and no history, ``expected_version`` absent or 0. Reusing a
    deleted identity is a conflict; resurrection is ``revert``. Update: the
    head's version is required (428 when omitted); kind and archived are kept."""

    async with _transaction(session):
        _require_writes(agent, actor)
        await _admit(session, agent, binding_scope, actor)
        head = await _locked_head(session, agent.id, binding_scope, fact_id)
        if head is None:
            latest = await _latest_revision(session, agent.id, binding_scope, fact_id)
            if latest is not None:
                raise VersionConflict(latest)
            if expected_version not in (None, 0):
                raise VersionConflict(0)
            record = canonical_value(value, actor=actor, archived=False)
            _require_learning(agent, actor, record["kind"])
            await _check_capacity(session, agent.id, binding_scope)
            await _enforce_bytes(session, agent.id, binding_scope, fact_id, record)
            head = WorkflowStateEntry(
                agent_id=agent.id,
                binding_scope=binding_scope,
                namespace=MEMORY_NAMESPACE,
                key=fact_id,
                value=record,
                version=1,
            )
            session.add(head)
            operation = "create"
        else:
            if expected_version is None:
                raise HTTPException(
                    status.HTTP_428_PRECONDITION_REQUIRED,
                    "updating a memory entry needs expected_version, the version you read",
                )
            if expected_version != head.version:
                raise VersionConflict(head.version)
            stored_kind = _kind_of(head.value)
            record = canonical_value(value, actor=actor, archived=_archived_of(head.value))
            if stored_kind is not None and record["kind"] != stored_kind:
                _unprocessable(f"kind is fixed at creation; this entry is a {stored_kind}")
            _require_learning(agent, actor, record["kind"], stored_kind)
            await _enforce_bytes(session, agent.id, binding_scope, fact_id, record)
            head.value = record
            head.version += 1
            operation = "update"
        await session.flush()
        _append_revision(
            session,
            agent.id,
            binding_scope,
            fact_id,
            version=head.version,
            operation=operation,
            snapshot=record,
            actor=actor,
        )
        await session.commit()
    await session.refresh(head)
    return head


async def delete_canonical(
    session: AsyncSession,
    agent: Agent,
    binding_scope: str | None,
    fact_id: str,
    expected_version: int | None,
    actor: Actor,
) -> None:
    """Forget one canonical head under compare-and-delete (raw ``DELETE``),
    recording a deletion revision. A malformed head is removable too."""

    async with _transaction(session):
        _require_writes(agent, actor)
        if expected_version is None:
            raise HTTPException(
                status.HTTP_428_PRECONDITION_REQUIRED,
                "forgetting a memory entry needs expected_version, the version you read",
            )
        await _admit(session, agent, binding_scope, actor)
        head = await _locked_head(session, agent.id, binding_scope, fact_id)
        if head is None:
            latest = await _latest_revision(session, agent.id, binding_scope, fact_id)
            raise VersionConflict(latest or 0)
        if expected_version != head.version:
            raise VersionConflict(head.version)
        _require_learning(agent, actor, _kind_of(head.value))
        version = head.version + 1
        await session.delete(head)
        _append_revision(
            session,
            agent.id,
            binding_scope,
            fact_id,
            version=version,
            operation="delete",
            snapshot=None,
            actor=actor,
        )
        await session.commit()


async def _missing_or_conflict(
    session: AsyncSession, agent_id: uuid.UUID, binding_scope: str | None, fact_id: str
) -> NoReturn:
    latest = await _latest_revision(session, agent_id, binding_scope, fact_id)
    if latest is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "memory entry not found")
    raise VersionConflict(latest)


async def set_archived(
    session: AsyncSession,
    agent: Agent,
    binding_scope: str | None,
    fact_id: str,
    expected_version: int,
    archived: bool,
    actor: Actor,
) -> MemoryRecordOut:
    """Archive or restore one entry under compare-and-set. Already in the
    requested state at the current version: unchanged, no revision."""

    async with _transaction(session):
        _require_writes(agent, actor)
        await _admit(session, agent, binding_scope, actor)
        head = await _locked_head(session, agent.id, binding_scope, fact_id)
        if head is None:
            await _missing_or_conflict(session, agent.id, binding_scope, fact_id)
        if expected_version != head.version:
            raise VersionConflict(head.version)
        if record_of(head) is None:
            _unprocessable("this memory entry is malformed; forget it instead")
        _require_learning(agent, actor, _kind_of(head.value))
        if _archived_of(head.value) == archived:
            # Nothing to change: end the transaction to release the lock.
            await session.commit()
        else:
            snapshot = {**head.value, "archived": archived}
            # The flag's serialized size differs by a byte, so a restore can
            # push a full namespace over its cap: refuse before any change.
            await _enforce_bytes(session, agent.id, binding_scope, fact_id, snapshot)
            head.value = snapshot
            head.version += 1
            await session.flush()
            _append_revision(
                session,
                agent.id,
                binding_scope,
                fact_id,
                version=head.version,
                operation="archive" if archived else "restore",
                snapshot=snapshot,
                actor=actor,
            )
            await session.commit()
            _record_mutation(agent.id, binding_scope, fact_id, actor, deleted=None)
    await session.refresh(head)
    record = record_of(head)
    assert record is not None  # validated above, and archived is the only change
    return record


async def revert(
    session: AsyncSession,
    agent: Agent,
    binding_scope: str | None,
    fact_id: str,
    expected_version: int,
    to_version: int,
    actor: Actor,
) -> None:
    """Return an entry to an earlier version's snapshot as a NEW version, or
    remove its head when the target is absence (``to_version`` 0, or a
    deletion). History is never rewritten; the same identity is kept."""

    async with _transaction(session):
        _require_writes(agent, actor)
        await _admit(session, agent, binding_scope, actor)
        head = await _locked_head(session, agent.id, binding_scope, fact_id)
        if head is not None:
            current = head.version
        else:
            latest = await _latest_revision(session, agent.id, binding_scope, fact_id)
            if latest is None:
                raise HTTPException(status.HTTP_404_NOT_FOUND, "memory entry not found")
            current = latest
        if expected_version != current:
            raise VersionConflict(current)
        if to_version >= current:
            _unprocessable(f"to_version must be an earlier version than {current}")
        target: dict[str, Any] | None = None
        if to_version != 0:
            revision: MemoryRevision | None = await session.scalar(
                select(MemoryRevision).where(
                    MemoryRevision.agent_id == agent.id,
                    MemoryRevision.scope == _revision_scope(binding_scope),
                    MemoryRevision.fact_id == fact_id,
                    MemoryRevision.version == to_version,
                )
            )
            if revision is None:
                _unprocessable(f"version {to_version} is not in this entry's history")
            if revision.snapshot is not None:
                if not _valid_snapshot(revision.snapshot):
                    _unprocessable(f"version {to_version} is not a valid memory entry")
                target = dict(revision.snapshot)
        _require_learning(
            agent, actor, _kind_of(head.value) if head is not None else None, _kind_of(target)
        )
        version = current + 1
        if target is None:
            if head is not None:
                await session.delete(head)
        else:
            if head is None:
                await _check_capacity(session, agent.id, binding_scope)
            await _enforce_bytes(session, agent.id, binding_scope, fact_id, target)
            if head is None:
                session.add(
                    WorkflowStateEntry(
                        agent_id=agent.id,
                        binding_scope=binding_scope,
                        namespace=MEMORY_NAMESPACE,
                        key=fact_id,
                        value=target,
                        version=version,
                    )
                )
            else:
                head.value = target
                head.version = version
        await session.flush()
        _append_revision(
            session,
            agent.id,
            binding_scope,
            fact_id,
            version=version,
            operation="revert",
            snapshot=target,
            actor=actor,
            reverted_to_version=to_version,
        )
        await session.commit()
    deleted = (head is not None) if target is None else None
    _record_mutation(agent.id, binding_scope, fact_id, actor, deleted=deleted)


def _revision_out(row: MemoryRevision) -> MemoryRevisionOut:
    return MemoryRevisionOut(
        version=row.version,
        operation=row.operation,  # type: ignore[arg-type]
        actor=row.actor,
        source=row.source,
        conversation=row.conversation,
        created_at=row.created_at,
        reverted_to_version=row.reverted_to_version,
        snapshot=row.snapshot,
    )


async def detail(
    session: AsyncSession,
    agent_id: uuid.UUID,
    binding_scope: str | None,
    fact_id: str,
    before_version: int | None = None,
) -> MemoryDetailOut:
    """An entry, live or deleted, with one page of its newest history. A never
    created identity is 404."""

    head: WorkflowStateEntry | None = await session.scalar(
        _head_query(agent_id, binding_scope, fact_id)
    )
    latest = await _latest_revision(session, agent_id, binding_scope, fact_id)
    if head is None and latest is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "memory entry not found")
    query = select(MemoryRevision).where(
        MemoryRevision.agent_id == agent_id,
        MemoryRevision.scope == _revision_scope(binding_scope),
        MemoryRevision.fact_id == fact_id,
    )
    if before_version is not None:
        query = query.where(MemoryRevision.version < before_version)
    rows = list(
        await session.scalars(query.order_by(MemoryRevision.version.desc()).limit(HISTORY_PAGE + 1))
    )
    more = len(rows) > HISTORY_PAGE
    rows = rows[:HISTORY_PAGE]
    version = head.version if head is not None else latest
    assert version is not None
    return MemoryDetailOut(
        entry=record_of(head) if head is not None else None,
        deleted=head is None,
        version=version,
        history=[_revision_out(row) for row in rows],
        history_before=rows[-1].version if more and rows else None,
    )


async def _guidance_value(session: AsyncSession, agent_id: uuid.UUID) -> Any:
    entry: WorkflowStateEntry | None = await session.scalar(
        select(WorkflowStateEntry).where(
            WorkflowStateEntry.agent_id == agent_id,
            WorkflowStateEntry.binding_scope.is_(None),
            WorkflowStateEntry.namespace == MEMORY_NAMESPACE,
            WorkflowStateEntry.key == GUIDANCE_KEY,
        )
    )
    return entry.value if entry is not None else None


async def guidance_for(session: AsyncSession, agent: Agent) -> MemoryGuidanceOut:
    """The agent's effective guidance and its source (ADR 0239 selection)."""

    text_, source = effective_guidance(
        await _guidance_value(session, agent.id), agent.agent_wide_learning
    )
    return MemoryGuidanceOut(text=text_, source=source)


def _newest_first(records: list[MemoryRecordOut]) -> list[MemoryRecordOut]:
    # Newest updated first; ties fall back to scope then id, so the order is
    # stable across calls.
    return sorted(records, key=lambda r: (r.updated_at, r.scope, r.id), reverse=True)


async def context(
    session: AsyncSession, agent: Agent, channel_scope: str | None
) -> MemoryContextOut:
    """The bounded boot context over agent memory plus at most one channel."""

    scope_filter = (
        WorkflowStateEntry.binding_scope.is_(None)
        if channel_scope is None
        else or_(
            WorkflowStateEntry.binding_scope.is_(None),
            WorkflowStateEntry.binding_scope == channel_scope,
        )
    )
    heads = await session.scalars(
        select(WorkflowStateEntry).where(
            WorkflowStateEntry.agent_id == agent.id,
            WorkflowStateEntry.namespace == MEMORY_NAMESPACE,
            WorkflowStateEntry.key.regexp_match(_FACT_KEY_SQL),
            scope_filter,
        )
    )
    records = [r for r in (record_of(head) for head in heads) if r is not None]
    active = [r for r in records if not r.archived]
    instructions = _newest_first([r for r in active if r.kind == "instruction"])
    facts = _newest_first([r for r in active if r.kind == "fact"])
    shown_instructions = (
        instructions[:CONTEXT_MAX_INSTRUCTIONS] if agent.agent_wide_learning else []
    )
    shown_facts = facts[:CONTEXT_MAX_FACTS]
    return MemoryContextOut(
        agent_wide_learning=agent.agent_wide_learning,
        memory_writes=agent.memory_writes,
        guidance=await guidance_for(session, agent),
        instructions=shown_instructions,
        index=[
            MemoryIndexItemOut(id=r.id, scope=r.scope, title=r.title, version=r.version)
            for r in shown_facts
        ],
        totals=MemoryTotalsOut(
            instructions=len(instructions),
            facts=len(facts),
            archived=len(records) - len(active),
        ),
        omitted=MemoryOmittedOut(
            instructions=len(instructions) - len(shown_instructions),
            facts=len(facts) - len(shown_facts),
        ),
    )


def _escape_like(query: str) -> str:
    return query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _encode_cursor(updated_at: datetime, key: str) -> str:
    raw = json.dumps([updated_at.isoformat(), key]).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _decode_cursor(cursor: str) -> tuple[datetime, str]:
    try:
        raw = base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4))
        stamp, key = json.loads(raw)
        if not isinstance(stamp, str) or not isinstance(key, str):
            raise ValueError("cursor fields")
        return datetime.fromisoformat(stamp), key
    except (binascii.Error, ValueError, TypeError) as exc:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "malformed cursor") from exc


async def search(
    session: AsyncSession,
    agent_id: uuid.UUID,
    binding_scope: str | None,
    query: str,
    cursor: str | None,
    limit: int,
) -> MemorySearchOut:
    """Literal, case-insensitive search of titles and statements in one scope,
    archived entries included, newest updated first."""

    # The JSONB column's own indexing: ``value["k"]`` is ``->`` with a text key
    # and ``.astext`` is ``->>``, both typed correctly for their operands.
    value = WorkflowStateEntry.value
    pattern = f"%{_escape_like(query)}%"
    stmt = select(WorkflowStateEntry).where(
        WorkflowStateEntry.agent_id == agent_id,
        WorkflowStateEntry.binding_scope == binding_scope,
        WorkflowStateEntry.namespace == MEMORY_NAMESPACE,
        WorkflowStateEntry.key.regexp_match(_FACT_KEY_SQL),
        func.jsonb_typeof(value) == "object",
        value["kind"].astext.in_(KINDS),
        func.jsonb_typeof(value["title"]) == "string",
        func.jsonb_typeof(value["statement"]) == "string",
        or_(
            value["title"].astext.ilike(pattern, escape="\\"),
            value["statement"].astext.ilike(pattern, escape="\\"),
        ),
    )
    if cursor is not None:
        after_stamp, after_key = _decode_cursor(cursor)
        stmt = stmt.where(
            tuple_(WorkflowStateEntry.updated_at, WorkflowStateEntry.key)
            < tuple_(literal(after_stamp), literal(after_key))
        )
    rows = list(
        await session.scalars(
            stmt.order_by(
                WorkflowStateEntry.updated_at.desc(), WorkflowStateEntry.key.desc()
            ).limit(limit + 1)
        )
    )
    more = len(rows) > limit
    rows = rows[:limit]
    entries = [r for r in (record_of(row) for row in rows) if r is not None]
    next_cursor = _encode_cursor(rows[-1].updated_at, rows[-1].key) if more and rows else None
    return MemorySearchOut(entries=entries, next_cursor=next_cursor)

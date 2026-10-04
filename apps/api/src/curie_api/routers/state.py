"""Durable agent-scoped key/value state (#23, #248).

A small compare-and-set KV/document store: namespace + key per agent, an
arbitrary-JSON value, Postgres JSONB backing. Operations: get / put-with-CAS /
list / delete / append. Two hard non-goals keep this from becoming a database
product: there is no query language (get-by-key + list-by-namespace only), and
both a single value and a whole namespace are size-capped (#248). This is the
API surface the approvals epic (#22) and other cross-turn workflow state
consume. It is also exposed to bundle code (#249) via the auto-mounted
``curie-state`` MCP server and the ``CURIE_STATE_URL`` / ``CURIE_STATE_TOKEN``
boot-env pair, so a skill reads and writes state without shipping its own server;
the sandbox authenticates with a scoped ``state`` token (ADR-0033), never the
platform key. On the ``memory`` namespace a sandbox credential is further held to
its own channel, to fact keys, and to the sender its per-turn credential names
(ADR-0188); on ``transcript`` it is held to its own channel's threads (#3767).
"""

import enum
import hashlib
import logging
import re
import uuid
from collections import OrderedDict
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Annotated, Any, Literal, NoReturn

import redis.asyncio as redis
from curie_internal import sandbox_token
from curie_telemetry import record_metric
from fastapi import APIRouter, Depends, Header, HTTPException, Request, Response, status
from sqlalchemy import Text, cast, delete, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from curie_api.crud import agents as crud_agents
from curie_api.crud import channels as crud_channels
from curie_api.schemas.state import (
    MemoryTurnClosedIn,
    StateAppendIn,
    StateEntryOut,
    StateEntryPut,
    StateNamespaceOut,
)

from .. import state_mutation, threadkeys, transcripts
from ..auth import require_internal_worker_token, verify_platform_key
from ..config import get_settings
from ..deps import SessionDep
from ..models import ThreadTranscript, WorkflowStateEntry
from ..transcripts import TRANSCRIPT_NAMESPACE
from ..transcripts import json_size as _json_size

logger = logging.getLogger(__name__)

# Two scoped-token scopes the state router accepts (ADR-0033). The BROAD scope is
# minted for the runner's own memory/history loaders and memory tools, which MUST
# read the reserved namespaces to rehydrate the agent across a suspend/resume
# (and read and write transcripts). It reaches every namespace, but on ``memory``
# its claims narrow it (ADR-0188, ``_check_memory_reach``): ``binding`` names the
# one channel whose memory it may reach, and ``memory`` says whether it may write
# (only the per-turn credential the worker puts on the turn's ACI ``Event`` is
# ``"write"``, and it carries the ``sender`` the API stamps as the fact's
# author). On ``transcript`` the ``binding`` claim likewise holds it to its own
# channel's threads (#3767, ``_check_transcript_reach``). The NARROW scope is
# minted for the bundle-facing ``CURIE_STATE_TOKEN`` and is refused on the reserved namespaces by
# ``forbid_reserved_namespace`` below -- so a skill using the mounted state
# interface (the ``curie-state`` MCP tools or a direct ``CURIE_STATE_URL``
# call) cannot reach the memory/history ports even by composing the URL itself.
# Both strings are mirrored at the worker mint site (``binding.py``); a
# byte-identical string on both sides is the contract, like ``sandbox_token``.
STATE_SCOPE = "state"
STATE_APP_SCOPE = "state.app"

# Namespaces owned by the memory (#264) and history (#20) ports; the narrow
# app-scoped (bundle) token may not touch them. Literals rather than an import
# because ``routers.memory`` imports ``enforce_caps`` from THIS module (a real
# import cycle otherwise). Mirrors the runner client's ``RESERVED_NAMESPACES``
# (``runner/src/curie_runner/state.py``) and ``memory.MEMORY_NAMESPACE`` /
# the history transcript key -- a bundle wanting durable memory uses the remember
# tool, not raw state. A future fixed namespace must be added here too.
MEMORY_NAMESPACE = "memory"
RESERVED_NAMESPACES = frozenset({MEMORY_NAMESPACE, TRANSCRIPT_NAMESPACE})

# The only memory keys a sandbox credential may write (ADR-0188): the fact ids
# the runner's memory tools mint. Mirrors ``memory_facts._FACT_ID`` in the
# runner; ``tests/test_memory_fact_key_parity.py`` pins the two. ``guidance``
# and the legacy ``log`` are written with the platform key only. ``\Z``, not
# ``$``: ``$`` also matches before a trailing newline.
_FACT_KEY = re.compile(r"^fact-[0-9a-f]{32}\Z")

# How long the API remembers that a turn ended (#3776): the longest a per-turn
# memory credential can live, the worker's ``binding.SANDBOX_TOKEN_TTL_SECONDS``
# (duplicated, since neither service imports the other's package). The record
# only has to outlive the credential it refuses.
MEMORY_TURN_CLOSED_TTL_S = 24 * 60 * 60
TURN_ENDED = "this conversation's turn has ended"


class StateCaller(enum.Enum):
    """Which credential authorized a state-router request, and thus how far it
    reaches. PLATFORM (the shared key) is unrestricted. STATE (the broad scoped
    token: the runner's loaders and memory tools) reaches every namespace, but
    on ``memory`` only as far as its claims allow (ADR-0188), and on
    ``transcript`` only its own channel's threads (#3767). APP (the narrow
    bundle token) is refused on ``RESERVED_NAMESPACES``."""

    PLATFORM = "platform"
    STATE = "state"
    APP = "app"


@dataclass(frozen=True)
class StatePrincipal:
    """Who a state-router request is, as far as the router needs to know.

    ``binding``, ``memory``, ``sender`` and ``turn`` are the ADR-0188 claims of a
    STATE token (all None for the platform key and the app token). A STATE token
    with no ``memory`` claim was minted by a pre-ADR-0188 worker: ``legacy``,
    which fails closed on memory (read-only, agent memory only)."""

    caller: StateCaller
    binding: str | None = None
    memory: Literal["read", "write"] | None = None
    sender: str | None = None
    turn: str | None = None

    @property
    def legacy(self) -> bool:
        return self.caller is StateCaller.STATE and self.memory is None


def _str_claim(payload: dict[str, Any], name: str) -> str | None:
    value = payload.get(name)
    return value if isinstance(value, str) else None


def _state_principal(payload: dict[str, Any]) -> StatePrincipal:
    if "memory" not in payload:
        return StatePrincipal(StateCaller.STATE)
    # Anything but an explicit "write" is read-only: fail closed.
    memory: Literal["read", "write"] = "write" if payload.get("memory") == "write" else "read"
    return StatePrincipal(
        StateCaller.STATE,
        binding=_str_claim(payload, "binding"),
        memory=memory,
        sender=_str_claim(payload, "sender"),
        turn=_str_claim(payload, "turn"),
    )


async def require_state_access(
    agent_id: uuid.UUID,
    x_api_key: Annotated[str | None, Header()] = None,
) -> StatePrincipal:
    """State-router auth (ADR-0033): the platform key (trusted callers) OR a
    scoped token bound to this path's ``agent_id`` (the sandbox). The broad
    ``state`` scope (the runner's loaders and memory tools) reaches every
    namespace, held on ``memory`` to its claims by ``_check_memory_reach``
    (ADR-0188); the narrow app scope (the bundle-facing ``CURIE_STATE_TOKEN``)
    is refused on the reserved namespaces by ``forbid_reserved_namespace``.
    Every other router keeps the platform-key-only ``require_api_key``. Returns
    the caller and its verified claims so the guards can apply the right
    reach."""

    if verify_platform_key(x_api_key):
        return StatePrincipal(StateCaller.PLATFORM)
    if x_api_key is not None:
        api_key = get_settings().api_key
        agent = str(agent_id)
        payload = sandbox_token.decode(x_api_key, api_key, agent=agent, scope=STATE_SCOPE)
        if payload is not None:
            return _state_principal(payload)
        if sandbox_token.verify(x_api_key, api_key, agent=agent, scope=STATE_APP_SCOPE):
            return StatePrincipal(StateCaller.APP)
    raise HTTPException(
        status.HTTP_401_UNAUTHORIZED, detail="missing or invalid credential"
    )


def _state_path(
    namespace: str, key: str | None, kind: str | None = None, address: str | None = None
) -> str:
    """The request's state path, for log lines (never carries a credential)."""
    parts = ["state"]
    if kind is not None and address is not None:
        parts += ["bindings", kind, address]
    parts.append(namespace)
    if key is not None:
        parts.append(key)
    return "/".join(parts)


def _refuse(principal: StatePrincipal, agent_id: uuid.UUID, path: str, reason: str) -> NoReturn:
    """Log and raise a 403 for a memory request the credential may not make.

    The log names the agent, the path and the turn claim, never the token."""

    if principal.legacy:
        logger.warning(
            "state: refused legacy sandbox token (no memory claim) for agent %s on %s: %s",
            agent_id,
            path,
            reason,
        )
    else:
        logger.warning(
            "state: refused sandbox credential for agent %s on %s (turn %s): %s",
            agent_id,
            path,
            principal.turn,
            reason,
        )
    raise HTTPException(status.HTTP_403_FORBIDDEN, reason)


def _check_memory_reach(
    principal: StatePrincipal,
    agent_id: uuid.UUID,
    namespace: str,
    *,
    requested_binding: str | None,
    key: str | None,
    write: bool,
    path: str,
    append: bool = False,
) -> None:
    """ADR-0188: what a sandbox (STATE) credential may do on ``memory``.

    The platform key keeps full reach, the app token is already fenced off by
    ``forbid_reserved_namespace``, and other namespaces are unchanged. Runs
    before ``_binding_scope``'s database lookup, so the 403 for another
    channel cannot be used to probe which bindings exist."""

    if principal.caller is not StateCaller.STATE or namespace != MEMORY_NAMESPACE:
        return
    if requested_binding is not None and (
        principal.legacy or principal.binding != requested_binding
    ):
        _refuse(principal, agent_id, path, "credential is scoped to another channel")
    if not write:
        return
    if principal.memory != "write":
        _refuse(principal, agent_id, path, "memory credential is read-only")
    if append:
        _refuse(
            principal,
            agent_id,
            path,
            "memory facts are written with PUT; append is not allowed with a sandbox credential",
        )
    if key is None or _FACT_KEY.fullmatch(key) is None:
        _refuse(
            principal, agent_id, path, "only fact keys are writable with a sandbox credential"
        )


def _transcript_key_admitted(principal: StatePrincipal, agent_id: uuid.UUID, key: str) -> bool:
    """Whether a non-legacy sandbox credential reaches the thread ``key`` (#3767).

    A transcript key is a thread key, mapped back to its binding by
    ``threadkeys.transcript_binding``. A bound credential reaches its own
    binding's threads, whatever identity segment the key carries. The unbound
    credential (a targetless cron's boot env names no binding) reaches only its
    own agent's ``@cron`` threads. A key no producer builds maps to no binding
    and is reached by no sandbox credential."""

    key_binding = threadkeys.transcript_binding(key)
    if key_binding is None:
        return False
    if principal.binding is None:
        return key_binding == f"@cron:{agent_id}"
    return key_binding == principal.binding


# Agents already warned about a legacy token in this process, oldest first.
_LEGACY_WARNED_LIMIT = 1024
_legacy_warned: OrderedDict[uuid.UUID, None] = OrderedDict()


def _note_legacy_transcript_use(agent_id: uuid.UUID, path: str) -> None:
    """Log and count a pre-ADR-0188 token's transcript request, which is allowed.

    Refusing it would cut off every warm sandbox's history at API deploy; the
    window closes when the last such token expires, at most 24 hours after the
    worker upgrade. The log names the agent and the path, never the token.

    A warm sandbox makes several such requests per turn, so the warning is
    logged once per agent per process, tracked in a small bounded set that
    evicts the oldest agent first. The metric still counts every request."""

    if agent_id not in _legacy_warned:
        _legacy_warned[agent_id] = None
        if len(_legacy_warned) > _LEGACY_WARNED_LIMIT:
            _legacy_warned.popitem(last=False)
        logger.warning(
            "state: allowed legacy sandbox token (no memory claim) for agent %s on %s; "
            "transcript reach is unscoped until the sandbox is replaced",
            agent_id,
            path,
        )
    record_metric(
        "curie.state.legacy_token",
        attributes={"service.name": "curie-api", "namespace": TRANSCRIPT_NAMESPACE},
    )


def _check_transcript_reach(
    principal: StatePrincipal,
    agent_id: uuid.UUID,
    namespace: str,
    *,
    requested_binding: str | None,
    key: str | None,
    path: str,
) -> None:
    """#3767: what a sandbox (STATE) credential may reach on ``transcript``.

    On a binding path, the path's binding must be the credential's; with a key,
    the key's binding must be too (``_transcript_key_admitted``), so neither
    can launder the other. A listing (``key`` None) is filtered afterwards by
    ``_visible_transcripts``. A legacy token keeps its reach, with a warning and
    a metric. The platform key is unaffected and the app token is already
    fenced off by ``forbid_reserved_namespace``. Runs before any database read,
    so a refused read cannot trigger pre-identity adoption either."""

    if principal.caller is not StateCaller.STATE or namespace != TRANSCRIPT_NAMESPACE:
        return
    if principal.legacy:
        _note_legacy_transcript_use(agent_id, path)
        return
    if requested_binding is not None and principal.binding != requested_binding:
        _refuse(principal, agent_id, path, "credential is scoped to another channel")
    if key is not None and not _transcript_key_admitted(principal, agent_id, key):
        _refuse(
            principal, agent_id, path, "transcript belongs to another channel or no channel"
        )


def _visible_transcripts(
    principal: StatePrincipal, agent_id: uuid.UUID, namespace: str, rows: list[StateEntryOut]
) -> list[StateEntryOut]:
    """A transcript listing narrowed to the threads a sandbox credential reaches."""

    if (
        principal.caller is not StateCaller.STATE
        or principal.legacy
        or namespace != TRANSCRIPT_NAMESPACE
    ):
        return rows
    return [row for row in rows if _transcript_key_admitted(principal, agent_id, row.key)]


def _closed_turn_key(agent_id: uuid.UUID, turn: str) -> str:
    """The Valkey record that ``turn`` of ``agent_id`` has ended (#3776). Keyed
    by agent too, so one agent closing a turn id cannot refuse another's."""

    return f"{get_settings().worker_key_prefix}:memory-turn-closed:{agent_id}:{turn}"


async def _check_turn_open(
    principal: StatePrincipal,
    agent_id: uuid.UUID,
    namespace: str,
    request: Request,
    path: str,
) -> None:
    """#3776: refuse a memory write made with a turn credential whose turn the
    worker has reported ended, even though the credential has not expired.

    Runs after ``_check_memory_reach`` on every write route. Reads are not
    checked, and the platform key (no turn claim) is unaffected. A Valkey
    failure is a 503, never a silently accepted write."""

    if (
        principal.caller is not StateCaller.STATE
        or namespace != MEMORY_NAMESPACE
        or principal.turn is None
    ):
        return
    client: redis.Redis = request.app.state.valkey
    try:
        closed = await client.exists(_closed_turn_key(agent_id, principal.turn))
    except (redis.RedisError, OSError) as exc:
        logger.warning("state: could not check memory turn %s: %r", principal.turn, exc)
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE, "could not check this credential's turn"
        ) from exc
    if closed:
        _refuse(principal, agent_id, path, TURN_ENDED)


def _stamp_author(
    principal: StatePrincipal,
    agent_id: uuid.UUID,
    namespace: str,
    data: StateEntryPut,
    path: str,
) -> StateEntryPut:
    """ADR-0188: a sandbox fact's author is the per-turn credential's ``sender``
    claim, whatever the body says, and its ``stated_at`` is the server's clock,
    so a sandbox write cannot backdate a fact. ``session_id`` is kept as sent.
    The platform key keeps the body as given."""

    if principal.caller is not StateCaller.STATE or namespace != MEMORY_NAMESPACE:
        return data
    if principal.sender is None:
        _refuse(principal, agent_id, path, "memory write credential names no sender")
    if not isinstance(data.value, dict):
        raise HTTPException(
            422,
            "a memory fact written with a sandbox credential must be a JSON object",
        )
    # The runner's ``memory_facts._now`` format: RFC3339 UTC with a ``Z``.
    stated_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    stamped = {**data.value, "author": principal.sender, "stated_at": stated_at}
    return data.model_copy(update={"value": stamped})


async def _binding_scope(
    session: AsyncSession, agent_id: uuid.UUID, kind: str, address: str
) -> str:
    """The `workflow_state_entries.binding_scope` value for one named binding
    (#1525 follow-up): `"{kind}:{address}"`, once confirmed to actually belong
    to this agent.

    For general state this is a partition key, not a permission: a sandbox
    credential for this agent may use any of the agent's bindings, the same as
    any `namespace`/`key` it could always freely choose. Checking it against
    `agent_channels` is a correctness guard -- a typo or a stale binding name
    fails loudly as 404 instead of silently opening a new, orphaned partition
    that corresponds to nothing.

    For the `memory` namespace the binding IS a permission (ADR-0188, which
    reverses the #1525 follow-up's rejection of a binding claim for memory): one
    channel's memory can hold a direct message, so a sandbox credential reaches
    only the channel its `binding` claim names. That check is
    `_check_memory_reach`, which callers run before this lookup so a 403 cannot
    be used to learn which bindings exist.

    No caller here NAMES an adapter -- the state API has no such parameter --
    and an agent's rows on one pair share this one scope whichever identity
    holds them, so this checks for any row of THIS agent on the pair rather
    than resolving the default identity's route the way `crud.channels.binding_for_route`
    does for a turn (an identity-narrowed read would 404 every named-identity
    binding's own state).
    """

    if not await crud_channels.agent_holds_channel_pair(session, agent_id, kind, address):
        raise HTTPException(
            status.HTTP_404_NOT_FOUND, f"this agent has no {kind}:{address} binding"
        )
    return f"{kind}:{address}"


async def forbid_reserved_namespace(
    namespace: str,
    principal: Annotated[StatePrincipal, Depends(require_state_access)],
) -> None:
    """Server-side backstop for the reserved-namespace rule (#249): a narrow
    app-scoped (bundle) token may not read or write the memory/transcript
    namespaces -- those belong to the memory (#264) and history (#20) ports. The
    platform key and the broad ``state`` token (the loaders) are unrestricted.
    Without this a skill could bypass the ``curie-state`` tool's own client-side
    refusal by composing ``CURIE_STATE_URL`` directly with the token it holds;
    here the token it holds is simply refused. The broad token's narrower reach
    on ``memory`` (ADR-0188) is ``_check_memory_reach``."""
    if principal.caller is StateCaller.APP and namespace in RESERVED_NAMESPACES:
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            f"namespace {namespace!r} is reserved by the platform "
            f"(reserved: {', '.join(sorted(RESERVED_NAMESPACES))}); "
            "use the memory or history tools instead",
        )


router = APIRouter(
    prefix="/agents", tags=["state"], dependencies=[Depends(require_state_access)]
)


# Advisory-lock class for the per-agent namespace-count cap (#933). The
# TWO-argument ``pg_advisory_xact_lock(int4, int4)`` form is used deliberately:
# Postgres keeps the two-int4 lock space entirely separate from the
# one-argument bigint space, so this can never collide with a
# ``pg_advisory_lock(<bigint>)`` taken anywhere else -- including the test-only
# write gates in apps/api/tests/, which use the one-arg form. The number is the
# issue.
_NAMESPACE_LOCK_CLASS = 933


def _namespace_lock_key(agent_id: uuid.UUID) -> int:
    """A stable int4 advisory-lock key for one agent (#933).

    Deterministic in every process and across restarts, which is the whole
    point: Python's builtin ``hash()`` is PER-PROCESS randomized (PYTHONHASHSEED),
    so two API workers would derive different keys for the same agent and the
    lock would silently stop serializing anything. Hence hashlib. blake2b over
    the UUID's 16 raw bytes, truncated to a signed int4 because
    ``pg_advisory_xact_lock(int4, int4)`` takes int4s.

    A collision between two DIFFERENT agents is SAFE: it only makes those two
    agents serialize their new-namespace creations against each other for the
    few statements the lock is held. It can never produce a wrong verdict,
    because every query in the critical section -- the existence probe, the
    re-check, and the ``count(distinct namespace)`` -- is still filtered by
    ``agent_id``.
    """
    digest = hashlib.blake2b(agent_id.bytes, digest_size=4).digest()
    return int.from_bytes(digest, "big", signed=True)


async def _namespace_exists(
    session: AsyncSession, agent_id: uuid.UUID, scope: str | None, namespace: str
) -> bool:
    """Does this agent already have any row in ``namespace``? (#933)

    Extracted so the unlocked pre-check and the re-check under the advisory lock
    are provably the same query; a future edit cannot let them drift apart.
    """
    found = await session.scalar(
        select(WorkflowStateEntry.namespace)
        .where(
            WorkflowStateEntry.agent_id == agent_id,
            WorkflowStateEntry.binding_scope == scope,
            WorkflowStateEntry.namespace == namespace,
        )
        .limit(1)
    )
    return found is not None


async def enforce_caps(
    session: AsyncSession,
    agent_id: uuid.UUID,
    scope: str | None,
    namespace: str,
    key: str,
    value: Any,
    *,
    reserve_bytes: int | None = None,
) -> None:
    """Reject a write that breaks the per-value or per-namespace size cap (#248).

    ``reserve_bytes`` (#2927) additionally refuses a value that fits the
    per-value cap but leaves fewer than that many bytes free under it. The
    runner's transcript appends set it so the worker's publication outcome
    append always has room; the refusal is the runner's compaction trigger, not
    a persistence failure, so it records no failure metric.

    The namespace total counts the incoming value plus every *other* key already
    in the namespace (the key being written replaces its own prior size). Both
    the byte totals and the namespace-count cap below are scoped by `scope`
    (#1525 follow-up): a memory=False agent's bindings are meant to be
    isolated, so one binding filling its own namespace or hitting the
    namespace-count cap must not block or inflate another's unrelated usage.
    """
    settings = get_settings()
    value_bytes = _json_size(value)
    if value_bytes > settings.state_max_value_bytes:
        raise HTTPException(
            413,
            f"value for key {key!r} is {value_bytes} bytes, over the "
            f"{settings.state_max_value_bytes}-byte per-value cap",
        )
    if reserve_bytes is not None and settings.state_max_value_bytes - value_bytes < reserve_bytes:
        raise HTTPException(
            413,
            f"value for key {key!r} is {value_bytes} bytes, leaving under the "
            f"{reserve_bytes}-byte reserve of the "
            f"{settings.state_max_value_bytes}-byte per-value cap",
        )

    # Cap unit is compact json.dumps (ensure_ascii=True). jsonb::text is not
    # byte-identical: object whitespace makes it an *upper* bound for ASCII
    # JSON, but UTF-8 output is shorter than ensure_ascii escapes, so a
    # multibyte sibling must take the exact fallback. The aggregates return
    # two scalars; fetching sibling values is only the over-bound path.
    sibling_filter = (
        WorkflowStateEntry.agent_id == agent_id,
        WorkflowStateEntry.binding_scope == scope,
        WorkflowStateEntry.namespace == namespace,
        WorkflowStateEntry.key != key,
    )
    sibling_text = (
        select(cast(WorkflowStateEntry.value, Text).label("json_text"))
        .where(*sibling_filter)
        .subquery()
    )
    sibling_bound, has_multibyte = (
        await session.execute(
            select(
                func.coalesce(func.sum(func.octet_length(sibling_text.c.json_text)), 0),
                func.coalesce(
                    func.bool_or(
                        func.octet_length(sibling_text.c.json_text)
                        != func.length(sibling_text.c.json_text)
                    ),
                    False,
                ),
            )
        )
    ).one()
    sibling_bound_bytes = int(sibling_bound or 0)
    if bool(has_multibyte) or (
        value_bytes + sibling_bound_bytes > settings.state_max_namespace_bytes
    ):
        others = await session.execute(
            select(WorkflowStateEntry.key, WorkflowStateEntry.value).where(*sibling_filter)
        )
        sizes = {key: value_bytes}
        sizes.update((other_key, _json_size(v)) for other_key, v in others)
        namespace_bytes = sum(sizes.values())
        if namespace_bytes > settings.state_max_namespace_bytes:
            # Name the key holding the most bytes (#2820), which is often not
            # the key whose write was refused.
            largest = max(sizes, key=lambda k: (sizes[k], k == key))
            raise HTTPException(
                413,
                f"namespace {namespace!r} would be {namespace_bytes} bytes, over the "
                f"{settings.state_max_namespace_bytes}-byte per-namespace cap; "
                f"largest key {largest!r} is {sizes[largest]} bytes",
            )

    # Per-agent namespace-count cap (#852): refuse only a NEW namespace, and only
    # once the agent is at its limit -- writes to an existing namespace never hit
    # this. Without it a sandbox could loop creating unbounded namespaces, each
    # under the byte caps, after #840 made the namespace agent-chosen.
    #
    # #933: the check and the caller's INSERT are separate statements, so the
    # bare check was a TOCTOU a concurrent burst could walk straight past (N
    # requests to N brand-new namespaces all read cap-1 and all pass). The guard
    # below is DOUBLE-CHECKED LOCKING: an unlocked pre-check keeps the hot path
    # free, and only a would-be namespace CREATION serializes on a per-agent
    # advisory lock held to COMMIT/ROLLBACK.

    # 1. Hot path. Every write into an already-existing namespace returns here,
    #    at exactly the cost of the single probe this code ran before #933 --
    #    no lock, no extra round trip. dadf93e2's "writes to an existing
    #    namespace are unaffected" is load-bearing and is preserved literally.
    if await _namespace_exists(session, agent_id, scope, namespace):
        return

    # 2. Serialize the creation. Transaction-level, so it is released by the
    #    COMMIT or the ROLLBACK with no explicit unlock and no leak path when
    #    the 403 below propagates. This REQUIRES a transaction to already be
    #    open -- outside one the lock would be released immediately and this
    #    guard would be vacuous. The byte-cap SQL bound SELECT above runs
    #    unconditionally and autobegins it; moving this block above those
    #    queries would silently make the lock meaningless.
    #
    #    LOCK ORDERING (no deadlock cycle exists, and this is what a future
    #    change would break): the advisory lock is only ever requested when the
    #    namespace has no rows for this agent, so no ``SELECT ... FOR UPDATE``
    #    row lock is held at that moment -- in ``append_state`` and in
    #    ``routers/memory.py`` a FOR UPDATE that matches zero rows takes no
    #    lock, and any caller that DOES hold a row lock is by definition writing
    #    an already-existing namespace and returned at step 1. The order is
    #    strictly one-way, advisory lock -> row locks. Moving a FOR UPDATE above
    #    the existence check, or making this lock unconditional, reopens that
    #    analysis from scratch.
    await session.execute(
        text("SELECT pg_advisory_xact_lock(:cls, :key)"),
        {"cls": _NAMESPACE_LOCK_CLASS, "key": _namespace_lock_key(agent_id)},
    )

    # 3. Re-check under the lock. A sibling request may have created this exact
    #    namespace while we waited; refusing it then would be a FALSE POSITIVE
    #    -- two concurrent writes to the same brand-new namespace must both
    #    succeed when the agent has room. Not optional.
    #
    #    READ COMMITTED DEPENDENCY: this re-check and the count below only see
    #    the sibling's commit because READ COMMITTED gives each statement a
    #    fresh snapshot. Under REPEATABLE READ or SERIALIZABLE the snapshot
    #    predates that commit and this guard degrades SILENTLY -- no error, just
    #    the old overshoot plus a spurious 403 here. Nothing sets an isolation
    #    level today; changing that breaks this.
    if await _namespace_exists(session, agent_id, scope, namespace):
        return

    namespace_count = await session.scalar(
        select(func.count(func.distinct(WorkflowStateEntry.namespace))).where(
            WorkflowStateEntry.agent_id == agent_id,
            WorkflowStateEntry.binding_scope == scope,
        )
    )
    if (namespace_count or 0) >= settings.state_max_namespaces:
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            f"agent is at its {settings.state_max_namespaces}-namespace cap; "
            f"delete a namespace or reuse an existing one before creating "
            f"{namespace!r}",
        )


def _transcript_out(row: ThreadTranscript) -> StateEntryOut:
    """A transcript row in the state API's entry shape (ADR-0170)."""
    return StateEntryOut(
        namespace=TRANSCRIPT_NAMESPACE,
        key=row.thread_key,
        value=row.value,
        version=row.version,
        updated_at=row.updated_at,
    )


async def _get_entry(
    session: AsyncSession, agent_id: uuid.UUID, scope: str | None, namespace: str, key: str
) -> WorkflowStateEntry | None:
    entry: WorkflowStateEntry | None = await session.scalar(
        select(WorkflowStateEntry).where(
            WorkflowStateEntry.agent_id == agent_id,
            WorkflowStateEntry.binding_scope == scope,
            WorkflowStateEntry.namespace == namespace,
            WorkflowStateEntry.key == key,
        )
    )
    return entry


async def _get_entry_locked(
    session: AsyncSession, agent_id: uuid.UUID, scope: str | None, namespace: str, key: str
) -> WorkflowStateEntry | None:
    """Same lookup as ``_get_entry``, row-locked for a read-modify-write (#2927).

    Used by both a CAS put and an append: a plain read let a concurrent write
    commit between the version check and the write, and the later write then
    overwrote it.
    """

    entry: WorkflowStateEntry | None = await session.scalar(
        select(WorkflowStateEntry)
        .where(
            WorkflowStateEntry.agent_id == agent_id,
            WorkflowStateEntry.binding_scope == scope,
            WorkflowStateEntry.namespace == namespace,
            WorkflowStateEntry.key == key,
        )
        .with_for_update()
    )
    return entry


async def _put_state(
    agent_id: uuid.UUID,
    scope: str | None,
    namespace: str,
    key: str,
    data: StateEntryPut,
    session: AsyncSession,
) -> StateEntryOut:
    # Unknown agent is a 404 (the FK would also reject, but this is the clear
    # signal). expected_version opts into compare-and-set.
    if await crud_agents.get_agent(session, agent_id) is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "agent not found")
    if namespace == TRANSCRIPT_NAMESPACE:
        row = await transcripts.put(
            session, agent_id, scope, key, data.value, data.expected_version
        )
        return _transcript_out(row)
    await enforce_caps(session, agent_id, scope, namespace, key, data.value)
    entry = await _get_entry_locked(session, agent_id, scope, namespace, key)
    if entry is None:
        if data.expected_version is not None:
            # A CAS put that expects a prior version cannot create the entry.
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                "version mismatch: entry does not exist yet",
            )
        entry = WorkflowStateEntry(
            agent_id=agent_id, binding_scope=scope, namespace=namespace, key=key, value=data.value
        )
        session.add(entry)
    else:
        if data.expected_version is not None and data.expected_version != entry.version:
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                f"version mismatch: expected {data.expected_version}, "
                f"stored {entry.version}",
            )
        entry.value = data.value
        entry.version += 1
    await session.commit()
    await session.refresh(entry)
    return StateEntryOut.model_validate(entry)


@router.put(
    "/{agent_id}/state/{namespace}/{key}",
    response_model=StateEntryOut,
    dependencies=[Depends(forbid_reserved_namespace)],
)
async def put_state(
    agent_id: uuid.UUID,
    namespace: str,
    key: str,
    data: StateEntryPut,
    session: SessionDep,
    request: Request,
    principal: Annotated[StatePrincipal, Depends(require_state_access)],
) -> StateEntryOut:
    path = _state_path(namespace, key)
    _check_memory_reach(
        principal, agent_id, namespace, requested_binding=None, key=key, write=True, path=path
    )
    _check_transcript_reach(
        principal,
        agent_id,
        namespace,
        requested_binding=None,
        key=key,
        path=path,
    )
    await _check_turn_open(principal, agent_id, namespace, request, path)
    data = _stamp_author(principal, agent_id, namespace, data, path)
    return await _put_state(agent_id, None, namespace, key, data, session)


@router.put(
    "/{agent_id}/state/bindings/{kind}/{address}/{namespace}/{key}",
    response_model=StateEntryOut,
    dependencies=[Depends(forbid_reserved_namespace)],
)
async def put_state_for_binding(
    agent_id: uuid.UUID,
    kind: str,
    address: str,
    namespace: str,
    key: str,
    data: StateEntryPut,
    session: SessionDep,
    request: Request,
    principal: Annotated[StatePrincipal, Depends(require_state_access)],
) -> StateEntryOut:
    path = _state_path(namespace, key, kind, address)
    _check_memory_reach(
        principal,
        agent_id,
        namespace,
        requested_binding=f"{kind}:{address}",
        key=key,
        write=True,
        path=path,
    )
    _check_transcript_reach(
        principal,
        agent_id,
        namespace,
        requested_binding=f"{kind}:{address}",
        key=key,
        path=path,
    )
    await _check_turn_open(principal, agent_id, namespace, request, path)
    data = _stamp_author(principal, agent_id, namespace, data, path)
    scope = await _binding_scope(session, agent_id, kind, address)
    return await _put_state(agent_id, scope, namespace, key, data, session)


async def _append_state(
    agent_id: uuid.UUID,
    scope: str | None,
    namespace: str,
    key: str,
    data: StateAppendIn,
    session: AsyncSession,
) -> StateEntryOut:
    """Append an item to a log-shaped (JSON array) entry (#248).

    Creates the entry as a single-element array if absent; otherwise the stored
    value must already be an array, else the append is a 409. Subject to the
    same per-value and per-namespace size caps as a put.
    """
    if await crud_agents.get_agent(session, agent_id) is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "agent not found")
    if namespace == TRANSCRIPT_NAMESPACE:
        row = await transcripts.append(
            session, agent_id, scope, key, data.item, data.reserve_bytes
        )
        return _transcript_out(row)
    entry = await _get_entry_locked(session, agent_id, scope, namespace, key)
    if entry is None:
        new_value = [data.item]
        await enforce_caps(
            session, agent_id, scope, namespace, key, new_value, reserve_bytes=data.reserve_bytes
        )
        entry = WorkflowStateEntry(
            agent_id=agent_id, binding_scope=scope, namespace=namespace, key=key, value=new_value
        )
        session.add(entry)
    else:
        if not isinstance(entry.value, list):
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                "cannot append: stored value is not a JSON array",
            )
        new_value = [*entry.value, data.item]
        await enforce_caps(
            session, agent_id, scope, namespace, key, new_value, reserve_bytes=data.reserve_bytes
        )
        entry.value = new_value
        entry.version += 1
    await session.commit()
    await session.refresh(entry)
    return StateEntryOut.model_validate(entry)


@router.post(
    "/{agent_id}/state/{namespace}/{key}/append",
    response_model=StateEntryOut,
    dependencies=[Depends(forbid_reserved_namespace)],
)
async def append_state(
    agent_id: uuid.UUID,
    namespace: str,
    key: str,
    data: StateAppendIn,
    session: SessionDep,
    principal: Annotated[StatePrincipal, Depends(require_state_access)],
) -> StateEntryOut:
    _check_memory_reach(
        principal,
        agent_id,
        namespace,
        requested_binding=None,
        key=key,
        write=True,
        append=True,
        path=_state_path(namespace, key),
    )
    _check_transcript_reach(
        principal,
        agent_id,
        namespace,
        requested_binding=None,
        key=key,
        path=_state_path(namespace, key),
    )
    return await _append_state(agent_id, None, namespace, key, data, session)


@router.post(
    "/{agent_id}/state/bindings/{kind}/{address}/{namespace}/{key}/append",
    response_model=StateEntryOut,
    dependencies=[Depends(forbid_reserved_namespace)],
)
async def append_state_for_binding(
    agent_id: uuid.UUID,
    kind: str,
    address: str,
    namespace: str,
    key: str,
    data: StateAppendIn,
    session: SessionDep,
    principal: Annotated[StatePrincipal, Depends(require_state_access)],
) -> StateEntryOut:
    _check_memory_reach(
        principal,
        agent_id,
        namespace,
        requested_binding=f"{kind}:{address}",
        key=key,
        write=True,
        append=True,
        path=_state_path(namespace, key, kind, address),
    )
    _check_transcript_reach(
        principal,
        agent_id,
        namespace,
        requested_binding=f"{kind}:{address}",
        key=key,
        path=_state_path(namespace, key, kind, address),
    )
    scope = await _binding_scope(session, agent_id, kind, address)
    return await _append_state(agent_id, scope, namespace, key, data, session)


async def _list_namespaces(
    agent_id: uuid.UUID,
    scope: str | None,
    session: AsyncSession,
    caller: StateCaller,
    *,
    hide_memory: bool = False,
) -> list[StateNamespaceOut]:
    """List the namespaces stored under one scope, each with its key count and
    the most recent write time (#250). This is the enumeration the operator's
    read/inspect surface needs on top of get-by-key + list-by-namespace; it stays
    within the store's non-goals (no query language, just a grouped summary).
    Namespaces are returned most-recently-written first.

    This route has no ``namespace`` path param, so ``forbid_reserved_namespace``
    cannot gate it; instead the reserved namespaces are filtered out for the
    narrow app (bundle) token (#856), the enumeration equivalent of that guard.
    Which SCOPE this lists is entirely a function of which URL was called
    (#1525 follow-up) -- the plain path always lists the shared scope, the
    ``/bindings/{kind}/{address}`` path always lists exactly that binding's,
    for every caller alike; an operator wanting the full picture of a
    memory=False agent calls once per binding, the same way its own bundle
    code only ever sees the one scope the worker handed it.

    ``hide_memory`` drops the ``memory`` row for a sandbox credential listing a
    binding whose memory it may not reach (ADR-0188), rather than refusing the
    whole listing: that binding's general state stays reachable.
    """
    query = (
        select(
            WorkflowStateEntry.namespace,
            func.count().label("key_count"),
            func.max(WorkflowStateEntry.updated_at).label("last_updated"),
        )
        .where(WorkflowStateEntry.agent_id == agent_id, WorkflowStateEntry.binding_scope == scope)
        .group_by(WorkflowStateEntry.namespace)
        .order_by(func.max(WorkflowStateEntry.updated_at).desc())
    )
    rows = await session.execute(query)
    listed = [
        StateNamespaceOut(
            namespace=row.namespace,
            key_count=row.key_count,
            last_updated=row.last_updated,
        )
        for row in rows
        # The narrow app (bundle) token must not even learn the reserved
        # namespaces exist -- their key counts and write times are exactly what
        # the state.app scope fences off (#856).
        if not (caller is StateCaller.APP and row.namespace in RESERVED_NAMESPACES)
        and not (hide_memory and row.namespace == MEMORY_NAMESPACE)
    ]
    # Transcripts live in their own table (ADR-0170) but are still listed here
    # as the reserved namespace the operator's inspector already knows. Only
    # the platform key gets that row: its count and write time cover every
    # channel's threads, which a sandbox credential may not learn (#3767).
    if caller is StateCaller.PLATFORM:
        threads = await transcripts.summary(session, agent_id, scope)
        if threads is not None:
            listed.append(
                StateNamespaceOut(
                    namespace=TRANSCRIPT_NAMESPACE,
                    key_count=threads[0],
                    last_updated=threads[1],
                )
            )
            listed.sort(key=lambda row: row.last_updated, reverse=True)
    return listed


@router.get("/{agent_id}/state", response_model=list[StateNamespaceOut])
async def list_namespaces(
    agent_id: uuid.UUID,
    session: SessionDep,
    principal: Annotated[StatePrincipal, Depends(require_state_access)],
) -> list[StateNamespaceOut]:
    return await _list_namespaces(agent_id, None, session, principal.caller)


@router.get("/{agent_id}/state/bindings/{kind}/{address}", response_model=list[StateNamespaceOut])
async def list_namespaces_for_binding(
    agent_id: uuid.UUID,
    kind: str,
    address: str,
    session: SessionDep,
    principal: Annotated[StatePrincipal, Depends(require_state_access)],
) -> list[StateNamespaceOut]:
    scope = await _binding_scope(session, agent_id, kind, address)
    hide_memory = principal.caller is StateCaller.STATE and (
        principal.legacy or principal.binding != scope
    )
    return await _list_namespaces(
        agent_id, scope, session, principal.caller, hide_memory=hide_memory
    )


async def _get_state(
    agent_id: uuid.UUID,
    scope: str | None,
    namespace: str,
    key: str,
    session: AsyncSession,
    response: Response,
) -> StateEntryOut:
    if namespace == TRANSCRIPT_NAMESPACE:
        headers = {
            "X-Curie-Transcript-Max-Bytes": str(get_settings().transcript_max_thread_bytes)
        }
        row = await transcripts.get(session, agent_id, scope, key)
        if row is None:
            raise HTTPException(
                status.HTTP_404_NOT_FOUND,
                "state entry not found",
                headers=headers,
            )
        response.headers.update(headers)
        return _transcript_out(row)
    entry = await _get_entry(session, agent_id, scope, namespace, key)
    if entry is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "state entry not found")
    return StateEntryOut.model_validate(entry)


@router.get(
    "/{agent_id}/state/{namespace}/{key}",
    response_model=StateEntryOut,
    dependencies=[Depends(forbid_reserved_namespace)],
)
async def get_state(
    agent_id: uuid.UUID,
    namespace: str,
    key: str,
    session: SessionDep,
    response: Response,
    principal: Annotated[StatePrincipal, Depends(require_state_access)],
) -> StateEntryOut:
    _check_memory_reach(
        principal,
        agent_id,
        namespace,
        requested_binding=None,
        key=key,
        write=False,
        path=_state_path(namespace, key),
    )
    _check_transcript_reach(
        principal,
        agent_id,
        namespace,
        requested_binding=None,
        key=key,
        path=_state_path(namespace, key),
    )
    return await _get_state(agent_id, None, namespace, key, session, response)


@router.get(
    "/{agent_id}/state/bindings/{kind}/{address}/{namespace}/{key}",
    response_model=StateEntryOut,
    dependencies=[Depends(forbid_reserved_namespace)],
)
async def get_state_for_binding(
    agent_id: uuid.UUID,
    kind: str,
    address: str,
    namespace: str,
    key: str,
    session: SessionDep,
    response: Response,
    principal: Annotated[StatePrincipal, Depends(require_state_access)],
) -> StateEntryOut:
    _check_memory_reach(
        principal,
        agent_id,
        namespace,
        requested_binding=f"{kind}:{address}",
        key=key,
        write=False,
        path=_state_path(namespace, key, kind, address),
    )
    _check_transcript_reach(
        principal,
        agent_id,
        namespace,
        requested_binding=f"{kind}:{address}",
        key=key,
        path=_state_path(namespace, key, kind, address),
    )
    scope = await _binding_scope(session, agent_id, kind, address)
    return await _get_state(agent_id, scope, namespace, key, session, response)


async def _list_state(
    agent_id: uuid.UUID, scope: str | None, namespace: str, session: AsyncSession
) -> list[StateEntryOut]:
    # Which scope this lists is a function of which URL was called, same as
    # _list_namespaces above -- uniform for every caller, no caller-type
    # branching here either.
    if namespace == TRANSCRIPT_NAMESPACE:
        rows = await transcripts.list_threads(session, agent_id, scope)
        return [_transcript_out(row) for row in rows]
    query = select(WorkflowStateEntry).where(
        WorkflowStateEntry.agent_id == agent_id,
        WorkflowStateEntry.binding_scope == scope,
        WorkflowStateEntry.namespace == namespace,
    )
    entries = await session.scalars(query.order_by(WorkflowStateEntry.key))
    return [StateEntryOut.model_validate(e) for e in entries]


@router.get(
    "/{agent_id}/state/{namespace}",
    response_model=list[StateEntryOut],
    dependencies=[Depends(forbid_reserved_namespace)],
)
async def list_state(
    agent_id: uuid.UUID,
    namespace: str,
    session: SessionDep,
    principal: Annotated[StatePrincipal, Depends(require_state_access)],
) -> list[StateEntryOut]:
    _check_memory_reach(
        principal,
        agent_id,
        namespace,
        requested_binding=None,
        key=None,
        write=False,
        path=_state_path(namespace, None),
    )
    _check_transcript_reach(
        principal,
        agent_id,
        namespace,
        requested_binding=None,
        key=None,
        path=_state_path(namespace, None),
    )
    rows = await _list_state(agent_id, None, namespace, session)
    return _visible_transcripts(principal, agent_id, namespace, rows)


@router.get(
    "/{agent_id}/state/bindings/{kind}/{address}/{namespace}",
    response_model=list[StateEntryOut],
    dependencies=[Depends(forbid_reserved_namespace)],
)
async def list_state_for_binding(
    agent_id: uuid.UUID,
    kind: str,
    address: str,
    namespace: str,
    session: SessionDep,
    principal: Annotated[StatePrincipal, Depends(require_state_access)],
) -> list[StateEntryOut]:
    _check_memory_reach(
        principal,
        agent_id,
        namespace,
        requested_binding=f"{kind}:{address}",
        key=None,
        write=False,
        path=_state_path(namespace, None, kind, address),
    )
    _check_transcript_reach(
        principal,
        agent_id,
        namespace,
        requested_binding=f"{kind}:{address}",
        key=None,
        path=_state_path(namespace, None, kind, address),
    )
    scope = await _binding_scope(session, agent_id, kind, address)
    rows = await _list_state(agent_id, scope, namespace, session)
    return _visible_transcripts(principal, agent_id, namespace, rows)


async def _delete_state(
    agent_id: uuid.UUID,
    scope: str | None,
    namespace: str,
    key: str,
    expected_version: int | None,
    session: AsyncSession,
    caller: StateCaller,
) -> Response:
    # expected_version opts into compare-and-delete (#2820), so an operator
    # that exported a transcript never deletes turns appended after the export.
    # The version is a predicate of the DELETE itself, so an append that
    # commits between the read and the delete cannot be removed with it.
    #
    # Each completed delete is recorded, including one that found nothing
    # (#3673). A 409 raises before the record: nothing was touched.
    def recorded(removed: bool) -> Response:
        state_mutation.record(
            op="delete",
            agent_id=agent_id,
            scope=scope,
            namespace=namespace,
            key=key,
            removed=removed,
            principal=caller.value,
        )
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    if namespace == TRANSCRIPT_NAMESPACE:
        return recorded(
            await transcripts.remove(session, agent_id, scope, key, expected_version)
        )
    entry = await _get_entry(session, agent_id, scope, namespace, key)
    if expected_version is None:
        if entry is not None:
            await session.delete(entry)
            await session.commit()
    else:
        stored = entry.version if entry is not None else None
        deleted = None
        if stored == expected_version and entry is not None:
            deleted = await session.scalar(
                delete(WorkflowStateEntry)
                .where(
                    WorkflowStateEntry.id == entry.id,
                    WorkflowStateEntry.version == expected_version,
                )
                .returning(WorkflowStateEntry.id)
            )
            await session.commit()
        if deleted is None:
            if stored is None:
                found = "entry does not exist"
            elif stored != expected_version:
                found = f"stored {stored}"
            else:
                found = "entry changed during the delete"
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                f"version mismatch: expected {expected_version}, {found}",
            )
    # Past the compare-and-delete, a versioned delete always removed its row.
    return recorded(entry is not None)


@router.delete(
    "/{agent_id}/state/{namespace}/{key}",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Depends(forbid_reserved_namespace)],
)
async def delete_state(
    agent_id: uuid.UUID,
    namespace: str,
    key: str,
    session: SessionDep,
    request: Request,
    principal: Annotated[StatePrincipal, Depends(require_state_access)],
    expected_version: int | None = None,
) -> Response:
    path = _state_path(namespace, key)
    _check_memory_reach(
        principal,
        agent_id,
        namespace,
        requested_binding=None,
        key=key,
        write=True,
        path=path,
    )
    _check_transcript_reach(
        principal,
        agent_id,
        namespace,
        requested_binding=None,
        key=key,
        path=_state_path(namespace, key),
    )
    await _check_turn_open(principal, agent_id, namespace, request, path)
    return await _delete_state(
        agent_id, None, namespace, key, expected_version, session, principal.caller
    )


@router.delete(
    "/{agent_id}/state/bindings/{kind}/{address}/{namespace}/{key}",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Depends(forbid_reserved_namespace)],
)
async def delete_state_for_binding(
    agent_id: uuid.UUID,
    kind: str,
    address: str,
    namespace: str,
    key: str,
    session: SessionDep,
    request: Request,
    principal: Annotated[StatePrincipal, Depends(require_state_access)],
    expected_version: int | None = None,
) -> Response:
    path = _state_path(namespace, key, kind, address)
    _check_memory_reach(
        principal,
        agent_id,
        namespace,
        requested_binding=f"{kind}:{address}",
        key=key,
        write=True,
        path=path,
    )
    _check_transcript_reach(
        principal,
        agent_id,
        namespace,
        requested_binding=f"{kind}:{address}",
        key=key,
        path=_state_path(namespace, key, kind, address),
    )
    await _check_turn_open(principal, agent_id, namespace, request, path)
    scope = await _binding_scope(session, agent_id, kind, address)
    return await _delete_state(
        agent_id, scope, namespace, key, expected_version, session, principal.caller
    )


# Worker-to-API (#3776): the worker reports a turn ended, so its per-turn memory
# credential is refused from then on. The same worker-token auth as the other
# ``/v1/internal`` routes; neither the platform key nor a sandbox token may call
# it. The turn rides the body, not the path: a turn claim is an event id plus a
# suffix and need not be a clean path segment.
internal_router = APIRouter(
    prefix="/v1/internal/memory",
    tags=["internal-memory"],
    dependencies=[Depends(require_internal_worker_token)],
)


@internal_router.post("/closed-turns", status_code=status.HTTP_204_NO_CONTENT)
async def close_memory_turn(data: MemoryTurnClosedIn, request: Request) -> Response:
    """Record that a turn has ended; idempotent. The record expires once no
    credential for the turn could still be valid (``MEMORY_TURN_CLOSED_TTL_S``)."""

    client: redis.Redis = request.app.state.valkey
    try:
        await client.set(
            _closed_turn_key(data.agent_id, data.turn), "1", ex=MEMORY_TURN_CLOSED_TTL_S
        )
    except (redis.RedisError, OSError) as exc:
        logger.warning("state: could not record memory turn %s as ended: %r", data.turn, exc)
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE, "could not record the turn as ended"
        ) from exc
    logger.info("state: memory turn %s ended for agent %s", data.turn, data.agent_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)

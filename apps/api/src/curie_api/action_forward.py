"""The forward execution seam: one execution per verified authority (ADR 0121).

@spec ACTION-EXECUTOR-19 @spec ACTION-EXECUTOR-1 @spec ACTION-EXECUTOR-2
@spec ACTION-EXECUTOR-7

"A server-side API function creates a ``forward`` execution from a verified
authority record" (ACTION-EXECUTOR-19); it is "an API function, not an HTTP
route" (ACTION-EXECUTOR-1), so no caller can name a tool or arguments for
execution. The authority sources build the record after verifying it: a policy
generation admitted by #4065 (``policy``) or an argument-bound approval from
#4069 (``approval``). Until they land nothing in the product calls this
function; the forward seam is built and integration tested without a producer.

What the function guarantees:

* only ``policy`` and ``approval`` authorize a forward execution; anything
  else, or a record that does not name a connector, digest and tool in their
  grammars, is ``authority_unavailable``;
* the record's arguments hash, in the proxy's canonical form, to the digest the
  authority bound, or the record is ``arguments_mismatch``;
* a replay of the same agent and idempotency key with the same record adopts
  the existing row in whatever state it is (``created`` false), so a dispatched
  call is never made twice; the same key naming anything else is
  ``arguments_mismatch`` and the first execution stands;
* ``observe_version``, and ``restore`` on a connector whose probe recorded the
  pair for this digest, are ``reserved_verb_via_forward``; a lone ``restore``
  is an ordinary tool (ACTION-EXECUTOR-8).

A refusal raises ``ForwardRefused`` and creates no row. No ledger row is
created here: the API creates it at the dispatch commit (``routers/
action_executions.py``), so a refusal before dispatch records no action.
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Final

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from .models import (
    ActionExecution,
    Agent,
    ConnectorCapability,
    ExecutionKind,
    ExecutionState,
)
from .remediation_nominations import canonical_text
from .schemas.action_executions import (
    CONNECTOR_MAX_LENGTH,
    CONNECTOR_PATTERN,
    DIGEST_PATTERN,
)

# @spec ACTION-EXECUTOR-1 @spec ACTION-EXECUTOR-19: the two authority kinds a
# forward execution may run under. ``undo_ruling`` and ``capability_probe``
# belong to the other two producers.
FORWARD_AUTHORITY_KINDS: Final = frozenset({"policy", "approval"})

# @spec ACTION-EXECUTOR-19: the paired verbs. ``observe_version`` is never a
# forward tool; ``restore`` is reserved only where the probe saw the pair.
OBSERVE_TOOL: Final = "observe_version"
RESTORE_TOOL: Final = "restore"

_CONNECTOR = re.compile(CONNECTOR_PATTERN)
_DIGEST = re.compile(DIGEST_PATTERN)
# An upstream MCP tool name: what follows ``mcp__<connector>__`` in the ledger.
_TOOL_MAX_LENGTH: Final = 128
_REF_MAX_LENGTH: Final = 512


@dataclass(frozen=True)
class ForwardAuthority:
    """The verified authority record an authority source hands this seam.

    ``tool`` is the upstream tool name and ``arguments`` the object the
    authority bound; ``arguments_sha256`` is the digest it bound over their
    canonical bytes. ``idempotency_key`` is the authority owner's own key.
    None of these ever come from an HTTP caller.
    """

    kind: str
    ref: str
    agent_id: uuid.UUID
    connector: str
    connector_digest: str
    tool: str
    arguments: Mapping[str, Any]
    arguments_sha256: str
    idempotency_key: str
    requested_by: str | None = None


@dataclass(frozen=True)
class ForwardCreated:
    """The created or adopted execution: identity, state, and whether it is new."""

    execution_id: uuid.UUID
    state: str
    created: bool


class ForwardRefused(Exception):
    """A refusal that created no row. ``code`` is an ACTION-EXECUTOR-20 code."""

    def __init__(self, code: str, reason: str) -> None:
        super().__init__(f"{code}: {reason}")
        self.code = code
        self.reason = reason


def canonical_arguments(arguments: Any) -> str:
    """The proxy's canonical text (ACTION-EXECUTOR-7), on the API side.

    The API's one executor canonicalizer, ``remediation_nominations.canonical_text``
    (sorted keys, ``,`` and ``:`` separators, ``ensure_ascii=False``, NaN and the
    infinities refused), held to the proxy's bytes by the shared vectors. Only a
    JSON object is a call's arguments.
    """

    if not isinstance(arguments, Mapping):
        raise TypeError("connector call arguments must be a JSON object")
    return canonical_text(dict(arguments))


def arguments_sha256(arguments: Any) -> str:
    """SHA-256 over the canonical UTF-8 bytes of ``arguments``."""

    return hashlib.sha256(canonical_arguments(arguments).encode("utf-8")).hexdigest()


def _well_formed(authority: ForwardAuthority) -> bool:
    """The record names a connector, digest, tool, ref and key in their grammars."""

    return (
        len(authority.connector) <= CONNECTOR_MAX_LENGTH
        and _CONNECTOR.fullmatch(authority.connector) is not None
        and _DIGEST.fullmatch(authority.connector_digest) is not None
        and 0 < len(authority.tool) <= _TOOL_MAX_LENGTH
        and authority.tool.strip() == authority.tool
        and 0 < len(authority.ref) <= _REF_MAX_LENGTH
        and bool(authority.idempotency_key)
    )


def _same_record(execution: ActionExecution, authority: ForwardAuthority, sha256: str) -> bool:
    """Whether an existing execution under this key is the same authorized call."""

    return (
        execution.kind == ExecutionKind.forward
        and execution.connector == authority.connector
        and execution.connector_digest == authority.connector_digest
        and execution.tool == authority.tool
        and execution.arguments_sha256 == sha256
        and execution.authority_kind == authority.kind
        and execution.authority_ref == authority.ref
    )


async def _existing(session: AsyncSession, authority: ForwardAuthority) -> ActionExecution | None:
    execution: ActionExecution | None = await session.scalar(
        select(ActionExecution)
        .where(
            ActionExecution.agent_id == authority.agent_id,
            ActionExecution.idempotency_key == authority.idempotency_key,
        )
        .execution_options(populate_existing=True)
    )
    return execution


def _adopt(execution: ActionExecution, authority: ForwardAuthority, sha256: str) -> ForwardCreated:
    if not _same_record(execution, authority, sha256):
        raise ForwardRefused(
            "arguments_mismatch",
            "an execution under this authority key already names another call",
        )
    return ForwardCreated(execution_id=execution.id, state=execution.state, created=False)


async def _restore_paired(session: AsyncSession, authority: ForwardAuthority) -> bool:
    """@spec ACTION-EXECUTOR-8: the probe recorded the pair for this digest."""

    capable = await session.scalar(
        select(ConnectorCapability.restore_capable).where(
            ConnectorCapability.agent_id == authority.agent_id,
            ConnectorCapability.connector == authority.connector,
            ConnectorCapability.digest == authority.connector_digest,
        )
    )
    return bool(capable)


async def create_forward_execution(
    session: AsyncSession, authority: ForwardAuthority
) -> ForwardCreated:
    """Create, or adopt on replay, the forward execution ``authority`` permits.

    @spec ACTION-EXECUTOR-19 @spec ACTION-EXECUTOR-2 @spec ACTION-EXECUTOR-7.
    Commits on success. Raises ``ForwardRefused`` (``authority_unavailable``,
    ``arguments_mismatch`` or ``reserved_verb_via_forward``) having written
    nothing. The row is ``requested`` with no subject action: the ledger row is
    created at dispatch.
    """

    if (
        authority.kind not in FORWARD_AUTHORITY_KINDS
        or not _well_formed(authority)
        or await session.get(Agent, authority.agent_id) is None
    ):
        await session.rollback()
        raise ForwardRefused(
            "authority_unavailable", "no verified forward authority names this call"
        )
    # Every refusal ends the caller's transaction, as the others do: the agent
    # read above opened it, and a refusal writes nothing.
    try:
        sha256 = arguments_sha256(authority.arguments)
    except (TypeError, ValueError):
        await session.rollback()
        raise ForwardRefused("arguments_mismatch", "the arguments have no canonical form") from None
    if sha256 != authority.arguments_sha256:
        await session.rollback()
        raise ForwardRefused(
            "arguments_mismatch", "the arguments differ from the ones the authority bound"
        )

    # A replay adopts before any other check, so the answer to the same record
    # never changes once its execution exists (a capability row landing later
    # cannot turn an adopted ``restore`` into a refusal).
    existing = await _existing(session, authority)
    if existing is not None:
        await session.commit()
        return _adopt(existing, authority, sha256)

    if authority.tool == OBSERVE_TOOL or (
        authority.tool == RESTORE_TOOL and await _restore_paired(session, authority)
    ):
        await session.rollback()
        raise ForwardRefused(
            "reserved_verb_via_forward", "the paired restore verbs are not forward tools"
        )

    while True:
        created = await session.scalar(
            insert(ActionExecution)
            .values(
                id=uuid.uuid4(),
                kind=ExecutionKind.forward.value,
                agent_id=authority.agent_id,
                connector=authority.connector,
                tool=authority.tool,
                subject_action_id=None,
                connector_digest=authority.connector_digest,
                arguments_sha256=sha256,
                forward_arguments=json.loads(canonical_arguments(authority.arguments)),
                authority_kind=authority.kind,
                authority_ref=authority.ref,
                requested_by=authority.requested_by,
                idempotency_key=authority.idempotency_key,
                state=ExecutionState.requested.value,
                attempt=0,
            )
            .on_conflict_do_nothing(constraint="uq_action_executions_agent_idempotency_key")
            .returning(ActionExecution.id)
        )
        await session.commit()
        if created is not None:
            return ForwardCreated(
                execution_id=created, state=ExecutionState.requested.value, created=True
            )
        # A concurrent creation took this key first: adopt it on the same terms.
        existing = await _existing(session, authority)
        if existing is not None:
            await session.commit()
            return _adopt(existing, authority, sha256)

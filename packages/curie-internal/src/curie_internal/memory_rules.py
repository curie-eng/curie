"""Learned memory rules: text bounds, lifecycle, scope keys and frozen selections.

A learned rule is a one-line, bounded text that a person accepts. It is later
delivered to a model, so the text must never be able to forge platform text:
no line breaks, no reserved headings, no section markers. This module is pure.
Apart from ``select_memory_rules`` and ``settle_lineage_candidates`` at the end,
which read and write through the caller's session or connection, it performs no
IO and touches no database.

``settle_lineage_candidates`` is the one settlement of a pull request lineage's
rule candidates (ADR 0287 decision 5, #5796). Every writer of a terminal
lineage state calls it inside the transaction of that write: the API's
``crud.lineages`` writers through ``curie_api.memory_rule_lifecycle`` and the
worker's ``PostgresPublicationStore.mark_lineage_terminal`` directly. The API's
lineage reconciler also sweeps terminal lineages that still hold candidates,
which a writer from before this feature left unsettled. A new terminal lineage
writer must call it too.

Lock order. Settlement takes no agent row lock: agent deletion locks the agent
row first and then, through its cascades, the lineage and the rule rows, so a
settlement that locked the agent after its caller's lineage write could
deadlock with it. Instead every caller holds the owning agent row ``FOR KEY
SHARE`` (or, on the publication advance, the WorkItem row, which deletion locks
second) before it writes the lineage, so deletion and settlement serialize on
that first row. Settlement then locks the lineage's ``candidate`` rows ``FOR
UPDATE`` in one fixed order, and a promotion takes the transaction advisory
lock of its scope (``rule_number_lock_key``) to allocate the number.

Arithmetic fact: the snapshot byte bound can bind before the count bound does.
Sixteen rules of 240 bytes with small rule numbers still fit (about 3.9 KiB),
but once the ``R<n>: `` prefixes carry large numbers (up to 19 digits) and the
line feeds are counted, the same sixteen rules exceed ``MAX_SNAPSHOT_BYTES``
(4096). ``SelectedMemoryRules`` therefore can refuse a full scope of long rules.
The accepting route is responsible for keeping a scope within both the count
bound and the snapshot byte bound.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping, Sequence
from types import MappingProxyType
from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)
from sqlalchemy import RowMapping
from sqlalchemy import text as sql_text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

MAX_RULE_BYTES = 240
MAX_ACCEPTED_PER_SCOPE = 16
MAX_SNAPSHOT_BYTES = 4096
MAX_RULE_NUMBER = 9_223_372_036_854_775_807
FORMAT_VERSION: Literal[1] = 1
RESERVED_PREFIXES = ("Learned rules:", "Requester preferences:")
SECTION_OPEN = "<<<curie-learned-rules"
SECTION_CLOSE = "<<<end-curie-learned-rules"
AGENT_SCOPE_KEY = ""

# Every character ``str.splitlines`` treats as a line boundary, plus NUL. LF and CR
# are the ones decision 8.2 names; the rest are refused too because other readers
# of the canonical text split on them, and an embedded boundary would let one rule
# forge a second numbered line.
_FORBIDDEN_CHARS = frozenset("\r\n\x00\x0b\x0c\x1c\x1d\x1e\x85\u2028\u2029")
_RESERVED_FRAGMENTS = (*RESERVED_PREFIXES, SECTION_OPEN, SECTION_CLOSE)


class InvalidRule(ValueError):
    """A redacted refusal; the supplied value is never retained or echoed."""

    def __init__(self, code: Literal["invalid_text", "too_large", "reserved_text"]) -> None:
        self.code = code
        super().__init__(code)


class InvalidTransition(ValueError):
    """A lifecycle event that is not allowed from the given state."""

    def __init__(self) -> None:
        super().__init__("invalid_transition")


class ScopeOverfull(ValueError):
    """A scope holds more accepted rules than one selection may carry."""

    def __init__(self) -> None:
        super().__init__("scope_overfull")


def _validate_text(value: object) -> str:
    try:
        if type(value) is bytes:
            value = value.decode("utf-8", errors="strict")
        if type(value) is not str:
            raise InvalidRule("invalid_text")
        text = value.strip(" ")
        if not text or any(char in text for char in _FORBIDDEN_CHARS):
            raise InvalidRule("invalid_text")
        encoded = text.encode("utf-8", errors="strict")
    except UnicodeError:
        raise InvalidRule("invalid_text") from None
    if len(encoded) > MAX_RULE_BYTES:
        raise InvalidRule("too_large")
    if any(fragment in text for fragment in _RESERVED_FRAGMENTS):
        raise InvalidRule("reserved_text")
    return text


def validate_rule_text(value: str | bytes) -> str:
    """Return the stripped one-line rule text, or refuse with a redacted code."""

    return _validate_text(value)


def validate_reason(value: str | bytes) -> str:
    """Validate a decision reason under the same bounds as rule text."""

    return _validate_text(value)


RuleState = Literal[
    "candidate", "proposed", "accepted", "rejected", "removed", "superseded", "discarded"
]
RuleEvent = Literal["propose", "promote", "accept", "reject", "remove", "supersede", "discard"]

TRANSITIONS: Mapping[tuple[RuleState, RuleEvent], RuleState] = MappingProxyType(
    {
        ("candidate", "promote"): "proposed",
        ("candidate", "supersede"): "superseded",
        ("candidate", "discard"): "discarded",
        ("proposed", "accept"): "accepted",
        ("proposed", "reject"): "rejected",
        ("accepted", "remove"): "removed",
    }
)
INITIAL_STATES: tuple[RuleState, RuleState] = ("candidate", "proposed")
FINAL_STATES: frozenset[RuleState] = frozenset({"rejected", "removed", "superseded", "discarded"})


def next_state(from_state: RuleState, event: RuleEvent) -> RuleState:
    """Apply an event to a state. ``propose`` has no from-state, so it never applies."""

    try:
        return TRANSITIONS[(from_state, event)]
    except (KeyError, TypeError):
        raise InvalidTransition from None


def initial_state(*, hidden: bool) -> RuleState:
    """State a rule takes on the ``propose`` event."""

    return "candidate" if hidden else "proposed"


def _scope_part(value: object, *, allow_colon: bool) -> str:
    if type(value) is not str or not value:
        raise InvalidRule("invalid_text")
    if any(char in value for char in _FORBIDDEN_CHARS) or (not allow_colon and ":" in value):
        raise InvalidRule("invalid_text")
    return value


def repository_scope_key(kind: str, host: str, project_id: str) -> str:
    """Key a repository scope as ``kind:host:project_id``.

    ``kind`` and ``project_id`` may not contain ``:`` while ``host`` may (a
    port), so the key parses back unambiguously: ``kind`` ends at the first
    colon and ``project_id`` starts after the last one.
    """

    return ":".join(
        (
            _scope_part(kind, allow_colon=False),
            _scope_part(host, allow_colon=True),
            _scope_part(project_id, allow_colon=False),
        )
    )


def binding_scope_key(kind: str, address: str) -> str:
    """Key a binding scope as ``kind:address``.

    ``kind`` may not contain ``:``, so it ends at the first colon and the
    address (which may contain colons) is everything after it.
    """

    return f"{_scope_part(kind, allow_colon=False)}:{_scope_part(address, allow_colon=True)}"


def binding_claim_scope_key(binding: object) -> str:
    """Key the binding scope named by a state credential's ``binding`` claim.

    The worker writes the claim as ``kind:address`` with the raw kind and
    address (ADR 0188), so it is split at the first colon and keyed through
    ``binding_scope_key``; a claim that is not such a pair is refused.
    """

    if type(binding) is not str:
        raise InvalidRule("invalid_text")
    kind, colon, address = binding.partition(":")
    if not colon:
        raise InvalidRule("invalid_text")
    return binding_scope_key(kind, address)


def _rule_number(value: object) -> int:
    if type(value) is not int or not 1 <= value <= MAX_RULE_NUMBER:
        raise InvalidRule("invalid_text")
    return value


def canonical_text(rules: Sequence[tuple[int, str]]) -> str:
    """Render ``R<number>: <text>`` lines joined by LF with no trailing newline."""

    if len(rules) == 0:
        raise InvalidRule("invalid_text")
    lines: list[str] = []
    seen: set[int] = set()
    for number, text in rules:
        if _rule_number(number) in seen:
            raise InvalidRule("invalid_text")
        seen.add(number)
        if validate_rule_text(text) != text:
            raise InvalidRule("invalid_text")
        lines.append(f"R{number}: {text}")
    return "\n".join(lines)


def rules_sha256(canonical: str) -> str:
    """Lowercase hex SHA-256 of the exact UTF-8 bytes of a canonical document."""

    if type(canonical) is not str:
        raise InvalidRule("invalid_text")
    try:
        encoded = canonical.encode("utf-8", errors="strict")
    except UnicodeError:
        raise InvalidRule("invalid_text") from None
    return hashlib.sha256(encoded).hexdigest()


Digest = Annotated[str, Field(strict=True, min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")]


class _Contract(BaseModel):
    model_config = ConfigDict(
        strict=True,
        frozen=True,
        extra="forbid",
        hide_input_in_errors=True,
        revalidate_instances="always",
    )


class SelectedMemoryRules(_Contract):
    kind: Literal["selected"] = "selected"
    rule_numbers: tuple[int, ...]
    text: str = Field(repr=False)
    sha256: Digest
    count: int
    format_version: Literal[1] = FORMAT_VERSION

    @field_validator("format_version", mode="before")
    @classmethod
    def _format_version_is_an_int(cls, value: object) -> object:
        # Strict ``Literal[1]`` still accepts ``True`` and ``1.0``, in Python and
        # in JSON, because both equal 1. Refuse anything that is not an int.
        if type(value) is not int:
            raise ValueError("invalid_selected_rules")
        return value

    @model_validator(mode="after")
    def _valid_selection(self) -> Self:
        numbers = self.rule_numbers
        if self.count != len(numbers) or not 1 <= self.count <= MAX_ACCEPTED_PER_SCOPE:
            raise ValueError("invalid_selected_rules")
        if len(set(numbers)) != len(numbers):
            raise ValueError("invalid_selected_rules")
        if any(not 1 <= number <= MAX_RULE_NUMBER for number in numbers):
            raise ValueError("invalid_selected_rules")
        lines = self.text.split("\n")
        if len(lines) != len(numbers):
            raise ValueError("invalid_selected_rules")
        try:
            for number, line in zip(numbers, lines, strict=True):
                prefix = f"R{number}: "
                if not line.startswith(prefix):
                    raise ValueError("invalid_selected_rules")
                body = line[len(prefix) :]
                if validate_rule_text(body) != body:
                    raise ValueError("invalid_selected_rules")
            if len(self.text.encode("utf-8", errors="strict")) > MAX_SNAPSHOT_BYTES:
                raise ValueError("invalid_selected_rules")
            if rules_sha256(self.text) != self.sha256:
                raise ValueError("invalid_selected_rules")
        except InvalidRule:
            raise ValueError("invalid_selected_rules") from None
        return self


class AbsentMemoryRules(_Contract):
    kind: Literal["absent"] = "absent"
    reason: Literal["no_rules"]


MemoryRulesSelection = Annotated[
    SelectedMemoryRules | AbsentMemoryRules, Field(discriminator="kind")
]


def selected_from_rules(rules: Sequence[tuple[int, str]]) -> SelectedMemoryRules:
    """Build a selection from ordered ``(number, text)`` pairs."""

    if len(rules) > MAX_ACCEPTED_PER_SCOPE:
        raise ScopeOverfull
    text = canonical_text(rules)
    return SelectedMemoryRules(
        rule_numbers=tuple(number for number, _ in rules),
        text=text,
        sha256=rules_sha256(text),
        count=len(rules),
    )


ENVELOPE_VERSION: Literal[1] = 1
MAX_EXECUTION_FENCE = 9_223_372_036_854_775_807


def _version(value: object) -> int:
    # Strict ``Literal[1]`` accepts ``True`` and ``1.0``; refuse anything but an int.
    if type(value) is not int or value != 1:
        raise ValueError("invalid_version")
    return value


def _uuid(value: object) -> str:
    if type(value) is not str:
        raise ValueError("invalid_identifier")
    try:
        if str(UUID(value)) != value:
            raise ValueError("invalid_identifier")
    except (ValueError, AttributeError):
        raise ValueError("invalid_identifier") from None
    return value


EnvelopeVersion = Annotated[Literal[1], BeforeValidator(_version)]
Identifier = Annotated[str, BeforeValidator(_uuid)]
Fence = Annotated[int, Field(strict=True, ge=1, le=MAX_EXECUTION_FENCE)]


class MemoryRulesSelectedV1(_Contract):
    """The pinned rules as delivered: ``count`` is derived, so the wire omits it.

    The model revalidates the numbers, every rule line, the snapshot byte bound and
    the digest against the text, so a tampered or truncated envelope is refused.
    """

    kind: Literal["selected"] = "selected"
    rule_numbers: list[int] = Field(max_length=MAX_ACCEPTED_PER_SCOPE)
    sha256: Digest
    format_version: EnvelopeVersion = FORMAT_VERSION
    text: str = Field(repr=False)

    @model_validator(mode="after")
    def _valid_selection(self) -> Self:
        if any(type(number) is not int for number in self.rule_numbers):
            raise ValueError("invalid_selected_rules")
        SelectedMemoryRules(
            rule_numbers=tuple(self.rule_numbers),
            text=self.text,
            sha256=self.sha256,
            count=len(self.rule_numbers),
        )
        return self


MemoryRulesEnvelopeSelection = Annotated[
    MemoryRulesSelectedV1 | AbsentMemoryRules, Field(discriminator="kind")
]


class MemoryRulesContextV1(_Contract):
    envelope_version: EnvelopeVersion = ENVELOPE_VERSION
    tenant_id: Identifier
    work_item_id: Identifier
    execution_request_id: Identifier
    execution_fence: Fence
    selection: MemoryRulesEnvelopeSelection


class MemoryRulesAcknowledgmentV1(_Contract):
    envelope_version: EnvelopeVersion = ENVELOPE_VERSION
    work_item_id: Identifier
    execution_request_id: Identifier
    execution_fence: Fence
    outcome: Literal["none", "applied", "failed"]
    sha256: Digest | None = Field(default=None, exclude_if=lambda value: value is None)

    @model_validator(mode="after")
    def _digest_when_applied(self) -> Self:
        if (self.outcome == "applied") != (self.sha256 is not None):
            raise ValueError("invalid_acknowledgment_digest")
        return self


SCOPE_KINDS = ("agent", "binding", "repository")


async def select_memory_rules(
    session: AsyncSession,
    *,
    agent_id: UUID,
    scope_kind: str,
    scope_key: str,
) -> SelectedMemoryRules | AbsentMemoryRules:
    """Read the accepted rules of one exact scope, oldest acceptance first.

    Runs inside the caller's transaction and writes nothing. A scope holding
    more than ``MAX_ACCEPTED_PER_SCOPE`` accepted rules raises ``ScopeOverfull``;
    the accept route keeps a scope within that bound.
    """

    if scope_kind not in SCOPE_KINDS or type(scope_key) is not str:
        raise InvalidRule("invalid_text")
    rows = (
        await session.execute(
            sql_text(
                "SELECT number, text FROM curie.memory_rules "
                "WHERE agent_id = :agent AND scope_kind = :kind AND scope_key = :key "
                "AND state = 'accepted' ORDER BY accepted_at, id LIMIT :limit"
            ),
            {
                "agent": agent_id,
                "kind": scope_kind,
                "key": scope_key,
                "limit": MAX_ACCEPTED_PER_SCOPE + 1,
            },
        )
    ).all()
    if not rows:
        return AbsentMemoryRules(reason="no_rules")
    return selected_from_rules([(row[0], row[1]) for row in rows])


_SAFE_SCHEMA = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,62}")
_PLATFORM_ACTOR = "platform"
_NUMBER_LOCK_DOMAIN = b"curie.memory_rules.number"


def rule_number_lock_key(agent_id: UUID, scope_kind: str, scope_key: str) -> int:
    """The ``pg_advisory_xact_lock`` key that serializes number allocation in one scope.

    The key is the first 8 bytes, read as a big-endian signed integer, of the
    SHA-256 of ``curie.memory_rules.number``, the agent id's canonical text,
    the scope kind and the scope key, each followed by a NUL byte (NUL cannot
    occur in any of them). A collision between two scopes only makes them wait
    for each other; it never shares a number, which the scope's unique index
    still guards.
    """

    digest = hashlib.sha256()
    parts = (_NUMBER_LOCK_DOMAIN, str(agent_id).encode(), scope_kind.encode(), scope_key.encode())
    for part in parts:
        digest.update(part)
        digest.update(b"\x00")
    return int.from_bytes(digest.digest()[:8], "big", signed=True)


async def _settle_one(
    connection: AsyncSession | AsyncConnection,
    schema: str,
    row: RowMapping,
    event: RuleEvent,
    *,
    number: int | None = None,
) -> bool:
    to_state = next_state("candidate", event)
    changed = (
        await connection.execute(
            sql_text(
                f'UPDATE "{schema}".memory_rules SET state = :to_state, number = :number, '
                "version = version + 1, updated_at = clock_timestamp() "
                "WHERE id = :id AND state = 'candidate' RETURNING id"
            ),
            {"to_state": to_state, "number": number, "id": row["id"]},
        )
    ).first()
    if changed is None:
        return False
    await connection.execute(
        sql_text(
            f'INSERT INTO "{schema}".memory_rule_events '
            "(rule_id, tenant_id, agent_id, event, from_state, to_state, actor_kind, actor) "
            "VALUES (:rule_id, :tenant_id, :agent_id, :event, 'candidate', :to_state, "
            "'platform', :actor)"
        ),
        {
            "rule_id": row["id"],
            "tenant_id": row["tenant_id"],
            "agent_id": row["agent_id"],
            "event": event,
            "to_state": to_state,
            "actor": _PLATFORM_ACTOR,
        },
    )
    return True


async def settle_lineage_candidates(
    connection: AsyncSession | AsyncConnection,
    *,
    lineage_id: UUID,
    terminal_state: str,
    schema: str = "curie",
) -> int:
    """Promote, supersede or discard a lineage's rule candidates; the count changed.

    Runs inside the caller's transaction, after its terminal lineage write and
    before its commit; it never commits or rolls back. The caller already holds
    the owning agent row ``FOR KEY SHARE`` or the WorkItem row (module
    docstring). The lineage's ``candidate`` rows are locked ``FOR UPDATE`` in
    one order, so a concurrent second settlement waits for the first and then
    finds none. ``merged`` promotes the newest candidate (by ``proposed_at``,
    then ``id``) to ``proposed`` with the next free number in its scope,
    allocated under the scope's transaction advisory lock, and supersedes every
    older one; ``closed`` discards every candidate. Only ``candidate`` rows
    change, so a second pass changes nothing. Each change bumps ``version`` and
    ``updated_at`` and writes one ``memory_rule_events`` row with actor kind
    ``platform``. Returns how many candidates changed.
    """

    if terminal_state not in ("merged", "closed"):
        raise ValueError("terminal publication lineage state is invalid")
    if not _SAFE_SCHEMA.fullmatch(schema):
        raise ValueError("memory rule schema is invalid")
    candidates = (
        (
            await connection.execute(
                sql_text(
                    "SELECT id, tenant_id, agent_id, scope_kind, scope_key "
                    f'FROM "{schema}".memory_rules '
                    "WHERE source_lineage_id = :lineage AND state = 'candidate' "
                    "ORDER BY proposed_at DESC, id DESC FOR UPDATE"
                ),
                {"lineage": lineage_id},
            )
        )
        .mappings()
        .all()
    )
    if not candidates:
        return 0
    if terminal_state == "closed":
        return sum([await _settle_one(connection, schema, row, "discard") for row in candidates])
    newest, older = candidates[0], candidates[1:]
    # Numbers are allocated per agent scope under the scope's advisory lock, so
    # two promotions in one scope never take the same number.
    await connection.execute(
        sql_text("SELECT pg_advisory_xact_lock(:key)"),
        {
            "key": rule_number_lock_key(
                newest["agent_id"], newest["scope_kind"], newest["scope_key"]
            )
        },
    )
    highest = (
        await connection.execute(
            sql_text(
                f'SELECT max(number) FROM "{schema}".memory_rules '
                "WHERE agent_id = :agent AND scope_kind = :kind AND scope_key = :key"
            ),
            {"agent": newest["agent_id"], "kind": newest["scope_kind"], "key": newest["scope_key"]},
        )
    ).scalar()
    changed = [await _settle_one(connection, schema, row, "supersede") for row in older]
    current = highest if isinstance(highest, int) else 0
    changed.append(await _settle_one(connection, schema, newest, "promote", number=current + 1))
    return sum(changed)

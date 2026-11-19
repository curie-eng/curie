"""Learned memory rules: text bounds, lifecycle, scope keys and frozen selections.

A learned rule is a one-line, bounded text that a person accepts. It is later
delivered to a model, so the text must never be able to forge platform text:
no line breaks, no reserved headings, no section markers. This module is pure.
It performs no IO and touches no database.

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
from collections.abc import Mapping, Sequence
from types import MappingProxyType
from typing import Annotated, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

MAX_RULE_BYTES = 240
MAX_ACCEPTED_PER_SCOPE = 16
MAX_SNAPSHOT_BYTES = 4096
MAX_RULE_NUMBER = 9_223_372_036_854_775_807
FORMAT_VERSION: Literal[1] = 1
RESERVED_PREFIXES = ("Learned rules:", "Requester preferences:")
SECTION_OPEN = "<<<curie-learned-rules"
SECTION_CLOSE = "<<<end-curie-learned-rules"
AGENT_SCOPE_KEY = ""

_FORBIDDEN_CHARS = ("\r", "\n", "\x00")
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


def _rule_number(value: object) -> int:
    if type(value) is not int or not 1 <= value <= MAX_RULE_NUMBER:
        raise InvalidRule("invalid_text")
    return value


def canonical_text(rules: Sequence[tuple[int, str]]) -> str:
    """Render ``R<number>: <text>`` lines joined by LF with no trailing newline."""

    if len(rules) == 0:
        raise InvalidRule("invalid_text")
    lines: list[str] = []
    for number, text in rules:
        _rule_number(number)
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

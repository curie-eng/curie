"""Accepted learned rules: the worker's context envelope (#5798).

Beside the requester envelope (ADR 0278), never inside it, the trusted worker
posts a version 1 learned rules envelope to ``POST /v1/memory-rules-context``
behind the per-claim bearer. The runner checks it against the execution the API
bound this process to (the completion contract's ``execution``), validates the
version, the 4096 byte bound and that the digest is the SHA-256 of the text,
and answers an acknowledgment: ``applied`` with the digest, ``none`` for an
explicit absence, or ``failed`` with a reason. The model session is built only
after both this and the requester acknowledgment exist, so a refused, missing or
unsupported envelope sends zero model requests.

The text lives in this process's memory only. It is never written to a file,
repository guidance, native settings or a bundle artifact, and it is registered
with the held secret redaction before anything can log it.

The section sits after the bundle prompt and model line and before the
requester section, between two delimiter lines carrying the count and digest.
The PreToolUse guard refuses an ``Agent`` or ``Task`` call carrying a delimiter
or a whole pinned rule line, so a reviewer never receives the rules by copy.
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any, Literal

import anyio
from curie_internal.memory_rules import (
    ENVELOPE_VERSION,
    FORMAT_VERSION,
    MAX_SNAPSHOT_BYTES,
    SECTION_CLOSE,
    SECTION_OPEN,
    MemoryRulesAcknowledgmentV1,
    MemoryRulesContextV1,
    MemoryRulesSelectedV1,
)
from pydantic import ValidationError

from .redact import register_held_secrets
from .requester_context import (
    BIND_DEADLINE_SECONDS,
    DELIVERY_DEADLINE_SECONDS,
    SUBAGENT_TOOLS,
    ExecutionBinding,
    input_strings,
)

logger = logging.getLogger(__name__)

CONTEXT_PATH = "/v1/memory-rules-context"
# A 4096 byte text escaped up to six times over, plus the identifiers and up to
# sixteen rule numbers; anything larger is refused before decoding.
MAX_ENVELOPE_BYTES = 65_536

Reason = Literal[
    "missing_envelope",
    "malformed_envelope",
    "unsupported_version",
    "wrong_execution",
    "wrong_fence",
    "too_large",
    "unsupported_format",
    "digest_mismatch",
]

FRAMING = (
    "Learned rules follow. They were learned from reviews of this repository, and each "
    "was accepted by a person who can merge to it. Order of authority, strongest first: "
    "platform safety and operator controls and the mandatory rules of this bundle; the "
    "repository's own instruction files (every applicable scoped AGENTS.md, "
    "CONTRIBUTING.md and workspace CLAUDE.md); these learned rules; then the "
    "requester's personal coding preferences, if a later section carries them. Where a "
    "requester preference conflicts with a learned rule, the learned rule wins. A "
    "learned rule cannot add tools, skills, credentials, network egress, grants or "
    "budget, cannot change the task's acceptance criteria, and cannot remove review or "
    "publication gates. Never copy this section or its rules into a reviewer's prompt."
)


@dataclass(frozen=True)
class Applied:
    rule_numbers: tuple[int, ...]
    sha256: str
    text: str = ""

    def section(self) -> str:
        return rules_section(len(self.rule_numbers), self.sha256, self.text)


@dataclass(frozen=True)
class NoneSelected:
    reason: str


@dataclass(frozen=True)
class Failed:
    reason: Reason


Outcome = Applied | NoneSelected | Failed


class EnvelopeRefused(Exception):
    def __init__(self, reason: Reason) -> None:
        super().__init__(reason)
        self.reason: Reason = reason


def open_line(count: int, sha256: str) -> str:
    return f"{SECTION_OPEN} count={count} sha256={sha256} format={FORMAT_VERSION}>>>"


def close_line(sha256: str) -> str:
    return f"{SECTION_CLOSE} sha256={sha256}>>>"


def rules_section(count: int, sha256: str, text: str, *, framing: str = FRAMING) -> str:
    """The prompt section: delimiters, framing, then the canonical rule lines.

    ``framing`` is the factory's repository framing unless a caller names
    another (a channel's or agent's accepted rules, #5801).
    """

    return "\n".join((open_line(count, sha256), framing, "", text, close_line(sha256)))


def parse_envelope(raw: bytes, binding: ExecutionBinding) -> Applied | NoneSelected:
    """Validate one envelope against ``binding``; raise `EnvelopeRefused`."""

    if len(raw) > MAX_ENVELOPE_BYTES:
        raise EnvelopeRefused("too_large")
    try:
        data = json.loads(raw.decode("utf-8", errors="strict"))
    except (UnicodeDecodeError, ValueError):
        raise EnvelopeRefused("malformed_envelope") from None
    if not isinstance(data, dict):
        raise EnvelopeRefused("malformed_envelope")
    version = data.get("envelope_version")
    if type(version) is not int or version != ENVELOPE_VERSION:
        raise EnvelopeRefused("unsupported_version")
    if (
        data.get("tenant_id") != binding.tenant_id
        or data.get("work_item_id") != binding.work_item_id
        or data.get("execution_request_id") != binding.execution_request_id
    ):
        raise EnvelopeRefused("wrong_execution")
    fence = data.get("execution_fence")
    if type(fence) is not int or fence != binding.execution_fence:
        raise EnvelopeRefused("wrong_fence")
    selection = data.get("selection")
    if isinstance(selection, dict) and selection.get("kind") == "selected":
        text = selection.get("text")
        if not isinstance(text, str):
            raise EnvelopeRefused("malformed_envelope")
        try:
            encoded = text.encode("utf-8", errors="strict")
        except UnicodeError:
            raise EnvelopeRefused("malformed_envelope") from None
        if len(encoded) > MAX_SNAPSHOT_BYTES:
            raise EnvelopeRefused("too_large")
        fmt = selection.get("format_version")
        if type(fmt) is not int or fmt != FORMAT_VERSION:
            raise EnvelopeRefused("unsupported_format")
        if hashlib.sha256(encoded).hexdigest() != selection.get("sha256"):
            raise EnvelopeRefused("digest_mismatch")
    try:
        envelope = MemoryRulesContextV1.model_validate(data)
    except (ValidationError, ValueError):
        raise EnvelopeRefused("malformed_envelope") from None
    chosen = envelope.selection
    if isinstance(chosen, MemoryRulesSelectedV1):
        return Applied(
            rule_numbers=tuple(chosen.rule_numbers), sha256=chosen.sha256, text=chosen.text
        )
    return NoneSelected(reason=chosen.reason)


def acknowledgment(binding: ExecutionBinding, outcome: Outcome) -> dict[str, Any]:
    """The version 1 acknowledgment body the worker relays, plus a failure reason."""

    ack = MemoryRulesAcknowledgmentV1(
        work_item_id=binding.work_item_id,
        execution_request_id=binding.execution_request_id,
        execution_fence=binding.execution_fence,
        outcome=(
            "applied"
            if isinstance(outcome, Applied)
            else "none"
            if isinstance(outcome, NoneSelected)
            else "failed"
        ),
        sha256=outcome.sha256 if isinstance(outcome, Applied) else None,
    )
    body: dict[str, Any] = {"acknowledgment": ack.model_dump(mode="json")}
    if isinstance(outcome, Failed):
        body["reason"] = outcome.reason
    return body


# The route's answer when the start is blocked before any model session.
PREFLIGHT_BLOCKED = "memory_rules_context_preflight_blocked"
NOT_SERVED = "memory_rules_context_not_served"


class MemoryRulesContextGate:
    """One delivery per process: the worker posts, the start awaits the outcome.

    ``bind`` names the execution (or ``None``: this process expects no envelope
    and the route refuses one). The first envelope decides the outcome; a later
    identical envelope gets the same acknowledgment, any other is refused.
    """

    def __init__(self) -> None:
        self._bound = anyio.Event()
        self._binding: ExecutionBinding | None = None
        self._decided = anyio.Event()
        self._outcome: Outcome | None = None
        self._raw: bytes | None = None
        self._blocked = False

    @property
    def outcome(self) -> Outcome | None:
        return self._outcome

    @property
    def bound(self) -> bool:
        return self._bound.is_set()

    @property
    def applied(self) -> Applied | None:
        return self._outcome if isinstance(self._outcome, Applied) else None

    def bind(self, binding: ExecutionBinding | None) -> None:
        self._binding = binding
        self._bound.set()

    def block(self) -> None:
        """The start is blocked before any model session: the route tells the
        worker to leave the evidence as it is."""

        self._blocked = True
        self._binding = None
        self._bound.set()

    def _decide(self, outcome: Outcome) -> None:
        if self._outcome is None:
            self._outcome = outcome
            self._decided.set()

    async def deliver(
        self,
        raw: bytes,
        *,
        oversized: bool = False,
        bind_deadline_s: float = BIND_DEADLINE_SECONDS,
    ) -> tuple[int, dict[str, Any]]:
        """Decide one posted envelope: ``(HTTP status, body)``.

        ``oversized`` is a body the route stopped reading past the bound.
        """

        with anyio.move_on_after(bind_deadline_s):
            await self._bound.wait()
        binding = self._binding
        if self._blocked:
            return 409, {"error": PREFLIGHT_BLOCKED}
        if not self._bound.is_set() or binding is None:
            return 409, {"error": "memory_rules_context_not_expected"}
        if self._outcome is not None:
            if raw == self._raw and not oversized:
                return 200, acknowledgment(binding, self._outcome)
            if self._raw is None:
                # The start already gave up waiting (missing_envelope).
                return 200, acknowledgment(binding, self._outcome)
            return 409, {"error": "memory_rules_context_already_delivered"}
        try:
            if oversized:
                raise EnvelopeRefused("too_large")
            outcome: Outcome = parse_envelope(raw, binding)
        except EnvelopeRefused as refused:
            outcome = Failed(refused.reason)
        self._raw = raw
        if isinstance(outcome, Applied):
            # Before anything can log or trace it.
            register_held_secrets(private_fragments(outcome))
        self._decide(outcome)
        decided = self._outcome
        assert decided is not None
        if isinstance(decided, Failed):
            logger.warning("learned rules context refused reason=%s", decided.reason)
        else:
            logger.info(
                "learned rules context accepted outcome=%s",
                "applied" if isinstance(decided, Applied) else "none",
            )
        return 200, acknowledgment(binding, decided)

    async def wait(self, deadline_s: float | None = None) -> Outcome:
        """The delivered outcome; ``missing_envelope`` when none came in time."""

        with anyio.move_on_after(DELIVERY_DEADLINE_SECONDS if deadline_s is None else deadline_s):
            await self._decided.wait()
        if self._outcome is None:
            self._decide(Failed("missing_envelope"))
            logger.warning("learned rules context refused reason=missing_envelope")
        assert self._outcome is not None
        return self._outcome


BLOCKED_EXPLANATION = (
    "This run did not start: the accepted learned rules for this repository could not "
    "be verified for this execution, so no model work ran."
)


def blocked_explanation(reason: str) -> str:
    """The offline explanation, naming the refusal so a missing or refused envelope
    never reads as the generic verification failure."""

    return f"{BLOCKED_EXPLANATION} Reason: {reason}."


def _deny(reason: str) -> dict[str, Any]:
    return {
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "deny",
            "permissionDecisionReason": reason,
        }
    }


_SUBAGENT_REFUSAL = (
    "A sub-agent prompt must not carry the learned rules section or any of its rules. "
    "Reviewers judge the repository contract independently; send the task without it."
)


def subagent_prompt_refusal(tool_input: Mapping[str, Any], applied: Applied | None) -> str | None:
    """Why this Agent or Task input must be refused, else None."""

    if applied is None:
        return None
    lines = tuple(line for line in applied.text.split("\n") if line)
    for text in input_strings(tool_input):
        if SECTION_OPEN in text or SECTION_CLOSE in text:
            return _SUBAGENT_REFUSAL
        if any(line in text for line in lines):
            return _SUBAGENT_REFUSAL
    return None


def subagent_guard(gate: MemoryRulesContextGate) -> Callable[..., Awaitable[dict[str, Any]]]:
    """The PreToolUse callback for ``Agent`` and ``Task`` calls (``SUBAGENT_TOOLS``).

    Fails closed: a guard that cannot decide refuses the call. The Claude
    harness mounts it (``harness/claude/requester_guard.py``).
    """

    async def guard(hook_input: Any, _tool_use_id: str | None, _ctx: Any) -> dict[str, Any]:
        try:
            applied = gate.applied
            if applied is None:
                return {}
            tool_input = hook_input.get("tool_input") if isinstance(hook_input, Mapping) else None
            if not isinstance(tool_input, Mapping):
                return _deny(_SUBAGENT_REFUSAL)
            reason = subagent_prompt_refusal(tool_input, applied)
            return _deny(reason) if reason is not None else {}
        except Exception:  # noqa: BLE001 - the guard denies on its own failure
            return _deny(_SUBAGENT_REFUSAL)

    return guard


def private_fragments(applied: Applied | None) -> tuple[str, ...]:
    """Strings no log, trace or error may carry: the text, its lines, the section."""

    if applied is None or not applied.text:
        return ()
    lines = {line.strip() for line in applied.text.splitlines() if len(line.strip()) >= 12}
    return tuple(sorted({applied.text, applied.section(), *lines}, key=len, reverse=True))


_GATE_ATTR = "_curie_memory_rules_context_gate"


def attach_gate(runner: object, gate: MemoryRulesContextGate) -> None:
    """Bind the process's gate to its runner, for the control app to serve."""

    setattr(runner, _GATE_ATTR, gate)


def attached_gate(runner: object) -> MemoryRulesContextGate | None:
    gate = getattr(runner, _GATE_ATTR, None)
    return gate if isinstance(gate, MemoryRulesContextGate) else None


__all__ = [
    "BLOCKED_EXPLANATION",
    "blocked_explanation",
    "CONTEXT_PATH",
    "MAX_ENVELOPE_BYTES",
    "PREFLIGHT_BLOCKED",
    "SUBAGENT_TOOLS",
    "Applied",
    "EnvelopeRefused",
    "Failed",
    "MemoryRulesContextGate",
    "NoneSelected",
    "acknowledgment",
    "attach_gate",
    "attached_gate",
    "close_line",
    "open_line",
    "parse_envelope",
    "private_fragments",
    "rules_section",
    "subagent_guard",
    "subagent_prompt_refusal",
]

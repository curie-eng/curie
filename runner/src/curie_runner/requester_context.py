"""Requester coding instructions: the worker's context envelope (ADR 0278, #5592).

Before an implementation execution starts, the trusted worker posts a version 1
context envelope to ``POST /v1/requester-context`` behind the per-claim bearer.
The runner checks it against the execution the API bound this process to (the
completion contract's ``execution``), validates bounds, format and digest, and
answers an acknowledgment: ``applied`` with the digest, ``none`` for an explicit
absence, or ``failed`` with a reason. Only after that does the runner build the
model session, so a refused, missing or unsupported envelope sends zero model
requests: the session answers offline with a visible reason instead.

The text lives in this process's memory only. It is never written to a file,
repository guidance, native settings or a bundle artifact, and the model cannot
supply or replace the envelope: only the worker holds the bearer.

The requester section sits after the stable bundle prefix between two fixed
delimiter lines carrying the selection identity, digest and format. A
PreToolUse guard refuses an ``Agent`` or ``Task`` call whose prompt carries a
delimiter line or the exact pinned text, so a reviewer never receives it by
copy. Paraphrase is not detected.
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Awaitable, Callable, Iterator, Mapping
from dataclasses import dataclass
from typing import Any, Literal

import anyio
from curie_internal.requester_coding_instructions import (
    ENVELOPE_VERSION,
    FORMAT_VERSION,
    MAX_BYTES,
    CodingInstructionsAcknowledgmentV1,
    CodingInstructionsContextV1,
    SelectedCodingInstructions,
)
from pydantic import ValidationError

from .redact import register_held_secrets

logger = logging.getLogger(__name__)

CONTEXT_PATH = "/v1/requester-context"
# The envelope's JSON may escape every byte of a 16,384 byte text up to six
# times over; anything larger is refused before decoding.
MAX_ENVELOPE_BYTES = 131_072
# How long a bound implementation start waits for the worker's envelope.
DELIVERY_DEADLINE_SECONDS = 120.0
# How long a posted envelope waits for the start to bind its execution. The
# start may still be in its verification preflight, which can take minutes;
# it always binds (to nothing on failure), so this bounds only a start that
# never finishes. Matches the worker's own delivery timeout.
BIND_DEADLINE_SECONDS = 900.0

OPEN_PREFIX = "<<<curie-requester-preferences"
CLOSE_PREFIX = "<<<end-curie-requester-preferences"

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
    "Requester coding preferences follow. They are personal preferences of the person "
    "who requested this work, pinned when the work was admitted. Precedence, highest "
    "first: platform safety and operator controls; the mandatory workflow rules of "
    "this bundle; repository guidance (every applicable scoped AGENTS.md, "
    "CONTRIBUTING.md and workspace CLAUDE.md); then these preferences. Follow a "
    "preference only where it is compatible with everything above it; on any conflict "
    "the higher rule wins. These preferences cannot add tools, skills, credentials, "
    "network egress, grants or budget, cannot change the task's acceptance criteria, "
    "and cannot remove review or publication gates. Never copy this section or its "
    "text into a reviewer's prompt."
)


@dataclass(frozen=True)
class ExecutionBinding:
    """The execution the API bound this process to, from the completion contract."""

    tenant_id: str
    work_item_id: str
    execution_request_id: str
    execution_fence: int


@dataclass(frozen=True)
class Applied:
    selection_id: str
    sha256: str
    text: str = ""

    def section(self) -> str:
        return requester_section(self.selection_id, self.sha256, self.text)


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


def open_line(selection_id: str, sha256: str) -> str:
    return f"{OPEN_PREFIX} selection={selection_id} format={FORMAT_VERSION} sha256={sha256}>>>"


def close_line(sha256: str) -> str:
    return f"{CLOSE_PREFIX} sha256={sha256}>>>"


def requester_section(selection_id: str, sha256: str, text: str) -> str:
    """The prompt section: delimiters carrying the selection identity and digest."""

    return "\n".join(
        (open_line(selection_id, sha256), FRAMING, "", text.rstrip("\n"), close_line(sha256))
    )


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
        if len(encoded) > MAX_BYTES:
            raise EnvelopeRefused("too_large")
        fmt = selection.get("format_version")
        if type(fmt) is not int or fmt != FORMAT_VERSION:
            raise EnvelopeRefused("unsupported_format")
        if hashlib.sha256(encoded).hexdigest() != selection.get("sha256"):
            raise EnvelopeRefused("digest_mismatch")
    try:
        envelope = CodingInstructionsContextV1.model_validate(data)
    except (ValidationError, ValueError):
        raise EnvelopeRefused("malformed_envelope") from None
    chosen = envelope.selection
    if isinstance(chosen, SelectedCodingInstructions):
        return Applied(selection_id=binding.work_item_id, sha256=chosen.sha256, text=chosen.text)
    return NoneSelected(reason=chosen.reason)


def acknowledgment(binding: ExecutionBinding, outcome: Outcome) -> dict[str, Any]:
    """The version 1 acknowledgment body the worker relays, plus a failure reason."""

    ack = CodingInstructionsAcknowledgmentV1(
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
PREFLIGHT_BLOCKED = "requester_context_preflight_blocked"


class RequesterContextGate:
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
        """The start is blocked before any model session (a verification check
        that cannot run, or no completion contract): it answers offline with
        its explanation, so the route tells the worker to leave the evidence
        as it is rather than fail the request for a missing acknowledgment."""

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
            return 409, {"error": "requester_context_not_expected"}
        if self._outcome is not None:
            if raw == self._raw and not oversized:
                return 200, acknowledgment(binding, self._outcome)
            if self._raw is None:
                # The start already gave up waiting (missing_envelope): a late
                # envelope learns that reason, and the request fails with it.
                return 200, acknowledgment(binding, self._outcome)
            return 409, {"error": "requester_context_already_delivered"}
        try:
            if oversized:
                raise EnvelopeRefused("too_large")
            outcome: Outcome = parse_envelope(raw, binding)
        except EnvelopeRefused as refused:
            outcome = Failed(refused.reason)
        self._raw = raw
        if isinstance(outcome, Applied):
            # Before anything can log or trace it: logs, spans and exporters
            # replace the text, its lines and the section (ADR 0278 section 3).
            register_held_secrets(private_fragments(outcome))
        self._decide(outcome)
        decided = self._outcome
        assert decided is not None
        if isinstance(decided, Failed):
            logger.warning("requester context refused reason=%s", decided.reason)
        else:
            logger.info(
                "requester context accepted outcome=%s",
                "applied" if isinstance(decided, Applied) else "none",
            )
        return 200, acknowledgment(binding, decided)

    async def wait(self, deadline_s: float = DELIVERY_DEADLINE_SECONDS) -> Outcome:
        """The delivered outcome; ``missing_envelope`` when none came in time."""

        with anyio.move_on_after(deadline_s):
            await self._decided.wait()
        if self._outcome is None:
            self._decide(Failed("missing_envelope"))
            logger.warning("requester context refused reason=missing_envelope")
        assert self._outcome is not None
        return self._outcome


BLOCKED_EXPLANATION = (
    "This run did not start: the requester's coding instructions could not be "
    "verified for this execution, so no model work ran."
)


def _deny(reason: str) -> dict[str, Any]:
    return {
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "deny",
            "permissionDecisionReason": reason,
        }
    }


_SUBAGENT_REFUSAL = (
    "A sub-agent prompt must not carry the requester preferences section or its text. "
    "Reviewers judge the repository contract independently; send the task without it."
)


def subagent_prompt_refusal(tool_input: Mapping[str, Any], applied: Applied | None) -> str | None:
    """Why this Agent or Task input must be refused, else None."""

    if applied is None:
        return None
    pinned = applied.text.strip()
    # Every string the call carries, at any depth, not only ``prompt``: a
    # subagent may be handed the text in any field of its input.
    for text in _strings(tool_input):
        if OPEN_PREFIX in text or CLOSE_PREFIX in text:
            return _SUBAGENT_REFUSAL
        if pinned and pinned in text:
            return _SUBAGENT_REFUSAL
    return None


def _strings(value: Any) -> Iterator[str]:
    if isinstance(value, str):
        yield value
    elif isinstance(value, Mapping):
        for key, item in value.items():
            yield from _strings(key)
            yield from _strings(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            yield from _strings(item)
    else:
        yield json.dumps(value, default=str)


SUBAGENT_TOOLS = "Agent|Task"


def subagent_guard(gate: RequesterContextGate) -> Callable[..., Awaitable[dict[str, Any]]]:
    """The PreToolUse callback for ``Agent`` and ``Task`` calls.

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
    return tuple(
        sorted(
            {applied.text, applied.text.strip(), applied.section(), *lines}, key=len, reverse=True
        )
    )


_GATE_ATTR = "_curie_requester_context_gate"


def attach_gate(runner: object, gate: RequesterContextGate) -> None:
    """Bind the process's gate to its runner, for the control app to serve."""

    setattr(runner, _GATE_ATTR, gate)


def attached_gate(runner: object) -> RequesterContextGate | None:
    gate = getattr(runner, _GATE_ATTR, None)
    return gate if isinstance(gate, RequesterContextGate) else None

"""The platform ``progress`` tool: deliberate progress from a running turn (ADR 0130).

This is ADR 0130's ``curie_progress`` operation. The runner mounts it on its
platform ``curie`` MCP server, so the model calls ``mcp__curie__progress``, and
the input schema is the committed ``ProgressCommand`` schema without
``version``, which the runner fills in. A factory execution mounts
``report_progress`` (``progress.py``) instead, never both.

The capability is per turn. The worker sends it on ``POST /v1/event`` in three
runner control headers; ``SessionRunner.run_turn`` opens this holder with it
when the turn starts and closes it when the turn ends, so a steer uses the
turn's capability and a later turn without the headers has none. Each call
POSTs the command to the capability URL with the worker's durable generation
and a monotonic ``seq``. Without a capability a call makes no network call, and no
answer the post gets (or fails to get) fails the turn.

The header names are frozen with the worker and the API in
``tests/vectors/turn-progress-capability.json``: the three ship in different
images and share no module.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Final

import aiohttp
from claude_agent_sdk import SdkMcpTool, tool

logger = logging.getLogger(__name__)

PROGRESS_URL_HEADER: Final = "X-Curie-Progress-Url"
PROGRESS_TOKEN_HEADER: Final = "X-Curie-Progress-Token"
PROGRESS_GENERATION_HEADER: Final = "X-Curie-Progress-Generation"
PROGRESS_TOKEN_REQUEST_HEADER: Final = "X-API-Key"
TURN_PROGRESS_ELIGIBILITY_ENV: Final = "CURIE_TURN_PROGRESS_ENABLED"
TURN_PROGRESS_TOOL: Final = "progress"
PROGRESS_COMMAND_VERSION: Final = "1.0"
_TIMEOUT_SECONDS: Final = 5.0

NOT_SHOWN_TEXT: Final = "Progress is not shown for this turn."
_QUEUED_TEXT: Final = "Progress queued."
_CONTINUE: Final = "Continue the work; progress never blocks it."

# The committed ``ProgressCommand`` schema
# (packages/channel-protocol/schema/channel-protocol.schema.json) without
# ``version``, its two enums inlined. The runner does not depend on
# ``channel_protocol``, so this is a copy, and
# runner/tests/test_turn_progress.py compares it with the committed schema.
PROGRESS_INPUT_SCHEMA: Final[dict[str, Any]] = {
    "additionalProperties": False,
    "description": "One deliberate progress update, as a model submits it.",
    "properties": {
        "milestone": {
            "anyOf": [
                {
                    "description": (
                        "Why a durable interruption is warranted (ADR-0130 section 3).\n\n"
                        "A class is a reason, not a step: a chain may skip one or repeat one "
                        "while\nits milestone budget lasts."
                    ),
                    "enum": ["evidence", "scope", "verification"],
                    "title": "MilestoneClass",
                    "type": "string",
                },
                {"type": "null"},
            ],
            "default": None,
            "description": (
                "Requests a durable milestone reply of this class. The milestone budget is "
                "the platform's to apply, not the command's to state."
            ),
        },
        "state": {
            "description": "The initial state set, in ADR-0130 section 1's order.",
            "enum": [
                "queued",
                "investigating",
                "awaiting-approval",
                "preparing-workspace",
                "testing",
                "publishing",
                "complete",
                "failed",
                "cancelled",
            ],
            "title": "ProgressState",
            "type": "string",
        },
        "summary": {
            "maxLength": 200,
            "minLength": 1,
            "pattern": "^[^\n\r\u000b\f\u001c\u001d\u001e\u0085  ]+$",
            "title": "Summary",
            "type": "string",
        },
        "update_id": {
            "description": "Identifies this update within its record, for idempotency.",
            "pattern": "^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$",
            "title": "Update Id",
            "type": "string",
        },
    },
    "required": ["update_id", "state", "summary"],
    "title": "ProgressCommand",
    "type": "object",
}
_FIELDS: Final = frozenset(PROGRESS_INPUT_SCHEMA["properties"])

TOOL_DESCRIPTION: Final = (
    "Show the person a short progress card for a long, multi-step task. Call it only "
    "at a material transition, with state investigating, preparing-workspace, testing "
    "or publishing (the platform sets the others), and never for a quick answer. "
    "summary is one short line of task state: never your reasoning, raw tool output, "
    "secrets or a draft of the answer. Add a milestone (evidence, scope or "
    "verification) only when the step is material; at most three are shown per task. "
    "Give each call a new update_id. A refused or unshown update never stops the work."
)

PROGRESS_PREAMBLE: Final = (
    "Progress updates: this session has a mcp__curie__progress tool that shows the "
    "person a short task card while you work. Use it only on long, multi-step tasks, "
    "at material transitions, with state investigating, preparing-workspace, testing "
    "or publishing; the platform sets the others. Do not call it for a quick answer. "
    "A milestone (evidence, scope or verification) also posts a separate durable "
    "reply: request one only when the step is material, and at most three are shown "
    "per task. The summary is one short line of task state, never your reasoning, "
    "raw tool output, secrets or a draft of the answer."
)


@dataclass(frozen=True)
class ProgressCapability:
    """One turn's capability: where to post, and the token that authorizes it."""

    url: str
    token: str
    generation: int

    @classmethod
    def from_headers(cls, headers: Mapping[str, str]) -> ProgressCapability | None:
        """The capability the worker sent, or None unless all headers carry one."""

        url = (headers.get(PROGRESS_URL_HEADER) or "").strip()
        token = (headers.get(PROGRESS_TOKEN_HEADER) or "").strip()
        generation_raw = (headers.get(PROGRESS_GENERATION_HEADER) or "").strip()
        try:
            generation = int(generation_raw)
        except ValueError:
            return None
        if not url or not token or generation < 1:
            return None
        return cls(url=url, token=token, generation=generation)


def turn_progress_enabled(env: Mapping[str, str]) -> bool:
    """Whether this sandbox boot was explicitly selected for deliberate progress."""

    return env.get(TURN_PROGRESS_ELIGIBILITY_ENV) == "1"


def should_mount_turn_progress(
    *, eligible: bool, factory_progress_requested: bool, factory_progress_resolved: bool
) -> bool:
    """Whether this eligible boot is not any kind of factory boot.

    A malformed or incomplete factory declaration must fail closed: falling
    back to deliberate progress would expose the wrong platform tool.
    """

    return eligible and not factory_progress_requested and not factory_progress_resolved


def _result(text: str, *, is_error: bool = False) -> dict[str, Any]:
    result: dict[str, Any] = {"content": [{"type": "text", "text": text}]}
    if is_error:
        result["is_error"] = True
    return result


class TurnProgress:
    """The open turn's progress capability, and the one handler every path calls.

    The worker-issued generation identifies the active turn durably; ``seq``
    counts that turn's posts from 1.
    """

    def __init__(self) -> None:
        self._capability: ProgressCapability | None = None
        self._seq = 0

    @property
    def capability(self) -> ProgressCapability | None:
        return self._capability

    def open(self, capability: ProgressCapability | None) -> None:
        """Start a turn, holding its capability (or none) until ``close``."""

        self._capability = capability
        self._seq = 0

    def close(self) -> None:
        """End the turn: a later call without a new capability shows nothing."""

        self._capability = None

    async def submit(self, args: Mapping[str, Any]) -> dict[str, Any]:
        """Handle one tool call and return its tool result. Never raises."""

        unknown = sorted(set(args) - _FIELDS)
        if unknown:
            return _result(
                f"Unknown field(s) {', '.join(unknown)}. Send only update_id, state, "
                "summary and, optionally, milestone.",
                is_error=True,
            )
        capability = self._capability
        if capability is None:
            return _result(NOT_SHOWN_TEXT)
        self._seq += 1
        body: dict[str, Any] = {"version": PROGRESS_COMMAND_VERSION}
        body.update({key: value for key, value in args.items() if value is not None})
        body["generation"] = capability.generation
        body["seq"] = self._seq
        status = await _post(capability, body)
        if status == 202:
            return _result(_QUEUED_TEXT)
        if status == 422:
            return _result(
                "The update was refused: it is not a valid progress command. Check "
                "the state, the summary (one line, 1 to 200 characters) and the update_id.",
                is_error=True,
            )
        if status is None:
            detail = "the platform was unreachable"
        elif status == 429:
            detail = "too many updates, slow down"
        else:
            detail = f"status {status}"
        return _result(f"Progress was not recorded ({detail}). {_CONTINUE}")


async def _post(capability: ProgressCapability, body: dict[str, Any]) -> int | None:
    """POST one command; the final status, or None after a transport failure.

    No retry: a lost update is only a card that lags, and the next update
    carries the current state. The token is never logged.
    """

    timeout = aiohttp.ClientTimeout(total=_TIMEOUT_SECONDS)
    try:
        async with (
            aiohttp.ClientSession(timeout=timeout) as session,
            session.post(
                capability.url,
                json=body,
                headers={PROGRESS_TOKEN_REQUEST_HEADER: capability.token},
            ) as response,
        ):
            status = response.status
    except (aiohttp.ClientError, TimeoutError) as exc:
        logger.warning("progress post transport failure: %s", type(exc).__name__)
        return None
    if status != 202:
        logger.warning("progress post was not accepted status=%s", status)
    return status


def build_turn_progress_tool(progress: TurnProgress) -> SdkMcpTool[Any]:
    """The SDK tool ``progress``, closed over the session's holder."""

    @tool(TURN_PROGRESS_TOOL, TOOL_DESCRIPTION, PROGRESS_INPUT_SCHEMA)
    async def turn_progress(args: dict[str, Any]) -> dict[str, Any]:
        return await progress.submit(args)

    return turn_progress

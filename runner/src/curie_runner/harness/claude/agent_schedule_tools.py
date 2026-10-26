"""The agent-schedule tools on their own in-process SDK MCP server (ADR 0248).

``list_schedules`` uses the boot read token. ``create_schedule``,
``update_schedule``, ``cancel_schedule`` and ``revert_schedule`` use the turn's
own credential. Create, update and revert are refused here on a turn with no
person behind it or run by a cron job (a scheduled run); list and cancel stay
allowed. The API enforces the
same from the credential's claims. No argument names an author: the API stamps
it from the credential.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import Any

from claude_agent_sdk import SdkMcpTool, create_sdk_mcp_server, tool
from claude_agent_sdk.types import McpSdkServerConfig

from ...agent_schedules import AgentSchedule, AgentSchedulesClient, AgentSchedulesError
from ...approval import _approval_error
from ...memory_facts import NO_PERSON, MemoryTurn
from ...tool_names import AGENT_SCHEDULES_SERVER_NAME

logger = logging.getLogger(__name__)

JOB_REFUSAL = "Scheduled runs cannot create or change schedules. Nothing changed."
NO_CREDENTIAL_REFUSAL = "Schedules cannot be changed from this turn. Nothing changed."

_ID = {"type": "string", "description": "The schedule's id."}
_REVISION = {"type": "integer", "description": "The schedule's current revision, as read."}


def _schema(properties: dict[str, Any], required: list[str]) -> dict[str, Any]:
    return {"type": "object", "properties": properties, "required": required}


_WHEN = {
    "cron": {"type": "string", "description": "Five-field cron for a recurring schedule."},
    "run_at": {
        "type": "string",
        "description": "ISO time for a one-shot, with an offset or in the schedule's zone.",
    },
    "timezone": {"type": "string", "description": "IANA zone; omit for the agent default."},
}
_CREATE_SCHEMA = _schema(
    {
        "prompt": {"type": "string", "description": "What to do when it runs, self-contained."},
        **_WHEN,
    },
    ["prompt"],
)
_UPDATE_SCHEMA = _schema(
    {
        "id": _ID,
        "expected_revision": _REVISION,
        "prompt": {"type": "string", "description": "A new prompt."},
        **_WHEN,
        "paused": {"type": "boolean", "description": "Pause or resume the schedule."},
    },
    ["id", "expected_revision"],
)
_CANCEL_SCHEMA = _schema({"id": _ID, "expected_revision": _REVISION}, ["id"])
_REVERT_SCHEMA = _schema(
    {
        "id": _ID,
        "expected_revision": _REVISION,
        "to_revision": {
            "type": "integer",
            "description": "The earlier revision to restore; omit for the previous one.",
        },
    },
    ["id", "expected_revision"],
)


def _ok(text: str) -> dict[str, Any]:
    return {"content": [{"type": "text", "text": text}]}


def _cadence(s: AgentSchedule) -> str:
    return f"cron {s.cron}" if s.cron else f"once at {s.run_at}"


def _next(s: AgentSchedule) -> str:
    return s.next_run_local or "no upcoming run"


def _render(s: AgentSchedule) -> str:
    state = "paused" if s.paused and s.state == "active" else s.state
    return (
        f"{s.id} (revision {s.revision}, {state}): {_cadence(s)}, next run {_next(s)}: {s.prompt}"
    )


def _zone_phrase(s: AgentSchedule) -> str:
    return f"the {s.timezone} zone" if s.timezone else "the schedule's zone"


def _readback(verb: str, s: AgentSchedule) -> str:
    return (
        f"{verb} schedule {s.id}, now revision {s.revision} ({_cadence(s)}). "
        f"Next run: {_next(s)}. Read this back to the channel in {_zone_phrase(s)}: "
        f"what it will do and when the next run is, exactly as written above. "
        f'Tell them replying "cancel" removes it.'
    )


def _failure(exc: AgentSchedulesError) -> dict[str, Any]:
    # The detail is the API's own message (a limit, a conflict, not found);
    # the exception never carries a token.
    if exc.status == 409:
        return _approval_error(
            f"Refused: {exc}. Read the schedules again and retry against the current revision. "
            "Nothing changed."
        )
    if exc.status is not None and 400 <= exc.status < 500:
        return _approval_error(f"Refused: {exc}. Nothing changed.")
    logger.warning(
        "agent schedule tool failed error_class=%s status=%s", type(exc).__name__, exc.status
    )
    return _approval_error("The schedules store could not be reached. Nothing changed.")


def _int(args: dict[str, Any], key: str) -> int:
    value = args.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise AgentSchedulesError(f"{key} must be a revision number", status=422)
    return value


def _ref(args: dict[str, Any]) -> str:
    return str(args.get("id") or "")


def build_agent_schedule_tools(
    *, client: AgentSchedulesClient, turn: MemoryTurn
) -> list[SdkMcpTool[Any]]:
    """The five tools. Every failure is an ``is_error`` result the model reads."""

    def scheduled_turn() -> bool:
        return turn.author == NO_PERSON or turn.author.startswith("cron:")

    def refuse(tool_name: str) -> dict[str, Any]:
        # Logged so the refusal is observable outside the model's reply.
        logger.info(
            "agent schedule write refused tool=%s author=%s reason=scheduled_run",
            tool_name,
            turn.author,
        )
        return _approval_error(JOB_REFUSAL)

    async def write(
        tool_name: str, subject: str, call: Callable[[], Awaitable[dict[str, Any]]]
    ) -> dict[str, Any]:
        if not turn.write_token:
            return _approval_error(NO_CREDENTIAL_REFUSAL)
        try:
            return await call()
        except AgentSchedulesError as exc:
            if exc.status is None or exc.status >= 500:
                logger.warning(
                    "agent schedule write unconfirmed error_class=%s status=%s",
                    type(exc).__name__,
                    exc.status,
                )
                return _approval_error(
                    f"Could not confirm whether the change to {subject} was saved. "
                    "Read the schedules with mcp__agent_schedules__list_schedules before retrying."
                )
            # The detail is the API's own message, never a token (see _failure).
            logger.info(
                "agent schedule write refused by the API tool=%s status=%s detail=%s",
                tool_name,
                exc.status,
                exc,
            )
            return _failure(exc)

    @tool("create_schedule", "Create a recurring or one-shot schedule.", _CREATE_SCHEMA)
    async def create_schedule(args: dict[str, Any]) -> dict[str, Any]:
        if scheduled_turn():
            return refuse("create_schedule")

        async def call() -> dict[str, Any]:
            s = await client.create(
                str(args.get("prompt")),
                cron=args.get("cron"),
                run_at=args.get("run_at"),
                timezone=args.get("timezone"),
            )
            text = _readback("Created", s)
            if s.cron:
                text += (
                    " Offer to run it once now as a test run; do not run it unless they say yes."
                )
            return _ok(text)

        return await write("create_schedule", "the new schedule", call)

    @tool("list_schedules", "List this agent's schedules with their revisions.", _schema({}, []))
    async def list_schedules(_args: dict[str, Any]) -> dict[str, Any]:
        try:
            schedules = await client.list()
        except AgentSchedulesError as exc:
            return _failure(exc)
        return _ok("\n".join(_render(s) for s in schedules) or "No schedules yet.")

    @tool(
        "update_schedule",
        "Change a schedule's prompt, timing or paused state, against the revision you read.",
        _UPDATE_SCHEMA,
    )
    async def update_schedule(args: dict[str, Any]) -> dict[str, Any]:
        if scheduled_turn():
            return refuse("update_schedule")

        async def call() -> dict[str, Any]:
            s = await client.update(
                _ref(args),
                _int(args, "expected_revision"),
                prompt=args.get("prompt"),
                cron=args.get("cron"),
                run_at=args.get("run_at"),
                timezone=args.get("timezone"),
                paused=args.get("paused"),
            )
            return _ok(_readback("Updated", s))

        return await write("update_schedule", _ref(args), call)

    @tool("cancel_schedule", "Cancel a schedule so it never runs again.", _CANCEL_SCHEMA)
    async def cancel_schedule(args: dict[str, Any]) -> dict[str, Any]:
        async def call() -> dict[str, Any]:
            rev = args.get("expected_revision")
            s = await client.cancel(_ref(args), _int(args, "expected_revision") if rev else None)
            return _ok(f"Cancelled schedule {s.id}. It will not run again.")

        return await write("cancel_schedule", _ref(args), call)

    @tool(
        "revert_schedule",
        "Restore an earlier configuration as a new revision, against the revision you read.",
        _REVERT_SCHEMA,
    )
    async def revert_schedule(args: dict[str, Any]) -> dict[str, Any]:
        if scheduled_turn():
            return refuse("revert_schedule")

        async def call() -> dict[str, Any]:
            to = args.get("to_revision")
            s = await client.revert(
                _ref(args),
                _int(args, "expected_revision"),
                _int(args, "to_revision") if to is not None else None,
            )
            return _ok(_readback("Reverted", s))

        return await write("revert_schedule", _ref(args), call)

    return [create_schedule, list_schedules, update_schedule, cancel_schedule, revert_schedule]


def build_agent_schedules_server(
    *, client: AgentSchedulesClient, turn: MemoryTurn
) -> McpSdkServerConfig:
    return create_sdk_mcp_server(
        name=AGENT_SCHEDULES_SERVER_NAME,
        version="1.0.0",
        tools=build_agent_schedule_tools(client=client, turn=turn),
    )

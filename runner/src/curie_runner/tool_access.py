"""Per-turn tool access: refuse every tool a read-only turn may not execute.

The contract is TOOL-ACCESS in ``docs/interfaces/aci-producer/INTERFACE.md``;
this runner's half is RUNNER-TOOL-ACCESS-1..8 in ``runner/README.md``.

``TurnToolAccess`` is one object shared by everything that decides a call: the
PreToolUse front, the permission-callback front, the fake model session and the
``SessionRunner``, which sets the access of the prompt it is about to send. The
decision is a pure function of the tool name and that access, so the fronts can
answer before any other callback runs and without touching approval state.

The fronts wrap callbacks rather than sit beside them as sibling matchers. The
CLI dispatches every matcher on one event concurrently, so a sibling approval
hook would still record a pending approval, or spend a one-shot grant, and a
sibling bundle command would still run, for a call the front refuses. Wrapping
means a refused call reaches none of the runner's registrations. (The CLI's own
native copy of the bundle's hooks is outside them; see RUNNER-TOOL-ACCESS-2.)
The added ``matcher=None`` front is
what decides a call no other matcher selects, and it is the only refusal layer a
session without an approval gate has, because such a session runs under
``bypassPermissions`` and never consults a permission callback.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from typing import Any

from aci_protocol import ToolAccess
from claude_agent_sdk import HookMatcher
from claude_agent_sdk.types import (
    CanUseTool,
    PermissionResultAllow,
    PermissionResultDeny,
    ToolPermissionContext,
)

#: The ``ToolAccess`` values this runner enforces, advertised on ``/status``.
ENFORCED_TOOL_ACCESS: tuple[ToolAccess, ...] = (ToolAccess.READ_ONLY,)

#: The ``ErrorEvent`` classification for a restricted turn this session cannot
#: enforce: no enforcement, or an unrestricted prompt already sent
#: (RUNNER-TOOL-ACCESS-4, RUNNER-TOOL-ACCESS-5).
TOOL_ACCESS_UNENFORCED_CLASSIFICATION = "tool-access-unenforced"
#: The ``ErrorEvent`` classification for a restricted turn whose own request
#: is not allowed, a slash command (RUNNER-TOOL-ACCESS-9).
TOOL_ACCESS_REFUSED_CLASSIFICATION = "tool-access-refused"

_UNNAMED_TOOL = "this tool call"
_DECISION_FAILED = (
    "This tool call was not run: its tool access could not be decided, so it is "
    "refused. Do not retry it."
)


class TurnToolAccess:
    """The tool access in force and the read-only set it is checked against."""

    def __init__(
        self,
        readonly_tools: Iterable[str],
        *,
        requires_approval: Callable[[str], bool] | None = None,
    ) -> None:
        # @spec RUNNER-TOOL-ACCESS-1: built once at boot from the same set the
        # side-effect classifier treats as read-only.
        self._readonly = frozenset(readonly_tools)
        self._requires_approval = requires_approval
        self.active: ToolAccess | None = None
        # Call ids refused since the last prompt, for the tool result counter.
        self.refused_call_ids: set[str] = set()

    def begin(self, access: ToolAccess | None) -> None:
        """Put ``access`` in force for the prompt about to be sent.

        @spec RUNNER-TOOL-ACCESS-4: called only immediately before a prompt, so
        it stays in force for anything that prompt's turn leaves running.
        """

        self.active = access
        self.refused_call_ids.clear()

    def refusal(self, tool_name: str | None) -> str | None:
        """Why ``tool_name`` may not run now, or ``None`` when it may.

        @spec RUNNER-TOOL-ACCESS-1 RUNNER-TOOL-ACCESS-2 RUNNER-TOOL-ACCESS-3
        RUNNER-TOOL-ACCESS-7
        """

        if self.active is None:
            return None
        if (
            tool_name
            and tool_name in self._readonly
            and not (self._requires_approval is not None and self._requires_approval(tool_name))
        ):
            return None
        return (
            f"{tool_name or _UNNAMED_TOOL} was not run: this turn is read-only. Only "
            "tools classified read-only may run on it, and it cannot request an "
            "approval. Do not retry the call; answer with what you can read."
        )

    def refuse(self, tool_name: str | None, tool_use_id: str | None) -> str | None:
        """``refusal``, remembering a refused call's id for the result counter."""

        reason = self.refusal(tool_name)
        if reason is not None and tool_use_id:
            # @spec RUNNER-TOOL-ACCESS-6
            self.refused_call_ids.add(tool_use_id)
        return reason


def _tool_name(hook_input: Any) -> str | None:
    value = (
        hook_input.get("tool_name")
        if isinstance(hook_input, Mapping)
        else getattr(hook_input, "tool_name", None)
    )
    return value if isinstance(value, str) else None


def _safe_refusal(
    access: TurnToolAccess,
    tool_name: str | None,
    tool_use_id: str | None,
    *,
    record: bool,
) -> str | None:
    """The decision, failing closed: a raising decision is a refusal.

    @spec RUNNER-TOOL-ACCESS-2: the CLI reports a raising PreToolUse callback as
    a hook error and then RUNS the call, so an exception here must never escape.
    """

    try:
        return access.refuse(tool_name, tool_use_id) if record else access.refusal(tool_name)
    except Exception:  # noqa: BLE001 - any failure to decide is a refusal
        if record and tool_use_id:
            # @spec RUNNER-TOOL-ACCESS-6: still a refusal, not a tool error.
            access.refused_call_ids.add(tool_use_id)
        return _DECISION_FAILED


def _deny(reason: str) -> dict[str, Any]:
    # No ``continue_: False``: the model is told and the turn carries on.
    return {
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "deny",
            "permissionDecisionReason": reason,
        }
    }


def front_pre_tool_use_hooks(
    hooks: Mapping[str, list[HookMatcher]] | None,
    access: TurnToolAccess,
) -> dict[str, list[HookMatcher]]:
    """``hooks`` with the read-only decision in front of every PreToolUse callback.

    @spec RUNNER-TOOL-ACCESS-2 RUNNER-TOOL-ACCESS-7
    """

    async def front(hook_input: Any, tool_use_id: str | None, _context: Any) -> dict[str, Any]:
        reason = _safe_refusal(access, _tool_name(hook_input), tool_use_id, record=True)
        return _deny(reason) if reason is not None else {}

    def wrap(callback: Any) -> Any:
        async def fronted(hook_input: Any, tool_use_id: str | None, context: Any) -> Any:
            reason = _safe_refusal(access, _tool_name(hook_input), tool_use_id, record=False)
            if reason is not None:
                return _deny(reason)
            return await callback(hook_input, tool_use_id, context)

        return fronted

    # Typed Any, as hooks.py does: the SDK's HookCallback union is narrower than
    # the mapping-or-dataclass input these callbacks deliberately accept.
    front_callback: Any = front
    composed = {event: list(matchers) for event, matchers in (hooks or {}).items()}
    composed["PreToolUse"] = [
        HookMatcher(matcher=None, hooks=[front_callback]),
        *(
            HookMatcher(
                matcher=matcher.matcher,
                hooks=[wrap(callback) for callback in matcher.hooks],
                timeout=matcher.timeout,
            )
            for matcher in composed.get("PreToolUse", [])
        ),
    ]
    return composed


def front_can_use_tool(inner: CanUseTool | None, access: TurnToolAccess) -> CanUseTool:
    """``inner`` with the read-only decision in front; allow when ``inner`` is None.

    @spec RUNNER-TOOL-ACCESS-2 RUNNER-TOOL-ACCESS-7
    """

    async def can_use_tool(
        tool_name: str,
        tool_input: dict[str, Any],
        context: ToolPermissionContext,
    ) -> PermissionResultAllow | PermissionResultDeny:
        reason = _safe_refusal(access, tool_name, context.tool_use_id, record=True)
        if reason is not None:
            return PermissionResultDeny(message=reason, interrupt=False)
        if inner is None:
            return PermissionResultAllow()
        return await inner(tool_name, tool_input, context)

    return can_use_tool

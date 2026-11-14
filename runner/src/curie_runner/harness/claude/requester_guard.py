"""The Claude SDK matcher for the requester section's subagent guard (ADR 0278, #5592).

`curie_runner.requester_context` owns the decision and stays free of the SDK
(ADR 0140); this module only mounts it as the in-process PreToolUse matcher
for ``Agent`` and ``Task`` calls.
"""

from __future__ import annotations

from typing import Any

from claude_agent_sdk import HookMatcher

from ...requester_context import SUBAGENT_TOOLS, RequesterContextGate, subagent_guard


def build_subagent_guard(gate: RequesterContextGate) -> dict[str, list[HookMatcher]]:
    """The PreToolUse matcher that refuses a subagent call carrying the section."""

    # ``Any`` as in approval.py: the SDK's ``HookCallback`` alias is typed
    # against its TypedDict union, and a ``dict[str, Any]`` return is not
    # assignable to it.
    callback: Any = subagent_guard(gate)
    return {"PreToolUse": [HookMatcher(matcher=SUBAGENT_TOOLS, hooks=[callback])]}

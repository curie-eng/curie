"""The Claude SDK matchers for the subagent guards (ADR 0278, #5592; #5798).

`curie_runner.requester_context` and `curie_runner.memory_rules_context` own
the decisions and stay free of the SDK (ADR 0140); this module only mounts
them as in-process PreToolUse matchers for ``Agent`` and ``Task`` calls.
"""

from __future__ import annotations

from typing import Any

from claude_agent_sdk import HookMatcher

from ...memory_rules_context import MemoryRulesContextGate
from ...memory_rules_context import subagent_guard as learned_rules_guard
from ...requester_context import SUBAGENT_TOOLS, RequesterContextGate, subagent_guard


def build_subagent_guard(gate: RequesterContextGate) -> dict[str, list[HookMatcher]]:
    """The PreToolUse matcher that refuses a subagent call carrying the section."""

    # ``Any`` as in approval.py: the SDK's ``HookCallback`` alias is typed
    # against its TypedDict union, and a ``dict[str, Any]`` return is not
    # assignable to it.
    callback: Any = subagent_guard(gate)
    return {"PreToolUse": [HookMatcher(matcher=SUBAGENT_TOOLS, hooks=[callback])]}


def build_learned_rules_guard(gate: MemoryRulesContextGate) -> dict[str, list[HookMatcher]]:
    """The PreToolUse matcher that refuses a subagent call carrying the learned rules."""

    callback: Any = learned_rules_guard(gate)
    return {"PreToolUse": [HookMatcher(matcher=SUBAGENT_TOOLS, hooks=[callback])]}

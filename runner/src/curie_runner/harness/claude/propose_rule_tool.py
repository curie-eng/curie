"""The Claude SDK tool ``propose_rule`` (ADR 0287, #5796).

`curie_runner.propose_rule` owns the schema, the description and the handler
and stays free of the SDK (ADR 0140); this module only wraps that handler as
the SDK tool the ``curie`` platform server mounts.
"""

from __future__ import annotations

from typing import Any

from claude_agent_sdk import SdkMcpTool, tool

from ...completion_contract import ResultHandler
from ...propose_rule import (
    CHANNEL_PROPOSE_RULE_DESCRIPTION,
    CHANNEL_PROPOSE_RULE_SCHEMA,
    PROPOSE_RULE_DESCRIPTION,
    PROPOSE_RULE_SCHEMA,
)
from ...tool_names import PROPOSE_RULE_TOOL


def wrap_propose_rule_handler(handler: ResultHandler) -> SdkMcpTool[Any]:
    """The SDK tool around a rule proposal handler."""

    return tool(PROPOSE_RULE_TOOL, PROPOSE_RULE_DESCRIPTION, PROPOSE_RULE_SCHEMA)(handler)


def wrap_channel_propose_rule_handler(handler: ResultHandler) -> SdkMcpTool[Any]:
    """The SDK tool around a channel rule proposal handler (#5801): the same
    name, with the channel description and the optional ``scope`` argument."""

    return tool(PROPOSE_RULE_TOOL, CHANNEL_PROPOSE_RULE_DESCRIPTION, CHANNEL_PROPOSE_RULE_SCHEMA)(
        handler
    )

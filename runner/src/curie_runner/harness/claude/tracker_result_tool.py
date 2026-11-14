"""The Claude SDK tool ``submit_tracker_result`` (ADR 0277, #5586).

`curie_runner.tracker_result` owns the contract discovery and the submission
handler and stays free of the SDK (ADR 0140); this module only wraps that
handler as the SDK tool the ``curie`` platform server mounts.
"""

from __future__ import annotations

from typing import Any

from claude_agent_sdk import SdkMcpTool, tool

from ...tool_names import TRACKER_RESULT_TOOL
from ...tracker_result import (
    TRACKER_RESULT_DESCRIPTION,
    TRACKER_RESULT_SCHEMA,
    ResultHandler,
    tracker_result_handler,
)


def wrap_tracker_result_handler(handler: ResultHandler) -> SdkMcpTool[Any]:
    """The SDK tool around a submission handler."""

    return tool(TRACKER_RESULT_TOOL, TRACKER_RESULT_DESCRIPTION, TRACKER_RESULT_SCHEMA)(handler)


def build_tracker_result_tool(url: str, token: str) -> SdkMcpTool[Any]:
    """The SDK tool ``submit_tracker_result``, closed over the route and capability."""

    return wrap_tracker_result_handler(tracker_result_handler(url, token))

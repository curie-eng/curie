"""The Claude SDK tool ``ask_questions`` (#5785).

`curie_runner.question_tool` owns the schema, the bounds check and the handler
and stays free of the SDK (ADR 0140); this module only wraps that handler as
the SDK tool the ``curie`` platform server mounts.
"""

from __future__ import annotations

from typing import Any

from claude_agent_sdk import SdkMcpTool, tool

from ...question_tool import QUESTIONS_DESCRIPTION, QUESTIONS_SCHEMA
from ...tool_names import QUESTION_TOOL
from ...tracker_result import ResultHandler


def wrap_questions_handler(handler: ResultHandler) -> SdkMcpTool[Any]:
    """The SDK tool around a questions handler."""

    return tool(QUESTION_TOOL, QUESTIONS_DESCRIPTION, QUESTIONS_SCHEMA)(handler)

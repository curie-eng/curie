"""The Claude SDK binding of the platform ``curie-slack`` server (ADR 0100, ADR 0200).

Only this package may import the SDK (ADR 0140), so the server object is built
here over the SDK-free holder and the read and canvas operations in
``curie_runner.platform_slack``. Each tool is present only with its own grant
and closes over the turn's holder; the capability itself is never a tool
argument.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from claude_agent_sdk import SdkMcpTool, create_sdk_mcp_server, tool
from claude_agent_sdk.types import McpSdkServerConfig
from plugin_format import CHANNEL_READ_SERVER_NAME

from ...platform_slack.canvases import (
    EDIT_CELL_SPEC,
    LIST_SPEC,
    READ_CANVAS_SPEC,
    edit_canvas_cell,
    list_canvases,
    read_canvas,
)
from ...platform_slack.capability import ChannelReadTurn
from ...platform_slack.reads import (
    HISTORY_SPEC,
    MESSAGE_SPEC,
    THREAD_SPEC,
    read_history,
    read_message,
    read_thread,
)

_Operation = Callable[[ChannelReadTurn, dict[str, Any]], Awaitable[dict[str, Any]]]


def _bind(
    spec: tuple[str, str, dict[str, Any]], operation: _Operation, turn: ChannelReadTurn
) -> SdkMcpTool[Any]:
    name, description, schema = spec

    async def handler(args: dict[str, Any]) -> dict[str, Any]:
        return await operation(turn, args)

    return tool(name, description, schema)(handler)


def build_channel_read_server(turn: ChannelReadTurn, grants: frozenset[str]) -> McpSdkServerConfig:
    """The in-process ``curie-slack`` server carrying only the granted tools.

    ``channelRead`` brings the three history tools, and ``canvasList``,
    ``canvasRead`` and ``canvasEdit`` each bring their one canvas tool. An
    empty grant set mounts nothing, so it is refused rather than built empty.
    """

    tools: list[SdkMcpTool[Any]] = []
    if "channelRead" in grants:
        tools += [
            _bind(HISTORY_SPEC, read_history, turn),
            _bind(THREAD_SPEC, read_thread, turn),
            _bind(MESSAGE_SPEC, read_message, turn),
        ]
    if "canvasList" in grants:
        tools.append(_bind(LIST_SPEC, list_canvases, turn))
    if "canvasRead" in grants:
        tools.append(_bind(READ_CANVAS_SPEC, read_canvas, turn))
    if "canvasEdit" in grants:
        tools.append(_bind(EDIT_CELL_SPEC, edit_canvas_cell, turn))
    if not tools:
        raise ValueError("a curie-slack server needs at least one platform Slack grant")
    return create_sdk_mcp_server(name=CHANNEL_READ_SERVER_NAME, tools=tools)

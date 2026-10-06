"""Tiny stdio MCP connector for the runner executor route's vector tests.

@spec ACTION-EXECUTOR-6 @spec ACTION-EXECUTOR-15. It advertises the tools in
``tests/vectors/runner-execute.json``'s ``list`` response (a forward ``scale``,
the paired ``restore`` and ``observe_version``), so a test drives the route's
phases against a real MCP session, not a stub of the route.

``CURIE_TEST_EXECUTOR_TOOLS`` picks the advertised set: ``paired`` (default),
``lone_restore`` (no ``observe_version``), ``no_restore``, or
``readonly_restore`` (``restore`` annotated read-only).
``CURIE_TEST_OBSERVE_REPLY`` is the JSON ``structuredContent`` that
``observe_version`` answers with (``null`` answers text only).
``CURIE_TEST_CALL_REPLY`` is the JSON ``structuredContent`` every write call
answers with.

Every ``tools/call`` that reaches the server is appended to the file named by
``CURIE_TEST_EXECUTOR_CALLS`` as one JSON line ``{"name", "arguments"}``, which
is how a test proves a refused phase never dialed and that a call carried
exactly the canonical arguments.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import anyio
from mcp import Tool, types
from mcp.server import ServerRequestContext
from mcp.server.lowlevel import Server
from mcp.server.stdio import stdio_server
from mcp.types import CallToolResult, ListToolsResult, TextContent, ToolAnnotations

_TARGET_SCHEMA = {
    "type": "object",
    "properties": {"target": {"type": "object"}},
    "required": ["target"],
}
_RESTORE_SCHEMA = {
    "type": "object",
    "properties": {
        "target": {"type": "object"},
        "prior_state": {"type": "object"},
        "expected_version": {"type": "string"},
    },
    "required": ["target", "prior_state"],
}


def _tools() -> list[Tool]:
    mode = os.environ.get("CURIE_TEST_EXECUTOR_TOOLS", "paired")
    tools = [
        Tool(
            name="scale",
            description="Scale an example deployment.",
            inputSchema={"type": "object"},
            annotations=ToolAnnotations(readOnlyHint=False),
        )
    ]
    if mode != "no_restore":
        tools.append(
            Tool(
                name="restore",
                description="Restore a sealed snapshot.",
                inputSchema=_RESTORE_SCHEMA,
                annotations=ToolAnnotations(readOnlyHint=mode == "readonly_restore"),
            )
        )
    if mode != "lone_restore":
        tools.append(
            Tool(
                name="observe_version",
                description="Report the version of an example target.",
                inputSchema=_TARGET_SCHEMA,
                annotations=ToolAnnotations(readOnlyHint=True),
            )
        )
    return tools


async def list_tools(
    _context: ServerRequestContext[object],
    _params: types.PaginatedRequestParams | None,
) -> ListToolsResult:
    return ListToolsResult(tools=_tools())


def _record(name: str, arguments: object) -> None:
    calls = os.environ.get("CURIE_TEST_EXECUTOR_CALLS")
    if calls:
        with Path(calls).open("a", encoding="utf-8") as output:
            output.write(json.dumps({"name": name, "arguments": arguments}) + "\n")


def _structured(variable: str) -> object:
    raw = os.environ.get(variable)
    return json.loads(raw) if raw else None


async def call_tool(
    _context: ServerRequestContext[object], params: types.CallToolRequestParams
) -> CallToolResult:
    _record(params.name, params.arguments)
    variable = (
        "CURIE_TEST_OBSERVE_REPLY" if params.name == "observe_version" else "CURIE_TEST_CALL_REPLY"
    )
    structured = _structured(variable)
    if isinstance(structured, dict):
        return CallToolResult(
            content=[TextContent(type="text", text=json.dumps(structured))],
            structuredContent=structured,
        )
    return CallToolResult(content=[TextContent(type="text", text="done")])


async def main() -> None:
    server = Server(
        "executor-fixture",
        version="1.0.0",
        on_list_tools=list_tools,
        on_call_tool=call_tool,
    )
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


if __name__ == "__main__":
    anyio.run(main)

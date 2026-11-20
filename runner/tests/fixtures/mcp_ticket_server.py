"""Tiny stdio MCP connector for the runner's approval ticket carry tests (ADR 0288).

``prepare`` answers the way an ADR 0271 preparation tool does: a JSON text
block whose ``apply_grant_arguments`` carry the signed ticket named by
``CURIE_TEST_TICKET``. ``apply`` is the gated tool; every call that reaches it
appends its exact arguments, as one JSON line, to the file named by
``CURIE_TEST_TICKET_CALLS``, which is how a test proves which ticket the
connector actually received.
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
from mcp.types import CallToolResult, ListToolsResult, TextContent

RESOURCE = "example-repo"


async def list_tools(
    _context: ServerRequestContext[object],
    _params: types.PaginatedRequestParams | None,
) -> ListToolsResult:
    schema = {
        "type": "object",
        "properties": {"resource": {"type": "string"}, "approval_ticket": {"type": "string"}},
    }
    return ListToolsResult(
        tools=[
            Tool(name="prepare", description="Prepare a test request.", input_schema=schema),
            Tool(name="apply", description="Apply a test request.", input_schema=schema),
        ]
    )


async def call_tool(
    _context: ServerRequestContext[object], params: types.CallToolRequestParams
) -> CallToolResult:
    if params.name == "prepare":
        ticket = os.environ["CURIE_TEST_TICKET"]
        reply = {"apply_grant_arguments": {"resource": RESOURCE, "approval_ticket": ticket}}
        return CallToolResult(content=[TextContent(type="text", text=json.dumps(reply))])
    calls = os.environ.get("CURIE_TEST_TICKET_CALLS")
    if calls:
        with Path(calls).open("a", encoding="utf-8") as output:
            output.write(json.dumps({"tool": params.name, "arguments": params.arguments}) + "\n")
    return CallToolResult(content=[TextContent(type="text", text=f"{params.name} ok")])


async def main() -> None:
    server = Server("acme", version="1.0.0", on_list_tools=list_tools, on_call_tool=call_tool)
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


if __name__ == "__main__":
    anyio.run(main)

"""Tiny stdio MCP connector for the runner's tool result signal tests.

Three tools, one per result a connector can give the runner: ``read_ledger``
answers with ``isError`` set, ``list_files`` answers normally, and
``delete_files`` would answer normally but is the one a test gates for approval,
so it must never be reached. Every call that does reach the server is appended
to the file named by ``CURIE_TEST_TOOL_RESULT_CALLS``, which is how a test proves
a held call never ran.
"""

from __future__ import annotations

import os
from pathlib import Path

import anyio
from mcp import Tool, types
from mcp.server import ServerRequestContext
from mcp.server.lowlevel import Server
from mcp.server.stdio import stdio_server
from mcp.types import CallToolResult, ListToolsResult, TextContent

# The text a failing call answers with. A test asserts the runner's log never
# carries it, so it is distinctive on purpose.
LEDGER_ERROR_TEXT = "upstream answered 401 for acme-ledger-PLACEHOLDER"


async def list_tools(
    _context: ServerRequestContext[object],
    _params: types.PaginatedRequestParams | None,
) -> ListToolsResult:
    schema = {"type": "object", "properties": {"account": {"type": "string"}}}
    return ListToolsResult(
        tools=[
            Tool(name="read_ledger", description="Read a test ledger.", input_schema=schema),
            Tool(name="list_files", description="List test files.", input_schema=schema),
            Tool(name="delete_files", description="Delete test files.", input_schema=schema),
        ]
    )


async def call_tool(
    _context: ServerRequestContext[object], params: types.CallToolRequestParams
) -> CallToolResult:
    calls = os.environ.get("CURIE_TEST_TOOL_RESULT_CALLS")
    if calls:
        with Path(calls).open("a", encoding="utf-8") as output:
            output.write(f"{params.name}\n")
    if params.name == "read_ledger":
        return CallToolResult(
            content=[TextContent(type="text", text=LEDGER_ERROR_TEXT)],
            is_error=True,
        )
    return CallToolResult(content=[TextContent(type="text", text=f"{params.name} ok")])


async def main() -> None:
    server = Server(
        "acme",
        version="1.0.0",
        on_list_tools=list_tools,
        on_call_tool=call_tool,
    )

    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options(),
        )


if __name__ == "__main__":
    anyio.run(main)

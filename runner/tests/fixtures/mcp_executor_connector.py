"""Tiny MCP connector for the runner executor route's vector tests.

@spec ACTION-EXECUTOR-6 @spec ACTION-EXECUTOR-15. It advertises the tools in
``tests/vectors/runner-execute.json``'s ``list`` response (a forward ``scale``,
the paired ``restore`` and ``observe_version``), so a test drives the route's
phases against a real MCP session, not a stub of the route. It runs two ways:

* over stdio (``python mcp_executor_connector.py``), configured by env, as a
  connector the executor cannot attach a grant to and as the catalogue probe's
  boot connector;
* over streamable HTTP (``http_app``), configured in process, as the hosted
  connector a grant header rides to; the app records every JSON-RPC request
  with its headers, so a test sees the grant and every ``tools/list`` page.

Settings (env name in parentheses for stdio):

* ``tools`` (``CURIE_TEST_EXECUTOR_TOOLS``): ``paired`` (default),
  ``lone_restore``, ``no_restore`` or ``readonly_restore``.
* ``observe_reply`` (``CURIE_TEST_OBSERVE_REPLY``, JSON): the
  ``structuredContent`` ``observe_version`` answers with; null answers text only.
* ``call_reply`` (``CURIE_TEST_CALL_REPLY``, JSON): the ``structuredContent``
  every write call answers with.
* ``list_pages`` (``CURIE_TEST_LIST_PAGES``): how many ``tools/list`` pages the
  catalogue spans; every page but the last carries a ``nextCursor``.
* ``call_result_bytes`` (``CURIE_TEST_CALL_RESULT_BYTES``): when set, a write
  call answers with a structured result padded past that many bytes.

Over stdio every ``tools/call`` is appended to the file named by
``CURIE_TEST_EXECUTOR_CALLS`` as one JSON line ``{"name", "arguments"}``.
"""

from __future__ import annotations

import json
import os
from collections.abc import Callable
from pathlib import Path
from typing import Any

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


def _tools(mode: str) -> list[Tool]:
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


def build_server(
    settings: dict[str, Any], record: Callable[[str, object], None] | None = None
) -> Server[Any]:
    """One connector server for ``settings``; ``record`` sees every call."""

    pages = int(settings.get("list_pages") or 1)

    async def list_tools(
        _context: ServerRequestContext[object],
        params: types.PaginatedRequestParams | None,
    ) -> ListToolsResult:
        cursor = params.cursor if params is not None else None
        page = int(cursor.removeprefix("page-")) if cursor else 1
        if page < pages:
            return ListToolsResult(tools=[], nextCursor=f"page-{page + 1}")
        return ListToolsResult(tools=_tools(str(settings.get("tools") or "paired")))

    async def call_tool(
        _context: ServerRequestContext[object], params: types.CallToolRequestParams
    ) -> CallToolResult:
        if record is not None:
            record(params.name, params.arguments)
        if params.name == "observe_version":
            structured = settings.get("observe_reply")
        else:
            structured = settings.get("call_reply")
            padding = settings.get("call_result_bytes")
            if padding:
                structured = {"ok": True, "padding": "x" * (int(padding) + 1)}
        if isinstance(structured, dict):
            return CallToolResult(
                content=[TextContent(type="text", text="structured reply")],
                structuredContent=structured,
            )
        return CallToolResult(content=[TextContent(type="text", text="done")])

    return Server(
        "executor-fixture",
        version="1.0.0",
        on_list_tools=list_tools,
        on_call_tool=call_tool,
    )


def http_app(settings: dict[str, Any], requests: list[dict[str, Any]]) -> Any:
    """The connector over streamable HTTP at ``/mcp``, recording every request.

    Each JSON-RPC request lands in ``requests`` as ``{"method", "params",
    "headers"}`` (header names lowercased), in arrival order.
    """

    inner = build_server(settings).streamable_http_app(
        streamable_http_path="/mcp",
        json_response=True,
        stateless_http=True,
        host="127.0.0.1",
    )

    async def app(scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] != "http" or scope.get("method") != "POST":
            await inner(scope, receive, send)
            return
        chunks: list[bytes] = []
        more = True
        while more:
            message = await receive()
            chunks.append(message.get("body", b""))
            more = message.get("more_body", False)
        body = b"".join(chunks)
        headers = {key.decode().lower(): value.decode() for key, value in scope["headers"]}
        try:
            parsed = json.loads(body)
        except ValueError:
            parsed = None
        for item in parsed if isinstance(parsed, list) else [parsed]:
            if isinstance(item, dict) and "method" in item:
                requests.append(
                    {"method": item["method"], "params": item.get("params"), "headers": headers}
                )
        replayed = False

        async def replay() -> dict[str, Any]:
            nonlocal replayed
            if replayed:
                return await receive()
            replayed = True
            return {"type": "http.request", "body": body, "more_body": False}

        await inner(scope, replay, send)

    return app


def _env_settings() -> dict[str, Any]:
    def loaded(name: str) -> object:
        raw = os.environ.get(name)
        return json.loads(raw) if raw else None

    return {
        "tools": os.environ.get("CURIE_TEST_EXECUTOR_TOOLS", "paired"),
        "observe_reply": loaded("CURIE_TEST_OBSERVE_REPLY"),
        "call_reply": loaded("CURIE_TEST_CALL_REPLY"),
        "list_pages": os.environ.get("CURIE_TEST_LIST_PAGES"),
        "call_result_bytes": os.environ.get("CURIE_TEST_CALL_RESULT_BYTES"),
    }


def _record_to_file(name: str, arguments: object) -> None:
    calls = os.environ.get("CURIE_TEST_EXECUTOR_CALLS")
    if calls:
        with Path(calls).open("a", encoding="utf-8") as output:
            output.write(json.dumps({"name": name, "arguments": arguments}) + "\n")


async def main() -> None:
    server = build_server(_env_settings(), _record_to_file)
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


if __name__ == "__main__":
    anyio.run(main)

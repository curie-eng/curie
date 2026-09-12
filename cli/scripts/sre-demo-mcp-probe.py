"""Inspect the actual pinned connector; never emit its potentially secret response text."""

import asyncio
import json
import logging
import re
import sys


async def probe_session(session, expected_namespace):
    # MCP tools/list is paginated and tools/call can fail at protocol or tool
    # level: https://modelcontextprotocol.io/specification/2025-11-25/server/tools
    # A transport failure is not a configuration_view denial.
    from mcp.shared.exceptions import MCPError

    catalog = await session.list_tools()
    if catalog.next_cursor:
        raise AssertionError("catalog is incomplete")
    names = {tool.name for tool in catalog.tools}
    if "namespaces_list" not in names or "configuration_view" in names:
        raise AssertionError("connector catalog violates the required tool boundary")
    healthy = await session.call_tool("namespaces_list", {})
    words = set(re.findall(r"[a-z0-9][a-z0-9-]*", healthy.model_dump_json()))
    if healthy.is_error or expected_namespace not in words:
        raise AssertionError("permitted read did not return the task-owned namespace")
    try:
        forbidden = await session.call_tool("configuration_view", {})
    except MCPError as exc:
        # Invalid/missing tool must be explicit; arbitrary server failures and
        # unavailable transport cannot pass this negative control.
        refused = exc.code in {-32601, -32602} and bool(
            re.search(
                r"unknown tool[: ]+configuration_view\b|"
                r"tool [\"']?configuration_view[\"']? "
                r"(?:not found|not available|does not exist)\b",
                exc.message,
                re.I,
            )
        )
    else:
        text = forbidden.model_dump_json()
        refused = forbidden.is_error and bool(
            re.search(
                r"unknown tool[: ]+configuration_view\b|"
                r"tool [\"']?configuration_view[\"']? "
                r"(?:not found|not available|does not exist)\b",
                text,
                re.I,
            )
        )
    if not refused:
        raise AssertionError("forbidden invocation was not explicitly refused")
    return {"catalog": "pass", "permitted_read": "pass", "forbidden_invocation": "pass"}


async def main(url, expected_namespace):
    from mcp import ClientSession
    from mcp.client.streamable_http import streamable_http_client

    # Same installed SDK transport as curie_runner.mcp_tool_capability.
    async with asyncio.timeout(60):
        async with streamable_http_client(url) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                result = await probe_session(session, expected_namespace)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    # SDK errors may embed a complete tool result, including kubeconfig. Raw
    # responses and exception groups never enter logs, tracebacks or evidence.
    logging.disable(logging.CRITICAL)
    try:
        asyncio.run(main(*sys.argv[1:]))
    except Exception:
        raise SystemExit("MCP outcome probe failed; response withheld") from None

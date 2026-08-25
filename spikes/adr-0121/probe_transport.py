"""Can a process invoke a connector's tool WITHOUT the agent SDK?

ADR-0117's spike concluded the runner cannot: "the SDK owns those client
sessions", and reaching one would mean "building a second MCP client inside the
runner, which is a large new mechanism for one job". ADR-0121 inherits that as
its stated cost.

This probes the premise. A hosted connector's MCP entry is
`{"type": "http", "url": "http://<service>:<port>/mcp"}` -- streamable HTTP, not
a stdio subprocess the SDK spawned. So the question is not "can we take a session
from the SDK" but "how big is an HTTP client for one call".

Measured against the real k8s-scale connector, started here.
"""

import asyncio
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CONNECTOR = ROOT / "examples/sre-bot/connectors/k8s-scale"


async def call_tool(url: str, tool: str, arguments: dict) -> dict:
    """One MCP tool call over streamable HTTP. This is the whole client."""
    from mcp import ClientSession
    from mcp.client.streamable_http import streamablehttp_client

    async with streamablehttp_client(url) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.call_tool(tool, arguments)
            return {
                "is_error": bool(result.isError),
                "content": [c.text for c in result.content if hasattr(c, "text")],
            }


async def list_tools(url: str) -> list[str]:
    from mcp import ClientSession
    from mcp.client.streamable_http import streamablehttp_client

    async with streamablehttp_client(url) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            return [t.name for t in (await session.list_tools()).tools]


async def main() -> int:
    url = os.environ.get("SPIKE_MCP_URL", "http://127.0.0.1:8931/mcp")
    print(f"== connector at {url}")
    tools = await list_tools(url)
    print(f"   tools/list -> {tools}")

    reply = await call_tool(
        url, "scale_deployment", {"namespace": "public", "name": "api", "replicas": 3}
    )
    print(f"   tools/call -> is_error={reply['is_error']}")
    for text in reply["content"]:
        try:
            parsed = json.loads(text)
            print(f"   reply keys  -> {sorted(parsed)}")
            print(f"   ok={parsed.get('ok')} summary={parsed.get('summary')!r}")
        except json.JSONDecodeError:
            print(f"   reply (prose) -> {text[:120]}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

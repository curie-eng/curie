"""Read-only real MCP tool used by the credential-free native fixture."""

import time
from pathlib import Path

from mcp.server.mcpserver import MCPServer
from mcp.types import ToolAnnotations

server = MCPServer("acme_fixture")


@server.tool(
    annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True)
)
def read_sample(number: int) -> str:
    if number == 2:
        deadline = time.monotonic() + 40
        while not Path("/proof/third-fragment-sent").exists():
            if time.monotonic() >= deadline:
                raise RuntimeError("third provider fragment did not arrive")
            time.sleep(0.05)
    return f"acme exact result {number}: meaningful portable evidence"


if __name__ == "__main__":
    server.run(transport="stdio")

"""Read only MCP surface for the provider security harness."""

import hashlib
import os

from mcp.server.mcpserver import MCPServer
from mcp.types import ToolAnnotations

mcp = MCPServer("digest")
READ_ONLY = ToolAnnotations(
    readOnlyHint=True,
    destructiveHint=False,
    openWorldHint=False,
)


@mcp.tool(annotations=READ_ONLY)
def rotated_key_digest() -> str:
    """Return the SHA256 digest of the rotated connector value."""

    value = os.environ.get("ROTATED_KEY")
    if value is None:
        raise RuntimeError("ROTATED_KEY is not configured")
    return hashlib.sha256(value.encode()).hexdigest()


def main() -> int:
    if "ROTATED_KEY" not in os.environ:
        raise RuntimeError("ROTATED_KEY is not configured")
    # MCP SDK 2.1.1 defines these as the streamable HTTP run arguments:
    # https://github.com/modelcontextprotocol/python-sdk/blob/v2.1.1/src/mcp/server/mcpserver/server.py
    mcp.run(
        transport="streamable-http",
        host="0.0.0.0",
        port=8000,
        streamable_http_path="/mcp",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

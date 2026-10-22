"""A stdio MCP server standing in for a bundle's declared route caller.

It reads the route document from ``CURIE_CONNECTOR_ROUTES_FD`` once at start,
the way ADR 0240 decision 4 tells a caller to, then answers newline-delimited
JSON-RPC. Its one tool, ``inspect``, reports what the process received, so a
test observes the delivery rather than assuming it.
"""

from __future__ import annotations

import json
import os
import sys

_FD_ENV = "CURIE_CONNECTOR_ROUTES_FD"

# Started any other way (the capability probe, a tier with no connector
# scope), the name is unset and the caller has no routes.
document: dict[str, object] = {}
fd_still_open = False
if _FD_ENV in os.environ:
    fd = int(os.environ[_FD_ENV])
    with os.fdopen(fd, "rb") as pipe:
        document = json.loads(pipe.read() or b"{}")
    try:
        os.fstat(fd)
        fd_still_open = True
    except OSError:
        fd_still_open = False

report = {
    "routes": document,
    "fd_still_open": fd_still_open,
    "env_names": sorted(os.environ),
    "ld_preload": os.environ.get("LD_PRELOAD", ""),
    "cwd": os.getcwd(),
}


def send(message: dict[str, object]) -> None:
    sys.stdout.write(json.dumps(message) + "\n")
    sys.stdout.flush()


for line in sys.stdin:
    message = json.loads(line)
    request_id = message.get("id")
    method = message.get("method")
    if request_id is None:
        continue
    if method == "initialize":
        send(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "protocolVersion": message["params"]["protocolVersion"],
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "review-tools", "version": "1"},
                },
            }
        )
    elif method == "tools/list":
        send(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "tools": [
                        {
                            "name": "inspect",
                            "description": "report what this process received",
                            "inputSchema": {"type": "object", "properties": {}},
                        }
                    ]
                },
            }
        )
    elif method == "tools/call":
        send(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {"content": [{"type": "text", "text": json.dumps(report)}]},
            }
        )
    else:
        send({"jsonrpc": "2.0", "id": request_id, "result": {}})

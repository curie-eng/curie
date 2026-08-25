"""The full ADR-0121 loop, with real code at every step.

    ledger row  ->  API rules on the undo  ->  executor calls the connector's
    restore verb  ->  the world moves  ->  the executor confirms to the ledger

What this is probing, in order of how likely it was to be wrong:

1. That the platform can hand the connector back its OWN words. The ledger holds
   `{"spec": {"replicas": 3}}` and the connector's verb takes `prior_state` --
   no mapping table between them, anywhere. That is ADR-0121's whole claim.
2. That the ruling and the execution stay separable: a REFUSED undo must reach
   no connector at all.
3. That confirmation closes ADR-0117's compromise, where `undone_at` is stamped
   at ruling time because nothing reports back.
"""

import asyncio
import json
import os
import subprocess
import sys
import urllib.request

API = os.environ.get("SPIKE_API", "http://localhost:28000")
MCP = os.environ.get("SPIKE_MCP_URL", "http://127.0.0.1:8000/mcp")
KEY = subprocess.run(
    ["docker", "exec", "curie-curie-api-1", "printenv", "API_KEY"],
    capture_output=True, text=True, check=True,
).stdout.strip()


def api(method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(f"{API}{path}", data=data, method=method)
    req.add_header("X-API-Key", KEY)
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


async def call_tool(tool: str, arguments: dict) -> dict:
    from mcp import ClientSession
    from mcp.client.streamable_http import streamablehttp_client

    async with streamablehttp_client(MCP) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.call_tool(tool, arguments)
            text = next((c.text for c in result.content if hasattr(c, "text")), "{}")
            return json.loads(text)


async def world() -> int:
    """Read the connector's view by scaling to what it already is."""
    reply = await call_tool(
        "scale_deployment", {"namespace": "public", "name": "api", "replicas": 0}
    )
    current = reply["prior"]["spec"]["replicas"]
    await call_tool(
        "scale_deployment", {"namespace": "public", "name": "api", "replicas": current}
    )
    return current


async def main() -> int:
    print("== 1. an agent changes the world, and the ledger records it")
    forward = await call_tool(
        "scale_deployment", {"namespace": "public", "name": "api", "replicas": 10}
    )
    print(f"   connector: {forward['summary']}")

    status, opened = api("POST", "/actions", {
        "conversation_id": "C-SPIKE", "call_id": "spike-1", "tool": "scale_deployment",
        "arguments": {"namespace": "public", "name": "api", "replicas": 10},
        "dedupe_key": f"spike-{os.getpid()}:1",
    })
    action_id = opened["id"]
    api("POST", f"/actions/{action_id}/complete", {
        "failed": not forward["ok"], "result": forward,
        "prior_state": forward["prior"], "post_state": forward["post"],
        "target": forward["target"], "detail": "spike",
    })
    _, row = api("GET", f"/actions/{action_id}")
    print(f"   ledger:    undoable={row['undoable']} prior={json.dumps(row['prior_state'])}")

    print("\n== 2. a REFUSED undo must not reach the connector")
    before = await world()
    status, refused = api("POST", f"/actions/{action_id}/undo",
                          {"actor": "U-op", "observed_state": {"spec": {"replicas": 7}}})
    after = await world()
    print(f"   API: HTTP {status} {refused.get('detail')}")
    print(f"   world unchanged by the refusal: {before} -> {after}  {'OK' if before == after else 'FAIL'}")

    print("\n== 3. an AUTHORIZED undo: the API rules, the executor replays")
    status, ruling = api("POST", f"/actions/{action_id}/undo",
                         {"actor": "U-op", "observed_state": row["post_state"]})
    print(f"   API: HTTP {status}, restore = {json.dumps(ruling['restore'])}")

    # THE EXECUTOR. It hands the connector back its own words -- no mapping.
    restored = await call_tool("restore", {
        "target": ruling["restore"]["target"],
        "prior_state": ruling["restore"]["prior_state"],
    })
    print(f"   connector: {restored['summary']}")
    print(f"   world now: {await world()}")

    print("\n== 4. the executor confirms, closing ADR-0117's open compromise")
    status, _ = api("POST", f"/actions/{action_id}/confirm-undo",
                    {"ok": restored["ok"], "summary": restored["summary"]})
    print(f"   POST /actions/{{id}}/confirm-undo -> HTTP {status}"
          f"{'  (endpoint does not exist yet -- see below)' if status == 404 else ''}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

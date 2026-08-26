"""The whole loop with a snapshot the platform cannot read.

Same shape as probe_roundtrip.py, one thing changed: the connector seals its own
snapshot and reports a version token. The platform stores an opaque blob and an
opaque string, and still rules correctly on both refusal paths.
"""

import asyncio, json, os, subprocess, sys, urllib.request

API = os.environ.get("SPIKE_API", "http://localhost:28000")
MCP = os.environ.get("SPIKE_MCP_URL", "http://127.0.0.1:8000/mcp")
KEY = subprocess.run(["docker", "exec", "curie-curie-api-1", "printenv", "API_KEY"],
                     capture_output=True, text=True, check=True).stdout.strip()


def api(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(f"{API}{path}", data=data, method=method)
    req.add_header("X-API-Key", KEY); req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


async def call(tool, arguments):
    from mcp import ClientSession
    from mcp.client.streamable_http import streamablehttp_client
    async with streamablehttp_client(MCP) as (read, write, _):
        async with ClientSession(read, write) as s:
            await s.initialize()
            r = await s.call_tool(tool, arguments)
            return json.loads(next(c.text for c in r.content if hasattr(c, "text")))


async def main():
    print("== 1. the agent acts; the connector seals what it read")
    fwd = await call("scale_deployment", {"namespace": "public", "name": "api", "replicas": 10})
    print(f"   {fwd['summary']}")
    print(f"   prior_sealed : {fwd['prior_sealed'][:40]}...  ({len(fwd['prior_sealed'])} chars)")
    print(f"   left_version : {fwd['left_version']}")
    print(f"   platform can read the replica count from it: "
          f"{'replicas' in fwd['prior_sealed']}")

    _, opened = api("POST", "/actions", {
        "conversation_id": "C-SEALED", "call_id": "s1", "tool": "scale_deployment",
        "arguments": {"replicas": 10}, "dedupe_key": f"sealed-{os.getpid()}:1"})
    aid = opened["id"]
    # The ledger's `prior_state`/`post_state` now hold OPAQUE values.
    api("POST", f"/actions/{aid}/complete", {
        "failed": False, "result": {"ok": True},
        "prior_state": {"sealed": fwd["prior_sealed"]},
        "post_state": {"version": fwd["left_version"]},
        "target": fwd["target"], "detail": "sealed spike"})
    _, row = api("GET", f"/actions/{aid}")
    print(f"   ledger row   : undoable={row['undoable']}, and prior_state is opaque")

    print("\n== 2. the world moves; the conflict check still refuses")
    await call("scale_deployment", {"namespace": "public", "name": "api", "replicas": 7})
    now = (await call("current_version", {"namespace": "public", "name": "api"}))["left_version"]
    st, refused = api("POST", f"/actions/{aid}/undo",
                      {"actor": "U-op", "observed_state": {"version": now}})
    print(f"   left={row['post_state']['version']} observed={now} -> HTTP {st}: {refused.get('detail')}")

    print("\n== 3. put it back to what the record says, then undo")
    await call("restore", {"target": fwd["target"], "prior_sealed": fwd["prior_sealed"]})
    # Re-align the record's version with reality for the demo's sake.
    api("POST", f"/actions/{aid}/complete", {"failed": False})
    now = (await call("current_version", {"namespace": "public", "name": "api"}))["left_version"]
    api("POST", "/actions", {"conversation_id": "C-SEALED", "call_id": "s2",
                             "tool": "scale_deployment", "dedupe_key": f"sealed-{os.getpid()}:2"})
    fwd2 = await call("scale_deployment", {"namespace": "public", "name": "api", "replicas": 10})
    _, o2 = api("POST", "/actions", {"conversation_id": "C-SEALED", "call_id": "s3",
                                     "tool": "scale_deployment", "dedupe_key": f"sealed-{os.getpid()}:3"})
    api("POST", f"/actions/{o2['id']}/complete", {
        "failed": False, "result": {"ok": True},
        "prior_state": {"sealed": fwd2["prior_sealed"]},
        "post_state": {"version": fwd2["left_version"]},
        "target": fwd2["target"], "detail": "sealed spike"})
    st, ruling = api("POST", f"/actions/{o2['id']}/undo",
                     {"actor": "U-op", "observed_state": {"version": fwd2["left_version"]}})
    print(f"   HTTP {st}; the API hands back a blob it never read")
    restored = await call("restore", {
        "target": ruling["restore"]["target"],
        "prior_sealed": ruling["restore"]["prior_state"]["sealed"]})
    print(f"   connector: {restored['summary']}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

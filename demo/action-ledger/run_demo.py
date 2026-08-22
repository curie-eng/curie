"""ADR-0117: what a turn did to your systems, and taking it back.

Real connector, real ACI translate seam, real API against a real Postgres, real
receipt card. The only stand-in is the Kubernetes API server, which is a dict
holding one number.
"""
import importlib.util
import json
import os
import tempfile
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

import yaml
from claude_agent_sdk import AssistantMessage, ToolResultBlock, ToolUseBlock, UserMessage
from curie_runner.side_effects import CLAUDE_READONLY_TOOLS, SideEffectClassifier
from curie_runner.translate import TurnState, translate_message
from curie_worker.blocks import receipt_card

C = {"g": "\033[32m", "r": "\033[31m", "y": "\033[33m", "b": "\033[1m", "d": "\033[2m",
     "c": "\033[36m", "x": "\033[0m"}
API = os.environ.get("CURIE_API", "http://127.0.0.1:28999")
KEY = os.environ.get("CURIE_API_KEY", "curie-dev-key")
ROOT = Path(__file__).resolve().parents[2]
CLUSTER = {"public/api": 3}
TOOL = "mcp__k8s-scale__scale_deployment"
TURN = f"turn-{uuid.uuid4().hex[:8]}"


def say(text: str = "") -> None:
    print(text, flush=True)
    time.sleep(0.35)


class _Resp:
    def __init__(s, code, payload=None):
        s.status_code, s._p = code, payload or {}

    def json(s):
        return s._p

class _Api:
    """The Kubernetes API server, as a dict."""
    def __init__(s, t):
        s.t = t

    def __enter__(s):
        return s

    def __exit__(s, *e):
        return False

    def get(s, path):
        return _Resp(200, {"spec": {"replicas": CLUSTER[s.t]}})

    def patch(s, path, body):
        CLUSTER[s.t] = body["spec"]["replicas"]
        return _Resp(200, {})


def load_connector():
    tmp = Path(tempfile.mkdtemp())
    cfg = tmp / "kubeconfig"
    cfg.write_text(yaml.safe_dump({"clusters": [{"cluster": {"server": "https://k8s:6443"}}],
                                   "users": [{"user": {"token": "t"}}]}), encoding="utf-8")
    os.environ.update(KUBECONFIG_PATH=str(cfg), K8S_SCALE_ALLOWLIST="public/api",
                      K8S_SCALE_MAX_REPLICAS="50")
    spec = importlib.util.spec_from_file_location(
        "demo_k8s_scale", ROOT / "examples/sre-bot/connectors/k8s-scale/server.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    m._client = lambda: _Api("public/api")
    return m


def api(method, path, body=None):
    req = urllib.request.Request(
        f"{API}{path}", method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"X-API-Key": KEY, "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return r.status, json.loads(r.read() or "null")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or "null")


def through_the_runner(tool, args, reply):
    """The real ACI seam: the runner emits one frame for the call, one for its result."""
    state, clf = TurnState(), SideEffectClassifier(CLAUDE_READONLY_TOOLS)
    call = [e for e in translate_message(
        AssistantMessage(content=[ToolUseBlock(id="c1", name=tool, input=args)], model="demo"),
        state, clf, None) if type(e).__name__ == "SideEffectFlag"]
    res = [e for e in translate_message(
        UserMessage(content=[ToolResultBlock(tool_use_id="c1", content=reply)]),
        state, clf, None) if type(e).__name__ == "SideEffectFlag"]
    return call[0], (res[0] if res else None)


def record(call_flag, result_flag, tool):
    r = (result_flag.result if result_flag else None) or {}
    ok = bool(r.get("ok"))
    _, row = api("POST", "/actions", {
        "conversation_id": "C0DEMO:1", "turn_id": TURN, "tool": tool,
        "arguments": call_flag.arguments, "target": r.get("target"),
        "snapshot": r.get("prior"),
        "snapshot_status": "captured" if r.get("prior") else "absent",
        "post_state": (
            {"spec": {"replicas": call_flag.arguments.get("replicas")}}
            if ok and r.get("prior")
            else None
        ),
        "outcome": "succeeded" if ok else "failed",
        "irreversible_reason": None if r.get("prior") else "restarting pods cannot be undone",
        "dedupe_key": f"{TURN}-{uuid.uuid4().hex[:8]}",
        "reply_kind": "slack", "reply_channel": "C0DEMO"})
    return row


def main():
    srv = load_connector()
    say(f"{C['b']}An SRE bot is about to change production.{C['x']}")
    say(f"{C['d']}public/api is running {CLUSTER['public/api']} replicas.{C['x']}")
    say()

    say(f"{C['b']}[1] the agent scales it, and the connector reports what it read{C['x']}")
    args = {"namespace": "public", "name": "api", "replicas": 10}
    reply = srv.scale_deployment(**args)
    say(f"    {C['d']}{reply}{C['x']}")
    say(f"    live cluster: public/api = {C['y']}{CLUSTER['public/api']}{C['x']}")
    say()

    say(f"{C['b']}[2] the runner turns that into an action, the API records it{C['x']}")
    c, r = through_the_runner(TOOL, args, reply)
    scaled = record(c, r, TOOL)
    say(f"    {C['c']}POST /actions{C['x']} -> {scaled['id']}")
    say(f"    snapshot = {json.dumps(scaled['snapshot'])}   undoable = "
        f"{C['g'] if scaled['undoable'] else C['r']}{scaled['undoable']}{C['x']}")

    c2, r2 = through_the_runner("mcp__k8s-write__restart_deployment",
                                {"namespace": "public", "name": "api"},
                                "restart triggered for public/api")
    restarted = record(c2, r2, "mcp__k8s-write__restart_deployment")
    say(f"    {C['c']}POST /actions{C['x']} -> {restarted['id']}")
    say(
        f"    snapshot = null                       undoable = "
        f"{C['r']}{restarted['undoable']}{C['x']}"
        f"  {C['d']}(prose reply, nothing to restore){C['x']}"
    )
    say()

    say(f"{C['b']}[3] the receipt the on-call sees{C['x']}")
    _, rows = api("GET", f"/actions?turn_id={TURN}")
    for row in rows:
        row["summary"] = ("scaled public/api from 3 to 10" if row["undoable"]
                          else "restarted public/api")
    fallback, blocks = receipt_card(rows)
    for b in blocks:
        text = (b.get("text") or {}).get("text") or (b.get("elements") or [{}])[0].get("text", "")
        button = "  [ Undo ]" if "accessory" in b else ""
        colour = C['g'] if button else (C['d'] if b["type"] == "context" else "")
        say(f"    {colour}{text}{C['x']}{C['g']}{button}{C['x']}")
    say()

    say(f"{C['b']}[4] someone presses Undo{C['x']}")
    observed = {"spec": {"replicas": CLUSTER['public/api']}}
    code, out = api("POST", f"/actions/{scaled['id']}/undo",
                    {"actor": "U_ONCALL", "actor_channel": "C0DEMO", "observed_state": observed})
    say(f"    {C['c']}POST /actions/{{id}}/undo{C['x']} -> {code}")
    if code == 200:
        prior = scaled["snapshot"]["spec"]["replicas"]
        srv.scale_deployment(namespace="public", name="api", replicas=prior)
        say(f"    {C['g']}restored: public/api = {CLUSTER['public/api']}{C['x']}")
    say()

    say(f"{C['b']}[5] the same button, after a human already fixed it by hand{C['x']}")
    args2 = {"namespace": "public", "name": "api", "replicas": 10}
    reply2 = srv.scale_deployment(**args2)
    c3, r3 = through_the_runner(TOOL, args2, reply2)
    second = record(c3, r3, TOOL)
    say(f"    {C['d']}the bot scaled it again; action {second['id'][:8]} recorded{C['x']}")
    CLUSTER["public/api"] = 7
    say(f"    {C['y']}a human sets it to 7{C['x']}")
    code, out = api("POST", f"/actions/{second['id']}/undo",
                    {"actor": "U_ONCALL", "observed_state": {"spec": {"replicas": 7}}})
    say(
        f"    {C['c']}POST /actions/{{id}}/undo{C['x']} -> "
        f"{C['r']}{code}{C['x']} {out.get('detail', '')}"
    )
    say(
        f"    {C['g']}public/api = {CLUSTER['public/api']}{C['x']}"
        f"  {C['d']}the manual fix survives{C['x']}"
    )
    _, audit = api("GET", f"/actions/{second['id']}/audit")
    say(f"    {C['d']}audit: {audit[0]['action']} -- {json.dumps(audit[0]['evidence'])}{C['x']}")


if __name__ == "__main__":
    main()

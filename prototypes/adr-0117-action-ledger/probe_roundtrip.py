"""SPIKE (throwaway): the whole loop, with real code at every step but the API server.

  real connector  ->  real translate seam  ->  assembled record  ->  undo  ->  restored

The only fake is the Kubernetes API itself, which is a dict. The connector, the
SDK message types, and curie_runner.translate.translate_message are the real ones.
"""
import importlib.util, json, os, sys, yaml
from pathlib import Path
from claude_agent_sdk import AssistantMessage, ToolUseBlock, ToolResultBlock, UserMessage
from curie_runner.translate import translate_message, TurnState
from curie_runner.side_effects import SideEffectClassifier, CLAUDE_READONLY_TOOLS

C = {"g": "\033[32m", "r": "\033[31m", "y": "\033[33m", "b": "\033[1m", "d": "\033[2m", "x": "\033[0m"}
ROOT = Path(__file__).resolve().parents[2]

CLUSTER = {"public/api": 3}          # the fake world
TOOL = "mcp__k8s-scale__scale_deployment"


class _Resp:
    def __init__(self, code, payload=None):
        self.status_code, self._p = code, (payload or {})
    def json(self): return self._p

class _Api:
    """Stands in for the Kubernetes API server. Holds one number."""
    def __init__(self, target): self.target = target
    def __enter__(self): return self
    def __exit__(self, *e): return False
    def get(self, path): return _Resp(200, {"spec": {"replicas": CLUSTER[self.target]}})
    def patch(self, path, body):
        CLUSTER[self.target] = body["spec"]["replicas"]; return _Resp(200, {})


def load_connector(tmp: Path):
    cfg = tmp / "kubeconfig"
    cfg.write_text(yaml.safe_dump({
        "clusters": [{"cluster": {"server": "https://k8s.example:6443"}}],
        "users": [{"user": {"token": "t"}}]}), encoding="utf-8")
    os.environ.update(KUBECONFIG_PATH=str(cfg), K8S_SCALE_ALLOWLIST="public/api",
                      K8S_SCALE_MAX_REPLICAS="50")
    spec = importlib.util.spec_from_file_location(
        "probe_k8s_scale", ROOT / "examples/sre-bot/connectors/k8s-scale/server.py")
    m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
    m._client = lambda: _Api("public/api")
    return m


def through_the_runner(tool, args, connector_reply):
    """Push one real tool call and its real reply through the real translate seam."""
    state, clf = TurnState(), SideEffectClassifier(CLAUDE_READONLY_TOOLS)
    am = AssistantMessage(content=[ToolUseBlock(id="c1", name=tool, input=args)], model="probe")
    call_flags = [e for e in translate_message(am, state, clf, None)
                  if type(e).__name__ == "SideEffectFlag"]
    um = UserMessage(content=[ToolResultBlock(tool_use_id="c1", content=connector_reply)])
    result_flags = [e for e in translate_message(um, state, clf, None)
                    if type(e).__name__ == "SideEffectFlag"]
    # The RUNNER assembles this now. The probe only reads the frames it emitted.
    rec = {"tool": call_flags[0].tool, "arguments": call_flags[0].arguments,
           "result": result_flags[0].result if result_flags else None,
           "_frames": len(call_flags) + len(result_flags)}
    return rec


def main():
    import tempfile
    tmp = Path(tempfile.mkdtemp())
    srv = load_connector(tmp)

    print(f"{C['b']}world before{C['x']}   public/api replicas = {C['y']}{CLUSTER['public/api']}{C['x']}")

    print(f"\n{C['b']}[1] the agent scales it (real connector){C['x']}")
    args = {"namespace": "public", "name": "api", "replicas": 10}
    reply = srv.scale_deployment(**args)
    print(f"    connector reply: {C['d']}{reply}{C['x']}")
    print(f"    world now:       public/api replicas = {C['y']}{CLUSTER['public/api']}{C['x']}")

    print(f"\n{C['b']}[2] through the real runner translate seam{C['x']}")
    rec = through_the_runner(TOOL, args, reply)
    prior = (rec.get("result") or {}).get("prior")
    undoable = bool(rec["result"] and rec["result"].get("ok") and prior)
    print(f"    {C['g']}frames the RUNNER emitted: {rec['_frames']}{C['x']}"
          f"  {C['d']}(one for the call, one for its result){C['x']}")
    print(f"    record.tool      = {rec['tool']}")
    print(f"    record.arguments = {rec['arguments']}")
    print(f"    record.prior     = {prior}")
    print(f"    {C['g'] if undoable else C['r']}undoable = {undoable}{C['x']}")

    print(f"\n{C['b']}[3] world-moved check, then undo (platform replays; no model){C['x']}")
    expected_now = rec["arguments"]["replicas"]
    actual_now = CLUSTER["public/api"]
    if actual_now != expected_now:
        print(f"    {C['r']}REFUSED: expected {expected_now}, found {actual_now}{C['x']}")
        return
    print(f"    {C['d']}world still at {actual_now}, matches what the action left{C['x']}")
    restore = {"namespace": "public", "name": "api", "replicas": prior["spec"]["replicas"]}
    undo_reply = json.loads(srv.scale_deployment(**restore))
    print(f"    undo call: scale_deployment(replicas={restore['replicas']})  ok={undo_reply['ok']}")
    print(f"    {C['g']}world after undo: public/api replicas = {CLUSTER['public/api']}{C['x']}")
    assert CLUSTER["public/api"] == 3, "undo did not restore"

    print(f"\n{C['b']}[4] the same loop for an IRREVERSIBLE tool{C['x']}")
    rec2 = through_the_runner("mcp__k8s-write__restart_deployment",
                              {"namespace": "public", "name": "api"},
                              "restart triggered for public/api")
    prior2 = (rec2.get("result") or {}).get("prior") if rec2["result"] else None
    print(f"    connector reply is prose, not JSON -> result = {rec2['result']}")
    print(f"    {C['r']}undoable = False{C['x']}  {C['d']}(nothing reported a prior state){C['x']}")

    print(f"\n{C['b']}[5] the rule that matters most: somebody changed it by hand{C['x']}")
    srv.scale_deployment(namespace="public", name="api", replicas=10)   # the bot again
    CLUSTER["public/api"] = 7                                           # a human, by hand
    expected, actual = 10, CLUSTER["public/api"]
    print(f"    action left it at {expected}; the world is now at {C['y']}{actual}{C['x']}")
    if actual != expected:
        print(f"    {C['g']}REFUSED{C['x']}: public/api changed since this action "
              f"(expected {expected}, found {actual})")
        print(f"    {C['g']}world untouched: public/api replicas = {CLUSTER['public/api']}{C['x']}"
              f"  {C['d']}(the manual fix survives){C['x']}")
        assert CLUSTER["public/api"] == 7, "a refused undo must change nothing"
    else:
        print(f"    {C['r']}BUG: the conflict check did not fire{C['x']}")

    print(f"\n{C['b']}=== receipt the user would see ==={C['x']}")
    print(f"  {C['g']}[undo]{C['x']}  scaled public/api 3 -> 10")
    print(f"  {C['r']}  --  {C['x']}  restarted public/api   {C['d']}restarting pods cannot be undone{C['x']}")

if __name__ == "__main__":
    main()

"""SPIKE (throwaway): does an action record survive the runner's translate seam?

Answers three questions with the real SDK types and the real translate module:
  1. what today's runner emits for a side-effecting call
  2. whether the tool RESULT reaches the runner at all
  3. whether a per-call record can be assembled from what arrives
Nothing here is product code.
"""
import json, sys
from claude_agent_sdk import AssistantMessage, ToolUseBlock, ToolResultBlock, UserMessage
from curie_runner.translate import translate_message, TurnState
from curie_runner.side_effects import SideEffectClassifier, CLAUDE_READONLY_TOOLS

C = {"g": "\033[32m", "r": "\033[31m", "b": "\033[1m", "d": "\033[2m", "x": "\033[0m"}
clf = SideEffectClassifier(CLAUDE_READONLY_TOOLS)
state = TurnState()

TOOL = "mcp__k8s-write__scale_deployment"
ARGS = {"namespace": "prod", "name": "payments-api", "replicas": 10}
RESULT = {"ok": True, "summary": "scaled prod/payments-api from 3 to 10",
          "prior": {"spec": {"replicas": 3}},
          "target": {"namespace": "prod", "name": "payments-api"}}

print(f"{C['b']}[1] two side-effecting calls in one turn -- what does the runner emit today?{C['x']}")
msg = AssistantMessage(content=[
    ToolUseBlock(id="call-1", name=TOOL, input=ARGS),
    ToolUseBlock(id="call-2", name="mcp__k8s-write__restart_deployment",
                 input={"namespace": "prod", "name": "worker-api"}),
], model="probe")
events = translate_message(msg, state, clf, None)
flags = [e for e in events if type(e).__name__ == "SideEffectFlag"]
print(f"    tool calls made: 2")
print(f"    {C['r'] if len(flags)<2 else C['g']}SideEffectFlag frames emitted: {len(flags)}{C['x']}")
for f in flags:
    print(f"      tool={f.tool!r} detail={f.detail!r} "
          f"arguments={getattr(f,'arguments','<field does not exist>')}")

print(f"\n{C['b']}[2] the tool RESULT arrives -- what does the runner do with it?{C['x']}")
um = UserMessage(content=[ToolResultBlock(tool_use_id="call-1", content=json.dumps(RESULT))])
print(f"    UserMessage carries: content blocks + tool_use_result field")
out = translate_message(um, state, clf, None)
print(f"    {C['r'] if not out else C['g']}events produced from the result: {len(out)}{C['x']}"
      f"  {C['d']}(v0.1 contract drops UserMessage){C['x']}")

print(f"\n{C['b']}[3] can a per-call record be assembled from what already arrives?{C['x']}")
pending = {}
for b in msg.content:
    if isinstance(b, ToolUseBlock) and clf.is_side_effecting(b.name):
        pending[b.id] = {"tool": b.name, "arguments": b.input}
for b in um.content:
    if isinstance(b, ToolResultBlock) and b.tool_use_id in pending:
        try:
            pending[b.tool_use_id]["result"] = json.loads(b.content)
        except Exception:
            pending[b.tool_use_id]["result"] = None
rec = pending["call-1"]
prior = (rec.get("result") or {}).get("prior")
print(f"    {C['g']}assembled record{C['x']}: tool={rec['tool']}")
print(f"      arguments = {rec['arguments']}")
print(f"      prior     = {prior}")
undoable = prior is not None
print(f"      {C['g'] if undoable else C['r']}undoable = {undoable}{C['x']}"
      f"  {C['d']}(restore replicas {prior['spec']['replicas'] if undoable else '?'}){C['x']}")
rec2 = pending["call-2"]
print(f"    {C['g']}assembled record{C['x']}: tool={rec2['tool']}")
print(f"      {C['r']}undoable = False{C['x']}  {C['d']}(no result reported -> nothing to restore){C['x']}")

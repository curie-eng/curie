# PT-0117: can an action record be assembled from what the runner already receives?

Throwaway probe behind `docs/design/action-ledger-and-undo.md`. It is not product
code and nothing imports it.

## Why it exists

The design's first draft took a snapshot in the runner's pre-execution permission
callback. That seam cannot reach a snapshot: the runner never invokes an MCP tool
outside the model loop, because the SDK owns those client sessions. Rather than
build a second MCP client inside the runner, the probe asks whether the
information is already arriving.

## Run it

```bash
uv run --project runner python prototypes/adr-0117-action-ledger/probe_ledger.py
```

It uses the real `claude_agent_sdk` message types and the real
`curie_runner.translate.translate_message`. No network, no cluster, no model.

## What it found

Two side-effecting calls in one turn produce **one** `SideEffectFlag`, because
`state.side_effect_emitted` caps it and the only consumer is a boolean. `detail`
is the constant string `"non-idempotent tool executed"`, and there is no field
for the arguments even though they are in hand.

The tool result produces **zero** events. It arrives as a `UserMessage` carrying
a `ToolResultBlock` (and the SDK also exposes a `tool_use_result` field on that
message), and the v0.1 contract drops the whole message on purpose.

From exactly those two arrivals, a per-call record assembles with no new
transport:

```
tool      = mcp__k8s-write__scale_deployment
arguments = {'namespace': 'prod', 'name': 'payments-api', 'replicas': 10}
prior     = {'spec': {'replicas': 3}}
undoable  = True

tool      = mcp__k8s-write__restart_deployment
undoable  = False        (no prior state reported, so nothing to restore)
```

The second line is the part worth keeping. An irreversible tool falls out as
not-undoable because nothing reported prior state, not because anything special
cased it, which is the same deny-by-default posture the read-only allowlist in
`runner/src/curie_runner/side_effects.py` already takes.

## What it does not show

The connector does not report prior state today. `scale_deployment` does not
exist yet, and `restart_deployment` reads the Deployment before patching it and
discards what it read. The probe scripts the result a cooperating connector would
return, which is the first task of the plan and is not evidence on its own.

## Round trip: `probe_roundtrip.py`

```bash
uv run --project runner python prototypes/adr-0117-action-ledger/probe_roundtrip.py
```

The second probe runs the whole loop with real code at every step but the
Kubernetes API server, which is a dict holding one number:

```
world before   public/api replicas = 3
[1] real connector scales it            reply carries prior {"spec": {"replicas": 3}}
[2] real translate seam                 record.prior captured, undoable = True
[3] world-moved check passes, undo      world back to 3
[4] the irreversible tool               prose reply, result None, undoable = False
[5] a human sets it to 7 by hand        REFUSED, world stays 7
```

Step 5 is the rule worth having. A refused undo changes nothing, so somebody's
manual fix survives an undo pressed after they made it.

## What is real here and what is not

Real: `examples/sre-bot/connectors/k8s-scale/server.py` including its allowlist,
ceiling, subresource path and refusal paths; the `claude_agent_sdk` message
types; `curie_runner.translate.translate_message`.

Not real: the Kubernetes API server. No cluster, no network, no model, no
platform. The receipt at the end is printed by the probe to show the shape the
design proposes, not rendered by any card code, which does not exist yet.

Update: the runner does the assembly now. It emits two frames per action, one
for the call carrying `arguments` and one for the result carrying `result`, and
the probe only reads them. The line it prints, "frames the RUNNER emitted: 2", is
the difference between this probe measuring a proposal and measuring the product.

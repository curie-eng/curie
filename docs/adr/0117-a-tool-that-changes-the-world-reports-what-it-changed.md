# 117. A tool that changes the world reports what it changed, and the platform can put it back

Date: 2026-08-21

Status: Draft

Completes the pair [ADR-0010](0010-approval-gates-and-human-in-the-loop.md)
opened. That decision answers a mutating action **before** it happens; this one
answers it **after**. It changes no isolation boundary:
[ADR-0006](0006-security-rails-as-chart-defaults.md)'s rails and
[ADR-0008](0008-multi-tenancy.md)'s tenant compute stand as they are, and a
sandbox reaches nothing new.

## Context

Curie already detects that a turn changed the world, and records one bit.

[`runner/src/curie_runner/side_effects.py`](../../runner/src/curie_runner/side_effects.py)
classifies every tool absent from a harness-declared read-only allowlist as
side-effecting, deny-by-default, so an unknown tool is assumed to mutate. The
runner emits a `side_effect_flag`, and the kernel reduces the whole stream to
`saw_side_effect: bool`
([`apps/worker/src/curie_worker/kernel.py`](../../apps/worker/src/curie_worker/kernel.py))
for exactly one purpose: refusing to auto-retry a failed turn that already
touched something (ADR-0013).

That is the entire memory the platform keeps of an agent acting on production.
Not which tool. Not with what arguments. Not against what resource. Not whether
it worked. So when a bot changes something, the person who asked gets a sentence
of prose from the model and no account from the platform, and there is nothing to
act on afterwards.

The gap is not that the information is hard to obtain. It is that the information
is already in hand and is being discarded, in three separate places.

- **The arguments.** The frame is built at
  [`runner/src/curie_runner/translate.py::_translate_assistant`](../../runner/src/curie_runner/translate.py)
  from the SDK's tool-use block, which carries the call's input. The frame was
  built as `SideEffectFlag(tool=block.name, detail="non-idempotent tool
  executed")`: the tool name kept, the arguments dropped, and `detail` filled
  with a constant string.

- **The result.** A tool result arrives as a `UserMessage`, and the v0.1 contract
  dropped that message whole, with the comment "carry no outbound-visible
  content in the v0.1 contract; they are intentionally dropped."

- **The prior state.** The example's own write connector reads the resource
  before patching it and throws the object away, keeping only the status code
  ([`examples/sre-bot/connectors/k8s-write/server.py::restart_deployment`](../../examples/sre-bot/connectors/k8s-write/server.py)).
  Its RBAC says so out loud: `get` "is needed to read the Deployment before
  patching it and to confirm the patch was accepted afterwards"
  ([`examples/sre-bot/manifests/write-role.yaml`](../../examples/sre-bot/manifests/write-role.yaml)).

There is also a cap that only makes sense for a boolean consumer: the flag fired
**once per turn**, because a boolean cannot be set twice. A turn that called
three mutating tools reported one.

### What a spike ruled out

The first design took the snapshot in the runner's pre-execution permission
callback, `build_can_use_tool` in
[`runner/src/curie_runner/approval.py`](../../runner/src/curie_runner/approval.py),
which does see the arguments before the call executes and cannot be skipped by
the model. That seam cannot reach a snapshot. The runner never invokes an MCP
tool outside the model loop: MCP servers are handed to the SDK
([`runner/src/curie_runner/adapter.py`](../../runner/src/curie_runner/adapter.py)),
the SDK owns those client sessions, and every out-of-loop call the runner makes
([`runner/src/curie_runner/state.py`](../../runner/src/curie_runner/state.py),
[`runner/src/curie_runner/memory.py`](../../runner/src/curie_runner/memory.py))
is plain HTTP to the platform API. Taking a snapshot there would mean building a
second MCP client inside the runner, which is a large new mechanism for one job.

The spike is in
[`prototypes/adr-0117-action-ledger/`](../../prototypes/adr-0117-action-ledger/).

## Decision

**A tool that changes the world reports what it changed, in its own reply. The
platform records that report as an action, and can replay the recorded prior
state to put it back, with no model in the path.**

1. **The tool's reply is the declaration.** There is **no manifest surface** for
   reversibility. A connector that can be undone returns the state it read
   immediately before it wrote; a connector that cannot says why, in the same
   reply. Reversibility is deny-by-default: a tool that reports neither is not
   undoable, and third-party MCP servers land in that case without anyone
   declaring anything.

   A manifest declaration was considered and rejected. It would be a second
   source of truth that can disagree with what the tool actually returned, and
   the disagreement resolves the wrong way: the manifest says reversible, the
   reply carries nothing, and the platform either lies to the user or ignores the
   manifest. One source, and it is the one that was there when the write
   happened.

2. **The platform executes the compensation deterministically.** Undo replays the
   recorded prior state through the same tool. It is not a turn, the model is not
   asked, and nothing is inferred. If the answer to "I am afraid to let a bot
   touch production" is a bot reasoning about how to un-touch it, the fear
   doubles rather than resolving.

3. **An undo requires the authorization the forward action required, and no
   more.** If the tool was gated by an approval policy, undoing it needs an
   authorizer of that same route, resolved in the API against membership the way
   [ADR-0034](0034-approval-authorizers-resolve-membership-in-the-api.md) resolves
   an approver. If the tool was not gated, the undo is not gated either.

   The rule follows from what a restore does. The state being restored is one the
   cluster was already in, and it got there without anyone approving it, because
   it predates the action. Nobody needs permission to put back what was there.
   What they need is to be someone who could have permitted the change in the
   first place, which is exactly the symmetry above.

4. **A restore is refused when the world has moved.** Before replaying, the
   platform compares the resource's live state to what the action left. On a
   mismatch it refuses and names both states. This is the rule the feature lives
   or dies on: a blind restore silently reverts a human's manual fix, which turns
   an undo button into a way for the platform to fight the operator.

5. **A recorded action does not expire.** Approvals carry `expires_at` because a
   question left unanswered goes stale. A record of something that happened does
   not. Time is also the wrong bound: a change one minute later makes a restore
   unsafe and a quiet week does not make it safer, so decision 4's conflict check
   is the real bound and a TTL would refuse safe undos while permitting unsafe
   ones inside the window.

6. **The action is the unit, not the turn.** The side-effect frame fires once per
   call rather than once per turn, and the record, the receipt line, and the undo
   are all per action. `saw_side_effect` still latches on the first, so the
   no-retry rule reads exactly the signal it read before.

7. **The user acts on a receipt.** A turn that changed anything ends with a card
   listing each action, each carrying either an undo control or the stated reason
   it has none, rendered through the existing approval-card path. Whole-turn undo
   is a later, thin layer over the same rows and is not decided here.

**The ledger invariant** (what we test and review to): every side-effecting call
produces exactly one durable record; a record claims to be undoable only when it
holds a prior state, a target, and a successful outcome; and an undo either
restores the recorded state or changes nothing at all.

## Business case

**The fear this answers is the one that stops the sale.** Curie's premiere
example is an SRE bot, and the objection to an SRE bot is never "can it read
metrics". It is that somebody has to let it write. ADR-0010 answered the half
before the action. The half after is empty: today a bot changes production and
the platform's entire account of it is prose the model wrote about itself.

**It converts the platform's most conservative posture into a feature.** The
read-only allowlist means Curie already treats every unknown tool as dangerous.
That posture currently produces nothing except a refusal to retry. The same
classification, recorded rather than reduced, is a per-action receipt, and the
same deny-by-default gives a truthful answer for tools nobody declared anything
about.

**"Undo" is a category Curie's competitors do not have, because they do not have
the pieces.** Reversal needs a per-conversation durable record, a gated tool
surface, an audit trail, and a channel that can render an interactive card.
Curie has all four already, built for approvals. This is the second product built
on machinery that was paid for once.

**The honest half sells as well as the undoable half.** A receipt that says
"restarting pods cannot be undone" next to one that says "scaled 3 to 10, undo"
tells an operator the system knows the difference. That is the thing they are
actually buying: not a bot that cannot make mistakes, but a platform that knows
which mistakes it can take back.

## Evidence (built and measured 2026-08-21)

The mechanism is implemented and tested, not proposed. Everything below is a
suite run on this branch.

**The connector reports prior state.**
[`examples/sre-bot/connectors/k8s-scale/server.py`](../../examples/sre-bot/connectors/k8s-scale/server.py)
returns `{"ok", "summary", "prior", "target"}` on every path, including refusals.
A failed read refuses to write at all, because an action that happened and cannot
be undone is worse than one that did not: it leaves the platform holding a record
it cannot act on. **14 tests**, and **54** with both connector suites in one
pytest run, which is the module-name collision the `k8s-write` suite warns about.

It is also a narrower grant than the connector it sits beside. A rollout restart
is a PATCH of the pod template, so RBAC `patch` on `deployments` is the same
grant as `set image`, which is why `k8s-write` enforces the separation in Python.
Scaling has `deployments/scale` as its own subresource, so Kubernetes refuses an
image change rather than this codebase doing it.

**The ACI carries it.** `SideEffectFlag` gained `arguments` and `result`, both
optional with a `None` default. That is a **patch** under
[ADR-0036](0036-aci-semver-and-reader-policy.md), not a minor:
[`packages/aci-protocol/src/aci_protocol/version.py`](../../packages/aci-protocol/src/aci_protocol/version.py)
states that a new optional field is a patch because tolerant consumers ignore it,
and the minor is reserved for breaking changes under 0.x. `0.4.1` to `0.4.2`, and
the wire-lock fingerprint moved with it, which is that gate working as designed.
**175 tests.**

**The runner records per call.** The once-per-turn cap is gone and tool results
are forwarded for side-effecting calls only, matched to their call by
`tool_use_id`. Read-only results stay dropped, because file contents are the
model's working material rather than wire traffic. A prose reply carries no
structured result, deliberately: guessing structure out of a sentence is how
something downstream restores a guess. **566 tests.**

**The ledger exists.** `agent_actions` and `action_audit_entries` are shaped after
`approvals` and `approval_audit_entries`, and `undoable` is a derived property so
a row cannot claim reversibility it has no snapshot for. The migration is
hand-written, because autogenerate compared against the wrong search path and
proposed dropping Langfuse's `monitors`, `table_view_presets` and
`default_llm_models`, which share the database. It round-trips upgrade, downgrade,
upgrade against a real Postgres. **911 tests.**

**The loop runs.** `prototypes/adr-0117-action-ledger/probe_roundtrip.py`, with
real code at every step but the API server:

```
world before   public/api replicas = 3
[1] connector scales it           reply carries prior {"spec": {"replicas": 3}}
[2] the RUNNER emits 2 frames     record.prior captured, undoable = True
[3] conflict check, then undo     world back to 3
[4] the irreversible tool         prose reply, no prior state, undoable = False
[5] a human sets it to 7 by hand  REFUSED, world stays 7
```

Step 5 is decision 4 working: a refused undo changes nothing, so a manual fix
survives an undo pressed after it.

**The loop runs through the platform.** `demo/action-ledger/run_demo.py`, recorded
at [`docs/demo/adr-0117-undo.gif`](../demo/adr-0117-undo.gif), with the API
answering over HTTP against a real Postgres:

```
[1] connector scales it              reply carries prior {"spec": {"replicas": 3}}
[2] runner emits 2 frames            POST /actions x2, one undoable, one not
[3] the receipt                      both lines, the second stating its reason
[4] undo                             200, recorded state goes back
[5] a human set it to 7 first        409, both states named, the fix survives
```

**A latent test defect surfaced.**
`apps/api/tests/test_migration_0021a_v0_6_x_reconcile.py` pinned head as the
literal `"0027"` in two places, so every future migration would have failed a
test about the 0021 collision. Both now resolve the head from the script
directory, which is the claim the test actually makes.

## Alternatives considered

- **A mapping DSL: "tool X is undone by calling Y with arguments derived from
  X's."** Rejected on two grounds, and the second is the decisive one. It needs
  an expression language, which is the commodity-engine category
  [ADR-0007](0007-adopt-not-build-boundaries.md) exists to keep out. And it does
  not have the information: undoing `scale_deployment(replicas=10)` needs the
  count from **before** the call, which no function of the forward arguments can
  produce. Snapshot-restore is not the safer mechanism, it is the only one that
  knows the answer.

- **Snapshot in the pre-execution permission callback.** Rejected by the spike
  above: the runner cannot invoke an MCP tool outside the model loop without a
  second MCP client that exists for this one job.

- **A reversibility declaration in `plugin.json`, beside `approvalPolicy`.**
  Rejected per decision 1. Two sources of truth for one fact, and the failure
  mode is the platform telling a user an action is undoable when nothing captured
  the state.

- **Letting the agent perform the undo as another turn.** Rejected per decision
  2. It is more flexible and strictly less trustworthy, and the undo would itself
  be a side-effecting turn needing its own record and its own approval, which
  regresses infinitely.

- **A TTL on undoability, mirroring `approvals.expires_at`.** Rejected per
  decision 5. Symmetry with approvals is not a reason, and time is the wrong
  variable.

- **Whole-turn undo as the primary surface.** Deferred, not rejected. Per-action
  is the honest unit because actions succeed and fail independently, and a
  turn-level control has to answer what "undo" means when two of three actions
  are reversible. It is a thin layer over these rows once they exist.

## Consequences

- **A connector that wants to be undoable has to return JSON.** That is a real
  cost to every write connector anyone writes, and the reason it is acceptable is
  that the connector was already reading the state; the change is to report it
  rather than discard it.

- **Third-party MCP write tools are not undoable, and will say so on the
  receipt.** This is the honest outcome and it is also a gap a user will notice.
  It is the same shape as the read-only allowlist's deny-by-default, and the
  remedy is the same: wrap the tool in a connector that reports.

- **An additive ACI field is free for readers and not for constructors.** The
  reader policy holds: a consumer that predates `arguments` and `result` parses
  the frame unchanged. But the generated Rust is an enum with struct variants,
  and a struct-variant literal has to name every field, so
  [`cli/src/render.rs`](../../cli/src/render.rs) failed to compile until it
  named the two new ones. Pattern matches with `..` were unaffected. The lesson
  is that "additive" describes the wire, not every language binding, and the
  Rust build is what says so.

- **The kernel now sees more side-effect frames than before.** It reads the same
  signal (`saw_side_effect` still latches on the first), so the no-retry rule is
  unchanged, but `kernel.py` is sacred under ADR-0013 and the widened stream is
  the part of this change to review hardest.

- **The receipt is a new surface that can be noisy.** A turn with many small
  writes produces many lines. Whole-turn undo and grouping are the answers, and
  both are out of scope here, so the first version will be verbose for a busy
  agent.

- **Snapshots can hold secrets.** A snapshot of a Kubernetes object can carry
  environment variables and secret references. They pass through the existing
  redaction path, which means some resources are honestly not snapshot-able and
  must report themselves irreversible rather than storing credentials in the
  control plane.

- **Undo is a new write path into a customer's infrastructure**, authorized by
  decision 3 and audited by `action_audit_entries`. It reuses the connector's own
  credential and allowlist, so it can reach nothing the forward action could not.

## The one decision this ADR does not make

**Where the undo executor lives is open, and building the demo is what surfaced
it.** Nothing in the platform can reach a connector today: neither
`apps/api` nor `apps/worker` has an MCP client, and every out-of-loop HTTP call
either makes is to the platform's own API. So the API rules on an undo and
returns, and something else has to perform the call the ruling authorizes.

Three candidates, none settled here:

- **An MCP client in the worker.** The dependency is already present
  transitively, so this is the smallest change. It also makes the control plane
  a direct client of tenant connectors, which is a reachability question ADR-0008
  has opinions about.
- **A new ACI verb, executed by the runner in a sandbox.** Architecturally the
  most conservative, because the sandbox is where a connector's credential,
  allowlist and network policy already apply, so an undo could reach nothing the
  forward call could not. It is also the most work: the runner would need to
  invoke an MCP tool outside the model loop, which is precisely what the spike
  found it cannot do today.
- **The connector exposes a plain HTTP replay endpoint.** Avoids MCP entirely and
  changes the connector contract for every author.

The ruling and the execution are already separate in the code, which is what
makes deferring this safe: a refusal is recorded and returned before anything
could act on it, so whichever executor lands cannot bypass the check by holding
the connector's address.

## Out of scope

- **Whole-turn and time-windowed undo**, per the alternatives above.
- **Preview before acting.** Showing what a tool *would* do without doing it is
  the other half of the same intuition and is its own decision.
- **Retention and pruning of the ledger.** Rows accumulate; nothing here decides
  when they leave.
- **Undo from the console.** The receipt lands on the channel that asked. The UI
  surface is a separate piece of work.

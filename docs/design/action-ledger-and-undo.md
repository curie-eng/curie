# Design pass: the action ledger and undo

> Status: **Design pass** behind
> [ADR-0117](../adr/0117-a-tool-that-changes-the-world-reports-what-it-changed.md)
> (Draft), which settles the open questions this document raised.
> No implementation is committed in this doc, and no ADR authorizes one yet
> ([ADR-0085](../adr/0085-acceptance-not-implementation-authorizes-an-adr.md), as
> amended by [ADR-0102](../adr/0102-accepted-alongside-implementation-with-explicit-approval.md)).
>
> Related: [ADR-0010](../adr/0010-approval-gates-and-human-in-the-loop.md)
> (approval gates, the pre-action half of this),
> [ADR-0013](../adr/0013-concurrency-and-delivery-model.md) (the kernel
> invariants, including the no-retry-after-side-effects rule this builds on),
> [ADR-0046](../adr/0046-converged-approval-gates-and-durable-provenance.md)
> (durable provenance), [ADR-0060](../adr/0060-the-harness-is-a-declared-package.md)
> (the harness declares its own tool semantics),
> [ADR-0078](../adr/0078-approval-cards-over-connected-transport.md) (cards on a
> channel), and [ADR-0007](../adr/0007-adopt-not-build-boundaries.md), which is
> why one of the three mechanisms below is rejected outright.

## The problem in one paragraph

Curie already detects that a turn changed the world. It records **one bit**. The
runner classifies every tool that is not on a harness-declared read-only
allowlist as side-effecting, emits a `side_effect_flag`, and the kernel reduces
that to `saw_side_effect: bool` used for exactly one purpose: refusing to retry.
Nothing records **what** was changed, on **what**, with **what arguments**, or
whether it worked. So when a bot changes production, the person who asked gets a
sentence of prose from the model and no account from the platform, and there is
nothing to act on afterwards. Approval gates (ADR-0010) answer this **before** an
action. Nothing answers it **after**.

## What already exists, and why the scope is smaller than it looks

The pieces are unusually well positioned, and the design below is mostly a matter
of not discarding what is already computed.

- **The classifier is already deny-by-default.**
  [`runner/src/curie_runner/side_effects.py`](../../runner/src/curie_runner/side_effects.py)
  is a read-only allowlist: a harness declares which of its tools only read, and
  everything else is treated as potentially side-effecting. A new or unknown tool
  escalates rather than being silently retried. The same posture is exactly right
  for reversibility, and this design reuses the shape rather than inventing one.

- **The ACI frame already names the tool.** `SideEffectFlag` carries `tool` and an
  optional `detail`. The runner fills them at
  [`runner/src/curie_runner/translate.py::_translate_assistant`](../../runner/src/curie_runner/translate.py):

  ```python
  SideEffectFlag(tool=block.name, detail="non-idempotent tool executed")
  ```

  `detail` is a constant string. The arguments are in `block` at that exact
  moment and are dropped on the floor.

- **There is already a pre-execution seam that sees the arguments.**
  `build_can_use_tool` in
  [`runner/src/curie_runner/approval.py`](../../runner/src/curie_runner/approval.py)
  is the SDK permission callback, and it receives `tool_name` **and
  `tool_input`** *before the call executes*. Its own docstring draws the
  distinction this design needs: the decision is "proactive -- the call is
  blocked before execution -- unlike the reactive `side_effect_flag` classifier,
  which only reports after the fact." A snapshot has to be taken before the
  mutation, and this is where that is possible. ADR-0099's bundle-declared
  `PreToolUse` hooks are the second such seam.

- **The durable-record pattern exists, with an audit trail.** `approvals` and
  `approval_audit_entries` in
  [`apps/api/src/curie_api/models.py`](../../apps/api/src/curie_api/models.py)
  already model "a thing that happened to a conversation, with routing back to
  the surface that asked, a status lifecycle, an expiry, a resolver, and an
  evidence blob."

- **Cards with buttons on a channel already work.** `ApprovalCardStore` and
  `apps/worker/src/curie_worker/blocks.py` in the worker render and track interactive cards, and the API
  exposes `GET /{approval_id}`, `GET /{approval_id}/audit`, and
  `POST /{approval_id}/resolve`. A receipt with an Undo button is that machinery
  pointed at a different row.

- **The read-before-write habit is already in the example's RBAC.**
  [`examples/sre-bot/manifests/write-role.yaml`](../../examples/sre-bot/manifests/write-role.yaml)
  grants `get` because it "is needed to read the Deployment before patching it
  and to confirm the patch was accepted afterwards." The snapshot this design
  needs is a read the write path was already making.

## Decisions taken

These were settled during the design conversation and are recorded here so the
ADR can cite them rather than re-argue them.

**1. The platform executes a compensation, deterministically, with no model in
the path.** Undo is a replay of a declared inverse, not a second round of
inference. If the answer to "I am afraid to let a bot touch production" is a bot
reasoning about how to un-touch it, the fear doubles. It also keeps the demo
reproducible, and it means an undo cannot invent an action nobody recorded.

**2. The mechanism is snapshot-then-restore, and a compensation is declared, not
inferred.** Before a declared-reversible tool executes, the platform captures the
prior state of its target through a declared read; undo restores that state
through a declared write. Reversibility is deny-by-default: a tool with no
declaration is not reversible, exactly as a tool absent from the read-only
allowlist is assumed to mutate.

**3. The user acts on individual actions, through a receipt.** A turn that
changed anything ends with a card listing each action, each carrying either an
Undo control or the declared reason it cannot be undone. Whole-turn undo is a
later, thin layer over the same rows and is not in this design.

**4. The ledger is Postgres, beside `approvals`.** It is product state that gets
executed against, not telemetry. Langfuse is the observability and eval backbone
(ADR-0004) and is the wrong home for a row whose lifecycle a user drives.

## Rejected: a mapping DSL

The obvious alternative to snapshot-restore is to declare "tool X is undone by
calling tool Y with arguments derived from X's arguments." That needs an
expression language for the derivation, which is a small engine, which is the
category [ADR-0007](../adr/0007-adopt-not-build-boundaries.md) exists to keep out
of this codebase. It also fails in the case that matters most: the argument to
the inverse call is usually not derivable from the forward call at all. Undoing
`scale_deployment(replicas=10)` needs the replica count from **before** the call,
which no function of the forward arguments can produce. Snapshot-restore is not
merely the safer mechanism, it is the one that has the information.

## Architecture

```
turn                                                     ledger
----                                                     ------
model asks to call a mutating tool
   |
   v
can_use_tool / PreToolUse  ........ approval gate (exists, ADR-0010)
   |                       \
   |                        \..... snapshot hook (NEW)
   |                                  declared read -> prior state
   v
tool executes
   |
   v
SideEffectFlag(tool, detail)  ..... detail carries the recorded action (CHANGED)
   |
   v
worker kernel  .................... writes an agent_actions row (NEW)
   |
   v
receipt card on the channel ....... reuses ApprovalCardStore + blocks.py
   |
   v
[Undo] -> POST /actions/{id}/undo . platform replays the declared restore (NEW)
```

The turn path gains one hook and one field. Everything after the flag is new but
sits on machinery that already exists.

### Where the snapshot runs (revised after a spike)

The first draft put the snapshot in `can_use_tool`, on the reasoning that it runs
before the tool and cannot be skipped by the model. **A spike showed that seam
cannot reach a snapshot.** The runner never invokes an MCP tool outside the model
loop: MCP servers are handed to the SDK
([`runner/src/curie_runner/adapter.py`](../../runner/src/curie_runner/adapter.py)),
the SDK owns those client sessions, and every out-of-loop call the runner does
make ([`runner/src/curie_runner/state.py`](../../runner/src/curie_runner/state.py),
[`runner/src/curie_runner/memory.py`](../../runner/src/curie_runner/memory.py),
[`runner/src/curie_runner/history.py`](../../runner/src/curie_runner/history.py))
is plain HTTP to the platform API. Taking a snapshot there would mean building a
second MCP client inside the runner, which is a large new mechanism for the one
job.

The spike also found the snapshot is already being taken. The example's write
tool reads the resource before patching it and then discards what it read:

```python
# examples/sre-bot/connectors/k8s-write/server.py::restart_deployment
existing = client.get(path)
if existing.status_code == 404: ...
```

And tool results already reach the runner. They arrive as `UserMessage` content
and are dropped on purpose:

```python
# runner/src/curie_runner/translate.py, _translate_message
# UserMessage, SystemMessage, and StreamEvent carry no outbound-visible
# content in the v0.1 contract; they are intentionally dropped.
```

**So the snapshot is taken by the connector and travels in its tool result.** The
connector already holds the prior state; it returns prose today and returns prose
plus a structured prior-state block instead. The runner stops dropping tool
results for side-effecting tools and forwards what it finds. No new MCP client,
no new pre-execution hook, and the platform still never asks a model what
happened.

The cost of the revision is honest and matches the deny-by-default posture: a
connector that does not report prior state produces an action that is not
undoable. Third-party MCP servers will not report it, and that is exactly the
`undeclared` case the receipt already has to render.

### One flag per turn is not enough

`_translate_assistant` emits at most one `SideEffectFlag` per turn, guarded by
`state.side_effect_emitted`, because its only consumer is a boolean. A ledger
needs one record per action, so that cap is lifted and the flag becomes
per-call. This is the change with the widest blast radius in the runner and the
one to review hardest, because the existing no-retry rule reads the same signal.

### Why not in the connector alone

The snapshot must be taken by something that runs before the tool and cannot be
skipped by the model. `can_use_tool` is that thing. Putting the snapshot inside
the connector instead would mean every connector implements it, third-party MCP
servers never will, and a bundle author has no way to add it for a tool they do
not own.

### What a declaration looks like

Shape is illustrative, not a proposed grammar; the ADR settles it. It lives in
the bundle manifest beside `approvalPolicy`, because that is where the bundle
already declares per-tool policy that is validated at deploy time (ADR-0050), and
because the bundle is the versioned unit an operator reviews.

```yaml
reversibility:
  tools:
    - tool: mcp__k8s-write__scale_deployment
      snapshot: { via: mcp__kubernetes__get_deployment, keys: [namespace, name] }
      restore:  { via: mcp__k8s-write__scale_deployment, from: spec.replicas }
    - tool: mcp__k8s-write__restart_deployment
      irreversible: "restarting pods cannot be undone"
```

Two things carry weight here. `irreversible` is a **declared string the user
sees**, not a silence: today a bot restarts a Deployment and the platform says
nothing, and the improvement is as much about naming what cannot be undone as
undoing what can. And an undeclared tool is treated as irreversible with an
unattributed reason, which keeps the default safe while making the gap visible
rather than silent.

### Data model

`agent_actions`, mirroring `Approval`'s shape because the routing, lifecycle and
evidence requirements are the same:

| column | why |
| --- | --- |
| `id`, `agent_id`, `conversation_id`, `turn_id` | identity and the turn it belongs to |
| `tool`, `arguments` | what was called; arguments redacted through `runner/src/curie_runner/redact.py` |
| `target` | the declared key of the thing changed, so two actions on one resource can be reasoned about |
| `snapshot`, `snapshot_status` | prior state, or why there is none |
| `outcome`, `outcome_detail` | whether the call succeeded; a failed call still gets a row |
| `reversibility`, `irreversible_reason` | `reversible` / `irreversible` / `undeclared` plus the declared sentence |
| `undo_status`, `undone_at`, `undone_by` | lifecycle, mirroring `Approval.status` / `resolved_by` |
| `reply_kind`, `reply_channel`, `card_channel` | routing back to the surface, copied from `Approval` |
| `created_at` | ordering, and the basis for reverse-order replay later |

An `action_audit_entries` table mirrors `ApprovalAuditEntry` for the undo
decisions themselves, so an undo is as auditable as an approval.

### Undo execution

`POST /actions/{id}/undo` validates authorization, checks the world has not moved
(below), then executes the declared restore through the same connector path the
forward call used, and records the result. The model is not involved. The undo is
itself a mutating action and gets its own ledger row, linked to the row it
reverses, so an undo can be seen and audited but the chain terminates: an undo
row is never itself reversible.

## Failure modes, which are most of the design

- **The snapshot fails.** The action still executes and still gets a row, marked
  `snapshot_status: failed` and therefore not undoable. Refusing the action
  because a snapshot failed would turn a new feature into a new outage.
- **The world moved.** Someone scaled the Deployment by hand after the bot did.
  Restoring blindly would clobber them. The restore compares current state to the
  post-action state it expects and, on mismatch, refuses and says what changed.
  This is the single most important correctness rule here.
- **A partial undo.** Each action undoes independently; there is no transaction
  across actions. That is why the receipt is per-action rather than per-turn, and
  why whole-turn undo is deferred rather than assumed.
- **Undo of an undo.** Not supported. The forward action can be re-run as a new
  turn if that is what someone wants.
- **A stale declaration.** The bundle version that recorded the action is the one
  whose declaration is used to undo it, read from the immutable version, not from
  whatever is deployed now.
- **The connector is gone.** A deployment can be deleted between action and undo.
  The undo fails with a named reason rather than a traceback.

## Security

- **Authorization mirrors approvals.** Who may undo is resolved the way
  [ADR-0034](../adr/0034-approval-authorizers-resolve-membership-in-the-api.md)
  resolves who may approve: in the API, against membership, not from a
  caller-asserted identity.
- **Undo is a side effect.** Whether an undo of a gated tool needs its own
  approval is a real question and the ADR must answer it rather than assume.
  The default proposed here is that it does not, on the grounds that restoring a
  recorded prior state is strictly less dangerous than the action that was
  already approved. That is arguable and should be argued.
- **Snapshots can hold secrets.** A snapshot of a Kubernetes object can contain
  environment variables and secret references. Snapshots go through the existing
  redaction path (`runner/src/curie_runner/redact.py`) and are stored redacted,
  which means some resources are honestly not snapshot-able and must be declared
  irreversible instead of silently storing credentials.

## What proves it

- The `scale_deployment` round trip end to end on a real cluster: scale, receipt
  renders with one undoable and one irreversible row, undo, and the Deployment
  returns to its prior replica count, verified by reading the cluster and not by
  reading the ledger.
- A conflict case: change the Deployment by hand between action and undo, and the
  undo refuses with the mismatch named.
- A snapshot-failure case: the action still lands and is recorded as not
  undoable.
- An undeclared-tool case: the action appears on the receipt as irreversible with
  an unattributed reason.
- Mutation coverage on the "world moved" comparison specifically, since it is the
  rule whose absence would let this feature destroy someone's manual fix.

## Out of scope

- Whole-turn undo, which is a later thin layer over these rows.
- Time-windowed undo ("undo the last ten minutes"), which crosses conversations
  and other people's changes.
- Preview or dry-run before acting, which is the other half of the same intuition
  and is its own decision.
- Any change to what a sandbox may reach. This design records and reverses; it
  does not widen.

## Open questions, and how the ADR settled them

1. ~~Does undoing a gated tool require its own approval?~~ **An undo requires the
   authorization the forward action required, and no more.** The state being
   restored is one the cluster was already in, reached without anyone approving
   it, so nobody needs permission to put back what was there; what they need is
   to be someone who could have permitted the change.
2. ~~Does the declaration live in `plugin.json` or in `connectors.yaml`?~~
   **Neither. The tool's reply is the declaration.** A manifest surface would be
   a second source of truth, and the disagreement resolves the wrong way: the
   manifest claims reversible, nothing captured the state, and the platform
   either lies or ignores the manifest.
3. ~~Is `target` declared per tool or derived from the snapshot read?~~ **It comes
   from the tool's reply, with the same reasoning as 2**, and its absence is one
   of the three things that make a row not undoable.
4. ~~How long does a row stay undoable?~~ **It does not expire.** Time is the
   wrong bound: a change one minute later makes a restore unsafe and a quiet week
   does not make it safer. The world-moved check is the real bound.

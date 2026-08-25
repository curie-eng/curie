# 121. A restore is the connector's own verb, run under the same pinned connector

Date: 2026-08-25

Status: Draft

Answers the one decision
[ADR-0117](0117-a-tool-that-changes-the-world-reports-what-it-changed.md)
deliberately did not make. It changes no isolation boundary:
[ADR-0006](0006-security-rails-as-chart-defaults.md)'s rails and
[ADR-0008](0008-multi-tenancy.md)'s tenant compute stand as they are, and the
whole argument below is about not weakening them.

## Context

ADR-0117 shipped. Every side-effecting call is recorded with what it read, what
it left, and what it acted on; the API rules on an undo, refuses when the world
has moved, and refuses an actor who could not have permitted the forward change.
Then it returns, because **nothing in the platform can perform the restore it
just authorized.**

That gap is unchanged and re-verified for this ADR. Neither `apps/api` nor
`apps/worker` holds an MCP client — the `ClientSession` references in
[`apps/worker/src/curie_worker/reply_sink.py`](../../apps/worker/src/curie_worker/reply_sink.py)
and
[`apps/worker/src/curie_worker/runner_client.py`](../../apps/worker/src/curie_worker/runner_client.py)
are `aiohttp`, and every out-of-loop call either app makes is to the platform's
own API. MCP servers are handed to the SDK in
[`runner/src/curie_runner/adapter.py`](../../runner/src/curie_runner/adapter.py),
and the SDK owns those sessions.

ADR-0117 named three candidates and left them open. Building the ruling half
showed that they conflate two questions that have different answers, and that the
harder one was never really asked.

### The question ADR-0117's candidates skipped

All three candidates describe **who calls the connector**. None answers **what
call to make.**

A restore replays a recorded state: `{"spec": {"replicas": 3}}` onto a
Deployment. Turning that into `scale_deployment(replicas=3)` requires knowing
that this snapshot's `spec.replicas` is that tool's `replicas` argument. That
knowledge is per-tool and per-resource, and a platform that acquires it has built
the mapping DSL ADR-0117 rejects in its Alternatives — the one that "needs an
expression language, which is the commodity-engine category ADR-0007 exists to
keep out."

The connector already holds that knowledge. It produced the snapshot.

## Decision

**A restore is a verb the connector exposes, invoked by the runner in a sandbox
carrying the same pinned connector image and the same policy envelope as the
forward call.**

Not the original sandbox. Sandboxes do not survive their turn -- the reason
`WorkflowStateEntry` exists at all is that "sandboxes do not survive suspend" --
and an undo can be pressed days later. What has to be identical is the ceiling
and the code, not the container.

1. **The connector restores its own resource.** A write connector that reports a
   prior state also accepts one back: a `restore` tool taking the recorded
   `target` and `prior_state` exactly as the ledger holds them, returning the
   same reply shape as any other write. The platform hands back the connector's
   own words and never interprets them.

   This is the same argument as ADR-0117 decision 1, applied to the return leg.
   The reply was the declaration because a manifest could disagree with what the
   tool actually returned; the restore is the connector's verb because a
   platform-derived call could disagree with what the tool actually does.

2. **The runner invokes it in a sandbox under the connector's own binding.**
   That is where the connector's credential, allowlist and network policy already
   apply, so **an undo can reach nothing the forward call could not.** Any other
   caller has to re-derive that ceiling, and a ceiling re-derived in a second
   place is one that drifts -- the two write allowlists that disagreed for four
   days on a real install are the same failure, observed.

   The envelope, named so it can be checked rather than assumed: the connector's
   own credential, its allowlist, and the sandbox network policy that reaches it.
   Sameness is of those three and of decision 3's digest, not of a container that
   no longer exists.

3. **The ledger pins the connector that produced the snapshot, by resolved image
   digest.** A snapshot is only meaningful to code that understands its shape,
   and the shape is whatever that image produced. So the forward record carries
   the digest `connectors.lock.yaml` resolved for that connector, and a restore
   runs against a connector at **that** digest or does not run.

   No new schema-version field, and deliberately so. ADR-0113 already makes the
   resolved digest "the only identity rendered into a Deployment or used to start
   the local connector", so pinning it pins the snapshot's shape transitively. A
   separate snapshot-schema version would be a second thing to keep in agreement
   with the first, and the interesting failure -- a connector upgraded under a
   stored snapshot -- is exactly what a digest mismatch already names.

   **The compatibility contract is refusal, not migration.** If no connector at
   the pinned digest is deployable, the undo is refused with that as its stated
   reason. Not migrated, not best-effort replayed against a newer version: a
   snapshot reinterpreted by code that did not write it is the guess this whole
   design exists to avoid, and the receipt has to say so instead of trying.

4. **The executor confirms completion to the ledger, and the ruling stops
   claiming it.** Today `undone_at` is set when the undo is authorized, because
   nothing reports back — a restore that never runs leaves a record saying it
   did. The ruling becomes a claim with an outcome: authorized, then confirmed or
   failed, with the failure on the record and on the receipt.

5. **Capability is declared statically; reversibility of a given action stays
   runtime.** Whether a particular call returned a prior state is a runtime fact
   and cannot be validated ahead of time. Whether the connector has a restore
   verb at all is a property of its advertised tool list, which can.

   So the checkable rule is about capability, and it is deliberately broader than
   the runtime question: **a connector advertising any tool that is not read-only
   must also advertise `restore`, or it advertises no restore at all and is
   treated as restoring nothing.** Both halves are inspectable before deploy --
   `readOnlyHint` is already how this repository's own gate finds write tools --
   and neither is a claim about a specific action.

   This is not the manifest ADR-0117 rejected. That rejection was of a
   declaration that an action *is reversible*, because it could disagree with
   what the tool actually returned. A tool list says which verbs exist, which MCP
   already publishes and which cannot disagree with itself.

   The two halves fail closed together: **a connector with no advertised restore
   never produces an undoable record, and any `prior` it reports is recorded as
   history rather than as something to act on.** A connector cannot make its
   actions undoable by reporting a snapshot it has no verb to replay.

## Prerequisite: the snapshot's exposure has to be settled first

**This decision cannot be implemented before the redaction question in
[#1873](https://github.com/curie-eng/curie/issues/1873) is answered, and that is
a prerequisite rather than a consequence.**

What the return leg does and does not change is worth stating precisely, because
the honest answer is narrower than "it widens exposure" and still blocking.

It adds no new party. The snapshot goes back to the same connector that produced
it, through the same runner, into a sandbox under the same binding. Every one of
those already saw the value on the way out.

What it does add is **duration and repetition**. On the forward leg the snapshot
existed for the length of one reply. Under this decision it is stored in the
control plane indefinitely (ADR-0117 decided a record does not expire) and then
transmitted again, arbitrarily later, to a process that did not exist when it was
captured.

And the guarantee that would make that acceptable cannot currently be stated.
`runner/src/curie_runner/redact.py` scopes itself to the runner's stdout and its
span attributes, and says in its own words that the ACI frames are "a larger
surface left deliberately untouched here". A snapshot travels on those frames. So
there is no redaction pass on the path this decision depends on, in either
direction.

ADR-0117 left the resolution to the connector author: a resource that cannot be
snapshotted safely "must report itself irreversible rather than storing
credentials in the control plane". That is a real answer for the forward leg and
it is not sufficient here, because it asks every author to reason correctly about
a control plane they cannot see. Whatever #1873 decides -- scrub the frames, or
state that the ledger is a credential-bearing store and secure it as one -- the
executor should not ship before it.

## Alternatives considered

- **An MCP client in the worker** (ADR-0117's first candidate). The smallest
  change, and the reason to reject it is decision 2. The worker is the control
  plane; giving it a direct client of tenant connectors makes the platform
  reachable to places ADR-0008 keeps separate, and the undo would then run under
  the control plane's network reachability rather than the sandbox's. An undo
  that can reach something the forward call could not is a privilege escalation
  wearing a safety feature's clothes.

- **A plain HTTP replay endpoint on each connector** (ADR-0117's third
  candidate). Avoids MCP, and pays for it twice: every connector author
  implements a second transport and its authentication, and the platform grows a
  second way to call a connector that the first way's allowlists do not cover.
  Decision 1 gets the same property — the connector deciding how to restore
  itself — without a new transport.

- **Deriving the restore call from the recorded arguments.** Rejected in
  ADR-0117 and rejected again here for a sharper reason: it is not merely
  unsafe, it is unavailable. `scale_deployment(replicas=10)` contains nothing
  that produces 3.

- **Letting the agent perform the restore as another turn.** Rejected in
  ADR-0117 decision 2 and unchanged: the undo would itself be a side-effecting
  turn needing its own record and its own approval, which regresses infinitely.

- **Executing nothing, and rendering the ruling as a suggested command.** Honest,
  and it is what the platform effectively does today. Rejected because it moves
  the one deterministic step in this design back onto a human at a keyboard,
  which is where the errors an undo exists to correct come from.

## Consequences

- **The runner must invoke an MCP tool outside the model loop, which it cannot do
  today.** This is the largest cost and it was ADR-0117's stated reason for
  deferring: the SDK owns the client sessions, so a second client inside the
  runner is new mechanism. It is a smaller mechanism than it looked, because it
  is needed for exactly one verb with a fixed argument shape and no model in the
  path.

- **A write connector that wants to be undoable now has two verbs to write, not
  one.** ADR-0117 already charged authors for returning JSON; this adds the
  return leg. The honest mitigation is that both are the same tool's read and
  write halves, and a connector that cannot restore can still report prose and be
  honestly irreversible.

- **A restore is a sandbox turn without a model**, which is a shape the platform
  does not have. Whether it reuses the existing sandbox lifecycle or gets a
  narrower one is an implementation question this ADR does not settle.

- **A pinned digest that is no longer deployable makes an undo permanently
  refused.** An operator who upgrades a connector loses the ability to undo
  actions recorded under the previous image, and the receipt will say so. That is
  the intended trade against replaying a snapshot through code that did not write
  it, and it is a cost worth naming: undoability now has a shelf life set by
  deploys rather than by time, which is not what ADR-0117's "a record does not
  expire" leads a reader to expect.

- **The broader capability rule refuses some honest connectors.** A write
  connector that genuinely cannot restore anything must advertise no restore verb
  and accept that its actions are never undoable -- which is correct -- but the
  rule as stated gives it no way to say "this one tool is reversible and that one
  is not" beyond splitting them into two servers. That is what `k8s-write` and
  `k8s-scale` already do, so the cost is real but the pattern is established.

- **This lands after #1873, not beside it.** See the prerequisite above: the
  return leg adds duration and repetition rather than a new party, and the
  guarantee that would make that acceptable cannot be stated while no redaction
  pass covers the ACI frames at all.

- **Whole-turn undo stays out of scope**, as in ADR-0117. Per-action remains the
  honest unit while a restore can fail independently per action.

## Out of scope

- **Retention and pruning of the ledger.** Unchanged from ADR-0117.
- **Undo from the console.** The receipt lands on the channel that asked.
- **Restoring anything the forward call did not report**, including side effects
  a connector performed but did not name. The ledger's account is the ceiling on
  what can be put back, and widening it is a new decision.

# 211. A factory run that loses its sandbox is re-admitted like one that loses its worker

Date: 2026-10-08

Status: Draft

This draft proposes amending
[ADR 0013](0013-concurrency-and-delivery-model.md) for one case only: a
factory WorkItem execution whose runner sandbox is confirmed gone. It
proposes extending
[ADR 0206](0206-a-factory-run-lost-with-its-worker-is-re-admitted-as-a-new-attempt.md)
to that case, and amending ADR 0206 point 2 with the replay safety bound in
point 6 below, so that both successor paths share one bound. It builds on
[ADR 0162](0162-work-items-own-durable-execution-identity.md) and
[ADR 0117](0117-a-tool-that-changes-the-world-reports-what-it-changed.md).
It does not authorize implementation.

## Context

ADR 0013 says a failed turn that flagged a side effect escalates to a person
instead of retrying. The runner's classifier,
`SideEffectClassifier` in
[`runner/src/curie_runner/side_effects.py`](../../runner/src/curie_runner/side_effects.py),
is deny by default: every tool outside a short read only list flags the turn.
`Bash`, `Edit`, `Write`, `Skill` and `Agent` all flag. A factory run calls one
of them within its first few seconds.

In `_process_event` in
[`apps/worker/src/curie_worker/kernel.py`](../../apps/worker/src/curie_worker/kernel.py),
the `saw_side_effect` escalation runs before the retry check. The retry check
already lists `sandbox-terminated` in `RETRYABLE_CLASSIFICATIONS`, but a factory
turn never reaches it after its opening seconds. So any loss of a factory
run's sandbox (a node drain, an eviction, a pod deleted by an operator or an
upgrade) ends the WorkItem as needs human.

ADR 0206 already decided the opposite for the same work. When the worker is
lost, the request ends `failed/owner_lost` and a successor request reruns the
whole run in a new sandbox, including every `Bash` and `Edit` the lost run
made. The same physical event, a factory run losing the process that ran its
turn, now has two outcomes depending on which component died.

A factory resilience run on a disposable install, driven through a dedicated
test GitHub App and fixture repository, deleted the sandbox pod of a running
factory request on 2026-10-08. The runner stream dropped, the turn had flagged
a side effect, and the request ended `runner_escalated` with the issue marked
as needing a person. A worker deletion in the same round ended in a successor
request that opened a green pull request.

That run also showed a detection gap. `pod_termination` in
[`apps/worker/src/curie_worker/sandbox/k8s.py`](../../apps/worker/src/curie_worker/sandbox/k8s.py)
recognizes eviction and out of memory kills but returns nothing for a deleted
pod, so the loss was classified `runner-error` instead of `sandbox-terminated`.
Fixing that classification is a separate bug fix that needs no ADR. This ADR
depends on it: point 2 needs a confirmed loss.

## Decision

**A factory execution request whose sandbox is confirmed gone mid turn ends
`failed/sandbox_lost` and is followed by a successor request, under the same
rules and the same consecutive loss cap as `owner_lost`, provided the lost
run stayed inside the replay safety bound in point 6.**

1. Scope. Only a turn executing a factory WorkItem's `ExecutionRequest` is
   covered. Interactive, cron, eval and other non factory turns keep ADR 0013
   unchanged. A turn with no flagged side effect keeps today's in place retry
   for `sandbox-terminated`.
2. Sandbox lost means both of these hold: the turn's stream ended without a
   terminal frame, and the substrate confirms the sandbox gone (pod not
   found, deleted, evicted, or killed) when the worker reads its termination
   state. A stream drop with the sandbox still present, or with no evidence
   either way, is not sandbox loss and ADR 0013 applies. The Docker substrate
   reports no termination evidence, so local runs keep ADR 0013.
3. On a sandbox loss after a flagged side effect, the worker neither retries
   the turn in place nor escalates. It finishes the request
   `failed/sandbox_lost`. The worker's normal terminal path releases the
   claim, so the sandbox is gone before anything new starts, as ADR 0206
   point 1 requires.
4. The API admits the successor in the transaction that settles
   `failed/sandbox_lost`, exactly as ADR 0206 points 2, 3 and 5 define for
   `owner_lost`: sequence plus one, waiting, a fresh waiting deadline, the
   same retry marker, starting from the beginning of the work.
5. The existing refusals apply unchanged: a cancelled WorkItem, a pending
   relabel, a closed pull request lineage, and a publication that owns the
   terminus (this request's own publication in any status, or one in flight
   on the lineage or conversation) admit nothing. A CI fix turn after a
   successful publication is therefore never re-admitted.
6. Replay safety bound. A successor reruns every tool call of the lost run,
   so the API admits one, for `sandbox_lost` and for `owner_lost` alike, only
   when both checks pass:
   1. Static: the agent's deployed bundle mounts no connector MCP server, and
      the install grants the agent no per agent connector egress
      (`agentSandbox.connectorEgress`).
   2. Ledger: every action ledger row recorded for the lost request's events
      names a harness built in tool or a platform tool on a fixed list
      (`mcp__curie__get_issue`, `mcp__curie__progress`,
      `mcp__curie__report_progress`, `mcp__curie__request_approval`,
      `mcp__curie__publish_changes`), and no row carries a gate approval.
      Any other tool name, including one the list does not know, refuses. This
      is deny by default, as the side effect classifier is.

   If either check fails, no successor is admitted and the WorkItem ends for
   a person as today, with a status comment naming the connector or tool that
   blocked the retry.
7. Count. The consecutive loss streak counts `owner_lost` and `sandbox_lost`
   terminals together, newest first. At 3 no successor is admitted. The status
   comment names how many of each occurred. Reason for one shared cap: a run
   that destroys its own sandbox and a run that kills its worker are the same
   failure to the issue reader, and two separate caps would allow up to five
   attempts.

### Why `Bash` is inside the bound

`Bash` is not sandbox local by nature. It reaches whatever the sandbox network
policy allows. Per agent, that is: DNS; the fleet wide allow list
(`security.networkPolicy.allowedEgress`, which holds the model API plus any
operator added web egress); per agent registry egress; per agent connector
egress; and, when configured, the object store and telemetry endpoints. The
static check removes connector egress. The dark factory contract adds that the
sandbox holds no GitHub, push or publication credential: the issue is read
through an execution scoped platform tool, and publication goes through
`publish_changes`, which the API fences by request and runtime epoch.

What a replayed `Bash` command can still repeat is therefore: model spend,
which ADR 0206 already accepts; package registry reads, which the operator's
registry proxy restricts to `GET` and `HEAD`; writes to platform state scoped
to the WorkItem's own conversation, such as its channel memory; and
unauthenticated requests to hosts on the fleet wide allow list. The last item
is the residual risk. This ADR accepts it, names it, and notes that ADR 0206
already accepts the same risk on worker loss without saying so.

The ledger check alone is not enough. The runner emits a side effect frame
when a call is made and the worker records it on receipt, so a call in flight
when the stream dropped may never reach the ledger, and `Bash` rows carry no
target. The static check covers that gap because it bounds what any call
could have reached, recorded or not. The ledger check covers drift between the
declared surface and what ran.

### Realizing paths

When this ADR is accepted, the realizing paths are expected to be the
`saw_side_effect` branch of `_process_event` and `_ESCALATION_CAUSES` in
`apps/worker/src/curie_worker/kernel.py`; `pod_termination` in
`apps/worker/src/curie_worker/sandbox/k8s.py`; `_terminalize_execution`,
`_admit_owner_lost_successor` and `owner_lost_streak` in
`apps/api/src/curie_api/workitems.py`; and the factory status comment that
renders the loss counts and the blocking tool.

## Consequences

1. A sandbox deletion, eviction or node drain during a factory run costs the
   elapsed run time and its model spend, not the run. The issue reader is
   involved only after the third consecutive loss of either kind.
2. Factory runs and ADR 0013 now differ on purpose: inside the bound, a
   factory request's unit of retry is the whole request, with its own
   deadline, refusals and cap. Outside it, ADR 0013 holds.
3. Point 6 tightens ADR 0206. A factory agent that mounts a connector, or
   whose lost run called a tool outside the fixed list, no longer gets an
   automatic successor after a worker loss. The stock dark factory mounts no
   connectors, so it is unaffected. Another factory agent that relies on ADR
   0206 retries while mounting a write connector loses them, deliberately.
4. Unauthenticated requests to fleet wide allow listed hosts may repeat on a
   successor. An operator who adds web egress for a factory agent accepts that.
5. Local Docker runs gain nothing, because that substrate cannot confirm a
   loss.
6. A new terminal cause, `sandbox_lost`, appears on requests and in status
   comments. Consumers that read only `owner_lost` as the retry signal must
   read the retry marker instead.

## Alternatives considered

1. Keep ADR 0013 for factory sandbox loss. Rejected: every sandbox loss after
   the first few seconds needs a person, while the same work survives a worker
   loss under ADR 0206. The inconsistency has no safety argument behind it,
   since ADR 0206 already replays the same calls.
2. Retry the turn in place in the same request with a new sandbox. Rejected:
   the conversation would describe workspace edits the new sandbox does not
   have, and it would get none of the request boundary's refusals or cap. The
   replay risk is the same as a successor's.
3. Narrow the side effect classifier so `Bash`, `Edit` and `Write` count as
   idempotent. Rejected: the classifier serves every agent, and for an
   interactive agent with connectors or egress `Bash` is not idempotent. It
   would weaken ADR 0013 everywhere to fix one lane.
4. Extend ADR 0206 to sandbox loss with no replay bound. Rejected: it keeps
   the boundary implicit, and a factory agent with a write connector would
   double act on every sandbox loss and every worker loss.
5. Bound the replay by the action ledger alone. Rejected: an in flight call
   may be missing from the ledger, and a `Bash` row carries no target, so the
   ledger cannot show what a command reached.
6. Separate caps for worker loss and sandbox loss. Rejected: up to five
   attempts for one issue, and the reader cannot tell why the count differs.
7. Reattach to the running turn or resume from a checkpoint. Rejected for the
   same reason ADR 0206 rejected it: it needs a runner that outlives its
   stream, which deserves its own ADR.

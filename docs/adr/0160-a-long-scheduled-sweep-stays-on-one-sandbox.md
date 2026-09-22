# 160. A long scheduled sweep stays on one sandbox

Date: 2026-09-21

Revised: 2026-09-22, before merge. The first draft split the sweep across
hook turns that each take a sandbox. That shape is rejected. The sweep
keeps the one sandbox it already has.

Status: Accepted

Maintainer acceptance for this record is its merge onto the feature train.
The decision is published here so implementation can follow it. No
implementation lands with this record. Issue
[#2878](https://github.com/curie-eng/curie/issues/2878) tracks the realizing
work and stays open until a cluster install shows the reference sweep
finishing on one sandbox, and shows a sweep cut short naming what it did
not cover.

This is the decision [ADR-0099](0099-hooks-are-bundle-declared-turns-the-system-starts.md)
deferred when it set hook firing semantics and left sweep budgets and
wall-clock caps on a maintenance pass to a later ADR. It also answers the
question [ADR-0131](0131-a-delivery-has-one-deadline-and-one-renewable-fenced-owner.md)
left open: a per-turn execution budget would widen the API, database, UI, and
deployment contracts, and needed its own decision. The answer here is that a
turn does not get its own budget.

It does not reopen [ADR-0079](0079-inbound-triggers-as-a-new-event-kind.md)
or ADR-0099. The wire shape, the placeholder-less post, the idle rule, the
standing prompt as authorization, skip on overlap, bounded catch-up, and the
run record outcomes stay as those records decided them. A sweep that still
has work left is not the failed-run hard stop ADR-0099 already decided.
That case is below.

## Context

One delivery has one deadline. `delivery_budget_s` in
`apps/worker/src/curie_worker/config.py` is that deadline: default 600
seconds, operator maximum 1800 seconds. `runner_total_timeout_s` is a
per-request ceiling inside the same budget, with the same default and the
same maximum, and it must not exceed the budget. ADR-0131 fixes the rest of
the coupling. Attempts consume the remaining budget, reclaim does not start
a fresh one, and voluntary termination grace must be at least the budget
plus the 60 second shutdown reserve. An operator who sets the 1800 second
maximum therefore needs at least 1860 seconds of grace. The lease heartbeat
keeps a healthy turn from being stolen. It does not extend the deadline.

A program manager sweep does not fit in that deadline. The reference
workload is a weekday 05:00 pass that reads Slack channels, GitHub across
repositories, and meeting notes, then posts one daily plan. Done by hand,
with about ten parallel readers, it takes about 90 minutes. That is 5400
seconds: nine times the default deadline and three times the operator
maximum.

ADR-0099 already classifies this work as a hook: a bundle-declared
ordinary turn, started by a schedule, with a standing prompt as its only
authorization. A later fire of the same hook is skipped while an earlier
fire is still in flight. A failed or refused fire is not retried inside
the fire. None of that says how a sweep longer than one delivery still
finishes, or how a sweep that stops early tells a person what it did not
cover.

The sandbox is not the delivery. [ADR-0003](0003-stateless-first-rehydrate-on-resume.md)
says a resumed thread rehydrates from history, and prompt-cache warmth
exists only while the same claim stays up. `suspend` in
`apps/worker/src/curie_worker/sandbox/substrate.py` deletes the pod. A
slice that waits for a later cron, or that runs as a second hook, finds
that pod gone and starts another sandbox. The sweep then loses the tree
and the warm session it already had.

Three ways to cross the deadline were weighed.

1. Staggered short triggers. The author declares several ordinary hook
   turns, each expected to finish inside the delivery budget, and each
   hands the next turn a checkpoint through agent memory. Each of those
   turns is free to take its own sandbox.
2. A per-trigger turn budget override. The declaration names a longer
   wall clock for that hook, and one fire runs the whole sweep.
3. Subagent fan-out inside one turn. The fire spawns parallel readers and
   synthesizes their results before the parent turn ends.

## Decision

**A sweep that cannot finish inside one delivery stays on the one sandbox
that first fire already claimed. Later slices are new deliveries on that
same claim. The claim is not suspended between them, and a second sandbox
is not started while it is alive. Each delivery stays inside the platform
budget. The budget does not move.**

### The ceiling stays operator-wide

`delivery_budget_s` remains the wall clock of one delivery. A hook
declaration cannot raise it, and neither can a single turn. Option 2 is
rejected. The reference sweep needs about 5400 seconds, which is past the
1800 second maximum already shipped, so an override cannot express the
workload without lifting ADR-0131's deadline, the grace that is derived
from it, and the upgrade drain that waits out live deliveries. Those are
one contract, shared by every turn on the worker. A bundle does not get a
private copy.

Raising the platform default from 600 seconds to something that fits 90
minutes is the same rejection. Every interactive turn would inherit a
ceiling that exists for one maintenance shape, and a wedged turn would
hold a delivery for that long.

Splitting one delivery into several runner requests that each reset a 600
second clock, without consuming one budget, is also rejected. ADR-0131
already decided that attempts consume the remaining time and that reclaim
never starts a fresh budget. A later slice is a new delivery with its own
budget. It is not a reset of the delivery that just ended.

### One hook, one claim

The sweep is one ADR-0099 hook. The cron fire starts it and claims one
sandbox for that hook's thread. When a delivery ends because the budget
ran out and the sweep still has sources uncovered, the platform enqueues
the next delivery on that same thread and keeps the claim. The agent does
not enqueue it, and it does not spawn a sandbox. The standing prompt is
the same prompt. The live session is the handoff.

The claim stays up across the gap. `suspend` does not run between slices
of an open sweep. A second claim is not bound for this hook while the
first is alive. Issue #2878 has to show the second delivery running on
the same claim the first delivery used, and show that a live sweep does
not gain a second sandbox.

ADR-0099 skip still applies to a second fire that arrives while a
delivery of this hook is in flight. The continuation starts only after
that delivery has ended. This ADR does not add buffering, and it does
not add a second hook to get around the skip.

A refused or failed turn is still ADR-0099's hard stop. The platform
does not continue it. A delivery that ends only because the budget ran
out, with uncovered sources still listed, is the slice boundary this ADR
adds. ADR-0099 did not decide that case. It left sweep budgets to this
record.

Option 1 as a chain of sandboxes is rejected. A later cron slot, a
second hook name, or a partition would each be another thread and
another sandbox. The sweep has one conclusion, so it stays on one
thread. [ADR-0134](0134-a-hook-shares-one-thread-per-partition.md)
partitions and [ADR-0115](0115-agents-call-each-other-with-no-third-party.md)
delegation are not how it crosses the ceiling. Readers run inside the
one sandbox. They are sequential across deliveries. In-process tool
calls inside one delivery do not create a sandbox, and they do not
extend that delivery's budget. Option 3 is rejected for the same
reason as before: children of one turn share that turn's deadline, the
reference sweep is already parallel at about 90 minutes, and a
model-chosen child is the recursive spawn ADR-0099 refused.

If the claim is already gone, the sweep stops. A suspend, a node
failure, or a deploy that drains the claim ends it. The platform does
not mint a replacement sandbox to finish the remaining sources. The
partial report below is the result. Ordinary thread resume, as ADR-0003
decided it, is unchanged for threads that are not in an open sweep.
This record does not make a dead sweep claim immortal, and it does not
make a new pod the way the sweep continues.

### The checkpoint records coverage

The live session is the working state. The checkpoint is the coverage
record, so a budget cut or a dead claim can say what was not finished.
It is not a boot strap for a new sandbox.

Each slice appends one record to the agent memory log as soon as it
finishes a source, not only at the end of the delivery. A slice that
waits until the last moment and is then cancelled has not covered the
source it was still writing. The log is the existing store: namespace
`memory`, key `log`, written by the runner memory port in
`runner/src/curie_runner/memory.py` and read back through
`apps/api/src/curie_api/routers/memory.py`. The record restates the
whole sweep, not a delta and not a lesson:

```
sweep-checkpoint
sweep: weekday-plan
date: 2026-09-22
hook: weekday-plan
covered: slack
uncovered: github, notes, plan
```

`date` is the local date of the sweep's first slot, in the schedule zone
ADR-0099 already requires. `covered` lists only sources this sweep has
finished and appended. A source still in progress when the delivery
budget cancels the turn stays uncovered. `uncovered` restates every
remaining source.

Today's consolidator, `consolidate_records`, collapses records whose
content matches and keeps provenance. A checkpoint whose covered list
changed is different content, so that pass does not drop it.
[ADR-0111](0111-the-default-memory-compaction-algorithm.md) is still a
draft. This ADR does not accept it. A future summarizer must keep the
newest parseable checkpoint for an open sweep date intact. Ordinary
memory entries stay under whatever that draft decides.

The checkpoint is data. ADR-0099 already decided that content a turn
fetches is not fresh consent. The standing prompt remains the only
authorization.

No new manifest field is required. The author names the sources in the
standing prompt, and the checkpoint carries them in the labels above.
`TriggerDeclaration` in
`packages/plugin-format/src/plugin_format/models.py` stays as it is.
`packages/aci-protocol` stays as it is.

### A partial sweep names what it did not cover

Coverage for a sweep date is the newest parseable checkpoint.

When a delivery on the open claim posts the plan, that post is the
human-visible report. ADR-0079 already decided that a placeholder-less
hook post goes to the hook's target. The post has two parts: the plan,
built only from covered sources, and a section that lists every
uncovered source. An empty uncovered section means the sweep finished.
The slice does not describe an uncovered source as if it had been read.
After that post, the claim may suspend. The sweep is closed.

When the sweep stops without that post, the channel still hears what
was missed, as long as the hook declares a target. The stop is a
refused or failed turn, or a claim that is already gone while uncovered
sources remain. The scheduler ADR-0099 places in the worker posts one
coverage notice to the target. The notice quotes the newest
checkpoint's uncovered list and names the run record outcome (`failed`,
`skipped`, or `blocked`, the members ADR-0099 already defined). It does
not contain a plan. A budget cut that continues on the same claim does
not post the notice. The sweep is not over.

If no parseable checkpoint exists, the notice says that nothing was
recorded as covered. It does not invent sources.

A hook with no target stays silent in the channel, which is ADR-0099's
silence rule. The memory log is queryable today through the memory API.
The run record is ADR-0099's surface, queryable once that record
exists. The notice does not add a store, a retry, or an outcome member.

## Alternatives considered

- **A new sandbox per slice.** The first draft of this ADR. Rejected.
  The next slice would lose the tree and the warm session, and the
  platform would start sandboxes while a perfectly good one was already
  held. Staggered crons and extra hook names are the same rejection.
- **Per-trigger turn budget override.** Rejected above. It cannot express
  a 5400 second sweep inside the 1800 second maximum, and a maximum high
  enough for the sweep would reopen ADR-0131 for every turn that shares
  the worker.
- **Subagent fan-out inside one turn.** Rejected above. The parent
  deadline still applies, the reference workload is already parallel at
  about 90 minutes, and model-spawned children break ADR-0099's rule
  that the declared prompt is the only authorization and that a refusal
  is not a spawn trigger.
- **Platform default raised above 600 seconds.** Rejected with the
  override. The default is the interactive ceiling. The sweep stays
  inside it by using more than one delivery on one claim.
- **One delivery, many runner calls, budget reset per call.** Rejected.
  It contradicts ADR-0131's single deadline.
- **Partition the sweep (ADR-0134) or delegate it (ADR-0115).** Not this
  decision. Partitions are for independent items in one delivery.
  Delegation cannot be reached from a sandbox today and is still a
  draft. Both would add sandboxes. This sweep uses one.

## Consequences

- An author who writes one cron prompt and expects a single delivery to
  run for 90 minutes will be cut off at the delivery budget. The same
  sandbox continues if uncovered sources remain. The uncovered list is
  the result when the sweep actually stops.
- The claim is held for the whole open sweep, so a long sweep pins one
  sandbox across many deliveries. It does not pin one delivery past the
  budget. A hung delivery still ends at the budget. A dead claim is not
  replaced.
- Each finished source appends one checkpoint. The memory log grows by
  that much. Exact-content consolidation does not merge distinct
  checkpoints. The next delivery does not need that record to resume,
  because it is the same session. The record is what the partial notice
  reads.
- The coverage notice is new scheduler behavior. It does not exist until
  issue #2878 lands. This record authorizes it and does not claim a
  current code path performs it. The path to build is the worker's hook
  thread binding: keep the claim, do not call `suspend` between slices,
  route the next delivery to that claim, and post the notice through the
  placeholder-less target path ADR-0079 assigned to the kernel when the
  sweep stops short. If the run record itself is not built yet, the
  notice lands with that record, not before it.
- Issue #2878 is the realizing work. Its positive case is a sweep of the
  reference shape that finishes across more than one delivery on one
  claim and posts a plan whose uncovered section is empty. Its negative
  case is a sweep that stops with the claim gone or the turn refused,
  after which the target receives a notice naming the uncovered sources
  and containing no plan, and no second sandbox is created to continue.
  A second negative case is a slice cancelled at the delivery budget: the
  claim stays up, the next delivery is on that claim, and a source that
  was not appended stays uncovered.
- Cost moves from one long delivery to several ordinary ones on one
  sandbox. The author pays with the same keys ADR-0099 already named.
  This ADR adds no frequency floor.
- Observability of a partial sweep is the checkpoint in the memory log
  plus the post or the notice, with the run record outcome beside them.
  No new outcome and no new dashboard.

## What this does not change

- ADR-0079's event kind, ingress, and placeholder-less post.
- ADR-0099's declaration, authorization, overlap skip, catch-up bound,
  idle deferral, failed-run hard stop, and run record outcomes.
- ADR-0131's single delivery deadline, lease, and grace coupling.
- ADR-0003's cold resume for a thread whose claim has actually ended.
- The frozen ACI session contract and the current `TriggerDeclaration`
  shape.

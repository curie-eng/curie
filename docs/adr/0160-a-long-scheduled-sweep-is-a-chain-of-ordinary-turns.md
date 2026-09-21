# 160. A long scheduled sweep is a chain of ordinary turns

Date: 2026-09-21

Status: Accepted

Maintainer acceptance for this record is its merge onto the feature train.
The decision is published here so implementation can follow it. No
implementation lands with this record. Issue
[#2878](https://github.com/curie-eng/curie/issues/2878) tracks the realizing
work and stays open until a cluster install shows the reference sweep
finishing, and shows a sweep cut short naming what it did not cover.

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
run record outcomes stay as those records decided them.

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
fire is still in flight. A failed fire is not retried inside the fire. None
of that says how a sweep longer than one delivery still finishes, or how a
sweep that stops early tells a person what it did not cover.

Three ways to cross the deadline were weighed.

1. Staggered short triggers. The author declares several ordinary hook
   turns, each expected to finish inside the delivery budget, and each
   hands the next turn a checkpoint through agent memory.
2. A per-trigger turn budget override. The declaration names a longer
   wall clock for that hook, and one fire runs the whole sweep.
3. Subagent fan-out inside one turn. The fire spawns parallel readers and
   synthesizes their results before the parent turn ends.

## Decision

**A sweep that cannot finish inside the delivery budget is a declared chain
of ordinary hook turns. Each turn stays inside the platform budget. The
turns hand off through a structured checkpoint in the agent memory log. The
budget does not move, and a turn does not spawn subagents to outrun it.**

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
never starts a fresh budget.

### The chain is declared

The author breaks the sweep into ADR-0099 hooks whose standing prompts
each fit in the configured delivery budget. A slice that depends on an
earlier slice is scheduled at least one configured budget later, so a
slice that uses the whole budget has exited before the next slot. If it
has not, ADR-0099 skip applies and that slot does not queue. This ADR
does not add buffering.

Independent slices may share a start time. Overlap skip is per agent and
hook, not per sweep, which ADR-0099 already decided. Reader hooks
scheduled together are ordinary turns, each with its own budget. The
slice that posts the plan is scheduled after the readers it joins. That
lets the reference sweep run its readers at the same time without making
them subagents of one turn.

The agent does not enqueue the next slice. The schedules are the chain.
The agent does not spawn subagents inside the turn. Option 3 is rejected.
Children of one turn share that turn's delivery budget, so the parent
still ends at the ceiling. The reference sweep is already about ten
readers in parallel and still takes about 90 minutes, so more parallelism
inside the same turn does not bring it under 600 seconds. A model-chosen
child is also the recursive spawn ADR-0099 refused. The declared prompt
would stop being the only instruction the turn starts with, and a refused
tool could become a reason to spawn.

[ADR-0134](0134-a-hook-shares-one-thread-per-partition.md) and
[ADR-0115](0115-agents-call-each-other-with-no-third-party.md) are not
this mechanism. A partition splits one delivery into independent threads
keyed by a field the operator named. This sweep has one conclusion.
Agent-to-agent delegation is a separate draft, and a sandbox still holds
no credential that can mint another agent's turn. Neither record is
reopened here, and neither is how a sweep crosses the ceiling.

### Handoff is a memory checkpoint

Each slice appends one record to the agent memory log as soon as it
finishes a source, not only at the end of the turn. A slice that waits
until the last moment and is then cancelled has not covered the source
it was still writing. The log is the existing store: namespace `memory`,
key `log`, written by the runner memory port in
`runner/src/curie_runner/memory.py` and read back through
`apps/api/src/curie_api/routers/memory.py`. The record is one checkpoint
for the whole sweep so far, not a delta and not a lesson. Its content
uses these labels, one per line:

```
sweep-checkpoint
sweep: weekday-plan
date: 2026-09-21
hook: read-slack
covered: slack
uncovered: github, notes, plan
```

`date` is the local date of the sweep's first slot, in the schedule zone
ADR-0099 already requires. `covered` lists only sources this chain has
finished and appended. A source still in progress when the delivery
budget cancels the turn stays uncovered. `uncovered` restates every
remaining source, including sources whose slice was skipped, failed, or
never started. The next slice trusts the newest parseable checkpoint for
that sweep name and date, and ignores an older date instead of
continuing yesterday's plan.

The next slice must see that record at its own start. The runner already
loads the log into the system prompt at boot through
`format_memory_preamble`. A warm sandbox that keeps the preamble from an
earlier boot does not satisfy the handoff. The realizing work reloads
the log at the start of each sweep fire, including when the hook thread's
sandbox is still warm. Issue #2878 has to show a second fire observing
the first fire's checkpoint.

Today's consolidator, `consolidate_records`, collapses records whose
content matches and keeps provenance. A checkpoint whose covered list
changed is different content, so that pass does not drop it.
[ADR-0111](0111-the-default-memory-compaction-algorithm.md) is still a
draft and is the place a future summarizer would be decided. This ADR
does not accept it. It adds one constraint that summarizer must keep if
it is accepted later: the newest parseable checkpoint for an open sweep
date is load-bearing and is not rewritten into prose. Ordinary memory
entries stay under whatever that draft decides.

The checkpoint is data. ADR-0099 already decided that content a turn
fetches is not fresh consent. The standing prompt of each slice remains
the only authorization.

No new manifest field is required for the handoff. The author names the
sweep, the sources, and which hook posts the plan inside the standing
prompts, and the checkpoint carries the same names in the labels above.
`TriggerDeclaration` in
`packages/plugin-format/src/plugin_format/models.py` stays as it is.
`packages/aci-protocol` stays as it is. A later change that wants a
machine-checked sweep group on the declaration is a frozen contract
change and has to land as its own reviewed change before any runtime
reads it. This record does not make that change.

### A partial sweep names what it did not cover

Coverage for a sweep date is the newest parseable checkpoint. There is
nothing else to consult.

When the reporting slice runs to a successful exit, its post is the
human-visible report. ADR-0079 already decided that a placeholder-less
hook post goes to the hook's target. The post has two parts: the plan,
built only from covered sources, and a section that lists every
uncovered source from that checkpoint. An empty uncovered section means
the sweep finished. The slice does not describe an uncovered source as
if it had been read.

When the reporting slice does not successfully exit, the channel still
hears what was missed, as long as the hook declares a target. After that
hook's slot has aged out under ADR-0099's catch-up bound, and only if
that hook has no successful exit for the sweep date, the scheduler
ADR-0099 places in the worker posts one coverage notice to the target.
The notice quotes the newest checkpoint's uncovered list and names the
run record outcome (`failed`, `skipped`, or `blocked`, the members
ADR-0099 already defined). It does not contain a plan. A skipped overlap
that a later slot in the same sweep date can still replace does not post
the notice early. An aged-out deferral is recorded `skipped` under
ADR-0099, and that is when the notice is due.

If no parseable checkpoint exists, the notice says that nothing was
recorded as covered. It does not invent sources.

A reporting hook with no target stays silent in the channel, which is
ADR-0099's silence rule. The memory log is queryable today through the
memory API. The run record is ADR-0099's surface, queryable once that
record exists. The notice does not add a store, a retry, or an outcome
member.

## Alternatives considered

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
  override. The default is the interactive ceiling. The sweep moves
  around it by using more than one turn.
- **One delivery, many runner calls, budget reset per call.** Rejected.
  It contradicts ADR-0131's single deadline.
- **Partition the sweep (ADR-0134) or delegate it (ADR-0115).** Not this
  decision. Partitions are for independent items in one delivery.
  Delegation cannot be reached from a sandbox today and is still a
  draft. A sweep that must post one plan is a chain of turns of one
  agent, joined by memory.

## Consequences

- An author who writes one cron prompt and expects it to run for 90
  minutes will be cut off at the delivery budget. The uncovered list is
  the result, not a longer turn. Decomposing the sweep is bundle
  authorship, which is the same place ADR-0099 already put the standing
  prompt.
- Each finished source appends one checkpoint that restates the whole
  sweep. The memory log grows by that much. Exact-content consolidation
  does not merge distinct checkpoints.
- Dependent slices scheduled closer together than the configured budget
  are skipped when the earlier slice is still running. The checkpoint
  keeps those sources uncovered, so the skip shows up in the report.
- The coverage notice is new scheduler behavior on a reporting hook that
  never successfully exits. It does not exist until issue #2878 lands.
  This record authorizes it and does not claim a current code path
  performs it. The path to build is the worker scheduler from ADR-0099,
  posting through the placeholder-less target path ADR-0079 assigned to
  the kernel, using the memory log as the coverage source. If the run
  record itself is not built yet, the notice lands with that record,
  not before it.
- Issue #2878 is the realizing work. Its positive case is a sweep of the
  reference shape that finishes across more than one turn on a cluster
  install and posts a plan whose uncovered section is empty. Its
  negative case is a reporting fire that does not successfully exit,
  after which the target receives a notice naming the uncovered sources
  and containing no plan. A second negative case is a slice cancelled at
  the delivery budget: a source it had not appended stays uncovered.
- Cost moves from one long delivery to several ordinary ones. The author
  pays with the same keys ADR-0099 already named. This ADR adds no
  frequency floor.
- Observability of a partial sweep is the checkpoint in the memory log
  plus the post or the notice, with the run record outcome beside them.
  No new outcome and no new dashboard.

## What this does not change

- ADR-0079's event kind, ingress, and placeholder-less post.
- ADR-0099's declaration, authorization, overlap skip, catch-up bound,
  idle deferral, and run record outcomes.
- ADR-0131's single delivery deadline, lease, and grace coupling.
- The frozen ACI session contract and the current `TriggerDeclaration`
  shape.

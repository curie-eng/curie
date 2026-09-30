# 99. Hooks are bundle-declared turns the system starts

Date: 2026-08-06

Status: Accepted

Decides the semantics a bundle-declared background turn runs with, and is the
decision the per-agent cron scheduler (issue
[#268](https://github.com/curie-eng/curie/issues/268), part of Epic
[#29](https://github.com/curie-eng/curie/issues/29)) builds to. It sits on top
of [ADR-0079](0079-inbound-triggers-as-a-new-event-kind.md), which already
decided the wire shape (`source` on the queued event, an optional placeholder,
a kernel that posts rather than edits, and the idle rule for a trigger aimed at
a live thread). This ADR does not reopen any of that. It adds the layer above:
what a declaration carries, what authorizes the turn, when a fire is allowed to
run, and what the operator can see afterwards.

Scope note: this ADR decides declaration and firing semantics. Bounding the
cost of a single long-running background turn (sweep budgets, wall-clock caps
on a maintenance pass) is a separate decision and belongs to its own ADR.

## Context

Every turn Curie runs today begins with a user message. `QueuedTurn`
(`packages/aci-protocol/src/aci_protocol/turn.py`) carries an `author`, a
`text`, and a `ReplyHandle` whose `placeholder` is required; the kernel
(`apps/worker/src/curie_worker/kernel.py`) unconditionally edits that
placeholder as it streams. There is no way for the system itself to start a
turn: no schedule, no reaction to a platform event, no background work of any
kind.

Three prior decisions have been converging on this gap without closing it:

- [ADR-0079](0079-inbound-triggers-as-a-new-event-kind.md) (Accepted,
  unimplemented) decided the wire shape: a `source` field on the queued event
  (`slack | webhook | cron`), an optional placeholder, and an API ingress at
  `POST /hooks/{agent}/{hook}`. None of it is built; `QueuedTurn` still has no
  `source` and `placeholder` is still required.
- The bundle format already carries a `triggers` authoring extension
  (`TriggerDeclaration` in `packages/plugin-format/src/plugin_format/models.py`,
  types `cron` and `webhook`). It is validated at deploy time and consumed by
  nothing; Epic [#29](https://github.com/curie-eng/curie/issues/29) tracks the
  runtime that was never started, and
  [#268](https://github.com/curie-eng/curie/issues/268) is its first slice, the
  per-agent cron schedule.
- ADR-0095 (Draft, in review) needs exactly this machinery twice: a memory
  bootstrap turn when an agent is bound to a channel, and a nightly compaction
  turn. It deliberately deferred the machinery to this ADR's question: what
  runs a turn no user started?

So the declaration exists, the wire contract exists on paper, and the first
two consumers are already specified. What no ADR has decided is the semantics:
what authorizes such a turn, what prompt, model, and credentials it runs with,
what happens when a fire overlaps a still-running fire, where the output goes
when nobody is watching, and how an author tests one before trusting it to a
schedule.

External systems that solved this converge on a small vocabulary (researched
2026-08). Durable-execution frameworks (Temporal, Inngest, Restate, Hatchet)
all offer per-entity serialization keyed by the caller with a named overlap
policy, and their default for periodic maintenance is "skip if still running."
Letta's sleep-time agents designate the background agent as the sole heavy
writer to shared memory rather than locking, and recommend it run the strong
model, not a cheap one, because consolidation quality compounds. Claude Code
Routines contributes the one reasoned answer to authorization: the
pre-registered prompt is the standing authorization, and anything fetched
during the run is data, never fresh consent. The documented failure modes are
equally consistent: silently skipped runs discovered from their absence,
overnight no-progress loops discovered from the bill, and refusal paths that
agents treat as an obstacle to route around.

## Decision

**A hook is a bundle-declared unit of background work: a name, a trigger, and
a standing prompt, run through the kernel as an ordinary turn that no user
started and no channel message answers.**

### Declaration

Hooks extend the existing `triggers` list in the bundle manifest; there is no
new file and no new namespace. A declaration carries:

- `name`: unique within the bundle; the identity used by the API ingress, the
  run record, and the CLI.
- `type`: `cron`, `bind`, or `webhook`. An open vocabulary: later triggers
  (`unbind`, message-count thresholds) add a value, not a mechanism.
- `schedule`: a five field cron expression, required for `cron`, forbidden
  otherwise.
- `timezone`: an IANA zone name (`America/Los_Angeles`), optional, valid only
  alongside `schedule`, defaulting to `UTC`. A schedule with no zone is a
  schedule whose author has not said whether 09:00 means their morning or the
  cluster's, and the two differ twice a year. The zone is resolved per slot, so
  a daily 09:00 hook stays at 09:00 local across a DST transition; a slot that a
  forward transition skips does not fire, and a slot a backward transition
  repeats fires once, because the per slot claim below is keyed on the resolved
  instant.
- `target` (optional): the channel or thread a cron hook's reply posts to, in
  the same shape the queued event already carries. With no target the turn is
  silent (see Silence and the run record).
- `prompt`: the standing task, required. This text is the turn's input.
- `systemPrompt` (optional): replaces the bundle `systemPrompt` for this
  hook's turns. Background work is genuinely different work; a compaction turn
  should not boot with a conversational persona.
- `model` (optional): a per-hook model override.
- `env` (optional): extra environment for this hook's turns, resolved through
  the bundle's sealed-secret mechanism
  ([ADR-0094](0094-a-bundle-carries-its-own-sealed-connector-keys.md)).

`validate_bundle` enforces the shape at deploy time, as it already does for
the declaration-only form.

### Triggers in v1

- `cron`: a fixed-interval scheduler loop **in the worker** scans due schedules
  and enqueues fires as `source=cron` events on the runs stream, exactly the
  event kind ADR-0079 defined. It is the connector reconcile loop's pattern
  (`apps/worker/src/curie_worker/connector_loop.py`, ADR-0090): a plain asyncio
  loop inside a process that already holds the run lifecycle, the agent tables,
  and the lease and reclaim primitives this ADR needs
  (`apps/worker/src/curie_worker/delivery_lease.py`). No new Deployment, no
  ServiceAccount, no image, and no hand-applied Kubernetes CronJob per agent.

  **Exactly one fire per slot, across every replica.** The scheduler is not a
  singleton and needs no leader election. Every replica may notice the same due
  slot; the fire is admitted by a compare-and-set on a run-record row keyed by
  (agent, hook, resolved slot instant), so the first writer wins and every other
  replica finds the slot already claimed and does nothing. A slot is a value, not
  a moment the loop has to be awake for, which is what makes the guarantee hold
  under restarts, rolling upgrades, and clock skew between replicas, and what
  makes the scheduler's own tick interval an implementation detail rather than
  a semantic.
- `bind`: fires when the agent is bound to its channel. The control plane
  emits it directly, because binding is a platform action that produces no
  surface event.
- `webhook`: already decided by ADR-0079 (`POST /hooks/{agent}/{hook}`, HMAC
  verified). This ADR adds nothing to its ingress; it adds the same execution
  semantics the other two triggers get.

**Catch-up is bounded to one slot.** When the scheduler comes back and finds
slots it slept through (platform down, worker rolled, schedule paused), it fires
the most recent missed slot once and abandons every older one, recording them
`skipped`. One catch-up, never a queue of them.

Both extremes are wrong for this work. Full backfill converts an outage into a
thundering burst of stale turns, each one spending real tokens on work a later
slot already subsumes. Zero catch-up loses the case the schedule exists for: a
nightly hook whose slot fell inside a ten minute rollout does not run that
night at all, and nobody notices until the absence is a week old. A single
catch-up is the smallest rule that makes a brief outage invisible while keeping
a long one from spending anything extra, because for periodic maintenance the
most recent slot's work is a superset of the ones before it. The abandoned
slots are recorded rather than dropped, so the run record still answers what
happened across the gap.

Catch-up is bounded in age as well as in count: a missed slot older than the
schedule's own interval, or older than a fixed ceiling for a coarse schedule, is
recorded `skipped` and not fired. A monthly hook that comes back four weeks late
should start fresh, not open with a turn written against a world that moved.

### Authorization

The declared prompt is the standing authorization for the turn. It was
written by the bundle author at deploy time, reviewed as part of the bundle,
and is the only instruction the turn starts with. Content the turn fetches
while running (channel history, connector output) is data and can never act
as consent for anything.

Approval gates are not suspended. A hook turn that reaches a gated tool posts
its approval card and suspends, exactly as a user-started turn does; the
async approval machinery (expiry sweeper, resume reconciler, dead-letter
backstop) was built for absent users and needs nothing new. A hook that must
never block, such as ADR-0095's memory hooks, simply avoids gated tools and
treats reaching a gate as a failed run.

A failed or refused run is a hard stop. It is recorded and waits for the next
fire. It is never retried within the fire, and never an input the agent may
plan around; the documented pathology here is a denial becoming a recursive
spawn trigger.

The kill switch and the budget are fail closed for hook fires. A fire for an
agent that is killed, or whose budget is spent, records `blocked` and never
starts a turn. Both mechanisms exist today
(`apps/worker/src/curie_worker/killswitch.py` and the platform budget), and an
unattended schedule is exactly where a killed agent that kept firing would be
discovered late, so an implementer should not have to derive the answer.

A hook turn gets the agent's ordinary tools, including the channel read verbs
(ADR-0100) when the bundle enables them. Reading the bound surface is much of
the point of background work, and the same author writes the hook, the standing
prompt, and the enablement flag, so the platform fences neither against the
other. That ADR carries the residual exposure this creates and the hardening
lane for it.

### Silence and the run record

A hook turn has no placeholder. ADR-0079 already decided what that means on the
wire: the event carries no placeholder and the kernel posts its reply instead of
editing one. This ADR keeps that and says where the post goes.

A hook that declares a `target` posts its reply there, through the kernel's
placeholder-less output path. That is the case #268 asks for: a scheduled job
that fires on its cron and posts to the target thread.

A hook with no `target` has nowhere to post, so it runs silent: nothing appears
in any channel unless the turn's own tools deliberately post (a digest hook
posting its digest is a tool action like any other, subject to the same approval
policy). The memory hooks ADR-0095 deferred here are of this kind; their whole
output is a side effect.

Silence is therefore a property of the declaration, not of hooks as a class. An
author who wants a visible scheduled report names a target and gets one, and an
author who wants background maintenance names none and the channel stays quiet.

The durable output is the turn's side effects plus a run record: agent, hook,
trigger kind, the resolved slot instant and its zone, outcome (`ran`,
`deferred`, `skipped`, `blocked`, `reclaimed`, `failed`), duration, and usage. A run that started and exited is not a task
that succeeded, so the outcome is recorded from the turn's exit status, not its
existence.

The outcome members stay distinct rather than collapsing into one non-run
value, because the question a run record has to answer is why a hook has not
run since Tuesday: `deferred` is a fire that waited on a live interactive
session, `skipped` is a fire that overlapped a live run of its own hook or an
abandoned catch-up slot, `blocked` is
a fire the kill switch or a spent budget refused, `reclaimed` is a claim
recovered past its lease, and `failed` is a turn that ran and exited badly.
Each has a different fix and a query that cannot tell them apart sends the
operator looking in the wrong place.
Observability stays lightweight: the run record, queryable through the
platform API and CLI, plus the OTel the runner already emits. No bespoke
dashboards in v1, but "silent" never means "invisible": a hook that failed
three nights running must be visible in one query.

### Concurrency and idle

A fire is idle-aware. ADR-0079 fixed the rule and the reason: a trigger aimed at
a thread with a live interactive session waits for idle, because jobs are
outputs, not steering inputs. A cron fire whose target thread holds a live
session does not queue, does not steer that session, and does not force a second
one; it records `deferred` and is retried on the next scheduler tick until the
thread goes idle or its slot ages out under the catch-up bound above. The
mechanism already exists: the per thread lock
(`apps/worker/src/curie_worker/threadlock.py`) is what makes one live session
per thread true across workers, and the scheduler consults it rather than
inventing a second notion of busy. A hook with no target has no thread to wait
on and is never deferred for this reason.

Deferral is bounded on purpose. Without the age bound, a thread that stays busy
all day silently accumulates one pending nightly fire that lands at an arbitrary
hour, which is worse than not running.

Fires also serialize per (agent, hook) with skip semantics: a fire that finds the
previous run still in flight records `skipped` and does not queue. A skipped
fire is a recorded outcome, never a silent no-op. The mechanism is a CAS
claim on the run record, the same replica-safe shape that admits one fire per
slot above. Skip is the right member of the standard overlap vocabulary here
because a missed maintenance pass self-heals and a doubled one corrupts.

The claim carries a lease, and a claim held past its lease is reclaimable by
the next fire, which records `reclaimed` and proceeds. Without one, a run that
never settles because a worker died or a sandbox wedged holds the claim
forever and every later fire skips: visible in a query, but a permanent stall
that no component resolves on its own. The worker already solved this class for
stream deliveries (`reclaim_min_idle_ms` and `XAUTOCLAIM` in
`apps/worker/src/curie_worker/stream_consumer.py`); the lease is that shape
applied to the run claim. Making the reclaim its own recorded outcome is what
keeps the stall diagnosable rather than inferred from a gap.

### Model, prompt, and credentials

The model precedence gains one layer: hook override, then agent pin, then
platform default, extending the existing two-layer resolution in
`apps/worker/src/curie_worker/binding.py`. There is no automatic cheap tier
for background work; a hook that says nothing runs on the agent's own model,
because consolidation-quality work compounds and the external evidence says
background turns want the strong model, not the cheap one.

Hooks spend the same credentials the agent's ordinary turns spend, resolved
the same way. Per-hook `env` rides the sealed-secret path and is subject to
the same reserved-name fence
([ADR-0049](0049-boot-env-contract.md)); the operator-authority keys (API
backend, credential env key) remain non-overridable by any bundle surface,
hooks included.

There is deliberately no platform frequency floor or daily run cap in v1. The
bundle author's schedule burns the bundle author's keys, so the author owns
that trade. The trade is narrower than it sounds: the author's keys cover model
spend, and the operator carries the sandbox, the queue, and the scheduler (see
Consequences). What is rejected here is a platform wide floor, not an
operator's ability to set one for their own cluster. The first deployment model
where someone else pays for the tokens too (multi-tenancy, Epic #158) must
revisit this before it ships.

### Test-fire from the CLI

Every tier gets a fire verb, per the tier-parity rule: `curie skill hook fire
<name>` against the local session, and the `local` and `cluster` twins taking
an agent. A test fire runs the hook immediately, bypassing the schedule but
not the serialization or the run record, and prints the record when the turn
settles. An author must be able to kick a hook and read its outcome before a
schedule ever runs it unattended.

At the skill tier the fire verb is the whole surface. That tier has no API and
no Postgres, so it runs the hook against the local session and prints the
turn's outcome with no durable record behind it (see Consequences).

## Consequences

- This ADR sequences behind ADR-0079's implementation: the `source` field,
  the optional placeholder, the kernel's post-instead-of-edit branch at every
  placeholder call site, and the tri-language contract regeneration are all
  prerequisites, and the contract work lands with lane adoption as always.
- `TriggerDeclaration` grows the fields above; the schema-compat gate and
  `validate_bundle` cover them.
- A run-record table and its API surface are new; the CLI grows the fire verb
  at three tiers. The table is the concurrency mechanism as well as the
  observability surface: the per slot uniqueness constraint is what makes
  "exactly one fire per slot" a database guarantee rather than a loop's good
  behavior, so it cannot be added later as a reporting afterthought.
- Cron expressions and IANA zone resolution are the scheduler's only new
  library surface, and slot resolution is pure: given a schedule, a zone, and a
  window, it yields the set of slot instants. That is where the DST, catch-up,
  and skew cases are tested, with no loop, no database, and no turn.
- ADR-0095's bootstrap and compaction become the first `bind` and `cron`
  hooks. That ADR remains the memory decision; this one is the machinery it
  deferred.
- The scheduler is a worker-side loop, so hooks fire wherever the worker runs,
  which is the `local` and `cluster` tiers, without substrate-specific plumbing
  and without an operator hand-applying a CronJob per agent. The
  skill tier has neither a worker nor Postgres (`curie skill message` drives a
  local runner container directly, bypassing the dispatcher and worker by
  design), so it has no loop to host the scheduler and no table to hold a run
  record. It gets manual test-fire only: no schedule, no durable record, and
  the schedule and record verbs report the capability unavailable by design,
  the same shape
  [ADR-0077](0077-skill-tier-durable-approvals-stay-unavailable.md) set for
  durable approvals. That keeps the tier-parity rule honest rather than
  quietly unmet.
- The scheduler loop, the queue traffic, and a fresh sandbox per fire are borne
  by the cluster operator, not the bundle author, so "the author pays" is true
  of model spend and of nothing else. That asymmetry does not wait for
  multi-tenancy; it exists as soon as the author and the operator are different
  people, which is the ordinary case for a deployed agent.
- ADR-0079's optional placeholder changes a closed field on the frozen ACI
  contract (`ReplyHandle.placeholder` is `placeholder: str` with no default
  today), so that prerequisite bumps `PROTOCOL_VERSION` and regenerates the
  three language artifacts under that package's own frozen-interface rule,
  gated by `test_schema_compat.py` and `test_wire_lock.py`. The closed-schema
  rules in ADR-0101 and ADR-0103 govern the agent-facing CLI result schemas, a
  different namespace, and do not decide this one.
- Failure handling is intentionally minimal: no retry, no backoff, no
  dead-letter queue for hook fires in v1. The next fire is the retry.

## Alternatives considered

- **A hand-applied Kubernetes CronJob per agent, or an external scheduler.**
  Rejected: two of the three tiers have no cluster, the schedule is bundle data
  that changes on every deploy while a CronJob is cluster state an operator
  applies by hand, and the loop pattern already runs everywhere the worker
  runs. A substrate-specific scheduler would fork the behavior the tiers are
  required to share, and it would put the fire outside the run record that
  serializes and reports it.
- **The scheduler in the API rather than the worker.** Considered, since the
  API owns the expiry sweeper (`apps/api/src/curie_api/sweeper.py`) and that
  loop is the closest existing shape. Rejected: the worker already owns the run
  lifecycle, the per thread lock the idle rule consults, and the lease and
  reclaim primitives the claim needs. Hosting the loop in the API would mean
  reaching across a service boundary for all three on every tick.
- **Leader election for the scheduler.** Rejected as machinery bought for
  nothing. Per slot CAS gives exactly-once admission without electing anyone,
  and unlike an election it stays correct through a partition, a paused
  replica, and a clock that disagrees.
- **Unbounded backfill of missed fires** (Temporal's catch-up windows).
  Rejected: every intended consumer is self-healing periodic maintenance, and
  full backfill converts an outage into a thundering burst of stale work whose
  results the newest slot would have produced anyway.
- **No catch-up at all.** The earlier draft of this ADR. Rejected on the
  operational case: a rolling upgrade is minutes, a nightly slot is a point,
  and a decision that lets a routine deploy silently cost a night of work
  teaches operators to distrust the schedule. One bounded catch-up buys that
  back without reopening the burst.
- **Buffered fires** (Temporal's `BufferOne` for overlap). Rejected for the
  overlap case for the reason skip is chosen above: a doubled maintenance pass
  corrupts, and a buffered one is a doubled one delayed.
- **A naive UTC-only schedule.** Rejected: the first real consumer is a
  briefing a person reads in the morning, and a UTC-only schedule silently
  moves that by an hour twice a year. Storing a zone name rather than an offset
  is what makes the local hour the stable thing.
- **Platform frequency floors and daily caps in v1.** Considered, since
  comparable products ship them (Claude Code Routines enforces an hourly
  floor and a daily cap). Rejected while the author pays with their own
  keys; recorded as the first requirement of any payer-splitting deployment
  model. The operator-borne share of the cost is named in Consequences rather
  than fixed by a platform floor in v1.
- **A separate tiny ADR for cron versus event hooks.** Considered, since a
  cron is not conceptually a lifecycle event. Kept together because the two
  share every semantic except the trigger: declaration, authorization,
  silence, serialization, run record, and CLI are identical, and splitting
  would duplicate the entire middle of this document to separate one
  paragraph of trigger vocabulary.
- **Retry with backoff on failed runs.** Rejected for v1: it drags in
  poison-pill detection and dead-lettering for a class of work whose next
  scheduled fire is already the retry.
- **A pub-sub event bus for platform events.** Rejected as over-general: the
  concrete need is three trigger kinds. A consumer who wants fan-out can
  point a `webhook` hook at their own infrastructure.

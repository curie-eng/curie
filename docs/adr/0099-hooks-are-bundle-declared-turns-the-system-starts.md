# 99. Hooks are bundle-declared turns the system starts

Date: 2026-08-06

Status: Draft

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
  nothing; Epic #29 tracks the runtime that was never started.
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
- `schedule`: a cron expression, required for `cron`, forbidden otherwise.
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

- `cron`: a fixed-interval scheduler loop in the API scans due schedules and
  enqueues fires. This is the approval expiry sweeper's pattern
  (`apps/api/src/curie_api/sweeper.py`): a plain asyncio loop, safe under
  multiple replicas because the unit of work is claimed by CAS, so no leader
  election and no new service.
- `bind`: fires when the agent is bound to its channel. The control plane
  emits it directly, because binding is a platform action that produces no
  surface event.
- `webhook`: already decided by ADR-0079 (`POST /hooks/{agent}/{hook}`, HMAC
  verified). This ADR adds nothing to its ingress; it adds the same execution
  semantics the other two triggers get.

There is no catch-up. A fire that was missed (platform down, schedule paused)
is simply missed; the next fire covers it. Every intended consumer of a
schedule is periodic maintenance, which self-heals on the next cycle, and
backfilling turns a stall into a burst.

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

### Silence and the run record

A hook turn has no reply handle. Nothing appears in the channel unless the
turn's own tools deliberately post (a digest hook posting its digest is a tool
action like any other, subject to the same approval policy).

The durable output is the turn's side effects plus a run record: agent, hook,
trigger kind, fire time, outcome (`ran`, `skipped`, `failed`), duration, and
usage. A run that started and exited is not a task that succeeded, so the
outcome is recorded from the turn's exit status, not its existence.
Observability stays lightweight: the run record, queryable through the
platform API and CLI, plus the OTel the runner already emits. No bespoke
dashboards in v1, but "silent" never means "invisible": a hook that failed
three nights running must be visible in one query.

### Concurrency

Fires serialize per (agent, hook) with skip semantics: a fire that finds the
previous run still in flight records `skipped` and does not queue. A skipped
fire is a recorded outcome, never a silent no-op. The mechanism is a CAS
claim on the run record, the same replica-safe shape the approval sweeper
uses. Skip is the right member of the standard overlap vocabulary here
because a missed maintenance pass self-heals and a doubled one corrupts.

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
the trade. The first deployment model where someone else pays (multi-tenancy,
Epic #158) must revisit this before it ships.

### Test-fire from the CLI

Every tier gets a fire verb, per the tier-parity rule: `curie skill hook fire
<name>` against the local session, and the `local` and `cluster` twins taking
an agent. A test fire runs the hook immediately, bypassing the schedule but
not the serialization or the run record, and prints the record when the turn
settles. An author must be able to kick a hook and read its outcome before a
schedule ever runs it unattended.

## Consequences

- This ADR sequences behind ADR-0079's implementation: the `source` field,
  the optional placeholder, the kernel's post-instead-of-edit branch at every
  placeholder call site, and the tri-language contract regeneration are all
  prerequisites, and the contract work lands with lane adoption as always.
- `TriggerDeclaration` grows the fields above; the schema-compat gate and
  `validate_bundle` cover them.
- A run-record table and its API surface are new; the CLI grows the fire verb
  at three tiers.
- ADR-0095's bootstrap and compaction become the first `bind` and `cron`
  hooks. That ADR remains the memory decision; this one is the machinery it
  deferred.
- The scheduler is an API-side loop, so hooks fire wherever the API runs:
  skill, local, and cluster tiers all get them without substrate-specific
  plumbing.
- Failure handling is intentionally minimal: no retry, no backoff, no
  dead-letter queue for hook fires in v1. The next fire is the retry.

## Alternatives considered

- **Kubernetes CronJobs or an external scheduler.** Rejected: two of the
  three tiers have no cluster, and the sweeper-loop pattern already runs
  everywhere the API runs. A substrate-specific scheduler would fork the
  behavior the tiers are required to share.
- **Buffered or backfilled fires** (Temporal's `BufferOne`, catch-up
  windows). Rejected: every intended consumer is self-healing periodic
  maintenance, and buffering converts an outage into a thundering burst of
  stale work.
- **Platform frequency floors and daily caps in v1.** Considered, since
  comparable products ship them (Claude Code Routines enforces an hourly
  floor and a daily cap). Rejected while the author pays with their own
  keys; recorded as the first requirement of any payer-splitting deployment
  model.
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

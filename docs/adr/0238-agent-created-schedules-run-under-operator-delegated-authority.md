# 238. Agent-created schedules run under operator-delegated authority

Date: 2026-10-20

Status: Accepted

Accepted by the maintainer on 2026-10-19 to land alongside its implementation on epic/self-improving.

Amends [ADR 0099](0099-hooks-are-bundle-declared-turns-the-system-starts.md) and
[ADR 0160](0160-a-long-scheduled-sweep-stays-on-one-sandbox.md) by status-line
back link, for agent-created schedules, a second and mutable schedule source.
Builds on [ADR 0188](0188-the-sandbox-memory-credential-is-scoped-to-its-own-channel.md),
whose per-turn state credential carries the new write authority.

Realizing code paths:
`apps/api/src/curie_api/routers/agent_schedules.py`,
`apps/worker/src/curie_worker/cron_loop.py`,
`apps/worker/src/curie_worker/hook_runs.py` and
`runner/src/curie_runner/harness/claude/agent_schedule_tools.py`.

## Context

ADR 0099 makes a scheduled turn a bundle-declared hook. The bundle author
writes the trigger and its standing prompt, the bundle is reviewed and
published, and the operator installs it. The prompt's authority comes from that
author, and it cannot change without a new bundle revision.

An agent that learns a recurring need in conversation ("send me the standup
summary every weekday at 8") has no way to act on it. The only route is to
ask a human to edit a bundle. The platform needs a second schedule source that
an agent can write at runtime, without turning the agent's own text into
authority it was never given.

That raises questions bundle hooks never faced. Who authorized the prompt? What
stops a scheduled run from scheduling more runs? What happens when the
schedule is edited while a run is queued, or while an approval is pending? How
do a one-shot and a crash interact? Which timezone does "8am" mean?

## Decision

### 1. The operator delegates authority with a per-agent toggle

The operator turns on the per-agent `agent_schedules` setting (default off).
Turning it on is the delegation: the agent may create schedules for the
people it serves, and the platform runs them. A self-written prompt is never
itself authorization. ADR 0099's bundle-author standing-prompt authority is
unchanged for bundle hooks.

With the toggle off, the schedule tools are not mounted, the API refuses agent
credentials, and the cron loop fires nothing from the agent's stored
schedules. An operator can still list, cancel and revert while it is off.

### 2. Provenance, change log and immutable run snapshots

Every schedule records its creator, the conversation, the turn and its
configuration revision. `revision` moves only when the prompt, cron, run time
or timezone change, or on revert. Pause, resume, cancel and completion do not
move it. Every change, including those, appends to a change log keyed by
(schedule, sequence) with the revision, action, author, source, conversation,
turn and time.

Each firing writes an immutable snapshot of the exact configuration it ran
(prompt, cron or run time, timezone, destination, revision), keyed by the
hook run. Later edits never rewrite what a past run did.

### 3. Existing tool approval gates remain the boundary

A scheduled run gets the tool access the agent already has, under the same
approval gates. Nothing scheduled widens tool access (ADR 0190 stays the rule
for automated sources). The toggle delegates the right to cause a turn, not
the right to take any action in it.

### 4. Who may change a schedule

Anyone the agent serves may list, change, pause, cancel or revert a schedule,
because the agent acts on their behalf in a shared conversation. The operator
may do the same through the CLI and API. Revert restores an earlier revision's
configuration as a new revision; a restored cron must meet the current minimum
interval, as a new one would. Updates carry an
`expected_revision`, and a stale one is a conflict.

### 5. Triggers are named `agent-schedule:<id>`

The cron loop enumerates active agent schedules beside bundle triggers and
admits them through the same guarded path: source guard, advisory lock,
`hook_runs` slot claim, lease, stream and kernel. The name `agent-schedule:<id>`
means the schedule-safety ledger (spend, budget block) and the three-failure
auto-pause apply with no extra code. Hook fire and the pause and resume routes
accept that name.

### 6. Pause is the shared `schedule_controls` row

Pause is the `schedule_controls.paused_at` row that auto-pause already writes.
There is no second pause flag. The read-back `paused` is derived from that row
while the stored state is `active`. Stored states are `active`, `cancelled` and
`done`.

Cancel moves an `active` schedule, paused or not, or a `done` one to
`cancelled`, stamps who cancelled it and when, logs one `cancel` entry and
leaves the revision unchanged. Cancelling a cancelled schedule is a no-op.
Cancelling a `done` one-shot revokes a pending approval continuation from its
run, because approval resume admits a `done` schedule and refuses a cancelled
one (section 10).

### 7. Limits

Each agent has at most 20 active schedules (paused ones count) and a minimum
interval of 15 minutes between firings. The operator can adjust both per agent
(1 to 100 schedules, 1 to 1440 minutes). The API enforces both server-side;
the runner tool's checks are advisory.

The minimum interval is the exact elapsed minimum, the smallest of three parts.

1. The intra-day gaps between consecutive minute-of-day values the cron allows,
   with no wrap.
2. The wrap gap across midnight (1440 minus the latest plus the earliest value),
   counted only when the day rules (day of month, month, day of week, with
   the standard OR rule when both day fields are restricted) allow two
   consecutive calendar days, decided by iterating the dates of the next 1,461
   days.
3. The real elapsed gaps around each UTC-offset transition of the zone in the
   next 366 days.

So `5,55 1,3 * * *` in a spring-forward zone yields 10 from part 3 and is
refused at 15, `5,55 0,23 1 * *` yields 50 and passes, and `*/10 * 1 1 *` yields 10.

Accepted residual: the day rules are checked over a finite horizon, so a rule
that first allows consecutive days beyond 1,461 days is read as never doing so.
Part 3 is evaluated over 366 days of transitions.

### 8. Time zone

Each agent has a default IANA time zone (`UTC` until set). A schedule may name
its own. Read-back names the zone and the next run in local time (for example
"Mon 2026-10-26 08:00 EDT (America/New_York)"), so the person can check what
the agent understood. A local time that is ambiguous (fall-back overlap) or
nonexistent (spring-forward gap) is refused with a question asking which
instant is meant. The agent is guided to ask that clarifying question.

### 9. One-shots fire at most once

A one-shot's slot is `run_at + (revision - 1) microseconds`. Replicas of one
revision compute the same slot and race on the unique `hook_runs` insert, so
exactly-once admission rests on that key and not on the stored state. The
stored state stays `active` after admission.

Each pass reconciles active one-shots that have a row at the current slot.

1. A terminal outcome marks the schedule `done`, with a `complete` log entry.
   This includes `skipped` with reason `schedule_changed` at the current
   revision's slot, so a toggle flipped off and on, or a destination unbound and
   rebound, cannot block the schedule forever. Only a newer revision (a new
   slot) re-arms it.
2. `deferred` retries through the existing deferred path, bounded by one hour.
3. An open row with a lapsed lease is closed `reclaimed` with reason
   `claim_expired` and the schedule is marked `done`.

A slot older than one hour is recorded `skipped` with reason `catch_up_expired`.
A manual test fire uses its own slot and never consumes a one-shot. A
one-shot's `run_at` can change only while no row exists for it, and a schedule
that has fired cannot become a one-shot (by update or revert): reconciliation
reads any earlier non-`schedule_changed` row as the one-shot's own firing, so
the API refuses the change with 409 and the person cancels and creates a new
one-shot instead.

If a worker dies between the commit and the stream append, that firing is lost
visibly (last outcome `reclaimed`) rather than risking a second delivery from a
surviving stream entry that is later reclaimed.

### 10. Revalidation at runner start and at approval resume

One lock order everywhere: the per (agent, name) transaction advisory lock,
then the schedule row lock. Every API mutation of a schedule takes both.

At runner start, `HookRunRecorder.start_guard` holds the advisory lock across
`start_turn` and requires that the schedule exists and is not cancelled, the
toggle is on, the configuration revision equals the run snapshot's revision,
the schedule is not paused, and the exact destination binding (kind, address,
adapter) still exists. Otherwise it closes the row `skipped` with the new
reason `schedule_changed` and commits that close before refusing, so the
kernel's later close is a no-op and the scheduler never sees a deferral. An
ordinary pause keeps its resumable deferred path.

At approval resume, the same predicate runs under the same lock, held across
`start_turn` and the steer call. The originating run is identified from the
approval's `dedupe_key`, which keeps the original cron event id. The resume
compares that run's snapshot revision with the current revision. The guard
applies only when the approval chain starts at a cron event id; the origin is
read whenever the toggle is on or the conversation is a schedule's, and an
unreadable or unresolved origin (a failed read, an unparseable resume id, a
missing or cross-agent link, a cycle, an overlong chain) counts as cron. An
approval whose chain resolves to a person,
even in a schedule's conversation, resumes as an ordinary unscheduled turn
with no schedule guard at intake, start or steer. A refused
resume is completed as dropped with a log line and no reply. A one-shot that is
`done` still passes, so a person revokes that pending continuation by
cancelling the done one-shot.

The guards read the agent row and the exact destination row `FOR SHARE` inside
the guard transaction. A concurrent toggle-off or binding removal therefore
waits until the start commits and applies to every later start. A run that has
already started before they commit finishes.

Cost: `PATCH /agents` on that agent, and the hook generation compare-and-set,
wait for a runner start in progress. Edits to other agents or unrelated
bindings take no lock.

### 11. A scheduled run cannot create or change schedules

The worker adds a signed `scheduled` claim to the per-turn credential for a
CRON turn, and for an approval resume whose approval chain leads back to a
cron event id. The claim is exact: only JSON `false` admits create, update and
revert, only `true` marks a scheduled run, and a missing or malformed claim is
refused. The API refuses create, update (pause and resume are updates) and
revert when the claim is set, and when the sender is `<no person>` or starts
with `cron:`. List and cancel stay allowed. No conversation-prefix detection is
used. The runner tool also refuses by author, as defense in depth only.

Learned skills (ADR 0237) already refuse writes from a `<no person>` or `cron:`
sender. An approval continuation of a scheduled run names the approver as its
sender, so the learned-skills API also refuses when the claim is `true`, and the
worker reads the approval's origin whenever learned skills are on, not only
when agent schedules are.

### 12. The runner learns the toggle by an API read

The runner's environment is frozen at boot, so the toggle cannot ride in it.
The runner reads the agent's schedule configuration from the API with its boot
read token and mounts the tools only when enabled. The worker mints the
per-turn write credential when the agent has memory writes or schedules, with
claims `schedules` ("write"), `conversation` and `scheduled`. The `memory`
claim is `write` only with memory writes, so enabling schedules alone does not
grant memory write.

### 13. Destination

The destination is the turn's binding resolved at create time to one
`agent_channels` row (kind, address, adapter). Fire time, start guard and
resume guard match that exact row, with no fallback to another binding.

### 14. Idempotent create

The idempotency key hashes the turn's event id (the claim with only the final
`#<suffix>` stripped), the normalized cron or run time, the timezone and the
prompt, and is unique per agent. A repeat returns the existing row, and a
replay at the active limit returns it before the capacity check.

## Consequences

1. Agents can schedule work for the people they serve, bounded by an operator
   toggle, a count, a minimum interval and the existing approval gates.
2. Every firing is attributable to a creator, conversation, turn and revision.
3. The safety ledger and auto-pause cover agent schedules without a second
   accounting path.
4. DST can shorten one real gap per transition by the shift. The check covers
   known transitions, and the residual in decision 7 is accepted.
5. A start in progress delays `PATCH /agents` on that agent (decision 10).
6. A worker crash in the commit-to-append window loses one one-shot firing,
   visibly, instead of risking two.
7. Known limitation: approval continuations of a scheduled run do not count
   toward the schedule-safety ledger or the failure streak. The first segment
   closes the hook run when it reaches awaiting-approval and the resumed turn
   carries no hook run. This is identical for bundle hooks today, and it is
   left to the schedule-safety owner rather than changed here.

## Alternatives considered

1. **A stored paused state.** Rejected: auto-pause writes only
   `schedule_controls`, so a second flag would disagree with it.
2. **Republishing an open one-shot row with a lapsed lease.** Rejected: the
   original stream entry may still be redelivered, and both deliveries would
   see an open row, which is a possible double delivery.
3. **Finite cron sampling for the minimum interval.** Rejected: a sample window
   can miss a short gap (monthly or day-restricted rules, DST). The static
   analysis of decision 7 decides it exactly within its stated horizon.
4. **Conversation-prefix detection of scheduled continuations.** Rejected:
   it infers provenance from a name. The signed claim, set from the turn source
   and the approval's cron origin, is trusted.
5. **Per-person scope.** Rejected: agents act in shared conversations, so any
   person the agent serves may manage a schedule, and the operator holds the
   toggle.

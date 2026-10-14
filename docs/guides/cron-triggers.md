# Cron triggers

A bundle can wake its agent on a schedule. Declare a `cron` trigger in
`plugin.json`; the worker's cron scheduler
(`apps/worker/src/curie_worker/cron_loop.py::CronSchedulerLoop`, ADR-0099) fires
it for every in-force deployment. No extra service or chart value turns it on.

## Declaring a trigger

```json
{
  "name": "weekly-report",
  "version": "0.1.0",
  "triggers": [
    {
      "type": "cron",
      "name": "friday-summary",
      "schedule": "0 16 * * FRI",
      "timezone": "America/New_York",
      "target": "C0123456789",
      "prompt": "Summarize this week's merged pull requests and post the summary."
    },
    {
      "type": "cron",
      "name": "nightly-cleanup",
      "schedule": "30 2 * * *",
      "prompt": "Close stale work items older than 30 days."
    }
  ]
}
```

Fields, checked at deploy by
`packages/plugin-format/src/plugin_format/validate.py::_validate_triggers`:

- `name`: required, non-empty, unique among the bundle's triggers. It keys the
  hook's run history, so renaming a trigger starts a new history.
- `schedule`: required, a standard five-field cron expression (minute, hour,
  day of month, month, day of week). Names such as `FRI` or `JAN` are allowed.
- `timezone`: optional IANA zone name such as `Europe/Berlin`. It defaults to
  UTC. Host aliases such as `localtime` are rejected. A wall time skipped by a
  spring-forward change does not fire; a wall time repeated by a fall-back
  change fires once.
- `target`: optional channel address. When present, it must be a channel bound
  to this agent, and the turn's reply is a new top-level post in that channel.
  When omitted, the turn is targetless: it runs silently and posts nothing.
- `prompt`: required, non-empty. It is the text of the scheduled turn.

## What happens at runtime

Every worker replica ticks every `CURIE_CRON_TICK_INTERVAL_S` seconds (default
30). A slot fires on the first tick at or after its time, so the tick interval
bounds how late a run starts.

Each slot is recorded as one row in the `hook_runs` table, unique on agent,
trigger name and slot. Every replica computes the same slots and only one
insert wins, so a slot runs once however many workers are running. Each row
persists `source`: `schedule` for worker-created slots and `manual` for an
operator fire. The row's outcome is one of:

- `ran`: the turn completed.
- `failed`: the turn failed, or `target` does not match exactly one channel
  bound to the agent (the turn is never queued), or a targetless turn hit an
  approval gate.
- `blocked`: the agent is killed or its budget is spent, so the slot was not
  run.
- `skipped`: an earlier run of the same trigger was still in flight, or the
  slot was missed and is not the one catch-up fires.
- `reclaimed`: the run's worker died before closing it, and the trigger's
  next fire found its claim past its lease and took the hook back.

An in-flight run holds its claim under a lease. The lease defaults to the
worker's delivery budget (`CURIE_DELIVERY_BUDGET_S`), the longest a turn can
run, and `CURIE_HOOK_CLAIM_LEASE_S` sets a longer one. It cannot be shorter
than the budget, since that would reclaim a run that is still going. The worker
renews the lease when it starts the turn, so time a fire spends queued does not
count against it. A run left with no lease by an older worker is held for one
lease from its start. Until the
lease lapses, later fires record `skipped`; after it lapses, the next fire
records the old run `reclaimed` and runs its own slot.

Catch-up is bounded to one slot. When the scheduler comes back and finds slots
it slept through since the trigger's last recorded slot, it fires only the
newest one and records every older one `skipped`. The newest is skipped as well
when it is older than the schedule's own interval or 24 hours, whichever is
shorter, so a weekly trigger that comes back two days late starts fresh. The
scheduler looks back at most 35 days and never past the agent's current
deployment, records at most the newest 1000 skipped slots per trigger, and a
trigger with no recorded slot
never fires a slot from before the worker started.

Approvals fail closed on targetless turns (#3007). A targetless turn has no
channel to post an approval card to or resume in, so a gated tool call ends the
run as `failed` and the tool never runs. Give a trigger a `target` if its work
needs approval.

## Long sweeps

A turn ends at the worker's delivery budget (`worker.deliveryBudgetSeconds`).
A sweep that needs longer, such as a daily plan built from several sources,
continues on the same sandbox when the budget cuts it off, provided it saved a
checkpoint that shows progress and still lists sources it has not covered. The
worker then sends the same prompt again into the same live session
(ADR-0160).

A sweep can continue only when:

1. The trigger has a `target`. A hook with no `target` never continues.
2. Memory writes are on for the agent
   (`curie cluster overrides <agent> --memory-writes on`).
3. `worker.runnerTotalTimeoutSeconds` equals `worker.deliveryBudgetSeconds`,
   which is the default. A turn cut by the per-request ceiling while budget is
   left is not a budget cut, and the sweep stops.

The prompt names the sources and tells the agent to keep one
`sweep-checkpoint` fact per sweep, updating it in place with the `update`
memory tool after each source. A memory holds at most 200 facts and a
statement at most 500 characters, so a new fact per source does not scale. The
checkpoint has one key per line:

```
sweep-checkpoint
sweep: weekday-plan
date: 2026-10-05
hook: weekday-plan
covered: slack
uncovered: github, notes
```

`date` is the slot's local date in the trigger `timezone` (UTC when it is
omitted), and `hook` is the trigger `name`. `covered` lists only finished
sources. `none` or an empty value is an empty list, and a source may not appear
in both lists. The prompt should also say that if today's checkpoint exists the
agent continues from its uncovered list, and that it ends with a post whose
last section lists every uncovered source, which is empty when the sweep
finished.

A source does not have to finish inside one turn: it may span up to two cut
turns before it is saved as covered. Sources are counted by distinct name,
ignoring case. A sweep stops after 3 turns in a row that save no new source,
and after 48 turns in all. While a sweep continues its run has no outcome yet, so
later fires of the same trigger record `skipped` or `deferred`. A sweep never
moves to a new sandbox: if its sandbox is gone it stops.

When a sweep stops short (a failed or refused turn, no progress, its sandbox
gone, or the hook paused), the run records `failed` or `blocked` and the
target gets one notice as a new message, for example:

```
Scheduled sweep "weekday-plan" for 2026-10-05 stopped before it finished (run outcome: failed). Not covered: github, notes.
```

With no checkpoint the notice says nothing was recorded as covered. A sweep
that finishes posts only its own plan. A gated tool inside a sweep pauses the
run as `ran`, and the sweep does not continue after the approval resumes it.

## Reading the record

`curie local schedules` and `curie cluster schedules` list every cron hook on
the in force deployment of each agent. Pass `--agent` to limit the list to one
agent. Each hook shows its trigger, schedule, zone, and two independent
histories: the newest scheduled slot and the newest manual fire. Scheduled
`last_fire_at`, `last_outcome`, and `last_reason` read only rows whose `source`
is `schedule`. `last_manual_fire_at`, `last_manual_outcome`, and
`last_manual_reason` read only `manual` rows. A newer manual fire cannot replace
the scheduled result. Missing history is null in JSON and shown as a dash in
human output. A missing `timezone` is reported as `UTC`. `GET /schedules` is
the same list. `curie skill schedules` is refused, because that tier has no
platform API and no run record.

Read one persisted run with `curie local hook record <agent> <name> <id>` or
`curie cluster hook record <agent> <name> <id>`. Both print the current record
and exit 0 when it is found, including a non-`ran` outcome or an in-flight row
whose outcome is null. They do not wait for the turn to settle. `--json`
includes the persisted `source` and `reason`.

## Firing a hook now

`curie local hook fire <agent> <name>` and `curie cluster hook fire <agent> <name>`
run that hook immediately. The schedule is skipped. An in-flight run of the
same hook is not: the new fire is recorded `skipped` and no second turn is
queued. The fire persists `source=manual` and prints the run record once the
turn settles. If waiting times out, use the run id from the error with
`curie local hook record <agent> <name> <id>` or the corresponding cluster
command to read its current state.

`curie skill hook fire <name>` runs the hook's prompt against the local runner
and prints that turn's outcome. It does not write a run record. `curie skill
hook schedule` and `curie skill hook record` are refused, because that tier
has no scheduler and no run table. `curie skill hook record` exits 4.

A hook that failed on its newest scheduled slot remains visible in schedules,
including when the three newest scheduled slots all failed. A newer manual
fire appears in its separate history. A slot that has not ended
yet has no outcome. The scheduler records `ran`, `failed`, `blocked`, `deferred`,
`skipped`, and `reclaimed`. A row that is not `ran` and not still in flight
also carries a `reason` code, such as `agent_killed` or `target_unbound`.
`ran`, an open row, and a row written before the column existed leave `reason`
empty. Human output prints the reason after the outcome on a hook fire or
record and on the scheduled history when it is present. `--json` includes
`reason` on a hook fire or record, and `last_reason` and `last_manual_reason`
on a schedule. The manual history reason is included only in JSON.

A fire aimed at a thread that holds a live session does not steer that
session or open a second one. It records `deferred`, and the scheduler fires
it again on each later tick until the thread is idle. A deferred slot that
waits longer than the schedule's interval, or longer than six hours for a
coarser schedule, records `skipped` instead. A hook with no `target` is never
deferred.

Webhook triggers validate at deploy but are not yet wired to a live wake-up;
see the [triggers seam](../interfaces/triggers/INTERFACE.md).

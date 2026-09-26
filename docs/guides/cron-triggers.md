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
insert wins, so a slot runs once however many workers are running. The row's
outcome is one of:

- `ran`: the turn completed.
- `failed`: the turn failed, or `target` does not match exactly one channel
  bound to the agent (the turn is never queued), or a targetless turn hit an
  approval gate.
- `blocked`: the agent is killed or its budget is spent, so the slot was not
  run.
- `skipped`: an earlier run of the same trigger was still in flight, or the
  slot was missed and is not the one catch-up fires.

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

## Reading the record

`curie local schedules` and `curie cluster schedules` list every cron hook on
the in force deployment of each agent. Pass `--agent` to limit the list to one
agent. Each hook shows its trigger, schedule, zone, newest slot, and how that
slot ended. A missing `timezone` is reported as `UTC`. `GET /schedules` is the
same list. `curie skill schedules` is refused, because that tier has no
platform API and no run record.

A hook that failed on its newest slot is visible in that one response,
including when the three newest slots all failed. A slot that has not ended
yet has no outcome. The scheduler records `ran`, `failed`, `blocked`, and
`skipped`. `deferred` and `reclaimed` are part of the run record vocabulary
and are not written by the current scheduler.

Webhook triggers validate at deploy but are not yet wired to a live wake-up;
see the [triggers seam](../interfaces/triggers/INTERFACE.md).

# 249. A factory run may take twelve hours

Date: 2026-10-26

Status: Accepted

Partially supersedes [ADR 0171](0171-a-factory-run-may-take-three-hours-and-is-bounded-by-time-not-turns.md):
the 10800 second bounds in its decision 1 (the per-agent execution deadline) and
the 10800 second bounds on the worker delivery budget and runner request ceiling
in its decision 2. Everything else in ADR 0171 stands, including the fixed-at-start
rule for a request's deadline and the turn budget rules.

Accepted alongside implementation under the coordinated exception in
[ADR 0102](0102-accepted-alongside-implementation-with-explicit-approval.md) and
step 4 of `docs/adr/AGENTS.md`. The recorded maintainer approval is Brian Conn
(repository maintainer), who decided on 2026-10-26 that the factory run ceiling
goes to twelve hours, recorded in issue #5067. The realizing code path is this
pull request's change to `MAX_EXECUTION_DEADLINE_SECONDS` in
`apps/api/src/curie_api/models.py`, migration
`0136_execution_deadline_twelve_hours`, `MAX_DELIVERY_BUDGET_S` in
`apps/worker/src/curie_worker/config.py`, and the maxima of
`worker.deliveryBudgetSeconds` and `worker.runnerTotalTimeoutSeconds` in
`charts/curie/values.schema.json`.

## Context

ADR 0171 set the ceiling for an agent's execution deadline at 10800 seconds. On
2026-10-26 a production factory run on a large monorepo needed about 40 minutes
of toolchain bootstrap and then ran a test suite of more than 30 minutes. It
reached all checks green two minutes before its 10800 second deadline and ended
without a pull request. The agent could not be given a longer deadline, because
the database check, the CLI, the worker and the chart schema all stopped at
10800 seconds.

## Decision

1. **The execution deadline is per agent, 60 to 43200 seconds.** The platform
   ceiling `MAX_EXECUTION_DEADLINE_SECONDS` goes from 10800 to 43200. Migration
   0136 replaces `agents_execution_deadline_seconds_ck` and
   `execution_requests_deadline_ck` so the database enforces the new ceiling.
   The default deadline stays 1800 seconds.
2. **The worker bounds follow.** `MAX_DELIVERY_BUDGET_S` and the chart schema
   maxima for `worker.deliveryBudgetSeconds` and
   `worker.runnerTotalTimeoutSeconds` go to 43200. The shipped chart values stay
   at 10800.
3. **The CLI accepts the new range.** `curie cluster overrides` and
   `curie local overrides` accept `--execution-deadline` from 60 to 43200, and
   the overrides JSON schema moves to v3.1.
4. **Other defaults and bounds do not change.** The platform default deadline
   stays 1800 seconds, the factory quickstart deadline stays 10800 seconds, and
   the CI wait bound stays 10800 seconds. The CI wait is a separate bound.
5. **The migration downgrade rewrites no data.** Downgrading 0136 refuses with an
   error naming the counts while any agent deadline is above 10800 seconds or any
   request deadline is more than 10800 seconds after its start. Shortening a
   running request's deadline would contradict ADR 0171's rule that a request's
   deadline is fixed at start.
6. **The publication precheck replay window grows.** The capability replay window
   of [ADR 0174](0174-publication-prechecks-use-an-execution-scoped-capability.md)
   derives from the execution deadline, so it becomes at most twelve hours.
   `CAPABILITY_TTL_SECONDS` in `apps/api/src/curie_api/issue_read.py` derives
   from the maximum and is now 45000 seconds. The capability still conveys only
   the capped read operation and still ends on expiry, lease loss or epoch
   replacement.

## Consequences

- A run of up to twelve hours needs an agent deadline of up to 43200 seconds and a
  worker delivery budget and runner ceiling at least as long. An operator raises
  all three; the request deadline still bounds each run.
- An agent left at the default is still bounded by 1800 seconds, and an install
  that changes nothing behaves as before.
- An agent held at an approval can keep its lease up to the deadline, so capacity
  can be held, and the ADR 0174 read allowance can last, up to twelve hours.
- A rollout that has to drain a long turn can take as long as the delivery budget
  per pod, as ADR 0171 states. Operators who choose a long budget accept that cost.
- Once any agent or request uses a deadline above 10800 seconds, migration 0136
  cannot be downgraded until those rows are brought back within three hours.

## Alternatives considered

**Raise the shipped defaults to 43200 seconds.** Rejected. A higher default
delivery budget lengthens the upgrade drain and the Helm timeout of every
install, including ones that never run a long factory job.

**Remove the ceiling.** Rejected. The deadline also bounds how long a stalled
agent holds capacity and how long the ADR 0174 capability window lasts, so it
needs a finite platform maximum.

**A 24 hour ceiling.** Rejected. No run has needed more than twelve hours, and a
larger ceiling widens the capacity hold and capability window with no observed
need. A later ADR can raise it.

**A data-rewriting downgrade that clamps deadlines to 10800 seconds.** Rejected.
Shortening a running request's deadline contradicts ADR 0171's fixed-at-start
rule, and a silent rewrite hides data loss. The downgrade refuses instead.

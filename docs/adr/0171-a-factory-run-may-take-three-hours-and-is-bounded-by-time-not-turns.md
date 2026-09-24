# 171. A factory run may take three hours and is bounded by time, not turns

Date: 2026-09-24

Status: Accepted

Partially supersedes [ADR 0162](0162-work-items-own-durable-execution-identity.md):
the clause that a started `ExecutionRequest` has an execution deadline exactly
1800 seconds after its start. Everything else in ADR 0162 stands.

Accepted alongside implementation under the coordinated exception in
[ADR 0102](0102-accepted-alongside-implementation-with-explicit-approval.md).
The recorded maintainer approval is issue #3071, filed by the maintainer, which
directs this change and asks for supersession through the ADR procedure. The
realizing code path is `workitems.py` (`_start_execution`), migration
`0054_execution_deadline_seconds`, the worker `delivery_budget_s`,
`runner_total_timeout_s` and `work_item_max_turns` settings, and the chart's
effective termination grace helper.

## Context

A hands-on dark factory run on 2026-09-24 showed that real factory jobs need
about three hours and far more than 20 model turns. Every bound on the path
stopped them well short of that:

- ADR 0162 fixed each execution deadline at exactly 1800 seconds, and the
  database enforced equality.
- The worker delivery budget and runner request ceiling were capped at 1800
  seconds in config and in the chart schema, so a longer deadline could not be
  used even when it was set.
- A factory run inherited the runner's generic default of 20 turns. One run
  ended with the SDK result `error_max_turns` after about 22 tool calls in
  198 seconds and was reported as `unclassified`.

## Decision

1. **The execution deadline is per agent, 60 to 10800 seconds.** An agent may
   carry `execution_deadline_seconds`. It is an operator override with the same
   ownership and the same set, clear and unchanged semantics as the per-agent
   model and thinking overrides. A null value means the default of 1800 seconds,
   so an unconfigured install behaves as it did. A started request's deadline is
   its start plus the owning agent's value at start. The database checks that
   the deadline is after the start and at most 10800 seconds later. Changing the
   agent later does not move a running request's deadline.
2. **The worker bounds follow.** The worker delivery budget and the runner
   request ceiling may each be set up to 10800 seconds. The chart renders the
   worker's termination grace as the larger of the configured grace and the
   delivery budget plus the shutdown reserve, so the ADR 0131 invariant holds
   by construction instead of failing a render when the budget grows.
3. **Time, not turns, bounds a factory run.** A work item delivery boots its
   runner with a turn budget from the worker setting `work_item_max_turns`
   (chart `worker.workItemMaxTurns`, default 1000). Other deliveries keep the
   runner default. A live sandbox
   records the turn budget it booted with. A new turn never opens on a sandbox
   booted with a different budget; the worker replaces it first. A running turn
   stays steerable.
4. **Running out of turns has its own cause.** The SDK result `error_max_turns`
   is classified `max-turns`. It is not retried, and the operator text names the
   turn budget setting.

## Consequences

- A three hour factory run needs an agent deadline of up to 10800 seconds and a
  worker delivery budget and runner ceiling at least as long. The execution
  request deadline still bounds each run; the worker values only stop cutting it
  short.
- Raising the delivery budget raises the rendered termination grace with it. A
  rollout that has to drain a long turn can take that long per pod. Operators
  choose that cost when they choose a long budget.
- The outcome text states the request's own deadline rather than a fixed 1800
  seconds.

## Alternatives considered

**Raise the fixed deadline to 10800 seconds for every request.** Rejected. A
short agent would hold capacity for three hours after it stalled, and the
default install would change behaviour without anyone asking for it.

**Declare the deadline in the bundle manifest.** Rejected for now. Bundles have
no per-run runtime knob today; the model and thinking overrides are operator
owned for the same reason. A bundle field can be added later without changing
the stored shape.

**Keep failing the chart render when grace is shorter than the budget.**
Rejected. It made every budget increase a two value change and was the bound
that blocked the factory run; deriving the grace keeps the same invariant.

**Leave the turn count at the runner default and ask operators to set extraEnv.**
Rejected. That changes every agent on the install, not only factory runs.

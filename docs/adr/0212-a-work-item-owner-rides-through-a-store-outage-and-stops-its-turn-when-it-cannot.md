# 212. A work item owner rides through a store outage and stops its turn when it cannot

Date: 2026-10-08

Status: Draft

This draft proposes extending
[ADR 0207](0207-a-delivery-owner-rides-through-an-ownership-store-outage-shorter-than-its-lease.md)
from the Valkey delivery lease to the SQL runtime heartbeat that
[ADR 0162](0162-work-items-own-durable-execution-identity.md) and
[ADR 0157](0157-factory-work-dispatches-from-sql-over-the-runs-stream.md)
give each started factory `ExecutionRequest`. It builds on
[ADR 0206](0206-a-factory-run-lost-with-its-worker-is-re-admitted-as-a-new-attempt.md).
It does not authorize implementation.

## Context

A started request has one runtime owner, identified by a runtime epoch. The
owner heartbeats the API, and each confirmed heartbeat sets
`runtime_heartbeat_expires_at` to the database commit time plus
`work_item_runtime_ttl_seconds` (default 45 seconds). The API grants a
heartbeat interval of one third of that TTL, 15 seconds.

The API ends a running request only through the reconciler:
`_request_owner_lost_cancellations` in
[`apps/api/src/curie_api/workitem_reconciler.py`](../../apps/api/src/curie_api/workitem_reconciler.py)
requests an `owner_lost` cancellation once the expiry plus one more TTL has
passed, which is 90 seconds after the last confirmed heartbeat. A request with
a succeeded publication is excluded. No second worker can take over a running
request before that: start happens once, and `claim_termination` applies only
to a request already in `cancellation_requested`.

The worker gives up much sooner, and does the wrong thing when it does.
`WorkItemRun._heartbeat_loop` in
[`apps/worker/src/curie_worker/workitem_dispatch.py`](../../apps/worker/src/curie_worker/workitem_dispatch.py)
counts heartbeat transport failures (an unreachable API or a 5xx) and, after
`_HEARTBEAT_TRANSPORT_FAILURES` (3) of them, about 45 seconds, calls the
stale owner callback. That callback, `_abandon_stale_work_item` in
[`apps/worker/src/curie_worker/kernel.py`](../../apps/worker/src/curie_worker/kernel.py),
marks the run finished and forgets it, but does not interrupt the turn. The
turn keeps running in its sandbox with no owner heartbeating it, under an
epoch the API still considers current.

`_abandon_stale_work_item` is correct for the case it was written for, a
`409 stale_owner` refusal. There another epoch owns the request, the API
already refuses the old epoch's writes, and touching the thread route could
disturb the new owner. A transport failure is a different case: the epoch is
still current, so nothing on the API side fences the unowned turn.

A factory resilience run on a disposable install, driven through a dedicated
test GitHub App and fixture repository, held Postgres unavailable for 75
seconds on 2026-10-08. The API and worker did not restart. The worker gave up
heartbeating one running request after three failures. Its turn kept running.
About one second after Postgres returned, the turn's `publish_changes` created
an auto approved publication. About two seconds after that, the reconciler
cancelled the request as `owner_lost`, because the expiry plus one TTL had
passed and the publication had not yet succeeded. No successor was admitted,
because the request's own publication owns the terminus. The publication
never ran, because claiming it requires a running request. The issue ended as
needing a person with no pull request, although the work itself was fine. Two
other runs lost in the same window had no publication and recovered through
ADR 0206 successors.

ADR 0207 consequence 6 left the truthful cause for a run that fails closed on
a store outage undecided. This ADR decides it for the SQL heartbeat.

## Decision

**A work item owner rides through heartbeat transport failures until a local
deadline that is always earlier than the API's lapse. At that deadline it
stops its turn and its sandbox instead of abandoning them, so no turn outlives
its owner. A request that ends this way records `store_unavailable`, not
`owner_lost`.**

1. A heartbeat refusal is handled as today: `stale_owner` abandons without
   touching the route, and `cancellation_requested` stops the owned run
   through `_stop_owned_work_item`.
2. A heartbeat transport failure is not ownership loss by itself. The owner
   keeps a local deadline: the monotonic time at which it sent its last
   heartbeat that the API confirmed, plus two TTLs, minus one heartbeat
   interval. With the defaults that is 75 seconds after the last confirmed
   heartbeat. The owner keeps heartbeating on the normal interval until the
   deadline passes or a heartbeat is confirmed. The heartbeat grant carries
   the TTL, so the worker does not derive it from the interval.
3. The deadline is safe without coordination. The send time precedes the
   commit time that set the expiry, so the local deadline is at least one
   interval before the reconciler's lapse. Until the lapse no other actor can
   end or take the request.
4. When the local deadline passes, the owner fences its run: it interrupts the
   runner, then halts the runtime and observes the claim and sandbox gone,
   the same chain `_stop_owned_work_item` runs today. That chain goes from the
   worker to the runner and to Kubernetes, not through the work item store,
   so it works during the outage. After the deadline no turn of that request
   is running, so nothing can publish under its epoch.
5. Publication creation for a work item request refuses a request whose
   heartbeat has lapsed (expiry plus one TTL at or before now), the same
   predicate the reconciler uses, checked in the transaction that creates the
   publication. This is the server side backstop for an owner that could not
   stop its turn, for example because Kubernetes was also unreachable.
6. Truthful cause. After fencing, the owner reports the termination with cause
   `store_unavailable` and its observation, retrying until the API answers.
   The API accepts it under the epoch the request had when it lapsed: a
   request still `running` goes straight to `failed/store_unavailable`, and a
   request the reconciler already moved to `cancellation_requested` with
   `owner_lost`, and that no terminator has claimed, settles as
   `failed/store_unavailable` instead. If another terminator claimed it first,
   `owner_lost` stands. The status comment says the work item store was
   unavailable, not that the worker stopped responding.
7. A `store_unavailable` request is followed by a successor under ADR 0206,
   with ADR 0206's refusals, and counts in the same consecutive loss streak as
   `owner_lost` (and as `sandbox_lost` if Draft ADR 0211 is accepted). The work
   did not fail on its own account, and the owner observed its sandbox gone
   before reporting.
8. Scope. The rule applies to every work item runtime heartbeat, which is one
   loop used by both run paths in the kernel. The acquire renewal between
   acquire and start is unchanged: no turn runs in that window. Interactive,
   cron and eval runs have no SQL heartbeat; ADR 0207 governs their Valkey
   lease.

When this ADR is accepted, the realizing paths are expected to be
`WorkItemRun._heartbeat_loop` and `WorkItemDispatchClient.record_termination`
in `apps/worker/src/curie_worker/workitem_dispatch.py`; a new stop callback
beside `_stop_owned_work_item` in `apps/worker/src/curie_worker/kernel.py`;
the heartbeat grant in `apps/api/src/curie_api/workitem_dispatch.py`;
`_record_runtime_termination`, `_admit_owner_lost_successor` and
`owner_lost_streak` in `apps/api/src/curie_api/workitems.py`; and
`create_publication` in `apps/api/src/curie_api/crud.py`.

### Related fixes that this ADR does not depend on

Two bug fixes from the same run are filed separately and need no ADR. The
first makes the reconciler's `owner_lost` guards exclude a request with an in
flight publication (pending, approved, launching or running), not only a
succeeded one. The second fences sandbox release by claim, so a finishing run
can only release its own claim and never a successor's sandbox on the same
thread. Each is correct with or without this ADR, and this ADR is correct
with or without them.

## Consequences

1. With the defaults, a work item store outage that ends within about 60 to
   75 seconds of its start, depending on when the last heartbeat landed, no
   longer ends healthy runs.
2. A longer outage still fails closed, and now fails closed truthfully: the
   turn is stopped, the sandbox is gone, the cause says the store was
   unavailable, and a successor reruns the work.
3. The 75 second hold in the evidence, plus the time Postgres took to become
   ready again, exceeds the default bound. That run would have ended
   `store_unavailable` with a successor, not ridden through. An operator who
   needs a longer ride through raises `CURIE_WORK_ITEM_RUNTIME_TTL_SECONDS`;
   both bounds scale with it, and so does the time to detect a worker that
   really died.
4. During the outage the owner keeps producing effects in its sandbox without
   a freshly confirmed heartbeat for up to one interval less than the lapse.
   That is the margin the two TTL lapse already pays for, as in ADR 0207.
5. A turn that finishes during the outage cannot record its finish until the
   store returns; it waits as today, bounded by the local deadline.
6. A `store_unavailable` cause is new on requests and status comments, and a
   successor follows it.

## Alternatives considered

1. Keep the three failure limit and fix only the cause. Rejected: the turn
   would still outlive its owner and could still publish into a request the
   reconciler is about to cancel, which is exactly the observed failure.
2. Keep abandoning, and rely on the reconciler guard for in flight
   publications. Rejected as the whole answer: it saves the publication in the
   observed race, but leaves an unowned turn running with a live epoch and no
   fence, which the next race will find.
3. Ride through until the execution deadline. Rejected: once the lapse passes
   the reconciler cancels the request and a successor can start, and the old
   turn would run beside it.
4. Raise the default TTL so the observed outage rides through. Rejected for
   now: it delays detection of a really dead worker and its ADR 0206 successor
   for every run, to cover one outage length. The TTL stays an operator
   setting.
5. Move the fence to the API alone (point 5 without points 2 and 4). Rejected:
   the API can refuse publication but cannot stop a turn or release a
   sandbox, so the run would still burn its deadline and leave its sandbox
   behind.
6. Infer the cause on the API from its own record of the outage. Rejected: the
   API cannot write during the outage it would need to record, and the owner
   that stopped the turn is the only party that knows why.

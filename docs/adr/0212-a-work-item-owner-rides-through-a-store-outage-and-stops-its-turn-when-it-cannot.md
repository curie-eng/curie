# 212. A work item owner rides through a store outage and stops its turn when it cannot

Date: 2026-10-08

Status: Draft

This draft proposes extending
[ADR 0207](0207-a-delivery-owner-rides-through-an-ownership-store-outage-shorter-than-its-lease.md)
from the Valkey delivery lease to the SQL runtime heartbeat that
[ADR 0162](0162-work-items-own-durable-execution-identity.md) and
[ADR 0157](0157-factory-work-dispatches-from-sql-over-the-runs-stream.md)
give each started factory `ExecutionRequest`, and deciding, for both
ownership stores, how a factory run that fails closed on a store outage, or on
losing its path to the API, ends.
That second part settles ADR 0207 consequence 6. It builds on
[ADR 0206](0206-a-factory-run-lost-with-its-worker-is-re-admitted-as-a-new-attempt.md)
and proposes amending
[ADR 0013](0013-concurrency-and-delivery-model.md) for one case only, as
point 9 states. It does not authorize implementation.

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

The Valkey side has the opposite problem: one fault, two outcomes. ADR 0207
lets a factory owner ride through a Valkey outage until a local deadline, 35
seconds after its last confirmed renewal with the defaults, through three
loops that each fail closed when that deadline passes. Two of them race on
the same deadline during a factory turn:

1. Path A, the side effect marker. `_mark_side_effect_with_retry` in
   `kernel.py` retries the marker write while ownership is held (ADR 0207
   point 4). When the local deadline passes it sets classification
   `ownership-store-unavailable` and raises. Because the turn has already
   flagged a side effect, the `saw_side_effect` branch of `_process_event`
   escalates before the retry check, as ADR 0013 requires.
   `_ESCALATION_CAUSES` has no entry for that class, so the request ends
   `runner_escalated`, the issue needs a person, and no successor follows.
2. Path B, consumer liveness. The consumer's liveness loop (ADR 0207 point 8)
   reaches its own local deadline, raises `ConsumerLivenessExpired`, and
   cancels its in flight handlers, so the kernel drops the run. The worker's
   orphan sweep, `WorkItemOrphanSweeper` in
   [`apps/worker/src/curie_worker/workitem_orphans.py`](../../apps/worker/src/curie_worker/workitem_orphans.py),
   then sees a request carrying its own consumer name that the kernel no
   longer holds, and declares it `owner_lost` through the API, which is
   reachable because only Valkey is down. ADR 0206 admits a successor.

Two factory resilience runs on disposable installs, driven the same way, each
held Valkey unavailable for about 80 seconds during a live factory turn. On
v0.12.3 Path B won: the request ended `owner_lost` and its successor
completed. On main at b5bce2e5, with no change to either path in between,
Path A won: the request ended `runner_escalated` and the issue needed a
person. Which outcome an operator gets for a Valkey outage past the ride
through bound depends on which loop observes the deadline first. Neither
cause is truthful: the worker did not stop responding, and the runner did not
fail.

The SQL heartbeat also fails with both stores healthy. A factory resilience
run on a disposable install, driven the same way, refused new worker to API
connections for 90 seconds on 2026-10-09 while Postgres and Valkey stayed
healthy. Each heartbeat is one `_post` attempt in
`WorkItemDispatchClient` with no retry, so the third refused connection
stopped the heartbeat loop and abandoned the run as above. The reconciler
then cancelled the request as `owner_lost`, and the turn's publication, which
was already in flight, succeeded 0.57 seconds later. Nothing was unavailable
except the path between the owner and the API, so a cause that names a store
outage would be as false here as `owner_lost`.

ADR 0207 consequence 6 left the truthful cause for a run that fails closed on
a store outage undecided. This ADR decides it for both ownership stores and
for an unreachable API, with one cause that names what the owner actually
knows: it was alive and could not confirm its ownership.

## Decision

**A work item owner rides through heartbeat transport failures until a local
deadline that is always earlier than the API's lapse. At that deadline it
stops its turn and its sandbox instead of abandoning them, so no turn outlives
its owner. A factory run that fails closed because its owner could not
confirm ownership, whether the API was unreachable, the SQL work item store
behind it was unavailable, or Valkey was unavailable, records
`ownership_unconfirmed`, not `owner_lost` or `runner_escalated`, and gets an
ADR 0206 successor, whichever path observes the failure first.**

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
   `ownership_unconfirmed` and its observation, retrying until the API answers.
   The API accepts it under the epoch the request had when it lapsed: a
   request still `running` goes straight to `failed/ownership_unconfirmed`,
   and a request the reconciler already moved to `cancellation_requested` with
   `owner_lost`, and that no terminator has claimed, settles as
   `failed/ownership_unconfirmed` instead. If another terminator claimed it
   first, `owner_lost` stands. The report carries what the owner observed on
   its last heartbeat failure: the API refused or timed out the connection, or
   the API answered with a server error. The status comment says the worker
   could not confirm its ownership because the API was unreachable, or because
   the API could not reach the work item store, not that the worker stopped
   responding. The cause is the same for both, because the owner cannot tell
   from a refused connection, a timeout or a 5xx which link failed, and the
   handling is identical.
7. The Valkey side converges on the same stop. For a factory WorkItem
   execution, each ADR 0207 fail closed that comes from a local deadline
   passing on transport failures, and not from a refusal, runs the fence in
   point 4 and the report in point 6. That covers the delivery lease
   deadline (ADR 0207 point 2), the side effect marker deadline (point 4)
   and the consumer liveness deadline (point 8). Concretely:
   1. The kernel maps classification `ownership-store-unavailable` to cause
      `ownership_unconfirmed` in `_ESCALATION_CAUSES`, and for a factory
      execution that class ends the request `failed/ownership_unconfirmed`
      instead of taking the `saw_side_effect` escalation.
   2. When the consumer cancels a factory handler on
      `ConsumerLivenessExpired` from its local deadline, the kernel runs the
      same fence and report rather than dropping the run.
   3. The kernel keeps holding the run, so `owns_work_item` stays true, from
      the moment it fails closed until its termination is recorded or
      refused. The orphan sweep therefore cannot declare its own process's
      run `owner_lost` while the stop is in progress. If the sweep or the
      reconciler wins anyway, for example because the worker process died,
      point 6's settlement rule applies and the outcome is still a
      successor.
   4. The fence must not depend on Valkey. It addresses the runtime by the
      claim and sandbox names the `WorkItemRun` already holds. Where the
      runner interrupt needs the Valkey route lookup (ADR 0207 consequence
      5) and that lookup fails, halting the claim stops the turn.
   5. The report goes to the API under the request's runtime epoch, which
      ADR 0162 fences in SQL and which a Valkey outage does not block. The
      cause is `ownership_unconfirmed`, as on the SQL side, and the status
      comment says the worker could not confirm its ownership because the
      delivery store was unavailable. If the API is unreachable too, the
      report retries as point 6 describes.

   A refusal (lease held by another token or generation, liveness held by
   another generation, or the key gone after Valkey returns) is ownership
   loss, not store unavailability, and keeps today's handling and cause.
8. An `ownership_unconfirmed` request is followed by a successor under ADR 0206,
   with ADR 0206's refusals, and counts in the same consecutive loss streak as
   `owner_lost` (and as `sandbox_lost` if Draft ADR 0211 is accepted). The work
   did not fail on its own account, and the owner observed its sandbox gone
   before reporting.
9. ADR 0013 and replay. A successor reruns the whole run, including every
   call that flagged a side effect, which ADR 0013 otherwise answers with
   escalation. This ADR amends ADR 0013 for factory WorkItem executions that
   end `ownership_unconfirmed` only, on the same footing as ADR 0206 already
   does for `owner_lost`. It adds no replay exposure: Path B already reruns
   these calls today under `owner_lost` on the same fault, so this ADR
   removes the race, not a safeguard. It does not decide whether replay is
   safe. That question is the one Draft ADR 0211 (pull request #4326) answers
   for sandbox loss, and its point 6 replay safety bound applies to every
   successor ADR 0206 admits. The boundary is:
   1. If Draft ADR 0211 is accepted, its replay safety bound gates
      `ownership_unconfirmed` successors exactly as it gates `owner_lost` and
      `sandbox_lost` ones, and a run outside the bound ends for a person
      with cause `ownership_unconfirmed` and the blocking tool named.
   2. Until then, an `ownership_unconfirmed` successor carries exactly the
      replay risk ADR 0206 carries for `owner_lost`, no more.

   Non factory turns (interactive, cron, eval) keep ADR 0013 unchanged: a
   turn that fails closed on `ownership-store-unavailable` after a side
   effect still escalates, and its reply names that class.
10. Scope. On the SQL side the rule applies to every work item runtime
    heartbeat, which is one loop used by both run paths in the kernel. The
    acquire renewal between acquire and start is unchanged: no turn runs in
    that window. On the Valkey side point 7 applies only to factory WorkItem
    executions, the only runs with a request to fail and a successor to
    admit. Interactive, cron and eval runs keep ADR 0207 as written.

When this ADR is accepted, the realizing paths are expected to be
`WorkItemRun._heartbeat_loop` and `WorkItemDispatchClient.record_termination`
in `apps/worker/src/curie_worker/workitem_dispatch.py`; a new stop callback
beside `_stop_owned_work_item`, `_mark_side_effect_with_retry`, the
`saw_side_effect` branch of `_process_event`, `_ESCALATION_CAUSES` and
`owns_work_item` in `apps/worker/src/curie_worker/kernel.py`; the handler
cancellation in `StreamConsumer._liveness_refresh_loop` in
`apps/worker/src/curie_worker/stream_consumer.py`; the heartbeat grant in
`apps/api/src/curie_api/workitem_dispatch.py`;
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

1. With the defaults, a work item store outage or a loss of the worker's
   path to the API that ends within about 60 to 75 seconds of its start,
   depending on when the last heartbeat landed, no longer ends healthy runs.
   The 90 second partition in the evidence exceeds that bound and would end
   `ownership_unconfirmed` with a successor, with the turn stopped before the
   lapse so its publication could not race the reconciler.
2. A longer outage still fails closed, and now fails closed truthfully: the
   turn is stopped, the sandbox is gone, the cause says the owner could not
   confirm its ownership and the status comment says why, and a successor
   reruns the work.
3. The 75 second hold in the evidence, plus the time Postgres took to become
   ready again, exceeds the default bound. That run would have ended
   `ownership_unconfirmed` with a successor, not ridden through. An operator who
   needs a longer ride through raises `CURIE_WORK_ITEM_RUNTIME_TTL_SECONDS`;
   both bounds scale with it, and so does the time to detect a worker that
   really died.
4. During the outage the owner keeps producing effects in its sandbox without
   a freshly confirmed heartbeat for up to one interval less than the lapse.
   That is the margin the two TTL lapse already pays for, as in ADR 0207.
5. A turn that finishes during the outage cannot record its finish until the
   store returns; it waits as today, bounded by the local deadline.
6. An `ownership_unconfirmed` cause is new on requests and status comments,
   and a successor follows it.
7. A Valkey outage past the ADR 0207 bound now has one outcome for a factory
   run: `failed/ownership_unconfirmed` and a successor, the outcome Path B
   reached by luck in the v0.12.3 run above. The 80 second hold in the
   evidence would end that way on either path.
8. Factory and non factory turns now differ on `ownership-store-unavailable`
   after a side effect: a factory request gets a successor, an interactive
   turn escalates. The difference is deliberate and matches the one ADR 0206
   already draws for worker loss.
9. A factory handler cancelled on `ConsumerLivenessExpired` now takes as long
   as the fence takes before the kernel lets go of it, instead of being
   dropped at once. A worker that restarts during that window leaves the run
   to the orphan sweep and point 6, as today.
10. The replay exposure of an `ownership_unconfirmed` successor is whatever ADR
    0206 allows for `owner_lost`. Accepting Draft ADR 0211 narrows both
    together; rejecting it leaves both where ADR 0206 put them.

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
7. Map `ownership-store-unavailable` to a truthful cause but keep the
   escalation. Rejected: the cause would be truthful, but the outcome would
   still depend on which path won, a person on Path A and a successor on Path
   B, for the same fault.
8. Make Path B match Path A: treat any store outage after a side effect as an
   ADR 0013 escalation, with no successor. Rejected: it is consistent, but
   every Valkey outage past 35 seconds would end every running factory run
   for a person, while a worker crash with the same replay exposure gets an
   ADR 0206 successor. The safety question belongs in one replay bound for
   every successor, which is Draft ADR 0211's point 6, not in which component
   happened to fail.
9. Leave the race to the orphan sweep, so Valkey loss always ends
   `owner_lost`. Rejected: it keeps the false statement that the worker
   stopped responding, which ADR 0207 consequence 6 set out to remove, and
   it leaves Path A's escalation in place whenever the marker loop wins.
10. Order the two paths with a shared lock. Rejected: the store that would
    hold the lock is the one that is down, and point 7's rule that the
    kernel keeps holding the run until it reports does the ordering locally.
11. Name the cause `store_unavailable`, and add a sibling such as
    `api_unreachable` for a partition. Rejected: the worker sees the same
    transport failure whether the API is unreachable or the API cannot reach
    Postgres (a timeout can be either), so it would have to guess between two
    names for one handling, and a wrong guess is the false statement this ADR
    removes. One cause with the observation in the status comment is truthful
    in every case.
12. Name the cause `owner_unreachable`. Rejected: it reads as the API failing
    to reach the worker, which is what `owner_lost` already says, and it is
    false for a Valkey outage, where the API reaches the worker and receives
    the report normally.

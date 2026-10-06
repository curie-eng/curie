# 207. A delivery owner rides through an ownership store outage shorter than its lease

Date: 2026-10-06

Status: Draft

This Draft proposes to supersede in part
[ADR 0131](0131-a-delivery-has-one-deadline-and-one-renewable-fenced-owner.md),
on one point only: the Decision sentence "If Valkey cannot confirm renewal, the
owner fails closed as lease-lost" and the matching Consequence "A transient
ownership-store outage may interrupt otherwise healthy work". Everything else
in ADR 0131 stands: one overall delivery deadline, the fenced lease as delivery
authority, fenced reclaim, fenced terminal settlement, and runs and evals
sharing one lease implementation. If this ADR is accepted, ADR 0131 gains a
back link under its Status line as
[ADR 0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md)
allows, and stays Accepted.

## Context

ADR 0131 sets a 45 second delivery lease renewed every 10 seconds, and requires
the lease to span at least three heartbeat periods.
`DeliveryLeaseStore.heartbeat_interval_s` in
`apps/worker/src/curie_worker/delivery_lease.py` documents why: "so two
consecutive missed renewals still leave a healthy owner's lease live". The
renewal loop never uses that margin. `StreamConsumer._heartbeat_lease` in
`apps/worker/src/curie_worker/stream_consumer.py` sets `lease.lost` on the
first renewal that raises, and a mid turn side effect marker write
(`markers.mark_side_effect`, called from `_apply_frame` in
`apps/worker/src/curie_worker/kernel.py`) that raises is not caught anywhere in
the turn.

A factory resilience run on a disposable kind install, driven through a
dedicated test GitHub App and fixture repository, restarted the Valkey pod once
with persistence on (AOF everysec). The stream and consumer group survived. The
headless service had no address for about 28 seconds, so every Valkey call
failed with a name resolution error until the new pod was ready. That single
restart ended two healthy factory runs, each by a different path:

1. Side effect marker. A frame needing a marker arrived 5 seconds into the
   outage. `mark_side_effect` raised `ConnectionError`, the exception left the
   turn, the consumer logged the entry as left pending, and the run was dropped
   from the kernel. The worker's own orphan sweep then declared it lost.
2. Lease renewal. 13 seconds into the outage a renewal raised
   `ConnectionError` and the lease was marked lost. Interrupting the runner also
   needs Valkey (the sandbox route lookup) and failed, the fence refused every
   terminal write, and the worker's orphan sweep declared the run lost.

Both requests ended `failed / owner_lost` with issue text saying the worker
running them stopped responding, while that worker never restarted. A run
suspended awaiting approval at the time, and every run started after Valkey
returned, completed normally. Five short restarts of the worker's other Valkey
backed tasks in the same window all recovered on their own.

During such an outage no replacement can take the delivery. Acquisition and
transfer are Valkey scripted operations, and a lease key cannot expire on a
server that is not running. Valkey persists key expiry as an absolute time, so
a second owner can appear only after Valkey returns and the lease's server side
expiry has passed. The owner can bound that moment from its own monotonic clock.

Since ADR 0131, [ADR 0157](0157-factory-work-dispatches-from-sql-over-the-runs-stream.md)
and [ADR 0162](0162-work-items-own-durable-execution-identity.md) moved factory
execution identity into SQL with its own heartbeat and epoch, which the API
fences independently. That second fence is unaffected by a Valkey outage.

## Decision

1. A renewal that Valkey refuses (wrong token, wrong generation, PEL row gone)
   is lease lost at once, as today.
2. A renewal that raises (connection error, timeout, name resolution failure)
   is not lease lost by itself. The owner keeps a local lease deadline: the
   monotonic time at which it sent its last renewal that Valkey confirmed, plus
   the lease TTL, minus one heartbeat interval. The owner keeps retrying on the
   normal interval and fails closed as lease lost only when the local deadline
   passes. With the defaults the deadline is 35 seconds after the last
   confirmed renewal, so the renewals due at 10, 20 and 30 seconds may all fail
   while the owner still holds. The one heartbeat of margin covers an AOF
   everysec restart that lost the last confirmed renewal: the server side
   expiry then falls back to the previous renewal plus the TTL, which is no
   earlier than the local deadline.
3. A renewal is timed from before the call is sent, so the local deadline is
   never later than the server side expiry.
4. A side effect marker write that raises is retried with backoff (0.5, 1, 2,
   4, then 5 seconds) until it succeeds or the local lease deadline passes. The
   frame that needs the marker is not applied until the marker is written, so
   the marker still precedes the effect. If the deadline passes, the turn ends
   with classification `ownership-store-unavailable`.
5. Terminal settlement stays fenced in Valkey as ADR 0131 requires. A turn that
   finishes during the outage waits for Valkey to settle, bounded by the
   delivery deadline.
6. A Valkey that returns without the lease key (data lost) answers the next
   renewal with a refusal, which point 1 treats as lease lost. Durable state
   loss is not ridden through.
7. Runs and evals share this rule through `StreamConsumer`, as ADR 0131
   requires of every lease change.

The realizing code path is `StreamConsumer._heartbeat_lease` in
`apps/worker/src/curie_worker/stream_consumer.py` and
`apps/worker/src/curie_worker/delivery_lease.py` for points 1 to 3 and 6, and
`_apply_frame` in `apps/worker/src/curie_worker/kernel.py` with
`mark_side_effect` in `apps/worker/src/curie_worker/markers.py` for point 4.

## Consequences

1. A Valkey restart shorter than about 35 seconds no longer ends healthy runs.
   A longer outage still fails closed.
2. The duplicate effect risk does not grow. No second owner can exist while the
   first is inside its local deadline, because the server side lease outlives
   that deadline by at least one heartbeat.
3. While Valkey is down the owner keeps producing effects without a freshly
   confirmed fence for up to 35 seconds. ADR 0131 rejected that. This ADR
   accepts it, bounded by the margin ADR 0131's own three heartbeat rule
   already pays for.
4. A side effect frame that arrives during the outage holds the turn until the
   marker is written, so a short outage shows up as latency rather than as a
   failed run.
5. Interrupting the runner on lease loss still needs Valkey for the route
   lookup, so a run that does fail closed during an outage may keep its runner
   busy until Valkey returns. That is unchanged from today.
6. The cause recorded for a run that does fail closed on an ownership store
   loss is not decided here. Today such a run is mislabelled `owner_lost`;
   giving it a truthful terminal cause is a separate change that does not need
   this ADR, and is the companion of the API outage handling in
   [#4174](https://github.com/curie-eng/curie/issues/4174).

## Alternatives considered

1. Keep ADR 0131 as it is. Rejected: every Valkey restart ends every in flight
   factory run that renews or writes a marker during the outage, and the three
   heartbeat margin is paid for and never used. A truthful cause alone would
   not save the work.
2. Ride through any outage until the delivery deadline. Rejected: once the
   server side TTL passes, a replacement can take the delivery the moment Valkey
   returns, and the old owner would run beside it.
3. Move delivery authority for factory work to Postgres, using the ADR 0162
   epoch alone. Rejected for now: a much larger change, and evals and
   interactive runs would still depend on Valkey for the same lease.

# 184. Settled stream entries are trimmed; receipts live in Postgres; backlog is counted, not rated

Date: 2026-09-30

Status: Draft

Tracked in [#1523](https://github.com/curie-eng/curie/issues/1523).

Extends [ADR-0013](0013-concurrency-and-delivery-model.md) (at-least-once
streams with consumer groups) and
[ADR-0039](0039-bounded-delivery-and-a-dead-letter-graveyard.md) (bounded
delivery and a capped graveyard). It supersedes no Accepted ADR.
[ADR-0109](0109-retention-capacity-and-back-pressure-are-one-policy.md) (Draft)
uses "retention" only for how long a thread pins its sandbox. It says nothing
about stream entries or delivery receipts, and as a Draft it authorizes nothing.
This record is the authority for those two, and leaves route retention to 0109.

## Context

ADR-0013 made Valkey Streams the transport and ADR-0039 bounded the graveyard
with an approximate `MAXLEN`. Nothing bounds the streams the graveyard drains.
No code path outside the tests ever runs `XTRIM` or `XDEL` on `curie:runs` or
`curie:evals`, and none of their producers passes `MAXLEN`: the dispatcher, the
channel and hook ingress, the resume queue, the work item reconciler, the cron
loop, the capacity wake path and the CLI all append without a cap. Every turn
from every source therefore stays in Valkey forever after it is acknowledged.

The in-chart Valkey runs `noeviction` under a ceiling of 75% of a 256Mi limit.
When the ceiling is reached Valkey refuses every write, including the locks,
markers and leases the kernel needs, and the whole install stops. The time to
failure depends only on traffic, so every install reaches it.

Channel ingress adds two more unbounded sets. A delivery receipt
(`curie:channel:delivery:<binding>:<sha16>`, and the hook equivalent) holds the
stream id with no expiry, on purpose: an expiring receipt lets a retried
`delivery_id` enqueue a second time and answer the correspondent twice. And the
ingress quota is a fixed window counter, so it limits the rate of new
deliveries, not how many are still waiting.

The spike on #1523 measured the candidate mechanisms against the pinned
`valkey/valkey:8.1.10` with the real ingress Lua:

- A receipt TTL re-admits the duplicate, settled or not.
- `MAXLEN` bounds the stream but can reach into the pending entries list. The
  pending id then has no body, `XAUTOCLAIM` drops it, and the receipt still
  answers the retry, so the turn is lost and cannot be re-enqueued. `MAXLEN`
  also never touches receipts.
- Valkey 8.1 has no acknowledgement aware trim (`XACKDEL`, `XDELEX`, or an
  `ACKED` option on `XTRIM`).
- `XTRIM MINID` at the oldest unsettled id removed exactly the settled entries.
  Pending entries stayed readable, undelivered entries were still delivered,
  and a retry of a trimmed delivery was still answered without a second `XADD`.

## Decision

**A stream keeps only what some consumer group still owes, plus a lag window.
Receipts are durable rows, not Valkey keys. Ingress refuses on unsettled
backlog, not on arrival rate.**

1. **Settled entries are trimmed by a worker maintenance pass.** For each
   stream the platform consumes through a consumer group (`curie:runs` and
   `curie:evals`, by their configured names), the worker periodically runs
   `XTRIM <stream> MINID <floor>`. A group's floor is its oldest pending id when
   its pending entries list is not empty, and otherwise the id just after its
   `last-delivered-id`. The stream's floor is the minimum over every group on
   the stream, whoever created it. A stream with no consumer group is not
   trimmed, because nothing proves any of its entries settled. The floor is read
   and applied inside one Lua script, so no delivery or acknowledgement can land
   between the read and the trim. The trim is exact, so the stream length is
   the unsettled entries plus the lag window.

2. **A lag window keeps recent settled entries.** No entry younger than
   `worker.streamRetention.minAgeSeconds` (env
   `CURIE_STREAM_RETENTION_MIN_AGE_S`, default 86400, bounded 3600 to 31536000)
   is trimmed. Age is the millisecond part of the entry id, which is the
   server's time at `XADD`. The window keeps a day of settled turns available
   to an operator reading the stream, and keeps the pass well clear of any
   delivery still inside its budget (at most 10800 seconds).

3. **Producers never trim.** No producer of a consumed stream passes `MAXLEN`
   or `MINID`. A producer cannot tell settled entries from pending ones, and a
   cap there is the turn loss the spike measured. Streams with no consumer
   group (the graveyard, progress and marker streams) keep the approximate
   `MAXLEN` each already carries; this decision does not change them.

4. **Receipts live in Postgres.** A table
   `channel_delivery_receipts(binding_id, source, delivery_sha, stream_id,
   created_at)`, unique on `(binding_id, source, delivery_sha)`, with its binding
   foreign key `ON DELETE CASCADE`, records every accepted delivery. `source`
   separates channel deliveries from hook deliveries so their id spaces never
   collide. The Valkey key keeps only the `pending:<token>` lease phase with its
   TTL. The enqueue order is: win the Valkey lease, `XADD`, insert the receipt,
   delete the Valkey key. A duplicate check reads the Valkey lease first and the
   Postgres receipt second. A receipt lives as long as its binding, which is
   the ADR-0013 promise that a delivery is answered once.

5. **Backlog is counted.** Each binding has an unsettled counter in Valkey. The
   enqueue script increments it in the same step as the `XADD`, and the
   worker's acknowledgement and dead-letter paths decrement it; the binding key
   rides on the entry as a stream field beside `payload`, so the frozen
   `QueuedTurn` does not change. Ingress answers 429 with `Retry-After` when the
   counter is at or above `channels.maxBacklog` (default 64). The worker
   recomputes each counter at boot from the entries still on the stream, which
   decision 1 keeps small, so a lost decrement cannot wedge a binding. The fixed
   window rate quota is removed when the counter ships.

The realizing path for decisions 1 to 3 is
`curie_worker.stream_retention`, a supervised worker loop beside the consumers;
`curie.queue.depth` (the runs stream's `XLEN`, already recorded every
maintenance tick) becomes the signal that the bound holds. Decisions 4 and 5 are
tracked in #1523 and ship as their own changes, each with its migration and
tests.

## Consequences

- A healthy install's Valkey footprint for streams stops growing with total
  traffic and tracks the lag window plus the backlog instead.
- A consumer group that stops acknowledging (a stuck or absent eval lane, an
  operator's inspection group left behind) pins the floor for its stream. That
  is the conservative failure: memory grows, no turn is lost. Removing an
  abandoned group is an operator action, and `curie.queue.depth` makes the
  growth visible on the runs stream.
- After the window, a settled entry is gone. A duplicate hook delivery retried
  after that is still answered as a duplicate with its stream id, but without
  the conversation id read back from the entry
  (`curie_api.routers.hooks._landed_conversation_id` already returns None for a
  trimmed entry).
- The ADR-0131 vanished probe reads "no body and not pending" as vanished. A
  settled entry older than the window reads the same way, so the probe could
  misreport one only if an owner lost its lease on an entry already settled by a
  peer and older than the window. The window's floor above the delivery budget
  is what keeps that out of reach.
- Receipts move out of Valkey memory into a table that grows with accepted
  deliveries per binding and is pruned with the binding. The ingress path gains
  one Postgres write per accepted delivery.
- A slow correspondent is no longer refused for sending steadily, and a burst is
  refused as soon as the agent falls 64 turns behind rather than after a minute
  of arrivals.

## Alternatives considered

- **`MAXLEN` at the producers.** Rejected. It bounds memory by trimming whatever
  is oldest, pending or not, which the spike showed loses turns permanently.
  ADR-0039's graveyard can use it only because nothing consumes the graveyard.
- **An expiry on receipts.** Rejected. The spike showed an expired receipt
  enqueues the duplicate, and a time window on "answered once" contradicts
  ADR-0013.
- **Acknowledgement aware deletion (`XACKDEL`, `XDELEX`).** Not available on the
  pinned Valkey 8.1. Worth revisiting when the pin moves, since it would move
  the trim into the settle path; the floor computed here remains correct either
  way.
- **`XDEL` on every acknowledgement.** Rejected. It is per entry work on the hot
  path, leaves tombstones that make group lag unknown, and is wrong with more
  than one group, since one group's acknowledgement does not settle the entry
  for another.
- **An eviction policy on Valkey.** Rejected. Eviction would pick locks, leases
  and markers as readily as stream entries, and Langfuse's queues require
  `noeviction`.
- **Keeping receipts in Valkey under a count cap.** Rejected for the same reason
  as a TTL: the receipt that falls off the cap is the one whose retry is
  answered twice.
- **Keeping the rate quota and adding the counter beside it.** Rejected. Two
  limits that measure different things refuse for reasons the operator cannot
  tell apart, and the counter alone bounds what actually costs memory.

---
seam: Queue / stream (Valkey)
kind: CLEAN
impls: 1 (redis-py) behind the broker port
grade: not separately graded
epics:
  - "#85"
  - "#7"
order: 11
---
# INTERFACE: Queue / stream (Valkey)

> Part of the Curie swappable-seam catalog — see the [seam index](../../interfaces.md).
<!-- BEGIN GENERATED: header (curie dev docs-lint) -->
> **Kind:** CLEAN &nbsp;·&nbsp; **Implementations today:** 1 (redis-py) behind the broker port &nbsp;·&nbsp; **Swap-readiness grade:** not separately graded
<!-- END GENERATED: header -->

**Kind legend:** CLEAN = a real `Protocol`/typed port class · SOFT = swap via env/URL/prefix/wire, no code interface · NONE = not built yet.

## The black line

The seam is the Valkey Stream wire contract between the dispatcher (producer) and the
worker (consumer): a named stream carrying a frozen one-field payload. As of #284 /
ADR-0027 the stream verbs are drawn behind a thin **broker port** at the two non-sacred
seams — a `StreamPublisher` `Protocol` on the producer (`apps/dispatcher/src/curie_dispatcher/queue.py::StreamPublisher`:
`xadd` + the `SET NX EX` dedupe-claim + `delete` / `release_event`) and a `StreamBroker` `Protocol` on the consumer
transport (`apps/worker/src/curie_worker/broker.py::StreamBroker`:
`xgroup_create`/`xreadgroup`/`xack`/`xautoclaim`/`xinfo_consumers`/`xclaim`/`xpending_range`/`xrange`/`xadd`).
The routing, consumer-group concurrency, dedupe, and reclaim rules stay opinionated
**core**. `redis.Redis` / `redis.asyncio.Redis` structurally satisfy the ports, so
redis-py is the one backing today with no adapter; a redis-compatible backend (Valkey,
Redis, a managed equivalent) is still a URL change, and a non-redis broker (Kafka, SQS)
is now a drop-in implementation of the two Protocols rather than a grep-and-replace of
every call site. The **second broker itself is not built** — no second-broker demand
exists (ADR-0007); only the port is extracted.

## Current contract

A second broker must honor the stream key, the payload encoding, and the Stream verbs:

- **Stream key** — `"curie:runs"`, defaulted identically on both ends:
  `DispatcherConfig.stream` (`apps/dispatcher/src/curie_dispatcher/config.py::DispatcherConfig`,
  env `CURIE_STREAM`) and `WorkerConfig.stream`
  (`apps/worker/src/curie_worker/config.py::WorkerConfig`).
- **Payload encoding** — one Stream field, `STREAM_PAYLOAD_FIELD = "payload"`
  (`apps/dispatcher/src/curie_dispatcher/queue.py::STREAM_PAYLOAD_FIELD`),
  holding `model_dump_json()`. Produced by `enqueue` via `redis_client.xadd(config.stream, fields)`
  (`apps/dispatcher/src/curie_dispatcher/queue.py::enqueue`) and reconstructed by
  `from_stream_fields` (`apps/dispatcher/src/curie_dispatcher/queue.py::from_stream_fields`)
  into a `QueuedTurn`.
- **Consumer verbs** — the worker reads with `xreadgroup` over a consumer group; the
  sacred subclass supplies the loop spec and handler
  (`apps/worker/src/curie_worker/consumer.py::Consumer._read_loop`) and the base issues the
  verb (`apps/worker/src/curie_worker/stream_consumer.py::StreamConsumer._consume`). It
  rebuilds the model at
  `apps/worker/src/curie_worker/consumer.py::Consumer._handle`, and acknowledges with `xack`
  (`apps/worker/src/curie_worker/stream_consumer.py::StreamConsumer._ack`). The group is
  `"curie-workers"` (`WorkerConfig.consumer_group`, `apps/worker/src/curie_worker/config.py::WorkerConfig`).
- **Delivery cap and dead-letter graveyard** (#505, ADR-0039) — an entry already
  delivered `WorkerConfig.max_delivery` times (default 5, floor 2) is dead-lettered
  instead of reclaimed again. `StreamConsumer._dead_letter_over_cap`
  (`apps/worker/src/curie_worker/stream_consumer.py::StreamConsumer._dead_letter_over_cap`) reads
  the pending list's delivery counts with `xpending_range` before the reclaim's
  `xautoclaim` bumps them; an over-cap entry's original fields are fetched with
  `xrange` in `StreamConsumer._entry_fields`
  (`apps/worker/src/curie_worker/stream_consumer.py::StreamConsumer._entry_fields`)
  and moved with `xadd` in `StreamConsumer._dead_letter`
  (`apps/worker/src/curie_worker/stream_consumer.py::StreamConsumer._dead_letter`), then acked off
  the group. The target stream is `WorkerConfig.dead_letter_stream`
  (`apps/worker/src/curie_worker/config.py::WorkerConfig`), defaulting to
  `"<stream>:dead"`. This writer produces rows with the original stream fields
  and `dl_original_id`, `dl_delivery_count`, `dl_reason`, and
  `dl_dead_lettered_at`. The second writer,
  `Markers.dead_letter_completion`
  (`apps/worker/src/curie_worker/markers.py::Markers.dead_letter_completion`),
  writes terminal completion-outbox rows to the same stream with `event_id`,
  serialized `completion`, `dl_reason`, `dl_delivery_count`, `dl_source`, and
  `dl_dead_lettered_at`; these rows have no `dl_original_id` and are not
  replayable as inbound stream entries. Both writers use the bounded,
  best-effort graveyard; collision escaping applies to the stream-consumer
  row family.
- **Consumer liveness and prompt reclaim** (#1532) — `XINFO CONSUMERS` idle is
  only a cheap candidate filter: it rises while a worker drains a turn or waits
  at its local concurrency limit. Before either lane reads, the narrow
  `ConsumerLivenessStore`
  (`apps/worker/src/curie_worker/consumer_liveness.py::ConsumerLivenessStore`)
  writes a renewable alive lease, then a renewable capability marker, and
  refreshes both throughout its lifetime, including graceful handler drain.
  The prompt path may transfer a pending entry only after a peer's capability
  marker is present and its alive lease is absent on two observations separated
  by a full heartbeat TTL. Alive restoration, missing peers, store errors, and
  a new consumer generation reset that proof. A live, draining, or saturated
  peer is consequently not reclaimed merely because its stream-consumer idle
  value is high.
- **Recovery ownership** — a short per-dead-consumer `SET NX PX` lease in the
  adjacent liveness store selects one replacement before `XCLAIM`, preventing
  replicas from racing through the delivery budget. A restarted generation
  also recovers rows under its own stable consumer name through the same
  pre-claim cap check before it reads new entries.
- **Lease expiry** (#2433): a pending row whose delivery state exists and whose
  delivery lease has expired is transferred regardless of whether its PEL consumer
  is alive, because a handler that raised released its lease and no prompt path
  looks at a peer that is not dead. The scan is `xpending_range` with an `IDLE`
  filter at `CURIE_LEASE_EXPIRED_IDLE_MS` (default one delivery lease TTL) and the
  claim is `xclaim` at that same min-idle, which makes the claim an atomic
  compare-and-claim on the row's idle clock. An entry with no delivery state
  carries no evidence a lease was ever granted and remains on the 900-second
  `XAUTOCLAIM` fallback.
- **Compatibility and the prompt cap rule** — an unknown/pre-marker peer keeps
  the unchanged 900-second `XAUTOCLAIM` backstop. For a proven-dead capable
  peer, the prompt path first reads `XPENDING` metadata and skips local
  in-flight IDs; it directly dead-letters an at/over-cap row before `XCLAIM`,
  even when the row is younger than the heavy reclaim window. A below-cap row
  is claimed only after the same proof. This narrow exception preserves the
  delivery bound without treating idle alone as liveness.

Idempotency lives beside the stream, not in it: `claim_event` does a
`SET <dedupe_key> 1 NX EX <ttl>` before `XADD` (`apps/dispatcher/src/curie_dispatcher/queue.py::claim_event`).

## Implementations today

One, redis-py against Valkey. The dispatcher `XADD`s
(`apps/dispatcher/src/curie_dispatcher/queue.py::enqueue`); the worker runs
a consumer group with `XREADGROUP`/`XACK`, crash-recovery `XAUTOCLAIM`,
capable-peer prompt reclaim `XINFO CONSUMERS`/`XCLAIM` (#1532), and the
delivery-cap dead-letter path's `XPENDING`/`XRANGE`/`XADD`. The runs and eval
lanes share the ordered liveness publication, sustained-absence proof, and cap
enforcement in `apps/worker/src/curie_worker/stream_consumer.py::StreamConsumer`;
an unmarked peer remains on the 900-second `XAUTOCLAIM` fallback. All stream
verbs are issued from that shared base
(`apps/worker/src/curie_worker/stream_consumer.py::StreamConsumer`);
the sacred `apps/worker/src/curie_worker/consumer.py::Consumer` subclass supplies the
specs and handlers and issues only its own pre-claim `xpending_range`
(`apps/worker/src/curie_worker/consumer.py::Consumer._pending_delivery_count`). A second, sibling stream
`"curie:evals"` uses the same one-field `payload` convention
(`apps/worker/src/curie_worker/eval/stream.py`), reinforcing the wire shape as the
real contract.

## The port (as of #284 / ADR-0027)

Drawn only at the **non-sacred** seams; the sacred concurrency kernel
(`apps/worker/src/curie_worker/kernel.py` / `apps/worker/src/curie_worker/consumer.py` /
`apps/worker/src/curie_worker/threadlock.py` / `apps/worker/src/curie_worker/markers.py`)
is not touched:

- **Producer** — `StreamPublisher` (`apps/dispatcher/src/curie_dispatcher/queue.py::StreamPublisher`): `xadd`, the
  `SET NX EX` dedupe-claim, and `delete` (the `release_event` path). `enqueue`/`claim_event`/`release_event` type against it.
- **Consumer transport** — `StreamBroker` (`apps/worker/src/curie_worker/broker.py::StreamBroker`):
  `xgroup_create`/`xreadgroup`/`xack`/`xautoclaim`, plus — since the bounded-delivery
  dead-letter path (#505, ADR-0039) — `xpending_range`/`xrange`/`xadd`, plus — since
  dead-consumer prompt reclaim (#1532): `xinfo_consumers`/`xclaim`, which the
  lease-expiry pass (#2433) reuses as an `IDLE`-filtered `xpending_range` plus an
  `xclaim` at the same min-idle. The non-sacred `StreamConsumer`
  base (`apps/worker/src/curie_worker/stream_consumer.py`) holds a `StreamBroker`; the sacred `consumer.py` subclass
  inherits it unchanged (its `XAUTOCLAIM` reclaim now targets the port by inheritance).
- **Consumer liveness store** — `ConsumerLivenessStore`
  (`apps/worker/src/curie_worker/consumer_liveness.py::ConsumerLivenessStore`)
  is deliberately a separate, narrow Redis string-key adapter for ordered
  alive/capability publication, renewal, observation, alive-lease cleanup, and
  token-checked prompt-reclaim arbitration.
  It is not an expansion of the stream-only `StreamBroker`: a different stream
  broker must either supply this limited liveness boundary or explicitly retain
  the long `XAUTOCLAIM` compatibility behavior.

The verbs return a bare `Awaitable`/value matching redis-py's own typing, so
`redis.asyncio.Redis` / `redis.Redis` satisfy the ports structurally with no adapter.

## Known leakage

- **The composition root still touches redis directly.** The client construction in
  `apps/worker/src/curie_worker/run.py` (by design) builds concrete
  `redis.Redis` / `redis.asyncio.Redis` handles and passes them into the ports.
  (The audit's companion claim that the sacred `consumer.py` calls `XAUTOCLAIM`
  directly does not hold: `consumer.py` names `XAUTOCLAIM` only in a docstring and a
  comment, and the actual call is the base's
  `apps/worker/src/curie_worker/stream_consumer.py::StreamConsumer._reclaim_once`,
  which goes through the port.)
- **Set verbs sit beside the stream on the same connection, on neither port.** The
  operator thread-reset feature (#713, #812) needs Valkey Set semantics, which
  `StreamBroker` deliberately does not cover, so the sacred consumer keeps a second,
  concretely-typed `redis.asyncio.Redis` handle onto the same connection
  (`self._valkey`) and claims a member with `EVAL` Lua (`SPOP`+`SADD` atomic,
  `_THREAD_RESET_CLAIM_LUA`) plus `SREM` on it in
  `apps/worker/src/curie_worker/consumer.py::Consumer._drain_thread_reset_requests`.
  The API half is the same shape: `SADD`/`SISMEMBER` in
  `apps/api/src/curie_api/threadreset.py::ThreadResetRequests`. A second broker that
  implements only the two stream Protocols would leave this feature unbacked; it is a
  Valkey dependency, not a stream-contract one, and no port names it today.
- **Liveness string keys are another intentionally narrow adjacent dependency.**
  `ConsumerLivenessStore` uses the worker's concrete async Redis client for its
  renewable alive/capability markers and short token-checked arbitration lease;
  generic `SET`/`EXISTS`/`DELETE` plus the EVAL Lua in
  `apps/worker/src/curie_worker/consumer_liveness.py::ConsumerLivenessStore.release_reclaim`
  (token-checked `DEL`) were
  not added to `StreamBroker`. The independent marker protocol is what lets
  prompt recovery protect a live consumer whose `XINFO` idle time is high while
  retaining a 900-second fallback for a pre-marker peer.
- **The API both writes the runs stream and reads the graveyard outside the ports.**
  Correcting an earlier claim that the API's redis only backs the kill-switch /
  eval-queue: two off-port `curie:runs` writers sit in the API. The approval-resume
  path enqueues resume turns via `ResumeQueue`
  (`apps/api/src/curie_api/resumequeue.py::ResumeQueue.enqueue`), driven both by the
  approvals router and by the expiry sweeper
  (`apps/api/src/curie_api/sweeper.py::sweep_expired_approvals`). Channel and hook
  intake enqueue via `enqueue_owned`
  (`apps/api/src/curie_api/delivery.py::enqueue_owned`), used by
  `apps/api/src/curie_api/routers/channels.py` and
  `apps/api/src/curie_api/routers/hooks.py`. Both are an `xadd` (the latter inside
  EVAL Lua) that bypasses the dispatcher's `StreamPublisher` port entirely. The API also *reads* the worker's
  `<stream>:dead` graveyard directly, with `xrevrange`/`xrange` on its own raw client:
  `apps/api/src/curie_api/graveyardwatcher.py::GraveyardWatcher` (`xrevrange` to seed the
  cursor in `seed_cursor`, `xrange` to scan in `scan_once`) and
  `apps/api/src/curie_api/resumequeue.py::ResumeQueue.read_dead_letter` (`xrevrange`),
  whose rows the #532 backstop
  (`apps/api/src/curie_api/resumereconciler.py::ResumeReconciler.reopen_dead_lettered_resumes`)
  consumes. `GraveyardWatcher` and `ResumeQueue.read_dead_letter` may each see
  both graveyard row families. The watcher alerts on every row but always
  projects the stream-consumer metadata fields `dl_original_id`,
  `dl_delivery_count`, `dl_reason`, and `dl_dead_lettered_at`, using `?` when a
  field is absent; it does not report completion `event_id` or `dl_source`.
  The resume reconciler only acts on stream-consumer rows with a `payload` and
  the expected resume metadata, so it skips completion-outbox rows. The API
  re-derives the graveyard name as `<stream>:dead` rather than reading the
  worker's config. The PEL writer already uses `StreamBroker.xadd`; a second
  broker must additionally account for the off-port completion-outbox writer,
  the two off-port `curie:runs` writers (`ResumeQueue.enqueue` and
  `enqueue_owned`), and these two API-side readers.
- **The redis-py exception surface leaks.** The ports type the verbs but not the error
  contract: `redis.exceptions` propagate through the callers unabstracted, so a non-redis
  broker must either raise redis-py-compatible exceptions or the call sites must learn its
  error types.
- **Payload vendor-neutrality (now closed on this seam).** The payload was once Slack-shaped
  by name; #7 promoted it into `packages/aci-protocol` as the channel-neutral
  `QueuedTurn`, so the queue seam no longer carries Slack-shaped field names. The queue
  seam's own remaining constraint is narrower: the contract assumes redis Stream semantics
  (ordered entries, consumer groups, pending-entry reclaim), so a swap that stays
  redis-compatible is a URL change while a non-redis broker rewrites the wire and the
  consumer verbs.

## Cross-links

- **Epic(s):** #85 — vision: make the broker itself swappable behind the stream contract
- **Epic(s):** #7 — payload promotion into `packages/aci-protocol` (overlaps the channel seam, landed)
- **Vision doc:** [architecture-vision.md](../../architecture-vision.md) — opinionated core (`curie:runs` stream), not one of the six swap jobs
- **ADR(s):** [ADR-0027](../../adr/0027-thin-broker-port-defer-second-broker.md) — the broker port at the non-sacred seams; [ADR-0007](../../adr/0007-adopt-not-build-boundaries.md) — adopt-not-build (Valkey adopted; second broker deferred); [ADR-0039](../../adr/0039-bounded-delivery-and-a-dead-letter-graveyard.md) — the delivery cap and dead-letter graveyard that added `xpending_range`/`xrange`/`xadd` to the port; [ADR-0131](../../adr/0131-a-delivery-has-one-deadline-and-one-renewable-fenced-owner.md) — an adjacent fenced-owner store for delivery liveness and the execution budget, not a `StreamBroker` verb

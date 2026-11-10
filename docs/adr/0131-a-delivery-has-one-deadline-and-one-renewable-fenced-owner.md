# 131. A delivery has one deadline and one renewable fenced owner

Date: 2026-08-27

Status: Accepted

**Superseded in part by [ADR 0207](0207-a-delivery-owner-rides-through-an-ownership-store-outage-shorter-than-its-lease.md)**

This decision was explicitly approved by a maintainer on 2026-08-27 before
implementation began. It extends [ADR-0013](0013-concurrency-and-delivery-model.md)
and [ADR-0039](0039-bounded-delivery-and-a-dead-letter-graveyard.md): at-least-once
stream delivery and the delivery cap remain, while ownership liveness and the
total execution budget become explicit distributed state.

## Context

The worker currently has two unrelated clocks. `RunnerClient` gives each runner
request a 600-second total HTTP deadline. The stream consumer may reclaim a
pending entry after 900 seconds of PEL idleness. Neither model progress nor a
live handler renews PEL ownership across worker replicas.

A controlled isolated experiment against candidate
`0f20d32cf523293ad72d4cb8b3486709251af860` observed all three boundaries. A
runner that continued to emit progress was cancelled at 600 seconds. A pending
entry was not reclaimable immediately before 900 seconds and was reclaimable
immediately after it. At that boundary a second replica entered the handler
while the original handler remained live. Raising the runner timeout alone
would therefore make the overlap larger, not make a thirty-minute delivery
safe. The detailed experiment notes remain an implementation input, not a
published architecture artifact.

The completion outbox already makes terminal transport retryable and prevents a
second runner execution in the observed acknowledged-sink path. It cannot make
a network send exactly once: a receiver may apply an effect and lose the
acknowledgement. The decision must distinguish delivery attempts from the
user-visible terminal effect.

The rollout recovery shipped for #1532 adds dead-consumer discovery and waits
out terminating workers before enqueue. That is retained as candidate discovery
and rollout hardening. Consumer disappearance alone is not authority to steal a
delivery while its renewable lease remains live.

## Decision

### One overall delivery deadline

A stream delivery has one overall deadline covering initial claim, runner
requests, retry backoff, reclaim, and terminal cleanup. The default remains 600
seconds. Operators may configure 1,800 seconds. Attempts consume the remaining
time; reclaim never starts a fresh budget.

The initial owner records the absolute deadline from Valkey server time in
internal delivery state using create-if-absent semantics. A new owner reads the
same deadline. Within one process, elapsed-time enforcement uses a monotonic
clock anchored to the last Valkey-time observation so wall-clock adjustment
cannot extend the budget. Internal delivery state is retained through the
deadline plus shutdown reserve and removed after terminal acknowledgement or
dead-letter settlement.

The first implementation exposes an operator-wide worker setting. Per-agent or
per-turn persistence would widen the API, database, UI, and deployment contracts
and requires a separate decision.

### A renewable lease is delivery authority

Owning a PEL row is necessary but not sufficient to execute or settle it. Each
`(stream, group, entry_id)` also has a short Valkey lease containing an opaque
owner token and monotonically increasing fencing generation.

The initial defaults are:

- heartbeat every 10 seconds;
- lease expiry after 45 seconds;
- reclaim scan every 10 seconds; and
- shutdown reserve of 60 seconds.

Configuration validates that the lease spans at least three heartbeat periods,
the reclaim interval is shorter than the lease, and platform termination grace
is at least the execution budget plus shutdown reserve. A configured
1,800-second budget therefore requires at least 1,860 seconds of voluntary
termination grace.

Acquisition and transfer are Valkey-scripted operations. They verify PEL
ownership, increment the fencing generation, and publish the new token before a
handler may enter. A heartbeat verifies both PEL ownership and the current lease
token, extends the lease, and resets same-owner PEL idle with `JUSTID` without
incrementing the delivery count.

Progress, tool output, model traffic, and human-facing status never renew the
lease. The heartbeat is operational and invisible to users.

### Reclaim fences before it recovers

Dead-consumer inspection and PEL scans identify candidates. A replacement may
transfer a delivery only after the lease has expired. The transfer and fencing
generation change serialize authority in Valkey. A stale owner that fails a
renewal immediately loses authority, interrupts its runner through the existing
bounded control path, and may not ACK, dead-letter, clear an outbox record, or
emit a terminal result.

Before executing reclaimed work, the replacement checks the existing
side-effect marker and retained runner:

1. A side-effect marker forbids replay and settles to human escalation.
2. A runner that still reports an active turn is interrupted and must become
   idle or disappear before retry.
3. An unreadable runner fails closed; a replacement does not run beside a
   possibly active turn.
4. Rehydration and retry are allowed only after old authority is fenced, the old
   runner is inactive, and no side-effect marker exists.

The delivery cap from ADR-0039 remains. A live lease is checked before cap
evaluation so a healthy long turn cannot be dead-lettered. Delivery count is
still PEL-backed and is not reset by heartbeat, restart, or reclaim.

If Valkey cannot confirm renewal, the owner fails closed as lease-lost. Loss of
the ownership store cannot be treated as permission to continue producing
effects.

### Terminal transport is at-least-once; terminal effect is idempotent

Terminal settlement is fenced. One atomic operation verifies the current lease,
writes the done marker and completion outbox, and identifies the winning owner.
Only that owner may attempt terminal delivery. ACK and lease release occur only
after the kernel returns a settled result.

The completion outbox retries by stable `event_id`. Exactly-once means one
user-visible terminal effect for that identifier, not one network send. A
channel adapter may claim this property only when the receiving boundary can
apply `event_id` idempotently or the adapter mutates one stable target. A local
"sent" flag cannot close the apply-success/ack-loss window and is not sufficient.
Adapters without an idempotent receiving boundary remain explicitly
at-least-once and may not advertise exactly-once terminal effect.

### Voluntary rollout drains; forced loss expires

On voluntary shutdown a worker stops taking new stream entries immediately but
continues heartbeats and in-flight processing through the delivery deadline and
shutdown reserve. On force-kill, renewal stops and a replacement becomes
eligible after lease expiry. Rollout readiness must not route new work to a
terminating worker, preserving the #1532 hardening.

The realizing code path is the shared worker stream consumer and kernel:
`apps/worker/src/curie_worker/stream_consumer.py` owns lease lifecycle for runs
and evals, `apps/worker/src/curie_worker/kernel.py` owns the overall deadline and
fenced settlement, `apps/worker/src/curie_worker/runner_client.py` enforces the
remaining request time, and channel adapters own terminal-effect idempotency.
No ACI or plugin-format wire change is authorized.
Implementation and verification are tracked in
[#1971](https://github.com/curie-eng/curie/issues/1971).

## Consequences

- A healthy delivery may run for thirty minutes without becoming reclaimable,
  while a force-killed owner becomes recoverable after at most one short lease.
- A retry or a replacement cannot multiply the configured budget. The deadline
  survives process and node changes because Valkey time, not a process-local
  monotonic epoch, establishes it.
- Voluntary worker termination may take the configured budget plus reserve.
  Operators choose that rollout cost when they choose a long execution budget.
- Valkey availability becomes part of authority. A transient ownership-store
  outage may interrupt otherwise healthy work; continuing without a fence would
  risk duplicate effects and is rejected.
- The existing side-effect marker, delivery cap, completion outbox, thread lock,
  and sandbox route keep their distinct purposes. None substitutes for the
  delivery lease.
- Runs and evals must share the lease implementation by construction. A fix on
  only one consumer lane is incomplete.
- The first configuration is deliberately operator-wide. Per-agent budgets,
  durable user controls, and human progress rendering remain outside this
  decision.
- Exactly-once network delivery remains impossible. Tests and documentation
  must name the idempotent receiving boundary before claiming exactly-once
  terminal effect.

## Alternatives considered

1. **Raise the 600-second runner timeout to 1,800 seconds.** Rejected because a
   second replica can still reclaim the entry while the first is healthy, and
   retries could multiply the total time.
2. **Raise the 900-second reclaim threshold above the maximum turn.** Rejected
   because crash and rollout recovery would then take longer than the turn.
3. **Treat progress as the heartbeat.** Rejected because quiet model work is
   valid, channel delivery can fail independently, and user-facing cadence is
   not distributed ownership authority.
4. **Reclaim immediately when a consumer disappears.** Rejected as the sole
   authority because process discovery and retained runner lifetime can diverge.
   Dead-consumer state remains a useful candidate signal behind the lease fence.
5. **Persist per-agent or per-turn budget controls now.** Rejected from the first
   implementation because it broadens API, database, UI, and deployment scope
   without improving lease safety.
6. **Record a local receipt before or after terminal send.** Rejected as an
   exactly-once claim. Before-send recording can lose the effect; after-send
   recording can duplicate it. Idempotency must reach the receiving boundary.

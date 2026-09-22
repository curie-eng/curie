# 157. Factory work dispatches from SQL over the runs stream

Date: 2026-09-19

Status: Accepted

Partially amends
[ADR 0146](0146-headless-capacity-is-a-wait-not-a-reply.md)
and
[ADR 0162](0162-work-items-own-durable-execution-identity.md).

This ADR is Accepted with explicit maintainer approval recorded on September 18,
2026 for [issue 2573](https://github.com/curie-eng/curie/issues/2573). The
coordinated acceptance follows
[ADR 0102](0102-accepted-alongside-implementation-with-explicit-approval.md).
The realizing paths are `apps/api/alembic/versions/0047_work_item_dispatch.py`,
`apps/api/src/curie_api/workitem_dispatch.py`,
`apps/api/src/curie_api/workitem_reconciler.py`,
`apps/api/src/curie_api/routers/work_items.py`,
`apps/api/src/curie_api/crud.py` (`create_publication`),
`apps/worker/src/curie_worker/kernel.py`,
`apps/worker/src/curie_worker/workitem_dispatch.py`, and
`apps/worker/src/curie_worker/sandbox/substrate.py`.

## Context

ADR 0162 persisted WorkItem and ExecutionRequest identity and transition fences.
It implemented no dispatch loop, runtime admission, or process termination. ADR
0146 decided that headless capacity is a wait, not a reply, and used the stream
pending list as that wait. Once SQL owns the request, the pending list is
transport state. A worker that cannot claim a sandbox must ACK the wake, record
the wait in PostgreSQL, and let a reconciler republish later.

Factory execute also needs a generation-scoped event id. Kernel `_complete` is
the only `mark_done` site, and `_process_event` skips a later delivery whose id
is already terminal. A stable execute id marked done by an unstarted settlement
would skip every later republish of the same request while SQL still said
`waiting`.

## Decision

**PostgreSQL owns the wait. The existing `curie:runs` stream only wakes a worker.**

Admission is an internal-worker-token route. It creates or replays the WorkItem
and ExecutionRequest, writes an immutable reply snapshot, and makes the request
dispatch-due. The waiting deadline is computed once from database now plus the
configured wait budget. A replay of the same request UUID reuses the stored
deadline.

A lifespan reconciler claims due rows under a lease, ensures the runs consumer
group exists (`XGROUP CREATE ... $ MKSTREAM`, swallow `BUSYGROUP`), `XADD`s a
`QueuedTurn` wake, then fences `published_generation`. No transaction spans
Valkey I/O. A crash between `XADD` and the fence republishes the same generation.
Acquire is idempotent for that generation even when the fence has not committed.
A missing `AgentChannel` at execute publish leaves the row due.

**D1. Execute wake ids are per dispatch generation:
`work-item-{request_id}-execute-{generation}`.** Duplicates of one generation
share an id and are fenced by SQL. The terminate wake keeps the stable id
`work-item-{request_id}-terminate` because its handler never reaches `_complete`.

**D2. Dispatch and ownership columns are fenced by their own counters, not the
ADR 0162 row `version`.** `dispatch_generation`, `dispatch_epoch`, `runtime_epoch`,
and the acquire owner fence ownership. Updates that touch only those columns do
not advance `version` and lock only `execution_requests`. Lifecycle transitions
keep WorkItem-first lock order and version CAS.

**D3. The SQL row, not the pending list, is the wait; a deferred wake is ACKed.**
On `CapacityExhaustedError` the worker calls `defer` (same request, same waiting
deadline, `capacity_deferrals + 1`, `dispatch_generation + 1`) and returns so the
consumer ACKs. Interactive capacity refusal is unchanged. Expiry is the SQL
terminal row with cause `capacity_wait_expired`, plus a WARNING log. There is no
graveyard entry for capacity expiry.

**D4. `schema_min` rises to 0046.** The API lifespan task reads 0046 columns. An
image must not boot against a 0044 or 0045 database.

**D5. Execution ends at the execute wake's first terminal settlement.**
`delivered` and `awaiting-approval` record `completed`. `escalated` records
`failed`. An approval resume turn is an ordinary conversation turn, not part of
the ExecutionRequest.

A lost runtime owner is marked `cancellation_requested` with cause `owner_lost`.
Termination is observed absence of the write-once claim and sandbox names stored
at `start`. The observation is required to fail an `owner_lost` request. A
terminate wake is built from the SQL snapshot only; a removed `AgentChannel` does
not block termination. This change does not authorize ownership migration.

`create_publication` refuses a new publication when the conversation's WorkItem
is cancelled, its active request is `cancellation_requested`, or a running
request's id and runtime epoch do not match the caller. Replays already adopted
are untouched.

## Consequences

Factory work can wait for capacity across process restarts without holding a
stream pending entry. Operators see expiry as `expired` /
`capacity_wait_expired` on the internal request read. A default install still
bounds the runner by the worker delivery budget (600 s), so a work item may fail
with `deadline_halted` before the 1800 s execution deadline unless that budget is
raised.

ADR 0146 decisions 1 and 4 are partially amended: admission still happens before
claim, but the durable wait and expiry live in SQL rather than the pending list
and graveyard. ADR 0162's "no dispatch loop, runtime admission, process
termination" scope and its cancellation cause set are realized and extended here
with `owner_lost`.

## Alternatives considered

### Keep the pending list as the wait (ADR 0146 as written)

Rejected. The pending list cannot preserve one request identity, one waiting
deadline, or a sequence of deferrals across worker restart.

### Pre-claim quota probe

Rejected. It is a check-then-act race against claim-time debounce, has no Docker
equivalent, and is unnecessary once claim-time `CapacityExhaustedError` defers
into SQL.

### Stable execute event id

Rejected. Unstarted settlements would mark the id done and skip later republishes
while SQL still said `waiting`.

### Graveyard expiry

Rejected. Capacity wait expiry is a SQL terminal row with a WARNING log and
metric, not a dead-letter graveyard entry.

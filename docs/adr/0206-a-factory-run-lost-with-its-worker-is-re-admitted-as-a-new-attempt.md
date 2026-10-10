# 206. A factory run lost with its worker is re-admitted as a new attempt

Date: 2026-10-06

Status: Accepted

Amends [ADR 0157](0157-factory-work-dispatches-from-sql-over-the-runs-stream.md)
D5 and builds on
[ADR 0162](0162-work-items-own-durable-execution-identity.md).

The maintainer explicitly approved this decision on 2026-10-06; the approval is
recorded on issue #4168. It is Accepted alongside its implementation under
[ADR 0102](0102-accepted-alongside-implementation-with-explicit-approval.md).
Realized by `_record_runtime_termination` in
`apps/api/src/curie_api/workitems.py`, which admits the successor request in the
transaction that settles a request `failed/owner_lost`, and by the
`owner_lost_retry` column on `execution_requests`.

## Context

ADR 0157 D5 fails a request whose runtime owner is lost (`owner_lost`), and
nothing re-runs the work. A worker restart (rollout, eviction, crash) therefore
fails every running factory request, and a person has to relabel each issue.
In a 2026-10-06 test install one worker deletion failed 7 healthy runs within
33 s. The run cannot be resumed in place: the runner interrupts the turn when
the worker's stream disconnects, and ADR 0162 gives a started request exactly
one start and one execution deadline.

## Decision

**A request that ends `failed/owner_lost` is followed by a new request on the
same WorkItem, up to 3 owner_lost requests in a row. The third fails the
WorkItem for a person as today.**

1. The lost request keeps today's path unchanged: `cancellation_requested`
   with `owner_lost`, the terminate chain, observed absence, then
   `failed/owner_lost`. Its sandbox is gone before anything new starts.
2. When that request settles, the API admits a successor `ExecutionRequest`
   on the same WorkItem in the same transaction (sequence + 1, `waiting`, a
   fresh waiting deadline), as a relabel readmit does, with
   `owner_lost_retry = true` recorded on it.
3. The successor starts from the beginning of the work: a new sandbox from the
   WorkItem's repository snapshot and canonical conversation. There is no
   durable mid-run checkpoint to resume from, so no phase resume is attempted.
4. Count = consecutive owner_lost terminals on the WorkItem, read from its
   requests in sequence order. At 3, no successor is admitted and the status
   comment reads "the worker running this request stopped responding 3 times".
   Reason for 3: one rolling upgrade plus one unplanned crash during a single
   run still completes, while a run that itself kills its worker stops after
   about three attempts instead of looping.
5. Idempotency: execute wake ids are per dispatch generation (ADR 0157 D1) and
   the successor is a different request, so a redelivered wake for the lost
   request is refused by `acquire` and ACKed. Publication refuses the lost
   request's epoch as it does today.
6. Issue cancellation, a closed lineage, and a request waiting on publication
   are never re-admitted.

## Consequences

A worker restart costs the elapsed run time, not the run. Model spend for the
lost attempt is repeated. Operators see a second request on the WorkItem and a
status comment that names the retry. The issue reader is involved only after
the third loss.

## Alternatives considered

### Return the lost request to waiting

Rejected. It gives a started request a second start and execution deadline,
which ADR 0162 forbids, and reuses a row whose termination is already
observed.

### Reattach the new worker to the running turn

Rejected for now. It needs the runner to run the turn apart from the HTTP
stream, keyed by event id and runtime epoch, plus a reattach endpoint and a
claim ownership hand-off. That is the better end state and deserves its own
ADR once retries exist.

### Fail as today and rely on the relabel

Rejected. Every worker restart becomes manual work on every running issue.

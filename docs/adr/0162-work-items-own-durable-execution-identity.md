# 162. WorkItems own durable execution identity

Date: 2026-09-18

Status: Accepted

Partially amended by [ADR 0157](0157-factory-work-dispatches-from-sql-over-the-runs-stream.md), which realizes dispatch, runtime admission and termination.

Partially superseded by [ADR 0171](0171-a-factory-run-may-take-three-hours-and-is-bounded-by-time-not-turns.md): the execution deadline is the owning agent's value from 60 to 10800 seconds, default 1800, not exactly 1800 seconds.

Partially amends
[ADR 0145](0145-a-labelled-issue-is-a-backlog-item-and-the-stream-is-its-queue.md)
and
[ADR 0146](0146-headless-capacity-is-a-wait-not-a-reply.md).

This ADR is Accepted with explicit maintainer approval recorded on September 18,
2026 for [issue 2573](https://github.com/curie-eng/curie/issues/2573). The
coordinated acceptance follows
[ADR 0102](0102-accepted-alongside-implementation-with-explicit-approval.md).
The realizing paths are `apps/api/src/curie_api/models.py`,
`apps/api/alembic/versions/0046_work_items.py`, and
`apps/api/src/curie_api/workitems.py`.

Signed GitHub issue intake that calls this service is
[ADR 0161](0161-signed-github-issue-events-admit-one-work-item.md).

## Context

The tracker remains the backlog and the place where people author issue content.
Curie still does not poll the tracker or copy issue bodies, acceptance criteria,
or tracker task state into its database. Once trusted intake admits work,
however, a stream delivery does not provide enough durable identity for waiting,
replay, cancellation, deadlines, or a later execution request.

ADR 0145 treated the stream as the queue for admitted work. ADR 0146 used the
presence of a human reply route to identify the headless lane and used the stream
pending list as its durable wait. Those choices cannot identify one admitted
issue across process restarts or preserve a sequence of bounded execution
requests against the same issue and pull request.

## Decision

**One trusted GitHub repository and issue identity owns one durable `WorkItem`.
Each attempt to execute that work owns one bounded `ExecutionRequest`.**

The `WorkItem` stores Curie's execution identity and lifecycle only. It records
the trusted repository and issue identity, installation provenance, agent,
repository snapshot, canonical conversation, cancellation state, sequence
allocator, and one optional publication lineage. It is not another issue record
and stores no issue body, acceptance criteria, dependency graph, readiness
judgment, task verdict, or model judgment.

One `WorkItem` may own sequential `ExecutionRequest` records, with at most one
active request. Each request has an immutable waiting deadline. A started request
has one start time and one execution deadline exactly 1800 seconds later.
Capacity waiting keeps the same request identity, sequence, and waiting deadline.
It cannot reset either deadline. Every mutation compares the caller supplied
expected version with the stored version, and every state transition advances the
version of the row it changes. Completion, failure, expiry, and cancellation are
execution lifecycle facts. None says that the ticket itself was correct or
complete.

Cancelling a waiting request makes it terminal immediately. Cancelling running
work, whether because the issue was cancelled or the execution deadline elapsed,
moves it to `cancellation_requested`. It remains active until the future runtime
owner supplies a real termination observation. Issue cancellation is sticky on
the `WorkItem`. Once cancellation is requested, completion and publication are
refused. A linked publication lineage remains linked, so cancellation retains the
pull request rather than clearing or replacing it.

The optional publication lineage is unique and can be linked once. This keeps
one ticket to one pull request while allowing another authorized execution
request against the same work. Cancellation retains the link. Publication
approval remains governed by
[ADR 0147](0147-publication-approval-is-a-per-agent-operator-policy.md), which
this decision does not amend.

For factory work, explicit `WorkItem` identity determines the lane, execution,
waiting deadline, and capacity outcome. The stream pending list is transport
state rather than the durable waiting authority. Capacity waiting consumes no
execution retry, emits no terminal success, and ends in execution or visible
expiry, preserving the remaining guarantees of ADR 0146.

A configured factory label is the intake convention that requests initial
admission. It is not itself authorization authority. The trusted intake boundary
decides authorization before calling this service. A later request requires an
explicitly authorized mention revision. Ordinary issue comments do not admit
factory work until issue 2574 defines that intake. The internal service receives
trusted normalized facts. It does not inspect labels, mentions, comment authors,
signatures, or GitHub authorization.

The platform owns durable lifecycle facts and retained evidence. The bundle owns
verification judgments about the issue and its acceptance criteria. A completed
request records execution state and does not manufacture a bundle judgment.

This decision takes only the narrow identity boundaries previously discussed in
the absent proposals ADR 0148 and ADR 0150. It does not accept either proposal
as a whole. ADR 0149 is not Accepted and supplies no model authority. Fixed
model selection per agent is the entire model scope for version 0.10. This
boundary does not reject future model routing under separate authority.

[ADR 0165](0165-the-tracker-owns-dependencies-and-curie-admits-ready-work.md)
remains Draft and deferred. This foundation adds no dependency observations,
dependency admission, readiness checks, stacked publication, prerequisite
assembly, tracker reconciliation, or fields anticipating those features.

This foundation persists records and transition fences only. It implements no
dispatch loop, runtime admission, process termination, or claim that a runtime
was stopped.

## Consequences

Curie can fence replay, concurrency, cancellation, deadlines, and publication
lineage in PostgreSQL while the tracker remains authoritative for the work
people authored. A process restart does not erase which issue is waiting or
which execution request owns the next transition.

The platform now owns admitted execution lifecycle state and must preserve its
database constraints and version checks. Transport state can be repaired from
that identity, but it cannot silently create another request or change immutable
deadlines.

## Alternatives considered

### Keep the stream as the durable authority

Rejected. The pending list cannot express canonical issue replay, sticky
cancellation, sequential requests, or a retained publication lineage.

### Copy the tracker record into Curie

Rejected. The tracker remains the backlog. Copying issue content or task
judgments would create the competing authority that ADR 0145 sought to avoid.

### Add dependency and stacked publication state now

Rejected. ADR 0165 is Draft and supplies no implementation authority.

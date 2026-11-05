# 269. Factory failure retries share a bounded fresh request allowance

Date: 2026-11-05

Status: Accepted

## Context

A recoverable factory failure normally needs an operator to readmit the work.
[ADR 0157](0157-factory-work-dispatches-from-sql-over-the-runs-stream.md)
D5 ends execution at terminal settlement. Its exception,
[ADR 0206](0206-a-factory-run-lost-with-its-worker-is-re-admitted-as-a-new-attempt.md),
admits fresh requests after owner loss and counts consecutive owner losses.
Neither authorizes a general automatic successor after another failure.

Counting owner loss separately from other failure retries would multiply their
allowances. Counting only consecutive losses would also replenish owner-loss
authority whenever another cause intervened. A shared finite allowance needs
to replace that counting rule, even though the fresh-request shape is unchanged.

The maintainer authorized acceptance and merge of a necessary retry amendment
on 2026-11-05: "if we really need one then go ahead and file and merge it."
The exact contradiction is the terminal-only rule in ADR 0157 and the
consecutive-owner-loss counting rule in ADR 0206. Implementation is tracked
in #5424 through #5431 and remains blocked until its prerequisites pass.
Acceptance supplies design authority, not a claim of implemented recovery.

## Decision

**One factory WorkItem has at most two automatic failure successors after its
initial implementation attempt. Owner loss spends that same allowance. Each
successor is a new bounded request, starts fresh and receives fresh deadlines;
it never resumes a failed request or selects a recovery checkpoint.**

### Request identity, allowance and time

1. A successor has a distinct ExecutionRequest identity and sequence. The old
   terminal request and its outcome remain immutable. Admission reserves one
   successor slot under the WorkItem lock and version compare-and-set, in the
   predecessor's settling transaction. A due time and optional typed condition
   defer dispatch of that admitted waiting request. Repeated
   settlement, wake delivery, relabeling, another failure class and process
   restart cannot replenish this WorkItem-lifetime automatic allowance.
   This feature supplies no override once it is exhausted. Existing separately
   authorized readmission is not an automatic allowance reset.
2. An owner-loss successor retains observed teardown before admission. There
   is no separate batch of owner-loss successors. A mixed failure sequence
   exhausts the same two slots as repeated owner loss.
3. Each successor has a fresh immutable waiting deadline using the configured
   allowance, currently 86400 seconds by default. At its own start it receives
   one execution deadline from the configured agent allowance, currently 1800
   seconds by default with a 43200-second ceiling. No existing request's
   waiting or execution deadline is renewed. The waiting deadline begins at
   successor admission and includes its backoff and condition wait. A condition
   expiry names that condition; it is not reported as capacity expiry.
   The fresh execution allowance is
   an explicit planning assumption consistent with the maintainer's ruling
   on fresh waiting allowances.
4. Capacity and structural workspace waits keep their existing request,
   deadline and uncounted-start behavior. The five counted start failures are
   not replenished by an automatic successor. Waiting holds no sandbox.
5. Factory full-implementation failure retries have one durable owner. They
   must not multiply the kernel's in-delivery retry count by the fresh-request
   allowance. Interactive turn retry policy is unchanged. Existing bounded
   review and CI continuation rounds remain separate workflow turns, not
   permission for another full implementation attempt; their used counts
   survive successors. This definition of an attempt is a planning assumption.
6. There is no new per-item dollar cap or reservation subsystem in this
   decision. That is an explicit assumption: existing agent budget checks
   continue to apply, and a Curie budget refusal stays terminal. Existing usage
   estimates are not a newly claimed hard spend guarantee.

### Failure disposition and authority

1. Factory policy selects retry with backoff, wait for a verified condition,
   publication-only recovery or terminal disposition. The platform enforces
   the allowance, current authority, isolation and versioned transitions.
   Unknown causes and unproved conditions cannot default to retry-now.
2. Preserve cancellation, issue deletion, newer-revision precedence, closed
   lineage, current admission and publication ownership fences. A refusal such
   as no_pull_request is not authority to try again.
3. [ADR 0013](0013-concurrency-and-delivery-model.md)'s side-effect refusal
   remains for ordinary failed turns. The worker must send a required typed
   safety fact to terminal settlement: no effects, flagged or uncertain. Only
   proved no-effects permits a new ordinary-failure successor. Cause alone
   is insufficient: a model-unreachable failure can follow an external effect.
   Missing safety never means clean, and every caller must migrate.
   Owner loss retains ADR 0206's existing teardown-based exception: that path
   has no complete side-effect fact today and is not newly claimed effect-free.
   Its only change here is the common allowance. No new cause inherits that
   exception. The kernel guard and the unaccepted sandbox-loss proposal in
   ADR 0211 are otherwise unchanged.
4. A typed provider reset can schedule a wait. A relevant configuration change
   and successful probe can release a condition gate. Display prose, a generic
   HTTP 403, an unrelated rollout and an unconfirmed runner stop cannot grant
   a retry. An unchanged limit that caused exhaustion does not grant a fresh
   execution merely because a successor would have a fresh deadline.
5. Durable scheduling precedes terminal transcript expiry when authorized
   continuation exists. Status shows waiting or publication recovery until
   final disposition, then reports the cause and exhausted bound once. A
   historical failed request is never rewritten as successful.

### Retained publication candidates

A publication candidate is private data, not an approval or a recovery
checkpoint. Retain its exact base, patch, digest and publication intent before
expensive validation, through authorized recovery and for 86400 seconds after
the owning request's final disposition. The 24-hour retention is the planning
recommendation the maintainer delegated to the author. Storage is bounded and
collected independently of claim-owned objects and authority-bearing rows.

Publication-only recovery reconciles existing publication, branch and PR facts
before a write and runs no model turn. Current repository, base, validation,
approval and lineage policy still apply. Existing publication reconciliation
limits cannot be reset by restart or replay. Recovery that needs new request
authority admits a distinct request within the same successor allowance and
uses its actual normal runtime lifecycle. A typed publication-recovery request
starts through ordinary acquire/start, receives a fresh runtime epoch and boots
the actual validation runtime. It never revives an old epoch,
copies an old approval or fabricates a running request. Missing bytes,
ambiguous writes, changed facts and policy refusal do not buy an implementation
restart. A new request may use a fresh validation sandbox without a model turn.
The current per-agent approval policy applies unchanged. An approval-gated
agent creates a fresh approval and waits within its finite authority; acceptance
fixtures use an agent explicitly configured for automatic publication.

Retention after denial, cancellation or expiry permits no automatic publication.
Checkpoint retention and explicit restoration under
[ADR 0251](0251-a-failed-factory-run-keeps-a-private-recovery-checkpoint-that-only-explicit-restoration-reads.md)
and #5146 are unchanged. Automatic successors never read those checkpoints.

### Exact amendment and delivery boundary

This partially amends ADR 0157 D5 only to permit a separately admitted,
bounded failure successor after a terminal request, and ADR 0206's Decision
and point 4 only to replace consecutive-owner-loss accounting with the shared
allowance. Its teardown, fresh-sandbox, identity and cancellation guarantees
remain. The rest of both ADRs stands.

It preserves ADR 0013's side-effect boundary, ADR 0162's sequential identity and
single deadlines, ADR 0251's explicit checkpoint selection, and ADR 0222's
separation of bundle workflow policy from generic execution mechanisms.
Approval and exact-head lineage authority under ADR 0125, ADR 0143 and ADR 0147
are not relaxed. Draft ADR 0233 concerns lost database ownership and supplies
no permission to infer missing authority here.

Realization belongs in #5424, the successor-policy and scheduling work beside
`apps/api/src/curie_api/workitems/lifecycle.py` and `workitem_dispatch.py`, the
factory's owned policy, and worker kernel failure settlement. #5425 supplies the
standalone optional ACI carrier; #5426 supplies typed provider conditions;
#5427 supplies effective resource gates; #5428 and #5429 supply private
publication candidates and their consumer; #5430 supplies read-only CI
observation recovery; and #5431 supplies an authenticated operator retry
operation. The corresponding API, worker,
runner and CLI tests must drive these paths with real backing services.
No new frozen ACI field lands from a dependent lane; a compatible metadata
carrier requires its own reviewed contract change first.

## Consequences

Recoverable failures can proceed without tracker-label manipulation, while
mixed owner-loss and transient failures remain bounded. Fresh deadlines can
increase total elapsed time across attempts; the shared count bounds automatic
full restarts. A final publication failure need not discard its private data,
but retained data cannot override current authority.

The implementation needs durable counter, due-time and condition state,
idempotent successor admission, candidate collection and truthful notices.
Acceptance of this ADR supplies design authority only. It supplies no production
deployment, live-provider spend grant or runtime proof.

## Alternatives considered

1. Keep relabeling every failed issue. Rejected because known transient
   failures remain operator work and completed publication candidates are lost.
2. Stack a general retry allowance on the existing owner-loss allowance.
   Rejected because mixed causes multiply full implementation attempts.
3. Return a failed request to waiting or share a deadline across requests.
   Rejected because each started request retains one immutable execution
   deadline and the maintainer chose fresh waiting allowances for successors.
4. Resume a checkpoint automatically or continue after reconciled side effects.
   Rejected for new ordinary-failure successors. Explicit checkpoint selection,
   the ordinary side-effect refusal and the existing owner-loss exception
   remain; broader continuation needs separate authority.
5. Add the draft's per-item dollar reservation scheme. Not selected: only the
   retry count was approved, and this change preserves existing budget checks.
6. Rerun implementation after every publication failure. Rejected because an
   exact retained candidate can be verified and published without another
   model run, provided current authority and finite allowances still permit it.

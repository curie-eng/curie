# 237. Ordinary hook receipts preserve policy after stream retention

Date: 2026-10-19

Status: Draft

This proposal is not authorization to implement. It supersedes no Accepted ADR.
It complements the unaccepted receipt migration in
[ADR 0184](0184-settled-stream-entries-are-trimmed-receipts-live-in-postgres-backlog-is-counted.md)
and preserves the separate authority in
[ADR 0191](0191-protected-hook-delivery-authority.md).

## Context

Ordinary hook admission writes a permanent delivery claim containing only the
stream ID. A completed duplicate reads the original queue entry to verify its
selected tool policy. The conditional read-only adoption specification requires
409 when that entry is unavailable, even when both requests omit a restriction.
The worker's settled stream retention can remove that entry while preserving the
claim. Deduplication remains correct, but a faithful retry can no longer obtain a
successful receipt. Deleting the claim would instead permit a second execution.

ADR 0184 proposes moving receipts to Postgres but remains Draft. It neither
authorizes that migration nor specifies the retained tool policy. A second
independent database write after XADD also needs a defined crash recovery
protocol before it can establish an immutable receipt.

## Proposed decision requiring maintainer acceptance

Preserve a bounded ordinary hook receipt beside the existing permanent claim in
the same Valkey authority, atomically with the successful owned enqueue. The
receipt records a format version, original stream ID, original conversation ID,
and original selected tool policy, including explicit null. Its namespace is
agent, hook and delivery digest, matching the claim. It contains no request body,
headers, credentials or execution transcript. It has the same indefinite lifetime
as the existing claim. This proposal does not move receipts into Postgres or
change quota accounting.

A duplicate first authenticates through the existing current source gates. A
valid retained receipt must agree with the existing claim's stream ID. It may
attest acceptance only when the retry's selected policy matches the recorded
policy. Policy mismatch, malformed metadata or disagreement with the claim
returns 409 without writes. Queue retention does not remove this metadata.

For a legacy claim without metadata, retain the existing original queue lookup.
If that entry is already missing, continue returning 409. Never infer policy from
the retry, synthesize a new delivery ID, remove the claim or silently re-enqueue.
Ordinary pending retries remain 202, restricted pending retries remain 409, and
ordinary receipts never attest a protected source generation or protected runtime
qualification. Returning a duplicate does not schedule another investigation of a
still firing alert. Periodic investigation needs a separately accepted trigger
contract; this proposal never changes the stable delivery identity. Protected admission and source activation exclusion stay unchanged.

The realizing paths after acceptance are `curie_api.delivery.enqueue_owned` for
atomic claim, enqueue and receipt persistence, and
`curie_api.routers.hooks._duplicate_receipt` for strict receipt verification.
A hook-specific optional metadata write must not alter channel ingress callers.
The adoption specification must be amended before tests and implementation.

## Acceptance tests for a later implementation

1. An authenticated ordinary delivery followed by acknowledgement and production
   settled retention returns its original accepted receipt on a faithful retry,
   while the stream stays empty and the permanent claim stays unchanged.
2. A restricted ordinary delivery behaves the same, while both policy mismatch
   directions return 409 before and after retention.
3. Missing legacy evidence, malformed metadata, claim disagreement, pending claims,
   invalid signatures and protected source transitions preserve their refusals.
4. Concurrent winners, expired leases, lost enqueue responses and successor claims
   cannot create an orphan successful receipt, relabel a policy or enqueue twice.
5. Channel ingress, protected delivery and shared enqueue regressions remain green.

The explicit reproduction is
`uv run pytest apps/api/tests/hook_receipt_retention_proposal.py -q`.
It is outside the default test filename pattern because its proposed successful
retry assertion intentionally fails until this decision is accepted and realized.
The existing fail-closed tests remain unchanged.

## Consequences

Receipt metadata increases permanent Valkey storage by a small bounded record per
accepted hook delivery. The existing indefinite receipt lifecycle remains a
separate capacity concern tracked by #1523. Legacy trimmed receipts cannot be
reconstructed. A mixed-version rollout needs careful legacy fallback and must
never treat absence of metadata as proof of unrestricted policy.

## Alternatives considered

- Keep returning 409 after supported retention: safe and the current contract,
  but a faithful retry cannot distinguish this from a policy conflict.
- Return success from the retry's policy or merely from the claim: rejected,
  because the original policy is not proven.
- Expire or delete claims, or mint new delivery IDs: rejected, because these
  permit repeat execution and weaken the original delivery identity.
- Retain all queue bodies: rejected, because completed payloads would defeat
  settled stream retention and retain more data than the receipt needs.
- Move receipts to Postgres now: deferred to ADR 0184's separate acceptance and
  crash consistency design; this proposal retains the existing atomic authority.

## Implementation stop point

A maintainer must explicitly accept the durable ordinary receipt contract and
publish this ADR as Accepted before receipt storage or duplicate handling changes.
The Draft and reproduction provide no permission to change existing refusals.

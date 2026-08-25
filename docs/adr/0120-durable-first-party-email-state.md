# 120. First-party email delivery state is local durable single-writer state

Date: 2026-08-25

Status: Accepted

Accepted with explicit maintainer approval on 2026-08-25 for issue #1515 and
published alongside its implementation under
[ADR-0102](0102-accepted-alongside-implementation-with-explicit-approval.md).
Realizing code paths are `apps/mail-adapter/src/curie_mail_adapter/state.py` for
the SQLite transaction and recovery boundary,
`apps/mail-adapter/src/curie_mail_adapter/adapter.py` and
`apps/mail-adapter/src/curie_mail_adapter/egress.py` for durable ingress and
reply ownership, and `charts/curie/templates/mail-adapter.yaml` plus
`charts/curie/templates/mail-adapter-persistence.yaml` for the one-writer RWO
deployment.

## Context

The first-party email adapter owns two at-least-once boundaries. It polls an
AgentMail inbox and submits a stable provider message id to Curie's channel
ingress. In the other direction it receives streamed reply events and turns one
completion into one correspondent-visible email. The upstream and downstream
systems each have durable idempotency evidence, but the original adapter kept
the relationship between them only in process memory.

That memory-only boundary loses facts on an ordinary replacement. Mail arriving
while the pod is down can be mistaken for first-start history and primed away. A
202, 429, 401, server error, or ambiguous transport result can be treated as a
finished ingress attempt even though the platform did not accept it. Two turns
in one mail thread can share or clear conversation-global reply text. A crash
after AgentMail accepts a reply but before local acknowledgement can resend the
same email. Pinning one replica prevents concurrent maps from disagreeing; it
does not make those maps survive.

Giving the adapter the platform Postgres or Valkey credential would make the
facts durable, but it would also turn a channel-edge service into a platform
data-plane principal. The adapter deliberately has only its scoped channel
token, egress secret, and AgentMail key. Preserving that least-capability
boundary is more important than reusing a central store.

## Decision

### 1. One adapter process owns one local SQLite database on one RWO volume

The email adapter is one serialized SQLite writer. The chart hardcodes one
replica and `Recreate`, mounts either a chart-managed ReadWriteOnce PVC or an
operator-supplied same-namespace RWO Filesystem claim, and mounts a separate
writable `/tmp` under a non-root, read-only-root filesystem. The runtime pod has
no ServiceAccount token, Kubernetes RBAC, platform database credential, or
platform API key.

SQLite uses transactional writes, WAL recovery, full synchronous durability,
and one adapter-owned lock for poller and HTTP-handler access. The state path and
capacity bounds are operator configuration; replica count and access mode are
not. A second writer is unsupported even when a storage implementation happens
to permit multi-attach.

### 2. The database records ownership and recovery facts, never credentials

Durable rows bind the stable inbound `delivery_id` to its provider message,
thread and `reply_ref`; record the security decision and retry/floor state;
distinguish first-start prime from restart confirmation; own accumulated reply
text at `(conversation_id, reply_ref)`; and record completion leases and terminal
event receipts. Pending work is admitted before provider mail is considered
seen. At count or byte capacity the adapter leaves new provider mail recoverable
instead of evicting unresolved work.

The database contains none of the AgentMail key, scoped channel token, egress
secret, platform API key, platform database credential, or Kubernetes token.
It is nevertheless PII-bearing application data because identifiers, addresses,
message/reply text needed for recovery, and delivery receipts may be present.

### 3. Ingress settles only on terminal success

Every attempt for one upstream message retains the same `delivery_id`.
Transport ambiguity, 202, 429 with `Retry-After`, 401, and server errors remain
pending across retries and replacements. A documented 200 receipt, including a
duplicate receipt for the same id, is terminal. An expired scoped token is an
operator rotation: the adapter has no authority to mint one, and replacement
reopens the same pending row.

A new database primes the current inbox once before Ready so enabling an old
mailbox is not a backfill operation. An initialized database performs one
provider confirmation without marking mail seen and resumes pending or downtime
mail. After startup `/readyz` checks local database readiness only; third-party
availability is not a Kubernetes scheduling signal.

### 4. Reply ownership is per ref and accepted sends have an independent witness

Reply text and its target remain paired by `(conversation_id, reply_ref)`.
Completion ownership is a timed, reclaimable lease, so a dead process cannot
leave redelivery at 503 forever. The invariant is one correspondent-visible
email for one `event_id` under at-least-once worker delivery, not transport
exactly-once.

After an uncertain provider send, the adapter reads the event witness carried on
the AgentMail thread before deciding to send again. A found witness settles the
event with no second send. A readable absence plus an admitted reply row permits
one leased send. An unreadable witness, or an absence with no admitted row,
returns a retryable failure without sending. Local absence is never treated as
proof that AgentMail did not accept a prior request.

### 5. The chart owns the storage and egress boundaries explicitly

An existing claim is checked at install or upgrade for same-namespace RWO
Filesystem shape by a short-lived, resource-name-scoped preflight. The runtime
pod's single egress-only NetworkPolicy allows DNS, this release's Curie API pods,
and operator-declared AgentMail or controlled-proxy CIDRs on TCP 443. Enabling
the adapter without a provider CIDR fails closed. There is no mail-pod
Kubernetes API egress or RBAC, and the policy never selects runner sandboxes.

The chart continues to reference all three credentials from its Secret and
rolls the pod when any changes. References keep values out of the Deployment
manifest, not out of Helm release storage or away from a cluster administrator
who can read Secrets; RBAC and the external secret manager remain the operator
boundary.

## Consequences

- A normal `Recreate` upgrade, scoped-token rotation, or process crash resumes
  the durable inbox and reply work instead of priming or forgetting it.
- The adapter keeps its narrow capability surface. This adds a local database
  file, not access to Curie's shared databases.
- The PVC must be sized, backed up, retained, and erased as mail data. A
  chart-managed claim follows Helm deletion and the StorageClass reclaim policy;
  an `existingClaim` remains operator-owned. Complete erasure includes the PVC,
  retained PV, snapshots, and backups.
- Backups must be SQLite-consistent: use a storage snapshot with appropriate
  consistency guarantees or stop the one writer. Restore the claim before the
  Deployment starts.
- Database schema is forward-migrated on open. An old image refuses a newer
  schema; rollback means restoring the pre-upgrade volume snapshot or rolling
  forward, not deleting live state to make the old process start.
- Horizontal scale remains unsupported. Moving to multiple writers or a shared
  remote store changes the failure, consistency, and capability boundaries and
  requires a separate ADR.
- Provider CIDR maintenance is operational work. Where AgentMail has no stable
  range, a controlled egress proxy with a stable CIDR is the supported way to
  preserve fail-closed NetworkPolicy rather than opening arbitrary HTTPS.

## Alternatives considered

1. **Keep process memory and document replacement loss.** Rejected because pod
   replacement and credential rotation are ordinary operation, not exceptional
   disasters, and the resulting losses are silent.
2. **Use Curie's Postgres or Valkey.** Rejected because it grants the channel
   edge a platform data credential and couples email availability and migration
   to stores the adapter otherwise does not need.
3. **Use a chart-managed remote database dedicated to email.** Rejected for one
   serialized writer: it adds another network service, credential, backup plan,
   and failure domain without providing a required scaling property.
4. **Use an ephemeral volume plus the provider thread witness.** Rejected
   because the witness covers accepted outbound mail, not pending ingress,
   first-start state, security decisions, or reply text ownership.
5. **Run multiple replicas against RWX storage.** Rejected because filesystem
   attachability is not a multi-writer protocol. Lease and admission semantics
   would need a shared-store design and a new acceptance decision.

# 251. A failed factory run keeps a private recovery checkpoint that only explicit restoration reads

Date: 2026-10-27

Status: Draft

Tracked in [#5092](https://github.com/curie-eng/curie/issues/5092), which is
the design and delivery umbrella for this decision. This Draft authorizes no
implementation.

## Context

A factory request that fails loses every unpublished edit in its checkout when
its sandbox is released. Two mechanisms retain sandbox state today, and neither
survives an ordinary failure.

The publication snapshot is captured only on publication. The worker calls the
runner's `/v1/snapshot` route (`runner/src/curie_runner/server.py`,
`_snapshot`) only in `apps/worker/src/curie_worker/kernel/attempt.py`, when the
outcome is `AWAITING_APPROVAL` with publish provenance.
`capture_workspace_snapshot` in `runner/src/curie_runner/workspace_snapshot.py`
diffs the checkout against `HEAD` (`git diff --no-renames --binary
--full-index HEAD --` plus untracked files) and enforces `MAX_PATCH_BYTES =
900_000` through `enforce_patch_cap`. Its `base_sha` is `git rev-parse HEAD`,
the live HEAD. A commit made inside the sandbox is therefore silently absorbed
into the base, and the contents of local commits relative to the trusted base
are not captured.

The base archive is built and uploaded by `WorkspacePreparer.prepare` in
`apps/worker/src/curie_worker/workspace.py`, bounded by
`WorkspaceLimits.max_archive_bytes` and the `workspace_max_archive_bytes`
setting in `apps/worker/src/curie_worker/config.py` (environment variable
`CURIE_WORKSPACE_MAX_ARCHIVE_BYTES`, default 268435456).
`WorkspaceClaimCoordinator.release` deletes the claim's owned objects
(`_delete_owned_objects`) and its ownership ledger at claim release. Expired
claims are collected by `enumerate_expired`, `begin_expired_reap` and
`finish_expired_reap`, which carry a `stale_object_keys` retry list. There is
no tombstone concept.

Ordinary readmission starts fresh. A label rerun goes through `readmit` in
`apps/api/src/curie_api/workitem_dispatch.py` to `readmit` in
`apps/api/src/curie_api/workitems/lifecycle.py`, where `_base_values`
overwrites `base_branch`, `base_source` and `base_commit` with a base freshly
resolved by `resolve_base` in `apps/api/src/curie_api/factory_base.py`. A
request still stopping parks that base in `readmit_base_*` columns, applied by
`admit_pending_readmit`. The owner-loss successor, `_admit_owner_lost_successor`
(through `_insert_waiting_request`, `owner_lost_retry` and
`OWNER_LOST_RETRY_LIMIT = 3`), sets no base columns.

An ordinary terminal failure is the path where `apps/worker/src/curie_worker/kernel/completion.py`
routes a delivered, escalated or runner_failed outcome to `_finish_or_settle`
in `apps/worker/src/curie_worker/kernel/work_items.py` with outcome failed; the
API side is `terminalize_execution` and `fail_execution` in
`workitems/lifecycle.py`. Nothing fences sandbox writes between finish and
release. The release itself is fenced: `_release_work_item_sandbox` acquires
the per-thread `ThreadLock` (`apps/worker/src/curie_worker/threadlock.py`, key
from `config.lock_key`), calls `substrate.release_if_claim(thread_key,
claim_name)` (`apps/worker/src/curie_worker/sandbox/substrate.py`), calls
`workspace.release` only when that returns true, and then releases the lock.
Its callers include `_clean_settling_work_item` and the `finally` block in
`apps/worker/src/curie_worker/kernel/lifecycle.py`. No predecessor generation
fence exists beyond this. Cancellation, execution deadline and owner-loss
terminals take a different path, `_halt_work_item_runtime` and
`substrate.terminate_thread` with `record_runtime_termination_where`, which
require an observed sandbox absence. Owner loss on a stale owner
(`_abandon_stale_work_item`) releases nothing.

The trusted base is known in two places: `PublicationContext.expected_head` in
`packages/aci-protocol/src/aci_protocol/events.py`, and the worker's
`PreparedWorkspace.base_sha`, set at clone, where `WorkspacePreparer.prepare`
pins `base_commit`. No ACI event or plugin hook marks a milestone checkpoint;
the ACI outbound events are `TextDelta`, `ToolNote`, `Final`, `ErrorEvent` and
`SideEffectFlag`.

### Evidence

Local prototypes run outside the repository demonstrated these mechanisms
individually:

1. A patch plus an independently reconstructed trusted base, including locally
   committed changes, restored exact file hashes and modes.
2. Against a real S3-compatible store using the shipped archive builder, a base
   and patch retained independently survived the actual
   `WorkspaceClaimCoordinator.release` and restored exact hashes. Real
   multipart create and abort, and an own-container timeout, were observed. A
   256 MiB transfer took about 5.13 seconds; that measures transport only, not
   full capture. A race between selection and garbage collection was
   reproduced. Distributed retention fencing exists only in prototype form.
3. A mounted-filesystem archive was restored after its original namespace was
   deleted in a throwaway cluster. Filesystem restoration is demonstrated. RAM
   or process restoration and real capability revocation were not.
4. Real PostgreSQL and API readmission showed that sequence plus one, a fresh
   finite deadline and fences on the old tuple already hold.
5. A safe-boundary prototype recovered the last complete checkpoint and
   detected a torn capture during active writes. No trusted platform quiescence
   trigger exists.

### Not yet proved

No joined factory path has run. Not yet proved: capture inside the worker
kernel before real cleanup; the trusted-base runner capture seam; joined
restoration into a new request through the worker and the sandbox; PostgreSQL
garbage collection ownership in product code; quota accounting under
concurrency; and the 60-second aggregate capture deadline end to end.

## Relationship to accepted decisions

1. **Extends [ADR 0125](0125-managed-repository-workspaces-and-approval-gated-publication.md).**
   Today the only retained sandbox output is the publication snapshot,
   captured only on publication, and the retained base archive is deleted at
   claim release. This ADR adds a second private artifact class, triggered by
   recovery, with its own retention that outlives the claim. It keeps ADR
   0125's private access discipline: the sandbox holds no bucket credentials,
   every read is a signed read of one object, and artifact bytes never appear
   in list or read responses.
2. **Partially amends [ADR 0186](0186-a-factory-ticket-declares-its-base-and-keeps-it.md)
   section 5, for explicit restoration only.** Section 5 says that changing the
   base of in-flight work needs a fresh admission that resolves the base anew.
   An explicit restoration is instead a new request that pins the checkpoint's
   original trusted base commit and keeps its recorded PR target branch, still
   subject to the current base allowlist and repository checks. Ordinary label
   reruns keep fresh base resolution, and the rest of section 5 is unchanged.
3. **Preserves [ADR 0206](0206-a-factory-run-lost-with-its-worker-is-re-admitted-as-a-new-attempt.md).**
   Automatic owner-loss successors still restart from the beginning and are
   never fed a checkpoint; its points 1 and 3 are unchanged.
   **Preserves [ADR 0162](0162-work-items-own-durable-execution-identity.md)**:
   one WorkItem, sequential bounded ExecutionRequests, deadlines and version
   compare-and-set. Restoration admits a distinct successor request and revives
   nothing. **Preserves [ADR 0222](0222-coworker-bundles-own-workflow-policy-and-curie-enforces-execution.md)**:
   the platform owns identity, isolation, retention and quotas, and the bundle
   owns workflow policy. Recovery adds no factory policy to the generic
   platform.
4. **Acceptance prerequisite.** Under
   [ADR 0085](0085-acceptance-not-implementation-authorizes-an-adr.md) as
   amended by [ADR 0102](0102-accepted-alongside-implementation-with-explicit-approval.md),
   this Draft authorizes nothing. Its policies are proposals for review, and
   recovery implementation stays gated on acceptance. Implementation starts only after this ADR is
   published `Accepted` with explicit maintainer approval. It does not claim
   the ADR 0102 coordinated exception. At acceptance, add exactly one
   back-link under
   [ADR 0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md),
   immediately after the `Status:` line of ADR 0186. ADR 0045 permits a
   back-link only for a whole or partial supersession, so the extension of ADR
   0125 is recorded here, from the forward side, and ADR 0125 is not edited.

   On ADR 0186:

   > **Partially amended by [ADR 0251](0251-a-failed-factory-run-keeps-a-private-recovery-checkpoint-that-only-explicit-restoration-reads.md)**
   > (back-link added under [ADR 0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md)):
   > in section 5, for explicit restoration only, a new request pins the
   > checkpoint's original trusted base commit and recorded PR target branch
   > instead of resolving the base anew. Everything else in this ADR stands.

   This Draft PR leaves the body and status of every Accepted ADR unchanged.

The related Drafts [ADR 0211](0211-a-factory-run-that-loses-its-sandbox-is-re-admitted-like-one-that-loses-its-worker.md)
(sandbox-loss successors) and [ADR 0233](0233-factory-publication-recovery-requires-explicit-operator-adoption.md)
(publication recovery adoption) are separate decisions. This ADR amends
neither and feeds neither a checkpoint.

## Decision

**Workspace recovery is off by default. When an operator enables it, an
ordinary terminal failure of a factory request captures a private checkpoint
of its files against the trusted base before the sandbox is released. Only an
explicit, authorized restoration reads it, and it does so by admitting a new
request.** The numeric limits below are proposed for ADR review.

### 1. Recovery is opt-in and capped installation wide

Recovery defaults off. Enabling it requires an operator-configured,
positive installation-wide byte cap; there is no default cap, and enabling
recovery without one is a configuration error. A checkpoint is retained for
24 hours from capture completion.

The cap counts every recovery byte the installation holds: complete artifacts,
manifests, staging uploads, orphaned objects awaiting deletion and objects
pinned by readers. When the cap is full, Curie refuses new captures and
records the refusal as the capture outcome. It never evicts a checkpoint that
a reader is using to make room.

### 2. A checkpoint is a trusted base plus a patch

Each retained WorkItem holds one independent trusted base, at most two
complete patches and at most one staging patch. The base is a copy under
recovery ownership, not the claim's owned base archive, so claim release
cannot delete it. It is bounded by the effective workspace archive limit,
currently 268435456 bytes by default. Each patch is bounded by a 900000-byte
raw ceiling. Each patch has one manifest of at most 1048576 bytes, so a
WorkItem holds at most three manifests. Manifests are capped private objects,
not one SQL row per file. Artifact and manifest bytes count toward the
installation cap.

A patch records the working tree against the trusted base commit: tracked,
untracked and binary files, deletions and file modes. Changes committed inside
the sandbox are captured as file contents relative to the trusted base.
Ignored files are not captured. Restoration reproduces file contents and modes;
it does not promise Git commit topology.

A capture whose patch exceeds the ceiling, or whose manifest exceeds its cap,
retains nothing new and records that outcome. A capture on a trusted base
commit different from the WorkItem's retained base is refused while any record
for the retained base exists, including tombstones; the earlier checkpoints
stay selectable until they expire.

### 3. Metadata is bounded

Per installation, at most 10000 WorkItems retain recovery state. Per WorkItem,
there are at most three checkpoint records (two complete and one staging),
seven object records (one base, three patches, three manifests) and eight
reader leases. Each retained WorkItem keeps one latest capture outcome, which
is overwritten by the next capture. Tombstoned records and incomplete staging
consume these limits until cleanup finishes. A capture that would exceed any
limit is refused and recorded.

When a staging checkpoint completes, the oldest complete checkpoint is
tombstoned in the same transaction, so a WorkItem never exposes more than two
complete checkpoints.

### 4. Deadlines, leases and signed reads are finite

Capture has an aggregate deadline of 60 seconds covering runner capture, base copy
and uploads. A failed or late upload gets five seconds of foreground cleanup,
including multipart abort; anything left is staging, which expires after 600
seconds and is then collected. Capacity is reserved transactionally before any
upload, and the checkpoint pointer becomes visible only when every object is
uploaded and its digest recorded. An incomplete pointer is never published.

A reader lease lasts 300 seconds and is not renewable. Artifact expiry denies
new selection, while a reader that already holds a lease may finish within it.
Signed reads go through `WorkspaceObjectPort.presign_get` with the effective
workspace reference TTL (`workspace_reference_ttl_seconds`, environment
variable `CURIE_WORKSPACE_REFERENCE_TTL_SECONDS`, default 300 seconds), capped
at the lease's remaining time.

### 5. PostgreSQL serializes selection and collection

Selection and garbage collection tombstoning are serialized with PostgreSQL row
locks and version compare-and-set, following the `lock_work_item`,
`lock_request` and `FOR UPDATE SKIP LOCKED` patterns already used in
`workitems/lifecycle.py` and `apps/api/src/curie_api/thread_attachments.py`.
Object I/O happens outside row locks. Once a checkpoint is tombstoned, new
readers are refused. Deletion is finalized idempotently: a collector that
crashes between deleting objects and finalizing metadata repeats the deletion
after restart and then finalizes. Recovery objects live under keys owned by
these records, never in a claim's ownership ledger, so
`_delete_owned_objects` cannot remove them. The first version does no
deduplication across WorkItems.

### 6. Recovery conveys files only

A restoration restores files. It restores no credentials, approvals,
publication authority, verification claims or running processes; the restored
request earns each of those again through its normal gates. Lists, status
output and logs expose metadata only, never source bytes or signed
capabilities. Captured code can contain secrets that a task wrote into
unignored files, and Curie makes no claim of generic secret filtering; the
checkpoint carries the same exposure as the code a publication would carry,
under stricter access.

### 7. Capture runs on ordinary terminal failure, after writes stop

Capture happens only on an ordinary terminal failure, the path through
`_finish_or_settle` with outcome failed, after the runner's turn has ended and
before the sandbox is released. Writes must be quiescent: the worker issues no
further turn, and the capture path detects a tree that changed while it was
read and discards that capture as torn rather than retaining it.
Cancellation, execution deadline and owner-loss teardown are not extended to
salvage files; they keep their observed-absence path.

Capture reuses the existing release fence. It runs inside
`_release_work_item_sandbox` while the per-thread lock is held and before
`release_if_claim` is called, so no other owner of the thread can release or
reclaim the sandbox during capture. Because a capture may take up to 60
seconds, the capture slice holds the lock through the renewable
`ThreadLock.hold` lease rather than the plain acquire, and confirms ownership
with `LockLease.ensure_owned` before `release_if_claim` runs. A lost lease
abandons the capture, never the release. `release_if_claim` and the
conditional `workspace.release` are otherwise unchanged. This ADR does not
invent a predecessor generation fence. A late release from a
predecessor still goes through the guarded path and touches only its own claim
and workspace ownership.

The existing snapshot diffs against `HEAD` and cannot serve. Capture needs a
new internal checkpoint-capture path in the runner that diffs the working tree
against an explicit trusted base commit supplied by the worker from
`PreparedWorkspace.base_sha`. This ADR names that seam and does not implement
it. Any change to a frozen protocol package needs its own accepted contract
change.

### 8. Restoration is explicit and admits a new request

Restoration happens only when an authenticated, authorized principal selects a
specific checkpoint for a specific WorkItem. Ordinary label reruns and
automatic owner-loss successors never read a checkpoint.

Before any sandbox starts, restoration checks that the checkpoint is complete,
unexpired and not tombstoned; that its WorkItem, repository, trusted base
commit and object digests match the recorded identity; and that the
repository and recorded target branch still pass the current base allowlist
and repository checks. A failure refuses the restoration without starting a
sandbox and without creating an approval or publication.

A passing restoration admits a new ExecutionRequest on the same WorkItem, at
sequence plus one with a fresh finite deadline, under the WorkItem's row lock
and version compare-and-set. It pins the checkpoint's original trusted base
commit and recorded PR target branch rather than resolving the base anew. The
new sandbox starts from the retained trusted base and applies the patch; the
restored tree is verified against the manifest before the request runs.

### 9. Demonstrated and undemonstrated restoration

Filesystem restoration has been demonstrated. RAM or process restoration and
real capability revocation have not. This ADR therefore restores files only
and defers retained pods and full filesystem archives from the first delivery
(see Alternatives considered).

### 10. Milestone checkpoints remain research

A checkpoint taken mid-run would need a trusted quiescence trigger, and none
exists. This ADR invents no ACI event or plugin hook for it. Milestone
checkpoints stay a research slice until a trigger is chosen and proved.

## Delivery slices and acceptance tests

Each slice is one bounded PR with its own integration proof. #5092 tracks them.
No implementation ticket is filed while this ADR is Draft, and every slice
stays gated on this ADR being Accepted.

1. **Storage and PostgreSQL garbage collection ownership.** Seams: new
   recovery records beside `WorkItem` and `ExecutionRequest` in
   `apps/api/src/curie_api/models.py`, locked with the
   `workitems/lifecycle.py` row lock and compare-and-set pattern and the
   `thread_attachments.py` `FOR UPDATE SKIP LOCKED` collector pattern, over
   `WorkspaceObjectPort.delete` and `list_keys`. Tests, against real
   PostgreSQL and a real S3-compatible store:
   1. Interleave selection and collection in every order, including a crash
      between object deletion and metadata finalization. A selection either
      wins a lease before the tombstone or is refused after it, and the
      restarted collector finalizes without error.
   2. Fill the installation cap, then request a capture. It is refused and
      recorded, while a checkpoint under a reader lease stays readable to the
      end of its lease.
   3. Exceed each per-WorkItem record limit and the 10000 WorkItem limit, and
      observe refusal with tombstones and staging counted.
2. **Terminal capture.** Seams: the new runner checkpoint-capture path beside
   `capture_workspace_snapshot`; the worker call on the `_finish_or_settle`
   failed route, inside `_release_work_item_sandbox` under the held thread
   lock and ahead of `release_if_claim`. Tests, through the
   actual worker and runner:
   1. A deterministic failing task writes tracked, untracked, binary and
      executable files and commits some of them locally. The complete
      checkpoint record exists before `release_if_claim` runs, and its patch
      is relative to the trusted base, not `HEAD`.
   2. A late predecessor cleanup, driven through the guarded
      `_release_work_item_sandbox` path after a successor has started, leaves
      the successor's claim, workspace and recovery objects intact.
   3. A capture that misses the 60-second deadline leaves no visible pointer,
      and its staging is collected after 600 seconds.
3. **Authenticated artifact access.** Seams: `WorkspaceObjectPort.presign_get`
   and `workspace_reference_ttl_seconds`. Tests: a list or status read returns
   no source bytes and no signed URL; a signed read covers one object and
   expires at the lesser of the reference TTL and the lease's remaining time;
   a ninth concurrent lease on one WorkItem is refused; a lease is not
   renewable past 300 seconds.
4. **Explicit restoration.** Seams: a restoration admission beside `readmit`
   in `workitems/lifecycle.py` that sets base columns from the checkpoint
   instead of `_base_values` over `resolve_base`; `WorkspacePreparer.prepare`
   started from the retained base. Tests:
   1. Restore after the original sandbox is destroyed and its claim and base
      archive are deleted. The
      tracked, untracked, binary and executable files match the original
      SHA-256 hashes and modes exactly.
   2. Present an expired checkpoint, a corrupt one (digest mismatch) and
      wrong-identity ones (wrong WorkItem, repository, base or digest). Each
      is refused, no sandbox starts and no approval or publication row is
      created.
   3. The new request has sequence plus one, a fresh finite deadline, the
      checkpoint's base commit and target branch, and no approval carried
      over; a base removed from the allowlist since capture is refused.
5. **Safe milestone checkpoints (research).** Seam: none chosen; no ACI event
   or plugin hook is assumed. The slice ends when a trigger is chosen and a
   test shows it: active writes during a triggered capture yield either the
   previous complete checkpoint or a detected torn capture, never a mixed
   tree.

## Consequences

1. An operator who enables recovery can retry a failed run from its last files
   instead of from the beginning, at a storage cost bounded by a cap the
   operator chose.
2. Installations that do not enable recovery see no behavior change and hold
   no recovery objects.
3. Recovery storage becomes a second private artifact class with its own
   collector, metadata and quota accounting, which must be operated and
   monitored.
4. A failed run takes up to 60 seconds longer to release its sandbox while
   capture runs.
5. A restored request can target a base commit older than the one a fresh
   rerun would resolve. That is intended, and it is visible in the request's
   recorded base.
6. A capture on a different trusted base is refused while older checkpoints
   for the WorkItem remain, so a fresh-base rerun that fails may keep nothing
   until those expire.
7. Retained code may contain secrets written into unignored files. Access is
   private and time-bounded, but no filtering is promised.

## Alternatives considered

### Patch plus trusted base artifacts (chosen)

A trusted base copy and a diff against it reconstruct exact file contents and
modes, including local commits, without depending on the sandbox, the claim or
the forge still holding the commit. Patches are small and bounded by the
existing raw ceiling, the base is bounded by the existing archive limit, and
local prototypes demonstrated each step. It conveys files and nothing that
carries authority.

### Retained pods (grace pods)

Keeping the failed sandbox alive would preserve everything, including
processes. It holds node capacity and the sandbox's credentials for the whole
retention window, and real capability revocation was not demonstrated.
Deferred from the first delivery on scope, capacity and credential grounds,
not rejected as impossible.

### PVC-only retention

Retaining a volume ties recovery to the cluster's storage class, zone and
namespace lifecycle, and the volume does not outlive namespace deletion in
every substrate. It also lacks a bounded per-WorkItem size without extra
accounting. Rejected for the first version.

### Full private filesystem archives

Archiving the whole filesystem was demonstrated to restore after namespace
deletion, but its size is bounded by the sandbox disk rather than the patch
ceiling, and it captures caches, tool state and credential material that
recovery must not convey. Deferred from the first delivery on scope, capacity
and credential grounds, not rejected as impossible.

### Periodic checkpoints

Capturing on a timer would cover runs that die without an ordinary failure,
but without a trusted quiescence trigger it captures torn trees, multiplies
storage and contends with the running task. It is the research slice, not the
first delivery.

### Process snapshots

RAM or CRIU-style snapshots would resume a run mid-thought. RAM restoration
was not demonstrated, the snapshot would contain live credentials, and it
conflicts with ADR 0206's restart-from-the-beginning successor. Rejected.

### Automatic recovery on every rerun versus explicit selection

Feeding every rerun the latest checkpoint would silently change what label
reruns and owner-loss successors mean and could replay a broken state in a
loop. Explicit selection keeps those paths unchanged and puts a person or an
authorized principal behind each restoration. Explicit selection chosen.

### Re-resolving the base on restoration

The patch is only meaningful against the base it was taken from. Applying it
to a newly resolved base would produce conflicts or a tree nobody tested.
Restoration pins the original base, which is why it partially amends ADR 0186
section 5.

### Reusing Approval or Publication rows as recovery storage

Those rows carry authority, and their lifecycle ends with publication.
Storing recovery data there would mix files with permissions and risk a
restoration being read as an approval. Rejected.

### Cross-WorkItem deduplication

Sharing base objects across WorkItems would save space but couples deletion
across identities and complicates quota and garbage collection. Not in the
first version.

### Generic secret filtering

No filter reliably recognizes every secret a task may write, and claiming one
would overstate the guarantee. Recovery keeps captured files private and
time-bounded instead and makes no filtering claim.

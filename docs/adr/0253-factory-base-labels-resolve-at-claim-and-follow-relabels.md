# 253. Factory base labels resolve at claim and follow relabels

Date: 2026-10-28

Status: Accepted

Accepted with explicit maintainer approval on 2026-10-28, including
implementation in the same pull request against `next`. This uses the
coordinated exception in
[ADR 0102](0102-accepted-alongside-implementation-with-explicit-approval.md).
The realizing paths are `apps/api/src/curie_api/factory_base.py` for base
validation and resolution, `apps/api/src/curie_api/workitem_dispatch.py` for
claim and restart coordination, and
`apps/api/src/curie_api/factory_base_reconcile.py` for durable reconciliation
of an existing factory PR. Acceptance authorizes implementation; it does not
claim completed verification.

This ADR partially amends
[ADR 0186](0186-a-factory-ticket-declares-its-base-and-keeps-it.md), sections
1, 3, 4, 5 and 6: the allowlist accepts patterns, the base resolves at claim,
and ordinary factory work follows later base label changes. Its refusal of
missing or disallowed branches and its visible base provenance remain.
The explicit restoration exception in
[ADR 0251](0251-a-failed-factory-run-keeps-a-private-recovery-checkpoint-that-only-explicit-restoration-reads.md)
retains its exact checkpoint pins.

## Context

ADR 0186 records a useful safety boundary: a ticket chooses a base permitted
by deployment configuration, and a missing or disallowed branch is refused
instead of silently replaced. Its exact branch list requires configuration
changes whenever an operator creates a release branch. Its admission-time
snapshot also makes a base label added shortly after the factory selection
label too late to affect the work.

Freezing that snapshot for the lifetime of the work item makes an ordinary
label correction expensive. Before a PR exists, the factory can restart from
the corrected branch. After a PR exists, changing its target also requires
replaying its commits onto that branch. Retargeting alone leaves the old
branch's history in the PR. Asking the model to perform the rewrite would
make a platform branch operation depend on a prompt and could reuse approval
or verification evidence for a different tree.

The factory therefore needs a current, deterministic base choice, a short
settling interval for selection labels, and a platform operation that can
rebase an existing factory branch without losing its publication lineage.

## Decision

### 1. Allowed bases are case-sensitive branch patterns

The repository's configured `bases` accepts case-sensitive glob patterns.
An exact entry such as `main` continues to permit that branch only.
`release/*` permits matching release branches, and `*` permits any
syntactically valid branch that exists on the remote. Existing exact lists
retain their meaning. An omitted list retains ADR 0186's repository-default
behavior.

`default_base` remains a literal branch name. It must be syntactically valid
and match at least one configured pattern. A pattern authorizes a branch
name; it never creates that branch. Existence is always checked against the
remote repository, including when `*` is configured. Tags and local-only
branches do not satisfy that check.

Exactly one `base:<branch>` label selects that literal branch. No base label,
including removal of the previous label, selects `default_base`. Multiple
base labels are refused. Invalid names, unmatched names and missing remote
branches are refused with a visible reason. None substitutes another branch.

### 2. Resolve immediately before claim, after labels settle

The factory waits a configurable interval after the factory selection label
is applied before claiming the ticket. The default is 60 seconds. A new
application of the selection label begins a new settling interval.

After the interval, the claim path reads the current labels, resolves the
branch using decision 1, and records its name, choice source and remote base
commit before work starts. An earlier scan may identify a candidate but
cannot freeze its base. A missing or disallowed branch at this point refuses
the claim and reports the reason.

The interval gives related label edits time to arrive. It does not replace
reconciliation of later edits.

### 3. A changed base before a PR restarts from the new branch

If the effective branch changes while ordinary factory work has no PR, the
platform stops the old execution and automatically starts fresh work from
the newly resolved branch. It waits for the old execution to stop before
starting its replacement. The replacement receives fresh execution authority
and the new recorded base; old approvals and verification claims do not
authorize it.

A provenance change that selects the same branch, such as replacing the
deployment default with an explicit label for that branch, may update the
status and recorded source without restarting. Changes to the branch's tip
alone are not base label changes.

### 4. A changed base after a PR rebases before retargeting

For an existing open factory PR, the platform owns an asynchronous, durable
reconciliation operation. It resolves and validates the requested remote
branch, replays the PR's commits onto that branch, and validates the replay
before changing the remote head or PR target. It never asks the model to
rewrite history.

A clean replay is pushed only with an expected-head fence against the
recorded remote PR head. An unexpected remote head refuses the push instead
of overwriting intervening work. The PR is retargeted only after the replayed
head is confirmed on the remote. Durable progress records allow a restarted
reconciler to finish a push-to-retarget gap without replaying an already
completed remote change or treating partial progress as success.

A replay conflict changes neither the remote branch nor the PR target. The
factory records the conflict in status and waits for the factory selection
label to be reapplied. That explicit retry starts with current labels and
remote state; a label edit alone does not repeatedly retry a conflicting
rebase. Other refusals also remain visible rather than being reported as a
successful move.

### 5. Reconciliation preserves authority and lineage checks

The operation remains attached to the existing factory work item, PR and
recorded publication lineage. It cannot adopt another branch or PR merely
because the issue labels name a target. Existing ownership, request authority,
publication identity and lineage verification boundaries continue to apply.

The new head and base are a new verification identity. Prior approvals and
verification results cannot authorize publication of the replayed tree.
The request that authorized the transition remains immutable. CI settlement
has a separate request owner. If a later factory marking starts a successor
on the same unsettled head and base, its fenced acquisition transfers CI
ownership only from an older terminal request of the same work item. That
transfer clears CI readiness and invalidates earlier observations. The new
request must hand off its own verification before CI can settle it, including
when it publishes no revision. A mismatched prior owner refuses acquisition
instead of dropping the CI requirement.
Required validation runs after replay, and publication rechecks the current
head, base and authority through its existing gates. A resumed reconciliation
checks observed remote progress before advancing its durable state.

The first rebase push can trigger CI while the PR still targets the old base.
That intermediate head cannot supply CI evidence for the new target. After
retargeting is durably confirmed, the platform appends a deterministic empty
commit with the same tree and pushes it under an expected-head lease. CI must
pass on this final head. Its new push event occurs with the new PR target,
including on repositories whose workflows do not run for target edits. A
check's start time alone is insufficient because an old-target job can remain
queued until after the retarget.

A follow-up that needs no further code changes still waits for CI on that
final head. The completed base move supplies the CI gate's head, base and
changed paths when the request has no new publication. Red checks use the
existing bounded repair path. If the follow-up publishes a new revision,
the gate follows that newer head instead. A no-change reply cannot bypass
verification or finish the request while its required checks are pending.

The status comment continues to state the base and whether it came from a
label or the deployment default. During reconciliation it identifies the
requested target and progress or refusal; after completion it reflects the
new recorded base. A source-only update remains visible even when no restart
or rebase is needed.

### 6. Explicit restoration keeps its checkpoint base

ADR 0251 restoration still starts a new request pinned to the checkpoint's
original trusted base commit and recorded PR target branch. It does not
reinterpret the patch against current labels or a newer branch tip. The
recorded target must pass the current repository and pattern allowlist
checks required for restoration. This ADR changes ordinary selection and
relabel handling, not checkpoint identity or restoration authority.

## Consequences

1. Operators can authorize release branch families without editing deployment
   configuration for each branch. Broad patterns deliberately grant broader
   branch choice, while remote existence and syntax checks still apply.
2. Selection waits about a minute by default. The claim uses the labels that
   are current after that wait, and later corrections have an explicit path.
3. Correcting the base before a PR discards the prior execution's unpublished
   work and starts fresh. It does not implicitly restore a checkpoint.
4. Correcting the base of an existing PR can rewrite its factory branch's
   commit history. The expected-head fence protects intervening remote work,
   and durable reconciliation must recover from partial completion.
5. Conflicts require an explicit retry through the selection label. The
   platform leaves the remote branch and PR target intact while it waits.
6. A successful replay requires fresh validation and publication checks.
   Existing approvals cannot make a changed tree appear already verified.

## Alternatives considered

1. Keep exact lists only. Rejected because every new branch in an approved
   release family would still require a deployment configuration change.
2. Resolve at the first scan or admission. Rejected because a base label set
   shortly after selection would still be ignored before work began.
3. Keep the lifetime base freeze. Rejected because an ordinary correction
   would still require closing the PR and recreating factory work.
4. Retarget the PR without rebasing. Rejected because it can carry unrelated
   commits from the old base into the new target and leave verification tied
   to the wrong tree.
5. Ask the model to rebase. Rejected because branch mutation, expected-head
   protection and crash recovery belong to the platform's durable operation.
6. Force-push without an expected-head fence. Rejected because it can overwrite
   changes made after reconciliation observed the PR head.
7. Fall back when the requested branch is missing. Rejected because it hides
   the selection error and can publish against the wrong branch.
8. Reinterpret recovery checkpoints using current labels. Rejected because a
   checkpoint patch is meaningful against its recorded trusted base and ADR
   0251 requires that exact identity.

# 165. The tracker owns dependencies and Curie admits ready work

Date: 2026-09-18

Status: Draft

Proposes a partial amendment to
[ADR 0145](0145-a-labelled-issue-is-a-backlog-item-and-the-stream-is-its-queue.md):
factory selection requests admission, but only dependency readiness permits it.
It also replaces that ADR's unconditional prohibition on tracker reconciliation
with bounded reconciliation of selected work. The tracker remains the backlog.
Acceptance must add the corresponding partial amendment backlink under
[ADR 0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md).

This proposal uses the durable WorkItem and sequential ExecutionRequest design
specified in [#2573](https://github.com/curie-eng/curie/issues/2573) and
[#2574](https://github.com/curie-eng/curie/issues/2574). Those records describe
admitted execution, not the unstarted tracker backlog. It extends
[ADR 0143](0143-thread-owned-pull-request-lineage.md) with prerequisite revision
evidence and a separate PR base for stacked publication. The existing publication
authority and ownership fences remain required.

## Context

A factory must be able to accept a selection of interdependent tickets without
starting every ticket immediately. If B needs A, starting both and asking B's
coding agent to notice its blocker spends execution capacity before the admission
decision has been made. Creating a waiting WorkItem for every blocked issue also
makes Curie responsible for a backlog that already belongs to the tracker.

[GitHub issue dependencies](https://docs.github.com/en/issues/tracking-your-work-with-issues/using-issues/creating-issue-dependencies)
and [Linear issue relations](https://linear.app/docs/issue-relations) already
provide explicit blocking relationships. Those are the appropriate authoring
surfaces. Issue body prose, related links, subissues and similar titles do not
establish an execution dependency.

Dependency completion and Git integration are separate facts. A completed coding
attempt can leave a usable implementation on an open PR. Requiring that PR to
merge would unnecessarily serialize implementation and review. Conversely, a PR
existing or an issue closing does not prove that the required implementation is
available. Closure as cancelled or not planned cannot satisfy a prerequisite.

The prerequisite handoff therefore needs both completion evidence and exact
repository identity. A branch name alone can move, disappear or refer to the
wrong repository. Several individually usable branches may also fail to identify
one checkout that contains every prerequisite.

## Decision

**The tracker owns dependency relationships. Curie checks those relationships
and their implementation evidence before admitting work. Initially blocked
tickets have no WorkItem. A verified unmerged prerequisite may supply the base of a stacked
PR; merging is not a condition of starting dependent work.**

### Selection, readiness and capacity are separate

An authorized factory selection, such as the configured GitHub label, grants
standing permission for initial admission while that selection remains valid.
Selection alone does not create a WorkItem. Initial admission requires a current
authorization check and a ready dependency assessment.

When blockers clear, Curie reevaluates selected dependents and may admit them
without another label action. Clearing a blocker cannot authorize an unselected
ticket, choose another agent or repository, or grant publication, merge or deploy
authority. Removing selection or closing the selected ticket withdraws pending
initial admission. Existing cancellation behavior governs already admitted work.

After admission, one canonical issue identity owns one WorkItem and one PR
lineage, with at most one active ExecutionRequest. Waiting for sandbox capacity
belongs to those durable records under
[ADR 0146](0146-headless-capacity-is-a-wait-not-a-reply.md) and #2573. Waiting for
an initial prerequisite consumes no execution attempts, runtime budget or sandbox.

Dependency changes do not request another execution of an existing WorkItem.
The explicit authorized request rule in #2574 continues to govern subsequent
executions. Every requested execution passes the same readiness gate; a manual
start is not an override.

### Readiness is a platform admission boundary

A tracker adapter reads native relations and resolves stable provider, account,
repository or project, and issue identities. One configured tracker is the
authority for each ticket's dependency graph. Links to another tracker do not
create a second writable copy of that graph.

The adapter supplies normalized dependency facts to the admission service.
Provider authentication and schema interpretation stay at that boundary. The
coding bundle still reads the ticket and judges its acceptance criteria; it does
not decide whether the platform may ignore a blocking relationship.

An assessment returns ready, blocked or unknown, with each prerequisite, its
evidence, the source observation time, and a specific reason. Cycles, including
self dependencies, block admission. Missing permissions, unavailable providers,
incomplete pagination and unresolved identities return unknown. Neither blocked
nor unknown may execute. Graph traversal has explicit bounds; exceeding a bound
is an unresolved assessment, never an empty dependency list.

The first implementation uses GitHub. Linear demonstrates that the ownership
boundary is not GitHub specific, but this ADR does not claim a shipped Linear
adapter or require a general connector framework. A future adapter must preserve
the difference between successful completion, cancellation and an explicitly
removed dependency, including where the provider changes relation presentation
after resolution.

### A prerequisite provides a verified outcome and, when needed, code

A dependency is satisfied only by an outcome that meets its declared completion
criteria. For factory work, that means a completed execution with explicit
verification evidence supplied by the bundle, tied to the implementation
revision. The platform records and validates that evidence's identity; it does
not manufacture a correctness judgment from a successful process exit or PR
publication. Failed, interrupted, skipped and cancelled attempts do not qualify.

Work completed outside Curie may qualify through an authorized completion
attestation tied to the source ticket and evidence. A code prerequisite still
requires a verified repository handoff. This does not require importing that
ticket as a WorkItem or launching an agent merely to close the dependency.

Each code handoff records the source issue, completion evidence, repository
identity, intended integration target, original base commit, implementation
branch, exact head commit, PR identity, and observed integration state. A merged
handoff also identifies the merge result. Noncode prerequisites carry their
completion evidence without pretending to provide a Git base.

Curie verifies these claims against the authorized repository. The issue must
identify the implementation being offered; an arbitrary linked PR is insufficient.
Completion evidence for an older commit does not authorize a newer branch head.
An open PR with no qualifying completion evidence remains a blocker.

### Stacked work pins both a checkout and a PR base

For an unmerged prerequisite, the dependent execution starts from its exact
verified head. The dependent owns a new branch and PR lineage; its PR targets
the prerequisite branch. It never writes to the prerequisite's branch or reuses
the prerequisite's lineage. The handoff preserves both the immediate PR base and
the eventual integration target.

For a merged prerequisite, use an exact commit on the integration target that
contains the required implementation. Verify incorporation using repository
history and merge evidence, including squash or rebase merges where the original
head need not be an ancestor. An unavailable or ambiguous incorporation proof
blocks admission. A deleted prerequisite branch is not a blocker when its
implementation is verified on the target.

All dependencies must be satisfied. Where several code prerequisites constrain
one repository, Curie selects a base only if it demonstrably contains all their
required revisions or verified integrated equivalents. A common ancestry chain
can supply such a base. Divergent heads produce `integration_required`; admission
does not pick one parent, silently merge branches or resolve conflicts. Explicit
integration work or completed merges must supply a usable combined base.

Prerequisites from other repositories need explicit artifact or revision
consumption evidence. They cannot be treated as branches of the child's checkout.
If the first implementation cannot verify that consumption, it reports the
unsupported handoff as blocked rather than claiming readiness.

### Observations are refreshed and admission is durable

Authenticated tracker dependency, completion and selection events trigger fresh
reads of affected selected tickets. Events are hints to reevaluate current
truth, not instructions to trust an old payload. A bounded reconciliation pass
over the configured selection scope repairs missed events, startup gaps and
provider outages. It does not scan or execute the entire tracker backlog.

Curie may persist provider cursors, dependency observations, reverse lookup
indexes and assessment results. These are rebuildable projections plus audit
evidence, not editable task records. PostgreSQL holds this state. Before initial
admission, these records have no execution lifecycle, retry budget or PR lineage.
The tracker remains sufficient to reconstruct which tickets are selected.

Each admission policy specifies a maximum observation age. Expired observations
must be refreshed; cached readiness cannot authorize a start through an outage.
An explicit removal of a dependency changes the graph, while a provider's
presentation change on completion still requires its completion evidence.

The final admission transaction checks the current local assessment generation
and authorization, records the exact evidence, and uniquely creates the WorkItem
and initial ExecutionRequest with durable dispatch intent. Duplicate events,
concurrent evaluators and lost acknowledgements cannot create another initial
execution. A restart reconciles incomplete dispatch from that intent.

External tracker and Git reads cannot participate atomically in this database
transaction. Admission records that observation boundary honestly. The worker
refreshes readiness before starting, verifies the selected commit on disk, and
refuses stale evidence. Work that becomes blocked while waiting for capacity
retains its admitted identity and visible cause without starting a sandbox.

If a prerequisite is revised, reopened, abandoned or replaced after a child
starts, retain the child's pinned checkout and report that its handoff needs
revalidation. Do not silently advance its base or edit its live workspace.
Publication rechecks dependency evidence and refuses an invalidated handoff.
An authorized subsequent execution can reconcile the child after the new
prerequisite is verified. Tracker changes alone cannot create that execution.

A prerequisite merging does not itself invalidate the child. Reconcile the
integration proof and the child's PR base. Any required retarget or rebase goes
through the existing trusted publication and lineage fences, with explicit
authority for the operation. Admission grants no automatic rebase, force push,
merge or deploy authority. The immediate PR base is part of publication's checked
identity, alongside its repository, branch and expected head.

### Blocked work remains explainable before it has a WorkItem

The admission read surface accepts a source ticket identity and reports factory
selection, assessment freshness, blockers, completion evidence and the proposed
implementation base. It can explain why no WorkItem exists. CLI and console use
that same result and link to the authoritative tracker relationships.

After admission, the WorkItem records the assessment used and links it to each
execution and publication revision. Unknown observations and invalidated
handoffs remain visible; publication success is never presented as proof that a
prerequisite's acceptance criteria were met.

## Consequences

An operator can select a dependency graph once. Independent roots may execute
concurrently, and downstream work may start on verified open PRs while review
continues. Implementation ordering no longer requires merge ordering.

Curie gains a narrow dependency admission service and tracker reconciliation
responsibility. This deliberately extends #2574's current exclusion of scheduling
across tickets. It does not add backlog authoring, project planning, inferred
dependencies or automatic integration of divergent branches.

Repository handoff becomes durable evidence rather than a prompt suggestion.
Stacked publication must account for two base identities, moving prerequisites
and merge methods that rewrite commits. These checks cost provider reads and
can delay admission when evidence is unavailable.

Blocked source tickets and admitted work have different lifecycles. The operator
surface must show both without manufacturing WorkItems for the former. Existing
publication policy remains responsible for whether a verified proposal may be
written to GitHub.

## Alternatives considered

### Store the authoritative dependency graph on WorkItems

Rejected. It requires creating Curie execution records before work is eligible
and makes tracker edits compete with a second source of truth. Persisting the
assessment used for admission is sufficient for execution provenance.

### Require every prerequisite PR to merge

Rejected. A verified branch can supply the necessary code, and stacked PRs keep
dependent implementation moving while review proceeds. Merge status alone is
also insufficient evidence of the required outcome.

### Let the coding bundle discover blockers after launch

Rejected. This spends capacity and execution budget before deciding whether the
work may start, and makes enforcement depend on model behavior.

### Follow branch names or infer readiness from closed issues

Rejected. Neither identifies the verified implementation consumed by the child.
Cancelled issues, moved heads and unrelated PRs would produce false readiness.

### Require relabelling after every blocker clears

Rejected. The authorized selection already expresses initial execution intent.
Requiring a second action would make a dependency graph depend on manual
sequencing. Fresh authorization checks and durable admission preserve that
intent without multiplying executions.

### Use webhooks without reconciliation

Rejected. A missed completion or selection event could strand eligible work
indefinitely. A bounded read of the authoritative selection scope repairs that
gap without creating another editable backlog.

## Realization and acceptance evidence

This ADR is a proposal, not a claim that dependency admission exists. The
implementation must extend the scope of #2574 for tracker observation and
admission, #2573 for durable execution fencing, and
[#2577](https://github.com/curie-eng/curie/issues/2577) for admission visibility.
Stacked workspace and publication behavior belongs at the existing boundaries
in `apps/worker/src/curie_worker/workspace.py`,
`apps/worker/src/curie_worker/publication_loop.py`,
`apps/api/src/curie_api/routers/publications.py`, and the lineage persistence
in `apps/api/src/curie_api/models.py`. These are proposed integration points,
not an assertion that those paths implement this decision today.

Acceptance of implementation requires these observable cases:

1. Select A, B and C, with B depending on A and C on B. Only A initially has a
   WorkItem. Verified unmerged A admits B from A's exact commit, and B publishes
   a separate PR targeting A. Verified B then admits C without relabelling.
2. Unrelated roots can execute concurrently. A child with multiple prerequisites
   waits for all of them, uses a base containing all verified code, and reports
   divergent branches as `integration_required` without starting an execution.
3. Missing access, provider failure, cycles, failed completion and cancelled
   prerequisites produce visible blocked or unknown assessments and no initial
   WorkItem. Withdrawing selection prevents later automatic admission.
4. Replayed events, concurrent admission, process restart and missed events
   preserve one initial execution. A blocked manual start cannot bypass the
   gate. Capacity waiting spends no execution attempts on dependency checks.
5. A moved prerequisite head, changed graph or lost authorization before
   dispatch prevents model execution. Changes during execution preserve the
   pinned workspace and prevent publication using invalidated evidence.
6. Normal and squash merges, deleted merged branches, abandoned PRs and explicit
   stacked PR retargeting preserve truthful integration evidence and existing
   publication authority. No admission action writes a prerequisite branch.
7. CLI and console explain a selected ticket with no WorkItem and distinguish
   source blockers from capacity waiting after admission. Genuine provider events
   and repository reads supplement local PostgreSQL and Valkey proofs.

No implementation is authorized while this ADR is Draft. If realizing the
decision needs a frozen ACI or plugin format change, that contract change must
be reviewed and versioned separately before dependent implementation proceeds.

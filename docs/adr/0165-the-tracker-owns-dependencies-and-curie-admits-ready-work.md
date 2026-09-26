# 165. The tracker owns dependencies and Curie admits ready work

Date: 2026-09-24

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

Revised on 2026-09-24 after a maintainer design review. The first draft of
2026-09-18 accepted bundle supplied verification as completion, stopped a
multi-parent child at `integration_required`, and refused publication of a
child whose prerequisite moved. This revision replaces those three positions
and records the repository CI requirements that stacking depends on.

## Context

A factory must be able to accept a selection of interdependent tickets without
starting every ticket immediately. If B needs A, starting both and asking B's
coding agent to notice its blocker spends execution capacity before the admission
decision has been made. Creating a waiting WorkItem for every blocked issue also
makes Curie responsible for a backlog that already belongs to the tracker.

The need is observed, not hypothetical. The factory's own v0.10.0 tickets carry
dependencies: orphan recovery
([#3076](https://github.com/curie-eng/curie/issues/3076)) depends on per item
start authority, and the live status comment
([#3077](https://github.com/curie-eng/curie/issues/3077)) depends on the relabel
and cancel semantics and on early close on PR. Those dependencies exist only as
issue body prose, so a person sequenced them by hand. An external work queue
that treated an open, green parent PR as a met dependency hit two further
failures: a met dependency did not start the child until the next periodic
scan, and a child pinned to the integration branch started without its
parent's unmerged code.

[GitHub issue dependencies](https://docs.github.com/en/issues/tracking-your-work-with-issues/using-issues/creating-issue-dependencies)
and [Linear issue relations](https://linear.app/docs/issue-relations) already
provide explicit blocking relationships. Those are the appropriate authoring
surfaces. Issue body prose, related links, subissues and similar titles do not
establish an execution dependency.

Dependency completion and Git integration are separate facts. A completed coding
attempt can leave a usable implementation on an open PR. Requiring that PR to
merge would serialize implementation behind review. Conversely, a PR existing or
an issue closing does not prove that the required implementation is available.
Closure as not planned cannot satisfy a prerequisite.

Stacking has repository preconditions. In curie-eng/curie the factory publishes
from `curie/publication-<id>` branches, while the CI workflow runs on pull
requests into `main`, `next` and `task/**` only, and only for the `opened`,
`synchronize` and `reopened` actions. A PR into a factory branch therefore gets
no checks, and GitHub's automatic retarget after a parent merges emits
`edited`, which starts no checks either. The repository merges with merge
commits and deletes merged head branches, so GitHub does retarget a stacked PR
to the parent's base when the parent merges.

## Decision

**The tracker owns dependency relationships. Curie checks those relationships
and their implementation evidence before admitting work. Initially blocked
tickets have no WorkItem. A prerequisite is satisfied by a published PR whose
required checks are green on its exact head; merging is not a condition of
starting dependent work. A child stacks on a single unmerged prerequisite
chain and otherwise waits for merges. Stacks are repaired lazily, once, when a
prerequisite merges.**

### Selection, readiness and capacity are separate

An authorized factory selection, such as the configured GitHub label, grants
standing permission for initial admission while that selection remains valid.
Selection alone does not create a WorkItem. Initial admission requires a current
authorization check and a ready dependency assessment. An operator labels a
dependency graph once; nobody relabels a child after its blockers clear.

When blockers clear, Curie reevaluates selected dependents and admits them
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

### Dependencies are tracker native, behind an adapter

Only the tracker's native blocking relation declares a dependency. For GitHub
that is the issue dependency (blocked by and blocking) relation. Issue body
text such as "depends on #N" is not parsed and never blocks or starts work.
Tools that file factory tickets set the native relation.

A tracker adapter reads native relations and resolves stable provider, account,
repository or project, and issue identities. It supplies normalized dependency
facts to the admission service; provider authentication and schema
interpretation stay at that boundary. Admission never reads a provider schema
directly. One configured tracker is the authority for each ticket's dependency
graph. Links to another tracker do not create a second writable copy of that
graph.

The first implementation ships a GitHub adapter only. Tracker native
dependencies are a standing product direction, and Linear is the expected
second adapter; adding it must not change the admission service. A future
adapter must preserve the difference between successful completion,
cancellation and an explicitly removed dependency, including where the provider
changes relation presentation after resolution.

The coding bundle still reads the ticket and judges its acceptance criteria; it
does not decide whether the platform may ignore a blocking relationship.

### Readiness is a platform admission boundary

An assessment returns ready, blocked or unknown, with each prerequisite, its
state, its evidence, the source observation time, and a specific reason.
Cycles, including self dependencies, block admission. Missing permissions,
unavailable providers, incomplete pagination and unresolved identities return
unknown. Neither blocked nor unknown may execute. Graph traversal has explicit
bounds; exceeding a bound is an unresolved assessment, never an empty
dependency list.

### What satisfies a prerequisite

A prerequisite built by Curie is satisfied when all of the following hold:

1. its latest execution completed,
2. its PR is published through the trusted publication path, and
3. every check the repository requires is green on that PR's exact head commit.

Bundle supplied verification evidence is recorded but is not sufficient on its
own; the repository's required checks are the objective gate. Failed,
interrupted, skipped and cancelled executions do not qualify. An open PR whose
required checks are pending, red or absent remains a blocker. When the
publication policy requires human approval, the prerequisite is not satisfied
until that approval publishes the PR. Evidence for an older commit does not
satisfy a newer head.

A prerequisite that Curie did not build, because it is unselected or a person
is implementing it, is satisfied only by a merged PR that closes that issue.
The dependent then starts from the integration target; it never stacks on a
branch Curie did not verify. Closure as completed without a merged PR does not
qualify, and closure as not planned never qualifies. An operator who wants to
drop such a prerequisite removes the relation.

Each code handoff records the source issue, completion evidence, repository
identity, intended integration target, original base commit, implementation
branch, exact head commit, PR identity, and observed integration state. A merged
handoff also identifies the merge result. Curie verifies these claims against
the authorized repository. The issue must identify the implementation being
offered; an arbitrary linked PR is insufficient.

### A child stacks on one chain and otherwise waits for merges

For a single unmerged prerequisite, or several unmerged prerequisites that form
one ancestry chain, the dependent execution starts from the exact verified head
at the top of that chain. The dependent owns a new branch and PR lineage; its PR
targets the prerequisite's branch. It never writes to the prerequisite's branch
or reuses the prerequisite's lineage. The handoff preserves both the immediate
PR base and the eventual integration target.

When unmerged prerequisites diverge, for example two parents each built from
the integration target, no single commit contains them all. The dependent
waits, blocked with the reason "waiting on merge", until enough of them merge
that one chain or the integration target contains every required revision. It
is then admitted automatically. Curie never synthesizes a base by merging
parent heads together and never resolves conflicts between parents.

For a merged prerequisite, use an exact commit on the integration target that
contains the required implementation. Verify incorporation using repository
history and merge evidence. A deleted prerequisite branch is not a blocker when
its implementation is verified on the target. A dependent whose integration
target differs from its prerequisite's target waits until the prerequisite's
implementation is verified on the dependent's own target.

Prerequisites from other repositories are not supported in the first
implementation. They report blocked with an unsupported handoff reason rather
than claiming readiness.

### Stacks are repaired lazily, at retarget

A prerequisite that is revised after its dependent starts, for example by a
review feedback round, does not propagate up the stack. The dependent finishes
on its pinned checkout and publishes its PR against the prerequisite's branch.
The status comment notes that the prerequisite was revised after the dependent
started. Curie does not rebase, force push or restart dependents when a lower
PR changes.

Repair happens once, when a prerequisite merges and GitHub retargets the
dependent PR to the integration target. Curie then merges the new base into the
dependent's branch through the trusted publication path with a normal push.
That push is the dependent's first check run against the integration target
and surfaces any conflict immediately. A merge conflict or red required checks
at that point move the dependent to needs human; an authorized subsequent
execution may repair it. A tall stack therefore unwinds from the bottom, one
merge at a time, with no cascade.

If a prerequisite's PR is closed unmerged or its issue is reopened after a
dependent started, the dependent keeps its pinned workspace and moves to needs
human with the cause. Admission grants no rebase, force push, merge or deploy
authority. The immediate PR base is part of publication's checked identity,
alongside its repository, branch and expected head.

### The repository must check stacked PRs

Stacking beyond one level requires the repository to run its required checks on
pull requests into the factory's publication branch prefix. Without that, a
dependent PR into a prerequisite branch has no checks, so it can never satisfy
its own dependents. Curie does not treat absent checks as green: it reports the
prerequisite as blocked with a missing checks reason, and the chain waits for
merges instead. curie-eng/curie adds its publication prefix to the CI pull
request branch filter as part of realizing this decision.

### A failed prerequisite blocks visibly and recovers on retry

A prerequisite whose execution fails or is cancelled, or whose PR closes
unmerged before any dependent started, leaves its selected dependents blocked
with that cause. Nothing cascades: dependents keep their selection and are not
cancelled or moved to needs human. Retrying the prerequisite through its normal
authorized request clears them automatically once it is satisfied. Removing the
relation is an explicit graph change and reassesses the dependent without that
prerequisite.

### Observations are refreshed and admission is durable

Readiness is reevaluated on these hints: a prerequisite's execution reaching a
terminal state, a change in a prerequisite PR's check or merge state observed
by the publication poll, and a tracker dependency event (for GitHub, the
`issue_dependencies` webhook). Events are hints to reevaluate current truth, not
instructions to trust an old payload. A bounded reconciliation pass over the
configured selection scope repairs missed events, startup gaps and provider
outages. It does not scan or execute the entire tracker backlog.

Curie may persist provider cursors, dependency observations, reverse lookup
indexes and assessment results. These are rebuildable projections plus audit
evidence, not editable task records. PostgreSQL holds this state. Before initial
admission, these records have no execution lifecycle, retry budget or PR lineage.
The tracker remains sufficient to reconstruct which tickets are selected.

Each admission policy specifies a maximum observation age. Expired observations
must be refreshed; cached readiness cannot authorize a start through an outage.

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

### Blocked work is visible on the issue

A selected ticket that is blocked carries a `curie:blocked` label alongside the
factory state labels. It has one status comment, the same comment that becomes
its live run card once admitted
([#3077](https://github.com/curie-eng/curie/issues/3077)). While blocked, that
comment lists each prerequisite with its current state (running, PR open,
checks pending, checks red, failed, waiting on merge, unknown) and the base the
dependent will use. Cycles and unknown assessments appear there with their
reason. The comment is edited in place through admission, execution and
completion; the ticket never accumulates one comment per state change.

The admission read surface accepts a source ticket identity and reports factory
selection, assessment freshness, blockers, completion evidence and the proposed
implementation base. It can explain why no WorkItem exists. The status comment,
CLI and console use that same result and link to the authoritative tracker
relationships.

After admission, the WorkItem records the assessment used and links it to each
execution and publication revision. Publication success is never presented as
proof that a prerequisite's acceptance criteria were met.

## Consequences

An operator can select a dependency graph once. Independent roots execute
concurrently, and a dependent on a single chain starts on verified open PRs
while review continues. Implementation ordering no longer requires merge
ordering, except where a dependent has diverged prerequisites.

Required checks become part of admission. A repository whose CI does not run on
PRs into the factory branch prefix gets one level of stacking and merge
ordering beyond it. The factory's GitHub App needs read access to check runs
and commit statuses in addition to issues.

Curie gains a narrow dependency admission service, a tracker adapter seam and
tracker reconciliation responsibility. This deliberately extends #2574's
current exclusion of scheduling across tickets. It does not add backlog
authoring, project planning, inferred dependencies or automatic integration of
divergent branches.

Lazy repair keeps stacks cheap but moves conflict discovery to the moment a
prerequisite merges. A reviewer can still merge a dependent PR into its
prerequisite's branch before the prerequisite lands; guarding that is left to
implementation.

Blocked source tickets and admitted work have different lifecycles. The operator
surface shows both without manufacturing WorkItems for the former. Existing
publication policy remains responsible for whether a verified proposal may be
written to GitHub.

## Alternatives considered

### Store the authoritative dependency graph on WorkItems

Rejected. It requires creating Curie execution records before work is eligible
and makes tracker edits compete with a second source of truth. Persisting the
assessment used for admission is sufficient for execution provenance.

### Require every prerequisite PR to merge

Rejected. A verified branch can supply the necessary code, and stacked PRs keep
dependent implementation moving while review proceeds. Merge ordering is kept
only for diverged prerequisites, where no single verified base exists.

### Accept bundle supplied verification as completion

Rejected, and replaced from the first draft. The agent would grade its own
work. Required checks on the exact head are objective and already gate merges.

### Require an approving review on the prerequisite

Rejected. Review latency would serialize the graph again and remove most of the
benefit of stacking.

### Synthesize a base for diverged prerequisites

Rejected. Merging parent heads into a dependent's branch makes the dependent
PR carry unreviewed parent code, lets a dependent merge ahead of its parents,
and turns every later parent revision into another integration problem.

### Serialize parallel prerequisites

Rejected. Building one parent on another to avoid divergence invents
dependencies nobody declared and serializes independent work.

### Restack on every prerequisite revision

Rejected. Rebasing and force pushing each level, as stacked PR tools do for a
person at a terminal, cascades through tall stacks, restarts checks at every
level, detaches review comments, and needs force push authority the factory
does not hold. Merging each revision down the stack cascades the same way with
merge commits. Refusing publication until a prerequisite settles, as the first
draft proposed, stalls a whole stack on every review round.

### Cascade a failed prerequisite to needs human

Rejected. It forces a relabel of every dependent after the parent is fixed,
reintroducing manual sequencing.

### Parse issue body prose as dependencies

Rejected. Prose is ambiguous, cannot distinguish a removed dependency from an
edited sentence, and has no equivalent across trackers.

### Let the coding bundle discover blockers after launch

Rejected. This spends capacity and execution budget before deciding whether the
work may start, and makes enforcement depend on model behavior.

### Follow branch names or infer readiness from closed issues

Rejected. Neither identifies the verified implementation consumed by the child.
Cancelled issues, moved heads and unrelated PRs would produce false readiness.

### Require relabelling after every blocker clears

Rejected. The authorized selection already expresses initial execution intent.
Requiring a second action would make a dependency graph depend on manual
sequencing.

### Use webhooks without reconciliation, or reconciliation without hints

Rejected. A missed completion or selection event could strand eligible work
indefinitely, and a sweep alone makes every edge wait a full interval. Hints
give latency; the bounded sweep gives correctness.

## Realization and acceptance evidence

This ADR is a proposal, not a claim that dependency admission exists. The
implementation must extend the scope of #2574 for tracker observation and
admission, #2573 for durable execution fencing, #3077 for the status comment
and labels, and [#2577](https://github.com/curie-eng/curie/issues/2577) for
admission visibility. Stacked workspace and publication behavior belongs at the
existing boundaries in `apps/worker/src/curie_worker/workspace.py`,
`apps/worker/src/curie_worker/publication_loop.py`,
`apps/api/src/curie_api/routers/publications.py`, and the lineage persistence
in `apps/api/src/curie_api/models.py`. GitHub webhook intake extends
`apps/api/src/curie_api/routers/github.py`. The per ticket base branch choice in
[#3095](https://github.com/curie-eng/curie/issues/3095) supplies each ticket's
integration target. These are proposed integration points, not an assertion
that those paths implement this decision today.

Acceptance of implementation requires these observable cases:

1. Select A, B and C, with B blocked by A and C blocked by B. Only A initially
   has a WorkItem. A's PR with green required checks admits B from A's exact
   head, and B publishes a separate PR targeting A's branch. B's PR with green
   checks admits C, all without relabelling.
2. Unrelated roots execute concurrently. A child blocked by two diverged
   unmerged parents shows "waiting on merge" and starts from the integration
   target automatically once both have merged, or stacks on the survivor once
   one has merged and the other is its descendant.
3. A prerequisite with pending, red or absent required checks keeps its
   dependents blocked. A repository without CI on the publication prefix
   stacks one level and reports the missing checks reason beyond it.
4. Merging a prerequisite retargets its dependent PR, Curie merges the new base
   into the dependent branch once, checks run, and a conflict moves the
   dependent to needs human. A revision to a prerequisite during a
   dependent's execution does not restart or block that dependent.
5. Missing access, provider failure, cycles, failed or cancelled prerequisites
   and prerequisites closed as not planned produce visible blocked or unknown
   assessments and no initial WorkItem. Retrying a failed prerequisite unblocks
   its dependents. Removing a relation reassesses the dependent. Withdrawing
   selection prevents later automatic admission.
6. A prerequisite built outside Curie is satisfied only by a merged PR that
   closes it, and the dependent starts from the integration target.
7. Replayed events, concurrent admission, process restart and missed events
   preserve one initial execution. A blocked manual start cannot bypass the
   gate. Capacity waiting spends no execution attempts on dependency checks.
8. A blocked ticket shows `curie:blocked` and a single status comment listing
   each prerequisite and its state; the same comment becomes the live run card
   on admission. CLI and console explain a selected ticket with no WorkItem.
9. Admission reads dependencies only through the tracker adapter interface, and
   a test double adapter drives the admission service with no GitHub schema.

No implementation is authorized while this ADR is Draft. If realizing the
decision needs a frozen ACI or plugin format change, that contract change must
be reviewed and versioned separately before dependent implementation proceeds.

# 165. The tracker owns dependencies and Curie admits ready work

Date: 2026-09-24

Status: Draft

Proposes a partial amendment to
[ADR 0145](0145-a-labelled-issue-is-a-backlog-item-and-the-stream-is-its-queue.md):
factory selection requests admission, and a ticket whose native blockers are
not all merged into its resolved base is not admitted. The tracker remains the
backlog. Acceptance must add the corresponding partial amendment backlink under
[ADR 0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md).

This proposal uses the durable WorkItem and sequential ExecutionRequest design
specified in [#2573](https://github.com/curie-eng/curie/issues/2573) and
[#2574](https://github.com/curie-eng/curie/issues/2574). Those records describe
admitted execution, not the unstarted tracker backlog.

Revised on 2026-10-05 to conform to
[ADR 0199](0199-the-factory-admits-only-tickets-it-can-start-and-finish.md),
accepted the same day. ADR 0199 makes blocker readiness one check in a binary
admission gate with no waiting state. This revision removes the positions that
created one: the visible `curie:blocked` state, automatic admission when
blockers clear, stacking a dependent on an unmerged prerequisite PR, lazy stack
repair at retarget, and the CI requirement on the publication branch prefix
that stacking depended on. The earlier revisions of 2026-09-18 and 2026-09-24
are in git history.

## Context

A factory must be able to accept a selection of interdependent tickets without
starting a ticket whose prerequisites are not in its base. If B needs A,
starting B before A has landed spends a run on a checkout that does not contain
what B builds on, and asks B's coding agent to notice its blocker after the
sandbox is paid for.

The need is observed, not hypothetical. The factory's own v0.10.0 tickets carry
dependencies: orphan recovery
([#3076](https://github.com/curie-eng/curie/issues/3076)) depends on per item
start authority, and the live status comment
([#3077](https://github.com/curie-eng/curie/issues/3077)) depends on the relabel
and cancel semantics and on early close on PR. Those dependencies exist only as
issue body prose, so a person sequenced them by hand. An external work queue
that treated an open, green parent PR as a met dependency started a child
pinned to the integration branch without its parent's unmerged code.

[GitHub issue dependencies](https://docs.github.com/en/issues/tracking-your-work-with-issues/using-issues/creating-issue-dependencies)
and [Linear issue relations](https://linear.app/docs/issue-relations) already
provide explicit blocking relationships. Those are the appropriate authoring
surfaces. Issue body prose, related links, subissues and similar titles do not
establish an execution dependency.

Issue state does not prove that a prerequisite landed. A closing keyword in a
PR into a non-default branch does not close the issue, an issue can be closed
by hand without any code, and closure as not planned means the work will not
exist. Only a merged PR whose merge commit is in the base the dependent branches
from proves the prerequisite is available to it.

ADR 0199 sets the rule this ADR serves: a ticket is admitted only if every
precondition is already met, and a ticket that fails is rejected and returned
to the tracker, never parked.

## Decision

**The tracker owns dependency relationships. Curie reads the native relations
at admission, and a ticket is admitted only if every prerequisite is merged
into its resolved base. Anything else is an ADR 0199 rejection or, for an
outage, its internal unknown. Curie keeps no waiting state for blocked
tickets and does not stack work on unmerged prerequisites.**

### Dependencies are tracker native, behind an adapter

Only the tracker's native blocking relation declares a dependency. For GitHub
that is the issue dependency (blocked by and blocking) relation. Issue body
text such as "depends on #N" is not parsed into a dependency and never blocks
or starts work. ADR 0199 asks a ticket to restate its blockers in a Blocked by
section; a mismatch with the native relation is a rejection that says to set
the relation natively, not a second source of truth. Tools that file factory
tickets set the native relation.

A tracker adapter, the dependency operation of the ADR 0197 Tracker port, reads
native relations and resolves stable provider, account, repository or project,
and issue identities. It supplies normalized dependency facts to the admission
gate; provider authentication and schema interpretation stay at that boundary.
Admission never reads a provider schema directly. One configured tracker is the
authority for each ticket's dependency graph. Links to another tracker do not
create a second writable copy of that graph.

The first implementation ships a GitHub adapter only. Linear is the expected
second adapter; adding it must not change the admission gate. A future adapter
must preserve the difference between successful completion, cancellation and
an explicitly removed dependency, including where the provider changes
relation presentation after resolution. An adapter that declares the
dependency operation unsupported falls back to the ticket's Blocked by section
under ADR 0199.

### Readiness is one admission check

An assessment returns ready, blocked or unknown, with each prerequisite, its
state, its evidence, the source observation time, and a specific reason.

1. **Ready** passes ADR 0199's first can-it-start check.
2. **Blocked** is an ADR 0199 rejection. The rejection comment lists each
   unmet prerequisite and why it is unmet.
3. **Unknown** is ADR 0199's internal unknown: retried silently, never shown
   on the issue as waiting.

Cycles, including self dependencies, are blocked. Missing permissions,
unavailable providers, incomplete pagination and unresolved identities are
unknown. Graph traversal has explicit bounds; exceeding a bound is unknown,
never an empty dependency list. Only direct blockers are assessed: a direct
blocker that is merged into the base already carries its own prerequisites.

### What satisfies a prerequisite

A prerequisite is satisfied only by a merged pull request that closes, or
carries a closing reference to, the prerequisite issue, and whose merge commit
is an ancestor of the dependent's resolved base commit (ADR 0186). This holds
whether Curie or a person built it.

1. An open PR does not satisfy a prerequisite, whatever its checks say.
2. A PR merged into a different branch does not satisfy a prerequisite until
   its change is in the dependent's base.
3. An issue closed as completed without such a PR does not qualify. An issue
   closed as not planned never qualifies. An operator who wants to drop a
   prerequisite removes the relation.
4. Prerequisites in another repository are not supported in the first
   implementation. They are blocked with an unsupported reason rather than
   claimed ready.

The admission verdict records the assessment used: each prerequisite, the PR
and merge commit that satisfied it, and the base commit checked. The WorkItem
links that record to each execution. No stacked handoff, immediate PR base, or
second lineage exists: a dependent branches from and targets its resolved base
like every other ticket.

### A blocked ticket is rejected, and a person re-admits it

A blocked ticket has no WorkItem, no `curie:blocked` label, and no waiting
status comment. It carries ADR 0199's rejection label and marked comment. When
its blockers merge, nothing re-admits it automatically. A person edits or
relabels the issue, which changes its ADR 0199 fingerprint, and the next intake
pass runs the gate again. Curie therefore keeps no reverse index from
prerequisites to waiting dependents, no reevaluation hints, and no
reconciliation pass over blocked tickets.

A prerequisite that fails or is cancelled affects only itself. Its dependents
were never admitted, so nothing cascades.

### Admission is durable

The ADR 0199 admission transaction records the dependency evidence with the
verdict and uniquely creates the WorkItem and initial ExecutionRequest.
Duplicate notices, concurrent intake and lost acknowledgements cannot create
another initial execution. The base commit is frozen on the WorkItem at
admission (ADR 0186), and every prerequisite was checked against that commit,
so the worker does not reassess dependencies before starting.

## Consequences

A dependency graph lands in merge order. Independent roots run concurrently. A
dependent starts only after a person has merged its prerequisites and edited
or relabelled it. Deep graphs are slower than with stacking, and each level
needs one human action after its blockers merge. That is the cost of a factory
with no waiting stage.

The factory never builds on unreviewed code. A dependent PR contains only its
own change, targets the base like any other PR, and needs no retarget, merge
down, or conflict repair when a parent lands.

The repository needs no CI on PRs into the factory's publication branch
prefix, because no factory PR targets another factory branch.

Curie gains a dependency read at admission through the Tracker port and stores
the assessment with the verdict. It does not gain tracker reconciliation, a
backlog of blocked tickets, or scheduling across tickets, so #2574's exclusion
of cross-ticket scheduling stands.

ADR 0186's paragraph on a stacked child's immediate PR base described the
2026-09-24 revision of this ADR. Under this revision no child is stacked, and
every dependent uses its resolved base.

## Alternatives considered

### Stack a dependent on a verified unmerged prerequisite

The 2026-09-24 revision: a prerequisite was satisfied by a published PR whose
required checks were green on its exact head, and a dependent stacked on it.
Rejected under ADR 0199. A precondition that exists only on an open PR is not
met in the base, the dependent builds on code no person has reviewed, and the
stack needs lazy repair, retargeting, and CI on the publication prefix.

### Keep a visible blocked state and admit automatically when blockers clear

Rejected under ADR 0199. It makes the factory own a waiting stage whose exit
usually depends on a person merging a PR, keeps a backlog of tickets that look
in progress, and requires event hints, a reverse index, and a reconciliation
pass to notice when blockers clear. A rejection returns the ticket to the
tracker; an edit or relabel is the signal that it is ready.

### Store the authoritative dependency graph on WorkItems

Rejected. It requires creating Curie execution records before work is eligible
and makes tracker edits compete with a second source of truth. Recording the
assessment used for admission is sufficient for provenance.

### Parse issue body prose as dependencies

Rejected. Prose is ambiguous, cannot distinguish a removed dependency from an
edited sentence, and has no equivalent across trackers. The ADR 0199 Blocked by
section is checked against the native relation, never used in its place where
the tracker supports relations.

### Let the coding bundle discover blockers after launch

Rejected. It spends a sandbox and execution budget before deciding whether the
work may start, and makes enforcement depend on model behavior.

### Infer readiness from closed issues or branch names

Rejected. A closed issue does not prove its change is in the dependent's base,
and a branch name does not identify a merged implementation.

## Realization and acceptance evidence

This ADR is a proposal, not a claim that dependency admission exists. It
realizes ADR 0199's first can-it-start check. The dependency read belongs to
the ADR 0197 Tracker port; the check runs in the ADR 0199 gate inside
`admit_notice` in `apps/api/src/curie_api/github_factory.py`; the evidence is
stored with the ADR 0199 verdict. These are proposed integration points, not an
assertion that those paths implement this decision today.

Acceptance of implementation requires these observable cases:

1. Select A, B and C, with B blocked by A and C blocked by B. A is admitted. B
   and C are rejected with comments naming their unmet blockers, and get no
   WorkItem. After A's PR merges into the base, B is still rejected until a
   person edits or relabels it; it is then admitted from a base commit that
   contains A's merge commit. C follows the same way after B merges.
2. An open prerequisite PR with green checks, a PR merged into another branch,
   an issue closed as completed without a merged PR, and an issue closed as not
   planned each leave the dependent rejected with that reason.
3. A cycle, a self dependency, and a cross-repository prerequisite are
   rejected with their reasons. Missing access, provider failure, incomplete
   pagination and an exceeded traversal bound produce the internal unknown, no
   comment, no label, and a silent retry.
4. A Blocked by section that disagrees with the native relation is rejected
   with a reason that says to set the relation natively.
5. Replayed notices and concurrent intake preserve one initial execution.
6. Admission reads dependencies only through the Tracker port, and a test
   double adapter drives the gate with no GitHub schema.

No implementation is authorized while this ADR is Draft.

# 233. Factory publication recovery requires explicit operator adoption

Date: 2026-10-15

Status: Draft

## Context

Gauntlet campaign 4 found that a fresh install or loss of the factory database
re-admits issues whose trigger label remains and opens duplicate PRs or MRs.
[#4629](https://github.com/curie-eng/curie/issues/4629), filed for v0.13.1,
addresses duplicate suppression before publication. It deliberately excludes
rebuilding lost publication authority. Suppressing another PR does not restore
the WorkItem that can receive review feedback or continue CI on the original.

Several existing decisions constrain recovery.

1. [ADR 0162](0162-work-items-own-durable-execution-identity.md) makes the
   WorkItem the durable execution identity, with sequential bounded requests,
   sticky cancellation and one linked publication lineage. The tracker owns
   ticket content; Curie owns execution lifecycle facts.
2. [ADR 0143](0143-thread-owned-pull-request-lineage.md) makes lineage a
   versioned concurrency fence. The forge owns the PR's current head and open
   or terminal state. An unexpected head is a conflict, never silently adopted.
   [ADR 0125](0125-managed-repository-workspaces-and-approval-gated-publication.md)
   and [ADR 0147](0147-publication-approval-is-a-per-agent-operator-policy.md)
   keep publication authority in the platform's approval plane.
3. [ADR 0187](0187-the-factory-polls-github-and-the-platform-reads-the-issue.md)
   admits authenticated observations through verified intake and deduplicates
   them in storage. Its cursors and delivery records do not survive database
   loss. [ADR 0197](0197-the-factory-reaches-code-hosts-and-trackers-through-two-ports.md)
   puts external operations behind Tracker and CodeHost ports and qualifies
   identities by host and immutable tracker scope or repository id.
4. [ADR 0215](0215-each-tracker-surface-starts-factory-work-under-its-own-trigger-label.md)
   routes initial admission by the effective trigger label, then retains the
   owning agent, admitting label and lineage binding. A current label alone
   cannot recover which agent owned an older publication.
5. [ADR 0208](0208-a-factory-pr-opened-after-its-request-was-cancelled-is-adopted-by-the-next-run.md)
   repairs a missing WorkItem link when the database still proves that an
   earlier request of that WorkItem published the lineage. It does not cover
   loss of the WorkItem, publication and lineage themselves.

The forge retains useful but incomplete evidence: factory-authored open and
closed PRs or MRs, branches named `curie/publication-<id>`, revision commit
markers, tracker back-links in PR bodies, factory-authored status comments,
`curie-factory:pr-open` and other tracker state labels, and Jira remote links.
Names, labels and editable text are not approval records. They cannot prove
lost cancellation state, which feedback was handled, or how much of a bounded
execution or CI repair allowance was spent. Another installation can also use
the same factory account.

## Decision

**Discovery suppresses duplicate publication automatically. Rebuilding lost
WorkItems and publication lineage requires an explicit authenticated operator
adoption of verified forge facts at an exact head. Recovery records what is
known and grants fresh, bounded continuation authority; it never invents lost
execution or approval history.**

This proposes a narrow amendment to ADR 0143: after database loss, an operator
may establish an explicitly audited baseline at a verified existing PR head.
Ordinary reconciliation still refuses an unexpected head. ADR 0208 remains
the automatic repair for surviving local lineage. No Accepted ADR is changed
by this Draft, and implementation requires separate maintainer acceptance.

### Discovery and the recovery boundary

1. The guard in #4629 runs before fresh admission and again before publication,
   independently of recovery. A matching open factory publication suppresses
   fresh work and reports its link. Replaying an old trigger after a completed
   publication is suppressed too. Incomplete reads defer without advancing the
   admission cursor. Discovery creates no execution or write credential.
2. A proposed `curie cluster factory adopt` operation previews candidates by
   default. Applying a reviewed preview explicitly names its binding, agent,
   tracker issue, code repository, PR identity and expected head. The API
   authenticates the operator under its administrative authorization and
   re-reads every authority-bearing fact before committing. Previewing is not
   adoption, and adoption itself does not push, reopen or create a PR.
3. Discovery enumerates all pages of open, closed and merged factory
   publications in the selected configured repository through CodeHost. It
   follows issue references through Tracker, including Jira remote links.
   Historical discovery is resumable and rate limited. A partial scan is
   visibly incomplete and cannot prove that no prior publication exists.
4. Correlation requires the fully qualified immutable tracker issue identity
   and code repository identity, authenticated factory authorship, and
   consistent branch and publication evidence. A branch suffix is an opaque
   lineage identifier, not proof of ownership or an issue number. Tracker state
   labels, Jira links and PR body back-links are search hints until independently
   checked against authenticated objects. A human-authored lookalike grants
   nothing. Changed paths and Jira keys are resolved from immutable ids.
5. Current configuration must permit the selected repository, base and agent
   surface. Recovery must not infer the old agent from a remaining trigger
   label or bind another agent automatically. The operator explicitly confirms
   the ownership assignment and admitting label, including when historical
   ownership is unavailable. Conflicting local ownership, multiple open
   candidates, reused lineage ids, missing provenance or contradictory links
   refuse adoption with candidate links. Operators repair the ambiguity before
   retrying; the command has no force option that bypasses these checks.

### What is rebuilt and what remains unknown

1. Applying adoption atomically creates or links one tracker-qualified
   WorkItem, one lineage and a recovery observation of the existing
   publication. It stores the verified repository and PR ids, binding and
   agent, original branch, base ref, exact head, terminal state where applicable,
   and the actor, time and evidence digest of adoption. Preserve the original
   lineage id when its branch and verified evidence prove it; a collision
   refuses. Recovery has its own durable identity and is idempotent on the
   repository and PR identity.
2. A recovery observation is publication history with explicit provenance,
   not a synthetic successful Publication job. It must remain distinguishable
   from a normal publication throughout API and UI projections. Do not create
   fictional patches, approvals, request outcomes, elapsed times, costs,
   reviewer verdicts or CI repair rounds to satisfy existing row constraints.
   The schema must represent those facts as unknown where evidence is absent.
3. Never reactivate old leases, capabilities, worker claims or unfinished
   approval requests. A surviving partial database's cancellation, denial,
   ownership and version fences cannot be erased by adoption. If cancellation
   history was entirely lost, its absence is disclosed in the preview and the
   operator's explicit continuation grant is new authority. A closed tracker
   issue or removed admitting label permits historical recovery only.
4. Closed and merged PRs restore terminal publication facts and suppress stale
   trigger replay. They do not become revision targets. A verified new trigger
   after completion follows intentional readmission policy; old labels or a
   scan cannot manufacture that event. Historical adoption never reopens a PR
   or treats a merge as proof that the bundle met the ticket's criteria.
5. No tracker body or review transcript becomes a second backlog in SQL.
   Recovery retains identities, lifecycle facts, evidence digests and explicit
   uncertainty, within the data boundary of ADR 0162.

### Authority when storage and the forge disagree

The forge is authoritative for the immutable repository and PR identities,
current branch head, base and open, closed or merged state. The tracker is
authoritative for current issue identity, closure, labels and actor permissions.
The database is authoritative for surviving ownership, cancellation, approved
publication contracts, expected-parent fences and request budgets. Neither a
forge label nor an adoption scan overrides those records.

When the database is absent, applying adoption establishes a new operator
baseline at the reviewed exact head. Commit markers help explain history but
are not signatures or substitutes for that grant. When storage survives and
the head disagrees, recovery reports a conflict and preserves the accepted
head. Reuse ADR 0143's ordinary exact-revision and parent proof where available;
do not generalize adoption into permission to overwrite another writer.

External reads happen outside row locks. Apply then takes the issue and
lineage locks, compares the preview's evidence and expected versions, and
commits all recovered links together. A changed head, terminal transition,
competing owner or stale preview requires a fresh preview. The next publication
still uses exact-parent force-with-lease, current approval policy and the
existing pre-push and post-push PR checks. A database recovery cannot fence
another still-running installation: the operator must quiesce the prior
writers before applying, and the API must refuse known active local owners.

### Review feedback and CI continuation

1. Open adopted publications become eligible for the existing review and CI
   readers only after the atomic recovery commit. Feedback routes through the
   recovered lineage binding to the selected WorkItem and replies in its
   original discussion. Recheck code-host author authority, current feedback,
   head and thread state; retain stable provider-derived request ids.
2. The preview inventories feedback and records a recovery cutoff plus explicit
   dispositions. Historical feedback is not assumed handled or blindly replayed.
   The operator selects still-actionable unresolved feedback for bounded
   continuation, or explicitly waives it; later feedback uses ordinary intake.
   Persist these dispositions so cursor loss cannot replay the recovery batch.
3. CI is read on the exact adopted head and shown as observed, pending, failed
   or unverified. Green checks do not reconstruct preflight evidence or a
   successful lost execution. Observing CI may resume immediately; a model
   repair requires the operator's explicit continuation grant, current policy
   and a new bounded ExecutionRequest. It does not resume a vanished request.
4. The preview states that prior CI rounds, execution budgets and deadlines are
   unknown. Applying a continuation grant names a new finite allowance and
   deadline, records that it starts a recovery session, and consumes the normal
   agent budget and concurrency limits. No automatic poll or repeated adoption
   resets that allowance. Surviving limits remain in force. Unknown required
   proof remains unverified, never green.

### Operator outcome and realizing boundary

The proposed command, factory API detail, Factory view and marked tracker
status comment must show the same recovery disposition: discovered and
suppressed, eligible for adoption, adopted, historical, conflict, or unreadable.
Each includes the existing publication link, a stable reason, and which evidence
or action is missing. An adopted item says it is continuing an existing PR,
who authorized recovery and which history is unknown. A discovery failure must
not look like an idle queue or a completed implementation.

Implementation would extend `apps/api/src/curie_api/forges/ports.py` and its
adapters for complete publication discovery and ownership evidence; integrate
the #4629 guard at `factory_poll_intake.py`, `factory_tracker_intake.py` and
`publication_pulls.py`; add recovery persistence beside `workitems.py` and
`crud/publications.py`; and consume it in `publication_truth.py`,
`github_factory_review.py`, `factory_review_feedback.py`, `factory_ci.py` and
`factory_notices.py`, all under `apps/api/src/curie_api/`. The proposed CLI and
Factory view consume the API's recovery state rather than infer it from labels.
These are proposed responsibilities, not claims of implemented recovery.
Acceptance must name separately tracked implementation work for these paths;
#4629 remains the bounded duplicate-suppression fix.

## Consequences

1. A reinstall does not silently republish or silently resume privileged work.
   Operators can recover useful open review objects without direct SQL edits.
2. Recovery needs complete forge reads, new persistence for observed publication
   facts, and a deliberate operator action. It is slower than restoring a valid
   database backup and cannot recover private patches or execution transcripts.
3. Old publications with insufficient identity or provenance remain suppressed
   and require evidence repair. This design prefers a visible recovery refusal
   to guessing ownership or granting authority from editable text.
4. Recovery starts an auditable bounded session when continuation is requested.
   Its new allowance is an explicit cost decision; it is not a claim that the
   lost session had unused capacity.

## Alternatives considered

1. **Never rebuild; require relabeling or a new issue.** This avoids a recovery
   schema but strands open review history and makes relabeling a potential
   duplicate trigger. Retain refusal for unprovable cases, not as the only
   disaster-recovery policy.
2. **Rebuild lazily on the first intake poll.** This makes recovery convenient,
   but an old label cannot authorize a new agent assignment, accept an unknown
   head or replenish lost budgets. A silent first poll is the wrong authority
   boundary. Keep automatic discovery and duplicate suppression only.
3. **Reconstruct every Publication and approval from forge history.** The
   forge does not hold denied proposals, unpushed patches, cancellation history
   or execution limits. Fabricating them would corrupt the audit trail.
4. **Trust a branch prefix, state label or Jira link as ownership.** Those
   values can be copied or edited and do not prove immutable scope, current
   head or permission. Use them to find evidence, never as write authority.
5. **Restore a database backup as the only supported path.** A coherent backup
   remains the preferred way to recover full history, followed by normal forge
   reconciliation. It does not cover a fresh install against existing factory
   publications or an installation without a usable backup.

## Validation required before implementation is complete

The implementing work must drive empty and partially restored storage through
real API, worker, Postgres and Valkey wiring, with external forge fixtures and
separate live provider proof where required. It must cover open and terminal
PRs across GitHub, GitLab and Jira with GitHub, GitLab or either Bitbucket host;
authenticated authorship versus lookalikes; moved issue keys and repository
paths; ambiguous candidates; partial pagination and rate limits; concurrent
adoption and changed heads; sticky cancellation and denial; stale trigger versus
new relabel; feedback cutoff and cursor loss; bounded CI continuation; and
repeated adoption without duplicate rows, PRs or budget resets. No such runtime
proof is claimed by this Draft.

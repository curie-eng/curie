# 150. Whether the work was right is the bundle's question, not the platform's

Date: 2026-09-10

Status: Draft

Proposed as part of the dark-factory decision set, discussed in
[discussion #2551](https://github.com/curie-eng/curie/discussions/2551).

Builds on [ADR-0014](0014-git-push-is-the-deploy.md) and
[ADR-0019](0019-freeze-eval-case-format.md) (evals are CI for an agent version). It
supersedes no ADR, and it deliberately does not resolve
[ADR-0042](0042-llm-as-a-verifier-grader-and-progress-signal.md) or
[ADR-0132](0132-worst-cohort-eval-gates-and-cross-family-verifiers.md), which
are about grading a bundle rather than grading a task.

## Context

The platform has an excellent record of *what was pushed* and no record of
*whether it was right*. `ThreadPublicationLineage` and `Publication` carry the
repository, branch, pull request number, head, patch, changed paths, status and
error, all under compare-and-set, and the chain terminates the moment the pull
request URL is posted. Nothing in the data model records what a task was
supposed to achieve. There is no acceptance-criteria field, no done-when frame
in the ACI, no per-task verdict, and no runs table in which one could be stored.
The platform also never reads the produced pull request's CI: the publication
client reads open/merged/closed state and the head sha, and nothing reads
check-runs.

The eval plane that does exist is aimed at a different object. ADR-0014 makes a
push to a bundle's branch the deploy, ADR-0019 froze a case format, and the
result is CI *for an agent version*: cases fan out on a bundle push and post a
commit status on the bundle's commit. That is verification of the agent, on a
git-push axis, and it is the right axis for what it grades.

A headless factory needs the other axis — did this pull request do what this
ticket asked — and an earlier framing of this proposal identified building it as
the largest structural gap in the design. It would mean a task record, an
acceptance-criteria field, a verdict, and probably a verifier grader, which is
the ADR-0042 work that is Accepted and unbuilt.

The reason not to build it is not that it is hard. It is that the thing being
verified is not a platform artifact. "Did this satisfy the ticket" is a judgement
about a diff against a prose criterion, made by a model, and it is exactly the
kind of judgement that improves by iterating on a prompt and measuring the
result. A platform release cycle is the wrong loop for that; a bundle is the
right one, because a bundle is versioned, evaluated, and deployable by a push.

There is also a boundary argument. If the platform grades the task, the platform
needs to read the ticket, which means learning a tracker's schema — the thing
[ADR-0145](0145-a-labelled-issue-is-a-backlog-item-and-the-stream-is-its-queue.md)
declined to do — and it needs a model credential and an opinion about what
"done" means, which is bundle territory in every other part of this system.

## Decision

**Per-task verification is bundle behaviour. The platform records what was
produced and does not judge whether it was correct. No task record, no
acceptance-criteria field, and no per-task verdict enters the data model.**

1. **The skill verifies its own work inside its turn.** It reads the ticket, does
   the work, runs the repository's tests, reviews the diff against the ticket's
   acceptance criteria, and only then calls `publish_changes`. A turn that cannot
   satisfy the criteria says so and publishes nothing, which is a far cheaper
   outcome than a pull request that looks finished.

2. **The existing eval plane is how that behaviour is measured.** The factory
   skill is a bundle, its cases are bundle cases, and its quality is graded on
   the axis ADR-0014 and ADR-0019 already established. Iterating on how strictly
   the skill self-reviews is a bundle push, not a release.

3. **The platform's contribution is evidence, not judgement.** What the platform
   already records — the patch, the changed paths, the lineage, the pull request
   URL, the publication status and error — is the evidence a human or a later
   verifier needs. That is where its responsibility stops.

4. **Reading the pull request's CI is optional platform work, and it is a
   reading, not a verdict.** The publication reconciler already holds the
   credential and already calls GitHub for this pull request. Adding a check-runs
   read and persisting the rollup would give an honest "is CI green" column with
   no new credential, no new egress, and no new architecture. It reports what
   GitHub says. It does not decide whether the ticket was satisfied, and it does
   not gate anything.

5. **This is a v1 boundary, held explicitly rather than by default.** The
   argument for a platform-side per-task verdict — that it is the only place a
   *cross-bundle* quality signal could live — is real and is not answered here.
   If a second factory bundle ever needs to be compared against a first on the
   same tickets, this decision is what gets revisited, and ADR-0042's verifier
   grader is where that conversation starts.

## Consequences

The largest item on the gap list stops being platform work. That is the whole
practical effect: an epic-sized data-model change becomes prompt iteration
inside a bundle, measured by machinery that already exists.

Verification quality becomes a property of a bundle version, which means it can
be improved between releases and regressed between releases. A factory whose
skill got worse at self-review produces confident, wrong pull requests, and
nothing in the platform will say so. The compensating control is the bundle's
own eval suite and the human who merges.

**The platform cannot answer "did that ticket succeed."** It can answer "was a
pull request opened, and did it error", which is close enough to look like the
same question and is not. Anyone building the board should be careful with the
column headings, because "9 PRs out" invites the reading "9 tickets done".

The failure signal for a ticket the skill could not satisfy lands in the thread
and the trace, not in a queryable row. An operator asking "which of last night's
forty tickets produced nothing" is reading escalation messages, which is
worse than reading a table and is a known cost of not having the table.

Nothing here forecloses the other axis. Adding a task record later is additive:
no decision in this set depends on its absence, and the evidence the platform
already stores would be its inputs.

## Alternatives considered

**Add acceptance criteria and a verdict to the data model.** Rejected for v1. It
requires the platform to read tickets (declined in ADR-0145), to hold an opinion
about done-ness, and to grow a task record this decision set otherwise avoids —
and it puts the judgement on a release cycle instead of a bundle push.

**Build ADR-0042's verifier grader and point it at task diffs.** Rejected as
mis-aimed rather than wrong. ADR-0042 grades a bundle's *behaviour* against a
frozen case; re-pointing it at per-task work products is a new axis, not a
configuration of the existing one, and ADR-0042 remains unbuilt on its own axis.

**Gate publication on green CI.** Rejected, and it is tempting. It inverts the
order — CI runs on a pull request, which requires the pull request to exist —
so gating on it would mean opening, testing, and closing, which is worse than
opening and letting a human read the checks. The pull request is the artifact
under review, not the candidate for it.

**Have the platform re-run the repository's tests outside the sandbox.** Rejected.
It duplicates the repository's own CI, needs a toolchain per target repository,
and puts the platform in the business of building arbitrary code.

**Say nothing and leave verification undefined.** Rejected, which is why this ADR
exists at all. "The platform does not do this" is a decision with real
consequences for what a board can claim, and it should be findable.

## Realizing code path

None required for the decision itself. The optional check-runs reading in point
4 would land in `apps/worker/src/curie_worker/publication_clients.py` beside the
existing pull request read, with the rollup persisted on the lineage in
`apps/api/src/curie_api/models.py`.

This ADR is **Draft** and authorizes nothing by itself. Under
[ADR-0085](0085-acceptance-not-implementation-authorizes-an-adr.md) as amended
by [ADR-0102](0102-accepted-alongside-implementation-with-explicit-approval.md),
acceptance is a maintainer act.

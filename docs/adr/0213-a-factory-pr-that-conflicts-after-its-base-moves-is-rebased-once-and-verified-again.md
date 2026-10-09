# 213. A factory PR that conflicts after its base moves is rebased once and verified again

Date: 2026-10-09

Status: Draft

Amends the merge conflict ending of the factory CI gate, which shipped for
[#4263](https://github.com/curie-eng/curie/issues/4263) in
[#4270](https://github.com/curie-eng/curie/pull/4270). No ADR recorded that
ending; it lives in `apps/api/src/curie_api/factory_ci.py` and
`docs/operations.md`, so this ADR records it and the change together. It adds
one sanctioned history rewrite to
[ADR 0143](0143-thread-owned-pull-request-lineage.md), keeps the recorded base
of [ADR 0186](0186-a-factory-ticket-declares-its-base-and-keeps-it.md), stays
inside the request lifecycle that
[ADR 0208](0208-a-factory-pr-opened-after-its-request-was-cancelled-is-adopted-by-the-next-run.md)
and [ADR 0206](0206-a-factory-run-lost-with-its-worker-is-re-admitted-as-a-new-attempt.md)
build on, and explains why it is consistent with the no retry after a side
effect rule of [ADR 0013](0013-concurrency-and-delivery-model.md).

## Context

A factory request does not end when its publication succeeds. The workitem
reconciler hands the settlement to `gate` in
`apps/api/src/curie_api/factory_ci.py`, which observes the pull request and
decides with the pure `decide`. Since #4270, `decide` treats a pull request
that GitHub reports as `mergeable: false` with `mergeable_state: dirty`, and
not merged, as conflicted. Inside the first `CI_GRACE_SECONDS` (120 s) after
publication the verdict is `pending` with reason `merge_conflict_grace`; after
that it is `merge_conflict`, whatever the checks say. `gate` settles the
request `failed / merge_conflict` through `workitems.settle_ci_verdict` with no
CI fix round, and the status comment from `apps/api/src/curie_api/factory_notices.py`
tells a person the pull request stays open and the conflicts are theirs to
resolve. Before #4270 the gate waited out the whole CI wait on a conflicted
pull request and could complete it as no CI.

Factory load rounds, driven through a dedicated test GitHub App and fixture
repository, showed the fast fail working as designed: two runs branched from
the same base, a sibling's pull request merged, the second run's pull request
was born conflicting, and the run ended NEEDS HUMAN promptly instead of burning
its CI wait. Every such ending hands a person a pull request whose change may
still be correct and whose only problem is that its base moved.

Four existing mechanisms bear on what the factory could do instead.

1. Publication. A publication carries a patch and a `base_sha`. The worker's
   publication Job (`apps/worker/src/curie_worker/publication_k8s.py`) clones
   the repository, checks out `base_sha`, applies the patch, commits, and
   pushes with `--force-with-lease` pinned to the exact head the lineage
   expects. `build_publication_resources` refuses a payload whose `base_sha`
   differs from `expected_prior_head`, so today every revision is a fast
   forward of the factory's own last head. ADR 0143 calls the lease the compare
   and set for that transition and forbids an unfenced force push or silently
   rebasing another writer's work.
2. Verification preflight. The runner runs the declared checks inside the run's
   sandbox (`preflight_workspace_verification` in
   `runner/src/curie_runner/verification.py`) and records one observation per
   check through `factory_progress.record_verification`. The publication
   precheck and the CI gate both read those observations.
3. The CI fix round. On a caused CI failure below `CI_MAX_ROUNDS`, `gate` calls
   `_continue`, which claims the round in Valkey, fences on the request,
   publication and head through `workitems.hold_for_ci_fix`, and dispatches one
   continuation turn for the SAME request on the same conversation
   (`WorkItemReconciler._dispatch_ci_turn`), with an enqueue marker as the
   idempotency key. The flake rerun (#3741) is a one shot step keyed on request
   and head that does not spend a round.
4. Successor and follow up runs. ADR 0206 admits a new request on the same work
   item after `owner_lost`; ADR 0208 links an orphaned PR so the next request
   continues on it. Both start a new request, a new model run, and a new
   sandbox, and both settle the earlier request first.

## Decision

**When the CI gate would end a factory request `merge_conflict`, the factory
first rebases the run's branch onto the current tip of its recorded base
branch, once, inside the same request and the run's own sandbox. If the rebase
applies with no textual conflict, the rebased change is verified and published
as a revision of the same pull request, and the CI gate waits on the rebased
head exactly as for a fresh publication. Otherwise the request ends
`merge_conflict` as today.**

1. Trigger. The rebase is attempted only where `decide` returns
   `merge_conflict` today: the pull request is open, unmerged, `dirty`, and
   past the 120 s grace, while the request is still `running` in the CI gate
   before its execution deadline. A pull request that conflicts after its
   request reached a terminal state is unchanged by this ADR.
2. Where it runs. The rebase runs in the existing request and its sandbox,
   dispatched through the CI gate's continuation machinery: the same Valkey
   claim, the same `hold_for_ci_fix` fence on request version, publication and
   head, and the same enqueue marker, on the same conversation the CI fix round
   uses. The continuation carries a fixed platform directive rather than prose
   for the model. The runner executes the rebase and the preflight itself
   before any model call, and the model is not asked to touch the branch. It is
   not a successor or follow up run under ADR 0206 or ADR 0208.
3. One attempt. The attempt is keyed on the request, not the head, and is
   recorded durably on the request before any push, as the flake rerun is. It
   never opens or spends a CI fix round and is available at any round,
   including the last. A second conflict on the same request, from the base
   moving again or from anything else, ends `merge_conflict`.
4. The rebase. The runner fetches the recorded base branch (ADR 0186; the
   branch name never changes and no label is re read) and applies the run's
   cumulative change onto its current tip with a three way merge. Any textual
   conflict aborts the attempt, leaves the remote branch untouched, and ends
   the request `merge_conflict` with the conflicting paths in the Reason line.
   The model never resolves a conflict.
5. Verification. On a clean rebase the runner runs the full verification
   preflight on the rebased tree, recording observations as for a first
   publication. Any failed or unreadable required check ends the request
   `merge_conflict` with reason `rebase_verification_failed` and leaves the
   remote branch untouched.
6. Publication. The rebased change is published through the ordinary
   publication path as a revision of the same lineage and pull request, with
   `base_sha` set to the base tip it was rebased onto and the lease still
   pinned to the lineage's current head. This is the one case where `base_sha`
   may differ from `expected_prior_head`, and it is valid only for a
   publication the API marks as this request's rebase. The lease rule of ADR
   0143 is unchanged: if anyone else has pushed to the branch, the push loses
   and the request ends `merge_conflict`.
7. CI on the rebased head. The gate observes the rebased head as it would a
   fresh publication: a new grace window, the pre existing failure exclusion of
   #4105, and the one flake rerun of #3741. A caused failure that remains,
   unreadable or timed out CI, or a renewed conflict ends the request
   `merge_conflict`, with a reason naming which, and the rebase is never
   followed by a CI fix round.
8. Reporting. The status comment and the request record name the rebase: the
   old head, the new base tip, the new head, and its outcome. A person
   reviewing the pull request sees GitHub's own force push entry with the same
   two heads.

The realizing code paths are `decide` and `gate` in
`apps/api/src/curie_api/factory_ci.py` for points 1, 3 and 7; `_continue`,
`workitems.hold_for_ci_fix` and `WorkItemReconciler._dispatch_ci_turn` for
point 2; the runner's continuation handling and
`runner/src/curie_runner/verification.py` for points 4 and 5; the publication
precheck in `apps/api/src/curie_api/routers/publications.py` and
`build_publication_resources` in `apps/worker/src/curie_worker/publication_k8s.py`
for point 6; and `apps/api/src/curie_api/factory_notices.py` for point 8.

### Why the run's own sandbox and not a follow up run

The rebase needs a workspace for the verification preflight, and the CI gate
already owns a fenced, at most once way to put one more step into the running
request's sandbox. A follow up run would settle the request `failed` first,
post a NEEDS HUMAN status that the next run then contradicts, admit a new
request, start a new model run in a new sandbox, and need a new refusal set
alongside ADR 0206's, all to perform a step that needs no model at all.
Keeping it in the request also keeps ADR 0208's lineage rules untouched: the
lineage never leaves the work item, so there is nothing to adopt.

### Why the force push is allowed under ADR 0013

ADR 0013 escalates instead of retrying when a prior attempt flagged a side
effect, because a retry can fire the same tool twice. The rebase is not a
retry. The first publication succeeded and is not repeated; the rebase is a
new forward step chosen from an observed state, and it cannot fire twice:

1. its one attempt is recorded on the request before the push, and the
   continuation's enqueue marker keeps it to one dispatch;
2. its only external effect is a lease fenced push to the branch the
   factory's own lineage owns, pinned to the exact head the factory last
   published, so a replay or a concurrent writer's push makes it lose rather
   than overwrite;
3. it reaches no system other than that branch and its pull request, and the
   prior head stays recoverable from the pull request's force push record.

A failure at any point after the attempt is recorded ends the request NEEDS
HUMAN, which is the escalation ADR 0013 asks for.

### Relation to ADR 0143 and ADR 0186

ADR 0143 forbids silently rebasing another writer's work. This rebase is of
the factory's own head only, proven by the lease, and it is reported in the
status comment and on the pull request, so it is neither another writer's work
nor silent. ADR 0186 freezes the base branch; the rebase follows that same
branch's tip and never retargets, so its rule that Curie does not rebase to
follow a relabel still holds.

## Consequences

1. A factory pull request that GitHub reports conflicting, and whose change
   still applies cleanly to the moved base, completes with no person in the
   loop when verification and CI pass on the rebased head.
2. The silent semantic merge risk is bounded, not eliminated. A rebase with no
   textual conflict can still combine two changes into code that is wrong:
   a function renamed on the base while the run added a caller, a constant
   changed under the run's assumption, a behavior the run relied on removed.
   Requiring the verification preflight and CI to pass again on the rebased
   tree catches only what those checks exercise. A semantic break that no
   declared check or required CI check covers will reach review looking green.
   A repository with thin checks gets correspondingly thin protection.
3. How often the rebase can succeed is an open question to settle before
   acceptance. GitHub reports `dirty` when its own three way merge of the pull
   request into the base has textual conflicts. The rebase in point 4 applies
   the same cumulative change onto the same base tip from the same merge base,
   which is the same three way merge. On that reading, a pull request that
   reached `merge_conflict` will usually conflict on rebase as well, and the
   clean rebase case is limited to whatever GitHub's mergeability reports
   differently from a local merge. The load round evidence above is of that
   kind: two siblings editing the same lines. Acceptance should either confirm
   from load round data that clean rebases occur often enough to justify the
   machinery, or widen the trigger or the resolution before this is built.
4. The branch history of a factory pull request is rewritten once per request
   at most. A reviewer's earlier comments on the old head may show as outdated,
   and a repository whose branch protection dismisses stale approvals will
   dismiss any approval given before the rebase.
5. The rebase, its preflight, and the second CI wait all run inside the
   request's existing execution deadline. A request near its deadline may end
   `merge_conflict` or `ci_timeout` instead of completing, as a CI fix round
   can today.
6. The publication Job and precheck gain exactly one shape where `base_sha`
   differs from `expected_prior_head`, tied to the request's single recorded
   rebase. Any other mismatch is still refused.
7. Every ending after a failed rebase still says `merge_conflict`, so tracker
   automation and dashboards keyed on that cause see no new cause. The Reason
   line distinguishes a textual conflict, failed verification, failed CI, and
   a second conflict.

## Alternatives considered

1. Keep today's fast fail. Rejected as the end state: every base move that
   conflicts costs a person a rebase the factory could attempt with the same
   evidence bar it uses for a first publication. It remains the behavior for
   every case the rebase cannot clear.
2. Rebase in a follow up run under ADR 0208 lineage rules. Rejected: it
   settles the request failed and then contradicts itself, costs a new model
   run and sandbox for a step that needs no model, and adds a successor path
   beside ADR 0206 with its own refusal set to keep correct.
3. Merge the new base into the branch instead of rebasing, as Draft
   [ADR 0165](0165-the-tracker-owns-dependencies-and-curie-admits-ready-work.md)
   proposes for stack repair at retarget. That avoids the force push, but a
   two parent merge commit is not a patch on a single base, which is the only
   shape the publication Job builds, and it conflicts in exactly the cases the
   rebase does. Rejected for this decision; ADR 0165's stack repair is a
   separate path and is not changed here.
4. Let the model resolve textual conflicts in a continuation turn, bounded by
   the same verification and CI. Rejected: it widens the semantic merge risk
   from code no one changed to code the model wrote to reconcile two intents,
   and the decision that conflicts belong to a person stands. Consequence 3
   names it as one way to widen the decision if clean rebases prove rare.
5. Unlimited rebase attempts, or one per base move. Rejected: a busy base can
   keep a run rebasing until its deadline, and each attempt is another
   rewrite of a branch under review. One attempt bounds both.
6. Use GitHub's update branch API. Rejected: its REST endpoint merges rather than
   rebases, pushes outside the lease fenced publication path, and gives the
   preflight no workspace to run in.

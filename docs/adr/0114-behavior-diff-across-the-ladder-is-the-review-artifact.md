# 114. A cross-tier behavior diff is the review artifact for a bundle change

Date: 2026-08-19
Status: Draft

## Context

A code diff is close to useless for reviewing a bundle change. Three words in a
skill file can change what the agent does; a two hundred line refactor can change
nothing. The reviewer sees the same kind of diff either way and has no evidence
about behavior, so approval rests on the author's account of what the change does.

The eval plane answers one question today: did this suite pass. It reports
`N/M passed` as a commit status. That number cannot express the two things a
reviewer actually needs to know:

1. What behavior changed, relative to the bundle currently deployed. A suite that
   passed before and passes now is reported identically whether the agent took
   the same route or a completely different one.
2. Whether that behavior holds at every rung of the ladder. The ladder already
   proves that a rung DISAGREES. It never says why, so a cluster-only failure
   arrives as a red check and a manual investigation.

The trajectory is already available and already discarded. `EvalRunner._run_case`
accumulates the ordered tool names to feed the `tool_called` grader
(ADR-0022) and drops the list when the turn ends. Nothing downstream can ask what
route a turn took, which is why no consumer today can see a route change.

Prior art matters here and should be stated rather than discovered in review.
Braintrust's `eval-action` posts per scorer eval deltas as pull request comments;
LangSmith, Langfuse, Confident AI, and Arize occupy adjacent space. Posting eval
results on a pull request is not a new category. Separately, dev/prod parity is a
named 12-factor problem, and the industry answer to it is to run Kubernetes
locally so the environments match. Nobody in either group DIAGNOSES a
disagreement between environments, because nobody else runs one immutable bundle
and one eval suite across a fixed ladder of tiers. That is the asset this ADR
proposes to spend.

## Decision

Report a bundle change as a behavior diff against the deployed bundle, computed
per tier, and surface it in the terminal and on the pull request.

**1. The observed tool-call trajectory is part of an eval result.**
`EvalCaseResult` carries `trajectory: tuple[str, ...]`, the ordered tool names the
turn invoked. It is recorded on every result the runner constructs after
streaming begins, including failures: a partial route names where a failing tier
died, which is most of the diagnosis. `aggregate` carries the representative
sample's trajectory rather than a union, so the route stays consistent with the
`output` and `detail` beside it. This is a result shape, not the frozen eval case
shape, and it is additive with a default, so no consumer breaks.

**2. A behavior change is measured per tier, and combined only if the tiers
agree.** Each tier of the candidate is diffed against the SAME tier of the
baseline, which is what holds the environment constant. When every comparable
tier reports the same difference, that difference is the change. When they
disagree, the row is `CONFOUNDED`: the bundle and the environment both moved and
the report declines to say which. Choosing one tier as the single point of
comparison would report an allowlist gap to the author as a behavior regression,
which is the specific error this rule exists to prevent.

**3. Tier agreement is a second, independent axis on the same row.** A row
carries both `change` (versus the deployed bundle) and `tier_agreement` (within
the candidate). They are not collapsed. A case can be unchanged and still
disagree across tiers, which is a latent environment problem the change merely
revealed, and that is worth reporting even though nothing about the bundle moved.

**4. A tier disagreement is attributed by signature match, and may go
unattributed.** A short table maps runner error text to a named cause and, where
one exists, a remedy command. No catch-all entry. An unmatched failure renders as
`unclassified`, and the count of unclassified rows is a headline number in the
report. A confidently wrong cause costs a reviewer more than an admitted unknown.

**5. A difference that does not reproduce is reported as suite instability, not
as a change.** The diff takes several repeats of the candidate. A row whose
classification is identical in every repeat is a finding; a row that classifies
differently is `flaky`, carries its disagreement rate, and is excluded from the
change count.

**6. The surface is `curie dev tier-diff`, reading run artifacts.** It does not
run evals; it reads artifacts the platform produces, so it is offline and needs no
credential. It lives in the `dev` namespace because the report's JSON has no
committed versioned schema yet. Finding a difference is a successful run and
exits 0; `--fail-on-change` is the opt-in for a CI gate. Exit code 3 is not
reused for "found something", because 3 means transient and a retry cannot change
a diff.

## Consequences

A reviewer sees which cases changed verdict, which changed route while still
passing, which tiers disagree, the named cause and remedy where there is one, and
how much the report could not explain. Approval stops resting on the author's
account.

The report is deliberately quiet. Unchanged, tier-consistent rows are counted and
not listed, because a report that prints every case is a report nobody reads by
the third time, and then the one row that matters scrolls past.

Three weaknesses are accepted and must be stated whenever this is presented.

- **The category is not novel.** Posting eval deltas on a pull request is done.
  What is not done is diffing the trajectory rather than the scores, diffing
  against what is actually deployed rather than a branch, and carrying the tier
  axis at all. If the tier axis is dropped, this becomes a reimplementation of
  existing tools and should not be built.
- **Eval determinism is a real risk.** Two runs of one bundle can differ, and a
  differ over nondeterministic inputs manufactures findings. The mitigation is to
  report only what reproduces across repeats and to show the flake rate as a
  first-class number rather than hiding it. This reduces the risk; it does not
  remove it.
- **Attribution is a heuristic.** The signature table matches text that runners
  happen to emit today. It will miss causes, and its entries can go stale when a
  message changes. That is why `unclassified` is a designed outcome with its own
  headline count, and why no catch-all entry is permitted.

Debts this ADR does not pay, named so they are not mistaken for done:

- No committed versioned JSON schema for the report, and no `cli/schema` entry.
  That is what a promotion out of `dev` owes.
- No pull request comment poster. The Markdown surface exists; the thing that
  posts it does not.
- No artifact writer on the eval commands. The tiers produce results; nothing yet
  writes them out in the artifact shape the differ reads.
- `Tier` is declared in the diff module while `cli/scripts/e2e-ladder.sh` names
  the same three rungs in shell. Two declarations of one vocabulary can drift.

## Alternatives considered

### Diff pass rates instead of trajectories

This is what existing tools do and it is cheaper. It cannot see a case that
passes both before and after while the agent stops calling a tool, which is the
change class most likely to reach production unnoticed. Rejected because the
invisible class is the valuable one.

### Compare against the pull request base branch

Cheaper, and it is what a code diff does. The base branch is not what is running.
A bundle is promoted from a built artifact, so the honest baseline is the deployed
version, and comparing against a branch would report differences that no operator
would ever experience.

### Pick the heaviest tier as the single comparison point

Simplest to implement and it produces one clean verdict per case. It reports an
environment failure as a behavior regression, sending an author to audit a prompt
when the problem is an egress rule. Rejected: the false attribution is worse than
the extra axis.

### Collapse the two axes into one ranked verdict

A single enum per row would render more compactly. It destroys the distinction
between "unchanged but the tiers disagree" and "changed identically everywhere",
which is precisely the distinction that decides who should look at the row.

### Infer the cause with a model instead of a signature table

An LLM would attribute more failures and would sometimes be wrong in a fluent,
convincing way. A wrong cause is the expensive failure mode for this feature.
Rejected for now; a deterministic table that admits ignorance is the safer
starting point, and a model could later propose entries for the table rather than
verdicts for the report.

### Run the evals as part of the diff

More convenient in one command, and it would let the tool guarantee its own
inputs. It also makes the tool need credentials, a cluster, and minutes per
invocation, and it welds two separable jobs together. The eval plane already runs
evals; this reads what it produced.

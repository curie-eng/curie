# 199. The factory admits only tickets it can start and finish

Date: 2026-10-05

Status: Accepted

Accepted 2026-10-05 with explicit maintainer approval from Brian Conn
(TheConnMan), given after the Draft merged in
[#4020](https://github.com/curie-eng/curie/pull/4020).

This ADR decides a viability gate at dark factory admission. It partially
amends two Accepted ADRs, back-linked on each under
[ADR 0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md):

1. [ADR 0187](0187-the-factory-polls-github-and-the-platform-reads-the-issue.md),
   "The platform reads the issue for the bundle", item 3: the platform reads
   and classifies the ticket once at admission. It still stores no ticket
   content, and the sandbox read stays verbatim and unparsed.
2. [ADR 0145](0145-a-labelled-issue-is-a-backlog-item-and-the-stream-is-its-queue.md):
   a selection label requests admission, and only a passing viability verdict
   permits it.

Draft
[ADR 0165](0165-the-tracker-owns-dependencies-and-curie-admits-ready-work.md)
was revised in the same change to conform: it drops the visible
`curie:blocked` waiting state, automatic admission when blockers clear, and
stacking a dependent on an unmerged prerequisite PR, and becomes the
dependency read behind decision 2's first start check. It composes with
[ADR 0186](0186-a-factory-ticket-declares-its-base-and-keeps-it.md) (base
resolution runs first) and
[ADR 0197](0197-the-factory-reaches-code-hosts-and-trackers-through-two-ports.md)
(the probes run through the Tracker and CodeHost ports).

## Context

The maintainer's rule for factory work: a ticket is implementable only if every
precondition is already met, every decision is already made, and the factory
can achieve and verify every acceptance criterion with no human in the loop.
"Awaiting human" is not a stage the factory has. A no on "can it start" or "can
it finish" means the ticket needs human judgment, which the factory does not
do. Such a ticket is rejected and returned to the tracker with the reason. It
is never parked.

Today nothing checks that before a run starts. File and line references below
are as of `origin/next` at `08683759d`.

1. **Admission is one function, and it already has a reject precedent.**
   `admit_notice` (`apps/api/src/curie_api/github_factory.py:424`) is the single
   admission path for the webhook and both pollers
   (`factory_label_reconcile.py:274-277`,
   `factory_poll_intake.py:378` `_admit_labeled`). For a fresh admission it
   resolves the base with `_fresh_base` (`github_factory.py:394-421`, called at
   `:449`). When base resolution fails, `factory_base.comment_refusal`
   (`factory_base.py:190`) upserts one marked comment
   (`REFUSAL_MARKER`, `factory_base.py:29`), the function raises
   `FactoryRefused`, and no WorkItem is created. The comment says Curie "will
   pick the issue up again on its next pass" (`factory_base.py:181-186`). That
   is a reject with a reason, no WorkItem, and re-admission through the normal
   intake: the shape this ADR generalizes.
2. **The platform does not look inside the ticket.** ADR 0187 has the platform
   read the issue for the sandbox and return it verbatim; `issue_read.py:1-8`
   says "Nothing is parsed, modelled or stored". Dependencies are ADR 0165,
   which is Draft and unimplemented: a ready, blocked or unknown assessment
   from the tracker's native blocked-by relation only.
3. **The only viability judgment is the agent's own, inside a paid run.** The
   bundle's phases are `examples/dark-factory/progress/phases.json`
   (`read_issue`, `pin_criteria`, `plan`, and on). The skill stops when the
   issue cannot be read (`skills/implement-issue/SKILL.md:160-162`) and
   "instead of guessing" when the request is ambiguous, a required fact is
   missing, or the criteria contradict each other or the repository
   (`SKILL.md:172-176`), ending with `Could not complete:`. Those stops overlap
   the gate this ADR proposes, but they run after a sandbox is claimed and the
   repository is cloned. Every failed run gets `curie-factory:needs-human`
   (`factory_notices.py:380-398`, `_DESIRED_LABEL["failed"]`). There is no
   outcome that says "this ticket was not something the factory can do".
4. **The runner's capabilities are fixed and mostly undeclared.** The platform
   runner has Python 3.13, Node 22 and git (`runner/Dockerfile:27-39`). The
   dark factory layer adds uv, Rust 1.95 and pnpm
   (`examples/dark-factory/runner.Dockerfile:12-19`). There is no docker,
   kubectl, helm, gh, browser, or GitHub credential in the sandbox, and
   `examples/dark-factory/connectors.yaml:6` declares `connectors: {}`, so there
   is no end-to-end connector. The one declared capability today is the
   verification declaration (`runner/src/curie_runner/verification.py:1-20`):
   checks from the bundle's `verification/checks.json` or the repository's
   `.curie/verification.json`, each routed executable, delegated to a named
   required PR check, or blocked. The dark factory bundle ships no
   `checks.json`. The runtime fallback is the boot preflight block
   (`runner/src/curie_runner/__main__.py:636`, returned at `:1078-1079`), which
   stops a run before the model starts, again after the sandbox is paid for.
5. **Publication approval parks runs on a person.** For a manual install the
   README says "Human approval of each pull request stays the default"
   (`examples/dark-factory/README.md:197-198`). Under that policy a finished run
   waits for a human before its PR exists. The quickstart sets `auto`
   (`cli/src/factory_quickstart.rs:1256-1264`).
6. **The cost asymmetry is large.** A factory run is about $1 of model spend and
   25 to 70 minutes of wall time, plus a sandbox claim. A single bounded
   classification call is cents and seconds. A ticket that cannot finish costs
   the whole run, and then a person still has to read the needs-human card to
   learn what a two-line rejection comment would have said up front.

Two repository facts sharpen the start question. A closing keyword in a PR into
`next` does not close the issue (`AGENTS.md:1000-1004`), so issue state alone
cannot say whether a blocker landed or the issue is already fixed. And a
prerequisite that exists only on an open PR is not in the base the factory
branches from.

## Decision

**Every fresh factory admission passes a binary viability gate before any
planning. The gate asks two questions, can it start and can it finish, answers
them with deterministic probes and at most one bounded model call, and either
admits or rejects. A rejection is a distinct outcome with its own label and
one marked comment, returned to the tracker. There is no waiting state.**

### 1. The gate is binary and runs at admission, after base resolution

1. The gate runs inside `admit_notice`, on the fresh-admission branch only,
   immediately after `_fresh_base` returns a resolved base and before
   `workitem_dispatch.readmit`. A mention revision, or work that already has an
   open factory PR, keeps today's path; it is not a new ticket.
2. The ADR 0186 base refusal stays the first start check and keeps its own
   comment and marker. It also applies the rejection label (decision 5), so one
   tracker query finds all rejected work.
3. The gate returns exactly one of `admit` or `reject` to the caller. A third,
   internal `unknown` exists only for outages (decision 6) and never leaves the
   platform.
4. A rejected ticket gets no WorkItem, no ExecutionRequest, no sandbox, and no
   execution budget. As with the base refusal, `FactoryRefused` carries the
   reason code.

### 2. Two questions, each a fixed list of checks

**Can it start.** All of these must hold against the resolved base commit:

1. **Blockers merged.** Every native blocked-by relation on the issue (through
   the Tracker port's dependency operation, ADR 0197) resolves to a merged PR
   whose merge commit is an ancestor of the resolved base commit. An open PR,
   green or not, does not satisfy a blocker. An issue closed as not planned
   never does.
2. **Prerequisite paths present.** Every repository path the ticket's
   Preconditions section names exists at the resolved base commit (CodeHost
   read of the commit's tree).
3. **No open PR claims the issue.** No open pull request on the repository
   carries a closing reference to the issue. The factory's own open PR never
   reaches the gate; decision 1.1 routes it to revision.
4. **Not already fixed.** No merged PR with a closing reference to the issue
   has its merge commit in the resolved base.
5. **Shape and decisions.** The ticket has the sections decision 7 requires,
   its Base section agrees with the resolved base, and its Decisions and
   Preconditions contain no open question, alternative left to the
   implementer, or precondition that is neither probed true here nor verifiable
   from the base.

**Can it finish.** All of these must hold:

1. **Every acceptance criterion has a verification the factory can run.** Each
   criterion names a Verified by and a Runs in. `Runs in: sandbox` passes only
   if every executable and service the verification needs is in the runner
   capability manifest (decision 3). `Runs in: ci:<check>` passes only if
   `<check>` is a required check on the resolved base branch (CodeHost read) and
   the manifest's verification declaration routes a check delegated to it.
2. **Every criterion is achievable in the sandbox.** Nothing a criterion
   requires (a deploy, a cluster, a browser, a third-party account, a manual
   step, a person's sign-off) is outside the manifest.
3. **Publication completes without a person.** The agent's publication policy
   is `auto` (decision 8). "Finish" means a published PR whose required checks
   are green on its exact head. Merging stays a human act after the factory is
   done, and is not part of the run.

A no on any check is a rejection that names the question and the check.

### 3. The runner capability manifest

The gate compares criteria against a declared, closed-world manifest of what a
run can do. Anything not in it is absent.

1. **Contents.** The manifest generalizes the verification declaration. It
   lists:
   1. executables on the runner `PATH` with their versions (for the dark
      factory: python3, node, npm, git, uv, cargo, rustc, pnpm, and the build
      toolchain);
   2. services the sandbox can reach (none today);
   3. declared connectors and their tools, from the bundle's `connectors.yaml`
      (none today);
   4. the resolved verification declaration: each check id, its paths, and its
      route (executable, or delegated to a named required PR check);
   5. the network egress class and the absence of any code host credential in
      the sandbox (ADR 0187 consequence 5).
2. **Authorship.** Nobody hand-writes the manifest. The bundle author owns
   its inputs: `runner.Dockerfile` (ADR 0173), `connectors.yaml`, and
   `verification/checks.json`. The repository maintainer owns
   `.curie/verification.json`. `curie build --plugin-dir` generates the
   executable list by probing the built runner image, so the manifest cannot
   claim a binary the image lacks, and `curie example dark-factory render`
   locks it with the runner digest it already locks. The deployment records the
   manifest and its digest with the deployed bundle, where the API reads it.
3. **Required checks are read live.** Which PR checks are required on the
   base comes from the code host at admission, not from the manifest, because
   the repository owns branch protection.
4. **The preflight block stays.** `verification.py` and the boot preflight
   block remain the runtime fallback for drift between the manifest and the
   running image. A preflight block after a passing gate is a gate miss and is
   counted as one (decision 8.3).

### 4. Deterministic probes first, then at most one bounded model call

1. **Order.** The start checks 1 to 4 and the shape half of check 5 are
   deterministic reads and parses, cheapest first. The first failure rejects,
   and no model call is made.
2. **The model call.** Only a ticket that passes every deterministic check
   gets one classification call. It judges what a parser cannot: whether
   Decisions and Preconditions leave anything open, and whether each criterion's
   Verified by needs only what the manifest lists. Its input is the ticket text,
   the manifest, and the probe results. Its output is a fixed schema: for each
   question, pass or fail, and for each failure, the criterion or section
   number and a manifest reference or missing-capability name. Free text is
   limited to one short reason per failure.
3. **Bounds.** No tools, a single request, a fixed output token cap, a fixed
   timeout, a per-deployment classification model setting, and the agent's
   existing provider credential. At most one call per issue fingerprint
   (decision 4.6).
4. **Issue text is untrusted.** It is passed as delimited data, never as
   instructions. The call has no tools and no side effects. Its admit vote is
   necessary, not sufficient: the deterministic checks already passed, and the
   model can only confirm or reject. A prompt injected into an issue can at
   worst admit a ticket into a normal factory run, which is what every labelled
   ticket gets today, and only a user with write access can apply the label
   (ADR 0161).
5. **Nothing is stored but the verdict.** The platform keeps the issue
   identity, a digest of the classified input, the manifest digest, the
   verdict, reason codes, evidence references (criterion numbers, PR and issue
   numbers, check names, paths), and the observation time. It stores no ticket
   text, consistent with ADR 0145.
6. **Fingerprint.** The digest covers the title, the body, and the label set
   excluding the factory's own `curie-factory:*` state labels. A pass whose
   fingerprint matches a stored rejection does not re-run the gate.

### 5. Rejection is its own outcome, and re-admission is an edit or a relabel

1. **Label.** A rejected ticket carries `curie-factory:not-implementable`.
   It joins the closed set of factory state labels (`STATE_LABELS`,
   `factory_notices.py:380-385`) and is never `curie-factory:needs-human`. The
   selection label is left alone: the factory never removes human labels
   (`factory_notices.py:377-379`).
2. **One marked comment.** The gate upserts one comment with its own marker,
   edited in place and never duplicated, the same mechanism as
   `comment_refusal`. It names the failed question (can it start or can it
   finish), each failing check with its evidence, and what changes would pass,
   for example "blocked by #N: PR #M is open and not merged into `next`", or
   "AC3 runs in `sandbox` but needs `docker`, which this runner does not have;
   delegate it to a required CI check or split it out".
3. **Re-admission.** Editing the issue or changing its labels changes the
   fingerprint, and the next intake pass runs the gate again. Removing and
   reapplying the selection label does the same. Nothing else re-admits a
   rejected ticket: a blocker merging, a deploy that adds a tool, or time
   passing does not. The person who fixes the cause says so with an edit or a
   relabel.
4. **On admission after a rejection.** The label pass replaces the rejection
   label with `curie-factory:queued`, and the rejection comment is edited to
   say it was superseded by an admission.

### 6. Outages are an internal unknown, never a human wait

1. A code host or tracker error, rate limiting, incomplete pagination, a model
   provider error or timeout, or a model response that fails the schema makes
   the verdict `unknown`.
2. `unknown` creates no WorkItem, posts no comment, applies no label, and
   leaves any earlier verdict on the issue untouched. The gate retries on later
   intake passes with capped exponential backoff.
3. `unknown` is visible to the operator only: a log line, a metric, and the
   admission read in the CLI. A ticket that stays unknown past a deployment
   threshold raises an operator alert. It never reads as waiting on a person
   on the issue.
4. ADR 0165's unknown assessment maps to this state; its blocked assessment
   is a start rejection.

### 7. The ticket shape the gate expects

A factory ticket has these sections, as headings in the issue body:

1. **Problem**: what is wrong or missing, observable on the base.
2. **Base**: the branch, matching the `base:` label or the deployment default
   (ADR 0186). The label stays authoritative; a mismatch is a rejection.
3. **Blocked by**: issue numbers, matching the tracker's native blocked-by
   relations, or "None". The native relation stays authoritative (ADR 0165).
   A mismatch is a rejection that says to set the relation natively. A tracker
   whose port reports the dependency operation unsupported uses this section.
4. **Decisions**: every choice already made, with its answer. No open
   questions.
5. **Preconditions**: facts that must already be true on the base, with any
   repository paths in code spans so the path probe can read them, or "None".
6. **Acceptance criteria**: a numbered list. Each criterion has a **Verified
   by** (a command, or a test file and test name) and a **Runs in** (`sandbox`
   or `ci:<required check name>`), the same two locations the bundle's
   verification contract uses (`examples/dark-factory/verification/contract.md`).
7. **Out of scope**: what the run must not change.

### 8. Consequences for publication approval and the bundle's ambiguity stop

1. **Publication defaults to `auto` for factory agents.** A require-approval
   policy makes every run wait on a person, so under it no ticket can finish.
   Deploying the dark factory bundle sets `auto` as the quickstart already does,
   and the README line changes to match. An operator may still set a
   require-approval policy; the gate then rejects every ticket under can it
   finish with that reason, rather than letting runs park. Human review stays
   where it belongs, on the PR, before merge.
2. **The ambiguity stop stays, as defense in depth.** The `pin_criteria` stop
   in `SKILL.md` is unchanged in what it detects. After admission it should be
   rare. When it fires, the run ends with the not-implementable outcome and the
   agent's questions, not needs-human, because the cause is the ticket, not the
   run.
3. **Gate misses are measured.** A run that ends in the ambiguity stop or a
   preflight block after a passing verdict records a gate miss against the
   verdict's fingerprint. That is the gate's false-admit rate.
4. **`needs-human` narrows.** It stays for runs that fail for run reasons
   (`NEEDS_HUMAN_CAUSES`, `factory_notices.py:161-163`). A ticket-quality
   failure no longer lands there.

## Consequences

1. Non-implementable tickets cost cents and seconds instead of about $1 and up
   to 70 minutes, and the author gets a specific reason on the issue instead of
   a needs-human card.
2. The factory's run outcomes separate into "the ticket was not doable" and
   "the run failed", which makes run failure rates meaningful.
3. Tickets without the decision 7 shape are rejected. Existing labelled
   tickets in that state will be rejected on their next intake pass after the
   gate ships. The rejection comment lists the missing sections, and the bundle
   should ship an issue template with the shape.
4. The API gains an outbound model call at admission, made with the agent's
   existing provider credential, and a new verdict table with a migration. It
   still stores no ticket content.
5. The API reads the issue body at admission, which ADR 0187 did not allow.
   The sandbox read stays verbatim and unparsed.
6. Dependent work no longer stacks on an unmerged prerequisite PR, and a
   blocked ticket no longer waits and auto-admits. Draft ADR 0165 is revised
   to match: no `curie:blocked` state, no automatic admission when blockers
   clear, and no stacked publication. A dependency graph now lands
   in merge order, and a person relabels each dependent once its blockers
   merge. That is slower for deep graphs. It is the cost of having no factory
   owned waiting stage.
7. A model call per new fingerprint lengthens the poll pass, which runs on one
   replica under one lock (`factory_poll_intake.py`). The bounded timeout and
   the fingerprint cache keep that proportional to new and edited tickets.
8. The runner capability manifest becomes part of the bundle's build output,
   so changing the runner image or connectors changes what the gate admits.
   Earlier verdicts are not re-evaluated by a deploy (decision 5.3).
9. Gate accuracy depends on the classification model. The deterministic checks
   carry every fact a parser can establish, and the gate miss metric shows how
   often the model admits what the run then cannot do.

## Alternatives considered

### A bundle phase before plan

Add an `admit` phase ahead of `read_issue` in `phases.json` and let the agent
judge viability. Rejected. It runs after a sandbox is claimed, the runner image
is pulled and the repository is cloned, so a rejection costs most of a run's
floor. It ends as a failed run labelled needs-human unless the platform adds
the same distinct outcome anyway. Enforcement depends on model behavior inside
the run, and each bundle would implement it again. The platform already holds
every input the start checks need. ADR 0165 rejected letting the bundle
discover blockers after launch for the same reasons.

### No gate, better tickets only

Rely on the implementable-ticket discipline when tickets are filed. Rejected.
It checks a ticket once, when it is written, and tickets go stale: a blocker
that was merged is reverted, a path is renamed, a PR claiming the issue
appears, the base moves. Any user with write access can apply the label to any
issue. Nothing checks at the moment the factory commits money to a run. Better
tickets remain the goal; the gate is what holds them to it.

### Park the ticket for a human

Keep a visible waiting state (a `curie:blocked` or awaiting-human label) and
resume automatically when the cause clears. Rejected under the maintainer's
rule. It makes the factory own a stage whose exit is a person's action, keeps
work out of the tracker's normal flow while looking in progress, accumulates
stale parked items, and needs the factory to track every condition that might
clear it. A rejection returns ownership to the tracker, where people already
triage.

### A model-only gate

One classification call over the whole ticket, with no deterministic probes.
Rejected. Blocker merges, path presence, claiming PRs and required checks are
facts the code host answers exactly. A model would answer them less reliably,
at higher cost, with a larger injection surface, and differently on two
evaluations of the same ticket.

### Remove the selection label on rejection

Rejected. The factory never touches human labels, and removing the label would
mean an edit alone could never re-admit the ticket, because the poller lists
only labelled issues.

### Store the parsed ticket for later runs

Rejected. ADR 0145 keeps tracker content out of Curie, ADR 0187 explains why a
copy goes stale, and the sandbox already reads the live issue.

## Realizing code paths

These are the proposed integration points, not a claim that they implement
this decision today.

1. `apps/api/src/curie_api/github_factory.py`: `admit_notice` calls the gate
   on the fresh-admission branch after `_fresh_base`.
2. A new `apps/api/src/curie_api/factory_admission_gate.py`: ticket shape
   parse, start and finish checks, the fingerprint, the classification call,
   and the verdict.
3. `apps/api/src/curie_api/models.py` and a new Alembic migration: the
   verdict table, and the state label check constraint that today enumerates
   the four `curie-factory:*` labels.
4. `apps/api/src/curie_api/factory_notices.py`: `STATE_LABELS` gains
   `curie-factory:not-implementable`; the rejection comment marker and body.
5. `apps/api/src/curie_api/factory_base.py`: the base refusal applies the
   rejection label.
6. Tracker and CodeHost ports (ADR 0197): dependency relations, claiming
   and closing PRs for an issue, path presence at a commit, required checks on
   a branch, and commit ancestry.
7. `runner/src/curie_runner/verification.py` for the declaration schema,
   `cli/src/connector_build.rs` for probing the built bundle runner layer in
   `curie build`, and `cli/src/examples.rs` for the lock in
   `curie example dark-factory render`.
8. `examples/dark-factory/`: README publication default,
   `skills/implement-issue/SKILL.md` ambiguity stop outcome, and an issue
   template with the decision 7 shape.
9. `docs/operations.md`, "Admitting a labelled GitHub issue": the gate, its
   label, its comment, and re-admission.

## Follow-up issues after acceptance

To be filed only after this ADR is Accepted, each written to the
implementable-ticket shape it describes:

1. Gate core: the fresh-admission hook in `admit_notice`, the deterministic
   start checks, the ticket shape parser, the fingerprint and verdict table,
   and the internal unknown with backoff.
2. Rejection outcome: the `curie-factory:not-implementable` label in the state
   label set and constraint, the marked rejection comment, supersession on
   admission, and the base refusal applying the label.
3. Runner capability manifest: generation in `curie build`, the lock in
   `curie example dark-factory render`, recording with the deployed bundle,
   and the API read.
4. Finish checks: criterion mapping against the manifest and live required
   checks.
5. Classification call: the per-deployment model setting, credential
   resolution, output schema, bounds, and prompt injection tests.
6. Port operations under ADR 0197 for the probes, shared with ADR 0165 where
   they overlap.
7. Publication default: `auto` on dark factory deploy, the README change, and
   the gate reason under a require-approval policy.
8. Bundle: the ambiguity stop reports the not-implementable outcome, the gate
   miss metric, and the issue template.
9. Operator surface: the admission read in the CLI showing the verdict,
   reasons and unknown state, and the operator alert threshold.
10. A labelled admit and reject corpus to measure gate accuracy before and
    after release.

---
name: implement
description: Implement a ticket or feature with proportional planning, test first behavior changes, independent review when available, and verification through the real affected surface.
# disable-model-invocation sits under metadata because the Agent Skills spec
# only allows a fixed set of top-level fields; Claude Code reads this key only
# at top level, so it no longer takes effect here. Full rationale:
# docs/interfaces/bundle-format/INTERFACE.md
metadata:
  disable-model-invocation: "true"
---

# Implement

**Usage:** `/implement [ticket URL, ID, or description]`

Use this as the default entry point for a ticket with clear acceptance criteria.
Use the lightest process that can prove the requested behavior. Stop and ask for
direction when the ticket cannot be read, its acceptance criteria are materially
ambiguous, or implementation would require a decision outside the ticket.

## Portable provider policy

Use the active environment for implementation. Do not require a particular agent,
model, CLI, plugin, or hosted service.

If another independent provider is available locally, use it for a read only plan
or diff review. Claude, Codex, and OpenCode are examples, not requirements. Keep
execution with one provider and use another only to challenge the plan or completed
diff. When no independent provider is available, use a fresh reviewer context in
the active environment and state that the review was single provider.

Never install, configure, authenticate, or invoke a provider solely to satisfy this
workflow. Never depend on home directory paths, custom profiles, personal hooks,
metrics, queues, databases, background jobs, private credentials, or machine local
helper scripts. Repository instructions and available tools are authoritative.

## Setup

1. Read the ticket and its linked requirements. Do not infer scope from an ID alone.
2. Read the repository instructions and the relevant architecture or component docs.
3. Check for an existing branch or worktree for the ticket. If one exists, read
   `.projects/plans/<branch-name>.state.json` inside it and resume from the stage
   it records rather than repeating finished work. See Run state below. Otherwise
   select the target release train before creating work:

   | Work | Base and PR target |
   | --- | --- |
   | General bug, security fix, or shared change | `main` |
   | v0.7 feature or a bug unique to unreleased v0.7 work | `next` |

   Fetch the chosen base and create the worktree from its remote tip:

   ```bash
   git fetch origin <base>
   git worktree add <path> -b task/<short-description> "$(git rev-parse origin/<base>)"
   ```

   Never commit directly to either release train. If the ticket does not make
   the target clear, stop and ask.
4. Run the focused existing tests, type checks, and lint for the affected area. Stop
   on a relevant baseline failure. Record unrelated failures without widening scope.

## Run state

Keep a small state file inside the worktree at
`.projects/plans/<branch-name>.state.json`, beside the plan when there is one.
`.projects/` is git ignored, so it never reaches a commit.

```json
{
  "branch": "task/short-description",
  "ticket": "<id or null>",
  "path": "direct | quick | build",
  "stage": "setup | triage | plan | de risk | build | review | done check | finish",
  "status": "in progress | blocked | done",
  "open_findings": ["<review finding not yet fixed>"],
  "e2e": {
    "decision": {
      "skill": { "status": "required | not applicable", "reason": "<surface reached, or the surface this change does not reach>" },
      "local": { "status": "required | not applicable", "reason": "<...>" },
      "local-release": { "status": "required | not applicable", "reason": "<...>" },
      "cluster": { "status": "required | not applicable", "reason": "<...>" },
      "live-provider": { "status": "required | not applicable", "reason": "<...>" },
      "external-integration": { "status": "required | not applicable", "reason": "<...>" }
    },
    "evidence": [
      {
        "tier": "<tier key from decision>",
        "criterion": "<the acceptance criterion this observation proves>",
        "command": "<exact command as run>",
        "commit": "<candidate commit the run observed>",
        "artifact": "<image tag, chart release, bundle ref, or trace id, or null>",
        "mode": "fake | live",
        "outcome": "pass | fail | blocked",
        "observed": "<the literal assertion, output line, or value observed, not a restatement of outcome>",
        "negative": "<the falsifiable negative or second path, and what it observed>",
        "teardown": "<what was torn down, or why it was left running>",
        "blocker": "<what stopped this run, or null>",
        "skip_reason": "<why this required tier produced no evidence, or null>"
      }
    ]
  },
  "updated": "<ISO8601 UTC>"
}
```

Update it at each stage boundary, including `open_findings` as reviews produce
findings and fixes clear them. A run gets interrupted: a session ends, a context
fills, a day passes. Without this file the next run either repeats finished stages
or skips an unfinished one, and outstanding findings that lived only in a lost
context are lost with it. A direct path change finishes with nothing to resume, so
the file is optional there.

Record the tier decision once, at triage. Record one evidence entry per
required tier and criterion, filled in as the run observes it, with the
positive proof in `command`, `mode`, `outcome`, and `observed`, and its
falsifiable negative or second path in `negative` on that same entry. Two
observations make one record, not two. Fill `command`, `commit`, `mode`,
`outcome`, and `observed` from the run itself, never from intent, and set
`artifact` whenever the run produced an identifiable image, release, bundle, or
trace. `observed` carries the literal line the run printed, because an `outcome`
of pass with nothing observed behind it is the self-report this record exists to
replace. `teardown` is filled when the surface is released, naming what came
down or why it was left running. A required tier that could not run records
`outcome` blocked with its `blocker` and `skip_reason` rather than being dropped
from the decision. A verification observed only in a context that
is now gone cannot be re-checked, and a tier nobody recorded a decision for is
indistinguishable from one that was skipped.

When a blocker or an unresolvable ambiguity appears after triage, set `status` to
blocked, record the open question, surface it, and stop. The resume check in Setup
picks the run up from there.

## Triage

Classify from the work, not from a requested process size.

| Path | Use when | Minimum proof |
| --- | --- | --- |
| Direct | Mechanical, non behavioral change | Focused validation and review of the diff |
| Quick | Small, single stream behavior change with clear criteria | Failing test, implementation, focused suite, code and scope review |
| Build | Multiple streams, architecture, security, deployment, or unclear interactions | Written plan, separate test and implementation contexts, reviews, and real surface verification, which is required by default |

Promote a direct or quick change when its real scope exceeds the selected path. Do
not add a planning artifact for a trivial mechanical change. If a quick change
turns up an assumption that qualifies for the de risk gate below, promote it to
build before its first edit. If that assumption only appears after an edit, stop
and either escalate or restart as a build path change; do not carry those edits
forward.

Classify the end-to-end tiers before the first edit, per the tier decision rule
in AGENTS.md: every behavior changing path marks each of skill, local,
local-release, cluster, live provider, and external integration required or not
applicable, with a concrete reason. Any behavior-bearing path needs at least one
required tier, and a change that bears no runtime behavior records that reason
once instead. A direct path change makes no behavior change and classifies
nothing.

The path set is runner MCP catalog projection, unscoped PreToolUse,
in-process platform MCP tools, workspace publication, and
built-in coding-tool session capability. A behavior-bearing change that
reaches any of those reaches both live-provider and Slack external-integration.
Those two rows are required on that path. "No model routing change" is not a valid n/a reason.
Fake-model kind, skill ladder, and helper-only tests remain useful and are
not sufficient for those acceptance criteria. Leave the required-tier item
open when the evidence is missing; do not close it by marking the row n/a.
This rule does not pull the live e2e ladder onto unrelated pull requests.

## Build rules

For a behavior change, write or update the test first and confirm it fails for the
intended reason. The test writer and implementer use separate fresh contexts when
the environment supports delegation. Otherwise keep the same separation in order:
write the test, run it red, then implement.

Keep edits within the ticket's acceptance criteria. Do not weaken tests, add
compatibility paths merely to preserve a caller, or replace a real integration with
an internal mock. Use mocks only for external or slow dependencies. Review every
caller when a return type or contract changes.

Keep the roles separate, whether they are separate agents or one session working
in order:

- The test writer touches test paths only, never production source.
- The implementer touches its own stream's source, never the tests, and never its
  own review findings.
- A reviewer reads and reports, and never edits.
- A fix addresses one finding area. Do not batch unrelated findings into a single
  pass, and do not let a fix widen into a refactor.

For a build path, write a short plan that identifies behavior sites, affected files,
test strategy, edge cases, and observable done conditions before implementation.
Have an independent available provider review the plan when the change is
architectural or crosses a contract boundary.

Where the plan quotes source excerpts, confirm each one still resolves on the fresh
base before implementation starts, with a read only search per excerpt. An excerpt
that no longer matches means the plan was written against code that has moved or
never existed. Send those back to be relocated rather than handing the implementer
a stale target or quietly dropping the block.

## Prior intent

On the build path, before implementing, find out what the code you are about to
change was there for. Blame the target line ranges, take the recent touching
commits, and read the tickets or issues they reference. Record what you find in the
plan so the implementer builds with it and the reviewer can check the change
against it.

The failure this prevents is silent. A change can satisfy every one of its own
acceptance criteria while undoing an earlier deliberate decision. Nothing in the
current ticket describes that decision, so no other step in this workflow would
catch it.

## Context discipline

Where the environment supports delegation, every brief tells its agent to return
under about three hundred words and to write anything longer to a file and return
the path. An agent's report stays in the caller's context and is re read on every
later turn, so a long report is paid for again on each one. This governs where
output goes and never what an agent checks or builds.

The same rule binds the caller. Send test suite output, diffs, reviews, and log
sweeps to a file and read back a count or a tail. Do not read a large artifact into
the caller's own context in order to check it.

## De risk gate

Default to no spike. Evaluate this gate only on the build path, after the plan and
any plan review, and before the first test is written. A direct or quick change
never runs a spike.

Run a spike only when all four of these hold:

1. The plan depends on a specific claim that has not been observed.
2. If that claim is false, the plan, scope, a dependency, or the architecture
   materially changes.
3. Documentation, source, existing tests, and prior observed evidence cannot settle
   the claim confidently.
4. A bounded experiment is materially cheaper than discovering the false claim
   during implementation.

If a failed assumption would not change an implementation decision, do not spike.
Treat the economics as a high bar, not a calculation. There should be a plausible
chance the assumption is wrong, failure should cost about a day of work or
invalidate a whole stream, the probe should fit inside thirty minutes, and the
avoided waste should clearly exceed the probe cost. Do not compute a numeric
probability.

Choose one of two kinds:

- Tracer, preferred whenever the experiment can safely be the first narrow
  production slice. It is kept production code and follows the normal test first,
  review, and verification rules. Run that slice alone: write its test, implement
  it, and harvest its observable result before releasing any remaining stream.
- Throwaway, allowed only when execution is required and the evidence cannot come
  from a safe production slice. Brief exactly one uncertainty, one observable pass
  or fail result, a scope limited to that question, a cap of thirty minutes, and a
  written finding. Run it outside the committed tree, never commit it, harvest the
  evidence, then delete it. Code existing is not a finding.

Run at most two throwaway spikes in one run without explicit user approval. If a
spike does not settle its question inside the cap, stop it and either halt or
escalate. Do not let it drift into implementation.

Every finding must confirm, revise, or reject the plan. When a finding materially
changes the plan, revise the plan and repeat whichever plan review applied to it. A
throwaway must settle before any test for the change is written. After a tracer, no
remaining stream proceeds until a revised plan clears that same review. When a
finding exposes unclear acceptance criteria or product behavior, stop and ask. A
spike gathers technical evidence and never makes a product decision.

## Review and verification

Every behavior changing change receives a code review and a scope review after the
affected tests pass. Reviewers are read only. Route findings back to the executor,
then rerun the relevant checks.

Those two reviews are not waivable. No instruction in a task prompt, no time
pressure, and no session setting skips them on a change that alters behavior. If an
instruction appears to waive one, run it anyway and record in the summary that the
instruction was overridden and why. Verification of a required tier is not
waivable either, on the same terms. Everything else in this workflow is
proportional to the change and may legitimately not run.

Each reviewer writes its full findings to
`.projects/plans/<branch-name>.findings.<reviewer>.md` and returns only a routing
index: how many findings, in which areas, touching which files. Fixes read the
detail back out of that file. A review is not complete until its findings file
exists, and that holds when the count is zero. A review whose only record is the
claim that it passed leaves nothing to check, and a skipped review looks exactly
the same from the outside.

Add a security review for authorization, secrets, payments, personally identifiable
information, or tenant boundaries. Verify every required tier through its real
surface, and record each observation in run state as it happens. Static checks
alone never prove a runtime acceptance criterion, and neither does a fake-tier pass
on a criterion about a real provider. Each meaningful acceptance criterion carries
positive proof plus a falsifiable negative or a second independent path, per
Evidence per acceptance criterion in AGENTS.md.

Before completion, run the full affected test suite plus the relevant type check and
lint. Check sibling paths when the change touches a known seam or guard. Demonstrate
that a new or modified guard rejects violating input through its real consumer path.

### P0, release-blocker, and sre-bot-e2e-demo Closes

When the pull request this run will open uses a GitHub closing keyword
(`Closes`, `Fixes`, `Resolves`, and their past-tense forms) on an issue labeled
`P0`, `release-blocker`, or `sre-bot-e2e-demo`, the review stage adds one extra
read-only spec-vs-impl pass. That pass's only job is: each issue acceptance
criterion is visible in the product diff, not only in the e2e table or the Fix
pin selector. Scope review already asks whether each hunk traces to an AC; this
pass asks the converse, whether each AC is in the diff.

A string or refusal-text test cannot be the sole pin for a routing, catalog, or
live-trace AC. The documented insufficient pin is #2209, which closed #2202 with
a message-only Fix pin on the record-miss copy; #2248 reopened the routing AC.

If the diff does not implement an AC, the PR uses `Ref` and leaves the issue
open.

Ordinary bugfixes that close neither of those labels are unchanged: the existing
code review, scope review, and Fix pin rules still apply, and this extra pass
does not run.

## Stop and escalate

Hand the decision back rather than pressing on when reviews cannot converge after
three rounds, when implementation reveals an approach the plan did not cover, when
real surface verification shows the shape is wrong, or when a choice needs
authority the ticket does not carry.

Stop for the same reason when the run itself stops converging: repeated fix and
review rounds without the review stage closing, an agent or a spawn count far past
what the change warrants, or a context that keeps hitting its limit. A run that
reports it is not converging is a successful outcome; a run that keeps spending is
not. Neither stop ever skips a review or weakens a gate.

## Done check

Work through this list before presenting the diff, and report it as the filled list
itself, each item checked or marked not applicable with one line of evidence. Prose
that covers similar ground does not satisfy it. The filled list is the artifact, and
an item nobody had to answer for is an item nobody checked.

- [ ] Failing tests were written and seen to fail before any implementation code
- [ ] The test writer and the implementer worked in separate contexts
- [ ] Code review passed, with its findings file on disk
- [ ] Scope review passed, with its findings file on disk, and every non trivial
      hunk traces to an acceptance criterion, a mechanical exception, or an out of
      scope note in the pull request
- [ ] Every broadened return type or changed contract had its callers checked
- [ ] Sibling paths enumerated, and each one covered by this change, routed through
      a shared helper, or filed as a follow up issue with its number
- [ ] Every new or changed guard was seen rejecting violating input through its real
      consumer path, by running it rather than by reading it
- [ ] Security review passed, where the change touches that surface
- [ ] The real affected surface was exercised, where the change reaches one
- [ ] All six end-to-end tiers are classified required or not applicable in run
      state, each with a concrete reason
- [ ] Every required tier has an evidence record whose outcome is pass, naming
      the exact command, the candidate commit, the mode, and the observed result
- [ ] Every meaningful acceptance criterion carries positive proof plus a
      falsifiable negative or a second independent path
- [ ] The change does not undo an earlier deliberate decision, per Prior intent
- [ ] Full affected test suite, type check, and lint pass on the final code
- [ ] If this PR closes a `P0`, `release-blocker`, or `sre-bot-e2e-demo` issue,
      spec-vs-impl confirmed each issue AC against the diff, not only the e2e
      table or the Fix pin selector. Ordinary bugfixes that close neither label
      mark this n/a.

A quick path change marks the failing tests, separate contexts, and prior intent
items not applicable. A direct path change applies only the suite and lint items.
The two review items are never not applicable on a change that alters behavior.
On a quick or build path they are never not applicable, and a required tier whose
evidence record is missing, failed, or blocked leaves its item open.

Route a failed item back into the stage that owns it. Do not present the diff with
an item still open. An open end-to-end item is a blocker: report it, with the tier,
the command attempted, and what stopped it, rather than presenting the diff or
opening the pull request.

## Finish

Confirm that the final diff satisfies every acceptance criterion and that validation
evidence reflects the final code. Present the diff and verification results. Open
the PR against the selected release train. For a general fix merged to `main`,
create or request the follow up PR that merges `main` forward into `next`; do not
normally cherry pick the fix. Follow the repository's commit, push, pull request,
and ticket status rules. Do not create a pull request or publish changes without
the authority required by those rules.

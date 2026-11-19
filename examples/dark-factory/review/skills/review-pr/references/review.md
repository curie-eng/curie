# Inspection and local verification

## Establish purpose and feature context

Read linked issues and explicit dependencies or follow up PRs. For a feature
slice, distinguish the full feature goal, what this PR delivers, and what
remains. Do not infer a roadmap from a branch name or label. Trace the change
from its public entry point through callers and downstream components to the
visible result. An added file is not active functionality until a production
caller or registration path reaches it.

Ask what selects each changed path, what state it changes, what happens on a
failure or repeat, and how a fresh installation differs from an upgrade.
Check the current default and explicitly configured modes. Distinguish the
PR's stated intent from its actual runtime behavior.

## Build a navigable explanation

Scale the walkthrough to the PR. For a small change, a few connected paragraphs
and file links suffice. For a larger change, show a component table with each
responsibility, repository path and relevant entry point, then explain the caller
sequence or data flow. Distinguish preexisting components from new or modified
ones. Include configuration, persistence and external boundaries when reached.
Use a diagram when it clarifies those relationships.

For a sequence diagram, show material actors and calls rather than every
function. Include an alternate branch for a failure or fallback when it
changes the outcome. Mark what persists after a command exits. Show old and
new flows when that is needed to see the change. Use prose for a single action.
Choose the diagram format for the client's actual display. If Mermaid fences
render, use a concise Mermaid sequence diagram. Claude Code's terminal and
Desktop Code transcript currently show Mermaid fences as source; treat these
as unsupported unless the current interface demonstrably renders them. Do
not mistake syntax highlighting for a rendered diagram.

In an unsupported client, draw the swimlane inline in a monospaced code block.
Use a wider layout, around 90 to 110 columns when the display permits, with
short actor names and message labels. Each actor's vertical lifeline must stay
at the same character position on every row, and each arrowhead must land on
the receiving lifeline. Number the messages. Put a state note below the block
and identify the step that establishes it; never place an unlabeled state note
between lifelines where it could be mistaken for an arrow label. For an
alternate group, show common steps once, then label each branch outside the
lanes as `ALT A` and `ALT B` with its condition. Number branch messages `4a`,
`4b` and so on while keeping the same actor columns. For an optional or
repeated group, label the guard or loop condition and the step at which it
ends. Put state notes below the block and name the branch they apply to.
Split old and new flows instead of squeezing both into one crowded diagram.
Avoid colors, emoji and external HTML files for this inline view. Check the
alignment and line length before replying; shorten labels or split the flow
if the host will wrap it. For example:

```text
          User                                 Access CLI                                  Kubernetes
          |                                    |                                           |
1.        |---------- start access ------------>                                           |
2.        |                                    |------------- forward Console ------------->
3.        |                                    |------------ forward Langfuse ------------->

ALT A: both forwards become ready
4a.       <--------- localhost URLs -----------|                                           |
5a.       |------------- Ctrl-C --------------->                                           |
6a.       |                                    |----------- stop both forwards ------------>

ALT B: either forward exits
4b.       |                                    |----------- stop other forward ------------>
5b.       <---------- return error ------------|                                           |
```

State after step 4a: both forwards remain active while the command runs.
State after step 6a or 4b: neither forward remains active. These state notes
describe what the arrows accomplished; they are not additional messages.

Trace significant hunks to the requested outcome. Explain changed inputs,
outputs, errors, defaults, compatibility, permissions and migrations where
applicable. Call out observable behavior changes even when they are incidental
to the stated feature. Do not infer unchanged behavior merely from unchanged
files; inspect callers when contracts change. Link to verified source locations.
Verify each PR diff link's path, side and line on the current head. Link an
unchanged dependency to source without presenting it as part of the diff.
For each new module or helper, find its production caller or registration path.
If none exists, do not describe it as active behavior.
For an algorithmic change, explain the decision rule with concrete inputs and
outcomes; discuss complexity only when scale or cost actually changes. For a
data model change, inspect storage and API contracts, migration order, existing
records, version compatibility, rollback and backfill. An in memory enum is
not a database migration.
When asked to dig deeper, follow a concrete request through that component's
entry point, dependencies, state and failure behavior, using actual source.

Review the security posture of changed defaults. Check host and network
binding, identity, authorization, credential handling, public endpoints,
tenant boundaries, and whether access lasts only during a command or for a
deployment lifetime. Inspect production configuration separately from
development defaults. A loopback listener still admits other processes on
the host; a port forward may require cluster RBAC and can bypass network
policy. State the threat model and uncertainty rather than calling every
exposure a vulnerability.

Read linked issues and relevant human decisions. Maintain a compact mapping:

| Requirement or decision | Implementation | Verification | Remaining gap |
| --- | --- | --- | --- |
| Observable behavior | File and entry point | Assertion or manual step | None or explicit limitation |

Do not silently turn external qualification into a local test claim or weaken an
acceptance criterion to make the PR look complete. Explain whether a gap is an
implementation defect, missing coverage, an unavailable local environment or an
already approved deferred check. Let the human decide consequential scope changes.

## Explain the test evidence

Read the test diff and enumerate scenarios rather than just counting test files.
For each new or modified scenario, identify the test location, setup, behavior
asserted and important failure or edge cases. Explain what the assertion would
catch. Flag vacuous assertions, skipped checks and mocks that stop before the
production behavior being claimed. Cite unchanged regression tests relevant to
changed callers, contracts and sibling paths; do not assert that unrelated green
tests establish absence of regressions.

Say whether an automated end to end test was added to this PR. A test of a
plan builder is an integration or plan test if it never executes the external
effect. A reported live run is manual evidence, even when it uses real
infrastructure. Name the product entry point and observed result for each
actual end to end scenario and identify substituted dependencies.

Keep these evidence sources distinct:

* Author reports: claims from the PR, not independently observed results.
* CI: check identity, conclusion, SHA and logs inspected; distinguish missing,
  skipped, pending and passing checks.
* Local agent runs: exact command, checkout SHA, relevant environment, result and
  skipped tests or limitations.
* Human runs: instructions supplied versus the output the human actually reports.

For a relevant failure, use an isolated base checkout and equivalent environment
when useful to distinguish a regression from a preexisting failure. Do not alter
the user's checkout or rerun costly unrelated suites automatically. A base
comparison that was not run remains unknown.

## Make manual tests reproducible

Choose a small set of tests that exercises the actual user entry point, the main
changed behavior and a meaningful failure or boundary. Automated test commands
can supplement this, but do not substitute for a manual product scenario when
that surface can be exercised locally. If it cannot, identify the missing
capability and what local tests can establish instead.

Each step states:

1. Where to run it: checkout, terminal or UI, prerequisites, revision and isolated
   test resources. Confirm environment readiness before proposing execution.
2. Exact command or interaction, with obvious placeholders for user inputs and
   no secrets embedded in logs or chat.
3. Expected observable output or state and a recognizable failure signal.
4. What the result verifies and what it leaves unproved.
5. Cleanup of only this session's resources, preserving unrelated work.

During guided testing, wait for the human's result before giving a dependent
step. Record failures without coaching them into a pass. An interrupted test or
unanswered instruction remains unexecuted. After a failed result, inspect the
evidence and propose a focused next check or feedback item. Do not fix the PR
locally unless the human asks for implementation; the normal revision path is
approved feedback to the factory.

If the PR head or base changes, show the delta since the reviewed revision,
invalidate affected walkthrough, test and line-comment evidence, and revise the
plan. Previous results remain historical observations, not proof for the new
revision. Preserve unrelated human decisions that still apply.

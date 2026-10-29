# Inspection and local verification

## Build a navigable explanation

Scale the walkthrough to the PR. For a small change, a few connected paragraphs
and file links suffice. For a larger change, show a component table with each
responsibility, repository path and relevant entry point, then explain the caller
sequence or data flow. Distinguish preexisting components from new or modified
ones. Include configuration, persistence and external boundaries when reached.
Use a diagram when it clarifies those relationships.

Trace significant hunks to the requested outcome. Explain changed inputs,
outputs, errors, defaults, compatibility, permissions and migrations where
applicable. Call out observable behavior changes even when they are incidental
to the stated feature. Do not infer unchanged behavior merely from unchanged
files; inspect callers when contracts change. Link to verified source locations.
When asked to dig deeper, follow a concrete request through that component's
entry point, dependencies, state and failure behavior, using actual source.

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

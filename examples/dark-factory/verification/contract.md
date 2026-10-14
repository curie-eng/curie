# Factory verification contract

This contract is the one set of verification rules for this run. The review
gate hook gives it to the implementing agent with the first message and with
every `wait_ci` round message, and appends the same text to every plan review
and diff review call. Planning, implementation, plan review and diff review
apply it identically. It adds to the repository's own instructions
(`AGENTS.md`, `CONTRIBUTING.md`, the documented test commands and the
repository's CI); it never relaxes them.

## 1. The verification table

Every plan carries one row per numbered acceptance criterion. The implementer
keeps the table current through every `implement` round, every diff review and
every `wait_ci` round.

| Column | What it holds |
| --- | --- |
| Criterion | The criterion number, `AC<n>`. |
| Check | The exact command, or the test file and test name, that verifies it. |
| Location | `sandbox`, or `ci:<required check name>` for a delegated check. |
| Evidence | What must exist before publication (section 3). |
| Delegation | The declared check id and its `delegated_to` name, or `none`. |

## 2. Execution locations

Each row takes exactly one route.

1. `sandbox`: the check runs in this sandbox. Every check whose prerequisites
   are present runs here.
2. `delegated`: the check runs only in a named required pull request CI check.
   A row is delegated only when all three conditions hold:
   1. the test's only blocker is an absent service the repository's CI starts
      (Postgres, Valkey, or another server), not a missing binary, package,
      locked dependency or file;
   2. the resolved declaration has a check whose `paths` cover the test and
      the changed files it verifies, that names that required check in
      `delegated_to`, and whose startup result was `unavailable`; the
      implementer quotes that check's line from the declared data block of
      its startup instructions. The resolved declaration is the bundle's
      `verification/checks.json` when it declares any checks (the review gate
      appends it to every review call, after this contract), otherwise the
      repository's `.curie/verification.json` in the checkout, which a
      reviewer reads itself. Only that recorded check is held by the
      platform's CI gate. A row relying on a repository declaration shadowed
      by bundle checks, or on a check without that quoted `unavailable` line,
      is refused `missing_ci_route`;
   3. the test exercises the criterion through the changed code and would
      fail on the base code. At plan review the test does not exist yet: the
      reviewer judges the proposed test as the plan describes it (what it
      asserts, which changed code it goes through, why it fails on base) and
      never demands the written test. At diff review the reviewer reads the
      written test.
3. `refused`: every other row. A refused row blocks publication.

When a row is validly delegated, neither reviewer asks for that test's local
run results, real-service evidence, or an observed red-on-base run. None of
them can be produced in this sandbox. The platform holds the run until the
named check has run and passed on the pull request's head commit, and a
failure there returns the run to `implement`.

## 3. Required prepublication evidence

1. A `sandbox` row: the command, its exit status and a one-line result from a
   run after the last edit, and it passed.
2. A `delegated` row: the test is written; every check for the changed area
   that can run here passed; the diff reviewer approved the test as exercising
   the criterion; and the pull request body names the required check, states
   that in-sandbox verification was unavailable and CI is pending proof, states
   that red-on-base was not observed in the sandbox, and gives the red-on-base
   procedure with the base commit and changed files filled in: keep the new
   test file, restore only the changed non-test files with
   `git checkout <base-sha> -- <changed source files>`, start the services the
   repository documents, and run the recorded command. It must fail on the
   bug, not at import.
3. A criterion with no feasible test (documentation, pure configuration): the
   row says how the change is verified instead, and that verification ran.

## 4. What always blocks

At plan review and at diff review alike, each of these refuses the row:

1. `missing_coverage`: a criterion has no row, or its row names no exact check.
2. `invalid_test`: the test (at plan review, the proposed test) is wrong,
   misses the criterion, does not go through the changed code, or would pass
   on the base code.
3. `failing_check`: a check that can run here failed, or at diff review was
   not run after the last edit.
4. `missing_ci_route`: a row relies on CI coverage that is hypothetical,
   undeclared, not a required check, not named by a `delegated_to` of the
   resolved declaration whose paths cover the test, declared only in a
   shadowed repository file, not recorded `unavailable` at startup, or
   delegates a check that can run here.
5. `package_dependency`: the blocker is a missing package, toolchain or locked
   dependency fetch. A missing package dependency is never a service gap.

Undeclared CI coverage and a failed check are never reclassified as a service
gap.

## 5. Findings

1. Each blocking finding names the criterion (`AC<n>`) or the concrete
   correctness or verification defect.
2. A refused row is a blocking finding for its criterion, so a review that
   refuses any row returns `VERDICT: CHANGES`.
3. Optional improvements stay notes under the reviewer's existing rules.
4. Nothing approves automatically. A valid delegation removes only the demand
   for evidence this sandbox cannot produce, never another blocking finding.

## 6. The VERIFICATION block

Both reviewers end their reply with this block, one line per criterion in
criterion order:

```
VERIFICATION:
- AC<n>: sandbox <check>
- AC<n>: delegated <required check name>
- AC<n>: refused <reason>
```

`<reason>` is one of `missing_coverage`, `invalid_test`, `failing_check`,
`missing_ci_route` or `package_dependency`. The hook records the block on the
run; it never changes the verdict.

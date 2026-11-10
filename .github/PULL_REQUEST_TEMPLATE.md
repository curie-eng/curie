<!-- This repository is public. Anonymize customer and downstream information
     in this PR's title/body, comments, links, attachments, and every outgoing
     commit subject/body. Do not reproduce a leaked value when explaining a
     redaction. Live-test evidence and provenance are not exceptions. -->

## Summary

<!-- One paragraph: what changed, and why. Skip the play by play. -->

## Related issue

<!-- Closes #NNN. Use `Ref #NNN` if this does not fully close the issue. -->

Closes #

## Trigger

<!-- Required for a patch release PR (title: Prepare the vX.Y.Z release
     where Z is not 0). List the issue numbers of the defects that
     triggered this patch, for example #2202. Other pull requests may
     leave this comment in place.

     If you moved an open issue out of this milestone within 24 hours of
     the cut, that issue needs a comment naming the release it moved from. -->

## Live proof

<!-- Required for a patch release PR. Name a run URL that re-verified each
     trigger on a live surface, or an explicit waiver of the form:
     waiver: <reason>

     To defer proof to this pull request's own CI, replace this comment with
     the exact marker below on its own visible line:
this-pr-ci

     Wait for the CI checks on this pull request's current head to pass before
     merging. A passing run proves only the ladder rungs CI executes on that
     head. Include separate live proof or a waiver for any broader surface.
     This pull request's own CI run URL also counts for those same rungs;
     do not rerun a rung locally after CI has proved it on the same tree.
     Other pull requests may leave this comment in place. -->

## Fix pin verification

<!-- Verification is required for declared fixes. For a fix pull request,
     replace this comment with exactly one selector changed by this pull request:
Fix pin: <supported selector>

     Supported selector forms:
     apps/*/tests/*.py::test
     packages/*/tests/*.py::test
     runner/tests/*.py::test
     cli/tests/local/test_*.py::test
     cli/tests/name.rs::test
     charts/curie/ci/name.sh

     A local Python selector must start the actual isolated local services it
     owns. The verifier runs it twice in separate pytest processes. Each
     baseline and reversed invocation must begin from fresh state, register
     cleanup before startup, and verify cleanup. Unavailable Docker, ports,
     binaries, or services must produce errors, never skips or green results.
     Put prerequisite checks, service startup, and cleanup verification in
     pytest fixture setup or teardown so environmental failures are errors,
     which the verifier refuses. Only product behavior assertions belong in
     the test body.

     The declaration must be present before opening the pull request. If it is
     added or corrected later, the body edit automatically revalidates the
     required gate.

     REQUIRED when this pull request closes an issue labeled `bug` (a GitHub
     closing keyword plus a same-repo #N): CI fails without a Fix pin line. If
     there is no selector to declare (a revert, a docs-only fix, or a bug
     closed by deletion), use the escape hatch instead, with a non-empty
     reason:
Fix pin: n/a - <reason>

     A lower-tier pin that reproduces the defect is accepted without a waiver,
     regardless of the issue's discovery label. The verifier still requires a
     changed test that passes and fails when the fix is reversed.

     For a non fix pull request that does not close a bug-labeled issue, leave
     this section empty. -->

## Verification

<!-- State what you ran locally, with commands and observed outcomes.
     Include any live or external proof CI cannot produce.
     CI tier results are read from CI, not restated here.
     A change that reaches runner MCP catalog projection, unscoped PreToolUse,
     in-process platform MCP tools, workspace publication, or
     built-in coding-tool session capability must record live-provider plus
     external-integration evidence, or leave that proof open.
     "No model routing change" is not a valid n/a reason.
     Test at the cheapest boundary that proves the changed contract;
     a higher tier needs a failure mode the lower tier cannot see. -->

Local checks:

Live or external proof CI cannot produce:

## Checklist

1. [ ] Tests pass for the area I touched (see CONTRIBUTING.md for the commands).
1. [ ] Docs updated if behavior changed.
1. [ ] An ADR is added under `docs/adr/` if this is an architectural decision.
1. [ ] Reviewed the public title, body, commits, branch name, links, and attachments for customer and downstream information; used anonymous roles and placeholders throughout.

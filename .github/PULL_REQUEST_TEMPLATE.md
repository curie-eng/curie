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
     The pull request's own CI run URL counts as proof for any ladder rung
     CI ran on the same tree; do not rerun that rung locally.
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

     The pin's tier is derived from the selector's location, not from prose:
     unit tests, cli/tests/local/test_*.py (local), charts/curie/ci/* (cluster
     helm-render), or other test_live.py selectors (live). If the closed issue
     carries found:unit, found:local, found:cluster, or found:live and the pin
     is below that surface, add:
Fix pin waiver: <reason>

     A unit pin for a found:live issue fails without that waiver.

     For a non fix pull request that does not close a bug-labeled issue, leave
     this section empty. -->

## End-to-end verification

<!-- Choose exactly one path.

     Behavior-bearing: keep the tier table and three evidence checkboxes below.
     Classify all seven tiers required or n/a with a concrete reason, and paste the
     exact command plus what you observed for each required tier.

     No runtime behavior: delete the tier table and three evidence checkboxes.
     Replace them with one explanation and the scoped checks you ran:

     This change does not alter runtime behavior because <reason>.
     Scoped verification: `<command>` - <observed outcome>.

     Documentation and ADR changes are not automatically exempt. If they alter
     runtime behavior, use the behavior-bearing path. See "E2E verification is
     mandatory" in AGENTS.md.

     A change that reaches runner MCP catalog projection, unscoped PreToolUse,
     in-process platform MCP tools, workspace publication, or
     built-in coding-tool session capability must record live-provider plus
     external-integration evidence, or leave those required-tier rows open.
     "No model routing change" is not a valid n/a reason. Fake-model kind,
     skill ladder, and helper-only tests are not sufficient.

     Factory is required for API factory_runtime, factory_ci, factory_progress,
     or routers/publications changes; runner verification preflight or factory
     progress; examples/dark-factory; and worker work item execution. Run the
     production factory scenario through the changed components. A canned
     fixture or fake scenario does not close this tier. Until #3814 ships its
     scenario, use `curie dev factory-e2e run --scenario issue-to-pr`.

     The guard derives minimum required tiers from changed files. Body prose
     cannot omit a required row or make it n/a. Skill and local may use fake
     in the mode column when the row supplies its exact command and an observed
     completed outcome. Live provider, external integration, and factory require
     `live` in the mode column; a blank cell or any other mode leaves the row
     unproved.

     Evidence text saying blocked, not run, or fake leaves a required row
     unproved at any tier. For a completed negative test, describe the observed
     outcome as denied, refused, or returned 401; reserve blocked, not run,
     and fake for unproved status requiring a waiver. To proceed as discovery,
     include a visible
     line of this exact form with a concrete reason and an open issue:
Discovery waiver: <reason> #N

     Keep the row required and state the missing proof. Table rows and waiver
     lines must be visible. Text inside HTML comments, fenced code blocks, or
     indented code blocks does not count as evidence. Any follow up named
     in the tier table must include its issue number. -->

| Tier | Required / n/a | Reason | Mode (fake / live) | Command and observed outcome |
| --- | --- | --- | --- | --- |
| skill | | | | |
| local | | | | |
| local-release | | | | |
| cluster | | | | |
| live provider | | | | |
| external integration | | | | |
| factory | | | | |

1. [ ] Every proved required tier above names its exact command, the commit it ran
      against, the mode it ran in, and the literal outcome observed.
2. [ ] Each meaningful acceptance criterion has positive proof plus a falsifiable
      negative or a second independent path.
3. [ ] Every unproved required tier names its blocker in the table and has a
      visible Discovery waiver naming an open issue. Any follow up in the
      table includes its issue number.

## Checklist

- [ ] Tests pass for the area I touched (see CONTRIBUTING.md for the commands).
- [ ] Docs updated if behavior changed.
- [ ] An ADR is added under `docs/adr/` if this is an architectural decision.
- [ ] Reviewed the public title, body, commits, branch name, links, and attachments for customer and downstream information; used anonymous roles and placeholders throughout.

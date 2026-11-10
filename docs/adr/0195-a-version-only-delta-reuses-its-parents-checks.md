# 195. A version-only delta reuses its parent's checks

Date: 2026-10-04

Status: Accepted

This ADR amends only the Checks rule of
[ADR 0058](0058-tag-push-is-not-release-authority.md). Ancestry, the tag
class, the nightly gate, the `release-publish` environment, and every other
part of the release gate are unchanged.

This ADR is Accepted alongside its implementation under
[ADR 0102](0102-accepted-alongside-implementation-with-explicit-approval.md).
Explicit maintainer approval is Brian's recorded grant
`adr-0058-version-only-amendment` of 2026-10-04 for issue #3858. The realizing
code path is `release/authorize.py`.

## Context

The v0.11.2 preparation PR #3845 changed only five version files. It still ran
every tier: pytest shards for about 12 minutes, the kind cluster rung, the chart
runtime regressions, and upgrade matrix s01 for 18 minutes. The merge commit
then ran main CI again, for 23 minutes, before the tag gate would authorize the
tag. Both runs re-proved a tree that differed from an already green tree only by
its version strings.

## Decision

1. **One definition of the set.** `release/atlas.py` defines
   `version_only_paths(version)` as `VERSION_ONLY_PATHS`
   (`charts/curie/Chart.yaml`, `cli/Cargo.lock`, `cli/Cargo.toml`,
   `docs/architecture-atlas/versions.json`) plus
   `docs/architecture-atlas/snapshots/<tag>.json`.
2. **Tag gate.** When the tagged commit's own required check-runs are absent or
   still running, and none has concluded non-passing, `release/authorize.py`
   walks first-parent ancestors, nearest first, at most 10. It continues while
   the aggregate `git diff` from the ancestor to the tagged commit stays inside
   that set. It accepts the first ancestor whose `REQUIRED_CHECK_NAMES` all
   passed, excluding the current workflow run. Any own required check that
   concluded failure, cancelled, skipped, or any other non-passing result is
   contrary evidence on the tagged tree and still refuses. Only first-parent
   ancestors count, so a PR head (the second parent) whose PR run was
   path-reduced is never the proof.
3. **PR selection.** `tools/e2e-ci-selection/select_tiers.py` treats a PR whose
   whole diff is in the set as version-only. The snapshot is the one named for
   the head's `cli/Cargo.toml` version. Such a PR runs no kind tiers, no pytest
   shards, and no Rust test partitions. It keeps version consistency and the
   schema window (rust-lint), clippy, the Python static gates plus
   `release/tests`, chart lint and assertions (Helm CI), compose, and the
   local-release rung. `ci.yaml` gates `python-pytest` on the selector's pytest
   output with a job-level `if`.

## Consequences

1. A patch preparation PR's critical path becomes the Helm CI chart shards,
   projected at about 6.7 minutes from runs 37037684957 and 37037685088. It is
   to be measured live at the next patch release.
2. A tag can be authorized before main CI on the merge commit finishes. A later
   red main run on that commit is no longer a release blocker. If that run
   finishes red before the tag, the gate refuses.
3. Push CI on `main` and `next` still runs every tier.
4. This reconciles with #3840. Its scope line "Release tagging still requires a
   completed matrix on the tagged SHA" is satisfied by accepting the completed
   matrix from the version-only first-parent ancestor, whose tree differs only by
   version files. The other changes in #3840 are unaffected.

## Alternatives considered

1. **Always require the tagged commit's own checks.** Rejected, that is the
   23 minute wait this issue removes.
2. **Accept any green ancestor regardless of the delta.** Rejected, it launders
   code changes past the gate.
3. **Accept the PR head's checks.** Rejected, path-reduced PR runs are not a
   full matrix.
4. **Let a version-only PR skip the local-release rung too.** Rejected, that rung
   proves the version identity in a running stack.
5. **Derive the set from a registry entry in `.github/e2e-selection.yaml`.**
   Rejected, two definitions of one set drift.

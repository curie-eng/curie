# 58. A pushed tag is not release authority: ancestry, checks, and an approval environment gate publication

Date: 2026-07-21

Status: Accepted

**Amended by [ADR-0065](0065-tag-protection-not-the-workflow-gate-binds-a-write-actor.md)**
(back-link added under [ADR-0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md)):
the Context sentence below, "The workflow-level gate below is enforceable
today without that access," is not correct against this ADR's own threat
model -- the gate and the workflow file that invokes it are both read from the
tagged commit, so a write actor willing to edit either is not bound by it.
0065 restates the trust boundary: tag protection on `v*` and required
reviewers on `release-publish` are the controls that actually bind such an
actor, both still admin-gated and unconfigured. The Decision below (ancestry,
checks, the environment reference) is unaffected and stands as an
accident-prevention control.

**Amended by [ADR-0066](0066-a-skip-propagates-through-the-whole-ancestor-graph.md)**
(back-link added under [ADR-0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md)):
the Decision's claim that the jobs downstream of `build` are "covered without
further edits", and the Consequence that a push straight to `main` is
"unaffected", are both false as built. A skip propagates through the whole
ancestor graph, so `merge` and the overlay jobs beneath it were skipped on every
main push and the continuous image tags stopped being published. 0066 guards
each of those jobs. The gate itself (ancestry, checks, the environment
reference) is unaffected and stands as decided.

**Amended by [ADR-0195](0195-a-version-only-delta-reuses-its-parents-checks.md)**
(back-link added under [ADR-0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md)):
the Checks bullet's requirement that the tagged commit's own check-runs pass
now has one exception. When those checks are absent or still running and none
has concluded non-passing, a version-only delta from a first-parent ancestor
whose required checks passed is accepted as the proof. Ancestry and the
environment reference stand as decided.

Implements [#628](https://github.com/curie-eng/curie/issues/628).

## Context

`release.yaml` triggers the full publish pipeline -- version-tagged multi-arch
images, CLI binaries, the packaged chart, and a signed GitHub Release -- on
`push: tags: ["v*"]`. The trigger asks nothing about the tagged commit: any
repository write actor can create a `v*` tag on any commit reachable from any
branch, including one that never went through review, and push it. GitHub does
not treat a tag push as a deployment by default, so nothing paused the pipeline
for a human decision either. Two separate gaps, one hole: the pipeline could not
tell "this is main, reviewed, and green" from "this is a tag," and even when it
was the former, publication needed no explicit sign-off beyond push access.

ADR-0052 built the trust chain a *consumer* verifies (checksum, signature,
provenance) once assets exist. This ADR is upstream of that: it decides whether
the workflow run that produces those assets should have started at all.

Two things this ADR does not attempt: repository-level tag protection rules
(Settings -> Tags) and required reviewers on a GitHub Environment are both
admin-only settings this account does not hold (`gh api repos/curie-eng/curie
--jq .permissions` reports `push`-only, no `admin`/`maintain`). The workflow-level
gate below is enforceable today without that access; the two admin actions
harden it further and are called out under Consequences as follow-ups, not
assumed.

## Decision

**A gate job the whole tag-only pipeline depends on.** `authorize-release` runs
only `if: startsWith(github.ref, 'refs/tags/v')`, checked out with full history
(`fetch-depth: 0`), and calls `release/authorize.py`, which fails closed on
either of:

- **Ancestry**: the tagged commit is not `git merge-base --is-ancestor <sha>
  origin/main` -- i.e. not reachable from `origin/main`. A tag on a feature
  branch, a rebased-away commit, or anything that bypassed a merged PR is
  refused before any image builds.
- **Checks**: the commit's check-runs (`GET
  /repos/{repo}/commits/{sha}/check-runs`) are not all `success` (`neutral` and
  `skipped` also pass; anything else, or zero check-runs, fails). A commit that
  is on main but whose CI never ran, or ran and failed, is refused the same way.

Both conditions live in `release/authorize.py` as two separately-testable
functions (`commit_is_on_reviewed_main`, `required_checks_passed`) rather than
inline shell, so the negative case -- an unreviewed or check-less commit is
refused -- is an ordinary pytest assertion against a constructed git fixture, not
a manual demonstration against the real repo.

**Every publishing job needs it, transitively.** `build` gains `needs:
[authorize-release]`; every job downstream of `build` (`merge`,
`worker-local-build`, `worker-local-merge`) already depended on it and so is
covered without further edits. `cli-binaries` and `chart`, which have no other
dependency, gain the same `needs:` directly. GitHub Actions treats a job whose
`if:` evaluated false as *skipped*, not failed, so on an ordinary push to `main`
(no tag) `authorize-release` is skipped and every dependent job proceeds exactly
as before -- the continuous sha/latest image builds are untouched. On a tag
push, a failed `authorize-release` skips everything beneath it: no images, no
binaries, no chart, no release.

**An environment is the hook for human approval, not a fiction.**
`authorize-release` also declares `environment: release-publish`. A GitHub
Environment with no protection rules configured has no effect -- the job runs
immediately, exactly as it does today. The line is there so that the day a
maintainer adds required reviewers to that environment (an admin-only settings
action, not a code change), every tag push pauses for their sign-off *before*
the ancestry/checks script even runs, with no further workflow edit. Putting the
environment on the *first* job in the tag path, rather than on `release` at the
end, matters: `merge` already pushes public, version-tagged image manifests to
GHCR well before the `release` job creates anything, so gating only the last job
would let images out while the "release" was still unapproved. Gating the entry
point blocks the whole chain.

## Consequences

- A tag on a commit not reachable from `origin/main`, or reachable but lacking
  green checks, cannot produce any published artifact -- images, binaries,
  chart, or GitHub Release -- because every producing job is downstream of the
  refused gate. This is directly demonstrable: tag a throwaway commit off main
  and push it, and `authorize-release` fails before `build` starts.
- A push straight to `main` (the continuous sha/latest image path) is
  unaffected: `authorize-release`'s `if:` is false, so it is skipped and every
  dependent job runs exactly as before.
- The `environment: release-publish` line is inert until a maintainer with
  admin access adds required reviewers to that environment in repository
  Settings. Until then, criterion 2 of #628 (publication needs explicit release
  authority beyond ordinary write access) is not yet met; the ancestry/checks
  gate alone satisfies criteria 1 and 4.
- Tag protection for `v*` (so pushing or deleting a matching tag itself requires
  elevated permission, criterion 3) is a repository Settings -> Tags rule, also
  admin-only, also not yet configured. Recorded here rather than silently
  assumed. The auditable break-glass path for both admin actions is the normal
  one: a maintainer changes the Settings page, which is itself logged in the
  repository's audit log.
- `release/authorize.py`'s two functions are unit-testable without network
  access (a local temp git repo for ancestry, an in-memory check-run list for
  checks); only the `main()` entry point that fetches real check-runs needs
  `gh`/`GITHUB_TOKEN`, matching the `manifest`/`verify` split in
  `release/integrity.py`.

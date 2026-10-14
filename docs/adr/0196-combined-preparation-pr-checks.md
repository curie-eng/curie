# 196. Combined preparation PR checks can prove an unchanged release merge

Date: 2026-10-04

Status: Draft

This draft proposes amending the Checks rule of
[ADR 0195](0195-a-version-only-delta-reuses-its-parents-checks.md), including its
exclusion of the second-parent PR head. It does not authorize implementation or
tagging through that proposed path. Ancestry, the tag class, the nightly gate,
the atlas pin, the `release-publish` environment, and the other release gates
would remain unchanged.

Issue: [#3859](https://github.com/curie-eng/curie/issues/3859).

## Context

A patch cut can carry its fixes, version bump, and architecture snapshot in one
preparation PR. The atlas pins the final non-version commit; a following commit
changes only the release version and snapshot registration. The preparation
PR's final CI checks that combined tree. A separate candidate PR would add
another CI round for substantially the same work.

The timing recorded in #3859 includes a prior candidate CI round from 16:35 to
16:56, preparation PR CI from 16:59 to 17:22, merge CI on `main` from 17:22 to
17:45, and tag publication from 17:46 to 17:54. Those rounds took 21, 23, 23,
and 8 minutes respectively. Removing the candidate round still leaves 54
minutes of preparation CI, merge CI, and publication, before review or queue
delays.

ADR 0195 permits green first-parent checks to prove a version-only delta. Its
Decision explicitly excludes the second-parent PR head because a path-reduced
PR run is not a full matrix. That restriction also excludes a combined
preparation head whose full required checks passed and whose merge tree is
unchanged. The first parent is the previous `main` tip, so the delta from it to
the merge includes the fixes and cannot use ADR 0195's version-only exception.
Current `release/authorize.py` therefore waits for the merge commit's own
required checks. An atlas pass does not remove that wait.

## Proposed decision

1. **Use one preparation PR.** Put the fixes and release preparation in one
   branch targeting current `main`. Commit all non-version changes first. Pin
   the atlas to that final non-version commit, then add the version/snapshot
   commit. The whole delta after the pin must stay inside
   `release/atlas.py::version_only_paths` for this tag.
2. **Bind proof to the checked preparation head.** In addition to the existing
   first-parent rule, permit the merged preparation PR's exact head as a proof
   candidate. Require full CI on that head, with every `REQUIRED_CHECK_NAMES`
   entry present and completed with a passing conclusion. An absent, running,
   skipped, or non-passing required check refuses this proof. A path-reduced
   version-only PR run, an older head's green run, or an unrelated green
   ancestor is not sufficient.
3. **Require an up-to-date merge.** The checked head must include the `main`
   tip that becomes the merge's first parent. If `main` advances before merge,
   update the preparation branch and rerun its required checks. Accept its head
   only when it is the merge's second parent and the aggregate tree delta from
   that head to the tagged merge is empty or contains only this release's
   `version_only_paths`. Non-version conflict resolution or a different merged
   head refuses this proof.
4. **Keep contrary evidence fatal.** As in ADR 0195, any required check on the
   tagged commit that has concluded non-passing refuses authorization. Missing
   or still-running own checks may use the proposed proof only after every
   condition above passes. The current workflow run remains excluded from its
   own check evidence.
5. **Keep live evidence specific.** The preparation PR may put the exact
   standalone `this-pr-ci` marker in its Live proof section to reference its
   own final-head CI. This is a body-content declaration, not a claim that CI
   has passed or that it exercised an unrun live surface. Each trigger whose
   required live surface is outside that run still needs a qualifying run URL
   or an explicit waiver. No separate candidate CI round is required.

## Consequences

1. If this ADR is explicitly Accepted and implemented in
   `release/authorize.py`, the projected critical path from the recorded rounds
   is one preparation PR CI run of 23 minutes plus publication of 8 minutes,
   about 31 minutes excluding review and merge queue delays. This is a
   conditional projection, not a measured release result. The next real patch
   cut must measure its preparation CI, merge, authorization, and publication
   times.
2. Until acceptance and implementation, combined preparation remains available
   but direct tagging waits for the merge commit's own complete required
   checks. Its corresponding projection is about 54 minutes. This draft does
   not grant a waiver of that blocker.
3. Push CI on `main` and `next` still runs. An own required failure already
   concluded when the tag is authorized blocks publication; a later result
   cannot retroactively change the gate's decision, as in ADR 0195.
4. Changes to the preparation branch or its base invalidate the earlier proof.
   A new final head needs new checks, and non-version changes after the atlas
   pin also need a new pin and snapshot. No architecture change may be hidden
   in the merge delta.
5. The authorizer needs positive coverage for an up-to-date merge with a full
   green preparation head and for its empty or version-only delta. Negative
   coverage must reject a stale base, changed merge content, reduced or missing
   checks, a different head, and contrary tagged-commit evidence before this
   proposal can be implemented and used.

## Alternatives considered

1. **Wait for the combined merge's own checks.** This is the current authorized
   path. It retains the extra 23 minute round in the recorded timing.
2. **Run a candidate PR before preparation.** Rejected as the target workflow;
   the combined preparation PR can check the fixes and version together.
3. **Accept every green second parent.** Rejected. The exact merged head,
   up-to-date base, full required checks, and restricted merge delta are all
   necessary evidence.
4. **Treat `this-pr-ci` as release authority.** Rejected. A body marker provides
   a reference; it does not prove checks or any unrun live behavior.
5. **Rewrite Accepted ADR 0195 or implement before approval.** Rejected under
   [ADR 0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md)
   and [ADR 0102](0102-accepted-alongside-implementation-with-explicit-approval.md).
   Preserve its history and obtain explicit acceptance of this amendment before
   implementing the new authorization path.

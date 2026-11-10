# 247. A tree-identical parent or a schema window registration reuses checks

Date: 2026-10-24

Status: Accepted

This ADR amends only Decision item 2 (Tag gate) of
[ADR 0195](0195-a-version-only-delta-reuses-its-parents-checks.md), which stays
immutable. Ancestry, the tag class, the nightly gate, the `release-publish`
environment, the PR selector (Decision item 3 of ADR 0195), and
`VERSION_ONLY_PATHS` are unchanged.

This ADR is Accepted alongside its implementation under
[ADR 0102](0102-accepted-alongside-implementation-with-explicit-approval.md).
Explicit maintainer approval is Brian's approval of both rules, recorded in
issue #4862 on 2026-10-21 (grant `adr-4862-coordinated-acceptance`). The
realizing code path is `release/authorize.py`.

## Context

ADR 0195 lets a version-only preparation commit borrow its first-parent
ancestor's green checks. Two problems keep that from firing in practice.

1. `curie dev bump-version` also registers the release's schema window in
   `cli/src/application_schema_windows.json`. That file is outside
   `VERSION_ONLY_PATHS`, so the first-parent walk ended at the first candidate
   and every release paid for a full CI run on the tagged commit.
2. For a minor release, the `next`-into-`main` merge commit's first parent is the
   old `main`, so the walk stops at once: it sees all of the feature work. The
   release PR head, the merge commit's second parent, already passed every
   required check. When that PR was up to date with `main`, the merge commit has
   the identical git tree, yet merged `main` still waited for its own full push
   CI.

## Decision

1. **Tree-identity inheritance.** When the tagged commit's own required
   check-runs are absent or still running, and none has concluded non-passing,
   `release/authorize.py` looks at every parent of the tagged commit, including
   the second parent of a merge. A parent whose `git rev-parse <sha>^{tree}`
   equals the tagged commit's tree, and whose `REQUIRED_CHECK_NAMES` are all
   current and green excluding the current workflow run, is the proof. This is
   checked before the version-only walk. Identical trees mean identical bytes, so
   there is no path allowlist and no depth limit. A parent with a different tree
   is never consulted.
2. **Schema window registration is version identity.** In the version-only walk,
   a delta that also changes `cli/src/application_schema_windows.json` counts
   only when the parsed file adds exactly `windows[X.Y.Z]`, where `X.Y.Z` is the
   tag without its leading `v` (an rc tag registers `X.Y.Z-rc.N`), the new entry
   equals the ancestor's `candidate` (what `curie dev bump-version` promotes), and
   no other key or existing window changes. Anything else ends the walk,
   including malformed JSON or a missing file. `VERSION_ONLY_PATHS` is not
   broadened.
3. **Contrary evidence still refuses.** An own required check on the tagged
   commit that concluded failure, cancelled, skipped, or any other non-passing
   result refuses in both cases, whatever parent is available.
4. **Output.** The authorizer names the path it used, `tree-identical parent` or
   `version-only ancestor`, and the proof commit.

## Consequences

1. A minor release merge that is up to date with `main` authorizes from the
   release PR head without waiting for push CI on `main`.
2. A patch preparation commit whose delta includes its schema window
   registration now reaches the version-only ancestor as ADR 0195 intended.
3. Push CI on `main` and `next` still runs every tier.
4. ADR 0195 rejected reusing the PR head (alternative 3) because path-reduced PR
   runs are not a full matrix. This ADR accepts a PR head only when its tree is
   identical to the tagged commit and every required check-run is present and
   green. A required job that was skipped is refused, as in #1470. The
   maintainer accepted this tradeoff when approving the rule.
5. This relates to [Draft ADR 0196](0196-combined-preparation-pr-checks.md). The
   tree-identical case covers an up-to-date preparation merge whose tree equals
   its head. ADR 0196 remains a Draft and this ADR does not accept it.

## Alternatives considered

1. **Accept any green second parent regardless of tree.** Rejected, it launders
   the merge result past the gate.
2. **Add the schema catalog to `VERSION_ONLY_PATHS`.** Rejected, arbitrary edits
   to the catalog change runtime compatibility.
3. **Recurse tree identity through ancestors.** Rejected, it is not needed: one
   hop covers the release merge.
4. **Walk first, then try tree identity.** Rejected, tree identity is the
   stronger proof and is cheaper.

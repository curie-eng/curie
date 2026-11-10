# 257. A release registers its schema window only at the cut

Date: 2026-11-02

Status: Accepted

This ADR amends [ADR 0247](0247-a-tree-identical-parent-or-a-schema-window-registration-reuses-checks.md),
which stays immutable. ADR 0247 Decision item 2 limited the schema window
registration rule to the release authorizer and left the PR selector and the
atlas pin gate on the plain `VERSION_ONLY_PATHS` set. This ADR applies the same
rule to all three gates. `VERSION_ONLY_PATHS` itself is unchanged.

This ADR is Accepted alongside its implementation under
[ADR 0102](0102-accepted-alongside-implementation-with-explicit-approval.md).
Explicit maintainer approval is Brian's approval on 2026-11-02, given when he
directed that the next release is never named before it is cut and that the
window is registered at the time of release. The realizing code path is
`release/atlas.py` (`version_only_extra`), called by `release/atlas.py`'s pin
gate, `release/authorize.py`'s version-only walk, and
`tools/e2e-ci-selection/select_tiers.py`.

## Context

The next release from a branch can be a patch or a minor release, and that is
decided at the cut. The schema window catalog already records the next
release's window under `candidate`. `curie dev bump-version X.Y.Z` copies it to
`windows[X.Y.Z]` in the release preparation commit.

Two gates did not accept that copy as version identity.

1. The atlas pin gate (`release/atlas.py`) refused any path outside
   `VERSION_ONLY_PATHS` between the atlas pin and the tag.
2. The PR selector treated a preparation PR that touched the catalog as a full
   change and ran every tier.

So every patch release registered `windows[X.Y.Z]` on `main` ahead of the cut,
which named the next release before it was decided. A test also asserted the
literal current release and enumerated every allowed window, which forced a
second non-version edit at each cut.

## Decision

1. `release/atlas.py` owns one function, `version_only_extra`, that returns the
   paths in a delta that are not version identity for a release. A path in
   `version_only_paths(<tag>)` is version identity. The schema window catalog
   is version identity only when its sole change is a new `windows` key equal to
   the tag without its leading `v`, whose value equals the base's `candidate`,
   with every other key and window unchanged (the ADR 0247 rule).
2. The atlas pin gate, the release authorizer's version-only walk and the PR
   selector in revision mode all decide with that function. The selector's path
   mode has no revisions to compare and keeps the plain path set.
3. No file on `main` or `next` names an unreleased version. Tests assert that no
   window names a release newer than the in-tree version, and that the in-tree
   version has a window, without naming either.

## Consequences

1. `curie dev bump-version` output is version-only for every gate, so the atlas
   pin stays at the preparation base and the preparation PR runs the version-only
   selection.
2. Nothing registers a window before the cut. A missing window for the next
   release is expected until `bump-version` runs.
3. A window registered for a release that has not been cut fails the catalog
   test, so a future-version entry cannot return silently.
4. A window for a version that was never released, but is older than the in-tree
   version, is no longer caught by an enumerated list. Released windows stay
   pinned by their explicit assertions and fixture.

## Alternatives considered

1. **Add the catalog to `VERSION_ONLY_PATHS`.** Rejected for the reason ADR 0247
   gave: arbitrary catalog edits change runtime compatibility.
2. **Never write the release's own window; read `candidate` for the in-tree
   version.** Rejected because a later CLI still needs every released window,
   so the window would have to be recorded after the tag instead, which adds a
   post-release step for no gain.
3. **Keep registering ahead of the cut.** Rejected because it guesses the next
   release number.

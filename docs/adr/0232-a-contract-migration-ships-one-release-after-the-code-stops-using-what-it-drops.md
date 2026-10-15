# 232. A contract migration ships one release after the code stops using what it drops

Date: 2026-10-15

Status: Draft

Extends decision 4 of
[ADR 0142](0142-database-compatibility-windows-and-a-single-upgrade-phase.md).
ADR 0142 stays Accepted and is not superseded. This Draft does not authorize
implementation. The enforcing gate is
[#4610](https://github.com/curie-eng/curie/issues/4610), blocked by
[#4608](https://github.com/curie-eng/curie/issues/4608).

## Context

ADR 0142 makes patch migrations expand and contract, and it says application
N-1 keeps serving after an expand. It sets no rule for when a contract
revision may ship relative to the code that stops using what it drops.

An upgrade to v0.13.0 ran contract revision 0103, which drops
`work_items.github_repository_id` and
`thread_publication_lineages.github_repository_id`, while the previous
application generation still served and still queried those columns. That
overlap is recorded in
[#4606](https://github.com/curie-eng/curie/issues/4606). On 2026-10-15 the
maintainer ruled that each contract migration ships one release after the
code stops using it.

## Decision

1. A revision of kind `contract` or `irreversible` in
   `apps/api/src/curie_api/revision_kinds.json` may ship in a release only if
   the previous released tag on the same line has no application code that
   reads or writes the table or column it drops or renames. The release that
   stops using a column ships first. The contract ships in a later release.
2. This rule extends ADR 0142 decision 4. It does not supersede any decision
   in ADR 0142.
3. Enforcement is a CI gate that compares the pull request head with the
   newest `vX.Y.Z` tag that is an ancestor of it. That gate is a separate
   issue, blocked by this ADR, and is not part of this change.

## Consequences

- The release that removes the last reader or writer of a dropped or renamed
  table or column and the release that contracts it are different releases on
  the same line.
- Expand still lands in the same release that starts writing the new shape.
  Application N-1 keeps serving after that expand, as ADR 0142 decision 4
  already requires.
- ADR 0142 decision 3 stays in force: migrations still apply in one phase
  (`curie-migrate` locally, the Helm migrate Job on cluster). This decision
  does not split that phase.
- Until the gate in #4610 lands, this timing is a review rule only.

## Alternatives considered

1. Split the migrator so expand runs pre-upgrade and contract runs
   post-upgrade. Rejected because release timing removes the overlap for
   every install path, including the compose `curie-migrate` phase, without
   changing the single migration phase ADR 0142 decision 3 sets.

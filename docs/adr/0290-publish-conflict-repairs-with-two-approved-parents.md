# 290. Publish a factory conflict repair with two approved parents

Date: 2026-11-23

Status: Accepted

The maintainer accepted this decision on 2026-11-23 for [issue #5944](https://github.com/curie-eng/curie/issues/5944).
The API enforces this decision during publication admission and commit
verification. The worker prepares the workspace and runs the publication Job.

## Context

A published factory pull request can conflict with new commits on its existing
base branch. An ordinary revision adds a commit on the old PR head. Even when
its files contain the desired result, that history gives Git no evidence that
the revision incorporated the advancing base. A base label change has a
separate admission and handoff under [ADR 0253](0253-factory-base-labels-resolve-at-claim-and-follow-relabels.md).

## Decision

1. Only a successfully completed factory request can receive a conflict repair.
   The maintenance service admits one successor against the same open PR and
   consumes the lineage's shared repair allowance. An active request keeps its
   existing conflict outcome. The maintenance feature defaults off.
2. The immutable admission supplies the authorized PR head H, the named base,
   and its observed tip T. The successor starts with a fresh checkout of T and
   a local ref for H. Its objective asks it to squash-merge H, resolve the
   conflicts, and run the repository checks for both behaviors. Ambiguous
   intent or failing checks require human attention.
3. The API selects the stored publication mode `base_conflict_merge` only
   from the successor's admission. The approval binds that mode, H, T and the
   digest of the patch. Publication validation runs on the cumulative change
   from T, including the PR's original changes. The existing protected path,
   patch size, dependency and review requirements apply to this change.
4. A prepare Job builds the approved tree from T and the patch and computes
   its commit with ordered parents H and T and one publication revision
   marker. Before any push, the worker records the exact commit SHA once on
   the publication row. The push Job checks that it built that commit before
   updating the existing PR branch under an exact lease on H.
5. The lineage base must be an ancestor of or equal to T, and T must be an
   ancestor of or equal to the current tip of the named base. H must not
   already contain T. A later fast-forward on the base preserves the admitted
   pins. After a lost response, the worker reads the PR branch even if the run
   has ended and the push Job and its logs are gone. Recovery requires no
   new Job or push credential. Only the stored merge SHA can pass commit
   verification and advance the lineage. H means nothing was pushed. An ended
   run waits for any live push Job before its final read. A foreign head, failed ancestry
   proof, or closed PR produces a handoff.
6. The API verifies the parents and ancestry and compares the proof with
   the locked lineage before recording the new head. Fresh CI must verify that
   head. Publication and CI settlement leave merging the PR to its owner.

## Consequences

This partially amends the expected-parent rule in [ADR 0143](0143-thread-owned-pull-request-lineage.md)
and items 1 and 4 of parent selection in [ADR 0256](0256-publication-validates-and-pushes-a-git-object-overlay.md).
The future Git object overlay publisher must carry this stored mode and enforce
the same approval, parent order, tree and push lease. Ordinary and first
publications retain their existing authority. A later label retarget of this
merge history reaches the existing non-linear history handoff.

## Alternatives considered

A single-parent commit on H leaves the conflicting base outside its ancestry.
Replaying the PR onto T can discard the identity of H that fenced the approval.
Repairing an active request requires replacing its live workspace while its
execution and settlement still own that workspace. A successor can start from
both stored pins with a fresh workspace.

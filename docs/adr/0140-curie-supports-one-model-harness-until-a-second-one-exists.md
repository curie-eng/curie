# 140. Curie supports one model harness until a second one exists

Date: 2026-09-01

Status: Draft

When Accepted, this ADR supersedes the Accepted decisions in
[ADR-0060](0060-the-harness-is-a-declared-package.md) and
[ADR-0062](0062-harness-conformance-has-teeth.md). It also closes the unfinished
program described by Draft
[ADR-0061](0061-out-of-process-harness-boundary.md). Until then, it authorizes
nothing.

## Context

Curie ships one model harness: Claude.

ADRs 0060 through 0062 started a broader program for multiple model engines. A
small part of that program exists today: `HarnessContribution`, a registry, and
import boundaries that keep Claude-specific code contained. The product does not
have a supported harness selector, a second harness, or the proposed
out-of-process boundary and cross-harness conformance suite.

The contribution manifest therefore promises more than Curie delivers. Five of
its fields are used by production: `name`, `aliases`, `build_spawn_env`,
`compile_bundle`, and `readonly_tools`. Five others have no production reader:
`image`, `install`, `auth`, `model_override_env_keys`, and `labels`. The `image`
field is particularly misleading because deployment configuration, not the
manifest, selects the runner image.

There is no value in completing a general multi-harness system before there is a
real second engine to test it against. Doing so would force Curie to guess what
that engine needs.

## Decision

1. **Curie supports one model harness for now.** We will not add a public harness
   selector or finish the multi-harness program until a real second engine is
   being added.
2. **The contribution manifest keeps only what production reads:** `name`,
   `aliases`, `build_spawn_env`, `compile_bundle`, and `readonly_tools`. The five
   unused fields and tests that only assert their literal values are removed.
3. **The minimal extension seam stays.** The registry, its fail-closed guards,
   the built-in Claude shortcut, and the import-linter boundaries remain. They
   keep the current code organized; they are not a claim that Curie already
   supports multiple engines. The existing runner-local `CURIE_HARNESS` knob
   remains internal; it is not promoted into the CLI, worker, compose, or chart.
4. **The unfinished program stops.** The selector, Omnigent boundary spike, fake
   harness rewrite, capability matrix, and expanded cross-harness conformance
   work tracked in [#844](https://github.com/curie-eng/curie/issues/844) are no
   longer current obligations.
5. **A real second engine reopens the decision.** A PR containing a working
   second harness is the trigger for a new ADR. That ADR may add the selector,
   manifest fields, deployment support, and conformance evidence that the
   concrete engine actually requires.
6. When this ADR is Accepted, ADRs 0060 and 0062 become `Superseded by ADR-0140`.
   Draft ADR 0061 receives a status-line backlink recording that its proposed
   program stopped before its prerequisite spike ran. Their bodies remain
   unchanged under [ADR-0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md).

## Consequences

The follow-up implementation is intentionally small and behavior-preserving:

- remove the unused fields and their helper types from
  `runner/src/curie_runner/harness/contribution.py`;
- remove those values from Claude's declaration in
  `runner/src/curie_runner/harness/claude.py`;
- update the affected harness tests and generated interface documentation.

The registry, runner boot behavior, deployment image selection, and current
Claude sessions do not change.

Curie gives up the appearance that another engine can be enabled by filling in a
manifest. When a second engine arrives, some machinery may need to be rebuilt.
That is deliberate: it will be designed against a real engine instead of a
hypothetical one.

## Alternative considered

**Finish the multi-harness system now.** Rejected. With only Claude in the tree,
the selector would have one choice and the remaining contracts would be designed
against guesses. The second implementation should shape the abstraction.

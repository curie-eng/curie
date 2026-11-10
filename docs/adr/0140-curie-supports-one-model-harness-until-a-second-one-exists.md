# 140. Curie supports one model harness until a second one exists

Date: 2026-09-01

Status: Accepted

Maintainer approval is recorded in
[#3828](https://github.com/curie-eng/curie/issues/3828).

This ADR supersedes the Accepted decisions in
[ADR 0060](0060-the-harness-is-a-declared-package.md) and
[ADR 0062](0062-harness-conformance-has-teeth.md). It also closes the unfinished
program described by Draft
[ADR 0061](0061-out-of-process-harness-boundary.md).

## Context

Curie ships one model harness: Claude.

ADRs 0060 through 0062 started a broader program for multiple model engines. A
small part of that program exists today: `HarnessContribution`, a registry, and
import boundaries that keep some Claude code contained. The product does not
have a supported harness selector, a second harness, or the proposed process
boundary and conformance suite covering multiple harnesses.

The contribution manifest therefore promises more than Curie delivers.
Production consumes `name`, `aliases`, `build_spawn_env`, `compile_bundle`,
`readonly_tools`, and the `supports_structured_replay` capability. The registry
uses the identity and aliases. The replay capability decides whether recovered
structured history may be passed to the harness.

The `auth` field also has a current production reader: runner boot takes
`auth.credential_env_keys` as the credential names to redact. This change
migrates that reader to `sdk_auth.DEFAULT_CREDENTIAL_ENV_KEYS`, preserving the
Claude credential redaction policy before removing `auth`. The `image`,
`install`, `model_override_env_keys`, and `labels` fields have no production
reader. Deployment configuration, not the manifest, selects the runner image.

The runner currently resolves a built in Claude name through a direct import,
but other names reach registry discovery and can select an installed
contribution. That behavior is broader than the supported product. Approval
policy also imports Claude SDK types in the runner core, so its provider
boundary is incomplete.

There is no value in completing a general system for multiple harnesses before
there is a real second engine to test it against. Doing so would force Curie to
guess what that engine needs.

## Decision

1. **Curie supports one model harness for now.** We will not add a public harness
   selector or finish the program for multiple harnesses until a real second
   engine is being added.
2. **The contribution manifest keeps only active fields:** `name`, `aliases`,
   `build_spawn_env`, `compile_bundle`, `readonly_tools`, and
   `supports_structured_replay`. Remove `image`, `install`, `auth`,
   `model_override_env_keys`, `labels`, and their unused helper types. First
   migrate credential redaction to `sdk_auth.DEFAULT_CREDENTIAL_ENV_KEYS`.
3. **The minimal extension seam stays.** Keep the registry, its guards, aliases,
   the structured replay capability, and the import boundaries. They organize
   the current code without promising support for another engine. The internal
   runner setting `CURIE_HARNESS` remains available only for the built in Claude
   name and aliases. `_resolve_harness` admits those names by direct import and
   rejects every other name with `UnsupportedHarnessError` before discovery,
   including a name that an installed nonClaude contribution has registered.
4. **Approval policy has a provider boundary.** `ApprovalGate` and its decision
   results stay in `runner/src/curie_runner/approval.py` without Claude SDK
   imports. Claude permission callbacks and hook matchers move to
   `runner/src/curie_runner/harness/claude/approval.py`. The Claude contribution
   becomes a package at `runner/src/curie_runner/harness/claude/__init__.py`.
   The import rules forbid SDK imports from the gate and ratchet the remaining
   legacy SDK imports, exempting only the Claude harness package.
5. **The unfinished program stops.** The selector, Omnigent boundary spike,
   fake harness rewrite, capability matrix, and expanded conformance work
   tracked in [#844](https://github.com/curie-eng/curie/issues/844) are no longer
   current obligations.
6. **A real second engine reopens the decision.** A PR containing a working
   second harness is the trigger for a new ADR. That ADR may add the selector,
   manifest fields, deployment support, and conformance evidence that the
   concrete engine actually requires.
7. ADRs 0060 and 0062 are `Superseded by ADR 0140`. Draft ADR 0061 has a
   status line backlink recording that its proposed
   program stopped before its prerequisite spike ran. Their bodies remain
   unchanged under [ADR 0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md).

## Consequences

The implementation removes unused manifest fields, preserves Claude credential
redaction, and separates approval policy from the Claude SDK adapter. The
registry still validates contributions independently, but runner boot changes
from generic registry selection to supported Claude names only. Unknown names
and registered nonClaude names fail before entry point discovery. A broken
sibling entry point therefore cannot affect the default or an alias boot path.
Deployment image selection and current Claude session behavior remain the same.

The realizing code is `_resolve_harness` in
`runner/src/curie_runner/__main__.py`, the policy gate in
`runner/src/curie_runner/approval.py`, and the Claude adapter in
`runner/src/curie_runner/harness/claude/approval.py`. The Claude contribution
is declared in `runner/src/curie_runner/harness/claude/__init__.py`.
[#3828](https://github.com/curie-eng/curie/issues/3828) records maintainer
approval and tracks the realizing implementation.

Curie gives up the appearance that another engine can be enabled by filling in a
manifest. When a second engine arrives, some machinery may need to be rebuilt.
It will be designed against a real engine instead of a hypothetical one.

## Alternative considered

**Finish the system for multiple harnesses now.** Rejected. With only Claude in
the tree, the selector would have one supported choice and the remaining
contracts would be designed against guesses. The second implementation should
shape the abstraction.

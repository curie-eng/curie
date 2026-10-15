# Proposed input contract compatibility binding

Status: proposed contract for review; no runtime support is claimed.

This is the smallest static compatibility slice of [ADR 0222](../../adr/0222-coworker-bundles-own-workflow-policy-and-curie-enforces-execution.md) and #4589. It does not implement input interpretation, readiness evaluation, or a new workflow controller. The preparation installation work in #4554 can proceed independently; deployment enforcement depends on a separately reviewed frozen contract change.

## Declaration

Add the optional `inputContracts` field to the bundle manifest as a strict Curie declaration inside the otherwise lenient plugin manifest:

```json
{
  "inputContracts": {
    "interface": "curie/input-contracts@1",
    "accepts": ["example.org/review-request@1"],
    "required": true
  }
}
```

The interface string is exact. Contract identifiers are opaque, nonempty ASCII identifiers of at most 200 bytes, restricted to letters, digits, `.`, `_`, `/`, `-`, `:` and `@`; no whitespace, URLs with credentials, wildcard, or semantic version inference. The `required` field is literal true in interface version 1; false is rejected rather than interpreted as a bypass. The accepted set has 1–32 unique identifiers. Unknown nested keys, malformed IDs, duplicates and unsupported interface versions refuse deployment. Producers use explicit new identifiers for incompatible inputs. Their owners define backward compatibility by listing every supported identifier, never by a platform guess about product release numbers.

The declaration is part of the immutable bundle archive and its existing SHA-256. It adds no independent mutable settings override. A declaration-bearing bundle requires the caller to name enforcement support for `curie/input-contracts@1` when validating, following the existing tool-policy capability handshake pattern. Consumers must not silently ignore this control-bearing field merely because manifest decoding is lenient.

A missing declaration preserves existing bundles' runtime behavior. It means unknown compatibility to preparation clients, never support for every contract. Old platforms that predate enforcement cannot safely accept newly declared bundles; release tooling must require the enforcement capability before deploying one. This limitation must be visible, since an optional field alone cannot retroactively make an old lenient validator reject it.

## Read-only deployed identity

Existing read endpoints expose `Deployment.version_id` and version `bundle_sha256`; reuse these identities rather than inventing another deployment store. The version files endpoint is an archive viewer, not a compatibility handshake and does not attest that the API enforces the declaration.

Add an authenticated, tenant-scoped `GET /deployments/{deployment_id}/input-contracts` response containing `interface`, `deployment_id`, `agent_id`, `version_id`, `bundle_sha256`, `accepts`, and `enforcement`. `enforcement` is the literal `admission-and-start` only after both production checks exist; earlier implementations must return unavailable rather than advertise support. The server reads the declaration from the exact digest-bound stored archive under existing extraction caps, verifies digest integrity, and returns unknown on absent/unavailable/unsupported bindings. No tracker content or credentials are returned. Clients additionally verify that the selected deployment is active and is the current binding for the configured repository/start surface.

## Opaque per-input binding

A contract claim is an author declaration, not proof that its content satisfies that contract. Do not put a new canonical heading into tickets or infer a contract from seven section names. Do not use a label name as the contract identity.

Use a single strict opaque HTML comment prefix, followed by a line feed and the unchanged specification body:

```text
<!-- curie-input@1 {"contract":"example.org/review-request@1","deployment":"00000000-0000-4000-8000-000000000001","bundle_sha256":"<64 lowercase hexadecimal digits>","body_sha256":"<64 lowercase hexadecimal digits>"} -->
<specification body>
```

The prefix must begin at byte zero, fit within 2048 UTF-8 bytes including its terminating line feed, and contain exactly those four keys with string values; duplicate/unknown keys, duplicate prefixes, malformed JSON, unsupported versions, missing delimiter/newline and invalid UUID/hash grammar refuse compatibility. Never scan arbitrary comments to find a convenient matching marker. `body_sha256` is SHA-256 of the exact UTF-8 encoding of the remaining body, without line-ending or Unicode normalization and without trimming. The placeholder example is deliberately not a valid digest. A publication readback must hash the tracker-returned remaining body rather than assume it preserved local line endings. Contract and deployment IDs are synthetic in examples.

This prefix is transport metadata, not a factory section or a new semantic interpretation of the body. Existing factory-owned parsers may ignore comments, but the generic compatibility gate reads this one explicitly. The adapter fetches current input and authorized start facts, computes the digest, verifies selected deployment ownership and the current immutable bundle SHA, then compares the opaque contract ID to `accepts`. User-authored prefixes never grant start authority or prove semantic correctness. A different published title does not change this body-format compatibility claim; title semantics remain evaluator-owned.

The existing `ExecutionRequest` has no appropriate generic metadata field. Add one nullable strict receipt with only `interface`, `contract`, `deployment_id`, `version_id`, `bundle_sha256`, and `body_sha256` through a backward-compatible migration. An implementation may use explicit nullable columns or typed JSONB with a shape constraint, but cannot reuse `objective` as the receipt. The receipt is all-null for legacy requests, is copied atomically with admission snapshots and queued revisions, and is immutable once admitted. It stores no raw tracker body, title or conversation. Compare-and-set/replay checks include receipt equality. Required receipt fields and exact migration are reviewed before merging enforcement code.

## Enforcement and rollout

For declared bundles, all initial/revision/reselection admission paths resolve the current deployed owner, read the current authoritative input and match its prefix, accepted contract ID and immutable bundle SHA-256 before any implementation WorkItem or ExecutionRequest is created. Missing, stale, unsupported or unavailable compatibility refuses new work as unknown or changes-required under existing lifecycle conventions. It does not automatically select the issue later. Execution start rechecks the accepted request's pinned identities and current relevant facts before planning.

A local preselection check is advisory. The server repeats it, closing a deployment-change race between checking and adding the start label. A caller may publish reviewed tracker content without a compatibility proof but must not add a start label while compatibility is unknown. Existing unmodified legacy bundles retain legacy admission. Existing requests retain their original deployment/policy owner; rollback changes new work selection without reinterpreting active requests.

The static checker only compares typed identifiers/digests. Factory-owned canonical input validation still needs #4589's bounded evaluator. A matching declaration is necessary compatibility evidence, never full readiness, semantic correctness, approval, or a permission grant. No factory preparation Python is imported into the API, worker or shared platform packages.

## Required contract artifacts and proof

The implementation contract PR must add the manifest model/strict declaration reader, committed JSON Schema, field-parity mirror declaration or justified omission, validator enforcement handshake and documented version compatibility together. Any ACI extension must be independently justified; this static API slice does not inherently require one.

Before dependent implementation merges, prove:

- schema regeneration agrees with committed artifacts, and unknown base plugin fields remain accepted;
- malformed/duplicate/unsupported declarations are rejected, including missing enforcement capability;
- a minimal non-factory bundle and the factory fixture use the same opaque declaration;
- read-only identity refers to the actual selected deployment and archive digest, and refuses stale/missing/corrupt archives without fallback;
- two repositories in the same client session select different preparation contracts with no global-version interference;
- all intake modes and start rechecks refuse absent/stale/incompatible bindings with no new implementation work;
- a deployment change after client checking is refused by intake, while an unchanged compatible case passes;
- raw tracker data is not retained in platform records/logs; idempotency, authorization, rollback and old-request owner pinning remain intact.

Implementation sequencing: first review the frozen manifest declaration and prefix serialization in this proposal; then implement the declaration/schema/validation handshake in a separate contract change. Dependent deployment/admission/start code needs the nullable receipt migration above and must use existing unknown/backoff lifecycle rather than invent another scheduler. The declaration and prefix can be reviewed independently of that runtime implementation. This proposal does not authorize merging enforcement that omits the start recheck or claims a static match as readiness.

## Managed launch enforcement capability

The runner also validates a bundle before boot, including standalone local launches. It must not name input-contract enforcement unconditionally: a standalone launch has no authoritative tracker intake or start recheck.

Add optional `BootEnv.input_contract_enforcement: str | None = None`, owned by the worker and rendered as `CURIE_INPUT_CONTRACT_ENFORCEMENT`. This is a generic platform boot capability, separate from the ACI `SessionConfig`. It is an additive optional ACI schema change and bumps the protocol patch from 0.5.22 to 0.5.23, with regenerated schema, Rust, TypeScript, env example and wire lock. Unknown opaque strings remain decodable but confer no enforcement capability on the runner.

The authenticated API acquire grant carries the admitted opaque receipt. The worker compares the selected trusted deployment/version/bundle identities to that receipt before sandbox claim and emits the supported enforcement interface only for a matching managed request. The runner passes the input-contract validator handshake only when this launch field equals the supported interface. Standalone launches and old workers omit it and refuse declaration-bearing bundles. The API still rechecks input, selected target and actual worker runtime identities before any model planning. A boot capability does not replace the start fence or grant tracker/publication authority.

The declaration governs platform tracker implementation admission. Standalone local skill/evaluation runs have no such admission and may validate declaration syntax using explicit validator context `standalone`, without claiming compatibility enforcement or gaining tracker publication authority. The validator defaults to `tracked-implementation`, which requires enforcement; unknown context values fail. Managed runner launches must never classify themselves as standalone solely because the newly added capability field is missing. Runtime applicability must use actual trusted work-item launch context, and supported platform versions must enforce intake/start independently under mixed worker versions.

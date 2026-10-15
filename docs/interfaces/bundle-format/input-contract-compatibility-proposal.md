# Proposed input contract compatibility binding

Status: proposed contract for review; no runtime support is claimed.

This is the smallest static compatibility slice of [ADR 0222](../../adr/0222-coworker-bundles-own-workflow-policy-and-curie-enforces-execution.md) and #4589. It does not implement input interpretation, readiness evaluation, or a new workflow controller. The preparation installation work in #4554 can proceed independently; deployment enforcement depends on a separately reviewed frozen contract change.

## Declaration

Add the optional `inputContracts` field to the bundle manifest as a strict Curie declaration inside the otherwise lenient plugin manifest:

```json
{
  "inputContracts": {
    "interface": "curie/input-contracts@1",
    "accepted": ["example.org/review-request@1"]
  }
}
```

The interface string is exact. Contract identifiers are opaque, nonempty ASCII identifiers of at most 200 bytes, restricted to letters, digits, `.`, `_`, `/`, `-`, `:` and `@`; no whitespace, URLs with credentials, wildcard, or semantic version inference. The accepted set has 1–32 unique identifiers. Unknown nested keys, malformed IDs, duplicates and unsupported interface versions refuse deployment. Producers use explicit new identifiers for incompatible inputs. Their owners define backward compatibility by listing every supported identifier, never by a platform guess about product release numbers.

The declaration is part of the immutable bundle archive and its existing SHA-256. It adds no independent mutable settings override. A declaration-bearing bundle requires the caller to name enforcement support for `curie/input-contracts@1` when validating, following the existing tool-policy capability handshake pattern. Consumers must not silently ignore this control-bearing field merely because manifest decoding is lenient.

A missing declaration preserves existing bundles' runtime behavior. It means unknown compatibility to preparation clients, never support for every contract. Old platforms that predate enforcement cannot safely accept newly declared bundles; release tooling must require the enforcement capability before deploying one. This limitation must be visible, since an optional field alone cannot retroactively make an old lenient validator reject it.

## Read-only deployed identity

Existing read endpoints expose `Deployment.version_id` and version `bundle_sha256`; reuse these identities rather than inventing another deployment store. The version files endpoint is an archive viewer, not a compatibility handshake and does not attest that the API enforces the declaration.

Add an authenticated, tenant-scoped `GET /deployments/{deployment_id}/input-contracts` response containing `interface`, `deployment_id`, `agent_id`, `version_id`, `bundle_sha256`, `accepted`, and `enforcement`. `enforcement` is the literal `admission-and-start` only after both production checks exist; earlier implementations must return unavailable rather than advertise support. The server reads the declaration from the exact digest-bound stored archive under existing extraction caps, verifies digest integrity, and returns unknown on absent/unavailable/unsupported bindings. No tracker content or credentials are returned. Clients additionally verify that the selected deployment is active and is the current binding for the configured repository/start surface.

## Opaque per-input binding

A contract claim is an author declaration, not proof that its content satisfies that contract. Do not put a new canonical heading into tickets or infer a contract from seven section names. Do not use a label name as the contract identity.

Before selection, a generic authenticated input-binding operation records the approved contract ID against a verified tracker input fingerprint, repository/start binding, selected deployment and immutable bundle SHA-256. The request carries only typed source identifiers and contract/deployment identities; the platform rereads tracker content through the authorized adapter and computes the fingerprint itself. It performs no tracker writes. The binding is invalidated by any input or target change. Store only authorized digests, contract identifiers and compact status codes, not body/title text or preparation conversations.

For tracker inputs, fingerprint version `curie/tracker-input@1` hashes UTF-8 compact JSON of the ordered array `[title, body]`, with Unicode preserved, no insignificant spaces, no Unicode normalization and no trailing newline. Missing title/body are adapter-normalized to empty strings before hashing. Both the client-side evidence and server adapter must use common test vectors. This serialization is a generic tracker adapter identity rule, not the factory's semantic canonicalization fingerprint.

The operation must use current tracker start authority, tenant and repository grants; public issue authors cannot mint bindings for unauthorized start surfaces. It must use compare-and-set on input fingerprint and target bundle, reject stale expectations, and be idempotent for identical bindings. It creates no implementation WorkItem or ExecutionRequest. Exact endpoint and durable storage migration belong in the implementation review; their presence is prerequisite to claiming label-based handoff is protected.

## Enforcement and rollout

For declared bundles, all initial/revision/reselection admission paths resolve the current deployed owner, read the current authoritative input and match its binding, accepted contract ID and immutable bundle SHA-256 before any implementation WorkItem or ExecutionRequest is created. Missing, stale, unsupported or unavailable compatibility refuses new work as unknown or changes-required under existing lifecycle conventions. It does not automatically select the issue later. Execution start rechecks the accepted request's pinned identities and current relevant facts before planning.

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

Open review points before freezing implementation: the exact generic binding endpoint/storage model; mapping compatibility failures into the existing unknown/backoff lifecycle; and whether admission currently has enough pinned deployment identity to avoid a model migration. These are explicit prerequisites, not permission to treat label publication as sufficient enforcement.

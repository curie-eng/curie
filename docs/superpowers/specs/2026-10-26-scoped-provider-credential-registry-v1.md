# Scoped provider credential snapshots v1

This realizes only the in-memory registry binding from Accepted [ADR 0246](../../adr/0246-common-admission-resolves-an-optional-http-identity-before-authorized-enqueue.md) and [authority configuration](2026-10-23-http-authority-config-v1.md), reusing [transport](2026-10-23-http-identity-transport-v1.md). It introduces no filesystem loader, settings discovery, credential generation, startup wiring, activation or runtime consumer.

## 1. Trusted initializer and immutable registry

<!-- @spec HTTP-CREDENTIAL-REGISTRY-V1-1 -->
Add pure `curie_api.identity.http_credential_registry.build_provider_credential_registry(registrations)`, returning an opaque `ProviderCredentialRegistry` or `CredentialRegistryRefusal`. Input is an actual tuple of at most 256 actual two-element tuples `(AuthorityConfigV1, ProviderBearer)`. Empty is valid. Reject other outer/entry shapes, non-HTTP selections and invalid constructed/mutated inputs. No mapping, generator, plaintext secret or file-reference input is accepted. Revalidate existing configuration and bearer grammar through their existing owners; do not duplicate those grammars or change their acceptance.

The initializer is trusted server code, not an inbound request/provider. It owns protected material loading, independent randomness, separation from the platform key, immutable provisioning inventory and continuity across restart/registry replacement. This library cannot establish those properties from a Python object and does not claim to detect an administrator rewriting trusted inventory and material together. An arbitrary new registry is not a credential-rotation protocol. Same-reference file replacement is not rotation. Rotation requires a distinct reference/generation plus the authority version/invalidation protocol already specified; no source of ambient credentials or previous-generation fallback is added.

Construction is all-or-nothing and performs no I/O. Snapshot accepted bearer bytes in the existing immutable `ProviderBearer`; retain no registration tuple or configuration object supplied by the initializer. Detach metadata through ordinary existing model revalidation. This is in-process immutable binding, not a durable secret store or authority owner.

## 2. Exact binding and duplicate refusal

<!-- @spec HTTP-CREDENTIAL-REGISTRY-V1-2 -->
Each entry binds `(tenant_id, issuer, lookup_url, credential_ref.id, credential_ref.generation)` exactly. Keys remain canonical/exact under existing configuration validation; no normalization, issuer-only matching, URL inference, prefix/wildcard, case folding or default generation. Reject every repeated `(id,generation)` within one build, even identical repeats, conflicting scope or different bearer bytes. Multiple distinct explicit generations may coexist, but no generation is selected implicitly.

Authority version and freshness are not secret identity: an otherwise exact registration may serve a later valid configuration version or explicit freshness settings without rebinding secret bytes. The consumer must capture/revalidate those current authority values through the established owner. This library supplies no authority-version CAS or revocation fence.

## 3. Resolution and detached transport inputs

<!-- @spec HTTP-CREDENTIAL-REGISTRY-V1-3 -->
`registry.resolve(configuration)` accepts only an `AuthorityConfigV1` instance with valid HTTP selection, revalidating nested fields. Invalid, non-HTTP, missing reference, absent generation or any tenant/issuer/URL mismatch returns `CredentialRegistryRefusal(code="provider_credential_unavailable")`; never expose whether a reference exists in another scope. No fallback to another entry, local projection or other credential.

Each successful resolution creates a new detached opaque `ResolvedProviderCredential` with read-only explicit properties `target`, `limits` and `bearer`. Target is the existing `LookupTarget`, derived from the exact validated input's URL, tenant, issuer and authority version. Limits is the existing `FreshnessLimits`, derived without defaults from its three explicit freshness inputs. Bearer is the accepted snapshot's existing `ProviderBearer`; no second secret class or conversion to an inline configuration field. No sender, request ID, purpose, allow verdict or extra authority is manufactured.

No input model or mutable initializer collection is retained by registry/result. Metadata and limits are detached. A later in-place initializer mutation does not change the accepted registry; resolving a newly supplied changed configuration still requires its exact registered binding. Future compositions must use target, limits and bearer from the same resolved handle, not swap independently sourced inputs.

## 4. Opaque handles and safe refusals

<!-- @spec HTTP-CREDENTIAL-REGISTRY-V1-4 -->
Registry and resolved handle have fixed `str`/`repr` values `ProviderCredentialRegistry()` and `ResolvedProviderCredential()`, identity equality/hash, no public constructor, instance dict, iteration or mapping/model serialization. Reject assignment/deletion with fixed text. Reject `__getstate__`, `__reduce__` and `__reduce_ex__` with fixed `TypeError` text and no original exception chain. Deliberate Python introspection and removal of all runtime secret copies are not promised.

Explicit `bearer.value` remains the intentional sensitive access already owned by transport. No secret is exposed by ordinary handle formatting/errors. Resolved-handle truth testing raises `TypeError("provider_credential_is_not_permission")`; it is not an admission decision.

The only refusal codes are `invalid_provider_credential_registry` for unsuccessful construction and `provider_credential_unavailable` for unsuccessful resolution. `CredentialRegistryRefusal` is frozen, code-only and forbids extras. No validation detail, source object, path, registry ID, generation, tenant, issuer, endpoint, digest, bearer or OS/exception message is included in refusals/logs. Emit no logs. Ordinary invalid inputs and revalidation exceptions become the fixed applicable refusal without retaining or chaining the original error.

## 5. No runtime authority claim

<!-- @spec HTTP-CREDENTIAL-REGISTRY-V1-5 -->
Beyond ordinary Python module loading (including Pydantic schema initialization's `PYDANTIC_DISABLE_PLUGINS` read) and the reused transport module's existing passive lock construction, import/build/resolve perform no material-file or environment discovery, network, DB, clock, process or started-thread activity. The registry introduces no locks. The Pydantic exception is confined to its existing schema-loader call during class initialization; build/resolve permit no environment access. Do not add caches of identity decisions, expiry defaults, locks, mutable revoke methods, startup hooks, routes or writers. No existing caller uses this module in this slice. A consumer still needs authoritative current configuration/registry selection, protected provisioning, generation invalidation and post-lookup current-context checks. An old registry/handle remaining in memory is not magically revoked when the initializer creates another registry or changes a file.

This slice cannot prove atomic authorized enqueue, current worker authority, active identity, principal kind, policy permission, source freshness or safe rollout. A registry gate alone cannot prevent an independently delayed queue operation after SQL-lock/process loss. Do not describe this binding library as an activated credential lifecycle.

## 6. Executable proof

<!-- @spec HTTP-CREDENTIAL-REGISTRY-V1-6 -->
Use independently authored synthetic configurations/bearers and no real credential, endpoint or secret locator. Prove empty/valid/multiple entries, the 256/257-entry boundary and exact tuple types; every exact binding component and unknown generation; duplicate reference/generation with equal and conflicting material/scope; valid version/freshness changes; malformed/constructed/nested-mutated inputs; snapshot detachment; fixed refusals and opaque immutable formatting/state/pickle behavior; direct properties compatible with existing transport types; fresh-process import/build/resolve without external I/O or runtime service imports. Include positive and negative/secondary paths for every criterion. No tests manufacture full permission or claim entropy/provider acceptance from syntactic bearer validity.

Commit the specification before independent intended-red tests and source. Keep the local dependency combination unpublished until both prerequisite PRs land; publish only the new task's own commits after rebasing onto their converged public ancestry. Parent evidence is not final-head CI proof. Exact-head required CI remains necessary before merge; real protected provisioning and integration remain separate qualification gates.

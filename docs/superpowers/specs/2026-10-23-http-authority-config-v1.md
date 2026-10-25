# HTTP authority configuration representation v1

This contract defines only pure internal representations and validation. It adds no runtime consumer, administration route, persistence, settings/secret loader, provider artifact, activation or transition operation.

The realizing architecture is [Accepted ADR 0246](../../adr/0246-common-admission-resolves-an-optional-http-identity-before-authorized-enqueue.md). Facts remain governed by [HTTP identity facts v1](2026-10-23-http-identity-facts-v1.md).

## 1. Representation and exact scope

<!-- @spec HTTP-AUTHORITY-CONFIG-V1-1 -->

All object members below are required. No member permits null. Objects reject unknown fields. Integers are actual integers, never booleans, strings or floats. Strings are actual strings; no trimming, case folding, identifier inference or URL normalization occurs.

```text
CredentialReferenceV1 = {
  id: registry_id,
  generation: integer >= 1
}

AuthoritySelectionV1 =
  { mode: "disabled" }
  | {
      mode: "local_projection",
      issuer: issuer,
      governed_domains: DirectoryConfigIn.governed_domains,
      managed_by: DirectoryConfigIn.managed_by
    }
  | {
      mode: "http",
      issuer: issuer,
      lookup_url: lookup_url,
      credential_ref: CredentialReferenceV1,
      max_identity_age_ms: integer > 0,
      max_membership_age_ms: integer > 0,
      max_clock_skew_ms: integer >= 0
    }

AuthorityConfigV1 = {
  schema_version: integer 1,
  tenant_id: canonical UUID,
  version: integer >= 1,
  selection: AuthoritySelectionV1
}
```

`tenant_id` uses the lowercase hyphenated canonical UUID representation already specified by T1. `issuer` uses T1's existing issuer validation semantics, remains an exact string, and has the existing `DirectoryConfigIn.issuer` 2,048-character configuration ceiling. This configuration ceiling does not narrow T1's independent provider-facts representation. Local `managed_by` and governed-domain validation/canonical domain ordering reuse `DirectoryConfigIn`; do not duplicate or change their grammar. Thus the existing intentional sorted governed-domain list is the sole collection normalization inherited here; issuer and identifier strings remain exact.

The captured configured scope is `(tenant_id, selection.issuer)` when that mode has an issuer. No issuer exists in a disabled selection. HTTP lookup construction later copies these values to T1 `scope.tenant_id` and `scope.expected_issuer`; provider results cannot substitute either. Channel identity, installation, namespace and native sender remain authenticated operation-specific inputs, not configurable sender overrides in this object. There is no principal UUID, group grant, role filter, human-kind override, policy verdict or message text in this configuration.

Strict mode separation is intentional: disabled rejects all local/HTTP fields; local rejects lookup, credential and freshness fields; HTTP rejects governed_domains and managed_by. A selected HTTP representation does not delete the old projection metadata or authorize its conversion; future persistence must preserve the single authority record and the explicit migration contract.

## 2. Existing version ownership and absence

<!-- @spec HTTP-AUTHORITY-CONFIG-V1-2 -->

`schema_version=1` identifies this representation. `version` is the existing authority configuration version owned by `TenantIdentityProvider.version`; it is not a second sequence. A later caller captures this same value as T1 `authority_version`. Credential `generation`, directory `identity_epoch`, principal `authorization_version`, team epoch and policy version remain distinct values and cannot substitute for it.

This slice does not introduce `expected_authority_version`, an update command or a new create sentinel. The existing directory administration contract remains unchanged: `DirectoryConfigIn.expected_version` is an integer >=0; `directory_service.update_configuration` compares it with the existing row's version or 0 for an absent row, creates version 1 and increments an existing version on a successful write. A later authority administration realization must reuse that ownership/CAS convention rather than create parallel state. No pure model validation here reads a row, checks CAS or increments a version.

Missing new configuration is represented outside `AuthorityConfigV1` by absence, not a synthetic `{mode:"disabled",version:0}`. It preserves the installation's existing authority behavior. Do not infer disabled, HTTP, or a newly selected local mode merely from absence. An existing known local projection may be represented explicitly with its current tenant/issuer/domain/managed_by/version values; doing so is a pure projection, not a migration or a write.

Explicit disabled is a well-formed selection, not a request to change mode. Validating any selection establishes only representability, never transition eligibility or active support. Current HTTP-to-local/disabled direct downgrade remains refused by the later runtime contract; accepting the target's shape does not override that rule.

## 3. Configured lookup URL

<!-- @spec HTTP-AUTHORITY-CONFIG-V1-3 -->

`lookup_url` is a nonempty absolute HTTPS URL string with a hostname, an optional valid numeric port from 1 through 65,535, and an optional path. It has no userinfo (reject any `@` in its authority), query delimiter, fragment delimiter, any whitespace character (`str.isspace()`), ASCII control characters, DEL, or backslash. This excludes interior hostname/path whitespace as well as surrounding whitespace. Percent-encoded path whitespace such as `%20` remains allowed and exact; validation does not decode or normalize it. Preserve the accepted string byte-for-byte; do not append `/lookup`, resolve a relative reference, normalize issuer/path, infer a URL from an issuer, or use it as a template. `https://identity.example/lookup` is a synthetic example, not a product default.

This name is new because T1 intentionally has no configured endpoint field; its provider-hosted reference operation `POST /lookup` does not supply a deployment URL. HTTPS and credential-free URL shape validation are pure configuration checks. No DNS/HTTP lookup, redirect check, deployment reachability, address/SSRF policy, TLS qualification or credential binding is performed or claimed by this slice. Those runtime obligations belong to the transport/configuration-binding realization and cannot be inferred from successful parsing.

## 4. Immutable server-registry credential reference

<!-- @spec HTTP-AUTHORITY-CONFIG-V1-4 -->

`registry_id` is a case-sensitive ASCII string matching the complete grammar `[A-Za-z0-9][A-Za-z0-9_-]{0,63}`. No whitespace, slash, dot, colon, URL, interpolation, shell syntax or filesystem path is admitted. `generation` is required and >=1. Both fields are metadata; neither is a credential value or a credential-minting instruction.

The reference selects exactly one immutable generation in a server-owned registry. A later binding realization must authorize that registry entry for the exact tenant, issuer and configured lookup endpoint, load its independently random credential through the server's protected mechanism, and bind the credential bytes to the captured generation. The same `(id,generation)` must never silently resolve to different secret bytes. Rotation requires a distinct generation/reference and the authority version/invalidation protocol; mutable file replacement under an unchanged reference is not that protocol.

This slice defines the reference's syntax only: it creates no registry, path resolution, file loader, secret store, key derivation, generation allocation, random-byte production or check that an entry exists. A syntactically valid reference does not prove readiness, randomness, separation from the platform key or permission to call the provider. Runtime missing/unbound/unreadable references must refuse, with no previous-generation or local-authority fallback, under their later runtime contract.

Plaintext credential, arbitrary path and inline secret fields are rejected, including aliases such as `token`, `credential`, `secret`, `token_file` or `credential_path`. Existing forge token-file loading is precedent for protected server-held secrets, not permission to expose filesystem paths in this representation. Do not copy the hook secret's platform-key derivation: ADR 0246 requires an independent random credential.

## 5. Explicit freshness inputs

<!-- @spec HTTP-AUTHORITY-CONFIG-V1-5 -->

HTTP selection requires all three existing T1 names: `max_identity_age_ms`, `max_membership_age_ms` and `max_clock_skew_ms`, with T1's integer bounds above and no defaults. Zero identity/membership age, negative skew and omission refuse. Identity and membership remain independently bounded; neither substitutes for the other. This slice adds no additional freshness ceiling or product TTL policy.

These values are configuration inputs for T1 validation, not evidence about source observation, invalidation or provider ordering. Pure configuration validation does not construct observations, compute a grant, cache a positive identity or establish that a later provider response is causally newer. Provider freshness and rotation/runtime generation checks remain later qualification gates.

## 6. Pure validation, readback and bounded refusal

<!-- @spec HTTP-AUTHORITY-CONFIG-V1-6 -->

A pure validator consumes an already decoded internal mapping and returns a validated `AuthorityConfigV1` or a refusal carrying only one stable code. The API-owned contract names are `validate_authority_config_v1(value)` and `AuthorityConfigRefusal(code)`; these are not HTTP endpoints. There is no bytes/JSON parser or request-body policy in this slice. Decoded duplicate-key handling belongs to a future decoding boundary; do not claim it here.

The only refusal codes are `invalid_authority_configuration` and `unsupported_authority_configuration_version`. Check the top-level schema_version first: if the root is an object and schema_version is an actual integer other than 1, return the unsupported-version code. Missing/noninteger schema_version, a nonobject root or any other invalid shape/value returns the invalid-configuration code. Valid schema version followed by any union/value failure returns the invalid-configuration code. A caller cannot obtain raw validation messages, input values, URLs, reference IDs, field locations, provider exceptions or stack traces from this refusal.

Successful intentional configuration readback contains exactly the validated representation above, including registry id/generation and the configured URL. It contains no plaintext credential, storage path, resolved secret, arbitrary extras, or claimed `ready`/`active` state. Registry metadata is not proof that a secret exists. This is deliberate readback to an authorized internal caller, not blanket permission to log or publish configured values. Invalid candidates are never serialized as readback or echoed in errors. Safe refusal formatting uses only the stable code; framework validation exceptions, reprs and arbitrary original inputs must not escape the pure boundary.

No API OpenAPI drift, provider OpenAPI/JSON Schema artifact or exporter change is needed: no external route/provider consumes this internal configuration model. Add such an artifact only if a later explicitly scoped interface actually needs one.

## 7. Boundaries and later gates

<!-- @spec HTTP-AUTHORITY-CONFIG-V1-7 -->

Completion of this slice proves strict representation and pure refusal/readback behavior only. It does not implement tenant mode writes, authority CAS, lifecycle adoption, conflicting-writer enforcement, pre-I/O generation capture, provider ordering, secrets, role migration or activation.

The already-ruled later contracts still require exclusive HTTP ownership of adopted shared principal status, stable canonical UUID/no silent kind retag, and atomic status/session/version/generation/audit handling. An HTTP-owned disabled/revoked transition permanently revokes existing console sessions, including old cookies when adopting an already-inactive account. Reactivation requires a new login. Legacy/local mode remains unchanged. Stored status blocks console only after an HTTP status commit; this shape creates neither a console-purpose lookup nor a push path or new T1 field.

Unresolved remains independently established human plus exact explicit raw-role match; null alone refuses. Caller qualification can precede approver consumers on an owned disposable candidate with approvals explicitly deny/unsupported, preserving existing installed approvals. HTTP activation still requires the applicable explicit legacy-team migration with retained caller/approval-route narrowing, current API-owned authorized enqueue and coordinated provenance/current-authority gates. Direct reverse transition is refused in v1; a reverse-migration feature requires a separate contract. These are later runtime gates, not new input fields or questions required to finish this pure shape.

## Differences from existing contracts

| Existing surface | Deliberate v1 relationship/difference |
| --- | --- |
| `TenantIdentityProvider` and `DirectoryConfigOut` | Same authority version/scope ownership; no second row or generation. Tagged mode selection is new internal representation, not a storage migration. |
| `DirectoryConfigIn` | Reuse local issuer ceiling, domain/managed_by validation; no write request added. Existing expected_version/create-CAS behavior unchanged. |
| `DirectoryConfigOut` | Retain names tenant_id, issuer, governed_domains, managed_by, version; issuer/local fields move into the selected mode. schema_version/selection/mode are new. |
| T1 request | Config issuer maps later to expected_issuer; config version maps to authority_version. No sender override or second canonical identifier. |
| T1 freshness inputs | Reuse exact names and ranges with explicit values; no new defaults. |
| Forge configuration | Registry id+generation replaces caller-selected file-path metadata for this surface. No loader or registry implementation is borrowed or added. |
| Provider reference `POST /lookup` | New lookup_url is an exact configured operation URL, not an inferred reference path. No provider artifact changes. |

## Concise criterion/test matrix

| Criterion | Required pure test assertions |
| --- | --- |
| 1 | Every mode validates only its fields; mixed/unknown/null/missing fields refuse; strict UUID/int/string types; local domain ordering and existing grammar preserved; exact issuer/scope case retained. |
| 2 | schema_version differs from version and credential generation; version>=1 only; absent configuration stays absent without selecting mode; representing existing local fields does not allocate/increment state. |
| 3 | HTTPS exact URL accepted; relative/HTTP/userinfo/query/fragment/control/backslash/invalid-port forms refuse; accepted configured path is not appended or normalized; no network dependency. |
| 4 | Registry boundary lengths 1/64 pass and 0/65 fail; permitted ASCII grammar and exact case; positive generation only; path/URL/inline-secret extras refuse; parse does not read a file/registry or claim existence. |
| 5 | Independent positive identity/membership ages and nonnegative skew; missing/zero/negative/coerced values refuse as applicable; no default fills an omitted member. |
| 6 | Unsupported actual-integer schema version gets exact code; noninteger/missing version and nested failures get exact invalid code; distinctive secret/URL/path markers never appear in refusal str/repr/output; valid readback has exactly metadata shape and no readiness claims. |
| 7 | Pure tests/imports require no app, DB, provider, registry, loader or runtime initialization; existing directory/T1 contracts unchanged; no new runtime API/provider artifact. |

## Existing source contracts

- `apps/api/src/curie_api/models.py` (`TenantIdentityProvider`): existing tenant_identity_providers scope/version/identity_epoch.
- `apps/api/src/curie_api/schemas/directory.py` (`DirectoryConfigIn`, `DirectoryConfigOut`, `DirectoryCredentialIn`): DirectoryConfigIn, DirectoryConfigOut, credential config_version naming and exact existing bounds.
- `apps/api/src/curie_api/directory_service.py` (`update_configuration`, `authenticate`): existing configuration expected_version comparison, create/update version behavior and credential invalidation checks.
- `docs/superpowers/specs/2026-10-23-http-identity-facts-v1.md`: request scope/authority_version, strict UUID/issuer semantics, independent explicit freshness inputs and later-slice gates.
- `apps/api/src/curie_api/schemas/http_identity.py` (`_uuid`, `_issuer`): exact issuer/UUID validation behavior; reuse rather than invent normalization.
- `apps/api/src/curie_api/forges/loader.py` (`load_forges_json`): protected token-file/no-inline-secret precedent only; this contract adds no loader.
- `apps/api/src/curie_api/hook_signing.py` (`derive` and module rationale): database secret-storage limitation and derived hook-secret mechanism; not reused for the independent HTTP credential.

Completion requires the pure criterion tests and relevant static/documentation checks, followed by required exact-head CI. No running provider, database or installation is required for this representation. This does not establish runtime authority ownership, provider qualification or installation activation.

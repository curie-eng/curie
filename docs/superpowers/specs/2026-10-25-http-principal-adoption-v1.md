# HTTP principal adoption persistence v1

This contract realizes [Accepted ADR 0246](../../adr/0246-common-admission-resolves-an-optional-http-identity-before-authorized-enqueue.md) for API-owned authority persistence and canonical principal lifecycle. It reuses the exact models and version ownership in [HTTP authority configuration representation v1](2026-10-23-http-authority-config-v1.md) and the validation contract in [HTTP identity facts v1](2026-10-23-http-identity-facts-v1.md).

## 1. Scope and trust boundary

<!-- @spec HTTP-PRINCIPAL-ADOPTION-V1-1 -->

Implement internal API-owned authority persistence, explicit adoption, authority capture, canonical lifecycle application, and conflicting-writer/session safeguards. No administration/activation route, settings loader, registry implementation, HTTP request, role filter, team migration, enqueue, approval consumer, worker or deployment is added.

The new functions are trusted in-process transaction primitives, like `directory_sync`: calling them is not authentication. Production routes must not call them in this slice. Tests supply synthetic T1 response bytes; accepting such bytes in a unit/integration test is not evidence of provider authenticity or a bound credential. There is no `ready`, `enabled`, `qualified` or `active_consumer` flag.

Persisting HTTP ownership is a real lifecycle mutation, including console effects. It is not channel activation and must not be inferred from parsing `AuthorityConfigV1`. Qualification is limited to an isolated database whose participating writers all run this candidate. Mixed old binaries, arbitrary SQL and existing-installation rollout are unsupported here.

## 2. One existing authority owner; precise storage

<!-- @spec HTTP-PRINCIPAL-ADOPTION-V1-2 -->

Extend `TenantIdentityProvider`, keyed by its existing `tenant_id`, with:

| Field | Storage and meaning |
| --- | --- |
| `http_selection` | Nullable JSONB. SQL NULL means existing local projection behavior. Otherwise exactly the validated `HttpSelectionV1` object, including `mode`, issuer, lookup_url, credential_ref and the three freshness inputs. No other union alternative is stored here. |
| `lifecycle_generation` | Non-null bigint, default 0 for existing rows, CHECK >=0. Core-owned scope invalidation sequence, separate from configuration `version`, directory `identity_epoch` and principal `authorization_version`. HTTP-owned rows require >=1. |

`TenantIdentityProvider.version` remains the sole authority configuration version. Reconstruct `AuthorityConfigV1(schema_version=1, tenant_id=str(row.tenant_id), version=row.version, selection=...)` using the existing class. Do not store a second config version or duplicate configuration model. For HTTP rows, validate the persisted selection and require its issuer to equal the existing `row.issuer` exactly. Corrupt persisted configuration refuses with `invalid_authority_state`; it must never fall back to local behavior.

Retain existing issuer, governed_domains and managed_by on adoption of a local row. To support the existing expected-version-0 create convention without inventing a local projection manager, permit SQL NULL in governed_domains/managed_by for a newly created HTTP row only. A local row (`http_selection IS NULL`) must still have both fields non-null. A fresh HTTP row has its exact selected issuer, NULL local fields, identity_epoch 0, version 1 and lifecycle_generation 1. Projection-authority reads (`directory_service.configuration`, authentication and directory readback) require a local row. The distinct retained connection-metadata reads in criterion 10 may inspect an adopted row only after checking that both retained fields are non-null and its exact issuer and successful-snapshot predicates hold; they grant no projection authority. Fresh HTTP rows return None from that connection-metadata path before either nullable field is interpreted. No string sentinel or fictitious directory manager is inserted. The storage check permits preserved local fields on an adopted row. Existing local values and readback remain unchanged.

For absence of the authority row, internal expected version and expected lifecycle generation are both 0; `AuthorityConfigV1(version=0)` is never constructed. Readback for absence returns None. A local row can be projected using the existing `LocalProjectionSelectionV1`. Explicit disabled remains representable in the pure contract, but this slice adds no disabled persistence transition. It also adds no general setter for all union modes.

No ownership column is added to every Principal. HTTP ownership of existing and future principals is defined by the single adopted tenant/exact-issuer scope. Other/blank-issuer rows are neither retagged nor silently adopted. Unknown/foreign identities cannot be used to bypass the selected tenant's HTTP-owned OIDC creation rule.

## 3. Internal service interfaces and transaction ownership

<!-- @spec HTTP-PRINCIPAL-ADOPTION-V1-3 -->

Realizing module: a new `principal_authority` module in the existing API package. Reuse the existing config/T1 types; the following are internal Python interfaces, not new HTTP or provider schemas:

```python
read_authority(session, *, tenant_id) -> AuthorityConfigV1 | None

adopt_http_authority(session, *, tenant_id, expected_version,
                     expected_lifecycle_generation, selection: HttpSelectionV1)
    -> AuthorityWriteResult

replace_http_configuration(session, *, tenant_id, expected_version,
                           expected_lifecycle_generation, selection: HttpSelectionV1)
    -> AuthorityWriteResult

capture_http_authority(sessionmaker, *, tenant_id) -> AuthorityCapture

apply_http_principal(session, *, capture: AuthorityCapture,
                     request: LookupRequestV1, response_bytes: bytes,
                     clock: Callable[[], datetime] = utc_now)
    -> PrincipalApplication
```

`AuthorityWriteResult` contains only `configuration: AuthorityConfigV1`, `lifecycle_generation: int`, and `revoked_session_count: int`. `AuthorityCapture` is an immutable, detached, revalidated value containing `configuration: AuthorityConfigV1` and `lifecycle_generation: int`; it contains no session, ORM object, lock, secret or provider-readiness assertion. `PrincipalApplication` contains `outcome` (`resolved`, `inactive`, `unresolved`), nullable principal_id and authorization_version, and resulting lifecycle_generation. It contains no permission verdict. An unresolved result has both principal fields None.

Mutation functions require an existing caller-owned transaction, use an operation savepoint, flush but never commit, and leave the outer transaction open on success. A refusal rolls back the entire operation savepoint before raising `PrincipalAuthorityRefusal(code)`; a caller that catches it and commits must not persist partial state or audit. A later outer rollback also rolls back everything. Do not return tokens or successful effects before their owning transaction commits.

`capture_http_authority` owns a new short session/read transaction, copies a coherent snapshot while holding the scope lock, then closes it before returning. It cannot be called with a session already used by the later mutation. No service function performs provider I/O. Future HTTP composition must call capture, receive the detached value, perform I/O with no database transaction/fence, then begin its short mutation transaction.

Expected versions are actual integers >=0, excluding bool. Wrong types/negative values refuse `invalid_authority_input`. Selection inputs are revalidated through the existing pure model, including model_construct/nested mutation attempts. No raw framework exception or candidate value escapes this boundary.

## 4. Total lock order and participation

<!-- @spec HTTP-PRINCIPAL-ADOPTION-V1-4 -->

The order for participating current writers is:

1. Existing global identity mutation fence, **if the outer caller already requires it**.
2. Tenant row FOR UPDATE (tenant UUID order if a future operation has several).
3. Its TenantIdentityProvider row FOR UPDATE, if present.
4. Team rows in UUID order, then connection-token rows in UUID order, then namespace rows in UUID order, when that operation needs them.
5. Principal rows in UUID order, then principal-bound ConsoleSession rows in UUID order.

The tenant row is the existing-row scope lock even when authority/principal rows are absent. An absent principal alone cannot supply the serialization boundary. All participants acquire the tenant lock before their first authority/canonical/connection/session write lock. Discovery queries may read identifiers without a lock; after acquiring scope, reload and revalidate before locking/mutating the discovered row. Do not trust the discovery object afterward.

The new persistence primitives acquire **no global identity fence** themselves. They must never acquire it after a scope/data lock. Existing directory/identity middleware keeps its global fence outside the handler; its existing outer global transaction may remain while the handler acquires scope/data locks. Console callbacks acquire scope/data locks only. This removes the global-after-scope cycle. Future exposed composition that needs legacy cache invalidation must acquire its existing global fence first, before invoking these primitives; this slice makes no new admission-cache invalidation promise.

Reorder `live_token(lock=True)` callers that currently lock a connection token before discovering/locking its tenant. Peek at the token's tenant without a write lock, acquire scope, then lock/revalidate the token. Equivalent scope-first handling is required before the existing resolver, confirmation and verified-email write paths take namespace/principal locks. Preserve team-before-namespace-before-principal ordering used by directory ingestion.

A scope read/capture may use the same exclusive tenant lock for simplicity; release it at transaction end. No tenant lock, global fence or transaction spans OIDC exchange or future HTTP lookup. OIDC login-attempt consumption keeps its existing short committed transaction before external exchange.

## 5. Explicit adoption and metadata-only replacement

<!-- @spec HTTP-PRINCIPAL-ADOPTION-V1-5 -->

`adopt_http_authority` under the lock order above:

1. Requires an existing active tenant; otherwise `tenant_unavailable`.
2. Compares expected_version with row.version or 0, then expected_lifecycle_generation with row.lifecycle_generation or 0. Mismatches give `stale_authority_version` or `stale_lifecycle_generation`.
3. Refuses an already HTTP-owned row with `authority_already_adopted`. For an existing local row, selection.issuer must equal row.issuer; otherwise `authority_scope_mismatch`. It never silently switches issuer.
4. Stores HttpSelectionV1 and sets configuration version to old+1 (or 1), lifecycle_generation to old+1 (or 1). Existing local fields and identity_epoch are unchanged; absent-row fields follow criterion 2.
5. Locks existing principals of exactly the tenant/issuer in UUID order. It leaves their UUID, subject, type, status and authorization_version unchanged. For each disabled/revoked principal, stamps `revoked_at` on every previously unrevoked ConsoleSession with that principal_id, including expired/unexchanged rows. Never clears or overwrites an existing revocation timestamp.
6. Records `identity.http_authority_adopted` in the same transaction, with authority version, lifecycle generation and revoked-session count only. No selection, secret reference, endpoint, display value or response bytes are placed in audit details.

Adoption of zero subjects is valid and protects future first-create. Adoption of existing inactive subjects revokes their cookies even though principal status/version is unchanged. Adoption creates no IdentityLink, membership, role migration or principal solely because configuration was saved.

`replace_http_configuration` requires an already HTTP-owned active-tenant scope, exact expected version/generation, and the same issuer. It stores the supplied HttpSelectionV1 and increments configuration version and lifecycle generation even for an equal selection, with `identity.http_authority_changed` audit containing only those numbers. It does not write principal status or sessions. Changing issuer is refused with `authority_scope_mismatch`; an issuer/reverse migration requires a separate contract. A local/disabled selection cannot enter this typed operation. Existing directory configuration updates must refuse `authority_owned_by_http` rather than reset the adopted row.

Both operations store metadata only. They never check that credential_ref resolves to bytes, that it belongs to this tenant/issuer/endpoint, or that the endpoint can be reached. Their success must not be described as usable runtime configuration or credential rotation qualification.

## 6. Capture and authority-only CAS

<!-- @spec HTTP-PRINCIPAL-ADOPTION-V1-6 -->

Capture requires an active tenant and a valid HTTP-owned row, or refuses `tenant_unavailable`, `http_authority_required` or `invalid_authority_state`. It returns the exact persisted config/version and current lifecycle_generation after closing its session.

At `apply_http_principal`, after scope locking, revalidate capture and request models, reload the tenant/configuration, and compare:

- active tenant, exact captured tenant/issuer and HTTP ownership;
- exact configuration version **and complete HttpSelectionV1 equality**, including endpoint, credential reference/generation and freshness inputs;
- exact captured lifecycle_generation;
- request.authority_version equals captured config.version and request.scope.tenant_id/expected_issuer equal captured scope.

Configuration mismatch gives `stale_authority_version`, scope mismatch `authority_scope_mismatch`, lifecycle mismatch `stale_lifecycle_generation`. The exact selection comparison defends against malformed/unversioned internal state changes; it does not make old binaries supported. The canonical subject remains unknown until T1 validation succeeds.

Call existing `validate_lookup(response_bytes, request, FreshnessLimits(...captured fields...), now=now)` **inside the short commit transaction after acquiring locks**. Reuse its exact bound/parse/echo/freshness semantics, including the equality boundary; do not trust precomputed expiry or handwritten ValidatedFacts. Map its refusal code to `identity_facts_<T1 code>`. This is local validation, not provider I/O. Wrong purpose/request_id/sender scope echoes are caught by T1.

The service calls `clock()` only after all scope and relevant canonical row locks have been acquired, immediately before T1 validation and writes. Its result is an aware UTC datetime; an invalid clock value refuses invalid_authority_input. `utc_now` returns the actual UTC time. Tests inject a callable whose value can advance while the function waits for locks. Never freeze a timestamp before a lock wait and call it commit-time validation. Since the subject is needed to select a canonical lock, a preliminary pure T1 parse/validation may identify it; the service repeats `validate_lookup` with a fresh post-lock clock value before any writes. The second validation is authoritative.

This capture deliberately contains **no binding generation, installation/namespace state, policy version or agent authorization token**. Those are operation-specific values not yet implemented by this slice. A later caller/approval composition must capture and revalidate them itself, keep its effect and applicable lifecycle writes in the same short outer transaction, and roll it back on operation-context conflict. Returning PrincipalApplication is never an enqueue permit.

## 7. Canonical result application and generation policy

<!-- @spec HTTP-PRINCIPAL-ADOPTION-V1-7 -->

For a validated unresolved result, return the unresolved shape without creating/updating any Principal, session, generation or audit. The later raw-account policy must independently establish human kind plus exact raw match; this service neither establishes that permission nor interprets a null filter.

For resolved/inactive, require the exact canonical issuer already enforced by T1 and the adopted scope. Persistable kinds are the table's existing human/service values; agent refuses `unsupported_principal_kind` before any principal/session/audit write. Persisting a service identity grants no human session, invocation or approval capability.

Find/create by exact `(tenant_id, issuer, subject)` while holding the tenant scope lock. Never merge by email, use a provider-supplied UUID, create an identity link or infer a subject. Existing kind mismatch refuses `principal_kind_conflict`; never retag. Preserve existing display/email/last_seen fields because T1 supplies none of them.

For first creation use a new core UUID, given kind/status, authorization_version 1. For an existing principal, change status to the validated status and increment authorization_version exactly once iff status changes. A same-status observation preserves authorization_version.

Every successful resolved/inactive application advances scope lifecycle_generation exactly once, including a same-status observation. This is a core commit sequence, not a provider revision. Thus two lookups captured at generation G cannot both apply: after one successful application the other's G is stale even if the first observed no status change. Unresolved and refusals do not advance it.

Record `identity.http_principal_observed` in the same transaction, with principal_id in its structured audit column and details limited to status, authorization_version, lifecycle_generation and revoked-session count. No positive identity/membership cache, response copy or source-order token is stored. Group facts are not persisted by this slice.

## 8. Atomic inactive revocation and reactivation

<!-- @spec HTTP-PRINCIPAL-ADOPTION-V1-8 -->

A successful disabled/revoked result stamps all unrevoked sessions of that principal in the same transaction as principal creation/status/version, lifecycle_generation and audit. Repeat inactive observation also catches any inconsistent unrevoked row without restoring earlier timestamps. Active results never clear revoked_at.

Reactivation from disabled/revoked to active increments authorization_version and scope generation, preserves all session revocations, and permits only a subsequent successful login to mint a new cookie. HTTP adoption of an already-inactive principal has the same permanent-session consequence even without a status delta.

Do not confuse an operation denial with a malformed identity result: a valid inactive lifecycle observation may be committed by an authorized future composition even if invocation is denied. An operation-context conflict must instead roll back the entire short transaction. This service decides neither case and returns no allow/deny. Provider unavailable, malformed, out-of-scope or expired observations never produce partial lifecycle writes.

## 9. Every legacy writer and direct primitive

<!-- @spec HTTP-PRINCIPAL-ADOPTION-V1-9 -->

The following functions must acquire/reuse the scope guard in their own transaction, not rely only on routing/middleware:

| Writer | Required behavior |
| --- | --- |
| `directory_service.update_configuration` | HTTP-owned row refuses authority_owned_by_http. Existing local expected-version/create behavior and local audit remain; a successful local configuration write also increments new lifecycle_generation once (new row starts at 1). |
| `directory_service.authenticate` | HTTP-owned scope refuses before using retained local metadata or minting/accepting projection authority. Config-version invalidation is retained but is not the sole guard. |
| `directory_service.configuration` / directory readback | Existing directory configuration readback requires a local row; HTTP rows refuse authority_owned_by_http before serializing nullable retained-local fields. Internal `read_authority` is the typed metadata readback, not a new route. Existing local readback is unchanged. |
| `directory_service.ingest_snapshot` | Recheck current owner/config under scope lock even when passed a stale ORM config; HTTP-owned scope refuses before canonical, profile, link or membership changes. Successful local creation of any canonical principal advances lifecycle_generation once for the whole snapshot when an authority row exists; profile-only refresh does not. Existing team/identity epochs retain their semantics. |
| `directory_sync.set_account_status` | Direct calls recheck HTTP ownership under scope lock and refuse it, even with a synthetically constructed DirectoryScope/current expected authorization_version. Local writes keep existing authorization_version behavior and advance lifecycle_generation once when an authority row exists. |
| `directory_sync.locked_team`, `apply_snapshot`, `record_failure` and directory team provisioning | Scope guard must precede the first team lock, including the direct locked_team helper; a direct projection write cannot bypass an adopted owner using a new DirectoryScope. Local semantics remain. These methods do not become HTTP group-storage endpoints. |
| OIDC `crud.console.resolve_principal` | Scope-first participation and adopted behavior in criterion 10; local first creation advances new lifecycle_generation if a config row exists. An absent config remains absent; do not create one just to count local OIDC writes. |

The scope lock serializes pre-adoption local first-create/status and adoption in both orders. A local writer that commits first is included in the adoption's resulting existing state; adoption that commits first makes the writer refuse. A lookup begun after adoption captures its new generation; old configuration credentials alone never restore the displaced owner.

Do not alter principal status/version semantics for local snapshots or introduce permanent-session revocation into local directory status changes. New internal generation bookkeeping must not change existing response schemas, config versions, team epochs or identity epochs beyond their existing operations.

## 10. OIDC and existing connection participation

<!-- @spec HTTP-PRINCIPAL-ADOPTION-V1-10 -->

For a tenant with an adopted HTTP authority, `resolve_principal` requires claims.issuer to equal its selected issuer and an already existing matching human Principal. Unknown subject refuses `http_principal_not_provisioned`; mismatched issuer refuses `authority_scope_mismatch`; service match refuses `principal_kind_conflict`. It never first-creates, changes status or changes kind in HTTP mode. It may refresh compatible human email/display/last_seen under the scope lock. Existing login active checks still decide whether the refresh transaction commits.

For an unadopted tenant, preserve existing OIDC first-create/profile behavior. Human-kind checks introduced for adopted session issuance do not silently expand into a separate legacy policy change.

Both ordinary console and account-connection callback branches use this guarded resolver. They cannot create a principal before checking mode. The callback catches its bounded ownership/mint refusals, rolls back, uses existing `_refuse_callback` (indistinguishable 401), and clears connection cookies on the connection branch as it already does for terminal connection failures; no uncaught exception becomes a 500. Existing connection-token/browser-proof/confirmation/verified-email paths participate in scope-first order and reload principal active status/kind before writes; an old proof cannot mint or reactivate a principal or revive a revoked console session.

This slice does not create an HTTP account-linking workflow or indiscriminately delete existing links/tokens. Existing projection connection logic and its proofs may continue for retained compatible local metadata, subject to its original policy and the guarded adopted human/status check. `identity.connect.configured_provider` is the retained connection-metadata reader, distinct from the local-authority guard in `directory_service.configuration`; it must not call that guard for an adopted row. For an adopted row it requires an active tenant, valid HTTP selection whose issuer exactly equals row.issuer, non-null governed_domains and managed_by, and the existing successful-snapshot predicate: a same-tenant idp_group Team with matching managed_by, epoch >0 and non-null last_synced_at. It returns None before interpreting either nullable field when retained metadata is absent, and returns None when the existing snapshot predicate fails.

Connection consumers (`live_token`, `preserve_verified_claims`, `confirm` and `link_verified_email`) keep the exact token/claims/principal issuer comparisons against this selected issuer. Before proof/link mutations involving a principal, they must reload and require an adopted human with active principal and tenant under criterion 4's lock order. Sender-token issuance still has only its existing authenticated-sender evidence; metadata alone cannot provision a human or bypass later proof checks. The existing unadopted connection path remains unchanged. Authority ownership does not turn frozen retained projection metadata into HTTP membership, directory-writer authority or caller permission. These existing links are not consumed as the HTTP identity source, and no new HTTP purpose is introduced.

## 11. Every session mint path and monotone revocation

<!-- @spec HTTP-PRINCIPAL-ADOPTION-V1-11 -->

`create_principal_console_session` must reload the principal under scope/principal locks immediately before inserting the session. For an adopted tenant it requires exact issuer, human kind, active principal and active tenant; otherwise refuse without session/profile partial commit. Inactive tenant refuses tenant_unavailable; a missing principal refuses http_principal_not_provisioned; wrong issuer refuses authority_scope_mismatch; nonhuman kind refuses principal_kind_conflict; an existing human whose stored status is disabled or revoked refuses principal_inactive. Apply these checks in that order. Never rely on a stale caller-supplied Principal.status. Keep locks through commit. It may retain its existing outer commit wrapper; any shared inner helper must be flush-only.

For a direct mint call, principal_inactive raises `PrincipalAuthorityRefusal("principal_inactive")` without inserting a session, returning a token or committing. The OIDC callback rolls back its entire resolver/mint transaction and maps that refusal to existing `_refuse_callback`, the same indistinguishable 401 as its other login refusals. If a principal-bound login-code exchange encounters principal_inactive, it returns None without consuming/changing the exchange row or committing; its existing indistinguishable exchange response is preserved. Test all three paths with a stored disabled/revoked principal and a stale active ORM object. Tenant inactivity retains tenant_unavailable and follows the same wrapper mappings.

The OIDC resolver and mint run within one transaction after OIDC I/O. Direct mint callers receive the same safeguards. Mint-before-inactive-commit produces a cookie the inactive transaction permanently revokes. Inactive-commit-before-mint refuses mint. Reactivation-before-a-new-successful-login permits a newly issued cookie; old revoked rows remain revoked.

Inventory and guard `exchange_console_login_code` too: normal legacy rows with principal_id None keep their existing subject/session contract. If a principal-bound row reaches exchange, discover identifiers without a session-row write lock, acquire scope/principal/session locks in order, and apply the same adopted active-human guard before minting. A random consumed login-code field on normal OIDC rows remains unusable. This prevents a second principal-bound mint path from escaping the contract.

`revoke_console_session` is monotone removal, not a mint/lifecycle owner: it may retain its existing single-session update without acquiring scope afterward and must never clear revoked_at. Its existing explicit revocation timestamp behavior is unchanged; the new HTTP bulk revoker only stamps rows whose revoked_at is NULL. Subject-only legacy console sessions (principal_id None) are outside HTTP principal adoption and are not bulk-revoked. Reads continue to enforce stored principal/tenant activity; no new provider read is added.

## 12. Credential and consumer prerequisite; exact limits

<!-- @spec HTTP-PRINCIPAL-ADOPTION-V1-12 -->

A separate credential-binding realization must qualify registry authorization for exact tenant/issuer/endpoint and immutable secret bytes for `(id,generation)`, including missing/unreadable/changed bytes and cross-replica/restart behavior. That realization must bind one credential byte snapshot to the captured authority and must not reload mutable bytes at send time. This contract implements none of it. Metadata-only replacement invalidates an old authority capture but cannot prove a secret was rotated, read or used. No key-management infrastructure or checksum field is added here.

Production HTTP consumption is blocked until that binding and transport composition exist. Later caller activation additionally requires role/team migration, API-owned enqueue and supported owner-provided provenance/current-authority integration. No operation-specific policy/binding version is invented in this persistence slice; #1921 remains outside scope.

HTTP owns **stored** shared status only after successful lifecycle commit. Console request/session checks discover no new provider suspension by themselves. T1 keeps invocation/approval purposes only. No console/login purpose, push invalidation or immediate directory-suspension promise is introduced.

The core generation rejects overlapping stale work. A later request can still receive older provider backing data that meets T1 freshness and apply it. No source-monotonic guarantee is made, and `as_of` is not used as a revision.

v1 refuses direct HTTP-to-local/disabled and issuer replacement. Mixed-old-binary exclusion is a separate database/upgrade-owner decision: schema serving currently accepts unknown newer revisions, and new application guards cannot police old code. This does not authorize a trigger, capability framework, release bump or change to ADR 0142. Controlled all-new disposable qualification must record its exclusive writer artifacts; it is not existing-installation rollout support.

## 13. Refusal and evidence contract

<!-- @spec HTTP-PRINCIPAL-ADOPTION-V1-13 -->

The internal refusal carries one of: invalid_authority_input, invalid_authority_state, tenant_unavailable, http_authority_required, authority_already_adopted, stale_authority_version, stale_lifecycle_generation, authority_scope_mismatch, authority_owned_by_http, http_principal_not_provisioned, principal_kind_conflict, principal_inactive, unsupported_principal_kind, or identity_facts_ followed by an existing T1 refusal code. Directory writers/readback translate authority_owned_by_http into existing `DirectoryConflict` with that exact code, using their existing 409 mapping. Existing authenticated credential verification may retain its intentionally indistinguishable authentication refusal. Direct adopted-principal mint raises the bounded internal refusal; its OIDC callback wrapper rolls back and returns the existing indistinguishable 401; principal-bound login-code exchange returns None on its adopted-principal refusal without consumption or commit. principal_inactive means an existing adopted human's stored status is disabled/revoked, never a missing identity, wrong kind or inactive tenant. No provider/config/secret value is included.

No route/status-code policy is added. Existing local refusal/status responses remain unchanged; new HTTP-owned refusals stay bounded. Audit is transaction-coupled and contains only the fields specified above. Test failure evidence uses synthetic subjects/issuers; no real installation data is needed.

## Criterion/test matrix

| Criterion | Exact required assertions |
| --- | --- |
| 1 | Import/runtime-call inventory proves no route/transport/worker consumer; synthetic bytes alone are labeled persistence evidence, not provider authenticity. No ready/enabled claim. |
| 2 | Existing local row migration preserves all values and mode; no-row read returns None; absent-row HTTP adoption uses no fabricated local manager; illegal local-null/HTTP issuer mismatch/corrupt JSON refuse; pure config shape and version remain exact. Adopted-row projection-authority reads refuse while eligible retained connection-metadata reads remain available; fresh HTTP connection-metadata reads return None before nullable-field use. |
| 3 | Returned capture has no live transaction/ORM/secret; mutation success flushes without commit; caught refusal plus outer commit leaves no partial row/session/audit; outer rollback erases success. Strict expected versions and constructed-model attacks refuse. |
| 4 | Barrier-controlled real-Postgres tests run legacy global-fence→scope and new scope-only operations in both orders without a lock cycle. Token discovery/reload does not invert scope order. No global acquire after scope, and no transaction/fence remains after capture. |
| 5 | Absent and existing-local adoption; stale config/generation; foreign issuer; duplicate adoption; already-inactive cookie revocation with unchanged authorization_version; retained local metadata and no team/link writes; no-op replacement increments both config/generation. |
| 6 | Capture then endpoint/ref/generation/freshness/config/owner/tenant/lifecycle change causes refusal before writes; request authority/scope mismatch; T1 wrong echoed sender/request/purpose; fake clock advanced during lock wait proves post-lock freshness, equality and membership expiry. |
| 7 | Human/service create and matching update; agent unsupported; human/service kind conflict never retags; UUID stable; same status preserves authorization_version but advances scope generation; two same-generation results allow only one commit; unresolved writes nothing. |
| 8 | Inactive creation/update/repeat and already-inactive adoption permanently revoke all unrevoked principal sessions atomically; injected audit/session failure rolls everything back; reactivation never revives old cookies. |
| 9 | Every listed direct primitive refuses after HTTP adoption, including stale config object/current synthetic DirectoryScope/platform-key path; both adoption/local-create and adoption/status commit orders; local response/epoch/version/session behavior unchanged. |
| 10 | Unknown HTTP OIDC subject refuses on ordinary and connect callbacks; wrong issuer/service row refuses; existing active human profile refresh succeeds; inactive rolls back; connection stale-proof race obeys scope/principal locking. Paired tests prove retained adopted metadata with exact issuer/successful snapshot permits the existing proven active-human connection path, while fresh HTTP/null metadata, absent snapshot or issuer mismatch refuses; no new link workflow or authority from retained metadata. |
| 11 | Both mint/inactive commit orders; stored disabled/revoked plus stale active ORM object gives exact principal_inactive for direct mint, callback rollback plus indistinguishable 401, and principal-bound exchange None with no consumption/commit. Inactive tenant retains tenant_unavailable. Earlier revoked_at preserved; subject-only legacy session unchanged; reactivation requires newly successful login. |
| 12 | Source/import checks confirm no secret loader/readiness claim; metadata replacement invalidates capture but proves no byte binding; sequential older-but-T1-fresh response demonstrates the explicit ordering limit; no HTTP calls on console reads. |
| 13 | Bounded code-only failures never expose synthetic endpoint/ref/path/secret markers; principal_inactive is distinct from missing/kind/tenant failures and follows all three criterion-11 mappings; all audit rows roll back on failure; no changed legacy local response schema. |

Use focused PostgreSQL race/transaction tests with deterministic barriers; mocks do not prove locking. A code scan verifies every production Principal insert/status writer and principal-bound session mint is in the participation matrix; adding a writer requires adding its guard/test. Candidate schema graph/window metadata may move under the existing development contract, but published release windows and Cargo/Chart versions must remain unchanged.

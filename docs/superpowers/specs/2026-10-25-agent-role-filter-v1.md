# Pure agent role filters and migration representation v1

## 1. Scope and existing authority

<!-- @spec AGENT-ROLE-FILTER-V1-1 -->
Realize the representations authorized by [Accepted ADR 0246](../../adr/0246-common-admission-resolves-an-optional-http-identity-before-authorized-enqueue.md), section 3, under `AgentAccessPolicy`'s existing `(tenant_id, agent_id)` scope and `version` ownership. Reuse [facts v1](2026-10-23-http-identity-facts-v1.md) and [authority-config v1](2026-10-23-http-authority-config-v1.md), section 7.

No persistence/migration execution, evaluator, database access, API route, authority activation, HTTP I/O, enqueue, approval consumer, worker, settings or dependency change. No runtime permission result or readiness claim is introduced.

The existing `apps/api/src/curie_api/schemas/agent_access.py`, `apps/api/src/curie_api/agent_access.py` and `apps/api/src/curie_api/models.py::AgentAccessPolicy` remain the ownership reference. The service's “nothing consults” text is stale: `team_authorization.check_binding` already reads this policy. Do not use that text to justify bypassing it.

## 2. Exact selectors

<!-- @spec AGENT-ROLE-FILTER-V1-2 -->
use this discriminated union, with all displayed fields required and all others forbidden:
```text
SurfaceAccountSelectorV1 = {
  kind: "surface_account", provider: Opaque,
  namespace: NamespaceV1, native_id: Opaque
}
IssuerSubjectSelectorV1 = {kind: "issuer_subject", issuer: Issuer, subject: Opaque}
IssuerGroupSelectorV1 = {kind: "issuer_group", group: GroupV1}
RoleFilterV1 = {entries: [SelectorV1, ...]}
```
Import `CanonicalUUID`, `Issuer`, `Opaque`, `NamespaceV1`, `GroupV1` and exact string semantics from `apps/api/src/curie_api/schemas/http_identity.py`; do not copy their grammars, normalize strings or introduce local principal IDs, profile email or display-name inference. Namespace authority may be empty. Lists remain ordered in readback although matching order conveys no preference.

A raw selector's durable equality key is `(tenant_id, provider, namespace.id, namespace.authority, namespace.kind, namespace.key, native_id)`. Tenant comes from the enclosing policy, never from an entry. Every component compares exactly. This is a projection of authenticated `SenderV1`, deliberately excluding its receiving `channel_identity_id` and `provider_installation_id`, and excluding channel/binding IDs, generation, credential IDs and message fields.

Rationale: ADR 0198 decisions 2, 5 and 9 make the namespace tenant-owned and the native account independent of receiving bots/connections. The namespace UUID binds the grant to that row's lifetime; its descriptor prevents substitution of a different namespace tuple. A recreated row with the same natural tuple has a different UUID and requires a new explicit grant. A different receiving installation or bot can report the same authenticated account without changing that grant. Its own active/connected/scope checks still apply at ingress. ADR 0201's reviewed sender derivation is not replaced by parser acceptance.

Pure validation establishes shape, not that a namespace exists, is active, belongs to the tenant/provider or was authenticated. Those are later current-scope checks. Raw selectors never encode a receiving-channel restriction accidentally; existing binding/route restrictions remain separate narrowing.

## 3. Absent, null, empty and role independence

<!-- @spec AGENT-ROLE-FILTER-V1-3 -->
represent policy metadata and independently versioned role configuration as:
```text
AgentRolePolicyV1 = {
  schema_version: 1, tenant_id: CanonicalUUID, agent_id: CanonicalUUID,
  version: integer 1..2,147,483,647, roles: AgentRolesV1
}
AgentRolesV1 = {invocation?: RoleConfigurationV1, approver?: RoleConfigurationV1}
RoleConfigurationV1 =
  {schema_version: 1, legacy_state: "retained", filter: RoleFilterV1 | null}
  | {schema_version: 1, legacy_state: "migrated", filter: RoleFilterV1 | null,
     raw_account_revocation: "separate_removal_required"}
```
The `approver` spelling reuses the existing role/team owner; T1 lookup purpose remains `approval`. Neither naming convention is changed.

Missing role key means no new configuration for that role and retains current behavior; serialize absence by omission. A role key with value null is invalid, preventing confusion with an explicit configured `filter: null`. Missing `filter` is invalid. Configured null is unrestricted only at this filter layer; `{entries: []}` matches nobody; populated entries use any-match within that one role. Invocation never supplies approval membership or vice versa.

Missing entire new configuration remains absence outside these models. A policy row can already exist with `roles: {}` at its current version. No absent policy becomes an artificial version-0 snapshot or a synthetic unrestricted role. These objects represent the new configuration portion, not a replacement for all existing policy/readback fields.

The migrated union's fixed `raw_account_revocation` text is explicit disclosure, not a permission verdict or user-selectable bypass switch. Retained roles reject that field; migrated roles require it even when their current filter has no raw entry, so subsequent raw entries cannot obscure the role's chosen semantics.

## 4. Explicit migration intent and old clients

<!-- @spec AGENT-ROLE-FILTER-V1-4 -->
publish one-role intent, not a whole-policy replacement:
```text
AgentRoleChangeV1 = {
  schema_version: 1, tenant_id: CanonicalUUID, agent_id: CanonicalUUID,
  expected_version: integer 0..2,147,483,647, role: "invocation" | "approver",
  operation: "set_filter" | "migrate_role", filter: RoleFilterV1 | null,
  raw_account_revocation: "separate_removal_required"  # migrate_role only
}
```

Make this a strict operation union: `set_filter` forbids the disclosure member; `migrate_role` requires its exact literal. No role array, whole-policy payload, legacy-team override, binding/route clearing flag, reverse-migration operation or authority-mode field is accepted.

`schema_version` versions representation only. `version`/`expected_version` reuse `AgentAccessPolicy.version` and its existing absent-row expected-version 0 convention. constrain only these new counter representations to the existing PostgreSQL `Integer` domain: snapshot `version` is 1..2,147,483,647 and intent `expected_version` is 0..2,147,483,647. Rationale: both name the same stored `AgentAccessPolicy.version`, not an unbounded new sequence; accepting impossible stored values would require unnecessary large-integer readback machinery. Existing legacy PUT schemas and facts integer grammar stay unchanged. Parsing neither reads/checks CAS nor allocates/increments a version; counter exhaustion handling belongs to the later write contract. The future successful write uses that sole existing counter, creates version 1 for absence and otherwise advances it once.

A future `set_filter` preserves the target role's existing legacy state; first configuration is retained. It cannot reset migrated to retained. `migrate_role` explicitly selects migrated state and replaces only that role's applicable legacy agent-team requirement; a role without a team still needs this express migration to choose migrated raw-grant semantics. The other role, its team/filter/migration state and all binding caller/explicit approval-route restrictions remain unchanged. Repeating migration of an already migrated role is refused by the later write contract; editing its filter uses `set_filter`.

Until express migration, legacy agent teams AND binding lists AND explicit approval-route restrictions continue to narrow the filter. New null/filter/HTTP configuration clears none of them. This slice defines no removal of binding/route narrowing, including for a migrated role. Existing legacy `allowed_callers` null/open and invalid-empty writes are unchanged.

after either role is migrated, reject an old whole-policy PUT in its entirety before any mutation, even if the submitted team IDs appear unchanged. Never apply its other-role half, clear a filter, reset state or turn it into a new intent. Before any migration, retain that PUT's existing behavior. This conservative compatibility rule avoids an old two-required-team-fields client silently rewriting state it cannot represent; actual refusal/enforcement belongs to the persistence/route slice.

## 5. Carried authorization obligations, not a new evaluator

<!-- @spec AGENT-ROLE-FILTER-V1-5 -->
For the new HTTP-consuming authorization contract, the already-ruled obligations remain: active tenant/agent, valid authenticated transport/sender scope and permitted principal kind precede role matching. Provider unavailability, malformed/stale facts or invalid scope never fall through to raw/null/legacy matching. A definitive inactive identity can match an exact raw entry only for an expressly migrated role, with all global denials and retained narrowing still applied. Retained roles gain no inactive-account exception.

On that new HTTP-consuming path, an unresolved outcome needs independently established human kind plus an explicit exact raw entry in the requested role. Null alone, a subject/group selector, a display name, profile email or merely trusting the unresolved provider's kind is insufficient. This representation contains no writable human-proof field or permission flag. Proof provenance and its runtime validation remain a named later gate.

The new HTTP-consuming contract must refuse nonhuman invocation until separately qualified; service/agent identities never resolve human-gated approvals. This pure slice changes no installed legacy behavior, including the existing no-team unrestricted path, and does not reinterpret machine-token, hook or schedule contracts. Filter parsing qualifies none of those consumers. Answering-actor authority, undo/recovery and invocation are independent consumers. No tests of these pure models can establish admission or approval enforcement.

## 6. Strict bounded parsing and intentional readback

<!-- @spec AGENT-ROLE-FILTER-V1-6 -->
expose only pure bytes boundaries `parse_agent_role_policy_v1(data: bytes)` and `parse_agent_role_change_v1(data: bytes)`. Return their corresponding validated models or `RoleFilterRefusal(code)`; never raw Pydantic/JSON exceptions. Models are strict, extra-forbidden, frozen and revalidated on internal reuse; nested collection mutations/constructed models must not bypass a readback boundary.

Accept at most 65,536 UTF-8 JSON bytes including whitespace; 65,537 refuses. Reject BOM, invalid UTF-8, unpaired surrogates, duplicate JSON object keys at every depth, nonfinite numbers, trailing non-whitespace, invalid syntax and non-JSON inputs. Integer fields require integer JSON tokens: bool, string, decimal and exponent forms (including mathematically integral `1.0` or `1e0`) refuse; strings and enum literals are exact. No new identifier grammar/length ceiling narrows imported facts types. cap entries at 128 per role, including before duplicate checking; zero is valid. The byte and entry limits are configuration bounds, not facts-response amendments.

Only codes `invalid_agent_role_configuration` and `unsupported_agent_role_configuration_version` exist. The first failing stage below determines the code, independently of JSON member insertion order:

1. Byte limit, UTF-8/JSON syntax, duplicate object keys, nonfinite constants and Unicode validity: any failure is invalid.
2. Root must be an object with an actual-integer `schema_version`: missing/noninteger is invalid; every other integer, including 0, negative or size-fitting very large tokens, is unsupported **before** checking root extras or other fields.
3. With root version 1, validate the shallow envelope: exact root required/allowed keys, scope UUIDs, bounded counter and, for policy, the roles object with only invocation/approver keys; for change, role/operation/disclosure union control fields. Present role values and filter contents are still opaque here. Envelope failure is invalid, even if a nested role version is unsupported.
4. For a policy, visit present roles in fixed order **invocation then approver**, fully validating one before the next. A nonobject role is invalid. For an object, check its actual-integer schema_version first (missing/noninteger invalid; any other integer unsupported before that role's extras/content), then its complete role/filter shape, limits, duplicates and within-filter namespace consistency. A change has no nested role version; validate its complete filter after its envelope. Ordinary content failures are invalid.
5. After both policy roles pass, check cross-role namespace descriptor consistency; contradiction is invalid. No additional version scan overrides an earlier content failure.

Thus invocation version 2 beats approver version null; an invalid invocation filter or invocation:null beats unsupported approver version; unsupported invocation version beats its own extras. Reversing JSON member order changes none of these outcomes. No refusal includes original input, field paths, validation detail, repr, identifiers or exception chain.

Reject duplicate entries by exact discriminator-specific equality: raw key from section 2, `(issuer,subject)` for subject, `(group.issuer,group.id)` for group. Do not normalize/deduplicate silently. The same selector in different roles is valid; different selector kinds remain distinct even when they might later identify one person. repeated namespace UUIDs must have identical provider/authority/kind/key descriptors throughout a policy and within a change payload's own filter, or refuse internal contradiction. Consistency against the stored other role is a later write check, never a parser database read. No cross-entry alias inference occurs.

`render_agent_role_policy_v1(policy: AgentRolePolicyV1) -> bytes | RoleFilterRefusal` revalidates and emits compact, deterministic UTF-8 JSON readback without ASCII-escaping non-ASCII text, with exactly the validated fields, preserving missing role keys, explicit null, empty entries, list order and the migrated disclosure literal. Every valid size-fitting policy must roundtrip through this renderer and parser without losing field presence or identity values; apply the same byte ceiling to emitted canonical JSON. Invalid/oversize constructed or mutated inputs return only the stable invalid code. A size-fitting 5,000-digit policy counter is invalid under section 4 and must refuse before ordinary integer serialization; no hidden digit ceiling or process-global digit-guard mutation is added. Size-fitting arbitrary integer schema-version tokens still reach the unsupported-version code through the inherited facts decoder mechanics. It does not read legacy fields, check a database, authenticate an operator or authorize logging/publication of readback values.

## 7. Export and synthetic vectors

<!-- @spec AGENT-ROLE-FILTER-V1-7 -->
publish a standalone draft-2020-12 JSON Schema at docs/interfaces/agent-access/agent-role-filter-v1.schema.json containing the policy and change model definitions. No external/runtime API OpenAPI or provider OpenAPI changes: no route or provider operation is added. Use a pure `agent_role_filter_export` module, deterministic formatting/order, no timestamps and a byte-for-byte drift test. Integrate its regeneration/drift path with existing `curie dev contracts` through `scripts/check-contracts.sh`, adding no CLI command.

Export strict required/forbidden members, discriminator constants, `maxItems: 128` and exact-object `uniqueItems`; namespace descriptor consistency remains semantic. JSON Schema integer/const semantics admit mathematically integral decimal/exponent values such as 1.0 and 1e0; integer-token strictness therefore remains a byte-parser check, with explicit schema-accept/parser-refuse cases. Retain inherited Issuer Unicode-NFKC URL checks and unpaired-surrogate rejection as semantic/Unicode checks; do not change the imported grammar to manufacture schema agreement. Set `$id` to https://curietech.ai/schemas/agent-role-filter-v1.schema.json and keep both root models available by local `$defs` reference.

Use tests/vectors/agent-role-filter-v1.json for synthetic independent valid/invalid policy and change fixtures, each naming target parser and expected stable result/code. JSON syntax/duplicate-key/UTF-8/byte-boundary cases carry exact byte construction in focused tests rather than losing their shape in decoded vectors. Exercise exported schemas against the same decoded vectors with separate expected structural-schema and parser outcomes; require agreement only for structurally expressible constraints. Name byte limits, duplicate keys, integer-token spelling, refusal-code precedence, namespace consistency, inherited Issuer Unicode-NFKC validity and unpaired-surrogate rejection as explicit exceptions, using exact-byte or decoded-semantic cases as appropriate. No dependency is added merely for schema testing; reuse the repository's existing schema-validation test machinery.

## 8. Criterion/test matrix

| Criterion | Required pure assertions and limits |
| --- | --- |
| 1 | Pure imports/parser/exporter need no app, DB, registry, provider or worker; no existing consumer wiring changes. |
| 2 | All three exact selectors roundtrip; one-at-a-time scope component differences stay distinct; receiving IDs/extras refuse; NamespaceV1/Issuer behavior reused unchanged. No claim of live-scope authentication. |
| 3 | Missing whole configuration stays absent; existing-row roles:{}; omitted role vs role:null invalid vs filter:null vs [] vs populated; role independence/order; strict nested schema versions and disclosure union. |
| 4 | Both operation variants and roles; counter endpoints 0/1/2,147,483,647 accepted where applicable and 2,147,483,648 refused; bool/negative/coercion refused; no whole-policy/reset/reverse/clearing flags; no CAS/write execution. Old-PUT rule is a future consumer test, not proven here. |
| 5 | Permission/proof/ready/activation extras refuse; retained/migrated shapes disclose the distinction. Unresolved/inactive/outage/global-denial runtime cases remain later evaluator tests. |
| 6 | Exact byte boundary; malformed/duplicates at every depth; 128/129 entries; exact duplicate vs cross-role reuse; namespace contradiction in policy and change; mixed-error precedence under swapped member order; 5,000-digit schema-version unsupported versus counter/model-readback invalid; constructed/mutated readback; distinctive synthetic markers absent from refusal str/repr/serialization. |
| 7 | Shared vectors agree for structurally expressible constraints; integral decimal/exponent, inherited Unicode-NFKC/surrogate and other semantic-only exceptions carry separate expected outcomes; drift bytes stable; existing provider/API artifacts unchanged. |

## 9. Deferred gates

Persistence must later specify storage, CAS/concurrent writers, the atomic per-role migration and old-PUT refusal; it is not designed or exercised here. Evaluators must later prove section 5 and retained narrowing against current policy/facts/scope. HTTP activation compatibility with applicable legacy teams, human-proof provenance, API-owned enqueue/provenance and approval/worker rechecks remain existing named gates under ADR 0246 and authority-config section 7.

Removing binding/route restrictions, allowing reverse migration, accepting nonhuman invocation or inventing human proof would change authorization semantics and require a separately authorized contract. They are neither inferred nor proposed here.

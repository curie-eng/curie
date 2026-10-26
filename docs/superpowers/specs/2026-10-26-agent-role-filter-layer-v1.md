# Pure role filter layer v1

This realizes only the filter layer of Accepted [ADR 0246](../../adr/0246-common-admission-resolves-an-optional-http-identity-before-authorized-enqueue.md), using the [role representation](2026-10-25-agent-role-filter-v1.md) and [HTTP facts](2026-10-23-http-identity-facts-v1.md). It is an internal pure calculation, not invocation or approval permission. It introduces no runtime caller, persistence, migration execution, credential access, HTTP request, cache, route or enqueue.

## 1. Internal inputs

<!-- @spec AGENT-ROLE-LAYER-V1-1 -->
Add `curie_api.agent_role_evaluation.evaluate_role_filter_layer(policy, facts, *, context, now)`. Policy is `AgentRolePolicyV1`, `RoleFilterRefusal` or explicit `None`; facts is `ValidatedFacts` or `FactsRefusal`. Other outer types refuse. The frozen internal `RoleFilterContext` contains canonical `tenant_id`, canonical `agent_id`, role `invocation` or `approver`, and the exact expected `LookupRequestV1` as `request`. It forbids extra fields and has no proof, permission or human flag. Construction of this object establishes no authentication. Approver maps to lookup purpose `approval`; invocation maps to `invocation`.

The trusted composition must derive context and policy from current server state and supply T1 facts validated from the original accepted response bytes after authoritative locks. A Python `ValidatedFacts` instance is not proof of provider authenticity or unforgeable provenance. This function does not reconstruct response bytes or repeat T1's full response-union, group-completeness or source-lag calculation. It performs ordinary shape validation and only the defensive coherence checks enumerated in section 3. Ordinary valid fixtures use real `validate_lookup`; invalid shape or those contradictory components refuse, but detecting coherent same-process forgery is outside this boundary.

Revalidate context and present policy, including nested mutations. Use the existing bounded role render/parse boundary to detach the policy; preserve absence and omitted roles. Revalidate ordinary facts shape without claiming that shape validation proves T1 provenance. Do not retain mutable selector lists between calls, create a version-zero policy, accept arbitrary dictionaries or introduce callbacks for missing trust checks.

## 2. Conditional results

<!-- @spec AGENT-ROLE-LAYER-V1-2 -->
Return a frozen `RoleFilterLayerResult` with `status` exactly `match`, `no_match` or `refusal`, a fixed `reason`, and constant `permission = "not_evaluated"`. There is no `allowed` member or permission conversion. Truth testing raises `TypeError("role_filter_layer_is_not_permission")`. Result strings/reprs contain only fixed status/reason/permission values, never identity, policy, context or exception data.

Non-refusal reasons are `unrestricted`, `exact_selector`, `exact_raw`, `not_configured`, `empty_filter` and `no_selector_match`, used only as specified below. Revalidate FactsRefusal/RoleFilterRefusal instances before preserving their existing codes as refusal reasons; invalid constructed refusal codes become fixed `invalid_facts` or `invalid_agent_role_configuration`, respectively. Additional refusal reasons are `invalid_context`, `invalid_facts`, `forbidden_kind`, `inactive_requires_migrated_raw`, `unresolved_requires_raw` and `independent_human_required`. `scope_mismatch`, `identity_stale` and `membership_stale` reuse existing facts codes. No result grants permission or satisfies retained restrictions.

## 3. Guard order and clock

<!-- @spec AGENT-ROLE-LAYER-V1-3 -->
Apply this precedence, independent of selector order:

1. Unsupported outer argument type, invalid context/role/request shape or invalid UTC clock: `invalid_context`.
2. A revalidated policy refusal or bounded policy revalidation failure: its existing role refusal code; an invalid constructed refusal becomes `invalid_agent_role_configuration`. Explicit absence is not failure.
3. A revalidated facts refusal: its existing code; an invalid constructed refusal becomes `invalid_facts`. Otherwise invalid facts shape or missing required outcome components: `invalid_facts`.
4. Context tenant must equal its request tenant; any present policy tenant/agent must equal context. Complete facts request must equal context request, including request ID, purpose, authority version and full sender. Role must agree with purpose. Any mismatch: `scope_mismatch`.
5. Identity expiry first, then resolved membership expiry: `identity_stale` or `membership_stale`.
6. Service or agent kind: `forbidden_kind`, before any raw/null matching. Unknown kind is supported only for unresolved facts; contradictory kind/outcome components refuse as `invalid_facts` in step 3.
7. Apply the selected role's definitive-outcome table.

Use the existing T1 `_clock` contract: supplied `now` is an aware datetime with zero UTC offset; reject naive/nonzero-offset clocks. Compare exact Fraction epoch values, never floats. Equality with expiry remains usable; any positive excess is stale. Resolved facts require identity and membership expiries; inactive/unresolved require identity expiry and no membership components. Resolved facts require active canonical identity matching sender kind and expected issuer, complete tuple groups and membership observation; inactive facts require disabled/revoked identity matching sender kind and issuer; unresolved requires no identity/groups/membership. A missing required component refuses, never becomes absence or unrestricted. Do not change, extend or round expiry, read a clock internally or claim to recompute T1's source-lag/skew contract.

## 4. Exact selector equality

<!-- @spec AGENT-ROLE-LAYER-V1-4 -->
Select only context.role; one role never supplies the other's entries. Raw equality is `(tenant_id, provider, namespace.id, namespace.authority, namespace.kind, namespace.key, native_id)`; enclosing policy/context supplies tenant. Receiving channel/installation IDs belong to exact request correspondence, not durable raw equality. Subject equality is `(issuer, subject)`; group equality is `(group.issuer, group.id)`. Use any-match within one role without entry precedence. Do not trim, case-fold, normalize URLs, infer aliases/email/names or substitute local principal IDs.

## 5. Outcome table

<!-- @spec AGENT-ROLE-LAYER-V1-5 -->
For fresh resolved active human facts: missing policy/requested role gives `no_match/not_configured`; configured filter null gives `match/unrestricted`; entries empty gives `no_match/empty_filter`; any exact raw/subject/group gives `match/exact_selector`; otherwise `no_match/no_selector_match`.

For definitive inactive human facts: only the requested role's expressly migrated configuration with an exact explicit raw entry gives `match/exact_raw`. Every other configuration, including retained raw, missing, null, empty, subject or group, gives `refusal/inactive_requires_migrated_raw`. Disabled and revoked are equivalent here.

For unresolved facts with human or unknown sender kind: an exact raw entry gives only `refusal/independent_human_required`; all other configurations give `refusal/unresolved_requires_raw`. Provider-claimed human kind and raw equality cannot independently establish human provenance. This slice deliberately does not implement unresolved eligibility and accepts no input to discharge that obligation. It adds no new migration prerequisite for unresolved raw detection. Service/agent facts already refused before this table.

## 6. Composition and proof boundary

<!-- @spec AGENT-ROLE-LAYER-V1-6 -->
The future composition must apply active tenant/agent, authenticated transport/installation/namespace/binding scope, permitted current kind and global denials before matching. It must AND the layer with every applicable retained team, binding caller and explicit approval-route restriction and revalidate current configuration/policy/generations around external lookup. Migration replaces only the selected legacy agent-team requirement; null clears none of those restrictions. Missing configuration is not fallback after a failed HTTP lookup. Approval answering actor, undo and recovery remain separate authorities. No runtime wiring or complete-allow helper belongs in this slice.

Tests independently cover both roles; every exact equality component; active null/empty/absent/any-match; migrated versus retained inactive raw; unresolved raw remaining refused; all existing refusal codes; independent identity/membership expiry including rational sub-microsecond boundaries; precedence with mixed errors; constructed/mutated invalid inputs; constant safe results and rejected truth testing. At least one negative/secondary path accompanies each criterion. They execute actual production parsing/matching without services or secrets. These proofs establish pure matching only, not provider freshness, human proof, retained-restriction enforcement, atomic delivery, worker rechecks or activation.

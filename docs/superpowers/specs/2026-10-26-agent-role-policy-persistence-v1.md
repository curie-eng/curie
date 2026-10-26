# Internal agent role-policy persistence v1

This realizes the storage and old-client compatibility portion of [Accepted ADR 0246](../../adr/0246-common-admission-resolves-an-optional-http-identity-before-authorized-enqueue.md), section 3. It consumes [role-filter representation v1](2026-10-25-agent-role-filter-v1.md), [authority configuration v1](2026-10-23-http-authority-config-v1.md), section 7, and the scope-lock contract in [principal adoption v1](2026-10-25-http-principal-adoption-v1.md), sections 3, 4 and 12. It introduces no authorization evaluator or activated role consumer.

## 1. Scope and the absence of live activation

<!-- @spec AGENT-ROLE-PERSISTENCE-V1-1 -->
Implement one additive stored role-configuration field, internal read/change primitives, transaction-coupled audit and a participating existing whole-policy writer guard. New role writes are invoked only by isolated tests in this slice. No route, settings/startup caller, backfill, command, hook, provider/registry integration, enqueue, approval consumer or worker change is added. Ordinary legacy writes never manufacture role configuration.

Existing consumers already consult the legacy team columns: `team_authorization.check_binding`, `admission.evaluate_invocation` and `approval_teams.approval_team_decision`. They do not evaluate the new filters. Correct the stale “nothing consults” description in the touched policy service; it is not authority to bypass consumers.

All preexisting rows remain without new configuration after upgrade. Keep both physical team IDs, binding caller restrictions and explicit approval-route restrictions unchanged when recording new role state. A migrated marker is stored migration intent/state for the later converged consumer, not proof that effective authorization has migrated. Clearing a team now could open a team-only reader; retaining a team still does not enforce a new restrictive filter. Consequently this slice authorizes no live population of the new field.

## 2. Existing row ownership and exact storage

<!-- @spec AGENT-ROLE-PERSISTENCE-V1-2 -->
Add nullable `AgentAccessPolicy.role_configuration`, PostgreSQL JSONB with `none_as_null=True`, no non-null default and no backfill. It stores exactly the `AgentRolesV1` object. Retain the existing table/key, tenant foreign keys, both team columns, PostgreSQL Integer `version`, and `updated_at`. Add `agent_access_policies_role_object_ck` with:

```sql
role_configuration IS NULL OR jsonb_typeof(role_configuration) = 'object'
```

SQL NULL means the whole new configuration is absent. JSON null is invalid. An explicitly stored `{}` means present configuration with neither role; omitted role keys, configured null filters, empty entries and populated entries remain distinct. Never persist a second policy version, scope envelope or copy of legacy team fields in the JSON.

Select/lock AgentAccessPolicy by its `agent_id` primary key alone, selecting `role_configuration IS NULL` from that same row. Do not add tenant ownership to the query predicate: a corrupt mismatched-tenant row must remain visible for validation, never become absence or enable a version-zero create. Stored-state validation uses this fresh row plus its independently selected SQL-nullness result. Do not infer SQL NULL from deserialized Python None. Before returning or changing an existing policy, require exact row tenant/agent ownership and an actual integer version in 1..2,147,483,647. For non-SQL-NULL configuration, reconstruct `AgentRolePolicyV1(schema_version=1, tenant_id=str(row.tenant_id), agent_id=str(row.agent_id), version=row.version, roles=row.role_configuration)` and validate through the existing pure role contract. Apply its canonical renderer's 65,536-byte bound and cross-role namespace consistency. Unsupported stored versions, malformed JSON/roles, noncanonical identity, invalid counters and oversize readback all refuse `invalid_role_policy_state`; none selects absence or unrestricted behavior.

The database object check rejects ordinary nonobject writes. Reader checks also cover corrupted/imported state where constraints were bypassed. JSONB has already decoded JSON, so the stored-state reader does not claim to recover original duplicate-key or numeric-token spelling. New external byte input remains the pure parser's responsibility; this internal operation accepts its typed model.

## 3. Internal interfaces and clean transaction entry

<!-- @spec AGENT-ROLE-PERSISTENCE-V1-3 -->
Realizing module: `curie_api.agent_role_policy`.

```text
read_role_policy(session, *, tenant_id: UUID, agent_id: UUID)
    -> AgentRolePolicyV1 | None
apply_role_change(session, *, change: AgentRoleChangeV1)
    -> AgentRolePolicyV1
RolePolicyRefusal(code)
```

Both entries require an active caller-owned transaction and empty `session.new`, `session.dirty` and `session.deleted` sets at entry. Refuse otherwise with `invalid_role_policy_input` before any query, flush, savepoint or rollback. Previously flushed caller work is allowed and remains owned by its outer transaction, subject to criterion 4’s outer lock-order precondition. Clean entry does not establish that already-held locks were acquired in a safe order. The primitive does not roll back, expunge or implicitly flush preexisting pending caller work. This precondition prevents `begin_nested()` from flushing unrelated pending state before its savepoint.

Read IDs must be UUID instances. Change must be an instance of one of the existing `AgentRoleChangeV1` union models and is revalidated, including constructed models and nested mutation. Invalid input gives the fixed input code. No raw mapping or bytes overload is added.

Each operation uses its own savepoint inside that transaction, with `no_autoflush` during discovery/reload. Success leaves the outer transaction open; apply flushes all its policy/audit writes but never commits. Returned models are detached validated values, never ORM/session/lock objects. A refusal rolls back the complete operation savepoint before raising. A caller catching refusal then committing cannot retain any operation-owned partial change. Outer rollback removes even a successful operation's changes. Unexpected database exceptions propagate after savepoint rollback and never become a successful or invented business result.

Read returns None only when the agent exists in the supplied tenant but its policy is absent or its new field is SQL NULL. A present `{}` returns the existing positive-version snapshot. Missing tenant/agent or wrong scope refuses; no version-zero new snapshot is constructed. Existing `agent_access.read_policy` continues to expose the legacy counter/readback; a caller needing the counter for an existing SQL-NULL row can use it within the same transaction. This is not an additional version owner.

## 4. Scope, lock order and participating writers

<!-- @spec AGENT-ROLE-PERSISTENCE-V1-4 -->
Reuse `principal_authority.lock_scope`; do not duplicate its tenant/authority locking. Both new operations first lock the supplied tenant and its authority row through this guard, then Agent, then its existing AgentAccessPolicy selected by agent_id alone, all with current-state reload (`populate_existing`). Validate policy ownership after selection, including for SQL-NULL configuration. Require the locked agent's tenant to equal the supplied tenant. Agent locking serializes creation when no policy row exists. Read uses `require_active=False` and permits inactive tenant/agent readback. Apply requires active tenant and agent; scope absence, mismatch or inactivity gives `role_policy_scope_unavailable`. This is a storage precondition, not actor authentication or invocation permission. Role operations require no HTTP authority mode and change no authority row or lifecycle generation.

The extended total order for operations needing these rows is: existing global identity fence when required by the outer composition; Tenant; TenantIdentityProvider; Agent; AgentAccessPolicy; Team rows in UUID order if needed; connection-token rows; namespace rows; Principal rows; principal-bound ConsoleSession rows, retaining the principal contract's ordering within those groups. New role primitives acquire no global fence themselves and never acquire one after scope/data locks. They take no team/namespace/principal/session write locks and perform no provider or registry I/O.

All callers must preserve this order for locks already held and for caller work flushed before entry. Pending or previously flushed work does not permit entering these primitives after out-of-order locks. For legacy pending Tenant/authority/Agent/policy work, the outer caller first acquires any required global fence, then obtains `principal_authority.lock_scope`, and only then stages/flushes dependent work in this order. This is a caller composition precondition, not a new runtime proof flag or a guarantee against arbitrary dirty-session deadlocks. Do not add generic state restoration or change the principal guard’s lock mode.

The existing legacy `replace_policy` participates directly: first preserve caller work through criterion 6’s preflush, then start its guard/write savepoint, discover the agent's tenant without a write lock, enter `lock_scope(require_active=False)`, then lock/reload Agent and policy before checking state or writing. Do not trust the discovery ORM object. Missing/disappeared agent keeps its existing None result. Retain legacy inactive-scope behavior. Missing tenant with a surviving invalid agent row is corrupt state. No legacy route-only guard substitutes for this direct primitive participation.

Current writer inventory:

| Owner | Participation in this slice |
| --- | --- |
| `agent_access.replace_policy` | Sole existing direct policy insert/update; scope-first locking and criterion 6 guard. Its route has the outer identity mutation fence through main middleware. The sole production route receives a fresh SessionDep, performs no prior session mutation, and calls the wrapper directly; authentication uses a separate session. |
| `crud.agents.delete_agent` | Existing dependent-row deletion then Agent deletion cascades policy removal. It acquires no tenant/authority lock afterward. New role code touches none of its child tables. Preserve deletion behavior and prove both race orders; do not add a speculative lock refactor. |
| `directory_service.update_configuration`, `directory_sync.locked_team`, directory snapshot/provisioning paths | Existing principal scope before team/namespace/principal work. They do not modify AgentAccessPolicy. Retain their order and prove same-tenant coexistence. |
| `teams.list_team_sync` and teams routes | Read-only; no additional writer. |
| `team_authorization` | Existing policy reader, unchanged and not a new filter evaluator. |

A production caller that violates the outer lock-order precondition is an engineering blocker; report it rather than silently changing the route contract. No additional production policy writer was found in the reviewed source. The implementation must recheck this inventory; a newly found writer or an agent/team-first path that later takes tenant scope is an engineering blocker until participation/order is specified. No unreviewed bulk SQL/import path is declared safe.

## 5. One-role intent and sole policy CAS

<!-- @spec AGENT-ROLE-PERSISTENCE-V1-5 -->
The change's canonical tenant/agent IDs identify the locked scope. Validate existing state before comparing `expected_version` with the stored policy version, or zero for row absence. Mismatch gives `stale_role_policy_version`. Absent row success creates version 1 with both physical team IDs NULL; existing row success advances its sole version exactly once, including an equal-filter write. Different roles still share one CAS counter; do not merge stale independent edits or add per-role versions.

For `set_filter`, create a retained role when absent, otherwise preserve its existing legacy_state and mandatory migrated disclosure. Replace only that role's filter. For `migrate_role`, refuse an already migrated role with `role_already_migrated`; otherwise store migrated state, supplied filter and exact `separate_removal_required` disclosure. A role without a team still requires this explicit operation to choose migrated semantics. Do not create the absent other role or reset the other role's state.

Before mutation, build and fully validate the proposed next snapshot with the incremented counter. Namespace consistency includes the stored other role. Assign a fresh serialized whole `AgentRolesV1` value to the JSON column; do not rely on in-place nested JSON tracking. Preserve the untouched role semantically, entry order and omission, both physical team IDs, and every binding/route restriction. JSONB object-key order is not a byte-preservation promise.

Physical team retention preserves their foreign-key lifetime and does not mean a future migrated consumer should continue applying that replaced team requirement. Effective replacement belongs to that consumer's explicit migration interpretation; it is not enacted here. No reverse migration, team clearing, binding/route clearing, authority conversion or legacy rewrite is accepted.

## 6. Existing whole-policy PUT guard and compatibility

<!-- @spec AGENT-ROLE-PERSISTENCE-V1-6 -->
Keep `AgentAccessPolicyWrite`, `AgentAccessPolicyOut`, router signatures and successful legacy `replace_policy` commit behavior unchanged. Do not make the new primitive call this committing wrapper. Existing missing-agent 404 mapping, absent-row version zero, pre-migration team validation messages and stale-version mismatch message remain unchanged.

Under its criterion 4 locks, the direct legacy primitive first validates stored state. Corruption raises `PolicyConflict("invalid_role_policy_state")`. If either role is migrated, raise `PolicyConflict("agent_role_policy_migrated")` before CAS/team changes, even when submitted team IDs are identical or only the other role would change. The existing router maps PolicyConflict to 409; no new router/status model is needed. Refuse the entire write, with no own row/team/filter/version/timestamp mutation and no success audit.

For valid unmigrated state, retain the existing CAS mismatch text and `_require_idp_team` validation semantics/order (invocation then approver). After CAS, refuse an exhausted counter with `PolicyConflict("role_policy_version_exhausted")` before any increment. Valid writes change only the two legacy team columns, version and timestamp, preserving SQL NULL or retained role JSON exactly as stored. No implicit role migration, filter reset or configuration creation occurs. The old schema's broader integer input acceptance is unchanged; execution cannot exceed the stored Integer domain.

The existing wrapper may begin/commit its transaction as before and is not subject to the new entries' clean-session rejection. Its entry sequence is fixed: explicitly flush preexisting caller work before any discovery/current-state query or `populate_existing`; after that flush succeeds, begin the operation savepoint; discover scope, acquire/reload scope and rows without autoflush, validate state and migrated guard, compare CAS, check exhaustion, validate teams in the existing order, write/flush/refresh, then complete the savepoint and commit on success. This preserves the historical preflush before the old first query, including pending new/dirty/deleted objects and pending values on Tenant, TenantIdentityProvider, Agent or policy objects subsequently reloaded. The outer caller must satisfy criterion 4’s lock-order precondition before staging/flushing such work.

A preflush error propagates before any operation savepoint or own write; its normal SQLAlchemy failed-transaction state belongs to the caller. Successfully preflushed caller changes remain outside the operation savepoint's rollback promise and under the outer transaction. A failed guard or later own-write error rolls back the operation savepoint and never commits; it does not erase that preflushed work. A caller may then commit the preserved outer work. Successful legacy commit retains its historical effect of committing caller work too. No generic session-restoration mechanism or broader legacy transaction redesign is introduced.

## 7. Counter, refusal ordering and rollback guarantees

<!-- @spec AGENT-ROLE-PERSISTENCE-V1-7 -->
Versions 1..2,147,483,647 are valid stored/readback counters. Expected-version zero is permitted only by the existing create convention. At maximum, readback succeeds but every attempted policy increment refuses before mutation. No larger integer storage, wrapping, reset or second counter is introduced.

For new apply, the first failing stage is: active/clean transaction and typed-model revalidation; current agent scope/activity/ownership; complete stored-policy ownership/state validity; expected-version CAS; repeated-migration compatibility; exhaustion; proposed final snapshot validation; writes/audit. Invalid proposed state after a valid input is `invalid_role_policy_input`; invalid current state is `invalid_role_policy_state`. Read uses transaction/input, agent scope/ownership (without activity requirement), then stored-policy ownership/state validation. Legacy ordering is criterion 6. Fix these outcomes independently of JSON member order or ORM cache state.

`RolePolicyRefusal` carries only one of `invalid_role_policy_input`, `invalid_role_policy_state`, `role_policy_scope_unavailable`, `stale_role_policy_version`, `role_already_migrated`, `role_policy_version_exhausted`. Its str/repr/args expose that code only. Translate expected model/scope failures without original error chain, candidate values, identifiers, field paths or validation detail. Unexpected storage failures are not claimed as code-only domain refusals.

All operation-owned JSON, counter, timestamp and audit changes are atomic under the savepoint. A failed final validation writes nothing. A real audit insert/flush error erases preceding policy writes; caught domain refusal plus caller commit leaves no operation side effects; outer rollback erases successful operation effects. Previously flushed outer work is neither committed nor rolled back by the new primitive. No external successful action is returned or performed before the owning caller commits.

## 8. Transaction-coupled bounded audit

<!-- @spec AGENT-ROLE-PERSISTENCE-V1-8 -->
Each successful new change appends one `identity_audit.record` event in the same transaction:

```text
event_type = "identity.agent_role_policy_changed"
actor = "role_policy"
tenant_id = locked tenant
principal_id = None
team_id = None
details = {
  "agent_id": canonical agent UUID string,
  "role": "invocation" | "approver",
  "operation": "set_filter" | "migrate_role",
  "previous_version": integer (0 for absent policy),
  "version": resulting positive integer,
  "previous_legacy_state": "retained" | "migrated" | None,
  "legacy_state": "retained" | "migrated"
}
```

The fixed actor classifies the internal owner, not an authenticated person. The API accepts no claimed audit actor. Do not record raw filters, selector identities, namespace descriptors, endpoint, credential reference, response bytes or entire policy JSON. No audit or version increment occurs on refusal, including repeated migration or exhaustion. Legacy writes keep their existing audit behavior; this slice adds no retrospective event requirement to unrelated operations. Use database time for the existing `updated_at` behavior; do not claim strictly increasing timestamps within one transaction.

Deletion has two explicit outcomes. If agent deletion commits first, a subsequent new role operation refuses scope and creates no audit; legacy replacement retains its missing-agent None result. If a new role write commits first and deletion follows, policy removal cascades but its committed audit event remains as immutable historical evidence: IdentityAuditEvent has no agent foreign key. Never delete that valid history or add an agent FK. Refusal or rollback leaves no operation-owned event.

## 9. Additive schema and precise candidate pins

<!-- @spec AGENT-ROLE-PERSISTENCE-V1-9 -->
At this specification's foundation the graph head is 0132 and no 0133 revision exists. Add `0133_agent_role_configuration.py`, revision 0133, down_revision 0132, classified expand. Preserve existing migration history. Upgrade adds only criterion 2's column/check and leaves row values/version/timestamps unchanged. Do not change team foreign keys or synthesize role JSON.

Downgrade refuses with fixed RuntimeError text `agent_role_configuration_present` if any row has `role_configuration IS NOT NULL`, including `{}` or corrupted JSON null. Otherwise remove only the new check/column. It neither clears data to permit downgrade nor implements semantic reverse migration.

Because existing ORM AgentAccessPolicy reads will include the new column, the candidate serving floor and head both become 0133. Update the API window, chart schema-compat mirror, CLI candidate/revision catalog and shared protected-hooks graph/window in their existing owners. Add 0133 to revision_kinds as expand and actual graph metadata as child of 0132. Preserve every published release window, all Cargo/Helm release versions, unknown-newer-revision policy and the existing upgrade mechanism.

Before source begins or after a rebase, re-resolve the actual graph. If 0133 is occupied or the parent changed, stop the affected migration/pin work and amend this specification before tests/source; do not manufacture an empty bridge, duplicate history, dangling parent or automatic graph merge. Public source publication must exclude any temporary dependency-combination commit and wait for the prerequisite contracts' converged public ancestry. This is a development compatibility update, not release preparation.

## 10. Evidence and remaining activation gates

<!-- @spec AGENT-ROLE-PERSISTENCE-V1-10 -->
Use isolated real PostgreSQL for storage, locks, rollback and races. No mocked database/row lock or timing-only sleep is proof. Independent tests precede source; missing implementation/revision assertions belong inside test bodies so intended RED is not a collection/setup failure. Use actual committed migration in final proof. Test all scope/CAS/refusal/rollback boundaries with synthetic values and no external provider.

Real population remains blocked until current invocation and answering-actor consumers enforce the reviewed role semantics or affected operations are demonstrably denied through a separately reviewed supported guard. Required convergence includes global activity/scope/kind checks; exact role matching with retained binding/route narrowing; API-owned authorized enqueue with no dispatcher cache/offline bypass; current policy/configuration/credential/freshness checks across lookup; pending approval and undo/recovery rechecks; and the separately owned supported provenance/current-authority path at claim/resume/privileged effects. Registry or SQL row locks alone do not make an independently delayed queue operation safe. No worker work is added here.

Unknown-newer schema serving does not exclude mixed old binaries or arbitrary SQL. This slice qualifies only a controlled all-new disposable candidate and records that limit. It does not claim safe installation rollout, live migration, complete authorization, provider freshness or deployment. A required exact-head CI check must pass before any separately authorized merge; synthetic persistence proof is not runtime activation proof.

## Criterion/test matrix

| Criterion | Required positive and negative/independent evidence |
| --- | --- |
| 1 | Upgrade preserves old absence and consumer behavior; source/call inventory proves no production new-writer, backfill or evaluator. |
| 2 | Missing policy/SQL NULL versus present {}, both roles/null/empty/populated readback; JSON null/scalar/array constraints; corrupted object/unsupported versions/oversize and mismatched-tenant row selected by agent_id alone, including SQL NULL, refuse without absence/CAS-zero fallback. |
| 3 | Flush without commit, caller commit and outer rollback; absent/failed transaction and each new/dirty/deleted entry precondition refuse before autoflush; constructed/nested-mutated model refusal; previously flushed outer work preserved under the explicit outer lock-order precondition. |
| 4 | First-create serialization; stale ORM reload; missing/wrong/inactive scope semantics; direct legacy preflush then scope-first participation; clean production entry and order-disciplined pending scope/Agent callers; adoption/directory/global-fence and deletion races in both orders without a cycle. |
| 5 | Set/migrate for each role including no team; shared counter and equal-filter increment; repeated migration; immutable untouched role/teams/bindings/routes; cross-role namespace contradiction refusal. |
| 6 | Unmigrated old PUT retains response/CAS/team/inactive-scope behavior and preserves retained JSON; either migrated role rejects unchanged and changed old PUT atomically via direct and router entry; corrupt/exhausted bounded conflicts; preflush preserves pending new/dirty/deleted and scope/Agent/policy values through guard refusal and own-write failure without committing. |
| 7 | Counter endpoints, max readable/max write refusal; fixed mixed-error precedence; failed final validation and actual audit DB failure leave no policy/audit partial state after outer commit; new entries never implicitly flush caller-pending work; legacy explicit preflush remains outside own-savepoint rollback. |
| 8 | Exactly one bounded event per accepted operation including equal filter; no sensitive marker in details/errors; no audit on refusals; success rolled back with its event; committed write-before-delete retains historical audit, delete-first creates none. |
| 9 | Actual 0132 to 0133 graph/upgrade, old data preservation, SQL-NULL-only downgrade success and populated {} downgrade refusal; candidate pins consistent; published windows/versions unchanged. |
| 10 | Deterministic same/different-role CAS, migrate/legacy PUT, set/migrate, first-create, scope-writer and delete races; final focused/static/docs/schema evidence; no live activation claim. |

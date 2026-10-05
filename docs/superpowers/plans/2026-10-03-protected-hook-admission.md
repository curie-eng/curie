# Protected hook atomic admission implementation plan

Follow the repository implement workflow and parent plan ownership. Specification, observed failing tests and implementation remain separate commits.

**Goal:** Supply a real unwired broker admission transaction and bounded exact ID recovery, while protected activation remains closed.

**Architecture:** A closed AtomicAdmission facade consumes an explicitly supplied enqueue scoped Redis client. Immutable original intent and binding, mutable preparation/failure state, a separate immutable final commit and temporary recovery bytes support partial Lua writes without claiming rollback. One EVAL checks actual INFO server/TIME and current source/control snapshots before new admission effects.

**Tech Stack:** Existing Python protected-hooks package, redis 8.1.0 and measured disposable Valkey 8.1.10. The existing workspace aci-protocol parser becomes a declared direct dependency; no new external dependency or frozen interface change.

**Spec:** [Atomic admission](../specs/2026-10-03-protected-hook-admission.md), with parent [lane](../specs/2026-10-02-protected-hook-lane.md) and [source](../specs/2026-10-02-protected-hook-source-policy.md) contracts. Read all three before implementation.

## Global constraints

* Remain unwired; no API/ordinary dispatch/worker startup, source protected publication, qualification or guard/provisioning implementation.
* Pass an explicit enqueue scoped redis.Redis. Do not read credentials from environment or export a raw client/generic command.
* Stream is exactly curie:runs on the separate broker; consumer inventory belongs to its existing owner.
* Broker TIME determines the 300000 ms preparation deadline and ten attempt limit. No XADD * on recovery.
* Metadata maximum 16384 bytes; internal queued payload maximum 262144 bytes. Frozen QueuedTurn and plugin format remain unchanged.
* Original receipt metadata has no TTL; temporary recovery payload is removed before committed/failed state. Never release accepted quota without the future completion owner.
* Specification commit first. A test author then commits observed failing tests alone. An independent implementer follows. Every implementation/test unit cites ADMISSION and corresponding SOURCE/LANE IDs.
* Anonymous fixtures only; exact owned container and label cleanup; no production service, credential, raw log or external message.

## Review focus

* A retry after runtime rotation returns the original committed receipt even if new readiness is absent.
* First SET succeeds but preparing State write fails; recovery still owns the immutable intent.
* XADD succeeds but final commit fails; no success result or model eligibility until exact recovery.
* Stream IDs exceed exact Lua integer range; decimal comparison and overflow must retain uniqueness.
* Cleanup writes fail; no assumed quota refund or re-admission of the delivery identity.

## File ownership

<!-- doclint:ignore-line -->
Create `packages/protected-hooks/src/curie_protected_hooks/admission_records.py` for strict immutable input/result/envelope/intent/state/receipt shapes and digest/key derivation. Create `admission_scripts.py` for atomic duplicate, admission and recovery scripts. Create `atomic_admission.py` for the closed facade and safe dependency error translation. Create `admission_acl.py` for provisioner installed closed enqueue/verifier permission recipes. Tests live in the existing default collected `packages/protected-hooks/tests/`; share an owned disposable broker fixture locally without touching existing worker integration.

Read `authority_records.py` for parse_manifest/parse_qualification/parse_readiness/validate_authority, `source_policy_records.py` for exact policy_fingerprint and validation, `source_fence.py` for the actual persisted source shape, and `broker_metadata.py` for existing reset/handshake conventions. Do not regenerate these working modules.

## Task 1: Complete transaction contract tests, committed red

<!-- doclint:ignore-line -->
**Files:** Create `tests/test_admission_records.py`, `tests/test_atomic_admission.py`, `tests/test_admission_recovery.py`, `tests/test_admission_acl.py` under packages/protected-hooks. Create fixture helper `tests/admission_broker.py` if the existing conftest cannot isolate this authority subset. No production edit in this task.

**Interfaces:** Tests import AdmissionRequest, DeliveryIdentity, Envelope, Intent, State, Receipt, AdmissionResult, AdmissionUnavailable from the new records module; AtomicAdmission from atomic_admission; admission_acl_rules from admission_acl. Record classes store canonical bytes like authority_records, expose canonical_bytes and fresh as_dict; input request alone carries exact queued_payload bytes. Provide `delivery_digest(identity) -> str`, `execution_session_key(runtime_id, runtime_generation, logical_conversation_key) -> str`, and `parse_envelope/parse_intent/parse_state/parse_receipt(raw: bytes)` returning their immutable types. Request constructors must validate direct construction too.

- [ ] Write record grammar tests with exact field sets from ADMISSION-2/3, strict null/policy handling, actual unchanged hook QueuedTurn event IDs, non-null reply and empty attachments. Assert byte/metadata limits, unknown/duplicate fields, bool as integer refusal, canonical framing collision resistance, caller mutation isolation, exact envelope/payload digests and safe repr.
- [ ] Write real broker happy/concurrent admission tests: one original intent, one ZSET member and one exact two-field stream entry; accepted receipt original selection/policy/body/generation; repeat status duplicate with byte-equivalent receipt and no writes. Use actual TIME and INFO; fixtures write trusted control/source records only with their test provisioner.
- [ ] Write duplicate tests clearing readiness/selection and trimming runs after original commit; exact source-authenticated request still returns original receipt. Freshly signed body digest stays equal; whitespace/body/requested policy/generation/op/fingerprint change returns conflict. Do not require a changed new payload on a committed duplicate to match new runtime/event reconstruction.
- [ ] Write authority refusal tests for each manifest/qualification/readiness/source field, revoked source, future/expired proof, wrong run_id, admission closure, quota bound, wrong key type and event collision. Snapshot all broker keys before and after; expected fresh admission refusal has identical snapshots. Test retained records after orderly restart with changed live run_id; it refuses before new intent.
- [ ] Write recovery tests using exact fixture key ACL selectors to deny writes after each successful boundary. Test first intent only, state only, quota/binding/recovery records, append
  success/final commit failure by allowing mutable state SET while denying
  literal protected:admission:commit:{d} SET with exact-key fixture ACL
  selectors, lost response, absent reserved ID after stream advancement, exact existing entry, contradictory entry, closed readiness attempt, missing quota with backlog filled by a later
  delivery, tenth attempt and actual broker-time deadline. Advance broker time condition by creating bounded old intent through the test provisioner; do not use caller wall clock as product authority. Assert quota/refund and recovery-byte removal, retained receipt/digests and no second stream ID.
- [ ] Write scoped ACL tests: enqueue source/control reads plus declared EVAL selector and denied inner writes; curie:runs append/recovery only; denied XREADGROUP/XACK/reclaim/admin/pubsub/other curie keys. Verifier writes readiness only, denies manifest/qualification/selection/source/admission/stream. Recipes preserve existing principal password/on state after reset and remove prior overprivilege. Test unknown role and absent INFO permission safe refusal.
- [ ] Run `uv run --frozen pytest packages/protected-hooks/tests/test_admission_records.py packages/protected-hooks/tests/test_atomic_admission.py packages/protected-hooks/tests/test_admission_recovery.py packages/protected-hooks/tests/test_admission_acl.py -q` against unchanged product. Record actual missing production symbol/product failure, not fixture failure. Inspect public diff and commit failing tests alone.

## Task 2: Implement the complete foundation

**Files:** The four production modules mapped above. Also modify packages/protected-hooks/pyproject.toml and regenerate uv.lock
for the existing aci-protocol workspace dependency. The root already declares
aci-protocol as a workspace source; do not duplicate its resolved mapping. The
uv.lock change maps local-release to required under the repository tier gate.
Update the existing import
boundary configuration if required. No apps/, runner/, chart/ or frozen package
changes. The integration owner alone handles shared configuration.

**Interfaces:** `AtomicAdmission(client, *, broker_identity, trusted_max_readiness_ms, backlog_limit)`, `admit(request) -> AdmissionResult`, `recover(identity) -> AdmissionResult`; `admission_acl_rules(role: str) -> tuple[str, ...]`. Shapes, key families and all outcomes are exactly ADMISSION-1 through ADMISSION-6.

- [ ] Implement closed immutable records and derivation helpers from ADMISSION-2/3 using existing record conventions. Declare the existing aci-protocol workspace parser dependency explicitly;
  validate unchanged QueuedTurn without requiring an agent field it does not
  have. Eligible sources are WEBHOOK or CRON, with cron hook identity matched. Source policy binds agent/hook. Never add approval-required to the existing one-value ToolAccess enum.
- [ ] Implement recipes from ADMISSION-6 with separate command/key selectors: enqueue source/control read only, admission write plus exact curie:runs append/recovery, EVAL declared key selector; verifier readiness write only. Use +info|server and existing reset tokens, not unsupported -info|server removal. No ACL installation method on product facades.
- [ ] Implement atomic duplicate check before new control reads. Current source snapshot is decoded/validated then compared unchanged in the duplicate EVAL. Committed exact duplicate returns immutable original result without readiness/selection/payload reconstruction. Preparing returns its original identity for recovery, failed stays failed and contradictions refuse.
- [ ] Implement new admission EVAL with exact raw source/control snapshot equality, live INFO server/TIME, exact authority comparisons, key TYPE and shape/bound checks, event-binding collision and ZCARD before first write. Reserve stream ID using decimal uint64 algorithm from ADMISSION-4. Write intent NX, preparing State, quota NX, binding NX, recovery bytes NX, explicit XADD, DEL recovery, SET immutable commit key NX with committed State and original Receipt last.
  Confirm NX success. Mutable state accepts preparing/failed only; no state
  update follows validated commit. Expose no success after an unexpected command failure.
- [ ] Implement recovery EVAL from ADMISSION-5: original reserved ID only, bounded attempt/deadline, current authority, exact XRANGE two-field verification, absent-ID appendability
  and idempotent quota. A missing member must pass fresh ZCARD capacity before
  repair; quota full cannot append and consumes only a bounded attempt. Terminal cleanup works when source is revoked/readiness closed and cannot authorize execution. Validated commit/failed cannot transition again. Duplicate/recovery validates
  commit against Intent, binding and original receipt before acceptance; orphan
  commit or commit plus failed state returns unavailable. Missing recovery bytes may finish from an exact existing stream entry; verify
  SHA256 in Python then compare its exact raw fields again inside EVAL. An
  authenticated retry may restore original digest-matched bytes when append
  never occurred; unattended recovery cannot invent them and ends boundedly. Unexpected errors remain unavailable and retained partial state remains repairable.
- [ ] Implement facade orchestration with explicit caller client only, bounded reads, strict response decode and safe exception translation. Compare exact raw broker snapshots in each EVAL after Python canonical validation so races never authorize stale facts. Recover uses original immutable tuple, never today's selection to reinterpret old payload. Document caller SQL/auth/TLS obligations without claiming them as implemented.
- [ ] Run the same focused command until it passes. Run existing protected-hooks suite, source-control/source-fence regressions, ruff/mypy/import/docs checks and complete required isolated Python acceptance baseline from AGENTS. Coordinate shared acceptance resources with the integration owner; no mock substitution or CI waiver.
- [ ] Independent reviewers check spec conformance, authority boundaries and failure state transitions. Fix findings, rerun affected checks, inspect exact anonymous outgoing diff and commit implementation after the already committed red tests.

## Tier classification before implementation

These documentation repairs preserve the admission decisions and introduce no
runtime behavior. Run the documentation validator now. Before the first test
or code change, the integration owner records the following seven-tier
classification and rechecks it against the exact changed-file gate:

| Tier | Classification | Proof or concrete boundary |
| --- | --- | --- |
| skill | Not applicable | No skill/plugin packaging, runner turn loop, ACI event contract or skill eval/check changes. The existing ACI parser is consumed without changing it. |
| local | Required | Real isolated admission, ACL, partial-write and recovery campaign using `uv run --frozen pytest packages/protected-hooks/tests -q`; full affected isolated Python baseline remains required. Ordinary Compose/HTTP/worker wiring remains unchanged. Record the actual completed outcome. |
| local-release | Required | Direct workspace dependency declaration and uv.lock reach the release path mapping. Run `CURIE_E2E_TIERS=local-release curie dev e2e-ladder` against the exact candidate. Unperformed proof remains required and unproved. |
| cluster | Not applicable | No chart, RBAC, NetworkPolicy, sandbox claim or init-container changes; the facade is not wired into cluster workloads. |
| live provider | Not applicable | No model routing, product credential resolution/provider auth, token/cost accounting or model/MCP execution changes. Broker credentials are explicitly supplied and no runtime execution is activated. |
| external integration | Not applicable | No Slack, git webhook, OAuth or third-party API surface changes. The broker campaign uses owned disposable local services. |
| factory | Not applicable | No factory runtime/CI/progress/publication, runner factory preflight, dark-factory example or worker work-item execution changes. |

If local-release proof is unperformed, the public discovery record must retain
the required row and the visible line below, rather than mark it not applicable:

Discovery waiver: The required local-release campaign for the direct workspace dependency and uv.lock change is unperformed; missing release-path proof and complete protected runtime qualification remain open activation blockers under #3603.

This line records missing proof only and does not replace any required local
CI/acceptance check, qualify a runtime or authorize source activation. The
integration owner replaces it with actual completed evidence when performed.

## Measurement recipe and handoff boundaries

Executed privately on 2026-10-03 with redis 8.1.0 and Valkey 8.1.10:

```python
roles = ('%R~protected:control:*', '%R~protected:source:*',
         '%RW~protected:admission:*', '%RW~curie:runs',
         '(+eval %RW~protected:control:* %RW~protected:source:* '
         '%RW~protected:admission:* %RW~curie:runs)')
producer.eval("local i=redis.call('INFO','server');local t=redis.call('TIME');"
              "local k=redis.call('TYPE',KEYS[1]);return {"
              "string.match(i,'run_id:([0-9a-f]+)'),t[1],t[2],k.ok,"
              "redis.call('GET',KEYS[1])}", 1, 'protected:control:probe')
producer.xinfo_stream('curie:runs')['last-generated-id']
producer.xrange('curie:runs', reserved_id, reserved_id)
producer.zadd('protected:admission:quota:probe', {intent_member: 1}, nx=True)
```

Main command rules included +eval +type +get +info|server +time +set +zadd
+zcard +zrem +zscore +xinfo|stream +xrange +xadd and standard handshake.
The exact sample role used broader admission key access for datatype probes;
the realizing recipe and complete test inventory must enforce its closed
product families. INFO clients and inner control SET/consume refused. A Lua
SET/XADD followed by an unauthorized LPUSH errored while both earlier writes
persisted. A lower absent explicit stream ID then refused append. Removing
INFO with -info caused inner INFO failure. Wrong-type preflight wrote nothing.
These commands measured primitives; they are not complete role qualification.

The owned Docker wrapper supplied an anonymous public image context. Inventory
preceded creation; only the exact recorded CID with matching owned label was
removed. Private report and full executable probe stay gitignored; public
records contain no machine endpoint, credentials, deployment or customer facts.

The implementer must record complete actual product ACL/error/restart campaigns
in the ADR evidence record after running them. This plan does not claim the
API reconciler, HTTP ingress, caller authentication/gate, source activation,
accepted-quota completion owner or worker model guard exists. Parent #3603
tracks each; installation remains closed until its complete campaign passes.

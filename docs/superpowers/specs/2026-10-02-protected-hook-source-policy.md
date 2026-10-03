# Protected hook source authority — initial implementation contract

Realizing contract for [ADR 0190](../../adr/0190-automated-hook-sources-cannot-widen-their-tool-access.md) and [ADR 0191](../../adr/0191-protected-hook-delivery-authority.md), tracked in [#3603](https://github.com/curie-eng/curie/issues/3603). This specification precedes implementation and does not claim activation or runtime verification.

## Existing behavior inspected

`hook_signing.derive` derives an agent-wide key from API_KEY, agent UUID and
`Agent.hook_generation`; `_material` signs the decoded hook, requested policy,
delivery ID, timestamp and raw body. `/agents/{id}/hook-secret` returns that key
under platform authentication and `Cache-Control: no-store`. `ingest_hook`
checks the requested policy before claim; `_duplicate_receipt` currently reads
the ordinary stream's queued turn and compares only its effective policy.
`HookAccepted.tool_access` attests queue policy, never execution or delivery.
`fire_hook` is a platform-authenticated cron producer with a separate run row
and ordinary resume queue. The shared administrative API key is trusted source
administration, including its present ordinary-worker holders. None of these
inspection statements constitutes an observed Postgres/Valkey behavior.

## Persistence and source identity

<!-- @spec PROTECTED-HOOK-SOURCE-1 -->
Add one additive `hook_source_policies` table. Its primary key is
`(agent_id UUID FK agents ON DELETE CASCADE, hook VARCHAR(63))`. Use the existing
hook-name validation, decoded once. Columns: `generation BIGINT NOT NULL > 0`,
`operation_id UUID NOT NULL`, `mode VARCHAR NOT NULL` in `protected|ordinary`,
`tool_access VARCHAR NULL`, `runtime_id VARCHAR NULL`, `qualification_id VARCHAR
NULL`, `bundle_digest VARCHAR NULL`, `legacy_generation BIGINT NOT NULL`, and
`updated_at TIMESTAMPTZ NOT NULL`. Protected mode requires exactly `read-only`
and all three immutable runtime/qualification/bundle references; ordinary mode
requires those policy fields null. Generations are positive, never wrap or
reuse. The row survives policy removal as an ordinary tombstone. No source
secret or consumer credential is stored in it. Agent UUID deletion/recreation
cannot revive a key because the UUID changes.

One protected runtime identity is initially permitted for each deployment;
its endpoint identity is immutable. Runtime generation changes belong to the
provisioning contract, not source CRUD. This removes cross-endpoint source
fencing from the first implementation. Refuse a different runtime ID instead
of silently moving an active source to another broker. Bundle or qualification
changes still require a fresh source generation.

<!-- @spec PROTECTED-HOOK-SOURCE-2 -->
Use one transaction-scoped advisory lock keyed by agent UUID for source CRUD,
legacy rotation/secret retrieval and named-hook ingress, held through admission
and enqueue. Locking the agent rather than a missing policy row also covers
first activation and agent-wide legacy-key rotation. Reload agent/policy under
that lock, rather than acting on an earlier ORM snapshot. Hook ingress first
validates its bounded raw request and authenticates its signature against a
read-only current agent/policy snapshot before requesting the advisory lock.
An unauthenticated request must not acquire that agent-wide lock. After acquiring
it, reload the authoritative agent/policy and reauthenticate the same requested
policy, raw body and signature against the current key and timestamp window;
the preliminary result is never admission authority. Rotation or policy changes
while waiting therefore refuse the stale request before claim/enqueue. All producers for a
configured named hook use this resolver. Cron/test-fire uses trusted producer
authentication but the same effective policy and private admission; it cannot
bypass policy via the ordinary resume queue. Unsupported cron/private routing
must refuse before its run claim, not enqueue ordinary work. Never-configured
ordinary hooks retain today's optional signed restriction and ordinary path.

Use `pg_advisory_xact_lock(hashtextextended('hook-source:' || agent_uuid, 0))`
as the single lock helper, called by all those paths. Hash collision can only
serialize otherwise independent agents, not weaken exclusion. Publication
occurs after the authoritative transaction commits and therefore releases this
lock; its broker CAS supplies the remaining protection.

The lock orders a legacy request already entering enqueue before the policy
mutation. The mutation cannot activate while that enqueue is in flight. A
request arriving afterward reloads the new policy. This lock is required even
though protected claim/enqueue is atomic on a different broker. No distributed
transaction between Postgres and Valkey is assumed.

## API DTOs and credential lifecycle

<!-- @spec PROTECTED-HOOK-SOURCE-3 -->
Administrative routes under `/agents/{agent_id}/hooks/{hook}/source-policy`
use existing platform authentication; ingress credentials authorize none.
`GET` returns `HookSourcePolicyOut` with `agent_id`, `hook`, `generation`,
`mode`, `tool_access`, `runtime_id`, `qualification_id`, `bundle_digest`,
`legacy_generation`, `activation` (`closed|active`), `updated_at` and optional
stable `refusal_reason`. No-row reads return ordinary generation zero.
`activation` is computed by comparing the authoritative row with the current
broker floor/operation/active record, not a writable SQL boolean.

`PUT` takes `HookSourcePolicyWrite {expected_generation, operation_id,
runtime_id, qualification_id, bundle_digest}` and selects mandatory read-only;
there is no wider policy enum or consumer credential field. `DELETE` takes
`expected_generation` and `operation_id` as query parameters and retains an
ordinary tombstone. `POST .../rotate` takes those same two fields and keeps
the protected configuration while allocating a fresh generation. Stale CAS
is 409; unknown configuration/artifact references are 422; unavailable fence
or readiness is 503. No successful mutation response promises activation if
publication failed: return 503 with a non-secret committed generation detail,
and GET reports closed. A repeated operation ID with the same committed intent
may resume publication; same ID with different intent is 409. An operation
older than the current row cannot modify it.

Check current matching-operation idempotency before rejecting a stale expected
generation, so response-loss retries of the successful operation can recover.
If the original operation reserved but never committed, recovery uses a fresh
operation ID and generation instead of pretending the mutation committed.

`GET .../secret` returns `HookSourceSecretOut {agent_id, hook, generation,
secret}` only for an active protected source, with `Cache-Control: no-store`.
Read does not rotate. Ordinary/tombstoned/unpublished policies refuse. No
source-policy GET, AgentOut, generic secret export, support response or audit
log includes this secret. No route returns/mints protected consumer or proof
issuer authority using platform API_KEY. The source secret is intentionally a
source-administrative credential, not protected broker consumption authority.

<!-- @spec PROTECTED-HOOK-SOURCE-4 -->
Derive a base64url HMAC-SHA256 source key using the platform key and compact
ASCII JSON bytes for `["curie.hook.source.v1", canonical_agent_uuid,
decoded_hook, generation]`. Use structured framing rather than ambiguous
colon concatenation. Reuse the existing `curie.hook.delivery.v2` signed bytes
and headers unchanged. A configured protected hook verifies only its current
scoped key, never agent-wide legacy fallback. Authenticate the actual requested
policy before deriving effective read-only. Scoped keys are invalid for another
hook, another agent, ordinary tombstone mode or another generation. Unknown
agent, bad key/signature and revoked key retain uniform ingress 401 behavior.

<!-- @spec PROTECTED-HOOK-SOURCE-5 -->
Every ordinary-to-protected transition bumps `agents.hook_generation` in the
same authoritative SQL commit as the new policy. This intentionally rotates
legacy credentials even when the operator does not claim previous exposure:
the server has no reliable inventory of who retained an agent-wide key.
`legacy_generation` records the newly committed counter. The administrator
reissues the new ordinary key to unaffected sources using the existing
hook-secret route. The restricted source receives only its scoped key. GET
source policy reveals the changed counter so API/CLI/UI can report unaffected
sources requiring credential replacement; no claim of automatic redistribution.
Protected-to-protected rotation does not rotate unrelated ordinary credentials.
Removal revokes the scoped key and restores ordinary behavior under the current
legacy key. Re-enabling allocates a higher scoped generation and rotates legacy
again, so a scoped key or previously retained shared key cannot revive. All
legacy derivation/verification uses the freshly locked agent counter.

## Broker-authoritative activation and recovery

<!-- @spec PROTECTED-HOOK-SOURCE-6 -->
Maintain one private broker source-control record per agent/hook: monotone
`floor`, reserved `operation_id`, and optional active `{generation,
operation_id, mode, policy_fingerprint}`. This is authority metadata, not a
dispatchable copy. A source-control writer credential may change only this
family, cannot publish runtime evidence or read protected payload, and is
separate from consume authority. Workers/source signers cannot change it.
The policy fingerprint covers every persisted decision field including
`legacy_generation`; use canonical serialization and SHA256.

Under the agent SQL lock, validate expected generation and selected references,
then call atomic broker `reserve_and_revoke(expected_floor, operation_id,
min_generation)`: clear active, allocate a generation strictly above both the
broker floor and committed SQL generation, and bind the reservation to the
operation. Repeated reservation of the same current operation is idempotent.
Persist that generation/operation and policy fields in SQL, including any
legacy counter bump; commit SQL before broker publication. Publish active by
CAS only if broker floor/operation exactly match the committed row and its
fingerprint, and required protected runtime evidence is current. This consumes
no delivery claim. Never reduce a floor or restore the pre-mutation active
record after failure. Every protected delivery atomically compares current
source generation/operation/fingerprint, runtime generation/qualification and
evidence expiry using broker-authoritative time with its claim and enqueue.

Ordinary tombstone publication confirms revocation and SQL removal; its ingress
continues checking matching active ordinary metadata under the agent lock before
ordinary enqueue. Missing broker state after a previously configured source
therefore closes that source rather than silently restoring unrestricted access.
Never-configured ordinary sources require no protected broker.

<!-- @spec PROTECTED-HOOK-SOURCE-7 -->
Crash before reserve leaves current state untouched. Crash after reserve but
before SQL commit leaves the source closed. Recovery cannot republish the old
SQL row: its operation does not match the new reservation. A new explicit or
automatic reconciliation operation allocates a higher generation and commits
the desired configuration before publishing; this rotates its scoped key.
Crash after SQL commit before publish can finish that exact committed operation
without another generation, if still current. A later edit first revokes and
reserves above it; a delayed earlier publisher then fails CAS. Concurrent edits
share the agent SQL lock; after lock release they must re-read and obey expected
generation. An administrator retrying an already superseded operation receives
409. Broker failures refuse before SQL mutation where possible; SQL failure
after reservation leaves the already revoked broker closed. Post-publish SQL
response loss is recovered by GET/retry without another enqueue or key revival.

Broker data loss, restart rollback or restored stale snapshots invalidate runtime
readiness and close admission. Reprovision a new independently issued runtime
epoch, clear readiness, reconcile source floors against durable SQL, and publish
only current rows after qualification. Do not treat an empty broker as generation
zero safe to activate, nor trust retained pre-reset proof. The
[ADR 0191 dependency evidence](../../adr/evidence/0191-protected-hooks/README.md)
records measured broker ACL, script/time, retained-session revocation and
Postgres commit-release behavior, and orderly AOF restart with stale-proof refusal.
Broker crash/rollback durability and SQL
disconnect recovery remain unmeasured; those observations do not qualify the
protected runtime or establish its provisioning boundary.

## Receipt and duplicate contract

<!-- @spec PROTECTED-HOOK-SOURCE-8 -->
Add receipt fields `requested_tool_access`, `effective_tool_access`, and
`source_generation` (null for never-configured ordinary hooks). Retain
`tool_access` as the effective-policy compatibility alias. An admitted private
receipt stores agent/hook/delivery ID, requested policy, effective policy,
`request_body_sha256` (lowercase hex SHA-256 of the exact raw signed body bytes),
source generation/operation, runtime generation, fingerprint, stream/event/conversation
IDs and acceptance status beside the private claim. It contains no raw body,
prompt or consumer credential. Claim/receipt/enqueue are one broker operation,
and original receipt evidence survives stream trimming and terminal cleanup.

Deduplication namespace remains agent/hook/delivery ID across ordinary/private
transport, not generation-scoped. Under the same SQL lock, ingress checks the
existing ordinary delivery claim before private admission; a prior ordinary
claim prevents a second private enqueue. A protected claim is never mirrored
as a dispatchable turn in the ordinary store. Protected-to-ordinary restoration
checks retained private receipts too; private broker failure closes tombstone
ingress. Never-configured ordinary requests keep their existing pending behavior.
Receipt stores must outlive transport movement and must not expose payload.

For a private duplicate, requested AND effective policy AND source generation
AND `request_body_sha256` must exactly match the immutable original receipt.
Signature and timestamp headers are excluded from that duplicate tuple so a
caller may freshly sign an otherwise identical retry within the current window.
No JSON normalization, body reserialization or decoded-text digest replaces
the exact raw-body digest. A re-signed changed body under the same delivery ID
returns 409 without claim/enqueue even when its policy tuple matches.
Rotation/removal/change returns
409 without a new claim; an old ordinary delivery cannot become a restricted
success. A legacy receipt with unavailable requested-policy evidence cannot
attest a protected retry and returns 409. Pending private duplicates may return
202 only with a stored exact tuple and explicit no-accepted-stream status;
missing/unknown original evidence returns 409. Authenticating against the current
key remains necessary before looking up duplicates. A prior receipt is never
relabeled using the retry's current policy. A support probe reads the same policy
and readiness but creates no claim, enqueue, message, sandbox or model turn.

## Surface parity and smallest implementation task

<!-- @spec PROTECTED-HOOK-SOURCE-9 -->
API is authoritative; CLI and UI are siblings that serialize the same DTOs.
Local/cluster CLI gain hook policy show/set/clear/rotate/secret commands, with
generation CAS, dry-run showing method/path and secret redaction, and matching
JSON outputs. UI gains named-hook policy controls, closed/active status, scoped
secret reveal/copy and the legacy replacement notice; it must not infer support
from OpenAPI presence or an effective receipt. Existing generic hook-secret
action remains explicitly ordinary shared-key issuance. Skill-tier local runner
test-fire has no server/source authority; it must not claim to validate the
platform policy or positive protected admission. Cron API/fire and scheduler
must route configured hooks privately or refuse before run creation. Support
responses contain no credentials and no writable policy field. Regenerate
OpenAPI and CLI command manifest through their existing generators.

The new additive support endpoint is `POST /hooks/{agent_id}/{hook}/support`;
the existing hooks router has only its delivery `POST /hooks/{agent_id}/{hook}`
and no support contract to replace. `HookSupportIn` is a strict JSON object with
only `tool_access: ToolAccess | null` (default null); this is the requested
policy used in signature context, not configuration. The raw JSON bytes are
signed without normalization, including this field. The endpoint uses existing
signature, timestamp and delivery-ID headers and validation even though that
ID creates no delivery claim.

Use the current source secret for HMAC-SHA256 over
`b"curie.hook.support.v1\n" + existing_delivery_v2_material`. A new support
signing helper prepends that purpose label to the exact existing canonical
delivery-v2 bytes; the existing delivery signer remains unchanged. The support
endpoint verifies only this purpose-prefixed form, while delivery ingress
verifies only its original delivery-v2 form. A support signature cannot enqueue
at ingress, and a captured delivery signature cannot authenticate the support
probe. Existing delivery-ID values are not newly reserved. Preauthenticate
before acquiring the agent advisory lock, then reload and reauthenticate the
requested policy under that lock as SOURCE-2 requires. A protected hook accepts
only its current scoped key; an ordinary/unconfigured hook authenticates with
its current legacy agent key and reports protected support unavailable.

`HookSupportOut` contains exactly `requested_tool_access`,
`effective_tool_access`, `source_generation` (null when never configured),
`runtime_id`, `runtime_generation`, `qualification_id`, `supported: bool`, and
`reason`. Runtime/qualification members are nullable when no authoritative
selection is available. Policy members describe the current server resolution
even on unavailable support. `reason` is a closed enum: `supported`,
`source_unconfigured`, `source_closed`, `runtime_unavailable`,
`qualification_unavailable`, `evidence_missing`, `evidence_expired`,
`broker_unavailable`, `broker_identity_mismatch`, or `configuration_unsupported`.
Return `supported=true, reason=supported` only with HTTP 200 and all runtime
members present. Authenticated unavailability returns HTTP 503 with this same
safe DTO and `supported=false`; never default unknown capability to success.
Unknown agent or failed signature/key/timestamp returns the existing uniform
401 authentication detail without this DTO. Invalid strict input has the
ordinary validation response and performs no admission mutation.

The probe evaluates current source activation, manifest/qualification,
readiness expiry against broker time, authenticated live broker `run_id`, and
first-release eligibility under the delivery-lane contract. It issues or
refreshes no proof and writes no state: no hook/cron claim, queue entry, quota
reservation, workspace, sandbox, placeholder, external message or model turn.
Its response is observational and is not an admission reservation. Existing
ordinary delivery and optional restriction behavior is unchanged; the new
endpoint makes no claim that an ordinary hook has protected-lane support.
Local/cluster CLI `hook policy support` serializes this exact request and
verifies this DTO rather than inspecting OpenAPI. It reads the scoped key from
an explicitly supplied secret file (never a literal secret flag), signs with
the new purpose-specific helper, prints the non-secret DTO, and treats HTTP
503 as unavailable with the safe reason. Dry-run prints method/path and
requested policy without reading or printing the secret or making a request.

First implementation task: additive migration + validated DTO/CRUD + scoped
HMAC unit + real Postgres/Valkey source-control CAS + ingress policy resolution,
receipts/dedup and support probe, with protected admission initially refusing
until the separate runtime/provisioning stream provides valid evidence. This
task does not activate runtime resources or clear the intake installation gate.
Build order is specification commit, committed failing tests observed failing,
then implementation commit. No frozen ACI/plugin-format fields change: outer
private metadata binds the existing `QueuedTurn.tool_access`.

## Acceptance cases and commands

Each test/implementation unit cites its corresponding ID above. Required cases:

* SOURCE-1/2: migration preserves existing agents, constraints refuse invalid
  protected/ordinary rows; first-policy activation vs ordinary enqueue is
  serialized; no-row race and reload after lock wait; invalid signatures never
  acquire the advisory lock; valid preliminary authentication followed by key
  rotation while waiting fails authoritative reauthentication; cron/fire cannot bypass.
* SOURCE-3/4: administrative auth only; consumer/evidence secrets absent from
  every response; scoped derivation differs across hook/agent/generation;
  exact existing signature bytes; missing requested read-only still effective
  read-only; changing the signed request policy fails authentication.
* SOURCE-5: retained old shared key fails every agent hook after activation;
  reissued shared key still operates unrelated ordinary hook; rotate/remove/
  re-enable never resurrect scoped key and don't leak secret in DTOs/logs.
* SOURCE-6/7: real broker CAS races, crash at every persistence/publication
  boundary, lost responses, superseded reconcilers, reservation orphan, stale
  expected SQL generation, broker outage/rollback/reset; no revoked generation
  becomes active. Evidence expiration at broker time refuses without claim.
* SOURCE-8: requested null/effective read-only duplicate vs requested read-only
  mismatch, policy change/rotation and prior ordinary receipt conflict, private
  stream trim with retained receipt, old pending legacy delivery, restored
  ordinary policy retaining private ID conflict; changed raw body (including
  whitespace-only JSON change) under a valid fresh signature conflicts; the same
  body under a fresh timestamp/signature matches; no second enqueue.
* SOURCE-9: API/OpenAPI/CLI/UI sibling shape, dry-run redaction, support probe
  side-effect absence, unchanged ordinary hook and human approval behavior;
  mutual support-signature/delivery-signature replay refusal; ordinary
  authenticated support returns safe unavailable 503; proof expiry, broker
  `run_id` mismatch and unsupported selection return safe false DTOs, with no
  state writes or execution. Fresh valid support signs exact raw JSON bytes.

Run `uv run pytest apps/api/tests/test_hook_tool_access.py
apps/api/tests/test_hooks.py -q` plus the
new focused migration/source-control/integration selectors (define them in the
test-first commit). Run the full isolated Python acceptance baseline in AGENTS,
`uv run python -m curie_api.export_openapi`, and
`uv run pytest apps/api/tests/test_openapi_drift.py -q`. API migration experiments
use owned disposable Postgres, never a shared database. CLI: `cargo fmt --check`,
`cargo clippy --all-targets -- -D warnings`, `cargo test` from cli. UI:
`pnpm lint`, `pnpm typecheck`, `pnpm test`, `pnpm e2e` from apps/ui. Names above
must be checked for existence before execution; proposed new selectors are not
current evidence. Actual provider/connector/orchestration acceptance belongs to
the other streams and cannot be closed by these source tests.

The [ADR 0191 evidence record](../../adr/evidence/0191-protected-hooks/README.md)
supplies the pinned dependency versions, exact probes and bounded observations.
Those dependency observations are not full runtime qualification. Remaining
implementation prerequisites are source-generation CAS/crash recovery,
broker restart durability, Postgres advisory-lock disconnect release and
concurrent policy writes, plus the complete source-control/application ACL and
cross-store idempotency inventory. Coordinate their key/role names with the
delivery-lane contract and record actual observations before claiming them.

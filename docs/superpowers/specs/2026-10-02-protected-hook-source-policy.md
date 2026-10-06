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
serialize otherwise independent agents, not weaken exclusion. The outer gate
transaction remains open across the separate registration and
authoritative work commits. Release the gate after the authoritative commit
and before publication; the broker CAS supplies the remaining protection.
The durable operation ordering is owned by SOURCE-10 below.

The lock orders a legacy request already entering enqueue before the policy
mutation. The mutation cannot activate while that enqueue is in flight. A
request arriving afterward reloads the new policy. This lock is required even
though protected claim/enqueue is atomic on a different broker. No distributed
transaction between Postgres and Valkey is assumed.

The shared internal library owns a typed transaction gate and source snapshot
resolver used by API and worker. API owns HTTP and administrative SQL writes.
Use a dedicated bounded gate connection pool, separate from the work/claim
pool. Every path acquires the agent gate before a work connection. Release
preliminary authentication read transactions before waiting for the gate,
then reload and reauthenticate through a fresh work transaction after locking.
A locked signed request first reads only the current authentication key-family
fields and reauthenticates, before validating full source references or operation
history. A stale signature therefore retains the uniform authentication refusal
even when the new source configuration is unavailable. Immediately before each
delivery claim attempt, validate the gate context's current task, live ownership
and transaction, and probe its held database connection. Loss detected before
that first effect refuses without a claim. This probe does not make a later
database disconnect atomic with an already started broker operation.
Signed ingress repeats that held-connection probe before backlog reservation,
workspace selection and owned enqueue after prior awaited work completes.
Acquire the workspace work connection before its probe. Before failed-delivery
settlement, probe again; detected gate loss leaves already-created claims or
quota to their existing expiry/recovery rather than authorizing more writes.
Secondary settlement or diagnostic failure preserves the original refusal.
Request cancellation retains the existing live-gate refund behavior: shield the
probe and settlement in the registered request task for a bounded five-second
cleanup scope, then propagate the original cancellation. No replacement task
may borrow the gate context, and detected gate loss still authorizes no cleanup.
A lock waiter must not retain a work/claim connection. Pass an acquired gate
context to inner helpers; never reacquire the same agent lock on another
connection. Close the outer transaction on every error or cancellation path.
Any failure to acquire the gate connection closes the gate as unavailable,
whether or not SQLAlchemy wraps it. Observed on 2026-10-05 with SQLAlchemy
2.0.52 and asyncpg, by opening `SourceGate.hold` against a missing database, a
wrong password and a refused port: asyncpg `InvalidCatalogNameError`,
`InvalidPasswordError` and a builtin `OSError` escaped unwrapped. Exceptions
raised by the caller while the gate is held keep their existing mapping.

A scheduled producer's explicit context belongs to exactly its provided guard,
agent, exact raw hook name, current task and active outer scope. Validate that
provenance at every effect entry; another guard's context is not authority even
for the same agent and name. The guard uses the producer's actual work engine
and a distinct same-DSN gate pool. No missing guard/context grants ordinary
admission, and no ambient context or implicit replacement pool supplies one.
A supplied work connection must belong to that exact work engine before any
SQL statement. Enqueue-failure settlement matches the authorized agent and exact
hook name in addition to its run ID; another source record remains unchanged.
Secondary cleanup telemetry or diagnostic failures preserve the original
enqueue exception, even after the settlement transaction commits.

Scheduled target discovery is a read-only hint. After acquiring the source gate,
reload the currently preferred deployment using existing precedence, then its
bundle and exact declaration before planning any effect. Refresh budget,
control, binding and kill decisions under that scope. A removed declaration
has no effect. Retain the existing immutable-version bundle cache key.
Report durable outcome counts and metrics only after their containing work
transaction commits; rolled-back reclamation is not a committed outcome.

Manual and scheduled cron producers validate the acquired gate context and
probe its held connection after any blocking inner hook lock and immediately
before each hook-run or schedule-control INSERT or UPDATE and each ordinary
enqueue, including enqueue-failure cleanup. Keep the outer gate across the
existing durable claim commit and enqueue. A failed probe authorizes no
following mutation or enqueue; never route detected gate loss through an
unguarded failure-cleanup write. Loss detected after a durable claim commit
may leave that claim for existing lease/recovery behavior; it does not roll
back the commit or authorize a replacement enqueue. A later cleanup failure
does not replace the original safe admission/enqueue refusal. These probes
establish current held-connection liveness, not atomicity between later SQL
effects and a broker operation.

Preserve existing durable cron claim commit before ordinary enqueue. The outer
agent gate spans the inner claim commit and enqueue, including manual fire and
scheduled, deferred, retry, skipped, blocked and reclaim paths. Configured
sources whose private routing is unavailable refuse before a run claim or
mutation. A source snapshot includes both policy and attempt-history presence:
only absence of both means never configured. SOURCE-10 owns pending history
and its absent-policy refusal. Existing declared cron names retain their exact
identity, including names outside the signed hook's canonical grammar. For such
names, check both source tables by the exact declared name under the same gate;
any policy or attempt history closes the unavailable configured path. Do not
lowercase, truncate or otherwise alias a legacy name to another source. Absence
of both retains existing ordinary fire behavior. Never treat a missing or unreadable table as
an ordinary source. The wired candidate requires the actual new ledger
migration head as its schema minimum; preserve prior registered windows. The
source-control candidate is `0.12.2`, with schema head and minimum both `0076`.
Register that minimum before connecting producer paths to ledger reads; missing
ledger support must fail startup, not fall back to ordinary admission. Keep all
previous windows, including the unwired `0.12.1` foundation, unchanged. These
are candidate compatibility records, not a published release.

The existing internal protected-hooks package owns one installed Python serving
resource containing candidate window, known revision IDs and actual ancestry,
and one pure serving decision shared by API and worker. Validate that resource
against the actual Alembic graph, CLI candidate and chart; keep every prior
registered window unchanged. Preserve existing first-parent serving ancestry
when a revision names multiple parents; extraction does not widen the decision
to reachability through another parent. Preserve the current presumed-compatible
unknown future-expand rule. No duplicate numeric minimum, API application import in
worker/shared package, source-tree runtime asset lookup or root API-asset-copy
pattern is permitted. API retains migration kinds, planner, upgrade commands
and non-startup compatibility CLI behavior. Retained resource paths, if needed,
are checked generated mirrors of the single owner.

Both API and standalone worker startup call the shared read-only live revision
and required-structure probe under each caller's own configured database
identity, before serving or producers. Worker completes the probe before
`build` and all boot effects, within a 30-second observation deadline covering
connection acquisition, revision read and both checks. Subsequent owned-engine
cleanup is separately bounded; this is not a strict 30-second whole-wall-time
claim. Missing/multiple version rows, incompatible known revision, unreadable/
malformed metadata, unavailable database, insufficient SELECT authority or
deadline exhaustion refuses without migration, mutation, enqueue or ordinary
fallback. Safe stable diagnostics contain no credential, DSN or exception text.

Revision compatibility alone does not prove ledger support. Independently
probe these actual required source columns using zero-row projections and
compatible catalog types in a read-only transaction: `curie.agents` (`id`,
`hook_generation`); `curie.hook_source_policies` (the eleven SOURCE-1 columns);
and `curie.hook_source_operations` (the seven SOURCE-10 columns). Require UUID
identities, the shipped INTEGER agent counter, BIGINT source generations,
supported textual fields and timestamp-with-time-zone times. Additive columns
remain compatible. A known-compatible or unknown-future stamp with missing or
unusable required structure refuses both startups. The configured schema only
locates version metadata; current source helpers/migrations still use `curie`.
This adds no custom-schema producer support and proves readable structure,
not provenance, all constraints, runtime authority or qualification.

Own the probe engine immediately and dispose it on every outcome. Worker
composition owns one separate same-DSN source-gate pool: size four, overflow
zero, checkout timeout 30 seconds, pre-ping. Register disposal before any later
construction can fail. Preserve READ COMMITTED and gate-before-work ordering;
gate waiters hold no work connection. Checkout timeout does not bound network
connect or advisory-lock wait.

Own the scheduled bundle reader's S3 client immediately after construction and
register its close before any later construction can fail. Close it through the
existing bounded synchronous transport adapter only after cron and its owned
reads join. This adds ownership of that reader, without changing other legacy
bundle-client lifecycles.

Cleanup stops and joins supervised cron before gate disposal, with a ten-second
stop/cancel/join budget. Thread-backed scheduled bundle reads keep an owned
task until the synchronous read finishes, even after supervisor cancellation.
Cancellation of the waiting coroutine is not thread completion. A read that
remains active keeps cron unjoined so the same producer budget reaches fatal
exit. Each subsequent owned engine/transport close has at
most five seconds, and an earlier close error cannot skip later attempts.
On cooperative paths retain the primary exception/cancellation; secondary
cleanup failures produce safe diagnostics, and failure without a primary cause
fails shutdown. Shield only an owned bounded cleanup task; do not detach
unbounded work. If producer join expires, perform process-level fatal exit
within one further second, preserving safe primary-cause diagnostics. Diagnostic
errors or blocked log sinks must not prevent that fatal exit. Owned cleanup
diagnostics are nonthrowing and bounded; an output still blocked after five
seconds uses the same fatal exit instead of leaving an unjoined diagnostic
task or thread. Merely
raising/returning to `asyncio.run` is insufficient. Do not dispose gate/work
engines under unjoined producers or claim successful termination/disposal or
normal exception propagation after fatal exit. Fatal-path tests run only in
an exclusively owned subprocess.
Worker close budgets use observed task deadlines, not only cooperative timeout
cancellation. If an owned worker closer remains live at its five-second
deadline, use the same bounded fatal exit; do not detach it or report cleanup
complete. A cooperative close error still attempts later independent resources
and preserves the primary cause.

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

New source administrative DTOs encode generation, expected_generation and
legacy_generation as canonical decimal strings. Accept `0` or positive decimal
without signs, whitespace, exponents or leading zeros, up to BIGINT maximum;
committed source generations are positive. IDs use canonical lowercase UUID
strings. The source bundle reference is the manifest bundle's bare lowercase
64-hex content digest. Request bodies forbid extra fields and coercion. DELETE
uses the same generation and operation-ID grammar in its query parameters.
These choices do not change the existing legacy secret response or the integer
generation in the scoped key's signed derivation bytes.

No-row output has generation `"0"`, ordinary mode, null policy references and
updated_at, closed activation, and the locked agent's current legacy counter.
Its ordinary admission status depends on SOURCE-10 attempt history. For both
absent and existing policies, the GET output reports the freshly locked agent
counter as `legacy_generation`, so another hook's activation cannot hide the
current ordinary credential replacement requirement. This output does not alter
the policy row's persisted counter snapshot used by SOURCE-6 fingerprints. Row
timestamps serialize in UTC. Stable reasons contain no exception text, broker
endpoint or credential; use `pending_history` for the absent-policy history
case. Administrative unknown agent is 404, malformed input is 422, rotation
of an ordinary or absent policy is 409, and exhausted generation space is 409.
The committed intent and historical operation-ID rules are owned by SOURCE-10.
A fresh rotation uses a fresh operation ID.

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

Before an ordinary-to-protected reservation, require the shipped agent
counter to be nonnegative and below 2147483647. Exhaustion refuses with 409
before broker or SQL mutation; an invalid stored counter closes with 503.
Do not widen, wrap or reset that counter as part of source administration.
The measured Postgres datatype and dependency versions are recorded in the
[dependency evidence](../../adr/evidence/0191-protected-hooks/README.md).

## Broker-authoritative activation and recovery

<!-- @spec PROTECTED-HOOK-SOURCE-6 -->
Maintain one private broker source-control record per agent/hook: monotone
`floor`, reserved `operation_id`, and optional active `{generation,
operation_id, mode, policy_fingerprint}`. This is authority metadata, not a
dispatchable copy. A source-control writer credential may change only this
family, cannot publish runtime evidence or read protected payload, and is
separate from consume authority. Workers/source signers cannot change it.
The policy fingerprint is SHA256 of compact sorted-key ASCII JSON containing
exactly `agent_id`, `hook`, `generation`, `operation_id`, `mode`, `tool_access`,
`runtime_id`, `qualification_id`, `bundle_digest` and `legacy_generation`.
UUIDs are canonical, counters/generations are decimal strings and nullable
fields are explicit null. `updated_at` is audit metadata and is excluded,
as are activation, readiness and current time. Use the row's committed legacy
counter snapshot, not a later counter from another hook's activation.

After the SOURCE-10 durable pending registration under the agent gate,
call atomic broker `reserve_and_revoke(expected_floor, operation_id,
min_generation)` with the exact registered allocation defined by SOURCE-10: clear active, allocate a generation strictly above both the
broker floor and committed SQL generation, and bind the reservation to the
operation. Repeated reservation of the same current operation is idempotent.
Persist that generation/operation and policy fields atomically with the
SOURCE-10 ledger transition and any legacy counter bump. SOURCE-10 owns
the work commits and gate release before broker publication. Publish active by
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
readiness and close admission. Source-floor reconciliation includes every
durable SOURCE-10 attempted generation, pending as well as committed; a
current-policy row alone is not the durable high-water mark. Reprovision a new independently issued runtime
epoch, clear readiness, reconcile source floors against durable SQL, and publish
only current rows after qualification. Do not treat an empty broker as generation
zero safe to activate, nor trust retained pre-reset proof. The
[ADR 0191 dependency evidence](../../adr/evidence/0191-protected-hooks/README.md)
records measured broker ACL, script/time, retained-session revocation and
Postgres commit-release behavior, and orderly AOF restart with stale-proof refusal.
Broker crash/rollback durability and SQL
disconnect recovery remain unmeasured; those observations do not qualify the
protected runtime or establish its provisioning boundary.

## Durable attempted-operation identity

<!-- @spec PROTECTED-HOOK-SOURCE-10 -->
The current policy and broker reservation remember only the current operation.
Add `hook_source_operations` as a separate additive table; keep the eleven
policy columns unchanged. Its primary key is `(agent_id UUID, hook VARCHAR(63),
operation_id UUID)`, with an agent foreign key and ON DELETE CASCADE. Columns
are `intent_sha256 CHAR(64) NOT NULL`, `status VARCHAR NOT NULL`,
`generation BIGINT NOT NULL > 0`, and `attempted_at TIMESTAMPTZ NOT NULL`
default now. The intent is lowercase 64-hex. Status is exactly
`pending|committed`. All attempted generations, including pending ones, are
unique per agent/hook. Identity, intent, generation and attempt time are
immutable; only pending-to-committed status transition is allowed, once.
Never delete or expire operation history except by agent deletion.

Intent is the lowercase SHA256 of compact sorted-key ASCII JSON with exactly
`mode`, `tool_access`, `runtime_id`, `qualification_id` and `bundle_digest`,
including explicit nulls. It is the desired target configuration: exclude HTTP
method, expected CAS, operation ID, generated counters and timestamps. The
additive migration backfills each existing current policy as committed, using
its target intent, generation and updated_at. No older history is reconstructed
or claimed; the foundation has no source administration wiring. Allocate the
new migration against the fresh base without rewriting prior migrations or
registered application windows. The wired schema minimum is that new head.

Under the agent gate, validate current policy/ledger consistency. A current
policy must have matching committed operation, generation and target intent;
inconsistency closes with 503 before a broker write. A matching current
committed operation and intent is replayed before stale-CAS rejection, without
a new generation, counter or timestamp. Different intent is 409. Any historical
committed operation or any pending operation is 409, even with fresh CAS.
Pending never authorizes publication or resumes a mutation as committed.
A fresh operation cannot resurrect an older UUID after another edit.

Validate expected refusals, references, counter ranges and trusted runtime
epoch before registration. Under the agent gate, allocate a generation one
above the maximum of current policy generation (zero if absent), all ledger
generations for this agent/hook, and the observed authenticated broker floor.
Reject BIGINT exhaustion before registration or broker writes. Commit that
positive pending generation through a separate work-pool transaction while
the outer agent gate remains held. Only after confirmed registration commit
call the SOURCE-6 reservation with expected_floor equal to the observed floor
and min_generation equal to the registered generation minus one. The returned
generation must equal the registered generation. Conflict, mismatch or failure
leaves the pending allocation consumed; never rewrite or delete it. Uncertain
registration commit returns 503 without a broker write; later locked read
and a fresh operation resolve recovery. Commit the policy at that registered
generation, required legacy counter bump and ledger committed status atomically
in a subsequent work transaction. Match pending status and immutable intent
and generation on transition.
SQL failure or uncertain authoritative commit returns unavailable; do not
assume rollback proved non-commit. A later locked read determines whether the
exact current operation committed. Release the outer gate only after that
authoritative transaction finishes, before broker CAS publication. A delayed
publisher cannot replace a later reservation. This adds no cross-store
transaction, activation proof or credential authority.

Absence of policy plus any attempted-operation history is closed, not never
configured. Closure begins at durable pending registration, even if reserve
failed or was never reached. Legacy preliminary authentication may succeed,
but fresh locked resolution returns 503 `pending_history` before any claim,
run mutation, quota, placeholder or enqueue. GET retains the no-row generation
zero and reports closed with that reason; it creates no protected key. All API
and worker named-hook producers check history presence. Pre-registration
validation or default resolver failure writes no history and preserves truly
never-configured ordinary behavior. A later successfully committed current
policy is governed by its exact current binding and private-receipt rules;
older pending history does not supersede it, although its UUID remains unusable.

Runtime selection is one immutable provisioner-owned deployment input, not a
platform-writable registry. Unknown references or another runtime ID are 422,
before any broker call, ledger registration or SQL write;
missing trusted broker identity, epoch or readiness is 503. Separate control
read and source-writer authority. Default resolution is unavailable. Missing
source keys do not prove a fresh epoch; require independently established
source-floor recovery, including pending ledger allocations. A new broker
epoch never permits reuse of a durably allocated source generation. A positive
pending generation grants no source key, activation or readiness authority.
Protected publication remains unavailable until the LANE-4 ingress
admission change, per [administrative route exposure](#administrative-route-exposure).
Pure record matching or a local clock check cannot establish activation.

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

The hook name is validated first (400), then the bounded raw body (413), as
on the delivery route. The strict `HookSupportIn` parse follows, because its
requested policy is part of the signed material; malformed input returns 422
before any database read. The purpose-prefixed signature is verified against
the current key read without the gate. Unlike the delivery route, a missing
delivery ID is reported (400) before the gate rather than under it, since the
probe has no admission step that needs the gate first; it is still reported only
after the signature succeeds. The gate-held reload and reauthentication precede
the snapshot read. Database or gate failure returns 503 `authority_unavailable`
without this DTO, because no current server resolution could be read.

`source_generation` and `runtime_generation` serialize as canonical decimal
strings, as the source administrative DTOs do. The gate-held snapshot resolves
the remaining members as follows:

| Snapshot | `effective_tool_access` | `source_generation` | `reason` |
| --- | --- | --- | --- |
| No row, no attempt history | requested | null | `source_unconfigured` |
| Ordinary tombstone row | requested | row generation | `source_closed` |
| No row, attempt history present | `read-only` | null | `source_closed` |
| Protected row | `read-only` | row generation | broker evaluation |

`source_closed` marks every state whose delivery ingress currently admits
nothing. A tombstone stays closed until broker confirmation of its ordinary
publication is available to ingress, per SOURCE-6 and SOURCE-8; its effective
member reports the ordinary resolution the row records. Pending history without
a committed row reports `read-only`, the most restrictive policy any pending
operation could commit, rather than inferring an ordinary resolution from
incomplete history. Runtime members stay null until an authenticated broker
evaluation supplies them; the policy row's own runtime, qualification and bundle
references are writable configuration and are never echoed. The status always
follows `supported`.

Broker evaluation of a protected row uses the API protected runtime bootstrap.
The setting `CURIE_PROTECTED_RUNTIME_DIR` names a directory that only the out of
band provisioner writes and mounts read only into the API. It holds
`manifest.json` (the trusted runtime manifest bytes), `ca.pem` (the broker CA
certificates), `bootstrap.json` and, for administration, the
`source_writer.json` defined under
[administrative route exposure](#administrative-route-exposure); the probe
reads only the first three. `bootstrap.json` is a strict object containing exactly
`schema_version: 1`, `max_readiness_ms` (a positive canonical decimal string)
and `control_reader: {username, password}` for the control reader principal.
No route, CLI verb or chart default creates, returns or mounts this directory
here, and it is never mounted into an ordinary worker or runner; provisioning
and its preventive guards remain LANE-8 work. The probe reads the files afresh
on each evaluation so a provisioner rotation needs no restart, and never logs
their content. An unset setting, or a missing, unreadable or invalid file,
evaluates to `runtime_unavailable`.

The probe releases the source gate, and ends its request database transaction,
before any broker I/O: the evaluation is observational, every delivery repeats
it, and no database connection may wait on the broker. Each API process runs at
most four broker evaluations at once; a probe beyond that limit reports
`broker_unavailable` without connecting rather than queueing. One evaluation
has a five second budget across connection and every read, and exceeding it
reports `broker_unavailable`. The budget starts after address resolution of the
manifest endpoint; name resolution is bounded by the host resolver, not by this
budget. A nested budget can only shorten an enclosing one. Bootstrap files are read relative to one opened
directory, must each be a regular file after symlink resolution, are opened
without blocking on special files, and are bounded in size; anything else makes
the bootstrap invalid. Validating `ca.pem` takes time linear in its size. It opens one
`AuthenticatedMetadataReader` from the bootstrap off the event loop, performs
the reads below on that connection, and closes it. The first failing step
decides the reason:

1. The bootstrap manifest's `runtime_id` differs from the policy row's:
   `configuration_unsupported` (one runtime per deployment, SOURCE-1).
2. The reader cannot connect, authenticate or confirm the manifest's live
   `run_id`, or any later read fails: `broker_unavailable`. The reader's single
   safe error does not distinguish these causes.
3. `read_source` has no active record, or its generation, operation, mode or
   `policy_fingerprint` differs from the committed row under SOURCE-6:
   `source_closed`.
4. `protected:control:selection:{runtime_id}` is absent or malformed, its
   `manifest_digest` differs from the bootstrap manifest, or the manifest
   control record differs from the bootstrap bytes: `runtime_unavailable`.
5. The selection's `broker_run_id`, or the run_id that `observe()` returns,
   differs from the manifest's: `broker_identity_mismatch`.
6. The selected qualification record is absent: `qualification_unavailable`.
7. The selected readiness record is absent: `evidence_missing`.
8. Broker time from `observe()` is at or after the readiness `expires_at_ms`:
   `evidence_expired`.
9. `validate_authority` refuses the manifest, qualification and readiness with
   the bootstrap `max_readiness_ms` and observed broker time, or the
   selection's runtime identifier, runtime generation, qualification identifier
   or qualification generation differs from them: `qualification_unavailable`.
10. The selection's qualification or the manifest's bundle digest differs from
    the policy row's references: `configuration_unsupported`.
11. The selection has `admission_open: false`: `runtime_unavailable`.

A control record that is present but malformed counts as absent at its own
step: selection or manifest at step 4, qualification at step 6, readiness at
step 7. Manifest comparisons use canonical bytes, so a parseable but
non-canonical `manifest.json` matches its canonical control record. Extra files
in the bootstrap directory are ignored. A `default` control reader username,
like any credential the reader refuses before connecting, makes the bootstrap
invalid. A committed row whose policy fingerprint cannot be computed returns
the 503 `authority_unavailable` refusal without this DTO. Runtime members are
reported only once steps 4 through 9 have validated the selected tuple. A row that passes every step still reports
`configuration_unsupported`, HTTP 503, until delivery ingress admits protected
deliveries under LANE-4; the probe must not claim support that ingress cannot
honor. Unconfigured, tombstoned and pending-history rows never open a reader.

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

## Administrative route exposure

One service path serves every caller. The administrative service exposes only
the operations these routes define; earlier internal methods whose answers this
section changes (absent-row removal without history, unrestricted reads) are
removed rather than kept beside the new ones, so a later CLI or console caller
cannot inherit a retired answer.

This section realizes the SOURCE-3 routes and the SOURCE-6/7/10 broker path
for the API. The [route exposure plan](../plans/2026-10-06-source-admin-routes.md)
orders the work. It extends the criteria above without changing their IDs and
applies the base SOURCE-10 rule that unknown references or another runtime ID
are 422.

The base below was inspected on origin/main `bcb2d3161`. A statement marked
pinned is asserted by an existing test against real Postgres and Valkey. The
two pinning suites, `apps/api/tests/test_hook_source_mutation.py` and
`apps/api/tests/test_hook_source_admin.py`, were rerun on 2026-10-05 against
local disposable stores: 59 passed and none skipped. Every other statement is
code inspection and is not a runtime observation.

* `apps/api/src/curie_api/hook_source_mutation.py::SourceMutationCoordinator`
  checks replay, operation history, stale CAS and exhaustion before resolving
  authority, then registers the pending generation, reserves, commits policy,
  counter and ledger together, releases the gate and publishes. Its publication
  branch refuses every protected row with 503 and the committed generation and
  publishes only ordinary tombstones. Pinned by
  `apps/api/tests/test_hook_source_mutation.py::test_enable_rotate_remove_reenable_preserves_counter_and_closed_authority`.
  No production `SourceAuthorityResolver` exists; tests supply a fake external
  authority over unauthenticated role clients. The resolver protocol carries
  no replay signal.
* The coordinator's `remove` accepts an absent policy with expected generation
  zero and creates a tombstone above every attempted generation. Pinned with
  pending history by
  `apps/api/tests/test_hook_source_mutation.py::test_all_pending_attempts_bound_fresh_generation_after_fake_external_recovery`.
  Four tests also start from an absent row with no history:
  `apps/api/tests/test_hook_source_mutation.py::test_current_ordinary_replay_bypasses_stale_cas_without_new_sql`,
  `apps/api/tests/test_hook_source_mutation.py::test_delayed_ordinary_cas_loses_after_new_operation_and_gate_is_released`,
  `apps/api/tests/test_hook_source_mutation.py::test_boundary_wait_cancellation_or_actual_gate_loss_precedes_registration`
  and
  `apps/api/tests/test_hook_source_mutation_commit_loss.py::test_authoritative_ordinary_commit_response_loss_never_publishes_until_exact_replay`.
* `apps/api/src/curie_api/hook_source_admin.py::SourceAdminService` reports
  closed activation for every row and refuses every mutation and secret read
  with 503. No route constructs it or the coordinator.
  `apps/api/src/curie_api/protected_support.py` imports the coordinator module,
  which imports the admin module, so the admin side cannot import the probe
  module's private bootstrap loader without a cycle.
* `packages/protected-hooks/src/curie_protected_hooks/source_fence.py::SourceFence`
  exposes `reserve_and_revoke`, `read` and `publish_ordinary` over a caller
  supplied synchronous client. No protected publication script exists.
* `packages/protected-hooks/src/curie_protected_hooks/broker_metadata.py::metadata_acl_rules`
  gives the source writer GET, SET and script operations on
  `protected:source:*` and neither INFO nor TIME.
  `packages/protected-hooks/src/curie_protected_hooks/broker_transport.py::AuthenticatedMetadataReader`
  sends INFO server on every connection and before every read and reports any
  run_id mismatch only as its single safe unavailable error. A connection's
  budget watchdog is fixed when the connection is created.
* `apps/api/src/curie_api/protected_support.py::_load_bootstrap` opens exactly
  `manifest.json`, `ca.pem` and `bootstrap.json` and accepts exactly three
  bootstrap members with `schema_version` 1.
* `apps/api/src/curie_api/hook_source_auth.py::authenticated_source` refuses,
  after authentication, every hook with a policy row or attempt history with
  503 before any claim, so delivery ingress admits nothing for a protected or
  tombstoned source.
* `apps/api/src/curie_api/routers/agents.py::get_hook_secret` serves the legacy
  agent key under the agents router's `apps/api/src/curie_api/auth.py::require_api_key`
  dependency, which accepts the platform key or a live console session with
  console origin enforcement; the session check reads the work database.
* The API source gate pool from
  `apps/api/src/curie_api/db.py::create_source_gate_engine` has four
  connections and no overflow, shared with ingress and the legacy secret route.

<!-- @spec PROTECTED-HOOK-SOURCE-3 -->
A new source policy router under the `/agents` prefix serves `GET`, `PUT` and
`DELETE` on `/agents/{agent_id}/hooks/{hook}/source-policy`,
`POST .../rotate` and `GET .../secret`. It uses the same `require_api_key`
dependency as the legacy secret route, so a platform key or a live console
session with console origin enforcement authenticates, as the console sibling
[#4054](https://github.com/curie-eng/curie/issues/4054) requires; a hook
signature never does. Authentication runs first and may read the console
session table. Request shape violations (agent UUID, hook name pattern, strict
body, strict query) then return FastAPI's ordinary 422 validation list before
any source database read. Handlers take no request database session; every
source database connection comes from the gate pool first and the work pool
second. Every refusal raised by the source services has the body
`{"detail": {"code": <stable code>, "committed_generation": <decimal string or null>}}`.
`committed_generation` is non null only after a confirmed authoritative commit
of the requested operation. It names a generation, never a key.

Protected publication stays unavailable in this slice. It moves to the LANE-4
ingress admission change tracked with
[#4075](https://github.com/curie-eng/curie/issues/4075), which owns the
publication evidence check, its shared evaluation with the probe, protected
and tombstone ingress admission under SOURCE-8, and the secret's active path.
This slice never produces an active protected source and never reopens
delivery for a configured hook.

Mutations follow one order. (1) Authentication and request shape 422. (2) An
unset runtime directory setting is 503 `runtime_unavailable`. (3) An
administrative executor slot, else 503 `broker_unavailable`. (4) The runtime
files, read once on that slot for the whole request, else 503
`runtime_unavailable`. (5) For PUT, any reference other than the deployment's
one runtime, meaning a `runtime_id`, `qualification_id` or `bundle_digest`
different from the manifest's `runtime_id`, `qualification_id` or bundle
`sha256`, is 422 `unknown_source_reference`. (6) The agent gate and locked
snapshot: unknown agent 404; then the coordinator's replay, operation history,
stale CAS and exhaustion checks; DELETE of an absent row without attempt
history is 409 `source_not_configured` immediately after the agent lookup;
rotate of an ordinary or absent row is 409 `source_rotation_conflict`, then the
current row's references get the step 5 check. (7) Registration, broker and
SQL effects. Every reference refusal therefore precedes any broker call,
ledger registration or SQL write, and applies equally to an exact replay whose
manifest has since changed; GET still shows that committed generation. DELETE
carries no reference, so it may tombstone a row that names another runtime.

PUT targets mandatory read-only with the body references. It registers,
reserves and revokes, commits the protected row with any SOURCE-5 counter
bump, and answers 503 `source_publication_deferred` with the committed
generation. That code means the commit happened, the agent's legacy counter
may have advanced, and the source stays closed until the LANE-4 change; it is
not a transient failure. Rotate keeps the current protected target and answers
the same way. After the LANE-4 change, an exact replay of that committed
operation publishes it when its reservation still matches; otherwise a fresh
rotation does. DELETE targets the ordinary tombstone. With pending history and
no row it commits a tombstone through the normal SOURCE-10 path, a fresh
operation at a generation above every attempt, without rotating the legacy
counter. A successful DELETE or exact DELETE replay publishes the tombstone and
returns 200 with `HookSourcePolicyOut` built from the committed row with
`activation: active` and null `refusal_reason`; its `legacy_generation` is the
row's committed counter. A tombstone is the state that restores ordinary
delivery under the current legacy key once tombstone ingress admission lands
in the LANE-4 change; until then ingress refuses it. The existing 404, 409 and
422 codes keep their meaning.

GET runs steps 1 and 6 for reading, releases the gate and ends its
transaction, then evaluates activation on an administrative slot without
database connections. Its 503 is `source_state_unavailable` and arises only
from gate or SQL failure. A hook with no row opens no broker connection: its
reason is null, or `pending_history` with attempt history. A protected row is
closed with `publication_deferred` and opens no broker connection. An ordinary
tombstone row is `active`, with null `refusal_reason`, only when one
authenticated reader session reads a source record whose floor and operation
equal the row's generation and operation and whose active record has that
generation, operation, mode `ordinary` and the SOURCE-6 fingerprint of the
committed row. Otherwise it is closed with the first applicable reason:
`authority_unavailable` (the fingerprint cannot be computed),
`runtime_unavailable` (setting unset or a file invalid), `broker_unavailable`
(connection, identity or read failure, a full executor or an exhausted budget)
or `source_closed` (any record mismatch). `activation` reports source
publication only, never delivery support; the support probe owns that question.

The secret route refuses in this slice and writes nothing: after steps 1 and
6, an absent row, attempt history alone or a tombstone is 409
`source_not_protected`, and a protected row is 503
`source_publication_deferred` with a null committed generation. Every handler
response carries `Cache-Control: no-store`. Authentication 401 and request
shape 422 come from dependencies before the handler and carry no source data.
No response, log, metric, trace attribute or error detail contains a source
key.

<!-- @spec PROTECTED-HOOK-SOURCE-6 -->
The API obtains the source writer principal from one additional provisioner
written file, `source_writer.json`, in the SOURCE-9 runtime directory. It is a
strict object with exactly `schema_version: 1` and
`source_writer: {username, password}`, read under the same descriptor, regular
file, size and duplicate member rules as `bootstrap.json`. The username must be
nonempty, differ from `default` and differ from the control reader username;
otherwise the file is invalid. It parses into a new frozen, slotted
`SourceWriterCredential(username, password)` whose representation redacts both
fields. Extending `bootstrap.json` was rejected: its strict v1 grammar refuses
another member or version, and a reader only deployment would then have to
carry writer credentials. One new API module owns loading the runtime files
for administration and the probe alike. It imports no source service or probe
module, which removes the import cycle; the probe module imports its loader
from it unchanged in behavior, and
[#4076](https://github.com/curie-eng/curie/issues/4076) later moves that
grammar into the shared package. The probe, GET and secret route never open
the writer file. No route, CLI verb, chart default, environment variable or
platform key creates, returns or derives it, and it is never mounted into an
ordinary worker or runner. Its absence leaves GET, secret and probe behavior
unchanged and makes every mutation unavailable at step 4.

A new `AuthenticatedSourceWriter.connect(manifest, credential, ca_pem)` beside
the metadata reader applies the same input validation, TLS, CA, hostname, SPKI
pin, RESP3 HELLO AUTH, two second timeouts, disabled retries, redaction and
`metadata_reader_budget` watchdog. It sends no INFO, because the writer role
has none, and it never reconnects: any connection loss makes it permanently
unusable. It exports only `reserve_and_revoke`, `publish_ordinary` and
`close`. Since the writer cannot see the live run_id, every writer effect is
bracketed by the control reader on the same pinned endpoint. A reader read
precedes it, and a reader read follows it and must show the effect. Because
the reader reports a changed run_id only as its safe unavailable error, a
broker restart or identity change detected around a writer effect is 503
`broker_unavailable` on this path; the support probe's reasons are unchanged.
A readable confirmation that does not show the effect is an uncertain effect.
The bootstrap control reader supplies those reads and `read_reconciled_floor`.
That floor is the validated source floor from a connection whose live run_id
equals the manifest's. Allocation above every durable ledger generation is the
independently established floor recovery that SOURCE-10 requires for
reservation. A missing key reading as floor zero therefore never reuses a
generation, and it never authorizes publication. Ordinary tombstone
publication needs no runtime tuple: it confirms revocation and SQL removal and
opens nothing while ingress refuses configured hooks.

Administrative broker work runs on its own executor of two threads, separate
from the probe's, and fails rather than queueing when no slot is free. A
mutation keeps its slot through publication; GET takes one only after
releasing the gate. A slot is released when its thread finishes, not when the
request ends. The gate phase has one five second deadline applied to the
reader and writer connections opened for it, covering their connect, floor
read, reservation and confirmation calls. SQL registration and the
authoritative commit run on work connections and are never cancelled by that
deadline; a deadline that passes during registration makes the following
broker call fail, leaving the pending generation consumed. Tombstone
publication after gate release opens fresh reader and writer connections under
its own five second deadline; a new connection is not a reconnect.
Cancellation never releases the gate while a writer call is in flight: the
gate waits until that call returns or its deadline ends.

The two slots bound administrative broker latency but not gate pool use.
While a mutation holds an agent gate for its deadline plus SQL time, ingress,
secret and legacy secret requests for that agent each hold a gate connection
waiting on the advisory lock, which no checkout timeout bounds. Two slow
mutations plus two such waiters can exhaust the four connection pool and stall
gated ingress for every agent for that long. This slice accepts that bound and
proves it with a paused owned broker; a bounded lock wait for ingress gate
waiters belongs to the ingress owner. Administrative requests bound their own
waits: a mutation, GET or secret request that has not acquired the agent gate
within five seconds answers 503 `source_state_unavailable` without registering,
reserving or writing, and a mutation releases its administrative slot when it
gives up.

<!-- @spec PROTECTED-HOOK-SOURCE-7 -->
Recovery uses the existing coordinator unchanged in order. An exact replay of
the current committed operation with the same intent, even with stale expected
generation, allocates nothing: a tombstone resumes its idempotent publication,
and a protected row answers 503 `source_publication_deferred` with its
committed generation again, decided from SQL alone without opening a broker
connection, so the answer does not depend on broker reachability. Different intent under that operation, any
historical operation and any pending operation are 409
`source_operation_conflict` with no broker call. A crash or failure before
reservation leaves pending history only; the source closes, and recovery is a
fresh operation, including a DELETE that commits a tombstone. A failed or
unconfirmed reservation returns 503 before the SQL commit with a null
committed generation. An uncertain authoritative commit also returns 503 with
a null committed generation; a later GET or exact replay decides it. After a
confirmed tombstone commit, publication failure returns 503 with the committed
generation and the first failing reason. When a readable reader record shows
that the committed tombstone's reservation is gone, through broker reset,
restored snapshot or a provisioner change, the reason is
`source_reservation_lost` and only a fresh DELETE operation recovers,
allocating above every durable attempt. A delayed publisher loses its CAS to
any later reservation. This slice adds no automatic reconciliation loop.

<!-- @spec PROTECTED-HOOK-SOURCE-10 -->
While no runtime is provisioned, meaning the directory setting is unset, a
file is missing or invalid, or the writer file is absent, routes stay closed
without history. GET reports the closed resolution above. Every mutation
returns 503 `runtime_unavailable` at step 2 or 4, before the gate, any SQL
read, pending registration or broker call, with a null committed generation;
it therefore precedes the 404, 409 and reference checks, which need the gate
or the manifest. Request shape 422 still comes first. A full administrative
executor is 503 `broker_unavailable` at step 3; with the setting unset no slot
is taken. Failed reader or writer connection and an exhausted deadline refuse
before registration when they occur before it.

Out of scope here: protected publication, the secret's active path, LANE-4
protected and tombstone ingress admission and the probe's `supported` answer,
all in the change tracked with
[#4075](https://github.com/curie-eng/curie/issues/4075); the worker lane;
provisioning, bootstrap writing and the shared bootstrap grammar
([#4076](https://github.com/curie-eng/curie/issues/4076), LANE-8); automatic
floor reconciliation; and the CLI and console siblings
([#4053](https://github.com/curie-eng/curie/issues/4053),
[#4054](https://github.com/curie-eng/curie/issues/4054)). The parity seam rule
is met by naming those siblings: no CLI structure mirrors these DTOs and no
console action is added, so no gate requires them in this slice. OpenAPI is
regenerated by its existing generator.

## Ingress admission wiring

This section realizes the LANE-4 ingress change that the route exposure
section defers: protected delivery admission on the signed hook route, the
SOURCE-8 receipts across both stores, tombstone ingress, the SOURCE-6 active
protected publication, the secret's active path, the probe's `supported`
answer on one authority evaluation shared with admission
([#4075](https://github.com/curie-eng/curie/issues/4075)) and the API
admission reconciliation owner. It extends the criteria above, the
[lane contract](2026-10-02-protected-hook-lane.md#atomic-admission-duplicate-receipt-and-activation)
and the [admission contract](2026-10-03-protected-hook-admission.md#ingress-wiring)
without changing their IDs, under accepted ADR 0191. The
[ingress admission plan](../plans/2026-10-06-ingress-admission.md) orders the
work. It targets `next`.

The base below was inspected on origin/next `00c421da5`. Every statement is
code inspection, not a runtime observation.

* `apps/api/src/curie_api/hook_source_auth.py::authenticated_source` refuses
  every snapshot that is not never configured with 503 before any claim, so
  protected and tombstoned sources admit nothing.
* `apps/api/src/curie_api/routers/hooks.py::ingest_hook` claims the ordinary
  key `HOOK_KEY_PREFIX:delivery:{agent}:{hook}:{sha16}` on the ordinary Valkey,
  takes the ordinary per agent backlog slot through
  `apps/api/src/curie_api/delivery.py::take_backlog_slot`, may select a writable
  workspace through `crud_workspaces.select_thread_workspace`, and builds the
  turn with `apps/api/src/curie_api/routers/hooks.py::_mint_turn`, whose
  placeholder comes from the request and whose `received_at` is API wall time.
  `apps/api/src/curie_api/routers/hooks.py::HookAccepted` has no requested
  policy, effective policy, source generation or acceptance status member.
* `apps/api/src/curie_api/protected_support.py::_decide` ends a fully valid
  tuple with `configuration_unsupported`. Its reasons differ from
  `packages/protected-hooks/src/curie_protected_hooks/atomic_admission.py::AtomicAdmission`,
  whose `_authority` validates with the readiness `issued_at_ms` standing in
  for broker time and leaves time to the script, never compares the control
  manifest bytes to a trusted manifest, and receives only a broker identity.
* `packages/protected-hooks/src/curie_protected_hooks/atomic_admission.py::AtomicAdmission._operate`
  answers conflict for a preparing original whose retried payload digest
  differs, and reads its records with separate GETs. The torn read fix
  [#4094](https://github.com/curie-eng/curie/pull/4094) (commits `1423b5133`,
  `487378808`, `45440e152`, merged as `a9b587075`) landed on main only and is
  absent from next.
* `packages/protected-hooks/src/curie_protected_hooks/broker_transport.py` has
  `AuthenticatedMetadataReader` and `AuthenticatedSourceWriter` and no enqueue
  transport. `packages/protected-hooks/src/curie_protected_hooks/admission_acl.py::admission_acl_rules`
  grants the enqueue role ZADD, ZCARD, ZREM and ZSCORE on the quota key and no
  ZRANGE.
* `packages/protected-hooks/src/curie_protected_hooks/source_fence.py::SourceFence`
  publishes only ordinary records.
  `apps/api/src/curie_api/hook_source_mutation.py::SourceMutationCoordinator`
  answers every committed protected row with `source_publication_deferred`.
* `apps/api/src/curie_api/routers/hook_fire.py` and
  `apps/worker/src/curie_worker/hook_source_guard.py` refuse every configured
  hook before a run claim. `apps/api/src/curie_api/main.py::lifespan` starts no
  protected task.

The existing internal library realizes the broker transaction. This section
decides only what the specifications left open for wiring it.

<!-- @spec PROTECTED-HOOK-SOURCE-6 -->
**Enqueue principal.** The API obtains the LANE-3 enqueue principal from one
more provisioner written file in the SOURCE-9 runtime directory,
`enqueue.json`. It is a strict object with exactly `schema_version: 1`,
`credential_ref: {id, generation}` and `enqueue: {username, password}`, read
under the descriptor, regular file, size and duplicate member rules of
`bootstrap.json`. `credential_ref` must equal the manifest's
`credential_refs.enqueue` exactly, so a stale file after a provisioner rotation
is invalid rather than silently used. The username must be nonempty, differ
from `default` and differ from the control reader username. It parses into a
new frozen, slotted `EnqueueCredential(username, password)` beside the reader
and writer credentials, whose representation redacts both fields. The
existing runtime file module gains one loader returning the bootstrap plus
this credential; the writer file is never opened by it, and the administrative
loader never opens `enqueue.json`. No route, CLI verb, chart default,
environment variable or platform key creates, returns or derives it, and it is
never mounted into an ordinary worker or runner. Extending `bootstrap.json`
was rejected for the same reason as the writer file: its strict v1 grammar
refuses another member, and a probe or administration only deployment would
then carry enqueue authority.

<!-- @spec PROTECTED-HOOK-SOURCE-2 -->
**Connections and gate.** Protected ingress uses three connections and no new
pool. The request session and the existing four connection gate pool carry
the gate-held reload, reauthentication and snapshot read; a protected delivery
writes no SQL. The ordinary Valkey client carries only the ordinary claim
lookup below. One fresh `AuthenticatedEnqueueClient` connection per delivery
carries every protected broker read and the admission script; it is opened
after the gate-held reauthentication, under one five second budget covering
connection and every call, and always closed. It never reconnects. Admission
runs on its own executor of two threads that fails rather than queueing, so at
most two gate connections wait on broker I/O for ingress at any time. A
request cancelled during a broker call keeps the gate until that call returns
or its budget ends, as the administrative slot does. When the ungated
authentication found a protected row, signed delivery ingress acquires the
agent gate with a five second advisory lock bound and answers 503
`authority_unavailable` when the bound passes; this is the ingress owner's
bounded wait that the route exposure section names. Every other signed
delivery, including never configured and tombstoned hooks, keeps today's
unbounded gate wait, so ordinary ingress behavior is unchanged. The
gate-held reload still decides the path, so a row that changed while the
request waited is resolved under the lock as before. Gate pool checkout itself stays bounded only by the pool
timeout until [#4091](https://github.com/curie-eng/curie/issues/4091). The
SOURCE-2 rule that the gate is held through admission is unchanged: the
admission script runs while the gate is held, and the gate is released only
after the response is decided.

<!-- @spec PROTECTED-HOOK-LANE-4 -->
**Protected turn construction.** A protected delivery's `QueuedTurn` needs no
placeholder, workspace or SQL write. After authentication and the delivery ID
check, a protected source refuses a caller supplied `conversation_id` or
`placeholder` with 422 `protected_reply_target_unsupported`, so no turn joins
a preposted message in an existing thread. The reply surface is selected by
the existing kind, address and adapter rules and their existing 422, 404 and
409 answers. The conversation is the synthetic
`hook_conversation_id(agent.id, hook, partition)` with the existing partition
rule. An agent whose `source_bindings` is not empty refuses with 503
`configuration_unsupported`, decided from configuration rather than from the
body, so no protected delivery reaches a workspace selection and the probe can
decide the same condition. The turn has source `WEBHOOK`, author
`hook:{hook}`, the existing event ID, the existing delivery text with no
mapping block, `tool_access` `read-only`, no attachments and a reply handle
with a null placeholder. Its exact JSON bytes above the ADMISSION-2 limit of
262144 refuse with 413 before any broker I/O. The ordinary per agent backlog
slot is not taken and no ordinary claim is written. Cron and manual fire keep
refusing configured hooks; their private routing is not part of this change.

<!-- @spec PROTECTED-HOOK-SOURCE-8 -->
**Receipts and duplicates across both stores.** The signed route's steps for
a protected row are, in order: hook name 400, bounded body 413, ungated
authentication 401, bounded gate and gate-held reauthentication 401, delivery
ID 400, reply target and surface checks, source binding refusal, an unset
setting or invalid runtime or enqueue file 503 `runtime_unavailable`, a full
admission executor 503 `broker_unavailable`, the ordinary claim lookup, turn
construction and payload bound, then admission. The ordinary claim lookup
reads the ordinary key for the same agent, hook and delivery ID; any value,
pending or a stream ID, answers 409 `delivery_conflict` without a broker
write, because a prior ordinary claim prevents a private enqueue. The
admission request carries the decoded delivery ID header, the committed row's
SOURCE-6 fields, the requested policy as signed, the SHA256 of the exact raw
body and the exact turn bytes. The live gate is probed before the ordinary
lookup and before admission.

A tombstone row authenticates with the current legacy key, then, before any
ordinary claim, quota or workspace effect, requires valid runtime and enqueue
files and one enqueue connection that reads the source record and the
delivery's private intent key. The record must hold an active ordinary
publication whose generation, operation and fingerprint equal the committed
row; otherwise 503 `source_closed`. A present private intent answers 409
`delivery_conflict`; a broker failure answers 503 `broker_unavailable`. The
connection closes before the unchanged ordinary path runs under the same
gate. Never configured hooks open no broker connection and keep every current
answer. Pending history keeps 503 `pending_history`.

`HookAccepted` gains `requested_tool_access`, `effective_tool_access`,
`source_generation` (a canonical decimal string or null) and
`acceptance_status` (`accepted`, `pending` or `preparing`), keeping
`tool_access` as the effective alias. Ordinary answers report the requested
policy as both policies, `pending` on the existing 202 and a null source
generation, except that a fresh tombstone acceptance reports the committed
row's generation. An ordinary duplicate reports a null generation, because
the ordinary store never recorded one and a duplicate is never relabeled.
Protected admission results map as follows:

| Admission result | HTTP | Answer |
| --- | --- | --- |
| accepted | 200 | Receipt: `duplicate` false, stream ID, conversation, `accepted`. |
| duplicate | 200 | The original receipt: `duplicate` true, original stream ID, conversation and generation. |
| preparing | 202 | Null stream ID and conversation, `preparing`, the request's matching tuple. |
| failed | 409 | `protected_delivery_failed`; the delivery ID never admits again. |
| conflict | 409 | `delivery_conflict`. |
| refused, `quota_full` | 429 | `protected_backlog_full`, with `Retry-After` of the hook backlog window. |
| refused, other reason | 503 | The admission reason as the detail. |
| unavailable, budget, connection or identity failure | 503 | `broker_unavailable`. |

<!-- @spec PROTECTED-HOOK-LANE-4 -->
**Quota.** Protected backlog is the broker's global quota set only. Its limit
is a fixed 64 members in this release, equal to the ordinary per agent
default; it is capacity, not authority, so it is an API constant rather than a
provisioner file member. This global limit stands for the first release; per
agent fairness among protected sources belongs to the protected worker lane. Members of committed deliveries are released only by
the future protected worker's completion under LANE-6; failed intents release
theirs through ADMISSION-5 cleanup.

<!-- @spec PROTECTED-HOOK-LANE-4 -->
**Reconciliation owner.** One `ProtectedAdmissionReconciler` task per API
process owns preparing intents. The lifespan starts it inside the source
resource scope after the source gate and stops it before the Valkey client
and engines close: a stop signal, then cancellation and a ten second join;
an in flight broker call finishes under its own budget on the reconciler's
single dedicated thread, never an ingress or probe thread. Each tick starts
five seconds after the previous tick ends. A tick with the setting unset does
nothing; otherwise it loads the runtime and enqueue files afresh, and an
invalid file skips the tick without broker I/O. A valid tick opens one enqueue
connection under a five second budget, lists preparing intents through the
admission facade and calls its `recover` for each until the budget ends,
leaving the rest to the next tick. It holds no SQL gate and fabricates no HTTP
authentication: recovery checks the original source authority in the broker,
which every SQL removal or rotation has already revoked. Every API replica
runs its own reconciler. Attempts count per intent across replicas, so with
several replicas the ten attempt bound can be reached sooner than fifty
seconds; the 300 second broker time deadline is unchanged. Each tick logs only
counts and a safe outcome, never identities beyond the delivery digest, a
payload or a credential. No Postgres bookkeeping is recorded after broker
acceptance in this release.

<!-- @spec PROTECTED-HOOK-LANE-4 -->
**Without a protected worker.** LANE-6 and LANE-7 are out of scope here, so no
consumer reads the protected stream. An admitted payload parks privately on
the protected broker with its binding, receipt and quota membership; it is
never copied to the ordinary store and never dispatched. Once 64 deliveries
are admitted the quota answers 429 until the worker lane releases members.
This creates no path to `supported` without a runtime: the probe and admission
still require a selection, a qualification record and current readiness,
which LANE-2 lets only the separately credentialed verifier write after the
full qualification campaign that needs the protected worker and LANE-8
guards. Tests seed those records with a fixture administrator; that is test
setup, never qualification.

<!-- @spec PROTECTED-HOOK-SOURCE-9 -->
**One authority evaluation.** A new pure module in the internal package,
`authority_evaluation`, owns the decision both paths make over one set of
reads: the source record, the selection, manifest, qualification and
readiness control bytes, and one broker observation. Its input target is the
source generation, operation, policy fingerprint, runtime ID, qualification ID
and bundle digest; admission builds it from the request's committed row and
recovery from the original intent and binding. It returns a closed outcome:
`accept`, `source_closed`, `runtime_unavailable`, `broker_identity_mismatch`,
`qualification_unavailable`, `evidence_missing`, `evidence_expired`,
`configuration_unsupported` or `admission_closed`, decided in the existing
probe step order: step 1, then steps 3 through 11, with step 11 yielding
`admission_closed`. The probe replaces `_decide` with it and maps outcomes to
its reasons one to one, except `admission_closed`, which stays the probe's
`runtime_unavailable`. Admission's Python preflight calls the same function
with an observation read on its enqueue connection, then its script compares
the exact snapshots and rechecks run_id and expiry against live broker time
atomically. A frozen table in the module maps outcomes to admission reasons:
`source_closed` to `source_unavailable`, `configuration_unsupported` and
`runtime_unavailable` to `runtime_unavailable`, both evidence outcomes to
`evidence_unavailable`, and the rest unchanged. The facade receives the
trusted manifest rather than a bare broker identity, so admission also
requires the control manifest bytes to equal the provisioner manifest, as the
probe does.

<!-- @spec PROTECTED-HOOK-SOURCE-9 -->
**The `supported` answer.** The probe answers HTTP 200 with `supported` true,
reason `supported` and all runtime members if and only if, in one request:
the gate-held snapshot holds a committed protected row whose fingerprint
computes; the agent's `source_bindings` is empty; the setting is set and the
runtime files are valid; one control reader session yields `accept`; and
`enqueue.json` is valid and bound to the manifest. The source binding check
is step 1a, after step 1 and before broker I/O, reporting
`configuration_unsupported` without runtime members. The enqueue file check is
step 12, after step 11 passes, reporting `runtime_unavailable` with runtime
members; the probe parses that file and never connects with it. Every other
outcome is the existing 503 DTO. The final `configuration_unsupported` of the
base contract is removed. The probe still reads with the control reader, so
an enqueue principal that the broker refuses makes ingress answer
`broker_unavailable` while the probe reported support; that is an availability
fault, not a tuple difference. `supported` does not cover per delivery
conditions: quota, an existing receipt or conflict, the ordinary claim, the
reply surface, the body bound, executor slots and broker reachability. Under
those exclusions and with no record or time change between them, a delivery is
accepted exactly when the probe reports `supported`; the parity test drives
both paths over the same broker states.

<!-- @spec PROTECTED-HOOK-SOURCE-6 -->
**Protected publication.** `SourceFence` gains `publish_protected`, a sibling
CAS script that sets the active protected record only when floor and
operation equal the committed row and no other active record exists, and is
idempotent for the same record. `AuthenticatedSourceWriter` exports it; the
writer role already covers it. Because the writer cannot read control records
or time, the evidence check is reader bracketed after gate release, on fresh
reader and writer connections under one five second deadline: the reader
confirms the reservation, reads the control records and one observation, and
the shared evaluation in its publication phase must return `accept` or
`admission_closed`. Its publication phase replaces the active record check with
the reservation check. Then the writer publishes, and a reader read must show
the active protected record. A refusal answers 503 with the committed
generation and the outcome as its code (`runtime_unavailable`,
`broker_identity_mismatch`, `qualification_unavailable`, `evidence_missing`,
`evidence_expired` or `configuration_unsupported`), or `source_reservation_lost`
or `broker_unavailable`. This check is not atomic with the CAS and need not
be: publication admits nothing, and every delivery repeats the evaluation
atomically. Success answers 200 with `activation: active`. An exact replay of
a committed protected operation now publishes when its reservation still
matches. `source_publication_deferred` and GET's `publication_deferred` are
retired.

<!-- @spec PROTECTED-HOOK-SOURCE-3 -->
**GET and secret.** GET reports a protected row `active`, with a null reason,
on the same rule and reasons as an ordinary tombstone with mode `protected`.
The secret route, after steps 1 and 6 and gate release, serves
`HookSourceSecretOut` for a protected row only when one reader session shows
its active protected record; otherwise 503 with `source_closed`,
`runtime_unavailable` or `broker_unavailable` and a null committed generation.
Absent, history only and tombstone rows keep 409 `source_not_protected`. A
rotation that commits after that read makes the returned key already revoked,
which authenticates nothing. Every handler response keeps `no-store`.

**Out of scope and its end to end consequence.** The protected worker lane
(LANE-6, LANE-7), provisioning and the shared bootstrap grammar (LANE-8,
[#4076](https://github.com/curie-eng/curie/issues/4076)), private cron
routing, the CLI and console siblings
([#4053](https://github.com/curie-eng/curie/issues/4053),
[#4054](https://github.com/curie-eng/curie/issues/4054)) and #4091 stay out.
End to end evidence therefore stops at the broker: real signed HTTP delivery
to a private stream entry and receipt, with fixture seeded authority on a
disposable broker. No protected turn reaches a worker, runner, model or reply,
read only enforcement is not observed on a runner, and no provisioner supplies
a runtime to a running stack. #3603 stays open and the installation gate stays
closed.

## Acceptance cases and commands

Each test/implementation unit cites its corresponding ID above. Required cases:

* SOURCE-1/2: migration preserves existing agents, constraints refuse invalid
  protected/ordinary rows; first-policy activation vs ordinary enqueue is
  serialized; no-row race and reload after lock wait; invalid signatures never
  acquire the advisory lock; valid preliminary authentication followed by key
  rotation while waiting fails authoritative reauthentication; cron/fire cannot bypass.
* SOURCE-2/10: standalone worker schema and cron campaigns resolve their configured
  database through the existing settings owner and pass without a preceding API
  test setting `DATABASE_URL`. Templates and clones remain real, isolated and owned.
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
* Route exposure (SOURCE-3/6/7/10): over real HTTP, real Postgres and a
  disposable TLS broker with distinct writer and reader principals, PUT and
  rotate commit and answer 503 `source_publication_deferred` with the
  committed generation while GET stays closed; DELETE publishes a tombstone,
  including from pending history without a counter change; replay and
  recovery leave exact durable state; writer and reader credentials cannot do
  each other's work; no runtime, a missing writer file and a foreign reference
  refuse before any SQL or broker effect; a broker restart around a writer
  effect is `broker_unavailable`; lost reservation refuses with the committed
  generation; DELETE of an absent row without history is 409; the secret route
  refuses every state and no response or log contains a source key.
* Ingress admission wiring (SOURCE-2/3/6/8/9, LANE-4): over real HTTP, real
  Postgres, the ordinary Valkey and a disposable TLS broker with distinct
  enqueue, reader and writer principals, a signed protected delivery yields
  exactly one private entry, binding and receipt and nothing in the ordinary
  store or database; an exact retry, a freshly signed retry and a retry after
  readiness closure return the original receipt; a changed body, requested
  policy or generation conflicts; a prior ordinary claim conflicts without a
  broker write; a tombstone admits only with its ordinary publication active
  and refuses a delivery ID with a private intent; every refusal row of the
  result table answers as specified with no write; the probe reports
  `supported` exactly when admission accepts over the same broker states;
  protected PUT, rotate and replay publish only with current evidence; the
  reconciler finishes, fails and refunds preparing intents with no caller
  retry; the enqueue file, its binding to the manifest and its absence behave
  as specified; and no response, log or error contains a credential, payload
  or source key.

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

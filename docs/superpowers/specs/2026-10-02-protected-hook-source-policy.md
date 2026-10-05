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
platform-writable registry. Unknown references or another runtime ID are 422;
missing trusted broker identity, epoch or readiness is 503. Separate control
read and source-writer authority. Default resolution is unavailable. Missing
source keys do not prove a fresh epoch; require independently established
source-floor recovery, including pending ledger allocations. A new broker
epoch never permits reuse of a durably allocated source generation. A positive
pending generation grants no source key, activation or readiness authority.
Protected publication remains unavailable until the actual atomic authority
path is implemented. Pure record matching or a local
clock check cannot establish activation.

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
band provisioner writes and mounts read only into the API. It holds exactly
`manifest.json` (the trusted runtime manifest bytes), `ca.pem` (the broker CA
certificates) and `bootstrap.json`, a strict object containing exactly
`schema_version: 1`, `max_readiness_ms` (a positive canonical decimal string)
and `control_reader: {username, password}` for the control reader principal.
No route, CLI verb or chart default creates, returns or mounts this directory
here, and it is never mounted into an ordinary worker or runner; provisioning
and its preventive guards remain LANE-8 work. The probe reads the files afresh
on each evaluation so a provisioner rotation needs no restart, and never logs
their content. An unset setting, or a missing, unreadable or invalid file,
evaluates to `runtime_unavailable`.

The probe releases the source gate before any broker I/O: the evaluation is
observational, and every delivery repeats it. It opens one
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
   the bootstrap `max_readiness_ms` and observed broker time, or the selection
   generations do not match them: `qualification_unavailable`.
10. The selection's qualification or the manifest's bundle digest differs from
    the policy row's references: `configuration_unsupported`.
11. The selection has `admission_open: false`: `runtime_unavailable`.

A control record that is present but malformed counts as absent at its own
step: selection or manifest at step 4, qualification at step 6, readiness at
step 7. Manifest comparisons use canonical bytes, so a parseable but
non-canonical `manifest.json` matches its canonical control record. Extra files
in the bootstrap directory are ignored. A `default` control reader username,
like any credential the reader refuses before connecting, makes the bootstrap
invalid. Runtime members are reported only once steps 4 through 9 have
validated the selected tuple. A row that passes every step still reports
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

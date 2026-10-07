# Protected hook atomic admission foundation

This realizes the broker transaction and recovery primitive in
[LANE-4/5/6](2026-10-02-protected-hook-lane.md#atomic-admission-duplicate-receipt-and-activation)
and [SOURCE-6/7/8/10](2026-10-02-protected-hook-source-policy.md#broker-authoritative-activation-and-recovery)
under accepted [ADR 0191](../../adr/0191-protected-hook-delivery-authority.md).
Tracked by [#3603](https://github.com/curie-eng/curie/issues/3603).
It remains unwired. It does not publish protected source activation, start a
worker, qualify a runtime, implement preventive guards or clear installation.
The five second API reconciliation supervisor, HTTP integration, ordinary
receipt exclusion under the SQL gate and worker verification remain the parent
plan's tasks 3/4. The worker inventory remains its current owner's work.

## Closed operation boundary

<!-- @spec PROTECTED-HOOK-ADMISSION-1 -->
`AtomicAdmission(client: redis.Redis, *, broker_identity: BrokerIdentity,
trusted_max_readiness_ms: int, backlog_limit: int)` receives an explicitly
supplied enqueue scoped client authenticated to database 0. Trusted connection
construction must independently authenticate TLS server identity and SPKI under
LANE-2; this facade does not construct connections or establish that guarantee.
It acquires no credentials, reads no environment settings and exports no raw
client, generic command, source publication or worker operation. It does not
close the caller owned client. `backlog_limit` is a strict integer from 1 through
2147483647; `trusted_max_readiness_ms` is a strict integer from 1 through
9007199254740991. Broker identity uses the existing LANE-2 shape.

Expose `admit(request: AdmissionRequest) -> AdmissionResult` and
`recover(identity: DeliveryIdentity) -> AdmissionResult`. The latter is the
bounded primitive for an authenticated caller retry or the trusted future API
reconciler. Neither authenticates HTTP or acquires a SQL gate. Caller retry
must first authenticate the current source and pass its current policy into
admit. Trusted reconciliation cannot fabricate an authenticated HTTP retry.
No successful duplicate result means a new turn was admitted.

Inputs/results are immutable closed records. Invalid input refuses before I/O.
Expected invalid broker data, command/connection/authentication failures and
malformed responses return a safe `AdmissionUnavailable` exception with only
`protected admission unavailable`; exceptions, repr and diagnostics exclude
credentials, endpoints, raw broker replies and payload. Refusal/conflict are
closed results, never raw dependency exceptions.

## Request and key domain

<!-- @spec PROTECTED-HOOK-ADMISSION-2 -->
Reuse LANE-2 UUID, generation, millisecond, digest and canonical JSON grammar.
Records reject duplicate fields recursively, unknown fields, nonfinite numbers,
invalid UTF-8 and coercion. Immutable byte storage follows authority_records.
Admission metadata, envelope, intent, status and receipt each have a 16384 byte
canonical limit. Exact queued payload bytes are separately limited to 262144
bytes for this internal protected operation; ordinary public ingress limits do
not change. Unsupported protected input refuses before effects.

`DeliveryIdentity` contains exactly `agent_id: UUID`, `hook` under SOURCE-1's
63 character decoded grammar and `delivery_id: str`, a nonempty UTF-8 string of
at most 1024 encoded bytes without C0 controls or DEL. The delivery digest `d`
is SHA256 of canonical ASCII JSON `[agent_id, hook, delivery_id]`. It excludes
source/runtime generations. No truncated digest or ambiguous concatenation is
used. `AdmissionRequest` contains exactly `identity: DeliveryIdentity`,
`source_policy` (the existing closed SOURCE-6 fingerprint fields, without audit
metadata), `requested_tool_access: null | "read-only"`,
`request_body_sha256: sha256`, and `queued_payload: bytes`. Effective access is
always `read-only`; ordinary policies refuse. The caller supplies an already
constructed unchanged QueuedTurn; its parser must accept it, its effective
`tool_access` must equal `read-only`, and the source policy agent/hook must agree with DeliveryIdentity. Its event_id
is an ACI string restricted here to opaque_ref for the closed protected
operation; unsupported values refuse without changing ACI or ordinary ingress; its
conversation_id is the logical conversation key. It requires an existing
non-null reply_handle and no attachments; unsupported targetless or attachment
configuration refuses. Eligible source is exactly existing TurnSource.WEBHOOK
or TurnSource.CRON; cron hook_run agent_id/name must match DeliveryIdentity.
Ordinary human/slack source cannot inject a protected delivery. Requested policy is authenticated upstream and is
never overwritten by effective policy. The body digest is of exact raw signed
bytes, excluding signature/timestamp headers. The facade does not possess the
source key or claim to verify the raw body without those upstream inputs.

Closed broker keys are:

| Key | Type and owner |
| --- | --- |
| `protected:source:{agent_id}:{hook}` | Existing source string, source writer only. |
| `protected:control:selection:{runtime_id}` | Provisioner string selecting one epoch. |
| `protected:control:manifest:{manifest_digest}` | Immutable manifest string, provisioner. |
| `protected:control:qualification:{qualification_id}:{qualification_generation}` | Immutable qualification string, provisioner. |
| `protected:control:readiness:{runtime_id}:{runtime_generation}` | Readiness string, verifier. |
| `protected:admission:intent:{d}` | Immutable original preparation string. |
| `protected:admission:state:{d}` | Monotonic preparation/failure string and attempt count. |
| `protected:admission:commit:{d}` | Immutable committed State with original accepted Receipt. |
| `protected:admission:recovery:{d}` | Exact payload bytes while preparation needs them. |
| `protected:admission:binding:{event_id}` | Immutable envelope bytes. |
| `protected:admission:quota` | Global protected backlog ZSET; member `d`, score created broker ms. |
| `curie:runs` | Stream on this separate broker only. |

Selection contains exactly `schema_version: 1`, `runtime_id: UUID`,
`runtime_generation: generation`, `manifest_digest: sha256`,
`qualification_id: UUID`, `qualification_generation: generation`,
`broker_run_id` under LANE-2 run_id grammar, and `admission_open: bool`.
Selection is a provisioner controlled input after SOURCE-7 epoch/source-floor
recovery; its presence is not proof that recovery or guard qualification was
performed. This change exports no writer for it. The selected tuple must match
source_policy runtime/qualification/bundle and independently trusted broker
identity. No worker snapshot or caller chosen record substitutes for control.

## Envelope, intent and result records

<!-- @spec PROTECTED-HOOK-ADMISSION-3 -->
`Envelope` contains exactly `schema_version: 1`, `event_id: opaque_ref`,
`source_revision: generation`, `runtime_id: UUID`,
`runtime_generation: generation`, `manifest_digest: sha256`,
`qualification_id: UUID`, `runner_image_digest: oci_digest`,
`bundle_digest: sha256`, `execution_config_digest: sha256`,
`logical_conversation_key: str`, `execution_session_key: str`, and
`payload_sha256: sha256`, plus the optional `remediation_generation` described
below (Intent and Receipt likewise). Logical key is the unchanged QueuedTurn conversation
ID, a nonempty string of at most 1024 UTF-8 bytes without C0 controls/DEL.
Execution key is `protected:{runtime_id}:{runtime_generation}:{h}`, where `h`
is SHA256 of canonical ASCII JSON `[runtime_id, runtime_generation,
logical_conversation_key]`. The library derives this from trusted selection;
a source does not choose it. A future worker independently derives and checks
it before using its private execution domain under LANE-7.

Envelope, Intent and Receipt also carry `remediation_generation`
(AUTOMATED-REMEDIATION-4 of
`docs/superpowers/specs/2026-10-07-automated-remediation.md`): the hook's
remediation policy generation current at admission, a canonical `generation`
string. The key is optional: when no remediation policy is bound it is omitted,
so those records have exactly the fields above and stay readable by a release
that predates the key. A record carrying it is a one-way change: a rollback
below the version that introduced it rejects the records written while a
policy was bound, and those deliveries' retries and recovery are unavailable
until they age out. It sits beside
`source_revision` as internal transport metadata (ADR 0191), not an ACI field.
The protected ingress reads it under the agent's source gate, which remediation
policy writes also take, so it is the generation before or after any racing
write, never a mix; the caller supplies it on `AdmissionRequest`
(`remediation_generation`, refused outside the grammar). Admission copies it
from the request into the Intent and from the Intent into the binding, so the
envelope carries it exactly when its Intent does, and a binding whose value
disagrees with its Intent is unavailable. A record written before the field
existed parses without it and gains nothing on read; consumers treat the
absence as no admitted generation, which refuses automatic remediation and
never the turn. A retry never relabels a binding: the first admission's
generation stays. The nomination route reads the binding through the enqueue
client's `read_binding(event_id)`, a single GET of an `opaque_ref` event's
binding that never writes or expires it.

`Intent` contains exactly `schema_version: 1`, `identity: DeliveryIdentity`,
`requested_tool_access`, `effective_tool_access: "read-only"`,
`request_body_sha256`, `source_generation`, `source_operation_id`,
`policy_fingerprint`, `manifest_digest`, `runtime_id`, `runtime_generation`,
`qualification_id`, `event_id`, `conversation_id`, `payload_sha256`,
`envelope_sha256`, `reserved_stream_id`, `created_at_ms`, and `deadline_ms`.
Types follow the grammars above. Deadline is exactly created broker time plus
300000 ms; overflow refuses. Stream ID is two canonical unsigned decimal
components separated by one hyphen, each at most 18446744073709551615;
`0-0` refuses. Intent contains no raw body or prompt. Its original identity,
binding, times and reserved ID never change. Envelope and payload digests are
computed over their exact bytes before broker writes.

`State` contains exactly `schema_version: 1`, `status` in
`preparing|committed|failed`, `recovery_attempts` (strict integer 0 through 10),
`reason` in `null|deadline|attempts_exhausted|stream_id_unappendable`, and
`receipt: null | Receipt`. Preparing has null reason/receipt. Committed has
null reason and a receipt; failed has a nonnull reason and null receipt.
The mutable state key accepts only preparing or failed State. The separate
commit key accepts only committed State and is written NX as the final write.
A valid commit overrides preparing state; commit together with failed state
is contradictory and unavailable. No state write follows a committed outcome.
An intent without State is an interrupted preparing intent with zero attempts,
never accepted. An orphan state/commit/binding/recovery without matching intent
refuses and requires repair; it is not authorization to overwrite anything.

`Receipt` contains exactly the Intent fields except `created_at_ms`,
`deadline_ms`, `reserved_stream_id`, `envelope_sha256`, plus
`stream_id`, `acceptance_status: "accepted"` and
`tool_access: "read-only"` (the effective compatibility alias).
Receipt is created only inside the final commit key SET NX write and is immutable
thereafter. Matching retries return this exact original receipt. A new runtime
selection cannot relabel it. Metadata has no TTL, matching existing delivery
receipt retention. Committed/failed state retains original digest/identity
metadata while recovery payload is removed; only the stream retains accepted
payload under its own retention. Binding survives every dispatchable copy.
No foundation operation trims the stream, deletes a binding, expires a receipt
or releases accepted backlog capacity. Completion ownership remains future
worker integration; a full quota safely refuses new work until then.

`AdmissionResult` contains exactly `status` in
`accepted|duplicate|preparing|failed|conflict|refused`, `reason` in
`null|source_unavailable|runtime_unavailable|qualification_unavailable|evidence_unavailable|broker_identity_mismatch|admission_closed|quota_full|delivery_conflict|deadline|attempts_exhausted|stream_id_unappendable`,
and `receipt: null | Receipt`. Accepted/duplicate alone carry a receipt and
null reason. Preparing has no accepted stream or receipt. Failed carries the
terminal State reason. Conflict has delivery_conflict. Refused carries an
unavailability/closed/quota reason. Unsupported or malformed stored authority
returns AdmissionUnavailable rather than success. No payload is returned.

## Atomic new admission and duplicate ordering

<!-- @spec PROTECTED-HOOK-ADMISSION-4 -->
The facade first obtains current source authority and original intent/state,
validates the source with existing source_policy_records, and performs the
atomic duplicate check. Only when no exact committed duplicate exists does it
obtain bounded control snapshots and validate them with authority_records. Lua compares exact raw
snapshots to the current keys within EVAL before using their decoded fields;
a concurrent mutation refuses. Digest comparisons are against canonical
validated bytes, not cjson reserialization. A preflight read is not admission.
The script independently reads `INFO server` and `TIME` in that EVAL and uses
the live run_id/time. Missing/unsupported identity, type or command permission
refuses before a new intent. Source floor/operation/active fingerprint matches
the exact source_policy and protected mode. Active source generation and
operation match floor/reservation, selection matches manifest/qualification,
and readiness matches existing validate_authority comparisons and broker time.
Admission must be open. Every expected refusal, source/control shape, bound,
key type and event-binding collision is checked before the first write.

Before new readiness/open/quota checks, inspect original Intent, mutable State
and commit key. A commit authorizes acceptance only after validating its exact
original receipt against immutable Intent, binding and reserved stream identity.
An orphan/malformed commit, missing binding or commit alongside failed State
returns unavailable. A retained valid commit does not require a trimmed stream
entry still to exist. An existing committed receipt with current matching source authority and the
same requested/effective policies, body digest, source generation/operation and
fingerprint returns duplicate without writes, even if readiness expired,
admission closed, manifest rotated or stream trimmed. Changed duplicate identity
returns conflict. A preparing retry must match that same original tuple and
exact payload/envelope digests before recovery; it cannot choose a new event,
selection or reserved stream ID. Failed never becomes new admission. Invalid
or missing original evidence closes, rather than inferring success from stream.
Intent, State, commit, recovery, source and binding are read by separate GETs,
not one atomic snapshot, so a concurrent admission of the same delivery that
commits between those reads can present an internally inconsistent view, such
as State or commit without Intent. Before refusing on such a view, the facade
re-reads every admission record it read. If any changed, the view was torn and
the attempt is retried within the existing bounded attempt loop; exhausting
that loop is unavailable. Only a view confirmed consistent by an unchanged
re-read that still violates these invariants refuses as unavailable. A retry
authorizes nothing by itself: EVAL still compares the exact raw snapshots it
receives before any write.
HTTP current authentication and cross-store ordinary receipt exclusion remain
mandatory caller responsibilities in the parent SOURCE-8 integration.

Use TYPE to require none/string for new intent/state/commit/recovery/binding,
none/zset for quota and none/stream for runs. Existing unmatched records refuse.
Read XINFO STREAM last-generated-id when present. Reserve an explicit ID
strictly greater than that ID using broker TIME milliseconds: if time is above
the last millisecond, use `{now_ms}-0`; otherwise increment the last sequence,
carrying to the next millisecond on unsigned overflow. Implement decimal string
comparison/increment for stream IDs; Lua floating point must not compare uint64
IDs. Check overflow before writes. Check ZCARD below trusted backlog_limit.

Writes, in order, are SET intent NX; SET preparing State; ZADD quota NX;
SET binding NX; SET recovery payload NX; XADD curie:runs reserved ID with
exactly `payload` and `protected_envelope`; DEL recovery; SET commit key NX
with committed State and original Receipt. Confirm that final NX succeeded.
The last write alone establishes acceptance. Every NX must be confirmed and
all writes follow preflight comparison. Lua isolation is not rollback. Any
unexpected command failure is unavailable, even after XADD; partial Intent
remains recoverable. Product code contains no failure injection parameter.
The provisioner qualifies the exact scoped recipe out of band; product never
uses ACL administration or ACL DRYRUN to manufacture authority.

## Recovery and terminal cleanup

<!-- @spec PROTECTED-HOOK-ADMISSION-5 -->
Recovery selects the original Intent; it never allocates a replacement identity
or stream ID. It checks broker identity and all applicable types/immutable
bindings before writing. Existing validated commit or failed State returns its original
outcome without writes. A commit and failed State cannot legitimately coexist;
their coexistence refuses. Commit validation compares original Intent, binding
and exact receipt fields before a duplicate may attest acceptance. Preparation has at most ten recovery attempts. A
reachable recovery increments attempts once; broker communication failure does
not assume an increment or capacity release. Deadline uses broker TIME.
At now >= deadline or exhausted attempts, DEL recovery, ZREM quota member and
SET failed State, in that order, retaining intent/binding evidence. Cleanup is
idempotent, including partial cleanup errors; it cannot invent a receipt.
Terminal cleanup needs no still-active revoked source or expired evidence
because it authorizes no execution. It still uses the trusted broker identity.

Otherwise recheck current matching source, original selected runtime tuple,
qualification and readiness inside the script. Closed authority leaves preparing
undispatchable and records the bounded attempt; the tenth unsuccessful attempt
fails and refunds membership. No closed authority path appends. Inspect XRANGE
reserved ID reserved ID. Exact entry must have exactly the two original fields
and original bytes/digests. It can finish preparation only with matching current
authority. A mismatching entry returns unavailable and requires administrative
repair; it cannot be deleted, overwritten or accepted. If absent, compare
XINFO last-generated-id: a no-longer-appendable reserved ID fails with
stream_id_unappendable and removes quota. Otherwise recover missing preparing
state and missing binding NX. If quota membership is missing, recheck ZCARD
below the trusted backlog limit before ZADD NX; another intent may have filled
the backlog since interruption. Full quota leaves preparation undispatchable
and consumes only the bounded recovery attempt, never append or overcapacity.
Then append only that ID from the
retained recovery payload. Missing recovery bytes with absent entry closes unattended recovery. An
authenticated admit retry may restore those bytes from its exact original
payload after Python digest validation and atomic matching Intent/authority
checks. It cannot replace Intent or binding. This covers interruption before
recovery bytes were written; an unattended reconciler cannot invent missing
payload and eventually performs bounded terminal cleanup.
If entry exists after recovery bytes were deleted, the facade reads that exact
XRANGE entry, validates its payload/envelope SHA256 in Python against Intent
and immutable binding, then supplies the exact raw two-field snapshot to EVAL.
EVAL compares the current XRANGE fields byte for byte to that validated
snapshot before final commit key SET NX; the preliminary read alone authorizes nothing.
Do not assume Lua offers SHA256 or replace it with redis.sha1hex. Recovery
never uses XADD *, trusts caller time, or removes another intent's quota member.

A successful append followed by failed commit does not permit model dispatch.
The future worker parks that private entry under LANE-4 until exact immutable commit key
State agrees; it cannot XACK, charge a delivery retry or start a model while
preparing. A failed transition permits terminal acknowledgement without model
execution. This foundation includes no worker consumer or reconciliation loop.

## Scoped permission subsets and measured boundary

<!-- @spec PROTECTED-HOOK-ADMISSION-6 -->
`admission_acl_rules(role: str) -> tuple[str, ...]` accepts exactly `enqueue`
and `verifier`. It resets command/key/channel/selectors using the existing
metadata reset tokens, preserves password/enabled state and contains no user,
credential or broker configuration. Only provisioner installs it.
Enqueue main rules grant GET/TYPE on source/control, INFO server, TIME, EVAL,
GET/TYPE/SET/DEL on admission records, ZADD/ZCARD/ZREM/ZSCORE on admission quota,
and XINFO STREAM/XRANGE/XADD on exactly curie:runs. No wildcard curie execution
rights, consume/reclaim/XACK, evidence/source write, pubsub or administration.
Use the separate EVAL declared-key selector on those exact key families;
commands inside scripts remain constrained by the main selector. Verifier
subset grants GET/TYPE on control/source and SET only on
protected:control:readiness:* using a separate write selector, plus INFO server,
TIME and handshake; no EVAL, qualification/manifest/selection/source/admission
writes or payload/stream reads. These are metadata permission subsets, not
complete runtime or worker ACL inventories.

Observed 2026-10-03 on separately owned disposable Valkey 8.1.10 using redis
8.1.0: scoped EVAL executed INFO server, TIME, TYPE/GET; explicit XADD,
XRANGE, XINFO STREAM and unique ZSET membership succeeded. SET control inside
Lua, stream consumption and INFO clients refused. Wrong type before first
write returned refusal and left intent absent. A forbidden command after
SET preparation and XADD retained both writes; an absent lower explicit ID
could not append after stream advancement. Removing INFO with `-info` made
inner INFO refuse. `-info|server` is unsupported as a removal token on this
version. Grant `+info|server`, reset commands with `-@all`, and do not rely on
that unsupported removal form. Exact reproduction and complete role/error
campaign are required in the test-first realization; these observations alone
do not qualify the role inventory, TLS or runtime. The anonymous local probe
used the pinned image already recorded in ADR 0191 evidence and removed its
exact container only after verifying its owned label and CID.

## Ingress wiring

The [ingress admission wiring](2026-10-02-protected-hook-source-policy.md#ingress-admission-wiring)
section of the source policy contract wires this foundation into the signed
hook route and the API reconciler. It changes the facade as follows, keeping
the IDs. The torn read retry that
[#4094](https://github.com/curie-eng/curie/pull/4094) added to ADMISSION-4 on
main is a prerequisite. Signed retries of one delivery are serialized by the
agent gate, but the gate free API reconcilers of every replica call `recover`
on the same intents concurrently with those retries and with each other, which
is exactly the torn view it retries. It reaches next through
[#4131](https://github.com/curie-eng/curie/pull/4131), a cherry pick of its
three commits, before this wiring.

<!-- @spec PROTECTED-HOOK-ADMISSION-1 -->
The facade receives `trusted_manifest: Manifest` from the provisioner runtime
files in place of a bare broker identity, and derives the broker identity from
it. Its client comes from the LANE-3 enqueue transport, which owns and closes
the connection; the facade still constructs none and closes none.

<!-- @spec PROTECTED-HOOK-ADMISSION-4 -->
The preflight reads one broker observation through INFO server and TIME on the
same connection and decides authority through the shared `authority_evaluation`
module, so the probe and admission take the same decision and their reasons
follow its frozen mapping. The script still compares the exact snapshots it is
given and rechecks run_id and readiness expiry against live broker time; a
change between preflight and script refuses or retries as today and never
accepts on the preflight alone. A preparing original whose authenticated
retry carries the same requested policy, body digest, source generation,
operation and fingerprint but different payload bytes is a recovery attempt
without a supplied payload, not a conflict: the retried turn differs only in
API receive time or in reply coordinates the original already fixed, and the
original wins as it does on the ordinary path. Only byte identical payload may
restore missing recovery bytes. To make that path reachable over HTTP, a
protected turn's `received_at` is the canonical UTC ISO rendering of the
signed `X-Curie-Timestamp`, not API wall time, so an upstream resending the
same signed request with the same reply selection yields identical bytes. A
freshly signed retry yields different bytes and recovers without restoring
them; if the recovery bytes are missing it cannot restore, and that intent
ends in terminal failure at its deadline.

<!-- @spec PROTECTED-HOOK-ADMISSION-5 -->
The facade gains `preparing(limit: int) -> tuple[DeliveryIdentity, ...]` for
the trusted reconciler, called with `limit` equal to the backlog limit. It
reads every quota member in score order with ZRANGE, the set never exceeding
the backlog limit, reads each member's intent, state and commit, and returns the
identities of intents with neither a commit nor a failed state. It writes
nothing and authorizes nothing; `recover` decides each one. An orphan member
without an intent is skipped and reported only as a count.

<!-- @spec PROTECTED-HOOK-ADMISSION-6 -->
The enqueue rules add ZRANGE on exactly `protected:admission:quota`, within
the existing quota selector, only once a measured observation against the
pinned Valkey is recorded in the
[ADR 0191 evidence record](../../adr/evidence/0191-protected-hooks/README.md):
the exact recipe, command and observed outcome, including that ZRANGE on the
quota key succeeds and on any other key refuses. Until that record exists the
recipe stays unchanged and `preparing` is unavailable.

<!-- @spec PROTECTED-HOOK-ADMISSION-7 -->
Acceptance adds: a preparing retry with different payload bytes recovers the
original and never conflicts; a byte identical retry restores missing recovery
bytes, proven over HTTP by resending one signed request; a reconciler
`recover` racing a signed retry of the same delivery returns one entry and the
original receipt; `preparing` returns exactly the outstanding intents and writes nothing;
a control manifest differing from the trusted manifest refuses; and a frozen
vector of broker states yields the same decision from the probe evaluation
and from `admit`, with reasons following the frozen mapping.

## Acceptance

<!-- @spec PROTECTED-HOOK-ADMISSION-7 -->
Tests are registered in default root pytest. Real disposable broker tests must
prove actual atomic acceptance and unchanged ordinary/source-publication paths;
one intent/member/stream entry under concurrent delivery, duplicate after
readiness closure/rotation/trim, changed policy/body/source conflict, and refusal
with no writes for every authority/time/type/quota bound. Exercise INFO denial,
live restart run_id mismatch with retained keys, selector separation, verifier
write boundaries and enqueue consume/administration refusal. Validate exact
uint64 reservation and overflow behavior, unknown/duplicate/malformed record
fields, safe errors and immutable inputs/results. Exercise real errors after
each write, including missing initial State, quota/binding/recovery persistence,
append success/commit failure, lost response, exact-ID recovery, stream advance,
wrong existing entry, closed recovery, tenth attempt and 300 second deadline.
Use actual broker errors, not a fake success model or a production fault hook.
Use literal exact-key ACL selectors to deny each next write while permitting
all earlier writes. In particular, allow mutable state SET and deny SET on
protected:admission:commit:{d}; the actual Lua append and recovery-byte deletion
then precede an actual final commit permission error. Restore only the fixture's
original scoped role. Seeded partial state does not prove this final-write
error; execute the real script and inspect its retained earlier writes. No product ACL administration is introduced.
Retain parent worker/provisioning/HTTP/reconciliation acceptance as open.

# Protected hook lane: implementable first-release contract

Realizing contract for [ADR 0190](../../adr/0190-automated-hook-sources-cannot-widen-their-tool-access.md) and [ADR 0191](../../adr/0191-protected-hook-delivery-authority.md), tracked in [#3603](https://github.com/curie-eng/curie/issues/3603). Existing ACI policy and kernel recovery semantics remain binding. This specification precedes implementation and does not claim qualification or provisioning evidence.

## Closed first-release design

<!-- @spec PROTECTED-HOOK-LANE-1 -->
Use a separate Valkey instance, protected worker deployment and protected
sandbox execution domain. All Valkey clients constructed for that protected
worker point at the protected endpoint, including async, synchronous affinity,
pressure, stream retention, completion outbox, capacity wait, delivery leases,
progress, locks, markers and consumer liveness. Ordinary workers retain their
current endpoint and state. This avoids splitting every kernel key across two
brokers. Postgres conversation transcripts and immutable object storage remain
durable data services; neither may expose a protected dispatch/replay endpoint
to ordinary consumers. Protected workers do not run eval, cron, publication,
connector-dispatch or orphan-work-item producer loops in this release.

The protected worker consumes only authenticated protected hook envelopes;
ordinary QueuedTurn payloads on its stream are refused. There is no transport
fallback, including on broker failure or incompatible rollback. Existing platform API key holders remain trusted for source administration,
as ADR 0191 states. Neither ordinary nor protected workers receive protected
provisioning, evidence issuance or consumer credential minting authority.
Existing API credentials do not authorize those out of band operations.
Do not add a new frozen boot credential or alter shipped API authentication
as a passenger on this lane.

## Runtime manifest and evidence

<!-- @spec PROTECTED-HOOK-LANE-2 -->
An immutable canonical JSON manifest, with SHA-256 identity, contains exactly:

| Member | Meaning |
| --- | --- |
| `schema_version` | `1`; unknown versions refuse. |
| `runtime_id`, `runtime_generation` | Stable lane identity and never-reused monotonic generation. |
| `broker_identity` | Provisioner-assigned instance identity, TLS endpoint/server identity, live `INFO server` `run_id` and database `0`; never a mutable DNS name alone. |
| `worker_image_digest` | One exact qualified OCI digest. |
| `runner_image_digest` | One exact qualified OCI digest. |
| `bundle_digest` | One exact content digest and immutable object identity. |
| `execution_config_digest` | Canonical harness, runner configuration, tool/connector policy, credential-reference generations and init/fetch image digests. Secret values excluded. |
| `qualification_id` | Immutable enforcement campaign record for this exact tuple. |
| `substrate` | `docker` or `kubernetes`, plus immutable protected launch/template identity. |
| `guard_identity` | Provisioner-owned preventive control identity and configuration digest. |
| `credential_refs` | Distinct enqueue, worker and verifier credential references/generations; no secret values. |

A generation has one tuple, not a mutable set of eligible images. Changing any
tuple member creates a new generation; a newer active deployment does not
replace the tuple already bound to a queued delivery. Qualified bundle content
<!-- doclint:ignore-line -->
contains no executable lifecycle command hooks, including `hooks/hooks.json`.
Static Skill loading and connector actual-effect qualification are prerequisites
where used; a label or readOnlyHint does not qualify them.

Readiness evidence contains manifest digest, runtime/qualification generation,
broker identity including `run_id`, guard revision, verifier identity, `issued_at_ms`,
`expires_at_ms`, and immutable measurement-record identity. Expiry is bounded
by provisioner policy and evaluated against broker `TIME`; caller time is
irrelevant. Evidence is written only by the separately credentialed verifier.
It records verified enforced control-plane configuration, qualification and
deployment identity, not worker self-attestation or a fleet snapshot. Broker
server identity is authenticated independently of evidence echoed by a worker.
A credential issuer and verifier may share the provisioner process, but neither
authority is held by API, ordinary workers, protected workers or runners.

The qualification authorization record binds the qualified artifact tuple to
this broker instance and live `run_id`. Retained qualification authorization
or readiness cannot authorize a restarted broker merely because its restored
keys contain the same manifest and unexpired timestamps. The provisioner must
establish the new runtime epoch and current source floors under the source
recovery contract before issuing matching qualification authorization and
readiness. Earlier artifact observations may remain historical evidence; they
do not themselves establish the new broker's preventive boundary.

## Closed internal record encoding

<!-- @spec PROTECTED-HOOK-LANE-2 -->
The v1 runtime manifest, qualification authorization and readiness records use the closed field sets below. Every listed field is required and non-null. Unknown fields at every nesting level and unknown schema versions refuse. No optional extensions are accepted in v1. Input validation is strict: booleans are not integers, strings do not coerce to numbers, and numbers do not coerce to strings. Internal generation representation does not change the existing public API representation.

Identifiers called UUID below use canonical lowercase hyphenated UUID spelling; parsing and reformatting must equal the input. A `generation` is a decimal string matching `[1-9][0-9]*`, with numeric value at most 9223372036854775807. Zero, sign, exponent, whitespace and leading zeros refuse. A `millisecond` is a decimal string matching `0|[1-9][0-9]*`, with numeric value at most 9007199254740991, preserving exact broker/Lua comparisons. A `sha256` is exactly 64 lowercase hexadecimal characters. An `oci_digest` is exactly `sha256:` followed by a `sha256`. An `opaque_ref` matches `[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,255}` and is a reference to provisioner-owned state, never a secret value, mutable locator or instruction to fetch a URL.

Each encoded record is at most 16384 bytes. Decoder rejects invalid UTF-8, duplicate JSON object member names recursively, trailing data, NaN/Infinity, floats and unknown nested members before constructing a record. References and identity fields use only the defined ASCII alphabets; no Unicode normalization or case folding silently changes identity.

Canonical record bytes are ASCII JSON with object keys sorted lexicographically, no insignificant whitespace, standard JSON string escaping and no nonfinite numbers. The only JSON integers are schema version, broker port and database. Decimal generations and milliseconds remain JSON strings. Implementations produce the same bytes as `json.dumps(validated_primitives, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("ascii")` for these closed v1 shapes. Input whitespace/member order does not change canonical content. Manifest identity is the lowercase SHA256 of its canonical bytes, without a prefix or an embedded self-digest member. Mutable dictionaries are not retained as record state: records and nested values remain immutable, and cached identities cannot survive content mutation.

The authority-record grammar tests under `packages/protected-hooks/tests` are
part of the default root pytest collection and its CI execution. An explicitly
selected focused run does not replace registration in the complete suite.

## Nested shared records

<!-- @spec PROTECTED-HOOK-LANE-2 -->
`BrokerIdentity` contains exactly:

| Field | Type and meaning |
| --- | --- |
| `instance_id` | UUID assigned by the provisioner. |
| `endpoint` | Object containing exactly `host` and `port`. |
| `endpoint.host` | Lowercase ASCII DNS name without trailing dot, or canonical IP address; no scheme, userinfo, brackets, path, query or fragment. DNS labels follow ordinary hostname syntax, total at most 253 characters; a dotted numeric address cannot fall back to DNS parsing when noncanonical. IP addresses use canonical `ipaddress.ip_address(...).compressed` spelling. |
| `endpoint.port` | Strict integer from 1 through 65535. |
| `tls_server_name` | Lowercase ASCII DNS name or canonical IP address under the same grammar; the independently verified TLS identity. |
| `tls_spki_sha256` | SHA256 of the designated server leaf certificate's DER SubjectPublicKeyInfo. |
| `run_id` | Exactly 40 lowercase hexadecimal characters, the pinned Valkey server's observed live `INFO server` identifier. |
| `database` | Strict integer literal 0. |

The authenticated TLS client performs standard trust-chain and server-name validation and also matches the provisioner-authorized SPKI pin. Trust anchors and the expected pin are obtained from trusted provisioner configuration; source input, worker output and echoed evidence cannot select the trust root. The manifest binds the expected identity; matching a parsed manifest does not itself authenticate a connection. Renewal to another pinned public key changes the tuple and runtime generation. Missing identity facts or unsupported identity format refuses.

`CredentialReference` contains exactly `id: opaque_ref` and `generation: generation`. These are immutable references to credential issuances; they never carry tokens, keys, passwords or credential material. `GuardIdentity` contains exactly `control_id: UUID`, `revision: generation` and `config_sha256: sha256`. `SubstrateIdentity` contains exactly `kind: "docker" | "kubernetes"`, `authority_domain_id: UUID`, `launch_identity: opaque_ref` and `launch_config_sha256: sha256`.

`authority_domain_id` identifies the actual provisioner-owned preventive execution domain. The launch reference identifies an immutable, generation-specific Docker launch-policy or Kubernetes template record inside that domain. Names and labels for mutable templates do not satisfy this identity. Guard configuration observations and their digest include that authority domain and all credential-bearing creation paths. An identifier or digest is a binding to verified facts, never evidence by itself that the preventive guard is active or inaccessible to ordinary identities.

## Runtime manifest v1

<!-- @spec PROTECTED-HOOK-LANE-2 -->
The manifest contains exactly:

| Field | Type |
| --- | --- |
| `schema_version` | Strict integer literal 1. |
| `runtime_id` | UUID. |
| `runtime_generation` | generation. |
| `broker_identity` | BrokerIdentity. |
| `worker_image_digest` | oci_digest. |
| `runner_image_digest` | oci_digest. |
| `bundle_digest` | Object containing exactly `sha256: sha256` and `object_identity: opaque_ref`. |
| `execution_config_digest` | sha256. |
| `qualification_id` | UUID. |
| `substrate` | SubstrateIdentity. |
| `guard_identity` | GuardIdentity. |
| `credential_refs` | Object containing exactly `enqueue`, `worker` and `verifier`, each a CredentialReference. |

The three credential reference IDs are pairwise distinct. Provisioning also proves they do not alias one principal or credential issuance; different reference spelling alone does not establish authority separation. The separate SOURCE-6 source-writer credential is not substituted for enqueue and remains in its own provisioner authority inventory. It is not an additional v1 manifest member.

The bundle object identity identifies an immutable content-addressed or versioned object. Its bytes must match the bound digest when fetched through the qualified artifact path. Registry/artifact retrieval configuration comes from trusted provisioning; the serializer does not fetch any reference. An OCI digest is a content identity, not a mutable image tag or a complete registry locator. Qualification records the trusted pinned image references and architecture-specific mapping from OCI identity to the substrate's actual imageID. Serializer validation alone does not establish that mapping.

## Qualification authorization v1

<!-- @spec PROTECTED-HOOK-LANE-2 -->
The provisioner-owned qualification authorization record contains exactly `schema_version: 1`, `qualification_id: UUID`, `qualification_generation: generation`, `runtime_id: UUID`, `runtime_generation: generation`, `manifest_digest: sha256`, `broker_identity: BrokerIdentity`, `execution_config_digest: sha256`, `guard_identity: GuardIdentity` and `measurement_record_id: opaque_ref`.

The runtime ID/generation, manifest digest, qualification ID, broker identity and execution/guard identities match the immutable manifest exactly. The provisioner assigns a never-reused qualification authorization generation; readiness binds it. The immutable measurement record contains the actual qualified artifact tuple and enforcement observations. An old measurement may be historical input to a new authorization, but cannot authorize a new live broker run_id without the runtime epoch and source-floor recovery already required by SOURCE-7 and LANE-5. Application identities cannot create or overwrite this authorization.

## Readiness evidence v1

<!-- @spec PROTECTED-HOOK-LANE-2 -->
Readiness contains exactly `schema_version: 1`, `manifest_digest: sha256`, `runtime_id: UUID`, `runtime_generation: generation`, `qualification_id: UUID`, `qualification_generation: generation`, `broker_identity: BrokerIdentity`, `guard_identity: GuardIdentity`, `verifier_identity: CredentialReference`, `issued_at_ms: millisecond`, `expires_at_ms: millisecond` and `measurement_record_id: opaque_ref`.

Manifest/runtime/qualification/broker/guard fields match the selected manifest and current qualification authorization exactly. The verifier identity matches the manifest's verifier credential reference. The measurement record identifies the immutable readiness measurement of current guard/deployment and qualification facts; it need not be the earlier artifact qualification measurement record, but both are authenticated provisioner/verifier-owned records. Equality of measurement IDs is not substituted for those distinct observations.

Structural validation requires `issued_at_ms < expires_at_ms`. Currentness evaluation requires `issued_at_ms <= broker_now_ms < expires_at_ms` and `expires_at_ms - issued_at_ms <= trusted_max_readiness_ms`, where the maximum is a positive integer supplied by trusted provisioner policy, never by source input or evidence itself. Broker TIME and live INFO identity are read inside the existing admission atomic operation; a parser using local wall time cannot attest readiness. Bounded timestamp conversion is exact within the stated safe-integer range. Any missing, stale, future-issued, mismatched or unsupported record refuses.

Verifier-only broker write authority authenticates readiness under LANE-3. These internal records introduce no application-held issuer key or portable proof-signature scheme. A decoder cannot create evidence authority, validate ACL separation, verify guard operation or reopen admission. Pure serialization tests leave actual TLS/INFO/TIME, role separation, preventive-control negative actions and restart/source-floor recovery qualification open.

## Broker authority and key partition

<!-- @spec PROTECTED-HOOK-LANE-3 -->
Protected broker default user is disabled; it does not accept ordinary Valkey
credentials. TLS and protected network isolation are mandatory on a deployment
claiming support. New scoped credentials are not mounted into an ordinary
Deployment, ServiceAccount or Docker container, nor passed to runner env.

| Identity | Allowed authority |
| --- | --- |
| Provisioner | Configure users/network/instance, manifest and monotonic runtime fences; revoke credentials and terminate authenticated sessions. Kept out of application deployments. |
| Source policy writer | Reserve/revoke `protected:source:*` authority before SQL commit, then CAS-publish matching committed activation under SOURCE-6; cannot issue runtime evidence, read payload or consume. |
| Verifier | Read authoritative deployment/guard facts; write bounded evidence under `protected:control:*`; cannot fabricate qualified tuple or change ACL/deployment. |
| API enqueue | Read control/evidence, execute atomic admission and write admission/receipt/envelope records and protected runs entries; no evidence/control writes, consume/reclaim or broker administration. |
| Protected worker | Read control/evidence/envelope bindings, consume/reclaim protected runs, and read/write existing worker execution key families; no control/evidence writes, source activation or broker administration. |
| Ordinary worker/source/runner | No broker authentication or protected payload access. |

Runtime control keys use `protected:control:*`; source activation keys use
`protected:source:*`; immutable delivery bindings and receipt
keys use `protected:admission:*`; existing kernel execution families remain
`curie:*` on the separate broker. Worker authority never includes a wildcard
that covers control/admission writes. API must not acquire worker-consume
authority merely to inspect a duplicate receipt. The realizing ACL command/key
inventory must be measured against the pinned Valkey version, including Lua
permissions and key read/write selectors. Enqueue and consumer roles require
`INFO server` read permission for live broker identity checks; this grants no
control/evidence writes. Preserve the measured separation between script
declared-key authorization and the commands permitted inside the script.
If the complete product rights cannot be separated
on that version, use a provisioner-owned admission gateway with exclusive
broker-write credential and equivalent atomic operation; do not weaken the
roles. This fallback changes deployment machinery and must be recorded before
implementation. The dependency observations identified in ADR 0191 establish
the measured ACL/Lua primitives, not the complete product role inventory or
runtime qualification.

## Metadata role realization

<!-- @spec PROTECTED-HOOK-LANE-3 -->
The first broker realization exports two closed metadata permission subsets,
`source_writer` and `control_reader`, through
`metadata_acl_rules(role) -> tuple[str, ...]` in the internal protected hooks
package. Unknown roles refuse. Each recipe resets prior permissions and
selectors before granting its named subset. Permission reset preserves the
principal's existing password and enabled state. It contains no username,
password, credential reference, enabled user flag or broker configuration.
Only the out of band provisioner installs these rules, enables its distinct
principals and supplies their independently issued credentials. Recipes do
not disable another service's default user or modify a running broker.

The source writer grants GET, SET and the script operations used by the
existing SourceFence only on `protected:source:*`, plus connection handshake
commands. It has no control, admission, execution stream, consumption, INFO,
TIME or broker administration authority. Existing reservation and ordinary
publication retain their SOURCE-6/7 semantics. Protected publication remains
unavailable.

The control reader grants GET on `protected:source:*` and
`protected:control:*`, INFO server and TIME, plus connection handshake and
read only EVAL. Its separate declared key selector admits EVAL on those
families without granting any inner write command. It grants no SET, DEL,
stream, consume, administration or unrestricted key permission. Its operation
facade, `AuthorityMetadataReader`, receives an explicitly supplied scoped
Redis client. It never constructs a connection or reads environment
credentials. `read_source(agent_id, hook)` preserves SourceFence's validated
read result. `read_control(key)` accepts only ASCII keys matching
`protected:control:[A-Za-z0-9:_-]{1,256}` and returns bytes or absence.
`observe()` calls INFO with exactly the server section and TIME with no
caller supplied arguments. Its immutable `BrokerObservation` contains only
the canonical forty lowercase hexadecimal character `run_id` and
`now_ms`, bounded by the LANE-2 millisecond range. Invalid observations or
broker errors refuse through a safe unavailable error without connection
details. These separate reads are not atomic admission or readiness proof.

Measure the exact emitted command and key selectors against the pinned
dependency before claiming this realization. The INFO subcommand permission
checks the first section argument; neither a recipe nor this facade claims
general isolation of arbitrary INFO argument combinations. Tests use a
separate disposable broker with its default user disabled and distinct
credentials. They exercise positive CAS and metadata reads and negative
authentication, cross role direct and Lua writes, consume and administration
operations. Do not change a shared backing service's ACLs to run them.

These subsets are not the complete enqueue, worker or verifier inventories.
They issue no authority, authenticate no TLS or provisioning boundary and
do not enable source administration, runtime qualification or protected
delivery. The default resolver remains unavailable until the separate
authority, admission, provisioning and execution requirements pass.

## Authenticated metadata reader transport

<!-- @spec PROTECTED-HOOK-LANE-2/3 -->
The next metadata realization adds `AuthenticatedMetadataReader` in the
internal protected hooks package. The exported credential is
`MetadataReaderCredential(username: str, password: str)`, a frozen, slotted
dataclass whose representation redacts both fields. Both values must be
nonempty strings and username must differ from `default`. The exported
classmethod `AuthenticatedMetadataReader.connect(manifest: Manifest,
credential: MetadataReaderCredential, ca_pem: str) -> AuthenticatedMetadataReader`
accepts only a trusted parsed Manifest, that explicit credential and CA PEM
data from provisioning. It eagerly completes TLS, pin, named authentication
and live run_id verification before returning a reader. No unverified public
constructor is provided. Credential arguments cannot override transport settings. No
connection is derived from environment variables, URLs, WorkerConfig, source
input, worker output or evidence. The manifest remains the existing closed v1
shape. This reader does not prove the credential issuance or role separation;
provisioning must supply the actual control reader principal.

The factory refuses malformed inputs and tuples whose broker endpoint host
is different from `tls_server_name` before network operations. That tuple is
unsupported by this first transport realization. It cannot weaken hostname
validation to support it. CA PEM data is a nonempty string containing only PEM certificates, with no
private key or other PEM block; it is validated before network operations.
No caller selected file paths or broad connection keyword arguments are
accepted. The connection uses database zero, certificate validation required,
standard hostname validation and the manifest endpoint. After normal TLS
chain and hostname validation, a narrow SSLConnection subclass obtains the
server leaf certificate in DER, extracts its DER SubjectPublicKeyInfo and
matches its SHA256 against the trusted manifest pin before transmitting
credentials or application commands. Missing certificate, malformed
certificate or pin mismatch closes the socket and refuses. Every new socket,
including reconnects, repeats this validation.

Named authentication uses explicit RESP3 HELLO AUTH. No password only AUTH,
default user fallback, alternative endpoint, plaintext or weaker verification
retry is permitted. Connect and socket timeouts are each two seconds; retries
are disabled. Maintenance notifications, client tracking and caller supplied
connection callbacks are disabled. After authentication on every connection
or reconnect, INFO with exactly the server section must yield the canonical
live run_id bound by the manifest before an application command is sent.
Changed, missing or unreadable run_id closes that connection and refuses.
These checks do not issue readiness or authorize a restored broker epoch.

The reader exports only `read_source(agent_id, hook)`, `read_control(key)`,
`observe()` and `close()`, retaining the existing metadata reader argument and
result shapes. Source coordinates and control keys are validated before
identity checks or other I/O. The authenticated transport translates all public
input validation failures, including SourceFenceInvalid, into the existing
BrokerMetadataUnavailable error; AuthorityMetadataReader retains its existing
validation and exception behavior. No raw client, generic command, write, provisioning or
credential issuance interface is exported. Its operations are serialized on
one privately owned connection so the authentication and identity check apply
to the connection executing the read. Each public read checks live INFO server
identity before reading metadata; observe returns validated identity and TIME
from that connection. Separate commands remain separate observations, not
atomic admission. A reconnect cannot silently substitute another broker.
Close releases owned connections and permanently refuses later operations;
close is idempotent. Public credential and reader representations, exceptions
and transport log output exclude credentials, certificate bytes, endpoints,
raw broker responses and underlying exception details. This is not a claim
that debugger inspection or third party traceback-local capture cannot inspect
private objects; test fixtures must redact their own diagnostic output. Expected validation, TLS, authentication and broker failures
surface only the existing safe metadata unavailable error.

Real disposable TLS broker tests must prove successful source/control reads
and observation, and refusal with wrong CA, hostname, leaf SPKI pin, named
credential and live run_id. Wrong CA/name/pin must transmit no authentication.
A failed named principal must never attempt default authentication. Retained
connection and forced reconnect cases must verify pin and run_id enforcement,
and close must prevent reuse. Validated wrong tuple/input refusals precede
connection attempts. These tests enable no source administration,
activation, admission, runtime evidence issuance, protected worker startup or
qualification. The only consumer is the observational support probe under
[PROTECTED-HOOK-SOURCE-9](2026-10-02-protected-hook-source-policy.md#surface-parity-and-smallest-implementation-task),
which reads through it and writes nothing.

## Atomic admission, duplicate receipt and activation

<!-- @spec PROTECTED-HOOK-LANE-4 -->
Support probe authenticates the source and reads policy, manifest,
qualification, evidence and broker identity. It creates no sandbox, hook claim,
quota reservation, workspace mutation, placeholder, queue entry or model turn.
Its successful response identifies current tuple/generations;
it grants no reservation and every delivery re-evaluates current authority.

The API authenticates requested policy before deriving effective read-only,
resolves reply/source coordinates and selects the manifest tuple. Before
checking new-admission readiness or open admission, compare any existing
immutable receipt under
[PROTECTED-HOOK-SOURCE-8](2026-10-02-protected-hook-source-policy.md#receipt-and-duplicate-contract).
A duplicate lookup requires current source authentication and matching current
source generation; it cannot bypass revoked-source authentication. An exact
match returns the original receipt even when new admission is closed, without
a new claim, quota reservation, enqueue or other write. Changed signed content
or authority conflicts rather than becoming a new delivery. Only a delivery
without an existing receipt proceeds to new-admission validation. The protected
admission transaction/script then reads broker `TIME` and `INFO server` within
that same atomic operation and validates all of:
active source revision/key generation; active runtime/manifest generation;
qualification identity/config digest; current nonexpired evidence; matching
broker identity, including equality of the live server `run_id` to the
manifest, qualification authorization and evidence; and open admission.
Missing/unreadable `run_id`, mismatch or a restarted broker refuses before any
claim, quota reservation or enqueue even if restored evidence is unexpired.
On refusal it writes nothing.

Within the same broker-atomic operation: compare idempotency record, reserve
the protected backlog quota, store immutable binding and receipt, append one
protected stream envelope and mark the delivery accepted with its stream ID.
Lua isolation is not transactional rollback on command failure. Before its
first write, validate all expected refusals, key types, serialization bounds
and the measured command permissions. The first write is an immutable
`preparing` intent containing the exact request/binding digests and a reserved
explicit stream ID. Choose that ID from broker time and the stream's last
generated ID so it is strictly greater at reservation. Never use `XADD *`
when recovering that intent. Backlog quota is represented by a unique intent
member in a protected sorted set, so membership addition/removal is idempotent
and recovery needs no ambiguous counter increment marker. Count reservations
before admitting a new intent; do not silently expire an outstanding member.

Append the reserved ID and write `committed` only as the final admission write.
An unexpected broker command error returns unavailable, never a successful
receipt, and may leave an intent requiring recovery. The worker refuses model
dispatch unless the committed binding and exact stream ID/payload agree.
An authenticated retry with current matching authority and valid readiness
inspects `XRANGE reserved_id reserved_id`: an exact existing entry can finish
that intent; if absent and still appendable, append only that reserved ID. If
a later stream entry has made the absent reserved ID unappendable, record a
terminal admission failure and remove its quota member idempotently; never
substitute a later ID or create a second delivery. A mismatching existing entry
closes recovery and requires administrative repair. Preparing or failed intents
cannot attest successful delivery. Recovery rechecks source/runtime authority
and readiness; closed admission leaves preparation undispatchable. Terminal
cleanup removes quota membership idempotently while retaining original intent
and receipt evidence. Exercise failures after every write with real broker
errors, including append-success/final-write failure and stream advancement.
When an actual stream entry exists but its intent is still preparing, the
protected consumer parks it privately without model execution, XACK,
dead-letter/terminalization or a delivery retry-budget charge. Recheck the
binding before dispatch; a committed transition resumes that same entry,
while a failed transition permits terminal acknowledgement without execution.
API admission reconciliation owns preparing intents even if the caller never
retries. Each intent records broker-time creation, a 300-second deadline and
a maximum of ten reconciliation attempts. Run reconciliation every five
seconds, using the same atomic recovery and authority checks as a retry.
Closed readiness leaves the intent undispatchable and may consume a bounded
reconciliation attempt; it is not a new no-write admission refusal. On deadline
or attempt exhaustion, atomically mark failed, remove quota membership
idempotently and retain dedupe evidence. A late caller receives failure and
cannot turn that delivery ID into a new admission. Failure to reach the broker
cannot safely release its reservation: recover it on reconnection, never grant
replacement capacity by assuming deletion. These bounds apply only to
uncommitted admission, not to the existing kernel's accepted-turn retries.
No preliminary protected hook claim is made in the ordinary store or database.
Constructing the turn cannot require a placeholder or writable workspace:
protected first-release ingress uses validated existing reply coordinates and
read-only investigation inputs. Any required durable bookkeeping is recorded
after broker acceptance using an idempotent reconciliation record; it cannot
be treated as a cross-store transaction or recreate an ordinary dispatch copy.

Duplicate identity uses the existing agent/hook/delivery-ID domain, excluding
runtime generation so a rollout cannot execute a delivery twice. The immutable
receipt retains original requested/effective policies, source generation,
manifest identity and accepted stream ID. A matching repeat returns the
original result; changed signed content/requested policy/source authority
conflicts. A duplicate never becomes a new success under a newer restriction.
An authenticated retry may read its original receipt even while new admission
is closed; it cannot make a stale generation enqueue. Dedupe retention stays
at least the existing declared delivery retention; it is not silently extended
into permanent unbounded payload storage.

<!-- @spec PROTECTED-HOOK-LANE-5 -->
Source activation and recovery use
[PROTECTED-HOOK-SOURCE-6/7](2026-10-02-protected-hook-source-policy.md#broker-authoritative-activation-and-recovery).
That contract owns agent locking, broker reservation/revocation, generation
allocation, SQL commit, CAS publication, tombstones and crash recovery. The
lane reads that exact source floor/operation/active fingerprint in its atomic
admission operation; it neither allocates a parallel source revision nor
republishes source authority during runtime readiness renewal. Missing or
contradictory source authority closes admission. Broker restart/data-loss
recovery cannot issue readiness that bypasses source reconciliation. Source
writers cannot issue runtime evidence and runtime verifiers cannot change
source authority.

Runtime rotation first closes admission. Safe compatible draining retains the
old immutable tuple and consumption authority until its accepted payloads
settle; expiry of admission evidence alone does not delete accepted work.
Before any incompatible guard/deployment/secret change: prohibit further
execution, revoke affected credentials, terminate broker sessions and stop
old workers/runners, then apply the change. Existing payload remains private
and parked. Requalification may authorize only a matching tuple; otherwise
terminal refusal and reply use the protected completion path. Never reinterpret
an old envelope as the new generation or copy it to ordinary Valkey.

## Envelope, session and selected runtime binding

<!-- @spec PROTECTED-HOOK-LANE-6 -->
Stream transport fields contain the unchanged `payload` encoding of QueuedTurn
plus `protected_envelope`, a versioned internal JSON object containing:
`schema_version`, `event_id`, `source_revision`, `runtime_id`,
`runtime_generation`, `manifest_digest`, `qualification_id`, runner/bundle/
execution-config digests, `logical_conversation_key`, `execution_session_key`
and digest of the exact queued payload. It is not a member of QueuedTurn or
Event. Unknown/missing/contradictory metadata refuses before kernel/model work.
The API owns immutable binding storage keyed by event ID; workers verify each
dispatchable copy against that binding. A copy with only QueuedTurn must still
resolve/verify its binding and is never interpreted as ordinary work.

Before dispatch and every later model-start retry, the consumer/runner guard
reads the authenticated broker's live `INFO server` `run_id` and requires the
same broker identity bound by the envelope's manifest and qualification
authorization. An unreadable or changed identity refuses execution; restored
binding/evidence keys alone cannot authorize it. Accepted payload stays private
through restart recovery and is never silently rebound to a replacement epoch.

All recovery/copy sites preserve the envelope or immutable binding reference:
pending PEL recovery, cap-based graveyard, kernel flag-clean retry, capacity
parking/grant/wakeup and completion outbox. Worker-internal retries of the same
delivery keep its event ID, policy, selection and execution session. The
binding is retained at least as long as any dispatchable/outbox copy. Missing
binding is a protected refusal, not permission to decode the ordinary path.
Existing max-delivery, positive-idle reclaim, live-owner fencing,
XADD-before-XACK, no-auto-retry-after-effects, retry classifications and bounded
graveyard semantics remain unchanged. Refused work follows existing bounded
terminal/reply handling rather than an endless retry classification.

<!-- @spec PROTECTED-HOOK-LANE-7 -->
The external conversation identity and reply coordinates remain unchanged.
Protected execution keys are derived inside the worker from runtime generation
and logical conversation key under a protected namespace; no source chooses
them. Affinity/routes/locks/SDK sessions are private to the protected broker and
substrate. A guarded kernel facade and protected binding/substrate wrappers receive the
internal execution context; they map sandbox routes and boot history to the
protected execution key while the unchanged kernel operates on the private
broker. No ACI identity is rewritten. A protected worker never
adopts an ordinary sandbox, SDK session, workspace process, pending approval
or background work. Durable protected history continues through the existing transcript API,
using a distinct canonical channel-scoped protected conversation segment and
a scoped credential; it never shares the mutable transcript/epoch key of an
ordinary human session. The wrapper sets the existing CURIE_HISTORY_REF to
this protected transcript key. Protected history is rehydrated as data into a
fresh SDK session. Continuous human/protected history is not promised in this
release, and no history bridge or cross-lane handoff is introduced.

Each newly dispatched delivery boots a clean pinned runner; safe retries may
use only that delivery's verified restricted session, or cold-create and
rehydrate history. Finish retires the SDK route before a subsequent protected
delivery. Resource/per-agent/global overrides and generic warm pools are not
eligible in release one. Docker creates only the manifest digest on its
protected daemon; Kubernetes cold-creates through its protected immutable
template/pool (no generic discovery or resource-derived template copies).
Attachment/workspace replacement is unavailable unless it preserves the exact
qualified tuple and cold-session rule; unsupported configuration refuses at
support/admission rather than silently selecting ordinary machinery.

Before the first prompt and after adoption/replacement/restart, verify actual
Docker image identity/container ID or Kubernetes pod/container imageID and
template identity from the authoritative substrate control plane, bind it to
the authenticated SandboxHandle and token, then perform the existing exact
runner `/status` enforcement check. An image reference reported by the runner
or a worker-provided label is insufficient. Missing facts or mismatch refuses
without model work. SDK session contamination remains a refusal even for the
right image. Immutable digest-platform mapping must be qualified on the actual
architecture; do not assume OCI index digest equals runtime imageID.

Release one requires dedicated protected reply threads with ingress/controller
ownership identifying protected investigation dispatch separately from human
messages. Preserve actual external reply coordinates and receipt targeting;
never replace Slack thread_ts with the execution key. A human can start a
separate ordinary conversation for approval-backed recovery. Replies or mentions
observed by the ordinary dispatcher must not acquire protected broker/session
credentials; if a human message legitimately creates an ordinary session, its
independent agent/runner authority and transcript key remain ordinary. The
one-live-session invariant applies independently to each execution identity;
this release introduces no claim of global serialization across both lanes.
Admission ownership must prevent a protected delivery from joining an ordinary
live route. A controller policy that merely asks an old worker to obey a new
marker does not establish that ownership. The actual dispatch/reply behavior
must be measured with both workers before activation.

## Preventive provisioning boundary

<!-- @spec PROTECTED-HOOK-LANE-8 -->
Docker: provisioner owns a separate daemon/host execution boundary and its
credentials, not a second Compose project on the ordinary root-capable Docker
socket. Ordinary workers must have no socket/host mount/remote daemon route
that can inspect protected env, mount protected secrets or alter protected
containers. Protected worker receives sandbox launch through the controlled
boundary with pinned image policy; it cannot mutate credential distribution
or guard policy. The actual daemon authorization or launch mediator must
prevent unqualified creation before secret assignment. Running both workers
under the same unrestricted daemon is unsupported, regardless of namespace.

Kubernetes: provisioner owns a separate protected namespace/execution boundary,
ServiceAccounts, immutable Secret references, admission guard and protected
template. RBAC denies ordinary identities secret access and pod/controller/
template creation or mutation in that boundary. A fail-closed admission guard
rejects every unqualified pod, controller revision, secret mount, image,
init/ephemeral container and launch-template override before it can receive
protected credentials. Protect guard configuration/RBAC/namespace labels from
ordinary identities too; controllers and operators capable of synthesizing
pods are part of the guarded authority set. Namespace separation alone does
not fence an ordinary cluster-admin or privileged host/node-capable worker.
Use a separate cluster/node authority domain when those rights cannot be
removed without changing shipped ordinary behavior. Credential-bearing pods
must not expose their secrets through shared host networking/mounts or an
ordinary runner API. Verify actual CNI policy; additive NetworkPolicy objects
cannot be treated as restrictive intersection.

The verifier authenticates those actual controls and attempts negative actions
with ordinary identities. A periodic drift scan is supplementary. A normal
image/secret rollback must be rejected preventively, or performed by the trusted
provisioner only after the revoke/terminate sequence. A guard gap makes evidence
issuance unavailable. Administrative compromise/deliberate administrator
disclosure remains outside ADR0191's guarantee; ordinary workers with broad
existing authority are not presumed harmless for this fence.

## Implementation ownership, phases and evidence

Specification changes commit first; each phase commits its failing real test
alone and observes failure before the implementation commit. One owner handles
kernel/consumer/threadlock/markers changes and the mandatory adversarial review.
No broad kernel rewrite or unrelated eval/reclaim change.

<!-- doclint:ignore-line -->
1. `apps/api/src/curie_api/protected_runtime.py` and
   `protected_admission.py` (proposed new modules), source-policy persistence,
   `apps/api/src/curie_api/routers/hooks.py`: model manifest/evidence/envelope, source activation and
   real Valkey atomic admission. Tests race expiry/revocation/source CAS,
   crash recovery/idempotence and no-write refusal with real backing stores.
<!-- doclint:ignore-line -->
2. `apps/worker/src/curie_worker/protected_lane.py`, `protected_run.py`,
   `protected_binding.py`, `protected_substrate.py` (proposed new modules):
   separate guarded entrypoint composes existing stores and Consumer/Kernel
   with private clients; avoids changing ordinary run.build or enabling its
   ancillary loops. Test actual command/key inventory and retained ordinary credentials
   against read/new-group/reclaim/pending/outbox/dead-letter access.
3. New guarded Consumer adapter overrides the entry handler before the base
   `Consumer._handle` acquires/dispatches the turn, validates durable bindings,
   and passes unchanged fields to the base implementation. Capacity records
   retain full fields today; wakeup enters the same handler. A guarded kernel
   facade validates `process_event` and all capacity/recovery calls, while the
   protected substrate wrapper checks exact artifacts on claim/lookup/touch/
   adopt and before actual start. Terminal completion/reply repair needs no
   model execution; its dispatchable payload stays private. The runner-client
   wrapper revalidates the selected context before each start, including the
   kernel's internal retry loop, so adapter admission is not its only check.
   Compose pinned binding plus clean-session substrate behavior without editing
   `kernel.py`, `consumer.py`, `threadlock.py` or `markers.py` in this release.
   Inspect every delegated method before implementing a facade; no unguarded
   `__getattr__` forwarding of a model-start capability. If a required seam is
   inaccessible, record the specific unavoidable core change and defer that
   dependent phase until a separately owned reviewed plan resolves other open
   kernel work. Exercise existing provoking concurrency tests unchanged and
   negative artifact substitutions; no sacred-module side-effect edits.
4. `charts/curie/templates/` protected worker/broker/guard surfaces, values/BYO
   profile and chart assertions; separate Docker provisioner tooling and tests.
   Follow chart backing-store toggle/BYO idiom and existing mandatory runtime
   preflight rules. Out-of-band owner provisions guards/credentials; API routes
   never become an administrative credential-mint endpoint.
5. Actual isolated ingress→broker→worker→runner→reply campaign: read succeeds;
   mutation/unknown-tool/approval produces no effects/cards; paused/restarted/
   newly joined old worker cannot access payload; proof expiry/races refuse
   before claim/enqueue; cold boot/parking/outboxes remain private; ordinary
   human tool/approval flow still succeeds in its own execution session.

The dependency observations identified in
[ADR 0191](../../adr/0191-protected-hook-delivery-authority.md)
cover the pinned Valkey ACL/Lua/TIME/INFO primitives and live-session
revocation. They do not qualify the protected runtime. Remaining measurements
include complete product key/command permissions, concurrency, persistence and
restart recovery; pinned SDK
clean-session/static-skill behavior; Docker preventive authorization and image
identity; Kubernetes admission controller/controller-created/ephemeral/init
pod coverage, ordinary worker RBAC/node reach and enforced network isolation;
actual connector effects with qualified credentials; transcript scoped-key
compatibility; complete execution-key and cross-store replay inventory.
Record dependency version, exact command and observation. Full preventive
guard, actual provider/connector and ingress-to-delivery campaigns must pass
before runtime qualification or readiness issuance. Any unavailable guard or scoped credential is an
enforceability blocker, not an implementation detail to defer after activation.

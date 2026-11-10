# Protected SDK lifecycle custody specification

Realizing specification for
[Accepted ADR 0245](../../adr/0245-protected-lifecycle-evidence-binds-host-owned-contexts-to-actual-sdk-closure.md),
tracked in [#4557](https://github.com/curie-eng/curie/issues/4557).
Architecture accepted with explicit maintainer approval in
[PR #5017](https://github.com/curie-eng/curie/pull/5017). This specification defines the
realizing sequence; it supplies no runtime qualification. Existing producers continue to
refuse unavailable genuine SDK/join evidence until the complete new profile is supported.

## Decision and trust roots

Use the actual measured runner process as the producer of its genuine logical SessionRunner
close, including an actual owned SDK-capsule wait and all owned task/thread/IPC/server
closures. Do not claim the runner reaped itself. Its final evidence-log write is explicitly
still open until exit; independently authenticated Kubernetes original-container termination
corroborates that evidence tail. ADR0242's special host-verifier exception is not used.
There is no new runner-parent custodian or persistent production signer.

The first profile is Linux Kubernetes, qualified_connectors=[], actual SDK static Skills,
fixture-only Read/Glob/Grep and explicit Skill names, fixed platform permission callbacks,
no arbitrary code/user command hooks/executable Skill metadata/local or remote
MCP/subagents/dynamic writable settings. Retain actual SDK setting_sources
user/project/local with a closed immutable measured inventory for each. The PreToolUse gate
must refuse builtin bypasses, mutation, unknown tools and approval before effects/cards.
Static Skill loading remains real SDK loading, not prompt concatenation. Exact pinned
SDK/CLI/helper inventory is a prerequisite; unknown factories or helpers refuse and cannot
be repaired by a final process census.

Each cold runner has exactly one planned SDK client object/capsule attempt. No reset,
reconnect, warm reuse, hidden retry or replacement capsule. A failed or unused intent
remains retained and prevents qualification. CLI version/capability helpers are explicitly
tracked child processes within that attempt, not invented SDK objects or extra SDK session
intents. An actual additional SDK object remains an unregistered session and aborts the run.
Public SDK construction must be demonstrated inert before the binding barrier; if the pin
performs useful I/O in construction, this profile is unavailable.

## Projection and guarded launch context

Introduce a distinct kubernetes-custodied-pod-v1 projection and protected-sdk-custody-v1 SDK
profile. Old v1 observations cannot be relabelled. Keep the existing static Skill projection
semantics and the original three Secret-slot purposes. Add explicit public delivery slots,
never unknown fields inside an existing Secret:

1. A read-only immutable installation ConfigMap at /etc/curie/lifecycle/installation.json,
   exact canonical closed
   {schema_version:1,record_type:protected_sdk_installation_context,installation_id,profile_digest,verification_key};
   Ed25519 public key is 64 lowercase hex digits. SDK profile does not include this
   blob/hash recursively; projection binds both the independent closed SDK profile and this
   public trust anchor.

2. A read-only immutable per-launch ConfigMap at /etc/curie/lifecycle/launch.json.
   Projection defines the closed delivery-slot grammar, while actual context
   bytes/UID/launch binding are observed per launch; dynamic per-launch values are not
   silently made static or excluded from launch checks. This is a reviewed new public slot,
   not a change to frozen BootEnv or reuse of CURIE_SESSION_ID.

The latter contains exactly
{schema_version:1,record_type:protected_sdk_launch_context,installation_id,profile_digest,context}.
context is exactly one tagged shape:

- probe:
  {kind:probe,run_id,probe_id,challenge,limits,max_duration_ms,runner_intent_id,sdk_intent_id}.
  Existing fresh probe ownership/budget/challenge remain authoritative; no synthetic
  campaign C. probe_id is the existing fresh primitive-probe UUID.

- trial:
  {kind:trial,run_id,ordinal,campaign,launch_id,tuple_digest,request_id,request_digest,runner_intent_id,sdk_intent_id}.
  campaign is the exact existing PC7 C, never a campaign ID alone. tuple_digest retains the
  full InstallationTuple encoding and independent reconstruction obligations.

- production: {kind:production,launch_id,launch_request_digest,manifest_digest}. These name
  the existing fully validated production launch, not caller-selected values. There is no
  commissioning run/C/intent exemption chosen by a caller.

Existing UUID/generation/sha256/PC1 limits grammars apply; unknown members/modes refuse and
missing context never falls back to production. ConfigMaps are immutable, uniquely
launch-owned, exact-content/UID recorded and guarded against update/delete/replacement until
ordinary retirement. No new runner token/credential mount, sidecar, init container,
exec/proxy/capability or host mount is introduced. The independently authenticated guard API
reader necessarily retains its own explicitly measured Kubernetes credential outside the
runner. New public mounts, delivery slots, effective ConfigMap RBAC and admission rules are
explicit reviewed changes in the new projection/guard composition.

The production/trial discriminator is a new closed mediator handoff with separate actual
Kubernetes writer/Pod-creator identities. Production and commissioning mediators are
separate measured processes/ServiceAccounts; neither holds the other's credential. No
generic mode input chooses the server. The production authorizer alone is trusted to run
existing PC4/PC7 production authorization before constructing production context; the guard
does not claim to observe which Python constructor ran or independently replay that private
authorization. It independently enforces the principal/kind separation below. This explicit
trusted-authorizer boundary is included in the measured profile and its mandatory
production-principal-refusal test.

New admission mapping (authenticated AdmissionReview.userInfo.username, never an object
creator field): host provisioner may create only installation public config and probe/trial
contexts; production mediator may create only production contexts; commissioning mediator
may create only typed assignment-request objects, and may not create any lifecycle context.
Production Pod creation requires the production-mediator principal and production context;
commissioning Pod creation requires commissioning-mediator principal and host-created trial
context; primitive Pod creation requires commissioning-mediator principal and a host-created
probe context. The provisioner creates public contexts, not probe workloads.
Missing/foreign principals and any cross-kind create refuse before Pod admission.
Ordinary/control/candidate identities get no context or request write right. No process may
possess both mediator ServiceAccount identities. These are explicit new Kubernetes
roles/process separation, not new broker ACL principals or reused credentials.

The guard uses authenticated Kubernetes API GET of the exact ConfigMap names in the
protected namespace to validate Pod context references. Add an explicit measured read-only
guard identity grant configmaps:get in that namespace, no list/watch, Secret, write, exec or
proxy grant. Current code is not assumed to have this right. The host independently
validates effective reader/writer RBAC and admission coverage. Host-created trial context is
never accepted via caller-supplied JSON alone.

All lifecycle context and request creates pass the new guard from the start: the fresh owned
namespace initially has no lifecycle contexts/requests, and the provisioner records that
baseline before admitting any workload. There is no adoption of an older unguarded context.
On ConfigMap CREATE the guard enforces the principal/kind mapping, immutable:true, exact
canonical closed data, unique launch/request identity, installation and profile; on
UPDATE/PATCH it rejects data, mode, labels, owner-reference or immutability changes; on
DELETE it permits only exact-UID retirement by the authenticated provisioner. Original Pod
termination/retirement is an independently observed trusted-provisioner obligation, not
something the configmaps:get-only guard claims to observe. Cleanup-only recovery may finish
retirement but cannot qualify. Configuration names/labels are not authentication. Their
authenticated origin follows from empty initial baseline, uninterrupted CREATE/UPDATE/DELETE
admission coverage and immutable original UID. Any coverage uncertainty, unknown preexisting
object or replacement invalidates the profile. The guard's own effective bypass/RBAC
protections remain required.

A Pod carries an immutable exact lifecycle-context UID/reference bound to the original
mediator/provisioner launch row. Its submitted volume name alone is insufficient: guard
freshly GETs original context, checks actual UID/content and the creator-principal/kind
mapping above, plus original launch ID/request digest, and denies substitution. Guard
forbids later Pod volume/env/annotation/role changes. These API reads are bounded, fail
closed and expose no object contents in errors. The runner has no Kubernetes credential: it
trusts the guarded mounted bytes for pre-release mode selection, then validates host-signed
release against those bytes. The host, not the runner, reobserves backing ConfigMap UIDs. A
production-kind downgrade by the commissioning mediator is therefore rejected at both
context creation and Pod admission, rather than merely detected later.

This extends projection, launch records, guard, effective RBAC and distinct mediator
deployment inventories. Those changes must land together before enabling the producer. They
are not silently inferred from the current CampaignRunnerAssignment or the old
five-namespace profile. All added resources/identities enter ownership/profile measurement;
activation keeps the same measured installation and entry point.

## Two intents and the actual-binding barrier

For a probe launch, the independent provisioner knows the exact fresh probe identity before
creating anything. It registers the exact runner_intent_id and sdk_intent_id, fsyncs
file+directory, then exclusively creates that probe's immutable context and observes/binds
its actual API UID. Only that original context can be referenced by the Pod. Ambiguous
context CREATE grants no creation permission and fails the run; the pending ownership intent
remains for exact cleanup.

The v2 primitive controller runs in the commissioning mediator, retains the fresh
per-probe runner bearer privately, and owns the real target and release relay. This is an
explicit composition change: the existing control-process probe adapter is not presumed
to supply that ownership. The host selects the fresh probe UUID and challenge before any
probe workload exists and supplies only the derived exact context name as a locator.
The controller independently GETs and validates the original host-created immutable probe
context, records its actual UID/digest and exact probe identity in the durable original
probe ownership row before creating any Pod/Secret/Service, and never generates a replacement
probe identity during boot. A locator alone grants no authority. Missing, duplicate,
conflicting or late assignments refuse without another probe attempt. Existing probe
budgets and fixed case ordering remain mandatory. Actual probe targets and their bearer
custody must be available before SDK readiness; the host receives neither bearer nor
network access. Both probe and trial paths exercise the same signed-release consumer.
The original probe registry retains its cleanup-only recovery and refusal of replacement
attempts; restart never restores positive probe authority.

For a trial launch use an explicit new immutable assignment-request ConfigMap as the bridge.
The commissioning mediator validates/reserves the actual PC7 launch and durably records
campaign ownership BEFORE awaiting assignment (current order must explicitly change). It
records a context-request creation intent before CREATE and creates exactly one request with
closed canonical body
{schema_version:1,record_type:protected_sdk_assignment_request,installation_id,run_id,ordinal,campaign,launch_id,launch_request_digest,launch_reservation_id,projection_digest,pod_namespace,pod_name,request_id,nonce,context_name};
nonce fresh 64-hex-character nonce, request_id fresh UUID, context_name derived
deterministically from launch_id by the fixed profile. It emits the request's original
name/UID as a bounded structured event in the original measured commissioning mediator's
log. This event is a locator only, not proof or permission; there is no prefix-based
resource discovery.

The host retrieves that exact request through its authenticated Kubernetes client and checks
original UID/content, expected writer class/guard coverage, original installation/C and
launch/projection/reservation binding. It independently corroborates the retained charged
reservation and original-C launch ownership through its existing dedicated read-only
evidence observer; unknown request IDs/operations or missing independent correlation to the
challenged case abort. The host registers the two exact fresh session intents in its private
exclusive custody and fsyncs file+directory. It then creates the named immutable trial
context, containing the two intent UUIDs and binding request_id/request_digest in addition
to the trial shape above (these are explicit required trial members). That original context
CREATE is the post-fsync acknowledgement. No RPC or control-writable marker stands in for
it.

The mediator only GETs that predeclared exact context name, within the precommitted
assignment deadline; it cannot create/update the context. It verifies original request
ID/digest/nonce binding, actual UID, complete context/C/launch/projection and host-kind
admission fence, then durably records that context UID/digest in its original launch
registry before any Pod/Secret/Service CREATE. A lost, late, stale, conflicting or
ambiguously created context fails the launch without positive retry; no alternate
context/SDK/Pod attempt is substituted. The two intents are specific to this one launch, not
a pool. Host and mediator retain all request/context UIDs and the original ownership records
through cleanup. Later SessionBindings use actually observed resource/process identities,
never preinvented UIDs.

The request and context have separate original ownership entries and bounded exact-UID
retirement, with ambiguous-create intents retained as blockers. Effective mediator request
CREATE/context GET and host context CREATE/request GET rights are explicit measured
namespace-scoped additions. Host log-reader authority for the original mediator and runner
is separately measured. Only one pending assignment per mediator and fixed precommitted
per-run ceilings are allowed. The existing twelve-fact channel and ordinary control markers
carry no new grant or receipt authority.

Runner boot establishes nondumpable/descriptor fences, validates public context, opens only
its bounded health/lifecycle server, spawns its one capsule, and prepares exactly one real
SDK client object without connecting it. The capsule factory assigns the SDK object a fresh
stable local instance UUID, sdk_session_id, and retains the actual object for that lifetime.
It is explicitly a platform instance identity of the real SDK client, not a claim that a
provider conversation ID is available before query. The SDK typed binding names this capsule
process {container_id,pid,start_ticks}. Its eventual CLI/helper identities are retained
separately. No SDK connect/query/permission callback/provider dispatch occurs before
release.

The capsule reports its original PID/start ticks and fresh instance UUID over private IPC
with kernel peer PID/UID checks, actual parent relation and original start ticks. Same UID
is insufficient. The runner emits prepared binding evidence through its exclusively owned
original container log; this is preparation only, not a completed join. The host directly
reads authenticated original-container logs, independently observes original Pod
UID/container ID/image ID/restartCount0 and admitted spec, reobserves public ConfigMap
UIDs/content, and durably binds both real runner and real prepared SDK-object/capsule
identities to the prior intents. Only then may it sign release.

A dedicated new POST /v1/commissioning/lifecycle/release accepts existing per-claim bearer
plus a host Ed25519 signature. Body is exactly {release,signature}; signature 128 lowercase
hex digits. Signed bytes are domain separator curie-protected-sdk-release-v1 followed by LF
and canonical release JSON. release is exactly
{schema_version:1,record_type:protected_sdk_release,installation_id,run_id,context_digest,installation_context_uid,launch_context_uid,pod_uid,container_id,image_id,profile_digest,boot_nonce,runner_intent_id,sdk_intent_id,runner_process,sdk_process,sdk_session_id}.
All UUID/opaque_ref/process fields use existing closed scalar grammars. boot_nonce is fresh
64 lowercase hex digits from original runner boot; context_digest is the exact per-launch
context canonical bytes, which bind either complete trial C/launch/ordinal or the
pre-campaign probe identity/budget.

The runner validates all fields it can observe (local immutable bytes, its original process
and capsule identities, object tag, boot nonce, intent UUIDs, installation/profile) and
signature under its measured public key. UID/container/image observations are
host-authenticated claims bound to this exact original boot, not runner self-discovery. A
fixed120000ms monotonic preparation deadline begins once at boot and cannot extend the
original probe/campaign/run deadline. It is not positive-work authority. At most one release
is accepted; duplicate, replay, changed C/context, wrong key, late release, signature
failure, restart or input after stop irreversibly refuses. Missing useful-operation binding
never becomes healthy merely because HTTP is reachable.

Release delivery does not give the host the mediator-private runner bearer or assume direct
host-to-Pod networking. After actual identity binding and signing, the host exclusively
creates one immutable release ConfigMap at the deterministic exact release name from the
original context/launch. Its sole canonical data record has exactly
{schema_version:1,record_type:protected_sdk_release_delivery,context_uid,context_digest,body};
body is exactly the existing {release,signature}. It is governed by the same host-only
CREATE/immutable/exact-UID provisioner retirement fence and original ownership ledger.
Context UID/digest and signed boot nonce bind the payload; no body includes its own
not-yet-created release-object UID. Ambiguous CREATE, replacement or late discovery fails
without positive retry.

The mediator GETs only that predeclared release name under its explicit measured
namespace-scoped configmaps:get grant, records original release UID/content digest durably,
checks the context UID/digest and signature, and forwards only those signed body bytes to
its original bound runner target through its existing per-claim bearer and existing target
custody. It does not learn host signing material or choose new commands/keys/targets. The
host directly reads the runner's subsequent original-container released event to corroborate
exact release digest; relay progress alone is insufficient. The read/forward is bounded by
the original preparation/campaign deadlines; duplicate/late/changed release or target
identity refuses. Release/context/request objects remain retained until original-runner
termination and evidence collection; provisioner alone retires exact UIDs, after independent
checks. Production creates no release object or fabricated trial intent.

The lifecycle app binds before SDK connect, matching existing health-before-slow-start
behavior. Mediator target custody and its bounded release relay must be available before SDK
readiness, without treating release as ready-for-turn; existing target identity/bearer
authentication remains mandatory. The host observes Running original container/prepared
evidence, not SDK-ready status, so there is no readiness/SDK-connect cycle. After release
the capsule connects the real SDK exactly once. The provider gateway still independently
refuses every dispatch without the actual valid PC7 launch and original Pod binding.
Lifecycle release grants no model spend, campaign issuance, turn, reply or production
authority. Unknown network activity during supposedly inert preparation makes the profile
unsupported.

Production context uses the same fixed capsule/supervision/tool policy but the existing
fully verified production launch admission as its start authority; it neither waits for a
discarded commissioning key nor supplies trial evidence. The closed guarded branch, not an
environment flag or caller hint, selects this behavior. The sealed public trust anchor,
code, projection and profile remain unchanged at activation; per-launch public context
values follow the new projection's predeclared delivery contract.

## Actual ownership, stop and cleanup

The capsule is the sole SDK-client factory. It supplies an explicitly version-pinned
OwnedCLITransport through ClaudeSDKClient's custom transport parameter. This is a NEW
reviewed adapter contract for this one pinned SDK profile, not reliance on a stable provider
promise: the provider calls Transport an unstable internal interface, although exported for
custom transports. Primary reference:
https://github.com/anthropics/claude-agent-sdk-python/blob/v0.2.163/src/claude_agent_sdk/_internal/transport/__init__.py
and the client parameter in
https://github.com/anthropics/claude-agent-sdk-python/blob/v0.2.163/src/claude_agent_sdk/client.py
. Do not subclass the provider default subprocess transport or inspect its private
process/task fields.

OwnedCLITransport owns the actual subprocess handles, stdin/stdout/stderr, reader tasks and
retained wait outcomes for the pinned CLI and each explicitly enumerated version/capability
helper. It uses the actual production protected options builder, accepts only the closed
measured supported options, and translates these to the pinned public CLI flags, whose
primary contract is https://code.claude.com/docs/en/cli-reference . It must configure every
required supported option, including permissions, Skills/plugins/settings, or refuse;
supplying a custom transport does not imply the SDK will configure these for it. Real
SDK/CLI conformance must exercise the actual consumer, not copied argument fixtures. All
subprocesses begin only after host release (or actual guarded production admission), and
their original PID/start ticks are observed from the exact owned handles before useful CLI
protocol traffic. Helpers have fixed enumerated roles and finite counts. New SDK versions,
unknown API/flags, hidden helpers, incorrect parentage, executable/config drift, additional
client factories, abandoned tasks/threads or unregistered children irreversibly refuse. A
final process census does not repair unknown history.

OwnedCLITransport has a closed no-reconnect state machine new→connected→closing→closed or
failed. Its public close starts irreversible shutdown before scheduling a bounded owned
teardown task. Repeated cancellation cannot detach that task; caller awaits its actual
outcome before cancellation is re-raised. Successful SDK disconnect must be followed by
independently checked exact transport closed state, all I/O joins and actual owned
subprocess waits; a failed disconnect/close cannot be repaired into success by later killing
its child. No pointer disappears while a process can still be live. This supplies actual
helper wait_status rather than inferring it from private SDK fields or a missing PID.

All child stdout/stderr and other descriptors are closed or redirected before untrusted
SDK/CLI code. Only the measured runner owns container stdout/stderr; SDK diagnostic output
is captured in a private bounded in-memory buffer (65536bytes per child); no diagnostic
bytes are forwarded to container stdout/stderr or inserted into lifecycle records. Buffer
overflow irreversibly refuses and starts cleanup. Normal SDK messages still use the existing
typed execution/ACI path, not this evidence stream. Nondumpable protection is established
before child spawn; it complements descriptor closure. The capsule inherits only its
enumerated IPC/diagnostic descriptors and existing narrowly scoped provider credentials, not
runner server/listener or lifecycle origin descriptors. Fixed callback/IPC ownership must
exercise production wiring; no SDK private process fields or monkey-patching is the claimed
proof.

Actual campaign supervisor observes expiry/exhaustion/revocation/broker-loss according to
existing broker checks, irreversibly fences further launch/dispatch, and requests
exact-original-UID Pod deletion. That deletion is the concrete SIGTERM/full-lifetime close
trigger; /v1/interrupt is not used as a lifetime join. The runner signal handler atomically
marks stopping, refuses release/reset/newquery/factories, revokes useful work, and performs
genuine SessionRunner.close. It awaits SDK disconnect in the capsule, all
callbacks/tasks/threads/readers, original CLI/helper cleanup, successful capsule exit, and
its actual parent wait. It then closes actual HTTP server/control connections and its owned
IPC/readers before terminal evidence and exit. A zero child exit code without genuine SDK
disconnect and complete work closure is insufficient.

Positive authority ends at its existing broker expiry; cleanup has a separate precommitted
interval and continues after authority/run expiry with no useful work. It never renews
authority or resets a budget. The Pod's existing five-second termination grace is unchanged
and applies once deletion starts. Forced kill, late/incomplete close, unbound attempt or
missing evidence is cleanup-only. No hard wall-clock closure is promised if the host/cluster
is unavailable. Successful qualification requires actual ordered close and terminal evidence
within observed original termination, not forced timeout inference.

The original Pod's protected finalizer keeps it observable after original container
termination. The host obtains complete logs only after that termination, brackets reads by
original UID/CID/image/restartCount0 and immutable spec, retains exact canonical bytes, and
only then completes normal finalizer removal/absence. Missing/rotated/truncated logs refuse
qualification; cleanup may still finish without repairing evidence. Different original
resource/container identities cannot pass via names or replacement objects.

## Closed evidence stream

Each canonical JSONL event has exactly
{schema_version:1,record_type:protected_sdk_lifecycle_event,installation_id,profile_digest,context_digest,boot_nonce,sequence,previous_digest,event,payload}.
Sequence is generation starting1, predecessor first64zeroes then sha256 of preceding
canonical line excluding LF. Each record<=16384bytes; stream<=4096records and67108864bytes;
no arbitrary chunks, paths or URLs. Hash integrity is not independent authenticity: only
original-container direct Kubernetes retrieval after source exclusivity and identity checks
supplies origin.

Closed event variants and ordering:

1. boot payload {runner_process,installation_context_digest,launch_context_digest}; original
   local process shape is exactly {pid,start_ticks}; there is no container_id member before
   authenticated host binding. This is not the final session binding grammar.

2. sdk-prepared payload {runner_instance_id,sdk_session_id,runner_process,sdk_process}; both
   local process shapes are {pid,start_ticks}. Exactly once after boot, actual inert client
   construction/capsule identity before connect. These instance UUIDs identify actual
   logical runner/client objects in every mode and are not commissioning intents. For
   probe/trial only, the host combines them with outer CID and the two prior context intent
   UUIDs into existing SessionBindings. Production records no phantom commissioning intents.
   A missing/prepared/failed client cannot create a binding by assertion.

3. released payload {release_digest}; exactly once after sdk-prepared, before any CLI/SDK
   transport open. Production instead emits production-admitted payload
   {launch_request_digest,manifest_digest} after sdk-prepared, no run release, matching its
   guarded context. No mixing paths.

4. helper-bound payload {role,process}; process {pid,start_ticks}, role exactly one measured
   enumerated helper role. sdk-opened payload {sdk_session_id}; only after actual SDK
   connect. Helper count/order and SDK-opened multiplicity are exact profile rules. A helper
   may not substitute its PID for the capsule identity.

5. stopping payload {reason}; fixed reason one of
   deletion,preparation-timeout,local-failure. Actual causal campaign reason stays in
   independently observed host/broker evidence; SIGTERM does not self-prove
   expiry/revocation. Irreversible at any state, no later useful events. Early failure
   retains all intents, including unbound ones.

6. helper-reaped payload {role,process,wait_status}; actual successful original helper
   wait/stop witness supplied by measured owner, not process-name absence. sdk-closed
   payload
   {sdk_session_id,sdk_process,disconnect_completed,owned_work_joined,transport_closed};
   strict true values only after genuine operations. Their boolean encoding is not its
   authenticity. Missing owner/public interface to actual helper wait makes the profile
   unsupported rather than synthesizing this event.

7. capsule-reaped payload {sdk_session_id,sdk_process,wait_status}; original owned capsule
   actual wait, zero successful exit required. No pending live helper/task/thread/IPC may
   remain. May precede later server closure only.

8. terminal payload
   {preceding_head,runner_instance_id,sdk_session_id,runner_close_completed,execution_transports_closed};
   preceding_head equals this record's previous_digest, never a self digest. Both booleans
   strict true after actual runner logical close and server/reader joins. Exactly one
   terminal, last event. It explicitly does not claim own process reap or closed stdout.
   Subsequent independently observed original container termination closes that evidence
   tail.

No generic error payload extends the grammar. Failure emits stopping and may emit bounded
fixed failure event {code}; code is an enumerated constant from the closed implementation,
never caller text. Any failure event forbids a successful terminal and acceptance. Complete
state machine/codec tests must reject duplicates, missing/order violations, unknown
fields/events and cross-context records. Numbering above describes event variants, not an
artificial requirement that all helpers stay live until stopping: a version/capability
helper may be bound and reaped during initialization before sdk-opened. Each main/helper
bound→reaped lifecycle is paired exactly once by original role/process identity. No helper
can start after stopping, while genuine outstanding joins may finish then. Successful
sdk-closed and capsule-reaped require every earlier helper paired/reaped. Terminal requires
actual runner/server closure; all earlier stream variants have closed mode-independent local
instance UUIDs. This stream is producer evidence, not a new receipt type or authority in the
existing twelve-fact channel.

## Required verification before qualification

Actual immutable SDK/CLI/helper inventory and source-bound factory/descriptor fences; all
mandatory PC5 primitives through real pinned SDK; positive owned-fixture read and real
static Skill loading; denial of executable Skill
metadata/settings/commands/MCP/subagents/unknown tool/approval; real callback ownership.
Protocol tests include wrong key/nonce/C/UID/context/kind downgrade, unused/unbound intents,
persistence failure before Pod create, useful work before release, replay/reset/reconnect,
same-UID IPC spoof, inherited stdout forgery, hidden/short-lived helper, PID reuse,
abandoned thread/cancellation, forced kill, five-second grace, container restart/name/UID
replacement/log truncation and genuine post-expiry closure. Stub-model/local-process tests
prove plumbing only.

Architecture acceptance authorizes implementation, not paid model calls, cloud spend,
deployment or runtime activation. Reviewed closed contracts must land before dependent
consumers. Actual tests may still show that the pinned provider cannot meet its finite
helper/join contract; that profile remains unavailable and returns to design review.

Existing #4908 fail-closed treatment of never-started/uncertain Pods is unchanged. Moving
ownership before assignment records more failure history but does not infer a container join
from absence or grant a cleanup exception. The independent start-gate discussion remains a
separate design question.

## Explicit artifact compatibility and realizing sequence

The existing ArtifactMeasurement v1 delivery_slots map is closed; the new public delivery
contract cannot be hidden under its old identifiers. Introduce a distinct
ArtifactMeasurementV2 with schema_version:2 and the same top-level members/kind as v1. Its
delivery_slots has exactly v1's members with schema_version:2 plus
lifecycle_context:guarded-lifecycle-context-v1 and sdk_lifecycle:owned-cold-sdk-capsule-v1.
Its launch_projection schema_version is 2, format kubernetes-custodied-pod-v1;
runtime_profile protected-sdk-custody-v1. All remaining canonical/member/image/platform
grammars stay exact. V1 stays closed and unchanged; no parser rewrites or missing-field
fallback. Explicit v2 consumers must support the whole new projection, slot and evidence
profile; an old caller refuses. Existing Manifest/Qualification/Readiness/ACI/plugin formats
gain no member. A contract-only reviewed PR must land before dependent runtime consumers. No
Curie/Cargo/Helm release-version bump is authorized by this protocol.

New projection schema_version:2 adds explicit lifecycle_profile
{format:protected-sdk-custody-v1,profile_digest,installation_context} where
installation_context is the original ConfigMap namespace/name/UID/content digest and fixed
mount. Its delivery slots define the exact per-launch public context/release handshake
above, separately from the three existing Secret slots. Per-launch original object
identities live in new typed launch assignment/registry bindings and are independently
observed; an immutable static projection never pretends to predict future Pod/ConfigMap
UIDs. The runtime record/version mappings and new control-profile identities must all be
supported before actual use. A publisher cannot accept generic v1 hashes as v2 observations.

Realizing order: separately reviewed closed artifact/projection/lifecycle record contract;
guarded host-owned context and separated mediator assignments; actual pinned
owned-transport/capsule logical close; genuine original-container evidence collector and
complete registry composition; real SDK conformance and full fresh trial series. The exact
closed protocol/spec still receives security/correctness review before consumers are enabled.
Pin's observed SDK
0.2.163 / CLI 2.1.286 identities are source audit inputs, not a qualification claim; actual
image/library/binary hashes and helper inventory are mandatory and unavailable until
measured.

Control-profile compatibility is also explicit: new ControlProfileV2 schema_version:2 keeps
v1's top-level shape but uses slot_policy:host-custodied-cold-sdk-v1; principals has exactly
the v1 controller-array members and provisioner, replaces the single mediator member with
production_mediator and commissioning_mediator, and adds guard_reader. All four
noncontroller principals are distinct, valid authenticated Kubernetes usernames and outside
controller/system identities. This is a separate closed record type/decoder contract, not
permission to mutate ControlProfile v1 or add unknown map members. These are Kubernetes
principals, not additions to B2 broker credential issuances; no existing mediator
CredentialReference is silently duplicated/relabelled. G2 retains its exact grammar and
binds the actual new admission/RBAC/profile artifacts through their existing digests. The
realizing contract PR includes full exact schema/negative vectors and explicit version
selection before dependent admission/projection consumers.

## Exact public object and locator encoding

All lifecycle objects use the existing protected workload namespace. The installation
ConfigMap name is `protected-sdk-installation`. Per-launch names are exactly
`lifecycle-request-<uuidhex>`, `lifecycle-context-<uuidhex>` and
`lifecycle-release-<uuidhex>`, with lowercase unhyphenated launch UUID; probe context uses
its probe UUID. Names are derived, never caller-selected lookup paths. Every ConfigMap
has `immutable: true` and exactly one canonical UTF-8 JSON value in `data`: respectively
`installation.json`, `request.json`, `launch.json` or `release.json`. Unknown data keys,
`binaryData`, duplicate JSON members and noncanonical bytes refuse. Original name,
namespace, UID and content digest enter the corresponding owned creation/launch record.
No matching name or label permits adoption of a preexisting object.

The mediator's assignment locator is its own closed canonical JSONL record with exactly
`schema_version: 1`, `record_type: protected_sdk_assignment_pending`, `installation_id`,
`run_id`, `campaign`, `launch_id`, and `request`. The last member is exactly
`{namespace, name, uid, digest}`, naming its actual created request. Standard existing
UUID, full-C, opaque-reference and sha256 grammars apply. This is a separate producer
record, not an extra opcode or arbitrary payload under the existing operator markers or
signed twelve-fact channel. Each locator is at most 16384 bytes, one per registered
assignment, under the fixed run record/byte limits. A locator grants no permission: the
host still independently retrieves and validates the actual original immutable request
and authenticated retained reservation/ownership before creating its context.

V2 selection is end to end. Mixed measurement/projection/control-profile versions refuse
without fallback. All four distinct Kubernetes principals and their actual effective
rights must be measured; unchanged B2/G2 broker grammars never imply duplicated credentials
or additional broker authority.

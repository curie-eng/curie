# Protected Kubernetes controlled cold launch

Realizing implementation contract for the existing
[LANE-2](2026-10-02-protected-hook-lane.md#runtime-manifest-v1),
[LANE-7](2026-10-02-protected-hook-lane.md#envelope-session-and-selected-runtime-binding)
and [LANE-8](2026-10-02-protected-hook-lane.md#preventive-provisioning-boundary)
under Accepted [ADR 0191](../../adr/0191-protected-hook-delivery-authority.md).
Tracked by [#3603](https://github.com/curie-eng/curie/issues/3603).
This selects a reversible Kubernetes realization of the existing provisioner
boundary. It introduces no new authority, application credential issuer or frozen
ACI field. Specification precedes implementation; source implementation and tests
do not establish deployment qualification, readiness or activation.

## Controlled launch owner

<!-- @spec PROTECTED-HOOK-LANE-2 -->
<!-- @spec PROTECTED-HOOK-LANE-8 -->

Implement one small **provisioner-owned launch/control mediator server**, its
private durable registry, and an authenticated worker client. Kubernetes only.
The server cold-creates a direct Pod from one immutable provisioner template;
it does not use the ordinary SandboxClaim/controller/warm-pool launch path.
The worker receives a mediator route and an operation capability, never a runner
bearer, Pod create rights, Secret read rights or generic Kubernetes client.
The mediator owns actual runner calls and serializes start/admit with retirement.
This is an implementation of the existing controlled boundary, not an ordinary
API endpoint, a new application issuer, or portable signed attestations.

Why Kubernetes: an immutable Secret object UID plus exact Pod references supplies
a concrete independently owned assignment record; explicit Pod UID deletion can
fence uncertain starts. Using a direct Pod avoids pretending existing Sandbox
controller defaults and mutable template copying are already qualified. This
requires an explicit direct-Pod observation path; do not forge a Sandbox owner.

The mediator is the sole runner-control ingress and runs as the existing
provisioner principal, with server and private durable state implemented together.
An endpoint-only client or parsed receipt cannot supply positive authority.
The existing delivery-lane contract retains authority over activation, broker
identity, incompatible rotation and qualification; this contract defines its
provider projection and registry transport for one initial Kubernetes format.

## Closed static projection: kubernetes-cold-pod-v1

<!-- @spec PROTECTED-HOOK-LANE-2 -->

Use the measurement-selected content-addressed file loader and existing canonical
ASCII JSON rules. Stored bytes must be canonical, at most 262144 bytes; strict
integer/string types, duplicate and unknown members refused at every level.
Use existing LANE-2 UUID, sha256, oci_digest, generation, CredentialReference and
opaque_ref grammars. No YAML, templates, expressions, JSON pointers or arbitrary
Kubernetes dictionaries are accepted. Digest is SHA256 of this whole object;
manifest.substrate.launch_config_sha256 and measurement.launch_projection select
it. It does not include its own digest, the execution-config digest or manifest
digest, avoiding a circular identity. Those are compared separately.

Exactly these top-level fields:

```
{
  "schema_version": 1,
  "format": "kubernetes-cold-pod-v1",
  "authority_domain_id": UUID,
  "launch_identity": opaque_ref,
  "namespace": dns_label,
  "service_account": dns_label,
  "runtime_class": dns_label,
  "image": {"reference": pinned_reference, "digest": oci_digest},
  "embedded_bundle": {"object_identity": opaque_ref, "sha256": sha256,
                      "path": "/bundles/current"},
  "platform": {"os": "linux", "architecture": "amd64" | "arm64",
               "variant": null, "runtime_profile": opaque_ref},
  "image_environment": [{"name": env_name, "value": string}],
  "environment": [{"name": env_name, "value": string}],
  "model_credential_ref": CredentialReference,
  "history_signing_credential_ref": CredentialReference,
  "resources": {"cpu_millicores": integer, "memory_mib": integer},
  "network_policy_ref": {"id": opaque_ref, "revision": generation,
                         "config_sha256": sha256},
  "delivery_slots": <exact measurement.delivery_slots object>
}
```

`dns_label` is `[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?`; env_name is
`[A-Z_][A-Z0-9_]{0,127}`. Environment is sorted by name, unique, at most 64 entries;
values are UTF-8 strings of at most 8192 bytes with no NUL. Pinned reference has
a nonempty repository plus `@` plus exact image.digest. Resources use existing
ProtectedExecutionConfig limits and exactly match that profile. The image must
match the manifest and the selected measured platform row, including its trusted
pinned reference. Embedded bundle equals the manifest bundle. Credential refs
match the closed execution profile and are distinct. No connection endpoint or
principal is selected by these data: refs resolve only in provisioner config.

### Exact environment ownership

Both environment lists are name-sorted, unique and bounded as above.
`image_environment` is the complete independently measured OCI image-config Env
map of the selected platform image, including inherited base-image values. No
credential-bearing or delivery-variable key may occur in it. It is not a worker
report. Qualification verifies it against that exact image, then the renderer
uses those exact nonsecret values without guessing inherited defaults.

The image must preserve the explicit settings in [the runner Dockerfile](../../../runner/Dockerfile):
`HOME=/home/runner`, `CURIE_RUNNER_PORT=8080`,
`CURIE_PROC_DUMPABLE_PRELOAD=/usr/local/lib/curie/libproc_dumpable.so`, and a PATH
whose actual measured value begins `/app/.venv/bin:`. The remaining PATH suffix
is the image's value, never a hardcoded substitute. The preloader file and its
behavior are part of image qualification. A different value requires a newly
qualified projection/image; source parsing does not qualify it.

`environment` is exactly the following nonsecret static output. Render through
[BootEnv](../../../packages/aci-protocol/src/aci_protocol/session.py) and its
`env_key`/`render_worker` mapping, not a second field-name table in implementation.
The names here document the current contract and are checked by source tests.

| Environment name | Static value |
| --- | --- |
| `CURIE_PLUGIN_DIR` | profile runner.plugin_dir, exactly `/bundles/current` |
| `CURIE_BUDGET` | `Budget(max_output_tokens_per_run=profile value, max_usd_per_day=float(Decimal(profile value))).model_dump_json()` including its default task_budget_hint |
| `CURIE_BUNDLE_REF` | manifest bundle.object_identity |
| `CURIE_MODEL` | profile runner.model |
| `CURIE_REVIEWER_MODEL` | profile runner.reviewer_model |
| `CURIE_FAKE_MODEL` | literal `0` (render_worker(False) alone omits this key) |
| `ANTHROPIC_BASE_URL` | profile runner.model_base_url |
| `CURIE_MODEL_API_BACKEND` | literal `messages` |
| `CURIE_THINKING` | literal `disabled` |
| `CURIE_MODEL_ENV_KEY` | literal `ANTHROPIC_API_KEY` |
| `CURIE_MAX_TURNS` | decimal profile runner.max_turns |
| `CURIE_MEMORY_WRITES` | literal `0` |
| `CURIE_HARNESS` | literal `claude-agent-sdk` |
| `CURIE_FALSE_COMPLETION_CHECK` | literal `0` |

The last two are runner-internal settings read by
[RunnerConfig](../../../runner/src/curie_runner/config.py), not new BootEnv fields.
The effective static environment is measured image_environment overlaid by this
exact profile-rendered list. A duplicate across the two is permitted only when
values are identical; otherwise the image is unsupported, rather than silently
changing its behavior. Materialize the complete static map explicitly in the Pod
and match it on observation. No envFrom, implicit service env, worker extras,
operator extras, OTel injection or connector configuration.

Only these six environment entries are added at launch:

| Environment name | Validator and source |
| --- | --- |
| `CURIE_SESSION_ID` | `execution_identity`: exact existing `agent-{agent_id}-thread-{protected_history_key(delivery)}` rendering; recompute from verified delivery |
| `CURIE_HISTORY_REF` | `history_binding`: selected runner-facing API base plus `/agents/{agent_id}/state/transcript/` plus percent-encoded exact protected_history_key, using quote(..., safe="") |
| `CURIE_SANDBOX_ID` | `workload_identity`: issuer's fresh Pod name, expressed as literal value and checked against actual metadata.name |
| `CURIE_RUNNER_TOKEN` | `runner_token`: SecretKeyRef to runner-token in issuer's exact immutable launch Secret |
| `CURIE_HISTORY_TOKEN` | `history_token`: SecretKeyRef to history-token in that same owned Secret |
| `ANTHROPIC_API_KEY` | static model_credential_ref resolution, SecretKeyRef to model-credential in that same owned Secret; it is not a free delivery slot |

`dial_identity` is control-plane identity, not another environment entry.
[protected_history_key](../../../apps/worker/src/curie_worker/protected_history.py)
and the current protected binding supply the deterministic history/session
algorithm. The signer receives its exact parsed protected transcript binding and
agent ID. Expiry is issuer time plus the profile execution deadline, optionally
shortened by the remaining verified delivery deadline; never worker-supplied
claims. Receipt records that exact expiry. It cannot expand the credential's
existing transcript scope. Model and history credential reference generations
remain static in the projection/profile; no credential bytes enter those records.

All other BootEnv fields are absent, including memory/state credentials and refs,
EXAMPLE_PRIVATE_CREDENTIAL_SOURCE, bundle_version, deployment_environment, approval markers,
progress/issue-read capabilities, attachments, channel/connector/repository fields,
history-window overrides and OTel fields. `render_worker`'s mandatory empty
memory_ref output is removed. No profile-required value may depend on an ordinary
worker's defaults. Profile deadline is a mediator control bound, not an invented
runner env variable. Source tests compare real rendering with this full key set.

Rendering is fixed by the format (these constants are therefore digest-bound):

- One `v1/Pod` with exactly the finalizer `curietech.ai/protected-launch`,
  owned exclusively by the issuer and protected against other-principal mutation.
  This is a digest-bound format constant. One container named `runner`;
  `imagePullPolicy: Always`, command
  `[/app/.venv/bin/python, -m, curie_runner]`, args empty, workingDir `/app`.
- `restartPolicy: Never`, `automountServiceAccountToken: false`,
  `enableServiceLinks: false`, `hostNetwork/hostPID/hostIPC: false`,
  `dnsPolicy: ClusterFirst`, `terminationGracePeriodSeconds: 5`.
- Named serviceAccountName/runtimeClassName from the projection; schedulerName
  `default-scheduler`; nodeSelector exactly `kubernetes.io/os=linux` plus selected
  architecture. Labels are selection hints only; Node.status.nodeInfo must still
  match the independently measured row. No nodeName at submission.
- Pod runAsNonRoot true, runAsUser/runAsGroup/fsGroup 1000;
  seccompProfile `{type: RuntimeDefault}`. Container privileged false,
  allowPrivilegeEscalation false, readOnlyRootFilesystem true,
  capabilities `{drop:[ALL]}`. No other security context member.
- Requests equal limits: CPU `<cpu_millicores>m`, memory `<memory_mib>Mi`.
- Three memory-backed emptyDirs mounted rw at `/tmp`, `/home/runner`, and
  `/scratch`, each sizeLimit equal to memory_mib Mi; fixed names `tmp`, `home`,
  `scratch`; no subPath. Container port 8080/TCP named `http`; no hostPort.
- No init/ephemeral/sidecar containers, probes, lifecycle hooks, volumes beyond
  those three, devices, host mounts, imagePullSecrets, controller, warm-pool,
  affinity, priority, topology, scheduling gates or annotations.
  Exactly two tolerations are rendered, sorted by key: `node.kubernetes.io/not-ready`
  and `node.kubernetes.io/unreachable`, each with operator `Exists`, effect
  `NoExecute`, tolerationSeconds integer 300 and absent/empty value. These are
  digest-bound format constants, not arbitrary admission extras. Their standard
  injection behavior is documented by [Kubernetes](https://kubernetes.io/docs/concepts/scheduling-eviction/taint-and-toleration/#taint-based-evictions).
  Private-registry authentication must already be a qualified node capability;
  requiring Pod imagePullSecrets is unsupported by this v1 projection.
- One fresh immutable Opaque Secret holds only runner token, scoped transcript
  credential and the exact resolved model credential. Pod env uses exactly those
  three SecretKeyRefs with `optional:false`. Secret is never exposed to worker.
  Pod does not mount the Secret as a volume. No Secret/projected service token.
- One mediator-owned headless Service with one TCP 8080->8080 port. Selector
  names an unguessable server-assigned launch label; actual EndpointSlice target
  Pod UID and IP are required, not selector equality alone. Only mediator may
  reach runner TCP; worker reaches mediator only.

This format has one bundle mode: the selected immutable runner image
already contains the exact eligible bundle at `/bundles/current`, read-only in
its root filesystem. No fetch/init staging path executes. Existing
`launch.fetch_image_digest` remains a profile binding but is unused for this
format, which must explicitly say so; `init_image_digests=[]` remains required.
Qualifying a base runner image with no embedded bundle does NOT satisfy this
contract. A newly built combined image needs a new manifest/qualification.
This avoids introducing a mutable volume or another unqualified execution image.

### Extraction and defaults

<!-- @spec PROTECTED-HOOK-LANE-2 -->

The same pure renderer is called by launcher and guard. The guard compares all
security-bearing submitted and admitted PodSpec members to this exact shape.
Unknown members refuse; absent defaults are filled only from the enumerated
constants above before comparison. No generic 'drop default fields' function.
The observation top-level object permits exactly apiVersion, kind, metadata, spec,
status; apiVersion/kind are the expected provider version/resource. Pod metadata
permits exactly name, namespace, uid, resourceVersion, creationTimestamp,
managedFields, labels, finalizers, ownerReferences, generation, deletionTimestamp and
deletionGracePeriodSeconds. Name/namespace/UID must match registry ownership;
resourceVersion is a nonempty bounded string and generation, if present, a positive
integer. Timestamps, if present, parse as RFC3339. Deletion fields must be absent
or null for positive observations. Owner references must be absent or empty for
this direct Pod. Labels are exactly the single `curietech.ai/protected-launch`
key with the issuer-assigned launch UUID. Finalizers must be exactly the single
`curietech.ai/protected-launch` entry until the issuer completes termination
verification during retirement. No annotations, generateName or other metadata
allowance. ManagedFields is control-plane bookkeeping only;
never mine it for trust or launch settings. Bound its aggregate serialized size
by the response cap and allow only the Kubernetes ManagedFieldsEntry keys
manager, operation, apiVersion, time, fieldsType, fieldsV1 and subresource. The
fieldsV1 subtree is the one opaque provider field-ownership trie; never executes,
selects or suppresses a spec check. Other unknown metadata members refuse.

Scheduler-assigned nodeName is a workload-identity observation, empty/absent in
the submitted Pod; bind it to the observed Node UID and measured platform.
Status fields are consumed solely by the explicit factual reader; they cannot
supply static profile values. The raw PodSpec checksum remains diagnostic only.
Besides renderer fields, normalize only the following Kubernetes serialization
defaults: Pod deprecated `serviceAccount` alias must be absent or equal to
serviceAccountName; container terminationMessagePath `/dev/termination-log`,
terminationMessagePolicy `File`, stdin/stdinOnce/tty false; mount readOnly false
and mountPropagation `None`; each toleration's omitted value equals empty string.
Explicitly render those defaults where the API permits it. Sort exactly the two
tolerations by key. An absent toleration list is not accepted on observation;
both required entries must be present, with no duplicate or changed duration.
Empty optional collections are accepted only where this contract explicitly
forbids members; null is not a generic synonym for absence. Any other injected
PodSpec member or changed environment refuses, including runtimeClass overhead
or added scheduling constraints. Qualify the actual chosen runtimeClass and
cluster defaults; do not promise compatibility with every cluster or chart.

Only named slots vary: execution identity and protected history are recomputed
from the committed verified delivery; runner token is launcher-generated 256-bit
randomness; history credential is minted by the existing scoped signer for that
exact transcript and deadline; workload identity is fresh generated object names,
API-returned UIDs and observed scheduling facts; dial identity is the actual
Pod/Service/EndpointSlice identity. Model credential issuance generation stays
static. Slots are not arbitrary environment paths. Store no credential bytes in
projection, measurement, receipt, logs or test evidence.

## Server, trust selection and registry

<!-- @spec PROTECTED-HOOK-LANE-2 -->
<!-- @spec PROTECTED-HOOK-LANE-8 -->

Ship server and worker client together, for example new isolated modules
`protected_launch_server`, `protected_launch_client`, `protected_launch_records`,
`protected_kubernetes_projection`; preserve the existing measurement codec and loader.
The server entrypoint runs outside ordinary API/worker processes under the
provisioner owner. A client cannot construct a production launch adapter without
explicit provisioner TLS config, the selected immutable identities and that
server connection. No ambient URL, kubeconfig, proxy, default credential or
ordinary API fallback. Loading a configuration is not qualification.

Choose HTTPS mTLS with explicitly provisioned CA, server-name validation and a
server SPKI pin selected in the provisioner's trusted config. The server maps an
exact client certificate public-key identity to a provisioned worker principal,
authorized runtime/domain and allowed operations. This mapping is local
provisioner configuration, never a requested identity or HTTP header. No platform
API key, bearer-only access, redirects, HTTP environment proxy or auto-retry.
Use bounded bodies, existing canonical codec validation, 5-second absolute control
request deadline and safe fixed error codes. Stream duration is separately bounded
by the execution deadline; no prompt/token/endpoint in transport diagnostics.

The server owns one SQLite registry on provisioner-only persistent storage, one
writer process and an exclusive OS lock held for the server lifetime. This is
local state inside existing authority, not application Postgres or ordinary
Valkey. The initial implementation is single-host/single-server, no HA lease or
leader-election claim. Replacement cannot start until the prior process is
confirmed stopped and its runner-control network path fenced. An unreachable old
host blocks failover; an expiring lease alone never transfers launch authority.

Registry durable records: launch_id UUID; worker_principal_id opaque_ref;
request_id UUID; exact request_digest; runtime_id/generation; manifest_digest;
qualification_id/generation; measurement_ref; execution_config_digest;
event_id; committed_stream_id; payload_digest; execution_session_key;
projection_digest; guard_identity; lifecycle revision; state; boot_id UUID;
object names/UIDs; private Secret assignment hashes/references; operation
journal; owned epoch; retirement outcome. SQLite stores no control capability, runner bearer, history/model credential or
source signing key, original source_fields or decoded prompt, including in journals/WAL. During a single server boot the
control capability and runner bearer may stay in private process memory. Runner
bearer recovery for bounded owned stop operations may read the exact immutable
Secret through the issuer's existing Kubernetes authority and check its recorded
UID and assignment digest. No additional secret store or issuer is introduced. A database
row alone is never authority; only the authenticated server with its registry,
Kubernetes controls and current verified control facts can issue a response.

On server restart refuse all positive operations on prior boot_id launches,
mark them retiring, revoke routes and delete their exact Pod UIDs before new
launches for those deliveries. A create retry referring to an earlier boot returns
retired/conflict, never a successful old capability or positive old launch. Recover uncertain create by recorded unique name
and registry intent plus verified API UID; never adopt a merely label-matching
Pod. Retain tombstones across restart. Unknown create outcome is not a reason to
create a second Pod. Retrying an operation ID with changed bytes is conflict.

## Exact HTTP contract

<!-- @spec PROTECTED-HOOK-LANE-2 -->
<!-- @spec PROTECTED-HOOK-LANE-7 -->

All request/response objects below are closed; schema_version is integer 1.
UUID/digest/ref types use the existing grammars. HTTP errors have exactly
`{schema_version:1, code: <invalid|unauthorized|conflict|unavailable|retired>}`;
no raw upstream text. Read/stop metadata has a 16384-byte canonical cap. Launch HTTP bodies have a
2097152-byte cap to accommodate JSON string escaping, with decoded original
payload bounded at 262144 UTF-8 bytes and protected_envelope at 16384 bytes.
Start requests retain the existing 262144-byte event/payload bound, and complete
control-plane observations are capped at 1048576 bytes. Enforce a bounded read
before parsing. Receipt revisions use generation grammar; event_id uses the
existing protected opaque_ref restriction, and epochs use the runner's existing
bounded base64url grammar. Only authenticated owner may access its launch. Receipt
IDs never select a different server, namespace or authority domain.

`POST /v1/launches` request exactly:
`{schema_version, request_id, runtime_id, runtime_generation, manifest_digest,
 event_id, committed_stream_id, payload_sha256, execution_session_key,
 source_fields:{payload:string,protected_envelope:string}}`.
The two source_fields strings are the byte-exact originals retained by
Dispatchable.source_fields, not reserialized parsed records. After decoding the
outer HTTP object, encode each original string as strict UTF-8 to recover the
original bytes; normalization, whitespace changes or re-encoding a parsed object
is not permitted.
The server independently reads the immutable committed binding and current
protected authority over its provisioned authenticated read capability, verifies
the complete tuple and selects profile/measurement/projection itself. No worker
manifest body, image, token, env, Pod JSON, registry path or qualification boolean.
It runs the existing
[verify_delivery](../../../packages/protected-hooks/src/curie_protected_hooks/worker_verification.py)
against these exact strings using its real pinned private broker read client,
including durable binding, intent and commit proof; it never trusts the worker's
payload hash or parsed Dispatchable as authorization. Match the full verified
envelope to the selected manifest and compare all request identities, including
payload_sha256 computed from the original payload bytes and committed stream ID.
Do not require XRANGE of the original stream entry: trimming and private recovery
copies do not erase the independently retained binding/commit authority.

Retain exact source_fields and verified delivery only in this boot's private
memory for the launch. Every positive start/admit re-runs actual broker
verification against those original strings, never a canonical reconstruction.
SQLite/journals, logs and artifacts contain no original payload/envelope strings
or decoded prompt. Registry stores only the bounded identities and hashes needed
for ownership and crash cleanup. Preserve the original Event/nomination attribution
from verified queued bytes when comparing each requested model start; no
worker-supplied replacement identity can become first-nomination authority.
After restart old launches admit no positive operation; first finish owned UID
cleanup, then a worker may submit a fresh request with both original strings and
fresh independent broker proof. This does not authorize quota refund, a new
finder/census mechanism, ordinary recovery copies or frozen wire fields. Existing
protected retention and terminal-settlement requirements remain separate.

It derives the launch from that committed delivery. Before any Kubernetes effect,
it durably stores the preparing record, request digest, assigned launch ID and all
planned object names, and registers owned cleanup. It then starts exactly one
owned asynchronous creation job for that request; no model work occurs in this
job. Each individual HTTP control request retains its five-second absolute limit;
Pod image pull, scheduling and SDK readiness do not run inside a long create
HTTP request.

An initial create returns HTTP 202 with the closed `LaunchPendingSuccess` shape:
`{schema_version:1, launch_id, receipt_revision, state:"preparing", receipt:null,
 control_capability}`. Until the server proves idle with a complete receipt, this
capability authorizes only owned observation and stop/retirement; it cannot
start or admit a turn. The ready `LaunchSuccess` shape is exactly
`{schema_version:1, launch_id, receipt_revision, state:"idle", receipt,
 control_capability}`, with a non-null fully proved receipt. HTTP 201 is allowed
only if the first response can already prove ready within its control-request
budget; an identical same-boot retry returns 202 while preparing or 200 when idle,
with the same launch ID and capability. It never schedules another creation job.
Changed request bytes conflict. Old-boot retries remain conflict/retired and
cannot recover positive authority.

The creation job has one absolute monotonic deadline, capped at the minimum of
120 seconds, the profile execution_deadline_seconds, and the remaining
independently verified delivery execution deadline when present. An exhausted or
nonpositive budget refuses before effects. Persist the selected bounded creation
budget with the intent; after restart do cleanup, not a resumed creation job or
a reset timer. Deadline, readiness failure, cancellation or ambiguous effect
moves the launch to retiring and invokes actual owned cleanup. Neither a timeout
nor a failed observation permits a second Pod/create RPC. Reconcile uncertain
creation only through recorded names, verified UIDs and the existing retirement
rules. Register cleanup before launching the job and join/cancel the owned job
on retirement or server shutdown. Serialize each create effect and its ownership
journal update against retirement, and recheck lifecycle before every next effect;
no stale background job may create objects after retirement fences it. Missing
cleanup proof leaves retiring rather than creating replacement capacity.

After a pending response the client uses explicit bounded GET polling of this
launch's existing status resource, each request at most five seconds and shortened
to the caller's remaining claim budget. Poll at most once per second, with at
most 120 observations and no extension of the original whole-claim deadline.
Ready requires state idle plus the fully validated non-null receipt. A malformed,
unauthorized or failed read is not readiness; it does not trigger transport or
start retries. Caller cancellation, exhausted claim budget or an unsuccessful
terminal state ends polling and requests bounded owned retirement. A failed
retirement request does not release ownership; the server's independent creation
deadline and existing cleanup/restart paths still apply. Polling cannot make a
new launch, duplicate an RPC or keep a background creation job alive indefinitely.

`control_capability` is 32 random bytes base64url without
padding (43 chars), bound to authenticated worker principal and launch_id; it is
never a runner bearer. Return over authenticated transport only; hash it in the
registry; retain raw capability only in this server boot's private memory so an
identical create retry by the same mTLS owner can return it without another Pod.
Never persist/log it as receipt data. A retry after memory/boot loss refuses
positive reuse and retires the old launch. Client must additionally send it in
`X-Curie-Launch-Capability` for existing-launch operations. No general capability
read or rotation endpoint exists. Bounded stop authorization after restart uses
mTLS principal plus registry ownership; it does not require a lost old capability
or permit positive operations. Same-boot reads/stops still require the capability.

Receipt object exactly:
`{schema_version, launch_id, authority_domain_id, runtime_id, runtime_generation,
 manifest_digest, qualification_id, qualification_generation,
 execution_config_digest, measurement_ref, launch_identity, projection_sha256,
 guard_identity, event_id, committed_stream_id, payload_sha256,
 execution_session_key, workload, assignment, platform, route}`.

- workload: `{namespace,pod_name,pod_uid,container_id,started_at,node_uid,
  service_name,service_uid}`; all observed from owned Kubernetes API objects.
- assignment: `{secret_name,secret_uid,secret_resource_version,immutable:true,
  runner_token_key:"runner-token",runner_token_sha256,
  history_token_key:"history-token",history_binding_sha256,
  history_signing_credential_ref,history_expires_at_ms,
  model_key:"model-credential",model_credential_ref}`.
- platform: exact measured mapping row, after matching actual Node and container
  observations. The issuer cannot invent qualification merely by returning it.
- route: `{kind:"mediator-v1",launch_id,worker_principal_id}`. Never Pod URL,
  runner token or a URL supplied by a worker. HTTP origin is trusted local config.

The assignment proof is the authenticated response from this registry-owning
server: the server generated/obtained the values, created that immutable Secret,
checked returned UID/resourceVersion and exact bytes against its private source,
then observed exactly those keyRefs in the owned Pod. UID/name alone is not proof.
Secret cannot be recreated or remapped while Pod runs; guard/RBAC protect both
Secret and Pod assignment against every other identity. Changes, missing proof,
Secret recreation or unexpected resourceVersion refuse start. Secret material
is never returned. This replaces the reader's SecretKeyRef refusal only on this
new authenticated provider path, not by relaxing its token test globally.

`GET /v1/launches/{id}` response exactly
`{schema_version,launch_id,receipt_revision,state,receipt}`; state one of
`preparing|idle|waiting|running|retiring|retired|failed`; receipt null until proven.
This is observational only. A copied response/dataclass cannot authorize calls.

`POST /v1/launches/{id}/start` request exactly
`{schema_version,operation_id,expected_receipt_revision,capacity_admission,event}`.
Event is the unchanged existing ACI Event, validated against this delivery and
its exact accepted payload; no alternate prompt or policy. The server performs
the full guard below, opens the actual runner stream, durably records its exact
returned epoch, then streams unchanged ACI frames. Headers return `X-Curie-Launch-Id`,
`X-Curie-Operation-Id` and the exact existing `X-Curie-Turn-Epoch`. Same operation replay never issues a
second runner POST; ambiguous response means stop/fence, never retry start.

`POST /v1/launches/{id}/admit` request exactly
`{schema_version,operation_id,expected_receipt_revision,epoch,allow}`.
Positive allow rechecks the full chain and authenticated waiting epoch. Negative
allow is stop-only and remains authorized after execution proof loss.
Success response `{schema_version,launch_id,operation_id,epoch,result:"applied"}`.

`GET /v1/launches/{id}/status` and `/capacity-status?epoch=<owned epoch>` return
`{schema_version,launch_id,receipt_revision,runner_status}` using unchanged
validated existing runner status response from
[the server status surface](../../../runner/src/curie_runner/server.py); never make an execution claim from that read.

`POST /v1/launches/{id}/interrupt` and `/timeout` requests exactly
`{schema_version,operation_id,epoch}`. Interrupt uses a fixed server-owned reason;
no generic forwarding. Epoch must be owned; success same applied response.

`POST /v1/launches/{id}/retire` request exactly
`{schema_version,operation_id}`; response exactly
`{schema_version,launch_id,operation_id,state:"retiring"|"retired"}`.
202 means cleanup incomplete, 200 retired means fencing and cleanup confirmed.
No steer, execute, workspace, approval, snapshot mutation or arbitrary-path
proxy endpoint. `turn_deadline_s` is local pure calculation only.

## Start, capacity and retirement fence

<!-- @spec PROTECTED-HOOK-LANE-7 -->
<!-- @spec PROTECTED-HOOK-LANE-8 -->

Each launch has a server-owned mutex; immutable registry authorization and journal
updates occur in durable transactions. Starts/admit-true and retire share that
mutex. Hold it through outbound runner request acceptance and epoch capture,
not the whole event stream. Immediately before each outbound positive call:

1. Independently verify committed delivery, exact payload/session, current live
   protected broker run_id, generation/qualification and execution authority.
   Admission-evidence expiry by itself follows existing accepted-drain semantics;
   it cannot invent revocation or delete accepted payload.
2. Require owned unretired registry row, same server boot, manifest-selected
   measurement/projection, assignment and control-plane workload/endpoint facts.
3. Extract exact static projection and match independently measured image/platform
   row; require unchanged Secret assignment and zero restarts.
4. Authenticate actual runner status using issuer-only bearer, and check exact
   runner enforcement/session freshness. Start requires idle clean session;
   admit-true requires this operation's waiting epoch on this same actual target.

Worker also verifies authenticated response tuple and its current dispatch
context before calling. That client check supplements server enforcement; it
cannot replace it. Server never accepts a worker boolean saying checks passed.

Retire durably transitions to retiring under that same mutex before any later
positive call may proceed. Revoke positive capability use while preserving owned
stop/read operations; stop the existing epoch and request Pod deletion with the
exact UID precondition while retaining the issuer finalizer. Read the authenticated
owned Pod until the exact recorded runner container_id has state.terminated, with
its termination fields present, and record that observation durably in the
retirement journal before removing the finalizer. The observation includes Pod
UID, container ID, API resourceVersion, exitCode and finishedAt; no prompt or
credential. It must describe this runner, not another terminated container or
an earlier restart. No background/host execution escape is permitted by this
initial profile; actual runtime termination semantics still require qualification.

Only after this proof may the issuer remove exactly its own
`curietech.ai/protected-launch` finalizer, using UID/resourceVersion checks without
rewriting other metadata. Confirm the same Pod UID is absent, then remove only
UID-owned Service and Secret. A crash after proof persistence may resume finalizer
removal/absence confirmation from that exact journaled observation. A bare
NotFound without prior durable termination proof is not a retired launch.
Unreachable node, missing finalizer, missing/mismatched termination proof or
unconfirmed deletion leaves state retiring; never force-delete or infer shutdown
from API disappearance. A separately established out-of-band node/runtime fence
may resolve the case under provisioner authority; this v1 worker HTTP protocol
cannot assert or manufacture that evidence.
Runner ingress stays exclusive to mediator. No retired/retiring row returns to
active. Every new delivery/retry cold-creates a new launch. If start won the mutex
first, retirement stops that accepted start before reporting retired; if retirement
won, there is no later start POST. Uncertain outbound acceptance transitions to
retiring and fences the Pod rather than retrying. Guard/credential changes use
LANE-5's close/revoke/terminate-before-change order as well.

Proof-loss must not disable status, deny, interrupt, timeout or retirement for
an established owned target. Those use retained ownership/epoch and bounded
credentials only, not fresh positive qualification. If the runner is unreachable,
server may request deletion by UID but must retain retiring until termination
is established through the finalizer protocol; if Kubernetes is unreachable, report retiring
and retain ownership, never release capacity on assumed cleanup. Provisioner
has a separate out-of-band stop path if mediator itself fails.

### New internal types; no ACI changes

Use separate types: `WorkerLaunchCapability` (mediator route, launch ID, mTLS owner,
opaque control token), `IssuerPodBinding` (private actual handle and runner token),
`AuthenticatedLaunchObservation` (validated response plus client-origin binding).
Never use one SandboxHandle to mean both routes. The existing Kubernetes reader
requires a real Pod/Service handle and raw token match; it cannot observe a proxy
handle. Keep that behavior. A new issuer-side direct-Pod reader consumes
IssuerPodBinding plus registry-owned Secret assignment, and reuses factual
Pod/Node/Service/EndpointSlice checks where compatible. Worker guarded substrate
maps its kernel handle to WorkerLaunchCapability privately and only the explicit
mediator runner client can use it. Generic RunnerClient must never receive it.

ProtectedBindingResolver currently resolves credentials and generates a runner
token in the worker. Split its deterministic nonsecret delivery rendering from
credential resolution; the mediator owns positive launch rendering and minting.
No worker-generated environment/token is promoted into an assignment proof.
Existing measurement parser, raw launch_document_sha256, facts parser and parsed
receipt remain non-authoritative building blocks.

## Implementation ownership and source evidence

<!-- @spec PROTECTED-HOOK-LANE-2 -->
<!-- @spec PROTECTED-HOOK-LANE-7 -->
<!-- @spec PROTECTED-HOOK-LANE-8 -->

1. Implement projection renderer/extractor plus strict request/receipt codecs
   against this contract; reuse the
   selected measurement loader. Golden canonical vectors and mutation refusals.
2. Implement owned mediator server, private durable registry and real HTTPS mTLS
   client together. Implement credential assignment and direct-Pod read adapter.
   No client-only success path, no fixture-issued production receipt constructor.
3. Implement explicit worker substrate/runner wrappers and move launch secrets
   from worker boot rendering to issuer; wire entrypoint only when all exact
   provisioner inputs exist. Keep ordinary kernel modules untouched.
4. Run source integration tests against the real server/client and fresh isolated
   real protected broker; fake only Kubernetes API and runner external endpoints.
   Authenticate their transport in tests. Never fake both registry issuer and
   client. These tests prove source behavior, not Kubernetes preventive controls.

Required source tests: wrong CA/name/pin/client identity/capability; duplicate and
unknown fields; pending202/ready response grammar; creation longer than a control
request without extending its deadline; same-boot pending retry with one job and
one Pod; bounded creation/poll timeout, cancellation and cleanup-before-job;
no positive control with a pending receipt; exact original source-field
preservation after stream trim;
reverification against changed binding/intent/commit and payload hash mismatch;
no original bytes persisted in SQLite/WAL; forged/copied receipt; receipt mixing across worker/event/runtime;
projection roundtrip and every unsupported field; canonical/default handling;
Secret immutable/reference/UID/resourceVersion/key/assignment substitutions;
actual imageID/platform mismatch; endpoint/UID/restart/token mismatch; retained
ordinary identities without launch/control authority; second start revalidation;
start-vs-retire and admit-vs-retire barriers around actual outbound POST; proof
loss with stop still effective; wrong/stale waiting epoch; forbidden steer;
uncertain create/start; crash after each registry/Kubernetes effect; restart
refuses old positive capabilities; incomplete deletion never means retired;
finalizer preservation, exact terminated-container proof before finalizer removal,
crash between proof/removal/absence, and NotFound-without-proof refusal;
no shared or globally swept resources. Assert server actually owns/writes the
registry and actual client output is consumed by worker wrapper.

## Qualification boundary

<!-- @spec PROTECTED-HOOK-LANE-8 -->

No deployment is qualified. Out-of-band provisioner must install and measure
fail-closed admission covering Pod create/update/ephemeral/debug and every
controller/operator route; Secret/Service/EndpointSlice mutations; guard/RBAC/
namespace/runtime policy changes; ordinary node/host/admin reach; mTLS trust and
registry storage ownership; and the real network restriction making mediator the
only runner ingress. Namespace or NetworkPolicy declarations alone are not proof.
Its selected Kubernetes version/admission defaults must match this projection.

It must additionally qualify the embedded-bundle runner image on the actual
architecture/runtime, exact static Skill/no-lifecycle behavior, current SDK
clean-session/tool enforcement, history-scoped credential behavior and actual
provider use. Existing qualified_connectors=[] stays empty; this format adds
no MCP/operational connector. Source fake-runner tests cannot issue measurement,
readiness or support, and do not close the actual ingress-to-reply campaign.

At specification time there is no owned mediator server/registry, authenticated
Secret assignment adapter, proxy-versus-Pod internal type split or retirement
fence. The current binding still generates tokens in the worker. Those are named
source dependencies; the required preventive boundary and real runtime evidence
are separate qualification work. No sample values or passing source tests fill
those evidence slots.

## Disjoint implementation briefs

- **Projection and records owner:** own new projection/launch-record modules and
  their tests; implement canonical closed codecs, real BootEnv rendering,
  image-environment binding, provider extraction/defaults, request/receipt types
  and malformed/substitution vectors. No registry, server or existing measurement
  codec edits. Hand immutable records and pure render/extract functions to server.
- **Registry and server owner:** own new registry/mediator modules and tests;
  implement single-process SQLite ownership, no-secret persistence, authenticated
  server, direct-Pod creation/assignment reader, registry journal, real control
  serialization, same-boot idempotency and crash/retirement cleanup. Depend on
  records; fake only external Kubernetes/runner endpoints for source plumbing.
  Prove the production server is the receipt producer consumed by the real client.
- **Client and worker wrapper owner:** own new mTLS client and explicit protected
  substrate/runner wrappers and their tests; separate proxy capability from
  issuer Pod identity, move positive secret creation behind mediator, and wire
  guarded protected entrypoint only with exact dependencies. Depend on records
  and server HTTP contract; integrate against real server once available. Changes
  to existing protected binding/entrypoint files have one coordinated owner.

Do not edit kernel, consumer, threadlock or markers as a side effect. No frozen
wire, chart release version or ADR change is part of these implementation briefs.
The server owner and wrapper owner agree the module APIs before shared-file
integration; disjoint source tests can proceed independently, then the final
combined candidate requires the repository's complete applicable verification.

# Protected runtime commissioning

Realizing specification for
[ADR 0224](../../adr/0224-protected-runtime-commissioning-precedes-installation-acceptance.md),
with the operator process split of
[ADR 0236](../../adr/0236-the-commissioning-operator-splits-provisioner-and-control-processes.md).
Tracked in [#4557](https://github.com/curie-eng/curie/issues/4557). It builds on
[ADR 0190](../../adr/0190-automated-hook-sources-cannot-widen-their-tool-access.md),
[ADR 0191](../../adr/0191-protected-hook-delivery-authority.md), the
[delivery lane](2026-10-02-protected-hook-lane.md) and the
[source policy](2026-10-02-protected-hook-source-policy.md) contracts, and the
shared bootstrap grammar of [#4076](https://github.com/curie-eng/curie/issues/4076).

This specification was approved by the maintainer on 2026-10-20. It changes no code, no
record format and no ADR. It precedes any change to authority evaluation or to
the closed record formats, as ADR 0224 requires. It claims no qualification,
provisioning or activation evidence. The design choices that the Accepted ADRs
leave to a realizing specification are fixed under
[Design decisions](#design-decisions), each with the clauses it rests on.

## Scope

In scope are the stages ADR 0236 plans: one disposable local Kubernetes
installation, the local Cilium network producer, the `curie dev` operator
command, and a campaign whose execution profile has `qualified_connectors=[]`,
no executable lifecycle hooks, no operational target access and no production
approval authority. The only target is an owned read-only fixture. The campaign
uses the actual signed ingress, protected broker, worker, mediator, runner and
reply path. Fake-model plumbing and direct provider replies do not satisfy it.

Out of scope until separately decided: the Docker substrate (no commissioning
composition is declared for it, so it refuses), a managed cloud CNI producer and
the qualification of any production installation that depends on one, a
`curie cluster` command form, any connector-bearing profile, and any choice of
static Skill projection beyond the existing `kubernetes-cold-pod-v1`. Every
slice of this specification rests only on the ADRs linked above. Production
qualification of a managed CNI installation additionally depends on a separate
ADR (decision 3 under [Design decisions](#design-decisions)).

Implementation lands in four dependent slices. A slice cannot enable production
or claim that a later slice passed.

1. Closed records and pure binding validation (clauses 1, 2 and 7).
2. The authenticated control journal and aggregate reservations on a real
   isolated broker (clause 3).
3. Commissioning composition: ingress, worker entry, mediator authorizer,
   provider gateway and supervisor (clauses 4 and 8).
4. The operator command, campaign, acceptance and activation (clauses 5, 6
   and 9).

## Principals and key families

ADR 0236 places the decision in the provisioner process and the in-cluster
producers in the control process. This specification assigns broker authority
to match.

| Principal | Process | Authority | Never |
| --- | --- | --- | --- |
| Provisioner | Provisioner | Creates the installation, broker ACL users, bootstrap, guard and manifest; writes `protected:control:selection:*` and `protected:control:manifest:*`; disables principals and kills their sessions. | Writes a permit, journal, Qualification or Readiness. |
| Commissioning issuer | Provisioner | Writes each permit once with `SET NX` under `protected:commissioning:permit:*`; writes the acceptance record. | Writes journal or reservation keys, or production control keys. |
| Commissioning control | Control Pod | Compare and set on `protected:commissioning:journal:*`, `protected:commissioning:reservation:*` and `protected:commissioning:ownership:*`; writes committed campaign admissions. | Writes a permit, acceptance, or any `protected:control:*` key. |
| Candidate enqueue | Commissioning ingress | Reads permit and journal; submits bounded intents to the campaign lane. | Writes committed admissions, permit, journal, or any production key. |
| Candidate worker | Commissioning worker | Reads permit, journal and committed admissions; consumes the campaign lane. | Writes control, permit, journal, admission or production lane keys. |
| Production enqueue, worker, verifier | Production entry points | Unchanged LANE-3 roles from the manifest's `credential_refs`. | Reads or writes any `protected:commissioning:*` key. |
| ACL observer | Verifier and guard reobservation | Reads broker ACL users with `ACL GETUSER` only, for the check of [design decision 7](#7-acl-principal-state-is-observed-not-hashed). | Writes any ACL user, or reads or writes any key or channel. |
| Ordinary worker, source, runner | None | No broker authentication. | Everything above. |

All seven broker identities and the ACL observer are distinct ACL users with
distinct credential issuances. No principal holds a wildcard write grant that covers another row's
keys. The realizing ACL inventory is measured against the pinned Valkey
version, including Lua key selectors, as LANE-3 requires. If the pinned version
cannot separate these rights, the campaign refuses; it does not weaken a role.

The campaign lane derives its broker names from the runtime and campaign
identity the way `packages/protected-hooks/src/curie_protected_hooks/execution_namespace.py::derive_lane_namespace`
derives the production lane, under a `protected:commissioning:lane:` prefix and
with its own stream. If the shared kernel transport cannot take a lane stream
other than the production one without editing `kernel.py`, `consumer.py`,
`threadlock.py` or `markers.py`, that is recorded as the blocker LANE phase 3
describes, and slice 3 waits for a separately reviewed plan.

## Permit grammar

<!-- @spec PROTECTED-COMMISSIONING-1 -->
`CommissioningPermit` is a new closed internal record. It follows the LANE-2
record encoding: UTF-8 input, recursive duplicate member refusal, canonical
ASCII JSON, no floats or nonfinite numbers, immutable values and the
16384 byte limit. It reuses the LANE-2 scalar spellings for UUID, generation,
millisecond, sha256, opaque_ref, `BrokerIdentity`, `GuardIdentity` and
`CredentialReference`. Counts are generation strings, never JSON integers.

| Member | Type and constraint |
| --- | --- |
| `schema_version` | Strict integer literal 1. |
| `record_type` | Literal `commissioning_permit`. |
| `campaign_id` | UUID. |
| `campaign_generation` | generation, above the runtime's durable campaign floor. |
| `runtime_id`, `runtime_generation` | Equal the final manifest. |
| `manifest_digest` | sha256 of the final manifest's canonical bytes. |
| `bootstrap_digest` | sha256 of the commissioning bootstrap (clause 2). |
| `execution_config_digest` | Equals the final manifest. |
| `broker_identity` | `BrokerIdentity`; equals the final manifest, including `run_id`. |
| `guard_identity` | `GuardIdentity`; equals the final manifest. |
| `issuer_identity` | `CredentialReference` of the commissioning issuer. |
| `control_identity` | `CredentialReference` of the commissioning control principal. |
| `candidate_credentials` | Object with exactly `enqueue` and `worker`, each a `CredentialReference`. |
| `sources` | Sorted array of 1 to 64 objects, each exactly `{source_id: UUID, generation: generation, policy_fingerprint: sha256}`. |
| `targets` | Sorted array of 1 to 64 distinct `opaque_ref` values naming owned fixtures. |
| `issued_at_ms`, `expires_at_ms` | millisecond; broker time; `issued_at_ms < expires_at_ms`. |
| `limits` | Object with exactly `deliveries`, `launches`, `model_calls`, `output_tokens` and `spend_microusd`, each a generation. |

Sources sort by canonical UUID and have distinct IDs. Targets sort
lexicographically. Unknown members, unknown record types, empty or repeated
scope, invalid scalars and unsorted scope arrays refuse. Canonical key order and
insignificant whitespace do not change identity. The permit's identity is the
sha256 of its canonical bytes.

The issuer, control and two candidate credential IDs are pairwise distinct, and
none equals any manifest `credential_refs` ID at any generation. Every source
and target is a member of the bootstrap's declared scope. The permit duration
does not exceed the bootstrap's `max_permit_ms`.

`parse_commissioning_permit(bytes)` performs closed structural validation only.
`validate_commissioning_binding` takes a typed final `Manifest`, the
commissioning bootstrap, an independently authenticated broker observation
(identity, `run_id` and `TIME`), the trusted issuer and control references and
the trusted maximum duration. It checks every binding, the time bounds and
credential separation. It authorizes nothing by itself.

The production `Manifest`, `Qualification`, `Readiness` and selection decoders
remain closed. A permit stored under a production key fails them because its
members differ; no production reader reads a `protected:commissioning:*` key.

## Bootstrap selection and issuer and reader boundaries

<!-- @spec PROTECTED-COMMISSIONING-2 -->
`CommissioningBootstrap` is the immutable provisioner-owned record ADR 0224
calls the bootstrap. It is distinct from the API runtime directory files of
SOURCE-9 and [#4076](https://github.com/curie-eng/curie/issues/4076) and from the
worker bootstrap read by
`apps/worker/src/curie_worker/protected_worker_bootstrap.py::load_worker_bootstrap`;
those keep their grammars. It uses the same encoding rules as the permit.

| Member | Type and constraint |
| --- | --- |
| `schema_version` | Strict integer literal 1. |
| `record_type` | Literal `commissioning_bootstrap`. |
| `runtime_id`, `runtime_generation` | The final tuple's runtime. |
| `execution_config_digest` | The final execution configuration digest. |
| `entry_points` | Object with exactly `production` and `commissioning`, distinct `opaque_ref` values naming the two predeclared worker and ingress compositions. |
| `production_credentials` | Object with exactly `enqueue`, `worker` and `verifier`; must equal the manifest's `credential_refs`. |
| `candidate_credentials` | Object with exactly `enqueue` and `worker`. |
| `issuer_identity`, `control_identity` | `CredentialReference` values. |
| `mediator_identity`, `gateway_identity` | `CredentialReference` values for the launch mediator and provider gateway. |
| `source_scope` | Sorted array of 1 to 64 source objects, as in the permit. |
| `target_scope` | Sorted array of 1 to 64 `opaque_ref` values. |
| `max_permit_ms` | generation. |

Hash order breaks the cycle between the records:

1. Construct bootstrap B. It contains no manifest digest, guard digest, permit
   bytes or reference to a file whose hash includes B.
2. Construct guard configuration G containing `sha256(B)`, the admission
   policy, RBAC, network policy and broker ACL rule recipes. G hashes each
   principal's rules, not its enabled state or password. The ACL principal
   state check of design decision 7 covers what G excludes.
3. Construct the final v1 manifest M with
   `guard_identity.config_sha256 = sha256(G)`.
4. Issue each permit bound to `sha256(B)` and `sha256(M)`. M and permits are
   authenticated outputs outside B's hashed set.

The production entry point is the existing composition: the API ingress loaded
by `apps/api/src/curie_api/protected_runtime_files.py::load_ingress` and the
worker opened by `apps/worker/src/curie_worker/protected_entry.py::open_runtime`.
The commissioning entry point is a separate standalone process composition that
reads a provisioner-owned directory, verifies B's digest against G and M, and
authenticates as a candidate principal. No HTTP field, environment variable,
workload label, CLI flag or request member selects between them. Possessing the
commissioning directory without a valid permit and a running journal is inert.

Control state is read only through authenticated broker sessions whose server
identity is pinned by M, as LANE-2 requires. A record ID never selects a URL,
file outside the opened directory, or trust root.

## Journal, reservations, expiry and retention

<!-- @spec PROTECTED-COMMISSIONING-3 -->
The journal for one campaign moves through `prepared`, `running`, `closing`,
then terminal `closed` or `failed`. Acceptance is not a journal state: it is the
separate issuer-written record of clause 7, valid only over a `closed` journal,
so the control principal can never mark its own campaign accepted. Every transition is a compare
and set on an expected generation. No transition returns to `running`, resets a
counter or reuses an attempt identity. A campaign generation is never reused:
the control principal advances a durable per runtime floor before `prepared`.

Before each delivery, launch or provider request, the control principal reserves
the worst case charge in one atomic broker operation that reads broker `TIME`,
verifies the exact permit and journal binding and the `running` state, and
increments immutable aggregate counters. A reservation has an operation ID bound
to the campaign, operation kind, request digest and exact charge. A replay with
the same ID returns the recorded status and never authorizes a second dispatch;
a changed kind, digest or charge refuses. Dispatch authority is consumed durably
before forwarding. An ambiguous forward consumes the reservation and needs
reconciliation; a genuine retry is a new charged attempt. There are no refunds.

Reaching any limit moves the journal to `closing`, records the exhausted
counter, and blocks every positive operation, including dispatch of an already
charged reservation. Changing scope or limits requires a new campaign. Retries,
parked deliveries, a new control process or a restored broker cannot renew one.

At `expires_at_ms`, at limit exhaustion, on a broker `run_id` change or on
revocation, the campaign refuses new positive work and the supervisor terminates
every recorded owned SDK and runner session. Status, stop and retire stay
available after expiry. A draining production delivery cannot extend a
commissioning deadline. Uncertain ownership or termination blocks acceptance and
never triggers a prefix sweep.

Retention: permits, journals, reservations, ownership records and the acceptance
record carry no TTL and are kept until the installation is retired. They are the
durable proof that blocks a second promotion and a resumed campaign after any
restart. A control process restart resumes `closing` from durable ownership,
not from an empty in-memory map. A broker restart changes `run_id`; the permit
binding then refuses and the campaign can only close or fail.

## Callers and refusal order

<!-- @spec PROTECTED-COMMISSIONING-4 -->
These callers can create, admit or launch protected work today. Each keeps its
production rules and must reject every commissioning record before a positive
claim, enqueue or start.

| Boundary | Production entry |
| --- | --- |
| Support probe | `apps/api/src/curie_api/protected_support.py::evaluate_protected_support` |
| Signed hook ingress | `apps/api/src/curie_api/protected_ingress.py::admit` |
| Atomic admission | `packages/protected-hooks/src/curie_protected_hooks/atomic_admission.py::AtomicAdmission` |
| Admission reconciliation | `apps/api/src/curie_api/protected_reconciler.py::ProtectedAdmissionReconciler` |
| API runtime files | `apps/api/src/curie_api/protected_runtime_files.py::load_bootstrap` |
| Worker composition | `apps/worker/src/curie_worker/protected_entry.py::open_runtime` |
| Artifact selection | `packages/protected-hooks/src/curie_protected_hooks/worker_artifact_selection.py::select_artifact` |
| Sandbox substrate | `apps/worker/src/curie_worker/protected_substrate.py::ProtectedSandboxSubstrate` |
| Mediated launch and retirement | `apps/worker/src/curie_worker/protected_launch_server.py::ProtectedLaunchServer` |

The launch server has no production executable composition today. The operator
command must supply one; a fixture constructor is not one. Automated remediation
qualification is a separate domain and supplies no permit.

Commissioning composition checks, in this order, before a positive enqueue,
claim, launch or provider request. The first failure decides the refusal and
nothing after it runs.

1. The process is the predeclared commissioning entry point and B's digest
   matches G and M.
2. The broker session authenticates with M's pinned server identity and the
   live `run_id` equals the permit's.
3. The commissioning issuer and control identities authenticate as the trusted
   references (`ACL WHOAMI`), not by possessing a reference.
4. The permit parses and `validate_commissioning_binding` accepts it.
5. The guard revision and primitive measurements re-observed through the ADR
   0236 channel match the premeasured tuple, and the ACL principal state check
   of design decision 7 passes.
6. The journal is `running` and broker `TIME` is below `expires_at_ms`.
7. The source, generation, policy fingerprint and target are inside the
   permit's scope.
8. The atomic aggregate reservation succeeds.
9. The existing protected envelope and transport constraints pass.

No path substitutes a permit for Qualification or Readiness, or seeds those
records so that production admission succeeds. Unknown record types and mixed
production and commissioning references refuse.

## Campaign cases and evidence

<!-- @spec PROTECTED-COMMISSIONING-5 -->
The operator command of ADR 0236 owns a unique installation, its labels and
IDs, cleanup registered before startup, the exact image and configuration tuple,
and one bounded campaign. Each case below needs a passing observation from this
installation's own producers or from signed channel replies bound to it.

Positive: a supplied synthetic alert arrives through signed ingress, a read-only
turn reads the owned fixture, and a grounded reply reaches the operator's own
reply receiver.

Negative: source and target escape; payload, signature and scope tampering;
mutation, unknown tool and approval requests refused before any effect or card;
ordinary and old worker access; production principals refusing without
production records; replay of a permit or admission from another installation;
cross kind record substitution at every caller in clause 4; duplicate and
ambiguous dispatch; counter limit boundaries; retries, parking and dead letters;
broker restart and control process restart; expiry during a live session;
interrupted closing; image, credential, network and ephemeral container
violations. Static Skill loading, clean session and read-only tool restrictions
are observed through the pinned SDK in the actual runner process; reading
instruction text does not qualify them.

Evidence names the authenticated control and broker observations, exact artifact
identity, the provider request profile, the delivered reply receipt, each
expected and observed refusal, resource ownership and independently confirmed
termination. It excludes secrets and credential values. An unavailable, skipped,
timed out or fake-model observation never yields acceptance. Code, fixtures and
component observations do not close the campaign.

## Activation is a predeclared authorization state transition

<!-- @spec PROTECTED-COMMISSIONING-6 -->
ADR 0224 requires activation to change only independently controlled
authorization state for the already measured production entry point. This
clause demonstrates that transition against the existing production readers.
If an implementation cannot hold every invariant below, this specification
returns to ADR review; it does not redefine "unchanged tuple".

Before the campaign the provisioner installs, and the campaign measures:

* M, with its `qualification_id` fixed, and the production credentials in
  `credential_refs`, enabled in the broker;
* the selection record `protected:control:selection:{runtime_id}` with M's
  digest, `qualification_id`, a predeclared `qualification_generation`, the
  live `broker_run_id` and `admission_open: false`;
* the production API ingress and protected worker workloads at their final pod
  templates, images, service accounts, mounts and Secret references;
* the content-addressed artifact measurement file in the production worker's
  `measurements` directory, whose `enforcement_evidence_ref` names only artifact
  enforcement observations made before the campaign;
* the launch mediator and provider gateway configurations, each naming both
  entry points.

With no Qualification or Readiness record present, the production worker's
`open_runtime` refuses before consuming and the support probe answers
`qualification_unavailable`. Those refusals are the ADR 0224 observation that
production credentials are assigned and isolated while production dispatch
authority is unavailable.

Activation then performs exactly these writes, in order, each by its own
principal:

1. Control moves the journal to `closed` after ingress is closed, accepted
   deliveries are settled, sessions are terminated and campaign workloads and
   targets are removed with confirmed absence.
2. The provisioner disables the candidate ACL users and kills their sessions.
   Their rules stay as G measured them.
3. The issuer writes the closed `commissioning_acceptance` record (clause 7)
   after the ADR 0236 verifier accepts.
4. The existing Qualification and Readiness publishers that ADR 0236 lets the
   verifier construct only after acceptance, holding the manifest's verifier
   credential, write Qualification at
   `protected:control:qualification:{qualification_id}:{qualification_generation}`
   and Readiness at `protected:control:readiness:{runtime_id}:{runtime_generation}`,
   both in their unchanged v1 grammars and bound to M.
5. The provisioner compares and sets the selection from `admission_open: false`
   to `true`, with every other member unchanged.

None of these keys or states is an input to `sha256(B)`, `sha256(G)`,
`sha256(M)` or the execution configuration digest, and no pod template, image,
mount, Secret reference, ACL rule or guard revision changes. The production
worker starts on its existing restart; the existing readers in
`packages/protected-hooks/src/curie_protected_hooks/worker_verification.py::_retained_authority`
and the admission path then accept because the records they already require now
exist. After step 5 the verifier re-reads B, G, M and the execution digest and
records that they equal the premeasured values. Real source activation remains
the separate SOURCE-6 administrative step.

The provider gateway named by the execution configuration's model base URL is
therefore a permanent installation component: it authorizes production launches
under production rules and commissioning launches only against a running
journal, selected by authenticated launch identity, never by configuration.

The specification returns to ADR review if any of these is found necessary:
recreating a production workload with a different pod template or mount;
writing a measurement or bootstrap file after the campaign; a guard revision or
ACL rule change; a different execution configuration for production and
commissioning; or production principals that cannot be enabled and observed
refusing before activation. Failure or uncertainty at any step leaves the
journal in `closing` or `failed` and blocks activation after any restart.

## Campaign record families

<!-- @spec PROTECTED-COMMISSIONING-7 -->
Each record has `schema_version` 1 and a literal `record_type`, and follows the
permit's encoding rules. Returned nested values are fresh copies. A campaign ID
never occupies a production `qualification_id` field.

Campaign binding C has exactly `campaign_id`, `campaign_generation`,
`permit_digest`, `bootstrap_digest`, `manifest_digest`, `runtime_id`,
`runtime_generation` and `broker_run_id`. Delivery binding D has exactly
`delivery_id`, `attempt_id`, `reservation_id`, `request_digest`,
`payload_digest`, `payload_bytes` (a generation no greater than 32768), `source`
(as in the permit) and `target_ref`.

| Type | `record_type` | Members after `schema_version` and `record_type` |
| --- | --- | --- |
| CampaignEnvelope | `commissioning_envelope` | `campaign: C`, `delivery: D`, `tool_access` literal `read-only` |
| CampaignAdmission | `commissioning_admission` | `campaign: C`, `delivery: D`, `envelope_digest`, `stream_entry_id`, `accepted_at_ms` |
| CampaignLaunchRequest | `commissioning_launch_request` | `campaign: C`, `delivery: D`, `admission_digest`, `launch_id`, `launch_reservation_id`, `projection_digest` |
| CampaignLaunchOwnership | `commissioning_launch_ownership` | `campaign: C`, `delivery: D`, `launch_id`, `launch_request_digest`, `projection_digest`, `workload: W`, `state`, `revision` |
| CommissioningAcceptance | `commissioning_acceptance` | `campaign: C`, `evidence_digest`, `qualification_id`, `qualification_generation`, `accepted_at_ms` |

`stream_entry_id` is two `0|[1-9][0-9]*` components, each at most
9007199254740991, joined by one hyphen. Workload binding W is tagged: `unbound`
has exactly `kind`, `namespace`, `pod_name` and `create_operation_id`; `bound`
adds `pod_uid`. Namespace and Pod name are DNS labels of at most 63 characters.
Ownership states are `creating`, `bound`, `retiring`, `retired` and `uncertain`.
A bound UID never changes or returns to unbound. One NotFound never makes an
ambiguous create `retired`. `retired` is terminal; unknown or `uncertain`
ownership blocks acceptance. `CommissioningAcceptance` is the only link from a
production Qualification back to campaign evidence, because the v1
Qualification grammar has no campaign member and this specification adds none.

Every decoder is distinct from the production envelope, intent, receipt,
selected artifact and launch registry codecs. Substituting one kind for another
refuses before a positive enqueue, claim, provider request or workload start.

## Kubernetes mediator, gateway and supervisor

<!-- @spec PROTECTED-COMMISSIONING-8 -->
Kubernetes is the only commissioning substrate. Create, observe, start, stop and
retire mechanics are shared by two closed authorizers: the existing production
one, which keeps its exact types and Qualification and Readiness checks, and a
commissioning one, which cannot construct those types. The mediator
authenticates the candidate worker principal and its entry point over mTLS
before reserving a launch. Candidate client certificates expire no later than
the permit. A model-facing API can neither issue launch authority nor promote
an installation.

Ownership is persisted as `creating` before workload creation, with request and
projection digests and a deterministic owned namespace and name. The observed
UID is bound after creation. An ambiguous create is resolved by the exact
recorded name, the UID when known and the guard-owned identity; a same-name
object is never adopted. The guard permits only the measured image, service
account, mounts, Secret reference generations, network and ephemeral container
policy.

The provider gateway binds each request to campaign, launch and request digest,
checks the measured model, backend, thinking setting, token ceiling and worst
case price, and reserves and consumes one dispatch before forwarding. It
reserves the hard input ceiling plus the output allowance at worst case prices,
including cache writes. Remote attachments, provider-managed tools and
server-side compaction refuse before dispatch. Launch identity comes from the
direct socket peer and the observed Pod UID captured when the TLS connection is
established, never from HTTP fields or forwarded headers. If cross Pod,
ordinary worker, host path, retained connection and IP reuse negatives cannot
be observed on the actual substrate, this composition is unavailable.

The supervisor in the control process owns cancellation after expiry,
exhaustion, revocation or broker loss. It joins every owned SDK and runner
session, retires each recorded workload, and confirms that the original runner
container terminated before the protected finalizer is removed and that the
workload is then absent. Finalizer removal is permitted only for the exact
mediator or provisioner identity, on an update that changes nothing but that
finalizer and managed field bookkeeping. Before a permit exists, the
provisioner's primitive probes follow the same ownership rules in a separate
registry that grants no campaign or production authority.

## Evidence reuse

<!-- @spec PROTECTED-COMMISSIONING-9 -->
Artifact observations may be reused only when recorded independently, content
addressed, and bound to an unchanged exact tuple: image digests and their
observed image IDs per platform, the launch projection digest, and pinned SDK
primitive observations for that runner image and runtime profile. The verifier
re-validates the binding on reuse.

Installation observations are never reused: guard, RBAC, CNI and network
enforcement, broker ACL and identity, credential separation, revocation,
cleanup and the ingress to reply path. Installation A cannot qualify
installation B. A broker `run_id` change, a new manifest, execution
configuration, guard or bootstrap invalidates the bound campaign evidence and
requires a new campaign against the new tuple.

## Verification boundary

Closed codec and pure binding tests prove grammar and refusal only. Real
isolated Valkey tests prove the authenticated journal, reservation and restart
behavior. Actual Kubernetes controls and pinned SDK tests prove preventive
controls. Only the complete campaign of clause 5 on a fresh installation proves
installation delivery. No slice claims production activation or release
eligibility.

## Design decisions

These decisions fix what the Accepted ADRs leave to a realizing specification.
Each states what the design does, why, and the Accepted clauses it rests on. A
slice that would need a different answer stops and returns to review; it does
not reinterpret the decision.

### 1. Only the provisioner creates the protected lane

The protected lane is created only by the independent provisioner process. The
main Helm chart and the Compose files render no protected broker, protected
worker, guard, commissioning ingress or protected runtime directory, and expose
no value that turns them on. An installation obtains a protected lane only from
the provisioner's pinned profile, projection and images.

Reasoning. ADR 0191 makes protected runtime provisioning out of band from the
ordinary API key: an independently controlled provisioning authority owns
credential issuance, image qualification and deployment policy. ADR 0236 gives
the provisioner process the creation of the installation and the registration
of ownership of every resource before it is created, and keeps its signing key
out of the control Pod. A chart rendering would make the ordinary release and
values path a second creator of the same objects, so the measured pod templates,
mounts and Secret references of clause 6 would have two owners and the guard
configuration G could not attribute them. A single creator also keeps the
predeclared entry points of ADR 0224 (both fixed in the immutable bootstrap
before the campaign) out of reach of a values override.

Phase 4 of the [delivery lane](2026-10-02-protected-hook-lane.md) plans
protected worker, broker and guard surfaces under `charts/curie/templates/`.
Where that wording conflicts with this decision, this specification supersedes
it: no protected lane object is added to the main chart or to Compose. The
phase 4 rule that an out of band owner provisions guards and credentials, and
that API routes never mint an administrative credential, stands and is realized
by the provisioner.

### 2. The first remediation profile is connector-empty

The first automated remediation profile runs on a commissioned installation
whose execution profile has `qualified_connectors=[]`. The protected turn reads
the delivered alert and nominates a pre-qualified action; it holds no connector
credential and calls no connector. Precondition and verifier reads run as read
executions in executor sandboxes outside the protected lane, under the read
connector's own binding (AUTOMATED-REMEDIATION-12), and actions run through the
connector action executor.

Reasoning. ADR 0224 fixes the initial profile as strictly
`qualified_connectors=[]` and does not authorize a connector-bearing profile;
the current execution configuration codec
(`apps/worker/src/curie_worker/protected_configuration.py::parse_execution_config`)
already requires an empty list. ADR 0203 makes remediation an action the
platform executes and verifies, and AUTOMATED-REMEDIATION-12 already places
every precondition and verifier read in an executor sandbox rather than in the
worker. Nothing in the nominate path therefore needs a connector inside the
lane. A connector-bearing protected profile stays out of scope and requires its
own ADR before any specification work.

### 3. Production qualification on a managed CNI is a separate dependency

This specification qualifies only disposable local installations with the
local Cilium producer. It does not specify, and no slice claims, qualification
of a production installation whose CNI is a managed cloud CNI.

Reasoning. ADR 0224 requires every installation intended for production to run
its own campaign, and ADR 0236 lets the operator command qualify only disposable
local installations until a producer exists for the target CNI, naming source
identity, spoof measurement and retained connection refusal as part of that
producer's realization.
[ADR 0238](../../adr/0238-a-managed-cni-installation-qualifies-through-its-own-behavioral-network-producer.md)
(Accepted) decides that producer and the production installation form. Its
realization is a separate specification and is out of this specification's
scope; until it ships, ADR 0236's explicit outcome holds and a managed CNI
installation refuses before the campaign starts.

### 4. The activation transition is demonstrated before implementation

ADR 0224 requires activation to change only independently controlled
authorization state for the measured production entry point, and requires the
realizing specification to demonstrate that transition or return for a revised
decision. Clause 6 is that demonstration, and it is a precondition for slice 1:
no slice starts while any row below cannot hold. If review finds a row that
cannot hold, this specification returns to ADR review rather than redefining
"unchanged tuple".

| State | Measured by the campaign | Changed by activation | Writer at activation |
| --- | --- | --- | --- |
| Bootstrap B, entry points, credential assignments | Yes | No | None |
| Guard configuration G: ACL rules, RBAC, network and admission policy | Yes | No | None |
| Manifest M, its `credential_refs` and `qualification_id` | Yes | No | None |
| Execution configuration and its digest | Yes | No | None |
| Production ingress and worker pod templates, images, mounts, Secret references | Yes | No | None |
| Artifact measurement file | Yes | No | None |
| Mediator and gateway configuration naming both entry points | Yes | No | None |
| Selection record, all members except `admission_open` | Yes | No | None |
| Selection `admission_open` | Yes, as `false` | `false` to `true` | Provisioner |
| Commissioning journal | Yes | `closing` to `closed` | Commissioning control |
| Candidate ACL users' enabled state and live sessions | Rules only | Disabled, sessions killed | Provisioner |
| `commissioning_acceptance` | No | Created | Commissioning issuer |
| Qualification and Readiness | Absent | Created | Verifier's publishers |

Every changed row is broker state written by a principal whose identity and
rules G already measured, and, as clause 6 requires, none is an input to
`sha256(B)`, `sha256(G)`, `sha256(M)` or the execution configuration digest.
No pod template, image, mount, Secret reference, ACL rule or guard revision
changes. The only measured value that changes is the selection's
`admission_open`, the predeclared authorization switch; its other members stay
equal under compare and set. The verifier re-reads B, G, M and the execution
digest after the last write and records that they equal the premeasured values. Before
activation the production worker refuses in `open_runtime` and the support
probe answers `qualification_unavailable`; after it, the same unchanged readers
accept because the records they already require now exist.

### 5. Evidence bar

An upstream automated remediation issue closes on evidence from a freshly
commissioned disposable installation: the complete clause 5 campaign, accepted
by the ADR 0236 verifier, on an installation created for that run. Code, fake
model plumbing, direct provider replies or a reused installation do not close
it. Closing an upstream issue is not production activation: each production
installation still runs its own campaign against its own final tuple and is
activated only through clause 6.

Reasoning. ADR 0224 states that success in one installation does not qualify
another and that installation observations are never rebound to another
manifest. ADR 0236 limits the command to disposable local installations, so a
fresh disposable installation is the strongest evidence the upstream project can
produce, and it is installation independent by construction.

### 6. ADR status lines are authoritative

Where an Accepted ADR's body still reads as a proposal or a Draft (for example
ADR 0224's "Proposed decision" heading), its `Status:` line decides its
authority. ADR 0045 makes the status line the mutable part of an immutable ADR,
and ADR 0085 makes the Accepted status the authority for implementation. This
specification edits no ADR file and cites each ADR at its current status.

### 7. ACL principal state is observed, not hashed

Decided by the maintainer on 2026-10-20.

The pinned Valkey cannot scope `ACL SETUSER` to named users or to disabling. The
provisioner keeps `ACL SETUSER` and `CLIENT KILL`, which clause 6 step 2 needs,
so it could also change a principal's password or re-enable a disabled user.
G does not hash enabled state or passwords: G is fixed at installation and
bound into M, while enabled state must change at activation (clause 6 step 2)
and a credential rotation must not change M. Misuse of this right is therefore
detected, not prevented.

The administrator-run installation bootstrap, before any campaign, records the
baseline: for every broker identity in the principals table, the set of
password hashes and the enabled state. The provisioner never writes this
reference. Every
reobservation, meaning each guard call (refusal order step 5) and each
Readiness issuance, then compares, for each commissioning and production
principal:

* the password hash set with the value recorded at installation; and
* the enabled state with the value recorded at installation and with the state
  expected for the current campaign phase (for example, candidate principals
  disabled after activation step 2).

Any difference, or an unavailable observation, refuses.

A legitimate credential rotation changes a password hash set. Only the
administrator-run installation step that recorded the baseline updates it,
never the provisioner at campaign time. Until the record is updated, every
guard call and Readiness issuance refuses.

A dedicated ACL observer principal reads this state. Its rules grant only
`ACL GETUSER`: no `ACL SETUSER`, no other ACL subcommand, no key and no
channel. It is distinct from the provisioner, so the party being checked never
reads or reports its own evidence. Its rules are part of G like every other
principal's.

Residual risk: a change made between two observations is detected at the next
observation, not prevented. A guard call or Readiness issuance after the change
refuses.

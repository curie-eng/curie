# Commissioning trial series and final acceptance

Realizing specification for
[ADR 0241](../../adr/0241-a-commissioning-run-separates-destructive-trials-from-final-acceptance.md),
tracked in [#4974](https://github.com/curie-eng/curie/issues/4974).
It amends the [commissioning specification](2026-10-19-protected-runtime-commissioning.md)
only as explicitly stated below. No runtime implementation or qualification is
claimed by this document. ADR PR review precedes implementation.

## Scope and compatibility

<!-- @spec COMMISSIONING-SERIES-1 -->
The first realization remains one fresh disposable local Kubernetes installation,
the actual Cilium producer, `qualified_connectors=[]`, owned synthetic sources
and read-only targets, and the actual protected SDK/ingress/broker/worker/
mediator/runner/reply path. Fake-model or direct-provider results do not qualify.
Managed-CNI case definitions, production deployment and paid runs are unchanged
and not authorized here.

Except for the explicit internal bootstrap v2 and evidence-observer addition
below, PC-1 through PC-4, PC-7 and PC-8 retain their record grammars,
principal separation, per-campaign reservations, broker time, refusal order,
supervision and ownership obligations. PC-5's evidence unit becomes the run
below. PC-6 applies to the **final campaign** and stays unchanged from its
premeasurement through activation. PC-9 gains only the explicitly listed
historical-trial composition below; other installation evidence is not reusable.
There is no caller-supplied evidence/phase flag or production-reader bypass.

## Run authority, durable custody and aggregate bounds

<!-- @spec COMMISSIONING-SERIES-2 -->
The provisioner owns a private local run directory outside the cluster and
broker. The parent and directory are owner-only; files are owner-only regular
files, opened without symlink traversal. The initial local implementation uses
one exclusive OS file lock held from precommitment through termination. A second
coordinator refuses before provisioning or issuing authority. The directory is
not mounted into control, candidate, runner or ordinary product processes.

Before installation creation, the provisioner writes a create-once run intent
fixing its ID, installation ID, exact provisioning recipe, trial inventory and
ceilings, and registers ownership cleanup. After actual provisioning it seals
the plan with independently observed UIDs/image IDs, checks those against the
intent and recipe, and persists its prepared journal head. No permit exists
until that plan is sealed. A provisioning failure leaves an intent that allows
cleanup only. The sealed plan cannot amend the intent. Updates use an
expected revision, an immutable event and predecessor digest, file and directory
fsync before reporting success. Incomplete writes, duplicate revisions, a broken
chain, missing files, conflicting content or uncertain durability refuse. No
event or evidence file has a TTL. The directory, installation ownership ledger,
campaign floors, journals and observations remain until installation retirement.

This is a trusted local custody boundary, not protection against a malicious
host administrator restoring the entire host. Such a restore is outside the
supported continuity model and cannot resume or qualify an installation. The
operator must retire it. A copied directory, different host/process owner or
reconstructed head never authorizes continuation. The initial realization has
no import, checkpoint restore, automatic retry or positive-work resume verb.

The local custody directory is the fixed `commissioning-run` child of the
owned installation ledger root; callers cannot choose an alternate run child
for that installation. Creation requires an owner-only ledger with no created,
uncertain or removed resource history. Predeclared `intended` ownership rows
are allowed. Existing custody never becomes a fresh live handle. Copying only
part of an installation's state is unsupported restoration, not a reset path.

Local storage has exactly `lock`, `records`, `intent`, `head`, `location` and,
after sealing, `plan`. The owner-only immutable files under `records` are named
by their lowercase sha256, with no suffix; each is one canonical typed record.
The pointer files contain exactly 64 lowercase ASCII hex bytes. `intent` and
`plan` name their typed records; `head` names the last event, or 64 zeroes before
any event. `location` is sha256 of canonical ASCII JSON with exactly
`ledger_owner`, `parent_device`, `parent_inode`, `run_device`, `run_inode`,
`records_device`, `records_inode`, `lock_device`, `lock_inode` and
`root_path`, all strings derived from the opened ledger/run directories and
normalized absolute run path; numeric filesystem identities use canonical
unsigned decimal. This is host custody metadata, not portable evidence or a new
record accepted by any broker reader. Moved/copied/replaced custody refuses.
Regular files must be owner-only, single-link files, and directories owner-only.
Unreferenced records, extra files, unfinished writes and broken pointers refuse.
The live handle additionally pins its creating process and opened directory/lock
identities; an inherited handle after fork has no positive authority.

Run states are `prepared`, `running`, `closing`, then `closed` or `failed`.
There is one irreversible transition to running. A fresh invocation finding an
existing run may only observe/finish cleanup and mark failure, never issue a
permit, resume trials, accept or activate. A control-process restart *within*
an uninterrupted provisioner run is a scheduled test; a provisioner crash is
not. No second run is admitted to that installation, even after failure.

The plan fixes all five aggregate limits: deliveries, launches, model calls,
output tokens and spend in microusd. Before each permit is issued, one durable
run event burns the permit's **entire** five-counter envelope. The sum of burned
ceilings cannot exceed any run limit; ceilings cannot be transferred or refunded.
An ambiguous issuance burns its ordinal and fails the run. The existing atomic
broker reservation/dispatch rules enforce each campaign's individual envelope.
This conservatively bounds the series without trusting a restarted broker to
reconstruct aggregate provider charges. No successful trial replenishes budget.

The plan also fixes a total elapsed-time ceiling in milliseconds. The same
uninterrupted provisioner process measures it with a monotonic clock, including
startup, waits, broker downtime, transitions and final verification. Before
permit issuance it freshly reads broker TIME and limits permit duration to the
remaining run allowance; existing expiry supervision stays active. The run
watchdog independently closes admission and joins/retires owned work at the
aggregate deadline. A suspended process or unobservable/uncertain clock causes
failure on recovery; it grants no fresh allowance. Cleanup continues after the
deadline with no positive work. Missing cleanup never becomes success because
a timeout elapsed. No hard wall-clock termination bound is claimed if the host
or cluster is unavailable.

## Independent evidence observation

<!-- @spec COMMISSIONING-SERIES-3 -->
The existing provisioner/issuer can read journals but cannot read reservation
and ownership families. Control reports cannot fill that authority gap. This
series therefore adds one dedicated `evidence_observer` broker principal, issued
by the administrator before B/G construction, held only by the independent
provisioner verifier and never by the control Pod or a candidate. It is distinct
from every existing credential issuance, including the ACL observer.

The new internal CommissioningBootstrap v2 has exactly v1's members plus
`evidence_observer_identity`, a CredentialReference included in pairwise
distinctness checks. Its `schema_version` is 2. Its record type, canonical
encoding and remaining invariants are unchanged. v1 stays closed; it is not
accepted by a series composition. Production Manifest, Qualification, Readiness
and runtime-directory bootstrap formats stay unchanged. Every series binder,
guard, issuer, worker, mediator and verifier must support this exact v2 before
any series permit is admitted; there is no schema coercion or fallback.

The series G uses `schema_version: 2`, the existing
`kind: commissioning_guard_config`, and exactly the existing G members. Its
closed `broker_acl` map has exactly the existing principal names plus
`evidence_observer`, whose value is exactly `{credential, rules_sha256}`; the
credential equals B v2's evidence_observer_identity. All other entries retain
their existing meanings. The provisioner profile and recipe inventory explicitly
include that issuance and exact rules before G is built. Existing G v1 stays
closed and cannot qualify a series.

G includes the new principal's fixed rules. They start with the existing reset/
session-handshake inventory and grant `ACL WHOAMI`, `INFO server`, `TIME` `CLIENT LIST` for independent connection-presence observation on this dedicated
broker, and only `GET`/`TYPE` on `protected:commissioning:permit:*`,
`protected:commissioning:journal:*`, `protected:commissioning:reservation:*`,
`protected:commissioning:ownership:*` and `protected:commissioning:admission:*`.
It has no production keys, channels, key scanning, scripts, writes, other CLIENT verbs or other ACL
subcommands. The server is the installation's dedicated broker, never a shared
foreign broker. Its read-only families are fixed before G and need no per-trial
ACL update. The verifier derives exact keys from the plan/current C and retained
operation IDs; it never enumerates keys or trusts a report-supplied key/path.

The administrator baseline and every ACL reobservation cover this principal's
passwords, enabled state and exact rules. The verifier authenticates its own
observer session, pins broker TLS identity and live epoch, parses retained
records through their exact product decoders and verifies C, operation, charge
and ownership bindings. It combines these independent reads with actual owned
workload/session/receiver observations; stored control-written state alone is
not proof of causality, dispatch or cleanup. Missing or inconsistent observations
refuse. Existing principals and the twelve-fact guard channel gain no rights.

The bounded read component is
`apps/worker/src/curie_worker/commissioning_evidence_observer.py`. It uses exact
B2/G2 inputs and the dedicated role, derives only known keys, reads their bytes
twice around fresh client/time/epoch observations, and joins the original C's
journal, reservation, ownership and admission records. It preserves the original
epoch separately from the actual live epoch during historical reads. Pure stored
record decoders and joins live in
`packages/protected-hooks/src/curie_protected_hooks/commissioning_retained.py`
and
`packages/protected-hooks/src/curie_protected_hooks/commissioning_evidence_records.py`.
These components emit observations only. Full measured series composition,
complete trial-session inventory, final observer teardown and successful
receipts/acceptance remain separate obligations; these reads do not establish
them. Existing v1 issuer/control compositions do not issue B2 campaigns.

## Versioned operator transport custody

<!-- @spec COMMISSIONING-SERIES-8 -->
[Accepted ADR 0242](../../adr/0242-commissioning-session-scope-makes-the-last-observer-finite.md)
partially supersedes the all-installation-session cleanup scope below. All SDK,
runner and positive-work-capable transports remain trial-owned under original C.
Only the predeclared measured operator roles in that decision may be run-owned;
scope never changes after intent. Existing v1 records are historical, closed and
unchanged. Their readers must reject a v2 custody directory rather than ignore
its operator history. Neither a parsed journal nor a role label proves actual
rights, complete membership, joins, cleanup or qualification.

The explicit v2 custody extension retains the same fixed child, fd/inode/process
pinning, exclusive lock, suspend-inclusive elapsed deadline and cleanup-only
recovery. It adds exactly `format` (the single ASCII byte `2`), `operators`
(a private directory of content-addressed records), `operator-recipe` and
`operator-head` (canonical 64-byte digest pointers). Existing `records`, `intent`,
optional `plan`, `head`, `location` and `lock` retain their existing meanings.
The location pin additionally binds the operator-directory device/inode and
format. There is no second lock or portable/resumable positive handle. Both
record directories share the existing 4096-record / 67108864-byte bounds;
each record remains at most 16384 bytes. All records must be referenced by their
typed chains, including failed or unbound attempts; unknown extra files refuse.

New records use strict integer `schema_version: 2`, canonical ASCII JSON and
the existing closed scalar grammars. Exact additional members are:

| Record type | Additional members |
| --- | --- |
| `commissioning_operator_recipe` | `provisioning_recipe_digest`, `roles` |
| `commissioning_operator_event` | `run_id`, `revision`, `previous_digest`, `elapsed_ms`, `kind`, `intent_id`, `role`, `phase`, `ordinal`, `binding_digest`, `identity` |

The original run intent's `recipe_digest` binds the complete operator recipe,
which in turn binds the immutable provisioning recipe. `roles` contains exactly
one row for each of `administrator`, `evidence-observer`, `acl-observer`,
`acceptance-publisher`, `qualification-publisher`, `activation-publisher`, sorted
lexically by role. Each row has exactly `role`, `credential` (CredentialReference)
and `max_connections` (generation, at most 256); credentials are distinct and
the sum of ceilings is at most 1000. A ceiling counts every intended attempt,
including failed, unbound and closed attempts, without refund or automatic retry.
This recipe supplies no grant to open a connection. Fresh B2/G2, administrator
baseline, measured ownership and phase predicates still authorize each actual
adapter. An unrealized role or phase fails closed.

Operator events have consecutive revisions from `1`, a zero first predecessor,
nondecreasing elapsed milliseconds and one immutable run/intent/role/phase/ordinal
binding per attempt. `kind` is `intended`, `bound`, `closed` or `uncertain`.
`intended` is durable before opening; its identity and binding digest are null.
`bound` carries the actual identity and a null binding digest. `closed` carries
the preceding bound-event digest and a null identity; it is written only by the
actual adapter after transport closure and tracked I/O joins. `uncertain` has a
null identity and either the original binding digest or null if unbound; it
retains failure, never establishes closure. Each attempt permits only intended,
optionally bound, then one terminal event. A missing terminal record is uncertain.
An unbound attempt cannot be repaired or reused by another connection.

For broker operator transports, identity has exactly `client_id` (generation),
`broker_identity` (BrokerIdentity), `credential` (CredentialReference) and
`host_process`. The Linux host-process binding has exactly `pid`, `start_ticks`,
`pid_namespace_inode` (generations), and `boot_id` (UUID), freshly observed from
the trusted supervisor's own procfs. PID alone cannot identify a process.
Unsupported host identity observation refuses; no invented container ID or PID
fallback is permitted. TLS/authentication plus the minimum HELLO/INFO identity
exchange may precede binding; retained reads may not.

The preliminary paired-verifier factory checks its sealed plan's B2/G2 and
execution-configuration bindings plus both exact recipe credentials before
opening. Any existing live, unbound or uncertain operator attempt refuses a new
factory. It binds the evidence connection first, then durably intends and binds
the independent ACL connection before its first GETUSER. The ACL role needs only
its existing HELLO handshake and GETUSER permissions: its actual client ID/user
is corroborated through the independently authenticated evidence connection and
fresh broker epoch. No WHOAMI/INFO privilege is added to that ACL role.

Before each retained read, fresh GETUSER replies for both verifier users must
match their exact product permission recipes, selector/key/channel permissions,
single expected password hash and enabled state. The original credentials and
actual process/connection identities stay distinct. Missing, widened or changed
rights refuse and end both connections. Each observation runs as an owned I/O
task; the outer trusted owner joins those tasks and the original TLS writers
before storing terminal connection events. A failed observation revokes positive
custody; another factory cannot repair it. These observations do not establish
the complete installation inventory or administrator baseline, nor produce
trial cleanup, a checkpoint, acceptance or a final operator verdict.

Close starts irreversible transport shutdown before scheduling its owned teardown
task. Caller cancellation, including repeated cancellation, cannot cancel or
detach that bounded task: the caller waits for its actual outcome before
cancellation is re-raised. Failed teardown remains a refusal on later close calls.
Registered observations and joined-close witnesses additionally pin their process
PID and CPython parent-fork generation. An observed `os.fork` after transport
creation irreversibly refuses useful observations and successful closure, even
after the child exits. Parent-owned descriptors are still closed and joined;
unresolved history and revoked positive custody report uncertainty, never child
cleanup. This opt-in fence does not change v1 parent-custody behavior or default
unregistered observations. It is not complete native child-process accounting;
the measured composition and complete process inventory remain required before
any campaign cleanup or final operator verdict can be accepted.

`phase` is `setup`, `trial`, `transition`, `decision`, `publication` or `teardown`.
`ordinal` is `0` for setup/teardown and a trial ordinal `1` through `10` otherwise;
decision/publication require `10`. Administrators are restricted to quiescent
setup/transition/teardown; publishers to post-decision publication. Read-only
verification roles may operate only after their actual rights and separation
are freshly corroborated. The first implementation may persist setup/trial
operator history but must refuse unimplemented transitions, decisions and
publication. No event or caller-supplied phase advances the trial series.

The future pre-decision checkpoint must bind complete trial results/rosters and
the complete operator registry including live verifiers. A distinct typed,
append-only post-decision tail must bind that checkpoint and prior head for
fresh PC-6 observations/publication/activation and terminal operator closure.
No v1 dossier, digest-only extension or this preliminary journal may substitute
for either record or claim full acceptance. The final verifier's actual local
transport close plus task/process join is witnessed by the measured supervisor;
trial broker sessions still require independent fresh epoch/client absence.
Before activation, uncertain teardown withholds activation; after an activation
write, report its actual state plus failure/uncertainty. A successful operator
result additionally requires the durable final tail and lock release.

## Closed plan and evidence records

<!-- @spec COMMISSIONING-SERIES-4 -->
These are new internal **provisioner custody records**, not new broker grants
or production records. Each uses PC-1 canonical ASCII JSON, recursive duplicate
member refusal, immutable values, no floats and the existing UUID, generation,
millisecond and sha256 scalar spellings. Unknown members refuse. Counts and
maximum durations are generation strings. `elapsed_ms` uses the existing
millisecond scalar, which permits zero. An event ordinal is either the literal
string `0` for a run-level event or a positive trial generation; it does not
reinterpret the existing positive-only generation scalar. Each record is at
most 16384 bytes; large
evidence is split by typed observation into separate bounded records, never
URLs or arbitrary paths. There is no raw chunk, concatenation or arbitrary
payload format. Each record is at most 16384 bytes, the run contains at most
4096 records and at most 67108864 retained bytes; crossing either bound refuses.
A canonical record's sha256 names exactly its own bytes, not a reconstructed
stream. Missing referenced content refuses.

Each record has `schema_version: 1` and its literal `record_type`:

| Record type | Exact additional members |
| --- | --- |
| `commissioning_run_intent` | `run_id`, `installation_id`, `recipe_digest`, `trials`, `aggregate_limits`, `max_elapsed_ms` |
| `commissioning_run_plan` | `run_id`, `installation_id`, `intent_digest`, `static_binding`, `trials`, `aggregate_limits`, `max_elapsed_ms` |
| `commissioning_run_event` | `run_id`, `revision`, `previous_digest`, `kind`, `ordinal`, `subject_digest`, `elapsed_ms` |
| `commissioning_trial_start` | `run_id`, `ordinal`, `plan_digest`, `campaign`, `tuple_digest`, `challenge`, `ownership_digest`, `charged_limits` |
| `commissioning_trial_result` | `run_id`, `ordinal`, `start_digest`, `terminal_state`, `observations`, `cleanup_digest`, `transition_digest` |
| `commissioning_case_receipt` | `run_id`, `ordinal`, `start_digest`, `case`, `challenge`, `tuple_digest`, `outcome`, `model`, `producer`, `evidence_digests` |
| `commissioning_transition` | `run_id`, `ordinal`, `trial_tuple_digest`, `ownership_digest`, `broker_before`, `broker_after`, `derived_inputs_digest`, `acl_state_digest` |
| `commissioning_run_dossier` | `run_id`, `plan_digest`, `event_head_digest`, `trial_result_digests`, `final_campaign`, `final_tuple_digest`, `final_observation_digests` |

`campaign` and `final_campaign` are the exact PC-7 C binding, not an ID alone.
`broker_before` and `broker_after` use the existing BrokerIdentity grammar.
`static_binding` has exactly the sha256 members `bootstrap`, `guard`,
`execution_config`, `control_profile`, `projection`, `image_set`, `sdk_profile`,
`network_identity` and `ownership_anchors`. The image-set blob binds immutable
image digests and observed platform image IDs; the anchor blob names the
original installation/namespace/storage/control resource UIDs and ownership
records, not labels alone. Their existing product decoders validate retained
content; a matching hash does not replace live checks.

`recipe_digest` binds the exact immutable provisioning recipe, its expected
static identities and the ownership creation requests; observed UIDs are
validated after creation, not invented before it. `intent_digest` names the
original intent. Common intent/plan members must be byte-identical.

`trials` is the exact ordered inventory in clause 5, each row exactly
`{ordinal, case, limits, max_duration_ms}`. Ordinals are consecutive generation
strings beginning at `1`. `limits`, `charged_limits` and `aggregate_limits`
have exactly PC-1's five limit members. A challenge is a fresh canonical UUID
generated by the provisioner before the observation, never by the report author.
`observations`, `evidence_digests`, `trial_result_digests` and
`final_observation_digests` are ordered arrays of distinct sha256 values,
containing 1 to 256 entries; case order follows clause 5. Empty/duplicate/
omitted/excess observations refuse. The dossier contains exactly ten ordered
trial results, including the final campaign result; its final observation
array references that final result's receipts, not a second set of observations.

Event revisions begin at `1`; the first predecessor is 64 zeroes. Event `kind`
is one of `prepared`, `running`, `charged`, `started`, `settled`, `closing`,
`closed`, `failed`. Run-level events use ordinal `0`; prepared, running and closing subject the
sealed plan digest, closed subjects the completed dossier, and failed subjects
the plan (or intent if provisioning failed before sealing). Campaign charged
events subject the canonical scheduled trial-plan row, started events subject
the start record and settled events subject the result record. Every event
kind has only that subject type. `elapsed_ms`
never decreases. The only successful event sequence is prepared, running,
then charged/started/settled for each of the ten ordinals, closing, closed.
Failure permits closing/failed and cleanup records only. No new charged event
follows closing. `closed` records the verified dossier digest; the dossier's
head is the preceding closing event, which subjects the plan and never the
dossier. Thus no event digest depends on the dossier that contains it.

`terminal_state` is `closed` or `failed`, matching the authenticated campaign
journal. Receipt `outcome` is exactly `pass`; any other outcome is an abort
report and cannot enter a successful dossier. `model` is `real` when model
activity was needed for that case, otherwise `none`; fabricated calls cannot
fill either role. `producer` is a digest of the measured producer binding.
Receipt `case` is an inventory case or final-case name from clause 5.

Custody authenticates these records: the independent provisioner creates them
only from challenges it issued and effects it freshly corroborated through its
own measured producers. It never imports a signed/hashed result supplied by a
caller. On every read it validates the chain, plan, expected record type, full
C, ordinal, challenge, original resource identities and all referenced bytes.
The control process may report progress but cannot write this directory or
declare a receipt passing. For a control-private claim the provisioner must
independently observe its causal effect and use its dedicated evidence-observer
session for authenticated journal/reservation/ownership state. If the existing producers cannot corroborate a case, that case
is unavailable; a new attestation protocol requires separate specification and
review before use. The existing twelve-fact guard channel is unchanged and
cannot sign these records. A digest provides integrity within custody, not
independent authenticity by itself.

### Typed digest targets

Every aggregate digest above has one closed wrapper, with schema_version 1,
its literal record_type below and exactly the listed additional members. A
reference is accepted only at the expected type, never by hash alone.

| Record type | Exact additional members and reference use |
| --- | --- |
| `commissioning_image_set` | `images`: sorted unique rows `{role, image_digest, platform, image_id}`; target of static image_set. |
| `commissioning_ownership_snapshot` | `owner`, `entries`: the existing OwnershipLedger entry grammar, sorted by kind/key; target of ownership_anchors and ownership_digest. |
| `commissioning_cleanup` | `run_id`, `ordinal`, `ownership_digest`, `session_roster_digest`, `resource_checks`, `session_checks`; target of cleanup_digest. |
| `commissioning_session_roster` | `run_id`, `ordinal`, `sessions`: sorted rows `{intent_id, kind, owner_digest, launch_id, binding_digest}`. |
| `commissioning_session_binding` | `run_id`, `ordinal`, `intent_id`, `kind`, `owner_digest`, `launch_id`, `identity`. |
| `commissioning_derived_inputs` | `manifest_digest`, `measurement_digest`, `selection_digest`, `runtime_inputs`: sorted rows `{role, content_digest}`; target of derived_inputs_digest. |
| `commissioning_acl_snapshot` | `bootstrap_digest`, `guard_digest`, `campaign`, `principals`: sorted rows `{role, credential, rules_digest, password_set_digest, enabled}`; target of acl_state_digest. |
| `commissioning_producer_binding` | `case`, `image_set_digest`, `control_profile_digest`, `projection_digest`, `execution_config_digest`, `subcases`; target of producer. |
| `commissioning_case_evidence` | `run_id`, `ordinal`, `case`, `challenge`, `tuple_digest`, `checks`: sorted rows `{subcase, subject_digest, expected_digest, observed_digest, outcome}`; target of receipt evidence_digests. |

Arrays contain 1 to 256 rows, except an ownership snapshot may have zero
entries only before provisioning; a passing cleanup must name the complete
nonempty trial roster. All sorted keys are ASCII lexical order with no
duplicates. `role`, `case`, `subcase` and runtime-input roles use the existing
CaseObservation entry spelling `[a-z0-9][a-z0-9._/-]{0,127}`. Values of all
`*_digest` members are sha256. `credential` is CredentialReference; `enabled`,
`absent`, `join_observed` and `transport_closed` are strict JSON booleans.
Passing cleanup requires each applicable predicate below, and case check outcome
must be literal `pass`. A
subject is a sha256 of its original ownership entry including its identity; it
never names a replacement resource or arbitrary API target.

Image rows sort by role then platform, with no repeated role/platform pair;
one role may retain its distinct observed platforms. Image roles are exactly
the measured profile's complete image inventory, with
OCI sha256 digests, OCI platform strings and actual observed image IDs under
the existing artifact/platform validation. Ownership snapshots use
`packages/protected-hooks/src/curie_protected_hooks/commissioning_local_ownership.py::OwnershipLedger`
and its exact entry grammar; evidence normalizes the retained parsed entries
to canonical bytes without rewriting the operational ledger. Original owned
UID/container identity is required even for removed rows. Absent or uncertain
identity never proves cleanup. Targets are included in the infrastructure trial roster; sessions use the
separate typed roster below and are never invented as OwnershipLedger kinds.
Neither roster is discovered by a prefix scan.

A session intent_id is a fresh UUID registered before opening its session;
launch_id is its owning PC-7 launch UUID, or null only for installation-owned
control/broker transports. owner_digest names the actual owning resource entry,
including its observed UID/container identity, which may be a retained control
resource. kind is exactly sdk, runner or broker. Identity is a tagged shape:
SDK has exactly `{session_id, process}`, runner has exactly `{container_id,
process}`, and broker has exactly `{client_id, broker_identity, credential}`.
Session/container IDs use opaque_ref; client_id is a generation; broker_identity
and credential use their existing closed grammars. process has exactly
`{container_id, pid, start_ticks}`, with opaque_ref container ID and generation
PID/Linux process-start ticks, so PID reuse cannot pass. Roster rows sort by
kind/intent_id and reference a matching binding with identical common members.
All intents and actual bindings remain retained; an unbound, unknown or missing
session blocks acceptance. The measured session registry's complete membership
is compared against both the retained roster and actual owned workload/process
and broker observations; an author's claim that its list is complete is not
proof. A session opened without prior intent aborts the run.

Cleanup resource_checks are sorted rows exactly `{subject, container_terminated,
absent}` for every trial-owned infrastructure entry. absent must be true.
container_terminated must be true for original runner containers/Pods before
finalizer removal; it is null for nonprocess targets, never an inferred join.
Retained installation controls are excluded from the removal roster and stay
identity-checked. session_checks are sorted rows exactly `{intent_id,
binding_digest, join_observed, transport_closed}` covering every roster member;
both booleans must be true. The join witness is a measured producer's actual
completed join/close operation corroborated against original process/transport
identity and independently observed stop effects. Resource absence or a control
progress report alone cannot set join_observed. Broker client absence is freshly
checked by epoch/client ID via the independent evidence observer; it is not a
claim that INFO or a local socket alone proves remote absence. SDK/runner join
and process identity require their genuine measured producers. If that witness
or complete membership cannot be obtained, the case is unavailable, never
replaced by all-true flags. These records bind required observations and do not
claim that existing component plumbing already supplies them.

Derived inputs retain the existing Manifest, ArtifactMeasurement and selection
canonical bytes named by those members. The runtime input role set is exactly
the predeclared directory inventory; its content digests are observed again
from the actual mounted inputs. ACL rows cover exactly G v2's principal map;
credential references, canonical exact rule-list digests and canonical sorted
password-hash-set digests are freshly measured through the separate ACL observer.
Credential values and raw password hashes never enter evidence.

Producer bindings are created from trusted measured compositions, never caller
registrations. `subcases` is a nonempty sorted array of the exact required
subcase names for that producer in the pinned profile. Its binding and expected
subcase set are fixed before its challenge. Case-evidence checks cover each
required subcase/subject exactly; the independent provisioner computes both
expected and observed digests from its real observations, validates the actual
predicate, and emits pass only when that predicate holds. Expected and observed
digests need not be equal for a test whose predicate is a required change.
Cross-linking a report or comparing its claimed digests does not evaluate that
predicate. A new producer without this concrete measured predicate/subcase
inventory is unavailable; no generic success predicate or extension map exists.

The installation tuple digest retains its existing encoding: canonical object
with exactly campaign_id, campaign_generation, permit, manifest, bootstrap,
guard, execution_config, broker_run_id, runner_image and artifact, as measured
by `InstallationTuple` in
`packages/protected-hooks/src/curie_protected_hooks/commissioning_verifier.py`.
It is reconstructed from retained typed permit/B/G/M/measurement records, not
accepted as a caller assertion. Changing C changes the tuple even without a
broker restart.

## Inventory, historical coverage and fresh final coverage

<!-- @spec COMMISSIONING-SERIES-5 -->
The initial plan has exactly ten campaign ordinals in this order. Each receives
a fresh C/permit, advances the durable campaign floor and starts only after
the previous campaign has settled and its cleanup and transition passed.

| Ordinal | Case | Required causal observation |
| --- | --- | --- |
| 1 | `exhaust-deliveries` | Real live owned work; delivery limit boundary, charged-last-operation exception, later dispatch refusal and complete stop. |
| 2 | `exhaust-launches` | The same proof for the launch counter, with its own live session. |
| 3 | `exhaust-model-calls` | The same proof for genuine gateway model dispatch charges. |
| 4 | `exhaust-output-tokens` | The same proof for the reserved output-token ceiling. |
| 5 | `exhaust-spend` | The same proof for worst-case spend reservations. |
| 6 | `live-session-expiry` | Broker TIME reaches expiry while an owned session is live; old authority refuses and all work terminates. |
| 7 | `control-restart` | Restart the actual owned control process while work exists; retained ownership resumes closing, no positive replay, all original work terminates. |
| 8 | `interrupted-closing` | Interrupt actual closing after ownership is durable; recovery only finishes cleanup, without reopening or replay. |
| 9 | `broker-restart` | Restart the actual owned broker process on the original storage; observe changed run_id, refusal of old permit, retained durable history and complete cleanup. |
| 10 | `final` | Fresh final-tuple checks below, revocation, termination and absence, with a closed journal. |

Trials 1 to 9 may finish `failed` only as the predefined consequence of their
trigger. A terminal label alone proves nothing. Every one has an independently
challenged live session, old-authority refusal and joined SDK/runner/broker
sessions, original-container termination before finalizer removal, and absence
of all trial-owned work and targets. The causal trigger must be observed, not
inferred from a timeout or forced journal write. No unrelated limit/deadline
may substitute for the scheduled trigger. An unscheduled fault aborts the run.

These nine results are historical transition evidence only. They satisfy the
old composite `counter-limit-boundaries`, `broker-restart`, `control-restart`,
`live-session-expiry` and `interrupted-closing` cases; they are not renamed
final-epoch observations. Every other PC-5 case runs freshly in the final
campaign: `positive-delivery`, `source-target-escape`,
`payload-signature-scope-tampering`, `mutation-tool-approval-refusal`,
`ordinary-and-old-worker`, `production-principals-refuse`,
`cross-installation-replay`, `cross-kind-substitution`,
`duplicate-and-ambiguous-dispatch`, `retries-parking-dead-letter`,
`image-violation`, `credential-violation`, `network-violation`,
`ephemeral-container-violation`, `static-skill-loading`, `clean-session`,
`read-only-tool-restriction`, `revocation`, `termination-and-absence`.

The final inventory additionally includes `final-broker-and-acl`,
`final-credential-separation`, `final-guard-and-network` and
`final-retained-history`. The independent provisioner freshly observes the
post-restart broker identity, exact instantiated ACLs/password baseline/phase,
credential separation, guard/RBAC/CNI/network enforcement, retained journals
and ownership. Checks repeat on every guard call and before publication where
the original contract requires them. The final positive case uses the actual
signed ingress and a real-model grounded reply receipt. SDK primitive cases
are measured in the actual pinned runner; this first series realization does
not reuse artifact observations. Each case has its own challenge and exactly
one passing receipt, with every required composite subcase passing.

Final cases use invalid requests or disposable hostile peers to exercise
violations; they cannot mutate measured retained controls and then repair them.
A case that needs to change the final tuple is unavailable and returns to
specification review. Revocation and termination/absence occur last and include
all work, not just the last positive delivery. Retrying a trial, using a spare
prestarted session after a stop or reclassifying a failed final case refuses.

## Allowed transitions and final premeasurement

<!-- @spec COMMISSIONING-SERIES-6 -->
Each result's transition is a **post-cleanup snapshot of that same trial**;
trial_tuple_digest equals its start record's tuple, and broker_before is that
trial's authenticated starting identity. broker_after is the freshly observed
identity after cleanup (changed only for trial 9). derived_inputs_digest names
the inputs actually retained at that point; after trial 9 these may still be
old, now-invalid M/selection/measurement bytes. This records their invalidation,
not a passing final tuple. The transition binds no future permit/C and never
contains a later start or result digest. Ordinal 10 has the same transition
shape, with unchanged broker identity and inputs, after final cleanup and
before publication; it has no successor campaign.

Each transition is independently observed and retained before the next permit.
Stable anchors include the installation ID, control Namespace UID, guard
configuration/revision, broker workload/storage ownership, network enforcement
identity, credential references/password baseline, B, G, execution configuration,
image/platform set, SDK profile and projection. A same-name replacement is not
the same anchor. Replacement of retained storage, installation or guard refuses.

Allowed changes are the new campaign C/generation/lane, the next trial's owned
temporary fixtures/workloads, the scheduled control process incarnation in
trials 7/8 and the scheduled broker process run_id in trial 9. Expected process
replacement does not authorize a new image, template, mount, service account,
Secret reference generation, storage or unrecorded resource adoption. The
ownership ledger records creates before action and binds new UIDs explicitly;
retired original UIDs remain in evidence. Its pending/uncertain rows refuse.

At the next ordinal's start the verifier compares that new fully measured tuple
and owned roster against the previous transition and the allowed changes below.
Prospective permit bytes may be constructed to measure the new C; the run
charges its full envelope durably before actual issuer SET NX, and persists
started only after successful readback. Ambiguous write/charge fails the run.
No transition grants authority by itself.

Candidate ACL templates in G stay fixed. Between campaigns, first stop/join all
old users' sessions and workloads, then install only the new permit-derived lane
rules and observe their exact instantiation and expected enabled state. No old
connection may acquire the new lane's authority. This does not rotate/reissue a
credential or update the administrator-owned password/state reference.

After trial 9's old authority is closed/failed and cleanup proven, derive the
new broker-bound M, selection metadata and manifest-bound artifact measurement
bytes in the existing dependency/hash order. The measurement must bind the
new M; calling its bytes an unchanged artifact is invalid. Reobserve actual
runtime-directory inputs, final production templates/mounts/Secrets, mediator
and gateway configuration and all final tuple facts. Selection is closed and
Qualification/Readiness absent. Only then premeasure the final tuple, durably
charge and issue the final permit, and begin ordinal 10.

From final premeasurement through activation, PC-6 permits only its existing
authorization-state writes. No further manifest/measurement/runtime file,
candidate instantiated ACL rule, template, image, mount, Secret reference,
guard or execution configuration change is permitted. Every final case checks
the current tuple against that premeasurement; publication rechecks it again.
No earlier trial's tuple digest is treated as the final tuple's digest.

## Acceptance, refusal and proof obligations

<!-- @spec COMMISSIONING-SERIES-7 -->
The independent verifier validates the complete closed plan, durable event
sequence and budget, ten start/results, every fresh challenged receipt and
referenced observation, exact allowed transitions, all cleanup and final tuple.
It must use measured producer compositions and actual retained state, not
report-writer assertions. No missing implementation defaults to success.

Only ordinal 10's closed campaign may receive PC-7 CommissioningAcceptance.
Its existing `evidence_digest` is the SHA-256 of this canonical dossier; it
binds the full prior history and final C. This is a changed evidence semantic,
not a new production record grammar. The issuer's existing binding checks do
not validate a dossier; its writer is constructed/invoked only after this
independent verifier returns a bound successful decision. Separate production
publishers and PC-6 selection activation remain unchanged. No trial gets an
acceptance record, partial Qualification or temporary production authority.

Proof must include genuine scheduled stops and exact final activation, plus
refusal for mixed installation/epoch/C/challenge, altered inventory/order,
omitted/duplicated results, forged progress, missing evidence, budget/reset/
concurrent coordinator attempts, crash after charge or ambiguous permit write,
retained-history loss, ownership replacement, old connection reuse, changed
final measurement/ACL/guard, unavailable producers, uncertain cleanup and
attempted publication for a destructive trial. Codec tests prove grammar only;
real broker/Kubernetes tests prove their components; only the complete real
series on a fresh installation proves this acceptance path. CI skips and fake
model campaigns never qualify it.

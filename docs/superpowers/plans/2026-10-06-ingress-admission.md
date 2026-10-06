# Protected ingress admission plan

Slice A3 of [#3603](https://github.com/curie-eng/curie/issues/3603) under the
[parent plan](2026-10-02-protected-hooks.md), task 3 there, after the
[route exposure plan](2026-10-06-source-admin-routes.md). Its contract is the
[ingress admission wiring](../specs/2026-10-02-protected-hook-source-policy.md#ingress-admission-wiring)
amendment to SOURCE-2, SOURCE-3, SOURCE-6, SOURCE-8 and SOURCE-9, the matching
LANE-3 and LANE-4 paragraphs in the [lane contract](../specs/2026-10-02-protected-hook-lane.md)
and the [admission ingress wiring](../specs/2026-10-03-protected-hook-admission.md#ingress-wiring)
amendment to ADMISSION-1 and ADMISSION-4 through ADMISSION-7. It realizes
accepted [ADR 0191](../../adr/0191-protected-hook-delivery-authority.md) and
adds no ADR. It is a feature for the next release and targets `next` from
origin/next `00c421da5`. It closes the code half of
[#4075](https://github.com/curie-eng/curie/issues/4075).

**Goal:** a signed delivery to a published protected source is admitted
atomically onto the private broker with an immutable receipt, a tombstone
restores ordinary delivery only while its ordinary publication is active,
protected PUT and rotate publish once evidence is current, the secret route
serves an active protected source, the probe reports `supported` exactly when
admission would accept, and preparing intents are reconciled without a caller.
No protected turn is consumed: the worker lane and provisioning stay out.

## Ownership and order

Commit the amendment and this plan first, alone. For each task a test author
writes behavioral tests from the contract without implementing it, observes
them fail against the unchanged implementation and commits them alone. A
separate implementer follows in a later commit. Every new test and unit cites
its SOURCE, LANE or ADMISSION ID. Independent specification, security and
quality reviews gate each task before its dependents start. One owner edits
each file.

Owners: the protected hooks package owner edits
`packages/protected-hooks/src/curie_protected_hooks/` and its tests; the API
bootstrap owner edits `apps/api/src/curie_api/protected_runtime_files.py`; the
API source owner edits the coordinator, admin service, source broker and
source policy router modules and their tests; the integration owner alone
edits `apps/api/src/curie_api/routers/hooks.py`,
`apps/api/src/curie_api/hook_source_auth.py`,
`apps/api/src/curie_api/protected_support.py`, `apps/api/src/curie_api/main.py`,
the OpenAPI artifact and the operator note. Leave the worker, kernel,
consumer, thread locks, markers, ACI, plugin format, charts, cron fire and
`apps/api/src/curie_api/auth.py` unchanged.

Tasks 1 through 8 land in one pull request to `next`, because #4075 requires
the `supported` answer to land with the ingress wiring, and an active
protected publication before ingress admission would hand out a scoped key
that ingress refuses. That pull request depends on
[#4131](https://github.com/curie-eng/curie/pull/4131), which brings the torn
read fix to next; its tests start only after #4131 merges.

## Tasks

| Task | Owner | Criteria | Depends on |
| --- | --- | --- | --- |
| 0. Torn read fix on next, #4131 | Protected hooks package owner | ADMISSION-4 | #4131 merged |
| 1. Shared authority evaluation | Protected hooks package owner, then integration owner for the probe | SOURCE-9, ADMISSION-4 | Spec commit |
| 2. Enqueue file, transport and measured ZRANGE recipe | API bootstrap owner, protected hooks package owner | SOURCE-6, LANE-3, ADMISSION-6 | Spec commit |
| 3. Facade amendments | Protected hooks package owner | ADMISSION-1, ADMISSION-4, ADMISSION-5, ADMISSION-7 | Tasks 0, 1, 2 |
| 4. Protected publication, GET and secret | Protected hooks package owner, API source owner | SOURCE-3, SOURCE-6 | Tasks 1, 2 |
| 5. Ingress wiring and receipts | Integration owner | SOURCE-2, SOURCE-8, LANE-4 | Tasks 2, 3 |
| 6. Probe `supported` and parity | Integration owner | SOURCE-9 | Tasks 1, 5 |
| 7. Reconciliation owner | Integration owner | LANE-4, ADMISSION-5 | Tasks 2, 3 |
| 8. OpenAPI, operator note, gates and ladder | Integration owner | All of the above | Tasks 4 to 7 |

Tasks 1 and 2 may run in parallel on independent files and broker resources.
Task 4 seeds control records directly and need not wait for task 3. Task 7
may run beside task 5 once task 3 lands, under the same owner in sequence
because both edit the API composition.

**Task 0.** [#4131](https://github.com/curie-eng/curie/pull/4131) cherry picks
`1423b5133`, `487378808` and `45440e152` from main (merged there as
`a9b587075`) onto `next`; they apply cleanly to `00c421da5`. A wholesale
forward merge of main is not used: main also carries the source feature
reverts (`4028180c1`, `5ce293357`, `468ddef03`), which would undo the next
only administration and probe work. This plan starts its tests only after
#4131 merges, and records that
`uv run --frozen pytest packages/protected-hooks/tests -q -p no:randomly`
passes on the resulting base. Without it, task 5's concurrent retry tests
would flake on the same `AdmissionUnavailable`.

**Task 1.** Add the pure `authority_evaluation` module with its closed
outcome, its target and reads records, its admission and publication phases
and its frozen reason table. Move the probe's step logic into it and make
`apps/api/src/curie_api/protected_support.py::_decide` a call plus the one to
one reason map. Failing first tests: a frozen vector covering every step's
first failing condition, malformed records counting as absent at their own
step, the publication phase accepting a held reservation without an active
record, and the mapping table; then
`apps/api/tests/test_hook_source_support_broker.py::test_first_failing_step_decides_the_reason`
and its siblings pass unchanged except the final valid case, which task 6
changes.

**Task 2.** Add the `enqueue.json` loader and `EnqueueCredential`, and
`AuthenticatedEnqueueClient`. Before ZRANGE joins the enqueue recipe, measure
it against the pinned Valkey on an owned disposable broker and record the
exact recipe, command and observed outcome in
`docs/adr/evidence/0191-protected-hooks/README.md`; the recipe changes only in
the commit after that record. Failing first
tests for the loader: extra, missing or duplicate members, other version,
numbers, `default` username, the reader's username, a `credential_ref`
differing from the manifest, oversize, FIFO and directory all refuse; a valid
file parses and redacts; the administrative loader and the probe's three file
loader never open it. Transport tests on the disposable TLS broker with an
`admission_acl_rules("enqueue")` principal: connection verifies pin, CA,
hostname and run_id before commands; a killed connection is never reopened;
an expired budget refuses; the enqueue principal cannot write source or
control keys, consume, XACK or administer; ZRANGE succeeds on the quota key
and refuses elsewhere; nothing exported is a raw client.

**Task 3.** Change the facade constructor to the trusted manifest, route its
preflight through task 1 with an observation, apply the preparing retry rule
and add `preparing`. Failing first tests on the real broker: control manifest
bytes differing from the trusted manifest refuse with no write; a preparing
retry with a different `received_at` recovers the original and returns its
receipt once authority opens; a byte identical retry restores deleted
recovery bytes; `preparing` lists exactly outstanding intents in score order,
skips committed, failed and orphan members and writes nothing (key and stream
snapshot before and after). Every existing test under
`packages/protected-hooks/tests` passes.

**Task 4.** Add `publish_protected` to the fence and writer, replace the
coordinator's deferred branch with reader bracketed publication on fresh
connections, and complete GET activation and the secret's active path.
Failing first tests on real stores: PUT and rotate publish and answer 200
active when evidence is current; each evaluation outcome refuses with the
committed generation and no active record; an expired readiness between the
reader check and the CAS still publishes, and a delivery then refuses
atomically; exact replay publishes a committed unpublished operation; a lost
reservation answers `source_reservation_lost`; a delayed publisher loses to a
later reservation; GET reports protected active and each closed reason; the
secret is served only for an active protected record, with `no-store`, and is
refused for every other state with no key in any response, error or captured
log. Rewrite the existing tests that pin `source_publication_deferred` and
`publication_deferred` in `apps/api/tests/test_hook_source_admin.py`,
`apps/api/tests/test_hook_source_admin_routes.py` and
`apps/api/tests/test_hook_source_admin_faults.py` to the new answers, keeping
each one's mechanic.

**Task 5.** Wire `ingest_hook` for protected and tombstone rows, the bounded
ingress gate wait, the two thread admission executor, the new `HookAccepted`
members and the result table. Failing first tests over real HTTP: one
protected delivery yields one intent, binding, quota member and stream entry
with the exact envelope and turn, no ordinary claim, backlog slot, workspace
row or SQL write; explicit reply target 422; declared source bindings 503;
oversize turn 413 with no broker I/O; exact, freshly signed and post closure
retries return the original receipt; changed body, policy and generation
conflict; a prior ordinary claim, pending or enqueued, conflicts with no
broker write; each refusal row answers as tabled with an unchanged broker
snapshot; 24 concurrent deliveries of one ID on eight threads produce one
entry; a stale signature after rotation keeps 401; tombstone ingress admits
only with its ordinary publication active, refuses a private intent and closes
on broker failure; never configured hooks keep every existing answer and open
no broker connection; a third gate waiter on a protected row answers 503
within the bound while an ordinary waiter keeps waiting without a bound;
cancellation releases the gate only after the broker call. Update
`apps/api/tests/test_hook_source_ingress.py::test_configured_or_history_source_is_closed_before_every_effect`
so that only pending history and unpublished rows stay closed.

**Task 6.** Add step 1a and step 12 to the probe and remove the final
`configuration_unsupported`. Failing first tests: a fully valid tuple answers
200 `supported` with runtime members; source bindings, a missing or unbound
enqueue file and closed admission each answer their reason; then the parity
test, which drives the same seeded broker states through the probe over HTTP
and through a real signed delivery and asserts that the delivery is accepted
exactly when the probe answered `supported`, outside the stated per delivery
exclusions. Update
`apps/api/tests/test_hook_source_support_broker.py::test_fully_valid_tuple_reports_unsupported_with_runtime_members`.

**Task 7.** Add the reconciler and its lifespan wiring. Failing first tests:
with no caller retry, an interrupted intent commits once authority opens; a
closed authority consumes one attempt per tick and fails with refund at the
tenth; the 300 second deadline fails and refunds; an unset setting or invalid
file performs no broker I/O; two reconcilers on one broker never double append
or double refund; shutdown joins within ten seconds with a paused broker; logs
carry no payload or credential.

**Task 8.** Regenerate OpenAPI with `uv run python -m curie_api.export_openapi`
and run `uv run pytest apps/api/tests/test_openapi_drift.py -q`. Extend the
operator note in `docs/interfaces/triggers/INTERFACE.md`: protected
publication and its refusal codes, the enqueue file, tombstone restoration,
the new receipt members, the 429 backlog and its release only by the future
worker. Then run the full affected suites, the isolated Python baseline,
Ruff, mypy, import boundary and `scripts/check-docs.sh`, and the tier
evidence below.

## CI gate reminders

* A new request body module goes into
  `apps/api/tests/test_nullable_override_parity.py::KNOWN_BODY_MODULES`. This
  slice plans none: `HookSupportIn` and the source policy bodies already live
  in the registered `curie_api.hook_source_policy_schemas`, and `HookAccepted`
  is a response. If an implementer adds one, register it in the same commit.
* A new route goes into the operations list in
  `packages/telemetry/src/curie_telemetry/metrics.py`, checked by
  `apps/api/tests/test_openapi_telemetry_operations.py`. This slice adds no
  route; the hook, probe and source policy routes are already listed.
* Any Slack style identifier in a fixture, test or document uses only
  `C0EXAMPLE<n>`. Fixtures use placeholder agents, hooks and delivery IDs.
* No new Settings field is planned, so the generated environment example does
  not change; a new field would need its drift gate regenerated.

## Real backing store strategy

Postgres is real and owned per test through `isolated_migration_db` at head,
as the existing source tests use it. The ordinary Valkey is the existing
owned test instance. The broker is the owned disposable TLS Valkey from
`packages/protected-hooks/tests/admission_broker.py`, default user disabled,
with exact container identity and label cleanup and ephemeral fixture CA and
server certificates. Provision three distinct named principals with
`admission_acl_rules("enqueue")`, `metadata_acl_rules("control_reader")` and
`metadata_acl_rules("source_writer")`. A fourth fixture administrator seeds
selection, manifest, qualification and readiness, injects faults and inspects
state; it never appears in the runtime directory. Write the runtime directory
privately with mode 0700 per test.

Inject faults only through real store operations on owned resources: literal
key ACL changes on owned users, client kills and pauses, key deletion or
restore, owned container restart and seeded readiness expiry against broker
time. Product code gains no failure injection parameter. Fixtures redact their
own diagnostics and publish only anonymous values.

## Tier classification

| Tier | Classification | Reason or command |
| --- | --- | --- |
| skill | not applicable | No runner loop, ACI event, bundle or skill packaging change; the turn shape is the existing QueuedTurn. |
| local | required, tasks 5 to 8 | API wiring and a lifespan task change. `curie local up --build`, then `CURIE_E2E_TIERS=local curie dev e2e-ladder`, plus signed deliveries against that stack: a never configured hook enqueues and dedupes as before; an unpublished protected source and a probe answer 503 `runtime_unavailable` with no runtime directory; the reconciler idles. |
| local-release | not applicable | No migration, version pin, release compose or image identity change. Promote if the path guard maps a changed file. |
| cluster | not applicable | No chart template, RBAC, secret mount or NetworkPolicy; mounting the runtime directory is LANE-8 and #4076. |
| live provider | not applicable | No protected turn reaches a model, runner, MCP catalog, PreToolUse or workspace path in this slice. |
| external integration | required, tasks 5 and 6 | Signed hook ingress changes. Drive real signed deliveries and probes from an independent sender over the network to the candidate stack, in live mode, for the ordinary and closed protected cases. Positive protected admission is proven only by the real store suites with fixture seeded authority. |
| factory | not applicable | No factory runtime, CI, progress, publication or work item change. |

Positive protected admission against a running stack needs an out of band
provisioner for the runtime directory and a qualified runtime, and its
delivery to a runner needs the protected worker. The PR records
`Discovery waiver: no provisioner can supply a protected runtime to the local stack #4076`
and
`Discovery waiver: no protected worker consumes or replies to an admitted delivery #3603`.

## Prior intent to preserve

* `apps/api/src/curie_api/hook_signing.py::material` binds hook, requested
  policy, delivery ID, timestamp and raw body to the delivery signature; the
  bytes stay unchanged and the support purpose stays separate.
* `7e9861074` (#3808) releases only owned failed ordinary claims and refunds
  only owned reservations. The ordinary path keeps that; protected failure never
  becomes success or a fresh delivery ID.
* `ce20ade6a`, `842dfa773` and `f68b02750` ordered replay and history before stale CAS,
  registration before reservation, one authoritative commit and gate release
  before publication. Only the protected publication branch changes.
* `796f151dc`, `b8e110b63`, `c5fc414e0` and `a8b28ae06` built the probe, its
  four slot executor and budgets. They stay; only the decision moves into the
  shared module and two steps are added.
* `fc2eca6ac` authenticated the reader and `84b7463ce` fixed the role recipes.
  The enqueue transport is a sibling; the recipe changes only by ZRANGE.
* The atomic admission foundation, ADMISSION-1 through ADMISSION-7, and the
  torn read retry stay authoritative for every broker effect; ingress adds no
  second broker write path.
* The cron durable claim order and the refusal of configured hooks in
  `apps/api/src/curie_api/routers/hook_fire.py` and
  `apps/worker/src/curie_worker/hook_source_guard.py` stay unchanged.

## Review risks

* The bounded five second gate wait applies only to protected ingress and
  administrative requests, chosen by the ungated authentication; ordinary
  waiters stay unbounded and can still hold gate connections under contention.
* Gate pool pressure: two admission slots plus two administrative mutations
  can hold all four gate connections for a broker budget. Pool checkout
  itself stays outside the bound until #4091.
* The preparing retry rule loosens a conflict into recovery. Review that the
  signed tuple, not the turn bytes, is the duplicate identity under SOURCE-8,
  and that a different reply surface on retry cannot redirect the original.
* Publication's evidence check is not atomic with its CAS. Safety rests on
  every delivery repeating the evaluation atomically.
* Parity is tuple level only. The probe uses the control reader; ingress uses
  the enqueue principal. A refused enqueue credential makes the two differ as
  an availability fault.
* The global quota of 64 stands for the first release; per agent fairness
  belongs to the protected worker lane. Released only by the future worker,
  it fills and answers 429 until LANE-6 lands.
* Declared source bindings on any hook of the agent exclude it from protected
  delivery.
* Reconciler attempts count across replicas.
* The enqueue file shares the runtime directory; keeping it out of ordinary
  workers and runners is a LANE-8 guard concern.

## Completion

No protected turn reaches a worker, runner, model or reply in this slice. Do
not close #3603, #4076, #4053, #4054 or #4091. Close #4075 only when the
parity test and a real accepted ingress delivery are recorded on the final
candidate. Name #4053 and #4054 as the CLI and console siblings in the PR body
under the parity seam rule. No provisioning, deployment, downstream import or
external message is part of this plan.

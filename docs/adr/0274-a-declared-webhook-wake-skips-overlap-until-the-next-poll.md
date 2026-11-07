# 274. A declared webhook wake skips overlap until the next poll

Date: 2026-11-05

Status: Accepted

Accepted on 2026-11-07 with explicit maintainer approval from Brian Conn,
before implementation.

This decision amends [ADR 0079](0079-inbound-triggers-as-a-new-event-kind.md),
[ADR 0099](0099-hooks-are-bundle-declared-turns-the-system-starts.md), and
[ADR 0258](0258-a-scheduled-hook-turn-carries-its-deployment-authority-on-http-identity-bindings.md)
only as specified below. It preserves the boundaries of
[ADR 0190](0190-automated-hook-sources-cannot-widen-their-tool-access.md),
[ADR 0191](0191-protected-hook-delivery-authority.md), and
[ADR 0259](0259-a-binding-requires-http-identity-when-its-invocation-or-approver-role-needs-http-facts.md).
It supplies the opt-in targetless part of
[#3666](https://github.com/curie-eng/curie/issues/3666), without closing that
issue or changing its existing path-only declarations. Acceptance authorizes
implementation and does not claim runtime support.

## Context

A hosted connector owns a durable work queue and polls an external system.
Empty polls need no model. When work needs judgment, the connector needs to
wake one declared hook, let the agent read and update that queue through its
MCP tools, and receive no automatic channel reply.

Source inspection after fetching `origin/next`, pinned to
`678a5665d8fe7f10e78fdb282aee8d9210b37021`, establishes these starting points:

1. `packages/plugin-format/src/plugin_format/models.py::TriggerDeclaration`
   already has optional prompt and target fields. The validator in
   `packages/plugin-format/src/plugin_format/validate.py::_validate_triggers`
   accepts path-only webhook declarations. Ordinary ingress in
   `apps/api/src/curie_api/routers/hooks.py::ingest_hook` does not resolve
   those declarations; it queues payload text to a reply binding.
2. `packages/aci-protocol/src/aci_protocol/turn.py::QueuedTurn._validate_targetless_cron_identity`
   already permits a targetless `TurnSource.CRON` turn with a `HookRunRef`.
   `apps/api/src/curie_api/routers/hook_fire.py::fire_hook` uses that queue
   label even though its run record has `source='manual'`. Queue source and
   run provenance already serve different purposes.
3. `apps/worker/src/curie_worker/kernel/identity.py::prepare` returns before
   binding classification when the reply handle is absent. Targetless cron
   therefore does not invoke scheduled-authority version rechecks.
   `apps/worker/src/curie_worker/kernel/lifecycle.py` routes it through
   `BindingResolver.resolve_agent` and refuses targetless approval gates.
4. `apps/worker/src/curie_worker/hook_runs.py::HookRunRecorder` already owns
   start guards, lease renewal and terminal updates. Admission locking and
   lease reclamation exist in `apps/worker/src/curie_worker/cron_loop.py`.
   These are the foundation for overlap handling, rather than a new outbox
   or recovery service.
5. `packages/plugin-format/src/plugin_format/connectors.py` refuses
   `sealed_secrets` because no production decrypt path exists. Its existing
   `SecretRef` supports operator-managed secret delivery. A single plain
   string secret can become a connector client bearer and reach the sandbox;
   an implicit `SecretRef` does not select that bearer.

## Decision

### 1. A declaration opts in explicitly by name

<!-- @spec TARGETLESS-WEBHOOK-1 -->
A webhook is a wake declaration only with literal `wake: true`, a required
nonblank `name` and standing `prompt`, and no `target`. The optional boolean
field is additive on the lenient `TriggerDeclaration` model. Omitting it or
setting `wake: false` preserves existing webhook declarations, including
path-only declarations and declarations that already have a prompt and no
target. An explicitly null or blank target remains invalid. There is no new
trigger type or generated prompt.

For a wake declaration, `name` is its hook identity and must match
`apps/api/src/curie_api/hook_partition.py::HOOK_NAME`. Names remain unique
across the bundle's named triggers, including cron hooks. `path` stays
required with its existing #3666 meaning; it neither selects the wake route
nor has to equal `name`. Deploy validation and runtime declaration resolution
agree on these rules. The standing prompt and deployed tool policy supply
the entire turn; delivery data supplies neither. Targeted webhook mapping
remains outside this change.

For example:

```json
{"type":"webhook","wake":true,"name":"queue-ready","path":"/inbound","prompt":"Read pending work through the connector tools and record each result."}
```

Deploy receipts expose wake URLs and targetless mode without credentials.
The scheduler continues to select cron declarations only; declaring a wake
creates no timer and empty connector polling creates no platform execution.

### 2. A derived key wakes one hook through a separate route

<!-- @spec TARGETLESS-WEBHOOK-2 -->
Wake requests use their own `POST /hooks/{agent_id}/{hook}/wake` route.
`ingest_hook` and its protected branch retain their existing behavior. The
wake route accepts no query parameters and only the empty JSON object as its
body. It never accepts a target, prompt, tool policy or reply coordinates.

The installation signing key derives a reproducible key over agent id, exact
hook identity and one stored generation integer. A wake-specific derivation
label and signed-material prefix, such as `curie.hook.wake.v1`, separate it
from ordinary ingress and protected-source signatures. Reuse the existing
HMAC framing, required timestamp and delivery id, and
`hook_signing.TOLERANCE_S` of 300 seconds. The signature binds the agent,
hook, timestamp, delivery id and exact body. Reusing header names does not
reuse another route's signed purpose.

The API authenticates with wake metadata before reading the deployment's
bundle. Unknown and invalid credentials receive the same auth refusal. It
never tries the shared agent key or a protected-source key after a wake-key
failure. A wake key cannot authenticate another hook, ordinary
ingress, an operator operation or an MCP request. Authentication is followed
by declaration and ordinary-source checks before any run mutation.

The existing administrative authority reads the current derived key and
rotates it by incrementing the one stored generation integer per agent and
hook. Key retrieval follows
`apps/api/src/curie_api/routers/agents.py::get_hook_secret`, including
`Cache-Control: no-store`. Repeated reads reproduce the same current key.
Rotation invalidates the previous key immediately, with no overlap period.
Only the generation is stored, never the clear key. There is no enable or
disable state. Rotation serializes with admission and affects new admissions;
it does not revoke queued runs or undo effects. Existing operator pause
refuses new admission and prevents a queued run from starting; kill keeps its
existing execution control. Exact administrative verbs and CLI shapes belong
in the implementing interface contract.

The operator provisions the returned key into an operator-managed secret
referenced by the connector's `secrets` through `SecretRef.from_secret`.
There is no sealing API, recipient key or dependency on unimplemented sealed
secret delivery. The interface contract designates the wake-key environment
name so both deploy validators can reject its explicit or implicit selection
as `bearer_secret`, including a same-named plain string secret. A separate
client bearer, when needed, has a different name; `no_client_auth` keeps its
existing meaning. The wake key is absent from sandbox environment, MCP
headers and results, deploy receipts, logs and traces. The authorized key
retrieval response and the connector's own secret delivery are its only
intended clear-value surfaces.

Issuance and wake admission refuse a hook with source policy or unresolved
source history, a `remediation_policies` row, a partition, or a source binding.
Reuse the source gate and
`apps/api/src/curie_api/routers/hook_fire.py::_require_ordinary_source` for
policy and history checks; explicitly check the other three configurations.
Unavailable authority refuses closed on key retrieval, rotation and every
wake admission. Nothing clears protected history or reaches `_ingest_protected`,
changes protected consumption authority, or qualifies an ordinary runner
for protected execution. ADRs 0190 and 0191 remain intact.

Accepted parity: the existing shared agent key can already address a cron
hook's name through ordinary ingress and its shared hook conversation id.
This change does not isolate that pre-existing path or claim that only wake
requests can ever address that conversation. It gives the connector a
narrower key without distributing the shared key.

### 3. The worker runs a silent targetless CRON-labeled turn

<!-- @spec TARGETLESS-WEBHOOK-3 -->
Admission resolves the ranked active deployment and its wake declaration,
checks pause, kill and existing configured-limit guards, and inserts an open
`hook_runs` row with `source='webhook'` and the admitted version id. It enqueues
the standing prompt as `TurnSource.CRON`, with a complete `HookRunRef`, the
ordinary hook conversation identity, and no reply handle or human context.
The UTC slot is a server-allocated unique run instant, not a cron schedule.

CRON is the existing execution-routing label for this targetless shape.
Webhook is the run's recorded provenance. No ACI field, enum, validator,
wire version or generated ACI reader changes. Operator queries report the
webhook trigger and source honestly rather than presenting a scheduled fire.
The source CHECK constraint and typed run readers gain `webhook`.

The worker keeps targetless cron behavior: it validates the open run, resolves
the agent directly, applies existing pause, kill and runner controls, and
boots with the ordinary agent-scoped connector caller token. It selects no
chat binding and fabricates no person, including when the agent's channel
bindings require HTTP identity. A connector method requiring a human still
refuses that caller token.

A wake run that encounters pause after admission closes terminally as
`blocked` with reason `hook_paused`. It never becomes a deferred slot for the
cron scheduler to reopen. Resuming permits a later connector poll to wake
fresh work, with no automatic replay of the blocked run. This changes only
webhook-provenance runs; scheduled and manual cron behavior stays intact.
The worker reads provenance from the stored run, not from the CRON queue
label. Wake runs never continue as sweep slices or emit sweep coverage
notices. Existing targetless exclusions remain in force; cron-only trigger
lookups do not acquire a wake branch.

There is deliberately no new deployment-version comparison at prepare,
claim or start. As with targetless cron, the prompt and run provenance can
come from the admitted version while the worker resolves a newer active
deployment. Removal or replacement after admission does not automatically
cancel that queued run. Operators use pause or kill when that distinction
matters. The stronger HTTP-bound scheduled-authority predicate remains for
targeted schedules; it is not claimed for this path.

There is no placeholder, automatic reply, status post, approval card or
approval resume. A gated tool fails before its effect with
`approval_gate_targetless`. Ordinary deployed tools may still write; silence
is not a read-only policy. Existing explicit channel-read rules remain.

### 4. Suppress overlap and deduplicate signed deliveries

<!-- @spec TARGETLESS-WEBHOOK-4 -->
Reuse the existing per-agent, per-hook transaction advisory lock for admission
and expired-lease reclamation. There is no follow-up flag, successor admission,
permanent delivery receipt store, SQL outbox, reconciler or failure counter.
`HookRunRecorder.close` keeps its boolean result and only settles the current row.

After authentication, declaration resolution and all ordinary-source, pause,
kill and configured-limit checks, reserve the authenticated delivery id with
Valkey `SET NX EX` before any run mutation. Scope the key to installation,
agent, exact hook, current generation and delivery id. Retain it until after
the signed timestamp plus `hook_signing.TOLERANCE_S`, including the inclusive
acceptance boundary and accepted future timestamps; a fixed 300 seconds from
receipt is insufficient. Valkey failure or an uncertain reservation refuses
admission without SQL mutation or stream publication. An existing reservation
returns `coalesced`, even after the original run settles.

For a newly reserved delivery id:

1. Reclaim expired open claims under the advisory lock. If a live open run
   exists, return `coalesced` without changing that run, retaining a follow-up,
   or appending a stream entry. Only the bounded dedupe reservation is written.
2. Otherwise insert one open webhook run with an explicit claim lease, commit,
   enqueue the targetless turn and return `queued` after enqueue succeeds.
3. Success, failure and duplicate or stale closes never admit a successor.
   Work arriving after the final queue read waits for another connector poll
   and a fresh delivery id.

The bound is one open run per hook. Distinct hooks remain independent.
Cron overlap retains its existing recorded skip behavior.

API settings and deployment wiring must use the same effective claim lease as
`WorkerConfig.effective_hook_claim_lease_s`: `CURIE_HOOK_CLAIM_LEASE_S` when set,
otherwise `CURIE_DELIVERY_BUDGET_S`, with the lease covering the delivery budget.
The current manual-fire insert supplies no lease and is insufficient.

A crash or uncertain stream append after SQL commit may leave an open run
without dispatchable work. Retain the dedupe reservation. A later connector
poll uses a fresh delivery id; after claim-lease expiry it can reclaim and admit
fresh work. A known enqueue failure is transient and may close the row as
`enqueue_failed`; an uncertain append never proves that nothing was admitted.
There is no autonomous dispatch recovery or exactly-once external processing
claim. Existing delivery ownership, renewal and terminal guards handle stale
queue entries. Polling and backoff add delay beyond the claim lease.

A current key holder can still sign fresh deliveries and sustain serialized
model cost. One open run bounds concurrency, not spending or effects; existing
configured-limit checks are not a daily spend ledger.

Operator-only queries expose webhook provenance and terminal reasons.
Connector polling, durable item claims, idempotent writes and backoff belong
in interface and bundle documentation. Poll again when unprocessed work
remains, using a fresh delivery id; no privileged run-query access or knowledge
of settlement is required.

## Amendments and supersessions

1. ADR 0079 decisions 1 and 2 gain this opt-in wake class: a separate scoped
   credential and route, with no automatic channel output. Ordinary ingress
   and its existing declaration work under #3666 are unchanged.
2. ADR 0099's Declaration and Triggers sections gain this opt-in mapping.
   Its approval-card authorization is superseded for this class by decision
   3's targetless refusal. Its per-hook serialization and rejection of buffered
   fires remain; this class returns `coalesced` without recording a skipped
   run, as decision 4 states. Cron keeps its existing schedule and overlap
   rules.
3. ADR 0258 decision 2's writer inventory gains the wake admission path.
   External wake admission is authenticated by a hook-scoped key rather than
   an operator key and creates declaration-derived run rows. Completion
   remains a terminal update, never an admission writer. Its targeted
   scheduled-authority predicate and boundary rechecks remain unchanged;
   this targetless path intentionally retains cron's absence of those
   version comparisons. The queue source remains routing data.
4. ADRs 0190, 0191 and 0259 are not superseded. No part of this proposal
   establishes protected execution, human invocation or approval authority.
   No prior ADR is superseded in whole.

## Realizing work and size

The estimate is 4 to 5 engineer-days, with a 4.5-day working split. It assumes
available test infrastructure, existing secret references and targetless
execution. It excludes sealed secret implementation, a new queue source,
dispatch services and protected worker qualification. Acceptance authorizes the
realizing work; review or infrastructure waits are not engineering execution
time.

1. **Declaration and secret-role contract, 0.75 day.**
   `packages/plugin-format/src/plugin_format/models.py`,
   `packages/plugin-format/src/plugin_format/validate.py`,
   `packages/plugin-format/src/plugin_format/connectors.py`, and
   `cli/src/connector_build.rs` own explicit `wake: true`, name identity and
   shared credential-role cases. The frozen-package change lands as a
   separately reviewed compatible contract prerequisite, with generated
   artifacts and compatibility checks as required by `packages/CLAUDE.md`.
   Existing path-only and prompt-bearing webhook fixtures stay valid without
   opting in; a wake may have different `name` and `path`.
2. **API, persistence and migration, 1.25 days.**
   `apps/api/src/curie_api/routers/hook_fire.py`,
   `apps/api/src/curie_api/routers/agents.py`,
   `apps/api/src/curie_api/routers/schedules.py`,
   `apps/api/src/curie_api/models.py`, and
   `apps/api/src/curie_api/schemas/schedules.py` supply the existing patterns
   and operator controls. New wake-specific route and signing code owns
   authentication, generation retrieval/rotation, ordinary-source refusal
   and locked admission. One migration adds generation metadata and
   `webhook` to the source CHECK constraint, with no flag or enablement state.
   Follow `apps/api/alembic/versions/0080_hook_run_source.py`; update migration
   registration, serving metadata and schema-head assertions without editing
   historical migrations. Refuse a lossy downgrade. Update
   `apps/api/src/curie_api/routers/hook_fire.py::_record`, which currently
   hardcodes `trigger="cron"`, and the `HookRunSource` alias used by
   `HookFireOut`, which currently accepts only `schedule` and `manual`.
   Add API lease settings in `apps/api/src/curie_api/config.py` and share the
   effective worker values through `charts/curie/templates/api.yaml`,
   `charts/curie/templates/worker.yaml` and `compose.dev.yaml`. Audit
   `cli/src/local.rs` and its `up_command`, which starts that stack, for local
   propagation. Cover configured overrides and the delivery-budget default;
   do not instantiate worker runtime configuration inside the API.
3. **Worker pause settlement and cron audit, 0.5 day.**
   `apps/worker/src/curie_worker/hook_runs.py` supplies stored provenance to
   the worker and correct metric labels while preserving
   `HookRunRecorder.close`'s boolean contract. In
   `apps/worker/src/curie_worker/kernel/lifecycle.py`, close a paused wake as
   `blocked` / `hook_paused`, never `deferred`. Audit
   `apps/worker/src/curie_worker/cron_loop.py` for cron-only selection and
   deferred reopening; reuse its admission/reclaim pattern in the wake API.
   Audit `apps/worker/src/curie_worker/kernel/sweep_slices.py`,
   `apps/worker/src/curie_worker/sweep.py`,
   `apps/worker/src/curie_worker/kernel/completion.py`, and
   `apps/worker/src/curie_worker/kernel/hooks.py` for existing targetless
   exclusion, terminal settlement and honest provenance. Wake runs never
   slice as sweeps or produce coverage notices. No successor enqueue, new
   worker loop or identity exemption is needed.
4. **Operator surfaces and interface docs, 0.75 day.**
   `cli/src/commands/deploy.rs` supplies `DeployOutput` and
   `deploy_cron_triggers_from_manifest`, the existing cron receipt patterns
   alongside which wake receipts belong. `cli/src/commands/agent_actions.rs`
   and `cli/src/api.rs` own the action/client patterns. Extend the schedule
   router's cron-only pause-name resolver to include declared wake names;
   resume of a wake permits fresh polling without catch-up. Add key retrieval,
   rotation and run reads in local and cluster modes, command manifest and
   console mappings as required by the CLI contract. The source-only skill
   mode remains unavailable for durable admission. Update
   `docs/interfaces/triggers/INTERFACE.md` with provenance, credential handling
   and polling/backoff requirements. Keep wire spelling and procedural
   connector guidance there and in bundle docs.
5. **Integration and acceptance, 1.25 days.** Finish the executable controls
   below across real SQL and stream services and the supported worker
   substrates, including live-provider and external-integration proof for
   the actual connector, sandbox and MCP path. Reuse negative cases across
   tiers. `tests/vectors/hook-run-reasons.json` already includes `hook_paused`;
   extend it and both typed readers only if a new reason is needed. Sweep
   migration pins, deploy/runtime validator parity, API/CLI field parity and
   rendered lease-setting parity. Missing required runtime evidence remains
   a completion blocker.

Package 1 precedes dependent contract adoption. Packages 2 and 3 agree on stored
source and lease semantics, then can develop in parallel. Package 4 uses that
fixed contract; package 5 verifies the integrated result. Expose wake admission
only after the migration, lease wiring and pause-aware worker pool are deployed.
Use operator pause and settle admitted wake runs before rolling that worker pool
back; rotating a key does not settle them. No follow-up state needs a rollout or
rollback protocol. Wake admission must remain disabled until decision 4's dedupe
is available.

## Required acceptance evidence

Each item records the candidate commit, exact command and observed outcome. No
runtime evidence is claimed.

1. Production declaration validators and runtime loaders consume shared cases
   for explicit `wake: true`, unchanged path-only and prompt-bearing webhooks
   without opt-in, valid different `name`/`path`, invalid or missing wake
   names, invalid targets and name collisions. Both deploy validators reject
   selecting the wake-key role as an MCP bearer; the real renderer and
   sandbox boot confirm that the key stays in the connector alone.
2. Real API, database and stream tests prove exact agent/name scope, distinct
   signing purpose, no shared-key fallback, stale timestamp refusal,
   immediate rotation, reproducible no-store key retrieval and refusal of
   body/query overrides. Source policy/history, remediation, partition and
   source-binding controls refuse before run mutation. Race rotation, pause
   and source-policy mutation with admission. Pause refuses fresh admission
   and prevents queued starts; rotation leaves admitted execution intact.
3. Concurrent wakes against a blocked real worker yield one open run. Additional
   fresh delivery ids answer `coalesced` without hook-run mutation or another
   stream entry; only their bounded dedupe reservations are written. Wakes
   before start, at the last queue read and during close never retain a
   follow-up. Success, failure and duplicate/stale closes create no successor.
   Leave an item after that last read, then show that a fresh poll and wake
   after settlement processes it. Lose a stream append, restart, expire the
   explicit claim lease and recover through the next wake. Exercise stale queued
   entries and normal lease renewal. Replaying the same signed delivery admits
   no second run, including after settlement. Exercise concurrent replay,
   accepted future timestamps, dedupe expiry, Valkey failure and fresh-delivery
   recovery; stale signatures fail. Prove cron overlap still records its
   existing skip.
4. Drive the hosted connector through API, worker, sandbox and MCP to read an
   item and record its result without a chat reply or fabricated human.
   Repeat with an agent whose chat binding requires HTTP identity. An empty
   queue poll creates no run or sandbox. A gated tool produces
   `approval_gate_targetless` with no tool effect, card or resume. Pause after
   enqueue and before worker start: the wake becomes terminal `blocked` /
   `hook_paused`, remains terminal through resume and scheduler ticks, and a
   later fresh wake can run. Wakes never produce a sweep slice or coverage
   notice. Replace the deployment after admission and demonstrate the
   explicitly accepted cron parity. Verify kill and operator-only run-query
   access independently of key rotation.
5. Run local and cluster evidence through production wiring, with no fakes
   inside the changed boundary. Execute the required live-provider and
   external-integration tiers for connector/session behavior. Migration proof
   accepts webhook rows, preserves scheduled/manual history and refuses a
   lossy downgrade. Operator queries, metrics and structured deploy receipts
   identify the wake correctly even though its queue label is CRON. With a
   nondefault delivery budget and an explicit lease override, observe the
   inserted row's lease and recovery timing to prove API/worker configuration
   parity; verify the unset-override case too.

Source verification used `git fetch origin next` and the pinned tree above.
All cited paths, explicit symbols and relative ADR links resolve.

## Consequences

1. Empty polls avoid sandbox and model work; accepted wakes use the existing
   silent targetless execution path. Overlapping wakes retain no follow-up;
   work missed by the current run waits for another connector poll.
2. A compromised key or a benign loop where agent writes create more queue items
   can sustain serialized model work on one hook. One open run bounds
   concurrency, not daily spend or side effects. Deployed tool policy and
   operator pause remain essential. Decision 4 requires bounded delivery-id
   dedupe against captured-signature replay; a current key holder can still
   authorize fresh deliveries.
3. Lost stream publication can stall until the lease expires and another wake
   arrives. This is an accepted recovery trade for a small implementation,
   with no exactly-once external processing or autonomous recovery claim.
4. Targetless cron version behavior is preserved deliberately. The run records
   admission provenance; it does not pin which deployment eventually boots.

## Alternatives considered

1. **Poll with cron.** Empty polls still start model work; retained for work
   whose timing, rather than an external queue, is the trigger.
2. **Use ordinary ingress or distribute the shared agent key.** This creates
   a reply-bound payload turn and wider signing authority than the connector
   needs. The separate wake route keeps declaration resolution out of that
   route's pre-authentication and protected branches.
3. **Add a targetless WEBHOOK wire shape and version rechecks.** This changes
   a frozen protocol and many consumers for behavior targetless cron already
   supplies. The stronger version contract is deferred explicitly.
4. **Add sealed credentials, overlap rotation, permanent deduplication or an
   outbox and reconciler.** These expand the implementation beyond the required
   scope. Existing secret references, immediate rotation and next-wake lease
   recovery meet this decision's contract. Decision 4 selects bounded Valkey
   delivery-id dedupe; permanent deduplication remains outside scope.
5. **Retain one follow-up or enqueue every notification.** A follow-up flag
   requires fresh admission from worker completion, expanding its authority
   and lifecycle contract; enqueueing each notification accumulates redundant
   turns. Choose overlap suppression with no retained notification: the
   connector's next poll recovers work arriving after the final queue read.
6. **Add key enable/disable state.** Rotation revokes the old credential and
   existing operator pause controls admission and queued starts. Another
   lifecycle duplicates those controls and is omitted.

# Automated remediation: first release contract

Realizing contract for [ADR 0203](../../adr/0203-automated-remediation-is-a-pre-qualified-action-the-platform-executes-and-verifies.md)
(automated remediation is a pre-qualified action the platform executes and
verifies), clauses REMEDIATION-1 to REMEDIATION-15 and rulings 1 to 10. Tracked
under epic [#4074](https://github.com/curie-eng/curie/issues/4074) by
[#4063](https://github.com/curie-eng/curie/issues/4063) to
[#4073](https://github.com/curie-eng/curie/issues/4073), with
[#4144](https://github.com/curie-eng/curie/issues/4144) (recurrence prevention
and alert rule tuning). It builds on the
[connector action executor contract](2026-10-06-connector-action-executor.md)
(ACTION-EXECUTOR-1 to 25, abbreviated AE-N here), which realizes REMEDIATION-5,
and on the protected hook contracts for
[ADR 0190](../../adr/0190-automated-hook-sources-cannot-widen-their-tool-access.md)
and [ADR 0191](../../adr/0191-protected-hook-delivery-authority.md)
([source policy](2026-10-02-protected-hook-source-policy.md),
[delivery lane](2026-10-02-protected-hook-lane.md)), tracked in
[#3603](https://github.com/curie-eng/curie/issues/3603). This specification
precedes implementation and claims no runtime behavior.

**Decision authority.** ADR 0203 was accepted on `main` on 2026-10-05 together
with ADR 0121 and ADR 0124, and forwarded to `next` by #4082 (merge
`fc1527985`). ADR 0190 and ADR 0191 are Accepted and stand unchanged for the
model turn (REMEDIATION-2). ADR 0117 is amended by ADR 0203 for the undo of a
policy-executed action (REMEDIATION-14), and ADR 0106 governs every approver.
This contract follows the accepted texts as written. Where they leave a choice
open, it takes the stricter reading and says so: the generation used for
admission must also be the current generation (AUTOMATED-REMEDIATION-4); at most
one automatic action leaves one turn (AUTOMATED-REMEDIATION-10); a breaker opens
on any outcome other than `verified`, whatever authorized the action
(AUTOMATED-REMEDIATION-11); recurrence prevention and alert rule tuning are never
automatic in the first release (AUTOMATED-REMEDIATION-24); and qualification
evidence must be observed in the installation that binds the action
(AUTOMATED-REMEDIATION-23). Likewise, a nomination with no bounded action for a person
to approve (malformed, an unknown action, a duplicate, or arguments outside the
action's schema) or one from a stopped agent ends refused instead of becoming an
approval request (AUTOMATED-REMEDIATION-7, -8), because ADR 0203 lets an injected
alert reach no tool the policy omits and a stopped agent asks nobody. The ADR does
not forbid any of these stricter readings; each narrows what runs without a
person and none widens it.

**Maintainer rulings, 2026-10-07.** The maintainer ruled on the four questions
this specification raised as open, and the affected criteria carry the rulings
as decided text:

* **Incident.** "Per incident per target" is a per-target incident window that
  opens with the first automatic action on a target and stays open for one hour
  after that action's verification finishes. A policy may only lengthen the
  window. It is never derived from the alert body (AUTOMATED-REMEDIATION-10).
* **Administrative principal.** Policy writes and breaker closes require an
  ADR 0106 operator principal in addition to the administrative credential, and
  that principal is recorded as the actor (AUTOMATED-REMEDIATION-3, -11, -14).
  This contract applies the ruling to qualification writes and verifier runs as
  well, because a qualification record enables automatic execution
  (AUTOMATED-REMEDIATION-22).
* **Alert rule tuning (#4144).** Tuning stops at the nomination and the approval
  card; an approved tuning request ends refused with no write. Automated
  rule-owner change requests need a separate Draft ADR later
  (AUTOMATED-REMEDIATION-25).
* **Predicate grammar.** One JSON pointer and a closed comparator set is not an
  expression language under ADR 0007 or ADR 0117 (AUTOMATED-REMEDIATION-17).

This contract also amends the executor contract in eight places, listed under
"Amendments to the executor contract", and records under "Needs an ADR" what it
does not decide. Where realizing a decision meets an existing invariant, the
conflict is stated, not designed around.

## Evidence labels

Every claim about existing code cites a file and symbol on `origin/next` and
carries one label. Code was inspected at `9313d753e`.

* **Inspected**: read in the source. Static; nothing executed.
* **Documented**: stated in an ADR, issue, specification or plan and not
  re-verified.

No claim here is Measured. The plan names the measurements that must precede
the dependent tasks.

## Existing behavior inspected

**Source policy store.** `apps/api/src/curie_api/models.py::HookSourcePolicy`
holds one row per `(agent_id, hook)` with a positive `generation`,
`operation_id`, `mode` (`protected` or `ordinary`), `tool_access`, runtime,
qualification and bundle references and `legacy_generation`; the row is
upserted in place. `apps/api/src/curie_api/models.py::HookSourceOperation` is
the attempt ledger whose rows a database trigger refuses to delete while the
agent exists, which is where generation history lives.
`apps/api/src/curie_api/hook_source_mutation.py::SourceMutationCoordinator._execute`
advances the generation above every recorded attempt, compares and swaps on
`expected_generation`, is idempotent on `operation_id`, and revokes the broker's
active generation (`packages/protected-hooks/src/curie_protected_hooks/source_fence.py::SourceFence.reserve_and_revoke`)
before persisting and publishing. (Inspected.)

**Who administers it.** The source policy routes in
`apps/api/src/curie_api/routers/hook_source_policy.py` are mounted under
`apps/api/src/curie_api/auth.py::require_api_key`, which accepts the platform
key or a live console session. `apps/api/src/curie_api/auth.py::require_platform_key`
accepts the platform key only and is not used there. Neither dependency yields a
principal identity. The hook source authenticates with a key derived per agent,
hook and source generation (`apps/api/src/curie_api/hook_source_signing.py::derive`),
which authorizes deliveries and support probes and nothing administrative.
(Inspected.)

**What a delivery records.** `apps/api/src/curie_api/routers/hooks.py::_protected_turn`
builds a `QueuedTurn` with `tool_access` `read-only` and an `event_id` derived
from the agent, hook and delivery id; `apps/api/src/curie_api/routers/hooks.py::_ingest_protected`
admits it to the protected broker. The source generation active at admission is
recorded only in Valkey (the admission intent and receipt, and `source_revision`
in the envelope built by `packages/protected-hooks/src/curie_protected_hooks/atomic_admission.py::AtomicAdmission._envelope`)
and in the HTTP receipt `apps/api/src/curie_api/routers/hooks.py::HookAccepted`.
No Postgres row records a `/hooks` delivery (other webhook receivers, such as
the review webhook, keep their own rows), and `QueuedTurn` carries no
generation. (Inspected.)

**No incident identity.** No platform record ties a delivery to an incident, an
alert id or an alert fingerprint. Deliveries are deduplicated by delivery id
(`packages/protected-hooks/src/curie_protected_hooks/admission_records.py::delivery_digest`).
The nearest stable identity is the hook partition
(`apps/api/src/curie_api/hook_partition.py::derive_partition`), a JSON pointer
into the untrusted body that selects a conversation. The SRE example's alert
identity rule is bundle policy (`examples/sre-bot/docs/ALERT-IDENTITY.md`).
(Inspected.)

**No protected worker yet.** The protected lane's worker modules (plan task 4 of
the [protected hooks plan](../plans/2026-10-02-protected-hooks.md)) do not exist
on `next`: nothing in the worker consumes `protected_envelope`. A protected turn
is admitted but not yet executed. PROTECTED-HOOK-LANE-7 plans a guarded kernel
facade and protected binding and substrate wrappers around an unchanged kernel;
it plans no runner client wrapper, which AUTOMATED-REMEDIATION-6 adds.
(Inspected; the planned modules are Documented.)

**Where a turn's final text is visible.** `packages/aci-protocol/src/aci_protocol/events.py::Final`
carries `text` and `status`. The kernel stores and renders it
(`apps/worker/src/curie_worker/kernel/attempt.py::_apply_frame`); the kernel is
off-limits to this work. The same function streams each `TextDelta` to the reply
before the `Final` arrives, and on a protected turn (no placeholder) the first
stream emit posts a message. Outside the kernel, the raw frames are visible only in
the frame stream returned by `apps/worker/src/curie_worker/runner_client.py::RunnerClient.start_turn`
(a `TurnStream`); a `apps/worker/src/curie_worker/reply_sink.py::ReplySink`
sees only rendered text with the receipt appended. No post-turn callback carries
the final text. The runner, not the worker, appends the conversation transcript
(`runner/src/curie_runner/history.py`, addressed by `CURIE_HISTORY_REF`). The one precedent for a structured block in model output is the
`curie-reply` fence parsed by `apps/worker/src/curie_worker/blocks.py::parse_reply`.
(Inspected.)

**Read-only turns.** `packages/aci-protocol/src/aci_protocol/events.py::ToolAccess`
has one value. `runner/src/curie_runner/tool_access.py::TurnToolAccess.refusal`
refuses any tool outside the read-only set and any approval request on a
`read-only` turn. (Inspected.)

**Approvals.** `apps/api/src/curie_api/models.py::Approval` has `granted_tool`
and `granted_arguments` (canonical arguments of a permission-gated call), a
`route`, a nullable `expires_at`, NOT NULL `conversation_id`, `reply_kind` and
`reply_channel` (the other reply columns are nullable), a `dedupe_key` unique
across all statuses, and a `purpose`
constrained by `approvals_purpose_ck` to `session` or `publication`.
`apps/api/src/curie_api/routers/approvals.py::create_approval` creates `session`
approvals under `require_api_key` (the platform key or a console session), and
`apps/api/src/curie_api/crud/approvals.py::create_approval` sets `expires_at` only
when an expiry is given, so there is no default expiry; `apps/api/src/curie_api/routers/approvals.py::resolve_approval`
authenticates an ADR 0106 principal, selects the route's approver set
(`apps/api/src/curie_api/slack_approvers.py::SlackApproverSetSelector`),
authorizes (`apps/api/src/curie_api/authorizer.py::authorize_approval`), claims
the decision by compare and set, and then enqueues a model wake
(`apps/api/src/curie_api/resumequeue.py::build_resume_turn`) unless the purpose
is `publication`. A publication approval wakes no model: the worker's
`apps/worker/src/curie_worker/publication_loop.py::PublicationReconciler.deliver_pending_card`
posts its card through an injected reply sink and records it in
`apps/worker/src/curie_worker/approval_cards.py::ApprovalCardStore`. Every approval
today follows a model turn: a session gate the model raised, or a publication the
platform raised from a turn's request to publish. (Inspected.)

**The ledger and executor on `next`.** `apps/api/src/curie_api/models.py::AgentAction`
carries `authority_kind`, `authority_ref`, `connector`, `connector_digest` and
`post_version` (migration 0085) beside `gate_approval_id`, `undone_at` and
`undone_by`; no code writes `connector` or `connector_digest` yet, so no record
can be undoable today. `apps/api/src/curie_api/models.py::ActionExecution`
constrains `kind` to `restore`, `forward` or `probe` and has `forward_arguments`,
`arguments_sha256`, `authority_kind` and `authority_ref`; nothing constrains
`authority_kind`, and only `undo_ruling` and `capability_probe` are written.
`apps/api/src/curie_api/routers/action_executions.py::dispatch_execution`
refuses any non-restore dispatch. No forward creation function exists
(AE-19 is open). `apps/worker/src/curie_worker/action_executor.py` holds pure
helpers only; the worker executor loop (AE task 11) does not exist and
`apps/worker/src/curie_worker/config.py::WorkerConfig` declares
`action_executor_enabled` without a reader. (Inspected.)

**Undo authorization.** `apps/api/src/curie_api/routers/actions.py::_authorize_undo`
reads only `gate_approval_id`: none means ungated, otherwise the principal must
belong to that approval route's approver set. It never reads `authority_kind`.
(Inspected.)

**Approver sets.** `apps/api/src/curie_api/approvers.py::ExplicitUsers` accepts
operator, console and chat principals;
`apps/api/src/curie_api/slack_approvers.py::SlackUserGroupMembers` accepts chat and
console principals and not an operator principal. (Inspected.)

**The executor's runner side.** The `/v1/execute` phases live in
`runner/src/curie_runner/executor.py`; executor mode loads no harness, so the
read-only tool set a turn builds is absent there and only a tool's
`readOnlyHint` from `list` is available. (Inspected.)

**Stops.** `apps/worker/src/curie_worker/killswitch.py::KillSwitch.is_killed`
reads the per-agent kill key. (Inspected.)

**Receipts and telemetry.** `apps/worker/src/curie_worker/receipt.py::render_receipt`
renders one line per action appended to the turn's reply and has no execution,
verification or actor field. `packages/telemetry/src/curie_telemetry/metrics.py::_METRICS`
declares every metric with closed attribute domains, and
`packages/telemetry/src/curie_telemetry/metrics.py::_HTTP_OPERATIONS` lists the
route labels; no action, execution or remediation metric exists. (Inspected.)

**Actor attribution.** `principal_kind` exists on approval audit rows only;
`apps/api/src/curie_api/models.py::ActionAuditEntry` records a free `actor`
string. (Inspected.)

## Scope of the first release

A hook that an administrator has made protected (ADR 0190, ADR 0191) may also
be bound to a remediation policy. The protected turn stays `read-only`. If its
final output carries a nomination block, the platform parses it after the turn,
admits at most one nomination for automatic execution against the policy, the
limits and an independent precondition read, executes it through the connector
action executor with no model, verifies recovery through a different connector,
and reports. Every other well-formed nomination becomes an argument-bound
approval request on the policy's route, executed without a model when approved.
Undo of a policy-executed action is an approval-gated decision. Recurrence
prevention and alert rule tuning are nomination kinds that always ask a person.

Automatic execution is available only on the cluster tier, because the executor
is (AE-16). Nothing here ships before the executor loop (AE task 11) and the
protected worker (#3603) exist.

## Activation

<!-- @spec AUTOMATED-REMEDIATION-1 -->
**AUTOMATED-REMEDIATION-1. Closed by default.** One chart value,
`remediation.enabled` (default off), and the matching compose value render
`CURIE_REMEDIATION_ENABLED` into the API and the worker. Enabling it requires
`actionExecutor.enabled`; a render with remediation on and the executor off
fails. With the setting off the API refuses every nomination submission with
`remediation_disabled`, creates no nomination, approval or execution, and keeps
the policy routes readable and writable so a policy can be staged before
activation. A policy applies only to a hook with an active protected source
policy (ADR 0203 "Relationship to existing decisions": a remediation policy
applies only to restricted hooks).

Acceptance: with the setting off, a well-formed submission for a protected event
returns `remediation_disabled` and creates no row; with it on, the same
submission creates one nomination row. A chart render with remediation on and
the executor off fails, and a render assertion proves the API and worker receive
the same value. Binding a policy to a hook whose source policy is ordinary or
absent is refused `hook_not_protected`.

## The policy store (#4063)

<!-- @spec AUTOMATED-REMEDIATION-2 -->
**AUTOMATED-REMEDIATION-2. Policy records and generations.** One additive,
hand-written migration (ADR 0117 found autogenerate unsafe against the shared
database; the revision number is the next free one on `next` at implementation
time, checked against `main`) adds:

* `remediation_policies`: primary key `(agent_id, hook)`, foreign key to the
  agent with cascade; `generation BIGINT > 0`; `operation_id UUID`; `armed
  BOOLEAN`; `active BOOLEAN`; `updated_at`. One row per bound hook. A removal
  writes a new generation with `active` false, `armed` false and no actions; the
  row keeps a positive generation, as the source policy's ordinary tombstone does.
* `remediation_policy_generations`: primary key `(agent_id, hook, generation)`;
  `operation_id`, `intent_sha256`, `document JSONB` (the whole policy below),
  `armed`, `active`, `bound_by` (the operator principal, AUTOMATED-REMEDIATION-3),
  `created_at`. Rows are immutable and never deleted while the agent exists,
  enforced by a trigger like the one on `hook_source_operations`, so an earlier
  generation can always be read back.

The policy document is closed (unknown keys refused) and holds:

* `route`: the approval route name, which must exist in the agent's
  `approval_routes` with an explicit approver set (`approvers.users`, an
  `apps/api/src/curie_api/approvers.py::ExplicitUsers` set, or `approvers.group`,
  an `apps/api/src/curie_api/slack_approvers.py::SlackUserGroupMembers` set). The
  channel-members fallback is refused, because anyone in an alert channel would
  approve. A user group set accepts chat and console principals but not an
  operator principal, so a route driven from the CLI needs `approvers.users`;
* `limits`: `per_policy_per_hour` (default and ceiling 3),
  `per_incident_per_target` (default and ceiling 1), optional
  `per_action_per_hour` (at most the policy ceiling; the policy limit already
  bounds every action), `incident_window_seconds` (AUTOMATED-REMEDIATION-10) and
  `approval_ttl_seconds` (default 14400, at most 86400; AUTOMATED-REMEDIATION-15);
* `actions`: a non-empty list, each with `name` (`[a-z0-9][a-z0-9_-]{0,62}`,
  unique), `kind` (`remediate`, `prevent` or `tune`, AUTOMATED-REMEDIATION-24),
  `connector` and `tool` (the forward verb), `arguments` (a closed argument
  schema: each key with a type and an allowed set or an absolute range),
  `target` (AUTOMATED-REMEDIATION-7), `reversibility` (`reversible` or
  `idempotent`, REMEDIATION-4 and REMEDIATION-11), `precondition` and `verifier`
  (each a declared read, AUTOMATED-REMEDIATION-17), `automatic` (boolean), and
  `qualification` (a reference, AUTOMATED-REMEDIATION-22). A `remediate` or
  `prevent` action must declare both a precondition and a verifier whether or
  not it is automatic, so every executed action has a verdict (REMEDIATION-8).
  A `tune` action declares evidence reads instead (AUTOMATED-REMEDIATION-25).
  Magnitude is bounded by absolute ranges only: a delta bound would need a
  trusted baseline, and the first release offers none (see "Out of scope").

Generations follow the source policy rule: a write advances the generation
above every recorded attempt, compares and swaps on `expected_generation`, is
idempotent on `operation_id` (a different intent under the same id is
`policy_operation_conflict`), and never reuses a number. Arming, disarming,
binding, tightening, widening and removal each create a generation.

Acceptance: real Postgres upgrade, downgrade and upgrade round trip; a write
with a stale `expected_generation` is refused `stale_policy_generation` and
changes nothing; a replayed operation returns the committed generation; deleting
a generation row while the agent exists fails; a limit above its ceiling, an
unknown key, a `remediate` or `prevent` action without a precondition or
verifier, a delta bound, and a route bound to the channel-members fallback are
each refused with a named code and create no generation; a removal leaves a
positive generation with `active` false.

<!-- @spec AUTOMATED-REMEDIATION-3 -->
**AUTOMATED-REMEDIATION-3. Administration, and the source cannot write it.**
Routes under `/agents/{agent_id}/hooks/{hook}/remediation-policy` (`GET`, `PUT`,
`DELETE`, and `POST .../arm`, `POST .../disarm`, `POST .../breakers/{breaker_id}/close`)
use the same dependency as the source policy routes,
`require_api_key` (REMEDIATION-13: "the same trust model as ADR 0190 source
policy"). Every write (`PUT`, `DELETE`, arm, disarm and breaker close) also
requires an ADR 0106 operator principal, verified as the approval resolve path
verifies one, and records that principal as the actor on the generation or the
breaker row (maintainer ruling, 2026-10-07); a write with the administrative
credential and no principal is refused `operator_principal_required`. Reads need
no principal. Responses carry `Cache-Control: no-store`. The hook ingress, its
scoped key, the support probe, the delivery body and the nomination submission
route have no write path to these tables; a request to the policy routes signed
with a hook key and no platform credential is refused `401`. Validation is
mirrored in a new `remediation-policy` verb group under `curie local` and
`curie cluster` (`show`, `apply`, `arm`, `disarm`, `close-breaker`), with
`--json`, ADR-0021 exit codes and `{"error","fix"}` errors, following the CLI
and API validation convention.

Acceptance: a policy write with the platform key and an operator principal
creates a generation naming that principal as `bound_by`; the same write without
a principal is refused `operator_principal_required` and creates nothing; the same
write carrying only a valid hook source signature returns `401` and creates
nothing; a delivery body or nomination containing a policy document changes no
policy row; every CLI verb emits one JSON object under `--json`, refusals
included, and the CLI refuses an over-ceiling limit with the API's reason.

<!-- @spec AUTOMATED-REMEDIATION-4 -->
**AUTOMATED-REMEDIATION-4. The generation active at admission, and now.**
REMEDIATION-3 evaluates a nomination against the policy generation active when
the delivery was admitted. Because no Postgres row records a protected delivery,
the remediation generation (or its absence) is added to the protected admission
intent and to the envelope beside `source_revision`, as
`remediation_generation`, read by the protected ingress under the same agent
gate that orders source policy writes. This is internal transport metadata
(ADR 0191: the envelope is "not an additional ACI field"), versioned with the
envelope schema and reviewed by its owner under #3603. A nomination is admitted
for automatic execution only when the admitted generation and the current
generation are the same and armed. Any difference, an absent admitted
generation, or an envelope without the field sends every nomination of that
turn to an approval request (option A behavior), recording both generations.

Acceptance: a nomination from a delivery admitted under generation N while N is
current and armed may be automatic; the same nomination after a write to N+1
(including a disarm) becomes an approval request naming N and N+1; a delivery
admitted before any policy existed yields approval requests only; an envelope
missing the field refuses automatic execution and does not refuse the turn.

## Nominations (#4064)

<!-- @spec AUTOMATED-REMEDIATION-5 -->
**AUTOMATED-REMEDIATION-5. The nomination block.** A nomination leaves the turn
only as one fenced block in the final output, opened by a line that is exactly
three backticks followed by `curie-remediation` and closed by a line of three
backticks, holding one JSON object:

```json
{"version": 1, "nominations": [
  {"action": "scale-out-api", "arguments": {"namespace": "app", "deployment": "api", "replicas": 4}, "reason": "free text, at most 500 characters"}
]}
```

`version` is the integer 1. `nominations` holds one to five entries, each with
exactly `action` (a policy action name), `arguments` (an object) and an optional
`reason`. Kind-specific fields for `tune` are in AUTOMATED-REMEDIATION-25. The
block is at most 16 KiB of UTF-8. A final output with more than one block, a
block that does not close, a duplicate key at any depth, a value JSON cannot
represent exactly (NaN, infinities), or any key outside the grammar makes the
whole block malformed. Two entries naming the same action and the same canonical
arguments are a duplicate. Canonical arguments are the executor's canonical form
(AE-7). The block is data: it is not a tool call, and the turn's tool access is
unchanged (REMEDIATION-12).

Acceptance: the shared nomination vector's valid cases parse to the expected
entries and its invalid cases (two blocks, unclosed, duplicate key, NaN, extra
key, six entries, over size, wrong version) are refused as wholes, through the
API's production parser and the worker's extractor.

<!-- @spec AUTOMATED-REMEDIATION-6 -->
**AUTOMATED-REMEDIATION-6. Capture after the turn, outside the kernel.** The
kernel streams each `TextDelta` to the reply before the `Final` arrives
(`apps/worker/src/curie_worker/kernel/attempt.py::_apply_frame`), so removing
the block from `Final` alone would still post it. The capture seam is therefore
the frame stream itself: a runner client wrapper (a subclass of
`apps/worker/src/curie_worker/runner_client.py::RunnerClient`, whose
`start_turn` returns the `TurnStream` the kernel iterates) that the protected
lane's composition passes to `apps/worker/src/curie_worker/kernel/core.py::Kernel`
as its `runner`, exactly as `apps/worker/src/curie_worker/run.py::build` passes
`RunnerClient` today. PROTECTED-HOOK-LANE-7 plans a kernel facade and binding and
substrate wrappers, not this wrapper; this work adds it to the protected lane's
composition, reviewed by the #3603 owner.

The kernel holds one runner for every turn, and `start_turn` receives an ACI
`Event`, which carries no event id, so the wrapper learns the turn's `event_id`
from a context variable owned by a new protected lane module (not the kernel's
own context variables). The protected lane's guarded consumer adapter, which
already resolves each entry's protected binding before delegating to the base
consumer (protected hooks plan, phase 3), sets it to the binding's `event_id`
before delegating, so every task the kernel creates for that turn inherits it.
The wrapper reads it in `start_turn`; when it is absent or names an event with no
binding, the wrapper still filters the block but submits nothing and logs
`nomination_unattributed` (fail closed). `start_turn` returns a `TurnStream`
subclass that wraps the parent stream, keeps its `turn_epoch` attribute and its
async context manager protocol (`async with turn: async for frame in turn`),
and filters frames in `__aiter__`. The wrapper:

* passes `TextDelta` text through a line filter that withholds everything from a
  line that is exactly the opening fence to the closing fence line, holding back
  a trailing partial line until it is complete so a fence split across deltas is
  still caught; a block that never closes is withheld to the end of the stream;
* when the stream ends with a `Final` whose `status` is `done`, extracts the block
  from `Final.text`, submits it with the turn's `event_id` to
  `POST /v1/internal/remediation/nominations`, and yields a `Final` whose `text`
  has the block removed; an unclosed block is removed from the yielded `Final`
  from its opening fence line to the end, in every status;
* submits nothing for any other status or when no block is present, and submits
  the raw text of a block it cannot extract so the API records it as malformed.

The stored transcript is written by the runner, not the worker
(`runner/src/curie_runner/history.py`, addressed by `CURIE_HISTORY_REF`), so the
wrapper cannot keep the block out of it. The first release does not try: the
block stays in the protected conversation's own history segment, which
PROTECTED-HOOK-LANE-7 keeps separate from any human session and which is
rehydrated only into later protected turns. A later turn that repeats an old
block produces a new submission under its own `event_id`, evaluated afresh
(duplicates dedupe as in AUTOMATED-REMEDIATION-15). Keeping it out would need a
runner change in a qualified protected artifact, which is out of scope.

The route requires the internal worker token and accepts only `event_id` and the
block. The API resolves `agent_id`, `hook`, the admitted generation, the thread
and the reply handle from the protected binding keyed by that `event_id`, never
from the request, and refuses an event with no binding `not_protected_event`.
The binding must therefore be retained until the turn's submission window closes
(the protected lane's binding retention, reviewed with task 5 of the plan). The
route is idempotent per `event_id`: the first accepted submission wins, a
byte-identical replay returns it, and a different one is refused
`nomination_conflict`, so a retried turn cannot add nominations. The worker never
reads the policy and never decides admission. No kernel, consumer, thread lock
or markers file changes, and no ACI frame or field is added.

Residual trust, named: any holder of the internal worker token can submit
nominations for a protected event. A nomination carries no authority beyond what
an injected alert already has (REMEDIATION-3), and admission is unchanged.

Acceptance (protected worker, real runner): a `done` turn with a block submits
it once; no streamed reply edit and not the final reply contains the fence,
including when the fence line is split across two deltas; an awaiting or failed
turn submits nothing; a second submission with different bytes for the same
event is refused; a submission for an ordinary event id is refused
`not_protected_event`; a request carrying an `agent_id` or `hook` field is
refused as malformed; the kernel package, `consumer.py`, `threadlock.py` and
`markers.py` are unchanged in the diff.

<!-- @spec AUTOMATED-REMEDIATION-7 -->
**AUTOMATED-REMEDIATION-7. Parsing and validation.** The API parses the
submission with one production parser and writes one `remediation_nominations`
row per entry (or one row for a malformed block) with: `id`, `agent_id`, `hook`,
`event_id`, `admitted_generation`, `current_generation`, `action`, `kind`,
`arguments` (canonical), `arguments_sha256`, `target`, `reason` (stored, never
used as evidence), `state` (AUTOMATED-REMEDIATION-8), `refusal_code`,
`approval_id`, `execution_id`, `verification_outcome`, `created_at`,
`decided_at`. Refusals that end a nomination without asking a person, because
there is no bounded action for a person to approve: `nomination_malformed`,
`unknown_action`, `nomination_duplicate`, `arguments_schema_mismatch` (a key
outside the action's schema or a wrong type). An entry whose values are of the
right type but outside the policy's allowed values, ranges or target list is not
refused: it becomes an approval request (REMEDIATION-7). The action's `target`
names the argument that identifies the target and lists its allowed values
literally; the row's `target` is the target key of AUTOMATED-REMEDIATION-10.

Acceptance: each refusal code is produced through the real route and writes a
row with that code and no approval or execution; an out-of-bounds value of the
right type produces an approval request carrying exactly those arguments.

## Admission (#4065, #4071)

<!-- @spec AUTOMATED-REMEDIATION-8 -->
**AUTOMATED-REMEDIATION-8. Admission order.** Each well-formed nomination is
evaluated in this order; the first failing check decides:

1. remediation enabled and the executor enabled; otherwise `remediation_disabled`;
2. the agent's kill switch, read through the same key the worker reads; a killed
   agent or an unreadable switch ends every nomination `agent_stopped` with no
   approval request (a stopped agent asks nobody);
3. the policy generation rule of AUTOMATED-REMEDIATION-4;
4. the policy is armed; a disarmed policy sends the nomination to approval;
5. the action's `kind` is `remediate` and `automatic` is true;
6. the action's qualification record is present and valid for the connector
   digest now in force (AUTOMATED-REMEDIATION-22);
7. verifier independence against the in-force version (AUTOMATED-REMEDIATION-17),
   otherwise `verifier_not_independent`;
8. the arguments and target are within bounds;
9. reversibility: `reversible` requires the connector's capability row to record
   the `restore` and `observe_version` pair at that digest (AE-13) and key custody
   (AE-16); `idempotent` is admitted under REMEDIATION-11 only;
10. no open breaker for the action's connector, tool and target
    (AUTOMATED-REMEDIATION-11);
11. the limits and the one-automatic-action-per-turn rule
    (AUTOMATED-REMEDIATION-10), reserved transactionally;
12. the precondition read holds (AUTOMATED-REMEDIATION-9).

A nomination failing any of checks 3 to 12 becomes an approval request
(AUTOMATED-REMEDIATION-15) whose card names the failed check. Because check 12
is asynchronous, the transition from `precondition_pending` to `admitted`
re-runs checks 2 to 11 under the AUTOMATED-REMEDIATION-10 lock, in the same
transaction that creates the execution; any check failing then sends the
nomination to approval (or ends it `agent_stopped`) and releases the
reservation. States: `received`, `refused`, `precondition_pending`, `admitted`,
`approval_requested`, `approved`, `rejected`, `expired`, `executing`,
`verifying`, `finished`. An unreadable policy, breaker, limit or capability row
fails closed to the approval path, never to execution.

Acceptance: a table-driven test drives each check through its real producer and
asserts the resulting state and code; with every check passing, one execution is
created; injecting a database error at each read yields an approval request or
`agent_stopped`, never an execution; disarming, opening a breaker or killing the
agent while a nomination is `precondition_pending` makes the transition create
no execution.

<!-- @spec AUTOMATED-REMEDIATION-9 -->
**AUTOMATED-REMEDIATION-9. The precondition read.** The condition an action
fixes is confirmed by the action's declared precondition read, never taken from
the alert body or the nomination's `reason`. The read is a `read` execution
(AUTOMATED-REMEDIATION-12) of the declared connector, read tool and arguments,
whose arguments may reference the nomination's target and nothing else of it,
evaluated by the declared predicate (AUTOMATED-REMEDIATION-17) with one sample.
The predicate holding moves the nomination to the re-check of
AUTOMATED-REMEDIATION-8; not holding sends it to approval with
`precondition_not_met`; a read that fails or times out sends it to approval with
`precondition_unavailable`. The limit reservation of check 11 is released when
the nomination does not execute.

Acceptance: with the reference fixtures, a precondition read that observes the
condition admits; one that observes it absent produces an approval request and
no execution, though the alert body claimed it; an unreachable read connector
produces `precondition_unavailable`; the reservation is released in both cases.

<!-- @spec AUTOMATED-REMEDIATION-10 -->
**AUTOMATED-REMEDIATION-10. Limits.** Defaults are ruling 9's: at most one
automatic action per incident per target and at most three automatic actions per
policy per rolling hour; a policy may only tighten them. Further:

* at most one automatic action leaves one turn; every later admissible nomination
  of the same turn becomes an approval request (ADR 0203 option D: "once per
  turn");
* at most one automatic action per target is live (not finished verifying) at a
  time, across every hook and policy of the agent;
* per-action hourly limits apply when declared; the policy limit bounds every
  action otherwise, and the incident window bounds every target.

The target key is the action's connector plus the canonical JSON of the target
argument's value, which must be a literal member of the action's allowed target
list (a namespace, a label selector string or a resource name exactly as the
policy lists it); a value that is not a literal member is out of bounds and goes
to approval. The incident window, the one-live rule and the breaker are looked up
per agent and target key, never per policy or hook, so a second hook cannot act
on a target inside another hook's incident window. Counts and reservations are
taken under one transaction-scoped advisory lock keyed by the agent, and the
reservation is written in the same transaction, so two concurrent admissions
cannot both take the last slot, whichever hook they came from.

Incident (maintainer ruling, 2026-10-07): an incident on a target opens with the
first action executed on it under this contract, automatic or approved, and stays
open for `incident_window_seconds` (default 3600; a policy may only lengthen it)
after that action's verification finished; a later executed action on the target
extends it. While it is open, a further automatic action on the target goes to
the approval path with `incident_limit`. The incident is never derived from the
alert body, a source fingerprint or the nomination; a policy declaring a window
below the default is refused at write.

Acceptance: a fourth admissible nomination within an hour becomes an approval
request with `policy_rate_limit`; a second automatic action on one target within
the incident window becomes an approval request with `incident_limit`, also when
the second nomination comes from another hook of the same agent; two nominations
in one turn yield one execution and one approval request; two concurrent
admissions from two hooks racing for one target yield exactly one execution (real
Postgres); a target value equivalent to but not literally in the allowed list
goes to approval; a policy that declares a looser limit is refused at write.

<!-- @spec AUTOMATED-REMEDIATION-11 -->
**AUTOMATED-REMEDIATION-11. Breaker, disarm and kill switch fail closed.** A
`remediation_breakers` row keyed by `(agent_id, connector, tool, target key)`
opens on any verification outcome other than `verified`
(AUTOMATED-REMEDIATION-18), whether the action ran under the policy or under an
approval, and on an execution that ended `failed` or `indeterminate`. An open
breaker sends every later nomination for that action and target, from any hook
of the agent, to approval. Only the policy's administrative route closes it
(`POST .../breakers/{breaker_id}/close`), which requires an operator principal
and records it as the closing actor with a reason; no nomination, delivery,
verifier or approval closes it. Disarm writes a new generation with `armed`
false and takes effect for every nomination evaluated after it, including those
from deliveries admitted earlier (AUTOMATED-REMEDIATION-4).

Between admission and dispatch, a policy-authorized forward execution is
re-validated when it is claimed: the nomination's generation is still current and
armed and no breaker is open for its target key; otherwise the execution ends
`refused` with `policy_changed` before any sandbox claim and the nomination goes
to approval. The check is a remediation authority hook that the claim route calls
for `authority_kind` `policy` only, owned by the remediation layer; this is
executor amendment E8. The kill switch is read at admission (check 2) and again by the
executor at claim and before dispatch (AE-21), so a kill between admission and
dispatch refuses `agent_stopped` with no write call.

Acceptance: a `not-recovered` outcome opens a breaker and the next nomination for
that action and target becomes an approval request; closing the breaker through
the hook source key, the worker token or an approval is refused; closing it with
the platform key and an operator principal re-allows automatic admission, while
the platform key alone is refused `operator_principal_required`; disarming after
an execution is created and before it is claimed yields `policy_changed` and no
write; killing the agent after admission yields `agent_stopped` with no write
observed at the connector.

## Execution and the ledger (#4067 task 12, #4068, #4073)

<!-- @spec AUTOMATED-REMEDIATION-12 -->
**AUTOMATED-REMEDIATION-12. Read executions.** Precondition and verifier reads
run through the executor, in a sandbox under the read connector's own binding,
never as a direct client in the worker or the API (ADR 0121 decision 2's
reachability argument applied to reads; see "Amendments to the executor
contract"). Concretely:

* `action_executions.kind` gains `read` (the check constraint is replaced in the
  same migration), with `authority_kind` `policy`, `approval` or
  `qualification` and `authority_ref` naming the nomination or qualification
  verifier run;
* the runner's `/v1/execute` (phases in `runner/src/curie_runner/executor.py`)
  gains a `read` phase: after `list`, one or more `read` requests of the same tool
  with the same canonical arguments, each calling `tools/call` once. Executor mode
  loads no harness, so the runner's read-only set does not exist there. The read
  is authorized by the policy generation that declares it (and the
  qualification record), never by its annotation; as an additional fail-closed
  refusal, a tool not advertised with `readOnlyHint: true` in that sandbox's own
  `list` is refused `tool_not_read_only` without dialing (executor amendment
  E4). No grant is sent. Residual trust, named
  as ADR 0191 names it: `readOnlyHint` is not proof of no effects, so the
  qualification's worst case statement covers the read tools
  (AUTOMATED-REMEDIATION-22);
* the request carries the predicate's JSON pointer; the runner returns only the
  value at that pointer in the result's structured content (a JSON scalar of at
  most 256 characters) or `pointer_absent`, never the whole result;
* the worker reports each sample's scalar to
  `POST /action-executions/{id}/samples` (worker token, fenced like the other
  transitions); the API evaluates the predicate;
* lifecycle: a read execution never enters `dispatched`, because no sample can
  write. It runs in `claimed`, renewing its lease at each sample; it ends
  `confirmed` when sampling ends, or `refused` with `runner_unavailable` on lease
  expiry or any refusal, which the verifier treats as unsuccessful samples. It is
  never re-queued;
* a read execution holds its sandbox for at most its deadline and at most 60
  samples (AUTOMATED-REMEDIATION-17 refuses a declaration whose deadline divided
  by its interval exceeds 60), then releases it on every path;
* the sandbox carries only the read connector's credentials, which requires the
  executor's per-claim stripped template ([#4204](https://github.com/curie-eng/curie/issues/4204),
  unmerged); without it the pool template would put the acting connector's
  credential in the verifier's sandbox and defeat REMEDIATION-15.

Acceptance: against a read fixture, one `read` returns the pointed scalar and the
API records it; a tool without `readOnlyHint`, an unadvertised tool and a write
tool are refused `tool_not_read_only` with no call observed; the API never
receives more than the scalar (log and payload capture); the sandbox is released
after the last sample and after a worker crash (sweeper); a cluster exec check
finds no acting connector credential in a verifier read sandbox; the vector fails
on a one-sided field change.

<!-- @spec AUTOMATED-REMEDIATION-13 -->
**AUTOMATED-REMEDIATION-13. Forward execution with an authority.** An admitted
nomination, or an approved remediation approval, creates one `forward`
execution through the AE-19 creation function: connector, tool and canonical
arguments come from the nomination row, never from a caller; `authority_kind` is
`policy` with `authority_ref` `policy:<agent_id>:<hook>:<generation>:<nomination
id>`, or `approval` with the approval id; the idempotency key is per authority,
`remediation:<nomination id>:policy` or
`remediation:<nomination id>:approval:<approval id>`, so each authority yields at
most one execution and a replay of either adopts its own. (Refinement: a single
`remediation:<nomination id>` key would make the approval that follows a
`not_reversible_now` refusal adopt the refused policy execution, contradicting
the rule below. A nomination has at most one approval, because the approval's
own `dedupe_key` of AUTOMATED-REMEDIATION-15 is unique across all statuses; an
approval id other than the one recorded on the nomination authorizes nothing.)
At dispatch the API creates exactly one
`agent_actions` row (AE-19) carrying those authority fields, the connector and
digest, `gate_approval_id` set to the approval id for an approval authority, and
the new columns `delivery_event_id` and `nomination_id`. A forward execution of a
`reversible` action whose capability or custody no longer holds at dispatch is
refused `not_reversible_now` and its nomination goes to approval; this applies
to a `policy` authority only, since an approver accepted the action as it stands.
The kill switch
and digest checks of AE-14 and AE-21 apply unchanged.

Acceptance (cluster, reference connector): one admitted nomination yields one
execution, one ledger row with `authority_kind` `policy` and the generation in
`authority_ref`, and one write call; a replayed admission creates nothing; an
approval authority yields `gate_approval_id` equal to the approval id; arguments
differing from the nomination are refused `arguments_mismatch`; a policy
execution refused `not_reversible_now` followed by the nomination's approval
yields a second, approval execution that dispatches, and a replay of either
authority adopts its own execution.

<!-- @spec AUTOMATED-REMEDIATION-14 -->
**AUTOMATED-REMEDIATION-14. Authority, verification and actor on the record.**
Additive columns on `agent_actions`: `delivery_event_id`, `nomination_id`,
`verification_outcome` (null, `verified`, `not-recovered`,
`verifier-unavailable`, `superseded`), `verified_at`, and `actor_kind`
(`model_turn`, `policy`, `approval`, `undo_ruling`); and on
`action_audit_entries`: `actor_kind`. A check constraint closes
`authority_kind` on both tables to `undo_ruling`, `capability_probe`, `policy`,
`approval`, `qualification` and null (null only for rows a model turn recorded).
Every executed remediation produces exactly one ledger record whose authority is
a policy generation or an approval (REMEDIATION-6). The record and its audit rows
name: the policy and generation, the operator principal that bound that
generation (maintainer ruling, 2026-10-07), the source delivery's `event_id`, the
precondition and verifier read execution ids, and the outcome. A policy actor is
represented as `actor_kind` `policy` with the policy reference as `actor`, never
as an empty human field (#4073, extending #3653).

Acceptance: real Postgres round trip; an unknown `authority_kind` violates the
check; a policy-executed record exposes every field above through
`GET /actions/{id}` and the audit route; a record written by a model turn keeps
`actor_kind` `model_turn` and null authority; existing rows survive unchanged.

## Approval requests (#4069)

<!-- @spec AUTOMATED-REMEDIATION-15 -->
**AUTOMATED-REMEDIATION-15. Argument-bound approval requests.** A nomination that
is well formed but not admitted creates one `Approval` with `purpose`
`remediation` (the `approvals_purpose_ck` constraint gains the value), `route`
the policy's route, `granted_tool` `mcp__<connector>__<tool>`,
`granted_arguments` the nomination's canonical arguments, `dedupe_key`
`remediation:<nomination id>`, `conversation_id`, `reply_kind` and
`reply_channel` (and the nullable reply columns, null for a protected turn)
copied from the protected delivery's `QueuedTurn`, `author` the policy reference,
and an explicit expiry of the policy's `approval_ttl_seconds` (default 14400).
The create path sets no expiry by itself (it is null unless given), so the
expiry is always passed. Deduplication is done on the nomination rows, not on
`Approval.dedupe_key`, which is unique across all statuses: while an approval
raised for the same agent, hook, action and `arguments_sha256` is pending, a
further identical nomination attaches to it (a count and the newest `event_id`),
raises no new card, and finishes with that approval's outcome without ever
executing separately; once that approval is resolved or expired, the next
identical nomination raises a new one. A recurring alert therefore produces one
open decision at a time.

The card is posted by a worker remediation loop through an injected reply sink
and recorded in `ApprovalCardStore`, as `PublicationReconciler.deliver_pending_card`
does; it uses the existing approval action ids, so the dispatcher is unchanged.
It is rendered from the nomination row and the policy generation, never from the
approval row: the platform-rendered call (action, target and arguments), the
admission check that failed, the precondition read's observed value when one was
taken, and the model's `reason` as escaped plain text (no mentions, links or
markup) labeled as unverified model text. It never shows the alert body.

Acceptance: an out-of-bounds nomination produces one pending approval whose
`granted_arguments` equal the canonical arguments and whose `expires_at` is set;
a second identical nomination from a later delivery produces no second approval
and increments the count, and finishes with the first approval's outcome; after
that approval is rejected, a third identical nomination raises a new approval; a
`reason` containing a mention, a link and markup renders inert; a card built from
a delivery whose body contains a marker string never contains that string; a
`session` approval keeps today's behavior.

<!-- @spec AUTOMATED-REMEDIATION-16 -->
**AUTOMATED-REMEDIATION-16. Approval executes exactly the bound call, without a
model.** Resolution keeps `resolve_approval`'s authentication, approver set
selection and compare and set. For `purpose` `remediation` it enqueues no model
wake. On `approved` it builds the forward execution of AUTOMATED-REMEDIATION-13
from the nomination row (connector, tool and canonical arguments), and first
requires that the approval's `granted_tool` equals `mcp__<connector>__<tool>` of
that action and that the SHA-256 of the approval's canonical `granted_arguments`
equals the nomination's `arguments_sha256`; any difference refuses
`arguments_mismatch` and creates nothing. It also refuses `policy_changed` when
the current policy generation no longer has the action with the same connector
and tool, so a withdrawn action is not executed on an old card. On `rejected` or
`expired` it creates nothing and finishes the nomination and any nominations
attached to it. Resume reconciliation excludes this purpose as it excludes
`publication`. The approved call is verified like an automatic one
(AUTOMATED-REMEDIATION-18), and every such action has a verifier
(AUTOMATED-REMEDIATION-2). ADR 0035's tool-name grant is not used.

Acceptance: approving yields one execution whose arguments hash equals the
nomination's; no resume turn is enqueued (queue observed); rejecting or letting
the approval expire yields no execution and no write call; a principal outside
the route's approver set is refused `403` and nothing executes; editing the
approval row's `granted_arguments` in the database after the card is posted and
then approving is refused `arguments_mismatch` with no execution; approving after
a generation that removed the action is refused `policy_changed`.

## Verification (#4070)

<!-- @spec AUTOMATED-REMEDIATION-17 -->
**AUTOMATED-REMEDIATION-17. The verifier declaration.** A verifier (and a
precondition) declares: `connector`, `tool`, `arguments` (canonical, may
reference the target), `pointer` (an RFC 6901 JSON pointer), `comparator` (one
of `eq`, `ne`, `lt`, `le`, `gt`, `ge`, `in`, `absent`), `value` (a JSON scalar or,
for `in`, a list of at most 16 scalars), and for a verifier `settle_seconds` (at least `interval_seconds`, ADR 0203's minimum settle
interval), `deadline_seconds` (greater than settle, at most 3600, and at most 60
times the interval, matching the sample cap of AUTOMATED-REMEDIATION-12),
`interval_seconds` (at least 10) and `consecutive` (at least 1, default 1). The
ordering comparators (`lt`, `le`, `gt`, `ge`) apply only when both sides are
numeric, compared by numeric value. A side is numeric when it is a JSON number or
a string matching a strict decimal grammar: an optional sign, ASCII digits, an
optional fraction (a point and digits), an optional exponent, and nothing else
(no whitespace, hexadecimal, `NaN`, `Infinity` or `inf`). A Prometheus sample
value such as `"0.5"` therefore satisfies `gt` 0.4. A boolean is never numeric,
and an ordering comparator with a non-numeric side is unsatisfied. `eq`, `ne` and
`in` never read a string as a number: they compare JSON numbers numerically and
any other value by its type and exact value. There are no functions, arithmetic,
variables or nesting; the maintainer ruled on 2026-10-07 that this closed form is
not an expression language under ADR 0007 or ADR 0117, and anything richer needs
a new decision. The numeric string rule was confirmed by the maintainer on
2026-10-07. Independence
(REMEDIATION-15) is checked at policy write and again at admission against the
in-force version: the verifier's connector differs from the acting connector,
and the set of secret names the verifier connector's MCP headers expand is
disjoint from the acting connector's. Residual trust, named: disjoint names do not
prove distinct credentials (two names can hold one token, and a connector with no
credential is trivially disjoint), so the qualification's worst case statement
covers it (AUTOMATED-REMEDIATION-22). No model output, alert body or connector
write reply is an input.

Acceptance: a shared predicate vector of pointer, comparator and value cases is
evaluated identically by the API evaluator and the runner's pointer extraction;
a verifier naming the acting connector, or a connector sharing a credential name
with it, is refused `verifier_not_independent` at write and, after a bundle
version introduces the overlap, at admission; an unknown comparator, a nested
value, an `in` list of 17 entries, a settle below the interval and a deadline over
60 intervals are each refused at write.

<!-- @spec AUTOMATED-REMEDIATION-18 -->
**AUTOMATED-REMEDIATION-18. Running the verifier.** When a forward execution of a
remediation ends `confirmed`, the API creates one verifier `read` execution. The
worker samples at `interval_seconds` until `deadline_seconds` after dispatch.
Samples before `settle_seconds` are taken and recorded but never count. The
outcome is:

* `verified`: `consecutive` samples at or after settle satisfy the predicate
  before the deadline;
* `superseded`: before `verified`, another ledger record on the same target key
  is created, or, for an action whose connector advertises `observe_version`, a
  separate `observe` execution against the acting connector at each sample
  reports a version different from the action's recorded `post_version`. That
  execution is a `read`-kind execution whose phase is `observe` (sequence `list`
  then repeated `observe`), with the same `authority_kind`, lease and 60-sample
  rules as the verifier read it accompanies. This is
  attribution, not a recovery verdict, so the acting connector may report it; it
  is never read as success;
* `not-recovered`: the deadline passes with at least one successful sample after
  settle and no `verified`;
* `verifier-unavailable`: the deadline passes with no successful sample after
  settle, or the read execution is refused.

Reads never dispatch, so the kill switch's dispatch check does not stop them: a
verifier keeps sampling after the agent is killed, up to its deadline, and the
receipt for a killed agent never says that verification stopped.

An execution that ends `failed`, `indeterminate` or `refused` after admission
gets no verifier and finishes `not-recovered` for reporting, with the execution
code. The outcome is written on the nomination and the ledger record once; a
second outcome is refused.

Acceptance (cluster, reference connector and read fixture): a recovered target
is `verified` only after settle; a target healthy before settle and unhealthy
after is `not-recovered`; a read connector made unreachable yields
`verifier-unavailable`; an operator change to the target during the window
yields `superseded`; none of these reads the model's reply or the acting
connector's success reply as evidence.

<!-- @spec AUTOMATED-REMEDIATION-19 -->
**AUTOMATED-REMEDIATION-19. Not verified: report, escalate, never undo
automatically.** Every outcome other than `verified` posts a failure report to
the policy's route and the delivery's thread, opens the breaker
(AUTOMATED-REMEDIATION-11) and, for a `reversible` action whose record is
undoable, offers undo as an approval-gated decision: the API creates an undo
approval request on the policy's route bound to the restore of that record, and
an approval drives the existing undo ruling path (AE-3) under the approving
principal. No code path calls the undo ruling for a policy-executed record
without an approving principal (REMEDIATION-14). `_authorize_undo` gains
authority awareness: a record with `authority_kind` `policy` requires a principal
in the policy route's approver set, and a record with `authority_kind` `approval`
requires one in the approval route's set (through `gate_approval_id`), replacing
AE-19's interim `refused_authority_unresolved`. This authority-aware undo
authorization is the part AE-19 assigns to
[#4068](https://github.com/curie-eng/curie/issues/4068). A REMEDIATION-11 action escalates
immediately on any outcome other than `verified` and offers no undo.

Acceptance: a `not-recovered` outcome produces a report and an undo approval and
no restore execution; approving it produces one restore execution under the
approving principal; an undo request for a policy record by a principal outside
the policy route is refused `refused_unauthorized`; an ungated actor can no
longer undo a policy record (ADR 0117 decision 3 no longer applies to it).

## Receipts and telemetry (#4072)

<!-- @spec AUTOMATED-REMEDIATION-20 -->
**AUTOMATED-REMEDIATION-20. Receipts.** The worker remediation loop posts, in the
delivery's thread, one message per nomination decision and one per verification
outcome, as separate messages after the investigation's reply (ADR 0203 option
D's stated cost). Each names the stage (`nominated`, `refused`,
`approval_requested`, `executed`, `verified`, `not-recovered`,
`verifier-unavailable`, `superseded`, `undo_requested`, `undone`, `escalated`),
the action, target key, authority and code, and never any other argument value,
an envelope, a read result, the model's `reason` or the alert body. The CLI (`curie local|cluster
remediation list` and `show <nomination id>`) is the operator receipt with the
same fields under `--json`. The turn receipt of ADR 0117 is unchanged.

Acceptance: each stage, driven through its real producer, produces exactly one
thread message naming it; a refusal never produces a "changed" line; a capture of
every thread message over the campaign contains none of the fixture's non-target
argument values, sampled values or alert body marker; the CLI
`show` output matches the API row for every terminal state.

<!-- @spec AUTOMATED-REMEDIATION-21 -->
**AUTOMATED-REMEDIATION-21. Telemetry.** One counter
`curie.remediation.lifecycle` with closed attributes `stage` (the list above),
`kind` (`remediate`, `prevent`, `tune`), `authority` (`policy`, `approval`,
`none`) and `code` (the closed refusal and outcome codes), declared in
`_METRICS` so it appears in `declared_metric_manifest`; the new routes join
`_HTTP_OPERATIONS`. Spans carry the same attributes and the nomination id. No
metric, span or log carries arguments, read values, the reason text or the
alert body.

Acceptance: the metric manifest test includes the counter with its bounded
domains; a log and span capture over a full automatic remediation and a full
approval contains none of the fixture's argument values or sampled values.

## Qualification (#4066)

<!-- @spec AUTOMATED-REMEDIATION-22 -->
**AUTOMATED-REMEDIATION-22. Qualification records.** A
`remediation_qualifications` row records, for one `(agent_id, connector, tool,
connector_digest, verifier declaration digest)`, the evidence ruling 5 requires,
as references to rows in this installation that the API checks when the record
is written:

* reversible: a `confirmed` restore execution of a record produced by this tool
  at this digest, and a `refused` restore with `version_conflict` and its audit
  row naming both versions (AE-15);
* idempotent: two `confirmed` forward executions with the same canonical
  arguments whose second left the same `post_version` (or, for a connector
  without versions, whose verifier samples are equal), so the repeat had no
  additional effect;
* every action: one verifier evaluation ending `verified` and one ending
  `not-recovered` under the same verifier declaration;
* every action: a worst case statement (text, at most 2000 characters) and the
  policy bounds it was evaluated against, covering the read tools' residual trust
  (AUTOMATED-REMEDIATION-12) and the credential distinctness that secret names
  cannot prove (AUTOMATED-REMEDIATION-17).

The record is written by `PUT /agents/{agent_id}/remediation-qualifications/{id}`
and verifier evaluations are started by
`POST /agents/{agent_id}/remediation-qualifications/{id}/verifier-runs`; both use
the administrative dependency and require an operator principal recorded as the
actor (the 2026-10-07 principal ruling applies, because a record enables
automatic execution). The verifier-run route accepts no tool or argument fields
and can only start `read` executions of the declared verifier with
`authority_kind` `qualification`, against a target that is a literal member of
the action's allowed list. Forward evidence is produced only through ordinary
remediation approvals: a drill is a protected delivery to the bound hook (in the
binding installation, against a disposable target inside the policy's bounds)
whose turn nominates the action with `automatic` false, and whose approval
executes it. No route lets an administrator request a write directly. A record is
valid only for its digest: a connector upgrade invalidates it and admission check
6 sends the action to approval until it is requalified.

Acceptance: writing a record with a missing, wrong-state or other-digest evidence
reference is refused with a named code; a write or verifier run without an
operator principal is refused `operator_principal_required`; a complete record
makes check 6 pass; deploying a new connector digest makes the same nomination an
approval request with `qualification_stale`; the verifier-run route refuses a body
with tool or argument fields and a target outside the allowed list.

<!-- @spec AUTOMATED-REMEDIATION-23 -->
**AUTOMATED-REMEDIATION-23. Evidence is local and observed.** Qualification
evidence imported from another installation, a staging report, a fixture run or
a static review does not satisfy AUTOMATED-REMEDIATION-22. The process document
for adding an action (plan task 17) states the drill order: bind the action with
`automatic` false, run the qualification drills through approvals on a disposable
target within the policy's bounds, record the qualification, then write a
generation with `automatic` true. The `qualification_required` check runs at
policy write (plan task 3 owns the validator, task 15 adds the check); until it
lands, admission check 6 is what blocks automatic execution.

Acceptance: a policy write setting `automatic` true for an action without a valid
qualification record is refused `qualification_required`; the same write with a
valid record is accepted and the next in-bounds nomination is admitted.

## Nomination kinds (#4144)

<!-- @spec AUTOMATED-REMEDIATION-24 -->
**AUTOMATED-REMEDIATION-24. Kinds, declared by the administrator.** The policy
declares each action's `kind`; the model names only the action, so it cannot
choose a kind. `remediate` follows the whole admission order. `prevent`
(recurrence prevention: an action whose purpose is that the alert does not fire
again, such as a capacity or configuration change) carries a precondition and a
verifier like any action and is never automatic in the first release; its
nominations always become approval requests that are executed and verified as in
AUTOMATED-REMEDIATION-16 and -18. `tune` is AUTOMATED-REMEDIATION-25. A policy
write with `automatic` true on a `prevent` or `tune` action is refused
`kind_not_automatic`. This is within ADR 0203: the ADR never requires an action
to be automatic.

Acceptance: a `prevent` nomination that passes every bound produces an approval
request, never an execution, and its approved execution is verified; a policy
write with `automatic` true on `prevent` and on `tune` is each refused
`kind_not_automatic`.

<!-- @spec AUTOMATED-REMEDIATION-25 -->
**AUTOMATED-REMEDIATION-25. Alert rule tuning requests.** A `tune` action
declares the rule owner connector and tool that apply a rule change, the closed
set of rule identifiers it may change, and a closed change schema: `field` one of
`threshold`, `for_duration`, `group_by`, `dedupe`, `retire`, with per-field
allowed values or ranges, and for `retire` a required `duplicate_of` rule
identifier from the same set. A nomination carries exactly `action`, `rule`,
`field`, `value` and an optional `reason`. The platform renders the diff from
the structured change and the rule's current definition read through the
declared read; the model never supplies diff text. The evidence on the card
(fire count, duration distribution, overlap with another rule, how often the
alert resolved without action) comes only from reads the action declares; any
model-supplied figure is shown as unverified model text. Dedupe by the canonical
change (AUTOMATED-REMEDIATION-15) makes a recurring series produce one request.
Rejection or expiry leaves the rule untouched and records it. Approved execution
stops at the approval (maintainer ruling, 2026-10-07): an approved `tune` request
ends `refused` with `tune_execution_not_automated`, with no write call, and the
receipt says so. Automated rule-owner change requests (for example a change
request to the repository that holds the rules) need a separate Draft ADR before
any execution path is built.

Acceptance: a recorded alert series with a duplicate rule, replayed as
deliveries, produces exactly one tuning approval request whose card shows the
platform-rendered diff and the declared-read evidence; a nomination naming an
undeclared rule or field is refused `arguments_schema_mismatch`; approval
produces `tune_execution_not_automated` and no write; rejection changes nothing.

## Cross-image seams and frozen contracts

<!-- @spec AUTOMATED-REMEDIATION-26 -->
**AUTOMATED-REMEDIATION-26. Parity seams.** New shared vectors under
`tests/vectors`, each read by every consumer through its production code, and
registered in the AGENTS.md parity seam list in the same change:

* `remediation-nomination`: the fence grammar, size bounds and valid and invalid
  blocks; read by the protected worker's extractor and the API parser (different
  images);
* `remediation-predicate`: pointers, comparators, values and expected results;
  read by the runner's pointer extraction, the worker's sample report and the API
  evaluator;
* `runner-execute` (existing): the `read` phase, its request and response, its
  refusal codes and the new accepted sequences;
* `remediation-policy`: valid and invalid policy documents, read by the API
  validator and the CLI's mirrored validation;
* `remediation-codes`: the closed nomination states, refusal codes and
  verification outcomes, read by the API, the worker receipt renderer and the CLI.

Acceptance: changing a field name or code on one side alone fails that side's
vector test; the AGENTS.md entries name each vector.

<!-- @spec AUTOMATED-REMEDIATION-27 -->
**AUTOMATED-REMEDIATION-27. No frozen contract changes.** Nothing in
`packages/aci-protocol` or `packages/plugin-format` changes. The nomination rides
inside the free text of `Final.text`; `ToolAccess` keeps its one value; the
policy lives in the API, not in a bundle; the `read` phase is runner-private like
`/v1/execute`; `remediation_generation` is internal envelope metadata under
ADR 0191. The protected envelope and intent schemas in `packages/protected-hooks`
are internal and versioned, and their change is reviewed by the #3603 owner before
dependent work merges.

This would become a blocker, to stop and raise rather than design around, if a
reviewer requires: a nomination as an ACI frame, field or tool; a second
`ToolAccess` value; a policy or nomination kind declared in `plugin.json` or
`connectors.yaml`; or the remediation generation as a `QueuedTurn` field. Each is
a frozen contract change that lands as its own reviewed PR first.

Acceptance: the wire lock, the ACI schema compatibility test and the
plugin-format schema export are unchanged by every realizing PR.

## Amendments to the executor contract

This contract amends the [executor contract](2026-10-06-connector-action-executor.md)
as follows (E1 to E8); each amendment lands with the task that needs it and updates the
executor spec text in the same change.

* **E1, AE-1 and AE-2 (producers and authority).** The closed producer set gains
  the remediation creation functions (forward executions from admission and
  approval, read executions from admission, verification and the qualification
  verifier-run route). `authority_kind` gains `policy`, `approval` (already named
  by AE-2) and `qualification`, closed by a check constraint
  (AUTOMATED-REMEDIATION-14). The rule that no route accepts a tool name or
  arguments for execution stands: every remediation tool and argument comes from a
  policy generation or a nomination row.
* **E2, AE-2 (kind).** `kind` gains `read` (AUTOMATED-REMEDIATION-12).
* **E3, AE-6 (phases and sequences).** A `read` phase is added, with the sequence
  `list` then one or more `read`s of one tool and arguments. `observe` is also
  accepted, repeatedly and with no `call`, in an execution that only observes a
  forward action's target for the `superseded` check (AUTOMATED-REMEDIATION-18);
  AE-6's "observe only before a restore" otherwise stands, and the one-`call` rule
  is unchanged.
* **E4, AE-4 and AE-6 (what authorizes a read).** The authority for a
  platform-originated read is the policy generation that declares it (and, for
  automatic execution, the action's qualification record). `readOnlyHint` is
  never an authorization: ADR 0121 decision 5 calls it a runtime hint. In
  executor mode the read-only set is absent, so the runner adds one fail-closed
  refusal on top of that authority: a declared read whose tool is not advertised
  with `readOnlyHint: true` in the sandbox's own `list` is refused
  `tool_not_read_only`.
* **E5, AE-17 (lifecycle).** Read executions never enter `dispatched`, renew their
  lease per sample, and end `confirmed` or `refused` without re-queueing.
* **E6, AE-20 and AE-24 (codes).** Pre-dispatch codes gain `tool_not_read_only`,
  `not_reversible_now` and `policy_changed`; `pointer_absent` is a sample result,
  not a refusal. The `runner-execute` vector carries all of them.
* **E7, AE-5 (sandbox credentials).** Read executions require the per-claim
  stripped template proposed by #4204 (unmerged); this contract does not ship
  verification on the pool template.
* **E8, AE-17 and AE-21 (claim-time authority re-check).** AE-21 leaves limits,
  the breaker and disarm to admission. For a `policy`-authorized forward
  execution, the claim route additionally calls the remediation authority hook
  (AUTOMATED-REMEDIATION-11) and ends the execution `refused` with
  `policy_changed` when it fails, before any sandbox claim. Executions of any
  other authority are claimed exactly as AE-17 says.

## Needs an ADR

Judged against ADR 0121, ADR 0124 and ADR 0203, none of E1 to E8 is a new
architectural decision. Routing reads through executor sandboxes applies ADR 0121
decision 2's reachability argument to reads and adds no reach: a sandbox under the
read connector's binding reaches only what a model turn of the same agent already
reaches through that connector. Observing the target through the acting
connector for `superseded` is attribution, which REMEDIATION-15 does not govern,
and uses an existing read-only verb. The remaining items are contract mechanics
inside decisions already made.

Two things would need an ADR, and this contract does not decide them:

* **Automated execution of an alert rule change** through the rules' owner, which
  the 2026-10-07 ruling sends to a later Draft ADR (AUTOMATED-REMEDIATION-25).
* **Any platform read of a tenant connector outside an executor sandbox**, for
  example a direct client in the worker or the API to make verification cheaper.
  ADR 0121 rejected that reach for restores; adopting it for reads is a boundary
  change.

## Out of scope for the first release

* Composite or multi-step actions; one nomination is one write call.
* Automatic undo of any kind (REMEDIATION-14), and undo of a REMEDIATION-11
  action.
* Remediation on ordinary or unconfigured hooks, on human turns, and on the local
  or skill tier.
* A time-boxed standing approval from on-call (ADR 0203 alternatives).
* Triage capacity isolation from the incident's failure domain
  ([#4145](https://github.com/curie-eng/curie/issues/4145)); executor and verifier
  sandboxes share it, so during a sandbox incident they refuse
  `sandbox_unavailable` and the outcome is `verifier-unavailable` or an approval
  request, never a silent success.
* A console surface for policies or nominations; the CLI is the operator surface
  and no console action is added, so the console parity map gains no entry.
* Importing qualification evidence from another installation.
* Delta bounds on magnitude (for example a maximum replica change), which need a
  trusted baseline read; absolute ranges bound magnitude in the first release.
* Keeping a nomination block out of the runner-written protected transcript.
* Executing an approved alert rule tuning change, including automated change
  requests to the rules' owner; a separate Draft ADR decides it later.

## Acceptance commands

Each test and implementation unit cites its criterion ID. Run the touched
areas' suites from the repository root (`uv run pytest -q` with focused
selectors defined in each test-first commit),
`uv run python -m curie_api.export_openapi` with
`uv run pytest apps/api/tests/test_openapi_drift.py -q`, the CLI checks
(`cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`,
`cargo test`), chart render assertions, and the docs check
`scripts/check-docs.sh` (run with bash). Postgres and Valkey are disposable and
owned by the run. Runtime acceptance is per tier in the plan; a fake model, a
rendered chart or a stubbed connector never closes a runtime criterion.

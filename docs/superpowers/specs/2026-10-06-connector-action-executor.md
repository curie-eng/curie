# Connector action executor: first release contract

Realizing contract for [ADR 0121](../../adr/0121-a-restore-is-the-connectors-own-verb-run-under-the-same-pinned-connector.md)
(a restore is the connector's own verb, run under the same pinned connector),
[ADR 0124](../../adr/0124-a-snapshot-is-sealed-to-the-connector-that-wrote-it.md)
(a snapshot is sealed to the connector that wrote it) and clause REMEDIATION-5
of [ADR 0203](../../adr/0203-automated-remediation-is-a-pre-qualified-action-the-platform-executes-and-verifies.md),
which extends the executor from restores to forward actions. Tracked in
[#4067](https://github.com/curie-eng/curie/issues/4067) under epic
[#4074](https://github.com/curie-eng/curie/issues/4074); it closes the last
slice of [#1861](https://github.com/curie-eng/curie/issues/1861)
([#1867](https://github.com/curie-eng/curie/issues/1867)) and realizes the
answer to [#1873](https://github.com/curie-eng/curie/issues/1873). This
specification precedes implementation and claims no runtime behavior.

**Decision authority.** The three ADRs were accepted on `main` on 2026-10-05
(merge `bc153ac3e`) and forwarded to `next` by #4082 (merge `fc1527985`),
including the ADR 0121 decision 5 errata. This contract follows the accepted
texts as written. Two realization choices fill what the ADRs leave open:
ADR 0124 decision 4 requires "the version observed now" without naming how it
is observed, and this design observes it through a third connector verb,
`observe_version`, beside the two (forward write and `restore`) that ADR 0121's
consequences name; and the checkable capability rule of ADR 0121 decision 5 is
applied to the pair (`restore` together with `observe_version`), which is
stricter than `restore` alone, so a connector advertising only `restore` is
treated as restoring nothing (ACTION-EXECUTOR-8). Where realizing a decision meets an existing invariant, the
conflict is stated, not designed around.

## Evidence labels

Every claim about existing code cites a file and symbol on `origin/next` and
carries one label. Code was inspected at `f75f88bea`; the only difference to
`fc1527985` is ADR text.

* **Inspected**: read in the source. Static; nothing executed.
* **Measured**: observed by executing something. The only measurements here
  are existing suites:
  `uv run pytest apps/worker/tests/test_action_client.py runner/tests/test_redact.py -q`
  (171 passed) and `uv run pytest apps/api/tests/test_action_undo.py -q`
  (12 passed). They establish today's ledger, redaction and undo ruling
  baseline, nothing about the executor.
* **Documented**: stated in an ADR, issue or interface document and not
  re-verified. The ADR 0121 spike results (a 13 line streamable HTTP MCP client,
  the scale round trip) are documented, not re-run.

## Existing behavior inspected

**The ledger.** `apps/api/src/curie_api/models.py::AgentAction` stores one row
per side-effecting call with `arguments`, `result`, `prior_state`,
`post_state`, `target`, `gate_approval_id`, `status`, `undone_at` and
`undone_by`; `AgentAction.undoable` derives reversibility from a succeeded
status, a prior state, a post state, a target and no prior undo. No column
records a connector, an image digest, a version token or an authority other
than an approval. `agent_id` is nullable. (Inspected.)

**The ruling.** `apps/api/src/curie_api/routers/actions.py::undo_action`
authorizes first (`_authorize_undo`, ADR 0117 decision 3), then refuses on
already undone, unsuccessful, irreversible, unobserved (`observed_state`
absent), uncomparable and conflict (`observed_state != post_state`). Every
refusal goes through `_refuse`, which commits an audit row. On success
the former `claim_action_undo` helper in `apps/api/src/curie_api/crud/actions.py` (removed when task 4
made the ruling create an execution) set `undone_at` at
authorization time, an audit row records `{"restoring": prior_state}`, and
`apps/api/src/curie_api/schemas/actions.py::ActionUndoOut` returns the cleartext
`target` and `prior_state`. The conflict refusal stores both states in
`apps/api/src/curie_api/models.py::ActionAuditEntry` evidence. No completion
route exists. (Inspected; the refusal ordering is Measured by
`apps/api/tests/test_action_undo.py`.)

**Nobody drives the ruling.** No CLI verb, dispatcher handler or worker module
calls `/actions/{id}/undo`. It is named only by the generated UI types and as a
route label in `packages/telemetry/src/curie_telemetry/metrics.py`. The turn
receipt, `apps/worker/src/curie_worker/receipt.py`, renders no undo control by
design. There is no `actions` CLI verb group. (Inspected.)

**Recording.** `apps/worker/src/curie_worker/kernel/attempt.py::_record_action`
opens a record on the first `side_effect_flag` of a call and completes it on the
second, passing only `event_id`, `conversation_id`, `agent_id` and the gating
approval, and is deliberately not best effort.
`apps/worker/src/curie_worker/actions.py::_snapshot` reads `prior`, `post` and
`target` from the reply and records no state when the frame is marked
`redacted`. (Inspected; the redacted case is Measured by
`apps/worker/tests/test_action_client.py::test_a_redacted_snapshot_never_produces_an_undoable_action`.)

**Frames are scrubbed today.** `runner/src/curie_runner/redact.py::OutboundRedactor.push`
applies held secret literals and the shared pattern rules to every content
field of every outbound frame, including the whole `result` of a
`side_effect_flag`, and sets `redacted` when anything changed. ADR 0124
decision 7 requires the frames to stay verbatim for the envelope; ACTION-EXECUTOR-10
realizes it. (Inspected; Measured by `runner/tests/test_redact.py`.)

**readOnlyHint at boot.** As ADR 0121 decision 5 (as corrected) states,
`runner/src/curie_runner/mcp_tool_capability.py::_probe_server_once` lists every
connector's tools at runner boot and `runner/src/curie_runner/__main__.py::_readonly_tools`
unions the names annotated `readOnlyHint=true` into the read-only set. No
deploy-time restore capability check exists. The ADR and the code agree.
(Inspected.)

**An MCP client already exists outside the SDK.** The same module opens
standalone sessions (`runner/src/curie_runner/mcp_tool_capability.py::_server_streams`)
over streamable HTTP, SSE or stdio, with the connector's caller headers
expanded. It never calls `tools/call`. (Inspected.)

**Argument-bound one-shot grants exist at the connector edge.**
`apps/worker/src/curie_worker/connector_grant.py::mint` signs a `ccg` grant
over `agent`, `connector`, `tool`, canonical `args`, `exp` and `jti`;
`apps/worker/src/curie_worker/kernel/claim.py::_connector_tool_grant` mints one
for an approved connector call with `json.dumps(sort_keys=True,
separators=(",", ":"), ensure_ascii=False)`; the caller proxy
(`apps/worker/src/curie_connector_proxy/server.py::_grant_refused`) refuses a
call to a gated tool unless exactly one live grant matches the agent,
connector, tool and canonical arguments, and spends its `jti` once in Valkey,
failing closed without a store. The gated set is rendered per connector by
`apps/api/src/curie_api/bundles.py::gated_tools_for_connector`. The runner reads
the grant from `runner/src/curie_runner/connectors.py::_GRANT_ENV` and sends it
in the header named by `runner/src/curie_runner/connectors.py::GRANT_HEADER`.
The local tier runs no caller proxy. (Inspected.)

**Connector identity.** `packages/plugin-format/src/plugin_format/connector_lock.py::apply_lock`
renders every `build:` connector at its locked `image@sha256:` digest and
carries `image:` and `url:` connectors through unchanged, so an `image:`
connector may render a mutable tag. Hosted connectors are reconciled for the
agent's in-force version only, by `apps/worker/src/curie_worker/connector_loop.py::ConnectorReconcileLoop`
through `apps/worker/src/curie_worker/connector_k8s.py::KubernetesConnectorClient`,
whose `list_owned` also lists Secrets and may rewrite one. The worker Role in
`charts/curie/templates/worker.yaml` grants `create`, `list`, `patch` and
`delete` on Deployments and no `get`. (Inspected.)

**Connector secret custody.** A plain `secrets:` name is stored on the agent row
and read by `apps/worker/src/curie_worker/binding.py::BindingResolver.secrets_for`;
`apps/worker/src/curie_worker/binding.py::inject_connector_secrets` injects
those values into the sandbox boot env, withholding only
`apps/worker/src/curie_worker/binding.py::SANDBOX_WITHHELD_CONNECTOR_SECRETS`.
A second path, `charts/curie/templates/agent-connector-secrets.yaml`, renders
`agentSandbox.connectorSecrets` into a per-agent Secret that the runner sandbox
reads through a SandboxTemplate `secretKeyRef`, bypassing both API intake and
`inject_connector_secrets`; it withholds the same three ADR 0176 names. A
`packages/plugin-format/src/plugin_format/connectors.py::SecretRef` value is
never resolved by Curie (`ConnectorSpec.resolved_secrets`) and reaches only the
hosted connector. The local tier refuses `SecretRef` (`refuse_out_of_band_secrets`
in `cli/src/connector_build.rs`). (Inspected. Whether a named secret's value
transits the API on the cluster tier is measured in plan task 2.)

**Sandbox lifecycle.** `apps/worker/src/curie_worker/sandbox/substrate.py::SandboxSubstrate.claim`
and `SandboxSubstrate.release` are public and keyed by a thread key; per-claim
env becomes value-only claim env, except the tokens named in
`apps/worker/src/curie_worker/sandbox/claim_tokens.py::CLAIM_TOKEN_ENVS`, which
move to a Secret. The grant variable is not in that list, so a grant placed on
claim env is stored in plain text on the claim object. Live routes are offered
to `apps/worker/src/curie_worker/kernel/capacity.py::_reclaim_idle_route`
through `SandboxSubstrate.pressure_candidates`. `runner/src/curie_runner/__main__.py::_serve`
resolves the model credential before serving, and
`runner/src/curie_runner/server.py::create_app` requires a started session runner
and gates control routes through `_auth_middleware` over `_GATED_PATHS`.
(Inspected.)

**No in-repo reversible connector.** The `k8s-scale` connector the ADRs cite
lives only on the spike branch;
`examples/sre-bot/connectors/self-upgrade/server.py::_reply` reports `prior` as
null. (Inspected.)

**Precedent for a may-have-applied write.**
`apps/api/src/curie_api/models.py::ChannelCanvasEdit` commits an `attempted` row
before calling the provider and settles only on a definite answer. (Inspected.)

## Scope of the first release

The executor runs, without a model, in a sandbox under the target connector's
own binding: an authorized restore of a recorded action, a forward action whose
authority its owner verified, or a read-only capability probe. Each is recorded
as authorized, then confirmed, failed, indeterminate or refused. Restores are
supported on the cluster tier only (ACTION-EXECUTOR-16). Forward execution is
built and integration tested but has no producer until its authority owners
land (ACTION-EXECUTOR-19).

## Activation and authority

<!-- @spec ACTION-EXECUTOR-1 -->
**ACTION-EXECUTOR-1. Closed by default; three named producers.** One chart
value, `actionExecutor.enabled` (default off), and the matching compose value
render the same setting `CURIE_ACTION_EXECUTOR_ENABLED` into both the API and
the worker. With it off the worker claims nothing and the API refuses an undo
that would authorize a restore with `executor_disabled`, through `_refuse`, so
one audit row is written and no execution is created. Execution rows are created
only by:

1. the undo ruling (ACTION-EXECUTOR-3), `authority_kind = undo_ruling`;
2. the forward creation function (ACTION-EXECUTOR-19), `authority_kind` `policy`
   or `approval`; an API function, not an HTTP route. Its remediation caller
   (automated remediation amendment E1, AUTOMATED-REMEDIATION-13) builds the
   authority from an admitted or approved nomination row and the policy
   generation that declares its action, never from a request;
3. the capability probe route `POST /connector-capabilities/probes`
   (ACTION-EXECUTOR-13), worker API key only, whose body is exactly
   `{agent_id, connector, digest}`, `authority_kind = capability_probe`, and
   which can only produce a `tools/list`.

No route accepts a tool name or arguments for execution.

Acceptance: with the setting off, an undo of an undoable record returns
`executor_disabled`, writes one refusal audit row and no execution; with it
on, the same request creates exactly one execution. A probe body carrying a
`tool` or `arguments` key is rejected and creates no row; every executions
route rejects a body naming a tool or arguments. A render assertion proves the
API and worker receive the same value from one chart value.

<!-- @spec ACTION-EXECUTOR-2 -->
**ACTION-EXECUTOR-2. The execution record.** One additive, hand-written
migration (ADR 0117 found autogenerate unsafe against the shared database)
adds `action_executions`:

| Column | Type | Meaning |
| --- | --- | --- |
| `id` | UUID PK | Execution identity. |
| `kind` | text: `restore`, `forward`, `probe`; amendment E2 adds `read` (revision 0090, AUTOMATED-REMEDIATION-12) | What is executed. |
| `agent_id` | UUID FK agents, cascade, not null | Whose binding the call runs under. |
| `connector` | text | Connector name. |
| `tool` | text, null for `probe` | Upstream tool name; `restore` for a restore. |
| `subject_action_id` | UUID FK agent_actions, nullable | Restore: the action put back. Forward: the record created at dispatch. |
| `arguments_sha256` | text, nullable | SHA-256 of the canonical argument bytes (ACTION-EXECUTOR-7). |
| `forward_arguments` | JSONB, nullable | Forward and read: the canonical arguments the authority bound. |
| `connector_digest` | text | The `sha256:` digest the call must run against. |
| `authority_kind`, `authority_ref` | text | `undo_ruling` with the authorizing audit row id, `policy` with the generation reference, `approval` with the approval id, `capability_probe` with the reconcile pass id. Amendment E1 adds `qualification`; a check constraint closes `authority_kind` to those five (revision 0089, AUTOMATED-REMEDIATION-14). |
| `requested_by` | text, nullable | The ruling's actor; copied to `undone_by` on confirmation. |
| `idempotency_key` | text, unique per `agent_id` | Restore: `restore:<action id>:<authorizing audit row id>`. Forward: supplied by the authority owner. Probe: `probe:<agent>:<connector>:<digest>`. |
| `state` | text | ACTION-EXECUTOR-17. |
| `refusal_code`, `failure_code` | text, nullable | ACTION-EXECUTOR-20. |
| `attempt`, `lease_owner`, `lease_expires_at` | int, text, timestamptz | Claim fencing. |
| `dispatched_at`, `finished_at`, `created_at` | timestamptz | Lifecycle. |
| `outcome` | JSONB, nullable | Version strings, key identifier and codes only; never an envelope, a state or a result. |
| `not_before` | timestamptz, nullable | Amendment E9: handed out only once due; NULL is due (revision 0090). |
| `pointer` | text, nullable | Read only: the predicate's RFC 6901 pointer, bound with the tool and arguments (revision 0090). |
| `sample` | JSONB, nullable | Read only: the one `{sample, value}` reported, or the API's `skipped`; never the result (revision 0090). |

Constraints: check constraints on `kind` and `state`; a unique partial index on
`subject_action_id` for `kind = 'restore'` and `state <> 'refused'`; uniqueness
of `idempotency_key` within one `agent_id`, so one agent's key can never adopt
another agent's execution; and a composite foreign key from
(`subject_action_id`, `agent_id`) to the subject action's (`id`, `agent_id`), so
an execution always runs under the agent whose action it concerns.

Acceptance: real Postgres upgrade, downgrade and upgrade round trip; existing
`agent_actions` rows survive unchanged; a second non-refused restore for one
action violates the index; an unknown state violates the check; a replayed
creation with the same idempotency key adopts the existing row; the same key
under another agent creates a distinct row; an execution whose `agent_id`
differs from its subject action's agent violates the foreign key.

<!-- @spec ACTION-EXECUTOR-3 -->
**ACTION-EXECUTOR-3. A ruling becomes an execution request without a model.**
`undo_action` keeps its authorization step and its refusal ordering. Because
the platform now performs the observation itself through the pinned connector
(ACTION-EXECUTOR-15), the caller-supplied `observed_state` is no longer
accepted as evidence; the ruling instead applies the record checks of
ACTION-EXECUTOR-11 and refuses `refused_no_agent` when the record has no
agent. On success it writes, in one transaction, the execution row and the
`authorized` audit row, whose evidence names the execution id, the key
identifier and the recorded version only. It returns `202` with the execution
id and state, no longer returns `target` or `prior_state`, and no longer sets
`undone_at`. A ruling while a restore of the same action is live refuses
`refused_restore_in_flight`. OpenAPI and the UI types are regenerated in the
same change.

Acceptance: an authorized undo yields one `requested` execution and one audit
row, and the response body contains no `prior_state`; a concurrent second
undo is refused `refused_restore_in_flight`; every refusal writes its audit row
and creates no execution; an unauthorized actor learns no version.

<!-- @spec ACTION-EXECUTOR-4 -->
**ACTION-EXECUTOR-4. No model in the path.** The worker loop reads only the
execution row, the ledger row it names, the agent's resolved binding and the
connector Deployment state. It never reads conversation text, alert bodies,
model output or turn history. The executor sandbox carries no model
credential and no history, memory, state, progress or issue token, and the
runner in executor mode loads no harness or SDK session.

Acceptance: the executor sandbox's claim object and runner env contain none of
those names, asserted on the real claim and by the executor mode status body
(ACTION-EXECUTOR-6); a runner started in executor mode with a model credential
in its env refuses to boot.

## Where it runs

<!-- @spec ACTION-EXECUTOR-5 -->
**ACTION-EXECUTOR-5. A sandbox under the connector's own binding.** Per ADR 0121
decision 2, the connector's credential, allowlist and network policy must be
the forward call's. The executor claims through `SandboxSubstrate.claim` for
the agent's in-force deployment with `agent_name` set, so the claim selects the
pool and labels an ordinary turn of that agent would, thread key
`action-exec:<execution id>` and `fresh_only=True`. Its env is the binding's
boot env minus everything ACTION-EXECUTOR-4 excludes, minus every connector
secret except those the target connector's derived MCP entry headers expand,
plus the caller token and the runner-private `CURIE_RUNNER_MODE=execute`
(not a `BootEnv` field; it joins the non-boot allowlist beside
`CURIE_CONNECTOR_TOOL_GRANT`). The grant never rides claim env. Claim env
cannot remove what the pool template already carries: the chart renders every
`agentSandbox.connectorSecrets` name onto the runner as a `secretKeyRef` and
bakes `CURIE_CREDENTIALS` into every non-fake runner template (measurement M3).
The executor claim therefore runs from its own per-claim template, written by
the substrate the way `apps/worker/src/curie_worker/sandbox/claim_tokens.py::claim_template_spec` already
writes one for token-bearing claims: a copy of the agent's pool template with
`CURIE_CREDENTIALS` and the model env-key declaration removed and the connector
secret `secretKeyRef`s limited to the target connector's header set. Labels and
the pool source stay the agent's, so reach is unchanged. The template also drops every other model credential the runner's executor mode refuses (the SDK credential variables and the variable `CURIE_MODEL_ENV_KEY` names), from `env` and `envFrom`; a target connector secret that shares such a name is dropped too, which fails closed, and a `valueFrom` model env-key declaration refuses the claim. `resume` and `handoff` refuse an `action-exec:` route. Executor routes
are excluded from `SandboxSubstrate.pressure_candidates`, so idle reclamation
never selects one, and a quota rejection maps to `sandbox_unavailable`. The
sandbox is released after the outcome is reported and on every error path.
Amendment E9 (AUTOMATED-REMEDIATION-12): the claim route never hands out more
than `actionExecutor.maxConcurrentSandboxes` live (claimed or dispatched)
executions across the installation (default 2, at least 1, rendered into the
API and the worker), so ordinary turns keep sandbox quota; executor routes stay
out of the pressure path. A read execution claims its own sandbox under the
read connector's binding and releases it before its sample is reported; a live
loop's pass that claims nothing releases the sandbox of a read the API ended
after its holder crashed.

Acceptance (cluster): the executor sandbox reaches the agent's own connector
and is refused at the network layer toward another agent's connector; its
labels and pool source equal an ordinary turn sandbox's for the same agent, and
on an install with a real model credential (fake model off) the executor pod
spec lists no `CURIE_CREDENTIALS`, no model env-key declaration and no
connector secret outside the target's header set, while an ordinary turn's pod
still lists them; under quota
pressure an ordinary turn never reclaims a live executor sandbox and an
executor claim over quota refuses `sandbox_unavailable`; the claim object
carries no grant, envelope or secret value.

<!-- @spec ACTION-EXECUTOR-6 -->
**ACTION-EXECUTOR-6. The runner executor route.** In executor mode `_serve`
branches before harness and credential resolution into a separate
`create_executor_app` that reuses `_auth_middleware` (with `/v1/execute`
added to `_GATED_PATHS`) and serves `GET /healthz`, `GET /status`,
`GET /v1/status` and `POST /v1/execute`; any other control route returns `409`
naming the mode. The executor mode status body is
`{"mode": "execute", "status", "ready", "turn_active", "history_durable":
false}` plus the boot attestation, where `turn_active` is true while a phase is
open; it carries no capacity admission fields.

`/v1/execute` takes `{execution_id, phase, connector, tool, arguments, grant,
target}` with `phase` one of:

* `list`: `tools/list` only; returns names, annotations and the input schemas
  of `restore` and `observe_version`. No tool, arguments or grant.
* `observe`: calls the read verb `observe_version` with `{"target":
  <target>}` and returns its `version`. Read-only, ungated, no grant.
* `call`: issues exactly one `tools/call` of `tool` with the canonical
  `arguments` text and the grant header, after the preflight below.
* `read` (amendments E3 and E4, AUTOMATED-REMEDIATION-12): the request adds
  `pointer`, an RFC 6901 pointer, and no other phase carries one. After
  `list`, exactly one `tools/call` of a declared read tool with its canonical
  `arguments` text and no grant; a tool not advertised `readOnlyHint: true` in
  this sandbox's own `list` is refused `tool_not_read_only` without dialing.
  The answer is only `{phase, sample, value}`: the scalar at the pointer in
  the structured content, else in the strict JSON of a result's one and only
  text block within the result bound, else `result_unstructured`.

The runner derives the connector's MCP entry exactly as an ordinary boot does
and opens each session through the standalone client, promoted from the
private `_server_streams` to a public helper in the same module. Within one
sandbox the only accepted sequences are `list`, or `list` then `observe` then
`call` for a restore, or `list` then `call` for a forward action, or `list`
then one `read` (amendment E3), or `list` then one `observe` with no `call` in
an observe-only execution (amendment E3, AUTOMATED-REMEDIATION-18: a `read`
kind execution of the acting connector's `observe_version`, bound to the
action's recorded target with no pointer, whose version the worker posts
unjudged to `POST /action-executions/{id}/observation` for the verifier's
`superseded` check); anything else, including a second `call` or a
second `read`, returns `409` without dialing. Preflight before
`call`: the tool is advertised; for `restore`, ACTION-EXECUTOR-13's capability
rule holds; the argument text parses to an object whose canonical form is
byte-identical to the text received. The route emits no ACI frame and logs no
argument, envelope or result.

Acceptance: against the reference connector, one `call` request yields one
`tools/call` observed at the connector; a non-canonical argument text, an
unadvertised tool, an out-of-order phase and a second `call` each return their
refusal with no call observed; `/v1/event` returns `409` in executor mode.

## Exact arguments and the paired verbs

Each executor sandbox serves exactly one `call`. A `call` refused by a
preflight check still consumes the sequence: any later `call` in the same
sandbox is refused `phase_out_of_order` and dials nothing. A forward tool
call follows `list` then `call`; the `observe` phase is accepted only before a
restore. A `call` whose connector grant cannot be attached (for example a
connector the executor cannot reach by URL) is refused before dispatch. `list`
pagination and a `call` result are bounded (100 pages and 1 MiB), and exceeding
either refuses or fails the execution without retrying.

<!-- @spec ACTION-EXECUTOR-7 -->
**ACTION-EXECUTOR-7. Exact canonical arguments, bound at the edge.** Canonical
form is the proxy's: sorted keys, separators `,` and `:`, `ensure_ascii=False`.
The creator stores `arguments_sha256`; the worker recomputes it over the text
it sends and refuses `arguments_mismatch` on any difference. Per `call` the
worker mints one `ccg` grant with `connector_grant.mint` over exactly that
text, a fresh `jti` and an expiry no longer than the dispatch deadline. The
target tool must be in the connector proxy's rendered gated set, so the proxy
verifies the exact arguments and spends the grant once; otherwise the worker
refuses `tool_not_grant_bound` before dispatch. Because the local tier has no
caller proxy, every grant-bound execution there refuses, and that refusal is
what the local tier proves.

Acceptance: a shared vector feeds the same argument objects to the worker's
canonicalizer and the proxy's production parser and asserts identical bytes,
including non-ASCII strings and nested objects; at the real proxy, a replayed
grant, a grant for other arguments and a call without a grant are refused; a
forward request naming an ungated tool is refused before any sandbox claim.

Canonicalization refuses values JSON cannot represent exactly, including
NaN and infinities, on every side; the worker and the proxy share one
canonicalizer implementation rather than copies.

<!-- @spec ACTION-EXECUTOR-8 -->
**ACTION-EXECUTOR-8. The paired verbs are the deploy-time capability rule.**
A connector's `restore` tool is the executor's restore verb (ADR 0121 decision
1) only when the same connector also advertises `observe_version`, the read verb
ACTION-EXECUTOR-15 needs. The pair is what ADR 0121 decision 5's advertised list
is checked for:

* **Paired.** The connector advertises both `restore` and `observe_version`.
  The runner, from its own boot `tools/list`, adds `mcp__<connector>__restore`
  to the disallowed tools passed by
  `runner/src/curie_runner/adapter.py::build_options`, so `restore` is absent
  from the model catalogue. Once the capability probe records the pair for that
  digest (ACTION-EXECUTOR-13), the API adds `restore` to that connector's
  rendered gated set, so the proxy refuses it without a grant; the reconcile
  re-applies the connector with the new gated set. Until that re-render lands,
  every restore for the connector refuses `tool_not_grant_bound`, so the
  executor never calls an ungated `restore`. Whether the pair then counts as
  capable also needs the schema and annotation checks of ACTION-EXECUTOR-13.
* **Lone `restore`.** The connector advertises `restore` without
  `observe_version`. The tool stays an ordinary tool with today's behavior: in
  the model catalogue, gated only if the bundle's approval patterns gate it, and
  never called by the executor. The connector is treated as restoring nothing,
  so its actions are never undoable.

Hiding fails closed: when the runner's boot probe of a connector failed or was
incomplete, it hides `mcp__<connector>__restore` for that connector too, since it
cannot tell a paired `restore` from a lone one, and a paired `restore` must never
be visible while the proxy may not yet gate it. A lone `restore` is therefore
hidden only on a boot whose probe of its connector failed.
`observe_version` is never hidden or gated; it is read-only. No existing tool
named `restore` changes behavior unless its connector also adopts
`observe_version`. The paired case reaches the runner MCP catalog projection
path set, so it carries the live provider and external integration tiers.

Acceptance: paired, on a live model turn with the reference connector, the
catalogue has no `restore`, a direct proxy call to `restore` without a grant is
refused `grant_required` after the re-render, and an authorized undo executes
through the executor; lone, a fixture connector whose `restore` takes
`{backup_id}` and which has no `observe_version` keeps `restore` in the
catalogue, a model turn calls it exactly as today (ungated unless the bundle
gates it), its capability row is not capable, and its actions are refused
`refused_not_restore_capable`; a boot whose probe of a paired connector fails
shows no `restore` in the catalogue.

## Sealed snapshots

<!-- @spec ACTION-EXECUTOR-9 -->
**ACTION-EXECUTOR-9. The sealed reply convention.** A restore-capable write
connector replies with `prior`, `version` and `target`. `prior` is an envelope
with exactly the keys `sealed` (the constant `curie.snapshot.v1`), `kid` (1 to
64 characters of `[A-Za-z0-9._-]`, non-secret) and `ciphertext` (standard
base64, no line breaks, at most 65536 decoded bytes). `version` is a non-empty
string of at most 256 characters naming the version the call left (ADR 0124
decision 4). The platform never seals, opens or inspects ciphertext. The
worker's `_snapshot` records `prior_state` exactly when the frame is not
`redacted`, `prior` validates as an envelope, and neither `target` nor `version`
contains the shared redaction placeholder prefix `[REDACTED:`. The `redacted`
condition is the frozen consumer rule on
`packages/aci-protocol/src/aci_protocol/events.py::SideEffectFlag` (a redacted
`result` is never restorable state), and it is what protects against any
producer's scrubber, including a second ACI server with its own placeholder;
the placeholder check is defense in depth for Curie's own. A cleartext `prior`
is history: it stays in `result` and `prior_state` stays null. `post` is no
longer required or read for a sealed record.

Acceptance: one shared vector of valid and invalid replies (extra key, wrong
constant, URL-safe alphabet, oversized ciphertext, empty version, placeholder in
ciphertext, target or version, `redacted` set with an otherwise valid reply,
cleartext prior) is read by the worker's production parser, by the runner's
`OutboundRedactor` envelope validation (ACTION-EXECUTOR-10) and by the reference
connector's tests, with the same outcomes. The worker and runner sides ship in
different images and join the parity seam entry of ACTION-EXECUTOR-24.

<!-- @spec ACTION-EXECUTOR-10 -->
**ACTION-EXECUTOR-10. The envelope crosses the frame verbatim (ADR 0124
decision 7).** `OutboundRedactor` treats the replay inputs of a
`side_effect_flag` result, `result.prior` when it validates as an envelope,
`result.version` and `result.target`, as verbatim or withheld, never altered:

* pattern rules are not applied to a valid envelope's `ciphertext`, which is
  the opaque value decision 7 says leaves nothing to scrub;
* pattern rules still run over the envelope's `kid`, whose alphabet admits token
  shapes, and over `target` and `version`; a match in any of them removes all
  three replay inputs from the frame (set to null) and sets `redacted`;
* the held secret literal check still runs over all three, ciphertext included;
  a held literal anywhere in them removes all three and sets `redacted`, so no
  held secret leaves the sandbox;
* every other field of `result`, and `arguments`, keeps today's scrubbing, as
  ADR 0124's consequences keep them a separate concern.

This realizes decision 7 for the field it governs without weakening the held
secret guarantee: the envelope either crosses exactly as the connector wrote it
or does not cross. `redacted` keeps its frozen meaning and its consumer rule:
it is set whenever anything in `result` was replaced or withheld, and a
`redacted` frame records no snapshot (ACTION-EXECUTOR-9), even when the
envelope beside the scrubbed field crossed unaltered. A reply whose only pattern
match is inside the ciphertext is not `redacted`, because nothing was replaced,
and stays undoable.

Acceptance: an envelope whose ciphertext matches a pattern rule (constructed
for the test) crosses byte-identical, the frame is not `redacted` and the record
is undoable; an envelope containing a held secret literal, or a `kid` shaped
like a token, is withheld, the frame is `redacted` and the record is not
undoable; a secret in `result.summary` is scrubbed, the valid envelope beside it
crosses unaltered, the frame is `redacted` and the record is not undoable; the
existing redaction suites still pass.

The held-literal check applies to the decoded ciphertext bytes as well as to
the envelope text, so a plaintext secret wrapped in base64 withholds the replay
inputs exactly as an unwrapped one does. `post_version` is at most 256 characters of
printable ASCII without placeholders; the worker and the API both refuse a
longer or malformed value rather than truncating it.

<!-- @spec ACTION-EXECUTOR-11 -->
**ACTION-EXECUTOR-11. The ledger records what a restore needs.** Additive
columns on `agent_actions`: `post_version`, `connector`, `connector_digest`,
`authority_kind` and `authority_ref` (shared with
[#4068](https://github.com/curie-eng/curie/issues/4068); plan task 3 adds them
and #4068 adopts them). `undoable` becomes: succeeded, an agent, a valid
envelope in `prior_state`, a `post_version`, a `target`, a `connector_digest`,
a `restore_capable` capability row for that agent, connector and digest
(ACTION-EXECUTOR-13), sealing key custody computed from the agent's in-force
version at read and ruling time (ACTION-EXECUTOR-16), and no restore execution
that is not `refused`. `undone_at` and `undone_by` are written
only when a restore is confirmed. The completion route takes `connector` and `connector_digest` only
together, only under the internal worker token (`403` otherwise), and only when
`connector` is the `mcp__<connector>__` prefix of the action's stored tool
(`422` otherwise); a refusal stores nothing. Every other completion field keeps
the platform key, an accepted existing trust: a key holder can already write
`prior_state`, `target` and `post_version`, but cannot attribute a digest.
Rows written before this change, including
any with a cleartext `prior_state`, are not undoable and are not migrated or
purged. Audit evidence never stores a state or an envelope; refusals name
versions. Ruling refusal codes for missing ingredients: `refused_unsealed`,
`refused_unversioned`, `refused_no_digest`, `refused_not_restore_capable`,
`refused_key_custody`, `refused_no_agent`.

The undo route applies this derivation from the change that adds it: an action
whose `undoable` is false is refused with its code before any audit evidence of
a granted undo is written, so a read and a ruling never disagree.

Acceptance: each missing ingredient alone makes `undoable` false through the
real API read and maps to its code; the undo route refuses the same action with
that code and writes no granted-undo audit row; a legacy cleartext row is refused
`refused_unsealed`; a confirmed restore sets `undone_at`; a failed or
indeterminate one does not and still blocks a second undo.

<!-- @spec ACTION-EXECUTOR-12 -->
**ACTION-EXECUTOR-12. Attributing the digest without the kernel.** The kernel's
record call passes no deployment, so the worker composes a recorder wrapper
around `ActionClient` that implements the same `ActionRecorder` protocol. On
the opening frame and again on the closing frame it reads the target
connector's owned Deployment by name (a new single-object `get`, never
`list_owned`): image reference, `metadata.generation`,
`status.observedGeneration` and replica counts. Each read is bounded at two
seconds, so the wrapper adds at most four seconds per action, and a timeout or
error records null. It records `connector` and `connector_digest` only when both
reads show the same generation, a completed rollout and an image reference
pinned by `@sha256:`. A read shows a completed rollout only when all four
hold: `status.observedGeneration` is at least `metadata.generation`, and
`status.updatedReplicas`, `status.availableReplicas` and `status.replicas` each
equal `spec.replicas`. The last condition is not redundant: during a surge
rollout the first three can hold while an old pod still serves, which leaves
`status.replicas` above `spec.replicas` (measurement M5). A failed read never
fails the record or the turn. The local tier has no reconciled Deployment and
records null. The wrapper sends the pair on the completion under the internal
worker token (ACTION-EXECUTOR-11); the worker composes it only when that token
is configured. Each read makes one attempt, with no client retries, so an
abandoned read does not outlive its bound.

Acceptance (cluster): a call during a completed rollout records the digest; a
call that straddles a rollout, a tag-referenced `image:` connector, a plugin
MCP server, an unreadable Deployment and an API server delayed past the bound
each record null; the turn completes in every case, within the bound.

The worker's `get` on Deployments is granted only when the executor is enabled
and the connector reconciler that already manages those Deployments is
enabled; with the executor off, the worker Role is unchanged and actions record
no digest, so they are not undoable. Least privilege outweighs recording digests
for an executor that is not running. The gate narrows the single-object read
path rather than creating a read capability: the reconciler's existing `list`
grant already returns the same Deployment objects (measurement M5).

<!-- @spec ACTION-EXECUTOR-13 -->
**ACTION-EXECUTOR-13. Restore capability from the advertised list.** When the
connector reconcile observes a hosted connector rolled out at a digest with no
capability row, a wrapper around the reconcile pass calls the probe route of
ACTION-EXECUTOR-1. The executor runs the probe as a `list` phase, bracketed by
the completed-rollout-at-digest check of ACTION-EXECUTOR-12 and -14 immediately
before and after it; if either check fails, the probe is refused
`connector_digest_unavailable` and records nothing, so a tool list is never
attributed to a digest that was not the one serving. The API stores
`connector_capabilities (agent_id, connector, digest, restore_capable,
observed_at)`. `restore_capable` is true exactly when
`restore` is advertised, not annotated read-only, and its input schema requires
`target` and `prior_state`, and `observe_version` is advertised, annotated
read-only, and requires `target`. A digest's tool list is a property of the
image, so the row is keyed on the digest; key custody depends on the version's
declarations and is not stored here (ACTION-EXECUTOR-16). Every other outcome, including
a probe failure, is not capable, which is decision 5's "treated as restoring
nothing". The `call` preflight rechecks the rule before every restore.

Timing: decision 5 calls capability inspectable before deploy. A sandbox can
reach a hosted connector only after it is rolled out, so the probe runs after
rollout and before any undo can be ruled. Because `undoable` is derived at read
time, actions recorded under a digest before its probe completes become
undoable once the row lands. That is intended: the snapshot and the digest were
already recorded, and only the capability was unknown.

Acceptance: a conforming reference connector is recorded capable and its
actions become undoable, including one recorded before the probe completed; a
connector without `restore`, with a read-only `restore`, without
`observe_version`, or unreachable is not capable and its actions stay not
undoable; a probe whose bracket observes a rollout in progress records no row;
a capable digest whose live list later drops `restore` is refused at
preflight.

<!-- @spec ACTION-EXECUTOR-14 -->
**ACTION-EXECUTOR-14. The pinned digest or nothing.** Immediately before the
`observe` phase the worker requires that the agent's in-force version renders
the target connector at `connector_digest` and that the owned Deployment shows a
completed rollout at that exact image; otherwise it refuses
`connector_digest_unavailable`. No old image is started to serve a stale
snapshot; the compatibility contract is refusal, not migration.

Acceptance: after an upgrade to a new digest, an undo of an action recorded under
the old one is refused with no call reaching the connector; reverting to the old
digest makes the same undo executable again.

<!-- @spec ACTION-EXECUTOR-15 -->
**ACTION-EXECUTOR-15. The platform compares versions before the restore.**
ADR 0117 decision 4 and ADR 0124 decision 4 put the conflict check on the
platform, before replaying, comparing two opaque strings; ADR 0203 reuses it
unchanged. In one executor sandbox, after `list`:

1. `observe` calls the pinned connector's `observe_version` with the recorded
   `target`. This is the version observed now.
2. The worker reports it to the API's observation route, and the API compares
   it with the recorded `post_version`. On any difference, or an absent or
   malformed version, the API writes audit `refused_conflict` naming
   both versions, ends the execution `refused` with `version_conflict`, and
   makes no `restore` call.
3. Only on equality does it commit `dispatched` and run `call` with
   `{"target", "prior_state", "expected_version"}`, where `expected_version` is
   passed only when the connector's `restore` schema declares it.

Connector-side compare-and-swap on `expected_version` is defense in depth for
the window between steps 1 and 3; it is never the check. A connector that
refuses with `{"ok": false, "refused": "version_conflict"}` during `call` ends
`failed` with `version_conflict_at_write`. The residual trust is named: the
platform trusts that `observe_version` reports the live version honestly, as it
trusts the connector's reply for the forward record; a connector without
compare-and-swap leaves the seconds between observe and restore unguarded, as
any check then replay does.

A conflict is a provable non-write (only a read was called), so it ends
`refused` and releases the action: a later ruling may try again and is refused
again while the version differs. This is the observed conflict refusal that
ADR 0203 ruling 5 asks qualification to record: the `refused` execution with
`version_conflict`, its audit row naming both versions, and the resource
observed unchanged, which #4066 consumes.

Acceptance: with the reference connector, a version moved after the forward
action yields `refused_conflict` with no `restore` call observed at the
connector and the operator's value intact; a later ruling is refused the same
way; a fixture whose `restore` ignores `expected_version` is still protected by
step 2; a version moved between `observe` and `call` against a compare-and-swap
connector ends `version_conflict_at_write`.

<!-- @spec ACTION-EXECUTOR-16 -->
**ACTION-EXECUTOR-16. Key custody.** ADR 0124 decision 1 requires the key to
reach only the hosted connector. The first release recognizes it by two
reserved names, `SNAPSHOT_SEALING_KEY` and `SNAPSHOT_SEALING_KEYS_RETAINED`
(retired keys kept while their records are undoable, decision 3), and enforces
custody on every path that can carry a connector secret:

* API intake refuses either name in any form other than a `SecretRef`;
* `inject_connector_secrets` withholds both names from every sandbox;
* `charts/curie/templates/agent-connector-secrets.yaml` refuses to render either
  name under `agentSandbox.connectorSecrets` (a `fail`, like the reserved-name
  guard beside it), so the chart path cannot place it in a sandbox-readable
  Secret;
* the executor sandbox receives no secret beyond ACTION-EXECUTOR-5's header set.

Custody is also an `undoable` ingredient, computed by the API at read and
ruling time from the agent's in-force version, never cached: it holds only when
that version declares `SNAPSHOT_SEALING_KEY` as a `SecretRef` on that connector.
A later version with the same digest that drops the `SecretRef` therefore makes
the connector's actions not undoable at once. A connector that seals with a key under any other name, or a
plain named one, therefore never produces an undoable record. Limitation, named
because no code can close it: the platform verifies the declaration, not which
key the connector's code actually uses. The local tier refuses `SecretRef`, so
it produces no undoable record. A missing or retired key surfaces as the
connector refusal `sealing_key_unavailable` during `call`, with no fallback.

Acceptance: a bundle declaring `SNAPSHOT_SEALING_KEY` as a plain string, a
`secret_files` entry or a sealed secret is refused at intake; a chart render with
it under `agentSandbox.connectorSecrets` fails; with a `SecretRef` the key is in
the connector pod and absent from the runner env of an ordinary turn and of an
executor sandbox (cluster exec check); a connector sealing with a plain
`MY_SEAL_KEY` fails custody and its actions are not undoable; deploying a new
version with the same digest and the `SecretRef` removed makes previously
undoable actions refuse `refused_key_custody`; retaining an old key keeps an earlier record restorable, dropping it
yields `sealing_key_unavailable`.

## Lifecycle, reporting and recovery

<!-- @spec ACTION-EXECUTOR-17 -->
**ACTION-EXECUTOR-17. At most once past dispatch.** States: `requested`,
`claimed`, `dispatched`, `confirmed`, `failed`, `indeterminate`, `refused`. A
worker claims a `requested` row with a lease and a fencing attempt number;
every later transition presents the fence. The `list` and `observe` phases run
in `claimed`. Before the `call` request the worker commits `dispatched`. Lease
expiry in `claimed` returns the row to `requested` with the attempt
incremented, at most three times, then `refused` with the last pre-dispatch
code. Lease expiry in `dispatched`, a lost response, a crash after the commit,
or a deadline moves the row to `indeterminate`, which is terminal: a call that
may have reached the connector is never repeated, as with the no-retry marker
and `ChannelCanvasEdit`. `refused` means provably no write call and releases the
action. `confirmed`, `failed` and `indeterminate` are terminal. A sweeper in
the executor loop applies the expiry rules.

Amendments E5 and E9 (AUTOMATED-REMEDIATION-12): a `read` execution never
enters `dispatched`. It runs `list` and its one `read` in `claimed` and ends
`confirmed` through `POST /action-executions/{id}/samples`, or `refused`; a read
whose lease expires in `claimed` ends `refused` with `runner_unavailable` and is
never re-queued. The claim route hands out only due executions (`not_before`
NULL or past), oldest `not_before` first, under one transaction-scoped advisory
lock so the installation-wide cap holds across API replicas; while two or more
slots exist, at most all but one live executions are reads. A requested read
whose next sample in its series (same agent, `authority_ref`, connector, tool,
arguments and pointer) is already due ends `confirmed` with the sample
`skipped`, never claimed. The worker loop runs up to the cap at once; the API's
count is the authority.

Acceptance (fault injection on real Postgres, Valkey and the reference
connector): kill the worker before the `dispatched` commit, between the commit
and the request, after the connector call and before the report, and during
release; the connector observes at most one write call, the row ends `refused`
(first case), `indeterminate`, or `confirmed` when the report survived, and no
second grant is minted for one execution.

<!-- @spec ACTION-EXECUTOR-18 -->
**ACTION-EXECUTOR-18. Reporting to the ledger and the receipt.** The worker
reports through API-key routes `POST /action-executions/claim`,
`POST /action-executions/{id}/observation`, `POST /action-executions/{id}/dispatch`
and `POST /action-executions/{id}/outcome`, plus `GET /action-executions/{id}`.
A forward execution's holder also reads its bound call through
`POST /action-executions/{id}/arguments` (ACTION-EXECUTOR-19). Each transition
is idempotent for the same fence and payload and returns `409` for a
conflicting one. A confirmed restore sets `undone_at` and `undone_by`
(from `requested_by`) and appends audit `confirmed`; a failed or indeterminate
one appends its code. These routes replace the `confirm-undo` route the ADR
0121 spike named, so restores and forward actions share one surface.

Receipt. ADR 0121 decisions 3 and 4 put refusals and failures on the receipt,
and its scope says the receipt lands on the channel that asked. In the first
release the only channel that can ask for an undo is the CLI
(ACTION-EXECUTOR-23), so the receipt is the CLI `undo` and `execution` output,
which states the execution state and code, including
`connector_digest_unavailable` and every failure. No chat channel can ask, so
none is answered; a chat undo control and its receipt belong to
[#4072](https://github.com/curie-eng/curie/issues/4072), and decisions 3 and 4
are realized for chat channels only when it lands.

Acceptance: a replayed outcome returns the stored row unchanged; a different
outcome for the same execution is refused and the first stands; a stale fence is
refused; the CLI receipt for each terminal state names it.

<!-- @spec ACTION-EXECUTOR-19 -->
**ACTION-EXECUTOR-19. Forward actions.** A server-side API function creates a
`forward` execution from a verified authority record: a policy generation
admitted by [#4065](https://github.com/curie-eng/curie/issues/4065) or an
argument-bound approval from [#4069](https://github.com/curie-eng/curie/issues/4069).
Connector, tool and canonical arguments come from that record, never from a
caller, and it refuses `authority_unavailable` while no authority source
exists. A forward execution never calls `restore` or `observe_version`,
whether or not the connector pairs them: the frozen `runner-execute` vector
treats a `call` of `restore` as the restore phase, which requires `observe`
first, so a lone `restore` stays an ordinary tool for model turns but cannot be
a forward action. Either verb is refused `reserved_verb_via_forward` before
dispatch (amended during plan task 14: the worker refuses it from the `list`
reply, before the `dispatched` commit, because a runner preflight refusal after
the commit could only end `indeterminate`). At dispatch the API creates exactly one
`agent_actions` row: `dedupe_key` and `call_id` both `exec:<execution id>`,
tool `mcp__<connector>__<tool>`, the canonical arguments, the authority fields,
and `connector` and `connector_digest` copied from the execution, status
`pending`, listed under the conversation `action-exec:<execution id>`. The
outcome completes it with the same snapshot parsing as a model turn's call, so
a platform-executed forward action is undoable on the same terms.

The receipt (`ExecutionOut`) never carries arguments, so before dispatch the
worker reads the bound call through `POST /action-executions/{id}/arguments`,
internal worker token, whose body is exactly the claim's fence. It answers
`{tool, arguments}` for a `claimed` forward execution only, `409` for a stale
fence or any other kind or state, and moves nothing. The worker refuses
`authority_unavailable` when the route will not answer and recomputes
`arguments_sha256` over the canonical text it will send (ACTION-EXECUTOR-7).
A forward run is the kill switch, that read, the pinned digest with the tool in
the caller proxy's gated set, the sandbox, `list` (the tool must be advertised,
`tool_not_advertised`), the digest again, the kill switch, the `dispatched`
commit, one grant and one `call` of that tool over that text, then the
completion through `POST /actions/{id}/complete` under the worker token with the
execution's `connector` and `connector_digest`, then the outcome. It never runs
`observe`. A forward tool is an ordinary connector tool: a reply that is not a
tool error confirms, and a tool error or a structured `ok: false` fails. Until #4068 delivers authority-aware undo authorization, undo of a
forward-executed record (any record carrying an `authority_kind`) is refused
`refused_authority_unresolved` with HTTP 409 and one audit row, before the
authorization check, and the derived `undoable` is false;
REMEDIATION-14 makes it approval gated, and ADR 0117 decision 3's ungated
default must not apply.

Acceptance (integration, authority rows inserted by the test in real Postgres):
a forward execution produces one ledger row before the call carrying the digest
and completes it; a replayed creation creates nothing; arguments differing from
the authority record are refused; undo of the result is refused
`refused_authority_unresolved`.

<!-- @spec ACTION-EXECUTOR-20 -->
**ACTION-EXECUTOR-20. Closed refusal and failure codes.** Each code belongs to
one stage; only ruling and pre-dispatch codes are provable non-writes.

| Stage | Codes |
| --- | --- |
| Ruling (HTTP 409, 412 or 503; audit row, no execution) | `executor_disabled`, `refused_restore_in_flight`, `refused_no_agent`, `refused_unsealed`, `refused_unversioned`, `refused_no_digest`, `refused_not_restore_capable`, `refused_key_custody`, `refused_authority_unresolved`, `refused_actor_mismatch` (HTTP 403), `refused_duplicate_ruling` (HTTP 409), plus the existing ruling refusals |
| Pre-dispatch (`refused`) | `agent_stopped`, `authority_unavailable`, `reserved_verb_via_forward`, `arguments_mismatch`, `tool_not_grant_bound`, `connector_not_hosted`, `connector_digest_unavailable`, `restore_not_advertised`, `restore_schema_mismatch`, `tool_not_advertised`, `version_conflict`, `sandbox_unavailable`, `runner_unavailable`, `connector_unreachable`; amendment E6 adds `tool_not_read_only` (worker reported) |
| Connector refusal during `call` (`failed`) | `version_conflict_at_write`, `sealing_key_unavailable`, `snapshot_unopenable` |
| Post-dispatch (`failed` or `indeterminate`) | `connector_error`, `unstructured_reply`, `response_lost`, `deadline_exceeded` |

An unknown code from a runner or connector is normalized to `connector_error`
or `response_lost` by stage, never passed through. Amendment E6: a read's
`pointer_absent`, `result_unstructured` and a `skipped` sample are sample
results stored on the execution, never refusal codes, and an unknown refusal of
a read is `runner_unavailable`, because a read cannot write.
The ruling route answers `executor_disabled` with HTTP 503 and every other
ruling refusal with the status its existing refusal used.

API route decisions the worker relies on: the worker reports the version it
observed through `observe_version` to the API's observation route, and the API
compares it with the recorded `post_version` and records `version_conflict`
with its audit row, so the ledger owner makes the comparison. The claim route
reclaims an execution whose lease has expired, with the next attempt number; a
report fenced by a stale attempt or lease owner is refused. These internal
routes use the platform API key the worker already holds. A finished probe
records one `connector_capabilities` row for its agent, connector and digest,
with `restore_capable` true only when the probe observed both `restore` and
`observe_version` (ACTION-EXECUTOR-8).

Review decisions for the routes, which the worker and later tasks rely on:

* A chat principal may undo only an action whose gating approval its token
  names, an adapter principal only an action whose gating approval it serves
  (anything else reads as not found), and an ungated action accepts no chat
  credential.
* The undo ruling derives its actor and channel evidence from an authenticated
  chat, console, operator or adapter principal, exactly as the approval resolver
  does under ADR 0106; a self-asserted `actor` in the request body is not
  authority, and a body actor that differs from the principal is refused. A
  ruling now causes a real restore, so a bare platform key with a body actor no
  longer rules. Operator and adapter principals are signed with that key, so
  its holder can still mint an operator principal naming any approver; that is
  the same trust ADR 0106 places in approving, not a stronger guarantee. The
  undo route authenticates the principal before it looks up the action, so an
  unauthenticated caller cannot learn which action identifiers exist.
* The internal probe, claim, observation, report and dispatch routes require
  the internal worker token, not the platform or operator key, so a key holder
  cannot forge a confirmed restore or a `restore_capable` capability row. With
  the executor disabled, claim and dispatch hand out nothing.
* The ruling stores `arguments_sha256` for the restore call it authorizes, as
  ACTION-EXECUTOR-7 requires of the creator.
* A probe's `authority_ref` is its probe key. A probe that ended `refused` or
  `failed` does not block a later probe of the same agent, connector and digest:
  the next probe is a new execution whose key carries the next probe attempt
  number, and only a non-terminal or confirmed probe is adopted.
* Lease expiry: a claim is attempted at most three times; an expired claim
  before dispatch is reclaimed with the next attempt, the third expiry ends
  `refused` with `runner_unavailable`, and an expired dispatched execution ends
  `indeterminate` with `response_lost`.
* Every terminal restore outcome writes one closing audit row naming its state
  and code, with versions only. A database uniqueness violation is mapped to
  the constraint it names; only the live restore index maps to
  `refused_restore_in_flight`.

Acceptance: a table-driven test drives each code through its real producer and
asserts the terminal state; an injected unknown code is normalized.

<!-- @spec ACTION-EXECUTOR-21 -->
**ACTION-EXECUTOR-21. Stops.** The per-agent kill switch
(`apps/worker/src/curie_worker/killswitch.py::KillSwitch.is_killed`) is read at
claim and again immediately before the `dispatched` commit; a stopped agent or
an unreadable switch refuses `agent_stopped`. Rate limits, the circuit breaker
and the per-policy disarm belong to admission
([#4071](https://github.com/curie-eng/curie/issues/4071)).

Acceptance: stopping the agent between claim and dispatch yields `agent_stopped`
with no write call; an unreachable switch yields a refusal, not a dispatch.

<!-- @spec ACTION-EXECUTOR-22 -->
**ACTION-EXECUTOR-22. Telemetry.** Spans and metrics carry kind, state, stage,
code and connector name only; logs never carry arguments, envelopes, results or
versions.

Acceptance: a log and span capture over a full restore contains none of the
reference connector's argument values, ciphertext or versions.

<!-- @spec ACTION-EXECUTOR-23 -->
**ACTION-EXECUTOR-23. Operator surface.** A new `actions` verb group under both
`curie local` and `curie cluster` (clap surface in `cli/src/main.rs`, handlers in a
new module under `cli/src/commands`; the existing agent lifecycle module
`cli/src/commands/agent_actions.rs` is unrelated) provides `list`, `show <id>`, `undo <id>` and
`execution <id>`, with `--json`, ADR-0021 exit codes and `{"error","fix"}`
errors. `undo` prints the execution id and state, never a snapshot; `execution`
is the receipt of ACTION-EXECUTOR-18. The CLI manifest is regenerated; no
console action is added, so the console parity map gains no entry. The CLI bundle check mirrors ACTION-EXECUTOR-16's reserved sealing name
refusal, following the CLI and API validation convention.

Acceptance: each verb emits exactly one JSON object under `--json`, refusals
included; the CLI check refuses a plain `SNAPSHOT_SEALING_KEY` with the API's
reason.

## Cross-image seam and frozen contracts

<!-- @spec ACTION-EXECUTOR-24 -->
**ACTION-EXECUTOR-24. The worker and runner route is a frozen pair.** The
worker's `execute` client and the runner's `/v1/execute` ship in different
images, as do the mode variable's writer and reader. A new vector,
`runner-execute` under `tests/vectors`, freezes the request and response shape
for each phase, every refusal code the route returns, the executor mode status
body and the exact mode variable name and value; the worker client, the runner
route and the worker's boot env builder each read it through their production
code. AGENTS.md's parity seam registry gains the entry, tagged with the vector,
in the same change, together with a second entry for the sealed envelope pair
(the runner redactor and the worker's `_snapshot`, ACTION-EXECUTOR-9). The ACI producer interface document lists `/v1/execute` as
an optional runner-private route and updates its control route count;
`run_conformance` does not cover it, which the document states, and a server
without it answers `404`, which the worker maps to `runner_unavailable`.

Acceptance: changing a field name on one side alone fails that side's vector
test; a runner image without the route yields `runner_unavailable`, not a
crash.

<!-- @spec ACTION-EXECUTOR-25 -->
**ACTION-EXECUTOR-25. No frozen contract changes.** Nothing in
`packages/aci-protocol` or `packages/plugin-format` changes. The envelope and
version ride inside the existing free-form `result` of
`packages/aci-protocol/src/aci_protocol/events.py::SideEffectFlag`, and
`redacted` keeps its meaning and its consumer rule (set whenever outbound
redaction replaced or withheld anything in `result`; a redacted `result` is
never restorable); the mode variable and grant are runner-private, outside `BootEnv`;
`/v1/execute` carries no ACI frame, like `/v1/turn-admit`; the sealing key uses
the existing `SecretRef` seam; the gated set is computed by the API.

This would become a blocker, to stop and raise rather than design around, if a
reviewer requires: the executor mode or the execution request as a `BootEnv`
field or an ACI frame; an explicit sealing key declaration field in
`connectors.yaml` instead of the reserved names; capability, digest or per-field
redaction carried on `side_effect_flag`. Each is a frozen contract change that
lands as its own reviewed PR first.

Acceptance: the wire lock, the ACI schema compatibility test and the
plugin-format schema export are unchanged by every realizing PR.

## Out of scope for the first release

* Starting an old connector image to serve a stale snapshot.
* Restores on the local or skill tier, plugin-local MCP servers, `url:`
  connectors and tag-referenced `image:` connectors.
* Whole-turn undo, undo of an undo, and any automatic undo (REMEDIATION-14).
* A chat undo control and chat receipts (#4072).
* Policy storage, nomination parsing, admission, rate limits, the verifier and
  qualification evidence (#4063 through #4066, #4070, #4071).
* Connector reported recovery limitations ([#3652](https://github.com/curie-eng/curie/issues/3652));
  the executor never reads `result.reversal`.
* Scrubbing policy for `arguments` and the non-replay parts of `result`, and
  ledger retention, including legacy cleartext snapshots.
* Multi-step or composite forward actions; one execution is one write call.

## Acceptance commands

Each test and implementation unit cites its criterion ID. Run the touched
areas' suites from the repository root (`uv run pytest -q` with focused
selectors defined in each test-first commit),
`uv run python -m curie_api.export_openapi` with
`uv run pytest apps/api/tests/test_openapi_drift.py -q`, the UI checks for the
regenerated types, the CLI checks (`cargo fmt --check`,
`cargo clippy --all-targets -- -D warnings`, `cargo test`), chart render
assertions, and the docs check `scripts/check-docs.sh` (run with bash).
Postgres and Valkey are disposable and owned by the run. Selector names a
test-first commit proposes are not evidence until run. Runtime acceptance is per
tier in the plan; a fake model, a rendered chart or a stubbed connector never
closes a runtime criterion.

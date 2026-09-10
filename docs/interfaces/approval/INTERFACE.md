---
seam: Approval / authorizer
kind: CLEAN
impls: 3 approver sets behind one authorizer (Slack channel, Slack user group, explicit user list)
grade: not separately graded
epics:
  - "#22"
order: 13
---

# INTERFACE: Approval / authorizer

Repository publication uses this same approval plane but does not wake the
sandbox after resolution. The trusted worker atomically creates the approval
and private publication row through `POST /v1/internal/publications`; requester
equality has the same meaning as every other approval: it neither grants nor
denies membership in the selected approver set. Approve schedules the platform publication reconciler, while
deny and expiry terminalize without redeeming a write credential. Terminal
results are delivered from a bounded durable outbox, independently of patch and
credential cleanup, so adapter retries cannot repeat a GitHub mutation.

> Part of the Curie swappable-seam catalog — see the [seam index](../../interfaces.md).

<!-- BEGIN GENERATED: header (curie dev docs-lint) -->
> **Kind:** CLEAN &nbsp;·&nbsp; **Implementations today:** 3 approver sets behind one authorizer (Slack channel, Slack user group, explicit user list) &nbsp;·&nbsp; **Swap-readiness grade:** not separately graded
<!-- END GENERATED: header -->

**Kind legend:** CLEAN = a real `Protocol`/typed port class · SOFT = swap via env/URL/prefix/wire, no code interface · NONE = not built yet.

## The black line

The black line is an **Authorizer** port: a server-side decision, at approval-resolution
time, of whether an authenticated principal is allowed to resolve a given pending approval — plus the
`awaiting-approval` lifecycle state that lets a session durably pause on that decision. What
stays opinionated core is *where* the decision is enforced (server-side, at resolution) and
that gates are policy-triggered, never phase-hardcoded by the platform. What becomes
swappable is the authorizer *implementation* (channel-membership first, then user-group,
explicit user-list, platform-RBAC) behind that one server-side check.

## Current contract

The durable base landed with #244, the gates with #245, the authorizer and cards with #246,
and the policy/route/audit layer with #247 — the epic's full primitive is live. What exists
in code now:

- **The durable record + resolve-once semantics (landed, #244).** The `Approval` table
  (`apps/api/src/curie_api/models.py`) with the resolve-once compare-and-set
  (`crud.claim_approval_resolution`, a conditional `UPDATE ... WHERE status='pending'`) behind
  `POST /approvals/{id}/resolve`; losers of the claim race get 409 naming who resolved it,
  a past-SLA record flips to expired (410) and now also enqueues the expiry resume turn
  (#412, below) so the late resolver's dead end no longer strands the session. Creation is
  idempotent on `dedupe_key` (the triggering event id).
- **The `awaiting-approval` status (landed, #244).** `SessionStatus.AWAITING_APPROVAL` plus
  the optional `Final.approval_summary` field
  (`packages/aci-protocol/src/aci_protocol/events.py`), regenerated across all three language
  targets as a backward-compatible frozen-contract change (ADR-0010 authorized it). Two
  further optional `Final` fields carry structured, runner-authored gate provenance
  (landed, #544, ADR-0046): `approval_gate_kind` (`'permission'` or `'policy'`) records which
  trigger type produced the request, and `approval_granted_tool` names the tool a resume-boot
  grant may release (or `None`, always `None` for a policy gate). Both are additive patch-bump
  fields (ADR-0036) and are persisted as the `gate_kind`/`granted_tool` columns on the
  `Approval` record (migration `0015_approval_gate_provenance`). They replace the old
  summary-prefix sniff as the durable source of grant provenance — see the #430 bullet below.
  A further optional `Final` field, `approval_display` (#2565, Draft ADR-0151), carries the
  human sentence a bundle-authored `approvalPolicy.gates[].summary` template rendered. It is
  additive (ACI patch 0.4.5). The worker uses `approval_display or approval_summary` for the
  Slack card, the awaiting-approval notice (`Awaiting approval (<id>): ...`), and the
  resolved card; `Approval.summary` stays the machine `summarize_tool_call` string so the
  `gate_kind IS NULL` prefix fallback and the audit record are unchanged. A gate without a
  template leaves `approval_display` unset, which is today's bytes.
- **The lifecycle (landed, #244; pager advertisement narrowed, #1444).** A skill raises a
  policy gate through the runner's in-process `mcp__curie__request_approval` tool
  (`runner/src/curie_runner/approval.py`) when that tool is present. The runner advertises
  this generic pager only when the observed MCP surface has an action that may write — a
  tool not explicitly `readOnlyHint=true`, including an unknown or unreachable surface — or
  when an explicit actionable approval gate exists. A surface with no MCP tools or only
  explicitly read-only tools carries no generic pager, because approval cannot unlock an
  action it cannot perform. `readOnlyHint` is not authorization and does not change gates
  or tool execution. A live probe that explicitly reports `readOnlyHint=true` also feeds
  the MCP tool's SDK-visible name to the read-only classifier, suppressing the side-effect
  flag, no-retry-after-side-effects classification, and therefore its receipt line. A
  missing or `false` hint, an unknown surface, or a failed probe remains potentially
  write-capable and is classified conservatively. Publication's dedicated approval flow
  and state tools remain independent. When the pager is used, the turn ends
  `awaiting-approval`, the worker persists the record and suspends the sandbox
  (`kernel._pause_for_approval` — the first live use of the dormant ADR-0003 suspend path);
  resolution enqueues a resume turn onto the ordinary runs stream
  (`apps/api/src/curie_api/resumequeue.py`), and the kernel's claim path rehydrates the
  thread with its bound boot env (`substrate.resume(env=...)`).
- **Expiry resume (landed, #412).** A prior gap: an approval whose SLA lapsed with no
  resolver stayed `pending` forever, since the only expiry path lived inside the resolve
  endpoint. A periodic sweeper in the API lifespan (`apps/api/src/curie_api/sweeper.py`,
  `run_expiry_sweeper` driving `sweep_expired_approvals`) now flips lapsed `pending` records
  to `expired` through the same `crud.expire_approval` compare-and-set, appends an `expired`
  audit row (`authorizer="ExpirySweeper"`), and enqueues a platform-authored (`author="system"`)
  resume turn so the suspended session resumes down its timeout branch (ADR-0003). The
  single-wakeup guarantee comes from the pending-guarded compare-and-set in
  `crud.expire_approval`: only the flip winner (the sweeper or a racing resolver) enqueues.
  Both paths also reuse the deterministic `resume_event_id(approval.id)`, but that shared key
  only keeps a redelivery of an already-terminally-handled turn from re-running; it does not
  collapse a duplicate landing while the resumed turn is still in flight. Cadence is `approval_sweep_interval_s` (env
  `APPROVAL_SWEEP_INTERVAL_S`, Helm `api.approvalSweepIntervalSeconds`, default 30s; `<= 0`
  disables). A failure after the flip but before the audit/enqueue (a Valkey blip, a pod
  shutdown mid-batch) no longer drops that wakeup. Both expiry paths mark `resumed_at` only
  once the enqueue succeeded (`crud.mark_approval_resumed`), so a flipped record with a NULL
  `resumed_at` is an owed wake, and #418 widened the reconciler's work-list
  (`apps/api/src/curie_api/crud.py::list_resolved_unresumed`) to include `expired` rows, which
  the pending-guarded sweeper can never re-select. The backstop is the resume reconciler
  (`apps/api/src/curie_api/resumereconciler.py::ResumeReconciler`, #411), which re-enqueues
  every owed wake past a grace horizon. In Helm, `api.resumeReconciler.graceSeconds` derives
  from `worker.deliveryBudgetSeconds + worker.deliveryShutdownReserveSeconds`, and an explicit
  non-null override below that floor is refused at render time so a duplicate resume cannot
  reach an active turn. Outside Helm, `resume_reconciler_grace_seconds` remains the intentionally
  conservative 900s Settings fallback. Since #532, it first runs
  `ResumeReconciler.reopen_dead_lettered_resumes` to re-open an approval whose *delivered*
  resume turn died at the worker's ADR-0039 delivery cap and was dead-lettered (`resumed_at`
  set, so the NULL-gated finder alone would never re-select it). Residual: the backstop is a
  separate switch (`resume_reconciler_enabled`, env `RESUME_RECONCILER_ENABLED`, Helm
  `api.resumeReconciler.enabled`, default on), and with it off nothing retries a lost expiry
  wake at all, which the sweeper's own failure log states outright (the resolve endpoint
  compensates for the same switch by re-raising its enqueue failure as a 500 instead of
  deferring, `apps/api/src/curie_api/routers/approvals.py`, but a sweeper flip has no caller
  to raise to). The reconciler is a backstop, not an unconditional exactly-once guarantee: a
  worker retry loop can keep a turn live past the grace after an inline mark failure, for
  which a worker-side in-flight lease is the named follow-up.
- **The permission gate (landed, #245, #1852).** Per-agent config
  (`agents.approval_required_tools`, forwarded as `CURIE_APPROVAL_REQUIRED_TOOLS` by the
  worker binding) marks tools approval-required. The runner intercepts those calls
  first through an SDK `PreToolUse` hook (`build_approval_hook`,
  `runner/src/curie_runner/approval.py::build_approval_hook`) -- the call is denied
  before execution, and the turn ends `awaiting-approval` on the same override the
  policy gate uses, so both trigger types share one record/suspend/resume lifecycle.
  The SDK `can_use_tool` callback (`build_can_use_tool`,
  `runner/src/curie_runner/approval.py::build_can_use_tool`) is the backstop for a
  tool the hook abstains on, or if no hook is registered. The one-shot grant is
  spent at hook decision time; a concurrent bundle `PreToolUse` matcher can still
  veto the approved call and burn that grant (the CLI dispatches matchers on one
  event concurrently). An agent with no configured gates keeps the historical
  `bypassPermissions` posture verbatim (zero behavior change).
- **The one-shot post-approval allowance (landed, #430, ADR-0035; provenance converged, #544,
  ADR-0046).** A prior gap: after an
  approval was granted and the session resumed, the resume turn re-called the gated tool
  and `can_use_tool` denied it again, because the approval-required set is rebuilt from
  durable config on every claim -- so a manifest `approvalPolicy`-gated tool (the
  unliftable, production-intended form) could never complete post-approval without an
  operator PATCH. Now, when the worker builds the boot env for a claim whose `event_id` is
  the deterministic resume id (`resumequeue.resume_event_id` -> `approval-<id>-resolved`)
  AND that approval is `status='approved'`, the worker's `approval_grant_tool`
  (`binding.approval_grant_tool`) decides grant eligibility from the **durable
  `gate_kind`/`granted_tool` columns** rather than sniffing the summary (ADR-0046 supersedes
  ADR-0035's summary-prefix discriminator): for a `gate_kind='permission'` row it injects
  `CURIE_APPROVAL_GRANT_TOOL=<granted_tool>` (`GRANT_TOOL_ENV`, the exact tool name a
  runner gate layer denied or, for `publish_changes` only, the runner's own stream observer
  recorded (#2294); a trusted runner-authored value either way); for a `gate_kind='policy'` row it
  injects the same `CURIE_APPROVAL_GRANT_TOOL` from the `granted_tool` column **when that
  column is non-null** — the runner sets it only for a manifest gate the operator opted into
  grantability via `grantableViaPolicy` (#558, ADR-0056), with the granted tool sourced from
  the manifest's `gate` value, **never a model-supplied string**, so the model's arguments still
  can never select which tool receives bypass authority (the #430 invariant, now enforced by
  the manifest opt-in and the column rather than the prefix). A policy gate the operator did
  not mark grantable leaves `granted_tool` NULL and still grants nothing — the #544 default is
  unchanged; and only for a `gate_kind IS NULL` row (the rolling-deploy window where an
  in-flight older runner image emitted no provenance) does it fall back to the OLD
  `summarize_tool_call` summary-prefix parse, byte-identical to prior behavior. The runner gate
  allows exactly one call to that tool on
  the boot turn (`ApprovalGate.consume_grant`), then re-denies; `reset()` expires an unspent
  grant on the next turn so an adopted warm-pod follow-up cannot inherit it. The grant is
  **tool-name-scoped** (a different gated tool, or a second call to the same one, still
  gates), **agent-bound** (delivered only when the approval's `agent_id` matches the
  agent resolved for the channel, so a rebound channel cannot cross-grant), scoped to
  **a permission gate, or an operator-opted (`grantableViaPolicy`) policy gate** (enforced by
  the `gate_kind`/`granted_tool` columns; for the NULL-fallback window the
  `summarize_tool_call` prefix is still a RESERVED namespace, and the runner guards
  model-authored policy-gate summaries out of it via `guard_reserved_summary`, so a
  policy-gate request cannot forge a permission-gate grant in that window either), and
  **server-side** --
  derived by the worker from the durable record, never minted by the sandbox, so the
  ADR-0010/0033/0034 "enforced server-side, unspoofable from the sandbox" guarantee holds.
  The membership guarantee is upstream: the authorizer checks every authenticated principal,
  including the requester, against the selected approver set before the status flips to
  `approved`. Requester equality neither grants nor vetoes that membership; a distinct-person
  requirement needs its own future policy. **Known gaps:** (1) *fail-safe adoption* -- if the pod is
  still live when the resume arrives (suspend failed, or a user mention resumed the thread
  first), `claim()` adopts it and the boot env is ignored, so the grant is lost and the
  action re-pauses (self-heals via re-approval). (2) *tool-name, not argument, scoping* --
  the granted tool may be invoked on the resume turn with different arguments than the
  human saw. ADR-0035 named a durable structured-provenance follow-up for this; that
  provenance has now LANDED as ADR-0046 (the `gate_kind`/`granted_tool` columns above), but it
  discriminates *which* gate may grant rather than binding the granted *arguments*, so
  argument-scoping remains open (deferred to #558's operator-gated grantability). Among gates
  that set `grantableViaPolicy`, deploy validation also requires a route be claimed by only one
  distinct tool: two grantable gates on the same route naming the same tool are a duplicate and
  validate fine, but naming different tools is rejected as `approval_policy.grant_route_ambiguous`,
  because the shared normalizer `grantable_routes` (the same helper the runner's loader calls)
  excludes an ambiguous route and would otherwise let the policy validate green while arming no
  grant. See the [bundle format seam](../bundle-format/INTERFACE.md) for the deploy-time
  enforcement detail.
- **Observe-only resume reconciliation (landed, #544, ADR-0046, Decision A2).** To make the
  residual "approved, then the model never re-called the tool" case observable, the worker
  injects an **authority-free** marker `CURIE_APPROVAL_RESUMED_KIND=policy` (`RESUMED_KIND_ENV`,
  `binding.approval_resumed_kind`) at resume boot — a fact about the past that grants nothing,
  set only for a `status='approved'` policy approval, contrast the authority-conferring
  `GRANT_TOOL_ENV` above. At boot-turn end, if the marker is present, gates are armed, no
  permission-gate block occurred, and no side-effecting tool ran, the runner emits a structured
  warning frame (`APPROVAL_NOT_ACTED_CLASSIFICATION`) naming the approval id and **leaves the
  final CLEAN** — no non-clean terminal status. It is instrumentation, not proof: its signal
  (`side_effect_emitted`) is a documented weak proxy for "some tool ran", so it false-alarms on
  the legitimate text-only decision and false-passes on an incidental non-allowlisted tool. A
  signal this weak must not gate a terminal status, so it ships observe-only and earns data for
  a later enforce decision (#559).
- **The resume-boot decision fact (landed, #889, ADR-0076 Stone 3).** A third resume-boot key,
  `CURIE_APPROVAL_DECISION` (`DECISION_ENV`,
  `apps/worker/src/curie_worker/binding.py::BindingResolver.approval_decision`), completes the
  resume-boot env contract alongside `GRANT_TOOL_ENV` and `RESUMED_KIND_ENV`. Like
  `RESUMED_KIND_ENV` it is **authority-free**: it confers nothing and only reports an outcome
  the worker already resolved. It differs from that marker in two ways. It carries all three
  terminal statuses (`approved`, `rejected`, `expired`), not just the approved case, so a
  rejected or expired gate is observable too; `pending` is never returned. And it is consumed
  purely as telemetry, stamped on the turn's root span as `gen_ai.approval.decision`
  (`runner/src/curie_runner/otel.py`), which is what closes the "did an approval get requested"
  gap ADR-0038 named open. It is **agent-bound** on the same terms as the grant: an unknown
  approval, one belonging to another agent, a non-approval event id, or a still-pending record
  all yield None and inject nothing, so a channel rebind cannot leak the fact across agents. It
  is a declared `BootEnv` field on the frozen ACI contract
  (`packages/aci-protocol/src/aci_protocol/session.py::BootEnv`), not a runner-local knob.
- **The policy/route/audit layer (landed, #247; route resolution hardened, #544, ADR-0046).**
  The bundle manifest's `approvalPolicy`
  gates (schema + deploy validation from #273) are consumed at runner boot
  (`load_approval_policy`): each `{gate, route}` pair adds the tool to the permission gate
  and tags it with a route NAME, versioned with the agent. That consumption is
  **fail-closed** (#520, ADR-0050): enforcement intent is read from the RAW declaration, so a
  declared policy that cannot be armed exactly — unparseable, or any distinct declared gate
  name arming nothing — refuses the boot instead of returning the empty map, which builds no
  gate at all and silently restores the bypass posture. The gated-tool set stays the UNION of
  the manifest gates and the operator's `CURIE_APPROVAL_REQUIRED_TOOLS`, and that union is
  the anti-hollow-out mechanism: a bundle may ADD gated names but can never remove an
  operator's. The policy-gate tool accepts an
  optional `route` argument for skill-raised requests, and the runner now **validates that
  route against the manifest's declared routes and fails loud** (#544, ADR-0046, Decision B):
  `build_approval_server(gate)` is per-gate so the `request_approval` tool can see the
  declared routes (`_distinct_routes`), and it binds an omitted route only when exactly one is
  declared; an omitted-and-ambiguous route (>1 declared) or an unknown route returns an
  `is_error` to the model naming the valid routes and creates NO approval, so the model can
  retry in the same turn and nothing widens. Route comparison normalizes identically to the
  manifest reader (`load_approval_policy` / `plugin_format.validate_bundle`: `.strip()`,
  case-sensitive), pinned to agree by an executed test so a validator/loader divergence cannot
  silently arm nothing. A manifest with zero declared routes yields a generic approval
  (`route=None`), for which ADR-0034's channel-membership default is correct. Route names are
  bound to workspace
  targets per agent (`agents.approval_routes`, deployment config, never in the bundle);
  the worker resolves a raised route through its required Slack `resolution` target, posts
  the one interactive card there, and persists that address as `card_channel` on the durable
  record. The optional `notification` target receives a separate text-only ping with the
  approval id and a direction to use the configured approval channel, but no channel
  identifier, `ConfirmIntent`, action value, card-store entry, or other resolving
  affordance. **A named-but-unbound route now escalates
  loudly and creates no approval**
  (#544, ADR-0046, AC2), reversing #247's earlier warn-and-route-to-requesting-channel
  fallback — authority must never silently widen. This is distinct from genuinely
  agent-less generic approvals. ADR-0123 brings resolve time into line
  with that creation-time rule: the API's `get_approval_route_binding` channel fallback
  (`apps/api/src/curie_api/crud.py`) now applies only to a **routeless** approval, so a
  routed approval with no binding is refused at BOTH ends of the lifecycle — no approval is
  created for it, and one already pending stops being resolvable. The
  card's transport follows the same split (#451): a bound channel that differs from the
  requesting channel is deployment policy, not part of the triggering conversation, so the
  card posts top-level over the worker's default Slack transport rather than the trigger's
  per-turn endpoint (this is what lets a non-Slack-triggered turn, e.g. CLI or API, still
  deliver a real Slack card); the requesting-channel case keeps the trigger's endpoint and
  threads under the conversation as before. Notification transport is independent: a Slack
  notification may use the default transport, while another kind stores its own required
  `endpoint` and `adapter`. Those transport fields are redacted from API/CLI reads. Every
  resolution attempt appends to the platform audit log (`approval_audit_entries`,
  `GET /approvals/{id}/audit`): actor, authenticated principal kind, channel evidence,
  decision, and the authorizer snapshot -- who resolved, how the platform authenticated
  them, and why they counted (or were refused). Rows written before ADR-0106 retain
  `principal_kind=NULL` and `authenticated=false` rather than being retroactively trusted.
- **The declared/bound join is also checked at configuration time (landed, #2436).**
  `check_approval_route_bindings` and its sibling helpers
  (`apps/api/src/curie_api/deploy.py`) refuse, before a version becomes live, whenever the
  bundle declares a route with no entry in `approval_routes`: at `POST /deployments`, at
  both git push paths (dev deploy and prod promote), at a bundle attached to a version an
  active deployment already references, and at an `approval_routes` write that would drop
  a route an active deployment still declares. It deliberately checks only presence, not
  validity, of a binding -- a binding that fails schema validation is still treated as
  bound -- and ADR-0046's request-time escalation above is retained unchanged as the
  backstop for that residual.

### Arming a gate: bare MCP shorthand is normalized; unresolvable names fail closed

The permission gate (`agents.approval_required_tools`, forwarded as
`CURIE_APPROVAL_REQUIRED_TOOLS`) matches a tool by its LIVE, fully-namespaced
runtime name. For a bundle-declared MCP tool that live name is
`mcp__plugin_<bundle>_<server>__<tool>`, where `<bundle>` is the
.claude-plugin/plugin.json `name` and `<server>` is the `.mcp.json` server key,
for example `mcp__plugin_github-issues_github__create_issue`.

A bundle's `connectors.yaml` connectors (ADR-0086) are the exception, and they
are namespaced differently: the runner mounts a connector straight onto the
SDK's `mcp_servers` map rather than loading it as a plugin, so its live name is
the bare `mcp__<connector>__<tool>` with no `plugin_<bundle>_` infix (#1495).

**Since #703**, `build_approval_gate` (`runner/src/curie_runner/approval.py`)
normalizes each operator-supplied name to its effective runtime form before
arming, delegating to
`packages/plugin-format/src/plugin_format/approval_policy.py::effective_operator_gates`.
That helper is deliberately SHARED with the deploy-time validator, so a gate
name that validates green resolves identically at runtime rather than the two
paths normalizing separately and disagreeing.

**Normalization returns a UNION of live names, not one rewrite (#1564).** Every
naming rule contributes the live name it would arm and none of them returns on
its own match: a built-in (no `mcp__` prefix) arms verbatim and short-circuits,
since no other rule can read it; a `mcp__<server>__<tool>` naming a declared
`connectors.yaml` server arms unchanged; the same shape naming a declared bundle
MCP server arms the rewritten `mcp__plugin_<bundle>_<server>__<tool>`; and an
already-`mcp__plugin_`-prefixed name arms verbatim **only when it matches an
expected `mcp__plugin_<bundle>_<server>__` prefix for a declared server**, never
blindly. One name routinely satisfies several of those rules and nothing in the
inputs says which server actually hosts the tool, so returning on the first
match would pick a reading by branch ordering. Because `build_can_use_tool`
compares by exact string equality, every live name the ordering skipped would
gate nothing at all, silently. Over-arming costs at most one approval card for a
tool nobody calls; under-arming is a total fail-open. Where a server name itself
contains `__`, the server is resolved by matching against the declared set
(longest match wins) rather than splitting at the first `__`.

There is no verbatim carve-out for an `mcp__`-shaped name that no rule can
verify. When the union comes out empty, or the inputs are refused outright (an
unreadable `.mcp.json`/manifest/`connectors.yaml`, or a declared connector and a
declared MCP server colliding on the same name, which has two different live
forms and no principled winner), the runner refuses to boot with
`ApprovalPolicyError` rather than arming nothing -- previously this failed open:
an unresolvable shorthand silently never matched and the gated tool ran with no
approval whatsoever.

Confirm the exact live name before arming a gate rather than guessing it.
`curie skill check` prints a `match: <server> -> plugin:<bundle>:<server>`
line; rewrite that `plugin:<bundle>:<server>` value into the
`mcp__plugin_<bundle>_<server>__<tool>` prefix and append the tool name. A
tool-call trace also shows the exact live name directly.

A related trap sits on the built-in side: the bundled Claude Code CLI keeps
an alias-to-canonical map for some of its tool names (for example `Task` to
`Agent`, `BashOutput` to `TaskOutput`, `KillShell` to `TaskStop`,
`ListMcpResources` to `ListMcpResourcesTool`). That map is consumed only by
the CLI's own permission-rule parser (the settings.json allow/deny path); it
is not in the path this gate uses. The SDK `can_use_tool` callback that
`build_can_use_tool` compares against always reports the canonical name, so
an operator who gates on an alias arms a literal that never matches and the
gate silently arms nothing. Gate on the canonical name. Since #736,
`build_approval_gate` (`runner/src/curie_runner/approval.py`) treats alias
names as unrecognized on purpose, so arming one still trips the existing
"may be a silent no-op" warning.

## Implementations today

**One authorizer** (`apps/api/src/curie_api/authorizer.py`, pure policy with no Slack in
it) over **three approver sets** behind the `ApproverSet` port (ADR-0034), after an
independent authentication boundary resolves one of ADR-0106's `chat`, `console`, or
`operator` principals. A set answers only "is this actor in the set"; every rule that is
not membership lives in the authorizer, applied identically whatever the set. Requester
equality is deliberately not a rule: the selected set is always consulted, so a requester
who belongs may confirm and one who does not remains denied. A deployment that needs
two-person separation of duties must declare a future distinct policy rather than inherit
one from every ordinary gate. The durable record, the
`awaiting-approval` status, both gate trigger types, the card click-to-resolve flow, and the
suspend/resume lifecycle are live (#244, #245, #246).

The resolve body carries policy input only: `decision` and optional `note`. The API derives
`resolved_by` and channel evidence from exactly one credential; caller-supplied
`resolved_by` and `actor_channel` fields are rejected. The shared platform API key may mint
operator principals and Console login codes but, alone, is not a human identity and cannot
resolve an approval.

Two of the three sets are Slack's, and that is the honest framing: a channel and a user
group are two ways Slack says "who is in the authorized set", not a neutral baseline plus a
Slack feature.

- **`SlackChannelMembers`** (#246, `slack_approvers.py`), the zero-setup default. Channel
  membership is proven by the resolution attempt's channel — the worker routes the Block Kit
  approval card into the approval's channel, Slack only renders that message (and accepts
  clicks) for members of that channel, and the click reaches the platform over the
  dispatcher's authenticated Socket Mode connection. The dispatcher mints a short-lived
  `chat` attestation bound to the Slack user, channel, and approval ID; the API derives
  `actor_channel` from it. `operator` and `console` principals carry no channel and are
  ineligible for this set. Performs no lookup.
- **`SlackUserGroupMembers`** (#420, `slack_approvers.py`), a Slack user group as the
  approver set. Owns its lookup, through the `GroupMembershipSource` port below. Membership
  is never accepted from the caller: a dispatcher-asserted membership claim would be
  forgeable by any platform-key holder, so ADR-0034 rejected it. Authenticated `chat` and
  subject-bound `console` principals are eligible because the API performs the lookup;
  terminal principals remain explicit-user-only. The only set that can come back
  undetermined.
- **`ExplicitUsers`** (#420, `approvers.py`), a literal allowlist of user IDs. Pure, no I/O.
  It owes Slack no *lookup*, but it can still only be **configured with Slack-validated user
  IDs**: the binding schema rejects anything that is not a Slack `U`/`W`-prefixed ID
  (`apps/api/src/curie_api/schemas.py::_SLACK_USER_ID`), never a handle or a name, so even this
  "Slack-free" set is expressed in Slack-shaped identifiers. It is the only set eligible
  for `operator` principals; Console principals may use it or a verified user group. The
  authenticated subject must appear in the selected set.

Platform-RBAC remains the epic's fourth set and is not built.

**The audit vocabulary is frozen.** Each set's `audit_name` pins its pre-ADR-0034 class
name, so `approval_audit.authorizer` still records `ChannelMembershipAuthorizer`,
`ExplicitUserListAuthorizer`, and `UserGroupAuthorizer`. Those classes no longer exist. The
column is append-only history and rows already on main carry those values, so renaming the
vocabulary would make old rows lie about what decided.

### Unfusing notification, resolution, and authority

The route binding has three separate concerns: required `resolution` says where the one
verified interactive card posts; optional `notification` says where else to announce the
request without making that place interactive; and optional `approvers` says who may act.
That lets a second channel carry visibility without widening the single resolution surface:

```
approval_routes: {
  "<route-name>": {
    "resolution": {
      "kind": "slack",                     # only verified interactive surface today
      "address": "C0123ABCD"
    },
    "notification": {                       # optional text-only ping, never a card
      "kind": "email",
      "address": "approvals@example.com",
      "endpoint": "https://adapter.example.com/replies",
      "adapter": "mail"
    },
    "approvers": {                          # optional; absent means resolution membership
      "group": "S0123ABCD",                 # Slack user-group ID
      "users": ["U0123ABCD", "U0456EFGH"]   # explicit allowlist
    }
  }
}
```

Resolution, group, and user entries take IDs, never `@handles` or names, matching the
channel-ID precedent (#143): names never route and fail silently. Notification text carries
the durable approval ID and directs humans to the configured approval channel without
disclosing its kind or address; its message has `interaction=None` and no action values.
Only the resolution card is remembered for later settlement. The group and user-list
authorizers deliberately ignore `actor_channel` — the whole point is that authority does
not depend on card location. Terminal principals remain explicit-user-only. A Console
principal may use the server-side group lookup because its session authenticates the
subject, but it still cannot satisfy channel membership without an attested channel.

`resolution.kind` is the explicit extension point, but the writer rejects every kind except
Slack today. A second interactive channel first needs an adapter-scoped credential that
establishes a verified resolver identity; this change does not build that credential or
turn notification delivery into resolution. Notification `endpoint` and `adapter` remain
stored server-side for egress and are omitted from read responses.

**Precedence: `users` > `group` > channel membership.** When `users` is set, `group` is
ignored and no Slack call is made. When neither is declared, channel membership decides.

**Fail closed, precisely scoped.** When a binding DOES declare an `approvers` spec, every
lookup and config error denies: no bot token configured, HTTP error, network error,
`ok: false`, malformed body, malformed approvers JSON. All deny with a could-not-verify
reason and an audit row; none falls back to channel membership. Failures are not cached.
This does NOT mean the absence of a declaration fails closed: no `approvers` declared means
channel membership, by design. A group that legitimately resolves to zero members is a
successful lookup, not a failure: the actor is simply not a member and is denied as a
non-approver.

**The binding is read fresh at resolve time**, from `agents.approval_routes` via the
approval's `agent_id` and `route`; nothing is snapshotted onto the record at creation. That
is the correct TOCTOU direction for authorization: revocation takes effect at the decision
point, and a user removed from the approver group yesterday cannot resolve a stale pending
approval today. That direction is unchanged, and revocation still takes effect on the next
click. What ADR-0123 changes is the ABSENT binding: deleting or renaming a binding while an
approval pends no longer WIDENS that approval to card-channel membership — it makes the
approval **unresolvable**. A pending approval that named a route is resolvable only through
that route's binding, so with the binding gone the selector returns `UnboundRoute`, which
admits nobody and reports `undetermined`; the resolve attempt is a 403 and the audit row
records `UnboundRouteBinding` with the missing route in its evidence. ADR-0123 supersedes
exactly this one consequence of ADR-0034; ADR-0034's AC4 zero-setup default stands, so a
binding that is PRESENT and declares no `approvers` still means card-channel membership.
An approval with a NULL `agent_id` that nonetheless names a route falls under the same
rule and is refused too — `get_approval_route_binding` returns a bare None for every miss,
and the split is on whether the approval named a route, not on why the binding is missing.
An approval that names NO route never had a narrower set to lose and keeps channel
membership. The recovery from a stuck approval is to restore the binding, after which it
resolves normally; nothing is lost.

### The three ports

**`ApproverSet`** (`approvers.py`) is the black line #420 draws: `async contains(actor,
actor_channel) -> MembershipVerdict`, plus `audit_name`, `operator_eligible`, and
`console_eligible` policies for the audit and principal eligibility checks. It is async
because a set may own a lookup; `ExplicitUsers` simply never awaits. `MembershipVerdict`
carries a third state beyond member/not-member: `undetermined`, meaning the set could not
find out. The authorizer fails closed on it, and it is deliberately never collapsed into
`member=False` — "you are not in the set" and "we could not check" deny for different
reasons, and telling a clicker the first when the second is true sends them arguing with
policy over an outage.

The two Slack sets are asymmetrical and the port does not hide it. `contains` takes
`actor_channel` precisely because channel membership proves membership from the authenticated
card click and performs no lookup, while the user group has no such free evidence and must
ask. `actor_channel` is server-derived from the `chat` attestation, never a resolve-body
assertion.

A fourth set, **`InvalidApprovers`** (`approvers.py`), covers a declared block the platform
cannot read: it admits nobody and reports `undetermined`. Modelling that as a set rather
than a special path is what lets the authorizer have exactly one code path and never learn
that config errors exist.

**`ApproverSetSelector`** (`approvers.py`) picks the set a binding calls for:
`(approval, binding) -> ApproverSet`. It performs no I/O and never raises. Its
implementation, `SlackApproverSetSelector` (`slack_approvers.py`), lives on the Slack side
deliberately — reading a binding means parsing the Slack-shaped `approvers` schema, so
selection is provider-aware by nature. That placement is what keeps `authorizer.py` free of
Slack entirely.

**`GroupMembershipSource`** (`usergroups.py`) is the narrowest port, behind
`SlackUserGroupMembers`: `async members(group_id) -> UserGroupMembership`, raising
`UserGroupLookupError` for every mode that yields no member set. Its one implementation is
`SlackUserGroupClient` (`slack_usergroups.py`), which reads `usergroups.users.list` with the
API's own bot token (`SLACK_BOT_TOKEN`, `usergroups:read` scope) and caches member sets in
process for `slack_usergroup_cache_ttl_s` (env `SLACK_USERGROUP_CACHE_TTL_S`, default 60s;
`0` forces a per-resolve fetch).

**`main.py` is the composition root**: the only module that names Slack to build the
selector, so the authorizer and the resolve endpoint depend on ports rather than a provider.

This narrows the coupling; it does not make the path provider-neutral. **The binding schema
is still Slack-shaped**: `schemas.py` validates usergroup IDs as `S...` and channel IDs as
`C...`, so a non-Slack provider would need a schema change plus an adapter and a selector.
What the ports buy is dependency direction — #420 is the first outbound Slack call
`apps/api` makes, and the authorization decision must not be what holds that client — plus
the authenticated-principal boundary. There is no second provider today.

**Audit records the authority, not just the actor.** Each attempt's audit row carries a
structured `evidence` object naming the basis of the decision: the channel pair for channel
membership; the group ID, the actor's membership verdict, the member count, and the fetch
time for a user group; the list and the actor's presence in it for a user list; the failure
class for a lookup failure. The full member list is deliberately not stored.

## Known leakage

The placement constraint held in the landed base and must keep holding: the authorizer is
**enforced server-side at resolution time**, not inside the sandbox or runner. The runner
only *raises* a request (its tool marks the turn; the record, the resolve CAS, and the
resume enqueue all live with the API/worker), so a compromised sandbox cannot mint or
resolve an approval. That guarantee holds only while the sandbox does not carry a
resolve-capable credential: earlier the worker forwarded the shared platform API key
into the sandbox as the memory/transcript token, and because `POST /approvals/{id}/resolve`
was guarded by the same platform key, a compromised sandbox could resolve its own gated
tool call under any asserted identity. ADR-0033 (#410) closed the sandbox-key gap by
minting a scoped, agent-bound `state` token that only the state router accepts. ADR-0106
closes the remaining caller-assertion gap: the resolve endpoint now accepts only a
dispatcher-attested `chat` token, a live subject-bound Console session, or a signed
subject-bound `operator` token. The platform key alone resolves nothing. A
notification transport credential likewise confers no resolution capability: the
notification contains no interaction, and this contract exposes no second-channel resolver.
The runtime `PreToolUse` hook (`build_approval_hook`, #1852) is the first interceptor
of a gated tool call, with the SDK `canUseTool` callback (`build_can_use_tool`, #245)
as backstop, but
the authorization decision (who may resolve a pending approval) stays on the server that
owns the durable `Approval` record. Policy gate points ship versioned in the bundle; route
bindings (where the verified card resolves, where a text-only notification goes, and who may
approve) are per-agent deployment config (#247, #1460).

The audit trail now records both halves rather than overstating either one. Authentication
establishes the actor and writes `principal_kind` (`chat`, `console`, or `operator`) with
`authenticated=true`; authorization writes the selected set's evidence and verdict.
Historical assertion-era rows remain visibly unauthenticated with a null principal kind.
An audit row may truthfully show the same principal as requester and approver: that says
one authenticated member confirmed their own request, not that a second person reviewed it.

## Cross-links

- **Epic(s):** [#22](https://github.com/curie-eng/curie/issues/22) — approval gates and human-in-the-loop; adds the durable record, `awaiting-approval` status, `canUseTool` gate, and the authorizer interface.
- **Vision doc:** [architecture-vision.md](../../architecture-vision.md) — not one of the six graded jobs; a cross-cutting core lifecycle change, not separately graded.
- **ADR(s):** [ADR-0010](../../adr/0010-approval-gates-and-human-in-the-loop.md) — Approval gates and human-in-the-loop (Accepted); grounds this intended line, including the authorizer sequence (channel membership first, then user-group, explicit user-list, platform-RBAC). [ADR-0034](../../adr/0034-approval-authorizers-resolve-membership-in-the-api.md) — Approval authorizers resolve membership in the API (Accepted); adds the user-group and user-list sets, the API-resident membership lookup, the scoped fail-closed rule, and fresh-read binding resolution. Supersedes ADR-0010's framing of those four as `Authorizer` implementations: they are approver SETS behind one authorizer, and platform-RBAC becomes the fourth set. [ADR-0106](../../adr/0106-an-approver-is-an-authenticated-principal.md) — An approver is an authenticated principal (Accepted); removes caller-asserted resolver identity/channel, makes membership the boundary even for the requester, limits operators to explicit users, and lets Console subjects pass through the same membership sets their authenticated identity can satisfy. Composes with [ADR-0003](../../adr/0003-stateless-first-rehydrate-on-resume.md) (stateless-first suspend/resume, the pause mechanism).

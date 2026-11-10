# 271. Per-request approver sets and separation of duties

Date: 2026-11-05

Status: Accepted

Accepted on 2026-11-07 with explicit maintainer approval from Brian Conn,
before implementation. Implementation follows
[ADR 0085](0085-acceptance-not-implementation-authorizes-an-adr.md) and
[ADR 0102](0102-accepted-alongside-implementation-with-explicit-approval.md).
All realizing changes and required evidence below remain required.

Depends on [ADR 0270](0270-hosted-connectors-receive-the-verified-requester-as-a-signed-caller-claim.md)
for connector requester attribution and its enforced live-turn requester
boundary; selector registration requires `connector_requester_identity: true`.
Delivery remains on existing card surfaces;
[ADR 0272](0272-approver-dm-delivery-uses-one-selected-recipient.md) separately
adds one-recipient DMs. [ADR 0273](0273-approval-routes-own-expiry-and-reminders.md)
is independent and may be accepted and shipped first.

Partially supersedes [ADR 0034](0034-approval-authorizers-resolve-membership-in-the-api.md)
decision 2 and [ADR 0123](0123-a-pending-approvals-approver-set-does-not-follow-its-route-binding.md)'s
no-snapshot clause, only for per-request selection. Current authority and
missing-route refusal remain. Amends
[ADR 0220](0220-one-identity-provider-decides-who-may-invoke-and-approve-on-every-channel.md)
decision 10's permission for a requester on the approver team to approve:
dynamic routes exclude the requester by default, with an explicit audited
operator opt-out or a current canonical group exemption. Realizes the distinct policy
reserved by [ADR 0106](0106-an-approver-is-an-authenticated-principal.md),
leaving ordinary self-confirmations unchanged.

Extends [ADR 0246](0246-common-admission-resolves-an-optional-http-identity-before-authorized-enqueue.md)
decision 3 with an additional canonical selected-person restriction, including
on migrated approver roles. Its raw surface-account grants do not establish a
canonical selected person. Amends
[ADR 0259](0259-a-binding-requires-http-identity-when-its-invocation-or-approver-role-needs-http-facts.md)
decision 1 to classify bindings capable of raising these routes as requiring
HTTP identity. Extends [ADR 0178](0178-the-connector-caller-token-names-the-run-and-work-item.md)
decisions 5 and 6's proxy header/trust contract with a verified-grant id.
These are partial amendments; no ADR is superseded in full.

Source inspection: `origin/next` at
`678a5665d8fe7f10e78fdb282aee8d9210b37021`, fetched on 2026-11-05.
This is source evidence, not runtime qualification.

## Context

A connector can know who should review a particular action without the
platform understanding its resource-selection policy. Static route bindings
cannot express that choice per request. A model must not choose an approver
outside an operator's maximum policy or approve its own action by choosing a
looser route.

Today the bundle chooses a tool's approval route in `runner/src/curie_runner/approval.py::ApprovalGate._record_pending`.
The worker carries that choice to creation and mints a connector grant on an
approved resume. Operator ownership of route bindings alone does not prevent
the bundle selecting another bound route or omitting a tool gate. The API
needs a bidirectional tool-to-selector route lock, and the connector needs
proof that the proxy admitted this exact call under a spent grant.

The rendering path in
`packages/plugin-format/src/plugin_format/connector_render.py` and
`apps/api/src/curie_api/routers/agents.py::read_version_connectors` exposes the
key-custody boundary. A selector signature protects against model/sandbox edits. It does not prove
that the operator controls the signing code. A bundle declares connector images
and secret references, and connectors can request keys from the agent's shared
Secret. The bundle can replace a registered connector's image or ask another
connector to mount its signing key. The mandatory operator ceiling and API-side
separation rule are therefore the controls limiting this delegated selector.

## Decision

### 1. Register a selector inside one operator ceiling

`@spec REQUEST-APPROVAL-1`

An operator route may declare `approver_ticket`: connector name, exact gated
upstream tool name, Ed25519 public key/key id, and a mandatory ceiling. Normalize
the tool to its connector plus exact upstream name, not a wildcard or a display
label. Registration/replacement requires an expected configuration version and
audit. Agent-owned policy/filter writes cannot alter this registration.

Refuse registration unless ADR 0270's `connector_requester_identity` flag is
on, and refuse turning it off while any selector remains registered. Validate
flag and route updates under the same configuration-version check and recheck
at creation. HTTP identity without this flag permits cross-human steering and
cannot bind the selector's requester to the person directing the live turn.

Any route with `approver_ticket` defaults `separation.exclude_requester` to
true. Only an explicit operator write of false opts out; audit that choice
under decision 6. This changes the security boundary because the selector's
bundle-influenceable code can select the requester inside the ceiling.

There is one ceiling kind: the agent's restricted HTTP approver role, optionally
narrowed by an explicit bounded set of canonical users. Do not add separate
IdP-group or team-id ceiling variants, or reinterpret legacy native user ids.
An absent, unrestricted, empty or unreadable ceiling is invalid. Pin the
operator-approved restricted role definition in the registration; current
identity facts evaluate it. An agent-owned role edit cannot silently widen that
pinned ceiling. Current agent role, retained team and route restrictions still
apply as additional narrowing. Widening the registered ceiling requires an
operator registration update.

The selector may choose any canonical people inside that ceiling. Its resource
lookup and selection policy live in its own README or docs. The platform
records no primary/backup policy, inactivity-selection rule or resource-specific
access tuple. Selection code remains bundle-influenceable. A compromised signer
can select any currently eligible ceiling member, but cannot override the
ceiling, original requester comparison or API-side separation policy. After
an explicit exclusion opt-out, requester self-selection inside the ceiling is
permitted; the signature alone supplies no separation guarantee.

Keep signing keys outside sandbox/runner/model inputs. This decision does not
claim custody against other bundle-declared connector code. A renderer refusal
to mount a registered key in another connector is optional hardening requiring
its own behavioral proof; it is not assumed by the security argument or a
prerequisite for this scope.

### 2. Lock the connector/tool to its registered route in both directions

`@spec REQUEST-APPROVAL-2`

If any route of an agent registers a selector for a connector/tool pair, both
API creation owners refuse an approval for that pair on any other route,
including a routeless approval. Reject conflicting registrations that would
make the pair's owning route ambiguous. Recheck the map at the final creation
commit. The registration applies across every binding of that agent.

Derive the lock key only from `granted_tool`, with the exact live-name parsing
used by `kernel/claim.py::_connector_tool_grant`: accept
`mcp__<connector>__<upstream-tool>`, split once on the first `__` after
`mcp__`, and exclude `mcp__plugin_` names. Preserve the upstream remainder
exactly. `gate_kind` is optional and never enables or bypasses the lock.
Reject selector registration unless the agent's serving bundle actually
gates that exact pair on the owning route, using the runner's effective
last-declaration-wins route map. Revalidate relevant bundle/deployment changes
and fail closed at creation on mismatch; do not infer a gate from registration
alone. This reads the existing plugin format without extending it.

Conversely, any top-level `approval_ticket` argument on a route without that
pair's registered selector refuses. The registered route refuses a missing
or invalid ticket and a different connector/tool. A caller cannot remove the
ticket, choose a looser route and obtain an ordinary grant for the same tool.
Legacy creation cannot create a provenance-free new-policy approval.

A tool invoked without any approval gets no verified-grant header and must
be refused by its connector's execution handler under decision 7. The route
lock and connector check close different paths; neither substitutes for the
other. The bundle can still choose routes for ordinary unrelated tools.

### 3. Sign a bounded canonical set and the actual argument digest

`@spec REQUEST-APPROVAL-3`

A registered selector issues `cat1.<payload>.<signature>`. Encode compact,
sorted-key, ASCII-escaped JSON in unpadded base64url; sign the exact ASCII
`cat1.<payload>` bytes with Ed25519. Use strict bounded parsing, rejecting
unknown/duplicate keys, non-finite numbers, malformed encoding and wrong types.
The complete `granted_arguments`, including the ticket, must fit existing
argument limits.

The payload binds these platform facts:

1. Version and purpose `approval_request`, tenant, agent id, connector,
   exact tool, named route and registered key id.
2. The original requester's canonical tenant/issuer/subject from ADR 0270.
   There is no caller epoch or active-lease lookup.
3. A random single-use nonce, issue time and preparation expiry at most five
   minutes after issue. Preparation expiry is distinct from approval expiry.
4. SHA-256 of actual execution arguments, excluding only the top-level
   `approval_ticket` metadata member. Use the production canonical-arguments
   contract and its numeric/Unicode rules on every reader.
5. A nonempty set of at most 16 distinct canonical approvers, each identified
   by tenant/issuer/subject. Optional candidate recipient accounts name their
   exact provider installation and namespace; the API verifies any account
   used for delivery against that canonical person.

Carry the opaque ticket in `granted_arguments.approval_ticket`. The runner
transports it through the existing permission gate; it does not parse or
select approvers. This defines no connector preparation-tool arguments,
resource ownership schema or downstream selection algorithm.

### 4. Verify once at creation and preserve immutable selection

`@spec REQUEST-APPROVAL-4`

Both creation owners use one verifier for the route lock, signature, scope,
key, freshness, requester, selected set and execution-argument digest. Compare
the requester with the API's verified original invocation/resume association,
not `Approval.author`. Require active canonical human facts and enforce the
registered ceiling. Candidate native accounts are not proof of canonical
association; verify them through the identity owner when used.

Consume the nonce, create the approval, store the original requester,
selector/key references, argument digest and selected set,
and audit in one Postgres transaction. Enforce nonce uniqueness within the
registered selector scope. An identical retry under the original dedupe key
returns the original receipt, including after preparation expiry. Different
arguments or tickets under that key conflict; reuse under another key refuses.
No nonce is consumed without the corresponding committed approval.

External lookups stay outside database transactions and identity fences.
Before commit, use existing identity mutation/final-reader ownership to
revalidate scope, canonical requester, key/config versions and provider
lifecycle. Include every immutable authorization-bearing addition in
`approval_record_digest` and the existing HTTP continuity seal. Version the internal
creation response to return persisted `granted_tool` and `granted_arguments`,
plus verified selection metadata needed by delivery. The worker renders these
returned stored values rather than trusting its original model summary.
Coordinate the record-digest version/migration with the existing sealing owner;
adding snapshot columns must not invalidate ordinary pending approvals. Keep
ordinary and dynamic policy modes explicit, without guessing from malformed data.

Changing selector source data never replaces selected people on a pending
request. A new selection needs a new approval. Preparation-ticket expiry after
accepted creation neither expires the approval nor invalidates its grant.

### 5. Apply the snapshot and current authority at answer and resume

`@spec REQUEST-APPROVAL-5`

An answer must satisfy the stored selected canonical set,
the currently registered ceiling, retained route/team constraints and the
current HTTP approver role. Recheck active human status, account association,
key registration and applicable policy. A missing route, revoked key,
unavailable lookup or malformed snapshot refuses. Widening current policy
cannot add someone outside the immutable stored selection. The creation
verifier already proved that selection lay inside the then-registered ceiling;
do not store a second creation-ceiling column or include one in the record
digest. The operator-pinned registration ceiling in decision 1 remains.

Enforce this in the shared route/authorizer composition, including
`_route_decision`'s migrated-role early return, final answer commit, approved
resume publication and current resume/grant checks. Dynamic selection is an
explicit narrowing even when legacy `approvers` is absent. Existing rows with
no snapshot retain their historical mode; enabling a route does not invent
canonical provenance for them.

ADR 0246 raw surface-account entries can match independently after a definitive
identity result. They cannot satisfy the canonical selected-person requirement:
an unresolved or inactive canonical person cannot answer a dynamic request
through a raw grant. This additional restriction does not rewrite ordinary
migrated-role semantics.

For the initial realization these routes require HTTP identity and registered
receivers. Dynamic selection, requester exclusion or an exemption makes every
binding able to raise that route HTTP-required under ADR 0259. Validate enabling
writes, and fail closed on invalid opted-in reads or legacy bypass attempts.

Operator principals cannot answer dynamic or separation-enforced routes,
regardless of an underlying approver set's `operator_eligible` value. An
operator token cannot establish the selected canonical human or an inequality
comparison. Refuse it before any set-specific shortcut at answer and approved
resume. Ordinary routes retain their existing operator eligibility;
administrative rejection/recovery is not an approval impersonating a human.

### 6. Separation of duties is explicit and canonical

`@spec REQUEST-APPROVAL-6`

A route may set `separation.exclude_requester: true`; it defaults to true
whenever `approver_ticket` is registered. Other routes retain ordinary
ADR 0106 self-confirmation unless they explicitly enable exclusion. A dynamic
route may opt out only through an explicit operator write of
`separation.exclude_requester: false`. Persist the effective choice; audit the
operator, configuration version and old/new values at opt-out and later edits.
Omission never disables exclusion, and agent-owned writes cannot opt out.

Keep optional `exempt_group`, identified by canonical issuer/stable group id,
for routes with exclusion enabled. An exemption without exclusion is invalid.

Compare the answering and original requesting canonical tenant/issuer/subject,
including across different channel accounts. Equality denies approval unless
the requester currently belongs to the exact configured exemption group.
An exemption skips only the inequality check. It does not select the requester,
change the argument digest, remove ceiling checks or waive active status.
No connector-specific rule determines when that exemption becomes useful.
An eligible rejection may stop the action without satisfying inequality.

One policy owner serves answer, final commit and approved resume/grant checks.
Missing canonical facts or unavailable exemption lookup refuses; a model's
admin/exemption flag grants nothing. Audit requester, actor, policy version,
equality outcome and any fresh exemption evidence/use, including refusals.

### 7. The proxy attests a spent grant directly to the execution tool

`@spec REQUEST-APPROVAL-7`

The API already stores `granted_arguments`. After an authorized approved resume,
the worker mints the existing `ccg` for that exact connector/tool and complete
argument object, ticket included. The proxy verifies signature, expiry, agent,
connector, tool and canonical arguments, then spends the grant once.

Strip every inbound `X-Curie-Grant-Verified`, including repeated and differently
cased forms. Only after successful verification and spend set one such header
whose value is the grant's canonical UUID `jti`. This is a grant id, not an
approval-row id or an `approved` argument. Never emit it on a grant-less call,
a failed spend, or a non-tool route. A request bearing that attestation has
exactly one `tools/call`; reject batches that could apply it to another call.

The runner attaches `X-Curie-Connector-Grant` as a static MCP header for the
sandbox lifetime, including on unrelated calls after approved resume and warm
reuse. Its presence alone must not require a spend on every call. For a tool
in `approval_required_tools`, preserve strict verification, exact matching and
single-use spend: missing, invalid, mismatched or spent grants refuse.

For an ungated tool, consider a presented grant for attestation only when its
signature verifies and its tool and canonical arguments match this call.
Then require the remaining expiry, agent and connector checks and a successful
spend before emitting `X-Curie-Grant-Verified`. A matching but expired or spent
grant refuses that attempted authorized execution. Otherwise strip the
incidental grant header and forward by ordinary rules without attestation;
an unrelated call must not fail or spend a grant because of the static header.
Never attach an attestation to a batch. The protected connector execution tool
refuses a missing attestation even if absent from the proxy's gated list.
The connector trusts the header only from its own proxy connection and uses
per-request context, as for ADR 0270's principal. Direct callers cannot forge it.

The connector checks its ticket's signature/scope/requester/argument digest
and its current downstream policy before the effect. It does not reapply the
five-minute preparation deadline to an accepted delayed approval. API answer
and resume checks enforce selected actor and separation policy; the connector
needs no result-read endpoint, signed read purpose or nonce-result lookup.
A spent grant is never replayable after an uncertain upstream outcome.

### 8. Render the stored arguments through the existing describer

`@spec REQUEST-APPROVAL-8`

The card describes the persisted `granted_tool` and `granted_arguments` through
the existing `describe_approval`/`approval_display` presentation path. Exclude
only opaque `approval_ticket` metadata from the human preview; it remains in
the stored, grant-bound object. A runner/model summary cannot replace these
stored values. Use the same stored values for settlement and reminder context.

Keep the describer's bounded preview behavior and explicit truncation notice;
do not claim it displays arbitrarily large arguments in full. No platform
resource/principal/access-level schema or second action projection is added.
Rendering supplies information, never authority or a replacement argument set.

## Realizing code paths and required evidence

The named proposed owners are:

1. `apps/api/src/curie_api/schemas/approvals.py::ApprovalRouteBinding`,
   `ApprovalRouteBindingOut`, `schemas/agents.py::AgentUpdate`,
   `routers/agents.py::update_agent` and
   `crud/agents.py::update_agent_approval_routes` own operator registration,
   route-map consistency, requester-flag checks, exclusion defaults/audited
   opt-out, expected-version writes and readback. New
   `approver_tickets.py::check_bundle_gate` reads the immutable serving bundle
   through `bundles.py::extract_stored_bundle` and the existing plugin parser.
   Extend `deploy.py::check_routes_from_bytes` and
   `check_approval_route_bindings`, used by `gitflow.py::process_push` and
   `routers/deployments.py::create_deployment`, to recheck gate ownership
   before changing the serving bundle or attaching its archive. These are proposed additions
   to validation, not existing gate/registration enforcement.
   `routers/agent_access.py::put_agent_access_policy` cannot rewrite the
   ceiling or exclusion choice. `models.py::Approval` and a new Alembic migration persist the
   snapshot/nonce constraint; migration numbering is allocated at implementation.
2. New `apps/api/src/curie_api/approver_tickets.py::verify_ticket` owns the
   shared verifier and `check_tool_route`. Both
   `routers/approvals.py::create_approval` and
   `identity/http_approval.py::create_http_approval` call it, with
   `crud/approvals.py::insert_approval`, `_same_approval`,
   `identity/http_approval.py::_route_snapshot`, `_RECORD_FIELDS` and
   `approval_record_digest` preserving atomic/idempotent creation and seals.
3. New `apps/api/src/curie_api/approval_selection.py::check_selected_approver`
   and `check_separation` serve
   `authorizer.py::authorize_approval`,
   `slack_approvers.py::SlackApproverSetSelector`,
   `identity/http_approval_answer.py::_route_decision`, `_Composition.verify`
   and `check_http_approval_actor`, and
   `identity/http_resume.py::_check_resume`. Extend
   `identity/http_invocation.py::binding_requires_http_identity` for the policy.
   `authorizer.py::authorize_approval` refuses operator principals on dynamic
   and separation-enforced routes before approver-set eligibility shortcuts.
4. `apps/worker/src/curie_worker/kernel/approval.py::_pause_for_approval`,
   `approvals.py::ApprovalClient` and
   `approval_wording.py::describe_approval`/`approval_display`, with the API's
   `approval_wording.py` sibling, render persisted arguments.
   `kernel/claim.py::_connector_tool_grant` retains exact-argument grant minting.
   `apps/worker/src/curie_connector_proxy/server.py::_grant_refused`,
   `_grant_spent`, `_forwardable` and `_handle` set the verified-grant header.
5. `cli/src/commands/approvals.rs::approvals` and `cli/src/api.rs` expose
   operator route JSON writes/readback. Regenerate `apps/api/openapi.json`,
   CLI mirrors and command manifests if their surfaces change, preserving
   console command parity. `packages/curie-internal/src/curie_internal/identity_approval.py`
   is the existing internal creation contract owner to version before readers.
6. Shared synthetic vectors extend `tests/vectors/action-canonical-arguments.json`
   and a new `tests/vectors/approver-ticket.json`. Selector signer and API
   verifier exercise the same encoding/digest corpus. Document the grant
   header in `docs/interfaces/connector-host/INTERFACE.md`. Connector signing,
   execution checks and selection policy belong to connector implementations.

At implementation record exact commands, candidate commit and outcomes:

1. Through the real HTTP approval creation boundary with Postgres and Valkey,
   accept a valid ticket; reject bad signatures, scope/requester/argument
   changes, expired preparation, nonce replay and conflicting dedupe. Through
   the legacy creation boundary, refuse new-policy approval creation even with
   an otherwise valid ticket, and verify ordinary legacy creation remains
   unchanged. Race identical creation and prove one row plus one consumed nonce,
   or neither on rollback. Refuse registration with requester transport off and
   disablement with a registration, including concurrent updates and invalid
   stored state.
2. Register the pair on one route, then choose a looser bound route or omit
   the route; observe refusal and no grant-backed effect. Also reject a ticket
   on an unregistered route, a missing ticket on the owning route, and a direct
   execution call without the verified-grant header.
   Repeat route bypasses with missing or misleading `gate_kind`; reject a
   registration whose effective bundle gate is missing or points elsewhere,
   and a serving-bundle change that breaks a registered gate.
3. Change selection source data after creation: selection stays fixed. Remove
   ceiling membership/key/route or canonical active status: answer and resume
   refuse. Include a migrated approver role without a legacy route block and
   a definitive raw-account match lacking the selected canonical human. Widen
   the registered ceiling and prove nobody outside the stored selected set
   becomes eligible; revocation of a selected member still refuses.
4. Exercise distinct humans, the same human through different channel aliases,
   exemption membership/removal/unavailability, and an exempt but unselected
   actor through answer and resume. Inspect the actual audit and effect.
   Omit separation on a dynamic route and observe requester exclusion; an
   explicit operator opt-out permits eligible self-approval and emits the
   policy-change audit. An agent-owned opt-out refuses. Refuse an operator
   principal on dynamic and static exclusion routes even with an explicit-user
   approver set. Drive allowed answers through a test HTTP identity provider
   and scripted registered-receiver answer proofs over the real API path,
   not CLI operator-principal resolution; retain real-provider qualification.
5. Through real worker/proxy/connector wiring, remove the tool from the proxy
   gated list: a grant-less call still has no effect, a valid spent grant emits
   its id and permits the approved arguments, and replay/forged headers/changed
   arguments/batched calls refuse. Test uncertain execution without a resend.
   Before and after spending the grant, call an unrelated ungated tool using
   the same boot headers, including on warm reuse: it succeeds without grant
   spend or attestation. A mismatched ungated call also forwards unattested;
   replay of the matching protected effect remains refused.
6. Render a stored argument set differing from the model summary and verify
   that the card uses stored values with existing preview limits. Run required
   live-provider/external-integration tiers for the connector path. Source,
   helper and fake-model tests alone do not qualify it.

No runtime acceptance evidence is claimed. Frozen ACI/plugin changes are not
part of this design; a required change must first get its own contract review.

## Consequences

1. Selection is stable while authority remains revocable. Source edits cannot
   move a pending request; a stranded approval requires rejection/recovery or
   a new approval, never silent substitution.
2. The mandatory operator ceiling limits bundle-influenceable signing code.
   A signature is not proof of operator-controlled image or exclusive key custody.
3. Dynamic routes exclude the requester by default; operators may explicitly
   opt out with an audit. A canonical group exemption waives only requester
   inequality and never selects a person or expands the ceiling.
4. Existing stored arguments and single-use grants are reused. There is no
   connector result-read API or additional signed-request purpose. A spent
   grant with an uncertain effect requires recovery rather than replay.

## Alternatives considered

1. Let the model name people or routes without a registered pair lock.
   Rejected because a looser route could turn review into self-confirmation.
2. Trust the selector key alone. Rejected because bundle-selected connector
   code and shared secret references do not establish exclusive key custody.
3. Standardize one connector's selection rule or action tuple. Rejected because
   the durable platform boundary is the canonical set plus argument digest.
4. Snapshot permanent permission. Rejected because revocation must still stop
   answers and resumed execution.
5. Add a connector result-read API. Rejected because stored arguments, current
   answer/resume checks and a proxy-attested spent grant provide the needed proof.

## ADR 0045 back-links on acceptance

Keep Accepted statuses and bodies. Add partial-amendment back-links after status:

1. ADR 0034 decision 2 and ADR 0123's no-snapshot clause: ADR 0271 stores selected
   people for dynamic routes while preserving current authority and missing-route refusal.
2. ADR 0220 decision 10 and ADR 0106's ordinary self-confirmation policy:
   ADR 0271 defaults dynamic routes to canonical separation, with an audited
   operator opt-out and narrow current-group exemptions.
3. ADR 0246 decision 3: ADR 0271 adds canonical selection/ceiling constraints
   that a raw surface-account grant cannot satisfy independently.
4. ADR 0259 decision 1: ADR 0271 adds route policy to HTTP-required classification.
5. ADR 0178 decisions 5 and 6: ADR 0271 adds the proxy-only spent-grant id and
   its connector trust rule.

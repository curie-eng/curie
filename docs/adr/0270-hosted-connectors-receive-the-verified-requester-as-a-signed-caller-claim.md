# 270. Hosted connectors receive the verified requester as a signed caller claim

Date: 2026-11-05

Status: Draft

This draft does not authorize implementation. Acceptance follows
[ADR 0085](0085-acceptance-not-implementation-authorizes-an-adr.md) and
[ADR 0102](0102-accepted-alongside-implementation-with-explicit-approval.md).
The realizing changes and acceptance checks below are proposed.

Partially supersedes [ADR 0178](0178-the-connector-caller-token-names-the-run-and-work-item.md)
decision 2's byte-identical non-work-item token promise for opted-in human
requests, decision 3's lifetime for requester-bearing tokens, decision 4's
closed claim set, and decision 5's header set. Decision 1's agent and run
semantics remain. Extends [ADR 0168](0168-one-installation-hosts-several-bot-identities.md)
decision 7 with optional requester attribution after agent admission;
`admits` continues to judge only the agent. These are partial amendments;
no ADR is superseded in full.

Consistent with [ADR 0088](0088-per-user-delegated-oauth-for-mcp.md)'s one-human
execution boundary. This decision transports identity and does not realize
that ADR's delegated credential store or gateway. It can be accepted without
ADRs 0271, 0272 or 0273.

Source inspection: `origin/next` at
`678a5665d8fe7f10e78fdb282aee8d9210b37021`, fetched on 2026-11-05.
This is source evidence, not runtime qualification.

## Context

The worker signs `cct` caller tokens outside the sandbox. The connector proxy
verifies them, strips the bearer, and supplies agent and optional work-item
identity headers. A connector learns which agent called but cannot identify
the human from a model-written argument or conversation text.

`QueuedTurn.author_principal` is provenance in the frozen ACI contract.
`TurnAuthorityResponseV1` currently returns a decision and checked scope,
without a canonical requester. HTTP invocation and approval-resume owners
already verify the original sender's canonical association. Those owners must
project it explicitly; signing queued provenance would not establish authority.

MCP headers bind at sandbox boot. Reusing a sandbox after its human requester
changes would reuse the previous person's identity. Session caller tokens
currently last 24 hours. `_cap_caller_token_to_deadline` applies to owned
factory WorkItemRuns, not ordinary session turns. The existing warm-sandbox
check compares state-token expiry, so it needs an explicit caller-token
comparison to support a shorter caller lifetime.

## Decision

### 1. Requester transport is an operator opt-in in HTTP identity mode

`@spec CONNECTOR-REQUESTER-1`

Add operator-owned agent setting `connector_requester_identity`, default
false. Enabling it requires HTTP identity selection and registered receivers
for human bindings that use this capability. Validate writes and fail closed
on malformed opted-in reads. Agent-owned access-filter writers cannot toggle
this setting. No plugin-format field is needed.

An `approver_ticket` selector registration under ADR 0271 requires this flag
to be on. Refuse registration while it is off, and refuse disabling it while
any such registration exists. All operator writers enforce the same invariant
with expected-version checks, including concurrent flag and route updates;
invalid stored combinations fail closed at use. HTTP identity mode alone is
insufficient because it does not prevent another human steering a live turn.

Only opted-in agents mint requester-bearing tokens and enforce the requester
handoff rule below. Having HTTP identity and a hosted connector alone does
not enable either behavior. A missing verified human association produces no
requester claim; human-required connector tools refuse it. A schedule, hook,
eval, machine turn or legacy request cannot acquire a human through an author
string, operator key or raw surface-account grant.

The API extends its versioned internal current-authority response with an
optional typed requester projection: `tenant_id`, canonical `issuer` and
`subject`, bound to the response's existing request/event/context/scope fields.
It returns that projection only after verifying the exact invocation or
resume and checking current authority. The worker checks those association
fields before using the projection. A resume retains the original requester,
not the approver or system-authored queue actor. A changed canonical
association refuses. Refused or unavailable checks carry no usable projection.

Publish the internal contract and migrate every API and worker reader before
minting. No reader guesses between versions. This changes
`packages/curie-internal`, not the frozen ACI or plugin format. A necessary
frozen-contract change must first land as its own reviewed contract PR.

### 2. The signed caller has an optional canonical principal

`@spec CONNECTOR-REQUESTER-2`

The closed `cct` payload gains optional `principal`, an object with exactly
`tenant_id`, `issuer` and `subject`. The tenant is a canonical lowercase UUID;
issuer and subject are bounded canonical strings compared exactly. There is
no epoch claim or active-epoch store. The principal is either complete or
absent. Null, partial, empty, duplicate-key, unknown-key and incorrectly typed
forms refuse. The whole token must fit the verifier's existing size bound;
refuse an oversized projection before boot.

The existing worker Ed25519 key signs the claim. `agent`, `exp` and the
optional `run`/`work_item` pair retain their meanings. A token without a
principal remains valid for agent-only tools. A human-required tool refuses
that token with a named missing-requester reason and never substitutes an
argument, previous request or service account.

Keep the `cct` prefix, signed encoding and existing vector bytes. Add
principal-bearing and malformed cases to the existing shared caller-token
and refusal corpora. The proxy accepts the new closed set before any worker
mints it; unsupported consumers refuse unknown claims.

`ccr` route tokens remain agent/run credentials without human authority,
with their existing 24-hour/factory-deadline lifetime calculated separately
from requester-bearing `cct`. Forged principal headers are stripped there too. Extending route authority
would require a separate amendment to
[ADR 0240](0240-a-hosted-connector-declares-the-routes-its-bundles-stdio-servers-call-through-the-caller-proxy.md).

### 3. Replace on requester change and bound copied credentials to one hour

`@spec CONNECTOR-REQUESTER-3`

For opted-in agents, record the trusted canonical requester, or its absence,
and caller-token expiry in the sandbox handle and durable route record. Compare
them beside `caller_run` in `_boots_differently`. Replace the sandbox before a
new turn with a different tenant/issuer/subject or a transition between human
and principal-less execution. Missing old requester metadata cannot prove a
match. Keep the existing cross-run replacement rule. Rehydrate durable history,
never credentials from history.

An active execution keeps one requester. Same-requester steering may continue.
Another requester waits for a subsequent execution at the existing durable
handoff boundary; it cannot enter the live runner as a steer. Enforce this
before the active-turn shortcut that currently suppresses replacement. Do not
terminate another person's in-flight action to switch identity. The setting's
disablement also requires a handoff before adopting a principal-bearing handle.

Requester-bearing `cct` expiry is at most mint time plus 3,600 seconds and is
also capped by the existing token cap and a known factory execution deadline.
Principal-less session tokens keep the existing 24-hour cap. This decision
does not claim ordinary sessions already have the factory deadline cap.
Factory deadline correction preserves the exact principal while shortening
expiry, and must never re-sign back to the longer lifetime.

Use the existing replacement mechanism when a warm token cannot cover the
next turn, adding the caller expiry comparison explicitly. Before starting a
turn, ensure its authorized runtime bound fits inside the requester token's
remaining lifetime. If a configured turn can exceed one hour, bound that
opted-in execution to the credential window or refuse before start; never
silently mint a longer token or refresh identity inside a live runner.
Approval resume checks current authority and boots with a fresh short-lived
credential for the original requester.

There is no Valkey caller-epoch lease, renewal or proxy lookup. Replacement
prevents the next sandbox from inheriting another person's bearer; it does
not revoke a copied token. A copied bearer can be replayed until its signed
expiry, for at most the remainder of one hour, even after handoff, suspension
or termination. This residual window is accepted. Expiry refuses later
admission and cannot cancel an upstream effect already in progress.

### 4. Only the proxy supplies the principal header

`@spec CONNECTOR-REQUESTER-4`

Strip all inbound `X-Curie-Principal` values, case-insensitively and including
repetitions, alongside the existing identity headers. After signature, expiry,
agent admission and any required tool-grant checks, set exactly one header
from the verified principal. Encode its three-field object as compact,
sorted-key UTF-8 JSON in unpadded base64url. Both readers use bounded strict
parsers. A principal-less token supplies no header. Never forward the bearer.

The header is a proxy-attested identity, not a portable credential. A connector
trusts it only on the connection from its own caller proxy under ADR 0178
decision 6. Direct-Service traffic cannot establish a human with that header.
Connector context owns the decoded principal per HTTP request; process globals,
MCP session ids and previous requests cannot supply it to another call.

### 5. A connector decides current downstream authority

`@spec CONNECTOR-REQUESTER-5`

The claim identifies the original human within the configured receiver and
identity-provider trust boundary. It carries no group membership, admin role,
entitlement or exemption. A human-restricted tool checks current downstream
policy for that exact tenant/issuer/subject before its effect. Unavailable
authority refuses. Model-written flags and names supply no authority.

For tools using
[ADR 0271](0271-per-request-approver-sets-and-separation-of-duties.md), the
connector requires the proxy-only verified-grant
header and uses the approval-bound arguments. That header carries the spent
grant's id; it does not require a connector result-read API. Requester identity
alone never proves an approval. Ordinary human-restricted tools need not adopt
the per-request approver contract.

## Realizing code paths and required evidence

These paths name proposed owners, not shipped implementation:

1. `apps/api/src/curie_api/schemas/agents.py::AgentUpdate`,
   `routers/agents.py::update_agent`, `models.py::Agent` and a new Alembic
   migration own the operator flag. `routers/agent_access.py::put_agent_access_policy`
   cannot change it. `routers/agents.py::update_agent` and
   `crud/agents.py::update_agent_approval_routes` validate the flag/selector
   invariant together with expected-version writes. `cli/src/api.rs` and
   `cli/src/commands/approvals.rs::approvals` expose operator configuration and
   JSON readback; regenerate API/client schemas and manifests as applicable.
2. `apps/api/src/curie_api/identity/http_invocation.py::check_http_turn_authority`
   and `turn_authority_response`, `identity/http_resume.py::_check_resume`, and
   `routers/identity_authority.py::check_turn_authority` supply the verified
   original requester. `packages/curie-internal/src/curie_internal/identity_authority.py::TurnAuthorityResponseV1`
   is the current contract to revise in a new version.
   `apps/worker/src/curie_worker/identity_authority.py::HttpTurnAuthorityClient.check`
   and `kernel/identity.py::check` consume and retain the checked projection.
3. `apps/worker/src/curie_worker/caller_token.py::mint`,
   `binding.py::BindingResolver`, `kernel/claim.py::_boots_differently`,
   `_route_and_start` and `_cap_caller_token_to_deadline`,
   `sandbox/types.py::SandboxHandle` and `RouteRecord`, and
   `sandbox/substrate.py::SandboxSubstrate` own minting and handoff. Preserve
   requester/expiry metadata through both `sandbox/docker.py` and
   `sandbox/k8s.py` adapters and route serialization.
4. `apps/worker/src/curie_connector_proxy/caller.py::decide` and
   `server.py::_forwardable` and `_handle` own verification and
   header projection. Update `docs/interfaces/connector-host/INTERFACE.md`
   with encoding, opt-in, transport trust and the accepted replay window.
   Connector implementations own request context and current policy checks.
5. `tests/vectors/connector-caller-token.json` and
   `tests/vectors/connector-caller-refusal.json` cover both production readers
   under the parity-seam registry. Existing agent-only vectors stay unchanged.

Record exact commands, candidate commit and observed outcomes at implementation:

1. Execute both production readers over shared positive and negative vectors,
   including tampering with each principal field and forged/repeated inbound
   headers on principal-less `cct` and `ccr` requests.
2. Through real API, worker, runner and proxy wiring, send two verified humans'
   requests on one thread. Observe correct connector principals, different-human
   steering deferred and sandbox replacement at handoff. Verify an agent with
   the flag off keeps ordinary collaboration. Exercise both substrate adapters.
   Refuse selector registration with the flag off and flag disablement while
   a selector exists; race those writes and prove no invalid combination can
   commit. Malformed stored combinations refuse before tool execution.
3. Capture an old token and demonstrate the declared limit: it can remain
   admitted before expiry and is refused after expiry. Use controlled clocks
   for expiry controls. No immediate-revocation claim is an acceptance criterion.
4. Exercise a warm handle whose state credential is sufficient but caller
   credential is too short, a same-requester thread idle past the one-hour
   caller expiry, approval resume by another approver, and factory deadline
   correction. Observe replacement/fresh minting and unchanged
   original-requester attribution; a turn longer than the cap cannot start.
5. Drive an actual human-restricted connector effect with real identity facts:
   allowed human succeeds, another human and revoked membership refuse, and
   a forged model argument or direct header cannot create authority. Run the
   required live-provider and external-integration tiers for the changed path.

No runtime acceptance evidence is claimed here. Lower-tier or source evidence
cannot qualify provider identity or connector enforcement.

## Consequences

1. HTTP-identity agents can opt into verified requester attribution for hosted
   connectors. The requester-attribution need is met only in HTTP identity mode;
   disabled/local-projection identity modes gain no equivalent claim.
2. Requester changes cost replacement and serialize cross-author steering for
   opted-in agents. The one-hour token also forces a cold sandbox replacement
   after a thread is idle about an hour, even for the same requester; a
   shorter remaining window can require replacement sooner to cover a turn.
   Other agents keep their current collaboration behavior.
3. A copied token remains usable for the remainder of its one-hour expiry.
   There is no additional caller-lease store or store-outage failure mode.
4. Opting in exposes canonical subjects to the agent's admitted hosted
   connectors. Tokens and real principal values must stay out of logs, public
   examples and evidence. Send no profile or membership list.
5. Contract and verifier rollout precede minting. Rolling back minting makes
   human-required tools fail closed.

## Alternatives considered

1. Trust queued provenance or a model-supplied requester. Rejected because
   neither is the API's current verified human association.
2. Mutate MCP headers in a live runner. Rejected because boot-time connections
   have no trusted identity-update path; replacement uses existing ownership.
3. Add a Valkey active-epoch lease. Rejected in favor of replacement and a
   one-hour expiry. It adds a revocation store and a custody boundary absent
   from the present connector deployment; immediate revocation is not promised.
4. Retain the 24-hour human bearer. Rejected because replacement alone would
   leave that much copied-token exposure.
5. Forward the bearer or put entitlements inside it. Rejected because that
   exposes a reusable credential or leaves downstream permissions stale.

## ADR 0045 back-links on acceptance

Keep prior bodies and Accepted status. Add a back-link immediately after each
status, identifying the partial amendment:

1. ADR 0178: ADR 0270 changes decision 2's non-work-item byte promise,
   decision 3's requester-token lifetime, decision 4's closed claims and
   decision 5's identity-header projection, only for the opted-in capability.
2. ADR 0168: ADR 0270 extends decision 7 with requester attribution after
   agent-only admission; the admission list itself is unchanged.

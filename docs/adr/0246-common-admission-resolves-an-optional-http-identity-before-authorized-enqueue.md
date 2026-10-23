# 246. Common admission resolves an optional HTTP identity before authorized enqueue

Date: 2026-10-23

Status: Accepted

Accepted 2026-10-23 with explicit maintainer approval from Junwon Jung,
including agent-owned role filters and retained channel/approval-route narrowing.
Tracked in [#4890](https://github.com/curie-eng/curie/issues/4890).

Acceptance authorizes this architecture and the behavior-preserving shared
admission extraction. Code that depends on new tenant-mode, HTTP, filter or
migration interfaces waits for their published versioned contracts. The
existing policy and transport behavior remains until its explicit migration;
an extracted composition alone does not complete the gateway.

## Context

[ADR 0220](0220-one-identity-provider-decides-who-may-invoke-and-approve-on-every-channel.md)
provides directory projections, agent invocation and approver teams, and
identity links. Its implementation is reusable: directory ingestion is in
`apps/api/src/curie_api/routers/directory.py`, role membership is checked in
`apps/api/src/curie_api/team_authorization.py`, and approval actors are checked
in `apps/api/src/curie_api/approval_teams.py`.

There are two admission compositions. The Slack dispatcher asks
`POST /channels/admission` and subsequently enqueues its own turn. HTTP channel
adapters use `POST /channels/turns`, which admits and enqueues in the API.
Both handlers are in `apps/api/src/curie_api/routers/channels.py`. Sharing
the list predicate in `apps/api/src/curie_api/admission.py` does not make
those enqueue boundaries the same.

An external identity service needs a stable place to resolve an authenticated
sender before authorization. It must not supply authorization decisions,
replace trusted transport evidence, or add vendor-specific directory code to
the platform. A request-time service also introduces failure and freshness
behavior that the projection model deliberately avoids. This decision makes
that behavior opt-in and preserves existing policy semantics until migration.

## Decision

### 1. The API owns admission and authorized enqueue

Channel transports authenticate receipt and deliver replies. Trusted
installation and credential scope determine the tenant, issuer, binding and
sender namespace. A single authenticated sender feeds identity resolution,
caller authorization and audit. A display author, message body or second actor
field cannot replace that sender.

The API composes resolution, policy evaluation and authorized enqueue through
one shared admission service. Slack migrates onto that boundary; an earlier
behavior-preserving extraction may land separately, but does not complete the
gateway. Denied invocation creates no delivery claim, reply placeholder,
queued work or sandbox. An accepted account-connection flow is a separate
interaction with its own authority.

External I/O runs outside database transactions and identity fences. After the
lookup the API revalidates provider configuration, binding generation, tenant
and agent activity, and policy version before committing enqueue or an
approval answer. Actor-authorized undo and recovery commits obey the same
post-lookup rule. A change invalidates the decision; external lookups never
run while holding those commit transactions or fences. A separate allow
response cannot authorize an unchecked enqueue later.

Slack's Socket Mode ownership, rolling handoff, duplicate delivery and reply
placeholder lifecycle remain explicit migration requirements. A second
socket owner is not introduced as an incidental part of resolution.

### 2. An optional HTTP service returns identity facts

A tenant selects one identity authority mode: disabled, existing local
projection, or HTTP. Existing installations retain their current mode. HTTP
mode uses a server-configured endpoint and its own random credential, separate
from the platform API key. Neither an inbound message nor a provider result
can select a different endpoint or scope.

The versioned request carries a request identifier, purpose (invocation or
approval), and the authenticated, server-scoped sender. The result identifies
the same scope and a canonical issuer/subject, principal kind, account status,
stable issuer-scoped group identifiers and observation metadata. Distinct
outcomes cover unresolved, inactive and unavailable subjects. The service
receives no message text and returns no allow/deny policy verdict.

Initial transport bounds are a two-second total lookup deadline,
no automatic retry inside the request, a 64 KiB response limit and no
redirects. Unsupported versions, contradictory results, scope mismatches,
unavailable endpoints and invalid responses refuse the operation. They never
silently select legacy authorization. Configured HTTP mode cannot be skipped
by an open-route shortcut or a stale admission allow.

The core keeps no positive identity cache in this mode. Providers declare and
qualify their backing-directory freshness and invalidation behavior. An HTTP
round trip alone does not establish that a suspension or group removal is
already visible. The versioned contract must bound identity and membership
staleness separately before support is claimed.

The HTTP contract, OpenAPI description and synthetic conformance vectors are
public. Provider implementations remain independent. Provider credentials and
configuration have explicit rotation, invalidation and failure handling.

### 3. New filters are versioned and opt-in

Existing policies keep ADR 0220's agent-wide teams and AND narrowing by
binding/route lists. Existing `allowed_callers` writes are not reinterpreted:
null remains open and an empty array remains an invalid write.

New policy configuration uses a separately versioned filter for each role.
Entries identify exact surface accounts, issuer-scoped subjects or
issuer-scoped groups. A populated filter matches any listed entry; null is
unrestricted and an explicit empty filter matches nobody. Invocation and
approval roles do not grant each other permission. Scope and global denials
are applied before matching.

Each new caller or approver filter belongs to the agent's versioned access
policy and applies across its supported channels. Any-match is confined to
entries within that role's filter. Existing binding caller lists, explicit
approval-route restrictions and agent-team restrictions continue to narrow
access until an explicit, versioned migration changes the applicable
restriction. Enabling HTTP resolution or adding a filter never clears them
implicitly. Ambiguous coexistence is rejected. Missing new configuration
retains existing behavior; new null does not clear a legacy restriction.
Migration uses expected policy versions and preserves the other role.
Old-client writes cannot reset or reinterpret a migrated role silently.

The any-match rule makes an exact surface-account entry an
independent grant after a definitive identity result. Removing or suspending
an IdP account therefore does not revoke that raw entry. Operators must remove
the raw entry too. This tradeoff is exposed in validation and readback and
is retained only for an explicitly migrated role. A retained legacy team or
route constraint can still refuse that caller. Provider unavailability is not a
definitive identity result and never falls through to the raw entry. Inactive tenants or
agents, invalid scope and forbidden principal kinds remain global denials.

HTTP mode does not add a new account-linking workflow. Existing projection
connection flows remain until a separately accepted migration changes them.
Unproven surface-to-directory matching is not repaired by trusting display
names or arbitrary profile email addresses.

### 4. Provenance and current authority follow the turn

[#4868](https://github.com/curie-eng/curie/issues/4868) adds initiating-principal
provenance to the frozen ACI contract as a separate backward-compatible
change. Producers and consumers depend on it only after that contract lands.
The initiating principal is distinct from the executing agent; a principal
reference grants no authority.

The admission context retained for a logical turn includes authenticated
sender, binding and generation, credential scope, and resolution and policy
versions. Worker claim, resume and later privileged actions recheck current
authority through a supported core contract. They do not import API internals.
The migration must specify handling of old work without provenance. Rechecks
do not promise to interrupt an action already executing.

Approval resolves the answering actor independently and checks current
approval authority, including for pending requests after membership changes.
Undo and recovery use that actor authority rather than invocation permission.

Principal kind is assigned through an explicit operator-configured authority,
not sign-in order or first observation. Human, service and agent principals
are distinct. Non-human invocation is denied until its own policy is
qualified; service and agent identities never resolve human-gated approvals.
Enabling channel-based agent invocation additionally requires an atomic,
deduplicated, conversation-scoped loop bound based on authenticated kind.
Unknown senders and replayed turns cannot reset it. Machine-token ingress and
hook/schedule authorization remain separate contracts.

## Realization and acceptance evidence

[#4890](https://github.com/curie-eng/curie/issues/4890) tracks these realizing
paths, none of which this architecture record claims implemented:

1. A shared API admission service, delegated to by the existing channel entry
   points, and an optional HTTP resolver with versioned configuration.
2. Slack adapter convergence onto API-owned enqueue, with rolling ownership,
   denied-delivery side effects and deduplication tested through the transport.
3. Versioned caller and approver filters, operator validation/readback and an
   explicit legacy-policy migration.
4. Turn provenance production, admission-context retention and worker checks
   through the approved boundary, after #4868.
5. Approval, undo and recovery checks for current answering-actor authority,
   principal-kind denials and any enabled agent-invocation loop guard.

Contract tests must include valid and deliberately broken independent
providers, scope and credential isolation, response bounds, outage refusal,
configuration and policy races, including approver-policy or provider
configuration changes during an in-flight approval lookup, and disabled-mode
compatibility. Runtime
qualification must demonstrate a permitted member's delivered answer and a
non-member's refusal before side effects on an owned disposable installation.
Membership removal, account suspension, pending approvals, upgrades and every
claimed channel are separate cases. Synthetic conformance is not real
directory mapping or freshness qualification.

## Staged contracts and partial supersession

The HTTP request/response, tenant mode/configuration lifecycle, freshness and
role migration schemas are published before implementation depends on them.
The migration contract resolves or rejects incompatible authority-mode and
legacy-team combinations; a mode change never converts or drops a team.
Provenance production/rechecks additionally depend on #4868 and their supported
core contract, including explicit treatment of old queued work.

After Slack migrates to API-owned enqueue, every enqueue needs current API
authority. A cached or stale allow is only a hint and never authorizes an
offline enqueue. Configured HTTP resolution cannot be skipped by cached/open
route answers. Existing caches and dispatcher enqueue remain unchanged during
the behavior-preserving extraction and on unmigrated paths.

This ADR supersedes only the following historical clauses in the stated scope:

- **ADR 0175 decision 2, Slack and Caching, and decision 7's first paragraph:**
  migrated Slack uses one authenticated sender and API-owned authorized enqueue;
  neither the earlier allow nor the five-minute stale-allow path authorizes it.
  Existing multi-ID callers/cache behavior remains during extraction.
- **ADR 0175 decision 6:** the complete capability changes Slack's enqueue
  composition; new filters do not reinterpret legacy list entries as links.
  Decision 1's existing null/open and invalid-empty writes remain unchanged.
- **ADR 0198 decision 7, step 7 only:** HTTP mode uses scoped canonical provider
  facts instead of requiring a local link as its identity source. The migrated
  raw-entry exception is explicit. Steps 1 to 6, reviewed sender derivation,
  namespace/installation lifecycle and local link trust rules remain binding.
- **ADR 0220's opening Decision and decision 1's projection-only choice:** a
  tenant can explicitly select HTTP facts instead. Read-only projection,
  complete-snapshot and non-expiring membership semantics remain for local mode;
  HTTP adds no administrator-created identity mappings.
- **ADR 0220 decision 3 step 1, decision 9 step 1 and decision 4's local-link
  storage requirement:** HTTP uses its defined outcomes without a new connect
  flow or fallback. Existing projection connection flows remain. An HTTP email
  realization must separately qualify mailbox facts and sender authenticity.
- **ADR 0220 decision 7's final inactive-person bullet, decision 9 step 1's
  inactive-person refusal and decision 10's first active-team requirement:**
  only an expressly migrated role gains the disclosed definitive-result raw
  grant exception. Tenant, agent, scope, kind and outage denials remain.
- **ADR 0220 decision 8's team-only role fields, decision 9 step 2 and decision
  10's team-only mechanism:** an explicit per-role migration can replace its
  former team with the versioned agent-owned filter. Decision 9 step 3's AND
  narrowing and explicit approval-route narrowing are not superseded.
- **ADR 0220 decision 11's retained TTL/API-outage behavior and local cache
  tuple:** HTTP has no positive core identity cache; migrated enqueue requires
  current API authority and the published config/policy version contract.
  Legacy local versions/fences and extraction-stage caches remain unchanged.

These ADRs remain Accepted in all other respects. Their bodies stay immutable;
only the corresponding partial-supersession backlinks are added.

## Consequences

The platform owns one authorization boundary and independent services can
implement directory lookup without importing platform internals. HTTP-mode
availability affects the corresponding operations, and raw grants carry an
explicit revocation tradeoff. Transport migration and current-authority
rechecks are real implementation work, not consequences established by
adding a resolver interface.

The common capability is complete only when the accepted paths and safeguards
are qualified together. Extraction, contracts, real-provider qualification,
artifact publication and installation activation are reported separately.
Enterprise packaging, vendor implementations and production activation are
not authorized by accepting this architecture alone.

## Alternatives considered

1. **Projection only.** Retains ADR 0220's availability and rate-limit tradeoff,
   but does not provide a synchronous external resolver. It remains a supported
   mode, rather than being silently replaced.
2. **A resolver inside each transport.** Duplicates authorization composition
   and allows different sender or outage semantics across surfaces.
3. **Let a front door return an allow verdict.** Moves authorization out of the
   platform and lets provider-specific decisions bypass core safeguards.
4. **Treat an earlier admission allow as enqueue authority.** Leaves a policy
   or binding change between decision and enqueue unchecked.
5. **Replace existing policies with any-match filters in place.** Widens
   legacy team/list combinations and changes empty-list behavior without opt-in.
6. **Fall back after provider failure.** Converts an outage or malformed result
   into an authorization bypass; operator-selected disabled mode is explicit
   configuration, not an error path.

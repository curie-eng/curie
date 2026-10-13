# 220. One identity provider decides who may invoke and approve, on every channel

Date: 2026-10-13

Status: Draft

This ADR builds on
[ADR 0166](0166-tenant-boundary-and-principal-identity-land-together.md),
[ADR 0175](0175-a-bot-may-limit-who-can-talk-to-it.md),
[ADR 0034](0034-approval-authorizers-resolve-membership-in-the-api.md),
[ADR 0106](0106-an-approver-is-an-authenticated-principal.md),
[ADR 0123](0123-a-pending-approvals-approver-set-does-not-follow-its-route-binding.md),
[ADR 0177](0177-an-approval-is-answered-where-it-was-asked-including-by-email.md)
and
[ADR 0198](0198-identity-links-live-in-provider-identity-namespaces.md).

If accepted, it supersedes in part:

1. ADR 0177 decision 3 (on any channel other than Slack, only the requester may
   answer), for agents that name an approver team. ADR 0177 marks that rule as
   an interim to be replaced once approvers are principals linked on every
   channel. This ADR is that replacement. ADR 0177's other decisions stand.

It also answers ADR 0198 open question 4 (mailbox-level evidence for email)
for addresses an identity provider governs, and realizes ADR 0198 decision 8
(link creation) as concrete flows. ADR 0198's tables, resolution order and
other decisions stand. ADR 0177 and ADR 0198 are Accepted and this Draft does
not edit them; back links wait for this ADR's acceptance, as
[ADR 0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md)
requires.

Tracking issue: #4273.

## Terms

- **Identity provider (IdP)**: the tenant's directory and sign-in service, such
  as Okta or Entra ID. It is the source of truth for people and groups
  ([ADR 0166](0166-tenant-boundary-and-principal-identity-land-together.md)
  decision 2).
- **Person**: a `principals` row, keyed by the IdP's `(issuer, subject)`.
- **Team**: a `teams` row. An `idp_group` team mirrors one IdP group.
- **Channel account**: who someone is on one channel: a Slack user id, an email
  address, a Teams user id, and so on.
- **Link**: an `identity_links` row that binds one channel account to one person
  ([ADR 0198](0198-identity-links-live-in-provider-identity-namespaces.md)
  decision 5).
- **Directory adapter**: the external integration that reads the IdP's groups
  and delivers team membership to Curie.
- **Invocation team** / **approver team**: the teams whose members may start a
  turn with an agent, and resolve its approvals.

## Context

A company that uses an IdP already decides who belongs to which team in one
place. Curie ignores that and asks operators to repeat it per channel, in
provider-native ids:

1. **Invocation.** [ADR 0175](0175-a-bot-may-limit-who-can-talk-to-it.md) gives
   each binding an optional `allowed_callers` list of Slack ids or email
   addresses. NULL means open, an empty list is refused, and a list holds at
   most 100 ids. A binding added later starts open.
2. **Approval.** A route's `approvers` block names Slack users or a Slack user
   group, or falls back to members of the card's channel
   ([ADR 0034](0034-approval-authorizers-resolve-membership-in-the-api.md)). On
   email and every other adapter channel, only the requester may answer
   ([ADR 0177](0177-an-approval-is-answered-where-it-was-asked-including-by-email.md)
   decision 3).

So the same person is written down once per channel, removals do not propagate,
groups of zero or more than 100 cannot be expressed, and a person who is an
approver in Slack cannot approve by email at all.

The pieces for one answer are half built. `principals` (with `status` and
`authorization_version`), `teams` and `principal_teams` exist on `next`. Generic
OIDC sign-in exists (#2908). ADR 0198 decides how a channel account is linked
to a person, and says link creation is the trust boundary, but no link table,
resolver or creation flow is implemented, and email is explicitly left
unresolvable.

Maintainer review of #4273 set four constraints that this ADR keeps:

- invocation and approval are one mechanism with different teams;
- no short wall-clock expiry on membership, because one sync outage would then
  refuse every caller of the bot; directory sync keeps the last known state;
- a broken sync must still be visible, for example as the time it last synced;
- immediate deactivation of a person stays a separate mechanism.

## Decision

**The tenant's identity provider is the only source of who a person is and which
teams they belong to. Every channel account reaches Curie's policy only through
a link to that person, and a link is created only with the identity provider's
proof. An agent names an invocation team and an approver team, and both apply on
every channel the agent is reachable through.**

```
                   IdP (people, groups, account status)
                     │                         │
          sign-in (OIDC)                 directory adapter
                     │                         │
          links: channel account → person   teams: person ∈ team
                     │                         │
   Slack / email / other channel ──► person ──► agent's invocation team
                                             └► agent's approver team
```

### 1. One identity provider per tenant

- A tenant configures one IdP issuer for people. People are `principals` keyed
  by that issuer and the IdP subject, as today. Email and display name are
  attributes, never keys.
- Teams that policies reference have `source = idp_group` and the IdP group id
  as `external_id`.
- A tenant also declares which email domains its IdP governs. Section 3 relies
  on that list.

### 2. Chat accounts are linked by signing in to the IdP from the channel

This realizes ADR 0198 decision 8's `provider_event_verified` source for every
chat provider that has an ADR 0198 decision 6 realization (Slack today).

1. The person sends a link request to any agent on the channel, for example
   `link`. The channel's own authentication identifies the sender, and ADR 0198
   decision 6 derives the sender's namespace and native id.
2. Curie stores a challenge: a random token (hashed at rest), the namespace and
   native id, a 10 minute expiry, and single use.
3. The agent sends the token as a URL only the sender can see (an ephemeral or
   direct reply).
4. Opening the URL requires IdP sign-in. A confirmation page names the channel
   account being linked ("Slack user @name in workspace W"). On confirm, Curie
   consumes the challenge and creates the link to the signed-in person, in one
   transaction, with an audit row.
5. Curie tells the channel account that it was linked.

A challenge that is expired, consumed or for another tenant is refused.
Requests are rate limited per channel account.

### 3. Email addresses are linked by the identity provider's attestation

This answers ADR 0198 open question 4 for IdP-governed addresses.

1. When a person signs in through the IdP, Curie reads the ID token's `email`
   claim. When `email_verified` is true and the address's domain is one the
   tenant's IdP governs (section 1), Curie creates or keeps a link from that
   address to the person, with source `idp_attested`.
2. An inbound email resolves to a person only when both hold: the adapter
   authenticated the sender (aligned DMARC, as today), and an active link exists
   for the exact address. DMARC proves the domain. The IdP's attestation proves
   which person holds the mailbox in that domain.
3. **Reassignment.** The IdP is authoritative over its domains. When a sign-in
   attests an address that is actively linked to a different person, Curie
   revokes the older link and links the new person. When a person's sign-in
   attests a different address than before, their old address link is revoked.
   Deactivating a person (section 6) stops every link to them from resolving.
4. Addresses in domains the IdP does not govern are never linked this way. They
   can be linked by an administrator (section 4) or stay on exact-address
   `allowed_callers` lists.

### 4. Administrators may map accounts by hand, with audit

ADR 0198's `admin_mapped` source stays available for accounts that cannot sign
in to the IdP from the channel, and for initial rollout. Every such link records
the acting administrator, and status reads show its source.

### 5. Other channels follow the same two rules

A new chat provider joins by supplying its ADR 0198 decision 6 realization;
section 2 then applies unchanged. A provider without one cannot resolve senders,
so its callers and approvers are refused on agents that name a team. No
provider gets its own team, list or approver mechanism.

### 6. Account status is immediate

A person's `status` is separate from team membership.

- An authoritative status change (suspend, disable, revoke, reactivate) names
  the person by `(issuer, subject)` and the version it expects. It bumps
  `authorization_version` and writes an audit row.
- An older reactivation cannot undo a newer suspension.
- A person who is not active is refused for invocation and approval on every
  agent and every channel from the next decision on, without waiting for any
  sync.

### 7. Team membership is a complete snapshot

A directory adapter replaces a team's whole membership in one transaction.

- A team gains `epoch`, `member_count`, `digest`, `last_synced_at`,
  `last_attempt_at`, `last_error` and `managed_by`.
- A snapshot names the epoch it expects. A snapshot from an older run is
  refused, so a slow run cannot restore members a newer run removed.
- A partial or invalid snapshot is refused. The attempt updates
  `last_attempt_at` and `last_error` only.
- An empty complete snapshot is valid and admits nobody. A team that has never
  received a complete snapshot also admits nobody.
- Each applied snapshot writes an audit row with epoch, member count and digest.

### 8. Sync health is reported, not enforced

A projection never expires. A team whose adapter stopped keeps the membership it
last received.

- Status reads show each adapter-managed team's `last_synced_at`,
  `last_attempt_at`, `last_error`, `epoch`, `member_count` and `digest`.
- The platform exports each team's sync age and failed-attempt count so
  operators can alert on a stalled adapter.
- These signals never change an authorization decision.

### 9. An agent names two teams, and they apply everywhere

An agent gains an access policy with two optional team references,
`invocation_team_id` and `approver_team_id`, and a version that writes must
match.

- The policy belongs to the agent, not to a binding, so it covers every channel
  the agent listens on, including bindings added later.
- Each role is decided only by its own team. Neither team grants the other role.
  The two may be the same team if an operator chooses; nothing derives one from
  the other.
- A team in another tenant is refused.
- A role with no team keeps today's behavior for that role.

### 10. Invocation: one check on every channel

The invocation team is enforced in ADR 0175's single `admit` function, which
ADR 0175 decision 6 reserves for this principal check.

1. Resolve the sender to a person through ADR 0198's resolution order and the
   links above. An unlinked account, a link in another tenant, or an inactive
   person is refused.
2. Admit only members of the invocation team's current projection.
3. When a binding also carries `allowed_callers`, require both. A list can
   narrow the team, never widen it.
4. Refuse before a claim, placeholder, queued turn or sandbox, and answer a
   refused caller with nothing, as ADR 0175 decision 3 requires.
5. The dispatcher's open-route and install-wide open shortcuts do not apply to
   an agent with an invocation team.

Signed hooks, schedules, work items and bot handoffs keep their own
authorization. A human team never grants a machine caller anything.

### 11. Approval: one approver set on every channel

An approver team becomes one `ApproverSet`
([ADR 0034](0034-approval-authorizers-resolve-membership-in-the-api.md)
decision 4), evaluated when the approval is answered (ADR 0034 decision 2).

- Whoever answers is resolved to a person the same way as in section 10: a Slack
  click through its Slack link, an email reply through its address link, a
  console action through the console session's IdP sign-in, and any other
  adapter through its provider's link. That person must be active and in the
  approver team's current projection.
- For an agent with an approver team, this replaces ADR 0177 decision 3's
  requester-only rule. Any approver-team member who can see the card may answer
  it on that channel. Where the card is shown is unchanged (ADR 0177 decisions 1
  and 2).
- Every route of the agent uses the team. A route's own `approvers` block may
  still narrow it. The channel-membership default does not apply.
- Removing someone from the approver team stops them from resolving approvals
  that are still pending.
- A requester who is also on the approver team may approve, as
  [ADR 0106](0106-an-approver-is-an-authenticated-principal.md) allows.
- The approval audit row records the team, its epoch and its digest, in new
  columns. The frozen `authorizer` vocabulary is not redefined.
- An agent with no approver team keeps every existing set, including ADR 0123's
  unbound-route refusal and ADR 0177's requester-only set.

### 12. Caches follow versions, not clocks

Cached admission answers are valid only for the team epoch, policy version,
person `authorization_version` and link they were computed from. A newer value
of any of them invalidates the answer. ADR 0175's cache TTL and its behavior
while the API is unreachable are otherwise unchanged.

### 13. Each credential does one thing

- The directory adapter writes snapshots and sync attempts only for teams whose
  `managed_by` names it, in its own tenant. It cannot create or revoke links,
  change policies, resolve approvals, or touch other teams.
- Only sections 2, 3 and 4 create links.
- Handing a team from an adapter to manual management is an explicit, audited
  operation.

### 14. Operators use `curie`

Admin API routes and `curie cluster` subcommands, each with dry run and JSON
output, cover: the tenant's IdP issuer and governed domains, identity
namespaces, link listing and revocation, team status, agent policies, and team
ownership handoff. Exact names are settled in implementation review.

### 15. Rollout

A policy enforces from its first release, as in ADR 0175 decision 7, because it
applies only to agents an operator configures. An operator can start with
administrator-mapped links (section 4) and move people to self-service linking
(sections 2 and 3) without changing the policy.

## Consequences

1. One place, the IdP, decides who may use and approve for an agent, on Slack,
   email and any channel that later gains a resolver. Leaving an IdP group or
   being deactivated there takes effect on every channel.
2. Each person links each chat account once, by signing in to the IdP. Email
   addresses in IdP-governed domains link automatically at that sign-in.
3. Approvers on email and other adapter channels can answer, which ADR 0177
   could not offer.
4. A stalled directory adapter does not take a bot down; removals stop reaching
   Curie until it is fixed, the sync-age signal shows it, and section 6 covers
   urgent removals.
5. This ADR depends on implementing ADR 0198's links and resolver (#2910,
   #2914), identity audit (#2912, #2913) and tenant scoping (#2911). Enforcement
   cannot ship before them. Team projection, sync status, sign-in linking and
   administration can.
6. The IdP must offer OIDC sign-in with a verified `email` claim, and a way for
   the directory adapter to read group membership. Provider-specific adapters
   stay outside the open source platform (ADR 0166 consequence 3).
7. New schema: projection state on `teams`, the tenant's IdP configuration and
   governed domains, link challenges, and agent access policies. Migrations use
   the number the base allocates. No release version changes because of this
   ADR.
8. Channel visibility is not covered. Refusing a caller does not hide history or
   retract delivered output.

## Alternatives considered

1. **Separate ADRs for team policy, chat linking and email linking.** Rejected.
   The point is one identity source for every channel. Splitting it would let one
   channel ship on a different rule.
2. **A wall-clock authorization lease on team membership.** Rejected. One
   adapter outage would refuse every caller of every bot using the team.
3. **Per-channel teams or lists.** Rejected. It is the current problem.
4. **Match channel accounts to people by profile email or display name.**
   Rejected, as ADR 0166 requires. A chat profile's email is often user-editable
   and is not always visible for external users.
5. **Let the directory adapter create links from the directory's email field.**
   Rejected. It would give the sync credential the power to decide who is who.
   Sign-in attestation (section 3) needs no extra credential.
6. **Trust DMARC alone for email.** Rejected. DMARC authenticates a domain, not
   a mailbox (ADR 0198).
7. **Resolve groups live from the IdP on each request.** Rejected. It puts the
   IdP on every turn's path and makes its outages Curie outages.
8. **Derive the approver team from the invocation team.** Rejected. Requesters
   and approvers are usually different people.

## Open questions

1. Notifying approvers on a channel other than the card's. This ADR lets any
   approver-team member answer where the card is shown. Delivering the card to
   approvers wherever they work is a separate decision.
2. Slack Connect payloads that omit the sender's team (ADR 0198 open question 2)
   cannot resolve until that question is settled.
3. Whether an IdP's `email_verified` claim is sufficient for every IdP, or an
   operator must opt in per IdP.

## Reference

- Issue: #4273
- Related: #2907, #2908, #2910, #2911, #2912, #2913, #2914, #3885, #4246, #3063

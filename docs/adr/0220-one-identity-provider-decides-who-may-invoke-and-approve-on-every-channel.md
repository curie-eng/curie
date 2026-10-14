# 220. One identity provider decides who may invoke and approve, on every channel

Date: 2026-10-13

Status: Accepted

Accepted 2026-10-14 with explicit maintainer approval from Brian Conn, recorded
as an approving review on the publishing pull request
([#4513](https://github.com/curie-eng/curie/pull/4513)). The review thread on
that pull request settled four points, and this text carries them:

1. The IdP's own knowledge of a person's channel accounts is the first path.
2. A one-time connect button covers only accounts the IdP does not know.
3. There are no administrator-made mappings.
4. People are a read-only projection of the IdP, not a second source of truth.

This ADR builds on
[ADR 0166](0166-tenant-boundary-and-principal-identity-land-together.md),
[ADR 0175](0175-a-bot-may-limit-who-can-talk-to-it.md),
[ADR 0034](0034-approval-authorizers-resolve-membership-in-the-api.md),
[ADR 0106](0106-an-approver-is-an-authenticated-principal.md),
[ADR 0123](0123-a-pending-approvals-approver-set-does-not-follow-its-route-binding.md),
[ADR 0177](0177-an-approval-is-answered-where-it-was-asked-including-by-email.md)
and
[ADR 0198](0198-identity-links-live-in-provider-identity-namespaces.md).

It supersedes in part ADR 0177's amendment (A1 to A3). For an agent that names
an approver team, approver-team membership replaces the route's
`approvers.emails` list, and the "no list, nobody" rule that goes with it.
Amendment A1 already anticipated this: identity links absorb the list once they
land. Everything else in ADR 0177 stands, including where a card is shown and
who answers an email card on an agent without an approver team.

It also realizes ADR 0198 decision 8 (link creation) as concrete flows, and
answers ADR 0198 open question 4 (mailbox evidence for email) for addresses an
identity provider governs. ADR 0198's tables, resolution order and other
decisions stand.

Tracking issue: #4273.

## Terms

- **Identity provider (IdP)**: the tenant's directory and sign-in service, such
  as Okta or Entra ID. It is the source of truth for people, groups, account
  status, and whatever channel accounts it knows about.
- **Person**: a `principals` row, keyed by the IdP's `(issuer, subject)`. It is
  a cached copy of an IdP user, never edited in Curie.
- **Team**: a `teams` row. An `idp_group` team mirrors one IdP group.
- **Channel account**: who someone is on one channel: a Slack user id, an email
  address, a Teams user id, and so on.
- **Link**: an `identity_links` row that binds one channel account to one person
  ([ADR 0198](0198-identity-links-live-in-provider-identity-namespaces.md)
  decision 5).
- **Directory adapter**: the external integration that reads the IdP and
  delivers its people, groups and known channel accounts to Curie.
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
2. **Approval.** On Slack, a route's `approvers` block names Slack users or a
   Slack user group, or falls back to members of the card's channel
   ([ADR 0034](0034-approval-authorizers-resolve-membership-in-the-api.md)). On
   email, the route's `approvers.emails` list decides (ADR 0177 amendment).

So the same person is written down once per channel, removals do not propagate,
and groups of zero or more than 100 cannot be expressed.

The pieces for one answer are half built. `principals` (with `status` and
`authorization_version`), `teams` and `principal_teams` exist on `next`. Generic
OIDC sign-in exists (#2908). ADR 0198 decides how a channel account is linked
to a person, and says link creation is the trust boundary. No link table,
resolver or creation flow is implemented, and email is explicitly left
unresolvable.

Maintainer review of #4273 and #4513 set these constraints:

- invocation and approval are one mechanism with different teams;
- there is no short wall-clock expiry on membership, because one sync outage
  would then refuse every caller of the bot; directory sync keeps the last
  known state;
- a broken sync must still be visible, for example as the time it last synced;
- immediate deactivation of a person stays a separate mechanism;
- the IdP is the source of truth: use what it knows, keep user friction minimal,
  and allow no hand-made mappings.

## Decision

**The tenant's identity provider is the only source of who a person is, which
teams they belong to, and, where it knows them, which channel accounts are
theirs. Curie keeps a read-only copy and never edits it. A channel account the
IdP does not know is linked once, by the person signing in to the IdP. An agent
names an invocation team and an approver team, and both apply on every channel
the agent is reachable through.**

```
                IdP (people, groups, status, known channel accounts)
                  │                                   │
        directory adapter (sync)          one-time sign-in (only for
                  │                       accounts the IdP does not know)
                  ▼                                   ▼
        read-only copy: people, teams, links  ◄───────┘
                  │
   Slack / email / other channel ──► person ──► agent's invocation team
                                             └► agent's approver team
```

### 1. The IdP is the source; Curie holds a read-only copy

- A tenant configures one IdP issuer. People are `principals` keyed by that
  issuer and the IdP subject.
- `principals`, `teams`, `principal_teams` and IdP-sourced links are a
  projection of the IdP, as ADR 0166 decision 2 defines. No API, CLI or console
  path creates or edits a person, a team's membership, or a link by hand. A
  change happens in the IdP and reaches Curie through sections 2 to 5.
- Curie keeps this copy, instead of asking the IdP on each request, for three
  reasons:
  1. A chat event or inbound email carries no IdP token, so there is nothing for
     the IdP to evaluate per request.
  2. A live lookup per message makes IdP latency and outages bot outages.
  3. IdP management APIs are rate limited per organization and shared with the
     customer's other integrations. Okta, for example, gives one token at most
     half of an endpoint's org-wide limit by default. A periodic sync costs a few
     calls a minute regardless of chat traffic.
- A tenant declares which email domains its IdP governs. Sections 2 and 4 rely
  on that list.

### 2. Channel accounts the IdP knows are linked from the IdP

The directory adapter delivers, with each person, the channel accounts the IdP
holds for them:

- their email address, when its domain is one the IdP governs;
- a provider user id, when the IdP provisions that provider for the person, for
  example a Slack user id for a workspace the IdP provisions through SCIM.

Curie writes these as links with source `idp_directory`, replacing the person's
previous IdP-sourced links in the same transaction as the snapshot (section 5).
The person does nothing. An account the IdP stops reporting is unlinked at the
next complete snapshot.

### 3. Other chat accounts are linked once, with a connect button

The IdP does not always know a person's chat account. It cannot know an account
in a workspace it does not provision, for example a user who reaches the bot
from another organization over Slack Connect. For those accounts, ADR 0198
decision 8's `provider_event_verified` source is realized as follows, for every
chat provider that has an ADR 0198 decision 6 realization (Slack today):

1. When a sender the resolver cannot resolve first uses an agent, the agent
   answers privately (an ephemeral message, or the app's home view) with a
   **Connect account** button. The sender types no command.
2. The button carries a single-use, 10 minute token, stored hashed with the
   sender's namespace and native id as ADR 0198 decision 6 derived them.
3. The button opens an OIDC sign-in to the tenant's IdP. There is no Curie
   account or password. With an active SSO session it is one click.
4. A confirmation page names the account being linked ("Slack user @name in
   workspace W"). On confirm, Curie consumes the token and links the account to
   the signed-in person, with an audit row. The original request is not replayed;
   the person asks again.

An expired, consumed or cross-tenant token is refused. Connect prompts are rate
limited per channel account. An account the IdP later reports for a person
(section 2) takes precedence over a connected link for the same account.

### 4. Email resolves through IdP-known addresses

1. An address in an IdP-governed domain is linked from the IdP's directory data
   (section 2). A connect sign-in also links the `email` claim when
   `email_verified` is true and the domain is governed.
2. An inbound email resolves to a person only when both hold: the adapter
   authenticated the sender (aligned DMARC, as today), and an active link exists
   for the exact address. DMARC proves the domain; the IdP proves who holds the
   mailbox in that domain.
3. Reassignment follows the IdP. When the IdP reports an address for a
   different person, the older link is revoked and the new one written.
4. Addresses outside governed domains are never linked. They stay on
   exact-address `allowed_callers` and `approvers.emails` lists, on agents
   without a team.

### 5. Team membership is a complete snapshot

The directory adapter replaces a team's whole membership in one transaction.

- A team gains `epoch`, `member_count`, `digest`, `last_synced_at`,
  `last_attempt_at`, `last_error` and `managed_by`.
- A snapshot names the epoch it expects. A snapshot from an older run is
  refused, so a slow run cannot restore members a newer run removed.
- A partial or invalid snapshot is refused. The attempt updates
  `last_attempt_at` and `last_error` only.
- An empty complete snapshot is valid and admits nobody. A team that has never
  received a complete snapshot also admits nobody.
- Each applied snapshot writes an audit row with epoch, member count and digest.

### 6. Sync health is reported, not enforced

A projection never expires. A team whose adapter stopped keeps the membership it
last received.

- Status reads show each team's `last_synced_at`, `last_attempt_at`,
  `last_error`, `epoch`, `member_count` and `digest`.
- The platform exports each team's sync age and failed-attempt count so
  operators can alert on a stalled adapter.
- These signals never change an authorization decision.

### 7. Account status comes from the IdP, immediately

A person's status is the IdP's fact, copied into Curie. It is separate from team
membership, and it is not something Curie decides.

- When the IdP suspends, deactivates or reactivates a user, the adapter delivers
  that change as soon as it learns it, for example from an IdP event, without
  waiting for the next group sync. The change names the person by
  `(issuer, subject)` and the version it expects. It bumps
  `authorization_version` and writes an audit row.
- An older reactivation cannot undo a newer suspension.
- A person who is not active is refused for invocation and approval on every
  agent and every channel from the next decision on.

### 8. An agent names two teams, and they apply everywhere

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

### 9. Invocation: one check on every channel

The invocation team is enforced in ADR 0175's single `admit` function, which
ADR 0175 decision 6 reserves for this principal check.

1. Resolve the sender to a person through ADR 0198's resolution order and the
   links above. An unlinked account gets the connect prompt (section 3) and no
   turn. A link in another tenant, or an inactive person, is refused.
2. Admit only members of the invocation team's current projection.
3. When a binding also carries `allowed_callers`, require both. A list can
   narrow the team, never widen it.
4. Refuse before a claim, placeholder, queued turn or sandbox. A refused,
   resolved caller gets nothing back, as ADR 0175 decision 3 requires.
5. The dispatcher's open-route and install-wide open shortcuts do not apply to
   an agent with an invocation team.

Signed hooks, schedules, work items and bot handoffs keep their own
authorization. A human team never grants a machine caller anything.

### 10. Approval: one approver set on every channel

An approver team becomes one `ApproverSet`
([ADR 0034](0034-approval-authorizers-resolve-membership-in-the-api.md)
decision 4), evaluated when the approval is answered (ADR 0034 decision 2).

- Whoever answers is resolved to a person the same way as in section 9. A Slack
  click resolves through its Slack link, an email reply through its address
  link, a console action through the console session's IdP sign-in, and any
  other adapter through its provider's link. That person must be active and in
  the approver team's current projection.
- Where the card is shown is unchanged (ADR 0177 decisions 1 and 2). Any
  approver-team member who can see the card may answer it on that channel.
- Every route of the agent uses the team. A route's own `approvers` block may
  still narrow it. The channel-membership default does not apply.
- Removing someone from the approver team stops them from resolving approvals
  that are still pending.
- A requester who is also on the approver team may approve, as
  [ADR 0106](0106-an-approver-is-an-authenticated-principal.md) allows.
- The approval audit row records the team, its epoch and its digest, in new
  columns. The frozen `authorizer` vocabulary is not redefined.
- An agent with no approver team keeps every existing set, including ADR 0123's
  unbound-route refusal and ADR 0177's email lists.

### 11. Caches follow versions, not clocks

Cached admission answers are valid only for the team epoch, policy version,
person `authorization_version` and link they were computed from. A newer value
of any of them invalidates the answer. ADR 0175's cache TTL and its behavior
while the API is unreachable are otherwise unchanged.

### 12. Each credential does one thing

- The directory adapter writes only what the IdP says: snapshots, sync attempts,
  status changes and IdP-known channel accounts, for teams whose `managed_by`
  names it, in its own tenant. It cannot change policies, resolve approvals, or
  touch other teams. Because it carries the IdP's word on who is who, its
  credential is treated as sensitive as an IdP admin token for those fields.
- The connect flow (section 3) creates only `provider_event_verified` links for
  the signed-in person.
- Nothing else creates links.

### 13. Operators use `curie`

Admin API routes and `curie cluster` subcommands, each with dry run and JSON
output, cover the tenant's IdP issuer and governed domains, identity
namespaces, agent policies, and read-only views of people, teams, links and sync
status. Exact names are settled in implementation review.

### 14. Rollout

A policy enforces from its first release, as in ADR 0175 decision 7, because it
applies only to agents an operator configures.

## Consequences

1. One place, the IdP, decides who may use and approve for an agent, on Slack,
   email and any channel that later gains a resolver. Leaving an IdP group or
   being deactivated there takes effect on every channel.
2. Where the IdP already knows a person's accounts, the person does nothing.
   Otherwise they press one button and sign in once per chat account.
3. Approvers on email and other adapter channels are decided by the same team as
   on Slack, replacing per-route address lists for agents with an approver team.
4. A stalled directory adapter does not take a bot down. Removals stop reaching
   Curie until it is fixed, and the sync-age signal shows it. Deactivation has
   its own fast path (section 7).
5. This ADR depends on implementing ADR 0198's links and resolver (#2910,
   #2914), identity audit (#2912, #2913) and tenant scoping (#2911). Enforcement
   cannot ship before them. Team projection, sync status, the connect flow and
   read-only administration can.
6. The IdP must offer OIDC sign-in with a verified `email` claim, and a way for
   the directory adapter to read users, groups and account status.
   Provider-specific adapters stay outside the open source platform (ADR 0166
   consequence 3).
7. New schema: projection state on `teams`, the tenant's IdP configuration and
   governed domains, connect tokens, link sources, and agent access policies.
   Migrations use the number the base allocates. No release version changes
   because of this ADR.
8. Channel visibility is not covered. Refusing a caller does not hide history or
   retract delivered output.

## Alternatives considered

1. **Separate ADRs for team policy, chat linking and email linking.** Rejected.
   The point is one identity source for every channel.
2. **Ask the IdP live on every message instead of keeping a copy.** Rejected for
   the three reasons in section 1. The maintainer accepted the tradeoff in the
   review of #4513.
3. **A wall-clock authorization lease on team membership.** Rejected. One
   adapter outage would refuse every caller of every bot using the team.
4. **Administrator-made mappings between channel accounts and people.** Rejected.
   The IdP is the source of truth, and a hand mapping is a second one.
5. **A typed link command with a pasted code.** Rejected for its friction. A
   button that opens the IdP sign-in proves the same two facts.
6. **Per-channel teams or lists.** Rejected. It is the current problem.
7. **Match chat accounts to people by profile email or display name.** Rejected,
   as ADR 0166 requires. A chat profile's email is user-editable unless the IdP
   controls it, and Curie cannot verify that in general.
8. **Trust DMARC alone for email.** Rejected. DMARC authenticates a domain, not a
   mailbox (ADR 0198).
9. **Derive the approver team from the invocation team.** Rejected. Requesters
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
- Pull request: #4513
- Related: #2907, #2908, #2910, #2911, #2912, #2913, #2914, #3885, #4246, #3063

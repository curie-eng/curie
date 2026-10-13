# 220. An agent names the teams that may invoke it and approve for it

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
[ADR 0198](0198-identity-links-live-in-provider-identity-namespaces.md). It
supersedes nothing. Every existing list and approver set keeps its accepted
meaning for an agent that names no team.

Tracking issue: #4273.

## Terms

- **Team**: a row in `teams` ([ADR 0166](0166-tenant-boundary-and-principal-identity-land-together.md)
  decision 2). An `idp_group` team mirrors a group in the customer's identity
  provider.
- **Projection**: Curie's copy of one team's complete membership, as last
  delivered by a directory adapter.
- **Directory adapter**: the external integration that reads the identity
  provider and writes projections. Provider collection stays outside the open
  source platform ([ADR 0166](0166-tenant-boundary-and-principal-identity-land-together.md)
  consequence 3).
- **Invocation team**: the team whose members may start a turn with an agent.
- **Approver team**: the team whose members may resolve that agent's approvals.

## Context

Two hand-maintained lists decide who may use a bot and who may approve for it
today.

1. [ADR 0175](0175-a-bot-may-limit-who-can-talk-to-it.md) gives each binding an
   optional `allowed_callers` list of provider-native ids. NULL means open, an
   empty list is refused, and a list holds at most 100 ids. ADR 0175 calls
   itself a stopgap for #2914.
2. A route's `approvers` block names explicit Slack users, a Slack user group,
   or nothing, in which case members of the card's channel may approve
   ([ADR 0034](0034-approval-authorizers-resolve-membership-in-the-api.md)).
   Email approvals are answered by the requester alone
   ([ADR 0177](0177-an-approval-is-answered-where-it-was-asked-including-by-email.md)
   decision 3), which that ADR marks as an interim.

These lists have the same four problems:

1. **They drift.** A person who leaves a team in the identity provider stays on
   every Curie list until someone edits it by hand.
2. **They cannot express real groups.** A group of zero cannot be stored, and a
   group above 100 cannot fit.
3. **A new ingress starts open.** The list lives on the binding, so a DM or
   email binding added later carries no list and admits everyone.
4. **They are provider ids, not people.** A Slack id says nothing about the
   same person's email address, so one policy cannot cover both ingress kinds.

The tables needed for a better answer already exist on `next`. `principals`
carries `status` and `authorization_version`. `teams` carries `source` and
`external_id`. `principal_teams` carries `source`, `version` and `synced_at`.
No code reads or writes them outside console login, nothing records a team's
sync state, and `identity_links`
([ADR 0198](0198-identity-links-live-in-provider-identity-namespaces.md)) is
decided but not built.

Maintainer review of #4273 shaped four points of this decision:

- invocation and approval are the same mechanism with different teams, so one
  ADR covers both;
- a short wall-clock expiry on membership turns one sync outage into an outage
  for every caller of the bot, and directory sync does not normally work that
  way;
- a broken sync still needs a visible signal, such as the time a team last
  synced;
- immediate deactivation of a person is a separate mechanism and stays.

## Decision

**An agent may name an invocation team and an approver team. Both are canonical
teams whose membership is a complete projection delivered by a directory
adapter. The last complete projection stays authoritative until a newer one
replaces it. Sync health is reported, never enforced by a timer. Deactivating a
principal takes effect at once, independent of any sync.**

### 1. One policy, two independent roles

An agent gains an access policy with two optional, tenant-scoped team
references: `invocation_team_id` and `approver_team_id`.

- Each role is decided only by its own team. Membership in the invocation team
  never grants approval, and membership in the approver team never grants
  invocation.
- Nothing derives one team from the other. An operator may name the same team
  in both roles, but the platform never assumes it.
- A team in another tenant is refused when the policy is written.
- The policy carries a version. A write names the version it expects and is
  refused with a conflict if another write landed first.
- No policy, or a policy that names no team for a role, leaves that role on
  today's behavior.

### 2. A projection is complete or it is not applied

A directory adapter replaces a team's whole membership in one transaction. It
never adds or removes members one at a time.

- A team gains projection state: `epoch`, `member_count`, `digest`,
  `last_synced_at`, `last_attempt_at`, `last_error` and `managed_by`.
- A snapshot names the epoch it expects. A snapshot from an older run is refused
  with a conflict, so a slow run that finishes late cannot restore members a
  newer run removed.
- A snapshot that the adapter marks partial, or that fails validation, is
  refused. The failed attempt updates `last_attempt_at` and `last_error` and
  nothing else. Membership, `epoch` and `last_synced_at` stay as they were.
- An empty complete snapshot is valid. It means the team has no members, and the
  role it serves admits nobody.
- A team that has never received a complete snapshot admits nobody.
- Each applied snapshot writes an identity audit row in the same transaction,
  naming the epoch, member count and digest.

### 3. Sync health is reported, not enforced

There is no expiry on a projection. A team whose adapter has stopped keeps the
membership it last received.

- Status reads show, for every adapter-managed team, `last_synced_at`,
  `last_attempt_at`, `last_error`, `epoch`, `member_count` and `digest`, with no
  credentials.
- The platform exports the age of each team's last complete sync and a count of
  failed attempts, so operators can alert on a stalled or failing adapter.
- These signals never change an authorization decision. A stalled sync is an
  operational incident to fix, not a reason to refuse current members.

### 4. Deactivating a principal is immediate

A principal's `status` is separate from team membership. Removing someone from
one team does not deactivate them, and deactivating someone does not wait for
any team to resync.

- An authoritative status change (suspend, disable, revoke, reactivate) names
  the principal by `(idp_issuer, idp_subject)` and the version it expects. It
  bumps `authorization_version` and writes an audit row.
- An older reactivation cannot undo a newer suspension.
- A principal that is not active is refused for both roles on every agent, in
  every channel, from the next decision on.

### 5. Invocation: one check, every ingress

The invocation team is enforced inside the one admission function of
[ADR 0175](0175-a-bot-may-limit-who-can-talk-to-it.md) decision 2, which that
ADR's decision 6 reserves for #2914's principal check.

- The caller's authenticated provider id is resolved to a principal through an
  identity link in its namespace
  ([ADR 0198](0198-identity-links-live-in-provider-identity-namespaces.md)). An
  unlinked id, a link in another tenant's namespace, or an inactive principal is
  refused.
- The team applies to the agent, so it covers every binding, including one
  added after the policy was set.
- When a binding also carries `allowed_callers`, the caller must pass both. A
  list can narrow the team; it cannot widen it.
- A refused caller gets nothing back, as in ADR 0175 decision 3. The refusal
  happens before a claim, a placeholder, a queued turn or a sandbox.
- The route-level and install-wide "open to everyone" cache shortcuts in the
  dispatcher do not apply to an agent with an invocation team.
- Signed hooks, schedules, work items and bot-to-bot handoffs are out of scope.
  A human team never grants a machine caller anything.

### 6. Approval: a team-backed approver set

An approver team becomes one more `ApproverSet`
([ADR 0034](0034-approval-authorizers-resolve-membership-in-the-api.md)
decision 4), resolved at the moment of the click
([ADR 0034](0034-approval-authorizers-resolve-membership-in-the-api.md)
decision 2).

- The authenticated approver
  ([ADR 0106](0106-an-approver-is-an-authenticated-principal.md)) is resolved to
  a principal the same way as in decision 5, and must be active and a member of
  the approver team's current projection.
- When an agent names an approver team, every route of that agent uses it. A
  route's own `approvers` block may still narrow it: the approver must pass both.
  The channel-membership default does not apply to such an agent.
- Removing someone from the approver team stops them from resolving approvals
  that are already pending.
- An agent with no approver team keeps every existing set unchanged, including
  ADR 0123's unbound-route refusal and ADR 0177's requester-only set.
- The audit row records the team, its epoch and its digest as evidence, in new
  columns. The frozen `authorizer` vocabulary is not redefined.
- A requester who is also on the approver team may approve, as
  [ADR 0106](0106-an-approver-is-an-authenticated-principal.md) already allows.

### 7. Caches follow versions, not clocks

The dispatcher keeps ADR 0175's cache and its bounds for agents with an
invocation team, with one change: a cached answer is valid only for the team
epoch, policy version and principal `authorization_version` it was computed
from. A newer value of any of them invalidates it. A removal, a deactivation or
a policy change therefore reaches Slack within ADR 0175's existing bound, and an
older cache entry cannot bring access back.

ADR 0175's behavior while the API is unreachable is unchanged: answers cached
within the stale window stay valid, and an uncached caller is refused.

### 8. The adapter credential can do one thing

A directory adapter writes snapshots and sync attempts only for teams whose
`managed_by` names it, in its own tenant. It cannot create identity links,
change any agent's policy, resolve approvals, or touch teams it does not manage.

An operator cannot edit the membership of an adapter-managed team by hand. Handing
a team from an adapter to manual management is an explicit, audited operation.

### 9. Operators configure and read it through `curie`

Policy read and write, projection status, and ownership handoff are API routes
under the platform's admin credential and `curie cluster` subcommands, each with
a dry run and JSON output. Exact command names are settled in implementation
review.

### 10. Enforcing from the first release

As in ADR 0175 decision 7, a team takes effect from its first release, because
it applies only where an operator set one.

## Consequences

1. One mechanism covers invocation and approval. A person who leaves a team in
   the identity provider loses that role in Curie at the next sync, with no
   hand edit anywhere.
2. Groups of any size, including zero, can be expressed, and a new binding
   cannot open a bot that has an invocation team.
3. A stalled directory adapter does not take a bot down. Its cost is that
   removals stop reaching Curie until it is fixed, and the sync-age signal is
   what tells operators. Urgent removals go through decision 4, which does not
   depend on the adapter.
4. Email enforcement depends on verified email identity links. Until those exist
   and are qualified, an email caller on an agent with an invocation team cannot
   be resolved and is refused. An operator who needs email access in the
   meantime keeps an exact-address `allowed_callers` list on an agent without a
   team.
5. This ADR depends on identity links and principal resolution (#2910, #2914),
   identity audit (#2912, #2913) and tenant scoping (#2911). Invocation and
   approval enforcement cannot ship before them. The projection tables, status
   reporting and administration can.
6. Two schema changes follow: projection state on `teams`, and an agent access
   policy. Both use the migration number the base allocates. No release version
   changes because of this ADR.
7. Channel visibility is not covered. Refusing a caller does not hide channel
   history or retract output already delivered.

## Alternatives considered

1. **A fixed authorization lease (for example, five minutes) after which a team
   admits nobody.** Rejected. One adapter outage then refuses every caller of
   every bot that uses the team. Directory sync elsewhere keeps the last known
   state and reports staleness, and decision 4 already covers urgent removal.
2. **An optional per-team maximum staleness that operators opt into.** Deferred,
   not adopted. Nothing asks for it yet, and it reintroduces the outage
   coupling of alternative 1 for whoever turns it on.
3. **Separate mechanisms, or separate ADRs, for invocation and approval.**
   Rejected. The membership source, freshness, deactivation and audit are
   identical; only the team differs.
4. **Raise ADR 0175's 100-entry limit and sync provider ids into
   `allowed_callers`.** Rejected. It keeps the list on the binding, so new
   bindings stay open, and it keeps provider ids instead of people.
5. **Incremental membership events from the adapter.** Rejected as the source of
   truth. A missed event leaves membership wrong with no way to detect it. A
   complete snapshot is self-correcting. An adapter may still send snapshots as
   often as it likes.
6. **Derive the approver team from the invocation team.** Rejected. The people
   who ask and the people who approve are usually different, and coupling them
   would let a requester-team change grant approval.
7. **Resolve groups live from the provider at decision time, as the Slack user
   group set does.** Rejected for this mechanism. It puts the identity provider
   on the request path for every turn and makes its outages Curie outages.

## Reference

- Issue: #4273
- Related: #2907, #2910, #2911, #2912, #2913, #2914, #3885, #4246, #3063

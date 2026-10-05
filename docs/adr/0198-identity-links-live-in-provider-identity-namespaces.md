# 198. Identity links live in provider identity namespaces

Date: 2026-10-03

Status: Accepted

Accepted 2026-10-04 with maintainer approval from TheConnMan, recorded as an
approving review on the publishing pull request
([#3928](https://github.com/curie-eng/curie/pull/3928)).

This ADR builds on
[ADR 0166](0166-tenant-boundary-and-principal-identity-land-together.md) and
[ADR 0168](0168-one-installation-hosts-several-bot-identities.md).

It supersedes four clauses:

- **ADR 0166 decision 2,** that `identity_links` bind a provider native id
  "inside one provider installation". A link is scoped to an identity namespace
  instead (decision 5).
- **ADR 0168 decision 1,** its choice to make a channel identity a row in
  `provider_installations`.
- **ADR 0168 decision 3,** in part: "ADR-0155 decision 3's installation
  reference on a binding is this name in `adapter`". The name still lives in
  `adapter`, but it now references a channel identity, not a provider
  installation.
- **ADR 0168's Tracking clause** that has #2909 "create one bootstrap row per
  declared identity" in that table.

The rest of both ADRs stands. In ADR 0166 that includes `provider_installations`
as "a connected external account" and "resolution never guesses". In ADR 0168
it includes one connection per identity, the identity stamped from the
connection, and routes keyed `(kind, adapter, address)`.

Per [ADR 0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md),
both stay `Accepted` and carry back links naming exactly these clauses.

## Context

An identity link answers one question when an event arrives: which principal is
this provider user id? A user id only means something inside the scope in which
the provider makes it unique and stable. This ADR calls that scope the user's
**identity namespace**.

Three different things are involved, and across providers they do not line up:

- the **identity namespace**, where user ids are unique;
- the **provider installation**, the connection a tenant authorised, which is
  where events come from;
- the **channel identity**, who Curie speaks as.

### The namespace is a property of the provider

This table is informative. Decision 6 says which parts are normative now.

| Provider | Installation | User identity namespace | Stable native id |
| --- | --- | --- | --- |
| Slack | a workspace, or an Enterprise Grid organisation-wide install | the user's workspace; under Grid, ids are organisation-wide | user id |
| Microsoft Teams and M365 | the app in an Entra tenant | an **Entra tenant** | Entra object id (`aadObjectId`) |
| Google Workspace | a Workspace customer | **Google, global** | Google account id |
| GitHub | a GitHub App installation | the **host**: github.com or one GHES instance | numeric user id |
| Jira and Confluence Cloud | an Atlassian site | **Atlassian, global** | `accountId` |
| Jira and Confluence Data Center | the instance | the **instance** | user key |
| Linear | a workspace | the **workspace** | user id |
| Email | a mailbox | not settled; see decision 6 | not settled |

The rows rest on these sources:

- **Slack:** "within an Enterprise org, all users have a single, global ID", and
  Slack converts earlier workspace ids to it
  ([developing for enterprise orgs](https://docs.slack.dev/enterprise/developing-for-enterprise-orgs/)).
  The same page documents `user_team`, "the team the user sending the message
  belongs to", alongside `source_team`, `team_id`, `enterprise_id` and
  `is_enterprise_install`.
- **Teams:** the bot-facing user id "is unique to your agent ID and a particular
  user" and cannot be reused between agents
  ([proactive messages](https://learn.microsoft.com/en-us/microsoftteams/platform/bots/how-to/conversations/send-proactive-messages)).
- **Google:** a Chat `users/{id}` is the same id as in the People and Directory
  APIs ([identify users](https://developers.google.com/workspace/chat/identify-reference-users)).
- **Atlassian:** an `accountId` identifies a user across all Atlassian cloud
  products ([user privacy guide](https://developer.atlassian.com/cloud/confluence/user-privacy-developer-guide/)).
- **Linear:** one account holds a separate user per workspace
  ([profile](https://linear.app/docs/profile)).

The namespace can be narrower than the installation, the same as it, or much
wider. A link keyed to the installation, or to the bot, is therefore wrong for
most providers.

### What ADR 0168 decision 1 did to links

ADR 0166 decision 2 defines `provider_installations` as "a connected external
account" and binds links "inside one provider installation". For a standard
Slack workspace those coincide with the namespace. ADR 0168 decision 1 then
made each channel identity "a row in ADR-0155's `provider_installations`", so
two bots in one workspace "are two rows".

The pending implementation, #2909 in PR #3040, follows that decision. Sibling
identities repeat one `external_account_id`, and a row created at boot holds
the placeholder `static` until an operator sets the real team id. These column
details belong to that unmerged pull request, not to either ADR. The conflict
surfaced while #2910 (PR #3054) was building identity links on top of it.

Take one tenant whose Slack team `T1` runs two bots, `support` and `ops`. A
third bot, `alerts`, runs in team `T2`. Alice is `U1` in `T1`.

**Keyed per row, as ADR 0166's text reads once a row is a bot.** An
administrator links `U1` to Alice on `support`.

- Alice messages `ops`. The lookup `(ops, U1)` finds nothing, so she is
  unresolved, although she is the same person in the same workspace. Adding a
  bot leaves every existing person unresolved on it until they are linked again.
- The two links for `U1` are independent rows. Relinking on one bot and not the
  other makes the same Slack user resolve to two different people.
- Removing `support` is refused while any link references it, or, if the key
  cascades, deletes those links. Either way, a person's identity hangs on a bot.

**Keyed on the team id string, `(slack, T1, U1)`.** Resolution has no workspace
row, so it takes the receiving bot's `external_account_id`.

- If `ops` still says `static` while `support` says `T1`, Alice resolves through
  `support` and not through `ops`.
- If `ops` (in `T1`) and `alerts` (in `T2`) both say `static`, they appear to be
  one workspace. A link made under `static` resolves `U1` through either bot.
  That is the cross-namespace guess ADR 0166 forbids.
- The link has no foreign key, so a mistyped team id silently never matches.
- The bot's workspace stands in for the user's. A Slack Connect user from
  another workspace is looked up in the wrong namespace.

Both keyings are Slack-shaped. Neither fits Teams, where the bot-facing id
differs per bot, or GitHub, Google and Atlassian, whose ids span every
installation.

### Why ADR 0168 chose this, and what is reversed

ADR 0168 gives two reasons that bear on its table choice:

1. **No table per provider.** Its alternative 2 rejects a table per channel
   kind, such as `slack_identities`.
2. **One row per identity, addressed by a unique name,** so a route's `adapter`
   can name it on every kind (its decision 3).

A third reason is inferred, not stated. ADR 0168 placed identities in a table
ADR 0166 had already planned, rather than adding one.

This ADR keeps the first two. `channel_identities` is one generic table, and
each row is still one identity whose unique name `adapter` refers to. Only the
reuse of `provider_installations` is reversed.

ADR 0168's context is about routing, credentials and which bot is speaking. It
does not consider identity links or the namespace of provider user ids. Its
routing decisions do not change.

No deployment has these tables yet. #2909 (PR #3040), #2910 (PR #3054) and
#2911 (PR #3052) are unmerged, so this correction needs no conversion of
existing rows.

## Decision

### 1. Three concepts, three tables

| Table | Holds | Scoped by |
| --- | --- | --- |
| `identity_namespaces` | where a provider's user ids are unique | tenant |
| `provider_installations` | a connection the tenant authorised, as ADR 0166 defines it | tenant |
| `channel_identities` | who Curie speaks as, as ADR 0168 defines it | an installation |

Identity links reference a namespace. Bindings reference a channel identity
through `adapter`.

### 2. Identity namespaces

`identity_namespaces` holds `(tenant_id, provider, authority, kind, key,
status)`.

- **`authority`** is the provider endpoint the ids belong to. It is empty for a
  provider with one global service, such as Slack, Google or Atlassian Cloud. It
  is the canonical hostname for a self-hosted or per-host provider, such as a
  GHES instance or an Atlassian Data Center base URL.
- **Uniqueness:** unique on `(tenant_id, provider, authority, kind, key)`, and
  on `(tenant_id, id)` as the link target.
- **Kinds:** `kind` comes from a closed vocabulary that each provider's
  realization defines, and `key` follows that kind's grammar. For Slack, the
  kind is `slack_workspace` and the key is a `T…` id.
- **No placeholder.** A namespace row exists only once it is known.
- **Per tenant.** Each tenant has its own rows, including for global namespaces.
  A tenant recognises identities only from namespaces it holds.
- **Creation.** Connecting an installation creates its home namespace by atomic
  find-or-create. An administrator may add a further namespace explicitly, such
  as a Slack Connect partner's workspace. Resolving an event never creates a
  namespace.

### 3. Provider installations are connections

`provider_installations` keeps ADR 0166's meaning: a connected external account.
Its columns are `tenant_id`, `provider`, `authority`, `external_account_id`,
`display_name`, `status`, `installed_by_principal_id`, `installed_at` and
`disconnected_at`.

- **`authority`:** the same field as in decision 2. It is persisted from the
  configured connection, never from an event. This is what keeps a GHES
  installation id from colliding with the same id on another host.
- **`external_account_id`:** what the provider calls the installation target,
  such as a Slack team or Grid organisation id, an Entra tenant id, a GitHub App
  installation id, an Atlassian `cloudId`, or a Google customer id.
- **Uniqueness:** unique on
  `(tenant_id, provider, authority, external_account_id)` and on
  `(tenant_id, provider, id)`.
- **No placeholder.** An installation row exists only once its target is known.
- **Deleting** an installation is refused while channel identities are attached
  to it: the foreign key takes no action. Detaching an identity is an explicit
  operation, so deletion cannot bypass the disconnection check in decision 7.

### 4. Channel identities are children of an installation

A tenant-scoped `channel_identities` table holds what ADR 0168 decision 1 put in
`provider_installations`.

- **Columns:** `provider`; `name`, unique per `(tenant_id, provider)` and the
  value a binding's `adapter` names; `credential_ref`;
  `webhook_verification_ref`; `scopes`; `attributes`; `status`.
  `status` is one of active, disabled or revoked.
- **Installation reference:** a nullable composite foreign key
  `(tenant_id, provider, provider_installation_id)` to
  `provider_installations(tenant_id, provider, id)`. An identity can attach only
  to an installation of its own tenant and provider.
- **Attachment:** a declared identity is created at boot unattached. For Slack,
  preflight's `auth.test` with the identity's own token reports its team (#3039).
  The API then finds or creates that installation and attaches the identity, so
  siblings converge by construction.
- **Mismatch:** a later report may name a different installation target. The
  identity is then marked `installation_mismatch` and keeps its attachment. The
  mark clears only on evidence: either a fresh report from the identity's own
  credential matches again, or an operator reattaches it to the reported target.
- **Routing** does not depend on attachment. ADR 0168 routes by name.

### 5. Identity links reference a namespace

`identity_links` binds one native id inside one identity namespace to exactly
one principal or one bot.

- **Namespace:** a composite foreign key `(tenant_id, identity_namespace_id)` to
  `identity_namespaces(tenant_id, id)`.
- **Targets:** a composite foreign key `(tenant_id, principal_id)` to
  `principals(tenant_id, id)`. The bot target becomes
  `(tenant_id, bot_id)` to `agents(tenant_id, id)` once #2911 gives agents a
  tenant. No link crosses a tenant.
- **Uniqueness:** among active links, unique on
  `(identity_namespace_id, provider_native_id)`, whether the target is a
  principal or a bot. One native id in one namespace means one thing at a time.
  A revoked link keeps its row, its evidence and its `revoked_at`, and does not
  count toward uniqueness. A provider bot user
  that fronts several agents is identified by its channel identity, not by bot
  links.
- **Native id:** the provider's stable, canonical id, never a mutable or
  bot-scoped one. Examples are a Teams `aadObjectId` rather than the bot-facing
  `from.id`, and a GitHub numeric id rather than the login.
- **Immutable.** A link's target, source and evidence never change. Changing a
  mapping revokes the existing link, then creates a new one.
- **Reach.** One link recognises a person through every installation and channel
  identity that reports them in that namespace. Removing a channel identity or
  an installation never removes or moves links.

### 6. Deriving a user's namespace from an event

The receiving channel identity is known from the connection the event arrived on
(ADR 0168 decision 2). The tenant comes from that identity. A caller never
supplies or overrides either.

Each provider realization supplies one deterministic function. It returns
`(authority, kind, key, native id)` from the event, and it must have all of
these properties:

1. **Sender-bound.** The namespace comes from fields the provider documents as
   describing the sending user. It never comes from fields that describe the
   receiving installation, the channel or the authorization.
2. **Canonical.** The native id is the provider's stable id, converted to its
   canonical form where the provider documents a conversion.
3. **Connection-scoped authority.** `authority` comes only from the receiving
   installation's configured value, never from the event.
4. **Fails closed.** When the fields are missing or disagree, it returns
   unresolved with a named reason. It never falls back to the receiving
   installation's namespace.

**Slack is normative now,** because #2910 realizes it:

- The namespace is `slack_workspace`, keyed by the sending user's own team: the
  event's `user_team`, or an interaction payload's `user.team_id`.
- The native id is Slack's canonical global user id. Where Slack's
  [translation layer](https://docs.slack.dev/enterprise/developing-for-enterprise-orgs/#toggle-the-translation-layer)
  governs whether a payload carries global or historical ids, the realization
  configures it to deliver global ids. Links are stored only in that form.
- It never uses `team_id`, `source_team`, `enterprise_id` or the authorization's
  team, which describe the channel or the installation.
- An absent user id or sender team gives `subject_unidentified`. Two sender team
  fields that disagree give `namespace_conflict`.
- A Slack Connect payload type that does not carry the sender's team is
  therefore refused with `subject_unidentified` until open question 2 is
  settled.
- **Enterprise Grid** users therefore resolve in their own workspace's
  namespace. Their organisation-wide id makes the same id valid in each member
  workspace, so a person active in two Grid workspaces needs a link in each.
  That duplicates links but guesses nothing. Folding a Grid organisation into
  one namespace needs workspace-membership evidence, and is an open question.

**Every other provider is informative** until its realization adopts links.
Each must define its function against the properties above, with its own review:

- **Microsoft Teams** has to choose the tenant that owns the sender's
  `aadObjectId`. In a shared channel that can differ from the host tenant.
  Unsupported combinations are refused.
- **Email** is not resolvable under this ADR. An aligned DMARC pass
  authenticates a domain, not a mailbox ([RFC 7489 §3.1](https://datatracker.ietf.org/doc/html/rfc7489#section-3.1)).
  Mailbox-level evidence and address reassignment need their own decision.

### 7. Resolution order

`resolve_principal` still never guesses. It applies these checks in order. The
first that fails returns an unresolved result with the named reason, and nothing
after it runs.

1. The receiving channel identity exists in the tenant. Otherwise:
   `identity_not_found`.
2. Its status is active. Otherwise: `identity_inactive`.
3. It is not marked `installation_mismatch`, and its installation is attached
   and connected. Otherwise: `installation_mismatch`, `installation_unattached`
   or `installation_disconnected`.
4. Decision 6 yields a namespace and a native id. Otherwise, one of:
   - `subject_unidentified`: no stable user id or no sender namespace, including
     an id that cannot be put in canonical form;
   - `namespace_conflict`: the sender's namespace fields disagree;
   - `provider_unsupported`: the provider has no decision 6 function yet.
5. The tenant holds that namespace. Otherwise: `namespace_unknown`.
6. The namespace is active. Otherwise: `namespace_disabled`.
7. An active link is found by exact match on `(namespace, native id)`. Revoked
   links are not consulted, so a native id whose only links are revoked gives
   `no_link`, like one never linked. The link's principal must be active,
   otherwise `principal_inactive`. A bot target gives `linked_to_bot`.

### 8. Link creation carries the trust

Resolution trusts a link it finds, so link creation is the trust boundary. This
ADR sets that policy. ADR 0166 names a link's targets, not how a link is
established.

A link needs evidence on both sides:

- the **provider account** is the one being linked;
- the **principal** is entitled to it.

Two ways satisfy both:

- **`provider_event_verified`:** a principal signed in to Curie proves control of
  the provider account in the same flow, for example by signing in through that
  provider. The link targets that principal only.
- **`admin_mapped`:** an administrator authorised for the tenant asserts the
  mapping.

Every link records its source, the acting principal and the time. Which sources
an enforcing check accepts, especially for global namespaces, belongs to #2914.

### 9. Lifecycle

Each refusal below is reached only when no earlier check in decision 7 has
already answered.

- **Installations and identities.**
  - Disconnecting an installation refuses resolution through its identities at
    decision 7, step 3.
  - Disabling or revoking an identity refuses at step 2.
  - Neither changes a namespace or its links. Other installations may report the
    same namespace.
- **Namespaces.**
  - Disabling a namespace keeps its links, and resolution into it answers
    `namespace_disabled`. Re-enabling it restores them, and nothing is recreated.
  - A namespace cannot be deleted while links reference it: the foreign keys take
    no action.

## Consequences

1. **#2909 (PR #3040)**:
   - `provider_installations` keeps the connection columns, gains `authority`,
     and takes decision 3's uniqueness.
   - `channel_identities` takes the identity columns.
   - The boot bootstrap creates unattached identities instead of the `static`
     placeholder.
2. **#2910 (PR #3054)**:
   - adds `identity_namespaces` and `identity_links` (decisions 2 and 5);
   - resolves in decision 7's order;
   - implements decision 6 for Slack. The dispatcher sends its own identity name,
     the sender's team fields and the user id from the payload.
3. **#2911 (PR #3052)**: puts a binding's reference on the channel identity,
   which `adapter` names, and tightens the bot link target to a tenant-scoped
   key.
4. **#3039**: attaches Slack identities to installations and creates their home
   namespaces. Until it lands, an operator does both through admin routes.
   Resolution through an active identity that nobody has attached answers
   `installation_unattached`, in decision 7's order.
5. **Each later provider** supplies its decision 6 function and its namespace
   kinds, reviewed against the properties there. It needs no schema change.
6. **Slack Connect users** resolve when the payload carries their own team, the
   tenant holds that workspace's namespace and an active link exists. Otherwise
   one of decision 7's reasons answers, in its order.

## Alternatives considered

1. **Links scoped to a workspace,** as an earlier revision of this Draft had it.
   This fits standard Slack only. Rejected: Teams, Google, GitHub and Atlassian
   are not workspace-shaped.
2. **Links scoped to the installation,** as ADR 0166's text reads. Rejected: the
   namespace is narrower than the installation for a Grid organisation install,
   and wider for GitHub, Google and Atlassian.
3. **Links scoped to the channel identity.** Rejected: a person needs a link per
   bot, and for Teams the bot-facing id differs per bot anyway.
4. **The namespace as a string on the link, with no table.** Rejected: there is
   no foreign key, so a mistyped key silently never matches, there is no
   per-tenant lifecycle, and nothing records which namespaces a tenant
   recognises.
5. **Infer the user's namespace from the receiving installation.** Rejected by
   decision 6: it misplaces Slack Connect users, and for Teams it can pick the
   host tenant.
6. **A table per provider,** such as `slack_workspaces`. Rejected for the reason
   ADR 0168 alternative 2 gives.
7. **Specify every provider's rule now.** Rejected: the rules for Teams shared
   channels, Grid organisations and email need evidence this ADR does not have.
   A wrong normative rule there would be a guess written into the architecture.

## Open questions

1. Folding an Enterprise Grid organisation into one namespace, using
   workspace-membership evidence.
2. How Slack reports the sending team of a Slack Connect external user in each
   payload type.
3. The Teams rule for shared channels and guest accounts.
4. Mailbox-level evidence for email, and address reassignment.
5. Replacing an `authority` when a self-hosted instance changes hostname.

## Tracking

Realized by #2909, #2910 and #2911, and by #3039 for attaching identities.
#2911 may proceed separately without its binding reference.

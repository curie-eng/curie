# 193. A provider installation is a workspace, and it hosts channel identities

Date: 2026-10-03

Status: Draft

This ADR builds on
[ADR 0166](0166-tenant-boundary-and-principal-identity-land-together.md) and
[ADR 0168](0168-one-installation-hosts-several-bot-identities.md).

When Accepted, it supersedes two parts of ADR 0168:

- decision 1's choice to make a channel identity a row in
  `provider_installations`;
- the Tracking clause that has #2909 "create one bootstrap row per declared
  identity" in that table.

It also fixes what one sentence of ADR 0168 decision 3 refers to. "ADR-0155
decision 3's installation reference on a binding is this name in `adapter`"
continues to hold, and the name it means is a channel identity's name.

Everything else in ADR 0168 stands:

- one connection per identity;
- the identity stamped from the connection;
- routes keyed `(kind, adapter, address)`.

ADR 0166 is not superseded. This ADR restores the meaning its decision 2 and
terminology table gave a provider installation. Per
[ADR 0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md),
ADR 0168 stays `Accepted` and gains a scoped back link at acceptance.

## Context

ADR 0166 decision 2 defines `provider_installations` as "a connected external
account", and its terminology table gives the example "one Slack workspace".
The same decision defines `identity_links`, which bind "one provider native id
inside one provider installation to exactly one principal or one bot".

ADR 0168 decision 1 then says: "A channel identity is a row in ADR-0155's
`provider_installations`". A channel identity is what an agent speaks as, such
as one Slack app. The decision adds that two bots in one workspace "are two
rows". ADR 0168 does not amend ADR 0166's definition.

The pending implementation of that decision, #2909 in PR #3040, follows it.

- Each declared Slack identity is a row.
- Sibling identities in one workspace repeat the same `external_account_id`.
- A row created at boot holds the placeholder `static` until an operator sets
  the real team id.

These column details belong to that unmerged pull request, not to either ADR.
The conflict surfaced while #2910 (PR #3054) was building identity links on top
of it.

Under that shape one table holds two different things:

| Concept | Cardinality | What depends on it |
| --- | --- | --- |
| Workspace or connected account | one per external tenant, such as Slack team `T1` | the namespace of provider user ids, so identity links; who installed it; whether it is connected |
| Channel identity | many per workspace | credentials, bindings, the reply route, the session key |

Folding them into one row leaves the workspace with no row of its own. The
workspace survives only as a string repeated on every sibling identity. Three
problems follow.

1. **Links have nothing to reference.** Slack assigns user ids per workspace.
   Keyed per identity, a person needs one link per sibling bot, or the link
   drops its foreign key and keys on a string.
2. **Nothing keeps siblings consistent.** Two identities in one workspace can
   record different team ids. One can still hold `static` while the other
   holds the real id. Keyed on that string, every identity still on `static`
   appears to share one workspace, even across different real workspaces.
3. **The workspace's lifecycle is tied to one bot.** Removing the last bot that
   happened to carry the team id loses the workspace and every link scoped to it.

### How this affects links and their resolution

A link answers one question at resolution time: when a provider user id
arrives, which principal is it? The answer depends on which namespace the id is
looked up in. Slack user ids are scoped to a workspace. Alice is `U1` in team
`T1`, whichever bot she messages.

Take one tenant whose Slack team `T1` runs two bots, `support` and `ops`. Under
ADR 0168's table these are two `provider_installations` rows. A third bot,
`alerts`, runs in team `T2`. A link can be keyed in only one of two ways, and
each fails.

**Keyed per row, as ADR 0166's text reads once a row is a bot.** An
administrator links `U1` to Alice on `support`.

- Alice messages `ops`. The lookup is `(ops, U1)` and finds nothing, so she is
  unresolved, although she is the same person in the same workspace. Each
  person needs one link per sibling bot, and adding a bot to a workspace leaves
  every existing person unresolved on it until they are linked again.
- The two links for `U1` are independent rows. Relinking `U1` on `support` and
  not on `ops` makes the same Slack user resolve to Alice through one bot and
  to someone else through the other.
- Links reference a bot row. Removing `support` is refused while any link
  references it, or, if the key cascades, deletes those links. Either way, a
  person's identity hangs on a bot, although the people and the workspace are
  unchanged.

**Keyed on the workspace string `(provider, external_account_id)`.** One link,
`(slack, T1, U1) → Alice`, is meant to cover both bots. Resolution has no
workspace row to read, so it takes the receiving bot's `external_account_id`
and looks the link up under it.

- If `ops` still says `static` while `support` says `T1`, Alice resolves
  through `support` and not through `ops`.
- If `ops` (in `T1`) and `alerts` (in `T2`) both still say `static`, they
  appear to be one workspace. A link made under `static` would resolve a `U1`
  arriving through either bot, although those are different workspaces. That
  is the cross-namespace guess ADR 0166 forbids.
- The link carries no foreign key. Nothing checks that `T1` is a workspace
  this tenant has connected, and a mistyped team id makes a link that never
  matches without any error.
- The bot's workspace stands in for the user's. A Slack Connect or Enterprise
  Grid user from another workspace who messages `support` is looked up under
  `T1`, which is the wrong namespace for their id.

A workspace row removes each of these failures. A link references that row,
so it covers every identity attached to the workspace. An identity attached to
no workspace resolves to `workspace_unknown` and is never matched against
another. The user's reported workspace is checked against a row the tenant
actually has.

### Why ADR 0168 chose this, and what is reversed

ADR 0168 gives two reasons that bear on its table choice:

1. **No table per provider.** Its alternative 2 rejects a table per channel
   kind, such as `slack_identities`.
2. **One row per identity, addressed by a unique name,** so a route's `adapter`
   can name it on every kind (its decision 3).

A third reason is inferred, not stated. ADR 0168 placed identities in a table
ADR 0166 had already planned, rather than adding one.

This ADR keeps the first two. `channel_identities` is one generic table for
every provider, and each row is still one identity whose unique name `adapter`
refers to. Only the reuse of `provider_installations` is reversed.

ADR 0168's context is about routing, credentials and which bot is speaking. It
does not consider identity links or the namespace of provider user ids, which
ADR 0166 had tied to the same table. Reusing the table merged two things with
different cardinalities: a workspace has many identities. That cost surfaced
only when links were being built. It is not a flaw in the routing decisions
ADR 0168 was about, and none of them changes.

No deployment has these tables yet. #2909 (PR #3040), #2910 (PR #3054) and
#2911 (PR #3052) are unmerged, so this correction needs no conversion of
existing rows.

## Decision

### 1. `provider_installations` is the workspace

`provider_installations` holds one row per connected external account in a
tenant, as ADR 0166 defines it. Its columns are `tenant_id`, `provider`,
`external_account_id`, `display_name`, `status`, `installed_by_principal_id`,
`installed_at` and `disconnected_at`.

- `external_account_id` is the provider's own account id, such as a Slack team
  id. It has no placeholder: a workspace row exists only once its account id is
  known.
- The table is unique on `(tenant_id, provider, external_account_id)`. Creating
  a workspace is an atomic find-or-create on that key, so two identities
  reporting the same team at once converge on one row.
- It is also unique on `(tenant_id, provider, id)`, the target of the channel
  identity foreign key, and on `(tenant_id, id)`, the target of the link
  foreign key.
- An installation is one workspace. An Enterprise Grid organisation-wide
  installation is not attachable to a row (decision 4).

### 2. Channel identities are child rows

A new tenant-scoped table, `channel_identities`, holds what ADR 0168 decision 1
put in `provider_installations`.

- **Columns:** `provider`; `name`, unique per `(tenant_id, provider)` and the
  value a binding's `adapter` names; `credential_ref`;
  `webhook_verification_ref`; `scopes`; `attributes`; `status`.
- **Workspace reference:** it references its workspace through a nullable
  composite foreign key `(tenant_id, provider, provider_installation_id)` to
  `provider_installations(tenant_id, provider, id)`. An identity can therefore
  attach only to a workspace of its own tenant and its own provider. NULL means
  the workspace is not yet known.
- **Attachment:** a declared Slack identity is created at boot with no
  workspace. The dispatcher's preflight calls `auth.test` with that identity's
  own token, and #3039 reports the team id it returns. The API then finds or
  creates that workspace row and attaches the identity. Siblings therefore
  converge on one row by construction.
- **Mismatch:** a later report may name a different team than the attached
  workspace. The identity is then marked `workspace_mismatch`. It keeps its
  attachment, so links are not moved. It stays ineligible for principal
  resolution until the contradiction is resolved by evidence. Either a fresh
  `auth.test` report from the identity's own token names the attached workspace
  again, or an operator reattaches it to the workspace the report names. An
  operator cannot clear the mark without one of those. Routing is unaffected: ADR 0168 routes by name, never by
  workspace.
- **Unchanged from ADR 0168:** the naming, `default`, and the chart's identity
  list. Only the table they live in changes.

### 3. Identity links reference the workspace

`identity_links` keeps ADR 0166 decision 2 as written: one provider native id
inside one provider installation, now one workspace, binds to exactly one
principal or one bot.

- It has a composite foreign key `(tenant_id, provider_installation_id)` to
  `provider_installations(tenant_id, id)`, so a link stays inside its tenant.
- It is unique on `(provider_installation_id, provider_native_id)` across all
  links, principal or bot. One native id in one workspace means one thing.
- A provider bot user that fronts several agents is identified by its channel
  identity, not by bot links.
- One link recognises a person on every channel identity attached to that
  workspace.
- Removing a channel identity never removes or moves links.

### 4. Which workspace namespaces a user, and on what evidence

The receiving identity is known from the connection the event arrived on (ADR
0168 decision 2), and the tenant is derived from that identity. A caller never
supplies or overrides either.

The user's workspace is taken only from the provider's own payload on that
authenticated connection, the same evidence the user id itself comes from. For
Slack, these are the documented
[user team fields](https://docs.slack.dev/enterprise/developing-for-enterprise-orgs/).
It is never taken from the receiving identity's workspace.

Resolution answers unresolved with a named reason when:

- the payload carries no user workspace: `workspace_unknown`;
- the payload's user workspace fields disagree with each other:
  `workspace_conflict`;
- the user's workspace is not an installation of the receiving identity's
  tenant at all: `foreign_workspace`;
- the event or the receiving identity belongs to an Enterprise Grid
  organisation-wide installation, or carries an organisation-wide user id:
  `enterprise_unsupported`.

Organisation-wide Grid identity is deliberately out of scope. Supporting it
needs its own decision about namespaces that span workspaces.

### 5. Resolution order

`resolve_principal` still never guesses. It applies these checks in order. The
first that fails produces an unresolved result with the named reason, and
nothing after it runs.

1. The receiving identity exists in the tenant. Otherwise: `installation_not_found`.
2. The receiving identity is attached to a workspace. Otherwise:
   `workspace_unknown`. It is never matched against another workspace.
3. It is not marked `workspace_mismatch`. Otherwise: `workspace_mismatch`.
4. The receiving identity's workspace is connected. Otherwise:
   `installation_disconnected`.
5. The user's workspace evidence passes decision 4, so it names an installation
   of this tenant.
6. That user workspace is connected. Otherwise: `installation_disconnected`.
7. The link is found by exact match on `(user's workspace, native id)`. Its
   principal must be active, otherwise `principal_inactive`. A bot target gives
   `linked_to_bot`, and no link gives `no_link`.

### 6. Lifecycle

- **Disconnect.** A workspace is marked `disconnected`. Its links and attached
  identities are kept. Resolution through it, as the receiving workspace, or
  into it, as the user's workspace, answers `installation_disconnected`, unless
  an earlier check in decision 5 has already answered.
  Reconnecting the same account id clears the mark, and the existing links
  apply again. Nothing is recreated.
- **Delete.** A workspace row cannot be deleted while identities or links
  reference it: the foreign keys take no action. An operator detaches or
  removes those first, which keeps deleting a workspace a deliberate act.
- **Identities.** Removing a channel identity, including the last one in a
  workspace, never changes the workspace's status.

## Consequences

1. **#2909 (PR #3040)** splits its table.
   - `provider_installations` keeps the workspace columns, with the uniqueness
     in decision 1.
   - `channel_identities` takes the identity columns.
   - The admin routes and the boot bootstrap follow. The bootstrap creates
     identities with no workspace instead of the `static` placeholder.
2. **#2910 (PR #3054)** keys links to the workspace (decision 3) and resolves in
   the order of decision 5. The dispatcher sends its own identity name and the
   user's workspace fields from the payload.
3. **#2911 (PR #3052)** puts the binding's installation reference on the channel
   identity, which `adapter` already names, not on the workspace.
4. **#3039** becomes what attaches identities to workspaces. Until it lands, or
   until an operator attaches an identity through the admin routes, resolution
   through that identity answers `workspace_unknown`.
5. ADR 0168's consumers that read identity fields read `channel_identities`.
   These are the dispatcher's identity list, binding validation and the chart.
   Their behaviour does not change.
6. Two-bot installs need one link per person per workspace, not one per bot.

## Alternatives considered

1. **Keep ADR 0168's table and key links on `(provider, external_account_id)`.**
   This is the least work. Rejected: the workspace stays an unenforced string
   repeated per bot, siblings can disagree, `static` makes unfilled identities
   look like one workspace, and links carry no foreign key.
2. **Keep links per identity, as ADR 0166's text reads under ADR 0168's table.**
   Rejected: one person needs a link per sibling bot, and an admin who links on
   one bot gets an unresolved person on its sibling.
3. **A table per provider for workspaces,** such as `slack_workspaces`.
   Rejected for the reason ADR 0168 alternative 2 gives: each provider adds a
   table and puts its fields in the schema.
4. **Infer the user's workspace from the receiving identity.** Rejected by
   decision 4: it attributes a Slack Connect or Grid user to the wrong
   namespace, which is a guess.

## Tracking

Realized by #2909, #2910 and #2911, and by #3039 for attaching identities to
workspaces. The three pull requests hold until this ADR is Accepted.

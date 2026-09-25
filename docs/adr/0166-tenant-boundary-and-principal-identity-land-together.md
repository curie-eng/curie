# 166. Tenant boundary and principal identity land together

Date: 2026-09-20

Status: Accepted

The acceptance condition, the bounded SET LOCAL probe on the pooled connection factory, is still to be run.

Accepted 2026-09-21 with explicit maintainer approval from Brian Conn, recorded
on the publishing pull request (PR #2848, commit `9eda2941`). The decision was
first published on `main` as ADR 0155; it is republished here on `next`, the
branch that feature work targets, under the next free number because `main`
was reconciled forward into `next` after this republish was first drafted,
bringing `main`'s own ADR 0155 onto `next` at its original number and pushing
this republish's number from 0162 (already reused by `next`'s own renumbered
"WorkItems own durable execution identity" ADR during that reconciliation) to
0166, the next free number as of this merge. The body below is that text,
with the Draft wording in the consequences aligned to this Accepted status.

## Context

Curie has no answer to "who" and no answer to "whose". Both gaps are old, both
are already half decided in accepted ADRs, and each new feature that needs
either one invents another untyped string column instead of resolving them.

On "whose": no `tenant_id` column exists on any table in
`apps/api/src/curie_api/models.py`.
[ADR 0008](0008-multi-tenancy.md) is Accepted and unimplemented. It already
decided the shape: one code path with a tenant count of one to many, pooled
Postgres with `FORCE ROW LEVEL SECURITY`, `SET LOCAL app.tenant_id` per
transaction, a non superuser runtime role without `BYPASSRLS`, and a CI gate
that fails when a tenant table has no policy. The chart layer already treats one
Helm release as one tenant at the compute layer
(`charts/curie/templates/tenant-resourcequota.yaml`), which is consistent with
that decision and is not the database half of it.

On "who": every authenticated caller today is a machine. `auth.py` implements
one primitive, an HMAC comparison against a shared secret, specialized into four
non overlapping trust boundaries, and its own docstring says the MVP is one
shared key. Human identity is carried as a free string wherever it is needed at
all. `Approval.author` and `Approval.resolved_by` hold either a raw provider
native id or an operator typed name. `ConsoleSession.subject`
([ADR 0083](0083-console-sessions-and-cli-minted-login-codes.md)) is
administrator selected at login code mint and points at nothing.
`QueuedTurn.author` in `packages/aci-protocol/src/aci_protocol/turn.py` is
documented as the Slack user id verbatim. The dispatcher relays Slack's own
authenticated payload straight into the approval row without a lookup against
any platform record.

Two accepted decisions already describe the missing layer without building it.
[ADR 0088](0088-per-user-delegated-oauth-for-mcp.md) is prose describing a
principal that carries the Curie workspace, the ingress provider, the provider
workspace, the provider user identifier and a canonical Curie user identifier
once account linking exists.
[ADR 0106](0106-an-approver-is-an-authenticated-principal.md) typed the approval
*credential* and deliberately stopped there: the resolved row still stores a
string. That is three decisions pointing at one absent entity, which is the
re-explanation threshold `AGENTS.md` names for promoting a question from an
issue to an ADR.

There is also a naming hazard worth closing before more code is written.
"Installation" already means a cluster install
([ADR 0097](0097-one-file-declares-an-installation.md)), a Slack app install,
and an agent bundle install. "Release" already means this platform's own git
flow process under `release/`. A fourth or fifth overload is cheap to introduce
now and expensive to unwind later.

## Decision

**Implement [ADR 0008](0008-multi-tenancy.md)'s tenant boundary and introduce a
tenant scoped principal identity layer as one decision, because neither is
correct alone: a tenant column with no principal leaves authorization on raw
provider strings, and a principal with no tenant has nothing to scope it to.**

### 1. The tenant boundary is implemented, not re-decided

[ADR 0008](0008-multi-tenancy.md)'s decisions stand as written and are not
reopened here. Self host auto provisions exactly one default tenant as a
migration data step, so there is no application branch for the single tenant
case. Postgres is the enforcement point: `FORCE ROW LEVEL SECURITY` on every
tenant scoped table, a policy on
`tenant_id = current_setting('app.tenant_id')::uuid`, a dedicated non superuser
role without `BYPASSRLS`, `SET LOCAL app.tenant_id` issued once per request
scoped and per claim scoped transaction, and a CI gate introspecting
`pg_policies` that fails when a maintained tenant table lacks a policy.

One claim in this mechanism is not asserted from reading. Whether transaction
scoped `SET LOCAL` composes safely with the existing pooled async session
factory, with no leakage across a reused connection, is an observable property
of this codebase's engine configuration and not of Postgres in the abstract.
Acceptance of this ADR is conditional on a bounded probe that demonstrates both
directions: two tenants isolated under the runtime role, and a transaction that
never issued `SET LOCAL` denied rather than served another tenant's rows.

### 2. A principal is the identity; a provider string is evidence of it

New tenant scoped tables: `principals` keyed on the IdP subject, with email and
display name as attributes that are never the key; `teams` and `principal_teams`
as a projection of the IdP's groups; `provider_installations` for a connected
external account; and `identity_links` binding one provider native id inside one
provider installation to exactly one principal or one bot.

The customer IdP stays the source of truth. `principals` is a rebuildable
projection of it, which is what makes a generic OIDC login sufficient in open
source and lets certified adapters remain an enterprise concern.

Resolution never guesses. An unlinked provider id produces an explicit
unresolved result and a refusal row in `identity_events`, never a best effort
match on email or display name. This is the same discipline the existing binding
resolver applies to an unbound channel: resolve to nothing, answer politely,
drop the event.

### 3. Bot identity extends `Agent` rather than forking a `bots` table

`Agent` already satisfies the hard half of bot identity. It has a stable id and
name, it survives release upgrades, and since
[ADR 0118](0118-binding-cardinality-is-the-multi-surface-opt-in.md) it owns more
than one channel binding. It gains `tenant_id`, a `status` of active, paused,
draining or retired, an optional owning team, and opaque policy references.
`AgentChannel` becomes the binding table the functional spec names, in place,
gaining a nullable provider installation reference so today's statically
configured rows keep working unchanged.

### 4. A release becomes shareable, and `Deployment` is not replaced

`agent_products`, `agent_releases` and `agent_installations` give a release the
publisher identity and cross bot shareability that `AgentVersion` never had, and
give an operator a record of "this bot runs this release under this policy" with
promotion history. `Deployment` keeps owning the dev and prod axis
([ADR 0091](0091-git-flow-resolves-deploy-targets-so-one-repo-serves-many-agents.md)).
An install creates or reuses a deployment; it is not a second deploy mechanism.

### 5. Resolution is a shared read only query layer, not a cross package import

`apps/worker/src/curie_worker/binding.py` states why it queries the shared
tables directly instead of importing the API package: importing it would pull
FastAPI and its ORM into the worker. The identity resolvers that
`apps/worker`, `apps/dispatcher` and `apps/mail-adapter` call follow that same
precedent rather than reversing it. No new runtime service is introduced, and no
service acquires a dependency on the API package it deliberately avoids today.

### 6. Every identity column is additive; no existing credential path changes

`Approval.author`, `Approval.resolved_by` and `ConsoleSession.subject` keep their
current meaning and keep working for a channel with no identity link. The
principal foreign keys are nullable and written alongside them. The four machine
credential dependencies in `auth.py` are untouched, and a new principal session
dependency is layered beside them rather than widening any of them, which is the
rule [ADR 0106](0106-an-approver-is-an-authenticated-principal.md) already
applies to its two non interchangeable keys.

### 7. Terminology is fixed before the code is written

| Term | Means | Collision it avoids |
| --- | --- | --- |
| `provider_installation` | A connected external account, such as one Slack workspace | The cluster install of [ADR 0097](0097-one-file-declares-an-installation.md) |
| `agent_installation` | A tenant's binding of one bot to one release under policy | Both other senses of install |
| `agent_release` | An immutable, bot shareable published bundle version | This platform's own `release/` process |
| `bot` | The durable addressable persona, today's `Agent` row | |

Normative for new code and docs under this subsystem. Existing names are not
renamed.

### 8. The enforcing flip is staged

The step that authorizes inbound events before they are enqueued is the only one
that can silently drop real traffic. It ships log only for one release before it
blocks, matching the rolling deploy discipline already used for columns that must
tolerate an older runner.

## Consequences

1. Authorization stops being a string comparison against whatever the provider
   sent. A run, an approval and an external action can each name the tenant, the
   principal, the bot, the release and the authorization version that produced
   them.
2. Postgres, not application code, becomes the last line of defense on the
   tenant boundary. A forgotten predicate in a query is no longer a cross tenant
   disclosure.
3. The open source appliance keeps working with no enterprise or hosted service
   reachable. Certified IdP adapters, SCIM, credential brokering and fleet
   governance stay on the far side of the boundary and none of them becomes a
   hard dependency.
4. Eleven ordered, independently reviewable changes follow from this decision,
   each a migration plus a minimal module, and the system stays deployable after
   every one. The realizing code paths are tracked as a linked issue set filed
   against this ADR at acceptance, per the division of labor in `AGENTS.md`.
   This Accepted ADR authorizes that work. The issue set tracks it.
5. The row level security step carries the highest blast radius in the set,
   because a misconfigured role or a missing `SET LOCAL` locks out a working
   deployment. It lands last, after every column it depends on exists, and it
   needs the widest coverage including a pooled connection regression test.
6. `packages/aci-protocol` gains additive fields on the queued turn and nothing
   else, under the existing semver and wire lock discipline
   ([ADR 0036](0036-aci-semver-and-reader-policy.md)).
7. This ADR is Accepted. Per
   [ADR 0085](0085-acceptance-not-implementation-authorizes-an-adr.md) as amended
   by [ADR 0102](0102-accepted-alongside-implementation-with-explicit-approval.md),
   a maintainer has published it as Accepted, and that publication authorizes
   implementation.

## Alternatives considered

1. **A separate `bots` table alongside `agents`.** Rejected. It creates a second
   system of record for the entity users address by name, and `Agent` already
   provides the property that is hard to retrofit, which is an identity that
   survives a release upgrade.
2. **Application level tenant filtering with no row level security.** Rejected.
   It puts the boundary in every query author's hands, and the failure mode of a
   single forgotten predicate is a silent cross tenant disclosure rather than an
   error.
3. **A database or schema per tenant.** Rejected by
   [ADR 0008](0008-multi-tenancy.md) already, on migration fan out and
   connection cost. Not re-litigated here.
4. **Replace the string author columns with foreign keys outright.** Rejected.
   It forces a big bang migration and breaks every channel whose user has no
   identity link yet, which in an early deployment is most of them.
5. **Import the API package into the worker to reuse the resolvers.** Rejected.
   It reverses a boundary `binding.py` deliberately holds, pulling FastAPI and
   an ORM into a service that does not need them.
6. **A new identity runtime service.** Rejected. It adds a network hop and a
   failure mode to the inbound path for logic that is a read only query over
   tables the callers already reach.
7. **Widen the approval principal credential into the general identity model.**
   Rejected. It is deliberately scoped to resolving one approval, and widening it
   would dissolve the property that its two signing keys are never
   interchangeable.
8. **Certified IdP adapters and SCIM in open source.** Rejected for this layer.
   A generic OIDC projection is what open source needs to be complete on its own;
   certification and directory sync are an enterprise concern and must not become
   a dependency of the appliance.
9. **Defer tenancy until a hosted offering exists.** Rejected. Every table added
   in the meantime has to be retrofitted with the column, the backfill and the
   policy, and the retrofit, not the column, is the expensive part.

## Reference

- Issue: #2917 (tracking epic), #2906-#2916 (implementation steps)
- Original publication: `main`, PR #2848, ADR 0155

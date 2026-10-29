# 254. HTTP identity applies to the channel identities an operator registers

Date: 2026-10-29

Status: Accepted

Accepted 2026-10-29 with explicit maintainer approval from jw3329, including
merging alongside its realizing code path, recorded in the publishing pull
request ([#5176](https://github.com/curie-eng/curie/pull/5176)). The same day the
maintainer separately approved decision 2's clause for agents whose invocation
role is migrated or filtered, added after the first CI run. The realizing
code path is `curie_api.identity.http_invocation.binding_requires_http_identity`
and the worker's `BindingResolver` identity classifier.

Tracked in [#4890](https://github.com/curie-eng/curie/issues/4890). Partially
supersedes [ADR 0246](0246-common-admission-resolves-an-optional-http-identity-before-authorized-enqueue.md)
decision 2, as stated below.

## Context

ADR 0246 decision 2 lets a tenant select one identity authority mode, and says
configured HTTP mode cannot be skipped by an open-route shortcut. Its realization
applies that mode to every binding of the tenant: once the tenant's HTTP
selection exists, the API refuses every channel-token turn that lacks a receiver
proof, and the worker refuses every context-free turn on any of the tenant's
bindings.

The receivers that can produce a proof are configured separately. An operator
registers each receiving channel identity in the protected API and dispatcher
files that the startup initializers load. A binding whose channel identity has
no registration cannot be reached through HTTP identity at all. Under the
tenant-wide rule, every such binding stops working the moment the tenant adopts
HTTP authority: unmigrated agents, synthetic health turns and every other
producer of context-free work. There is also no supported way to clear the
selection, so adoption cannot be rolled back either.

A tenant needs to move agents onto HTTP identity one at a time, and to roll
back by removing a receiver.

## Decision

1. The tenant's HTTP selection remains the provider configuration: issuer,
   lookup endpoint, credential reference and freshness bounds. Selecting it no
   longer moves every binding of the tenant onto HTTP identity by itself.
2. A binding requires HTTP identity when the tenant's HTTP selection is present
   and the operator's receiver registrations name the binding's channel
   identity: a Slack binding whose identity name (its adapter, or `default`)
   equals a registration's `channel_identity_name` in the same tenant. Every
   binding of that identity requires it, including bindings the registration
   does not list as permitted, which therefore refuse rather than fall back.
   A binding whose agent's invocation role has been migrated, or carries an
   agent-owned filter, also requires it even when its identity has no
   registration. Those filters are evaluated only from HTTP facts, and a
   migrated role keeps no team requirement for the legacy path to check, so
   the legacy path would admit callers the filter excludes. Such a binding
   refuses until its identity is registered or the role is changed. An
   unreadable stored policy counts as requiring HTTP identity.
3. Any other binding on an identity without a registration keeps its existing
   legacy admission path in the same tenant. So does every non-Slack binding, and every
   binding when the API was started without HTTP registrations.
4. The registrations loaded by the API are the one source of this
   classification. The API classifies its own ingress from them. The worker
   receives no registration file; for a binding whose tenant has an HTTP
   selection, it asks the API through its existing internal worker credential
   and treats an unanswered question as unavailable, never as legacy.
5. Removing a receiver's registration from both files and replacing the
   processes returns that identity's bindings to the legacy path. Clearing the
   tenant's selection is not needed for that and is still not offered.

Everything else in ADR 0246 decision 2 remains binding for a registered
identity: HTTP mode for it cannot be skipped by an open-route shortcut or a
stale admission allow, and a recognized sealed context always requires core
verification, whatever the current classification.

## Supersession

This partially supersedes ADR 0246 decision 2 in scope only. "A tenant selects
one identity authority mode" now selects the provider configuration, which
applies to the registered channel identities. "Configured HTTP mode cannot be
skipped by an open-route shortcut" applies to the bindings of those identities.
ADR 0246 remains Accepted in every other respect.

Approval continuity still classifies by tenant until its own realizing
criterion moves to the same per-identity rule. Until then, an unregistered
binding in an adopted tenant can start a turn but cannot create a
provenance-free approval.

## Consequences

An operator can register one identity, move the agents bound to it, observe
them, and add the next. A tenant that adopted HTTP authority but registered no
receivers behaves as it did before adoption for turns.

The worker's classification of a context-free turn in an adopted tenant now
depends on the API answering. Unavailable leaves the delivery pending under the
existing bounded reclaim and dead-letter budget. Tenants without an HTTP
selection make no new call.

The rollout unit is the receiving identity, not the agent. Agents that share
one identity move together.

## Alternatives considered

1. **Keep the tenant-wide rule and make every agent move together.** Gives no
   staged rollout and no rollback, and stops synthetic health turns.
2. **Record the registered identities in a database column.** Adds a second
   operator declaration that can disagree with the registration files and needs
   a new write surface. The files already hold the authority.
3. **Classify by the registration's permitted binding list.** A new binding on a
   registered identity would silently stay legacy. Classifying the whole
   identity makes such a binding refuse until the operator permits it.
4. **Give the worker a copy of the registration file.** The worker must not
   hold receiver or core keys, and a second reader can drift from the API's.

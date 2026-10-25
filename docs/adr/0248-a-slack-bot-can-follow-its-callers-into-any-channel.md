# 248. A Slack bot can follow its callers into any channel

Date: 2026-10-24

Status: Draft

## Summary

Today a Slack bot works only in channels an operator has bound by hand, one
binding per channel. Anywhere else, a mention gets a public "No agent is
configured" error that everyone in the channel can see.

This Draft lets an operator turn on "follow" for one bot identity. The bot then
answers its listed callers in any channel it is invited to, and in direct
messages. The first time a listed caller reaches the bot in a new channel, the
platform creates the binding itself and marks it as followed. Anyone else gets
silence, and nothing is created.

Who may call the bot does not get wider. Only where it answers changes: from
"channels an operator bound" to "any channel the bot is in, once a listed
caller speaks there".

## Terms

- **Binding**: one `agent_channels` row linking an agent to one place it
  listens, keyed by `(kind, adapter, address)`
  ([ADR 0168](0168-one-installation-hosts-several-bot-identities.md)
  decision 3). For Slack the address is a channel id or a direct message id.
- **Identity**: one Slack bot that Curie speaks as. One installation can host
  several, each a `channel_identities` row
  ([ADR 0198](0198-identity-links-live-in-provider-identity-namespaces.md)).
- **Declared binding**: a binding an operator created through the API or
  `curie`. Every binding today is declared.
- **Followed binding**: a binding the platform created under this decision.
  It is a real row, marked followed.
- **Follow agent**: the one agent an identity serves in channels where it has
  no binding.
- **Caller list**: the agent-level restriction on who may start a turn. It is
  either an ADR 0220 invocation team or the agent-level invocation filter of
  ADR 0246 decision 3, holding exact Slack user ids.
- **Admission**: the API's single "may this caller start a turn" check, run
  before any placeholder
  ([ADR 0175](0175-a-bot-may-limit-who-can-talk-to-it.md) decision 2).

## Context

A mention in a channel travels from the dispatcher to admission, then to the
worker. Admission looks up the binding for the route. With no binding,
`admit` in `apps/api/src/curie_api/admission.py` answers allowed and
unrestricted, and leaves the decision to routing. The dispatcher posts a
placeholder and queues the turn. The worker resolves the route in
`apps/worker/src/curie_worker/binding.py`, finds no row, and edits the
placeholder into a public error naming the channel. An unbound channel is not
silent.

Several decisions shape any change here.

1. [ADR 0118](0118-binding-cardinality-is-the-multi-surface-opt-in.md)
   decision 2 makes the number of bindings the only opt in. There is no flag
   and no enable operation, and every later mutation uses the binding
   subresource. An agent keeps at least one binding.
2. [ADR 0168](0168-one-installation-hosts-several-bot-identities.md)
   decision 3 makes a route `(kind, adapter, address)`. The identity belongs to
   the binding, not the agent. Its alternative 4 rejects an identity per agent.
3. [ADR 0175](0175-a-bot-may-limit-who-can-talk-to-it.md) puts an optional
   caller list on each binding, checks it once before any placeholder, and
   answers refused callers with silence. Its alternative 1 rejected one list
   per bot because Slack and email ids differ in shape.
4. [ADR 0220](0220-one-identity-provider-decides-who-may-invoke-and-approve-on-every-channel.md)
   sections 8 and 9 give the agent an invocation team that covers every
   binding, including bindings added later. A binding list can narrow the
   team but never widen it. Section 10 lets an approver team replace the
   channel membership default.
5. [ADR 0246](0246-common-admission-resolves-an-optional-http-identity-before-authorized-enqueue.md)
   decision 3 adds a versioned caller filter that belongs to the agent and
   applies across its channels, with exact surface accounts as entries. It
   answers ADR 0175 alternative 1's objection at the agent level. Its
   decision 1 makes the API own admission and authorized enqueue.
6. [ADR 0123](0123-a-pending-approvals-approver-set-does-not-follow-its-route-binding.md)
   keeps "channel members may approve" as the default for an approval that
   names no route.
7. [ADR 0188](0188-the-sandbox-memory-credential-is-scoped-to-its-own-channel.md)
   scopes channel memory to one binding. The state API refuses a channel the
   agent has no binding for (`apps/api/src/curie_api/routers/state.py`).

Some agents, such as a personal assistant bot for one owner, follow a small
group of people wherever they work. For those agents a binding per channel is
pure setup cost, and the person who needs the bot in a new channel is rarely
the operator. The caller list is the real security boundary. The channel list
is a second gate that, for these agents, adds friction and little protection.

## Decision

**An operator may name one follow agent for a Slack identity. That agent must
carry a caller list. When a listed caller first mentions or messages the bot in
a channel with no binding for that identity, admission creates a followed
binding before the turn proceeds. Anyone else gets silence and no row.**

### 1. Follow is an explicit, per-identity setting, off by default

- A Slack identity names at most one follow agent. The default is none, which
  keeps today's behavior exactly.
- The follow agent must already hold at least one declared binding on that
  identity. Follow extends where an agent answers through an identity it
  already uses. It never attaches an agent to a new identity.
- Only an operator sets or clears follow, through the API and `curie`. There
  is no switch inside Slack.
- Setting follow is refused unless the follow agent has a caller list (section
  2). Removing the agent's last caller restriction while follow is on is also
  refused. Follow is never open to everyone.
- Clearing follow stops new followed bindings. Existing followed bindings stay
  and keep working until an operator removes them (section 6).

### 2. The agent has one caller list

The caller list is the agent's invocation restriction. It is one of:

- the agent's ADR 0220 invocation team, or
- the agent-level invocation filter of ADR 0246 decision 3, holding exact Slack
  user, enterprise user or bot ids. Entries follow ADR 0175 decision 1's shape
  rules for Slack: exact ids with no patterns, duplicates dropped, at most 100.
  An empty or null filter does not count as a caller list for follow.

When both are set, both must pass. This decision adds no third list shape and
no follow-only list. A binding's own `allowed_callers` still narrows that one
binding, as ADR 0175 and ADR 0220 section 9 step 3 already require.

### 3. The first admitted contact creates the binding

When admission sees a Slack route with no binding, and the route's identity
names a follow agent, it decides in this order:

1. Refuse unless the event reports `is_ext_shared_channel` as exactly `false`.
   A missing value counts as shared. A channel shared with an external
   organization is never followed. An operator can still bind it by hand.
2. Check the caller against the follow agent's caller list. A caller not on
   it is refused with ADR 0175 decision 3's silence and logged as
   `caller_not_allowed`. No row is created.
3. For an admitted caller, insert the binding in the same API transaction,
   under the existing per-agent binding lock (`lock_agent_bindings` in
   `apps/api/src/curie_api/crud/channels.py`) and backed by the route's unique
   constraint, so two simultaneous first mentions create one row. The row is
   marked followed, has no binding list, and belongs to the follow agent.
4. Only then answer admitted. The worker finds a normal binding and needs no
   change.

Direct messages follow the same path. Each direct message conversation with a
listed caller becomes one followed binding.

If the API is unreachable, the caller is refused as `admission_unavailable`
and no row is created. A declared binding always wins: once any binding
exists for the route, follow plays no part and today's checks apply. When the
identity names no follow agent, an unbound route behaves exactly as today.

### 4. A followed binding is a real binding

Routing, replies, channel memory, approvals, channel reads and canvases treat
a followed binding like a declared one. Each followed channel gets its own
empty channel memory under ADR 0188. An agent with agent-wide memory enabled
carries that memory into every followed channel, as it does across declared
bindings today. No frozen contract changes: the queued turn, the session
protocol and the plugin format are untouched.

### 5. Operational paths skip followed bindings

- **Dispatcher boot preflight** probes only declared bindings. Followed
  bindings can be many, and some point at channels the bot has left, so
  probing them could stop a dispatcher from starting.
- **Deploy notices** go only to declared bindings, so a rejected build does
  not post into every followed channel.
- **Approvals with no route.** When an approval that names no route is shown
  in a followed channel, it is answerable only by the agent's approver team
  when one is set, and otherwise only by the agent's caller list. Channel
  members are not approvers there, because anyone who can invite the bot
  shapes who is in that channel. Refused clicks are audited as today.
  Approvals that name a route are unchanged.

### 6. Changes are fenced and visible

- The follow setting lives under the identity resource and the caller list
  under the agent's access policy. Both paths are already guarded by the
  admission fence in `apps/api/src/curie_api/identity_fence.py`, so every
  change to either starts a new cache generation. No stale "open" answer
  survives a change.
- An unbound route of an identity that names a follow agent answers
  "restricted", so the dispatcher caches it per caller, never as open to
  everyone. Such an identity counts as restricted for the installation-wide
  open shortcut, as ADR 0220 section 9 step 5 already requires for teams.
- Removing the bot from a channel deletes nothing, because Slack then sends
  no events. Operators list followed bindings and remove them with `curie`.
  An operator may also convert a followed binding into a declared one. A
  removed followed binding is created again if a listed caller reaches the
  bot there later.

## Relationship to earlier decisions

This Draft changes no Accepted ADR and authorizes no implementation. Upon
acceptance, the following back-links would be added under
[ADR 0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md).

It would **supersede in part**:

- **ADR 0118 decision 2**, only its "no enable operation" and "every later
  mutation uses the binding subresource" clauses. Follow is an explicit
  enable operation per identity, and the platform, not only an operator, may
  add a binding. Cardinality stays the multi-surface opt in for declared
  bindings, and an agent still keeps at least one binding.
- **ADR 0123's routeless bullet**, only for an approval shown in a followed
  channel when the agent has no approver team. The channel members default
  becomes the agent's caller list.
- **ADR 0220 decision 8's "neither team grants the other role"** and **ADR
  0246 decision 3's "invocation and approval roles do not grant each other
  permission"**, only for that same routeless approval default. Open question
  3 records the alternative that keeps both intact.

It would **extend**, without superseding:

- **ADR 0168 decision 3.** A route is still `(kind, adapter, address)` and the
  identity still belongs to the binding. The follow agent is a pointer used
  only for unbound routes, not the identity per agent its alternative 4
  rejected. The same identity can still be bound by hand to other agents.
- **ADR 0175 decisions 2 and 3.** Follow runs inside the same single check,
  before any placeholder, with the same silence. Decision 1's binding list is
  unchanged.
- **ADR 0220 sections 8 and 9.** The invocation team already covers bindings
  added later, so followed bindings are governed without new rules. The team
  is one of the two accepted caller lists.
- **ADR 0246 decision 3.** Its agent-level invocation filter is the other
  accepted caller list. This decision depends on that filter's published
  versioned contract for the id list form.

ADR 0188, ADR 0012 and ADR 0020 are unchanged. ADR 0099's `bind` trigger,
not yet built (#2936), is not fired by follow; whether it should be is left to
that work.

## Consequences

- An operator turns on follow once per identity instead of binding each
  channel. A listed caller can bring the bot into a new channel or a direct
  message without asking anyone.
- Anyone who can invite the bot can add a channel to what the agent may read,
  but only once a listed caller speaks there. Channel reads, canvases and
  agent-wide memory then reach that channel. Follow suits agents with small
  caller lists.
- Admission, read-only today, gains one write. The insert must be proven
  free of duplicates under concurrency on real Postgres.
- Followed bindings accumulate. Listing and removing them is a supported
  operator action, not an afterthought.
- One identity serves at most one follow agent. Other agents on that identity
  answer only where they are declared.
- Channels shared with an external organization still need a declared
  binding.

## Where this would land

These are realizing paths for after acceptance, not work this Draft starts.

1. `apps/api`: a migration adding the follow agent to `channel_identities` and
   a followed marker to `agent_channels`; follow-time creation in admission
   (`apps/api/src/curie_api/admission.py` and
   `apps/api/src/curie_api/routers/channels.py`); the enable and remove
   refusals; the routeless approver default in
   `apps/api/src/curie_api/slack_approvers.py`; skipping followed bindings in
   `apps/api/src/curie_api/deploy_notice.py`.
2. `apps/dispatcher`: skipping followed bindings in
   `apps/dispatcher/src/curie_dispatcher/preflight.py`; no change to the
   Slack app manifest.
3. `cli`: `curie` subcommands to set and clear follow, and to list, convert
   and remove followed bindings.
4. Docs: the channel ingress interface document, `docs/capabilities.md` and
   the operator guide.

Tests use real Postgres and Valkey and fake only Slack. Each refusal above is
proven by running it: an unlisted caller, an external shared channel, an
unreachable API, enabling follow with no caller list, and two concurrent first
mentions. End to end, on an owned disposable installation with real Slack, a
listed caller invites and mentions the bot in a new channel and gets a reply,
an unlisted caller gets nothing, and a direct message works the same way.

## Alternatives considered

1. **Create the binding when Slack reports the bot joined a channel
   (`member_joined_channel`).** Rejected. It needs a new event subscription,
   so every installed Slack app must be updated. There is no event for
   channels the bot is already in, and joins while the dispatcher is down are
   lost. The row exists before any listed caller has spoken, so anyone who
   invites the bot widens what the agent can read at once. Leaving a channel
   needs its own handling. It has the same schema needs as the decision, with
   more code and weaker guarantees.
2. **Route with no row, falling back to the identity's agent.** Rejected.
   Every reader that expects a binding breaks: channel memory refuses the
   channel, adapter approval answers are refused, and the canvas surface check
   fails. Admission would need a second check path with no row to read. It is
   exactly the address-only fallback that `apps/worker/src/curie_worker/binding.py`
   forbids, because it brings back silent misroutes.
3. **Identity provider team only.** Same as the decision, but follow accepts
   only an ADR 0220 invocation team. It keeps one source of truth and avoids
   any raw id entries. It is not rejected outright: it waits on identity
   provider teams being configured, and every caller must first link their
   Slack account. The decision accepts either form, and the maintainer may
   narrow it to this variant (open question 1).
4. **A follow-only Slack id list on the agent.** Rejected. ADR 0246 decision 3
   already places an invocation filter on the agent. A second agent-level list
   would be a third list shape beside the binding list and that filter, with
   its own validation and cache rules.
5. **Follow on by default for every identity.** Rejected. An install that
   never asked for it would start creating bindings, and the caller list
   requirement would have nothing to hold.

## Open questions

Each question has a recommended answer for the maintainer to confirm or
change.

1. **An id list, a team, or either?** Recommended: either, as section 2
   says. The id list form is ADR 0246's agent filter and waits on its
   published contract, while the team form needs identity provider teams
   configured. Whichever is ready first can enable follow. If the maintainer
   prefers one concept, narrow follow to teams only (alternative 3).
2. **Should channels shared with an external organization be followed?**
   Recommended: no. They stay declared bindings only, because people from
   outside the organization are present and the invite may come from outside.
3. **Who answers a routeless approval in a followed channel when no approver
   team is set?** Recommended: the agent's caller list, so an owner can
   approve their own bot's actions without extra setup. This partly
   supersedes the role independence of ADR 0220 and ADR 0246. The
   alternative is to refuse every answer until an approver team or approver
   filter is set, which keeps role independence intact at the cost of setup.
4. **Should direct messages follow too?** Recommended: yes. Direct messages
   are a main use for a personal bot. One row per direct message partner is
   acceptable, and operators can list and remove them.
5. **Who may turn follow on?** Recommended: operators only, through the API
   and `curie`. No self-service switch in Slack.

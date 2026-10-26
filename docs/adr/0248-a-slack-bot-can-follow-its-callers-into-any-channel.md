# 248. A Slack bot can follow its callers into any channel

Date: 2026-10-24

Status: Draft

## Summary

Today a Slack bot works only in channels an operator has bound by hand, one
binding per channel. Anywhere else, a mention gets a public "No agent is
configured" error that everyone in the channel can see.

This Draft lets an operator turn on "follow" for one bot identity. The bot then
answers its listed callers in any channel it is invited to, and in direct
messages. The first time a listed human caller reaches the bot in a new
channel, the platform creates the binding itself and marks it as followed.
Anyone else gets silence, and nothing is created.

Who may call the bot does not get wider. Only where it answers changes: from
"channels an operator bound" to "any channel the bot is in, once a listed
caller speaks there". A followed channel never falls back to "channel members
may approve", and a channel shared with an external organization is never
followed, even if it becomes shared later.

Follow depends on Slack moving onto the API's authorized enqueue from
[ADR 0246](0246-common-admission-resolves-an-optional-http-identity-before-authorized-enqueue.md)
decision 1. It cannot ship on today's split path, where the dispatcher asks
and then enqueues on its own.

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
- **Explicit approval policy**: approvers declared on an approval route, the
  agent's ADR 0220 approver team, or the agent's ADR 0246 approver filter when
  it is not null. An explicit empty approver filter is a policy that allows
  nobody.
- **Admission**: the API's single "may this caller start a turn" check, run
  before any placeholder
  ([ADR 0175](0175-a-bot-may-limit-who-can-talk-to-it.md) decision 2).
- **Authorized enqueue**: ADR 0246 decision 1. The API decides, revalidates
  the policy it decided on, and enqueues the turn in one commit.

## Context

A mention in a channel travels from the dispatcher to admission, then to the
worker. Admission looks up the binding for the route. With no binding,
`admit` in `apps/api/src/curie_api/admission.py` answers allowed and
unrestricted, and leaves the decision to routing. The dispatcher posts a
placeholder and queues the turn. The worker resolves the route in
`apps/worker/src/curie_worker/binding.py`, finds no row, and edits the
placeholder into a public error naming the channel. An unbound channel is not
silent.

Three facts about today's code matter here.

- The dispatcher asks admission, then claims and enqueues on its own. Its
  cache fence stops it from storing an answer under a newer generation, but
  it still uses a fresh answer when the generation changed during the request
  (`apps/dispatcher/src/curie_dispatcher/admission.py`). A policy change can
  commit between the decision and the enqueue.
- Admission already writes. It records refusal audits and can issue account
  connect tokens (`apps/api/src/curie_api/routers/channels.py`).
- The dispatcher builds Slack evidence, including `is_ext_shared_channel`,
  only when the event names a `user`
  (`apps/dispatcher/src/curie_dispatcher/identity.py`). A bot event with no
  `user` carries no sharing flag. A bot event that also names a bot user,
  beside its `bot_id`, does carry one, so the evidence alone cannot tell a
  human from a bot. The `bot_id` reaches admission only mixed into the caller
  ids.

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
   decision 3 adds versioned caller and approver filters that belong to the
   agent and apply across its channels, with exact surface accounts as
   entries. Null is unrestricted and an explicit empty filter matches nobody.
   It answers ADR 0175 alternative 1's objection at the agent level. Its
   decision 1 makes the API own admission and authorized enqueue, and its
   decision 4 adds current authority rechecks. Neither is built yet for Slack.
6. [ADR 0123](0123-a-pending-approvals-approver-set-does-not-follow-its-route-binding.md)
   keeps "channel members may approve" as the default for an approval that
   names no route, and for a route whose binding declares no approvers. It
   refuses an approval whose named route has lost its binding, so an operator
   change can never turn a server-checked approver set into a channel
   membership check.
7. [ADR 0188](0188-the-sandbox-memory-credential-is-scoped-to-its-own-channel.md)
   scopes the sandbox memory credential to one channel. Channel memory itself
   is keyed `kind:address` within the agent. Every identity the agent holds on
   that channel shares it, and deleting a binding does not delete it. The
   state API refuses a channel the agent holds no binding for
   (`apps/api/src/curie_api/routers/state.py`).
8. [ADR 0099](0099-hooks-are-bundle-declared-turns-the-system-starts.md)
   defines a `bind` hook trigger that fires when the agent is bound to its
   channel. It is not built yet (#2936).

Some agents, such as a personal assistant bot for one owner, follow a small
group of people wherever they work. For those agents a binding per channel is
pure setup cost, and the person who needs the bot in a new channel is rarely
the operator. The caller list is the real security boundary. The channel list
is a second gate that, for these agents, adds friction and little protection.

## Decision

**An operator may name one follow agent for a Slack identity. That agent must
carry a caller list. When a listed human caller first mentions or messages the
bot in a channel with no binding for that identity, the API creates a followed
binding and enqueues the turn in one authorized commit. Anyone else gets
silence and no row.**

### 1. Follow is an explicit, per-identity setting, off by default

- A Slack identity names at most one follow agent. The default is none, which
  keeps today's behavior exactly.
- The follow agent must already hold at least one declared binding on that
  identity. Follow extends where an agent answers through an identity it
  already uses. It never attaches an agent to a new identity. While follow
  names the agent, its last declared binding on that identity cannot be
  removed.
- Only an operator sets or clears follow, through the API and `curie`. There
  is no switch inside Slack.
- Setting follow is refused unless the follow agent has a caller list (section
  2).
- Removing the agent's last caller restriction is refused while follow names
  the agent **or** while the agent holds any followed binding. To open the
  agent to everyone, an operator first clears follow and removes or converts
  every followed binding. Follow is never open to everyone.
- Clearing follow stops new followed bindings. Existing followed bindings stay
  and keep working until an operator removes them (section 8).

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

Follow requires Slack to be on ADR 0246's authorized enqueue (its realization
item 2). The API makes the decision, creates the binding and enqueues the turn
in one commit, after the revalidation ADR 0246 decision 1 requires. An allow
answer followed by a separate enqueue is not enough, because a caller list
change or a follow change could commit between the two. Follow does not ship
on the current split path.

When admission sees a Slack route with no binding, and the route's identity
names a follow agent, it decides in this order:

1. Refuse a bot-authored event, logged as `follow_human_only`, even when
   its bot id is on the caller list (section 4).
2. Refuse unless the evidence reports `is_ext_shared_channel` as exactly
   `false`. A missing value counts as shared. The refusal is logged as
   `follow_shared_channel`. An operator can still bind such a channel by hand.
3. Check the caller against the follow agent's caller list. A caller not on
   it is logged as `caller_not_allowed`.
4. For an admitted caller, take `lock_agent_bindings`
   (`apps/api/src/curie_api/crud/channels.py`) and look up the route again.
   The follow agent always holds a declared binding, so this lock orders
   concurrent first contacts for the agent, and the second one sees the row
   the first one created.
   - If a row now exists, insert nothing. Evaluate the caller against that row
     exactly as today's admission would, and proceed or refuse through it.
   - Otherwise insert the binding. The row is marked followed, has no binding
     list, and belongs to the follow agent.
   - If the insert hits the route's unique constraint, because an operator or
     another agent's binding committed first, roll back only the insert, read
     the winning row, and evaluate the caller against it as above.
5. Revalidate, enqueue and commit together. Two authorized callers who mention
   the bot at the same moment both get their turns, through one row.

Every refusal is ADR 0175 decision 3's silence. No row is created and no
placeholder is posted.

Direct messages follow the same path. Each direct message conversation with a
listed caller becomes one followed binding.

If the API is unreachable, the caller is refused as `admission_unavailable`
and no row is created. When the identity names no follow agent, an unbound
route behaves exactly as today.

### 4. A followed binding keeps follow's limits on every event

Once a followed binding exists, admission applies today's checks for that row
and two follow conditions on every event, not only the first.

- **External sharing is checked continuously.** An event must report
  `is_ext_shared_channel` as exactly `false`. If a followed channel becomes
  shared with an external organization, or the flag is missing, the binding
  stops admitting, logged as `follow_shared_channel`. It stays silent until an
  operator converts it into a declared binding or removes it. Nothing is
  deleted.
- **Bot-authored events are refused.** An event is bot-authored when Slack
  marks it with a `bot_id`, whether or not it also names a user. Follow
  refuses it at first contact and on an existing followed binding, logged as
  `follow_human_only`, whatever its sharing evidence says. The dispatcher
  passes that marker to admission as its own field of the Slack evidence,
  not inferred from the caller ids or from a missing `user`. Once ADR 0246
  decision 4's authenticated principal kind exists, it replaces this marker.
  Listed bots therefore cannot start turns in a followed channel, and never
  create one. They keep working in declared bindings. This is deliberate:
  anyone can make an invited bot speak, and ADR 0246 decision 4 already
  denies non-human invocation until its own policy is qualified. Open
  question 6 covers lifting it later.

A declared binding carries neither condition. Converting a followed binding
removes both.

### 5. A followed binding is otherwise a real binding

Routing, replies, channel memory, approvals (with section 7's rule), channel
reads and canvases treat a followed binding like a declared one.

Channel memory keeps its current scope: `kind:address` within the agent,
shared by every identity the agent holds on that channel, and kept when a
binding is deleted. A followed binding does not get fresh memory. If the agent
already has memory for that channel, from a binding through another identity
or from an earlier followed binding that was removed, the followed binding
sees it at once. The state API refuses a channel with no binding, so an
operator who wants that memory gone clears it before removing the binding. An
agent with agent-wide memory enabled carries that memory into every followed
channel, as it does across declared bindings today.

No frozen contract changes: the queued turn, the session protocol and the
plugin format are untouched.

### 6. Operational paths skip followed bindings

- **Dispatcher boot preflight** probes only declared bindings. Followed
  bindings can be many, and some point at channels the bot has left, so
  probing them could stop a dispatcher from starting.
- **Deploy notices** go only to declared bindings, so a rejected build does
  not post into every followed channel.
- **The ADR 0099 `bind` trigger** fires only when an operator declares a
  binding. Creating a followed binding does not fire it. The caller's own
  turn is already arriving, and a platform-started turn in every channel
  someone invited the bot to is what follow must avoid. Converting a followed
  binding into a declared one does not fire it either, since the agent was
  already bound there.

### 7. Approvals never fall back to channel members in a followed channel

When no explicit approval policy applies, a Slack approval today is answerable
by the members of the channel its card is shown in. ADR 0123 keeps that default
for a routeless approval and for a route whose binding declares no approvers.
In a followed channel, anyone who can invite the bot shapes that membership,
so the default does not apply there.

- **Explicit approval policy comes first and always governs**: route-declared
  approvers, the agent's ADR 0220 approver team and the agent's ADR 0246
  approver filter, combined as those ADRs already combine them. An explicit
  empty approver filter allows nobody, in a followed channel too. ADR 0123's
  refusal when a named route has lost its binding is unchanged.
- **Only when no explicit approval policy is set**, the agent's caller list
  stands in for channel members.
- **Which approvals this covers.** When an approval is created, it records
  whether its card channel is held by a followed binding. It is covered if
  that record is set, or if its card channel is held by a followed binding
  when someone answers. The record rules out the channel members default and
  nothing else. It is not a snapshot of who may answer.
- **Membership is checked fresh** at answer time, from current policy, as ADR
  0123 and ADR 0246 decision 4 already require. Converting, deleting or
  recreating the binding while the approval waits never brings channel
  members back.
- **Fail closed.** If neither an explicit approval policy nor a caller list
  can be established at answer time, for example because the agent is gone or
  its policy cannot be read, the answer is refused and audited with that
  reason. Refused clicks are audited as today.

### 8. Changes are fenced and visible

- The follow setting lives under the identity resource and the caller list
  under the agent's access policy. Both paths already bump the admission fence
  in `apps/api/src/curie_api/identity_fence.py`, so the dispatcher never
  reuses or stores an answer from before a change. The fence protects that
  cache only. The authorized enqueue in section 3 is what keeps a change from
  slipping in between a decision and its turn.
- An unbound route of an identity that names a follow agent answers
  "restricted", so the dispatcher caches it per caller, never as open to
  everyone. Such an identity counts as restricted for the installation-wide
  open shortcut, as ADR 0220 section 9 step 5 already requires for teams.
- Removing the bot from a channel deletes nothing, because Slack then sends
  no events. Operators list followed bindings and remove them with `curie`.
  An operator may also convert a followed binding into a declared one. A
  removed followed binding is created again if a listed caller reaches the
  bot there later, and it sees the same channel memory (section 5).
- A followed binding never blocks a declared one. An operator's declared
  create on a route held by a followed binding replaces that row in one
  transaction. For the same agent this is a conversion. For another agent the
  followed row is removed. Pending approvals keep section 7's rule either way.

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
- **ADR 0123's channel members default**, both its routeless bullet and its
  "route present, no approvers" bullet, only for an approval covered by
  section 7 when no explicit approval policy is set. The default becomes the
  agent's caller list. ADR 0123's refusal for a named route with no binding is
  unchanged.
- **ADR 0220 decision 8's "neither team grants the other role"** and **ADR
  0246 decision 3's "invocation and approval roles do not grant each other
  permission"**, only for that same default, and only when no explicit
  approval policy exists. Explicit approver teams and filters, including an
  empty filter, are untouched. Open question 3 records the alternative that
  keeps both intact.
- **ADR 0099's `bind` trigger**, only its scope: `bind` fires when an operator
  declares a binding, not when the platform creates a followed binding (section
  6). Its other triggers and its control plane emission are unchanged.

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
- **ADR 0246 decision 1.** Follow depends on Slack's move onto API-owned
  authorized enqueue and adds binding creation to that same commit.
- **ADR 0246 decision 3.** Its agent-level invocation filter is the other
  accepted caller list, and its approver filter is part of the explicit
  approval policy in section 7. This decision depends on those filters'
  published versioned contract for the id list form.

ADR 0188, ADR 0012 and ADR 0020 are unchanged.

## Consequences

- An operator turns on follow once per identity instead of binding each
  channel. A listed caller can bring the bot into a new channel or a direct
  message without asking anyone.
- Anyone who can invite the bot can add a channel to what the agent may read,
  but only once a listed caller speaks there. Channel reads, canvases and
  agent-wide memory then reach that channel. Follow suits agents with small
  caller lists.
- A followed channel can surface channel memory the agent already holds there,
  from another identity's binding or from an earlier followed binding.
  Removing a followed binding does not erase that memory. Clearing it is a
  separate step that must come first.
- Follow cannot ship before Slack moves onto ADR 0246's authorized enqueue.
- Admission, which already writes refusal audits and connect tokens, gains a
  binding creation side effect. It must be proven free of duplicates and lost
  turns under concurrency on real Postgres.
- Followed bindings accumulate. Listing and removing them is a supported
  operator action, not an afterthought.
- To open a follow agent to everyone, an operator must first clear follow and
  remove or convert its followed bindings.
- One identity serves at most one follow agent. Other agents on that identity
  answer only where they are declared.
- Listed bots cannot start turns in followed channels. They need declared
  bindings.
- A channel shared with an external organization needs a declared binding.
  A followed channel that becomes shared goes silent until an operator
  converts it.
- In a followed channel, channel members never approve by default. An
  approval there can become unanswerable if its agent's policy cannot be read.
  Its expiry bounds that, as under ADR 0123.

## Where this would land

These are realizing paths for after acceptance, not work this Draft starts.

1. `apps/api`: a migration adding the follow agent to `channel_identities`, a
   followed marker to `agent_channels` and the followed channel record to
   approvals; follow-time creation inside Slack's authorized enqueue
   (`apps/api/src/curie_api/admission.py` and
   `apps/api/src/curie_api/routers/channels.py`), after ADR 0246 realization
   item 2; the continuous sharing check; the enable and remove refusals; the
   approver default in `apps/api/src/curie_api/slack_approvers.py`; skipping
   followed bindings in `apps/api/src/curie_api/deploy_notice.py`; declared
   create replacing a followed row in `apps/api/src/curie_api/crud/channels.py`.
2. `apps/dispatcher`: skipping followed bindings in
   `apps/dispatcher/src/curie_dispatcher/preflight.py`; the bot-authored
   marker in the Slack evidence built by
   `apps/dispatcher/src/curie_dispatcher/identity.py`; no change to the Slack
   app manifest. Its separate admission call goes away with ADR 0246's
   migration, not with this decision.
3. `apps/worker`: no follow-specific change. Followed bindings get ADR 0246
   decision 4's current authority recheck like every other binding.
4. `cli`: `curie` subcommands to set and clear follow, and to list, convert
   and remove followed bindings.
5. Docs: the channel ingress interface document, `docs/capabilities.md` and
   the operator guide.

Tests use real Postgres and Valkey and fake only Slack. Each rule above is
proven by running it:

- an unlisted caller, an unreachable API, and enabling follow with no caller
  list are refused, with no row;
- an externally shared channel at first contact is refused, and a followed
  channel that later reports shared, or no flag, stops admitting;
- a bot-only first contact from a listed bot id is refused, with no row;
- a bot event that carries `user`, `bot_id` and `is_ext_shared_channel`
  `false` is refused, both on an unbound route and on an existing followed
  binding;
- removing the last caller restriction is refused while followed bindings
  exist, after follow is cleared;
- a caller list change that commits between decision and enqueue leaves no
  turn;
- two simultaneous first mentions from two listed callers both proceed, and
  exactly one row exists;
- a first mention racing an operator's declared create ends with the declared
  row, and the mention proceeds or is refused by that row's policy;
- in a followed channel, an explicit empty approver filter refuses every
  answer;
- a pending approval whose followed binding is converted, deleted and then
  recreated refuses a channel member who is not a listed caller at each step.

End to end, on an owned disposable installation with real Slack, a listed
caller invites and mentions the bot in a new channel and gets a reply, an
unlisted caller gets nothing, and a direct message works the same way.

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
6. **Check external sharing only when the binding is created.** Rejected. A
   channel can be shared with an external organization later, and the
   followed binding would then answer and read there with nobody having
   chosen that.
7. **Fresh memory for each followed binding.** Rejected for now. Memory is
   keyed by channel within the agent and shared across identities, so a fresh
   start would need a new memory generation key that collides with the other
   bindings sharing that scope. Disclosing the shared, retained scope is
   simpler and honest.

## Open questions

Each question has a recommended answer for the maintainer to confirm or
change.

1. **An id list, a team, or either?** Recommended: either, as section 2
   says. The id list form is ADR 0246's agent filter and waits on its
   published contract, while the team form needs identity provider teams
   configured. Whichever is ready first can enable follow. If the maintainer
   prefers one concept, narrow follow to teams only (alternative 3).
2. **Should channels shared with an external organization be followed?**
   Recommended: no, checked on every event and not only at creation. They
   stay declared bindings only, because people from outside the organization
   are present and the invite may come from outside.
3. **Who answers an approval in a followed channel when no explicit approval
   policy is set?** Recommended: the agent's caller list, so an owner can
   approve their own bot's actions without extra setup. This partly
   supersedes the role independence of ADR 0220 and ADR 0246. The
   alternative is to refuse every answer until an approver team or approver
   filter is set, which keeps role independence intact at the cost of setup.
   Under either answer, an explicit approval policy governs first, including
   an empty filter that allows nobody.
4. **Should direct messages follow too?** Recommended: yes. Direct messages
   are a main use for a personal bot. One row per direct message partner is
   acceptable, and operators can list and remove them.
5. **Who may turn follow on?** Recommended: operators only, through the API
   and `curie`. No self-service switch in Slack.
6. **Should listed bots work in followed channels?** Recommended: not in this
   decision. It needs ADR 0246's authenticated principal kind and its
   non-human invocation policy to be qualified, and the sharing flag sent for
   every bot event. Even then, a bot's first contact should not create a followed
   binding.

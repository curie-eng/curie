# 248. A Slack bot can follow its callers into any channel

Date: 2026-10-24

Status: Draft

## Summary

Today a Slack bot answers only in channels an operator has bound by hand.
Anywhere else, a mention gets a public "No agent is configured" error.

This Draft lets an operator turn on "follow" for one bot identity. When a
listed human caller first reaches the bot in a new channel or a direct message,
the platform creates a followed binding and answers. Anyone else gets silence,
and nothing is created.

Who may call the bot does not widen, only where it answers. A followed channel
never lets channel members approve by default, and a channel shared with an
external organization is never followed, even if it becomes shared later.
Follow ships only on the API's authorized enqueue ([ADR
0246](0246-common-admission-resolves-an-optional-http-identity-before-authorized-enqueue.md)
decision 1).

## Terms

- **Binding**: an `agent_channels` row routing one `(kind, adapter, address)`
  to an agent ([ADR
  0168](0168-one-installation-hosts-several-bot-identities.md) decision 3). For
  Slack the address is a channel or direct message id.
- **Identity**: one Slack bot Curie speaks as, a `channel_identities` row. An
  installation can host several ([ADR
  0198](0198-identity-links-live-in-provider-identity-namespaces.md)).
- **Declared binding**: created by an operator through the API or `curie`, as
  every binding is today.
- **Followed binding**: a real row the platform creates under this decision,
  marked followed.
- **Follow agent**: the one agent an identity serves where it has no binding.
- **Caller list**: the agent-level restriction on who may start a turn (section
  2).
- **Explicit approval policy**: route-declared approvers, the agent's ADR 0220
  approver team, or its ADR 0246 approver filter when not null. An explicit
  empty approver filter allows nobody.
- **Admission**: the API's single "may this caller start a turn" check, run
  before any placeholder ([ADR
  0175](0175-a-bot-may-limit-who-can-talk-to-it.md) decision 2).
- **Authorized enqueue**: ADR 0246 decision 1: the API decides, revalidates
  that policy and enqueues the turn in one commit.

## Context

With no binding for a route, `admit` in `apps/api/src/curie_api/admission.py`
answers allowed and unrestricted, the dispatcher posts a placeholder and queues
the turn, and the worker (`apps/worker/src/curie_worker/binding.py`) finds no
row and edits the placeholder into a public error naming the channel. Three
more facts matter.

- The dispatcher asks admission, then enqueues on its own. Its cache fence
  still uses a fresh answer when the generation changed during the request
  (`apps/dispatcher/src/curie_dispatcher/admission.py`), so a policy change can
  commit between decision and enqueue.
- Admission already writes refusal audits and account connect tokens
  (`apps/api/src/curie_api/routers/channels.py`).
- Slack evidence, including `is_ext_shared_channel`, is built only for events
  that name a `user` (`apps/dispatcher/src/curie_dispatcher/identity.py`). A
  bot event may or may not name a bot user beside its `bot_id`, so the evidence
  alone cannot tell a human from a bot, and the `bot_id` reaches admission only
  mixed into the caller ids.

Callers are already restricted per binding (ADR 0175) or per agent (ADR 0220
invocation teams, ADR 0246 decision 3 filters), but every channel still needs
its own binding. Some agents, such as a personal assistant for one owner,
follow a few people wherever they work. For them a binding per channel is pure
setup cost, and whoever needs the bot in a new channel is rarely the operator.
The caller list is the real security boundary. The channel list adds friction
and little protection.

## Decision

**An operator may name one follow agent for a Slack identity. That agent must
carry a caller list. When a listed human caller first mentions or messages the
bot in a channel with no binding for that identity, the API creates a followed
binding and enqueues the turn in one authorized commit. Anyone else gets
silence and no row.**

### 1. Follow is an explicit, per-identity setting, off by default

- A Slack identity names at most one follow agent. The default is none: today's
  behavior exactly.
- The follow agent must already hold a declared binding on that identity, so
  follow never attaches an agent to a new identity. While follow names the
  agent, its last declared binding there cannot be removed.
- Only an operator sets or clears follow, through the API and `curie`, never
  from inside Slack.
- Setting follow is refused unless the agent has a caller list (section 2).
  Removing its last caller restriction is refused while follow names it **or**
  it holds any followed binding. Follow is never open to everyone: to open the
  agent, an operator first clears follow and removes or converts every followed
  binding.
- Clearing follow stops new followed bindings. Existing ones keep working until
  an operator removes them.

### 2. The agent has one caller list

The caller list is the agent's ADR 0220 invocation team, or its ADR 0246
decision 3 invocation filter of exact Slack user, enterprise user or bot ids
under ADR 0175 decision 1's Slack rules (no patterns, duplicates dropped, at
most 100). An empty or null filter does not count. When both are set, both must
pass. There is no third list shape and no follow-only list. A binding's own
`allowed_callers` still narrows that binding (ADR 0175, ADR 0220 section 9 step
3).

### 3. The first admitted contact creates the binding

Follow requires Slack on ADR 0246's authorized enqueue, which its realization
item 2 has not yet built: the API decides, creates the binding, revalidates the
policy it decided on as ADR 0246 decision 1 requires, and enqueues the turn in
one commit. On today's split path a caller list or follow change could commit
between allow and enqueue, so follow does not ship there.

For a Slack route with no binding whose identity names a follow agent,
admission:

1. Refuses a bot-authored event as `follow_human_only`, even when its bot id is
   listed (section 4).
2. Refuses as `follow_shared_channel` unless `is_ext_shared_channel` is exactly
   `false`. A missing value counts as shared. An operator can still bind such a
   channel by hand.
3. Refuses a caller not on the caller list as `caller_not_allowed`.
4. Takes `lock_agent_bindings` (`apps/api/src/curie_api/crud/channels.py`) and
   looks up the route again. The follow agent always holds a declared binding,
   so this lock orders concurrent first contacts and the second sees the first
   one's row.
   - If a row now exists, it inserts nothing and evaluates the caller against
     that row exactly as today's admission would.
   - Otherwise it inserts a binding marked followed, with no binding list,
     belonging to the follow agent.
   - If the insert hits the route's unique constraint (an operator's or another
     agent's binding committed first), it rolls back only the insert and
     evaluates the caller against the winning row.
5. Revalidates, enqueues and commits together. Two listed callers who mention
   the bot at once both get their turns, through one row.

Every refusal, logged with its reason, is ADR 0175 decision 3's silence: no
row, no placeholder. An unreachable API refuses as `admission_unavailable`,
with no row. Direct messages take the same path, one followed binding per
conversation with a listed caller. An identity that names no follow agent
behaves exactly as today.

### 4. A followed binding keeps follow's limits on every event

On every event, admission applies today's checks for the row plus two follow
conditions. A declared binding carries neither, and converting a followed
binding removes both.

- **External sharing.** Unless an event reports `is_ext_shared_channel` as
  exactly `false`, the binding stops admitting (`follow_shared_channel`) and
  stays silent until an operator converts or removes it. Nothing is deleted.
- **Bot-authored events.** An event is bot-authored when Slack marks it with a
  `bot_id`, with or without a user. Follow refuses it as `follow_human_only` at
  first contact and on followed bindings, whatever its sharing evidence says.
  The dispatcher passes this marker as its own Slack evidence field, not
  inferred from the caller ids or a missing `user`, until ADR 0246 decision 4's
  authenticated principal kind replaces it. Listed bots therefore never create
  a followed channel or start turns in one, but keep working in declared
  bindings. This is deliberate: anyone can make an invited bot speak, and ADR
  0246 decision 4 already denies non-human invocation until its policy is
  qualified (open question 6).

### 5. A followed binding is otherwise a real binding

Routing, replies, channel memory, approvals (under section 7), channel reads
and canvases treat a followed binding like a declared one. No frozen contract
changes: the queued turn, the session protocol and the plugin format are
untouched.

Channel memory keeps its [ADR
0188](0188-the-sandbox-memory-credential-is-scoped-to-its-own-channel.md)
scope: keyed `kind:address` within the agent, shared by the agent's identities
on that channel, and kept when a binding is deleted. A followed binding
therefore sees at once whatever memory the agent already holds there, from
another identity's binding or an earlier, removed followed binding. The state
API (`apps/api/src/curie_api/routers/state.py`) refuses a channel with no
binding, so an operator who wants that memory gone clears it before removing
the binding. An agent with agent-wide memory enabled carries it into every
followed channel, as it does across declared bindings.

### 6. Operational paths skip followed bindings

- **Dispatcher boot preflight** probes only declared bindings. Followed ones
  can be many, some in channels the bot has left, and could stop a dispatcher
  from starting.
- **Deploy notices** go only to declared bindings, so a rejected build does not
  post into every followed channel.
- **The [ADR 0099](0099-hooks-are-bundle-declared-turns-the-system-starts.md)
  `bind` trigger** (not built yet, #2936) fires only when an operator declares
  a binding. Creating a followed binding does not fire it: the caller's turn is
  already arriving, and a platform-started turn in every channel the bot is
  invited to is what follow must avoid. Converting one does not fire it either,
  since the agent was already bound there.

### 7. Approvals never fall back to channel members in a followed channel

[ADR
0123](0123-a-pending-approvals-approver-set-does-not-follow-its-route-binding.md)
lets channel members answer an approval with no route, or whose route's binding
declares no approvers. Anyone who can invite the bot shapes a followed
channel's membership, so that default never applies there.

- **Explicit approval policy always governs first**: route-declared approvers,
  the ADR 0220 approver team and the ADR 0246 approver filter, combined as
  those ADRs already combine them. An explicit empty filter allows nobody here
  too. ADR 0123's refusal when a named route has lost its binding, which keeps
  an operator change from turning a server-checked approver set into a
  membership check, is unchanged.
- **Only with no explicit approval policy** does the agent's caller list stand
  in for channel members.
- **Which approvals this covers.** An approval records at creation whether a
  followed binding holds its card channel. It is covered if that record is set
  or a followed binding holds its card channel at answer time. The record rules
  out the channel members default and nothing else; it is not a snapshot of who
  may answer.
- **Membership is checked fresh** at answer time from current policy (ADR 0123,
  ADR 0246 decision 4). Converting, deleting or recreating the binding while
  the approval waits never brings channel members back.
- **Fail closed.** If neither an explicit approval policy nor a caller list can
  be established at answer time (the agent is gone, or its policy cannot be
  read), the answer is refused and audited with that reason. Refused clicks are
  audited as today.

### 8. Changes are fenced and visible

- The follow setting (on the identity resource) and the caller list (in the
  agent's access policy) both already bump the admission fence
  (`apps/api/src/curie_api/identity_fence.py`), so the dispatcher never reuses
  or stores a pre-change answer. The fence protects only that cache, not the
  enqueue (section 3).
- An unbound route of a follow identity answers "restricted", so the dispatcher
  caches it per caller, never as open to everyone, and the identity counts as
  restricted for the installation-wide open shortcut (as ADR 0220 section 9
  step 5 requires for teams).
- Removing the bot from a channel deletes nothing, since Slack then sends no
  events. Operators list, remove, and convert followed bindings into declared
  ones with `curie`. A removed followed binding is recreated if a listed caller
  returns, with the same channel memory.
- A followed binding never blocks a declared one. An operator's declared create
  on its route replaces it in one transaction: a conversion for the same agent,
  a removal for another agent. Pending approvals keep section 7's rule either
  way.

## Relationship to earlier decisions

This Draft changes no Accepted ADR and authorizes no implementation. Upon
acceptance, the following back-links would be added under [ADR
0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md).

It would **supersede in part**:

- **[ADR 0118](0118-binding-cardinality-is-the-multi-surface-opt-in.md)
  decision 2**, only its "no enable operation" and "every later mutation uses
  the binding subresource" clauses: follow is a per-identity enable operation,
  and the platform may add a binding. Cardinality stays the multi-surface opt
  in for declared bindings, and an agent still keeps at least one binding.
- **ADR 0123's channel members default** (its routeless and its "route present,
  no approvers" bullets), only for an approval covered by section 7 with no
  explicit approval policy, where the default becomes the caller list. Its
  refusal for a named route with no binding is unchanged.
- **[ADR
  0220](0220-one-identity-provider-decides-who-may-invoke-and-approve-on-every-channel.md)
  decision 8's "neither team grants the other role"** and **ADR 0246 decision
  3's "invocation and approval roles do not grant each other permission"**,
  only for that same default. Explicit approver teams and filters, including an
  empty filter, are untouched. Open question 3 records the alternative that
  keeps both intact.
- **ADR 0099's `bind` trigger**, only its scope: operator-declared bindings,
  not followed ones (section 6). Its other triggers and its control plane
  emission are unchanged.

It would **extend**, without superseding:

- **ADR 0168 decision 3.** Routes stay `(kind, adapter, address)` and the
  identity still belongs to the binding. The follow agent is a pointer used
  only for unbound routes, not the identity per agent its alternative 4
  rejected, and the identity can still be bound by hand to other agents.
- **ADR 0175 decisions 2 and 3.** Follow runs inside the same single check,
  before any placeholder, with the same silence. Decision 1's binding list is
  unchanged.
- **ADR 0220 sections 8 and 9.** The invocation team already covers later
  bindings, so it governs followed bindings without new rules, as one of the
  two caller lists.
- **ADR 0246 decision 1.** Follow depends on Slack's move onto API-owned
  authorized enqueue and adds binding creation to that commit.
- **ADR 0246 decision 3.** Its invocation filter, which already answers ADR
  0175 alternative 1's objection to a per-bot list (Slack and email ids differ
  in shape) at the agent level, is the other caller list, and its approver
  filter is part of the explicit approval policy. The id list form depends on
  those filters' published versioned contract.

ADR 0188, ADR 0012 and ADR 0020 are unchanged.

## Consequences

- Follow is set once per identity, and a listed caller brings the bot into a
  new channel or direct message without asking anyone.
- Anyone who can invite the bot can widen what the agent reads (channel reads,
  canvases, agent-wide memory) once a listed caller speaks there, so follow
  suits small caller lists.
- A followed channel can surface memory the agent already holds there, and
  clearing it must precede removing the binding.
- Admission gains a binding creation side effect that must be proven free of
  duplicates and lost turns under concurrency on real Postgres.
- Followed bindings accumulate, so listing and removing them is a supported
  operator action, and other agents on a follow identity, listed bots and
  externally shared channels still need declared bindings.
- An approval in a followed channel can become unanswerable if its agent's
  policy cannot be read, bounded by its expiry as under ADR 0123.

## Where this would land

These are realizing paths for after acceptance, not work this Draft starts.

1. `apps/api`: a migration adding the follow agent to `channel_identities`, a
   followed marker to `agent_channels` and the followed channel record to
   approvals; follow-time creation inside Slack's authorized enqueue
   (`apps/api/src/curie_api/admission.py`,
   `apps/api/src/curie_api/routers/channels.py`) after ADR 0246 realization
   item 2; the continuous sharing check; the enable and remove refusals; the
   approver default in `apps/api/src/curie_api/slack_approvers.py`; skipping
   followed bindings in `apps/api/src/curie_api/deploy_notice.py`; declared
   create replacing a followed row in
   `apps/api/src/curie_api/crud/channels.py`.
2. `apps/dispatcher`: skipping followed bindings in
   `apps/dispatcher/src/curie_dispatcher/preflight.py`; the bot-authored marker
   in `apps/dispatcher/src/curie_dispatcher/identity.py`; no Slack app manifest
   change. Its separate admission call goes away with ADR 0246's migration, not
   this decision.
3. `apps/worker`: no follow-specific change. ADR 0246 decision 4's current
   authority recheck covers followed bindings like any other.
4. `cli`: `curie` subcommands to set and clear follow, and to list, convert and
   remove followed bindings.
5. Docs: the channel ingress interface document, `docs/capabilities.md` and the
   operator guide.

Tests use real Postgres and Valkey, fake only Slack, and prove each rule by
running it:

- refused with no row: an unlisted caller, an unreachable API, enabling follow
  with no caller list, and a bot-only first contact from a listed bot id;
- a bot event carrying `user`, `bot_id` and `is_ext_shared_channel` `false` is
  refused on an unbound route and on a followed binding;
- an externally shared channel is refused at first contact, and a followed
  channel later reporting shared, or no flag, stops admitting;
- removing the last caller restriction is refused while followed bindings
  exist, after follow is cleared;
- a caller list change committed between decision and enqueue leaves no turn;
- two simultaneous first mentions from listed callers both proceed with one
  row, and one racing an operator's declared create ends with the declared row,
  whose policy decides it;
- in a followed channel, an explicit empty approver filter refuses every
  answer, and a pending approval whose binding is converted, deleted and
  recreated refuses an unlisted channel member at each step.

End to end, on an owned disposable installation with real Slack: a listed
caller invites and mentions the bot in a new channel and gets a reply, an
unlisted caller gets nothing, and a direct message works the same way.

## Alternatives considered

1. **Create the binding when Slack reports the bot joined a channel
   (`member_joined_channel`).** Rejected. Every installed Slack app needs a new
   event subscription, channels the bot is already in send no event, joins
   during dispatcher downtime are lost, and leaving needs its own handling. The
   row exists before any listed caller speaks, so any inviter widens what the
   agent reads at once. Same schema, more code, weaker guarantees.
2. **Route with no row, falling back to the identity's agent.** Rejected. Every
   reader that expects a binding breaks (channel memory, adapter approval
   answers, the canvas surface check), and admission would need a second path
   with no row. It is the address-only fallback
   `apps/worker/src/curie_worker/binding.py` forbids because it brings back
   silent misroutes.
3. **Identity provider team only.** Follow would accept only an ADR 0220
   invocation team: one source of truth, no raw ids. Not rejected outright, but
   it waits on configured identity provider teams and on every caller linking
   their Slack account. The maintainer may narrow the decision to it (open
   question 1).
4. **A follow-only Slack id list on the agent.** Rejected. ADR 0246 decision 3
   already puts an invocation filter on the agent; another list would be a
   third shape with its own validation and cache rules.
5. **Follow on by default for every identity.** Rejected. Installs that never
   asked would start creating bindings, and the caller list requirement would
   have nothing to hold.
6. **Check external sharing only when the binding is created.** Rejected. A
   channel shared later would leave the followed binding answering and reading
   there without anyone choosing that.
7. **Fresh memory for each followed binding.** Rejected for now. Memory is
   keyed by channel within the agent and shared across identities, so a fresh
   start needs a new memory generation key that collides with the other
   bindings sharing that scope. Disclosing the shared, retained scope is
   simpler and honest.

## Open questions

Each question has a recommended answer for the maintainer to confirm or change.

1. **An id list, a team, or either?** Recommended: either (section 2), so
   whichever is ready first (ADR 0246's published filter contract or configured
   identity provider teams) can enable follow. For one concept, narrow to teams
   only (alternative 3).
2. **Should channels shared with an external organization be followed?**
   Recommended: no, checked on every event, because outsiders are present and
   may send the invite. These stay declared only.
3. **Who answers an approval in a followed channel when no explicit approval
   policy is set?** Recommended: the caller list, so an owner approves their
   own bot's actions without setup, partly superseding ADR 0220's and ADR
   0246's role independence. The alternative refuses every answer until an
   approver team or filter is set, keeping that independence at a setup cost.
   Either way, explicit approval policy governs first, including an empty
   filter.
4. **Should direct messages follow too?** Recommended: yes, a main use for a
   personal bot. One row per direct message partner is acceptable, and
   operators can list and remove them.
5. **Who may turn follow on?** Recommended: operators only, through the API and
   `curie`, with no self-service switch in Slack.
6. **Should listed bots work in followed channels?** Recommended: not in this
   decision. That needs ADR 0246's authenticated principal kind, a qualified
   non-human invocation policy and the sharing flag on every bot event, and
   even then a bot's first contact should not create a followed binding.

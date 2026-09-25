# 175. A binding may name the callers it admits

Date: 2026-09-24

Status: Draft

This ADR builds on
[ADR-0096](0096-port-adapters-are-deployed-services.md) (the channel port),
[ADR-0118](0118-binding-cardinality-is-the-multi-surface-opt-in.md) (several
bindings per agent),
[ADR-0166](0166-tenant-boundary-and-principal-identity-land-together.md)
(tenant and principal identity) and
[ADR-0168](0168-one-installation-hosts-several-bot-identities.md) (several bot
identities in one installation). It supersedes nothing. It is an interim,
per-binding form of the inbound authorization that ADR-0166 places in #2914,
and it is written so that #2914 replaces its body without changing its callers.

## Context

Admission today is by channel. The worker resolves a turn's `(kind, address)`
pair to an agent (`apps/worker/src/curie_worker/binding.py`), so anyone who can
post in a bound Slack channel, message a bound identity directly, click a
button on one of its replies, or email a bound inbox triggers a turn. Nothing
asks who the sender is.

Two per-sender filters exist. Both are deploy-time, surface-specific, and not
per binding:
- The dispatcher's threaded-bot allowlist, `CURIE_SLACK_THREADED_BOT_ALLOWLIST`
  (`apps/dispatcher/src/curie_dispatcher/config.py`), holds exact
  `(channel_id, bot_id)` pairs. It is an exception to a loop guard: it lets a
  listed bot's threaded mention through `relevance.classify`, which otherwise
  refuses it as `bot_authored_thread_reply`. It never refuses a person.
- The mail adapter's `CURIE_MAIL_ALLOWED_SENDERS`
  (`apps/mail-adapter/src/curie_mail_adapter/config.py`) accepts full
  addresses, bare domains, or `*`, and `run.py` refuses to boot with ingress
  enabled and the list empty. It covers every binding that adapter serves,
  and changing it means redeploying the adapter.

An agent that acts for one person, such as a personal assistant carrying that
person's mail, calendar and chat credentials, must refuse everyone else before
a turn exists, on every surface it is bound to. A turn is the unit that reads
those credentials; refusing inside it is too late, and the model is not an
authorization boundary. Today that agent can be deployed safely only in a
channel nobody else can reach and behind a mail adapter whose deploy-time list
names one address. A shared channel, a direct message from a colleague, or a
second binding on the same agent reopens it.

The general answer is already decided. ADR-0166 introduces principals and
`identity_links`, and #2914 authorizes inbound events against them. #2914
depends on #2910 (identity links), #2911 (tenant-scoped core tables and bot
identity) and #2913 (identity events), all open, under the #2917 epic. That is
several reviewed steps away, and an agent that acts for one person cannot wait
for it.

Measured on `next` while drafting:
- `POST /channels/turns` loads the binding row, checks the channel token's
  claims against it, re-reads the generation, and then claims and enqueues
  (`apps/api/src/curie_api/routers/channels.py`). No step reads the sender.
- The Slack dispatcher has no database access. Its only API calls are the
  boot preflight (`preflight.py`) and approval resolution
  (`approval_actions.py`), both with `CURIE_API_KEY`.
- On the mention, direct-message and block-action paths, `process_event` and
  `process_action` in `handlers.py` run envelope validation and
  `relevance.classify`, then `claim_event`, then `_mint_turn`, which posts the
  placeholder and enqueues. Every refusal is an enumerated
  `relevance.DropReason` logged once by `relevance.drop`, which emits no
  metric today.
- `QueuedTurn` carries `author` and no bot id
  (`packages/aci-protocol/src/aci_protocol/turn.py`). The dispatcher mints
  `author` from `event.user`, so a bot-authored event's `bot_id` does not
  reach the worker.
- `agent_channels.generation` is bumped on every binding write, including one
  that changes nothing (`crud.update_channel_binding`), and a channel token is
  valid only at the generation it was minted against. Any write through
  `PATCH /agents/{agent_id}/channels` therefore revokes the adapter's token.
- The mail adapter treats every non-200 answer from `POST /channels/turns`
  except 401 and 429 as "try again later" (`adapter.py`, `post_turn` and
  `_deliver_turn`). A 403 today would leave the message pending and retried.

## Decision

**A binding may carry a list of the callers it admits. One function in the API
decides admission for every person-authored ingress, and a refused caller gets
no turn, no placeholder and no reply.**

### 1. `agent_channels.allowed_callers`

- A nullable list of provider-native sender ids on the binding row, matched
  exactly. No wildcards, no domains, no patterns.
- NULL admits every caller, as today. That is the default for every existing
  and every new binding, so the feature is opt-in per binding.
- An empty list is rejected on write with a 422 naming NULL as the way to
  admit everyone. An empty list reads like "no restriction" to one operator
  and "deny all" to the next; neither is written by accident.
- Entries are validated per kind, beside the address validator in
  `schemas._validate_channel_binding`:
  - `slack`: a user, enterprise user or bot id, starting with `U`, `W` or
    `B`.
  - `email`: one bare address (`local@domain`), no display name, stored
    lowercased because the mail adapter lowercases the sender it sends as
    `author`.
  - Any other kind: the generic address rule (non-empty, no whitespace).
- Duplicates are removed on write, and the list is bounded (100 entries), so
  one row cannot grow without limit.
- The read side (`ChannelBindingOut`) gains the field, additively.

### 2. One admission function in the API decides

A new module in `apps/api` holds one function that takes the loaded binding row
and the caller's ids and returns admit or refuse. Nothing else decides.

- **The channel port.** `POST /channels/turns` calls it with `TurnIn.author`
  after the credential and binding checks and before the delivery claim and
  the enqueue. A refused caller gets 403 with a fixed detail, and nothing is
  claimed or enqueued. The caller already authenticated for this binding, so
  the 403 discloses nothing the 401 matrix hides. The adapter is trusted to
  send a sender it has authenticated, as it already is for `author`.
- **The Slack dispatcher.** It has no database access, so it asks the API
  through a new `POST /channels/admission` (platform key only), which calls
  the same function. It asks after `relevance.classify` and before
  `claim_event`, so before the placeholder, on all three turn-minting paths:
  - `app_mention` and the direct-message lane of `message`: the caller ids are
    `event.user` and, for a bot-authored event, `event.bot_id`;
  - block actions that become turns: the caller id is `body.user.id`.

  An entry matching any of the ids admits.
- **The dispatcher caches answers per route.** A route whose binding has no
  list is cached as open for every caller, so an installation that never
  uses the feature makes one call per route per TTL, not one per message. A
  listed route caches the verdict per caller. Entries live for a configured
  TTL (default 30 seconds). While the API is unreachable, an expired entry is
  served stale, up to a configured ceiling (default 5 minutes). A cold miss
  while the API is unreachable refuses.
- The route key is the binding's route: `(kind, address)` today, and
  `(kind, adapter, address)` once ADR-0168 decision 3 lands.

Out of scope: approval buttons, which resolve through the API and never become
turns (ADR-0106 decides who may resolve them); signed hooks (ADR-0079), which
have no person as a caller; scheduled fires and work items.

### 3. A refusal is silent to the sender and visible to the operator

A refused caller receives nothing. No placeholder is posted, because the check
runs before `_mint_turn`, and no reply follows. A polite refusal would tell a
stranger that the agent exists and is reachable, which is what an agent that
acts for one person must not disclose.

Each refusal is recorded:
- In the dispatcher, as two new `relevance.DropReason` members, each with its
  rationale in `DROP_RATIONALES` and exercised by
  `apps/dispatcher/tests/test_inbound_relevance.py`, as that enum requires:
  - `caller_not_allowed`: the binding has a list and no caller id is on it.
  - `admission_unavailable`: the API could not be reached and nothing usable
    was cached. Reporting this as `caller_not_allowed` would send an
    operator looking for a typo in a list during an outage.
- In the API, as a log record naming the binding and the reason, never the
  message.
- As a counter, `curie.turn.refused`, registered in
  `packages/telemetry/src/curie_telemetry/metrics.py` with bounded
  attributes (`service.name` and `reason`), emitted by both services.

A durable audit row arrives with #2913's `identity_events`; this ADR does not
add a table for it.

### 4. Editing the list does not bump the generation

The list is written through its own endpoint, not
`PATCH /agents/{agent_id}/channels`, and that write leaves
`agent_channels.generation` unchanged. The generation fences the route and the
credential scoped to it (#2379). Who may use the route is a
different question, and bumping the generation on every edit would revoke the
adapter's channel token each time an operator added a person.

The comment on `generation` in `models.py` and the docstring of
`crud.update_channel_binding` say "every binding write". Both are reworded to
name this one write as not a rotation.

### 5. Existing filters are unchanged and run first

The threaded-bot allowlist still runs inside `relevance.classify`, before the
admission call, and still only lets a bot through the loop guard. A listed bot
on a binding with a caller list must also be on that list. The mail adapter's
sender allowlist and its provider-label authentication still run before
`POST /channels/turns`.

Once this lands, an adapter may keep authenticating senders and leave
authorization to the binding. For the mail adapter today that means
`CURIE_MAIL_ALLOWED_SENDERS=*` with a caller list on the binding, because
`run.py` still requires the variable to be set while ingress is enabled.
Relaxing that boot gate is a separate change.

The mail adapter does change in one place: it treats 403 from
`POST /channels/turns` as terminal and settles the message without a turn,
instead of retrying it. The channel port's interface document states that 403
is terminal for every adapter.

### 6. #2914 replaces the body, not the callers

When #2914 lands, `authorize_channel_event` becomes the body of the admission
function, and the entries in `allowed_callers` migrate to `identity_links`
rows granting those principals the binding. Whether the dispatcher then keeps
asking the API or reads the shared query layer that ADR-0166 decision 5 names
is #2914's decision; this ADR does not pre-empt it and does not amend
ADR-0166.

This ADR differs from #2914 as filed in one respect: #2914 answers a refusal
with a polite placeholder. This ADR posts nothing, for the reason in decision
3, and #2914 should adopt that or record why not.

### 7. Revocation lag and enforcement

- Removing a caller takes effect at `POST /channels/turns` on the next
  request, because that route reads the row each time.
- At the Slack dispatcher it takes effect within one TTL (30 seconds by
  default). While the API is unreachable it can take up to the stale ceiling
  (5 minutes by default). Adding a caller has the same lag, because refusals
  are cached too.
- The list is enforcing from the first release. ADR-0166 decision 8 ships
  #2914 log-only for one release because #2914 turns on for every binding at
  once and can silently drop traffic nobody chose to restrict. A caller list
  cannot: it applies only to a binding an operator gave one, and NULL, the
  default, admits as today. A log-only phase would leave the one agent that
  asked for protection unprotected for a release. A mistyped entry is visible
  at once as `caller_not_allowed` refusals and is undone by editing the list.
  No rule in this repository requires a log-only release for an opt-in
  control.

## Consequences

- An agent that acts for one person can be bound to a shared channel, reached
  by direct message, and given a mail binding, and everyone else is refused
  before a sandbox is claimed or a placeholder is posted.
- The Slack inbound path gains an API call on a cache miss. The API was
  already required at dispatcher boot; after boot, an API outage now refuses
  first contact on routes not in the cache, including routes with no list.
  That is the cost of failing closed, and `admission_unavailable` names it.
- A sibling identity's bot (ADR-0168 decision 6) is refused on a binding whose
  list does not name its bot id. That is intended; a personal agent should not
  take turns from other agents unless its operator lists them.
- `POST /channels/turns` gains a 403. Third-party adapters that retry every
  non-200 retry it until they read the updated interface document.
- The allowlist is stored as raw provider ids, the kind of untyped string
  ADR-0166 set out to replace. It is bounded to one column and one function,
  and #2914 migrates it.
- No frozen contract changes. `QueuedTurn` and the plugin format are
  untouched.

## Realizing work

Code paths, all on `next`:
1. A new Alembic migration in `apps/api/alembic/versions` adding the nullable
   column.
2. `apps/api/src/curie_api/models.py`: the column, and the reworded
   `generation` comment.
3. `apps/api/src/curie_api/schemas.py`: per-kind entry validation, the
   empty-list refusal, the bound, and the field on `ChannelBindingOut`.
4. A new admission module in `apps/api/src/curie_api`: the one function.
5. `apps/api/src/curie_api/routers/channels.py`: the 403 in `ingest_turn`
   before the claim, and `POST /channels/admission`.
6. `apps/api/src/curie_api/routers/agents.py` and
   `apps/api/src/curie_api/crud.py`: the list's own write endpoint, with no
   generation bump.
7. `packages/telemetry/src/curie_telemetry/metrics.py` and
   `packages/telemetry/schema/metrics.json`: the new HTTP operation and
   `curie.turn.refused`.
8. A new admission client with the route cache in
   `apps/dispatcher/src/curie_dispatcher`, and its TTL and stale ceiling in
   `apps/dispatcher/src/curie_dispatcher/config.py`.
9. `apps/dispatcher/src/curie_dispatcher/handlers.py`: the call on the
   mention, direct-message and block-action paths, after `classify` and
   before `claim_event`.
10. `apps/dispatcher/src/curie_dispatcher/relevance.py`: the two drop reasons
    and their rationales.
11. `apps/mail-adapter/src/curie_mail_adapter/adapter.py`: 403 is terminal.
12. `cli/src/main.rs` and `cli/src/commands.rs`: a `curie` subcommand that
    sets, shows and clears a binding's list, with the command manifests
    regenerated.
13. `docs/interfaces/channel-ingress/INTERFACE.md` and
    `docs/interfaces/port-adapter-service/INTERFACE.md`: the admission step
    and the terminal 403.

Tests, against real Postgres and Valkey, with only Slack and the mail provider
faked:
- Entry shapes per kind, the empty-list 422, NULL admitting everyone, and the
  bound.
- `POST /channels/turns` from an unlisted author returns 403 and leaves the
  stream length and the claim keys unchanged; a listed author enqueues.
- Editing the list leaves `generation` unchanged, and a channel token minted
  before the edit still enqueues after it.
- Each dispatcher path, fed a real event shape from an unlisted user, makes no
  `chat.postMessage` call, takes no claim, and logs `caller_not_allowed` once.
- Cache behavior: expiry after the TTL, stale service while the API is down,
  refusal as `admission_unavailable` on a cold miss while it is down.
- The threaded-bot allowlist still runs first.
- The mail adapter settles a 403 without a turn and does not retry it.
- End to end at the tier `AGENTS.md` requires: an unlisted person mentions the
  agent in a real Slack channel and gets no reply, and a listed person does.

## Acceptance conditions

- Explicit maintainer approval, published as `Status: Accepted`, per
  [`docs/adr/AGENTS.md`](AGENTS.md).
- The maintainer confirms the silent refusal (decision 3), knowing that #2914
  as filed posts a placeholder.
- The maintainer confirms enforcing from the first release (decision 7)
  rather than ADR-0166 decision 8's log-only release.
- The maintainer confirms the default TTL and stale ceiling, since together
  they are the revocation lag an operator is told.

## Alternatives considered

1. **A list per agent.** Rejected. ADR-0118 lets one agent hold a Slack
   binding and a mail binding, and their sender ids are different shapes
   that only the binding's kind can validate. A per-agent list also cannot
   restrict one binding and leave another open.
2. **A deploy-time dispatcher variable, like the threaded-bot allowlist.**
   Rejected. It covers Slack only, it is one list for every agent the
   dispatcher serves, and every change is a restart. It would also turn a loop
   guard exception into an authorization list.
3. **The check in the worker.** Rejected. By the time the worker sees a turn,
   the dispatcher has posted a placeholder, which already tells the sender the
   agent is there. The wire carries no bot id, so a bot entry could not be
   matched. Adding one is a change to `packages/aci-protocol`, a frozen
   contract that must land first as its own reviewed change.
4. **The dispatcher reads the binding table directly.** Deferred to #2914. It
   is the shape ADR-0166 decision 5 names for resolvers, but it gives the
   dispatcher database credentials, an engine and a connection budget it has
   never had. #2914 carries that change with the rest of the identity layer.
5. **Wait for #2914.** Rejected. It depends on #2910, #2911 and #2913, and
   #2911 alone tenant-scopes the core tables. An agent that acts for one
   person cannot be deployed safely until then, and this ADR is shaped so that
   #2914 replaces its body without new callers.

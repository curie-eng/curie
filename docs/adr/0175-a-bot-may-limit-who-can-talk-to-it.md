# 175. A bot may limit who can talk to it

Date: 2026-09-24

Status: Draft

Today anyone who can reach a bot, in a Slack channel, a direct message or its
email inbox, can use it. A personal assistant bot holds one person's mail,
calendar and chat access, so it must answer only that person. This ADR gives
each place a bot listens an optional list of who may talk to it, checked once,
before anything happens, and everyone else gets nothing back. No list means
today's behavior, each channel still proves who the sender is (email still
checks SPF, DKIM and DMARC), and since the platform now decides who may use the
bot, an adapter's own sender list becomes redundant.

This ADR builds on [ADR-0096](0096-port-adapters-are-deployed-services.md),
[ADR-0118](0118-binding-cardinality-is-the-multi-surface-opt-in.md),
[ADR-0166](0166-tenant-boundary-and-principal-identity-land-together.md) and
[ADR-0168](0168-one-installation-hosts-several-bot-identities.md), and
supersedes nothing. It is a stopgap for the full check ADR-0166 plans in #2914.

## Terms

- **Binding**: the link between a bot and one place it listens: a Slack channel,
  its direct messages, or an email inbox.
- **Caller**: whoever sent the message, identified by the id their channel gives
  them: a Slack user or bot id, or an email address.
- **Turn**: one run of the bot for one message. The turn is what uses the bot's
  credentials.
- **Placeholder**: the "working on it" reply posted in Slack before a turn runs.
- **Dispatcher**: the service that receives Slack events. **Adapter**: a service
  that brings another channel, such as email, in through the channel port.

## Context

Nothing checks who a caller is. Two sender filters exist, and neither fits: the
dispatcher's deploy-time list of bots allowed to mention it in a thread only
exempts them from the loop guard and never refuses a person, and the mail
adapter's deploy-time sender list covers every inbox it serves and takes a
redeploy to change.

A personal assistant must refuse everyone else on every binding before a turn
starts. Inside the turn is too late, since the turn reads the credentials, and
the model is not a security boundary. Today it is safe only where nobody else
can reach it.

The full answer, checking each event against the principals ADR-0166 introduces,
is #2914, which waits on #2910, #2911 and #2913, all open.

## Decision

**Each binding may carry a list of who may talk to the bot through it. One check
in the API decides for every channel. Anyone not on the list gets no turn, no
placeholder and no reply.**

### 1. The list

- No list (NULL) lets everyone in, as today, and is the default for every
  binding.
- An empty list is rejected (422, pointing to NULL), because one operator reads
  empty as "no limit" and the next as "nobody".
- Entries are exact ids, with no wildcards, domains or patterns, checked by the
  binding's kind: for Slack a user, enterprise user or bot id (`U`, `W` or `B`
  first); for email one bare address, stored lowercase as the mail adapter sends
  it; for any other kind, non-empty with no spaces.
- Duplicates are dropped; at most 100 entries. Reading a binding shows the list.

### 2. One check, before anything happens

One function in the API takes the binding and the caller's ids and answers yes
or no. Nothing else decides.

- **Adapters.** The channel port runs the check after verifying the adapter's
  token and before claiming or queueing anything, and answers a refused caller
  with 403. The adapter is trusted to send the sender it authenticated, as
  today, and has already proved it speaks for this binding, so the 403 reveals
  nothing new.
- **Slack.** The dispatcher has no database, so it asks the API through a new
  `POST /channels/admission` (platform key only), which runs the same function.
  It asks after its existing filters and before claiming the event, so before
  any placeholder, on mentions, direct messages and turn-starting button clicks.
  The ids are the sender (plus the bot id if a bot sent it) or the user who
  clicked. Any match lets the caller in.
- **Caching.** The dispatcher caches answers per route (the binding's kind and
  address, plus adapter once ADR-0168 decision 3 lands). A route with no list is
  cached as open to all, so unused installs make one call per route, not per
  message. Answers last 30 seconds by default. While the API is down, expired
  answers count for up to 5 minutes; with nothing cached, the caller is refused.

Not covered: approval buttons, which never start turns (ADR-0106); signed hooks
(ADR-0079), which have no person behind them; schedules and work items.

### 3. Refused callers get nothing; operators can see it

There is no placeholder and no reply, because a polite refusal tells a stranger
the bot exists.

The dispatcher logs one of two new drop reasons: `caller_not_allowed`, or
`admission_unavailable` when the API was down and nothing was cached, so nobody
hunts for a list typo during an outage. The API logs the binding and reason,
never the message. Both count `curie.turn.refused`, labeled only by service and
reason. An audit table waits for #2913.

### 4. Editing the list does not revoke tokens

Each binding has a generation counter that every write bumps, and an adapter's
token only works at the generation it was issued for (#2379). The list gets its
own endpoint that leaves the generation alone, since who may use a route is a
separate question from the route itself. Comments saying every write bumps the
generation are reworded.

### 5. Existing filters stay and run first

The dispatcher's list of bots allowed in threads still runs first; a bot on it
must also be on the binding's list, if there is one. The mail adapter still
checks sender authentication and its own list first. Since the binding now
decides who may use the bot, an operator can set `CURIE_MAIL_ALLOWED_SENDERS=*`
and keep the real list on the binding. Letting the adapter start with that
variable unset is a separate change.

The mail adapter retries every channel port error except 401 and 429. It will
treat 403 as final and settle the message without a turn, and the channel port's
interface document says 403 is final for every adapter.

### 6. #2914 later replaces the inside, not the callers

When #2914 lands, its check becomes this function's body and each entry becomes
an identity link. Whether the dispatcher then keeps asking the API or reads the
shared query layer (ADR-0166 decision 5) is #2914's call; this ADR does not
amend ADR-0166. #2914 as filed posts a polite refusal; it should adopt the
silence in decision 3 or record why not.

### 7. How fast changes apply, and enforcing from day one

A change applies at the channel port on the next message, and in Slack within 30
seconds (5 minutes while the API is down), for additions and removals alike.

The list enforces from its first release. ADR-0166 decision 8 ships #2914
log-only for a release because it turns on for every binding at once and could
silently drop traffic nobody meant to limit. This list applies only where an
operator added one, and a log-only release would leave the one bot that asked
for protection unprotected. A typo shows at once as `caller_not_allowed` and is
fixed by editing the list.

## Consequences

- A personal assistant can sit in shared channels and own an inbox, and everyone
  else is refused before a sandbox is claimed.
- While the API is down, first contact on an uncached Slack route is refused,
  even with no list. That is the cost of failing closed.
- Another bot in the same install (ADR-0168 decision 6) is refused unless
  listed. That is intended.
- Third-party adapters that retry every error will retry the new 403 until they
  follow the interface document.
- The list stores raw provider ids, the untyped strings ADR-0166 set out to
  replace, in one column until #2914 migrates them.
- No frozen contract changes: the queued turn and plugin format are untouched.

## Where this lands in the code

1. `apps/api`: a migration adding `agent_channels.allowed_callers`; `models.py`
   and its `generation` comment; entry checks in `schemas.py` beside
   `_validate_channel_binding`; a new admission module.
2. `apps/api/src/curie_api/routers/channels.py`: the 403 in `ingest_turn`, and
   `POST /channels/admission`.
3. `apps/api/src/curie_api/routers/agents.py` and
   `apps/api/src/curie_api/crud.py`: the list's own endpoint, apart from
   `PATCH /agents/{agent_id}/channels`.
4. `packages/telemetry/src/curie_telemetry/metrics.py` and
   `packages/telemetry/schema/metrics.json`: the counter.
5. `apps/dispatcher`: an admission client and cache; the call in `handlers.py`
   after `relevance.classify` and before `claim_event`; the two drop reasons in
   `relevance.py`.
6. `apps/mail-adapter/src/curie_mail_adapter/adapter.py`: 403 is final.
7. `cli/src/main.rs` and `cli/src/commands.rs`: a `curie` subcommand to set,
   show and clear a list.
8. `docs/interfaces/channel-ingress/INTERFACE.md` and
   `docs/interfaces/port-adapter-service/INTERFACE.md`: the check and the final
   403.

Tests use real Postgres and Valkey, fake only Slack and the mail provider, and
prove each rule above. End to end, at the tier `AGENTS.md` requires, an unlisted
person mentions the bot in a real Slack channel and gets no reply, and a listed
person does.

## Acceptance conditions

- Explicit maintainer approval, published as `Status: Accepted`, per
  [`docs/adr/AGENTS.md`](AGENTS.md).
- The maintainer confirms the silent refusal (decision 3), enforcing from the
  first release (decision 7), and the 30-second and 5-minute cache limits, which
  together are the delay operators are told to expect.

## Alternatives considered

1. **One list per bot.** Rejected. Slack and email ids differ in shape, only the
   binding's kind can check them, and a per-bot list cannot lock one binding and
   leave another open.
2. **A deploy-time dispatcher setting, like its thread bot list.** Rejected.
   Slack only, one list for every bot, a restart per change.
3. **Check in the worker.** Rejected. The placeholder is already posted by then,
   and the queued turn carries no bot id; adding one changes a frozen contract
   (`packages/aci-protocol`).
4. **Let the dispatcher read the binding table.** Deferred to #2914, which gives
   the dispatcher the database access it has never had.
5. **Wait for #2914.** Rejected. It needs #2910, #2911 and #2913, and a personal
   assistant cannot be deployed safely until then.

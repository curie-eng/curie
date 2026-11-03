# 263. HTTP identity accepts a one-to-one direct message to a registered receiver's bot

Date: 2026-11-02

Status: Draft

## Context

A registered Slack receiver submits `app_mention` events and approval answers.
Its message listener drops every direct message, so an agent that people
reach in a direct message with its bot cannot use HTTP identity at all, even
when the person, the bot and the conversation are exactly the ones the
operator registered.

A direct message is not a mention. Slack delivers it as event type `message`,
and that event type also carries group direct messages, channel messages,
edits, deletions and bot posts, distinguished by `channel_type` and
`subtype`. The team fields of a real direct message delivery have not been
captured in this repository, so whether its envelope carries
`is_ext_shared_channel`, and which of `user_team`, `team` and `source_team`
it carries, is not measured.

## Decision

### 1. Only a plain one-to-one direct message is a new source

The selected receiver accepts a `message` event only when `channel_type` is
exactly `im`, the event has no `subtype` and no `bot_id`, and it names a user.
Group direct messages, channel messages, edits, deletions, file shares and
bot posts stay refused. Every check a mention already passes still applies:
the envelope, the receiving identity and bot, a registered binding for the
native channel, the native event id and the thread timestamp.

### 2. The proof carries the conversation type

The receiver evidence gains an optional `channel_type`, set only by the
selected receiver for a `message` delivery and serialised only when present.
Every mention proof signs the same bytes as before.

### 3. The API attributes the sender from positive statements only

The API decides, not the dispatcher. A `message` delivery resolves only when
the user id is present, the channel type is exactly `im`, and the envelope
says `is_ext_shared_channel` is exactly `false`. A direct message across a
Slack Connect boundary is refused on every registration kind.

- **Ordinary workspace registration.** No enterprise signal may be present.
  At least one of `user_team` and `team` must be present, and every present
  team field, `source_team` included, must equal the installation account.
  The sender resolves in the installation's own namespace.
- **Grid workspace registration.** ADR 0252's enterprise checks come first,
  and the sender resolves in the organisation; team fields are neither the
  namespace nor a check, as for a non-shared Grid mention.

Trusted partner namespaces
([ADR 0261](0261-http-identity-resolves-slack-connect-senders-from-operator-trusted-partner-namespaces.md))
never widen a direct message. A missing user, a missing or different channel
type, or an absent or `true` shared flag is `subject_unidentified`; on an
ordinary registration, missing team evidence is `subject_unidentified` and a
foreign team is `namespace_conflict`.

### 4. A direct message is routed by its registered binding

The binding is found by the native channel id the operator registered, and
the API still requires the proof's native channel to equal the binding's
address and the channel token's binding. A direct message conversation bound
to one agent cannot be accepted for another.

### 5. Refusals are answered like a mention's

A refused or unavailable direct message gets the mention lane's fixed notice
as a reply in the conversation, under the same human-sender rules. Every
other `message` event the listener drops stays silent.

## Consequences

People can use an agent in a direct message with its bot under the same
identity rules as a mention.

The attribution rule is deliberately stricter than for mentions because the
payload shape is unmeasured: every statement it relies on must be present and
agree. A real capture that shows a field absent may refuse valid direct
messages until a later decision relaxes the rule with evidence.

## Alternatives considered

1. **Accept every `message` event in a registered channel.** Would admit
   edits, deletions and bot posts as turns and widen the source far beyond a
   person speaking to the bot.
2. **Accept group direct messages too.** A group conversation has several
   humans and no registered owner; it needs its own decision about which
   binding it belongs to.
3. **Let the dispatcher decide the sender.** The API already owns attribution
   for every other source; splitting it would put two attribution rules in
   two processes.
4. **Treat an absent shared flag as not shared.** Absence is not a statement.
   Requiring `false` keeps an unmeasured shape from being read as the
   permissive one.

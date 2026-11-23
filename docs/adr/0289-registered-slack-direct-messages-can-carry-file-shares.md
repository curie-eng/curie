# 289. Registered Slack direct messages can carry file shares

Date: 2026-11-23

Status: Accepted

Accepted by Junwon Jung (jw3329) on 2026-11-23 under ADR 0102 for
coordinated acceptance alongside implementation. The realizing paths
are the Slack message listener and the selected intake in
`curie_channel_gateway.slack.http_receiver.HttpReceiverRuntime`.

## Context

[ADR 0263](0263-http-identity-accepts-a-one-to-one-direct-message-to-a-registered-receivers-bot.md)
decision 1 excludes Slack messages with a subtype. A person sharing a
file in a one-to-one DM sends a `message` with subtype `file_share`.
That delivery never reaches the existing attachment-reference path.

## Decision

Amend decision 1 of ADR 0263 to also admit a human `file_share` message
with at least one usable Slack attachment reference in a registered
one-to-one DM. Preserve the receiver, channel, binding, sender, Slack
Connect, signing and idempotency checks. Continue to refuse bot
messages and other subtypes. Use the existing attachment references
in the signed invocation body and the existing fetch path.

## Consequences

A human can supply a file directly to the registered bot. The API
continues to attribute a `message` delivery by the same identity evidence,
and the wire contract stays the same.

## Alternatives considered

- Admitting all subtypes would treat edits and other Slack lifecycle
  notifications as new invocations.
- A separate file event subscription would separate the file from
  the human's message and require another source to authorize.

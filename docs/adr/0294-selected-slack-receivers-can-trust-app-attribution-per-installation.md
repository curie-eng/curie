# 294. Selected Slack receivers can trust app attribution per installation

Date: 2026-11-25

Status: Accepted

Accepted by Brian Conn under [ADR 0102](0102-accepted-alongside-implementation-with-explicit-approval.md)
alongside implementation in `curie_channel_gateway.slack.http_receiver.HttpReceiverRuntime`.

## Context

Slack may deliver a mention with both an attributed user ID and the posting
app's bot ID. Slack authenticates delivery. Its
[Events API](https://docs.slack.dev/apis/events-api/) and
[bot message documentation](https://docs.slack.dev/reference/events/message/bot_message/)
do not establish which human action caused that mixed-field post.

## Decision

A selected HTTP receiver may accept an app-authored `app_mention` when
the operator lists the sending Slack app ID for that receiver's
`provider_installation_id` in `dispatcher.appMentionAllowlist`.
Standalone dispatchers read the same list from
`CURIE_SLACK_APP_MENTION_ALLOWLIST`. The allowlist is empty by default. The
receiver requires a native bot profile identifying the sending bot and
its bot user. An app ID may come from the event or its bot profile, and
the IDs must agree when both are present. The receiver rejects its own
app, its own bot, and a sender equal to the posting bot user. It submits
the event's human user under the existing caller, binding, and replay checks.

## Consequences

The operator grants a listed app authority to attribute these mentions to
users for that installation. Choose apps that can vouch for the users they
name. This applies to the selected mention lane. Ordinary mentions, DMs,
plain thread replies, legacy routing, and the isolated test drivers in
[ADR 0202](0202-a-test-installation-admits-a-listed-bots-actions-and-approval-replies.md)
keep their existing rules.

## Alternative considered

Trust every Slack-delivered mention that carries a user ID. That would
implicitly delegate user attribution to every app that can post a mention.

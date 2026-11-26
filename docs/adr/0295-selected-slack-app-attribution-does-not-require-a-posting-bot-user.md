# 295. Selected Slack app attribution does not require a posting bot user

Date: 2026-11-25

Status: Accepted

Accepted by Brian Conn under [ADR 0102](0102-accepted-alongside-implementation-with-explicit-approval.md)
alongside implementation, with explicit maintainer approval recorded, in
`curie_channel_gateway.slack.http_receiver.HttpReceiverRuntime._allows_app_mention`.

Supersedes in part [ADR 0294](0294-selected-slack-receivers-can-trust-app-attribution-per-installation.md):
exactly one requirement of its Decision, that the native bot profile identify
the posting bot's user, and the dependent check against a sender equal to that
posting bot user when no such ID is supplied. Everything else in 0294 stands.

## Context

A live test of an approved post through a listed Slack app produced an
`app_mention` that reached a selected receiver over authenticated Socket Mode.
It carried the person's user ID, matching event and bot-profile app IDs, and
matching event and bot-profile bot IDs, but no bot-profile user ID. ADR 0294
required that ID, so the receiver refused a mention its operator had
allowlisted.

## Decision

Slack must still independently name the posting Slack app, through the event
`app_id` or `bot_profile.app_id`, equal when both are present, and that app must
be on `dispatcher.appMentionAllowlist` for the receiving
`provider_installation_id`. The receiver still requires the posting bot ID in
the event and a bot profile whose id matches it. It still rejects its own app,
its own bot, and a sender equal to its own bot user, as well as conflicting or
malformed supplied app attribution.

The receiver no longer requires a posting bot user ID. When the bot profile
supplies one, it must be well formed and differ from the attributed sender, and
it must not be the receiver's bot user. The attributed human is still submitted
under the existing caller, binding, and replay checks, and the API's refusal
still stands. Subtypes, the test drivers in
[ADR 0202](0202-a-test-installation-admits-a-listed-bots-actions-and-approval-replies.md),
and ordinary human mentions keep their rules. A mention without app metadata
never uses the allowlist.

## Consequences

The trust the operator grants a listed app is unchanged. The bot user ID was an
extra consistency check that the provider does not always supply, and its
absence does not weaken self-post rejection, because the receiver compares
against its own bot user and bot ID. Live acceptance against a patched receiver
is still outstanding at the time of this decision.

## Alternative considered

Keep requiring the bot user ID, or look it up with an extra Slack API call
(`bots.info`) at delivery. The first refuses the provider's real shape. The
second adds a network call and a new failure mode to every selected app mention
for a value that adds no authority the allowlisted app ID and bot ID do not
already give.

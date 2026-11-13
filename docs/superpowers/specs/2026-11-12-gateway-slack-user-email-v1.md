# Gateway Slack user email lookup v1

An external HTTP identity provider maps a Slack sender to a directory account.
It receives the sender's Slack user ID and workspace in the
[HTTP identity lookup request](../../apps/api/src/curie_api/schemas/http_identity.py)
and typically needs that user's email address to find the directory account.
Until now the provider had to call Slack `users.info` with its own bot token.
An installation that keeps every Slack token in the channel gateway
([ADR 0275](../../adr/0275-channel-gateway-provider-boundary.md)) could not run
such a provider without handing it a token.

This spec adds one bounded gateway read that answers "which email belongs to
this Slack user" to a separately authenticated identity caller. It follows the
existing group-membership read: the gateway holds the token, the caller holds a
gateway credential only, and every failure refuses. It stays inside ADR 0275's
boundary (the gateway owns provider reads and credentials) and is off until
configured. It changes no API admission, authorization or lookup contract.

## 1. A separate identity caller

<!-- @spec GATEWAY-USER-EMAIL-1 -->
The gateway accepts an optional third caller credential,
`CURIE_CHANNEL_GATEWAY_IDENTITY_TOKEN`. It uses the existing caller scheme:
the `X-Curie-Adapter-Secret` header plus the signed, single-use request proof
of `request_replay.request_headers`. The three caller credentials must be
distinct and non-blank. The identity caller may call only `user_email`; the
`api` and `worker` callers may not call it. Without the identity credential no
caller can reach `user_email`.

## 2. The read

<!-- @spec GATEWAY-USER-EMAIL-2 -->
`POST /v1/gateway/{identity}/user_email` with body
`{"user_id": "U...", "workspace": "T... or E..."}` reads Slack `users.info`
with the bot token of the named gateway Slack identity. It answers 200
`{"user_id": ..., "email": ...}` only when Slack answers `ok`, the answered
user ID equals the requested one, the user belongs to the requested workspace
(its `team_id`, its `enterprise_user.enterprise_id`, or one of its
`enterprise_user.teams`), the user is not deleted, not a bot and not an app
user, and Slack returned a non-empty email. The answer is read fresh on every
call; the gateway caches no email.

## 3. Refusals

<!-- @spec GATEWAY-USER-EMAIL-3 -->
A user Slack does not know or show, a workspace mismatch, a deleted, bot or
app user, and a missing email all answer 404 `{"code": "user_email.unknown"}`.
An unknown gateway identity, a Slack error, a non-JSON body and a network
failure answer 502. A Slack call that does not finish within the gateway's
bound (`CURIE_CHANNEL_GATEWAY_USER_EMAIL_TIMEOUT_S`, default 5 seconds)
answers 504. No refusal echoes the request, the provider body or an email.

## 4. Identity-provider client

<!-- @spec GATEWAY-USER-EMAIL-4 -->
`curie_channel_gateway.user_email_client.GatewayUserEmails` is the supported
client for an identity provider. `GatewayUserEmailSettings` reads
`CURIE_CHANNEL_GATEWAY_URL`, `CURIE_CHANNEL_GATEWAY_IDENTITY_TOKEN`,
`CURIE_CHANNEL_GATEWAY_SLACK_IDENTITY` (default `default`),
`CURIE_CHANNEL_GATEWAY_USER_EMAIL_TIMEOUT_S` (default 5 seconds),
`CURIE_CHANNEL_GATEWAY_CA_FILE` and `CURIE_CHANNEL_GATEWAY_ALLOW_INSECURE`.
Construction refuses any Slack token in its settings or environment, through
the existing `token_custody.refuse_slack_tokens` check. `email_of(user_id,
workspace)` returns the email or raises `UserEmailLookupError`;
`UserEmailUnknown` is the subclass for the 404 answer. A gateway error, an
authentication refusal, a malformed or mismatched answer and the client's own
timeout all raise `UserEmailLookupError`. The client never returns an empty or
default email.

## 5. No email in logs

<!-- @spec GATEWAY-USER-EMAIL-5 -->
Neither the gateway nor the client logs an email address, at any level, on
success or failure. Errors carry fixed text.

## 6. Chart

<!-- @spec GATEWAY-USER-EMAIL-6 -->
`channelGateway.identityLookup.existingSecret` and
`channelGateway.identityLookup.tokenKey` (default `token`) name an
operator-supplied Secret whose key holds the identity caller credential. When
set, the gateway receives it as `CURIE_CHANNEL_GATEWAY_IDENTITY_TOKEN`. When
unset, nothing changes. The operator gives the same value to the identity
provider.

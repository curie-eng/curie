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
A missing email when Slack reports that the token lacks `users:read.email` is a
configuration outage and answers 502 instead. An unknown gateway identity, a Slack error, a non-JSON body and a network
failure answer 502. A Slack call that does not finish within the gateway's
bound (`CURIE_CHANNEL_GATEWAY_USER_EMAIL_TIMEOUT_S`, default 5 seconds)
answers 504. No refusal echoes the request, the provider body or an email.

## 4. Identity-provider client

<!-- @spec GATEWAY-USER-EMAIL-4 -->
`curie_channel_gateway.user_email_client.GatewayUserEmails` is the supported
client for an identity provider. `GatewayUserEmailSettings` reads
`CURIE_CHANNEL_GATEWAY_URL`, `CURIE_CHANNEL_GATEWAY_IDENTITY_TOKEN`,
`CURIE_CHANNEL_GATEWAY_SLACK_IDENTITY` (default `default`),
`CURIE_CHANNEL_GATEWAY_USER_EMAIL_TIMEOUT_S` (default 8 seconds, longer than
the gateway's bound), `CURIE_CHANNEL_GATEWAY_CA_FILE` and `CURIE_CHANNEL_GATEWAY_ALLOW_INSECURE`.
Construction refuses any Slack token in its settings or environment, through
the existing `token_custody.refuse_slack_tokens` check. `email_of(user_id,
workspace)` returns the email or raises `UserEmailLookupError`;
`UserEmailUnknown` is the subclass for the 404 answer carrying the
`user_email.unknown` code; any other 404 is an outage. A gateway error, an
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

## 7. Lookup identities

<!-- @spec GATEWAY-USER-EMAIL-7 -->
An operator may give directory reads their own provider credential instead of
adding a directory scope to an agent's app. A declared Slack identity
(`CURIE_SLACK_IDENTITIES`) needs an app token and Socket Mode, which a
directory-lookup app has no use for, so the gateway accepts a separate
declaration, `CURIE_CHANNEL_GATEWAY_LOOKUP_IDENTITIES`: a JSON list of
`{"name", "kind", "token_env"}` entries, defined in
`curie_channel_gateway.lookup_identities`. `name` follows the Slack identity
name pattern of `aci_protocol.slack_identities`. `kind` names the provider;
`slack` is the only kind, a bot token with `users:read` and
`users:read.email`. `token_env` names the variable holding the credential and
must be `CURIE_CHANNEL_GATEWAY_LOOKUP_TOKEN__<index>`. An entry carries no other
field. Adding a kind adds one adapter and one registry entry in the gateway
and leaves the declaration's shape unchanged.

<!-- @spec GATEWAY-USER-EMAIL-8 -->
A lookup identity's credential reaches directory reads only, which today is
the `user_email` read of sections 2 and 3. Replies, group membership, file
fetches, channel and canvas reads, sender identities, account profiles,
preflight and the dispatcher never receive it, and every other operation
naming a lookup identity answers as for an unknown identity. A `user_email`
request resolves its identity across the declared Slack identities and the
lookup identities; for a lookup identity it reads with that identity's kind
adapter and credential, under the same answers, refusals and logging rules as
sections 2, 3 and 5.

<!-- @spec GATEWAY-USER-EMAIL-9 -->
The gateway refuses to start when the declaration is not a JSON list of such
entries, when two entries share a name or a `token_env`, when a name is
`default`, `curie-cluster-message` or a declared Slack identity's name, when a
kind is not supported (the refusal names the supported kinds), or when a
declared `token_env` is unset or blank. No refusal echoes a credential. When
the declaration is absent or empty, every identity and read behaves as
before.

<!-- @spec GATEWAY-USER-EMAIL-10 -->
`token_custody.refuse_slack_tokens` refuses any variable named with the
`CURIE_CHANNEL_GATEWAY_LOOKUP_TOKEN__` prefix in every process that consumes
gateway ports, and the worker drops that prefix from every agent child
environment.

## 8. Chart: lookup identities

<!-- @spec GATEWAY-USER-EMAIL-11 -->
`channelGateway.lookupIdentities` (default `[]`) lists
`{name, kind, existingSecret, key}` entries, `key` defaulting to `token`;
`values.schema.json` limits `kind` to the supported kinds. For a non-empty
list the chart renders `CURIE_CHANNEL_GATEWAY_LOOKUP_IDENTITIES` and one
`CURIE_CHANNEL_GATEWAY_LOOKUP_TOKEN__<index>` per entry, read from that entry's
operator-supplied Secret, into the gateway workload only. A repeated or
reserved name fails the render. An empty list renders the manifests unchanged
and no other workload receives either variable.

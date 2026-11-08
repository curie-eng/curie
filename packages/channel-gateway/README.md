# Channel gateway

`curie_channel_gateway` owns provider connections, intake, rendering, clicks,
egress and provider reads. This extraction is behavior-neutral. Slack intake and connections live in `slack/`;
mail intake, provider I/O and durable delivery state live in `mail/`. The
dispatcher retains its Valkey construction, admission cache and queue transport,
and the standalone mail executable delegates to this package.

Slack intentionally imports dispatcher helpers while the dispatcher depends on
this package: Redis construction, queue publication, admission caching and the
existing approval client retain their owners until those extractions land.
This reverse dependency is temporary. Mail must remain independently installable;
the import-linter contract forbids `curie_channel_gateway.mail` from importing
`curie_channel_gateway.slack` or `curie_dispatcher`, including indirectly.

The boundary proposal is recorded in
[Draft ADR 0275](../../docs/adr/0275-channel-gateway-provider-boundary.md).

## Ports

Import ports from `curie_channel_gateway.ports`; their defining modules are:

| Module | Port or value |
| --- | --- |
| `ports/reply.py` | `ReplyEventTransport[Route]`, `AuthenticatedReplyTransport[Route]`, `ReplyCapabilities`, `DeclaredReplyTransport[Route]` |
| `ports/admission.py` | `AdmissionClient`, `AdmissionRequest`, `AdmissionAnswer` |
| `ports/intake.py` | `TurnSubmission[Turn, Receipt]`, `ApprovalResolution[Outcome]` |
| `ports/groups.py` | `GroupMembershipSource[Membership]` |
| `ports/reads.py` | `IdentitySelection[Route]`, `FileSource`, `SenderIdentities`, `AddressIdentityLookup`, `ChannelReader[Route, Window, Page, Message]`, `CanvasReader[Route, File, Page, Document]` |

Reply events and acknowledgements remain the existing `channel_protocol.reply`
types: `turn.status`, `reply.update`, `reply.post`, `turn.completed` and `{ref}`.
Wire 1.1 carries progress and delivery identities; wire 1.2 carries literal text
updates. Status and completion retain their 1.0 wire. Authentication, endpoint
selection and credential selectors stay outside that wire.

Generic parameters bind directly to today's worker `TargetRoute`, API
`UserGroupMembership`, `BindingRoute`, `ResolvedWindow`, `ProviderPage`,
`ChannelMessage`, `CanvasFile`, `CanvasPage` and `CanvasDocument`. They preserve
the current typed signatures without importing applications or moving records.
Existing HTTP emitters satisfy `ReplyEventTransport`; read implementations satisfy
their ports structurally. `AuthenticatedReplyTransport` also requires synchronous
`edits_in_place(kind, route)` and `undeliverable_reason(kind, route)` hooks.
`DeclaredReplyTransport` retains explicit `ReplyCapabilities` for each kind and
route beside an HTTP emitter. Missing declarations raise rather than silently
returning False or None. Composition supplies the provider configuration snapshot
and replaces it when identities change, before admitting turns against it.
Lookup errors retain their existing identity and semantics. The admission port
is a wrapper contract: map `evidence` to the API's existing provider evidence
field and preserve `None` as unavailable. The lane adapter fixes `kind` as a class
constant, never from the inbound request. `connect_url` is single-use and must
never be cached or shared. The ports add no platform endpoint.

## Authenticated HTTP replies

`HttpReplyTransport` and `reply_application` compose the network boundary. The
receiver requires a fixed provider `kind`, a server-owned `route`, and a durable
`state_path`. `ReplyDestination` supplies gateway-owned route metadata without
importing an application. Event JSON and reply wire versions remain unchanged.

1. Transport endpoints target the configured `/reply` ingress and require HTTPS. `allow_insecure_http=True` is an explicit
   development or test opt-out; other schemes remain refused. Use a dedicated reply secret, separate from the chat-attester key. Each request
   signs its path, exact body digest, request key, best-effort flag, timestamp and
   nonce. The receiver checks a 120-second freshness window and persists consumed
   nonces. Replaying a signed attempt is refused, including after restart.
2. The gateway fixes the provider route. Slack per-turn endpoint overrides,
   including the CLI stub origin and its transport fallback, are refused over
   HTTP. Existing direct Slack delivery retains those paths. Provider origins
   configured when constructing the Slack adapter remain supported.
3. The gateway bounds provider execution to 120 seconds. The client defaults to
   180 seconds and refuses timeouts below 150 seconds. A request key identifies
   exact event bytes and the best-effort flag at one destination; identical
   attempts share an in-flight delivery and its persisted result. Distinct posts
   with identical content must use distinct `delivery_id` values on wire 1.1.
   A lost outcome, expired execution budget or unconfirmed claim after restart
   fails with `IndeterminateReplyDeliveryError`; it never re-posts automatically.
   An operator must reconcile that outcome before authorizing a new delivery.
4. `best_effort_unreachable` travels in a signed request header and is passed to
   the provider. An unreachable acknowledgement retains its marker across the
   hop, so worker completion authority and delivery telemetry keep their meaning.
   Responding refusals remain failures even when best effort was requested.
5. `reply_errors.py` defines the closed status and fixed-detail vocabulary.
   Permanent errors retain their exception class across the hop, without
   forwarding provider diagnostics or credentials. The receiver defaults to an
   explicit 64 MiB body limit, configurable by composition; larger events fail
   permanently with `ReplyPayloadTooLargeError`. Answers above 1 MiB are supported.

The SQLite ledger retains delivery claims and results, including failures, for
as long as their request keys can be retried. It stores request hashes, authentication nonces, opaque result references and
fixed diagnostics, not message bodies or bearer credentials.
Only expired authentication nonces are pruned automatically. `HttpReplyAdapter`
is the existing generic adapter HTTP client; it does not implement this signed
gateway ingress protocol. Compose worker-to-gateway delivery with
`HttpReplyTransport` and its explicit capability snapshot.

## ADR 0202 implementation: declared HTTP test drivers

The selected Slack HTTP receiver uses the existing
[ADR 0202 declaration](../../docs/adr/0202-a-test-installation-admits-a-listed-bots-actions-and-approval-replies.md).
`testInstallation.enabled` remains false by default. The dispatcher passes
`CURIE_TEST_INSTALLATION_ENABLED`, `CURIE_TEST_INSTALLATION_DRIVERS` and
`CURIE_TEST_INSTALLATION_THREAD_TURN_LIMIT` through its existing configuration;
there is no second gateway allowlist or receiver-file setting.

Only an `app_mention` whose stripped text begins with `[test action]` can use
this admission. Its `bot_id` and channel must match a declared driver. The
receiver signs the configured `bot_user_id` as both the turn author and the
sender evidence, ignoring the event's user for attribution. The API performs
its normal identity lookup and caller-group check for that configured user.
The receiver's own bot, user and app identities from `auth.test` remain refused,
even if listed. Bot direct messages and unmarked bot mentions remain refused.

Before a driver reply, dedupe claim or turn-budget reservation, the receiver
sends the signed turn to `POST /channels/turns/admission`. That operation runs
the same authenticated identity admission as enqueue and returns 204 without
claiming or enqueueing a delivery. Refused caller groups return 403 and an
unavailable directory fails closed. A normal driver action is checked again by
`POST /channels/turns` when it is submitted.

A root `[test action] ping` gets the fixed admission reply in its own thread
without starting a turn. A threaded ping is refused. Driver turns reuse the
legacy Valkey dedupe and ten-minute per-thread budget, including the configured
`testInstallation.threadTurnLimit`; pings and duplicate deliveries spend no
turn budget. With no declaration, the existing human intake and bot filters
perform no additional API or Valkey operations.

Approval replies are a remaining gap: `[test action] approve ...` and
`[test action] reject ...` remain refused by selected HTTP receivers. They
neither resolve approvals nor start turns. Existing human approval interactions
keep their authenticated HTTP path.

## Authority and storage

The gateway authenticates Slack deliveries by verifying the Slack signature or
using Socket Mode. It owns minting the chat-attester principal and signing the
receiver answer proof over the exact resolve bytes and principal. The API checks
our proof signatures and evidence shape and derives the principal from that
evidence; it cannot confirm that a delivery actually came from Slack. Whoever
holds the platform key and attester secret can assert any user.

The API retains admission authority, authorization, verified-principal policy,
approval decisions and resolve-once. Mandatory uncached group resolution stays
an API requirement through `fresh_members`; a provider read grants no authority.
The worker retains resume enqueue, stream consumption, `_ThrottledReply`,
completion/outbox and its neutral `ReplySink`. API admission state, Valkey
dedupe/cache, the worker stream and mail's local SQLite delivery state keep
their current owners. The gateway acquires no Postgres storage or worker coordination
authority. The optional HTTP reply receiver owns a separate SQLite replay and
delivery ledger; composition must preserve that file across restarts.

Stock Slack still calls `/channels/admission`, claims dedupe, posts a
placeholder, then enqueues. A refusal claims nothing and posts nothing. Mail
retains authenticated `/channels/turns` ingress with its scoped credential and
its standalone entrypoint. `TurnSubmission` wraps that HTTP ingress and
`ApprovalResolution` wraps the existing approval endpoint, preserving receipts
and verdicts. AgentMail still cannot authenticate inbound senders;
the positive intake fixture uses the existing verifying test provider.

## Move sequence

Every move deletes the duplicated protocol it replaces and migrates all callers.
U2 replaces dispatcher `AdmissionAnswer`; U4 replaces API `usergroups.py`
`GroupMembershipSource` and `readers.py` `ChannelReader`/`CanvasReader`. Never keep
both definitions or add a compatibility alias. Neutral value records needed by
both gateway and applications belong in `channel-protocol`, with all callers
migrated in the same move.

1. U2 moves Slack intake and connection code and the mail adapter behind these
   ports, preserving entrypoints, admission order and all existing storage owners.
2. U3 moves Slack egress, cards and clicks and neutral authenticated HTTP delivery.
   Before moving renderers, put the general `Reply` value shape from worker
   `blocks.py` in `channel-protocol` and migrate every caller. The worker must
   never import gateway rendering to obtain that shared value.
3. U4 moves credential-bearing provider reads and pure provider evidence
   validators. The API still executes the validators and authorizes every read
   and approval, including uncached `fresh_members`; then remove provider tokens
   from API and worker. Bind the read ports to the existing API record types.

## Regression proof

From the repository root, run:

```bash
env -u CURIE_SLACK_TRUSTED_ORIGINS CI_REQUIRE_VALKEY_TESTS=1 uv run pytest -q -n 0 @packages/channel-gateway/behavior.pytest
```

Set `TEST_VALKEY_HOST`, `TEST_VALKEY_PORT` and `TEST_VALKEY_PW` for the isolated
real Valkey instance. Clear ambient development origin trust so it cannot change
the foreign-endpoint negative control. No infrastructure skip counts as proof.
The argument file selects current executable provider paths, the shared reply
corpus, approval action and principal vectors, mail provider conformance, and
positive and negative admission/reply paths. The additional Slack fixture checks ordering
at the real dedupe write, placeholder call and stream append. HTTP fixtures
send the shared corpus through the current worker transport to a real HTTP
receiver. Keep these selectors and assertions working after each move; update
only import wiring when a provider's module location changes.

Run serially against private Valkey; shared Valkey plus parallel caller-admission
tests can produce false failures. The selected-click acknowledgement and
exactly-once submission cases are included in the argument file.

## Upgrading existing installations

The Helm chart automatically deploys the gateway when Slack is configured and
no gateway URL is supplied. Keep the same values file and Secret objects. The
chart moves bot-token references from the API and worker to the gateway. Slack
intake remains in the dispatcher. Separate gateway caller credentials and
internal TLS are provisioned automatically. Other optional capabilities remain
subject to their existing configuration switches. See the chart's
[upgrade and certificate rotation guide](../../charts/curie/README.md#channel-gateway-upgrade).

Custom API and worker launchers must remove raw Slack tokens from their process
environments and configure `CURIE_CHANNEL_GATEWAY_URL` and their separate
`CURIE_CHANNEL_GATEWAY_TOKEN`. The runtime refuses provider credentials rather
than supporting both configurations. For a private CA, mount its PEM file and
set `CURIE_CHANNEL_GATEWAY_CA_FILE`; the additional trust is scoped to gateway
traffic. Never disable certificate verification.

### Protected-worker process files

Stop the protected worker and its provisioner before migrating a writable copy
of `process.json`. Provision the gateway with the former Slack tokens and the
same identity names, then write a private `gateway.json` containing the v2
`channel_gateway` object:

```json
{
  "url": "https://gateway.example.com",
  "token": "REPLACE_WITH_WORKER_CALLER_CREDENTIAL",
  "allow_insecure_http": false,
  "identities": ["default"]
}
```

With the upgraded CLI, run the one-shot migration with the runtime directory
and private gateway configuration paths:

```bash
curie migrate-protected-process --runtime-dir /private/acme/runtime --gateway-config /private/acme/gateway.json
```

The command accepts only v1, validates the v2 process contract, writes it atomically with mode 0600, and removes the `slack` object and
its raw tokens. Database, API, adapter and launch TLS configuration are retained.
A refusal leaves the source file unchanged and prints no input values. Restart
the provisioner with the migrated file and recreate any read-only projected
copy before starting the worker. Schema v1 remains rejected at runtime and its
error names this migration. Helm-managed ordinary workers have no `process.json`
and require no manual file migration.

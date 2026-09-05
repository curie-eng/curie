# apps/dispatcher

The Slack dispatcher: Slack Bolt for Python in Socket Mode.
On an `app_mention` (channel) or direct `message` (DM) for the bot it acks the
Socket Mode envelope fast, posts an in-thread placeholder reply, and enqueues a
normalized job onto a Valkey Stream keyed by the Slack event id (idempotent).
All of this runs under reconnect supervision with graceful shutdown.

It does exactly that and no more: routing, the finish-race, steer/interrupt, and
run orchestration are the worker's job, not the dispatcher's.

Approval actions are the dispatcher's one authenticated-principal responsibility
([ADR-0106](../../docs/adr/0106-an-approver-is-an-authenticated-principal.md)). Slack
authenticates the click over Socket Mode; the dispatcher signs a short-lived `chat`
principal containing that Slack user, channel, and approval ID, then sends it in
`X-Curie-Approval-Principal`. The resolve body carries only `decision` and optional
`note`. It never forwards caller-controlled `resolved_by` or `actor_channel`, and the
platform API key alone cannot resolve. Authorization still happens in the API: requester
equality neither grants nor denies, so the same authenticated requester may confirm only
when the selected approver set admits them.

Slack may distribute one app's interactive payloads across any of its open
Socket Mode connections. If two Curie releases share that app, the release whose
API does not contain the approval leaves the Socket Mode envelope unacked, so
Slack retries another connection; only the release whose API owns the row
acknowledges and can pass the existing compare-and-set. Separate Slack apps per
long-lived release still avoid sharing a connection pool.

## What is ingested, and what is refused

Ingest admits more than it used to (#2006). A message whose body lives in Block Kit
`blocks` or legacy `attachments` — an alert app's post, typically — is normalized by
`inbound_text.derive_text` instead of being read off an empty top-level `text`, so it
reaches the model with its content instead of minting an empty turn. What that walker
leaves out is a closed denylist of keys, not a judgement about which strings are prose: it
refuses to descend into an `accessory`, `confirm`, `options`, `option_groups`, `placeholder`
or `hint` subtree, and into an `actions` block's `elements`, so button labels,
confirmation-dialog copy, select placeholders and option lists stay out of the turn. It is
not a complete exclusion of interface chrome, and should not be read as one — an input
block's `label` is not on that denylist, so its text does reach the derived turn. Message
subtypes are
now an open world: only the closed `relevance.NON_CONTENT_SUBTYPES` denylist (edits,
deletes, tombstones, EKM-redacted bodies, assistant thread-start markers) is refused, so
`file_share`, `thread_broadcast` and any subtype Slack ships in future are ingested. And a
bot-authored `app_mention` posted at the root of a channel is ingested, so an alert bot can
@-mention the agent; the DM lane does not inspect bot authorship at all (Bolt's own
`IgnoringSelfEvents` middleware drops the bot's own posts before any listener runs).

Still refused: a bot-authored `app_mention` that carries a `thread_ts`, as a loop guard
between two Curie installations in one workspace, unless its exact sender/channel pair
is explicitly trusted as described below; a `message` event outside the DM lane, which the app
never subscribed to; a redelivery whose dedupe key is already claimed; and a button click
with nowhere to reply — an App Home or modal click, which carries no channel and no
message.

Every refusal these handlers make on the message lanes is logged at INFO with its
enumerated reason and rationale (the full list is `relevance.DROP_RATIONALES`), so an
operator chasing a message that produced no turn can grep the dispatcher log for
`dropped inbound slack delivery` and read why. The click lane logs the same way for the
refusal it can make; two further reasons in that list, `no_action_in_payload` and
`empty_action_command`, are defensive guards Bolt's catch-all action matcher does not
currently deliver, so they are not dispositions to expect in production logs — they are
kept on purpose, and `docs/interfaces/channel-ingress/INTERFACE.md` says why. If the grep
finds nothing, the refusal was Bolt's, above these handlers — that same document is the
system of record for this contract.

### Trusted bot continuations

`CURIE_SLACK_THREADED_BOT_ALLOWLIST` is a JSON array of exact channel/bot pairs:

```bash
export CURIE_SLACK_THREADED_BOT_ALLOWLIST='[{"channel_id":"C0EXAMPLE1","bot_id":"B0EXAMPLE1"}]'
```

It defaults to `[]`. Malformed JSON, missing or extra keys, blank identifiers,
non-channel or non-bot identifiers, and wildcards prevent dispatcher startup.
A pair admits only that bot's threaded channel mentions; pairs never form a
cross product. Use the event's `bot_id`, not the bot's user ID or display name.
The authenticated Slack event supplies both identifiers. Deduplication, content
filters, and Bolt's self-event suppression still run. This grants no approval
rights and does not create a human principal.

Trust only a dedicated sender that does not automatically reply to Curie.
The dispatcher cannot tell whether another bot identity is another Curie
installation: allowlisting such a bot removes the cross-installation loop guard
for that pair. All unlisted bot/channel pairs retain the default refusal.
Remove the pair and restart the dispatcher to revoke threaded admission.

Compose forwards this variable in both dev and generated release stacks. Helm
operators can set the same variable with `dispatcher.extraEnv`; no new chart
value is required. Runtime Slack proof requires an installed sender app and
live delivery; the Bolt/Valkey tests alone do not establish that Slack delivered
or that the worker answered.

## The queue seam (what the worker consumes)

The dispatcher `XADD`s onto a Valkey Stream (`CURIE_STREAM`, default
`curie:runs`). Each entry carries one field, `payload`, holding the JSON of a
`QueuedTurn` (from `aci_protocol`). Its fields are channel-neutral; the
parenthetical is what the Slack adapter maps onto each one:

| field | meaning |
|---|---|
| `event_id` | idempotency key for the delivery (the Slack event id) |
| `conversation_id` | canonical thread/conversation key (the thread ts) |
| `author` | who authored the message (the Slack user id) |
| `text` | message text |
| `reply_handle` | where the reply is delivered: a `ReplyHandle` of `channel`, required nullable `placeholder`, and an optional per-turn `endpoint`. The Slack adapter currently supplies the ts of its already posted placeholder. |
| `received_at` | ISO-8601 UTC timestamp the adapter received it |

The worker reconstructs it with `from_stream_fields(fields)`, a module-level
helper in `curie_dispatcher.queue`. The model lives in the frozen `aci_protocol`
package (promoted out of the dispatcher in issue #7) so the producer and the
Rust/TS consumers share one schema-gated contract instead of a hand-mirrored copy;
the dispatcher's queue module owns only the Stream transport of it. The
single-`payload`-field encoding keeps the seam explicit and lets fields be added
without reshaping the Stream schema.

## Dedupe (idempotency)

Idempotency key = Slack event id (detailed-architecture 2b rule 5). A retried
Slack delivery must not enqueue twice. Before posting or enqueuing, the dispatcher
claims the event with a Valkey `SET <dedupe_prefix><event_id> 1 NX EX <ttl>`; the
first delivery wins and proceeds, a retry finds the key set and is dropped (still
acked, never re-posted, never re-enqueued). Chosen over stream-side dedupe because
it is O(1), TTL-bounded (no unbounded dedupe set to prune), and needs no Stream
scan. Order is claim -> post placeholder -> `XADD`, so a duplicate never produces
a second placeholder.

## Reconnect supervision and shutdown

`dispatcher.supervisor.Supervisor` drives a transport-agnostic `Connection`
(anything that blocks in `run` until the link drops and unblocks on `close`). The
builtin Slack client self-heals transient websocket drops; the supervisor is the
outer net for failures it cannot recover (the connection factory raising on
connect, an unrecoverable exit) and the owner of graceful shutdown. On a drop it
sleeps for an exponential, capped backoff (`BackoffPolicy`) and reconnects with a
fresh connection; `request_stop` (wired to SIGINT/SIGTERM) closes the current
connection and exits the loop without reconnecting. The Socket Mode adapter
(`app.SocketModeConnection`) is the thin production `Connection`.

## Config surface (env vars)

Read from the environment by `DispatcherConfig()` (a `pydantic_settings.BaseSettings`).

| env var | default | meaning |
|---|---|---|
| `SLACK_APP_TOKEN` | "" | app-level token (`xapp-...`), Socket Mode |
| `SLACK_BOT_TOKEN` | "" | bot token (`xoxb-...`), Web API |
| `SLACK_SIGNING_SECRET` | "" | optional; unused in Socket Mode, kept for Bolt App construction |
| `VALKEY_HOST` | `localhost` | Valkey host (in-cluster: `valkey`) |
| `VALKEY_PORT` | `6379` | Valkey port (compose maps it to `26379` on the host) |
| `VALKEY_PASSWORD` | "" | Valkey password (compose dev: `valkeypass`) |
| `VALKEY_DB` | `0` | Valkey db index |
| `CURIE_SLACK_THREADED_BOT_ALLOWLIST` | `[]` | exact `{channel_id, bot_id}` pairs allowed to mention the agent inside channel threads |
| `VALKEY_TLS` | `false` | when `true`, the client connects over TLS and verifies against the system CA bundle; the chart sets this from `valkey.tls` |
| `CURIE_STREAM` | `curie:runs` | Stream the jobs land on |
| `CURIE_DEDUPE_PREFIX` | `curie:dedupe:` | dedupe key prefix |
| `CURIE_DEDUPE_TTL_SECONDS` | `3600` | dedupe guard TTL |
| `CURIE_PLACEHOLDER_TEXT` | `On it. Working on your request.` | placeholder reply text |
| `CURIE_BACKOFF_INITIAL_SECONDS` | `1.0` | first reconnect backoff |
| `CURIE_BACKOFF_MAX_SECONDS` | `30.0` | backoff cap |
| `CURIE_BACKOFF_MULTIPLIER` | `2.0` | backoff growth factor |
| `CURIE_API_URL` | `http://localhost:8000` | platform API used to resolve approval clicks (compose: `http://curie-api:8000`). `CURIE_API_BASE_URL` is a deprecated alias. |
| `CURIE_API_KEY` | `curie-dev-key` | platform administrative key; sent for compatibility with API plumbing, but it is not resolver identity and cannot authorize a resolution alone |
| `CURIE_APPROVAL_CHAT_ATTESTER_SECRET` | `curie-dev-approval-chat-attester` | independent HMAC secret shared only with the API; signs short-lived, approval-bound `chat` principals. Must be nonblank and must not equal `CURIE_API_KEY`. |
| `CURIE_API_PREFLIGHT_TIMEOUT_SECONDS` | `30.0` | API-health budget, followed by a fresh same-size discovery-and-Slack budget; the Helm chart supplies 120 seconds while a directly run dispatcher keeps this 30-second default; must be positive |

### Boot preflights

Before Socket Mode starts, `main()` runs these bounded preflights in order:

1. Platform API reachability: poll `GET {CURIE_API_URL}/health`, reusing the
   `CURIE_BACKOFF_*` tunables for the poll interval, within
   `CURIE_API_PREFLIGHT_TIMEOUT_SECONDS`.
2. Destination discovery: make an authenticated `GET {CURIE_API_URL}/agents`
   using `CURIE_API_KEY` and collect configured Slack destinations.
3. Slack metadata capability attempts: make one bounded
   `conversations.list(types=public_channel, limit=1)` public-channel
   capability call, then use `conversations.info` for every configured
   destination before opening the Socket Mode connection.

After health succeeds, discovery and Slack metadata checks share a fresh
`CURIE_API_PREFLIGHT_TIMEOUT_SECONDS` budget. The reachability and discovery
phases retry rather than probing once so a slow-starting API does not fail a
healthy stack. If the health budget is exhausted, correct `CURIE_API_URL`. If
discovery cannot finish, check platform API availability and configuration,
then retry.

If the Slack phase cannot attempt every configured destination before its
budget, startup refuses safely; restore Slack availability or increase
`CURIE_API_PREFLIGHT_TIMEOUT_SECONDS`, then retry. These failures identify only
the failing phase and recovery action; they never print configured Slack
destinations or credentials.

The public-channel capability call always runs after discovery, including when
there are zero configured destinations. Its Slack `missing_scope` or documented
`invalid_types` permission outcome maps unambiguously to the recovery below
without reading or logging its `needed` or `provided` sets. Production creates
a dedicated no-retry Slack WebClient for each metadata attempt, with an integer
timeout derived from the remaining phase budget and capped at two seconds. The
phase budget prevents starting additional calls after it expires; a final
already-started call can extend it only by that bounded timeout.

API-health exhaustion exits nonzero with guidance to check `CURIE_API_URL` and
whether the API pod is Ready. The Helm chart supplies 120 seconds for each
phase and gives its startup probe an earliest failure cutoff of 260 seconds,
strictly beyond the two 120-second budgets plus the final bounded two-second
Slack call. Rendering rejects settings where the startup cutoff does not
strictly outlast that full application startup envelope.

The gates run once at boot only. The heartbeat starts only after every preflight
succeeds, so Kubernetes startup, readiness, and liveness checks remain gated
while the dispatcher is polling. The startup probe prevents Kubernetes from
restarting the pod during the application startup envelope. If a dependency
never becomes ready, the application still reaches its own bounded terminal
failure before the startup probe can restart it. The gate is not a liveness monitor:
an API restart later does not kill the dispatcher (the heartbeat probes own
liveness, and the resolve call degrades per call on its own). There is no off
switch: the gate is the point, so a nonpositive timeout is rejected as a config
error at boot.

`/health` is deliberately unauthenticated and proves reachability only. The
following `/agents` discovery is authenticated with `CURIE_API_KEY`. Discovery
failures remain a fixed, redaction-safe platform API failure rather than
printing credentials or response bodies. The independent approval chat-attester
secret is not checked by these preflights.

The only boot-fatal Slack metadata responses are `missing_scope` and
`invalid_types` from that fixed public-channel capability request. Both use the
same missing-public-permission recovery:
`Slack channel capability preflight failed: bot token is missing required scope channels:read. Add channels:read under OAuth & Permissions > Bot Token Scopes, then reinstall the app to the workspace.`
The preflight logs only a safe public capability state (`verified` or
`unverified`) and aggregate checked or unverified destination counts. Other
capability-call failures, plus private, stale, or transient per-destination
`conversations.info` outcomes, remain aggregate unverified warnings and do not
stop Socket Mode once every destination has been attempted; no tokens or
destination identifiers are printed.

An unverified warning can recur for a private channel, DM, stale destination,
or transient provider outcome. It is deliberately non-terminal and does not
assert private-channel or DM scope coverage; inspect the trusted configuration
and correct the destination or access separately.

The two Slack tokens and the independent approval chat-attester key are secrets. When a workspace exists the Slack tokens come from
the app's install (App-Level Token with `connections:write` for `SLACK_APP_TOKEN`;
Bot User OAuth Token for `SLACK_BOT_TOKEN`), delivered as env vars (a K8s Secret
in the chart). The chart or compose stack supplies the attester secret to the dispatcher
and API only; it must not be reused as the platform key or exposed to workers, runners,
the Console, or operators.

## Run it

```bash
python -m curie_dispatcher
```

## Runbook: point it at a real Slack workspace (once one exists)

1. Create a Slack app. Fastest path: at <https://api.slack.com/apps> choose
   "From a manifest" and paste [`slack-app-manifest.yaml`](slack-app-manifest.yaml),
   which already sets Socket Mode on and the bot scopes (`app_mentions:read`,
   `chat:write`, `channels:read`, `im:history`, `im:read`), plus the
   `app_mention` and `message.im` event subscriptions. If the app was already
   installed before `channels:read` was added, reinstall it to the workspace so
   its bot-token grant is refreshed. (To configure the app manually, include
   those same scopes.)
2. Generate an App-Level Token with `connections:write` -> `SLACK_APP_TOKEN`
   (`xapp-...`); copy the Bot User OAuth Token -> `SLACK_BOT_TOKEN` (`xoxb-...`).
3. Set both env vars (plus `VALKEY_*` for the target Valkey) and run
   `python -m curie_dispatcher`. @-mention the bot in a channel it is in, or DM
   it: you should see the placeholder reply appear and one entry land on the
   Stream (`XLEN curie:runs`). The worker consumes from there.

## Verification (Slack-free)

All tests run without a Slack workspace: the Slack Web API client and socket
transport are the only things faked; Stream and dedupe assertions run against the
real Valkey from `compose.dev.yaml` (host port `26379`). From the repo root:

```bash
docker compose -f compose.dev.yaml up -d valkey
uv run pytest apps/dispatcher/tests -q
```

`tests/test_dispatch.py` drives Bolt's real `SocketModeHandler.handle` end to end
(envelope -> ack -> placeholder -> `XADD`), including the duplicate-delivery and
event-filtering cases; `tests/test_queue.py` covers the seam and dedupe against
real Valkey; `tests/test_supervisor.py` covers backoff, reconnect, and graceful
shutdown with a fake connection.

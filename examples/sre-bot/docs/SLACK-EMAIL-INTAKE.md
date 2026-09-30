# Slack Email alert intake

Tracked by [issue #3527](https://github.com/curie-eng/curie/issues/3527).
This is an opt-in source for installations whose alert provider delivers email
into a Slack channel. It does not replace the direct Alertmanager source.

The intake owns only discovery and acknowledgement. Curie still owns the turn,
the sandbox, the SRE skill, and delivery. The intake uses the same Slack bot
identity as the selected Curie Slack adapter so the worker is allowed to edit
the placeholder it posts.

## Contract

### SRE-EMAIL-1 — select only configured alert roots

On every poll, the intake lists top-level messages in one configured channel,
at or after a configured timestamp. A candidate must:

- have the configured Slack Email source user, and the configured source bot
  when one is supplied;
- carry exactly one `text/html` file; and
- have a file title or name beginning with one of the configured subject
  prefixes.

Source identities, channel ids, prefixes, and the timestamp floor are runtime
configuration. This public example carries no tenant-specific values. The scan
is oldest first and follows Slack pagination, so a later alert cannot starve an
older one.

One known matching root is configured as the canary. Every complete scan must
still find it, classify it as a candidate, read its replies, download its file,
and receive the source conversation id from the signed hook. Once its original
delivery has completed, the stable delivery id makes this a duplicate receipt,
not another turn. This distinguishes a genuinely quiet channel from a broken
source identity, subject prefix, file permission, or hook configuration.

### SRE-EMAIL-2 — one ordinary turn per unacknowledged root

A nonempty reply from the configured Curie bot is a durable acknowledgement.
When no such reply exists, the intake posts exactly
`Investigating this alert...` in the root thread, downloads the private HTML
file, converts it to bounded text, and sends one signed `email-alert` hook.

The hook names the root message as `conversation_id` and the reply as
`placeholder`. Its delivery id is stable for the source channel and root
timestamp. A retry reuses the existing placeholder and the same delivery id;
the hook receipt must name the requested root. These constraints make retries
idempotent on an API that supports explicit reply targets.

Curie v0.11.0 does not accept `conversation_id` or `placeholder` on the hook
route. It ignores those query parameters, queues a placeholderless turn on its
synthetic hook conversation, and then returns a receipt naming that conversation.
The intake rejects that receipt and stops, but the turn has already been queued:
its answer posts at channel level, outside the email thread. Retrying the same
delivery id returns the original conversation and cannot repair its target.
Receipt validation therefore detects this incompatibility only after enqueue.

The payload contains source metadata and the extracted email text. It is
untrusted evidence, never instructions. The SRE bot remains read-only for these
automated turns: it may inspect and explain, but it must not call a mutating
tool or raise an approval from an email alert.

### SRE-EMAIL-3 — no silent failure

Configuration, Slack API, file download, hook authentication, hook routing, and
receipt mismatches are fatal. Network calls have finite timeouts. An unchanged
placeholder older than the configured deadline is also fatal. The Deployment
uses one replica and `Recreate`, so rollouts cannot race two placeholder posts.
Slack API and private-file redirects are followed only within `slack.com`; an
external redirect is fatal before the bot bearer token can be forwarded.

The process serves `/livez`, `/readyz`, and Prometheus metrics. Readiness is
true only after a successful complete scan and becomes false when that success
is older than two poll intervals. Kubernetes restarts a failed process, while
the SRE observability rules page when the Deployment has no ready replica, is
scaled to zero, or its container restarts.

The `sre-slack-email-intake-code` ConfigMap is the durable opt-in marker. While
it exists, an absent Deployment is a failure and continues paging; a deliberate
uninstall removes the Deployment, Secret, code ConfigMap, and its alert routing
together. This avoids an absence alarm on installations that never opted in
without letting a deleted Deployment become quiet after metric retention ends.

Failures after the hook was accepted stay covered by the platform alerts:

| Broken stage | Signal |
| --- | --- |
| Intake stopped, hung, misconfigured, or unable to scan | `SreSlackEmailIntakeNotReady` |
| Intake process crashed and restarted | `SreSlackEmailIntakeRestarted` |
| Turn rejected or classified as failed | `CurieTaskFailure` |
| Accepted turn remains queued | `CurieQueueMessageAgeHigh` |
| Completion cannot reach Slack | `CurieCompletionOutboxAgeHigh`, `CurieReplyDeliveryRefused` |
| Completion-delivery telemetry disappears | `CurieCompletionOutboxSignalAbsent` |

Prometheus evaluating a rule is not delivery proof. Operators must route the
page-level rules to an independently observed notification destination and use
the existing Alertmanager heartbeat to detect a broken notification path.

## Configuration

Before applying the intake Deployment, install an API release that includes
[ADR 0182](../../../docs/adr/0182-a-signed-hook-may-complete-a-preposted-reply.md).
Confirm that the running API's OpenAPI description declares both
`conversation_id` and `placeholder` query parameters on
`POST /hooks/{agent_id}/{hook}`. Curie v0.11.0 does not meet this prerequisite;
do not send a trial hook to discover compatibility, because it can enqueue a
detached turn before the intake rejects its receipt.

The Deployment reads a Secret named `sre-slack-email-intake` with these keys:

- `SLACK_BOT_TOKEN`
- `SLACK_CHANNEL_ID`
- `SLACK_EMAIL_SOURCE_USER_ID`
- `ALERT_SUBJECT_PREFIXES` (comma-separated)
- `SLACK_SCAN_NOT_BEFORE` (Slack timestamp or Unix seconds)
- `SLACK_CANARY_THREAD_TS` (a matching root at or after that floor)
- `CURIE_HOOK_URL` (the full `/hooks/{agent}/email-alert` URL)
- `CURIE_HOOK_SECRET`

Optional keys are `SLACK_EMAIL_SOURCE_BOT_ID`, `CURIE_SLACK_ADAPTER`,
`POLL_SECONDS`, `PLACEHOLDER_STALE_SECONDS`, and `HTTP_TIMEOUT_SECONDS`.
`PLACEHOLDER_STALE_SECONDS` must be greater than two poll intervals.

The Slack app needs permission to read the configured channel and its thread
replies, download the private email file, and post in the thread. Downloading a
`url_private` file requires the bot token's `files:read` OAuth scope; message
history access alone is insufficient. Add the scope and reinstall the app
before using that token for the intake. It must be a member of a private
channel. The selected Curie Slack binding and adapter must address the same
channel and use this bot identity.

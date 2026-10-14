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

The first scan after startup lists top-level messages in one configured channel
at or after the configured timestamp floor. Later successful scans move the
discovery window to the previous scan start minus the placeholder deadline and
two poll intervals. Failed scans never advance that window. Pending roots remain
tracked until a completed reply acknowledges them, even outside the discovery
window. Restart reconstructs acknowledgement state from Slack starting at the
configured floor. A candidate must:

- have the configured Slack Email source user, and the configured source bot
  when one is supplied;
- carry exactly one `text/html` file; and
- have a file title or name beginning with one of the configured subject
  prefixes.

Source identities, channel ids, prefixes, and the timestamp floor are runtime
configuration. This public example carries no tenant-specific values. The scan
is oldest first and follows Slack pagination, so a later alert cannot starve an
older one.

Acknowledged roots within the discovery overlap are cached in memory and do
not reread their replies. That cache expires with the overlap window.

One known matching root is configured as the canary. Each scan looks it up
directly by timestamp, independent of the discovery window. Every complete scan
must still classify it as a candidate, read its replies, download its file,
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
untrusted evidence, never instructions. The SRE skill instructs these automated
turns to inspect and explain, without calling a mutating tool or raising an
approval. This is standing prompt policy, not runtime enforcement: hook turns
currently retain the agent's ordinary tools and approval flow under ADR 0099.
[Issue #3603](https://github.com/curie-eng/curie/issues/3603) tracks the required
trusted per-turn restriction. Installations requiring enforced no-mutation and
no-approval must wait for that reviewed contract and its worker/runner adoption.

### SRE-EMAIL-3 — no silent failure

Configuration, non-rate-limit Slack API and file download errors, hook
authentication, hook routing, and receipt mismatches are fatal. Slack HTTP 429
and `ratelimited` responses pause scanning according to `Retry-After` (one poll
interval when the header is absent or invalid). An interrupted scan never marks
readiness successful; prolonged throttling expires readiness and pages without
a restart loop. Network calls have finite timeouts. An unchanged
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

## Remediation policy boundary

Automated remediation ([operator guide](../../../docs/operations.md#automated-remediation))
does not apply to this intake, and nothing here should be read as if it did.

- A remediation policy binds only to a **protected** hook, one with an active
  protected source policy. The `email-alert` hook this intake signs is an
  ordinary hook, and neither this bundle nor its installer configures a protected
  source policy or a remediation policy. A turn it starts cannot nominate: a
  `curie-remediation` block in its answer is plain text that is neither captured
  nor submitted, so no approval is raised and nothing executes.
- Turning remediation on does not change that. The "inspect and explain, never
  mutate" rule above is still standing prompt policy, not enforcement, until the
  hook is protected under
  [ADR 0190](../../../docs/adr/0190-automated-hook-sources-cannot-widen-their-tool-access.md)
  and [ADR 0191](../../../docs/adr/0191-protected-hook-delivery-authority.md).
- A policy is a bound on what the platform will execute after a nomination. It
  does not prove the email was authentic, because the email text is untrusted
  evidence whether or not the hook is protected, and it does not make the model's
  analysis correct. The platform instead rechecks the condition with its own
  declared read and verifies recovery through a different connector.
- The pinned Kubernetes connector this example uses is an upstream server and is
  not a reversible connector in the sense of
  [Writing a connector the platform can undo](../../../docs/writing-a-reversible-connector.md):
  the bundle carries no `observe_version` or `restore` for it. A policy written
  for this bundle could therefore declare only `idempotent` actions, each needing
  its own qualification drills, and would still need a different connector to
  verify them.

Moving this intake onto a protected hook is a separate decision for the
installation. Do it, qualify each action
([drill order](../../../docs/operations.md#qualifying-an-action-the-drill-order)),
and only then rely on a policy.

## Configuration

Before applying the intake Deployment, install an API release that includes
[ADR 0182](../../../docs/adr/0182-a-signed-hook-may-complete-a-preposted-reply.md).
Confirm that the running API's OpenAPI description declares both
`conversation_id` and `placeholder` query parameters on
`POST /hooks/{agent_id}/{hook}`. Curie v0.11.0 does not meet this prerequisite;
do not send a trial hook to discover compatibility, because it can enqueue a
detached turn before the intake rejects its receipt.

Before scanning or posting a placeholder, startup fetches the running API's
OpenAPI description without a hook signature and requires both `conversation_id`
and `placeholder` query parameters on `POST /hooks/{agent_id}/{hook}`. Missing,
unreadable, redirected, or incompatible descriptions stop startup before any
hook can enqueue. Redirects are refused so a different service cannot advertise
capabilities for the configured hook API.
This checks reply-target support, not runtime read-only tool enforcement.

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
`PLACEHOLDER_STALE_SECONDS` must be greater than two poll intervals. Timing
values must be finite positive numbers; timestamp bounds must be finite
nonnegative numbers so an invalid bound or retry fallback cannot disable scanning.

The Slack app needs permission to read the configured channel and its thread
replies, download the private email file, and post in the thread. Downloading a
`url_private` file requires the bot token's `files:read` OAuth scope; message
history access alone is insufficient. Add the scope and reinstall the app
before using that token for the intake. It must be a member of a private
channel. The selected Curie Slack binding and adapter must address the same
channel and use this bot identity.

# apps/mail-adapter

The email channel adapter: an AgentMail inbox bridged to a Curie channel binding.

Two halves, and neither one knows anything about Slack:

- **Ingress.** It polls the inbox and POSTs each new message to the platform's
  channel ingress (`POST /channels/turns`) under its own scoped channel token,
  with the AgentMail `message_id` as the `delivery_id` so a retry is idempotent.
- **Egress.** It serves the neutral reply wire (`turn.status`, `reply.update`,
  `reply.post`, `turn.completed`), authenticating the platform on
  `X-Curie-Adapter-Secret` before any side effect, and sends one threaded
  AgentMail reply per `turn.completed`.

It holds no platform API key, no queue credential, and no platform database
access. Binding is an operator action at deploy time. Delivery and reply
ownership live in a local SQLite file on a ReadWriteOnce volume. The chart pins
one serialized writer and uses `Recreate`; persistence makes replacement safe,
not multi-writer operation.

## Inbound security

This section is the canonical one; the chart comment, `docs/operations.md` and
the channel-adapter guide summarize it. Four things sit between a stranger with
the inbox address and an agent turn, and they are not equally load-bearing.

1. **The provider filters first, and that is where the protection comes from
   today.** AgentMail runs SPF, DKIM and DMARC checks and drops a message whose
   authentication headers are present and explicitly fail, so a hard-fail never
   reaches the API at all. It also excludes the `spam`, `blocked` and
   `unauthenticated` categories from List Messages results by default
   (<https://www.agentmail.to/docs/messages>,
   <https://www.agentmail.to/docs/spam-virus-detection>).
2. **The adapter states those exclusions in its request rather than inheriting
   them.** Every list call, the priming one included, sends `include_spam=false`,
   `include_blocked=false` and `include_unauthenticated=false`
   (<https://docs.agentmail.to/api-reference/inboxes/messages/list>). That changes
   nothing about what a correct provider returns today, and that is the point: a
   provider that changes a default, or a key that carries the label-read
   permissions, cannot silently widen what reaches the agent. They are constants
   in the client. There is no values key and no env var that can turn them on.
3. **The `labels` check is defense in depth.** Any message whose `labels` array
   carries `unauthenticated`, `spam` or `blocked` is rejected before the
   allow-list and before any state write. In a correct install this never fires,
   and it is not what makes the install safe. It exists to catch a widened
   provider default or an over-permissioned key.
4. **The allow-list is a filter on the `From` header, and it authenticates
   nobody.** An SMTP `From` is attacker-controlled. `CURIE_MAIL_ALLOWED_SENDERS`
   keeps unwanted correspondents out; it is not sender authentication, and Curie
   performs none.

**A spoofed `From` from a non-enforcing domain defeats the allow-list.** AgentMail
delivers a DMARC failure when the sender's own policy is `none`, and such a
message carries authentication headers, so it is not labeled `unauthenticated`. An
allow-listed domain that publishes no DMARC record, or `p=none`, is therefore
still spoofable end to end, and no code in this adapter can close that. It closes
at the allow-listed domain's DNS.

**Enforcing DMARC does not close the gap, it narrows it.** DMARC aligns and
validates the sending *domain*; it says nothing about the mailbox local part. With
`alice@example.com` allow-listed, anyone who can send authenticated mail for
`example.com` -- another employee, a compromised account, a permissive relay, any
signer the domain authorizes -- can set `From: alice@example.com`, pass DMARC under
`p=reject`, carry none of the labels layer 3 rejects, and trigger an agent turn. Read
the four layers together this way: the provider's filtering, the explicit
`include_*=false` parameters and the `labels` check bound **what** gets in; DMARC
bounds **which domain** may claim to send; **nothing here authenticates an individual
mailbox**. So allow-listing a domain, or an address at a domain with senders other
than that one person, grants agent-trigger authority to everyone who can send
authenticated mail for that domain. Size the allow-list to that blast radius.

Two operator prerequisites follow, and neither is optional. Both are necessary and
neither is sufficient:

- [ ] **Every domain on the allow-list publishes `p=quarantine` or `p=reject`.**
      Without an enforcing DMARC policy the entry buys nothing.
- [ ] **The AgentMail key the adapter is given has `label_spam_read`,
      `label_blocked_read` and `label_unauthenticated_read` set to `false`**, per
      AgentMail's own guidance for agent-facing keys
      (<https://docs.agentmail.to/permissions>). That makes the exclusion a
      permission the provider enforces rather than a default it chooses.

## The allow-list

`CURIE_MAIL_ALLOWED_SENDERS` is a comma-separated list. Each entry is one of:

| entry | matches |
|---|---|
| `alice@example.com` | that address exactly |
| `example.com` | any address at that domain, with no subdomain matching |
| `*` | anyone at all |

Matching is case-insensitive, entries are stripped of surrounding whitespace,
empty segments (a trailing comma, a doubled comma) are dropped rather than read
as a match-anything entry, and a `From` header carrying a display name is matched
on the bare address inside it.

**Empty means deny everything, and it is refused at boot rather than served as
deny-all.** With `ADAPTER_INGRESS_ENABLED=true` and no allow-list the process
exits non-zero naming the variable. Allow-all is reachable only by writing `*`
explicitly, so the dangerous state is always named by an operator rather than
produced by omission.

**Mail rejected by the allow-list is dropped permanently, and widening the
allow-list later does not reprocess it.** The rejected `message_id` and security
decision are already durable. A rejection is logged once at WARNING with the
reason and a one-way correlation token, never the sender, subject, body, provider
message/thread id, or reply text. The message is left in the mailbox unmodified:
nothing is deleted, labeled or bounced. So after widening the list, use the
durable state and provider mailbox under the operator's PII controls, and ask the
correspondent to resend. There is no replay or reprocess-on-widen mechanism.

## Config surface (env vars)

Read from the environment by `MailAdapterConfig()` (a
`pydantic_settings.BaseSettings`). Every aliased field reads only its alias, so a
stray generic `PORT` or `POLL_INTERVAL` in the pod environment cannot reach one.

| env var | default | meaning |
|---|---|---|
| `AGENTMAIL_API_KEY` | "" | AgentMail API key, sent as `Authorization: Bearer`. Required |
| `AGENTMAIL_INBOX` | "" | the inbox address this adapter owns. Required |
| `AGENTMAIL_BASE_URL` | `https://api.agentmail.to/v0` | AgentMail API base |
| `CURIE_API_URL` | `http://localhost:8000` | platform API the ingress POST goes to (in-cluster: `http://curie-api:8000`). `CURIE_API_BASE_URL` is a deprecated alias |
| `CURIE_CHANNEL_TOKEN` | "" | the scoped `chn` token, sent as `X-API-Key` on ingress. Required |
| `CURIE_EGRESS_SECRET` | "" | shared secret the platform presents on `X-Curie-Adapter-Secret`. Required |
| `ADAPTER_INGRESS_ENABLED` | `true` | gates the poller only, never the egress server |
| `CURIE_MAIL_POLL_INTERVAL_SECONDS` | `5.0` | seconds between listings; must be greater than zero. A transport failure or 429 arms bounded exponential backoff on top, up to 60s, reset by the next successful listing |
| `CURIE_MAIL_INGRESS_ATTEMPTS` | `3` | short in-process attempts for transport ambiguity and retryable status; durable retry continues after this budget |
| `CURIE_MAIL_INGRESS_RETRY_DELAY_SECONDS` | `2.0` | base delay between those attempts; 429 may extend it with `Retry-After` |
| `CURIE_MAIL_PORT` | `8080` | port the egress server binds |
| `CURIE_MAIL_STATE_PATH` | `/var/lib/curie-mail/state.sqlite3` | local SQLite delivery-state file. The chart mounts it on a RWO PVC |
| `CURIE_MAIL_MAX_PENDING_DELIVERIES` | `1000` | maximum unresolved inbound deliveries admitted to SQLite; capacity refusal leaves provider mail recoverable |
| `CURIE_MAIL_MAX_BODY_BYTES` | `1048576` | maximum provider message body read or stored, in bytes |
| `CURIE_MAIL_MAX_REPLY_BYTES` | `1048576` | maximum accumulated outbound reply, in bytes |
| `CURIE_MAIL_MAX_STATE_BYTES` | `268435456` | maximum SQLite page budget; size the volume above this for the WAL and filesystem overhead |
| `CURIE_MAIL_ALLOWED_SENDERS` | "" | the allow-list above. Required while ingress is enabled |

### Boot gates

`main()` refuses to start and exits non-zero, naming the variable, when any of
`AGENTMAIL_INBOX`, `AGENTMAIL_API_KEY`, `CURIE_CHANNEL_TOKEN` or
`CURIE_EGRESS_SECRET` is unset, when `CURIE_MAIL_POLL_INTERVAL_SECONDS` is not
positive (a chart typo would otherwise be a tight loop against a third-party
API), or when ingress is enabled with an empty allow-list.

### Health and readiness

`GET /healthz` answers 200 with a fixed body and reveals nothing about the
install. `GET /readyz` stays non-200 until SQLite has opened and the first-start
prime or restart confirmation has completed. After startup it checks local state
only: an AgentMail outage does not flap readiness. `POST` to either path is not
special-cased; it requires the egress secret like every other POST, so a probe
path cannot become an unauthenticated write.

## Operations notes

- **Only first boot primes.** A new SQLite file lists the inbox and durably
  records the initial floor before becoming ready, so enabling an existing inbox
  does not replay its history. A replacement that opens an initialized file
  performs one provider confirmation without marking messages seen, then resumes
  pending and downtime mail. A new PVC is a new first boot.
- **Ingress is durable until terminal success.** Transport ambiguity, 202, 429
  (honoring `Retry-After`), 401 and server errors leave the same `delivery_id`
  pending. A documented terminal 200, including a 200 duplicate receipt, settles
  it. Token rotation therefore restarts the single replica and resumes the
  original row rather than losing it.
- **Provider failures are loud.** A `turn.completed` whose AgentMail send fails
  acks 502, so the platform retries and eventually dead-letters, instead of
  acking 200 and silently losing the email. A duplicate completion whose first
  attempt is still in flight acks 503 (come back later). An AgentMail outage
  therefore now produces visible retries and dead letters; that is the intended
  behavior, not a regression.
- **Reply ownership is per message.** Accumulated text is durable under
  `(conversation_id, reply_ref)`, and every update and completion uses the exact
  ref the platform returned. Two turns in one thread cannot clear or inherit one
  another's text. A null-ref post attaches only when exactly one live ref is
  unambiguous.
- **Provider-visible dedupe closes the accepted-send crash window.** The local
  event receipt is the fast path. After an uncertain send, the adapter reads the
  marker carried on the provider thread before retrying: found settles without a
  second email, absent plus an admitted row sends once, and unreadable or absent
  without an admitted row returns 502 without sending.
- **Capacity is fail-closed and recoverable.** Pending count, body bytes, reply
  bytes and SQLite pages are bounded before allocation. At capacity the adapter
  does not mark the provider message seen or evict older unresolved work; it
  leaves the message recoverable and logs back pressure.
- **The `chn` token expires.** The adapter cannot re-mint it (that would need a
  platform key it must not hold). It persists the ingress 401 and keeps the mail
  pending; the operator re-mints the scoped token and rolls the pod.
- **Logs are single-line JSON on stderr, and export is opt-in.** The adapter
  bootstraps the shared `curie-telemetry` service logger at start, so its output
  is one JSON object per record on stderr carrying `service.name:
  curie-mail-adapter`, a severity, the module logger name and a redacted
  message, rather than the plain text earlier versions printed -- expect a
  log-shipper or `kubectl logs` grep written against the old format to need
  updating. With no `OTEL_EXPORTER_OTLP_ENDPOINT` set, nothing is exported
  anywhere and only that redacting stderr handler runs, which is the supported
  local and air-gapped mode. With the chart's in-cluster collector deployed
  (`otelCollector.deploy=true`) the chart both sets the OTLP env and opens the
  adapter's egress policy to the collector. With `otelCollector.deploy=false`
  and an external `otelCollector.endpoint`, the env is set but the adapter's own
  egress policy has no peer for that address, so the operator must apply an
  additional egress policy selecting the adapter or the exports are silently
  dropped -- see the mail-adapter section of `charts/curie/README.md`. What is
  exported is log records: the adapter authors no spans of its own yet, so a
  trace search for it comes back empty even on a healthy export path.

### State, privacy, and recovery

The SQLite volume is sensitive application data. It can contain email addresses,
provider message/thread identifiers, message or reply text needed for recovery,
security decisions, and terminal delivery receipts. It contains no AgentMail
key, channel token, egress secret, platform key, or platform database credential.
Access to the PVC, its snapshots, and node-level backups is therefore access to
mail content even though it is not credential access.

The application bounds live state by count and bytes, but the PVC and its backup
retention are operator policy. Back up the SQLite file only with a
SQLite-consistent snapshot or after stopping the one writer. Restore the PVC
before starting the Deployment. Rolling back to a binary older than the on-disk
schema is refused; restore the pre-upgrade volume snapshot or roll forward
instead of deleting state to make an old image boot.
There is no selective erase command. For complete erasure, stop the adapter,
delete its PVC and every snapshot/backup, and start with a new claim, accepting
that the next start is a first boot and primes the current inbox.

## Run it

```bash
python -m curie_mail_adapter
```

## Verify

```bash
uv run pytest apps/mail-adapter/tests -q
```

Only the two external dependencies are faked, both as real local HTTP servers:
AgentMail's API and the platform's channel ingress. Everything inside
`curie_mail_adapter` runs for real, the egress server included, and the boot
gates are driven through the real `python -m curie_mail_adapter` entry point in a
subprocess.

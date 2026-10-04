# apps/mail-adapter

The email channel adapter: an AgentMail inbox bridged to a Curie channel binding.

Two halves, and neither one knows anything about Slack:

1. **Ingress.** It polls the inbox and rejects every AgentMail message with
   `authentication_unverifiable`. AgentMail supplies no positive sender
   authentication verdict that Curie can verify, so no message starts a turn or
   resolves an approval. The channel ingress transport uses the AgentMail
   `message_id` as the `delivery_id` so retries remain idempotent if a verified
   authentication source becomes available.
2. **Egress.** It serves the neutral reply wire (`turn.status`, `reply.update`,
   `reply.post`, `turn.completed`), authenticating the platform on
   `X-Curie-Adapter-Secret` before any side effect, and sends one threaded
   AgentMail reply per `turn.completed`.

It holds no platform API key, no queue credential, and no platform database
access. Binding is an operator action at deploy time. Delivery and reply
ownership live in a local SQLite file on a ReadWriteOnce volume. The chart pins
one serialized writer and uses `Recreate`; persistence makes replacement safe,
not multi-writer operation.

## Inbound security

The inbound gate fails closed. A message may reach channel ingress or approval
resolution only when its sender matches `CURIE_MAIL_ALLOWED_SENDERS` and Curie
itself verifies a positive authentication verdict for that sender. A provider
label, the absence of a rejection label, or a provider supplied header is never
that verdict.

**Every AgentMail message is currently rejected with
`authentication_unverifiable`.** The documented [Get Message response](https://docs.agentmail.to/api-reference/inboxes/messages/get)
and [List Messages response](https://docs.agentmail.to/api-reference/inboxes/messages/list)
contain labels and a generic headers map. They supply no trusted positive aligned
authentication verdict and guarantee neither header provenance nor stripping of
sender supplied authentication headers. AgentMail's [inbound authentication handling](https://docs.agentmail.to/knowledge-base/inbound-emails-missing)
also permits DMARC failure when the sending domain publishes `p=none`.
Neither an enforcing DMARC policy nor an allowlisted sender makes those API
fields independently verifiable. There is no supported authentication source in
this adapter, so it refuses unlabelled mail, allowlisted mail, and approval
answers alike before either platform endpoint can be called.

Every listing still sends `include_spam=false`, `include_blocked=false` and
`include_unauthenticated=false`, including the first listing. Those constants
reduce unwanted provider results; they establish no sender identity and cannot
substitute for authentication. Keep AgentMail's `label_spam_read`,
`label_blocked_read` and `label_unauthenticated_read` permissions disabled as
recommended in its [permissions guide](https://docs.agentmail.to/permissions).
Permission restrictions also cannot satisfy this gate.

The allowlist filters the sender's claimed `From` address. It authenticates no
sender. A wildcard changes only this filter and never bypasses the required
verdict. This section is canonical; chart values, `docs/operations.md` and the
channel adapter guide summarize it.

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

The platform checks the binding's own caller list (ADR 0175) after this
adapter's gate. That list can be edited through the platform without a redeploy;
it provides another authorization check and never supplies authentication.

**An empty allowlist fails boot while ingress is enabled.** The error names
`CURIE_MAIL_ALLOWED_SENDERS` and asks for explicit sender addresses or domains.
Any list containing `*` also fails boot unless
`CURIE_MAIL_ALLOW_ALL_SENDERS=true` is explicitly set. Its default is `false`;
the chart exposes the same opt in as `mailAdapter.allowAllSenders`. Even with
both settings present, all AgentMail messages remain rejected with
`authentication_unverifiable`.

**A security rejection is permanent, and widening the allowlist later does not
reprocess it.** The rejected `message_id` and security
decision are already durable. A rejection is logged once at WARNING with the
reason and a one-way correlation token, never the sender, subject, body, provider
message/thread id, or reply text. The message is left in the mailbox unmodified:
nothing is deleted, labeled or bounced. Inspect the durable state and provider
mailbox under the operator's PII controls. A future verifiable authentication
source would require new messages; rejected messages are not replayed.

**Another inbox of this installation must pass the same gate.** A second
mail adapter is a second identity (ADR-0168), and one inbox answers another only
if its address, or a domain entry that covers it, is on this list and its
sender authentication can be verified by Curie. Currently every such AgentMail
message is refused with `authentication_unverifiable`. The worker rate limits
an exchange between two inboxes if a verified authentication source later
permits one.

**A dropped completion that recorded no text sends no mail, for any drop
reason.** A dropped turn was never processed, so there is nothing of its own
to reply with, and mailing the empty-reply notice would be a new message —
from a sibling inbox, the next turn of the exchange the drop just ended. A
dropped completion that did record text (an undeployed or paused agent's
notice, say) still sends it, and a delivered completion with no text still
sends the empty-reply notice as before; only a drop with nothing recorded is
silent.

## Approvals by email

`CURIE_ADAPTER_PRINCIPAL` supplies the credential for carrying approval
answers (ADR 0177 and its amendment). Every answer must first pass the same
sender authentication and allowlist gate as a new turn. Currently every
AgentMail answer is rejected with `authentication_unverifiable`, including an
allowlisted approver quoting a live reference. No approval is resolved by email.
The request and settlement egress behavior remains available. The reply flow
below requires an independently verifiable sender authentication source before
it can admit an answer. Without the principal credential, the card's text is
mailed and email answers cannot be carried.

**The request.** When the worker posts the approval card into the thread, the
adapter adds the instructions and a random single-use reference, and keeps that
reference with the approval id and the thread. The card's ack carries a ref, so
the worker can settle this card later. The reference links a reply to its
approval. It proves nothing about who sent the reply: every reply quotes it.

The instructions say who can approve (ADR-0177 amendment A5). The worker sends the route's listed approver addresses with the card, as `Approver` fields, and the adapter reads the asking message's To and Cc from the provider. If a listed address is already on the thread (the requester counts), the email names the list and says who on the thread can answer. If none is, it says so, names the list, and asks the requester to reply all and add one or more of them, as many as they like. If the asking message cannot be read, it is worded to hold either way. The adapter uses the list only for wording; the platform still decides who may answer. A card from a worker that sends no `Approver` fields gets the generic instructions ("reply with APPROVE or REJECT on the first line; anything after it is your note; only an approver listed for this request can answer").

**Who receives what.** The request email is sent reply all to the asking message, so a listed approver copied there receives it. Every other reply goes to its sender only, as before, including the resumed answer, which therefore reaches the requester. The adapter never mails an address that is not already on the thread: bringing an approver in is the requester's choice, made by copying them. A reply from someone not listed that copies a listed approver in and carries no decision is the requester doing what was asked, and gets nothing back.

**The reply.** A message in a thread with an approval pending is never a turn.
It is an answer only when all of these hold:

1. Curie itself verified a positive sender authentication verdict, and the
   claimed sender matched `CURIE_MAIL_ALLOWED_SENDERS`. Provider labels or
   headers cannot supply this verdict, so every current AgentMail answer fails
   here before any approval logic and receives no response.
2. It names a reference issued in this thread, and that reference is still live.
3. It was not sent automatically: no `Auto-Submitted` other than `no` (RFC 3834),
   no `X-Autoreply` style header, no `Precedence: bulk`, `junk`, `list` or
   `auto_reply`, not a delivery report, not from `mailer-daemon` or `postmaster`.
   A message whose headers the provider did not return is treated as automatic.
4. The first line of its new text (AgentMail's `extracted_text`, which has the
   quoted history stripped) is `APPROVE` or `REJECT`. The rest of the new text is
   the note.

The adapter then calls `POST /approvals/{id}/resolve` with its credential and
the sender's bare address (lowercased, never the display name) as
`X-Curie-Approval-Actor`, and the platform decides: the binding's
`allowed_callers` must admit the sender, and the address must be on the route's
approver `emails` (ADR-0177 amendment). The person who asked is not admitted by default. A
reply that is not an answer gets the instructions back; a sender the platform
does not list is told they are not an approver, and who is; a sender the binding's
`allowed_callers` refuse gets nothing back; a reply to a spent reference is told
it was already answered. An automatic message gets no
response at all, so nothing loops. If the platform cannot be reached, or rejects
this adapter's credential, the message stays pending and a later pass carries
the same answer again.

**When it ends.** A sent email cannot be edited, so when the worker settles the
card the adapter sends one short follow-up in the thread (approved or rejected,
by whom, with the note, or expired) and spends the reference. It also reopens
the asking message's reply, so the resumed turn's answer is mailed in the same
thread.

The follow-up is sent reply all to the message that carried the winning answer, so the approver, the requester and everyone copied on it see who decided. If the requester is not on that message, because the approver replied to the bot alone, the requester also gets it as a direct reply to the asking message. With no email answer (an expiry), it is sent reply all to the asking message. Each of these sends is counted, so a settlement that failed part way is retried without sending a part twice. The first answer the platform accepts is final: any later answer is told the approval was already answered.

**A reference is not identity evidence.** It associates an answer with its
approval and never replaces sender authentication. Provider headers claiming
DMARC success, a copied reference, and membership in either allowlist cannot
resolve an approval. Use another supported channel for approval answers while
AgentMail sender authentication remains unverifiable.

The adapter principal is not rotated by the adapter yet. Re-mint it with `POST
/approvals/principals/adapter` before it expires, the same operator step as
`CURIE_CHANNEL_TOKEN`.

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
| `CURIE_ADAPTER_PRINCIPAL` | "" | the adapter principal credential (ADR 0156), sent as `X-Curie-Adapter-Principal` when carrying an admitted approval answer. Every AgentMail answer currently fails sender authentication before this credential is used (see "Approvals by email") |
| `ADAPTER_INGRESS_ENABLED` | `true` | gates the poller only, never the egress server |
| `CURIE_MAIL_POLL_INTERVAL_SECONDS` | `5.0` | seconds between listings; must be greater than zero. A transport failure or any 4xx refusal arms bounded exponential backoff on top, up to 60s; a successful 200 listing resets it, while 5xx responses retain their existing semantics and neither arm nor clear an already armed delay |
| `CURIE_MAIL_INGRESS_ATTEMPTS` | `3` | short in-process attempts for transport ambiguity and retryable status; durable retry continues after this budget |
| `CURIE_MAIL_INGRESS_RETRY_DELAY_SECONDS` | `2.0` | base delay between those attempts; 429 may extend it with `Retry-After` |
| `CURIE_MAIL_PORT` | `8080` | port the egress server binds |
| `CURIE_MAIL_STATE_PATH` | `/var/lib/curie-mail/state.sqlite3` | local SQLite delivery-state file. The chart mounts it on a RWO PVC |
| `CURIE_MAIL_MAX_PENDING_DELIVERIES` | `1000` | maximum unresolved inbound deliveries admitted to SQLite; capacity refusal leaves provider mail recoverable |
| `CURIE_MAIL_MAX_BODY_BYTES` | `1048576` | maximum provider message body read or stored, in bytes |
| `CURIE_MAIL_MAX_REPLY_BYTES` | `1048576` | maximum accumulated outbound reply, in bytes |
| `CURIE_MAIL_MAX_STATE_BYTES` | `268435456` | maximum SQLite page budget; size the volume above this for the WAL and filesystem overhead. Terminal `completion_events` rows (`delivered=1` or `deleted=1`, not both-required) are compacted oldest-first under the same derived ceiling as terminal receipts (at most 4096, and at most one quarter of the page budget). Unresolved and leased completion rows are never evicted to admit newer mail. A late duplicate whose row was evicted and whose provider marker is gone resends; keep the cap above the worker's 7-day completion retention if that duplicate must not fire |
| `CURIE_MAIL_ALLOWED_SENDERS` | "" | the claimed sender allowlist above. Required while ingress is enabled; never substitutes for sender authentication |
| `CURIE_MAIL_ALLOW_ALL_SENDERS` | `false` | explicit opt in permitting a `*` entry at boot. Never bypasses sender authentication, so every current AgentMail message is still refused |
| `CURIE_MAIL_AGENTMAIL_EGRESS_CIDRS` | "" | comma-separated CIDRs the egress policy admits for AgentMail. When set, the AgentMail client dials only addresses inside them (see below). Empty dials whatever DNS returns |
| `CURIE_MAIL_DISCOVERY_UNREADY_AFTER_SECONDS` | `120` | how long a continuous discovery failure run lasts before `/readyz` reports 503. Must be greater than zero |

### Egress pinning and discovery readiness

AgentMail sits behind CloudFront, which rotates edge IPs, while the chart's
NetworkPolicy admits a fixed CIDR snapshot. A dial to a rotated edge is rejected
by the cluster and logs the fixed cause `connection_refused` at status 0. With
`CURIE_MAIL_AGENTMAIL_EGRESS_CIDRS` set, the AgentMail client prefers resolved
addresses inside those CIDRs, falls back to the configured `/32` and `/128`
addresses, and fails the call (status 0) when nothing admitted is available. Only
the dialed IP changes: TLS still verifies the certificate against the URL
hostname. Platform API calls are never pinned.

Pinning cannot help once every configured `/32` has gone stale: CloudFront moves
`api.agentmail.to` between edges, and a pin list resolved at install time
eventually admits no live edge (#2824). Configure published provider ranges, or
`mailAdapter.agentmail.egressMode=publicHttps`, which leaves this variable empty;
see the mail adapter section of `docs/operations.md`.

Every discovery pass that does not return 200 extends the current failure run;
a 200 clears it. `/statusz` reports `discovery` as `ok`, `failing`, or
`unreachable` with the failure count and duration. Once the run exceeds
`CURIE_MAIL_DISCOVERY_UNREADY_AFTER_SECONDS`, the state is `unreachable`,
`/readyz` returns 503, and one ERROR is logged; one INFO is logged on recovery.
`/healthz` does not follow discovery, so an outage does not restart the pod.
The Service renders with `publishNotReadyAddresses: true`, so this 503 flips
the Deployment's `Available` condition (the operator signal for a discovery
outage) without removing the pod from Service endpoints: reply/completion
deliveries from the worker do not depend on discovery and keep reaching the
pod's POST handler through the outage.

### Boot gates

`main()` refuses to start and exits non-zero, naming the variable, when any of
`AGENTMAIL_INBOX`, `AGENTMAIL_API_KEY`, `CURIE_CHANNEL_TOKEN` or
`CURIE_EGRESS_SECRET` is unset, when `CURIE_MAIL_POLL_INTERVAL_SECONDS` is not
positive (a chart typo would otherwise be a tight loop against a third-party
API), or when ingress is enabled with an empty allowlist. A list containing
`*` is refused unless `CURIE_MAIL_ALLOW_ALL_SENDERS=true` is explicitly set.
The empty list error suggests explicit sender addresses or domains, never `*`.

### Health and readiness

`GET /healthz` answers 200 with a fixed body and reveals nothing about the
install. `GET /readyz` stays non-200 until SQLite has opened and the first-start
prime or restart confirmation has completed. It also returns 503 when ingress is
enabled and the configured channel token is missing, malformed, expired, or was
rejected with HTTP 401. Expiry is checked on every probe, including before any
inbound mail. An AgentMail outage does not flap readiness. Liveness remains 200
so an expired credential does not create a restart loop.

`GET /statusz` returns safe diagnostic metadata: token presence, the unverified
`exp` claim as Unix seconds, state (`ok`, `expiring` within five minutes,
`expired`, `rejected`, `missing`, `invalid`, or `disabled`), and the last platform
ingress HTTP status since process start. It never returns the token or its
channel identity. The adapter cannot verify the signature: `ok` means the
decoded expiry is in the future and no 401 has been observed, not proof that
the platform will accept the token. A 401 latches rejection until a successful
ingress response or process restart. `POST` to a probe path still requires the
egress secret like every other POST.

`curie cluster status --json` includes these diagnostics on the mail pod and
reports unhealthy for an unusable token. `curie doctor --json` reports a mail
channel check with expiry, last ingress status, and a recovery command. Both
read the adapter through Kubernetes pod proxy access, so diagnostics remain
reachable regardless of readiness state; the Service also keeps routing the
pod's address while unready (`publishNotReadyAddresses: true`), so reply and
completion deliveries reach it the same way. Unavailable diagnostics report
unknown instead of healthy.

For recovery, run `curie cluster channel-token <agent> --kind email --address <inbox>`.
That mints a replacement via `POST /channels/token`, writes it into the Secret
key supplying `CURIE_CHANNEL_TOKEN`, rolls the mail-adapter Deployment, and
prints `exp` without printing the token. `curie doctor` reports the same `exp`
and names that verb as the fix. No platform signing key is given to the adapter.

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
- **A caller-list refusal is final; any other 403 is not.** The channel port
  answers 403 with `{"detail": "caller_not_allowed"}` when the binding's own
  caller list does not admit the sender (ADR 0175). The adapter settles that
  message without a turn, the same as one its own gate rejected, and never posts
  it again; nothing is sent back to the sender. A 403 with any other body, such
  as one from a proxy or firewall in front of the platform, leaves the delivery
  pending and is retried like a 5xx.
- **Provider failures are loud.** A TCP connection refusal while reading the
  provider thread witness or sending the reply returns 424 with
  `{"detail":"provider egress refused"}`. The worker stores that fixed cause on
  the owed completion record, which remains pending for retry. Other retryable
  witness and send failures return 502 and also leave the completion owed.
  A duplicate completion whose first attempt is still in flight returns 503.
  These failures produce visible retries instead of silently losing the email.
- **Reply ownership is per message.** Accumulated text is durable under
  `(conversation_id, reply_ref)`, and every update and completion uses the exact
  ref the platform returned. Two turns in one thread cannot clear or inherit one
  another's text. A null-ref post attaches only when exactly one live ref is
  unambiguous.
- **Deliberate progress is silent.** A `reply.update` or `reply.post` carrying
  reply wire 1.1 `progress` (ADR-0130) is acknowledged 200 with no `ref` and
  changes nothing: not the buffered reply text, not its ref, and no email. An
  email turn sends one message, so a card edit has nothing to edit, and a
  progress post appended to the buffered reply would put task-status lines
  into the answer the correspondent reads. Silence is the conforming choice the
  channel-adapter guide allows.
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
  egress policy has no peer for that address, so the chart requires
  `mailAdapter.otelEgress.httpsCidrs` and refuses the render without it -- see
  the mail-adapter section of `charts/curie/README.md`. What is
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

A file already larger than `CURIE_MAIL_MAX_STATE_BYTES` (typically after
lowering the cap; `PRAGMA max_page_count` already clamps organic growth) refuses
to boot. Deleting rows does not shrink `st_size`. Recover an offline copy with
no manual SQL, then swap it in while the adapter is stopped:

```bash
python -m curie_mail_adapter recover --state /path/to/copy.sqlite3
```

Do not point recover at a live writer. The command compacts terminal
completions, VACUUMs the copy, and exits 0 only when the file is under the byte
budget.

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

### Deleted provider threads

A thread lookup returning HTTP 404 after admission is terminal for that reply
only when the 404 carries the provider's own JSON body. A 404 whose body is not
JSON came from an edge, gateway or stale route rather than AgentMail, and stays
retryable as an unreadable witness (502): the receipt below is permanent, and a
signal this ambiguous must never write one.
The adapter durably records deletion, logs one warning with a hashed correlation,
and returns HTTP 410 on the first and any duplicate completion, including after
restart. The response body carries `{"detail":"thread deleted at provider"}`;
HTTP 410 without that explicit classification remains retryable for other adapters.
It never records the reply as delivered. The worker dead-letters the
completion on the first definitive refusal (N = 1) with reason `thread deleted
at provider`, atomically removing its owed completion from the pending index.
The existing configured dead-letter stream and size cap apply. Earlier transient
failures do not spend this terminal budget: provider 5xx and transport failures
still return 502 and remain retryable. A missing local admission record also
remains retryable. HTTP 410 applies only to completion delivery; other reply
events and other error statuses keep their existing retry behavior.

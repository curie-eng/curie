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

It holds no platform API key, no queue credential, and no database access.
Binding is an operator action at deploy time. All routing state is in memory and
process-local, so the chart pins it to a single replica.

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
allow-list later does not reprocess it.** The rejected `message_id` is already
marked seen and the poller only looks forward. A rejection is logged once at
WARNING naming the address, the `message_id` and the reason, and the message is
left in the mailbox unmodified: nothing is deleted, labeled or bounced. So after
widening the list, check the adapter's logs for what was dropped, and ask the
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
| `CURIE_MAIL_POLL_INTERVAL_SECONDS` | `5.0` | seconds between listings; must be greater than zero |
| `CURIE_MAIL_INGRESS_ATTEMPTS` | `3` | ingress POST attempts, on transport failure only |
| `CURIE_MAIL_INGRESS_RETRY_DELAY_SECONDS` | `2.0` | delay between those attempts |
| `CURIE_MAIL_PORT` | `8080` | port the egress server binds |
| `CURIE_MAIL_ALLOWED_SENDERS` | "" | the allow-list above. Required while ingress is enabled |

### Boot gates

`main()` refuses to start and exits non-zero, naming the variable, when any of
`AGENTMAIL_INBOX`, `AGENTMAIL_API_KEY`, `CURIE_CHANNEL_TOKEN` or
`CURIE_EGRESS_SECRET` is unset, when `CURIE_MAIL_POLL_INTERVAL_SECONDS` is not
positive (a chart typo would otherwise be a tight loop against a third-party
API), or when ingress is enabled with an empty allow-list.

### Health

`GET /healthz` answers 200 with a fixed body and reveals nothing about the
install. `POST /healthz` is not special-cased: it requires the egress secret like
every other POST, so a probe path cannot become an unauthenticated write.

## Operations notes

- **Priming on start discards history.** The adapter lists the inbox at startup
  and marks everything seen, so a restart drops mail that arrived while the pod
  was down. That is the documented behavior for a live adapter, not a backfill
  tool.
- **Provider failures are loud.** A `turn.completed` whose AgentMail send fails
  acks 502, so the platform retries and eventually dead-letters, instead of
  acking 200 and silently losing the email. A duplicate completion whose first
  attempt is still in flight acks 503 (come back later). An AgentMail outage
  therefore now produces visible retries and dead letters; that is the intended
  behavior, not a regression.
- **Two turns in one thread share their reply text.** The reply *target* is
  per-message (`target.reply_ref`), so each answer lands on the message that
  asked. The reply *text* is keyed by `conversation_id`, because the platform
  keeps one live session per conversation and `reply.post` accumulates within it.
  Two messages racing in one thread therefore produce two correctly addressed
  replies both carrying the conversation's latest text. Two *turns* racing in one
  thread is a known defect; see Known reliability limitations below.
- **Memory profile.** `seen` (polled message ids), `replied_event_ids`, and
  `body_retry` (messages whose body fetch failed, retried by id on a later
  pass) are all bounded FIFO maps, `RETRY_MAX` capping `body_retry` at 200
  entries. A single message in `body_retry` is retried at most
  `BODY_ATTEMPT_MAX` (5) times before it is abandoned with an error log.
  `conversations` is not bounded: it grows with the number of distinct
  threads from admitted senders since the last restart, and evicting from it
  would make a legitimate late `turn.completed` unrepliable. `page_cursor`
  holds a single optional page token: a poll pass walks at most
  `POLL_MAX_PAGES` (5) listing pages and resumes from that cursor on the next
  pass instead of restarting at page one, so a large backlog drains over
  several passes rather than one, except when a list call fails and clears the
  cursor (see Known reliability limitations below).
- **The `chn` token expires.** The adapter cannot re-mint it (that would need a
  platform key it must not hold). It logs the ingress 401 and keeps polling; the
  operator re-mints and rolls the pod.

### Known reliability limitations

Three defects are open in the shipped adapter and tracked together in #1584 for
the reliability rework. Each needs an adverse provider condition or a specific
concurrent interleaving; none is reachable on the normal path. Each one is
silent, so what an operator sees is a message that never arrives as a turn, or a
reply carrying `EMPTY_REPLY_TEXT` ("Curie processed your message but produced no
text") instead of the answer.

- **A listing failure while a large backlog is draining strands the mail behind
  it.** A pass walks at most `POLL_MAX_PAGES` (5) pages of `POLL_LIMIT` (20) and
  keeps its stopping point in `page_cursor`, but a failed list call clears that
  cursor. The next pass restarts at the newest page and stops as soon as a page
  carries mail already in `seen`, so messages older than that point are never
  listed again and never become turns. Triggering condition: a burst larger than
  `POLL_LIMIT * POLL_MAX_PAGES` (100 messages) still draining when the provider
  returns an error on a list call.
- **Two turns racing in one mail thread can cross their answers.** Reply text is
  held per conversation, and it is cleared on a successful send without checking
  that the text still belongs to the turn that sent it. If a second turn records
  its answer while the first turn's send is in flight, the second turn's answer
  can be cleared or attributed to the first turn, and one of the two turns emails
  `EMPTY_REPLY_TEXT` instead of its real answer. Triggering condition: two
  concurrent turns in one thread with that interleaving.
- **Enough simultaneously failing body fetches drop messages.** A message whose
  body fetch fails is retried out of `body_retry`, bounded at `RETRY_MAX` (200)
  entries with `BODY_ATTEMPT_MAX` (5) attempts each. Beyond that bound the oldest
  entries are evicted while their ids stay in `seen`, so they are neither retried
  nor picked up by a later listing. Triggering condition: body fetches failing
  for more than `RETRY_MAX` distinct messages at once.

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

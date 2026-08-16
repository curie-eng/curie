# Building a channel adapter

How to put Curie on a channel it has never heard of (a mail server, a support
desk, a webhook bus) without changing the platform. Since ADR-0096 phase 2 the
channel port is neutral: the platform never learns your channel's shape, and you
never need a patch merged into this repo to ship one.

## 1. What an adapter is

One service you deploy and own, doing two things:

- **Ingress.** It posts each inbound message to `POST /channels/turns` on the
  Curie API, under a credential scoped to one binding.
- **Egress.** It serves an HTTP endpoint that the worker POSTs reply events to,
  authenticated with a shared per-adapter secret.

Nothing else. The adapter holds no platform key, no queue credential, and no
database access. Binding is an operator action at deploy time.

The worked example referenced throughout is [`apps/mail-adapter`](../../apps/mail-adapter),
the first-party email adapter that ships in this repo: a real component with its own
image, chart wiring and test suite, built to exactly the shape described here. It is a
worked example, not a framework you extend; yours is a separate service you own, and
nothing below needs a patch merged into this repo.

## 2. Bind an agent

A binding is four fields, written through the agent write path (`POST /agents`
or `PATCH /agents/{agent_id}`, both platform key over `X-API-Key`):

```json
{
  "channel": {
    "kind": "email",
    "address": "agent@example.com",
    "endpoint": "https://mail-adapter.internal:8080/curie",
    "adapter": "agentmail-sandbox"
  }
}
```

- **`kind`** names the adapter that owns the binding and must be a lowercase
  slug. It is also half the routing key: the worker resolves on the
  `(kind, address)` pair, so one address can be bound twice under two kinds.
- **`address`** is an opaque routing key matched on equality. For an
  unregistered kind the only rule is non-empty and no whitespace, so an email
  address, a queue name, or a tenant id all work.
- **`endpoint`** is where reply events are POSTed. It must be an absolute
  `http`/`https` URL with a host and no userinfo.
- **`adapter`** is a lowercase slug naming the egress identity whose secret
  authenticates those replies.

`endpoint` and `adapter` are both-or-neither: setting one without the other is
refused at write time. Both absent is legal, which is what lets a cutover bind
the agent first and PATCH the route in later. `slack` is the one kind exempt
from needing them, because its replies go through the worker's configured Slack
origin.

The route is write-only. Agent reads return exactly `{kind, address}`.

## 3. Get credentials

Two credentials, in opposite directions, both operator-issued.

**Inbound (your credential for calling the platform).** The operator mints a
`chn` token, platform key only:

```bash
curl -X POST "$CURIE_API_URL/channels/token" \
  -H "X-API-Key: $CURIE_PLATFORM_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"kind":"email","address":"agent@example.com","ttl_s":3600}'
```

Response is `{"token": "..."}`. `ttl_s` defaults to 3600 and is capped at
604800. The mint returns 404 if the pair is not bound, and 409 if a non-`slack`
binding has no reply route yet, so a half-configured route is caught at bind
time instead of mid-turn.

The token claims the binding row's id plus its current `generation`. Every
binding write bumps that generation unconditionally, including a re-assert of
identical values, so a rebind kills every outstanding token for it. Plan for
re-minting: treat a 401 from ingress as "ask the operator for a fresh token",
not as a bug.

**Outbound (the platform's credential for calling you).** The operator puts a
shared secret under your adapter slug in the worker's credential map:
`worker.adapterCredentials` in the chart (rendered into the chart Secret key
`adapterCredentials`, read by the worker as `CURIE_ADAPTER_CREDENTIALS`), or
`CURIE_ADAPTER_CREDENTIALS` directly in compose. Your endpoint gets the same
value by whatever secret mechanism you use, and verifies it on every request.

Egress fails closed. A missing endpoint, a missing adapter slug, or a slug with
no credential configured raises in the worker and sends nothing, rather than
delivering anonymously.

## 4. Inbound: posting a turn

```bash
curl -X POST "$CURIE_API_URL/channels/turns" \
  -H "X-API-Key: $CURIE_CHANNEL_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{
    "kind": "email",
    "address": "agent@example.com",
    "delivery_id": "msg_01H...",
    "conversation_id": "thr_01H...",
    "author": "someone@example.com",
    "text": "Subject line\n\nBody text",
    "reply_ref": "msg_01H..."
  }'
```

- **`delivery_id` must be stable and derived from your upstream message id.**
  The platform derives the turn's `event_id` from `(binding id, delivery_id)`,
  claims it, and keeps the claim as a permanent receipt. A retry of the same
  `delivery_id` therefore converges on the same answer forever instead of
  enqueuing a second turn and answering your correspondent twice.
- **`conversation_id`** is the thread key the platform keeps one live session
  per (an email thread id, a ticket id).
- **`reply_ref`** is opaque and adapter-minted. The platform never parses it and
  hands it back on every reply event. Email uses the upstream message id.
- **`kind` and `address` ride in the body, not the path**, because an address
  may contain `@`, `.`, `/`, `?` or `#`.
- Unknown fields are ignored, not rejected. In particular the body cannot name
  `endpoint` or `adapter`: the platform reads both off the binding row, so no
  token can point the platform's authenticated egress at a URL of its choosing.

Responses:

| Status | Meaning |
|---|---|
| 200 `{event_id, stream_id, duplicate}` | Accepted. `duplicate: false` means this request enqueued it. |
| 202 `{event_id, stream_id: null, duplicate: true}` | Another request holds the claim and has not enqueued yet. Come back. |
| 401 | Missing, malformed, expired, or stale-generation credential. One detail string for all of them, deliberately. |
| 404 | No agent bound to that `(kind, address)`. |
| 409 | The binding has no reply route configured. |
| 413 | Body over 256 KiB. The bound is enforced before parsing or authenticating. |
| 429 + `Retry-After` | This binding's new-delivery quota for the window is spent (64 per 60s by default). Retries of an already-claimed delivery do not count against it. |

**Retry transport failures only.** A response that arrived is final, duplicate
or not. `apps/mail-adapter` makes three attempts on a connection error and logs a
drop after that; it never re-posts after a status came back.

## 5. Outbound: serving the reply wire

The worker sends one JSON event per POST to your `endpoint`, with
`Content-Type: application/json` and `X-Curie-Adapter-Secret: <your secret>`.
There are four events, all carrying `version: "1.0"` and a `target`:

```json
{"kind": "email", "address": "agent@example.com",
 "conversation_id": "thr_01H...", "reply_ref": "msg_01H..."}
```

`conversation_id` is null for a message belonging to no conversation (a
policy-routed approval card), and `reply_ref` is null when the channel has no
addressable handle yet.

In the order of a typical turn:

1. **`turn.status`** adds `status`, a liveness caption. An empty string is the
   clear. Best-effort: a failure here never gates the turn, so a channel with no
   caption affordance can ignore it entirely.
2. **`reply.update`** is the turn's reply, edited in place where the channel
   supports it. It carries `text` (a streamed or final reply), or `message`
   (an `OutboundMessage`, whose `text` is always a complete usable fallback)
   plus `settled` (`{requested_by, decision, resolver, note}`) for an approval
   card being resolved or expired. Optional `nav` is `{label, command}`, the way
   back; absent means no affordance, so never render a dead one.
3. **`reply.post`** is a NEW platform-owned message (the approval card), with
   `message` and `requested_by`.
4. **`turn.completed`** adds `event_id` and `outcome`, one of `delivered`,
   `dropped`, `escalated`, `awaiting-approval`. This is the delivery trigger for
   a channel like email that sends once per turn rather than streaming.

Answer 2xx with a JSON body. The only field read off it is `ref`, an optional
adapter-minted handle for what you just posted; a channel with nothing editable
(email) answers `{}` and the kernel does not care.

Rules the transport enforces, so build to them:

- **Verify the secret fail-closed, before reading the body or touching state.**
  Anyone who can reach your service could otherwise forge a completion. Compare
  in constant time, and refuse when your own secret is unset.
- **Ack fast.** Any status at or above 400 is a delivery failure, and the turn
  is retried or eventually dead-lettered.
- **Never redirect.** A 3xx is treated as a delivery failure and is not
  followed, because following it would replay the egress secret at whatever
  origin the redirect named.
- **Keep the ack under 64 KiB.** Oversize is a delivery failure, not a
  truncation.
- **Delivery is at-least-once and duplicates carry the same `event_id`.**
  `turn.completed` may also arrive for a conversation you already consider
  finished, as a redelivery or a sweeper draining a record after an outage.
  Dedupe on `TurnCompleted.event_id`.

## 6. Operational patterns worth copying

From [`apps/mail-adapter`](../../apps/mail-adapter):

- **Prime on first start.** On startup it lists the inbox and marks every
  pre-existing message as seen before entering the poll loop, so bringing the
  adapter up does not replay a month of history as new turns.
- **Stage the cutover behind an ingress flag.** `ADAPTER_INGRESS_ENABLED=false`
  serves egress while sending nothing inbound. The platform side can then be
  bound, minted, and exercised end to end before any real correspondent traffic
  reaches it, and ingress is turned on as a separate step.
- **Dedupe in two layers.** A bounded in-memory set of replied `event_id`s is
  the fast path; the durable half is a marker line written into the outgoing
  message itself, which survives a restart. In-memory only is the conformance
  floor and will double-send after a restart.
- **Key per-conversation state on `conversation_id` for the reply *text* only, and
  take the reply *target* from the event.** Every reply event carries
  `target.reply_ref`, the opaque handle you sent on ingress, and the platform hands
  it back untouched. Keeping "the latest upstream message in this conversation" and
  replying to that looks equivalent and is not: a second message can arrive in the
  same thread before the first turn completes, and the first answer then lands on the
  wrong message. The text is safe to keep per conversation, because the platform runs
  one live session per conversation, so `reply.update` overwrites and `reply.post`
  appends within it.
- **Filtering inbound senders is not authenticating them.** Find out what your
  provider already drops or withholds by default, ask for that filtering explicitly in
  every request rather than inheriting it, so a changed provider default cannot widen
  your install silently, and keep a cheap check on the provider's own verdict behind
  that as defense in depth. An allow-list on a sender identifier the sender controls
  (an email `From` header, a display name, a caller ID) sits on top of all three and
  authenticates nobody: it is meaningful only where that identifier is independently
  enforced, which for email means the sending domain publishing an enforcing DMARC
  policy. [`apps/mail-adapter/README.md`](../../apps/mail-adapter/README.md) is the
  worked version, with AgentMail's parameter names and the key permissions it needs.

## 7. Conformance floor

An adapter must:

1. Send a `delivery_id` that is stable across retries of the same upstream
   message, and retry only transport failures.
2. Treat any response from ingress as final, including 202.
3. Verify `X-Curie-Adapter-Secret` on every egress request, in constant time,
   before any side effect, and refuse when unset.
4. Answer 2xx with a JSON body under 64 KiB, and never redirect.
5. Handle all four events, and tolerate ones it does not use.
6. Dedupe on `TurnCompleted.event_id`, at minimum in memory, and tolerate a
   completion arriving for a conversation it already considers finished.
7. Re-mint its `chn` token rather than treating a 401 as fatal, since any
   rebind invalidates it.

## Related

- [`docs/interfaces/channel-ingress/INTERFACE.md`](../interfaces/channel-ingress/INTERFACE.md): the seam
  this guide sits on.
- [`docs/interfaces/channel-interaction/INTERFACE.md`](../interfaces/channel-interaction/INTERFACE.md):
  the `OutboundMessage` contract carried by `reply.update` and `reply.post`.
- [`docs/approvals.md`](../approvals.md): what an approval card is and why
  `reply.post` and `settled` exist.

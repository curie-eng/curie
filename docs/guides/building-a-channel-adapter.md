# Building a channel adapter

For a complete production-shaped example, see
[`adapters/discord`](../../adapters/discord/) and
[`Connect one Curie agent to Discord and Slack`](discord-adapter.md).

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
platform database access. It may own a local durable delivery store, as the mail
adapter does, but that store must not become a route to platform credentials or
state. Binding is an operator action at deploy time.

The worked example referenced throughout is [`apps/mail-adapter`](../../apps/mail-adapter),
the first-party email adapter that ships in this repo: a real component with its own
image, chart wiring and test suite, built to exactly the shape described here. It is a
worked example, not a framework you extend; yours is a separate service you own, and
nothing below needs a patch merged into this repo.

## 2. Bind an agent

A binding is four fields. `POST /agents` writes the agent's first binding at
creation time (platform key over `X-API-Key`); every later add, move, or
remove goes through the binding subresource, `POST` / `PATCH` / `DELETE
/agents/{agent_id}/channels` (ADR-0118) -- `PATCH /agents/{agent_id}` no
longer accepts a `channel` key and 422s if you send one:

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

The route is write-only. Agent reads return a list of `{kind, address}`,
ordered by `(kind, address)` -- an agent may hold more than one binding.

## 3. Get credentials

Two credentials, in opposite directions, both operator-issued.

**Inbound (your credential for calling the platform).** The operator mints a
`chn` token with the platform key:

```bash
curl -X POST "$CURIE_API_URL/channels/token" \
  -H "X-API-Key: $CURIE_PLATFORM_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"kind":"email","address":"agent@example.com","ttl_s":3600}'
```

An adapter principal (`adp` credential, ADR-0154) can also mint a `chn` token
itself, but only for a binding in its own set; minting for any other binding is
403. The platform key issues an adapter principal at
`POST /approvals/principals/adapter`, and the adapter can rotate its own
credential afterward without going back to the platform key.

For the first-party mail adapter, `curie cluster channel-token <agent> --kind email --address <inbox>` mints, writes the Secret the adapter reads, and rolls it, so recovery is that one command rather than a curl plus a kubectl patch.

Response is `{"token": "..."}`. `ttl_s` defaults to 3600 and is capped at
604800. The mint returns 404 if the pair is not bound, and 409 if a non-`slack`
binding has no reply route yet, so a half-configured route is caught at bind
time instead of mid-turn.

The token claims the binding row's id plus the `generation` the mint stamps.
Every mint bumps that generation, and every write to the binding's route (a move,
or a re-assert of identical values) bumps it too, so a remint or a rebind kills
every outstanding token for the pair. Editing the binding's caller list (ADR 0175)
does not bump it, so an operator can add or remove a person without revoking your
token. Plan for re-minting: treat a 401 from ingress
as "ask the operator for a fresh token", not as a bug. The previous token is
already dead; installing the new one is what restores enqueue.

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
    "reply_ref": "msg_01H...",
    "attachments": [{"id": "msg_01H.../att_1", "name": "report.pdf"}]
  }'
```

- **`delivery_id` must be stable and derived from your upstream message id.**
  The platform derives the turn's `event_id` from `(binding id, delivery_id)`,
  claims it, and keeps the claim as a permanent receipt. A retry of the same
  `delivery_id` therefore converges on the same answer forever instead of
  enqueuing a second turn and answering your correspondent twice. This
  `delivery_id` is inbound and yours: it names your upstream message. The reply
  wire's `delivery_id` in section 5 is outbound and the platform's, and the two
  never refer to each other.
- **`conversation_id`** is the thread key the platform keeps one live session
  per (an email thread id, a ticket id).
- **`reply_ref`** is opaque and adapter-minted. The platform never parses it and
  hands it back on every reply event. Email uses the upstream message id.
- **`attachments`** is optional and defaults to `[]`. Each entry is a file
  reference, never the bytes: a required `id` and `name`, plus optional
  `mime_type` and `size_bytes`. The `id` is yours and opaque. A reference with
  no `id` or `name` is a 422. To give the agent the file, the worker calls
  `GET {endpoint}/attachments/{id}` on your binding's endpoint, with the same
  `X-Curie-Adapter-Secret` your reply endpoint verifies (ADR-0153). The `id` is
  percent-encoded with `/` kept. Answer 200 with the bytes. Any other status,
  including a 404 for an id you no longer hold or a redirect, refuses the
  turn's whole set, and the person is told the file could not be made
  available. The worker only fetches when the operator has turned the
  attachment lane on (`worker.attachments.enabled`), and the per-file cap
  applies.
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
| 403 `{"detail": "caller_not_allowed"}` | The binding's caller list (ADR 0175) does not admit the turn's `author`. Final: settle the delivery without a turn, never retry it, and send nothing back to the sender. A 403 with any other body is not this refusal; retry it. |
| 404 | No agent bound to that `(kind, address)`. |
| 409 | The binding has no reply route configured. |
| 413 | Body over 256 KiB. The bound is enforced before parsing or authenticating. |
| 429 + `Retry-After` | This binding's new-delivery quota for the window is spent (64 per 60s by default). Retries of an already-claimed delivery do not count against it. |

**Settle only on documented terminal success.** A 200 receipt, including
`duplicate: true`, is terminal. Transport ambiguity, 202, 429 (honor
`Retry-After`), 401, and 5xx are retryable and must retain the same stable
`delivery_id`; 401 additionally needs an operator to re-mint the scoped token.
Do not treat “a response arrived” as final: 202 explicitly says another claim is
not yet enqueued, and dropping that response loses the upstream message.

**A 403 with `{"detail": "caller_not_allowed"}` is final, for every adapter.** The
platform checked the binding's caller list after your token and before claiming
anything, and the author is not on it. Match the `detail` code exactly, not the
status alone: a 403 from a proxy or firewall in front of the API carries no such
code, is an infrastructure fault, and must stay retryable. Settle
the delivery the way you settle mail your own sender filter rejected, and do not
answer the sender: a polite refusal tells a stranger the bot exists. An adapter that
retries every error will retry this one forever. Run your own sender checks first,
as the mail adapter does, and send the sender you authenticated as `author`; for
email that is the bare address, lowercased.

The remaining 4xx statuses are terminal for the current configuration, but they
are not success: log the recovery instruction and retain enough durable evidence
to diagnose or deliberately replay after the binding/configuration is corrected.
Never mint a new `delivery_id` to escape an error, because that bypasses the
platform's idempotency receipt.

## 5. Outbound: serving the reply wire

The worker sends one JSON event per POST to your `endpoint`, with
`Content-Type: application/json` and `X-Curie-Adapter-Secret: <your secret>`.
There are four events, each carrying a `version` (see
[Wire versions](#wire-versions)) and a `target`:

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
   a channel like email that sends once per turn rather than streaming. A
   `dropped` completion you recorded no `reply.update` text for owes no
   message: the turn was never processed, so there is nothing of its own to
   send. Answering it anyway would post a new message from your side of the
   conversation -- on a sibling-limit drop (ADR-0168 decision 6), the next turn
   of the exchange the drop just ended, at your poll or retry speed.

### Wire versions

Every event above is version `"1.0"`. An adapter that decodes only 1.0 keeps
receiving exactly the bodies it received before progress existed: a 1.1 field
that is absent is left out of the body, never sent as null.

Version `"1.1"`
([ADR-0130](../adr/0130-deliberate-progress-is-bounded-durable-channel-state.md))
adds two optional fields to `reply.update` and `reply.post` and nothing else. A
body is `"1.1"` exactly when it carries `delivery_id`, and `turn.status` and
`turn.completed` are always `"1.0"`. The platform refuses to build a body that
breaks either rule.

- **`delivery_id`** is a canonical lowercase UUID the platform mints for one
  externally visible operation: one new post, or one edit of a message you
  posted. It is outbound, and it is not the inbound `delivery_id` of section 4:
  that one is yours and names your upstream message, this one is the
  platform's and names what it asks you to do. Use it as your idempotency key
  for that operation. A retry of an ambiguous attempt carries the same
  `delivery_id`, so answer it with the result of the first attempt (the same
  `ref` for a post) instead of posting again. Slack maps it to the post's
  `client_msg_id`, the way an approval card's id already is.
- **`progress`** is a rendering-free progress payload, and a body carrying it
  always carries `delivery_id`. It is one of two shapes, told apart by `kind`:
  - A **card**, `{"kind": "card", "state", "summary", "revision", "terminal"}`,
    is the one mutable task card of a logical turn chain. Its first revision
    arrives as a `reply.post`. Later revisions arrive as a `reply.update` whose
    `target.reply_ref` is the `ref` you acked for that post. `revision` counts
    from 1, and a higher revision supersedes a lower one, so tracking it lets
    you drop a stale redelivery. `terminal` is true exactly when `state` is
    `complete`, `failed` or `cancelled`: render the card closed and leave it in
    the conversation, because the final answer arrives separately and does not
    replace it.
  - A **milestone**, `{"kind": "milestone", "milestone", "summary", "ordinal"}`,
    is a durable progress reply and always arrives as a `reply.post`.
    `milestone` names why it interrupts: `evidence` (intake or material
    evidence acquired), `scope` (a material hypothesis or scope change) or
    `verification` (a verification result). `ordinal` is the chain's reserved
    milestone slot, 1 to 3; a chain never gets a fourth.

  `state` is one of `queued`, `investigating`, `awaiting-approval`,
  `preparing-workspace`, `testing`, `publishing`, `complete`, `failed` or
  `cancelled`. `summary` is a single line of 1 to 200 characters.

A progress body is never answer text. A `reply.update` carrying `progress`
carries no `text`, `message`, `settled` or `nav`, so check `progress` first and
never let one replace or clear the answer you are buffering. A `reply.post`
carrying `progress` still carries `message`, whose `text` is the plain-text
fallback for a channel with no card affordance, and that message never carries
an `interaction`: the approval card stays the only actionable message.

[`packages/channel-protocol/schema/reply-wire.corpus.json`](../../packages/channel-protocol/schema/reply-wire.corpus.json)
holds a body for every 1.0 form, exactly as the platform serializes it, a body
for every 1.1 form, and bodies the wire refuses, each with the reason. Decode
it in your adapter's tests. Each cross-field rule refuses with its own error
type (`reply_wire_version`, `progress_delivery_id`, `progress_not_an_answer`,
`progress_not_actionable`, `progress_terminal`), so match on the type, not on
message text.

What an adapter that decodes 1.1 must do with progress:

- **Branch on `progress` first**, on both events, before any answer or
  approval handling. A progress `reply.update` carries no answer fields, so an
  adapter that reads `text` first sees an empty answer and can blank the
  message it edits or clear the answer it is buffering.
- **Post idempotently.** A progress `reply.post` is a create, so key it on
  `delivery_id`: remember the `ref` you acked for it and answer a repeat with
  that `ref` instead of posting again. A redelivery after an ambiguous attempt
  carries the same `delivery_id`, never a new one.
- **Edit the card, never the answer.** A card `reply.update` names the card
  through `target.reply_ref`, the `ref` you acked for its first post. Edit that
  message and nothing else; it is not the placeholder and not the approval card.
- **Keep the card after completion.** A `terminal` card is rendered closed and
  left in the conversation. The final answer arrives separately and does not
  replace it.
- **Render the summary so it cannot notify anyone.** It is short, model-authored
  task state. Send it through whatever your channel offers that does not turn
  text into a mention or a broadcast.
- **Stay silent if your channel cannot show progress usefully.** A channel that
  sends one message per turn, as email does, may answer every progress body
  2xx with no `ref` and render nothing. Silence is conforming; changing the answer
  is not.

`channel_protocol.progress.progress_text` is the channel-neutral plain-text
rendering of a card or a milestone, for a channel with no card affordance. A
card edit carries no `message`, so that function, not a `message.text`, is
where an adapter gets the text for one; the adapters in this repository use it
for posts too, so every revision of a card reads alike.

The adapters in this repository handle progress this way. Slack renders the
card and milestones as Block Kit and maps `delivery_id` to the post's
`client_msg_id`; its rendering contract is in the
[worker README](../../apps/worker/README.md#how-the-slack-adapter-renders-progress).
[`adapters/discord`](../../adapters/discord/) posts and edits the plain-text
fallback and keeps each `delivery_id` with the message it posted in its SQLite
state. [`apps/mail-adapter`](../../apps/mail-adapter) is silent: a progress
body never touches the reply it buffers for the turn's email. Nothing on the
platform sends a 1.1 body yet.

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

- **Prime only on first start.** A new durable store records the current inbox
  floor before becoming ready, so bringing the adapter up does not replay a
  month of history. A restart opens the existing store, confirms the provider
  once without marking messages seen, and resumes pending/downtime work.
- **Stage the cutover behind an ingress flag.** `ADAPTER_INGRESS_ENABLED=false`
  serves egress while sending nothing inbound. The platform side can then be
  bound, minted, and exercised end to end before any real correspondent traffic
  reaches it, and ingress is turned on as a separate step.
- **Dedupe in two durable layers.** A local terminal receipt is the fast path;
  the independent witness is a marker written into the provider-visible thread.
  After an ambiguous accepted send, read that witness before resending. In-memory
  only will double-send after a restart.
- **Key reply text on `(conversation_id, reply_ref)`, and take the reply target
  from the event.** Every reply event carries
  `target.reply_ref`, the opaque handle you sent on ingress, and the platform hands
  it back untouched. Keeping "the latest upstream message in this conversation" and
  replying to that looks equivalent and is not: a second message can arrive in the
  same thread before the first turn completes, and the first answer then lands on the
  wrong message or clear the second turn's text. Persist ownership at the same
  granularity as the target.

**Verify sender authentication before accepting a turn or approval answer.**
An allowlist filters a claimed sender identifier and never authenticates it.
Require a positive authentication verdict that your own code verifies. A
provider label, the absence of a rejection label, or a provider supplied header
cannot supply that verdict. If no verifiable verdict is available, refuse the
message with a named reason before either platform endpoint is called. Keep
provider result exclusions explicit as an additional filter. For email,
AgentMail supplies no trusted positive aligned verdict, guarantees neither
header provenance nor stripping, and permits DMARC failure under `p=none`;
Curie refuses every message with `authentication_unverifiable`, including
allowlisted senders and approval answers. A wildcard needs a separate boot opt
in and never bypasses authentication. The provider evidence and complete gate
contract are in [`apps/mail-adapter/README.md`](../../apps/mail-adapter/README.md).

## 7. Conformance floor

An adapter must:

1. Send a `delivery_id` that is stable across retries of the same upstream
   message, and retain it across process restarts.
2. Settle it only on documented terminal success. Retry transport ambiguity,
   202, 429 (honoring `Retry-After`), 401 after operator re-mint, and 5xx without
   changing the `delivery_id`.
3. Verify `X-Curie-Adapter-Secret` on every egress request, in constant time,
   before any side effect, and refuse when unset.
4. Answer 2xx with a JSON body under 64 KiB, and never redirect.
5. Handle all four events, and tolerate ones it does not use.
6. Dedupe `TurnCompleted.event_id` durably and use an independent
   provider-visible witness before repeating an ambiguous side effect. Tolerate
   completion arriving for a conversation already considered finished.
7. Treat 401 as an operator credential-rotation condition: retain the same
   delivery, surface the recovery action, and resume it after the `chn` token is
   re-minted. A channel adapter must not acquire a platform key merely to mint
   its own token.
8. Decode every 1.0 body. An adapter that also decodes 1.1 must treat
   `delivery_id` as the idempotency key of the post or edit it names, answer a
   repeated `delivery_id` with the first attempt's result, and handle
   `progress` before any answer path so a progress body never changes answer
   text.

### Running the floor

The floor is executable. `channel_protocol.conformance`
(`packages/channel-protocol/src/channel_protocol/conformance.py`) ships fourteen
checks covering ingress normalization, reply delivery, edit and buffered streaming,
attachments, approval cards, progress, and error mapping. Wrap your adapter in a
`ChannelAdapterSubject` and declare its `Capabilities`: kind, streaming mode (edit,
buffered, silent or relay), ingress, authenticates, refuses_foreign_targets and
serves_attachments. Then run every check
`packages/channel-protocol/src/channel_protocol/conformance.py::applicable_checks`
returns for that declaration. A check your declaration does not cover is not
returned, never skipped.

```python
for check in applicable_checks(subject.capabilities):
    await check.run(subject, CheckContext(validate_turn=my_validator))
```

In this repository, the suite in `tests/channel_conformance/` runs every check
against Discord, the mail adapter, GitHub and the worker's `HttpReplyAdapter`, each
driven through the worker's real `ReplySinkRouter`. Adding an adapter is one `_Entry`
(its declared `Capabilities` plus a factory that opens the subject) in the `_ENTRIES`
table of `tests/channel_conformance/subjects.py`, which feeds the `REGISTRY` the
tests parametrize over. If you build a third-party relay, drive your egress through
the worker's `HttpReplyAdapter` so header names and statuses are proven across the
real wire.

## Related

- [`docs/interfaces/channel-ingress/INTERFACE.md`](../interfaces/channel-ingress/INTERFACE.md): the seam
  this guide sits on.
- [`docs/interfaces/channel-interaction/INTERFACE.md`](../interfaces/channel-interaction/INTERFACE.md):
  the `OutboundMessage` contract carried by `reply.update` and `reply.post`,
  and the progress card and milestone the 1.1 `progress` field carries.
- [ADR-0130](../adr/0130-deliberate-progress-is-bounded-durable-channel-state.md):
  why progress is bounded durable channel state and why its delivery has a
  stable identity.
- [`docs/approvals.md`](../approvals.md): what an approval card is and why
  `reply.post` and `settled` exist.

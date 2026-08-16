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

The worked example referenced throughout is the AgentMail reference adapter, a
single-file email adapter built during ADR-0096 phase 2. It is a spike, not a
shipped component, but its shape is the shape being described here.

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
or not. The reference adapter retries three times on a connection error and
logs a drop after that; it never re-posts after a status came back.

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

From the AgentMail reference adapter:

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
- **Keep per-conversation state keyed by `conversation_id`**, holding the
  upstream message to reply to and the latest reply text, so `reply.update`
  overwrites and `reply.post` appends.

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
7. Never treat a 401 as fatal: hold the delivery, stay alive, surface a loud
   stale credential signal so the operator notices, and resume once the
   operator supplies a replacement token. An adapter holds no platform key and
   must never try to mint its own replacement (see section 3); the mint stays
   platform key only.

## 8. Building tooling: the binding profile, `curie adapter`, and the conformance kit

Everything below reads or writes an `adapter.yaml` file. Scaffold one,
validate it, bind it to an agent, mint a token against it, smoke test the
deployed adapter, then run the full conformance kit before calling the
adapter done.

### The binding profile

`adapter.yaml` is a **per install binding file**: the channel kind an adapter
owns, the address shape it accepts, the endpoint this install's worker POSTs
reply events to, and the names of the credentials involved. It is not the
install agnostic composition manifest ADR-0096 decision 2 describes (image
reference, config and secret schema, platform performed composition). That
document is separate and later; nothing here pre-empts it.

Fields:

- `version`: the profile format version, checked before anything else (see
  Compatibility below).
- `kind`: the channel kind this adapter owns, a lowercase slug.
- `endpoint`: the reply route. Optional here; `bind`, `token`, and
  `smoke-test` need a concrete one and either read it off the profile or take
  an override.
- `address`: `description`, `pattern` (a regex that has to compile in both
  Python `re` and the Rust `regex` crate, so no lookaround and no
  backreferences), and `example`.
- `credentials`: `egress` (a slug, a suggestion only, see below),
  `egress_secret_env` and `ingress_token_env` (documentation of what the
  adapter itself reads; Curie never resolves either name).
- `conformance`: `wire_version`, the reply wire this adapter speaks.

The schema is closed (`additionalProperties: false`), so a typo'd key is
refused rather than silently ignored.

### The address pattern's regex dialect

`address.pattern` has to compile under two engines: Python `re`, used by the
Python conformance kit, and the Rust `regex` crate, used by `curie adapter
validate`. The Rust crate is the narrower of the two, so it is the one that
decides what is portable. `curie adapter validate` refuses a pattern the
crate cannot compile even when Python `re` accepts it, so an author never
ends up with a pattern that behaves differently on the two paths.

Refused constructs:

- Lookahead and lookbehind, positive or negative: `(?=`, `(?!`, `(?<=`, `(?<!`
- Named backreferences: `(?P=`
- Atomic groups: `(?>`
- Conditional groups: `(?(`
- Inline comment groups: `(?#`
- The Python only ASCII flag: `(?a`
- The Python only `\Z` end of string anchor (the crate spells this `\z`)
- The Python only `\N` named character escape, which the crate has no
  equivalent for

### Compatibility policy

A third party commits an `adapter.yaml` you cannot force it to upgrade. Both
`curie adapter validate` and the Python kit read the raw `version` key and
check it before touching the schema. A version they do not accept is refused
with a message naming both versions, for example: this curie understands
adapter profile 1.0; the file declares 1.1. That check has to run first: a
1.1 file checked against the closed schema trips `additionalProperties:
false`, and the operator reads "additional property not allowed" instead of
the version they actually have to act on. A missing `version` key gets the
same refusal, never a default.

`version` also has to be spelled canonically: plain digits, no leading zero.
`01.0` or `1.00` are refused as malformed before the acceptance check even
runs, rather than parsed as `1.0`.

Acceptance is same major, less or equal minor: a 1.0 build refuses 1.1 (it
cannot know the new field is optional) and refuses 2.0 outright; a 1.1 build
still accepts a 1.0 file.

On a version you do not recognize: upgrade `curie`, or pin the older profile
version. Never delete the `version` key to work around the refusal.

The change class table, so an adapter author can predict the bump before a
change lands:

| Change | Bump | Why |
|---|---|---|
| Add an optional property | minor, 1.0 to 1.1 | The schema is closed, so a consumer on 1.0 rejects the new payload. |
| Add a required property; remove or rename one; change a type; tighten a pattern; make an optional property required | major, 1.0 to 2.0 | Invalidates a file a conforming author previously wrote. |
| Loosen a pattern, widen an enum, relax a bound | minor | Same closed schema reasoning as an added optional property. |
| Edit a description, a title, or a comment | none | No shape change. |

### `curie adapter` verbs

All five read or write the profile at `--file` (default `adapter.yaml`).

- **`scaffold <name>`** writes one `adapter.yaml` under `<dir>/<name>/`
  (`--dir` defaults to the current directory) from `--kind`, `--address`,
  `--endpoint`, and `--adapter`. The generated `address.pattern` matches
  exactly the address it was scaffolded for; widen it by hand to the real
  shape of the channel's addresses.
- **`validate`** checks the version, then the schema, then the one rule the
  schema cannot express: `address.pattern` has to compile with the Rust
  `regex` crate as well as Python `re`. Pass `--address` to also check that a
  concrete address matches the declared shape.
- **`bind <agent>`** writes the agent's four field channel route. It takes an
  explicit `--address` and an explicit `--adapter-slug`, both operator
  supplied, never taken from the profile: `address.example` is authoring
  documentation, not an operator confirmed value, and the profile's
  `credentials.egress` is only a suggestion. Requires `--yes` to actually
  write.
- **`token`** mints a `chn` token for one concrete `(kind, address)` pair. It
  also takes an explicit `--address`. `--ttl-s` defaults to 3600, and the API
  accepts 1 to 604800.
- **`smoke-test`** probes a deployed adapter from the outside: does it accept
  the egress secret you supply, does it refuse a wrong one, are the route
  fields present for this pair. It also takes an explicit `--address`, plus
  `--secret-file` or `--secret-stdin` for the egress secret. It is a narrower
  check than the conformance kit, not a replacement for it.

`--address` and `--adapter-slug` are operator owned on all three verbs for the
same reason: the worker's credential map is indexed by the route's adapter
slug, so a profile that named the wrong slug, or a stale `address.example`,
would point a real credential at the wrong destination. That is a security
boundary, so the CLI asks a human to confirm it rather than trusting the
file.

`smoke-test` also never reads the egress secret from an environment variable
the profile names. `credentials.egress_secret_env` documents what the adapter
itself reads; Curie never resolves that name. The secret has to come from
`--secret-file` or `--secret-stdin`, supplied at the command line, or the
command refuses, because a hostile profile could otherwise name any
environment variable on the operator's box and have its value read and sent.

### The conformance kit

`channel-protocol[conformance]` ships two front doors onto the same seven
rule floor:

- **The importable runner.** `from channel_protocol.conformance import
  run_floor`. Call it with an `AdapterUnderTest`, an `IngressDriver`, and a
  side effect probe, and assert `report.automated_floor == "pass"` in your own
  test suite.
- **The console script.** `curie-adapter-conformance --profile adapter.yaml
  --endpoint <url> --secret-file <path>` (or `--secret-stdin`), the command a
  vendor runs in its own repo and quotes in its own README. Add `--driver
  module:attr` naming a zero argument factory for an `IngressDriver`, or rules
  1, 2, and 7 and clause 3b report `not_run`. `--json` emits the report as
  JSON; `--mode diagnostic` reports partial results while an adapter is still
  being built and never reaches a passing verdict.

The exit code follows `automated_floor`: nothing short of a full strict pass
exits 0, so a README cannot claim conformance off a partial run.

### The ingress driver

Rules 1, 2, and 7, plus clause 3b, need something only you can provide: a way
to point your adapter at the kit's ingress, inject an upstream message under a
known id, tell whether your adapter is done with that delivery, and restart
your adapter with one credential swapped. You give the kit that access by
implementing `IngressDriver` (`channel_protocol.conformance.driver`) and
passing it in, or naming a zero argument factory for one on `--driver`.

Every method on `IngressDriver` runs under a hard bound (five seconds by
default). The kit calls each one on its own thread and walks away the moment
the bound expires; it never waits on a callback that has already shown it
does not answer. A callback that misses the bound does not hang the run and
does not pass silently either: it turns into a named clause FAILURE that
tells you the defect is in your driver, not in your adapter, so you are not
left debugging the wrong code.

The methods, and what each has to guarantee:

- **`start(*, ingress_url, token)`** points your already running adapter at
  the kit's ingress. Configuration only: the kit refuses to check an adapter
  that has to be edited to be checked, because that is not the adapter your
  operator runs.
- **`reserve() -> UpstreamIdentity`** declares the `delivery_id` the next
  `release` will carry, before that message exists. This is the load bearing
  method. The kit correlates an observed POST to a stimulus by matching the
  wire's `delivery_id` against what `reserve` returned, because that is the
  only correlation an adapter running in another process or another language
  can satisfy; it never reads your private in process state. If your
  `reserve` returns an id your adapter does not actually send, rules 1 and 2
  fail naming the mismatch, the same as a genuinely nonconformant adapter
  would.
- **`release(identity)`** delivers the message `reserve` declared. Injection
  is deliberately the second step: rule 2 has to arm the ingress to answer
  202 for that one identity before the identity can reach it, and a single
  call that both declared and injected would leave a window where a
  different, unrelated delivery in flight consumes that response instead.
- **`settled(identity) -> bool`** answers whether every attempt at this
  delivery has been retired: none in flight, and none still scheduled.
  "Retired" is that precise claim, not a description of the active queue
  being empty; a delivery you pulled off the queue but left a retry sitting
  on a timer is not retired, and answering `True` for it is the naive
  implementation this method exists to catch. The kit does not take your
  `True` on faith. Rule 2's finality check waits on it rather than a fixed
  clock, because no timer survives a retry schedule patient enough to outlast
  it, but once you claim retired the kit keeps watching the wire for a
  bounded grace period afterward, and any post carrying that delivery id in
  that window is reported as a failure of your claim, named as such, not a
  quiet pass. Implementing `settled` as a short sleep will fail here, and
  that is deliberate: only your adapter's own retry state can answer this
  honestly. Answer `False` for as long as your adapter might still retry; a
  driver that never reports `True` leaves rule 2 with no finality evidence to
  judge.
- **`restart(*, egress_secret=..., token=...)`** restarts your adapter,
  optionally replacing one credential. Both arguments default to the
  unchanged sentinel, `Ellipsis`, because `None` is itself meaningful for
  each: clause 3b calls `restart(egress_secret=None)` to prove your adapter
  refuses to serve with no egress secret of its own, and rule 7 calls
  `restart(token=<fresh>)` to hand you an operator-issued replacement ingress
  token without disturbing the egress secret.
- **`stop()`** stops your adapter, called once at the end of a run.

Without a driver, rules 1, 2, and 7 and clause 3b all report `not_run`, and
`not_run` is nonconformant: the kit never reads missing evidence as a pass.
Supply the driver to get a real verdict on your delivery_id stability, your
handling of a final 202, and your stale credential recovery.

### Verdict semantics

`automated_floor` is `pass` or `fail` over the automatable clauses only. Two
clauses are outside that domain:

- **3c**, that the egress secret is compared in constant time. No HTTP status
  carries the answer, because two rejections that take different amounts of
  wall time are indistinguishable from two rejections on a loaded box.
- **7c**, that a stale ingress credential is signalled loudly. That signal is
  for the adapter's own operator, in a log, a metric, or a page, so it never
  crosses the wire the kit observes.

Both are listed in `manual_review_required`, with why no check decides them
and how to review them by hand. A `pass` does not assert either one: read the
adapter's secret check yourself for 3c, and arm a 401 and confirm the signal
for 7c.

Missing evidence is never success. Any automatable clause that is not
`pass`, including `not_run` from an unsupplied ingress driver or an adapter
with no side effect probe, makes `automated_floor` fail. There is no
`partial` status and no `skipped` status.

## Related

- [`docs/interfaces/channel-ingress/INTERFACE.md`](../interfaces/channel-ingress/INTERFACE.md): the seam
  this guide sits on.
- [`docs/interfaces/channel-interaction/INTERFACE.md`](../interfaces/channel-interaction/INTERFACE.md):
  the `OutboundMessage` contract carried by `reply.update` and `reply.post`.
- [`docs/approvals.md`](../approvals.md): what an approval card is and why
  `reply.post` and `settled` exist.

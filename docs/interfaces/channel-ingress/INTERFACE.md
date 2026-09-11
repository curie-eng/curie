---
seam: Channel / ingress
kind: SOFT
impls: 3 production channels (Slack, Discord, email) plus CLI stub
grade: B-
vision_row: Communication
epics:
  - "#7"
  - "#19"
  - "#27"
  - "#38"
  - "#1515"
order: 4
---
# INTERFACE: Channel / ingress

> Part of the Curie swappable-seam catalog — see the [seam index](../../interfaces.md).
<!-- BEGIN GENERATED: header (curie dev docs-lint) -->
> **Kind:** SOFT &nbsp;·&nbsp; **Implementations today:** 3 production channels (Slack, Discord, email) plus CLI stub &nbsp;·&nbsp; **Swap-readiness grade:** B-
<!-- END GENERATED: header -->

**Kind legend:** CLEAN = a real `Protocol`/typed port class · SOFT = swap via env/URL/prefix/wire, no code interface · NONE = not built yet.

## The black line

The line that makes the communication channel swappable is the pair of contracts at
the two ends of the run: the ingress payload the dispatcher enqueues (`QueuedTurn`) and
the egress port the kernel writes replies through (`ReplySink`). Everything between them —
routing, concurrency, sandboxing — is opinionated core and channel-agnostic. Since #7 and
#19 the ingress payload and the per-turn reply routing are channel-neutral, so this is no
longer the least-clean seam by its wire contract, and #1459 took the Slack shape off the
binding surface too; the remaining vendor shape is on the egress semantics
(edit-in-place streaming, plus posting and settling platform-owned cards). Three
implementations today, Slack, Discord, and email, confirm that the seam boundary is the
HTTP wire rather than only the in-process port: `adapters/discord` and
`apps/mail-adapter` are services outside the core that neither construct a `QueuedTurn`
nor implement `ReplySink`.

## Current contract

A channel joins at one of two boundaries: in process, producing the ingress payload and
satisfying the egress Protocol, or out of process over the HTTP wire.

- **Wire** — the out-of-process boundary, and the one a channel the core has never heard
  of uses. Ingress is `POST /channels/turns` under a binding-scoped `chn` token; egress is
  the same four reply events POSTed to the binding's `endpoint` with an
  `X-Curie-Adapter-Secret` header, addressed by `target.reply_ref`, an opaque
  adapter-minted handle the platform stores and hands back untouched.
  [`docs/guides/building-a-channel-adapter.md`](../../guides/building-a-channel-adapter.md)
  is normative for this boundary, down to the conformance floor an adapter must meet;
  `apps/mail-adapter` is the worked example.
- **Ingress** — `QueuedTurn` (`packages/aci-protocol/src/aci_protocol/turn.py::QueuedTurn`),
  a Pydantic model in the frozen ACI package with channel-neutral fields: `event_id`
  (idempotency key), `conversation_id` (the conversation/thread key routing keeps one live
  session per), `author`, `text`, `received_at`, `source` (`QueuedTurn.source`, what
  started the turn: a person's message or a job), `attachments`
  (`QueuedTurn.attachments`, a list of `Attachment` file REFERENCES — an
  adapter-scoped opaque `id` plus a `name`, never bytes and deliberately never a
  url, so only the producing adapter can resolve one), and `reply_handle` — a `ReplyHandle`
  (`packages/aci-protocol/src/aci_protocol/turn.py::ReplyHandle`) carrying the required
  `kind` and `channel` routing pair, required nullable `placeholder`, an optional
  per-turn `endpoint`, and optional `adapter` (`ReplyHandle.adapter`, the egress
  adapter identity whose credential authenticates the reply). The Slack adapter
  currently supplies the pre-posted reply ts that the worker edits in place. The
  dispatcher serializes the turn to a single Stream field via `to_stream_fields`
  (`apps/dispatcher/src/curie_dispatcher/queue.py::to_stream_fields`), keyed by
  `STREAM_PAYLOAD_FIELD = "payload"`. That key is not the dispatcher's: it is frozen-package
  contract in
  `packages/aci-protocol/src/aci_protocol/service_config.py::STREAM_PAYLOAD_FIELD`, imported
  by every producer and consumer of the stream (the dispatcher, the API resume queue in
  `apps/api/src/curie_api/resumequeue.py`, and the Rust CLI through the generated constant in
  `cli/src/queue.rs`), so a second ingress adopts the package constant rather than copying
  the literal.
  For the Slack adapter, `event_id` is the Slack event id, `conversation_id` is the thread
  ts, `author` is the Slack user id, and `reply_handle` carries the `slack` kind, Slack
  channel, and placeholder ts.
- **Egress** — the `ReplySink` Protocol (`apps/worker/src/curie_worker/reply_sink.py::ReplySink`),
  whose one method is `async def emit(self, event, *, route, best_effort_unreachable=False)`
  (`apps/worker/src/curie_worker/reply_sink.py::ReplySink.emit`) — four versioned neutral
  events (`turn.status`, `reply.update`, `reply.post`, `turn.completed`) over a
  worker-local `TargetRoute`. One verb, but a second channel still supports three shapes,
  not one:
  - `reply.update` (`packages/channel-protocol/src/channel_protocol/reply.py::ReplyUpdate`)
    carries either a streamed or final `text`, edited in place over the ingress placeholder,
    or a `message` plus `settled` pair that turns an already-posted platform message into
    its settled form (an approval card that expired or was resolved). It also carries the
    agent's hub affordances (`nav`).
  - `reply.post` (`packages/channel-protocol/src/channel_protocol/reply.py::ReplyPost`)
    posts a NEW platform-owned message, such as the approval card, and is acked with its
    ref. Both it and the settling form of `reply.update` carry a channel-neutral
    `OutboundMessage`
    (`packages/channel-protocol/src/channel_protocol/models.py::OutboundMessage`, ADR-0020)
    plus, for the settled form, a `SettledOutcome`
    (`packages/channel-protocol/src/channel_protocol/reply.py::SettledOutcome`) carrying the
    outcome semantically (who asked, what was decided, by whom, with what note), so no
    channel-native markup crosses the seam.
  - `turn.status` (`packages/channel-protocol/src/channel_protocol/reply.py::TurnStatus`) is
    the best-effort thread status caption, a no-op on a channel with no equivalent, and an
    empty status is the clear.

  Slack dialect and widget shape stay BELOW that port, in `SlackReplyAdapter`
  (`apps/worker/src/curie_worker/slack_sink.py::SlackReplyAdapter`) — its edit-in-place
  `chat.update`, its `chat.postMessage`, and its assistant-thread status — plus `to_mrkdwn`
  (`apps/worker/src/curie_worker/mrkdwn.py::to_mrkdwn`) and the Block Kit rendering in
  `render` (`apps/worker/src/curie_worker/blocks.py::render`) and `approval_card`
  (`apps/worker/src/curie_worker/blocks.py::approval_card`).
- **Binding** — a channel resolves to a deployment by exact `(kind, address)` equality in
  `BindingResolver.resolve` (`apps/worker/src/curie_worker/binding.py::BindingResolver.resolve`).
  Both halves are required, with no address-only fallback, and uniqueness is on the same
  pair. The binding is written as a neutral `{kind, address}` pair (ADR-0096, #1459), so a
  second channel binds its agent without a schema change and the same address may belong to
  different adapter kinds.

## Implementations today

Three first-party production channels, plus a CLI transport stub. Slack ingress
is `apps/dispatcher` (Bolt / Socket Mode); Slack egress is `SlackReplyAdapter`
(`apps/worker/src/curie_worker/slack_sink.py::SlackReplyAdapter`) on the Slack
Web API. Discord (`adapters/discord`) and email (`apps/mail-adapter`) join on
the authenticated HTTP edge: they neither construct a `QueuedTurn` nor implement
`ReplySink`; the worker delivers through `HttpReplyAdapter`
(`apps/worker/src/curie_worker/reply_sink.py::HttpReplyAdapter`) to the
binding's server-controlled endpoint. The swap proof that the protocol (not just
the service) is the seam: the Rust CLI mints the exact
`QueuedTurn` wire payload with the same channel-neutral fields
(`cli/src/queue.rs`) and drives the whole deployed system with zero Slack contact
via `curie local message` / `cluster message` (`cli/src/chat.rs`, `cli/src/message.rs`).

## Adapter admission and the drop contract (Slack)

This clarifies *this* seam's ingress contract for the Slack dispatcher adapter (#2006). It
is a Slack-adapter statement, not a vocabulary other adapters are asked to adopt.

**The rule.** The adapter normalizes what it receives; routing decides what is relevant.
Anything decidable from the routed payload belongs at the routing seam
(`BindingResolver.resolve` above), so what stays in `apps/dispatcher` is normalization plus
the few refusals that seam structurally cannot make. Every drop the adapter's own code
makes is an enumerated member of
`apps/dispatcher/src/curie_dispatcher/relevance.py::DropReason`, carries a documented
rationale in `apps/dispatcher/src/curie_dispatcher/relevance.py::DROP_RATIONALES`, and is
emitted as exactly one INFO record by
`apps/dispatcher/src/curie_dispatcher/relevance.py::drop`. There are no silent drops inside
the adapter: a bare `return None` on the ingest path is the defect #2006 closed, not a
style preference. The module is the system of record for this table, and the dispatcher's
tests assert the two sets equal in both directions.

| `DropReason` | Documented rationale |
|---|---|
| `MALFORMED_ENVELOPE` | The delivery omits a field the turn is minted from -- the event id on an event, the channel, the thread key, or the interaction identity on a click -- and refusing it before the idempotency claim beats raising after it, because Bolt has already acked and would swallow the exception while the claim survived for work that never happened. |
| `UNSUBSCRIBED_LANE` | The delivered envelope is outside the adapter's declared subscription surface — `apps/dispatcher/slack-app-manifest.yaml` subscribes to `app_mention` and `message.im` only, so a message event on any other channel type cannot legitimately arrive, and refusing it is envelope validation rather than a relevance judgement. |
| `BOT_AUTHORED_THREAD_REPLY` | Loop guard across installations: Curie's own replies and placeholders are always threaded, so admitting a bot-authored mention that carries a thread timestamp would let two Curie installations in one workspace mention-loop each other indefinitely, which Bolt's self filter cannot stop because the two bot identities differ. An exact operator-trusted sender/channel pair may bypass this refusal; self-event suppression remains mandatory. |
| `NON_CONTENT_SUBTYPE` | The subtype marks something other than new user content: an edit, a delete, a tombstone, a body redacted by Enterprise Key Management, or an assistant thread-start marker. |
| `DUPLICATE_DELIVERY` | Slack redelivered a delivery whose idempotency key was already claimed, so processing it again would post a second placeholder and mint a second turn for one message. |
| `NO_ACTION_IN_PAYLOAD` | A block-action payload carrying no actions names no command and addresses nothing, so there is no turn to mint from it. |
| `EMPTY_ACTION_COMMAND` | A clicked button carrying neither a value nor a usable action id names no command, so the turn text would be empty. |
| `UNADDRESSABLE_ACTION` | An App Home or modal click carries no channel and no message, so there is no thread in which a reply could be delivered. |

The last three sit on the Block Kit click lane, which is a real ingest lane:
`apps/dispatcher/src/curie_dispatcher/handlers.py::process_action` mints a `QueuedTurn`
exactly as `apps/dispatcher/src/curie_dispatcher/handlers.py::process_event` does, so a
dropped click is inbound loss of the same class. Of the three, only `UNADDRESSABLE_ACTION`
is a disposition Bolt actually delivers today: an App Home or modal click really does
arrive with no channel and no message. `NO_ACTION_IN_PAYLOAD` and `EMPTY_ACTION_COMMAND`
are **defensive guards on that lane that the current matcher does not reach**. The
catch-all registration in
`apps/dispatcher/src/curie_dispatcher/handlers.py::register_handlers` is
`@app.action(re.compile(r".+"))`, and `slack_bolt` resolves the clicked action inside its
own matcher before invoking the listener: a payload with an empty `actions` list raises
there rather than arriving at `process_action`, and an empty `action_id` never matches
`.+` at all. The dispatcher's tests therefore exercise those two rows directly instead of
through Bolt. They are kept, not deleted, for three reasons: they are one comparison each;
the matcher's payload handling is a `slack_bolt` implementation detail rather than a
contract this seam holds, so a release that starts delivering either shape must meet a
logged refusal; and a silent `return None` on that path is the defect #2006 removes, which
is exactly what a deleted guard would restore. An approval-card click is deliberately
absent — it is *handled* by the dedicated approval listener, not dropped.

- **The framework's admission boundary sits above the adapter, and is documented rather
  than re-implemented.** Three of Bolt's own mechanisms admit or refuse an event before any
  listener in `apps/dispatcher/src/curie_dispatcher/handlers.py` runs, so none of them can
  be a `DropReason`. `slack_bolt`'s built-in `IgnoringSelfEvents` middleware compares the
  incoming identity against the authorized bot identity and acks a self-authored event
  without invoking a listener, logging only at DEBUG — so at production log levels that
  drop is invisible. Bolt's authorization middleware refuses an event whose token or team
  does not authorize. Bolt's listener matchers match on event *type* only and are
  subtype-blind, so an unregistered type is acked and dropped, and every `message.im`
  subtype does reach the adapter's listener. These are Bolt's decisions on Bolt's terms and
  the dispatcher deliberately does not duplicate them. `IgnoringSelfEvents` is the real
  self-event loop guard, which is why the adapter no longer carries a blanket
  bot-authorship filter of its own: that filter was redundant with the middleware *and*
  harmful, discarding legitimate incoming-webhook, Workflow Builder and other-app posts.
- **`UNSUBSCRIBED_LANE` is envelope validation, not a relevance decision.** The Slack app
  subscribes to exactly `app_mention` and `message.im`
  (`apps/dispatcher/slack-app-manifest.yaml`), so refusing another lane is the adapter
  asserting that the delivered envelope matches its declared subscription — the same family
  as refusing a payload with no event id, not a judgement that channel chatter is
  uninteresting. It also could not move downstream even if we wanted it to: `QueuedTurn`
  carries no Slack lane and no subtype, and `BindingResolver.resolve` receives only the
  `(kind, channel)` pair, so the routing seam has no field on which to tell a mention from
  ordinary chatter and would simply mint a turn.
- **Subtype handling is open-world.**
  `apps/dispatcher/src/curie_dispatcher/relevance.py::NON_CONTENT_SUBTYPES` is a small
  closed denylist of things that are structurally not new user content; everything else,
  including subtypes Slack ships after this was written, is admitted and left to routing.
  The inverse rule this replaced — refuse *any* subtype — swallowed `file_share` (a person
  uploading a file with a comment) and `thread_broadcast` (a person's thread reply), both
  real user content. The open default is bounded by the manifest: with only `app_mention`
  and `message.im` subscribed, channel-lane noise (joins, leaves, topic and purpose
  changes, pins) cannot reach these lanes in production, so the denylist stays small rather
  than becoming a speculative catalogue of every subtype Slack documents.
- **Bot authorship is a mention-lane rule, and its cost is accepted.** A foreign bot's
  *root* `@`-mention is admitted — the alert-app case this work exists for, whose body
  typically arrives as Block Kit and is normalized by
  `apps/dispatcher/src/curie_dispatcher/inbound_text.py::derive_text` rather than read off
  an empty top-level `text`. A bot-authored mention carrying `thread_ts` is refused as
  `BOT_AUTHORED_THREAD_REPLY` for the cross-installation loop above, unless the event's
  exact channel/bot pair is in `CURIE_SLACK_THREADED_BOT_ALLOWLIST`; on the DM lane bot
  authorship is not consulted at all. The allowlist defaults to empty and malformed
  entries fail dispatcher configuration. Only trust a dedicated sender that does not
  automatically respond to Curie: an allowlisted second Curie installation could loop.
  Bolt still drops this installation's own bot even when it is listed. Exact-pair,
  cross-product, self-bot, content-filter and duplicate outcomes are exercised through
  Bolt and real Valkey in `apps/dispatcher/tests/test_threaded_bot_allowlist.py`.
  See `apps/dispatcher/README.md` for configuration and the live-proof boundary.

  Its known consequence, stated here rather than left to be found: a bot-authored event
  normally carries no human `user`, so the turn is queued with an empty `author` while its
  text becomes a model turn like any other. That is not a new prompt-injection primitive —
  the same text was already reachable from any member of the workspace — but it does widen
  the set of *automated* principals that can drive the model, a compromised third-party app
  or the holder of an incoming-webhook URL among them, and routing cannot tell one from a
  person afterwards because the adapter carries neither `bot_id` nor the message subtype
  onto the queue. Carrying machine provenance onto the turn is deliberately deferred, not
  overlooked: `QueuedTurn`
  (`packages/aci-protocol/src/aci_protocol/turn.py::QueuedTurn`) cannot gain a field
  without a protocol-version bump
  (`packages/aci-protocol/src/aci_protocol/version.py::PROTOCOL_VERSION`) and the matching
  `packages/aci-protocol/schema/wire.lock` regeneration, which is a change to the frozen
  package rather than to this adapter.
- **Sibling ingress paths — audited, unchanged.** The first-party HTTP ingress paths
  (`apps/api/src/curie_api/routers/hooks.py`,
  `apps/api/src/curie_api/routers/channels.py`) already refuse explicitly: every refusal is
  an HTTP status returned to the caller and every duplicate is a flagged response, so they
  need no equivalent vocabulary today, and extending the enumerated reasons to them if they
  ever grow relevance logic is deliberately deferred.

## Known leakage

Two ends and the binding surface were cleaned; what remains is egress semantics and
incomplete adapter coverage and conformance.

- **Fixed (#7).** The ingress field names were Slack's (`slack_event_id`, `thread_ts`,
  `placeholder_ts`); the payload was promoted into `packages/aci-protocol` as `QueuedTurn`
  with channel-neutral names.
- **Fixed (#19).** The reply base URL was worker-global; per-turn reply routing now rides
  `ReplyHandle.endpoint`, so a real Slack workspace and a no-Slack CLI stub can coexist on
  one deployment. `WorkerConfig.slack_api_base_url` (`apps/worker/src/curie_worker/config.py::WorkerConfig`)
  is now only the default when a turn sets no `endpoint`, fed to `SlackReplyAdapter`
  (`apps/worker/src/curie_worker/slack_sink.py::SlackReplyAdapter.__init__`) — which
  also makes it the only TRUSTED Slack origin, so a per-turn endpoint elsewhere is
  refused rather than handed the platform bot token.
- **Still leaks — egress semantics.** The streamed reply is edit-a-placeholder: it runs on
  Slack's `chat.update` against the message the ingress already posted, so a channel with no
  in-place edit must emulate it. That is no longer the whole egress model: the adapter also
  posts platform-owned messages such as approval cards and settles them in place afterwards.
  Email is another datapoint: it accumulates reply events per
  `(conversation_id, reply_ref)` and sends one threaded mail on `turn.completed`
  (`apps/mail-adapter/src/curie_mail_adapter/adapter.py::MailAdapter.send_reply`).
- **Fixed (#1459, ADR-0096).** The binding surface was Slack-typed in the control plane, not
  just at the channel edges: the agents table carried a `slack_channel` column, and agent
  create/update validated it as a Slack channel id, so binding any other channel kind took a
  schema change. The binding is now a neutral `{kind, address}` object
  (`apps/api/src/curie_api/schemas.py::ChannelBinding`) on its own table
  (`apps/api/src/curie_api/models.py::AgentChannel`), and the write gate is kind-dispatched
  (`apps/api/src/curie_api/schemas.py::_validate_channel_binding`): a registered kind
  validates on its own address shape, an unregistered one on a generic non-empty rule, so a
  new kind binds with no schema change. Still no multi-channel adapter framework (#27) — the
  restraint stands; only the Slack-shaped assumption is gone.
- **Fixed (#1459, ADR-0096 phase 2).** `ReplyHandle.kind` is required, and the worker
  resolves the required `(kind, address)` pair with uniqueness on that same pair. There is
  no address-only overload or default kind: two adapters can own the same address without
  silently selecting one another's binding.
- **Still leaks — registered address shapes.** Slack is the only registered kind in
  `_CHANNEL_ADDRESS_SHAPES`
  (`apps/api/src/curie_api/schemas.py::_CHANNEL_ADDRESS_SHAPES`); Discord and email
  bind under the generic non-empty rule. There is still no multi-channel adapter
  framework (#27). The routing pair removes the binding ambiguity; it does not by
  itself give other kinds a registered address shape.

## Cross-links

- **Epic(s):** #7 — promote the queue payload into `packages/aci-protocol` with
  channel-neutral field names (landed)
- **Epic(s):** #19 — per-turn reply routing (landed)
- **Epic(s):** #27 — deliberately defers a pluggable multi-channel framework
- **Epic(s):** #38 — channel-seam hardening / follow-up
- **Vision doc:** [architecture-vision.md](../../architecture-vision.md) — Job 6 (Communication channel), grade B-
- **ADR(s):** none directly on this seam
- **Interaction contract:** [Channel interaction](../channel-interaction/INTERFACE.md)
  defines the semantic reply before this Slack adapter renders it.

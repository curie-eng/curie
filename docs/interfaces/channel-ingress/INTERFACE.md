---
seam: Channel / ingress
kind: SOFT
impls: "2 (Slack, email)"
grade: C
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
> **Kind:** SOFT &nbsp;·&nbsp; **Implementations today:** 2 (Slack, email) &nbsp;·&nbsp; **Swap-readiness grade:** C
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
(edit-in-place). Two implementations today, Slack and email, and what the second one
taught is that the seam boundary is the HTTP wire rather than only the in-process port:
`apps/mail-adapter` is a service outside the core that neither constructs a `QueuedTurn`
nor implements `ReplySink`.

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
  session per), `author`, `text`, `received_at`, and `reply_handle` — a `ReplyHandle`
  (`packages/aci-protocol/src/aci_protocol/turn.py::ReplyHandle`) carrying `channel`,
  required nullable `placeholder`, and an optional per-turn `endpoint`. The Slack adapter
  currently supplies the pre-posted reply ts that the worker edits in place. The
  dispatcher serializes it to a single Stream field via `to_stream_fields`
  (`apps/dispatcher/src/curie_dispatcher/queue.py::to_stream_fields`), keyed by
  `STREAM_PAYLOAD_FIELD = "payload"` (`apps/dispatcher/src/curie_dispatcher/queue.py::STREAM_PAYLOAD_FIELD`).
  For the Slack adapter, `event_id` is the Slack event id, `conversation_id` is the thread
  ts, `author` is the Slack user id, and `reply_handle` carries the Slack channel plus the
  placeholder ts.
- **Egress** — the `ReplySink` Protocol (`apps/worker/src/curie_worker/reply_sink.py::ReplySink`),
  whose one method is `async def emit(self, event, *, route, best_effort_unreachable=False)`
  (`apps/worker/src/curie_worker/reply_sink.py::ReplySink.emit`) — four versioned neutral
  events (`turn.status`, `reply.update`, `reply.post`, `turn.completed`) over a
  worker-local `TargetRoute`. Slack's edit-in-place `chat.update`, its assistant-thread
  status, and the mrkdwn dialect all sit BELOW that port, in `SlackReplyAdapter` and
  `to_mrkdwn` (`apps/worker/src/curie_worker/mrkdwn.py::to_mrkdwn`).
- **Binding** — a channel resolves to a deployment by `agent_channels.address`
  equality in `BindingResolver.resolve` (`apps/worker/src/curie_worker/binding.py::BindingResolver.resolve`).
  The binding is written as a neutral `{kind, address}` pair (ADR-0096, #1459), so a second
  channel binds its agent without a schema change.

## Implementations today

Two: Slack and email.

- **Slack.** Ingress is `apps/dispatcher` (Bolt / Socket Mode); egress is
  `SlackReplyAdapter` (`apps/worker/src/curie_worker/slack_sink.py::SlackReplyAdapter`) on
  the Slack Web API.
- **Email (#1515).** Ingress and egress are one process outside the core,
  `apps/mail-adapter`: it polls an AgentMail inbox and POSTs each new message to the
  platform's channel ingress under a scoped `chn` token, then serves the four neutral
  reply events on its own HTTP endpoint and sends one threaded reply per `turn.completed`,
  addressed by the event's `target.reply_ref`. It holds no platform API key, no queue
  credential and no database access, which is what makes it the seam's proof: everything
  it needs is on the wire.

The swap proof that the protocol (not just the service) is the seam: the Rust CLI mints the exact
`QueuedTurn` wire payload with the same channel-neutral fields
(`cli/src/queue.rs`) and drives the whole deployed system with zero Slack contact
via `curie local message` / `cluster message` (`cli/src/chat.rs`, `cli/src/message.rs`).

## Known leakage

Two ends and the binding surface were cleaned; what remains is egress semantics and a
routing key that carries no kind.

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
- **Still leaks — egress semantics.** The reply model is edit-a-placeholder —
  `update(channel, ts, text)` on `chat.update`, not post-a-message — so any channel without
  in-place edit must emulate it. Email is the datapoint: with no editable message, the mail
  adapter accumulates the reply events per conversation and sends one threaded mail on
  `turn.completed` (`apps/mail-adapter/src/curie_mail_adapter/adapter.py::MailAdapter.send_reply`).
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
- **Still leaks — `kind` is stored, not routed.** The queue wire carries no channel kind, so
  the resolver matches on `address` alone and the uniqueness constraint is on `address`
  alone. Until `ReplyHandle` carries a kind, two adapters cannot own the same address, and
  `kind` selects the address validator and names the owning adapter without deciding
  anything at routing time.

## Cross-links

- **Epic(s):** #7 — promote the queue payload into `packages/aci-protocol` with
  channel-neutral field names (landed)
- **Epic(s):** #19 — per-turn reply routing (landed)
- **Epic(s):** #27 — deliberately defers a pluggable multi-channel framework
- **Epic(s):** #38 — channel-seam hardening / follow-up
- **Vision doc:** [architecture-vision.md](../../architecture-vision.md) — Job 6 (Communication channel), grade C
- **ADR(s):** none directly on this seam
- **Interaction contract:** [Channel interaction](../channel-interaction/INTERFACE.md)
  defines the semantic reply before this Slack adapter renders it.

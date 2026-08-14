---
seam: Channel / ingress (Slack)
kind: SOFT
impls: 1
grade: C
vision_row: Communication
epics:
  - "#7"
  - "#19"
  - "#27"
  - "#38"
order: 4
---
# INTERFACE: Channel / ingress (Slack)

> Part of the Curie swappable-seam catalog — see the [seam index](../../interfaces.md).
<!-- BEGIN GENERATED: header (curie dev docs-lint) -->
> **Kind:** SOFT &nbsp;·&nbsp; **Implementations today:** 1 &nbsp;·&nbsp; **Swap-readiness grade:** C
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
(edit-in-place). One implementation
today; the port is the wire + Protocol contract, extracted further only when a second
channel demands it ("the second implementation teaches the interface").

## Current contract

A second channel must produce the ingress payload and satisfy the egress Protocol:

- **Ingress** — `QueuedTurn` (`packages/aci-protocol/src/aci_protocol/turn.py::QueuedTurn`),
  a Pydantic model in the frozen ACI package with channel-neutral fields: `event_id`
  (idempotency key), `conversation_id` (the conversation/thread key routing keeps one live
  session per), `author`, `text`, `received_at`, and `reply_handle` — a `ReplyHandle`
  (`packages/aci-protocol/src/aci_protocol/turn.py::ReplyHandle`) carrying `channel`,
  `placeholder` (the pre-posted reply the worker edits in place), and an optional per-turn
  `endpoint`. The dispatcher serializes it to a single Stream field via `to_stream_fields`
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

One: Slack. Ingress is `apps/dispatcher` (Bolt / Socket Mode); egress is
`SlackReplyAdapter` (`apps/worker/src/curie_worker/slack_sink.py::SlackReplyAdapter`) on the Slack Web API. The swap proof that the
protocol (not just the service) is the seam: the Rust CLI mints the exact
`QueuedTurn` wire payload with the same channel-neutral fields
(`cli/src/queue.rs`) and drives the whole deployed system with zero Slack contact
via `curie local message` / `cluster message` (`cli/src/chat.rs`, `cli/src/message.rs`).

A third party channel adapter sits outside this seam entirely, over the
`POST /channels/turns` ingress and reply event egress described in
`docs/guides/building-a-channel-adapter.md`. Two artifacts help an author
build one: an `adapter.yaml` binding profile (a per install binding file
naming the channel kind, address shape, reply endpoint, and credential
identities; not the install agnostic composition manifest ADR-0096 decision 2
describes, which is separate and later), read and written by the `curie
adapter` verbs (`scaffold`, `validate`, `bind`, `token`, `smoke-test`); and
the `channel-protocol[conformance]` kit, a black box HTTP checker against the
seven rule wire floor, run either as an importable `run_floor` or as the
`curie-adapter-conformance` console command. Both live in
`packages/channel-protocol`.

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
  in-place edit must emulate it.
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

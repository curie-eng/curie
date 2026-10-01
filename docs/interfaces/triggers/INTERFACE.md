---
seam: Triggers
kind: SOFT
impls: 5 hardcoded (Slack, GH push, GH review, commit poll, generic HMAC hook) + per-agent cron scheduler (worker cron_loop)
grade: not separately graded
epics:
  - "#29"
order: 17
---

# INTERFACE: Triggers

> Part of the Curie swappable-seam catalog — see the [seam index](../../interfaces.md).

<!-- BEGIN GENERATED: header (curie dev docs-lint) -->
> **Kind:** SOFT &nbsp;·&nbsp; **Implementations today:** 5 hardcoded (Slack, GH push, GH review, commit poll, generic HMAC hook) + per-agent cron scheduler (worker cron_loop) &nbsp;·&nbsp; **Swap-readiness grade:** not separately graded
<!-- END GENERATED: header -->

**Kind legend:** CLEAN = a real `Protocol`/typed port class · SOFT = swap via env/URL/prefix/wire, no code interface · NONE = not built yet.

## The black line

A "trigger" is the thing that wakes an agent: an inbound event that gets turned into a
run. **Trigger is not a swappable seam, and there is deliberately no shared
`Trigger`/`EventSource` port** (decided on Epic #29). A trigger is a new event *kind* on the
runs stream ([ADR-0079](../../adr/0079-inbound-triggers-as-a-new-event-kind.md) decision 2):
its ingress is bespoke code that verifies its own source's credential and shape, then
enqueues a `QueuedTurn` (`packages/aci-protocol/src/aci_protocol/turn.py::QueuedTurn`)
whose `source` (`packages/aci-protocol/src/aci_protocol/turn.py::TurnSource`) names what
caused it. The five ingresses below share that one stream contract, and **that contract
is the seam.** A new trigger is a new producer of `QueuedTurn`, owned by whichever service
already receives its source, not an implementation of a trigger interface. Downstream of
the stream, the consumer, kernel, and claim path learn nothing about which ingress
produced a turn beyond `TurnSource.is_job`.

Why no port: the ingresses differ exactly where a port would sit. Slack arrives over
Socket Mode under the app token, GitHub and the generic hook arrive as HMAC-signed HTTP,
and the commit poll and the cron scheduler are timers with no inbound request at all. A
port over those would either restate the `QueuedTurn` contract under another name or
abstract away the authentication each receiver exists to perform. The `SOFT` kind above
means exactly this: the line is a wire payload, not a code interface. The wire payload
itself is owned by the [queue / stream seam](../queue-stream/INTERFACE.md) and the frozen
ACI protocol; this file catalogs its trigger producers.

## Current contract

The cross-trigger contract is the queued turn, nothing more: a new trigger means adding
another handler that mints a `QueuedTurn` with the right `source`. The five that exist:

- **Slack mention** — `apps/dispatcher/src/curie_dispatcher/handlers.py::process_event`:
  the `@app.event("app_mention")` listener (wired in
  `apps/dispatcher/src/curie_dispatcher/handlers.py::register_handlers`) calls
  `process_event(...)` to enqueue a run. (An adjacent `@app.event("message")` DM handler
  in the same `register_handlers`, gated to `channel_type == "im"`, shares the path.)
- **GitHub push** — `apps/api/src/curie_api/routers/github.py::github_webhook`:
  `@router.post("/webhook")` verifies the HMAC signature, then branches on
  `x_github_event`; a `"push"` event is handed to `process_push(...)`, a `"ping"`
  is answered `"pong"`, review events follow the review ingress when
  `github_review_ingress_enabled` is on, and `issues` events plus plain issue
  comments follow signed factory intake when `github_factory_ingress_enabled`
  is on. Every other event is `"ignored"`.
- **GitHub review feedback**: `apps/api/src/curie_api/routers/github.py::github_webhook`
  accepts actionable `issue_comment`, `pull_request_review_comment`, and
  `pull_request_review` deliveries after HMAC verification. Plain issue comments
  take the factory path first when that gate is on. Review ingress claims the
  delivery UUID, persists a durable `GitHubReviewFeedback` outbox row, and exposes
  worker-only provider truth and lineage checks through
  `apps/api/src/curie_api/routers/github_reviews.py`.

- **Commit poll** — `apps/api/src/curie_api/commitpoller.py::CommitPoller.run_forever`:
  a timer in the API asks GitHub whether the deploy branches moved and hands any
  new commit to the same `process_push(...)`. Off unless
  `api.commitPollIntervalSeconds` is set. It exists because the webhook above is
  an INBOUND request, and a self-hosted cluster behind a firewall cannot receive
  one at all -- outbound always works (#1239).
- **Generic HMAC hook** — `apps/api/src/curie_api/routers/hooks.py::ingest_hook`:
  `@router.post("/{agent_id}/{hook}")` verifies a Curie HMAC over
  `X-Curie-Timestamp`, `X-Curie-Delivery-Id`, the decoded hook name, the parsed
  requested `tool_access` policy and the raw body. The context is compact ASCII
  JSON for `[hook, tool_access]`, with `null` for an omitted policy. The signed
  bytes are `b"curie.hook.delivery.v2\n"` followed by
  `{timestamp}.{delivery_id}.{len(context)}:`, the context and raw body. The fixed
  prefix separates this format from previous signatures. The context byte
  length fixes its boundary; the delivery id may
  not contain `.`, which would make the earlier boundary ambiguous. A captured
  signature cannot change the hook's receipt namespace or add or remove a
  policy restriction. A timestamp more than 5 minutes from the server clock is
  refused with the same 401 as a bad signature. The route claims the delivery
  id and enqueues a `QueuedTurn` with `source=WEBHOOK`. The
  turn replies through one of the agent's bindings: its only one, or the route
  the `kind`, `address` and optional `adapter` query parameters name (the
  identity for Slack, the adapter slug for any other kind; ADR-0168 decision 3).
  Optional `tool_access=read-only` narrows this turn under the existing
  [TOOL-ACCESS contract](../aci-producer/INTERFACE.md). Omission retains ordinary
  hooks and approvals; untrusted body text never selects the policy. The receipt's
  `tool_access` proves only the queued value. Completed retries must match the
  original value, and return 409 if it differs or the original queued turn is
  unavailable. A pending restricted retry also returns 409; an ordinary pending
  202 receipt proves no accepted turn. Before opting in, the operator must verify
  homogeneous worker artifacts implementing TOOL-ACCESS-6 and compatible runners;
  this API cannot discover or exclude old workers. Implementing workers verify the
  exact runner's advertisement before dispatch. Old/mixed fleets remain an intake
  installation blocker (#3603); this is neither automatic fleet admission nor
  mandatory source policy. This is a hardcoded platform ingress, not consumption
  of a bundle-declared `webhook` path.

  **Signing upgrade:** every custom signer must migrate to the context format
  above; the API refuses signatures made with the previous format. Update the
  API and the example's copied `alert-signer-code` ConfigMap together, then
  restart its signer. The [Alert source installation](../../../examples/sre-bot/README.md#alert-source-opt-in)
  documents the existing ConfigMap update steps.

The five share no abstraction: a Slack Bolt event listener, two paths through a FastAPI
GitHub HMAC route, an asyncio timer, and a FastAPI generic HMAC route. The GitHub push
and commit poll converge one step earlier than the others -- both call
`process_push`, deliberately, so the two deploy ingresses cannot disagree about
what a push means.

**Three further wake paths the inventory omitted.** Beyond the external triggers above,
two platform-internal paths and one operator-driven path also turn an event into a run on
the same `curie:runs` stream, and a truthful inventory names them:

- **Slack block-action (button click)** —
  `apps/dispatcher/src/curie_dispatcher/handlers.py::process_action` normalizes a Block
  Kit button click into a `QueuedTurn` (dedupe, in-thread placeholder, enqueue) so a click
  is answered exactly as if the user had typed the button's command. Approval-card clicks
  are excluded here and resolve through the API instead.
- **Approval-resume** — resolving or expiring a durable approval enqueues a
  platform-authored resume turn onto the runs stream via
  `apps/api/src/curie_api/resumequeue.py::ResumeQueue.enqueue`, so a suspended session
  wakes down the identical consumer/kernel/claim path a Slack mention takes (see the
  [approval seam](../approval/INTERFACE.md)).
- **CLI enqueue** — `curie local message` builds a `QueuedTurn` with
  `synthetic_turn`, then runs the dispatcher's Slack-free one-shot producer in
  Compose. That producer owns dedupe, the producer span, W3C carrier injection,
  and the Stream append without constructing a Slack client. `curie cluster
  message` retains the direct `xadd` path in `cli/src/queue.rs` as the legacy
  carrierless compatibility control. Both are operator-driven wakes that skip
  the live Slack listener, GitHub webhook, and commit poller, and both hand-mint
  their dedupe id (`new_event_id` in `cli/src/queue.rs`), which is the leak
  recorded below.

**Declaration vs. consumption (#273/#270).** The bundle manifest now carries deploy-time-validated
`triggers` declarations (`TriggerDeclaration` in `packages/plugin-format`, `triggers.*` validation
codes), so an agent's non-chat wake-ups ship in one reviewable artifact and a malformed declaration
is rejected at deploy. A cron declaration needs a unique non-empty name, a non-empty prompt, and a
five-field schedule; timezone, when present, is an IANA zone name matching an exact key in packaged
tzdata, so host only aliases such as `localtime` are rejected; it defaults to UTC only when omitted and is legal only with a
schedule; target, when present, is a non-empty channel address string; schedule is forbidden on other
types; webhook `{type, path}` is unchanged. Declared `cron` triggers are consumed: the worker's
per-agent cron scheduler (`apps/worker/src/curie_worker/cron_loop.py::CronSchedulerLoop`, ADR-0099,
#268) fires each declared schedule as a CRON `QueuedTurn`. See
[Cron triggers](../../guides/cron-triggers.md) for the operator guide. A generic HMAC hook ingress
is shipped (`ingest_hook` above). Mapping a declared `webhook` path onto that handler at deploy is
not built (#3666), so a declared webhook validates its shape but does not yet wire a live wake-up;
`ingest_hook` serves any valid hook name whether or not the bundle declared it.

## Implementations today

Five hardcoded external triggers in two different processes, plus the declared per-agent cron
scheduler in the worker:

1. Slack `app_mention` in the dispatcher (`apps/dispatcher/src/curie_dispatcher/handlers.py::process_event`).
2. GitHub `push` webhook in the API (`apps/api/src/curie_api/routers/github.py::github_webhook`).
3. Commit poll in the API (`apps/api/src/curie_api/commitpoller.py::CommitPoller.run_forever`),
   opt-in via `api.commitPollIntervalSeconds`. Timer-driven wake is therefore no longer
   entirely unbuilt: this one is real, though it is a single hardcoded platform timer. The
   per-agent declared `cron` is item 6.
4. Generic HMAC hook in the API (`apps/api/src/curie_api/routers/hooks.py::ingest_hook`).
5. GitHub review feedback in the API
   (`apps/api/src/curie_api/routers/github.py::github_webhook`), with worker-only
   provider truth and lineage checks in `apps/api/src/curie_api/routers/github_reviews.py`.
6. Declared cron triggers in the worker
   (`apps/worker/src/curie_worker/cron_loop.py::CronSchedulerLoop.run_forever`, ADR-0099, #268):
   each tick reads every in-force deployment's `cron` triggers, records the due slot in
   `hook_runs`, and enqueues one CRON turn. `GET /schedules`
   (`apps/api/src/curie_api/routers/schedules.py::list_schedules`, #2933) lists
   those hooks with the newest slot. `curie local schedules` and
   `curie cluster schedules` read that route. Operator guide: [Cron triggers](../../guides/cron-triggers.md).

Plus three further wake paths that also enqueue a run without going through any of those
five: the Slack block-action handler
(`apps/dispatcher/src/curie_dispatcher/handlers.py::process_action`), the approval-resume
enqueue (`apps/api/src/curie_api/resumequeue.py::ResumeQueue.enqueue`), and the CLI's own
enqueue (`cli/src/message.rs` via `synthetic_turn`/`xadd`/`new_event_id` in
`cli/src/queue.rs`), which is operator-driven rather than platform-internal.

## Known leakage

Each trigger carries its source's shape up to the stream and no further: Slack triggers
are Bolt-event-shaped and authed by the Slack app token; the GitHub trigger is
HMAC-signature-shaped and lives "outside the X-API-Key dependency" (`github.py`
docstring). That is by design, not a gap awaiting a port: the common event contract these
ingresses reconcile into is the `QueuedTurn` itself, and each transport-specific receiver
stays where its source arrives.

A second, narrower leak the CLI path exposes: **the dedupe id is minted by whoever enqueues**,
under a different rule per producer, with nothing enforcing that the rules stay disjoint.
`apps/dispatcher/src/curie_dispatcher/handlers.py::process_event` passes Slack's own `event_id`
through verbatim; `apps/dispatcher/src/curie_dispatcher/handlers.py::process_action` synthesizes
`action-<interaction id>`; `apps/api/src/curie_api/resumequeue.py::resume_event_id` returns a
deterministic `approval-<id>-resolved`; GitHub review feedback first claims the delivery UUID
through `apps/api/src/curie_api/github_review_audit.py::claim_review_delivery`, then mints a
stable `github-feedback-<uuid5>` event id from repository id, event kind, and provider feedback
id in `apps/api/src/curie_api/github_review_events.py::UnverifiedFeedback.event_id`; and the CLI generates a random uuid behind an `EvSIM-`
prefix (`cli/src/queue.rs`), chosen expressly so it cannot collide with a real Slack `Ev...` id.
Idempotency across producers therefore holds by convention, not by contract. Because there is
no trigger port, taking ownership of that rule would be a change to the shared turn contract,
not to any trigger.

## Cross-links

- **Epic(s):** #29, closed with the decision above: trigger is not a seam, and new triggers are new event kinds on the runs stream. Remaining work is tracked by #2935 (per-hook model, prompt and env), #2936 (bind hooks from the control plane), #2938 (a cron hook targeting a thread), and #3666 (map a declared webhook onto its hook).
- **Vision doc:** [architecture-vision.md](../../architecture-vision.md) — not one of the six swappable jobs; not separately graded.
- **ADR(s):** [ADR-0079](../../adr/0079-inbound-triggers-as-a-new-event-kind.md) (Accepted) — inbound triggers as a new event kind, ingested by the API; [ADR-0099](../../adr/0099-hooks-are-bundle-declared-turns-the-system-starts.md) (Accepted) — hooks are bundle-declared turns the system starts.

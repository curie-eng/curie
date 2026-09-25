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
run. Today there are **five hardcoded triggers** wired directly into their respective
ingress handlers, with **no shared `Trigger`/`EventSource` port** between them. There
is no swappable line here yet — each trigger is bespoke code. The open architectural
question (Epic #29) is whether "trigger" is even a real seam, or whether new triggers
are just new *event types* handled inside the existing Slack-dispatcher and
API-webhook ingresses. This file records the current state honestly; it does not
assert a port that does not exist.

## Current contract

There is no cross-trigger contract to satisfy — a new trigger today means adding
another hardcoded handler. The five that exist:

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
  `@router.post("/{agent_id}/{hook}")` verifies a Curie HMAC over the raw body,
  claims the delivery id, and enqueues a `QueuedTurn` with `source=WEBHOOK`. This
  is a hardcoded platform ingress, not consumption of a bundle-declared
  `webhook` path.

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
is shipped (`ingest_hook` above); mapping a declared `webhook` path onto that handler is still the
open Epic #29 question and is not built, so a declared webhook validates its shape but does not yet
wire a live wake-up.

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

The whole seam is "leakage" in the sense that nothing is abstracted yet. Each trigger
carries its source's shape end to end: Slack triggers are Bolt-event-shaped and
authed by the Slack app token; the GitHub trigger is HMAC-signature-shaped and lives
"outside the X-API-Key dependency" (`github.py` docstring). A future `Trigger` port —
if Epic #29 concludes one is warranted — must reconcile these two auth models and
payload shapes into a common event contract, and would live alongside the ingress
handlers rather than replacing the transport-specific receivers.

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
Idempotency across producers therefore holds by convention, not by contract, and that is the
first thing a real `Trigger` port would have to take ownership of.

## Cross-links

- **Epic(s):** #29 — triggers: decide whether "trigger" is a real seam (extract an `EventSource` port) or just new event types on the existing ingresses.
- **Vision doc:** [architecture-vision.md](../../architecture-vision.md) — not one of the six swappable jobs; not separately graded.
- **ADR(s):** [ADR-0079](../../adr/0079-inbound-triggers-as-a-new-event-kind.md) (Accepted) — inbound triggers as a new event kind, ingested by the API; [ADR-0099](../../adr/0099-hooks-are-bundle-declared-turns-the-system-starts.md) (Accepted) — hooks are bundle-declared turns the system starts.

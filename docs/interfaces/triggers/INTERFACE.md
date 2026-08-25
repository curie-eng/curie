---
seam: Triggers
kind: SOFT
impls: 3 hardcoded (Slack, GH push, commit poll)
grade: not separately graded
epics:
  - "#29"
order: 17
---

# INTERFACE: Triggers

> Part of the Curie swappable-seam catalog — see the [seam index](../../interfaces.md).

<!-- BEGIN GENERATED: header (curie dev docs-lint) -->
> **Kind:** SOFT &nbsp;·&nbsp; **Implementations today:** 3 hardcoded (Slack, GH push, commit poll) &nbsp;·&nbsp; **Swap-readiness grade:** not separately graded
<!-- END GENERATED: header -->

**Kind legend:** CLEAN = a real `Protocol`/typed port class · SOFT = swap via env/URL/prefix/wire, no code interface · NONE = not built yet.

## The black line

A "trigger" is the thing that wakes an agent: an inbound event that gets turned into a
run. Today there are **three hardcoded triggers** wired directly into their respective
ingress handlers, with **no shared `Trigger`/`EventSource` port** between them. There
is no swappable line here yet — each trigger is bespoke code. The open architectural
question (Epic #29) is whether "trigger" is even a real seam, or whether new triggers
are just new *event types* handled inside the existing Slack-dispatcher and
API-webhook ingresses. This file records the current state honestly; it does not
assert a port that does not exist.

## Current contract

There is no cross-trigger contract to satisfy — a new trigger today means adding
another hardcoded handler. The three that exist:

- **Slack mention** — `apps/dispatcher/src/curie_dispatcher/handlers.py::process_event`:
  the `@app.event("app_mention")` listener (wired in
  `apps/dispatcher/src/curie_dispatcher/handlers.py::register_handlers`) calls
  `process_event(...)` to enqueue a run. (An adjacent `@app.event("message")` DM handler
  in the same `register_handlers`, gated to `channel_type == "im"`, shares the path.)
- **GitHub push** — `apps/api/src/curie_api/routers/github.py::github_webhook`:
  `@router.post("/webhook")` verifies the HMAC signature, then branches on
  `x_github_event`; a `"push"` event is handed to `process_push(...)`, a `"ping"`
  is answered `"pong"`, and every other event is `"ignored"`.

- **Commit poll** — `apps/api/src/curie_api/commitpoller.py::CommitPoller.run_forever`:
  a timer in the API asks GitHub whether the deploy branches moved and hands any
  new commit to the same `process_push(...)`. Off unless
  `api.commitPollIntervalSeconds` is set. It exists because the webhook above is
  an INBOUND request, and a self-hosted cluster behind a firewall cannot receive
  one at all -- outbound always works (#1239).

The three share no abstraction: a Slack Bolt event listener, a FastAPI route with
GitHub HMAC auth, and an asyncio timer. The last two converge one step earlier than
the others -- both call `process_push`, deliberately, so the two deploy ingresses
cannot disagree about what a push means.

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
  their dedupe id (`new_event_id` in `cli/src/message.rs`), which is the leak
  recorded below.

**Declaration vs. consumption (#273/#270).** The bundle manifest now carries deploy-time-validated
`triggers` declarations (`cron` with a `schedule`, `webhook` with a `path`; `TriggerDeclaration` in
`packages/plugin-format`, `triggers.*` validation codes), so an agent's non-chat wake-ups ship in one
reviewable artifact and a malformed declaration is rejected at deploy. This is the *declaration*
surface only — the *runtime* that acts on a declared trigger (a cron scheduler, a per-agent webhook
ingress) is still the open Epic #29 question above and is not built. Declaring a trigger validates its
shape; it does not yet wire a live wake-up.

## Implementations today

Three external triggers, all hardcoded, in two different processes:

1. Slack `app_mention` in the dispatcher (`apps/dispatcher/src/curie_dispatcher/handlers.py::process_event`).
2. GitHub `push` webhook in the API (`apps/api/src/curie_api/routers/github.py::github_webhook`).
3. Commit poll in the API (`apps/api/src/curie_api/commitpoller.py::CommitPoller.run_forever`),
   opt-in via `api.commitPollIntervalSeconds`. Timer-driven wake is therefore no longer
   entirely unbuilt: this one is real, though it is a single hardcoded platform timer and
   not the per-agent declared `cron` the trigger DECLARATION surface anticipates.

Plus three further wake paths that also enqueue a run without going through any of those
three: the Slack block-action handler
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
deterministic `approval-<id>-resolved`; and the CLI generates a random uuid behind an `EvSIM-`
prefix (`cli/src/queue.rs`), chosen expressly so it cannot collide with a real Slack `Ev...` id.
Idempotency across producers therefore holds by convention, not by contract, and that is the
first thing a real `Trigger` port would have to take ownership of.

## Cross-links

- **Epic(s):** #29 — triggers: decide whether "trigger" is a real seam (extract an `EventSource` port) or just new event types on the existing ingresses.
- **Vision doc:** [architecture-vision.md](../../architecture-vision.md) — not one of the six swappable jobs; not separately graded.
- **ADR(s):** none yet — no accepted ADR governs the trigger seam.

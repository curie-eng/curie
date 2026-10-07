---
seam: Triggers
kind: SOFT
impls: 8 hardcoded (Slack, GH push, GH review, commit poll, generic HMAC hook, GH factory issue intake, GH factory poll intake, factory missed-label reconcile) + per-agent cron scheduler (worker cron_loop)
grade: not separately graded
epics:
  - "#29"
order: 17
---

# INTERFACE: Triggers

> Part of the Curie swappable-seam catalog — see the [seam index](../../interfaces.md).

<!-- BEGIN GENERATED: header (curie dev docs-lint) -->
> **Kind:** SOFT &nbsp;·&nbsp; **Implementations today:** 8 hardcoded (Slack, GH push, GH review, commit poll, generic HMAC hook, GH factory issue intake, GH factory poll intake, factory missed-label reconcile) + per-agent cron scheduler (worker cron_loop) &nbsp;·&nbsp; **Swap-readiness grade:** not separately graded
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
caused it. The eight ingresses below share that one stream contract, and **that contract
is the seam.** A new trigger is a new producer of `QueuedTurn`, owned by whichever service
already receives its source, not an implementation of a trigger interface. Downstream of
the stream, the consumer, kernel, and claim path learn nothing about which ingress
produced a turn beyond `TurnSource.is_job`.

Why no port: the ingresses differ exactly where a port would sit. Slack arrives over
Socket Mode under the app token, GitHub and the generic hook arrive as HMAC-signed HTTP,
and the commit poll, the factory poll intake, the factory label reconciler, and the cron scheduler are timers with no inbound request at all. A
port over those would either restate the `QueuedTurn` contract under another name or
abstract away the authentication each receiver exists to perform. The `SOFT` kind above
means exactly this: the line is a wire payload, not a code interface. The wire payload
itself is owned by the [queue / stream seam](../queue-stream/INTERFACE.md) and the frozen
ACI protocol; this file catalogs its trigger producers.

## Current contract

The cross-trigger contract is the queued turn, nothing more: a new trigger means adding
another handler that mints a `QueuedTurn` with the right `source`. The eight that exist:

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

- **Signed factory issue intake**: when `github_factory_ingress_enabled` is on,
  `apps/api/src/curie_api/routers/github.py::github_webhook` hands an `issues`
  delivery, or a plain issue comment, to
  `apps/api/src/curie_api/github_factory.py::handle_factory_delivery` after HMAC
  verification. It claims the delivery UUID, checks the installation, repository
  allowlist, and sender permission, and admits one canonical WorkItem without reading
  the issue body into the platform. It does not enqueue directly: the WorkItem
  reconciler later publishes the execute turn
  (`apps/api/src/curie_api/workitem_reconciler.py::WorkItemReconciler._publish_execute_wakes`).
- **Factory poll intake** (ADR-0187):
  `apps/api/src/curie_api/factory_poll_intake.py::poll_once` is the default factory
  door. `github_factory_intake` is `"poll"` unless an operator sets `"webhook"`. One
  pass lists labeled issues, mentions, and review feedback for each bound repository
  with the installation credential and admits through the same verification the
  webhook uses, without writing a delivery receipt. Cursors live in
  `curie.factory_poll_cursors`, and one API replica polls at a time under a Postgres
  advisory lock. It runs as a step of the API's WorkItem reconciler loop
  (`apps/api/src/curie_api/workitem_reconciler.py::WorkItemReconciler._reconcile_missed_labels`),
  every `github_factory_poll_interval_s` (45 seconds by default), and only when
  factory ingress is on. In poll mode no webhook secret is needed to boot.
- **Factory missed-label reconciliation** (webhook mode only):
  `apps/api/src/curie_api/factory_label_reconcile.py::reconcile_missed_labels` lists
  open issues carrying the factory label on every bound repository and admits any with
  no WorkItem, through the same verification as the webhook, once the label is older
  than `github_factory_reconcile_grace_s`. GitHub does not redeliver a failed webhook,
  so without it a label is lost. It runs in the same step of the API's WorkItem
  reconciler loop in place of `poll_once`, at most every
  `github_factory_reconcile_interval_s` (300 seconds by default, 0 disables it), and
  only when factory ingress is on and `github_factory_intake` is `"webhook"`. In that
  mode the signed webhook remains the immediate intake path and reconciliation
  recovers labels whose delivery was missed. In poll mode this function does not run.
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
  A trusted intake that already owns a source thread may also pass a nonempty
  `conversation_id` and `placeholder` pair, so the ordinary worker completes
  that preposted reply in place. Message coordinates never replace the stored
  binding's endpoint or adapter route (ADR-0182).
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

  **Source policy administration (operator note, #3603):** the routes
  `GET`, `PUT` and `DELETE /agents/{agent_id}/hooks/{hook}/source-policy`,
  `POST .../rotate` and `GET .../secret` take the platform key or a live console
  session, never a hook signature. Every mutation needs the provisioner's
  runtime directory (`CURIE_PROTECTED_RUNTIME_DIR`), including its
  `source_writer.json`; without it each one answers 503 `runtime_unavailable`
  and changes nothing. The first protected policy of an agent advances its
  legacy hook counter, which invalidates every ordinary hook key of that agent;
  reissue them through the legacy `GET /agents/{agent_id}/hook-secret` route.

  **Protected publication.** A protected `PUT` or `rotate` commits the policy
  row, then publishes it on the broker once a control reader session finds the
  runtime evidence current, and answers 200 with `activation: active`. A
  refusal after the commit answers 503 with the `committed_generation` and one
  of `runtime_unavailable`, `broker_identity_mismatch`,
  `qualification_unavailable`, `evidence_missing`, `evidence_expired`,
  `configuration_unsupported`, `source_reservation_lost` or
  `broker_unavailable`. The row is durable and stays closed; do not retry with
  a new operation. Once the cause is fixed, an exact replay of the same
  operation publishes it while its broker reservation still matches, and
  otherwise a fresh `rotate` does. `source_publication_deferred` and GET's
  `publication_deferred` no longer exist. GET `active` and the `secret` route
  attest publication only, never current readiness: the secret is served, with
  `no-store`, only for a published protected source, and is refused 503
  (`source_closed`, `runtime_unavailable`, `broker_unavailable`) otherwise.
  `DELETE` publishes an ordinary tombstone without moving the counter. Signed
  delivery to that tombstone runs the ordinary path only while its ordinary
  publication is active on the broker; otherwise it answers 503
  `source_closed`, a delivery ID that already has a private intent answers 409
  `delivery_conflict`, and a broker failure answers 503 `broker_unavailable`.

  **Runtime files.** The provisioner writes `manifest.json`, `ca.pem`,
  `bootstrap.json`, `source_writer.json` and `enqueue.json` into the runtime
  directory and mounts it read only into the API. `enqueue.json` holds exactly
  `schema_version: 1`, `credential_ref` and the `enqueue` username and password.
  Its `credential_ref` must equal the manifest's `credential_refs.enqueue`, and
  its username must differ from `default` and from the control reader, so a
  stale file after a provisioner rotation is invalid rather than used. Ingress
  and the reconciler read it; administration never does; the probe parses it
  and never connects with it. No route, CLI verb, chart default or environment
  variable creates it, and mounting the directory in a cluster is LANE-8
  (#4076).

  **Support probe.** `POST /hooks/{agent_id}/{hook}/support` answers 200
  `supported` exactly when a delivery would be admitted: a published protected
  row, no `source_bindings` on the agent, valid runtime and enqueue files, and
  one control reader session that finds the selection open, the manifest equal
  to the provisioner's, the qualification present and the readiness current.
  Every other state is 503 with its first failing reason. `supported` does not
  cover per delivery conditions such as the quota, a duplicate, the reply
  target or broker reachability for the enqueue principal. A published
  tombstone still answers `source_closed`; GET's `activation` says whether
  ordinary delivery is restored.

  **Protected delivery.** A signed delivery to a published protected source is
  admitted atomically onto the private broker and never touches the ordinary
  store, backlog slot, workspace or SQL. It refuses a caller supplied
  `conversation_id` or `placeholder` with 422
  `protected_reply_target_unsupported`, an agent with `source_bindings` with
  503 `configuration_unsupported`, and a turn larger than 262144 bytes with
  413. An ordinary claim already enqueued for the same delivery ID answers 409
  `delivery_conflict`; a pending one answers 503 `ordinary_delivery_pending`
  with `Retry-After`. The receipt adds `requested_tool_access`,
  `effective_tool_access`, `source_generation` and `acceptance_status`
  (`accepted`, `pending` or `preparing`); `tool_access` remains the effective
  policy. Admission answers 200 `accepted`, 200 with `duplicate` true and the
  original receipt for any retry, 202 `preparing` while an interrupted intent
  awaits recovery, 409 `protected_delivery_failed` for an intent that failed
  for good, 409 `delivery_conflict`, and 503 with the admission reason or
  `broker_unavailable`. A full protected backlog answers 429
  `protected_backlog_full` with no `Retry-After`: the quota is 64 members
  across the broker, and only the future protected worker releases them, so
  waiting does not help.

  **Reconciler.** Each API process runs one protected admission reconciler,
  idle while `CURIE_PROTECTED_RUNTIME_DIR` is unset or a runtime file is
  invalid. Every five seconds it recovers preparing intents with no caller:
  one commits once authority opens, and one that stays closed fails with its
  quota refunded after ten attempts or 300 seconds. Each tick logs only counts:
  `protected admission reconciler tick outcome=ok quota=N parked=N preparing=N
  recovered=N failed=N skipped=N`, or `outcome=broker_unavailable`. `quota` is
  occupancy, `parked` counts committed deliveries waiting for a worker, and
  `failed` counts intents failed this tick; a caller retry of a failed delivery
  ID then answers 409 `protected_delivery_failed`.

  **Still unavailable.** No protected worker exists (LANE-6, LANE-7), so an
  admitted payload parks privately on the broker with its receipt and quota
  member and is never dispatched or answered; after 64 admissions every
  delivery answers 429. Provisioning and the shared bootstrap grammar (LANE-8,
  #4076), private cron routing and the CLI and console siblings (#4053, #4054)
  remain out. If a misprovisioned runtime opens admission early, set
  `admission_open: false` in the provisioner's selection: new admission stops
  at once, parked entries and their quota stay private until LANE-6, and the
  tick log's `quota` and `parked` counts show them.

The eight share no abstraction: a Slack Bolt event listener, three paths through a
FastAPI GitHub HMAC route, three asyncio timers, and a FastAPI generic HMAC route. The GitHub push
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

Eight hardcoded external triggers in two different processes, plus the declared per-agent cron
scheduler in the worker:

1. Slack `app_mention` in the dispatcher (`apps/dispatcher/src/curie_dispatcher/handlers.py::process_event`).
2. GitHub `push` webhook in the API (`apps/api/src/curie_api/routers/github.py::github_webhook`).
3. Commit poll in the API (`apps/api/src/curie_api/commitpoller.py::CommitPoller.run_forever`),
   opt-in via `api.commitPollIntervalSeconds`. Timer-driven wake is therefore no longer
   entirely unbuilt: this one is real, though it is a single hardcoded platform timer. The
   per-agent declared `cron` is item 9.
4. Generic HMAC hook in the API (`apps/api/src/curie_api/routers/hooks.py::ingest_hook`).
5. GitHub review feedback in the API
   (`apps/api/src/curie_api/routers/github.py::github_webhook`), with worker-only
   provider truth and lineage checks in `apps/api/src/curie_api/routers/github_reviews.py`.
6. Signed factory issue intake in the API
   (`apps/api/src/curie_api/github_factory.py::handle_factory_delivery`, reached from
   `apps/api/src/curie_api/routers/github.py::github_webhook`), admitting a WorkItem
   that the WorkItem reconciler later enqueues.
7. Factory missed-label reconciliation in the API
   (`apps/api/src/curie_api/factory_label_reconcile.py::reconcile_missed_labels`), a
   step of the WorkItem reconciler loop that, in webhook mode only, recovers labels
   whose signed webhook delivery was missed.
8. Factory poll intake in the API
   (`apps/api/src/curie_api/factory_poll_intake.py::poll_once`), the default intake: a
   step of the same loop that lists labeled issues, mentions, and review feedback from
   GitHub and admits them.
9. Declared cron triggers in the worker
   (`apps/worker/src/curie_worker/cron_loop.py::CronSchedulerLoop.run_forever`, ADR-0099, #268):
   each tick reads every in-force deployment's `cron` triggers, records the due slot in
   `hook_runs`, and enqueues one CRON turn. `GET /schedules`
   (`apps/api/src/curie_api/routers/schedules.py::list_schedules`, #2933) lists
   those hooks with independent newest scheduled and manual histories, selected
   by the persisted `hook_runs.source` (`schedule` or `manual`). Scheduled
   `last_fire_at`, `last_outcome`, and `last_reason` exclude manual fires;
   `last_manual_fire_at`, `last_manual_outcome`, and `last_manual_reason` report
   the newest manual fire. `curie local schedules` and `curie cluster schedules`
   read that route. `curie local hook record <agent> <name> <id>` and its cluster
   counterpart read one persisted run, including an open or non-`ran` row,
   without waiting and exit 0 when found. Operator guide:
   [Cron triggers](../../guides/cron-triggers.md).

Plus three further wake paths that also enqueue a run without going through any of those
eight: the Slack block-action handler
(`apps/dispatcher/src/curie_dispatcher/handlers.py::process_action`), the approval-resume
enqueue (`apps/api/src/curie_api/resumequeue.py::ResumeQueue.enqueue`), and the CLI's own
enqueue (`cli/src/message.rs` via `synthetic_turn`/`xadd`/`new_event_id` in
`cli/src/queue.rs`), which is operator-driven rather than platform-internal.

## Known leakage

Each trigger carries its source's shape up to the stream and no further: Slack triggers
are Bolt-event-shaped and authed by the Slack app token; the GitHub trigger is
HMAC-signature-shaped and lives "outside the X-API-Key dependency" (`github.py`
docstring). The factory poll intake and the label reconciler have no inbound request; it reaches GitHub with
the installation credential and re-runs the webhook path's installation, label, and
write-permission checks itself. That is by design, not a gap awaiting a port: the common event contract these
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
id in `apps/api/src/curie_api/github_review_events.py::UnverifiedFeedback.event_id`; factory
missed-label reconciliation derives its request id from GitHub's labeled event id so replicas and
repeated passes converge (`apps/api/src/curie_api/factory_label_reconcile.py`); and the CLI generates a random uuid behind an `EvSIM-`
prefix (`cli/src/queue.rs`), chosen expressly so it cannot collide with a real Slack `Ev...` id.
Idempotency across producers therefore holds by convention, not by contract. Because there is
no trigger port, taking ownership of that rule would be a change to the shared turn contract,
not to any trigger.

## Cross-links

- **Epic(s):** #29, closed with the decision above: trigger is not a seam, and new triggers are new event kinds on the runs stream. Remaining work is tracked by #2935 (per-hook model, prompt and env), #2936 (bind hooks from the control plane), #2938 (a cron hook targeting a thread), and #3666 (map a declared webhook onto its hook).
- **Vision doc:** [architecture-vision.md](../../architecture-vision.md) — not one of the six swappable jobs; not separately graded.
- **ADR(s):** [ADR-0079](../../adr/0079-inbound-triggers-as-a-new-event-kind.md) (Accepted) — inbound triggers as a new event kind, ingested by the API; [ADR-0099](../../adr/0099-hooks-are-bundle-declared-turns-the-system-starts.md) (Accepted) — hooks are bundle-declared turns the system starts; [ADR-0187](../../adr/0187-the-factory-polls-github-and-the-platform-reads-the-issue.md) (Accepted): the factory polls GitHub for its work, and the webhook becomes optional.

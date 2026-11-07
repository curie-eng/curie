---
seam: Triggers
kind: SOFT
impls: 10 hardcoded (Slack, GH push, GH review, commit poll, generic HMAC hook, GH factory issue intake, GH factory poll intake, factory missed-label reconcile, tracker intake poll, code-host review feedback poll) + per-agent cron scheduler (worker cron_loop)
grade: not separately graded
epics:
  - "#29"
order: 17
---

# INTERFACE: Triggers

> Part of the Curie swappable-seam catalog — see the [seam index](../../interfaces.md).

<!-- BEGIN GENERATED: header (curie dev docs-lint) -->
> **Kind:** SOFT &nbsp;·&nbsp; **Implementations today:** 10 hardcoded (Slack, GH push, GH review, commit poll, generic HMAC hook, GH factory issue intake, GH factory poll intake, factory missed-label reconcile, tracker intake poll, code-host review feedback poll) + per-agent cron scheduler (worker cron_loop) &nbsp;·&nbsp; **Swap-readiness grade:** not separately graded
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
caused it. The ten hardcoded producers below share that one stream contract, and **that contract
is the seam.** A new trigger is a new producer of `QueuedTurn`, owned by whichever service
already receives its source, not an implementation of a trigger interface. Downstream of
the stream, the consumer, kernel, and claim path learn nothing about which ingress
produced a turn beyond `TurnSource.is_job`.

Why no port: the ingresses differ exactly where a port would sit. Slack arrives over
Socket Mode under the app token, GitHub and the generic hook arrive as HMAC-signed HTTP,
and five hardcoded producers are timers: commit poll, factory poll intake,
factory label reconciliation, tracker intake poll and code-host review feedback
poll. The declared cron scheduler is another timer. None has an inbound request. A
port over those would either restate the `QueuedTurn` contract under another name or
abstract away the authentication each receiver exists to perform. The `SOFT` kind above
means exactly this: the line is a wire payload, not a code interface. The wire payload
itself is owned by the [queue / stream seam](../queue-stream/INTERFACE.md) and the frozen
ACI protocol; this file catalogs its trigger producers.

## Current contract

The cross-trigger contract is the queued turn, nothing more: a new trigger means adding
another handler that mints a `QueuedTurn` with the right `source`. The ten hardcoded producers that exist:

1. **Slack mention**: `apps/dispatcher/src/curie_dispatcher/handlers.py::process_event`:
  the `@app.event("app_mention")` listener (wired in
  `apps/dispatcher/src/curie_dispatcher/handlers.py::register_handlers`) calls
  `process_event(...)` to enqueue a run. (An adjacent `@app.event("message")` DM handler
  in the same `register_handlers`, gated to `channel_type == "im"`, shares the path.)
2. **GitHub push**: `apps/api/src/curie_api/routers/github.py::github_webhook`:
  `@router.post("/webhook")` verifies the HMAC signature, then branches on
  `x_github_event`; a `"push"` event is handed to `process_push(...)`, a `"ping"`
  is answered `"pong"`, review events follow the review ingress when
  `github_review_ingress_enabled` is on, and `issues` events plus plain issue
  comments follow signed factory intake when `factory_enabled`
  (`factory.enabled`) is on. Every other event is `"ignored"`.
3. **GitHub review feedback**: `apps/api/src/curie_api/routers/github.py::github_webhook`
  accepts actionable `issue_comment`, `pull_request_review_comment`, and
  `pull_request_review` deliveries after HMAC verification. Plain issue comments
  take the factory path first when that gate is on. Review ingress claims the
  delivery UUID, persists a durable `GitHubReviewFeedback` outbox row, and exposes
  worker-only provider truth and lineage checks through
  `apps/api/src/curie_api/routers/github_reviews.py`.

4. **Signed factory issue intake**: when `factory_enabled` (`factory.enabled`) is on,
  `apps/api/src/curie_api/routers/github.py::github_webhook` hands an `issues`
  delivery, or a plain issue comment, to
  `apps/api/src/curie_api/github_factory.py::handle_factory_delivery` after HMAC
  verification. It claims the delivery UUID, checks the installation, repository
  allowlist, and sender permission, and admits one canonical WorkItem without reading
  the issue body into the platform. It does not enqueue directly: the WorkItem
  reconciler later publishes the execute turn
  (`apps/api/src/curie_api/workitem_reconciler.py::WorkItemReconciler._publish_execute_wakes`).
5. **Factory poll intake** (ADR-0187):
  `apps/api/src/curie_api/factory_poll_intake.py::poll_once` is the default factory
  door. `factory_intake` (`factory.intake`) is `"poll"` unless an operator sets `"webhook"`. One
  pass lists labeled issues, mentions, and review feedback for each bound repository
  with the installation credential and admits through the same verification the
  webhook uses, without writing a delivery receipt. Cursors live in
  `curie.factory_poll_cursors`, and one API replica polls at a time under a Postgres
  advisory lock. It runs as a step of the API's WorkItem reconciler loop
  (`apps/api/src/curie_api/workitem_reconciler.py::WorkItemReconciler._reconcile_missed_labels`),
  every `poll_interval_s` of the GitHub tracker in `CURIE_FORGES` (45 seconds by default), and only when
  factory ingress is on. In poll mode no webhook secret is needed to boot.
6. **Factory missed-label reconciliation** (webhook mode only):
  `apps/api/src/curie_api/factory_label_reconcile.py::reconcile_missed_labels` lists
  open issues carrying the factory label on every bound repository and admits any with
  no WorkItem, through the same verification as the webhook, once the label is older
  than `factory_reconcile_grace_s`. GitHub does not redeliver a failed webhook,
  so without it a label is lost. It runs in the same step of the API's WorkItem
  reconciler loop in place of `poll_once`, at most every
  `factory_reconcile_interval_s` (300 seconds by default, 0 disables it), and
  only when factory ingress is on and `factory_intake` is `"webhook"`. In that
  mode the signed webhook remains the immediate intake path and reconciliation
  recovers labels whose delivery was missed. In poll mode this function does not run.
7. **Commit poll**: `apps/api/src/curie_api/commitpoller.py::CommitPoller.run_forever`:
  a timer in the API asks GitHub whether the deploy branches moved and hands any
  new commit to the same `process_push(...)`. Off unless
  `api.commitPollIntervalSeconds` is set. It exists because the webhook above is
  an INBOUND request, and a self-hosted cluster behind a firewall cannot receive
  one at all -- outbound always works (#1239).
8. **Generic HMAC hook**: `apps/api/src/curie_api/routers/hooks.py::ingest_hook`:
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
  refused with the same 401 as a bad signature. Source authentication also reads the
  current key, then verifies it again while holding the agent's source gate
  (`apps/api/src/curie_api/hook_source_auth.py::authenticated_source`). Protected policy
  selects its hook-specific generation key; ordinary or absent policy selects the agent's
  legacy generation key. A valid signature is not admission: this route accepts only a
  never-configured source, with neither a policy nor durable attempt history. Configured
  sources are refused, and history without a policy is refused as `pending_history`.
  Missing, unreadable, or invalid authority fails closed with 503. Gate liveness is
  rechecked before receipt, retry, and enqueue effects through
  `apps/api/src/curie_api/hook_source_auth.py::AuthenticatedHookSource.ensure_live`.
  An admitted route claims the delivery id and enqueues a `QueuedTurn` with `source=WEBHOOK`. The
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
  original value. They read a bounded retained receipt beside the delivery claim
  (`apps/api/src/curie_api/hook_receipts.py::read_receipt`), falling back to the
  original queued turn only when that receipt is absent for a legacy claim.
  Policy mismatch, missing evidence or malformed evidence returns 409; a malformed
  retained receipt never falls back to the queue. A pending restricted retry also
  returns 409; an ordinary pending
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

9. **Tracker intake poll**:
   `apps/api/src/curie_api/factory_tracker_intake.py::poll_trackers` polls every
   configured non-GitHub tracker binding through `Tracker.poll_marked` and
   verifies each notice through the tracker and code-host ports. It resolves
   the repository and base, then admits, revises or cancels through the shared
   WorkItem lifecycle. A Postgres advisory lock selects one replica; cursors
   advance only after notices are applied or deliberately refused. It runs from
   `WorkItemReconciler._poll_trackers` whenever factory ingress is enabled,
   independently of the GitHub intake mode.
10. **Code-host review feedback poll**:
    `apps/api/src/curie_api/factory_review_feedback.py::poll_review_feedback`
    reads open non-GitHub factory pull requests through
    `CodeHost.list_review_feedback`, checks the mention and feedback authority,
    then admits one revision per feedback-derived request id. The per-lineage
    cursor lives in Valkey. It runs after tracker intake in the reconciler's
    `finally` block, so a tracker poll failure does not suppress it; the
    binding tracker's poll interval bounds each lineage's reads.

The ten share no trigger abstraction: a Slack Bolt event listener, three paths
through a FastAPI GitHub HMAC route, five hardcoded timers, and a generic HMAC
route. The tracker and review timers use the existing `Tracker` and `CodeHost`
ports; they do not introduce a trigger port. The GitHub push
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

**Channel adapter ingress.** `POST /channels/turns` accepts binding-token
requests from out-of-process channel adapters, including Discord. Its
normalization, admission and reply contract lives in
[channel-ingress](../channel-ingress/INTERFACE.md), rather than adding another
hardcoded trigger kind here.

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
`ingest_hook` may admit a valid hook name whether or not the bundle declared it, but
only while its source has never been configured; the source-authentication fence above
still applies.

## Declared webhook wake contract (ADR 0274)

This section freezes the wire contract for the dependent API, operator and
worker slices. Declaration validation and the runtime declaration reader are
implemented; the routes, generation storage, admission, deploy receipts and
runtime credential-isolation proof described here still require those slices.
It does not claim that a declared wake can execute yet. Ordinary and protected
ingress retain their existing contracts.

### Declaration and credential role

Only a `webhook` with literal `wake: true` opts in. Its `name` is required,
matches `[a-z0-9][a-z0-9._-]{0,62}` in full, and is used exactly as written.
Its standing `prompt` must be nonblank and `target` must be absent, including
no null or blank value. `path` remains required and nonblank under #3666; it
may differ from `name` and does not select the wake route. Names remain unique
across all named triggers, including cron, using the existing stripped,
case-sensitive duplicate check. No new trigger type or generated prompt exists.

Omitted `wake` and literal `wake: false` retain path-only and prompt-bearing
webhook behavior. The optional model field defaults to false; null, strings
and numbers are refused. Unknown extension fields remain allowed. Previously
unmodeled, nonboolean `wake` values therefore become invalid. Cron selection
remains cron-only; a wake declaration creates no timer.

`plugin_format.validate.load_wake_triggers` shares the deploy trigger rules.
`apps/api/src/curie_api/bundles.py::read_manifest_wake_triggers` requires a
readable, well-formed manifest and returns only explicit wake declarations
keyed by exact name. It refuses invalid trigger lists and duplicate names,
including malformed sibling cron declarations, rather than silently returning
no authority. An invalid stored bundle must be corrected by deployment.
The existing raw cron reader is unchanged. The connector-build entrypoint
also validates wake declarations when a manifest exists, even without
`connectors.yaml`; malformed manifest JSON or trigger shapes now refuse that
build. Connector-only build directories may still omit a manifest.

The connector environment role is exactly `CURIE_HOOK_WAKE_KEY`. Deliver it
only through a hosted connector's `secrets` `SecretRef.from_secret`:

```yaml
connectors:
  queue-reader:
    image: ghcr.io/acme-corp/queue-reader:1
    env:
      WAKE_URL: https://api.example.com/hooks/00000000-0000-4000-8000-000000000001/queue-ready/wake
    secrets:
      - name: CURIE_HOOK_WAKE_KEY
        from_secret: acme-wake-credential
        key: key
```

Each connector instance holds the key matching its configured wake URL;
separate instances can hold
different hooks' keys using the same environment name and different Secret
references. Multiple wake declarations are permitted.

Both deploy validators refuse that role as an explicit or inferred
`bearer_secret`, including a same-named plain string secret. The role is also
refused in plain secrets, `secret_files`, `sealed_secrets`, `config`, direct
`env` assignment, remote connector secrets, or references in `url`,
`unhosted_url`, headers and bearer configuration. A separately named client
bearer and `no_client_auth` keep their meanings. The existing reserved
`CURIE_` fences for manifest secrets, API secret maps, worker injection and
Helm stay intact. No sealing path is introduced. Only authorized key retrieval
and the connector's own Secret delivery may expose the clear key; sandbox
environment, MCP headers/results, deploy receipts, logs and traces must not.
Renderer and sandbox-boot verification remains part of the later integration
slice, including the remaining half of ADR acceptance item 1.

### Wake request and exact signing bytes

The route is `POST /hooks/{agent_id}/{hook}/wake`. `agent_id` is a canonical
lowercase, hyphenated UUID; `hook` is the exact declaration name. There are no
query parameters. The body is exactly the two UTF-8 bytes `{}`. Whitespace,
additional keys and every other JSON shape are refused. Content-Type does not
select parsing or signing behavior. In particular the request never supplies
a target, prompt, tools, tool policy, reply coordinates or human identity.

Use the existing `apps/api/src/curie_api/hook_signing.py` header spellings:

| Header | Required value |
| --- | --- |
| `X-Curie-Timestamp` | 1 to 12 ASCII decimal digits for Unix seconds. Leading zeroes are accepted and signed verbatim. |
| `X-Curie-Delivery-Id` | 1 to 128 ASCII letters, digits, underscores or hyphens, freshly chosen per wake attempt. |
| `X-Curie-Signature-256` | `sha256=` followed by 64 lowercase hexadecimal HMAC-SHA256 digits. |

The server accepts a timestamp only when its absolute difference from server
Unix time is at most `hook_signing.TOLERANCE_S`, exactly 300 seconds, including
both boundaries. Missing or malformed headers have the same 401 as any invalid
credential. Compare signature bytes in constant time.

Let `C` be the ASCII encoding of compact JSON `[agent_id,hook]`, with no spaces
and ASCII escaping, equivalent to Python
`json.dumps([agent_id, hook], ensure_ascii=True, separators=(",", ":"))`.
Let `L` be the canonical ASCII decimal byte length of `C`. The signed material
is exactly this concatenation, with no final newline:

```text
ASCII("curie.hook.wake.v1") || 0x0a ||
ASCII(timestamp) || ASCII(".") || ASCII(delivery_id) || ASCII(".") ||
ASCII(L) || ASCII(":") || C || raw_body
```

`||` means byte concatenation. HMAC-SHA256 uses the UTF-8 bytes of the returned
derived key string, without decoding it from base64url. The signature header
is `sha256=` plus the lowercase hexadecimal digest. The exact body bytes,
timestamp spelling, delivery id, agent and hook are all bound. The wake prefix
is distinct from ordinary `curie.hook.delivery.v2` and protected support
signing; neither route's material can authenticate a wake, or vice versa.
There is no shared-agent-key or protected-source-key fallback.

### Key derivation and administration

The installation signing key is exactly `get_settings().api_key`, the same
setting used by `get_hook_secret`. Let `G` be the generation integer for this
agent and exact hook, bounded by `1 <= G <= 2^63-1`. Absent generation metadata
means generation 1; reading the key need not create a row. Rotation inserts
generation 2 when metadata is absent, or increments the stored generation
otherwise.
Its encoding is canonical ASCII decimal, with no sign or leading zeroes.
The derivation material is exactly:

```text
ASCII("curie.hook.wake.key.v1:") || ASCII(agent_id) || ASCII(":") ||
ASCII(hook) || ASCII(":") || ASCII(G)
```

Compute HMAC-SHA256 keyed by UTF-8 installation-key bytes over these bytes;
return the 32 digest bytes encoded as RFC 4648 base64url with all `=` padding
removed. Store only the generation. Never store or return the installation key.
The fixed vectors and independent test reference are in
`tests/vectors/hook-wake-signing.json` and
`packages/plugin-format/tests/test_hook_wake_signing.py`.

Both administrative operations use the same platform-key or live-console
authority as `apps/api/src/curie_api/routers/agents.py::get_hook_secret`,
with `Cache-Control: no-store` on their responses. A wake signature does not
authorize either operation. After administrative authentication, a nonempty
query string returns 400 `{"detail":"wake_query_forbidden"}`, including a
parameter with an empty value. Then any nonempty request body returns 422
`{"detail":"wake_body_invalid"}`. Both operations require zero body bytes;
even `{}` is refused.

| Operation | Success schema and exact fields |
| --- | --- |
| `GET /agents/{agent_id}/hooks/{hook}/wake-secret` | 200 `HookWakeSecretOut`: `{"agent_id":"<uuid>","hook":"<name>","generation":1,"secret":"<unpadded-base64url>"}` |
| `POST /agents/{agent_id}/hooks/{hook}/wake-secret/rotate` | 200 `HookWakeRotationOut`: `{"agent_id":"<uuid>","hook":"<name>","generation":2}` |

Every listed field is required; no extra fields are emitted. `agent_id` and
`hook` are strings, `generation` is an integer, and `secret` is a string.
Repeated reads reproduce the current key. Rotation increments generation under
the same source/admission gate and invalidates the old key immediately, with
no overlap. Its response carries no secret; use GET to retrieve the new key.
At the maximum generation rotation returns 503
`{"detail":"wake_generation_exhausted"}` without mutation. Rotation does not
cancel an admitted run or undo its effects.

Administration and admission require a current wake declaration and ordinary
source authority. Refuse a source policy, unresolved source history,
`remediation_policies` row, partition or source binding. Unavailable authority
fails closed. Administrative errors use `HookWakeErrorOut` below: missing
agent or wake declaration is 404 `wake_not_declared`; invalid stored bundle is
503 `wake_declaration_unavailable`. On both wake-key admin routes and the wake
route, existing source policy or unresolved history outcomes from
`apps/api/src/curie_api/routers/hook_fire.py::_require_ordinary_source` map to
409 `wake_source_not_ordinary`, including when that helper reports 503.
Only genuine metadata/source authority unavailability remains 503
`authority_unavailable`. Check declaration before source classification.
Administrative authentication failures retain the existing admin auth contract.

### Admission responses and ordering

These are the complete wake success schemas, with all fields required and no
additional fields. `run_id` is a UUID string naming the admitted hook run.
Coalescing exposes no run identifier, including when a replay's original run
has already settled.

An admitted wake stores `hook_runs.source` as `webhook`; operator run queries
report `trigger` as `webhook`.

| HTTP | Schema | Exact body |
| --- | --- | --- |
| 202 | `HookWakeQueuedOut` | `{"status":"queued","run_id":"<uuid>"}` |
| 200 | `HookWakeCoalescedOut` | `{"status":"coalesced"}` |

`HookWakeErrorOut` is exactly `{"detail":"<reason>"}`, with one required string
field and no other fields. Evaluate refusals in this order:

1. Before authentication, bound the streamed body using
   `Settings.hook_max_body_bytes` (default 1048576 bytes). Oversize is 413
   `{"detail":"hook body exceeds the maximum size"}` even without credentials.
   Parse canonical path identity and required headers, load wake generation
   metadata and verify the signature before reading the deployment bundle.
   Unknown agent/key, malformed path identity/header, invalid signature and
   stale timestamp all return 401 `{"detail":"invalid_wake_signature"}`.
   Unavailable metadata authority returns 503 `authority_unavailable`.
2. With valid authentication, a nonempty query string returns 400
   `wake_query_forbidden`, including a parameter with an empty value. Then a
   body other than exact `{}` returns 422 `wake_body_invalid`.
3. Resolve the ranked active deployment and exact explicit wake declaration.
   Absence returns 404 `wake_not_declared`; unreadable or malformed required
   bundle returns 503 `wake_declaration_unavailable`.
4. Recheck ordinary-source authority. Protected or configured source state,
   including existing source policy or unresolved history refused by
   `apps/api/src/curie_api/routers/hook_fire.py::_require_ordinary_source`, returns 409
   `wake_source_not_ordinary`. Only genuine authority unavailability returns
   503 `authority_unavailable`. Neither ordinary ingress nor protected
   ingestion is called as a fallback.
5. Pause returns 409 `hook_paused`, then kill returns 409 `agent_killed`, then
   existing configured-limit refusal returns 429 `budget_exhausted`. These
   guards run before delivery reservation, including before a duplicate could
   otherwise return `coalesced`.
6. Reserve the signed delivery id in Valkey with `SET NX EX`. An existing
   reservation returns 200 `coalesced`. Failed or uncertain reservation returns
   503 `wake_dedupe_unavailable`, with no hook-run mutation or stream append.
7. Under the per-agent/per-hook advisory lock, reclaim expired open claims and
   coalesce into any remaining live open run, whether queued or started. Only
   the dedupe reservation is written. Otherwise insert one open webhook run
   with its explicit claim lease, commit and enqueue. Return 202 `queued` only
   after enqueue succeeds. Known or uncertain enqueue failure returns 503
   `wake_enqueue_unavailable`, retaining the reservation; a known failure may
   close the row as `failed` / `enqueue_failed`. An uncertain append retains
   the open claim until lease recovery. No terminal close admits a successor.

Recheck current generation, source authority, pause, kill and limits under the
gate before run mutation. A generation mismatch returns 401
`invalid_wake_signature`. Source rechecks return 409
`wake_source_not_ordinary` or 503 `authority_unavailable` as in step 4; pause,
kill and limit rechecks return 409 `hook_paused`, 409 `agent_killed` and 429
`budget_exhausted`, respectively, as in step 5. Guard refusals create no run.
Enqueue failure is the explicit exception because the open
claim has already committed. Missing/invalid credentials are never separated
into observable unknown-agent versus wrong-key responses.

The dedupe key namespace is
`curie:hook-wake:v1:{installation_scope}:{agent_id}:{hook}:{generation}:{delivery_id}`,
where `installation_scope` is the lowercase hexadecimal HMAC-SHA256 digest
keyed by UTF-8 installation-key bytes over exactly
`ASCII("curie.hook.wake.scope.v1")`, without a trailing newline. Never use the
installation key or its raw SHA256 digest as the scope: for keys longer than
64 bytes that raw digest is an equivalent HMAC signing key. The scope label
is distinct from every credential derivation label. The namespace is separate
from ordinary ingress claims and scoped to current generation.
Retain the reservation strictly beyond the signed
timestamp plus 300 seconds. At receipt time `now`, its integer TTL in seconds
is at least `max(1, floor(int(timestamp) + 300 - now) + 1)`, permitting an
additional clock-skew margin. This includes accepted
future timestamps and the inclusive acceptance boundary. No permanent receipt
or follow-up state exists. A captured signed delivery remains coalesced after
settlement while its reservation exists. Publication failure retains it too.

### Deploy receipt and connector polling

The dependent deploy slice adds `DeployOutput.wake_triggers`, an array of
`HookWakeDeployReceipt` objects, with no credential:

```json
{"wake_triggers":[{"name":"queue-ready","path":"/hooks/00000000-0000-4000-8000-000000000001/queue-ready/wake","mode":"targetless"}]}
```

Each entry has exactly `name` (string), `path` (string) and `mode`
(literal `targetless`). The path is the exact route with the agent id and hook
name filled in. There is no absolute `url` field. The connector operator
prefixes the API base they reach the installation through. An empty
declaration set emits `[]`.
This array alone is not key-retrieval or runtime-isolation evidence.

The connector owns queue polling. Configure a bounded cadence appropriate to
the upstream service's rate limit, keep polling through run settlement, and
use exponential backoff with jitter for transient failures. An empty queue
poll sends no wake and creates no platform execution. Use durable item claims
and idempotent result writes: neither admission nor stream delivery provides
exactly-once external processing.

Every wake attempt, including the next attempt after timeout or non-2xx,
uses a fresh delivery id and current timestamp. Never reuse a signed delivery
as a progress retry. `queued` acknowledges admission, not queue completion.
`coalesced` means try again on the connector's next poll if work remains; it
does not promise that the live run has seen that work. Work arriving after a
run's final queue read waits for that next poll. Pause or kill requires
operator intervention; rotation requires reprovisioning the current key.
A lost publication can block until claim-lease expiry, followed by a fresh
poll and wake. No privileged run-query access, successor enqueue, automatic
channel reply or fabricated human is required.

## Implementations today

Ten hardcoded trigger producers in two different processes, plus the declared per-agent cron
scheduler in the worker:

1. Slack `app_mention` in the dispatcher (`apps/dispatcher/src/curie_dispatcher/handlers.py::process_event`).
2. GitHub `push` webhook in the API (`apps/api/src/curie_api/routers/github.py::github_webhook`).
3. Commit poll in the API (`apps/api/src/curie_api/commitpoller.py::CommitPoller.run_forever`),
   opt-in via `api.commitPollIntervalSeconds`. Timer-driven wake is therefore no longer
   entirely unbuilt: this one is real, though it is a single hardcoded platform timer. The
   per-agent declared `cron` is item 11.
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
9. Tracker intake poll in the API
   (`apps/api/src/curie_api/factory_tracker_intake.py::poll_trackers`), admitting
   non-GitHub tracker notices through `Tracker` and the shared WorkItem lifecycle.
10. Code-host review feedback poll in the API
    (`apps/api/src/curie_api/factory_review_feedback.py::poll_review_feedback`),
    admitting revisions through `CodeHost` for non-GitHub factory pull requests.
11. Declared cron triggers in the worker
   (`apps/worker/src/curie_worker/cron_loop.py::CronSchedulerLoop.run_forever`, ADR-0099, #268):
   each tick reads every in-force deployment's `cron` triggers, records the due slot in
   `hook_runs`, and enqueues one CRON turn. `GET /schedules`
   (`apps/api/src/curie_api/routers/schedules.py::list_schedules`, #2933) lists
   those hooks with independent newest scheduled and manual histories, selected
   by the persisted `hook_runs.source` (`schedule`, `manual`, or `webhook`). Scheduled
   `last_fire_at`, `last_outcome`, and `last_reason` exclude manual fires;
   `last_manual_fire_at`, `last_manual_outcome`, and `last_manual_reason` report
   the newest manual fire. `curie local schedules` and `curie cluster schedules`
   read that route. `curie local hook record <agent> <name> <id>` and its cluster
   counterpart read one persisted run, including an open or non-`ran` row,
   without waiting and exit 0 when found. Operator guide:
   [Cron triggers](../../guides/cron-triggers.md).

Plus three further wake paths that also enqueue a run without going through any of those
ten: the Slack block-action handler
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
`apps/dispatcher/src/curie_dispatcher/handlers.py::process_event` uses Slack's own `event_id`,
and `apps/dispatcher/src/curie_dispatcher/handlers.py::process_action` synthesizes
`action-<interaction id>`. Both pass their id through
`apps/dispatcher/src/curie_dispatcher/identities.py::delivery_key`, which keeps it
unchanged for the default identity and appends `:identity` for every other identity.
`apps/api/src/curie_api/resumequeue.py::resume_event_id` returns a
deterministic `approval-<id>-resolved`; GitHub review feedback first claims the delivery UUID
through `apps/api/src/curie_api/github_review_audit.py::claim_review_delivery`, then mints a
stable `github-feedback-<uuid5>` event id from repository id, event kind, and provider feedback
id in `apps/api/src/curie_api/github_review_events.py::UnverifiedFeedback.event_id`; factory
missed-label reconciliation derives its request id from GitHub's labeled event id so replicas and
repeated passes converge (`apps/api/src/curie_api/factory_label_reconcile.py`); and the CLI generates a random uuid behind an `EvSIM-`
prefix (`cli/src/queue.rs`), chosen expressly so it cannot collide with a real Slack `Ev...` id.
The forge paths centralize their request and feedback identities in
`apps/api/src/curie_api/forges/identity.py`: `notice_request_id`,
`reconcile_delivery_id`, `feedback_event_id` and `revision_request_id` retain
GitHub derivations and namespace other forges by their typed kind, host, scope
and object identity. Those helpers serve tracker intake and review polling,
while the dispatcher, resume and CLI paths retain their own schemes.
Idempotency across all producers therefore still depends on keeping those
schemes disjoint. Because there is
no trigger port, taking ownership of that rule would be a change to the shared turn contract,
not to any trigger.

## Cross-links

- **Epic(s):** #29, closed with the decision above: trigger is not a seam, and new triggers are new event kinds on the runs stream. Remaining work is tracked by #2935 (per-hook model, prompt and env), #2936 (bind hooks from the control plane), #2938 (a cron hook targeting a thread), and #3666 (map a declared webhook onto its hook).
- **Vision doc:** [architecture-vision.md](../../architecture-vision.md) — not one of the six swappable jobs; not separately graded.
- **ADR(s):** [ADR-0079](../../adr/0079-inbound-triggers-as-a-new-event-kind.md) (Accepted) — inbound triggers as a new event kind, ingested by the API; [ADR-0099](../../adr/0099-hooks-are-bundle-declared-turns-the-system-starts.md) (Accepted) — hooks are bundle-declared turns the system starts; [ADR-0187](../../adr/0187-the-factory-polls-github-and-the-platform-reads-the-issue.md) (Accepted): the factory polls GitHub for its work, and the webhook becomes optional.

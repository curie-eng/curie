# apps/worker

The worker is three parts: the concurrency kernel (routing rule, finish-race CAS (compare-and-swap), steer/interrupt, no-retry-after-side-effects, resume-rehydrate), the Agent Sandbox substrate module (warm pool, thread-to-sandbox affinity, claim/release), and the eval runner module. Reads Valkey Streams via redis-py consumer groups; drives claimed sandboxes running the runner image.

## Deployment binding + kill switch (`curie_worker.binding` + `curie_worker.killswitch`)

Wires the kernel to the deployment tables and the kill switch. Both are
optional on the kernel: absent, it runs a generic sandbox (the kernel's default
behavior); present, it binds per-channel and gates killed agents.

**Binding** (`binding.py`): on each event, resolve `channel -> agent -> active
deployment -> version` with one read-only SELECT over the API, git-flow, and kill-switch Postgres tables
(a thin query layer, not the API's ORM (Object-Relational Mapper), to avoid pulling FastAPI into the worker).
Prod wins over dev, then most recent. The kernel claims the sandbox with a boot
env built from the resolution: `CURIE_BUDGET` (the agent's
`max_usd_per_day`/`max_output_tokens_per_run`, platform defaults when NULL),
`CURIE_SESSION_ID`, `CURIE_PLUGIN_DIR`, and
`CURIE_BUNDLE_REF` (the RustFS key). An unmapped channel is a polite placeholder
edit and drop, never a crash. The boot env also carries a per-sandbox
`CURIE_RUNNER_TOKEN` (minted with `secrets.token_urlsafe`) that the `RunnerClient`
sends as an `Authorization: Bearer` header on every ACI call to that sandbox
(issue #63). On Kubernetes this token and the other scoped tokens never ride the
`SandboxClaim`: the worker writes them to a per-claim Secret that a per-claim
SandboxTemplate copy reads by `secretKeyRef`, all garbage-collected with the
claim and swept by the reaper if orphaned. The Docker substrate still passes them
in the container env.

> Handoff: `CURIE_BUNDLE_REF` is a RustFS object key; the runner reads
> `CURIE_PLUGIN_DIR` as a local mounted path and does not fetch. Fetching the
> bundle key into the plugin dir is sandbox provisioning (an init container in the
> sandbox substrate's SandboxTemplate / the chart), owned there, not by the worker.

**Kill switch** (`killswitch.py`): subscribes to the kill-switch Valkey channel
`curie:kill-events`; on `kill` for an agent it interrupts that agent's live
turns (a run registry maps agent -> active threads). New runs are refused while
the flag `curie:kill:<agent_id>` is set - the kernel checks the flag before
opening a turn, which also covers a kill event missed while the subscriber was
down. `resume` clears the flag (API-side); no worker action needed.

Tests (`apps/worker/tests/binding` + `tests/kernel/test_binding_integration.py`):
the resolver against the real compose Postgres (channel resolution, prod
preference, unknown -> None, budget/env); the kill switch against real Valkey
(flag gate, subscriber dispatch); and kernel-level behaviors (unmapped drop,
boot-env on claim, killed-agent refusal, kill interrupts a live turn).

### Named cluster-message canary routing

The cluster-message relay delivers a Slack-kind turn whose reply handle uses
`adapter=curie-cluster-message` for egress. Its optional `identity` selects the
Slack binding independently of that delivery adapter (INGRESS-CANARY-1).

- **WORKER-CANARY-1:** An absent identity selects the `default` Slack binding.
  A declared named identity such as `sre-bot` selects only the binding with
  that identity on the same channel. Ordinary Slack turns continue to select
  their binding by `adapter`.
- **WORKER-CANARY-2:** An unknown identity returns no binding. An empty or
  malformed identity is refused before a sandbox claim. None of these cases
  may fall back to `default`, including the bound-but-undeployed diagnostic.
- **WORKER-CANARY-3:** A named relay turn's internal thread key includes its
  selected identity. A default and named turn on one channel and conversation
  cannot adopt each other's sandbox, history, lock, or approval state.
- **WORKER-CANARY-4:** Resolved and bound-but-undeployed relay replies keep
  `curie-cluster-message` as their egress adapter. The binding's Slack identity
  never replaces the relay; ordinary Slack reply routing is unchanged.
- **WORKER-CANARY-5:** Active deployment selection for the selected binding
  retains prod-over-dev and most-recent ordering. A stale or undeployed named
  route cannot run or answer as a different identity.

### Per-turn tool access

The worker's half of TOOL-ACCESS in
[the ACI producer seam](../../docs/interfaces/aci-producer/INTERFACE.md), for a
queued turn whose `tool_access` is set (a canary sets `read-only`):

- **WORKER-TOOL-ACCESS-1:** The worker forwards `QueuedTurn.tool_access`
  unchanged as `Event.tool_access` on the turn it opens. A turn without it opens
  exactly as before, with no extra runner call.
- **WORKER-TOOL-ACCESS-2:** Whichever path opens a restricted turn on a
  runner (a fresh claim, a replacement, an attachment handoff, a work-item
  continuation), the worker first reads the status of that runner, from that
  sandbox's own address with its own token when it has one, and opens the
  turn only when the value is listed under `tool_access`. A status that
  answers without it means the runner would run the turn unrestricted, or
  cannot enforce it on this session, so the model is never asked: the turn
  fails once, escalated with the class `tool-access-unenforced` and the text
  `This agent cannot start: its runner cannot enforce read-only tool access
  for this turn, so the turn was not run.`, and is not retried. A status that
  cannot be read, or is not a JSON object, opens nothing and is retried like
  any turn the runner did not accept. The read is bounded to two seconds.
- **WORKER-TOOL-ACCESS-3:** A restricted turn never steers a live turn: when
  the thread has one, it is not started and the delivery stays pending for a
  later redelivery, as a job's does (ADR-0079), and a capacity wake re-parks
  it instead. It never takes a greeting or help pack's canned reply, which
  would answer it without a runner.
- **WORKER-TOOL-ACCESS-4:** The worker never creates an approval from a
  `read-only` turn and never delivers an approval grant to its boot. A runner
  final that nonetheless ends `awaiting-approval` records no approval and
  posts no card; the turn is a failed turn, escalated with the reply `This
  read-only turn asked for an approval, which it may not do. No approval was
  created.`
- **WORKER-TOOL-ACCESS-5:** The runner's own refusal classes for a restricted
  turn, `tool-access-unenforced` and `tool-access-refused`, are platform error
  classes: a turn the runner refuses escalates under its class, never as
  `unclassified`.

The worker's half of channel read (ADR 0100, ADR 0200, `kernel/channel_read.py`),
for a bundle whose manifest grants any of `channelRead`, `canvasList`,
`canvasRead` or `canvasEdit`. The worker is the only issuer
of the capability the runner's `curie-slack` tools use:

- **WORKER-CHANNEL-READ-1:** At turn open, on every path that opens a turn (a
  fresh claim, a replacement, an attachment handoff, a work-item continuation),
  the worker reads the grant from the manifest of the bundle the sandbox booted
  with, cached by bundle ref, and mints a `chr` capability through the API's
  internal context route. An ungranted bundle makes no mint call and the event
  carries a null `channel_read`. A retained sandbox keeps the deployment it
  first booted with: a pin names the claim and deployment, and when the pin is
  missing the claim's own bundle ref must equal the resolved one to recover it,
  otherwise the turn gets no capability.
- **WORKER-CHANNEL-READ-2:** A granted turn on a runner whose status does not
  carry `channel_read` as literal `true` is refused once, escalated with the
  class `channel-read-unenforced`, and not retried, whatever the mint outcome.
  When the grant cannot be read from the bundle and the mint also fails, the
  worker cannot tell whether enforcement is owed, so the turn is not run and is
  retried like any turn the runner did not accept (`runner-error`).
- **WORKER-CHANNEL-READ-3:** The capability is a lease. The API's active key
  lives 90 seconds, and a heartbeat child of the attempt renews it every 30
  seconds with an owner-checked refresh until the attempt ends, so a worker
  that dies or loses Valkey lets it lapse within one lease. A steer renews
  the live logical turn (same deployment, event and default channel, no owner)
  only when the retained runner reports a live turn and advertises channel
  read; any failure sends null, which clears the runner's credential.
- **WORKER-CHANNEL-READ-4:** Every attempt end, including cancellation,
  deletes the live record, tombstones the attempt's owner (so a mint the API
  commits late is refused), and revokes by owner directly in the shared Valkey,
  covering a mint whose answer was lost. A failed revoke is retried with
  bounded backoff until the token's expiry and attempted again at shutdown.

## The eval lane (`curie_worker.eval`)

Runs an eval suite against a plugin version and records the grid the eval matrix
and PR check read.

```
EvalSuite (cases: input + grader)
   -> EvalRunner: deliver each case as an ACI `eval_case` event over the runner's
      HTTP channel (the kernel's RunnerClient), take the `final` text as the answer
   -> Grader: exact | contains | regex (on the answer text) | tool_called (on the
      turn's tool-call trajectory) -> pass/fail (deny-by-default; a case must
      name a grader)
   -> EvalRunResult: per-case rows + the "N/M passed" summary
   -> LangfuseEvalRecorder: a trace + `eval_pass` score per case, tagged
      `version:<sha>` and `suite:<name>`, via the Langfuse ingestion API
```

Run a Job with `python -m curie_worker.eval`; it loads a suite JSON, runs it
against a runner endpoint, records to Langfuse if configured, prints the
`EvalRunResult` as JSON (for the PR-check reporter), and exits non-zero if any
case failed (so the Job / GitHub check reflects the result).

Env: `CURIE_EVAL_SUITE` (suite JSON path), `CURIE_EVAL_TARGET_URL` (runner
base_url), `CURIE_EVAL_VERSION` (version/sha tag, default `local`),
`LANGFUSE_HOST` / `LANGFUSE_PUBLIC_KEY` / `LANGFUSE_SECRET_KEY` (record scores
when all set).

**Handoffs (not in apps/worker):** the API's eval **matrix endpoint** reads the
grid back from Langfuse filtered by the `version:` tag (a trace's `eval_pass`
score is 1.0/0.0; query traces by tag, read each trace's score); the **PR-check**
reporter (the git-flow engine) turns the printed `EvalRunResult` summary into a GitHub commit
status; the **UI** matrix tab renders the grid; the **Job fan-out** per version @
sha on a PR webhook is the API's (a Job template runs this module). Per-case
sandbox isolation (a fresh sandbox per case) is that Job-orchestration layer's
choice; `EvalRunner` runs a suite against one endpoint.

Tests (`apps/worker/tests/eval`): graders and rollups (unit); `EvalRunner`
against a scriptable fake runner; `LangfuseEvalRecorder` against the REAL compose
Langfuse (record + read back by version tag, never mocked).

## The eval-stream consumer (`curie_worker.eval.stream`)

Where the eval lane is the eval Job entrypoint (run one suite against one endpoint), the eval-stream consumer is the
long-running worker that turns a queued eval request into a full run. It is a second
consumer group (`curie-eval-workers`) on a distinct Valkey stream `curie:evals`,
running on its own connection so its blocking read never stalls the runs consumer.
The API's git-flow engine is the producer; build against this written contract, not its
code.

```
XREADGROUP curie:evals            EvalStreamConsumer (own consumer group)
   -> payload: EvalJob JSON            one stream field `payload` (dispatcher seam)
   -> BundleStore.get(bundle_ref)      RustFS GET, extract, load evals/cases.json
   -> run_eval_suite(target)           target_url shortcut, else provision a runner
   -> LangfuseEvalRecorder.record      per-case eval_pass scores, tagged version:<sha>
   -> POST /evals/report               platform API (repo, sha, passed_count, total)
   -> XACK                             only AFTER the report POST attempt completes
```

Stream seam (the exact producer contract): each entry carries one field `payload`
holding an `EvalJob` JSON object (the shared `aci_protocol.EvalJob` model) with
`agent_id`, `version_id`, `sha`, `suite`
(the suite NAME, used to select/tag, not the cases themselves), `bundle_ref` (the
RustFS object key), optional `target_url`, and `requested_at` (ISO-8601 UTC). The
cases come FROM the bundle's own `evals/cases.json` (the `EvalSuite` JSON shape the
eval lane's loader reads), never from the stream.

Runtime rules (each has a provoking integration test in `tests/eval/test_stream.py`):

- **Suite loads from the bundle.** RustFS GET `bundle_ref` from bucket
  `curie-bundles`, extract, load `evals/cases.json`; the `suite` field renames it
  and tags Langfuse. A missing/corrupt bundle or missing evals dir is a **failed run**
  (0/0) reported and acked, never a crash.
- **`target_url` present -> eval it directly** (the dev/test shortcut). Absent ->
  **provision a runner via the sandbox substrate** (the same warm-pool `claim` chat runs
  use, boot env carrying `CURIE_BUNDLE_REF` + budget), eval against it, and tear it
  down in a `finally`. A provisioning failure is a failed run reported and acked.
- **XACK only after the report POST attempt completes** (success, or terminally failed
  after bounded retries and logged). A worker crash before that leaves the entry
  pending. The shared runs/eval liveness path promptly transfers a capable dead
  consumer's PEL after two lease-absence observations. A live consumer's own
  stranded row (delivery state present, delivery lease expired) is separately
  recovered by the lease-expiry pass after about one lease TTL. Unknown older
  consumers, or an entry with no delivery state at all,
  retain the 15-minute `XAUTOCLAIM` fallback. A restarted generation first
  recovers rows under its own stable consumer name. The entry is re-run, so
  delivery is **at-least-once** with a best-effort
  report. A malformed payload cannot be processed on any redelivery, so it is logged
  and acked (a poison-pill drop). A failing eval case is a failed COUNT in the report,
  not a consumer crash.

Config surface (read by `WorkerConfig`): `CURIE_EVAL_STREAM` /
`CURIE_EVAL_CONSUMER_GROUP`; RustFS/S3 `S3_ENDPOINT_URL` / `S3_ACCESS_KEY` /
`S3_SECRET_KEY` / `S3_REGION` / `BUNDLE_BUCKET` (mirroring the API's env names); the
platform API `CURIE_API_URL` (deprecated alias `CURIE_API_BASE_URL`) / `CURIE_API_KEY` for `POST /evals/report`; and
`LANGFUSE_HOST` / `LANGFUSE_PUBLIC_KEY` / `LANGFUSE_SECRET_KEY` for score recording.
The consumer is wired into `python -m curie_worker` alongside the runs consumer and
the kill switch.

Tests (`apps/worker/tests/eval/test_stream.py`, real Valkey + real RustFS bundle + real
Langfuse, only the report POST mocked): the seam cycle (XADD the exact payload ->
one consume->eval->report), the poison-pill drop, a missing-bundle failed run,
ack-after-report even when the report terminally fails, and a provisioned-runner
end-to-end (no `target_url`) that boots via the substrate and releases in a finally.

## The concurrency kernel (`curie_worker.kernel` + `curie_worker.consumer`)

The kernel closes the loop: it consumes `QueuedTurn` entries the dispatcher
puts on the `curie:runs` Valkey stream, routes each to a runner turn in a
claimed sandbox, streams the NDJSON (newline-delimited JSON) reply back into
the Slack thread by editing the placeholder in place, and gets every failure
mode right.

```
XREADGROUP curie:runs        Consumer (consumer group; a pending entry is
   -> QueuedTurn                  reclaimed by proof of death, by lease expiry,
   -> Kernel.process_event        or as a compatibility backstop by XAUTOCLAIM
                                  after an idle timeout, then reprocessed
                                  idempotently)
        -> SlackSink     chat.update placeholder to booting text (best effort)
        -> substrate.lookup/claim/resume   (sandbox substrate)
        -> RunnerClient  POST /v1/event | /v1/steer | /v1/interrupt   (runner)
        -> SlackSink     chat.update the placeholder as frames stream
   -> XACK
```

A targeted cron turn posts one message containing the final reply and does not
show the booting caption or stream partial edits.

Rules (detailed-architecture 2b), each with an integration test that provokes it
(`tests/kernel/`):

- **One live session per thread.** A follow-up to a thread with a live turn is a
  *steer* (`POST /v1/steer`) into that turn, not a new turn. The per-thread lock
  (Valkey `SET NX PX` across workers, plus an in-process FIFO lock for arrival
  ordering) wraps the route decision, and the new turn is opened *before* the
  lock is released, so a concurrent follow-up can only steer, never fork a second
  turn. The lock is not held during streaming, so steering is never blocked.
- **The finish race.** A steer that arrives as the turn ends returns 409; the
  kernel then opens a fresh turn on the same idle sandbox. This check-and-fall-
  back is the compare-and-swap the worker owns.
- **Steer vs interrupt.** Default is steer. `Kernel.interrupt_thread` is the
  explicit hard stop (`POST /v1/interrupt`), which a Slack `:stop:` affordance
  would call; the kernel never keyword-guesses intent.
- **No auto-retry after side effects.** A failed run that emitted
  `side_effect_flag` escalates to a human (the placeholder is edited to say so)
  instead of retrying. The flag is persisted to Valkey the instant it is seen, so
  a worker crash mid-side-effect still escalates on reclaim rather than re-running
  a non-idempotent action. For noncron turns, flag-clean failures retry by
  classification:
  `rate-limit`, `runner-error`, `runner-timeout`, `sandbox-capacity`,
  `sandbox-terminated` and `workspace-error` are transient (bounded exponential
  backoff);
  `budget-exceeded` and everything else escalate.
  `runner-timeout` is the runner's streaming budget expiring mid-turn (#2011),
  told apart from `runner-error`, which is a plain transport failure without
  confirmed sandbox termination. When Kubernetes confirms that the sandbox pod
  terminated during the stream, the worker classifies the failure as
  `sandbox-terminated`; the terminal notice includes the Kubernetes termination
  reason so an operator can diagnose it before retrying.
  `workspace-error` is a managed-workspace preparation FAULT before the turn was
  ever accepted (#2004): the clone, the archive, or the upload. It is told apart
  from `runner-error` for the same
  reason, and it always carries a `workspace start failed` WARNING naming the
  agent, the deployment, the repository the turn asked for and the stage that
  failed -- such a turn used to ack, create no sandbox and log nothing at all.
  A deliberate repository-selection refusal is the other half of that split and
  is NOT this: it is a decision rather than a fault, so it stays terminal,
  answers the user, and logs at INFO instead. A turn that names a repository
  by github.com URL while the coordinator is off for the worker is such a
  refusal (#2659). A bare `owner/repo` token is only a guess (#2947) and with
  the coordinator off there is no allowlist to confirm it, so it names no
  repository and the turn stays generic, on a new thread and on a retained
  route alike (#3671). Generic
  turns continue through the normal claim path while it is off. A retained live
  or suspended route that already has a repository workspace and verified review
  feedback are also terminal refusals, because both require repository authority.
  The disabled lane does not consult repository selections or webhook operator
  mappings held only by the server. A turn with no repository message, verified
  review, or retained route that carries a repository stays generic and runs
  without a workspace.
  A turn that attaches a workspace because its own message named the repository
  ends its reply with one platform line naming that repository, placed after the
  model's answer and before the receipt or the awaiting-approval notice.
- **Idempotency + crash recovery.** The Slack event id gates a `done` marker, so
  a redelivered or reclaimed entry that already finished is skipped.
  A renewable worker lease distinguishes process death from ordinary consumer
  idle. After two absent observations, one replacement wins a Valkey arbitration
  lease and transfers the dead consumer's pending entries without racing other
  replicas through the delivery budget. A delivery whose handler raised released
  its lease and left its entry pending under a consumer that is still alive; the
  lease-expiry pass recovers that row after about one lease TTL
  (`CURIE_LEASE_EXPIRED_IDLE_MS`) rather than leaving it to the backstop.
  Unknown older consumers, and any entry with no delivery state, retain the
  15-minute `XAUTOCLAIM` fallback; a restarted generation also recovers pending
  rows left under its own stable consumer name. The markers make reprocessing safe.
- **Bounded delivery + a dead-letter graveyard** (ADR-0039, an Architecture
  Decision Record; #505). Reclaim is
  capped, not infinite. An entry already delivered `max_delivery` times
  (`CURIE_MAX_DELIVERY`, default 5, floor 2) and still failing is moved to a
  dead-letter stream and acked off the group instead of re-dispatched, so it can
  never be reclaimed again. Without the cap one permanently-failing entry -- for
  example a turn whose reply endpoint died with the process that created it --
  is reclaimed forever, starves the shared consumer group, and silently stalls
  every later turn. **Transient failures are unaffected:** a worker that crashes
  mid-turn still has its entry reclaimed, retried, and acked exactly as before;
  only an exhausted delivery budget dead-letters.
- **Exception: best-effort reply on an approval-resume turn, pure-offline
  loop** (#708). The scenario above is exactly an approval-resume turn whose
  reply endpoint (the CLI's throwaway stub) died with the process that
  created it. When there is genuinely no default Slack transport configured
  (`self._default_base_url is None`), that unreachable reply no longer
  dead-letters the resume: the kernel marks the turn `best_effort` and
  `AsyncSlackSink._with_transport_fallback` logs and swallows the
  unreachable-endpoint error instead of raising, so the already-granted tool
  call still executes and the turn ACKs (the reply is still captured in the
  transcript, just not delivered). A reply over a *configured* default
  transport, and any non-resume turn, still raises on an unreachable
  endpoint and follows the normal retry/dead-letter path above.

### Turns between sibling identities

A turn whose author is one of this installation's own identities counts against
two fixed-window Valkey counters (ADR-0168 decision 6), under
`<key_prefix>:sibling:`. On Slack, that author is an identity's bot user from
`auth.test`. On the channel port, it is the address a binding is bound at. One
counter is kept per session key, admitting 5 sibling-written turns
(`SIBLING_TURN_LIMIT`), and one per ordered identity pair, admitting 5
conversations the pair opens (`SIBLING_OPEN_LIMIT`), both in a 600 s window
(`SIBLING_WINDOW_SECONDS`); these are `curie_worker.sibling_turns` constants,
not configuration. Past them the kernel logs
`dropping event <id> from a sibling identity: sibling_conversation_limit` (or
`sibling_pair_limit`). On Slack it edits the placeholder with a notice that
mentions nobody, and on every kind it completes the turn as dropped. An install
with one Slack identity and at most one adapter builds none of this.

The counters count delivery attempts, not events: `check()` takes no event id,
so a non-terminal redelivery of a turn already counted -- a binding lookup that
raised, a lock lost mid-turn, a worker crash after the check but before the
turn finished -- increments both counters again on retry. A legitimate sibling
exchange can therefore be cut short a little early during an outage that
redelivers it. That is consistent with failing safe: the counters undercount a
conversation's true budget, never let one run longer than intended.

### The dead-letter graveyard

The dead-letter stream is `CURIE_DEAD_LETTER_STREAM`, or `<stream>:dead`
(`curie:runs:dead`) when that is unset. Setting it **equal to** `CURIE_STREAM`
is rejected at startup: the worker XADDs to the graveyard before it XACKs, so a
self-targeting graveyard would re-queue every failure onto the stream it was
consumed from and hot-loop on an unparseable one. The derived default can never
collide, so only an explicit override trips this.

The API-side graveyard watcher now honors the same `CURIE_DEAD_LETTER_STREAM` /
`CURIE_STREAM` override via the shared derivation, so the operator and the API
agree on the graveyard stream name with no manual sync.

The first `XADD` creates the stream; nothing pre-creates it. It is a sink, **not**
a second worker processing lane: it has no consumer group, and no worker
replays its rows. The API graveyard watcher observes it read-only, while the
resume reconciler acts only on matching resume rows. Stream-consumer rows can
be inspected with `XRANGE` and, if an operator wants to replay one, re-`XADD`ed
onto the main stream. Completion-outbox rows are terminal completion records,
not inbound stream entries, and must not be replayed onto the main stream.

**The graveyard is bounded, and its rows are best-effort.**

- Every `XADD` passes an approximate `MAXLEN` of `CURIE_DEAD_LETTER_MAXLEN`
  (default `10000`, minimum `1`), so under a flood the oldest rows are
  evicted and those failures are lost.
- That loss is deliberate: the unparseable path dead-letters per **inbound**
  entry, so a wire-format drift would otherwise grow the graveyard at full
  ingest rate on the same Valkey that holds the kernel's per-thread locks and
  side-effect markers, i.e. a platform-wide OOM. Bounded record loss is
  traded against that. Do not treat the graveyard as a durable audit log.
- Because the trim is approximate (Valkey trims on node boundaries), the
  stream is bounded at *at least* the configured length, not exactly it.
  Below the cap it holds every dead-lettered entry, and its length reads the
  system's poison rate, saturating rather than growing once the bound is
  hit.

The graveyard has three row families. Stream-consumer rows carry the original
entry's fields verbatim plus namespaced failure metadata, so a human or replay
tool can inspect exactly what inbound entry died and why:

| Field | Meaning |
|---|---|
| `dl_original_id` | the entry's id on the source stream |
| `dl_delivery_count` | deliveries made before it was given up on |
| `dl_reason` | `max-delivery-exceeded`, `unparseable`, or `broker-entry-vanished` |
| `dl_dead_lettered_at` | UTC ISO-8601 timestamp |

Completion-outbox rows are written by `Markers.dead_letter_completion` only for
the exact `DeletedReplyTargetError` deleted-thread classification. They carry
`event_id`, the
serialized `completion`, `dl_reason="thread deleted at provider"`,
`dl_delivery_count="1"`, `dl_source="completion-outbox"`, and
`dl_dead_lettered_at`; they have no `dl_original_id` and are not replayable as
inbound stream entries. Every other completion delivery failure leaves the
completion owed in the outbox for re-emission.

Progress-outbox rows are written by `ProgressStore.dead_letter` for a progress
delivery whose attempt budget is spent. They carry `delivery_id`,
`progress_id`, the stored `event`, `dl_reason="max-attempts-exceeded"`,
`dl_delivery_count` (the attempts made), `dl_source="progress-outbox"`, and
`dl_dead_lettered_at`. Like completion-outbox rows they have no
`dl_original_id` and are not replayable as inbound stream entries; see
[Deliberate progress (ADR 0130)](#deliberate-progress-adr-0130).

The `dl_` prefix keeps the stream-consumer metadata namespaced, but the
unparseable path stores
an arbitrary, malformed field map verbatim, so an original field could itself be
named `dl_something` and collide with one of the four keys above.
`Consumer._dead_letter` escapes any original key already starting with `dl_`
by doubling the prefix (`dl_reason` in the original becomes `dl_dl_reason` in
the row) before writing the metadata last. The escape is injective: an escaped key
always starts with `dl_dl_`, so it can never collide with the metadata, and
un-escaping strips exactly one leading `dl_`. The API graveyard watcher and
resume reconciler read these rows; any reader of stream-consumer rows must strip
one leading `dl_` to recover an original field whose name collided, or it will
silently misread it. Completion-outbox rows do not use this escaped original-
field convention.

An entry that is pending while its message has been trimmed off the source
stream produces a metadata-only stream-consumer row and is still acked. Every
stream-consumer dead-letter emits a loud error log naming the entry, its
delivery count, the reason, and the target stream.

Two behaviors worth knowing before changing this path. The delivery count is read
from Valkey's pending-entries list on every pass rather than tracked in worker
memory, so a restarted or replacement worker still sees the accumulated count and
still caps; a process-local counter would reset on restart and let a crash-looping
worker retry poison forever. And the `XADD` to the graveyard happens before the
`XACK`, so a crash between the two costs a duplicate graveyard row rather than a
lost entry. Two replicas racing the same over-cap entry produce the same
acceptable duplicate.

**Unparseable entries take the same route** (`dl_reason="unparseable"`) instead of
being silently acked away, so poison is observable rather than vanishing.

**`CURIE_MAX_DELIVERY` is not `CURIE_MAX_ATTEMPTS`.** The delivery cap bounds
how many times a stream entry may be handed to a handler; `max_attempts` governs
the kernel's flag-clean per-turn retry classification *inside* a single delivery.
The floor of 2 is enforced because `max_delivery=1` would dead-letter every
ordinary worker crash on its first reclaim; values below 3 undermine the crash
recovery of ADR-0013.

Config surface (`WorkerConfig`): `VALKEY_*`, `SLACK_BOT_TOKEN`,
`CURIE_STREAM` / `CURIE_CONSUMER_GROUP` / `CURIE_CONSUMER_NAME`,
`CURIE_WORKER_MAX_CONCURRENCY` (turns one worker runs at once, default `16`,
`1` through `256`; chart `worker.maxConcurrency`),
`CURIE_MAX_ATTEMPTS`, `CURIE_MAX_DELIVERY` / `CURIE_DEAD_LETTER_STREAM` /
`CURIE_DEAD_LETTER_MAXLEN` (approximate graveyard cap, default `10000`, minimum
`1`), `CURIE_LEASE_EXPIRED_IDLE_MS` (the lease-expiry reclaim threshold, default
one delivery lease TTL), `CURIE_TURN_NOT_STARTED_TEXT` (the placeholder edit
when a delivery's handler raises), `CURIE_TURN_RECEIPT` (what the receipt
beneath a reply shows: `all`, the default, `failures` or `off`; ADR-0180) and
`CURIE_PROGRESS_RENDER` (deliberate progress rendering, off; see
[Deliberate progress (ADR 0130)](#deliberate-progress-adr-0130)),
plus `CURIE_NAMESPACE` / `CURIE_WARM_POOL` / `CURIE_RUNNER_PORT` for the
substrate. Run with `python -m curie_worker`.

<!-- @spec WORKER-RECEIPT-1 -->
A receipt must explain an irreversible action without exposing generic runner
bookkeeping such as `non-idempotent tool completed`, `non-idempotent tool executed`
or `tool result too large to record`. When a successful action has no prior state
and only that generic detail, show that nothing reported a prior state. Keep the
call visible; this does not classify the tool as read-only or change the action
ledger, retry latch, receipt mode, or Bash grouping. A meaningful connector
explanation, a failed-action warning, and an undoable-action verdict take
precedence over this fallback.

For successful native instruction or shell requests with no meaningful summary,
no undo capability and only generic runner detail, use plain request-completion
wording. State that changes were not summarized and undo information is
incomplete; do not infer that no prior snapshot exists from undo capability
alone. Preserve counts and every stored action. Do not suppress instruction
requests or promote them to read-only: loading one can execute dynamic context.
Custom descriptions, summaries, failed warnings and undoable verdicts retain
their existing meaning.

<!-- @spec WORKER-RECEIPT-2 -->
Every receipt description uses plain action wording, including failed native
requests, MCP calls, and connector-provided summaries and details. Strip the
MCP namespace, split identifier words, and describe the action without inferring
success; an absent or invalid identifier becomes `action`. Replace identifier
references in presentation metadata while preserving surrounding content and
restore/failure meaning. Stored ledger identifiers and arguments remain unchanged.
An MCP tool named like a native request is still a connector action, never
suppressed or treated as a native completed request.

Tests: `uv run pytest apps/worker/tests/kernel -q` runs against the real Valkey
from `compose.dev.yaml`, the real sandbox substrate with a fake Kubernetes client whose
sandboxes resolve to a local in-process fake runner, and a recording Slack sink.
Only Slack and the model behind the runner are faked.

### Settled stream retention (ADR 0184)

A supervised `stream-retention` loop trims `CURIE_STREAM` and
`CURIE_EVAL_STREAM` every `CURIE_STREAM_RETENTION_INTERVAL_S` (default 60).
`apps/worker/src/curie_worker/stream_retention.py::trim_settled` runs one
script that reads every consumer group on the stream and runs
`XTRIM MINID` at the lowest of each group's oldest pending id (or the id after
its `last-delivered-id` when nothing is pending) and now minus
`CURIE_STREAM_RETENTION_MIN_AGE_S` (default 86400, bounded 3600 to 31536000,
chart `worker.streamRetention.minAgeSeconds`).

- Pending and undelivered entries are never trimmed, whatever their age, so a
  reclaim always finds the body it re-runs.
- A stream with no consumer group is left alone. The graveyard, progress and
  marker streams have no group and keep their own approximate `MAXLEN`.
- A group that stops acknowledging holds the floor for its stream, so memory
  grows rather than a turn being lost. `curie.queue.depth` is the runs
  stream's `XLEN` and shows it.
- Producers never pass `MAXLEN` on a consumed stream; a producer cannot tell a
  settled entry from a pending one.

### Running the worker as a bare process (`curie_worker.run`, Docker substrate)

`curie_worker.run` is the `python -m curie_worker` entrypoint: it reads
`WorkerConfig`, builds the Valkey clients, the sandbox substrate, the
`RunnerClient`, and the Slack sink, then drives the consumer until a signal
stops it. `_sandbox_client` picks the substrate via `CURIE_SANDBOX_SUBSTRATE`
(`kubernetes`, the default, claims agent-sandbox CRs; `docker` boots runner
containers on the host Docker daemon instead -- what `curie local up` sets
when it runs the worker as a compose service).

To hand-run the worker as a bare host process against an already-running
`compose.dev.yaml` stack instead of letting `curie local up` manage it as a
compose service -- useful for attaching a debugger or iterating on worker
source without a container rebuild -- point it at the stack's exposed ports:

```bash
export CLAUDE_CODE_OAUTH_TOKEN=...        # or ANTHROPIC_API_KEY=... / CURIE_CREDENTIALS=...
env CURIE_SANDBOX_SUBSTRATE=docker \
    VALKEY_HOST=localhost VALKEY_PORT=26379 VALKEY_PASSWORD=valkeypass \
    SLACK_API_BASE_URL=http://localhost:8155/api/ SLACK_BOT_TOKEN=xoxb-dev \
    CURIE_DOCKER_NETWORK=curie_default \
    OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318 \
    uv run python -m curie_worker &
```

- With the Docker substrate, `_sandbox_client` requires a model credential
  (`CURIE_CREDENTIALS`, `CLAUDE_CODE_OAUTH_TOKEN`, or `ANTHROPIC_API_KEY`),
  `CURIE_MODEL_BASE_URL` (local-model mode), or `CURIE_FAKE_MODEL=1`
  (explicit offline opt-in) -- absent all three it raises `SystemExit` at
  startup rather than booting a runner that fails cryptically.
- `CURIE_DOCKER_NETWORK` joins each spawned runner container to the compose
  network (`docker.py`'s `--network`); without it a runner is on the default
  bridge network and can't reach the stack's other services (including the
  OTel collector) by hostname.
- `OTEL_EXPORTER_OTLP_ENDPOINT` is forwarded into each spawned runner; unset,
  runners still boot but just don't export traces (a warning, not a startup
  failure).
- `CURIE_RUNNER_IMAGE` overrides the runner image tag (default
  `curie-runner`).
- `VALKEY_HOST`/`VALKEY_PORT`/`VALKEY_PASSWORD` and `SLACK_BOT_TOKEN` point at
  the compose stack's exposed ports and dev bot token (see `compose.dev.yaml`);
  `SLACK_API_BASE_URL` points at the stack's Slack stub instead of real
  slack.com.

Drive a turn the normal way once it's up (`curie local deploy` / `curie
local message`, pinned at whichever API port you brought up) -- the hand-run
worker claims runner containers on the same Docker daemon exactly as the
compose-managed one would. For an offline round-trip, add `CURIE_FAKE_MODEL=1`
to the env above and drop the credential.

## Delivery ownership: one deadline, one fenced owner (ADR-0131)

Owning a PEL row is necessary but not sufficient to execute or settle a
delivery. Every `(stream, group, entry_id)` also carries an absolute Valkey-time
deadline (created once, never restarted by a retry or a reclaim) and a short
renewable lease holding an opaque owner token and a monotonically increasing
fencing generation (`delivery_lease.py`, driven by the shared lifecycle in
`stream_consumer.py` so the runs and eval lanes share one implementation). A
heartbeat renews it; a renewal that Valkey cannot confirm fails **closed** —
the owner is lease-lost, its runner is interrupted through the bounded control
path, and it may no longer settle anything.

### Capability audit: every owner verb and the fence that gates it

One row per verb a delivery owner can perform that produces a durable or
user-visible effect.

| Verb | Where | What fences it |
|---|---|---|
| Execute or retry a turn | `kernel/` (the attempt loop; `start_turn` / `steer`) | The lease is checked before every attempt, and attempts consume the one overall deadline. A loss mid-turn fires the base's lease-lost handler, which interrupts the live turn. |
| Re-execute a *reclaimed* delivery | `kernel/` reclaim preflight (generation > 1) | A side-effect marker forbids replay and escalates; a runner still reporting an active turn is interrupted and waited out; an unreadable runner fails closed. |
| ACK (runs lane) | `consumer.py`, immediately before `XACK` | `lease.raise_if_lost()`. A refusal leaves the entry pending for the current owner. |
| ACK (eval lane) | `eval/stream.py`, immediately before `XACK` | The same pre-ACK `raise_if_lost()`. |
| ACK **via dead-letter**, handler path | `stream_consumer.py` `_dead_letter` → `_dead_letter_refusal` | Dead-letter is a terminal settlement (it ACKs, then deletes the lease and delivery state). A handler holds a registered lease, so the question is whether that lease is still ours. The one exception is a broker entry that a fresh read shows is gone and whose lease token is still ours or already absent (`broker-entry-vanished`): there is no successor to leave it for. |
| ACK **via dead-letter**, over-cap scan | `stream_consumer.py` `_dead_letter_over_cap` | A live-lease check runs *before* cap evaluation, so a healthy long turn cannot be dead-lettered, and `_dead_letter_refusal` re-reads the lease before writing. Both fail closed on an unreadable answer. |
| Write the done marker + the completion-outbox record | `markers.py` `settle_fenced`, whose only caller is `kernel/` `_complete` — the only `mark_done` call site | One Lua script verifies the lease token and the fencing generation and then performs the terminal write. A loser writes nothing and returns `None`. |
| Emit the terminal reply (`turn.completed`) | `kernel/` `_complete` → `_deliver_completion` | Only reachable past `settle_fenced`; the fenced-out owner returns having emitted nothing. |
| Clear an outbox record | `markers.py` `clear_completion`, via `_deliver_completion` | The same fence, plus the record-generation compare-and-check, so a stale pass cannot delete a fresh record. |
| Write platform progress, terminal states included | `progress.py` `ProgressStore.apply_platform_update` (no caller yet; ADR 0130) | One Lua script checks the lease token and the fencing generation, the same two checks `settle_fenced` makes, before it writes anything. A loser writes no state and enqueues no delivery, and is refused `lease-lost`. |
| Publish an eval report | `eval/stream.py` `_report` → `POST /evals/report` | The lease is resolved from the entry's stream id (never from a field that is `None` on the failure paths) and checked immediately before the send. A fenced lane whose lease cannot be resolved refuses to publish. |

Stated plainly, as ADR-0131 requires: **a fenced-out owner is refused ACK,
dead-letter, outbox clearing, and terminal emit** — and is refused starting a
new attempt.

Three verbs are **deliberately not lease-fenced**, and none is an oversight:

- **The side-effect marker** (`markers.mark_side_effect`, written from
  `kernel/` the instant a `side_effect_flag` is seen). A side effect that
  happened must be recorded even by an owner that has since lost its fence. The
  marker is itself a hard no-replay fence, and it is what stops the
  *replacement* from re-running a non-idempotent action; fencing the write would
  let a lost lease erase the evidence. This is the one place where fail-closed
  means "write anyway".
- **The completion-outbox sweeper** (`kernel/` `sweep_pending_completions`).
  The sweeper is not an owner: it drains records for entries that may already be
  acked off the group, where no lease exists or ever will. Its guard is the
  record's done flag plus the compare-and-checked `clear_completion`, not a
  lease. Do not "complete the fence" by adding one here.
- **The progress-outbox sweeper** (`progress.py` `sweep_pending_progress`,
  called from the maintenance tick next to the completion sweeper). It is not an
  owner for the same reason: a pending progress delivery outlives the stream
  entry whose turn caused it. Its guard is the record's generation, compared on
  every attempt charge, acknowledgement and dead-letter, not a lease.

Applying a model's progress command (`ProgressStore.apply_model_command`,
called by the kernel's per-turn pump) is not an owner verb either. The pump
applies what the ingress accepted from the authenticated running turn, not a
stream delivery, so its guard is the `(epoch, seq)` order and terminal
monotonicity rather than a lease: an owner that lost its fence can only apply
its own turn's commands, which the record orders like anyone else's. While
rendering is off the pump also removes the deliveries an applied command
enqueued (`ProgressStore.discard_deliveries`); nothing a person sees depends on
that write.

### Adapter idempotency: which channel may claim one terminal effect

Terminal transport is at-least-once; the completion outbox retries by stable
`event_id`. Exactly-once therefore means **one user-visible terminal effect for
that identifier, never one network send** — ADR-0131 states exactly-once network
delivery is impossible, and nothing below claims it. An adapter may claim the
property only when its receiving boundary applies `event_id` idempotently or the
adapter mutates one stable target.

Slack has two verbs here and only one of them takes a key. `chat.update` takes
none, so an edit is idempotent by its stable target. `chat.postMessage` takes
`client_msg_id`, which is how the approval card has always survived an
ambiguous retry, and every create that carries a reply wire 1.1 `delivery_id`
passes it there (ADR-0130 section 4), including an approval post. Existing 1.0
approval posts keep using the approval UUID because they carry no
`delivery_id`; the CLI recognizes either form from the structured Approve
button's UUID value. On 2026-09-29, exact candidate
`0b26aa3e3bd5d0663267e4393030200d88ece156` ran
`apps/worker/tests/test_live.py::test_live_slack_client_msg_id_dedupes_an_ambiguous_retry`
against real Slack. The duplicate call answered `ok: true` with the first
message's `ts`; the thread contained exactly one card and one fresh-key
milestone. `_adopt_posted_ts` adopts that returned `ts`. An API error still
raises, so the delivery remains retryable under the same key.

| Adapter / path | Receiving boundary | Idempotent apply? | Claim |
|---|---|---|---|
| `SlackReplyAdapter` (`slack_sink.py`) | One Slack message: `chat.update` on the placeholder's stable `(channel, ts)`. `turn.completed` has no Slack expression and sends nothing, so an outbox retry is a no-op on this channel. | Yes, by **stable target**, not by `event_id`: `chat.update` takes no idempotency key. | One user-visible terminal effect per `event_id`. |
| `SlackReplyAdapter`, placeholder-less turn (`reply_ref is None`, the ADR-0079 triggered turn), 1.0 body | `chat.postMessage`, a **create**, not a mutation, until the minted ts is adopted as the turn's ref. A 1.0 body carries no `delivery_id`, so the post carries no `client_msg_id`. | No. | Explicitly at-least-once for that first post; the edits that follow it are covered by the row above. |
| `SlackReplyAdapter`, placeholder-less turn, 1.1 body carrying `delivery_id` | The same `chat.postMessage`, with `client_msg_id` set to the `delivery_id`. | Slack deduplicates by `client_msg_id`; the measured duplicate returned the original `ts` (above). | One visible answer message per `delivery_id`; the adapter adopts the original `ts`. |
| `SlackReplyAdapter`, approval card (`reply.post` with a `ConfirmIntent`) | For a 1.0 body, `chat.postMessage` keeps the approval UUID as `client_msg_id`. For a 1.1 body, `client_msg_id` is the wire operation's `delivery_id`; the approval UUID remains in the structured Approve button value, where `cli/src/chat.rs::approval_card_id` reads it. | Slack deduplicates by whichever stable key the body form supplies. | One visible card per approval UUID on 1.0, or per `delivery_id` on 1.1. |
| `SlackReplyAdapter`, progress post (`reply.post` carrying `progress`: a card's first revision or a milestone) | `chat.postMessage` with `client_msg_id` set to the `delivery_id`, which the coordinator derives and never re-mints for a retry. | Slack deduplicates by `client_msg_id`; the measured duplicate returned the original `ts` (above). | One visible card or milestone per `delivery_id`; the adapter adopts the original `ts`. |
| `SlackReplyAdapter`, progress edit (`reply.update` carrying `progress`) | `chat.update` on the card's own ts, the `ref` acknowledged for its first post, never the placeholder's. Never the answer path, and never the approval card's settle path. | Yes, by **stable target**. The `delivery_id` has no Slack expression on an edit. | One visible card whatever the retry count. Which revision shows last is the coordinator's order, because Slack keeps the last edit it received. |
| `HttpReplyAdapter` (`reply_sink.py`) | Whatever the binding's operator-controlled endpoint does with one POST. `turn.completed` carries `event_id` in the body, so the key is on the wire, but this repo cannot verify what the receiver does with it. | Unknown — receiver-owned, unverifiable from here. | **Explicitly at-least-once.** May not advertise exactly-once terminal effect. |
| Eval report (`eval/stream.py` `_report` → `POST /evals/report`) | The platform API's report endpoint. `EvalReport` carries `repo_full_name`, `sha`, and counts — no idempotency key. | No. | **Explicitly at-least-once.** The pre-send lease check closes most of the window, not the send-then-lose-the-ack window; closing it needs eval-report idempotency at the platform API (follow-up F2). |

### Cron hook run outcomes

A cron turn carries a `hook_run` carrier with `agent_id`, `name`, and `slot_utc`.
The scheduler producer must supply all three fields for every cron turn, with
`slot_utc` as an ISO8601 UTC timestamp.

When a started turn exits, the worker writes `ran` for a normal exit, including
an approval pause, or `failed` for a bad exit or deadline, with `ended_at`
before writing the done marker. A prestart deferral leaves the row open for
scheduler reconciliation. A cron failure is not retried within its fire.
Slack and webhook turns perform no hook run writes.

A long scheduled sweep is the one cron turn that continues past its delivery
budget (ADR-0160, #2878). A targeted cron delivery cut by the budget
(`runner-timeout` or `runner-timeout-unconfirmed` with no budget left) whose
newest checkpoint fact still lists uncovered sources renews the hook lease and
publishes a continuation `<base>:sweep:<n>:<covered>:<stalled>` on the runs
stream inside the same fenced script that settles the slice
(`Markers.settle_fenced_and_publish`), leaving the row open. `covered` is the
count of distinct covered names, ignoring case, and `stalled` counts the
slices in a row that added none: a source may span up to two cut slices, and
the sweep stops after `sweep.MAX_STALLED_SLICES` (3) stalled slices in a row or
past `sweep.MAX_SWEEP_SLICES` (48) slices. The script publishes before it
writes the record and the done marker, so a failed publish settles nothing. It
also sets a per-event published marker beside the publish, and a retried
script that finds the marker holding its own generation writes nothing again,
even after the outbox record was cleared.
`curie_worker.sweep.SweepCoverage` reads the checkpoint from agent and channel
memory with the platform key, keeping only facts the API stamped with the cron
sender for this hook, sweep date and run. A continuation only adopts the live
route and never claims, resumes or hands off a sandbox (`SweepClaimGone`). It
waits out a winding-down turn inside its own budget and closes `blocked` on a
paused hook. Every stop that closes `failed`, `skipped` or `blocked` with a
checkpoint, or on a continuation, posts one coverage notice as a new message
after the settle is won. The hook lease renewed at a cron delivery's start
carries `sweep.HOOK_LEASE_START_MARGIN_S` on top, so the interrupt and the
coverage read at a budget cut cannot let the next fire reclaim the run first.

If persistence fails before durable closure, the delivery stays pending and
may run again on redelivery. The close and the done marker use PostgreSQL and
Valkey, so they are not atomic across both systems.

## Deliberate progress (ADR 0130)

`curie_worker.progress` is the worker coordinator's durable state for
[ADR 0130](../../docs/adr/0130-deliberate-progress-is-bounded-durable-channel-state.md):
one progress record per logical turn chain, its milestone budget, and an outbox
of the card and milestone deliveries the record owes its channel. It lives in
Valkey, like the completion outbox; Postgres holds none of it.

A model's command reaches it through the running turn: the kernel hands an
eligible turn a progress capability, the runner's `progress` tool posts each
command to the API's scoped ingress, the API appends it to the chain's inbox,
and a per-turn pump in the kernel applies it to the record (see
[The capability and the pump](#the-capability-and-the-pump) below). Rendering is
off: no adapter is called for progress, and the maintenance tick runs its
sweeper without a deliverer (below).

### Keys

Every key is built by a `WorkerConfig` helper under `key_prefix`
(`curie:worker` by default), like every other worker key.

| Key | Type | Holds |
|---|---|---|
| `<key_prefix>:progress:{pid}` | hash | The record: `state`, `summary`, `revision`, `epoch`, `last_seq`, `turn_generation`, `active_generation`, `milestones_used`, `card_ref`, `answer_ref`, `terminal`, `inbox_cursor`, `update_count`, and one field per accepted update id. |
| `<key_prefix>:progress:delivery:{delivery_id}` | hash | One pending delivery: the semantic event (`event`), its route (`route`), `attempts`, `gen`, and the `pid`, `slot` and `created_at` its scripts and the sweeper read. |
| `<key_prefix>:progress:pending` | set | The index of pending delivery ids, so the sweeper never scans the keyspace. |
| `<key_prefix>:progress:chain:{event_id}` | string | The `pid` an approval resume event continues. |
| `<key_prefix>:progress:inbox:{pid}` | stream | The chain's inbox: one entry per command the API accepted, with the fields `command` (the `ProgressCommand` as JSON), worker-issued `generation` and runner-issued `seq`. The API writes it; the live pump or maintenance drainer reads it. |
| `<key_prefix>:progress:inbox:pending` | set | Progress ids with inbox work not yet reflected by `inbox_cursor`. The API adds atomically with `XADD`; the worker removes only after proving no later stream id exists. |
| `<key_prefix>:progress:rate:{token digest}` | hash | The API's per-token rate limit bucket (`tokens`, `at`). |

The inbox and the rate bucket are written by the API, which shares the
worker's `KEY_PREFIX` (`worker_key_prefix`), and their shape is frozen in
[`tests/vectors/turn-progress-capability.json`](../../tests/vectors/turn-progress-capability.json).
The API caps the inbox at 128 entries and gives it a 14 day expiry on every
append, and the bucket expires a minute after its last use. Every other key
expires after `max(completion_max_retention_s, 14 days)`. Fourteen
days is the approval card's own lifetime (`approval_cards.DEFAULT_CARD_TTL_S`),
because a chain lives across the approval it suspends for. The record's expiry
is renewed by every accepted update. A delivery keeps the expiry it was written
with, and the pending set's expiry is renewed by every enqueue, so the set
outlives every member it indexes; the sweeper drops a member whose delivery has
expired.

### Identity

- `progress_id_for(thread_key, root_event_id)` is
  `uuid5(PROGRESS_ID_NAMESPACE, thread_key + "\0" + root_event_id)` with a fixed
  namespace constant, so a redelivered root event reopens the same record
  rather than minting a second one.
- `ProgressStore.chain_for_turn` is how a turn finds its record. A fresh turn
  opens, idempotently, the record its own event id derives. An approval resume
  only follows `chain:{resume_event_id}`, the pointer `link_resume` writes when
  the chain suspends, so it continues the same record with the same milestone
  budget. When that pointer has expired the resume gets no record at all: the
  helper returns `None` and the caller renders nothing. A resume never derives a
  record from its own event id, so an expired pointer cannot become a fresh
  budget.
- Delivery ids are derived, never minted, so a retry cannot change identity.
  The card's first post is `uuid5(pid, "card")`, the card edit at revision r is
  `uuid5(pid, "card:r")`, and milestone n is `uuid5(pid, "milestone:n")`, each in
  the canonical lowercase form reply wire 1.1's `DeliveryId` requires.

### Who writes which state

- `apply_model_command` takes a validated `ProgressCommand` and accepts only
  `investigating`, `preparing-workspace`, `testing` and `publishing`. A command
  naming `queued`, `awaiting-approval`, `complete`, `failed` or `cancelled` is
  refused (`platform-only-state`) before Valkey is touched.
- `apply_platform_update` is the platform's separate entry point and may write
  any state, the terminal ones included. It takes the caller's ADR-0131 lease
  (owner token and fencing generation, with the delivery triple that names its
  keys). Its script first checks that the lease key still holds the token and
  that the delivery state's generation is still the caller's, the two checks
  `markers.py` `settle_fenced` makes, and writes nothing when either has moved
  (`lease-lost`). The leaseless sentinel, `unfenced_lease()`, holds no token and
  is refused the same way.
- Model and platform update ids are recorded in separate namespaces (`m:` and
  `p:` fields), so a model cannot pre-empt a platform write by reusing its id.

### Update rules

Each update is one Lua script. Its checks run in this order, and the first that
fails answers:

1. The record must exist (`no-chain`). Only opening a chain creates one; no
   update does.
2. An update id already accepted is a no-op reported as `duplicate`, not an
   error, and writes nothing.
3. A terminal record (`complete`, `failed`, `cancelled`) refuses every update
   (`terminal`), so nothing reopens it.
4. Updates are ordered by `(epoch, seq)`, where a model command's epoch is the
   durable generation the worker allocated for its turn. A command from an older epoch is
   refused (`stale-epoch`), and within the record's epoch a `seq` at or below
   `last_seq` is refused (`stale-seq`); a newer epoch is accepted and restarts
   the sequence. A platform update names an epoch and no seq. It is refused from
   an older epoch, and at a newer one it moves the record to that epoch, which
   refuses every later command of the older one: an `awaiting-approval` written
   for the resume's epoch fences out a late command from the suspended session.
5. A chain accepts at most 50 updates (`MAX_PROGRESS_UPDATES`), and the 51st
   non-terminal update is refused (`update-cap`). The terminal write is exempt,
   so a chain that spent its updates can still close its card. Terminal is
   monotonic, so the exemption adds at most one.

An accepted update increments `update_count`, records its id, and advances
`epoch` and `last_seq`. It increments `revision` by exactly one when it changes
the state or the summary, and only then; an update that changes neither leaves
the revision, and the card, as they were.

The card payload and every delivery id depend on the revision and the
milestone count, and only Python derives them. So the script also compares the
two counts it is about to advance against the ones the store read before
building them, and when another update moved either in between it writes
nothing and the store reads again and retries. Each retry means another change
was accepted, and a chain has a bounded number of revisions and reservations,
so the retry loop is bounded too.

### Milestones

A model command carrying `milestone` reserves the next ordinal in the same
script that accepts the update, while `milestones_used` is below 3. The fourth
request is refused, reported as `milestone_refused`, and the update itself
still applies: its state and summary still move the card, it still counts
toward the 50, and its id is still recorded, so a retry of it is a duplicate
rather than a second try for a slot. An update that is refused reserves
nothing. Reservations are fields of the record, so a restarted worker and an
approval resume (the same pid) read the same count. Approval cards and the
canonical final answer use no slot, and the platform entry point cannot
request one.

### The outbox

The script that accepts a change also enqueues the delivery it owes, in the
same atomic step: the delivery record and its `progress:pending` membership.
Revision 1 owes the card's first post (`reply.post`), each later revision one
card edit (`reply.update`), and each reservation one milestone post. The stored
event is the semantic payload (`ProgressCard` or `ProgressMilestone`), its
operation and its reply target. An edit is addressed to the record's
`card_ref`, which only exists once the first post is acknowledged, so it is
read at delivery time rather than stored in the edit.

- `ack(delivery_id, generation, card_ref=...)` clears a delivery only when its
  stored generation is the caller's, so a late acknowledgement cannot clear a
  record written after it. For the card's first post it also records the
  adapter's ref as `card_ref` in the same script, so a crash cannot separate
  the two.
- `sweep_pending_progress` is bounded: it samples at most 64 members, stops
  after 30 seconds, and bounds each delivery by the time left. For each member:
  - a malformed record is quarantined: its index membership is removed, the
    payload is left in place for an operator to inspect, and an ERROR is
    logged, so one bad record cannot crash-loop the tick;
  - a member whose delivery has expired or been cleared is dropped from the
    index;
  - a delivery that has used its 5 attempts (`PROGRESS_MAX_ATTEMPTS`) is
    dead-lettered to the graveyard (`dl_source=progress-outbox`, described
    [above](#the-dead-letter-graveyard));
  - otherwise, given a deliverer and once the delivery is older than a 60
    second grace that keeps the sweeper out of the live path's window, it
    charges one attempt, calls the deliverer with the stored record, whose
    `delivery_id` never changes, and acknowledges on success. A failure leaves
    the delivery for a later pass, and the attempt that reaches the cap
    dead-letters it at once.
- The maintenance tick calls the sweep right after `sweep_pending_completions`,
  with no deliverer, because nothing delivers progress yet. That sweeper
  quarantines, drops and dead-letters, and never charges an attempt it cannot
  make, so a replica that can deliver never finds its deliveries' budget spent
  by an older one during a rolling upgrade. A failed pass is logged and does
  not stop the rest of the tick.

Every attempt charge, acknowledgement and dead-letter compares the stored
generation, so a pass holding a stale read can neither clear nor accuse a
delivery written after it.

### The capability and the pump

`curie_worker.turn_progress` is the kernel's side of the ingress.

- **Eligibility.** Only a human's Slack turn gets a capability: its source is
  `slack`, its reply handle's kind is `slack`, it has a reply target, and it is
  neither a factory work-item turn nor a `curie cluster message` relay turn
  (adapter `curie-cluster-message`). An approval resume of such a turn is
  eligible too. Before claiming its sandbox, the worker writes
  `CURIE_TURN_PROGRESS_ENABLED=1` only for an eligible turn. That boot fact is
  part of sandbox reuse comparison, so a sandbox with the opposite eligibility
  is cold-recreated rather than adopted. The runner mounts the progress tool
  and prompt only when the fact is present. A job, cron turn, targetless hook,
  factory execution and relay turn therefore see neither the tool nor its
  prompt, in addition to receiving no progress headers.
- **The chain.** A fresh turn's chain is `progress_id_for(thread_key,
  event_id)`, so a retry of the same event, in the same delivery or a
  redelivery, names the same record. An approval resume follows only the
  pointer its suspended turn wrote; when that pointer has expired the resume
  gets no capability. The record is opened (idempotently) when the turn's
  stream is consumed, never when an event only steers a live turn, so a steer
  opens no chain. When an eligible turn pauses for approval the kernel links
  the resume event `approval-<id>-resolved` to its chain with `link_resume`.
- **The capability.** Each actual turn start atomically increments the record's
  durable `turn_generation`, marks it as `active_generation`, and mints a
  sandbox token (the byte-identical `sandbox_token` module) with scope
  `turn.progress` and subject `progress_id:generation`. It sends the token,
  generation, and URL to the runner on `POST /v1/event` in
  `X-Curie-Progress-Token`, `X-Curie-Progress-Generation`, and
  `X-Curie-Progress-Url`. They are runner control headers, like
  `X-Curie-Turn-Epoch`, and not ACI fields. The API's append script checks the
  signed generation is still active and its Valkey-server-time lease has not
  passed. The lease lasts five seconds. Renewal begins as soon as activation
  succeeds, before the worker waits for the runner's response headers, and is
  handed to the live pump once stream consumption starts. Both renew only the
  active, unexpired generation; a missed lease cannot be revived. The worker clears the
  generation and deadline when the turn closes; if that best-effort clear loses
  Valkey, expiry within one lease is the fail-closed backstop. A
  retry or cold resume advances it first, so an old token cannot enqueue or
  fence the current turn. Nothing opens a generation when the API key is unset.
- **The pump.** A startup lease keeper covers runner admission and response-header
  delay. While the kernel consumes the turn's stream, a pump takes over renewal of
  the active lease and reads the
  chain's inbox after the record's `inbox_cursor`, at most 64 entries every
  half second, and applies each entry with `apply_model_command` at the
  entry's `(epoch, seq)`, advancing `inbox_cursor` past it. The cursor only
  moves forward and is never written to an expired record. When the stream
  ends the pump drains what remains, bounded to 5 seconds, and stops. A
  malformed entry is logged and skipped. The pump never fails a turn: a Valkey
  error is logged and the turn goes on. Every accepted append also puts the
  progress id in `progress:inbox:pending`; the maintenance loop drains that
  index after a crash, cancellation, final-drain timeout, or transient read
  failure. It removes membership only with a script that proves the stream has
  no id after the durable cursor, so a concurrent append cannot be orphaned.
  Any failure after runner start but before pump handoff stops the startup
  keeper and closes the generation. Keeper shutdown never consumes cancellation
  of the owning delivery: it attempts the generation close first, then
  re-propagates cancellation.
- **Rendering is off.** `CURIE_PROGRESS_RENDER` (default `false`) is the
  temporary switch the rendering change will turn on; the chart does not set
  it. With it off, the same Lua update records state, revision and milestone
  reservations but does not enqueue a delivery at all. The no-delivery choice
  is therefore atomic with acceptance: a crash or transient Valkey failure
  cannot strand an outbox row for a later release to replay. This worker has no
  progress deliverer, so it refuses to start with
  `CURIE_PROGRESS_RENDER=true`.

`answer_ref` is the reply ref of the turn's answer, given when the chain is
opened. Nothing reads it yet.

### How the Slack adapter renders progress

`SlackReplyAdapter.emit` is ADR-0130's Slack adapter path. It checks `progress`
before anything else on both events, so a progress body never reaches the
answer path, the placeholder, or the approval card's settle path. The Block Kit
comes from `apps/worker/src/curie_worker/blocks.py::progress_card` and
`apps/worker/src/curie_worker/blocks.py::progress_milestone`, and nothing else
builds a progress block.

- **A card's first revision** (`reply.post`, `progress.kind == "card"`) is posted
  into the turn's thread with `client_msg_id` set to its `delivery_id`, and the
  `ts` Slack answers is the `ref` the coordinator records as the card's ref.
- **A later revision** (`reply.update` carrying `progress`) is a `chat.update`
  of `target.reply_ref`, the card's ref. One without a ref raises, because there
  is no card to edit and posting a second one would break the one-card rule.
- **A milestone** (`reply.post`, `progress.kind == "milestone"`) is a new message
  in the thread, posted the way a card's first revision is. It is never edited.

What a reader sees:

- The card states its state in plain words (`queued` reads "Queued",
  `awaiting-approval` "Waiting for approval", `preparing-workspace` "Preparing
  the workspace") and then the summary. An open card reads "Task status:
  <state>". A terminal card reads "Task complete", "Task failed" or "Task
  cancelled" and adds a closing line saying the card will not change again, so
  a closed card is visibly closed when the final answer arrives beneath it.
- A milestone names its class ("Evidence acquired", "Scope changed",
  "Verification result") and then the summary.
- Every text element either builder renders is `plain_text` with `emoji`
  false, never `mrkdwn`. A summary is model-authored, and Slack reads mentions
  and emoji codes out of `mrkdwn` only, so `<!channel>` or `<@U0EXAMPLE1>` in a
  summary is shown as those characters and pings nobody. The message's `text` fallback, which
  Slack does parse, is the channel-neutral `progress_text` with `&`, `<` and
  `>` escaped the way Slack's formatting reference requires. Neither adds an
  emoji.
- A rejected Block Kit payload falls back to text only, like every other
  Slack path here. For an edit that fallback sends an empty `blocks` list,
  because Slack keeps a message's previous blocks when an update omits them,
  and a stale card would otherwise stay on screen under a changed fallback.

Every block either builder renders carries a stable `block_id` prefix:
`curie-progress-card:` for a card and `curie-progress-milestone:` for a
milestone. A card's ids also carry its revision, because Slack asks for a new
`block_id` on each iteration of an updated message. The prefixes are frozen in
[`tests/vectors/progress-blocks.json`](../../tests/vectors/progress-blocks.json)
with the CLI's Slack stub (`cli/src/chat.rs`), which uses them to tell a
progress post or edit from the turn's answer: a progress call is shown as a
status line at most, and never becomes the reply `local message`, `cluster
message` or local eval report. Changing a prefix means changing that file, and
both lanes' tests fail until it is.

## The sandbox substrate (`curie_worker.sandbox`)

The lifecycle seam between the worker kernel and kubernetes-sigs/agent-sandbox
v0.5.0 (core `Sandbox` CRD (Custom Resource Definition) + the extensions `SandboxClaim`/`SandboxWarmPool`/
`SandboxTemplate`). The kernel talks in `thread_key` (the Slack `thread_ts`) and
`SandboxHandle`; everything Kubernetes-shaped stays behind this module.

```python
from curie_worker.sandbox import (
    AffinityStore, KubernetesSandboxClient, SandboxSubstrate, SubstrateConfig,
)

substrate = SandboxSubstrate(
    KubernetesSandboxClient(namespace),          # or any SandboxClient impl
    AffinityStore(
        redis_client,
        pressure_client=pressure_redis_client,
    ),                                           # bounded async pressure lane
    SubstrateConfig(namespace=..., warm_pool="<release>-runner-pool"),
)

handle = substrate.claim(thread_ts)   # existing live route, or warm-pool claim
# handle.base_url -> http://<serviceFQDN>:8080  (dial from inside the cluster)
substrate.suspend(thread_ts, history_ref=thread_history_ref)
handle = substrate.resume(thread_ts)  # new claim, CURIE_HISTORY_REF injected
substrate.release(thread_ts)          # delete claim -> sandbox+pod reaped
substrate.reap_orphans()              # periodic tick: claims with no live route
```

Contract notes the kernel must know:

Quota pressure reclamation runs only after a real ResourceQuota refusal and
only when the remaining delivery budget is at least 70 seconds plus
`claim_timeout_seconds`. A lower budget skips the scan. An interactive Slack
turn waits durably for capacity under its original event identity, with a
fixed deadline set by `CURIE_CAPACITY_WAIT_BUDGET_S` (24 hours by default).
The worker acknowledges a parked stream delivery, then wakes the turn through
the same stream when its retry is due. Waiting does not use a runner attempt or
hold a conversation lock. Its placeholder says queued while waiting and
receives an expiry message if the deadline passes. An approval resume does not
wait. It runs the same reclamation pass after its own quota refusal, and when
the pass frees nothing, or the one retry after it is refused again, the attempt
fails under `sandbox-capacity` instead (#3693, #3700). The failure retries, and
each retry that is refused again may run the pass once more, so a resume that
stays refused can free up to one idle route per attempt. Its terminal notice
tells the person the agent was at capacity and could not continue after the
approval decision, with the quota detail left to the worker's warning. An
earlier attempt's confirmed pod termination is carried onto that notice. Other turn sources
retain their capacity response. Operators can inspect the persisted wait state and
`curie.capacity.wait` metrics for waiting, active, and expired turns.

The rejection retains every exceeded resource and its requested, used, and
hard quantity. Before scanning, the worker validates the complete map with
Kubernetes quantity semantics, including CPU DecimalSI, memory BinarySI, pod
counts, and combined rejections. Invalid or incomplete evidence skips
pressure reclamation with `outcome=refused-invalid-quota` and performs no
pressure Redis call or deletion.

After one exact idle route is detached and deleted, the worker polls the exact
named ResourceQuota in its configured namespace within the existing 20 second
cleanup window. It retries only when live spec and status hard limits agree and
every rejected resource has enough current headroom. The worker Role grants
only namespaced `get` on core `resourcequotas`. A missing permission, malformed
quantity, mismatched spec and status limits, or timeout fails closed. The
rejected claim or another quota can consume the freed capacity before the full
retry. That bounded race records `reclaimed-retry-refused`; it does not delete
another victim or schedule work.

The inventory scans at most eight pages with a SCAN `COUNT` hint of 8192,
roughly 65,000 keys in the whole logical database. `COUNT` is approximate. A
separate limit counts at most 256 matching route keys before filtering, so
suspended routes count toward it, and at most four candidates are probed.
Candidates are probed idle eval routes first (conversation ids with the `eval:`
isolate prefix), then every other route, each group in expiry order. A
database outside either finite window fails closed. Alert on
`curie.sandbox.lifecycle` with `operation=reclaim` and
`outcome=scan-incomplete`. Redis or Valkey before 7.0 does not support
`PEXPIRETIME`, so the pass returns `expiry-unsupported`.

An operator thread reset (`reset-thread`, `POST /agents/{id}/threads/{key}/reset`)
is drained on the maintenance tick. The drain records the outcome under
`curie:thread-reset-result:<thread_key>` for an hour, as `released` when a route
existed or `no-route` when the key matched none, before it clears the
in-progress marker. The API reports it as `route_existed`, so a reset that freed
nothing (typically a hand-built key that left out a named bot's identity
segment) is visible to the caller. A release that raises records no result and
leaves the request in progress. Each drained reset also increments
`curie.sandbox.lifecycle` with `operation=thread-reset` and `outcome=released`,
`no-route` or `failed`; alert on `no-route` to find resets that freed nothing.

On a terminal pressure timeout, cancellation is delivered once and victim lock
release can add one finite cold pressure Redis operation, at most four seconds.
That tail consumes unused claim reserve and never authorizes requester retry.

- **One live session per thread.** `claim()` is claim-or-adopt: a lost
  creation race deletes the loser's claim and returns the winner's handle. The
  route lives in Valkey (`curie:sandbox:route:<thread_key>`) with a TTL;
  `claim()`/`touch()` refresh it on activity.
- **Sub-second claims require a warm pool.** The chart's
  `agentSandbox.deploy=true` installs `<release>-runner` (SandboxTemplate) and
  `<release>-runner-pool` (SandboxWarmPool). Claims without per-claim env bind
  a pre-warmed sandbox (0.04-0.07 s measured on a scratch k3s cluster); claims **with**
  env through resume or retained attachment replacement get a fresh sandbox
  instead. This cold create takes seconds rather than less than one second
  because env cannot be injected into a running pod.
- **Suspend/resume is a cold rehydrate.** `suspend()` flips the Sandbox
  to `Suspended` (the pod is deleted) and records the caller-supplied history
  ref. `resume()` retires the old claim and creates a new one whose per-claim
  env injects `CURIE_HISTORY_REF` (+ the original `CURIE_SESSION_ID`); the
  runner resolves that ref to the thread's transcript on the durable state store
  and replays the prior turns as a boot-time system-prompt preamble (ADR-0029),
  not an SDK-native resume id. Never assume process or prompt-cache warmth across
  a suspend.
- **Producing the history ref is the caller's job.** The ref is a deterministic
  state-store URL (`.../state/transcript/<thread_key>`), so there is nothing to
  capture off the ACI `final` frame and no frozen-contract change (ADR-0029); do
  not reintroduce a frame-captured resume id.
- **Late workspace acquisition is a fenced cold replacement.** A repository URL
  on an existing generic route proceeds only when the bearer-authenticated old
  runner is idle, its complete structured history is durable, and it is not
  suspended on an approval or unresolved side-effect boundary. The worker
  revalidates that status after workspace preparation, then cold-creates a
  candidate carrying the same logical session and history reference. Before the
  affinity compare-and-swap, the candidate must attest its exact session and
  sandbox identities, a ready idle state, durable history, and cwd `/workspace`.
  Any unreadable or mismatched status deletes only the unexposed candidate and
  leaves the generic route authoritative (ADR-0136).
- **Reaping.** `release()` deletes the claim (the claim owns its sandbox and
  pod). Routes that expire in Valkey leave orphaned claims; `reap_orphans()`
  lists claims labeled `curietech.ai/managed-by=curie-sandbox-substrate` and
  deletes any not referenced by a live route. Run it on a periodic worker tick.
  It additionally spares any claim younger than `claim_timeout_seconds` plus a
  fixed margin: a claim still inside its bind window has no route yet, so "no
  live route" alone would read as litter and reaping it would delete a live
  sandbox out from under a blocked turn. A claim whose age is unknown is spared
  and logged at WARNING.
- The client seam (`SandboxClient` protocol) is sync; wrap calls in a thread if
  the kernel goes async. Unit tests fake only this protocol (the K8s control
  plane); Valkey is never mocked. The env-gated e2e
  (`tests/sandbox/test_e2e_k8scratch.py`, `CURIE_SANDBOX_E2E=1`) drives the
  real cluster.

### Retained thread attachments

An attachment on an idle retained thread needs new claim environment, so the
worker cold creates a candidate runner. Replacement requires an authenticated
old runner that is inactive, has a safe completed or idle status, and reports
durable history. The old route remains authoritative while the candidate binds.
The worker then swaps the candidate over the exact old claim and generation in
one affinity operation. This temporarily requires capacity for both the old and
candidate runners. A capacity refusal, bind failure, or lost fence deletes only
the candidate and preserves the old route.

The replacement keeps the logical session identity and durable transcript
reference, but it loses prompt cache warmth, process memory, and other container
local state. Text without an attachment keeps the existing steering behavior.
An active runner asks the sender to wait. An unauthenticated, unreadable,
nondurable, malformed, or otherwise unsafe runner asks the sender to start a
new thread. Neither refusal processes the message text.

In v0.9.1 a retained thread with an open repository workspace says that its
workspace is already open and asks the sender to start a new thread. A generic
retained thread whose new file message selects a repository, or whose server
state already holds a repository selection, gives a separate repository
selection refusal and the same recovery. When repository workspaces are
disabled, the existing workspaces disabled refusal takes precedence before the
file is resolved. Fresh workspace claims and suspended workspace resumes still
receive attachments. Retained workspace replacement is tracked in #2728.

### Every boot rebuilds the thread's files (ADR 0205)

With the attachment lane on and `CURIE_INTERNAL_WORKER_TOKEN` set, the worker
wires the API's thread attachment ledger
(`curie_worker.ledger_client.ThreadAttachmentLedgerClient`, the
`/v1/internal/thread-attachments` routes behind `X-Curie-Worker-Token`). Without
the token no ledger is wired and attachments behave exactly as above: a file
turn resolves only its own files and a text turn reads nothing.

- **A file turn** reads the ledger outside the route lock, then
  `AttachmentCoordinator.prepare_thread_set` fetches the message's files all or
  nothing and the thread's earlier files best effort. A failed ledger read
  refuses the turn before any claim (`stage=ledger`). Once the files are
  installed and the turn has opened, the worker appends the message's refs to
  the ledger, outside the lock and idempotent per event and file. A failed append is
  logged at WARNING and the turn goes on. A turn refused after preparing
  appends nothing and discards what it wrote.
- **A text turn that boots** (fresh claim, suspended resume, turn budget or
  workspace handoff) reads the ledger and prepares the earlier files under the
  route lock. The ledger read is bounded by
  `CURIE_ATTACHMENT_THREAD_PREPARE_TIMEOUT_SECONDS` (default 30) and the
  delivery's remaining budget. The prepare has the same deadline plus a short
  grace. A failed read boots with `ledger_unavailable` in the manifest. An
  adopt, a steer and a sweep continuation read nothing.
- **Names are fixed when recorded.** A current file's disk name is cleaned with
  the init container's rules (a leading `.` becomes `_`) and disambiguated
  against every name the ledger holds, then stored on its ref and never
  recomputed.
- **Bytes stay short lived.** An earlier file whose parked copy outlives the
  capability by a margin is re-minted, and a new owner record (still
  `"version": 1`, with optional `agent` and `shas` fields) keeps it from the
  reaper. Otherwise it is fetched again through the agent's current bindings
  (`BindingResolver.routes_for_agent`), never a recorded endpoint, and its
  digest must match. One that cannot be had is named unavailable (`no_route`,
  `no_credential`, `not_found`, `forbidden`, `rate_limited`, `timeout`,
  `digest_changed`, `deadline`, `fetch_failed`) and never fails the boot.
- **The per-thread budget** (`CURIE_ATTACHMENT_THREAD_MAX_FILES`, default 20,
  never below the per-message cap; `CURIE_ATTACHMENT_THREAD_MAX_BYTES`, default
  256 MiB) keeps the current files and then the newest earlier files. The rest
  are named as omitted.
- Capabilities are presigned only after every fetch. The claim env carries
  `CURIE_ATTACHMENTS_REF` (each entry with its exact name `n` and `c` 1 for
  current, 0 for earlier) and `CURIE_ATTACHMENTS_MANIFEST` for the runner
  (names and reasons only, no URL).

Approval metadata references follow the same sentence boundary rules across API,
worker, runner and the UI fallback for older API responses. A tool identifier
followed by a sentence-ending period (including whitespace or closing punctuation)
uses its plain action label. Filenames, extensions, paths, URLs and identifiers
embedded within another word remain literal data, including Unicode text. The
stored summary, exact grant target, arguments and nested content remain unchanged.

Receipt and CLI progress metadata use the same reference-boundary vector as
approval displays. Closing code quotes end a sentence reference just as closing
parentheses, brackets and ordinary quotes do; requested filenames and paths stay
literal. This wording changes neither answers nor stored actions.

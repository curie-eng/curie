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
edit and drop, never a crash. The claim env also carries a per-sandbox
`CURIE_RUNNER_TOKEN` (minted with `secrets.token_urlsafe`) that the `RunnerClient`
sends as an `Authorization: Bearer` header on every ACI call to that sandbox
(issue #63).

> Handoff: `CURIE_BUNDLE_REF` is a RustFS object key; the runner reads
> `CURIE_PLUGIN_DIR` as a local mounted path and does not fetch. Fetching the
> bundle key into the plugin dir is sandbox provisioning (an init container in the
> sandbox substrate's SandboxTemplate / the chart), owned there, not by the worker. Per-channel
> dev/prod bot-identity routing (the dispatcher carrying which bot was addressed)
> is a git-flow/dispatcher refinement.

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
  `rate-limit`, `runner-error`, `runner-timeout` and `workspace-error` are
  transient (bounded exponential backoff); `budget-exceeded` and everything else
  escalate.
  `runner-timeout` is the runner's streaming budget expiring mid-turn (#2011),
  told apart from `runner-error` -- the sandbox or the transport dying -- so an
  operator can see which one happened.
  `workspace-error` is a managed-workspace preparation FAULT before the turn was
  ever accepted (#2004): the clone, the archive, or the upload. It is told apart
  from `runner-error` for the same
  reason, and it always carries a `workspace start failed` WARNING naming the
  agent, the deployment, the repository the turn asked for and the stage that
  failed -- such a turn used to ack, create no sandbox and log nothing at all.
  A deliberate repository-selection refusal is the other half of that split and
  is NOT this: it is a decision rather than a fault, so it stays terminal,
  answers the user, and logs at INFO instead. A turn that names a repository
  while the coordinator is off for the worker is such a refusal (#2659). Generic
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

The graveyard has two row families. Stream-consumer rows carry the original
entry's fields verbatim plus namespaced failure metadata, so a human or replay
tool can inspect exactly what inbound entry died and why:

| Field | Meaning |
|---|---|
| `dl_original_id` | the entry's id on the source stream |
| `dl_delivery_count` | deliveries made before it was given up on |
| `dl_reason` | `max-delivery-exceeded`, or `unparseable` |
| `dl_dead_lettered_at` | UTC ISO-8601 timestamp |

Completion-outbox rows are written by `Markers.dead_letter_completion` only for
the exact `DeletedReplyTargetError` deleted-thread classification. They carry
`event_id`, the
serialized `completion`, `dl_reason="thread deleted at provider"`,
`dl_delivery_count="1"`, `dl_source="completion-outbox"`, and
`dl_dead_lettered_at`; they have no `dl_original_id` and are not replayable as
inbound stream entries. Every other completion delivery failure leaves the
completion owed in the outbox for re-emission.

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
`CURIE_MAX_ATTEMPTS`, `CURIE_MAX_DELIVERY` / `CURIE_DEAD_LETTER_STREAM` /
`CURIE_DEAD_LETTER_MAXLEN` (approximate graveyard cap, default `10000`, minimum
`1`), `CURIE_LEASE_EXPIRED_IDLE_MS` (the lease-expiry reclaim threshold, default
one delivery lease TTL) and `CURIE_TURN_NOT_STARTED_TEXT` (the placeholder edit
when a delivery's handler raises), plus `CURIE_NAMESPACE` / `CURIE_WARM_POOL` / `CURIE_RUNNER_PORT` for
the substrate. Run with `python -m curie_worker`.

Tests: `uv run pytest apps/worker/tests/kernel -q` runs against the real Valkey
from `compose.dev.yaml`, the real sandbox substrate with a fake Kubernetes client whose
sandboxes resolve to a local in-process fake runner, and a recording Slack sink.
Only Slack and the model behind the runner are faked.

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
| Execute or retry a turn | `kernel.py` (the attempt loop; `start_turn` / `steer`) | The lease is checked before every attempt, and attempts consume the one overall deadline. A loss mid-turn fires the base's lease-lost handler, which interrupts the live turn. |
| Re-execute a *reclaimed* delivery | `kernel.py` reclaim preflight (generation > 1) | A side-effect marker forbids replay and escalates; a runner still reporting an active turn is interrupted and waited out; an unreadable runner fails closed. |
| ACK (runs lane) | `consumer.py`, immediately before `XACK` | `lease.raise_if_lost()`. A refusal leaves the entry pending for the current owner. |
| ACK (eval lane) | `eval/stream.py`, immediately before `XACK` | The same pre-ACK `raise_if_lost()`. |
| ACK **via dead-letter**, handler path | `stream_consumer.py` `_dead_letter` → `_dead_letter_refusal` | Dead-letter is a terminal settlement (it ACKs, then deletes the lease and delivery state). A handler holds a registered lease, so the question is whether that lease is still ours. |
| ACK **via dead-letter**, over-cap scan | `stream_consumer.py` `_dead_letter_over_cap` | A live-lease check runs *before* cap evaluation, so a healthy long turn cannot be dead-lettered, and `_dead_letter_refusal` re-reads the lease before writing. Both fail closed on an unreadable answer. |
| Write the done marker + the completion-outbox record | `markers.py` `settle_fenced`, whose only caller is `kernel.py` `_complete` — the only `mark_done` call site | One Lua script verifies the lease token and the fencing generation and then performs the terminal write. A loser writes nothing and returns `None`. |
| Emit the terminal reply (`turn.completed`) | `kernel.py` `_complete` → `_deliver_completion` | Only reachable past `settle_fenced`; the fenced-out owner returns having emitted nothing. |
| Clear an outbox record | `markers.py` `clear_completion`, via `_deliver_completion` | The same fence, plus the record-generation compare-and-check, so a stale pass cannot delete a fresh record. |
| Publish an eval report | `eval/stream.py` `_report` → `POST /evals/report` | The lease is resolved from the entry's stream id (never from a field that is `None` on the failure paths) and checked immediately before the send. A fenced lane whose lease cannot be resolved refuses to publish. |

Stated plainly, as ADR-0131 requires: **a fenced-out owner is refused ACK,
dead-letter, outbox clearing, and terminal emit** — and is refused starting a
new attempt.

Two verbs are **deliberately not lease-fenced**, and neither is an oversight:

- **The side-effect marker** (`markers.mark_side_effect`, written from
  `kernel.py` the instant a `side_effect_flag` is seen). A side effect that
  happened must be recorded even by an owner that has since lost its fence. The
  marker is itself a hard no-replay fence, and it is what stops the
  *replacement* from re-running a non-idempotent action; fencing the write would
  let a lost lease erase the evidence. This is the one place where fail-closed
  means "write anyway".
- **The completion-outbox sweeper** (`kernel.py` `sweep_pending_completions`).
  The sweeper is not an owner: it drains records for entries that may already be
  acked off the group, where no lease exists or ever will. Its guard is the
  record's done flag plus the compare-and-checked `clear_completion`, not a
  lease. Do not "complete the fence" by adding one here.

### Adapter idempotency: which channel may claim one terminal effect

Terminal transport is at-least-once; the completion outbox retries by stable
`event_id`. Exactly-once therefore means **one user-visible terminal effect for
that identifier, never one network send** — ADR-0131 states exactly-once network
delivery is impossible, and nothing below claims it. An adapter may claim the
property only when its receiving boundary applies `event_id` idempotently or the
adapter mutates one stable target.

| Adapter / path | Receiving boundary | Idempotent apply? | Claim |
|---|---|---|---|
| `SlackReplyAdapter` (`slack_sink.py`) | One Slack message: `chat.update` on the placeholder's stable `(channel, ts)`. `turn.completed` has no Slack expression and sends nothing, so an outbox retry is a no-op on this channel. | Yes — by **stable target**, not by `event_id`; Slack exposes no idempotency key. | One user-visible terminal effect per `event_id`. |
| `SlackReplyAdapter`, placeholder-less turn (`reply_ref is None`, the ADR-0079 triggered turn) | `chat.postMessage` — a **create**, not a mutation, until the minted ts is adopted as the turn's ref. | No. | Explicitly at-least-once for that first post; the edits that follow it are covered by the row above. |
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

If persistence fails before durable closure, the delivery stays pending and
may run again on redelivery. The close and the done marker use PostgreSQL and
Valkey, so they are not atomic across both systems.

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
`claim_timeout_seconds`. A lower budget returns the capacity response without
scanning. Reclamation adds no periodic timer, scheduler, or warm pool.

The rejection retains every exceeded resource and its requested, used, and
hard quantity. Before scanning, the worker validates the complete map with
Kubernetes quantity semantics, including CPU DecimalSI, memory BinarySI, pod
counts, and combined rejections. Invalid or incomplete evidence returns the
capacity response with `outcome=refused-invalid-quota` and performs no pressure
Redis call or deletion.

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
suspended routes count toward it, and at most four candidates are probed. A
database outside either finite window fails closed. Alert on
`curie.sandbox.lifecycle` with `operation=reclaim` and
`outcome=scan-incomplete`. Redis or Valkey before 7.0 does not support
`PEXPIRETIME`, so the pass returns `expiry-unsupported`.

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

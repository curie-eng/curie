# CLAUDE.md - apps/worker

The concurrency kernel plus the Agent Sandbox substrate, and the F3 eval-stream
runner (`curie_worker.eval`), which runs eval suites off the `curie:evals`
stream against the frozen eval-case format (#8, ADR-0019). Full behavior spec
lives in `apps/worker/README.md`; this file is the enforceable-rule summary. Read
`../../ARCHITECTURE.md`'s message-flow diagram before changing `kernel.py`.

## The kernel is sacred: single owner, adversarial review

`kernel.py`/`consumer.py`/`threadlock.py`/`markers.py` are correctness logic
with races that only show up under concurrent load. **One change owns this
module at a time, it is never split across parallel work, and any change needs
an adversarial review (spec-vs-impl + side-effects-detective, minimum) before
merge.** If you are touching `kernel.py` as a side effect of another change,
stop -- that is scope creep on the sacred module.

## The four rules the kernel enforces (each has a provoking integration test)

1. **One live session per thread.** A follow-up to a thread with a live turn
   is a *steer*, never a new turn. The per-thread lock (Valkey `SET NX PX`
   plus an in-process FIFO lock) wraps the route decision; the new turn
   opens *before* the lock releases. The lock is never held during
   streaming -- if you find yourself holding it longer, you broke "steering
   is never blocked."
2. **The finish race.** A steer arriving as the turn ends returns 409; the
   kernel opens a fresh turn on the same idle sandbox. Do not "fix" a 409 by
   retrying the steer instead of falling back to a new turn.
3. **Steer vs. interrupt.** Default is steer; `Kernel.interrupt_thread` is
   the explicit hard stop. **The kernel never keyword-guesses intent** -- do
   not add text-sniffing to decide between the two; that is an explicit
   caller signal.
4. **No auto-retry after side effects.** A failed run that emitted
   `side_effect_flag` escalates to a human instead of retrying; the flag is
   persisted to Valkey the instant it is seen so a crash mid-side-effect
   still escalates on reclaim. Flag-clean failures retry by classification
   (`rate-limit`/`runner-error`/`runner-timeout`/`sandbox-capacity`/`sandbox-terminated`/`workspace-error`/`model-unreachable` transient,
   everything else escalates). `workspace-error` (#2004) is a workspace
   preparation FAULT before the turn was accepted; it must log a WARNING naming
   the agent, deployment, repository and stage, because a silent one acks the
   turn and creates no sandbox with nothing in the log to find it by. A
   deliberate selection refusal is the other half of that split: a decision, not
   a fault, so it stays terminal, answers the user, and logs at INFO.

## Delivery is bounded (ADR-0039, #505)

The reclaim loop is **not** infinite. An entry already delivered `max_delivery`
times (`CURIE_MAX_DELIVERY`, default 5, floor 2) is dead-lettered to
`<stream>:dead` and acked off the group instead of re-dispatched. Unbounded
reclaim let one permanently-failing entry starve the whole consumer group -- a
silent total stall. Removing or bypassing the cap is that regression, not a
simplification.

- **The delivery count is read from the PEL every pass, never tracked in
  process memory.** A process-local counter resets on restart, so a
  crash-looping worker would retry poison forever -- the exact stall shape.
- **Read the count with `XPENDING` *before* `XAUTOCLAIM`**, which bumps it on
  claim; the pre-claim value is the budget already spent. Cap at `>=`. Keep the
  `IDLE` filter equal to `XAUTOCLAIM`'s `min_idle_time`, and keep the
  `_inflight_ids` skip ahead of the cap check.
- **Narrow prompt-reclaim exception:** the normal heavy-maintenance pass keeps
  that equal-`IDLE` rule. A younger at/over-cap row may instead be
  dead-lettered directly only after the owning consumer has advertised
  capability *and* its independent alive lease has remained absent for the
  sustained-absence proof. That path still reads pre-claim `XPENDING` metadata
  and applies the local `_inflight_ids` skip before either a direct
  dead-letter or an `XCLAIM`; it never charges a live peer another delivery.
- **Lease-expiry exception (#2433).** A pending row whose delivery state exists
  but whose delivery lease has expired is cap-evaluated and graveyard-eligible
  at `lease_expired_idle_ms` (`CURIE_LEASE_EXPIRED_IDLE_MS`, default one lease
  TTL, 45 s) rather than at `reclaim_min_idle_ms` (900 s), regardless of whether
  its PEL consumer is still alive. It still reads the delivery count from the
  pre-claim `XPENDING` row, caps at `>=`, and keeps the `_inflight_ids` skip
  ahead of the cap check. Implementation:
  `apps/worker/src/curie_worker/stream_consumer.py::StreamConsumer._select_lease_expired_entries`.
  An entry with no delivery state carries no evidence a lease was ever granted
  and stays on the unchanged 900-second backstop.
- **Compare-and-claim: every transfer `XCLAIM` passes the idle the caller
  observed for that row, and `0` is never passed.** The lease-expiry pass passes
  its scan `IDLE`; the proven-dead pass passes each row's own
  `time_since_delivered` from the pre-claim `XPENDING` read. `XCLAIM` with a
  positive min-idle is an atomic compare-and-claim on the row's idle clock, and
  any competing transfer (a claim, a heartbeat, `XAUTOCLAIM`) resets that clock,
  so two transfer paths cannot both move one row. Accepted residual: a row this
  process claimed whose dispatch then waits past one lease TTL can be re-claimed
  by a peer, and the late handler's `acquire` is refused `not-owner` and returns
  without acking. The cost is one charged delivery, logged at WARNING, never a
  double run and never a stranded turn.
- **`XADD` before `XACK`, never the reverse.** A crash between them costs a
  duplicate graveyard row; the reverse costs the entry.
- **`max_delivery` is not `max_attempts`.** The latter is the kernel's
  flag-clean retry *classification* inside one delivery. Do not conflate them.
- **Transient failure is unchanged**: a mid-turn crash still reclaims, retries,
  and acks. Only an exhausted budget dead-letters.
- **Exception (#708): approval-resume reply is best-effort in the pure-offline
  loop.** When an approval-resume turn's reply endpoint is unreachable AND no
  default transport is configured, the adapter swallows the unreachable error
  instead of raising -- the turn ACKs without delivering the reply instead of
  dead-lettering. Both adapters honor it: `SlackReplyAdapter` when no default
  Slack transport is configured, `HttpReplyAdapter` by logging and returning
  with no fallback target at all (a fallback there would post an email reply
  into a Slack workspace). A configured default transport, or any non-resume
  turn, still raises and dead-letters normally. A MISSING egress credential is
  never swallowed: fail-closed outranks best-effort.
- **The graveyard has no consumer group, but it IS bounded** by an approximate
  `MAXLEN` (`CURIE_DEAD_LETTER_MAXLEN`, default 10000) on every `XADD`. The
  unparseable path dead-letters per inbound entry, so an unbounded graveyard hands
  a wire-DTO drift a full-ingest-rate OOM against the same Valkey that holds the
  kernel's locks and markers. Rows are therefore **best-effort**: under a flood the
  oldest are evicted. Do not treat the graveyard as a complete audit log, and do
  not add a consumer group, a retention policy, or replay tooling as a passenger
  on another change.
- **A dead-letter stream equal to the source stream is rejected at config
  validation.** `XADD` precedes `XACK`, so a self-targeting graveyard re-queues
  failures onto the stream they came from and hot-loops. Do not soften that
  validator into a warning.

## Consumer liveness and prompt recovery (#1532)

`XINFO CONSUMERS` idle is an observation filter, not a process-health signal:
it grows while a worker drains an in-flight turn or waits on a saturated local
semaphore. Runs and eval consumers therefore use the same
`ConsumerLivenessStore`
(`apps/worker/src/curie_worker/consumer_liveness.py::ConsumerLivenessStore`)
beside the stream broker, rather than widening `StreamBroker` with generic
string-key verbs.

Delivery and liveness renewals retain authority after an unconfirmed raise only
until the last confirmed send plus TTL minus refresh interval, while a refused
renewal drops authority immediately (ADR 0207).

- **Publish before reading.** A generation writes its renewable short `alive`
  lease before its renewable `capability` marker, and does not issue
  `XREADGROUP` until that ordered publication succeeds. Both markers refresh
  for the consumer lifetime, including graceful in-flight drain. On graceful
  exit only the short alive lease is removed; the longer capability marker
  remains long enough for a replacement to recognize a departed capable peer.
- **Prompt reclaim needs positive proof of death.** The dedicated liveness
  cadence considers only a consumer that is idle enough to be a candidate,
  has a capability marker, and has an absent alive lease on two observations
  separated by at least one heartbeat TTL. A restored lease, a missing
  consumer, a liveness-store error, or a generation restart clears that proof.
  This protects live, draining, and saturated peers from duplicate dispatch.
- **One replacement owns the transfer.** After the death proof, a short Valkey
  `SET NX PX` arbitration lease per dead consumer selects one replacement. That
  prevents replicas crossing the proof threshold together from issuing racing
  `XCLAIM`s and consuming several delivery attempts. A generation restarted
  under the same stable consumer name first recovers its own canceled PEL rows
  through the same pre-claim cap path before it reads new entries.
- **Terminal teardown is joined, not instantaneous.** A renewal timeout that can
  no longer be retried safely stops new reads and joins canceled handlers before
  resetting generation state. Thread-backed work may delay that join; peers
  still require the full sustained-absence proof and arbitration before transfer.
- **Mixed-version compatibility is intentional.** An unknown or pre-marker
  consumer is not guessed dead by the prompt path; its PEL entries stay on the
  existing 900-second `XAUTOCLAIM` backstop. The prompt path is additional
  recovery for a proven-dead capable peer, not a shorter global idle timeout.
- **A live consumer's own stranded row is not a prompt-path case (#2433).** A
  handler that raised released its delivery lease and left the entry pending,
  and no prompt path looks at a peer that is not dead. `_reclaim_once`'s
  lease-expiry pass recovers such a row once its lease has expired, whoever owns
  it. A pre-lease or pre-marker entry with no delivery state still waits out the
  unchanged 900-second backstop. While the row waits,
  `apps/worker/src/curie_worker/kernel.py::Kernel.notify_turn_not_started` edits
  the placeholder to `turn_not_started_text` (`CURIE_TURN_NOT_STARTED_TEXT`),
  best effort, so the thread is not silent.
- **Runs/eval parity is mandatory.** Both
  `apps/worker/src/curie_worker/consumer.py::Consumer` and
  `apps/worker/src/curie_worker/eval/stream.py::EvalStreamConsumer` use the shared
  `apps/worker/src/curie_worker/stream_consumer.py::StreamConsumer` lifecycle, selection,
  cap, and sustained-absence rules. Do not give either lane a private reclaim
  shortcut.

## The sandbox substrate (`curie_worker.sandbox`)

- **The kernel talks in `thread_key` and `SandboxHandle` only.** Everything
  Kubernetes-shaped stays behind the `SandboxSubstrate`/`SandboxClient` seam
  -- never leak a K8s type across it into the kernel.
- **`claim()` is claim-or-adopt.** A lost creation race deletes the loser's
  claim; the route lives in Valkey (`curie:sandbox:route:<thread_key>`)
  with a TTL that `claim()`/`touch()` refresh.
- **Claims carrying per-claim env (the resume path) cold-create** instead of
  binding the warm pool, because env cannot be injected into an
  already-running pod. Expected latency, not a bug.
- **Suspend deletes the pod.** Never assume a suspended sandbox's process or
  prompt cache survives; a resumed or restarted sandbox rehydrates from external
  state via `CURIE_HISTORY_REF`.
- **The client seam is sync; unit tests fake only `SandboxClient`** (the K8s
  control plane) -- Valkey is never mocked.
- **The history ref is a deterministic state-store URL, not an SDK session id**
  (ADR-0029). `binding.boot_env` sets `CURIE_HISTORY_REF` to this thread's
  transcript key on the durable state store (`.../state/transcript/<thread_key>`)
  on **every** claim, so a fresh, restarted, or resumed sandbox all boot with the
  same ref and the runner rehydrates the conversation as a preamble. There is no
  SDK session id to capture off the ACI `final` frame and no frozen-contract
  change; do not reintroduce a frame-captured resume id.

## Deployment-to-runtime binding (implemented)

`binding.py` resolves a thread's Slack channel to its agent, that agent's active
deployment (prod outranks dev, then most recent), and the resolved
`CURIE_BUNDLE_REF`, and injects it into the sandbox claim so the boot picks up
the bundle version the API's git-flow engine produced. It is a read-only query
layer over the shared Postgres tables (agents -> deployments -> agent_versions),
deliberately not an import of the API package. The seam to remember: a thread
keeps the sandbox and bundle it first booted with; only a fresh mention (a new
claim) picks up a newer deployment. Any change that touches the kernel's claim
path here is still bound by the sacred-module rule above.

## Verify

```bash
uv run pytest apps/worker/tests/kernel -q     # real Valkey, fake K8s client, in-process fake runner, recording Slack sink
uv run pytest apps/worker/tests/sandbox -q    # unit; CURIE_SANDBOX_E2E=1 additionally drives a real cluster
```

Only Slack and the model behind the runner are faked in the kernel suite.

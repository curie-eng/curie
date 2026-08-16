# Curie Architecture (as built)

Curie (codename **Relay**) turns a Slack thread into a conversation with a
versioned, sandboxed AI agent, and turns a git push into a deployment of that
agent. Slack and email are the two wired channels today; both sit behind a
channel-agnostic message port ([ADR 0020](docs/adr/0020-message-port-rendering-free-channel-interface.md)),
so additional channels are additive, not a rewrite. This document is the as-built map. It covers:

- the components
- the two runtime modes and what is identical between them
- how one Slack turn and one eval run flow through the system
- how model credentials reach the model
- how traces come back out

Every claim carries a repo path you can jump to. Paths are relative to the repo
root. Where main does not yet contain something the design calls for, it is
marked **not yet in main** rather than described as shipped. Those items are
tracked in [GitHub issues](https://github.com/curie-eng/curie/issues).

The narrative "why" behind the big calls lives in the ADRs (Architecture
Decision Records) ([`docs/adr/`](docs/adr/)). This doc is the "what talks to
what." It supersedes the pre-build plans that the MVP (Minimum Viable Product)
was built from, which are preserved in git history.

## Table of contents

- [Overview](#overview)
- [Component map](#component-map)
  - [Adopted, not built](#adopted-not-built)
- [Handling a Slack mention (message flow)](#handling-a-slack-mention-message-flow)
  - [The four kernel invariants](#the-four-kernel-invariants)
  - [Handling approvals (human in the loop)](#handling-approvals-human-in-the-loop)
- [Pushing agent versions with git (deploy flow)](#pushing-agent-versions-with-git-deploy-flow)
- [One worker, two hidden seams: substrate and transport](#one-worker-two-hidden-seams-substrate-and-transport)
  - [Substrate seam — `SandboxClient`](#substrate-seam--sandboxclient)
  - [Slack seam — a per-turn reply endpoint and the CLI stub](#slack-seam--a-per-turn-reply-endpoint-and-the-cli-stub)
- [The credential path](#the-credential-path)
- [The observability pipeline](#the-observability-pipeline)
- [The UI: always the real API, no demo mode](#the-ui-always-the-real-api-no-demo-mode)
- [Frozen contracts](#frozen-contracts)
- [Deployment, CI, and release](#deployment-ci-and-release)
- [What is built vs deferred](#what-is-built-vs-deferred)

## Overview

What Curie does, in short:

- Connect Slack.
- Author a Claude-Code-format plugin (skills + tools + MCP) in the browser or a repo.
- Deploy it as a bot identity.
- Get traces, evals, budgets, and git-driven deploys for free.

The core loop: a Slack mention is queued, picked up by a worker that claims a
sandboxed runner, and the runner's reply is edited back into the Slack thread.
The same worker code runs unchanged against Kubernetes in production or Docker
locally, and the CLI can stand in for Slack entirely for local testing — see
[Handling a Slack mention](#handling-a-slack-mention-message-flow) for the full
flow and [One worker, two hidden seams](#one-worker-two-hidden-seams-substrate-and-transport)
for how that substrate-agnosticism is built.

## Component map

This is the static "who talks to whom." For the flows through it, read the
focused diagram docs, each a single clean picture:

- **[How a message comes in and a reply goes out](docs/diagrams/message-flow.md)** — the core loop.
- **[Kubernetes architecture](docs/diagrams/kubernetes.md)** — the cluster and how a sandbox pod is built.
- **[The ACI](docs/diagrams/aci.md)** — the ACI, short for Agent Container Interface: the frozen contract between the worker and the agent in the box.

```mermaid
flowchart TB
    Slack["Slack"]
    Email["Email<br/>(AgentMail inbox)"]
    CLI["CLI / laptop"]
    GH["GitHub push"]

    subgraph core["Agent runner core (apps/)"]
        Dispatcher["dispatcher<br/>ingress + dedupe"]
        MailAdapter["mail-adapter<br/>email ingress + threaded reply"]
        Queue["Valkey<br/>queue + routing"]
        Worker["worker kernel<br/>one session per thread"]
        API["api<br/>git-driven deploy · bundles · read proxy"]
    end

    Sandbox["runner pod<br/>Claude Code + skill"]
    Anthropic["Model<br/>(Anthropic default)"]

    UI["ui console"]
    Store[("RustFS / S3<br/>skill bundles")]
    PG[("Postgres<br/>agents · versions · deployments")]

    subgraph obs["Observability"]
        OTel["OTel Collector"]
        LF["Langfuse (+ ClickHouse)"]
        OTel --> LF
    end

    Slack --> Dispatcher
    Email --> MailAdapter
    CLI -- XADD --> Queue
    CLI --> API
    GH --> API
    Dispatcher --> Queue --> Worker --> Sandbox --> Anthropic
    MailAdapter -- channel ingress --> API
    API -- channel turns --> Queue
    Worker -- reply events --> MailAdapter
    API -- evals --> Queue
    Sandbox -. bundle-fetch .-> Store
    Worker -. bundle-fetch .-> Store
    Worker --> API
    Worker -- read --> PG
    API --> Store
    API --> PG
    UI --> API
    Sandbox --> OTel
    Worker -- eval scores --> LF
    API -- read --> LF
```

The reply travels back out the way it came in — sandbox to worker to the
originating thread — kept off the diagram to avoid a tangle of return arrows.
[The message-flow doc](docs/diagrams/message-flow.md) shows that round trip.
Two substrate implementations sit behind the single `runner pod` box
(Kubernetes for production, Docker for local). [One worker, two hidden seams](#one-worker-two-hidden-seams-substrate-and-transport) covers that seam.

The worker is not a pass-through between the queue and the sandbox. It is a hub
with four outbound dependencies of its own:

- It reads the deployment binding from Postgres
  ([`apps/worker/src/curie_worker/run.py::build`](apps/worker/src/curie_worker/run.py)
  opens the engine on the same `DATABASE_URL` the API uses).
- It fetches a version's immutable bundle from the object store for eval runs
  ([`apps/worker/src/curie_worker/eval/stream.py::EvalStreamConsumer`](apps/worker/src/curie_worker/eval/stream.py)).
- It calls the API for approvals and `POST /evals/report`
  ([`apps/worker/src/curie_worker/approvals.py::ApprovalClient`](apps/worker/src/curie_worker/approvals.py),
  [`apps/worker/src/curie_worker/eval/stream.py::EvalReporter`](apps/worker/src/curie_worker/eval/stream.py)).
- It writes eval scores straight to Langfuse
  ([`apps/worker/src/curie_worker/eval/recorder.py::LangfuseEvalRecorder`](apps/worker/src/curie_worker/eval/recorder.py)).

The worker has **no** OTel dependency — the runner is the only emitter. Its one
connection to observability is the eval-score write to Langfuse above. The CLI
likewise never calls the dispatcher directly — see
[the Slack seam](#slack-seam--a-per-turn-reply-endpoint-and-the-cli-stub) for
how it enqueues a turn instead.

### Adopted, not built

Curie leans on these systems rather than building its own (ADR-0007,
[`docs/adr/0007-adopt-not-build-boundaries.md`](docs/adr/0007-adopt-not-build-boundaries.md)):

- Langfuse (traces + evals)
- Kubernetes Agent Sandbox (interactive runtime)
- Slack Bolt (Socket Mode)
- AgentMail (the email inbox, and the SPF/DKIM/DMARC filtering in front of it; see [`docs/operations.md`](docs/operations.md))
- Valkey Streams (queue)
- Postgres (app state)
- the OTel Collector
- **claude-agent-sdk** as the harness (ADR-0005) — one of the two most load-bearing adopt calls of all
- **the Claude Code plugin format verbatim** — the other, which ADR-0007 calls "the distribution wedge — do not invent a format"

Curie builds **seven** things around that spine: the API, the dispatcher, the mail
adapter, the worker+runner glue, the UI, the CLI, and the umbrella Helm chart ([Deployment, CI, and release](#deployment-ci-and-release)). The
chart is a built thing, not a packaging afterthought. The security rails are
chart defaults, so the chart is where a rail either ships or does not.

The per-package directory listing — path, language, and what each package owns
— lives in the [README Component map](README.md#component-map), not duplicated
here so the two cannot drift apart. The Python packages are one **uv workspace**
(root [`pyproject.toml`](pyproject.toml)); see [`CLAUDE.md`](CLAUDE.md) for
verify commands.

## Handling a Slack mention (message flow)

```mermaid
sequenceDiagram
    participant U as Slack user
    participant D as Dispatcher
    participant V as Valkey
    participant W as Worker kernel
    participant S as Sandbox substrate
    participant R as Runner
    participant A as Anthropic API
    participant P as apps/api
    participant O as OTel -> Langfuse

    U->>D: app_mention / DM message
    D->>V: SET dedupe:<event_id> NX EX ttl
    Note over D: retried delivery finds the key set, is dropped (still acked, never re-posted)
    D->>U: post placeholder ("On it...")
    D->>V: XADD curie:runs {QueuedTurn}

    W->>V: XREADGROUP (consumer group)
    W->>V: SET NX PX thread lock (routing CAS)
    W->>W: binding: resolve agent+version+bundle_ref by channel address
    alt no live turn for this thread
        W->>S: claim(thread_ts) / resume
        S-->>W: SandboxHandle (pod cold-created from SandboxTemplate)
        W->>R: POST /v1/event {message}
    else turn already live for this thread
        W->>R: POST /v1/steer {text}
        Note over W,R: 409 if the turn finished first (finish race), worker opens a fresh turn on the same idle sandbox
    end

    R->>A: model call (streaming)
    R-->>W: NDJSON: text_delta*, tool notes*, final
    R--)O: gen_ai spans (agent.run -> generation -> tool)

    alt turn completes
        W->>V: markers (done / side_effect_flag as seen)
        W->>U: chat.update the placeholder in place
        W->>V: XACK
    else turn terminates AWAITING_APPROVAL (a gate fired)
        W->>P: POST /approvals (durable record)
        W->>S: suspend the session
        Note over W,V: the event is DONE and XACKed here. The resolution arrives later as its own queued turn, not as a blocked consumer
        U->>P: human clicks Approve / Reject in Slack
        P->>V: XADD the resolution turn
        W->>R: resume and finish, or terminate rejected
        W->>U: chat.update with the outcome
    end
```

The pieces, cited:

- **Dedupe + placeholder + enqueue** live in the dispatcher:
  - dedupe `SET NX` at [`apps/dispatcher/src/curie_dispatcher/queue.py::claim_event`](apps/dispatcher/src/curie_dispatcher/queue.py)
  - placeholder post at [`apps/dispatcher/src/curie_dispatcher/handlers.py::process_event`](apps/dispatcher/src/curie_dispatcher/handlers.py)
  - `XADD curie:runs` at [`apps/dispatcher/src/curie_dispatcher/queue.py::enqueue`](apps/dispatcher/src/curie_dispatcher/queue.py)

  The Socket Mode handler is at [`apps/dispatcher/src/curie_dispatcher/app.py::SocketModeConnection`](apps/dispatcher/src/curie_dispatcher/app.py). The stream name is configured on [`apps/dispatcher/src/curie_dispatcher/config.py::DispatcherConfig`](apps/dispatcher/src/curie_dispatcher/config.py) (default `curie:runs`), and the payload model is the channel-neutral [`packages/aci-protocol/src/aci_protocol/turn.py::QueuedTurn`](packages/aci-protocol/src/aci_protocol/turn.py).
- **The kernel** consumes at [`apps/worker/src/curie_worker/consumer.py::Consumer.run`](apps/worker/src/curie_worker/consumer.py) and processes at [`apps/worker/src/curie_worker/kernel.py::Kernel.process_event`](apps/worker/src/curie_worker/kernel.py). It talks to the runner over `POST /v1/event`, `/v1/steer`, `/v1/interrupt` ([`apps/worker/src/curie_worker/runner_client.py::RunnerClient`](apps/worker/src/curie_worker/runner_client.py)). These are the same routes the runner serves at [`runner/src/curie_runner/server.py::create_app`](runner/src/curie_runner/server.py).
- **Deployment binding**: a run resolves its agent, version, and `bundle_ref` by exact-match on the channel address against the active deployment, joining `agents` -> `agent_channels` -> `deployments` -> `agent_versions` ([`apps/worker/src/curie_worker/binding.py::BindingResolver`](apps/worker/src/curie_worker/binding.py)). This is how one worker serves many agents: the channel selects the bundle.

### The four kernel invariants

Each has an integration test under `apps/worker/tests/`:

1. **One live session per thread.** A Valkey thread lock (`SET NX PX`) is the routing CAS (compare-and-swap) ([`apps/worker/src/curie_worker/threadlock.py::ThreadLock`](apps/worker/src/curie_worker/threadlock.py)).
2. **The finish race.** A follow-up during a live turn is a steer; if the turn finished first, the runner returns 409 and the kernel opens a fresh turn on the same idle sandbox ([`apps/worker/src/curie_worker/kernel.py::Kernel._route_and_start`](apps/worker/src/curie_worker/kernel.py)).
3. **No auto-retry after a side-effectful failure.** If a prior attempt flagged a side effect, the kernel escalates to a human instead of retrying ([`apps/worker/src/curie_worker/kernel.py::Kernel.process_event`](apps/worker/src/curie_worker/kernel.py)).
4. **Crash recovery.** Pending stream entries are reclaimed with `XAUTOCLAIM` ([`apps/worker/src/curie_worker/stream_consumer.py::StreamConsumer._reclaim_once`](apps/worker/src/curie_worker/stream_consumer.py)). The runs consumer group is created at `$`, so a cold worker never replays ancient backlog ([`apps/worker/src/curie_worker/consumer.py::Consumer.ensure_group`](apps/worker/src/curie_worker/consumer.py)).

Beyond these four invariants, a **kill switch** — a Valkey pub/sub channel
`curie:kill-events` plus per-agent kill keys — gates and interrupts live runs
for a killed agent
([`apps/worker/src/curie_worker/killswitch.py::KillSwitch`](apps/worker/src/curie_worker/killswitch.py)).

### Handling approvals (human in the loop)

A turn does not always terminate in an answer. When a gate fires, the turn
terminates `AWAITING_APPROVAL`. The kernel persists a durable approval record,
then suspends the session until a human resolves it
([`apps/worker/src/curie_worker/kernel.py::Kernel.process_event`](apps/worker/src/curie_worker/kernel.py),
which calls
[`apps/worker/src/curie_worker/kernel.py::Kernel._pause_for_approval`](apps/worker/src/curie_worker/kernel.py); ADR-0010,
[`docs/adr/0010-approval-gates-and-human-in-the-loop.md`](docs/adr/0010-approval-gates-and-human-in-the-loop.md)).
This is the governance story, and it is load-bearing: it is why an agent can hold
a side-effectful tool call rather than firing it. The user-facing walkthrough of
the whole plane -- declaring a route, binding it to a channel, who may resolve,
and the resume turn a skill must handle -- is
[`docs/approvals.md`](docs/approvals.md).

The design property that makes it survive restarts is that **the paused turn is
not a blocked consumer**. The event is marked done and acked immediately. The
resolution arrives later as its **own** queued turn
([`apps/api/src/curie_api/resumequeue.py::ResumeQueue`](apps/api/src/curie_api/resumequeue.py)
mints it via `resume_turn_for`). Nothing holds a stream entry, a thread lock, or a
worker slot across a human's lunch break.

**Suspend/resume is a cold rehydrate, not a live hibernate** (ADR-0003,
[`docs/adr/0003-stateless-first-rehydrate-on-resume.md`](docs/adr/0003-stateless-first-rehydrate-on-resume.md)):
suspending a sandbox deletes its pod. Resume creates a fresh one and rehydrates
from history. Prompt-cache warmth is real within one continuous claim and is
never assumed across a suspend. The `thread_ts -> sandbox_id` affinity store
([`apps/worker/src/curie_worker/sandbox/affinity.py`](apps/worker/src/curie_worker/sandbox/affinity.py))
is what routes a thread back to its sandbox.

Two TTLs, and the difference matters:

- **`route_ttl_seconds: int = 3600`** — the live route, one hour ([`apps/worker/src/curie_worker/sandbox/types.py`](apps/worker/src/curie_worker/sandbox/types.py)).
- **`suspended_route_ttl_seconds: int = 86400`** — a **suspended** route survives 24 hours, which is the real budget a human has to click Approve ([`apps/worker/src/curie_worker/sandbox/substrate.py`](apps/worker/src/curie_worker/sandbox/substrate.py) applies it on suspend).

Two background loops close the loop rather than trusting the click to arrive:

- an **expiry sweeper** resolves approvals nobody answered ([`apps/api/src/curie_api/sweeper.py::sweep_expired_approvals`](apps/api/src/curie_api/sweeper.py), looped by `run_expiry_sweeper` in the API lifespan)
- a **resume reconciler** re-drives resolutions whose resume turn never landed ([`apps/api/src/curie_api/resumereconciler.py::ResumeReconciler`](apps/api/src/curie_api/resumereconciler.py))

Three properties keep an approval from becoming a standing permission:

- Membership for "who may approve" resolves in the API, never in the sandbox (ADR-0034).
- The resumed sandbox boots with a scoped state token rather than the platform key (ADR-0033).
- The post-approval allowance is one-shot and bound to the granting agent (ADR-0035), so an approval cannot be replayed into a standing permission.

## Pushing agent versions with git (deploy flow)

A push is verified with an HMAC (Hash-based Message Authentication Code)
signature, archived, validated, and stored as an immutable versioned bundle.
A **dev-branch** push builds the artifact and fans out its eval suite as a CI
check. A **prod-branch** push promotes that same artifact without rebuilding.
One diagram, both branches:

There are **two ways a push reaches this flow**, and they converge immediately.
The webhook below is the fast path. The second is a timer: `CommitPoller` in the
API asks GitHub whether the deploy branches moved and hands any new commit to
the same `process_push`, so the two cannot disagree about what a push means. It
is off unless `api.commitPollIntervalSeconds` is set, and it exists because a
webhook is an INBOUND request -- a self-hosted cluster behind a firewall cannot
receive one, while outbound always works (#1239).

```mermaid
sequenceDiagram
    participant Dev as Developer
    participant GH as GitHub
    participant API as apps/api (gitflow.py)
    participant PF as plugin-format validator
    participant S3 as RustFS / S3
    participant PG as Postgres
    participant V as Valkey (curie:evals)
    participant W as Worker eval consumer
    participant LF as Langfuse

    Dev->>GH: git push (dev or prod branch)
    GH->>API: POST /github/webhook (push, HMAC-signed)
    API->>API: verify_signature(x-hub-signature-256)
    alt push to dev branch
        API->>API: clone_and_archive(sha)
        API->>PF: validate_bundle(archived tree)
        PF-->>API: ValidationResult (path-qualified errors)
        API->>S3: store immutable versioned bundle
        API->>PG: create Version + Deployment (env=dev)
        Note over API: the dev bot now serves this sha
        API->>V: XADD curie:evals {job} (deduped)
        W->>V: XREADGROUP (separate eval consumer group)
        W->>S3: load eval cases from the bundle
        W-->>LF: run each case, record trace with eval tags
        W->>API: POST /evals/report
        API->>GH: set commit status (pass/fail)
    else push to prod branch
        API->>PG: find the already-built Version for this sha
        API->>PG: create Deployment (env=prod)
        Note over API: promotes the same artifact, no rebuild
    end
```

- **Git-flow fan-out** in [`apps/api/src/curie_api/gitflow.py`](apps/api/src/curie_api/gitflow.py):
  - HMAC signature verify at [`::verify_signature`](apps/api/src/curie_api/gitflow.py)
  - archive at [`::clone_and_archive`](apps/api/src/curie_api/gitflow.py)
  - the branch fan-out itself at [`::process_push`](apps/api/src/curie_api/gitflow.py) — one function that resolves the ref to an environment ([`::environment_for_ref`](apps/api/src/curie_api/gitflow.py)), then either archives+validates+stores+creates a Version and enqueues its evals (dev, deduped on redelivery) or **promotes the already-built artifact without rebuilding** (prod)

  The webhook receiver is at [`apps/api/src/curie_api/routers/github.py::github_webhook`](apps/api/src/curie_api/routers/github.py).
- **Eval stream** `curie:evals` is produced by the API ([`apps/api/src/curie_api/evalqueue.py::EVAL_STREAM`](apps/api/src/curie_api/evalqueue.py)) and consumed by the worker's eval consumer, which is a **separate** consumer group from the runs kernel ([`apps/worker/src/curie_worker/eval/stream.py::EvalStreamConsumer`](apps/worker/src/curie_worker/eval/stream.py)). It POSTs results to `/evals/report` ([`apps/worker/src/curie_worker/eval/stream.py::EvalReporter`](apps/worker/src/curie_worker/eval/stream.py)).
- **The eval matrix endpoint** `GET /evals/matrix` reads pass/fail from Langfuse trace tags/metadata, not a scores join ([`apps/api/src/curie_api/routers/evals.py::eval_matrix`](apps/api/src/curie_api/routers/evals.py)).
- **The manual path** (`GET /agents`, `/agents/{id}/versions`, `/agents/{id}/versions/{vid}/bundle`) and the webhook path terminate at the same `Version`/`Deployment` tables and the same `plugin_format.validate_bundle`. As a result, a plugin authored in the browser, pushed by `curie local deploy`, or promoted by a git push all go through one pipeline. Bundle store/fetch at [`apps/api/src/curie_api/storage.py::BundleStore`](apps/api/src/curie_api/storage.py) and [`apps/api/src/curie_api/routers/bundles.py::download_bundle`](apps/api/src/curie_api/routers/bundles.py).

## One worker, two hidden seams: substrate and transport

The platform never learns which substrate it is running on, and the runner never
learns whether the message came from Slack. Two seams make that true, and both
are real code, not aspiration.

These are two of several such seams tracked in the interface catalog
([`docs/interfaces.md`](docs/interfaces.md), one `INTERFACE.md` per seam under
[`docs/interfaces/`](docs/interfaces)) — the system of record for the full
list, including `StreamBroker`, `ApproverSet`/`ApprovalCreator`, `MemoryStore`,
`TranscriptStore`, `Scorer`, `ObjectStore`, and `CliOutput`. Substrate
(`SandboxClient`) is a worked example here because it has two real
implementations; Slack is one because its leakage is the most instructive.
This section does not duplicate the catalog — read it there for the rest.

### Substrate seam — `SandboxClient`

The worker talks to a `SandboxClient` Protocol
([`apps/worker/src/curie_worker/sandbox/types.py::SandboxClient`](apps/worker/src/curie_worker/sandbox/types.py))
whose methods are `create_claim`, `get_claim`, `delete_claim`, `list_claims`,
`get_sandbox`, `set_sandbox_mode`. Two implementations satisfy it:

- **`KubernetesSandboxClient`** ([`apps/worker/src/curie_worker/sandbox/k8s.py::KubernetesSandboxClient`](apps/worker/src/curie_worker/sandbox/k8s.py)) — creates `SandboxClaim` CRDs (Custom Resource Definitions) against the agent-sandbox controller. This is the production path.
  - The claim references a `SandboxWarmPool` **by name** (`spec.warmPoolRef.name`); the pool object must still exist, or every claim fails `Ready=False reason=WarmPoolNotFound`.
  - The shipped default is `replicas: 0` (no pre-warmed pods), and a real-model claim **cold-creates from the `SandboxTemplate`** regardless, since per-claim env injection cannot bind a pre-warmed pod (the `envVarsInjectionPolicy: Overrides` gotcha). Pre-warming is a dev/fake-model fast path, not the production path ([`charts/curie/templates/agent-sandbox.yaml`](charts/curie/templates/agent-sandbox.yaml)).
- **`DockerSandboxClient`** ([`apps/worker/src/curie_worker/sandbox/docker.py::DockerSandboxClient`](apps/worker/src/curie_worker/sandbox/docker.py)) — runs the same runner image as a local Docker container. This is "middle mode": a full backend on a laptop with no Kubernetes.

Everything above the protocol (the kernel, routing, budgets, kill switch, resume
path) is identical across modes. The runner image, the ACI it speaks, and the
plugin bundle it loads are also identical; only the thing that starts the
container differs.

### Slack seam — a per-turn reply endpoint and the CLI stub

The worker reaches Slack through a **per-turn** reply target, not a single
worker-global setting. `ReplyHandle.endpoint` on the queued turn carries the base
URL of the channel API that this turn's reply is delivered through
([`packages/aci-protocol/src/aci_protocol/turn.py::ReplyHandle`](packages/aci-protocol/src/aci_protocol/turn.py)).
The sink builds (and caches) a client per endpoint
([`apps/worker/src/curie_worker/slack_sink.py::SlackReplyAdapter`](apps/worker/src/curie_worker/slack_sink.py),
behind the [`ReplySink`](apps/worker/src/curie_worker/reply_sink.py) port), and refuses
any endpoint outside the configured Slack origin.
The worker-global `SLACK_API_BASE_URL`
([`apps/worker/src/curie_worker/config.py::WorkerConfig.slack_api_base_url`](apps/worker/src/curie_worker/config.py))
is now the **fallback**: `endpoint = None` means "use the worker's configured
default", i.e. real Slack. That is what routes a reply back to the ingress that
enqueued the turn. As a result, a real Slack workspace and a no-Slack CLI stub
can coexist on **one** worker rather than needing one worker per channel.

The CLI's `curie local message` path does four things:

- starts a local Slack Web API stub ([`cli/src/chat.rs`](cli/src/chat.rs))
- mints the exact `QueuedTurn` the dispatcher would produce, with `endpoint` pointed at the stub
- `XADD`s it onto the very same `curie:runs` stream the dispatcher uses ([`cli/src/queue.rs`](cli/src/queue.rs))
- waits for the worker to finalize the turn by calling the stub's Slack API back

The worker cannot distinguish the stub from Slack: same queue payload, same
`chat.update` call. This is what lets most of the verification suite run with
no Slack workspace at all.

The honest limit: the **per-turn payload** and the **binding surface** are both
channel-neutral now (#1459, ADR-0096) — a deployment binds an agent by
exact-match on a `{kind, address}` channel row, not a Slack-typed column. Email
is the second wired channel (#1515, [`apps/mail-adapter`](apps/mail-adapter)), so
the catalog now carries two implementations of the channel/ingress seam, but it
still grades the seam `C`: a second implementation is not a regrade, and there is
no multi-channel adapter framework yet (#27). `slack` also remains the only kind
with a registered address shape
([`apps/api/src/curie_api/schemas.py::_validate_channel_binding`](apps/api/src/curie_api/schemas.py));
`email` binds on the generic non-empty rule, which is ADR-0096 working as designed
rather than a gap. "The system does not care which channel" is true of a turn in
flight and of how an agent gets bound to one; two wired channels is still not the
same as any channel.

Net effect: a developer can run the entire product loop — real model call
included — on a laptop with Docker, no cluster, and no Slack. The code
exercised is the code that runs in production.

## The credential path

A model credential flows from a Helm Secret to the model env variable the SDK
reads, without any application process brokering it:

```
values.agentSandbox.runner.credentials
  -> chart Secret key "agentCredentials"        charts/curie/templates/secrets.yaml
  -> worker env CURIE_CREDENTIALS             charts/curie/templates/worker.yaml
     (also wired as a warm-pod fallback)        charts/curie/templates/agent-sandbox.yaml
  -> worker injects it into the claim's boot env  apps/worker/src/curie_worker/binding.py::apply_model_env
  -> runner maps the prefix onto the SDK env     runner/src/curie_runner/sdk_auth.py::resolve_model_credential
```

The runner's mapping is prefix-based and fails loud on anything it cannot use
([`runner/src/curie_runner/sdk_auth.py::resolve_model_credential`](runner/src/curie_runner/sdk_auth.py)):

- `sk-ant-oat...` -> `CLAUDE_CODE_OAUTH_TOKEN` (checked first; OAuth tokens share the `sk-ant-` prefix).
- `sk-ant-...` -> `ANTHROPIC_API_KEY`.
- `sk-or-...` (OpenRouter) -> routed through the shared **base-URL-override seam**. The base URL points at OpenRouter's native Anthropic Messages endpoint. The real key is placed in `ANTHROPIC_API_KEY` (sent as the `x-api-key` header, which OpenRouter's Anthropic endpoint authenticates on), overriding the non-empty placeholder the seam sets. `ANTHROPIC_AUTH_TOKEN` is left blank. Staying on the Anthropic wire format keeps prompt caching intact.
- `sk-...` (bare OpenAI-style) -> raises `UnsupportedCredentialError` rather than forwarding a key the Anthropic SDK cannot use.
- Anything else -> treated as an OAuth token.

The same base-URL-override seam ([`runner/src/curie_runner/sdk_auth.py::resolve_base_url_override`](runner/src/curie_runner/sdk_auth.py)) is provider-agnostic: it targets any Anthropic-compatible endpoint without a real Anthropic credential. Canonical base URLs ship in `PROVIDER_BASE_URLS` ([`runner/src/curie_runner/sdk_auth.py`](runner/src/curie_runner/sdk_auth.py)). Three **provider-native** endpoints — **Zhipu**, **Moonshot**, and **DeepSeek** — are selected by base URL rather than key prefix. **OpenRouter** is in the same dict for reference, even though it is prefix-routed (`sk-or-`) rather than base-URL-selected. A **bundled local model** (opt-in Ollama / Qwen3 demo mode, `--local-model`) rides the same seam.

Every one of these keeps the **Anthropic wire format**, which is the whole
point. The module's own comment explains why:

> keep the Anthropic wire format -- and therefore provider automatic prefix
> caching -- rather than the OpenAI chat-completions shape.

So "non-Anthropic providers" are supported. What is genuinely absent is the
**native OpenAI wire format** — that is why a bare `sk-...` key raises
`UnsupportedCredentialError` instead of being forwarded.

An explicit SDK credential already in the env always wins. The mapping is a
no-op when `CURIE_CREDENTIALS` is unset.

**Real model is the default.** The runner makes a real model call unless
`CURIE_FAKE_MODEL` is explicitly set, in which case it swaps in a scripted
`FakeModelSession` ([`runner/src/curie_runner/__main__.py::build_runner`](runner/src/curie_runner/__main__.py)).
`CURIE_FAKE_MODEL` is a test-only knob. The worker's local middle mode defaults
to the real model and treats a missing credential as fail-closed rather than
silently degrading to fake ([`apps/worker/src/curie_worker/binding.py::apply_model_env`](apps/worker/src/curie_worker/binding.py),
[`apps/worker/src/curie_worker/sandbox/docker.py::DockerSandboxClient`](apps/worker/src/curie_worker/sandbox/docker.py)).

**Per-agent connector secrets.** Beyond the model credential, an agent carries its own connector secrets (a GitHub token, a vendor API key), injected into the claim's boot env at [`apps/worker/src/curie_worker/binding.py::inject_connector_secrets`](apps/worker/src/curie_worker/binding.py) (ADR-0009, [`docs/adr/0009-per-agent-connector-auth.md`](docs/adr/0009-per-agent-connector-auth.md)). Two properties are load-bearing:

- **A reserved-name policy fences agent-supplied env against platform boot vars.** Every secret is filtered through `is_reserved_boot_env_name` regardless of env ordering, so a connector secret named after an ACI contract key or a model credential (e.g. `ANTHROPIC_BASE_URL`) can never clobber it. A reserved name is dropped and logged rather than raising, since raising would crash a live claim. A dropped key never carries its value into the log or the injected-keys marker.
- **These secrets live in their own Kubernetes Secret**, deliberately separate from the chart-managed platform Secret, so one agent's token is not readable by every component in the release. The isolation is the point, not an implementation detail.

## The observability pipeline

The write path runs down; the read path (arrows reversed) runs back up from
Langfuse to whoever's asking:

```
runner (OTLP spans, resource attr curie.session_id)
  --> OTel Collector (OTLP gRPC 4317 / HTTP 4318 in)
        --> Langfuse v3 over HTTP (ClickHouse-backed)
              <-- apps/api Langfuse proxy (trace tree, metrics, cost)
                    <-- apps/ui Runs / Metrics / Cost / Logs views
```

- The runner emits `gen_ai`-style spans (`agent.run -> generation -> tool`) with a resource including `service.name`, `curie.session_id`, and `curie.sandbox_id` ([`runner/src/curie_runner/otel.py`](runner/src/curie_runner/otel.py)). The `sandbox_id` is what lets a trace be tied back to the sandbox that served it.
- **Langfuse OTLP (OpenTelemetry Protocol) ingest is HTTP-only.** Services send to the OTel Collector (which may take gRPC or HTTP), and the collector always exports to Langfuse over HTTP. Collector config is at [`otel/collector-config.yaml`](otel/collector-config.yaml). The load-bearing constraint is documented in [`CLAUDE.md`](CLAUDE.md).
- The API reconstructs the tool-call tree from Langfuse's public API via `parentObservationId` ([`apps/api/src/curie_api/langfuse.py::build_tree`](apps/api/src/curie_api/langfuse.py)) and proxies metrics/cost ([`apps/api/src/curie_api/langfuse.py::LangfuseClient`](apps/api/src/curie_api/langfuse.py), surfaced at [`apps/api/src/curie_api/routers/observability.py`](apps/api/src/curie_api/routers/observability.py)).
- The UI's Runs (`RealTraces.tsx`), Metrics (`RealMetrics.tsx`), Cost (`RealCost.tsx`), and Logs (`RealLogs.tsx`) views render these live in wired mode ([`apps/ui/src/views/obs/`](apps/ui/src/views/obs/)).

The `sandbox_id` is also known worker-side (the affinity store and
`SandboxHandle`), so a trace, its session, and its serving sandbox all line up.
The API hoists it out of the trace's observations onto the trace itself
([`apps/api/src/curie_api/langfuse.py::hoist_sandbox_id`](apps/api/src/curie_api/langfuse.py)),
so sandbox identity is surfaced, not pending.

**The observability CLI.** `curie local observability` prints the local
observability surfaces — the console, Langfuse traces/cost, and the API base.
`curie cluster observability` is its cluster twin. Both are per-tier
subcommands, not a top-level one ([`cli/src/main.rs`](cli/src/main.rs)). It is
deliberately a **thin client over the same `apps/api` proxy the UI uses, not a
second backend** (ADR-0038,
[`docs/adr/0038-observability-cli-helper-for-the-agent-dev-loop.md`](docs/adr/0038-observability-cli-helper-for-the-agent-dev-loop.md)).
There is no Prometheus and no separate metrics store behind it. It prints URLs and
opens nothing unless `--open` is passed, and `--json` never opens a browser — the
agent-facing default is inert output.

## The UI: always the real API, no demo mode

The UI is always backed by the live API — there is no fixture/demo world and no
`isWired()` branch. Every view fetches from `apps/api` same-origin under `/api`
(proxied by Vite; the API key resolves via [`apps/ui/src/api/config.ts`](apps/ui/src/api/config.ts)).

- **Backed by the real API:** Agents/Fleet, Runs/Traces, Metrics, Logs, Cost, Versions, create/deploy, Evals, Approvals, and Memory are all wired to `apps/api`. [`apps/ui/src/views/wired/WiredVersions.tsx`](apps/ui/src/views/wired/WiredVersions.tsx) is a real view with its own rollback test ([`apps/ui/src/views/wired/WiredVersions.rollback.test.tsx`](apps/ui/src/views/wired/WiredVersions.rollback.test.tsx)). Connections is a real Slack-connect panel ([`apps/ui/src/views/wired/WiredStubs.tsx`](apps/ui/src/views/wired/WiredStubs.tsx)). Memory reuses the `WiredAgentMemory` panel behind an agent selector (`GET`/`PUT`/`DELETE /agents/{id}/memory`).
- **Not-yet-wired surfaces are honest stubs, never demo data:** Usage and Settings render a `ComingSoon` placeholder ([`apps/ui/src/views/wired/WiredStubs.tsx`](apps/ui/src/views/wired/WiredStubs.tsx)). These stubs state plainly what is not wired yet rather than showing fictional data.

The former `acme-corp` fixture dataset and the `?state=N` / `?api=1` dual-world
gate have been removed (#542). A single build serves the live product, and
views degrade honestly (empty lists, zero metrics) when a workspace is fresh.

## Frozen contracts

Two packages are **frozen interfaces**. Every lane compiles against them across
three languages, so an unreviewed change in one silently breaks the others unless
the schema-compat gate catches it.

- **`packages/aci-protocol`** — the ACI session protocol (open/steer/interrupt, NDJSON `text_delta` / tool notes / `final`, budget, side-effect flag). Pydantic models under [`packages/aci-protocol/src`](packages/aci-protocol/src) are the source of truth. Committed JSON Schema under [`schema/`](packages/aci-protocol/schema) and generated TypeScript + Rust under [`generated/`](packages/aci-protocol/generated) are derivatives.
- **`packages/plugin-format`** — the Claude Code plugin bundle shape, verbatim (`plugin.json` + `skills/**/SKILL.md` + `.mcp.json` + `scripts/`). `validate_bundle` lives in [`packages/plugin-format/src`](packages/plugin-format/src) and is the single validator every deploy path calls. Choosing the real Claude Code plugin shape (not an invented format) is the distribution wedge (ADR-0005, [`docs/adr/0005-claude-agent-sdk-adapter-and-frozen-aci.md`](docs/adr/0005-claude-agent-sdk-adapter-and-frozen-aci.md)).

The compat gate regenerates the schema and Rust in-process and fails on drift
([`packages/aci-protocol/tests/test_schema_compat.py`](packages/aci-protocol/tests/test_schema_compat.py)). The
repo-root [`scripts/check-contracts.sh`](scripts/check-contracts.sh) runs the
full regenerate-and-compile sweep. CI enforces it as the `contracts-ts` job
([`.github/workflows/ci.yaml`](.github/workflows/ci.yaml)), which also
`git diff --exit-code`s the generated TypeScript.

A task that needs either package to change **stops and escalates** rather than
working around it — see [`CLAUDE.md`](CLAUDE.md).

## Deployment, CI, and release

**The chart** ([`charts/curie`](charts/curie)) is an umbrella that brings up:

- Postgres, Valkey, Langfuse, ClickHouse, RustFS, and the OTel Collector
- Deployments/Services for api/dispatcher/mail-adapter/worker/ui (the dispatcher has no inbound port and so no Service)
- the mail adapter is **off by default** (`mailAdapter.deploy: false`) and, unlike the dispatcher, does get a Service: the worker POSTs reply events to it ([`charts/curie/templates/mail-adapter.yaml`](charts/curie/templates/mail-adapter.yaml))

Templates live under [`charts/curie/templates/`](charts/curie/templates).
Security rails are all chart defaults (ADR-0006,
[`docs/adr/0006-security-rails-as-chart-defaults.md`](docs/adr/0006-security-rails-as-chart-defaults.md)):

- **Default-deny egress NetworkPolicy** with an explicit `except: 169.254.169.254/32` carve-out so the cloud metadata endpoint stays blocked ([`charts/curie/templates/security-networkpolicy.yaml`](charts/curie/templates/security-networkpolicy.yaml)).
- **gVisor RuntimeClass** option on the runner, plus a preflight Job that runs under the class and fails if the kernel is not gVisor ([`charts/curie/templates/preflight-gvisor.yaml`](charts/curie/templates/preflight-gvisor.yaml)).
- **AVX (a CPU instruction-set extension)/ClickHouse preflight** — a blocking pre-install hook that fails when the CPU lacks AVX and the ClickHouse tag is not the SSE4.2-safe pin ([`charts/curie/templates/preflight-avx.yaml`](charts/curie/templates/preflight-avx.yaml)). It is the productized form of the `:24.8` pin documented in [`CLAUDE.md`](CLAUDE.md).
- **Bundle-fetch init containers** on the sandbox template, fail-closed if a bundle ref is set but no archive is fetched ([`charts/curie/templates/agent-sandbox.yaml`](charts/curie/templates/agent-sandbox.yaml)), with a RustFS egress carve-out.
- **A chart-managed platform Secret** ([`charts/curie/templates/secrets.yaml`](charts/curie/templates/secrets.yaml)) carries:
  - backing-store passwords
  - Langfuse keys
  - the model `agentCredentials`
  - the API key
  - the GitHub webhook secret
  - Slack tokens

  **Per-agent connector secrets are deliberately a separate Secret** ([`charts/curie/templates/agent-connector-secrets.yaml`](charts/curie/templates/agent-connector-secrets.yaml)) — secrets isolation; see the per-agent connector secrets section in [The credential path](#the-credential-path) above.

**Install verification status.** As of **v0.4.0-rc.3** the GHCR (GitHub
Container Registry)-default install is proven end to end on a fresh k3s
cluster:

- `helm install` from published sha-pinned GHCR images
- a CLI deploy + chat loop answered through an in-cluster sandbox
- the trace confirmed in the in-cluster Langfuse

A subsequent upgrade flipped on real model credentials and an in-cluster Slack
dispatcher (Socket Mode connected from the cluster).

**The cold-start rehearsal passed at rc.3** — a timed, README-only run reached
a real Slack approve/reject click driving a real downstream effect. It is no
longer an outstanding acceptance gate. It surfaced **five** friction findings, not
two. Operational detail lives in [`docs/operations.md`](docs/operations.md).

**Local dev stack** is [`compose.dev.yaml`](compose.dev.yaml): the same backing
components at fixed host ports (see [`CLAUDE.md`](CLAUDE.md)). Every backend
integration test and UI E2E runs against it.

**CI** ([`.github/workflows/ci.yaml`](.github/workflows/ci.yaml)) runs jobs
across three areas — backend/frontend testing, image builds, and the parity
ladder; see the workflow file for the complete, current list. Notable ones:

- `python` (ruff + mypy + pytest) — the one that boots the full compose stack, runs real Alembic migrations on a virgin Postgres (`version_table_schema=curie`, [`apps/api/alembic/env.py::do_run_migrations`](apps/api/alembic/env.py)), and runs the whole workspace pytest suite against those live services
- `rust`, `rust-build` (the release binary), `contracts-ts`, `ui` (lint + vitest + build + headless Playwright)
- `images`, `worker-local-image`, `dispatcher-image-smoke`, `mail-adapter-image-smoke` — the **image build gates**. An operator reading this list to know what protects a release needs them named, since a green `python` says nothing about whether the images build.
- `eval-falsifiability`, `commit-messages` (no AI attribution)
- `e2e-ladder`, `e2e-ladder-release`, `e2e-ladder-cluster` — the parity ladder's three rungs, each its own job, gated by an internal `changes` path filter

**Release** ([`.github/workflows/release.yaml`](.github/workflows/release.yaml))
publishes `ghcr.io/curie-eng/curie-{runner,api,dispatcher,mail-adapter,worker,ui}` as
multi-arch (`linux/amd64` + `linux/arm64`) manifests (both `latest` and long-SHA
tags) on every push to `main`. It also publishes a seventh image,
**`ghcr.io/curie-eng/curie-worker-local`** (the worker-local overlay, built and
merged by its own `worker-local-build` / `worker-local-merge` jobs). A `v*` tag
additionally cuts a GitHub Release with CLI binaries for
`x86_64-unknown-linux-gnu` and `aarch64-apple-darwin`. It also runs the `chart`
job, which packages the **Helm chart** and releases the **compose** artifact.
The chart and compose files are release artifacts in their own right: an
operator installs from those, not from the images alone.

## What is built vs deferred

**Built and live-verified end to end.** This covers a real Slack conversation
on a real model, a local middle-mode loop, and the GHCR-default install
rehearsal on a fresh k3s cluster (see [Deployment, CI, and release](#deployment-ci-and-release) for the install-verification detail).
The following are built and verified:

- the frozen contracts
- the API (agents/versions/deployments, git-driven deploys, evals, Langfuse proxy, bundle pipeline)
- the runner
- the dispatcher
- the worker kernel and its four invariants
- both substrate clients
- the eval plane
- the chart with its security rails
- the CLI
- the wired UI ([see The UI section for the full surface list](#the-ui-always-the-real-api-no-demo-mode))

**Deferred:**

- ripping out the UI fixture/showroom surface (the code is still in the tree; wired-and-live is the target, [the UI section](#the-ui-always-the-real-api-no-demo-mode))
- binding the UI's eval matrix view to the live `GET /evals/matrix` endpoint ([Pushing agent versions with git](#pushing-agent-versions-with-git-deploy-flow))
- **running** the soak/chaos suite at N1 scale (the suite itself is 762 lines of real Python, env-gated on `CURIE_SOAK` — what is deferred is the run, not the code)
- the **live cluster run** of the email channel (the adapter itself ships: [`apps/mail-adapter`](apps/mail-adapter) with its test suite, its `mail-adapter-image-smoke` gate and its chart wiring behind `mailAdapter.deploy`; what is deferred is the on-cluster send-and-reply rehearsal, so email is not yet in the live-verified list above)
- the Interview-Me onboarding compiler
- automatic memory generation
- the **native OpenAI wire format**

These are tracked in [GitHub issues](https://github.com/curie-eng/curie/issues).

Three things previously listed here have **shipped** and are called out
because a stale Deferred list understates the product:

- sandbox identity is surfaced ([`apps/api/src/curie_api/langfuse.py::hoist_sandbox_id`](apps/api/src/curie_api/langfuse.py))
- the timed README-only cold-start rehearsal **passed at v0.4.0-rc.3** ([Deployment, CI, and release](#deployment-ci-and-release))
- non-Anthropic providers are built — Zhipu, Moonshot, DeepSeek, and OpenRouter all route today, plus opt-in local Ollama ([The credential path](#the-credential-path))

Only the OpenAI *wire format* is
genuinely absent.

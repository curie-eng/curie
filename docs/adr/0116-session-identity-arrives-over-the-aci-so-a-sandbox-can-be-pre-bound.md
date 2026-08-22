# 116. Session identity arrives over the ACI so a sandbox can be pre-bound

Date: 2026-08-20

Status: Draft

Extends [ADR-0059](0059-sandbox-is-a-bounded-resource-envelope.md) into the
dimension it deliberately left out of scope -- **throughput and the wall-clock
deadline on the claim path** -- and revisits the resume framing of
[ADR-0003](0003-stateless-first-rehydrate-on-resume.md) without displacing it.
It changes no isolation boundary: [ADR-0006](0006-security-rails-as-chart-defaults.md)'s
rails and [ADR-0008](0008-multi-tenancy.md)'s namespace-per-tenant compute stand
exactly as they are.

## Context

A Curie turn is almost entirely waiting on a remote model. Measured on the
`skill` tier against a real (local Ollama) model, a turn consumed **2.3% of one
core** for a 2-output-token reply and **8.8%** for a 354-output-token reply; the
rest of the wall clock was I/O wait. An idle runner consumed **0.043% of one
core**. Curie is an I/O broker, and its steady-state compute cost is close to
nothing.

Almost none of the platform's resource cost is therefore inherent to serving a
turn. Two things dominate, and neither is the turn:

**1. Every new conversation pays a cold sandbox create on its critical path.**
Measured end to end on a Kubernetes cluster -- idle 12-core node, runner image
already resident (`imagePullPolicy: Never`), a 6,961-byte bundle, gVisor off --
a `SandboxClaim` took **17.39 seconds** to reach a ready runner. That is the
best case, and it is guarded by a hard `claimTimeoutSeconds: 90` that must stay
under the worker's 120s thread-lock TTL.

The composition matters more than the total:

| Phase | Measured | Share |
| --- | --- | --- |
| claim admitted, sandbox named, pod created | 0.75s | 4% |
| pod scheduling and container starts | 1.93s | 11% |
| `bundle-fetch` (`aws-cli` `s3 cp`) | 4.11s | 24% |
| `bundle-extract` (`busybox` tar) | 0.42s | 2% |
| container transition | 1.25s | 7% |
| runner boot until `/healthz` answers | 8.93s | 51% |

The Kubernetes machinery is 4% of it. The cost is the bundle work and the
runner boot -- and the bundle work is almost pure fixed overhead: the pod events
show `bundle-fetch` running for **five wall-clock seconds to download 6,961
bytes**. That is `aws-cli` process startup, configuration, and endpoint
resolution, not transfer. It is paid on every pod generation, for an artifact
that is immutable and content-addressed, and ADR-0003's resume is a new pod, so
it is paid again on every resume.

**2. A thread pins a sandbox for up to an hour after its last message.**
`routeTtlSeconds` defaults to 3600. A live runner against a real model measured
**~505 MiB** in its cgroup, of which **233.9 MiB is private anonymous memory**
and 272.4 MiB is file-backed page cache shared per node. The private cost splits
across two processes: the SDK's bundled Claude Code child at **259.5 MiB RSS**
and `python -m curie_runner` at **74.1 MiB**. Conversation growth is mild and
stable -- **+1.6 MiB per turn** averaged over ten turns, projecting ~163 turns
before the 768Mi container limit.

So an idle thread holds roughly **334 MiB of marginal memory at 0.043% of a
core** for up to 59 minutes after anyone stopped talking to it. The chart says
plainly why that is not merely wasteful:

> Thread affinity is the point -- a follow-up should reuse the warm sandbox
> rather than pay a ~30s cold create again. But traffic to a triage bot is
> mostly ONE-SHOT: someone asks, reads the answer, never replies. At the 3600s
> default that sandbox holds a slot for the remaining ~59 minutes, and with
> `agentSandbox.warmPool.replicas: 0` every new thread cold-creates alongside
> it. On a small node enough of them accumulate that a cold create stops
> fitting inside `claimTimeoutSeconds`, and the turn escalates as an opaque
> `runner-error`.

### The warm pool cannot absorb this today

`agentSandbox.warmPool.replicas` defaults to **0**, and the chart records that
this is deliberate rather than an oversight:

> Default 0 = no pre-warmed pods; claims cold-create sandboxes from the
> template (the intended behavior). Raise it only for a fake-model/dev pool
> where an unbound warm pod boots cleanly -- a real-model warm pod has no
> bundle (`CURIE_PLUGIN_DIR=/unused`, no `CURIE_BUNDLE_REF`), raises
> `PluginBundleError`, and CrashLoopBackOffs, and a real claim cold-creates
> anyway (per-claim env injection forces a cold create, the
> `envVarsInjectionPolicy: Overrides` gotcha).

Both halves were reproduced while writing this ADR. A `SandboxClaim` carrying no
bundle ref reached a running runner in 3.05s and then failed; a claim carrying a
bundle ref but not `CURIE_PLUGIN_DIR` failed the same way, with
`PluginBundleError: invalid plugin bundle at /unused: bundle path is not a
directory: /unused`.

The mechanism is the point. **A sandbox's capability is decided by pod
environment, injected per claim.** A pod that does not yet know its claim
therefore cannot be useful, so the pool can only hold pods that are warm in the
cheapest dimension (scheduling) and cold in the two expensive ones (bundle and
boot). The fast-bind path exists in the substrate and is architecturally
unreachable.

### Why this is an availability decision, not a cost one

The chart records an incident, twice:

> This was `cpu: "2"`, and on a 2-vCPU node that is the WHOLE machine. A
> Langfuse merge burst then took ~175% CPU with 0.0% idle, and every new
> sandbox pod -- one per Slack thread, on a hard 90s claim-bind deadline --
> missed it. Three attempts timed out and the turn escalated to a human as an
> opaque `runner-error`. Nothing flagged it: CPU starvation sets no node
> condition, so pods were Running, probes green, disk fine.

and, on the recurrence after halving the ClickHouse ceiling:

> Necessary but NOT sufficient, which the recurrence proved. Capping at 1 core
> halved the blast radius and left the cause untouched.

"Three attempts" is `WorkerConfig.max_attempts`, default **3**
([`apps/worker/src/curie_worker/config.py`](../../apps/worker/src/curie_worker/config.py)).

The chart also names the amplifier:

> under contention the kernel divides CPU in proportion to REQUESTS, not
> limits, and the critical-path containers (bundle-fetch, runner) request 50m
> each. A limit is a ceiling, never a guarantee.

Confirmed against the live release: the `curie-runner` SandboxTemplate requests
`cpu: 50m` for the runner, `bundle-fetch`, and `bundle-extract` alike, while
ClickHouse requests `cpu: 200m` and may burst to a full core. **The critical
path holds a smaller share of the CPU than the observability database it is being
observed by.** Read off the live node, the weights the kernel actually divides by
are 11 for a 50m sandbox and 29 for ClickHouse, so the sandbox takes 27.5% when
the two contend. The evidence section below has the full mapping and the reason
the declared 4x difference in millicores compresses to 2.6x in weight.

The general shape of the defect is this: **an absolute wall-clock deadline sits
on the path with the smallest relative resource share in the cluster.** The
deadline is a fixed 90 seconds; the share is proportional and can be divided
away by any neighbour that requests more. Tightening the neighbour is symptom
treatment; it was tried twice and the second attempt only halved the blast
radius.

How far a given share stretches a 17.39s boot is not something to derive on
paper, because the boot is a mix of CPU work, I/O waits, and scheduling rather
than a single CPU-bound stretch. It was measured instead: under a reproduced
saturation the same claim crossed the 90-second deadline at 91.02s and was still
not ready at 110s. Making the bind sub-second removes the deadline from reach
instead, and that was measured too, at 7.86s under the identical load.

## Decision

**Session identity is delivered to a sandbox over the ACI after it is bound,
not through pod environment at claim time. A sandbox's pod environment carries
only what an agent version needs to exist; what a conversation needs to exist
arrives afterwards.** The claim path becomes a bind to an already-booted,
already-bundle-loaded runner.

1. **Pod environment carries version-scoped capability only.** The bundle ref,
   plugin dir, model, budget, and boot contract are properties of an *agent
   version*, and they are baked into that version's SandboxTemplate. Two
   sandboxes of the same version are interchangeable before a conversation
   claims one.

2. **Session-scoped state arrives with the turn, not with the pod.**
   `CURIE_SESSION_ID` and `CURIE_HISTORY_REF` stop being pod env and become
   **optional fields on the ACI `Event` frame** the worker already posts to
   `/v1/event`. This is the load-bearing half: while capability is decided by pod
   env, a pre-warmed pod cannot be useful, and no amount of pool tuning changes
   that.

   Carrying them on the existing frame rather than adding a session-lifecycle
   endpoint is deliberate. The frame already carries per-turn identity (`user`,
   `ts`, `type`), so session identity belongs to the same shape; the change is
   additive, which under [ADR-0036](0036-aci-semver-and-reader-policy.md)'s
   reader policy means a runner that predates the fields ignores them and keeps
   working, so there is no new surface to authenticate, route, or gate. An
   earlier draft of this ADR proposed a `POST /v1/session` endpoint; it was
   rejected during review for buying the same thing at a higher price.

   **The runner token stays pod env, the pool mints it, and the worker reads it
   instead of minting it.** A pre-warmed pod cannot be handed a token that is
   minted when a conversation claims it, so the pool mints one per pod at creation
   and the worker resolves the bound pod's token through the Kubernetes API at
   bind time. This is a **requirement, not a convenience**: a pool pod today
   carries no token, and a runner with no token configured passes its gated paths
   through, which was confirmed by an unauthenticated `POST /v1/event` returning
   200 against a bound pool pod. Raising the pool without minting a token ships an
   open ACI endpoint per warm pod.
   The worker already holds the RBAC to read those pods, and the token's exposure
   is unchanged from today, where it is pod env as well. Per-pod rather than
   per-pool matters: a shared pool token would let one compromised sandbox
   authenticate as its siblings.

3. **The warm pool is keyed by deployed version, not generic.** The platform
   already knows exactly which versions are in force -- `curie.deployments`
   holds that row -- so a pool is rendered per active deployment and its pods
   pre-fetch, pre-extract, and pre-boot that version. A claim binds a ready
   runner. Pool sizing is an operator knob with a bounded total, and a
   deployment with no recent traffic keeps zero pre-warmed pods and accepts the
   cold path.

4. **An immutable bundle is fetched once per node, not once per pod.** The
   bundle is content-addressed, so `bundle-fetch` and `bundle-extract` leave
   the per-pod critical path in favour of a node-local, digest-keyed,
   read-only cache. The `aws-cli` container leaves the boot path with them.

5. **Residency becomes short because re-binding is cheap, and its length is
   derived from the prompt cache TTL rather than chosen.** With a sub-second bind,
   `routeTtlSeconds` stops being the term that decides how many sandboxes exist.
   An idle thread releases its runner and re-binds on its next message.
   ADR-0003's rehydrate-from-history remains the correctness contract for a
   resumed thread; this decision only makes the release affordable.

   **A cheap re-bind fixes the latency of a resume and not its token cost.** A
   pre-bound pod is a *fresh* runner with no transcript, so a returning thread
   still rehydrates, and ADR-0003 records that a rehydrate is cache-cold. Measured
   here: 20,875 input tokens per turn on a *scaffolded* bundle, before any
   conversation history is replayed on top. So residency is not a quantity to
   drive toward zero. It has a natural ceiling and a natural floor:

   - **Past the prompt cache TTL, holding buys nothing on tokens.** The cache is
     gone whether the sandbox is held or not, so every second beyond it spends a
     quota slot for the avoided rehydration overhead alone.
   - **Below the TTL, releasing throws away a real saving.** ADR-0003 measured
     `cache_read_input_tokens = 16045` inside one claim, exactly the value the
     first call created.

   So the target is **on the order of the cache TTL**, which makes today's 3600s
   wrong in a second way this ADR had not named: it is not merely wasteful of
   slots, it is far longer than the window in which holding pays for itself.

   The trade also depends on traffic shape, and the shape favours releasing. The
   chart's own note is that triage traffic is mostly one-shot, and a one-shot
   conversation has no second turn, so there is no cache to lose and short
   residency costs it nothing. A long multi-turn thread is where releasing trades
   tokens for slots, and that is the case an adaptive value should protect.

6. **The critical path declares a real resource share, and the timeouts on it
   survive starvation.** Runner and bundle containers request from measurement
   rather than convention, and the turn plane carries a `PriorityClass` above the
   observability plane, extending ADR-0059 decision 5's platform-over-sandbox
   ranking to a second axis. Every absolute timeout the claim path waits on --
   `claimTimeoutSeconds` and the runner's own readiness probe alike -- is sized
   against a starved node rather than a quiet one, because a timeout that only
   holds when nothing else is running is not a timeout, it is an assumption.

   **This decision is ordered after decisions 1 through 4, not independent of
   them.** `requests.cpu` is both the scheduler's reservation and the kernel's
   contention weight, so lowering it toward the measured idle need also shrinks
   the share the sandbox gets in a fight. That is safe only once the
   deadline-bound CPU work has left the claim path. Applied first, in isolation,
   it makes the recorded incident more likely rather than less.

**The residency invariant** (what we test and review to): a conversation that
is not currently being served holds no sandbox, and a conversation that becomes
active binds one in under a second on a quiet node **whenever a warm pod of its
version is available**. The qualifier is load-bearing: measured under a burst
that empties the pool, the claim behind it is a cold create again and throughput
is unchanged. Pool depth against arrival rate is what keeps the invariant true,
and sizing it is an operator obligation this decision creates rather than
removes.

## Business case

**The problem has already cost user-visible turns twice, in production, with no
alarm.** Both occurrences were the same failure -- three attempts timing out
into an opaque `runner-error` escalated to a human -- and both were invisible to
monitoring, because CPU starvation sets no node condition. Pods were Running,
probes were green, disk was fine. A platform whose failure mode is a silent
wrong answer to the person who asked is not a platform anyone deploys twice.

**It puts a floor under the hardware Curie can be sold on.** Curie's
distribution story is self-hosting: someone runs it on their own cluster. On a
2-vCPU node the platform's own CPU requests total 1735m -- **87% of the machine
before a single agent runs** -- and the observability stack alone is 4096Mi of
the 7136Mi of memory limits, 57% of the total. That means the smallest honest
recommendation today is not small, and the cheapest node is the one the incident
happened on. Turning "you need a real cluster" into "two cores is enough" moves
Curie from an infrastructure commitment to a thing someone tries on a spare box,
which is the difference between an evaluation and an install.

**It stops the correctness model from costing what it costs.** A sandbox per
conversation is not, in this repository's records, a security boundary. It falls
out of [ADR-0013](0013-concurrency-and-delivery-model.md)'s **one live session per
thread**, which is a routing CAS on a Valkey lock and exists to stop duplicate
side effects; the security boundary is the tenant, per
[ADR-0008](0008-multi-tenancy.md)'s namespace-per-tenant compute, whose stated
reason is "untrusted, prompt-injectable code that holds customer credentials" and
which says nothing about threads. An earlier draft of this section called the
per-conversation sandbox Curie's strongest security claim; that was this ADR
inventing a rationale the repository does not assert, and it is corrected here
because it matters for what may and may not be traded away.

What that reframes: the expensive thing is a *correctness* invariant, not a
security one, and it is expensive for reasons that have nothing to do with
either. The cost is not the separation -- an idle runner is 0.043% of a core --
it is the 17-second boot and the 59 idle minutes. Remove those and the
correctness model stops being the reason capacity is scarce, without anyone
having to argue about weakening it.

**Under the hosted multi-tenant future ADR-0008 anticipates, residency is
gross margin.** Idle sandboxes are the dominant unit cost of a hosted Curie,
and one-shot triage traffic is the common case, so the ratio between useful
seconds and paid seconds is close to the whole margin. Nothing else on the
roadmap moves it by a comparable factor.

## Evidence (measured 2026-08-20)

All figures below were measured, not estimated. Cluster measurements are from a
minikube node reporting 12 CPU / 7.75 GiB / ~1007 GiB ephemeral, running release
`curie-0.7.0`, with `security.gvisor.mode=off`. Runner-process measurements use
cgroup v2 accounting (`cpu.stat`, `memory.current`, `memory.stat`) rather than
sampled `docker stats`.

**Cold create, real bundle** -- `SandboxClaim` applied directly with a deployed
bundle ref and `CURIE_PLUGIN_DIR`, timed by polling claim and pod status:

```
 0.61s  claim appeared          3.29s  init RUNNING bundle-fetch
 0.99s  sandbox assigned        7.40s  init DONE    bundle-fetch
 1.36s  pod phase Pending       7.82s  init DONE    bundle-extract
                                9.07s  container RUNNING runner
                               18.00s  container READY runner
```

Claim-to-ready **17.39s**. Kubernetes events independently place `bundle-fetch`
at 18:07:49 → 18:07:54, i.e. **~5s for a 6,961-byte object**.

**The same runner on the `skill` tier**, Docker, no init containers: **1.28s**
to ready under the fake model, **4.85s** under a real model. The cluster path
costs an order of magnitude more for identical work.

**Turn cost, real model** (local Ollama, `qwen2.5:0.5b`, so wall time is
dominated by local inference and the CPU share is if anything overstated
relative to a remote API):

| Turn | Wall | CPU | CPU / wall | Tokens in / out |
| --- | --- | --- | --- | --- |
| short | 9.22s | 0.211s | 2.3% | 20,875 / 2 |
| long | 11.66s | 1.030s | 8.8% | 20,933 / 354 |

Per-turn cost is roughly a 0.2 CPU-second fixed component plus ~2.3ms per output
token. Note the input side: a **scaffolded** bundle already re-sends 20,875
input tokens per turn, which is the concrete size of the prompt-cache term
ADR-0003 chose to model as cache-cold on resume.

**Idle cost**: 0.0085 CPU-seconds over 20 seconds, **0.043% of one core**, with
memory flat.

**Memory, real model**: cgroup `memory.current` ~505 MiB, `anon` 233.9 MiB,
`file` 272.4 MiB. Process split inside the container: SDK-bundled Claude Code
child **259.5 MiB RSS**, `python -m curie_runner` **74.1 MiB**. `/v1/reset`
returns the child from 298 MiB to 259 MiB, confirming one session per child
process, replaced rather than accumulated. Conversation growth **+1.6 MiB/turn**
over ten turns.

**Declared versus measured, per sandbox:**

| Axis | Chart request | Measured | Note |
| --- | --- | --- | --- |
| CPU | 50m | 0.43m idle, ~90m active | **116x over-reserved at idle** |
| memory | 192Mi | ~505 MiB cgroup, 334 MiB marginal | **under-reserved; the 768Mi limit carries it** |

The CPU over-reservation is what caps density on a small node: 1735m of platform
requests plus 50m per sandbox leaves room for about five sandboxes on two cores,
while the measured idle need is two orders of magnitude smaller.

**Reproduced failures**: `PluginBundleError: invalid plugin bundle at /unused`,
twice, exactly as the chart's warm-pool comment predicts.

**And a third time, by the repository's own end-to-end harness.** This is the
strongest evidence in this ADR that the coupling is a trap rather than a
curiosity, because nobody was testing for it.

`scripts/chart-runtime-e2e.sh` builds a "bound sandbox" pod from the shipped
SandboxTemplate and asserts it reaches *init containers Completed, runner
Running*. Its binding step replaces `CURIE_BUNDLE_REF` in every container that
declares it, and nothing else. So the init pair does its job perfectly -- the
seeded object is fetched and extracted, and the harness prints both logs proving
it:

```
download: s3://curie-bundles/e2e/probe.tgz to ../bundles/.bundle-archive
extracted bundle 'e2e/probe.tgz' into /bundles/current
```

and then the runner boots with the template's baked `CURIE_PLUGIN_DIR=/unused`
and exits 1 one second later. Observed on the pod:

```
runner:
  State:      Terminated
    Reason:   Error
    Exit Code: 1
    Started:  22:19:26
    Finished: 22:19:27
  Environment:
    CURIE_PLUGIN_DIR:   /unused
```

**The bundle is in `/bundles/current` and the runner is told to look at
`/unused`.** That is decision 1 and decision 3 of this ADR stated as a bug
report, written independently by whoever built the harness.

Two things compound it, and both are the same shape as the incident this ADR
opens with. The harness reports the failure as `bound sandbox did not reach
(init Completed + runner Running) within 180s`, which reads like a slow node.
And it prints `bundle-fetch` and `bundle-extract` logs on failure but **not the
runner's**, so the actual `PluginBundleError` never appears in the output at all.
A correct diagnosis requires describing the pod by hand.

Adding a nine-line `CURIE_PLUGIN_DIR` binding for the runner container made the
same harness pass end to end on the same cluster, including its runtime security
assertions:

```
bound sandbox ready: init containers Completed, runner Running
    /bundles/current/.claude-plugin/plugin.json
PASS: API Service NodePort and runtime security assertions
```

So the defect is confirmed by construction in both directions: present when the
plugin dir is left at the template's default, absent when it is bound. This is
filed separately from this ADR, since fixing a test harness is not an
architectural decision -- but it is why decision 1's split between
version-scoped and session-scoped environment is worth making explicit rather
than leaving to each caller to remember.

### The pre-bind path, measured directly

Decisions 1 and 3 were tested rather than assumed, on the same release, against
an isolated `SandboxTemplate` and `SandboxWarmPool` created alongside the
shipped ones so no live claim was disturbed. The template carried the bundle ref
and `CURIE_PLUGIN_DIR=/bundles/current` **baked in per pool** instead of injected
per claim.

- Two warm pods pre-fetched, pre-extracted, and pre-booted that bundle, reaching
  `readyReplicas: 2` in **~28 seconds**. This is a real-model install, so it also
  shows the chart's "only a fake-model pool boots cleanly" caveat is a
  consequence of the pool being *generic*, not of the pool being warm. A pool
  that knows its version boots fine.
- A `SandboxClaim` carrying **no env at all** bound one of those pods and
  reached a ready runner in **0.19 seconds**:

  ```
  0.08s  claim applied (no env)
  0.13s  bound to sandbox probe-runner-pool-zrp4j
  0.13s  claim Ready=True (DependenciesReady)
  0.19s  RUNNER READY
  ```

  Against the 17.39s cold baseline that is **91x**, and it is the sub-second bind
  the residency invariant depends on.
- The bound pod was genuinely usable, not an empty shell: `/bundles/current`
  contained the bundle's `AGENTS.md`, `evals`, and `skills`. The pool refilled
  itself afterwards without intervention.
- An otherwise identical claim carrying **one** env entry did not bind a pool pod.
  It created its own `Sandbox` named after the claim and attempted a new pod,
  which is the cold-create path. Its wall clock is **not** established here: the
  attempt failed on the namespace quota rather than completing, so only the
  behavioural difference is demonstrated, and a clean timing of that arm is
  outstanding.

### The deadline arm, measured under controlled contention

The claim that pre-binding removes the deadline from reach was tested on a second,
dedicated cluster with a controlled neighbour rather than a hoped-for ClickHouse
burst. The neighbour is a Deployment of busy-loop pods requesting `200m` each,
which is what ClickHouse requests, against the runner and bundle containers'
`50m`. Two full runs, and the harness and its recording are in
[`prototypes/adr-0116-residency/`](../../prototypes/adr-0116-residency/):

|                      | quiet node | under contention |
| -------------------- | ---------- | ---------------- |
| today (cold create)  | 4.72s      | **never ready**  |
| pre-bound            | **0.17s**  | 7.79s            |

Under contention today's path crossed `claimTimeoutSeconds` at 91.02s and was
still not ready when the harness stopped at 110s. In production the worker gives
up at 90s, and three of those escalate as an opaque `runner-error` -- the recorded
incident, reproduced on demand rather than waited for.

The pre-bound arm under the same contention took 7.79s. It degrades, and by a
large factor, but 7.79s against a 90-second ceiling is the point: **the deadline
stops being reachable.** That is the residency invariant holding under the exact
condition that broke it twice.

The cold baseline is cluster-shaped and this cluster is generous to it: fake
model, no observability stack resident, a 6,961-byte bundle, gVisor off, so 4-5s
is its floor against the 17.39s measured on the real-model install above. The
column that carries the argument is the second one, where the shape of the
failure does not depend on the baseline.

### gVisor does not touch the bind, and a warm pod is unauthenticated

Two things the earlier arms could not answer were measured on a third cluster,
this one on containerd with minikube's `gvisor` addon enabled and the chart
installed at `security.gvisor.mode=require`, so `RuntimeClass gvisor` (handler
`runsc`) is what sandbox pods actually run under and the chart's own gVisor
preflight passed.

|                     | gVisor off (docker runtime) | gVisor required (containerd) |
| ------------------- | --------------------------- | ---------------------------- |
| cold create         | 4.72s                       | 7.06s                        |
| pre-bound bind      | 0.17s                       | **0.18s**                    |

**gVisor costs the boot and leaves the bind alone.** That is the expected shape:
`runsc` intercepts syscalls, so it taxes booting and running a process, while a
bind is a control-plane operation plus a readiness check against a process that
is already up. It also means the argument for pre-binding is *stronger* under the
production default, not weaker, because the cost it removes is the one gVisor
inflates. The two clusters differ in container runtime as well, so 4.72 to 7.06
is not a clean gVisor-only delta; the column that matters is the second row,
where the number does not move.

The sub-second bind has now been measured four times across three clusters:
0.19s, 0.196s, 0.17s, 0.18s.

**A pre-warmed pod exposes an unauthenticated ACI.** The runner gates
`/v1/event`, `/v1/steer`, `/v1/interrupt`, and `/v1/reset` on a bearer token, and
that token is minted per claim by the worker and injected through
`SandboxClaim.spec.env`. A pool pod is created by the pool controller from the
template, so it receives the template's baked env and **no token at all** -- and
the runner's documented behaviour with no token configured is to pass the gated
paths through. Confirmed directly against a bound pool pod:

```
POST /v1/event, no Authorization header
-> HTTP 200 ACCEPTED WITHOUT AUTH
```

The turn then failed at the model boundary on a deliberately bogus credential,
which is its own confirmation: **a pre-bound pod does serve ACI turns**, up to the
model call, with no further work. But it serves them to anyone who can reach it.

Today this is latent, because `warmPool.replicas` is 0 and no warm pod exists.
Decision 3 is what makes it real: raising the pool ships one open ACI endpoint per
warm pod inside the cluster network. That is why decision 2's per-pod token is a
requirement of this ADR rather than an implementation detail of it.

### Tuning the request cannot win, because of where the pod sits in the tree

The declared-versus-measured table above invites an obvious move: the runner
requests 50m and needs 0.43m at idle, so lower it. That move is wrong on its own,
and the reason is worth recording because it is the same reason the incident
happened.

`requests.cpu` decides two unrelated things at once. It is the amount the
scheduler reserves, which sets density, and it is the weight the kernel divides
contested CPU by, which sets resilience. A low-duty-cycle, latency-sensitive
workload wants opposite values for the two, and Kubernetes has no field that
separates them.

Read from the live node, the weights are not a metaphor. `requests.cpu` lands in
`cpu.weight`, and `limits.cpu` lands in `cpu.max`:

| pod | `requests.cpu` | `cpu.weight` | `limits.cpu` | `cpu.max` |
| --- | --- | --- | --- | --- |
| prewarm | 10m | 4 | 50m | `5000 100000` |
| valkey (same request as a sandbox) | 50m | 11 | 250m | |
| api / worker / postgres | 100m | 17 | | |
| langfuse-web | 150m | 24 | | |
| clickhouse | 200m | 29 | 1 | `100000 100000` |

So a sandbox contends at weight 11 against ClickHouse's 29. **That is 2.6:1, not
the 4:1 the declared millicores suggest** -- the conversion has a positive
intercept, so a 4x difference in request compresses to 2.6x in weight. An earlier
draft of this ADR quoted the millicore ratio; the weight ratio is what the kernel
divides by.

The tree matters more than the ratio:

```
/sys/fs/cgroup/kubepods            weight=469
├── burstable                      weight=68     <- all 16 running pods
│   ├── clickhouse   29
│   ├── valkey       11            <- a sandbox sits here too
│   └── ...
└── besteffort                     weight=1
   (Guaranteed pods sit directly under kubepods, as siblings of burstable)
```

A Burstable pod competes twice: against its siblings inside the slice, and then
the slice competes above. A Guaranteed pod competes once, at the top, against the
**entire** burstable slice as a single opponent. Guaranteed is therefore not a
bigger number, it is a different position -- and it is unreachable here, because
it requires `requests.cpu == limits.cpu`. At `limits.cpu: 1` that reserves a core
per sandbox and the namespace `requests.cpu: 4` quota caps concurrency at four,
worse than today's eight; at 200m it hard-throttles a legitimately busy agent,
which decision 6 of ADR-0059 exists to prevent.

**The conclusion is that no setting of these knobs is correct while
deadline-bound CPU work sits on the claim path.** Decisions 1 through 4 remove
that work: pool refill runs on a background schedule with no deadline, so its
share becomes a throughput question rather than a correctness one, and the bind
that remains inside the deadline costs almost no CPU. Only then is a small
request right for both of its jobs at once. **The ordering is load-bearing:
lowering the request before the boot leaves the critical path makes the incident
more likely, not less.**

### The same defect appears on probe timeouts, and there it kills

The shape this ADR is about -- an absolute timeout on a proportionally starved
path -- is not unique to `claimTimeoutSeconds`. It is also in the probes, and
measured while testing the arms above.

**On the claim path, and therefore in scope here.** The runner's readiness probe
is what `_claim_fresh` waits for, and it is configured
`periodSeconds: 2`, `timeoutSeconds: 2`, `failureThreshold: 30`. A probe that
answers in milliseconds on a quiet node can exceed a 2s exec timeout on a starved
one, and every spurious failure adds a period to the claim. The wall-clock
deadline the claim races is therefore partly made of probe timeouts, which starve
in exactly the same proportion as the boot they are measuring.

**On the data tier, and therefore not in scope here.** Postgres declares:

```yaml
livenessProbe:
  exec: ["sh", "-c", "pg_isready -U postgres"]
  initialDelaySeconds: 20
  periodSeconds: 10
  # timeoutSeconds   unset -> Kubernetes default 1
  # failureThreshold unset -> Kubernetes default 3
```

The **readiness** probe on the same container sets `timeoutSeconds: 5` and
`failureThreshold: 12` explicitly. The **liveness** probe, the one that kills, is
left on defaults: three consecutive `pg_isready` invocations that each fail to
complete inside one second restart the database. It is an exec probe, so each
attempt forks a shell and runs a binary inside the container, which is not
something a starved node completes in a second.

Observed directly on a starved node: `curie-postgres-0` at **9 restarts**, each
with `lastState.terminated.reason=Completed` and `exitCode=0`, its log reading
`received fast shutdown request`. A healthy database, killed by its own liveness
probe, nine times, while nothing was wrong with it.

### Under sustained load, pre-binding buys latency and not throughput

The sub-second bind above is a single claim finding a warm pod waiting. Whether
one is waiting is a capacity question, and it was tested rather than assumed:
60 one-shot conversations across three regimes, each conversation counted only
when a real ACI turn came back with a terminal frame, on the shipped 8-slot
namespace quota.

| regime | setup | claim p50 | claim p95 | throughput |
| --- | --- | --- | --- | --- |
| burst exceeds pool depth | 24 conversations, pool 3, 5 concurrent | 6.22s | **11.40s** | **1.0x** |
| pool depth >= burst | 4 conversations, pool 4 | **0.21s** | 0.22s | 4 in 0.4s |
| arrivals below refill rate | 8 conversations, 9s apart, pool 3 | **0.14s** | 0.18s | all sub-second |

The cold-create arm on the same cluster ran 24 conversations at p50 5.25s and
p95 7.56s, 48.3 per minute. **Pre-binding matched it at 46.7 per minute.** In the
first regime only the first three conversations were served sub-second, one per
warm pod; every claim after that waited on a refill, and a refill is the same
cold create the claim used to do inline.

**So the total work is unchanged.** Pre-binding does not remove a boot, it
prepays one. What it removes is the boot from the conversation's critical path
and, with it, from the 90-second deadline. That is the availability fix this ADR
argues for, and it is worth having on its own. It is not a capacity fix, and this
ADR should not be read as claiming one.

**The claims elsewhere in this document are scoped accordingly.** "0.18s" and
"the deadline is no longer reachable" hold *while a warm pod is available*. When
a burst empties the pool, the claim behind it is a cold create and the deadline
is reachable again, exactly as before. Pool depth is therefore not a tuning
detail; it is what decides whether the availability property holds under a given
arrival pattern.

**And a pool that is too shallow for the load is worse than none.** In the first
regime the pre-bound arm's p95 was 11.40s against the cold path's 7.56s. Two
reasons, both structural: the three pool pods hold three of the eight quota
slots, so the same concurrency has less room for bound sandboxes; and once the
pool is empty, refills queue against each other and against live sandboxes for
the same node and the same quota. Spending budget on a pool that cannot cover the
burst buys a worse tail.

Correctness held in every regime: 60 of 60 conversations returned a terminal
frame, no failures, no deadline crossings. What varied was only how long the
claim took.

#### That comparison measured the wrong baseline

The arms above are not "before and after". **Both released the claim as soon as
the turn returned**, so both already ran with a residency of effectively zero.
The shipped default is `routeTtlSeconds: 3600`. So the cold arm was not the
product's behaviour; it was an already-optimised baseline that happens to isolate
one variable, the claim path, and it isolates it well. It just is not what a
deployment does today.

Against the shipped default the binding constraint is not the claim at all, it is
the quota divided by the residency. Eight sandbox slots, each held for the hour
after its last message, is **eight one-shot conversations per hour**, and the
chart says what happens next:

> At the 3600s default that sandbox holds a slot for the remaining ~59 minutes,
> and with `agentSandbox.warmPool.replicas: 0` every new thread cold-creates
> alongside it. On a small node enough of them accumulate that a cold create
> stops fitting inside `claimTimeoutSeconds`, and the turn escalates as an opaque
> `runner-error`.

**The failure this ADR opens with is that sentence.** It is not a burst problem;
it is idle accumulation. Slots fill with conversations nobody is talking to, and
the next real one cannot get a sandbox.

Which reorders what this decision buys:

| | bound by | measured or arithmetic |
| --- | --- | --- |
| shipped today | quota / residency: 8 slots at 3600s | **8 per hour**, arithmetic from the defaults |
| with short residency | claim path: 8 slots turning over | **48.3 per minute**, the cold arm above |
| with short residency and pre-binding | refill rate | **46.7 per minute**, the pre-bound arm above |

So the capacity gain is roughly **two orders of magnitude**, and it comes from
**decision 5**, not from pre-binding. Pre-binding's contribution is that it makes
decision 5 affordable: shortening residency without a cheap re-bind trades a
compute saving for a token bill, since a resumed thread is cache-cold and a
scaffolded bundle already re-sends 20,875 input tokens per turn.

The earlier reading -- "pre-binding buys latency and not throughput" -- is true of
the comparison as run and false as a summary of the decision. Stated correctly:
**pre-binding does not raise capacity directly; it removes the reason capacity is
being spent on idleness.** What remains genuinely unimproved is a simultaneous
burst larger than the pool, which the three regimes above do measure, and which
was never the failure mode this ADR set out to fix.

#### The residency claim, measured

Rather than leave that as arithmetic, it was run. Fourteen one-shot
conversations, one arriving every 3 seconds, against the shipped 8-slot quota,
each counted only on a terminal ACI frame. The only difference between the arms
is whether a conversation keeps its sandbox after its turn: 45 seconds, which is
`routeTtlSeconds` scaled down by 80, against releasing immediately.

| | served | **blocked by quota** | throughput |
| --- | --- | --- | --- |
| holds its sandbox 45s | 14/14 | **6** | **6.9 / min** |
| releases at end of turn | 14/14 | **0** | **18.8 / min** |

The claim latencies are the whole story:

```
holding:   1-8    4.5 - 7.2s     took the eight slots immediately
          12-14   ~25.4s         waited
           9-11   ~45.5s         waited out a full residency window
releasing: 1-14   4.8 - 6.4s     uniform, nothing waited
```

**The quota wall appears only when the sandbox is held.** Six of fourteen
conversations hit `exceeded quota: curie-sandbox-quota` in the holding arm and
none did when the slot was released, which is the mechanism this section
predicted, observed directly rather than inferred.

Two things this measurement understates, both in the same direction:

- **The hold was 45 seconds, not 3600.** At the shipped TTL a slot frees once an
  hour, so the eight of them serve **8 conversations per hour** against the 18.8
  per minute measured with immediate release. The measured 2.7x is the scaled
  figure; the shipped ratio is roughly **140x**.
- **Everything eventually succeeded here because the wait fit.** A 45-second wait
  is inside `claimTimeoutSeconds: 90`. At a 3600-second hold the wait is an hour,
  which is not, and three attempts of it is the opaque `runner-error` this ADR
  opens with. The failure mode is not that throughput is low; it is that the
  conversation is dropped.

### A released pod is destroyed, not handed to the next conversation

Decision 5 releases a sandbox as soon as a conversation goes quiet, which is a
new opportunity for a pod that served one conversation to serve another. Holding
for an hour never had that opportunity, so it was checked rather than assumed:
bind, write a marker inside the pod, release, bind again, look for the marker.

```
conversation A bound  pod=iso-pool-44nb6  uid=0dc8eb2b
marker written inside A: curie-iso-marker-A
A released
A's pod destroyed after release: True (4s)
conversation B bound  pod=iso-pool-9dc79  uid=6d959e0c

same pod name as A : no
same pod UID as A  : no
A's marker visible : no
```

**A pod serves at most one conversation and is destroyed on release**, in four
seconds, and the pool replaces it with a fresh one. So the separation that falls
out of ADR-0013's one-live-session-per-thread survives releasing aggressively; it
is not something short residency trades away.

What this decision *does* change on that axis is two things, both recorded
elsewhere in this ADR rather than hidden here: a warm pool pod carries no runner
token until decision 2 mints one, which is why decision 3 cannot land first; and
decision 4's node-local bundle cache is a new shared surface that this ADR
explicitly declines to decide.

### A third, tighter density cap surfaced while testing

The failed with-env claim reported:

```
exceeded quota: curie-sandbox-quota, requested: limits.cpu=1,
used: limits.cpu=8, limited: limits.cpu=8
```

ADR-0059 decision 4's namespace quota is denominated in `limits.cpu` and set to
8, while each sandbox declares `limits.cpu: 1`. The namespace therefore admits
**exactly eight concurrent sandboxes**, even though the same quota allows
`pods: 50` and its `requests.cpu` was only 400m of an allowed 4000m -- 10% used
at the moment the limit-denominated dimension hit 100%.

That is the CPU over-declaration of the table above turned into a hard
concurrency ceiling, and it is tighter than either of the other two caps
(`max_concurrency` at 16, and node-level CPU requests). It is not an argument
against the quota, which exists for good reasons; it is an argument that a
ceiling chosen generously per sandbox becomes a cluster-wide cap when a quota
counts it, which decision 6 is what fixes.

## Alternatives considered

- **Many sessions inside one runner process.** Measured and rejected. Each
  session is served by its own SDK-bundled Claude Code child at 259.5 MiB RSS;
  sharing the process would amortise only the 74.1 MiB Python runner, worth
  **1.25x at ten sessions**. A spike measured this before it reached this ADR;
  it is a minor optimisation, not an architecture.

- **Cutting `routeTtlSeconds` without making the bind cheap.** Rejected: it
  trades compute for tokens. ADR-0003 records that a resume is cache-cold, and
  the measured 20,875 input tokens per turn on a trivial bundle is what that
  costs. Short residency is only affordable once re-binding is cheap, which is
  why decision 5 depends on decisions 1 through 4.

- **Raising `agentSandbox.warmPool.replicas` above 0.** Rejected: it does not
  work, for the reason the chart already documents and this ADR reproduced. A
  generic warm pod has no bundle and crash-loops, and per-claim env injection
  forces a cold create regardless.

- **Constraining the observability plane further.** Rejected as the primary
  remedy: it was done twice, and the chart records that the second attempt
  "halved the blast radius and left the cause untouched". It remains worth doing
  as hygiene; it does not remove a wall-clock deadline from a proportionally
  starved path.

- **Process checkpoint/restore (`runsc checkpoint`) to preserve a live session
  and its prompt cache across idleness.** Deferred, not rejected. It is the
  natural extension of decision 5 and gVisor is already the default sandbox
  runtime, but restoring a process holding an open ACI socket, and a checkpoint
  image containing a live runner token, are each their own decision. This ADR
  deliberately buys most of the benefit without them.

- **Replacing the adopted harness to shrink the 259.5 MiB per session.**
  Rejected under ADR-0007: `claude-agent-sdk` and the Claude Code plugin format
  are the two most load-bearing adopt calls in the system, and the per-session
  floor they set is a cost of that choice, not a defect to engineer away.

## Consequences

- The claim path stops being a boot path. A conversation's first turn no longer
  races a 90-second deadline, so the failure mode that produced two recorded
  incidents is removed rather than mitigated, and `claimTimeoutSeconds` becomes
  a backstop instead of a live constraint.

- **The ACI gains two optional fields, which is the cheapest shape this change
  has.** ADR-0005 freezes the ACI and
  [ADR-0036](0036-aci-semver-and-reader-policy.md) governs how it may change;
  this is a minor version, and because the fields are additive a runner that
  predates them ignores them rather than refusing the frame. The skew that
  remains is the opposite direction: a runner expecting session identity on the
  frame, paired with a worker that still injects it as pod env, boots and then
  serves a turn with no session identity at all. That is a silent wrong answer
  rather than a loud failure, so the rollout order is worker-last, and the
  runner must treat absent fields as "fall back to boot env" for the whole
  compatibility window.

- Pool count becomes a managed resource proportional to active deployments, and
  `git push` is the deploy, so a high-churn repository churns pools. This needs
  a bounded total and a reaper, and it is a new way to exhaust a small node if
  it is unbounded.

- A node-local bundle cache is a new shared surface across tenants. It holds
  immutable, digest-addressed, read-only artifacts, which is the mildest form of
  sharing available, but ADR-0008 hard-silos compute per tenant and this shares
  something beneath that line. It needs its own decision, and this ADR should
  not be read as making it.

- Right-sizing the CPU request raises density and therefore raises the number of
  sandboxes that can contend for one node's cycles. Decision 6's priority class
  is what keeps that from converting a solved latency problem into a new
  starvation problem.

- Decision 6 has to move the CPU **limit** as well as the request, because
  ADR-0059 decision 4's quota counts `limits.cpu`. Leaving a generous per-sandbox
  ceiling in place while raising density means the namespace quota, not the node,
  becomes the first thing a busy release hits, and it does so at eight sandboxes
  on the shipped numbers.

- **There is a way to keep the prompt cache without holding the sandbox, and it
  is worth a decision of its own.** Anthropic's prompt cache is server-side and
  keyed by the request prefix, not by the process that sends it, so a *different*
  runner reconstructing the *same* prefix hits it. Today a resumed thread cannot,
  for two reasons visible in
  [`runner/.../history.py`](../../runner/src/curie_runner/history.py): the
  transcript is delivered as a **boot-prompt preamble** rather than as message
  history, and it is **truncated to roughly 16 KB**. Both change the prefix, so
  every resume is a miss.

  Rehydrating as message history with a stable prefix instead would decouple
  caching from residency entirely: slots could be released aggressively and a
  returning thread would still hit the cache, within the TTL. That is a change to
  ADR-0003's rehydrate delivery and to ADR-0029's design, so it is
  [ADR-0119](0119-a-resumed-thread-rebuilds-its-prefix-so-the-prompt-cache-still-hits.md)
  rather than part of decision 5, and it needs the measurements above first.
  It is called out here because it is the answer to decision 5's main objection,
  and because the 16 KB bound exists for a reason -- something else has to bound
  the prompt if the truncation goes.

- **The token cost of a resume is the largest axis this ADR has not measured.**
  Every figure here for turn cost came from a fake model or a local Ollama, so the
  input-token side of decision 5 is arithmetic on one measured number (20,875 per
  turn on a trivial bundle) rather than an observed bill. Three things need a real
  credential before decision 5's value is set: what a rehydrate actually costs
  against a warm follow-up, what effective cache TTL the harness's requests carry,
  and how the replayed transcript grows with conversation length. Choosing
  `routeTtlSeconds` without them is guessing, and the guess is currently 3600.

- **Pool depth becomes an operational parameter with a sharp edge.** Sized at or
  above the burst, every conversation binds sub-second. Sized below it, the
  surplus conversations cold-create anyway and the tail is worse than having no
  pool, because the pool's own pods hold quota slots that bound sandboxes could
  have used. Measured: p95 11.40s with a pool of three against 7.56s with none,
  on the same 24-conversation burst. An autoscaling signal for pool depth is the
  obvious follow-up and is not part of this decision.

- **Decision 3 cannot land before decision 2's per-pod token.** Shipping a warm
  pool while pool pods carry no runner token would expose an unauthenticated ACI
  on every warm pod, which is a reachability regression rather than the
  performance change this ADR is arguing for. The ordering is not a preference.

- Nothing here changes what a sandbox may *reach*. ADR-0006's rails and
  ADR-0008's tenant boundary are untouched, and a pre-bound runner is bound to
  exactly one conversation for the life of that binding.

## Out of scope

- **An adaptive residency policy.** Decision 5 sets the *shape* of the answer,
  bounded above by the cache TTL, but a per-thread policy that holds a chatty
  conversation longer than a one-shot one is a separate decision and wants the
  measurements above first.

- **The worker's concurrency ceiling.** `max_concurrency` is hardcoded at 16 per
  replica in [`consumer.py`](../../apps/worker/src/curie_worker/consumer.py).
  Once boot and residency are fixed, that constant is the binding limit on a
  small node. ADR-0059 left throughput out of scope and so does this ADR; it is
  named here so the next person does not have to rediscover it.

- **Relocating the observability plane.** Langfuse is the eval backbone
  (ADR-0004), not only telemetry -- the worker writes eval scores to it and the
  nightly graded parity ladder ([ADR-0081](0081-nightly-graded-parity-ladder.md))
  depends on it. Moving it off-node is a real decision with a functional
  dependency attached, and it is not this one.

- **The data tier's liveness probes.** The Postgres finding above is the same
  defect as this ADR's, but on a different path and with a worse outcome, and it
  belongs to the data-tier availability record that
  [ADR-0059](0059-sandbox-is-a-bounded-resource-envelope.md) already carved out
  and declined to write. That record now has a second reason to exist: the single
  unreplicated replica it worries about is also killed by its own liveness probe
  under starvation, so replication and probe sizing are one decision, not two.
  Fixing it is a one-line-per-probe change and deliberately not bundled here.

- **The runner image's composition.** 980 MB locally, of which Curie's own code
  is about 1.5 MB; the remainder is a Python virtualenv, a Node runtime, and a
  bundled MCP server. It affects node scale-out rather than per-pod boot once
  decision 4 lands, and it is a separate concern.

- **Checkpoint/restore**, per the alternatives above.

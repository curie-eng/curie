# Design pass: sandbox residency and pre-binding

> Status: **Design / measurement pass** behind
> [ADR-0116](../adr/0116-session-identity-arrives-over-the-aci-so-a-sandbox-can-be-pre-bound.md)
> (Draft). This document carries the measurement method, the seams each decision
> touches, and a staged plan. **No implementation is committed here, and a Draft
> ADR does not authorize one** ([ADR-0085](../adr/0085-acceptance-not-implementation-authorizes-an-adr.md),
> as amended by [ADR-0102](../adr/0102-accepted-alongside-implementation-with-explicit-approval.md)).
>
> Related: [ADR-0003](../adr/0003-stateless-first-rehydrate-on-resume.md)
> (stateless-first resume), [ADR-0005](../adr/0005-claude-agent-sdk-adapter-and-frozen-aci.md)
> and [ADR-0036](../adr/0036-aci-semver-and-reader-policy.md) (the ACI and how it
> may change), [ADR-0013](../adr/0013-concurrency-and-delivery-model.md) (the
> kernel invariants a bind must not break),
> [ADR-0059](../adr/0059-sandbox-is-a-bounded-resource-envelope.md) (the resource
> envelope this extends).

## Problem recap in one paragraph

A turn costs almost nothing to serve -- 2.3% to 8.8% of one core, measured
against a real model -- but a *new conversation* costs 17.39 seconds of cold
sandbox create on its critical path, guarded by a hard 90-second deadline, on
the smallest CPU share in the cluster (50m, against ClickHouse's 200m). And a
*finished* conversation keeps holding ~334 MiB of marginal memory for up to 59
idle minutes. The first fact produced two recorded production incidents; the
second is what makes a small node fill up until the first one fires. Both trace
to one mechanism: **a sandbox's capability is decided by pod environment
injected per claim**, so a pre-warmed pod cannot be useful and the pool that
would absorb the boot is architecturally unreachable.

## What was measured, and how to re-measure it

Every number in ADR-0116 came from the commands below. They are recorded here so
a reviewer can reproduce them rather than trust them. Nothing here is a fixture:
the cluster figures are from a real `curie-0.7.0` release on minikube
(12 CPU / 7.75 GiB), the process figures from cgroup v2 accounting inside a live
runner.

### Cold create, timed by phase

Apply a `SandboxClaim` directly, so the measurement isolates the substrate from
the worker. The bundle ref comes from a real `curie cluster deploy`; the plugin
dir must be injected too, or the runner boots against the template's generic
`CURIE_PLUGIN_DIR=/unused` and fails.

```yaml
apiVersion: extensions.agents.x-k8s.io/v1beta1
kind: SandboxClaim
metadata:
  name: probe-cold
  namespace: curie
  labels:
    curietech.ai/managed-by: curie-sandbox-substrate
spec:
  warmPoolRef:
    name: curie-runner-pool
  env:
    - name: CURIE_BUNDLE_REF
      value: "bundles/<agent-id>/<version-id>.tar.gz"
    - name: CURIE_PLUGIN_DIR
      value: "/bundles/current"
    - containerName: bundle-fetch
      name: CURIE_BUNDLE_REF
      value: "bundles/<agent-id>/<version-id>.tar.gz"
    - containerName: bundle-extract
      name: CURIE_BUNDLE_REF
      value: "bundles/<agent-id>/<version-id>.tar.gz"
```

Poll `sandboxclaim` status and the resulting pod's `initContainerStatuses` and
`containerStatuses`, recording first transition to each state. Cross-check
against `kubectl get events --field-selector involvedObject.name=<pod>`, which
timestamps each init container independently -- that is what confirmed
`bundle-fetch` at ~5 wall seconds for a 6,961-byte object.

### Turn and residency cost, by cgroup accounting

`docker stats` reports a memory figure with inactive file cache subtracted,
which understated the runner by roughly 5x against a real model. Read the cgroup
directly instead:

```bash
docker exec curie-runner-local sh -c \
  'grep usage_usec /sys/fs/cgroup/cpu.stat; cat /sys/fs/cgroup/memory.current; \
   grep -E "^(anon|file) " /sys/fs/cgroup/memory.stat'
```

Diff `usage_usec` across a turn for exact CPU-microseconds, and split
`memory.current` into `anon` (private, paid per sandbox) and `file` (page cache,
paid once per node). Enumerate the processes through `/proc` -- the image ships no
`ps`:

```bash
docker exec curie-runner-local sh -c \
  'for p in /proc/[0-9]*; do [ -r $p/status ] || continue; \
     r=$(awk "/^VmRSS/{print \$2}" $p/status); \
     c=$(tr "\0" " " < $p/cmdline | cut -c1-70); \
     [ -n "$r" ] && echo "$r $c"; done | sort -rn | head'
```

This is what showed the SDK-bundled Claude Code child at 259.5 MiB against the
Python runner's 74.1 MiB, and therefore what killed the many-sessions-per-process
alternative before it reached the ADR.

### Real-model turns without a paid credential

The runner reaches its model through `ANTHROPIC_BASE_URL`, and Ollama 0.32
implements the Anthropic `/v1/messages` surface, so a local model measures a real
streaming turn at no API cost. `--local-model` runs Curie's own pinned Ollama
image (~8.9 GB per ADR-0093's no-implicit-download rule); pointing at a host
Ollama avoids that entirely:

```bash
export ANTHROPIC_BASE_URL=http://host.docker.internal:11434
export CURIE_CREDENTIALS=ollama-local-no-auth
curie skill up --model qwen2.5:0.5b --secret ANTHROPIC_BASE_URL
```

The fake model is unsuitable for this measurement and actively misleading: it
returns canned frames in 0.10s, reports synthetic token counts, and left the
runner at 110 MiB where a real model sits near 505 MiB. Any residency or turn
figure taken under `--fake-model` should be discarded.

## The four workstreams

Decision 2 of ADR-0116 is the only one that is load-bearing on its own; the other
three are independently shippable and each stands up without the ADR being
accepted. They are ordered by risk, not by value.

### W1 -- Node-local immutable bundle cache (decision 4)

**Reclaims 4.5s of 17.4s and removes two containers from the boot path.**

The bundle is content-addressed; the deploy already prints its digest. Two
shapes, and the choice is a real one:

- A DaemonSet-populated, digest-keyed, read-only host path the sandbox mounts.
  Cheap to build, but introduces a cross-tenant shared surface beneath
  ADR-0008's per-tenant compute boundary -- which ADR-0116 explicitly declines to
  decide, so this shape needs its own ADR.
- A thin OCI layer per version, built at deploy time, letting the kubelet's
  existing image cache do the deduplication. No new trust surface, and it reuses
  machinery that already exists, at the cost of an image build inside the deploy
  path -- which matters because `git push` is the deploy
  ([ADR-0014](../adr/0014-git-push-is-the-deploy.md)).

**Recommendation: the OCI layer.** It buys the same win without asking for a new
sharing decision, and the runner base image is fixed so only a small layer
changes per version.

Seams touched: `charts/curie/templates/agent-sandbox.yaml` (init containers),
`charts/curie/values.yaml` (`bundleFetch`), the API's bundle pipeline.

### W2 -- Right-sized requests and a turn-plane priority class (decision 6)

**Shrinks the starvation amplifier and hardens the timeouts on the claim path.
No new interfaces. Must land after W1 and W3, not before.**

CPU requests on the runner, `bundle-fetch`, and `bundle-extract` are 50m each
against a measured 0.43m idle and ~90m active. Memory request is 192Mi against a
measured ~505 MiB. Both should come from measurement, with ADR-0059 decision 6's
operator override preserved. The `PriorityClass` extends ADR-0059 decision 5 to a
second axis: turn plane over insight plane.

**The ordering constraint is the important part of this workstream.**
`requests.cpu` is simultaneously the scheduler's reservation and, as `cpu.weight`,
the kernel's contention share. Measured on the live node: 50m becomes weight 11,
200m becomes weight 29, so a sandbox takes 27.5% against ClickHouse rather than
the 20% the millicore ratio implies. Lowering the request toward the measured idle
need therefore *shrinks the share the sandbox wins in a fight*. Done first, in
isolation, it makes the incident more likely. It is safe only once W1 and W3 have
taken the deadline-bound CPU work off the claim path, at which point the pool
refill has no deadline to miss and the bind that remains needs almost no CPU.

Also in this workstream: every absolute timeout on the claim path gets sized
against a starved node. `claimTimeoutSeconds` is the obvious one; the runner's
readiness probe (`periodSeconds: 2`, `timeoutSeconds: 2`, `failureThreshold: 30`)
is the one that hides, because `_claim_fresh` waits on it and each spurious exec
timeout adds a period to the claim.

This is the cheapest workstream and the one most likely to be mistaken for the
whole fix. It is not: it makes starvation less likely without removing the
wall-clock deadline that starvation attacks.

Seams touched: `charts/curie/values.yaml`, the priority class template.

### W3 -- Version-keyed warm pools (decision 3)

**Reclaims the remaining ~12s, but only after W4.**

One `SandboxWarmPool` per in-force deployment, its template carrying that
version's bundle ref, so its pods pre-fetch, pre-extract, and pre-boot. The
platform already knows the set: `curie.deployments`. Needs a bounded total pool
count, a reaper for retired versions, and zero pre-warmed pods for deployments
with no recent traffic.

**W3 is proven, and its dependency on W4 is now sharp rather than assumed.** An
isolated pool built exactly this way -- bundle ref and `CURIE_PLUGIN_DIR` baked
into the pool's own template -- pre-booted two pods to ready in ~28s, and a claim
carrying **no env** bound one of them and reached a ready runner in **0.19s**
against the 17.39s cold baseline. The bound pod held the real bundle, and the
pool refilled itself.

The dependency is therefore not "a pool cannot pre-boot"; it demonstrably can.
It is that **the moment a claim must carry session env, it stops binding a pool
pod and creates its own sandbox instead.** An otherwise identical claim with one
env entry did exactly that. So W3 delivers the sub-second bind only for claims
that carry nothing, which is precisely what W4 makes possible. This ordering is
the single most important thing to carry out of this document.

**Pool depth is the whole variable, and it was load tested.** 60 one-shot
conversations on the shipped 8-slot quota, each counted only on a terminal ACI
frame:

| regime | claim p50 | claim p95 | throughput vs cold path |
| --- | --- | --- | --- |
| burst (24) exceeds pool (3) | 6.22s | 11.40s | **1.0x** |
| pool (4) >= burst (4) | 0.21s | 0.22s | 4 in 0.4s |
| arrivals 9s apart, pool 3 | 0.14s | 0.18s | all sub-second |

A refill is the same cold create the claim used to do inline, so **pre-binding
prepays a boot rather than removing one**. Under a burst that empties the pool
the throughput is identical and the tail is worse, because the pool's pods hold
quota slots that bound sandboxes could have used. W3 therefore ships with a
sizing story or it makes the tail worse than doing nothing.

Seams touched: `charts/curie/templates/agent-sandbox.yaml`,
`apps/worker/src/curie_worker/sandbox/substrate.py`, the deployment reconciler.

### W4 -- Session identity over the ACI (decision 2)

**The enabling change, and the only one that needs an ACI version.**

`CURIE_SESSION_ID` and `CURIE_HISTORY_REF` move out of `SandboxClaim.spec.env`
and become **optional fields on the `Event` frame** the worker already posts to
`/v1/event`. No new endpoint: the frame already carries `user`, `ts`, and `type`,
so session identity joins the same shape, and an additive field is the one ACI
change that a pre-existing runner ignores instead of refusing.

The runner token stays pod env. A pre-warmed pod cannot receive a token minted at
claim time, so the pool mints one per pod and **the worker resolves the bound
pod's token through the Kubernetes API at bind time** rather than generating it.
The worker already reads those pods; the token's exposure is unchanged. Per-pod,
not per-pool, or one compromised sandbox could authenticate as its siblings.

The substrate's `claim()` and `resume()` therefore stop building a session boot-env
overlay and start (a) reading the bound pod's token and (b) putting the session
fields on the turn they were already sending.

Constraints this must respect:

- **ADR-0013's kernel invariants.** One live session per thread stays the
  routing CAS; the finish race, the side-effect flag, and the no-auto-retry rule
  are untouched. A bind that silently allowed two sessions on one runner would
  break the thing `kernel.py` exists to protect.
- **ADR-0003's resume contract.** Rehydrate-from-history remains how a resumed
  thread recovers. This changes the *delivery* of the history ref, not the
  contract.
- **ADR-0036's reader policy.** Additive fields, so a runner that predates them
  ignores them rather than refusing the frame. The dangerous direction is the
  other one: a runner expecting session identity on the frame, paired with a
  worker still injecting it as pod env, boots and then serves a turn with no
  session identity at all. That is a silent wrong answer, so **roll the runner
  first and the worker last**, and have the runner fall back to boot env whenever
  the fields are absent for the whole compatibility window. Worth noting that ACI
  skew already fails late today -- an 0.2.7 CLI against an 0.4.1 runner boots
  cleanly and fails on the first message.
- **The runner token.** Minting it per claim is what makes it die with the
  claim. Delivering it over the ACI means the bind itself must be authenticated
  by something the pool pod already holds, which is a real design question this
  document does not settle.

Seams touched: `packages/aci-protocol/schema/aci-protocol.schema.json`,
`runner/src/curie_runner/server.py`, `apps/worker/src/curie_worker/sandbox/substrate.py`,
`apps/worker/src/curie_worker/binding.py`.

### Then, and only then: short residency (decision 5)

`routeTtlSeconds` drops from 3600 to something on the order of seconds. This is
a values change, and it is safe **only** once a re-bind is sub-second -- before
that it trades a compute saving for a token bill, because a resumed thread is
cache-cold and a scaffolded bundle already re-sends 20,875 input tokens per turn.

## The demo, recorded

The claim that matters is not "faster". It is that **the deadline stops being
reachable**. That is what the recording shows, in
[`docs/demo/adr-0116-residency.gif`](../demo/adr-0116-residency.gif) (the raw
asciicast is beside it, and the harness is in
[`prototypes/adr-0116-residency/`](../../prototypes/adr-0116-residency/)):

|                      | quiet node | under contention |
| -------------------- | ---------- | ---------------- |
| today (cold create)  | 4.72s      | **never ready**  |
| pre-bound            | **0.17s**  | 7.79s            |

Contention is a Deployment of busy-loop pods requesting `200m` each against the
critical path's `50m`, which is the ratio the chart names as the amplifier. Under
it, today's path crossed 90s at 91.02s and never became ready; the pre-bound path
took 7.79s. Two full runs agreed.

What the recording does **not** show, and should not be read as showing:

- The ACI change. Arms C and D pass no env at all, which is why they bind. The
  ADR's decision 2 is what would let a real conversation do the same.
- A cold-path number under contention. Arm B is reported as never ready, because
  the crossing is the result and the tail is not interesting.
- Pod count dropping to zero while a thread stays answerable. That is the arm
  that would read as a product capability rather than an optimisation, and it
  needs decision 2 first.

## Open questions

- ~~**How is a bind authenticated** if the runner token no longer arrives as pod
  env?~~ **Answered and confirmed.** The token stays pod env, minted per pool pod
  at creation, and the worker resolves it from the bound pod through the Kubernetes
  API instead of minting it. Confirming it turned up the reason it is mandatory: a
  pool pod today carries **no** token, and an unauthenticated `POST /v1/event`
  against a bound pool pod returned **200**. W3 therefore cannot ship before this
  part of W4, or every warm pod is an open ACI endpoint. The same probe confirmed
  the other half for free: a pre-bound pod does serve ACI turns, failing only at
  the model call.
- **What does the cold path cost when it is not quota-blocked?** The with-env arm
  of the pre-bind test failed on `curie-sandbox-quota` rather than completing, so
  its wall clock is unmeasured; only its behaviour (create its own sandbox rather
  than bind a pool pod) is established.
- **Can the platform manage the pool set?** Not prototyped. The shape is already
  in the repo: [ADR-0090](../adr/0090-a-reconciler-applies-connectors-so-agent-repos-need-no-cli.md)'s
  connector reconciler converges Kubernetes objects toward what the database says
  should exist, on an interval, and a pool-per-in-force-deployment reconciler is
  the same loop over a different object. Treated as low risk on that precedent
  rather than measured, which is a judgement and should be read as one.

- **Can a real in-cluster model reach a host-local Ollama?** No, by design, and
  this is worth recording because it looks like a configuration mistake. The
  agent-sandbox controller's default NetworkPolicy allows public internet **minus
  RFC1918**, so a model served from the host's private address is unreachable from
  a sandbox whatever credential is set. The real-model turn figures in the ADR come
  from the `skill` tier, where no such policy applies.

- **What signal should size the pool?** Depth at or above the arrival burst gives
  every conversation a sub-second claim; depth below it gives the surplus a cold
  create and a worse tail than no pool at all. Both were measured. An autoscaler
  keyed on recent arrival rate per version is the obvious answer and is not
  designed here.

- **Should the CPU limit move with the request?** ADR-0059 decision 4's quota
  counts `limits.cpu`, and at the shipped `limits.cpu: 1` per sandbox the
  namespace admits eight concurrent sandboxes while its `pods` allowance is 50
  and its `requests.cpu` sits at 10%. Raising density without moving the ceiling
  just relocates the wall.
- **What is the per-session marginal cost inside one process**, if the SDK could
  ever multiplex sessions in one Claude Code child? The measured 259.5 MiB child
  is per-session today; whether it must be is unmeasured, and it is the only
  path to a memory win larger than 1.25x.
- **Does the node-local bundle cache need its own ADR?** ADR-0116 says it does
  not decide the shared-surface question. W1's OCI-layer recommendation is
  chosen partly to avoid needing that decision at all.
- ~~**gVisor overhead is unmeasured.**~~ **Measured.** On containerd with the
  `gvisor` addon and `security.gvisor.mode=require`, so sandbox pods really run
  under `RuntimeClass gvisor` / `runsc`: cold create 7.06s against 4.72s with
  gVisor off, and the pre-bound bind **0.18s against 0.17s**. gVisor taxes the
  boot and leaves the bind alone, which makes the case for pre-binding stronger
  under the production default rather than weaker. The two clusters differ in
  container runtime too, so the cold-create delta is not gVisor-only; the bind row
  is the one that matters, and it does not move.

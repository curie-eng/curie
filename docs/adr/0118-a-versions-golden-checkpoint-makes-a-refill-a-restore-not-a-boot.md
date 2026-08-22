# 118. A version's golden checkpoint makes a refill a restore, not a boot

Date: 2026-08-22

Status: Draft

Depends on [ADR-0116](0116-session-identity-arrives-over-the-aci-so-a-sandbox-can-be-pre-bound.md)
and is not an alternative to it. 0116 takes the boot off a conversation's
critical path; this ADR addresses the ceiling that its own load test then
measured. Nothing here changes what a sandbox may reach, so
[ADR-0006](0006-security-rails-as-chart-defaults.md)'s rails and
[ADR-0008](0008-multi-tenancy.md)'s tenant boundary stand as they are.

## Context

ADR-0116 removed a 17.39s boot from the 90-second claim deadline by binding a
conversation to an already-booted runner from a version-keyed warm pool. The
bind was measured at 0.18s, four times across three clusters. It also load
tested the result, and the load test found the ceiling:

| regime | claim p50 | claim p95 | throughput vs cold path |
| --- | --- | --- | --- |
| burst of 24, pool of 3, 5 concurrent | 6.22s | **11.40s** | **1.0x** |
| burst of 4, pool of 4 | **0.21s** | 0.22s | 4 in 0.4s |
| arrivals 9s apart, pool of 3 | **0.14s** | 0.18s | all sub-second |

Only the first three conversations were served sub-second, one per warm pod.
Every claim after that waited on a refill, **and a refill is the same cold create
the claim used to do inline.** Pre-binding prepays a boot; it does not remove
one. On the same 24-conversation burst the cold path ran 48.3 conversations per
minute and the pre-bound path 46.7, and the pre-bound tail was worse, because
the pool's own pods hold quota slots that bound sandboxes could otherwise use.

So the property ADR-0116 delivers is conditional on pool depth against arrival
rate, and the thing that decides whether depth can keep up is **the cost of
creating one more ready runner.** Today that cost is a full boot.

### What a boot is actually made of

Measured end to end on a real release, idle node, runner image already resident,
6,961-byte bundle:

| phase | measured | can a restore skip it? |
| --- | --- | --- |
| claim admitted, sandbox named, pod created | 0.75s | **no** |
| pod scheduling, container starts | 1.93s | **no** |
| `bundle-fetch` (`aws-cli s3 cp`) | 4.11s | yes |
| `bundle-extract` | 0.42s | yes |
| container transition | 1.25s | partly |
| runner boot until `/healthz` answers | 8.93s | **yes, and it is the largest** |

A restore can skip **13.46 of the 17.39 seconds**. What it cannot skip is the pod
lifecycle: Kubernetes still has to admit the claim, schedule a pod, and have the
runtime stand up a sandbox. That leaves a floor of roughly **3.9s**.

This matters for expectation setting, because it is the difference between two
claims that sound the same and are not:

| | measured or projected | why |
| --- | --- | --- |
| bind an already-running pool pod | **0.18s** (measured) | nothing is created |
| restore a fresh runner | **~3.9s** (projected from the table above) | a pod is still created |
| boot a fresh runner | **17.39s** (measured) | everything from scratch |

**A restore does not replace a warm bind, and this ADR does not propose removing
the pool.** It proposes making the pool refill roughly four times cheaper, which
is what raises the arrival rate the pool can absorb before it empties.

### Why this is reachable here specifically

Two properties of the system as built, neither of them arranged for this purpose:

**gVisor is already the default sandbox runtime.** Verified on a live install:
`RuntimeClass gvisor` with handler `runsc`, the chart's own gVisor preflight
passing, and `security.gvisor.mode=require` honoured. `runsc` is one of the few
container runtimes with first-class checkpoint and restore. The isolation cost
the product already pays is what makes this cheap to reach. It is also the cost
a restore removes most of: measured, gVisor took the cold create from 4.72s to
7.06s while leaving the bind at 0.18s, so the boot is exactly where `runsc` is
expensive.

**The kernel already has turn boundaries.**
[ADR-0013](0013-concurrency-and-delivery-model.md) gives every session a
well-defined point with no in-flight request: the finish race, the steer path,
and `/v1/reset` all key off it. Checkpointing a process that holds an open
socket is the classic hard case for process migration, and the way out is to
quiesce first. This system did not have to invent that point; it has one.

### What the pool actually needs restored

A refill does not need a conversation restored. It needs **a generic, booted,
bundle-loaded runner with no session** -- which is precisely what a pool pod is
before anything claims it, and precisely the state the template already names
`CURIE_SESSION_ID=warm-unbound`. That is a much weaker requirement than
hibernating a live conversation, and it is what keeps this decision separable
from the ADR-0003 question (below).

## Decision

**A deployed version's artifacts include a golden checkpoint of a booted,
unbound runner, and a warm pool refills by restoring that image rather than by
booting.**

1. **The checkpoint is a per-version build artifact, produced once.** A deploy
   already turns a push into an immutable version with an immutable bundle
   ([ADR-0014](0014-git-push-is-the-deploy.md)). It now also boots that version's
   runner once, quiesces it at a turn boundary with no session established, and
   stores the checkpoint beside the bundle, keyed by the same content digest.
   Producing it at deploy time and not at claim time is the whole point: the cost
   is paid once per version instead of once per pod.

2. **A checkpoint contains no conversation and no credential.** It is taken
   before any session exists, so there is no transcript, no history ref, and no
   session id in the image. The model credential and the runner token are
   excluded from the checkpointed address space and injected on restore, the same
   way a booting pod receives them today. **An image that cannot be shown to
   exclude them is not written.**

3. **Restore is an optimisation with a boot as its fallback.** A pool pod
   attempts a restore; on any failure -- a missing or unreadable image, a runtime
   that refuses it, a version mismatch -- it boots normally and records the
   reason. `runsc` checkpoint/restore is the least proven component in this
   decision, and the system must not lose the ability to refill when it
   misbehaves.

4. **The pool stays.** Restore is for refill, not for the claim path. A claim
   binds a ready pod at 0.18s; a claim that has to wait for a restore is a claim
   that waited, and under contention it is a claim racing the 90-second deadline
   again. Pool depth remains the operator's lever for latency; restore is what
   makes that lever affordable to hold.

5. **A checkpoint's lifecycle is its version's lifecycle.** It is created when a
   version is deployed, is immutable, and is garbage collected when the version
   is no longer in force. A stale checkpoint is never restored against a
   different bundle digest: the digest is part of the key, so a mismatch is a
   cache miss that falls back to decision 3's boot.

**The refill invariant** (what we test and review to): the cost of making one
more ready runner of an in-force version does not include booting its harness,
and a pool whose depth is exceeded recovers at restore speed rather than boot
speed.

## What this is expected to buy, and what it is not

Stated plainly because the load test in ADR-0116 already showed how easy it is
to overclaim here.

- **Refill cost from roughly 15s to roughly 4s.** That is projected from the
  phase table, not measured, and the projection's honest error bar is the
  `container transition` row and whatever `runsc` restore itself costs, neither
  of which is known yet.
- **Therefore the sustainable arrival rate rises by about the same factor**, and
  a pool of a given depth covers a burst roughly four times larger before it
  empties.
- **It does not make the claim faster.** A warm bind is 0.18s with or without
  this decision.
- **It does not remove the pod lifecycle floor.** Kubernetes still schedules a
  pod, and no amount of process restoration changes that.
- **It does not increase the quota.** The namespace admits 8 concurrent
  sandboxes on the shipped numbers regardless, and ADR-0116's out-of-scope note
  on that stands.

## Spike evidence (measured 2026-08-22)

This decision was spiked before being proposed, on a cluster running containerd
2.2.1 with minikube's `gvisor` addon, `runsc release-20260817.0`, the chart
installed at `security.gvisor.mode=require`, and a real bundle-loaded runner
sandbox. Five paths were tried. **The cheap half is confirmed cheap and the
expensive half is confirmed unreachable**, which is the opposite balance from the
projection above.

| path | result |
| --- | --- |
| `runsc checkpoint` on the runner container | **47 ms**, 59 MB image, container still `running` afterwards with `-leave-running` |
| `runsc checkpoint` on the pod sandbox | **38 ms**, 59 MB image |
| `runsc restore` of the runner sub-container, out of band | **refused by design** |
| `runsc restore` of the pod sandbox, out of band | **sandbox fails to start** |
| `crictl checkpoint` (the CRI `CheckpointContainer` path) | **routes to CRIU, not to `runsc`** |

### Checkpointing is already cheap, and cheaper than this ADR assumed

**38 to 47 milliseconds, and a 59 MB image.** The consequences section below
originally projected "roughly 505 MiB" from the runner's measured cgroup
footprint; that was wrong by an order of magnitude, because most of that 505 MiB
is file-backed page cache (272.4 MiB measured) which a checkpoint does not need
to carry, and much of the remaining anonymous memory is untouched. `runsc` also
ships `-compression` and `-exclude-committed-zero-pages`, neither of which was
used for these numbers.

`-leave-running` works: the container was `running` immediately after the
checkpoint returned. Without it the container is destroyed, and on a live pod the
kubelet then restarts it -- observed, `restarts: 1` and a fresh container id. So a
golden checkpoint can be taken either on a dedicated build pod or, if wanted,
against a live one without disturbing it. Decision 1's "produced once at deploy
time" stands and is now cheap rather than merely justified.

### Restore is the entire cost, and there is no path to it today

Three distinct failures, each with a precise cause:

**A sub-container cannot be restored on its own.**

```
starting sub-container [python -m curie_runner]:
sandbox is not being restored, cannot restore subcontainer: state=started
```

In gVisor a Kubernetes pod is one sandbox hosting several sub-containers, `pause`
and `runner` here, sharing a single sandbox process -- observed as two `runsc
list` entries with the same PID. **The checkpoint and restore unit is therefore
the pod sandbox, not the runner container.**

**A sandbox restored out of band does not come up.**

```
cannot create sandbox: cannot read client sync file:
waiting for sandbox to start: EOF
```

Restoring needs the environment containerd builds around a sandbox -- network
namespace, mounts, the rootfs at its expected path -- and `runsc restore` given
only an image and a preserved `config.json` does not reconstruct it. Related and
found the same way: the original OCI bundle directory is **deleted** when
containerd reaps a destroyed container, so a checkpoint image is not
self-sufficient. Its OCI spec has to be preserved deliberately alongside it.

**The CRI path that exists does not cover gVisor.**

```
CRIU binary not found or too old (<31600) ...
exec: "criu": executable file not found in $PATH
```

containerd 2.2.1 implements `CheckpointContainer`, but its implementation assumes
a runc plus CRIU stack and does not delegate to `runsc checkpoint`. So the
obvious integration point is present in name and absent in substance for this
runtime.

### What that does to the shape of this decision

The projection above -- "a restore skips 13.46 of 17.39 seconds, leaving a floor
near 3.9s" -- is **unmeasured and remains so**, because no restore completed. It
should be read as the value of the prize, not as a result.

The work is therefore not "checkpoint at deploy, restore on refill". It is
**build sandbox-level restore integration**, and that is a substantially larger
undertaking with two plausible owners:

- **The Agent Sandbox controller**, reconstructing a pod's sandbox environment and
  then handing off to `runsc restore`. It already owns sandbox and warm-pool
  lifecycle, so it is the natural place, but it means this decision depends on an
  upstream component Curie adopts rather than builds (ADR-0007).
- **containerd**, routing `CheckpointContainer` and a restore path to `runsc` for
  the gVisor runtime handler. Cleanest long term, entirely outside this
  repository.

Neither is a change Curie can make alone, which is the single most important
thing this spike establishes. Decision 3's boot fallback is consequently not a
safety net for an unlikely failure; **it is the behaviour of the system until one
of those two integrations exists.**

## Alternatives considered

- **Restore on demand, with no warm pool.** Rejected. At a projected ~3.9s it is
  20x slower than a warm bind, and it puts pod creation back inside the 90-second
  deadline where contention can stretch it. It also gives up the one property
  ADR-0116 was written to obtain. Restore complements the pool; it does not
  replace it.

- **Deeper warm pools alone.** Rejected as sufficient, kept as necessary. The
  load test measured that a pool shallower than the burst has a *worse* tail than
  no pool (p95 11.40s against 7.56s), and the namespace quota caps total sandbox
  count at 8 on the shipped values, so depth cannot be bought past that point.
  Depth without cheaper refill just moves where the cliff is.

- **Checkpointing a live conversation to hibernate an idle thread.** Deferred,
  and deliberately a different decision. That is the inversion of
  [ADR-0003](0003-stateless-first-rehydrate-on-resume.md), whose live-cluster
  evidence is that suspend deletes the pod and resume is a cold rehydrate. It is
  attractive for a different reason -- a resumed thread today re-sends its whole
  prompt, measured at 20,875 input tokens per turn on a *scaffolded* bundle -- but
  it puts a conversation's transcript and a live credential into an image at rest,
  which is a confidentiality decision this ADR does not make. Decision 2 keeps the
  two separable on purpose.

- **Many sessions inside one restored process.** Rejected, already measured under
  ADR-0116: each session is served by its own SDK-bundled Claude Code child at
  259.5 MiB RSS, so sharing a process amortises only the 74.1 MiB Python runner,
  worth 1.25x at ten sessions.

- **Baking a booted runner into the container image instead of checkpointing.**
  Rejected: an image layer can carry a filesystem, not a running process with its
  interpreter warm and its bundle compiled, which is the 8.93s this decision is
  trying to skip. The thin-OCI-layer idea is still the right answer for the
  *bundle*, and it is ADR-0116 decision 4, which this ADR assumes rather than
  repeats.

## Consequences

- **Deploy gets slower and less simple.** `git push` currently ends when a bundle
  is stored; it would now end when a version has also been booted and
  checkpointed. That is a new failure mode on the deploy path, and it needs a
  clear answer for what a deploy means when the checkpoint step fails. Decision 3
  makes that answer available -- ship the version without a checkpoint and refill
  by booting -- but it must be a deliberate degraded state and not a silent one.

- **Checkpoints are smaller than expected, which weakens a worry rather than
  removing it.** Measured at **59 MB** uncompressed and without
  `-exclude-committed-zero-pages`, not the ~505 MiB a cgroup footprint suggests.
  Storage is still per in-force version on the same object store as bundles, and
  decision 5's garbage collection still matters for a repository that deploys
  often, but it is tens of megabytes per version rather than hundreds.

- **The credential exclusion in decision 2 is the security-critical part of this
  ADR.** Getting it wrong writes a live model credential to an object store. It
  needs a test that asserts absence, not a review that asserts intent.

- **This adds a dependency on `runsc` checkpoint/restore behaviour**, which the
  project does not otherwise rely on, and the spike found that half of it does not
  exist yet. Checkpointing works today at 38 to 47 ms. Restoring has no
  integration path in containerd 2.2.1 for the gVisor handler, so until either the
  Agent Sandbox controller or containerd provides one, decision 3's fallback is
  the steady state and this ADR buys nothing operationally. Decision 3 is what
  keeps that from being a regression: the fallback is exactly today's behaviour.
  **This ADR should not be accepted as implementable work on its current
  evidence; it should be accepted, if at all, as the decision to pursue that
  integration.**

- **The eval plane gains a new axis.** Two runners of the same version can now
  reach ready by two different paths, boot and restore, and
  [ADR-0081](0081-nightly-graded-parity-ladder.md)'s parity ladder is where a
  behavioural difference between them would surface. A restored runner that
  answers differently from a booted one is the failure this ADR is most likely to
  produce and least likely to notice.

## Out of scope

- **Per-conversation hibernation**, per the alternatives above. It is the larger
  prize and the larger decision, and ADR-0003 is the record it would supersede.

- **Sharing a checkpoint across nodes.** A restore reads its image from
  somewhere; whether that is the object store on every refill, a node-local
  cache, or an image layer is the same question ADR-0116 decision 4 asks about
  bundles, and it should be answered once for both rather than twice.

- **The quota and the concurrency ceiling.** `limits.cpu` denominated quota caps
  sandboxes at 8, and `max_concurrency` is hardcoded at 16 per worker replica.
  Both were named as out of scope by ADR-0116 and remain so; a cheaper refill
  makes them bind sooner, not later.

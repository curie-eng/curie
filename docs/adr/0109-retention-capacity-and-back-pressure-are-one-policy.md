# 109. Retention, capacity, and back pressure are one policy: a claim is admitted, not discovered

Date: 2026-08-14

Status: Accepted

Accepted on 2026-10-27 with explicit maintainer approval.

Extends [ADR-0059](0059-sandbox-is-a-bounded-resource-envelope.md) (the sandbox
is a bounded resource envelope) by closing the throughput question that ADR
explicitly deferred, and builds on [ADR-0003](0003-stateless-first-rehydrate-on-resume.md)
(stateless-first sessions) and [ADR-0013](0013-concurrency-and-delivery-model.md)
(one live session per thread). It supersedes no ADR. What it does supersede is
the run of point fixes that have been setting this policy one knob at a time:
`routeTtlSeconds` exposure (#1380), the zero-value bounds (#1388), bounded claim
creation on the eval lane only (#709), and the quota defaults shipped with
ADR-0059 (#758, #759).

## Context

A thread holds a sandbox. ADR-0013 makes that exclusive (one live session per
thread, enforced by a Valkey `SET NX PX` thread lock with a 120s TTL), ADR-0059
bounds what that sandbox may consume, and the chart bounds how many may exist at
once. Three separate mechanisms decide the same thing, and none of them knows
about the others.

**The arithmetic nobody performs.** On a default install
([`charts/curie/values.yaml`](../../charts/curie/values.yaml)):

| term | default | what it decides |
|---|---|---|
| `agentSandbox.runner.resources.limits.cpu` | `1` | what one held sandbox costs against the quota |
| `resourceQuota.hard.limitsCpu` | `8` | how many may exist at once |
| `resourceQuota.hard.sandboxPodCount` | `50` | a ceiling the CPU line reaches first |
| `worker.routeTtlSeconds` | `3600` | how long a thread pins its sandbox after its last message |
| `worker.suspendedRouteTtlSeconds` | `86400` | the same, for a route waiting on an approval |
| `worker.claimTimeoutSeconds` | `90` | how long a claim waits before giving up |
| `agentSandbox.warmPool.replicas` | `0` | how much of a claim's cost is prepaid |

The concurrent-sandbox ceiling is the minimum across the quota dimensions
divided by the per-sandbox envelope. At these defaults that is `8 / 1`, so
**eight**. Memory would allow 32 and the pod count would allow 50; CPU limits
bind first. Nothing in the chart, the CLI, or the worker states that number.

**Issue #1534** is that number meeting the product. Eight CLI messages, or eight
eval cases from a suite of twelve, hold the namespace, and every later claim
times out. The default install cannot complete its own eval suite for this
reason alone, on a node measured idle at roughly 14 percent CPU. ADR-0013 gave
evals their own consumer group specifically so eval load would never starve
interactive turns, but that separation is at the stream. Beneath it, both lanes
draw on one undeclared pool of eight.

**Issue #1492** is the other face of the same policy: what a claim costs. Every
thread pays a full cold boot (11s to `Ready`, and roughly 11 core-seconds), and
the turn itself draws about 48 percent of a 2 vCPU node for around 66s. On six
concurrent cold creates, five of six fail. An idle claimed sandbox measured 6 to
7 millicores against a 150m request while holding its slot for the full
`routeTtlSeconds`: roughly **11 percent occupancy** for a slot held at 100
percent. The warm pool that exists to prepay the boot cannot be used, and not
because of a conservative default. `CURIE_BUNDLE_REF` must reach the
`bundle-fetch` and `bundle-extract` init containers, init containers run exactly
once at pod birth, and environment variables cannot be changed on a running
container. A pre-booted generic pod has already run them, so binding it to a
bundle requires recreating it, which is a cold create. The knob exists and is
structurally inert on the real-model path.

**Back pressure, at the boundary, is a guess.** There is none of the usual
machinery: no admission check, no queue, no refusal. The ninth claim creates a
`SandboxClaim`, its pod is refused by `ResourceQuota` admission or starves for
CPU, the claim never goes `Ready`, and
[`substrate.py`](../../apps/worker/src/curie_worker/sandbox/substrate.py) polls
until `claimTimeoutSeconds` elapses and raises `ClaimTimeoutError`. That error
names CPU saturation as the likeliest cause. The three real causes hit in one
day were quota exhaustion, controller RBAC, and reply delivery poisoning. The
substrate could not have said otherwise: `ClaimView`
([`types.py:127`](../../apps/worker/src/curie_worker/sandbox/types.py)) carries
`name`, `ready`, and `sandbox_name`, and discards the claim's `Ready` condition
reason and message, so the one field that names the cause is dropped before
anything can report it.

The result is a failure that is **late** (90s, when it was knowable at 0s),
**misattributed** (a hint that sends operators to `kubectl top node` on an idle
node), **opaque to the user** (the turn escalates as `runner-error`), and
**self-amplifying** (timed-out turns retry, retries reload the node, and a
3-concurrent burst that passes from idle fails when launched into the retry
backlog of a previous one).

These are not three bugs. Retention decides how many sandboxes exist, the quota
decides how many may exist, the warm pool decides what one costs, and back
pressure decides what happens where those meet. They are one policy, currently
written in four places by four changes that never met.

## Decision

**Sandbox capacity is a declared, published number; a claim is admitted against
it before a `SandboxClaim` is created; and retention is a reclaimable lease
rather than a fixed pin.** Exhaustion is a first-class, attributable outcome at
`t=0`, never a timeout at `t=90s`.

1. **The concurrent-sandbox ceiling is derived and published, not implied.** The
   chart computes the ceiling as the minimum across quota dimensions divided by
   the per-sandbox envelope, renders it where the worker can read it, and
   surfaces it in preflight output. An operator who raises `routeTtlSeconds` or
   the runner's CPU limit sees the resulting ceiling move. Capacity stops being
   arithmetic across seven values in three files.

2. **Admission precedes creation.** The worker checks the ceiling (and, where
   available, live quota status) before creating a claim. A claim that cannot be
   admitted is never submitted, so the failure is known before any pod is
   scheduled and no partially-created sandbox has to be reaped.

3. **Back pressure is a bounded queue, then an explicit refusal.** Over-ceiling
   claims wait in a fair FIFO whose bound is the headroom under the ADR-0013
   thread lock TTL, not an arbitrary constant. Past that bound the turn fails
   with a distinct `CapacityExhaustedError` that names observed quota usage, the
   number of held routes, and the soonest expiring lease. This generalizes the
   eval lane's bounded claim creation (#709) to the interactive path, which
   converts #1492's "five of six fail" into "six of six queue", and #1534's
   "the ninth message times out" into "the ninth message waits, then is told
   why".

4. **Retention is a reclaimable lease, not a pin.** ADR-0003 already decided that
   a session survives losing its process: state is externalized and a resumed
   thread rehydrates from history. That makes an idle route **reclaimable**
   rather than sacred. When a claim would otherwise be refused, the least
   recently used idle route is reclaimed to serve it. `routeTtlSeconds` becomes
   a best-effort affinity hint with a floor rather than a hard hold, and
   suspended routes (`suspendedRouteTtlSeconds`, a full day by default) are
   reclaimed first, since resume already recreates the claim by construction.
   Two guards are load-bearing and are the part to test hardest: **never reclaim
   a route with a turn in flight**, and **never reclaim inside a minimum idle
   grace**. "Idle" is an ambiguous signal, and reclamation is destructive; a
   reclaim that races a live turn is worse than the exhaustion it prevents.

5. **Diagnostics carry evidence, never a guess.** `ClaimView` carries the claim's
   `Ready` condition reason and message, and the timeout path reports observed
   quota usage, held-route count, and controller condition instead of naming a
   likely cause. This is independent of the rest and can ship first (#1534).

6. **A warm pool is per-agent or it does not exist.** Given #1492's structural
   finding, a generic pre-booted pool cannot bind a real-model claim at any
   replica count. Either the pool is scoped to one agent, with the bundle
   delivered at pod birth and the session at request time, or `replicas` stays 0
   and the documentation stops implying a live knob. There is no supported third
   state in which a configurable knob is structurally inert.

**The capacity invariant** (what we test and review to): at any moment the number
of held sandboxes is at most the published ceiling; a claim beyond it either
queues within the lock TTL, reclaims an idle lease, or fails with an error naming
the exhausted resource; and no claim fails on a timeout whose stated cause was
not observed.

### Out of scope

- **The bundle/session identity split.** #1492's proposal (bundle per agent at
  pod birth, `CURIE_SESSION_ID` at request time) is the mechanism that makes
  decision 6's per-agent pool possible. It changes the ADR-0049 boot env
  contract and needs an explicit runner-state reset between threads, so it is
  its own decision record, tracked from this one.
- **Elastic capacity.** HPA and cluster autoscaler wiring remains undecided, as
  it was in ADR-0059. Admission is what makes elasticity safe to add later; it is
  not a substitute for it.
- **Data-tier availability.** Unchanged from ADR-0059's deferral.

## Alternatives considered

- **Tune the defaults (rejected).** Lower `routeTtlSeconds`, raise
  `resourceQuota.hard`, raise `requests.cpu`. This is the status quo path, and it
  is what every point fix so far did. #1492 tested it directly: `requests.cpu` at
  150m and 200m, `claimTimeoutSeconds` at 110s, and a lower `routeTtlSeconds`
  each moved the failure without removing it, and at 200m the sixth pod went
  `Pending`, trading starvation for unschedulability. More importantly, tuning
  changes where the cliff sits and leaves the behavior **at** the cliff exactly
  as it is: a 90s hang, a misattributed error, and a retry storm. Every default
  has a load that exceeds it.

- **Raise or remove the quota and let the node absorb it (rejected).** ADR-0059
  made the tenant `ResourceQuota` a capacity **isolation** boundary, on the
  reasoning that nodes are shared beneath the namespace boundary. Raising it to
  absorb load re-opens the cross-tenant availability hole that ADR was written to
  close. It also does not help the topology both issues measured, which is a
  single-node self-host install where there is no other node to absorb anything.

- **Per-agent warm pools alone (rejected as the whole answer).** #1492's proposal
  removes the boot cost, which is real and worth having. It does not remove the
  hold: a pre-booted pod still occupies quota, and a thread that pins a slot for
  3600s after 66s of work still exhausts an eight-slot ceiling at the same
  count. Warm pools change what a claim **costs**, not how many claims may
  exist, and they say nothing about what happens at the boundary. Complementary,
  and the right next record; not this decision.

- **Fail fast with no queue (rejected).** Refusing the ninth claim immediately is
  simpler than a bounded FIFO and fixes attribution, but it turns a survivable
  burst into a user-visible failure. #1492's six-concurrent case fits in eight
  slots comfortably given a few seconds of sequencing; refusing it outright would
  be a worse product than the timeout it replaces, for the one shape of load most
  likely to occur.

## Consequences

- Exhaustion becomes fast, attributable, and non-amplifying. The retry loop that
  turned a burst into several minutes of degradation stops being fed, because a
  refused claim is a decision rather than a timeout.
- A default install can complete its own twelve-case eval suite against an
  eight-slot ceiling, by queueing rather than by anyone raising a quota.
- **A refusal is now product surface.** Where a user previously saw a slow turn
  end in an opaque `runner-error`, they will see an explicit "at capacity"
  outcome. That message, and how a channel renders it, becomes something to
  design rather than a stack trace to hide.
- **Retention becomes best-effort, and some follow-ups get slower.** A thread
  whose route was reclaimed under pressure pays a cold create it previously would
  not have. ADR-0003 makes that lossless in state terms, not in latency terms, so
  the visible trade is that a busy install feels slower per follow-up in exchange
  for not failing. The minimum idle grace is what keeps an actual conversation
  from being reclaimed out from under itself.
- Reclamation is a new destructive path in the worker, adjacent to the ADR-0013
  thread lock. It has to interlock with that lock rather than beside it, and
  "the route is idle" must be derived from the same authority that decides a turn
  is in flight. This is the highest-risk part of the decision.
- The ceiling becomes an observable (held routes, queue depth, refusals, reclaim
  counts) rather than something inferred from timeouts, which makes capacity
  planning a reading instead of an autopsy.
- Operators lose the ability to change effective capacity silently. Raising
  `routeTtlSeconds` now visibly changes queue depth and reclaim pressure instead
  of quietly changing how many threads can be served at once.
- None of this changes isolation. One pod serves one thread at a time, ADR-0059's
  envelope and quota stand exactly as decided, and ADR-0006's rails are
  untouched. This decision governs scheduling **within** the ceiling, not whether
  the ceiling exists.

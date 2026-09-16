# 122. A warm pool's runner token is per version, not per pod

Date: 2026-08-25

Status: Accepted

**Supersedes in part
[ADR-0116](0116-session-identity-arrives-over-the-aci-so-a-sandbox-can-be-pre-bound.md)**,
whose decision 2 says the pool "mints one per pod at creation". It cannot.
Only the per-pod minting clause is replaced. Everything else in ADR-0116 is
unaffected, including the
half of decision 2 that moves session identity onto the ACI `Event` frame, and
including the ordering that puts the token before decision 3's warm pool.

## Acceptance and realization

The maintainer explicitly approved acceptance on 2026-09-04. The realizing
implementation and its live isolation proofs are tracked in
[#1492](https://github.com/curie-eng/curie/issues/1492). Acceptance authorizes
that work; it does not claim the mechanism is implemented or permit enabling
warm replicas before adoption, bootstrap retirement, and conversation isolation
are proven. The optional ACI session metadata prerequisite in
[#2309](https://github.com/curie-eng/curie/pull/2309) does not implement those
requirements.

## Context

ADR-0116 found that a warm pool pod answers an unauthenticated `POST /v1/event`
with 200, where a pod the worker created for a real turn answers 401. The cause
is that the runner's bearer token is minted per claim by the worker and injected
through `SandboxClaim.spec.env`, and a pool pod has no claim. Its decision 2
proposed to close that by having the pool mint a token per pod, with the worker
reading it back at bind time.

**The pool cannot mint anything.** It renders pods from a static
`SandboxTemplate`, and checking what that template can express:

- The CRD offers `env`, `envFrom`, `secretKeyRef`, and `secretRef`. There is no
  per-pod secret or token generation.
- The per-pod values a template *can* reach through `fieldRef` are
  `metadata.name` and `metadata.uid`. Neither is a secret; anything that can list
  pods can read both.

There is a second, deeper reason, and it is the same fact that motivated moving
session identity to the ACI in the first place: **environment cannot be injected
into a pod that is already running.** A claim binding a warm pod cannot hand it a
token any more than it can hand it a session id. So the token must either be
baked in before the pod starts, and therefore shared by every pod the template
produces, or it must not be a pre-shared bearer token at all.

### The identity-based alternative, and why not

The obvious way to avoid a shared secret is to stop using one: have the worker
present its own ServiceAccount token and have the runner verify it with a
`TokenReview` against the API server. That is a standard pattern and it needs no
per-pod minting.

It is rejected here because of what it would cost the sandbox boundary:

- The runner's egress allows **DNS and the OTel collector only**. Reaching the
  API server means opening Rail 1 for every sandbox.
- `agentSandbox.runner.serviceAccount.automountToken` is `false`, and the value's
  own comment states why: "The runner does not call the K8s API, so its token is
  not mounted."
- The runner has no RBAC, so every sandbox would need `TokenReview` permission.

Giving prompt-injectable code a route to the Kubernetes API to authenticate a
call it receives is a larger concession than the problem it solves. ADR-0006's
rails and ADR-0008's boundary are worth more than avoiding a shared secret.

## Decision

**A pre-baked pool token may authenticate adoption and nothing else. It must
stop being sufficient the moment a pod is bound to a conversation.**

That constraint, not the shape of the secret, is what this ADR decides. An
earlier version of this record decided "one token per version" outright, and
review showed why that is not enough on its own: pool pods are interchangeable
only while unbound. Once several are claimed concurrently they hold different
conversations, and a credential every one of them accepts is a credential that
reaches a sibling's live conversation. The reasoning is recorded below under
*What review found*, because it is the substance of this decision.

1. **The pool's token is a bootstrap credential.** Every pod of a pool boots with
   it, supplied from one `Secret` per pool via `secretKeyRef`, since a static
   `SandboxTemplate` can express nothing per-pod. Its only authority is to let a
   claim adopt an unbound pod.

2. **Adoption installs a per-conversation credential and retires the bootstrap.**
   ADR-0116 decision 2 already sends session identity to a bound runner over the
   ACI; that same call is where a fresh per-conversation token is delivered and
   the bootstrap stops being accepted for that pod. After it, possession of the
   version token authenticates nothing against that pod.

3. **The per-conversation token lives where the route lives.** The substrate
   already carries a token on its `RouteRecord`
   ([`substrate.py`](../../apps/worker/src/curie_worker/sandbox/substrate.py)), and
   that record is in Valkey rather than in a worker's memory, which is how any
   replica already recovers a bound thread's token today.

4. **Reading the bootstrap needs a read grant, and this ADR asks for one.** An
   earlier version claimed the creator "already knows the value and does not have
   to read the Secret back". That only holds for the single process that created
   it; a restart or a second worker replica has neither. The grant is `get` on
   that one Secret.

### What review found, and why the decision changed

The first version of this record argued that a shared token is harmless because
**pool pods of one version are interchangeable by construction** -- same bundle,
same credentials, no session, no transcript -- so reaching a sibling gains an
attacker nothing. That is true, and it is true only of *unbound* pods.

The token does not stop at binding. Once several pods of a pool are claimed
concurrently they are no longer interchangeable: each holds a different live
conversation, and every one of them still accepts the same credential. So
possession of the version token authenticates `event`, `steer`, and `interrupt`
against **a sibling's conversation**. Sandbox egress does not save this, because
the runner's ingress policy admits the whole release namespace rather than only
the worker.

That is why decision 1's authority is bounded to adoption and decision 2 retires
the bootstrap. With those, the shared credential's reach is exactly the set of
pods for which the original argument holds -- unbound, interchangeable, holding
nothing.

The claim in the first version that a token "never spans two conversations' data"
was wrong as written. Destroying a pod on release prevents *sequential* reuse,
which ADR-0116 measured; it says nothing about pods bound *simultaneously*, which
is the case that matters here.

The boundaries that are preserved, and were not the issue:

- **Cross-version**: a different agent version is a different pool with a
  different Secret.
- **Cross-tenant**: unchanged; ADR-0008's namespace boundary is where tenants are
  separated and nothing here crosses it.

## Evidence (measured 2026-08-25)

The whole mechanism was spiked before being proposed, because ADR-0116's version
of it was written without checking that a pool could mint. Four checks, each
acting as a control on the others, against a real release with a bundle-loaded
pool:

| check | result |
| --- | --- |
| warm pod, no token in the template, unauthenticated `POST /v1/event` | **HTTP 200** (the gap, reproduced) |
| warm pod, token via `secretKeyRef`, unauthenticated | **HTTP 401** (gap closed) |
| same pod, correct bearer | **HTTP 200** (still usable) |
| same pod, wrong bearer | **HTTP 401** (not vacuous) |

And the property this must not break:

| check | result |
| --- | --- |
| env-free claim binds the tokened pool pod | **0.16s** |

So a pre-baked token costs nothing that ADR-0116 bought, and proving it required
**no product code**: `secretKeyRef` is already expressible in the shipped
`SandboxTemplate`, which is why the spike is configuration only.

What this measures is narrow, and worth stating precisely. It shows a pre-baked
token gates the ACI and does not cost the sub-second bind. It says **nothing**
about the adoption-and-retire behaviour that decisions 1 and 2 now rest on: that
does not exist yet and has not been spiked.

## Consequences

- **This is no longer separable from ADR-0116 decision 2.** The bootstrap is only
  safe because something retires it at adoption, and the only place to do that is
  the ACI call decision 2 introduces. If decision 2 does not land, this does not
  either, and the warm pool cannot be raised safely at all. That is a tighter
  coupling than the first version of this record implied.

- **A leaked bootstrap exposes that version's *unbound* pods until the pool
  rolls.** An attacker can adopt one, which means getting a runner of that agent
  version to run turns with that version's credentials. That is real, and it is
  the trade being made against a status quo of no token at all. What it does not
  reach, once the bootstrap is retired at adoption, is a conversation already
  bound.

- **Rotation means replacing pods**, since a container reads its environment
  once. Acceptable for a value whose lifetime is a version's lifetime, and worth
  stating as the operational difference from a per-claim token.

- **Two token paths exist during the transition**, per-claim for cold creates and
  bootstrap-then-per-conversation for pool binds. That lasts as long as the cold
  path does.

- **The worker gains `get` on one Secret per pool.** Small, but it is a new grant,
  and the first version of this record wrongly claimed it was avoidable.

## Unresolved

Three questions from review, listed rather than papered over. All three have
since been spiked and are answered in the sections below; the wording is kept so
the record shows what review asked before the evidence existed.

1. ~~**Generation skew during a roll.**~~ **Spiked; see below.**

2. ~~**Concurrent pool creation.**~~ **Spiked; see below.**

3. ~~**What retiring the bootstrap means concretely.**~~ **Spiked; see below.**

## Retiring the bootstrap, measured

Review asked what "the runner stops accepting it" means concretely: whether the
runner holds one active credential or a set, what happens to an in-flight request
during the swap, and what a failed adoption leaves behind. Rather than answer on
paper a third time, the mechanism was built and run.

The runner's gate today captures the token in a closure at app construction, and
says so: *"The configured token is invariant for the process, so encode it once
here rather than on every gated request."* The spike replaced that with a holder
the middleware reads per request, and added a gated `POST /v1/adopt` that swaps
it. **One active credential, replaced outright** -- not a set, not an overlap
window.

**Adoption and retirement, 8/8:**

| | |
| --- | --- |
| `event`, no auth / wrong token, before adoption | 401 / 401 |
| `event`, bootstrap, before adoption | 200 |
| `adopt` presenting the bootstrap | 200 |
| `event`, **bootstrap, after adoption** | **401** |
| `event`, new conversation token | 200 |
| `adopt` again presenting the bootstrap | 401 |

So the bootstrap's authority ends exactly at adoption, and it cannot re-adopt.

**An in-flight turn survives the swap.** Rotating while a turn was genuinely
running -- `/status` reporting `turn_active: true` at the moment of the call,
against a real streaming model rather than the fake one -- the turn completed
normally: HTTP 200 after 11.4s. The reason is structural rather than lucky: the
gate runs at admission, in middleware, so a request already past it is not
re-checked. Afterwards the old token returned 401 and the new one 200.

**A failed adoption is inert, 8/8.** Malformed JSON, a missing `token` field, an
empty string, and a non-string all return 400, and after each one the current
credential still returns 200. The swap happens only after validation, so a failed
adoption leaves no partial state.

The spike is throwaway and deliberately minimal -- it proves the shape is
reachable and cheap, not that this is the final surface. What it settles is the
question review actually asked: retirement is expressible, an in-flight turn is
not collateral, and a bad adopt cannot brick a pod.

## Skew and concurrent creation, measured

Both leftovers turn out to be questions about Kubernetes rather than about
Curie, so they were settled in a scratch namespace with one busybox Deployment
and two Secrets.

**Concurrent creation is already solved by the API.** Two actors racing to create
the same Secret: exactly one succeeded, and the loser got `AlreadyExists` rather
than a partial write or a silent overwrite. That is a deterministic signal to
**adopt the winner's value instead of guessing**, which is all a reconciler needs.
No design is required here beyond using create-or-adopt.

**Generation skew is real, and it lasts longer than "mid-roll" suggests.**

| | |
| --- | --- |
| pod boots, reads Secret | `WORKER-ONE-TOKEN` |
| Secret rotated underneath it | pod still reports `WORKER-ONE-TOKEN` |
| pod replaced | replacement reports `ROTATED-GEN-TWO` |

Environment is read once at start, so a running pod keeps its generation and only
a replacement picks up the new one. **A pool therefore holds two live token
generations at once**, and a binder that reads only the current Secret will
present the wrong one to an older pod.

How long that lasts is the part worth naming. `SandboxWarmPool.updateStrategy`
takes `Recreate` or `OnReplenish`, and **`OnReplenish` is the default** the chart
uses. On that setting a pool does not roll itself when its template changes; an
old warm pod persists until something consumes it. The two generations coexist
for as long as the pool is left alone, not for the length of a roll.

**So the answer is not to reconcile the skew but to avoid creating it.** A pool is
keyed by agent version, and a version's bundle is immutable
([`bundles.py`](../../apps/api/src/curie_api/routers/bundles.py) refuses a second
write: "bundles are immutable"). A token change should therefore be **a new pool,
not a mutation of an existing one**: pod and Secret are created together, retire
together, and no pod ever outlives the Secret it booted from. Rotation becomes
pool replacement, which is the same operation a new version already performs.

The generic mechanic above is measured. The pool-controller half -- that
`OnReplenish` leaves old pods in place while `Recreate` rolls them -- is read from
the CRD rather than run, because a `SandboxWarmPool` in a scratch namespace could
not start: the shipped template references Secrets that live in the release
namespace. Worth confirming before implementation, and flagged rather than
presented as measured.

### The same fact for the policy gate, and what it means for revocation

Whether a live pod can have its bundle's `approvalPolicy` change underneath it:
**no.** `resolve_approval_policy` is called inside `build_runner`
(`__main__.py:180`), so the gate is resolved once at boot from the bundle in that
pod, and a bundle is immutable for its version. A second session exercised this
on the live cluster in the fail-closed direction only, by adding a gate to a read
tool rather than removing one: a thread continued with `--continue` after the new
bundle shipped still ran the tool ungated, while a new thread stopped for
approval. The gate was then removed and the cluster returned to its prior gate
set.

That confirmation carries a consequence worth stating plainly, because "different
pods of different versions" reads as though the two generations are contemporaries
that will sort themselves out. They are not. `adopt` refreshes the route on every
use (`kernel.py:1748`), so `routeTtlSeconds: 3600` is an **idle** bound rather
than a cap: a thread used more often than hourly pins its sandbox indefinitely.
There is no elapsed time after which an operator may assume an added gate has
reached existing threads. The edge points the wrong way, too, since
`suspendedRouteTtlSeconds` is `86400`: a thread suspended awaiting an approval
pins its pod for a day, so the threads that hold a pre-change policy longest are
exactly the ones already doing approval-gated work.

The token behaves identically, and this decision depends on it. Exercised against
shipped code with the route round-tripped through its JSON form and the source
already rotated to a second generation, `adopt` returned the pinned first
generation and refreshed the TTL:

```
token persisted through Valkey   yes
source has rotated to            gen-two
adopt() presented                gen-one (pinned)
touch refreshed the TTL          [('affinity.touch', 'T1', 3600)]
```

`RouteRecord.to_json` serializes the whole handle, and `adopt` returns
`record.handle` after checking claim and sandbox readiness. It never consults a
live source for the credential. That is what makes this decision safe to ship:
rotating a pool's Secret cannot strand an in-flight thread with a token its pod
will refuse, because the binder presents the one the route recorded.

What has **not** been exercised, stated rather than composed away: the two halves
are the binder handing back the recorded token (above) and the pod keeping the
token it booted with (measured separately, in the skew spike). Nobody has run a
gated write against a long-lived adopted thread, which is where they meet. Half of
that is reachable today and half is not, because in shipped code the token is
minted per claim, so the shared per-version source this decision introduces does
not exist yet to be rotated underneath a live thread. Concretely:

- **Reachable now, and worth doing before decision 2 ships:** a gated write on a
  thread adopted well after its claim still admits with its recorded token. This
  exercises the runner's admission check end to end, which neither half above did.
- **Not reachable until decision 2 exists:** the same write after the per-version
  source has rotated. Simulating it with a patched runner would test the spike,
  not the decision, so it is named here as a pre-merge check on the implementing
  PR instead of being claimed now.

The same property is why rotation is **not** revocation. A live thread keeps
presenting its pinned token, its pod keeps accepting it, and replacing the pool
does not disturb either.

Neither finding is a hole the platform did not know about. `reset-thread` was
shipped for exactly this (#737), and its own help text names the condition:

> The worker's next maintenance tick deletes the thread's claim and route, so its
> next message cold-creates a fresh sandbox instead of adopting one that may be
> running stale env.

So the acknowledged mechanism is stale **env**. What review and this record add is
that two things nobody wrote down travel inside it. State it once, since it is a
single property with two instances:

**What a sandbox booted with is what it keeps. Changing the source changes new
sandboxes only. Revocation requires cycling the thread.** The credential is one
instance and the resolved `approvalPolicy` is another, and a reader who knows the
rule does not have to rediscover it per field.

Two operational details belong with that, because "cycle the thread" is easy to
read as instant and free:

- It is **not immediate.** The release lands on the worker's maintenance tick,
  `reclaim_interval_s` defaulting to `30.0`; `reset-thread` polls and reports the
  release as still pending after `RESET_RELEASE_TIMEOUT`, 45s.
- It **interrupts a live turn**, which is why the command refuses without `--yes`.
  The remedy for "a gate I just added does not cover this thread" costs whatever
  that thread was mid-way through.

So the blast radius of a rotation until someone cycles the thread is "for as long
as the thread stays active", not "until the pool drains".

## Out of scope

- **Whether the runner should authenticate callers by identity rather than by
  bearer token at all.** The `TokenReview` shape is rejected here on the cost of
  reaching the API server from a sandbox, not on merit. If Rail 1 ever has a
  reason to allow that route, this decision is worth revisiting.

- **The ingress policy's breadth.** `curie-runner-ingress` selects by
  `namespaceSelector` with no `podSelector`, so it admits any pod in the release
  namespace rather than only the worker, which is wider than its own comment
  describes. Measured separately: a sandbox still cannot exploit that, because
  its egress is default-deny. It is a pre-existing shape, unchanged by this
  decision, and worth its own look.

# 122. A warm pool's runner token is per version, not per pod

Date: 2026-08-25

Status: Draft

**Supersedes in part [ADR-0116](0116-session-identity-arrives-over-the-aci-so-a-sandbox-can-be-pre-bound.md)**,
whose decision 2 says the pool "mints one per pod at creation". It cannot, and
this record replaces that clause. Everything else in ADR-0116 stands, including
the half of decision 2 that moves session identity onto the ACI `Event` frame,
and including the ordering that puts the token before decision 3's warm pool.

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

**A warm pool's runner token is one secret per pool, and because a pool is keyed
by agent version (ADR-0116 decision 3), that is one token per version.** The
component that creates a version's pool also generates its token, creates the
`Secret` holding it, and references that Secret from the pool's
`SandboxTemplate` with `secretKeyRef`. Every pod the pool produces boots with
`CURIE_RUNNER_TOKEN` set, so the ACI is gated from the moment it is ready.

1. **The creator of the pool is the creator of the token.** It generates the
   value, so it already knows it and does not have to read the Secret back. This
   keeps the change to a `create`, not a new read grant on secrets.

2. **A bound claim does not re-mint.** The worker uses the pool's token for a pod
   it binds from that pool, rather than generating a per-claim one it has no way
   to deliver. The existing per-claim mint stays for the cold-create path, which
   still builds a pod from scratch and can still inject env.

3. **The token's lifecycle is the version's.** It is created with the pool,
   rotated only by replacing the pool's pods, since a running container reads its
   environment once, and deleted when the version's pool is retired.

### Why a shared token is the right shape here, not merely the available one

ADR-0116 rejected this with "a shared pool token would let one compromised
sandbox authenticate as its siblings." That objection assumed the siblings hold
something the attacker does not. Before a conversation binds them, they do not:
**pool pods of one version are interchangeable by construction** -- same bundle,
same credentials, same environment, no session, no transcript. That is the entire
point of pre-binding. An attacker who has compromised one such pod gains nothing
from being able to reach another.

The boundaries that do carry meaning are preserved exactly:

- **Cross-version**: a different agent version is a different pool with a
  different Secret, so its pods are not reachable with this token.
- **Cross-tenant**: unchanged, because ADR-0008's namespace boundary is where
  tenants are separated and this adds nothing across it.
- **Bound versus unbound**: a pod bound to a conversation is destroyed on
  release rather than returned to the pool, which ADR-0116 measured, so a token
  never spans two conversations' data.

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

So the fix costs nothing that ADR-0116 bought. It also requires **no product code
change to prove**: `secretKeyRef` is already expressible in the shipped
`SandboxTemplate`, which is why the spike is configuration only.

## Consequences

- **The gap ADR-0116 measured closes with the pool, not after it.** Since the
  same component creates both, there is no window in which a pool exists with
  untokened pods, which is what made the ordering constraint delicate before.

- **Rotation means replacing pods.** A container reads its environment once, so
  rotating a version's token is a pool roll. That is acceptable for a value whose
  lifetime is already a version's lifetime, and it is worth stating because it is
  the one operational difference from a per-claim token.

- **A leaked version token exposes that version's warm pods until the pool
  rolls.** The blast radius is bounded by the argument above -- those pods are
  interchangeable and hold no conversation -- but it is wider than a per-claim
  token's, which dies with its claim. This is the trade this ADR is making, and
  it is made against a status quo of *no token at all* on those pods.

- **Two token paths exist during the transition**, per-claim for cold creates and
  per-pool for pool binds. That is not a permanent split: it lasts as long as the
  cold path does.

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

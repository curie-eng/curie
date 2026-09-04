# 143. A custom connector is a bundle-built HTTP MCP server that holds its own credential

Date: 2026-09-04

Status: Draft

This Draft answers one question the connector chain has left open: **what shape
does a bundle author build when the external system has no MCP server at all?**
[ADR 0086](0086-bundles-declare-connectors-the-platform-hosts-them.md) decided
that bundles declare connectors and the platform hosts them. [ADR
0087](0087-the-api-renders-connector-objects-the-cli-applies-them.md) and [ADR
0090](0090-a-reconciler-applies-connectors-so-agent-repos-need-no-cli.md)
decided who renders and who applies. [ADR
0094](0094-a-bundle-carries-its-own-sealed-connector-keys.md) decided how a
credential may travel with the bundle. [ADR
0113](0113-bundles-declare-connector-build-inputs-and-tiers-deliver-pinned-images.md)
decided that a connector built from source is pinned by digest at every tier,
and [ADR
0121](0121-a-restore-is-the-connectors-own-verb-run-under-the-same-pinned-connector.md)
decided that a write connector restores through its own verb. Every one of
those assumes the server exists. None of them says how to write one, how it
holds and refreshes a provider credential, what it must log, what it may reach,
or what the CLI offers an author between "I have a Dockerfile" and "an agent
called my tool on a cluster."

It changes no decision above. It is the author-facing contract those decisions
imply, stated once, with the platform obligations that make the contract
keepable, and the author's half was measured before it was written: a
connector meeting the contract was built and run with a released binary on a
real cluster, while the platform obligations below are proposals the spike
showed the need for, not things it exercised. The evidence is in
[`evidence/0143-custom-connector-spike/`](evidence/0143-custom-connector-spike/README.md).

Three decisions in the body are the maintainer's to make and are marked as such.

## Context

### The chain stops at the image

`connectors.yaml` accepts three forms: an `image:` somebody else published, a
`url:` somebody else hosts, or a `build:` context inside the bundle. The first
two cover an ecosystem server. The third is the only path for a system that has
no MCP server, and the only guidance an author has for it is the source of the
reference bundle's four write connectors under `examples/sre-bot/connectors/`,
plus [`docs/writing-a-reversible-connector.md`](../writing-a-reversible-connector.md),
which covers the reply shape of an undoable write and nothing else.

That is not a documentation gap. It is an architecture gap, because the
platform holds opinions the author cannot see until they collide with them: the
renderer decides the process's uid, its read-only root filesystem, the absence
of a temp directory, the absence of a `command`, the absence of any egress
policy, the absence of a ServiceAccount, and the absence of any health probe.
Each of those is right for the platform and each is a trap for an author who
does not know it exists.

### What a real author hit, in one week

The first adopting agent repository needed a connector for a third-party
finance API in the last week of August 2026. Its notes record, generically:

1. **A container `ENTRYPOINT` defeated the platform's `args`.** `connectors.yaml`
   renders `args` into the container's Kubernetes `args`, which replace `CMD`
   and are appended to an `ENTRYPOINT`. An image with an `ENTRYPOINT` started
   the wrong server with the declared arguments hanging off it, answered
   `tools/list` with the wrong surface, and nothing reported a problem.
2. **`/tmp` is read-only in the connector pod.** The rendered container has
   `readOnlyRootFilesystem` and no `emptyDir`, so `tempfile` raises rather
   than falling back. Code that passed every test on a laptop failed on every
   call on the cluster.
3. **A reused mutable image tag deployed stale bytes while every layer reported
   success.** The pod template already carried the tag, so Kubernetes started
   no rollout, `rollout status` truthfully reported the previous one, and a
   `rollout restart` came back on the node's cached digest. From the cluster
   side there is no recovery from a reused tag.
4. **A platform-held credential broker could not serve an unattended
   connector.** A downstream experiment kept the provider's refresh token in a
   grant store and handed the connector a capability. That capability was a
   300 second ticket that only an administrator could mint, so a connector
   that must run for a quarter without a human could not use it. The
   repository moved the credential back into the connector, by reference to a
   Secret, which is what the reference bundle does.
5. **The connector logged nothing per request.** When a turn produced a wrong
   figure nobody could answer "did anyone call this, and what came back" from
   the cluster.
6. **Deployment configuration existed only in the cluster.** The Role that let
   the connector write a reissued token back, the Secret holding the token,
   and a keep-alive CronJob were `kubectl apply`'d by hand beside the bundle.
   One of them, applied with `kubectl apply`, kept a stale copy of the token in
   its `last-applied-configuration` annotation, so a re-apply would have
   silently rolled the live credential back to one the provider had retired.

Every item is a property of how Curie hosts a connector, not of the finance
API. That is exactly the class of knowledge ADR 0086 said the platform should
hold once rather than every bundle rediscovering.

### The spike: the shape built with nothing but what ships

To separate what the author got wrong from what the platform makes hard, the
smallest possible custom connector was built for a stub third-party finance API
and run through the released `curie` 0.8.5 binary and chart on a single-node
k3s cluster: a stand-in API with bearer-token reads and a token endpoint that
rotates the refresh token on every exchange; a two-tool read-only MCP server;
`build:` in `connectors.yaml`; a registry push; `curie cluster deploy`; three
agent turns; a pod restart; a redeploy; and one deliberate removal of the
write-back grant. The full record, commands and outputs is in the evidence
directory. What it established:

**The core loop works and needs no platform change.** A source change produced
a new registry digest, the lock pinned it, `cluster deploy` rendered exactly
that digest, an agent turn called `list_invoices` through the hosted server,
the connector rotated its refresh token, wrote the reissued one back to its
Secret before using the access token, and after a pod restart booted from the
written-back token and served the next turn. A replay of the retired token was
refused by the provider, which is the single-holder rule observed rather than
assumed. Because every tool carried `readOnlyHint`, the runner classified the
surface as read-only and omitted the approval pager. The operator-provisioned
Secret survived a redeploy.

**Ten frictions, measured, on a fresh install with a released binary:**

| # | Observed | Where it bit |
|---|---|---|
| 1 | `curie build --registry` on the default Docker builder pushes a plain schema-2 manifest, not an index. `cluster deploy` then refuses: `covers [] in the registry, but this cluster's nodes report [amd64]`. Routing the same command through a `docker-container` buildx builder produced an OCI index and the deploy passed. The declaration was correct both times; the operator's builder decided the outcome. | build |
| 2 | `tempfile.gettempdir()` itself raises under the rendered securityContext, so a server that merely asks where its temp directory is crashes at startup. The first build of the spike's own server did. | run |
| 3 | With the connector in `CrashLoopBackOff`, `cluster deploy` exited 0 and printed the connector's URL, and the agent turn finalized with a reply saying no such connector existed. The only platform signal was one `WARNING` line in the runner log: `MCP tool-capability probe failed server=stubfin`. | deploy, turn |
| 4 | Every `cluster deploy` adds a deployment row and none supersedes: three deploys left three rows `active`, and the only way to retire one is a `DELETE` per row. The worker resolves among them deterministically (prod first, newest `deployed_at`, then id), so accumulation is a hygiene and audit problem rather than a routing one. The first adopting repository measured twelve rows and reported turns on a stale bundle; that report is not re-examined here. | deploy |
| 5 | An unknown key in `connectors.yaml` (`reaches`, `serviceAccount`) is refused, which is right, but the human path prints only `Error: parse connectors.yaml`. The field name appears only under `--debug` or `--json`. | validate |
| 6 | The write-back needs a Role naming one Secret and a RoleBinding, and `connectors.yaml` cannot declare a ServiceAccount, so the binding goes to `default` and grants the patch to every pod in the namespace that runs as `default`. Both objects, and the credential Secret, are `kubectl` state beside the bundle. Removing the binding made the next refresh fail with a 403 that the model relayed verbatim, and left the provider one token ahead of the Secret. | credential |
| 7 | `curie skill check` reports `declared: []` for a bundle whose only capability is a hosted connector, and no verb in the CLI reports a connector's health, its tools, or its logs; `cluster status` lists the pod among sixteen. | verify |
| 8 | On a released install the worker has no `CURIE_CONNECTOR_RECONCILE` setting, so ADR 0090's reconciler is off and a git-flow push would create a version whose connectors nothing applies. The CLI path is the only path. | deploy |
| 9 | The rendered pod has no readiness or liveness probe and no telemetry environment, so "is it up" is a `kubectl` question and "what did it do" is whatever the author chose to print. | observe |
| 10 | The same source built twice, unchanged, produced two different registry digests, so a rebuild churns the lock without a source change. | build |

Items 1 through 3 and 6 reproduce four of the six frictions above from a clean
start. Item 4 confirms the first adopting repository's twelve-row finding on
0.8.5. Items 5, 7, 8, 9 and 10 are new.

## Decision

**A custom connector is a bundle-built HTTP MCP server that holds its own
credential.** The contract below is what an author writes to; the platform
obligations are what make it keepable. Where an obligation needs a schema field
on the frozen `plugin-format` interface, that field lands as its own reviewed,
backward-compatible change before anything depends on it, exactly as ADR 0094
and ADR 0113 required for theirs.

### 1. How a connector is written

**The contract is at the wire, not the language.** A connector is any process
that, started from its image with the platform's environment and nothing else:

- serves MCP over streamable HTTP on `0.0.0.0:<port>` at `/mcp`, stateless, on
  the port `connectors.yaml` declares (default 8000). stdio is not a connector
  transport: a hosted connector is a Deployment behind a Service, and a stdio
  process reaches end of input and exits 0, which reads as success.
- answers `tools/list` with no credential present, so the surface can be
  audited before a secret exists.
- declares all four standard annotations on every tool, truthfully. The runner
  classifies the whole surface as potentially write-capable if any one tool
  omits `readOnlyHint`, and a failed probe counts the same way. A connector
  that exposes a write tool also exposes `restore` or advertises no restore at
  all (ADR 0121, decision 5).
- distinguishes configuration from credential. Configuration (the provider's
  base address, an allowlist, a mode switch) that is absent stops the process at
  startup with the reason on stderr, because a connector configured to reach
  nothing has nothing to serve. A credential that is absent or unusable does not
  stop the process: `tools/list` still answers, and the first tool call fails
  with the reason in the `ToolError`. A healthy pod that 401s every call
  silently is the failure ADR 0094 named; the contract turns it into a call that
  names its own cause.
- ships `CMD`, never `ENTRYPOINT`, when it expects `args` to choose anything.
- asks for scratch space through `TMPDIR` rather than assuming `/tmp`.

**Python with the official `mcp` SDK is the reference shape, not a
requirement.** It is what the reference bundle's connectors and the spike use,
so its traps are the platform's known traps (annotation introspection under
stringized type hints, the transport wiring) and its scaffold is the one the
CLI generates (decision 6). A connector in another language that meets the
wire contract is a connector.

**The recommended boundary is one connector per external system**, split
further only for a credential reason. The platform cannot infer system identity
from a declaration and does not enforce this; it is guidance, and the scaffold
follows it. A write surface becomes a second connector when it needs a
separate credential or a narrower grant that the read credential must not hold,
which is why the reference bundle splits `kubernetes` from `k8s-write` and
`k8s-scale`, and ADR 0121 keeps that split as a legitimate boundary. A
connector per verb with no such reason has no stopping rule, and the scaffold
does not produce one.

Platform obligations: the renderer mounts an `emptyDir` at `/tmp` and sets
`TMPDIR`, so friction 2 becomes unrepresentable; `connectors.yaml` gains an
optional `command:` beside `args:` so an image with an `ENTRYPOINT` can be run
correctly and friction 1 of the adopting repository is a declaration rather than
a rebuild; the validator's human output names the offending key the way its
`--json` output already does (friction 5).

### 2. How it holds and refreshes a provider credential

**The connector holds the credential, by reference, and is its only holder.**
The credential arrives as environment variables from a Secret named in
`connectors.yaml`, either operator-provisioned (`from_secret`) or sealed to the
cluster (ADR 0094). No value appears in the bundle, the image, or release
history. `replicas: 1` is rendered by the platform, but one replica is not one holder:
the rendered Deployment uses the default `RollingUpdate` strategy, so during a
rollout an old and a new pod overlap, each started from whatever the Secret
held at its own start. Retiring any older holder is part of deploying a
connector whose provider rotates refresh tokens, because the provider retires
the previous token on reissue and a second holder dies at the next exchange.
The spike observed the refusal: a replay of a retired token returned
`invalid_grant`.

**A reissued token is written back before the access token it arrived with is
used.** The write is a `PATCH` of the one Secret the token came from, through the
pod's own service account, retried with backoff, and blocking: a slow tool call
is worth incomparably more than the only copy of a credential. If every attempt
fails, the tool call fails with the reason and an instruction not to restart
the connector, because at that moment the reissued token exists only in
process memory. The spike observed both halves: the write-back on every
rotation, and the exact failure when the grant was removed, followed by recovery
on the next rotation once the grant returned, with no restart.

**Decision 1 for the maintainer: where the credential lives.** This ADR
chooses the connector. The alternative, a platform-held grant store that keeps
the refresh token encrypted, serialises refreshes under a lock, and hands the
connector a short-lived capability, is better on every axis but one: it is not
reachable by an unattended connector today, and building it is a separate
architectural decision with its own ADR. Choosing the connector now does not
close that door. It fixes the contract the author writes to, so that a later
move to a grant store changes where a connector gets a token and nothing else.

There is a window this design cannot close. Between the provider retiring the
old token and the Secret accepting the new one, a crash loses the credential,
and no ordering of the two writes removes that: the provider's retirement is
the first write and it is not ours. The contract shrinks the window to one
retried `PATCH` and makes the failure loud; it does not claim the window is
gone. A provider that supports overlapping refresh tokens has no such window,
and the holder should use it when offered.

Platform obligations: a connector that declares a rotating credential gets its
own ServiceAccount and a Role that grants `patch` on exactly its own Secret,
rendered by the platform and pruned with the connector, so friction 6 stops
being hand-applied state bound to `default`; the same declaration renders the
Deployment with `strategy: Recreate`, so a rollout never runs two holders. The declaration is one field on the
secret reference, `writeback: true`, and it is refused on a sealed secret until
ADR 0094's reseal story can absorb a value the cluster itself changed. The CLI
mints an operator-provisioned Secret with `kubectl create`, never `kubectl
apply`, so no copy of a token rides on the object in an annotation.

### 3. How it is built and pinned

**Unchanged from ADR 0113, with the build made deterministic in outcome.** A
connector is declared as `build:` in `connectors.yaml`, `curie build
--plugin-dir <bundle> --registry <ref>` builds every declared platform, pushes,
and records the registry manifest digest in `connectors.lock.yaml`, and the
deploy renders only that digest. A mutable tag never reaches a pod template,
which closes the adopting repository's friction 3 by construction; the spike
watched a source change produce a new digest and the pod come up on exactly it.

Platform obligations: `curie build` produces an index the deploy preflight
accepts regardless of which buildx builder the operator's Docker happens to
select, or fails at build time naming the builder and the fix, so friction 1 of
the spike cannot surface at deploy. `curie build` is a no-op when the source
digest already in the lock matches the tree, so an unchanged source does not
churn the lock (friction 10).

### 4. How it declares egress reach

**A connector declares the hosts it reaches, with a reason, in
`connectors.yaml`.** Today a connector pod is rendered an Ingress policy and no
Egress policy: it is unrestricted outbound by construction, which is why the
call to a CDN-fronted provider succeeds there and nowhere else, and also why a
reader of the bundle cannot learn what the connector talks to without watching
the cluster. The adopting repository wrote a sidecar file for this because the
schema refused the key; the spike confirmed the refusal.

```yaml
connectors:
  finance:
    build: {context: connectors/finance, platforms: [linux/amd64, linux/arm64]}
    reaches:
      - host: api.finance.example.com
        why: the company file's reports, and the only provider this reads
      - host: oauth.finance.example.com
        why: the token exchange
```

**Decision 2 for the maintainer: declared, or declared and enforced.** This
ADR recommends enforced: a connector that declares `reaches:` is rendered an
Egress NetworkPolicy that admits cluster DNS and the declared hosts and nothing
else. A connector that declares nothing keeps today's unrestricted egress, so
no existing bundle changes behaviour, and a bundle can opt in one connector at
a time. The alternative is to accept the field as documentation only. That is
cheaper and it is honest, but it leaves the one place the platform could bound
a compromised connector's blast radius unbounded, and a declared-but-unenforced
list reads as a control to every reader who did not write it.

How a hostname becomes a rule is an implementation question this ADR leaves
open, because every answer touches a boundary an earlier ADR drew. Kubernetes
NetworkPolicy has no hostname form. Resolving at deploy time in the CLI, the
way `cluster up --allow-egress-host` resolves a closed set of model-provider
names at install, keeps ADR 0087's pure renderer (the addresses travel as
request parameters like `app_name` does) but goes stale when a CDN moves.
Re-resolving in the reconciler keeps the policy fresh but has the reconciler
deriving something the API did not render, which ADR 0090 ruled out. An
FQDN-aware CNI policy or an egress proxy that enforces by name would make the
declaration directly enforceable with no resolution step, at the cost of a
cluster prerequisite the chart cannot install, the same shape as the gVisor
runtime class. The recommendation is to land the field and the documentation
value first, and to choose the enforcement mechanism with the CNI question
answered rather than assumed.

### 5. How it logs per request

**One structured line per request on stdout is the contract, and the platform
makes those lines findable.** Every line is a JSON object carrying `event` and
`ts`. A `tool_call` event adds `tool`, `ok`, `upstream_status` and
`duration_ms`, and `error` when `ok` is false. A credential event
(`token_refresh`, `token_persisted`) adds `ok` and whatever describes the
outcome (`rotated`, `persisted`, `attempt`, `error`). A `startup` event records
what the process found (configuration present, credential present, scratch
space usable). No argument values that could carry a secret, no token, no
credential. The spike's server does this in twenty lines and it is what made
every finding above readable from `kubectl logs`: which tool was called, what
the provider answered, whether a rotation persisted, and why a call failed.

Platform obligations, which are what turn a convention into something an
operator can rely on: the renderer adds a TCP readiness probe on the declared
port so a crash-looping connector is never Ready; `cluster deploy` waits for
every connector it applied to become Ready and exits non-zero naming the one
that did not, with its last log lines, so friction 3 cannot report success; the
runner's failed capability probe is surfaced in the turn's diagnostics rather
than as a warning nobody reads; and a `curie cluster connectors` verb (decision
6) answers "did anyone use it and what failed" from those lines without a
tracing stack.

A TCP probe proves that something listens and nothing more. Credential health,
the state where the server is up and every authenticated call fails, is
deliberately not a readiness condition: a probe that exercised the provider
would spend the provider's rate limit and, for a rotating token, would itself be
a holder. It is instead an observe-time question: `curie cluster connectors`
performs `tools/list` and, with `--probe <tool>`, one read call the author
nominates, and reports the result. The contract's first-call refusal is what
makes that call informative.

### 6. What the CLI offers natively

The author's path is five verbs. Two exist today (`build` and `deploy`) and
three are new (`new`, `verify`, `connectors`):

| Step | Verb | State today |
|---|---|---|
| scaffold | `curie connector new <name> --plugin-dir <bundle>` | absent. Writes the reference shape into the bundle: the server with two annotated read tools and the credential holder, its Dockerfile with `CMD`, its `connectors.yaml` entry with `build:`, `reaches:` and a `from_secret` reference, and a contract test that probes `tools/list` and asserts every tool is annotated. |
| build | `curie build --plugin-dir <bundle> --registry <ref>` | exists (ADR 0113). Gains the index guarantee and the no-op on an unchanged source. |
| verify | `curie connector verify --plugin-dir <bundle>` | absent. Runs each locked image locally the way the platform will, uid 65532, read-only root, no credential, and reports: the process the container actually runs, whether `tools/list` answers, every tool's annotations, whether the startup refused cleanly without its secret, and whether the manifest in the registry covers the declared platforms. This is the gate that would have caught the adopting repository's frictions 1 and 2 and the spike's frictions 1 and 2 before a cluster was involved. |
| deploy | `curie cluster deploy --plugin-dir <bundle>` | exists (ADR 0087). Gains readiness waiting and supersession (below). `curie local deploy` keeps hosting the same locked image (ADR 0113). |
| observe | `curie cluster connectors <agent>` | absent. Lists each connector with its digest, readiness, tool count and read-only classification, and streams its structured log lines with `--logs`. `curie skill check` reports a declared hosted connector as declared rather than as nothing. |

**Decision 3 for the maintainer: a deploy supersedes.** This ADR proposes that
`cluster deploy` ends the previous active deployment for the same agent and
environment, so exactly one is active per environment. Today N deploys leave N
active rows; the worker and the connector reconciler both resolve the newest
deterministically, so nothing is misrouted by the accumulation, but every stale
row stays `active` until an operator issues a `DELETE` per row, the audit
question "what was active on this date" has N answers, and the first adopting
repository's operators learned to prune by hand. This is wider than the
custom-connector question and is marked as such: it is a deployment-lifecycle
decision, raised here because connector rollout is where an author meets it.
The draft canary decision on `next` wants a *weighted second active deployment*
as an explicit act; supersession by default is compatible with that, because a
canary is declared rather than accumulated.

## Consequences

- **The bundle stays the whole declaration.** A connector's source, its build
  inputs, its credential references, its write-back grant, its reach and its
  logging contract all appear in versioned files. The `kubectl` state the
  adopting repository carried beside its bundle (Role, RoleBinding) is rendered
  from the declaration and pruned with the connector; the Secret it provisions
  by hand is the one thing that stays out of band, by ADR 0094's design.
- **Three schema fields land on the frozen interface**, each additive and
  optional: `command`, `reaches`, and `writeback` on a secret reference. They
  ship as one reviewed compatible change before any consumer depends on them.
- **The renderer gains a ServiceAccount, a Role, a RoleBinding, a readiness
  probe, an `emptyDir` and, on opt-in, an Egress NetworkPolicy** per connector.
  The reconciler's RBAC widens by exactly those kinds, and `CONNECTOR_KINDS` in
  the Kubernetes client grows with it; an object of a new kind carries the
  owner label like the rest and is pruned by the same rule.
- **`cluster deploy` becomes slower and honest.** Waiting for readiness costs
  seconds on a healthy connector and turns a silent broken deploy into an exit
  code with the connector's last log lines.
- **A connector that rotates a credential cannot yet seal it.** `writeback` on a
  `sealed_secrets` entry is refused until a reseal-after-rotation story exists;
  such a connector uses `from_secret` today.
- **Enforced reach adds an outage mode** when a provider's addresses change
  under a resolved policy, unless enforcement is by name. Declaring nothing
  keeps today's behaviour, and that is the recommended default until the
  enforcement mechanism is chosen and has run against a real provider.
- **Every claim of observability above names its consumer**: the readiness
  probe is consumed by `cluster deploy`, the structured lines by `curie cluster
  connectors --logs`, and the capability probe by the turn diagnostics. Each is
  tracked as a follow-up issue filed at acceptance; none is claimed as present.

## Alternatives considered

- **Document the shape and change nothing.** Rejected. Six frictions were hit
  in one week by an author who had read the reference bundle, and a spike by a
  different author who had read that author's notes hit four of them again from
  a clean start. Two of the six are unrepresentable with a rendered `emptyDir`
  and a `command` field; documentation leaves them representable.
- **A platform-held credential broker now.** Deferred, not rejected: see
  decision 1. It is the better shape and it is a second ADR, and the contract
  here is written so that moving to it changes one function in a connector.
- **Avoid the write-back by avoiding the rotating token.** Four shapes do:
  a client-credentials flow (a static secret exchanged for short-lived access
  tokens, no rotation), provider workload identity (the cloud's identity
  federation issues the token), an External Secrets or operator-owned rotation
  loop that writes the Secret from outside the pod, or a refresh sidecar that
  owns the token and hands the server an access token over localhost. Each is
  better where the provider offers it, and the contract does not stop an
  author using one: a connector whose credential does not rotate declares no
  `writeback` and gets no grant. The write-back exists for the providers whose
  only unattended flow is a rotating refresh token, which is the case the
  first adopting repository had and the spike modelled. A sidecar was not
  chosen as the default because it doubles the image count per connector for
  the common case and still needs somewhere durable to persist the token.
- **A generic REST-to-MCP adapter connector**, configured from an OpenAPI
  document, so nobody writes a server. Rejected. The value of a custom
  connector is in what it refuses to expose: a finance provider's own MCP server
  carries full CRUD across every entity and no annotation on any tool, which is
  precisely the surface a bundle must not hand a prompt-injectable model. A
  generated adapter reproduces that surface with less thought, not more.
- **Require Python and the official SDK.** Rejected. The contract is testable at
  the wire, and `verify` tests it there. Python is the scaffold because it is
  what the platform's own connectors use, which keeps one set of known traps.
- **stdio connectors spawned by the runner**, as the in-bundle engine template
  is. Rejected for hosting by ADR 0113, and rejected here as a shape: it puts the
  credential in the sandbox, which is the property the whole connector chain
  exists to avoid.
- **Bind the write-back grant to `default`** and document it, as the adopting
  repository did. Rejected. It grants the patch to every pod in the namespace
  that runs as `default`, and it is exactly the hand-applied state ADR 0086 set
  out to remove.
- **A connector per verb**, as the reference bundle's four Kubernetes connectors
  are. Rejected as the general rule: it has no stopping rule. Kept where a verb
  needs its own credential, which is why that bundle is shaped as it is.
- **Make `reaches:` a sidecar file** beside `connectors.yaml`, since that file's
  schema forbids unknown keys. Rejected. A sidecar the platform does not read is
  documentation, and the strict schema exists so that a declaration the platform
  will act on has one home.

# 158. A custom connector is a bundle built HTTP MCP server that holds its own credential

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

This proposal keeps those boundaries. It defines an authoring contract for a
connector that owns its provider credential, including a rotating credential.
It does not require every connector to own a refresh token or replace a
platform grant service. The original 0.8.5 spike remains historical evidence in
[`evidence/0143-custom-connector-spike/`](evidence/0143-custom-connector-spike/README.md).
That directory retains its original number so the recorded run stays intact.

Revised: 2026-09-21. The proposal now separates the authoring contract from
optional platform improvements, egress enforcement and deployment lifecycle.
The original evidence was not rerun. Current source inspection is not live
acceptance evidence.

## Context

### The gap observed by the original author

At the spike baseline, `connectors.yaml` accepted three forms: an `image:` somebody else published, a
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

### Historical spike using released 0.8.5

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

## Current alignment

The public `main` and `next` source inspected on 2026-09-21 still separates
connector declarations from rendered objects in
[`connectors.py`](../../packages/plugin-format/src/plugin_format/connectors.py)
and [`connector_render.py`](../../packages/plugin-format/src/plugin_format/connector_render.py).
Neither tree declares `service_account`, `writeback` or `reaches`. The
historical list above is evidence about 0.8.5, not a claim that all ten
frictions remain open on a current release.

Subsequent adopting implementation retained the connector owned credential.
It added persistence before use, recovery from a token rotated by another
holder, and a process claim that refuses a second cooperating holder inside
the same container. A separate API grant service also exists in that adopting
implementation: it owns encrypted grant state and serialized refresh, while
connectors receive scoped access tokens through expiring capabilities. The
existence of that service does not establish an unattended renewal path for
the connector described here. Neither design is deprecated by this proposal.

That implementation also added an explicit ServiceAccount reference, disabled
automatic token mounting for unnamed connector accounts, selected `Recreate`
for hosted connector rollouts, and supported a declared readiness endpoint.
These are useful implementation precedents, not public release guarantees.
An operator still had to wait for the deployed parser to support the new
ServiceAccount field before changing the bundle. A strict older parser rejects
the entire declaration when it sees an unknown field.

The later credential investigation established another important limit:
permission to PATCH a Secret also permits reading its response, including its
data. It is not a write only grant and cannot isolate one key from other keys
in that same Secret. The public proposal must reflect this boundary.

## Proposed decision

### 1. Author a hosted HTTP MCP server

A custom connector is a bundle built HTTP MCP server, delivered through the
existing digest lock from ADR 0113. Its provider credential stays outside the
runner sandbox. The sandbox must never receive or retrieve the provider
credential, including access tokens, refresh tokens and client secrets.
The bundle owns the connector's deliberately limited tool
surface. Python and the MCP SDK are a reference implementation, not a language
requirement. Prefer one connector per external system; split where credentials
or authority must differ.

The server answers `tools/list` and gives every tool an explicit capability
classification under the existing connector rules. A read annotation describes
tool intent; it does not prove that the provider credential cannot write.
Provider authority needs its own evidence. A connector offering reversible
writes follows ADR 0121 rather than inventing a second restoration protocol.

Missing required configuration produces a specific startup refusal. A missing
or unusable provider credential leaves tool discovery available and makes the
first affected call fail with a safe, useful reason. Verification must
separately test those cases rather than expecting a missing credential to
crash the server. The image must run under the rendered user and filesystem
constraints. Use a declared scratch location and ensure container arguments
select the intended process.

### 2. The connector owns its provider credential

For this custom connector shape, the connector owns its provider credential.
Credentials arrive by Secret reference. No credential value belongs in source, image layers, bundle
history, logs or errors. Static credentials, provider workload identity and
externally managed access tokens do not require connector refresh storage.

When the connector owns a rotating refresh token, it must have a durable sink
before exchanging it. It persists a replacement before using the access token
returned by that exchange. Persistence failures remain visible and block
provider use until recovery. They must not silently leave the only valid token
in process memory while reporting success.

One credential has one active refresh owner across all deployment instances,
environments, diagnostic processes and scheduled probes. `replicas: 1` and
`Recreate` only address one Deployment; they do not establish exclusive
ownership across independently named Deployments or another process. A local
process claim protects cooperating processes sharing that claim, not other
pods or raw provider calls. Sharing a connector across consumers is preferable
to duplicating its rotating credential holder.

The provider may retire the old token before durable persistence succeeds.
A crash in that interval can still require operator reauthorization. A
persistence retry narrows this failure window; it does not remove it. Recovery
must be tested for both refused stale tokens and failed persistence without
accidentally creating another refresh owner.

A platform grant service is a distinct credential owner. A connector using it
must obtain only the access token it needs, through an authenticated grant
scoped to that connector. Adopting such a service as the default would require
an explicit decision about unattended capability renewal, revocation and
recovery. This ADR does not promise that migration is a one function change.

### 3. Bind Kubernetes authority explicitly

A connector needing Kubernetes authority names a dedicated ServiceAccount.
An unnamed account should receive no automatically mounted API token. A
rotating credential holder receives only the authority needed for its own
credential storage, never a grant attached to the namespace's `default`
account. A Secret used for this purpose must not contain unrelated secrets:
PATCH authority exposes the whole object's data through the response.

The proposed first step is an explicit account reference whose operator owns
the account and RoleBinding. Automatic generation and pruning of credentials,
accounts and Roles are a separate lifecycle choice. This avoids treating a
convenient `writeback: true` field as sufficient authorization for new security
policy. Sealed bundle inputs must not overwrite a rotated live token during
redeployment; automated writeback to sealed credentials remains out of scope
until persistence and resealing are designed together.

A supported platform version must reach the deployment before a bundle uses
any new field. Every frozen schema change lands in its own reviewed versioned
prerequisite before a renderer, CLI or adopting bundle depends on it. No schema
or runtime change is included in this PR.

### 4. Keep health and diagnostics useful without exposing credentials

Emit structured request and credential lifecycle events with event type,
time, tool name, success, duration and safe status information. Exclude
provider payloads, tokens, sensitive arguments and raw provider error bodies.
Tool discovery, process readiness and provider credential usability are
separate observations. A listening socket proves only a listening socket.
A readiness endpoint must declare whether it contacts a provider or spends
rate limit; it must not create another credential owner.

A platform verification command should exercise the locked image under the
real rendered constraints, discover its tools and inspect their classifications.
A diagnostic provider call must be explicitly selected and safe to perform.
Scaffolding, native connector logs and deploy readiness waiting are useful
followups, not existing commands or acceptance prerequisites for this authoring
proposal. Their CLI spelling and rollout behavior need their own implementation
review. The existing build and deploy commands continue to deliver the locked
image under ADRs 0087 and 0113.

## Maintainer decisions

| Review question | Recommendation | What accepting this proposal means |
| --- | --- | --- |
| Who owns the credential for this custom connector shape? | Confirmed by the maintainer on 2026-09-21: the connector owns it and the sandbox never has access. | Preserve persistence before use and one refresh owner. A separately selected platform grant service must also keep provider credentials out of the sandbox. |
| How should the holder get Kubernetes authority? | Start with an explicit dedicated account reference and operator owned grants. | Require a separate frozen schema prerequisite and deployment ordering; do not approve automatic RBAC generation. |
| Does declared provider reach become enforced policy? | Defer the field and enforcement mechanism together. | No `reaches` field or Egress NetworkPolicy is promised by this ADR. |
| Does connector deployment supersede previous active agent deployments? | Decide in the deployment lifecycle work. | Connector holder exclusivity remains required; agent deployment row semantics do not change here. |

## Deferred work and consequences

Hostname egress enforcement needs a separate decision about enforcement
mechanism, resolver ownership and cluster prerequisites. Merely documenting
hosts is useful author information but must not be presented as enforced
policy. This proposal does not weaken existing sandbox egress controls or
promise a hostname NetworkPolicy that the renderer cannot produce.

Default deployment supersession is broader than connector authoring. A new
agent deployment row and a new credential holder are different objects;
preventing concurrent refresh owners cannot depend on a future decision about
which deployment rows remain active. Canary design must address credential
ownership explicitly if both versions use the same provider grant.

The original spike and its measured limits remain committed unchanged. It
proves a narrow released connector loop, not the later ServiceAccount support,
provider credential authority, current deployment health or egress enforcement.
No live cluster or provider run was performed for this revision.

Acceptance would authorize the authoring contract above. It would not ship a
new CLI, approve a credential broker migration, create Kubernetes grants or
accept deferred egress and deployment policy. Implementation issues should be
scoped from an accepted decision and must name their versioned prerequisites.

## Alternatives considered

1. Require every provider to use a platform grant service. Deferred as a
   universal rule because unattended lifecycle support must be established for
   the actual connector. Existing service ownership remains a valid option.
2. Give every hosted connector access to the namespace's default account.
   Rejected. A narrow Secret Role still exposes its whole object to every
   process receiving that account's authority.
3. Treat `Recreate` as global credential exclusivity. Rejected. Separate
   deployments and diagnostic processes can still hold the same credential.
4. Generate a generic provider API surface. Not the reference shape. The author
   must deliberately select what the model can invoke and verify the provider
   credential's authority independently of tool annotations.
5. Require Python. Rejected. The contract is the hosted MCP behavior and the
   credential boundary; another language can implement both.
6. Make the runner host the provider refresh credential. Rejected for this
   connector shape because it crosses the existing hosted connector boundary.

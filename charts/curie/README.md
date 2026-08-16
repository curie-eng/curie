# charts/curie

The umbrella Helm chart that installs the whole Curie (Relay) stack on a
single node. It installs the backing-store stack (Langfuse + Postgres + Valkey +
ClickHouse + RustFS + OTel Collector, dev profile, BYO (bring-your-own)
toggles, the two preflights) plus the security rails as chart defaults.

The chart is a direct port of the proven `compose.dev.yaml` dev stack: same
images, same tags, same `:24.8` ClickHouse pin, same headless-bootstrapped
Langfuse dev project. So the compose stack and this chart verify
the identical stack. Rather than vendoring the upstream Langfuse chart and its
Bitnami subcharts, each component is a first-class template here -- this keeps
the single-node footprint controllable and avoids the Bitnami-catalog
(`bitnamilegacy/*`) instability. It still follows the Langfuse chart *idiom*:
every backing store is toggle-gated with a single-block bring-your-own surface.

## Install

The defaults are the flagship path: GHCR (GitHub Container Registry) images, the runner substrate and its
controller on, a modest single-node footprint, and graceful degradation when the
cluster lacks Slack tokens or runsc. So a fresh install is two commands.

**Step 1 -- bare install.** Nothing to build, no overlays, no `--set`:

```bash
helm install curie charts/curie -n curie --create-namespace
kubectl get pods -n curie -w
```

This brings up the full stack (Langfuse + stores + OTel), the four app services
from GHCR, and the runner sandbox substrate. Two things degrade gracefully so the
install is green with zero secrets:

- **Slack** is not connected (no tokens), so the dispatcher Deployment is skipped
  rather than crash-looped, and the runner stays in offline fake-model mode.
- **gVisor** kernel isolation is `auto`: if the cluster has the `gvisor`
  RuntimeClass, runner pods use it; if not, they run without it and `NOTES.txt`
  prints a warning. Either way the install does not block.

**Step 2 -- connect Slack + a real model.** When you have Slack tokens and a
model credential, upgrade in place (the exact command is also printed in
`NOTES.txt` after step 1):

```bash
helm upgrade curie charts/curie -n curie --reuse-values \
  --set dispatcher.slack.appToken=xapp-... \
  --set dispatcher.slack.botToken=xoxb-... \
  --set dispatcher.slack.signingSecret=... \
  --set agentSandbox.runner.fakeModel=false \
  --set agentSandbox.runner.credentials=sk-ant-... \
  --set 'security.networkPolicy.allowedEgress[0].cidr=160.79.104.0/23' \
  --set 'security.networkPolicy.allowedEgress[0].ports[0].protocol=TCP' \
  --set 'security.networkPolicy.allowedEgress[0].ports[0].port=443'
```

Setting the two Slack tokens is what makes the dispatcher deploy. The runner
NetworkPolicy is fail-closed (`security.networkPolicy.allowedEgress` is empty by
default), so the `allowedEgress` flags are required to let real model calls reach
the API -- here Anthropic's published range (`160.79.104.0/23`, TCP 443). Add
further entries for any MCP (Model Context Protocol) endpoints the runner must reach. Because this upgrade
flips to a real model, under the default `security.gvisor.mode=auto` it now fails
closed on a cluster without the `gvisor` RuntimeClass (runsc) -- install runsc +
the containerd handler on every node first, or add `--set security.gvisor.mode=off`
(or `-f charts/curie/values-e2e-nogvisor.yaml`) to run real code without kernel
isolation knowingly.

The dispatcher also needs to reach the platform API: a Slack Approve click is
relayed to the API as an approval resolve. By default it is wired to the in-chart
API Service (`http://<fullname>-api:<api.service.port>`, derived, so an overridden
`api.service.port` tracks automatically) and authenticates with the chart Secret's
`apiKey` by reference. Point it at an API this chart did not deploy with
`dispatcher.apiBaseUrl`:

```bash
helm upgrade curie charts/curie -n curie --reuse-values \
  --set dispatcher.apiBaseUrl=https://your-api.example
```

| Value | Default | Meaning |
| --- | --- | --- |
| `dispatcher.apiBaseUrl` | `""` | Empty derives the in-chart API Service. A set value is used verbatim (BYO), and is **required** when `api.deploy: false`, where no in-chart Service exists to derive from. |

Two limits worth knowing. With `api.deploy: false` and an empty
`dispatcher.apiBaseUrl` the dispatcher is pointed at a Service that does not
exist; its boot preflight fails and the pod CrashLoopBackOffs naming the
unreachable URL. That is intentional (a loud boot failure beats a silent
dead-end at click time) and the fix is to set `dispatcher.apiBaseUrl`. And
because the API's `/health` is unauthenticated, that preflight proves only
reachability, not that the key is right: a BYO API expecting a different key
still passes boot and fails at click time. Match `apiKey` to the API you point at.

**Cluster variants:**

- **Cluster already runs the agent-sandbox controller** (cluster-scoped, one per
  cluster): add `--set agentSandbox.controller.deploy=false`.
- **No runsc and you want the no-gvisor shape to be explicit/deterministic**
  (skip the RuntimeClass lookup): `-f charts/curie/values-e2e-nogvisor.yaml`.
  `auto` handles a runsc-less cluster only for the fake-model default; a
  real-model install (`fakeModel=false`) under `auto` now fails closed on a
  runsc-less cluster, so on such a cluster use this overlay (or `--set
  security.gvisor.mode=off`) to run real code without gVisor, or `--set
  security.gvisor.mode=require` to fail-hard.
- **Production sizing:** the default `resources`/persistence blocks are a modest
  single-node footprint (fits an 8-16 GB node). Raise them for real load.

**Local dev profile (offline, locally-built images).** `values-dev.yaml` repoints
every image at a locally-built, cluster-imported tag with `imagePullPolicy: Never`
for a fully disconnected cluster, so you MUST build and import the images first
(see "Publishing and pulling images" below) or the pods die `ErrImageNeverPull`:

```bash
# Prereq: build + import each first-party image into the cluster runtime first.
helm install curie-dev charts/curie -n curie-dev --create-namespace \
  -f charts/curie/values-dev.yaml
kubectl get pods -n curie-dev -w
```

Reach the Langfuse UI:

```bash
kubectl port-forward -n curie svc/curie-langfuse-web 3000:3000
# http://localhost:3000  -- dev keys: pk-lf-curie-dev / sk-lf-curie-dev
```

App services emit OTLP (OpenTelemetry Protocol) to the **collector**, never
straight to Langfuse (Langfuse OTLP ingest is HTTP-only): `curie-otel-collector:4317` (gRPC) /
`:4318` (HTTP). The collector forwards to Langfuse over HTTP.

## Components

| Component | Image | Notes |
|---|---|---|
| Langfuse web + worker | `langfuse/langfuse:3`, `langfuse/langfuse-worker:3` | Observability + eval backbone. Headless-bootstrapped dev org/project. |
| Postgres | `postgres:16-alpine` | Langfuse transactional store + app state. StatefulSet. |
| Valkey | `valkey/valkey:8-alpine` | Langfuse cache/queue + dispatcher Streams queue. |
| ClickHouse | `clickhouse/clickhouse-server:24.8` | Langfuse OLAP store. Tag pinned SSE4.2-safe (see preflight). |
| RustFS | `rustfs/rustfs:1.0.0-beta.12` plus `amazon/aws-cli:2.32.6` init | Langfuse object storage; BYO real S3 in prod. |
| OTel Collector | `otel/opentelemetry-collector-contrib:0.119.0` | OTLP (gRPC+HTTP) -> Langfuse over HTTP. |

## Publishing and pulling images

First-party service images are published to GHCR by the `Release images`
workflow (`.github/workflows/release.yaml`) on every push to `main`, as
`ghcr.io/curie-eng/curie-<service>` tagged with the commit SHA and `latest`.
All six first-party services build in the matrix: `curie-api`,
`curie-dispatcher`, `curie-mail-adapter`, `curie-worker`, `curie-ui`, and
`curie-runner`. The chart defaults every first-party image at its
`ghcr.io/curie-eng/curie-*` `:latest`, so the bare install (above) pulls from
GHCR with no image overrides -- except `curie-mail-adapter`, whose Deployment is
off by default (`mailAdapter.deploy`), so its image is published and defaulted
but not pulled until an operator enables the email channel.

- **Pull policy for the five Deployment-managed services** (api, dispatcher,
  mail-adapter, worker, ui): `imagePullPolicy: Always` -- they pull once per
  rollout, so `Always` just keeps a fresh install from serving a stale `latest`
  a node cached earlier.
- **The runner image is the exception:** it uses `imagePullPolicy: IfNotPresent`
  because a sandbox pod is cold-created per Slack thread, and an `Always`
  (re-)pull inside that boot window blew past the worker's claim timeout and
  killed runs. Its freshness comes instead from the `runner-prewarm` DaemonSet
  (`agentSandbox.runner.prewarm`, default on with the sandbox substrate),
  which pulls the runner image `Always` and keeps it pinned on every node; a
  Release-revision annotation rolls those pods on every `helm upgrade` so the
  pin refreshes a churned `latest`.
- **Pin an immutable tag** for reproducible deploys, where the pull policies
  are a cheap no-op.

A GHCR package inherits its repo's visibility, so on a **private** repo the image
is not anonymously pullable and the node needs credentials. Two supported paths:

- **Private + pull Secret (default posture).** Create a docker-registry Secret
  in the release namespace and reference it:
  ```bash
  kubectl create secret docker-registry ghcr-pull -n <ns> \
    --docker-server=ghcr.io --docker-username=<gh-user> \
    --docker-password=<a PAT or token with read:packages>
  helm install ... --set 'agentSandbox.runner.imagePullSecrets[0].name=ghcr-pull'
  ```
  The chart wires `imagePullSecrets` onto the runner SandboxTemplate pod.
- **Public package.** In the GHCR package settings make the package public; then
  no pull Secret is needed and `imagePullSecrets` stays empty.

For offline dev/e2e, `-f values-dev.yaml` overrides all six first-party images
back to locally-built, cluster-imported tags with `imagePullPolicy: Never`, so a
disconnected cluster never attempts a GHCR pull. `curie-mail-adapter` is
overridden the same way even though `mailAdapter.deploy` is false in that
profile, so `--set mailAdapter.deploy=true` on a disconnected cluster starts
rather than hanging on a GHCR pull. That path requires building and importing
each image first:

```bash
for svc in api dispatcher mail-adapter worker ui; do
  docker build -f apps/$svc/Dockerfile -t curie-$svc:local .
done
docker build -f runner/Dockerfile -t curie-runner:latest .
# import each into the cluster runtime, e.g. for k3s:
for img in curie-api:local curie-dispatcher:local curie-mail-adapter:local \
           curie-worker:local curie-ui:local curie-runner:latest; do
  docker save "$img" | ssh <node> 'sudo k3s ctr images import -'
done
```

Skip the build+import and the `Never` pull policy leaves the pods stuck at
`ErrImageNeverPull`. For a from-GHCR install with no local build, just use the
bare `helm install` (the default) -- no overlay needed.

## Values surface and the BYO idiom

Keys are **camelCase** (Go templates cannot dot-index hyphenated keys). Every
backing store is condition-gated by `<store>.deploy` and carries its BYO fields
on the same block. To use an external instance, flip `deploy: false` and fill
`host` / `port` / `auth` (or `existingSecret`):

```yaml
# Use a managed Postgres instead of the in-cluster one
postgres:
  deploy: false
  host: my-rds.example.com
  port: 5432
  auth: { username: curie, database: curie }
  existingSecret: my-pg-secret   # must carry key: postgresPassword
```

Toggles (all default `true`): `langfuse.deploy`, `postgres.deploy`,
`valkey.deploy`, `clickhouse.deploy`, `rustfs.deploy`, `otelCollector.deploy`.
Flipping any to `false` removes its resources from the render; consumers
(Langfuse env, the collector config) repoint at the BYO host automatically.

Secrets: all credentials are written to one `<release>-secrets` Secret. A sealed
`helm install` (the default) AUTO-GENERATES a strong random per release for the
nine chart-owned credentials (the backing-store passwords, the Langfuse
salt/encryptionKey/nextauthSecret, and the api/webhook keys) rather than shipping
the published dev defaults. The generated values are persisted via a `lookup` of
the release Secret, so `helm upgrade` re-uses them and never rotates a live store
credential (the Bitnami lookup-persist convention). Set
`security.allowDevDefaults: true` (values-dev.yaml, i.e. `curie cluster up
--dev`) to keep the deterministic published defaults for dev/CI. A per-store
`existingSecret` and explicit `--set` overrides still win in every mode -- an
override that differs from the published default beats the persisted value on
install AND upgrade (matching Bitnami's provided-value-first precedence), so
rotation/recovery works; point `langfuse.existingSecret` (and each store's
`existingSecret`) at your own Secrets to bring your own. `langfuse.encryptionKey`
must be 64 hex chars (`openssl rand -hex 32`).

Caveat: generation relies on Helm `lookup`, which is empty under client-side
rendering. Driving this chart via `helm template | kubectl apply` or ArgoCD's
client-side Helm (no live API lookup) regenerates these values on every sync and
would rotate live store credentials -- pin them via `--set`/`existingSecret` (or
use the `helm install/upgrade` path / `curie cluster up`) in that case.

Guard against shipping those dev defaults to a shared/production cluster with
`--set security.checkDefaultCredentials=true`: the chart then refuses to render
while `langfuse.init.projectSecretKey` or `langfuse.init.userPassword` still
carries its published dev default (a Langfuse admin-takeover risk on a reachable
UI; the project key also feeds the OTel Collector auth header). Override those
values or supply `langfuse.existingSecret` to clear the gate. It is off by
default so the zero-secret bare install stays green.

## The two preflights

Both run as Helm hooks (blocking a broken install) and are re-runnable via
`helm test <release> -n <ns>`.

**(a) CPU-AVX / ClickHouse-pin check** (`preflights.avxCheck`). A pre-install /
pre-upgrade hook Job.

- ClickHouse >= 25.x is compiled for AVX and SIGILLs with exit 132 on
  SSE4.2-only CPUs -- a crash-looping pod is a confusing way to learn that.
- The Job reads the node's `/proc/cpuinfo`; if the node lacks AVX it FAILS
  the install unless the configured ClickHouse tag is in
  `clickhouse.sse42SafeTags` (`24.8`, `24.3`, `23.8`). Skipped when
  `clickhouse.deploy: false`.
- Test knob `preflights.avxCheck.forceNoAvx: true` exercises the SSE4.2
  branch on an AVX-capable node.
- Read the verdict: `kubectl logs -n <ns> job/<release>-preflight-avx`.

**(b) NetworkPolicy-enforcement probe** (`preflights.networkPolicyProbe`). A
`helm test` Job. A CNI that silently ignores NetworkPolicy is a security
false-pass: the security rails' isolation policies would render but enforce nothing. The probe
does a before/after egress check -- reach an external target with no policy
(expect reachable), apply a default-deny-egress policy to itself (RFC1918
private ranges stay allowed so the control path survives; the public target is
denied), retry (expect blocked). It reports `enforcement=true` only if the after
egress is actually blocked, and `enforcement=false` (fails loudly) otherwise.

## Single-node footprint (measured on a disposable single-node k3s cluster, 4 GB / 4 core)

The dev profile fits the whole stack on one 4 GB node, but **tightly**: steady
state is ~3.3 GB / ~82% node memory once Langfuse migrations settle. Langfuse
web is the anchor (~950 MB resident with the heap cap raised to 1 GB; its Node
default heap of ~512 MB OOM-crashes under a tight container limit, so the dev
profile sets `NODE_OPTIONS=--max-old-space-size` and a 1536 MB web limit).
ClickHouse settles around ~255 MB single-replica with cluster mode off. This
matches the planned resize: everything runs in 4 GB for
chart/security verification, and a resize to >=8 vCPU / 16-20 GB gives
comfortable headroom for integration and soak testing.

## High availability and PodDisruptionBudgets

Every backing store (Postgres, Valkey, ClickHouse, RustFS) ships as a
single-replica StatefulSet -- correct for the single-node dev footprint, but it
means a node drain evicts the store with no budget guarding it. Two levers move
this toward production HA:

- **Real HA is a BYO concern.** These in-chart stores are single-writer and do
  not cluster. For genuine high availability, set `<store>.deploy: false` and
  point the BYO `host`/`port`/`auth` block at a managed or replicated instance
  (e.g. RDS/Aurora Postgres, a Valkey/Redis cluster, ClickHouse Cloud, real S3).
- **Optional PodDisruptionBudgets.** Each store carries a
  `<store>.podDisruptionBudget` block, OFF by default:

  ```yaml
  postgres:
    podDisruptionBudget:
      enabled: true
      minAvailable: 1
  ```

  When enabled, the chart renders a `policy/v1` PodDisruptionBudget selecting
  that store's pods. **Caveat for single-replica stores:** a `minAvailable: 1`
  budget on one replica allows zero voluntary disruptions, so a `kubectl drain`
  of the node blocks until an operator intervenes. That is the point -- it stops
  a routine drain from silently taking the datastore down -- but it requires you
  to handle drains deliberately (scale up first, or delete the PDB for planned
  maintenance). Enable it only once you run multiple replicas or explicitly want
  drains gated.

Production sizing (raise every `resources` block and persistence size, supply
real secrets) is covered in **Production sizing** above; PDBs and BYO stores are
the availability half of the same "not sized for prod out of the box" story.

## Sandbox resource envelope (ADR-0059)

[ADR-0059](../../docs/adr/0059-sandbox-is-a-bounded-resource-envelope.md)
(Architecture Decision Record) treats
sandbox capacity as a security property, not a performance-tuning afterthought:
disk is the one resource dimension a sandbox pod could otherwise consume without
limit. Decision 2 bounds every writable `emptyDir` in the sandbox pod with an
explicit `sizeLimit`:

| Volume | Values key | Default |
|---|---|---|
| `bundles` (fetched archive + extracted plugin dir) | `agentSandbox.runner.bundleFetch.sizeLimit` | `2Gi` |
| `aws-config` (init only AWS CLI path addressing config) | `agentSandbox.runner.bundleFetch.awsConfigSizeLimit` | `16Mi` |
| One per `agentSandbox.runner.hardening.writablePaths` entry (`/tmp`, `/home/runner` by default) | `agentSandbox.runner.hardening.writablePathSizeLimit` | `512Mi` |

**This is a backstop, not an instantaneous cap.** `sizeLimit` is enforced by
periodic kubelet measurement of the volume's usage, not a write-time quota, so a
fast writer can briefly overshoot it before the pod is evicted. That is still a
meaningful improvement: the offending sandbox is evicted on its own account
rather than exhausting node disk and pushing the node into `DiskPressure`, which
degrades every co-scheduled pod including other tenants' sandboxes. Pair it with
the `ephemeral-storage` request/limit on `agentSandbox.runner.resources` (ADR-0059
decision 1, tracked in issue #755) for the scheduling-time bound that keeps a node
from being overcommitted in the first place -- the two are complementary and both
should be set. Raise any of the values above for a legitimately disk-heavy agent
(a large repo clone, a big dependency tree, a generated artifact); the defaults
bound a runaway, not ordinary work.

## Security rails

The security-boundary rails ship **on by default** (ADR-0006). The runner-surface
rails attach to the agent-sandbox, so their NetworkPolicy / RBAC (Role-Based Access Control) / probe resources
render only when `agentSandbox.deploy: true` (there are no runner pods to protect
otherwise). With the sandbox off, the rendered manifests are byte-identical to a
chart without those rails. The data-tier ingress rail is independent of the
sandbox and renders whenever an in-chart store is deployed.

| Rail | What ships | Values |
|---|---|---|
| 1. Default-deny egress + metadata block | NetworkPolicies selecting `component: runner-sandbox`: default-deny egress, allow-DNS, an operator-declared egress allowlist, and (optional) ingress lock. Arbitrary internet AND `169.254.169.254` are denied by construction. | `security.networkPolicy.*` |
| 2. Per-agent secret isolation | Least-privilege runner ServiceAccount (no secret get/list, token not mounted). The per-agent `resourceNames`-scoped Role is bound by the control plane per agent. | `agentSandbox.runner.serviceAccount.*` |
| 3. Non-root / read-only rootfs | Pod + container securityContext on the runner: `runAsNonRoot`, uid 1000, `readOnlyRootFilesystem`, drop ALL caps, no privilege escalation, RuntimeDefault seccomp, plus writable emptyDir scratch (`/tmp`, `/home/runner`) and `HOME`. | `agentSandbox.runner.hardening.*` |
| 4. gVisor kernel isolation | `runtimeClassName` on runner pods, driven by the `security.gvisor.mode` tri-state (`auto`/`require`/`off`) + a preflight that fails the install if the RuntimeClass is missing or downgraded, firing in `require` (always) and in `auto` for real-model runs + an optional RuntimeClass object. | `security.gvisor.*`, `security.gvisorPreflight.*` |
| 5. Data-tier ingress isolation | Per deployed store (Postgres, RustFS, ClickHouse, Valkey): a default-deny-ingress NetworkPolicy plus a scoped-allow that permits ingress on the store's ports ONLY from this release's app pods (`name`+`instance` label). Blocks any co-tenant pod from opening `Postgres:5432` etc. | `security.dataTierNetworkPolicy.*` |
| 6. Tenant capacity ceiling | A `ResourceQuota` bounding aggregate cpu/memory and sandbox pod count (scoped to the sandbox PriorityClass; a scoped quota cannot constrain ephemeral-storage, so per-pod disk is bounded by the `LimitRange`/pod limits times the pod-count cap), plus a `LimitRange` supplying per-container defaults so a sandbox pod created outside this chart's own templates still inherits a ceiling. Renders whenever `agentSandbox.deploy: true`. | `resourceQuota.*`, `limitRange.*` |

**Fail-closed egress.** `security.networkPolicy.allowedEgress` is EMPTY by
default: a fresh install denies all egress except DNS until the operator declares
where the model API and MCP endpoints live (`{cidr, ports}` entries). An unset
allowlist never means allow-all.

**The controller does not get a second vote on egress (#765, ADR-0067).**
NetworkPolicy allows are additive across objects selecting the same pods -- Rail
1 above cannot narrow a separately-managed, broader policy, only be unioned with
it. Left to its own default, the vendored agent-sandbox controller reconciles
its own shared NetworkPolicy per SandboxTemplate with a built-in "allow public
internet minus RFC1918" rule, which would silently re-open exactly what Rail 1
was configured to close. Whenever `security.networkPolicy.enabled` is `true`
(the default), the runner SandboxTemplate sets
`spec.networkPolicyManagement: Unmanaged`, so the controller never creates that
policy for this template and Rail 1 is the only NetworkPolicy in effect. With
`security.networkPolicy.enabled: false` the field is left unset, so the
controller's own baseline policy still applies rather than nothing.

**Skill/tool web access.** The same allowlist also carries outbound web access a
skill or tool needs (e.g. a web-search provider). `curie cluster up --allow-web-egress
<CIDR>` (repeatable) appends one entry per CIDR on TCP 443, additive to the model
rule at index `[0]` and without weakening it; the raw helm equivalent is `--set
'security.networkPolicy.allowedEgress[1].cidr=<CIDR>'` plus
`...[1].ports[0].protocol=TCP` and `...[1].ports[0].port=443` (index `[1]`
because the model entry is `[0]`; use index `[0]` instead when installing sealed
with no model credential, so the array has no gap). This is the platform enablement the weather
example (#36) depends on -- its skill answers via a live web search, which the
sealed default denies. `--allow-web-egress 0.0.0.0/0` opens the open internet
(still minus the `169.254.169.254` metadata endpoint the chart carves out of
`0.0.0.0/0`); narrow the CIDR to a specific provider for a tighter posture. Omit
the flag and the install stays fully sealed.

**Data-tier ingress isolation.** The backing stores hold every credential and all
trace/app data, so `security.dataTierNetworkPolicy.enabled` (default `true`)
wraps each DEPLOYED in-chart store in a default-deny-ingress NetworkPolicy plus a
scoped-allow that only admits this release's own app pods (matched by
`app.kubernetes.io/name` + `app.kubernetes.io/instance`) on the store's ports.
Without it, any pod in a NetworkPolicy-enforcing cluster could open `Postgres:5432`
and exfiltrate. BYO stores (`<store>.deploy: false`) are external and get no
policy. Claim 5 of the security probe verifies it empirically (an app-labeled pod
reaches each store while a non-app-labeled pod is blocked), which also catches a
non-enforcing CNI.

**gVisor needs runsc on the node**, and `security.gvisor.mode` is a tri-state
(default `auto`):

- **`auto`** -- at install/upgrade time the chart looks up the `gvisor`
  RuntimeClass. Present -> runner pods use it. Absent -> pods run without it and
  `NOTES.txt` warns. Never blocks the install, so a bare install works on any
  cluster. (Helm's `lookup` returns empty under `helm template`/--dry-run, so a
  templated render always shows the no-gvisor shape.) This never-blocks behavior
  applies to the fake-model default only; enabling a real model
  (`fakeModel=false` or `inference.deploy`) under `auto` renders the blocking
  `preflight-gvisor` hook, so a runsc-less real-model install fails closed
  instead of silently running on the host kernel.
- **`require`** -- always stamp the RuntimeClass AND run the `preflight-gvisor`
  hook, which blocks the install with a clear remediation if the runtimeclass is
  missing or downgraded to runc. The fail-hard production posture.
- **`off`** -- never stamp a RuntimeClass; kernel isolation disabled knowingly.
  `-f charts/curie/values-e2e-nogvisor.yaml` selects this deterministically
  (skipping the lookup); every other rail stays on.

The class name and handler live on `security.gvisor.runtimeClassName` / `.handler`;
set `security.gvisor.installRuntimeClass=true` to have the chart create the
RuntimeClass object (the node must still provide the runtime).

**Tenant capacity ceiling (ADR-0059 decision 4).** ADR-0008 makes the namespace
a reachability boundary (namespace-per-tenant compute); the `ResourceQuota` and
`LimitRange` complete it with a bound on consumption, since nodes are shared
beneath the namespace and one tenant's sandboxes can otherwise exhaust node
capacity another tenant's sandboxes depend on. The `ResourceQuota` is scoped
via `scopeSelector` to the sandbox `PriorityClass` name
(`resourceQuota.sandboxPriorityClassName`, default `curie-sandbox` -- the
name ADR-0059 decision 5's `PriorityClass` is expected to define), so it binds
only sandbox pods and not the control plane or data tier that, in the N=1
self-host topology, share this same release namespace. The `LimitRange` has no
scope (Kubernetes does not support one on `LimitRange`) and so applies
namespace-wide, but ships `default`/`defaultRequest` only -- never `min`/`max`
-- so it only ever fills a resource dimension a container leaves undeclared
(today, `ephemeral-storage` everywhere in the chart) and can never reject an
already-configured control-plane pod at admission time. Both objects are
independently toggleable (`resourceQuota.enabled`, `limitRange.enabled`,
each default `true`) and every ceiling is overridable, per ADR-0059 decision 6.

**Verifying the rails.** The security-boundary probe suite re-runs as a `helm test`:

```bash
helm test <release> -n <ns>
kubectl logs -n <ns> job/<release>-security-probe            # claims 1, 2, 4, 5
kubectl logs -n <ns> <release>-security-probe-hardening      # claim 3
```

Claim 1 does a before/after egress control (reachable under a temporary allow-all
-> blocked under the chart default-deny) so a non-enforcing CNI is caught as a
false-pass rather than trusted. Claim 4 reports honestly: if the gvisor
runtimeclass is absent it is marked NOT-TESTABLE (per the security-boundary test plan, never faked), with
enforcement asserted separately by the preflight and proven live in the security-boundary test plan
(`uname` = `4.19.0-gvisor`).

## Uninstalling and CRD lifecycle

`helm uninstall <release> -n <ns>` removes everything the chart templated, but
**not** the CRDs (Custom Resource Definitions). The agent-sandbox CRDs (`sandboxes.agents.x-k8s.io` and the
related types) are vendored under `charts/curie/crds/`, which Helm installs
before any template but never upgrades or deletes (this is Helm's documented
`crds/` behavior, not a chart choice). A full teardown therefore needs a manual
step:

```bash
helm uninstall <release> -n <ns>
kubectl delete crd sandboxes.agents.x-k8s.io \
  sandboxtemplates.extensions.agents.x-k8s.io \
  sandboxwarmpools.extensions.agents.x-k8s.io \
  sandboxclaims.extensions.agents.x-k8s.io
```

Only delete the CRDs if no other release on the cluster uses the agent-sandbox
controller -- deleting a CRD deletes every custom resource of that kind
cluster-wide. Likewise, upgrading the CRDs to a newer controller release is a
manual `kubectl apply` of the new definitions; `helm upgrade` will not touch
them.

## What the agent-sandbox subchart needs to know

- **Fullname/labels:** resources are `<release>-<component>` and carry
  `app.kubernetes.io/{name,instance,component,managed-by}` plus `helm.sh/chart`.
  Reuse `curie.selectorLabels` / `curie.fullname` from `_helpers.tpl`.
- **Where to plug in:** add a `charts/curie/agent-sandbox/` (or an
  `agentSandbox.*` values block + templates) gated by `agentSandbox.deploy`,
  same condition+BYO idiom. The runner image pre-pull, `SandboxWarmPool`, and
  the control-channel Service belong there.
- **Backing services to target:** the runner queue is Valkey at
  `<release>-valkey:6379` (password in secret key `valkeyPassword`); traces go
  to the collector at `<release>-otel-collector:4317/4318`, NOT to Langfuse
  directly.
- **NetworkPolicy is enforced** on the k3s target (probe proves it), so the security rails'
  runner-egress policies will actually bite -- design the sandbox egress allow
  (model API + declared MCP endpoints) accordingly. RFC1918 vs public is a clean
  split point, as the probe's own deny policy demonstrates.
- **Resource headroom:** on the current 4 GB node the backbone leaves little
  room for bursty runner pods; the sandbox substrate's warm-pool sizing should assume the resize,
  or run against the planned `kind` fallback for pure lifecycle tests.

## Agent Sandbox substrate

`agentSandbox.deploy: true` (the default) adds the runner `SandboxTemplate`
(`<release>-runner`) and `SandboxWarmPool` (`<release>-runner-pool`) that the
worker's sandbox substrate (`curie_worker.sandbox`) claims from. Set it false
to install only the control plane + backing stores without the runner substrate.

- **CRDs** (`sandboxes.agents.x-k8s.io` + the three
  `*.extensions.agents.x-k8s.io`) are vendored from the upstream v0.5.0
  release into this chart's `crds/` directory, so Helm installs them before
  any template renders. Helm never upgrades or deletes `crds/` content:
  removing them after a teardown is a manual
  `kubectl delete crd <name>`.
- **Controller**: `agentSandbox.controller.deploy: true` installs the vendored
  upstream controller bundle (`files/agent-sandbox/controller.yaml`: namespace
  `agent-sandbox-system`, RBAC, webhook Service, and the Deployment running
  with `--extensions`). It is cluster-scoped; install it from exactly one
  release per cluster, or leave it false on clusters that already run
  agent-sandbox. **Cluster permissions (blast radius):** the controller's
  ClusterRoles grant cluster-wide `create/delete/get/list/patch/update/watch`
  on `pods`, `services`, and `persistentvolumeclaims` (it places sandbox pods
  and their Services), full control of the `sandboxes` / `sandboxclaims` /
  `sandboxtemplates` / `sandboxwarmpools` custom resources, `get/patch/update`
  on those four CRDs by name, plus `leases` (leader-election) and `events`. Its NetworkPolicy permission is
  **split by verb along the read/mutate line** (issue #350, ADR-0023): the
  vendored manifest drops the `networkpolicies` rule from its
  `agent-sandbox-controller-extensions` ClusterRole, and `templates/agent-sandbox.yaml`
  replaces it with (1) a cluster-scoped **read-only** ClusterRole/ClusterRoleBinding
  (`agent-sandbox-controller-networkpolicies-read`) granting only
  `get/list/watch` cluster-wide, and (2) a namespaced
  `Role`/`RoleBinding` (`agent-sandbox-controller-networkpolicies`) granting the
  mutating verbs `create/delete/patch/update` (plus `get`) in `.Release.Namespace`
  only. The cluster-wide read is required because the upstream controller's
  NetworkPolicy informer LISTs/WATCHes at cluster scope; without it the controller
  crash-loops before any `SandboxClaim` binds (#350). #66's guarantee still holds
  in its load-bearing form -- **no cluster-wide mutate**: a compromised controller
  (or a leaked SA token) can no longer delete the fail-closed egress NetworkPolicy
  that IS Rail 1's containment in any *other* namespace, because delete/patch are
  confined to this release's own namespace, where the controller legitimately
  manages those policies. The Deployment is also hardened
  with a non-root
  (uid 65532) / read-only-rootfs / drop-ALL-caps / RuntimeDefault-seccomp
  securityContext. On a shared multi-tenant cluster, prefer running the
  controller from a dedicated platform release (or `controller.deploy: false`
  with an externally-managed controller) given the residual cluster-wide
  pod/service/PVC reach.
- **Runner image**: the pool runs `curie-runner`, defaulting to
  `ghcr.io/curie-eng/curie-runner:latest` with `imagePullPolicy: IfNotPresent`
  (per-thread cold boots must not contain a pull; the `runner-prewarm` DaemonSet
  keeps the image fresh on every node -- see "Publishing and pulling images").
  For offline
  dev/e2e, `-f values-dev.yaml` overrides it to a locally-built, cluster-imported
  tag with `imagePullPolicy: Never` (`docker build -f runner/Dockerfile -t
  curie-runner .` from the repo root, then
  `docker save curie-runner:<tag> | ssh <node> 'sudo k3s ctr images import -'`).
  Fake-model mode (`agentSandbox.runner.fakeModel`, default true) round-trips ACI
  events with no credential.
- **Per-claim env**: the template sets `envVarsInjectionPolicy: Overrides` so
  the substrate's resume path can inject `CURIE_HISTORY_REF` /
  `CURIE_SESSION_ID` per claim. Claims carrying env bind a fresh sandbox
  rather than a pre-warmed one; the fast path (no env) binds warm.
- Traces flow to `<release>-otel-collector:4318` (HTTP), per the collector
  rule above; the env block is omitted when `otelCollector.deploy: false`.

## Deploying without inbound access

A deploy is normally triggered by a GitHub webhook — an *inbound* request. A
cluster behind a firewall or NAT cannot receive one, so on those installs
push-to-deploy does not work at all. That is the common case for self-hosted.

Outbound always works, so the API can ask instead:

```yaml
api:
  commitPollIntervalSeconds: 60     # 0 disables it (the default)
```

It asks GitHub whether `dev_branch` and `prod_branch` have moved, for every
repository an agent is bound to, and hands any new commit to the same
`process_push` the webhook calls — so the two lanes cannot disagree about what a
push means. `deploy.yaml` still decides which *agent* the push lands on.

The webhook stays the fast path wherever it reaches; polling is the floor.
Running both is safe: a poll for a push the webhook already handled is a no-op.

Polling is per *repository*, not per agent — several agents share one repository
(ADR-0091), and per-agent polling would race N deploys of the same commit.

Give the platform a credential even if every repository is public: an
unauthenticated caller gets 60 GitHub requests an hour, which a handful of
repositories on a 60s interval exhausts in minutes.

## Serving the API over TLS

The API is the only endpoint reached from outside the cluster. Two things cross
it in the clear without TLS: the `X-API-Key` header, on every authenticated
call, and the GitHub webhook delivery, whose HMAC protects integrity but not
confidentiality.

Off by default, because it cannot be defaulted honestly — an ingress needs a
controller, a hostname, and a certificate source, none of which the chart can
invent. A default-on Ingress with no controller renders an object that silently
does nothing.

```yaml
api:
  ingress:
    enabled: true
    host: curie.example.com
    className: nginx
    annotations:
      cert-manager.io/cluster-issuer: letsencrypt-prod
      # Match the webhook body cap; a smaller proxy limit rejects a large push
      # before the API can apply its own bound.
      nginx.ingress.kubernetes.io/proxy-body-size: 25m
    tls:
      enabled: true
      secretName: curie-api-tls     # empty = the controller's default cert
```

Then point the GitHub webhook at `https://curie.example.com/github/webhook`.

Set `tls.enabled: false` only when something upstream already terminates TLS —
it renders the routing rules without a `tls` block, rather than silently
keeping one.

## Secrets at rest

Curie holds real credentials in Kubernetes Secrets: the model credential, the
Slack tokens, the GitHub App private key, and every agent's connector secrets.

**Kubernetes Secrets are base64-encoded, not encrypted**, unless the cluster
encrypts them at rest. That is a property of the cluster, not of this chart, and
it is not detectable from inside one — a pod can read neither etcd nor the
apiserver's `EncryptionConfiguration`. There is therefore no preflight for it,
deliberately: a check that cannot fail correctly is worse than none, because it
reports a pass it did not earn. The install notice says so instead.

Verify it for your distribution:

| | |
|---|---|
| k3s | `k3s secrets-encrypt status` — see the note below before enabling |
| kubeadm | `--encryption-provider-config` on kube-apiserver |
| EKS | `aws eks describe-cluster --name <c> --query cluster.encryptionConfig` |
| GKE | on by default |

#### Enabling it on k3s

Order matters, and getting it wrong leaves a half-configured cluster.

**1. Start the server with the flag.** This must come *before* `enable`:

```yaml
# /etc/rancher/k3s/config.yaml
secrets-encryption: true
```

```bash
systemctl restart k3s
```

Running `enable` first, on a server started without the flag, half-succeeds: it
writes a config the server never loads, and reports `missing annotation on node`
followed by `Encryption Status: Disabled, no configuration file found`.

**2. Try k3s's own command.**

```bash
k3s secrets-encrypt enable && systemctl restart k3s && k3s secrets-encrypt reencrypt
k3s secrets-encrypt status     # expect: Encryption Status: Enabled
```

On **v1.36.2** this fails with `Put ".../v1-k3s/encrypt/config": EOF`, even with
step 1 done (issue #1243). If it succeeded, you are finished — skip step 3.

**3. If step 2 failed, write the configuration directly.**

Read the hazard below first. Generate a key and write the file k3s already
points the API server at:

```bash
head -c 32 /dev/urandom | base64          # the key; keep a copy somewhere safe

cat > /var/lib/rancher/k3s/server/cred/encryption-config.json <<'JSON'
{
  "kind": "EncryptionConfiguration",
  "apiVersion": "apiserver.config.k8s.io/v1",
  "resources": [
    {
      "resources": ["secrets"],
      "providers": [
        { "aescbc": { "keys": [ { "name": "key1", "secret": "PASTE_THE_BASE64_KEY" } ] } },
        { "identity": {} }
      ]
    }
  ]
}
JSON
chmod 600 /var/lib/rancher/k3s/server/cred/encryption-config.json
```

No restart is needed — k3s runs the API server with
`--encryption-provider-config-automatic-reload=true`, which you can confirm with
`journalctl -u k3s | grep encryption-provider-config`.

`aescbc` first means new writes are encrypted; `identity` second means Secrets
already stored in plaintext are still readable. Re-encrypt the existing ones by
rewriting them:

```bash
kubectl get secrets -A -o json | kubectl replace -f -
```

> **Hazard, before you do this.** Once anything is written under `aescbc`, that
> file is the only way to read it back. If it is lost, reverted to
> identity-only, or the key changes, **every Secret written while encryption was
> active becomes permanently undecryptable** — including the ones holding your
> Slack tokens and model credential. Back up both the file and the key, and do
> not let a config-management tool overwrite it.

If that risk is not one you want to carry on a single-node cluster, a reasonable
alternative is to leave encryption off and keep the most sensitive value out of
etcd entirely, as below.

### The GitHub App private key

This is the most sensitive value the chart handles: it can mint read tokens for
every repository the App is installed on. Two ways to supply it.

**Recommended — a Secret you manage.** The chart only references it, so the key
never passes through helm values and never lands in release history (helm
retains 10 revisions by default, and `helm get values` can print them):

```yaml
api:
  githubAppId: "1234567"
  githubAppExistingSecret: my-github-app
  githubAppExistingSecretKey: privateKey    # the default
```

Create that Secret however you like — `kubectl create secret generic`, External
Secrets Operator against AWS Secrets Manager or Vault, or Sealed Secrets. This
is the same bring-your-own idiom every backing store in this chart uses.

**Quick trial — let the chart hold it.** `curie cluster github-app --app-id …
--private-key …` puts the PEM in the chart's own Secret. Fine to prove the flow
works; move to `githubAppExistingSecret` before you rely on it.

### Rotating the key

A GitHub App can hold several private keys at once, so rotation has no downtime
and needs no coordination with any repository:

1. Generate a second private key on the App's settings page — both are now valid
2. Deploy the new one (update your Secret, or re-run `curie cluster github-app`)
3. Delete the first key on GitHub

Installations, permissions, and the App ID are untouched. This is a real
advantage over a personal access token, where rotation means re-issuing the
credential *and* re-authorizing what it could reach.

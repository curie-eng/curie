# charts/curie

The umbrella Helm chart that installs the whole Curie (Relay) stack on a
single node. It installs the backing-store stack (Langfuse + Postgres + Valkey +
ClickHouse + RustFS + OTel Collector, dev profile, BYO (bring-your-own)
toggles, the three preflights) plus the security rails as chart defaults.

The chart is a direct port of the proven `compose.dev.yaml` dev stack: same
images and the same headless-bootstrapped Langfuse dev project. The chart
ClickHouse default is `:25.12.11.4` (AVX required, coupled to
`langfuse.image.tag`) while compose stays on `:24.8.14.39` so an SSE4.2-only
developer host can still boot the local stack; both sides name a patch build
rather than a moving `25.12` / `24.8` alias (#2319). Rather than vendoring the upstream Langfuse chart and its
Bitnami subcharts, each component is a first-class template here -- this keeps
the single-node footprint controllable and avoids the Bitnami-catalog
(`bitnamilegacy/*`) instability. It still follows the Langfuse chart *idiom*:
every backing store is toggle-gated with a single-block bring-your-own surface.

## Install a released chart

Add the public chart repository, refresh its index, and confirm the available
Curie versions before installing:

```bash
helm repo add curie https://raw.githubusercontent.com/curie-eng/curie/gh-pages
helm repo update
helm search repo curie/curie --versions
helm install curie curie/curie --namespace curie --create-namespace
```

The Helm index supplies discovery and download metadata only. It does not
verify the release signature or provenance. Follow the
[release verification instructions](../../docs/release-verification.md) when
you need that stronger guarantee.

## Install from a source checkout

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

**Upgrading chart versions.** Merge the new chart defaults with the release's
retained values. Plain `--reuse-values` omits keys introduced by the new chart,
which can render a required value as blank. Helm 3.14 and newer provide the
safe merge directly:

```bash
helm upgrade curie <new-chart> -n curie --reset-then-reuse-values
```

For an auditable values file instead, capture the release's user supplied
values privately and pass that file over the new defaults:

```bash
(
  set -euo pipefail
  upgrade_values="$(mktemp)"
  trap 'rm -f "$upgrade_values"' EXIT HUP INT TERM
  chmod 600 "$upgrade_values"
  helm get values curie -n curie -o yaml > "$upgrade_values"
  test -s "$upgrade_values"
  helm upgrade curie <new-chart> -n curie -f "$upgrade_values"
)
```

Keep the file private because retained values can contain credentials. Remove
it after the upgrade even when Helm fails. The commands below use
`--reuse-values` only for same chart configuration changes, not a chart version
upgrade.

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
| `ui.apiBaseUrl` | `""` | Empty derives the in-chart API Service for the UI's `CURIE_API_TARGET` nginx upstream. A set value is used verbatim (BYO). **Required** when `api.deploy: false` and `ui.deploy` is true: the UI's probes hit nginx `/` rather than the upstream, so an empty override fails the render closed instead of shipping a Ready UI that fails every `/api/` request. |
| `dispatcher.apiPreflightTimeoutSeconds` | `120` | Bounded time for API `/health`, followed by a fresh same-size discovery-and-Slack budget. The bare dispatcher default remains 30 seconds. |
| `dispatcher.startupProbe.initialDelaySeconds` | `0` | Delay before Kubernetes begins the dispatcher heartbeat startup probe. |
| `dispatcher.startupProbe.periodSeconds` | `10` | Interval between dispatcher startup probes. |
| `dispatcher.startupProbe.timeoutSeconds` | `5` | Timeout for each dispatcher startup probe. |
| `dispatcher.startupProbe.failureThreshold` | `27` | Consecutive failures allowed. With the other defaults, the earliest failure cutoff is 260 seconds. |

The dispatcher starts its heartbeat only after every boot preflight succeeds.
Until then, the startup probe gates readiness and liveness so Kubernetes does
not restart the pod during normal API warmup or the following Slack checks.
With the defaults, API health and discovery/Slack each have a 120-second
budget, a final started Slack call can use at most two more seconds, and the
startup probe cannot fail the pod before 260 seconds. Helm rendering rejects a
cutoff that does not strictly outlast that full application startup envelope
and names the values to adjust. This keeps delayed API readiness restart free
while preserving a bounded failure: if the API never becomes ready, the
dispatcher exits with guidance to check `CURIE_API_URL` and whether the API pod
is Ready before the startup probe budget expires.

Two limits worth knowing. With `api.deploy: false` and an empty
`dispatcher.apiBaseUrl` the dispatcher is pointed at a Service that does not
exist; its boot preflight fails and the pod CrashLoopBackOffs naming the
unreachable URL. That is intentional (a loud boot failure beats a silent
dead-end at click time) and the fix is to set `dispatcher.apiBaseUrl`. And
because the API's `/health` is unauthenticated, that first preflight proves only
reachability. The following authenticated `/agents` discovery refuses startup
when a BYO API expects a different key. Match `apiKey` to the API you point at.

`api.deploy: false` also moves the runner sandbox's memory, history and state
calls onto that external host, and the sandbox is under Rail 1's default-deny
egress. The in-chart `runner-allow-api` policy selects this release's API pods,
so it does not render, and NetworkPolicy has no hostname peer to derive from
`dispatcher.apiBaseUrl`. Set `api.egress` to the endpoint's CIDRs (`{cidr,
ports}` entries, same shape as the allowlist); the chart requires it at render
whenever `dispatcher.apiBaseUrl` names an external API, because the failure it
prevents is silent — agents boot with no
prior memory and no thread transcript, `remember` writes never persist, and the
only symptom is a warning line inside the sandbox.
`mailAdapter.apiEgress.httpsCidrs` above is the same idea for the mail adapter
pod: one BYO peer, declared explicitly, per pod that has an egress policy.

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

Reach the Langfuse UI after a stock install:

```bash
curie cluster observability
```

The command reports the Langfuse UI URL and a connection hint when the Service
is not externally reachable. Pass `--open` to open a reachable Langfuse URL in
your browser. It reports access surfaces only and does not read or rotate
Langfuse credentials.

For a sealed install, Helm's post install notes show how to retrieve the
generated Langfuse admin password. Sign in as `dev@curie.local` unless
`langfuse.init.userEmail` was overridden.

## Collector operation and backends

API, dispatcher, worker, and runner emit OTLP traces, correlated logs, and
low-cardinality metrics to the **collector**, never directly to Langfuse or a
log/metrics backend. The in-chart collector receives gRPC on
`curie-otel-collector:4317` and HTTP on `:4318`. Applications use standard
`OTEL_EXPORTER_OTLP_*` settings. The chart owns one destination for every
instrumented workload:

- `otelCollector.deploy: true` (default) wires the in-cluster collector.
- `otelCollector.deploy: false` plus `otelCollector.endpoint` wires an
  external collector, with optional `protocol`, `headers`, or
  `headersExistingSecret`.
- `otelCollector.telemetryDisabled: true` is the explicit no-OTLP
  acknowledgement. `security.checkDefaultCredentials` refuses a production
  render that has neither a chart collector nor an external endpoint unless
  this flag is set.
- Outside that production gate, `deploy: false` with an empty endpoint remains
  the supported local/offline no-endpoint mode: SDK export stays inert while
  stderr diagnostics remain available.

`api.extraEnv`, `dispatcher.extraEnv`, `worker.extraEnv`, and
`agentSandbox.runner.extraEnv` remain per-workload overrides, including
`valueFrom`. They do not satisfy the production availability gate, because the
four services can drift.

```yaml
otelCollector:
  deploy: false
  endpoint: https://otel.example.com:4318
  protocol: http/protobuf
  headersExistingSecret: acme-otel-auth
  headersSecretKey: headers
```

Langfuse ingest is HTTP-only, so the collector forwards traces to Langfuse over
HTTP. Logs and metrics retain explicit collector pipelines. Their destinations
are operator-configured collector exporters; installing Grafana, Loki, Tempo,
Prometheus, or another retained backend is deliberately separate from this
chart's OTLP write path.

The chart-managed collector is a bounded gateway, not a lossless store. Every
network exporter uses retry plus a bounded sending queue. `memory_limiter` runs
before `batch`, and the default persistent `file_storage` queue uses a 1Gi PVC
at `/var/lib/otelcol/storage`; that volume and the queue limit bound outage
storage. Queue overflow and retry expiry are explicit loss, exposed through
collector self-metrics rather than hidden by an unbounded backlog. The default
60-second termination grace period gives the collector a finite drain window;
it does not promise a drain that cannot complete.

Collector internal metrics are exposed on port 8888 and include receiver
accepted/refused, exporter sent/send-failed/enqueue-failed, queue
size/capacity, process resource use, and uptime. They let an operator
distinguish no application traffic from a telemetry delivery problem. Process
health remains the health endpoint; data-flow health is these self-metrics and
stderr exporter diagnostics.

The Collector self-metrics port is ingress-isolated by the default-on
`security.otelCollectorNetworkPolicy`. The policy selects only this release's
Collector pods, leaves OTLP gRPC/HTTP ingestion unrestricted on 4317/4318,
admits the kubelet health probe on 13133, and does not admit port 8888 until
`metricsIngress` names an explicit standard
Kubernetes `NetworkPolicyPeer`. Empty peers, empty selectors, and catch-all
`0.0.0.0/0` or `::/0` IP blocks are rejected at render time. For example:

```yaml
security:
  otelCollectorNetworkPolicy:
    metricsIngress:
      - namespaceSelector:
          matchLabels:
            kubernetes.io/metadata.name: observability
        podSelector:
          matchLabels:
            app.kubernetes.io/name: prometheus
```

`otelCollector.service.metricsPort` changes the Service, container, and policy
port together. Set `security.otelCollectorNetworkPolicy.enabled: false` only
when an external control plane supplies equivalent isolation; setting
`otelCollector.deploy: false` omits the policy along with the chart-owned
Collector.

The production chart omits the `debug` exporter to avoid copying telemetry to
collector stdout. `values-dev.yaml` explicitly enables it and selects an
ephemeral queue for disposable development. Use the same explicit persistence
override for any short-lived test installation; production installs retain the
PVC by default.

Additional trace destinations are configured through
`otelCollector.extraExporters`, a map of exporter names to collector exporter
configuration, and `otelCollector.extraPipelineExporters`, an ordered list of
those names. The map is rendered under `exporters` and the list is appended to
the traces pipeline. `extraLogPipelineExporters` and
`extraMetricPipelineExporters` do the equivalent for the respective signal
pipelines. Helm rejects a missing exporter or a network exporter without its
bounded retry and persistent queue protections. The development-only `debug`
exporter is available only when enabled.

Built-in exporter names (`otlphttp/langfuse`, `nop/logs`, `nop/metrics`, and
`debug`) are reserved and cannot be overridden through `extraExporters`.
Sensitive header names such as `Authorization`, tokens, API keys, secrets,
passwords, and credentials must use exact Collector environment expansion
(`${env:NAME}`), with that variable supplied through `otelCollector.extraEnv`
and a Kubernetes Secret reference. This prevents a literal backend credential
from being rendered into the Collector ConfigMap.

Exporter credentials belong in a Kubernetes Secret, not in
`extraExporters`: that map becomes `collector-config.yaml` in a ConfigMap.
`otelCollector.extraEnv` accepts complete `EnvVar` entries so the exporter can
use Collector environment expansion while the credential itself is resolved
only on the Pod. This example adds an authenticated OTLP/HTTP destination with
the required bounded retry and persistent queue:

```yaml
otelCollector:
  extraEnv:
    - name: BACKEND_AUTH
      valueFrom:
        secretKeyRef:
          name: acme-otel-backend
          key: authorization
  extraExporters:
    otlphttp/acme-secure-sink:
      endpoint: https://otel-backend.example.com:4318
      headers:
        Authorization: ${env:BACKEND_AUTH}
      retry_on_failure:
        enabled: true
        max_interval: 5s
        max_elapsed_time: 5m
      sending_queue:
        enabled: true
        storage: file_storage
        queue_size: 1000
  extraPipelineExporters: [otlphttp/acme-secure-sink]
```

The referenced Secret is not copied into a ConfigMap or tracked values file.
Changes to either value roll the collector Deployment, so the configured
pipeline and environment are applied on `helm upgrade`.

## Components

| Component | Image | Notes |
|---|---|---|
| Langfuse web + worker | `langfuse/langfuse:3.225.5`, `langfuse/langfuse-worker:3.225.5` | Observability + eval backbone. Headless-bootstrapped dev org/project. Web and worker stay on one reviewed migration set; do not replace the version with floating `:3`. Both Deployments use Recreate, so a Langfuse image change is a brief outage while boot migrations run. |
| Postgres | `postgres:16.15-alpine@sha256:cf78e766...` | Langfuse transactional store + app state. StatefulSet. Digest-pinned: PostgreSQL's version is two components, and `postgres.podSecurityContext.fsGroup` (70) is the `postgres` gid read out of those exact bytes. |
| Valkey | `valkey/valkey:8.1.10-alpine` | Langfuse cache/queue + dispatcher Streams queue. Same pin as `compose.dev.yaml` and the CI rust job's Valkey service. |
| ClickHouse | `clickhouse/clickhouse-server:25.12.11.4` | Langfuse OLAP store. Coupled to `langfuse.image.tag` 3.225.5; chart default requires AVX (see preflight). `compose.dev.yaml` deliberately stays on the SSE4.2-safe `24.8.14.39`. |
| RustFS | `rustfs/rustfs:1.0.0-beta.12` plus `amazon/aws-cli:2.32.6` init | Langfuse object storage; BYO real S3 in prod. |
| OTel Collector | `otel/opentelemetry-collector-contrib:0.119.0` | Bounded OTLP gateway (gRPC+HTTP), durable queue by default; traces -> Langfuse over HTTP, logs/metrics -> configured exporters. |
| Mail adapter | `ghcr.io/curie-eng/curie-mail-adapter` | Off by default. One `Recreate` replica with durable SQLite on single-writer storage; no platform key/database credential or ServiceAccount token. The only first-party workload with its own egress NetworkPolicy, so its OTLP export needs an explicit peer (see below). |

The mail adapter's `mailAdapter.persistence` block renders a 1 GiB RWO PVC by
default or mounts a named same-namespace single-writer Filesystem `existingClaim`
with exactly one `ReadWriteOnce` or `ReadWriteOncePod` access mode. Its
root filesystem remains read-only; only the state mount and an `emptyDir` at
`/tmp` are writable. Enabling it also requires an explicit
`mailAdapter.agentmail.httpsCidrs` list. One egress-only NetworkPolicy then
allows DNS, this release's API pods, those provider/proxy CIDRs on TCP 443, and
-- while `otelCollector.deploy=true` -- this release's OTel Collector on its
gRPC and HTTP ports, so the adapter's OTLP export is not dropped by its own
rail. When `api.deploy=false`, the in-chart API selector is replaced by the
required `mailAdapter.apiEgress.httpsCidrs` peers on
`mailAdapter.apiEgress.port`; the chart does not infer IPs from `apiBaseUrl`.
The policy has no Kubernetes API carve-out and never selects runner sandboxes.

The adapter is the only first-party workload with an egress-restricting
NetworkPolicy, which makes one telemetry configuration asymmetric. With
`otelCollector.deploy=false` and an external `otelCollector.endpoint`, api,
dispatcher and worker export normally because nothing restricts their egress,
while the adapter's exports are dropped: its policy has no peer for an address
the chart cannot know, and the chart deliberately invents no broad allow for
one. Because NetworkPolicies union rather than intersect, the fix needs no chart
change -- apply an additional egress policy in the release namespace selecting
the adapter's labels (`app.kubernetes.io/component: mail-adapter` plus the
release's instance label) with a `to:` for the external collector. Everything
else about the rail, including the AgentMail CIDRs, keeps working unchanged. See
[`docs/operations.md`](../../docs/operations.md#connecting-email) for the
mode-0600 credential workflow, retention, erase, and recovery procedure.

## Publishing and pulling images

First-party service images are published to GHCR by the `Release images`
workflow (`.github/workflows/release.yaml`) on every push to `main`, as
`ghcr.io/curie-eng/curie-<service>` tagged with the commit SHA and `latest`.
All six first-party services build in the matrix: `curie-api`,
`curie-dispatcher`, `curie-mail-adapter`, `curie-worker`, `curie-ui`, and
`curie-runner`. The chart defaults every first-party image to the chart
`appVersion` (empty `tag` in values, resolved by the `curie.image` helper), so
the bare install (above) pulls `ghcr.io/curie-eng/curie-*:<appVersion>` from
GHCR with no image overrides -- except `curie-mail-adapter`, whose Deployment is
off by default (`mailAdapter.deploy`), so its image is published and defaulted
but not pulled until an operator enables the email channel. The workflow still
publishes a mutable `latest` tag alongside the SHA and version tags; that is a
fact about the release artifacts, not about what this chart resolves to.

- **Pull policy for the five Deployment-managed services** (api, dispatcher,
  mail-adapter, worker, ui): `imagePullPolicy: Always`. The default tag is the
  chart `appVersion`, so this is a cheap no-op on a node that already has that
  immutable ref, and it still self-heals a node that cached a same-named ref
  under a different digest.
- **The runner image is the exception:** it uses `imagePullPolicy: IfNotPresent`
  because a sandbox pod is cold-created per Slack thread, and an `Always`
  (re-)pull inside that boot window blew past the worker's claim timeout and
  killed runs. Its presence on the node comes from the `runner-prewarm` DaemonSet
  (`agentSandbox.runner.prewarm`, default on with the sandbox substrate),
  which pulls the runner image `Always` and keeps that same `appVersion` ref
  pinned on every node; a Release-revision annotation rolls those pods on every
  `helm upgrade` so an upgraded chart's new `appVersion` is pulled before the
  next claim.
- **Override `tag` (or set `digest`)** to pin an explicit version. Empty `tag`
  is the default and resolves to `.Chart.AppVersion`; `digest` wins over `tag`
  when set.

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
(Langfuse env, the collector config, application OTEL env) repoint at the BYO
fields on the same block (`host`/`port` for stores, `otelCollector.endpoint`
for an external collector).

BYO ClickHouse picks its URL scheme the same way: `clickhouse.scheme` governs
`CLICKHOUSE_URL` for both Langfuse deployments and their readiness gate, derived
as `https` when `clickhouse.deploy: false` and `httpPort` is 8443 and `http`
otherwise, with an explicit `http`/`https` winning. `https` additionally enables
TLS for the Langfuse migration connection on `nativePort`. A TLS ClickHouse
needs both ports set -- `httpPort: 8443` and `nativePort: 9440` (the migration
DSN uses `nativePort` and only the flag changes, not the port) -- or an
explicit `scheme: https` plus both ports.

A BYO Valkey that only accepts TLS -- in-transit-encrypted ElastiCache, Azure
Cache for Redis, Redis Cloud, Upstash -- also needs `valkey.tls: true` alongside
`valkey.deploy: false` and `valkey.host`. It reaches every consumer of that
store at once: the api, worker and dispatcher, both `worker-upgrade-drain` hook
Jobs, and both Langfuse Deployments. It requires `valkey.deploy: false` --
`valkey.tls: true` against the in-chart Valkey fails the render, because that
StatefulSet serves no TLS listener. Verification uses the system CA bundle, so a
store fronted by a private CA (or one requiring mutual TLS) is not supported by
this knob; that needs CA material distributed to all seven containers, which is
a separate decision.

BYO Langfuse requires a bare external hostname in `langfuse.host`. Consumers
compose its URL as
`<scheme>://<langfuse.host>:<langfuse.web.service.port>`, where the scheme is
`langfuse.scheme` when set (`http` or `https` only) and otherwise derived --
`https` when the port is 443, `http` otherwise. Set `langfuse.scheme: https`
for a TLS endpoint on any other port. Do not include a scheme, port, or path in
`langfuse.host`. With `langfuse.deploy: false`, the chart omits
the Langfuse Service, web and worker Deployments, and model-pricing Job. Helm
rendering fails with an error naming `langfuse.host` when that value is missing
or empty, instead of emitting the hostname of a Service the chart did not
create.

```yaml
langfuse:
  deploy: false
  host: langfuse.example.com
  existingSecret: acme-langfuse-credentials
  init:
    projectPublicKey: pk-lf-acme-example
  web:
    service:
      port: 3000
```

Set `langfuse.init.projectPublicKey` to the external project's public key. Its
secret key must match: supply it as `langfuse.init.projectSecretKey`, or under
`langfuseInitProjectSecretKey` in `langfuse.existingSecret`; otherwise the
sealed chart-generated key will not authenticate to the external project. When
the chart collector is enabled, that existing Secret must also supply its
complete `otlpAuthHeader` value. As an alternative for collector
authentication, set `otelCollector.otlpAuthHeader` explicitly; see the
credential details below.

The URL scheme is selected by `langfuse.scheme`: derived as `https` when the
port is 443 and `http` otherwise, with an explicit value winning. On the
cleartext path (`http`), the Basic header warning still applies -- deploy a
cleartext BYO Langfuse only across a trusted private transport or behind a
proxy that provides TLS: an on-path observer can recover the full project
credential because the Basic header is encoded, not encrypted.

### Langfuse Postgres startup readiness

`langfuse.web.postgresReadiness` is enabled by default. Before Langfuse web
starts, its `wait-for-postgres` init container makes a credential-free
`pg_isready` protocol-readiness check only; it does not validate authentication
or run migrations. Its `image` is empty by default and falls back to
`postgres.image`; override it for a BYO Postgres/private registry when the
default image is unavailable there.

Defaults are `attempts: 60`, `intervalSeconds: 2`, and
`probeTimeoutSeconds: 2`: the wait is bounded to approximately 2--4 minutes,
depending on how quickly each probe fails. While the Pod remains in `Init`, use
`kubectl logs <langfuse-web-pod> -c wait-for-postgres` and check the Postgres
endpoint, DNS, and network path. Exhaustion fails the init container so
Kubernetes restarts it. After a successful protocol check, Langfuse web owns
authentication and migrations: bad credentials and permanent migration failures
are terminal there and are not retried by this gate.

Secrets: all credentials are written to one `<release>-secrets` Secret. A sealed
`helm install` (the default) generates strong random values for all thirteen
chart owned credentials: the backing store passwords, Langfuse
salt/encryptionKey/nextauthSecret, the two Langfuse init credentials, the
api/webhook keys, `worker.internalWorkerToken`, and
`api.approvalChatAttesterSecret`. Set `security.allowDevDefaults: true`
(values-dev.yaml, i.e. `curie cluster up --dev`) to keep the deterministic
published defaults for dev/CI.

The eleven non init credentials persist through `lookup`, so `helm upgrade`
re-uses them. Their explicit `--set` overrides and per store `existingSecret`
values take precedence for rotation or recovery. The Langfuse init project
secret and user password are first boot inputs. A fresh install generates them,
but changing them during an upgrade does not rotate records already initialized
in Langfuse. Existing releases need Langfuse side rotation and coordinated
consumer Secret updates. This chart does not perform that migration. Point
`langfuse.existingSecret` (and each store's `existingSecret`) at your own
Secrets to bring your own. `langfuse.encryptionKey` must be 64 hex chars
(`openssl rand -hex 32`).

With chart-managed init credentials and no OTel header override, the collector
derives its header from the resolved Langfuse project secret key, so a sealed
install ships the generated credential rather than a published default.

Upgrade note for `langfuse.existingSecret`: that Secret must now also carry an
`otlpAuthHeader` key holding the full header value the OTel Collector sends to
Langfuse, `Basic <base64(publicKey:secretKey)>`. The collector follows
`langfuse.existingSecret` like every other Langfuse consumer instead of reading a
chart-derived header it could not authenticate with, so an install already on
this path must add that key (or set `otelCollector.otlpAuthHeader`) before
upgrading. Without it the collector pod fails to start with
`CreateContainerConfigError`, which is the deliberate replacement for a silent
401 on every trace export.

Upgrade note for `langfuse.existingSecret` (#2327): the chart now actually reads
`langfuseSalt` and `langfuseEncryptionKey` from that Secret. `values.yaml`
already listed both as required keys, but the template ignored them -- an
install already on this path was salting and encrypting with the chart-managed
Secret's values instead, because those two keys ignored `existingSecret` while
`NEXTAUTH_SECRET` and the `LANGFUSE_INIT_*` keys already honored it. Both
Deployments now read the BYO Secret for real: a missing key fails loud with
`CreateContainerConfigError`, a differing one fails silently and stops
decrypting columns already written. Before upgrading, copy the live values out
of the chart-managed `<release>-secrets` Secret into your BYO Secret, unless no
encrypted data is worth preserving.

Caveat: generation relies on Helm `lookup`, which is empty under client-side
rendering. Driving this chart via `helm template | kubectl apply` or ArgoCD's
client-side Helm (no live API lookup) regenerates these values on every sync and
would rotate live store credentials -- pin them via `--set`/`existingSecret` (or
use the `helm install/upgrade` path / `curie cluster up`) in that case.

Guard against supplied development values on a shared/production cluster with
`--set security.checkDefaultCredentials=true`: the chart then refuses to render
while `langfuse.init.projectSecretKey` or `langfuse.init.userPassword` still
carries its published dev default (a Langfuse admin-takeover risk on a reachable
UI; the project key also feeds the OTel Collector auth header on the
non-`existingSecret` path). Those two checks compare the chart inputs before
fresh-install credential generation, so they do not inspect the generated Secret.
Override those values, or point `langfuse.existingSecret` at a Secret this chart
does not manage, to clear them. Naming the chart's own Secret does not clear
them, because the chart still fills those keys from those very values. A third
condition fails the render whenever the header the collector would actually send
is the published dev header `Basic cGstbGYtY3VyaWUtZGV2OnNrLWxmLWN1cmllLWRldg==`,
whether `otelCollector.otlpAuthHeader` is set to it directly or the chart
composed it from the `langfuse.init` keys; `langfuse.existingSecret` does not
clear it, because that header is what the collector sends regardless of where the
Langfuse credential comes from. The same flag also refuses a render that disables
the chart collector without setting `otelCollector.endpoint` or
`otelCollector.telemetryDisabled=true`, so a production install cannot silently
drop every workload's OTLP destination. It is off by default so the zero-secret
bare install stays green.

### Key-free object store auth

Pointing the bundle store at a real cloud object store (`rustfs.deploy: false`)
normally means a scoped IAM user and long-lived keys in a Secret. That works and
is genuinely least-privilege, but it is not the only option: clearing
`rustfs.auth.accessKey` selects a key-free path (#1325) in which the chart omits
every S3 credential from the API, the worker, the sandbox bundle-fetch init
container, and Langfuse, so the AWS SDK falls through its provider chain to
the **web-identity provider** (`AWS_ROLE_ARN` + `AWS_WEB_IDENTITY_TOKEN_FILE`)
fed by a projected ServiceAccount token.

Bind the role through the ServiceAccount annotations. On EKS with IRSA the
pod-identity webhook reads the annotation and injects the projected token and
both env vars:

```yaml
rustfs:
  deploy: false
  host: s3.us-east-1.amazonaws.com
  port: 443
  region: us-east-1
  bucket: langfuse           # Langfuse event and media uploads
  auth:
    accessKey: ""            # selects the key-free path
  # Rail 1 is fail-closed. The in-chart runner-allow-rustfs policy selects the
  # in-cluster rustfs pod and does not render here. NetworkPolicy cannot
  # target rustfs.host (a DNS name), so these CIDRs are required at render.
  # On EKS the tight form is VPC interface endpoints for s3 and sts with
  # private DNS enabled, which turns each requirement into a /32 rather than
  # an AWS public service CIDR range.
  egress:
    - cidr: 192.0.2.10/32    # S3 VPC interface endpoint
      ports: [{ protocol: TCP, port: 443 }]
  stsEgress:
    - cidr: 192.0.2.11/32    # STS VPC interface endpoint
      ports: [{ protocol: TCP, port: 443 }]
api:
  serviceAccount:
    annotations:
      eks.amazonaws.com/role-arn: arn:aws:iam::000000000000:role/curie-api
worker:
  workspace:
    bucket: curie-workspaces # repository workspace archives
  serviceAccount:
    annotations:
      eks.amazonaws.com/role-arn: arn:aws:iam::000000000000:role/curie-worker
agentSandbox:
  runner:
    bundleFetch:
      bucket: curie-bundles  # plugin bundles (API writes, runner reads)
    serviceAccount:
      annotations:
        eks.amazonaws.com/role-arn: arn:aws:iam::000000000000:role/curie-runner
langfuse:
  serviceAccount:
    annotations:
      eks.amazonaws.com/role-arn: arn:aws:iam::000000000000:role/curie-langfuse
```

The chart talks to **three buckets** on that same endpoint, not one. Create them
before install; the in-chart RustFS Job creates them only while `rustfs.deploy`
is true. The defaults are three distinct names, so a single-bucket BYO install
fails on two of the three paths.

- `rustfs.bucket` (default `langfuse`): Langfuse event and media uploads.
- `worker.workspace.bucket` (default `curie-workspaces`): repository workspace archives.
- `agentSandbox.runner.bundleFetch.bucket` (default `curie-bundles`): plugin bundles.

The bundle fetch uses path style addressing, so it appends
`agentSandbox.runner.bundleFetch.bucket` to this endpoint. For another AWS
region, change the region in `rustfs.host` and set `rustfs.region` to control
the SigV4 signing region used by the bundle fetch init container and by
Langfuse's event/media uploads. This setting does not configure the API or
worker S3 clients.

**Runner egress is a separate required list.** Rail 1 default-denies the
sandbox, and the in-chart `runner-allow-rustfs` policy is a pod selector on
this release's rustfs component, so it does not render when
`rustfs.deploy` is false. Putting S3 in `security.networkPolicy.allowedEgress`
alongside the model API used to be the only workaround, and a model-only
allowlist then installed green while every sandbox hung in `Init` on the first
turn. The chart now refuses that shape at render: `rustfs.egress` must cover
the object-store endpoint, and on this key-free path `rustfs.stsEgress` must
cover STS (`AssumeRoleWithWebIdentity`). Both are `{cidr, ports}` entries like
`allowedEgress`. Do not put a DNS name here; NetworkPolicy has no hostname
peer. A default route or a CIDR that reaches `169.254.169.254` is refused:
these lists are store endpoints, not a second model allowlist.

On EKS, create VPC **interface** endpoints for `s3` and `sts` in the cluster
VPC with private DNS enabled, then put each endpoint's ENI address in as a
`/32` on TCP 443. That keeps the fail-closed posture meaningful instead of
opening an AWS public service CIDR range. Gateway endpoints for S3 do not
give the sandbox a unicast address to allow. Static-key BYO still needs
`rustfs.egress` (the fetch talks to S3) and does not need `rustfs.stsEgress`.
Opting out of Rail 1 (`security.networkPolicy.enabled: false`) skips both
requirements because there is then no runner NetworkPolicy to satisfy.

Scope each role to the bucket it actually uses:

- **API role:** read/write on the bundle bucket. The API writes each uploaded bundle.
- **Worker role:** read/write on the workspace bucket, and read on the bundle
  bucket (the eval lane fetches the same objects the API wrote).
- **Runner role:** read-only on the bundle bucket. NetworkPolicy selects pods
  rather than containers, so any identity the bundle-fetch init container can
  assume is equally reachable by the runner beside it, and the runner is
  prompt-injectable by design.
- **Langfuse:** `rustfs.bucket` for the `events/` and `media/` prefixes. Langfuse
  still consumes `rustfs.auth` static keys for that bucket; the key-free path
  only omits credentials from the API, the worker, and the sandbox bundle-fetch
  init container. Scope those keys (or a Langfuse-specific IAM user) to
  `rustfs.bucket`.

Two constraints are worth stating plainly.

**Clearing the key with `rustfs.deploy: true` is refused at render.** The
in-chart RustFS is configured with those same static credentials and has no
web-identity path, so the combination would install green and then fail every
bundle read and write. The chart fails with a message naming both options
instead.

**The instance role is deliberately unavailable, and this is not an oversight
to work around.** The instinct on AWS is to drop the keys and let the node's
IAM role answer via the metadata endpoint. Rail 1 denies `169.254.169.254` by
construction, and `security-networkpolicy.yaml` computes an `except` so that a
broad operator `allowedEgress` CIDR cannot re-permit it. Opening it would hand
the node's IAM role to a prompt-injectable agent, which is strictly worse than a
bucket-scoped IAM user. Web identity reads a **mounted token** rather than a
network endpoint, so it needs no metadata access and leaves Rail 1 intact --
which is exactly why it is the key-free path this chart supports.

Off EKS, there is no pod-identity webhook, so a self-managed cluster (k3s
included) needs an OIDC provider wired to IAM and the projected token volume
supplied by the operator before the key-free path resolves to anything. Until
that is in place, static keys in a Secret remain the supported choice, and they
are the safer of the two available options rather than a limitation to route
around.

## The three preflights

The chart ships three default-on preflights under `preflights.*`, plus a
conditional gVisor RuntimeClass preflight under `security.gvisor` (described
under the security rails; it is not in this block and does not run on the
fake-model default). (a) is a blocking `pre-install,pre-upgrade` hook. (b) is
a `helm test` that must be run explicitly; it never runs during
`helm install`. (c) is a blocking `post-install,post-upgrade` hook. A green
`helm install` does not prove NetworkPolicy is enforced, so run
`helm test <release> -n <ns>` before treating the security rails as live.
All three are re-runnable via that same `helm test` command.

**(a) CPU-AVX / ClickHouse-pin check** (`preflights.avxCheck`). A pre-install /
pre-upgrade hook Job.

- ClickHouse >= 25.x is compiled for AVX and SIGILLs with exit 132 on
  SSE4.2-only CPUs -- a crash-looping pod is a confusing way to learn that.
- Chart defaults require AVX: `clickhouse.image.tag` is `25.12.11.4` because
  `langfuse.image.tag` 3.225.5's migration set from 39 onward cannot apply on
  24.8, and the 25.12 line is not in `clickhouse.sse42SafeTags`.
- The Job reads the node's `/proc/cpuinfo`; if the node lacks AVX it FAILS
  the install unless the operator pins a tag in `clickhouse.sse42SafeTags`
  (`24.8`, `24.3`, `23.8`). That override cannot apply the current Langfuse
  migration set. Skipped when `clickhouse.deploy: false`.
- Test knob `preflights.avxCheck.forceNoAvx: true` exercises the AVX-required
  failure branch on an AVX-capable node when the default tag is set.
- Read the verdict: `kubectl logs -n <ns> job/<release>-preflight-avx`.

**(b) NetworkPolicy-enforcement probe** (`preflights.networkPolicyProbe`). A
`helm test` Job. A CNI that silently ignores NetworkPolicy is a security
false-pass: the security rails' isolation policies would render but enforce nothing. The probe
does a before/after egress check -- reach an external target with no policy
(expect reachable), apply a default-deny-egress policy to itself (RFC1918
private ranges stay allowed so the control path survives; the public target is
denied), retry (expect blocked). It reports `enforcement=true` only if the after
egress is actually blocked, and `enforcement=false` (fails loudly) otherwise.

**(c) Controller-ready gate** (`preflights.controllerReady`). A post-install /
post-upgrade hook Job, also re-runnable via `helm test`. Default `enabled:
true`, gated also on `agentSandbox.controller.deploy` (also default true).

- The vendored agent-sandbox controller runs a cluster-scope NetworkPolicy
  informer. If its RBAC cannot satisfy that cluster LIST, the manager
  crash-loops and no SandboxClaim ever binds. The Deployment has no
  readiness probe, so `rollout status` can pass while the manager still
  blocks on cache sync. The load-bearing signal is the "Starting workers"
  log line.
- The Job fails the Helm operation unless the controller becomes Available
  and logs "Starting workers" within
  `preflights.controllerReady.timeoutSeconds` (default 180).
- An upgrade over a crash-looping controller may need a manual pod delete
  plus `helm test`, because the hook waits for a healthy controller that
  never arrives.
- Skipped when `agentSandbox.controller.deploy: false` (BYO controller) or
  `preflights.controllerReady.enabled: false`.
- Read the verdict: `kubectl logs -n <ns> job/<release>-preflight-controller`.

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
allowlist never means allow-all. The BYO in-chart peers are not on that list
either, because each names one endpoint rather than a class of destinations: set
`rustfs.egress` (and `rustfs.stsEgress` on the key-free path) so the sandbox
bundle-fetch can reach S3 and STS, `otelCollector.egress` when
`otelCollector.deploy: false` points the runner at an external collector, and
`api.egress` when `api.deploy: false` points it at an external API. Each is
required at render on its BYO path, so the install fails loudly instead of
shipping a sandbox holding an address it can never reach. See **Key-free object
store auth** above.

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
via `scopeSelector` to the sandbox `PriorityClass` name. That name derives
from `priorityClasses.sandbox.name` when
`resourceQuota.sandboxPriorityClassName` is empty (the default -- empty
means derive). Set the override only for a PriorityClass managed outside
this chart whose name differs from `priorityClasses.sandbox.name`; pinning
it is what lets a later rename leave the quota scoped to a class nothing
carries. The quota then binds only sandbox pods and not the control plane
or data tier that, in the N=1 self-host topology, share this same release
namespace. The `LimitRange` has no scope (Kubernetes does not support one
on `LimitRange`) and so applies
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
  `ghcr.io/curie-eng/curie-runner:<appVersion>` with `imagePullPolicy: IfNotPresent`
  (per-thread cold boots must not contain a pull; the `runner-prewarm` DaemonSet
  keeps the image on every node -- see "Publishing and pulling images").
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
- Traces flow to `<release>-otel-collector:4318` (HTTP) when the chart collector
  is deployed, or to `otelCollector.endpoint` when that BYO field is set. The
  env block is omitted only for explicit `telemetryDisabled` or local/offline
  no-endpoint mode. The runner is under Rail 1, so on the BYO path the endpoint
  alone gets it nowhere: `otelCollector.egress` must name the collector's CIDRs
  or the default-deny drops every sandbox span while api, dispatcher and worker
  keep exporting normally. The chart requires it at render rather than letting
  that asymmetry ship silently.

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

This is the most sensitive value the chart handles: it can mint tokens carrying
the App's configured permissions for every repository where it is installed.
Two ways to supply it.

Managed repository workspaces need App **Contents: Read** permission. Approval-
gated publication additionally needs **Contents: Read and write** and **Pull
requests: Read and write**. Curie prefers a repository-scoped installation token
when the App is configured and falls back to `api.githubToken`; neither
credential is mounted into a sandbox. Publication Jobs run in the dedicated
namespace named by `worker.publication.namespace` (release-scoped when empty).
For a private runner image, create the referenced image pull Secret in that
namespace separately; the chart deliberately does not copy the platform Secret.

`curie ... deploy --workspace` and `--no-workspace` are deprecated compatibility
options and no longer enable or disable coding. Every session exposes the Claude
Code file tools and `mcp__curie__publish_changes`. The publication tool is only
an approval request: it cannot publish without a managed workspace and a human
approval, publication still runs outside the sandbox, and no GitHub credential
is mounted into the sandbox.

One allowed root `https://github.com/owner/repository` URL in the initial
message establishes the thread's selection and causes the worker to acquire its
managed workspace at claim time. An initial message without a repository URL
uses a generic sandbox and does not redeem a repository credential. If a root
URL arrives after that generic route is already running, Curie may acquire the
workspace only at an authenticated idle boundary where no turn can accept a
steer, the latest structured history is durable, and no approval suspension or
unresolved side-effect boundary is active. It prepares and verifies the
workspace, cold-claims a replacement with the same logical session and history
reference, and only then atomically fences the old route. If that safe
boundary cannot be verified, Curie refuses the handoff and keeps the old route
authoritative, as required by Accepted
[ADR 0136](../../docs/adr/0136-a-late-workspace-handoff-replaces-the-sandbox-at-a-fenced-turn-boundary.md).
The first established selection remains pinned to the agent and thread, and
choosing another repository requires a new thread.

Set `api.githubRepoAllowlist` to exact `owner/repository` entries or explicit
owner-wide `owner/*` entries. Empty is deny-all. Curie checks this policy before
selection or credential resolution and again before publication, including for
already-selected threads. The GitHub App path mints a token narrowed to the
selected repository. The `api.githubToken` fallback may carry broader authority
than one repository, so scope it narrowly; the allowlist controls where Curie
may present it, and credential audit rows record which path was used.

```yaml
api:
  githubRepoAllowlist:
    - acme-corp/acme-bot
    - acme-labs/*
```

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

The CLI can point a release at it instead of a values file:

```bash
curie cluster github-app --app-id 1234567 --existing-secret my-github-app
```

`--existing-secret-key` defaults to `privateKey`, the same default as the
chart — pass it explicitly if your Secret uses a different key, since the CLI
always sets this field. The command signs a JWT and calls GitHub `GET /app`
before the helm upgrade; a 401 or App-id mismatch leaves the last known-good
credential in place. The command also rolls the API deployment onto the
referenced Secret, so there is nothing further to restart.

**Quick trial — let the chart hold it.** `curie cluster github-app --app-id …
--private-key …` puts the PEM in the chart's own Secret. Fine to prove the flow
works; move to `githubAppExistingSecret` before you rely on it. Once a release
has `githubAppExistingSecret` set, `--private-key` is refused rather than
silently ignored — run `--existing-secret` again to adopt a (possibly new)
BYO Secret, or `--disconnect` first to go back to the chart-held path.

### Rotating the key

A GitHub App can hold several private keys at once, so rotation has no downtime
and needs no coordination with any repository:

1. Generate a second private key on the App's settings page — both are now valid
2. Deploy the new one, and roll the API onto it:
   - **BYO Secret**: update the Secret, then run
     `curie cluster github-app --app-id <id> --existing-secret <name>
     --existing-secret-key <key>` — it performs the rollout for you. Pass
     `--existing-secret-key` whenever the Secret uses a non-default key: the
     CLI always sets this field, so omitting it resets the reference to
     `privateKey`. Updating the Secret alone is not enough.
   - **Chart-held**: re-run
     `curie cluster github-app --app-id <id> --private-key <path>` — same
     rollout.
   - If you update the Secret by hand and skip the CLI, you must run
     `kubectl -n <ns> rollout restart deployment/<release>-api` yourself
     before the next step.
3. Delete the first key on GitHub

Installations, permissions, and the App ID are untouched. This is a real
advantage over a personal access token, where rotation means re-issuing the
credential *and* re-authorizing what it could reach.

Step 2 needs an explicit rollout because `GITHUB_APP_PRIVATE_KEY` reaches the
api pod as a `secretKeyRef` environment variable, and Kubernetes resolves that
exactly once, at pod start — nothing re-reads the Secret until the pod
restarts.

### The other direct-passthrough credentials

The GitHub App key was the first of twelve credential keys read straight from
`.Values` with no in-chart generation (`charts/curie/templates/secrets.yaml`
calls these direct passthrough, as opposed to the thirteen keys
`curie.managedSecret` generates and persists). Issue #1759 gave the other
eleven the same `existingSecret` / `existingSecretKey` escape, one pair per
key, all winning over their plain value when set:

| Credential | Plain value | BYO fields |
|---|---|---|
| Model credential | `agentSandbox.runner.credentials` | `agentSandbox.runner.credentialsExistingSecret` / `credentialsExistingSecretKey` (default key `agentCredentials`) |
| Per-adapter egress secrets | `worker.adapterCredentials` | `worker.adapterCredentialsExistingSecret` / `adapterCredentialsExistingSecretKey` (default key `adapterCredentials`) -- the referenced key must hold the already-JSON-encoded map |
| Outbound GitHub token | `api.githubToken` | `api.githubTokenExistingSecret` / `githubTokenExistingSecretKey` (default key `githubToken`) |
| Sealing private key | `sealing.privateKey` | `sealing.privateKeyExistingSecret` / `privateKeyExistingSecretKey` (default key `sealingPrivateKey`) |
| Sealing previous private key | `sealing.previousPrivateKey` | `sealing.previousPrivateKeyExistingSecret` / `previousPrivateKeyExistingSecretKey` (default key `sealingPreviousPrivateKey`) |
| Slack app token | `dispatcher.slack.appToken` | `dispatcher.slack.appTokenExistingSecret` / `appTokenExistingSecretKey` (default key `slackAppToken`) |
| Slack bot token | `dispatcher.slack.botToken` | `dispatcher.slack.botTokenExistingSecret` / `botTokenExistingSecretKey` (default key `slackBotToken`) |
| Slack signing secret | `dispatcher.slack.signingSecret` | `dispatcher.slack.signingSecretExistingSecret` / `signingSecretExistingSecretKey` (default key `slackSigningSecret`) |
| Mail channel token | `mailAdapter.channelToken` | `mailAdapter.channelTokenExistingSecret` / `channelTokenExistingSecretKey` (default key `mailChannelToken`) |
| Mail egress secret | `mailAdapter.egressSecret` | `mailAdapter.egressSecretExistingSecret` / `egressSecretExistingSecretKey` (default key `mailEgressSecret`) -- when set, `worker.adapterCredentialsExistingSecret` must source the worker's paired credential map |
| AgentMail API key | `mailAdapter.agentmail.apiKey` | `mailAdapter.agentmail.apiKeyExistingSecret` / `apiKeyExistingSecretKey` (default key `mailAgentmailApiKey`) |

For example, to keep the model credential and both Slack tokens entirely out
of helm values and release history:

```yaml
agentSandbox:
  runner:
    credentialsExistingSecret: my-model-credential
    credentialsExistingSecretKey: agentCredentials   # the default
dispatcher:
  slack:
    appTokenExistingSecret: my-slack-tokens
    appTokenExistingSecretKey: slackAppToken          # the default
    botTokenExistingSecret: my-slack-tokens
    botTokenExistingSecretKey: slackBotToken          # the default
```

As with `githubAppExistingSecret`, a Secret missing the referenced key fails
that one pod at `CreateContainerConfigError` rather than the chart silently
falling back to an empty credential.

**Known gap, tracked in #1801.** None of these 22 new fields are yet covered
by the CLI's preserved-values mechanism (the same one `COMMS_MANAGED_KEYS` and
`GITHUB_APP_MANAGED_KEYS` give the Slack tokens and the GitHub App identity in
`cli/src/ops/up.rs`). A plain `curie cluster up` runs a full `helm upgrade
--install` with no `--reuse-values`, so it resets any values key it does not
explicitly re-supply -- set one of these fields today and keep it declared in
the values file you pass to every `cluster up`/`helm upgrade`, the same way you
would for any other values key the CLI does not manage yet.

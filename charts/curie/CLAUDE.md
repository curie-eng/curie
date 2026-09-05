# CLAUDE.md - charts/curie

The umbrella Helm chart: Langfuse + Postgres + Valkey + ClickHouse + RustFS +
OTel Collector, plus the Agent Sandbox substrate and its security rails. Full
component and rail detail in `charts/curie/README.md`.

## Load-bearing invariants

- **Values, not templates, for anything that varies by environment.** Per
  the platform-level rule this repo also follows: resource limits, replica
  counts, and probe settings belong in `values.yaml` / a values overlay, not
  hardcoded in `templates/`. A value safe on a 4 GB scratch cluster can OOMKill
  on a smaller install.
  - **One recorded exception: `templates/mail-adapter.yaml`**, which hardcodes
    `replicas: 1` and `strategy: type: Recreate`. The reason is correctness, not
    sizing: the adapter is one serialized SQLite writer on a single-writer
    `ReadWriteOnce` or `ReadWriteOncePod` PVC.
    A second replica or a rolling update creates two writers and defeats the
    ownership/lease invariant even on storage that happens to multi-attach. Any
    count other than 1 is wrong at every cluster size, which is exactly what
    makes it unlike the resource limits and probe settings the invariant is
    about, and `Recreate` is required for the same reason (a rolling update runs
    two pods for the duration of every upgrade).
    There is deliberately **no `mailAdapter.replicas` key**: a values key that
    must never be changed advertises a knob that silently breaks reply routing,
    which is worse than no knob. Pinned behaviorally by
    `ci/mail-adapter-wiring-assertions.sh` assertion 10, which asserts
    `spec.replicas` is 1 even under `--set mailAdapter.replicas=3`. Horizontal
    scale for this adapter needs an Accepted shared-store/multi-writer design;
    when it lands, this exception is removed rather than extended. Do not
    generalize it into a rule about stateful services -- one named file, one
    named reason.
  - **Second named exception: `templates/langfuse.yaml`**, which hardcodes
    `replicas: 1` and `strategy: type: Recreate` on both the web and worker
    Deployments. The reason is correctness, not sizing: both images run Prisma
    and ClickHouse migrations at container boot with no cross-process lock, so
    a RollingUpdate of a single replica (default `maxSurge: 25%`, which rounds
    up to one extra pod) can apply two migration sets to the same database and
    leave Prisma in a failed state (#2216). Any count other than 1, or any
    strategy other than Recreate, is wrong at every cluster size. There is
    deliberately **no `langfuse.web.replicas` / `langfuse.worker.replicas` /
    strategy values key**. Pinned by `ci/langfuse-recreate-assertions.sh`.
    Horizontal scale for Langfuse needs an Accepted out-of-band migrator; when
    that lands, this exception is removed rather than extended.
  - **Probe handlers stay in the template; probe cadences live in values.**
    The probe *handler* -- `exec` / `httpGet` path / `tcpSocket` port -- is a
    correctness choice about the image, not an environment knob, so it is
    hardcoded in `templates/` on purpose (`langfuse-worker`'s `tcpSocket`
    liveness is load-bearing, #2330; see the liveness invariant below). The
    *cadence* keys -- `initialDelaySeconds`, `periodSeconds`, `timeoutSeconds`,
    `failureThreshold` -- are sizing, so they read from values:
    `postgres.readinessProbe`/`.livenessProbe`,
    `langfuse.web.readinessProbe`/`.livenessProbe`,
    `langfuse.worker.readinessProbe`/`.livenessProbe`, and
    `api.readinessProbe`/`.livenessProbe`, alongside the pre-existing
    `agentSandbox.runner.readinessProbe` and `dispatcher.startupProbe`. On top of
    that: **on every container carrying both probes, the liveness failure cutoff
    must strictly exceed the readiness one**, where
    `cutoff = initialDelaySeconds + (failureThreshold - 1) * periodSeconds` and
    omitted keys take the kubelet defaults (`periodSeconds: 10`,
    `failureThreshold: 3`, `timeoutSeconds: 1`). A liveness probe that gives up
    before readiness has stopped tolerating a slow boot restarts a container that
    is still legitimately booting -- postgres replaying WAL, Langfuse running its
    Prisma/ClickHouse boot migrations, the API warming up -- and the restart
    re-enters the same boot path. Every `livenessProbe` must also declare
    `timeoutSeconds` explicitly; the kubelet's 1s default is invisible in the
    manifest and each timed-out probe counts as a failure. Both rules are pinned
    for *every* rendered container, with no allowlist, by
    `ci/probe-window-assertions.sh`. The remaining hardcoded probe blocks
    (`ui.yaml`, `clickhouse.yaml`, `valkey.yaml`, `rustfs.yaml`,
    `otel-collector.yaml`, `mail-adapter.yaml`, `inference.yaml`, and the
    `curie.heartbeatProbes` helper used by `worker.yaml` and `dispatcher.yaml`)
    all satisfy the ordering invariant today; lifting their numbers onto values
    is a follow-up, and the assertion pins their ordering wherever the numbers
    live.
- **The instrumented set is exactly the workloads whose container `env` block
  includes the `curie.env.otel` helper.** That include *is* the boundary -- it
  is not a count and not a list kept in prose, and this rule exists because a
  written count of "four workloads" went stale the moment a fifth one landed
  (#2331). The include renders `OTEL_EXPORTER_OTLP_ENDPOINT`, `_PROTOCOL` and
  `_HEADERS` from the `otelCollector` block and calls `curie.otel.validate`, so
  adding it is what puts a workload inside chart-owned, validated telemetry, and
  omitting it is what leaves one outside. `grep -n 'curie.env.otel'
  charts/curie/templates/` is the authoritative membership answer at any commit;
  at the time of writing it selects `api.yaml`, `dispatcher.yaml`, `worker.yaml`,
  `agent-sandbox.yaml` (the runner) and `mail-adapter.yaml`. Adding a workload
  means adding the include. Configuring the same three variables through that
  workload's `extraEnv` instead does **not** satisfy the
  `security.checkDefaultCredentials` production gate: each workload would then
  carry its own copy of the destination, so any one of them can drift from the
  rest while the render still looks correct. Two corollaries. The chart half is
  only half the boundary -- the workload's own entrypoint must call
  `bootstrap_service_telemetry` (`packages/telemetry`), or the env arrives at a
  process that reads none of it and whose logs never pass `RedactingLogFilter`.
  And a workload with an egress NetworkPolicy needs a collector peer in it as
  well; today the mail adapter is the only such workload (see the next
  invariant).
- **`langfuse-worker`'s liveness probe is `tcpSocket`, not HTTP.** Both
  `/api/health` and `/api/ready` in the worker image run a Prisma `SELECT 1`
  plus a Redis ping and differ only in SIGTERM handling, so an HTTP liveness
  probe would poke the same stores the HTTP readiness probe already pokes.
  `langfuse-worker` is `replicas: 1` and runs Prisma and ClickHouse migrations
  at container boot, and init containers do NOT re-run on a liveness restart --
  so an HTTP liveness probe would restart the only replica straight into those
  boot migrations against a Postgres or Valkey that may still be recovering,
  which is exactly the crash-loop class
  this chart's readiness gates exist to prevent (#2330). Accepted trade-off:
  `tcpSocket` will not catch a wedged Node event loop -- the kernel still
  accepts on the listen backlog against a process doing nothing -- so
  detection of that rides on the **readiness** probe instead, which does
  exercise the event loop and the stores, and flips `Deployment.Available`.
  The chart's other first-party liveness probes are likewise process-local
  rather than store-probing: `api.yaml`'s `/health` does no store I/O,
  `mail-adapter.yaml` documents `/healthz` as a static liveness signal while
  `/readyz` alone waits on SQLite, `inference.yaml` uses `tcpSocket`, and
  `worker.yaml`/`dispatcher.yaml` use the heartbeat-file check
  (`curie.heartbeatProbes`). `langfuse-web` probing `/api/public/health` for
  *both* readiness and liveness is the exception, not the pattern -- do not
  copy its liveness shape onto the worker.
- **Mail-adapter egress is a separate fail-closed rail.** Enabling
  `mailAdapter.deploy` requires at least one
  `mailAdapter.agentmail.httpsCidrs` entry. One egress-only policy is the
  complete list of what that pod may reach, and every destination is a rule
  inside it rather than a second policy object, so one object still shows
  everything a pod holding three credentials can talk to -- read the rules in
  `templates/mail-adapter.yaml` for the current set rather than trusting a count
  here. As written today they are DNS, this release's API pods, those
  `agentmail.httpsCidrs` on TCP 443, and -- only while `otelCollector.deploy` is
  true -- this release's OTel Collector on its gRPC and HTTP ports. With
  `api.deploy` false, `mailAdapter.apiEgress.httpsCidrs` and `.port` replace the
  API pod selector with an explicit narrow BYO-API peer. Because this is the
  only first-party service with an egress policy at all, it is also the only one
  whose OTLP export can be dropped by its own rail: with `otelCollector.deploy`
  false and an external `otelCollector.endpoint`, this policy has no peer for
  that address, and the fix is an operator-supplied additional egress policy
  selecting the adapter (NetworkPolicies union), not a broad allow in the chart
  for an address the chart cannot know. It never selects a runner sandbox and
  never allows the Kubernetes API. The runtime pod mounts no ServiceAccount
  token and has no RBAC. Prefix-0 and prefix-1 routes fail render, including
  split default routes. Do not turn provider DNS into a broad CIDR or add a
  private-network allow: use current provider ranges or a controlled egress
  proxy with a stable range.
- **Every backing store follows the same toggle + BYO idiom.** `<store>.deploy`
  (default `true`) gates whether the in-chart resource renders; flipping it
  to `false` repoints consumers (Langfuse env, the collector config) at the
  BYO `host`/`port`/`auth`/`existingSecret` fields on the same block. A new
  backing store must follow this exact pattern -- do not add a store with a
  different enable/disable shape.
- **Values keys are camelCase, not hyphenated.** Go templates cannot
  dot-index a hyphenated key. Keep this consistent across any new values
  additions.
- **Placement-class lookups go through `curie.placement.class`; never
  index `.Values.placement.<class>` directly in a template.** Helm's
  values coalescing deletes a key whose replayed value is YAML null, so
  `helm upgrade --reuse-values` on a release created before placement
  classes existed -- which stored `placement: null` -- leaves
  `.Values.placement` nil, and a direct dereference crashes the render
  before any cluster mutation (#2008). A nil or missing tree/class
  degrades to the chart's empty defaults; a tree or class that is present
  but not a map is refused with `fail`, deliberately -- `fromYaml` returns
  an error map rather than an error for a non-map document, so softening
  that refusal into a default would silently drop every scheduling
  constraint the operator asked for. A new pod surface added to the chart
  uses the helper with its class name rather than a fresh direct index.
- **Fail-closed egress, always.** `security.networkPolicy.allowedEgress` is
  empty by default; an unset allowlist must never mean allow-all. If you add
  a new egress destination the runner needs, it goes into this allowlist
  explicitly -- never widen the default-deny baseline itself. The exception
  is a BYO object store (`rustfs.deploy: false`): that allow is
  `rustfs.egress` (and `rustfs.stsEgress` on the key-free path), required at
  render, not mixed into the model-API allowlist.
- **NetworkPolicy allows are additive, never restrictive-intersecting
  (#765, ADR-0067).** A second NetworkPolicy selecting the same pods can only
  widen what Rail 1 permits, never narrow it -- there is no such thing as one
  policy overriding another. This is why the runner SandboxTemplate sets
  `spec.networkPolicyManagement: Unmanaged` whenever Rail 1 is on: it stops the
  vendored controller from reconciling its own separately-managed, broader
  egress policy for the same pods. Do not add any other NetworkPolicy-adjacent
  mechanism (another controller, an operator, a second chart) that could select
  `component: runner-sandbox` pods without checking it does not reintroduce
  this exact union-defeats-default-deny failure mode.
- **The preflights are mandatory Helm hooks, not advisory scripts.** The
  CPU-AVX/ClickHouse-pin check (`preflights.avxCheck`), the
  NetworkPolicy-enforcement probe (`preflights.networkPolicyProbe`), and the
  controller-ready gate (`preflights.controllerReady`, which fails the install
  if the vendored agent-sandbox controller cannot sync its cluster-scope
  NetworkPolicy informer -- issue #350) block a broken install. Do not make
  any of them skippable by default, and do not add a new cluster-dependent
  assumption (a CNI feature, a kernel feature, an RBAC grant the controller
  needs to start) without a matching preflight -- an assumption that silently
  fails on a customer cluster is exactly the failure mode these exist to
  prevent.
- **gVisor needs `runsc` on the node; the chart cannot install it.** On a
  cluster without it, use the ready-made overlay
  `-f charts/curie/values-e2e-nogvisor.yaml` (sets `runtimeClassName=""` and
  disables the gVisor preflight, leaves every other rail on) rather than
  hand-editing `security.gvisor.*` -- the overlay is the supported opt-out
  path for e2e/scratch clusters.
- **CRDs in `crds/` are vendored, never templated.** Helm does not
  upgrade or delete `crds/` content; a teardown needs a manual
  `kubectl delete crd <name>`. Do not move CRD definitions into `templates/`
  to make them "manageable" -- that changes install ordering guarantees
  Helm's `crds/` convention provides.
- **The controller (`agentSandbox.controller.deploy`) is cluster-scoped.**
  Install it from exactly one release per cluster; leave it `false` on any
  cluster that already runs `agent-sandbox`. Do not default this to `true`
  in a values file intended for a shared/multi-release cluster.
- **The runner image is `IfNotPresent` + prewarm, NOT `Always`.** Images pull
  from GHCR; the four Deployment-managed services default to `Always` (fresh
  `:latest` on every rollout), but the runner must not -- a sandbox pod is
  created per Slack thread, and an in-boot image download can blow the
  worker's claim timeout (live incident 2026-07-06). The runner-prewarm
  DaemonSet (`agentSandbox.runner.prewarm`) pulls the runner image at
  install/upgrade instead, and every `helm upgrade` rolls it to refresh the
  cache. Do not flip the runner to `Always` and do not disable the prewarm
  on `:latest`-tag clusters without accepting stale-image risk.
- **`values.schema.json` is deliberately permissive, not a full contract.**
  The chart had no values schema at all before issue #1388; Helm now
  validates the ENTIRE coalesced values tree against `values.schema.json`
  on lint, template, install, and upgrade -- not just the keys a given
  operation touches. Because of that blast radius, the schema stays
  draft-07 with no top-level `required` and no `additionalProperties:
  false`, and it types only four bounded values: the three worker knobs
  (`worker.claimTimeoutSeconds`, `worker.routeTtlSeconds`,
  `worker.suspendedRouteTtlSeconds`) and the approval-chat attester's explicit
  nonblank contract (`api.approvalChatAttesterSecret`). Adding a `required` or
  `additionalProperties: false` constraint, or typing any other existing
  key, would fail every install whose values file happens not to match the
  new shape -- broaden the schema only for a key you are prepared to
  validate across every current values file and overlay.
  - Known trap: `api.githubAppId` must stay untyped. It reaches the
    coalesced values tree as a JSON string when `curie cluster
    github-app` sets it via `--set-string` (`cli/src/github_app.rs`,
    deliberately, to dodge Helm's float64 round-trip on a bare numeric
    `--set`), but as a number if anyone sets it with a plain `--set` or an
    unquoted YAML override. Typing it either way in the schema breaks the
    other path.
  - Corollary: `--set-string` on any of the three bounded worker knobs now
    fails by design -- a JSON string violates `type: integer` /
    `type: number` even though the rendered env var is itself a quoted
    string. The attester remains a string, but an explicitly supplied empty
    or whitespace-only string fails by design; omitting it still delegates to
    the managed-secret generator.
  - The schema is not the only gate. Helm drops nil-valued keys during
    values coalescing before schema validation runs, so
    `--set worker.routeTtlSeconds=null` is not caught by the schema; it
    renders an empty `CURIE_ROUTE_TTL_SECONDS` env value and is caught
    instead by the worker's own boot-time refusal. Both checks are
    load-bearing -- the schema catches a bad value early for the common
    cases, the worker's refusal is the backstop for the coalescing gap the
    schema cannot see. `placement` is the second instance of this exact
    coalescing gap -- a release created before placement classes existed
    can store `placement: null`, and `--reuse-values` on upgrade replays
    it, deleting the key -- and the `curie.placement.class` helper (see
    the values-keys invariant above) is its template-level backstop
    (#2008). `worker.publication.githubHttpsCidrs` is a third instance:
    `minItems: 1` catches `[]`, but a coalesced nil deletes the key and a
    `range` over it used to emit an empty `to:` (allow-all on 443 for the
    tokenless publication job). The template `fail` in
    `publication-owner.yaml` is the backstop (#2321).

## Verify

Static / chart-authoring checks (they render manifests but NEVER run a container,
so they cannot catch a bug that only surfaces at runtime):
```bash
helm lint charts/curie
helm template charts/curie -f charts/curie/values-dev.yaml   # chart-authoring check, no cluster contact
```

Runtime check (the cheap default for a chart/sandbox/bundle change): installs a
trimmed slice, runs the bundle-fetch init pair, and exec-asserts on the runner:
```bash
curie dev chart-runtime-e2e            # implemented by scripts/chart-runtime-e2e.sh
curie dev chart-runtime-e2e --force    # same, on a scratch context not named k8scratch
```
A ticket whose AC is a runtime check (like #56, the bundle-fetch credential
isolation) is only satisfied by running this and pasting its output -- lint /
template do not exercise the init container or the live runner.

It refuses any kube context not named `k8scratch`, because it installs and
uninstalls a real release; `--force` is the override, and points at a disposable
cluster only. The other script flags (`--namespace`, `--release`, `--chart`,
`--runner-image`, `--expect-vulnerable`, `--keep`) are not yet exposed through
`curie dev`; reach them with `bash scripts/chart-runtime-e2e.sh --help`.

Cluster verification (a disposable local cluster, `kind` or `k3s`):
```bash
helm install curie-dev charts/curie -n curie-dev --create-namespace \
  -f charts/curie/values-dev.yaml
kubectl get pods -n curie-dev -w
helm test curie-dev -n curie-dev                              # re-runs the three preflights + the security probe suite
kubectl logs -n curie-dev job/curie-dev-preflight-avx
kubectl logs -n curie-dev job/curie-dev-security-probe        # rails 1, 2, 4
kubectl logs -n curie-dev curie-dev-security-probe-hardening  # rail 3
```

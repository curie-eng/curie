# Operating the `cluster` target

This doc is the runbook for the **`cluster`** target: the Curie platform
running on a Kubernetes cluster (a Helm release). The same `curie` binary
installs and runs it, wrapping the umbrella Helm chart the way `linkerd` or
`cilium` wrap theirs. Every verb takes `--dry-run` to print the exact
`helm`/`kubectl` command line (secrets masked) without executing.

## The Kubernetes cluster

This doc covers the `cluster` target specifically. For `skill` or `local`
Targets, see the target comparison table in the
[README](../README.md#which-target-do-i-want) or [`cli/README.md`](../cli/README.md).

**Prerequisites:**

| Requirement | Why |
|---|---|
| `kubectl` and `helm` on PATH | Every `cluster` verb wraps one or both of them. |
| A reachable cluster | Every verb talks to the cluster's Kubernetes API server directly -- there's nothing to install onto or inspect without one. The chart's own preflights additionally need the `agents.x-k8s.io` Agent Sandbox CRDs (Custom Resource Definitions) installable and a NetworkPolicy-enforcing CNI (Container Network Interface) already present; see `charts/curie/README.md`. |
| `runsc` (gVisor) on every node for full kernel isolation | A real model first installs with gVisor enabled. If admission reports exactly that the `gvisor` RuntimeClass is absent, plain `cluster up` shows that attempt as retrying, applies `security.gvisor.mode=off`, and retries once. Other preflight failures remain closed. Fake model installs do not need it. |

**For testing**, pick between **k3s**, **kind**, and **minikube** based on
your host and how disposable the cluster needs to be. A single-node **k3s**
cluster (8 GB+ memory) is the lasting recommendation if you're on Linux --
its default kube-router CNI enforces NetworkPolicy out of the box, though
k3s itself only runs on Linux. **kind** and **minikube** work anywhere
Docker does and are fine for disposable local tests, but their Kubernetes
API server typically binds loopback, which can make it unreachable from a
pod; if `cluster message` can't auto-detect a pod-reachable host, pass
`--listen-host` explicitly (see `cli/README.md`).

**For production**, you'll likely point at a managed or self-hosted cluster
instead. Name it on every `cluster` command with `--context` (see below), so a
stale kubeconfig current-context cannot send a command at the wrong cluster:

```bash
curie cluster status --context <your-production-context>
```

## Installing and inspecting the Curie platform on the cluster

Every `curie cluster` verb takes `--context <NAME>`. The CLI resolves the
context once, prints `Kubernetes context: <NAME> (cluster <CLUSTER>)` on stderr,
and pins it for every `helm` and `kubectl` call it makes, including any ambient
`HELM_KUBECONTEXT`. Without the flag it pins the kubeconfig current-context. A
name that is not in the kubeconfig is refused before anything runs. Pass the
flag explicitly on a workstation whose kubeconfig also holds production
clusters.

### Running a build from a commit, without waiting for a release

Every CI run publishes an installable Linux binary for both architectures,
built the same way a tag builds them and carrying the same glibc 2.28 floor, so
a cluster can run an unreleased fix without compiling Rust on the host.

```bash
run=$(gh run list -R curie-eng/curie --commit <sha> \
        --workflow CI --json databaseId -q '.[0].databaseId')
gh run download "$run" -R curie-eng/curie -n curie-aarch64-unknown-linux-gnu-<sha>
chmod +x curie && sudo install -m 0755 curie /usr/local/bin/curie
curie --version
```

Swap `aarch64` for `x86_64` as needed. The chart is version-pinned to the
binary, so pass the matching chart when the commit changes it:

```bash
curie cluster up --chart /path/to/checkout/charts/curie ...
```

Two things to know:

- **Artifacts expire** (90 days by default) and are not signed or SBOM'd the way
  release assets are. This is for testing a fix and for hosts that cannot wait
  for a tag -- prefer a release for anything long-lived.
- **`curie-x86_64-linux-<sha>` is a different artifact** and not this one. That
  is a native build used by the e2e jobs; it inherits the runner's glibc and
  will not start on an older distro. The ones named for a full Rust target
  triple (`curie-<target>-<sha>`) are the portable pair.

### `curie apply`

Copy [`examples/curie.yaml`](../examples/curie.yaml) into your repository as
`curie.yaml` and customize it, or write the same starter from a released binary
with `curie apply --init`. Credential fields contain credential names, not
secret values. `install.context` (and `curie apply --context` / `curie diff
--context`, which win over the file) selects the kube context; `curie diff`
prints the cluster that context names. Shared-cluster singleton opt-outs are
modeled as `platform.sandbox_controller` and `platform.priority_classes.platform`
/ `sandbox`. `platform.gvisor` is `auto`, `require`, or `off` and controls
runner kernel isolation, not those singletons. `set:` values are always strings;
a boolean or null is refused and the error names the empty string form, except
for keys that have a modeled field. The input schema is
`curie schema-index curie-yaml`.
Before either command, provide values for `ANTHROPIC_API_KEY`,
`SLACK_APP_TOKEN`, and `SLACK_BOT_TOKEN` in the environment or store them with
`curie secrets set <NAME>`.

Preview the installation, then apply it:

```bash
curie apply --dry-run
curie apply
```

Use `curie cluster up` below for flag driven installs.

#### Declarative local inference

`platform.inference: true` opts into the in-chart Ollama inference deployment.
It does not permit the default implicit pull to an `emptyDir`: `curie apply`
refuses before Helm or Kubernetes creates resources unless the manifest chooses
durable storage or declares that its model is already provisioned. For the stock
Ollama image, enable persistence and size it for the model:

```yaml
platform:
  inference: true
  inference_persistence: true
set:
  inference.persistence.size: 40Gi
```

With `inference_persistence: true`, the existing `postStart` hook pulls the
model into the PVC. If `set.inference.persistence.size` is absent, the chart
uses `10Gi`; when supplied it must be a non-boolean string, and large models
need a larger size. The PVC carries `helm.sh/resource-policy: keep`, so an
upgrade that stops deploying inference or `helm uninstall` leaves it in place;
delete it yourself to reclaim the space. The annotation lives on the live PVC
object, not the chart: a PVC created before this chart added it only gets the
annotation once an upgrade with inference still enabled renders it.

The advanced alternative is a custom image or other provisioning that already
has the requested weights. Declare that explicitly instead:

```yaml
platform:
  inference: true
  inference_pull_model: false
```

This disables the `postStart` pull. The stock Ollama image on the default
`emptyDir` has no weights, so it cannot serve a model unless provisioning has
placed those weights at Ollama's data path. Direct Helm installs have the same
chart guard.

### `curie cluster up`

Installs (or upgrades) Curie's Helm chart onto the cluster you're pointed at:

```bash
curie cluster up
```

This is a full Helm upgrade. It carries forward the recorded generated secrets,
sealing keys, Slack tokens, GitHub App and token, runner model and credential,
gVisor mode, worker extra environment, trusted Slack origins, runner egress,
and the installed `mailAdapter.*` values with the worker's paired adapter
credential map or external Secret reference. Other values must be supplied again.
Explicit `--set` and `--set-string` inputs override retained values. Retained mail
values use a private temporary values file; the command shows field names only.
The offline `--dry-run` does not read an installed release, so it cannot display
that release's retained values.

| Flag / env var | What it does |
|---|---|
| `--chart <path-or-tgz>` | Install from a local chart instead of the pinned release asset (for chart development). |
| `-f <compose>` | Override a resolved local-dev artifact path. |
| `--image <ref>` | Override a resolved image reference. |
| `--no-expose` | Keep the UI and Langfuse ClusterIP-only instead of exposing them on node ports. |
| `--adopt` | Install into a pre-existing namespace that already has its own labels or its own objects. Without it, such a namespace is refused. See [Adopting a pre-existing namespace](#adopting-a-pre-existing-namespace). |
| `CURIE_CREDENTIALS` (alias `CURIE_MODEL_CREDENTIALS`) | A real model credential. The interactive check accepts Anthropic `sk-ant-`, OpenRouter `sk-or-`, Zhipu `id.secret`, and bare `sk-` shapes for Moonshot or DeepSeek. The first two prefixes select one provider and infer its egress when no provider flag is present. Other shapes do not identify a provider. Present credentials install live through masked `--set` machinery, so `--dry-run` never prints them. An absent credential uses fake mode on a fresh install and preserves the recorded model configuration on a rerun. |
| `--fake-model` | Explicitly downgrade to fake mode, even when a credential is present or a rerun has recorded live model configuration. On a local-model install the rerun stops deploying in-cluster inference, so the inference Deployment and Service are removed. The model-weights PVC is kept only if it already carries `helm.sh/resource-policy: keep`; see [`--fake-model` and the model-weights PVC](#--fake-model-and-the-model-weights-pvc) below for the upgrade prerequisite and how to restore local inference. A recorded credential stays stored in the release but is not mounted into the sandbox runner or worker while fake. |
| `--github-token <token>` (or `CURIE_GITHUB_TOKEN`) | The Curie API's own GitHub credential, for cloning a PRIVATE repo during a git-flow bundle deploy and for posting the eval commit status. Goes to helm through a private mode-0600 values file, never a command-line argument, so it never appears in the helm command, the printed plan, or that plan's JSON. Prefer the environment variable: a token typed after the flag still sits in `curie`'s own argv, so it still reaches your shell history and `ps`. Omitting both on a later `cluster up` preserves whatever the release already has. Errors if combined with `--set api.githubToken=`. |
| `--clear-github-token` | Remove the stored GitHub credential. Not a revocation: the running API keeps the old token until its pod restarts (`cluster up` prints the restart command), and the token itself stays valid at GitHub until you revoke it there. |
| `--allow-egress-host <provider>` (repeatable) | Explicitly open runner egress on TCP 443 to one named model provider: `anthropic`, `openrouter`, `zhipu`, `moonshot`, or `deepseek`. Names are lowercase exact. An explicit list must include the provider detected from an `sk-ant-` or `sk-or-` credential. |
| `--allow-web-egress <CIDR>` (repeatable) | Open runner egress on TCP 443 to an arbitrary CIDR (Classless Inter-Domain Routing block) -- for skill/tool web access, or a provider not covered above. |
| `--forward-only` | Apply contract or irreversible schema migrations during this upgrade. The default refuses those migrations before mutation so a patch rollback window stays intact. Expand-only patch migrations do not need the flag. |

A downloaded release binary needs no repo checkout; the chart resolves from
the version-pinned release asset by default.

#### `--fake-model` and the model-weights PVC

The `helm.sh/resource-policy: keep` annotation lives on the live PVC object,
not the chart, so it protects the PVC only if that object already carries it.
On an existing installation, check first:

```bash
kubectl get pvc <release>-inference -n <namespace> -o jsonpath='{.metadata.annotations}'
```

If the annotation is missing, run a plain `curie cluster up` (inference still
enabled) before `--fake-model`; that upgrade keeps inference deployed and
applies the annotation. Only then does `--fake-model` keep the PVC when it
removes the inference Deployment and Service.

`--fake-model` also drops the recorded inference values, including a
nondefault `inference.persistence.size`. A later `cluster up --local-model`
that does not resupply that size renders the chart default `10Gi`, and
Kubernetes refuses to shrink the existing PVC, so the upgrade fails. Resupply
the same model, persistence settings, and original size (for example
`--set inference.persistence.size=40Gi`) to reuse the kept PVC.

#### Adopting a pre-existing namespace

`cluster up` creates its namespace, and stamps what it created with
`curietech.ai/created-by=<release>` and `curietech.ai/created-in=<namespace>`.
`cluster down` deletes namespaces by exactly that label pair, so the stamp is
what makes a teardown safe: it can only delete what this install made.

That is also why a namespace which already exists is refused rather than
adopted. Adopting someone else's namespace would stamp it, and a later
`cluster down` would then delete it along with whatever was already inside.
`cluster up` therefore refuses a pre-existing namespace that has its own labels,
its own ownership labels, or any object beyond the two Kubernetes puts there
itself (the `default` ServiceAccount and the `kube-root-ca.crt` ConfigMap).

On a cluster where the namespace is pre-provisioned -- with a quota, a
NetworkPolicy, a Pod Security label, an Argo CD tracking label, or a pull
secret -- that refusal is the normal case, and deleting the namespace to get
past it is worse than the thing the refusal is protecting against. `--adopt` is
the supported way through:

```bash
curie cluster up --namespace platform-curie --adopt
```

**What it records.** The adoption is written onto the Namespace, not just to the
terminal, so it can be read back long afterward:

```bash
kubectl get namespace platform-curie -o yaml
```

| Key | What it holds |
|---|---|
| `curietech.ai/adopted-by` (label) | The release that adopted the namespace. |
| `curietech.ai/adopted-in` (label) | The install namespace of that release. |
| `curietech.ai/adopted-at` (annotation) | When the adoption happened, RFC 3339 UTC. |
| `curietech.ai/adopted-labels` (annotation) | The labels the namespace already carried. |
| `curietech.ai/adopted-contents` (annotation) | The non-default objects that were already in it. |

Pre-existing labels and annotations are preserved; the adoption record is merged
into them. A very long inventory is truncated, and says so.

**An adopted namespace is retained, not deleted.** `curietech.ai/adopted-by` and
`curietech.ai/adopted-in` are deliberately *not* the pair `cluster down` selects
on. A later `curie cluster down` uninstalls the release and leaves the namespace
and everything in it in place. Delete it yourself when you want it gone. A
namespace `cluster up` created itself is unaffected and is still swept.

**What `--adopt` does not do.**

- It never adopts the shared `agent-sandbox-system` controller namespace. That
  namespace is a cluster singleton shared by every install on the cluster.
- It never adopts a terminating namespace, which cannot accept new objects.
- It never weakens a read. If the namespace, the namespaced API inventory, or an
  aggregated `APIService` cannot be read completely, or the namespace is modified
  concurrently, the install still fails closed -- contents that cannot be read
  cannot be recorded.
- Passing it when nothing needed overriding changes nothing: an empty, unlabelled
  namespace is adopted the ordinary way and stays sweepable by `cluster down`.

A re-run does not need the flag again. `cluster up` recognises a namespace this
same release already adopted and converges without rewriting the original record.

**Provider-native runtime configuration.** Zhipu, Moonshot, and DeepSeek need
their matching documented `CURIE_MODEL_BASE_URL` in worker runtime configuration,
as well as a credential and their named egress entry. Their credential shapes do
not identify the provider: the base URL selects it.

**Ambiguous egress stays sealed.** An effective credential beginning `sk-ant-`
or `sk-or-` selects Anthropic or OpenRouter and plain `cluster up` infers the
matching named egress. Other credential shapes do not identify one provider,
so the sandbox stays fail closed until the operator opens its provider or web
egress. An explicit provider list that omits a detected provider is a usage
error. Neither flag bakes provider IPs into the binary. Only hostnames are
resolved to narrow `/32` and `/128` host routes at install time because
provider and CDN IPs rotate. Re-run `up` to resolve them again if calls start
failing. The named provider allowlist admits only the five documented lowercase
names above. Unknown names stay denied. `--allow-web-egress` is for agents whose
skills need open web access, such as search or weather lookup, beyond the named
model providers. `curie cluster up --allow-web-egress 0.0.0.0/0` opens the
internet except `169.254.169.254`; narrow the CIDR to a specific destination for
a tighter posture. A default route value (`0.0.0.0/0`, `::/0`, or any `/0`
prefix) prints a distinct rail removal warning, since it removes the default
deny rail for a prompt injectable sandbox.

During `cluster up`, an unambiguous Anthropic or OpenRouter credential from
`CURIE_CREDENTIALS`, `--set agentSandbox.runner.credentials`, or preserved
release values requires an explicit `--allow-egress-host` list that includes the
matching provider. Otherwise `cluster up` exits with a usage error before
changing the cluster. This is a consistency check only: a credential never
selects a provider or opens egress. Ambiguous credential shapes remain valid
with any known explicit provider.

You don't need to worry about ordering when using the CLI flags together --
`cluster up` composes `--allow-egress-host` and `--allow-web-egress` into
one list automatically, with named-provider entries first and web-egress
CIDRs after.

**Cluster facts are inferred only when they are complete.** Direct
`curie cluster up` inspects the two PriorityClasses and the
`agent-sandbox-controller` Deployment. When complete Helm ownership metadata
names another release, Curie applies the matching creation or deployment value
as false. Missing, malformed, unreadable, or incomplete ownership does not
authorize reuse and blocks the install. An explicit true value that contradicts
the detected owner is a usage error.

Before Helm runs, Curie establishes ownership of the primary install namespace
with both the release and install-namespace labels. An absent namespace is
created atomically with those labels. An existing namespace already owned by
that pair is reused; an unlabeled primary namespace is adopted only after a
safe inventory proves it is empty apart from the default ServiceAccount and
`kube-root-ca.crt` ConfigMap. A pre-existing unlabeled namespace containing
other objects, or a foreign, partially owned, or otherwise conflicting
namespace, is refused with a reason. The shared
`agent-sandbox-system` namespace keeps its create-only behavior and is never
adopted. The inventory is a point-in-time observation. The adoption patch
compares the Namespace UID and metadata resource version, but creation of a
namespaced object does not change that resource version, so the patch does not
serialize other writers; keep an unowned namespace unused by other writers
through adoption. An unavailable remote APIService registration, or inability
to read APIService availability, blocks empty-namespace adoption. A terminating
namespace is readable but is refused for `cluster up`.

The first gVisor preflight keeps the chart default. Only the exact admission
result `RuntimeClass "gvisor" not found` authorizes
`security.gvisor.mode=off` and one retry. That first attempt renders as
retrying, not as a failed install; the retry is the one installed or failed
result. An explicit `auto` or `require` mode contradicts that result and
errors. Other admission failures and an unavailable event watch remain closed.
Curie prints one standard error line for every inference, including the
equivalent override. Prepared `apply` and `diff` paths do not infer live
cluster facts.

A Helm release whose history is only `failed` (no `deployed` or `superseded`
revision) is not an upgrade. `curie apply` and `curie cluster up` uninstall that
record, then install. An in-flight status (`pending-install`, `pending-upgrade`,
`unknown`) is refused at once and names `curie cluster down`. A known-good
revision is left intact. This is the failed-first-install wedge: Helm would
otherwise fire the pre-upgrade drain hook against Secrets revision 1 never
created.

### `curie cluster status`

```bash
curie cluster status
```

A release that has not converged returns exit code 1 with its status report and
rollout reasons. `--json` retains the same report object with `healthy: false`.
The check compares the installed Helm target with live workload generations,
replica counts and serving pod images; an `Available` condition alone is insufficient.

A diagnosis the command could not MAKE is a warning, not a failure. When the
mail adapter is deployed but its `/statusz` cannot be read -- the pod proxy is
unreachable, the pod is restarting, the adapter predates that endpoint -- the
mail channel token reads as unknown, which prints a warning and appears under
`warnings` in `--json`, leaving the exit code and `healthy` alone. A token that
was read and found expired, rejected, missing or invalid is still a failure. The
probe follows `mailAdapter.deploy`, so a release with the adapter off is not
probed at all.

Reports whether the release is healthy, which pods are ready, and the URLs
to reach it -- including the web console, where you can see your agents,
their deployed versions, and their run history. That console URL includes a
`?api=1` parameter; leave it as-is when you open it, it's just what points
the console at this release's Curie API. `--json` also reports the current
upgrade phase and the last known-good version.

### `curie cluster upgrade`

```bash
# release build: --chart defaults to the version-pinned release asset for --to
curie cluster upgrade --to 0.9.0

# local chart file (e.g. a downloaded release archive): same metadata-read
# refusal path as a local directory
curie cluster upgrade --to 0.9.0 --chart ./curie-0.9.0.tgz

# resolvable ref: the command adds --version internally, do not pass it
curie cluster upgrade --to 0.9.0 --chart oci://<your-registry>/curie
```

The chart and `--to` must agree. On a release build, omitting `--chart`
resolves the GitHub release chart for the target version in `--to`. On a
dev build, omitting it uses the local `charts/curie` chart. An explicit
`--chart` override wins in either channel. Helm silently ignores `--version`
on a local directory or file chart, so for that case the command reads the
chart's own metadata instead of passing `--version`.
For a chart ref Helm resolves itself (a repo or OCI ref), the command
passes `--version <to>` internally and lets Helm enforce it; there is no
`--version` operator flag.

A release dry run still reads the cluster, but it does not download the default
release chart archive or change the installed release. If its target archive is
already cached, the command reads the chart version and renders its schema
compatibility metadata exactly as a real upgrade does. If the archive is absent,
the plan names the release URL and cache path and marks those two target checks
pending. An explicit Helm repository or OCI reference may require Helm to fetch
the chart while `helm template` renders its schema metadata. The command still
reads and migrates retained configuration, including reporting an ambiguous
configuration conflict. A real upgrade downloads the archive before Validate
and runs every target check before mutation.

| Flag | What it does |
|---|---|
| `--to <version>` | Target Curie version. Required. |
| `--chart` | Chart path or ref override. |
| `--yes` | Skip the confirmation prompt. |
| `--dry-run` | Print the redacted plan and exit without changing the installed release or downloading the default release chart archive. It still reads the installed release from the cluster, and retained-configuration checks always run. Available local charts and Helm refs also run target chart and schema checks; Helm may fetch an explicit repository or OCI ref for those metadata checks. A cold default release archive records those checks as pending until download. |
| `--forward-only` | Apply pending contract or irreversible schema migrations. Without this flag, Validate refuses those migrations before mutation so a patch rollback window stays intact. |

One resumable lifecycle: inspect and plan, validate configuration and
schema compatibility and refuse on an ambiguous migration conflict, drain
accepted work, checkpoint, apply, wait for exact convergence, run a
target-version canary, then record the new known-good version. There is no
separate migration step in the command: configuration migration happens at
Validate, and schema migration is the chart's pre-upgrade Job, which Apply
fires. The command chooses the values overlay; do not pass `--reuse-values`
or `--reset-then-reuse-values`. Configuration migration to the current
schema happens at Validate, before any mutation. Database/application
schema compatibility is also checked at Validate: an incompatible live
revision or a pending contract/irreversible migration without
`--forward-only` refuses before `helm upgrade`. `--forward-only` sets
`api.migrate.forwardOnly=true` on the overlay Apply hands Helm. The
`migrate` phase is a resumable checkpoint boundary only; it performs no
migration of its own.

The redacted plan names the configuration schema version the upgrade migrates
from and to (`config schema: <from> -> <to>`). It never carries credential
values. The plan's `helm upgrade` line is generated from the same chart
resolution and the same `--version` decision the command executes, so it names
the chart that will actually be applied: the target-version release asset on a
release build, local `charts/curie` on a dev build, or the explicit `--chart`
override. It shows `--version <to>` exactly when a resolvable ref makes it a
real pin. A release asset is applied from its downloaded local archive, so its
plan never shows `--version`, including when a cold dry run marks target checks
pending.

After Apply, the command reads the installed chart version from
`helm get metadata` and fails rather than reporting success if it is not the
target version; the canary reads it again. Convergence (image digests,
controller generations, replica counts, healthy hooks, the drained-queue gate,
and the retained manifest comparison) is observed the same way `curie cluster
up` observes it, not assumed.

`--json` reports the current phase, the last known-good version, whether
the previous version is still serving, and at most one fail-forward
command. Success is refused unless convergence is exact and the canary
passed. After a normal command failure, run the same command to resume only
when cleanup successfully released ownership.

This composes configuration migration (issue 2299) with a `drain_preflight`
phase that confirms the worker workload is reachable ahead of Apply (issue
2830): a resume after a completed preflight does not repeat it. That phase is
not the drain gate itself; the gate (issue 2010) is the chart's own
pre-upgrade Helm hook Job, which runs during Apply and whose outcome the
convergence check above reports as the drained-queue gate.

After confirmation, the command claims the namespaced
`<release>-upgrade-checkpoint` ConfigMap before it reads release snapshots or
runs Helm. The claim records an opaque holder identifier and a redacted action
that names the target version. Another current `curie cluster upgrade` process
that finds the claim refuses immediately and reports both values, even when it
requested the same target. Each checkpoint update and the ordinary holder
release test both that holder and the exact Kubernetes `resourceVersion`
returned by the preceding successful operation. A process that loses either
comparison stops without replacing the newer checkpoint.

This is cooperative ownership among concurrent `curie cluster upgrade`
processes from a current Curie CLI version. The current `curie cluster up`,
`curie cluster rollback`, and `curie cluster down` verbs do not participate. It
also does not fence an older CLI, a raw Helm command, a direct Kubernetes write,
or a cluster administrator. It does not make Helm and the other upgrade effects
one transaction or guarantee that an external side effect happens exactly once.

The checkpoint is namespaced. If its namespace does not exist, the command
refuses before Helm mutation and directs the operator to establish the install
with `curie cluster up` first. Ownership does not require a cluster scoped read
of the Namespace object.

Any interruption after ownership acquisition, including Ctrl C, SIGINT, and
SIGTERM, leaves the holder in place. A normal exit that reports an ownership
release CAS failure can also leave the holder. Do not rerun the upgrade in
either case until the checked recovery below is complete. There is no expiry,
heartbeat, or automatic takeover. First read the live checkpoint:

```bash
kubectl --context <context> -n <namespace> get configmap <release>-upgrade-checkpoint -o json
```

Record the exact `metadata.resourceVersion`,
`metadata.annotations["curietech.ai/upgrade-holder"]`, and action from that
response. Verify that the process identified by the holder has stopped and that
its Helm action is no longer running. Clearing a live holder can let another
upgrade overlap the original operation. Never delete the checkpoint as a
recovery step because it also contains the resumable lifecycle record.

Only after those checks, replace both values in this conditional patch with the
exact values just observed:

```bash
kubectl --context <context> -n <namespace> patch configmap <release>-upgrade-checkpoint \
  --type=json \
  --patch='[
    {"op":"test","path":"/metadata/resourceVersion","value":"<observed-resource-version>"},
    {"op":"test","path":"/metadata/annotations/curietech.ai~1upgrade-holder","value":"<observed-holder>"},
    {"op":"remove","path":"/metadata/annotations/curietech.ai~1upgrade-holder"},
    {"op":"remove","path":"/metadata/annotations/curietech.ai~1upgrade-action"}
  ]'
```

If either test fails, inspect the ConfigMap again and reassess its current
holder. Do not retry with stale values or remove the annotations
unconditionally.

### `curie cluster down`

```bash
curie cluster down
```

| Flag | What it does |
|---|---|
| `--yes` | Skip the confirmation prompt. |

`curie cluster down` safely removes everything this release created, and
only what it created. It first uninstalls the Helm release, then removes
release-labeled Helm hook Jobs from the release namespace, and finally sweeps
only namespaces bearing this release's ownership pair. Hook cleanup still runs
when the namespace is retained or Helm uninstall reports a failure. An
unlabeled or foreign namespace is retained with a warning, and retained Agent
Sandbox CRDs and the pre-existing shared controller namespace are untouched.
During namespace termination, `cluster down` can still inspect the namespace
and remove matching retained hook Jobs.

A release is identified by its name AND the namespace it was installed into,
and namespace cleanup requires both ownership labels. If you run a second
install of Curie on the same cluster (which normally means two releases
sharing the default name `curie` in different namespaces), tearing one down
never touches the other's namespaces.

It's also safe to re-run if something goes wrong. If the underlying
uninstall fails (say, a brief Kubernetes API-server hiccup), teardown doesn't just
stop -- it keeps going and cleans up whatever it safely can, so you're not
left with orphaned compute. If it still can't finish, the command tells
you exactly what to run next: an exact cleanup command you can copy-paste
once the cluster is reachable again. See ADR-0064 (Architecture Decision
Record; `docs/adr/0064-fail-forward-cluster-teardown.md`) for the full
fail-forward design.

### `curie cluster rollback`

```bash
curie cluster rollback
```

| Flag | What it does |
|---|---|
| `--revision <n>` | Roll back to this exact revision instead of the newest safe one. |
| `--allow-failed-revision` | Permit a `--revision` that Helm never finished applying. |
| `--live-schema-revision <rev>` | Assert the live Alembic revision instead of reading it from the API pod. The schema-window check still runs against this value. |
| `--yes` | Skip the confirmation prompt. |
| `--dry-run` | Print the commands that would run and exit. |

`curie cluster rollback` puts the release back on the newest revision that
Helm actually finished applying.

That is not what a bare `helm rollback` does, and the difference bites on a
cluster without gVisor. `cluster up` tries the install with the chart's
gVisor default first; if the cluster has no `runsc` RuntimeClass, that attempt
is recorded as a **failed** Helm revision before the successful retry with
gVisor off. Do that a few times and the release history alternates
failed/superseded/failed/superseded. `helm rollback` with no revision targets
the immediately preceding revision -- which, on that history, is a failed one:
a manifest Helm never finished putting on the cluster. Rolling back to it does
not restore a working release, it re-applies a broken one.

So this verb reads the history first, skips every revision whose status is not
`deployed` or `superseded`, and rolls back to the newest one that is. It prints
which revisions it passed over, so you can see exactly what a bare
`helm rollback` would have landed on instead.

Status is not the whole story. Every API pod still runs `alembic upgrade head`
at startup, so an older image refuses a live database revision it does not
know. After the status filter, `cluster rollback` reads the live revision from
the running API pod and refuses a target whose declared schema range does not
include it, before Helm mutates the release. The refusal names the
compatibility boundary and the newest safe fail-forward application version.
It does not print database contents or credentials. See issue #2296.

When every API replica is unexecutable (CrashLoopBackOff, Init, or
ImagePullBackOff; the ordinary reason to roll back), that probe cannot run.
`--live-schema-revision <rev>` lets the operator assert the live Alembic
revision (the output of `alembic current` against the release database) so the
schema-window check still runs without a live API pod. An incompatible target
is still refused. This is not a way to skip the window. See issue #2558.

If you know which revision you want, `--revision <n>` takes it. A revision that
isn't in the history is refused, and so is one Helm never finished applying --
unless you also pass `--allow-failed-revision` to say you accept that. If no
revision is safe to roll back to (a first install, or a release whose every
prior revision failed), the command tells you so rather than doing nothing or
rolling back to something broken. See issue #1899 for the original report.

## Deploying your plugin bundle onto the Curie platform

### Manually, with `curie cluster deploy`

This pushes your plugin bundle to the Curie API -- the control-plane component
that `cluster up` installs as part of the release.

```bash
curie cluster deploy --plugin-dir <bundle-dir>
```

| Flag / env var | What it does |
|---|---|
| `--plugin-dir <dir>` | The bundle directory to package and push. |
| `--repo <owner/name>` | Bind this agent to a GitHub repo so pushes deploy it; set only on the deploy that creates the agent and unchangeable after. Omit it and the agent can never use git-flow. |
| `--api-url <url>` / `CURIE_API_URL` | Direct-dial this URL instead of self-plumbing a loopback tunnel. |
| `--api-key <key>` / `CURIE_API_KEY` | Override the auto-discovered API key. |
| `--api-local-port <port>` | Local end of the self-plumbed tunnel. Default `0` lets the kernel assign an ephemeral port, so two deploys never fight over the same one. |

Beyond pointing it at your bundle, `cluster deploy` needs no `--api-url` or
`--api-key` by default: it automatically finds a way to reach the Curie API
and automatically finds the credentials to use, so
`curie cluster deploy --plugin-dir <bundle-dir>` just works. The one flag
you do need is `--repo`, and only if you want git-flow -- see
[Automatically, with git-flow](#automatically-with-git-flow) below.

Under the hood, it opens a secure local tunnel to the Curie API (so
nothing needs to be exposed publicly) and reads the API key straight out
of the release's own Kubernetes Secret -- the key is never printed or
stored anywhere in your shell history. Before posting the bundle, it
also checks the tunnel's unauthenticated `/health` to confirm it really
reaches the Curie API -- a squatted local port or a tunnel that resolved
to the wrong workload both look reachable, so a 404, an HTML response, a
non-`ok` JSON body, or a redirect is refused rather than posted to. This
check only runs on the self-plumbed tunnel; an explicit `--api-url` is
not probed.

Override this only for a non-default setup: `--api-url` to talk to a
specific address instead of tunneling, or `--api-key` to use a specific key
instead of the auto-discovered one. As a safety check, if you point at a
plain `http://` URL, `cluster deploy` refuses to send an auto-discovered
key over it unencrypted -- pass `--api-key` explicitly to confirm that's
what you want, switch to `https://`, or drop `--api-url` to go back to the
safe default.

If something's not working: a discovery failure means the release's Secret
couldn't be read (pass `--api-key` yourself); a tunnel failure usually means
the release isn't healthy (check with `curie cluster status`); and if
nothing's been deployed yet, `curie cluster message` will say so plainly.

### Automatically, with git-flow

Beyond `curie cluster deploy`, a bundle can also deploy automatically on
every `git push`. There are two delivery paths, and only one needs to be
armed: an inbound GitHub webhook (items 2 and 3 below), or commit polling
(`api.commitPollIntervalSeconds`, shipped in #1239), where the API pulls
the repo itself and neither item applies. Polling is the path for a
private or ClusterIP-only install that cannot receive an inbound webhook
at all. `curie cluster deploy --repo` and `curie doctor` (the `Push
delivery` check) now tell you which of these, if any, is actually armed
for a given agent -- configuration evidence, not proof a push has
deployed.

Four things need to be true for a push over the webhook path to actually
promote:

1. **The agent's repo is set.** (This applies to both delivery paths --
   commit polling still needs to know which repo to pull.) The webhook resolves which agent a push
   belongs to by matching the payload's `repo.full_name` (owner/name)
   against that agent's `repo_full_name`. This field is set when the
   agent is created (`curie <tier> deploy --repo owner/name`, or the
   Curie API), and a later `curie <tier> deploy --repo owner/name` binds
   an agent that has none yet. If the agent is already bound to a
   different repository, the deploy declines to rebind it and prints a
   warning naming the repository it kept, so `--repo` never silently
   reroutes which repository's pushes deploy an agent. The match is
   case sensitive, so the stored `repo_full_name` must match GitHub's
   canonical owner and repository casing exactly, or the lookup finds
   no agent, the push is silently ignored, and (unlike a rejection)
   nothing is logged, so the only symptom is a green delivery in GitHub
   with nothing deployed.
2. **GitHub can reach the Curie API.** Add a webhook, in the repo's GitHub
   settings, to `<your-api-url>/github/webhook`. This requires the
   Curie API to be reachable from GitHub's servers (an ingress, a load
   balancer, or a tunnel); how you expose it is an infrastructure decision
   this chart does not make for you.
3. **The webhook secret matches.** GitHub signs each delivery
   (`x-hub-signature-256`), verified against the chart-managed
   `githubWebhookSecret`. Retrieve the generated value from the same Secret
   `cluster deploy` reads its API key from:
   ```bash
   kubectl get secret <release>-secrets -o jsonpath='{.data.githubWebhookSecret}' | base64 -d
   ```
   and paste it into the webhook's secret field.
4. **The push comes from the configured clone origin.** The API derives the
   trusted clone URL from `GITHUB_CLONE_BASE` (chart value
   `api.githubCloneBase`), which defaults to `https://github.com`, and
   rejects any push whose `clone_url` doesn't match with the error code
   `git.origin_mismatch` -- the webhook still returns 200, so this fails
   silently from GitHub's side. The default covers github.com with no extra setup; set
   `GITHUB_CLONE_BASE` (or the chart's `api.githubCloneBase`) if your repos
   live elsewhere, such as GitHub Enterprise Server.

### Accepting review feedback from GitHub

Review feedback uses the same signed `/github/webhook` endpoint, but is a
separate, default-off GitHub App path. Set
`api.githubReviewIngressEnabled: true` (environment
`GITHUB_REVIEW_INGRESS_ENABLED=true`) only after all of the following are
configured:

- `api.githubReviewReconcilerIntervalSeconds` is greater than zero (environment
  `GITHUB_REVIEW_RECONCILER_INTERVAL_S`; the default is `5`).
- `api.githubAppId` and an App private key are present. Prefer
  `api.githubAppExistingSecret` and `api.githubAppExistingSecretKey` for the
  key; `api.githubAppPrivateKey` is the inline alternative.
- `api.githubWebhookSecret` is a non-default HMAC secret and matches the secret
  configured on the GitHub webhook.

The API refuses to start with review ingress enabled when any of those settings
is missing, the webhook secret is still the development default, or the
reconciler interval is not positive. Leaving
`api.githubReviewIngressEnabled: false` keeps review events inert while
preserving the existing push-webhook behavior.

If the worker uses a custom `KEY_PREFIX`, give the API the same `KEY_PREFIX` so
review reconciliation can find its exact completion and dead-letter markers.
The API follows the worker configuration's `KEY_PREFIX` alias;
`CURIE_KEY_PREFIX` is ignored.

Configure the App webhook with these three subscriptions, using GitHub's event
names exactly:

- **Issue comments** for `issue_comment.created` on a pull request.
- **Pull request review comments** for `pull_request_review_comment.created`.
- **Pull request reviews** for `pull_request_review.submitted`; Curie acts only
  on `commented` and `changes_requested` reviews.

GitHub documents the payloads under [webhook events and
payloads](https://docs.github.com/en/webhooks/webhook-events-and-payloads).
Give the App **Issues: Read** and **Pull requests: Read** so Curie can re-read
the pull request, issue comment, review comment, and review. Give it
**Administration: Read** so Curie can call GitHub's [repository-permission
lookup](https://docs.github.com/en/rest/collaborators/collaborators#get-repository-permissions-for-a-user).
That lookup must freshly match the sender's immutable user ID and report
`write` or `admin`; `author_association` by itself is not authority. Keep the
existing trusted publisher permissions, **Contents: Read and write** and **Pull
requests: Read and write**, because only that publisher may advance the branch
and pull request. The App's effective Pull requests permission is therefore
Read and write when both feedback ingestion and publication are enabled.

Only publication lineages created after authority capture are eligible. The
lineage must retain its immutable App-observed installation, repository, pull
request, base-ref, binding generation, and bare reply-conversation facts.
Historical lineages, PAT-backed lineages, and lineages with null authority are
not reconstructed and remain ineligible. Every accepted event routes to that
exact owning conversation. Immediately before a model turn, Curie rechecks the
open pull request, exact head, installation, repository, sender identity and
current `write`/`admin` permission, then reserves the same lineage generation.

Feedback never publishes automatically. The resulting revision must request a
new human publication approval, and the trusted publisher may add exactly one
tested commit to the same pull request only after that approval. This path
reports receipt and outcome in the owning conversation. GitHub status or
comment publication for review feedback is not configured and remains a no-op.

**Deploying a PRIVATE repo needs one more thing: a clone credential.**
Without it, git-flow can only deploy a public repository. A private one
fails with `git.archive_failed` (#1058). Supply the API's GitHub credential
with `--github-token` on `curie cluster up` (or, to keep it out of your
shell history and `ps`, the `CURIE_GITHUB_TOKEN` environment variable
instead; see the flag reference above for how it's kept out of the helm
command line too). A later plain `cluster up` that passes neither preserves
whatever the release already has, so you only set it once. Changing or
clearing it (`--clear-github-token`) does not restart the API pod
automatically; `cluster up` prints the exact restart command to run, and
until you run it the API keeps using the old value.

On the commit-polling lane, a repeated `git.archive_failed` for the same
commit now backs off geometrically -- five minutes, then ten, twenty,
forty, to a one-hour ceiling -- instead of re-cloning every poll interval,
and after three consecutive failures the API logs an error saying deploys
from that repository are NOT happening (#1309).

Once wired, a push to the agent's dev branch builds and deploys under its
dev bot identity; a push or merge to its prod branch promotes that same
built artifact without rebuilding.

### Admitting a labelled GitHub issue

Factory intake uses the same signed `POST /github/webhook` endpoint and is off
until `api.githubFactoryIngressEnabled` is true (environment
`GITHUB_FACTORY_INGRESS_ENABLED=true`). The API refuses to start with that gate
on unless the GitHub App id and private key are set, the webhook secret is not
the development default, `api.githubFactoryLabel` (`GITHUB_FACTORY_LABEL`) is a
single label name, `api.githubFactoryMention` (`GITHUB_FACTORY_MENTION`) is one
GitHub login, and `api.githubRepoAllowlist` is non-empty.

Bind the agent with a `github` channel whose address is the repository
`owner/name`. No Slack binding is required. The configured label is only the
initial admission convention. A later bounded execution requires a new issue
comment that explicitly mentions that login and whose sender currently has
write or admin permission. Ordinary comments, edits, and events sent by the
App do not execute work. Removing that label or closing the issue cancels
waiting work and requests termination of a running execution. Cancellation
stays requested until the runtime reports that it stopped. An already linked
pull request stays linked, and later publication is refused.

Subscribe the App webhook to **Issues** and **Issue comments** in addition to
the review subscriptions when both gates are on. Give the App **Issues: Read and write**
so Curie can re-read the issue and post one final comment, and **Metadata: Read** is already
implied by repository installation discovery.

Give the App **Checks: Read** so the work item detail can report CI for the
published head. Without it, CI reports `unavailable` / `github_forbidden` and
nothing else changes.

### The default factory agent

Curie ships its factory agent as the bundle in
[`examples/dark-factory`](../examples/dark-factory/README.md). Deploy it as the
agent `dark-factory` bound to the repository, on the default factory model
`z-ai/glm-5.3-flash` (`agentSandbox.runner.model`). It is one agent with one skill: it reads the
issue by link, pins the acceptance criteria, plans, writes a failing test where
one is feasible, implements, runs the repository's own checks, reviews its diff
against every criterion, and ends in one pull request or a stated reason. Any
other bundle can take its place; the platform does not require this one.

The bundle reads the issue through the GitHub MCP server the runner image
preinstalls, with its own `GITHUB_PERSONAL_ACCESS_TOKEN` bound at deploy
(`curie cluster deploy --secret GITHUB_PERSONAL_ACCESS_TOKEN`). Give it a token
limited to **Issues: Read**. Its `toolPolicy` allows only `github/get_issue`, so
the runner denies every GitHub write tool. Open runner egress to the GitHub API
CIDRs (`agentSandbox.connectorEgress.<agent>`), and raise
`worker.deliveryBudgetSeconds` and `worker.runnerTotalTimeoutSeconds` to 1800
so the execution deadline, not the 600 s default, bounds a run. Whether a run
executes the repository's tests is the bundle's instruction. The platform does
not check it.

### Factory work items wait for capacity

Factory execution waits in PostgreSQL rather than on the runs-stream pending
list. Admission, acquire, start, heartbeat, finish, and termination are internal
worker-token routes under `/v1/internal/work-items`. The API lifespan reconciler
publishes execute and terminate wakes onto `curie:runs`.

The knobs are `CURIE_WORK_ITEM_*` on the API (settable through `api.extraEnv`
until chart-owned values land):

| Variable | Default | Meaning |
|---|---|---|
| `CURIE_WORK_ITEM_RECONCILER_ENABLED` | `true` | Lifespan task off-switch |
| `CURIE_WORK_ITEM_RECONCILER_INTERVAL_SECONDS` | `5` | Pass interval |
| `CURIE_WORK_ITEM_BATCH_LIMIT` | `50` | Due rows claimed per pass |
| `CURIE_WORK_ITEM_WAIT_BUDGET_SECONDS` | `86400` | Waiting deadline from admission |
| `CURIE_WORK_ITEM_DISPATCH_LEASE_SECONDS` | `30` | Reconciler publish lease |
| `CURIE_WORK_ITEM_ACQUIRE_LEASE_SECONDS` | `300` | Worker acquire lease |
| `CURIE_WORK_ITEM_RUNTIME_TTL_SECONDS` | `45` | Runtime heartbeat expiry; interval is ttl / 3 |
| `CURIE_WORK_ITEM_CANCEL_SETTLE_SECONDS` | `120` | A cancellation with no worker teardown receipt settles as cancelled after this |
| `CURIE_WORK_ITEM_BACKOFF_BASE_SECONDS` | `10` | Defer backoff base |
| `CURIE_WORK_ITEM_BACKOFF_MAX_SECONDS` | `120` | Capacity defer backoff cap |
| `CURIE_WORK_ITEM_TERMINATE_RETRY_SECONDS` | `30` | Terminate wake republish window |
| `CURIE_CONSUMER_GROUP` | `curie-workers` | Runs consumer group the reconciler ensures |

There are two time bounds after start: the ExecutionRequest deadline (1800 s)
and the worker delivery budget (`worker.deliveryBudgetSeconds`, default 600).
The runner request is bounded by the smaller of the two remaining times. A
default install therefore fails a work item at 600 s (`deadline_halted`) unless
operators raise the delivery budget for factory agents.

Capacity wait expiry is visible as `expired` / `capacity_wait_expired` on
`GET /v1/internal/work-items/requests/{id}`. It is not written to the
dead-letter graveyard.

A labelled factory run posts exactly one final comment on the originating
issue. When publication succeeds, that comment names the exact pull request
URL. When the run cannot complete, the comment starts with `Could not complete:`
and a plain sentence for the cause. When the model provider refused the run,
a `Provider message:` line follows with the provider's own error text, redacted
of keys and tokens. A last `Cause:` line names the platform cause code
(`capacity_wait_expired`, `execution_deadline`, `issue_cancelled`,
`owner_lost`, `runner_escalated`, `runner_failed`, `no_pull_request`,
`publication_denied`, `publication_expired`, `publication_failed`, or a
classified run failure: `model_credit_exhausted`, `model_credential_rejected`,
`model_rate_limited`, `model_error`, `budget_exceeded`, `runner_timeout`, or
`workspace_error`). A model provider that answers HTTP 402 or reports exhausted
credits ends the run as `model_credit_exhausted` without retrying. The
work item reconciler posts the comment after the terminal row and any
publication lineage commit. A refused post is recorded on the notice and does
not change the execution row. Waiting for approval is not an ending: the
execution deadline stays 1800 seconds from start and covers that wait.

Review feedback on a factory pull request asks for one more revision of that
pull request. When a work item owns the PR, an `issue_comment`,
`pull_request_review_comment` or `pull_request_review` that mentions
`api.githubFactoryMention` goes to the factory, not to the Slack-bound review
path. The sender must have write access now, the PR must still be open at the
recorded head, and the delivery must come from the work item's installation.
Each refusal is a `factory_ignored` code (`ordinary_comment`,
`lineage_unbound`, `lineage_closed`, `installation_mismatch`,
`sender_permission_refused`, `terminal_pull_request`, `active_request`, and
others). An accepted mention becomes the work item's next execution request.
The first line of that request's objective is the same-repository feedback
URL, which keeps the existing issue, pull request, or review thread as the
reply target.
When that revision completes, or ends early, the reconciler answers on the pull
request: in the review thread for an inline comment, otherwise as a PR comment
that links the feedback. If GitHub refuses the thread reply with 422, the answer
is posted as a linked PR comment. PR review feedback reaches the factory only
when `api.githubFactoryIngressEnabled` is true. A PR no work item owns keeps
the existing review behavior.

A factory run on a `github` binding emits no booting, partial, or final chat
text. The worker acknowledges runtime output locally. The one reconciler
comment on a fresh issue is the run's only GitHub response, with the pull
request URL included when publication succeeds, so a `github` binding needs no
endpoint or adapter.

### Driving the factory end to end

`curie dev factory-e2e preflight` proves the signed intake loop against a real
GitHub App and a fixture repository, on a kube context you name. It needs a
source checkout, `kubectl`, `helm`, `openssl` and `cloudflared`.

1. Installs the candidate commit's published `sha-<commit>` images from that
   commit's chart into a namespace it creates (`test-factory-<commit>` by
   default, or `--namespace test-factory-<slug>`). If the cluster already runs
   the agent-sandbox controller, the install is consumer mode.
2. Turns on factory intake, binds one agent to the fixture repository, deploys
   the default factory bundle (`examples/dark-factory`) onto it with a
   short-lived installation token limited to Issues: Read as the bundle's
   GitHub credential, sets the agent's publication policy to `auto`, exposes
   the api through a temporary cloudflared quick tunnel, and points the App
   webhook at it with an App JWT.
3. Resets the fixture repository by closing its issues and pull requests and
   deleting every branch except the default. It then opens one labelled issue.
4. Passes when GitHub's delivery log shows that the `issues.labeled` delivery
   got HTTP 200 and `factory_admitted`, and
   `GET /v1/internal/work-items/requests/{id}` returns the admitted WorkItem.

On every exit it restores the webhook URL, resets the fixture again, stops the
tunnel, and deletes the namespace and its publication namespace. Each undo is
verified. The JSON evidence file (default `target/factory-e2e/<namespace>.json`)
records the candidate commit, namespace, delivery id, WorkItem id and every
teardown result. Teardown that cannot be verified fails the run.

Every identity is an operator input. Nothing names a specific App or account:

| Variable | Meaning |
|---|---|
| `CURIE_FACTORY_KUBE_CONTEXT` | Kube context (or `--context`); required |
| `CURIE_FACTORY_APP_DIR` | Directory holding `app.json` (`id`, `slug`, `installation_id`, optional `repo`), `app.pem` and `webhook_secret` |
| `CURIE_FACTORY_APP_ID`, `CURIE_FACTORY_INSTALLATION_ID` | Override `app.json` |
| `CURIE_FACTORY_APP_PRIVATE_KEY_FILE`, `CURIE_FACTORY_WEBHOOK_SECRET_FILE` | Override the files in the App directory |
| `CURIE_FACTORY_REPO` | Fixture repository `owner/name` |
| `CURIE_FACTORY_ACTOR_TOKEN` or `CURIE_FACTORY_ACTOR_GH_USER` | A human account with write access that opens the issue (a `gh` login for the second) |
| `CURIE_FACTORY_LABEL`, `CURIE_FACTORY_MENTION` | Intake label (default `curie-factory`) and mention login (default the App slug) |
| `CURIE_FACTORY_PRIORITY_CLASSES` | `<platform>,<sandbox>`: reuse existing PriorityClasses instead of creating them |
| `CURIE_FACTORY_WEBHOOK_RESTORE_URL` | URL to leave on the App webhook (default: the URL found at start) |
| `CURIE_FACTORY_CLOUDFLARED` | cloudflared binary (default `cloudflared` on PATH) |
| `CURIE_FACTORY_CURIE_BIN` | `curie` binary that deploys the bundle (default `curie` on PATH) |
| `CURIE_FACTORY_BUNDLE_DIR` | Bundle to deploy (default `examples/dark-factory`) |
| `CURIE_FACTORY_MODEL_API_KEY` | Model credential. Set, the install runs a real model with the worker budget raised to 1800 s; unset, the model is fake |
| `CURIE_FACTORY_MODEL` | Model name (default `z-ai/glm-5.3-flash`) |

A missing input is refused, with every missing name listed, before the cluster
or GitHub is touched. `curie dev factory-e2e run --scenario <name>` runs the
preflight and then one scenario driver: `issue-to-pr`, `revision`,
`cancel-waiting`, `cancel-running` or `evaluation`. `evaluation` runs six
labelled tickets (a correct change, a seeded failing test, an ambiguous
request, an unavailable dependency, an execution-deadline budget, and a
malicious instruction) on the configured model and again on
`CURIE_FACTORY_REFERENCE_MODEL` (default `anthropic/claude-sonnet-4.5`, same
credential), then one authorized same-PR revision and label removal of one
waiting request and one running request. After the waiting cancellation it
raises the sandbox pod quota to the chart default with `helm upgrade
--reuse-values`, so the per-agent sandbox warm pool survives. The sandbox sets `CLAUDE_CODE_DISABLE_TERMINAL_TITLE` so the model session
does not die while titling itself. A request that
has not started, a delivery the tunnel rejected, or a run that escalates in
the first few seconds is cancelled and opened again. A refusal still has to
end as `no_pull_request`, and the budget case still has to end as
`execution_deadline`. The case is given up well before the hour-long
never-started cap. Hidden checks run
against each resulting pull request and are not part of the ticket. The JSON evidence
includes the candidate commit, each verdict, configured and observed model,
usage or an explicit unverified record, and elapsed time. The command exits
non-zero when any of those fields is missing or any verdict is not passed.

`run --scenario issue-to-pr --issue-file <ticket.md> [--expect pr|comment|any] [--expect-cause <cause>]... [--expect-reason <regex>]...`
opens the ticket (first line is the title, the rest the body) as the one
labelled issue and waits for the marked final issue comment, even after the
execution row and pull request are ready. It fails unless the run posted
exactly one such comment inside the execution bound, with at most one pull
request, no `.github/` file or credential-shaped string in the diff, and the
default branch unmoved. `--expect pr` requires a pull request, `--expect
comment` requires no pull request, and `--expect any` accepts either. A success
comment must name the exact opened pull request URL anywhere in its body. A
failure comment must contain `Could not complete:` followed by an explanation.
Its cause must be one the run accepts: each `--expect-cause` given, or by
default `no_pull_request` for `--expect comment` and `no_pull_request` or
`execution_deadline` for `--expect any`. When `--expect comment` or `--expect
any` accepts a `no_pull_request` ending, the agent's final transcript reply must
separately contain `Could not complete:` followed by its reason. Each
`--expect-reason` must match that transcript reply, ignoring case. The platform
cause does not establish the agent's reason. Only a comment the App posted with
this run's execution request marker counts, and more than one fails. A rename out of
`.github/` fails like a change inside it. A known secret or credential-shaped
string in the pull request's title, body, diff, file names, final comment, or
agent reply fails the run. The comment, agent reply, and the pull request's
title, body, and file names are recorded with such strings replaced by
`[REDACTED]`.
A pull request ending also fails unless the WorkItem row's own
`publication_lineage_id` is set and its lineage records that pull request; the
read route's conversation fallback does not count, so the driver reads the
install's Postgres. Every outcome also clears the final notice's delivery
record and waits for the reconciler to record it again: it must record the
original comment by its marker, and the issue must still carry exactly one.
Elapsed time runs from the request's start to the final comment.
The evidence records the work item state and ending cause, the pull request
and its changed files, the final comment, the agent's final transcript reply
and its source, CI, elapsed and execution time, the configured model, and the
model spend
as the OpenRouter key's usage delta (or `unverified`). The key is shared, so
that delta includes any concurrent use of it.

`run --scenario revision --issue-file <ticket.md> [--revision-file <text.md>]`
needs the ticket's run to open a pull request. It then posts an ordinary
comment on that pull request, which must be delivered as `factory_ignored`
and add no request within 60 s. Next it posts a mention comment (the revision
file's text, or by default a request for a docstring and a test, prefixed
with `@<mention>` when absent). It passes when that delivery is
`factory_admitted`, the revision request belongs to the same WorkItem, the
WorkItem ends with two requests and the second `completed`, the same single
pull request gained a commit and a new head, exactly one App reply carries the
revision's marker and links the mention, no other App comment followed the
ordinary one, and the default branch is unmoved. Before posting feedback, the
driver also requires the initial run's one final issue comment to name that
pull request.

`run --scenario cancel-waiting` installs with the sandbox pod quota set to 0,
so every sandbox claim is refused and the request waits on capacity. Once it
shows a capacity deferral and no start, the driver removes the label. It
passes when the `issues.unlabeled` delivery is `factory_cancelled` and the next
read shows the request `cancelled` with cause `issue_cancelled`, never
`running` or `cancellation_requested`, and no pull request.

`run --scenario cancel-running [--issue-file <ticket.md>]` (by default a
multi-step ticket that keeps the run busy for minutes) removes the label as
soon as the request is `running`. It passes when the delivery is
`factory_cancellation_requested`, the request then reads `cancelled` with
cause `issue_cancelled` (a missed `cancellation_requested` read is covered by
the delivery status), and after a 180 s quiet window there is no pull
request, no WorkItem pull request, no published publication, no new branch,
at most one terminus comment with cause `issue_cancelled`, and the default
branch is unmoved.

`revision` and `cancel-running` need `CURIE_FACTORY_MODEL_API_KEY` and refuse
before installing without it; `cancel-waiting` runs with either model. Each
of the three runs `curie cluster work-items <id> --json` at every state it
judges and requires exit 0 with the api's state and request statuses, and
requires exit 1 for an unknown id. The evidence records every id, delivery,
comment, pull request head, status seen with its time, and CLI read.

### Reading work item outcomes

Operators read factory work through two read-only routes behind the platform
API key: `GET /work-items` (optional `agent_id`, `limit` 1 to 200, default 50,
newest update first, with a `truncated` flag) and `GET /work-items/{id}`
(optional `agent_id`). An unknown id and an id owned by another agent return
the same 404 body. Both responses carry `Cache-Control: no-store`.

Each item carries its issue link, its pull request (from the conversation's
publication lineage), its publication and approval status, every execution
request, a `state` and an `actionable_cause`. The states:

- `waiting`: admitted and waiting for sandbox capacity. The cause names
  capacity deferrals and says when the waiting deadline has elapsed but the
  reconciler has not yet expired the request.
- `running`: started, bounded by the 1800 s execution deadline.
- `cancellation_requested`: termination requested (`issue_cancelled`,
  `execution_deadline` or `owner_lost`) and awaiting a runtime observation.
- `cancelled`: the issue label was removed or the issue was closed. A pull
  request already opened is kept.
- `expired`: `capacity_wait_expired` or `execution_deadline`.
- `failed`: the cause names the terminal cause verbatim. For
  `deadline_halted`, raise `worker.deliveryBudgetSeconds`.
- `awaiting_approval`: a publication approval or a tool approval on the same
  conversation is pending.
- `publishing`: the publication is approved and in flight.
- `published`: a pull request is open for the work.
- `completed_unpublished`: the run completed with no publication, or its
  publication was denied, expired or failed.

The platform never asserts correctness: every item reports
`correctness: {"asserted": false, "owner": "bundle"}`. Verification belongs
to the bundle.

The detail route observes CI live for the pull request head and never stores
it. `ci.state` is `passing`, `failing`, `pending`, `none` (no check runs),
`not_applicable` (no pull request) or `unavailable` with a fixed `reason`:
`no_head_sha`, `app_not_configured`, `installation_refused`,
`github_unauthorized`, `github_forbidden`,
`github_not_found`, `github_rate_limited`, `github_error`, `timeout`,
`malformed_response`, `too_many_check_runs` or `observation_busy` (every
concurrent credential slot is held by an in-flight mint). The list route reports
`ci: null` and never calls GitHub.

The CLI reads the same routes: `curie cluster work-items [ID] [--agent
NAME_OR_ID] [--json]` and `curie local work-items`. Exit codes are 0 on
success, 1 for not found or refused, 2 for invalid input and 3 when the API
is unreachable. The skill tier has no work items and exits 4.

## Talking to your agent

The plugin bundle you just deployed is the agent's backend. There are two
frontends that can talk to it: your terminal (no Slack involved) or a real
Slack workspace.

### Driving the deployed agent

```bash
curie cluster message "hello, are you there?"
```

| Flag | What it does |
|---|---|
| `--continue` | Reuse the same conversation thread as your last `cluster message` call. |
| `--thread <id>` | Continue a specific earlier conversation thread by ID, instead of the most recent one. |

When the release has no dispatcher, this exercises it end to end from the
terminal. It:

- simulates the exact Slack event your bot would receive
- runs it through the real deployed worker and a real Kubernetes sandbox
- prints the reply in the terminal

When the release has a dispatcher connected to Slack, `cluster message` posts a
placeholder and routes the reply to the agent's bound Slack channel. When no
dispatcher is connected, it uses the terminal reply stub and prints the reply
in the terminal. The command handles port-forwards and channel resolution
itself, so none of that is something you need to set up. `--continue`
reads its saved context from `.curie/last-turn.json` in the current <!-- doclint:ignore-line -->
directory.

This lets a developer iterate on an agent built for someone else's
workspace with no Slack access. Full flag reference is in
[`cli/README.md`](../cli/README.md).

### Connecting Slack

```bash
SLACK_APP_TOKEN=xapp-... \
SLACK_BOT_TOKEN=xoxb-... \
curie cluster comms --slack
```

| Flag | What it does |
|---|---|
| `--disconnect` | Disconnect Slack and revert to CLI-driven testing. |
| `--dry-run` | Print the masked `helm` command without executing (env-backed token values are masked, never printed in full). |

`curie cluster comms --slack` wires your release up to a real Slack
workspace: it stores the tokens you pass and restarts the affected pods so the
change takes effect immediately. Connected `cluster message` replies go to the
agent's bound Slack channel; disconnected releases use the terminal stub.
Exactly one Curie release may connect to a given Slack app. Slack Socket Mode
fans events across every connected client, so two releases sharing one app
silently split mentions. Use a dedicated Slack app for this release; do not
share it with a local dispatcher or another cluster install.

For the `local`-target equivalent (`curie local comms --slack`), see
[`cli/README.md`](../cli/README.md).

### Connecting email

There is no `curie cluster comms --email` yet, so email is wired with a private
Helm values file. The mail adapter ships off by default
([`apps/mail-adapter`](../apps/mail-adapter)).

After email is configured, a plain `curie cluster up` preserves its recorded
settings, PVC configuration, and all three credential references together with
`worker.adapterCredentialsExistingSecret` and its key. Inline credentials on
older installs are also retained through the protected values-file path. An
explicit `--set mailAdapter.deploy=false` disables it; clearing an external
credential reference does not restore a stale inline credential. A nonempty
inline credential replaces its retained external reference; an empty inline
clear leaves the external source active. An empty worker credential map also
leaves its external source active. Changing the adapter's egress source while
the worker uses an external credential map requires an explicit paired worker
source decision; the CLI refuses an unpaired change before Helm runs. Restating
the worker's Secret name or key acknowledges a pairing updated inside that
Secret. The CLI checks this explicit decision, not equality of opaque credentials.

Two platform-side steps come first, in this order:

1. **Bind the agent** to `{"kind": "email", "address": "<the inbox address>"}` with a
   reply route: `endpoint` is the in-cluster Service the chart renders,
   `http://<fullname>-mail-adapter:<mailAdapter.service.port>/`, and `adapter` is
   `mail-adapter`. Neither half of that is a literal. `<fullname>` is the chart's
   `curie.fullname` ([`charts/curie/templates/_helpers.tpl`](../charts/curie/templates/_helpers.tpl)):
   it is the release name alone when the release name already contains `curie`, and
   `<release>-curie` otherwise, so release `curie` renders `curie-mail-adapter` while
   release `acme-bot` renders `acme-bot-curie-mail-adapter`. The port is
   `mailAdapter.service.port` (default `8080`), not a fixed `8080`. Getting either
   wrong points the reply route at nothing, and every completion retries and then
   dead-letters. Read both off your own release instead of deriving them:

   ```bash
   kubectl get svc -n <ns> \
     -l app.kubernetes.io/instance=<release>,app.kubernetes.io/component=mail-adapter \
     -o jsonpath='http://{.items[0].metadata.name}:{.items[0].spec.ports[0].port}/'
   ```

   The `adapter` value must equal `mailAdapter.adapterSlug`, because the worker looks
   its egress credential up under that key.
2. **Mint the channel token.** `curie cluster channel-token <agent> --kind email --address <inbox>`
   mints through `POST /channels/token` with the platform key, writes the token
   into the Secret the adapter actually reads (the chart Secret, or
   `mailAdapter.channelTokenExistingSecret` when that is set), rolls the adapter,
   and prints `exp`. Each mint bumps the binding's generation, so a remint
   revokes the previous token. It never prints the token and never writes it through Helm
   values, so `helm get values` cannot undo the rotation. `--show-exp` reports
   the installed token's `exp` and whether the platform still accepts it, the
   same observation `curie doctor` uses. The mint refuses with 409 for a
   non-`slack` binding that has no reply route, which is why the binding comes
   first.

Then turn the adapter on. Keep all three credentials out of `--set`, Helm
values, and release history by having a secret manager materialize an
operator-managed Kubernetes Secret before the install or upgrade. One Secret
can carry the adapter's three keys plus the worker's JSON credential map. The
map's `mail-adapter` entry must be generated from the same egress-secret source;
do not copy the value by hand. The checked-in, non-secret values file then
contains only references:

```yaml
mailAdapter:
  channelTokenExistingSecret: curie-mail-credentials
  channelTokenExistingSecretKey: channel-token
  egressSecretExistingSecret: curie-mail-credentials
  egressSecretExistingSecretKey: egress-secret
  agentmail:
    apiKeyExistingSecret: curie-mail-credentials
    apiKeyExistingSecretKey: agentmail-api-key
worker:
  adapterCredentialsExistingSecret: curie-mail-credentials
  adapterCredentialsExistingSecretKey: adapter-credentials
```

The referenced Secret keys carry, respectively, the scoped channel token, the
shared adapter egress credential, the AgentMail API key, and a JSON object that
maps the configured `mailAdapter.adapterSlug` to that same egress credential.
All `secretKeyRef`s are non-optional: a missing Secret or key prevents the pod
from starting instead of falling back to an empty or chart-held value.

The non-secret `values.yaml` contains the switch, inbox, allowed senders, and
network destination. Kubernetes NetworkPolicy cannot authorize an FQDN, so use
the provider's current HTTPS CIDRs or point `agentmail.baseUrl` at a controlled
egress proxy with a stable CIDR:

```yaml
mailAdapter:
  deploy: true
  inbox: agent@yourdomain.example
  allowedSenders: [alice@example.com, example.com]
  agentmail:
    baseUrl: https://api.agentmail.to/v0
    httpsCidrs: [203.0.113.0/24] # placeholder; replace from your provider/proxy
```

In the default `egressMode: cidrs`, an empty `mailAdapter.agentmail.httpsCidrs`
refuses to render when the adapter is enabled. Prefix-0 and prefix-1 routes refuse to render, including IPv4 or IPv6
split default routes; surrounding whitespace and expanded IPv6 spelling do not
bypass that gate. Use narrow current provider or controlled-proxy ranges.

**`/32` pins for AgentMail will break.** `api.agentmail.to` is fronted by
CloudFront, which moves the name between edge addresses without notice. A list of
`/32` entries resolved at install time is correct only until the next move; after
that every HTTPS open from the adapter pod is refused and mail stops (seen on
2026-09-18, #2824). The same applies to any CDN-fronted API. Pick one of:

- **Published ranges.** Declare the provider's published ranges instead of
  resolved addresses. For CloudFront that is every `CLOUDFRONT` prefix in
  <https://ip-ranges.amazonaws.com/ip-ranges.json>, which AWS keeps current;
  re-render when that file changes. Wide, but still limited to the CDN.
- **A controlled egress proxy** with a stable address, with
  `agentmail.baseUrl` pointing at it.
- **`egressMode: publicHttps`.** The policy admits TCP 443 to any public address
  and still denies private, loopback, link-local (cloud metadata), CGNAT,
  multicast, documentation and reserved ranges, plus anything in
  `publicHttpsExcept`. Pod, Service and node ranges in public address space
  (dual-stack IPv6 pod and Service CIDRs on EKS, GKE and AKS, GKE public pod
  ranges, public node IPs) are not denied by default; list them there. The adapter then dials whatever DNS returns. This survives any CDN
  move. The trade-off is that the credential-bearing mail pod can open HTTPS to
  any public host, not only AgentMail. `httpsCidrs` must be empty in this mode.

```yaml
mailAdapter:
  agentmail:
    egressMode: publicHttps
    publicHttpsExcept: [] # add public-space pod, Service and node ranges
```

NetworkPolicy has no FQDN peer. CNIs that add one (Cilium `toFQDNs`, Calico
DNS policy) can express "only api.agentmail.to" directly; the chart does not
render those CRDs, so apply one alongside the release if your CNI supports it.

For a bring-your-own platform API, declare the URL and its NetworkPolicy peer
independently; the chart cannot safely infer IP ranges from a hostname:

```yaml
api:
  deploy: false
ui:
  apiBaseUrl: https://api.example.com:8443
mailAdapter:
  apiBaseUrl: https://api.example.com:8443
  apiEgress:
    httpsCidrs: [198.51.100.0/24] # placeholder; use the real narrow API range
    port: 8443
```

| Value | What it does |
|---|---|
| `mailAdapter.deploy` | Renders the Deployment and Service. Default `false`; nothing about email exists in a default install. |
| `mailAdapter.inbox` | The AgentMail inbox this adapter polls and replies from. |
| `mailAdapter.pollIntervalSeconds` | Seconds between polls of that inbox (default `5`). Zero or negative fails the boot gate rather than tight-looping a third-party API. |
| `mailAdapter.maxPendingDeliveries` | Maximum unresolved inbound rows (default `1000`). At capacity new mail stays unclaimed at AgentMail rather than evicting accepted work. |
| `mailAdapter.maxBodyBytes` / `maxReplyBytes` / `maxStateBytes` | Allocation and SQLite page bounds. Size the PVC above `maxStateBytes` for the WAL and filesystem overhead. |
| `mailAdapter.allowedSenders` | Who may start a turn. Empty denies everyone, and with ingress on the pod refuses to boot rather than run an inbox that answers nobody; `*` is the explicit allow-all. |
| `mailAdapter.ingressEnabled` | `false` serves egress while sending nothing inbound. That is the staged-cutover position while the platform side of a new binding is being wired. |
| `mailAdapter.egressSecret` | The shared secret the worker presents on `X-Curie-Adapter-Secret` and the adapter checks before any side effect. |
| `mailAdapter.channelTokenExistingSecret` / `channelTokenExistingSecretKey` | Source the scoped channel token from an operator-managed Secret instead of the chart Secret (default key `mailChannelToken`). |
| `mailAdapter.egressSecretExistingSecret` / `egressSecretExistingSecretKey` | Source the adapter's egress credential externally (default key `mailEgressSecret`). This requires `worker.adapterCredentialsExistingSecret` to supply the paired worker map. |
| `mailAdapter.agentmail.apiKeyExistingSecret` / `apiKeyExistingSecretKey` | Source the AgentMail API key from an operator-managed Secret instead of the chart Secret (default key `mailAgentmailApiKey`). |
| `mailAdapter.agentmail.httpsCidrs` | Required provider/proxy destination CIDRs on TCP 443. The mail pod's egress policy otherwise allows only DNS and this release's API pods. The adapter dials only addresses these CIDRs admit, pinning to `/32` entries when DNS rotates to an edge outside the list (TLS is still verified against the hostname) -- this is what kept a CDN-fronted provider from being refused when its DNS rotated (#2731). Resolved `/32` pins for a CDN-fronted API still break when the CDN moves; see above (#2824). Must be empty when `egressMode` is `publicHttps`. |
| `mailAdapter.agentmail.egressMode` / `publicHttpsExcept` | `cidrs` (default) allows TCP 443 only to `httpsCidrs`. `publicHttps` allows TCP 443 to any public address with special-purpose ranges and `publicHttpsExcept` denied, trading provider-only egress for surviving CDN moves. List public-space pod, Service and node ranges in `publicHttpsExcept`. |
| `mailAdapter.discoveryUnreadyAfterSeconds` | Continuous discovery-failure seconds after which readiness goes `503` (default `120`); liveness is unaffected. The Service still publishes the pod's address while unready, so reply/completion deliveries from the worker keep routing through a discovery outage; readiness going 503 is the operator signal, not a routing cutoff. |
| `mailAdapter.apiEgress.httpsCidrs` / `port` | Required narrow destination peers when `api.deploy=false`; default port `8000`. Ignored for the in-chart API, whose pod selector and service port are used instead. |
| `mailAdapter.otelEgress.httpsCidrs` / `port` | Required narrow destination peers when the release's effective OTLP endpoint is external (`otelCollector.deploy=false` with `otelCollector.endpoint` set); the render is refused without it. `port` is optional and derives from the endpoint URL. Ignored for the in-chart collector, whose pod selector is used instead. |
| `mailAdapter.persistence.size` / `storageClass` | Chart-managed RWO SQLite PVC. The default size is `1Gi`; empty storage class inherits `global.storageClass` and then the cluster default. |
| `mailAdapter.persistence.existingClaim` | Mount an existing same-namespace RWO Filesystem PVC instead of rendering one. An install/upgrade hook checks the exact claim before replacing the pod. |

On the chart-managed path, do not write
`worker.adapterCredentials.mail-adapter` by hand. The chart derives it from
`mailAdapter.egressSecret`, accepts an equal migration value, and refuses a
conflict. Changing any of the three plain mail credential values and running
`helm upgrade` changes the adapter pod-template checksum; changing the egress
value also changes the derived worker map and rolls the worker.

On the external path, `mailAdapter.egressSecretExistingSecret` requires
`worker.adapterCredentialsExistingSecret`. The chart neither derives the mail
entry nor compares it with the unused plain egress value: the two referenced
Secret keys are the authority. Rotate both representations from one source,
then run `helm upgrade` so the adapter hashes the live referenced data and
recreates its pod. The worker reads its external JSON map only at pod start and
its checksum tracks the reference, not same-Secret data changes, so restart the
worker after an in-place external rotation with `kubectl -n <ns> rollout
restart deployment/<release>-worker`. A source name/key change through Helm
rolls both consumers through their source-reference checksums. The one
`Recreate` adapter replica reopens the same SQLite file and resumes pending
work. The adapter cannot mint a replacement channel token because it
deliberately holds no platform key.

The chart Secret contains a mail key only while that field is chart-managed;
setting its `existingSecret` omits the key so later upgrades cannot overwrite
the external source. Secret references keep credentials out of Deployment
manifests, and external references also keep their data out of Helm values and
release history. They do not hide data from a cluster administrator who can
read the referenced Secret. Restrict those permissions with cluster RBAC, and
rotate at the provider/platform when an administrator loses that trust.

Only a new SQLite file primes the current inbox as history. A restart performs
one provider confirmation without marking messages seen, then resumes durable
pending and downtime mail before `/readyz` becomes healthy. Steady readiness is
local-only; an AgentMail outage leaves the pod ready while retries remain visible
in logs and state.

The PVC is PII-bearing application data: it can hold email addresses, message and
thread identifiers, recovery text, and delivery receipts, though never the three
credentials or a platform database credential. Back up with a storage snapshot
that is consistent for SQLite, or stop the Deployment before copying the file.
Restore the claim before starting the writer.

A disposable synthetic restore of postgres records, immutable bundle objects,
that SQLite delivery state, and a Valkey dump is `curie dev restore-drill`
(#2427). It uses those existing export/restore mechanisms, requires operator
keys and config to be supplied separately, and refuses an omitted or corrupt
component before serving. It does not establish a recurring production backup,
an RPO/RTO target, or Valkey stream replay. An older image refuses a newer
schema; restore the pre-upgrade snapshot or roll forward rather than
deleting state to force a rollback. A chart-managed PVC is deleted by Helm
uninstall, subject to the StorageClass reclaim policy; an `existingClaim` is not
owned or deleted by the chart. Erasure means stopping the adapter and deleting
the PVC plus every retained PV, snapshot, and backup. Starting on a fresh claim
performs first-boot priming and intentionally does not backfill the inbox.

The remaining operator-relevant sender boundary is documented once in the
adapter's README rather than here: Curie authenticates no sender, so
`mailAdapter.allowedSenders` filters an attacker-controlled `From` header and
buys nothing unless every domain on it enforces DMARC. That section, the
AgentMail-specific parameter names, the full config surface and the boot gates all live in
[`apps/mail-adapter/README.md`](../apps/mail-adapter/README.md); to build an adapter for a
different channel, see [Building a channel adapter](guides/building-a-channel-adapter.md).

## Upgrading the chart

A chart upgrade is a **full** upgrade: anything the new chart does not render is
deleted. For a Deployment that means a restart. For a StatefulSet it means the
data too.

### State-identity migration (Alembic revision 0037)

Before upgrading to a release containing revision 0037, take a
transaction-consistent database backup. The revision restores the shared posture
for unambiguous legacy general state and makes a NULL `binding_scope` a single
state identity. Run these **read-only** preflights against the target database
immediately before the upgrade:

```sql
-- Every duplicate shared identity, including reserved namespaces.
SELECT agent_id, namespace, key, count(*) AS row_count
FROM curie.workflow_state_entries
WHERE binding_scope IS NULL
GROUP BY agent_id, namespace, key
HAVING count(*) > 1
ORDER BY agent_id, namespace, key;

-- memory=false owners that 0037 would promote, but whose general state is
-- already split between shared and binding-scoped rows.
SELECT agents.id AS agent_id,
       count(*) FILTER (WHERE state.binding_scope IS NULL) AS shared_rows,
       count(*) FILTER (WHERE state.binding_scope IS NOT NULL) AS isolated_rows
FROM curie.agents AS agents
JOIN curie.workflow_state_entries AS state ON state.agent_id = agents.id
WHERE agents.memory = false
  AND state.namespace NOT IN ('memory', 'transcript')
GROUP BY agents.id
HAVING bool_or(state.binding_scope IS NULL)
   AND bool_or(state.binding_scope IS NOT NULL)
ORDER BY agents.id;
```

Both result sets must be empty. Do not auto-merge a reported row: the database
cannot choose between state values or versions, nor infer whether a mixed
agent's general state should be shared or isolated. For each duplicate, inspect
its values and versions, then explicitly merge or delete until one row remains.
For each mixed `memory=false` agent, choose shared or isolated policy and
move/merge every general-state row into that one shape. Re-run the preflights,
then the upgrade. On any refusal, the whole 0037 transaction rolls back: agent
flags, state rows, the constraint, and the Alembic revision stay unchanged.

### Before you upgrade, check what would be removed

```bash
curie diff -f curie.yaml
```

`diff` reads the release's live StatefulSets and renders the target chart, so a
stateful component that chart would DELETE is reported directly as
`stateful_removals` and counted in `changes`, instead of surfacing as an
ordinary value add. A non-empty list is not a routine change count: `curie
apply` on that same file will REFUSE.

`migration` names the object-store rename (`minio` → `rustfs`) that `curie
apply --migrate-store` carries the data across. Its absence beside a non-empty
`stateful_removals` means there is no automatic carry -- a store disabled
through the chart's own BYO gate (`postgres.deploy: false`) removes a component
`--migrate-store` has nothing to move.

`chart_version_differs: true` means the value-level entry comparison cannot see
a NON-STATEFUL component added, removed, or renamed between versions. A renamed
component's old keys appear as ordinary resets, which reads far milder than the
swap it would be. Stateful components are the exception: those come from the
live read above, whatever the chart versions say.

`curie diff --chart <ref>` points the comparison at the same chart `curie apply
--chart <ref>` would use, and reports that chart's version as the target. A dev
build run outside a source checkout needs it, since resolving and rendering a
chart is now part of `diff`.

`curie apply` refuses outright when the upgrade would delete a StatefulSet the
release is running, and names it. `--migrate-store` is the option to reach
for: apply stages the object store, upgrades, loads it back, and verifies it,
all in one command, so the store's data survives. It carries the object store
only; if the same upgrade would also delete another stateful component, apply
still refuses and names that component, since `--migrate-store` gives it no
way to carry that data too. It is opt-in rather than automatic because the
migration has a window where the store is empty and the bot cannot answer, so
an apply that only changes a log level must never silently start moving data.
`--allow-stateful-removal` proceeds WITHOUT the data instead, for a store you
genuinely intend to discard. The two flags are
mutually exclusive: passing both is rejected by the parser with a nonzero
exit, never silently resolved by picking one.

If `curie apply` cannot read the cluster to run this check (an unreachable or
erroring apiserver), it now fails rather than assuming nothing is at risk. An
unreachable cluster classifies as transient (exit code 3), so an automation
loop can retry the same command. This also applies to `--dry-run`: a dry run
that could not read the cluster cannot honestly claim the store is safe, so it
now errors instead of printing a plan.

`curie diff` fails closed the same way, and classifies the same: it mutates
nothing and resolves no credential (an unresolvable one is reported, never
fatal), but answering "no removals" when the cluster read failed is the false
assurance this check exists to prevent.

Without the CLI, the same check by hand:

```bash
# what the release runs today
kubectl get sts -n <ns> --no-headers | awk '{print $1}'
# what the target chart would render
helm template <release> <chart> -n <ns> -f values.yaml \
  | awk '/^kind: StatefulSet/{f=1} f&&/^  name:/{print $2; f=0}'
```

Anything in the first list and not the second is about to be deleted.

### Pass a values FILE, not `--reuse-values`

`--reuse-values` does not merge the new chart's defaults, so any value key the
new chart introduces is simply absent. Upgrading across a chart that adds a
component fails outright:

```
Error: UPGRADE FAILED: template: <a template referencing a NEW value key>:
  executing ... at <.Values.rustfs.deploy>: nil pointer evaluating interface {}.deploy
```

Capture the release's current values and pass them as a file instead. That
merges over the new chart's defaults, so new keys get their defaults and your
settings are preserved -- including the generated store passwords, which must be
re-supplied or the upgrade rotates them out from under a running database.

```bash
helm get values <release> -n <ns> -o yaml > values.yaml
helm upgrade <release> <chart> -n <ns> -f values.yaml
```

`curie cluster up` and `curie apply` do this without asking the operator to
choose `--reuse-values` versus `--reset-then-reuse-values`. They persist
`config.schemaVersion` on the release, run pure migrations from supported
v0.8.x user values onto the v0.9.0 schema (legacy extraEnv entries with a
first-class successor, external Secret references), and overlay the result so
new chart defaults still apply. A second upgrade with no input change is a
no-op. Plan and diff output stay redacted.

### Migrating the bundle store (0.5.x → 0.6.0, `minio` → `rustfs`)

0.6.0 renamed the in-cluster object store. The chart cannot migrate it for you,
and the store is on the hot path of **every** turn: each Slack thread creates a
sandbox whose `bundle-fetch` init container downloads the bundle before the
runner starts. An empty store means the bot stops answering, not merely that
rollbacks break.

Export first, upgrade second, import third. The export and the rollback point
are both taken while the old store is still up.

```bash
# 1. Export every object while MinIO is still running
STAGE=/var/lib/curie-bundle-migration && mkdir -p "$STAGE"
aws configure set default.s3.addressing_style path
IP=$(kubectl get svc -n <ns> <release>-minio -o jsonpath='{.spec.clusterIP}')
export AWS_ACCESS_KEY_ID=minio
export AWS_SECRET_ACCESS_KEY=$(kubectl get secret -n <ns> <release>-secrets \
  -o jsonpath='{.data.minioRootPassword}' | base64 -d)
aws s3 sync s3://curie-bundles "$STAGE" --endpoint-url "http://$IP:9000"
find "$STAGE" -type f | wc -l          # note this count

# 2. Rollback point
helm get values <release> -n <ns> -o yaml > values.yaml
helm list -n <ns>                       # note the revision

# 3. Upgrade
helm upgrade <release> <chart> -n <ns> -f values.yaml

# 4. Import into RustFS
IP=$(kubectl get svc -n <ns> <release>-rustfs -o jsonpath='{.spec.clusterIP}')
export AWS_ACCESS_KEY_ID=rustfs
export AWS_SECRET_ACCESS_KEY=$(kubectl get secret -n <ns> <release>-secrets \
  -o jsonpath='{.data.rustfsSecretKey}' | base64 -d)
aws s3 mb s3://curie-bundles --endpoint-url "http://$IP:9000"
aws s3 sync "$STAGE" s3://curie-bundles --endpoint-url "http://$IP:9000"
```

Verify by object, not by total. Compare name-and-size for every object, and
checksum at least the bundle each active deployment points at -- `head-object`'s
ETag is the MD5 for a single-part upload:

```bash
aws s3api head-object --bucket curie-bundles --key "<active bundle key>" \
  --endpoint-url "http://$IP:9000" --query ETag --output text
md5sum "$STAGE/<active bundle key>"
```

A byte total is not enough on its own: a concurrent `git push` can legitimately
add an object mid-migration, so counts and totals can differ for a benign
reason. Diffing per object tells the two cases apart.

**The bot is down between steps 3 and 4** -- the store exists but is empty. The
window is however long the copy takes (seconds for a small install). Do it
deliberately rather than discovering it.

**Rolling back.** `helm rollback <release> <revision> -n <ns>` restores the
previous chart. Deleting a StatefulSet does not delete the PVCs its
`volumeClaimTemplates` created, so the old store's volume survives the upgrade
and the rollback re-attaches it with the data intact. Keep the export anyway.

### Approvals pending across a worker roll

An upgrade restarts the worker, and an approval can easily be pending for hours
or days -- so approvals routinely straddle a roll. The worker remembers where it
posted each Slack approval card so it can settle that card (strip the
Approve/Reject buttons) when the approval is resolved out of band or EXPIRES.
That memory used to be keyed by conversation and is now keyed by approval id.

A resolution through the buttons carries its own card location, so it settles
either way. An **expiry** carries no click: if the worker cannot find the
remembered card, the expired approval keeps buttons that answer every later
click with an error.

**No operator action is required.** On startup the worker moves any remaining
conversation-keyed entries onto their approval id once, so those approvals
settle normally. The pass is best-effort and cannot fail startup; if Valkey is
unreachable at that moment the affected cards simply stay live until their
memory lapses (14 days).

One narrow window stays open while the roll is in progress. The startup pass
runs once, so it cannot see an entry written after it finished -- and a replica
still on the old build keeps serving, and keeps recording cards the old way,
until it is replaced. An approval created by such a replica after the new one
started can therefore still keep live buttons if it later expires. The window
closes on its own once the roll completes and every replica is on the new
build; anything missed lapses with its existing 14 day memory.

One residual case is not recoverable: a card remembered by a build old enough
that the entry did not record which approval it belonged to cannot be paired
with anything. If such an approval expires, its message keeps its buttons.
Edit or delete that Slack message by hand, or ignore it -- the approval itself
is expired in the API either way, so a click on it cannot approve anything.

## When the schema upgrade refuses over an approval's reply identity

Two revisions record where an approval's reply has to go back through:
`0022` writes `approvals.reply_kind`, and `0024` writes the `reply_adapter`
that names the egress identity authenticating it. Both establish that from the
bindings the installation actually has, and both **refuse** rather than guess
when a row's identity cannot be established. An upgrade that stops here fails
the `-schema-migrate` Job with a message naming every offending approval id.

Nothing is deleted and no approval is settled to clear it. Deleting the row
destroys the audit history, and settling one does not help anyway: neither
preflight reads `status`. The supported recovery is a round trip.

### 1. Report

**Read the failed Job's log.** The refusal is self-sufficient: it lists every
approval the migration could not reconstruct, its `reply_channel`, its status
and why it could not be reconstructed, and then prints a declaration document
skeleton with one entry per row, ready to fill in.

```sh
kubectl -n curie logs job/curie-schema-migrate
```

That matters rather than being a convenience. `schema_compat.json` sets the
minimum schema the API serves to its own head, so the API that answers the
identity report **refuses to start against a pre-head schema** -- and a blocked
installation is on one by definition. On an installation that is *not* blocked,
the same facts come from the CLI:

```sh
curie --json cluster approvals <agent> --report-identity > identity-report.json
```

Two things about that command line are not optional. `cluster approvals` takes
a **required positional agent**, so the command has to name one even though this
report is installation-wide and ignores it -- pass any existing agent. And the
report is a payload, so it needs the global **`--json`** flag: the default human
output summarizes the facts and does not emit a document you can feed back.

The CLI wraps the report under `identity_report`, which carries `approvals`
(one facts entry per approval) and `declarations` (the skeleton, one entry per
row, every field but the id left for you). Lift the skeleton into the document
the migration consumes:

```sh
jq '{declarations: .identity_report.declarations}' identity-report.json > declarations.json
```

That is the same document the failed Job prints, in the same shape.

### 2. Declare

Fill the skeleton in by hand. It is a statement that a human knows what the
approval was **raised** on -- not what its address happens to be bound to now,
which is the thing the schema already cannot tell.

```json
{
  "declarations": [
    {
      "approval_id": "0f2b1d6e-...",
      "reply_kind": "email",
      "reply_adapter": "smtp-primary",
      "actor": "U0OPERATOR",
      "reason": "raised on the smtp-primary egress, retired since"
    }
  ]
}
```

Every field is required. `reply_adapter` may be `null`, and only `null`, for a
Slack row, which legitimately has no adapter. A document that is unparseable,
missing a field, or naming an approval the migration does not report is refused
whole: none of its declarations are applied, and the refusal names the file and
the offending entry. A declaration for a row the migration **can** reconstruct
is also refused -- the migration's own answer wins, because overriding
provenance the schema can still prove is a rewrite, not a recovery.

### 3. Supply it to the upgrade

The document is mounted from a Secret you create, never passed as a Helm value:
`helm get values` would keep an inline declaration in the release forever and
re-apply it to every later upgrade, whereas a Secret can be deleted afterwards,
which makes the grant single-use.

```sh
kubectl -n curie create secret generic curie-approval-declarations \
  --from-file=declarations.json=./declarations.json
```

Then set `api.migrate.provenanceDeclarationsSecret=curie-approval-declarations`
and run the upgrade. The Job mounts it read-only and reads it through
`CURIE_APPROVAL_PROVENANCE_DECLARATIONS`. Delete the Secret and unset the value
once the upgrade succeeds; a document left mounted that names an
already-migrated approval produces a loud refusal on the next upgrade rather
than silently re-applying.

### 4. Read the record back

Each honored declaration appends exactly one `approval_audit_entries` row, so
the bypass is attributed and reviewable rather than silent:

```sql
SELECT approval_id, actor, reason, evidence
FROM curie.approval_audit_entries
WHERE action = 'provenance_declaration_honored'
ORDER BY created_at;
```

`evidence` carries the declared kind and adapter, the revision that honored it,
and the reason the migration could not reconstruct the row on its own.

### The fence, and why your API stays up

Both revisions take `curie.agent_channels` and then `curie.approvals` in ACCESS
EXCLUSIVE for the length of their own transaction, so the preflight, the
backfill and the constraint tightening are one unit and a binding cannot be
re-pointed in the middle of them. A concurrent approval insert or binding write
**blocks and then succeeds**: an in-flight turn's approval request is queued,
never refused.

The mode is the strongest one each revision needs, taken up front on purpose.
A weaker fence would let a reader through, but the revision's own `ADD COLUMN`
needs ACCESS EXCLUSIVE anyway, so the fence would have to be upgraded mid
transaction -- and an ordinary resolver that reads an approval and then writes
its decision closes that cycle, which PostgreSQL breaks by aborting one side.
Taking the strong lock first costs concurrent reads of these two tables a brief
wait for the migration's duration, and buys back never killing a live
resolution.

The two tables are locked one after the other, `agent_channels` first, because
that is the order every writer touching both uses: deleting an agent removes its
bindings and then cascades into its approvals, and publication writes the
binding before the approval. In that order a writer cannot deadlock against the
fence. A reader can: the approval-recovery endpoint reads `approvals` and then
`agent_channels`, the opposite order, and no single order suits both. When a
read and the fence do cycle, PostgreSQL detects it after `deadlock_timeout`
(1 s by default) and aborts one side. Both outcomes are safe. An aborted
migration has changed nothing and the migrate Job retries it (`backoffLimit: 3`);
an aborted recovery read changed nothing and is answered with HTTP 409 and a
plain instruction to retry, rather than a 500.

If the fence cannot be taken inside `CURIE_MIGRATION_FENCE_LOCK_TIMEOUT_MS`
(`api.migrate.fenceLockTimeoutMs`, default 15000) the migration refuses
**before mutating anything**, names the session that held the table, and the
database is exactly as it was. Stop that writer, or raise the bound, and
re-run.

## Which claim env reaches which sandbox container

Almost nobody writes a `SandboxClaim` by hand -- the worker creates them. But
reproducing a sandbox by hand is the normal way to build a proof, a bug repro,
or a support investigation, and the claim's env has a shape that is easy to get
wrong in a way that looks like success.

A sandbox pod runs up to three staging **init containers** before the runner
starts: `bundle-fetch` and `bundle-extract` pull the plugin bundle out of the
object store into `CURIE_PLUGIN_DIR`, and `workspace-init` fetches and unpacks
the repository workspace. Each one reads its own env.

A `SandboxClaim` env entry carries an optional `containerName`. **An entry with
no `containerName` is injected into the runner container only** -- the
`envVarsInjectionPolicy: Overrides` on the SandboxTemplate governs which side
wins for a container the entry names, not which containers it reaches. So an
entry the init containers need must be repeated once per init container, each
with an explicit `containerName`:

```yaml
spec:
  env:
    # Reaches the runner only -- correct for runner-side keys.
    - name: CURIE_SESSION_ID
      value: thread-42
    # Staging keys must be repeated per init container.
    - name: CURIE_BUNDLE_REF
      value: bundles/my-agent-v7.tgz
    - name: CURIE_BUNDLE_REF
      value: bundles/my-agent-v7.tgz
      containerName: bundle-fetch
    - name: CURIE_BUNDLE_REF
      value: bundles/my-agent-v7.tgz
      containerName: bundle-extract
```

| Env | Claim-settable? | Who consumes it |
|---|---|---|
| `CURIE_BUNDLE_REF` | Yes -- runner **and** `containerName: bundle-fetch` **and** `containerName: bundle-extract` | The init pair fetches and extracts the bundle; the runner reads the ref only to diagnose a staging failure. |
| `CURIE_WORKSPACE_REF` | Yes -- `containerName: workspace-init` only | `workspace-init`. Deliberately NOT injected into the runner: it is a short-lived signed URL and the claim is plaintext in etcd. |
| `CURIE_WORKSPACE_SHA256` | Yes -- `containerName: workspace-init` only | `workspace-init`, to verify the fetched archive. |
| `CURIE_SESSION_ID`, `CURIE_HISTORY_REF`, `CURIE_PLUGIN_DIR`, and the rest of the boot env | Yes -- runner, no `containerName` | The runner. |
| `CURIE_CREDENTIALS` | Don't. Worker-filtered, not schema-rejected | The runner, from the chart Secret's `secretKeyRef`. The worker strips this key off every claim it writes; a claim you write yourself is not filtered, and the value would sit in plaintext in etcd. |
| Per-agent connector secrets (the keys named by `CURIE_CONNECTOR_SECRET_KEYS`) | Don't. Worker-filtered, not schema-rejected | The runner, from the per-agent SandboxTemplate's `secretKeyRef`. Same plaintext-in-etcd caveat. |
| `S3_ENDPOINT`, `BUNDLE_BUCKET`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `AWS_*` | Don't. Chart-managed defaults | `bundle-fetch`. Wired from values and Secrets, not a per-claim decision. An explicitly targeted claim entry would override them under `Overrides`. |

**Nothing on this list is enforced by the CRD.** The vendored `SandboxClaim`
schema accepts any env name, and `envVarsInjectionPolicy: Overrides` means a
claim entry that names a container wins over the template's own value for it.
The "Don't" rows above are the worker's discipline and the chart's wiring, both
of which a hand-written claim bypasses entirely. Treat them as what you must not
do, not as what you cannot do -- in particular, a credential you put on a claim
is persisted in plaintext in etcd and nothing will stop you.

**How the mistake shows up.** It does not look like a mistake. Every init
container exits 0 -- an empty ref is its documented no-op path, which is what a
warm or unbound pod needs. The claim reports `Pod is Running but not Ready`, and
the runner crash-loops on `[manifest.missing]`, which reads like a broken
bundle. Two things now name the real cause:

- each staging init container logs, on its no-op path, that a `spec.env` entry
  with no `containerName` reaches the runner only, and which `containerName` to
  add (`kubectl logs <pod> -c bundle-fetch`);
- the runner refuses to boot with `CURIE_BUNDLE_REF` set over an empty plugin
  dir, and says so in those terms instead of blaming the bundle.

## Known gotchas

Notes from the first installs of the chart on fresh clusters, kept for the
next operator.

- **A hand-written `SandboxClaim`'s `spec.env` reaches the runner container
  only** unless each entry names a `containerName`. Staging a plugin bundle or a
  workspace by hand therefore needs the entry repeated per init container --
  see [Which claim env reaches which sandbox container](#which-claim-env-reaches-which-sandbox-container)
  above. It used to fail silently: every init container exited 0 (#2612).
- **The agent-sandbox controller is enabled by default.** The chart ships the
  agent-sandbox CRDs and deploys the vendored controller when
  `agentSandbox.controller.deploy=true`, which is the default. A cluster that
  has the CRDs but no controller silently never binds claims. Plain `cluster
  up` keeps the default when the controller is absent and infers
  `agentSandbox.controller.deploy=false` only when an existing Deployment has
  complete Helm ownership metadata for another release.
- **gVisor stays off without runsc on the node.** Use the
  `values-e2e-nogvisor` overlay on nodes without `runsc`. All other
  security rails were verified ON in the first fresh-cluster install:
  default-deny egress, metadata-endpoint block, read-only rootfs, non-root,
  and per-agent secret isolation.
- **langfuse-web restarts ~2x during first boot** while ClickHouse and
  Postgres come up, then stabilizes. This is startup ordering, not a
  crashloop; do not treat the early restarts as a failure.
- **Exactly one Curie release may connect to a given Slack app.** Slack Socket
  Mode fans events across every connected client, so two releases sharing one
  app silently split mentions. Give each long-lived release its own app. If a
  second dispatcher is already connected, stop the extra client; do not retry
  mentions or approval clicks hoping Slack picks the owner. Leave-unacked
  approval routing is not an operator retry procedure. See
  [Slack's multiple-connections contract](https://docs.slack.dev/apis/events-api/using-socket-mode/#using-multiple-connections).
- **kube-router applies NetworkPolicy a few seconds after pod start.** A
  brand-new pod can see open egress for the first seconds before the policy
  lands. This is functionally irrelevant for runners (the first model call
  comes later) but worth knowing when reading probe output from the first
  seconds of a pod's life.

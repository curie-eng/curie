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
instead. Curie has no cluster-selection flag of its own -- every `cluster`
command just uses whatever `kubectl` and `helm` are already pointed at, so
switch clusters the normal `kubectl` way:

```bash
kubectl config use-context <your-production-context>
```

## Installing and inspecting the Curie platform on the cluster

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
`curie.yaml` and customize it. Credential fields contain credential names, not
secret values.
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
need a larger size.

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
| `CURIE_CREDENTIALS` (alias `CURIE_MODEL_CREDENTIALS`) | A real model credential. The interactive check accepts Anthropic `sk-ant-`, OpenRouter `sk-or-`, Zhipu `id.secret`, and bare `sk-` shapes for Moonshot or DeepSeek. The first two prefixes select one provider and infer its egress when no provider flag is present. Other shapes do not identify a provider. Present credentials install live through masked `--set` machinery, so `--dry-run` never prints them. An absent credential uses fake mode on a fresh install and preserves the recorded model configuration on a rerun. |
| `--fake-model` | Explicitly downgrade to fake mode, even when a credential is present or a rerun has recorded live model configuration. |
| `--github-token <token>` (or `CURIE_GITHUB_TOKEN`) | The Curie API's own GitHub credential, for cloning a PRIVATE repo during a git-flow bundle deploy and for posting the eval commit status. Goes to helm through a private mode-0600 values file, never a command-line argument, so it never appears in the helm command, the printed plan, or that plan's JSON. Prefer the environment variable: a token typed after the flag still sits in `curie`'s own argv, so it still reaches your shell history and `ps`. Omitting both on a later `cluster up` preserves whatever the release already has. Errors if combined with `--set api.githubToken=`. |
| `--clear-github-token` | Remove the stored GitHub credential. Not a revocation: the running API keeps the old token until its pod restarts (`cluster up` prints the restart command), and the token itself stays valid at GitHub until you revoke it there. |
| `--allow-egress-host <provider>` (repeatable) | Explicitly open runner egress on TCP 443 to one named model provider: `anthropic`, `openrouter`, `zhipu`, `moonshot`, or `deepseek`. Names are lowercase exact. An explicit list must include the provider detected from an `sk-ant-` or `sk-or-` credential. |
| `--allow-web-egress <CIDR>` (repeatable) | Open runner egress on TCP 443 to an arbitrary CIDR (Classless Inter-Domain Routing block) -- for skill/tool web access, or a provider not covered above. |
| `--forward-only` | Apply contract or irreversible schema migrations during this upgrade. The default refuses those migrations before mutation so a patch rollback window stays intact. Expand-only patch migrations do not need the flag. |

A downloaded release binary needs no repo checkout; the chart resolves from
the version-pinned release asset by default.

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

The first gVisor preflight keeps the chart default. Only the exact admission
result `RuntimeClass "gvisor" not found` authorizes
`security.gvisor.mode=off` and one retry. That first attempt renders as
retrying, not as a failed install; the retry is the one installed or failed
result. An explicit `auto` or `require` mode contradicts that result and
errors. Other admission failures and an unavailable event watch remain closed.
Curie prints one standard error line for every inference, including the
equivalent override. Prepared `apply` and `diff` paths do not infer live
cluster facts.

### `curie cluster status`

```bash
curie cluster status
```

A release that has not converged returns exit code 1 with its status report and
rollout reasons. `--json` retains the same report object with `healthy: false`.
The check compares the installed Helm target with live workload generations,
replica counts and serving pod images; an `Available` condition alone is insufficient.

Reports whether the release is healthy, which pods are ready, and the URLs
to reach it -- including the web console, where you can see your agents,
their deployed versions, and their run history. That console URL includes a
`?api=1` parameter; leave it as-is when you open it, it's just what points
the console at this release's Curie API. `--json` also reports the current
upgrade phase and the last known-good version.

### `curie cluster upgrade`

```bash
curie cluster upgrade --to 0.9.0
```

| Flag | What it does |
|---|---|
| `--to <version>` | Target Curie version. Required. |
| `--chart` | Chart path or ref override. |
| `--yes` | Skip the confirmation prompt. |
| `--dry-run` | Print the redacted plan and exit without mutating. |

One resumable lifecycle: inspect and plan, validate configuration and
compatibility, drain accepted work, checkpoint, migrate once, apply, wait
for exact convergence, run a target-version canary, then record the new
known-good version. The command chooses the values overlay; do not pass
`--reuse-values` or `--reset-then-reuse-values`.

`--json` reports the current phase, the last known-good version, whether
the previous version is still serving, and at most one fail-forward
command. Success is refused unless convergence is exact and the canary
passed. Re-run the same command to resume after an interruption.

This composes configuration migration (issue 2299) and schema
compatibility (issue 2300) rather than adding Helm special cases. The
pre-upgrade drain is the existing worker gate (issue 2010): a resume
after a completed drain does not drain accepted work again.

### `curie cluster down`

```bash
curie cluster down
```

| Flag | What it does |
|---|---|
| `--yes` | Skip the confirmation prompt. |

`curie cluster down` safely removes everything this release created, and
only what it created -- other things on the cluster are untouched,
including pre-existing namespaces and the Agent Sandbox CRDs.

A release is identified by its name AND the namespace it was installed
into, so if you run a second install of Curie on the same cluster (which
normally means two releases sharing the default name `curie` in different
namespaces), tearing one down never touches the other's namespaces.

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
every `git push`. Four things need to be true for a push to actually
promote:

1. **The agent's repo is set.** The webhook resolves which agent a push
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
2. **Mint the channel token.** `POST /channels/token` with the platform key returns a
   scoped `chn` token for that one binding. It refuses with 409 for a non-`slack`
   binding that has no reply route, which is why the binding comes first.

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

An empty `mailAdapter.agentmail.httpsCidrs` refuses to render when the adapter is
enabled. Prefix-0 and prefix-1 routes refuse to render, including IPv4 or IPv6
split default routes; surrounding whitespace and expanded IPv6 spelling do not
bypass that gate. Use narrow current provider or controlled-proxy ranges.

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
| `mailAdapter.agentmail.httpsCidrs` | Required provider/proxy destination CIDRs on TCP 443. The mail pod's egress policy otherwise allows only DNS and this release's API pods. |
| `mailAdapter.apiEgress.httpsCidrs` / `port` | Required narrow destination peers when `api.deploy=false`; default port `8000`. Ignored for the in-chart API, whose pod selector and service port are used instead. |
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
Restore the claim before starting the writer. An older image refuses a newer
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

## Known gotchas

Notes from the first installs of the chart on fresh clusters, kept for the
next operator.

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
- **Give long-lived releases separate Slack Socket Mode apps.** Slack permits
  up to ten connections for one app and may send each payload to any connection
  without a predictable distribution pattern. During a temporary overlap, a
  non-owning Curie release leaves the Socket Mode envelope unacked so Slack
  retries the owner; the non-owner does not resolve, reject, or mutate the
  card. Stop the local dispatcher after testing rather than leaving it
  competing with the in-cluster release. See
  [Slack's multiple-connections contract](https://docs.slack.dev/apis/events-api/using-socket-mode/#using-multiple-connections).
- **kube-router applies NetworkPolicy a few seconds after pod start.** A
  brand-new pod can see open egress for the first seconds before the policy
  lands. This is functionally irrelevant for runners (the first model call
  comes later) but worth knowing when reading probe output from the first
  seconds of a pod's life.

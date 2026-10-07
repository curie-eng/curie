# Dark factory quickstart

`curie factory quickstart` installs Curie on a local
[kind](https://kind.sigs.k8s.io/) cluster or an explicitly selected existing
cluster, then prints a GitHub App registration link. Your part is two
quickstart commands, registering and installing the App, and labelling an
issue. Curie polls the repository and publishes a pull request under the App's
identity. The command never opens a browser or runs `gh`.

For planning, reserve **about 70 minutes for configured waiting windows**,
plus variable time for Helm, image downloads, cluster creation, and App setup.
That rounded allowance comprises [60 minutes of factory execution](../../cli/src/factory_quickstart.rs),
[5 minutes of initial post-Helm convergence](../../cli/src/ops/convergence.rs),
[3 minutes of API rollout after factory configuration](../../cli/src/github_app.rs),
and [45 seconds for an intake polling interval](../../charts/curie/values.yaml).
These are configured phase limits and an interval, not a measured total or a
guarantee that the whole walkthrough finishes within the allowance.

Polling is the intake. There is no webhook, no tunnel, and no personal access
token. The platform reads the issue for the sandbox.

## What you need

| Tool | Used for |
|---|---|
| [Docker](https://docs.docker.com/get-docker/) | kind nodes, required only when neither `--context` nor a current kubeconfig context is set |
| [`kind`](https://kind.sigs.k8s.io/docs/user/quick-start/#installation) v0.24 or later | the local cluster, required only when neither `--context` nor a current kubeconfig context is set. kind's network plugin enforces NetworkPolicy from v0.24. |
| [`kubectl`](https://kubernetes.io/docs/tasks/tools/) | Kubernetes operations, required for every context |
| [`helm`](https://helm.sh/docs/intro/install/) | Curie installation, required for every context |
| `curie` v0.12.1 or later | install it with the command below |
| An [Anthropic](https://console.anthropic.com/) API key (`sk-ant-`) or an [OpenRouter](https://openrouter.ai/) API key (`sk-or-`) | Anthropic defaults to `claude-sonnet-5-5` for implementation and `claude-opus-5-5` for review. OpenRouter defaults to `z-ai/glm-5.3-flash` for implementation and `anthropic/claude-opus-5.5` for review. Plan on about 5 USD of available OpenRouter credit per run; actual spend varies. The command asks once, or reads `CURIE_CREDENTIALS`. |
| A GitHub account | your own GitHub App and the trial repository |

The command checks all required tools on `PATH` before running any command or
changing the cluster. If tools are missing, it lists every missing tool with
its official installation link. `--dry-run` performs the same prerequisite
check. An explicit or current context skips the Docker and kind requirements,
including when the context belongs to an existing kind cluster.

A released `curie` deploys the dark factory runner image that release published.
A binary built from a source checkout has no published layer for its own
version and stops before deploy, naming the release install.

## Install Curie

Use **v0.12.1 or later** for the behavior in this guide. Run this in Bash:

```bash
curl -fsSL https://raw.githubusercontent.com/curie-eng/curie/main/get-curie.sh | CURIE_VERSION=v0.12.1 bash
if [ -w /usr/local/bin ]; then
  export PATH="/usr/local/bin:$PATH"
else
  export PATH="$HOME/.local/bin:$PATH"
fi
hash -r
curie --version
```

The installer selects the asset for your machine, verifies its checksum, and
installs to `/usr/local/bin` when writable, otherwise `$HOME/.local/bin`. The
PATH block puts the selected install directory before an older binary.
The command selects v0.12.1 explicitly. To use a later release, change
`CURIE_VERSION` in that command to its release tag. Confirm the printed version
is at least `curie 0.12.1` before continuing. A stale older binary can report
`unrecognized subcommand 'factory'`.

Released assets cover Linux x86_64, Linux arm64, and macOS Apple silicon.
**There is no x86_64 macOS asset.** Intel Mac users need a supported Linux
machine for this released-binary quickstart. A source build alone cannot deploy
its unpublished factory runner image.

## Create the repository

In the GitHub website, create a repository with one commit (an initial README
is enough). The repository may be public or private. Either way, the GitHub App
must be installed on it, which the **Install App** step under
[Rerun with the App](#rerun-with-the-app) covers. The factory branches from the
default branch, so the repository needs that commit. It does not need CI. Note
the `owner/name` form, for example `acme-corp/acme-bot`.

## Choose the target and run the command

Use an interactive Bash session for the prompts below. Keep the variables in
that same session for both runs, inspection, and cleanup. Enter your
repository's `owner/name` when prompted. Copy and run each prompt block
separately, answering before you copy the next block:

```bash
read -r -p "Trial repository (owner/name): " CURIE_REPO
```

After answering, initialize the shared settings:

```bash
CURIE_NAMESPACE=curie
CURIE_RELEASE=curie
QUICKSTART_CONTEXT_ARGS=()
CURIE_CONTEXT=kind-curie-factory
```

Choose exactly one target path below. Do not run both paths.

For a **new local kind trial**, create an isolated empty kubeconfig so no
existing current context is selected:

```bash
CURIE_TRIAL_KUBECONFIG=$(mktemp)
export KUBECONFIG="$CURIE_TRIAL_KUBECONFIG"
```

Leave `QUICKSTART_CONTEXT_ARGS` empty. Quickstart then creates `curie-factory`
with context `kind-curie-factory` in this kubeconfig. Keep the temporary file
until cleanup.

For an **existing kind cluster or a remote cluster**, keep your existing
kubeconfig. Enter its intended context in this separate prompt:

```bash
read -r -p "Existing Kubernetes context: " CURIE_CONTEXT
```

After answering, supply the explicit context to both quickstart runs:

```bash
QUICKSTART_CONTEXT_ARGS=(--context "$CURIE_CONTEXT")
```

**For anything other than kind, always pass `--context` explicitly.**

Choose a namespace and release dedicated to this trial if `curie` already
belongs to another installation.

```bash
curie factory quickstart --repo "$CURIE_REPO" "${QUICKSTART_CONTEXT_ARGS[@]}" \
  --namespace "$CURIE_NAMESPACE" --release "$CURIE_RELEASE"
```

Check the first run's printed `Kubernetes context:` against `CURIE_CONTEXT`
before proceeding. For a new kind trial, it must be `kind-curie-factory`.
If it is anything else, stop and use the explicit context selection block
above to choose the intended cluster before rerunning.

What it does:

1. When no kubeconfig context is targeted, it creates kind cluster
   `curie-factory` (context `kind-curie-factory`) and scales CoreDNS to one
   replica. A current kind context skips creating a cluster and does not ask.
   A current context that is not kind, including a remote cluster, is not
   used until you confirm it in a terminal. Without a terminal the command
   stops and names the explicit context flag as the way to proceed. An explicit
   context never prompts.
2. It installs Curie with `cluster up`. On the kind path it sets
   `security.gvisor.mode=off` before the first install. It asks for the
   Anthropic or OpenRouter API key once when `CURIE_CREDENTIALS` is unset and the release is
   not already on a real model.
3. It prints a prefilled GitHub App registration link and stops. Nothing about
   the App is applied yet.

Open the link and click **Create GitHub App**. The form is filled in: a
private App, webhook off, and repository permissions for Metadata, Contents,
Issues, Pull requests, Checks, Commit statuses, and Actions. Then:

1. Note the **App ID**.
2. Under **Private keys**, click **Generate a private key** and save the `.pem` file.
3. Under **Install App**, install it on the trial repository. This is a click
   in the website. The command does not install the App for you.

## Rerun with the App

Run each prompt separately and answer it before copying the next block.
Enter the App ID first:

```bash
read -r -p "GitHub App ID: " CURIE_APP_ID
```

Enter the **absolute path** to the downloaded key. Start it with `/`; a literal
`~` entered at this prompt is not expanded to your home directory.

```bash
read -r -p "GitHub App private key file (absolute path): " CURIE_APP_KEY_FILE
```

Then rerun:

```bash
curie factory quickstart --repo "$CURIE_REPO" "${QUICKSTART_CONTEXT_ARGS[@]}" \
  --namespace "$CURIE_NAMESPACE" --release "$CURIE_RELEASE" \
  --app-id "$CURIE_APP_ID" --private-key-file "$CURIE_APP_KEY_FILE"
```

Each step is safe to repeat. A rerun after a failure resumes: kind is not
created again, `cluster up` keeps the recorded model credential, and the App
setup, deploy, surface, deadline, publication policy, and budget are applied
again to the same result.

By default, each run prints the Kubernetes context once and one line for each
step it performs. Cluster installation inference notices remain visible. The
first run ends with the App link and registration steps; the second ends with
four lines summarizing readiness, the reviewer model and per-run credit, intake
and the deployed image. Pass
`--debug` to see the chained commands and their detailed output. The printed
rerun command omits namespace, release and model flags when they match the
defaults.

Before any Helm call, the second run authenticates with the GitHub App and reads
the selected repository's workflows and standard manifests at one default-branch
commit. It announces inferred toolchains and warns when a required tool or
version is absent from the fixed factory runner. The runner includes Python
3.13, Node 22.23, and Rust 1.95. Matching minor versions are supported regardless
of patch; a bare Node 22 declaration also matches. A warning reports the tool,
version, and declaring file without changing the runner image or installing
tools. If a workflow names a version file that is missing, the version is
reported as unknown with a warning naming the file, and setup continues.
Authentication failures and malformed GitHub responses remain errors.
A repository with no declarations gets a `no toolchain signals` note.
The cluster factory command performs the same check for its allowlisted repositories.
Without `--app-id`, it prints `toolchain inference skipped: no --app-id`.
Dry-run describes the inference without reading GitHub.

An explicit `--model <id>` selects the implementer model instead of the
credential's default, even when it matches the OpenRouter default. Both
reviewers use the `opus` alias; the runner resolves it to the provider's Opus
model. To pin another reviewer model for this agent, use the same context,
namespace and release as the quickstart:

```bash
curie cluster overrides dark-factory --context "$CURIE_CONTEXT" \
  --namespace "$CURIE_NAMESPACE" --release "$CURIE_RELEASE" \
  --reviewer-model <provider-model-id>
```

Use `--clear-reviewer-model` in place of `--reviewer-model <provider-model-id>`
to restore the credential's default. `curie local overrides` accepts the same
reviewer flags for a local installation.

For OpenRouter credentials, before deploying, the second run reads the key's remaining credit from
OpenRouter (the smaller of the key's limit and the account balance). It warns
when that is below the recommended 5 USD of available credit for one run. When it cannot read
the credit, for example because the key only lives in the cluster, it prints
`OpenRouter credit not checked` and continues. The check never stops the
command. The ready output names the reviewer model and the per-run credit. A run
that runs out of credit ends with the out-of-credits cause on the issue.
With an Anthropic API key, quickstart skips this OpenRouter credit request.
If the provider reaches a usage limit, the run ends with cause
`model_usage_limited`. Wait for the limit to reset, then re-add the factory
label to try again. Credit exhaustion continues to use the add-credit remedy.

The recommended 5 USD of available provider credit is a planning allowance,
not a guaranteed minimum charge per run. Actual spend depends on the task,
including the Opus reviewers. Separately, quickstart configures the agent's
budget to 5 USD per day; several inexpensive runs can fit within that daily
budget. Having provider credit does not raise the configured daily budget.

The second run configures these values:

| Value | Source |
|---|---|
| Mention | The App slug, so a revision comment can mention `@<slug>`. |
| Allowlist | The repository you passed, checked against the App installation. |
| Label | `curie-factory`, created on that repository when it is missing. |
| Intake | Polling (`api.githubFactoryIntake=poll`). No webhook secret. |
| Agent | `dark-factory`, from the published runner image, environment `prod`. |
| GitHub surface | `github=<owner>/<name>` |
| Execution deadline | 3600 seconds |
| Publication | `auto` |
| Budget | 5 USD per day |

The card image stays a text checklist unless you later set a public
`--card-base-url` on the cluster factory command. Polling does not need one.

## Label an issue

In the GitHub website, open an issue on the repository and add the label
`curie-factory`. The person who adds the label must have write or admin access.
Anyone else's label is ignored.

The polling interval defaults to 45 seconds; admission then adds a status
comment. The factory reads the issue, plans, writes a test, implements, reviews
its own diff, and opens one pull request. It never pushes. Curie publishes the
patch from a separate job with the App's identity.

State labels Curie manages, one at a time:

| Label | Meaning |
|---|---|
| `curie-factory:queued` | Admitted, waiting for a sandbox |
| `curie-factory:running` | Running or stopping |
| `curie-factory:pr-open` | Finished with a pull request |
| `curie-factory:needs-human` | Failed or expired; the status comment says why |

A cancelled run removes all four. Curie never changes your `curie-factory` label.

## What a repository with no CI sees

1. After publishing, Curie waits on the pull request's checks. With no checks
   within 120 seconds of the push, and no required check configured, the run
   completes and the status comment says `Note: No CI checks appeared within 120 s.`
2. Before the model starts, the runner runs checks declared in
   `.curie/verification.json`. With no declaration it records `not_declared`. <!-- doclint:ignore-line -->
   See [Repository toolchain in the managed sandbox](repository-toolchain-in-the-managed-sandbox.md).
3. The run ends when the agent's turn ends. A command left in the background
   does not keep the run alive.

## Troubleshooting

| What you see | Cause | Fix |
|---|---|---|
| The command asks for a key and you have no terminal | `CURIE_CREDENTIALS` is unset | Export an `sk-or-` key and rerun |
| App setup says the App is not installed on the repository | The installation step was skipped | Install the App on that repository in the website and rerun the same command |
| App setup refuses the key or the Secret | The key does not match `--app-id`, or Secret `curie-github-app` holds another App | Pass the matching id and key, or delete a stale Secret and rerun |
| `no published dark factory runner` | This `curie` is a source build, or that version published no layer | Install a released `curie` and rerun |
| Work item stays `waiting for sandbox capacity` | Another run holds the sandbox, or the runner pod cannot start | Run the release inspection command below to see pod readiness and unhealthy reasons. The run starts when capacity frees |
| Status comment ends with `Could not complete:` | The run stopped. The comment's cause lines say why | Fix the cause, then relabel |
| `Could not resolve host: github.com` on kind | CoreDNS replica race | Rerun. The command scales CoreDNS to one replica |
| Nothing happens after the label | The labelling user lacks write access, the label name differs, or the agent has no `github=<owner>/<name>` surface | Fix that and relabel. Polling notices the label on its interval |
| Installation reports a pre-existing agent-sandbox controller | Another release owns it, or an unowned controller is already installed | Curie reuses a controller owned by another Helm release, or a healthy unowned controller with the matching chart image, by inferring `agentSandbox.controller.deploy=false`. An unhealthy or different unowned controller stops setup with a specific repair command. Have its owner make that repair, then rerun; release status cannot repair it. See [controller inference](../operations.md#curie-cluster-up) |

Inspect this trial's release with Curie:

```bash
curie cluster status --context "$CURIE_CONTEXT" \
  --namespace "$CURIE_NAMESPACE" --release "$CURIE_RELEASE"
```

## Another cluster

The explicit context block above skips creating kind and selects your existing
cluster for both quickstart runs. The App, polling intake, and deploy steps are
the same. On a non-kind cluster, `cluster up` checks the `gvisor` RuntimeClass.
When the direct lookup returns NotFound, it infers
`security.gvisor.mode=off` before the first install and prints the inference.
When lookup is forbidden, Curie proceeds with installation without inferring
gVisor off from that refusal. Only a subsequent admission error reporting the
exact missing `gvisor` RuntimeClass permits one gVisor-off retry. Other
failures remain errors. A kind context
gets gVisor off before installation because kind nodes do not ship `runsc`.
See [cluster installation](../operations.md#curie-cluster-up).

## Clean up

For an existing kind cluster or a remote cluster, remove only this trial's
release with the context, namespace, and release selected above:

```bash
curie cluster down --context "$CURIE_CONTEXT" \
  --namespace "$CURIE_NAMESPACE" --release "$CURIE_RELEASE" --yes
```

This removes the release and its owned runtime namespaces. It retains adopted
namespaces, the pre-existing shared controller namespace, and Agent Sandbox
CRDs. It does not delete the remote cluster or other releases.

If quickstart created the disposable `curie-factory` kind cluster, delete it
instead:

```bash
kind delete cluster --name curie-factory
rm -f -- "$CURIE_TRIAL_KUBECONFIG"
unset KUBECONFIG
```

Delete the GitHub App in the GitHub website when you are done with it. Delete
the downloaded private key file as well:

```bash
rm -f -- "$CURIE_APP_KEY_FILE"
```

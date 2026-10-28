# Software Factory quickstart

<a id="dark-factory-quickstart"></a>

Turn a labelled issue into a pull request or merge request you can review,
using the Software Factory co-worker. `dark-factory` remains its implementation name.
See the [solution overview](../software-factory.md) for the delegation workflow,
supported toolchains, review limits, and production differences.

This guide uses the latest released CLI and factory runner by default. See the
[capability reference](../capabilities.md) for supported integrations and limits.

## Choose your tracker and code host

| Your pairing | Start here |
|---|---|
| GitHub issues and GitHub pull requests | [GitHub walkthrough below](#what-you-need), starting with prerequisites and CLI installation |
| GitLab issues and GitLab merge requests | [Complete GitLab walkthrough](factory-gitlab-quickstart.md), including local cluster, credentials and first issue |
| Jira Cloud issues and GitHub pull requests | [Complete Jira Cloud and GitHub walkthrough](factory-jira-github-quickstart.md), including the GitHub App and start authority |
| Jira Cloud issues and Bitbucket Cloud or Data Center pull requests | [Bitbucket quickstart](#run-quickstart-for-bitbucket), with the Jira factory account from [Jira Cloud](#jira-cloud) and the [Bitbucket configuration](#bitbucket-cloud-and-data-center) limits |

The walkthrough on this page uses **GitHub issues and GitHub pull requests**.
The [GitLab](#gitlab) and [Jira Cloud](#jira-cloud) sections farther down are
advanced configuration references for those pairings.

`curie factory quickstart` installs Curie on a local
[kind](https://kind.sigs.k8s.io/) cluster or an explicitly selected existing
cluster, then prints a GitHub App registration link. Your part is two
quickstart commands, registering and installing the App, and labelling an
issue. Curie polls the repository and publishes a pull request under the App's
identity. The command never opens a browser or runs `gh`.

For planning, reserve **about 190 minutes for configured waiting windows**,
plus variable time for Helm, image downloads, cluster creation, and App setup.
That rounded allowance comprises [180 minutes of factory execution](../../cli/src/factory_quickstart/mod.rs),
[5 minutes of initial post-Helm convergence](../../cli/src/ops/convergence.rs),
[3 minutes of API rollout after factory configuration](../../cli/src/github_app.rs),
and [45 seconds for an intake polling interval](../../charts/curie/values.yaml).
These are configured phase limits and an interval, not a measured total or a
guarantee that the whole walkthrough finishes within the allowance.

For the GitHub walkthrough, polling is the intake. There is no webhook, no
tunnel, and no personal access token. The platform reads the issue for the sandbox.

Quickstart selects automatic initial PR publication and a 5 USD configured
session ceiling. Despite the `max_usd_per_day` field name, the SDK applies this
limit per session; it does not aggregate daily spending across runs. See
[budget limits](../administering-co-workers.md#configure-model-roles-and-bounded-runs).
Production agents default to human publication approval. Neither policy merges
the PR. A trial repository can have no CI, but that outcome carries a no-CI
note rather than evidence that tests passed. The factory cannot publish changes
under `.github/`. Start with a bounded issue in a small repository; larger
builds and other toolchains need the setup described in the
[bundle reference](../../examples/dark-factory/README.md#deploy-it-as-the-factory-agent).

## What you need

| Tool | Used for |
|---|---|
| [Docker](https://docs.docker.com/get-docker/) | kind nodes, required only when neither `--context` nor a current kubeconfig context is set |
| [`kind`](https://kind.sigs.k8s.io/docs/user/quick-start/#installation) v0.24 or later | the local cluster, required only when neither `--context` nor a current kubeconfig context is set. kind's network plugin enforces NetworkPolicy from v0.24. |
| [`kubectl`](https://kubernetes.io/docs/tasks/tools/) | Kubernetes operations, required for every context |
| [`helm`](https://helm.sh/docs/intro/install/) | Curie installation, required for every context |
| The released `curie` CLI | install it with the command below |
| An [Anthropic](https://console.anthropic.com/) API key (`sk-ant-`) or an [OpenRouter](https://openrouter.ai/) API key (`sk-or-`) | Anthropic defaults to `claude-sonnet-5-5` for implementation and `claude-opus-5-5` for review. OpenRouter defaults to `z-ai/glm-5.3-flash` for implementation and `openai/gpt-6.1-sol` for review. Plan on about 5 USD of available OpenRouter credit per run; actual spend varies. The command asks once, or reads `CURIE_CREDENTIALS`. |
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

Install the latest release. Run this in Bash:

```bash
curl -fsSL https://raw.githubusercontent.com/curie-eng/curie/main/get-curie.sh | bash
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
By default, the installer selects the latest published release. Confirm that `curie --version`
reports the release you installed before continuing. A stale older binary can report
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

### Apple Silicon recovery for older charts

The current source chart detects native ARM64 and leaves the preflight enabled.
Older released charts can reject ARM64 because they check an x86-only AVX flag.
If the first quickstart command stops with that specific preflight failure on
an ARM64 node, keep the same Bash session and trial kubeconfig, then run:

```bash
curie cluster up --context "$CURIE_CONTEXT" \
  --namespace "$CURIE_NAMESPACE" --release "$CURIE_RELEASE" \
  --set preflights.avxCheck.enabled=false
```

Rerun the first quickstart command after the installation succeeds to obtain
the App registration link. This recovery is only for the older architecture
check on ARM64. On x86, keep the AVX check enabled; the pinned x86 ClickHouse
build requires it. Do not downgrade ClickHouse separately from Langfuse.
For the README recipe, use its `CURIE_FACTORY_KUBECONFIG` and explicit
`kind-$CURIE_FACTORY_CLUSTER` context as shown there.

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

To serve more than one repository from the same agent, pass a comma separated
list such as `--repo acme/widgets,acme/gadgets` on both runs and install the App
on each. The first repository is the agent's git-flow repository, and each one
gets its own GitHub surface binding.

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
commit. It compares inferred declarations with the tool manifest of the selected,
digest-pinned runner image and warns about missing tools or version mismatches.
If the selected image's inventory cannot be read, it explicitly reports the
inventory as unknown. The operator's `--repo-runner OWNER/REPO=SPEC` option builds
and pins an image with the tools specified for an already bound repository.
Otherwise, inference is advisory and installs no inferred tools. If a workflow
names a version file that is missing, the version is
reported as unknown with a warning naming the file, and setup continues.
Authentication failures and malformed GitHub responses remain errors.
A repository with no declarations gets a `no toolchain signals` note.
The cluster factory command performs the same check for its allowlisted repositories.
Without `--app-id`, it prints `toolchain inference skipped: no --app-id`.
Dry-run describes the inference without reading GitHub.

For a repository-specific image based on issue and PR history, use the local
[factory environment setup skill](factory-environments.md) before reviewing a
`--repo-runner` build and binding change. Its companion maintenance skill
reviews later `needs human` outcomes for image and non-image repairs.

Factory commits are authored by the GitHub App's bot identity. Before any Helm
call, App setup resolves `<slug>[bot]` through GitHub's user API and configures
`worker.publication.gitUserName=<slug>[bot]` and
`worker.publication.gitUserEmail=<id>+<slug>[bot]@users.noreply.github.com`.
The numeric id is the bot user's id, not the App ID. Both quickstart's second
pass and `curie cluster factory` use this identity when neither author field
has a recorded operator value. If either field is already set, both fields
stay unchanged.

For an App already connected to the release, pass its bare slug with
`curie cluster factory --mention <slug>` to apply the same bot author identity.
This public user lookup needs neither a new private key nor an installation
token. Reconfiguring intake without App credentials or `--mention` keeps the
release's existing author configuration.

To choose another author identity, set it on the release before rerunning App
setup:

```bash
curie cluster upgrade --context "$CURIE_CONTEXT" \
  --namespace "$CURIE_NAMESPACE" --release "$CURIE_RELEASE" \
  --set worker.publication.gitUserName='Example Operator' \
  --set worker.publication.gitUserEmail=operator@example.com
```

The bot lookup still runs before Helm reads, including when the release has an
operator override. If GitHub cannot return a numeric bot user id, setup exits
3 without calling Helm and prints an author override hint. Restore access to
the bot user lookup before retrying setup.

An explicit `--model <id>` selects the implementer model instead of the
credential's default, even when it matches the OpenRouter default. Both
reviewers use the `opus` alias; the runner resolves it to the credential's
default reviewer model. To pin another reviewer model for this agent, use the same context,
namespace and release as the quickstart:

```bash
read -r -p "Reviewer model id for your provider: " CURIE_REVIEWER_MODEL
```

After answering, apply the override:

```bash
curie cluster overrides dark-factory --context "$CURIE_CONTEXT" \
  --namespace "$CURIE_NAMESPACE" --release "$CURIE_RELEASE" \
  --reviewer-model "$CURIE_REVIEWER_MODEL"
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
including the reviewers. Separately, quickstart configures the agent's
USD ceiling to 5 USD per session despite the daily-named configuration field.
Multiple sessions can therefore exceed 5 USD in aggregate in one day.
Having provider credit does not raise the configured session ceiling.

The second run configures these values:

| Value | Source |
|---|---|
| Mention | The App slug, so a revision comment can mention `@<slug>`. |
| Allowlist | The repositories you passed, checked against the App installation. |
| Label | `curie-factory`, created on each repository when it is missing. |
| Intake | Polling (`factory.intake=poll`). No webhook secret. |
| Agent | `dark-factory`, from the published runner image, environment `prod`. |
| GitHub surface | `github=<owner>/<name>`, one per repository |
| Execution deadline | 10800 seconds |
| Publication | `auto` |
| Budget | 5 USD configured session ceiling; no shared daily ledger |

The card image stays a text checklist unless you later set a public
`--card-base-url` on the cluster factory command. Polling does not need one.

## Forge flags and chart values

Quickstart writes the factory's forge settings for you. These flags select them:

1. `--code-host <kind>` and `--tracker <kind>` choose where the factory publishes
   and where it takes issues. Both default to `github`. `--tracker jira-cloud`
   with `--code-host github` is installed; see [Jira Cloud](#jira-cloud).
   `--code-host gitlab --tracker gitlab` is installed; see [GitLab](#gitlab).
   `--code-host bitbucket-cloud` or `bitbucket-dc` with `--tracker
   jira-cloud` is installed; see
   [Bitbucket Cloud and Data Center](#bitbucket-cloud-and-data-center).
2. `--forge-url`, `--token-file`, `--ca-file` and `--review-account` (repeatable)
   describe a non-GitHub forge: its base URL, a file holding its access token,
   a CA bundle for a private certificate, and the accounts whose reviews count.
   The token stays in the file and never enters argv. Each is refused for
   `github`, which authenticates with the App.
3. `--forge-username`, `--start-account` (repeatable) and `--start-group` are
   the Jira Cloud flags: the email the token belongs to, and who may start
   work. Each is refused for `github`.
4. `--tracker-url` and `--tracker-token-file` are the Jira site URL and token
   file beside a Bitbucket code host, whose own URL and token are
   `--forge-url` and `--token-file`. Beside GitHub, Jira takes `--forge-url`
   and `--token-file` instead, and these two are refused.

The chart holds the result in one top-level `factory:` block, which the API
reads as a single forge configuration document:

1. `factory.codeHosts` and `factory.trackers` list the connections. Each entry
   has a `name`, `kind`, `host` and `apiUrl`, and a `credential`. A tracker also
   carries its `label`, `mention` and `pollIntervalSeconds`.
2. A `credential` never holds key material. It names an existing Secret with
   `existingSecret` and `existingSecretKey`, and the chart mounts that Secret
   read-only. The chart refuses a credential missing either field.
3. `factory.bindings` join a tracker to repositories. Each entry under
   `repos` has an `alias`, a `codeHost`, a `projectId`, a `path`, optional
   `bases` and `defaultBase`, and an optional `ci` policy (`required` with
   `key`, `paths` and `pendingKeyPrefix`, and `metadataRerunKeys`).
4. `factory.codeHosts[].egress` and `factory.trackers[].egress` list the CIDRs
   and ports the entry needs. A GitHub entry with none gets the chart's built-in
   GitHub set. `factory.sandboxEgress.<agent>: true` enrolls an agent in that
   egress policy.
5. `factory.enabled`, `factory.intake` (`poll` or `webhook`),
   `factory.cardBaseUrl` and `factory.ciWaitSeconds` replace the matching
   `api.*` settings. `factory.baseLabelSettleSeconds` controls the wait after
   the selection label is applied before the factory reads the current base
   labels at sandbox acquisition. It defaults to 60 seconds; zero is valid.

The old `api.githubFactory*` keys are removed. To upgrade a v0.12.5
installation configured with `curie cluster factory`, use a CLI containing
the fix for [#4628](https://github.com/curie-eng/curie/issues/4628) and run the
normal `curie cluster upgrade --to <target-version>` flow. It automatically
migrates retained factory settings before candidate validation, preserving
labels, mention, intake, repository routing, bases, CI policies, credential
references and unrelated settings. The same upgrade performs drain,
checkpoint, convergence, canary and known-good recording.

Preview with `--dry-run --json`; migration plan lines and refusals name
settings without printing their values. Conflicting or malformed legacy
configuration and ambiguous bindings refuse before any Helm mutation, leaving
the old release serving.

Identity migration requires explicit repositories in `api.githubRepoAllowlist`
and a retained `api.githubAppExistingSecret` reference; `owner/*` and
inline-only App keys refuse. The CLI machine must reach the retained
`api.githubApiUrl` during live `curie cluster up`, `curie apply`, `curie diff`
and `curie cluster upgrade`, including upgrade `--dry-run`. Each migration
consumer may mint external App installation tokens and create audit entries.
Local `curie cluster up --dry-run` and `curie apply --dry-run` refuse legacy
settings needing live identity resolution. For a private CA, supply
`codeHostTrust.caBundle` with a namespace ConfigMap reference in the upgrade
values overlay.

For GHES, supply explicit typed code-host and tracker egress. Retained
publication CIDRs alone supply only the code-host route. Tracker inference
requires an unambiguous retained connector list with matching API ports and
either the same publication grants or every CIDR covering the literal origin
IP without excluding it. GHES migration may enroll an otherwise unlisted
agent when no factory agent is enrolled and its retained grant covers the
full forge egress union; this also adds per-agent sandbox and warm-pool
membership. The github.com path does not infer enrollment. See
[automatic migration](../operations.md#automatic-migration-of-a-legacy-github-factory)
for the precise trust/egress prerequisites and schema compatibility rules.

The released v0.13.0 CLI lacks this automatic migration, and its suggested
`cluster factory` remedy also fails on legacy installations. The
[historical manual procedure](../operations.md#manual-factory-migration-from-v0125-to-v0130)
applies when using that CLI. See
[factory settings](../operations.md#admitting-a-labelled-github-issue) for the
current configuration model.

`curie cluster status` and `curie doctor` print one line per forge credential,
for example `credential tracker github (github_app): none`. The expiry reads
`known <date>`, `unknown` or `none`. A GitHub App credential is re-minted, so it
reads `none`. A static token with a recorded expiry reads `known`, and the line
adds a rotation warning once the date is past or within 14 days. A token with no
recorded expiry reads `unknown`.

Each credential line also carries its authentication state from a live read
through the API: `accepted`, `rejected` or `unknown`, with a reason. This is
separate from expiry. See [Replace a static token](#replace-a-static-token).

## Replace a static token

This applies to a GitLab, Bitbucket Cloud, Bitbucket Data Center or Jira Cloud
static token. Curie never mints or rotates a static token. The operator
replaces it, and the running API picks the new value up without a restart.
GitHub App keys and OAuth client secrets are not covered by this procedure.

1. Save the new token in a local file outside the repository using your
   editor. Keep it out of command arguments, commits and chat. Look up the
   Secret name and key the install recorded for that credential in the
   release's `existingSecret` and `existingSecretKey` values.
2. Replace only that key of the existing Secret, reading the value from the
   file. A merge patch touches the selected key and leaves every other key in
   the Secret alone, and the token never appears in command arguments:

   ```bash
   printf '{"data":{"<key>":"%s"}}' "$(base64 -w0 < <token-file>)" > <patch-file>
   kubectl --context <ctx> -n <ns> patch secret <secret> \
     --type merge --patch-file <patch-file>
   rm <patch-file>
   ```

   Use the same Secret name and key. A different key or Secret needs a Helm
   upgrade, because the release references the one it recorded.
3. Wait. The credentials are a projected Secret volume at `/etc/curie/forges`,
   and the kubelet refreshes the projected file, typically within about a
   minute. The API rereads the file at its next forge call or git credential
   use. No pod restart, Helm upgrade or reload is needed.
4. Run `curie cluster status`. The replaced credential should read
   `authentication accepted`.

Curie never falls back to an older token. A rejected, missing, empty or
malformed file stops intake for that entry only, and other entries keep
working. Intake resumes by itself once a valid value is in place.

`curie cluster status`, in human form and with `--json`, shows each code host
and tracker credential's authentication state from a live read through the
API, separately from expiry (`known`, `unknown` or `none`).

1. `accepted` means the forge accepted the current token on a read.
2. `rejected` means the forge refused it. A rejected credential marks the
   cluster unhealthy and the command exits nonzero.
3. `unknown` means Curie could not tell: the API is unreachable, there is no
   binding to probe, the file is unreadable or the forge is unavailable.
   Unknown never reads as accepted.

Each `credentials[]` item in the JSON adds `authentication`,
`authentication_reason` and `observed_at` beside the existing expiry fields.

When intake is deferred because a credential was rejected or its file is
unreadable, the API logs a WARNING that names the tracker, binding and label.
It never logs the token.

## Jira Cloud

For a complete local install through the first pull request, follow the
[Jira Cloud and GitHub walkthrough](factory-jira-github-quickstart.md). The
sections here explain credentials and the chart values quickstart writes.

The factory can take its issues from Jira Cloud while it publishes to GitHub.
Pull requests, CI, review feedback and merges stay on GitHub, and the GitHub
App is still required: it reads each repository's id and publishes the pull
requests. Jira gets a status comment from the factory account, edited in place
as the run moves, and a remote link to the pull request. The pull request body
carries `Jira: <KEY> <issue URL>`.

### The factory account

Create a dedicated Atlassian account for the factory and an API token for it
at <https://id.atlassian.com/manage-profile/security/api-tokens>. Write the
token to a file; quickstart reads it from there and stores it in a Secret
that only the API mounts (see [Names and reruns](#names-and-reruns)). Give the
account these permissions:

1. Browse Projects, on the projects the factory should serve and no others.
2. Add Comments.
3. Edit Own Comments, so the factory can edit its status comment in place.
4. Edit Issues.
5. Link Issues.
6. Browse users and groups, only when start authority is a group.

Scope comes from Jira's permissions, not from Curie configuration. The factory
sees every issue the factory account can browse that carries the label, and
there is no project list to set. To keep the factory out of a project, do not
grant the account Browse Projects on it.

A classic API token uses the site root as `--forge-url`, for example
`https://example.atlassian.net`. A scoped token uses the API gateway,
`https://api.atlassian.com/ex/jira/<cloudId>`; the cloud id is the one
`https://<site>.atlassian.net/_edge/tenant_info` reports. Jira answers a scoped
token with 401 on the site root, so match the URL to the token. Either way,
quickstart reads the site host from Jira for issue links, and the tracker's
`apiUrl` is the URL you passed. The example below uses a classic token; for a
scoped token, pass `--forge-url https://api.atlassian.com/ex/jira/<cloudId>`.

### Who may start work

`--start-account <accountId>` (repeatable) allows those accounts to start work;
`--start-group <groupId>` allows the members of one group. Pass one form, not
both. Accounts are matched on the immutable `accountId`, never the email. A
label added by an app or customer account never starts work, so the factory
account cannot start a run even when it is listed by mistake.

### Run quickstart

```bash
curie factory quickstart --repo acme/widgets \
  --tracker jira-cloud --code-host github \
  --forge-url https://example.atlassian.net \
  --token-file /path/to/jira-token \
  --forge-username factory-bot@example.com \
  --start-account 557058:00000000-0000-0000-0000-000000000000
```

Add `--app-id` and `--private-key-file` on a first install, as in
[Rerun with the App](#rerun-with-the-app). On a release that already records
factory intake, leave them off and pass `--agent <name>` to add a Jira agent
to that install, as in [Add another factory agent](#add-another-factory-agent).
Before writing anything, quickstart checks the token against the site, reads
the site's cloud id and the factory account's `accountId`, confirms each start
account is a person (or reads the group), and reads each repository's id with
the App. It then writes the token Secret, the `trackers` and `bindings`
entries below and the Jira egress, and binds the agent to the binding as
`jira=<binding>` instead of one GitHub surface per repository.

### Names and reruns

Quickstart names what it creates after the instance, so several GitLab, Jira
and Bitbucket instances coexist on one release:

1. A new tracker or code host entry is named for its kind and the first 8 hex
   digits of the SHA-256 of its `apiUrl`, for example `jira-cloud-b6a8e8d4`.
2. A new binding is named `<tracker>-<trackerScopeId>`.
3. A new entry's token goes in the Secret `<release>-<entry>-token`, key
   `token`, labelled `app.kubernetes.io/managed-by=curie-quickstart` and
   `curie.dev/release=<release>`.
4. A CA bundle goes in the ConfigMap `<release>-code-host-trust`, key
   `ca.crt`, unless `codeHostTrust.caBundle` already names one. Quickstart adds
   only certificates that bundle lacks, and writes nothing when none are new.

A rerun finds recorded entries by identity, the kind and host for an instance
and the tracker and scope for a binding, whatever they are named, and keeps
their names, Secrets and trigger label. A new tracker always gets the label
`curie-factory`; `--label` names the agent's label, and quickstart prints the
label each tracker actually uses. An equivalent rerun writes no Secret or
ConfigMap.

Quickstart never rotates a token. When a recorded entry's Secret holds a
different token, or a Secret it would create already exists with other
content or belongs to someone else, it refuses before any write and names the
Secret: update or remove that Secret yourself, then rerun. It also refuses a
conflicting URL, an occupied name, a missing referenced CA ConfigMap, and a
Secret, ConfigMap or Helm revision that changed while it ran.

### The chart values it writes

The identifiers below are examples. The tracker's `mention` is the factory
account's `accountId`; the binding's `trackerScopeId` is the site's cloud id,
and each repository's `projectId` is its GitHub repository id.

```yaml
factory:
  trackers:
    - name: jira-cloud-b6a8e8d4
      kind: jira_cloud
      host: example.atlassian.net
      apiUrl: https://example.atlassian.net
      label: curie-factory
      mention: "712020:00000000-0000-0000-0000-000000000000"
      credential:
        type: static_token
        username: factory-bot@example.com
        existingSecret: curie-jira-cloud-b6a8e8d4-token
        existingSecretKey: token
      egress:
        - cidr: 192.0.2.10/32
          ports: [{protocol: TCP, port: 443}]
          host: example.atlassian.net
  bindings:
    - name: jira-cloud-b6a8e8d4-00000000-0000-0000-0000-000000000000
      tracker: jira-cloud-b6a8e8d4
      trackerScopeId: 00000000-0000-0000-0000-000000000000
      repos:
        - alias: widgets
          codeHost: github
          projectId: "123456789"
          path: acme/widgets
      defaultRepo: widgets
      startAuthority:
        type: allowlist
        accounts: ["557058:00000000-0000-0000-0000-000000000000"]
```

A group grant replaces `startAuthority` with
`{type: group, groupId: 00000000-0000-0000-0000-000000000000}`. With more than
one repository, an issue picks its repository with one `repo:<alias>` label,
else `defaultRepo`.

In Jira, add the label `curie-factory` to an issue as a start account. The
tracker polls every 45 seconds by default. Removing the label, or moving the
issue to a done status, cancels the run. The factory never changes the
issue's workflow status.

## GitLab

For a complete local install through the first merge request, follow the
[GitLab walkthrough](factory-gitlab-quickstart.md). The sections here retain
the advanced server, credential and chart configuration reference.

The factory can take issues from GitLab and publish merge requests to it, on
gitlab.com or a self-managed server. It labels the issue, opens a merge request
whose description says `Closes #<iid>`, follows the pipeline, answers review
discussions in the thread, and never merges or resolves a thread. The GitLab
server must be version 17.0 or later. See the
[capability reference](../capabilities.md) for verification limits.

### The factory account and token

The token needs the `api` and `write_repository` scopes, and its user needs the
Developer role (access level 30) or higher on the project. A Reporter cannot
start work, and quickstart refuses a token whose user is below Developer,
naming the level it found. The factory never needs merge rights.

1. On gitlab.com Free, create a dedicated bot user and use that user's personal
   access token.
2. On a self-managed server you may also use a service account, a project
   access token or a group access token.

Write the token to a file. Quickstart reads it from there and stores it in a
Secret that only the API mounts; it never prints the token or puts it in argv.

### Run quickstart

```bash
curie factory quickstart --code-host gitlab --tracker gitlab \
  --repo "$CURIE_REPO" --token-file "$CURIE_GITLAB_TOKEN_FILE" \
  --forge-url "$CURIE_GITLAB_URL"
```

Without `--forge-url` the target is `https://gitlab.com`. `--repo` is the full
project path including any subgroups, for example `group/subgroup/project`.

Before changing the cluster, quickstart:

1. Refuses a server older than 17.0.
2. Reads the token and refuses one that is inactive or missing `api` or
   `write_repository`, naming the missing scope.
3. Resolves the project to its numeric id, default branch and issues setting.
4. Refuses an access level below 30.
5. Creates the missing labels, as described below.

`--dry-run` runs these checks and prints the steps it would take, including the
`gitlab=<path>` surface, without writing anything.

### Self-managed servers

1. **Sub-path installs.** `--forge-url` is the web base, for example
   `https://host/gitlab`. Quickstart derives the API URL by appending
   `/api/v4`, so the tracker's `apiUrl` is `https://host/gitlab/api/v4`.
2. **Custom CA.** `--ca-file <pem>` must hold only PEM certificates. They are
   merged into the release's one `codeHostTrust.caBundle` ConfigMap, so the
   API trusts a private certificate. See [Names and reruns](#names-and-reruns).
3. **Plain http.** Only `localhost` and `127.0.0.1` are accepted without TLS.

### Labels

Quickstart creates the trigger label `curie-factory` and the state labels
`curie-factory:queued`, `curie-factory:running`, `curie-factory:pr-open` and
`curie-factory:needs-human` on the project, and leaves any that already exist.
These are plain labels. GitLab scoped labels are not used.

### Surface

The agent is bound with one surface per project, `gitlab=<group/subgroup/project>`,
added with `curie cluster surfaces`. The tracker's `mention` is the token
user's username, and the binding records the project id as both `project_id`
and `tracker_scope_id`.

### Token expiry

Quickstart prints `Token expires <YYYY-MM-DD> (<n> days)`, or `Token has no
expiry`, and records the date in the credential. Curie never mints or rotates
a static token; replace it before that date as described in
[Replace a static token](#replace-a-static-token).

### Polling cost

Intake is polling only. Quickstart writes `factory.enabled: true` and
`factory.intake: poll` with the GitLab entries, so a fresh install admits
labelled issues without a separate intake step. Every poll interval (`pollIntervalSeconds`) the factory
reads one page of the issue listing, then two reads for each issue updated since
the last poll and one read for each issue that is running.
## Bitbucket Cloud and Data Center

Bitbucket has no issue tracker the factory reads, so a Bitbucket code host
pairs only with a Jira Cloud tracker. The ticket is a Jira issue, and the pull
request, its build statuses and its review comments are on Bitbucket.

Each kind takes a static access token, never a username and password:

1. **Bitbucket Cloud** takes a repository, project or workspace access token
   with the `repository:write` and `pullrequest:write` scopes. API calls send
   it as a Bearer token, and git uses it as the password for the user
   `x-token-auth`. The entry's `host` is `bitbucket.org` and its `apiUrl` is
   `https://api.bitbucket.org/2.0`. A repository's `projectId` is its UUID,
   braces included, and its `path` is `<workspace>/<repo slug>`. The
   mergeability read uses the token's pull request read scope (covered by
   `pullrequest:write`) and never merges.
2. **Bitbucket Data Center** 9.4.0 or newer takes a project or repository HTTP
   access token with `REPO_WRITE`, sent as a Bearer token to the API and to
   git. An older server is refused. The entry's `host` is the server's
   hostname and its `apiUrl` is its base URL, including any context path. A
   repository's `projectId` is its numeric id, and its `path` is
   `<PROJECT KEY>/<repo slug>`.

A credential that sets `username` is refused for both kinds. A Bitbucket
access token cannot read whether a person has write access, so review feedback
on a pull request is acted on only when its author is listed in the binding's
`reviewFeedbackAllowlist` under the code host's name: Atlassian `account_id`
values on Cloud, numeric user ids on Data Center. Feedback is comments; an
approval or a "needs work" status with no text is not feedback. A Data Center
server with a private certificate needs its CA through `--ca-file`, which
lands in `codeHostTrust`.

### Run quickstart for Bitbucket

Quickstart installs Curie, then writes one Bitbucket code host named
`bitbucket`, the Jira tracker and one Jira binding named `jira`, and binds the
agent as `jira=jira`. It serves one repository. Prepare the Jira factory
account and token as [Jira Cloud](#jira-cloud) describes, and pass them with
`--tracker-url` and `--tracker-token-file`. The Bitbucket flags are
`--token-file`, `--review-account` (repeatable), and on Data Center
`--forge-url` and, for a private certificate, `--ca-file`. A Bitbucket install
registers no GitHub App, so `--app-id`, `--private-key-file` and `--org` are
refused. The identifiers below are examples:

```bash
# Bitbucket Cloud: --repo <workspace>/<repo slug>
curie factory quickstart --repo example-workspace/example-repo \
  --tracker jira-cloud --code-host bitbucket-cloud \
  --token-file /path/to/bitbucket-token \
  --review-account '712020:00000000-0000-0000-0000-000000000000' \
  --tracker-url https://example.atlassian.net \
  --tracker-token-file /path/to/jira-token \
  --forge-username factory-bot@example.com \
  --start-account 557058:00000000-0000-0000-0000-000000000000

# Bitbucket Data Center: --repo <PROJECT KEY>/<repo slug>
curie factory quickstart --repo FAC/demo \
  --tracker jira-cloud --code-host bitbucket-dc \
  --forge-url https://bitbucket.example.invalid \
  --ca-file /path/to/ca.pem \
  --token-file /path/to/bitbucket-dc-token \
  --review-account 101 \
  --tracker-url https://example.atlassian.net \
  --tracker-token-file /path/to/jira-token \
  --forge-username factory-bot@example.com \
  --start-account 557058:00000000-0000-0000-0000-000000000000
```

Before writing anything, quickstart checks the Jira token and start authority
as on GitHub, and reads the repository with the Bitbucket token: on Cloud its
UUID, with Atlassian's published Bitbucket address ranges as the egress, and
on Data Center the server version (9.4.0 or newer) and the numeric repository
id, with the addresses the server's host resolves to as the egress. It then
writes the Jira and Bitbucket token Secrets, merges `--ca-file` into the
release's CA bundle ConfigMap, and writes the entries, named as in
[Names and reruns](#names-and-reruns). A Bitbucket access token cannot read a person's account, so look up
each review account once: on Cloud the `account_id` on a comment or pull
request that person wrote, on Data Center the user's numeric `id` from
`/rest/api/latest/users/<slug>`.

After a reinstall or loss of factory storage on Bitbucket Cloud, a still-labelled
issue whose open factory pull request exists is refused with an operator
confirmation comment instead of a duplicate pull request, because Cloud cannot
verify who opened it. Closing or confirming the existing pull request clears
the block. Confirm it is the factory's by removing the trigger label from the
issue and reviewing or merging the pull request as usual. Closing it instead
lets the still-labelled issue be admitted on a later intake pass.
See [Admitting a labelled GitHub issue](../operations.md#admitting-a-labelled-github-issue) for the full rules.

To ask for a revision, a review account comments on the pull request and
mentions the Jira tracker's `mention`, the factory account's `accountId`, as
`@<accountId>`. The factory reads pull request comments on its poll interval
and acts only on one that carries that mention.

### Writing the values by hand

Several Bitbucket repositories in one binding, or two Bitbucket code hosts,
are not a quickstart run; write the `factory:` values yourself.

One Jira site takes exactly one binding. Quickstart already wrote a binding
for your site, so extend that binding rather than adding a second one:

1. Add each further repository under the existing binding's `repos`, with an
   alias no other repository in the binding uses.
2. Keep the binding's `defaultRepo`, its `startAuthority` and its
   `reviewFeedbackAllowlist` entries, adding an allowlist entry for each new
   code host.
3. On each Jira issue, pick the repository with one `repo:<alias>` label. An
   issue without one runs against `defaultRepo`.

A second binding with the same `trackerScopeId` on the same Jira site is
refused when the chart renders, even when it names a second tracker entry for
that site, so `helm upgrade` and `curie cluster upgrade` stop before anything
rolls out. The refusal names both bindings and the site.

The example below extends a quickstart binding on the tracker
`jira-cloud-b6a8e8d4`, carrying a Bitbucket Cloud and a Bitbucket Data Center repository.
These values use example identifiers only. Replace them with your own:

```yaml
factory:
  codeHosts:
    - name: bitbucket-cloud-7b02dee3
      kind: bitbucket_cloud
      host: bitbucket.org
      apiUrl: https://api.bitbucket.org/2.0
      credential:
        type: static_token
        existingSecret: curie-bitbucket-cloud-7b02dee3-token
        existingSecretKey: token
    - name: bitbucket-dc
      kind: bitbucket_dc
      host: bitbucket.example.invalid
      apiUrl: https://bitbucket.example.invalid
      credential:
        type: static_token
        existingSecret: factory-bitbucket-dc
        existingSecretKey: token
  bindings:
    - name: jira-cloud-b6a8e8d4-<Jira site id>
      tracker: jira-cloud-b6a8e8d4
      trackerScopeId: "<Jira site id>"
      repos:
        - alias: example-repo
          codeHost: bitbucket-cloud-7b02dee3
          projectId: "{00000000-0000-4000-8000-000000000001}"
          path: example-workspace/example-repo
        - alias: demo
          codeHost: bitbucket-dc
          projectId: "1"
          path: FAC/demo
      defaultRepo: example-repo
      startAuthority:
        type: allowlist
        accounts: ["<Jira account id>"]
      reviewFeedbackAllowlist:
        bitbucket-cloud-7b02dee3: ["<Atlassian account_id>"]
        bitbucket-dc: ["<numeric user id>"]
```

A Jira binding must name a `defaultRepo`, the alias a ticket runs against when
nothing picks another, and a `startAuthority` of type `allowlist` (Jira
account ids) or `group` (a Jira `groupId`): Jira cannot report repository
write access, so the default write check is refused. The tracker entry
under `factory.trackers` is the Jira Cloud half of the pairing, as
[Jira Cloud](#jira-cloud) shows it. Each code host also needs its `egress` entries: the Bitbucket
Cloud ranges from `https://ip-ranges.atlassian.com/` on port 443, or the
addresses your Data Center host resolves to on its port.

## Label an issue

For guided issue preparation, install the
[preparation skill](factory-preparation.md#install-prepare-and-hand-off) in your
coding agent. It helps clarify the request, presents the full specification for
review, and can publish and select it after explicit approval using your tracker
connection. The guide includes the install command, supported clients and source
build instructions while the feature awaits release. This uses the factory you
configured above; skill installation alone does not create one.

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
   within 120 seconds of the push, no required check configured, and the code
   host proving the PR mergeable or already merged, the run completes and the status comment says `Note: No CI checks appeared within 120 s.`
   On GitHub that proof is its mergeable state, on Bitbucket Cloud it is the
   `git_mergeability_check` of the pull request mergeability checks reporting
   PASSED clean, and on Bitbucket Data Center it is the merge endpoint.
2. Before the model starts, the runner runs checks declared in
   `.curie/verification.json`. With no declaration it records `not_declared`. <!-- doclint:ignore-line -->
   See [Repository toolchain in the managed sandbox](repository-toolchain-in-the-managed-sandbox.md).
3. The run ends when the agent's turn ends. A command left in the background
   does not keep the run alive.

## Add another factory agent

One install can host several factory agents, each with its own bundle and its
own trigger label. On a release that already records factory intake, run
quickstart with a new agent name and no App flags:

```bash
curie factory quickstart --agent fast --label curie-factory-fast
```

For example, `--agent fast --label dark-factory-fast` deploys an agent named
`fast` that starts work only when an issue carries `dark-factory-fast`. The
command adds the agent to the install without running `cluster up`, App
registration or intake again. Without `--plugin-dir` it deploys the published
dark factory bundle. With it, the bundle in that directory is deployed as given,
using its locked runner image or the platform runner. The execution deadline
defaults to `DEFAULT_DEADLINE_SECONDS`, the same as the first agent. Without
`--label`, the agent's GitHub bindings use the install label.

Two agents may serve the same repository, for example `acme/widgets`, as long
as their effective labels differ. The effective label is the one you passed, or
the tracker's configured label (`curie-factory` by default) when you passed none. The API
refuses a surface whose tracker location and effective label match another
surface's, and it refuses a second surface for the same agent on one location.
The effective label is compared, so an explicit label equal to the tracker's label also collides with an unlabelled surface. Fix
it by giving the new agent a distinct label such as `curie-factory-fast`.

Reference: the same binding is made by hand with
`curie cluster surfaces <agent> --add github=<owner/name> --label <label>`.
`--label` applies only when a surface is added, and the overlap rule above
applies to it as well.

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

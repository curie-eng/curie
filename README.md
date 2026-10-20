# Curie

[![CI](https://github.com/curie-eng/curie/actions/workflows/ci.yaml/badge.svg)](https://github.com/curie-eng/curie/actions/workflows/ci.yaml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Latest release](https://img.shields.io/github/v/release/curie-eng/curie)](https://github.com/curie-eng/curie/releases)
[![Docs](https://img.shields.io/badge/Docs-index-blue.svg)](docs/README.md)

**Turn agents into co-workers. Run them in your infrastructure. Start with a Software Factory co-worker**

Curie brings agents into the workspaces your team already uses. Delegate work,
follow progress, approve restricted actions, and inspect the result. The platform
is Apache-2.0 and runs in your infrastructure, with shared memory facilities,
model configuration, and cost visibility.

Start with a **Software Factory co-worker**. Use Codex, Claude Code, or another
coding agent to help investigate and specify a bounded GitHub, GitLab, or Jira
Cloud issue. Delegate the issue to Curie for implementation and review; it
returns a pull request or merge request for your team to accept. You can also
deploy an **SRE co-worker** for Kubernetes investigations, or adapt an existing
agent's instructions and tools to the same platform.

| Start here | First outcome |
| --- | --- |
| [Software Factory](#software-factory-on-your-laptop) | Label an issue and get a reviewed PR or merge request, or a clear failure reason. |
| [SRE co-worker](#sre-co-worker-on-your-laptop) | Ask about your local cluster in Slack; approve configured mutations. |
| [Bring your existing agent](#make-your-existing-agent-a-co-worker) | Reuse its instructions and tools, then connect it to a workspace. |

### See the Software Factory

<a href="https://vimeo.com/1231115133/eadc6eb936">
  <img src="https://i.vimeocdn.com/video/2206199274-0dcf4f345f0dfb2733cf53dfbdb85b3441e3f593dcd23cfdce0c45bb50d46146-d?f=webp&amp;region=us" width="720" alt="Watch the Curie Software Factory demo: GitHub issues to reviewed pull requests.">
</a>

[Watch the Software Factory demo](https://vimeo.com/1231115133/eadc6eb936), then
follow the local quickstart below. The video features GitHub; the quickstarts
also cover supported GitLab and Jira Cloud paths.

## Quickstart

The software factory and SRE co-workers below run on **local kind clusters**. They use the
same platform you can later install on your own Kubernetes infrastructure.

You need Docker running, kind, kubectl, Helm, and an Anthropic or OpenRouter
model credential. Use a Bash terminal and keep the variables in that session.
Allow enough Docker memory and disk for Curie and the SRE observability stack.
The [factory prerequisites](docs/guides/dark-factory-quickstart.md#what-you-need)
list supported machines and tools.

Install the released CLI:

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

Enter your model credential without echoing it, then select a model available
through that provider. For OpenRouter, use its full provider/model identifier.

```bash
read -rsp 'Model credential: ' CURIE_CREDENTIALS
printf '\n'
export CURIE_CREDENTIALS
read -rp 'Model ID: ' CURIE_MODEL
export CURIE_MODEL
```

### Software Factory on your laptop

Choose where your issues and code live:

| Issues | Code and result | Start here |
| --- | --- | --- |
| GitHub | GitHub pull request | Follow the GitHub steps below |
| GitLab | GitLab merge request | [GitLab quickstart](docs/guides/factory-gitlab-quickstart.md), for gitlab.com or self-managed GitLab |
| Jira Cloud | GitHub pull request | [Jira Cloud with GitHub quickstart](docs/guides/factory-jira-github-quickstart.md) |
| Jira Cloud | Bitbucket Cloud or Data Center pull request | [Jira Cloud with Bitbucket quickstart](docs/guides/dark-factory-quickstart.md#run-quickstart-for-bitbucket), for Bitbucket Cloud or Data Center 9.4.0 or later |

#### GitHub

Create a GitHub repository with an initial commit, or use a small existing
repository. Write a bounded issue with clear acceptance criteria.

**1. Start the local factory setup.** A dedicated kubeconfig keeps this trial
separate from your other clusters.

```bash
read -rp 'Repository (owner/name): ' CURIE_FACTORY_REPO
CURIE_FACTORY_CLUSTER="curie-factory-$(date +%s)"
CURIE_FACTORY_KUBECONFIG="$(mktemp)"
KUBECONFIG="$CURIE_FACTORY_KUBECONFIG" curie factory quickstart \
  --repo "$CURIE_FACTORY_REPO" --kind-name "$CURIE_FACTORY_CLUSTER" \
  --model "$CURIE_MODEL"
```

The command creates a local kind cluster, installs Curie, and prints a GitHub
App registration link. Open that link, create the App, generate its private key,
and install it on the selected repository. Note the App ID and the absolute
path of the downloaded key file.

On Apple Silicon, an older released chart can stop here with an AVX preflight
error. ARM64 does not use x86 AVX. Recover only that architecture-specific
failure in this trial cluster, then rerun the quickstart command using the
same variables. Do not repeat the cluster-name or kubeconfig initialization:

```bash
KUBECONFIG="$CURIE_FACTORY_KUBECONFIG" curie cluster up \
  --context "kind-$CURIE_FACTORY_CLUSTER" --model "$CURIE_MODEL" \
  --set preflights.avxCheck.enabled=false
KUBECONFIG="$CURIE_FACTORY_KUBECONFIG" curie factory quickstart \
  --repo "$CURIE_FACTORY_REPO" --kind-name "$CURIE_FACTORY_CLUSTER" \
  --model "$CURIE_MODEL"
```

The current source chart detects native ARM64 with the preflight enabled.
Keep the check enabled on x86. See the
[ARM recovery details](docs/guides/dark-factory-quickstart.md#apple-silicon-recovery-for-older-charts).

**2. Complete the setup with your App.**

```bash
read -rp 'GitHub App ID: ' CURIE_FACTORY_APP_ID
read -rp 'Private key file (absolute path): ' CURIE_FACTORY_APP_KEY
KUBECONFIG="$CURIE_FACTORY_KUBECONFIG" curie factory quickstart \
  --repo "$CURIE_FACTORY_REPO" \
  --kind-name "$CURIE_FACTORY_CLUSTER" \
  --model "$CURIE_MODEL" \
  --app-id "$CURIE_FACTORY_APP_ID" \
  --private-key-file "$CURIE_FACTORY_APP_KEY"
```

**3. Prepare and delegate the issue.** In the repository checkout, install the
preparation skill for your coding agent using a CLI build that contains it:

```bash
curie factory prepare install --agent codex
```

In Codex, invoke `$prepare-factory-ticket <issue-url>`. Answer its questions,
review the complete specification, then choose **publish and start the factory**.
The agent verifies the published issue and applies the confirmed factory label.
The [preparation guide](docs/guides/factory-preparation.md) covers Claude Code,
Copilot, Gemini and Cursor, global installation, and source builds when your
released CLI does not yet include this command. Skill installation does not
replace the factory setup in steps 1 and 2.

You can also prepare the issue yourself and add `curie-factory` in GitHub. The
account adding the label needs write or admin access to the repository.

The issue status comment shows progress through planning, implementation, review,
publication, and CI. Inspect the resulting PR and its verification information.
A failed or ambiguous task reports its reason. Mention the configured App in a
new comment to request a revision.

The trial enables automatic initial PR publication. Curie does not merge the PR;
your team accepts the result. Production agents default to human publication
approval. Configure implementation and reviewer models explicitly, then use
usage estimates and your accepted outcomes to evaluate costs.

See the [complete factory quickstart](docs/guides/dark-factory-quickstart.md)
for checkpoints, troubleshooting, timing and cost guidance, and cleanup.
The [factory overview](docs/software-factory.md) explains review and CI limits.

When finished with the trial, delete its local cluster:

```bash
kind delete cluster --name "$CURIE_FACTORY_CLUSTER"
```

### SRE co-worker on your laptop

Create a Slack app with Socket Mode and interactivity, install it in your
workspace, and invite it to a channel. Collect its app-level token, bot token,
channel ID, and the Slack user ID of an approver. Follow
[Slack app setup](docs/your-first-slack-agent.md#2-get-the-slack-tokens)
using the [app manifest](apps/dispatcher/slack-app-manifest.yaml).

**1. Create a local cluster and select your Slack channel.**

```bash
CURIE_SRE_CLUSTER="curie-sre-$(date +%s)"
CURIE_SRE_KUBECONFIG="$(mktemp)"
kind create cluster --name "$CURIE_SRE_CLUSTER" --kubeconfig "$CURIE_SRE_KUBECONFIG"
read -rp 'Slack channel ID: ' CURIE_SRE_CHANNEL
read -rp 'Approver Slack user ID: ' CURIE_SRE_APPROVER
```

**2. Install the co-worker and its observability stack.**

```bash
KUBECONFIG="$CURIE_SRE_KUBECONFIG" curie example sre-bot install \
  --context "kind-$CURIE_SRE_CLUSTER" --observability \
  --slack-channel "$CURIE_SRE_CHANNEL" --approvers "$CURIE_SRE_APPROVER"
```

The installer provisions Curie, Grafana, Loki, Tempo, and Prometheus, creates
the Kubernetes connector identity, binds the approval route, and deploys the
SRE bundle. The exported model credential from the quickstart is required for
real replies.

**3. Connect Slack.**

```bash
read -rsp 'Slack app-level token: ' SLACK_APP_TOKEN
printf '\n'
read -rsp 'Slack bot token: ' SLACK_BOT_TOKEN
printf '\n'
export SLACK_APP_TOKEN SLACK_BOT_TOKEN
KUBECONFIG="$CURIE_SRE_KUBECONFIG" curie cluster comms \
  --context "kind-$CURIE_SRE_CLUSTER" --slack
```

**4. Ask it to investigate.** Mention the bot in the bound Slack channel:

> Which workloads in this cluster are unhealthy? Show the evidence and suggest a next step.

Read-only Kubernetes tools run immediately. Configured mutations request a fresh
human approval; the default write permissions are limited to the disposable
`sre-demo` namespace. An approval does not expand those permissions.

Use a dedicated Slack app for this trial: one app must connect to only one Curie
release. GitHub is optional for triage; repository work needs the separate
allowlist and GitHub App configuration in the
[SRE guide](examples/sre-bot/README.md). That guide also covers installation
details, permission boundaries, and demo scenarios.

When finished with the trial, delete its local cluster:

```bash
kind delete cluster --name "$CURIE_SRE_CLUSTER"
```

## Make your existing agent a co-worker

Keep your agent's useful instructions and tools. Curie provides workspace
interaction, deployment, supervision, and operational controls around a
supported bundle.

[Watch how Curie turns an existing agent into a co-worker](https://vimeo.com/1230537103/2d607d04a5),
then follow the steps below to adapt your own agent.

1. **Adapt the package.** Put instructions into skills and declare tools through
   MCP. Existing deterministic code can remain behind a tool wrapper.
2. **Test a representative task locally.** Check tool discovery, run your agent
   with a real model, and inspect which tools it used.
3. **Connect a workspace.** Deploy the bundle and bind its Slack channel.
4. **Configure shared controls.** Choose approval routes, memory scope, models,
   and budgets appropriate to the job.

For an existing directory, start by scaffolding the supported bundle alongside
its files:

```bash
curie init --adopt ./my-existing-agent
```

Port the instructions and tool declarations using the
[adoption guide](docs/bring-your-agent.md), then test them:

```bash
cd my-existing-agent
curie skill check --json
curie skill up --model "$CURIE_MODEL"
curie skill message "Run a representative task using your tools."
curie skill down
```

Scaffolding preserves existing files; adapting the agent's behavior is manual.
The supported path reuses skills and tools in a Curie bundle. Attaching an
arbitrary agent runtime unchanged requires a separate integration.

Continue with [bringing your agent](docs/bring-your-agent.md),
[connecting Slack](docs/your-first-slack-agent.md), and
[administering co-workers](docs/administering-co-workers.md).

## Run it in your infrastructure

Both examples and your supported custom agents use the same platform.
Administrators control deployment, credentials, approvals, model settings,
memory, and bounded runs. Configured model providers and workspace services
receive the integration data they need.

1. [Production operations](docs/operations.md): installation, backing stores,
   egress, upgrades, and operational limits.
2. [Administrator guide](docs/administering-co-workers.md): approvals, memory,
   models, budgets, inspection, and stop controls.
3. [Factory outcome measurement](docs/guides/measuring-factory-outcomes.md):
   accepted work, estimated spend, and human review effort.
4. [Capabilities](docs/capabilities.md): supported integrations and their limits.
5. [Documentation](docs/README.md): guides and technical references.

Plain `curie cluster up` infers Anthropic or OpenRouter egress from an unambiguous
credential prefix. Before Helm runs, a `NotFound` GET for the rendered gVisor
RuntimeClass makes Curie apply `security.gvisor.mode=off`, unless the chart
creates that RuntimeClass. A `Forbidden` GET retains one retry only after
admission reports `RuntimeClass "gvisor" not found`. Other read errors and
admission refusals stay closed. Each inference is printed with the equivalent
override. Ambiguous credential shapes still need an explicit
`--allow-egress-host`, and explicit values that contradict detected facts are
errors.

For the `skill`, `local`, and `cluster` tiers, see the
[CLI target guide](cli/README.md#using-different-tiers) and
[bundle quickstart](QUICKSTART.md). The architecture reference covers
[local to production parity](ARCHITECTURE.md#local-to-production-parity) and
[what is built vs deferred](ARCHITECTURE.md#what-is-built-vs-deferred).

## Contributing to Curie

See [CONTRIBUTING.md](CONTRIBUTING.md) for contributor setup and checks, and
[AGENTS.md](AGENTS.md) for repository instructions. Join the
[Curie Discord community](https://discord.gg/YZASub2d5B) to discuss the project.

## License and trademarks

Curie is released under the [Apache License 2.0](LICENSE). See [NOTICE](NOTICE)
for attribution and [TRADEMARKS.md](TRADEMARKS.md) for use of the Curie name.

If Curie is useful to you, especially if you build on it commercially, we'd
love a link back to [github.com/curie-eng/curie](https://github.com/curie-eng/curie).
It is a friendly request, not a license condition: nothing in the Apache License
requires it, and you are free to use Curie whether or not you do.

## Where do I go next?
- Open open the
[interactive architecture atlas](https://htmlpreview.github.io/?https://github.com/curie-eng/curie/blob/main/docs/architecture-atlas/index.html)
to explore current and planned flows, maturity-rated seams, ADRs, and versioned snapshots.
- [`ARCHITECTURE.md`](ARCHITECTURE.md#component-map) -- the component diagram, the
message-flow and deploy-flow sequence diagrams, and the built/in-progress
split.
- [docs/adr/](docs/adr/) -- the load-bearing architecture decisions (Agent
Sandbox as substrate, stateless-first sessions, Langfuse as the
observability backbone, the frozen ACI, security rails as chart defaults,
adopt-not-build boundaries), each with the live-cluster evidence behind it.
- [docs/agents.md](docs/agents.md): the verification contract for an agent
driving Curie. The exact commands that prove an outcome, and the rule that a
file existing or a string appearing in output is never evidence. This is for
an agent using Curie, not one working in this repo.
- [`AGENTS.md`](AGENTS.md) -- the operative rules for anyone (human or agent)
working in this repo: the verify commands, the dev stack, the
frozen-contract escalation rule, and the build gotchas. Each top-level
directory also has its own scoped `CLAUDE.md` with rules specific to that area.

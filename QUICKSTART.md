# Curie quickstart

Welcome. This gets you a first agent reply in about a minute — no credentials,
no cluster, no Slack. It runs the `skill` target only: just the runner
container on your host Docker daemon, talking straight to the agent. Here,
`skill` names the runner only tier. The authored skill artifact lives at
`skills/<name>/SKILL.md`. For the
full tour across all three tiers through to production, see the
[README Quickstart](README.md#quickstart); this doc goes deeper on the `skill`
path itself — configuring a real model, alternate providers, and example
bundles. To go further still — a real local model, the full local/cluster
runbooks, or working on Curie itself — see [Where to go next](#where-to-go-next).

## Before you start

> **Note for zsh users on macOS:** Enable comment support before copy-pasting
> these commands by running `setopt interactivecomments` first, or add it to
> your `~/.zshrc` to make it permanent.

- **Docker** running locally.
- The **`curie`** binary on your PATH. One command downloads the prebuilt
  binary for your platform, verifies its signed checksum, and installs it:

  ```bash
  curl -fsSL https://raw.githubusercontent.com/curie-eng/curie/main/get-curie.sh | bash
  ```

  It always verifies the sha256 and runs `cosign verify-blob` when cosign is on
  PATH (`CURIE_REQUIRE_COSIGN=1` requires it). To run the download-verify-install
  steps by hand instead, see
  [`docs/release-verification.md`](docs/release-verification.md#verify-the-cli-before-installing-it).

  Working on Curie itself? Run that same script from a source checkout and it
  builds the CLI instead of downloading it — see
  [Where to go next](#where-to-go-next).

## The minimum bundle

`curie init` scaffolds seven files. Only these are required, and it is worth
knowing which — when a deploy is rejected or an agent behaves oddly, this is
what actually has to be true.

**One file, one field** is a complete, valid bundle:

```
my-agent/
└── .claude-plugin/
    └── plugin.json     {"name": "my-agent"}
```

It boots and answers. Everything else in the format is optional and validated
only when present.

That agent has no instructions, though. **Two files, three fields** is the
smallest useful one:

```
my-agent/
├── .claude-plugin/
│   └── plugin.json          {"name": "my-agent"}
└── skills/
    └── my-agent/
        └── SKILL.md
```

```markdown
---
name: my-agent
description: Answers questions about deployment status.
---

You are a deployment helper. Answer briefly and plainly.
```

`name` and `description` are the only required frontmatter fields; the body is
the agent's instructions.

Everything the scaffold adds beyond this — `evals/cases.json`, `.mcp.json`,
`AGENTS.md`, the `using-curie` primer — is useful and optional. Delete any of
it and the bundle still deploys.

## Your first agent reply

1. **Scaffold a bundle.** This creates a starter skill named for your agent,
   plus an `AGENTS.md` and a `using-curie` harness-primer skill, that you edit
   next.

   ```bash
   curie init my-agent
   cd my-agent
   ```

2. **Boot the runner** with the built-in fake model — offline, instant, no key.

   ```bash
   curie skill up --fake-model
   ```

3. **Ask it something** and watch the reply stream back.

   ```bash
   curie skill message "hello, are you there?"
   ```

4. **Done.** Tear the runner down.

   ```bash
   curie skill down
   ```

That is the full loop: `curie skill up` starts the runner container,
`curie skill message` sends a synthetic event and streams the reply, and
`curie skill down` stops it.

Edit `skills/my-agent/SKILL.md` and re-run steps 2–3 to see your change
answered — `skill up` runs an immutable snapshot of the bundle, so an edit
only reaches a runner after you run `curie skill up` again.

A committed first-party example lives at `examples/weather/`: `cd
examples/weather && curie skill up` runs it from a clean clone. For the
"engine as an in-bundle stdio MCP (Model Context Protocol) server" shape — a
bundle that ships its own tools as a stdio subprocess the harness spawns — see
the template at
[`examples/text-stats-engine/`](examples/text-stats-engine/README.md).

## Level up: a real model

The fake model returns scripted replies. For a genuine answer, drop
`--fake-model` and export a credential first (`curie` forwards it into the
runner container), then re-run `skill up`. Any one of `CLAUDE_CODE_OAUTH_TOKEN`,
`ANTHROPIC_API_KEY`, or `CURIE_CREDENTIALS` works for the Anthropic default:

```bash
export CLAUDE_CODE_OAUTH_TOKEN=...
curie skill up
curie skill message "What's the weather in Paris? Answer in one short sentence."
curie skill down
```

To use a different provider or model instead of the Anthropic default, bring
your own model through OpenRouter on the same `skill` path. Set
`CURIE_CREDENTIALS` to your OpenRouter key and name a model slug via
`--model`. The credential must arrive on `CURIE_CREDENTIALS` specifically,
not `ANTHROPIC_API_KEY` — that, paired with `--image`/`--model`, is what
tells `curie` to route to OpenRouter instead of the Anthropic default:

```bash
CURIE_CREDENTIALS="$OPENROUTER_TOKEN" curie skill up \
  --image ghcr.io/curie-eng/curie-runner:latest \
  --model z-ai/glm-5.2

curie skill message "What's the weather in Paris? Answer in one short sentence."
curie skill down
```

You should get a real answer instead of the canned loop.

## Deploy your own SRE bot

The most complete example in this repo is a production triage bot:
[examples/sre-bot/](examples/sre-bot/README.md). It uses one pinned upstream
Kubernetes MCP connector. Thirteen exact core read tools run immediately, six
exact mutation tools require approval, and unmatched tools deny.

Kubernetes RBAC is the capability boundary. The connector can read enumerated
non-secret operational resources cluster-wide and can mutate workload APIs only
inside the disposable `sre-demo` namespace. It cannot read Secrets or mutate
identity, RBAC, namespaces, nodes, admission configuration, CRDs, or any
cluster-scoped resource.

The supported installer applies that identity, constructs its file-mounted
kubeconfig in memory, installs the observability stack, and deploys the bundle:

```bash
export CURIE_CREDENTIALS=sk-ant-...
curie cluster up --allow-egress-host anthropic --set security.gvisor.mode=off
curie example sre-bot install --observability
curie cluster message "Is any pod crashlooping right now?"
```

Run the installer with `--dry-run` first to inspect its ordered mutation plan.
The optional `--platform-upgrade` flag adds a separate, much wider
zero-argument Job-trigger path; read
[the platform-upgrade Role](examples/sre-bot/manifests/platform-upgrade-role.yaml)
before enabling it. The general Kubernetes connector never receives that Role
or credential.

For manual deployment, apply
[the Kubernetes access manifest](examples/sre-bot/manifests/kubernetes-access.yaml),
construct a kubeconfig for `sre-bot-kubernetes`, store it under the owned
connector key `K8S_KUBECONFIG`, bind the bundle's declared approval route, and
deploy the unchanged bundle:

```bash
curie cluster approvals sre-bot --route-resolution sre-approvals=C0EXAMPLE1
curie cluster approvals sre-bot --list-routes
curie cluster deploy --plugin-dir examples/sre-bot
```

Deploy is refused (`422` / `approval_routes.unbound`) until the route is bound.
The
[permission map](examples/sre-bot/docs/PERMISSION-MAP.md) lists every mutation,
its approval classification, and the RBAC ceiling.

## Where to go next

The `skill` loop is just the runner container. From here:

- **A real local model, no Anthropic key** — the opt-in offline `--local-model`
  demo (a real model in a container over an Anthropic-compatible endpoint, model
  sizing, and gotchas): [`docs/local-model.md`](docs/local-model.md).
- **Run your bundle on the full local platform** — the queue, worker,
  sandbox, and traces — the same path a Slack mention takes — via docker
  compose (`curie local up` → `local deploy` → `local message`). Walkthrough
  and the ticket-verification loop in
  [`docs/onboarding.md`](docs/onboarding.md#the-real-product-loop-with-local).
- **Run it on Kubernetes** — the platform as a Helm release
  (`curie cluster up` → `cluster deploy` → `cluster message`). Full runbook,
  including credentials, web egress, and the Langfuse login, in
  [`docs/operations.md`](docs/operations.md).
- **A complete, real-world bundle** — the SRE triage bot's security shape, its
  gated write path, its RBAC manifests and its falsifiable eval suite, in
  [`examples/sre-bot/README.md`](examples/sre-bot/README.md).
- **Working on Curie itself** — the repo-checkout dev stack, tests, and
  from-scratch walkthrough in [`docs/onboarding.md`](docs/onboarding.md) and
  [`AGENTS.md`](AGENTS.md). One-command bootstrap from a clone:
  `./get-curie.sh` (builds the CLI, then runs `curie install` for deps and
  the runner image).
- **Every command and flag** — the complete reference in
  [`cli/README.md`](cli/README.md); the
  [README target guide](README.md#which-target-do-i-want) has a table to help
  you pick `skill` vs `local` vs `cluster`.

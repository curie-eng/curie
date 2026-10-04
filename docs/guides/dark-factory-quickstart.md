# Dark factory quickstart

`curie factory quickstart` takes you from nothing to a labelled GitHub issue
that Curie's dark factory can turn into a pull request. The command creates a
local [kind](https://kind.sigs.k8s.io/) cluster when you have no Kubernetes
context, installs Curie with gVisor off, and prints a GitHub App registration
link. A second run with the App id and private key turns on polling intake and
deploys the published dark factory image. It never opens a browser and never
runs `gh`.

Polling is the intake. There is no webhook, no tunnel, and no personal access
token. The platform reads the issue for the sandbox.

Budget about 30 minutes for the first pass, most of it waiting on Helm and on
the factory run.

## What you need

| Tool | Used for |
|---|---|
| [Docker](https://docs.docker.com/get-docker/) | kind nodes, required only when neither `--context` nor a current kubeconfig context is set |
| [`kind`](https://kind.sigs.k8s.io/docs/user/quick-start/#installation) v0.24 or later | the local cluster, required only when neither `--context` nor a current kubeconfig context is set. kind's network plugin enforces NetworkPolicy from v0.24. |
| [`kubectl`](https://kubernetes.io/docs/tasks/tools/) | Kubernetes operations, required for every context |
| [`helm`](https://helm.sh/docs/intro/install/) | Curie installation, required for every context |
| `curie` | this command ([releases](https://github.com/curie-eng/curie/releases)) |
| An [OpenRouter](https://openrouter.ai/) API key (`sk-or-`) | the factory model, `z-ai/glm-5.3-flash` by default, and the reviewers, which run `anthropic/claude-opus-5.5`. A run needs about 5 USD of credit. The command asks once, or reads `CURIE_CREDENTIALS`. |
| A GitHub account | your own GitHub App and the trial repository |

The command checks all required tools on `PATH` before running any command or
changing the cluster. If tools are missing, it lists every missing tool with
its official installation link. `--dry-run` performs the same prerequisite
check. An explicit or current context skips the Docker and kind requirements,
including when the context belongs to an existing kind cluster.

A released `curie` deploys the dark factory runner image that release published.
A binary built from a source checkout has no published layer for its own
version and stops before deploy, naming the release install.

## Create the repository

In the GitHub website, create a public repository with one commit (an initial
README is enough). The factory branches from the default branch, so the
repository needs that commit. It does not need CI. Note the `owner/name` form,
for example `acme/widgets`.

## Run the command

```bash
curie factory quickstart --repo <owner>/<name>
```

What it does:

1. When no kubeconfig context is targeted, it creates kind cluster
   `curie-factory` (context `kind-curie-factory`) and scales CoreDNS to one
   replica. A current kind context skips creating a cluster and does not ask.
   A current context that is not kind, including a remote cluster, is not
   used until you confirm it in a terminal. Without a terminal the command
   stops and names `--context <name>` as the way to proceed. An explicit
   `--context <name>` never prompts.
2. It installs Curie with `cluster up`. On the kind path it sets
   `security.gvisor.mode=off` before the first install. It asks for the
   OpenRouter key once when `CURIE_CREDENTIALS` is unset and the release is
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

```bash
curie factory quickstart --repo <owner>/<name> \
  --app-id <APP_ID> --private-key-file <PATH.pem>
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

Before deploying, the second run reads the key's remaining credit from
OpenRouter (the smaller of the key's limit and the account balance). It warns
when that is below 5 USD, the credit one factory run needs. When it cannot read
the credit, for example because the key only lives in the cluster, it prints
`OpenRouter credit not checked` and continues. The check never stops the
command. The ready output names the reviewer model and the per-run credit. A run
that runs out of credit ends with the out-of-credits cause on the issue.

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
`--card-base-url` on `curie cluster factory`. Polling does not need one.

## Label an issue

In the GitHub website, open an issue on the repository and add the label
`curie-factory`. The person who adds the label must have write or admin access.
Anyone else's label is ignored.

Within about a minute the issue gets a status comment. The factory reads the
issue, plans, writes a test, implements, reviews its own diff, and opens one
pull request. It never pushes. Curie publishes the patch from a separate job
with the App's identity.

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
| Work item stays `waiting for sandbox capacity` | Another run holds the sandbox, or the runner pod cannot start | `kubectl -n curie get pods`. The run starts when capacity frees |
| Status comment ends with `Could not complete:` | The run stopped. The comment's cause lines say why | Fix the cause, then relabel |
| `Could not resolve host: github.com` on kind | CoreDNS replica race | Rerun. The command scales CoreDNS to one replica |
| Nothing happens after the label | The labelling user lacks write access, the label name differs, or the agent has no `github=<owner>/<name>` surface | Fix that and relabel. Polling notices the label on its interval |

## Another cluster

Pass `--context <name>` to skip kind and use a remote cluster. The App, polling
intake, and deploy steps are the same. gVisor is left to that cluster. A kind
context still gets gVisor off, because kind nodes do not ship `runsc`.

## Clean up

```bash
kind delete cluster --name curie-factory
```

Delete the GitHub App when you are done with it.

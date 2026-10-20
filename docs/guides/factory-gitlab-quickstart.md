# Software Factory with GitLab

Delegate a GitLab issue and review the resulting merge request. This walkthrough
creates a disposable local kind cluster and uses GitLab for issues, code,
pipelines and review. No GitHub account or GitHub App is required.

Use a small trial project first. Initial publication is automatic; Curie never
merges the merge request. Quickstart configures a 5 USD ceiling per model
session, not an aggregate daily budget. Implementation and review both incur
provider costs. See [budget limits](../administering-co-workers.md#configure-model-roles-and-bounded-runs).

## Prepare the project and tools

Install Docker, kind, kubectl and Helm using the
[shared tool prerequisites](dark-factory-quickstart.md#what-you-need), then
[install the released Curie CLI](dark-factory-quickstart.md#install-curie).
The GitHub account requirement in that table applies only to the GitHub path.
Return here after installation; the remaining steps use GitLab throughout.

Create a GitLab trial project with an initial commit on its default branch and
issues enabled. Use a small repository whose checks already pass. For a Python
trial, commit a `greeting.py` containing a `greet(name)` function that returns
`f"Hello, {name}!"`, and a unittest under `tests/` that checks `greet("Ada")`.
Run `python3 -m unittest discover -s tests -v` before delegation. Configure a
GitLab pipeline to run that command if you want CI evidence from the trial.
The factory is not a substitute for establishing the baseline or the pipeline.

Use gitlab.com or a self-managed GitLab server version 17.0 or later. The
factory token needs `api` and `write_repository` scopes and a user with the
Developer role or higher on the project. On gitlab.com Free, use a dedicated
bot user's personal access token. A self-managed server can also use a service
account, project access token or group access token. Record its expiry and
plan to replace it before expiration. Curie never mints or rotates static
tokens; replace it as described in
[Replace a static token](dark-factory-quickstart.md#replace-a-static-token).

Save the token in a local file outside the repository using your editor. Do not
put it in a command, commit or chat. Quickstart reads the file into a Kubernetes
Secret mounted only by the API. It does not pass the token in command arguments.

## Select an isolated local trial

Use interactive Bash and keep this session open through cleanup. Run each
prompt separately and answer it before copying the next block.

```bash
read -r -p "GitLab project path (group/subgroup/project): " CURIE_REPO
```

```bash
read -r -p "GitLab web base URL (for example https://gitlab.com): " CURIE_GITLAB_URL
```

```bash
read -r -p "GitLab token file (absolute path): " CURIE_GITLAB_TOKEN_FILE
```

Restrict the token file to your user:

```bash
chmod 600 -- "$CURIE_GITLAB_TOKEN_FILE"
```

Curie ignores spaces, tabs and line breaks around the token, so a file saved
with a trailing newline works. A token with a line break or other control
character inside it is refused at startup, naming the entry and the file.

The URL is the web base, including any self-managed sub-path. For a private
certificate, also follow [custom CA configuration](dark-factory-quickstart.md#self-managed-servers)
and add the same `--ca-file` to every quickstart invocation below.

Enter a real Anthropic or OpenRouter API key silently:

```bash
read -r -s -p "Model API key: " CURIE_CREDENTIALS
printf '\n'
export CURIE_CREDENTIALS
```

Select explicit models for that credential:

```bash
case "$CURIE_CREDENTIALS" in
  sk-ant-*) CURIE_MODEL=claude-sonnet-5-5; CURIE_REVIEWER_MODEL=claude-opus-5-5 ;;
  sk-or-*) CURIE_MODEL=z-ai/glm-5.3-flash; CURIE_REVIEWER_MODEL=openai/gpt-6.1-sol ;;
  *) printf 'Use an Anthropic or OpenRouter key.\n'; false ;;
esac
```

Continue only after that block succeeds. You can change the two model variables
to other models supported by your provider before installation.

Create a separate kubeconfig and unique cluster name once:

```bash
CURIE_PREVIOUS_KUBECONFIG=${KUBECONFIG-}
CURIE_HAD_KUBECONFIG=${KUBECONFIG+x}
CURIE_TRIAL_DIR=$(mktemp -d)
CURIE_TRIAL_KUBECONFIG="$CURIE_TRIAL_DIR/kubeconfig"
touch "$CURIE_TRIAL_KUBECONFIG"
export KUBECONFIG="$CURIE_TRIAL_KUBECONFIG"
CURIE_KIND_NAME="curie-gitlab-$(date +%s)-${RANDOM}"
CURIE_CONTEXT="kind-$CURIE_KIND_NAME"
CURIE_NAMESPACE=curie
CURIE_RELEASE=curie
```

This empty kubeconfig prevents an existing current context from being selected.
Do not repeat this initialization when retrying; keep the same cluster and
kubeconfig until cleanup.

## Install and bind the factory

```bash
curie factory quickstart --code-host gitlab --tracker gitlab \
  --repo "$CURIE_REPO" --forge-url "$CURIE_GITLAB_URL" \
  --token-file "$CURIE_GITLAB_TOKEN_FILE" --kind-name "$CURIE_KIND_NAME" \
  --namespace "$CURIE_NAMESPACE" --release "$CURIE_RELEASE" \
  --model "$CURIE_MODEL"
```

Before changing the cluster, quickstart validates the server, token scopes,
project and role. It creates the trigger and state labels when missing. It
creates the local kind cluster, installs Curie, writes GitLab configuration,
deploys `dark-factory` and binds its project surface. Check that the printed
Kubernetes context equals `CURIE_CONTEXT`; stop if it does not. Kind uses
gVisor off because kind nodes do not ship its runtime.

Pin the reviewer model before adding any trigger label:

```bash
curie cluster overrides dark-factory --context "$CURIE_CONTEXT" \
  --namespace "$CURIE_NAMESPACE" --release "$CURIE_RELEASE" \
  --model "$CURIE_MODEL" --reviewer-model "$CURIE_REVIEWER_MODEL"
curie cluster status --context "$CURIE_CONTEXT" \
  --namespace "$CURIE_NAMESPACE" --release "$CURIE_RELEASE"
```

Release readiness and a successful override are setup evidence, not a completed
factory task. If installation fails, fix the reported cause and rerun the same
quickstart command with the same variables. Do not create another kubeconfig.
Do not supply GitHub App flags: GitLab quickstart refuses them.

## Delegate a bounded issue

You can use the [preparation skill](factory-preparation.md) in your coding agent
to investigate and review the complete issue before submission. Install it with
`curie factory prepare install --agent codex` using a CLI build that contains the
command; the guide covers other clients and source builds. Publication and
selection use your agent's tracker connection when available, with the same
start authority requirements described below.

For the Python trial above, create this issue in GitLab:

> Normalize greeting names. Trim leading and trailing whitespace in `greet`.
> An empty or whitespace-only name should return `Hello, world!`. Preserve
> `Hello, Ada!` for Ada. Add unittest coverage for padded, empty and
> whitespace-only names. Run `python3 -m unittest discover -s tests -v`.
> Do not add dependencies or change the pipeline configuration.

As a human with Developer access or higher, add `curie-factory`. A label from
someone without project write access does not authorize execution. Polling
runs every 45 seconds by default; wait for the issue's status comment rather
than repeatedly relabelling it.

Follow the comment through planning, implementation, review, publication and
CI. State labels are `curie-factory:queued`, `curie-factory:running`,
`curie-factory:pr-open` and `curie-factory:needs-human`. The `pr-open` label is
also used for a GitLab merge request. A queue waiting for sandbox capacity is
not yet an executing model turn.

Open the linked merge request. Check the diff, test changes, verification
information and GitLab pipeline on the current head. Read any unavailable
check or failure explanation. An open merge request is not evidence of passing
CI, and a project without a pipeline cannot provide that evidence. The merge
request description links the issue with `Closes #` followed by its project
issue number. Curie does not merge or resolve review discussions for you.

Inspect work outcomes separately from pod health:

```bash
curie cluster work-items --agent dark-factory --context "$CURIE_CONTEXT" \
  --namespace "$CURIE_NAMESPACE" --release "$CURIE_RELEASE"
```

## Request a revision or stop work

In a new GitLab issue comment, mention the token user's username and state a
specific change, for example `@factory-bot please add a tab-only name test`.
Replace `factory-bot` with your actual token user. The author needs project
write access. Ordinary comments and edits do not start work. Authorized merge
request feedback mentioning the same user is also supported; discussion
replies stay in their thread. Feedback publication requires fresh human
approval even when initial publication was automatic. See
[review feedback](../operations.md#accepting-review-feedback-from-github)
for the publication approval boundary.

Remove `curie-factory` or close the issue to cancel. Running work remains
cancellation requested until termination is observed; an existing merge
request remains open. After resolving a failure, re-add the label deliberately
and inspect existing work first. Do not relabel while a run is healthy.

## Clean up this local trial

Remove the trigger label or close the trial issue, and wait for active work to
stop. Delete only the kind cluster whose name this walkthrough generated:

```bash
kind delete cluster --name "$CURIE_KIND_NAME"
rm -f -- "$CURIE_TRIAL_KUBECONFIG"
rmdir -- "$CURIE_TRIAL_DIR"
if [ "$CURIE_HAD_KUBECONFIG" = x ]; then
  export KUBECONFIG="$CURIE_PREVIOUS_KUBECONFIG"
else
  unset KUBECONFIG
fi
unset CURIE_CREDENTIALS
```

This deletes the trial's Curie installation and stored model and GitLab
credentials. It does not delete your GitLab project or revoke its token.
Revoke a dedicated trial token in GitLab and remove its local file when no
longer needed. Keep shared credentials and pre-existing clusters intact.

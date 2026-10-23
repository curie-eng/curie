# Software Factory with Jira Cloud and GitHub

Delegate a Jira Cloud issue and review the resulting GitHub pull request. This
walkthrough creates a disposable local kind cluster. Jira holds the ticket and
status comment; GitHub holds the code, pull request, checks and review.

You need two separate credentials: a Jira factory account's API token and a
GitHub App installed on the trial repository. Jira does not replace the App.
Initial publication is automatic; Curie never merges the pull request.
Quickstart configures a 5 USD ceiling per model session, not a shared daily
budget. Implementation and review both incur provider costs. See
[budget limits](../administering-co-workers.md#configure-model-roles-and-bounded-runs).

## Prepare the repository, Jira account and tools

Install Docker, kind, kubectl and Helm using the
[shared tool prerequisites](dark-factory-quickstart.md#what-you-need), then
[install the released Curie CLI](dark-factory-quickstart.md#install-curie).
Return here for both quickstart passes and the Jira delegation steps.

Create a small GitHub trial repository with an initial commit on its default
branch. Establish passing checks before delegation. For a Python trial, commit
`greeting.py` with a `greet(name)` function returning `f"Hello, {name}!"`, and
a unittest under `tests/` checking `greet("Ada")`. Run
`python3 -m unittest discover -s tests -v`. Configure GitHub CI to run that
command if you want the trial to produce CI evidence; the factory cannot
publish changes under `.github/`.

Create a dedicated Atlassian factory account and an
[API token](https://id.atlassian.com/manage-profile/security/api-tokens).
Give it Browse Projects only on trial projects, Add Comments, Edit Own
Comments, Edit Issues and Link Issues. This account sees every labelled issue
it can browse; there is no separate project allowlist. Save its token in a
local file outside the repository using your editor. Keep the token out of
command arguments, commits and chat. Quickstart stores it in the API's
Kubernetes Secret.
Curie never mints or rotates this token; to replace it, follow
[Replace a static token](dark-factory-quickstart.md#replace-a-static-token).

Choose a separate human who can authorize trial work. Get that person's
immutable Jira `accountId` from your Jira administrator; an email address is
not a substitute. The recipe uses an explicit account allowlist. A group is
also supported, but requires Browse users and groups for the factory account;
see [start authority](dark-factory-quickstart.md#who-may-start-work).
An app, customer account or the factory account itself cannot start a run.

Use the correct Jira API URL for the token: a classic token uses the site root,
such as `https://example.atlassian.net`; a scoped token uses
`https://api.atlassian.com/ex/jira/` followed by the site's cloud id. Obtain the
cloud id from your site's `/_edge/tenant_info` endpoint. A scoped token on the
site root receives a 401. See the
[Jira credential reference](dark-factory-quickstart.md#the-factory-account).

## Select an isolated local trial

Use interactive Bash and keep the same session open through cleanup. Run each
prompt separately and answer before copying the next block.

```bash
read -r -p "GitHub trial repository (owner/name): " CURIE_REPO
```

```bash
read -r -p "Jira API URL for this token: " CURIE_JIRA_URL
```

```bash
read -r -p "Jira factory account email: " CURIE_JIRA_EMAIL
```

```bash
read -r -p "Jira token file (absolute path): " CURIE_JIRA_TOKEN_FILE
```

Restrict the token file to your user:

```bash
chmod 600 -- "$CURIE_JIRA_TOKEN_FILE"
```

Curie ignores spaces, tabs and line breaks around the token, so a file saved
with a trailing newline works. A token with a line break or other control
character inside it is refused at startup, naming the entry and the file.

```bash
read -r -p "Authorized human Jira accountId: " CURIE_START_ACCOUNT
```

Enter a real Anthropic or OpenRouter model key silently:

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

Continue only after that block succeeds. Change the model variables to other
models supported by your provider if desired, before installation.

Initialize an isolated kubeconfig and unique kind name once:

```bash
CURIE_PREVIOUS_KUBECONFIG=${KUBECONFIG-}
CURIE_HAD_KUBECONFIG=${KUBECONFIG+x}
CURIE_TRIAL_DIR=$(mktemp -d)
CURIE_TRIAL_KUBECONFIG="$CURIE_TRIAL_DIR/kubeconfig"
touch "$CURIE_TRIAL_KUBECONFIG"
export KUBECONFIG="$CURIE_TRIAL_KUBECONFIG"
CURIE_KIND_NAME="curie-jira-$(date +%s)-${RANDOM}"
CURIE_CONTEXT="kind-$CURIE_KIND_NAME"
CURIE_NAMESPACE=curie
CURIE_RELEASE=curie
```

This empty kubeconfig prevents an existing current context from being selected.
Keep these variables and the temporary file for both passes, retries, inspection
and cleanup. Do not reinitialize them when retrying.

## Install and register the GitHub App

The first pass includes all Jira flags but stops at GitHub App registration:

```bash
curie factory quickstart --tracker jira-cloud --code-host github \
  --repo "$CURIE_REPO" --forge-url "$CURIE_JIRA_URL" \
  --token-file "$CURIE_JIRA_TOKEN_FILE" --forge-username "$CURIE_JIRA_EMAIL" \
  --start-account "$CURIE_START_ACCOUNT" --kind-name "$CURIE_KIND_NAME" \
  --namespace "$CURIE_NAMESPACE" --release "$CURIE_RELEASE" \
  --model "$CURIE_MODEL"
```

Check that the printed Kubernetes context equals `CURIE_CONTEXT`. Stop if it
does not. This pass creates kind, installs Curie and prints the App registration
link. It does not configure working Jira intake yet. Kind uses gVisor off
because kind nodes do not ship its runtime.

Open the printed link and create the private GitHub App. The prefilled form
has webhook off and permissions for Metadata, Contents, Issues, Pull requests,
Checks, Commit statuses and Actions. Record the App ID, generate a private key,
and install the App only on the trial repository. The CLI neither creates nor
installs the App for you. Note its slug for GitHub review mentions.

Run each prompt separately:

```bash
read -r -p "GitHub App ID: " CURIE_APP_ID
```

```bash
read -r -p "GitHub App key file (absolute path): " CURIE_APP_KEY_FILE
```

Rerun with the same tracker, credential, authority, cluster and model flags:

```bash
curie factory quickstart --tracker jira-cloud --code-host github \
  --repo "$CURIE_REPO" --forge-url "$CURIE_JIRA_URL" \
  --token-file "$CURIE_JIRA_TOKEN_FILE" --forge-username "$CURIE_JIRA_EMAIL" \
  --start-account "$CURIE_START_ACCOUNT" --kind-name "$CURIE_KIND_NAME" \
  --context "$CURIE_CONTEXT" --namespace "$CURIE_NAMESPACE" \
  --release "$CURIE_RELEASE" --model "$CURIE_MODEL" \
  --app-id "$CURIE_APP_ID" --private-key-file "$CURIE_APP_KEY_FILE"
```

This pass validates the App's repository access, authenticates Jira, resolves the
site and factory account identities, and checks that the start account is a
person. It writes the tracker and repository binding, configures polling and
deploys `dark-factory`. Jira authentication and identity checks occur before
cluster mutation on this finishing pass. If it fails, fix the reported cause
and repeat this command with the same variables.

Pin the reviewer before labelling any ticket:

```bash
curie cluster overrides dark-factory --context "$CURIE_CONTEXT" \
  --namespace "$CURIE_NAMESPACE" --release "$CURIE_RELEASE" \
  --model "$CURIE_MODEL" --reviewer-model "$CURIE_REVIEWER_MODEL"
curie cluster status --context "$CURIE_CONTEXT" \
  --namespace "$CURIE_NAMESPACE" --release "$CURIE_RELEASE"
```

Ready pods prove setup, not a completed task. The one repository in this recipe
is the binding's default. Multi-repository routing uses a `repo:` label followed
by the configured alias; see the
[binding reference](dark-factory-quickstart.md#the-chart-values-it-writes).

## Delegate a bounded Jira ticket

You can use the [preparation skill](factory-preparation.md) in your coding agent
to investigate and review the complete issue before submission. Install it with
`curie skills install --agent codex` using a CLI build that contains the
command; the guide covers other clients and source builds. Publication and
selection use your agent's tracker connection when available, with the same
start authority requirements described below.

For the Python repository above, create a Jira issue in a project the factory
account can browse, with this description:

> Normalize greeting names. Trim leading and trailing whitespace in `greet`.
> An empty or whitespace-only name should return `Hello, world!`. Preserve
> `Hello, Ada!` for Ada. Add unittest coverage for padded, empty and
> whitespace-only names. Run `python3 -m unittest discover -s tests -v`.
> Do not add dependencies or change the CI configuration.

As the human whose `accountId` you allowed, add `curie-factory` to the ticket.
An initial `curie-factory` label is supported and attributed to the creator,
not the reporter; a later label addition uses its author.
Another person's label does not grant start authority. Polling runs every
45 seconds by default. Wait for the factory account's status comment rather
than repeatedly relabelling. The comment is edited in place as work moves
through planning, implementation, review, publication and CI. Curie adds a
remote link to the GitHub pull request, whose body identifies the Jira issue.
It does not move your Jira workflow status.

Open the linked GitHub pull request. Inspect the diff, tests, verification
information and checks on the current head. Read any unavailable check or
failure explanation. An open pull request does not prove that tests passed;
without configured CI, a no-CI result carries no CI evidence. Curie does not
merge the pull request.

Inspect work outcomes separately from release health:

```bash
curie cluster work-items --agent dark-factory --context "$CURIE_CONTEXT" \
  --namespace "$CURIE_NAMESPACE" --release "$CURIE_RELEASE"
```

## Request a revision or cancel

As the authorized Jira human, add a new comment to the still-labelled Jira
ticket. Use Jira's mention picker to mention the factory account, then state a
specific change, such as adding a tab-only name test. Jira must store a real
mention of the factory account's `accountId`; plain text resembling a mention
is insufficient. Ordinary comments and comment edits do not start work.

On GitHub, authorized pull request feedback uses the GitHub App's login instead
and checks the author's repository write access, independently of the Jira
start allowlist. Feedback publication requires fresh human approval even when
initial publication was automatic. See
[GitHub review feedback](../operations.md#accepting-review-feedback-from-github).

Remove the Jira trigger label or move the ticket to a Done status category to
cancel. Running work remains cancellation requested until termination is
observed; an existing GitHub pull request remains open. To retry a resolved
failure, return the ticket to an active status and deliberately re-add the
label as an authorized human. Inspect existing work first.

## Clean up this local trial

Remove the trigger label or move the Jira ticket to Done, and wait for active
work to stop. Delete only this walkthrough's generated kind cluster:

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

This removes the trial's installed credentials. It does not revoke the Jira
token, delete Jira tickets or remove the GitHub App. Revoke a dedicated trial
token and remove its local file when done. Delete an App and downloaded key
only if you created them solely for this trial. Preserve shared credentials,
existing Apps, repositories and pre-existing clusters.

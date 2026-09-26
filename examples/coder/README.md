# Built-in coding tools consumer

This deliberately skill-less bundle demonstrates Curie's built-in coding tools.
Every session receives the Claude Code file tools and
`mcp__curie__publish_changes`; the bundle does not carry a coder skill, GitHub
credentials, clone scripts, approval policy, or publication orchestration.

## Five-minute cluster path

Install Curie with Slack and the operator-owned GitHub credential, then deploy
this bundle. The retained `--workspace` and `--no-workspace` flags are deprecated
compatibility no-ops and are not needed:

```bash
export CURIE_GITHUB_TOKEN=<operator-token>
export SLACK_APP_TOKEN=xapp-...
export SLACK_BOT_TOKEN=xoxb-...

curie cluster up --set 'api.githubRepoAllowlist[0]=acme-corp/acme-bot'
curie cluster comms --slack
curie cluster deploy --plugin-dir examples/coder \
  --agent acme-dev --env dev --slack-channel C0EXAMPLE1
```

The chart default `api.githubRepoAllowlist: []` denies every runtime selection.
`curie cluster deploy --workspace` warns when that list is empty. The retained
`--workspace` flag is a deprecated compatibility no-op; the allowlist is the
real control. `owner/*` allows every repository under that owner.

Invite the bot; ask for the change in plain words and include the single
allowed root repository URL, for example "Make a focused change in
https://github.com/acme-corp/acme-bot: ...". No special phrasing is needed. Curie
acquires that repository when it claims the sandbox, mounts the credential-free
checkout at `/workspace`, and says in its reply which repository it inferred from the
URL. A message without a root URL runs without a managed checkout. A root URL in a
later message of a thread with no repository yet moves the conversation onto a
checkout once the thread is idle. A repository Curie cannot attach, such as one
outside the allowlist, gets a refusal that names the reason.

When the change is ready, ask the agent to publish. The built-in publication
tool posts an approval card in the same thread, ends the turn while approval is
pending, and never pushes from the sandbox. The requester may approve the card;
the platform publishes from outside the sandbox and posts the pull-request URL
back to the thread. The sandbox never receives the operator GitHub credential,
and publication does not depend on a synchronous Slack reply.

## Approving publication without Slack

This bundle declares no `approvalPolicy`, so its publication approval is raised
against the requesting channel. An operator principal
(`curie cluster approvals <agent> --mint-operator-principal <USER>`) cannot
resolve that; it gets `403 operator approval principals can resolve only routes
bound to an explicit user list`. To approve from the CLI, declare the gate in
`.claude-plugin/plugin.json`:

```json
"approvalPolicy": {
  "gates": [
    { "gate": "mcp__curie__publish_changes", "route": "publication" }
  ]
}
```

The route must be bound with an explicit user list before a deploy can succeed.
A deploy whose bundle declares a route the agent has not bound is refused with
no version, bundle, or deployment created; for a new agent, that first deploy
still creates the agent so the route can be bound. Its error prints the bind
command. Bind, then deploy again:

```bash
curie cluster deploy --plugin-dir examples/coder \
  --agent acme-dev --env dev --slack-channel C0EXAMPLE1   # refused; creates acme-dev
curie cluster approvals acme-dev \
  --route-resolution publication=C0EXAMPLE1 \
  --route-approvers publication=users:U0EXAMPLE1
curie cluster deploy --plugin-dir examples/coder \
  --agent acme-dev --env dev --slack-channel C0EXAMPLE1
```

The card then shows `route: publication`, and
`curie cluster approvals acme-dev --resolve <id>` with
`CURIE_APPROVAL_PRINCIPAL_TOKEN` set resolves it.

## Evals

`evals/cases.json` grades this skill-less bundle the same way at every tier.
Coding tools are a built-in session surface (ruling #2154), so the cases exercise
that runtime: the publication tool is named, git must not push, publication
without a managed workspace refuses, and file tools inspect /workspace.
They do not treat a coder skill or an SRE-bot merge as the capability.

With a live-model runner up from this directory, run:

```bash
curie skill eval
```

The cases are written to be falsifiable: a null agent and an input-parrot both
go red, and no expected token is present in its case's input. The
no-mount publication case grades a generic session; a cluster turn that already
acquired an allowlisted checkout takes the approval-card path instead.

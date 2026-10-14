# github-activity: what happened across your repositories, read only

An example bundle for a program manager who needs to know what moved since the
last update: which pull requests merged, which issues were opened and closed,
and what was added to or pulled from a milestone, across several repositories
in one answer. Ask "What merged across our repositories since Monday?" and the
agent reads that window from GitHub and reports it per repository.

It is read only by construction. The agent can look; it cannot comment, label,
close, or move anything.

## What's here

```
github-activity/
  .claude-plugin/plugin.json          bundle manifest and tool policy
  connectors.yaml                     one hosted connector, `github`
  connectors/github/                  the connector source (server.py, Dockerfile)
  skills/github-activity/SKILL.md     how the agent reads and reports a window
```

The connector exposes one tool, `repository_activity(since, until, repositories)`.
`since` is an ISO 8601 time, usually the last time the report ran; `until`
defaults to now. For each repository it returns merged pull requests, opened
issues, closed issues, milestone changes, and milestones created or closed in
the window. Each read is capped at a page limit. When a repository is too busy
to read whole, its `truncated` list names what was cut off, so a short answer
never passes for a complete one.

## Why a write is refused

Three independent layers, so no single mistake opens a write path:

1. The connector process only ever issues GET requests. It has no code path
   that sends any other method, and no write tool.
2. `plugin.json` declares a `toolPolicy` that allows exactly
   `github/repository_activity`. A tool no rule matches is denied, so a write
   tool added to the server later fails closed instead of being inherited.
3. The credential is read only. Even a request that somehow reached GitHub as
   a write is answered with a 403.

The token also never enters the agent's sandbox. `connectors.yaml` delivers it
as a reference to a Kubernetes Secret, which reaches only the connector pod.
The sandbox learns a URL and nothing else.

## Credential

Use one of:

* a fine-grained personal access token scoped to the target repositories with
  Issues: read, Pull requests: read, and Metadata: read;
* a GitHub App installation token minted with exactly those permissions.

Do not use a classic token with `repo` scope: it can write.

## Deploy to a cluster

Create the Secret in the release namespace:

```bash
kubectl create secret generic github-activity-token \
  --namespace <release-namespace> \
  --from-literal=GITHUB_TOKEN=<read-only-token>
```

Set the repositories to report on. In `connectors.yaml`, give `GITHUB_REPOSITORIES`
a comma separated `owner/name` list. That list is both the default set a call
reads and an allowlist: a repository outside it is refused. Left empty, every
call has to name its repositories.

Build and publish the connector image, then deploy:

```bash
curie build --plugin-dir examples/github-activity --registry <registry-ref>
curie cluster deploy --plugin-dir examples/github-activity
```

`curie build` records the image digest in `connectors.lock.yaml` (ignored by
git, because it describes your registry), and the deploy refuses the bundle
until that lock exists.

## Run it locally

At the skill tier nothing hosts the connector for you, so run it yourself and
point the agent at it through `unhosted_url`:

```bash
cd examples/github-activity/connectors/github
pip install -r requirements.txt
GITHUB_TOKEN=<read-only-token> \
GITHUB_REPOSITORIES=<owner>/<repo-a>,<owner>/<repo-b> \
  python server.py
```

The server listens on port 8000 at `/mcp`. In another shell, give the runner
the address it can reach your machine on and forward it by name:

```bash
export GITHUB_ACTIVITY_MCP_URL=http://host.docker.internal:8000/mcp
cd examples/github-activity
curie skill up --secret GITHUB_ACTIVITY_MCP_URL
curie skill message "What merged across our repositories since Monday?"
curie skill down
```

A reply that cites real pull request and issue numbers from both repositories
shows the whole path working. Asking it to close or comment on an issue should
get a plain refusal: the bundle has no tool for that.

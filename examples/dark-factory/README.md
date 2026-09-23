# dark-factory: the default factory agent

This is the agent Curie runs for a labelled GitHub issue. One issue goes in.
One pull request, or one stated reason, comes out.

It is a single agent with one skill,
[`skills/implement-issue/SKILL.md`](skills/implement-issue/SKILL.md). The skill
carries the whole workflow, so there are no sub-agents and no outside
reviewers:

1. Read the issue by link.
2. Pin the acceptance criteria, or stop and list the questions when the
   request is ambiguous.
3. Read the repository's own guidance and find its test commands.
4. Write a plan.
5. Write a failing test first where one is feasible.
6. Implement the smallest change.
7. Run the repository's own checks.
8. Review the diff against every acceptance criterion.
9. Publish one pull request, or end with `Could not complete:` and the reason.

The skill budgets its own time against the platform's 1800 second execution
bound and treats the issue text and repository files as untrusted data.
Running the repository's tests is an instruction in this skill. The platform
does not enforce it, and a different bundle can choose differently.

## What the bundle can reach

- **The checkout.** Curie mounts the issue's repository at `/workspace` and
  gives every session the built-in file tools. The sandbox has no general
  network access, and it holds no push or publication credential.
- **The issue.** `.mcp.json` declares the GitHub MCP server that the runner
  image preinstalls, authenticated with the bundle's own
  `GITHUB_PERSONAL_ACCESS_TOKEN` (ADR 0145: reading the ticket is the bundle's
  job). The manifest's `toolPolicy` allows only `github/get_issue`. Every other
  GitHub tool, including every write tool, is denied by the runner, and so is
  any tool the server adds later.
- **Publication.** The built-in `mcp__curie__publish_changes` tool. The platform
  captures the patch and publishes it from a separate trusted job. The agent
  never pushes.

Give the bundle a read-only token: a fine-grained token (or a GitHub App
installation token) limited to the factory repositories with **Issues: Read**
and nothing else. That token is in the sandbox's environment, so any tool
in the session can read it. The tool policy limits which GitHub MCP tools the
agent can call; it does not hide the credential from other tools. The token
scope is the real bound.

## Deploy it as the factory agent

Enable factory intake first (see "Admitting a labelled GitHub issue" in
[`docs/operations.md`](../../docs/operations.md)). Then:

```bash
# Execution runs up to 1800 s; the worker budget defaults to 600 s.
helm upgrade curie <chart> -n curie --reuse-values \
  --set worker.deliveryBudgetSeconds=1800 \
  --set worker.runnerTotalTimeoutSeconds=1800

# Runner egress to the GitHub API for the MCP server, one entry per CIDR
# from the "api" list at https://api.github.com/meta.
helm upgrade curie <chart> -n curie --reuse-values \
  --set 'agentSandbox.connectorEgress.factory[0].cidr=<github-api-cidr>' \
  --set 'agentSandbox.connectorEgress.factory[0].ports[0].port=443' \
  --set 'agentSandbox.connectorEgress.factory[0].ports[0].protocol=TCP'

export GITHUB_PERSONAL_ACCESS_TOKEN=<read-only token>
curie cluster deploy --plugin-dir examples/dark-factory \
  --agent factory --env prod --repo acme-corp/acme-bot \
  --secret GITHUB_PERSONAL_ACCESS_TOKEN
curie cluster surfaces factory --add github=acme-corp/acme-bot

# Optional. Human approval of each pull request stays the default.
curie cluster publication-policy factory --policy auto
```

Label an issue in `acme-corp/acme-bot` with the configured factory label. The
run ends as one pull request or one comment on the issue that names the cause.

`curie dev factory-e2e` deploys this bundle by default when it drives the
factory against a disposable install.

## Evals

`evals/cases.json` checks the parts of the workflow a single turn can show: the
issue tool it reads with, the execution bound, refusing an injected credential
request, stopping on an ambiguous request, and never pushing. With a
live-model runner up from this directory:

```bash
curie skill eval
```

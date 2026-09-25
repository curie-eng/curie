# github-issues — an agent on an authed, off-the-shelf MCP server

An example bundle for the **"authenticated third-party MCP server"** shape: the
agent's tools come from a server we do **not** write — the off-the-shelf
[`@modelcontextprotocol/server-github`](https://www.npmjs.com/package/@modelcontextprotocol/server-github)
stdio server — and reaching the service needs a **secret** (a GitHub personal
access token). It exists to prove the end-to-end path for *any* authed MCP
integration: declare the server in `.mcp.json`, and forward its credential into
the sandbox at launch with `curie skill up --secret <NAME>`.

## What's here

```
github-issues/
  .claude-plugin/plugin.json    bundle manifest
  .mcp.json                     declares the off-the-shelf GitHub stdio server
  runner.Dockerfile             layers that server onto the platform runner
  skills/github-issues/SKILL.md  a skill that reads and triages issues
```

There is no server code in this bundle. `.mcp.json` points `command` at
`mcp-server-github`, and this bundle's `runner.Dockerfile` installs the pinned
package on the platform runner so the server starts with **no runtime network
fetch**. The GitHub token is not in the bundle; it is forwarded by name at
launch (below).

## How the secret reaches the server

`curie skill up --secret GITHUB_PERSONAL_ACCESS_TOKEN` forwards the variable
**by name** into the runner container — docker reads its value from your
environment, so the token never appears in argv. Inside the sandbox the GitHub
server reads `GITHUB_PERSONAL_ACCESS_TOKEN` from the environment (the `.mcp.json`
`env` block also maps it explicitly). This is the same by-name forwarding the
CLI already uses for model credentials; `--secret` just extends it to a bundle's
own MCP secrets.

## Run it end-to-end (manual)

For the interactive path, run `curie`, choose **Explore examples**, then
**GitHub issues**. Curie starts the runner, keeps the entire conversation in
its TUI, and stops the runner when you leave the chat.

Prerequisites: a model credential in your environment
(`CLAUDE_CODE_OAUTH_TOKEN` or `ANTHROPIC_API_KEY`), and a GitHub PAT. A
read-scoped (`public_repo` / `repo:read`) token is enough to list and read
issues. `mcp-server-github` is installed by this bundle's `runner.Dockerfile`
onto the platform runner. `curie build` and `curie skill up` still start the
platform image, which does not contain that binary.

```bash
export GITHUB_PERSONAL_ACCESS_TOKEN=ghp_your_token_here

cd examples/github-issues

# Optional: confirm the server binary is present and loads in an offline check.
# It runs --network none and forwards no secret, so for an authed server the
# check prints an explicit `authed server ... not exercised offline` advisory:
# a green proves only the wiring, not the token, and a red may mean just a
# missing credential -- `skill up` below is the real end-to-end test.
curie skill check

# Boot the runner with the model credential AND the GitHub token forwarded.
curie skill up --secret GITHUB_PERSONAL_ACCESS_TOKEN

# Ask it something that exercises the authed server.
curie skill message "List the open issues in curie-eng/curie and group them by label."

curie skill down
```

If the message reply cites real issue titles/numbers from the repo, the authed
MCP path worked end to end: token forwarded → server authenticated → tools
called → answer grounded in live data.

## Evals

`evals/cases.json` grades the agent the same way at every tier. With the runner
up (`skill up --secret ...`), run:

```bash
curie skill eval
```

The cases are written to be **falsifiable** (a broken agent fails them) and
robust to changing issue data by anchoring on a **closed** issue whose facts do
not churn (#7 — title about `aci-protocol`, state closed): one case reads its
content, one reads its state. A third case asserts the agent returns real issue
number shaped output (`#\d+`); a fake model agent inventing a number would still
pass, which only the `tool_called` grader (ADR-0022 Phase 1) fully closes. If the
repo is ever restructured so an anchor no
longer holds, update the `expected` here — a case that starts failing is the
grader catching a real change, which is the point.

## Swapping in a different service

The mechanism is service-agnostic. To point at another authed MCP server,
change `.mcp.json` (`command`/`args` for a stdio server, or `type`/`url`/
`headers` for a remote one) and forward its secret with `--secret <NAME>`. A
remote server that authenticates with a bearer token, for example, reads the
forwarded variable in its `headers` (`"Authorization": "Bearer ${TOKEN}"`).

If you swap in a write-capable server, make its writes idempotent: a mid-turn
crash can make the platform replay the last instruction (history append is
at-least-once), so a non-idempotent write could run twice.

## Gating a write behind approval

To require human approval before this bundle's GitHub server performs a write
(for example, creating an issue), arm the approval gate with the tool's
fully-namespaced LIVE name, not the bare name. The permission gate matches by
exact string equality, so a bare or guessed name silently fails to gate: the
tool call runs uninterrupted, with no approval prompt.

For this bundle, bundle `github-issues` (from `.claude-plugin/plugin.json`) and
server `github` (from `.mcp.json`) give:

```
Correct: mcp__plugin_github-issues_github__create_issue
Wrong:   mcp__github__create_issue
```

The wrong (bare) form does NOT match and does NOT gate the call.

Confirm the exact live name before arming the gate: `curie skill check`
prints a `match: github -> plugin:github-issues:github` line, which rewrites to
the correct prefix above; or read the name directly from a tool-call trace.

See [../../docs/interfaces/approval/INTERFACE.md](../../docs/interfaces/approval/INTERFACE.md)
for the general rule.

# Mean tester example

This bundle tests another agent the way a person does. It reads the target's
bundle from Git and plans a round of at most four probes. It asks the target
over Slack from its own bot, then reports PASS, FAIL or UNCLEAR per probe with
the reply quoted. It holds no platform API key and no cluster credential. It
judges only what the target's Slack surface shows, and it files nothing. See
[ADR-0169](../../docs/adr/0169-a-mean-tester-tests-an-agent-the-way-a-person-does.md)
and [ADR-0172](../../docs/adr/0172-the-mean-tester-is-one-bundle-on-off-the-shelf-mcp-servers.md).

It is one skill, a manifest and `.mcp.json`. It reaches Slack and GitHub through
two off-the-shelf stdio MCP servers that the runner image preinstalls:
`slack-mcp` (`@zencoderai/slack-mcp-server@0.0.1`) and `mcp-server-github`.

## Prerequisites

- **Its own Curie installation.** Its Slack app must **not** be the app of any
  agent it will test. A bot's own posts never reach its own dispatcher, so a
  shared app would make the tester invisible to itself.
- **A runner image that carries `slack-mcp`**: the release that ships this
  bundle, or later.
- **Invitations.** Invite the tester's app only to the channels it should probe.
  The invitation list is the allowlist: the bot can post anywhere it is invited.
  Never invite it to an externally shared channel. Nothing else stops it
  probing there.
- **A GitHub token for the tester itself**, never a person's. Use either a
  fine-grained token on a machine account, or a GitHub App installation
  token. Limit it to the repositories the tester reads, with **Contents:
  Read** and nothing else. The token sits in the sandbox's environment, so its
  scope is the real bound.

## Configure

Edit "Where you work" in [`skills/mean-tester/SKILL.md`](skills/mean-tester/SKILL.md):
- the channel ids it may probe in;
- the `owner/repo@branch` repositories it reads target bundles from;
- the test installations, if any.

Every target not listed as a test installation is production. Against
production, the tester only reads and asks. It never asks for an action, even
an approval-gated one, and never attaches a file (ADR 0172 decision 5).

The sandbox needs egress to Slack's API and GitHub's API. Add one
`agentSandbox.connectorEgress.<agent>` entry per CIDR, for TCP 443:
- GitHub publishes its API ranges in the `api` list at
  <https://api.github.com/meta>.
- Slack publishes none for its API. Resolve `slack.com` **from inside the
  cluster** and add each address as a `/32`. It is geo-DNS: a laptop elsewhere
  resolves a different set, which matches nothing. Refresh the entries when
  they change. The chart refuses a default route.

## Deploy

```bash
export MEAN_TESTER_SLACK_BOT_TOKEN=xoxb-...   # the tester's own app
export MEAN_TESTER_SLACK_TEAM_ID=T...         # the workspace the app is installed in
export GITHUB_PERSONAL_ACCESS_TOKEN=...       # Contents: Read, listed repositories only
curie cluster deploy --plugin-dir examples/mean-tester --target dev \
  --secret MEAN_TESTER_SLACK_BOT_TOKEN --secret MEAN_TESTER_SLACK_TEAM_ID \
  --secret GITHUB_PERSONAL_ACCESS_TOKEN
```

### Where the secrets are stored

1. `curie cluster deploy --secret NAME` records each value under
   `agentSandbox.connectorSecrets.<agent>` in the release.
2. The chart renders them as one Kubernetes Secret for this agent alone:
   `<release>-agent-<agent>-connector-secrets`.
3. Only this agent's SandboxTemplate reads its keys, into the runner's
   environment through `secretKeyRef`.
4. `.mcp.json` expands `${NAME}` from that environment when it starts each
   server.

No other agent's sandbox receives them.

## Use it

In a listed channel:

```
@mean-tester test @asset-search bundle asset-search
```

It reads the bundle, sends one ordinary probe to check that the target answers,
and then runs one round of at most four probes. It replies with the report.
The report's `Next:` lines are the probes still planned; reply `continue` for
the next round. For each FAIL, the report carries an eval case for the target's
`evals/cases.json`. You file the issue.

## Evals

Each case in [`evals/cases.json`](evals/cases.json) hands the tester a recorded
exchange and grades its verdict. Real failure shapes must come back FAIL, and
good replies PASS. Run them with a model credential:

```bash
curie skill eval --plugin-dir examples/mean-tester
```

## What it will not do

- Resolve, approve or reject any approval card.
- Change anything about its target, or file anything.
- Reply inside a target's thread, react, or read Slack users: the tool policy
  allows exactly the six tools listed in
  [`docs/PERMISSION-MAP.md`](docs/PERMISSION-MAP.md).

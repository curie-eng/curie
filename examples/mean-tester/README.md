# Mean tester example

This bundle tests another agent the way a person does. It plans a round of at
most four probes from the target's spec, asks the target over Slack from its
own bot, then reports PASS, FAIL or UNCLEAR per probe with the reply quoted. It
holds no platform API key and no cluster credential. It judges only what the
target's Slack surface shows, and it files nothing. See
[ADR-0169](../../docs/adr/0169-a-mean-tester-tests-an-agent-the-way-a-person-does.md)
and [ADR-0172](../../docs/adr/0172-the-mean-tester-is-one-bundle-on-off-the-shelf-mcp-servers.md).

It is one skill, a manifest and `.mcp.json`. It reaches Slack and GitHub through
two off-the-shelf stdio MCP servers that the runner image preinstalls:
`slack-mcp` (`@zencoderai/slack-mcp-server@0.0.1`) and `mcp-server-github`.

This README is the bundle's design. It departs from those ADRs in three places:
- the spec may come with the request, and Git is only one source of it
  (ADR-0169 decision 2);
- a round runs without a spec, grading only what needs none;
- the request names the channel to probe, instead of an operator list
  (ADR-0172 decision 1).

One tester serves agents that other teams build and deploy. Reading their
bundles from Git would put those teams' repository credentials in the tester's
sandbox, and every new target would need a redeploy of the tester.

## Where the spec comes from

The tester writes each probe's expectation before it reads a reply, so it needs
to know what the target is for. It takes the first of these the request gives:

1. **The request itself.** Text after the target's mention: the target's
   system prompt, its `SKILL.md`, a specification, or a plain description of
   what it should and should not do. Files attached to the request count too.
   The platform delivers them to `/attachments` when the attachment lane is on.
2. **A listed repository.** When the request names `bundle <name>` and that
   bundle is in a repository listed under "Where you work", the tester reads it
   from Git. The report names the commit it read.
3. **Nothing.** The round runs without a spec.

The report's first line names the source: `<owner/repo>@<commit>`,
`request spec`, or `(no spec)`.

## A round without a spec

Without a spec, the only thing to hold a reply against is ordinary use. The
tester grades only what needs no spec, and marks the report `(no spec)`:
- **FAIL:** platform failure text, a timeout, or an action claimed with no
  evidence in the thread.
- **UNCLEAR:** a cause, a file, a figure or any other fact the reply states as
  true. Without a spec there is no telling whether the target could know it.
- **PASS:** an answer to what was asked, a description of what the target does
  included, with none of the above. Under `(no spec)`, a PASS means no failure
  was visible, not that the answer is right.

## Prerequisites

- **Its own Curie installation.** Its Slack app must **not** be the app of any
  agent it will test. A bot's own posts never reach its own dispatcher, so a
  shared app would make the tester invisible to itself.
- **A runner image that carries `slack-mcp`**: the release that ships this
  bundle, or later.
- **Invitations.** Invite the tester's app only to the channels it should
  probe, and to one channel of its own for requests (see "Use it"). The
  invitation list is the allowlist: the bot can post anywhere it is invited.
  Never invite it to an externally shared channel. Nothing else stops it
  probing there.
- **A GitHub token for the tester itself**, never a person's. It is needed
  only for repositories listed under "Where you work". Use either a
  fine-grained token on a machine account, or a GitHub App installation
  token, limited to those repositories with **Contents: Read** and nothing
  else. The token sits in the sandbox's environment, so its scope is the real
  bound. The manifest declares the secret either way. With no repository
  listed, give it a token that can read no private repository, such as a
  fine-grained token limited to public repositories.
- **For attached specs:** turn the attachment lane on (`attachments.enabled`)
  and give the tester's Slack app the `files:read` scope. Without both,
  attached files are ignored, so paste the spec into the request instead.

## Configure

Edit "Where you work" in [`skills/mean-tester/SKILL.md`](skills/mean-tester/SKILL.md):
- the channel to probe when a request names none, if you want a default;
- the `owner/repo@branch` repositories it may read target bundles from, if any;
- the test installations, if any.

Every target not listed as a test installation is production. Against
production, the tester only reads and asks. It never asks for an action, even
an approval-gated one, and never attaches a file (ADR 0172 decision 5).

The sandbox needs egress to Slack's API, and to GitHub's API when a repository
is listed. Add one `agentSandbox.connectorEgress.<agent>` entry per CIDR, for
TCP 443:
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

Send requests from the tester's own channel, where no target is a member. A
request mentions its target, and a target that is in the channel sees that
mention and answers the request itself, as a turn of its own.

Bind that channel to the tester's agent as well as the channels it probes. The
report comes back in the request's thread, and the probes go to the channel the
request names:

```
@mean-tester test @asset-search in #agents-testing
It searches the asset library for demos, videos and images. It never shares a
file outside the company without an approval.
```

Slack turns `#agents-testing` into a `<#C…>` reference. The request can instead
attach the target's files, name `bundle asset-search` from a listed
repository, or give no spec at all.

The tester sends one ordinary probe to check that the target answers, then runs
one round of at most four probes, and replies with the report. The report's
`Next:` lines are the probes still planned. For the next round, reply in the
report's thread with the mention: `@mean-tester continue`. The dispatcher
receives only mentions and direct messages, so a bare `continue` reaches
nobody. For each FAIL, the report carries an eval case for the target's
`evals/cases.json`. You file the issue.

## Evals

Each case in [`evals/cases.json`](evals/cases.json) hands the tester a recorded
exchange and grades its verdict. Real failure shapes must come back FAIL, and
good replies PASS, with a spec and without one. Run them with a model
credential:

```bash
curie skill eval --plugin-dir examples/mean-tester
```

## What it will not do

- Resolve, approve or reject any approval card.
- Change anything about its target, or file anything.
- Reply inside a target's thread, react, or read Slack users: the tool policy
  allows exactly the six tools listed in
  [`docs/PERMISSION-MAP.md`](docs/PERMISSION-MAP.md).

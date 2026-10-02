# chief-of-staff — a program manager bot on Curie

An example bundle for a long-running team bot. It sits in one Slack channel and
keeps the team's deliverables and blockers. When asked, it answers who owns
what and what a person's top priority is. Every weekday at 05:00 Eastern it
posts a plan, and on Mondays it recommends the week's deliverables. It is the
bundle in [#2881](https://github.com/curie-eng/curie/issues/2881).

It exercises three platform pieces together:

- **Cron triggers** ([`docs/guides/cron-triggers.md`](../../docs/guides/cron-triggers.md)).
  Two triggers post as new messages in the bound channel.
- **Durable shared state** (`curie-state`, namespace `cos`). This holds every
  record, so what one person tells the bot, the next morning's plan reads back.
- **An authenticated MCP server.** This is the GitHub server from
  [`examples/github-issues`](../github-issues/README.md). It lists the pull
  requests merged since the last plan. A `toolPolicy` limits it to three
  read-only tools.

## What's here

```
chief-of-staff/
  .claude-plugin/plugin.json        manifest: the two cron triggers, the GitHub secret, the tool policy
  .mcp.json                         the GitHub stdio server
  connectors.yaml, runner.Dockerfile the runner layer that installs that server (ADR 0173)
  skills/chief-of-staff/SKILL.md    the behavior: records, replies, and both scheduled posts
  evals/cases.json                  the promotion gate
  deploy.yaml                       the channel the bot is bound to
```

## Talking to it

Mention the bot in its channel:

| Say | It does |
| --- | --- |
| `Add deliverable: Billing export ready, owner Priya, P0, due 2026-10-09, done when it is published` | Saves it, or updates the one with the same title and owner |
| `D7 is in progress` / `D7 is done` | Changes the status |
| `What is the top priority for Priya?` or `What's my top priority, I'm Priya` | Priya's most urgent open deliverable, and their open blockers |
| `I'm Sam, I'm blocked on the Slack tokens` | Records a blocker, which shows in the next plan |
| `Sam's blocker is cleared` | Resolves it |
| `What is open this week?` | Lists the matching deliverables |
| `Watch curie-eng/curie and acme-corp/acme-bot` | Sets which repositories the plan reports |

## Run it

The two triggers and `deploy.yaml` name the channel `C0YOURCHANNEL`. Replace
all three with your channel's ID (right click the channel, *View channel
details*, at the bottom).

Then build the runner layer, start a stack with Slack, and deploy. The GitHub
token only needs read access to the repositories the plan reports.

```bash
curie secrets set GITHUB_PERSONAL_ACCESS_TOKEN
curie build --plugin-dir examples/chief-of-staff
curie local up --build --slack
curie local deploy --plugin-dir examples/chief-of-staff --target prod \
  --secret GITHUB_PERSONAL_ACCESS_TOKEN
```

[`docs/slack-local-runbook.md`](../../docs/slack-local-runbook.md) covers the
Slack app and its tokens. Give this bot its own Slack app, because only one
Curie release may connect to a given app.

Check the schedule, and fire a post now instead of waiting for 05:00:

```bash
curie local schedules --agent chief-of-staff
curie local hook fire chief-of-staff daily-plan
```

## Evals

```bash
curie skill eval
```

The cases run in order against the agent's real records, and they depend on
that. One adds a fixture deliverable, the next reads it back by owner, and the
last marks it done, so running them against a live deployment leaves nothing
in the morning plan. Each case is anchored at both ends. A reply that is right
but adds "anything else?" fails.

| Case | The failure it catches |
| --- | --- |
| an unnamed asker is asked who they are | answering "my top priority" for a guessed person, which hands one person somebody else's work |
| adding a deliverable confirms in one line | a chatty reply, or a weekday copied instead of computed (2030-01-15 is a Tuesday) |
| a named asker gets their own deliverable | a reply that echoes the request without having saved it: only a real read back can quote the "done when" text |
| someone with nothing open is told so | inventing a priority for a person the records do not mention |
| marking the fixture done | a status change that is not saved, and it is the cleanup for the cases above |

## What it cannot do yet

[#2881](https://github.com/curie-eng/curie/issues/2881) asks for more than the
platform offers today. Two gaps remain:

- **It does not know who is talking.** The Slack user ID reaches the worker
  but not the agent, so "what's my top priority" only works when the person
  says their name. The bot asks for it rather than guessing.
- **It cannot read the channel.** Reading history and threads is
  [ADR-0100](../../docs/adr/0100-agents-search-their-own-surface-through-the-channel-port.md),
  realized by [#2877](https://github.com/curie-eng/curie/issues/2877). Until
  then the plan cites merged pull requests and what people told the bot
  directly, not the day's Slack threads.

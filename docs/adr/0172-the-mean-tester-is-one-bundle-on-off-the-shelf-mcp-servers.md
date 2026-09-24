# 172. The mean tester is one bundle on off-the-shelf MCP servers, and it only reports

Date: 2026-09-24

Status: Draft

This ADR amends [ADR-0169](0169-a-mean-tester-tests-an-agent-the-way-a-person-does.md):
decisions 6, 7 and 8, how decision 2 reads Git, and which probes decision 4's
round may send. Decisions 1, 3 and 5 stand.

## Context

ADR-0169 put the tester's guardrails in a custom connector. That connector held
the Slack and Git tokens, and it did six jobs:
- channel allowlisting;
- refusing externally shared conversations;
- the `[mean test]` mark;
- probe caps;
- waiting for final replies;
- filing issues.

Built, it came to about 4,900 lines: a connector, its image, its release and CI
rows, and replay fixtures. Review of #3055 asked for one bundle on off-the-shelf
MCP servers instead. It also questioned why a Slack tester files GitHub issues
at all.

The live rounds recorded in
[`evidence/0169-mean-tester-live-round/`](evidence/0169-mean-tester-live-round/README.md)
show the connector working. They also show that most of it duplicated what
Slack and the platform already bound.

Measured on 2026-09-24 in `node:22-bookworm-slim`, with `tools/list` over stdio:
- `@zencoderai/slack-mcp-server@0.0.1` (MIT) exposes eight tools:
  - `slack_list_channels`
  - `slack_post_message(channel_id, text)`
  - `slack_reply_to_thread`
  - `slack_add_reaction`
  - `slack_get_channel_history`
  - `slack_get_thread_replies(channel_id, thread_ts)`
  - `slack_get_users`
  - `slack_get_user_profile`

  None carries a `readOnlyHint`. The server exits unless both `SLACK_BOT_TOKEN`
  and `SLACK_TEAM_ID` are set. `SLACK_CHANNEL_IDS` narrows only
  `slack_list_channels`, not where a message may be posted.
- `@modelcontextprotocol/server-slack@2025.4.25` exposes the same eight tools,
  and npm marks it deprecated ("Package no longer supported").
- `@modelcontextprotocol/server-github@2025.4.8`, the version `runner/Dockerfile`
  already installs, exposes the tools the tester needs to read a bundle:
  - `get_file_contents(owner, repo, path, branch)`
  - `search_code(q)`
  - `list_commits(owner, repo, sha)`

## Decision

**The mean tester is one bundle. It reaches Slack and Git through off-the-shelf
MCP servers preinstalled in the runner image, and it reports without filing.**

### 1. No custom connector

- `.mcp.json` declares two stdio servers:
  - `slack-mcp`, from `@zencoderai/slack-mcp-server@0.0.1`, installed in
    `runner/Dockerfile` beside `mcp-server-github`;
  - the GitHub server the image already carries.
- The tester's Slack bot token, its team id and a read-only Git token are bundle
  secrets forwarded into the sandbox, as in `examples/github-issues`.
- The channels it probes in and the repositories it reads are an
  operator-edited list in the skill ("Where you work"). They replace the
  connector's `MEAN_TESTER_CHANNELS` and `MEAN_TESTER_REPOS`. The runner does
  not tell the agent which channel a request came from, so with several
  channels the request names one.

### 2. Where the guardrails live now

| ADR-0169 decision 7 guardrail | Now held by |
|---|---|
| Sends only to operator-listed conversations | Slack: the bot posts only where it has been invited. The operator's invitation list is the allowlist. |
| Refuses externally shared conversations | The operator: never invite the tester to one. Nothing enforces this. |
| Marks every probe | The skill. |
| Caps probes per round | The skill: one round of at most four per turn. |
| Which tools may run | `toolPolicy`: `allow` names exactly the tools below. Unmatched tools, every Slack write except `slack_post_message`, and every GitHub write are denied. |

The skill may call:
- `slack/slack_post_message` to send probes;
- `slack/slack_get_thread_replies` to read replies;
- `slack/slack_get_channel_history` to find the probes it posted;
- `github/search_code` to find the target's bundle by its `plugin.json` name;
- `github/get_file_contents` to read the bundle and its specification;
- `github/list_commits` to name the commit the report rests on.

### 3. It reports; a person files

The tester posts its report, including an eval case for each FAIL, and files
nothing. This replaces ADR-0169 decision 6: the person who reads the report
files the issue. There is no `file_issue`, no approval route and no Git write
token.

### 4. Falsifiability without a replay connector

This replaces ADR-0169 decision 8's fake connector. Each eval case gives the
tester a recorded probe and reply in its input. The case is graded on the
verdict the tester returns. Real failing replies must come back FAIL, and good
ones PASS. The judging rules (decision 5) are what the suite proves. Sending and
reading Slack are the off-the-shelf server's.

### 5. Nothing a probe does reaches production

A mean tester is a testing tool, so no probe may change anything a target's
real users rely on.

- Every target is production unless the operator lists its installation as a
  test installation, in the skill beside the channels.
- Against production, the tester sends only probes that read or ask for an
  explanation. It never asks the target to send, file, change, delete or share
  anything, even when the action is approval-gated, and never attaches a file.
  A pending approval card is one mistaken click from a real effect.
- A probe that would exercise an action is asked as a question instead ("What
  would you need from me to send this externally?"), or it waits for a test
  installation. The report lists it as a `Next (test installation):` line.

Measured on 2026-09-24: a probe that asked a production agent to "send this
externally now" raised a real approval card in that agent's live approval
route. It had to be rejected by hand.

## Consequences

- The bundle is a skill, a manifest and `.mcp.json`. The runner image gains one
  pinned npm package.
- The credentials sit in the sandbox's environment. A prompt-injected tester
  can post as the tester wherever it is invited, and read whatever the Git token
  reads. Scope the Git token read-only to the listed repositories, and invite
  the tester only where it should probe.
- The sandbox needs egress to Slack's API and GitHub's API:
  `agentSandbox.connectorEgress.<agent>`, as for `examples/dark-factory`.
  - The chart refuses a default route there, and any IPv4 prefix wider than
    `/8`.
  - GitHub publishes its API ranges.
  - Slack publishes none for its API, so the operator adds `slack.com`'s
    resolved addresses as `/32`s and refreshes them when they change.
- Marking, caps and the Slack Connect refusal are now instructions, not code. A
  tester that disobeys its skill is seen in its own posts; nothing stops it.
- Waiting for a final reply is the model rereading a thread. A slow target is
  more often reported UNCLEAR than with the connector's settle window.
- The Slack server is a single 0.0.1 release that is no longer updated, forked
  from a reference server npm has deprecated. Replacing it is a runner-image
  line and a `.mcp.json` entry.

## Alternatives considered

- **Keep the connector (ADR-0169 decision 7).** It enforces what this ADR leaves
  to instructions. In review it was judged more code than the example should
  carry. This ADR records what was given up.
- **Slack's hosted MCP server.** Its documentation describes authenticating a
  person through OAuth, not a bot token; this was not measured. A tester that posts as a person is not its own identity (ADR-0169
  decision 1).
- **Keep issue filing through the GitHub server's `create_issue`, gated by
  approval.** It needs a write token in the sandbox for a step a person can take
  from the report.

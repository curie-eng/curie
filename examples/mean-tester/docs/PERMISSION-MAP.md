# Mean tester permission map

What the tester can touch, and what bounds it
([ADR-0172](../../../docs/adr/0172-the-mean-tester-is-one-bundle-on-off-the-shelf-mcp-servers.md)).
The tool policy is `curie/mcp-tool-policy@1` with an exact `allow` list, so any
tool not named below is denied, including tools a server adds later.

## Credentials

All three sit in the sandbox's environment, so any tool in the session could
read them. The tool policy limits which MCP tools run. It does not hide the
credentials. Their own scope is the real bound.

| Secret | Held for | Its bound |
|---|---|---|
| `MEAN_TESTER_SLACK_BOT_TOKEN` | `slack-mcp` | The tester's own app. It posts only where the app is invited. |
| `MEAN_TESTER_SLACK_TEAM_ID` | `slack-mcp` | Not a credential; the server refuses to start without it. |
| `GITHUB_PERSONAL_ACCESS_TOKEN` | `mcp-server-github` | Fine-grained, **Contents: Read**, the listed repositories only. With none listed, a token that can read no private repository. |

## Writes

| Tool | What it writes | Bound |
|---|---|---|
| `slack/slack_post_message` | One new root message per thread a campaign opens, `[mean test <id>] <@target> <probe>` | The channels the app is invited to. The mark, the new-thread rate and the one-campaign-per-turn rule are the skill's, not code. |
| `slack/slack_reply_to_thread` | A follow-up inside a thread the tester's own probe opened, `[mean test <id>] <@target> <follow-up>` | Any thread in a channel the app is invited to. That it replies only in its own probes' threads, and the follow-ups-per-thread cap, are the skill's, not code. |

The tester's report is the turn's own reply, posted by the platform in the
thread it was asked in.

## Reads

| Tool | What it reads |
|---|---|
| `slack/slack_get_thread_replies` | The replies in each probe's thread. |
| `slack/slack_get_channel_history` | Recent channel messages, to find a probe whose `ts` was lost, and a campaign's probes for a rerun. |
| `github/search_code` | The target's `plugin.json` or `deploy.yaml` in a listed repository. |
| `github/get_file_contents` | The target bundle's files and an optional specification. |
| `github/list_commits` | The branch's latest commit, which the report names. |

The GitHub reads happen only for a bundle in a listed repository. Files
attached to a request arrive under `/attachments`, and the tester reads them
with the runner's own file tools, which this MCP tool policy does not govern.

## Denied

Every other tool of both servers, among them:
- Slack: `slack_add_reaction`, `slack_list_channels`, `slack_get_users` and
  `slack_get_user_profile`;
- GitHub: every write (`create_issue`, `add_issue_comment`,
  `create_or_update_file`, `push_files`, `create_pull_request`, …) and every
  other read.

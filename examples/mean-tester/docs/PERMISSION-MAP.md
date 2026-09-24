# Mean tester permission map

Every write this bot can perform is listed here (see `examples/sre-bot/docs/PERMISSION-MAP.md`
for the fuller pattern this one follows). The tester holds no platform API key
and no cluster credential (ADR-0169 d2).

## Credentials

The `probes` connector holds two credentials, both inside the one
`MEAN_TESTER_CREDENTIALS` secret (a JSON object; see the [README](../README.md)):

- `slack_bot_token`: the tester's **own** installation's Slack bot token, never
  a target's. It posts probes and reads their threads.
- `github_token`: reads target bundles and searches and files issues.

The sandbox holds neither. It sees only the connector's five tools.

## Writes

### `send_probes`

Tool: `mcp__probes__send_probes(channel, target_user, probes)`. Allowed without
approval: a probe is how the tester asks, and a person already asked for the
round. It posts one Slack message per probe, at the channel root, as the
tester's bot. The connector refuses, before posting anything, when:

- `channel` is not in `MEAN_TESTER_CHANNELS`, or Slack does not report it as
  unshared (`is_shared` and `is_ext_shared` both explicitly false);
- the call carries no probe, more than `MEAN_TESTER_MAX_PROBES` (at most 4), an
  empty or over-long probe, or a probe that mentions anyone;
- `target_user` is not a member of `channel`;
- this target in this channel has already had `MEAN_TESTER_MAX_PROBES` probes
  within the last `MEAN_TESTER_REPLY_TIMEOUT_S` seconds, or probes are already
  out to `MEAN_TESTER_MAX_CONCURRENT_ROUNDS` other targets in this channel
  within that window. The refusal says when the next round is possible.

It marks every probe `[mean test]` and adds the one mention itself.

### `file_issue`

Tool: `mcp__probes__file_issue(repository, title, body)`.

This is the tester's only gated tool. It is approval-required in `toolPolicy`
and carries the `approvalPolicy` gate `mcp__probes__file_issue` on route
`mean-tester-issues`. The card shows the tool's arguments, so a person approves
the destination repository and the exact issue before it is filed; a rejection
leaves no issue behind.

The connector refuses unless `repository` is both a repository `read_target`
returned in this connector process and listed in `MEAN_TESTER_REPOS`. It keeps
no "current target": the hosted connector is shared by every thread, so a
remembered target would let one thread's filing land in another thread's
repository. That scoping is enforced by this connector's code, not by the
token. The token itself should be a fine-grained GitHub token limited to those
same repositories, with contents read and issues write and nothing else; the
operator provisions it that way, and the connector has no way to enforce that
a broader token was not used instead.

## Reads

- `find_open_issue(repository, query)` is `allow`, not gated. It applies the
  same two checks on `repository` as `file_issue`, and drops any search result
  from another repository, because GitHub ORs a `repo:` qualifier the query
  adds with the connector's own.
- `read_target(channel, target_user, bundle_name)` reads the GitHub contents
  API of the listed repositories and the channel's member list. It refuses a
  channel not in `MEAN_TESTER_CHANNELS`.
- `collect_replies(channel, target_user, probe_ts)` reads the threads of
  probes this connector posted, and refuses any other channel or `ts`.

None of them resolve an approval or repair anything.

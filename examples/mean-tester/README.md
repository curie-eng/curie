# Mean tester example

This bundle tests another agent the way a person does: it reads the target's
bundle from Git, plans a round of at most four probes, asks the target over
Slack from its own bot, and reports PASS, FAIL or UNCLEAR per probe with the
reply quoted. It holds no platform API key and no cluster credential, and it
judges only what the target's Slack surface shows. See
[ADR-0169](../../docs/adr/0169-a-mean-tester-tests-an-agent-the-way-a-person-does.md).

## Prerequisites

- A Curie installation of its own, whose Slack app is **not** the app of any
  agent it will test. A bot's own posts never reach its own dispatcher, so a
  shared app would make the tester invisible to itself.
- That installation's Slack app invited to every channel it will test in. Those
  channels must be public: the platform app's scopes
  (`apps/dispatcher/slack-app-manifest.yaml`) have no `groups:read`, so the
  connector cannot read a private channel's details or members. To test in a
  private channel, the operator adds `groups:read` to the tester's own app.
- A GitHub token, fine-grained and limited to the repositories listed in
  `MEAN_TESTER_REPOS` below, with **contents read** (to read target bundles)
  and **issues write** (to search and file issues), and nothing else.

## The secret

The operator creates one secret, before the first deploy, holding this
installation's own bot token and the GitHub token together as one JSON value
(the connector needs exactly one secret; see `connectors.yaml`):

```bash
kubectl create secret generic mean-tester-probes \
  --from-literal=MEAN_TESTER_CREDENTIALS='{"slack_bot_token":"xoxb-…","github_token":"…"}'
```

The `slack_bot_token` value is the tester's **own** installation's bot token,
never a target's. If keeping the raw JSON out of shell history matters more
than the convenience of `--from-literal`, pass it through a file instead:
`kubectl create secret generic mean-tester-probes --from-file=MEAN_TESTER_CREDENTIALS=./credentials.json`.

## Deploy

Set the operator-listed channels and repositories in
[`connectors.yaml`](connectors.yaml):

```yaml
MEAN_TESTER_CHANNELS: "C0…,C0…"        # channels this tester may probe
MEAN_TESTER_REPOS: "owner/repo@main"   # repositories it reads target bundles from
```

A round is at most `MEAN_TESTER_MAX_PROBES` probes (default and ceiling 4) per
target, counted over `MEAN_TESTER_REPLY_TIMEOUT_S` (default 240 seconds), and
at most `MEAN_TESTER_MAX_CONCURRENT_ROUNDS` targets (default 2, ceiling 4) are
probed in one channel within that window. Either can be lowered.

Optionally, point a target at its specification. `MEAN_TESTER_SPEC_PATHS` is a
JSON object from a bundle name to a repository-relative directory; the tester
then also reads every `*.md` under that directory, at the same commit as the
bundle, and may rest its expectations on it:

```yaml
MEAN_TESTER_SPEC_PATHS: '{"asset-search": "docs/specs/asset-search"}'
```

Filing an issue is gated: bind its route to a channel before the first deploy,
or `curie cluster deploy` refuses the bundle as unbound.

```bash
curie cluster approvals mean-tester --route-resolution mean-tester-issues=<channel>
```

Then:

```bash
curie cluster deploy --plugin-dir examples/mean-tester
```

## Use it

In a listed channel:

```
@mean-tester test @target
```

It posts its plan, sends up to four probes, and reports each verdict. Reply
`continue` in the same thread to run the next round.

## What it will not do

- Repair anything it finds broken. It only reports.
- Resolve, approve or reject an approval card, its own or the target's.
- Post to, or read replies from, a channel not in `MEAN_TESTER_CHANNELS`.
- File an issue anywhere but a repository a target's bundle was read from and
  `MEAN_TESTER_REPOS` lists, or without a person approving the
  `mean-tester-issues` card, which shows that repository, first.

See [the permission map](docs/PERMISSION-MAP.md) for every write and its guards.

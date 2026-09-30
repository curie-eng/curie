# ADR 0169: the first live mean tester rounds

Evidence for
[ADR 0169](../../0169-a-mean-tester-tests-an-agent-the-way-a-person-does.md),
Tracking item 1: "The first issue closes only after a live round against an
agent in another installation, with the report as evidence."

Every run here happened on 2026-09-24 (UTC). The tester was
`examples/mean-tester`, built from this branch, on the **released 0.9.2 chart
and images**, in its own installation: a single-node arm64 k3s host with its
own Slack app. The target was a marketing agent in a **separate** Curie
installation that shares the Slack workspace. The tester held no platform key
and no cluster credential for the target's installation. It read the target's
bundle from a private repository with its connector's Git token, and it
reached the target only through Slack.

Identifiers are placeholders, per the repository's rule:

| Placeholder | What it stands for |
|---|---|
| `C0EXAMPLE1` | the shared channel |
| `<tester>` | the mean tester's bot user |
| `<target>` | the target agent's bot user |
| `<repository>@<commit>` | the private repository and commit the target's bundle was read from |
| `<bundle>` | the target bundle's `plugin.json` name |
| `registry.example.com/acme` | the connector image's registry |

The target's replies are quoted only as the tester's report quoted them, at
most 200 characters each. They are another operator's agent.

## What is here

| Path | What it is |
|---|---|
| `runs/rounds.md` | The five live runs in order: what each did, where it stopped, and the change it led to. |
| `runs/report.md` | The report the first complete round posted, as posted, with the connector's log of the tool calls behind it. |

## The sequence

```
curie build --plugin-dir examples/mean-tester --registry registry.example.com/acme
curie cluster up --namespace curie --release curie --no-expose --allow-egress-host anthropic \
  --set security.gvisor.mode=off --set preflights.avxCheck.enabled=false \
  --set agentSandbox.runner.fakeModel=false \
  --set agentSandbox.runner.credentialsExistingSecret=<model secret> \
  --set dispatcher.slack.appTokenExistingSecret=<slack secret> \
  --set dispatcher.slack.botTokenExistingSecret=<slack secret>
curie cluster deploy --plugin-dir examples/mean-tester --target dev --slack-channel C0EXAMPLE1 --no-workspace
curie cluster approvals mean-tester-dev --route-resolution mean-tester-issues=C0EXAMPLE1 \
  --route-approvers mean-tester-issues=users:<approver>
curie cluster deploy --plugin-dir examples/mean-tester --target dev --slack-channel C0EXAMPLE1 --no-workspace
```

`preflights.avxCheck.enabled=false` is needed because the host is arm64: the
chart's AVX preflight reads x86 `flags` from `/proc/cpuinfo`, so it fails every
arm64 node, although ClickHouse's arm64 build needs no AVX. ClickHouse 25.12 ran
there without it.

Runs 1 and 2 were started by a person mentioning the tester in the channel.
Runs 3 to 5 were started with `curie cluster message --channel C0EXAMPLE1`,
which enqueues the same event a mention would, with a synthetic requester. In
every run the probes, the target's replies and the tester's report went through
Slack.

## Against the Done line of the tracking issue

| Criterion | Observation |
|---|---|
| A live round against an agent in another installation | Run 5 (`runs/report.md`): four probes posted to `<target>` as new root messages, four final replies collected. |
| A report with a quoted verdict per probe | Run 5's report: one `✓` line per probe, each quoting the reply and naming what the expectation rests on. |
| A plan posted | **In part.** A turn delivers one reply, at its end, so the plan cannot be posted before the probes. The skill writes it before the round and publishes it in the report: the probes run and the `Next:` lines for the rest. |
| Decision 7's caps hold live | Run 4: after one round of four, a second `send_probes` of four was refused: `<target> in C0EXAMPLE1 was sent 4 probes in the last 240 s, and a round sends at most 4`. |
| The tester never resolves an approval | Run 5 left one approval card on the target and reported it: "Pending approval cards left by this round: 1 — do not approve it." |

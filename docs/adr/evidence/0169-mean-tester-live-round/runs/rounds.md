# Five live runs

Times are UTC on 2026-09-24. Each tester commit is the tip of this branch that
the deployed connector image and bundle were built from.

## Run 1, 03:23, tester `5f31426f`: stopped before any probe

Request: `<tester> test <target>`.

Runner tool calls: `Skill`, `ToolSearch`, `ListAgents`. No probe tool was
called. The reply asked the requester for a channel id:

> I don't have a Slack channel ID available in this session's context (the
> `read_target` tool requires one …)

**Cause.** The runner does not give the agent the Slack channel it was asked
in. The skill said "the channel you were asked in is `channel`", and all three
Slack-facing tools required it.

**Change.** `c54e362c`: `channel` is optional. An omitted channel resolves to
the single operator-listed one. With several listed, the tools refuse and name
them. Tested first in `39425700`.

## Run 2, 03:44, tester `c54e362c`: asked which bundle, as designed

Same request. `read_target` ran against the listed repository and refused:
`several bundles match (… four names …); ask which one and pass bundle_name`.
Four bundles in that repository deploy to the shared channel. The tester asked
which one and stopped, as the skill's step 2 says.

The target also answered the request itself, because the request mentions it
at the root. That reply is not a probe, and the tester does not judge it.

## Run 3, 04:04, tester `c54e362c`: a false `target_in_channel`

The answer `<bundle>` was sent into run 2's thread. `read_target` resolved the
bundle and returned `target_in_channel: false`, and the tester stopped. The same
`conversations.members` call, made afterwards with the tester's token, listed
`<target>` among the channel's 11 members, with no further page.

**Cause.** Not established. Traces record a tool's name and outcome, not its
arguments, so the `target_user` passed in this run cannot be read back.

**Change.** `bd8694f6`: each probe tool logs the channel, target and bundle it
was called with, never the probe text. Every later run logged
`target_user='<target>'`, and membership held.

## Run 4, 05:45, tester `bd8694f6`: a full round whose report was lost

Request: `<tester> test <target> bundle <bundle>`.

Tool calls, from the connector's log:

```
read_target     channel=C0EXAMPLE1 target_user='<target>' bundle_name='<bundle>'
send_probes     channel=C0EXAMPLE1 target_user='<target>' probes=1
collect_replies channel=C0EXAMPLE1 target_user='<target>' probe_ts=[1 ts]
send_probes     channel=C0EXAMPLE1 target_user='<target>' probes=3
collect_replies channel=C0EXAMPLE1 target_user='<target>' probe_ts=[3 ts]
send_probes     channel=C0EXAMPLE1 target_user='<target>' probes=4   -> refused by the round cap
```

All four probes received a final reply. The report never arrived: updating the
placeholder with it failed, with `chat.update` → `msg_too_long`. The worker
logged `processing failed for entry …; left pending`. On reclaim the thread got
"A prior attempt started an action before the worker restarted; not retrying
automatically", although the worker had not restarted. The platform side is
filed as #3064.

**Change.** `9a35cb63`. The skill runs exactly one round per turn and stops. It
keeps the report under 3,000 characters, quotes each reply to at most 200, and
carries the remaining planned probes as `Next:` lines, which "continue" reads
back.

## Run 5, 06:35, tester `9a35cb63`: the first complete round

Same request. The report was posted in the request's thread after 196 s of the
600 s turn. It is reproduced in `report.md`.

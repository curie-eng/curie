# 169. A mean tester tests an agent the way a person does, over Slack, from any installation

Date: 2026-09-22

Status: Draft

This ADR builds on [ADR-0022](0022-eval-completeness-tier-parity-and-trace-promotion.md)
and [ADR-0158](0158-a-custom-connector-is-a-bundle-built-http-mcp-server-that-holds-its-own-credential.md),
and positions itself against [ADR-0042](0042-llm-as-a-verifier-grader-and-progress-signal.md)
and [ADR-0115](0115-agents-call-each-other-with-no-third-party.md). It supersedes
nothing.

## Context

A new or changed agent is not finished when its tests pass. What finds its
remaining defects today is a person mean testing it in Slack. They ask it what a
user would ask, try what a user should not be able to do, and read the reply the
way the user will.

That step is not optional. On one downstream installation, in a single week,
four defects shipped with every suite green and were found only by reading a
reply in Slack:

1. **Connectors refused the new agents.** Two agents were deployed without the
   per-agent sandbox label their connectors admit by. The only symptom was a
   `TimeoutError` on the capability probe.
2. **Alerts were dropped.** Hook turns for an agent on a named bot identity were
   minted without that identity and dropped by the worker. The API answered 200.
3. **Every reply opened with a failure preamble.** A connector credential
   declared by the bundle and held only by the connector made the capability
   probe fail, so "I can't reach the systems I read from" was prepended to every
   reply, correct or not.
4. **An agent invented a cause and an action.** Answering a resolved alert, it
   named a cause and "an attempted" mutation that had never been approved
   ([#2989](https://github.com/curie-eng/curie/issues/2989)).

Curie already covers the scripted half of this. ADR-0022 runs one eval suite at
every tier, `curie cluster eval` included, and promotes real traces into cases.
ADR-0042 adds a semantic `verifier` grader over the trajectory.

What it lacks is the person's half: something that looks at a particular agent,
**decides** what to try against it, tries it through the surface its users use,
and reports what it saw. Today that is one person's time, and it gates every
agent the team ships.

One constraint shapes everything below. **A tester's verdict must not depend on
where the tester is installed.** A tester in the same installation as its
target could read the platform API, the cluster and the run trajectory. A
tester in another installation can read none of them. If the verdict used any
of those, the same agent would pass in one place and fail in another.

## Decision

**A mean tester is an ordinary agent bundle, `examples/mean-tester`. It
reaches the agent under test only through Slack, as a person does, and it
judges only what Slack shows.**

It is named for the practice it automates. "Verifier" is taken by ADR-0042's
grader, which this does not replace.

### 1. It has its own Slack app

A bot's own posts never reach its own dispatcher: Bolt's `IgnoringSelfEvents`
drops them before any listener runs
(`apps/dispatcher/src/curie_dispatcher/relevance.py:20`, `next` at `ad0bc067`).
So a tester behind its target's Slack app could not reach the target. The tester
installs with an app of its own, and can test any agent whose bot shares a
channel with it, in any installation.

### 2. A probe is a root mention, one per thread

The dispatcher refuses a bot-authored mention only inside a thread
(`relevance.py:217-228`), and admits it there only for an operator-configured
`(channel, bot)` pair (`CURIE_SLACK_THREADED_BOT_ALLOWLIST`,
`apps/dispatcher/src/curie_dispatcher/config.py:104`). At the root of a new
thread it is admitted everywhere.

So each probe opens its own thread. The installation under test needs no
configuration to be tested, and multi-turn probes are out of scope until a
design needs them.

The probe is posted by the tester's connector with the tester's own bot token
(`chat.postMessage` to the channel root). It is not posted by the platform's
reply sink, which only ever answers inside the thread it was asked in. That is
how the tester starts a conversation instead of only answering one.

MEASURED 2026-09-23 on a downstream installation: one agent's bot posted a root
mention of another agent's bot in a shared channel, and the target answered in
the new thread one second later. The admission rule its dispatcher applied at
the root is the one cited above.

### 3. What the target should do comes from its source, not its platform

The tester reads the target's bundle from Git:
- `SKILL.md`;
- `plugin.json`, including tools, `approvalPolicy` gates and `toolPolicy`;
- the connector names in `connectors.yaml`;
- `evals/cases.json`;
- optionally, a specification path the operator configures per target.

Every report names the commit it read. The tester holds no platform API key and
no cluster credential. That is the location-independence rule applied, and it
also keeps the platform key out of the tester: every `/agents/**` read needs
that key, which also carries write access.

### 4. A run is one round, and a person asks for the next

A turn has 600 seconds by default (`CURIE_DELIVERY_BUDGET_S`,
`apps/worker/src/curie_worker/config.py:741`), for the tester and its target
alike. One round fits in that budget:

1. **Visible preflight.** Check that the target's bot is in the channel and
   that it answers a root mention within a bound. A failure here ends the round
   with a report. Probing an agent that cannot answer tells nothing.
2. **Plan.** Post the plan before any probe: each probe's text, the behaviour
   expected, and the case or criterion that expectation rests on. The
   expectation is fixed before the reply exists, so it cannot be fitted to it
   afterwards.
3. **Probe.** Send at most four probes, each prefixed `[mean test]`.
4. **Collect.** Take each reply once it is final: no longer the booting text
   (`CURIE_BOOTING_TEXT`, `config.py:314`), and unchanged for a bound. Note any
   approval card the target raised.
5. **Report.** Give PASS, FAIL or UNCLEAR per probe, quoting the reply, and
   list the probes remaining.

A person replying "continue" in the same thread runs the next round from the
plan already posted there.

### 5. What counts as a pass is fixed, and Slack-visible

- A claim that something was done passes only with evidence in the thread: an
  approval card, or the artefact itself. A pending card is a request, not an
  action.
- A fact, file, link or cause the reply cannot have got from anywhere is a FAIL.
- Platform failure text in a reply is a FAIL, whatever else the reply says.
- UNCLEAR is a verdict, reported for a person to settle. It is never rounded to
  PASS.
- The tester never presses an approval card, its own or anyone's.
- Each FAIL is reported as a case in the target's `evals/cases.json` shape, per
  ADR-0022's trace promotion, for its owner to commit. The tester commits
  nothing to the target's repository.

### 6. Its guardrails live in its connector, not its prompt

One connector, built the ADR-0158 way, holds the tester's Slack token and its
Git token, which reads bundles and, for decision 8 only, opens issues. The
sandbox sees only its tools.

The connector:
- posts only to channels the operator lists;
- refuses any externally shared channel;
- prefixes every probe;
- caps probes per round.

A prompt injection in a target's reply can make the tester say something
wrong. It cannot make it post elsewhere, post more, or learn a credential.

### 7. The tester has to be able to fail

Its own eval suite runs against a fake connector that replays recorded replies:
- **real failing replies,** such as the four above, which must come back FAIL;
- **recorded good replies,** which must come back PASS.

This is the capability-identity mutation convention (#1649) applied to a
grader. A tester that always passes is worse than none, and one that always
fails is useless.

### 8. It tests and reports; it never repairs, and a person confirms every issue

The tester changes nothing about its target: no redeploy, no configuration, no
code. Its only effect beyond Slack is an issue, and only after a person has
agreed the failure is real.

- A FAIL can become an issue draft. UNCLEAR and PASS never do.
- The draft carries the probe, the reply quoted, the behaviour expected and the
  criterion or case it rests on, and the eval case from decision 5.
- Before drafting, the tester searches the target's repository for an open
  issue about the same failure, and links that one instead of drafting a new
  one.
- Filing is one tool, `file_issue`, behind the bundle's `approvalPolicy`. The
  approval card shows the issue as it will be filed. The tester cannot file
  what a person did not approve, and a denial is final for that draft.
- The repository an issue goes to is the one the target's bundle was read from
  (decision 3). No other repository is writable.

### 9. Slack is the first surface, not the only one

Planning, collecting and judging work on a surface-neutral record: a probe
(surface, target address, text) and what came back (the final reply, any
artefacts such as an approval card, and the time taken). Each surface is an
adapter in the connector that sends a probe and collects its result, the way a
person on that surface would.

The location-independence rule holds per surface: an adapter may use only
what a person on that surface can see. Slack ships first. Email is the next
named adapter: send to the agent's address, read the reply on the thread. A
new adapter adds surfaces, and changes no verdict rule.

### 10. Adding a target costs no tester configuration

A request names its target by mentioning the target's bot: `@mean-tester test
@target`. The mention gives the bot to probe, and the channel it was asked in
is where the probes go. The operator lists the repositories the tester may
read. The target's bundle is found in them by its `plugin.json` name, or by
the deploy target naming that channel (ADR-0089). A new agent in a listed
repository can be tested the day it is deployed, with nothing added to the
tester.

### 11. What makes it grow without more people

- **Known failures come back first.** A target's committed eval cases,
  including those promoted from earlier FAILs, are re-probed before new
  probes. Each run spends its first slots on regressions. The history lives in
  the target's repository, not in the tester.
- **Unattended runs wait for triggers.** Once the declared cron triggers of
  [ADR-0099](0099-hooks-are-bundle-declared-turns-the-system-starts.md) fire as
  turns (#2876), a trigger can start one round per target on a schedule, under
  the same per-round cap. Until then a person starts every round.
- **Capacity is capped where it is spent.** Probes per round and concurrent
  rounds per channel are connector limits, so a schedule cannot exhaust a
  target installation's sandbox quota.

## Consequences

- **The same agent gets the same verdict wherever the tester runs.** One tester
  can serve every installation whose bots share a Slack workspace with it, and
  no installation hands another a credential.
- **It sees only what a person would see.** A trajectory, a connector's
  readiness or a pod's restart count is invisible to it. A failure there shows
  up as a symptom: no reply, or failure text. If a co-located white-box mode is
  added later, it must report separately, so the black-box verdict stays
  location-independent.
- **Tests leave marked messages** in the channels they run in.
- **Probes spend the target's sandboxes and quota.** Hence the per-round and
  per-channel caps, and a person-driven "continue" until triggers exist.
- **Issues cost a person's click.** That is the price of never filing a false
  one; a tester that filed on its own would train people to ignore it.
- **Git can differ from what is deployed.** The report names the commit read.
  It does not claim to have tested a deployed version it cannot see.
- **Only Slack ships first.** Email-only agents wait for the email adapter.

## Alternatives considered

- **Drive the eval lane, as `curie cluster eval` does.** Rejected as the
  mechanism:
  - It needs the target installation's queue and API, so the verdict depends
    on where the tester runs.
  - It skips the Slack surface, which is where the defects above were.
  - On a Slack-connected installation the same driver posts the reply into the
    agent's bound channel (`dispatcher_connected_strict` in
    `cli/src/message.rs` picks that transport), so it doesn't even buy a quiet
    channel.
- **Use delegate calls (ADR-0115).** They are for one agent asking another to do
  work within one installation, and they deliberately avoid human channels.
  Here the human channel is the thing under test, not a transport to avoid.
- **Extend ADR-0042's verifier.** A grader scores cases it is given. It neither
  chooses probes nor reaches a deployed agent over its surface. The tester's
  judgment could use that grader's rubric later without either depending on the
  other.
- **Read the deployed bundle and pending approvals from the platform API.**
  Rejected by the constraint above. It also needs the platform key (decision
  3).
- **File every FAIL automatically.** A FAIL is the tester's judgment, not a
  confirmed defect, and one wrong verdict filed as an issue costs more trust
  than a click costs time.
- **Let the tester repair what it finds.** It would need write access to the
  target's installation, which the location rule already excludes, and a
  tester that changes its target can no longer tell what it tested.
- **Fold it into an operations bot.** Mean testing is a different job from
  answering alerts. The bundles stay separate, and an operator who wants them
  on one Slack app across two installations cannot have that anyway (decision
  1).

## Tracking

On acceptance, file three issues:
1. the bundle and its connector with the Slack adapter (decisions 1 to 6, 10);
2. the falsifiability suite (decision 7);
3. approval-gated issue filing (decision 8).

The email adapter and scheduled runs (decisions 9 and 11) are follow-ups,
filed when they are started.

The first issue is not closed until one live round has run against a deployed
agent in another installation, with the report committed as evidence.

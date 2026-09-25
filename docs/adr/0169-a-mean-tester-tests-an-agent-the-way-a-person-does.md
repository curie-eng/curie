# 169. A mean tester tests an agent the way a person does, from any installation

Date: 2026-09-22

Status: Accepted

**Amended by [ADR-0172](0172-the-mean-tester-is-one-bundle-on-off-the-shelf-mcp-servers.md)**
(back-link added under [ADR-0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md)):
decisions 6, 7 and 8, how decision 2 reads Git, and which probes decision 4's
round may send.

This ADR builds on [ADR-0022](0022-eval-completeness-tier-parity-and-trace-promotion.md),
[ADR-0158](0158-a-custom-connector-is-a-bundle-built-http-mcp-server-that-holds-its-own-credential.md)
and the identity model of ADR-0168 (Draft, #2919). It supersedes nothing.

## Context

An agent is not finished when its tests pass. Its remaining defects are found by
a person who talks to it on the surface its users use: they ask what a user
would, try what a user should not be able to do, and read the reply as the user
will.

On one downstream installation, four defects shipped in one week with every
suite green. Each was found only by reading a reply:
- connectors refused two new agents that lacked a per-agent sandbox label;
- hook turns lost their bot identity and were dropped after the API answered 200;
- a connector credential the sandbox never holds made every reply open with a
  failure preamble;
- an agent answering a resolved alert invented a cause and an action (#2989).

ADR-0022 (`cluster eval`) and ADR-0042 (the `verifier` grader) cover scripted
cases. Nothing covers choosing what to try against a given agent, trying it on
its surface, and reporting. Today that is one person's time.

The verdict must not depend on where the tester is installed. A tester inside
the target's installation could read the API, the cluster and the trajectory;
one elsewhere can read none of them.

## Decision

**A mean tester is an agent bundle, `examples/mean-tester`, that talks to the
agent under test on a surface a person uses, as a person would, and judges only
what that surface shows.**

### 1. On each surface it has its own identity and opens its own conversations

An adapter for a surface must meet three requirements:
- **Own identity.** The tester is a separate provider installation (ADR-0168),
  never the target's. A surface does not deliver an identity its own messages.
- **A new conversation per probe.** One probe never depends on another's
  context.
- **Admission is measured, not assumed.** The adapter ships only once the
  surface is shown to deliver a conversation opened by another agent.

Slack is the first adapter:
- Bolt's `IgnoringSelfEvents` drops a bot's own posts
  (`apps/dispatcher/src/curie_dispatcher/relevance.py:20`, `next` at
  `ad0bc067`).
- A bot-authored mention is refused only inside a thread without an
  operator-listed pair (`relevance.py:217-228`), so a root mention needs no
  configuration on the target's installation.
- The tester's connector posts it with the tester's own token. The platform's
  reply sink only answers inside the thread it was asked in.
- MEASURED 2026-09-23 downstream: one bot root-mentioned another in a shared
  channel and got an answer in the new thread a second later.

Email is the next named adapter. Its admission rule has not been measured.

### 2. What the target should do comes from Git

The tester reads the target's `SKILL.md`, `plugin.json` (tools,
`approvalPolicy`, `toolPolicy`), connector names and `evals/cases.json`, plus
an optional specification path per target. Each report names the commit read.

It holds no platform key and no cluster credential. Every `/agents/**` read
needs the platform key, which also writes.

### 3. A target costs no tester configuration

A request names the target by addressing it on the surface. On Slack that is
`@mean-tester test @target`. The address is where probes go. The bundle is
found in operator-listed repositories by its `plugin.json` name, or by the
deploy target naming that address
([ADR-0089](0089-bundles-declare-their-deploy-targets.md)).

### 4. A run is one round

A turn has 600 seconds by default (`CURIE_DELIVERY_BUDGET_S`,
`apps/worker/src/curie_worker/config.py:741`). One round:
1. Check that the target answers at all.
2. Post the plan: each probe, the behaviour expected, and the case or criterion
   it rests on. The expectation is fixed before the reply exists.
3. Send at most four probes, each marked `[mean test]`. Committed eval cases,
   including earlier failures, take the first slots.
4. Collect final replies and any artefacts, such as an approval card.
5. Report PASS, FAIL or UNCLEAR per probe, quoting the reply.

A person asks for the next round. Scheduled rounds wait for
[ADR-0099](0099-hooks-are-bundle-declared-turns-the-system-starts.md)'s cron
triggers to fire as turns (#2876).

### 5. Verdict rules

- A claim that something was done needs evidence the surface shows. A pending
  approval is a request, not an action.
- A fact, file, link or cause the reply could not have is a FAIL.
- Platform failure text in a reply is a FAIL.
- UNCLEAR goes to a person and is never rounded to PASS.
- The tester never resolves an approval.

### 6. It never repairs, and a person confirms every issue

The tester changes nothing about its target. A FAIL may become an issue draft:
- The draft holds the probe, the quoted reply, the expectation and an eval case
  in the target's `evals/cases.json` shape (ADR-0022).
- An open issue for the same failure is linked instead.
- Filing is one tool, `file_issue`, behind the bundle's `approvalPolicy`. The
  card shows the issue as it will be filed.
- It files only to the repository the bundle was read from.

### 7. Guardrails live in the connector

One connector, built as ADR-0158 describes, holds the surface credentials and
the Git token. The sandbox sees only tools. The connector:
- sends only to operator-listed addresses;
- refuses externally shared conversations;
- marks every probe;
- caps probes per round and concurrent rounds per address.

### 8. The tester has to be able to fail

Its eval suite runs against a fake connector that replays recorded replies.
Real failing replies, including the four above, must come back FAIL. Good
replies must come back PASS (#1649).

## Consequences

- One agent gets one verdict wherever the tester runs, and no installation
  hands another a credential.
- It sees only what a person sees. A connector or pod failure shows only as a
  symptom.
- Probes leave marked messages and spend the target's quota.
- Git can differ from what is deployed. The report names the commit and claims
  nothing further.
- Issues cost a person's approval. That approval is what keeps the tester's
  issues trusted.

## Alternatives considered

- **The eval lane (`cluster eval`).** It needs the target's queue and API, so
  the verdict depends on location. It skips the surface where the defects above
  were. On a connected Slack installation it posts into the agent's channel
  anyway (`dispatcher_connected_strict`, `cli/src/message.rs`).
- **Delegate calls
  ([ADR-0115](0115-agents-call-each-other-with-no-third-party.md)).** They do
  work inside one installation, away from human surfaces. Here the human
  surface is what is under test.
- **[ADR-0042](0042-llm-as-a-verifier-grader-and-progress-signal.md)'s
  verifier.** A grader scores given cases. It neither chooses probes nor
  reaches a deployed agent.
- **Filing every FAIL, or repairing.** A FAIL is a judgment, not a confirmed
  defect. Repairing would need write access to the target's installation.

## Tracking

On acceptance, file three issues:
1. the bundle, its connector and the Slack adapter;
2. the falsifiability suite (decision 8);
3. approval-gated filing (decision 6).

The email adapter and scheduled rounds are filed when started. The first issue
closes only after a live round against an agent in another installation, with
the report as evidence.

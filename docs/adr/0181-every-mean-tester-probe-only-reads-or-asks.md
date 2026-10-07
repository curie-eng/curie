# 181. Every mean tester probe only reads or asks

Date: 2026-09-29

Status: Accepted

Superseded in part by [ADR 0202](0202-a-test-installation-admits-a-listed-bots-actions-and-approval-replies.md): the words "on every installation" no longer
apply to a test installation as ADR 0202 defines it. Everywhere else
read-or-ask stays the rule.

Accepted with explicit maintainer approval from Brian on 2026-09-29,
alongside the forward merge into `next` under ADR 0102.

This ADR amends decision 5 of ADR 0172. Its exception for an operator listed
test installation and its `Next (test installation):` action probes no longer
apply. Every other decision in ADR 0172 stands.

The realizing path is `examples/mean-tester/skills/mean-tester/SKILL.md`, with
the matching operator guidance in `examples/mean-tester/README.md` and
`examples/mean-tester/docs/PERMISSION-MAP.md`. The recorded cases in
`examples/mean-tester/evals/cases.json` verify the verdict rules without
sending an action probe.

## Context

ADR 0172 prevents a mean tester probe from changing anything a target's real
users rely on. It lets an operator list a test installation where an action
probe may run. That list says where the tester may send a message, but it does
not prove that the target's tools, approval routes, external accounts, and
downstream systems are isolated from real effects. An approval gated action
can also place a live card in front of a person before anyone resolves it.

The mean tester uses general Slack and Git tools. It does not provision or
verify an isolated action environment for each probe. Its recorded eval cases
can judge a reply without sending the recorded probe to a target.

## Decision

Every mean tester probe only reads or asks for an explanation, on every
installation. It never asks a target to send, file, change, delete, or share
anything. It never attaches a file or creates or resolves an approval card.
This applies to follow ups and reruns as well as the first probe.

For an action question, the tester asks what the target would need to perform
the action. It may judge a supplied recorded exchange without sending it.
The tester does not treat an operator listed test installation as permission
to execute an action probe.

## Consequences

The mean tester cannot prove that a target completes an action, even in a test
installation. Action behavior needs a separate test that establishes and
checks the isolation of its effects. The mean tester can still check whether
the target explains its authority, requirements, and refusal clearly.

Removing the test installation exception also removes a route by which a
misconfigured test target could reach a real approval or external system.

## Alternatives considered

Keeping the operator list as the action permission was rejected because a
channel label cannot prove isolation of downstream effects.

Allowing action probes only after a target claims to be isolated was rejected
because the claim comes from the component under test. A future test harness
may prove isolation at an external boundary and run those probes under its
own contract.

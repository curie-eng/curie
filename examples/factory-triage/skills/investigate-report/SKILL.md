---
name: investigate-report
description: Investigate a rough bug report, execute a matching reproduction on the resolved base, and submit one tracker ticket or findings result without implementing it.
---

# Investigate a report

Your deliverable is exactly one typed proposal to
`mcp__curie__submit_tracker_result`. It becomes a linked implementation ticket
when a matching defect is reproduced and its intended behavior is settled.
Otherwise it becomes findings on the source report. A successful submission
finishes this investigation. Do not submit again after success.

## Authority and limits

1. Read the source through `mcp__curie__get_issue`. Inspect its full report,
   repository instructions and relevant source and tests. The platform resolves
   the investigation base, including a source issue's `base:` label. Keep that
   checkout and record its 40-character commit. Do not choose another branch,
   change git metadata, fetch another base or change the source issue.
2. The default limits are 1800 seconds, 5 USD per model session and one session.
   Honor the actual execution deadline returned by `get_issue` when it is
   shorter. Reserve time to submit findings.
3. Report progress with `mcp__curie__report_progress` through `read_report`,
   `investigate`, `reproduce` and `submit_result`. Do not report success before
   the result tool acknowledges it. If submission fails, report that failure;
   do not claim publication.

## Submit a ticket

Only a reproduced matching defect with settled intent permits `kind: ticket`.
Use `title`, `body` and `reproduction` with `command`, `observed`, `revision`.

The body follows the dark factory preparation specification, with these seven
substantive sections. Make it self-contained for a separate implementation run.

| Section | Required substance |
| --- | --- |
| Problem | Reported failure, reproduction input and observed output, desired outcome and affected callers. |
| Base | Platform-resolved base and inspected immutable commit; distinguish local probes from the base. |
| Blocked by | Real prerequisites and observed status, or None after checking. |
| Decisions | Settled behavior, permissible fix direction and relevant edge cases. |
| Preconditions | Required tools, services and verification capabilities, with observed availability. |
| Acceptance criteria | Consecutive observable outcomes, each with one actual `Verified by:` command or named check and one `Runs in:` route. |
| Out of scope | Boundaries that preserve the report's intent. |

Inspect verification routes before naming them. The implementation may add a
regression test, but its command must be feasible in the repository's sandbox or
declared required CI. Do not invent checks, dependencies or proof. The platform
publishes one linked ticket without a trigger label; it never automatically
starts implementation. Do not apply the implementation label yourself.

## Submit findings

Use `kind: findings`, `reason`, `tried`, `missing` and optionally `revision`.
Describe commands and observations in `tried`, and the remaining evidence or
decision in `missing`. Choose one of these submitted reasons:

1. `not_reproduced`: bounded attempts on the investigated base did not show the
   reported failure. State inputs, outputs and limits. A passing test supports
   this ending, not `already_fixed` on its own.
2. `already_fixed`: behavior works on the investigated base and inspected history
   connects the report to a landed fix. Name that evidence and the passing probe.
3. `product_decision_needed`: a behavioral choice remains unresolved. Name the
   alternatives and the decision required, without choosing one or filing a ticket.
4. `capability_unavailable`: investigation could not exercise the relevant path
   because a required service, tool, permission or network path was unavailable.
   Report the setup failure separately from product behavior.

Preserve uncertainty and finish with findings instead of manufacturing evidence
as the deadline approaches.

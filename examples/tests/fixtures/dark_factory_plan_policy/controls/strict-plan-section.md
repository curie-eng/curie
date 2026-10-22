## 4. Plan review (phase `plan_review`, same `round` as the plan)

Call report_progress with phase `plan_review` and round `<n>`.

Call the `Agent` tool with `subagent_type` `"dark-factory:plan-reviewer"`,
`description` `"Plan review round <n>"`, a `prompt` with the issue, the
numbered acceptance criteria and the full plan with its verification table,
and `run_in_background` `false`. Wait for the reply. A real reply starts with
`REVIEWER: plan-reviewer`.

Read the `VERDICT:` line that follows. On `VERDICT: APPROVE`, go to step 5.
On `VERDICT: CHANGES` in round 1 or 2, go back to step 3 with the next round:
revise the plan to address every finding, then call the plan reviewer again
for that round. On `VERDICT: CHANGES` in round 3, stop as in "Loop cap"
below. Any other reply means the review did not happen; stop as in "Loop cap"
below.

No repository edit happens until the plan reviewer returns `VERDICT: APPROVE`.
The plan loop runs at most 3 rounds. This section overrides every other
statement in this skill about how many plan reviews a run has.


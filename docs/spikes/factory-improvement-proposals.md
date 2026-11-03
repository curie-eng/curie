# Spike: evidence-backed Software Factory improvements

Date: 2026-11-03
Status: design spike; no production run or cost result has been measured

## Question

Can an administrator review a concrete, evidence-backed suggestion to improve a factory agent, test it against a baseline, and roll a successful change into the agent or its operator-owned configuration?

This spike scopes the first suggestion to reviewer model selection. The same proposal format could later cover bundle instructions, review gates, repository runner images, and sandbox configuration.

## Findings from the current tree

- `GET /work-items`, `GET /work-items/{id}`, and `GET /work-items/{id}/usage` expose work item state, requests, progress, PR and CI observations, and usage. The list is newest-first, capped at 200, and reports `truncated`. It is not a complete historical export. See `apps/api/src/curie_api/routers/work_item_outcomes.py` and `docs/guides/measuring-factory-outcomes.md`.
- Usage is recorded once per request, turn, model, and role. The runner reports input, cached input, cache writes, and output tokens at turn end. The API estimates cost when a matching public price is available; `cost_complete=false` or `requests_without_usage>0` means the estimate is incomplete. See `runner/src/curie_runner/usage_report.py` and `apps/api/src/curie_api/factory_usage.py`.
- Factory progress reports carry phase, round, and activity, while the default bundle's review hook emits observed phase and review-gate events. The public usage response aggregates by role and model, not by phase or review round. A report must not claim exact phase spend from those aggregates. See `apps/api/src/curie_api/factory_progress.py` and `examples/dark-factory/hooks/review_gate.py`.
- The default bundle has an operator-owned reviewer-model override. The two reviewers declare an alias, and the override selects a concrete provider model without editing the bundle. See `examples/dark-factory/README.md`.
- The environment maintenance skill already groups failed work by cause and proposes operator-reviewed repairs. It is a precedent for proposals, not a source of measured model quality. See `examples/dark-factory/environment/skills/factory-environment-maintenance/SKILL.md`.
- A PR opening is not an accepted engineering outcome. Acceptance and human review effort still require an explicit trial ledger. See `docs/guides/measuring-factory-outcomes.md`.

## Thin slice to implement

Build an optional operator-side command that reads an explicit cohort ledger and the existing work-item detail and usage endpoints, then emits one proposal report. It does not run in a factory sandbox and receives the normal operator API credential. It does not write to Curie, change a model, create a PR, or route a live request.

The cohort ledger names work item IDs and records the observation cutoff, acceptance decision, human review minutes if measured, and any exclusion reason. An explicit list avoids treating the capped `GET /work-items` response as a historical cohort. The command fetches each named work item and usage response, joins all requests and revisions under that work item, and preserves missing values as unknown.

A report should contain:

```json
{
  "proposal_kind": "reviewer_model_experiment",
  "cohort": {"name": "example-trial", "cutoff": "2026-11-03T00:00:00Z"},
  "evidence": {"work_item_ids": ["..."], "eligible": 0, "accepted": 0,
               "usage_complete": 0, "requests_without_usage": 0},
  "baseline": {"implementer_model": "...", "reviewer_model": "...",
               "estimated_cost_per_accepted_usd": null},
  "candidate": {"reviewer_model": "...", "scope": "one operator binding"},
  "prediction": {"metric": "cost_per_accepted_issue", "direction": "decrease"},
  "guardrails": ["acceptance rate", "human review minutes",
                 "review rounds", "CI repair rate", "escaped defects"],
  "status": "candidate_requires_experiment"
}
```

These are proposal fields, not a claim that the current API stores this object. The generated report should link each underlying work item and label estimated costs and unpriced usage. The candidate model is supplied by an operator-approved allowlist; the analyzer cannot discover and switch to arbitrary provider IDs.

## First end-to-end experiment

1. Fix a baseline bundle commit, runner image digest, implementer model, reviewer model, task-selection rubric, and observation cutoff. Record model prices with their source and `as_of` time.
2. Select a small set of historical tasks for screening. Replay can reveal obvious regressions and compare reviewer findings, but replayed tasks do not establish live production savings.
3. If screening passes, apply the existing reviewer-model override to a limited live cohort. Keep the implementer model and factory workflow fixed. Record the assignment before each task starts; count all assigned tasks, including failures and cancellations.
4. Compare total model cost over all attempts per accepted issue. Report acceptance rate, review rounds, CI repair rate, elapsed time, human review minutes, and known escaped defects alongside it. If the accepted denominator is zero, cost per accepted issue is undefined. If usage is incomplete, report known estimated cost and coverage without claiming a complete cost comparison.
5. Promote or revert the operator setting after an administrator reviews the report. Preserve the previous setting and the experiment decision.

The initial screen should show the proposal, supporting and counterexample runs, data coverage, predicted benefit, guardrails, and exact operator change. An administrator can dismiss, request more evidence, approve a limited experiment, or approve a measured rollout. No suggestion applies itself.

## Gaps and follow-up implementation work

1. **Cohort and acceptance record.** Curie has work-item outcomes, but it does not infer human acceptance or human effort. Keep those fields in the operator ledger for the first slice. A later product UI could make them first-class.
2. **Historical completeness.** The bounded list is unsuitable for cohort discovery. The first slice requires explicit IDs. A paginated or date-filtered export is a separate API change if administrators need automatic recurring scans.
3. **Attribution.** Role-level costs support the first model experiment. Exact cost by factory phase or review round needs an additive usage/event join. Do not allocate turn-end usage to phases by timestamp alone when a turn crosses a phase boundary.
4. **Change identity.** A credible comparison needs bundle commit, runner image digest, model IDs, and assignment recorded per task. Verify which values the current work-item and deployment records can supply before automating experiment reports.
5. **Quality.** CI green and an opened PR are weak proxies. A human acceptance rubric and follow-up defect window are required for rollout decisions.
6. **Privacy and authority.** Run traces and issue content may contain private deployment data. Keep the operator-side evidence local to that installation. Public upstream reports use synthetic examples. The analyzer reads; existing operator controls perform any model or environment change.

## Spike conclusion and next step

The first vertical slice is feasible with existing outcome and usage APIs plus an explicit operator ledger. It can produce a reviewable reviewer-model experiment proposal without changing the runtime. The main unknown is whether administrators can consistently provide accepted-outcome labels and enough comparable tasks; no cost-saving claim follows from the current tree alone.

Implement the operator-side report with synthetic fixtures and tests for retries, incomplete usage, zero accepted outcomes, and a truncated work-item list. Then validate it on an authorized installation before adding scheduled scans or an admin UI.

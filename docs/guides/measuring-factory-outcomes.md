# Measuring Software Factory outcomes

Evaluate the factory by accepted work and human effort, alongside model usage.
A pull request or merge request being opened is an output; it is not automatically a completed
engineering task or saved developer time. Start with a bounded trial and record
both successes and failures. See the [factory overview](../software-factory.md)
and [administrator guide](../administering-co-workers.md).

## Available evidence and its limits

The outcome and usage APIs below provide evidence for a trial. Individual
executions can still have missing usage or unpriced models. Check the
[capability reference](../capabilities.md) and the surfaces available in your
installed release before choosing a measurement method.

| Evidence | Supported surface | What you must still measure |
|---|---|---|
| Issue, PR, execution requests, state, and cause | Issue status comment; `curie cluster work-items [ID]`; operator API `GET /work-items` and `GET /work-items/{id}` | Your cohort, completion definition, observation cutoff, and exclusions |
| CI at the PR or merge-request head | The selected code host's CI status; `ci` on the work item detail API | Historic CI snapshots if you need a time series; list API has `ci: null` |
| Tokens and estimated cost over requests | Operator API `GET /work-items/{id}/usage`; terminal issue usage summary when recorded | Provider-billed cost reconciliation and costs outside reported model usage |
| Implementer/reviewer split | Usage API `roles` and `models` | Whether a model choice improves acceptance or reduces review effort |
| Merged change | The code host's PR or merge-request state and timestamp | Whether it meets the agreed task definition and remains accepted |
| Accepted change | Human decision recorded in the code host or your trial ledger | Acceptance rubric; this is not a platform-derived business KPI |
| Human effort | Manual time log | Issue preparation, supervision, review, rework, and operational effort |

These APIs require an operator platform API key; follow
[reading work item outcomes](../operations.md#reading-work-item-outcomes).
The list endpoint returns at most 200 items and exposes `truncated`, with
newest updates first. It is not a complete historical export or a date-filtered
analytics API. A truncated list cannot establish a full cohort; keep a manual
issue ledger for the trial and fetch each known item's detail and usage.

The usage shape is defined in
[work item schemas](../../apps/api/src/curie_api/schemas/workitems.py), the routes in
[`work_item_outcomes.py`](../../apps/api/src/curie_api/routers/work_item_outcomes.py),
and aggregation/pricing in
[`factory_usage.py`](../../apps/api/src/curie_api/factory_usage.py).
`requests` sum retries and CI repair requests on the work item. Deduplicate by
work item and request IDs; do not sum both the work item total and its request
breakdown. Reconcile relabelled attempts by issue so one issue is counted once
in your outcome denominator.

## Define the cohort before the run

Use issues first delegated during a named interval, then record outcomes at a
stated cutoff. For example: ten eligible issues admitted during a seven-day
trial, inspected seven days after the trial ends. Keep still-running items in
the cohort and report them separately. For a cost comparison, state whether
cost includes all attempts through the cutoff or only charges incurred inside
the intake interval. Use the same boundary across both methods.

Record this minimal ledger manually:

| Field | Why it matters |
|---|---|
| Issue and work item IDs; PR URL | Join attempts, revisions, usage, and the accepted result |
| Delegated and observed timestamps | Define eligibility, elapsed time, and the cutoff |
| Outcome and cause | Distinguish opened PR, accepted, merged, failure, cancellation, and pending |
| All request IDs and retry/repair counts | Keep unsuccessful work in the cost numerator |
| Usage completeness and pricing source/date | Identify partial estimates rather than treating them as totals |
| Human minutes by activity | Measure effort retained after delegation |
| Baseline effort and selection method | Make any time savings estimate reproducible |

Use these definitions consistently:

- **PR output rate:** distinct issues with a PR / all delegated issues.
- **Accepted outcome rate:** distinct issues whose PR meets your human
  acceptance rubric / all delegated issues. Record acceptance even if merge
  is deferred; do not count multiple revisions as separate accepted outcomes.
- **Merged outcome rate:** distinct issues with an accepted, merged PR /
  all delegated issues. Report failures, cancellations, and pending separately.
- **Model cost per accepted or merged outcome:** cost of every attempt for
  the cohort / accepted or merged outcomes, respectively. Include failed,
  cancelled, retried, and CI repair work. If the denominator is zero, the
  metric is undefined, not zero.
- **Human effort per outcome:** all measured cohort preparation, supervision,
  review, and rework minutes / accepted or merged outcomes. Track one-time
  setup separately and disclose whether operational time is included.
- **Elapsed time:** delegation to acceptance or merge, including queueing and
  CI. This measures latency, not active human minutes or model execution time.

## Estimates, billing, and missing data

The usage API returns `estimated_cost_usd`, `cost_complete`,
`requests_without_usage`, per-role tokens, model breakdowns, and
`price_sources` with source and `as_of` timestamps. Its public model price
lookup is distinct from a provider invoice. Preserve those fields with each
measurement. Token counts include separate input, cached input, cache-write,
and output categories; price them with their corresponding recorded rates.

If `cost_complete` is false, the aggregate covers only the known priced usage;
it is a lower bound, not the full cost. A started request with no report is
missing coverage. Tokens for an unpriced model remain visible but its cost is
unknown. Known zero tokens can be zero cost; missing data cannot.
Never replace null with zero when averaging or summing. Report known estimates
and the number of incomplete items, and defer a complete cost-per-outcome
claim until coverage is sufficient.

Provider-billed cost must be collected separately from the provider's billing
or generation records and reconciled to the trial's attempts. Label it as
billed only after that reconciliation. Budget ceilings, token counts, and
SDK cost estimates are not invoices. Model costs also omit infrastructure,
licenses, and human time unless you explicitly add them.

## Worked example: illustrative anonymous data

These numbers are invented to show arithmetic, not Curie benchmark results.
The cohort has ten issues, observed at the same cutoff, with 14 execution
requests including retries and CI repair. All requests have complete estimated
usage in this example.

| Observed outcome | Issues | Estimated model cost across all attempts | Human minutes |
|---|---:|---:|---:|
| Accepted and merged PR | 6 | 24 USD | 150 |
| Open PR, not accepted | 2 | 8 USD | 50 |
| Failed without PR | 1 | 5 USD | 20 |
| Cancelled without PR | 1 | 3 USD | 10 |
| **Total** | **10** | **40 USD** | **230** |

Of the 40 USD estimate, 28 USD is implementer usage and 12 USD reviewer usage.
This is an accounting split, not evidence that reviewers saved money.

- PR output rate: `8 / 10 = 80%`.
- Accepted and merged outcome rates: `6 / 10 = 60%` each.
- Estimated model cost per accepted or merged outcome:
  `(24 + 8 + 5 + 3) / 6 = 6.67 USD`, rounded to cents.
- Human effort per merged outcome: `230 / 6 = 38.33 minutes`, including
  effort on unsuccessful issues. One-time setup is excluded here and disclosed.

Dividing only the successful issues' 24 USD by six would hide 16 USD of
unsuccessful work. If one of the failed attempts had missing usage, the
40 USD total would instead be partial and the 6.67 USD quotient would be only
a lower bound. If provider billing later reconciles to 43 USD for these same
attempts, billed model cost per merged outcome is `43 / 6 = 7.17 USD`.

## Before claiming ROI

Compare equivalent accepted work, with similar complexity and verification
requirements. Establish a baseline from measured human-only tasks or a
controlled matched cohort. Report how tasks were selected and the sample size;
a developer's guess about hours saved is an assumption, not an observation.

For an illustrative baseline of 360 measured human minutes to complete six
comparable accepted outcomes, the example's 230 cohort minutes implies
`360 - 230 = 130 minutes` less human effort. At an explicitly assumed loaded
labor rate of 60 USD/hour, that is `130 / 60 * 60 = 130 USD` of estimated labor
value. Subtracting 43 USD reconciled model cost leaves 87 USD before
infrastructure and setup. Neither the labor rate nor this hypothetical
comparison is a measured Curie saving.

For financial ROI, add all incremental operating and amortized setup costs,
state the comparison period, and define the denominator. One possible formula
is `(valued human effort reduction - incremental costs) / incremental costs`.
A zero cost denominator makes that ratio undefined. Include quality regressions
and downstream rework in follow-up observations; an accepted PR at the cutoff
can still need correction later. Use the first trial to decide what to change,
not to promise a universal productivity multiplier.

# Does preparation reduce human intervention?

Run a bounded comparison of usual issue intake and the preparation skill before
adding more workflow machinery. This protocol records evidence manually using
existing factory outcomes and usage. It creates no service, automatic upload or
publication requirement. The blank [outcomes.csv](outcomes.csv) contains no results.

## Set up a private trial

Copy this directory's CSV into `.curie/factory-preparation-pilot/` in your working
repository. Keep real issues, customer context and results out of this public
source tree. Verify that the local destination is ignored before saving records.
Keep sensitive details in your normal private tracker rather than copying them
into the ledger. The installed skill can record preparation observations in its
local report; it does not assign trial arms or upload reports.

Before inspecting outcomes, record these choices in a private `protocol.md` beside
the CSV:

1. The enrollment interval, eligibility rules, target number of original requests
   and a fixed observation cutoff for every request, such as seven days after
   submission. Set an equivalent cutoff from enrollment for requests never
   submitted. Record the actual deadline on every row.
2. The acceptance rubric and who independently verifies it. For example, the
   agreed behavior works, required checks pass, and the reviewer accepts the
   change. An opened PR alone is not acceptance. If practical, hide the trial arm
   from the acceptance reviewer.
3. The default two arms: `usual` follows the team's ordinary intake process; `prepared`
   uses the preparation skill with repository investigation, questions according
   to ambiguity, full specification review and explicit publication consent.
   An optional third `external` arm can use a named existing local skill workflow.
   Record its exact revision and whether it includes the Curie readiness review;
   keep that choice fixed rather than silently mixing preparation treatments.
   The authoring skill's popularity is not an outcome measure.
4. The assignment method. Enroll before preparation and assign before outcomes.
   Randomize within comparable task types or sizes when feasible. Otherwise use
   a documented matching rule and describe the comparison as observational.
   Do not put only easy or already specified requests in the prepared arm.
5. The execution setup: repository and base, factory version, model, harness,
   permissions, verification rules and run budget. Keep these comparable across
   arms. Record deliberate differences and changes during the trial.
6. The effect size that would justify continuing, the primary metric and how to
   report missing data. Choose these before observing results. Do not stop early
   solely because an appealing result appears.

One row represents one original request. Give it a local anonymous `request_id`
and record `arm`, `enrolled_at` and `cutoff_at` immediately. Coordinator supplied
IDs and assignment are inputs to preparation, not decisions for the skill to
invent. Do not switch assignment after seeing questions, failures or outcomes.
Measure ordinary issue preparation in the usual arm too.

If preparation splits a request into several issues, retain one original row and
list the child issues in its private `issue_urls` cell. Acceptance requires all
required children and integration proof. Sum time and usage across the children
and all attempts once. Relabels and retries do not create new enrolled requests.
Keep refused, cancelled, failed, timed out and never submitted requests in the
original assigned arm and denominator, even if preparation exposes an impossible
request. Report that discovery separately as useful evidence.

## Record observations

Use ISO 8601 timestamps with a timezone and decimal minutes for active human time.
Empty cells mean unknown. Enter zero only when observed or explicitly confirmed.
Do not estimate elapsed chat time as active work. Count related clarification
questions and rounds separately; review and publication choices are not product
clarification questions.

| CSV field | Recording rule |
| --- | --- |
| `request_id`, `arm` | Stable anonymous original request ID; `usual` or `prepared` |
| `enrolled_at`, `submitted_at`, `cutoff_at`, `observed_at` | Enrollment, first factory submission, fixed deadline and outcome observation; leave submission empty if never submitted |
| `issue_urls`, `work_item_ids` | Private joins to the original issue, required children and all attempts; separate several values with semicolons |
| `outcome` | `accepted`, `not_accepted`, `failed`, `refused`, `cancelled`, `timed_out`, `pending` or `not_submitted` at cutoff; explain the cause in notes |
| `accepted` | `yes`, `no` or empty if unassessed; use the predefined human rubric, not just PR state |
| `clarification_questions`, `clarification_rounds` | Observed preparation counts; missing logs remain empty |
| `preparation_minutes` | Active issue investigation, questions, drafting, spec review and publication before first submission, including usual intake preparation |
| `post_submission_interventions` | Distinct human intervention episodes needed to move work forward after submission: clarification, correction, rerouting or retry direction. Count one connected exchange as one episode, even if several messages. Routine final acceptance review alone is not an intervention; a requested correction is |
| `supervision_minutes` | Postsubmission monitoring, clarification and intervention time, excluding review and rework |
| `review_minutes` | Postsubmission inspection, testing and acceptance review time, excluding hands on rework |
| `rework_minutes` | Human editing or repair after submission, excluding time already counted as supervision or review |
| `estimated_model_cost_usd`, `cost_complete` | Existing usage estimate over every attempt through cutoff; `yes`, `no` or empty for completeness |
| `notes` | Acceptance evidence, setup deviations, missing data, exclusions from a specific analysis and causes; never remove the enrolled row |

Time categories must not overlap. Record one time block in one category only.
Track setup and shared operational effort separately in `protocol.md` with the
allocation rule; disclose whether totals include it. A not submitted request can
still incur preparation time or model usage. Review later corrections separately
if they occur after the fixed cutoff rather than silently rewriting the endpoint.

Read the existing [factory outcome and usage guide](../../../../docs/guides/measuring-factory-outcomes.md)
for work item detail, usage endpoints, pricing provenance and completeness rules.
The list API is not a complete cohort export. Use the enrolled ledger to find all
known work item IDs, including failed and retried attempts. Store usage snapshots
and price sources privately. Include preparation model cost if available through
your coding environment; otherwise report it as missing separately from factory
cost. Do not sum work item totals and their request breakdown again.

## Compare at the cutoff

Report each arm's enrolled count, observed outcomes and missing data before any
comparison. Use all original enrolled requests in these two primary denominators:

* Accepted outcome rate: accepted original requests divided by enrolled requests.
* Accepted without intervention rate: accepted original requests with observed zero
  postsubmission interventions divided by enrolled requests. Unknown intervention
  counts do not qualify for the numerator.

Also report intervention episodes per enrolled request, the distribution of those
counts, and total active human minutes per accepted original request. Total human
time is preparation plus supervision plus review plus rework across the entire
arm, including failures and requests never submitted. Dividing only successful
requests' time would hide the cost of unsuccessful work. Report each activity too,
so a reduction after submission can be checked against added preparation effort.

Show the difference between arms in percentage points for rates and in minutes or
counts for effort. If there are no accepted outcomes, time or cost per accepted
outcome is undefined. Unknown acceptance does not count as accepted; report the
number unassessed and do not interpret that conservative rate as complete truth.
For time, interventions and cost, show coverage by arm and known totals separately.
Do not replace missing observations with zero or compare a partial total with a
complete one as if they were equivalent. Cost estimates with incomplete usage are
lower bounds. Report factory cost and preparation cost coverage separately.

Interpret small samples as exploratory. Report task mix, assignment method,
uncertainty, quality regressions and deviations. A nonrandom comparison cannot
isolate preparation from task selection or setup changes. Use the result to decide
whether to continue, change the skill or gather a larger controlled sample; it is
not a promise that every factory will improve.

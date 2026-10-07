# Automated remediation implementation plan

This plan realizes the [automated remediation contract](../specs/2026-10-07-automated-remediation.md)
for epic [#4074](https://github.com/curie-eng/curie/issues/4074) and
[#4144](https://github.com/curie-eng/curie/issues/4144). Criterion names below
abbreviate `AUTOMATED-REMEDIATION-*` as `AR-*` and `ACTION-EXECUTOR-*` as `AE-*`.
The feature work targets `next`, where ADR 0203 is Accepted as of `fc1527985`
(#4082).

## Commit and ownership order

Commit the specification and this plan first, as a documentation change with no
runtime behavior. For every later task, a test author reads the specification
and writes behavioral tests without implementing anything, runs them against
the unchanged code, records the observed product failure, and commits the
failing tests alone. A separate implementer follows in a later commit. Every
new test and implementation unit cites its criterion with an `@spec` marker.
Specification, security and quality reviews follow each task; fix findings
before starting a task that depends on it. Independent tasks proceed in
parallel worktrees with their own disposable Postgres and Valkey. Each
hand-written migration takes the next free revision on `next` at the time it
merges and is checked against `main`'s numbering, because the two lines have
collided before.

## State of the executor this builds on

The executor plan lists tasks 0 to 10 and 13 as done, with task 6's API half and
task 7's fixture. Inspected on `next` at `9313d753e`, two differ and are recorded
here so no task here assumes them:

* **Executor task 13 (CLI `actions` group) is not on `next`.** It exists only on
  an unmerged task branch.
* **Executor task 10 (worker recording) is partial.** Nothing writes
  `agent_actions.connector` or `connector_digest`, so no record passes the
  `refused_no_digest` check and nothing is undoable yet. Reversible automatic
  actions (AR-8 check 8) and qualification of reversible actions (AR-22) depend
  on its completion.
* Task 7's reference connector is also on an unmerged branch, and task 11 (the
  worker executor loop) and task 12 (the forward seam, AE-19) are open.

## Tasks

Waves group tasks that can start in parallel once their dependencies merge.

| Task | Criteria | Owner (directory) | Depends on | Positive and negative proof |
| --- | --- | --- | --- | --- |
| 0. Commit spec and plan | none | docs | none | The docs check (`scripts/check-docs.sh`, run with bash) passes. |
| **Wave 1** | | | | |
| 1. Measure the open facts | AR-6, AR-12, AR-15, AR-18 | integration owner | 0 | Recorded, anonymized observations for M1 to M5 below. A fact that contradicts the spec stops the dependent task and returns to the spec. |
| 2. Shared vectors and parity entries | AR-5, AR-12, AR-17, AR-26 | `tests/vectors` and AGENTS.md (integration owner) | 0 | `remediation-nomination`, `remediation-predicate`, `remediation-policy`, `remediation-codes`, and the `read` phase added to `runner-execute`; failing reader tests on each consuming side; AGENTS.md entries. |
| 3. Policy store and administration | AR-1, AR-2, AR-3, AR-10 (limit schema), AR-24 (kind rules) | `apps/api` (regenerated `apps/ui` types) | 0 | Migration round trip on real Postgres; CAS, idempotent operation id, immutable generations; every validation refusal; a hook-key-signed write refused `401`; a write or breaker close without an operator principal refused `operator_principal_required` and the principal recorded as `bound_by`; chart and compose `remediation.enabled` render into API and worker, refused with the executor off. |
| **Wave 2** | | | | |
| 4. Policy CLI | AR-3 (CLI), AR-20 (CLI receipt) | `cli` | 2, 3 | `remediation-policy` and `remediation` verb groups under `local` and `cluster`; one JSON object per verb under `--json`; mirrored validation refuses with the API's reason; write verbs present the operator principal from `CURIE_APPROVAL_PRINCIPAL_TOKEN` and refuse without it; CLI manifest regenerated. |
| 5. Remediation generation at admission | AR-4 | `packages/protected-hooks`, `apps/api` (`apps/api/src/curie_api/routers/hooks.py`), reviewed by the #3603 owner | 3; protected hooks plan task 3 | Intent and envelope carry `remediation_generation` read under the agent gate; an envelope without it refuses automatic execution only; a policy write racing an admission yields either the old or the new generation, never a mix (real Valkey and Postgres). |
| 6. Nomination route and parser | AR-5, AR-7 | `apps/api` | 2, 3 | Idempotent per `event_id`; `nomination_conflict`, `not_protected_event`, `remediation_disabled`; one row per entry; every parse refusal from the vector. |
| 7. Read executions | AR-12 | `apps/api` (migration, sample route), `runner` (`read` phase), `apps/worker` (executor loop) | 2; executor task 11 | Pointed scalar only crosses; `tool_not_read_only` without dialing; sandbox released after the last sample and after a crash; vector fails one-sided. |
| 8. Forward execution with authority, ledger fields and actor | AR-13, AR-14 | `apps/api` with `apps/worker` | 3; executor tasks 11 and 12 | One ledger row per execution with closed `authority_kind`, generation in `authority_ref`, `actor_kind`, delivery and nomination ids; replay creates nothing; legacy rows unchanged; an unknown authority kind violates the check. |
| **Wave 3** | | | | |
| 9. Admission, limits, breaker, disarm | AR-8, AR-9, AR-10, AR-11 | `apps/api` | 5, 6, 7, 8 | Table-driven order through real producers; racing last slot yields one execution; injected read failures never execute; breaker closes only through the administrative route with an operator principal; incident window of one hour after verification, lengthen-only, never read from the alert body; disarm and kill switch take effect between admission and dispatch. |
| 10. Remediation approvals | AR-15, AR-16 | `apps/api` (purpose, resolution), `apps/worker` (card loop beside the publication loop) | 6, 8 | Bound `granted_arguments`; dedupe attaches; card on the route with existing action ids; approve yields one execution and no resume turn; reject and expiry yield none; tampered arguments refused. |
| 11. Verifier | AR-17, AR-18 | `apps/api` (evaluator, scheduling), `apps/worker` (sampling loop) | 7, 8 | Four outcomes on a real cluster; settle respected; independence refused at write and at admission; one outcome per record. |
| 12. Escalation and authority-aware undo | AR-19 | `apps/api` | 8, 10, 11 | Report, breaker and undo approval on any non-`verified`; approval drives one restore under the approving principal; policy and approval records refuse outsiders; no automatic undo path exists (no caller of the ruling without a principal). |
| **Wave 4** | | | | |
| 13. Receipts and telemetry | AR-20, AR-21 | `apps/worker`, `packages/telemetry`, `cli` | 9, 10, 11, 12 | One thread message per stage; metric manifest includes the counter with bounded domains; capture contains no argument, sample, reason or alert text. |
| 14. Nomination capture in the protected worker | AR-6 | `apps/worker` (protected lane runner client wrapper) | 2, 6; protected hooks plan task 4 | `done` turn submits once and the posted reply has no block; other statuses submit nothing; retries cannot add nominations; off-limits files untouched. |
| 15. Qualification records | AR-22, AR-23 | `apps/api`, `cli` | 8, 10, 11 | Evidence references checked by state and digest; digest upgrade makes the record stale; verifier-run route accepts no tool or arguments; `automatic` refused without a record. |
| 16. Tuning and prevention kinds | AR-24, AR-25 | `apps/api`, `apps/worker` (card rendering) | 9, 10 | `prevent` always asks and is verified when approved; one tuning request per recorded series with platform-rendered diff and declared-read evidence; approval ends `tune_execution_not_automated` with no write. |
| **Wave 5** | | | | |
| 17. Documentation | AR-12, AR-26, AR-27 | docs, `ARCHITECTURE.md`, ACI producer interface (seam owner review), example bundle docs | 7, 13, 14 | The `read` phase listed beside `/v1/execute`; the remediation data path; the nomination block author guide; the qualification drill order; the SRE example's intake guide states the policy boundary honestly. |
| 18. Complete campaign | all | integration owner | 1 to 17 | On the final artifacts: protected delivery to automatic remediation verified; out-of-bounds to approval to execution verified; `not-recovered` to report, breaker and approved undo; disarm, kill and limits refusals; a recorded duplicate-rule series to one tuning request. |

### Measurements in task 1

Each is run in a disposable environment, recorded with the exact command,
candidate commit and observed output, and published only with placeholder
identifiers.

* **M1.** Whether the protected worker's planned runner client wrapper sees the
  same `Final` the kernel renders on a retried attempt, and how many `Final`
  frames a retried protected turn produces, against the planned wrapper as soon
  as protected hooks task 4 has a branch.
* **M2.** Executor sandbox hold time and pool impact of a verifier read
  execution held for a 600 second deadline at 10 second intervals, against an
  ordinary turn's claim latency on the same pool.
* **M3.** Structured content shapes returned by representative read connectors
  (a metrics query and an alert state read) at a pinned digest, and whether a
  JSON pointer reaches the needed scalar without transformation.
* **M4.** That a remediation-purpose approval created with no model turn renders
  and resolves through the operator principal path with the dispatcher at zero
  replicas, as the publication purpose does.
* **M5.** Lock contention of the per-agent-and-hook admission lock under a burst
  of 50 deliveries for one hook.

### Parity seam entries added in task 2

To be appended to the AGENTS.md registry in the same change as the vectors:

> protected worker vs API remediation nomination (AR-5, AR-6) -- the protected
> worker's block extractor and the API's nomination parser ship in different
> images, so the fence grammar, bounds and valid and invalid blocks are frozen
> together. [vector: `remediation-nomination` under `tests/vectors`]
>
> runner vs worker vs API remediation predicate (AR-12, AR-17) -- the runner's
> pointer extraction, the worker's sample report and the API's evaluator must
> agree on every pointer and comparator case. [vector: `remediation-predicate`
> under `tests/vectors`]
>
> API vs CLI remediation policy validation and codes (AR-3, AR-20) -- the API
> validator and the CLI's mirrored validation, and the closed states, refusal
> codes and outcomes the API, the worker receipt and the CLI render. [vector:
> `remediation-policy` and `remediation-codes` under `tests/vectors`]

The existing `runner-execute` entry gains the `read` phase in the same change.

## End to end tiers

Every behavior-bearing task classifies all seven tiers. `R` is required; `n/a`
cites the reason code under the table. Tasks 0 and 17 change documentation
only, task 1 records observations, and task 2 adds vectors and failing tests;
none of them changes runtime behavior, so they carry no tier and run the
repository validators for their artifacts.

| Task | skill | local | local-release | cluster | live provider | external integration | factory |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 3 | n/a (f) | R | R | R | n/a (c) | n/a (d) | n/a (e) |
| 4 | n/a (f) | R | n/a (a) | R | n/a (c) | n/a (d) | n/a (e) |
| 5 | n/a (f) | R | n/a (a) | R | n/a (c) | n/a (d) | n/a (e) |
| 6 | n/a (f) | R | n/a (a) | n/a (b) | n/a (c) | n/a (d) | n/a (e) |
| 7 | R | R | n/a (a) | R | n/a (c) | n/a (d) | n/a (e) |
| 8 | n/a (f) | R | n/a (a) | R | n/a (c) | n/a (d) | n/a (e) |
| 9 | n/a (f) | R | n/a (a) | R | n/a (c) | n/a (d) | n/a (e) |
| 10 | n/a (f) | R | n/a (a) | R | n/a (c) | R | n/a (e) |
| 11 | n/a (f) | R | n/a (a) | R | n/a (c) | n/a (d) | n/a (e) |
| 12 | n/a (f) | R | n/a (a) | R | n/a (c) | n/a (d) | n/a (e) |
| 13 | n/a (f) | R | n/a (a) | R | n/a (c) | R | n/a (e) |
| 14 | R | R | n/a (a) | R | R | R | n/a (e) |
| 15 | n/a (f) | R | n/a (a) | R | n/a (c) | n/a (d) | n/a (e) |
| 16 | n/a (f) | R | n/a (a) | R | n/a (c) | R | n/a (e) |
| 18 | R | R | R | R | R | R | n/a (e) |

Reasons:

* (a) No released binary, image identity, install path, version pin or release
  compose change in this task. Task 3 adds the release compose value, so it
  carries the row.
* (b) No chart template, RBAC, securityContext, NetworkPolicy, sandbox claim or
  init container change; the behavior is in the API, exercised by the local
  tier.
* (c) No model routing, credential resolution, provider auth or token
  accounting change, and nothing on the runner MCP catalog projection,
  PreToolUse, platform MCP tool, workspace publication or coding-tool path set.
  Task 7 adds a runner route outside the model loop; task 14 reads model output,
  so it carries the row.
* (d) No Slack, webhook, connector OAuth or third-party API shape change. Tasks
  10, 13 and 16 post cards and messages to a channel, and task 14 changes the
  posted reply, so they carry the row.
* (e) No factory runtime, CI, progress, publication or work item path.
* (f) No plugin packaging, runner turn loop, ACI event or skill eval change.
  Task 7 changes the runner, and task 14 changes what a turn's final output
  produces, so they carry the row.

What the local tier proves: the local tier runs no caller proxy and refuses
`SecretRef`, so every grant-bound forward execution there refuses
(`tool_not_grant_bound`) and no record is undoable; the protected lane is not
available there either. The local evidence for tasks 7 to 16 is the policy, the
parse, the admission decision, the approval and the observed refusals; positive
execution, verification and undo evidence is cluster only. Until the protected
worker exists, task 14 and the delivery-driven rows of task 18 proceed only under
a visible `Discovery waiver` naming
[#3603](https://github.com/curie-eng/curie/issues/3603), which records the
missing proof and does not mark the criterion passed; tasks 6 to 13 are driven
by inserting the protected binding a delivery would have created, in real
Postgres and Valkey.

Commands: `CURIE_E2E_TIERS=<tier> curie dev e2e-ladder` per required tier,
`curie dev chart-runtime-e2e` for task 3's runtime assertion, and the live rungs
with `CURIE_E2E_LIVE=1` for tasks 14 and 18. Each acceptance criterion records a
positive observation and a falsifiable negative from the specification.

## Core files owned by others

The worker kernel package `apps/worker/src/curie_worker/kernel/`,
`apps/worker/src/curie_worker/consumer.py`,
`apps/worker/src/curie_worker/threadlock.py` and
`apps/worker/src/curie_worker/markers.py` are off-limits. No task edits them.
The wrappers:

* **Nomination capture (task 14).** Inside the protected lane's runner client
  wrapper, owned by protected hooks task 4; this task adds the extraction and
  submission there and nowhere in the kernel. The block is removed from the
  `Final` the wrapper yields, so the kernel renders a reply without it.
* **Remediation loop (tasks 10, 11, 13).** A separate worker loop composed in
  `apps/worker/src/curie_worker/run.py` beside the publication loop, with its own
  injected reply sink and `ApprovalCardStore`, never entering the consumer or the
  stream path, taking no thread lock and writing no markers.
* **Read executions (task 7).** Through the executor loop of executor task 11 and
  its sandbox substrate rules (AE-5), not a new claim path.

Shared files with other owners, each touched additively and reviewed by that
owner: `apps/worker/src/curie_worker/run.py` (integration owner only);
`apps/worker/src/curie_worker/runner_client.py` (the `read` phase);
`runner/src/curie_runner/server.py` (the `read` phase in the executor app);
`apps/api/src/curie_api/routers/approvals.py` and
`apps/api/src/curie_api/crud/approvals.py` (the `remediation` purpose and its
resume exclusion); `apps/api/src/curie_api/routers/actions.py` (authority-aware
undo); `apps/api/src/curie_api/routers/action_executions.py` (samples route,
forward dispatch); `apps/api/src/curie_api/routers/hooks.py` and
`packages/protected-hooks` (task 5, #3603 owner);
`packages/telemetry/src/curie_telemetry/metrics.py` (telemetry owner); the chart
and compose files (chart owner).

No task modifies `packages/aci-protocol` or `packages/plugin-format` (AR-27).

## Blockers and maintainer rulings

* **Executor tasks 11 and 12.** Read executions, forward execution and every
  positive runtime row wait for them; executor task 10's digest recording gates
  reversible actions.
* **The protected worker (#3603, protected hooks plan task 4).** No protected
  turn runs on `next`, so no nomination can be produced end to end. Task 14 is
  blocked on it; earlier tasks proceed with inserted bindings.
* **Protected envelope schema (task 5).** Adding `remediation_generation` to the
  intent and envelope is an internal schema change reviewed by the #3603 owner;
  it is not a frozen ACI change.
* **Maintainer rulings, 2026-10-07.** Settled and carried in the tasks: the
  incident is a per-target window of one hour after verification that a policy
  may only lengthen, never derived from the alert body (task 9); policy writes
  and breaker closes require an ADR 0106 operator principal recorded as the actor
  (tasks 3, 4 and 9); tuning stops at the nomination and the approval card, and an
  approved tuning request ends refused with no write (task 16); one JSON pointer
  and a closed comparator set is the predicate grammar (tasks 2, 7 and 11).
* **Tuning execution.** Automated rule-owner change requests need a separate
  Draft ADR; no task here builds an execution path for them.
* **Frozen contracts.** None needed. AR-27 lists the reviewer requests that would
  become a frozen contract blocker.

## Completion

Close nothing until task 18 passes on the final artifacts with every required
tier's evidence or a recorded waiver that keeps the item open. Then:

* **#4063** closes on tasks 3, 4 and 5 and the campaign's policy rows (write,
  CAS, hook key refused, generation at admission).
* **#4064** closes on tasks 6 and 14 with live provider and external integration
  evidence of a real protected turn's block captured and stripped.
* **#4065** closes on task 9 (with task 7's precondition reads) and the campaign's
  admission rows.
* **#4066** closes on task 15 plus one real qualification record written from
  observed evidence in a disposable installation, anonymized.
* **#4067** closes under the executor plan's completion rule; this plan's forward
  rows are its AE-19 evidence.
* **#4068** closes on task 8 and the verification outcome written by task 11.
* **#4069** closes on task 10.
* **#4070** closes on tasks 11 and 12.
* **#4071** closes on task 9's limit, breaker, disarm and kill switch rows.
* **#4072** closes on task 13.
* **#4073** closes on task 8's actor fields, including the operator principal on
  each policy generation.
* **#4144** closes on task 16 and the campaign's recurrence prevention and tuning
  rows (one request per series, bound card, approval refused with no write,
  rejection leaving the rule untouched); automated execution of a tuning change
  is tracked by the separate Draft ADR the maintainer asked for, not by #4144's
  closure.
* **#4074** closes when every issue above is closed. **#3603** is not closed by
  this work.

Review the complete outgoing diff for the public information boundary before
each commit, push and pull request update; live evidence uses placeholder names
and identifiers only.

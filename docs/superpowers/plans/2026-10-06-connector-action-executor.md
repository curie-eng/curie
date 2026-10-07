# Connector action executor implementation plan

This plan realizes the [connector action executor contract](../specs/2026-10-06-connector-action-executor.md)
for [#4067](https://github.com/curie-eng/curie/issues/4067) under epic
[#4074](https://github.com/curie-eng/curie/issues/4074). Criterion names below
abbreviate `ACTION-EXECUTOR-*` as `AE-*`. The feature work targets `next`, where
ADRs 0121, 0124 and 0203 are Accepted as of `fc1527985` (#4082).

## Commit and ownership order

Commit the specification and this plan first, as a documentation change with no
runtime behavior. For every later task, a test author reads the specification
and writes behavioral tests without implementing anything, runs them against
the unchanged code, records the observed product failure, and commits the
failing tests alone. A separate implementer follows in a later commit. Every
new test and implementation unit cites its criterion with an `@spec` marker.
Specification, security and quality reviews follow each task; fix findings
before starting a task that depends on it. Independent tasks proceed in
parallel worktrees with their own disposable Postgres and Valkey.

## Tasks

| Task | Criteria | Owner (directory) | Depends on | Positive and negative proof |
| --- | --- | --- | --- | --- |
| 0. Commit spec and plan | none | docs | none | The docs check (`scripts/check-docs.sh`, run with bash) passes. |
| 1. Measure the open facts | AE-5, AE-6, AE-12, AE-16 | integration owner | 0 | Recorded observations, anonymized, for M1 to M6 below. A fact that contradicts the spec stops the dependent task and returns to the spec. |
| 2. Shared vectors and the parity entry | AE-7, AE-9, AE-10, AE-15, AE-24 | `tests/vectors` and AGENTS.md (integration owner) | 0 | Vectors for canonical arguments, sealed replies, the restore and `observe_version` calls, and `runner-execute` (every phase, refusal codes, executor status body, mode variable); failing reader tests on each consuming side; the AGENTS.md parity seam entry below. |
| 3. Ledger, execution and capability persistence | AE-2, AE-11, AE-13 (schema), AE-16 (custody derivation) | `apps/api` | 0 | Migration round trip on real Postgres; `undoable` false for each missing ingredient, including capability and custody; partial index refuses a second live restore; legacy cleartext rows not undoable. |
| 4. Ruling, execution, observation and probe routes | AE-1, AE-3, AE-13 (route), AE-15 (observation record), AE-18, AE-20 | `apps/api` (regenerated `apps/ui` types) | 3 | Authorized undo creates one execution and no `undone_at`; every ruling refusal writes an audit row and no execution; probe route accepts only `{agent_id, connector, digest}`; fenced idempotent transitions; OpenAPI drift green. |
| 5. Chart and compose configuration | AE-1, AE-12, AE-14 | `charts/curie`, compose files, `apps/api` and `apps/worker` settings | 0 | One chart value renders `CURIE_ACTION_EXECUTOR_ENABLED` into API and worker (render assert they match), compose likewise; worker Role gains `get` on Deployments only; default off; runtime exec assert. |
| 6. Key custody and restore gating | AE-8 (API half), AE-16 | `apps/api`, `charts/curie`, `apps/worker` (`binding.py`) | 1 (M1), 3 | Reserved names refused at intake in every non `SecretRef` form; chart render fails with them under `agentSandbox.connectorSecrets`; worker withholds them; `restore` added to a connector's gated set only once its probe records the `restore` and `observe_version` pair, and never for a lone `restore`; cluster exec check that the key is absent from runner env. |
| 7. Reference reversible connector | AE-9, AE-13, AE-15, AE-16 | test fixture connector (integration owner) | 2 | Sealed reply, `observe_version`, optional compare-and-swap, missing key refusal and retained key restore, plus a non-conforming `restore` variant and a variant that ignores `expected_version`, each against a disposable cluster resource. |
| 8. Envelope crosses the frame verbatim | AE-9 (runner reader), AE-10 | `runner` (`redact.py`) | 2 | Pattern-matching ciphertext crosses byte-identical and the frame is not `redacted`; a held literal or a token-shaped `kid` withholds all replay inputs; a scrubbed `summary` beside a valid envelope leaves the envelope unaltered and the frame `redacted`; the runner reads the sealed-reply vector; existing redaction suites pass. |
| 9. Runner executor mode and route | AE-4, AE-6, AE-8 (runner half), AE-24 | `runner` | 1 (M2), 2, 7 | Phases `list`, `observe`, `call` against the reference connector; each preflight and ordering refusal makes no write call; `/v1/event` refused in executor mode; `restore` absent from a live model catalogue when paired with `observe_version`, unchanged when alone. |
| 10. Worker recording | AE-9, AE-11, AE-12 | `apps/worker` | 1 (M5), 2, 3, 5 | Envelope parsing per vector; recorder wrapper attributes a digest only inside a completed rollout, within its time bound, and never fails a turn. |
| 11. Worker executor loop | AE-4, AE-5, AE-7, AE-13 (probe trigger), AE-14, AE-15, AE-17, AE-21, AE-22 | `apps/worker` | 1 (M3, M4, M6), 4, 5, 6, 7, 9, 10 | Probe, observe, compare, dispatch and report through real Postgres, Valkey, sandbox, proxy and connector; conflict refused with no write call; digest and kill switch refusals before dispatch; fault injection at every boundary yields at most one write call. |
| 12. Forward execution seam | AE-19 | `apps/api` with `apps/worker` | 3, 4, 11; authority sources #4065 and #4069 for end to end | One ledger row per forward execution carrying the digest; replay creates nothing; argument drift refused; undo refused until #4068 provides authority aware authorization. |
| 13. Operator surface | AE-18 (receipt), AE-23 | `cli` | 4 | New `actions` group under `local` and `cluster`; one JSON object per verb under `--json`, refusals included; CLI manifest regenerated; CLI bundle check refuses a plain reserved sealing name. |
| 14. Documentation | AE-8, AE-24, AE-25 | docs, `ARCHITECTURE.md`, ACI producer interface (seam owner review) | 9, 11 | Ledger and executor data path described; `/v1/execute` listed as optional with the route count updated; connector author guide for the paired `restore` and `observe_version` verbs and sealing; release note that a lone `restore` is unchanged. |
| 15. Complete campaign | all | integration owner | 1 to 14 | Restore round trip, conflict refusal, digest refusal, key rotation, crash recovery, receipts, and forward execution with a test authority, on the final artifacts. |

### Measurements in task 1

Each is run in a disposable environment, recorded with the exact command,
candidate commit and observed output, and published only with placeholder
identifiers.

* **M1.** Trace a plain named connector secret's value on the local and cluster
  tiers, including the `agentSandbox.connectorSecrets` chart path: which of the
  API, the agent row, the worker and the sandbox it reaches.
* **M2.** Boot the runner image with `CURIE_RUNNER_MODE=execute`, no model
  credential and no state tokens, and confirm the branch point in
  `runner/src/curie_runner/__main__.py::_serve` is reached before credential
  resolution with no other boot step requiring those values.
* **M3.** Claim a sandbox with the AE-5 env for an agent with connector secrets
  and compare its pool, labels and NetworkPolicy reach with an ordinary turn's
  sandbox; confirm `SandboxSubstrate.claim` has no thread lock precondition for a
  unique `action-exec:` key.
* **M4.** At a real caller proxy with `restore` in the gated set, confirm refusal
  without a grant, acceptance with one matching grant, and refusal on replay.
* **M5.** Read a connector Deployment by name with the new `get` grant across a
  rollout and confirm the AE-12 bracket rule distinguishes a completed rollout
  from one in progress, and measure read latency against the two second bound.
  Observed 2026-10-06 on a disposable single-node cluster at `346e21ba3`:
  generation and availability alone called every one of 214 in-progress reads
  completed, and updated plus available replicas still called 69 of them
  completed while an old pod served; adding `status.replicas == spec.replicas`
  misclassified none, so ACTION-EXECUTOR-12 now names all four conditions.
  Over 727 reads p50 was 9.8 ms, p95 12.4 ms and max 20.0 ms against the two
  second bound. Enabling the executor added only `get` on Deployments in the
  release namespace; the reconciler's `list` already returns the same objects.
* **M6.** Confirm that filtering `action-exec:` routes out of
  `SandboxSubstrate.pressure_candidates` keeps them out of
  `apps/worker/src/curie_worker/kernel/capacity.py::_reclaim_idle_route`, and
  observe the substrate's quota refusal for an executor claim.

### Parity seam entry added in task 2

To be appended to the AGENTS.md registry in the same change as the vector:

> worker vs runner executor route: the worker's `execute` client and boot env
> mode variable (`apps/worker`) and the runner's `/v1/execute` route and
> executor mode status body (`runner`) ship in different images and cannot share
> code at runtime, so the request and response of every phase, the route's
> refusal codes, the status body and the mode variable are frozen together.
> [vector: `runner-execute` under `tests/vectors`]
>
> runner vs worker sealed envelope: the runner's redactor
> (`runner/src/curie_runner/redact.py`) decides whether an envelope crosses
> unaltered and the worker's `_snapshot`
> (`apps/worker/src/curie_worker/actions.py`) decides whether it is recorded;
> both validate the same envelope grammar in different images, so they read one
> sealed-reply vector. [vector: `sealed-snapshot-reply` under `tests/vectors`]

## End to end tiers

Every behavior-bearing task classifies all seven tiers. `R` is required; `n/a`
cites the reason code under the table. Tasks 0 and 14 change documentation
only, task 1 records observations, and task 2 adds vectors and failing tests;
none of them changes runtime behavior, so they carry no tier and run the
repository validators for their artifacts.

| Task | skill | local | local-release | cluster | live provider | external integration | factory |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 3 | n/a (f) | R | n/a (a) | n/a (b) | n/a (c) | n/a (d) | n/a (e) |
| 4 | n/a (f) | R | n/a (a) | n/a (b) | n/a (c) | n/a (d) | n/a (e) |
| 5 | n/a (f) | R | R | R | n/a (c) | n/a (d) | n/a (e) |
| 6 | n/a (f) | R | n/a (a) | R | n/a (c) | n/a (d) | n/a (e) |
| 7 | n/a (g) | n/a (h) | n/a (a) | R | n/a (c) | n/a (d) | n/a (e) |
| 8 | R | R | n/a (a) | n/a (b) | n/a (c) | n/a (d) | n/a (e) |
| 9 | R | R | n/a (a) | R | R | R | n/a (e) |
| 10 | n/a (f) | R | n/a (a) | R | n/a (c) | n/a (d) | n/a (e) |
| 11 | n/a (f) | R | n/a (a) | R | n/a (c) | n/a (d) | n/a (e) |
| 12 | n/a (f) | R | n/a (a) | R | n/a (c) | n/a (d) | n/a (e) |
| 13 | n/a (f) | R | n/a (a) | R | n/a (c) | n/a (d) | n/a (e) |
| 15 | R | R | R | R | R | R | n/a (e) |

Reasons:

* (a) No released binary, image identity, install path, version pin or release
  compose change in this task. Task 5 changes the release compose, so it carries
  the row.
* (b) No chart template, RBAC, securityContext, NetworkPolicy, sandbox claim or
  init container change; the behavior is in the API or in frame content, both
  exercised by the local tier.
* (c) No model routing, credential resolution, provider auth, token accounting
  or MCP catalog projection change. The executor sandbox holds no model
  credential, and the catalogue change is confined to task 9.
* (d) No Slack, webhook, connector OAuth or third-party API shape change, and
  nothing on the MCP, workspace or coding tool path set.
* (e) No factory runtime, CI, progress, publication or work item path.
* (f) No plugin packaging, runner turn loop, ACI event or skill eval change.
* (g) The fixture is a hosted connector, which the skill tier does not host.
* (h) The local tier refuses `SecretRef` (`cli/src/connector_build.rs`), so a
  sealed connector cannot run there by design (AE-16).

What the local tier proves for tasks 10 to 12: the local tier has no reconciled
connector Deployment and no caller proxy, so it records a null digest and every
grant-bound execution refuses (`tool_not_grant_bound` or
`connector_digest_unavailable`). Those refusals, observed end to end, are the
local evidence; positive restore and forward evidence is cluster only.

Task 9 reaches the runner MCP catalog projection path set by hiding `restore`
(AE-8), so live provider and external integration are required with live
evidence. Until the authority sources land, AE-19 has no producer to drive end
to end; its positive rows in tasks 12 and 15 proceed only under a visible
`Discovery waiver` naming
[#4065](https://github.com/curie-eng/curie/issues/4065) and
[#4069](https://github.com/curie-eng/curie/issues/4069), which records the
missing proof and does not mark the criterion passed.

Commands: `CURIE_E2E_TIERS=<tier> curie dev e2e-ladder` per required tier,
`curie dev chart-runtime-e2e` for the task 5 and task 6 runtime asserts, and the
live rungs with `CURIE_E2E_LIVE=1` for tasks 9 and 15. Each acceptance criterion
records a positive observation and a falsifiable negative from the
specification.

## Core files owned by others

The worker files named off-limits for this work are the kernel, the consumer,
the thread lock and the markers. On `next` the kernel is a package,
`apps/worker/src/curie_worker/kernel/`, and the whole package is treated as
off-limits, alongside `apps/worker/src/curie_worker/consumer.py`,
`apps/worker/src/curie_worker/threadlock.py` and
`apps/worker/src/curie_worker/markers.py`. No task edits them. The wrappers:

* **Digest attribution (task 10).** A recorder wrapper in a new worker module
  implements `apps/worker/src/curie_worker/actions.py::ActionRecorder` around
  `ActionClient`, composed in `apps/worker/src/curie_worker/run.py`; the kernel's
  `apps/worker/src/curie_worker/kernel/attempt.py::_record_action` is unchanged.
  Its reads are bounded (AE-12) because that call is not best effort.
* **Executor loop (task 11).** A separate loop launched beside the connector
  reconcile loop in `run.py`. It never enters the consumer or stream path, takes
  no thread lock (unique keys, confirmed by M3), writes no turn markers, and
  drives `SandboxSubstrate.claim` and `SandboxSubstrate.release` directly.
* **Pressure reclamation (task 11).** Executor routes are filtered out in
  `SandboxSubstrate.pressure_candidates` (substrate, not kernel), so the
  kernel's reclamation never sees them (M6).
* **Grant minting (task 11).** The loop calls
  `apps/worker/src/curie_worker/connector_grant.py::mint` itself rather than the
  kernel's private `_connector_tool_grant`; byte equality with the proxy is held
  by the task 2 vector.
* **No-retry semantics.** The execution state machine (AE-17) carries the rule
  the kernel's markers carry for turns; nothing is added to the markers.

Shared files with other owners, each touched additively and reviewed by that
owner: `apps/worker/src/curie_worker/run.py` (two compositions, integration
owner only); `apps/worker/src/curie_worker/runner_client.py` (an `execute`
call); `apps/worker/src/curie_worker/binding.py` (withheld reserved names);
`apps/worker/src/curie_worker/connector_loop.py` (probe request hook; the
reconcile decision is unchanged); `apps/worker/src/curie_worker/sandbox/substrate.py`
(the pressure filter); `runner/src/curie_runner/__main__.py` (one early branch in
`_serve`); `runner/src/curie_runner/server.py` (`create_executor_app` and the
gated path); `runner/src/curie_runner/adapter.py` (disallowed `restore` names);
`runner/src/curie_runner/mcp_tool_capability.py` (public standalone client
helper); `runner/src/curie_runner/redact.py` (AE-10);
`apps/worker/tests/binding/test_boot_env_single_declaration.py` (mode variable
on the non-boot allowlist); `charts/curie/templates/worker.yaml` and
`charts/curie/templates/agent-connector-secrets.yaml` (chart owner);
`apps/api/src/curie_api/routers/actions.py`, `apps/api/src/curie_api/bundles.py`
and `apps/api/src/curie_api/routers/agents.py` (API owner).

No task modifies `packages/aci-protocol` or `packages/plugin-format` (AE-25).

## Blockers and open rulings

* **Accepted texts on `next`.** Resolved by #4082 (`fc1527985`).
* **Ledger authority fields.** Task 3 adds `authority_kind` and
  `authority_ref`; [#4068](https://github.com/curie-eng/curie/issues/4068) adopts
  them. Forward undo stays refused until #4068 provides authority aware
  authorization. Forward end to end proof waits for #4065 and #4069.
* **Realization choices.** The read verb is `observe_version`, and the capability
  rule applies to the `restore` and `observe_version` pair, so existing tools
  named `restore` keep today's behavior unless their connector also advertises
  `observe_version` (AE-8). The spec's decision authority paragraph states how
  both refine what the ADRs leave open; no new ADR is needed.
* **Frozen contracts.** None needed. AE-25 lists the reviewer requests that
  would turn into a frozen contract blocker.

## Completion

Do not close #4067, #1867 or #1873 until task 15 passes on the final artifacts
with every required tier's evidence. #1873 closes on the sealed snapshot path
(tasks 6, 8, 10 and 15). ADR 0121 decisions 3 and 4 are realized for the CLI
receipt; chat receipts remain with #4072, which the #1867 closing comment names.
Review the complete outgoing diff for the public information boundary before
each commit, push and pull request update; live evidence uses placeholder names
and identifiers only.

## Review dispositions

Dispositions for the findings review of the first draft.

* **C1** Fixed: AE-15 puts the version comparison on the platform before the
  restore, through the pinned connector's read verb `observe_version`;
  compare-and-swap on `expected_version` is optional defense in depth; residual
  trust is named; the ruling 5 evidence shape for #4066 is stated. No new ADR.
* **M1** Fixed: the readOnlyHint passage now says the corrected ADR 0121
  decision 5 matches the code; the plan's discrepancy item is gone.
* **M2** Fixed: AE-10 realizes ADR 0124 decision 7 for the envelope (verbatim or
  withheld, never altered) while keeping the held secret guarantee; task 8 owns
  it. No conflicting invariant blocks it.
* **M3** Fixed: AE-1 names the probe as the third producer with its own closed
  route and `authority_kind`; AE-6 defines the `list` phase.
* **M4** Fixed: tasks 3 and 4 own the capability schema, probe route and
  custody computation; task 11 depends on them.
* **M5** Fixed: AE-16 refuses the reserved names in the chart template; task 6
  owns it with a render assertion and a cluster exec check.
* **M6** Fixed: key custody is an `undoable` ingredient with a negative
  case for a non-reserved key; the residual limitation is stated.
* **M7** Fixed: AE-18 defines the first release receipt as the CLI output on the
  only asking channel; AE-14 no longer claims a chat receipt; completion keeps
  chat receipts with #4072.
* **M8** Fixed: AE-1 renders one chart value into API and worker with a render
  assertion; task 5 owns it.
* **M9** Fixed: task 11 depends on 6 and 7; task 9 depends on 7; task 5 grants
  Deployment `get` before tasks 10 and 11.
* **M10** Fixed: no break. AE-8 makes the `restore` and
  `observe_version` pair the deploy-time capability rule; a lone `restore` stays
  an ordinary tool and its connector restores nothing. Acceptance cases cover
  both sides.
* **m1** Fixed: AE-24 and the parity entry above; the vector covers the mode
  variable; the interface count and conformance coverage are stated.
* **m2** Fixed: AE-6 specifies `create_executor_app`, `_GATED_PATHS` and the
  executor status body.
* **m3** Fixed: the telemetry route label is cited.
* **m4** Fixed: the grant variable now cites `_GRANT_ENV`.
* **m5** Fixed: a conflict is a pre-dispatch `refused` that releases the action;
  a later ruling is refused again while the version differs; tested in AE-15.
* **m6** Fixed: `refused_restore_in_flight` and `refused_unversioned` added;
  `executor_disabled` writes an audit row like every refusal.
* **m7** Fixed: restore idempotency key, `requested_by` and `refused_no_agent`
  defined in AE-2, AE-3 and AE-18.
* **m8** Fixed: forward rows carry `connector`, `connector_digest` and
  `call_id`.
* **m9** Fixed: task 2 carries no tier; local evidence for tasks 10 to 12 is
  the observed refusal.
* **m10** Fixed: task 12 depends on task 3 for the fields and on #4065 and
  #4069 for authority.
* **m11** Fixed: AE-12 bounds each read at two seconds.
* **m12** Fixed: AE-13 states and tests that earlier actions become undoable
  when the probe lands.
* **m13** Fixed: AE-23 is a new verb group with its module placement.
* **m14** Fixed: AE-23 mirrors the reserved name refusal in the CLI bundle
  check.
* **m15** Fixed: AE-5 excludes executor routes from pressure candidates and
  maps quota refusal; M6 verifies it.

### Round 2

* **R2-M1** Fixed with the contract-preserving option: a `redacted` frame records
  no snapshot (AE-9), matching the frozen consumer rule on `SideEffectFlag`; the
  runner still never alters the envelope, so ADR 0124 decision 7 holds (AE-10);
  AE-25's claim of no frozen contract change stays true.
* **R2-m1** Fixed: the spec's decision authority paragraph and AE-8 state the
  verb name and the paired capability rule as design, including that the pair
  refines ADR 0121 decision 5's rule and adds a third connector verb. Git
  records who decided.
* **R2-m2** Fixed: AE-13 brackets the probe's `list` with the completed
  rollout at digest check and records nothing otherwise.
* **R2-m3** Fixed: custody is computed from the in-force version at read and
  ruling time and is no longer stored on the capability row (AE-11, AE-13,
  AE-16), with a negative case for a version that drops the `SecretRef`.
* **R2-m4** Fixed: the runner redactor reads the sealed-reply vector (AE-9,
  task 8) and the pair joins the parity seam entry above.
* **R2-m5** Fixed: pattern rules apply to `kid`, and a match withholds the replay
  inputs (AE-10).
* **R2-m6** Fixed: AE-8 hides `restore` for any connector whose boot probe failed
  or was incomplete, with an acceptance case.

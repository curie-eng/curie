# 230. Cluster upgrade and rollback report outcomes from one lifecycle record checked against the cluster

Date: 2026-10-15

Status: Draft

A maintainer approved writing this record on 2026-10-15. That approval covers
drafting it for discussion, not its content. It authorizes no implementation
until it is published as `Accepted` under
[ADR 0085](0085-acceptance-not-implementation-authorizes-an-adr.md).

Every `path:line` citation below is pinned at commit `8b07d8d70` on `next`. Read
them with `git show 8b07d8d70:<path>`.

## Relationship to other ADRs

1. Builds on [ADR 0142](0142-database-compatibility-windows-and-a-single-upgrade-phase.md).
   Migrations still run in one phase and the schema window is still the
   compatibility contract. This ADR changes how the outcome of that phase is
   recorded and reported, not what it does.
2. Builds on [ADR 0209](0209-pause-authority-after-a-drain-is-a-lease-held-by-a-live-upgrade-step.md).
   Pause authority stays a renewed lease in Valkey held by a live upgrade step.
   The lifecycle record observes and records the drain; it does not hold or
   replace pause authority.
3. Amends Draft [ADR 0144](0144-the-upgrade-lifecycle-admits-before-it-mutates.md).
   ADR 0144 keeps each admission gate independent, and this ADR keeps that: the
   namespace, schema and drain gates still refuse on their own evidence. What it
   amends is ADR 0144's consequence that "an interrupted `cluster up` or
   `cluster rollback` is still an interrupted sequence of Helm calls". Under
   this ADR a rollback is a recorded lifecycle like an upgrade. ADR 0144
   decision 5 left a resumable checkpoint and post-upgrade proof open under
   #2301; `cluster upgrade` has since shipped both for upgrades only, and this
   ADR extends them to rollback and makes them the only source of the reported
   outcome. It does not take #2391's distributed execution lease, which ADR 0144
   rejected; ownership stays the compare-and-swap holder that exists today.
4. Does not touch Draft [ADR 0159](0159-a-canary-is-a-weighted-second-active-deployment-and-every-trace-carries-its-version.md).
   The `canary` phase here is the upgrade's post-apply smoke check, not the
   weighted second deployment of ADR 0159.
5. Consistent with Draft [ADR 0219](0219-a-deploy-binds-an-agents-runner-image-at-runtime-outside-the-helm-release.md).
   Runner bindings live outside the Helm release, so they are not part of the
   serving version this ADR defines.
6. Supersedes nothing.

## Context

A gauntlet campaign against v0.12.4 on a real k3s cluster, run on 2026-10-12,
found ten defects. Seven of them were version transitions that misreported their
outcome. They are #4461, #4462, #4495, #4496, #4497, #4498 and #4499.

1. #4461. The controller preflight hook failed a healthy upgrade. Its log
   signature matched another namespace's NetworkPolicy refusals, and a signature
   found only in the previous container ended the check after a restart that had
   already healed. Helm marked the revision failed and the CLI exited 1 after
   269 s while the release was healthy.
2. #4462. After that hook failure, Helm still listed the previous revision as
   `deployed`. The CLI compared Helm's deployed chart version with the
   known-good version, concluded the previous version was serving, and printed
   no warning, while the api, worker and ui Deployments already ran the target
   images.
3. #4495. Upgrades that failed at converge, canary, commit or drain_preflight
   printed `cluster upgrade failed` and carried `status: failed` in `--json`, but
   exited 0. Only an apply failure was turned into an error. The printed
   fail-forward command omitted `--context` and `--chart`. Four real upgrades in
   one round failed at converge and every one exited 0.
4. #4496. The convergence observer counted terminal pods left by kubelet
   evictions as replicas. After one eviction wave every later upgrade failed
   converge, and `cluster status` and `cluster up` reported an unconverged
   install whose Deployments were fully ready.
5. #4497. After a `cluster rollback` was killed mid-flight, Helm's newest row was
   `pending-rollback` with description `Rollback to 9` and the source revision
   was still `deployed`. A bare rollback treated that as a killed upgrade,
   selected the serving revision the operator was leaving, and would have exited
   0 reporting a rollback that changed nothing. Helm's description string was the
   only place the operator's intent survived, because `cluster rollback` writes
   no record.
6. #4498. A rollback refused on the schema window printed a bare error with no
   next step for a human, though the CLI had computed the fail-forward version.
   A dry run skipped the offline catalog check and exited 0 for a target the
   real run then refused.
7. #4499 (open). A rollback switched worker versions while a publication was in
   flight. The new worker refused the resources the old worker had applied, the
   publication dead-lettered as failed after 27.75 minutes, and the issue was
   told the pull request could not be opened while it was open. Neither the
   rollback nor anything after it read in-flight work.

Each was fixed or is being fixed where it was found. #4521 narrowed the hook
signature. #4523 made the live host snapshot Deployment images around a failed
apply. #4506 made every failed upgrade phase exit 1 and added `--context` and
`--chart` to the fail-forward. #4522 stopped counting terminal pods. #4525 made a
bare rollback resume an interrupted rollback toward the target parsed from Helm's
description. #4529 composed the next step into the rollback schema refusals and
ran the catalog check on a dry run. This ADR builds on that behavior as fixed.

The fixes share a cause they do not remove. The lifecycle infers state from
several sources, each read by its own rule:

1. Helm status strings and history rows. `inspect_version`
   (`cli/src/ops/upgrade.rs:3304`) takes the highest `deployed` revision.
   `current_revision` (`cli/src/ops/verbs.rs:1612`) does the same for rollback
   with its own fallback. `recover_pending_revision`
   (`cli/src/ops/upgrade.rs:3314`) reads the newest row's status, and
   `interrupted_rollback_target` (`cli/src/ops/verbs.rs:1834`) parses Helm's
   free-text description.
2. The checkpoint ConfigMap `{release}-upgrade-checkpoint`
   (`checkpoint_name`, `cli/src/ops/upgrade.rs:1996`), whose `record` key holds
   the upgrade's phases and known-good version, and whose
   `curietech.ai/upgrade-holder` annotation (`:2000`) names the invocation that
   owns it. Only upgrade writes it. `rollback` (`cli/src/ops/verbs.rs:2333`)
   takes no holder, writes no transition, runs a bare `helm rollback` without
   `--wait` (`:2540`) and reports `RolledBack` from Helm's exit status alone. So
   after a rollback, `cluster status` (`load_upgrade_status`,
   `cli/src/ops/upgrade.rs:4084`) still shows the last upgrade's phase and
   known-good version.
3. Raw pod and workload lists, read by the convergence observer
   (`observe_inner`, `cli/src/ops/convergence.rs:731`) with its own pod rule
   (`counted_pod`, `:471`), and by `cluster status` with another (`verbs.rs`, the
   #110 summary rule).
4. Per-phase results. `serving_previous` on the live host
   (`cli/src/ops/upgrade.rs:3928`) answers from an applied flag set by the image
   snapshot in `apply_target` (`:3818`), falling back to a Helm comparison. The
   commit phase stamps known-good from a separate post-condition read
   (`:1912`). `fail_forward_for` (`:1368`) picks the next step from a boolean
   computed upstream.
5. Controller and hook logs, read inside chart hooks, whose verdict reaches the
   CLI only as Helm's revision status.
6. In-flight work is not read at all by either verb. `live_drain_preflight`
   (`cli/src/ops/upgrade.rs:3701`) checks only that the worker Deployment exists.
   Only `cluster status` reads worker claim state (`worker_claims::select_cluster`,
   `cli/src/worker_claims.rs:331`).

When two of these sources disagree, whichever rule the reporting code happens to
consult wins. Each fix taught one rule about one disagreement, and the next round
found the adjacent one: #4462 fixed `serving_previous` for a failed apply, but
the commit phase, the status verb and rollback each still answer "what is
serving" their own way. #4497 fixed one Helm history shape by parsing a
description string, because nothing Curie owns remembered the rollback's target.

Three of the seven also got past unit tests whose fakes encode intended rather
than real behavior. The clearest is #4462: `FakeUpgradeHost`
(`cli/src/ops/upgrade.rs:408`) answers `serving_previous` from a `mixed_on_fail`
flag (`:663`) that models the post-apply failure correctly, while the live host
had no equivalent. A fake that returns the answer the code should compute cannot
catch the code computing it wrongly.

## Decision

`curie cluster upgrade` and `curie cluster rollback` share one durable lifecycle
record per release. Every outcome either command reports is derived from that
record and checked against observed cluster state before the command returns.

**Invariant.** A command never reports success, and never reports a version as
serving, unless the record and the observed state agree.

### 1. One record per release, shared by both verbs

1. The record lives in the existing ConfigMap `{release}-upgrade-checkpoint` in
   the release namespace, keyed `record`, with the existing holder annotation.
   Its JSON gains a schema version, an `operation` (verb, operation id, target,
   origin revision, holder, CLI version, start time) and an ordered transition
   log. Fields stay redacted as today: revision ids, versions, image
   references, counts and reasons; never values, Secrets or row contents.
2. Both verbs acquire the holder with the compare-and-swap `acquire_ownership`
   uses today before their first mutating call, and release it as
   `finish_owned_upgrade` does. A rollback while another holder is recorded is
   refused with the same `--take-over` hint an upgrade gets.
3. The record stays bounded. It keeps the current operation in full and a short
   summary of the last few terminal operations, so it stays far below the
   ConfigMap size limit.

### 2. Every phase writes its transition

1. Upgrade phases are plan, validate, drain_preflight, checkpoint, migrate,
   apply, converge, canary and commit. Rollback phases are rollback_plan,
   rollback_validate (the status filter and the ADR 0144 schema gate),
   rollback_drain_preflight, rollback_apply, rollback_converge and
   rollback_commit.
2. Each phase writes `started` before it acts and exactly one of `completed`,
   `failed` or `skipped` after, with the observation it acted on. A phase that
   errors out (a refusal, an observer error, a failed write) records `failed`
   with the reason, rather than leaving the operation `in_progress` as an `Err`
   raised with `?` does today.
3. Resume and take-over are transitions too. `resume` records which phase the
   operation continues from and why. `take_over` records the previous holder,
   the new holder and the phase the operation was in. The taker inherits the
   recorded operation: it resumes it or records it `abandoned` with the observed
   state, and it cannot start a different operation until the recorded one is
   terminal.
4. A rollback's target comes from the record. Parsing Helm's `Rollback to <N>`
   description (#4525) stays only as the fallback for a rollback the record does
   not hold, such as one started by a raw `helm rollback` or by an older CLI.

### 3. Observed state is one read with one set of rules

Before a command returns, and at each phase that needs it, one observation reads:

1. the Helm revision actually serving: the newest `deployed` row, its chart and
   app version, and any newer `pending-*` or `failed` row above it;
2. the release's workloads: for each Deployment, StatefulSet and DaemonSet, the
   images on ready pods, counting pods by the one rule `counted_pod` applies
   today (terminal pods left by evictions excluded, terminating pods counted);
3. in-flight work: the worker claim state and the runs and publications held,
   with the worker version holding each, from the read `cluster status` already
   performs.

`cluster upgrade`, `cluster rollback` and `cluster status` all use this
observation. No verb keeps its own pod or revision rule.

### 4. Outcome, exit code, serving version and next step are derived

One function maps the record and the observation to the report. Nothing else
prints an outcome.

1. **Succeeded, exit 0** only when the record's last transition is `commit` or
   `rollback_commit` for this operation, the serving Helm revision is the
   recorded target revision, every counted ready pod of the release runs the
   target's images with exact counts, and no in-flight run or publication is held
   by a worker version that is no longer running.
2. **Serving version.** The CLI prints a version as serving only when the Helm
   serving revision and the workload images name the same version. When they
   disagree it prints `mixed` with both, and when the observation could not be
   read it prints `unknown`. Neither is ever reported as success.
3. **Failed, exit 1** for every other terminal state, with the phase from the
   record and the serving version from the observation. #4462's case reads as
   "apply failed after the target manifests reached the cluster; target serving"
   and #4461's as "a post-upgrade hook failed; target serving and ready", both
   accurate and both nonzero.
4. **Interrupted, exit 1** when the record is `in_progress` and its holder is not
   this invocation.
5. **Next step** is one table over the record's terminal transition and the
   observation, replacing the boolean passed to `fail_forward_for`. It always
   carries `--release`, `--namespace`, `--context` and, for an upgrade, `--chart`
   (the #4506 rule). An interrupted operation's next step resumes the same verb
   toward the recorded target. Refusals compose the step into the message for
   human output (the #4529 rule).
6. `--json` carries the record's operation, the derived outcome, and the
   observation that justified it. The schema change is additive under
   [ADR 0101](0101-schema-compatibility-for-closed-schemas.md).
7. An observation that cannot be read fails closed: the outcome is not success,
   and the report says which read failed.

### 5. Tests drive the derivation with observations, not answers

The derivation is a pure function of a record and an observation. Its tests feed
it observations captured from real clusters (the upgrade matrix shards already
produce them) as well as hand-written ones. The fake host produces observations
and never answers derived questions such as `serving_previous`.

### 6. Not decided here

1. Whether a rollback quiesces the fleet the way the pre-upgrade drain hook does.
   `helm rollback` runs pre-rollback hooks, not pre-upgrade ones, so today a
   rollback rolls workers under in-flight work (#4499). This ADR requires a
   rollback to observe and record that work and forbids reporting success while
   any of it has no live owner. Draining for rollback is a separate decision.
2. The publication adoption rules in the worker, which #4499 changes.

## Realizing code paths

No code is proposed in this ADR. These are the paths an implementation would
change, pinned at `8b07d8d70`:

1. `cli/src/ops/upgrade.rs`: `UpgradeRecord` (`:211`), `run_lifecycle_inner`
   (`:1606`), `execute_phase` (`:1810`), `fail_forward_for` (`:1368`),
   `status_from_record` (`:681`), `acquire_ownership` (`:2581`),
   `persist_record` (`:3382`), `release_ownership` (`:3435`),
   `recover_pending_revision` (`:3314`), `serving_previous` on both hosts
   (`:663`, `:3928`), `apply_target` (`:3818`), `refuse_failed_upgrade`
   (`:4012`), `finish_owned_upgrade` (`:4046`) and `load_upgrade_status`
   (`:4084`), and `FakeUpgradeHost` (`:408`).
2. `cli/src/ops/verbs.rs`: `rollback` (`:2333`), `select_rollback_revision`
   (`:1741`), `interrupted_rollback_target` (`:1834`), `current_revision`
   (`:1612`), and the `cluster status` verb's use of the record.
3. `cli/src/ops/convergence.rs`: `observe_inner` (`:731`) and `counted_pod`
   (`:471`) as the single observation.
4. `cli/src/worker_claims.rs`: `select_cluster` (`:331`) as the in-flight read.
5. `cli/schema/cluster-upgrade.schema.json`,
   `cli/schema/cluster-rollback.schema.json` and
   `cli/schema/cluster-status.schema.json` for the additive report fields.
6. `cli/scripts/cluster-upgrade-matrix.sh` for captured observations and
   rollback shards.

## Consequences

1. A class closes rather than an edge. A report can be wrong only if the record
   or the observation is wrong, and both are single, named reads that can be
   tested against real clusters.
2. `cluster rollback` becomes slower and stricter. It now waits for convergence
   and reads in-flight work, and it refuses while another holder is recorded.
   Today it returns as soon as `helm rollback` exits.
3. `cluster status` after a rollback reports the rollback, not the last upgrade.
4. More outcomes are nonzero. A hook that fails while the target serves healthy
   now reports failed with the target serving, where an operator may have
   preferred success. That is the invariant: the record says the operation
   failed, so it is not success, but the report no longer misstates what serves.
5. The ConfigMap remains a Kubernetes object an operator with namespace write
   access can edit or delete. A deleted record makes the next command start a
   fresh operation from observation only, and its report says the record was
   absent.
6. A raw `helm rollback` or `helm upgrade` bypasses the record. The observation
   still catches the disagreement, so the next `curie` command reports `mixed`
   or a Helm revision the record does not hold instead of trusting either.
7. Mixing CLI versions within one operation is unsupported. Released CLIs read
   the existing record shape and ignore rollback transitions. Each transition
   names the CLI version that wrote it so a newer CLI can detect an older one's
   writes and refuse rather than guess.

## Alternatives considered

1. **Keep fixing each phase where a defect is found.** Rejected. That is what the
   campaign did, and seven of ten defects were the same class in adjacent places.
   Each fix added a rule to one reader and left the others. #4497's fix had to
   parse a Helm description string because no Curie-owned state held the
   rollback's intent.
2. **Make the Helm release the single source.** Rejected. Helm's status is the
   source that was wrong in #4461, #4462 and #4497: a revision marked `failed`
   whose manifests are serving, a previous revision still `deployed` after the
   target applied, and a `pending-rollback` row whose intent survives only as
   free text. Helm records no holder, no phase and no in-flight work, and its
   release Secret is capped at 1 MiB. Helm stays an input to
   the observation, never the verdict.
3. **A CRD for the record.** Rejected for now. It would give a typed,
   validated object and status subresource, but it needs cluster-scoped install
   permission, a CRD lifecycle Helm does not upgrade, and a migration path for
   every existing release. The ConfigMap already carries the holder and
   compare-and-swap semantics through `resourceVersion`. Revisit if the record
   needs server-side validation or watch-driven controllers.
4. **The Postgres datastore.** Rejected. The database is the thing the migrate
   phase changes and a rollback must stay compatible with, and the CLI reaches
   it only through the API pod. A rollback is most needed when the API is
   crashlooping (ADR 0144 open consequence 4), which is exactly when a record in
   Postgres could not be read or written.
5. **A record per verb.** Rejected. An upgrade's failure is resolved by a
   rollback, and a rollback's interruption is resolved by another rollback or an
   upgrade. Separate records make each verb guess at the other's state, which is
   #4497 and the stale `cluster status` after a rollback.
6. **On take-over, the taker starts a new operation and discards the old one.**
   Rejected. The taker would lose the target and phase that #4497 showed must
   survive a kill. Inheriting the operation and either resuming it or recording
   it abandoned keeps the intent, and the CAS on the holder keeps two invocations
   from both believing they own it.
7. **On take-over, the record has no owner and any invocation may write.**
   Rejected. Two concurrent writers would interleave transitions, and the report
   derived from the log would describe neither run.

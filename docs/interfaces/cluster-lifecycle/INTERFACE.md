---
seam: Cluster lifecycle admission
kind: SOFT
impls: 1 Helm and kubectl lifecycle coordinated by the Rust CLI with a worker pre-upgrade hook
grade: not separately graded
epics:
  - "#2301"
  - "#2010"
order: 24
---
# INTERFACE: Cluster lifecycle admission

> Part of the Curie swappable-seam catalog: see the [seam index](../../interfaces.md).
<!-- BEGIN GENERATED: header (curie dev docs-lint) -->
> **Kind:** SOFT &nbsp;·&nbsp; **Implementations today:** 1 Helm and kubectl lifecycle coordinated by the Rust CLI with a worker pre-upgrade hook &nbsp;·&nbsp; **Swap-readiness grade:** not separately graded
<!-- END GENERATED: header -->

**Kind legend:** CLEAN = a real `Protocol`/typed port class · SOFT = swap via env/URL/prefix/wire, no code interface · NONE = not built yet.

## The black line

Installing, upgrading and tearing down a Curie release is the one operation that
can strand every in-flight turn, every pending approval and the database at once.
The rule this seam holds is that the lifecycle **admits before it mutates**
(ADR-0144): the admission checks (configuration, credential pair, chart pin,
schema window and Helm timeout) all run before the first `helm upgrade`, and an
admission refusal leaves the release's workloads and schema untouched. The
guarantee is limited. Checkpoint ownership and any pending-revision recovery
happen before validation, and convergence and canary are checked after apply, so
a failure there fails forward with a recorded reason.

The seam is SOFT. There is no `Protocol` and no port class an alternative
installer could implement. `cli/src/ops/upgrade.rs` does declare an
`UpgradeDriver` trait, but it is a test-injection point inside the CLI (a fake
host for the phase machine and a live host that shells out), not a swap point.
The real boundary is a process boundary: the Rust CLI spawns `helm` and
`kubectl`, and the chart's hook Jobs run Python inside the worker and API images.
Anything that replaced this lifecycle (an operator, a GitOps controller, a raw
`helm upgrade`) would have to reproduce the chart contract below, because the
chart, not the CLI, carries the drain gate and the single migration phase.

A second lifecycle implementation is deliberately not built. The atlas's next
step is to harden recovery with release upgrade and interruption drills before
adding one.

## Current contract

1. **Explicit context, one cluster.** `curie cluster` takes `--context`, defaulting
   to the kubeconfig current-context. `cli/src/kube_context.rs` resolves the
   target once at dispatch, refuses a name not in the kubeconfig, and pins every
   child: a small kubeconfig that sets only `current-context` goes first in
   `KUBECONFIG`, and `HELM_KUBECONTEXT` is set to the same name, so an ambient
   value cannot point Helm and kubectl at different clusters.
2. **Resolve and pin the target chart.** `curie cluster upgrade --to <version>`
   takes a release chart, a local path or a chart reference. References are
   pinned with Helm's `--version`; a local chart must declare exactly the
   requested version or the run refuses before mutation. A release archive that
   is not yet downloaded is allowed only for a dry run.
3. **Nine durable phases.** `UpgradePhase` in `cli/src/ops/upgrade.rs` is plan,
   validate, drain_preflight, checkpoint, migrate, apply, converge, canary,
   commit, in that order. Each completed phase is persisted, so a rerun with the
   same `--to` resumes; a rerun with a different target while one is in progress
   refuses.
4. **One namespaced checkpoint with compare-and-set ownership.** The record lives
   in the ConfigMap `<release>-upgrade-checkpoint` in the release namespace.
   Writes are JSON patches that `test` the server-observed `resourceVersion`
   plus the `curietech.ai/upgrade-holder` annotation, and set the
   `curietech.ai/upgrade-action` annotation alongside it, with a fresh holder
   UUID per invocation. A live holder refuses a
   second operation; `--take-over <holder>` exists for a holder the operator has
   verified is gone, and the driver also checks for a running release hook Job.
5. **Pre-mutation admission.** Validate is computed once, before any phase runs,
   and a dry run reports the same refusal the real run would hit:
   1. Retained configuration is read from the serving revision's Helm values,
      migrated by `cli/src/config_migrate.rs`, and handed back to Helm as a JSON
      values file rather than `--set`, so booleans, numbers and strings keep
      their types and literal dotted keys stay single keys.
   2. The installed release must record a connector caller credential:
      `connectorCaller.existingSecret`, or both `connectorCaller.signingKey` and
      `connectorCaller.verifyKey`. An absent or partial pair refuses, including on
      dry run, same-version verification and resume. `curie cluster up` can
      generate a missing pair; upgrade never does, and its refusal prints the
      corrective `curie cluster up` command.
   3. Schema compatibility is read from the target chart itself.
      `charts/curie/templates/schema-compat.yaml` renders the packaged
      `charts/curie/files/schema-compat.json` (schema window plus the Alembic
      revision graph with each revision's kind), and `cli/src/schema_compat.rs`
      plans against the live database revision. Pending `contract` or
      `irreversible` revisions refuse unless `--forward-only` is passed, which
      merges `api.migrate.forwardOnly` into the overlay. The metadata is never a
      values override: the target binary decides compatibility.
   4. The Helm timeout is derived from the render. The pre-upgrade drain Job
      carries `curie.ai/minimum-helm-timeout-seconds`; the driver uses the larger
      of that and its default, and refuses if the drain Job is missing that
      annotation, is not a `pre-upgrade` hook, or renders more than once.
6. **Drain is the chart's, not the CLI's.** The drain_preflight phase only
   confirms the worker workload is reachable. The real gate is the
   `pre-upgrade` hook in `charts/curie/templates/worker-upgrade-drain.yaml`,
   which runs `curie_worker.upgrade_drain` once `helm upgrade` starts.
   `apps/worker/src/curie_worker/upgrade_drain.py::UpgradeDrainGate` writes an
   installation-scoped quiesce marker fenced by Helm revision (a higher-revision
   marker refuses the write), holds it as a short lease renewed by a heartbeat
   (`apps/worker/src/curie_worker/upgrade_drain.py::quiesce_lease_s`), and waits
   for every leased delivery to settle. A clean drain extends the marker to the
   roll hold; a refusal fails the hook, so Helm applies nothing. An attest hook
   fails closed if the drain Job was deleted rather than succeeding, and a
   `post-upgrade` hook releases the marker.
7. **Migrate once.** The migrate phase is a checkpoint boundary only. The one
   migrator is the `post-install,pre-upgrade` Job in
   `charts/curie/templates/schema-migrate.yaml`, ordered after the drain hook,
   which runs `apps/api/src/curie_api/schema_compat.py::upgrade_with_pause`. On an
   upgrade it renews the drain's pause marker through the renew-only
   `packages/upgrade-pause/src/curie_upgrade_pause/__init__.py::renew_pause` for
   its whole life and refuses to start Alembic once that authority is lost.
8. **Converge, canary, commit.** Helm success is not convergence. Converge checks
   target images, observed generations, ready replicas, zero unavailable
   replicas, hook health, the drain verdict from Helm's retained hook history,
   and the installed manifest (`cli/src/ops/convergence.rs`). A target-version
   canary must pass. Commit reads the release version back from Helm and records
   that observed value, never the requested string, as known-good.
9. **Same-version verification is recorded as skips.** When the target is already
   installed and known-good, drain_preflight, checkpoint, migrate and apply land
   in the record's `skipped` list, distinct from `completed`, and converge,
   canary and commit re-verify.
10. **Install and teardown.** `curie cluster up` (`cli/src/ops/up.rs`) does a full
    Helm upgrade rather than `--reuse-values`, re-supplying recorded runner values
    and credential references. It reuses an existing agent-sandbox controller
    only when it is Helm-owned by another release, or unowned, healthy and on an
    image compatible with the vendored one; anything else refuses. It reads the
    `gvisor` RuntimeClass before the first install and infers `off` only from a
    NotFound (ADR-0193). `curie cluster down` bounds owned namespace deletion to
    300 s, never clears finalizers, and on a remaining namespace returns a
    transient failure carrying the exact resume command.
11. **Controller preflight.** `charts/curie/templates/preflight-controller.yaml` is
    a read-only `post-install,post-upgrade,test` hook that fails the Helm
    operation unless the vendored controller reaches a serving state: current
    forbidden-NetworkPolicy logs reject first, then a stable leader (complete
    rollout, a Ready holder past the cache-sync bound, an advancing Lease renewal)
    passes, otherwise startup logs or successful reconcile metrics are required.

## Implementations today

One, in three processes:

1. **Coordinator:** the Rust CLI. `cli/src/ops/upgrade.rs` (phase machine,
   checkpoint, admission), `cli/src/ops/up.rs` (install and retained values),
   `cli/src/ops/verbs.rs` (teardown and shared Helm/kubectl verbs),
   `cli/src/ops/convergence.rs`, `cli/src/schema_compat.rs` and
   `cli/src/kube_context.rs`. `cli/src/schema_window.rs` with
   `cli/src/application_schema_windows.json` declares released applications'
   windows for rollback and for the source head of an upgrade plan.
2. **Drain hook:** `apps/worker/src/curie_worker/upgrade_drain.py`, with key
   derivation shared through `packages/upgrade-pause`.
3. **Migrate hook and serving check:** `apps/api/src/curie_api/schema_compat.py`,
   whose `apps/api/src/curie_api/schema_compat.py::assert_servable` refuses API
   startup against a live revision outside the image's window
   (`apps/api/src/curie_api/schema_compat.json`).

## Known leakage

1. **Helm and kubectl are the interface.** The CLI parses Helm history, values,
   metadata and rendered templates, and kubectl JSON. A Helm output change is a
   lifecycle change.
2. **The chart carries contract the CLI depends on.** The drain Job's component
   label, hook annotation and timeout annotation, and the schema-compat template
   path, are read by name. A chart that renamed them would be refused, not
   silently mis-run, but the coupling is by string.
3. **The checkpoint is a recovery owner, not a transaction.** It serializes
   operators and makes phases resumable. It cannot roll Helm and Kubernetes back
   together, and a failure after apply fails forward with a recorded reason.
4. **Worker drain state is a Valkey key the API also writes.** The migrate Job
   renews a worker-owned marker, so both images depend on `packages/upgrade-pause`
   staying in step.
5. **ADR-0209 is only partly built.** The shared renew-only module and
   schema-migrate renewal shipped. A clean drain still writes the full roll hold
   instead of a short hand-off lease, attest does not renew, and the planned
   `upgrade-drain-hold` hook does not exist, so a failure after a clean drain can
   still pause the fleet for up to the roll hold.
6. **Approval identity migrations are fenced inside the migrate Job.**
   `apps/api/src/curie_api/migration_fence.py::fence_identity_tables` bounds its
   lock wait on the assumption the worker is already drained. Approvals an
   upgrade strands have an opt-in, audited break-glass path in
   `apps/api/src/curie_api/routers/approval_recovery.py`, off by default.
7. **The target API's serving window is a second source of truth.** CLI admission
   plans from the target chart's packaged schema-compat metadata
   (`cli/src/ops/upgrade.rs`), while the API image independently checks the live
   revision against its own image metadata at startup
   (`packages/protected-hooks/src/curie_protected_hooks/schema_serving.py` and
   `apps/api/src/curie_api/schema_compat.py::assert_servable`). Nothing checks the
   two windows for equality. A chart whose metadata disagrees with its image may
   surface only as an API startup refusal after mutation, or not at all when both
   windows happen to accept the live revision.
8. **A raw `helm upgrade` bypasses admission.** The chart hooks still drain and
   migrate, but the checkpoint, credential and chart-pin checks live only in the
   CLI. Forward-only enforcement also runs in the migrate hook:
   `apps/api/src/curie_api/schema_compat.py::plan_upgrade` refuses pending
   `contract` or `irreversible` revisions unless `api.migrate.forwardOnly` is set,
   and `charts/curie/templates/schema-migrate.yaml` invokes it, so the hook still
   refuses those revisions under a raw Helm upgrade.

## Cross-links

1. **Related seam:** [relational-db](../relational-db/INTERFACE.md): the schema whose compatibility window and single migration phase this lifecycle admits.
2. **Related seam:** [approval](../approval/INTERFACE.md): pending approvals are the state identity migrations fence and the recovery router repairs.
3. **Related seam:** [queue-stream](../queue-stream/INTERFACE.md): the leased deliveries the drain gate waits on.
4. **Related seam:** [substrate](../substrate/INTERFACE.md): the agent-sandbox controller that `cluster up` reuses or installs and the controller preflight gates.
5. **Epic(s):** #2301 (one-command cluster upgrade with resume and proof); #2010 (a routine Helm upgrade must complete in-flight side-effecting turns rather than escalate them)
6. **Vision doc:** [architecture-vision.md](../../architecture-vision.md): cluster lifecycle is not one of the six swap-readiness Jobs; not separately graded
7. **ADR(s):** [ADR-0142](../../adr/0142-database-compatibility-windows-and-a-single-upgrade-phase.md): database compatibility is a release contract and migrations run in one upgrade phase; [ADR-0144](../../adr/0144-the-upgrade-lifecycle-admits-before-it-mutates.md) (Draft): admit before mutating, installation-scoped pause authority; [ADR-0193](../../adr/0193-cluster-up-reads-a-missing-runtimeclass-before-install.md): cluster up reads a missing RuntimeClass before install; [ADR-0209](../../adr/0209-pause-authority-after-a-drain-is-a-lease-held-by-a-live-upgrade-step.md): pause authority after a drain is a lease held by a live upgrade step

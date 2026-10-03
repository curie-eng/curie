---
seam: Cluster lifecycle
kind: SOFT
impls: 1 (Rust CLI over Helm and kubectl)
grade: not separately graded
vision_row: null
epics: ["#2301", "#3923"]
order: 24
---

# INTERFACE: Cluster lifecycle

> Part of the Curie swappable seam catalog. See the [seam index](../../interfaces.md).

<!-- BEGIN GENERATED: header (curie dev docs-lint) -->
> **Kind:** SOFT &nbsp;·&nbsp; **Implementations today:** 1 (Rust CLI over Helm and kubectl) &nbsp;·&nbsp; **Swap-readiness grade:** not separately graded
<!-- END GENERATED: header -->

**Kind legend:** CLEAN = a real `Protocol` or typed port class. SOFT = swap through config or wire without a code interface. NONE = not built yet.

## The black line

The operator CLI manages cluster installation, upgrade, rollback, status, and
teardown through Helm and `kubectl`. The Helm chart remains the source of
workload declarations; the CLI prepares values, applies admission checks,
executes commands, and reports observed outcomes (`cli/src/ops/mod.rs`). This
is a SOFT boundary through chart values and command wire, not a public
`ClusterLifecycle` trait or a generic orchestration adapter.

The upgrade path also owns a durable ConfigMap checkpoint. Its compare and
swap checks guard checkpoint ownership and persistence. They do not turn every
cluster mutation into one transaction.

## Current contract

The built operator entry points are dispatched from `cli/src/main.rs` into
`cli/src/ops/up.rs`, `cli/src/ops/upgrade.rs`, and `cli/src/ops/verbs.rs`:

1. **Install:** `curie cluster up` resolves inputs and retained values, validates
   provider and credential consistency, establishes the primary namespace's
   ownership before Helm, and invokes the chart through prepared command
   vectors. Recovery for a failed initial install distinguishes a release that
   never had a known good revision from an existing installation
   (`cli/src/ops/up.rs`).
2. **Upgrade:** `curie cluster upgrade` performs the ordered phases `plan`,
   `validate`, `drain_preflight`, `checkpoint`, `migrate`, `apply`, `converge`,
   `canary`, and `commit`. It persists progress for resumption of the same
   target and refuses a different target while a recorded upgrade is in
   progress (`cli/src/ops/upgrade.rs`).
3. **Rollback:** `curie cluster rollback` reads Helm history, selects an
   eligible deployed or superseded revision, establishes its application
   identity, and checks the target's database compatibility window against the
   live Alembic revision before invoking Helm. An explicit revision still
   passes the eligibility and schema checks (`cli/src/ops/verbs.rs`,
   `cli/src/schema_window.rs`, `cli/src/schema_compat.rs`).

The live upgrade acquires ownership of the ConfigMap named
`<release>-upgrade-checkpoint`. `CheckpointObservation.resource_version`
holds the server's `metadata.resourceVersion`. Acquisition tests that version
before adding the holder and action annotations; an existing holder causes a
refusal. Checkpoint persistence and ownership release both test the current
resource version and holder. After each successful write the driver validates
the returned record and adopts the newly observed version. An unreadable,
malformed, or conflicting response fails the operation rather than authorizing
an unconditional rewrite (`cli/src/ops/upgrade.rs`).

`drain_preflight` checks that the worker workload is reachable. The real drain
gate runs inside Helm's pre upgrade hook during `apply`
(`charts/curie/templates/worker-upgrade-drain.yaml`,
`apps/worker/src/curie_worker/upgrade_drain.py`). Likewise `migrate` is a
checkpoint boundary; the chart's migration hook owns database migration
execution (`charts/curie/templates/schema-migrate.yaml`). Convergence observes
the resulting workloads and retained hook outcomes
(`cli/src/ops/convergence.rs`). Commit requires exact convergence, a passing
canary, and a fresh release version observation matching the target.

Successful completion records the known good version. A rerun of that same
installed and known good version skips `drain_preflight`, `checkpoint`,
`migrate`, and `apply`, then verifies convergence and canary again. The
structured output carries phase failure and recovery information rather than
equating Helm completion with a verified upgrade (`cli/src/ops/upgrade.rs`).

## Implementations today

One production lifecycle implementation ships: the Rust CLI over the Curie
chart and the installed Helm and `kubectl` binaries. `OpsCommand` separates
command construction from execution and secret masking
(`cli/src/ops/command.rs`). The private `UpgradeDriver` trait has a live host
and `FakeUpgradeHost` for lifecycle tests (`cli/src/ops/upgrade.rs`); that test
driver is not a second production backend or a supported extension point.

The lifecycle has focused coverage in `cli/tests/cluster_upgrade.rs`,
`cli/tests/cluster_upgrade_matrix.rs`, `cli/tests/cluster_upgrade_live.rs`,
`cli/tests/cluster_rollback.rs`, `cli/tests/cluster_rollback_schema.rs`, and
`cli/tests/cluster_rollback_live_schema.rs`. These source references describe
the existing coverage, not a claim that every live tier was run for this
catalog entry.

## Known leakage

The boundary is tied to Helm release history, Kubernetes resource identity,
chart hooks, and the Curie schema window. Replacing Helm or Kubernetes requires
changing those contracts; changing chart values alone does not supply another
lifecycle implementation.

The checkpoint holder has no automatic expiry or lease renewal. A holder left
by a terminated process requires verification that the process stopped before
manual recovery. Resource version and holder comparisons fence writes to this
checkpoint; they do not establish a distributed execution lease over Helm,
`cluster up`, rollback, or direct operator commands
(`cli/src/ops/upgrade.rs`).

Admission is specific to each path. Namespace ownership precedes Helm on
install, schema admission precedes the rollback call, and the drain gate runs
inside Helm. There is no single admission point ahead of every mutating
command. A report that the previous version remains serving is an observation,
not an unconditional availability guarantee.

## Cross-links

1. **Related work:** #2301 implements resumable upgrades; #3923 catalogs the
   lifecycle boundary.
2. **Vision doc:** [architecture-vision.md](../../architecture-vision.md).
   Cluster lifecycle is not one of its six graded jobs.
3. **ADR:** [ADR 0144](../../adr/0144-the-upgrade-lifecycle-admits-before-it-mutates.md)
   documents the admission and installation scope decisions. Its status remains
   **Draft**, although the cited behavior is built. This catalog entry does not
   accept or amend the ADR.
4. **Related seam:** [Substrate](../substrate/INTERFACE.md) covers sandbox
   operations within an installation rather than the installation lifecycle.

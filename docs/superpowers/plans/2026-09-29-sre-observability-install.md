# SRE Observability Installer Portability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make #1912's one-command observability install select the correct log format on homogeneous supported clusters and name a Pending PVC failure, while documenting the manual Grafana Secret contract.

**Architecture:** Add a log-runtime selector over the Alloy DaemonSet's eligible nodes, separate from the Ready/schedulable capacity preflight. Keep the checked-in Alloy values as the CRI default, and render its Docker variant only in the embedded workspace. Diagnose Helm and Tempo rollout timeouts from bounded, read-only PVC and Event data. The independent Alloy/Prometheus zero-log alert has its own plan and gate.

**Tech Stack:** Rust CLI, Kubernetes JSON through `kubectl`, embedded Helm values, Helm template and CLI tests.

**Spec:** `docs/superpowers/specs/2026-09-29-sre-observability-portability-design.md`

## Global Constraints

- Current `main` already omits `storageClassName` from all four PVC values; do not reintroduce `local-path` or choose a StorageClass for the operator.
- `containerd://` and `cri-o://` select CRI; `docker://` selects Docker. Missing, unknown, and mixed runtimes across every node eligible for Alloy (including cordoned and NotReady nodes) fail before mutation, naming affected nodes. Capacity preflight retains its separate Ready/schedulable rule.
- `--dry-run` remains offline and explicitly states that live runtime selection happens on execution.
- Keep the checked-in `alloy-values.yaml` on CRI and render Docker with `dockercontainers: true` and `stage.docker { }`; no duplicated full values file.
- Present Helm pending-upgrade Secret cleanup only after its state is verified, and never as a PVC remedy. PVC diagnosis is read-only, wall-clock bounded, and additive; never delete a PVC.
- Never print a Grafana admin password or real cluster identifiers in tracked docs/tests.
- Required tiers: local (CLI output/error), local-release (install path), cluster (Helm/Kubernetes), external-integration (real Alloy and Kubernetes). Skill/live-provider are not reached by this installer slice.

## Review Focus

1. A mixed ready-node runtime must fail before any Helm or Secret mutation; Task 1 tests both orders.
2. An unsupported runtime must not silently fall back to CRI; Task 1 tests a named unknown runtime.
3. A cordoned or NotReady node with a different runtime must prevent a wrong DaemonSet parser; Task 1 tests both.
4. Full-install `--dry-run` must not call Kubernetes; Task 1 covers all dry-run entry points.
5. A timeout with no readable PVC Events must retain the original command error, not claim storage was the cause; Task 2 tests the fallback.
6. A Warning Event for a different PVC or previous UID must not be attributed to the Pending PVC; Task 2 tests the object identity match.
7. A Pending PVC must not trigger advice to delete a Helm Secret as if it fixes storage; Task 2 tests the user-facing message.

---

### Task 1: Select and render the node log format

**Files:**
- Modify: `cli/src/examples.rs` (`NodeStatus`, both `EmbeddedWorkspace::create` paths, shared file writer, three live install paths, and nearby tests)
- Test: `cli/src/examples.rs` unit tests for node selection and value rendering
- Test: `cli/tests/example_sre_bot_install.rs` released-command fixture for no-mutation, dry-run, and passed values
- Modify: `examples/sre-bot/README.md` (manual Docker edits and the `grafana-admin` key contract)

**Interfaces:**
- Produces: `enum LogRuntime { Cri, Docker }`, `fn select_log_runtime(nodes: &[Node]) -> Result<LogRuntime>`, `async fn preflight_log_runtime() -> Result<LogRuntime>`, and `fn render_alloy_values(contents: &[u8], runtime: LogRuntime) -> Result<Vec<u8>>` in `cli/src/examples.rs`.
- `EmbeddedWorkspace::create_observability(namespace: &str, runtime: LogRuntime)` and full-install `EmbeddedWorkspace::create(..., runtime: LogRuntime)` pass through `write_observability_files(..., runtime)`; only Alloy is runtime-rendered and other observability files retain namespace rewriting.

- [ ] **Step 1: Write failing tests** for uniform `containerd://`, `cri-o://`, and `docker://`; mixed and unknown nodes; cordoned/NotReady mixed-runtime refusal; exact rendered `dockercontainers` and parser stage for both variants. In the binary fixture, verify all three live paths refuse before mutation, full install passes the Docker-rendered values, and every `--dry-run` makes no kubectl call. Add `log_runtime_renders_chart_mount_and_stage`, which feeds each `EmbeddedWorkspace` variant into `helm template` and checks the rendered ConfigMap and Docker hostPath. Cite the Kubernetes Node API and DaemonSet scheduling docs in the provider-shape test comment.
- [ ] **Step 2: Run `cd cli && cargo test examples::tests::log_runtime -- --nocapture`; confirm failure from missing selector/render behavior.**
- [ ] **Step 3: Implement the selector and renderer** with the interfaces above. `preflight_log_runtime` uses the existing `read_kubernetes_json` but covers all Alloy-eligible nodes, not the capacity preflight's filter. Move full-install `preflight_capacity` after its dry-run return. Wire all three live install paths before their first mutation; dry-run plans remain offline and mention selection.
- [ ] **Step 4: Run the focused test command; `log_runtime_renders_chart_mount_and_stage` must confirm `stage.docker` and the `/var/lib/docker/containers` mount, while the CRI render retains `stage.cri` and no Docker mount.**
- [ ] **Step 5: Document the manual two-line Docker changes and Secret keys** without a password-bearing shell command; run `bash scripts/check-docs.sh` after committing docs. <!-- doclint:ignore-line -->
- [ ] **Step 6: Commit Task 1** after `cargo fmt --check`, focused tests, and diff review.

### Task 2: Name Pending PVC failures without misdiagnosing other timeouts

**Files:**
- Modify: `cli/src/examples.rs` (`run_install_command` Helm timeout branch and diagnostic helpers)
- Test: `cli/src/examples.rs` unit tests and `cli/tests/example_sre_bot_install.rs` external-command fixture with fake `helm`/`kubectl`

**Interfaces:**
- Produces: `fn pending_pvc_warning(pvcs: &serde_json::Value, events: &serde_json::Value) -> Option<String>` and a wall-clock-bounded `async fn pending_pvc_diagnostic(namespace: &str) -> String`.
- Helm timeout and `kubectl rollout status statefulset/tempo` timeout branches append the diagnostic to the original command failure. Helm Secret cleanup is conditional on verified release state, not on PVC Pending.

- [ ] **Step 1: Write failing tests** for one Pending PVC with its Warning Event, a Warning Event for another PVC or stale UID, unreadable/no Pending PVC data, a hanging kubectl diagnostic, and a non-PVC rollout timeout. The binary fixture must exercise both a failing fake Helm and Tempo rollout with read-only fake kubectl, checking the emitted cause and that Secret deletion is not portrayed as a PVC fix.
- [ ] **Step 2: Run the focused `cargo test examples::tests::pending_pvc -- --nocapture`; confirm the new assertions fail.**
- [ ] **Step 3: Implement the bounded read-only diagnostic** using `kubectl get pvc -n <namespace> -o json` and `kubectl get events -n <namespace> -o json` under one short total `tokio::time::timeout`. Match kind, name, namespace, and UID when present to the Pending PVC; include only its Warning `reason` and `message`. If no matching event exists or a read/parse fails, return exact `kubectl get pvc` and `kubectl describe pvc` commands as a hint; never claim storage caused the timeout without a matching Warning. Preserve the original command error. Verify pending Helm state before suggesting its Secret recovery action.
- [ ] **Step 4: Rerun focused and binary-fixture tests and the real `curie example sre-bot install --observability-only --dry-run`; confirm no Kubernetes read on any dry-run.**
- [ ] **Step 5: Commit Task 2** after `cargo fmt --check`, focused tests, and diff review.

### Task 3: Verify the installer slice and hand it to the alert slice

**Files:**
- Modify: `.projects/plans/codex-fix-1912-observability-portability.state.json` (ignored run evidence only)

**Interfaces:**
- Consumes Task 1 runtime variants and Task 2 diagnostics; produces no product API.

- [ ] **Step 1: Run `cd cli && cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo test` on the final installer commit.**
- [ ] **Step 2: Review code and issue scope independently; resolve all Important findings and rerun relevant checks.**
- [ ] **Step 3: Execute local, local-release, cluster, and external-integration proofs with isolated resources, recording command, candidate commit, positive observation, falsifiable negative, and teardown for each.** A blocked tier prevents PR creation and issue closure.
- [ ] **Step 4: Re-run `bash scripts/check-docs.sh` and `git diff --check`; then hand the candidate to the independent Alloy/Prometheus alert plan. #1912 remains open until both slices integrate and real positive/negative log delivery evidence passes.** <!-- doclint:ignore-line -->

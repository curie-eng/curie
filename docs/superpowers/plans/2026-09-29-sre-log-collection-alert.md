# SRE Log-Collection Alert Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make #1912's healthy-but-empty Alloy collector visible as a per-pod alert without paging on a genuinely quiet but attached collector.

**Architecture:** The pinned Alloy chart exposes each DaemonSet pod's metrics on port 12345. Annotate those pods for the shipped Prometheus `kubernetes-pods` scrape job, then alert on a scraped Alloy pod without an active log file and on reads without Loki sends. Keep the two conditions independent so one does not hide the other.

**Tech Stack:** Grafana Alloy Helm values, Prometheus chart rules, `promtool` rule tests, Helm render assertions.

**Spec:** `docs/superpowers/specs/2026-09-29-sre-observability-portability-design.md`

## Global Constraints

- The alert is per Alloy DaemonSet pod, never the load-balanced Alloy Service; the current Prometheus `kubernetes-pods` job retains `namespace`, `pod`, and `node` labels.
- The pinned chart is `grafana/alloy` 1.11.1 with `docker.io/grafana/alloy:v1.18.1`; verify metric names and `component_id` labels on that image before tests or implementation.
- Zero or missing `loki_source_file_files_active_total` while a pod scrape is `up == 1` fires after 10 minutes. A 15-minute window with source reads but no `loki_write_sent_entries_total` increase fires after 5 minutes.
- A quiet collector with positive active-file count and no new lines stays quiet; another healthy Alloy pod must not mask a broken one.
- The shipped Prometheus source boundary remains the observability namespace only; do not open cluster-wide annotation scraping.
- Required tiers: cluster (rendered Helm and running DaemonSet), external-integration (real Alloy-to-Loki). Skill, local, local-release, and live-provider do not reach this slice unless the final diff expands.

## Review Focus

1. A missing source gauge with `up == 1` must alert, not disappear from a PromQL comparison; Task 2 tests it.
2. One healthy pod must not mask another pod at zero files; Task 2 tests both labels in one fixture.
3. A pod with files but no new log lines is quiet, not broken; Task 2 tests it.
4. A source that reads lines while Loki sends none must alert even if the send counter has never appeared; Task 2 tests absent and zero send series.
5. A failed scrape should not be mislabeled as zero files; Task 2 tests `up == 0` stays out of this alert and points to ordinary target health.
6. A first observed read counter sample that is already positive must alert if no send counter exists; an old flat read counter must stay quiet.
7. Two Alloy pods must be assessed separately, even when component and endpoint labels differ.

---

### Task 1: Confirm the pinned metric and scrape contracts

**Files:**
- Create: the run's ignored probe record (experimental evidence, not committed)
- Modify: `examples/sre-bot/observability/alloy-values.yaml`
- Modify: `charts/curie/ci/observability-stack-assertions.sh`

**Interfaces:**
- Produces: the chart's `controller.podAnnotations` carrying `prometheus.io/scrape: "true"`, `prometheus.io/path: /metrics`, and `prometheus.io/port: "12345"` on every Alloy pod.

- [ ] **Step 1: Before product edits, run one bounded throwaway probe** of the pinned Alloy image with a local file-source configuration and owned local Loki-compatible receiver. Generate a unique line, observe a successful sent-entry increment, then make only that receiver unavailable and observe read-without-send. Inspect `/metrics` for `loki_source_file_files_active_total`, `loki_source_file_read_lines_total`, `loki_write_sent_entries_total`, and their exact labels. Record the observed names/labels and remove only this probe's container/files. If the contract differs, revise this plan before coding.
- [ ] **Step 2: Write a failing chart-render assertion** that the Alloy DaemonSet pod has the three scrape annotations and the pinned Prometheus `kubernetes-pods` job selects it while staying namespace-scoped.
- [ ] **Step 3: Run `bash charts/curie/ci/observability-stack-assertions.sh`; confirm the new assertion fails for missing annotations.** <!-- doclint:ignore-line -->
- [ ] **Step 4: Add the pod annotations to `alloy-values.yaml`; rerun the assertion script and confirm the rendered DaemonSet carries them.**
- [ ] **Step 5: Commit the annotation and assertion change.**

### Task 2: Prove empty and dropped log paths alert

**Files:**
- Modify: `examples/sre-bot/observability/prometheus-values.yaml`
- Modify: `examples/sre-bot/observability/reliability-alerts.test.yaml`
- Modify: `examples/sre-bot/docs/METRICS-ROLLOUT.md`

**Interfaces:**
- Consumes Task 1's per-pod scrape and the observed Alloy metric labels. Produces `CurieAlloyNoActiveLogFiles` and `CurieAlloyLogDeliveryStopped` rule names.

- [ ] **Step 1: Add failing `promtool` cases** for zero and absent active-file metrics, a second healthy pod, read-without-send (zero and absent send counter), first observed positive read counter with absent send counter, and old flat read counter, plus quiet/healthy and failed-scrape negatives. Use the exact pinned-image metric labels from Task 1, preserving pod/namespace while aggregating away component/endpoint labels.
- [ ] **Step 2: Run the `promtool` test through `bash charts/curie/ci/observability-stack-assertions.sh`; confirm the new cases fail because no rules exist.** <!-- doclint:ignore-line -->
- [ ] **Step 3: Add the two per-pod rules** in `prometheus-values.yaml`, with 10-minute zero/missing-file duration and 15-minute read window plus 5-minute delivery duration. Keep existing alert expressions and source boundary untouched.
- [ ] **Step 4: Rerun the chart assertion script; confirm all positive and negative alert cases pass, including the rendered Helm rules.**
- [ ] **Step 5: Document the alert interpretation and `kubectl`/Prometheus checks in `METRICS-ROLLOUT.md`; commit the rule, tests and runbook.**

### Task 3: Verify real collection and review the full #1912 diff

**Files:**
- Modify: the run's ignored state record (run evidence, not committed; the pull request carries the evidence)

**Interfaces:**
- Consumes Tasks 1 and 2 and the installer plan; produces no product API.

- [ ] **Step 1: Run code and scope reviews independently, resolve Important findings, and rerun their tests.**
- [ ] **Step 2: On an isolated cluster, install the CRI variant and demonstrate a unique pod log line reaches Loki; repeat with a Docker-runtime cluster for the Docker variant.** For each, query the source/read/write counters, then remove only an owned test mount or receiver and emit a fresh unique log line. Query Prometheus `/api/v1/alerts` for the same pod label and prove inactive → pending → firing after the configured duration; restore the path and prove recovery. Record stimulus, timing, queries, and owned-resource teardown.
- [ ] **Step 3: Record every required tier's exact command, candidate commit, positive and negative observation, and teardown; run the final chart assertions and docs checks.** #1912 remains open until this slice and the installer slice both pass, including the live negative path. Missing cluster or integration evidence blocks the PR and issue closure.

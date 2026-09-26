//! `curie cluster upgrade`: one resumable lifecycle that plans, validates,
//! checks the worker is reachable, checkpoints, migrates, applies, proves
//! exact convergence, runs a canary, and records the new known-good version
//! (issue #2301).
//!
//! Sibling slices this module composes and does not reimplement:
//! - versioned configuration migrations (#2299)
//! - database compatibility windows (#2300)
//! - the kind released-install upgrade CI rung (#2097)
//!
//! `DrainPreflight` is NOT the #2010 drain gate and must not be read as one
//! (issue #2830). It runs before Apply, while the real gate is the chart's
//! pre-upgrade Helm hook Job and only fires once Apply calls `helm upgrade`.
//! All this phase can do ahead of that call is confirm the worker workload is
//! reachable; whether the fleet actually drained is observed afterward, at
//! Converge, from Helm's own retained hook history (`Facet::Drain`, reported
//! as `queues_drained`). Resume after a completed preflight must not repeat
//! it needlessly.

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

use super::command::{mask_secret, plain, require_on_path, run_capture, CommonOpts, OpsCommand};

// These bound the DrainPreflight worker-reachability probe below, not the
// real #2010 drain gate (that gate's own timeout is
// `worker.upgradeDrain.timeoutSeconds` in the chart).
const DRAIN_TIMEOUT_ENV: &str = "CURIE_UPGRADE_DRAIN_TIMEOUT_SECS";
const DRAIN_TIMEOUT_DEFAULT_SECS: u64 = 30;
const DRAIN_POLL_INTERVAL: Duration = Duration::from_millis(100);
const TEST_FAIL_AT_ENV: &str = "CURIE_UPGRADE_TEST_FAIL_AT";
const TEST_INTERRUPT_AFTER_ENV: &str = "CURIE_UPGRADE_TEST_INTERRUPT_AFTER";

/// Durable phases of a cluster upgrade. Order is load-bearing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UpgradePhase {
    Plan,
    Validate,
    /// A worker-reachability check ahead of the real #2010 drain gate, never
    /// an observation of a drain (issue #2830). The `drain` alias keeps a
    /// checkpoint a pre-#2830 binary persisted under the old name resumable.
    #[serde(alias = "drain")]
    DrainPreflight,
    Checkpoint,
    Migrate,
    Apply,
    Converge,
    Canary,
    Commit,
}

impl UpgradePhase {
    pub const ALL: [UpgradePhase; 9] = [
        UpgradePhase::Plan,
        UpgradePhase::Validate,
        UpgradePhase::DrainPreflight,
        UpgradePhase::Checkpoint,
        UpgradePhase::Migrate,
        UpgradePhase::Apply,
        UpgradePhase::Converge,
        UpgradePhase::Canary,
        UpgradePhase::Commit,
    ];

    pub fn as_str(&self) -> &'static str {
        match self {
            UpgradePhase::Plan => "plan",
            UpgradePhase::Validate => "validate",
            UpgradePhase::DrainPreflight => "drain_preflight",
            UpgradePhase::Checkpoint => "checkpoint",
            UpgradePhase::Migrate => "migrate",
            UpgradePhase::Apply => "apply",
            UpgradePhase::Converge => "converge",
            UpgradePhase::Canary => "canary",
            UpgradePhase::Commit => "commit",
        }
    }

    /// Parse the durable phase name used by test-only env hooks.
    pub fn parse(raw: &str) -> Option<UpgradePhase> {
        let raw = raw.trim();
        Self::ALL.into_iter().find(|phase| phase.as_str() == raw)
    }
}

fn is_soak_identity(namespace: &str, release: &str) -> bool {
    matches!(namespace, "curie" | "default") || matches!(release, "curie" | "default")
}

/// Honor `CURIE_UPGRADE_TEST_*` only on a task-owned install. Soak identities
/// refuse the hook before any phase runs.
pub(crate) fn test_hook_phase(opts: &UpgradeOpts, env_name: &str) -> Result<Option<UpgradePhase>> {
    let raw = match std::env::var(env_name) {
        Ok(value) => value,
        Err(_) => return Ok(None),
    };
    if raw.trim().is_empty() {
        return Ok(None);
    }
    if is_soak_identity(&opts.common.namespace, &opts.common.release) {
        bail!(
            "refusing {env_name} against soak namespace '{}' or soak release '{}'; use a task-owned namespace and release",
            opts.common.namespace,
            opts.common.release
        );
    }
    match UpgradePhase::parse(&raw) {
        Some(phase) => Ok(Some(phase)),
        None => bail!("unknown upgrade test phase '{}' in {env_name}", raw.trim()),
    }
}

impl std::fmt::Display for UpgradePhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(UpgradePhase::as_str(self))
    }
}

/// Flags for `curie cluster upgrade`.
#[derive(Debug, Clone)]
pub struct UpgradeOpts {
    pub common: CommonOpts,
    pub to: String,
    pub chart: UpgradeChart,
    pub yes: bool,
    pub forward_only: bool,
}

/// The chart operand selected before the lifecycle starts, including whether
/// target-specific checks can run without fetching an absent release asset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpgradeChart {
    /// A local directory or packaged archive that is available now.
    AvailableLocal(String),
    /// A repository or OCI reference Helm resolves and pins with `--version`.
    HelmReference(String),
    /// A release archive a network-free dry-run will download before real Apply.
    PendingRelease {
        source_url: String,
        cache_path: String,
    },
}

impl UpgradeChart {
    fn operand(&self) -> &str {
        match self {
            Self::AvailableLocal(chart) | Self::HelmReference(chart) => chart,
            Self::PendingRelease { cache_path, .. } => cache_path,
        }
    }

    fn uses_helm_version(&self) -> bool {
        matches!(self, Self::HelmReference(_))
    }

    fn is_available_local(&self) -> bool {
        matches!(self, Self::AvailableLocal(_))
    }

    fn pending_release(&self) -> Option<(&str, &str)> {
        match self {
            Self::PendingRelease {
                source_url,
                cache_path,
            } => Some((source_url, cache_path)),
            _ => None,
        }
    }
}

/// What `cluster status` reports about the in-flight or last upgrade.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpgradeStatusView {
    pub phase: Option<String>,
    pub status: String,
    pub known_good_version: Option<String>,
    pub target_version: Option<String>,
}

impl UpgradeStatusView {
    pub fn idle(known_good_version: Option<String>) -> Self {
        Self {
            phase: None,
            status: "idle".into(),
            known_good_version,
            target_version: None,
        }
    }

    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "phase": self.phase,
            "status": self.status,
            "known_good_version": self.known_good_version,
            "target_version": self.target_version,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UpgradeRecord {
    target_version: String,
    from_version: Option<String>,
    known_good_version: Option<String>,
    completed: Vec<UpgradePhase>,
    status: String,
    plan: Vec<String>,
    /// Whether the DrainPreflight worker-reachability check has already run
    /// once for this attempt. Not an observation of the real #2010 drain
    /// gate, which `queues_drained` reports from Converge instead.
    drain_completed: bool,
    convergence: Option<Convergence>,
    canary: Option<Canary>,
    fail_forward: Option<FailForward>,
    resumed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Convergence {
    pub exact: bool,
    pub images: bool,
    pub generations: bool,
    pub replicas: bool,
    pub unavailable_zero: bool,
    pub hooks_healthy: bool,
    pub queues_drained: bool,
    pub manifest_matches: bool,
}

impl Convergence {
    fn exact_ok() -> Self {
        Self {
            exact: true,
            images: true,
            generations: true,
            replicas: true,
            unavailable_zero: true,
            hooks_healthy: true,
            queues_drained: true,
            manifest_matches: true,
        }
    }

    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "exact": self.exact,
            "images": self.images,
            "generations": self.generations,
            "replicas": self.replicas,
            "unavailable_zero": self.unavailable_zero,
            "hooks_healthy": self.hooks_healthy,
            "queues_drained": self.queues_drained,
            "manifest_matches": self.manifest_matches,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Canary {
    pub passed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailForward {
    pub command: String,
    pub reason: String,
}

/// Agent-facing result of `curie cluster upgrade`.
// Agent-facing `--json` payload; boxing it would churn every match site.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum ClusterUpgradeOutput {
    DryRun(crate::ui::DryRunPlan),
    Completed {
        status: String,
        phase: String,
        target_version: String,
        from_version: Option<String>,
        known_good_version: Option<String>,
        resumed: bool,
        previous_serving: bool,
        unchanged: bool,
        plan: Vec<String>,
        convergence: Option<Convergence>,
        canary: Option<Canary>,
        fail_forward: Option<FailForward>,
        compatibility: Option<serde_json::Value>,
    },
}

impl crate::ui::CliOutput for ClusterUpgradeOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            ClusterUpgradeOutput::DryRun(plan) => plan.to_json(),
            ClusterUpgradeOutput::Completed {
                status,
                phase,
                target_version,
                from_version,
                known_good_version,
                resumed,
                previous_serving,
                unchanged,
                plan,
                convergence,
                canary,
                fail_forward,
                compatibility,
            } => {
                let mut v = serde_json::json!({
                    "status": status,
                    "phase": phase,
                    "target_version": target_version,
                    "from_version": from_version,
                    "known_good_version": known_good_version,
                    "resumed": resumed,
                    "previous_serving": previous_serving,
                    "unchanged": unchanged,
                    "plan": plan,
                    "convergence": convergence.as_ref().map(Convergence::to_json),
                    "canary": canary.as_ref().map(|c| serde_json::json!({"passed": c.passed})),
                    "fail_forward": fail_forward.as_ref().map(|f| serde_json::json!({
                        "command": f.command,
                        "reason": f.reason,
                    })),
                    "compatibility": compatibility.clone().unwrap_or(serde_json::Value::Null),
                });
                if let Some(obj) = v.as_object_mut() {
                    if canary.is_none() {
                        obj.insert("canary".into(), serde_json::Value::Null);
                    }
                    if convergence.is_none() {
                        obj.insert("convergence".into(), serde_json::Value::Null);
                    }
                    if fail_forward.is_none() {
                        obj.insert("fail_forward".into(), serde_json::Value::Null);
                    }
                }
                v
            }
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            ClusterUpgradeOutput::DryRun(plan) => plan.render(ui),
            ClusterUpgradeOutput::Completed {
                status,
                phase,
                target_version,
                known_good_version,
                resumed,
                previous_serving,
                fail_forward,
                plan,
                ..
            } => {
                ui.payload(&format!(
                    "cluster upgrade {status} · phase {phase} · target {target_version} · known-good {}",
                    known_good_version.as_deref().unwrap_or("none")
                ));
                if *resumed {
                    ui.note("resumed from the last durable phase");
                }
                for line in plan {
                    ui.payload_plain(line);
                }
                if *status != "succeeded" && !previous_serving {
                    ui.warn("previous version is not serving; follow the fail-forward path");
                }
                if let Some(ff) = fail_forward {
                    ui.note(&format!("fail-forward: {} ({})", ff.command, ff.reason));
                }
            }
        }
    }
}

/// In-memory host used by the lifecycle tests. The live CLI path uses
/// [`LiveHost`] against helm/kubectl.
pub struct FakeUpgradeHost {
    current: Option<String>,
    known_good: Option<String>,
    record: Option<UpgradeRecord>,
    fail_at: Option<UpgradePhase>,
    interrupt_after: Option<UpgradePhase>,
    secret: Option<String>,
    refuse_schema: bool,
    refuse_config: bool,
    mixed_on_fail: bool,
    canary_ok: bool,
    converge_exact: bool,
    manifest_matches: bool,
    in_flight: Vec<String>,
    retained_values: bool,
    runner_layer_clears: Vec<String>,
    applied: bool,
    pub drain_calls: u32,
    pub mutate_calls: u32,
}

impl FakeUpgradeHost {
    pub fn empty() -> Self {
        Self {
            current: None,
            known_good: None,
            record: None,
            fail_at: None,
            interrupt_after: None,
            secret: None,
            refuse_schema: false,
            refuse_config: false,
            mixed_on_fail: false,
            canary_ok: true,
            converge_exact: true,
            manifest_matches: true,
            in_flight: Vec::new(),
            retained_values: false,
            runner_layer_clears: Vec::new(),
            applied: false,
            drain_calls: 0,
            mutate_calls: 0,
        }
    }

    pub fn installed(version: &str) -> Self {
        let mut h = Self::empty();
        h.current = Some(version.to_string());
        h.known_good = Some(version.to_string());
        h
    }

    pub fn with_known_good(mut self, version: &str) -> Self {
        self.known_good = Some(version.to_string());
        self
    }

    pub fn with_secret(mut self, secret: &str) -> Self {
        self.secret = Some(secret.to_string());
        self
    }

    pub fn fail_at(mut self, phase: UpgradePhase) -> Self {
        self.fail_at = Some(phase);
        self
    }

    pub fn interrupt_after(mut self, phase: UpgradePhase) -> Self {
        self.interrupt_after = Some(phase);
        self
    }

    pub fn refuse_schema(mut self) -> Self {
        self.refuse_schema = true;
        self
    }

    pub fn refuse_config(mut self) -> Self {
        self.refuse_config = true;
        self
    }

    pub fn mixed_versions_on_fail(mut self) -> Self {
        self.mixed_on_fail = true;
        self
    }

    pub fn canary_fails(mut self) -> Self {
        self.canary_ok = false;
        self
    }

    pub fn converge_incomplete(mut self) -> Self {
        self.converge_exact = false;
        self
    }

    pub fn manifest_mismatch(mut self) -> Self {
        self.manifest_matches = false;
        self
    }

    pub fn in_flight(mut self, ids: &[&str]) -> Self {
        self.in_flight = ids.iter().map(|s| (*s).to_string()).collect();
        self
    }

    /// Agents whose layered runner the upgrade will stop matching (#3218).
    pub fn with_runner_layer_clears(mut self, agents: &[&str]) -> Self {
        self.runner_layer_clears = agents.iter().map(|a| a.to_string()).collect();
        self
    }

    pub fn with_retained_values(mut self) -> Self {
        self.retained_values = true;
        self
    }

    pub fn clear_interrupt(&mut self) {
        self.interrupt_after = None;
        self.fail_at = None;
    }

    pub fn current_version(&self) -> String {
        self.current.clone().unwrap_or_default()
    }

    pub fn persisted_json(&self) -> String {
        self.record
            .as_ref()
            .map(|r| serde_json::to_string(r).unwrap_or_default())
            .unwrap_or_default()
    }

    pub fn status_view(&self) -> UpgradeStatusView {
        status_from_record(self.record.as_ref(), self.known_good.clone())
    }
}

impl UpgradeDriver for FakeUpgradeHost {
    fn current(&self) -> Option<String> {
        self.current.clone()
    }
    fn set_current(&mut self, version: Option<String>) {
        self.current = version;
    }
    fn known_good(&self) -> Option<String> {
        self.known_good.clone()
    }
    fn set_known_good(&mut self, version: Option<String>) {
        self.known_good = version;
    }
    fn retained_values(&self) -> bool {
        self.retained_values
    }
    fn runner_layer_clears(&self) -> Vec<String> {
        self.runner_layer_clears.clone()
    }
    fn load_record(&self) -> Option<UpgradeRecord> {
        self.record.clone()
    }
    fn store_record(&mut self, record: UpgradeRecord) -> Result<()> {
        self.record = Some(record);
        Ok(())
    }
    fn secret(&self) -> Option<&str> {
        self.secret.as_deref()
    }
    fn refuse_config(&self) -> bool {
        self.refuse_config
    }
    fn refuse_schema(&self) -> bool {
        self.refuse_schema
    }
    fn drain_preflight_once(&mut self) -> Result<bool> {
        self.drain_calls += 1;
        Ok(self.in_flight.is_empty())
    }
    fn apply_target(&mut self, to: &str) -> Result<()> {
        self.mutate_calls += 1;
        self.applied = true;
        self.set_current(Some(to.to_string()));
        Ok(())
    }
    fn observe_convergence(&self) -> Result<ConvergenceVerdict> {
        let mut conv = Convergence::exact_ok();
        if !self.converge_exact {
            conv.exact = false;
            conv.replicas = false;
        }
        if !self.manifest_matches {
            conv.exact = false;
            conv.manifest_matches = false;
        }
        Ok(conv.into())
    }
    fn run_canary(&self) -> Result<Canary> {
        Ok(Canary {
            passed: self.canary_ok,
        })
    }
    fn serving_previous(&self) -> bool {
        if self.mixed_on_fail && self.applied {
            return false;
        }
        match (&self.current, &self.known_good) {
            (Some(cur), Some(kg)) => cur == kg,
            (None, _) => true,
            _ => true,
        }
    }
    fn interrupt_after(&self) -> Option<UpgradePhase> {
        self.interrupt_after
    }
    fn fail_at(&self) -> Option<UpgradePhase> {
        self.fail_at
    }
}

fn status_from_record(
    record: Option<&UpgradeRecord>,
    fallback_known_good: Option<String>,
) -> UpgradeStatusView {
    match record {
        None => UpgradeStatusView::idle(fallback_known_good),
        Some(r) => UpgradeStatusView {
            phase: r.completed.last().map(|p| p.as_str().to_string()),
            status: r.status.clone(),
            known_good_version: r.known_good_version.clone().or(fallback_known_good),
            target_version: Some(r.target_version.clone()),
        },
    }
}

fn remaining_after(completed: &[UpgradePhase]) -> Vec<UpgradePhase> {
    UpgradePhase::ALL
        .into_iter()
        .filter(|p| !completed.contains(p))
        .collect()
}

fn plan_lines(
    opts: &UpgradeOpts,
    from: Option<&str>,
    secret: Option<&str>,
    schema_plan: Option<&str>,
    retained_values: bool,
    runner_layer_clears: &[String],
) -> Vec<String> {
    let apply = helm_upgrade_argv(
        opts,
        &opts.to,
        from.is_none(),
        retained_values.then_some(RETAINED_VALUES_PLACEHOLDER),
        runner_layer_clears,
    );
    let from = from.unwrap_or("none");
    let mut lines = vec![
        format!("phase plan: {from} -> {}", opts.to),
        "phase validate: configuration overlay migration and pre-mutation refusals".into(),
        "phase drain_preflight: confirm the worker workload is reachable; the worker upgrade drain gate itself runs later, inside Apply's Helm pre-upgrade hook (issue 2010)".into(),
        "phase checkpoint: persist recoverable release state".into(),
        // The chart's pre-upgrade hook Job owns schema migration and Apply
        // fires it; this phase is only a resumable checkpoint boundary
        // (issue 2588).
        "phase migrate: checkpoint boundary only; the chart's pre-upgrade hook Job \
         performs schema migration during apply"
            .into(),
        apply.join(" "),
        "phase converge: exact images, generations, replicas, unavailable=0, hooks, queues, manifest"
            .into(),
        "phase canary: target-version smoke".into(),
        "phase commit: record known-good version".into(),
    ];
    // #2299: the configuration schema version the upgrade moves from and to.
    if let Some(schema_plan) = schema_plan {
        lines.push(schema_plan.to_string());
    }
    if let Some(secret) = secret {
        lines.push(format!(
            "preserved credential api.credentials={}",
            mask_secret(secret)
        ));
    }
    if let Some(notice) = runner_layer_notice(runner_layer_clears) {
        lines.push(notice);
    }
    if let Some((source_url, cache_path)) = opts.chart.pending_release() {
        lines.push(format!(
            "phase validate pending: chart metadata and schema compatibility after release chart download from {source_url} to {cache_path}"
        ));
    }
    lines
}

/// The runner reference the upgraded release will render (#3218): the
/// retained overlay's `agentSandbox.runner` fields over the target chart's
/// defaults, with an empty tag meaning the target chart's appVersion, which
/// the release train holds equal to `--to`. `None` when no image is known.
pub(crate) fn target_runner_ref(
    chart_default: Option<&serde_json::Value>,
    overlay: &serde_json::Value,
    to: &str,
) -> Option<String> {
    let mut runner = serde_json::Map::new();
    for source in [chart_default, Some(overlay)].into_iter().flatten() {
        if let Some(fields) = source
            .pointer("/agentSandbox/runner")
            .and_then(|r| r.as_object())
        {
            for (key, value) in fields {
                if value.is_null() {
                    runner.remove(key);
                } else {
                    runner.insert(key.clone(), value.clone());
                }
            }
        }
    }
    let values = serde_json::json!({"agentSandbox": {"runner": runner}});
    crate::cluster_secrets::effective_runner_ref(&values, Some(to))
}

/// The operator-facing line naming every agent whose layered runner stops
/// matching (#3218, ADR 0173 decision 5). `None` when there are none.
pub(crate) fn runner_layer_notice(agents: &[String]) -> Option<String> {
    if agents.is_empty() {
        return None;
    }
    Some(format!(
        "runner layers: the platform runner changes, so the layered runner of agent(s) {} \
         will stop matching. This upgrade clears agentSandbox.runnerImages for them: they run \
         the platform runner WITHOUT their layer until their owners rebuild with `curie build \
         --plugin-dir <dir> --registry <ref>` against the upgraded CLI and redeploy with \
         `curie cluster deploy`",
        agents.join(", ")
    ))
}

fn completed_output(
    record: &UpgradeRecord,
    previous_serving: bool,
    failed_phase: Option<UpgradePhase>,
    compatibility: Option<serde_json::Value>,
) -> ClusterUpgradeOutput {
    let last = record
        .completed
        .last()
        .map(UpgradePhase::as_str)
        .unwrap_or("plan");
    ClusterUpgradeOutput::Completed {
        status: record.status.clone(),
        phase: if record.status == "succeeded" {
            "commit".into()
        } else {
            failed_phase
                .map(|p| p.as_str().to_string())
                .unwrap_or_else(|| last.into())
        },
        target_version: record.target_version.clone(),
        from_version: record.from_version.clone(),
        known_good_version: record.known_good_version.clone(),
        resumed: record.resumed,
        previous_serving,
        unchanged: record.status == "succeeded"
            && record.from_version.as_deref() == Some(record.target_version.as_str()),
        plan: record.plan.clone(),
        convergence: record.convergence.clone(),
        canary: record.canary.clone(),
        fail_forward: record.fail_forward.clone(),
        compatibility,
    }
}

/// Bound a fail-forward reason: the observer can report one line per unhealthy
/// container, and the operator needs a reason, not a log dump.
fn truncate_reason(detail: &str) -> String {
    const LIMIT: usize = 600;
    if detail.chars().count() <= LIMIT {
        return detail.to_string();
    }
    let kept: String = detail.chars().take(LIMIT).collect();
    format!("{kept}... (truncated; run `curie cluster status` for the full observation)")
}

fn fail_forward_for(opts: &UpgradeOpts, previous_serving: bool, reason: &str) -> FailForward {
    if previous_serving {
        FailForward {
            command: format!(
                "curie cluster rollback --yes --release {} --namespace {}",
                opts.common.release, opts.common.namespace
            ),
            reason: reason.to_string(),
        }
    } else {
        FailForward {
            command: format!(
                "curie cluster upgrade --to {} --release {} --namespace {}",
                opts.to, opts.common.release, opts.common.namespace
            ),
            reason: reason.to_string(),
        }
    }
}

trait UpgradeDriver {
    fn current(&self) -> Option<String>;
    fn set_current(&mut self, version: Option<String>);
    fn known_good(&self) -> Option<String>;
    fn set_known_good(&mut self, version: Option<String>);
    fn load_record(&self) -> Option<UpgradeRecord>;
    /// Persisting the checkpoint is part of the phase, not a side effect: a
    /// lost checkpoint means the next run cannot resume (R5).
    fn store_record(&mut self, record: UpgradeRecord) -> Result<()>;
    fn secret(&self) -> Option<&str> {
        None
    }
    /// The redacted `config schema: <from> -> <to>` plan line (#2299), when a
    /// retained configuration was read and migrated.
    fn schema_plan(&self) -> Option<String> {
        None
    }
    /// Whether Apply hands Helm a retained values overlay via `-f` (#2863).
    fn retained_values(&self) -> bool {
        false
    }
    /// The agents whose layered runner will stop matching the installation's
    /// runner after this upgrade (#3218). Apply clears each one's
    /// `agentSandbox.runnerImages.<agent>` in the same `helm upgrade`.
    fn runner_layer_clears(&self) -> Vec<String> {
        Vec::new()
    }
    fn redact(&self, text: &str) -> String {
        match self.secret() {
            Some(secret) => text.replace(secret, &mask_secret(secret)),
            None => text.to_string(),
        }
    }
    /// A pre-mutation refusal carrying its own operator-facing detail, checked
    /// before the two boolean gates below. [`LiveHost`] answers it; only
    /// [`FakeUpgradeHost`] relies on the boolean defaults.
    fn validate_refusal(&self) -> Option<String> {
        None
    }
    fn refuse_config(&self) -> bool {
        false
    }
    fn refuse_schema(&self) -> bool {
        false
    }
    /// A fresh read of the version the release actually reports. Defaults to
    /// the driver's own bookkeeping; [`LiveHost`] re-reads Helm so Commit
    /// stamps known-good from a post-condition, never from the request.
    fn observed_version(&self) -> Option<String> {
        self.current()
    }
    /// The DrainPreflight worker-reachability check, not an observation of
    /// the real #2010 drain gate (issue #2830). See `LiveHost::live_drain_preflight`.
    fn drain_preflight_once(&mut self) -> Result<bool>;
    fn apply_target(&mut self, to: &str) -> Result<()>;
    fn observe_convergence(&self) -> Result<ConvergenceVerdict>;
    fn run_canary(&self) -> Result<Canary>;
    fn serving_previous(&self) -> bool;
    fn interrupt_after(&self) -> Option<UpgradePhase> {
        None
    }
    fn fail_at(&self) -> Option<UpgradePhase> {
        None
    }
    fn compatibility_decision(&self) -> Option<serde_json::Value> {
        None
    }
}

/// Run the upgrade lifecycle against a host. Tests inject a [`FakeUpgradeHost`].
pub async fn run_lifecycle(
    opts: UpgradeOpts,
    host: &mut FakeUpgradeHost,
) -> Result<ClusterUpgradeOutput> {
    run_lifecycle_inner(opts, host).await
}

async fn run_lifecycle_inner<H: UpgradeDriver>(
    opts: UpgradeOpts,
    host: &mut H,
) -> Result<ClusterUpgradeOutput> {
    if opts.to.trim().is_empty() {
        bail!("--to requires a target version");
    }
    let from = host.current();
    let plan = plan_lines(
        &opts,
        from.as_deref(),
        host.secret(),
        host.schema_plan().as_deref(),
        host.retained_values(),
        &host.runner_layer_clears(),
    );
    let mut plan: Vec<String> = plan.into_iter().map(|l| host.redact(&l)).collect();

    if opts.common.dry_run {
        // The plan is what the command WILL do, so a dry run that computed the
        // read-only pre-mutation checks must show the refusal the real run
        // would hit at Validate instead of printing a clean nine-phase plan,
        // and must fail like that real run does (#2862).
        let refusal = host.validate_refusal().or_else(|| {
            if host.refuse_config() {
                Some("configuration compatibility check refused the overlay before mutation".into())
            } else if host.refuse_schema() {
                Some("database/application compatibility check refused the target schema before mutation".into())
            } else {
                None
            }
        });
        let refusal = refusal.map(|detail| host.redact(&detail));
        if let Some(detail) = &refusal {
            plan.push(format!("refusal at validate: {detail}"));
        }
        let output = ClusterUpgradeOutput::DryRun(crate::ui::DryRunPlan { lines: plan });
        return match refusal {
            Some(detail) => Err(crate::ui::ui().failed_report(&output, anyhow::anyhow!(detail))),
            None => Ok(output),
        };
    }

    let mut record = match host.load_record() {
        Some(existing)
            if existing.target_version == opts.to && existing.status == "in_progress" =>
        {
            let mut existing = existing;
            existing.resumed = true;
            existing
        }
        Some(existing)
            if existing.target_version != opts.to && existing.status == "in_progress" =>
        {
            bail!(
                "an upgrade to {} is already in progress; resume it or wait",
                existing.target_version
            );
        }
        _ => UpgradeRecord {
            target_version: opts.to.clone(),
            from_version: from.clone(),
            known_good_version: host.known_good(),
            completed: Vec::new(),
            status: "in_progress".into(),
            plan: plan.clone(),
            drain_completed: false,
            convergence: None,
            canary: None,
            fail_forward: None,
            resumed: false,
        },
    };

    let same_version = from.as_deref() == Some(opts.to.as_str())
        && host.known_good().as_deref() == Some(opts.to.as_str());

    // Resume after Validate still honors a freshly computed refusal and
    // must not replay DrainPreflight to reach it.
    if host.validate_refusal().is_some() || host.refuse_config() || host.refuse_schema() {
        execute_phase(UpgradePhase::Validate, &opts, host, &mut record)?;
    }

    for phase in remaining_after(&record.completed) {
        if same_version
            && matches!(
                phase,
                UpgradePhase::DrainPreflight
                    | UpgradePhase::Checkpoint
                    | UpgradePhase::Migrate
                    | UpgradePhase::Apply
            )
        {
            record.completed.push(phase);
            host.store_record(record.clone())?;
            continue;
        }
        if phase == UpgradePhase::DrainPreflight && from.is_none() {
            record.completed.push(phase);
            host.store_record(record.clone())?;
            continue;
        }

        match execute_phase(phase, &opts, host, &mut record)? {
            PhaseOutcome::Continue => {
                record.completed.push(phase);
                host.store_record(record.clone())?;
                if host.interrupt_after() == Some(phase) {
                    bail!("interrupted after durable phase {}", phase.as_str());
                }
            }
            PhaseOutcome::Failed => {
                record.status = "failed".into();
                let previous = host.serving_previous();
                if record.fail_forward.is_none() {
                    record.fail_forward = Some(fail_forward_for(
                        &opts,
                        previous,
                        &format!("upgrade failed during {}", phase.as_str()),
                    ));
                }
                // Ruling 8.4: the phase failure is the verdict the operator
                // must act on. A lost checkpoint here must travel beside it,
                // never replace it -- raising with `?` would drop the
                // structured output, the convergence payload and fail-forward.
                if let Err(error) = host.store_record(record.clone()) {
                    if let Some(forward) = record.fail_forward.as_mut() {
                        forward.reason =
                            format!("{}; {}", forward.reason, host.redact(&format!("{error:#}")));
                    }
                }
                return Ok(completed_output(
                    &record,
                    previous,
                    Some(phase),
                    host.compatibility_decision(),
                ));
            }
        }
    }

    record.status = "succeeded".into();
    record.known_good_version = Some(opts.to.clone());
    host.set_known_good(Some(opts.to.clone()));
    host.store_record(record.clone())?;
    Ok(completed_output(
        &record,
        true,
        None,
        host.compatibility_decision(),
    ))
}

enum PhaseOutcome {
    Continue,
    Failed,
}

fn execute_phase<H: UpgradeDriver>(
    phase: UpgradePhase,
    opts: &UpgradeOpts,
    host: &mut H,
    record: &mut UpgradeRecord,
) -> Result<PhaseOutcome> {
    if host.fail_at() == Some(phase) {
        return Ok(PhaseOutcome::Failed);
    }
    match phase {
        UpgradePhase::Plan => Ok(PhaseOutcome::Continue),
        UpgradePhase::Validate => {
            if let Some(detail) = host.validate_refusal() {
                bail!("{detail}");
            }
            if host.refuse_config() {
                bail!("configuration compatibility check refused the overlay before mutation");
            }
            if host.refuse_schema() {
                bail!("database/application compatibility check refused the target schema before mutation");
            }
            Ok(PhaseOutcome::Continue)
        }
        UpgradePhase::DrainPreflight => {
            if record.drain_completed {
                return Ok(PhaseOutcome::Continue);
            }
            if !host.drain_preflight_once()? {
                record.fail_forward = Some(fail_forward_for(
                    opts,
                    true,
                    "the worker workload could not be confirmed reachable ahead of the drain gate; retry once the cluster API responds",
                ));
                return Ok(PhaseOutcome::Failed);
            }
            record.drain_completed = true;
            Ok(PhaseOutcome::Continue)
        }
        UpgradePhase::Checkpoint => Ok(PhaseOutcome::Continue),
        UpgradePhase::Migrate => Ok(PhaseOutcome::Continue),
        UpgradePhase::Apply => {
            host.apply_target(&opts.to)?;
            Ok(PhaseOutcome::Continue)
        }
        UpgradePhase::Converge => {
            let verdict = host.observe_convergence()?;
            record.convergence = Some(verdict.convergence.clone());
            if !verdict.convergence.exact {
                // Carry the observer's own text (including an unreadable
                // cluster's recovery hint) instead of letting the generic
                // "upgrade failed during converge" replace it.
                if !verdict.issues.is_empty() {
                    let detail = host.redact(&verdict.issues.join("; "));
                    let detail = truncate_reason(&detail);
                    record.fail_forward = Some(fail_forward_for(
                        opts,
                        host.serving_previous(),
                        &format!("upgrade failed during converge: {detail}"),
                    ));
                }
                return Ok(PhaseOutcome::Failed);
            }
            Ok(PhaseOutcome::Continue)
        }
        UpgradePhase::Canary => {
            let canary = host.run_canary()?;
            record.canary = Some(canary.clone());
            if !canary.passed {
                return Ok(PhaseOutcome::Failed);
            }
            Ok(PhaseOutcome::Continue)
        }
        UpgradePhase::Commit => {
            if record.convergence.as_ref().is_none_or(|c| !c.exact)
                || record.canary.as_ref().is_none_or(|c| !c.passed)
            {
                return Ok(PhaseOutcome::Failed);
            }
            // Ruling 8.11: known-good is a claim about what the cluster is
            // serving, so it is stamped from a post-condition read of the
            // release, never from the requested string.
            let observed = host.observed_version();
            if observed.as_deref() != Some(opts.to.as_str()) {
                let previous = host.serving_previous();
                record.fail_forward = Some(fail_forward_for(
                    opts,
                    previous,
                    &format!(
                        "the release reports {} at commit, not the requested {}",
                        observed.as_deref().unwrap_or("no version"),
                        opts.to
                    ),
                ));
                return Ok(PhaseOutcome::Failed);
            }
            host.set_known_good(observed.clone());
            record.known_good_version = observed;
            Ok(PhaseOutcome::Continue)
        }
    }
}

/// A Converge verdict: the reported sub-flags plus the observer's own issue
/// text. The text is not part of the `--json` convergence payload; it is what
/// the Converge phase puts in `fail_forward.reason` so an actionable recovery
/// string from the observer reaches the operator instead of being replaced by
/// the generic "upgrade failed during converge".
pub struct ConvergenceVerdict {
    pub convergence: Convergence,
    pub issues: Vec<String>,
}

impl From<Convergence> for ConvergenceVerdict {
    fn from(convergence: Convergence) -> Self {
        Self {
            convergence,
            issues: Vec::new(),
        }
    }
}

/// Map one convergence observation onto the reported sub-flags. Every field
/// comes from a facet the observer genuinely determined; none is a literal.
fn convergence_from(observation: &super::convergence::Observation) -> Convergence {
    use super::convergence::Facet;
    let replicas = observation.holds(Facet::Replicas);
    Convergence {
        // `exact` is the whole verdict: any issue at all, including a
        // Rollout-facet one that no named flag covers, means not converged.
        exact: observation.issues.is_empty() && !observation.terminal,
        images: observation.holds(Facet::Image),
        generations: observation.holds(Facet::Generation),
        replicas,
        // Its own facet, not an alias of `replicas`: a workload can be
        // mid-rollout while `unavailableReplicas` is genuinely 0, and the
        // --json contract presents these as independent observations.
        unavailable_zero: observation.holds(Facet::Unavailable),
        hooks_healthy: observation.holds(Facet::Hook),
        // Ruling 13: the #2010 drain gate is a Helm pre-upgrade hook Job that
        // fires during Apply, so Converge is the only phase that can see its
        // verdict; DrainPreflight runs earlier and cannot (issue #2830).
        // `record.drain_completed` is DrainPreflight's own exactly-once flag,
        // not a convergence fact, and is deliberately not consulted.
        queues_drained: observation.holds(Facet::Drain),
        manifest_matches: observation.holds(Facet::Manifest),
    }
}

fn checkpoint_name(release: &str) -> String {
    format!("{release}-upgrade-checkpoint")
}

const UPGRADE_HOLDER_ANNOTATION: &str = "curietech.ai/upgrade-holder";
const UPGRADE_ACTION_ANNOTATION: &str = "curietech.ai/upgrade-action";
const UPGRADE_HOLDER_POINTER: &str = "/metadata/annotations/curietech.ai~1upgrade-holder";
const UPGRADE_ACTION_POINTER: &str = "/metadata/annotations/curietech.ai~1upgrade-action";

struct CheckpointObservation {
    resource_version: String,
    annotations: Option<serde_json::Map<String, serde_json::Value>>,
    data_present: bool,
    record: Option<serde_json::Value>,
}

fn parse_checkpoint_observation(output: &str, operation: &str) -> Result<CheckpointObservation> {
    let object: serde_json::Value = serde_json::from_str(output)
        .with_context(|| format!("{operation} returned malformed ConfigMap JSON"))?;
    let metadata = object
        .get("metadata")
        .and_then(serde_json::Value::as_object)
        .with_context(|| format!("{operation} returned no ConfigMap metadata object"))?;
    let resource_version = metadata
        .get("resourceVersion")
        .and_then(serde_json::Value::as_str)
        .filter(|version| !version.is_empty())
        .with_context(|| format!("{operation} returned no usable resourceVersion"))?
        .to_string();
    let annotations = match metadata.get("annotations") {
        None => None,
        Some(value) => Some(
            value
                .as_object()
                .with_context(|| format!("{operation} returned malformed annotations"))?
                .clone(),
        ),
    };
    let (data_present, record) = match object.get("data") {
        None => (false, None),
        Some(value) => {
            let data = value
                .as_object()
                .with_context(|| format!("{operation} returned malformed ConfigMap data"))?;
            (true, data.get("record").cloned())
        }
    };
    Ok(CheckpointObservation {
        resource_version,
        annotations,
        data_present,
        record,
    })
}

fn checkpoint_is_absent(stderr: &str, name: &str) -> bool {
    super::verbs::failure_reason(stderr)
        == format!("Error from server (NotFound): configmaps \"{name}\" not found")
}

fn namespace_is_absent(stderr: &str, namespace: &str) -> bool {
    let reason = super::verbs::failure_reason(stderr);
    reason.contains("(NotFound)")
        && reason.contains(&format!("namespaces \"{namespace}\" not found"))
}

fn create_lost_race(stderr: &str) -> bool {
    let reason = super::verbs::failure_reason(stderr).to_ascii_lowercase();
    reason.contains("conflict")
        || reason.contains("alreadyexists")
        || reason.contains("already exists")
        || reason.contains("object has been modified")
}

struct LiveHost {
    opts: UpgradeOpts,
    current: Option<String>,
    known_good: Option<String>,
    record: Option<UpgradeRecord>,
    fail_at: Option<UpgradePhase>,
    interrupt_after: Option<UpgradePhase>,
    secret: Option<String>,
    /// The migrated retained overlay Apply hands Helm via `-f`, computed ONCE
    /// before the lifecycle runs (Ruling 8.8): `refuse_config` takes `&self`,
    /// and the overlay Apply applies must be the overlay Validate approved.
    overlay: Option<String>,
    /// Why the configuration migration refused, if it did (#2299, R7).
    config_refusal: Option<String>,
    /// Why the chart cannot install `--to`, if it cannot (Ruling 2, R1).
    chart_refusal: Option<String>,
    /// The redacted configuration schema plan line (#2299).
    schema_plan: Option<String>,
    /// Redacted schema compatibility decision (#2588).
    schema_decision: Option<serde_json::Value>,
    /// Why the target schema was refused, if it was.
    schema_refusal: Option<String>,
    /// Layered agents whose runner stops matching the target runner (#3218).
    runner_layer_clears: Vec<String>,
    holder: String,
    checkpoint_resource_version: Option<String>,
    checkpoint_data_present: bool,
    checkpoint_record: Option<serde_json::Value>,
}

/// Ruling 2: Helm SILENTLY IGNORES `--version` for a local directory or
/// packaged chart file -- it renders the chart on disk and says nothing. A
/// `--version` there would be a pin that does nothing while looking like one,
/// so a local chart is pinned by refusing before mutation when its own
/// metadata is not `--to`, and only a ref Helm must resolve carries the flag.
/// The public command dispatch supplies one mandatory chart selection.
fn chart_ref(opts: &UpgradeOpts) -> &str {
    opts.chart.operand()
}

/// The `helm upgrade` argv, ONE definition shared by the plan line and the
/// mutating call so the printed plan cannot drift from the executed command.
/// A ref Helm resolves IS pinned by `--version`; a local path is pinned by
/// `chart_pin_refusal` before Apply ever runs, so it deliberately carries none.
/// `install` adds `--install` for a release that does not exist yet, and
/// `values` is the retained overlay's `-f` operand: the real tempfile path for
/// Apply, [`RETAINED_VALUES_PLACEHOLDER`] for the plan (#2863).
fn helm_upgrade_argv(
    opts: &UpgradeOpts,
    to: &str,
    install: bool,
    values: Option<&str>,
    runner_layer_clears: &[String],
) -> Vec<String> {
    let chart = chart_ref(opts);
    let mut argv = vec![
        "helm".to_string(),
        "upgrade".into(),
        opts.common.release.clone(),
        chart.to_string(),
        "-n".into(),
        opts.common.namespace.clone(),
        "--wait".into(),
        // Helm's --wait default timeout is 5m. A kind 0.9.1 -> 0.9.0 apply
        // with the schema-migrate hook overruns that, apply bails, and the
        // checkpoint stays in_progress so the next --to is refused.
        "--timeout".into(),
        "15m".into(),
    ];
    if opts.chart.uses_helm_version() {
        argv.push("--version".into());
        argv.push(to.to_string());
    }
    if install {
        argv.push("--install".into());
    }
    if let Some(values) = values {
        argv.push("-f".into());
        argv.push(values.to_string());
    }
    // After `-f`, so the clear wins over a retained layered digest (#3218).
    for pair in crate::cluster_secrets::runner_image_clears(runner_layer_clears) {
        argv.push("--set".into());
        argv.push(pair);
    }
    argv
}

/// Stands in for the retained values tempfile in the printed plan, which never
/// carries the path or the overlay contents.
const RETAINED_VALUES_PLACEHOLDER: &str = "<retained-values>";

fn api_workload_missing(stderr: &str) -> bool {
    let lower = stderr.to_lowercase();
    lower.contains("not found") && (lower.contains("deploy") || lower.contains("pod"))
}

/// Serialize the values document Helm will read.
///
/// #2741: Helm parses a `-f` values file with Go's YAML, which resolves the
/// YAML 1.1 boolean set -- `off`, `on`, `yes`, `no`, `y`, `n` -- from a bare
/// scalar. `serde_norway` emits YAML 1.2, where those words are ordinary
/// strings that need no quoting, so a retained string `"off"` went out as
/// `mode: off` and came back into Helm as the boolean `false`. That silently
/// turned `security.gvisor.mode: "off"` into a gVisor requirement the cluster
/// could not satisfy.
///
/// JSON is the fix rather than a quoting rule, because it removes the
/// disagreement instead of enumerating it: every JSON string is quoted by
/// construction, so no scalar word can be re-resolved, while real booleans,
/// numbers and nulls stay themselves. JSON is also a subset of YAML, and Helm
/// parses `-f` by content, not by file extension, so the document it reads is
/// unchanged in every other respect.
fn helm_values_document(values: &serde_json::Value) -> Result<String> {
    serde_json::to_string_pretty(values).context("could not serialize the migrated overlay")
}

fn merge_forward_only(overlay: Option<&str>) -> Result<String> {
    let mut doc: serde_json::Value = match overlay {
        Some(text) if !text.trim().is_empty() => {
            serde_norway::from_str(text).context("overlay YAML is malformed")?
        }
        _ => serde_json::json!({}),
    };
    if doc.is_null() {
        doc = serde_json::json!({});
    }
    let map = doc.as_object_mut().context("overlay is not a mapping")?;
    let api = map
        .entry("api".to_string())
        .or_insert_with(|| serde_json::json!({}));
    if api.is_null() {
        *api = serde_json::json!({});
    }
    let api_map = api.as_object_mut().context("api is not a mapping")?;
    let migrate = api_map
        .entry("migrate".to_string())
        .or_insert_with(|| serde_json::json!({}));
    if migrate.is_null() {
        *migrate = serde_json::json!({});
    }
    let migrate_map = migrate
        .as_object_mut()
        .context("api.migrate is not a mapping")?;
    migrate_map.insert("forwardOnly".into(), serde_json::Value::Bool(true));
    helm_values_document(&doc)
}

impl LiveHost {
    fn new(opts: UpgradeOpts) -> Result<Self> {
        let fail_at = test_hook_phase(&opts, TEST_FAIL_AT_ENV)?;
        let interrupt_after = test_hook_phase(&opts, TEST_INTERRUPT_AFTER_ENV)?;
        if fail_at.is_some() || interrupt_after.is_some() {
            eprintln!(
                "curie upgrade test hook armed fail_at={fail_at:?} interrupt_after={interrupt_after:?}"
            );
        }
        Ok(Self {
            opts: opts.clone(),
            current: None,
            known_good: None,
            record: None,
            fail_at,
            interrupt_after,
            secret: None,
            overlay: None,
            config_refusal: None,
            chart_refusal: None,
            schema_plan: None,
            schema_decision: None,
            schema_refusal: None,
            runner_layer_clears: Vec::new(),
            holder: uuid::Uuid::new_v4().to_string(),
            checkpoint_resource_version: None,
            checkpoint_data_present: false,
            checkpoint_record: None,
        })
    }

    fn run(&self, cmd: &OpsCommand) -> Result<(bool, String, String)> {
        tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(run_capture(cmd)))
    }

    fn chart_ref(&self) -> &str {
        chart_ref(&self.opts)
    }

    fn ownership_action(&self) -> String {
        format!("upgrade to {}", self.opts.to)
    }

    fn checkpoint_get_command(&self) -> OpsCommand {
        OpsCommand::new(
            "kubectl",
            vec![
                plain("get"),
                plain("configmap"),
                plain(checkpoint_name(&self.opts.common.release)),
                plain("-n"),
                plain(&self.opts.common.namespace),
                plain("-o"),
                plain("json"),
            ],
        )
    }

    fn checkpoint_diagnostic(&self) -> String {
        let (ok, out, err) = match self.run(&self.checkpoint_get_command()) {
            Ok(result) => result,
            Err(error) => {
                return format!("current checkpoint could not be read: {error:#}");
            }
        };
        if !ok {
            return format!(
                "current checkpoint could not be read: {}",
                super::verbs::failure_reason(&err)
            );
        }
        let object: serde_json::Value = match serde_json::from_str(&out) {
            Ok(object) => object,
            Err(_) => return "current checkpoint response is malformed".into(),
        };
        let display = |value: Option<&serde_json::Value>| match value {
            Some(serde_json::Value::String(value)) => value.clone(),
            Some(_) => "<malformed>".into(),
            None => "<absent>".into(),
        };
        let version = display(object.pointer("/metadata/resourceVersion"));
        let holder = display(
            object
                .pointer("/metadata/annotations")
                .and_then(|annotations| annotations.get(UPGRADE_HOLDER_ANNOTATION)),
        );
        let action = display(
            object
                .pointer("/metadata/annotations")
                .and_then(|annotations| annotations.get(UPGRADE_ACTION_ANNOTATION)),
        );
        format!("current checkpoint resourceVersion {version}, holder {holder}, action {action}")
    }

    fn validate_owned_observation(
        &self,
        observation: &CheckpointObservation,
        operation: &str,
        expected_record: Option<&str>,
    ) -> Result<()> {
        let annotations = observation
            .annotations
            .as_ref()
            .with_context(|| format!("{operation} returned no ownership annotations"))?;
        let holder = annotations
            .get(UPGRADE_HOLDER_ANNOTATION)
            .and_then(serde_json::Value::as_str)
            .with_context(|| format!("{operation} returned no valid holder annotation"))?;
        if holder != self.holder {
            bail!(
                "{operation} returned holder {holder}, not this invocation holder {}",
                self.holder
            );
        }
        let action = annotations
            .get(UPGRADE_ACTION_ANNOTATION)
            .and_then(serde_json::Value::as_str)
            .with_context(|| format!("{operation} returned no valid action annotation"))?;
        if action != self.ownership_action() {
            bail!("{operation} returned an unexpected ownership action");
        }
        if let Some(expected_record) = expected_record {
            if !observation.data_present {
                bail!("{operation} returned no ConfigMap data after the record write");
            }
            let observed_record = observation
                .record
                .as_ref()
                .and_then(serde_json::Value::as_str)
                .with_context(|| format!("{operation} returned no valid checkpoint record"))?;
            if observed_record != expected_record {
                bail!("{operation} returned a different checkpoint record");
            }
        }
        Ok(())
    }

    fn adopt_checkpoint_observation(&mut self, observation: CheckpointObservation) {
        self.checkpoint_resource_version = Some(observation.resource_version);
        self.checkpoint_data_present = observation.data_present;
        self.checkpoint_record = observation.record;
    }

    fn parse_acquired_record(&self) -> Result<Option<UpgradeRecord>> {
        let Some(value) = &self.checkpoint_record else {
            return Ok(None);
        };
        let record = value
            .as_str()
            .context("upgrade checkpoint record is not a string")?;
        serde_json::from_str(record)
            .context("upgrade checkpoint record is malformed")
            .map(Some)
    }

    fn run_checkpoint_patch(&self, patch: &serde_json::Value) -> Result<(bool, String, String)> {
        let tmp = tempfile::NamedTempFile::new().context("upgrade checkpoint patch tempfile")?;
        std::fs::write(tmp.path(), serde_json::to_vec_pretty(patch)?)?;
        let cmd = OpsCommand::new(
            "kubectl",
            vec![
                plain("patch"),
                plain("configmap"),
                plain(checkpoint_name(&self.opts.common.release)),
                plain("-n"),
                plain(&self.opts.common.namespace),
                plain("--type=json"),
                plain("--patch-file"),
                plain(tmp.path().to_string_lossy().into_owned()),
                plain("-o"),
                plain("json"),
            ],
        );
        self.run(&cmd)
    }

    fn acquire_ownership(&mut self) -> Result<()> {
        let checkpoint = checkpoint_name(&self.opts.common.release);
        let (ok, out, err) = self.run(&self.checkpoint_get_command())?;
        if !ok {
            if namespace_is_absent(&err, &self.opts.common.namespace) {
                let reason = super::verbs::failure_reason(&err);
                bail!(
                    "namespace {} does not exist, so upgrade ownership cannot be created: {reason}; establish the install with `curie cluster up` first",
                    self.opts.common.namespace,
                );
            }
            if !checkpoint_is_absent(&err, &checkpoint) {
                bail!(
                    "could not read upgrade ownership: {}",
                    super::verbs::failure_reason(&err)
                );
            }
            return self.create_ownership();
        }

        let observed = parse_checkpoint_observation(&out, "upgrade ownership read")?;
        let patch = match observed.annotations.as_ref() {
            None => serde_json::json!([
                {
                    "op": "test",
                    "path": "/metadata/resourceVersion",
                    "value": observed.resource_version,
                },
                {
                    "op": "add",
                    "path": "/metadata/annotations",
                    "value": {
                        (UPGRADE_HOLDER_ANNOTATION): self.holder,
                        (UPGRADE_ACTION_ANNOTATION): self.ownership_action(),
                    },
                },
            ]),
            Some(annotations) => {
                if let Some(holder) = annotations.get(UPGRADE_HOLDER_ANNOTATION) {
                    let holder = holder
                        .as_str()
                        .context("upgrade holder annotation is malformed")?;
                    let action = match annotations.get(UPGRADE_ACTION_ANNOTATION) {
                        Some(value) => value
                            .as_str()
                            .context("upgrade action annotation is malformed")?,
                        None => "<unspecified>",
                    };
                    bail!(
                        "upgrade ownership is held by {holder} for action {action}; wait for that upgrade to finish, or verify that holder has stopped before manual recovery"
                    );
                }
                serde_json::json!([
                    {
                        "op": "test",
                        "path": "/metadata/resourceVersion",
                        "value": observed.resource_version,
                    },
                    {
                        "op": "test",
                        "path": "/metadata/annotations",
                        "value": annotations,
                    },
                    {
                        "op": "add",
                        "path": UPGRADE_HOLDER_POINTER,
                        "value": self.holder,
                    },
                    {
                        "op": "add",
                        "path": UPGRADE_ACTION_POINTER,
                        "value": self.ownership_action(),
                    },
                ])
            }
        };
        let (ok, out, err) = self.run_checkpoint_patch(&patch)?;
        if !ok {
            let diagnostic = self.checkpoint_diagnostic();
            bail!(
                "could not acquire upgrade ownership: {}; {diagnostic}; wait for the current upgrade to finish, or verify that its holder has stopped before manual recovery",
                super::verbs::failure_reason(&err)
            );
        }
        let observation = parse_checkpoint_observation(&out, "upgrade ownership patch")
            .context("ownership may remain because the server response cannot be trusted")?;
        self.validate_owned_observation(&observation, "upgrade ownership patch", None)
            .context("ownership may remain because the server response cannot be trusted")?;
        self.adopt_checkpoint_observation(observation);
        Ok(())
    }

    fn create_ownership(&mut self) -> Result<()> {
        let manifest = serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": checkpoint_name(&self.opts.common.release),
                "namespace": self.opts.common.namespace,
                "labels": {
                    "app.kubernetes.io/managed-by": "curie",
                    "curietech.ai/upgrade": "checkpoint",
                },
                "annotations": {
                    (UPGRADE_HOLDER_ANNOTATION): self.holder,
                    (UPGRADE_ACTION_ANNOTATION): self.ownership_action(),
                },
            },
        });
        let tmp = tempfile::NamedTempFile::new().context("upgrade ownership tempfile")?;
        std::fs::write(tmp.path(), serde_json::to_vec_pretty(&manifest)?)?;
        let cmd = OpsCommand::new(
            "kubectl",
            vec![
                plain("create"),
                plain("-f"),
                plain(tmp.path().to_string_lossy().into_owned()),
                plain("-n"),
                plain(&self.opts.common.namespace),
                plain("-o"),
                plain("json"),
            ],
        );
        let (ok, out, err) = self.run(&cmd)?;
        if !ok {
            if namespace_is_absent(&err, &self.opts.common.namespace) {
                let reason = super::verbs::failure_reason(&err);
                bail!(
                    "namespace {} does not exist, so upgrade ownership cannot be created: {reason}; establish the install with `curie cluster up` first",
                    self.opts.common.namespace,
                );
            }
            if create_lost_race(&err) {
                let diagnostic = self.checkpoint_diagnostic();
                bail!(
                    "could not acquire upgrade ownership: {}; {diagnostic}; wait for the current upgrade to finish, or verify that its holder has stopped before manual recovery",
                    super::verbs::failure_reason(&err)
                );
            }
            bail!(
                "could not create upgrade ownership: {}",
                super::verbs::failure_reason(&err)
            );
        }
        let observation = parse_checkpoint_observation(&out, "upgrade ownership create")
            .context("ownership may remain because the server response cannot be trusted")?;
        self.validate_owned_observation(&observation, "upgrade ownership create", None)
            .context("ownership may remain because the server response cannot be trusted")?;
        self.adopt_checkpoint_observation(observation);
        Ok(())
    }

    /// The version an available local chart declares for itself.
    fn declared_chart_version(&self, chart: &str) -> Result<Option<String>> {
        let cmd = OpsCommand::new(
            "helm",
            vec![plain("show"), plain("chart"), plain(chart.to_string())],
        );
        let (ok, out, err) = self.run(&cmd)?;
        if !ok {
            bail!("could not read chart metadata for {chart}: {}", err.trim());
        }
        let doc: serde_json::Value =
            serde_norway::from_str(&out).context("chart metadata is malformed")?;
        Ok(match doc.get("version") {
            Some(serde_json::Value::String(version)) => Some(version.clone()),
            Some(serde_json::Value::Number(version)) => Some(version.to_string()),
            _ => None,
        })
    }

    /// R1: the pre-mutation half of the target pin, for a local chart only.
    fn chart_pin_refusal(&self) -> Option<String> {
        let chart = self.chart_ref();
        if !self.opts.chart.is_available_local() {
            return None;
        }
        let to = &self.opts.to;
        match self.declared_chart_version(chart) {
            Ok(Some(version)) if version == *to => None,
            Ok(Some(version)) => Some(format!(
                "chart {chart} declares version {version}, so it cannot install the requested {to}; \
                 point --chart at a chart whose version is {to}, or upgrade --to {version}"
            )),
            Ok(None) => Some(format!(
                "chart {chart} declares no version, so --to {to} cannot be pinned"
            )),
            Err(error) => Some(format!("{error:#}")),
        }
    }

    /// Available pre-mutation refusals and the migrated overlay are computed
    /// ONCE before any phase runs (Ruling 8.8). Apply then hands Helm exactly
    /// this overlay. A cold release dry-run keeps only target-dependent checks
    /// pending; retained configuration checks still run.
    fn compute_pre_mutation(&mut self) {
        self.chart_refusal = self.chart_pin_refusal();
        match self.retained_overlay() {
            Ok(Some((overlay, schema_plan))) => {
                self.overlay = Some(overlay);
                self.schema_plan = Some(schema_plan);
            }
            Ok(None) => {}
            Err(error) => self.config_refusal = Some(format!("{error:#}")),
        }
        if self.opts.forward_only {
            match merge_forward_only(self.overlay.as_deref()) {
                Ok(overlay) => self.overlay = Some(overlay),
                Err(error) => {
                    if self.config_refusal.is_none() {
                        self.config_refusal = Some(format!("{error:#}"));
                    }
                }
            }
        }
        if self.opts.chart.pending_release().is_none() {
            self.compute_schema_compat();
        }
        self.compute_runner_layer_clears();
    }

    /// Which layered agents the upgrade leaves on a stale base (#3218).
    ///
    /// The deploy guard guarantees every bound layer was built on the current
    /// installation's runner, so they all stop matching exactly when the
    /// target runner's digest differs from the current one. A release with no
    /// layered agent costs no extra read. Either runner being unknown counts
    /// as a change: keeping an old layer under a new worker is the failure this
    /// exists to prevent, and running the platform runner is always servable.
    fn compute_runner_layer_clears(&mut self) {
        let Some(overlay) = self
            .overlay
            .as_deref()
            .and_then(|o| serde_json::from_str::<serde_json::Value>(o).ok())
        else {
            return;
        };
        let layered = crate::cluster_secrets::layered_agents(&overlay);
        if layered.is_empty() {
            return;
        }
        let current = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(crate::cluster_secrets::installed_runner(&self.opts.common))
        })
        .ok()
        .map(|(_, pinned)| pinned);
        let chart_default = self.target_chart_runner_values();
        let target = target_runner_ref(chart_default.as_ref(), &overlay, &self.opts.to).and_then(
            |reference| {
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current()
                        .block_on(crate::cluster_secrets::pin_runner_reference(&reference))
                })
                .ok()
            },
        );
        self.runner_layer_clears = crate::cluster_secrets::layers_stopping_to_match(
            &layered,
            current.as_deref(),
            target.as_deref(),
        );
    }

    /// The target chart's own `agentSandbox.runner` defaults, when the chart
    /// is available to read. A pending release asset is not.
    fn target_chart_runner_values(&self) -> Option<serde_json::Value> {
        if self.opts.chart.pending_release().is_some() {
            return None;
        }
        let mut args = vec![plain("show"), plain("values"), plain(self.chart_ref())];
        if self.opts.chart.uses_helm_version() {
            args.push(plain("--version"));
            args.push(plain(&self.opts.to));
        }
        let (ok, out, _) = self.run(&OpsCommand::new("helm", args)).ok()?;
        if !ok {
            return None;
        }
        serde_norway::from_str(&out).ok()
    }

    fn compute_schema_compat(&mut self) {
        let live = match self.probe_live_revision() {
            Ok(rev) => rev,
            Err(reason) => {
                self.store_schema_decision(crate::schema_compat::refuse_with_reason(
                    None,
                    None,
                    Vec::new(),
                    reason,
                    self.opts.forward_only,
                    None,
                ));
                return;
            }
        };
        let source = self.source_head(live.as_deref());
        let target = match self.render_target_metadata() {
            Ok(target) => target,
            Err(reason) => {
                self.store_schema_decision(crate::schema_compat::refuse_with_reason(
                    live.as_deref(),
                    None,
                    Vec::new(),
                    reason,
                    self.opts.forward_only,
                    source.as_deref(),
                ));
                return;
            }
        };
        let pending = match crate::schema_compat::pending_revisions(live.as_deref(), &target) {
            Ok(pending) => pending,
            Err(reason) => {
                self.store_schema_decision(crate::schema_compat::refuse_with_reason(
                    live.as_deref(),
                    Some(&target),
                    Vec::new(),
                    reason,
                    self.opts.forward_only,
                    source.as_deref(),
                ));
                return;
            }
        };
        let decision = crate::schema_compat::plan_upgrade(
            live.as_deref(),
            &target,
            &pending,
            self.opts.forward_only,
            source.as_deref(),
        );
        self.store_schema_decision(decision);
    }

    fn source_head(&self, live: Option<&str>) -> Option<String> {
        crate::schema_window::window_for(self.current.as_deref().unwrap_or(""))
            .map(|window| window.schema_head)
            .or_else(|| live.map(ToOwned::to_owned))
    }

    fn store_schema_decision(&mut self, decision: crate::schema_compat::CompatDecision) {
        if decision.action == "refuse" {
            self.schema_refusal = Some(decision.reason.clone());
        }
        self.schema_decision = Some(crate::schema_compat::render_decision(&decision));
    }

    fn probe_live_revision(&self) -> Result<Option<String>, String> {
        let cmd = super::verbs::live_schema_revision_cmd(&self.opts.common);
        let (ok, out, err) = match self.run(&cmd) {
            Ok(v) => v,
            Err(error) => {
                return Err(crate::schema_window::redact_probe_text(&format!(
                    "{error:#}"
                )));
            }
        };
        if ok {
            return match crate::schema_window::parse_alembic_current_output(&out) {
                Some(rev) => Ok(Some(rev)),
                None if self.current.is_none() => Ok(None),
                None => Err("the API pod did not report a live database revision".into()),
            };
        }
        let redacted = crate::schema_window::redact_probe_text(err.trim());
        if self.current.is_none() && api_workload_missing(&err) {
            return Ok(None);
        }
        Err(if redacted.is_empty() {
            "could not read the live database revision from the API pod".into()
        } else {
            format!("could not read the live database revision from the API pod: {redacted}")
        })
    }

    fn render_target_metadata(&self) -> Result<crate::schema_compat::TargetMetadata, String> {
        let chart = self.chart_ref();
        let mut args = vec![
            plain("template"),
            plain(&self.opts.common.release),
            plain(chart),
            plain("--show-only"),
            plain("templates/schema-compat.yaml"),
            plain("-n"),
            plain(&self.opts.common.namespace),
        ];
        if self.opts.chart.uses_helm_version() {
            args.push(plain("--version"));
            args.push(plain(&self.opts.to));
        }
        let tmp = tempfile::NamedTempFile::new().ok();
        if let (Some(overlay), Some(tmp)) = (&self.overlay, &tmp) {
            if std::fs::write(tmp.path(), overlay).is_ok() {
                args.push(plain("-f"));
                args.push(plain(tmp.path().to_string_lossy().into_owned()));
            }
        }
        let cmd = OpsCommand::new("helm", args);
        let (ok, out, err) = match self.run(&cmd) {
            Ok(v) => v,
            Err(error) => {
                return Err(crate::schema_window::redact_probe_text(&format!(
                    "{error:#}"
                )));
            }
        };
        if !ok {
            let redacted = crate::schema_window::redact_probe_text(err.trim());
            return Err(format!(
                "could not render target schema compatibility metadata: {redacted}"
            ));
        }
        crate::schema_compat::parse_target_metadata(&out)
    }

    fn failed_schema_output(&self) -> ClusterUpgradeOutput {
        ClusterUpgradeOutput::Completed {
            status: "failed".into(),
            phase: UpgradePhase::Validate.as_str().into(),
            target_version: self.opts.to.clone(),
            from_version: self.current.clone(),
            known_good_version: self.known_good.clone(),
            resumed: self.record.as_ref().map(|r| r.resumed).unwrap_or(false),
            previous_serving: self.serving_previous(),
            unchanged: false,
            plan: self
                .record
                .as_ref()
                .map(|r| r.plan.clone())
                .unwrap_or_default(),
            convergence: None,
            canary: None,
            fail_forward: None,
            compatibility: self.schema_decision.clone(),
        }
    }

    /// R7: read the retained overlay, migrate it (#2299) and keep the result.
    /// `Ok(None)` means nothing was retained -- a first install.
    fn retained_overlay(&self) -> Result<Option<(String, String)>> {
        let values_cmd = OpsCommand::new(
            "helm",
            vec![
                plain("get"),
                plain("values"),
                plain(&self.opts.common.release),
                plain("-n"),
                plain(&self.opts.common.namespace),
                plain("-o"),
                plain("yaml"),
            ],
        );
        let (ok, out, err) = self.run(&values_cmd)?;
        if !ok {
            // Fail closed exactly like `cluster up` (`up.rs`
            // `helm_release_is_absent`): only Helm positively reporting that the
            // release does not exist means "nothing retained". Any other stderr
            // that merely mentions "not found" leaves the release state unknown,
            // and treating it as absent would run `helm upgrade` with no `-f`,
            // silently dropping every retained operator value.
            if super::verbs::failure_reason(&err) != "Error: release: not found" {
                bail!("could not read retained helm values: {}", err.trim());
            }
            return Ok(None);
        }
        if out.trim().is_empty() {
            return Ok(None);
        }
        let values: serde_json::Value =
            serde_norway::from_str(&out).context("retained helm values are malformed")?;
        // Unlike `up.rs`, the installed chart version is known here, so
        // `infer_schema_version` is not guessing when the overlay predates
        // `config.schemaVersion`.
        let outcome =
            crate::config_migrate::migrate_installed_config(values, self.current.as_deref())?;
        let schema_plan = crate::config_migrate::redacted_upgrade_plan(&outcome)
            .into_iter()
            .next()
            .unwrap_or_default();
        Ok(Some((helm_values_document(&outcome.values)?, schema_plan)))
    }

    /// The chart version the release reports. `scripts/check-version-consistency.sh`
    /// is required on every PR and release and asserts Chart.yaml `version` ==
    /// `appVersion` == the CLI version, so this one field is the whole answer
    /// and no appVersion branch is needed (driver Ruling 1). Helm status exposes
    /// a numeric release revision, while Helm metadata exposes the chart version.
    fn inspect_version(&self) -> Option<String> {
        let cmd = OpsCommand::new(
            "helm",
            vec![
                plain("get"),
                plain("metadata"),
                plain(&self.opts.common.release),
                plain("-n"),
                plain(&self.opts.common.namespace),
                plain("-o"),
                plain("json"),
            ],
        );
        let (ok, out, _) = self.run(&cmd).ok()?;
        if !ok {
            return None;
        }
        let v: serde_json::Value = serde_json::from_str(&out).ok()?;
        v.pointer("/version")
            .and_then(|x| x.as_str())
            .map(ToOwned::to_owned)
    }

    fn persist_record(&mut self, record: &UpgradeRecord) -> Result<()> {
        let json = serde_json::to_string(record)?;
        if let Some(secret) = &self.secret {
            if json.contains(secret) {
                bail!("refusing to persist an unredacted credential in the upgrade checkpoint");
            }
        }
        let resource_version = self
            .checkpoint_resource_version
            .as_ref()
            .context("upgrade checkpoint ownership has no trusted resourceVersion")?;
        let record_operation = if self.checkpoint_data_present {
            serde_json::json!({
                "op": "add",
                "path": "/data/record",
                "value": json,
            })
        } else {
            serde_json::json!({
                "op": "add",
                "path": "/data",
                "value": {"record": json},
            })
        };
        let patch = serde_json::json!([
            {
                "op": "test",
                "path": "/metadata/resourceVersion",
                "value": resource_version,
            },
            {
                "op": "test",
                "path": UPGRADE_HOLDER_POINTER,
                "value": self.holder,
            },
            record_operation,
        ]);
        let (ok, out, err) = self.run_checkpoint_patch(&patch)?;
        if !ok {
            let reason = self.redact(super::verbs::failure_reason(&err));
            let diagnostic = self.redact(&self.checkpoint_diagnostic());
            bail!("could not persist the upgrade checkpoint: {reason}; {diagnostic}");
        }
        let observation = parse_checkpoint_observation(&out, "upgrade checkpoint persistence")?;
        self.validate_owned_observation(
            &observation,
            "upgrade checkpoint persistence",
            Some(&json),
        )?;
        self.adopt_checkpoint_observation(observation);
        Ok(())
    }

    fn release_ownership(&mut self) -> Result<()> {
        let resource_version = self
            .checkpoint_resource_version
            .as_ref()
            .context("upgrade ownership has no trusted resourceVersion for release")?;
        let patch = serde_json::json!([
            {
                "op": "test",
                "path": "/metadata/resourceVersion",
                "value": resource_version,
            },
            {
                "op": "test",
                "path": UPGRADE_HOLDER_POINTER,
                "value": self.holder,
            },
            {
                "op": "remove",
                "path": UPGRADE_ACTION_POINTER,
            },
            {
                "op": "remove",
                "path": UPGRADE_HOLDER_POINTER,
            },
        ]);
        let (ok, out, err) = self.run_checkpoint_patch(&patch)?;
        if !ok {
            let reason = self.redact(super::verbs::failure_reason(&err));
            let diagnostic = self.redact(&self.checkpoint_diagnostic());
            bail!("could not release upgrade ownership: {reason}; {diagnostic}");
        }
        let observation = match parse_checkpoint_observation(&out, "upgrade ownership release") {
            Ok(observation) => observation,
            Err(error) => {
                let diagnostic = self.redact(&self.checkpoint_diagnostic());
                bail!(
                    "could not confirm upgrade ownership release from the server response: {error:#}; {diagnostic}"
                );
            }
        };
        if let Some(annotations) = observation.annotations.as_ref() {
            if annotations.contains_key(UPGRADE_HOLDER_ANNOTATION)
                || annotations.contains_key(UPGRADE_ACTION_ANNOTATION)
            {
                let diagnostic = self.redact(&self.checkpoint_diagnostic());
                bail!(
                    "could not confirm upgrade ownership release because the server response still contains ownership annotations; {diagnostic}"
                );
            }
        }
        self.adopt_checkpoint_observation(observation);
        self.checkpoint_resource_version = None;
        Ok(())
    }

    fn helm_upgrade(&self, to: &str) -> Result<()> {
        let tmp = tempfile::NamedTempFile::new().context("upgrade values tempfile")?;
        let values = match &self.overlay {
            Some(overlay) => {
                std::fs::write(tmp.path(), overlay)?;
                Some(tmp.path().to_string_lossy().into_owned())
            }
            None => None,
        };
        let args: Vec<_> = helm_upgrade_argv(
            &self.opts,
            to,
            self.current.is_none(),
            values.as_deref(),
            &self.runner_layer_clears,
        )
        .into_iter()
        .skip(1)
        .map(plain)
        .collect();
        let cmd = OpsCommand::new("helm", args);
        let (ok, _, err) = self.run(&cmd)?;
        if !ok {
            bail!("helm upgrade to {to} failed: {}", err.trim());
        }
        Ok(())
    }

    /// R3: the integrated observer answers this, not a hand-rolled
    /// `kubectl get deploy,sts,ds` read. `super::convergence` compares live
    /// containers, generations, replicas, selectors and Helm hooks against
    /// Helm's own retained target manifest; every sub-flag below is derived
    /// from the facet that observation genuinely disproved.
    fn live_convergence(&self) -> Result<ConvergenceVerdict> {
        let observation = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(super::convergence::wait_for_observation(&self.opts.common))
        });
        Ok(ConvergenceVerdict {
            convergence: convergence_from(&observation),
            issues: observation.issues.clone(),
        })
    }

    fn live_canary(&self) -> Result<Canary> {
        // R4: re-read here. Apply already set `current` from its own
        // post-condition read, so comparing against `current` would be a
        // self-comparison, and the release can move between the two phases.
        // Convergence is not re-observed: the Converge phase has already
        // failed the run unless it was exact.
        Ok(Canary {
            passed: self.inspect_version().as_deref() == Some(self.opts.to.as_str()),
        })
    }

    /// The DrainPreflight worker-reachability check. The real #2010 drain
    /// gate is the chart's pre-upgrade Job, which fires during Apply and is
    /// observed afterward at Converge (issue #2830) -- this method cannot see
    /// it and does not claim to. It only probes the worker Deployment:
    /// success or a NotFound skip proceeds; any other failure stays pending
    /// until the phase budget expires. Replica counts are not parsed, because
    /// the worker stays scheduled during this preflight.
    fn live_drain_preflight(&self) -> Result<bool> {
        tokio::task::block_in_place(|| {
            let budget = std::env::var(DRAIN_TIMEOUT_ENV)
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(DRAIN_TIMEOUT_DEFAULT_SECS);
            let deadline = Instant::now() + Duration::from_secs(budget);
            let mut probed = false;
            loop {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if probed && remaining.is_zero() {
                    return Ok(false);
                }
                let mut args = vec![
                    plain("get"),
                    plain("deploy"),
                    plain(format!("{}-worker", self.opts.common.release)),
                    plain("-n"),
                    plain(&self.opts.common.namespace),
                ];
                if !remaining.is_zero() {
                    args.push(plain(format!(
                        "--request-timeout={}s",
                        remaining.as_secs().max(1)
                    )));
                }
                let cmd = OpsCommand::new("kubectl", args);
                let (ok, _, err) = self.run(&cmd)?;
                probed = true;
                if let Some(reachable) = drain_preflight_observation(ok, &err) {
                    return Ok(reachable);
                }
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Ok(false);
                }
                std::thread::sleep(DRAIN_POLL_INTERVAL.min(remaining));
            }
        })
    }
}

/// Map one worker-reachability probe onto proceed / pending. This is the
/// DrainPreflight decision, not an observation of the real drain gate
/// (issue #2830).
///
/// `Some(true)` is a successful get or a NotFound skip (empty cluster).
/// `None` is still pending, so the caller retries until the phase budget
/// expires. Probe failure is not a terminal `Some(false)`; budget expiry is.
fn drain_preflight_observation(ok: bool, stderr: &str) -> Option<bool> {
    if ok || api_workload_missing(stderr) {
        Some(true)
    } else {
        None
    }
}

impl UpgradeDriver for LiveHost {
    fn current(&self) -> Option<String> {
        self.current.clone()
    }
    fn set_current(&mut self, version: Option<String>) {
        self.current = version;
    }
    fn known_good(&self) -> Option<String> {
        self.known_good.clone()
    }
    fn set_known_good(&mut self, version: Option<String>) {
        self.known_good = version;
    }
    fn load_record(&self) -> Option<UpgradeRecord> {
        self.record.clone()
    }
    fn store_record(&mut self, record: UpgradeRecord) -> Result<()> {
        self.persist_record(&record)?;
        self.record = Some(record);
        Ok(())
    }
    fn schema_plan(&self) -> Option<String> {
        self.schema_plan.clone()
    }
    fn retained_values(&self) -> bool {
        self.overlay.is_some()
    }
    fn runner_layer_clears(&self) -> Vec<String> {
        self.runner_layer_clears.clone()
    }
    fn validate_refusal(&self) -> Option<String> {
        self.chart_refusal
            .clone()
            .or_else(|| self.config_refusal.clone())
            .or_else(|| self.schema_refusal.clone())
    }
    fn refuse_config(&self) -> bool {
        self.config_refusal.is_some()
    }
    fn refuse_schema(&self) -> bool {
        self.schema_refusal.is_some()
    }
    fn compatibility_decision(&self) -> Option<serde_json::Value> {
        self.schema_decision.clone()
    }
    fn observed_version(&self) -> Option<String> {
        self.inspect_version()
    }
    fn drain_preflight_once(&mut self) -> Result<bool> {
        self.live_drain_preflight()
    }
    fn apply_target(&mut self, to: &str) -> Result<()> {
        self.helm_upgrade(to)?;
        // R2: what the release reports, not what was requested, is what was
        // installed. Helm can exit 0 on a revision that never moved.
        let observed = self.inspect_version();
        match observed.as_deref() {
            Some(version) if version == to => {
                self.set_current(observed);
                Ok(())
            }
            Some(version) => bail!(
                "helm upgrade to {to} reported success, but the release still reports {version}"
            ),
            None => {
                bail!("helm upgrade to {to} reported success, but the release reports no version")
            }
        }
    }
    fn observe_convergence(&self) -> Result<ConvergenceVerdict> {
        self.live_convergence()
    }
    fn run_canary(&self) -> Result<Canary> {
        self.live_canary()
    }
    fn serving_previous(&self) -> bool {
        match (&self.current, &self.known_good) {
            (Some(cur), Some(kg)) => cur == kg,
            (None, _) => true,
            _ => true,
        }
    }
    fn fail_at(&self) -> Option<UpgradePhase> {
        self.fail_at
    }
    fn interrupt_after(&self) -> Option<UpgradePhase> {
        self.interrupt_after
    }
}

/// Live `curie cluster upgrade` entry point.
pub async fn upgrade(opts: UpgradeOpts) -> Result<ClusterUpgradeOutput> {
    if !opts.common.dry_run && opts.chart.pending_release().is_some() {
        bail!("a pending release chart is only valid for a dry run; download the chart before starting a real upgrade");
    }
    if opts.common.dry_run {
        require_on_path("helm").ok();
        let mut live = LiveHost::new(opts.clone())?;
        live.current = live.inspect_version();
        live.known_good = live.current.clone();
        // Retained configuration is available without the target chart, so a
        // dry run always computes it. Chart metadata and schema compatibility
        // run when the selected local chart or Helm reference is available;
        // a cold release asset records those checks as pending instead (#2301).
        live.compute_pre_mutation();
        // A refusing dry run already failed with its plan as the report.
        return run_lifecycle_inner(opts, &mut live).await;
    }

    require_on_path("helm")?;
    require_on_path("kubectl")?;

    if !opts.yes
        && !super::verbs::confirm(&format!(
            "This upgrades release '{}' in namespace '{}' to {}. Continue? [y/N] ",
            opts.common.release, opts.common.namespace, opts.to
        ))?
    {
        bail!("upgrade aborted");
    }

    let mut live = LiveHost::new(opts.clone())?;
    live.acquire_ownership()?;
    let setup = live.parse_acquired_record();
    let result = match setup {
        Ok(record) => {
            live.record = record;
            live.current = live.inspect_version();
            live.known_good = live
                .record
                .as_ref()
                .and_then(|record| record.known_good_version.clone())
                .or_else(|| live.current.clone());
            live.compute_pre_mutation();
            if let Some(notice) = runner_layer_notice(&live.runner_layer_clears) {
                crate::ui::ui().warn(&notice);
            }
            run_lifecycle_inner(opts, &mut live).await
        }
        Err(error) => Err(error),
    };
    let result = wrap_schema_refusal(&live, result);
    finish_owned_upgrade(&mut live, result)
}

fn wrap_schema_refusal(
    live: &LiveHost,
    result: Result<ClusterUpgradeOutput>,
) -> Result<ClusterUpgradeOutput> {
    match result {
        Ok(out) => Ok(out),
        Err(err) if live.schema_refusal.is_some() => {
            Err(crate::ui::ui().failed_report(&live.failed_schema_output(), err))
        }
        Err(err) => Err(err),
    }
}

fn finish_owned_upgrade(
    live: &mut LiveHost,
    result: Result<ClusterUpgradeOutput>,
) -> Result<ClusterUpgradeOutput> {
    let release = live.release_ownership();
    match (result, release) {
        (Ok(output), Ok(())) => Ok(output),
        (Ok(mut output), Err(error)) => {
            let failed = matches!(
                &output,
                ClusterUpgradeOutput::Completed { status, .. } if status == "failed"
            );
            if failed {
                if let ClusterUpgradeOutput::Completed {
                    fail_forward: Some(fail_forward),
                    ..
                } = &mut output
                {
                    fail_forward.reason = format!("{}; {error:#}", fail_forward.reason);
                }
                Err(crate::ui::ui().failed_report(&output, error))
            } else {
                Err(error)
            }
        }
        (Err(error), Ok(())) => Err(error),
        (Err(error), Err(release_error)) => {
            let (message, remedy) = crate::exit::present_error(&error);
            Err(crate::exit::operator_context(
                error,
                format!("{message}; additionally, {release_error:#}"),
                remedy,
            ))
        }
    }
}

/// Load the upgrade status view for `cluster status`.
pub async fn load_upgrade_status(
    namespace: &str,
    release: &str,
    fallback_known_good: Option<String>,
) -> UpgradeStatusView {
    let cmd = OpsCommand::new(
        "kubectl",
        vec![
            plain("get"),
            plain("configmap"),
            plain(checkpoint_name(release)),
            plain("-n"),
            plain(namespace),
            plain("-o"),
            plain("jsonpath={.data.record}"),
        ],
    );
    let (ok, out, _) = match run_capture(&cmd).await {
        Ok(v) => v,
        Err(_) => return UpgradeStatusView::idle(fallback_known_good),
    };
    if !ok || out.trim().is_empty() {
        return UpgradeStatusView::idle(fallback_known_good);
    }
    match serde_json::from_str::<UpgradeRecord>(&out) {
        Ok(record) => status_from_record(Some(&record), fallback_known_good),
        Err(_) => UpgradeStatusView::idle(fallback_known_good),
    }
}

#[cfg(test)]
mod hook_tests {
    use super::*;

    fn opts(namespace: &str, release: &str) -> UpgradeOpts {
        UpgradeOpts {
            common: CommonOpts {
                namespace: namespace.into(),
                release: release.into(),
                dry_run: false,
            },
            to: "0.9.0".into(),
            chart: UpgradeChart::AvailableLocal("charts/curie".into()),
            yes: true,
            forward_only: false,
        }
    }

    struct EnvGuard {
        key: &'static str,
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            std::env::remove_var(self.key);
        }
    }

    fn set_env(key: &'static str, value: &str) -> EnvGuard {
        std::env::set_var(key, value);
        EnvGuard { key }
    }

    #[tokio::test]
    async fn unset_or_empty_hook_is_none() {
        let _lock = crate::PROCESS_ENV_LOCK.lock().await;
        std::env::remove_var(TEST_FAIL_AT_ENV);
        let owned = opts("acme-2590", "t2590");
        assert!(test_hook_phase(&owned, TEST_FAIL_AT_ENV)
            .expect("unset hook")
            .is_none());
        let _guard = set_env(TEST_FAIL_AT_ENV, "  ");
        assert!(test_hook_phase(&owned, TEST_FAIL_AT_ENV)
            .expect("empty hook")
            .is_none());
    }

    #[tokio::test]
    async fn soak_namespace_refuses_fail_at() {
        let _lock = crate::PROCESS_ENV_LOCK.lock().await;
        let _guard = set_env(TEST_FAIL_AT_ENV, "apply");
        let err = test_hook_phase(&opts("curie", "t2590"), TEST_FAIL_AT_ENV)
            .expect_err("soak namespace must refuse FAIL_AT");
        let shown = format!("{err:#}");
        assert!(
            shown.contains("soak") && shown.contains(TEST_FAIL_AT_ENV),
            "{shown}"
        );
    }

    #[tokio::test]
    async fn soak_release_refuses_interrupt_after() {
        let _lock = crate::PROCESS_ENV_LOCK.lock().await;
        let _guard = set_env(TEST_INTERRUPT_AFTER_ENV, "checkpoint");
        let err = test_hook_phase(&opts("acme-2590", "curie"), TEST_INTERRUPT_AFTER_ENV)
            .expect_err("soak release must refuse INTERRUPT_AFTER");
        let shown = format!("{err:#}");
        assert!(
            shown.contains("soak") && shown.contains(TEST_INTERRUPT_AFTER_ENV),
            "{shown}"
        );
    }

    #[tokio::test]
    async fn default_namespace_is_soak() {
        let _lock = crate::PROCESS_ENV_LOCK.lock().await;
        let _guard = set_env(TEST_FAIL_AT_ENV, "apply");
        test_hook_phase(&opts("default", "t2590"), TEST_FAIL_AT_ENV)
            .expect_err("namespace default is soak");
    }

    #[tokio::test]
    async fn owned_install_parses_fail_at_apply() {
        let _lock = crate::PROCESS_ENV_LOCK.lock().await;
        let _guard = set_env(TEST_FAIL_AT_ENV, "apply");
        let phase = test_hook_phase(&opts("acme-2590", "t2590"), TEST_FAIL_AT_ENV)
            .expect("owned FAIL_AT")
            .expect("apply");
        assert_eq!(phase, UpgradePhase::Apply);
    }

    #[tokio::test]
    async fn unknown_phase_is_refused() {
        let _lock = crate::PROCESS_ENV_LOCK.lock().await;
        let _guard = set_env(TEST_FAIL_AT_ENV, "not-a-phase");
        let err = test_hook_phase(&opts("acme-2590", "t2590"), TEST_FAIL_AT_ENV)
            .expect_err("unknown phase");
        assert!(format!("{err:#}").contains("unknown upgrade test phase"));
    }
}

#[cfg(test)]
mod drain_preflight_naming_tests {
    use super::*;

    /// Issue #2830: `live_drain_observation`'s old name and the phase's old
    /// `"drain"` label both claimed this check observed a drain it never
    /// watched. Going forward the phase must report its honest name.
    #[test]
    fn drain_preflight_serializes_under_its_honest_name() {
        assert_eq!(UpgradePhase::DrainPreflight.as_str(), "drain_preflight");
        let json = serde_json::to_string(&UpgradePhase::DrainPreflight).expect("serialize");
        assert_eq!(json, "\"drain_preflight\"");
    }

    /// A checkpoint a pre-#2830 binary persisted mid-upgrade carries the old
    /// `"drain"` phase name in its `completed` list. A binary carrying the
    /// rename must still resume it rather than fail to parse the checkpoint.
    #[test]
    fn upgrade_record_deserializes_legacy_drain_phase_name() {
        let legacy = serde_json::json!({
            "target_version": "0.9.0",
            "from_version": "0.8.6",
            "known_good_version": "0.8.6",
            "completed": ["plan", "validate", "drain"],
            "status": "in_progress",
            "plan": [],
            "drain_completed": true,
            "convergence": null,
            "canary": null,
            "fail_forward": null,
            "resumed": false,
        });
        let record: UpgradeRecord =
            serde_json::from_value(legacy).expect("legacy checkpoint must still deserialize");
        assert_eq!(
            record.completed,
            vec![
                UpgradePhase::Plan,
                UpgradePhase::Validate,
                UpgradePhase::DrainPreflight,
            ]
        );
    }

    /// The pure decision `live_drain_preflight` delegates to: a probe failure
    /// stays pending (retried) rather than being treated as "not drained",
    /// since this check never observed drain state to begin with.
    #[test]
    fn drain_preflight_observation_treats_probe_failure_as_pending() {
        assert_eq!(drain_preflight_observation(true, ""), Some(true));
        assert_eq!(
            drain_preflight_observation(
                false,
                "Error from server (NotFound): deployments.apps \"curie-worker\" not found"
            ),
            Some(true),
            "an absent worker Deployment is an empty-cluster skip"
        );
        assert_eq!(
            drain_preflight_observation(false, "connection refused"),
            None,
            "an unreadable API is still pending, not a terminal failure"
        );
    }
}

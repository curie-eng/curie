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
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use super::command::{mask_secret, plain, require_on_path, run_capture, CommonOpts, OpsCommand};
use super::verbs::release_fullname_from_values;

// These bound the DrainPreflight worker-reachability probe below, not the
// real #2010 drain gate (that gate's own timeout is
// `worker.upgradeDrain.timeoutSeconds` in the chart).
const DRAIN_TIMEOUT_ENV: &str = "CURIE_UPGRADE_DRAIN_TIMEOUT_SECS";
const DRAIN_TIMEOUT_DEFAULT_SECS: u64 = 30;
const HELM_TIMEOUT_DEFAULT_SECS: u64 = 15 * 60;
const MINIMUM_HELM_TIMEOUT_ANNOTATION: &str = "curie.ai/minimum-helm-timeout-seconds";
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
    /// @spec CLUSTER-VALUES-FILES c1-c4: immutable operator values.
    pub file_values: Option<super::PrivateHelmValues>,
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
    /// Phases passed over without executing: a same-version rerun's
    /// DrainPreflight through Apply, a fresh install's DrainPreflight. Kept
    /// apart from `completed` so the record never claims work that did not
    /// run (#2861); absent from checkpoints older binaries persisted.
    #[serde(default)]
    skipped: Vec<UpgradePhase>,
    /// The phase that stopped the run. `completed` stays the phases that
    /// finished, so cluster status can still name the failure (#3849).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    failed_phase: Option<UpgradePhase>,
    status: String,
    plan: Vec<String>,
    /// Whether the DrainPreflight worker-reachability check has already run
    /// once for this attempt. Not an observation of the real #2010 drain
    /// gate, which `queues_drained` reports from Converge instead.
    drain_completed: bool,
    convergence: Option<Convergence>,
    canary: Option<Canary>,
    /// The runner each layered agent's SandboxTemplate must render, taken
    /// from the layer plan when the record is created (#4321). A resume
    /// re-plans from values Apply already changed, so a cleared agent would
    /// drop out of a fresh plan; the Canary checks this recorded set instead.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    runner_canary: Option<Vec<(String, ExpectedRunner)>>,
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
    runner_layers: RunnerLayerPlan,
    template_images: Option<ObservedTemplates>,
    apply_error: Option<String>,
    applied: bool,
    /// Agents whose claims Apply retired (#3422), in retirement order.
    pub retired_claims: Vec<String>,
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
            runner_layers: RunnerLayerPlan::default(),
            template_images: None,
            apply_error: None,
            applied: false,
            retired_claims: Vec::new(),
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

    /// Owner-built agents whose layered runner the upgrade will stop
    /// matching, so it clears them (#3218).
    pub fn with_runner_layer_clears(mut self, agents: &[&str]) -> Self {
        self.runner_layers.clears = agents
            .iter()
            .map(|a| (a.to_string(), LayerClearReason::OwnerBuilt))
            .collect();
        self
    }

    /// What the upgrade does to each layered agent (#4321).
    pub fn with_runner_layer_plan(mut self, plan: RunnerLayerPlan) -> Self {
        self.runner_layers = plan;
        self
    }

    /// The SandboxTemplate images the canary observes for release fullname
    /// `curie`: the platform template, then each `(agent, image)` (#4321).
    /// Unset, the canary compares no templates.
    pub fn with_template_images(mut self, platform: &str, agents: &[(&str, &str)]) -> Self {
        let mut items = vec![("curie-runner".to_string(), Some(platform.to_string()))];
        items.extend(agents.iter().map(|(agent, image)| {
            (
                format!("curie-agent-{agent}-runner"),
                Some(image.to_string()),
            )
        }));
        self.template_images = Some(ObservedTemplates { items });
        self
    }

    pub fn with_retained_values(mut self) -> Self {
        self.retained_values = true;
        self
    }

    /// Helm apply returns this error and does not move the release (#3849).
    pub fn apply_error(mut self, message: &str) -> Self {
        self.apply_error = Some(message.to_string());
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
    fn runner_layer_plan(&self) -> RunnerLayerPlan {
        self.runner_layers.clone()
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
        if let Some(message) = &self.apply_error {
            bail!("{message}");
        }
        self.applied = true;
        self.set_current(Some(to.to_string()));
        Ok(())
    }
    fn retire_runner_layer_claims(&mut self) -> Result<()> {
        self.retired_claims
            .extend(self.runner_layers.retired_agents());
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
    fn run_canary(&self, expected: &[(String, ExpectedRunner)]) -> Result<CanaryVerdict> {
        let issues = self
            .template_images
            .as_ref()
            .map(|templates| template_canary_issues("curie", expected, templates))
            .unwrap_or_default();
        Ok(CanaryVerdict {
            canary: Canary {
                passed: self.canary_ok && issues.is_empty(),
            },
            issues,
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
            phase: r
                .failed_phase
                .map(|phase| phase.as_str().to_string())
                .or_else(|| r.completed.last().map(|p| p.as_str().to_string())),
            status: r.status.clone(),
            known_good_version: r.known_good_version.clone().or(fallback_known_good),
            target_version: Some(r.target_version.clone()),
        },
    }
}

fn remaining_after(record: &UpgradeRecord) -> Vec<UpgradePhase> {
    UpgradePhase::ALL
        .into_iter()
        .filter(|p| !record.completed.contains(p) && !record.skipped.contains(p))
        .collect()
}

fn plan_lines(
    opts: &UpgradeOpts,
    from: Option<&str>,
    secret: Option<&str>,
    schema_plan: Option<&str>,
    retained_values: bool,
    runner_layers: &RunnerLayerPlan,
    helm_timeout_seconds: u64,
) -> Vec<String> {
    let apply = helm_upgrade_argv(
        opts,
        &opts.to,
        from.is_none(),
        retained_values.then_some(RETAINED_VALUES_PLACEHOLDER),
        helm_timeout_seconds,
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
    ];
    lines.extend(
        runner_layer_retirements(&opts.common.namespace, &runner_layers.retired_agents())
            .iter()
            .map(OpsCommand::display),
    );
    lines.extend([
        "phase converge: exact images, generations, replicas, unavailable=0, hooks, queues, manifest"
            .into(),
        "phase canary: target-version smoke".into(),
        "phase commit: record known-good version".into(),
    ]);
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
    if let Some(rebound) = runner_layer_rebind_line(runner_layers, &opts.to) {
        lines.push(rebound);
    }
    lines.extend(runner_layer_warnings(runner_layers, opts));
    if let Some((source_url, cache_path)) = opts.chart.pending_release() {
        lines.push(format!(
            "phase validate pending: chart metadata, schema compatibility, and Helm timeout after release chart download from {source_url} to {cache_path}"
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

/// One `kubectl delete sandboxclaim` per agent whose runner layer the upgrade
/// clears or rebinds (#3422, #4321), the same retirement `cluster deploy` runs
/// (#3300).
fn runner_layer_retirements(namespace: &str, agents: &[String]) -> Vec<OpsCommand> {
    agents
        .iter()
        .map(|agent| crate::cluster_secrets::retire_claims_command(namespace, agent))
        .collect()
}

/// What an upgrade does to each `agentSandbox.runnerImages` binding (#3218,
/// #4321), in agent name order. Rebinds and clears change in the same
/// `helm upgrade`; kept bindings stay as they are.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RunnerLayerPlan {
    /// Stock agents rebound to the layer published for the target version.
    pub rebinds: Vec<(String, String)>,
    /// Agents whose binding is removed, so they run the platform runner.
    pub clears: Vec<(String, LayerClearReason)>,
    /// Bindings the upgrade leaves alone, because the runner does not change.
    pub kept: Vec<(String, String)>,
}

impl RunnerLayerPlan {
    /// Every agent whose binding changes, so whose live sandboxes are retired
    /// after Apply, in name order.
    pub fn retired_agents(&self) -> Vec<String> {
        let mut agents: Vec<String> = self
            .rebinds
            .iter()
            .map(|(agent, _)| agent.clone())
            .chain(self.clears.iter().map(|(agent, _)| agent.clone()))
            .collect();
        agents.sort();
        agents
    }

    /// The cleared agents whose layer their owner built.
    pub fn owner_built_clears(&self) -> Vec<String> {
        self.clears
            .iter()
            .filter(|(_, reason)| *reason == LayerClearReason::OwnerBuilt)
            .map(|(agent, _)| agent.clone())
            .collect()
    }

    /// The runner each layered agent's SandboxTemplate must render after the
    /// upgrade, in name order.
    pub fn canary_expectations(&self) -> Vec<(String, ExpectedRunner)> {
        let mut expected: Vec<(String, ExpectedRunner)> = self
            .rebinds
            .iter()
            .chain(self.kept.iter())
            .map(|(agent, image)| (agent.clone(), ExpectedRunner::Image(image.clone())))
            .chain(
                self.clears
                    .iter()
                    .map(|(agent, _)| (agent.clone(), ExpectedRunner::PlatformRunner)),
            )
            .collect();
        expected.sort_by(|a, b| a.0.cmp(&b.0));
        expected
    }

    /// Whether the upgrade changes no binding.
    pub fn is_empty(&self) -> bool {
        self.rebinds.is_empty() && self.clears.is_empty()
    }
}

/// Why an upgrade clears an agent's runner layer instead of rebinding it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LayerClearReason {
    /// The agent's owner built the layer, so only the owner can rebuild it.
    OwnerBuilt,
    /// No dark factory layer is published for the target version.
    StockLayerUnpublished,
    /// The layer published for the target version is built on another runner.
    StockBaseMismatch {
        published_base: String,
        target: String,
    },
    /// The registry could not answer which layer is published.
    StockRegistryUnreachable(String),
    /// The target runner digest is unknown, so no published base can match it.
    TargetRunnerUnknown,
}

/// The runner a layered agent's SandboxTemplate must render after the upgrade.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExpectedRunner {
    /// Exactly this digest-pinned layer.
    Image(String),
    /// The platform runner: no per-agent template, or one on the platform image.
    PlatformRunner,
}

/// The registry's answer for the dark factory layer published for `--to`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StockRunnerLookup {
    /// No affected binding is stock, so the registry was not asked.
    NotNeeded,
    Published(crate::examples::PublishedStockRunner),
    Unpublished,
    Unreachable(String),
}

/// Whether `binding` names the dark factory layer this project publishes:
/// the repository before `@` is exactly the published one. A lookalike
/// repository, another registry, or a tag-only binding is owner-built.
pub(crate) fn is_stock_layer(binding: &str) -> bool {
    binding.split_once('@').is_some_and(|(repository, _)| {
        repository == crate::examples::DARK_FACTORY_RUNNER_REPOSITORY
    })
}

/// The bindings the upgrade changes, given the current and target runners.
fn affected_bindings<'a>(
    bindings: &'a BTreeMap<String, String>,
    current: Option<&str>,
    target: Option<&str>,
) -> Vec<&'a String> {
    let layered: Vec<String> = bindings.keys().cloned().collect();
    let affected = crate::cluster_secrets::layers_stopping_to_match(&layered, current, target);
    bindings
        .keys()
        .filter(|agent| affected.contains(agent))
        .collect()
}

/// Whether planning needs the registry: a target runner is known and some
/// binding the upgrade changes is stock.
pub(crate) fn needs_stock_lookup(
    bindings: &BTreeMap<String, String>,
    current: Option<&str>,
    target: Option<&str>,
) -> bool {
    target.is_some()
        && affected_bindings(bindings, current, target)
            .into_iter()
            .any(|agent| is_stock_layer(&bindings[agent]))
}

/// The pure part of [`LiveHost::compute_runner_layers`] (#3218, #4321). The
/// affected set is [`crate::cluster_secrets::layers_stopping_to_match`]: the
/// deploy guard holds every bound layer on the current runner, so all of them
/// stop matching when the runner changes, and an unknown side counts as a
/// change. An affected stock binding is rebound only to the registry's answer
/// for `--to`, never to its own digest, and only when that layer's base is the
/// target runner (ADR 0173 decision 5). Every other affected binding clears.
pub(crate) fn plan_runner_layers(
    bindings: &BTreeMap<String, String>,
    current: Option<&str>,
    target: Option<&str>,
    stock: &StockRunnerLookup,
) -> RunnerLayerPlan {
    let affected = affected_bindings(bindings, current, target);
    let mut plan = RunnerLayerPlan::default();
    for (agent, binding) in bindings {
        if !affected.contains(&agent) {
            plan.kept.push((agent.clone(), binding.clone()));
            continue;
        }
        if !is_stock_layer(binding) {
            plan.clears
                .push((agent.clone(), LayerClearReason::OwnerBuilt));
            continue;
        }
        let Some(target) = target else {
            plan.clears
                .push((agent.clone(), LayerClearReason::TargetRunnerUnknown));
            continue;
        };
        let reason = match stock {
            StockRunnerLookup::Published(published)
                if crate::cluster_secrets::same_runner(&published.base_image, target) =>
            {
                plan.rebinds
                    .push((agent.clone(), published.layer_image.clone()));
                continue;
            }
            StockRunnerLookup::Published(published) => LayerClearReason::StockBaseMismatch {
                published_base: published.base_image.clone(),
                target: target.to_string(),
            },
            StockRunnerLookup::Unpublished => LayerClearReason::StockLayerUnpublished,
            StockRunnerLookup::Unreachable(detail) => {
                LayerClearReason::StockRegistryUnreachable(detail.clone())
            }
            StockRunnerLookup::NotNeeded => LayerClearReason::StockRegistryUnreachable(
                "the published layer was not looked up".into(),
            ),
        };
        plan.clears.push((agent.clone(), reason));
    }
    plan
}

/// The plan line naming every agent rebound to the dark factory layer
/// published for `to` (#4321). `None` when there are none.
pub(crate) fn runner_layer_rebind_line(plan: &RunnerLayerPlan, to: &str) -> Option<String> {
    if plan.rebinds.is_empty() {
        return None;
    }
    let pairs: Vec<String> = plan
        .rebinds
        .iter()
        .map(|(agent, image)| format!("{agent}={image}"))
        .collect();
    Some(format!(
        "runner layers rebound: the platform runner changes, so this upgrade binds {} to the \
         dark factory runner layer published for {to}, built on the target runner. Their live \
         sandboxes are retired after the helm upgrade",
        pairs.join(", ")
    ))
}

/// The operator-facing line naming every stock agent the upgrade clears, why,
/// and the stock remedy (#4321). Owner-built clears stay in
/// [`runner_layer_notice`]. `None` when there are none.
pub(crate) fn stock_runner_layer_notice(
    plan: &RunnerLayerPlan,
    namespace: &str,
    release: &str,
    to: &str,
) -> Option<String> {
    let stock: Vec<String> = plan
        .clears
        .iter()
        .filter_map(|(agent, reason)| {
            let why = match reason {
                LayerClearReason::OwnerBuilt => return None,
                LayerClearReason::StockLayerUnpublished => {
                    format!("the dark factory runner layer for {to} is not published")
                }
                LayerClearReason::StockBaseMismatch {
                    published_base,
                    target,
                } => format!(
                    "the layer published for {to} is built on {published_base}, not the target \
                     runner {target}"
                ),
                LayerClearReason::StockRegistryUnreachable(detail) => {
                    format!(
                        "the registry could not say which layer is published for {to}: {detail}"
                    )
                }
                LayerClearReason::TargetRunnerUnknown => {
                    "the target runner is unknown, so no published layer can be proven to match it"
                        .to_string()
                }
            };
            Some(format!(
                "{agent} ({why}; rebind with `curie cluster factory --namespace {namespace} \
                 --release {release} --runner-image \
                 {agent}={}@sha256:<digest>` using the digest of the layer published for {to}, \
                 or run `curie example dark-factory render --out <dir>` from a curie {to} CLI and \
                 then `curie cluster deploy --namespace {namespace} --release {release} \
                 --plugin-dir <dir> --agent {agent}`)",
                crate::examples::DARK_FACTORY_RUNNER_REPOSITORY
            ))
        })
        .collect();
    if stock.is_empty() {
        return None;
    }
    Some(format!(
        "stock runner layers: the platform runner changes and the dark factory runner layer of \
         these agent(s) cannot be rebound for {to}, so this upgrade clears \
         agentSandbox.runnerImages for them and they run the platform runner WITHOUT their \
         layer until rebound: {}. Their live sandboxes are retired after the helm upgrade",
        stock.join("; ")
    ))
}

/// The owner-built notice, then the stock notice, for every cleared agent.
fn runner_layer_warnings(plan: &RunnerLayerPlan, opts: &UpgradeOpts) -> Vec<String> {
    runner_layer_notice(&plan.owner_built_clears())
        .into_iter()
        .chain(stock_runner_layer_notice(
            plan,
            &opts.common.namespace,
            &opts.common.release,
            &opts.to,
        ))
        .collect()
}

/// The SandboxTemplates of one release as `(name, first container image)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedTemplates {
    items: Vec<(String, Option<String>)>,
}

/// Read `kubectl get sandboxtemplates -o json` into [`ObservedTemplates`].
pub(crate) fn parse_sandbox_templates(raw: &str) -> Result<ObservedTemplates> {
    let list: serde_json::Value =
        serde_json::from_str(raw).context("SandboxTemplate list is not JSON")?;
    let items = list
        .get("items")
        .and_then(|items| items.as_array())
        .context("SandboxTemplate list has no items")?;
    Ok(ObservedTemplates {
        items: items
            .iter()
            .filter_map(|item| {
                let name = item.pointer("/metadata/name")?.as_str()?.to_string();
                let image = item
                    .pointer("/spec/podTemplate/spec/containers/0/image")
                    .and_then(|image| image.as_str())
                    .map(str::to_string);
                Some((name, image))
            })
            .collect(),
    })
}

/// The effective `fullnameOverride` and `nameOverride`, as Helm merges values:
/// a key present in the overlay (even `""` or null) wins over the chart default.
pub(crate) fn effective_name_values(
    chart_defaults: Option<&serde_json::Value>,
    overlay: &serde_json::Value,
) -> serde_json::Value {
    let mut out = serde_json::Map::new();
    for key in ["fullnameOverride", "nameOverride"] {
        if let Some(value) = overlay
            .get(key)
            .or_else(|| chart_defaults.and_then(|defaults| defaults.get(key)))
        {
            out.insert(key.to_string(), value.clone());
        }
    }
    serde_json::Value::Object(out)
}

/// Compare each planned runner with the rendered SandboxTemplates (#4321).
/// Names come from the release's `fullname`, never from the templates that
/// happen to exist: the platform template is exactly `<fullname>-runner` and
/// an agent's is exactly `<fullname>-agent-<agent>-runner`, so `bot` never
/// matches `agent-bot`. A rebound or kept agent needs its template on the
/// planned image. A cleared agent may have no template, but one it has must
/// render the platform image.
pub(crate) fn template_canary_issues(
    fullname: &str,
    expected: &[(String, ExpectedRunner)],
    observed: &ObservedTemplates,
) -> Vec<String> {
    let platform_name = format!("{fullname}-runner");
    let platform = observed
        .items
        .iter()
        .find(|(name, _)| *name == platform_name);
    let shown = |image: Option<&String>| {
        image
            .cloned()
            .unwrap_or_else(|| "no runner image".to_string())
    };
    let mut issues = Vec::new();
    for (agent, runner) in expected {
        if platform.is_none() {
            issues.push(format!(
                "agent {agent}: the platform SandboxTemplate {platform_name} is missing, so the \
                 upgrade cannot confirm its runner"
            ));
            continue;
        }
        let name = format!("{fullname}-agent-{agent}-runner");
        let found = observed.items.iter().find(|(item, _)| *item == name);
        match (runner, found) {
            (ExpectedRunner::Image(image), None) => issues.push(format!(
                "agent {agent}: SandboxTemplate {name} is missing, but the upgrade planned {image}"
            )),
            (ExpectedRunner::Image(image), Some((_, rendered))) => {
                if rendered.as_deref() != Some(image.as_str()) {
                    issues.push(format!(
                        "agent {agent}: SandboxTemplate {name} renders {}, but the upgrade \
                         planned {image}",
                        shown(rendered.as_ref())
                    ));
                }
            }
            (ExpectedRunner::PlatformRunner, None) => {}
            (ExpectedRunner::PlatformRunner, Some((_, rendered))) => {
                let platform_image = platform.and_then(|(_, image)| image.as_ref());
                if rendered.is_none() || rendered.as_ref() != platform_image {
                    issues.push(format!(
                        "agent {agent}: SandboxTemplate {name} renders {}, but the upgrade \
                         planned the platform runner {}",
                        shown(rendered.as_ref()),
                        shown(platform_image)
                    ));
                }
            }
        }
    }
    issues
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
         `curie cluster deploy`. Their live sandboxes are retired after the helm upgrade, so \
         existing threads start fresh on the platform runner at their next turn",
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
    /// What this upgrade does to each layered agent (#3218, #4321). Apply
    /// rebinds or clears each changed `agentSandbox.runnerImages.<agent>` in
    /// the same `helm upgrade`.
    fn runner_layer_plan(&self) -> RunnerLayerPlan {
        RunnerLayerPlan::default()
    }
    /// Retire the SandboxClaims of every agent [`Self::runner_layer_plan`]
    /// rebinds or clears after Apply (#3422, #4321), so a live thread's next
    /// turn cold-starts on its new runner instead of keeping the old layer.
    fn retire_runner_layer_claims(&mut self) -> Result<()> {
        Ok(())
    }

    fn helm_timeout_seconds(&self) -> u64 {
        HELM_TIMEOUT_DEFAULT_SECS
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
    /// The Canary, checking each layered agent's template against `expected`
    /// (the record's stored expectations, or the host plan's when it has none).
    fn run_canary(&self, expected: &[(String, ExpectedRunner)]) -> Result<CanaryVerdict>;
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
    let same_version = from.as_deref() == Some(opts.to.as_str())
        && host.known_good().as_deref() == Some(opts.to.as_str());
    let mut plan = plan_lines(
        &opts,
        from.as_deref(),
        host.secret(),
        host.schema_plan().as_deref(),
        host.retained_values(),
        &host.runner_layer_plan(),
        host.helm_timeout_seconds(),
    );
    if same_version {
        // #2861: the rerun runs no Helm upgrade, so the plan must not show one.
        plan.retain(|line| {
            !line.starts_with("helm upgrade ")
                && !line.starts_with("kubectl ")
                && !line.starts_with("phase drain_preflight:")
                && !line.starts_with("phase checkpoint:")
                && !line.starts_with("phase migrate:")
        });
        plan.insert(
            2,
            format!(
                "phases drain_preflight, checkpoint, migrate, apply skipped: {} is already installed and known-good, so no helm upgrade runs; converge, canary and commit re-verify it",
                opts.to
            ),
        );
    }
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
            // Before Apply the plan may have been recomputed (a rerun can see a new
            // chart or registry), so the recorded expectations would be stale.
            if !existing.completed.contains(&UpgradePhase::Apply) {
                existing.runner_canary = Some(host.runner_layer_plan().canary_expectations());
            }
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
            skipped: Vec::new(),
            failed_phase: None,
            status: "in_progress".into(),
            plan: plan.clone(),
            drain_completed: false,
            convergence: None,
            canary: None,
            runner_canary: Some(host.runner_layer_plan().canary_expectations()),
            fail_forward: None,
            resumed: false,
        },
    };

    // Resume after Validate still honors a freshly computed refusal and
    // must not replay DrainPreflight to reach it.
    if host.validate_refusal().is_some() || host.refuse_config() || host.refuse_schema() {
        execute_phase(UpgradePhase::Validate, &opts, host, &mut record)?;
    }

    for phase in remaining_after(&record) {
        if same_version
            && matches!(
                phase,
                UpgradePhase::DrainPreflight
                    | UpgradePhase::Checkpoint
                    | UpgradePhase::Migrate
                    | UpgradePhase::Apply
            )
        {
            record.skipped.push(phase);
            host.store_record(record.clone())?;
            continue;
        }
        if phase == UpgradePhase::DrainPreflight && from.is_none() {
            record.skipped.push(phase);
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
                record.failed_phase = Some(phase);
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

/// The terminal failure for a phase whose observer reported `issues`: their
/// redacted, truncated text becomes the reason.
fn issues_fail_forward<H: UpgradeDriver>(
    opts: &UpgradeOpts,
    host: &H,
    phase: &str,
    issues: &[String],
) -> FailForward {
    let detail = truncate_reason(&host.redact(&issues.join("; ")));
    fail_forward_for(
        opts,
        host.serving_previous(),
        &format!("upgrade failed during {phase}: {detail}"),
    )
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
            if let Err(error) = host.apply_target(&opts.to) {
                // A Helm render or apply error used to leave the checkpoint
                // at migrate / in_progress (#3849). Store the same terminal
                // failure the other phases already record.
                record.fail_forward = Some(fail_forward_for(
                    opts,
                    host.serving_previous(),
                    &format!(
                        "upgrade failed during apply: {}",
                        truncate_reason(&host.redact(&format!("{error:#}")))
                    ),
                ));
                return Ok(PhaseOutcome::Failed);
            }
            // #3422: a cleared layer only reaches live threads once their
            // claims are gone, as `cluster deploy` does (#3300).
            if let Err(error) = host.retire_runner_layer_claims() {
                record.fail_forward = Some(fail_forward_for(
                    opts,
                    host.serving_previous(),
                    &format!(
                        "upgrade failed during apply: {}",
                        truncate_reason(&host.redact(&format!("{error:#}")))
                    ),
                ));
                return Ok(PhaseOutcome::Failed);
            }
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
                    record.fail_forward =
                        Some(issues_fail_forward(opts, host, "converge", &verdict.issues));
                }
                return Ok(PhaseOutcome::Failed);
            }
            Ok(PhaseOutcome::Continue)
        }
        UpgradePhase::Canary => {
            let expected = record
                .runner_canary
                .clone()
                .unwrap_or_else(|| host.runner_layer_plan().canary_expectations());
            let verdict = host.run_canary(&expected)?;
            record.canary = Some(verdict.canary.clone());
            if !verdict.canary.passed {
                // Name what the canary saw, as Converge does.
                if !verdict.issues.is_empty() {
                    record.fail_forward =
                        Some(issues_fail_forward(opts, host, "canary", &verdict.issues));
                }
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

/// A Canary verdict: the reported `{passed}` plus the issue text the Canary
/// phase puts in `fail_forward.reason` (#4321). The text is not part of the
/// `--json` canary payload.
pub struct CanaryVerdict {
    pub canary: Canary,
    pub issues: Vec<String>,
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
    /// What the upgrade does to each layered agent (#3218, #4321).
    runner_layers: RunnerLayerPlan,
    /// The target chart's own `fullnameOverride` and `nameOverride` defaults,
    /// read once with the runner defaults; the canary names templates from
    /// them under the retained overlay (#4321).
    chart_name_defaults: Option<serde_json::Value>,
    /// The target chart's rendered drain budget, computed with the exact
    /// retained overlay that Apply will hand to Helm.
    helm_timeout_seconds: u64,
    timeout_refusal: Option<String>,
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
    timeout_seconds: u64,
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
        "--timeout".into(),
        if timeout_seconds == HELM_TIMEOUT_DEFAULT_SECS {
            "15m".into()
        } else {
            format!("{timeout_seconds}s")
        },
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
    // Stale runner bindings are removed from the retained values document
    // before this argv is built (#3849). A `--set key=null` after `-f` is not
    // a deletion: Helm 3.20 keeps the previous digest, and a nil entry fails
    // the chart digest guard before a revision is created.
    argv
}

/// A disabled drain emits no Job. An emitted drain Job must carry an exact,
/// positive integer budget so the CLI can cover the chart's gate and worker
/// grace without guessing from values that Helm may have overridden.
fn parse_target_helm_timeout(rendered: &str) -> Result<u64> {
    let mut timeout = None;
    for document in serde_norway::Deserializer::from_str(rendered) {
        let value = serde_json::Value::deserialize(document)
            .context("target Helm timeout manifest is malformed")?;
        if value.get("kind").and_then(serde_json::Value::as_str) != Some("Job")
            || value
                .pointer("/metadata/labels/app.kubernetes.io~1component")
                .and_then(serde_json::Value::as_str)
                != Some("upgrade-drain")
        {
            continue;
        }
        if timeout.is_some() {
            bail!("target chart rendered more than one upgrade drain Job");
        }
        let annotations = value
            .pointer("/metadata/annotations")
            .and_then(serde_json::Value::as_object)
            .context("target upgrade drain Job has no annotations")?;
        if annotations
            .get("helm.sh/hook")
            .and_then(serde_json::Value::as_str)
            != Some("pre-upgrade")
        {
            bail!("target upgrade drain Job is not a pre-upgrade hook");
        }
        let raw = annotations
            .get(MINIMUM_HELM_TIMEOUT_ANNOTATION)
            .and_then(serde_json::Value::as_str)
            .context("target upgrade drain Job has no minimum Helm timeout annotation")?;
        let seconds = raw
            .parse::<u64>()
            .ok()
            .filter(|seconds| *seconds > 0)
            .context("target upgrade drain Job has an invalid minimum Helm timeout annotation")?;
        timeout = Some(seconds);
    }
    Ok(timeout
        .unwrap_or(HELM_TIMEOUT_DEFAULT_SECS)
        .max(HELM_TIMEOUT_DEFAULT_SECS))
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
            runner_layers: RunnerLayerPlan::default(),
            chart_name_defaults: None,
            helm_timeout_seconds: HELM_TIMEOUT_DEFAULT_SECS,
            timeout_refusal: None,
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
        match self.retained_overlay() {
            Ok(Some((overlay, schema_plan))) => {
                self.overlay = Some(overlay);
                self.schema_plan = Some(schema_plan);
            }
            Ok(None) => {}
            Err(error) => self.config_refusal = Some(format!("{error:#}")),
        }
        if self.current.is_some() && self.config_refusal.is_none() {
            let caller_configured = self
                .overlay
                .as_deref()
                .and_then(|overlay| serde_json::from_str::<serde_json::Value>(overlay).ok())
                .is_some_and(|values| {
                    let nonblank = |pointer: &str| {
                        values
                            .pointer(pointer)
                            .and_then(serde_json::Value::as_str)
                            .is_some_and(|value| !value.trim().is_empty())
                    };
                    nonblank("/connectorCaller/existingSecret")
                        || (nonblank("/connectorCaller/signingKey")
                            && nonblank("/connectorCaller/verifyKey"))
                });
            if !caller_configured {
                let mut args = vec![
                    plain("cluster"),
                    plain("up"),
                    plain("--namespace"),
                    plain(&self.opts.common.namespace),
                    plain("--release"),
                    plain(&self.opts.common.release),
                ];
                if let Ok(context) = std::env::var("HELM_KUBECONTEXT") {
                    if !context.trim().is_empty() {
                        args.push(plain("--context"));
                        args.push(plain(context));
                    }
                }
                if self.opts.chart.pending_release().is_none() {
                    args.push(plain("--chart"));
                    args.push(plain(self.chart_ref()));
                }
                let corrective = OpsCommand::new("curie", args).display();
                self.config_refusal = Some(format!(
                    "connector_caller_pair_required: installed release must record a nonblank \
                     connectorCaller.existingSecret or both connectorCaller.signingKey and \
                     connectorCaller.verifyKey; run sealed `{corrective}` without --dev to \
                     generate a missing pair before retrying upgrade. To repair a partial pair, \
                     add `--set connectorCaller.existingSecret=<secret-name>` or supply both \
                     connectorCaller.signingKey and connectorCaller.verifyKey"
                ));
                return;
            }
        }
        self.chart_refusal = self.chart_pin_refusal();
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
            if self.current.is_some()
                && self.chart_refusal.is_none()
                && self.config_refusal.is_none()
                && self.schema_refusal.is_none()
            {
                match self.render_target_helm_timeout() {
                    Ok(seconds) => self.helm_timeout_seconds = seconds,
                    Err(error) => self.timeout_refusal = Some(format!("{error:#}")),
                }
            }
        }
        self.compute_runner_layers();
    }

    /// What the upgrade does to each layered agent (#3218, #4321).
    ///
    /// The deploy guard guarantees every bound layer was built on the current
    /// installation's runner, so they all stop matching exactly when the
    /// target runner's digest differs from the current one. A release with no
    /// layered agent costs no extra read. Either runner being unknown counts
    /// as a change: keeping an old layer under a new worker is the failure this
    /// exists to prevent, and running the platform runner is always servable.
    /// A stock dark factory binding asks the registry for the layer published
    /// for `--to` and is rebound to it when its base is the target runner.
    fn compute_runner_layers(&mut self) {
        let Some(overlay) = self
            .overlay
            .as_deref()
            .and_then(|o| serde_json::from_str::<serde_json::Value>(o).ok())
        else {
            return;
        };
        let bindings = crate::cluster_secrets::layered_bindings(&overlay);
        if bindings.is_empty() {
            return;
        }
        let chart_default = self.target_chart_values();
        if let Some(defaults) = &chart_default {
            self.chart_name_defaults = Some(effective_name_values(
                Some(defaults),
                &serde_json::Value::Null,
            ));
        }
        // A same-version known-good rerun skips Apply, so no binding can
        // change: every one is kept and Canary checks the retained ones. A
        // transient runner or registry failure must not plan a clear here.
        let to = Some(self.opts.to.as_str());
        if self.current.as_deref() == to && self.known_good.as_deref() == to {
            self.runner_layers = RunnerLayerPlan {
                kept: bindings.into_iter().collect(),
                ..RunnerLayerPlan::default()
            };
            return;
        }
        let current = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(crate::cluster_secrets::installed_runner(&self.opts.common))
        })
        .ok()
        .map(|(_, pinned)| pinned);
        let target = target_runner_ref(chart_default.as_ref(), &overlay, &self.opts.to).and_then(
            |reference| {
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current()
                        .block_on(crate::cluster_secrets::pin_runner_reference(&reference))
                })
                .ok()
            },
        );
        let stock = if needs_stock_lookup(&bindings, current.as_deref(), target.as_deref()) {
            let published = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current()
                    .block_on(crate::examples::published_stock_runner(&self.opts.to))
            });
            match published {
                Ok(Some(published)) => StockRunnerLookup::Published(published),
                Ok(None) => StockRunnerLookup::Unpublished,
                Err(error) => StockRunnerLookup::Unreachable(format!("{error:#}")),
            }
        } else {
            StockRunnerLookup::NotNeeded
        };
        self.runner_layers =
            plan_runner_layers(&bindings, current.as_deref(), target.as_deref(), &stock);
        if self.runner_layers.is_empty() {
            return;
        }
        let Some(raw) = self.overlay.as_deref() else {
            self.config_refusal = Some(
                "stale runner image bindings could not be removed because the upgrade has no retained values"
                    .into(),
            );
            return;
        };
        let mut values: serde_json::Value = match serde_json::from_str(raw) {
            Ok(values) => values,
            Err(error) => {
                self.config_refusal = Some(format!(
                    "could not read retained values to remove stale runner image bindings: {error}"
                ));
                return;
            }
        };
        let cleared: Vec<String> = self
            .runner_layers
            .clears
            .iter()
            .map(|(agent, _)| agent.clone())
            .collect();
        crate::cluster_secrets::omit_runner_image_bindings(&mut values, &cleared);
        crate::cluster_secrets::rebind_runner_image_bindings(
            &mut values,
            &self.runner_layers.rebinds,
        );
        match helm_values_document(&values) {
            Ok(next) => self.overlay = Some(next),
            Err(error) => {
                self.config_refusal = Some(format!(
                    "could not remove stale runner image bindings before upgrade: {error:#}"
                ));
            }
        }
    }

    /// The target chart's own default values document, when the chart is
    /// available to read. A pending release asset is not.
    fn target_chart_values(&self) -> Option<serde_json::Value> {
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

    fn render_target_helm_timeout(&self) -> Result<u64> {
        let mut args = vec![
            plain("template"),
            plain(&self.opts.common.release),
            plain(self.chart_ref()),
            plain("-n"),
            plain(&self.opts.common.namespace),
            plain("--is-upgrade"),
        ];
        if self.opts.chart.uses_helm_version() {
            args.push(plain("--version"));
            args.push(plain(&self.opts.to));
        }
        let tmp = tempfile::NamedTempFile::new().context("upgrade values tempfile")?;
        if let Some(overlay) = &self.overlay {
            std::fs::write(tmp.path(), overlay).context("could not write upgrade values")?;
            args.push(plain("-f"));
            args.push(plain(tmp.path().to_string_lossy().into_owned()));
        }
        let (ok, out, err) = self.run(&OpsCommand::new("helm", args))?;
        if !ok {
            bail!(
                "could not render target Helm timeout metadata: {}",
                crate::schema_window::redact_probe_text(err.trim())
            );
        }
        parse_target_helm_timeout(&out)
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
        // @spec CLUSTER-VALUES-FILES c3-c4: never admit defaults after losing the overlay.
        let _tmp = if let Some(overlay) = &self.overlay {
            let tmp = tempfile::NamedTempFile::new()
                .map_err(|_| "could not prepare target metadata values".to_string())?;
            std::fs::write(tmp.path(), overlay)
                .map_err(|_| "could not write target metadata values".to_string())?;
            args.push(plain("-f"));
            args.push(plain(tmp.path().to_string_lossy().into_owned()));
            Some(tmp)
        } else {
            None
        };
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
    /// `Ok(None)` means no values were returned; the installed version still
    /// distinguishes an empty release from a first install.
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
            return if self.opts.file_values.is_some() {
                self.migrate_effective_overlay(serde_json::json!({}))
            } else {
                Ok(None)
            };
        }
        if out.trim().is_empty() {
            return if self.opts.file_values.is_some() {
                self.migrate_effective_overlay(serde_json::json!({}))
            } else {
                Ok(None)
            };
        }
        let values: serde_json::Value =
            serde_norway::from_str(&out).context("retained helm values are malformed")?;
        self.migrate_effective_overlay(values)
    }

    /// @spec CLUSTER-VALUES-FILES c3: admit the final merged input once.
    fn migrate_effective_overlay(
        &self,
        mut values: serde_json::Value,
    ) -> Result<Option<(String, String)>> {
        if let Some(files) = &self.opts.file_values {
            crate::config_migrate::clear_replaced_secret_refs(&mut values, &files.0);
            super::lint_values::merge_values(&mut values, files.0.clone());
        }
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

    /// The chart version of the revision Helm still marks deployed.
    /// A failed upgrade often leaves the previous revision deployed (#3421).
    fn observe_deployed_version(&mut self) {
        let history_cmd = super::verbs::helm_history_cmd(&self.opts.common);
        let Ok((true, history_out, _)) = self.run(&history_cmd) else {
            return;
        };
        let Ok(history) = serde_json::from_str::<serde_json::Value>(&history_out) else {
            return;
        };
        let Ok(revision) =
            crate::cluster_secrets::serving_revision(&history, &self.opts.common.release)
        else {
            return;
        };
        let metadata =
            crate::cluster_secrets::helm_get_json(&self.opts.common, "metadata", false, revision);
        let Ok((true, metadata_out, _)) = self.run(&metadata) else {
            return;
        };
        let Ok(document) = serde_json::from_str::<serde_json::Value>(&metadata_out) else {
            return;
        };
        let Some(version) = document
            .get("version")
            .and_then(serde_json::Value::as_str)
            .filter(|version| !version.is_empty())
        else {
            return;
        };
        self.set_current(Some(version.to_string()));
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
            self.helm_timeout_seconds,
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

    fn live_canary(&self, expected: &[(String, ExpectedRunner)]) -> Result<CanaryVerdict> {
        // R4: re-read here. Apply already set `current` from its own
        // post-condition read, so comparing against `current` would be a
        // self-comparison, and the release can move between the two phases.
        // Convergence is not re-observed: the Converge phase has already
        // failed the run unless it was exact.
        let version_ok = self.inspect_version().as_deref() == Some(self.opts.to.as_str());
        let issues = if expected.is_empty() {
            Vec::new()
        } else {
            // #4321: every layered agent's rendered template must carry the
            // runner the upgrade planned. An unreadable list fails closed.
            match self.read_sandbox_templates() {
                Ok(observed) => {
                    template_canary_issues(&self.release_fullname(), expected, &observed)
                }
                Err(error) => vec![format!(
                    "could not read the release's SandboxTemplates to confirm the planned runner \
                     layers: {error:#}"
                )],
            }
        };
        Ok(CanaryVerdict {
            canary: Canary {
                passed: version_ok && issues.is_empty(),
            },
            issues,
        })
    }

    /// The release's `curie.fullname` under the retained overlay Apply hands
    /// Helm (#4321). No overlay, or one that does not parse, renders with the
    /// chart defaults, as Helm would.
    fn release_fullname(&self) -> String {
        let overlay = self
            .overlay
            .as_deref()
            .and_then(|raw| serde_norway::from_str::<serde_json::Value>(raw).ok())
            .unwrap_or(serde_json::Value::Null);
        let values = effective_name_values(self.chart_name_defaults.as_ref(), &overlay);
        release_fullname_from_values(&self.opts.common.release, &values)
    }

    /// The release's SandboxTemplates, read once for the canary (#4321).
    fn read_sandbox_templates(&self) -> Result<ObservedTemplates> {
        let cmd = OpsCommand::new(
            "kubectl",
            vec![
                plain("-n"),
                plain(&self.opts.common.namespace),
                plain("get"),
                plain(crate::connectors::SANDBOX_TEMPLATE_KIND),
                plain("-l"),
                plain(format!(
                    "app.kubernetes.io/instance={}",
                    self.opts.common.release
                )),
                plain("-o"),
                plain("json"),
            ],
        );
        let (ok, out, err) = self.run(&cmd)?;
        if !ok {
            bail!("`{}` failed: {}", cmd.display(), err.trim());
        }
        parse_sandbox_templates(&out)
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
    fn runner_layer_plan(&self) -> RunnerLayerPlan {
        self.runner_layers.clone()
    }

    fn helm_timeout_seconds(&self) -> u64 {
        self.helm_timeout_seconds
    }
    fn validate_refusal(&self) -> Option<String> {
        self.chart_refusal
            .clone()
            .or_else(|| self.config_refusal.clone())
            .or_else(|| self.timeout_refusal.clone())
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
        if let Err(error) = self.helm_upgrade(to) {
            // Helm can fail after it has already moved the deployed revision.
            // Re-read that revision so previous_serving is not the pre-apply cache.
            self.observe_deployed_version();
            return Err(error);
        }
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
    fn retire_runner_layer_claims(&mut self) -> Result<()> {
        for cmd in runner_layer_retirements(
            &self.opts.common.namespace,
            &self.runner_layers.retired_agents(),
        ) {
            let (ok, _, err) = self.run(&cmd)?;
            if !ok {
                bail!(
                    "helm upgrade changed the runner layer, but retiring its sandboxes failed \
                     ({}): {}; run `{}` so live threads leave the old layer",
                    cmd.display(),
                    err.trim(),
                    cmd.display()
                );
            }
        }
        Ok(())
    }
    fn observe_convergence(&self) -> Result<ConvergenceVerdict> {
        self.live_convergence()
    }
    fn run_canary(&self, expected: &[(String, ExpectedRunner)]) -> Result<CanaryVerdict> {
        self.live_canary(expected)
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
            for notice in runner_layer_warnings(&live.runner_layers, &opts) {
                crate::ui::ui().warn(&notice);
            }
            run_lifecycle_inner(opts, &mut live).await
        }
        Err(error) => Err(error),
    };
    let result = wrap_schema_refusal(&live, result);
    // Apply failures stay a non-zero exit. The checkpoint is already the
    // terminal failed record, and ownership is released first (#3849).
    refuse_failed_apply(finish_owned_upgrade(&mut live, result))
}

fn refuse_failed_apply(result: Result<ClusterUpgradeOutput>) -> Result<ClusterUpgradeOutput> {
    match result {
        Ok(output) => match &output {
            ClusterUpgradeOutput::Completed {
                status,
                phase,
                fail_forward,
                ..
            } if status == "failed" && phase == "apply" => {
                let reason = fail_forward
                    .as_ref()
                    .map(|item| item.reason.clone())
                    .unwrap_or_else(|| "upgrade failed during apply".to_string());
                Err(crate::ui::ui().failed_report(&output, anyhow::anyhow!(reason)))
            }
            _ => Ok(output),
        },
        Err(error) => Err(error),
    }
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
            file_values: None,
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

#[cfg(test)]
mod runner_layer_guard_tests {
    use super::*;
    use std::collections::BTreeMap;

    fn runner_values(fields: serde_json::Value) -> serde_json::Value {
        serde_json::json!({"agentSandbox": {"runner": fields}})
    }

    #[test]
    fn target_runner_ref_layers_overlay_over_chart_default() {
        let chart_default = runner_values(serde_json::json!({
            "image": "ghcr.io/curie-eng/curie-runner",
            "tag": "0.9.0",
        }));
        // The overlay only sets a digest; the image field must still come
        // from the chart default underneath it.
        let overlay = runner_values(serde_json::json!({"digest": "sha256:aaaa"}));
        let reference = target_runner_ref(Some(&chart_default), &overlay, "0.9.0")
            .expect("image known from chart default");
        assert_eq!(reference, "ghcr.io/curie-eng/curie-runner@sha256:aaaa");
    }

    #[test]
    fn target_runner_ref_overlay_null_removes_chart_default_field() {
        let chart_default = runner_values(serde_json::json!({
            "image": "ghcr.io/curie-eng/curie-runner",
            "tag": "0.9.0",
            "digest": "sha256:aaaa",
        }));
        // An explicit null in the overlay clears the chart default's digest,
        // so the tag (falling back to `to`) takes over.
        let overlay = runner_values(serde_json::json!({"digest": null}));
        let reference = target_runner_ref(Some(&chart_default), &overlay, "0.9.9")
            .expect("tag known from chart default");
        assert_eq!(reference, "ghcr.io/curie-eng/curie-runner:0.9.0");
    }

    #[test]
    fn target_runner_ref_empty_tag_falls_back_to_to() {
        let overlay = runner_values(serde_json::json!({
            "image": "ghcr.io/curie-eng/curie-runner",
            "tag": "",
        }));
        let reference = target_runner_ref(None, &overlay, "0.9.5").expect("to backs an empty tag");
        assert_eq!(reference, "ghcr.io/curie-eng/curie-runner:0.9.5");
    }

    #[test]
    fn target_runner_ref_digest_wins_over_tag() {
        let overlay = runner_values(serde_json::json!({
            "image": "ghcr.io/curie-eng/curie-runner",
            "tag": "0.9.0",
            "digest": "sha256:bbbb",
        }));
        let reference = target_runner_ref(None, &overlay, "0.9.0").expect("digest known");
        assert_eq!(reference, "ghcr.io/curie-eng/curie-runner@sha256:bbbb");
    }

    #[test]
    fn target_runner_ref_none_when_no_image_known() {
        let overlay = serde_json::json!({});
        assert_eq!(target_runner_ref(None, &overlay, "0.9.0"), None);
    }

    const STOCK_OLD: &str = "ghcr.io/curie-eng/curie-dark-factory-runner@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const STOCK_NEW: &str = "ghcr.io/curie-eng/curie-dark-factory-runner@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const RUNNER_OLD: &str = "ghcr.io/curie-eng/curie-runner@sha256:1111111111111111111111111111111111111111111111111111111111111111";
    const RUNNER_NEW: &str = "ghcr.io/curie-eng/curie-runner@sha256:2222222222222222222222222222222222222222222222222222222222222222";
    const RUNNER_OTHER: &str = "ghcr.io/curie-eng/curie-runner@sha256:3333333333333333333333333333333333333333333333333333333333333333";
    const OWNER_LAYER: &str = "ghcr.io/acme/acme-bot-runner@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

    fn bindings(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(agent, image)| (agent.to_string(), image.to_string()))
            .collect()
    }

    fn published(base: &str) -> StockRunnerLookup {
        StockRunnerLookup::Published(crate::examples::PublishedStockRunner {
            layer_image: STOCK_NEW.to_string(),
            base_image: base.to_string(),
        })
    }

    fn agents(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| name.to_string()).collect()
    }

    fn owner_clears(names: &[&str]) -> Vec<(String, LayerClearReason)> {
        names
            .iter()
            .map(|name| (name.to_string(), LayerClearReason::OwnerBuilt))
            .collect()
    }

    /// The `kubectl get sandboxtemplates -o json` list for `(name, image)`.
    fn templates(items: &[(&str, &str)]) -> String {
        let items: Vec<serde_json::Value> = items
            .iter()
            .map(|(name, image)| {
                serde_json::json!({
                    "apiVersion": "extensions.agents.x-k8s.io/v1beta1",
                    "kind": "SandboxTemplate",
                    "metadata": {"name": name, "labels": {"app.kubernetes.io/instance": "rel"}},
                    "spec": {"podTemplate": {"spec": {"containers": [
                        {"name": "runner", "image": image}
                    ]}}},
                })
            })
            .collect();
        serde_json::json!({"apiVersion": "v1", "kind": "List", "items": items}).to_string()
    }

    fn issues(expected: &[(String, ExpectedRunner)], items: &[(&str, &str)]) -> Vec<String> {
        issues_for("rel-curie", expected, items)
    }

    /// [`issues`] for a release whose `curie.fullname` is `fullname`.
    fn issues_for(
        fullname: &str,
        expected: &[(String, ExpectedRunner)],
        items: &[(&str, &str)],
    ) -> Vec<String> {
        let observed = parse_sandbox_templates(&templates(items)).expect("template list parses");
        template_canary_issues(fullname, expected, &observed)
    }

    // Migrated from the `layer_clears_from_refs` seam (#3218): the affected
    // set is still `layers_stopping_to_match`, now carried by the planner.
    #[test]
    fn plan_same_runner_digest_clears_nothing() {
        let layered = bindings(&[("agent-a", OWNER_LAYER), ("agent-b", OWNER_LAYER)]);
        let reference = "ghcr.io/curie-eng/curie-runner@sha256:aaaa";
        let plan = plan_runner_layers(
            &layered,
            Some(reference),
            Some(reference),
            &StockRunnerLookup::NotNeeded,
        );
        assert!(
            plan.rebinds.is_empty() && plan.clears.is_empty(),
            "{plan:?}"
        );
        assert!(plan.retired_agents().is_empty());
    }

    #[test]
    fn plan_differing_runner_digest_clears_every_owner_built_layer() {
        let layered = bindings(&[("agent-a", OWNER_LAYER), ("agent-b", OWNER_LAYER)]);
        let current = "ghcr.io/curie-eng/curie-runner@sha256:aaaa";
        let target = "ghcr.io/curie-eng/curie-runner@sha256:bbbb";
        let plan = plan_runner_layers(
            &layered,
            Some(current),
            Some(target),
            &StockRunnerLookup::NotNeeded,
        );
        assert_eq!(plan.clears, owner_clears(&["agent-a", "agent-b"]));
        assert_eq!(plan.retired_agents(), agents(&["agent-a", "agent-b"]));
    }

    #[test]
    fn plan_clears_every_layer_when_either_runner_is_unknown() {
        let layered = bindings(&[("agent-a", OWNER_LAYER)]);
        let reference = "ghcr.io/curie-eng/curie-runner@sha256:aaaa";
        for (current, target) in [(None, Some(reference)), (Some(reference), None)] {
            let plan = plan_runner_layers(&layered, current, target, &StockRunnerLookup::NotNeeded);
            assert_eq!(
                plan.clears,
                owner_clears(&["agent-a"]),
                "{current:?} {target:?}"
            );
            assert!(plan.rebinds.is_empty() && plan.kept.is_empty(), "{plan:?}");
        }
    }

    /// #4321 AC1: a stock binding is rebound to the layer our registry
    /// publishes for `--to`, never to the binding's own digest. Two stock
    /// agents share the one answer.
    #[test]
    fn plan_rebinds_a_stock_layer_on_the_target_base() {
        let layered = bindings(&[("dark-factory", STOCK_OLD), ("night-factory", STOCK_OLD)]);
        let plan = plan_runner_layers(
            &layered,
            Some(RUNNER_OLD),
            Some(RUNNER_NEW),
            &published(RUNNER_NEW),
        );
        assert_eq!(
            plan,
            RunnerLayerPlan {
                rebinds: vec![
                    ("dark-factory".into(), STOCK_NEW.into()),
                    ("night-factory".into(), STOCK_NEW.into()),
                ],
                clears: Vec::new(),
                kept: Vec::new(),
            }
        );
        assert!(!plan.is_empty());
        assert_eq!(
            plan.retired_agents(),
            agents(&["dark-factory", "night-factory"])
        );
        assert!(plan.owner_built_clears().is_empty());
        assert_eq!(
            plan.canary_expectations(),
            vec![
                (
                    "dark-factory".to_string(),
                    ExpectedRunner::Image(STOCK_NEW.into())
                ),
                (
                    "night-factory".to_string(),
                    ExpectedRunner::Image(STOCK_NEW.into())
                ),
            ]
        );
    }

    /// #4321 AC4: a published layer built on another base would run a runner
    /// the worker cannot serve (ADR 0173 decision 5), so it is cleared.
    #[test]
    fn plan_clears_a_stock_layer_built_on_another_base() {
        let layered = bindings(&[("dark-factory", STOCK_OLD)]);
        let plan = plan_runner_layers(
            &layered,
            Some(RUNNER_OLD),
            Some(RUNNER_NEW),
            &published(RUNNER_OTHER),
        );
        assert!(plan.rebinds.is_empty(), "{plan:?}");
        assert_eq!(
            plan.clears,
            vec![(
                "dark-factory".to_string(),
                LayerClearReason::StockBaseMismatch {
                    published_base: RUNNER_OTHER.into(),
                    target: RUNNER_NEW.into(),
                },
            )]
        );
        assert!(plan.owner_built_clears().is_empty());
        assert_eq!(
            plan.canary_expectations(),
            vec![("dark-factory".to_string(), ExpectedRunner::PlatformRunner)]
        );
    }

    #[test]
    fn plan_clears_an_unpublished_stock_layer() {
        let layered = bindings(&[("dark-factory", STOCK_OLD)]);
        let plan = plan_runner_layers(
            &layered,
            Some(RUNNER_OLD),
            Some(RUNNER_NEW),
            &StockRunnerLookup::Unpublished,
        );
        assert_eq!(
            plan.clears,
            vec![(
                "dark-factory".to_string(),
                LayerClearReason::StockLayerUnpublished
            )]
        );
        assert_eq!(plan.retired_agents(), agents(&["dark-factory"]));
    }

    #[test]
    fn plan_clears_a_stock_layer_when_the_registry_is_unreachable() {
        let layered = bindings(&[("dark-factory", STOCK_OLD)]);
        let plan = plan_runner_layers(
            &layered,
            Some(RUNNER_OLD),
            Some(RUNNER_NEW),
            &StockRunnerLookup::Unreachable("connection refused".into()),
        );
        assert_eq!(
            plan.clears,
            vec![(
                "dark-factory".to_string(),
                LayerClearReason::StockRegistryUnreachable("connection refused".into()),
            )]
        );
        assert!(plan.rebinds.is_empty(), "{plan:?}");
    }

    /// With no target digest there is nothing to prove a published base
    /// against, so the planner clears without asking the registry.
    #[test]
    fn plan_clears_a_stock_layer_when_the_target_runner_is_unknown() {
        let layered = bindings(&[("dark-factory", STOCK_OLD)]);
        assert!(!needs_stock_lookup(&layered, Some(RUNNER_OLD), None));
        let plan = plan_runner_layers(
            &layered,
            Some(RUNNER_OLD),
            None,
            &StockRunnerLookup::NotNeeded,
        );
        assert_eq!(
            plan.clears,
            vec![(
                "dark-factory".to_string(),
                LayerClearReason::TargetRunnerUnknown
            )]
        );
    }

    #[test]
    fn plan_clears_an_owner_built_layer() {
        let layered = bindings(&[("acme-bot", OWNER_LAYER), ("dark-factory", STOCK_OLD)]);
        let plan = plan_runner_layers(
            &layered,
            Some(RUNNER_OLD),
            Some(RUNNER_NEW),
            &published(RUNNER_NEW),
        );
        assert_eq!(plan.clears, owner_clears(&["acme-bot"]));
        assert_eq!(
            plan.rebinds,
            vec![("dark-factory".to_string(), STOCK_NEW.to_string())]
        );
        assert_eq!(plan.owner_built_clears(), agents(&["acme-bot"]));
        // Rebinds and clears both retire claims, merged in name order.
        assert_eq!(plan.retired_agents(), agents(&["acme-bot", "dark-factory"]));
        assert_eq!(
            plan.canary_expectations(),
            vec![
                ("acme-bot".to_string(), ExpectedRunner::PlatformRunner),
                (
                    "dark-factory".to_string(),
                    ExpectedRunner::Image(STOCK_NEW.into())
                ),
            ]
        );
    }

    /// A same-runner upgrade touches nothing, but the canary still checks
    /// every binding it kept (edge case 3).
    #[test]
    fn plan_is_empty_when_the_runner_is_unchanged() {
        let layered = bindings(&[("acme-bot", OWNER_LAYER), ("dark-factory", STOCK_OLD)]);
        assert!(!needs_stock_lookup(
            &layered,
            Some(RUNNER_NEW),
            Some(RUNNER_NEW)
        ));
        let plan = plan_runner_layers(
            &layered,
            Some(RUNNER_NEW),
            Some(RUNNER_NEW),
            &StockRunnerLookup::NotNeeded,
        );
        assert!(plan.is_empty(), "{plan:?}");
        assert!(plan.rebinds.is_empty() && plan.clears.is_empty());
        assert_eq!(
            plan.kept,
            vec![
                ("acme-bot".to_string(), OWNER_LAYER.to_string()),
                ("dark-factory".to_string(), STOCK_OLD.to_string()),
            ]
        );
        assert!(plan.retired_agents().is_empty());
        assert_eq!(
            plan.canary_expectations(),
            vec![
                (
                    "acme-bot".to_string(),
                    ExpectedRunner::Image(OWNER_LAYER.into())
                ),
                (
                    "dark-factory".to_string(),
                    ExpectedRunner::Image(STOCK_OLD.into())
                ),
            ]
        );
        assert_eq!(runner_layer_rebind_line(&plan, "0.12.3"), None);
        assert_eq!(runner_layer_notice(&plan.owner_built_clears()), None);
        assert_eq!(
            stock_runner_layer_notice(&plan, "ns", "rel", "0.12.3"),
            None
        );
    }

    #[test]
    fn needs_stock_lookup_only_for_affected_stock_bindings() {
        let stock = bindings(&[("dark-factory", STOCK_OLD)]);
        let owner = bindings(&[("acme-bot", OWNER_LAYER)]);
        assert!(needs_stock_lookup(
            &stock,
            Some(RUNNER_OLD),
            Some(RUNNER_NEW)
        ));
        // An unknown installed runner counts as changed (edge case 1).
        assert!(needs_stock_lookup(&stock, None, Some(RUNNER_NEW)));
        assert!(!needs_stock_lookup(
            &stock,
            Some(RUNNER_NEW),
            Some(RUNNER_NEW)
        ));
        assert!(!needs_stock_lookup(&stock, Some(RUNNER_OLD), None));
        assert!(!needs_stock_lookup(
            &owner,
            Some(RUNNER_OLD),
            Some(RUNNER_NEW)
        ));
        assert!(!needs_stock_lookup(
            &BTreeMap::new(),
            None,
            Some(RUNNER_NEW)
        ));
    }

    /// #4321 AC5: only the exact repository we publish to is stock. A
    /// lookalike, another registry, or a tag-only binding is owner-built and
    /// never triggers a lookup, even when the registry would answer.
    #[test]
    fn lookalike_repository_is_owner_built() {
        assert!(is_stock_layer(STOCK_OLD));
        for lookalike in [
            "ghcr.io/curie-eng/curie-dark-factory-runner-fork@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "ghcr.io/curie-eng/curie-dark-factory-runnerx@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "docker.io/curie-eng/curie-dark-factory-runner@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "ghcr.io/curie-eng/curie-dark-factory-runner:0.12.2",
        ] {
            assert!(!is_stock_layer(lookalike), "{lookalike}");
            let layered = bindings(&[("dark-factory", lookalike)]);
            assert!(
                !needs_stock_lookup(&layered, Some(RUNNER_OLD), Some(RUNNER_NEW)),
                "{lookalike}"
            );
            for lookup in [StockRunnerLookup::NotNeeded, published(RUNNER_NEW)] {
                let plan =
                    plan_runner_layers(&layered, Some(RUNNER_OLD), Some(RUNNER_NEW), &lookup);
                assert_eq!(plan.clears, owner_clears(&["dark-factory"]), "{lookalike}");
                assert!(plan.rebinds.is_empty(), "{lookalike}: {plan:?}");
            }
        }
    }

    /// AC7: the v0.12.3 owner-built notice, byte for byte.
    #[test]
    fn owner_notice_text_is_unchanged() {
        assert_eq!(
            runner_layer_notice(&["a".to_string()]).as_deref(),
            Some(
                "runner layers: the platform runner changes, so the layered runner of agent(s) a \
                 will stop matching. This upgrade clears agentSandbox.runnerImages for them: they \
                 run the platform runner WITHOUT their layer until their owners rebuild with \
                 `curie build --plugin-dir <dir> --registry <ref>` against the upgraded CLI and \
                 redeploy with `curie cluster deploy`. Their live sandboxes are retired after the \
                 helm upgrade, so existing threads start fresh on the platform runner at their \
                 next turn"
            )
        );
        assert_eq!(runner_layer_notice(&[]), None);
    }

    #[test]
    fn rebind_line_names_each_agent_and_its_digest_pinned_image() {
        let plan = RunnerLayerPlan {
            rebinds: vec![("dark-factory".into(), STOCK_NEW.into())],
            clears: owner_clears(&["acme-bot"]),
            kept: Vec::new(),
        };
        let line = runner_layer_rebind_line(&plan, "0.12.3").expect("one rebind");
        assert!(line.starts_with("runner layers rebound:"), "{line}");
        assert!(
            !line.starts_with("runner layers:"),
            "the owner notice prefix must stay unique: {line}"
        );
        assert!(
            line.contains(&format!("dark-factory={STOCK_NEW}")),
            "{line}"
        );
        assert!(!line.contains("acme-bot"), "{line}");
    }

    /// #4321 AC4: a stock agent that cannot be rebound is told its reason and
    /// the stock remedy, never the owner-built `curie build` one.
    #[test]
    fn stock_notice_names_reason_and_stock_remedy_not_curie_build() {
        let plan = RunnerLayerPlan {
            rebinds: Vec::new(),
            clears: vec![
                ("acme-bot".into(), LayerClearReason::OwnerBuilt),
                (
                    "dark-factory".into(),
                    LayerClearReason::StockLayerUnpublished,
                ),
                (
                    "night-factory".into(),
                    LayerClearReason::StockRegistryUnreachable("connection refused".into()),
                ),
                (
                    "sky-factory".into(),
                    LayerClearReason::StockBaseMismatch {
                        published_base: RUNNER_OTHER.into(),
                        target: RUNNER_NEW.into(),
                    },
                ),
                ("sun-factory".into(), LayerClearReason::TargetRunnerUnknown),
            ],
            kept: Vec::new(),
        };
        let notice = stock_runner_layer_notice(&plan, "ns", "rel", "0.12.3").expect("stock clears");
        assert!(notice.starts_with("stock runner layers:"), "{notice}");
        for agent in [
            "dark-factory",
            "night-factory",
            "sky-factory",
            "sun-factory",
        ] {
            assert!(notice.contains(agent), "{agent}: {notice}");
            assert!(
                notice.contains(&format!(
                    "--runner-image {agent}=ghcr.io/curie-eng/curie-dark-factory-runner@sha256:<digest>"
                )),
                "{agent}: {notice}"
            );
            assert!(
                notice.contains(&format!("--agent {agent}")),
                "{agent}: {notice}"
            );
        }
        assert!(
            !notice.contains("acme-bot"),
            "owner-built stays in its own line: {notice}"
        );
        // Each reason.
        assert!(notice.contains("not published"), "{notice}");
        assert!(notice.contains("connection refused"), "{notice}");
        assert!(notice.contains("registry"), "{notice}");
        assert!(
            notice.contains("sha256:3333"),
            "the published base: {notice}"
        );
        assert!(
            notice.contains("sha256:2222"),
            "the target runner: {notice}"
        );
        assert!(notice.contains("target runner is unknown"), "{notice}");
        assert!(notice.contains("0.12.3"), "{notice}");
        // The stock remedy, spelled from `ClusterAction::Factory`,
        // `DarkFactoryAction::Render` and `ClusterAction::Deploy`.
        assert!(
            notice.contains(
                "curie cluster factory --namespace ns --release rel --runner-image \
                 dark-factory=ghcr.io/curie-eng/curie-dark-factory-runner@sha256:<digest>"
            ),
            "{notice}"
        );
        assert!(
            notice.contains("curie example dark-factory render --out <dir>"),
            "{notice}"
        );
        assert!(
            notice.contains(
                "curie cluster deploy --namespace ns --release rel --plugin-dir <dir> --agent dark-factory"
            ),
            "{notice}"
        );
        assert!(!notice.contains("curie build --plugin-dir"), "{notice}");

        // The owner line for the same plan lists only the owner-built agent.
        let owner = runner_layer_notice(&plan.owner_built_clears()).expect("owner clear");
        assert!(
            owner.contains("agent(s) acme-bot will stop matching"),
            "{owner}"
        );
        assert!(!owner.contains("factory"), "{owner}");

        let owner_only = RunnerLayerPlan {
            clears: owner_clears(&["acme-bot"]),
            ..RunnerLayerPlan::default()
        };
        assert_eq!(
            stock_runner_layer_notice(&owner_only, "ns", "rel", "0.12.3"),
            None
        );
    }

    /// #4321 AC6: the canary compares each planned runner with the rendered
    /// SandboxTemplate, by exact template name.
    #[test]
    fn template_canary_accepts_a_rebound_template_on_the_planned_layer() {
        let expected = [(
            "dark-factory".to_string(),
            ExpectedRunner::Image(STOCK_NEW.into()),
        )];
        assert!(issues(
            &expected,
            &[
                ("rel-curie-runner", RUNNER_NEW),
                ("rel-curie-agent-dark-factory-runner", STOCK_NEW),
            ]
        )
        .is_empty());
    }

    #[test]
    fn template_canary_flags_a_rebound_template_that_is_missing_or_off_plan() {
        let expected = [(
            "dark-factory".to_string(),
            ExpectedRunner::Image(STOCK_NEW.into()),
        )];
        let missing = issues(&expected, &[("rel-curie-runner", RUNNER_NEW)]);
        assert_eq!(missing.len(), 1, "{missing:?}");
        assert!(missing[0].contains("dark-factory"), "{missing:?}");
        assert!(
            missing[0].contains("rel-curie-agent-dark-factory-runner"),
            "{missing:?}"
        );
        assert!(missing[0].contains("missing"), "{missing:?}");
        assert!(missing[0].contains(STOCK_NEW), "{missing:?}");

        let off_plan = issues(
            &expected,
            &[
                ("rel-curie-runner", RUNNER_NEW),
                ("rel-curie-agent-dark-factory-runner", RUNNER_NEW),
            ],
        );
        assert_eq!(off_plan.len(), 1, "{off_plan:?}");
        assert!(off_plan[0].contains("dark-factory"), "{off_plan:?}");
        assert!(off_plan[0].contains(RUNNER_NEW), "observed: {off_plan:?}");
        assert!(off_plan[0].contains(STOCK_NEW), "expected: {off_plan:?}");
    }

    /// A cleared agent whose only per-agent key was `runnerImages` gets no
    /// template from `curie.agentSandboxPoolAgents` and runs the platform
    /// template, so absence is fine. A per-agent template still on a layer is
    /// not.
    #[test]
    fn template_canary_checks_cleared_agents_against_the_platform_runner() {
        let expected = [("acme-bot".to_string(), ExpectedRunner::PlatformRunner)];
        assert!(issues(&expected, &[("rel-curie-runner", RUNNER_NEW)]).is_empty());
        assert!(issues(
            &expected,
            &[
                ("rel-curie-runner", RUNNER_NEW),
                ("rel-curie-agent-acme-bot-runner", RUNNER_NEW),
            ]
        )
        .is_empty());
        let stale = issues(
            &expected,
            &[
                ("rel-curie-runner", RUNNER_NEW),
                ("rel-curie-agent-acme-bot-runner", OWNER_LAYER),
            ],
        );
        assert_eq!(stale.len(), 1, "{stale:?}");
        assert!(stale[0].contains("acme-bot"), "{stale:?}");
        assert!(stale[0].contains(OWNER_LAYER), "{stale:?}");

        let no_platform = issues(&expected, &[]);
        assert!(
            !no_platform.is_empty(),
            "a cleared agent needs the platform template"
        );
    }

    #[test]
    fn template_canary_matches_template_names_exactly() {
        let image = STOCK_NEW;
        let items = [
            ("rel-curie-runner", RUNNER_NEW),
            ("rel-curie-agent-bot-runner", image),
        ];
        let bot = [("bot".to_string(), ExpectedRunner::Image(image.into()))];
        assert!(issues(&bot, &items).is_empty());
        // `agent-bot` must find `rel-curie-agent-agent-bot-runner`, not the
        // `bot` agent's template whose name ends the same way.
        let agent_bot = [("agent-bot".to_string(), ExpectedRunner::Image(image.into()))];
        let found = issues(&agent_bot, &items);
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(found[0].contains("missing"), "{found:?}");
        // And the reverse: only `rel-curie-agent-agent-bot-runner` exists.
        let only_agent_bot = [
            ("rel-curie-runner", RUNNER_NEW),
            ("rel-curie-agent-agent-bot-runner", image),
        ];
        assert!(issues(&agent_bot, &only_agent_bot).is_empty());
        assert_eq!(issues(&bot, &only_agent_bot).len(), 1);
    }

    #[test]
    fn parse_sandbox_templates_rejects_malformed_json() {
        assert!(parse_sandbox_templates("{").is_err());
        assert!(parse_sandbox_templates("not json").is_err());
    }

    /// #4321 review: the canary names templates from the chart's own
    /// `curie.fullname` (charts/curie/templates/_helpers.tpl), never from a
    /// suffix heuristic over whatever templates happen to exist.
    #[test]
    fn release_fullname_follows_the_chart_helper() {
        let none = serde_json::json!({});
        assert_eq!(release_fullname_from_values("rel", &none), "rel-curie");
        assert_eq!(release_fullname_from_values("curie", &none), "curie");
        assert_eq!(
            release_fullname_from_values("my-curie-prod", &none),
            "my-curie-prod"
        );
        assert_eq!(
            release_fullname_from_values("rel", &serde_json::json!({"nameOverride": "acme"})),
            "rel-acme"
        );
        // `contains $name .Release.Name` uses the override name, not "curie".
        assert_eq!(
            release_fullname_from_values("acme-prod", &serde_json::json!({"nameOverride": "acme"})),
            "acme-prod"
        );
        assert_eq!(
            release_fullname_from_values(
                "curie-prod",
                &serde_json::json!({"nameOverride": "acme"})
            ),
            "curie-prod-acme"
        );
        assert_eq!(
            release_fullname_from_values(
                "rel",
                &serde_json::json!({"fullnameOverride": "acme-runner"})
            ),
            "acme-runner"
        );
        // fullnameOverride wins over nameOverride.
        assert_eq!(
            release_fullname_from_values(
                "rel",
                &serde_json::json!({"fullnameOverride": "acme-runner", "nameOverride": "x"})
            ),
            "acme-runner"
        );
        // An empty override is falsy in the template, so it falls through.
        assert_eq!(
            release_fullname_from_values(
                "rel",
                &serde_json::json!({"fullnameOverride": "", "nameOverride": ""})
            ),
            "rel-curie"
        );
        // trunc 63, then trimSuffix "-" removes exactly one trailing dash.
        let long = format!("{}-{}", "a".repeat(62), "b".repeat(7));
        assert_eq!(long.len(), 70);
        assert_eq!(
            release_fullname_from_values("rel", &serde_json::json!({"fullnameOverride": long})),
            "a".repeat(62)
        );
        let double_dash = format!("{}--{}", "a".repeat(61), "z".repeat(10));
        assert_eq!(
            release_fullname_from_values(
                "rel",
                &serde_json::json!({"fullnameOverride": double_dash})
            ),
            format!("{}-", "a".repeat(61))
        );
        let long_release = "r".repeat(70);
        assert_eq!(
            release_fullname_from_values(&long_release, &none),
            "r".repeat(63)
        );
        // With no override it is exactly `cluster deploy`'s fullname.
        let dash_at_63 = format!("{}-x", "p".repeat(62));
        for release in [
            "rel",
            "curie",
            "curieish",
            "my-curie-prod",
            "acme-prod",
            "platform",
            long_release.as_str(),
            dash_at_63.as_str(),
        ] {
            assert_eq!(
                release_fullname_from_values(release, &none),
                super::super::verbs::chart_fullname(release).as_str(),
                "{release}"
            );
        }
    }

    /// #4321 review P2: `fullnameOverride: acme-runner` renders
    /// `acme-runner-runner` and `acme-runner-agent-<agent>-runner`. Exactly one
    /// `-runner` belongs to the template, so a correct rebind passes.
    #[test]
    fn template_canary_honours_a_fullname_ending_in_runner() {
        let expected = [(
            "dark-factory".to_string(),
            ExpectedRunner::Image(STOCK_NEW.into()),
        )];
        let rendered = [
            ("acme-runner-runner", RUNNER_NEW),
            ("acme-runner-agent-dark-factory-runner", STOCK_NEW),
        ];
        assert!(
            issues_for("acme-runner", &expected, &rendered).is_empty(),
            "{:?}",
            issues_for("acme-runner", &expected, &rendered)
        );

        // And a cleared agent's real template is the one checked.
        let cleared = [("acme-bot".to_string(), ExpectedRunner::PlatformRunner)];
        let stale = issues_for(
            "acme-runner",
            &cleared,
            &[
                ("acme-runner-runner", RUNNER_NEW),
                ("acme-runner-agent-acme-bot-runner", OWNER_LAYER),
            ],
        );
        assert_eq!(stale.len(), 1, "{stale:?}");
        assert!(
            stale[0].contains("acme-runner-agent-acme-bot-runner"),
            "{stale:?}"
        );
        assert!(stale[0].contains(OWNER_LAYER), "{stale:?}");
    }

    /// #4321 review P1: with the platform template gone and a stale per-agent
    /// template left behind, that agent template is not mistaken for the
    /// platform one, so a cleared agent cannot pass on an absent name.
    #[test]
    fn template_canary_flags_a_missing_platform_template_beside_a_stale_agent_template() {
        let expected = [("old".to_string(), ExpectedRunner::PlatformRunner)];
        let found = issues_for(
            "rel-curie",
            &expected,
            &[("rel-curie-agent-old-runner", OWNER_LAYER)],
        );
        assert!(
            found.iter().any(|issue| issue.contains("missing")
                && (issue.contains("rel-curie-runner") || issue.contains("platform"))),
            "the missing platform template must be named: {found:?}"
        );
    }

    fn effective_fullname(defaults: serde_json::Value, overlay: serde_json::Value) -> String {
        release_fullname_from_values("rel", &effective_name_values(Some(&defaults), &overlay))
    }

    #[test]
    fn empty_fullname_override_in_overlay_beats_chart_default() {
        let defaults = serde_json::json!({"fullnameOverride": "acme-runner"});
        let overlay = serde_json::json!({"fullnameOverride": ""});
        assert_eq!(effective_fullname(defaults, overlay), "rel-curie");
    }

    #[test]
    fn null_fullname_override_in_overlay_deletes_chart_default() {
        let defaults = serde_json::json!({"fullnameOverride": "acme-runner"});
        let overlay = serde_json::json!({"fullnameOverride": null});
        assert_eq!(effective_fullname(defaults, overlay), "rel-curie");
    }

    #[test]
    fn absent_fullname_override_in_overlay_falls_back_to_chart_default() {
        let defaults = serde_json::json!({"fullnameOverride": "acme-runner"});
        let overlay = serde_json::json!({"other": "x"});
        assert_eq!(effective_fullname(defaults, overlay), "acme-runner");
    }

    #[test]
    fn empty_name_override_in_overlay_beats_chart_default() {
        let defaults = serde_json::json!({"nameOverride": "acme"});
        let overlay = serde_json::json!({"nameOverride": ""});
        assert_eq!(effective_fullname(defaults, overlay), "rel-curie");
    }

    #[test]
    fn null_name_override_in_overlay_deletes_chart_default() {
        let defaults = serde_json::json!({"nameOverride": "acme"});
        let overlay = serde_json::json!({"nameOverride": null});
        assert_eq!(effective_fullname(defaults, overlay), "rel-curie");
    }

    #[test]
    fn absent_name_override_in_overlay_falls_back_to_chart_default() {
        let defaults = serde_json::json!({"nameOverride": "acme"});
        let overlay = serde_json::json!({});
        assert_eq!(effective_fullname(defaults, overlay), "rel-acme");
    }
}

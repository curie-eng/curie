//! `curie cluster upgrade`: one resumable lifecycle that plans, validates,
//! drains, checkpoints, migrates, applies, proves exact convergence, runs a
//! canary, and records the new known-good version (issue #2301).
//!
//! Sibling slices this module composes and does not reimplement:
//! - versioned configuration migrations (#2299)
//! - database compatibility windows (#2300)
//! - the kind released-install upgrade CI rung (#2097)
//!
//! Helm owns the existing drain and migration hooks. The live driver records
//! transaction intent before Helm and completed milestones only after observing
//! successful hook/schema state. Independent hook interruption remains a
//! separate required released-upgrade matrix.

use super::upgrade_recovery::{self as recovery, Operation};
use anyhow::{bail, Context, Result};
use base64::Engine;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::command::{
    mask_secret, plain, require_on_path, run_capture, run_upgrade_capture, CommonOpts, OpsCommand,
};

/// Durable phases of a cluster upgrade. Order is load-bearing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UpgradePhase {
    Plan,
    Validate,
    Drain,
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
        UpgradePhase::Drain,
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
            UpgradePhase::Drain => "drain",
            UpgradePhase::Checkpoint => "checkpoint",
            UpgradePhase::Migrate => "migrate",
            UpgradePhase::Apply => "apply",
            UpgradePhase::Converge => "converge",
            UpgradePhase::Canary => "canary",
            UpgradePhase::Commit => "commit",
        }
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
    pub chart: Option<String>,
    pub yes: bool,
    pub forward_only: bool,
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
    drain_completed: bool,
    convergence: Option<Convergence>,
    canary: Option<Canary>,
    fail_forward: Option<FailForward>,
    resumed: bool,
    #[serde(default)]
    schema_decision: Option<serde_json::Value>,
    #[serde(default)]
    target_identity: Option<String>,
    #[serde(default)]
    helm_started: bool,
    #[serde(default)]
    retained_agents_fingerprint: Option<String>,
    #[serde(default)]
    operation: Option<Operation>,
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
    #[serde(default)]
    pub observed_images: Vec<super::upgrade_images::ObservedImage>,
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
            observed_images: Vec::new(),
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
            "observed_images": self.observed_images,
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
        convergence: Option<Box<Convergence>>,
        canary: Option<Canary>,
        fail_forward: Option<FailForward>,
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
                    "convergence": convergence.as_deref().map(Convergence::to_json),
                    "canary": canary.as_ref().map(|c| serde_json::json!({"passed": c.passed})),
                    "fail_forward": fail_forward.as_ref().map(|f| serde_json::json!({
                        "command": f.command,
                        "reason": f.reason,
                    })),
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
    fn drain_once(&mut self) -> Result<bool> {
        self.drain_calls += 1;
        Ok(self.in_flight.is_empty())
    }
    fn apply_target(&mut self, to: &str) -> Result<()> {
        self.mutate_calls += 1;
        self.applied = true;
        self.set_current(Some(to.to_string()));
        Ok(())
    }
    fn observe_convergence(&self) -> Result<Convergence> {
        let mut conv = Convergence::exact_ok();
        if !self.converge_exact {
            conv.exact = false;
            conv.replicas = false;
        }
        if !self.manifest_matches {
            conv.exact = false;
            conv.manifest_matches = false;
        }
        Ok(conv)
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
            (None, _) => false,
            _ => false,
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

fn source_status_command(opts: &CommonOpts) -> OpsCommand {
    OpsCommand::new(
        "helm",
        vec![
            plain("status"),
            plain(&opts.release),
            plain("-n"),
            plain(&opts.namespace),
            plain("-o"),
            plain("json"),
        ],
    )
}

fn source_metadata_command(opts: &CommonOpts, revision: &str) -> OpsCommand {
    OpsCommand::new(
        "helm",
        vec![
            plain("get"),
            plain("metadata"),
            plain(&opts.release),
            plain("-n"),
            plain(&opts.namespace),
            plain("--revision"),
            plain(revision),
            plain("-o"),
            plain("json"),
        ],
    )
}

fn source_metadata_error(message: &str) -> anyhow::Error {
    crate::exit::CliError::failure(message)
        .with_fix("inspect the selected release with helm status and helm get metadata --revision for that same revision; restore read access and resolve any release mismatch before rerunning this upgrade")
        .into()
}

fn plan_lines(opts: &UpgradeOpts, from: Option<&str>, secret: Option<&str>) -> Vec<String> {
    let from = from.unwrap_or(if opts.common.dry_run {
        "source not inspected"
    } else {
        "none"
    });
    let chart = opts
        .chart
        .clone()
        .unwrap_or_else(|| format!("curie-{}", opts.to));
    let mut lines = vec![
        super::upgrade_owner::capture_command().display(),
        super::upgrade_owner::namespace_command(&opts.common.namespace).display(),
        source_status_command(&opts.common).display(),
        format!("Command template (exact source/target revision resolved at execution): {}", recovery::metadata_command(&opts.common, "<verified-revision>").display()),
        format!("Conditional recovery evidence: {}", recovery::terminal_command(&opts.common).display()),
        format!("Command template (revision resolved from source Helm status at execution): {}", source_metadata_command(&opts.common, "<observed-revision>").display()),
        "Recovery preflight renders templates/worker-upgrade-drain.yaml and templates/schema-migrate.yaml from the exact target chart with a private retained-values file.".into(),
        "Conditional recovery reads metadata, values and manifest from the exact original pending Helm revision; no rollback is planned without the original completion witness.".into(),
        "Reject nonempty HELM_KUBE target/authentication overrides; use the selected kubeconfig".into(),
        "Bind all Helm/Kubernetes commands to one private captured kubeconfig; acquire same-host ownership by namespace UID and release before checkpoint mutation".into(),
        format!("phase plan: {from} -> {}", opts.to),
        "phase validate: configuration overlay and schema compatibility".into(),
        "phase drain: worker upgrade drain gate (issue 2010)".into(),
        "phase checkpoint: persist recoverable release state".into(),
        "phase migrate: one controlled schema migration".into(),
        format!(
            "helm upgrade {} {chart} -n {} --wait",
            opts.common.release, opts.common.namespace
        ),
        "phase converge: exact images, generations, replicas, unavailable=0, hooks, queues, manifest"
            .into(),
        "# Conditional image alias read: <pod-node> is resolved from selected serving Pods; this placeholder is not an executable argument and requires get-node access".into(),
        super::convergence::node_images_command("<pod-node>").display(),
        "phase canary: target-version smoke".into(),
        "phase commit: record known-good version".into(),
    ];
    if opts.common.dry_run {
        lines.push("Offline plan: source configuration and database compatibility not inspected; execution validates both before mutation".into());
    }
    if let Some(secret) = secret {
        lines.push(format!(
            "preserved credential api.credentials={}",
            mask_secret(secret)
        ));
    }
    lines
}

fn completed_output(
    record: &UpgradeRecord,
    previous_serving: bool,
    failed_phase: Option<UpgradePhase>,
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
        convergence: record.convergence.clone().map(Box::new),
        canary: record.canary.clone(),
        fail_forward: record.fail_forward.clone(),
    }
}

fn fail_forward_for(opts: &UpgradeOpts, _previous_serving: bool, reason: &str) -> FailForward {
    // Schema expansion may already have landed. A previous process still
    // serving does not establish that a chart rollback can safely restart it.
    fn quoted(value: &str) -> String {
        format!("'{}'", value.replace('\'', "'\\''"))
    }
    let mut command = format!(
        "curie cluster upgrade --to {} --release {} --namespace {} --yes",
        quoted(&opts.to),
        quoted(&opts.common.release),
        quoted(&opts.common.namespace),
    );
    if let Some(chart) = &opts.chart {
        command.push_str(&format!(" --chart {}", quoted(chart)));
    }
    FailForward {
        command,
        reason: reason.to_string(),
    }
}

trait UpgradeDriver {
    fn current(&self) -> Option<String>;
    fn set_current(&mut self, version: Option<String>);
    fn known_good(&self) -> Option<String>;
    fn set_known_good(&mut self, version: Option<String>);
    fn load_record(&self) -> Option<UpgradeRecord>;
    fn store_record(&mut self, record: UpgradeRecord) -> Result<()>;
    fn retained_agents_fingerprint(&self) -> Option<String> {
        None
    }
    fn owns_helm_transaction(&self) -> bool {
        false
    }
    fn reconcile_applied(&self) -> Result<bool> {
        Ok(false)
    }
    fn target_identity(&self) -> Option<String> {
        None
    }
    fn schema_decision(&self) -> Option<serde_json::Value> {
        None
    }
    fn validate(&mut self) -> Result<()> {
        if self.refuse_config() {
            bail!("configuration compatibility check refused the overlay before mutation");
        }
        if self.refuse_schema() {
            bail!("database/application compatibility check refused the target schema before mutation");
        }
        Ok(())
    }
    fn configuration_plan(&self) -> Vec<String> {
        Vec::new()
    }
    fn secret(&self) -> Option<&str> {
        None
    }
    fn redact(&self, text: &str) -> String {
        match self.secret() {
            Some(secret) => text.replace(secret, &mask_secret(secret)),
            None => text.to_string(),
        }
    }
    fn refuse_config(&self) -> bool {
        false
    }
    fn refuse_schema(&self) -> bool {
        false
    }
    fn drain_once(&mut self) -> Result<bool>;
    fn apply_target(&mut self, to: &str) -> Result<()>;
    fn observe_convergence(&self) -> Result<Convergence>;
    fn run_canary(&self) -> Result<Canary>;
    fn serving_previous(&self) -> bool;
    fn interrupt_after(&self) -> Option<UpgradePhase> {
        None
    }
    fn fail_at(&self) -> Option<UpgradePhase> {
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
    let mut plan = plan_lines(&opts, from.as_deref(), host.secret());
    plan.extend(host.configuration_plan());
    let plan: Vec<String> = plan.into_iter().map(|l| host.redact(&l)).collect();

    if opts.common.dry_run {
        return Ok(ClusterUpgradeOutput::DryRun(crate::ui::DryRunPlan {
            lines: plan,
        }));
    }

    let previous_record = host.load_record();
    if let Some(record) = &previous_record {
        if !matches!(
            record.status.as_str(),
            "in_progress" | "failed" | "succeeded"
        ) || record.completed.len() > UpgradePhase::ALL.len()
            || record.completed.as_slice() != &UpgradePhase::ALL[..record.completed.len()]
        {
            bail!("upgrade checkpoint phases are not a valid durable prefix; preserve the record before retrying");
        }
    }
    let unchanged_helm = host.owns_helm_transaction()
        && previous_record.as_ref().is_some_and(|record| {
            record.status == "succeeded"
                && record.target_version == opts.to
                && record.target_identity == host.target_identity()
                && host.current().as_deref() == Some(opts.to.as_str())
        });
    let mut record = match previous_record {
        Some(existing)
            if existing.target_version == opts.to
                && matches!(existing.status.as_str(), "in_progress" | "failed") =>
        {
            if host.owns_helm_transaction() && existing.target_identity != host.target_identity() {
                bail!("upgrade target artifacts or retained configuration changed; preserve the checkpoint and restore the original target before resuming");
            }
            let mut existing = existing;
            existing.resumed = true;
            existing.status = "in_progress".into();
            existing.fail_forward = None;
            // Observations can go stale while the CLI is stopped. Resume
            // idempotent writes, but always re-prove live convergence/canary.
            existing.completed.retain(|phase| {
                !matches!(
                    phase,
                    UpgradePhase::Converge | UpgradePhase::Canary | UpgradePhase::Commit
                )
            });
            existing.convergence = None;
            existing.canary = None;
            existing.schema_decision = host.schema_decision();
            existing
        }
        Some(existing)
            if existing.target_version != opts.to
                && matches!(existing.status.as_str(), "in_progress" | "failed") =>
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
            schema_decision: host.schema_decision(),
            target_identity: host.target_identity(),
            helm_started: unchanged_helm,
            retained_agents_fingerprint: host.retained_agents_fingerprint(),
            operation: None,
        },
    };

    let same_version = from.as_deref() == Some(opts.to.as_str())
        && host.known_good().as_deref() == Some(opts.to.as_str());

    for phase in remaining_after(&record.completed) {
        if record.completed.contains(&phase) {
            continue;
        }
        if phase == UpgradePhase::Drain && host.owns_helm_transaction() {
            // Helm owns drain -> migration -> rollout. Persist intent before
            // starting it; these milestones are complete only after observed
            // successful hooks and target release state, never before Helm.
            let reconciled = record.helm_started && host.reconcile_applied()?;
            if !reconciled {
                record.helm_started = true;
                host.store_record(record.clone())?;
                host.apply_target(&opts.to)?;
                if !host.reconcile_applied()? {
                    bail!("Helm returned without verifiable target hooks/schema; preserve the checkpoint and resume after inspecting the release");
                }
            }
            record.drain_completed = true;
            record.completed.extend([
                UpgradePhase::Drain,
                UpgradePhase::Checkpoint,
                UpgradePhase::Migrate,
                UpgradePhase::Apply,
            ]);
            host.store_record(record.clone())?;
            continue;
        }
        if same_version
            && !host.owns_helm_transaction()
            && matches!(
                phase,
                UpgradePhase::Drain
                    | UpgradePhase::Checkpoint
                    | UpgradePhase::Migrate
                    | UpgradePhase::Apply
            )
        {
            record.completed.push(phase);
            host.store_record(record.clone())?;
            continue;
        }
        if phase == UpgradePhase::Drain && from.is_none() {
            record.completed.push(phase);
            host.store_record(record.clone())?;
            continue;
        }

        match execute_phase(phase, &opts, host, &mut record)? {
            PhaseOutcome::Continue => {
                record.completed.push(phase);
                // Validation must precede the first cluster write, including
                // the checkpoint itself. Plan is included in the first record
                // persisted after validation succeeds.
                if phase != UpgradePhase::Plan {
                    host.store_record(record.clone())?;
                }
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
                host.store_record(record.clone())?;
                return Ok(completed_output(&record, previous, Some(phase)));
            }
        }
    }

    record.status = "succeeded".into();
    record.known_good_version = Some(opts.to.clone());
    host.set_known_good(Some(opts.to.clone()));
    host.store_record(record.clone())?;
    Ok(completed_output(&record, true, None))
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
            host.validate()?;
            Ok(PhaseOutcome::Continue)
        }
        UpgradePhase::Drain => {
            if record.drain_completed {
                return Ok(PhaseOutcome::Continue);
            }
            if !host.drain_once()? {
                record.fail_forward = Some(fail_forward_for(
                    opts,
                    true,
                    "accepted work is still in flight; retry once those deliveries settle",
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
            let conv = host.observe_convergence()?;
            record.convergence = Some(conv.clone());
            if !conv.exact {
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
            host.set_known_good(Some(opts.to.clone()));
            record.known_good_version = Some(opts.to.clone());
            Ok(PhaseOutcome::Continue)
        }
    }
}

fn hash_chart(path: &std::path::Path, digest: &mut Sha256) -> Result<()> {
    let metadata =
        std::fs::symlink_metadata(path).context("could not inspect target chart artifact")?;
    if metadata.file_type().is_symlink() {
        bail!("target chart artifact cannot contain unresolved symlinks");
    }
    if metadata.is_dir() {
        let mut entries = std::fs::read_dir(path)?.collect::<std::io::Result<Vec<_>>>()?;
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            let name = entry.file_name();
            let bytes = name.as_encoded_bytes();
            digest.update((bytes.len() as u64).to_be_bytes());
            digest.update(bytes);
            hash_chart(&entry.path(), digest)?;
        }
    } else if metadata.is_file() {
        digest.update(metadata.len().to_be_bytes());
        digest.update(std::fs::read(path)?);
    } else {
        bail!("target chart artifact contains an unsupported filesystem entry");
    }
    Ok(())
}

fn checkpoint_name(release: &str) -> String {
    format!("{release}-upgrade-checkpoint")
}

fn target_image_error(message: &str) -> anyhow::Error {
    crate::exit::CliError::failure(message)
        .with_fix("select the target package's declared API image and matching schema metadata, then rerun the same upgrade command; custom images require separately verified compatibility metadata")
        .into()
}

fn contains_manifest(actual: &serde_json::Value, expected: &serde_json::Value) -> bool {
    match (actual, expected) {
        (serde_json::Value::Object(actual), serde_json::Value::Object(expected)) => {
            expected.iter().all(|(key, value)| {
                actual
                    .get(key)
                    .is_some_and(|item| contains_manifest(item, value))
            })
        }
        (serde_json::Value::Array(actual), serde_json::Value::Array(expected)) => {
            actual.len() == expected.len()
                && actual
                    .iter()
                    .zip(expected)
                    .all(|(a, e)| contains_manifest(a, e))
        }
        _ => actual == expected,
    }
}

fn object_identity(value: &serde_json::Value) -> Option<(&str, &str)> {
    Some((
        value.get("kind")?.as_str()?,
        value.pointer("/metadata/name")?.as_str()?,
    ))
}

fn hooks_succeeded(status: &serde_json::Value) -> bool {
    status.pointer("/info/status").and_then(|v| v.as_str()) == Some("deployed")
        && status
            .get("hooks")
            .and_then(|v| v.as_array())
            .is_some_and(|hooks| {
                hooks.iter().all(|hook| {
                    let relevant = hook
                        .get("events")
                        .and_then(|v| v.as_array())
                        .into_iter()
                        .flatten()
                        .any(|event| {
                            matches!(
                                event.as_str(),
                                Some("pre-upgrade" | "post-upgrade" | "post-install")
                            )
                        });
                    !relevant
                        || hook.pointer("/last_run/phase").and_then(|v| v.as_str())
                            == Some("Succeeded")
                })
            })
}

fn workload_images(value: &serde_json::Value) -> Vec<(&str, &str)> {
    ["containers", "initContainers"]
        .into_iter()
        .flat_map(|field| {
            value
                .pointer(&format!("/spec/template/spec/{field}"))
                .and_then(|v| v.as_array())
                .into_iter()
                .flatten()
                .filter_map(|container| {
                    Some((
                        container.get("name")?.as_str()?,
                        container.get("image")?.as_str()?,
                    ))
                })
        })
        .collect()
}

#[derive(Deserialize)]
struct SchemaRevision {
    revision: String,
    parents: Vec<String>,
    kind: String,
    sha256: String,
}

#[derive(Deserialize)]
struct SchemaMetadata {
    schema_min: String,
    schema_head: String,
    revisions: Vec<SchemaRevision>,
}

impl SchemaMetadata {
    fn ancestors(
        &self,
        revision: &str,
        visiting: &mut std::collections::BTreeSet<String>,
        ordered: &mut Vec<String>,
    ) -> Result<()> {
        if ordered.iter().any(|item| item == revision) {
            return Ok(());
        }
        if !visiting.insert(revision.to_owned()) {
            bail!("target schema graph contains a cycle");
        }
        let item = self
            .revisions
            .iter()
            .find(|item| item.revision == revision)
            .context("database revision is unknown to the target schema graph")?;
        for parent in &item.parents {
            self.ancestors(parent, visiting, ordered)?;
        }
        visiting.remove(revision);
        ordered.push(revision.to_owned());
        Ok(())
    }

    fn plan(&self, source: &serde_json::Value, forward_only: bool) -> Result<serde_json::Value> {
        let mut seen = std::collections::BTreeSet::new();
        for revision in &self.revisions {
            if revision.revision.is_empty()
                || !seen.insert(&revision.revision)
                || !matches!(
                    revision.kind.as_str(),
                    "expand" | "contract" | "irreversible"
                )
            {
                bail!("target schema revision metadata is invalid");
            }
        }
        let mut target = Vec::new();
        self.ancestors(
            &self.schema_head,
            &mut std::collections::BTreeSet::new(),
            &mut target,
        )?;
        if !target.contains(&self.schema_min) {
            bail!("target schema minimum is outside its revision graph");
        }
        let current = source.get("current_revision").and_then(|v| v.as_str());
        let mut applied = Vec::new();
        if let Some(current) = current {
            if !target.iter().any(|revision| revision == current) {
                bail!("database revision is outside the target schema ancestry; downgrade or unknown schema is not an automatic upgrade");
            }
            self.ancestors(
                current,
                &mut std::collections::BTreeSet::new(),
                &mut applied,
            )?;
        }
        if current.is_some() {
            let source_revisions = source.get("source_revisions").and_then(|value| value.as_object())
                .context("serving API did not provide migration content identity; compatibility is unverified")?;
            for (revision, digest) in source_revisions {
                if applied.contains(revision) {
                    let expected = self
                        .revisions
                        .iter()
                        .find(|item| &item.revision == revision)
                        .expect("validated ancestry");
                    if digest.as_str() != Some(expected.sha256.as_str()) {
                        bail!("serving and target images disagree on migration content for revision {revision}; upgrade refused before mutation");
                    }
                }
            }
            if source_revisions.is_empty() {
                bail!("serving API migration identity is empty");
            }
        }
        let pending: Vec<_> = target
            .iter()
            .filter(|revision| !applied.contains(revision))
            .map(|revision| {
                self.revisions
                    .iter()
                    .find(|item| &item.revision == revision)
                    .expect("validated graph")
            })
            .collect();
        let destructive = pending.iter().any(|item| item.kind != "expand");
        if current.is_some() && destructive && !forward_only {
            return Err(crate::exit::CliError::failure("pending contract or irreversible schema migration requires explicit api.migrate.forwardOnly before any mutation")
                .with_fix("review the target contract migrations and retained-data backup, then rerun the same cluster upgrade command with --forward-only if forward-only migration is intended; otherwise select a compatible target")
                .into());
        }
        Ok(serde_json::json!({
            "decision": if pending.is_empty() { "noop" } else { "apply" },
            "current_revision": current, "source_head": source.get("source_head"),
            "source_metadata": {
                "source_head": source.get("source_head"),
                "source_revisions": source.get("source_revisions"),
                "schema_window": source.get("schema_window"),
                "database_endpoint_fingerprint": source.get("database_endpoint_fingerprint"),
            },
            "target_min": self.schema_min, "target_head": self.schema_head,
            "pending": pending.iter().map(|item| serde_json::json!({"revision": item.revision, "kind": item.kind})).collect::<Vec<_>>(),
            "forward_only": forward_only, "rollback_compatible": current.is_some() && !destructive && source.get("schema_window").is_some_and(|window| window.is_object()),
        }))
    }
}

const SCHEMA_PROBE: &str = r#"
import asyncio, hashlib, json
from importlib.resources import files
from pathlib import Path
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine
from curie_api.config import get_settings
from alembic.config import Config
from alembic.script import ScriptDirectory
async def probe():
    engine = create_async_engine(get_settings().database_url)
    try:
        async with engine.connect() as connection:
            exists = (await connection.execute(text("SELECT to_regclass('curie.alembic_version')"))).scalar()
            rows = (await connection.execute(text('SELECT version_num FROM curie.alembic_version'))).scalars().all() if exists else []
            assert len(rows) <= 1
        config = Config()
        config.set_main_option('script_location', '/app/alembic')
        script = ScriptDirectory.from_config(config)
        heads = script.get_heads()
        assert len(heads) == 1
        revisions = {revision.revision: hashlib.sha256(Path(revision.path).read_bytes()).hexdigest() for revision in script.walk_revisions()}
        window_file = files('curie_api').joinpath('schema_compat.json')
        window = json.loads(window_file.read_text()) if window_file.is_file() else None
        url = make_url(get_settings().database_url)
        endpoint = hashlib.sha256(json.dumps([url.host, url.port or 5432, url.database], separators=(',', ':')).encode()).hexdigest()
        print(json.dumps({'current_revision': rows[0] if rows else None, 'source_head': heads[0], 'source_revisions': revisions, 'schema_window': window, 'database_endpoint_fingerprint': endpoint}))
    finally:
        await engine.dispose()
asyncio.run(probe())
"#;

// These probes run inside the already owned service containers. Credentials
// remain in their environment and neither probe emits credentials or row data.
const DATABASE_RECOVERY_PROBE: &str = r#"
export PGPASSWORD="$POSTGRES_PASSWORD"
export PGOPTIONS='-c default_transaction_read_only=on'
exec psql -X -A -t --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -v ON_ERROR_STOP=1 -c "SELECT json_build_object('current_revision', (SELECT version_num FROM curie.alembic_version), 'database_name', current_database())"
"#;

const QUEUE_PROBE: &str = r#"
import asyncio, json
from curie_worker.config import WorkerConfig
from curie_worker.upgrade_drain import UpgradeDrainGate, _client
async def probe():
    config = WorkerConfig()
    client = _client(config)
    try:
        gate = UpgradeDrainGate(client, config)
        print(json.dumps({'queues_drained': not await gate.unsettled_deliveries()}))
    finally:
        await client.aclose()
asyncio.run(probe())
"#;

const API_CANARY: &str = r#"
import hashlib, json, urllib.request
from curie_api.config import get_settings
settings = get_settings()
request = urllib.request.Request('http://127.0.0.1:8000/agents', headers={'X-API-Key': settings.api_key})
with urllib.request.urlopen(request, timeout=15) as response:
    agents = json.load(response)
    assert response.status == 200 and isinstance(agents, list)
with urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=15) as response:
    assert response.status == 200 and json.load(response).get('status') == 'ok'
identities = sorted(str(agent['id']) for agent in agents)
assert len(set(identities)) == len(identities)
fingerprint = hashlib.sha256(json.dumps(identities, separators=(',', ':')).encode()).hexdigest()
print(json.dumps({'passed': True, 'agents_fingerprint': fingerprint}))
"#;

struct LiveHost {
    owner: Option<super::upgrade_owner::UpgradeOwner>,
    opts: UpgradeOpts,
    current: Option<String>,
    known_good: Option<String>,
    record: Option<UpgradeRecord>,
    record_version: Option<String>,
    record_uid: Option<String>,
    operation: Option<Operation>,
    recovery_revision: Option<u64>,
    schema_decision: Option<serde_json::Value>,
    target_identity: Option<String>,
    retained_agents_fingerprint: Option<String>,
    config: Option<crate::config_migrate::MigrationOutcome>,
}

impl LiveHost {
    fn run(&self, cmd: &OpsCommand) -> Result<(bool, String, String)> {
        let command = self
            .owner
            .as_ref()
            .map_or_else(|| cmd.clone(), |owner| owner.bind(cmd));
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(run_capture(&command))
        })
    }

    fn inspect_version(&self) -> Result<Option<String>> {
        let cmd = OpsCommand::new(
            "helm",
            vec![
                plain("list"),
                plain("--all"),
                plain("--filter"),
                plain(format!("^{}$", regex::escape(&self.opts.common.release))),
                plain("-n"),
                plain(&self.opts.common.namespace),
                plain("-o"),
                plain("json"),
            ],
        );
        let (ok, out, _) = self.run(&cmd)?;
        if !ok {
            bail!("could not inspect the installed Helm release; verify cluster access before retrying");
        }
        let releases: Vec<serde_json::Value> =
            serde_json::from_str(&out).context("installed Helm release list is not valid JSON")?;
        let Some(release) = releases.iter().find(|item| {
            item.get("name").and_then(|name| name.as_str())
                == Some(self.opts.common.release.as_str())
        }) else {
            return Ok(None);
        };
        let chart = release
            .get("chart")
            .and_then(|v| v.as_str())
            .context("installed Helm release has no chart version")?;
        let version = chart
            .strip_prefix("curie-")
            .filter(|version| !version.is_empty())
            .context("installed Helm release is not a versioned Curie chart")?;
        Ok(Some(version.to_owned()))
    }

    fn load_record(&mut self) -> Result<Option<UpgradeRecord>> {
        let cmd = OpsCommand::new(
            "kubectl",
            vec![
                plain("get"),
                plain("configmap"),
                plain(checkpoint_name(&self.opts.common.release)),
                plain("-n"),
                plain(&self.opts.common.namespace),
                plain("-o"),
                plain("json"),
                plain("--ignore-not-found"),
            ],
        );
        let (ok, out, _) = self.run(&cmd)?;
        if !ok {
            bail!("could not read the upgrade checkpoint; verify cluster access before retrying");
        }
        if out.trim().is_empty() {
            return Ok(None);
        }
        let object: serde_json::Value = serde_json::from_str(&out)
            .context("upgrade checkpoint response is malformed; preserve it before retrying")?;
        let version = object
            .pointer("/metadata/resourceVersion")
            .and_then(|value| value.as_str())
            .filter(|value| !value.is_empty())
            .context("upgrade checkpoint has no resource version; preserve it before retrying")?;
        let record = serde_json::from_str(
            object
                .pointer("/data/record")
                .and_then(|value| value.as_str())
                .context("upgrade checkpoint has no record")?,
        )
        .context(
            "upgrade checkpoint is malformed; preserve it and repair the record before retrying",
        )?;
        self.record_uid = object
            .pointer("/metadata/uid")
            .and_then(|value| value.as_str())
            .filter(|uid| !uid.is_empty())
            .map(str::to_owned);
        self.record_version = Some(version.to_owned());
        Ok(Some(record))
    }

    fn recovery_read(&self, command: &OpsCommand) -> Result<serde_json::Value> {
        let command = self
            .owner
            .as_ref()
            .ok_or_else(|| {
                recovery::refusal("upgrade recovery requires captured target ownership")
            })?
            .bind(command);
        let (ok, raw, _) = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(tokio::time::timeout(
                std::time::Duration::from_secs(10),
                run_capture(&command),
            ))
        })
        .map_err(|_| recovery::refusal("upgrade recovery evidence read timed out"))?
        .map_err(|_| recovery::refusal("upgrade recovery evidence read failed"))?;
        if !ok {
            return Err(recovery::refusal(
                "upgrade recovery evidence read was denied or failed",
            ));
        }
        serde_json::from_str(&raw)
            .map_err(|_| recovery::refusal("upgrade recovery evidence is malformed"))
    }

    fn prepare_operation(&mut self) -> Result<()> {
        let owner = self.owner.as_ref().context("upgrade owner absent")?;
        let local = owner.read_witness()?;
        if let Some(record) = self
            .record
            .as_ref()
            .filter(|record| record.target_version == self.opts.to)
        {
            if let Some(operation) = &record.operation {
                if record.status != "succeeded" {
                    let saved: Operation = local
                        .ok_or_else(|| recovery::refusal("local upgrade witness is missing"))
                        .and_then(|value| {
                            serde_json::from_value(value).map_err(|_| {
                                recovery::refusal("local upgrade witness is malformed")
                            })
                        })?;
                    let mut comparable = saved.clone();
                    if !record.helm_started && comparable.checkpoint_uid.is_empty() {
                        comparable.checkpoint_uid = operation.checkpoint_uid.clone();
                    }
                    if &comparable != operation
                        || (record.helm_started
                            && self.record_uid.as_deref()
                                != Some(operation.checkpoint_uid.as_str()))
                    {
                        return Err(recovery::refusal(
                            "local and cluster upgrade witnesses do not match",
                        ));
                    }
                }
                if record.status != "succeeded" && operation.rollback_started {
                    let expected = operation.expected_revision + 1;
                    let uid = operation.completed_revision_uid.as_deref().ok_or_else(|| recovery::refusal("rollback completion was not durably bound; automatic recovery is unsupported"))?;
                    let status = self.recovery_read(&source_status_command(&self.opts.common))?;
                    let metadata = self.recovery_read(&recovery::metadata_command(
                        &self.opts.common,
                        &expected.to_string(),
                    ))?;
                    if status["version"].as_u64() != Some(expected)
                        || status
                            .pointer("/info/status")
                            .and_then(|value| value.as_str())
                            != Some("deployed")
                        || recovery::metadata(
                            &metadata,
                            &self.opts.common,
                            expected,
                            "deployed",
                            Some(&operation.id),
                        )? != uid
                    {
                        return Err(recovery::refusal(
                            "completed rollback revision identity changed",
                        ));
                    }
                }
                self.operation = Some(operation.clone());
                return Ok(());
            }
        }
        let status = self.recovery_read(&source_status_command(&self.opts.common))?;
        let state = status
            .pointer("/info/status")
            .and_then(|value| value.as_str())
            .unwrap_or_default();
        if state.starts_with("pending-") {
            return Err(recovery::refusal(
                "pending Helm operation has no matching durable local witness",
            ));
        }
        let revision = status["version"]
            .as_u64()
            .filter(|n| *n > 0 && *n < u64::MAX - 1)
            .ok_or_else(|| recovery::refusal("source Helm revision is invalid"))?;
        if status["name"] != self.opts.common.release
            || status["namespace"] != self.opts.common.namespace
        {
            return Err(recovery::refusal("source Helm identity is invalid"));
        }
        let metadata = self.recovery_read(&recovery::metadata_command(
            &self.opts.common,
            &revision.to_string(),
        ))?;
        let uid = recovery::metadata(&metadata, &self.opts.common, revision, state, None)?;
        let id = uuid::Uuid::new_v4().to_string();
        if metadata[0]["labels"][recovery::LABEL] == id {
            return Err(recovery::refusal(
                "new upgrade operation marker is not fresh",
            ));
        }
        self.operation = Some(Operation {
            id,
            source_revision: revision,
            source_uid: uid,
            expected_revision: revision + 1,
            checkpoint_uid: self.record_uid.clone().unwrap_or_default(),
            target_identity: String::new(),
            hooks_identity: String::new(),
            pending_uid: None,
            original_hook_uids: std::collections::BTreeMap::new(),
            pending_manifest_identity: None,
            completed_revision_uid: None,
            rollback_started: false,
        });
        Ok(())
    }

    fn target_recovery_hooks(&self) -> Result<Vec<serde_json::Value>> {
        let config = &self.config.as_ref().context("configuration absent")?.values;
        let mut hooks = Vec::new();
        for template in [
            "templates/worker-upgrade-drain.yaml",
            "templates/schema-migrate.yaml",
        ] {
            let raw = self.render_target_template(template, config)?;
            for document in serde_norway::Deserializer::from_str(&raw) {
                let value = serde_json::Value::deserialize(document)
                    .map_err(|_| recovery::refusal("target recovery hook manifest is malformed"))?;
                if !value.is_null() {
                    hooks.push(value);
                }
            }
        }
        let operation = self.operation.as_ref().context("operation absent")?;
        recovery::validate_hooks(&hooks, &operation.id, &self.opts.common.namespace)?;
        hooks.sort_by_key(|hook| {
            hook["metadata"]["name"]
                .as_str()
                .unwrap_or_default()
                .to_owned()
        });
        Ok(hooks)
    }

    fn bind_operation_target(&mut self) -> Result<()> {
        if self.operation.is_none() {
            return Ok(());
        }
        let hooks_identity = recovery::digest(&self.target_recovery_hooks()?)?;
        let target = self
            .target_identity
            .clone()
            .context("target identity absent")?;
        let operation = self.operation.as_mut().context("operation absent")?;
        if !operation.target_identity.is_empty()
            && (operation.target_identity != target || operation.hooks_identity != hooks_identity)
        {
            return Err(recovery::refusal(
                "upgrade operation target or hook content changed",
            ));
        }
        operation.target_identity = target;
        operation.hooks_identity = hooks_identity;
        Ok(())
    }

    fn bind_original_completion_if_verified(&mut self) -> Result<()> {
        if self.operation.is_some() {
            if let Ok((uid, manifest, hook_uids)) = self.observe_original_completion() {
                let operation = self.operation.as_mut().expect("operation present");
                operation.pending_uid = Some(uid);
                operation.pending_manifest_identity = Some(manifest);
                operation.original_hook_uids = hook_uids;
                self.store_record(self.record.clone().context("checkpoint absent")?)?;
            }
        }
        Ok(())
    }

    fn observe_original_completion(
        &self,
    ) -> Result<(String, String, std::collections::BTreeMap<String, String>)> {
        let operation = self.operation.as_ref().context("operation absent")?;
        let status = self.recovery_read(&source_status_command(&self.opts.common))?;
        if status["version"].as_u64() != Some(operation.expected_revision)
            || status
                .pointer("/info/status")
                .and_then(|value| value.as_str())
                != Some("pending-upgrade")
        {
            return Err(recovery::refusal(
                "original invocation did not leave its expected pending revision",
            ));
        }
        let metadata = self.recovery_read(&recovery::metadata_command(
            &self.opts.common,
            &operation.expected_revision.to_string(),
        ))?;
        let uid = recovery::metadata(
            &metadata,
            &self.opts.common,
            operation.expected_revision,
            "pending-upgrade",
            Some(&operation.id),
        )?;
        let mut hooks = recovery::hook_manifests(&status, &operation.id)?;
        recovery::validate_hooks(&hooks, &operation.id, &self.opts.common.namespace)?;
        hooks.sort_by_key(|hook| {
            hook["metadata"]["name"]
                .as_str()
                .unwrap_or_default()
                .to_owned()
        });
        if recovery::digest(&hooks)? != operation.hooks_identity {
            return Err(recovery::refusal(
                "original pending hook content differs from selected target",
            ));
        }
        let observed = self.recovery_read(&recovery::terminal_command(&self.opts.common))?;
        recovery::all_terminal(
            &hooks,
            &observed,
            &operation.id,
            &self.opts.common.namespace,
        )?;
        let manifest = self.pending_manifest_identity(operation.expected_revision)?;
        let after = self.recovery_read(&recovery::metadata_command(
            &self.opts.common,
            &operation.expected_revision.to_string(),
        ))?;
        if metadata != after {
            return Err(recovery::refusal(
                "pending revision changed during original completion observation",
            ));
        }
        Ok((uid, manifest, recovery::job_uids(&hooks, &observed)?))
    }

    fn pending_manifest_identity(&self, revision: u64) -> Result<String> {
        let command = OpsCommand::new(
            "helm",
            vec![
                plain("get"),
                plain("manifest"),
                plain(&self.opts.common.release),
                plain("-n"),
                plain(&self.opts.common.namespace),
                plain("--revision"),
                plain(revision.to_string()),
            ],
        );
        let command = self.owner.as_ref().context("owner absent")?.bind(&command);
        let (ok, raw, _) = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(tokio::time::timeout(
                std::time::Duration::from_secs(10),
                run_capture(&command),
            ))
        })
        .map_err(|_| recovery::refusal("pending Helm manifest read timed out"))?
        .map_err(|_| recovery::refusal("pending Helm manifest read failed"))?;
        if !ok || raw.trim().is_empty() {
            return Err(recovery::refusal("pending Helm manifest is unavailable"));
        }
        recovery::digest(&raw)
    }

    fn validate_pending_recovery(&mut self, status: &serde_json::Value) -> Result<()> {
        let operation = self
            .operation
            .as_ref()
            .ok_or_else(|| recovery::refusal("pending operation has no witness"))?;
        let record = self
            .record
            .as_ref()
            .ok_or_else(|| recovery::refusal("pending operation has no checkpoint"))?;
        if status
            .pointer("/info/status")
            .and_then(|value| value.as_str())
            != Some("pending-upgrade")
            || operation.rollback_started
            || status["version"].as_u64() != Some(operation.expected_revision)
            || status["name"] != self.opts.common.release
            || status["namespace"] != self.opts.common.namespace
            || !record.helm_started
            || !matches!(record.status.as_str(), "failed" | "in_progress")
            || record.target_identity.as_deref() != Some(operation.target_identity.as_str())
            || self.record_uid.as_deref() != Some(operation.checkpoint_uid.as_str())
        {
            return Err(recovery::refusal(
                "pending Helm revision is not the exact original recoverable attempt",
            ));
        }
        let metadata = self.recovery_read(&recovery::metadata_command(
            &self.opts.common,
            &operation.expected_revision.to_string(),
        ))?;
        let uid = recovery::metadata(
            &metadata,
            &self.opts.common,
            operation.expected_revision,
            "pending-upgrade",
            Some(&operation.id),
        )?;
        if operation.pending_uid.as_deref() != Some(uid.as_str()) {
            return Err(recovery::refusal(
                "pending Helm UID was not bound by the original invocation",
            ));
        }
        let manifest = self.pending_manifest_identity(operation.expected_revision)?;
        if operation.pending_manifest_identity.as_deref() != Some(manifest.as_str()) {
            return Err(recovery::refusal(
                "pending Helm manifest content changed after original invocation",
            ));
        }
        let mut hooks = recovery::hook_manifests(status, &operation.id)?;
        recovery::validate_hooks(&hooks, &operation.id, &self.opts.common.namespace)?;
        hooks.sort_by_key(|hook| {
            hook["metadata"]["name"]
                .as_str()
                .unwrap_or_default()
                .to_owned()
        });
        if recovery::digest(&hooks)? != operation.hooks_identity {
            return Err(recovery::refusal(
                "pending Helm hook content differs from the verified target",
            ));
        }
        let values = self.recovery_read(&OpsCommand::new(
            "helm",
            vec![
                plain("get"),
                plain("values"),
                plain(&self.opts.common.release),
                plain("-n"),
                plain(&self.opts.common.namespace),
                plain("--revision"),
                plain(operation.expected_revision.to_string()),
                plain("-o"),
                plain("json"),
            ],
        ))?;
        if Some(&values) != self.config.as_ref().map(|config| &config.values) {
            return Err(recovery::refusal(
                "pending Helm retained configuration differs from the verified target",
            ));
        }
        let observed = self.recovery_read(&recovery::terminal_command(&self.opts.common))?;
        recovery::all_terminal(
            &hooks,
            &observed,
            &operation.id,
            &self.opts.common.namespace,
        )?;
        if recovery::job_uids(&hooks, &observed)? != operation.original_hook_uids {
            return Err(recovery::refusal(
                "an original retained hook Job UID changed",
            ));
        }
        let source_metadata = self.recovery_read(&recovery::metadata_command(
            &self.opts.common,
            &operation.source_revision.to_string(),
        ))?;
        let source_state = source_metadata[0]["labels"]["status"]
            .as_str()
            .filter(|state| matches!(*state, "deployed" | "superseded" | "failed"))
            .ok_or_else(|| recovery::refusal("source release state is uncertain"))?;
        if recovery::metadata(
            &source_metadata,
            &self.opts.common,
            operation.source_revision,
            source_state,
            None,
        )? != operation.source_uid
        {
            return Err(recovery::refusal("source release UID changed"));
        }
        let target_metadata = self.recovery_read(&source_metadata_command(
            &self.opts.common,
            &operation.expected_revision.to_string(),
        ))?;
        if target_metadata["name"] != self.opts.common.release
            || target_metadata["namespace"] != self.opts.common.namespace
            || target_metadata["revision"].as_u64() != Some(operation.expected_revision)
            || target_metadata["chart"] != "curie"
            || target_metadata["version"] != self.opts.to
            || target_metadata["appVersion"] != self.opts.to
            || target_metadata["status"] != "pending-upgrade"
        {
            return Err(recovery::refusal("pending target chart metadata changed"));
        }
        if self
            .schema_decision
            .as_ref()
            .is_none_or(|decision| decision["current_revision"] != decision["target_head"])
        {
            return Err(recovery::refusal(
                "all target schema migrations must already be reached before recovery",
            ));
        }
        self.recovery_revision = Some(operation.expected_revision);
        Ok(())
    }

    fn prepare_configuration(&mut self) -> Result<()> {
        let chart = self
            .opts
            .chart
            .as_deref()
            .context("target chart was not resolved")?;
        let (ok, out, _) = self.run(&OpsCommand::new(
            "helm",
            vec![plain("show"), plain("chart"), plain(chart)],
        ))?;
        if !ok {
            bail!("could not inspect the target chart before upgrade");
        }
        let metadata: serde_json::Value =
            serde_norway::from_str(&out).context("target chart metadata is malformed")?;
        if metadata.get("name").and_then(|v| v.as_str()) != Some("curie")
            || metadata.get("version").and_then(|v| v.as_str()) != Some(self.opts.to.as_str())
            || metadata.get("appVersion").and_then(|v| v.as_str()) != Some(self.opts.to.as_str())
        {
            bail!("target chart and app versions must match --to before upgrade");
        }
        let values = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(
                super::up::fetch_release_values_with_environment(
                    &self.opts.common,
                    self.owner
                        .as_ref()
                        .map_or_else(Vec::new, |owner| owner.environment()),
                ),
            )
        })?;
        if values.is_some() != self.current.is_some() {
            bail!("installed release changed during upgrade inspection; retry before mutation");
        }
        self.config = Some(crate::config_migrate::migrate_installed_config(
            values.unwrap_or_else(|| serde_json::json!({})),
            self.current.as_deref(),
        )?);
        if [
            "/worker/deploy",
            "/worker/upgradeDrain/enabled",
            "/api/migrate/enabled",
        ]
        .iter()
        .any(|path| {
            self.config
                .as_ref()
                .expect("prepared configuration")
                .values
                .pointer(path)
                .and_then(|value| value.as_bool())
                == Some(false)
        }) {
            // Preserve ordinary supported BYO/disabled-hook upgrades. They carry
            // no automatic forward-recovery authority.
            self.operation = None;
            self.config.as_mut().expect("prepared configuration").values["upgradeRecovery"] =
                serde_json::json!({"enabled": false, "operationId": ""});
        }
        if let Some(operation) = &self.operation {
            self.config.as_mut().expect("prepared configuration").values["upgradeRecovery"] =
                serde_json::json!({"enabled": true, "operationId": operation.id});
        }
        if self.opts.forward_only {
            let values = &mut self.config.as_mut().expect("prepared configuration").values;
            for path in ["/api", "/api/migrate"] {
                if values
                    .pointer(path)
                    .is_some_and(|value| !value.is_object() && !value.is_null())
                {
                    bail!("retained {path} must be a map before setting --forward-only");
                }
            }
            values["api"]["migrate"]["forwardOnly"] = true.into();
        }
        Ok(())
    }

    fn prepare_schema(&mut self) -> Result<()> {
        let config = &self
            .config
            .as_ref()
            .context("configuration not prepared")?
            .values;
        if config.pointer("/api/deploy").and_then(|v| v.as_bool()) == Some(false) {
            bail!("transactional upgrade requires an API database probe; enable the API or use the separately owned external migration procedure");
        }
        // A retained explicit image pin can contradict the target chart metadata.
        // Refuse until the operator selects matching target artifacts.
        for component in ["api", "worker", "dispatcher", "ui", "mailAdapter"] {
            if config
                .pointer(&format!("/{component}/deploy"))
                .and_then(|v| v.as_bool())
                == Some(false)
            {
                continue;
            }
            if config
                .pointer(&format!("/{component}/image/tag"))
                .and_then(|v| v.as_str())
                .is_some_and(|tag| !tag.is_empty() && tag != self.opts.to)
                || config
                    .pointer(&format!("/{component}/image/digest"))
                    .and_then(|v| v.as_str())
                    .is_some_and(|digest| !digest.is_empty())
            {
                bail!("target artifact metadata cannot verify the retained {component} image pin; select matching target image metadata before upgrade");
            }
        }
        let uses_runner = ["/worker/deploy", "/agentSandbox/deploy"]
            .into_iter()
            .any(|path| config.pointer(path).and_then(|value| value.as_bool()) != Some(false));
        if uses_runner
            && (config
                .pointer("/agentSandbox/runner/tag")
                .and_then(|value| value.as_str())
                .is_some_and(|tag| !tag.is_empty() && tag != self.opts.to)
                || config
                    .pointer("/agentSandbox/runner/digest")
                    .and_then(|value| value.as_str())
                    .is_some_and(|digest| !digest.is_empty()))
        {
            bail!("target artifact metadata cannot verify the retained runner image pin; select matching target image metadata before upgrade");
        }
        let rendered = self.render_target_template("templates/schema-compat.yaml", config)?;
        let object: serde_json::Value =
            serde_norway::from_str(&rendered).context("target schema metadata is malformed")?;
        self.verify_target_api_image(config, &object)?;
        if object
            .pointer("/data/application-version")
            .and_then(|v| v.as_str())
            != Some(self.opts.to.as_str())
        {
            bail!("target schema metadata application version does not match --to");
        }
        let metadata: SchemaMetadata = serde_json::from_str(
            object
                .pointer("/data/compatibility.json")
                .and_then(|v| v.as_str())
                .context("target schema compatibility metadata is missing")?,
        )
        .context("target schema compatibility graph is malformed")?;
        let source = if self.current.is_some() {
            let objects = self.retained_objects()?;
            let probe = self.service_probe(&objects, "api", SCHEMA_PROBE, "upgrade-schema")?;
            let probe = match probe {
                Some(probe) if probe.get("current_revision").is_some() => probe,
                _ => self.recover_schema_source(&objects)?,
            };
            if probe
                .get("current_revision")
                .is_none_or(|value| value.as_str().is_none_or(|revision| revision.is_empty()))
            {
                bail!("database schema probe did not return a verifiable revision; upgrade refused before mutation");
            }
            probe
        } else {
            serde_json::json!({"current_revision": null, "source_head": null})
        };
        let forward_only = config
            .pointer("/api/migrate/forwardOnly")
            .and_then(|v| v.as_bool())
            == Some(true);
        self.schema_decision = Some(metadata.plan(&source, forward_only)?);
        self.target_identity = Some(self.compute_target_identity()?);
        Ok(())
    }

    fn render_target_template(&self, template: &str, config: &serde_json::Value) -> Result<String> {
        let values = super::command::SecretValuesFileGuard::write_document(config)?;
        let (ok, rendered, _) = self.run(&OpsCommand::new(
            "helm",
            vec![
                plain("template"),
                plain(&self.opts.common.release),
                plain(
                    self.opts
                        .chart
                        .as_deref()
                        .context("target chart unresolved")?,
                ),
                plain("--namespace"),
                plain(&self.opts.common.namespace),
                plain("--show-only"),
                plain(template),
                plain("-f"),
                plain(values.path().to_string_lossy().into_owned()),
            ],
        ))?;
        if !ok {
            return Err(target_image_error(
                "target chart metadata or API manifest could not be rendered before mutation",
            ));
        }
        Ok(rendered)
    }

    fn verify_target_api_image(
        &self,
        config: &serde_json::Value,
        metadata: &serde_json::Value,
    ) -> Result<()> {
        let declared = metadata
            .pointer("/data/api-image")
            .and_then(|value| value.as_str())
            .filter(|image| !image.is_empty())
            .ok_or_else(|| {
                target_image_error("target schema metadata does not declare an API image")
            })?;

        // The effective declaration is values-controlled. A repository override
        // cannot authorize itself by changing both the metadata and Deployment.
        // Bind it to this package's default API image as well. This proves chart
        // declaration parity, not a signature or immutable registry identity.
        let mut packaged_config = config.clone();
        if let Some(api) = packaged_config
            .get_mut("api")
            .and_then(|value| value.as_object_mut())
        {
            api.remove("image");
        }
        let packaged =
            self.render_target_template("templates/schema-compat.yaml", &packaged_config)?;
        let packaged: serde_json::Value = serde_norway::from_str(&packaged)
            .map_err(|_| target_image_error("packaged target schema metadata is malformed"))?;
        if packaged
            .pointer("/data/api-image")
            .and_then(|value| value.as_str())
            != Some(declared)
        {
            return Err(target_image_error(
                "retained API image does not match the target package's schema declaration",
            ));
        }

        let rendered = self.render_target_template("templates/api.yaml", config)?;
        let mut images = Vec::new();
        let mut deployments = 0;
        for document in serde_norway::Deserializer::from_str(&rendered) {
            let object = serde_json::Value::deserialize(document)
                .map_err(|_| target_image_error("target API manifest is malformed"))?;
            if object.get("kind").and_then(|value| value.as_str()) != Some("Deployment")
                || object
                    .pointer("/metadata/labels/app.kubernetes.io~1component")
                    .and_then(|value| value.as_str())
                    != Some("api")
            {
                continue;
            }
            deployments += 1;
            let containers = object
                .pointer("/spec/template/spec/containers")
                .and_then(|value| value.as_array())
                .ok_or_else(|| target_image_error("target API Deployment has no containers"))?;
            for container in containers.iter().filter(|container| {
                container.get("name").and_then(|value| value.as_str()) == Some("api")
            }) {
                images.push(
                    container
                        .get("image")
                        .and_then(|value| value.as_str())
                        .unwrap_or_default()
                        .to_owned(),
                );
            }
        }
        if deployments != 1 || images.len() != 1 || images[0] != declared {
            return Err(target_image_error(
                "target API Deployment image does not match its schema metadata",
            ));
        }
        Ok(())
    }

    fn recover_schema_source(&self, objects: &[serde_json::Value]) -> Result<serde_json::Value> {
        // A fresh operation cannot infer source compatibility from the target.
        // Recovery reuses only this attempt's verified source artifact metadata,
        // then re-reads current schema state from the same owned database endpoint.
        let record = self.record.as_ref().filter(|record| {
            record.target_version == self.opts.to
                && matches!(record.status.as_str(), "in_progress" | "failed")
                && record.helm_started
                && record.completed.len() <= UpgradePhase::ALL.len()
                && record.completed.as_slice() == &UpgradePhase::ALL[..record.completed.len()]
        }).context("installed API is unavailable and no verified incomplete upgrade checkpoint permits database recovery")?;
        if record.target_identity.as_deref() != Some(self.compute_target_identity()?.as_str()) {
            bail!("API-unavailable recovery requires the checkpoint's exact target chart and retained configuration");
        }
        let mut source = record
            .schema_decision
            .as_ref()
            .and_then(|decision| decision.get("source_metadata"))
            .filter(|source| {
                source
                    .get("source_revisions")
                    .and_then(|value| value.as_object())
                    .is_some_and(|revisions| !revisions.is_empty())
            })
            .cloned()
            .context(
                "upgrade checkpoint has no verified source metadata for API-unavailable recovery",
            )?;
        if self
            .config
            .as_ref()
            .and_then(|config| config.values.pointer("/postgres/deploy"))
            .and_then(|value| value.as_bool())
            == Some(false)
        {
            bail!("external database recovery requires its separately owned read-only procedure");
        }
        for api in objects.iter().filter(|object| {
            object.get("kind").and_then(|value| value.as_str()) == Some("Deployment")
                && object
                    .pointer("/metadata/labels/app.kubernetes.io~1component")
                    .and_then(|value| value.as_str())
                    == Some("api")
        }) {
            let name = api
                .pointer("/metadata/name")
                .and_then(|value| value.as_str())
                .context("retained API has no name for availability inspection")?;
            let (ok, raw, _) = self.run(&OpsCommand::new(
                "kubectl",
                vec![
                    plain("get"),
                    plain("deployment"),
                    plain(name),
                    plain("-n"),
                    plain(&self.opts.common.namespace),
                    plain("--ignore-not-found"),
                    plain("-o"),
                    plain("json"),
                ],
            ))?;
            if !ok {
                bail!("could not inspect API availability before database recovery");
            }
            if !raw.trim().is_empty() {
                let live: serde_json::Value =
                    serde_json::from_str(&raw).context("API availability response is malformed")?;
                if live
                    .pointer("/status/readyReplicas")
                    .and_then(|value| value.as_u64())
                    .unwrap_or(0)
                    > 0
                {
                    bail!("running API schema probe failed; repair the probe instead of using API-unavailable recovery");
                }
            }
        }
        let databases: Vec<_> = objects
            .iter()
            .filter(|object| {
                object.get("kind").and_then(|value| value.as_str()) == Some("StatefulSet")
                    && object
                        .pointer("/metadata/labels/app.kubernetes.io~1instance")
                        .and_then(|value| value.as_str())
                        == Some(self.opts.common.release.as_str())
                    && object
                        .pointer("/spec/selector/matchLabels/app.kubernetes.io~1component")
                        .and_then(|value| value.as_str())
                        == Some("postgres")
            })
            .collect();
        if databases.len() != 1 {
            bail!("API-unavailable recovery requires exactly one matching Helm-owned Postgres StatefulSet");
        }
        let database = databases[0];
        if database
            .pointer("/metadata/namespace")
            .and_then(|value| value.as_str())
            .is_some_and(|namespace| namespace != self.opts.common.namespace)
        {
            bail!("retained Postgres StatefulSet belongs to a different namespace");
        }
        let name = database
            .pointer("/metadata/name")
            .and_then(|value| value.as_str())
            .context("owned Postgres StatefulSet has no name")?;
        let service = database
            .pointer("/spec/serviceName")
            .and_then(|value| value.as_str())
            .context("owned Postgres StatefulSet has no service identity")?;
        let container = database
            .pointer("/spec/template/spec/containers")
            .and_then(|value| value.as_array())
            .into_iter()
            .flatten()
            .find(|container| {
                container.get("name").and_then(|value| value.as_str()) == Some("postgres")
            })
            .context("owned Postgres StatefulSet has no database container")?;
        let database_name = container
            .get("env")
            .and_then(|value| value.as_array())
            .into_iter()
            .flatten()
            .find(|env| env.get("name").and_then(|value| value.as_str()) == Some("POSTGRES_DB"))
            .and_then(|env| env.get("value"))
            .and_then(|value| value.as_str())
            .context("owned Postgres database name is not verifiable")?;
        let port = objects
            .iter()
            .find(|object| {
                object.get("kind").and_then(|value| value.as_str()) == Some("Service")
                    && object
                        .pointer("/metadata/name")
                        .and_then(|value| value.as_str())
                        == Some(service)
            })
            .and_then(|service| service.pointer("/spec/ports"))
            .and_then(|value| value.as_array())
            .into_iter()
            .flatten()
            .find(|port| port.get("name").and_then(|value| value.as_str()) == Some("postgres"))
            .and_then(|port| port.get("port"))
            .and_then(|value| value.as_u64())
            .context("retained Postgres service port is not verifiable")?;
        let endpoint: String = Sha256::digest(serde_json::to_vec(&serde_json::json!([
            service,
            port,
            database_name
        ]))?)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
        if source
            .get("database_endpoint_fingerprint")
            .and_then(|value| value.as_str())
            != Some(endpoint.as_str())
        {
            bail!(
                "owned Postgres endpoint does not match the checkpoint's verified source database"
            );
        }
        let (ok, raw, _) = self.run(&OpsCommand::new(
            "kubectl",
            vec![
                plain("get"),
                plain("statefulset"),
                plain(name),
                plain("-n"),
                plain(&self.opts.common.namespace),
                plain("-o"),
                plain("json"),
            ],
        ))?;
        if !ok {
            bail!("could not inspect live Postgres ownership before database recovery");
        }
        let live_database: serde_json::Value =
            serde_json::from_str(&raw).context("live Postgres ownership response is malformed")?;
        if live_database
            .pointer("/metadata/name")
            .and_then(|value| value.as_str())
            != Some(name)
            || live_database
                .pointer("/metadata/namespace")
                .and_then(|value| value.as_str())
                != Some(self.opts.common.namespace.as_str())
            || live_database
                .pointer("/metadata/annotations/meta.helm.sh~1release-name")
                .and_then(|value| value.as_str())
                != Some(self.opts.common.release.as_str())
            || live_database
                .pointer("/metadata/annotations/meta.helm.sh~1release-namespace")
                .and_then(|value| value.as_str())
                != Some(self.opts.common.namespace.as_str())
            || live_database.pointer("/spec/selector/matchLabels")
                != database.pointer("/spec/selector/matchLabels")
        {
            bail!("live Postgres ownership does not match the retained release");
        }
        let (ok, output, _) = self.run(&OpsCommand::new(
            "kubectl",
            vec![
                plain("exec"),
                plain(format!("statefulset/{name}")),
                plain("-n"),
                plain(&self.opts.common.namespace),
                plain("-c"),
                plain("postgres"),
                plain("--"),
                plain("sh"),
                plain("-c"),
                plain(DATABASE_RECOVERY_PROBE),
                plain("upgrade-database-recovery"),
            ],
        ))?;
        if !ok {
            bail!("owned database read-only recovery probe failed; checkpoint preserved without Helm mutation");
        }
        let current: serde_json::Value = serde_json::from_str(&output)
            .context("owned database recovery probe returned malformed JSON")?;
        if current
            .get("database_name")
            .and_then(|value| value.as_str())
            != Some(database_name)
        {
            bail!("queried database catalog does not match the verified retained endpoint");
        }
        let revision = current
            .get("current_revision")
            .and_then(|value| value.as_str())
            .filter(|revision| !revision.is_empty())
            .context("owned database recovery probe returned no verifiable revision")?;
        source["current_revision"] = revision.into();
        Ok(source)
    }

    fn inspect_known_good(&mut self) -> Result<Option<String>> {
        let (ok, raw, _) = self.run(&source_status_command(&self.opts.common))?;
        if !ok {
            return Err(source_metadata_error(
                "could not inspect source Helm operation state before mutation",
            ));
        }
        let status: serde_json::Value = serde_json::from_str(&raw)
            .map_err(|_| source_metadata_error("source Helm status is malformed"))?;
        let state = status
            .pointer("/info/status")
            .and_then(|value| value.as_str())
            .ok_or_else(|| source_metadata_error("source Helm status has no operation state"))?;
        if state.starts_with("pending-") {
            self.validate_pending_recovery(&status)?;
            return Ok(None);
        }
        if state != "deployed" {
            return Ok(None);
        }
        let revision = status
            .get("version")
            .and_then(|value| value.as_u64())
            .filter(|revision| *revision > 0)
            .ok_or_else(|| {
                source_metadata_error("source Helm status has no valid release revision")
            })?;
        if status.get("name").and_then(|value| value.as_str()) != Some(&self.opts.common.release)
            || status.get("namespace").and_then(|value| value.as_str())
                != Some(&self.opts.common.namespace)
        {
            return Err(source_metadata_error(
                "source Helm status does not identify the selected release",
            ));
        }
        // Helm status deliberately removes Chart from its JSON output; obtain the
        // metadata from the exact observed revision instead of guessing its version.
        // https://github.com/helm/helm/blob/v3.16.4/cmd/helm/status.go
        // https://github.com/helm/helm/blob/v3.16.4/pkg/action/get_metadata.go
        let command = source_metadata_command(&self.opts.common, &revision.to_string());
        let command = self
            .owner
            .as_ref()
            .map_or_else(|| command.clone(), |owner| owner.bind(&command));
        let (ok, raw, _) = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(tokio::time::timeout(
                std::time::Duration::from_secs(10),
                run_capture(&command),
            ))
        })
        .map_err(|_| source_metadata_error("source Helm metadata read timed out"))?
        .map_err(|_| source_metadata_error("source Helm metadata read failed"))?;
        if !ok {
            return Err(source_metadata_error("source Helm metadata read failed"));
        }
        let metadata: serde_json::Value = serde_json::from_str(&raw)
            .map_err(|_| source_metadata_error("source Helm metadata is malformed"))?;
        if metadata.get("name").and_then(|value| value.as_str()) != Some(&self.opts.common.release)
            || metadata.get("namespace").and_then(|value| value.as_str())
                != Some(&self.opts.common.namespace)
            || metadata.get("revision").and_then(|value| value.as_u64()) != Some(revision)
            || metadata.get("status").and_then(|value| value.as_str()) != Some(state)
            || metadata.get("chart").and_then(|value| value.as_str()) != Some("curie")
            || metadata.get("version").and_then(|value| value.as_str()) != self.current.as_deref()
        {
            return Err(source_metadata_error(
                "source Helm metadata does not match the observed release revision and version",
            ));
        }
        Ok(self.current.clone())
    }

    fn prepare_retained_data(&mut self) -> Result<()> {
        if let Some(fingerprint) = self
            .record
            .as_ref()
            .filter(|record| {
                record.target_version == self.opts.to
                    && matches!(record.status.as_str(), "in_progress" | "failed")
            })
            .and_then(|record| record.retained_agents_fingerprint.clone())
        {
            self.retained_agents_fingerprint = Some(fingerprint);
            return Ok(());
        }
        let probe = self
            .service_probe(
                &self.retained_objects()?,
                "api",
                API_CANARY,
                "upgrade-source-canary",
            )?
            .context("source API is unavailable for retained identity checkpointing")?;
        if probe.get("passed").and_then(|value| value.as_bool()) != Some(true) {
            bail!("source API storage smoke failed before upgrade mutation");
        }
        let fingerprint = probe
            .get("agents_fingerprint")
            .and_then(|value| value.as_str())
            .filter(|value| value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()))
            .context("source API returned no valid retained identity fingerprint")?;
        self.retained_agents_fingerprint = Some(fingerprint.to_owned());
        Ok(())
    }

    fn compute_target_identity(&self) -> Result<String> {
        let mut digest = Sha256::new();
        hash_chart(
            std::path::Path::new(
                self.opts
                    .chart
                    .as_deref()
                    .context("target chart unresolved")?,
            ),
            &mut digest,
        )?;
        digest.update(serde_json::to_vec(
            &self
                .config
                .as_ref()
                .context("configuration not prepared")?
                .values,
        )?);
        Ok(digest
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect())
    }

    fn observed_hooks_succeeded(&self, status: &serde_json::Value) -> bool {
        if !hooks_succeeded(status) {
            return false;
        }
        let config = &self.config.as_ref().expect("prepared configuration").values;
        let enabled =
            |path: &str| config.pointer(path).and_then(|value| value.as_bool()) != Some(false);
        let mut expected = Vec::new();
        if enabled("/api/deploy") && enabled("/api/migrate/enabled") {
            expected.push("schema-migrate");
        }
        if self.current.is_some()
            && enabled("/worker/deploy")
            && enabled("/worker/upgradeDrain/enabled")
        {
            expected.extend(["upgrade-drain", "upgrade-drain-release"]);
        }
        expected.into_iter().all(|component| {
            status["hooks"].as_array().is_some_and(|hooks| {
                hooks.iter().any(|hook| {
                    let Some(manifest) = hook.get("manifest").and_then(|value| value.as_str())
                    else {
                        return false;
                    };
                    let Ok(object) = serde_norway::from_str::<serde_json::Value>(manifest) else {
                        return false;
                    };
                    object
                        .pointer("/metadata/labels/app.kubernetes.io~1component")
                        .and_then(|value| value.as_str())
                        == Some(component)
                        && hook
                            .pointer("/last_run/phase")
                            .and_then(|value| value.as_str())
                            == Some("Succeeded")
                })
            })
        })
    }

    fn helm_wait_timeout(&self) -> u64 {
        let values = &self.config.as_ref().expect("prepared configuration").values;
        let number = |path: &str, default: u64| {
            values
                .pointer(path)
                .and_then(|value| value.as_u64())
                .unwrap_or(default)
        };
        let delivery = number("/worker/deliveryBudgetSeconds", 600);
        let grace = number("/worker/terminationGracePeriodSeconds", 1860)
            .max(delivery.saturating_add(number("/worker/deliveryShutdownReserveSeconds", 60)));
        // Helm's timeout bounds each Kubernetes operation, including hooks:
        // https://helm.sh/docs/helm/helm_upgrade/
        grace
            .max(number("/worker/upgradeDrain/timeoutSeconds", 900))
            .max(600)
            .saturating_add(300)
    }

    fn reconcile_helm_apply(&self) -> Result<bool> {
        if self.inspect_version()?.as_deref() != Some(self.opts.to.as_str()) {
            return Ok(false);
        }
        let (ok, raw, _) = self.run(&OpsCommand::new(
            "helm",
            vec![
                plain("status"),
                plain(&self.opts.common.release),
                plain("-n"),
                plain(&self.opts.common.namespace),
                plain("-o"),
                plain("json"),
            ],
        ))?;
        let status: serde_json::Value = serde_json::from_str(&raw)
            .context("Helm status is malformed during transaction reconciliation")?;
        if !ok
            || status
                .pointer("/info/status")
                .and_then(|value| value.as_str())
                != Some("deployed")
            || !self.observed_hooks_succeeded(&status)
        {
            return Ok(false);
        }
        let values = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(
                super::up::fetch_release_values_with_environment(
                    &self.opts.common,
                    self.owner
                        .as_ref()
                        .map_or_else(Vec::new, |owner| owner.environment()),
                ),
            )
        })?;
        if values.as_ref() != self.config.as_ref().map(|config| &config.values) {
            return Ok(false);
        }
        let source = self
            .service_probe(
                &self.retained_objects()?,
                "api",
                SCHEMA_PROBE,
                "upgrade-schema",
            )?
            .context("target API unavailable during schema reconciliation")?;
        Ok(source.get("current_revision")
            == self
                .schema_decision
                .as_ref()
                .and_then(|decision| decision.get("target_head")))
    }

    fn persist_record(&mut self, record: &UpgradeRecord) -> Result<()> {
        let json = serde_json::to_string(record)?;
        let mut manifest = serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": checkpoint_name(&self.opts.common.release),
                "namespace": self.opts.common.namespace,
                "labels": {
                    "app.kubernetes.io/managed-by": "curie",
                    "curietech.ai/upgrade": "checkpoint",
                }
            },
            "data": { "record": json }
        });
        if let Some(version) = &self.record_version {
            manifest["metadata"]["resourceVersion"] = version.clone().into();
        }
        if let Some(uid) = &self.record_uid {
            manifest["metadata"]["uid"] = uid.clone().into();
        }
        let tmp = tempfile::NamedTempFile::new().context("upgrade checkpoint tempfile")?;
        std::fs::write(tmp.path(), serde_json::to_vec_pretty(&manifest)?)?;
        let cmd = OpsCommand::new(
            "kubectl",
            vec![
                plain(if self.record_version.is_some() {
                    "replace"
                } else {
                    "create"
                }),
                plain("-f"),
                plain(tmp.path().to_string_lossy().into_owned()),
                plain("-n"),
                plain(&self.opts.common.namespace),
                plain("-o"),
                plain("json"),
            ],
        );
        let (ok, out, _) = self.run(&cmd)?;
        if !ok {
            bail!("could not persist the upgrade checkpoint; another coordinator or cluster access may have changed; no subsequent phase was started");
        }
        let object: serde_json::Value =
            serde_json::from_str(&out).context("upgrade checkpoint write response is malformed")?;
        self.record_version = Some(
            object
                .pointer("/metadata/resourceVersion")
                .and_then(|value| value.as_str())
                .filter(|value| !value.is_empty())
                .context("upgrade checkpoint write returned no resource version")?
                .to_owned(),
        );
        let uid = object
            .pointer("/metadata/uid")
            .and_then(|value| value.as_str())
            .filter(|uid| !uid.is_empty())
            .ok_or_else(|| recovery::refusal("upgrade checkpoint write returned no UID"))?;
        if self
            .record_uid
            .as_deref()
            .is_some_and(|previous| previous != uid)
        {
            return Err(recovery::refusal("upgrade checkpoint UID changed"));
        }
        self.record_uid = Some(uid.to_owned());
        Ok(())
    }

    fn helm_upgrade(&self, to: &str) -> Result<()> {
        if self.target_identity.as_deref() != Some(self.compute_target_identity()?.as_str()) {
            bail!("target chart changed after validation; upgrade refused before Helm mutation");
        }
        let chart = self
            .opts
            .chart
            .as_deref()
            .context("target chart was not resolved")?;
        let config = self
            .config
            .as_ref()
            .context("retained configuration was not validated")?;
        let tmp = super::command::SecretValuesFileGuard::write_document(&config.values)?;
        let mut args = vec![
            plain("upgrade"),
            plain(&self.opts.common.release),
            plain(chart),
            plain("-n"),
            plain(&self.opts.common.namespace),
            plain("--wait"),
            plain("--timeout"),
            plain(format!("{}s", self.helm_wait_timeout())),
            plain("-f"),
            plain(tmp.path().to_string_lossy().into_owned()),
        ];
        if let Some(operation) = &self.operation {
            args.extend([
                plain("--labels"),
                plain(format!("{}={}", recovery::LABEL, operation.id)),
            ]);
        }
        if self.current.is_none() {
            args.push(plain("--install"));
            args.push(plain("--create-namespace"));
        }
        let cmd = OpsCommand::new("helm", args);
        let (ok, _, _) = tokio::task::block_in_place(|| {
            let cmd = self
                .owner
                .as_ref()
                .map_or_else(|| cmd.clone(), |owner| owner.bind(&cmd));
            tokio::runtime::Handle::current().block_on(run_upgrade_capture(
                &cmd,
                self.owner.as_ref().and_then(|owner| owner.ownership_fd()),
            ))
        })?;
        if !ok {
            bail!("helm upgrade to {to} failed; inspect the release and resume the same upgrade command");
        }
        Ok(())
    }

    fn retained_objects(&self) -> Result<Vec<serde_json::Value>> {
        let (ok, manifest, _) = self.run(&OpsCommand::new(
            "helm",
            vec![
                plain("get"),
                plain("manifest"),
                plain(&self.opts.common.release),
                plain("-n"),
                plain(&self.opts.common.namespace),
            ],
        ))?;
        if !ok {
            bail!("could not read the retained target manifest for convergence");
        }
        let mut objects = Vec::new();
        for document in serde_norway::Deserializer::from_str(&manifest) {
            let mut value = serde_json::Value::deserialize(document)
                .context("retained target manifest is malformed")?;
            // Kubernetes writes Secret.stringData into base64 data and omits
            // stringData on GET. Compare the persisted bytes, never print them.
            if value.get("kind").and_then(|v| v.as_str()) == Some("Secret") {
                if let Some(strings) = value.as_object_mut().and_then(|o| o.remove("stringData")) {
                    let strings = strings
                        .as_object()
                        .context("retained Secret stringData is not an object")?;
                    for (key, raw) in strings {
                        let raw = raw
                            .as_str()
                            .context("retained Secret stringData contains a non-string")?;
                        value["data"][key] =
                            base64::engine::general_purpose::STANDARD.encode(raw).into();
                    }
                }
            }
            if !value.is_null() {
                objects.push(value);
            }
        }
        if objects.is_empty() {
            bail!("retained target manifest contains no objects");
        }
        Ok(objects)
    }

    fn service_probe(
        &self,
        objects: &[serde_json::Value],
        component: &str,
        script: &str,
        marker: &str,
    ) -> Result<Option<serde_json::Value>> {
        let Some(object) = objects.iter().find(|object| {
            object.get("kind").and_then(|v| v.as_str()) == Some("Deployment")
                && object
                    .pointer("/metadata/labels/app.kubernetes.io~1component")
                    .and_then(|v| v.as_str())
                    == Some(component)
        }) else {
            return Ok(None);
        };
        let name = object
            .pointer("/metadata/name")
            .and_then(|v| v.as_str())
            .context("retained service Deployment has no name")?;
        let (ok, output, _) = self.run(&OpsCommand::new(
            "kubectl",
            vec![
                plain("exec"),
                plain(format!("deployment/{name}")),
                plain("-n"),
                plain(&self.opts.common.namespace),
                plain("-c"),
                plain(component),
                plain("--"),
                plain("python"),
                plain("-c"),
                plain(script),
                plain(marker),
            ],
        ))?;
        if !ok {
            return Ok(Some(serde_json::json!({})));
        }
        Ok(Some(serde_json::from_str(&output).context(
            "upgrade service probe returned malformed JSON",
        )?))
    }

    fn serving_node_images(&self, name: &str) -> Result<serde_json::Value> {
        let failure = || {
            crate::exit::CliError::failure("could not verify serving Node image inventory for upgrade convergence")
            .with_fix("restore get-node access for the serving Pod's node and retry the same upgrade; missing or ambiguous image inventory cannot prove a tagged alias")
        };
        let owner = self.owner.as_ref().ok_or_else(failure)?;
        let command = owner.bind(&super::convergence::node_images_command(name));
        let (ok, raw, _) = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(tokio::time::timeout(
                std::time::Duration::from_secs(10),
                run_capture(&command),
            ))
        })
        .map_err(|_| failure())?
        .map_err(|_| failure())?;
        if !ok {
            return Err(failure().into());
        }
        let node: serde_json::Value = serde_json::from_str(&raw).map_err(|_| failure())?;
        if node.get("kind").and_then(|v| v.as_str()) != Some("Node")
            || node.pointer("/metadata/name").and_then(|v| v.as_str()) != Some(name)
        {
            return Err(failure().into());
        }
        Ok(node)
    }

    fn live_convergence(&self) -> Result<Convergence> {
        let objects = self.retained_objects()?;
        let tmp = super::command::SecretValuesFileGuard::write_document(&serde_json::json!({
            "apiVersion": "v1", "kind": "List", "items": objects,
        }))?;
        let (ok, out, _) = self.run(&OpsCommand::new(
            "kubectl",
            vec![
                plain("get"),
                plain("--ignore-not-found"),
                plain("-f"),
                plain(tmp.path().to_string_lossy().into_owned()),
                plain("-n"),
                plain(&self.opts.common.namespace),
                plain("-o"),
                plain("json"),
            ],
        ))?;
        if !ok {
            bail!("could not read the live objects named by the retained target manifest");
        }
        let live: serde_json::Value =
            serde_json::from_str(&out).context("live owned objects are malformed")?;
        let items = match live.get("items").and_then(|v| v.as_array()) {
            Some(items) => items.clone(),
            None if object_identity(&live).is_some() => vec![live],
            None => bail!("live owned object list has no items"),
        };
        let (ok, raw_pods, _) = self.run(&OpsCommand::new(
            "kubectl",
            vec![
                plain("get"),
                plain("pods"),
                plain("-n"),
                plain(&self.opts.common.namespace),
                plain("-l"),
                plain(format!(
                    "app.kubernetes.io/instance={}",
                    self.opts.common.release
                )),
                plain("-o"),
                plain("json"),
            ],
        ))?;
        if !ok {
            bail!("could not read actual running Pod image identities for convergence");
        }
        let pod_list: serde_json::Value = serde_json::from_str(&raw_pods)
            .context("running Pod image observation is malformed")?;
        let pods = pod_list
            .get("items")
            .and_then(|value| value.as_array())
            .context("running Pod image observation has no items")?;
        let mut nodes = std::collections::BTreeMap::new();
        for name in super::upgrade_images::required_nodes(&objects, pods) {
            nodes.insert(name.clone(), self.serving_node_images(&name)?);
        }
        let mut conv = Convergence::exact_ok();
        let mut workloads = 0;
        for expected in &objects {
            let Some(actual) = items
                .iter()
                .find(|item| object_identity(item) == object_identity(expected))
            else {
                conv.manifest_matches = false;
                conv.images = false;
                conv.replicas = false;
                continue;
            };
            conv.manifest_matches &= contains_manifest(actual, expected);
            let kind = expected.get("kind").and_then(|v| v.as_str()).unwrap_or("");
            if !matches!(kind, "Deployment" | "StatefulSet" | "DaemonSet") {
                continue;
            }
            workloads += 1;
            conv.images &= !workload_images(expected).is_empty()
                && workload_images(actual) == workload_images(expected);
            let generation = actual
                .pointer("/metadata/generation")
                .and_then(|v| v.as_u64());
            let observed = actual
                .pointer("/status/observedGeneration")
                .and_then(|v| v.as_u64());
            conv.generations &= generation.is_some() && generation == observed;
            let (desired, ready, updated, unavailable) = if kind == "DaemonSet" {
                (
                    actual
                        .pointer("/status/desiredNumberScheduled")
                        .and_then(|v| v.as_u64()),
                    actual
                        .pointer("/status/numberReady")
                        .and_then(|v| v.as_u64()),
                    actual
                        .pointer("/status/updatedNumberScheduled")
                        .and_then(|v| v.as_u64()),
                    actual
                        .pointer("/status/numberUnavailable")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0),
                )
            } else {
                (
                    Some(
                        actual
                            .pointer("/spec/replicas")
                            .and_then(|v| v.as_u64())
                            .unwrap_or(1),
                    ),
                    actual
                        .pointer("/status/readyReplicas")
                        .and_then(|v| v.as_u64()),
                    actual
                        .pointer("/status/updatedReplicas")
                        .and_then(|v| v.as_u64()),
                    actual
                        .pointer("/status/unavailableReplicas")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0),
                )
            };
            conv.replicas &= desired.is_some()
                && ready.unwrap_or(0) == desired.unwrap_or(0)
                && updated.unwrap_or(0) == desired.unwrap_or(0);
            conv.unavailable_zero &= unavailable == 0;
            let (images_match, images) =
                super::upgrade_images::observe(expected, pods, desired, &nodes);
            conv.images &= images_match;
            conv.observed_images.extend(images);
        }
        conv.replicas &= workloads > 0;
        let (ok, status, _) = self.run(&OpsCommand::new(
            "helm",
            vec![
                plain("status"),
                plain(&self.opts.common.release),
                plain("-n"),
                plain(&self.opts.common.namespace),
                plain("-o"),
                plain("json"),
            ],
        ))?;
        let status: serde_json::Value = serde_json::from_str(&status).unwrap_or_default();
        conv.hooks_healthy = ok && self.observed_hooks_succeeded(&status);
        conv.queues_drained = self
            .service_probe(&objects, "worker", QUEUE_PROBE, "upgrade-queue-probe")?
            .is_none_or(|probe| {
                probe.get("queues_drained").and_then(|v| v.as_bool()) == Some(true)
            });
        conv.exact = conv.images
            && conv.generations
            && conv.replicas
            && conv.unavailable_zero
            && conv.hooks_healthy
            && conv.queues_drained
            && conv.manifest_matches;
        Ok(conv)
    }

    fn live_canary(&self) -> Result<Canary> {
        // API/storage smoke plus retained agent identity proof. This does not
        // claim a model turn, retained active approvals, or PR-lineage proof.
        let objects = self.retained_objects()?;
        Ok(Canary {
            passed: self
                .service_probe(&objects, "api", API_CANARY, "upgrade-canary")?
                .is_some_and(|probe| {
                    probe.get("passed").and_then(|value| value.as_bool()) == Some(true)
                        && probe
                            .get("agents_fingerprint")
                            .and_then(|value| value.as_str())
                            == self.retained_agents_fingerprint.as_deref()
                }),
        })
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
    fn store_record(&mut self, mut record: UpgradeRecord) -> Result<()> {
        record.operation = self.operation.clone();
        // Intent reaches the locked inode before the first cluster write. A
        // failed initial write can be reconciled only before any Helm intent.
        if let (Some(owner), Some(operation)) = (&self.owner, &self.operation) {
            owner.write_witness(operation)?;
        }
        self.persist_record(&record)?;
        if let Some(operation) = &mut self.operation {
            let uid = self.record_uid.clone().context("checkpoint UID absent")?;
            if operation.checkpoint_uid != uid {
                operation.checkpoint_uid = uid;
                record.operation = Some(operation.clone());
                self.persist_record(&record)?;
            }
        }
        if let (Some(owner), Some(operation)) = (&self.owner, &self.operation) {
            owner.write_witness(operation)?;
        }
        self.record = Some(record);
        Ok(())
    }
    fn retained_agents_fingerprint(&self) -> Option<String> {
        self.retained_agents_fingerprint.clone()
    }
    fn owns_helm_transaction(&self) -> bool {
        true
    }
    fn reconcile_applied(&self) -> Result<bool> {
        self.reconcile_helm_apply()
    }
    fn target_identity(&self) -> Option<String> {
        self.target_identity.clone()
    }
    fn schema_decision(&self) -> Option<serde_json::Value> {
        self.schema_decision.clone()
    }
    fn configuration_plan(&self) -> Vec<String> {
        let mut plan = self
            .config
            .as_ref()
            .map(crate::config_migrate::redacted_upgrade_plan)
            .unwrap_or_default();
        if let Some(decision) = &self.schema_decision {
            plan.push(format!(
                "Schema compatibility: {}{}",
                decision["decision"].as_str().unwrap_or("unknown"),
                if decision["forward_only"] == true {
                    " (explicit forward-only)"
                } else {
                    ""
                }
            ));
        }
        plan
    }
    fn drain_once(&mut self) -> Result<bool> {
        bail!("live drain is owned by the Helm transaction")
    }
    fn apply_target(&mut self, to: &str) -> Result<()> {
        if let Some(revision) = self.recovery_revision {
            // Recheck the complete read-only gate immediately before persisting
            // rollback intent. A second interruption is deliberately unsupported.
            let status = self.recovery_read(&source_status_command(&self.opts.common))?;
            self.validate_pending_recovery(&status)?;
            self.operation
                .as_mut()
                .context("operation absent")?
                .rollback_started = true;
            self.store_record(self.record.clone().context("checkpoint absent")?)?;
            let command = OpsCommand::new(
                "helm",
                vec![
                    plain("rollback"),
                    plain(&self.opts.common.release),
                    plain(revision.to_string()),
                    plain("-n"),
                    plain(&self.opts.common.namespace),
                    plain("--wait"),
                    plain("--timeout"),
                    plain(format!("{}s", self.helm_wait_timeout())),
                ],
            );
            let command = self.owner.as_ref().context("owner absent")?.bind(&command);
            let (ok, _, _) = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(run_upgrade_capture(
                    &command,
                    self.owner.as_ref().and_then(|owner| owner.ownership_fd()),
                ))
            })?;
            if !ok {
                return Err(recovery::refusal("target-forward Helm rollback failed; interrupted rollback is not automatically recoverable"));
            }
            let status = self.recovery_read(&source_status_command(&self.opts.common))?;
            let operation = self.operation.as_ref().context("operation absent")?;
            if status["version"].as_u64() != Some(revision + 1)
                || status
                    .pointer("/info/status")
                    .and_then(|value| value.as_str())
                    != Some("deployed")
                || status["name"] != self.opts.common.release
                || status["namespace"] != self.opts.common.namespace
            {
                return Err(recovery::refusal(
                    "rollback did not return the exact deployed successor revision",
                ));
            }
            let metadata = self.recovery_read(&recovery::metadata_command(
                &self.opts.common,
                &(revision + 1).to_string(),
            ))?;
            let uid = recovery::metadata(
                &metadata,
                &self.opts.common,
                revision + 1,
                "deployed",
                Some(&operation.id),
            )?;
            self.operation
                .as_mut()
                .context("operation absent")?
                .completed_revision_uid = Some(uid);
            self.store_record(self.record.clone().context("checkpoint absent")?)?;
            self.recovery_revision = None;
        } else if let Err(error) = self.helm_upgrade(to) {
            // Binding after a returned failure is deliberately narrow. A process
            // killed before this UID observation leaves recovery unsupported.
            self.bind_original_completion_if_verified()?;
            return Err(error);
        }
        if self.recovery_revision.is_none() {
            let status = self.recovery_read(&source_status_command(&self.opts.common))?;
            if status
                .pointer("/info/status")
                .and_then(|value| value.as_str())
                .is_some_and(|state| state.starts_with("pending-"))
            {
                self.bind_original_completion_if_verified()?;
                return Err(recovery::refusal(
                    "Helm returned before its release completion was durably persisted",
                ));
            }
        }
        self.set_current(Some(to.to_string()));
        Ok(())
    }
    fn observe_convergence(&self) -> Result<Convergence> {
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
}

/// Live `curie cluster upgrade` entry point.
pub async fn upgrade(mut opts: UpgradeOpts) -> Result<ClusterUpgradeOutput> {
    let resolved = crate::artifacts::resolve_chart(
        opts.chart.as_deref(),
        crate::artifacts::Channel::current(),
        &opts.to,
        crate::artifacts::cache_root,
        std::path::Path::new("charts/curie").exists(),
    )?;
    if opts.common.dry_run {
        opts.chart = Some(resolved.planned_target().to_string_lossy().into_owned());
        let mut live = LiveHost {
            owner: None,
            opts: opts.clone(),
            current: None,
            known_good: None,
            record: None,
            record_version: None,
            record_uid: None,
            operation: None,
            recovery_revision: None,
            schema_decision: None,
            target_identity: None,
            retained_agents_fingerprint: None,
            config: None,
        };
        return run_lifecycle_inner(opts, &mut live).await;
    }

    require_on_path("helm")?;
    require_on_path("kubectl")?;
    opts.chart = Some(
        crate::artifacts::ensure_cached(&resolved)
            .await?
            .to_string_lossy()
            .into_owned(),
    );

    if !opts.yes
        && !super::verbs::confirm(&format!(
            "This upgrades release '{}' in namespace '{}' to {}. Continue? [y/N] ",
            opts.common.release, opts.common.namespace, opts.to
        ))?
    {
        bail!("upgrade aborted");
    }

    let owner = super::upgrade_owner::UpgradeOwner::acquire(&opts.common).await?;
    let mut live = LiveHost {
        owner: Some(owner),
        opts: opts.clone(),
        current: None,
        known_good: None,
        record: None,
        record_version: None,
        record_uid: None,
        operation: None,
        recovery_revision: None,
        schema_decision: None,
        target_identity: None,
        retained_agents_fingerprint: None,
        config: None,
    };
    live.current = live.inspect_version()?;
    if live.current.is_none() {
        bail!("fresh installation requires cluster up before transactional upgrade; an absent Helm release does not prove an empty retained database");
    }
    live.record = LiveHost::load_record(&mut live)?;
    live.prepare_operation()?;
    live.prepare_configuration()?;
    live.prepare_schema()?;
    live.bind_operation_target()?;
    live.prepare_retained_data()?;
    let observed_known_good = live.inspect_known_good()?;
    live.known_good = live
        .record
        .as_ref()
        .and_then(|record| record.known_good_version.clone())
        .or(observed_known_good);
    let output = run_lifecycle_inner(opts, &mut live).await?;
    if let ClusterUpgradeOutput::Completed {
        status,
        phase,
        fail_forward,
        ..
    } = &output
    {
        if status == "failed" {
            let error =
                crate::exit::CliError::failure(format!("cluster upgrade failed during {phase}"))
                    .with_fix(
                        fail_forward
                            .as_ref()
                            .map(|recovery| recovery.command.clone())
                            .unwrap_or_else(|| "inspect the upgrade checkpoint and retry".into()),
                    );
            return Err(crate::exit::with_json_payload(
                error.into(),
                crate::ui::CliOutput::to_json(&output),
            ));
        }
    }
    Ok(output)
}

/// Load the upgrade status view for `cluster status`.
pub async fn load_upgrade_status(
    namespace: &str,
    release: &str,
    fallback_known_good: Option<String>,
) -> UpgradeStatusView {
    let unavailable = || UpgradeStatusView {
        phase: None,
        status: "unavailable".into(),
        known_good_version: None,
        target_version: None,
    };
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
            plain("--ignore-not-found"),
        ],
    );
    let (ok, out, _) = match run_capture(&cmd).await {
        Ok(v) => v,
        Err(_) => return unavailable(),
    };
    if !ok {
        return unavailable();
    }
    if out.trim().is_empty() {
        return UpgradeStatusView::idle(fallback_known_good);
    }
    match serde_json::from_str::<UpgradeRecord>(&out) {
        Ok(record) => status_from_record(Some(&record), fallback_known_good),
        Err(_) => unavailable(),
    }
}

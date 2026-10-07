//! Self contained example installation workflows.
//!
//! The SRE bot installer embeds both its observability values and its runtime
//! bundle so a released CLI drives the same one command path as a source
//! checkout. Kubernetes remains the source of capacity truth and every
//! cluster mutation happens only after that read succeeds.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use base64::Engine as _;
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::commands::{self, DeployOpts, DeployTier};
use crate::ui::{CliOutput, DryRunPlan, Ui};

const OBSERVABILITY_NAMESPACE: &str = "observability";
const CURIE_NAMESPACE: &str = "curie";
#[allow(dead_code)] // clap --release default; kept beside the other identity names
const CURIE_RELEASE: &str = "curie";
// #1765's issue text says 1248Mi, but its seven exact appendix requests totalled
// 1312Mi on one node once the enabled 64Mi kube-state-metrics request was
// included. #2059 then raised Tempo's own request from 192Mi to 256Mi to fit the
// measured single-pod envelope in examples/sre-bot/observability/tempo.yaml, so
// the one-node total is now 1376Mi. FIXED_MEMORY_MIB is the part that lands once
// per install regardless of node count: Grafana 128 + Loki 256 + Prometheus 512
// + kube-state-metrics 64 + Tempo 256 = 1216Mi. Alloy 128 and node exporter 32
// are DaemonSets, so PER_READY_NODE_MEMORY_MIB adds their 160Mi on every Ready
// schedulable node -- 1216 + 160 = 1376Mi on a single-node cluster. Move the
// Tempo term here whenever tempo.yaml's resources.requests.memory moves.
const FIXED_MEMORY_MIB: u128 = 1216;
const PER_READY_NODE_MEMORY_MIB: u128 = 160;
const MIB: u128 = 1024 * 1024;
const HELM_TIMEOUT: &str = "10m";
const MANAGED_HELM_RELEASES: [&str; 4] = ["grafana", "loki", "alloy", "prometheus"];
const GRAFANA_ADMIN_SECRET: &str = "grafana-admin";
const GRAFANA_CONNECTOR_SECRET: &str = "curie-grafana-connector";
const GRAFANA_CONNECTOR_KEY: &str = "GRAFANA_SERVICE_ACCOUNT_TOKEN";
const GRAFANA_RELEASE: &str = "grafana";
const READER_IDENTITY: &str = "sre-bot-kubernetes";
const READER_TOKEN_SECRET: &str = "sre-bot-kubernetes-token";
const PLATFORM_PUBLISH_GATE: &str = "mcp__curie__publish_changes";
const UPGRADE_GATE: &str = "mcp__self-upgrade__upgrade_self";
const UPGRADE_TOOL: &str = "self-upgrade/upgrade_self";
// The platform-upgrade verb. Stripped like the others on a read-only install:
// its Job, its identity and its CronJob are all absent by default, so shipping
// the gate without them would validate and never fire.
const PLATFORM_UPGRADE_GATE: &str = "mcp__self-upgrade__upgrade_platform";
const PLATFORM_UPGRADE_TOOL: &str = "self-upgrade/upgrade_platform";
const LATEST_RELEASE_TOOL: &str = "self-upgrade/latest_release";
// Platform publication and the six Kubernetes mutation verbs are always
// present in the shipped bundle. Only the self upgrade gates come and go with
// upgrade_enabled.
const KUBERNETES_MUTATION_TOOLS: &[&str] = &[
    "pods_delete",
    "pods_exec",
    "pods_run",
    "resources_create_or_update",
    "resources_delete",
    "resources_scale",
];
// The platform-upgrade objects this installer renders. Names are fixed rather
// than configurable: the connector is told the CronJob's name through its own
// env, and two places free to disagree is how a tool ends up refusing every call
// with nothing wrong in either file.
// The CONNECTOR's identity -- `create jobs`, so it can press the button. Distinct
// from PLATFORM_UPGRADER_IDENTITY below, which is what the Job itself runs as and
// is the one that can rewrite the release. Two identities on purpose: the thing
// that starts an upgrade and the thing that performs one should not be the same
// credential, or the connector would hold namespace-admin for the life of its pod.
const UPGRADER_IDENTITY: &str = "sre-bot-upgrader";
const UPGRADER_TOKEN_SECRET: &str = "sre-bot-upgrader-token";
const SELF_UPGRADE_KUBECONFIG_SECRET_KEY: &str = "SELF_UPGRADE_KUBECONFIG";
const PLATFORM_UPGRADER_IDENTITY: &str = "curie-platform-upgrader";
const PLATFORM_UPGRADE_CRONJOB_NAME: &str = "platform-upgrade";
const PLATFORM_UPGRADE_CONFIGMAP: &str = "platform-upgrade";
// The project whose releases define "newest" for the platform upgrade. Fixed
// rather than a flag: this installer installs THIS project's example, and an
// upgrade pointed at a different repository is a different thing entirely.
const PLATFORM_UPGRADE_SOURCE_REPO: &str = "curie-eng/curie";
const PLATFORM_UPGRADE_CRONJOB_ENV: &str = "PLATFORM_UPGRADE_CRONJOB";
const SELF_UPGRADE_CRONJOB_ENV: &str = "SELF_UPGRADE_CRONJOB";
// The rule shape this build knows how to render, asserted against the shipped
// manifest exactly as the write Role's is. The manifest is edited far more often
// than this file, so a widened grant must stop the install rather than ship in
// it -- and this is the widest grant the bundle has.
const PLATFORM_RULE_SHAPE: [(&str, &[&str], &[&str]); 6] = [
    (
        "",
        &[
            "secrets",
            "configmaps",
            "services",
            "serviceaccounts",
            "persistentvolumeclaims",
        ],
        &[
            "get", "list", "watch", "create", "update", "patch", "delete",
        ],
    ),
    (
        "apps",
        &["deployments", "statefulsets", "daemonsets", "replicasets"],
        &[
            "get", "list", "watch", "create", "update", "patch", "delete",
        ],
    ),
    (
        "batch",
        &["jobs", "cronjobs"],
        &[
            "get", "list", "watch", "create", "update", "patch", "delete",
        ],
    ),
    (
        "networking.k8s.io",
        &["networkpolicies", "ingresses"],
        &[
            "get", "list", "watch", "create", "update", "patch", "delete",
        ],
    ),
    (
        "rbac.authorization.k8s.io",
        &["roles", "rolebindings"],
        &[
            "get", "list", "watch", "create", "update", "patch", "delete",
        ],
    ),
    ("", &["pods", "events"], &["get", "list", "watch"]),
];
// The one grant the write path may carry. Read from the shipped manifest and
// asserted rather than assumed, so editing that file to widen the verb set stops
// the install instead of shipping in it -- the same posture the connector and
// gate removals below already take.
const READER_TOKEN_TIMEOUT: &str = "2m";
const KUBECONFIG_SECRET_KEY: &str = "K8S_KUBECONFIG";
// The gated write connector's published image. A `sha-<commit>` tag rather than a
// semver one because no tagged release has carried this connector yet: it is
// published by `release.yaml` on every push to a release branch, and a commit tag
// is immutable in the way `latest` is not. Move this to a semver tag once a
// release publishes one.
// The self-upgrade connector's published image. Same reasoning as the write
// connector's above: a `sha-<commit>` tag because no tagged release carries this
// connector yet, and an immutable one because `latest` is not.
const SELF_UPGRADE_IMAGE_REPOSITORY: &str = "ghcr.io/curie-eng/curie-sre-bot-self-upgrade";
const SELF_UPGRADE_IMAGE_TAG: &str = "sha-a391c48b591cf4bf9637ce816964031288151f8e";
const TEMPO_IMAGE_REPOSITORY: &str = "ghcr.io/curie-eng/curie-sre-bot-tempo";
const TEMPO_IMAGE_TAG: &str = "0.8.0";
const TEMPO_TAGGED_IMAGE: &str = "ghcr.io/curie-eng/curie-sre-bot-tempo:0.8.0";
const OCI_INDEX_MEDIA_TYPE: &str = "application/vnd.oci.image.index.v1+json";
const DOCKER_INDEX_MEDIA_TYPE: &str = "application/vnd.docker.distribution.manifest.list.v2+json";

const OBSERVABILITY_FILES: &[(&str, &[u8])] = &[
    (
        "grafana-values.yaml",
        include_bytes!("../../examples/sre-bot/observability/grafana-values.yaml"),
    ),
    (
        "loki-values.yaml",
        include_bytes!("../../examples/sre-bot/observability/loki-values.yaml"),
    ),
    (
        "alloy-values.yaml",
        include_bytes!("../../examples/sre-bot/observability/alloy-values.yaml"),
    ),
    (
        "prometheus-values.yaml",
        include_bytes!("../../examples/sre-bot/observability/prometheus-values.yaml"),
    ),
    (
        "tempo.yaml",
        include_bytes!("../../examples/sre-bot/observability/tempo.yaml"),
    ),
    (
        "curie-values.yaml",
        include_bytes!("../../examples/sre-bot/observability/curie-values.yaml"),
    ),
];

const BUNDLE_FILES: &[(&str, &[u8])] = &[
    (
        ".claude-plugin/plugin.json",
        include_bytes!("../../examples/sre-bot/.claude-plugin/plugin.json"),
    ),
    (
        "connectors.yaml",
        include_bytes!("../../examples/sre-bot/connectors.yaml"),
    ),
    (
        "deploy.yaml",
        include_bytes!("../../examples/sre-bot/deploy.yaml"),
    ),
    (
        "evals/cases.json",
        include_bytes!("../../examples/sre-bot/evals/cases.json"),
    ),
    (
        "manifests/kubernetes-access.yaml",
        include_bytes!("../../examples/sre-bot/manifests/kubernetes-access.yaml"),
    ),
    (
        "manifests/upgrade-role.yaml",
        include_bytes!("../../examples/sre-bot/manifests/upgrade-role.yaml"),
    ),
    (
        "manifests/platform-upgrade-role.yaml",
        include_bytes!("../../examples/sre-bot/manifests/platform-upgrade-role.yaml"),
    ),
    (
        "skills/sre-bot/SKILL.md",
        include_bytes!("../../examples/sre-bot/skills/sre-bot/SKILL.md"),
    ),
];

/// The platform-upgrade Job's template and the script it runs. Not in
/// `BUNDLE_FILES`: they are cluster objects this installer renders and applies,
/// not files the agent bundle carries.
const PLATFORM_UPGRADE_CRONJOB_YAML: &[u8] =
    include_bytes!("../../examples/sre-bot/platform-upgrade/cronjob.yaml");
const PLATFORM_UPGRADE_SCRIPT: &[u8] =
    include_bytes!("../../examples/sre-bot/platform-upgrade/upgrade.sh");

pub struct SreBotInstallOpts {
    pub observability: bool,
    pub observability_only: bool,
    pub dry_run: bool,
    pub slack_channel: Option<String>,
    /// Install the upgrade path: the self-upgrade connector, the platform
    /// upgrade Job and the two identities behind them. Absent, the connector is
    /// stripped exactly as it was before this flag existed, so an install that
    /// does not ask for it is unchanged.
    pub platform_upgrade: bool,
    pub namespace: String,
    pub release: String,
    pub observability_namespace: String,
    /// Repeatable `owner/repo` or `owner/*` entries for `api.githubRepoAllowlist`.
    pub workspace_repo: Vec<String>,
    /// Slack user IDs bound as the explicit approvers of the `sre-approvals`
    /// route. Each raw `--approvers` value may be comma separated. At least one
    /// explicit user is required.
    pub approvers: Vec<String>,
}

struct InstallIdentity {
    namespace: String,
    release: String,
    observability_namespace: String,
}

impl InstallIdentity {
    fn from_opts(opts: &SreBotInstallOpts) -> Self {
        Self {
            namespace: opts.namespace.clone(),
            release: opts.release.clone(),
            observability_namespace: opts.observability_namespace.clone(),
        }
    }
}

pub enum SreBotInstallResult {
    DryRun(DryRunPlan),
    Installed(Box<commands::DeployOutput>),
    ObservabilityInstalled(ObservabilityOnlyOutput),
}

pub struct ObservabilityOnlyOutput {
    pub namespace: String,
}

impl CliOutput for ObservabilityOnlyOutput {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({"observability_namespace": self.namespace, "ready": true})
    }

    fn render(&self, ui: &Ui) {
        ui.payload(&format!(
            "SRE observability stack ready in namespace {}",
            self.namespace
        ));
    }
}

pub struct SreBotRenderOpts {
    pub out: PathBuf,
    pub platform_upgrade: bool,
    pub namespace: String,
    pub release: String,
    pub observability_namespace: String,
}

pub struct SreBotRenderOutput {
    pub path: PathBuf,
}

impl CliOutput for SreBotRenderOutput {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({"bundle_dir": self.path, "rendered": true})
    }

    fn render(&self, ui: &Ui) {
        ui.payload(&format!("SRE bot runtime bundle: {}", self.path.display()));
    }
}

pub struct ObservabilityProvisionOpts {
    pub namespace: String,
    pub release: String,
    pub observability_namespace: String,
    pub chart: Option<String>,
    pub dry_run: bool,
}

pub enum ObservabilityProvisionResult {
    DryRun(DryRunPlan),
    Ready(ObservabilityProvisionOutput),
}

pub struct ObservabilityProvisionOutput {
    pub namespace: String,
    pub release: String,
    pub observability_namespace: String,
}

impl CliOutput for ObservabilityProvisionOutput {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "secret": GRAFANA_CONNECTOR_SECRET,
            "key": GRAFANA_CONNECTOR_KEY,
            "namespace": self.namespace,
            "release": self.release,
            "observability_namespace": self.observability_namespace,
            "ready": true,
        })
    }

    fn render(&self, ui: &Ui) {
        ui.payload(&format!(
            "Secret {GRAFANA_CONNECTOR_SECRET} key {GRAFANA_CONNECTOR_KEY} is ready in namespace {}",
            self.namespace
        ));
    }
}

#[derive(Clone)]
enum CommandArg {
    Plain(String),
    ObservabilityFile(&'static str),
    BundleFile(&'static str),
    CurieChart,
}

impl CommandArg {
    fn display(&self, chart: &Path) -> String {
        match self {
            Self::Plain(value) => value.clone(),
            Self::ObservabilityFile(name) => {
                format!("examples/sre-bot/observability/{name}")
            }
            Self::BundleFile(name) => format!("examples/sre-bot/{name}"),
            Self::CurieChart => chart.display().to_string(),
        }
    }

    fn live(&self, workspace: &EmbeddedWorkspace, chart: &Path) -> String {
        match self {
            Self::Plain(value) => value.clone(),
            Self::ObservabilityFile(name) => workspace
                .observability_dir()
                .join(name)
                .display()
                .to_string(),
            Self::BundleFile(name) => workspace.bundle_dir().join(name).display().to_string(),
            Self::CurieChart => chart.display().to_string(),
        }
    }
}

#[derive(Clone)]
struct InstallCommand {
    program: &'static str,
    args: Vec<CommandArg>,
    helm_target: Option<HelmTarget>,
}

impl InstallCommand {
    /// The live command, with workspace and chart paths resolved, as spawned.
    fn ops_command(&self, workspace: &EmbeddedWorkspace, chart: &Path) -> crate::ops::OpsCommand {
        ops_command(
            self.program,
            self.args.iter().map(|arg| arg.live(workspace, chart)),
        )
    }

    fn display(&self, chart: &Path) -> String {
        std::iter::once(self.program.to_string())
            .chain(self.args.iter().map(|arg| arg.display(chart)))
            .collect::<Vec<_>>()
            .join(" ")
    }
}

#[derive(Clone)]
struct HelmTarget {
    release: String,
    namespace: String,
}

/// A `kubectl` or `helm` invocation with plain arguments, spawned through the
/// ops runners so every child is built in one place (#3568).
fn ops_command<S: Into<String>>(
    program: &str,
    args: impl IntoIterator<Item = S>,
) -> crate::ops::OpsCommand {
    crate::ops::OpsCommand::new(program, args.into_iter().map(crate::ops::plain).collect())
}

fn plain(value: impl Into<String>) -> CommandArg {
    CommandArg::Plain(value.into())
}

fn helm_repo_commands() -> Vec<InstallCommand> {
    vec![
        InstallCommand {
            program: "helm",
            args: vec![
                plain("repo"),
                plain("add"),
                plain("grafana-community"),
                plain("https://grafana-community.github.io/helm-charts"),
                plain("--force-update"),
            ],
            helm_target: None,
        },
        InstallCommand {
            program: "helm",
            args: vec![
                plain("repo"),
                plain("add"),
                plain("grafana"),
                plain("https://grafana.github.io/helm-charts"),
                plain("--force-update"),
            ],
            helm_target: None,
        },
        InstallCommand {
            program: "helm",
            args: vec![
                plain("repo"),
                plain("add"),
                plain("prometheus-community"),
                plain("https://prometheus-community.github.io/helm-charts"),
                plain("--force-update"),
            ],
            helm_target: None,
        },
        InstallCommand {
            program: "helm",
            args: vec![
                plain("repo"),
                plain("update"),
                plain("grafana-community"),
                plain("grafana"),
                plain("prometheus-community"),
            ],
            helm_target: None,
        },
    ]
}

fn upstream_upgrade(
    release: &'static str,
    chart: &'static str,
    version: &'static str,
    values: &'static str,
    observability_namespace: &str,
) -> InstallCommand {
    InstallCommand {
        program: "helm",
        args: vec![
            plain("upgrade"),
            plain("--install"),
            plain(release),
            plain(chart),
            plain("--version"),
            plain(version),
            plain("--namespace"),
            plain(observability_namespace),
            plain("--create-namespace"),
            plain("-f"),
            CommandArg::ObservabilityFile(values),
            plain("--wait"),
            plain("--timeout"),
            plain(HELM_TIMEOUT),
        ],
        helm_target: Some(HelmTarget {
            release: release.to_string(),
            namespace: observability_namespace.to_string(),
        }),
    }
}

fn stack_install_commands(observability_namespace: &str) -> Vec<InstallCommand> {
    let mut commands = helm_repo_commands();
    commands.extend([
        upstream_upgrade(
            "grafana",
            "grafana-community/grafana",
            "12.11.1",
            "grafana-values.yaml",
            observability_namespace,
        ),
        upstream_upgrade(
            "loki",
            "grafana-community/loki",
            "18.10.1",
            "loki-values.yaml",
            observability_namespace,
        ),
        upstream_upgrade(
            "alloy",
            "grafana/alloy",
            "1.11.1",
            "alloy-values.yaml",
            observability_namespace,
        ),
        upstream_upgrade(
            "prometheus",
            "prometheus-community/prometheus",
            "29.27.0",
            "prometheus-values.yaml",
            observability_namespace,
        ),
        InstallCommand {
            program: "kubectl",
            args: vec![
                plain("apply"),
                plain("--namespace"),
                plain(observability_namespace),
                plain("-f"),
                CommandArg::ObservabilityFile("tempo.yaml"),
            ],
            helm_target: None,
        },
        InstallCommand {
            program: "kubectl",
            args: vec![
                plain("rollout"),
                plain("status"),
                plain("statefulset/tempo"),
                plain("--namespace"),
                plain(observability_namespace),
                plain(format!("--timeout={HELM_TIMEOUT}")),
            ],
            helm_target: None,
        },
    ]);
    commands
}

fn curie_integration_command(identity: &InstallIdentity) -> InstallCommand {
    InstallCommand {
        program: "helm",
        args: vec![
            plain("upgrade"),
            plain(&identity.release),
            CommandArg::CurieChart,
            plain("--namespace"),
            plain(&identity.namespace),
            plain("--reuse-values"),
            plain("-f"),
            CommandArg::ObservabilityFile("curie-values.yaml"),
            plain("--wait"),
            plain("--timeout"),
            plain(HELM_TIMEOUT),
        ],
        helm_target: Some(HelmTarget {
            release: identity.release.clone(),
            namespace: identity.namespace.clone(),
        }),
    }
}

fn read_access_command() -> InstallCommand {
    InstallCommand {
        program: "kubectl",
        args: vec![
            plain("apply"),
            plain("-f"),
            CommandArg::BundleFile("manifests/kubernetes-access.yaml"),
        ],
        helm_target: None,
    }
}

pub async fn install_sre_bot(opts: SreBotInstallOpts) -> Result<SreBotInstallResult> {
    if opts.observability_only {
        let stack_commands = stack_install_commands(&opts.observability_namespace);
        if opts.dry_run {
            let mut lines = vec![format!(
                "preserve or create Secret {GRAFANA_ADMIN_SECRET} in namespace {} without exposing its generated password",
                opts.observability_namespace
            )];
            lines.push(
                "select the Alloy log parser from the cluster node runtimes on live installation"
                    .to_string(),
            );
            lines.extend(
                stack_commands
                    .iter()
                    .map(|command| command.display(Path::new("charts/curie"))),
            );
            return Ok(SreBotInstallResult::DryRun(DryRunPlan { lines }));
        }
        preflight_capacity(&opts.observability_namespace).await?;
        let log_runtime = preflight_log_runtime().await?;
        // Render before the first mutation, so a values mismatch refuses
        // with the cluster untouched.
        let workspace =
            EmbeddedWorkspace::create_observability(&opts.observability_namespace, log_runtime)?;
        ensure_grafana_admin_secret(&opts.observability_namespace).await?;
        for command in &stack_commands {
            run_install_command(command, &workspace, Path::new("charts/curie")).await?;
        }
        return Ok(SreBotInstallResult::ObservabilityInstalled(
            ObservabilityOnlyOutput {
                namespace: opts.observability_namespace,
            },
        ));
    }
    if !opts.observability {
        return Err(crate::exit::usage(
            "the SRE bot example installer currently requires --observability",
        ));
    }
    for entry in &opts.workspace_repo {
        crate::api::validate_allowlist_entry(entry)
            .map_err(|err| crate::exit::usage(err.to_string()))?;
    }
    let approvers = parse_approvers(&opts.approvers)?;

    let identity = InstallIdentity::from_opts(&opts);
    let mut model = ModelCredential::resolve()?;

    let resolved_chart = crate::artifacts::resolve_chart(
        None,
        crate::artifacts::Channel::current(),
        crate::artifacts::version(),
        crate::artifacts::cache_root,
        Path::new("charts/curie").is_dir(),
    )?;
    let stack_commands = stack_install_commands(&identity.observability_namespace);
    let integration_command = curie_integration_command(&identity);
    let read_access_command = read_access_command();

    if opts.dry_run {
        let chart = resolved_chart.planned_target();
        let mut lines = vec![format!(
            "resolve {TEMPO_TAGGED_IMAGE} to its immutable OCI image index digest before cluster mutation"
        )];
        lines.push(
            "select the Alloy log parser from the cluster node runtimes on live installation"
                .to_string(),
        );
        lines.push(format!(
            "preserve or create Secret {GRAFANA_ADMIN_SECRET} in namespace {} without exposing its generated password",
            identity.observability_namespace
        ));
        lines.extend(stack_commands.iter().map(|command| command.display(&chart)));
        lines.extend(
            apply_curie_platform(&chart, true, &identity, &opts.workspace_repo, &model).await?,
        );
        lines.push(integration_command.display(&chart));
        lines.push(read_access_command.display(&chart));
        lines.push(format!(
            "kubectl wait --namespace {} --for=jsonpath={{.data.token}} secret/{READER_TOKEN_SECRET} --timeout={READER_TOKEN_TIMEOUT}",
            identity.namespace
        ));
        lines.push(
            "build the Kubernetes connector kubeconfig in memory from the ServiceAccount token"
                .to_string(),
        );
        let resolution = opts
            .slack_channel
            .clone()
            .unwrap_or_else(|| "<the agent's bound Slack channel>".to_string());
        lines.push(format!(
            "bind or rebind approval route {SRE_APPROVALS_ROUTE} on agent {SRE_BOT_AGENT} \
             (creating the agent if absent): resolution {resolution}, approvers users {} (the \
             only users, operator principals minted for them included, who may resolve its gates)",
            approvers.join(",")
        ));
        let mut deploy = format!(
            "curie cluster deploy --plugin-dir embedded:examples/sre-bot --namespace {} --release {}",
            identity.namespace, identity.release
        );
        if let Some(channel) = &opts.slack_channel {
            deploy.push_str(&format!(" --slack-channel {channel}"));
        }
        lines.push(deploy);
        if opts.platform_upgrade {
            // The widest grant this installer can create, and the reason someone
            // runs --dry-run at all. Omitting it was the first version of this
            // flag: the live path applied these four objects and the plan said
            // nothing, so an operator deciding whether to accept a
            // namespace-admin-equivalent identity could not see it in the one
            // output built for that decision.
            lines.push(format!(
                "kubectl apply -f examples/sre-bot/manifests/upgrade-role.yaml -- the CONNECTOR's \
                 identity ({UPGRADER_IDENTITY}): create on jobs, so it can start an upgrade"
            ));
            lines.push(platform_upgrade_role_plan_line(
                embedded_bundle_file("manifests/platform-upgrade-role.yaml")?,
                &identity.namespace,
            )?);
            lines.push(format!(
                "kubectl apply -f <rendered ConfigMap {PLATFORM_UPGRADE_CONFIGMAP}> -- the upgrade \
                 script the Job runs, from examples/sre-bot/platform-upgrade/upgrade.sh"
            ));
            lines.push(format!(
                "kubectl apply -f <rendered CronJob {PLATFORM_UPGRADE_CRONJOB_NAME}, suspend: \
                 true> -- never fires on its own; the gated {PLATFORM_UPGRADE_GATE} creates a Job \
                 from it when a human approves one"
            ));
            lines.push(format!(
                "leave {UPGRADE_GATE} unarmed: no self-upgrade CronJob is applied, so \
                 {SELF_UPGRADE_CRONJOB_ENV} is rendered empty and {UPGRADE_TOOL} is not allowed"
            ));
            lines.push(format!(
                "kubectl wait --namespace {} --for=jsonpath={{.data.token}} \
                 secret/{UPGRADER_TOKEN_SECRET} --timeout={READER_TOKEN_TIMEOUT}",
                identity.namespace
            ));
            lines.push(
                "build the self-upgrade connector kubeconfig in memory from the ServiceAccount token"
                    .to_string(),
            );
        }
        lines.push(
            "render and reconcile the deployed version connectors with the owned Kubernetes kubeconfig Secret override"
                .to_string(),
        );
        return Ok(SreBotInstallResult::DryRun(DryRunPlan { lines }));
    }

    preflight_capacity(&identity.observability_namespace).await?;
    let log_runtime = preflight_log_runtime().await?;
    let tempo_digest = resolve_tempo_index_digest().await?;
    let chart = crate::artifacts::ensure_cached(&resolved_chart).await?;
    crate::ops::require_on_path("helm")?;
    // Resolved before the workspace renders, like the other two: `build:` records
    // a LOCAL image id the cluster tier refuses, so a kept connector with no
    // resolved digest is one that can never start.
    let upgrade_digest = match opts.platform_upgrade {
        true => {
            Some(resolve_index_digest(SELF_UPGRADE_IMAGE_REPOSITORY, SELF_UPGRADE_IMAGE_TAG).await?)
        }
        false => None,
    };
    let workspace = EmbeddedWorkspace::create(
        &tempo_digest,
        &identity,
        upgrade_digest.as_deref(),
        log_runtime,
    )?;
    ensure_grafana_admin_secret(&identity.observability_namespace).await?;
    for command in &stack_commands {
        run_install_command(command, &workspace, &chart).await?;
    }
    // Before the apply, not after: the point is to refuse while the credential
    // still exists.
    if model.declared.is_none() {
        refuse_to_drop_a_recorded_model_credential(&identity).await?;
    } else {
        carry_recorded_runner_egress(&identity, &mut model).await?;
    }
    apply_curie_platform(&chart, false, &identity, &opts.workspace_repo, &model).await?;
    run_install_command(&integration_command, &workspace, &chart).await?;
    run_install_command(&read_access_command, &workspace, &chart).await?;
    let kubeconfig = kubernetes_connector_kubeconfig(&identity.namespace).await?;

    let bundle_dir = workspace.bundle_dir();
    let connection = resolve_embedded_cluster_connection(&identity).await?;
    // Before the deploy: it refuses a bundle whose declared routes are unbound.
    bind_sre_approvals_route(&connection, opts.slack_channel.as_deref(), &approvers).await?;
    let deployed =
        deploy_embedded_sre_bot(&bundle_dir, &connection, opts.slack_channel.as_deref()).await?;
    // ALWAYS after the deploy, never before. `install_sre_bot` orders privileged
    // identities by direction -- never create a NEW one before the deploy -- and
    // both of these are new every time: unlike the write Role there is no
    // allowlist here to NARROW, so the "tighten an existing one early" case that
    // justifies the pre-deploy apply simply does not arise. Creating them any
    // sooner would leave the widest credential this installer can create standing
    // for the whole length of a deploy that may still fail.
    let mut upgrade_kubeconfig = None;
    if opts.platform_upgrade {
        upgrade_kubeconfig =
            Some(apply_upgrade_path(&workspace, &chart, &identity.namespace).await?);
    }
    let mut secret_overrides = BTreeMap::from([(KUBECONFIG_SECRET_KEY.to_string(), kubeconfig)]);
    if let Some(upgrade_kubeconfig) = upgrade_kubeconfig {
        secret_overrides.insert(
            SELF_UPGRADE_KUBECONFIG_SECRET_KEY.to_string(),
            upgrade_kubeconfig,
        );
    }
    crate::connectors::sync_deployed_version(
        &connection.api_url,
        &connection.api_key,
        &identity.namespace,
        &identity.release,
        &deployed,
        &secret_overrides,
    )
    .await?;
    Ok(SreBotInstallResult::Installed(Box::new(deployed)))
}

/// Every tracked file of `examples/dark-factory`, embedded so a released
/// binary can render the factory bundle without a source checkout (#3619).
pub const DARK_FACTORY_BUNDLE_FILES: &[(&str, &[u8])] = &[
    (
        "README.md",
        include_bytes!("../../examples/dark-factory/README.md"),
    ),
    (
        ".gitignore",
        include_bytes!("../../examples/dark-factory/.gitignore"),
    ),
    (
        ".dockerignore",
        include_bytes!("../../examples/dark-factory/.dockerignore"),
    ),
    (
        "connectors.yaml",
        include_bytes!("../../examples/dark-factory/connectors.yaml"),
    ),
    (
        "runner.Dockerfile",
        include_bytes!("../../examples/dark-factory/runner.Dockerfile"),
    ),
    (
        ".claude-plugin/plugin.json",
        include_bytes!("../../examples/dark-factory/.claude-plugin/plugin.json"),
    ),
    (
        "agents/diff-reviewer.md",
        include_bytes!("../../examples/dark-factory/agents/diff-reviewer.md"),
    ),
    (
        "agents/plan-reviewer.md",
        include_bytes!("../../examples/dark-factory/agents/plan-reviewer.md"),
    ),
    (
        "progress/phases.json",
        include_bytes!("../../examples/dark-factory/progress/phases.json"),
    ),
    (
        "hooks/hooks.json",
        include_bytes!("../../examples/dark-factory/hooks/hooks.json"),
    ),
    (
        "hooks/review_gate.py",
        include_bytes!("../../examples/dark-factory/hooks/review_gate.py"),
    ),
    (
        "evals/cases.json",
        include_bytes!("../../examples/dark-factory/evals/cases.json"),
    ),
    (
        "skills/implement-issue/SKILL.md",
        include_bytes!("../../examples/dark-factory/skills/implement-issue/SKILL.md"),
    ),
    (
        "verification/contract.md",
        include_bytes!("../../examples/dark-factory/verification/contract.md"),
    ),
];

/// Files rendered with the executable bit, matching their tracked mode.
const DARK_FACTORY_EXECUTABLE_FILES: &[&str] = &["hooks/review_gate.py"];

pub struct DarkFactoryRenderOpts {
    pub out: PathBuf,
}

/// The dark factory runner layer each release publishes (#3747). Under ADR
/// 0173 the bundle's owner builds its runner layer, and for this example that
/// owner is the project, so `release.yaml` pushes it tagged by version.
pub const DARK_FACTORY_RUNNER_REPOSITORY: &str = "ghcr.io/curie-eng/curie-dark-factory-runner";
/// The platform runner the published layer is built on, at the same version.
const PLATFORM_RUNNER_REPOSITORY: &str = "ghcr.io/curie-eng/curie-runner";

pub struct DarkFactoryRenderOutput {
    pub path: PathBuf,
    /// The published runner layer the lock records, when one exists.
    pub runner_image: Option<String>,
    /// Why no published layer was locked, naming the build that replaces it.
    pub runner_note: Option<String>,
}

impl CliOutput for DarkFactoryRenderOutput {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "bundle_dir": self.path,
            "rendered": true,
            "runner_image": self.runner_image,
            "runner_note": self.runner_note,
        })
    }

    fn render(&self, ui: &Ui) {
        if let Some(image) = &self.runner_image {
            ui.note(&format!("runner layer locked to the published {image}"));
        }
        if let Some(note) = &self.runner_note {
            ui.note(note);
        }
        ui.payload(&format!("Dark factory bundle: {}", self.path.display()));
    }
}

/// Write the embedded dark-factory bundle into `opts.out`. Touches no cluster.
/// Refuses an existing non-empty directory before writing anything.
///
/// A release build then locks the runner layer the release published for its
/// own version, so `cluster deploy` needs no build step (#3747). A source build
/// has no published layer to name, and says so instead of failing at deploy.
pub async fn render_dark_factory(opts: DarkFactoryRenderOpts) -> Result<DarkFactoryRenderOutput> {
    let out = opts.out;
    if out.exists() {
        let non_empty = !out.is_dir()
            || std::fs::read_dir(&out)
                .with_context(|| format!("reading render output {}", out.display()))?
                .next()
                .is_some();
        if non_empty {
            return Err(crate::exit::usage(format!(
                "render output {} already exists and is not empty; pick an empty or new directory",
                out.display()
            )));
        }
    }
    for (name, bytes) in DARK_FACTORY_BUNDLE_FILES {
        let path = out.join(name);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating {}", parent.display()))?;
        }
        std::fs::write(&path, bytes).with_context(|| format!("writing {}", path.display()))?;
        #[cfg(unix)]
        if DARK_FACTORY_EXECUTABLE_FILES.contains(name) {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
                .with_context(|| format!("marking {} executable", path.display()))?;
        }
    }
    let version = crate::artifacts::version();
    let published = match crate::artifacts::Channel::current() {
        crate::artifacts::Channel::Dev => Err(format!(
            "this curie {version} is a source build, so no dark factory runner layer is \
             published for it."
        )),
        crate::artifacts::Channel::Release => {
            let layer = published_index_digest(DARK_FACTORY_RUNNER_REPOSITORY, version).await?;
            let base = published_index_digest(PLATFORM_RUNNER_REPOSITORY, version).await?;
            match (layer, base) {
                (Some(layer), Some(base)) => Ok((layer, base)),
                _ => Err(format!(
                    "no dark factory runner layer is published for curie {version} \
                     ({DARK_FACTORY_RUNNER_REPOSITORY}:{version} or \
                     {PLATFORM_RUNNER_REPOSITORY}:{version} was not found)."
                )),
            }
        }
    };
    match published {
        Ok((layer, base)) => {
            let image = format!("{DARK_FACTORY_RUNNER_REPOSITORY}@{layer}");
            lock_published_runner(
                &out,
                &image,
                &format!("{PLATFORM_RUNNER_REPOSITORY}@{base}"),
            )?;
            Ok(DarkFactoryRenderOutput {
                path: out,
                runner_image: Some(image),
                runner_note: None,
            })
        }
        Err(reason) => Ok(DarkFactoryRenderOutput {
            runner_note: Some(format!(
                "{reason} Build the runner layer before deploying: `curie build --plugin-dir {} \
                 --registry <ref>`.",
                out.display()
            )),
            path: out,
            runner_image: None,
        }),
    }
}

/// Record the published layer in the rendered bundle's lock, exactly as
/// `curie build --registry` would have: the same entry shape, and the
/// `source_digest` of the tree just written so the deploy's freshness check
/// passes until someone edits the layer.
fn lock_published_runner(out: &Path, image: &str, base: &str) -> Result<()> {
    use crate::connector_build as cb;
    let runner = cb::load(out)?
        .runner
        .context("the embedded dark factory connectors.yaml declares no runner layer")?;
    let (context, _) = cb::check_runner_source(out, &runner)?;
    let source_digest = cb::source_digest_of(&context, &runner.build).context("runner")?;
    cb::write_lock(
        out,
        &cb::ConnectorLockFileDecl {
            version: cb::LOCK_VERSION,
            connectors: Default::default(),
            runner: Some(cb::RunnerLockEntryDecl {
                image: image.to_string(),
                base: base.to_string(),
                delivery: cb::Delivery::Registry,
                platforms: runner.build.platforms.clone(),
                source_digest,
            }),
        },
        false,
    )
}

pub async fn render_sre_bot(opts: SreBotRenderOpts) -> Result<SreBotRenderOutput> {
    if opts.out.exists() {
        return Err(crate::exit::usage(format!(
            "render output {} already exists; choose a new directory",
            opts.out.display()
        )));
    }
    let tempo_digest = resolve_tempo_index_digest().await?;
    let upgrade_digest = if opts.platform_upgrade {
        Some(resolve_index_digest(SELF_UPGRADE_IMAGE_REPOSITORY, SELF_UPGRADE_IMAGE_TAG).await?)
    } else {
        None
    };
    let identity = InstallIdentity {
        namespace: opts.namespace,
        release: opts.release,
        observability_namespace: opts.observability_namespace,
    };
    let workspace = EmbeddedWorkspace::create(
        &tempo_digest,
        &identity,
        upgrade_digest.as_deref(),
        LogRuntime::Cri,
    )?;
    if let Some(parent) = opts
        .out
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating SRE bot render parent {}", parent.display()))?;
    }
    std::fs::create_dir(&opts.out)
        .with_context(|| format!("creating SRE bot render output {}", opts.out.display()))?;
    let copy_result = (|| -> Result<()> {
        for (name, _) in BUNDLE_FILES {
            if upgrade_digest.is_none()
                && matches!(
                    *name,
                    "manifests/upgrade-role.yaml" | "manifests/platform-upgrade-role.yaml"
                )
            {
                continue;
            }
            copy_rendered_file(&workspace.bundle_dir(), &opts.out, name)?;
        }
        if upgrade_digest.is_some() {
            for name in [
                "manifests/platform-upgrade-configmap.yaml",
                "manifests/platform-upgrade-cronjob.yaml",
            ] {
                copy_rendered_file(&workspace.bundle_dir(), &opts.out, name)?;
            }
        }
        Ok(())
    })();
    if let Err(err) = copy_result {
        let _ = std::fs::remove_dir_all(&opts.out);
        return Err(err);
    }
    Ok(SreBotRenderOutput { path: opts.out })
}

fn copy_rendered_file(source_root: &Path, out: &Path, name: &str) -> Result<()> {
    let target = out.join(name);
    if let Some(parent) = target.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    std::fs::copy(source_root.join(name), &target)
        .with_context(|| format!("copying rendered SRE bot file {name}"))?;
    Ok(())
}

pub fn observability_provision_plan(
    chart: &str,
    namespace: &str,
    release: &str,
    observability_namespace: &str,
) -> Vec<String> {
    let identity = InstallIdentity {
        namespace: namespace.to_string(),
        release: release.to_string(),
        observability_namespace: observability_namespace.to_string(),
    };
    let chart = Path::new(chart);
    let mut lines = vec![
        "select the Alloy log parser from the cluster node runtimes on live installation"
            .to_string(),
        format!("create namespace {observability_namespace} when it is absent"),
        format!(
            "preserve or create Secret {GRAFANA_ADMIN_SECRET} in namespace {observability_namespace} (without exposing its generated password)"
        ),
    ];
    lines.extend(
        stack_install_commands(observability_namespace)
            .into_iter()
            .map(|command| command.display(chart)),
    );
    lines.push(curie_integration_command(&identity).display(chart));
    lines.push(format!(
        "require Secret {GRAFANA_CONNECTOR_SECRET} in namespace {namespace} to contain key {GRAFANA_CONNECTOR_KEY}"
    ));
    lines
}

pub async fn provision_observability(
    opts: ObservabilityProvisionOpts,
) -> Result<ObservabilityProvisionResult> {
    if opts.dry_run {
        let chart = opts.chart.as_deref().unwrap_or("charts/curie");
        return Ok(ObservabilityProvisionResult::DryRun(DryRunPlan {
            lines: observability_provision_plan(
                chart,
                &opts.namespace,
                &opts.release,
                &opts.observability_namespace,
            ),
        }));
    }

    require_existing_release(&opts.release, &opts.namespace).await?;
    let chart = provision_chart(opts.chart.as_deref()).await?;
    preflight_capacity(&opts.observability_namespace).await?;
    let log_runtime = preflight_log_runtime().await?;
    // Render before the first mutation, so a values mismatch refuses with the
    // cluster untouched.
    let workspace =
        EmbeddedWorkspace::create_observability(&opts.observability_namespace, log_runtime)?;
    ensure_grafana_admin_secret(&opts.observability_namespace).await?;
    let identity = InstallIdentity {
        namespace: opts.namespace.clone(),
        release: opts.release.clone(),
        observability_namespace: opts.observability_namespace.clone(),
    };
    for command in stack_install_commands(&identity.observability_namespace) {
        run_install_command(&command, &workspace, &chart).await?;
    }
    let integration = curie_integration_command(&identity);
    run_install_command(&integration, &workspace, &chart).await?;
    require_grafana_connector_token(&identity.namespace).await?;
    Ok(ObservabilityProvisionResult::Ready(
        ObservabilityProvisionOutput {
            namespace: opts.namespace,
            release: opts.release,
            observability_namespace: opts.observability_namespace,
        },
    ))
}

async fn require_existing_release(release: &str, namespace: &str) -> Result<()> {
    crate::ops::require_on_path("helm")?;
    let (ok, _stdout, stderr) = crate::ops::run_capture(&ops_command(
        "helm",
        ["status", release, "--namespace", namespace],
    ))
    .await
    .with_context(|| format!("failed to run `helm status {release} --namespace {namespace}`"))?;
    if ok {
        return Ok(());
    }
    if crate::ops::failure_reason(&stderr) == "Error: release: not found" {
        return Err(crate::exit::usage(format!(
            "release {release} in namespace {namespace} does not exist; run `curie cluster up` before provisioning observability"
        )));
    }
    let reason = crate::ops::failure_reason(&stderr);
    Err(crate::exit::CliError::failure(format!(
        "could not read Helm status for release {release} in namespace {namespace}: {reason}"
    ))
    .into())
}

async fn provision_chart(chart: Option<&str>) -> Result<PathBuf> {
    if let Some(chart) = chart {
        let path = PathBuf::from(chart);
        if path.is_dir() {
            return Ok(path);
        }
        return Err(crate::exit::usage(format!(
            "chart directory {} does not exist",
            path.display()
        )));
    }
    let resolved = crate::artifacts::resolve_chart(
        None,
        crate::artifacts::Channel::current(),
        crate::artifacts::version(),
        crate::artifacts::cache_root,
        Path::new("charts/curie").is_dir(),
    )?;
    let path = crate::artifacts::ensure_cached(&resolved).await?;
    if path.exists() {
        return Ok(path);
    }
    Err(crate::exit::usage(format!(
        "chart {} does not exist",
        path.display()
    )))
}

async fn require_grafana_connector_token(namespace: &str) -> Result<()> {
    let (ok, stdout, _stderr) = crate::ops::run_capture(&ops_command(
        "kubectl",
        [
            "get",
            "secret",
            GRAFANA_CONNECTOR_SECRET,
            "--namespace",
            namespace,
            "-o",
            "json",
        ],
    ))
    .await
    .context("reading the Grafana connector Secret")?;
    let present = ok
        && serde_json::from_str::<serde_json::Value>(&stdout)
            .is_ok_and(|secret| grafana_connector_token_present(&secret));
    drop(stdout);
    if present {
        return Ok(());
    }
    Err(crate::exit::CliError::failure(format!(
        "key {GRAFANA_CONNECTOR_KEY} is absent from Secret {GRAFANA_CONNECTOR_SECRET} in namespace {namespace}"
    ))
    .into())
}

fn grafana_connector_token_present(secret: &serde_json::Value) -> bool {
    let Some(encoded) = secret
        .pointer("/data/GRAFANA_SERVICE_ACCOUNT_TOKEN")
        .and_then(serde_json::Value::as_str)
    else {
        return false;
    };
    let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(encoded) else {
        return false;
    };
    let Ok(token) = String::from_utf8(decoded) else {
        return false;
    };
    !token.trim().is_empty()
}

/// Every object `--platform-upgrade` applies, in apply order. One list so the
/// armed gate set can be checked against it: #2288 shipped `upgrade_self` armed
/// while this path never applied the CronJob it starts.
const UPGRADE_PATH_FILES: [&str; 4] = [
    "manifests/upgrade-role.yaml",
    "manifests/platform-upgrade-role.yaml",
    "manifests/platform-upgrade-configmap.yaml",
    "manifests/platform-upgrade-cronjob.yaml",
];

/// Apply the upgrade path's objects and mint the connector's kubeconfig.
///
/// Order within this function matters: the identities come first, then the
/// script, then the CronJob that references both. Applying the CronJob first
/// would leave a template naming a ServiceAccount that does not exist, which
/// Kubernetes accepts and which fails only when a Job is finally created from it
/// -- after a human has approved an upgrade.
async fn apply_upgrade_path(
    workspace: &EmbeddedWorkspace,
    chart: &Path,
    namespace: &str,
) -> Result<String> {
    for file in UPGRADE_PATH_FILES {
        let command = InstallCommand {
            program: "kubectl",
            args: vec![plain("apply"), plain("-f"), CommandArg::BundleFile(file)],
            helm_target: None,
        };
        run_install_command(&command, workspace, chart).await?;
    }
    connector_kubeconfig(UPGRADER_IDENTITY, UPGRADER_TOKEN_SECRET, namespace).await
}

/// Stop rather than silently reset a model credential this installer will drop.
///
/// `apply_curie_platform` goes through the declarative path with
/// `Credentials::default()`, and that path's contract is deliberate: a
/// configuration naming no model credential really does clear one, and `curie
/// diff` reports it as a reset because that is what happens
/// (`without_a_declared_model_credential_those_keys_are_resets`). The contract is
/// right; using it from an installer that declares nothing is not.
///
/// Run against a live install, that combination removed
/// `agentSandbox.runner.credentials` and left the release on the chart's
/// `fakeModel` default. Nothing looked wrong: helm reported success, every pod
/// stayed healthy, and the bot kept answering -- in three milliseconds, from the
/// fake model, "all done" (#2129).
///
/// So the installer asks first when it declares no credential: with
/// `CURIE_CREDENTIALS` exported it declares one (#2920) and nothing is dropped.
/// Without it, refusing prevents an invisible credential loss.
/// Does this release's recorded values carry a model credential?
///
/// Split out so the decision is testable without a cluster: the read is the part
/// that needs one, and the read is not what was wrong.
fn records_a_model_credential(existing: &serde_json::Value) -> bool {
    existing
        .pointer("/agentSandbox/runner/credentials")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|recorded| !recorded.trim().is_empty())
}

async fn refuse_to_drop_a_recorded_model_credential(identity: &InstallIdentity) -> Result<()> {
    let opts = crate::ops::CommonOpts {
        namespace: identity.namespace.clone(),
        release: identity.release.clone(),
        dry_run: false,
    };
    let Some(existing) = crate::ops::fetch_release_values(&opts).await? else {
        // No release yet: a fresh install has nothing to drop.
        return Ok(());
    };
    if !records_a_model_credential(&existing) {
        return Ok(());
    }
    Err(crate::exit::usage(format!(
        "release {} in namespace {} records a model credential, and this installer \
         would clear it.\n\n\
         It applies the declarative path with no credential declared, and that path \
         removes what it does not name -- so re-running here would leave the install \
         on the chart's fakeModel default, healthy in every way except that the agent \
         is no longer a model. That state is hard to see: pods stay Ready and turns \
         still answer.\n\n\
         Export CURIE_CREDENTIALS before re-running and the installer declares it, \
         so nothing is cleared. Otherwise use the normal Curie cluster lifecycle for \
         a release that already records a model credential.",
        identity.release, identity.namespace,
    )))
}

/// The model credential this installer declares, read the way `curie cluster up`
/// reads it.
///
/// The platform step used to declare no credential at all, so a
/// `CURIE_CREDENTIALS` exported before the install was ignored and the release
/// came up on the fake model (#2920). Declaring it by NAME keeps the value out of
/// the plan; the provider egress is inferred from the credential prefix, as
/// `cluster up` infers it, so the real model is reachable rather than sealed.
struct ModelCredential {
    declared: Option<crate::installation::Credentials>,
    egress: Vec<crate::installation::Egress>,
    /// Explicit runner egress values that stand in for `egress` on a rerun
    /// over a release that already records its own (see
    /// [`carry_recorded_runner_egress`]).
    egress_sets: BTreeMap<String, String>,
}

const MODEL_CREDENTIAL_ENV: &str = "CURIE_CREDENTIALS";

impl ModelCredential {
    fn resolve() -> Result<Self> {
        Ok(Self::from_value(
            crate::installation::resolve_credential(MODEL_CREDENTIAL_ENV)?.as_deref(),
        ))
    }

    /// Split out so the decision is testable without the environment.
    fn from_value(credential: Option<&str>) -> Self {
        let Some(credential) = credential.filter(|value| !value.trim().is_empty()) else {
            return Self {
                declared: None,
                egress: Vec::new(),
                egress_sets: BTreeMap::new(),
            };
        };
        Self {
            declared: Some(crate::installation::Credentials {
                model: Some(MODEL_CREDENTIAL_ENV.to_string()),
                ..Default::default()
            }),
            egress: crate::ops::provider_from_credential_prefix(credential)
                .map(|provider| crate::installation::Egress {
                    host: provider.to_string(),
                })
                .into_iter()
                .collect(),
            egress_sets: BTreeMap::new(),
        }
    }
}

const RUNNER_EGRESS_KEY: &str = "security.networkPolicy.allowedEgress";

/// The release's recorded runner egress, flattened to `--set` keys.
fn recorded_runner_egress(existing: &serde_json::Value) -> BTreeMap<String, String> {
    let mut flat = BTreeMap::new();
    crate::installation::flatten_values(existing, "", &mut flat);
    flat.into_iter()
        .filter(|(key, _)| key.starts_with(&format!("{RUNNER_EGRESS_KEY}[")))
        .collect()
}

/// The recorded entries kept verbatim, then one TCP 443 entry per provider
/// CIDR, the shape `cluster up` appends for an inferred provider. No dedupe
/// against recorded CIDRs: a recorded entry for the same address may allow a
/// different port, and a duplicate rule costs nothing.
fn carried_runner_egress_sets(
    recorded: BTreeMap<String, String>,
    provider_cidrs: &[String],
) -> BTreeMap<String, String> {
    let next_index = recorded
        .keys()
        .filter_map(|key| {
            key.strip_prefix(RUNNER_EGRESS_KEY)?
                .strip_prefix('[')?
                .split_once(']')?
                .0
                .parse::<usize>()
                .ok()
        })
        .max()
        .map_or(0, |index| index + 1);
    let mut sets = recorded;
    for (offset, cidr) in provider_cidrs.iter().enumerate() {
        let entry = format!("{RUNNER_EGRESS_KEY}[{}]", next_index + offset);
        sets.insert(format!("{entry}.cidr"), cidr.clone());
        sets.insert(format!("{entry}.ports[0].protocol"), "TCP".to_string());
        sets.insert(format!("{entry}.ports[0].port"), "443".to_string());
    }
    sets
}

/// A declared egress host REPLACES the release's recorded runner egress on the
/// declarative path, so on a rerun the provider route would drop entries an
/// operator recorded with `cluster up`, and simply not declaring it would leave
/// a newly selected provider unreachable. When the release records egress,
/// carry it forward explicitly and append the provider's resolved routes.
async fn carry_recorded_runner_egress(
    identity: &InstallIdentity,
    model: &mut ModelCredential,
) -> Result<()> {
    if model.egress.is_empty() {
        return Ok(());
    }
    let opts = crate::ops::CommonOpts {
        namespace: identity.namespace.clone(),
        release: identity.release.clone(),
        dry_run: false,
    };
    let Some(existing) = crate::ops::fetch_release_values(&opts).await? else {
        return Ok(());
    };
    let recorded = recorded_runner_egress(&existing);
    if recorded.is_empty() {
        return Ok(());
    }
    let providers: Vec<String> = model.egress.iter().map(|e| e.host.clone()).collect();
    let provider_cidrs =
        crate::ops::resolve_provider_egress_cidrs_for_current_environment(&providers)
            .context("resolving the model provider's egress hosts")?;
    model.egress_sets = carried_runner_egress_sets(recorded, &provider_cidrs);
    model.egress.clear();
    Ok(())
}

impl ModelCredential {
    /// `egress_sets` as typed `--set` arguments, so a port stays an integer.
    fn typed_egress_sets(&self) -> Vec<String> {
        self.egress_sets
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect()
    }
}

fn github_repo_allowlist_sets(repos: &[String]) -> BTreeMap<String, String> {
    repos
        .iter()
        .enumerate()
        .map(|(index, repo)| (format!("api.githubRepoAllowlist[{index}]"), repo.clone()))
        .collect()
}

fn platform_installation(
    identity: &InstallIdentity,
    workspace_repo: &[String],
    model: &ModelCredential,
) -> crate::installation::Installation {
    crate::installation::Installation {
        version: crate::installation::SUPPORTED_VERSION,
        install: crate::installation::Install {
            namespace: identity.namespace.clone(),
            release: identity.release.clone(),
            context: None,
        },
        platform: crate::installation::Platform {
            egress: model.egress.clone(),
            ..Default::default()
        },
        credentials: model.declared.clone().unwrap_or_default(),
        comms: crate::installation::Comms::default(),
        set: github_repo_allowlist_sets(workspace_repo),
    }
}

async fn apply_curie_platform(
    chart: &Path,
    dry_run: bool,
    identity: &InstallIdentity,
    workspace_repo: &[String],
    model: &ModelCredential,
) -> Result<Vec<String>> {
    let installation = platform_installation(identity, workspace_repo, model);
    let local = crate::installation::plan_installation(installation, dry_run)?
        .with_typed_sets(model.typed_egress_sets());
    match crate::installation::apply(crate::installation::ApplyOpts {
        local,
        chart: chart.display().to_string(),
        allow_stateful_removal: false,
        migrate_store: false,
    })
    .await?
    {
        crate::installation::ApplyOutput::DryRun(plan) => Ok(plan.lines),
        crate::installation::ApplyOutput::Applied { .. } => Ok(Vec::new()),
        crate::installation::ApplyOutput::WroteStarter { .. } => Ok(Vec::new()),
    }
}

#[derive(Deserialize)]
struct RegistryToken {
    #[serde(alias = "access_token")]
    token: String,
}

async fn resolve_tempo_index_digest() -> Result<String> {
    resolve_index_digest(TEMPO_IMAGE_REPOSITORY, TEMPO_IMAGE_TAG).await
}

/// The immutable index digest behind one `ghcr.io/<org>/<name>:<tag>`.
///
/// Generalised from the tempo-only resolver because the gated write connector
/// needs exactly the same treatment. The bundle declares it `build:`, which
/// records a LOCAL image id, and a cluster cannot pull an image that exists only
/// in one machine's Docker daemon -- so keeping the connector without resolving a
/// published digest produces a bundle whose write path can never come up.
async fn resolve_index_digest(repository: &str, tag: &str) -> Result<String> {
    match published_index_digest(repository, tag).await? {
        Some(digest) => Ok(digest),
        None => bail!(
            "could not resolve {repository}:{tag}: OCI index request returned HTTP 404 Not Found"
        ),
    }
}

/// [`resolve_index_digest`], with "this tag was never published" as `None`
/// rather than an error. Any other failure, an unreachable registry included,
/// stays an error: absence is a fact about the release, an outage is not.
async fn published_index_digest(repository: &str, tag: &str) -> Result<Option<String>> {
    let path = repository
        .strip_prefix("ghcr.io/")
        .with_context(|| format!("{repository} is not a ghcr.io repository"))?
        .to_string();
    let tagged = format!("{repository}:{tag}");
    let registry = std::env::var("CURIE_TEST_SRE_BOT_REGISTRY_ENDPOINT")
        .unwrap_or_else(|_| "https://ghcr.io".to_string());
    let registry = registry.trim_end_matches('/');
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .context("building the anonymous GHCR client")?;
    let token_response = client
        .get(format!("{registry}/token"))
        .query(&[
            ("service", "ghcr.io"),
            ("scope", format!("repository:{path}:pull").as_str()),
        ])
        .send()
        .await
        .with_context(|| format!("resolving {tagged} before cluster mutation"))?;
    if !token_response.status().is_success() {
        bail!(
            "could not resolve {tagged} before cluster mutation: anonymous GHCR token request returned HTTP {}",
            token_response.status()
        );
    }
    let token: RegistryToken = token_response
        .json()
        .await
        .with_context(|| format!("reading the anonymous token for {tagged}"))?;
    if token.token.is_empty() {
        bail!("could not resolve {tagged}: GHCR returned an empty token");
    }

    let manifest_response = client
        .get(format!("{registry}/v2/{path}/manifests/{tag}"))
        .bearer_auth(&token.token)
        .header(
            reqwest::header::ACCEPT,
            format!("{OCI_INDEX_MEDIA_TYPE}, {DOCKER_INDEX_MEDIA_TYPE}"),
        )
        .send()
        .await
        .with_context(|| format!("fetching the OCI image index for {tagged}"))?;
    if manifest_response.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    if !manifest_response.status().is_success() {
        bail!(
            "could not resolve {tagged}: OCI index request returned HTTP {}",
            manifest_response.status()
        );
    }
    let body = manifest_response
        .bytes()
        .await
        .with_context(|| format!("reading the OCI image index for {tagged}"))?;
    let manifest: serde_json::Value = serde_json::from_slice(&body)
        .with_context(|| format!("{tagged} returned a malformed OCI index"))?;
    let media_type = manifest
        .get("mediaType")
        .and_then(serde_json::Value::as_str);
    let is_index_media_type = matches!(
        media_type,
        Some(OCI_INDEX_MEDIA_TYPE) | Some(DOCKER_INDEX_MEDIA_TYPE)
    );
    let is_index = is_index_media_type
        && manifest
            .get("schemaVersion")
            .and_then(serde_json::Value::as_u64)
            == Some(2)
        && manifest
            .get("manifests")
            .is_some_and(serde_json::Value::is_array);
    if !is_index {
        bail!(
            "could not resolve {tagged}: expected an OCI image index, got {}",
            media_type.unwrap_or("no mediaType")
        );
    }
    let digest_hex = Sha256::digest(&body)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let digest = format!("sha256:{digest_hex}");
    validate_sha256_digest(&digest)?;
    Ok(Some(digest))
}

fn validate_sha256_digest(digest: &str) -> Result<()> {
    let Some(hex) = digest.strip_prefix("sha256:") else {
        bail!("resolved Tempo image digest must start with sha256:");
    };
    if hex.len() != 64 || !hex.chars().all(|character| character.is_ascii_hexdigit()) {
        bail!("resolved Tempo image digest must contain 64 lowercase hexadecimal characters");
    }
    if hex != hex.to_ascii_lowercase() {
        bail!("resolved Tempo image digest must contain 64 lowercase hexadecimal characters");
    }
    Ok(())
}

async fn ensure_grafana_admin_secret(observability_namespace: &str) -> Result<()> {
    ensure_observability_namespace(observability_namespace).await?;
    let (ok, _stdout, stderr) = crate::ops::run_capture(&ops_command(
        "kubectl",
        [
            "get",
            "secret",
            GRAFANA_ADMIN_SECRET,
            "--namespace",
            observability_namespace,
            "-o",
            "json",
        ],
    ))
    .await
    .context("inspecting the Grafana admin Secret")?;
    if ok {
        return Ok(());
    }
    let lower = stderr.to_ascii_lowercase();
    if !lower.contains("notfound") && !lower.contains("not found") {
        bail!(
            "could not inspect Secret {GRAFANA_ADMIN_SECRET} in namespace {observability_namespace} with `kubectl get secret {GRAFANA_ADMIN_SECRET} -n {observability_namespace}`: {}",
            stderr.trim()
        );
    }

    if grafana_release_exists(observability_namespace).await? {
        return migrate_grafana_admin_secret(observability_namespace).await;
    }

    let password = random_hex(32)?;
    let manifest = serde_json::to_vec(&serde_json::json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": GRAFANA_ADMIN_SECRET,
            "namespace": observability_namespace,
        },
        "type": "Opaque",
        "stringData": {
            "admin-user": "admin",
            "admin-password": password,
        },
    }))?;
    apply_private_manifest(&manifest, "Grafana admin Secret", observability_namespace).await
}

async fn grafana_release_exists(observability_namespace: &str) -> Result<bool> {
    let (ok, _stdout, stderr) = crate::ops::run_capture(&ops_command(
        "helm",
        [
            "status",
            GRAFANA_RELEASE,
            "--namespace",
            observability_namespace,
            "-o",
            "json",
        ],
    ))
    .await
    .context("inspecting the existing Grafana release")?;
    if ok {
        return Ok(true);
    }
    if stderr.trim() == "Error: release: not found" {
        return Ok(false);
    }
    bail!(
        "could not determine whether Grafana is already installed; run `helm status {GRAFANA_RELEASE} -n {observability_namespace}` and retry"
    )
}

#[derive(Clone)]
struct SecretKeyReference {
    name: String,
    key: String,
}

async fn migrate_grafana_admin_secret(observability_namespace: &str) -> Result<()> {
    let (ok, stdout, _stderr) = crate::ops::run_capture(&ops_command(
        "kubectl",
        [
            "get",
            "deployment,statefulset",
            "--namespace",
            observability_namespace,
            "-l",
            "app.kubernetes.io/instance=grafana",
            "-o",
            "json",
        ],
    ))
    .await
    .context("discovering the existing Grafana admin credential")?;
    if !ok {
        bail!("could not read the existing Grafana admin credential");
    }
    let workloads: serde_json::Value = serde_json::from_str(&stdout)
        .context("the existing Grafana workload response was malformed")?;
    let user = find_grafana_secret_reference(&workloads, "GF_SECURITY_ADMIN_USER")?;
    let password = find_grafana_secret_reference(&workloads, "GF_SECURITY_ADMIN_PASSWORD")?;

    let mut source_secrets = BTreeMap::new();
    for source_name in [&user.name, &password.name] {
        if source_secrets.contains_key(source_name) {
            continue;
        }
        let (ok, stdout, _stderr) = crate::ops::run_capture(&ops_command(
            "kubectl",
            [
                "get",
                "secret",
                source_name.as_str(),
                "--namespace",
                observability_namespace,
                "-o",
                "json",
            ],
        ))
        .await
        .context("reading the existing Grafana admin credential")?;
        if !ok {
            bail!("could not read the existing Grafana admin credential");
        }
        let secret: serde_json::Value = serde_json::from_str(&stdout)
            .context("the existing Grafana admin Secret response was malformed")?;
        source_secrets.insert(source_name.clone(), secret);
    }

    let user_data = secret_data_value(&source_secrets, &user)?;
    let password_data = secret_data_value(&source_secrets, &password)?;
    let manifest = serde_json::to_vec(&serde_json::json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": GRAFANA_ADMIN_SECRET,
            "namespace": observability_namespace,
        },
        "type": "Opaque",
        "data": {
            "admin-user": user_data,
            "admin-password": password_data,
        },
    }))?;
    apply_private_manifest(&manifest, "Grafana admin Secret", observability_namespace).await
}

fn find_grafana_secret_reference(
    workloads: &serde_json::Value,
    env_name: &str,
) -> Result<SecretKeyReference> {
    let mut references = Vec::new();
    for item in workloads
        .get("items")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
    {
        for container in item
            .pointer("/spec/template/spec/containers")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
        {
            for env in container
                .get("env")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
            {
                if env.get("name").and_then(serde_json::Value::as_str) != Some(env_name) {
                    continue;
                }
                let reference = env.pointer("/valueFrom/secretKeyRef").ok_or_else(|| {
                    anyhow!("could not read the existing Grafana admin credential")
                })?;
                let name = reference
                    .get("name")
                    .and_then(serde_json::Value::as_str)
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| {
                        anyhow!("could not read the existing Grafana admin credential")
                    })?;
                let key = reference
                    .get("key")
                    .and_then(serde_json::Value::as_str)
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| {
                        anyhow!("could not read the existing Grafana admin credential")
                    })?;
                references.push(SecretKeyReference {
                    name: name.to_string(),
                    key: key.to_string(),
                });
            }
        }
    }
    if references.len() != 1 {
        bail!("could not read the existing Grafana admin credential");
    }
    Ok(references.remove(0))
}

fn secret_data_value(
    secrets: &BTreeMap<String, serde_json::Value>,
    reference: &SecretKeyReference,
) -> Result<String> {
    let encoded = secrets
        .get(&reference.name)
        .and_then(|secret| secret.get("data"))
        .and_then(|data| data.get(&reference.key))
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("could not read the existing Grafana admin credential"))?;
    base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .ok()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("could not read the existing Grafana admin credential"))?;
    Ok(encoded.to_string())
}

async fn ensure_observability_namespace(observability_namespace: &str) -> Result<()> {
    let (ok, _stdout, stderr) = crate::ops::run_capture(&ops_command(
        "kubectl",
        ["get", "namespace", observability_namespace, "-o", "json"],
    ))
    .await
    .context("inspecting the observability namespace")?;
    if ok {
        return Ok(());
    }
    let lower = stderr.to_ascii_lowercase();
    if !lower.contains("notfound") && !lower.contains("not found") {
        bail!(
            "could not inspect namespace {observability_namespace} with `kubectl get namespace {observability_namespace}`: {}",
            stderr.trim()
        );
    }
    let (ok, _stdout, _stderr) = crate::ops::run_capture(&ops_command(
        "kubectl",
        ["create", "namespace", observability_namespace],
    ))
    .await
    .context("creating the observability namespace")?;
    if !ok {
        bail!(
            "could not create namespace {observability_namespace}; run `kubectl create namespace {observability_namespace}` and retry"
        );
    }
    Ok(())
}

async fn apply_private_manifest(
    manifest: &[u8],
    description: &str,
    observability_namespace: &str,
) -> Result<()> {
    let (ok, _stdout, _stderr) =
        crate::ops::run_capture_with_stdin(&ops_command("kubectl", ["apply", "-f", "-"]), manifest)
            .await
            .with_context(|| format!("running kubectl to apply {description}"))?;
    if !ok {
        bail!(
            "could not apply {description} {GRAFANA_ADMIN_SECRET} in namespace {observability_namespace}; inspect access with `kubectl auth can-i create secret -n {observability_namespace}`"
        );
    }
    Ok(())
}

fn random_hex(bytes: usize) -> Result<String> {
    let mut value = vec![0u8; bytes];
    getrandom::fill(&mut value)
        .map_err(|error| anyhow!("OS random number generator unavailable: {error}"))?;
    Ok(value.iter().map(|byte| format!("{byte:02x}")).collect())
}

async fn run_install_command(
    command: &InstallCommand,
    workspace: &EmbeddedWorkspace,
    chart: &Path,
) -> Result<()> {
    let cmd = command.ops_command(workspace, chart);
    crate::ui::ui().plumbing(&format!("+ {}", cmd.display()));
    let (ok, _stdout, stderr) = crate::ops::run_capture(&cmd).await?;
    if ok {
        return Ok(());
    }

    let stderr = stderr.trim().to_string();
    let args = cmd.argv();
    if let Some(target) = &command.helm_target {
        if is_helm_timeout(&stderr) {
            let diagnostic = pending_pvc_diagnostic(&target.namespace).await;
            let recovery = verified_helm_pending_upgrade_recovery(target).await;
            let mut message = format!(
                "Helm timed out waiting for release {} in namespace {}: {}. {}",
                target.release,
                target.namespace,
                if stderr.is_empty() {
                    "command timed out"
                } else {
                    &stderr
                },
                diagnostic,
            );
            if let Some(recovery) = recovery {
                message.push_str(&format!(
                    " Helm reports pending-upgrade; after checking why it is pending, recover the release with: {recovery}"
                ));
                return Err(crate::exit::CliError::failure(message)
                    .with_fix(recovery)
                    .into());
            }
            return Err(crate::exit::CliError::failure(message).into());
        }
    }
    if command.program == "kubectl"
        && is_helm_timeout(&stderr)
        && args.iter().any(|arg| arg == "statefulset/tempo")
        && args.iter().any(|arg| arg == "rollout")
    {
        let namespace = command
            .helm_target
            .as_ref()
            .map(|target| target.namespace.as_str())
            .or_else(|| {
                args.iter()
                    .position(|arg| arg == "--namespace")
                    .and_then(|index| args.get(index + 1))
                    .map(String::as_str)
            })
            .unwrap_or(OBSERVABILITY_NAMESPACE);
        let diagnostic = pending_pvc_diagnostic(namespace).await;
        bail!(
            "`{}` failed: {}. {}",
            command.display(chart),
            stderr,
            diagnostic
        );
    }
    bail!(
        "`{}` failed: {}",
        command.display(chart),
        if stderr.is_empty() {
            "command exited nonzero"
        } else {
            &stderr
        }
    )
}

fn is_helm_timeout(stderr: &str) -> bool {
    let lower = stderr.to_ascii_lowercase();
    lower.contains("timed out")
        || lower.contains("timeout")
        || lower.contains("context deadline exceeded")
}

fn helm_pending_upgrade_recovery(target: &HelmTarget) -> String {
    format!(
        "kubectl delete secret -n {} -l 'owner=helm,name={},status=pending-upgrade'",
        target.namespace, target.release
    )
}

async fn verified_helm_pending_upgrade_recovery(target: &HelmTarget) -> Option<String> {
    let status = ops_command(
        "helm",
        [
            "status",
            target.release.as_str(),
            "-n",
            target.namespace.as_str(),
            "-o",
            "json",
        ],
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let output = run_bounded_diagnostic(&status, deadline, "helm status")
        .await
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).ok()?;
    (value.get("name").and_then(serde_json::Value::as_str) == Some(target.release.as_str())
        && value.get("namespace").and_then(serde_json::Value::as_str)
            == Some(target.namespace.as_str())
        && value
            .pointer("/info/status")
            .and_then(serde_json::Value::as_str)
            == Some("pending-upgrade"))
    .then(|| helm_pending_upgrade_recovery(target))
}

/// Run one read-only timeout diagnostic, stopping it and everything it spawned
/// at `deadline`.
async fn run_bounded_diagnostic(
    command: &crate::ops::OpsCommand,
    deadline: tokio::time::Instant,
    what: &str,
) -> Result<std::process::Output> {
    let mut command = command.tokio_command();
    command
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        // kubectl and helm both run exec credential plugins, which may spawn
        // descendants. Give the diagnostic its own group so a timeout can stop
        // all of it.
        command.as_std_mut().process_group(0);
    }
    let child = command
        .spawn()
        .with_context(|| format!("running {what} for timeout diagnosis"))?;
    let child_id = child.id();
    let mut wait = Box::pin(child.wait_with_output());
    match tokio::time::timeout_at(deadline, &mut wait).await {
        Ok(result) => result.with_context(|| format!("running {what} for timeout diagnosis")),
        Err(_) => {
            #[cfg(unix)]
            if let Some(pgid) = child_id.and_then(|pid| i32::try_from(pid).ok()) {
                // SAFETY: this is the fresh process group created for our
                // diagnostic child, never the CLI's own process group.
                unsafe { libc::kill(-pgid, libc::SIGKILL) };
            }
            // Reap the direct child after killing the group. The bounded wait
            // also covers a credential plugin that kept an output pipe open.
            let _ = tokio::time::timeout(Duration::from_millis(250), &mut wait).await;
            bail!("{what} timed out during diagnosis");
        }
    }
}

async fn diagnostic_kubectl_json(
    namespace: &str,
    resource: &str,
    deadline: tokio::time::Instant,
) -> Result<serde_json::Value> {
    let command = ops_command("kubectl", ["get", resource, "-n", namespace, "-o", "json"]);
    let output =
        run_bounded_diagnostic(&command, deadline, &format!("kubectl get {resource}")).await?;
    if !output.status.success() {
        bail!("kubectl get {resource} failed");
    }
    serde_json::from_slice(&output.stdout).context("invalid Kubernetes diagnosis JSON")
}

async fn pending_pvc_diagnostic(namespace: &str) -> String {
    let hint = format!(
        "If a PVC is Pending, inspect it with `kubectl get pvc -n {namespace}` and `kubectl describe pvc -n {namespace} <claim>`."
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let read = async {
        let pvcs = diagnostic_kubectl_json(namespace, "pvc", deadline).await?;
        let pods = diagnostic_kubectl_json(namespace, "pods", deadline).await?;
        let events = diagnostic_kubectl_json(namespace, "events", deadline).await?;
        Ok::<_, anyhow::Error>((pvcs, pods, events))
    };
    match read.await {
        Ok((pvcs, pods, events)) => {
            pending_pvc_warning(&pvcs, &pods, &events, namespace).unwrap_or(hint)
        }
        _ => hint,
    }
}

fn pending_pvc_warning(
    pvcs: &serde_json::Value,
    pods: &serde_json::Value,
    events: &serde_json::Value,
    namespace: &str,
) -> Option<String> {
    let claims = pvcs.get("items")?.as_array()?;
    let pod_items = pods.get("items")?.as_array()?;
    let event_items = events.get("items")?.as_array()?;
    // A Warning names a failure; a Normal provisioning event only says what the
    // claim is still waiting for, so any matching Warning wins.
    let mut waiting = None;
    for claim in claims {
        if claim
            .pointer("/status/phase")
            .and_then(serde_json::Value::as_str)
            != Some("Pending")
        {
            continue;
        }
        let Some(name) = claim
            .pointer("/metadata/name")
            .and_then(serde_json::Value::as_str)
        else {
            continue;
        };
        if claim
            .pointer("/metadata/namespace")
            .and_then(serde_json::Value::as_str)
            != Some(namespace)
        {
            continue;
        }
        let uid = claim
            .pointer("/metadata/uid")
            .and_then(serde_json::Value::as_str);
        for event in event_items {
            let event_type = event.get("type").and_then(serde_json::Value::as_str);
            if event_type == Some("Normal") && waiting.is_none() {
                waiting = pending_pvc_waiting(event, name, uid, namespace);
                continue;
            }
            if event_type != Some("Warning") {
                continue;
            }
            let Some(object) = event
                .get("involvedObject")
                .or_else(|| event.get("regarding"))
            else {
                continue;
            };
            if object.get("namespace").and_then(serde_json::Value::as_str) != Some(namespace) {
                continue;
            }
            let Some(kind) = object.get("kind").and_then(serde_json::Value::as_str) else {
                continue;
            };
            let Some(object_name) = object.get("name").and_then(serde_json::Value::as_str) else {
                continue;
            };
            let object_uid = object.get("uid").and_then(serde_json::Value::as_str);
            let direct = kind == "PersistentVolumeClaim"
                && object_name == name
                && (uid.is_none() || object_uid.is_none() || uid == object_uid);
            let pod = kind == "Pod"
                && pod_items.iter().any(|pod| {
                    pod.pointer("/metadata/namespace")
                        .and_then(serde_json::Value::as_str)
                        == Some(namespace)
                        && pod
                            .pointer("/metadata/name")
                            .and_then(serde_json::Value::as_str)
                            == Some(object_name)
                        && object_uid.is_some()
                        && pod
                            .pointer("/metadata/uid")
                            .and_then(serde_json::Value::as_str)
                            == object_uid
                        && pod
                            .pointer("/spec/volumes")
                            .and_then(serde_json::Value::as_array)
                            .is_some_and(|volumes| {
                                volumes.iter().any(|volume| {
                                    volume
                                        .pointer("/persistentVolumeClaim/claimName")
                                        .and_then(serde_json::Value::as_str)
                                        == Some(name)
                                })
                            })
                });
            let reason = event
                .get("reason")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("Warning");
            let message = event
                .get("message")
                .or_else(|| event.get("note"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or("");
            if direct
                || (pod
                    && reason == "FailedScheduling"
                    && message.to_ascii_lowercase().contains("unbound")
                    && message
                        .to_ascii_lowercase()
                        .contains("persistentvolumeclaim"))
            {
                return Some(format!("Pending PVC {name} has Kubernetes Warning {reason}: {message}. Inspect `kubectl describe pvc {name} -n {namespace}`."));
            }
        }
    }
    waiting
}

/// Name what a Pending claim is waiting for from one of its own Normal events.
/// The claim's uid must match: a Normal event is ordinary progress, so one left
/// over from a deleted claim of the same name would be a confident wrong cause.
fn pending_pvc_waiting(
    event: &serde_json::Value,
    name: &str,
    uid: Option<&str>,
    namespace: &str,
) -> Option<String> {
    let object = event
        .get("involvedObject")
        .or_else(|| event.get("regarding"))?;
    let field = |key: &str| object.get(key).and_then(serde_json::Value::as_str);
    if field("kind") != Some("PersistentVolumeClaim")
        || field("name") != Some(name)
        || field("namespace") != Some(namespace)
        || uid.is_none()
        || field("uid") != uid
    {
        return None;
    }
    let reason = event.get("reason").and_then(serde_json::Value::as_str)?;
    let message = event
        .get("message")
        .or_else(|| event.get("note"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");
    let inspect = format!("Inspect `kubectl describe pvc {name} -n {namespace}`.");
    match reason {
        "ExternalProvisioning" => {
            let waits_for = external_provisioner_name(message)
                .map(|provisioner| {
                    format!(
                        " It waits for external provisioner \"{provisioner}\"; check that this provisioner is installed and running."
                    )
                })
                .unwrap_or_default();
            Some(format!(
                "Pending PVC {name} has Kubernetes Normal {reason}: {message}.{waits_for} {inspect}"
            ))
        }
        "WaitForFirstConsumer" => Some(format!(
            "Pending PVC {name} has Kubernetes Normal {reason}: {message}. Its StorageClass binds only after a Pod using the claim is scheduled, so check why that Pod is not. {inspect}"
        )),
        _ => None,
    }
}

/// The provisioner quoted in an `ExternalProvisioning` event message. Kubernetes
/// has quoted it both as `'name'` and as `"name"` across releases.
fn external_provisioner_name(message: &str) -> Option<&str> {
    let (_, rest) = message.split_once("external provisioner ")?;
    let quote = rest.chars().next().filter(|c| *c == '\'' || *c == '"')?;
    let rest = &rest[quote.len_utf8()..];
    let (provisioner, _) = rest.split_once(quote)?;
    (!provisioner.is_empty()).then_some(provisioner)
}

async fn kubernetes_connector_kubeconfig(namespace: &str) -> Result<String> {
    connector_kubeconfig(READER_IDENTITY, READER_TOKEN_SECRET, namespace).await
}

/// Build one connector's in-memory kubeconfig from a ServiceAccount token Secret.
///
/// Refuses an absent, malformed, or empty token rather than emitting a
/// kubeconfig the connector would only fail on later.
async fn connector_kubeconfig(
    identity: &str,
    token_secret: &str,
    namespace: &str,
) -> Result<String> {
    let wait_args = [
        "wait",
        "--namespace",
        namespace,
        "--for=jsonpath={.data.token}",
        &format!("secret/{token_secret}"),
        &format!("--timeout={READER_TOKEN_TIMEOUT}"),
    ];
    crate::ui::ui().plumbing(&format!("+ kubectl {}", wait_args.join(" ")));
    let (ok, _stdout, stderr) = crate::ops::run_capture(&ops_command("kubectl", wait_args))
        .await
        .context("waiting for the SRE bot ServiceAccount token")?;
    if !ok {
        bail!(
            "the connector token {token_secret} was not populated within {READER_TOKEN_TIMEOUT}: {}. Inspect it with `kubectl get secret {token_secret} -n {namespace}` and retry",
            if stderr.trim().is_empty() {
                "kubectl wait exited nonzero"
            } else {
                stderr.trim()
            }
        );
    }

    let get_args = [
        "get",
        "secret",
        token_secret,
        "--namespace",
        namespace,
        "-o",
        "json",
    ];
    let (ok, stdout, _stderr) = crate::ops::run_capture(&ops_command("kubectl", get_args))
        .await
        .context("reading the SRE bot ServiceAccount token")?;
    if !ok {
        bail!(
            "could not read Secret {token_secret} in namespace {namespace}; inspect it with `kubectl get secret {token_secret} -n {namespace}` and retry"
        );
    }
    let secret: serde_json::Value = serde_json::from_str(&stdout)
        .context("the SRE bot token Secret returned malformed JSON")?;
    let data = secret
        .get("data")
        .and_then(serde_json::Value::as_object)
        .context("the SRE bot token Secret has no data")?;
    let ca = data
        .get("ca.crt")
        .and_then(serde_json::Value::as_str)
        .context("the SRE bot token Secret has no ca.crt")?;
    base64::engine::general_purpose::STANDARD
        .decode(ca)
        .context("the SRE bot token Secret contains an invalid ca.crt")?;
    let token = data
        .get("token")
        .and_then(serde_json::Value::as_str)
        .context("the SRE bot token Secret has no token")?;
    let token = base64::engine::general_purpose::STANDARD
        .decode(token)
        .context("the SRE bot token Secret contains an invalid token")?;
    let token =
        String::from_utf8(token).context("the SRE bot token Secret contains a non UTF-8 token")?;
    if token.is_empty() {
        bail!("the SRE bot token Secret contains an empty token");
    }

    serde_json::to_string(&serde_json::json!({
        "apiVersion": "v1",
        "kind": "Config",
        "clusters": [{
            "name": "in-cluster",
            "cluster": {
                "server": "https://kubernetes.default.svc",
                "certificate-authority-data": ca,
            },
        }],
        "users": [{
            "name": identity,
            "user": {"token": token},
        }],
        "contexts": [{
            "name": identity,
            "context": {"cluster": "in-cluster", "user": identity},
        }],
        "current-context": identity,
    }))
    .context("serializing the read only connector kubeconfig")
}

struct EmbeddedClusterConnection {
    api_url: String,
    api_key: String,
    _port_forward: Option<tokio::process::Child>,
}

async fn resolve_embedded_cluster_connection(
    identity: &InstallIdentity,
) -> Result<EmbeddedClusterConnection> {
    let api_key = crate::ops::discover_api_key(&identity.namespace, &identity.release).await?;
    let explicit_api_url = std::env::var("CURIE_API_URL")
        .ok()
        .filter(|value| !value.trim().is_empty());
    let local_port = crate::message::DEFAULT_API_LOCAL_PORT;
    let tunnel = commands::deploy_api_tunnel(
        explicit_api_url.as_deref(),
        &identity.namespace,
        &identity.release,
        local_port,
        crate::message::API_REMOTE_PORT,
    )
    .await;
    let (api_url, port_forward) = match tunnel {
        Some((_fullname, command)) => {
            let (child, effective_port) =
                crate::message::start_port_forward(&command, local_port, "SRE bot deploy API")
                    .await?;
            (format!("http://localhost:{effective_port}"), Some(child))
        }
        None => {
            let url = explicit_api_url.expect("explicit API URL when no port forward is planned");
            if crate::api::is_insecure_endpoint(&url) {
                bail!(
                    "refusing to send the auto-discovered release key over cleartext HTTP to {url}; use an https:// URL or unset CURIE_API_URL to use the loopback port-forward"
                );
            }
            (url, None)
        }
    };
    Ok(EmbeddedClusterConnection {
        api_url,
        api_key,
        _port_forward: port_forward,
    })
}

/// The approval route every shipped Kubernetes mutation gate names.
const SRE_APPROVALS_ROUTE: &str = "sre-approvals";
/// The agent the embedded bundle deploys as (its plugin name).
const SRE_BOT_AGENT: &str = "sre-bot";

/// Split, trim, and validate the raw `--approvers` values. A blank id is a usage
/// error raised before any cluster work, never silently skipped: dropping it
/// would bind a narrower approver set than the operator typed.
fn parse_approvers(raw: &[String]) -> Result<Vec<String>> {
    if raw.is_empty() {
        return Err(crate::exit::usage(
            "at least one explicit Slack user ID is required; pass --approvers <USER_IDS>",
        ));
    }
    let mut ids = Vec::new();
    for value in raw {
        for id in value.split(',') {
            let id = id.trim();
            if id.is_empty() {
                return Err(crate::exit::usage(format!(
                    "--approvers {value:?} contains a blank user ID; pass comma separated Slack \
                     user IDs such as --approvers U0123ABCD,U0456DEFG"
                )));
            }
            if !ids.iter().any(|seen: &String| seen == id) {
                ids.push(id.to_string());
            }
        }
    }
    Ok(ids)
}

fn route_binding_as_write(
    binding: &crate::api::ApprovalRouteBindingResponse,
) -> crate::api::ApprovalRouteBindingWrite {
    crate::api::ApprovalRouteBindingWrite {
        resolution: binding.resolution.clone().into(),
        // The response omits the notification's transport (endpoint, adapter),
        // so it cannot be written back faithfully. Callers refuse any bound
        // notification first (`refuse_unwritable_notifications`).
        notification: None,
        approvers: binding.approvers.clone(),
    }
}

/// The full-replacement route map the installer writes: every other bound route
/// is kept as is, while `sre-approvals` is replaced with `channel` and the
/// explicit approver list.
///
/// Notifications do not survive this map; call
/// [`refuse_unwritable_notifications`] on `existing` before writing it.
fn sre_approvals_route_map(
    existing: Option<&BTreeMap<String, crate::api::ApprovalRouteBindingResponse>>,
    channel: &str,
    approvers: &[String],
) -> BTreeMap<String, crate::api::ApprovalRouteBindingWrite> {
    let mut map: BTreeMap<String, crate::api::ApprovalRouteBindingWrite> = existing
        .map(|routes| {
            routes
                .iter()
                .map(|(name, binding)| (name.clone(), route_binding_as_write(binding)))
                .collect()
        })
        .unwrap_or_default();
    map.insert(
        SRE_APPROVALS_ROUTE.to_string(),
        crate::api::ApprovalRouteBindingWrite {
            resolution: crate::api::ApprovalResolutionWrite::slack(channel),
            notification: None,
            approvers: Some(crate::api::ApprovalApprovers {
                group: None,
                users: Some(approvers.to_vec()),
                emails: None,
            }),
        },
    );
    map
}

/// Refuse to rewrite a route map that carries a notification target. The API
/// response redacts its endpoint and adapter, and a route write replaces the
/// whole map, so writing it back would silently drop or corrupt that ping.
fn refuse_unwritable_notifications(
    existing: Option<&BTreeMap<String, crate::api::ApprovalRouteBindingResponse>>,
) -> Result<()> {
    let with_notification: Vec<&str> = existing
        .into_iter()
        .flatten()
        .filter(|(_, binding)| binding.notification.is_some())
        .map(|(name, _)| name.as_str())
        .collect();
    if with_notification.is_empty() {
        return Ok(());
    }
    Err(crate::exit::CliError::usage(format!(
        "refusing to bind route {SRE_APPROVALS_ROUTE} on agent {SRE_BOT_AGENT}: route(s) {} \
         carry a notification target whose transport the API does not return, and a route \
         write replaces the whole map, so this installer cannot keep it. Nothing was deployed.",
        with_notification.join(", ")
    ))
    .with_fix(format!(
        "write the full route map yourself, including {SRE_APPROVALS_ROUTE} and every \
         notification, with `curie cluster approvals {SRE_BOT_AGENT} --routes-from <file>`, then \
         re-run this installer"
    ))
    .into())
}

/// Ensure the `sre-bot` agent exists and its `sre-approvals` route is bound,
/// writing only when the computed map differs from what is bound.
async fn bind_sre_approvals_route(
    connection: &EmbeddedClusterConnection,
    slack_channel: Option<&str>,
    approvers: &[String],
) -> Result<()> {
    let ui = crate::ui::ui();
    let client = crate::api::ApiClient::new(&connection.api_url, &connection.api_key)?;
    // The same resolution the deploy performs next: an absent agent is created
    // on --slack-channel or the platform default channel, so this adds nothing
    // the deploy would not.
    let (agent, _, _) = client
        .resolve_agent(SRE_BOT_AGENT, slack_channel, None)
        .await?;
    let existing = agent.approval_routes.as_ref();
    let channel = match slack_channel {
        Some(channel) => channel.to_string(),
        None => agent
            .channels
            .iter()
            .find(|binding| binding.kind == "slack")
            .map(|binding| binding.address.clone())
            .ok_or_else(|| {
                crate::exit::usage(format!(
                    "agent {SRE_BOT_AGENT} has no Slack channel binding to resolve route \
                     {SRE_APPROVALS_ROUTE} on; pass --slack-channel <CHANNEL>"
                ))
            })?,
    };
    let desired = sre_approvals_route_map(existing, &channel, approvers);
    let current: BTreeMap<String, crate::api::ApprovalRouteBindingWrite> = existing
        .map(|routes| {
            routes
                .iter()
                .map(|(name, binding)| (name.clone(), route_binding_as_write(binding)))
                .collect()
        })
        .unwrap_or_default();
    if desired != current {
        refuse_unwritable_notifications(existing)?;
        client.set_approval_routes(&agent.id, &desired).await?;
        let action = if existing.is_some_and(|routes| routes.contains_key(SRE_APPROVALS_ROUTE)) {
            "rebound"
        } else {
            "bound"
        };
        ui.note(&format!(
            "{action} approval route {SRE_APPROVALS_ROUTE} on agent {SRE_BOT_AGENT}: resolution \
             {channel}; approvers users {}",
            approvers.join(",")
        ));
    }
    Ok(())
}

async fn deploy_embedded_sre_bot(
    bundle_dir: &Path,
    connection: &EmbeddedClusterConnection,
    slack_channel: Option<&str>,
) -> Result<commands::DeployOutput> {
    let connect_hint = format!(
        "the platform API at {} is unreachable; confirm the Curie release with `curie cluster status` and retry this installer",
        connection.api_url
    );
    commands::deploy_with_commit_sha(
        DeployOpts {
            // This installer binds no repository (`repo: None`), so there is
            // no binding to make a push-delivery claim about, and delivery is
            // not assessed (#2496).
            delivery: None,
            agent: None,
            target: None,
            identity: None,
            plugin_dir: bundle_dir.to_path_buf(),
            api_url: connection.api_url.clone(),
            api_key: connection.api_key.clone(),
            slack_channel: slack_channel.map(str::to_string),
            repo: None,
            workspace: commands::WorkspaceIntent::Preserve,
            env: None,
            label: None,
            secret: vec![],
            secret_binding_supported: false,
            connect_hint,
            tier: DeployTier::Cluster,
        },
        crate::artifacts::commit_sha(),
    )
    .await
}

struct EmbeddedWorkspace {
    root: PathBuf,
}

impl EmbeddedWorkspace {
    fn create(
        tempo_digest: &str,
        identity: &InstallIdentity,
        upgrade_digest: Option<&str>,
        log_runtime: LogRuntime,
    ) -> Result<Self> {
        let root = std::env::temp_dir().join(format!(
            "curie-sre-bot-install-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir(&root)
            .with_context(|| format!("creating embedded SRE bot workspace {}", root.display()))?;
        let workspace = Self { root };
        workspace.write_observability_files(&identity.observability_namespace, log_runtime)?;
        for (name, contents) in BUNDLE_FILES {
            if *name == "connectors.yaml" {
                let runtime = runtime_connector_declaration(
                    contents,
                    tempo_digest,
                    &identity.observability_namespace,
                    upgrade_digest,
                )?;
                workspace.write(&Path::new("bundle").join(name), &runtime)?;
            } else if *name == ".claude-plugin/plugin.json" {
                let runtime = runtime_plugin_manifest(contents, upgrade_digest.is_some())?;
                workspace.write(&Path::new("bundle").join(name), &runtime)?;
            } else if *name == "manifests/upgrade-role.yaml" {
                // Both upgrade identities are written only when the path is
                // opted into. Writing them otherwise would leave manifests
                // describing grants this install deliberately did not create,
                // next to ones it did -- the same trap the write Role avoids.
                if upgrade_digest.is_none() {
                    continue;
                }
                let rendered = render_upgrade_role(contents, &identity.namespace)?;
                workspace.write(&Path::new("bundle").join(name), &rendered)?;
            } else if *name == "manifests/platform-upgrade-role.yaml" {
                if upgrade_digest.is_none() {
                    continue;
                }
                let rendered = render_platform_upgrade_role(contents, &identity.namespace)?;
                workspace.write(&Path::new("bundle").join(name), &rendered)?;
            } else if *name == "manifests/kubernetes-access.yaml" {
                let rendered = render_read_access(contents, &identity.namespace)?;
                workspace.write(&Path::new("bundle").join(name), &rendered)?;
            } else {
                workspace.write(&Path::new("bundle").join(name), contents)?;
            }
        }
        if upgrade_digest.is_some() {
            // Not bundle files: cluster objects this installer renders and
            // applies. They live beside the bundle in the workspace so a failed
            // install leaves exactly what was about to be applied on disk.
            workspace.write(
                Path::new("bundle/manifests/platform-upgrade-configmap.yaml"),
                &render_platform_upgrade_configmap(PLATFORM_UPGRADE_SCRIPT, &identity.namespace)?,
            )?;
            workspace.write(
                Path::new("bundle/manifests/platform-upgrade-cronjob.yaml"),
                &render_platform_cronjob(
                    PLATFORM_UPGRADE_CRONJOB_YAML,
                    &identity.namespace,
                    &identity.release,
                    PLATFORM_UPGRADE_SOURCE_REPO,
                )?,
            )?;
        }
        Ok(workspace)
    }

    fn create_observability(
        observability_namespace: &str,
        log_runtime: LogRuntime,
    ) -> Result<Self> {
        let root = std::env::temp_dir().join(format!(
            "curie-sre-bot-observability-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir(&root).with_context(|| {
            format!(
                "creating embedded observability workspace {}",
                root.display()
            )
        })?;
        let workspace = Self { root };
        workspace.write_observability_files(observability_namespace, log_runtime)?;
        Ok(workspace)
    }

    fn write(&self, relative: &Path, contents: &[u8]) -> Result<()> {
        let path = self.root.join(relative);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating {}", parent.display()))?;
        }
        std::fs::write(&path, contents).with_context(|| format!("writing {}", path.display()))
    }

    fn observability_dir(&self) -> PathBuf {
        self.root.join("observability")
    }

    fn bundle_dir(&self) -> PathBuf {
        self.root.join("bundle")
    }

    fn write_observability_files(
        &self,
        observability_namespace: &str,
        log_runtime: LogRuntime,
    ) -> Result<()> {
        for (name, contents) in OBSERVABILITY_FILES {
            let rendered = rewrite_observability_namespace(contents, observability_namespace);
            let rendered = if *name == "alloy-values.yaml" {
                render_alloy_values(&rendered, log_runtime)?
            } else {
                rendered
            };
            self.write(&Path::new("observability").join(name), &rendered)?;
        }
        Ok(())
    }
}

fn rewrite_observability_namespace(contents: &[u8], observability_namespace: &str) -> Vec<u8> {
    if observability_namespace == OBSERVABILITY_NAMESPACE {
        return contents.to_vec();
    }
    let text = String::from_utf8_lossy(contents);
    text.replace(
        &format!(".{OBSERVABILITY_NAMESPACE}.svc.cluster.local"),
        &format!(".{observability_namespace}.svc.cluster.local"),
    )
    .replace(
        &format!("namespace: {OBSERVABILITY_NAMESPACE}"),
        &format!("namespace: {observability_namespace}"),
    )
    // PromQL string matchers in the shipped Alloy alerts are not YAML keys.
    // They must follow --observability-namespace just like scrape targets do.
    .replace(
        &format!("namespace=\"{OBSERVABILITY_NAMESPACE}\""),
        &format!("namespace=\"{observability_namespace}\""),
    )
    .replace(
        &format!("kubernetes.io/metadata.name: {OBSERVABILITY_NAMESPACE}"),
        &format!("kubernetes.io/metadata.name: {observability_namespace}"),
    )
    .into_bytes()
}

fn rewrite_manifest_namespace(value: &mut serde_json::Value, namespace: &str) {
    if let Some(metadata) = value
        .get_mut("metadata")
        .and_then(serde_json::Value::as_object_mut)
    {
        if metadata
            .get("namespace")
            .and_then(serde_json::Value::as_str)
            == Some(CURIE_NAMESPACE)
        {
            metadata.insert(
                "namespace".to_string(),
                serde_json::Value::String(namespace.to_string()),
            );
        }
    }
    if let Some(subjects) = value
        .get_mut("subjects")
        .and_then(serde_json::Value::as_array_mut)
    {
        for subject in subjects {
            if let Some(object) = subject.as_object_mut() {
                if object.get("namespace").and_then(serde_json::Value::as_str)
                    == Some(CURIE_NAMESPACE)
                {
                    object.insert(
                        "namespace".to_string(),
                        serde_json::Value::String(namespace.to_string()),
                    );
                }
            }
        }
    }
}

fn render_read_access(source: &[u8], namespace: &str) -> Result<Vec<u8>> {
    if namespace == CURIE_NAMESPACE {
        return Ok(source.to_vec());
    }
    let source = std::str::from_utf8(source)
        .context("embedded SRE bot kubernetes-access.yaml is not UTF-8")?;
    let mut rendered = String::new();
    for document in serde_norway::Deserializer::from_str(source) {
        let mut value: serde_json::Value = serde::Deserialize::deserialize(document)
            .context("parsing embedded SRE bot kubernetes-access.yaml")?;
        rewrite_manifest_namespace(&mut value, namespace);
        rendered.push_str("---\n");
        rendered.push_str(
            &serde_norway::to_string(&value)
                .context("serializing the rendered SRE bot read identity")?,
        );
    }
    Ok(rendered.into_bytes())
}

/// The CONNECTOR's upgrade identity, in the install's namespace.
///
/// A namespace rewrite and nothing else: unlike the write Role, its grant is
/// fixed rather than derived from operator input, so there is no allowlist to
/// render and nothing for this to get wrong beyond the namespace.
fn render_upgrade_role(source: &[u8], namespace: &str) -> Result<Vec<u8>> {
    if namespace == CURIE_NAMESPACE {
        return Ok(source.to_vec());
    }
    let source =
        std::str::from_utf8(source).context("embedded SRE bot upgrade-role.yaml is not UTF-8")?;
    let mut rendered = String::new();
    for document in serde_norway::Deserializer::from_str(source) {
        let mut value: serde_json::Value = serde::Deserialize::deserialize(document)
            .context("parsing embedded SRE bot upgrade-role.yaml")?;
        rewrite_manifest_namespace(&mut value, namespace);
        rendered.push_str("---\n");
        rendered.push_str(
            &serde_norway::to_string(&value)
                .context("serializing the rendered SRE bot upgrade identity")?,
        );
    }
    Ok(rendered.into_bytes())
}

/// The ConfigMap carrying the upgrade script the Job runs.
///
/// Rendered rather than `kubectl create configmap --from-file`, so the whole
/// install is one stream of `kubectl apply` over files this process wrote --
/// idempotent on a re-install, and inspectable in the workspace when it fails.
fn render_platform_upgrade_configmap(script: &[u8], namespace: &str) -> Result<Vec<u8>> {
    let script = std::str::from_utf8(script)
        .context("embedded SRE bot platform upgrade script is not UTF-8")?;
    let document = serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": PLATFORM_UPGRADE_CONFIGMAP, "namespace": namespace},
        "data": {"upgrade.sh": script},
    });
    let mut rendered = String::from("---\n");
    rendered.push_str(
        &serde_norway::to_string(&document)
            .context("serializing the SRE bot platform upgrade ConfigMap")?,
    );
    Ok(rendered.into_bytes())
}

fn embedded_bundle_file(name: &str) -> Result<&'static [u8]> {
    BUNDLE_FILES
        .iter()
        .find(|(candidate, _)| *candidate == name)
        .map(|(_, contents)| *contents)
        .ok_or_else(|| anyhow!("embedded SRE bot is missing {name}"))
}

fn yaml_str_list<'a>(value: &'a serde_json::Value, key: &str) -> Vec<&'a str> {
    value
        .get(key)
        .and_then(serde_json::Value::as_array)
        .map(|items| items.iter().filter_map(serde_json::Value::as_str).collect())
        .unwrap_or_default()
}

/// The shipped Role's rules, asserted against what this build knows how to render.
///
/// This is the widest grant in the bundle -- namespace-admin in all but name --
/// so a manifest that grows a rule or a verb must stop the install rather than
/// have the installer grant it silently.
fn asserted_platform_rules(source: &[u8]) -> Result<Vec<serde_json::Value>> {
    let source = std::str::from_utf8(source)
        .context("embedded SRE bot platform-upgrade-role.yaml is not UTF-8")?;
    let mut rules: Option<Vec<serde_json::Value>> = None;
    for document in serde_norway::Deserializer::from_str(source) {
        let value: serde_json::Value = serde::Deserialize::deserialize(document)
            .context("parsing embedded SRE bot platform-upgrade-role.yaml")?;
        if value.get("kind").and_then(serde_json::Value::as_str) != Some("Role") {
            continue;
        }
        rules = Some(
            value
                .get("rules")
                .and_then(serde_json::Value::as_array)
                .context("the embedded platform-upgrade Role declares no rules")?
                .clone(),
        );
    }
    let rules = rules.context("the embedded platform-upgrade-role.yaml declares no Role")?;
    if rules.len() != PLATFORM_RULE_SHAPE.len() {
        bail!(
            "the embedded platform-upgrade Role declares {} rules; this build renders exactly \
             {}. Widening the grant needs a matching change here, because this installer is \
             what creates it.",
            rules.len(),
            PLATFORM_RULE_SHAPE.len()
        );
    }
    for (index, (group, resources, verbs)) in PLATFORM_RULE_SHAPE.iter().enumerate() {
        let rule = &rules[index];
        let groups = yaml_str_list(rule, "apiGroups");
        let actual = yaml_str_list(rule, "resources");
        let actual_verbs = yaml_str_list(rule, "verbs");
        if groups != [*group] || actual != *resources || actual_verbs != *verbs {
            bail!(
                "the embedded platform-upgrade Role's rule {index} is {groups:?}/{actual:?}/{actual_verbs:?}, but \
                 this build only knows how to render {:?}/{resources:?}/{verbs:?}",
                [group]
            );
        }
    }
    Ok(rules)
}

fn platform_upgrade_grant_summary(rules: &[serde_json::Value]) -> String {
    rules
        .iter()
        .map(|rule| {
            format!(
                "{:?}/{:?}/{:?}",
                yaml_str_list(rule, "apiGroups"),
                yaml_str_list(rule, "resources"),
                yaml_str_list(rule, "verbs")
            )
        })
        .collect::<Vec<_>>()
        .join("; ")
}

/// `--dry-run` disclosure for the Job identity, derived from the Role this
/// build would apply. A hand-written summary would keep describing the
/// unmodified grant after the YAML changed.
fn platform_upgrade_role_plan_line(source: &[u8], namespace: &str) -> Result<String> {
    let rules = asserted_platform_rules(source)?;
    Ok(format!(
        "kubectl apply -f examples/sre-bot/manifests/platform-upgrade-role.yaml -- the \
         JOB's identity ({PLATFORM_UPGRADER_IDENTITY}) in namespace {namespace}: {}. \
         READ THAT FILE. It exists for the ~90s an upgrade runs and the sandbox \
         never sees it",
        platform_upgrade_grant_summary(&rules)
    ))
}

/// The platform-upgrade identity, rendered into the release's namespace.
///
/// The shipped manifest's rules are ASSERTED against what this build knows how to
/// render before anything is emitted. This is the widest grant in the bundle --
/// namespace-admin in all but name -- so a manifest that grows a rule must stop
/// the install rather than have the installer grant it silently.
fn render_platform_upgrade_role(source: &[u8], curie_namespace: &str) -> Result<Vec<u8>> {
    let rules = asserted_platform_rules(source)?;

    let mut documents: Vec<serde_json::Value> = vec![
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "ServiceAccount",
            "metadata": {"name": PLATFORM_UPGRADER_IDENTITY, "namespace": curie_namespace},
        }),
        serde_json::json!({
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "Role",
            "metadata": {"name": PLATFORM_UPGRADER_IDENTITY, "namespace": curie_namespace},
            "rules": rules,
        }),
        serde_json::json!({
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "RoleBinding",
            "metadata": {"name": PLATFORM_UPGRADER_IDENTITY, "namespace": curie_namespace},
            "roleRef": {
                "apiGroup": "rbac.authorization.k8s.io",
                "kind": "Role",
                "name": PLATFORM_UPGRADER_IDENTITY,
            },
            "subjects": [{
                "kind": "ServiceAccount",
                "name": PLATFORM_UPGRADER_IDENTITY,
                "namespace": curie_namespace,
            }],
        }),
    ];
    // No static token Secret, unlike the reader and writer identities. This one
    // is a Job's ServiceAccount: Kubernetes projects its token into the pod for
    // the ninety seconds the upgrade runs. A static token would be a
    // namespace-admin credential sitting in a Secret forever, which is exactly
    // what this shape exists to avoid.
    let mut rendered = String::new();
    for document in documents.drain(..) {
        rendered.push_str("---\n");
        rendered.push_str(
            &serde_norway::to_string(&document)
                .context("serializing the rendered SRE bot platform upgrade identity")?,
        );
    }
    Ok(rendered.into_bytes())
}

/// The platform-upgrade CronJob, pointed at this install.
///
/// The shipped file carries `CHANGE ME` placeholders for the namespace, the
/// release and the repository. Rewriting them here is the whole reason this flag
/// exists: an operator editing four values across two files gets one of them
/// wrong, and the failure is a tool that refuses every call with nothing visibly
/// wrong in either file.
fn render_platform_cronjob(
    source: &[u8],
    curie_namespace: &str,
    release: &str,
    repo: &str,
) -> Result<Vec<u8>> {
    let source = std::str::from_utf8(source)
        .context("embedded SRE bot platform-upgrade cronjob.yaml is not UTF-8")?;
    let mut cronjob: Option<serde_json::Value> = None;
    for document in serde_norway::Deserializer::from_str(source) {
        let value: serde_json::Value = serde::Deserialize::deserialize(document)
            .context("parsing embedded SRE bot platform-upgrade cronjob.yaml")?;
        if value.get("kind").and_then(serde_json::Value::as_str) == Some("CronJob") {
            cronjob = Some(value);
        }
    }
    let mut cronjob =
        cronjob.context("the embedded platform-upgrade cronjob.yaml has no CronJob")?;

    let metadata = cronjob
        .get_mut("metadata")
        .and_then(serde_json::Value::as_object_mut)
        .context("the embedded platform-upgrade CronJob has no metadata")?;
    metadata.insert(
        "namespace".to_string(),
        serde_json::Value::String(curie_namespace.to_string()),
    );
    // The name the connector is told about. Asserted rather than trusted: if the
    // shipped file is renamed, the connector's env would point at a CronJob that
    // does not exist and every call would refuse.
    let name = metadata
        .get("name")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    if name != PLATFORM_UPGRADE_CRONJOB_NAME {
        bail!(
            "the embedded platform-upgrade CronJob is named {name:?}, but this build tells the \
             connector to start {PLATFORM_UPGRADE_CRONJOB_NAME:?}"
        );
    }
    // Suspended is not negotiable here. This installer's contract is that the
    // upgrade happens when a human approves one, never on a timer nobody chose.
    cronjob
        .pointer_mut("/spec")
        .and_then(serde_json::Value::as_object_mut)
        .context("the embedded platform-upgrade CronJob has no spec")?
        .insert("suspend".to_string(), serde_json::Value::Bool(true));

    let env = cronjob
        .pointer_mut("/spec/jobTemplate/spec/template/spec/containers/0/env")
        .and_then(serde_json::Value::as_array_mut)
        .context("the embedded platform-upgrade CronJob container declares no env")?;
    let mut seen = 0usize;
    for entry in env.iter_mut() {
        let Some(object) = entry.as_object_mut() else {
            continue;
        };
        let replacement = match object.get("name").and_then(serde_json::Value::as_str) {
            Some("PLATFORM_UPGRADE_NAMESPACE") => curie_namespace,
            Some("PLATFORM_UPGRADE_RELEASE") => release,
            Some("PLATFORM_UPGRADE_REPO") => repo,
            _ => continue,
        };
        object.insert(
            "value".to_string(),
            serde_json::Value::String(replacement.to_string()),
        );
        seen += 1;
    }
    if seen != 3 {
        bail!(
            "the embedded platform-upgrade CronJob carries {seen} of the 3 values this build \
             rewrites (namespace, release, repository); the rest would keep their placeholders"
        );
    }

    let mut rendered = String::from("---\n");
    rendered.push_str(
        &serde_norway::to_string(&cronjob)
            .context("serializing the rendered SRE bot platform upgrade CronJob")?,
    );
    Ok(rendered.into_bytes())
}

fn runtime_connector_declaration(
    source: &[u8],
    tempo_digest: &str,
    observability_namespace: &str,
    upgrade_digest: Option<&str>,
) -> Result<Vec<u8>> {
    let source =
        std::str::from_utf8(source).context("embedded SRE bot connectors.yaml is not UTF-8")?;
    let mut declaration: serde_json::Value =
        serde_norway::from_str(source).context("parsing embedded SRE bot connectors.yaml")?;
    let connectors = declaration
        .get_mut("connectors")
        .and_then(serde_json::Value::as_object_mut)
        .context("embedded SRE bot must declare connectors")?;
    // Self-upgrade stays out too, and for a stronger reason than scale. It is
    // inert without a CronJob this installer does not create, and its identity
    // holds namespace-wide `create` on `jobs` in the namespace that holds the
    // platform API key (see manifests/upgrade-role.yaml). A grant that wide is
    // an operator's decision made while reading that file, never a side effect
    // of running an installer.
    match upgrade_digest {
        // Kept for upgrade_platform only. The bundle ships
        // PLATFORM_UPGRADE_CRONJOB empty, and an install that hand-edits it finds
        // the worker's connector reconciler putting the declaration back within
        // the minute -- so the installer is the only thing that can make it real.
        // SELF_UPGRADE_CRONJOB is rendered empty on purpose (#2288): this path
        // never applies the self-upgrade CronJob, so naming it would arm a verb
        // that spends a human approval and then reports the Job missing. Empty
        // makes the connector refuse every upgrade_self call.
        Some(digest) => {
            let upgrade = connectors
                .get_mut("self-upgrade")
                .and_then(serde_json::Value::as_object_mut)
                .context("embedded SRE bot must declare connectors.self-upgrade")?;
            if upgrade.remove("build").is_none() || upgrade.contains_key("image") {
                bail!(
                    "embedded SRE bot self-upgrade connector must declare one build source and \
                     no image before immutable resolution"
                );
            }
            upgrade.insert(
                "image".to_string(),
                serde_json::Value::String(format!("{SELF_UPGRADE_IMAGE_REPOSITORY}@{digest}")),
            );
            let env = upgrade
                .entry("env")
                .or_insert_with(|| serde_json::Value::Object(Default::default()))
                .as_object_mut()
                .context("embedded SRE bot self-upgrade connector env is not a mapping")?;
            env.insert(
                PLATFORM_UPGRADE_CRONJOB_ENV.to_string(),
                serde_json::Value::String(PLATFORM_UPGRADE_CRONJOB_NAME.to_string()),
            );
            env.insert(
                SELF_UPGRADE_CRONJOB_ENV.to_string(),
                serde_json::Value::String(String::new()),
            );
        }
        // Stripped exactly as before this flag existed: inert without the Job,
        // and its identity is an operator's decision, never an installer's.
        None => {
            if connectors.remove("self-upgrade").is_none() {
                bail!("embedded SRE bot must declare connectors.self-upgrade");
            }
        }
    }
    // Fail closed on a connector this build does not know about. The bundle is
    // edited far more often than this file, so an
    // unrecognized connector must stop the install rather than ship in it.
    let known: &[&str] = match upgrade_digest.is_some() {
        true => &["kubernetes", "grafana", "tempo", "self-upgrade"],
        false => &["kubernetes", "grafana", "tempo"],
    };
    if let Some(unexpected) = connectors
        .keys()
        .find(|name| !known.contains(&name.as_str()))
    {
        bail!(
            "embedded SRE bot declares connector {unexpected}, which this build does not \
             know how to classify"
        );
    }
    let tempo = connectors
        .get_mut("tempo")
        .and_then(serde_json::Value::as_object_mut)
        .context("embedded SRE bot must declare connectors.tempo")?;
    if tempo.remove("build").is_none() || tempo.contains_key("image") {
        bail!(
            "embedded SRE bot Tempo connector must declare one build source and no image before immutable resolution"
        );
    }
    tempo.insert(
        "image".to_string(),
        serde_json::Value::String(format!("{TEMPO_IMAGE_REPOSITORY}@{tempo_digest}")),
    );
    let serialized = serde_norway::to_string(&declaration)
        .context("serializing the immutable SRE bot connector declaration")?;
    Ok(rewrite_observability_namespace(
        serialized.as_bytes(),
        observability_namespace,
    ))
}

fn is_self_upgrade_policy_entry(entry: &serde_json::Value) -> bool {
    let Some(name) = entry.as_str() else {
        return false;
    };
    matches!(name.split_once('/'), Some(("self-upgrade", _)))
}

fn runtime_plugin_manifest(source: &[u8], upgrade_enabled: bool) -> Result<Vec<u8>> {
    let mut manifest: serde_json::Value =
        serde_json::from_slice(source).context("parsing embedded SRE bot plugin.json")?;
    // Pinned, not merely present. approvalPolicy governs platform publication,
    // the optional self upgrade connector, and the Kubernetes mutations.
    let mut expected_gates = vec![
        serde_json::json!({"gate": PLATFORM_PUBLISH_GATE, "route": "sre-approvals"}),
        serde_json::json!({"gate": UPGRADE_GATE, "route": "sre-approvals"}),
        serde_json::json!({"gate": PLATFORM_UPGRADE_GATE, "route": "sre-approvals"}),
    ];
    for tool in KUBERNETES_MUTATION_TOOLS {
        expected_gates.push(serde_json::json!({
            "gate": format!("mcp__kubernetes__{tool}"),
            "route": "sre-approvals"
        }));
    }
    let expected_policy = serde_json::json!({ "gates": expected_gates });
    if manifest.get("approvalPolicy") != Some(&expected_policy) {
        bail!("embedded SRE bot must declare the exact gated write verbs");
    }
    let manifest = manifest
        .as_object_mut()
        .context("embedded SRE bot plugin.json must be an object")?;
    let tool_policy = manifest
        .get_mut("toolPolicy")
        .and_then(serde_json::Value::as_object_mut)
        .context("embedded SRE bot must declare toolPolicy")?;
    let allow = tool_policy
        .get_mut("allow")
        .and_then(serde_json::Value::as_array_mut)
        .context("embedded SRE bot toolPolicy.allow must be an array")?;
    for tool in [UPGRADE_TOOL, PLATFORM_UPGRADE_TOOL, LATEST_RELEASE_TOOL] {
        if !allow.iter().any(|entry| entry.as_str() == Some(tool)) {
            bail!("embedded SRE bot toolPolicy.allow must contain {tool}");
        }
    }
    // upgrade_self is never armed: no install path applies the CronJob it
    // starts (#2288). Out of allow, the tool policy refuses it before any
    // approval card is raised.
    allow.retain(|entry| entry.as_str() != Some(UPGRADE_TOOL));
    if !upgrade_enabled {
        // Default install strips connectors.self-upgrade. Any leftover
        // self-upgrade/* allow entry fails the bundle validator with
        // tool_policy.unknown_server, which is how latest_release escaped #2404.
        allow.retain(|entry| !is_self_upgrade_policy_entry(entry));
    }
    // Keep exactly the gates and tool-policy entries whose connectors survived.
    // Either kind of reference to a stripped connector fails bundle validation;
    // a kept connector without both layers would bypass the intended gate.
    // Platform publication and the Kubernetes connector are never stripped.
    // Their gates stay present regardless of upgrade_enabled so their calls
    // always carry a route that an operator principal can resolve.
    let mut kept =
        vec![serde_json::json!({"gate": PLATFORM_PUBLISH_GATE, "route": "sre-approvals"})];
    if upgrade_enabled {
        kept.push(serde_json::json!({"gate": PLATFORM_UPGRADE_GATE, "route": "sre-approvals"}));
    }
    for tool in KUBERNETES_MUTATION_TOOLS {
        kept.push(serde_json::json!({
            "gate": format!("mcp__kubernetes__{tool}"),
            "route": "sre-approvals"
        }));
    }
    // approvalPolicy is never removed because publication and the Kubernetes
    // gates above are always present.
    manifest.insert(
        "approvalPolicy".to_string(),
        serde_json::json!({"gates": kept}),
    );
    serde_json::to_vec_pretty(&manifest).context("serializing the SRE bot plugin manifest")
}

impl Drop for EmbeddedWorkspace {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

#[derive(Deserialize)]
struct KubeList<T> {
    items: Vec<T>,
}

#[derive(Deserialize)]
struct Node {
    metadata: ObjectMeta,
    #[serde(default)]
    spec: NodeSpec,
    status: NodeStatus,
}

#[derive(Deserialize)]
struct ObjectMeta {
    name: String,
    #[serde(default)]
    namespace: String,
    #[serde(default)]
    labels: BTreeMap<String, String>,
}

#[derive(Default, Deserialize)]
struct NodeSpec {
    #[serde(default)]
    unschedulable: bool,
    #[serde(default)]
    taints: Vec<NodeTaint>,
}

#[derive(Deserialize)]
struct NodeTaint {
    key: String,
    effect: String,
}

#[derive(Deserialize)]
struct NodeStatus {
    allocatable: BTreeMap<String, String>,
    conditions: Vec<NodeCondition>,
    #[serde(rename = "nodeInfo", default)]
    node_info: NodeInfo,
}

#[derive(Default, Deserialize)]
struct NodeInfo {
    #[serde(rename = "containerRuntimeVersion", default)]
    container_runtime_version: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LogRuntime {
    Cri,
    Docker,
}

fn alloy_can_schedule_on(node: &Node) -> bool {
    // The checked-in Alloy values have no custom tolerations or hostNetwork.
    // Kubernetes automatically adds only these DaemonSet tolerations.
    // https://kubernetes.io/docs/concepts/workloads/controllers/daemonset/
    // Condition taints are lifted by their controllers once the node is
    // healthy or initialized, and the DaemonSet then places Alloy there, so a
    // node behind one still decides the parser.
    // https://kubernetes.io/docs/reference/labels-annotations-taints/
    node.spec
        .taints
        .iter()
        .filter(|taint| !is_transient_condition_taint(&taint.key))
        .all(|taint| match taint.effect.as_str() {
            "NoExecute" => matches!(
                taint.key.as_str(),
                "node.kubernetes.io/not-ready" | "node.kubernetes.io/unreachable"
            ),
            "NoSchedule" => matches!(
                taint.key.as_str(),
                "node.kubernetes.io/disk-pressure"
                    | "node.kubernetes.io/memory-pressure"
                    | "node.kubernetes.io/pid-pressure"
                    | "node.kubernetes.io/unschedulable"
            ),
            _ => true,
        })
}

fn is_transient_condition_taint(key: &str) -> bool {
    matches!(
        key,
        "node.kubernetes.io/not-ready"
            | "node.kubernetes.io/unreachable"
            | "node.kubernetes.io/network-unavailable"
            | "node.cloudprovider.kubernetes.io/uninitialized"
    )
}

fn select_log_runtime(nodes: &[Node]) -> Result<LogRuntime> {
    if nodes.is_empty() {
        bail!("no nodes found to select Alloy log parser; inspect `kubectl get nodes -o json`");
    }
    let mut selected = None;
    let mut observed = Vec::new();
    for node in nodes.iter().filter(|node| alloy_can_schedule_on(node)) {
        let version = node.status.node_info.container_runtime_version.as_str();
        let runtime = if version.starts_with("containerd://") || version.starts_with("cri-o://") {
            Some(LogRuntime::Cri)
        } else if version.starts_with("docker://") {
            Some(LogRuntime::Docker)
        } else {
            None
        };
        observed.push(format!(
            "{}={}",
            node.metadata.name,
            if version.is_empty() {
                "<missing>"
            } else {
                version
            }
        ));
        match (selected, runtime) {
            (None, Some(runtime)) => selected = Some(runtime),
            (Some(previous), Some(runtime)) if previous == runtime => {}
            _ => bail!("Alloy needs one supported log format across every eligible node, including cordoned and NotReady nodes; found {}", observed.join(", ")),
        }
    }
    selected.ok_or_else(|| {
        anyhow!(
            "no nodes eligible for the Alloy DaemonSet have a supported runtime; inspect node taints and runtimes"
        )
    })
}

async fn preflight_log_runtime() -> Result<LogRuntime> {
    crate::ops::require_on_path("kubectl")?;
    let nodes: KubeList<Node> = read_kubernetes_json(
        &["get", "nodes", "-o", "json"],
        "kubectl get nodes -o json",
        "node container runtimes",
    )
    .await?;
    select_log_runtime(&nodes.items)
}

fn render_alloy_values(contents: &[u8], runtime: LogRuntime) -> Result<Vec<u8>> {
    if runtime == LogRuntime::Cri {
        return Ok(contents.to_vec());
    }
    let text = std::str::from_utf8(contents).context("Alloy values are not UTF-8")?;
    if text.matches("dockercontainers: false").count() != 1
        || text.matches("stage.cri { }").count() != 1
    {
        bail!("embedded Alloy values no longer match the supported CRI template");
    }
    Ok(text
        .replace("dockercontainers: false", "dockercontainers: true")
        .replace("stage.cri { }", "stage.docker { }")
        .into_bytes())
}

#[derive(Deserialize)]
struct NodeCondition {
    #[serde(rename = "type")]
    kind: String,
    status: String,
}

#[derive(Deserialize)]
struct Pod {
    metadata: ObjectMeta,
    spec: PodSpec,
    status: PodStatus,
}

#[derive(Deserialize)]
struct PodStatus {
    phase: String,
}

#[derive(Default, Deserialize)]
struct PodSpec {
    #[serde(rename = "nodeName")]
    node_name: Option<String>,
    containers: Vec<Container>,
    #[serde(rename = "initContainers", default)]
    init_containers: Vec<Container>,
    #[serde(default)]
    resources: ResourceRequirements,
    #[serde(default)]
    overhead: BTreeMap<String, String>,
}

#[derive(Deserialize)]
struct Container {
    name: String,
    #[serde(default)]
    resources: ResourceRequirements,
    #[serde(rename = "restartPolicy")]
    restart_policy: Option<String>,
}

#[derive(Default, Deserialize)]
struct ResourceRequirements {
    #[serde(default)]
    requests: BTreeMap<String, String>,
}

async fn preflight_capacity(observability_namespace: &str) -> Result<()> {
    crate::ops::require_on_path("kubectl")?;
    let node_command = "kubectl get nodes -o json";
    let nodes: KubeList<Node> = read_kubernetes_json(
        &["get", "nodes", "-o", "json"],
        node_command,
        "node allocatable memory",
    )
    .await?;

    let mut ready_nodes = BTreeMap::new();
    for node in nodes.items {
        let ready = node
            .status
            .conditions
            .iter()
            .any(|condition| condition.kind == "Ready" && condition.status == "True");
        if !ready || node.spec.unschedulable {
            continue;
        }
        let memory = node.status.allocatable.get("memory").ok_or_else(|| {
            anyhow!(
                "Ready node {} has no status.allocatable.memory; inspect with `{node_command}`",
                node.metadata.name
            )
        })?;
        ready_nodes.insert(
            node.metadata.name,
            parse_memory_quantity(memory)
                .with_context(|| format!("parsing allocatable memory from `{node_command}`"))?,
        );
    }
    if ready_nodes.is_empty() {
        bail!(
            "no Ready schedulable nodes expose allocatable memory; inspect the cluster prerequisite with `{node_command}`"
        );
    }

    let pod_command = "kubectl get pods --all-namespaces -o json";
    let pods: KubeList<Pod> = read_kubernetes_json(
        &["get", "pods", "--all-namespaces", "-o", "json"],
        pod_command,
        "scheduled pod memory requests",
    )
    .await?;
    let ready_names = ready_nodes.keys().cloned().collect::<BTreeSet<_>>();
    let mut scheduled_requests = 0u128;
    for pod in pods.items {
        if matches!(pod.status.phase.as_str(), "Succeeded" | "Failed") {
            continue;
        }
        if is_managed_observability_pod(&pod, observability_namespace) {
            continue;
        }
        let Some(node_name) = pod.spec.node_name.as_deref() else {
            continue;
        };
        if !ready_names.contains(node_name) {
            continue;
        }
        scheduled_requests = scheduled_requests
            .checked_add(effective_pod_memory_request(&pod)?)
            .ok_or_else(|| anyhow!("scheduled pod memory request total overflowed"))?;
    }

    let allocatable = ready_nodes.values().try_fold(0u128, |total, memory| {
        total
            .checked_add(*memory)
            .ok_or_else(|| anyhow!("Ready node allocatable memory total overflowed"))
    })?;
    let available = allocatable.saturating_sub(scheduled_requests);
    let required_memory_mib = FIXED_MEMORY_MIB
        .checked_add(
            PER_READY_NODE_MEMORY_MIB
                .checked_mul(ready_nodes.len() as u128)
                .ok_or_else(|| anyhow!("Ready node memory requirement overflowed"))?,
        )
        .ok_or_else(|| anyhow!("observability memory requirement overflowed"))?;
    let required_memory_bytes = required_memory_mib
        .checked_mul(MIB)
        .ok_or_else(|| anyhow!("observability memory byte requirement overflowed"))?;
    if available < required_memory_bytes {
        bail!(
            "curie example sre-bot install --observability has insufficient schedulable memory: required {required_memory_mib}Mi, available {}Mi; reduce scheduled pod requests or add Ready node memory, then rerun this command",
            available / MIB
        );
    }
    Ok(())
}

fn is_managed_observability_pod(pod: &Pod, observability_namespace: &str) -> bool {
    if pod.metadata.namespace != observability_namespace {
        return false;
    }
    let labels = &pod.metadata.labels;
    labels
        .get("app.kubernetes.io/instance")
        .is_some_and(|instance| MANAGED_HELM_RELEASES.contains(&instance.as_str()))
        || labels
            .get("app.kubernetes.io/name")
            .is_some_and(|name| name == "tempo")
}

async fn read_kubernetes_json<T: for<'de> Deserialize<'de>>(
    args: &[&str],
    display: &str,
    purpose: &str,
) -> Result<T> {
    let (ok, stdout, stderr) =
        crate::ops::run_capture(&ops_command("kubectl", args.iter().copied()))
            .await
            .with_context(|| format!("failed to invoke `{display}`"))?;
    if !ok {
        let stderr = stderr.trim().to_string();
        bail!(
            "could not read {purpose} with `{display}`: {}",
            if stderr.is_empty() {
                "kubectl exited nonzero"
            } else {
                &stderr
            }
        );
    }
    serde_json::from_str(&stdout)
        .with_context(|| format!("malformed JSON from `{display}` while reading {purpose}"))
}

fn effective_pod_memory_request(pod: &Pod) -> Result<u128> {
    let mut application = 0u128;
    for container in &pod.spec.containers {
        application = checked_add(
            application,
            resource_memory(&container.resources, &pod.metadata.name, &container.name)?,
        )?;
    }

    let mut restartable = 0u128;
    let mut max_init_stage = 0u128;
    for container in &pod.spec.init_containers {
        let request = resource_memory(&container.resources, &pod.metadata.name, &container.name)?;
        let stage = if container.restart_policy.as_deref() == Some("Always") {
            restartable = checked_add(restartable, request)?;
            restartable
        } else {
            checked_add(restartable, request)?
        };
        max_init_stage = max_init_stage.max(stage);
    }

    let steady_state = checked_add(application, restartable)?;
    let container_request = steady_state.max(max_init_stage);
    let pod_level = resource_memory(&pod.spec.resources, &pod.metadata.name, "pod")?;
    let overhead = optional_memory(&pod.spec.overhead, &pod.metadata.name, "pod overhead")?;
    checked_add(container_request.max(pod_level), overhead)
}

fn resource_memory(resources: &ResourceRequirements, pod: &str, container: &str) -> Result<u128> {
    optional_memory(&resources.requests, pod, container)
}

fn optional_memory(requests: &BTreeMap<String, String>, pod: &str, owner: &str) -> Result<u128> {
    requests.get("memory").map_or(Ok(0), |quantity| {
        parse_memory_quantity(quantity)
            .with_context(|| format!("invalid memory request for {pod}/{owner}"))
    })
}

fn checked_add(left: u128, right: u128) -> Result<u128> {
    left.checked_add(right)
        .ok_or_else(|| anyhow!("pod memory request overflowed"))
}

fn parse_memory_quantity(quantity: &str) -> Result<u128> {
    let quantity = quantity.trim();
    let (number, multiplier) = [
        ("Ei", 1024f64.powi(6)),
        ("Pi", 1024f64.powi(5)),
        ("Ti", 1024f64.powi(4)),
        ("Gi", 1024f64.powi(3)),
        ("Mi", 1024f64.powi(2)),
        ("Ki", 1024f64),
        ("E", 1000f64.powi(6)),
        ("P", 1000f64.powi(5)),
        ("T", 1000f64.powi(4)),
        ("G", 1000f64.powi(3)),
        ("M", 1000f64.powi(2)),
        ("K", 1000f64),
        ("k", 1000f64),
        ("m", 0.001f64),
        ("u", 0.000_001f64),
        ("n", 0.000_000_001f64),
    ]
    .into_iter()
    .find_map(|(suffix, multiplier)| {
        quantity
            .strip_suffix(suffix)
            .map(|number| (number, multiplier))
    })
    .unwrap_or((quantity, 1f64));
    let number = number
        .parse::<f64>()
        .with_context(|| format!("unsupported Kubernetes memory quantity {quantity:?}"))?;
    let bytes = number * multiplier;
    if !bytes.is_finite() || bytes < 0.0 || bytes > u128::MAX as f64 {
        bail!("unsupported Kubernetes memory quantity {quantity:?}");
    }
    Ok(bytes.ceil() as u128)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn external_provisioner_name_reads_both_quote_styles() {
        assert_eq!(
            external_provisioner_name(
                "Waiting for a volume to be created either by the external provisioner 'csi.example.com' or manually by the system administrator."
            ),
            Some("csi.example.com")
        );
        assert_eq!(
            external_provisioner_name(
                "waiting for a volume to be created, either by external provisioner \"csi.example.com\" or manually created by system administrator"
            ),
            Some("csi.example.com")
        );
        for message in [
            "waiting for first consumer to be created before binding",
            "external provisioner csi.example.com",
            "external provisioner ''",
        ] {
            assert_eq!(external_provisioner_name(message), None, "{message}");
        }
    }

    /// The helm render gate templates the Docker variant of the Alloy values
    /// from this file instead of re-implementing `render_alloy_values`.
    /// Regenerate it with `CURIE_TEST_UPDATE_ALLOY_FIXTURE=1 cargo test
    /// alloy_docker_variant_matches_ci_fixture` after changing the template.
    #[test]
    fn alloy_docker_variant_matches_ci_fixture() {
        let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../charts/curie/ci/fixtures/alloy-docker-values.yaml");
        let template = OBSERVABILITY_FILES
            .iter()
            .find(|(name, _)| *name == "alloy-values.yaml")
            .map(|(_, contents)| *contents)
            .expect("embedded Alloy values");
        // The same two steps `write_observability_files` takes for the
        // default namespace the gate renders into.
        let template = rewrite_observability_namespace(template, OBSERVABILITY_NAMESPACE);
        let rendered = render_alloy_values(&template, LogRuntime::Docker).unwrap();
        if std::env::var("CURIE_TEST_UPDATE_ALLOY_FIXTURE").as_deref() == Ok("1") {
            std::fs::create_dir_all(fixture.parent().expect("fixture directory"))
                .expect("create Alloy CI fixture directory");
            std::fs::write(&fixture, &rendered).expect("write Alloy CI fixture");
        }
        let committed = std::fs::read(&fixture).unwrap_or_else(|error| {
            panic!(
                "{} is unreadable ({error}); regenerate it with \
                 CURIE_TEST_UPDATE_ALLOY_FIXTURE=1",
                fixture.display()
            )
        });
        assert!(
            committed == rendered,
            "{} drifted from render_alloy_values; regenerate it with \
             CURIE_TEST_UPDATE_ALLOY_FIXTURE=1",
            fixture.display()
        );
    }

    #[test]
    fn log_runtime_classifies_supported_node_versions() {
        // Node.status.nodeInfo.containerRuntimeVersion is a runtime://version
        // string; DaemonSet pods can tolerate cordoned and NotReady nodes.
        // https://kubernetes.io/docs/reference/kubernetes-api/core/node-v1/
        // https://kubernetes.io/docs/concepts/workloads/controllers/daemonset/
        for (version, expected) in [
            ("containerd://1.7.0", LogRuntime::Cri),
            ("cri-o://1.30.0", LogRuntime::Cri),
            ("docker://24.0.0", LogRuntime::Docker),
        ] {
            let nodes: KubeList<Node> = serde_json::from_value(serde_json::json!({
                "items": [{
                    "metadata": {"name": "node-a"},
                    "status": {
                        "allocatable": {"memory": "4Gi"},
                        "conditions": [{"type": "Ready", "status": "True"}],
                        "nodeInfo": {"containerRuntimeVersion": version}
                    }
                }]
            }))
            .unwrap();
            assert_eq!(select_log_runtime(&nodes.items).unwrap(), expected);
        }
    }

    #[test]
    fn log_runtime_refuses_missing_or_unknown_node_version() {
        for version in ["", "mystery://1"] {
            let nodes: KubeList<Node> = serde_json::from_value(serde_json::json!({
                "items": [{
                    "metadata": {"name": "node-a"},
                    "status": {
                        "allocatable": {"memory": "4Gi"},
                        "conditions": [{"type": "Ready", "status": "True"}],
                        "nodeInfo": {"containerRuntimeVersion": version}
                    }
                }]
            }))
            .unwrap();
            let error = select_log_runtime(&nodes.items).unwrap_err().to_string();
            assert!(error.contains("node-a"), "{error}");
        }
    }

    #[test]
    fn log_runtime_ignores_nodes_excluded_by_untolerated_taints() {
        // The shipped Alloy DaemonSet has no custom tolerations. Kubernetes
        // adds only the documented built-in DaemonSet tolerations.
        // https://kubernetes.io/docs/concepts/workloads/controllers/daemonset/
        let nodes: KubeList<Node> = serde_json::from_value(serde_json::json!({
            "items": [
                {
                    "metadata": {"name": "worker"},
                    "status": {"allocatable": {}, "conditions": [],
                        "nodeInfo": {"containerRuntimeVersion": "containerd://1.7"}}
                },
                {
                    "metadata": {"name": "reserved-docker"},
                    "spec": {"taints": [{"key": "dedicated", "effect": "NoSchedule", "value": "other"}]},
                    "status": {"allocatable": {}, "conditions": [],
                        "nodeInfo": {"containerRuntimeVersion": "docker://24"}}
                },
                {
                    "metadata": {"name": "cordoned-cri"},
                    "spec": {"taints": [{"key": "node.kubernetes.io/unschedulable", "effect": "NoSchedule"}]},
                    "status": {"allocatable": {}, "conditions": [],
                        "nodeInfo": {"containerRuntimeVersion": "cri-o://1.30"}}
                }
            ]
        }))
        .unwrap();
        assert_eq!(select_log_runtime(&nodes.items).unwrap(), LogRuntime::Cri);
    }

    #[test]
    fn log_runtime_counts_nodes_behind_transient_condition_taints() {
        // The node lifecycle controller and the cloud controller add these
        // taints while a node is unhealthy or starting, then remove them. A
        // DaemonSet pod lands there afterwards, so the node's runtime counts.
        // https://kubernetes.io/docs/reference/labels-annotations-taints/
        for key in [
            "node.kubernetes.io/not-ready",
            "node.kubernetes.io/unreachable",
            "node.kubernetes.io/network-unavailable",
            "node.cloudprovider.kubernetes.io/uninitialized",
        ] {
            let nodes: KubeList<Node> = serde_json::from_value(serde_json::json!({
                "items": [
                    {
                        "metadata": {"name": "worker"},
                        "status": {"allocatable": {}, "conditions": [],
                            "nodeInfo": {"containerRuntimeVersion": "containerd://1.7"}}
                    },
                    {
                        "metadata": {"name": "recovering-docker"},
                        "spec": {"taints": [
                            {"key": key, "effect": "NoSchedule"},
                            {"key": key, "effect": "NoExecute"}
                        ]},
                        "status": {"allocatable": {}, "conditions": [],
                            "nodeInfo": {"containerRuntimeVersion": "docker://24"}}
                    }
                ]
            }))
            .unwrap();
            let error = select_log_runtime(&nodes.items).unwrap_err().to_string();
            assert!(error.contains("recovering-docker"), "{key}: {error}");
        }
    }

    #[test]
    fn log_runtime_refuses_when_no_nodes_are_eligible_for_alloy() {
        let nodes: KubeList<Node> = serde_json::from_value(serde_json::json!({
            "items": [{
                "metadata": {"name": "reserved"},
                "spec": {"taints": [{"key": "dedicated", "effect": "NoExecute"}]},
                "status": {"allocatable": {}, "conditions": [],
                    "nodeInfo": {"containerRuntimeVersion": "docker://24"}}
            }]
        }))
        .unwrap();
        assert!(select_log_runtime(&nodes.items)
            .unwrap_err()
            .to_string()
            .contains("no nodes eligible"));
    }

    fn sre_route(channel: &str, users: &[&str]) -> crate::api::ApprovalRouteBindingResponse {
        serde_json::from_value(serde_json::json!({
            "resolution": {"kind": "slack", "address": channel},
            "approvers": {"users": users},
        }))
        .unwrap()
    }

    fn approvers(ids: &[&str]) -> Vec<String> {
        ids.iter().map(|id| id.to_string()).collect()
    }

    #[test]
    fn sre_approvals_route_map_binds_users_when_approvers_given() {
        let map = sre_approvals_route_map(
            None,
            "C0EXAMPLE1",
            &approvers(&["U0EXAMPLE1", "U0EXAMPLE2"]),
        );
        assert_eq!(
            serde_json::to_value(&map).unwrap(),
            serde_json::json!({"sre-approvals": {
                "resolution": {"kind": "slack", "address": "C0EXAMPLE1"},
                "approvers": {"users": ["U0EXAMPLE1", "U0EXAMPLE2"]},
            }})
        );
    }

    #[test]
    fn sre_approvals_route_map_preserves_other_bound_routes() {
        let mut existing = std::collections::BTreeMap::new();
        existing.insert(
            "deploys".to_string(),
            sre_route("C0EXAMPLE2", &["U0EXAMPLE3"]),
        );
        let map =
            sre_approvals_route_map(Some(&existing), "C0EXAMPLE1", &approvers(&["U0EXAMPLE1"]));
        let value = serde_json::to_value(&map).unwrap();
        assert_eq!(
            value["deploys"],
            serde_json::json!({
                "resolution": {"kind": "slack", "address": "C0EXAMPLE2"},
                "approvers": {"users": ["U0EXAMPLE3"]},
            })
        );
        assert_eq!(
            value["sre-approvals"]["approvers"],
            serde_json::json!({"users": ["U0EXAMPLE1"]})
        );
        assert_eq!(value.as_object().unwrap().len(), 2);
    }

    #[test]
    fn sre_approvals_route_map_moves_an_existing_binding_and_replaces_approvers() {
        let mut existing = std::collections::BTreeMap::new();
        existing.insert(
            "sre-approvals".to_string(),
            sre_route("C0EXAMPLE1", &["U0EXAMPLE1", "U0EXAMPLE2"]),
        );

        let replaced =
            sre_approvals_route_map(Some(&existing), "C0EXAMPLE2", &approvers(&["U0EXAMPLE2"]));
        assert_eq!(
            serde_json::to_value(&replaced).unwrap(),
            serde_json::json!({"sre-approvals": {
                "resolution": {"kind": "slack", "address": "C0EXAMPLE2"},
                "approvers": {"users": ["U0EXAMPLE2"]},
            }})
        );
    }

    #[test]
    fn sre_approvals_route_map_matches_current_when_binding_is_identical() {
        let mut existing = std::collections::BTreeMap::new();
        existing.insert(
            "sre-approvals".to_string(),
            sre_route("C0EXAMPLE2", &["U0EXAMPLE2"]),
        );
        let desired =
            sre_approvals_route_map(Some(&existing), "C0EXAMPLE2", &approvers(&["U0EXAMPLE2"]));
        let current = existing
            .iter()
            .map(|(name, binding)| (name.clone(), route_binding_as_write(binding)))
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(desired, current);
    }

    #[test]
    fn memory_quantities_cover_the_kubernetes_shapes_used_by_nodes_and_pods() {
        assert_eq!(parse_memory_quantity("1Gi").unwrap(), 1024 * 1024 * 1024);
        // 1409024Ki is the exact one-node required total (FIXED_MEMORY_MIB
        // 1216 + PER_READY_NODE_MEMORY_MIB 160 = 1376Mi) in the Ki form a node
        // reports allocatable memory in; it moved from 1312Mi under #2059.
        assert_eq!(parse_memory_quantity("1409024Ki").unwrap(), 1376 * MIB);
        assert_eq!(parse_memory_quantity("500M").unwrap(), 500_000_000);
        assert_eq!(parse_memory_quantity("1e6").unwrap(), 1_000_000);
    }

    #[test]
    fn the_upgrade_path_off_strips_the_connector_and_its_gates() {
        // The behaviour before this flag existed, pinned so the flag cannot
        // change what an install that did not ask for it receives.
        let connectors = runtime_connector_declaration(
            bundle_file("connectors.yaml"),
            "sha256:tempo",
            OBSERVABILITY_NAMESPACE,
            None,
        )
        .unwrap();
        let parsed: serde_json::Value = serde_norway::from_slice(&connectors).unwrap();
        assert!(parsed["connectors"].get("self-upgrade").is_none());

        let manifest =
            runtime_plugin_manifest(bundle_file(".claude-plugin/plugin.json"), false).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&manifest).unwrap();
        assert_eq!(routed_gates(&parsed), always_retained_gate_set());
        assert_eq!(
            parsed["approvalPolicy"]["gates"].as_array().unwrap().len(),
            7
        );
        let allow = parsed["toolPolicy"]["allow"].as_array().unwrap();
        assert!(!allow.iter().any(is_self_upgrade_policy_entry));
    }

    #[test]
    fn the_upgrade_path_on_fills_in_only_the_platform_cronjob_name() {
        // The whole reason this flag exists. The bundle ships
        // PLATFORM_UPGRADE_CRONJOB empty, and the worker's connector reconciler
        // puts that declaration back within the minute over anything set by
        // hand -- so if the installer does not render a real value, nothing can.
        let connectors = runtime_connector_declaration(
            bundle_file("connectors.yaml"),
            "sha256:tempo",
            OBSERVABILITY_NAMESPACE,
            Some("sha256:upgrade"),
        )
        .unwrap();
        let parsed: serde_json::Value = serde_norway::from_slice(&connectors).unwrap();
        let env = &parsed["connectors"]["self-upgrade"]["env"];
        assert_eq!(
            env[PLATFORM_UPGRADE_CRONJOB_ENV],
            PLATFORM_UPGRADE_CRONJOB_NAME
        );
        // #2288: the self-upgrade CronJob is never applied, so its name is
        // rendered empty and the connector refuses upgrade_self outright.
        assert_eq!(env[SELF_UPGRADE_CRONJOB_ENV], "");
        // `build:` records a LOCAL image id the cluster tier refuses, so a kept
        // connector without a resolved digest is one that can never start.
        assert!(parsed["connectors"]["self-upgrade"].get("build").is_none());
        assert_eq!(
            parsed["connectors"]["self-upgrade"]["image"],
            format!("{SELF_UPGRADE_IMAGE_REPOSITORY}@sha256:upgrade")
        );
    }

    #[test]
    fn a_kept_upgrade_connector_keeps_exactly_its_platform_gate() {
        // A gate naming a stripped connector fails validation for everyone; a
        // kept connector with no gate is an ungated write. Both are decided from
        // the same condition, so both are asserted here.
        let manifest =
            runtime_plugin_manifest(bundle_file(".claude-plugin/plugin.json"), true).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&manifest).unwrap();
        let gates: Vec<&str> = parsed["approvalPolicy"]["gates"]
            .as_array()
            .unwrap()
            .iter()
            .map(|gate| gate["gate"].as_str().unwrap())
            .collect();
        assert!(!gates.contains(&UPGRADE_GATE));
        assert!(gates.contains(&PLATFORM_UPGRADE_GATE));
        assert!(gates.contains(&PLATFORM_PUBLISH_GATE));
        let mut expected = always_retained_gate_set();
        expected.insert((
            PLATFORM_UPGRADE_GATE.to_string(),
            "sre-approvals".to_string(),
        ));
        assert_eq!(routed_gates(&parsed), expected);
        assert_eq!(gates.len(), 8);
        let allow = parsed["toolPolicy"]["allow"].as_array().unwrap();
        assert!(allow
            .iter()
            .any(|tool| tool.as_str() == Some(LATEST_RELEASE_TOOL)));
        assert!(!allow.iter().any(|tool| tool.as_str() == Some(UPGRADE_TOOL)));
        assert!(allow
            .iter()
            .any(|tool| tool.as_str() == Some(PLATFORM_UPGRADE_TOOL)));
    }

    #[test]
    fn every_armed_upgrade_gate_starts_a_cronjob_the_upgrade_path_applies() {
        // #2288: upgrade_self was armed while apply_upgrade_path never applied
        // its CronJob. Tie the armed gates, the connector env naming each
        // CronJob, and the applied file set together so they cannot drift.
        // Each self-upgrade gate: (env naming its CronJob, applied file, the
        // rendered object's name when that file is applied).
        let platform = render_platform_cronjob(
            PLATFORM_UPGRADE_CRONJOB_YAML,
            "curie",
            "curie",
            PLATFORM_UPGRADE_SOURCE_REPO,
        )
        .unwrap();
        let platform: serde_json::Value = serde_norway::from_slice(&platform).unwrap();
        let platform_name = platform["metadata"]["name"].as_str().unwrap().to_string();
        let targets = [
            (
                UPGRADE_GATE,
                SELF_UPGRADE_CRONJOB_ENV,
                "manifests/self-upgrade-cronjob.yaml",
                None,
            ),
            (
                PLATFORM_UPGRADE_GATE,
                PLATFORM_UPGRADE_CRONJOB_ENV,
                "manifests/platform-upgrade-cronjob.yaml",
                Some(platform_name),
            ),
        ];

        let manifest =
            runtime_plugin_manifest(bundle_file(".claude-plugin/plugin.json"), true).unwrap();
        let manifest: serde_json::Value = serde_json::from_slice(&manifest).unwrap();
        let armed: BTreeSet<&str> = manifest["approvalPolicy"]["gates"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|gate| gate["gate"].as_str())
            .filter(|gate| gate.starts_with("mcp__self-upgrade__"))
            .collect();
        let allow = manifest["toolPolicy"]["allow"].as_array().unwrap();
        let connectors = runtime_connector_declaration(
            bundle_file("connectors.yaml"),
            "sha256:tempo",
            OBSERVABILITY_NAMESPACE,
            Some("sha256:upgrade"),
        )
        .unwrap();
        let connectors: serde_json::Value = serde_norway::from_slice(&connectors).unwrap();
        let env = &connectors["connectors"]["self-upgrade"]["env"];

        for (gate, env_key, file, rendered_name) in &targets {
            let applied = UPGRADE_PATH_FILES.contains(file);
            let tool = gate.replacen("mcp__self-upgrade__", "self-upgrade/", 1);
            let allowed = allow.iter().any(|entry| entry.as_str() == Some(&tool));
            assert_eq!(
                armed.contains(gate),
                applied,
                "{gate} armed vs {file} applied"
            );
            assert_eq!(allowed, applied, "{tool} allowed vs {file} applied");
            match applied {
                true => assert_eq!(
                    env[*env_key].as_str(),
                    rendered_name.as_deref(),
                    "{env_key} must name the applied CronJob"
                ),
                false => assert_eq!(env[*env_key], "", "{env_key} must be empty"),
            }
        }
        let known: BTreeSet<&str> = targets.iter().map(|target| target.0).collect();
        assert!(
            armed.is_subset(&known),
            "unclassified upgrade gate: {armed:?}"
        );
    }

    fn test_identity() -> InstallIdentity {
        InstallIdentity {
            namespace: "curie".to_string(),
            release: "curie".to_string(),
            observability_namespace: "observability".to_string(),
        }
    }

    #[test]
    fn an_exported_credential_is_declared_with_its_provider_egress() {
        // #2920: CURIE_CREDENTIALS was set and the installer still declared no
        // credential, so the release came up on the fake model.
        let model = ModelCredential::from_value(Some("sk-or-EXAMPLE"));
        let installation = platform_installation(&test_identity(), &[], &model);
        assert_eq!(
            installation.credentials.model.as_deref(),
            Some("CURIE_CREDENTIALS")
        );
        assert_eq!(installation.egress_hosts(), vec!["openrouter"]);
    }

    #[test]
    fn a_credential_with_no_known_prefix_is_declared_without_guessing_egress() {
        let model = ModelCredential::from_value(Some("zhipu-EXAMPLE"));
        let installation = platform_installation(&test_identity(), &[], &model);
        assert_eq!(
            installation.credentials.model.as_deref(),
            Some("CURIE_CREDENTIALS")
        );
        assert!(installation.egress_hosts().is_empty());
    }

    #[test]
    fn no_credential_keeps_the_fake_model_install() {
        for value in [None, Some(""), Some("  ")] {
            let model = ModelCredential::from_value(value);
            assert!(model.declared.is_none());
            let installation = platform_installation(&test_identity(), &[], &model);
            assert_eq!(installation.credentials.model, None);
            assert!(installation.egress_hosts().is_empty());
        }
    }

    #[test]
    fn a_rerun_keeps_recorded_egress_and_adds_the_provider_route() {
        let existing = serde_json::json!({"security": {"networkPolicy": {"allowedEgress": [
            {"cidr": "203.0.113.7/32", "ports": [{"protocol": "TCP", "port": 5432}]}
        ]}}, "api": {"logLevel": "info"}});
        let recorded = recorded_runner_egress(&existing);
        assert_eq!(recorded.len(), 3, "{recorded:?}");
        let sets = carried_runner_egress_sets(
            recorded,
            &["198.51.100.9/32".to_string(), "203.0.113.7/32".to_string()],
        );
        let key = |k: &str| {
            sets.get(&format!("{RUNNER_EGRESS_KEY}{k}"))
                .map(String::as_str)
        };
        assert_eq!(key("[0].cidr"), Some("203.0.113.7/32"));
        assert_eq!(key("[0].ports[0].port"), Some("5432"));
        assert_eq!(key("[1].cidr"), Some("198.51.100.9/32"));
        assert_eq!(key("[1].ports[0].port"), Some("443"));
        assert_eq!(key("[1].ports[0].protocol"), Some("TCP"));
        // Same address as the recorded 5432 entry, still gets its own 443 rule.
        assert_eq!(key("[2].cidr"), Some("203.0.113.7/32"));
        assert_eq!(key("[2].ports[0].port"), Some("443"));

        let mut model = ModelCredential::from_value(Some("sk-or-EXAMPLE"));
        model.egress_sets = sets;
        model.egress.clear();
        let installation = platform_installation(&test_identity(), &[], &model);
        assert!(installation.egress_hosts().is_empty());
        assert!(installation.set.is_empty(), "never through --set-string");
        assert!(model
            .typed_egress_sets()
            .contains(&format!("{RUNNER_EGRESS_KEY}[1].ports[0].port=443")));
    }

    #[test]
    fn a_release_without_runner_egress_records_none() {
        for existing in [
            serde_json::json!({}),
            serde_json::json!({"security": {"networkPolicy": {"allowedEgress": []}}}),
        ] {
            assert!(recorded_runner_egress(&existing).is_empty(), "{existing}");
        }
    }

    #[test]
    fn a_release_recording_a_model_credential_is_refused() {
        // The shape `helm get values -o json` returns for an install that has
        // one. This is the case that cost a credential.
        let existing = serde_json::json!({
            "agentSandbox": {"runner": {"credentials": "sk-ant-EXAMPLE", "fakeModel": false}}
        });
        assert!(records_a_model_credential(&existing));
    }

    #[test]
    fn a_release_without_one_is_not_refused() {
        for existing in [
            serde_json::json!({}),
            serde_json::json!({"agentSandbox": {}}),
            serde_json::json!({"agentSandbox": {"runner": {}}}),
            // Empty is not "present": an operator who cleared it did so
            // deliberately, and refusing would block them from re-running.
            serde_json::json!({"agentSandbox": {"runner": {"credentials": ""}}}),
        ] {
            assert!(!records_a_model_credential(&existing), "{existing}");
        }
    }

    fn platform_role_source() -> &'static [u8] {
        bundle_file("manifests/platform-upgrade-role.yaml")
    }

    #[test]
    fn the_platform_role_lands_in_the_release_namespace_with_no_static_token() {
        let rendered = render_platform_upgrade_role(platform_role_source(), "curie-prod").unwrap();
        let text = String::from_utf8(rendered).unwrap();
        assert!(text.contains("namespace: curie-prod"));
        assert!(!text.contains("namespace: curie\n"));
        // The reader and writer identities ship a static token Secret; this one
        // must not. A namespace-admin-equivalent token that outlives its Job is
        // the thing this shape exists to avoid.
        assert!(
            !text.contains("service-account-token"),
            "the platform upgrade identity must not get a static token: {text}"
        );
    }

    #[test]
    fn a_widened_platform_role_stops_the_install() {
        // The manifest is edited far more often than this file. A rule appended
        // there must fail here rather than be granted silently -- this is the
        // widest credential the bundle creates.
        let source = String::from_utf8(platform_role_source().to_vec()).unwrap();
        let widened = source.replace(
            "  - apiGroups: [\"\"]\n    resources: [\"pods\", \"events\"]\n    verbs: [\"get\", \"list\", \"watch\"]",
            "  - apiGroups: [\"\"]\n    resources: [\"pods\", \"events\"]\n    verbs: [\"get\", \"list\", \"watch\"]\n  \
             - apiGroups: [\"*\"]\n    resources: [\"*\"]\n    verbs: [\"*\"]",
        );
        assert_ne!(
            widened, source,
            "the fixture no longer matches the manifest"
        );
        let error = render_platform_upgrade_role(widened.as_bytes(), "curie-prod").unwrap_err();
        assert!(
            error.to_string().contains("rules"),
            "a widened Role must be refused by name: {error}"
        );
    }

    #[test]
    fn a_verb_widened_platform_role_stops_the_install() {
        // Count and apiGroups/resources stay put. The existing appended-rule
        // test never reaches the per-rule shape check, and that check used to
        // ignore verbs, so `["*"]` on one rule installed as a
        // namespace-admin-equivalent grant (#2287).
        let source = String::from_utf8(platform_role_source().to_vec()).unwrap();
        let widened = source.replace(
            "    resources: [\"secrets\", \"configmaps\", \"services\", \"serviceaccounts\", \"persistentvolumeclaims\"]\n    verbs: [\"get\", \"list\", \"watch\", \"create\", \"update\", \"patch\", \"delete\"]",
            "    resources: [\"secrets\", \"configmaps\", \"services\", \"serviceaccounts\", \"persistentvolumeclaims\"]\n    verbs: [\"*\"]",
        );
        assert_ne!(
            widened, source,
            "the fixture no longer matches the manifest"
        );
        assert_eq!(
            widened.matches("verbs: [\"*\"]").count(),
            1,
            "the mutation must change only one rule's verbs: {widened}"
        );
        let resource_widened = source.replace(
            "resources: [\"pods\", \"events\"]",
            "resources: [\"pods\", \"events\", \"namespaces\"]",
        );
        assert_ne!(resource_widened, source);
        let resource_error =
            render_platform_upgrade_role(resource_widened.as_bytes(), "curie-prod")
                .unwrap_err()
                .to_string();
        let error = render_platform_upgrade_role(widened.as_bytes(), "curie-prod")
            .unwrap_err()
            .to_string();
        assert!(
            resource_error.contains("only knows how to render"),
            "resource widening is the message class this test pins: {resource_error}"
        );
        assert!(
            error.contains("only knows how to render"),
            "a verb-widened Role must be refused by the same shape check as a resource widening: {error}"
        );
        assert!(
            error.contains("\"*\""),
            "the refusal must name the widened verbs: {error}"
        );
    }

    #[test]
    fn the_platform_upgrade_plan_line_is_derived_from_the_asserted_rules() {
        let line = platform_upgrade_role_plan_line(platform_role_source(), "curie-prod").unwrap();
        assert!(line.contains("curie-prod"), "{line}");
        assert!(line.contains(PLATFORM_UPGRADER_IDENTITY), "{line}");
        assert!(
            line.contains(
                r#"["secrets", "configmaps", "services", "serviceaccounts", "persistentvolumeclaims"]"#
            ),
            "the plan must name the resources this build would apply: {line}"
        );
        assert!(
            line.contains(r#"["get", "list", "watch", "create", "update", "patch", "delete"]"#),
            "the plan must name the verbs this build would apply: {line}"
        );
        assert!(
            line.contains(r#"["pods", "events"]"#) && line.contains(r#"["get", "list", "watch"]"#),
            "the plan must include the read-only pods/events rule: {line}"
        );
        assert!(
            !line.contains("namespace-admin"),
            "a hand-written characterization would describe a grant the YAML no longer has: {line}"
        );
        let source = String::from_utf8(platform_role_source().to_vec()).unwrap();
        let widened = source.replace(
            "    resources: [\"secrets\", \"configmaps\", \"services\", \"serviceaccounts\", \"persistentvolumeclaims\"]\n    verbs: [\"get\", \"list\", \"watch\", \"create\", \"update\", \"patch\", \"delete\"]",
            "    resources: [\"secrets\", \"configmaps\", \"services\", \"serviceaccounts\", \"persistentvolumeclaims\"]\n    verbs: [\"*\"]",
        );
        let error = platform_upgrade_role_plan_line(widened.as_bytes(), "curie-prod")
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("only knows how to render"),
            "a verb-widened Role must not produce a plan line describing the unmodified grant: {error}"
        );
    }

    #[test]
    fn the_platform_cronjob_is_pointed_at_this_install() {
        let rendered = render_platform_cronjob(
            PLATFORM_UPGRADE_CRONJOB_YAML,
            "curie-prod",
            "curie-prod-release",
            "acme/widget",
        )
        .unwrap();
        let text = String::from_utf8(rendered).unwrap();
        assert!(text.contains("namespace: curie-prod"));
        assert!(text.contains("curie-prod-release"));
        assert!(text.contains("acme/widget"));
        // The placeholders the shipped file carries must all be gone; one left
        // behind is a tool that refuses every call with nothing visibly wrong.
        assert!(
            !text.contains("curie-eng/curie"),
            "the repository placeholder survived: {text}"
        );
    }

    #[test]
    fn the_rendered_cronjob_is_suspended() {
        // The installer's contract: an upgrade happens when a human approves
        // one, never on a timer nobody chose.
        let rendered = render_platform_cronjob(
            PLATFORM_UPGRADE_CRONJOB_YAML,
            "curie-prod",
            "release",
            "acme/widget",
        )
        .unwrap();
        let text = String::from_utf8(rendered).unwrap();
        assert!(text.contains("suspend: true"), "{text}");
    }

    #[test]
    fn a_renamed_cronjob_stops_the_install() {
        // The connector is told this name through its own env. Two places free
        // to disagree is how a verb ends up refusing every call.
        let source = String::from_utf8(PLATFORM_UPGRADE_CRONJOB_YAML.to_vec()).unwrap();
        let renamed = source.replace("name: platform-upgrade", "name: something-else");
        assert_ne!(
            renamed, source,
            "the fixture no longer matches the manifest"
        );
        let error =
            render_platform_cronjob(renamed.as_bytes(), "curie-prod", "release", "acme/widget")
                .unwrap_err();
        assert!(error.to_string().contains("platform-upgrade"), "{error}");
    }

    fn bundle_file(name: &str) -> &'static [u8] {
        BUNDLE_FILES
            .iter()
            .find(|(candidate, _)| *candidate == name)
            .map(|(_, contents)| *contents)
            .unwrap_or_else(|| panic!("embedded bundle has no {name}"))
    }

    #[test]
    fn kubernetes_connector_survives_runtime_render_unchanged() {
        let rendered = runtime_connector_declaration(
            bundle_file("connectors.yaml"),
            "sha256:tempo",
            OBSERVABILITY_NAMESPACE,
            None,
        )
        .expect("connector declaration renders");
        let source: serde_json::Value =
            serde_norway::from_slice(bundle_file("connectors.yaml")).unwrap();
        let runtime: serde_json::Value = serde_norway::from_slice(&rendered).unwrap();
        assert_eq!(
            runtime["connectors"]["kubernetes"],
            source["connectors"]["kubernetes"],
            "the installer must preserve the pinned upstream image, core-only flags, and kubeconfig mount"
        );
        assert!(runtime["connectors"].get("k8s-write").is_none());
        assert!(runtime["connectors"].get("k8s-scale").is_none());
    }

    #[test]
    fn kubernetes_access_rewrites_only_the_curie_identity_namespace() {
        let rendered = render_read_access(
            bundle_file("manifests/kubernetes-access.yaml"),
            "curie-prod",
        )
        .expect("Kubernetes access manifest renders");
        let text = String::from_utf8(rendered).unwrap();
        assert!(
            text.contains("namespace: curie-prod"),
            "the connector identity must follow the selected release namespace: {text}"
        );
        assert!(
            text.contains("namespace: sre-demo"),
            "the disposable workload ceiling must remain in sre-demo: {text}"
        );
        assert!(
            !text.contains("namespace: curie\n"),
            "the shipped release namespace must be fully rewritten: {text}"
        );
    }

    #[test]
    fn upgrade_disabled_removes_only_self_upgrade_policy_entries() {
        let manifest =
            runtime_plugin_manifest(bundle_file(".claude-plugin/plugin.json"), false).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&manifest).unwrap();
        assert_eq!(routed_gates(&parsed), always_retained_gate_set());
        assert_eq!(
            parsed["approvalPolicy"]["gates"].as_array().unwrap().len(),
            7
        );
        let source: serde_json::Value =
            serde_json::from_slice(bundle_file(".claude-plugin/plugin.json")).unwrap();
        assert_eq!(
            parsed["toolPolicy"]["approvalRequired"],
            source["toolPolicy"]["approvalRequired"]
        );
        assert_eq!(parsed["toolPolicy"]["deny"], source["toolPolicy"]["deny"]);
        let allow = parsed["toolPolicy"]["allow"].as_array().unwrap();
        let source_allow = source["toolPolicy"]["allow"].as_array().unwrap();
        let stripped = source_allow
            .iter()
            .filter(|tool| is_self_upgrade_policy_entry(tool))
            .count();
        assert_eq!(allow.len(), source_allow.len() - stripped);
        assert!(!allow.iter().any(is_self_upgrade_policy_entry));
        assert!(allow
            .iter()
            .any(|tool| tool.as_str() == Some("kubernetes/pods_list")));
        assert!(allow
            .iter()
            .any(|tool| tool.as_str() == Some("grafana/query_loki_logs")));
    }

    const KUBERNETES_MUTATIONS: [&str; 6] = [
        "pods_delete",
        "pods_exec",
        "pods_run",
        "resources_create_or_update",
        "resources_delete",
        "resources_scale",
    ];

    fn kubernetes_gate_set() -> std::collections::BTreeSet<(String, String)> {
        KUBERNETES_MUTATIONS
            .iter()
            .map(|tool| {
                (
                    format!("mcp__kubernetes__{tool}"),
                    "sre-approvals".to_string(),
                )
            })
            .collect()
    }

    fn always_retained_gate_set() -> std::collections::BTreeSet<(String, String)> {
        let mut gates = kubernetes_gate_set();
        gates.insert((
            PLATFORM_PUBLISH_GATE.to_string(),
            "sre-approvals".to_string(),
        ));
        gates
    }

    fn routed_gates(manifest: &serde_json::Value) -> std::collections::BTreeSet<(String, String)> {
        manifest["approvalPolicy"]["gates"]
            .as_array()
            .expect("approvalPolicy.gates must be present")
            .iter()
            .map(|gate| {
                (
                    gate["gate"].as_str().unwrap().to_string(),
                    gate["route"].as_str().unwrap_or_default().to_string(),
                )
            })
            .collect()
    }

    #[test]
    fn always_retained_gates_survive_the_transform_with_upgrade_off_and_on() {
        // #2722: a Kubernetes mutation without a routed gate raises a route-less
        // approval no operator principal can resolve. Publication uses the same
        // route, so the installer must keep all seven gates in both modes.
        let source = bundle_file(".claude-plugin/plugin.json");

        let off = runtime_plugin_manifest(source, false).unwrap();
        let off: serde_json::Value = serde_json::from_slice(&off).unwrap();
        assert_eq!(routed_gates(&off), always_retained_gate_set());
        assert_eq!(off["approvalPolicy"]["gates"].as_array().unwrap().len(), 7);

        let on = runtime_plugin_manifest(source, true).unwrap();
        let on: serde_json::Value = serde_json::from_slice(&on).unwrap();
        let mut expected = always_retained_gate_set();
        expected.insert((
            PLATFORM_UPGRADE_GATE.to_string(),
            "sre-approvals".to_string(),
        ));
        assert_eq!(routed_gates(&on), expected);
        assert_eq!(on["approvalPolicy"]["gates"].as_array().unwrap().len(), 8);
    }

    #[test]
    fn runtime_connector_transform_refuses_an_unknown_connector() {
        let source = b"connectors:\n  kubernetes: {}\n  grafana: {}\n  tempo:\n    build:\n      context: connectors/tempo\n  self-upgrade: {}\n  mystery: {}\n";
        let error =
            runtime_connector_declaration(source, "sha256:fixture", OBSERVABILITY_NAMESPACE, None)
                .expect_err("an unclassified connector must stop the install");
        assert!(error.to_string().contains("mystery"), "{error:#}");
    }

    #[test]
    fn runtime_plugin_transform_requires_the_exact_gate_policy() {
        let exact: serde_json::Value =
            serde_json::from_slice(bundle_file(".claude-plugin/plugin.json")).unwrap();
        let mutate_gate = |name: &str, replacement: Option<serde_json::Value>| {
            let mut manifest = exact.clone();
            let gates = manifest["approvalPolicy"]["gates"].as_array_mut().unwrap();
            let index = gates
                .iter()
                .position(|gate| gate["gate"] == name)
                .unwrap_or_else(|| panic!("fixture must declare {name}"));
            match replacement {
                Some(replacement) => gates[index] = replacement,
                None => {
                    gates.remove(index);
                }
            }
            manifest
        };
        let mut missing_policy = exact.clone();
        missing_policy
            .as_object_mut()
            .unwrap()
            .remove("approvalPolicy");
        let mut additional_gate = exact.clone();
        additional_gate["approvalPolicy"]["gates"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!({
                "gate": "mcp__other__write",
                "route": "sre-approvals"
            }));
        let cases = [
            ("missing approval policy", missing_policy),
            (
                "renamed gate",
                mutate_gate(
                    UPGRADE_GATE,
                    Some(serde_json::json!({
                        "gate": "mcp__self-upgrade__upgrade_agent",
                        "route": "sre-approvals"
                    })),
                ),
            ),
            ("additional gate", additional_gate),
            (
                "kubernetes mutation gate missing",
                mutate_gate("mcp__kubernetes__pods_delete", None),
            ),
            (
                "platform upgrade gate dropped",
                mutate_gate(PLATFORM_UPGRADE_GATE, None),
            ),
            (
                "different route",
                mutate_gate(
                    UPGRADE_GATE,
                    Some(serde_json::json!({
                        "gate": UPGRADE_GATE,
                        "route": "other-approvals"
                    })),
                ),
            ),
            (
                "publication gate dropped",
                mutate_gate(PLATFORM_PUBLISH_GATE, None),
            ),
            (
                "publication gate renamed",
                mutate_gate(
                    PLATFORM_PUBLISH_GATE,
                    Some(serde_json::json!({
                        "gate": "mcp__curie__publish_changes_typo",
                        "route": "sre-approvals"
                    })),
                ),
            ),
            (
                "publication route changed",
                mutate_gate(
                    PLATFORM_PUBLISH_GATE,
                    Some(serde_json::json!({
                        "gate": PLATFORM_PUBLISH_GATE,
                        "route": "other-approvals"
                    })),
                ),
            ),
        ];

        for (case, manifest) in cases {
            let source = serde_json::to_vec(&manifest).expect("serialize fixture manifest");
            let error = match runtime_plugin_manifest(&source, false) {
                Ok(_) => panic!("{case} must be refused"),
                Err(error) => error,
            };
            assert!(
                error
                    .to_string()
                    .contains("must declare the exact gated write verbs"),
                "unexpected error for {case}: {error:#}"
            );
        }
    }
}

#[cfg(test)]
mod dark_factory_render_tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};

    fn source_root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../examples/dark-factory")
    }

    /// Every file under the example, relative to its root, excluding the lock
    /// a local build writes.
    fn source_files() -> Vec<(String, PathBuf)> {
        fn walk(root: &Path, dir: &Path, out: &mut Vec<(String, PathBuf)>) {
            for entry in std::fs::read_dir(dir).expect("read example dir") {
                let path = entry.expect("dir entry").path();
                if path.is_dir() {
                    if path.file_name().and_then(|n| n.to_str()) == Some("__pycache__") {
                        continue;
                    }
                    walk(root, &path, out);
                } else {
                    let rel = path
                        .strip_prefix(root)
                        .unwrap()
                        .to_string_lossy()
                        .replace('\\', "/");
                    if rel == "connectors.lock.yaml" {
                        continue;
                    }
                    out.push((rel, path));
                }
            }
        }
        let root = source_root();
        let mut out = Vec::new();
        walk(&root, &root, &mut out);
        out.sort();
        out
    }

    // A1
    #[test]
    fn every_dark_factory_file_is_embedded_with_identical_bytes() {
        let files = source_files();
        assert!(
            files.iter().any(|(rel, _)| rel == "hooks/review_gate.py"),
            "fixture walk found no review gate: {files:?}"
        );
        for (rel, path) in &files {
            let embedded = DARK_FACTORY_BUNDLE_FILES
                .iter()
                .find(|(name, _)| name == rel)
                .unwrap_or_else(|| panic!("{rel} is in examples/dark-factory but not embedded"));
            let on_disk = std::fs::read(path).unwrap();
            assert_eq!(embedded.1, on_disk.as_slice(), "{rel} bytes differ");
        }
        for (name, _) in DARK_FACTORY_BUNDLE_FILES {
            assert!(
                files.iter().any(|(rel, _)| rel == name),
                "{name} is embedded but not in examples/dark-factory"
            );
        }
    }

    // A2
    #[tokio::test]
    async fn render_writes_every_file_and_keeps_the_review_gate_executable() {
        let tmp = tempfile::tempdir().unwrap();
        let out = tmp.path().join("factory");
        let rendered = render_dark_factory(DarkFactoryRenderOpts { out: out.clone() })
            .await
            .expect("render into a new directory");
        assert_eq!(rendered.path, out);
        for (name, bytes) in DARK_FACTORY_BUNDLE_FILES {
            let written = std::fs::read(out.join(name))
                .unwrap_or_else(|e| panic!("{name} was not written: {e}"));
            assert_eq!(written.as_slice(), *bytes, "{name} bytes differ");
        }
        let mode = std::fs::metadata(out.join("hooks/review_gate.py"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(
            mode & 0o111,
            0o111,
            "review_gate.py mode {mode:o} is not executable"
        );
        let readme_mode = std::fs::metadata(out.join("README.md"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(readme_mode & 0o111, 0, "README.md should not be executable");
        let json = rendered.to_json();
        assert_eq!(json["rendered"], serde_json::json!(true));
        assert_eq!(json["bundle_dir"], serde_json::json!(out));
    }

    // A3
    #[tokio::test]
    async fn render_into_a_non_empty_directory_is_refused_and_writes_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let out = tmp.path().join("busy");
        std::fs::create_dir_all(&out).unwrap();
        std::fs::write(out.join("keep.txt"), b"mine").unwrap();
        let error = match render_dark_factory(DarkFactoryRenderOpts { out: out.clone() }).await {
            Ok(_) => panic!("a non-empty out dir must be refused"),
            Err(error) => format!("{error:#}"),
        };
        assert!(
            error.contains(&out.display().to_string()),
            "the refusal should name the directory: {error}"
        );
        let entries: Vec<_> = std::fs::read_dir(&out)
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(entries, vec![std::ffi::OsString::from("keep.txt")]);
        assert_eq!(std::fs::read(out.join("keep.txt")).unwrap(), b"mine");
    }
}

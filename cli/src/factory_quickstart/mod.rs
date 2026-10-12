//! `curie factory quickstart`: from no cluster to a labelled issue that the
//! dark factory can turn into a pull request (#3749, ADR 0187).
//!
//! The command chains kind (only when no kube context is targeted), `cluster
//! up` with gVisor off on that kind path, the printed App registration link,
//! and on the second run the App setup, polling intake, and the published
//! dark factory deploy. A current context that is not a kind context (`kind-`
//! prefix) is confirmed in a terminal and refused without one. An explicit
//! `--context` is not asked. It never opens a browser and never runs `gh`.

use std::io::{IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::process::{Output, Stdio};

use anyhow::Result;

use crate::exit::CliError;
use crate::factory_intake::{intake_values, FactoryIntakeOpts};
use crate::ops::{require_on_path, CommonOpts};
use crate::ui::DryRunPlan;

mod bitbucket_cloud;
mod bitbucket_dc;
mod github;
mod gitlab;
mod jira_cloud;

pub use github::{deploy_args, intake_args, registration_steps, RerunTarget};

pub const DEFAULT_KIND_NAME: &str = "curie-factory";
pub const DEFAULT_MODEL: &str = "z-ai/glm-5.3-flash";
pub const ANTHROPIC_DEFAULT_MODEL: &str = "claude-sonnet-5-5";
pub const DEFAULT_DEADLINE_SECONDS: u32 = 10800;
pub const DEFAULT_BUDGET_USD: f64 = 5.0;
pub const DEFAULT_AGENT: &str = "dark-factory";
pub const GVISOR_OFF_SET: &str = "security.gvisor.mode=off";
pub const POLL_INTAKE: &str = "poll";
/// Display model matching the dark-factory bundle's `progress/phases.json`.
/// The runner resolves the reviewers' `opus` alias from the credential and override.
pub const REVIEWER_MODEL: &str = "openai/gpt-6.1-sol";
/// The OpenRouter credit one factory run should have available (#3935).
pub const RUN_CREDIT_USD: f64 = 5.0;

#[derive(Debug, Clone)]
pub struct QuickstartOpts {
    pub repos: Vec<String>,
    pub app_id: Option<String>,
    pub private_key_file: Option<PathBuf>,
    pub context: Option<String>,
    pub namespace: String,
    pub release: String,
    pub org: Option<String>,
    pub kind_name: String,
    pub model: Option<String>,
    pub execution_deadline_seconds: u32,
    pub budget_usd: f64,
    /// The agent to deploy (`--agent`, default [`DEFAULT_AGENT`]).
    pub agent: String,
    /// The trigger label for this agent's GitHub bindings (`--label`).
    pub label: Option<String>,
    /// An operator bundle deployed as given (`--plugin-dir`).
    pub plugin_dir: Option<PathBuf>,
    pub chart: String,
    pub dry_run: bool,
    pub forge: ForgeFlags,
}

/// The factory intake a serving release records (#4362 decision 2), read from
/// the `factory.*` values `cluster factory` writes and `api.githubRepoAllowlist`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedIntake {
    pub app_id: String,
    /// The GitHub tracker label: the effective label of a binding with none.
    pub install_label: String,
    pub repo_allowlist: Vec<String>,
    /// The Secret and data key holding the App private key.
    pub pem_secret: String,
    pub pem_secret_key: String,
}

/// One agent's GitHub binding on the serving release, as the API lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentBinding {
    pub agent: String,
    pub repo: String,
    /// `None` is served by the install label.
    pub trigger_label: Option<String>,
}

/// The github entry of a `factory.codeHosts` or `factory.trackers` list.
fn github_entry<'a>(factory: &'a serde_json::Value, list: &str) -> Option<&'a serde_json::Value> {
    factory
        .get(list)?
        .as_array()?
        .iter()
        .find(|entry| entry.get("kind").and_then(|v| v.as_str()) == Some("github"))
}

fn non_empty_str(value: Option<&serde_json::Value>) -> Option<String> {
    match value? {
        serde_json::Value::String(text) if !text.is_empty() => Some(text.clone()),
        serde_json::Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

/// The recorded factory intake in a release's user values, or `None` unless
/// all of it is there: `factory.enabled` true, a non-empty `factory.intake`, a
/// github tracker with a label, and a github code host whose credential names
/// the App id and its key Secret.
pub fn recorded_intake(values: &serde_json::Value) -> Option<RecordedIntake> {
    let factory = values.get("factory")?;
    if factory.get("enabled") != Some(&serde_json::Value::Bool(true)) {
        return None;
    }
    non_empty_str(factory.get("intake"))?;
    let install_label = non_empty_str(github_entry(factory, "trackers")?.get("label"))?;
    let credential = github_entry(factory, "codeHosts")?.get("credential")?;
    let app_id = non_empty_str(credential.get("appId"))?;
    let pem_secret = non_empty_str(credential.get("existingSecret"))?;
    let pem_secret_key = non_empty_str(credential.get("existingSecretKey"))
        .unwrap_or_else(|| crate::github_app::DEFAULT_APP_KEY_DATA_KEY.to_string());
    let repo_allowlist = values
        .pointer("/api/githubRepoAllowlist")
        .and_then(|v| v.as_array())
        .map(|list| {
            list.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    Some(RecordedIntake {
        app_id,
        install_label,
        repo_allowlist,
        pem_secret,
        pem_secret_key,
    })
}

/// The code host the factory publishes to (`--code-host`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, clap::ValueEnum)]
pub enum CodeHostKind {
    #[default]
    Github,
    Gitlab,
    BitbucketCloud,
    BitbucketDc,
}

/// The tracker the factory takes issues from (`--tracker`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, clap::ValueEnum)]
pub enum TrackerKind {
    #[default]
    Github,
    Gitlab,
    JiraCloud,
}

/// The forge selection and the flags only a non-GitHub forge reads. GitHub
/// takes its URL and credential from the App (`--app-id`,
/// `--private-key-file`), so it refuses these.
#[derive(Debug, Clone, Default)]
pub struct ForgeFlags {
    pub code_host: CodeHostKind,
    pub tracker: TrackerKind,
    pub forge_url: Option<String>,
    pub token_file: Option<PathBuf>,
    pub ca_file: Option<PathBuf>,
    pub review_accounts: Vec<String>,
}

/// One port of a forge egress entry, as the chart's `egress[].ports` reads it.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct EgressPort {
    pub protocol: &'static str,
    pub port: u16,
}

/// One `factory.codeHosts[]` / `factory.trackers[]` `egress` entry (#4397).
/// `host` records the hostname a route was resolved from; the chart ignores it.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct EgressEntry {
    pub cidr: String,
    pub ports: Vec<EgressPort>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
}

/// The egress entries for one forge `target` on TCP `ports`. An IP or CIDR
/// passes through (an IP as a host route). A hostname is resolved here, on
/// the operator's machine, by `resolve` to one host route per address with
/// the hostname recorded; a name that resolves to nothing is refused.
pub fn forge_egress_entries(
    target: &str,
    ports: &[u16],
    resolve: impl Fn(&str) -> Result<Vec<std::net::IpAddr>>,
) -> Result<Vec<EgressEntry>> {
    use std::net::IpAddr;
    let ports: Vec<EgressPort> = ports
        .iter()
        .map(|&port| EgressPort {
            protocol: "TCP",
            port,
        })
        .collect();
    let host_route = |ip: IpAddr| match ip {
        IpAddr::V4(_) => format!("{ip}/32"),
        IpAddr::V6(_) => format!("{ip}/128"),
    };
    let entry = |cidr: String, host: Option<String>| EgressEntry {
        cidr,
        ports: ports.clone(),
        host,
    };
    if let Ok(ip) = target.parse::<IpAddr>() {
        return Ok(vec![entry(host_route(ip), None)]);
    }
    if let Some((addr, prefix)) = target.split_once('/') {
        let ip: IpAddr = addr.parse().map_err(|_| {
            CliError::usage(format!(
                "forge egress {target:?} is not an IP, CIDR or hostname"
            ))
        })?;
        let max = if ip.is_ipv4() { 32 } else { 128 };
        match prefix.parse::<u8>() {
            Ok(bits) if bits <= max => return Ok(vec![entry(format!("{ip}/{bits}"), None)]),
            _ => {
                return Err(CliError::usage(format!(
                    "forge egress {target:?} has an invalid prefix length"
                ))
                .into())
            }
        }
    }
    let mut addresses = resolve(target).map_err(|error| {
        CliError::failure(format!("could not resolve forge host {target}: {error:#}"))
    })?;
    let mut seen = std::collections::HashSet::new();
    addresses.retain(|ip| seen.insert(*ip));
    if addresses.is_empty() {
        return Err(CliError::failure(format!(
            "forge host {target} resolved to no address, so no egress can be written for it"
        ))
        .with_fix("pass the host's IP or CIDR instead, or fix its DNS")
        .into());
    }
    Ok(addresses
        .into_iter()
        .map(|ip| entry(host_route(ip), Some(target.to_string())))
        .collect())
}

/// The error for a forge kind this build has no quickstart module for.
fn forge_not_installed(kind: &str) -> anyhow::Error {
    CliError::unsupported(format!(
        "forge kind '{kind}' is not installed in this build"
    ))
    .with_fix("rerun with --code-host github and --tracker github, or omit both")
    .into()
}

/// Refuses a forge selection this build cannot run. Pure: it reads no file,
/// network, or cluster, so the caller runs it before resolving the chart.
/// GitHub refuses the non-GitHub flags; any other kind is routed to its module.
pub fn select_forge(forge: &ForgeFlags) -> Result<()> {
    match (forge.code_host, forge.tracker) {
        (CodeHostKind::Github, TrackerKind::Github) => {
            let shared = [
                ("--forge-url", forge.forge_url.is_some()),
                ("--token-file", forge.token_file.is_some()),
                ("--ca-file", forge.ca_file.is_some()),
                ("--review-account", !forge.review_accounts.is_empty()),
            ];
            match shared.iter().find(|(_, set)| *set) {
                Some((flag, _)) => Err(CliError::usage(format!(
                    "{flag} applies only to a non-github --code-host or --tracker; GitHub uses the App from --app-id and --private-key-file"
                ))
                .with_fix(format!("remove {flag}, or pass the --code-host or --tracker it is for"))
                .into()),
                None => Ok(()),
            }
        }
        // A reserved kind's not-installed error wins over any pairing rule.
        (CodeHostKind::Gitlab, _) => gitlab::check(forge),
        (CodeHostKind::BitbucketCloud, _) => bitbucket_cloud::check(forge),
        (CodeHostKind::BitbucketDc, _) => bitbucket_dc::check(forge),
        (CodeHostKind::Github, TrackerKind::Gitlab) => gitlab::check(forge),
        (CodeHostKind::Github, TrackerKind::JiraCloud) => jira_cloud::check(forge),
    }
}

#[derive(Debug, Clone)]
pub struct PlanInput {
    pub repos: Vec<String>,
    pub app_id: Option<String>,
    pub private_key_file: Option<String>,
    pub explicit_context: Option<String>,
    pub current_context: Option<String>,
    pub kind_name: String,
    pub existing_kind_clusters: Vec<String>,
    pub namespace: String,
    pub release: String,
    pub model: String,
    pub org: Option<String>,
    pub execution_deadline_seconds: u32,
    pub budget_usd: f64,
    pub chart: String,
    pub credential_in_env: bool,
    pub release_has_real_model: bool,
    pub interactive: bool,
    /// The release is already the chart and the quickstart values, so this
    /// plan must not invoke `cluster up`.
    pub release_at_target: bool,
    /// The agent to deploy.
    pub agent: String,
    /// The trigger label for this agent's bindings; `None` is the install label.
    pub label: Option<String>,
    /// An operator bundle deployed as given; `None` renders the published one.
    pub plugin_dir: Option<PathBuf>,
    /// `--model` was passed, which the add agent path refuses.
    pub model_explicit: bool,
    /// The factory intake the serving release records, if any.
    pub recorded: Option<RecordedIntake>,
    /// The GitHub bindings the serving release holds, read once.
    pub bindings: Vec<AgentBinding>,
    /// The operator bundle's locked runner image; `None` is the platform runner.
    pub runner_image: Option<String>,
}

/// Which run this is (#4362 decision 2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuickstartPath {
    /// No App flags and no recorded intake: print the App registration link.
    Register,
    /// App flags passed: install, intake, and deploy the agent.
    Install,
    /// No App flags on a release that records intake: add the agent to it.
    AddAgent,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CredentialDecision {
    /// Use the key already in the environment. Do not prompt.
    UseEnv,
    /// The release already runs a real model. Do not prompt.
    PreserveRecorded,
    /// Ask once, immediately before `cluster up`.
    PromptOnce,
    /// No key, no recorded model, and no terminal.
    RefuseNonInteractive,
    /// The add agent path runs no `cluster up`, so it needs no key.
    NotNeeded,
}

#[derive(Debug, Clone)]
pub enum Action {
    External {
        program: String,
        args: Vec<String>,
    },
    Cluster {
        args: Vec<String>,
    },
    Register {
        org: Option<String>,
        repos: Vec<String>,
        context: String,
        namespace: String,
        release: String,
        model: String,
        /// `--agent`, `--label`, and `--plugin-dir` when not their defaults.
        rerun_flags: Vec<String>,
    },
    Intake(Box<FactoryIntakeOpts>),
    /// Enroll an added agent for forge egress (`bind_forge_egress`).
    ForgeEgress {
        agent: String,
    },
    /// Create the trigger label on each repository that lacks it.
    TriggerLabel {
        label: String,
        repos: Vec<String>,
    },
    Deploy {
        context: String,
        namespace: String,
        release: String,
        repos: Vec<String>,
        deadline_seconds: u32,
        budget_usd: f64,
        agent: String,
        label: Option<String>,
        /// The operator bundle; `None` renders the published dark factory.
        bundle: Option<PathBuf>,
        /// The operator bundle's locked runner image; `None` is the platform runner.
        runner_image: Option<String>,
    },
}

#[derive(Debug, Clone)]
pub struct Planned {
    pub context: String,
    /// Why `context` was selected. `--dry-run` prints this before any step.
    pub context_reason: String,
    pub kind_cluster: Option<String>,
    pub credential: CredentialDecision,
    pub actions: Vec<Action>,
    /// `cluster up` was left out because the release is already at the target.
    pub skipped_cluster_up: bool,
    pub path: QuickstartPath,
    pub agent: String,
    pub release: String,
}

#[derive(Debug)]
pub enum QuickstartOutput {
    DryRun(DryRunPlan),
    Registration {
        context: String,
        url: String,
        steps: Vec<String>,
        kind_cluster: Option<String>,
    },
    Ready {
        context: String,
        repo: String,
        intake: String,
        app_id: String,
        slug: String,
        mention: String,
        mention_inferred: bool,
        repos: Vec<String>,
        label: String,
        label_inferred: bool,
        /// `None` when the agent runs the platform runner.
        runner_image: Option<String>,
        agent: String,
        /// The agent's own binding label; `None` when the install label serves it.
        trigger_label: Option<String>,
        /// The label that starts this agent's work.
        effective_label: String,
        deadline_seconds: u32,
        budget_usd: f64,
        /// OpenRouter credit left to the model credential, when it was read.
        credit_remaining_usd: Option<f64>,
    },
    /// An agent added to a release that already records factory intake.
    AgentAdded {
        context: String,
        agent: String,
        repos: Vec<String>,
        trigger_label: Option<String>,
        effective_label: String,
        /// `published`, or the operator bundle directory.
        bundle: String,
        /// `None` when the agent runs the platform runner.
        runner_image: Option<String>,
        deadline_seconds: u32,
        budget_usd: f64,
        credit_remaining_usd: Option<f64>,
    },
}

impl crate::ui::CliOutput for QuickstartOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            Self::DryRun(plan) => plan.to_json(),
            Self::Registration {
                context,
                url,
                steps,
                kind_cluster,
            } => serde_json::json!({
                "phase": "register",
                "context": context,
                "kind_cluster": kind_cluster,
                "github_app_registration_url": url,
                "steps": steps,
            }),
            Self::Ready {
                context,
                repo,
                intake,
                app_id,
                slug,
                mention,
                mention_inferred,
                repos,
                label,
                label_inferred,
                runner_image,
                agent,
                trigger_label,
                effective_label,
                deadline_seconds,
                budget_usd,
                credit_remaining_usd,
            } => serde_json::json!({
                "phase": "ready",
                "context": context,
                "repo": repo,
                "intake": intake,
                "github_app": {
                    "app_id": app_id,
                    "slug": slug,
                    "mention": mention,
                    "mention_inferred": mention_inferred,
                    "repos": repos,
                    "label": label,
                    "label_inferred": label_inferred,
                },
                "runner_image": runner_image,
                "agent": agent,
                "trigger_label": trigger_label,
                "effective_label": effective_label,
                "execution_deadline_seconds": deadline_seconds,
                "budget_usd": budget_usd,
                "reviewer_model": REVIEWER_MODEL,
                "run_credit_usd": RUN_CREDIT_USD,
                "credit_remaining_usd": credit_remaining_usd,
            }),
            Self::AgentAdded {
                context,
                agent,
                repos,
                trigger_label,
                effective_label,
                bundle,
                runner_image,
                deadline_seconds,
                budget_usd,
                credit_remaining_usd,
            } => serde_json::json!({
                "phase": "agent_added",
                "context": context,
                "agent": agent,
                "repos": repos,
                "trigger_label": trigger_label,
                "effective_label": effective_label,
                "bundle": bundle,
                "runner_image": runner_image,
                "execution_deadline_seconds": deadline_seconds,
                "budget_usd": budget_usd,
                "credit_remaining_usd": credit_remaining_usd,
            }),
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            Self::DryRun(plan) => plan.render(ui),
            Self::Registration { url, steps, .. } => {
                ui.payload("No App id was passed. Register the factory App, then rerun:");
                ui.payload_plain(url);
                for (index, step) in steps.iter().enumerate() {
                    ui.payload_plain(&format!("{}. {step}", index + 1));
                }
            }
            Self::Ready {
                context,
                repo,
                intake,
                slug,
                mention,
                mention_inferred,
                repos,
                label,
                label_inferred,
                runner_image,
                agent,
                effective_label,
                deadline_seconds,
                budget_usd,
                credit_remaining_usd,
                ..
            } => {
                let mention_note = if *mention_inferred {
                    "inferred from the App slug"
                } else {
                    "set explicitly"
                };
                let label_note = if *label_inferred {
                    "inferred"
                } else {
                    "set explicitly"
                };
                ui.payload(&format!(
                    "Factory quickstart ready on context {context} for {repo}."
                ));
                ui.payload(&format!(
                    "Intake {intake}. App {slug}. Mention {mention} ({mention_note}). Label {label} ({label_note}). Allowlist {}.",
                    repos.join(", ")
                ));
                ui.payload(&format!(
                    "Deployed {agent} from {}. Label {effective_label} starts its work. Execution deadline {deadline_seconds}s. Publication auto. Budget {budget_usd} USD.",
                    runner_source(runner_image.as_deref())
                ));
                // Worded apart from the pre-deploy low-credit warning.
                let credit_left = credit_remaining_usd
                    .map(|left| format!(" The key has {left:.2} USD left."))
                    .unwrap_or_default();
                ui.payload(&format!(
                    "Reviewers run {REVIEWER_MODEL}. Plan on {} USD of OpenRouter credit per run.{credit_left}",
                    budget_display(RUN_CREDIT_USD)
                ));
            }
            Self::AgentAdded {
                context,
                agent,
                repos,
                effective_label,
                bundle,
                runner_image,
                deadline_seconds,
                budget_usd,
                credit_remaining_usd,
                ..
            } => {
                ui.payload(&format!(
                    "Added {agent} to the factory install on context {context} for {}.",
                    repos.join(", ")
                ));
                ui.payload(&format!(
                    "Label {effective_label} starts its work. Bundle {bundle} on {}. Execution deadline {deadline_seconds}s. Publication auto. Budget {budget_usd} USD.",
                    runner_source(runner_image.as_deref())
                ));
                if let Some(left) = credit_remaining_usd {
                    ui.payload(&format!(
                        "The model key has {left:.2} USD of OpenRouter credit left."
                    ));
                }
            }
        }
    }
}

/// "image X" or "the platform runner", for the human output.
fn runner_source(image: Option<&str>) -> String {
    match image {
        Some(image) => image.to_string(),
        None => "the platform runner".to_string(),
    }
}

pub fn validate_repo(repo: &str) -> Result<()> {
    let Some((owner, name)) = repo.split_once('/') else {
        return Err(
            CliError::usage(format!("--repo {repo:?} is not owner/name"))
                .with_fix("pass the GitHub repository as owner/name, for example acme/widgets")
                .into(),
        );
    };
    let ok = |part: &str| {
        !part.is_empty()
            && !part.contains('/')
            && part
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
    };
    if !ok(owner) || !ok(name) {
        return Err(
            CliError::usage(format!("--repo {repo:?} is not owner/name"))
                .with_fix("pass the GitHub repository as owner/name, for example acme/widgets")
                .into(),
        );
    }
    Ok(())
}

/// Validates the quickstart repository list: at least one entry, each
/// owner/name, and no repository named twice (GitHub names are case
/// insensitive).
pub fn validate_repos(repos: &[String]) -> Result<()> {
    if repos.is_empty() {
        return Err(CliError::usage("--repo names no repository")
            .with_fix("pass the GitHub repositories as owner/name, for example --repo acme/widgets")
            .into());
    }
    let mut seen: Vec<String> = Vec::with_capacity(repos.len());
    for repo in repos {
        validate_repo(repo)?;
        let key = repo.to_ascii_lowercase();
        if seen.contains(&key) {
            return Err(
                CliError::usage(format!("--repo names {repo:?} more than once"))
                    .with_fix("list each GitHub repository once in --repo")
                    .into(),
            );
        }
        seen.push(key);
    }
    Ok(())
}

/// "a/x", "a/x and b/y", "a/x, b/y and c/z".
fn repos_prose(repos: &[String]) -> String {
    match repos {
        [] => String::new(),
        [only] => only.clone(),
        [head @ .., last] => format!("{} and {last}", head.join(", ")),
    }
}

pub fn validate_model_credential(key: &str) -> Result<()> {
    if ["sk-or-", "sk-ant-"]
        .iter()
        .any(|prefix| key.starts_with(*prefix) && key.len() > prefix.len())
    {
        return Ok(());
    }
    Err(
        CliError::usage("the model credential is not an Anthropic or OpenRouter credential")
            .with_fix("set CURIE_CREDENTIALS to an Anthropic API key starting with sk-ant- or an OpenRouter key starting with sk-or-")
            .into(),
    )
}

/// Preserve an explicit model, otherwise select the effective credential's default.
pub fn quickstart_model(model: Option<&str>, credential: Option<&str>) -> String {
    model
        .unwrap_or_else(|| {
            if credential.is_some_and(|key| key.starts_with("sk-ant-")) {
                ANTHROPIC_DEFAULT_MODEL
            } else {
                DEFAULT_MODEL
            }
        })
        .to_string()
}

pub fn credential_decision(
    credential_in_env: bool,
    release_has_real_model: bool,
    interactive: bool,
) -> CredentialDecision {
    if credential_in_env {
        CredentialDecision::UseEnv
    } else if release_has_real_model {
        CredentialDecision::PreserveRecorded
    } else if interactive {
        CredentialDecision::PromptOnce
    } else {
        CredentialDecision::RefuseNonInteractive
    }
}

pub fn kind_context_name(kind_name: &str) -> String {
    format!("kind-{kind_name}")
}

/// Kind writes kubeconfig contexts as `kind-<cluster>`. The same prefix selects
/// the gVisor-off path, so confirmation uses it too.
fn is_kind_context(name: &str) -> bool {
    name.starts_with("kind-")
}

fn unconfirmed_remote_context<'a>(
    explicit: Option<&'a str>,
    current: Option<&'a str>,
) -> Option<&'a str> {
    match (explicit, current) {
        (None, Some(name)) if !is_kind_context(name) => Some(name),
        _ => None,
    }
}

fn unconfirmed_context_error(name: &str) -> anyhow::Error {
    CliError::usage(format!(
        "refusing to install into Kubernetes context {name} because it is not a kind context and stdin is not a terminal. Pass --context {name} to proceed"
    ))
    .with_fix(format!("rerun with --context {name}"))
    .into()
}

fn context_confirmation_line(name: &str, line: &str) -> Result<()> {
    if matches!(line.trim(), "y" | "Y" | "yes" | "Yes") {
        return Ok(());
    }
    Err(CliError::usage(format!(
        "refusing to install into Kubernetes context {name} without confirmation. Pass --context {name} to proceed"
    ))
    .with_fix(format!("rerun with --context {name}"))
    .into())
}

fn confirm_remote_context(name: &str, interactive: bool) -> Result<()> {
    if !interactive {
        return Err(unconfirmed_context_error(name));
    }
    eprint!("Kubernetes context {name} is not a kind context. Install Curie into it? [y/N] ");
    let _ = std::io::stderr().flush();
    let mut line = String::new();
    std::io::stdin()
        .read_line(&mut line)
        .map_err(|err| CliError::failure(format!("reading confirmation from stdin: {err}")))?;
    context_confirmation_line(name, &line)
}

fn context_reason(
    explicit: Option<&str>,
    current: Option<&str>,
    context: &str,
    create_kind: bool,
    kind_cluster: Option<&str>,
) -> String {
    if explicit.is_some() {
        format!("Kubernetes context: {context} because --context was passed")
    } else if let Some(name) = current {
        if is_kind_context(name) {
            format!("Kubernetes context: {context} because the current context is a kind context")
        } else {
            format!(
                "Kubernetes context: {context} because it is the current context and it is not a kind context; a terminal must confirm before install, and a non-terminal run stops until --context {context} is passed"
            )
        }
    } else if create_kind {
        format!(
            "Kubernetes context: {context} because no current context is set, so a kind cluster is created"
        )
    } else {
        let name = kind_cluster.unwrap_or(context);
        format!(
            "Kubernetes context: {context} because no current context is set and kind cluster {name} already exists"
        )
    }
}

/// Where the published bundle renders for `agent`. The default agent's path
/// is the one every earlier quickstart used.
pub fn bundle_dir(namespace: &str, release: &str, agent: &str) -> PathBuf {
    std::env::temp_dir()
        .join(format!("curie-factory-quickstart-{namespace}-{release}"))
        .join(agent)
}

/// Decision 5.1 and 5.2 of #4362: the agent is a valid per-agent resource
/// name and the label passes the `surfaces --label` rule.
pub fn validate_agent_flags(agent: &str, label: Option<&str>) -> Result<()> {
    crate::cluster_secrets::validate_agent_resource_name(agent).map_err(|error| {
        anyhow::Error::from(
            CliError::usage(format!("--agent {agent:?} is refused: {error:#}")).with_fix(
                "pass a lowercase DNS label of at most 40 characters other than self, for example --agent dark-factory-fast",
            ),
        )
    })?;
    if let Some(label) = label {
        crate::commands::valid_trigger_label(label)?;
    }
    Ok(())
}

/// `--model` and `--org` configure the install, so the add agent path refuses
/// them (#4362 decision 3).
pub fn refuse_install_flags(model_explicit: bool, org: Option<&str>) -> Result<()> {
    if model_explicit {
        return Err(CliError::usage(
            "--model configures the install, not an added agent, and this release already records factory intake; change the install's model with `curie cluster up --model <MODEL>`",
        )
        .with_fix("drop --model and rerun")
        .into());
    }
    if org.is_some() {
        return Err(CliError::usage(
            "--org picks where the factory App is registered, and this release already records an App; manage it with `curie cluster github-app`",
        )
        .with_fix("drop --org and rerun")
        .into());
    }
    Ok(())
}

/// True when an allowlist entry admits `repo`: the same name, or `owner/*`.
fn allowlist_admits(allowlist: &[String], repo: &str) -> bool {
    let owner = repo.split_once('/').map(|(owner, _)| owner);
    allowlist.iter().any(|entry| {
        entry.eq_ignore_ascii_case(repo)
            || entry
                .strip_suffix("/*")
                .is_some_and(|wild| owner.is_some_and(|owner| owner.eq_ignore_ascii_case(wild)))
    })
}

/// Decision 5.4: every repository is admitted by the recorded allowlist.
fn check_recorded_allowlist(input: &PlanInput, recorded: &RecordedIntake) -> Result<()> {
    let missing: Vec<&String> = input
        .repos
        .iter()
        .filter(|repo| !allowlist_admits(&recorded.repo_allowlist, repo))
        .collect();
    if missing.is_empty() {
        return Ok(());
    }
    let mut entries = recorded.repo_allowlist.clone();
    entries.extend(missing.iter().map(|repo| repo.to_string()));
    let missing: Vec<String> = missing.into_iter().cloned().collect();
    Err(CliError::usage(format!(
        "the factory install on release {} does not admit {}; widen its allowlist with `curie cluster factory --repo {}`, then rerun",
        input.release,
        repos_prose(&missing),
        entries.join(",")
    ))
    .with_fix(format!(
        "run curie cluster factory --repo {} and rerun",
        entries.join(",")
    ))
    .into())
}

/// Decision 5.7: an install path run must not drop a recorded allowlist entry.
fn check_allowlist_kept(input: &PlanInput, recorded: &RecordedIntake) -> Result<()> {
    let dropped: Vec<String> = recorded
        .repo_allowlist
        .iter()
        .filter(|entry| {
            !input
                .repos
                .iter()
                .any(|repo| repo.eq_ignore_ascii_case(entry))
        })
        .cloned()
        .collect();
    if dropped.is_empty() {
        return Ok(());
    }
    let mut repos = input.repos.clone();
    repos.extend(dropped.iter().cloned());
    Err(CliError::usage(format!(
        "the intake would replace the recorded allowlist and drop {}, which another agent may serve",
        repos_prose(&dropped)
    ))
    .with_fix(format!("rerun with --repo {}", repos.join(",")))
    .into())
}

/// Decision 5.5: a binding's effective label is its own label or the install
/// label. Another agent may not hold the requested effective label on a
/// repository, and this agent's own binding there must already carry it.
fn check_bindings(input: &PlanInput) -> Result<()> {
    let install = input.recorded.as_ref().map(|r| r.install_label.as_str());
    let requested = input.label.as_deref().or(install);
    for repo in &input.repos {
        for binding in input
            .bindings
            .iter()
            .filter(|binding| binding.repo.eq_ignore_ascii_case(repo))
        {
            let held = binding.trigger_label.as_deref().or(install);
            let shown = |label: Option<&str>| label.unwrap_or("the install label").to_string();
            if binding.agent == input.agent {
                if held != requested {
                    return Err(CliError::usage(format!(
                        "agent {} already holds github={repo} under label {}, not {}; run `curie cluster surfaces {} --remove github={repo}`, then rerun",
                        input.agent,
                        shown(held),
                        shown(requested),
                        input.agent
                    ))
                    .with_fix(format!(
                        "run curie cluster surfaces {} --remove github={repo} and rerun",
                        input.agent
                    ))
                    .into());
                }
            } else if held == requested {
                return Err(CliError::usage(format!(
                    "agent {} already serves {repo} under label {}; pass --label with a label no other agent uses on {repo}",
                    binding.agent,
                    shown(held)
                ))
                .with_fix("rerun with a distinct --label <LABEL>")
                .into());
            }
        }
    }
    Ok(())
}

/// The flags a register path rerun carries when they are not their defaults.
fn rerun_flags(input: &PlanInput) -> Vec<String> {
    let mut flags = Vec::new();
    if input.agent != DEFAULT_AGENT {
        flags.extend(["--agent".to_string(), input.agent.clone()]);
    }
    if let Some(label) = &input.label {
        flags.extend(["--label".to_string(), label.clone()]);
    }
    if let Some(dir) = &input.plugin_dir {
        flags.extend(["--plugin-dir".to_string(), dir.display().to_string()]);
    }
    flags
}

/// True when helm user values show a real model install (`fakeModel: false`).
pub fn release_has_real_model(values: &serde_json::Value) -> bool {
    values.pointer("/agentSandbox/runner/fakeModel") == Some(&serde_json::json!(false))
}

pub fn quickstart_plan(input: &PlanInput) -> Result<Planned> {
    validate_repos(&input.repos)?;
    validate_agent_flags(&input.agent, input.label.as_deref())?;
    let finishing = match (&input.app_id, &input.private_key_file) {
        (None, None) => false,
        (Some(id), Some(path)) => {
            if id.is_empty() || !id.chars().all(|c| c.is_ascii_digit()) {
                return Err(CliError::usage(format!("--app-id {id:?} is not a number"))
                    .with_fix("pass the numeric App ID from the App's settings page")
                    .into());
            }
            if path.is_empty() {
                return Err(CliError::usage("--private-key-file is empty")
                    .with_fix("pass the .pem file downloaded from the App's settings page")
                    .into());
            }
            true
        }
        _ => {
            return Err(CliError::usage(
                "--app-id and --private-key-file go together; pass both or neither",
            )
            .with_fix("add the missing --app-id <ID> or --private-key-file <PATH>")
            .into());
        }
    };
    if input.kind_name.is_empty()
        || !input
            .kind_name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-')
    {
        return Err(CliError::usage(format!(
            "--kind-name {:?} is not a kind cluster name",
            input.kind_name
        ))
        .with_fix("pass a name of letters, digits, and hyphens, or omit --kind-name")
        .into());
    }
    if !input.interactive {
        if let Some(name) = unconfirmed_remote_context(
            input.explicit_context.as_deref(),
            input.current_context.as_deref(),
        ) {
            return Err(unconfirmed_context_error(name));
        }
    }
    let path = match (finishing, &input.recorded) {
        (true, _) => QuickstartPath::Install,
        (false, Some(_)) => QuickstartPath::AddAgent,
        (false, None) => QuickstartPath::Register,
    };
    // #4362 decision 5, in order, before any mutation.
    if let (QuickstartPath::AddAgent, Some(recorded)) = (path, &input.recorded) {
        refuse_install_flags(input.model_explicit, input.org.as_deref())?;
        check_recorded_allowlist(input, recorded)?;
    }
    check_bindings(input)?;
    if let (QuickstartPath::Install, Some(recorded)) = (path, &input.recorded) {
        check_allowlist_kept(input, recorded)?;
    }
    let add_agent = path == QuickstartPath::AddAgent;

    let owned_kind = kind_context_name(&input.kind_name);
    let context = input
        .explicit_context
        .clone()
        .or_else(|| input.current_context.clone())
        .unwrap_or_else(|| owned_kind.clone());
    // Kind is created only when nothing is targeted. A kind context, including
    // the one this command created on a previous run, still gets gVisor off
    // and the CoreDNS scale. A remote context does not.
    // The add agent path never touches the cluster underneath the release.
    let kind_target = !add_agent
        && (input.explicit_context.is_none() && input.current_context.is_none()
            || is_kind_context(&context));
    let kind_cluster = kind_target.then(|| {
        if is_kind_context(&context) {
            context.trim_start_matches("kind-").to_string()
        } else {
            input.kind_name.clone()
        }
    });
    let create_kind = input.explicit_context.is_none()
        && input.current_context.is_none()
        && kind_cluster
            .as_ref()
            .is_some_and(|name| !input.existing_kind_clusters.iter().any(|c| c == name));

    let mut actions = Vec::new();
    if let Some(name) = &kind_cluster {
        if create_kind {
            actions.push(Action::External {
                program: "kind".into(),
                args: vec![
                    "create".into(),
                    "cluster".into(),
                    "--name".into(),
                    name.clone(),
                ],
            });
        }
        actions.push(Action::External {
            program: "kubectl".into(),
            args: vec![
                "--context".into(),
                context.clone(),
                "-n".into(),
                "kube-system".into(),
                "scale".into(),
                "deployment/coredns".into(),
                "--replicas=1".into(),
            ],
        });
    }

    let mut up = vec![
        "cluster".into(),
        "up".into(),
        "--context".into(),
        context.clone(),
        "--namespace".into(),
        input.namespace.clone(),
        "--release".into(),
        input.release.clone(),
        "--model".into(),
        input.model.clone(),
        "--chart".into(),
        input.chart.clone(),
    ];
    if kind_target {
        up.push("--set".into());
        up.push(GVISOR_OFF_SET.into());
    }
    // ADR 0114 and ADR 0193 keep install-fact inference inside `cluster up`.
    // The skip stays here: quickstart does not invoke `cluster up` when the
    // release is already that chart and these values.
    let skipped_cluster_up = input.release_at_target && !add_agent;
    if !input.release_at_target && !add_agent {
        actions.push(Action::Cluster { args: up });
    }

    let bundle = input.plugin_dir.clone();
    let deploy = Action::Deploy {
        context: context.clone(),
        namespace: input.namespace.clone(),
        release: input.release.clone(),
        repos: input.repos.clone(),
        deadline_seconds: input.execution_deadline_seconds,
        budget_usd: input.budget_usd,
        agent: input.agent.clone(),
        label: input.label.clone(),
        runner_image: bundle.as_ref().and(input.runner_image.clone()),
        bundle,
    };
    match path {
        QuickstartPath::Install => {
            actions.push(Action::Intake(Box::new(github::finish_intake_opts(
                input, &context,
            ))));
            // The intake creates the install label; an agent label is ours.
            if let Some(label) = &input.label {
                actions.push(Action::TriggerLabel {
                    label: label.clone(),
                    repos: input.repos.clone(),
                });
            }
            actions.push(deploy);
        }
        QuickstartPath::AddAgent => {
            let install = input.recorded.as_ref().map(|r| r.install_label.clone());
            actions.push(Action::ForgeEgress {
                agent: input.agent.clone(),
            });
            if let Some(label) = input.label.clone().or(install) {
                actions.push(Action::TriggerLabel {
                    label,
                    repos: input.repos.clone(),
                });
            }
            actions.push(deploy);
        }
        QuickstartPath::Register => {
            actions.push(Action::Register {
                org: input.org.clone(),
                repos: input.repos.clone(),
                context: context.clone(),
                namespace: input.namespace.clone(),
                release: input.release.clone(),
                model: input.model.clone(),
                rerun_flags: rerun_flags(input),
            });
        }
    }

    let context_reason = context_reason(
        input.explicit_context.as_deref(),
        input.current_context.as_deref(),
        &context,
        create_kind,
        kind_cluster.as_deref(),
    );
    Ok(Planned {
        context,
        context_reason,
        kind_cluster,
        credential: if add_agent {
            CredentialDecision::NotNeeded
        } else {
            credential_decision(
                input.credential_in_env,
                input.release_has_real_model,
                input.interactive,
            )
        },
        actions,
        skipped_cluster_up,
        path,
        agent: input.agent.clone(),
        release: input.release.clone(),
    })
}

/// The intake line's runner clause: the published layer, an operator bundle's
/// locked image, or the platform runner (#4362 decision 9.2).
fn intake_runner_clause(opts: &FactoryIntakeOpts) -> String {
    match &opts.runner_binding {
        Some(binding) if binding.image == github::PUBLISHED_RUNNER_PENDING => {
            "the same helm upgrade binds the dark factory runner image".to_string()
        }
        Some(binding) => format!(
            "the same helm upgrade binds runner image {} for {}",
            binding.image, binding.agent
        ),
        None => format!(
            "no runner image is bound, so {} runs the platform runner",
            opts.forge_egress.join(",")
        ),
    }
}

pub fn describe(planned: &Planned) -> Vec<String> {
    let add_agent = planned.path == QuickstartPath::AddAgent;
    let toolchain = if add_agent
        || planned
            .actions
            .iter()
            .any(|action| matches!(action, Action::Intake(_)))
    {
        crate::factory_toolchain::PLAN_NOTE
    } else {
        crate::factory_toolchain::SKIPPED_NOTE
    };
    let mut lines = vec![toolchain.into(), planned.context_reason.clone()];
    if add_agent {
        lines.push(format!(
            "add agent {} to the factory install on release {}: skip cluster up, App registration, and intake",
            planned.agent, planned.release
        ));
    }
    match &planned.credential {
        CredentialDecision::UseEnv => {
            lines.push("model credential: use CURIE_CREDENTIALS (no prompt)".to_string());
        }
        CredentialDecision::PreserveRecorded => {
            lines.push(
                "model credential: keep the real model already recorded on the release (no prompt)"
                    .to_string(),
            );
        }
        CredentialDecision::PromptOnce => {
            lines.push(
                "model credential: prompt once for an Anthropic API key (sk-ant-) or OpenRouter key (sk-or-) before cluster up"
                    .to_string(),
            );
        }
        CredentialDecision::RefuseNonInteractive => {
            lines.push(
                "model credential: refuse because CURIE_CREDENTIALS is unset, the release has no real model, and stdin is not a terminal"
                    .to_string(),
            );
        }
        CredentialDecision::NotNeeded => {
            lines.push(
                "model credential: not needed; the release keeps the model it records (no prompt)"
                    .to_string(),
            );
        }
    }
    if planned.skipped_cluster_up {
        lines.push(
            "skip cluster up: the release is already at the target chart and values".to_string(),
        );
    }
    for action in &planned.actions {
        match action {
            Action::External { program, args } => {
                lines.push(format!("{program} {}", args.join(" ")));
            }
            Action::Cluster { args } => {
                lines.push(format!("curie {}", args.join(" ")));
            }
            Action::Register {
                org,
                repos,
                context,
                namespace,
                release,
                model,
                rerun_flags,
            } => {
                let org_note = org
                    .as_ref()
                    .map(|org| format!(" for organization {org}"))
                    .unwrap_or_default();
                lines.push(format!(
                    "print the GitHub App registration link{org_note} and the rerun steps for {}; apply no intake",
                    repos_prose(repos)
                ));
                let mut steps = registration_steps(&RerunTarget {
                    repos,
                    context,
                    namespace,
                    release,
                    model,
                    org: org.as_deref(),
                });
                github::carry_rerun_flags(&mut steps, rerun_flags);
                lines.extend(steps);
            }
            Action::Intake(opts) => {
                let values = intake_values(opts);
                let intake = values
                    .pointer("/factory/intake")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                lines.push(format!(
                    "configure factory intake factory.intake={intake} for {} with --app-id {} --private-key-file {} and no webhook secret; {}",
                    opts.repos.join(","),
                    opts.app_id.as_deref().unwrap_or(""),
                    opts.private_key_file
                        .as_ref()
                        .map(|p| p.display().to_string())
                        .unwrap_or_default(),
                    intake_runner_clause(opts)
                ));
            }
            Action::ForgeEgress { agent } => {
                lines.push(format!(
                    "enroll {agent} for forge egress: helm upgrade --reset-then-reuse-values --set factory.sandboxEgress.{agent}=true unless the release already records it; the API does not roll"
                ));
            }
            Action::TriggerLabel { label, repos } => {
                lines.push(format!(
                    "create the trigger label {label} on {} where it is missing",
                    repos_prose(repos)
                ));
            }
            Action::Deploy {
                context,
                namespace,
                release,
                repos,
                deadline_seconds,
                budget_usd,
                agent,
                label,
                bundle,
                runner_image,
            } => {
                let dir = match bundle {
                    None => {
                        lines.push(format!(
                            "render the dark factory and deploy the published {} image",
                            crate::examples::DARK_FACTORY_RUNNER_REPOSITORY
                        ));
                        bundle_dir(namespace, release, agent)
                    }
                    Some(dir) => {
                        lines.push(match runner_image {
                            Some(image) => format!(
                                "deploy the operator bundle {} as given with its locked runner image {image}",
                                dir.display()
                            ),
                            None => format!(
                                "deploy the operator bundle {} as given on the platform runner: it declares no runner layer",
                                dir.display()
                            ),
                        });
                        dir.clone()
                    }
                };
                for args in deploy_args(
                    context,
                    namespace,
                    release,
                    repos,
                    agent,
                    label.as_deref(),
                    &dir,
                ) {
                    lines.push(format!("curie {}", args.join(" ")));
                }
                lines.push(format!(
                    "curie cluster overrides {agent} --context {context} --namespace {namespace} --release {release} --execution-deadline {deadline_seconds}"
                ));
                lines.push(format!(
                    "curie cluster publication-policy {agent} --context {context} --namespace {namespace} --release {release} --policy auto"
                ));
                lines.push(format!(
                    "curie cluster budget {agent} --context {context} --namespace {namespace} --release {release} --limit {budget_usd}"
                ));
            }
        }
    }
    lines
}

pub fn plan_touches_forbidden_tool(lines: &[String]) -> Option<String> {
    for line in lines {
        let lower = line.to_ascii_lowercase();
        // Prose may say "Open the link". Flag a browser or gh invocation, not that sentence.
        if lower.starts_with("gh ")
            || lower.starts_with("xdg-open")
            || lower.contains(" xdg-open")
            || lower.contains(" gh ")
            || lower.contains("webbrowser")
        {
            return Some(line.clone());
        }
    }
    None
}

fn check_prerequisites(
    explicit_context: Option<&str>,
    current_context: Option<&str>,
    mut on_path: impl FnMut(&str) -> bool,
) -> Result<()> {
    let tools = [
        ("docker", "https://docs.docker.com/get-docker/"),
        (
            "kind",
            "https://kind.sigs.k8s.io/docs/user/quick-start/#installation",
        ),
        ("kubectl", "https://kubernetes.io/docs/tasks/tools/"),
        ("helm", "https://helm.sh/docs/intro/install/"),
    ];
    let required = if explicit_context.is_none() && current_context.is_none() {
        &tools[..]
    } else {
        &tools[2..]
    };
    let missing: Vec<String> = required
        .iter()
        .filter(|(tool, _)| !on_path(tool))
        .map(|(tool, url)| format!("{tool}: {url}"))
        .collect();
    if missing.is_empty() {
        return Ok(());
    }
    Err(CliError::failure(format!(
        "missing required tools on PATH: {}",
        missing.join("; ")
    ))
    .with_fix("install the listed tools using their official guides, add them to PATH, and rerun")
    .into())
}

/// True when `metadata` names `chart_version` and `values` already hold the
/// quickstart install: the requested model, a real model, and gVisor off on a
/// kind target.
pub fn quickstart_release_matches(
    chart_version: &str,
    metadata: &serde_json::Value,
    values: &serde_json::Value,
    model: &str,
    kind_target: bool,
) -> bool {
    let deployed = metadata
        .get("version")
        .and_then(|value| value.as_str())
        .filter(|version| !version.is_empty())
        .or_else(|| {
            metadata
                .get("chart")
                .and_then(|value| value.as_str())
                .and_then(|chart| chart.rsplit_once('-').map(|(_, version)| version))
        });
    if deployed != Some(chart_version) {
        return false;
    }
    let recorded_model = values
        .pointer("/agentSandbox/runner/model")
        .and_then(|value| value.as_str());
    let real_model =
        values.pointer("/agentSandbox/runner/fakeModel") == Some(&serde_json::json!(false));
    let gvisor_off = values
        .pointer("/security/gvisor/mode")
        .and_then(|value| value.as_str())
        == Some("off");
    recorded_model == Some(model) && real_model && (!kind_target || gvisor_off)
}

async fn capture_helm_json(cmd: &crate::ops::OpsCommand) -> Result<Option<serde_json::Value>> {
    let (ok, stdout, _) = crate::ops::run_capture(cmd).await?;
    if !ok {
        return Ok(None);
    }
    Ok(serde_json::from_str(&stdout).ok())
}

/// Values and metadata of the revision the release is serving. `None` when
/// helm has no `deployed` revision: a failed install can still carry the
/// target chart and values, and that must not count as installed (#3934).
async fn deployed_release_snapshot(
    common: &CommonOpts,
) -> Result<Option<(serde_json::Value, serde_json::Value)>> {
    let Some(history) = capture_helm_json(&crate::ops::helm_history_cmd(common)).await? else {
        return Ok(None);
    };
    let revision = match crate::cluster_secrets::serving_revision(&history, &common.release) {
        Ok(revision) => revision,
        Err(_) => return Ok(None),
    };
    let values = capture_helm_json(&crate::cluster_secrets::helm_get_json(
        common, "values", false, revision,
    ))
    .await?;
    let metadata = capture_helm_json(&crate::cluster_secrets::helm_get_json(
        common, "metadata", false, revision,
    ))
    .await?;
    Ok(match (values, metadata) {
        (Some(values), Some(metadata)) => Some((values, metadata)),
        _ => None,
    })
}

async fn release_matches_quickstart_target(
    opts: &QuickstartOpts,
    model: &str,
    preserve_recorded_model: bool,
    kind_target: bool,
) -> Result<(bool, String)> {
    let common = CommonOpts {
        namespace: opts.namespace.clone(),
        release: opts.release.clone(),
        dry_run: false,
    };
    let chart_version = crate::ops::chart_version(&opts.chart).await?;
    let Some((values, metadata)) = deployed_release_snapshot(&common).await? else {
        return Ok((false, model.to_string()));
    };
    // With no local credential the release keeps its recorded key. Keep its
    // model too, so an Anthropic rerun cannot install an OpenRouter-only id.
    let model = if preserve_recorded_model {
        values
            .pointer("/agentSandbox/runner/model")
            .and_then(|value| value.as_str())
            .filter(|value| !value.is_empty())
            .unwrap_or(model)
    } else {
        model
    };
    let matches =
        quickstart_release_matches(&chart_version, &metadata, &values, model, kind_target);
    Ok((matches, model.to_string()))
}

/// Runs the quickstart for the selected forge. Only GitHub is installed; the
/// selection is checked again here so a caller that skipped [`select_forge`]
/// still cannot run the GitHub flow for another kind.
pub async fn quickstart(opts: QuickstartOpts) -> Result<QuickstartOutput> {
    select_forge(&opts.forge)?;
    github::quickstart(opts).await
}

async fn release_model_recorded(opts: &QuickstartOpts) -> Result<bool> {
    let common = CommonOpts {
        namespace: opts.namespace.clone(),
        release: opts.release.clone(),
        dry_run: false,
    };
    // `Ok(None)` is helm reporting the release does not exist yet. Any other
    // failure stays an error so a rerun does not prompt as if no model was set.
    Ok(match crate::ops::fetch_release_values(&common).await? {
        Some(values) => release_has_real_model(&values),
        None => false,
    })
}

/// The key whose OpenRouter credit to check. An explicit key wins because
/// `cluster up` deploys it. The saved key counts only when the release records
/// no model; otherwise `cluster up` keeps the release's recorded key (#3848)
/// and the saved key's balance would belong to a different key.
pub fn credit_check_key(
    explicit: Option<String>,
    saved: Option<String>,
    release_has_real_model: bool,
) -> Option<String> {
    explicit.or(if release_has_real_model { None } else { saved })
}

/// Warns before deploy when the model credential has less OpenRouter credit
/// than one factory run needs, because a reviewer refused for credit ends the
/// run (#3935). Never fails the command; returns the credit left when known.
async fn check_credit(release_has_real_model: bool) -> Option<f64> {
    let ui = crate::ui::ui();
    let key = match credit_check_key(
        crate::ops::explicit_model_credential_env(),
        crate::ops::saved_model_credential(),
        release_has_real_model,
    ) {
        Some(key) => key,
        None if release_has_real_model => {
            ui.note("OpenRouter credit not checked: the deployed key is the one recorded in the cluster");
            return None;
        }
        None => {
            ui.note("OpenRouter credit not checked: no local model credential");
            return None;
        }
    };
    if key.starts_with("sk-ant-") {
        return None;
    }
    match crate::openrouter_credit::remaining_credit_usd(&key).await {
        Ok(Some(left)) => {
            if left < RUN_CREDIT_USD {
                ui.warn(&format!(
                    "OpenRouter credit left: {left:.2} USD, below the {} USD one factory run needs. The reviewers run {REVIEWER_MODEL}. Add credit at https://openrouter.ai/settings/credits before labelling an issue.",
                    budget_display(RUN_CREDIT_USD)
                ));
            }
            Some(left)
        }
        Ok(None) => {
            ui.note("OpenRouter credit not checked: the key has no limit and the account balance is not readable");
            None
        }
        Err(error) => {
            // The error is built without the key; the replace is a backstop.
            let reason = format!("{error:#}").replace(&key, "<key>");
            ui.note(&format!("OpenRouter credit not checked: {reason}"));
            None
        }
    }
}

fn budget_display(value: f64) -> String {
    if value.fract() == 0.0 {
        format!("{}", value as i64)
    } else {
        value.to_string()
    }
}

fn cluster_agent(
    verb: &str,
    agent: &str,
    context: &str,
    namespace: &str,
    release: &str,
    extra: &[String],
) -> Vec<String> {
    let mut args = vec![
        "cluster".into(),
        verb.into(),
        agent.into(),
        "--context".into(),
        context.into(),
        "--namespace".into(),
        namespace.into(),
        "--release".into(),
        release.into(),
    ];
    args.extend(extra.iter().cloned());
    args
}

fn ensure_credential(decision: &CredentialDecision) -> Result<()> {
    match decision {
        CredentialDecision::UseEnv => {
            let key = crate::ops::model_credential_env()?.unwrap_or_default();
            validate_model_credential(&key)?;
            Ok(())
        }
        CredentialDecision::PreserveRecorded | CredentialDecision::NotNeeded => Ok(()),
        CredentialDecision::PromptOnce => {
            if !std::io::stdin().is_terminal() {
                return Err(CliError::usage(
                    "model credential needed and stdin is not a terminal",
                )
                .with_fix(
                    "set CURIE_CREDENTIALS to an Anthropic API key starting with sk-ant- or an OpenRouter key starting with sk-or- and rerun",
                )
                .into());
            }
            eprint!("Anthropic or OpenRouter API key: ");
            let _ = std::io::stderr().flush();
            let key = rpassword::read_password().map_err(|err| {
                CliError::usage(format!("could not read the model credential: {err}"))
                    .with_fix("set CURIE_CREDENTIALS to an Anthropic API key starting with sk-ant- or an OpenRouter key starting with sk-or-")
            })?;
            let key = key.trim().to_string();
            validate_model_credential(&key)?;
            // The child `cluster up` reads this env. The value stays out of argv.
            std::env::set_var("CURIE_CREDENTIALS", key);
            Ok(())
        }
        CredentialDecision::RefuseNonInteractive => Err(CliError::usage(
            "model credential needed and stdin is not a terminal",
        )
        .with_fix("set CURIE_CREDENTIALS to an Anthropic API key starting with sk-ant- or an OpenRouter key starting with sk-or- and rerun")
        .into()),
    }
}

async fn kind_clusters() -> Result<Vec<String>> {
    require_on_path("kind")?;
    let output = tokio::process::Command::new("kind")
        .args(["get", "clusters"])
        .output()
        .await
        .map_err(|err| CliError::failure(format!("kind get clusters failed: {err}")))?;
    if !output.status.success() {
        return Err(CliError::failure(format!(
            "kind get clusters failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ))
        .into());
    }
    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect())
}

async fn run_self(args: &[String], step: &str) -> Result<serde_json::Value> {
    let program = std::env::current_exe().map_err(|err| {
        CliError::failure(format!("cannot locate the curie binary to continue: {err}"))
    })?;
    // Structured child results let intake cross the same process boundary as
    // the other steps. Plain captured diagnostics retain every inference while
    // parent plumbing controls whether the full child detail is shown.
    let mut args = args.to_vec();
    args.extend([
        "--json".into(),
        "--debug".into(),
        "--color".into(),
        "never".into(),
    ]);
    let output = run_command(&program, &args, step).await?;
    serde_json::from_slice(&output.stdout).map_err(|error| {
        step_error(
            &program,
            &args,
            step,
            CliError::failure(format!("child result was not JSON: {error}")).into(),
            None,
        )
    })
}

async fn run_program(program: &str, args: &[String], step: &str) -> Result<()> {
    require_on_path(program)?;
    run_command(Path::new(program), args, step).await?;
    Ok(())
}

fn step_error(
    program: &Path,
    args: &[String],
    step: &str,
    source: anyhow::Error,
    fix: Option<&str>,
) -> anyhow::Error {
    let paths = regex::Regex::new(r#"(^|[\s\"'`(])(?:/|~/)[^\s\"'`<>),;]+"#)
        .expect("static quickstart path redaction pattern");
    let redact = |text: &str| {
        let mut text = text.to_string();
        if program.is_absolute() {
            text = text.replace(&program.display().to_string(), "curie");
        }
        for pair in args.windows(2) {
            if pair[0] == "--chart" || pair[0] == "--plugin-dir" {
                text = text.replace(
                    &pair[1],
                    if pair[0] == "--chart" {
                        "<chart>"
                    } else {
                        "<bundle>"
                    },
                );
            }
        }
        paths
            .replace_all(&text, "$1<path>")
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
    };
    let fix = redact(&fix.map(str::to_string).unwrap_or_else(|| {
        format!("rerun curie factory quickstart with --debug to inspect {step}; address the reported cause and rerun; completed steps are safe to repeat")
    }));
    let cause = redact(&source.to_string().replace("Error:", ""));
    let message = format!("{step} failed: {cause}");
    let source = crate::exit::with_json_payload(
        source.context(format!("{} {}", program.display(), args.join(" "))),
        serde_json::json!({ "error": message, "fix": fix }),
    );
    crate::exit::operator_context(source, message, Some(fix))
}

async fn run_command(program: &Path, args: &[String], step: &str) -> Result<Output> {
    let ui = crate::ui::ui();
    ui.note(step);
    ui.plumbing(&format!("+ {} {}", program.display(), args.join(" ")));
    let mut cmd = tokio::process::Command::new(program);
    cmd.args(args).stdout(Stdio::piped()).stderr(Stdio::piped());
    if args.first().map(String::as_str) == Some("cluster")
        && args.get(1).map(String::as_str) == Some("factory")
    {
        // Poll quickstart has no webhook secret, just as its in-process plan.
        cmd.env_remove(crate::factory_intake::WEBHOOK_SECRET_ENV);
    }
    if args.first().map(String::as_str) == Some("cluster")
        && args.get(1).map(String::as_str) == Some("up")
    {
        // The child is still `cluster up`. Quickstart does not grow `--adopt`;
        // this tells that child to name a remedy the quickstart command can use.
        cmd.env(crate::ops::QUICKSTART_NAMESPACE_HINT_ENV, "1");
    }
    let output = cmd.output().await.map_err(|error| {
        step_error(
            program,
            args,
            step,
            CliError::failure(format!("failed to start: {error}")).into(),
            None,
        )
    })?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stderr.lines() {
        if line.starts_with("Kubernetes context:") {
            continue;
        }
        // ADR 0114 Decision 4 requires each detected inference to stay visible,
        // even when the wrapper hides the child's other successful output.
        if line.starts_with("inferred ") {
            ui.note(line);
        } else {
            ui.plumbing(line);
        }
    }
    for line in stdout.lines() {
        ui.plumbing(line);
    }
    if !output.status.success() {
        let json: Option<serde_json::Value> = serde_json::from_slice(&output.stdout).ok();
        let message = json
            .as_ref()
            .and_then(|value| value.get("error"))
            .and_then(|value| value.as_str());
        // Some children report only the process status in JSON. Their final
        // captured failure diagnostic carries the actual tool rejection.
        let diagnostic = stderr.lines().rev().find_map(|line| {
            line.split_once(" failed: ")
                .or_else(|| line.split_once("Error: "))
                .map(|(_, cause)| cause)
        });
        let message = match message {
            Some(message) if message.contains("exited nonzero") => {
                diagnostic.unwrap_or(message).to_string()
            }
            Some(message) => message.to_string(),
            None => diagnostic
                .map(str::to_string)
                .unwrap_or_else(|| format!("{}{}", stderr.trim(), stdout.trim())),
        };
        let class = match output.status.code() {
            Some(2) => crate::exit::ExitClass::Usage,
            Some(3) => crate::exit::ExitClass::Transient,
            Some(4) => crate::exit::ExitClass::Unsupported,
            _ => crate::exit::ExitClass::Failure,
        };
        let fix = json
            .as_ref()
            .and_then(|value| value.get("fix"))
            .and_then(|value| value.as_str());
        let source = CliError {
            message,
            fix: fix.map(str::to_string),
            class,
        }
        .into();
        return Err(step_error(program, args, step, source, fix));
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> PlanInput {
        PlanInput {
            repos: vec!["acme/widgets".to_string()],
            app_id: None,
            private_key_file: None,
            explicit_context: None,
            current_context: None,
            kind_name: DEFAULT_KIND_NAME.into(),
            existing_kind_clusters: Vec::new(),
            namespace: "curie".into(),
            release: "curie".into(),
            model: DEFAULT_MODEL.into(),
            org: None,
            execution_deadline_seconds: DEFAULT_DEADLINE_SECONDS,
            budget_usd: DEFAULT_BUDGET_USD,
            chart: "charts/curie".into(),
            credential_in_env: false,
            release_has_real_model: false,
            interactive: true,
            release_at_target: false,
            agent: DEFAULT_AGENT.into(),
            label: None,
            plugin_dir: None,
            model_explicit: false,
            recorded: None,
            bindings: Vec::new(),
            runner_image: None,
        }
    }

    #[test]
    fn a_matching_release_skips_cluster_up_and_a_model_change_does_not() {
        let metadata = serde_json::json!({"version": "0.12.2", "chart": "curie-0.12.2"});
        let values = serde_json::json!({
            "security": {"gvisor": {"mode": "off"}},
            "agentSandbox": {"runner": {"model": DEFAULT_MODEL, "fakeModel": false}}
        });
        assert!(quickstart_release_matches(
            "0.12.2",
            &metadata,
            &values,
            DEFAULT_MODEL,
            true
        ));
        assert!(!quickstart_release_matches(
            "0.12.1",
            &metadata,
            &values,
            DEFAULT_MODEL,
            true
        ));
        assert!(!quickstart_release_matches(
            "0.12.2",
            &metadata,
            &values,
            "other/model",
            true
        ));
        let mut drifted = values.clone();
        drifted["security"]["gvisor"]["mode"] = serde_json::json!("auto");
        assert!(!quickstart_release_matches(
            "0.12.2",
            &metadata,
            &drifted,
            DEFAULT_MODEL,
            true
        ));
        assert!(quickstart_release_matches(
            "0.12.2",
            &metadata,
            &drifted,
            DEFAULT_MODEL,
            false
        ));
        let mut input = base();
        input.release_at_target = true;
        input.current_context = Some("kind-curie-factory".into());
        let lines = describe(&quickstart_plan(&input).unwrap());
        let text = lines.join("\n");
        assert!(text.contains("skip cluster up"), "{text}");
        assert!(!text.contains("curie cluster up"), "{text}");
    }

    #[test]
    fn all_prerequisites_present_accepts_both_cluster_paths() {
        for (explicit, current, expected) in [
            (None, None, vec!["docker", "kind", "kubectl", "helm"]),
            (Some("acme-cluster"), None, vec!["kubectl", "helm"]),
            (None, Some("acme-cluster"), vec!["kubectl", "helm"]),
        ] {
            let mut checked = Vec::new();
            check_prerequisites(explicit, current, |tool| {
                checked.push(tool.to_string());
                true
            })
            .unwrap();
            assert_eq!(checked, expected);
        }
    }

    #[test]
    fn one_missing_prerequisite_names_the_tool_and_official_install_url() {
        for (explicit, current) in [
            (None, None),
            (Some("acme-cluster"), None),
            (None, Some("acme-cluster")),
        ] {
            let error = check_prerequisites(explicit, current, |tool| tool != "helm").unwrap_err();
            let message = error.to_string();
            assert!(message.contains("helm"), "{message}");
            assert!(
                message.contains("https://helm.sh/docs/intro/install/"),
                "{message}"
            );
            assert!(!message.contains("docker"), "{message}");
            assert!(!message.contains("kind"), "{message}");
            assert!(!message.contains("kubectl"), "{message}");
        }
    }

    #[test]
    fn missing_kind_path_prerequisites_are_reported_together() {
        let error = check_prerequisites(None, None, |tool| tool == "helm").unwrap_err();
        let message = error.to_string();
        for expected in [
            "docker",
            "https://docs.docker.com/get-docker/",
            "kind",
            "https://kind.sigs.k8s.io/docs/user/quick-start/#installation",
            "kubectl",
            "https://kubernetes.io/docs/tasks/tools/",
        ] {
            assert!(message.contains(expected), "{message}");
        }
        assert!(!message.contains("https://helm.sh/"), "{message}");
    }

    #[test]
    fn missing_existing_context_prerequisites_are_reported_together() {
        for (explicit, current) in [(Some("acme-cluster"), None), (None, Some("acme-cluster"))] {
            let error = check_prerequisites(explicit, current, |_| false).unwrap_err();
            let message = error.to_string();
            for expected in [
                "kubectl",
                "https://kubernetes.io/docs/tasks/tools/",
                "helm",
                "https://helm.sh/docs/intro/install/",
            ] {
                assert!(message.contains(expected), "{message}");
            }
            assert!(!message.contains("docker"), "{message}");
            assert!(!message.contains("kind"), "{message}");
        }
    }

    #[test]
    fn targeted_contexts_accept_missing_docker_and_kind_including_kind_contexts() {
        for (explicit, current) in [
            (Some("acme-cluster"), None),
            (None, Some("acme-cluster")),
            (Some("kind-acme-cluster"), None),
            (None, Some("kind-acme-cluster")),
        ] {
            check_prerequisites(explicit, current, |tool| matches!(tool, "kubectl" | "helm"))
                .unwrap();
        }
    }

    #[test]
    fn no_context_creates_kind_with_gvisor_off_and_stops_at_the_link() {
        let planned = quickstart_plan(&base()).unwrap();
        let lines = describe(&planned);
        let text = lines.join("\n");
        assert!(
            text.contains("kind create cluster --name curie-factory"),
            "{text}"
        );
        assert!(
            text.contains("scale deployment/coredns --replicas=1"),
            "{text}"
        );
        assert!(text.contains("--context kind-curie-factory"), "{text}");
        assert!(text.contains("--set security.gvisor.mode=off"), "{text}");
        assert!(text.contains("--model z-ai/glm-5.3-flash"), "{text}");
        assert!(
            text.contains("print the GitHub App registration link"),
            "{text}"
        );
        assert!(!text.contains("cluster deploy"), "{text}");
        assert!(plan_touches_forbidden_tool(&lines).is_none(), "{text}");
    }

    #[test]
    fn an_existing_kind_cluster_is_not_created_again() {
        let mut input = base();
        input.existing_kind_clusters = vec!["curie-factory".into()];
        let text = describe(&quickstart_plan(&input).unwrap()).join("\n");
        assert!(!text.contains("kind create"), "{text}");
        assert!(text.contains("already exists"), "{text}");
        assert!(!text.contains("is created"), "{text}");
        assert!(
            text.contains("scale deployment/coredns --replicas=1"),
            "{text}"
        );
        assert!(text.contains("--set security.gvisor.mode=off"), "{text}");
    }

    #[test]
    fn an_explicit_context_skips_kind_and_does_not_force_gvisor_off() {
        let mut input = base();
        input.explicit_context = Some("remote-cluster".into());
        let text = describe(&quickstart_plan(&input).unwrap()).join("\n");
        assert!(!text.contains("kind "), "{text}");
        assert!(!text.contains("security.gvisor.mode=off"), "{text}");
        assert!(text.contains("--context remote-cluster"), "{text}");
        assert!(text.contains("registration link"), "{text}");
    }

    #[test]
    fn a_current_context_skips_kind_the_same_way() {
        let mut input = base();
        input.current_context = Some("k8".into());
        let text = describe(&quickstart_plan(&input).unwrap()).join("\n");
        assert!(!text.contains("kind create"), "{text}");
        assert!(text.contains("--context k8"), "{text}");
        assert!(text.contains("must confirm"), "{text}");
        assert!(!text.contains("security.gvisor.mode=off"), "{text}");
    }

    #[test]
    fn a_declined_context_confirmation_names_the_context_flag() {
        let err = context_confirmation_line("work-cluster", "n").unwrap_err();
        let shown = format!("{err:#}");
        assert!(shown.contains("--context work-cluster"), "{shown}");
        assert!(context_confirmation_line("work-cluster", "y").is_ok());
        assert!(context_confirmation_line("work-cluster", "yes").is_ok());
        assert!(context_confirmation_line("work-cluster", "").is_err());
    }

    #[test]
    fn the_second_run_enables_polling_and_deploys_the_published_image() {
        let mut input = base();
        input.app_id = Some("12345".into());
        input.private_key_file = Some("/keys/app.pem".into());
        input.explicit_context = Some("kind-curie-factory".into());
        input.credential_in_env = true;
        let planned = quickstart_plan(&input).unwrap();
        assert!(planned.actions.iter().any(|action| matches!(
            action,
            Action::Intake(opts) if opts.intake.as_deref() == Some("poll")
                && opts.webhook_secret.is_none()
                && opts.repos == vec!["acme/widgets".to_string()]
        )));
        let lines = describe(&planned);
        let text = lines.join("\n");
        assert!(text.contains("factory.intake=poll"), "{text}");
        assert!(text.contains("no webhook secret"), "{text}");
        assert!(
            text.contains("ghcr.io/curie-eng/curie-dark-factory-runner"),
            "{text}"
        );
        assert!(text.contains("--agent dark-factory"), "{text}");
        assert!(text.contains("--env prod"), "{text}");
        assert!(text.contains("--repo acme/widgets"), "{text}");
        assert!(text.contains("--add github=acme/widgets"), "{text}");
        assert!(text.contains("--execution-deadline 10800"), "{text}");
        assert!(text.contains("--policy auto"), "{text}");
        assert!(text.contains("--limit 5"), "{text}");
        assert!(text.contains("use CURIE_CREDENTIALS (no prompt)"), "{text}");
        assert!(!text.contains("registration link"), "{text}");
        assert!(plan_touches_forbidden_tool(&lines).is_none(), "{text}");
    }

    #[test]
    fn registration_steps_name_the_rerun_and_no_browser_tool() {
        let steps = registration_steps(&RerunTarget {
            repos: &["acme/widgets".to_string()],
            context: "kind-curie-factory",
            namespace: "factory-ns",
            release: "factory",
            model: DEFAULT_MODEL,
            org: None,
        });
        let text = steps.join("\n");
        assert!(text.contains(
            "curie factory quickstart --repo acme/widgets --context kind-curie-factory --namespace factory-ns --release factory --app-id <APP_ID> --private-key-file <PATH.pem>"
        ));
        assert!(!text.contains("--model"));
        assert!(!text.to_ascii_lowercase().contains("gh "));
        assert!(!text.contains("webhook secret"));
        let url = crate::factory_app::registration_url(None, "curie-factory-abcdef12");
        assert!(url.contains("webhook_active=false"), "{url}");
    }

    #[test]
    fn the_printed_rerun_keeps_an_explicit_context() {
        let mut input = base();
        input.explicit_context = Some("remote-cluster".into());
        input.namespace = "acme".into();
        input.release = "trial".into();
        let text = describe(&quickstart_plan(&input).unwrap()).join("\n");
        assert!(text.contains(
            "--repo acme/widgets --context remote-cluster --namespace acme --release trial --app-id"
        ), "{text}");
        let rerun = text
            .lines()
            .find(|line| line.starts_with("Rerun:"))
            .unwrap();
        assert!(!rerun.contains("--model"), "{rerun}");
        assert!(!text.contains("kind create"), "{text}");
    }

    #[test]
    fn a_recorded_model_is_not_prompted_again() {
        assert_eq!(
            credential_decision(false, true, true),
            CredentialDecision::PreserveRecorded
        );
        assert_eq!(
            credential_decision(true, false, true),
            CredentialDecision::UseEnv
        );
        assert_eq!(
            credential_decision(false, false, true),
            CredentialDecision::PromptOnce
        );
        assert_eq!(
            credential_decision(false, false, false),
            CredentialDecision::RefuseNonInteractive
        );
    }

    #[test]
    fn factory_quickstart_accepts_direct_anthropic_and_openrouter_credentials() {
        // These are synthetic prefix shapes accepted by the runner's
        // sdk_auth.py, including the subscription OAuth shape it supports.
        for key in [
            "sk-or-PLACEHOLDER",
            "sk-ant-api03-PLACEHOLDER",
            "sk-ant-oat01-PLACEHOLDER",
        ] {
            assert!(validate_model_credential(key).is_ok());
        }
    }

    #[test]
    fn factory_quickstart_rejects_invalid_and_empty_credential_prefixes() {
        for key in ["", "sk-or-", "sk-ant-", "sk-ant", "invalid-credential"] {
            assert!(validate_model_credential(key).is_err());
        }
    }

    #[test]
    fn a_malformed_repo_or_half_an_app_pair_is_refused() {
        let mut input = base();
        input.repos = vec!["acme".to_string()];
        assert!(quickstart_plan(&input).is_err());
        input.repos = vec!["acme/widgets".to_string()];
        input.app_id = Some("12".into());
        assert!(quickstart_plan(&input).is_err());
        input.private_key_file = Some("/tmp/app.pem".into());
        input.app_id = Some("nope".into());
        assert!(quickstart_plan(&input).is_err());
    }

    #[test]
    fn real_model_detection_reads_fake_model_false_only() {
        assert!(release_has_real_model(
            &serde_json::json!({"agentSandbox": {"runner": {"fakeModel": false}}})
        ));
        assert!(!release_has_real_model(
            &serde_json::json!({"agentSandbox": {"runner": {"fakeModel": true}}})
        ));
        assert!(!release_has_real_model(&serde_json::json!({})));
    }

    fn multi_repo_input() -> PlanInput {
        let mut input = base();
        input.repos = vec!["a/x".to_string(), "b/y".to_string()];
        input.app_id = Some("12345".into());
        input.private_key_file = Some("/keys/app.pem".into());
        input.explicit_context = Some("kind-curie-factory".into());
        input.credential_in_env = true;
        input
    }

    #[test]
    fn quickstart_multi_repo_plan_intakes_both_deploys_first_and_binds_each() {
        let planned = quickstart_plan(&multi_repo_input()).unwrap();
        let both = vec!["a/x".to_string(), "b/y".to_string()];
        assert!(planned
            .actions
            .iter()
            .any(|action| matches!(action, Action::Intake(opts) if opts.repos == both)));
        assert!(planned
            .actions
            .iter()
            .any(|action| matches!(action, Action::Deploy { repos, .. } if *repos == both)));
        let lines = describe(&planned);
        let text = lines.join("\n");
        let deploy: Vec<&String> = lines
            .iter()
            .filter(|l| l.starts_with("curie cluster deploy "))
            .collect();
        assert_eq!(deploy.len(), 1, "{text}");
        assert!(
            deploy[0].ends_with("--env prod --repo a/x"),
            "{}",
            deploy[0]
        );
        assert!(!deploy[0].contains("b/y"), "{}", deploy[0]);
        let surfaces: Vec<&String> = lines
            .iter()
            .filter(|l| l.starts_with("curie cluster surfaces "))
            .collect();
        assert_eq!(surfaces.len(), 2, "{text}");
        assert!(surfaces[0].ends_with("--add github=a/x"), "{}", surfaces[0]);
        assert!(surfaces[1].ends_with("--add github=b/y"), "{}", surfaces[1]);
        assert!(
            text.contains("factory intake factory.intake=poll for a/x,b/y with"),
            "{text}"
        );
    }

    #[test]
    fn quickstart_multi_repo_single_repo_plan_is_unchanged() {
        let mut input = base();
        input.app_id = Some("12345".into());
        input.private_key_file = Some("/keys/app.pem".into());
        input.explicit_context = Some("kind-curie-factory".into());
        input.credential_in_env = true;
        let lines = describe(&quickstart_plan(&input).unwrap());
        let dir = bundle_dir("curie", "curie", DEFAULT_AGENT);
        // The complete ordered single-repository plan, retaining the stable deadline fix.
        let expected: Vec<String> = vec![
            crate::factory_toolchain::PLAN_NOTE.to_string(),
            "Kubernetes context: kind-curie-factory because --context was passed".to_string(),
            "model credential: use CURIE_CREDENTIALS (no prompt)".to_string(),
            "kubectl --context kind-curie-factory -n kube-system scale deployment/coredns --replicas=1".to_string(),
            "curie cluster up --context kind-curie-factory --namespace curie --release curie --model z-ai/glm-5.3-flash --chart charts/curie --set security.gvisor.mode=off".to_string(),
            "configure factory intake factory.intake=poll for acme/widgets with --app-id 12345 --private-key-file /keys/app.pem and no webhook secret; the same helm upgrade binds the dark factory runner image".to_string(),
            "render the dark factory and deploy the published ghcr.io/curie-eng/curie-dark-factory-runner image".to_string(),
            format!("curie cluster deploy --context kind-curie-factory --namespace curie --release curie --plugin-dir {} --agent dark-factory --env prod --repo acme/widgets", dir.display()),
            "curie cluster surfaces dark-factory --context kind-curie-factory --namespace curie --release curie --add github=acme/widgets".to_string(),
            "curie cluster overrides dark-factory --context kind-curie-factory --namespace curie --release curie --execution-deadline 10800".to_string(),
            "curie cluster publication-policy dark-factory --context kind-curie-factory --namespace curie --release curie --policy auto".to_string(),
            "curie cluster budget dark-factory --context kind-curie-factory --namespace curie --release curie --limit 5".to_string(),
        ];
        assert_eq!(lines, expected);
        let ready = QuickstartOutput::Ready {
            context: "kind-curie-factory".into(),
            repo: "acme/widgets".into(),
            intake: POLL_INTAKE.into(),
            app_id: "12345".into(),
            slug: "acme-factory".into(),
            mention: "@acme-factory".into(),
            mention_inferred: true,
            repos: vec!["acme/widgets".to_string()],
            label: "factory".into(),
            label_inferred: true,
            runner_image: Some("runner:dev".into()),
            agent: DEFAULT_AGENT.into(),
            trigger_label: None,
            effective_label: "factory".into(),
            deadline_seconds: DEFAULT_DEADLINE_SECONDS,
            budget_usd: DEFAULT_BUDGET_USD,
            credit_remaining_usd: None,
        };
        let json = crate::ui::CliOutput::to_json(&ready);
        assert_eq!(json["repo"], "acme/widgets");
        assert_eq!(
            json["github_app"]["repos"],
            serde_json::json!(["acme/widgets"])
        );
        let steps = registration_steps(&RerunTarget {
            repos: &["acme/widgets".to_string()],
            context: "kind-curie-factory",
            namespace: "curie",
            release: "curie",
            model: DEFAULT_MODEL,
            org: None,
        });
        assert_eq!(
            steps[2],
            "Click Install App and install it on acme/widgets."
        );
        assert_eq!(
            steps[3],
            "Rerun: curie factory quickstart --repo acme/widgets --context kind-curie-factory --app-id <APP_ID> --private-key-file <PATH.pem>"
        );
    }

    #[test]
    fn quickstart_multi_repo_rejects_a_duplicate_and_an_invalid_entry() {
        let mut input = base();
        input.repos = vec!["a/x".to_string(), "A/X".to_string()];
        let err = quickstart_plan(&input).unwrap_err();
        let shown = format!("{err:#}");
        assert!(shown.contains("A/X"), "{shown}");
        input.repos = vec!["a/x".to_string(), "b/y".to_string(), "a/x".to_string()];
        let shown = format!("{:#}", quickstart_plan(&input).unwrap_err());
        assert!(shown.contains("a/x"), "{shown}");
        for bad in [
            vec!["a/x".to_string(), "bogus".to_string()],
            vec!["a/x".to_string(), String::new()],
            vec!["a/x".to_string(), "b/y/z".to_string()],
            Vec::new(),
        ] {
            input.repos = bad.clone();
            assert!(quickstart_plan(&input).is_err(), "{bad:?}");
        }
        // A trailing comma in `--repo a/x,` reaches the plan as an empty entry.
        input.repos = "a/x,".split(',').map(str::to_string).collect();
        assert!(quickstart_plan(&input).is_err());
        // Two distinct repositories still plan.
        input.repos = vec!["a/x".to_string(), "b/y".to_string()];
        assert!(quickstart_plan(&input).is_ok());
    }

    #[test]
    fn quickstart_multi_repo_intake_passes_one_repo_flag_per_entry() {
        let planned = quickstart_plan(&multi_repo_input()).unwrap();
        let intake = planned
            .actions
            .iter()
            .find_map(|action| match action {
                Action::Intake(opts) => Some(opts),
                _ => None,
            })
            .expect("intake action");
        let args = intake_args("kind-curie-factory", intake, "ghcr.io/x/runner:1");
        let repo_values: Vec<&str> = args
            .iter()
            .enumerate()
            .filter(|(_, a)| a.as_str() == "--repo")
            .map(|(i, _)| args[i + 1].as_str())
            .collect();
        assert_eq!(repo_values, ["a/x", "b/y"], "{args:?}");
        assert!(!args.iter().any(|a| a.contains(',')), "{args:?}");
        assert_eq!(&args[..2], ["cluster", "factory"]);
        let pos = |flag: &str| args.iter().position(|a| a == flag).unwrap();
        assert_eq!(args[pos("--context") + 1], "kind-curie-factory");
        assert_eq!(args[pos("--intake") + 1], "poll");
        assert_eq!(args[pos("--app-id") + 1], "12345");
        assert_eq!(args[pos("--private-key-file") + 1], "/keys/app.pem");
        assert_eq!(
            args[pos("--runner-image") + 1],
            "dark-factory=ghcr.io/x/runner:1"
        );
    }

    #[test]
    fn quickstart_multi_repo_deploy_binds_every_repository_and_deploys_the_first() {
        let repos = vec!["a/x".to_string(), "b/y".to_string()];
        let argvs = deploy_args(
            "ctx",
            "ns",
            "rel",
            &repos,
            DEFAULT_AGENT,
            None,
            &bundle_dir("ns", "rel", DEFAULT_AGENT),
        );
        assert_eq!(argvs.len(), 3, "{argvs:?}");
        assert_eq!(&argvs[0][..2], ["cluster", "deploy"]);
        let repo_flags: Vec<&str> = argvs[0]
            .iter()
            .enumerate()
            .filter(|(_, a)| a.as_str() == "--repo")
            .map(|(i, _)| argvs[0][i + 1].as_str())
            .collect();
        assert_eq!(repo_flags, ["a/x"], "{:?}", argvs[0]);
        assert_eq!(argvs[0].last().unwrap(), "a/x");
        assert!(argvs[0]
            .windows(2)
            .any(|w| w == ["--agent", "dark-factory"]));
        assert!(argvs[0].windows(2).any(|w| w == ["--env", "prod"]));
        let expected = |repo: &str| {
            cluster_agent(
                "surfaces",
                DEFAULT_AGENT,
                "ctx",
                "ns",
                "rel",
                &["--add".to_string(), format!("github={repo}")],
            )
        };
        assert_eq!(argvs[1], expected("a/x"));
        assert_eq!(argvs[2], expected("b/y"));
        assert_eq!(
            argvs[1],
            [
                "cluster",
                "surfaces",
                "dark-factory",
                "--context",
                "ctx",
                "--namespace",
                "ns",
                "--release",
                "rel",
                "--add",
                "github=a/x"
            ]
        );
        // One repository keeps one deploy and one surfaces argv.
        assert_eq!(
            deploy_args(
                "ctx",
                "ns",
                "rel",
                &repos[..1],
                DEFAULT_AGENT,
                None,
                &bundle_dir("ns", "rel", DEFAULT_AGENT)
            )
            .len(),
            2
        );
    }

    #[test]
    fn quickstart_multi_repo_registration_names_every_repository() {
        let repos = vec!["a/x".to_string(), "b/y".to_string()];
        let steps = registration_steps(&RerunTarget {
            repos: &repos,
            context: "kind-curie-factory",
            namespace: "curie",
            release: "curie",
            model: DEFAULT_MODEL,
            org: None,
        });
        let install = steps
            .iter()
            .find(|s| s.starts_with("Click Install App"))
            .unwrap();
        assert!(
            install.contains("a/x") && install.contains("b/y"),
            "{install}"
        );
        assert!(install.contains("a/x and b/y"), "{install}");
        let rerun = steps.iter().find(|s| s.starts_with("Rerun:")).unwrap();
        assert!(
            rerun.starts_with(
                "Rerun: curie factory quickstart --repo a/x,b/y --context kind-curie-factory"
            ),
            "{rerun}"
        );
        // The first pass (no App id) prints the same through describe().
        let mut input = base();
        input.repos = repos;
        let text = describe(&quickstart_plan(&input).unwrap()).join("\n");
        assert!(text.contains("--repo a/x,b/y --context"), "{text}");
        assert!(text.contains("a/x and b/y"), "{text}");
    }

    // #4397 AC 6: the egress-write helper shared by the per-forge modules. An
    // IP or CIDR passes through; a hostname resolves on this machine to one
    // host route per address, with the hostname recorded beside it.
    fn egress_json(entries: &[EgressEntry]) -> serde_json::Value {
        serde_json::to_value(entries).expect("egress entries serialize to chart values")
    }

    #[test]
    fn forge_egress_entries_resolves_a_hostname_to_host_routes_and_records_the_host() {
        let resolve = |host: &str| -> Result<Vec<std::net::IpAddr>> {
            assert_eq!(host, "git.internal.example");
            Ok(vec![
                "10.20.0.5".parse().unwrap(),
                "10.20.0.6".parse().unwrap(),
                "fd12:3456::7".parse().unwrap(),
            ])
        };
        let entries = forge_egress_entries("git.internal.example", &[8443, 22], resolve)
            .expect("a resolving hostname gives host routes");
        let ports = serde_json::json!([
            {"protocol": "TCP", "port": 8443},
            {"protocol": "TCP", "port": 22}
        ]);
        assert_eq!(
            egress_json(&entries),
            serde_json::json!([
                {"cidr": "10.20.0.5/32", "ports": ports, "host": "git.internal.example"},
                {"cidr": "10.20.0.6/32", "ports": ports, "host": "git.internal.example"},
                {"cidr": "fd12:3456::7/128", "ports": ports, "host": "git.internal.example"},
            ])
        );
    }

    #[test]
    fn forge_egress_entries_passes_an_ip_and_a_cidr_through() {
        let never = |host: &str| -> Result<Vec<std::net::IpAddr>> {
            panic!("an address must not be resolved, got {host}")
        };
        let ports = serde_json::json!([{"protocol": "TCP", "port": 443}]);
        for (target, cidr) in [
            ("10.20.0.5", "10.20.0.5/32"),
            ("192.168.40.0/24", "192.168.40.0/24"),
            ("2001:db8::9", "2001:db8::9/128"),
            ("2001:db8:1::/48", "2001:db8:1::/48"),
        ] {
            let entries = forge_egress_entries(target, &[443], never)
                .unwrap_or_else(|error| panic!("{target}: {error:#}"));
            assert_eq!(
                egress_json(&entries),
                serde_json::json!([{"cidr": cidr, "ports": ports}]),
                "{target} passes through with no host recorded"
            );
        }
    }

    #[test]
    fn forge_egress_entries_refuses_a_name_that_resolves_to_nothing() {
        let nothing = |_: &str| -> Result<Vec<std::net::IpAddr>> { Ok(Vec::new()) };
        let error = forge_egress_entries("gone.internal.example", &[443], nothing)
            .expect_err("a hostname with no address must be refused");
        assert!(
            format!("{error:#}").contains("gone.internal.example"),
            "the refusal names the hostname: {error:#}"
        );
        let failing =
            |_: &str| -> Result<Vec<std::net::IpAddr>> { Err(anyhow::anyhow!("NXDOMAIN")) };
        let error = forge_egress_entries("gone.internal.example", &[443], failing)
            .expect_err("a resolver failure must be refused");
        assert!(
            format!("{error:#}").contains("gone.internal.example"),
            "{error:#}"
        );
    }

    #[test]
    fn the_finishing_intake_enrolls_the_deployed_agent_for_forge_egress() {
        // The quickstart writes factory.sandboxEgress.<agent> for the agent it
        // deploys and no CIDR list: GitHub egress is the chart's built-in set.
        let mut input = base();
        input.app_id = Some("12345".into());
        input.private_key_file = Some("/keys/app.pem".into());
        input.explicit_context = Some("kind-curie-factory".into());
        let planned = quickstart_plan(&input).unwrap();
        let intake = planned
            .actions
            .iter()
            .find_map(|action| match action {
                Action::Intake(opts) => Some(opts),
                _ => None,
            })
            .expect("the finishing pass has an intake action");
        assert_eq!(intake.forge_egress, vec![DEFAULT_AGENT.to_string()]);
        let values = intake_values(intake);
        assert_eq!(
            values["factory"]["sandboxEgress"],
            serde_json::json!({ (DEFAULT_AGENT): true }),
            "{values}"
        );
        let args = intake_args("kind-curie-factory", intake, "ghcr.io/x/runner:1");
        let flag = args
            .iter()
            .position(|a| a == "--forge-egress")
            .unwrap_or_else(|| panic!("the intake subprocess must pass --forge-egress: {args:?}"));
        assert_eq!(args[flag + 1], DEFAULT_AGENT, "{args:?}");
        assert!(!args.iter().any(|a| a == "--github-api-egress"), "{args:?}");
    }

    // ---- #4362: a second agent through the quickstart ----

    fn recorded() -> RecordedIntake {
        RecordedIntake {
            app_id: "12345".into(),
            install_label: "curie-factory".into(),
            repo_allowlist: vec!["acme/widgets".into()],
            pem_secret: "factory-app".into(),
            pem_secret_key: "private-key.pem".into(),
        }
    }

    fn binding(agent: &str, repo: &str, label: Option<&str>) -> AgentBinding {
        AgentBinding {
            agent: agent.into(),
            repo: repo.into(),
            trigger_label: label.map(str::to_string),
        }
    }

    /// A release that records factory intake, no App flags: the add agent path.
    fn add_input() -> PlanInput {
        let mut input = base();
        input.explicit_context = Some("k8".into());
        input.namespace = "factory-ns".into();
        input.release = "factory".into();
        input.credential_in_env = true;
        input.agent = "dark-factory-fast".into();
        input.label = Some("curie-factory-fast".into());
        input.recorded = Some(recorded());
        input
    }

    fn plan_text(input: &PlanInput) -> String {
        describe(&quickstart_plan(input).unwrap()).join("\n")
    }

    fn refusal(input: &PlanInput) -> String {
        format!("{:#}", quickstart_plan(input).unwrap_err())
    }

    #[test]
    fn agent_add_path_plans_no_kind_coredns_cluster_up_registration_or_intake() {
        let mut input = add_input();
        input.plugin_dir = Some("/ops/fast-bundle".into());
        let planned = quickstart_plan(&input).unwrap();
        assert!(planned.kind_cluster.is_none());
        assert!(!planned.actions.iter().any(|action| matches!(
            action,
            Action::External { .. }
                | Action::Cluster { .. }
                | Action::Register { .. }
                | Action::Intake(_)
        )));
        let text = describe(&planned).join("\n");
        assert!(
            text.contains("add agent dark-factory-fast to the factory install on release factory: skip cluster up, App registration, and intake"),
            "{text}"
        );
        for banned in [
            "kind create",
            "coredns",
            "curie cluster up",
            "registration link",
            "factory.intake=",
        ] {
            assert!(!text.contains(banned), "{banned}: {text}");
        }
        let order: Vec<usize> = [
            "cluster deploy",
            "cluster surfaces dark-factory-fast",
            "cluster overrides dark-factory-fast",
            "cluster publication-policy dark-factory-fast",
            "cluster budget dark-factory-fast",
        ]
        .iter()
        .map(|needle| {
            text.find(needle)
                .unwrap_or_else(|| panic!("{needle}: {text}"))
        })
        .collect();
        assert!(order.windows(2).all(|pair| pair[0] < pair[1]), "{text}");
        assert!(text.contains("--agent dark-factory-fast"), "{text}");
        assert!(text.contains("--plugin-dir /ops/fast-bundle"), "{text}");
        assert!(
            text.contains("--add github=acme/widgets --label curie-factory-fast"),
            "{text}"
        );
        // Liveness: the same input without recorded intake is the register path.
        let mut register = add_input();
        register.recorded = None;
        let planned = quickstart_plan(&register).unwrap();
        assert!(planned
            .actions
            .iter()
            .any(|action| matches!(action, Action::Register { .. })));
        // The add path also applies to the default agent name.
        let mut default = add_input();
        default.agent = DEFAULT_AGENT.into();
        default.label = None;
        let text = plan_text(&default);
        assert!(
            text.contains("add agent dark-factory to the factory install"),
            "{text}"
        );
    }

    #[test]
    fn agent_same_effective_label_on_another_agent_is_refused_naming_it() {
        let mut input = add_input();
        input.bindings = vec![binding(
            "dark-factory-fast",
            "acme/widgets",
            Some("curie-factory-fast"),
        )];
        input.agent = "dark-factory-dup".into();
        let error = refusal(&input);
        assert!(error.contains("dark-factory-fast"), "{error}");
        assert!(error.contains("--label"), "{error}");
        // Liveness: a different label on the same repository plans.
        input.label = Some("curie-factory-dup".into());
        quickstart_plan(&input).unwrap();
        // Liveness: the same label on another repository plans.
        input.label = Some("curie-factory-fast".into());
        input.bindings = vec![binding(
            "dark-factory-fast",
            "acme/other",
            Some("curie-factory-fast"),
        )];
        quickstart_plan(&input).unwrap();
    }

    #[test]
    fn agent_explicit_label_equal_to_install_label_beside_null_binding_is_refused() {
        let mut input = add_input();
        input.bindings = vec![binding("dark-factory", "acme/widgets", None)];
        input.label = Some("curie-factory".into());
        let error = refusal(&input);
        assert!(error.contains("dark-factory"), "{error}");
        // The omitted label is the install label too.
        input.label = None;
        let error = refusal(&input);
        assert!(error.contains("dark-factory"), "{error}");
        // Liveness: a distinct label beside the null binding plans.
        input.label = Some("curie-factory-fast".into());
        quickstart_plan(&input).unwrap();
    }

    #[test]
    fn agent_own_binding_same_label_plans() {
        let mut input = add_input();
        input.bindings = vec![binding(
            "dark-factory-fast",
            "acme/widgets",
            Some("curie-factory-fast"),
        )];
        let text = plan_text(&input);
        assert!(
            text.contains("--add github=acme/widgets --label curie-factory-fast"),
            "{text}"
        );
        // An own null binding equals an explicit install label.
        input.bindings = vec![binding("dark-factory-fast", "acme/widgets", None)];
        input.label = Some("curie-factory".into());
        quickstart_plan(&input).unwrap();
        input.label = None;
        quickstart_plan(&input).unwrap();
    }

    #[test]
    fn agent_own_binding_other_label_is_refused_naming_surfaces_remove() {
        let mut input = add_input();
        input.bindings = vec![binding(
            "dark-factory-fast",
            "acme/widgets",
            Some("old-label"),
        )];
        let error = refusal(&input);
        assert!(
            error.contains("curie cluster surfaces dark-factory-fast --remove github=acme/widgets"),
            "{error}"
        );
        // Null binding against a distinct requested label is the same refusal.
        input.bindings = vec![binding("dark-factory-fast", "acme/widgets", None)];
        let error = refusal(&input);
        assert!(error.contains("--remove github=acme/widgets"), "{error}");
        // Liveness: after the remove (no binding) it plans.
        input.bindings = Vec::new();
        quickstart_plan(&input).unwrap();
    }

    #[test]
    fn agent_repo_outside_recorded_allowlist_is_refused() {
        let mut input = add_input();
        input.repos = vec!["acme/widgets".into(), "acme/gadgets".into()];
        let error = refusal(&input);
        assert!(
            error.contains("curie cluster factory --repo acme/widgets,acme/gadgets"),
            "{error}"
        );
        // Liveness: an owner wildcard admits it.
        input.recorded.as_mut().unwrap().repo_allowlist = vec!["acme/*".into()];
        quickstart_plan(&input).unwrap();
    }

    #[test]
    fn agent_install_path_dropping_a_recorded_allowlist_entry_is_refused() {
        let mut input = add_input();
        input.app_id = Some("12345".into());
        input.private_key_file = Some("/keys/app.pem".into());
        input.recorded.as_mut().unwrap().repo_allowlist =
            vec!["acme/widgets".into(), "acme/gadgets".into()];
        let error = refusal(&input);
        assert!(error.contains("acme/gadgets"), "{error}");
        // Liveness: listing every recorded entry plans an intake.
        input.repos = vec!["acme/widgets".into(), "acme/gadgets".into()];
        let planned = quickstart_plan(&input).unwrap();
        assert!(planned
            .actions
            .iter()
            .any(|action| matches!(action, Action::Intake(_))));
    }

    #[test]
    fn agent_invalid_name_self_and_labels_are_refused() {
        let long = "a".repeat(41);
        for name in ["Bad_Name", "self", "", long.as_str(), "-lead"] {
            let mut input = add_input();
            input.agent = name.into();
            let error = refusal(&input);
            assert!(error.contains("agent"), "{name:?}: {error}");
        }
        let too_long = "l".repeat(51);
        for label in [
            "has space",
            "",
            too_long.as_str(),
            "curie-factory:x",
            "base:main",
            "repo:acme",
        ] {
            let mut input = add_input();
            input.label = Some(label.into());
            let error = refusal(&input);
            assert!(error.contains("--label"), "{label:?}: {error}");
        }
        // The name check runs on the register path as well.
        let mut input = base();
        input.agent = "self".into();
        assert!(quickstart_plan(&input).is_err());
        // Liveness: a valid 40 character name and a 50 character label plan.
        let mut input = add_input();
        input.agent = "a".repeat(40);
        input.label = Some("l".repeat(50));
        quickstart_plan(&input).unwrap();
    }

    #[test]
    fn agent_model_and_org_are_refused_on_add_path() {
        let mut input = add_input();
        input.model_explicit = true;
        let error = refusal(&input);
        assert!(error.contains("curie cluster up --model"), "{error}");
        let mut input = add_input();
        input.org = Some("acme".into());
        let error = refusal(&input);
        assert!(error.contains("curie cluster github-app"), "{error}");
        // Liveness: the register path still accepts both.
        let mut input = base();
        input.model_explicit = true;
        input.org = Some("acme".into());
        quickstart_plan(&input).unwrap();
    }

    #[test]
    fn agent_locked_runner_yields_runner_image_flag() {
        let mut input = base();
        input.app_id = Some("12345".into());
        input.private_key_file = Some("/keys/app.pem".into());
        input.explicit_context = Some("kind-curie-factory".into());
        input.agent = "dark-factory-fast".into();
        input.plugin_dir = Some("/ops/fast-bundle".into());
        input.runner_image = Some("registry.example/fast-runner@sha256:abc".into());
        let planned = quickstart_plan(&input).unwrap();
        let intake = planned
            .actions
            .iter()
            .find_map(|action| match action {
                Action::Intake(opts) => Some(opts),
                _ => None,
            })
            .expect("the install path has an intake");
        assert_eq!(intake.forge_egress, vec!["dark-factory-fast".to_string()]);
        assert_eq!(
            intake.runner_binding,
            Some(crate::factory_intake::RunnerImageBinding {
                agent: "dark-factory-fast".into(),
                image: "registry.example/fast-runner@sha256:abc".into(),
            })
        );
        let args = intake_args(
            "kind-curie-factory",
            intake,
            "registry.example/fast-runner@sha256:abc",
        );
        let flag = args.iter().position(|a| a == "--runner-image").unwrap();
        assert_eq!(
            args[flag + 1],
            "dark-factory-fast=registry.example/fast-runner@sha256:abc"
        );
        let text = describe(&planned).join("\n");
        assert!(
            text.contains("registry.example/fast-runner@sha256:abc"),
            "{text}"
        );
        // Liveness: a bundle with no runner passes no --runner-image and says
        // the platform runner serves the agent.
        input.runner_image = None;
        let planned = quickstart_plan(&input).unwrap();
        let intake = planned
            .actions
            .iter()
            .find_map(|action| match action {
                Action::Intake(opts) => Some(opts),
                _ => None,
            })
            .unwrap();
        assert!(intake.runner_binding.is_none());
        let text = describe(&planned).join("\n");
        assert!(text.contains("platform runner"), "{text}");
        assert!(!text.contains("curie-dark-factory-runner"), "{text}");
    }

    #[test]
    fn agent_bundle_without_runner_yields_platform_runner_line() {
        let mut input = add_input();
        input.plugin_dir = Some("/ops/fast-bundle".into());
        let text = plan_text(&input);
        assert!(text.contains("platform runner"), "{text}");
        assert!(!text.contains("--runner-image"), "{text}");
        // Liveness: a locked image names itself instead of the platform runner.
        input.runner_image = Some("registry.example/fast-runner@sha256:abc".into());
        let text = plan_text(&input);
        assert!(
            text.contains("registry.example/fast-runner@sha256:abc"),
            "{text}"
        );
        assert!(!text.contains("platform runner"), "{text}");
    }

    #[test]
    fn agent_bundle_dir_differs_per_agent_and_is_unchanged_for_default() {
        let default = bundle_dir("ns", "rel", DEFAULT_AGENT);
        let fast = bundle_dir("ns", "rel", "dark-factory-fast");
        assert_ne!(default, fast);
        assert_eq!(
            default,
            std::env::temp_dir()
                .join("curie-factory-quickstart-ns-rel")
                .join("dark-factory")
        );
        assert_eq!(fast.parent(), default.parent());
        assert_eq!(fast.file_name().unwrap(), "dark-factory-fast");
    }

    #[test]
    fn agent_register_rerun_line_carries_non_default_flags() {
        let mut input = base();
        input.explicit_context = Some("remote-cluster".into());
        input.agent = "dark-factory-fast".into();
        input.label = Some("curie-factory-fast".into());
        input.plugin_dir = Some("/ops/fast-bundle".into());
        let text = plan_text(&input);
        let rerun = text.lines().find(|l| l.starts_with("Rerun:")).unwrap();
        for flag in [
            "--agent dark-factory-fast",
            "--label curie-factory-fast",
            "--plugin-dir /ops/fast-bundle",
        ] {
            assert!(rerun.contains(flag), "{flag}: {rerun}");
        }
        // Liveness: defaults add none of the three flags.
        let mut input = base();
        input.explicit_context = Some("remote-cluster".into());
        let text = plan_text(&input);
        let rerun = text.lines().find(|l| l.starts_with("Rerun:")).unwrap();
        for flag in ["--agent", "--label", "--plugin-dir"] {
            assert!(!rerun.contains(flag), "{flag}: {rerun}");
        }
    }

    #[test]
    fn agent_add_path_binds_forge_egress_before_deploy() {
        let text = plan_text(&add_input());
        let egress = text
            .lines()
            .position(|l| l.contains("factory.sandboxEgress.dark-factory-fast"))
            .unwrap_or_else(|| panic!("no forge egress line: {text}"));
        let deploy = text
            .lines()
            .position(|l| l.contains("cluster deploy"))
            .unwrap();
        assert!(egress < deploy, "{text}");
        // The install path enrolls through the intake instead and plans no
        // separate bind.
        let mut input = base();
        input.app_id = Some("12345".into());
        input.private_key_file = Some("/keys/app.pem".into());
        input.explicit_context = Some("kind-curie-factory".into());
        let text = plan_text(&input);
        assert!(
            !text.contains("factory.sandboxEgress.dark-factory "),
            "{text}"
        );
    }

    #[test]
    fn agent_recorded_intake_reads_the_factory_block() {
        let values = serde_json::json!({
            "factory": {
                "enabled": true,
                "intake": "poll",
                "trackers": [{"kind": "github", "label": "curie-factory"}],
                "codeHosts": [{"kind": "github", "credential": {
                    "appId": "12345",
                    "existingSecret": "factory-app",
                    "existingSecretKey": "private-key.pem"
                }}]
            },
            "api": {"githubRepoAllowlist": ["acme/widgets"]}
        });
        assert_eq!(recorded_intake(&values), Some(recorded()));
        // Liveness: each missing piece is "not recorded".
        for pointer in [
            "/factory/enabled",
            "/factory/intake",
            "/factory/trackers/0/label",
            "/factory/codeHosts/0/credential/appId",
            "/factory/codeHosts/0/credential/existingSecret",
        ] {
            let mut broken = values.clone();
            let (parent, key) = pointer.rsplit_once('/').unwrap();
            let slot = broken.pointer_mut(parent).unwrap();
            if let Some(list) = slot.as_array_mut() {
                list.clear();
            } else {
                slot.as_object_mut().unwrap().remove(key);
            }
            assert_eq!(recorded_intake(&broken), None, "{pointer}");
        }
        // The legacy api.github* keys are not the record.
        let legacy = serde_json::json!({"api": {
            "githubFactoryIngressEnabled": true, "githubFactoryIntake": "poll",
            "githubAppId": "12345", "githubFactoryLabel": "curie-factory",
            "githubAppExistingSecret": "factory-app"
        }});
        assert_eq!(recorded_intake(&legacy), None);
    }
}

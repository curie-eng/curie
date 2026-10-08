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

pub const DEFAULT_KIND_NAME: &str = "curie-factory";
pub const DEFAULT_MODEL: &str = "z-ai/glm-5.3-flash";
pub const ANTHROPIC_DEFAULT_MODEL: &str = "claude-sonnet-5-5";
pub const DEFAULT_DEADLINE_SECONDS: u32 = 3600;
pub const DEFAULT_BUDGET_USD: f64 = 5.0;
pub const AGENT_NAME: &str = "dark-factory";
pub const GVISOR_OFF_SET: &str = "security.gvisor.mode=off";
pub const POLL_INTAKE: &str = "poll";
/// Display model matching the dark-factory bundle's `progress/phases.json`.
/// The runner resolves the reviewers' `opus` alias from the credential and override.
pub const REVIEWER_MODEL: &str = "openai/gpt-6.1-sol";
/// The OpenRouter credit one factory run should have available (#3935).
pub const RUN_CREDIT_USD: f64 = 5.0;

#[derive(Debug, Clone)]
pub struct QuickstartOpts {
    pub repo: String,
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
    pub chart: String,
    pub dry_run: bool,
}

#[derive(Debug, Clone)]
pub struct PlanInput {
    pub repo: String,
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
        repo: String,
        context: String,
        namespace: String,
        release: String,
        model: String,
    },
    Intake(Box<FactoryIntakeOpts>),
    Deploy {
        context: String,
        namespace: String,
        release: String,
        repo: String,
        deadline_seconds: u32,
        budget_usd: f64,
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
        runner_image: String,
        agent: String,
        deadline_seconds: u32,
        budget_usd: f64,
        /// OpenRouter credit left to the model credential, when it was read.
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
                "execution_deadline_seconds": deadline_seconds,
                "budget_usd": budget_usd,
                "reviewer_model": REVIEWER_MODEL,
                "run_credit_usd": RUN_CREDIT_USD,
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
                    "Deployed {agent} from {runner_image}. Execution deadline {deadline_seconds}s. Publication auto. Budget {budget_usd} USD."
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
        }
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

pub struct RerunTarget<'a> {
    pub repo: &'a str,
    pub context: &'a str,
    pub namespace: &'a str,
    pub release: &'a str,
    pub model: &'a str,
    pub org: Option<&'a str>,
}

pub fn registration_steps(target: &RerunTarget<'_>) -> Vec<String> {
    let mut rerun = format!(
        "Rerun: curie factory quickstart --repo {} --context {}",
        target.repo, target.context
    );
    for (flag, value, default) in [
        ("--namespace", target.namespace, "curie"),
        ("--release", target.release, "curie"),
        ("--model", target.model, DEFAULT_MODEL),
    ] {
        if value != default {
            rerun.push_str(&format!(" {flag} {value}"));
        }
    }
    if let Some(org) = target.org {
        rerun.push_str(&format!(" --org {org}"));
    }
    rerun.push_str(" --app-id <APP_ID> --private-key-file <PATH.pem>");
    vec![
        "Open the link and click Create GitHub App. The name, permissions, and the disabled webhook are already filled in.".to_string(),
        "On the App settings page, note the App ID and click Generate a private key to download the .pem file.".to_string(),
        format!("Click Install App and install it on {}.", target.repo),
        rerun,
    ]
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

pub fn bundle_dir(namespace: &str, release: &str) -> PathBuf {
    std::env::temp_dir()
        .join(format!("curie-factory-quickstart-{namespace}-{release}"))
        .join(AGENT_NAME)
}

/// True when helm user values show a real model install (`fakeModel: false`).
pub fn release_has_real_model(values: &serde_json::Value) -> bool {
    values.pointer("/agentSandbox/runner/fakeModel") == Some(&serde_json::json!(false))
}

pub fn quickstart_plan(input: &PlanInput) -> Result<Planned> {
    validate_repo(&input.repo)?;
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

    let owned_kind = kind_context_name(&input.kind_name);
    let context = input
        .explicit_context
        .clone()
        .or_else(|| input.current_context.clone())
        .unwrap_or_else(|| owned_kind.clone());
    // Kind is created only when nothing is targeted. A kind context, including
    // the one this command created on a previous run, still gets gVisor off
    // and the CoreDNS scale. A remote context does not.
    let kind_target = input.explicit_context.is_none() && input.current_context.is_none()
        || is_kind_context(&context);
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
    if !input.release_at_target {
        actions.push(Action::Cluster { args: up });
    }

    if finishing {
        actions.push(Action::Intake(Box::new(finish_intake_opts(
            input, &context,
        ))));
        actions.push(Action::Deploy {
            context: context.clone(),
            namespace: input.namespace.clone(),
            release: input.release.clone(),
            repo: input.repo.clone(),
            deadline_seconds: input.execution_deadline_seconds,
            budget_usd: input.budget_usd,
        });
    } else {
        actions.push(Action::Register {
            org: input.org.clone(),
            repo: input.repo.clone(),
            context: context.clone(),
            namespace: input.namespace.clone(),
            release: input.release.clone(),
            model: input.model.clone(),
        });
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
        credential: credential_decision(
            input.credential_in_env,
            input.release_has_real_model,
            input.interactive,
        ),
        actions,
        skipped_cluster_up: input.release_at_target,
    })
}

fn finish_intake_opts(input: &PlanInput, _context: &str) -> FactoryIntakeOpts {
    FactoryIntakeOpts {
        common: CommonOpts {
            namespace: input.namespace.clone(),
            release: input.release.clone(),
            dry_run: false,
        },
        chart: input.chart.clone(),
        repos: vec![input.repo.clone()],
        label: None,
        mention: None,
        card_base_url: None,
        webhook_secret: None,
        github_api_egress: Vec::new(),
        disable: false,
        timeout_seconds: None,
        app_id: input.app_id.clone(),
        private_key_file: input.private_key_file.as_ref().map(PathBuf::from),
        org: input.org.clone(),
        intake: Some(POLL_INTAKE.into()),
        runner_binding: None,
    }
}

pub fn describe(planned: &Planned) -> Vec<String> {
    let toolchain = if planned
        .actions
        .iter()
        .any(|action| matches!(action, Action::Intake(_)))
    {
        crate::factory_toolchain::PLAN_NOTE
    } else {
        crate::factory_toolchain::SKIPPED_NOTE
    };
    let mut lines = vec![toolchain.into(), planned.context_reason.clone()];
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
                repo,
                context,
                namespace,
                release,
                model,
            } => {
                let org_note = org
                    .as_ref()
                    .map(|org| format!(" for organization {org}"))
                    .unwrap_or_default();
                lines.push(format!(
                    "print the GitHub App registration link{org_note} and the rerun steps for {repo}; apply no intake"
                ));
                lines.extend(registration_steps(&RerunTarget {
                    repo,
                    context,
                    namespace,
                    release,
                    model,
                    org: org.as_deref(),
                }));
            }
            Action::Intake(opts) => {
                let values = intake_values(opts, &[]);
                let intake = values
                    .pointer("/api/githubFactoryIntake")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                lines.push(format!(
                    "configure factory intake api.githubFactoryIntake={intake} for {} with --app-id {} --private-key-file {} and no webhook secret; the same helm upgrade binds the dark factory runner image",
                    opts.repos.join(","),
                    opts.app_id.as_deref().unwrap_or(""),
                    opts.private_key_file
                        .as_ref()
                        .map(|p| p.display().to_string())
                        .unwrap_or_default()
                ));
            }
            Action::Deploy {
                context,
                namespace,
                release,
                repo,
                deadline_seconds,
                budget_usd,
            } => {
                let dir = bundle_dir(namespace, release);
                lines.push(format!(
                    "render the dark factory and deploy the published {} image",
                    crate::examples::DARK_FACTORY_RUNNER_REPOSITORY
                ));
                lines.push(format!(
                    "curie cluster deploy --context {context} --namespace {namespace} --release {release} --plugin-dir {} --agent {AGENT_NAME} --env prod --repo {repo}",
                    dir.display()
                ));
                lines.push(format!(
                    "curie cluster surfaces {AGENT_NAME} --context {context} --namespace {namespace} --release {release} --add github={repo}"
                ));
                lines.push(format!(
                    "curie cluster overrides {AGENT_NAME} --context {context} --namespace {namespace} --release {release} --execution-deadline {deadline_seconds}"
                ));
                lines.push(format!(
                    "curie cluster publication-policy {AGENT_NAME} --context {context} --namespace {namespace} --release {release} --policy auto"
                ));
                lines.push(format!(
                    "curie cluster budget {AGENT_NAME} --context {context} --namespace {namespace} --release {release} --limit {budget_usd}"
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

pub async fn quickstart(opts: QuickstartOpts) -> Result<QuickstartOutput> {
    let current = if opts.context.is_some() {
        None
    } else {
        crate::kube_context::current_context_name()
    };
    check_prerequisites(
        opts.context.as_deref(),
        current.as_deref(),
        crate::ops::on_path,
    )?;
    // The finishing pass discovers repository needs before pinning a kube
    // context or reading Helm. Dry-run describes these reads without making
    // them, and the first pass has no installation credential yet.
    if !opts.dry_run {
        match (&opts.app_id, &opts.private_key_file) {
            (Some(id), Some(file)) if !id.is_empty() && id.chars().all(|c| c.is_ascii_digit()) => {
                crate::factory_toolchain::preflight(id, file, std::slice::from_ref(&opts.repo))
                    .await?;
            }
            (None, None) => {}
            _ => {
                return Err(CliError::usage(
                    "pass a numeric --app-id and --private-key-file together",
                )
                .into())
            }
        }
    }
    // Confirm before pin or helm. A non-terminal refusal here never reads the
    // release, and a declined prompt never installs.
    let real_interactive = std::io::stdin().is_terminal();
    if !opts.dry_run {
        if let Some(name) = unconfirmed_remote_context(opts.context.as_deref(), current.as_deref())
        {
            confirm_remote_context(name, real_interactive)?;
        }
    }
    let existing = if opts.context.is_none() && current.is_none() {
        match kind_clusters().await {
            Ok(clusters) => clusters,
            Err(err) if opts.dry_run => {
                crate::ui::ui().note(&format!("kind was not queried: {err:#}"));
                Vec::new()
            }
            Err(err) => return Err(err),
        }
    } else {
        Vec::new()
    };
    let credential_in_env = crate::ops::model_credential_env()?.is_some();
    let targeted = opts.context.clone().or_else(|| current.clone());
    if let Some(context) = &targeted {
        if !opts.dry_run {
            // Helm value reads have no context flag. Pin first so a rerun
            // against --context sees that release, not the ambient one.
            crate::kube_context::pin_for_cluster_command(Some(context))?;
        }
    }
    let release_has_real_model = if opts.dry_run || targeted.is_none() {
        false
    } else {
        release_model_recorded(&opts).await?
    };
    let effective_credential = credit_check_key(
        crate::ops::explicit_model_credential_env(),
        crate::ops::saved_model_credential(),
        release_has_real_model,
    );
    let model = quickstart_model(opts.model.as_deref(), effective_credential.as_deref());
    let (release_at_target, model) = match &targeted {
        Some(context) if !opts.dry_run => {
            release_matches_quickstart_target(
                &opts,
                &model,
                opts.model.is_none() && effective_credential.is_none() && release_has_real_model,
                is_kind_context(context),
            )
            .await?
        }
        _ => (false, model),
    };
    let planned = quickstart_plan(&PlanInput {
        repo: opts.repo.clone(),
        app_id: opts.app_id.clone(),
        private_key_file: opts
            .private_key_file
            .as_ref()
            .map(|p| p.display().to_string()),
        explicit_context: opts.context.clone(),
        current_context: current,
        kind_name: opts.kind_name.clone(),
        existing_kind_clusters: existing,
        namespace: opts.namespace.clone(),
        release: opts.release.clone(),
        model,
        org: opts.org.clone(),
        execution_deadline_seconds: opts.execution_deadline_seconds,
        budget_usd: opts.budget_usd,
        chart: opts.chart.clone(),
        credential_in_env,
        release_has_real_model,
        // Dry-run reports a non-kind current context instead of refusing it.
        interactive: opts.dry_run || real_interactive,
        release_at_target,
    })?;
    if opts.dry_run {
        let mut planned = planned;
        planned.credential =
            credential_decision(credential_in_env, release_has_real_model, real_interactive);
        return Ok(QuickstartOutput::DryRun(DryRunPlan {
            lines: describe(&planned),
        }));
    }
    if planned.credential == CredentialDecision::RefuseNonInteractive {
        return Err(
            CliError::usage("model credential needed and stdin is not a terminal")
                .with_fix(
                    "set CURIE_CREDENTIALS to an Anthropic API key starting with sk-ant- or an OpenRouter key starting with sk-or- and rerun",
                )
                .into(),
        );
    }
    execute(&planned, &opts, release_has_real_model).await
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

async fn execute(
    planned: &Planned,
    opts: &QuickstartOpts,
    mut release_has_real_model: bool,
) -> Result<QuickstartOutput> {
    crate::ui::ui().note(&format!("Kubernetes context: {}", planned.context));
    if planned.skipped_cluster_up {
        crate::ui::ui().note("release already at the target chart and values; skipping cluster up");
    }
    let mut prompted = false;
    let mut prompted_model = None;
    let mut intake_json = serde_json::Value::Null;
    let mut rendered_image: Option<String> = None;
    let mut credit_remaining_usd = None;
    for action in &planned.actions {
        if matches!(action, Action::Deploy { .. }) {
            credit_remaining_usd = check_credit(release_has_real_model).await;
        }
        if matches!(action, Action::Cluster { .. }) && !prompted {
            prompted = true;
            if planned.kind_cluster.is_some() {
                // The plan could not read the release: the context did not
                // exist yet. Read it now, before `cluster up` records a key.
                // It only picks the key the credit check reads, so a failed
                // read never stops the install; it skips the saved key instead.
                release_has_real_model =
                    crate::kube_context::pin_for_cluster_command(Some(&planned.context)).is_err()
                        || release_model_recorded(opts).await.unwrap_or(true);
            }
            ensure_credential(&planned.credential)?;
            if planned.credential == CredentialDecision::PromptOnce {
                let credential = crate::ops::model_credential_env()?;
                prompted_model = Some(quickstart_model(
                    opts.model.as_deref(),
                    credential.as_deref(),
                ));
            }
        }
        match action {
            Action::External { program, args } => {
                let step = if program == "kind" {
                    "Creating kind cluster"
                } else {
                    "Scaling CoreDNS"
                };
                run_program(program, args, step).await?;
            }
            Action::Cluster { args } => {
                // Prompted credentials become available only immediately before
                // this action. Resolve again so an omitted model follows that key.
                let mut args = args.clone();
                if let Some(model) = &prompted_model {
                    let model_index = args
                        .iter()
                        .position(|arg| arg == "--model")
                        .expect("quickstart cluster up always specifies its model");
                    args[model_index + 1] = model.clone();
                }
                run_self(&args, "Installing Curie").await?;
            }
            Action::Register {
                org,
                repo,
                context,
                namespace,
                release,
                model,
            } => {
                let model = prompted_model.as_ref().unwrap_or(model);
                let name = crate::factory_app::random_app_name();
                let url = crate::factory_app::registration_url(org.as_deref(), &name);
                let steps = registration_steps(&RerunTarget {
                    repo,
                    context,
                    namespace,
                    release,
                    model,
                    org: org.as_deref(),
                });
                return Ok(QuickstartOutput::Registration {
                    context: planned.context.clone(),
                    url,
                    steps,
                    kind_cluster: planned.kind_cluster.clone(),
                });
            }
            Action::Intake(intake) => {
                crate::ui::ui().note("Rendering factory bundle");
                let image =
                    render_published_bundle(&intake.common.namespace, &intake.common.release)
                        .await
                        .map_err(|error| {
                            let (_, fix) = crate::exit::classify(&error);
                            step_error(
                                Path::new("curie"),
                                &[
                                    "--plugin-dir".into(),
                                    bundle_dir(&intake.common.namespace, &intake.common.release)
                                        .display()
                                        .to_string(),
                                ],
                                "Rendering factory bundle",
                                error,
                                fix.as_deref(),
                            )
                        })?;
                rendered_image = Some(image.clone());
                // ADR 0173 decision 5: refuse a layer built on another runner
                // before this upgrade binds `agentSandbox.runnerImages`.
                // `cluster deploy` repeats the check; it must not be the first
                // time the release learns the image.
                let bundle = bundle_dir(&intake.common.namespace, &intake.common.release);
                if let Err(error) =
                    crate::cluster_secrets::check_layered_runner_base(&intake.common, &bundle).await
                {
                    let (_, fix) = crate::exit::classify(&error);
                    return Err(step_error(
                        Path::new("curie"),
                        &["--plugin-dir".into(), bundle.display().to_string()],
                        "Checking the runner base",
                        error,
                        fix.as_deref(),
                    ));
                }
                let mut args = vec![
                    "cluster".into(),
                    "factory".into(),
                    "--context".into(),
                    planned.context.clone(),
                    "--namespace".into(),
                    intake.common.namespace.clone(),
                    "--release".into(),
                    intake.common.release.clone(),
                    "--chart".into(),
                    intake.chart.clone(),
                    "--repo".into(),
                    intake.repos.join(","),
                    "--intake".into(),
                    POLL_INTAKE.into(),
                    "--app-id".into(),
                    intake.app_id.clone().unwrap_or_default(),
                    "--private-key-file".into(),
                    intake
                        .private_key_file
                        .as_ref()
                        .map(|path| path.display().to_string())
                        .unwrap_or_default(),
                ];
                if let Some(org) = &intake.org {
                    args.extend(["--org".into(), org.clone()]);
                }
                args.push("--runner-image".into());
                args.push(format!("{AGENT_NAME}={image}"));
                intake_json = run_self(&args, "Configuring factory intake").await?;
                if intake_json.get("github_app").is_none() {
                    return Err(step_error(
                        Path::new("curie"),
                        &args,
                        "Configuring factory intake",
                        CliError::failure(
                            "factory intake did not record the App; nothing further was deployed",
                        )
                        .into(),
                        None,
                    ));
                }
            }
            Action::Deploy {
                context,
                namespace,
                release,
                repo,
                deadline_seconds,
                budget_usd,
            } => {
                let dir = bundle_dir(namespace, release);
                let image = match rendered_image.clone() {
                    Some(image) => image,
                    None => {
                        crate::ui::ui().note("Rendering factory bundle");
                        render_published_bundle(namespace, release)
                            .await
                            .map_err(|error| {
                                let (_, fix) = crate::exit::classify(&error);
                                step_error(
                                    Path::new("curie"),
                                    &["--plugin-dir".into(), dir.display().to_string()],
                                    "Rendering factory bundle",
                                    error,
                                    fix.as_deref(),
                                )
                            })?
                    }
                };
                run_self(
                    &[
                        "cluster".into(),
                        "deploy".into(),
                        "--context".into(),
                        context.clone(),
                        "--namespace".into(),
                        namespace.clone(),
                        "--release".into(),
                        release.clone(),
                        "--plugin-dir".into(),
                        dir.display().to_string(),
                        "--agent".into(),
                        AGENT_NAME.into(),
                        "--env".into(),
                        "prod".into(),
                        "--repo".into(),
                        repo.clone(),
                    ],
                    "Deploying dark factory",
                )
                .await?;
                run_self(
                    &cluster_agent(
                        "surfaces",
                        context,
                        namespace,
                        release,
                        &["--add".into(), format!("github={repo}")],
                    ),
                    "Binding GitHub repository",
                )
                .await?;
                run_self(
                    &cluster_agent(
                        "overrides",
                        context,
                        namespace,
                        release,
                        &["--execution-deadline".into(), deadline_seconds.to_string()],
                    ),
                    "Setting execution deadline",
                )
                .await?;
                run_self(
                    &cluster_agent(
                        "publication-policy",
                        context,
                        namespace,
                        release,
                        &["--policy".into(), "auto".into()],
                    ),
                    "Setting publication policy",
                )
                .await?;
                run_self(
                    &cluster_agent(
                        "budget",
                        context,
                        namespace,
                        release,
                        &["--limit".into(), budget_display(*budget_usd)],
                    ),
                    "Setting factory budget",
                )
                .await?;
                let app = intake_json
                    .get("github_app")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);
                return Ok(QuickstartOutput::Ready {
                    context: context.clone(),
                    repo: repo.clone(),
                    intake: POLL_INTAKE.into(),
                    app_id: app
                        .get("app_id")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    slug: app
                        .get("slug")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    mention: app
                        .get("mention")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    mention_inferred: app
                        .get("mention_inferred")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false),
                    repos: app
                        .get("repos")
                        .and_then(|v| v.as_array())
                        .map(|list| {
                            list.iter()
                                .filter_map(|v| v.as_str().map(str::to_string))
                                .collect()
                        })
                        .unwrap_or_default(),
                    label: app
                        .get("label")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    label_inferred: app
                        .get("label_inferred")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false),
                    runner_image: image,
                    agent: AGENT_NAME.into(),
                    deadline_seconds: *deadline_seconds,
                    budget_usd: *budget_usd,
                    credit_remaining_usd,
                });
            }
        }
    }
    Err(CliError::failure("quickstart plan produced no result").into())
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
    context: &str,
    namespace: &str,
    release: &str,
    extra: &[String],
) -> Vec<String> {
    let mut args = vec![
        "cluster".into(),
        verb.into(),
        AGENT_NAME.into(),
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
        CredentialDecision::PreserveRecorded => Ok(()),
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

async fn render_published_bundle(namespace: &str, release: &str) -> Result<String> {
    let dir = bundle_dir(namespace, release);
    if dir.exists() {
        std::fs::remove_dir_all(&dir).map_err(|err| {
            CliError::failure(format!(
                "could not replace the previous dark factory render at {}: {err}",
                dir.display()
            ))
        })?;
    }
    if let Some(parent) = dir.parent() {
        std::fs::create_dir_all(parent).map_err(|err| {
            CliError::failure(format!("could not create {}: {err}", parent.display()))
        })?;
    }
    let rendered =
        crate::examples::render_dark_factory(crate::examples::DarkFactoryRenderOpts { out: dir })
            .await?;
    match rendered.runner_image {
        Some(image) => Ok(image),
        None => Err(CliError::failure(
            rendered.runner_note.unwrap_or_else(|| {
                "no published dark factory runner image is available to deploy".into()
            }),
        )
        .with_fix(
            "install a released curie, which deploys ghcr.io/curie-eng/curie-dark-factory-runner for its version",
        )
        .into()),
    }
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
            repo: "acme/widgets".into(),
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
        assert!(text.contains("api.githubFactoryIntake=poll"), "{text}");
        assert!(text.contains("no webhook secret"), "{text}");
        assert!(
            text.contains("ghcr.io/curie-eng/curie-dark-factory-runner"),
            "{text}"
        );
        assert!(text.contains("--agent dark-factory"), "{text}");
        assert!(text.contains("--env prod"), "{text}");
        assert!(text.contains("--repo acme/widgets"), "{text}");
        assert!(text.contains("--add github=acme/widgets"), "{text}");
        assert!(text.contains("--execution-deadline 3600"), "{text}");
        assert!(text.contains("--policy auto"), "{text}");
        assert!(text.contains("--limit 5"), "{text}");
        assert!(text.contains("use CURIE_CREDENTIALS (no prompt)"), "{text}");
        assert!(!text.contains("registration link"), "{text}");
        assert!(plan_touches_forbidden_tool(&lines).is_none(), "{text}");
    }

    #[test]
    fn registration_steps_name_the_rerun_and_no_browser_tool() {
        let steps = registration_steps(&RerunTarget {
            repo: "acme/widgets",
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
        input.repo = "acme".into();
        assert!(quickstart_plan(&input).is_err());
        input.repo = "acme/widgets".into();
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
}

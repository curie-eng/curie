//! `curie factory quickstart`: from no cluster to a labelled issue that the
//! dark factory can turn into a pull request (#3749, ADR 0187).
//!
//! The command chains kind (only when no kube context is targeted), `cluster
//! up` with gVisor off on that kind path, the printed App registration link,
//! and on the second run the App setup, polling intake, and the published
//! dark factory deploy. It never opens a browser and never runs `gh`.

use std::io::{IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::process::Stdio;

use anyhow::Result;

use crate::exit::CliError;
use crate::factory_intake::{intake_values, FactoryIntakeOpts};
use crate::ops::{require_on_path, CommonOpts};
use crate::ui::DryRunPlan;

pub const DEFAULT_KIND_NAME: &str = "curie-factory";
pub const DEFAULT_MODEL: &str = "z-ai/glm-5.3-flash";
pub const DEFAULT_DEADLINE_SECONDS: u32 = 3600;
pub const DEFAULT_BUDGET_USD: f64 = 5.0;
pub const AGENT_NAME: &str = "dark-factory";
pub const GVISOR_OFF_SET: &str = "security.gvisor.mode=off";
pub const POLL_INTAKE: &str = "poll";

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
    pub model: String,
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
    pub kind_cluster: Option<String>,
    pub credential: CredentialDecision,
    pub actions: Vec<Action>,
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
            }),
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            Self::DryRun(plan) => plan.render(ui),
            Self::Registration {
                context,
                url,
                steps,
                kind_cluster,
            } => {
                if let Some(name) = kind_cluster {
                    ui.payload(&format!("Kind cluster {name}, context {context}."));
                } else {
                    ui.payload(&format!("Using Kubernetes context {context}."));
                }
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

pub fn validate_openrouter_key(key: &str) -> Result<()> {
    if key.starts_with("sk-or-") && key.len() > "sk-or-".len() {
        return Ok(());
    }
    Err(
        CliError::usage("the model credential is not an OpenRouter key")
            .with_fix("set CURIE_CREDENTIALS to an OpenRouter key starting with sk-or-")
            .into(),
    )
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
    let org = target
        .org
        .map(|org| format!(" --org {org}"))
        .unwrap_or_default();
    vec![
        "Open the link and click Create GitHub App. The name, permissions, and the disabled webhook are already filled in.".to_string(),
        "On the App settings page, note the App ID and click Generate a private key to download the .pem file.".to_string(),
        format!("Click Install App and install it on {}.", target.repo),
        format!(
            "Rerun: curie factory quickstart --repo {repo} --context {context} --namespace {namespace} --release {release} --model {model}{org} --app-id <APP_ID> --private-key-file <PATH.pem>",
            repo = target.repo,
            context = target.context,
            namespace = target.namespace,
            release = target.release,
            model = target.model,
        ),
    ]
}

pub fn kind_context_name(kind_name: &str) -> String {
    format!("kind-{kind_name}")
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
        || context.starts_with("kind-");
    let kind_cluster = kind_target.then(|| {
        if context.starts_with("kind-") {
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
    actions.push(Action::Cluster { args: up });

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

    Ok(Planned {
        context,
        kind_cluster,
        credential: credential_decision(
            input.credential_in_env,
            input.release_has_real_model,
            input.interactive,
        ),
        actions,
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
    }
}

pub fn describe(planned: &Planned) -> Vec<String> {
    let mut lines = Vec::new();
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
                "model credential: prompt once for an OpenRouter key (sk-or-) before cluster up"
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
                    "configure factory intake api.githubFactoryIntake={intake} for {} with --app-id {} --private-key-file {} and no webhook secret",
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

pub async fn quickstart(opts: QuickstartOpts) -> Result<QuickstartOutput> {
    let current = if opts.context.is_some() {
        None
    } else {
        crate::kube_context::current_context_name()
    };
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
    let interactive = std::io::stdin().is_terminal();
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
        model: opts.model.clone(),
        org: opts.org.clone(),
        execution_deadline_seconds: opts.execution_deadline_seconds,
        budget_usd: opts.budget_usd,
        chart: opts.chart.clone(),
        credential_in_env,
        release_has_real_model,
        interactive,
    })?;
    if opts.dry_run {
        return Ok(QuickstartOutput::DryRun(DryRunPlan {
            lines: describe(&planned),
        }));
    }
    if planned.credential == CredentialDecision::RefuseNonInteractive {
        return Err(
            CliError::usage("OpenRouter key needed and stdin is not a terminal")
                .with_fix(
                    "set CURIE_CREDENTIALS to an OpenRouter key starting with sk-or- and rerun",
                )
                .into(),
        );
    }
    execute(&planned).await
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

async fn execute(planned: &Planned) -> Result<QuickstartOutput> {
    let mut prompted = false;
    let mut intake_json = serde_json::Value::Null;
    for action in &planned.actions {
        if matches!(action, Action::Cluster { .. }) && !prompted {
            prompted = true;
            ensure_credential(&planned.credential)?;
        }
        match action {
            Action::External { program, args } => run_program(program, args).await?,
            Action::Cluster { args } => run_self(args).await?,
            Action::Register {
                org,
                repo,
                context,
                namespace,
                release,
                model,
            } => {
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
                crate::kube_context::pin_for_cluster_command(Some(&planned.context))?;
                let output = crate::factory_intake::factory_intake((**intake).clone()).await?;
                intake_json = output.to_json();
                if intake_json.get("github_app").is_none() {
                    return Err(CliError::failure(
                        "factory intake did not record the App; nothing further was deployed",
                    )
                    .into());
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
                let image = render_published_bundle(namespace, release).await?;
                let dir = bundle_dir(namespace, release);
                run_self(&[
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
                ])
                .await?;
                run_self(&cluster_agent(
                    "surfaces",
                    context,
                    namespace,
                    release,
                    &["--add".into(), format!("github={repo}")],
                ))
                .await?;
                run_self(&cluster_agent(
                    "overrides",
                    context,
                    namespace,
                    release,
                    &["--execution-deadline".into(), deadline_seconds.to_string()],
                ))
                .await?;
                run_self(&cluster_agent(
                    "publication-policy",
                    context,
                    namespace,
                    release,
                    &["--policy".into(), "auto".into()],
                ))
                .await?;
                run_self(&cluster_agent(
                    "budget",
                    context,
                    namespace,
                    release,
                    &["--limit".into(), budget_display(*budget_usd)],
                ))
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
                });
            }
        }
    }
    Err(CliError::failure("quickstart plan produced no result").into())
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
            validate_openrouter_key(&key)?;
            Ok(())
        }
        CredentialDecision::PreserveRecorded => Ok(()),
        CredentialDecision::PromptOnce => {
            if !std::io::stdin().is_terminal() {
                return Err(CliError::usage(
                    "OpenRouter key needed and stdin is not a terminal",
                )
                .with_fix(
                    "set CURIE_CREDENTIALS to an OpenRouter key starting with sk-or- and rerun",
                )
                .into());
            }
            eprint!("OpenRouter API key: ");
            let _ = std::io::stderr().flush();
            let key = rpassword::read_password().map_err(|err| {
                CliError::usage(format!("could not read the OpenRouter key: {err}"))
                    .with_fix("set CURIE_CREDENTIALS to an OpenRouter key starting with sk-or-")
            })?;
            let key = key.trim().to_string();
            validate_openrouter_key(&key)?;
            // The child `cluster up` reads this env. The value stays out of argv.
            std::env::set_var("CURIE_CREDENTIALS", key);
            Ok(())
        }
        CredentialDecision::RefuseNonInteractive => Err(CliError::usage(
            "OpenRouter key needed and stdin is not a terminal",
        )
        .with_fix("set CURIE_CREDENTIALS to an OpenRouter key starting with sk-or- and rerun")
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

async fn run_self(args: &[String]) -> Result<()> {
    let program = std::env::current_exe().map_err(|err| {
        CliError::failure(format!("cannot locate the curie binary to continue: {err}"))
    })?;
    run_command(&program, args).await
}

async fn run_program(program: &str, args: &[String]) -> Result<()> {
    require_on_path(program)?;
    run_command(Path::new(program), args).await
}

async fn run_command(program: &Path, args: &[String]) -> Result<()> {
    let ui = crate::ui::ui();
    ui.plumbing(&format!("+ {} {}", program.display(), args.join(" ")));
    let mut cmd = tokio::process::Command::new(program);
    cmd.args(args);
    if ui.json() {
        cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
        let output = cmd.output().await.map_err(|err| {
            CliError::failure(format!("{} failed to start: {err}", program.display()))
        })?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(CliError::failure(format!(
                "{} {} failed: {}{}",
                program.display(),
                args.join(" "),
                stderr.trim(),
                if stdout.trim().is_empty() {
                    String::new()
                } else {
                    format!(" {}", stdout.trim())
                }
            ))
            .into());
        }
        let stderr = String::from_utf8_lossy(&output.stderr);
        if !stderr.trim().is_empty() {
            eprint!("{stderr}");
        }
        Ok(())
    } else {
        let status = cmd.status().await.map_err(|err| {
            CliError::failure(format!("{} failed to start: {err}", program.display()))
        })?;
        if status.success() {
            Ok(())
        } else {
            Err(
                CliError::failure(format!("{} {} failed", program.display(), args.join(" ")))
                    .with_fix("fix the reported step and rerun; completed steps are safe to repeat")
                    .into(),
            )
        }
    }
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
        assert!(!text.contains("kind "), "{text}");
        assert!(text.contains("--context k8"), "{text}");
        assert!(!text.contains("security.gvisor.mode=off"), "{text}");
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
            "curie factory quickstart --repo acme/widgets --context kind-curie-factory --namespace factory-ns --release factory --model z-ai/glm-5.3-flash --app-id <APP_ID> --private-key-file <PATH.pem>"
        ));
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
            "--repo acme/widgets --context remote-cluster --namespace acme --release trial --model z-ai/glm-5.3-flash"
        ), "{text}");
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
    fn an_openrouter_key_is_required_and_other_shapes_are_refused() {
        assert!(validate_openrouter_key("sk-or-test-key").is_ok());
        assert!(validate_openrouter_key("sk-ant-test").is_err());
        assert!(validate_openrouter_key("sk-or-").is_err());
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

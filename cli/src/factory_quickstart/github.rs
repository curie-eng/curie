//! The GitHub code host and tracker for `curie factory quickstart` (#4397).
//!
//! The GitHub App registration link and rerun steps, the App preflight, the
//! poll intake, the published dark factory render, and the deploy with one
//! GitHub surface binding per repository. The shared planning, context,
//! credential, and child-process helpers live in the parent module.

use std::io::IsTerminal;
use std::path::{Path, PathBuf};

use anyhow::Result;

use super::*;

pub struct RerunTarget<'a> {
    pub repos: &'a [String],
    pub context: &'a str,
    pub namespace: &'a str,
    pub release: &'a str,
    pub model: &'a str,
    pub org: Option<&'a str>,
}

pub fn registration_steps(target: &RerunTarget<'_>) -> Vec<String> {
    let mut rerun = format!(
        "Rerun: curie factory quickstart --repo {} --context {}",
        target.repos.join(","),
        target.context
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
        format!(
            "Click Install App and install it on {}.",
            repos_prose(target.repos)
        ),
        rerun,
    ]
}

pub(super) fn finish_intake_opts(input: &PlanInput, _context: &str) -> FactoryIntakeOpts {
    FactoryIntakeOpts {
        common: CommonOpts {
            namespace: input.namespace.clone(),
            release: input.release.clone(),
            dry_run: false,
        },
        chart: input.chart.clone(),
        repos: input.repos.clone(),
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

pub(super) async fn quickstart(opts: QuickstartOpts) -> Result<QuickstartOutput> {
    // Reject a bad repository list before any prerequisite check, network
    // preflight, or kube context pin.
    validate_repos(&opts.repos)?;
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
                crate::factory_toolchain::preflight(id, file, &opts.repos).await?;
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
        repos: opts.repos.clone(),
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
                repos,
                context,
                namespace,
                release,
                model,
            } => {
                let model = prompted_model.as_ref().unwrap_or(model);
                let name = crate::factory_app::random_app_name();
                let url = crate::factory_app::registration_url(org.as_deref(), &name);
                let steps = registration_steps(&RerunTarget {
                    repos,
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
                let args = intake_args(&planned.context, intake, &image);
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
                repos,
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
                // `deploy_args` puts the deploy first, then one binding per repository.
                for (index, args) in deploy_args(context, namespace, release, repos)
                    .into_iter()
                    .enumerate()
                {
                    let step = if index == 0 {
                        "Deploying dark factory"
                    } else {
                        "Binding GitHub repository"
                    };
                    run_self(&args, step).await?;
                }
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
                    repo: repos.join(","),
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

/// The `cluster factory` argv quickstart runs for intake. That command's
/// `--repo` is repeatable and not comma split, so each repository gets its own
/// flag.
pub fn intake_args(context: &str, intake: &FactoryIntakeOpts, image: &str) -> Vec<String> {
    let mut args: Vec<String> = vec![
        "cluster".into(),
        "factory".into(),
        "--context".into(),
        context.into(),
        "--namespace".into(),
        intake.common.namespace.clone(),
        "--release".into(),
        intake.common.release.clone(),
        "--chart".into(),
        intake.chart.clone(),
    ];
    for repo in &intake.repos {
        args.extend(["--repo".into(), repo.clone()]);
    }
    args.extend([
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
    ]);
    if let Some(org) = &intake.org {
        args.extend(["--org".into(), org.clone()]);
    }
    args.push("--runner-image".into());
    args.push(format!("{AGENT_NAME}={image}"));
    args
}

/// The deploy argvs, in order: `cluster deploy` with the first repository as
/// the agent's git-flow repository, then one GitHub surface binding per
/// repository so the one agent serves each.
pub fn deploy_args(
    context: &str,
    namespace: &str,
    release: &str,
    repos: &[String],
) -> Vec<Vec<String>> {
    let first = repos.first().cloned().unwrap_or_default();
    let mut argvs = vec![vec![
        "cluster".into(),
        "deploy".into(),
        "--context".into(),
        context.into(),
        "--namespace".into(),
        namespace.into(),
        "--release".into(),
        release.into(),
        "--plugin-dir".into(),
        bundle_dir(namespace, release).display().to_string(),
        "--agent".into(),
        AGENT_NAME.into(),
        "--env".into(),
        "prod".into(),
        "--repo".into(),
        first,
    ]];
    for repo in repos {
        argvs.push(cluster_agent(
            "surfaces",
            context,
            namespace,
            release,
            &["--add".into(), format!("github={repo}")],
        ));
    }
    argvs
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

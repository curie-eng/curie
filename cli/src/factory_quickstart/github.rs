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

/// The App flags every printed rerun ends with.
const APP_FLAG_PLACEHOLDERS: &str = " --app-id <APP_ID> --private-key-file <PATH.pem>";

/// The runner image an install path intake binds for a published bundle until
/// the render resolves the published tag (the repository, with no tag).
pub(super) const PUBLISHED_RUNNER_PENDING: &str = crate::examples::DARK_FACTORY_RUNNER_REPOSITORY;

/// Add the register path's non-default `--agent`, `--label`, and
/// `--plugin-dir` (#4362 decision 2.3) to the printed rerun, before its App
/// flags.
pub(super) fn carry_rerun_flags(steps: &mut [String], flags: &[String]) {
    if flags.is_empty() {
        return;
    }
    for step in steps.iter_mut().filter(|step| step.starts_with("Rerun:")) {
        if let Some(head) = step.strip_suffix(APP_FLAG_PLACEHOLDERS) {
            *step = format!("{head} {}{APP_FLAG_PLACEHOLDERS}", flags.join(" "));
        }
    }
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
    rerun.push_str(APP_FLAG_PLACEHOLDERS);
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
        all_installed_repos: false,
        label: None,
        mention: None,
        card_base_url: None,
        webhook_secret: None,
        forge_egress: vec![input.agent.clone()],
        disable: false,
        timeout_seconds: None,
        app_id: input.app_id.clone(),
        private_key_file: input.private_key_file.as_ref().map(PathBuf::from),
        org: input.org.clone(),
        intake: Some(POLL_INTAKE.into()),
        // #4362 decision 7: the published layer, an operator bundle's locked
        // image, or nothing for the platform runner.
        runner_binding: match (&input.plugin_dir, &input.runner_image) {
            (None, _) => Some(crate::factory_intake::RunnerImageBinding {
                agent: input.agent.clone(),
                image: PUBLISHED_RUNNER_PENDING.into(),
            }),
            (Some(_), Some(image)) => Some(crate::factory_intake::RunnerImageBinding {
                agent: input.agent.clone(),
                image: image.clone(),
            }),
            (Some(_), None) => None,
        },
    }
}

/// #4362 decision 5.3: the operator bundle exists, has a readable manifest,
/// and passes the lock preflight `cluster deploy` runs. Returns its locked
/// runner image, or `None` when it declares no runner layer.
fn check_plugin_dir(dir: &Path) -> Result<Option<String>> {
    if !dir.is_dir() {
        return Err(CliError::usage(format!(
            "--plugin-dir {} is not a directory",
            dir.display()
        ))
        .with_fix("pass a bundle directory, for example one written by curie example dark-factory render --out <DIR>")
        .into());
    }
    let manifest = dir.join(".claude-plugin").join("plugin.json");
    std::fs::read_to_string(&manifest).map_err(|error| {
        CliError::usage(format!(
            "--plugin-dir {} has no readable .claude-plugin/plugin.json: {error}",
            dir.display()
        ))
        .with_fix("pass the root of a plugin bundle")
    })?;
    let decl = crate::connector_build::load(dir)?;
    if decl.runner.is_some() || decl.connectors.values().any(|spec| spec.build.is_some()) {
        let recomputed = crate::commands::recompute_source_digests(dir, &decl)?;
        let lock = crate::connector_build::load_lock(dir)?;
        crate::commands::lock_preflight(
            dir,
            &decl,
            lock.as_ref(),
            &recomputed,
            crate::commands::DeployTier::Cluster,
        )?;
    }
    crate::connector_build::locked_runner_image(dir)
}

/// Every agent's GitHub bindings on the serving release, read once through
/// the same API connection the `cluster` verbs use (#4362 decision 6).
async fn read_bindings(common: &CommonOpts) -> Result<Vec<AgentBinding>> {
    let env = |name: &str| std::env::var(name).ok().filter(|value| !value.is_empty());
    let (url, key, _port_forward) = crate::commands::resolve_cluster_conn(
        env("CURIE_API_URL"),
        env("CURIE_API_KEY"),
        &common.namespace,
        &common.release,
        false,
    )
    .await?;
    let agents = crate::api::ApiClient::new(&url, &key)?
        .list_agents()
        .await?;
    Ok(agents
        .into_iter()
        .flat_map(|agent| {
            let name = agent.name;
            agent
                .channels
                .into_iter()
                .filter(|binding| binding.kind == "github")
                .map(move |binding| AgentBinding {
                    agent: name.clone(),
                    repo: binding.address,
                    trigger_label: binding.trigger_label,
                })
        })
        .collect())
}

/// The serving release's recorded intake and, when this run may bind, its
/// GitHub bindings. `None` recorded when no revision is serving.
async fn read_install_state(
    common: &CommonOpts,
    opts: &QuickstartOpts,
) -> Result<(Option<RecordedIntake>, Vec<AgentBinding>)> {
    let Some((values, _)) = deployed_release_snapshot(common).await? else {
        return Ok((None, Vec::new()));
    };
    let recorded = recorded_intake(&values);
    let app_flags = opts.app_id.is_some() || opts.private_key_file.is_some();
    if !app_flags && recorded.is_none() {
        // The register path binds nothing, so it reads no bindings.
        return Ok((None, Vec::new()));
    }
    if !app_flags {
        // Decision 3: refuse the install's own flags before any further read.
        refuse_install_flags(opts.model.is_some(), opts.org.as_deref())?;
    }
    Ok((recorded, read_bindings(common).await?))
}

/// The App the add agent path authenticates as: the PEM is read from the
/// Secret the release records and stays in memory (#4362 decision 4).
async fn recorded_app(
    common: &CommonOpts,
    recorded: &RecordedIntake,
    repos: &[String],
) -> Result<crate::factory_toolchain::AppPreflight> {
    let pem = crate::ops::read_secret_key(
        &common.namespace,
        &recorded.pem_secret,
        &recorded.pem_secret_key,
    )
    .await
    .ok_or_else(|| {
        CliError::failure(format!(
            "could not read the factory App private key from Secret {} key {} in namespace {}",
            recorded.pem_secret, recorded.pem_secret_key, common.namespace
        ))
        .with_fix(format!(
            "confirm kubectl can read Secret {} in namespace {}, or rerun with --app-id and --private-key-file",
            recorded.pem_secret, common.namespace
        ))
    })?;
    if !crate::github_app::is_pem_private_key(&pem) {
        return Err(CliError::failure(format!(
            "Secret {} key {} does not hold a PEM private key",
            recorded.pem_secret, recorded.pem_secret_key
        ))
        .with_fix("rerun with --app-id and --private-key-file to record the App key again")
        .into());
    }
    crate::factory_toolchain::preflight_with_pem(&recorded.app_id, pem, repos, false).await
}

/// Full commit tags must exist for the entire platform before quickstart
/// reaches Kubernetes, Helm, or the forge, including on a dry run.
pub(super) async fn resolve_candidate_runner(
    commit: &str,
) -> Result<crate::examples::PublishedStockRunner> {
    let tag = format!("sha-{commit}");
    let mut missing = Vec::new();
    let mut published = crate::examples::PublishedStockRunner {
        layer_image: String::new(),
        base_image: String::new(),
    };
    for component in [
        "api",
        "dispatcher",
        "mail-adapter",
        "worker",
        "ui",
        "runner",
        "dark-factory-runner",
    ] {
        let repository = format!("ghcr.io/curie-eng/curie-{component}");
        match crate::examples::published_index_digest(&repository, &tag).await? {
            Some(digest) => match component {
                "runner" => published.base_image = format!("{repository}@{digest}"),
                "dark-factory-runner" => {
                    published.layer_image = format!("{repository}@{digest}");
                }
                _ => {}
            },
            None => missing.push(format!("{repository}:{tag}")),
        }
    }
    if !missing.is_empty() {
        return Err(CliError::failure(format!(
            "candidate {commit} is missing published images: {}",
            missing.join(", ")
        ))
        .into());
    }
    Ok(published)
}

pub(super) async fn quickstart(opts: QuickstartOpts) -> Result<QuickstartOutput> {
    // Reject a bad repository list, agent, label, or bundle before any
    // prerequisite check, App preflight, or kube context pin.
    validate_repos(&opts.repos)?;
    validate_agent_flags(&opts.agent, opts.label.as_deref())?;
    let runner_image = match &opts.plugin_dir {
        Some(dir) => check_plugin_dir(dir)?,
        None => None,
    };
    let published_candidate = opts.candidate.as_ref().map(|candidate| &candidate.runner);
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
    let mut app = None;
    if !opts.dry_run {
        match (&opts.app_id, &opts.private_key_file) {
            (Some(id), Some(file)) if !id.is_empty() && id.chars().all(|c| c.is_ascii_digit()) => {
                app =
                    Some(crate::factory_toolchain::preflight(id, file, &opts.repos, false).await?);
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
    // The Install path's Jira preflight follows its App preflight, before any
    // cluster call, so a refused token or start authority mutates nothing.
    // The add agent path runs it once the recorded App is read, below.
    let jira_tracker = opts.forge.tracker == TrackerKind::JiraCloud;
    let mut jira = None;
    let mut jira_reads = Vec::new();
    if jira_tracker && opts.dry_run {
        let (described, reads) = jira_cloud::described(&opts.forge, &opts.repos)?;
        jira = Some(described);
        jira_reads = reads;
    } else if let (true, Some(app)) = (jira_tracker, &app) {
        jira = Some(jira_cloud::preflight(&opts.forge, &opts.repos, app).await?);
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
    let common = CommonOpts {
        namespace: opts.namespace.clone(),
        release: opts.release.clone(),
        dry_run: false,
    };
    // #4362 decision 2: the path follows the App flags and the serving
    // release's recorded intake. A dry run reads only through an explicit
    // --context; without one, or when a read fails, it plans as if no intake
    // were recorded and says so.
    let mut assumed = None;
    let (recorded, bindings) = if opts.dry_run {
        match &opts.context {
            None => {
                assumed = Some("no --context was passed, so the release was not read; planned as if it records no factory intake".to_string());
                (None, Vec::new())
            }
            Some(context) => {
                let read = match crate::kube_context::pin_for_cluster_command(Some(context)) {
                    Ok(_) => read_install_state(&common, &opts).await,
                    Err(error) => Err(error),
                };
                match read {
                    Ok(state) => state,
                    // The add agent path's refusal of --model or --org.
                    Err(error)
                        if error
                            .downcast_ref::<CliError>()
                            .is_some_and(|e| e.class == crate::exit::ExitClass::Usage) =>
                    {
                        return Err(error)
                    }
                    Err(error) => {
                        assumed = Some(format!(
                            "the release could not be read ({error:#}); planned as if it records no factory intake"
                        ));
                        (None, Vec::new())
                    }
                }
            }
        }
    } else if targeted.is_some() {
        read_install_state(&common, &opts).await?
    } else {
        (None, Vec::new())
    };
    let add_agent = opts.app_id.is_none() && recorded.is_some();
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
        Some(context) if !opts.dry_run && !add_agent => {
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
    let install_label = recorded.as_ref().map(|r| r.install_label.clone());
    // The add agent path authenticates as the recorded App. A Jira agent
    // needs it now, for the repository ids its binding records.
    if let (true, true, None, Some(recorded)) = (jira_tracker, add_agent, &jira, &recorded) {
        let recorded_app = recorded_app(&common, recorded, &opts.repos).await?;
        jira = Some(jira_cloud::preflight(&opts.forge, &opts.repos, &recorded_app).await?);
        app = Some(recorded_app);
    }
    let mut planned = quickstart_plan(&PlanInput {
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
        chart_explicit: opts.chart_explicit,
        // Dry-run reports a non-kind current context instead of refusing it.
        interactive: opts.dry_run || real_interactive,
        release_at_target,
        agent: opts.agent.clone(),
        label: opts.label.clone(),
        plugin_dir: opts.plugin_dir.clone(),
        model_explicit: opts.model.is_some(),
        recorded: recorded.clone(),
        bindings,
        runner_image,
        jira,
    })?;
    if let Some(candidate) = &opts.candidate {
        let commit = &candidate.commit;
        for action in &mut planned.actions {
            match action {
                Action::Cluster { args } => {
                    for setting in candidate_install_settings(commit) {
                        args.extend(["--set".into(), setting]);
                    }
                }
                Action::Register { rerun_flags, .. } => {
                    rerun_flags.extend(["--candidate".into(), commit.clone()]);
                }
                _ => {}
            }
        }
    }
    if opts.dry_run {
        let mut planned = planned;
        if planned.path != QuickstartPath::AddAgent {
            planned.credential =
                credential_decision(credential_in_env, release_has_real_model, real_interactive);
        }
        let mut lines = describe(&planned);
        if let Some(note) = assumed {
            lines.insert(2, note);
        }
        // The Jira reads the preflight would make, ahead of the line that
        // writes what they find.
        if let Some(at) = planned.actions.iter().find_map(|action| match action {
            Action::JiraTracker(jira) => {
                let line = jira_tracker_line(jira);
                lines.iter().position(|planned| *planned == line)
            }
            _ => None,
        }) {
            lines.splice(at..at, jira_reads);
        }
        if let (Some(candidate), Some(published)) = (&opts.candidate, published_candidate) {
            lines.insert(
                0,
                format!("candidate {}: all seven images resolved", candidate.commit),
            );
            if opts.plugin_dir.is_none() {
                lines.insert(
                    1,
                    format!(
                        "runner layer lock: {} built on {}",
                        published.layer_image, published.base_image
                    ),
                );
            }
        }
        return Ok(QuickstartOutput::DryRun(DryRunPlan { lines }));
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
    // Decision 5.6: a locked runner layer must sit on the installation's
    // runner before anything is applied. The published layer is checked
    // when it renders.
    if let (Some(dir), true) = (&opts.plugin_dir, targeted.is_some()) {
        crate::cluster_secrets::check_layered_runner_base(&common, dir).await?;
    }
    // The add agent path's first mutation is the egress bind, so its
    // published layer is rendered and checked here, before it. The install
    // path renders inside intake, ahead of that path's first release write.
    let mut rendered_image = None;
    if let (QuickstartPath::AddAgent, None) = (planned.path, &opts.plugin_dir) {
        rendered_image = Some(render_and_check(&common, &opts.agent, published_candidate).await?);
    }
    if let (QuickstartPath::AddAgent, Some(recorded), None) = (planned.path, &recorded, &app) {
        app = Some(recorded_app(&common, recorded, &opts.repos).await?);
    }
    execute(
        &planned,
        &opts,
        release_has_real_model,
        app.as_ref(),
        install_label,
        rendered_image,
        published_candidate,
    )
    .await
}

/// Render the published bundle for `agent` and refuse a layer built on
/// another runner before anything binds it (ADR 0173 decision 5).
async fn render_and_check(
    common: &CommonOpts,
    agent: &str,
    published: Option<&crate::examples::PublishedStockRunner>,
) -> Result<String> {
    crate::ui::ui().note("Rendering factory bundle");
    let bundle = bundle_dir(&common.namespace, &common.release, agent);
    let image = render_published_bundle(&common.namespace, &common.release, agent, published)
        .await
        .map_err(|error| {
            let (_, fix) = crate::exit::classify(&error);
            step_error(
                Path::new("curie"),
                &["--plugin-dir".into(), bundle.display().to_string()],
                "Rendering factory bundle",
                error,
                fix.as_deref(),
            )
        })?;
    // `cluster deploy` repeats the check; it must not be the first time the
    // release learns the image.
    if let Err(error) = crate::cluster_secrets::check_layered_runner_base(common, &bundle).await {
        let (_, fix) = crate::exit::classify(&error);
        return Err(step_error(
            Path::new("curie"),
            &["--plugin-dir".into(), bundle.display().to_string()],
            "Checking the runner base",
            error,
            fix.as_deref(),
        ));
    }
    Ok(image)
}

fn app_str(app: &serde_json::Value, key: &str) -> String {
    app.get(key)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string()
}

fn app_bool(app: &serde_json::Value, key: &str) -> bool {
    app.get(key).and_then(|v| v.as_bool()).unwrap_or(false)
}

async fn execute(
    planned: &Planned,
    opts: &QuickstartOpts,
    mut release_has_real_model: bool,
    app: Option<&crate::factory_toolchain::AppPreflight>,
    install_label: Option<String>,
    mut rendered_image: Option<String>,
    published_candidate: Option<&crate::examples::PublishedStockRunner>,
) -> Result<QuickstartOutput> {
    crate::ui::ui().note(&format!("Kubernetes context: {}", planned.context));
    if planned.skipped_cluster_up {
        crate::ui::ui().note("release already at the target chart and values; skipping cluster up");
    }
    let mut prompted = false;
    let mut prompted_model = None;
    let mut intake_json = serde_json::Value::Null;
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
                rerun_flags,
            } => {
                let model = prompted_model.as_ref().unwrap_or(model);
                let name = crate::factory_app::random_app_name();
                let url = crate::factory_app::registration_url(org.as_deref(), &name);
                let mut steps = registration_steps(&RerunTarget {
                    repos,
                    context,
                    namespace,
                    release,
                    model,
                    org: org.as_deref(),
                });
                carry_rerun_flags(&mut steps, rerun_flags);
                return Ok(QuickstartOutput::Registration {
                    context: planned.context.clone(),
                    url,
                    steps,
                    kind_cluster: planned.kind_cluster.clone(),
                });
            }
            Action::Intake(intake) => {
                let image = match &intake.runner_binding {
                    Some(binding) if binding.image == PUBLISHED_RUNNER_PENDING => {
                        let image =
                            render_and_check(&intake.common, &binding.agent, published_candidate)
                                .await?;
                        rendered_image = Some(image.clone());
                        image
                    }
                    Some(binding) => binding.image.clone(),
                    None => String::new(),
                };
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
            Action::JiraTracker(jira) => {
                apply_jira_tracker(opts, jira).await?;
            }
            Action::ForgeEgress { agent } => {
                let chart = opts.chart.clone();
                crate::cluster_secrets::bind_forge_egress(
                    CommonOpts {
                        namespace: opts.namespace.clone(),
                        release: opts.release.clone(),
                        dry_run: false,
                    },
                    agent.clone(),
                    async move { Ok(chart) },
                )
                .await?;
            }
            Action::TriggerLabel { label, repos } => {
                let app = app.ok_or_else(|| {
                    CliError::failure(
                        "no authenticated factory App to create the trigger label with",
                    )
                })?;
                crate::ui::ui().note(&format!("Creating trigger label {label}"));
                for repo in repos {
                    crate::factory_app::create_label(&app.api, &app.app, repo, label).await?;
                }
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
                bound,
                jira_binding,
                chart,
            } => {
                let (dir, image) = match bundle {
                    Some(dir) => (dir.clone(), runner_image.clone()),
                    None => {
                        let image = match rendered_image.clone() {
                            Some(image) => image,
                            None => {
                                render_and_check(
                                    &CommonOpts {
                                        namespace: namespace.clone(),
                                        release: release.clone(),
                                        dry_run: false,
                                    },
                                    agent,
                                    published_candidate,
                                )
                                .await?
                            }
                        };
                        (bundle_dir(namespace, release, agent), Some(image))
                    }
                };
                // `deploy_args` puts the deploy first, then the Jira binding or
                // one GitHub binding per repository.
                for (index, args) in deploy_args(
                    context,
                    namespace,
                    release,
                    repos,
                    agent,
                    label.as_deref(),
                    &dir,
                    bound,
                    jira_binding.as_deref(),
                    chart.as_deref(),
                )
                .into_iter()
                .enumerate()
                {
                    let step = if index == 0 && agent == DEFAULT_AGENT {
                        "Deploying dark factory".to_string()
                    } else if index == 0 {
                        format!("Deploying {agent}")
                    } else if jira_binding.is_some() {
                        "Binding the Jira tracker".to_string()
                    } else {
                        "Binding GitHub repository".to_string()
                    };
                    run_self(&args, &step).await?;
                }
                for repo in bound {
                    crate::ui::ui().note(&skip_bound_line(agent, repo));
                }
                run_self(
                    &cluster_agent(
                        "overrides",
                        agent,
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
                        agent,
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
                        agent,
                        context,
                        namespace,
                        release,
                        &["--limit".into(), budget_display(*budget_usd)],
                    ),
                    "Setting factory budget",
                )
                .await?;
                if planned.path == QuickstartPath::AddAgent {
                    return Ok(QuickstartOutput::AgentAdded {
                        context: context.clone(),
                        agent: agent.clone(),
                        repos: repos.clone(),
                        trigger_label: label.clone(),
                        effective_label: label.clone().or(install_label).unwrap_or_default(),
                        bundle: match bundle {
                            Some(dir) => dir.display().to_string(),
                            None => "published".to_string(),
                        },
                        runner_image: image,
                        deadline_seconds: *deadline_seconds,
                        budget_usd: *budget_usd,
                        credit_remaining_usd,
                    });
                }
                let github_app = intake_json
                    .get("github_app")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);
                let install_label = app_str(&github_app, "label");
                return Ok(QuickstartOutput::Ready {
                    context: context.clone(),
                    repo: repos.join(","),
                    intake: POLL_INTAKE.into(),
                    app_id: app_str(&github_app, "app_id"),
                    slug: app_str(&github_app, "slug"),
                    mention: app_str(&github_app, "mention"),
                    mention_inferred: app_bool(&github_app, "mention_inferred"),
                    repos: github_app
                        .get("repos")
                        .and_then(|v| v.as_array())
                        .map(|list| {
                            list.iter()
                                .filter_map(|v| v.as_str().map(str::to_string))
                                .collect()
                        })
                        .unwrap_or_default(),
                    effective_label: label.clone().unwrap_or_else(|| install_label.clone()),
                    label: install_label,
                    label_inferred: app_bool(&github_app, "label_inferred"),
                    runner_image: image,
                    agent: agent.clone(),
                    trigger_label: label.clone(),
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
/// flag. `image` is the resolved runner image for the intake's runner binding;
/// with no binding the agent runs the platform runner and no image is passed.
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
    for agent in &intake.forge_egress {
        args.extend(["--forge-egress".into(), agent.clone()]);
    }
    if let Some(binding) = &intake.runner_binding {
        args.push("--runner-image".into());
        args.push(format!("{}={image}", binding.agent));
    }
    args
}

/// The deploy argvs, in order: `cluster deploy` of `bundle` as `agent` with the
/// first repository as its git-flow repository, then one GitHub surface
/// binding per repository, carrying `--label` when set, so the one agent
/// serves each. A repository in `bound` already carries this agent's binding
/// under the requested label and gets no `surfaces --add`. With
/// `jira_binding` the agent is bound once through that Jira binding instead,
/// which names every repository. `chart` is the operator's `--chart`, passed
/// on so the deploy writes connector Secrets with the chart the release runs
/// rather than resolving the released one; `None` leaves the default.
#[allow(clippy::too_many_arguments)]
pub fn deploy_args(
    context: &str,
    namespace: &str,
    release: &str,
    repos: &[String],
    agent: &str,
    label: Option<&str>,
    bundle: &Path,
    bound: &[String],
    jira_binding: Option<&str>,
    chart: Option<&str>,
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
        bundle.display().to_string(),
        "--agent".into(),
        agent.into(),
        "--env".into(),
        "prod".into(),
        "--repo".into(),
        first,
    ]];
    if let Some(chart) = chart {
        argvs[0].extend(["--chart".into(), chart.into()]);
    }
    let surfaces: Vec<String> = match jira_binding {
        Some(binding) => vec![format!("jira={binding}")],
        None => repos
            .iter()
            .filter(|repo| !bound.contains(repo))
            .map(|repo| format!("github={repo}"))
            .collect(),
    };
    for surface in surfaces {
        let mut extra = vec!["--add".into(), surface];
        if let Some(label) = label {
            extra.extend(["--label".into(), label.into()]);
        }
        argvs.push(cluster_agent(
            "surfaces", agent, context, namespace, release, &extra,
        ));
    }
    argvs
}

/// Apply the Jira token Secret, then write the Jira tracker and binding
/// entries on the release. The token is read from its file here and goes
/// only to `kubectl apply` on stdin.
async fn apply_jira_tracker(opts: &QuickstartOpts, jira: &JiraPlan) -> Result<()> {
    let common = CommonOpts {
        namespace: opts.namespace.clone(),
        release: opts.release.clone(),
        dry_run: false,
    };
    crate::ui::ui().note(&format!("Writing Secret {}", jira.secret_name));
    let token = jira_cloud::read_token(&jira.token_file)?;
    let manifest = jira_cloud::secret_manifest(&common.namespace, &token);
    let body = serde_json::to_vec(&manifest)?;
    // kubectl's output is dropped: a failed apply can echo the manifest.
    let (ok, _, _) = crate::ops::run_capture_with_stdin(
        &crate::factory_app::secret_apply_command(&common.namespace),
        &body,
    )
    .await?;
    if !ok {
        return Err(CliError::failure(format!(
            "writing the Jira token to Secret {} in namespace {} failed; the tracker was not written",
            jira.secret_name, common.namespace
        ))
        .with_fix(format!(
            "check access with `kubectl -n {} auth can-i create secrets` and rerun",
            common.namespace
        ))
        .into());
    }
    crate::factory_intake::apply_forge_entries(&common, &opts.chart, jira).await
}

async fn render_published_bundle(
    namespace: &str,
    release: &str,
    agent: &str,
    published: Option<&crate::examples::PublishedStockRunner>,
) -> Result<String> {
    let dir = bundle_dir(namespace, release, agent);
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
    let rendered = match published {
        Some(published) => {
            crate::examples::render_dark_factory_with_published_runner(dir, published)?
        }
        None => {
            crate::examples::render_dark_factory(crate::examples::DarkFactoryRenderOpts {
                out: dir,
            })
            .await?
        }
    };
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

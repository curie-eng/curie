//! The GitLab quickstart run: flags, the preflight (steps a to e), the labels
//! (step f), then the install, the forge entries and the deploy, or under
//! `--dry-run` the plan of them after steps a to e.

use std::io::IsTerminal;
use std::path::{Path, PathBuf};

use anyhow::Result;

use super::api::{self, Found, Gitlab};
use super::{
    egress_prose, gitlab_entries, gitlab_web_base, project_path, wanted_labels, web_egress,
    GitlabFacts, CA_CONFIG_MAP, ENTRY_NAME, SECRET_NAME,
};
use crate::exit::CliError;
use crate::factory_intake::{apply_forge_entries, native_forge_values, NativeForgeEntries};
use crate::factory_quickstart::{
    agent_settings, apply_manifest, bundle_dir, bundle_line, check_credit, check_prerequisites,
    cluster_agent, confirm_remote_context, credential_decision, credential_line, credit_check_key,
    ensure_credential, forge_egress_line, install_action_line, install_steps, is_kind_context,
    kind_clusters, quickstart_model, read_token, release_matches_quickstart_target,
    release_model_recorded, run_install_action, run_self, token_secret_manifest,
    unconfirmed_remote_context, validate_agent_flags, Action, CredentialDecision, InstallSteps,
    InstallTarget, QuickstartOpts, QuickstartOutput, DEFAULT_AGENT, SKIPPED_CLUSTER_UP_LINE,
};
use crate::ops::CommonOpts;
use crate::ui::DryRunPlan;

/// Everything a GitLab run applies, in the order it applies it.
struct GitlabPlan {
    install: InstallSteps,
    credential: CredentialDecision,
    found: Found,
    expiry: String,
    facts: GitlabFacts,
    entries: NativeForgeEntries,
    token_file: PathBuf,
    ca_file: Option<PathBuf>,
    namespace: String,
    release: String,
    agent: String,
    /// The agent's own binding label (`--label`); `None` lets the tracker
    /// label serve the binding.
    label: Option<String>,
    /// The operator bundle; `None` renders the published dark factory.
    bundle: Option<PathBuf>,
    /// The operator bundle's locked runner image.
    runner_image: Option<String>,
    /// The operator's `--chart`, passed on to `cluster deploy`.
    deploy_chart: Option<String>,
    deadline_seconds: u32,
    budget_usd: f64,
}

/// `--app-id`, `--private-key-file` and `--org` configure a GitHub App, which
/// a GitLab install has none of.
fn refuse_github_flags(opts: &QuickstartOpts) -> Result<()> {
    let set = [
        ("--app-id", opts.app_id.is_some()),
        ("--private-key-file", opts.private_key_file.is_some()),
        ("--org", opts.org.is_some()),
    ];
    match set.iter().find(|(_, set)| *set) {
        Some((flag, _)) => Err(CliError::usage(format!(
            "{flag} configures the GitHub App and does not apply to --code-host gitlab; the GitLab token comes from --token-file"
        ))
        .with_fix(format!("drop {flag}"))
        .into()),
        None => Ok(()),
    }
}

/// A `YYYY-MM-DD` date.
fn parse_date(raw: &str) -> Option<time::Date> {
    let mut parts = raw.splitn(3, '-');
    let year = parts.next()?.parse().ok()?;
    let month = time::Month::try_from(parts.next()?.parse::<u8>().ok()?).ok()?;
    let day = parts.next()?.parse().ok()?;
    time::Date::from_calendar_date(year, month, day).ok()
}

/// `Token expires <YYYY-MM-DD> (<n> days)` counted from `today`, or `Token has
/// no expiry`.
fn expiry_line(expires_at: Option<&str>, today: time::Date) -> Result<String> {
    let Some(raw) = expires_at else {
        return Ok("Token has no expiry".to_string());
    };
    let date = parse_date(raw).ok_or_else(|| {
        CliError::failure(format!(
            "GitLab reported the token's expiry as {raw:?}, which is not a date"
        ))
    })?;
    Ok(format!(
        "Token expires {raw} ({} days)",
        (date - today).whole_days()
    ))
}

/// The deploy argvs, in order: `cluster deploy` of `dir` as the agent, then
/// its one `gitlab=<path>` surface, carrying `--label` when set.
fn deploy_argvs(plan: &GitlabPlan, dir: &Path) -> Vec<Vec<String>> {
    let context = &plan.install.context;
    let mut deploy: Vec<String> = vec![
        "cluster".into(),
        "deploy".into(),
        "--context".into(),
        context.clone(),
        "--namespace".into(),
        plan.namespace.clone(),
        "--release".into(),
        plan.release.clone(),
        "--plugin-dir".into(),
        dir.display().to_string(),
        "--agent".into(),
        plan.agent.clone(),
        "--env".into(),
        "prod".into(),
    ];
    if let Some(chart) = &plan.deploy_chart {
        deploy.extend(["--chart".into(), chart.clone()]);
    }
    let mut surface = vec!["--add".into(), format!("gitlab={}", plan.facts.path)];
    if let Some(label) = &plan.label {
        surface.extend(["--label".into(), label.clone()]);
    }
    vec![
        deploy,
        cluster_agent(
            "surfaces",
            &plan.agent,
            context,
            &plan.namespace,
            &plan.release,
            &surface,
        ),
    ]
}

fn bundle_path(plan: &GitlabPlan) -> PathBuf {
    match &plan.bundle {
        Some(dir) => dir.clone(),
        None => bundle_dir(&plan.namespace, &plan.release, &plan.agent),
    }
}

fn settings(plan: &GitlabPlan) -> Vec<(Vec<String>, &'static str)> {
    agent_settings(
        &plan.agent,
        &plan.install.context,
        &plan.namespace,
        &plan.release,
        plan.deadline_seconds,
        plan.budget_usd,
    )
}

fn entries_what(plan: &GitlabPlan) -> String {
    format!(
        "the GitLab code host, tracker and binding `{}`",
        plan.entries.binding["name"].as_str().unwrap_or_default()
    )
}

fn describe(plan: &GitlabPlan) -> Vec<String> {
    let found = &plan.found;
    let facts = &plan.facts;
    let mut lines = vec![
        plan.install.context_reason.clone(),
        credential_line(&plan.credential).to_string(),
        format!("GitLab {} at {}: 17.0 or later", found.version, facts.web_base),
        plan.expiry.clone(),
        format!(
            "token user {}: active, with the api and write_repository scopes",
            found.username
        ),
        format!(
            "project {} has id {}, default branch {}, and issues enabled",
            found.path,
            found.project_id,
            found.default_branch.as_deref().unwrap_or("(none)")
        ),
        format!(
            "{} has access level {} ({}) on {}",
            found.username,
            found.access_level,
            api::role(found.access_level),
            found.path
        ),
        format!(
            "ensure labels {} on project {}, creating only the missing ones, before any cluster step",
            wanted_labels(&facts.label)
                .into_iter()
                .map(|(name, _)| name)
                .collect::<Vec<_>>()
                .join(", "),
            found.project_id
        ),
    ];
    if plan.install.skipped_cluster_up {
        lines.push(SKIPPED_CLUSTER_UP_LINE.to_string());
    }
    lines.extend(plan.install.actions.iter().filter_map(install_action_line));
    lines.push(format!(
        "write Secret {SECRET_NAME} key token in namespace {} from --token-file {} (the token is never printed)",
        plan.namespace,
        plan.token_file.display()
    ));
    if let Some(ca) = &plan.ca_file {
        lines.push(format!(
            "write ConfigMap {CA_CONFIG_MAP} key ca.crt in namespace {} from --ca-file {} and set codeHostTrust.caBundle.configMapRef.name={CA_CONFIG_MAP}",
            plan.namespace,
            ca.display()
        ));
    }
    let expiry = match &facts.expires_at {
        Some(date) => format!("credential expiresAt {date}T00:00:00Z"),
        None => "credential neverExpires".to_string(),
    };
    lines.push(format!(
        "write factory.enabled=true, factory.intake=poll, factory.codeHosts and factory.trackers entry {ENTRY_NAME} (apiUrl {}/api/v4, label {}, mention {}, {expiry}) and factory.bindings entry {} (project_id and tracker_scope_id {}, path {}): helm upgrade --reuse-values of release {}, then roll the API",
        facts.web_base,
        facts.label,
        facts.username,
        plan.entries.binding["name"].as_str().unwrap_or_default(),
        facts.project_id,
        facts.path,
        plan.release
    ));
    lines.push(format!(
        "forge egress for {ENTRY_NAME}: {}",
        egress_prose(&facts.egress)
    ));
    lines.push(forge_egress_line(&plan.agent));
    lines.push(bundle_line(
        plan.bundle.as_deref(),
        plan.runner_image.as_deref(),
    ));
    for args in deploy_argvs(plan, &bundle_path(plan)) {
        lines.push(format!("curie {}", args.join(" ")));
    }
    for (args, _) in settings(plan) {
        lines.push(format!("curie {}", args.join(" ")));
    }
    lines
}

/// The ConfigMap holding the CA bundle under `ca.crt`.
fn ca_manifest(namespace: &str, pem: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": CA_CONFIG_MAP, "namespace": namespace},
        "data": {"ca.crt": pem},
    })
}

pub(in crate::factory_quickstart) async fn quickstart(
    opts: QuickstartOpts,
) -> Result<QuickstartOutput> {
    // Every flag refusal comes before any read.
    let path = project_path(&opts.repos)?;
    refuse_github_flags(&opts)?;
    validate_agent_flags(&opts.agent, opts.label.as_deref())?;
    let web_base = gitlab_web_base(&opts.forge)?;
    let token_file = opts.forge.token_file.clone().ok_or_else(|| {
        CliError::usage("--code-host gitlab needs --token-file")
            .with_fix("pass --token-file <PATH>")
    })?;
    let runner_image = match &opts.plugin_dir {
        Some(dir) => crate::factory_quickstart::github::check_plugin_dir(dir)?,
        None => None,
    };
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

    // Steps a to e, under --dry-run too. Nothing is written yet.
    let gitlab = Gitlab::connect(&web_base, &token_file, opts.forge.ca_file.as_deref())?;
    let found = api::preflight(&gitlab, &path).await?;
    let expiry = expiry_line(
        found.expires_at.as_deref(),
        time::OffsetDateTime::now_utc().date(),
    )?;
    let label = opts
        .label
        .clone()
        .unwrap_or_else(|| crate::factory_app::DEFAULT_LABEL.to_string());
    let facts = GitlabFacts {
        egress: web_egress(&web_base)?,
        web_base,
        username: found.username.clone(),
        expires_at: found.expires_at.clone(),
        project_id: found.project_id,
        path: found.path.clone(),
        label,
        ca_config_map: opts
            .forge
            .ca_file
            .as_ref()
            .map(|_| CA_CONFIG_MAP.to_string()),
    };
    let entries = gitlab_entries(&facts);

    // Confirm before any write. A non-terminal refusal here mutates nothing.
    let real_interactive = std::io::stdin().is_terminal();
    if !opts.dry_run {
        crate::ui::ui().note(&expiry);
        if let Some(name) = unconfirmed_remote_context(opts.context.as_deref(), current.as_deref())
        {
            confirm_remote_context(name, real_interactive)?;
        }
        // Step f, the last step before any cluster call.
        crate::ui::ui().note("Ensuring the GitLab labels");
        let created =
            api::ensure_labels(&gitlab, found.project_id, &wanted_labels(&facts.label)).await?;
        if !created.is_empty() {
            crate::ui::ui().note(&format!("Created labels {}", created.join(", ")));
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
    if let (Some(context), false) = (&targeted, opts.dry_run) {
        // Helm value reads have no context flag. Pin first so a rerun against
        // --context sees that release, not the ambient one.
        crate::kube_context::pin_for_cluster_command(Some(context))?;
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
    let install = install_steps(&InstallTarget {
        explicit_context: opts.context.as_deref(),
        current_context: current.as_deref(),
        kind_name: &opts.kind_name,
        existing_kind_clusters: &existing,
        namespace: &opts.namespace,
        release: &opts.release,
        model: &model,
        chart: &opts.chart,
        release_at_target,
        add_agent: false,
    });
    let plan = GitlabPlan {
        install,
        credential: credential_decision(
            credential_in_env,
            release_has_real_model,
            real_interactive,
        ),
        found,
        expiry,
        facts,
        entries,
        token_file,
        ca_file: opts.forge.ca_file.clone(),
        namespace: opts.namespace.clone(),
        release: opts.release.clone(),
        agent: opts.agent.clone(),
        label: opts.label.clone(),
        bundle: opts.plugin_dir.clone(),
        runner_image,
        deploy_chart: opts.chart_explicit.then(|| opts.chart.clone()),
        deadline_seconds: opts.execution_deadline_seconds,
        budget_usd: opts.budget_usd,
    };
    if opts.dry_run {
        return Ok(QuickstartOutput::DryRun(DryRunPlan {
            lines: describe(&plan),
        }));
    }
    if plan.credential == CredentialDecision::RefuseNonInteractive {
        return Err(
            CliError::usage("model credential needed and stdin is not a terminal")
                .with_fix(
                    "set CURIE_CREDENTIALS to an Anthropic API key starting with sk-ant- or an OpenRouter key starting with sk-or- and rerun",
                )
                .into(),
        );
    }
    execute(&plan, &opts, release_has_real_model).await
}

async fn execute(
    plan: &GitlabPlan,
    opts: &QuickstartOpts,
    mut release_has_real_model: bool,
) -> Result<QuickstartOutput> {
    let ui = crate::ui::ui();
    ui.note(&format!("Kubernetes context: {}", plan.install.context));
    if plan.install.skipped_cluster_up {
        ui.note("release already at the target chart and values; skipping cluster up");
    }
    let mut prompted_model = None;
    for action in &plan.install.actions {
        if matches!(action, Action::Cluster { .. }) {
            if plan.install.kind_cluster.is_some() {
                // The plan could not read the release: the context did not
                // exist yet. Read it now, before `cluster up` records a key.
                release_has_real_model =
                    crate::kube_context::pin_for_cluster_command(Some(&plan.install.context))
                        .is_err()
                        || release_model_recorded(opts).await.unwrap_or(true);
            }
            ensure_credential(&plan.credential)?;
            if plan.credential == CredentialDecision::PromptOnce {
                let credential = crate::ops::model_credential_env()?;
                prompted_model = Some(quickstart_model(
                    opts.model.as_deref(),
                    credential.as_deref(),
                ));
            }
        }
        run_install_action(action, prompted_model.as_ref()).await?;
    }
    let common = CommonOpts {
        namespace: plan.namespace.clone(),
        release: plan.release.clone(),
        dry_run: false,
    };
    // The bundle's runner is checked against the installation before the
    // first forge write, so a mismatch leaves the release as it was.
    let dir = bundle_path(plan);
    let image = match &plan.bundle {
        Some(dir) => {
            crate::cluster_secrets::check_layered_runner_base(&common, dir).await?;
            plan.runner_image.clone()
        }
        None => Some(
            crate::factory_quickstart::github::render_and_check(&common, &plan.agent, None).await?,
        ),
    };
    let token = read_token(&plan.token_file)?;
    apply_manifest(
        &plan.namespace,
        &token_secret_manifest(&plan.namespace, SECRET_NAME, &token),
        &format!("Secret {SECRET_NAME}"),
    )
    .await?;
    if let Some(ca) = &plan.ca_file {
        let pem = std::fs::read_to_string(ca).map_err(|error| {
            CliError::failure(format!(
                "could not read --ca-file {}: {error}",
                ca.display()
            ))
        })?;
        apply_manifest(
            &plan.namespace,
            &ca_manifest(&plan.namespace, &pem),
            &format!("ConfigMap {CA_CONFIG_MAP}"),
        )
        .await?;
    }
    apply_forge_entries(&common, &opts.chart, &entries_what(plan), |recorded| {
        native_forge_values(recorded, &plan.entries)
    })
    .await?;
    let chart = opts.chart.clone();
    crate::cluster_secrets::bind_forge_egress(common.clone(), plan.agent.clone(), async move {
        Ok(chart)
    })
    .await?;
    let credit_remaining_usd = check_credit(release_has_real_model).await;
    for (index, args) in deploy_argvs(plan, &dir).into_iter().enumerate() {
        let step = match (index, plan.agent == DEFAULT_AGENT) {
            (0, true) => "Deploying dark factory".to_string(),
            (0, false) => format!("Deploying {}", plan.agent),
            _ => "Binding the GitLab project".to_string(),
        };
        run_self(&args, &step).await?;
    }
    for (args, step) in settings(plan) {
        run_self(&args, step).await?;
    }
    Ok(QuickstartOutput::AgentAdded {
        context: plan.install.context.clone(),
        agent: plan.agent.clone(),
        repos: vec![plan.facts.path.clone()],
        trigger_label: plan.label.clone(),
        effective_label: plan.facts.label.clone(),
        bundle: match &plan.bundle {
            Some(dir) => dir.display().to_string(),
            None => "published".to_string(),
        },
        runner_image: image,
        deadline_seconds: plan.deadline_seconds,
        budget_usd: plan.budget_usd,
        credit_remaining_usd,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_expiry_line_counts_days_from_today() {
        let today = parse_date("2026-10-13").unwrap();
        assert_eq!(
            expiry_line(Some("2026-11-12"), today).unwrap(),
            "Token expires 2026-11-12 (30 days)"
        );
        assert_eq!(expiry_line(None, today).unwrap(), "Token has no expiry");
        assert!(expiry_line(Some("soon"), today).is_err());
    }
}

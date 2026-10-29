//! The GitLab quickstart run: flags, the preflight (steps a to e), the
//! release's recorded instances, Secrets and CA bundle (#4620), the labels
//! (step f), then the install, the forge entries and the deploy, or under
//! `--dry-run` the plan of them after steps a to e.

use std::io::IsTerminal;
use std::path::{Path, PathBuf};

use anyhow::Result;

use super::api::{self, Found, Gitlab};
use super::{
    egress_prose, gitlab_entries, gitlab_web_base, project_path, resolve_gitlab_instance,
    wanted_labels, web_egress, CredentialRef, GitlabFacts,
};
use crate::exit::CliError;
use crate::factory_intake::{
    apply_forge_entries, native_forge_values, refuse_planned_factory, NativeForgeEntries,
};
use crate::factory_quickstart::instance::{self, Credential, ObjectWrite, Observation};
use crate::factory_quickstart::{
    agent_settings, bundle_dir, bundle_line, check_credit, check_prerequisites, cluster_agent,
    confirm_remote_context, credential_line, deployed_release_snapshot, ensure_factory_agent,
    forge_egress_line, initial_agent_line, install_plan_lines, keep_surface_plan_line,
    kept_surface_line, native, read_token, run_self, surface_to_keep, unconfirmed_remote_context,
    validate_agent_flags, Action, CredentialDecision, InstallSteps, QuickstartOpts,
    QuickstartOutput, DEFAULT_AGENT,
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
    /// The resolved entries: a recorded one as recorded, a new one under its
    /// derived name.
    entries: NativeForgeEntries,
    /// Which of the code host, tracker and binding the release records.
    recorded: [bool; 3],
    /// The credential Secrets the entries name.
    credentials: Vec<CredentialRef>,
    /// The new token Secrets to create: name and key. Empty under
    /// `--dry-run`, which describes `credentials` instead.
    secret_creates: Vec<(String, String)>,
    /// The CA bundle ConfigMap and key, and whether the release references
    /// none yet, for the plan text.
    trust_target: Option<(String, String, bool)>,
    /// The CA bundle write, when one is needed.
    trust: Option<ObjectWrite>,
    /// What the run read before its first write.
    observation: Observation,
    token_file: PathBuf,
    ca_file: Option<PathBuf>,
    namespace: String,
    release: String,
    agent: String,
    /// The agent's own binding label (`--label`); `None` lets the tracker
    /// label serve the binding.
    label: Option<String>,
    effective_label: String,
    /// The operator bundle; `None` renders the published dark factory.
    bundle: Option<PathBuf>,
    /// The operator bundle's locked runner image.
    runner_image: Option<String>,
    /// The operator's `--chart`, passed on to `cluster deploy`.
    deploy_chart: Option<String>,
    deadline_seconds: u32,
    budget_usd: f64,
    output_tokens: u64,
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
        plan.output_tokens,
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
        credential_line(&plan.credential, plan.install.cluster_up),
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
            wanted_labels(&plan.effective_label)
                .into_iter()
                .map(|(name, _)| name)
                .collect::<Vec<_>>()
                .join(", "),
            found.project_id
        ),
    ];
    lines.extend(install_plan_lines(
        plan.install.cluster_up,
        &plan.install.actions,
        &plan.release,
        &plan.namespace,
    ));
    for credential in &plan.credentials {
        lines.push(if credential.recorded {
            format!(
                "reuse Secret {} key {}, the credential factory entry {} records: it must hold the --token-file token and is never rewritten",
                credential.secret, credential.key, credential.entry
            )
        } else {
            format!(
                "write Secret {} key {} in namespace {} from --token-file {} (the token is never printed), unless this release's quickstart already left it holding that token",
                credential.secret,
                credential.key,
                plan.namespace,
                plan.token_file.display()
            )
        });
    }
    if let (Some(ca), Some((name, key, set_ref))) = (&plan.ca_file, &plan.trust_target) {
        lines.push(if *set_ref {
            format!(
                "write ConfigMap {name} key {key} in namespace {} from --ca-file {} and set codeHostTrust.caBundle.configMapRef.name={name}",
                plan.namespace,
                ca.display()
            )
        } else {
            format!(
                "merge the --ca-file {} certificates into ConfigMap {name} key {key}, the release's codeHostTrust bundle, adding only the missing ones",
                ca.display()
            )
        });
    }
    let expiry = match &facts.expires_at {
        Some(date) => format!("credential expiresAt {date}T00:00:00Z"),
        None => "credential neverExpires".to_string(),
    };
    let name = |entry: &serde_json::Value, recorded: bool| {
        let name = entry["name"].as_str().unwrap_or_default().to_string();
        if recorded {
            format!("{name} (kept as recorded)")
        } else {
            name
        }
    };
    lines.push(format!(
        "write factory.enabled=true, factory.intake=poll, factory.codeHosts entry {} and factory.trackers entry {} (apiUrl {}/api/v4, label {}, mention {}, {expiry}) and factory.bindings entry {} (project_id and tracker_scope_id {}, path {}): helm upgrade --reuse-values of release {}, then roll the API",
        name(&plan.entries.code_host, plan.recorded[0]),
        name(&plan.entries.tracker, plan.recorded[1]),
        facts.web_base,
        facts.label,
        facts.username,
        name(&plan.entries.binding, plan.recorded[2]),
        facts.project_id,
        facts.path,
        plan.release
    ));
    lines.push(format!(
        "forge egress for {}: {}",
        plan.entries.code_host["name"].as_str().unwrap_or_default(),
        egress_prose(&facts.egress)
    ));
    lines.push(forge_egress_line(&plan.agent));
    lines.push(bundle_line(
        plan.bundle.as_deref(),
        plan.runner_image.as_deref(),
    ));
    lines.push(initial_agent_line(
        &plan.agent,
        &crate::api::add_channel_body(
            "gitlab",
            &plan.facts.path,
            None,
            None,
            plan.label.as_deref(),
        ),
    ));
    for args in deploy_argvs(plan, &bundle_path(plan)) {
        lines.push(format!("curie {}", args.join(" ")));
    }
    if plan.label.is_none() {
        lines.push(keep_surface_plan_line("gitlab", &plan.facts.path));
    }
    for (args, _) in settings(plan) {
        lines.push(format!("curie {}", args.join(" ")));
    }
    lines
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
    // A malformed CA bundle refuses before any network read.
    let ca_certificates = match &opts.forge.ca_file {
        Some(ca) => Some(instance::read_ca_file(ca)?),
        None => None,
    };
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
    let real_interactive = std::io::stdin().is_terminal();
    if !opts.dry_run {
        crate::ui::ui().note(&expiry);
        if let Some(name) = unconfirmed_remote_context(opts.context.as_deref(), current.as_deref())
        {
            confirm_remote_context(name, real_interactive)?;
        }
    }
    let planned = native::plan_install(&opts, current.as_deref(), real_interactive).await?;
    let creates_kind = planned
        .install
        .actions
        .iter()
        .any(Action::creates_kind_cluster);
    let common = CommonOpts {
        namespace: opts.namespace.clone(),
        release: opts.release.clone(),
        dry_run: false,
    };
    let mut assumed = None;
    let snapshot = if opts.dry_run {
        match opts.context.as_deref() {
            None => {
                assumed = Some("no --context was passed, so the release was not read; planned as if it records no GitLab instance".to_string());
                None
            }
            Some(context) => {
                let read = match crate::kube_context::pin_for_cluster_command(Some(context)) {
                    Ok(_) => deployed_release_snapshot(&common).await,
                    Err(error) => Err(error),
                };
                match read {
                    Ok(snapshot) => snapshot,
                    Err(error) => {
                        assumed = Some(format!(
                            "the release could not be read ({error:#}); planned as if it records no GitLab instance"
                        ));
                        None
                    }
                }
            }
        }
    } else if !creates_kind {
        crate::kube_context::pin_for_cluster_command(Some(&planned.install.context))?;
        deployed_release_snapshot(&common).await?
    } else {
        None
    };
    let values = snapshot
        .as_ref()
        .map_or(serde_json::Value::Null, |snapshot| snapshot.values.clone());
    let trust_target =
        opts.forge
            .ca_file
            .as_ref()
            .map(|_| match instance::recorded_trust_ref(&values) {
                Some((name, key)) => (name, key, false),
                None => (
                    instance::code_host_trust_name(&opts.release),
                    instance::TRUST_KEY.to_string(),
                    true,
                ),
            });
    let mut facts = GitlabFacts {
        release: opts.release.clone(),
        egress: web_egress(&web_base)?,
        web_base,
        username: found.username.clone(),
        expires_at: found.expires_at.clone(),
        project_id: found.project_id,
        path: found.path.clone(),
        // A new tracker's label is the default, never --label (#4620).
        label: crate::factory_app::DEFAULT_LABEL.to_string(),
        ca_config_map: trust_target
            .as_ref()
            .filter(|(_, _, set_ref)| *set_ref)
            .map(|(name, _, _)| name.clone()),
    };
    // The recorded instance, found by identity; refusals come before any read
    // of the cluster's objects and before step f.
    let resolution = resolve_gitlab_instance(&values, gitlab_entries(&facts), &opts.release)?;
    facts.label = resolution.label.clone();
    let effective_label = opts.label.clone().unwrap_or_else(|| facts.label.clone());
    // The document the forge entries' upgrade writes, checked against the
    // release now, so a binding or reference conflict refuses before step f
    // and the install rather than after them.
    let document = native_forge_values(&values, &resolution.entries)?;
    refuse_planned_factory(&values, &document)?;

    // The Secrets and the CA bundle, read and checked before step f.
    let mut observation = Observation::new(&common, snapshot.as_ref().map(|s| s.revision));
    let mut secret_creates = Vec::new();
    let mut trust = None;
    if !opts.dry_run && !creates_kind {
        let token = read_token(&token_file)?;
        let credentials: Vec<Credential<'_>> = resolution
            .credentials
            .iter()
            .map(|credential| Credential {
                entry: &credential.entry,
                secret: credential.secret.clone(),
                key: credential.key.clone(),
                recorded: credential.recorded,
            })
            .collect();
        secret_creates = instance::check_credentials(
            &common,
            &credentials,
            &token,
            "--token-file",
            &mut observation,
        )
        .await?;
        if let Some(certificates) = &ca_certificates {
            let (planned, read) = instance::plan_trust(&common, &values, certificates).await?;
            observation.push(read);
            trust = planned;
        }
    } else if !opts.dry_run {
        // A cluster this run creates holds nothing yet.
        secret_creates = resolution
            .credentials
            .iter()
            .filter(|credential| !credential.recorded)
            .map(|credential| (credential.secret.clone(), credential.key.clone()))
            .collect();
        if let Some(certificates) = &ca_certificates {
            trust = Some(instance::new_trust(&common, certificates));
        }
    }

    if !opts.dry_run {
        crate::ui::ui().note("Ensuring the GitLab labels");
        let created =
            api::ensure_labels(&gitlab, found.project_id, &wanted_labels(&effective_label)).await?;
        if !created.is_empty() {
            crate::ui::ui().note(&format!("Created labels {}", created.join(", ")));
        }
        // An operator change while the labels were written refuses the run.
        observation.recheck().await?;
    }

    let release_has_real_model = planned.release_has_real_model;
    let plan = GitlabPlan {
        install: planned.install,
        credential: planned.credential,
        found,
        expiry,
        facts,
        entries: resolution.entries,
        recorded: resolution.recorded,
        credentials: resolution.credentials,
        secret_creates,
        trust_target,
        trust,
        observation,
        token_file,
        ca_file: opts.forge.ca_file.clone(),
        namespace: opts.namespace.clone(),
        release: opts.release.clone(),
        agent: opts.agent.clone(),
        label: opts.label.clone(),
        effective_label,
        bundle: opts.plugin_dir.clone(),
        runner_image,
        deploy_chart: opts.chart_explicit.then(|| opts.chart.clone()),
        deadline_seconds: opts.execution_deadline_seconds,
        budget_usd: opts.budget_usd,
        output_tokens: opts.output_tokens,
    };
    if opts.dry_run {
        let mut lines = describe(&plan);
        if let Some(note) = assumed {
            lines.insert(2, note);
        }
        return Ok(QuickstartOutput::DryRun(DryRunPlan { lines }));
    }
    native::refuse_without_credential(&plan.credential)?;
    execute_gitlab(plan, &opts, release_has_real_model).await
}

async fn execute_gitlab(
    mut plan: GitlabPlan,
    opts: &QuickstartOpts,
    release_has_real_model: bool,
) -> Result<QuickstartOutput> {
    let release_has_real_model = native::run_install(
        &plan.install,
        &plan.credential,
        release_has_real_model,
        opts,
        &mut plan.observation,
    )
    .await?;
    let common = CommonOpts {
        namespace: plan.namespace.clone(),
        release: plan.release.clone(),
        dry_run: false,
    };
    let dir = bundle_path(&plan);
    let image = native::check_bundle(
        &common,
        &plan.agent,
        plan.bundle.as_deref(),
        plan.runner_image.as_deref(),
    )
    .await?;
    let mut writes = Vec::new();
    if !plan.secret_creates.is_empty() {
        let token = read_token(&plan.token_file)?;
        for (secret, key) in &plan.secret_creates {
            writes.push(ObjectWrite::create(instance::token_secret(
                &plan.namespace,
                &plan.release,
                secret,
                key,
                &token,
            )));
        }
    }
    if let Some(write) = plan.trust.take() {
        writes.push(write);
    }
    // The objects must still be as observed, and the release at the
    // revision observed or, after the install's own upgrade, adopted.
    apply_forge_entries(
        &common,
        &opts.chart,
        &entries_what(&plan),
        &plan.observation,
        &writes,
        |recorded| native_forge_values(recorded, &plan.entries),
    )
    .await?;
    let plan = &plan;
    let chart = opts.chart.clone();
    crate::cluster_secrets::bind_forge_egress(common.clone(), plan.agent.clone(), async move {
        Ok(chart)
    })
    .await?;
    let credit_remaining_usd = check_credit(release_has_real_model).await;
    let held = ensure_factory_agent(
        &common,
        &plan.agent,
        crate::api::add_channel_body(
            "gitlab",
            &plan.facts.path,
            None,
            None,
            plan.label.as_deref(),
        ),
        None,
    )
    .await?;
    // A label-less rerun keeps the surface the agent already holds (#4620).
    let kept = surface_to_keep(&held, "gitlab", &plan.facts.path, plan.label.as_deref());
    for (index, args) in deploy_argvs(plan, &dir).into_iter().enumerate() {
        if index > 0 && kept.is_some() {
            crate::ui::ui().note(&kept_surface_line(&plan.agent, "gitlab", &plan.facts.path));
            continue;
        }
        let step = match (index, plan.agent == DEFAULT_AGENT) {
            (0, true) => "Deploying dark factory".to_string(),
            (0, false) => format!("Deploying {}", plan.agent),
            _ => "Binding the GitLab project".to_string(),
        };
        run_self(&args, &step, None, None).await?;
    }
    for (args, step) in settings(plan) {
        run_self(&args, step, None, None).await?;
    }
    Ok(QuickstartOutput::AgentAdded {
        context: plan.install.context.clone(),
        agent: plan.agent.clone(),
        repos: vec![plan.facts.path.clone()],
        trigger_label: plan.label.clone().or_else(|| kept.clone().flatten()),
        effective_label: kept
            .clone()
            .flatten()
            .unwrap_or_else(|| plan.effective_label.clone()),
        bundle: match &plan.bundle {
            Some(dir) => dir.display().to_string(),
            None => "published".to_string(),
        },
        runner_image: image,
        deadline_seconds: plan.deadline_seconds,
        budget_usd: plan.budget_usd,
        output_tokens: plan.output_tokens,
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

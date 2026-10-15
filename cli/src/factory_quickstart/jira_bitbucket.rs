//! Jira Cloud as the tracker with a Bitbucket Cloud or Data Center code host
//! in `curie factory quickstart` (#4400, #4401).
//!
//! Bitbucket has no issue tracker, so the ticket is a Jira issue and the pull
//! request, its build statuses and its review comments are on Bitbucket. The
//! run reads the Jira site and the Bitbucket repository, also under
//! `--dry-run`, then installs, writes the Bitbucket code host, the Jira
//! tracker and one Jira binding naming the repository, the two token Secrets
//! and, with `--ca-file`, the CA ConfigMap, and deploys the agent bound
//! through `jira=<binding>`.
//!
//! Bitbucket takes `--forge-url` (Data Center only), `--token-file`,
//! `--ca-file` and `--review-account`. Jira takes `--tracker-url`,
//! `--tracker-token-file`, `--forge-username` and `--start-account` or
//! `--start-group`. Each token is read from its file and goes only into the
//! preflight requests and its Secret: never into values, the plan, argv or a
//! message.

use std::io::IsTerminal;
use std::path::{Path, PathBuf};

use anyhow::Result;
use serde_json::{json, Value};

use super::jira_cloud::{self, preflight_site, JiraSite};
use super::native::{self, NativeInstall};
use super::{
    agent_settings, bitbucket_cloud, bitbucket_dc, bundle_dir, bundle_line, check_credit,
    check_prerequisites, cluster_agent, confirm_remote_context, credential_line,
    ensure_factory_agent, forge_egress_line, initial_agent_line, install_action_line, read_token,
    resolve_host, run_self, token_secret_manifest, unconfirmed_remote_context,
    validate_agent_flags, CodeHostKind, EgressEntry, QuickstartOpts, QuickstartOutput,
    DEFAULT_AGENT, SKIPPED_CLUSTER_UP_LINE,
};
use crate::exit::CliError;
use crate::factory_intake::{apply_forge_entries, native_forge_values, NativeForgeEntries};
use crate::ops::CommonOpts;
use crate::ui::DryRunPlan;

/// The name of the code host entry this module writes.
const CODE_HOST_NAME: &str = "bitbucket";
/// The Secret the code host entry names, under the data key `token`.
const CODE_HOST_SECRET: &str = "curie-bitbucket-token";
/// The ConfigMap holding `--ca-file`, under the data key `ca.crt`.
const CA_CONFIG_MAP: &str = "curie-bitbucket-ca";
const CLOUD_API: &str = "https://api.bitbucket.org/2.0";
const CLOUD_HOST: &str = "bitbucket.org";

/// What the Bitbucket probe learned and the entries written from it.
struct CodeHost {
    /// The `factory.codeHosts[]` entry.
    entry: Value,
    /// `<workspace>/<slug>` or `<PROJECT KEY>/<slug>`.
    path: String,
    project_id: String,
    /// The probe's findings, one plan line each.
    found: Vec<String>,
    egress: Vec<EgressEntry>,
}

/// Everything a run applies, in the order it applies it.
struct Plan {
    native: NativeInstall,
    site: JiraSite,
    code_host: CodeHost,
    entries: NativeForgeEntries,
    token_file: PathBuf,
    ca_file: Option<PathBuf>,
    namespace: String,
    release: String,
    agent: String,
    /// The agent's own binding label (`--label`); `None` lets the tracker
    /// label serve the binding.
    label: Option<String>,
    bundle: Option<PathBuf>,
    runner_image: Option<String>,
    deploy_chart: Option<String>,
    deadline_seconds: u32,
    budget_usd: f64,
}

fn usage(message: impl Into<String>, fix: impl Into<String>) -> anyhow::Error {
    CliError::usage(message.into()).with_fix(fix.into()).into()
}

/// `--app-id`, `--private-key-file` and `--org` configure a GitHub App, which
/// a Bitbucket install has none of.
fn refuse_github_flags(opts: &QuickstartOpts) -> Result<()> {
    let set = [
        ("--app-id", opts.app_id.is_some()),
        ("--private-key-file", opts.private_key_file.is_some()),
        ("--org", opts.org.is_some()),
    ];
    match set.iter().find(|(_, set)| *set) {
        Some((flag, _)) => Err(usage(
            format!("{flag} configures the GitHub App and does not apply to a Bitbucket code host; the Bitbucket token comes from --token-file"),
            format!("drop {flag}"),
        )),
        None => Ok(()),
    }
}

/// The one `--repo` a Bitbucket quickstart serves.
fn one_repo(repos: &[String]) -> Result<&str> {
    match repos {
        [repo] => Ok(repo),
        _ => Err(usage(
            "a Bitbucket code host serves one repository; pass --repo once",
            "pass --repo <workspace>/<repo slug> on Cloud or <PROJECT KEY>/<repo slug> on Data Center",
        )),
    }
}

/// Probes the repository with the factory's token and builds the code host
/// entry. Reads only.
async fn probe_code_host(opts: &QuickstartOpts, repo: &str) -> Result<CodeHost> {
    let forge = &opts.forge;
    let token_file = forge.token_file.as_deref().unwrap_or(Path::new(""));
    let ca_file = forge.ca_file.as_deref();
    let reviewers = &forge.review_accounts;
    let (values, found, egress) = match forge.code_host {
        CodeHostKind::BitbucketCloud => {
            let parsed = bitbucket_cloud::parse_repo(repo)?;
            let probed = bitbucket_cloud::probe(CLOUD_API, token_file, ca_file, &parsed).await?;
            let egress = bitbucket_cloud::cloud_egress(&bitbucket_cloud::fetch_ip_ranges().await?)?;
            let found = vec![
                format!(
                    "Bitbucket Cloud repository {} has id {}",
                    probed.path, probed.project_id
                ),
                bitbucket_cloud::expiry_line(CODE_HOST_NAME),
            ];
            let values = bitbucket_cloud::values(
                CODE_HOST_NAME,
                &probed,
                CLOUD_API,
                CLOUD_HOST,
                reviewers,
                &egress,
            );
            (values, found, egress)
        }
        CodeHostKind::BitbucketDc => {
            let raw = forge.forge_url.as_deref().unwrap_or_default();
            let base = raw.trim_end_matches('/');
            let parsed = bitbucket_dc::parse_repo(repo)?;
            let probed = bitbucket_dc::probe(base, token_file, ca_file, &parsed).await?;
            let egress = bitbucket_dc::dc_egress(base, resolve_host)?;
            let host = reqwest::Url::parse(base)
                .ok()
                .and_then(|url| url.host_str().map(str::to_string))
                .unwrap_or_default();
            let found = vec![
                format!(
                    "Bitbucket Data Center {} at {base}: 9.4.0 or later",
                    probed.version
                ),
                format!(
                    "Bitbucket Data Center repository {} has id {}",
                    probed.path, probed.project_id
                ),
                bitbucket_dc::expiry_line(CODE_HOST_NAME),
            ];
            let values =
                bitbucket_dc::values(CODE_HOST_NAME, &probed, base, &host, reviewers, &egress);
            (values, found, egress)
        }
        CodeHostKind::Github | CodeHostKind::Gitlab => {
            unreachable!("the quickstart dispatches only Bitbucket kinds here")
        }
    };
    let repo = &values["repo"];
    Ok(CodeHost {
        entry: values["codeHosts"][0].clone(),
        path: repo["path"].as_str().unwrap_or_default().to_string(),
        project_id: repo["projectId"].as_str().unwrap_or_default().to_string(),
        found,
        egress,
    })
}

/// The Jira binding naming the one Bitbucket repository, with the review
/// allowlist under the code host's name.
fn binding(site: &JiraSite, code_host: &CodeHost, reviewers: &[String]) -> Value {
    let mut binding = jira_cloud::binding_value(
        &site.cloud_id,
        CODE_HOST_NAME,
        &[(code_host.path.clone(), code_host.project_id.clone())],
        &site.flags.start,
    );
    binding["reviewFeedbackAllowlist"] = json!({ CODE_HOST_NAME: reviewers });
    binding
}

fn bundle_path(plan: &Plan) -> PathBuf {
    match &plan.bundle {
        Some(dir) => dir.clone(),
        None => bundle_dir(&plan.namespace, &plan.release, &plan.agent),
    }
}

/// The deploy argvs, in order: `cluster deploy` of `dir` as the agent, then
/// its one `jira=<binding>` surface, carrying `--label` when set.
fn deploy_argvs(plan: &Plan, dir: &Path) -> Vec<Vec<String>> {
    let context = &plan.native.install.context;
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
    let mut surface = vec!["--add".into(), format!("jira={}", jira_cloud::BINDING_NAME)];
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

fn settings(plan: &Plan) -> Vec<(Vec<String>, &'static str)> {
    agent_settings(
        &plan.agent,
        &plan.native.install.context,
        &plan.namespace,
        &plan.release,
        plan.deadline_seconds,
        plan.budget_usd,
    )
}

fn egress_prose(egress: &[EgressEntry]) -> String {
    let cidrs: Vec<&str> = egress.iter().map(|entry| entry.cidr.as_str()).collect();
    match cidrs.len() {
        0 => "none".to_string(),
        1..=4 => cidrs.join(", "),
        n => format!("{n} ranges"),
    }
}

fn describe(plan: &Plan) -> Vec<String> {
    let site = &plan.site;
    let flags = &site.flags;
    let mut lines = vec![
        plan.native.install.context_reason.clone(),
        credential_line(&plan.native.credential).to_string(),
        format!(
            "Jira site {} has cloud id {}; the factory account is {} ({})",
            site.host, site.cloud_id, flags.username, site.mention
        ),
    ];
    lines.extend(plan.code_host.found.iter().cloned());
    if plan.native.install.skipped_cluster_up {
        lines.push(SKIPPED_CLUSTER_UP_LINE.to_string());
    }
    lines.extend(
        plan.native
            .install
            .actions
            .iter()
            .filter_map(install_action_line),
    );
    lines.push(format!(
        "write Secret {} key {} in namespace {} from {} {} (the token is never printed)",
        jira_cloud::SECRET_NAME,
        jira_cloud::SECRET_KEY,
        plan.namespace,
        flags.token_flag,
        flags.token_file.display()
    ));
    lines.push(format!(
        "write Secret {CODE_HOST_SECRET} key token in namespace {} from --token-file {} (the token is never printed)",
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
    lines.push(format!(
        "write factory.enabled=true, factory.intake=poll, factory.codeHosts entry {CODE_HOST_NAME} ({}, repository {} id {}), factory.trackers entry {} and factory.bindings entry {} (default repository {}, reviews counted from {}): helm upgrade --reuse-values of release {}, then roll the API",
        plan.entries.code_host["kind"].as_str().unwrap_or_default(),
        plan.code_host.path,
        plan.code_host.project_id,
        jira_cloud::BINDING_NAME,
        jira_cloud::BINDING_NAME,
        plan.entries.binding["defaultRepo"].as_str().unwrap_or_default(),
        plan.entries.binding["reviewFeedbackAllowlist"][CODE_HOST_NAME]
            .as_array()
            .map(|ids| ids.iter().filter_map(Value::as_str).collect::<Vec<_>>().join(", "))
            .unwrap_or_default(),
        plan.release
    ));
    lines.push(format!(
        "forge egress for {CODE_HOST_NAME}: {}; for {}: {}",
        egress_prose(&plan.code_host.egress),
        jira_cloud::BINDING_NAME,
        egress_prose(&site.egress)
    ));
    lines.push(forge_egress_line(&plan.agent));
    lines.push(bundle_line(
        plan.bundle.as_deref(),
        plan.runner_image.as_deref(),
    ));
    lines.push(initial_agent_line(
        &plan.agent,
        &crate::api::add_channel_body(
            "jira",
            jira_cloud::BINDING_NAME,
            None,
            None,
            plan.label.as_deref(),
        ),
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
fn ca_manifest(namespace: &str, pem: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": CA_CONFIG_MAP, "namespace": namespace},
        "data": {"ca.crt": pem},
    })
}

pub(super) async fn quickstart(opts: QuickstartOpts) -> Result<QuickstartOutput> {
    // Every flag refusal comes before any read.
    let repo = one_repo(&opts.repos)?.to_string();
    refuse_github_flags(&opts)?;
    validate_agent_flags(&opts.agent, opts.label.as_deref())?;
    let jira_flags = jira_cloud::flags(&opts.forge)?;
    let token_file = opts.forge.token_file.clone().ok_or_else(|| {
        usage(
            "a Bitbucket code host needs --token-file",
            "pass --token-file <PATH>",
        )
    })?;
    let runner_image = match &opts.plugin_dir {
        Some(dir) => super::github::check_plugin_dir(dir)?,
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

    // The Jira and Bitbucket reads, under --dry-run too. Nothing is written yet.
    let site = preflight_site(jira_flags).await?;
    let code_host = probe_code_host(&opts, &repo).await?;
    let entries = NativeForgeEntries {
        code_host: code_host.entry.clone(),
        tracker: site.tracker(),
        binding: binding(&site, &code_host, &opts.forge.review_accounts),
        ca_config_map: opts
            .forge
            .ca_file
            .as_ref()
            .map(|_| CA_CONFIG_MAP.to_string()),
    };

    // Confirm before any write. A non-terminal refusal here mutates nothing.
    let real_interactive = std::io::stdin().is_terminal();
    if !opts.dry_run {
        if let Some(name) = unconfirmed_remote_context(opts.context.as_deref(), current.as_deref())
        {
            confirm_remote_context(name, real_interactive)?;
        }
    }
    let native = native::plan_install(&opts, current.as_deref(), real_interactive).await?;
    let plan = Plan {
        native,
        site,
        code_host,
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
    native::refuse_without_credential(&plan.native.credential)?;
    execute(&plan, &opts).await
}

async fn execute(plan: &Plan, opts: &QuickstartOpts) -> Result<QuickstartOutput> {
    let install = &plan.native;
    let release_has_real_model = native::run_install(
        &install.install,
        &install.credential,
        install.release_has_real_model,
        opts,
    )
    .await?;
    let common = CommonOpts {
        namespace: plan.namespace.clone(),
        release: plan.release.clone(),
        dry_run: false,
    };
    let dir = bundle_path(plan);
    let image = native::check_bundle(
        &common,
        &plan.agent,
        plan.bundle.as_deref(),
        plan.runner_image.as_deref(),
    )
    .await?;
    let jira_token = read_token(&plan.site.flags.token_file)?;
    let token = read_token(&plan.token_file)?;
    let mut manifests = vec![
        (
            jira_cloud::secret_manifest(&plan.namespace, &jira_token),
            format!("Secret {}", jira_cloud::SECRET_NAME),
        ),
        (
            token_secret_manifest(&plan.namespace, CODE_HOST_SECRET, &token),
            format!("Secret {CODE_HOST_SECRET}"),
        ),
    ];
    if let Some(ca) = &plan.ca_file {
        let pem = std::fs::read_to_string(ca).map_err(|error| {
            CliError::failure(format!(
                "could not read --ca-file {}: {error}",
                ca.display()
            ))
        })?;
        manifests.push((
            ca_manifest(&plan.namespace, &pem),
            format!("ConfigMap {CA_CONFIG_MAP}"),
        ));
    }
    apply_forge_entries(
        &common,
        &opts.chart,
        &format!(
            "the Bitbucket code host, the Jira tracker and binding `{}`",
            jira_cloud::BINDING_NAME
        ),
        &manifests,
        |recorded| native_forge_values(recorded, &plan.entries),
    )
    .await?;
    let chart = opts.chart.clone();
    crate::cluster_secrets::bind_forge_egress(common.clone(), plan.agent.clone(), async move {
        Ok(chart)
    })
    .await?;
    let credit_remaining_usd = check_credit(release_has_real_model).await;
    ensure_factory_agent(
        &common,
        &plan.agent,
        crate::api::add_channel_body(
            "jira",
            jira_cloud::BINDING_NAME,
            None,
            None,
            plan.label.as_deref(),
        ),
        None,
    )
    .await?;
    for (index, args) in deploy_argvs(plan, &dir).into_iter().enumerate() {
        let step = match (index, plan.agent == DEFAULT_AGENT) {
            (0, true) => "Deploying dark factory".to_string(),
            (0, false) => format!("Deploying {}", plan.agent),
            _ => "Binding the Jira tracker".to_string(),
        };
        run_self(&args, &step).await?;
    }
    for (args, step) in settings(plan) {
        run_self(&args, step).await?;
    }
    Ok(QuickstartOutput::AgentAdded {
        context: install.install.context.clone(),
        agent: plan.agent.clone(),
        repos: vec![plan.code_host.path.clone()],
        trigger_label: plan.label.clone(),
        effective_label: plan.label.clone().unwrap_or_else(|| {
            plan.entries.tracker["label"]
                .as_str()
                .unwrap_or_default()
                .to_string()
        }),
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
    fn one_repo_needs_exactly_one() {
        assert_eq!(one_repo(&["FAC/demo".into()]).unwrap(), "FAC/demo");
        for repos in [vec![], vec!["a/b".to_string(), "a/c".into()]] {
            let text = format!("{:#}", one_repo(&repos).unwrap_err());
            assert!(text.contains("--repo once"), "{repos:?}: {text}");
        }
    }
}

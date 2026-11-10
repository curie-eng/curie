//! Jira Cloud as the tracker with a Bitbucket Cloud or Data Center code host
//! in `curie factory quickstart` (#4400, #4401).
//!
//! Bitbucket has no issue tracker, so the ticket is a Jira issue and the pull
//! request, its build statuses and its review comments are on Bitbucket. The
//! run reads the Jira site and the Bitbucket repository, also under
//! `--dry-run`, then installs, writes the Bitbucket code host, the Jira
//! tracker and one Jira binding naming the repository, the two token Secrets
//! and, with `--ca-file`, the CA bundle, and deploys the agent bound through
//! `jira=<binding>`.
//!
//! The code host and the Jira site are each found on the release by identity
//! (#4620), whatever their names, and kept as recorded; a new one gets
//! derived names and its own Secret. A differing recorded Jira tracker or
//! binding, or a token that differs from a recorded Secret, refuses before
//! any write, as on the GitHub routes.
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

use super::instance::{self, Credential, ObjectWrite, Observation};
use super::jira_cloud::{self, preflight_site, JiraPlan, JiraSite};
use super::native::{self, NativeInstall};
use super::{
    agent_settings, bitbucket_cloud, bitbucket_dc, bundle_dir, bundle_line, check_credit,
    check_prerequisites, cluster_agent, confirm_remote_context, credential_line,
    deployed_release_snapshot, ensure_factory_agent, forge_egress_line, initial_agent_line,
    install_plan_lines, keep_surface_plan_line, kept_surface_line, read_token, resolve_host,
    run_self, surface_to_keep, unconfirmed_remote_context, validate_agent_flags, Action,
    CodeHostKind, EgressEntry, ForgeFlags, QuickstartOpts, QuickstartOutput, DEFAULT_AGENT,
};
use crate::exit::CliError;
use crate::factory_intake::{
    apply_forge_entries, native_forge_values, refuse_planned_factory, NativeForgeEntries,
};
use crate::ops::CommonOpts;
use crate::ui::DryRunPlan;

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
    /// The resolved entries: a recorded one as recorded.
    entries: NativeForgeEntries,
    /// The Jira tracker and binding, resolved and checked.
    jira: JiraPlan,
    /// The code host is the release's own, kept as recorded.
    code_host_recorded: bool,
    /// The Bitbucket token Secrets to create: name and key.
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
    /// The label that starts this agent's work.
    effective_label: String,
    bundle: Option<PathBuf>,
    runner_image: Option<String>,
    deploy_chart: Option<String>,
    deadline_seconds: u32,
    budget_usd: f64,
    output_tokens: u64,
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
/// entry as a new instance, under the name derived from its API base URL
/// and the release's Secret for it. Reads only.
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
            let name = super::forge_entry_name("bitbucket_cloud", CLOUD_API);
            let found = vec![
                format!(
                    "Bitbucket Cloud repository {} has id {}",
                    probed.path, probed.project_id
                ),
                bitbucket_cloud::expiry_line(&name),
            ];
            let values = bitbucket_cloud::values(
                &name,
                &super::forge_secret_name(&opts.release, &name),
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
            let name = super::forge_entry_name("bitbucket_dc", base);
            let found = vec![
                format!(
                    "Bitbucket Data Center {} at {base}: 9.4.0 or later",
                    probed.version
                ),
                format!(
                    "Bitbucket Data Center repository {} has id {}",
                    probed.path, probed.project_id
                ),
                bitbucket_dc::expiry_line(&name),
            ];
            let secret = super::forge_secret_name(&opts.release, &name);
            let values =
                bitbucket_dc::values(&name, &secret, &probed, base, &host, reviewers, &egress);
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

/// The Jira binding of `tracker` naming the one Bitbucket repository on the
/// code host `code_host_name`, with the review allowlist under that name.
fn jira_bitbucket_binding(
    site: &JiraSite,
    tracker: &str,
    code_host_name: &str,
    code_host: &CodeHost,
    reviewers: &[String],
) -> Value {
    let mut binding = jira_cloud::binding_value(
        tracker,
        &site.cloud_id,
        code_host_name,
        &[(code_host.path.clone(), code_host.project_id.clone())],
        &site.flags.start,
    );
    binding["reviewFeedbackAllowlist"] = json!({ code_host_name: reviewers });
    binding
}

fn forge_entry_label(entry: &Value) -> &str {
    entry["name"].as_str().unwrap_or_default()
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
    let mut surface = vec!["--add".into(), format!("jira={}", plan.jira.binding_name)];
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
        plan.output_tokens,
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
        credential_line(&plan.native.credential, plan.native.install.cluster_up),
        format!(
            "Jira site {} has cloud id {}; the factory account is {} ({})",
            site.host, site.cloud_id, flags.username, site.mention
        ),
    ];
    lines.extend(plan.code_host.found.iter().cloned());
    lines.extend(install_plan_lines(
        plan.native.install.cluster_up,
        &plan.native.install.actions,
        &plan.release,
        &plan.namespace,
    ));
    let secret_line = |recorded: bool,
                       secret: &str,
                       key: &str,
                       entry: &str,
                       flag: &str,
                       file: &Path| {
        if recorded {
            format!(
                "reuse Secret {secret} key {key}, the credential factory entry {entry} records: it must hold the {flag} token and is never rewritten"
            )
        } else {
            format!(
                "write Secret {secret} key {key} in namespace {} from {flag} {} (the token is never printed), unless this release's quickstart already left it holding that token",
                plan.namespace,
                file.display()
            )
        }
    };
    let code_host_name = forge_entry_label(&plan.entries.code_host);
    let tracker_name = forge_entry_label(&plan.entries.tracker);
    lines.push(secret_line(
        plan.jira.recorded_tracker.is_some(),
        &plan.jira.secret_name,
        &plan.jira.secret_key,
        tracker_name,
        flags.token_flag,
        &flags.token_file,
    ));
    let (code_host_secret, code_host_key) = instance::credential_ref(&plan.entries.code_host);
    lines.push(secret_line(
        plan.code_host_recorded,
        &code_host_secret,
        &code_host_key,
        code_host_name,
        "--token-file",
        &plan.token_file,
    ));
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
    lines.push(format!(
        "write factory.enabled=true, factory.intake=poll, factory.codeHosts entry {code_host_name} ({}, repository {} id {}), factory.trackers entry {tracker_name} and factory.bindings entry {} (default repository {}, reviews counted from {}): helm upgrade --reuse-values of release {}, then roll the API",
        plan.entries.code_host["kind"].as_str().unwrap_or_default(),
        plan.code_host.path,
        plan.code_host.project_id,
        plan.jira.binding_name,
        plan.entries.binding["defaultRepo"].as_str().unwrap_or_default(),
        plan.entries.binding["reviewFeedbackAllowlist"][code_host_name]
            .as_array()
            .map(|ids| ids.iter().filter_map(Value::as_str).collect::<Vec<_>>().join(", "))
            .unwrap_or_default(),
        plan.release
    ));
    lines.push(format!(
        "forge egress for {code_host_name}: {}; for {tracker_name}: {}",
        egress_prose(&plan.code_host.egress),
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
            &plan.jira.binding_name,
            None,
            None,
            plan.label.as_deref(),
        ),
    ));
    for args in deploy_argvs(plan, &bundle_path(plan)) {
        lines.push(format!("curie {}", args.join(" ")));
    }
    if plan.label.is_none() {
        lines.push(keep_surface_plan_line("jira", &plan.jira.binding_name));
    }
    for (args, _) in settings(plan) {
        lines.push(format!("curie {}", args.join(" ")));
    }
    lines
}

/// The release a run checks before its first write.
struct ReleaseView<'a> {
    common: &'a CommonOpts,
    /// The serving release's user values; null when none serves or it was
    /// not read.
    values: &'a Value,
    /// The serving Helm revision; `None` when none serves or it was not read.
    revision: Option<u32>,
    /// Read the namespace's Secrets and CA bundle: false under `--dry-run`
    /// and on a cluster this run creates, which holds nothing yet.
    reads: bool,
    dry_run: bool,
}

/// What [`check_release`] resolved and read.
struct ReleaseCheck {
    /// The resolved entries: a recorded one as recorded.
    entries: NativeForgeEntries,
    jira: JiraPlan,
    code_host_recorded: bool,
    secret_creates: Vec<(String, String)>,
    trust_target: Option<(String, String, bool)>,
    trust: Option<ObjectWrite>,
    observation: Observation,
}

/// Resolves the Bitbucket code host and the Jira site against the release by
/// identity (#4620), checks a recorded Jira tracker and binding, checks the
/// document the forge entries' upgrade would write, and then reads and
/// checks the credential Secrets and the CA bundle. Every identity,
/// credential, ownership and binding refusal is here, before the install
/// and before any write; it writes nothing itself. What it read, with the
/// serving Helm revision, is the run's observation.
async fn check_release(
    view: &ReleaseView<'_>,
    site: &JiraSite,
    code_host: &CodeHost,
    forge: &ForgeFlags,
    token_file: &Path,
    ca_certificates: Option<&[Vec<u8>]>,
) -> Result<ReleaseCheck> {
    let common = view.common;
    let release = &common.release;
    let values = view.values;
    let resolved = instance::resolve_instance(
        values,
        "codeHosts",
        code_host.entry.clone(),
        "--forge-url",
        release,
    )?;
    let code_host_recorded = resolved.recorded().is_some();
    let code_host_entry = resolved.into_entry();
    let tracker = site.tracker(release);
    let planned_binding = jira_bitbucket_binding(
        site,
        forge_entry_label(&tracker),
        forge_entry_label(&code_host_entry),
        code_host,
        &forge.review_accounts,
    );
    let jira = jira_cloud::new_jira_plan(
        tracker,
        planned_binding,
        site.egress.clone(),
        site.flags.token_file.clone(),
    );
    let mut jira = jira_cloud::resolve_jira_site(jira, values, site.flags.url_flag, release)?;
    jira_cloud::check_recorded(&jira, release)?;
    let trust_target = forge
        .ca_file
        .as_ref()
        .map(|_| match instance::recorded_trust_ref(values) {
            Some((name, key)) => (name, key, false),
            None => (
                instance::code_host_trust_name(release),
                instance::TRUST_KEY.to_string(),
                true,
            ),
        });
    let entries = NativeForgeEntries {
        code_host: code_host_entry,
        tracker: jira
            .recorded_tracker
            .clone()
            .unwrap_or(jira.tracker.clone()),
        binding: jira
            .recorded_binding
            .clone()
            .unwrap_or(jira.binding.clone()),
        ca_config_map: trust_target
            .as_ref()
            .filter(|(_, _, set_ref)| *set_ref)
            .map(|(name, _, _)| name.clone()),
    };
    // The document the upgrade writes, checked now rather than after the
    // install.
    let document = native_forge_values(values, &entries)?;
    refuse_planned_factory(values, &document)?;

    // The Secrets and the CA bundle, read and checked before any write.
    let (code_host_secret, code_host_key) = instance::credential_ref(&entries.code_host);
    let mut secret_creates = Vec::new();
    let mut trust = None;
    let mut observation = Observation::new(common, view.revision);
    if view.reads {
        jira = jira_cloud::verify_secret(jira, common, site.flags.token_flag, observation).await?;
        observation = jira.observation.clone();
        let token = read_token(token_file)?;
        let credential = Credential {
            entry: forge_entry_label(&entries.code_host),
            secret: code_host_secret,
            key: code_host_key,
            recorded: code_host_recorded,
        };
        secret_creates = instance::check_credentials(
            common,
            &[credential],
            &token,
            "--token-file",
            &mut observation,
        )
        .await?;
        if let Some(certificates) = ca_certificates {
            let (planned, read) = instance::plan_trust(common, values, certificates).await?;
            observation.push(read);
            trust = planned;
        }
    } else if !view.dry_run {
        // A cluster this run creates holds nothing yet.
        if !code_host_recorded {
            secret_creates.push((code_host_secret, code_host_key));
        }
        if let Some(certificates) = ca_certificates {
            trust = Some(instance::new_trust(common, certificates));
        }
    }
    Ok(ReleaseCheck {
        entries,
        jira,
        code_host_recorded,
        secret_creates,
        trust_target,
        trust,
        observation,
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
    // A malformed CA bundle refuses before any network read.
    let ca_certificates = match &opts.forge.ca_file {
        Some(ca) => Some(instance::read_ca_file(ca)?),
        None => None,
    };
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

    // Confirm before any write. A non-terminal refusal here mutates nothing.
    let real_interactive = std::io::stdin().is_terminal();
    if !opts.dry_run {
        if let Some(name) = unconfirmed_remote_context(opts.context.as_deref(), current.as_deref())
        {
            confirm_remote_context(name, real_interactive)?;
        }
    }
    let native = native::plan_install(&opts, current.as_deref(), real_interactive).await?;
    let creates_kind = native.install.actions.iter().any(|action| {
        matches!(action, Action::External { program, args }
            if program == "kind" && args.first().is_some_and(|arg| arg == "create"))
    });
    let common = CommonOpts {
        namespace: opts.namespace.clone(),
        release: opts.release.clone(),
        dry_run: false,
    };
    let reads = !opts.dry_run && !creates_kind;
    let snapshot = if reads {
        crate::kube_context::pin_for_cluster_command(Some(&native.install.context))?;
        deployed_release_snapshot(&common).await?
    } else {
        None
    };
    let values = snapshot
        .as_ref()
        .map_or(Value::Null, |snapshot| snapshot.values.clone());
    let checked = check_release(
        &ReleaseView {
            common: &common,
            values: &values,
            revision: snapshot.as_ref().map(|snapshot| snapshot.revision),
            reads,
            dry_run: opts.dry_run,
        },
        &site,
        &code_host,
        &opts.forge,
        &token_file,
        ca_certificates.as_deref(),
    )
    .await?;
    let effective_label = opts.label.clone().unwrap_or_else(|| {
        checked.entries.tracker["label"]
            .as_str()
            .unwrap_or(crate::factory_app::DEFAULT_LABEL)
            .to_string()
    });
    let ReleaseCheck {
        entries,
        jira,
        code_host_recorded,
        secret_creates,
        trust_target,
        trust,
        observation,
    } = checked;

    let plan = Plan {
        native,
        site,
        code_host,
        entries,
        jira,
        code_host_recorded,
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
        return Ok(QuickstartOutput::DryRun(DryRunPlan {
            lines: describe(&plan),
        }));
    }
    native::refuse_without_credential(&plan.native.credential)?;
    execute_jira_bitbucket(plan, &opts).await
}

async fn execute_jira_bitbucket(mut plan: Plan, opts: &QuickstartOpts) -> Result<QuickstartOutput> {
    let install = &plan.native;
    let release_has_real_model = native::run_install(
        &install.install,
        &install.credential,
        install.release_has_real_model,
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
    let mut writes: Vec<ObjectWrite> = jira_cloud::secret_write(&plan.jira, &common)?
        .into_iter()
        .collect();
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
    let plan = &plan;
    apply_forge_entries(
        &common,
        &opts.chart,
        &format!(
            "the Bitbucket code host, the Jira tracker and binding `{}`",
            plan.jira.binding_name
        ),
        &plan.observation,
        &writes,
        |recorded| native_forge_values(recorded, &plan.entries),
    )
    .await?;
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
            "jira",
            &plan.jira.binding_name,
            None,
            None,
            plan.label.as_deref(),
        ),
        None,
    )
    .await?;
    // A label-less rerun keeps the surface the agent already holds (#4620).
    let kept = surface_to_keep(
        &held,
        "jira",
        &plan.jira.binding_name,
        plan.label.as_deref(),
    );
    for (index, args) in deploy_argvs(plan, &dir).into_iter().enumerate() {
        if index > 0 && kept.is_some() {
            crate::ui::ui().note(&kept_surface_line(
                &plan.agent,
                "jira",
                &plan.jira.binding_name,
            ));
            continue;
        }
        let step = match (index, plan.agent == DEFAULT_AGENT) {
            (0, true) => "Deploying dark factory".to_string(),
            (0, false) => format!("Deploying {}", plan.agent),
            _ => "Binding the Jira tracker".to_string(),
        };
        run_self(&args, &step, None, None).await?;
    }
    for (args, step) in settings(plan) {
        run_self(&args, step, None, None).await?;
    }
    Ok(QuickstartOutput::AgentAdded {
        context: plan.native.install.context.clone(),
        agent: plan.agent.clone(),
        repos: vec![plan.code_host.path.clone()],
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
    use std::os::unix::fs::PermissionsExt;

    use base64::Engine;

    use super::*;
    use crate::factory_quickstart::{
        forge_entry_name, forge_secret_name, SecretUse, TrackerKind, RELEASE_LABEL,
    };

    const RELEASE: &str = "acme";
    const NAMESPACE: &str = "acme-ns";
    const BITBUCKET_TOKEN: &str = "bitbucket-fixture-token-4620-a1";
    const OTHER_TOKEN: &str = "bitbucket-fixture-token-4620-b2";
    const JIRA_TOKEN: &str = "jira-fixture-token-4620-c3";

    /// A `kubectl` on PATH that answers `get <kind> <name>` from
    /// `<kind>-<name>.json` in its directory (nothing when absent), logs every
    /// call, and fails anything else, so a write would both fail and show.
    struct Kubectl {
        dir: tempfile::TempDir,
        path: Option<std::ffi::OsString>,
    }

    impl Kubectl {
        fn install() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let shim = dir.path().join("kubectl");
            std::fs::write(
                &shim,
                format!(
                    "#!/bin/sh\necho \"kubectl $*\" >> \"{dir}/kubectl.log\"\ncase \" $* \" in *\" get \"*) f=\"{dir}/$4-$5.json\"; [ -f \"$f\" ] && cat \"$f\"; exit 0 ;; esac\nexit 1\n",
                    dir = dir.path().display()
                ),
            )
            .unwrap();
            std::fs::set_permissions(&shim, std::fs::Permissions::from_mode(0o755)).unwrap();
            let path = std::env::var_os("PATH");
            let mut entries = vec![dir.path().to_path_buf()];
            entries.extend(std::env::split_paths(&path.clone().unwrap_or_default()));
            std::env::set_var("PATH", std::env::join_paths(entries).unwrap());
            Self { dir, path }
        }

        /// Serves Secret `name` holding `token` under `key`, with `metadata`
        /// merged into its own.
        fn secret(&self, name: &str, key: &str, token: &str, metadata: Value) {
            let mut object = json!({
                "apiVersion": "v1", "kind": "Secret",
                "metadata": {"name": name, "namespace": NAMESPACE, "resourceVersion": "7"},
                "data": {key: base64::engine::general_purpose::STANDARD.encode(token)},
            });
            if let (Some(into), Value::Object(extra)) =
                (object["metadata"].as_object_mut(), metadata)
            {
                into.extend(extra);
            }
            std::fs::write(
                self.dir.path().join(format!("secret-{name}.json")),
                object.to_string(),
            )
            .unwrap();
        }

        fn log(&self) -> String {
            std::fs::read_to_string(self.dir.path().join("kubectl.log")).unwrap_or_default()
        }

        /// Every logged call that is not a read.
        fn writes(&self) -> Vec<String> {
            self.log()
                .lines()
                .filter(|line| !line.split_whitespace().any(|word| word == "get"))
                .map(str::to_string)
                .collect()
        }

        fn file(&self, name: &str, contents: &str) -> PathBuf {
            let path = self.dir.path().join(name);
            std::fs::write(&path, format!("{contents}\n")).unwrap();
            path
        }
    }

    impl Drop for Kubectl {
        fn drop(&mut self) {
            match &self.path {
                Some(path) => std::env::set_var("PATH", path),
                None => std::env::remove_var("PATH"),
            }
        }
    }

    fn bitbucket_fixture_common() -> CommonOpts {
        CommonOpts {
            namespace: NAMESPACE.into(),
            release: RELEASE.into(),
            dry_run: false,
        }
    }

    /// The flags of a Bitbucket `kind` run beside Jira, with the token files.
    fn bitbucket_forge_flags(
        kind: CodeHostKind,
        bitbucket_token: &Path,
        jira_token: &Path,
    ) -> ForgeFlags {
        ForgeFlags {
            code_host: kind,
            tracker: TrackerKind::JiraCloud,
            forge_url: (kind == CodeHostKind::BitbucketDc)
                .then(|| "https://bitbucket.example.test".to_string()),
            token_file: Some(bitbucket_token.to_path_buf()),
            review_accounts: vec!["17".into()],
            forge_username: Some("factory-bot@example.com".into()),
            start_accounts: vec!["557058:starter".into()],
            tracker_url: Some("https://acme.atlassian.net".into()),
            tracker_token_file: Some(jira_token.to_path_buf()),
            ..ForgeFlags::default()
        }
    }

    fn jira_site_fixture(forge: &ForgeFlags) -> JiraSite {
        JiraSite {
            flags: jira_cloud::flags(forge).unwrap(),
            host: "acme.atlassian.net".into(),
            cloud_id: "00000000-0000-4000-8000-0000000000c1".into(),
            mention: "712020:factory-bot".into(),
            egress: Vec::new(),
        }
    }

    /// The code host `probe_code_host` builds for `kind`, as a new instance.
    fn probed_code_host(kind: CodeHostKind) -> CodeHost {
        let reviewers = ["17".to_string()];
        let values = match kind {
            CodeHostKind::BitbucketCloud => {
                let name = forge_entry_name("bitbucket_cloud", CLOUD_API);
                bitbucket_cloud::values(
                    &name,
                    &forge_secret_name(RELEASE, &name),
                    &bitbucket_cloud::Probed {
                        project_id: "{00000000-0000-4000-8000-0000000000b1}".into(),
                        path: "acme/widgets".into(),
                    },
                    CLOUD_API,
                    CLOUD_HOST,
                    &reviewers,
                    &[],
                )
            }
            _ => {
                let base = "https://bitbucket.example.test";
                let name = forge_entry_name("bitbucket_dc", base);
                bitbucket_dc::values(
                    &name,
                    &forge_secret_name(RELEASE, &name),
                    &bitbucket_dc::Probed {
                        project_id: "88".into(),
                        path: "ACME/widgets".into(),
                        version: "9.4.0".into(),
                    },
                    base,
                    "bitbucket.example.test",
                    &reviewers,
                    &[],
                )
            }
        };
        let repo = &values["repo"];
        CodeHost {
            entry: values["codeHosts"][0].clone(),
            path: repo["path"].as_str().unwrap().to_string(),
            project_id: repo["projectId"].as_str().unwrap().to_string(),
            found: Vec::new(),
            egress: Vec::new(),
        }
    }

    async fn check_bitbucket_release_fixture(
        values: &Value,
        reads: bool,
        forge: &ForgeFlags,
        code_host: &CodeHost,
    ) -> Result<ReleaseCheck> {
        let common = bitbucket_fixture_common();
        check_release(
            &ReleaseView {
                common: &common,
                values,
                revision: Some(3),
                reads,
                dry_run: false,
            },
            &jira_site_fixture(forge),
            code_host,
            forge,
            forge.token_file.as_deref().unwrap(),
            None,
        )
        .await
    }

    /// The release values of an earlier run, recorded under operator names:
    /// the code host, the Jira site and the binding, each credential in an
    /// operator Secret with no owner markers.
    async fn operator_release(forge: &ForgeFlags, code_host: &CodeHost) -> Value {
        let mut host = code_host.entry.clone();
        host["name"] = json!("operator-code-host");
        instance::set_credential_ref(&mut host, "operator-forge-credential", "operator-key");
        let fresh = check_bitbucket_release_fixture(&Value::Null, false, forge, code_host)
            .await
            .unwrap();
        let mut tracker = fresh.entries.tracker;
        tracker["name"] = json!("operator-jira-site");
        instance::set_credential_ref(&mut tracker, "curie-jira-token", "operator-jira-key");
        let named = json!({"factory": {"codeHosts": [host], "trackers": [tracker]}});
        let planned = check_bitbucket_release_fixture(&named, false, forge, code_host)
            .await
            .unwrap();
        let mut binding = planned.entries.binding;
        binding["name"] = json!("operator-jira-binding");
        let mut values = named;
        values["factory"]["bindings"] = json!([binding]);
        values
    }

    fn bitbucket_refusal_text(result: Result<ReleaseCheck>) -> String {
        match result {
            Ok(_) => panic!("the run was not refused"),
            Err(error) => format!("{error:#}"),
        }
    }

    #[tokio::test]
    async fn bitbucket_recorded_resources_are_reused_and_conflicts_refuse_before_any_write() {
        let _env = crate::PROCESS_ENV_LOCK.lock().await;
        for kind in [CodeHostKind::BitbucketCloud, CodeHostKind::BitbucketDc] {
            let kubectl = Kubectl::install();
            let bitbucket_token = kubectl.file("bitbucket-token", BITBUCKET_TOKEN);
            let other_token = kubectl.file("other-token", OTHER_TOKEN);
            let jira_token = kubectl.file("jira-token", JIRA_TOKEN);
            let forge = bitbucket_forge_flags(kind, &bitbucket_token, &jira_token);
            let code_host = probed_code_host(kind);
            let values = operator_release(&forge, &code_host).await;
            kubectl.secret(
                "operator-forge-credential",
                "operator-key",
                BITBUCKET_TOKEN,
                json!({}),
            );
            kubectl.secret(
                "curie-jira-token",
                "operator-jira-key",
                JIRA_TOKEN,
                json!({}),
            );

            // Liveness: an equivalent rerun reuses every recorded resource.
            let checked = check_bitbucket_release_fixture(&values, true, &forge, &code_host)
                .await
                .unwrap();
            assert!(checked.code_host_recorded, "{kind:?}");
            assert!(checked.secret_creates.is_empty(), "{kind:?}");
            assert_eq!(checked.jira.secret, SecretUse::Reuse, "{kind:?}");
            assert_eq!(checked.entries.code_host, values["factory"]["codeHosts"][0]);
            assert_eq!(checked.entries.tracker, values["factory"]["trackers"][0]);
            assert_eq!(checked.entries.binding, values["factory"]["bindings"][0]);
            assert_eq!(checked.jira.binding_name, "operator-jira-binding");

            // A conflicting code host credential refuses.
            let conflicting = bitbucket_forge_flags(kind, &other_token, &jira_token);
            let text = bitbucket_refusal_text(
                check_bitbucket_release_fixture(&values, true, &conflicting, &code_host).await,
            );
            assert!(
                text.contains("operator-forge-credential") && text.contains("different token"),
                "{kind:?}: {text}"
            );
            assert!(!text.contains(OTHER_TOKEN) && !text.contains(BITBUCKET_TOKEN));

            // A recorded Secret another Helm release owns refuses.
            kubectl.secret(
                "operator-forge-credential",
                "operator-key",
                BITBUCKET_TOKEN,
                json!({"annotations": {"meta.helm.sh/release-name": "acme-other"}}),
            );
            let text = bitbucket_refusal_text(
                check_bitbucket_release_fixture(&values, true, &forge, &code_host).await,
            );
            assert!(
                text.contains("Secret operator-forge-credential")
                    && text.contains("owned by Helm release acme-other"),
                "{kind:?}: {text}"
            );
            kubectl.secret(
                "operator-forge-credential",
                "operator-key",
                BITBUCKET_TOKEN,
                json!({}),
            );

            // So does the recorded Jira Secret labelled for another release.
            kubectl.secret(
                "curie-jira-token",
                "operator-jira-key",
                JIRA_TOKEN,
                json!({"labels": {RELEASE_LABEL: "acme-other"}}),
            );
            let text = bitbucket_refusal_text(
                check_bitbucket_release_fixture(&values, true, &forge, &code_host).await,
            );
            assert!(
                text.contains("Secret curie-jira-token") && text.contains("acme-other"),
                "{kind:?}: {text}"
            );

            // A new instance does not adopt a foreign Secret at its derived name.
            let (derived, _) = instance::credential_ref(&code_host.entry);
            kubectl.secret(
                &derived,
                "token",
                BITBUCKET_TOKEN,
                json!({"annotations": {"meta.helm.sh/release-name": "acme-other"}}),
            );
            let text = bitbucket_refusal_text(
                check_bitbucket_release_fixture(&Value::Null, true, &forge, &code_host).await,
            );
            assert!(
                text.contains(&format!("Secret {derived}"))
                    && text.contains("Helm release acme-other"),
                "{kind:?}: {text}"
            );

            assert_eq!(kubectl.writes(), Vec::<String>::new(), "{kind:?}");
            assert!(
                kubectl.log().contains(" get secret "),
                "the checks read the cluster"
            );
        }
    }

    #[test]
    fn one_repo_needs_exactly_one() {
        assert_eq!(one_repo(&["FAC/demo".into()]).unwrap(), "FAC/demo");
        for repos in [vec![], vec!["a/b".to_string(), "a/c".into()]] {
            let text = format!("{:#}", one_repo(&repos).unwrap_err());
            assert!(text.contains("--repo once"), "{repos:?}: {text}");
        }
    }
}

//! `curie cluster factory`: turn on the GitHub factory intake on an existing
//! release (#3619), the sibling of `cluster github-app`.
//!
//! Every value rides a private 0600 values file passed with `-f` and removed
//! after the run, so the webhook secret never reaches argv. There is no
//! `--webhook-secret <value>` flag: the secret comes from a file or the
//! `CURIE_GITHUB_WEBHOOK_SECRET` environment variable only.

use std::path::Path;
use std::time::Duration;

use anyhow::Result;

use crate::exit::CliError;
use crate::ops::{
    fetch_release_values, plain, require_on_path, run_capture, CommonOpts, OpsCommand,
};

/// Environment variable carrying the webhook secret value.
pub const WEBHOOK_SECRET_ENV: &str = "CURIE_GITHUB_WEBHOOK_SECRET";

#[derive(Debug, Clone)]
pub struct FactoryIntakeOpts {
    pub common: CommonOpts,
    pub chart: String,
    /// `owner/repo` entries for `api.githubRepoAllowlist`; empty leaves it alone.
    pub repos: Vec<String>,
    pub label: Option<String>,
    pub mention: Option<String>,
    pub card_base_url: Option<String>,
    /// The resolved webhook secret value, never a path and never from argv.
    pub webhook_secret: Option<String>,
    /// Agents that get GitHub API egress (`agentSandbox.connectorEgress.<agent>`).
    pub github_api_egress: Vec<String>,
    /// Set `api.githubFactoryIngressEnabled=false` and nothing else.
    pub disable: bool,
    /// Explicit helm `--timeout` seconds; `None` derives it from the release.
    pub timeout_seconds: Option<u64>,
    /// The factory GitHub App id; paired with `private_key_file` (#3746).
    pub app_id: Option<String>,
    /// File holding the App's PEM private key. Its contents never enter argv.
    pub private_key_file: Option<std::path::PathBuf>,
    /// Organization whose registration form the printed link opens.
    pub org: Option<String>,
    /// `api.githubFactoryIntake` when this run must select a mode. `None`
    /// leaves the recorded value unchanged.
    pub intake: Option<String>,
    /// Digest-pinned runner image bound in the same upgrade as the intake
    /// settings. `None` leaves `agentSandbox.runnerImages` unchanged.
    pub runner_binding: Option<RunnerImageBinding>,
}

/// A runner image to set at `agentSandbox.runnerImages.<agent>` (#3934).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunnerImageBinding {
    pub agent: String,
    pub image: String,
}

/// Helm's floor for this command, matching `curie cluster upgrade`'s default.
pub const HELM_TIMEOUT_FLOOR_SECS: u64 = 15 * 60;

/// The annotation the chart stamps on its pre-upgrade worker drain Job with
/// the Helm timeout that Job needs (charts/curie/templates/_helpers.tpl,
/// `curie.worker.minimumHelmTimeoutSeconds`).
const MINIMUM_HELM_TIMEOUT_ANNOTATION: &str = "curie.ai/minimum-helm-timeout-seconds";

/// The chart's own minimum Helm timeout read from rendered hook manifests
/// (`helm get hooks`), so the arithmetic stays in the chart. `None` when no
/// drain Job carries a usable annotation (drain disabled, older chart).
pub fn minimum_helm_timeout_from_hooks(rendered: &str) -> Option<u64> {
    use serde::Deserialize;
    let mut found = None;
    for document in serde_norway::Deserializer::from_str(rendered) {
        let Ok(value) = serde_json::Value::deserialize(document) else {
            continue;
        };
        let seconds = value
            .pointer("/metadata/annotations")
            .and_then(|a| a.get(MINIMUM_HELM_TIMEOUT_ANNOTATION))
            .and_then(serde_json::Value::as_str)
            .and_then(|raw| raw.parse::<u64>().ok())
            .filter(|n| *n > 0);
        if let Some(seconds) = seconds {
            found = Some(found.map_or(seconds, |f: u64| f.max(seconds)));
        }
    }
    found
}

/// The effective helm timeout: an explicit value wins, else the release's
/// drain contract, never below [`HELM_TIMEOUT_FLOOR_SECS`].
pub fn effective_helm_timeout(explicit: Option<u64>, chart_minimum: Option<u64>) -> u64 {
    explicit.unwrap_or_else(|| {
        chart_minimum
            .unwrap_or(HELM_TIMEOUT_FLOOR_SECS)
            .max(HELM_TIMEOUT_FLOOR_SECS)
    })
}

async fn release_minimum_helm_timeout(common: &CommonOpts) -> Option<u64> {
    let cmd = OpsCommand::new(
        "helm",
        vec![
            plain("get"),
            plain("hooks"),
            plain(&common.release),
            plain("-n"),
            plain(&common.namespace),
        ],
    );
    let (ok, out, _) = run_capture(&cmd).await.ok()?;
    if !ok {
        return None;
    }
    minimum_helm_timeout_from_hooks(&out)
}

pub enum FactoryIntakeOutput {
    DryRun(crate::ui::DryRunPlan),
    Done { enabled: bool },
}

impl crate::ui::CliOutput for FactoryIntakeOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            FactoryIntakeOutput::DryRun(plan) => plan.to_json(),
            FactoryIntakeOutput::Done { enabled } => {
                serde_json::json!({"factory_intake_enabled": enabled})
            }
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            FactoryIntakeOutput::DryRun(plan) => plan.render(ui),
            FactoryIntakeOutput::Done { enabled } => ui.payload(if *enabled {
                "GitHub factory intake enabled"
            } else {
                "GitHub factory intake disabled"
            }),
        }
    }
}

/// Resolve the webhook secret from `--webhook-secret-file` or
/// `CURIE_GITHUB_WEBHOOK_SECRET`. Both set, an unreadable file, or an empty
/// value is a usage error. One trailing newline is trimmed.
pub fn resolve_webhook_secret(file: Option<&Path>) -> Result<Option<String>> {
    let env = std::env::var(WEBHOOK_SECRET_ENV)
        .ok()
        .filter(|value| !value.is_empty());
    let raw = match (file, env) {
        (Some(path), Some(_)) => {
            return Err(CliError::usage(format!(
                "both --webhook-secret-file {} and {WEBHOOK_SECRET_ENV} are set",
                path.display()
            ))
            .with_fix(format!(
                "unset {WEBHOOK_SECRET_ENV} or drop --webhook-secret-file"
            ))
            .into())
        }
        (Some(path), None) => std::fs::read_to_string(path).map_err(|error| {
            CliError::usage(format!(
                "cannot read --webhook-secret-file {}: {error}",
                path.display()
            ))
            .with_fix("pass a readable file holding the GitHub webhook secret")
        })?,
        (None, Some(value)) => value,
        (None, None) => return Ok(None),
    };
    let value = raw
        .strip_suffix("\r\n")
        .or_else(|| raw.strip_suffix('\n'))
        .unwrap_or(&raw)
        .to_string();
    if value.trim().is_empty() {
        return Err(CliError::usage("the GitHub webhook secret is empty")
            .with_fix("put the webhook secret configured on the GitHub webhook in the file or env")
            .into());
    }
    Ok(Some(value))
}

/// The `api` CIDR list of a GitHub `/meta` document, order preserved.
/// Missing or empty is an error.
pub fn github_api_cidrs(meta_json: &str) -> Result<Vec<String>> {
    let meta: serde_json::Value = serde_json::from_str(meta_json)
        .map_err(|error| CliError::failure(format!("GitHub /meta is not JSON: {error}")))?;
    let cidrs: Vec<String> = meta
        .get("api")
        .and_then(serde_json::Value::as_array)
        .map(|list| {
            list.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    if cidrs.is_empty() {
        return Err(CliError::failure("GitHub /meta has no `api` CIDR list").into());
    }
    Ok(cidrs)
}

/// Keep only IPv4 prefixes. The chart's connectorEgress NetworkPolicy
/// (charts/curie/templates/security-networkpolicy.yaml) refuses IPv6 prefixes
/// broader than /32, and GitHub publishes IPv6 api ranges such as /29, so
/// passing them makes the helm apply fail.
pub fn ipv4_cidrs(cidrs: &[String]) -> Vec<String> {
    cidrs.iter().filter(|c| !c.contains(':')).cloned().collect()
}

/// The values document applied with `--reuse-values -f`.
pub fn intake_values(opts: &FactoryIntakeOpts, cidrs: &[String]) -> serde_json::Value {
    let mut api = serde_json::Map::new();
    api.insert(
        "githubFactoryIngressEnabled".into(),
        serde_json::json!(!opts.disable),
    );
    if opts.disable {
        return serde_json::json!({ "api": api });
    }
    if let Some(label) = &opts.label {
        api.insert("githubFactoryLabel".into(), serde_json::json!(label));
    }
    if let Some(mention) = &opts.mention {
        api.insert("githubFactoryMention".into(), serde_json::json!(mention));
    }
    if let Some(url) = &opts.card_base_url {
        api.insert("githubFactoryCardBaseUrl".into(), serde_json::json!(url));
    }
    if !opts.repos.is_empty() {
        api.insert("githubRepoAllowlist".into(), serde_json::json!(opts.repos));
    }
    if let Some(secret) = &opts.webhook_secret {
        api.insert("githubWebhookSecret".into(), serde_json::json!(secret));
    }
    if let Some(intake) = &opts.intake {
        api.insert("githubFactoryIntake".into(), serde_json::json!(intake));
    }
    let mut values = serde_json::json!({ "api": api });
    if !opts.github_api_egress.is_empty() {
        let rules: Vec<serde_json::Value> = ipv4_cidrs(cidrs)
            .iter()
            .map(|cidr| serde_json::json!({"cidr": cidr, "ports": [{"port": 443, "protocol": "TCP"}]}))
            .collect();
        let egress: serde_json::Map<String, serde_json::Value> = opts
            .github_api_egress
            .iter()
            .map(|agent| (agent.clone(), serde_json::json!(rules)))
            .collect();
        values["agentSandbox"] = serde_json::json!({ "connectorEgress": egress });
    }
    // The quickstart finish upgrade is this one document. The chart is the
    // caller's `--chart` (the chart `cluster up` already installed), not a
    // second chart resolved from the release archive. `--reuse-values` merges
    // these additive keys onto the release. A null clear still belongs to
    // `bind_if_changed` (`--reset-then-reuse-values`, ADR 0173 decision 5);
    // this document only sets a digest.
    if let Some(binding) = &opts.runner_binding {
        insert_runner_image(&mut values, &binding.agent, &binding.image);
    }
    values
}

fn insert_runner_image(values: &mut serde_json::Value, agent: &str, image: &str) {
    let root = values.as_object_mut().expect("intake values are an object");
    let sandbox = root
        .entry("agentSandbox")
        .or_insert_with(|| serde_json::json!({}));
    if !sandbox.is_object() {
        *sandbox = serde_json::json!({});
    }
    let images = sandbox
        .as_object_mut()
        .expect("agentSandbox is an object")
        .entry("runnerImages")
        .or_insert_with(|| serde_json::json!({}));
    if !images.is_object() {
        *images = serde_json::json!({});
    }
    images
        .as_object_mut()
        .expect("runnerImages is an object")
        .insert(agent.to_string(), serde_json::json!(image));
}

fn helm_upgrade(opts: &FactoryIntakeOpts, values_file: &Path, timeout_seconds: u64) -> OpsCommand {
    // One chart source (the caller's chart) and one values mode
    // (`--reuse-values -f`). Intake settings and a runner binding share this
    // upgrade so a quickstart finish does not helm-upgrade the release twice.
    OpsCommand::new(
        "helm",
        vec![
            plain("upgrade"),
            plain(&opts.common.release),
            plain(&opts.chart),
            plain("-n"),
            plain(&opts.common.namespace),
            plain("--reuse-values"),
            plain("--timeout"),
            plain(format!("{timeout_seconds}s")),
            plain("-f"),
            plain(values_file.display().to_string()),
        ],
    )
}

/// The helm upgrade, then the api rollout, with the chart's default fullname.
/// The live run resolves the rendered fullname instead.
pub fn intake_commands(
    opts: &FactoryIntakeOpts,
    values_file: &Path,
    timeout_seconds: u64,
) -> Vec<OpsCommand> {
    let mut cmds = vec![helm_upgrade(opts, values_file, timeout_seconds)];
    cmds.extend(crate::github_app::rollout_commands(
        &opts.common.namespace,
        &crate::ops::chart_fullname(&opts.common.release),
    ));
    cmds
}

async fn fetch_github_api_cidrs() -> Result<Vec<String>> {
    let api = crate::github_app::github_api_url(crate::github_app::DEFAULT_CLONE_BASE);
    let url = format!("{}/meta", api.trim_end_matches('/'));
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(15))
        .build()
        .map_err(|err| CliError::failure(format!("could not build an HTTP client: {err}")))?;
    let response = client
        .get(&url)
        .header("Accept", "application/vnd.github+json")
        .header("User-Agent", format!("curie/{}", env!("CARGO_PKG_VERSION")))
        .send()
        .await
        .map_err(|err| {
            CliError::failure(format!("fetching {url} failed: {err}; nothing was applied"))
        })?;
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(CliError::failure(format!(
            "fetching {url} returned {status}; nothing was applied"
        ))
        .into());
    }
    let cidrs = ipv4_cidrs(&github_api_cidrs(&body)?);
    if cidrs.is_empty() {
        return Err(CliError::failure(format!(
            "{url} lists no IPv4 api ranges; nothing was applied"
        ))
        .into());
    }
    Ok(cidrs)
}

/// True for a bare GitHub login: alphanumeric runs joined by single hyphens
/// (the API's `valid_github_login`). A leading '@' is not part of a login.
pub(crate) fn valid_github_login(value: &str) -> bool {
    !value.is_empty()
        && value
            .split('-')
            .all(|part| !part.is_empty() && part.chars().all(|c| c.is_ascii_alphanumeric()))
}

fn api_str<'a>(api: &'a serde_json::Value, key: &str) -> &'a str {
    api.get(key).and_then(|v| v.as_str()).unwrap_or("")
}

/// The API's ingress preconditions (apps/api config.py) evaluated against the
/// recorded helm user values overlaid with the planned `api.*` values.
/// Returns the offending setting names; empty means the API will start.
pub fn intake_gate_offenders(
    recorded: &serde_json::Value,
    planned: &serde_json::Value,
) -> Vec<&'static str> {
    let mut api = recorded
        .get("api")
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default();
    if let Some(over) = planned.get("api").and_then(|v| v.as_object()) {
        for (k, v) in over {
            api.insert(k.clone(), v.clone());
        }
    }
    let api = serde_json::Value::Object(api);
    let mut bad = Vec::new();
    if api_str(&api, "githubAppId").is_empty() {
        bad.push("GITHUB_APP_ID");
    }
    if api_str(&api, "githubAppPrivateKey").is_empty()
        && api_str(&api, "githubAppExistingSecret").is_empty()
    {
        bad.push("GITHUB_APP_PRIVATE_KEY");
    }
    let intake = api_str(&api, "githubFactoryIntake");
    let secret = api_str(&api, "githubWebhookSecret");
    let security_values = if planned.pointer("/security/allowDevDefaults").is_some() {
        planned
    } else {
        recorded
    };
    let allow_dev_defaults =
        crate::ops::lookup_dotted_flag(security_values, "security.allowDevDefaults");
    let secret_is_missing = api
        .get("githubWebhookSecret")
        .is_none_or(serde_json::Value::is_null);
    let published_secret =
        secret == "dev-webhook-secret" || (secret_is_missing && allow_dev_defaults);
    if published_secret || (intake == "webhook" && secret.trim().is_empty()) {
        bad.push("GITHUB_WEBHOOK_SECRET");
    }
    let label = api_str(&api, "githubFactoryLabel");
    if label.is_empty() || label.chars().any(char::is_whitespace) || label.chars().count() > 50 {
        bad.push("GITHUB_FACTORY_LABEL");
    }
    if !valid_github_login(api_str(&api, "githubFactoryMention")) {
        bad.push("GITHUB_FACTORY_MENTION");
    }
    let repos_ok = api
        .get("githubRepoAllowlist")
        .and_then(|v| v.as_array())
        .is_some_and(|l| !l.is_empty());
    if !repos_ok {
        bad.push("GITHUB_REPO_ALLOWLIST");
    }
    let environment = api
        .get("environment")
        .and_then(|v| v.as_str())
        .unwrap_or("dev");
    if !card_base_url_valid(api_str(&api, "githubFactoryCardBaseUrl"), environment) {
        bad.push("GITHUB_FACTORY_CARD_BASE_URL");
    }
    bad
}

/// Mirrors the API's `Settings._check_factory_card_base_url`: empty, or an
/// https origin with a host and no query or fragment; plain http only for
/// localhost/127.0.0.1 when the API environment is `dev`. One trailing '/'
/// is ignored. `environment` is `api.environment` (chart default `dev`).
pub fn card_base_url_valid(raw: &str, environment: &str) -> bool {
    let trimmed = raw.trim();
    let value = trimmed.strip_suffix('/').unwrap_or(trimmed);
    if value.is_empty() {
        return true;
    }
    if value.contains('?') || value.contains('#') {
        return false;
    }
    match reqwest::Url::parse(value) {
        Ok(u) if u.host_str().is_some() => {}
        _ => return false,
    }
    let Some((scheme, rest)) = value.split_once("://") else {
        return false;
    };
    let scheme = scheme.to_ascii_lowercase();
    let netloc = rest.split('/').next().unwrap_or("");
    if netloc.is_empty() {
        return false;
    }
    let hostport = netloc.rsplit_once('@').map_or(netloc, |(_, h)| h);
    let hostname = if let Some(stripped) = hostport.strip_prefix('[') {
        stripped.split(']').next().unwrap_or("")
    } else {
        hostport.split(':').next().unwrap_or("")
    }
    .to_ascii_lowercase();
    let local_dev = environment.trim().eq_ignore_ascii_case("dev")
        && scheme == "http"
        && (hostname == "localhost" || hostname == "127.0.0.1");
    scheme == "https" || local_dev
}

fn offender_flag(name: &str) -> &'static str {
    match name {
        "GITHUB_APP_ID" | "GITHUB_APP_PRIVATE_KEY" => {
            "--app-id <ID> --private-key-file <PATH> (run without them for the App registration link)"
        }
        "GITHUB_WEBHOOK_SECRET" => "--webhook-secret-file",
        "GITHUB_FACTORY_LABEL" => "--label (no whitespace, 50 chars max)",
        "GITHUB_FACTORY_MENTION" => "--mention <app-slug> (the App login, no '@')",
        "GITHUB_FACTORY_CARD_BASE_URL" => {
            "--card-base-url https://<host> (no query or fragment; http only for localhost on a dev install)"
        }
        _ => "--repo owner/name",
    }
}

/// The App-derived settings resolved before any mutation (#3746).
struct AppPlan {
    app: crate::factory_app::InstalledApp,
    api: crate::factory_app::GithubApi,
    app_id: String,
    secret_name: String,
    secret_key: String,
    secret: crate::factory_app::SecretPlan,
    mention: crate::factory_app::Chosen<String>,
    repos: crate::factory_app::Chosen<Vec<String>>,
    label: crate::factory_app::Chosen<String>,
    /// Concrete repos (wildcards expanded) whose label is missing.
    labels_missing: Vec<String>,
}

fn recorded_api_str(recorded: &serde_json::Value, key: &str) -> Option<String> {
    recorded
        .pointer(&format!("/api/{key}"))
        .and_then(|v| match v {
            serde_json::Value::String(s) => Some(s.clone()),
            serde_json::Value::Number(n) => Some(n.to_string()),
            _ => None,
        })
        .filter(|s| !s.trim().is_empty())
}

fn app_values(plan: &AppPlan) -> serde_json::Value {
    serde_json::json!({
        "githubAppId": plan.app_id,
        "githubAppExistingSecret": plan.secret_name,
        "githubAppExistingSecretKey": plan.secret_key,
        "githubAppPrivateKey": "",
        "githubCloneBase": crate::github_app::DEFAULT_CLONE_BASE,
    })
}

fn merge_api(values: &mut serde_json::Value, extra: &serde_json::Value) {
    if let (Some(api), Some(extra)) = (
        values.get_mut("api").and_then(|v| v.as_object_mut()),
        extra.as_object(),
    ) {
        for (k, v) in extra {
            api.insert(k.clone(), v.clone());
        }
    }
}

/// Every GitHub read and the Secret ownership check; mutates nothing.
async fn plan_app(
    opts: &mut FactoryIntakeOpts,
    recorded: &serde_json::Value,
    app_id: &str,
    key_file: &Path,
) -> Result<AppPlan> {
    let pem = crate::factory_app::read_private_key(key_file)?;
    let api = crate::factory_app::GithubApi::new()?;
    let app = crate::factory_app::inspect_app(&api, app_id, &pem).await?;
    let repos_inferred = opts.repos.is_empty();
    let repos = crate::factory_app::resolve_allowlist(&opts.repos, &app)?;
    opts.repos = repos.clone();
    let mention_inferred = opts.mention.is_none();
    let mention = opts.mention.clone().unwrap_or_else(|| app.slug.clone());
    opts.mention = Some(mention.clone());
    let (label, label_inferred) = match opts.label.clone() {
        Some(label) => (label, false),
        None => match recorded_api_str(recorded, "githubFactoryLabel") {
            Some(label) => (label, false),
            None => (crate::factory_app::DEFAULT_LABEL.to_string(), true),
        },
    };
    opts.label = Some(label.clone());
    let secret_name = recorded_api_str(recorded, "githubAppExistingSecret")
        .unwrap_or_else(|| crate::factory_app::DEFAULT_SECRET_NAME.to_string());
    let secret_key = recorded_api_str(recorded, "githubAppExistingSecretKey")
        .unwrap_or_else(|| crate::github_app::DEFAULT_APP_KEY_DATA_KEY.to_string());
    let secret = crate::factory_app::plan_secret(
        &api,
        &opts.common.namespace,
        &secret_name,
        &secret_key,
        app_id,
        &pem,
    )
    .await?;
    let mut labels_missing = Vec::new();
    for repo in crate::factory_app::label_targets(&repos, &app.repos) {
        if !crate::factory_app::label_exists(&api, &app, &repo, &label).await? {
            labels_missing.push(repo);
        }
    }
    Ok(AppPlan {
        labels_missing,
        app,
        api,
        app_id: app_id.to_string(),
        secret_name,
        secret_key,
        secret,
        mention: crate::factory_app::Chosen {
            value: mention,
            inferred: mention_inferred,
        },
        repos: crate::factory_app::Chosen {
            value: repos,
            inferred: repos_inferred,
        },
        label: crate::factory_app::Chosen {
            value: label,
            inferred: label_inferred,
        },
    })
}

fn app_dry_run_lines(opts: &FactoryIntakeOpts, key_file: &Path) -> Vec<String> {
    let api = crate::github_app::github_api_url(crate::github_app::DEFAULT_CLONE_BASE);
    let ns = &opts.common.namespace;
    vec![
        format!(
            "# read the App private key from {} (never printed)",
            key_file.display()
        ),
        format!("# GET {api}/app with an App JWT (slug, id check)"),
        format!("# GET {api}/app/installations"),
        format!(
            "# POST {api}/app/installations/<id>/access_tokens, then GET {api}/installation/repositories per installation"
        ),
        format!(
            "kubectl -n {ns} get secret <api.githubAppExistingSecret or {}> -o json",
            crate::factory_app::DEFAULT_SECRET_NAME
        ),
        format!(
            "{} (Secret manifest on stdin, annotated {})",
            crate::factory_app::secret_apply_command(ns).display(),
            crate::factory_app::APP_ID_ANNOTATION
        ),
        format!("# GET/POST {api}/repos/<owner>/<repo>/labels for each allowlisted repo"),
    ]
}

pub async fn factory_intake(mut opts: FactoryIntakeOpts) -> Result<Box<dyn crate::ui::CliOutput>> {
    if let Some(mention) = opts.mention.as_deref() {
        if !opts.disable && !valid_github_login(mention) {
            return Err(
                CliError::usage(format!("--mention {mention:?} is not a GitHub login"))
                    .with_fix(format!(
                        "pass the bare App login without '@', e.g. --mention {}",
                        mention.trim_start_matches('@')
                    ))
                    .into(),
            );
        }
    }
    if let Some(org) = opts.org.as_deref() {
        if !valid_github_login(org) {
            return Err(
                CliError::usage(format!("--org {org:?} is not a GitHub login"))
                    .with_fix("pass the organization login as it appears in its GitHub URL")
                    .into(),
            );
        }
    }
    let app_args = match (opts.app_id.clone(), opts.private_key_file.clone()) {
        (Some(id), Some(file)) => {
            if id.is_empty() || !id.chars().all(|c| c.is_ascii_digit()) {
                return Err(CliError::usage(format!("--app-id {id:?} is not a number"))
                    .with_fix("pass the numeric App ID from the App's settings page")
                    .into());
            }
            Some((id, file))
        }
        (None, None) => None,
        _ => {
            return Err(CliError::usage(
                "--app-id and --private-key-file go together; pass both or neither",
            )
            .with_fix("add the missing --app-id <ID> or --private-key-file <PATH>")
            .into())
        }
    };
    if opts.disable && app_args.is_some() {
        return Err(CliError::usage(
            "--disable changes nothing else; drop --app-id/--private-key-file",
        )
        .into());
    }
    if opts.common.dry_run {
        let placeholder = Path::new("<private-values-file>");
        let mut lines = Vec::new();
        if let Some((_, file)) = &app_args {
            lines.extend(app_dry_run_lines(&opts, file));
        }
        if !opts.disable && !opts.github_api_egress.is_empty() {
            let api = crate::github_app::github_api_url(crate::github_app::DEFAULT_CLONE_BASE);
            lines.push(format!(
                "# fetch {}/meta for the GitHub API egress CIDRs",
                api
            ));
        }
        let timeout = effective_helm_timeout(opts.timeout_seconds, None);
        lines.extend(
            intake_commands(&opts, placeholder, timeout)
                .iter()
                .map(|c| c.display()),
        );
        return Ok(Box::new(FactoryIntakeOutput::DryRun(
            crate::ui::DryRunPlan { lines },
        )));
    }
    let mut app_plan = None;
    if !opts.disable {
        let recorded = fetch_release_values(&opts.common)
            .await?
            .unwrap_or(serde_json::Value::Null);
        match &app_args {
            None if recorded_api_str(&recorded, "githubAppId").is_none() => {
                // No App anywhere: print the registration link, apply nothing.
                // The CLI never opens a browser and never runs `gh`.
                let name = crate::factory_app::random_app_name();
                return Ok(Box::new(crate::factory_app::FactoryAppRegistrationOutput {
                    url: crate::factory_app::registration_url(opts.org.as_deref(), &name),
                    steps: crate::factory_app::registration_steps(),
                }));
            }
            None => {}
            Some((id, file)) => {
                app_plan = Some(plan_app(&mut opts, &recorded, id, file).await?);
            }
        }
        let mut planned = intake_values(&opts, &[]);
        if let Some(plan) = &app_plan {
            merge_api(&mut planned, &app_values(plan));
        }
        let offenders = intake_gate_offenders(&recorded, &planned);
        if !offenders.is_empty() {
            let fixes: Vec<String> = offenders
                .iter()
                .map(|o| offender_flag(o).to_string())
                .collect();
            return Err(CliError::usage(format!(
                "the API would refuse to start with factory intake enabled; missing or invalid: {}",
                offenders.join(", ")
            ))
            .with_fix(format!(
                "provide: {}; nothing was applied",
                fixes.join("; ")
            ))
            .into());
        }
    }
    require_on_path("helm")?;
    require_on_path("kubectl")?;
    let cidrs = if !opts.disable && !opts.github_api_egress.is_empty() {
        fetch_github_api_cidrs().await?
    } else {
        Vec::new()
    };
    let mut values = intake_values(&opts, &cidrs);
    if let Some(plan) = &app_plan {
        merge_api(&mut values, &app_values(plan));
    }
    let timeout = match opts.timeout_seconds {
        Some(explicit) => explicit,
        None => effective_helm_timeout(None, release_minimum_helm_timeout(&opts.common).await),
    };
    let ui = crate::ui::ui();
    let cl = ui.checklist();
    let mut labels_created = Vec::new();
    let mut secret_written = false;
    if let Some(plan) = &app_plan {
        if let crate::factory_app::SecretPlan::Write(manifest) = &plan.secret {
            let step = cl.step(&format!(
                "storing the App private key in Secret {}",
                plan.secret_name
            ));
            crate::factory_app::apply_secret(&opts.common.namespace, manifest).await?;
            step.done("stored");
            secret_written = true;
        }
        for repo in &plan.labels_missing {
            crate::factory_app::create_label(&plan.api, &plan.app, repo, &plan.label.value).await?;
            labels_created.push(repo.clone());
        }
    }
    {
        let file = crate::ops::SecretValuesFileGuard::private_document(&values)?;
        let cmd = helm_upgrade(&opts, file.path(), timeout);
        ui.plumbing(&format!("+ {}", cmd.display()));
        let step = cl.step(&format!(
            "configuring the factory intake on release {}",
            opts.common.release
        ));
        let (ok, _, stderr) = run_capture(&cmd).await?;
        if !ok {
            step.fail("helm upgrade failed");
            return Err(CliError::failure(format!(
                "helm upgrade of release {} failed: {}",
                opts.common.release,
                stderr.trim()
            ))
            .into());
        }
        step.done("configured");
    }
    let fullname = crate::ops::release_fullname(&opts.common.namespace, &opts.common.release).await;
    for cmd in crate::github_app::rollout_commands(&opts.common.namespace, &fullname) {
        ui.plumbing(&format!("+ {}", cmd.display()));
        let step = cl.step(&format!(
            "rolling {} to pick up the intake settings",
            opts.common.release
        ));
        let (ok, _, stderr) = run_capture(&cmd).await?;
        if !ok {
            step.fail("failed");
            return Err(CliError::failure(format!(
                "api rollout failed after the intake was configured: {}",
                stderr.trim()
            ))
            .into());
        }
        step.done("rolled");
    }
    if let Some(plan) = app_plan {
        return Ok(Box::new(crate::factory_app::FactoryAppSetupOutput {
            app_id: plan.app_id,
            slug: plan.app.slug,
            mention: plan.mention,
            repos: plan.repos,
            label: plan.label,
            labels_created,
            secret: plan.secret_name,
            secret_written,
        }));
    }
    Ok(Box::new(FactoryIntakeOutput::Done {
        enabled: !opts.disable,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::{CommonOpts, OpsCommand};
    use std::path::Path;

    const SECRET: &str = "whsec-test-value-9f2c";

    fn opts() -> FactoryIntakeOpts {
        FactoryIntakeOpts {
            common: CommonOpts {
                namespace: "curie".into(),
                release: "curie".into(),
                dry_run: true,
            },
            chart: "charts/curie".into(),
            repos: vec!["acme/widgets".into(), "acme/gadgets".into()],
            label: Some("factory".into()),
            mention: Some("curie-app".into()),
            card_base_url: Some("https://cards.example.trycloudflare.com".into()),
            webhook_secret: Some(SECRET.into()),
            github_api_egress: vec!["dark-factory".into()],
            disable: false,
            timeout_seconds: None,
            app_id: None,
            private_key_file: None,
            org: None,
            intake: None,
            runner_binding: None,
        }
    }

    fn argv(cmd: &OpsCommand) -> Vec<String> {
        cmd.argv()
    }

    fn cidrs() -> Vec<String> {
        vec!["192.30.252.0/22".into(), "140.82.112.0/20".into()]
    }

    // C1
    #[test]
    fn github_api_cidrs_reads_the_api_list_in_order() {
        let meta = r#"{"verifiable_password_authentication":false,"hooks":["10.0.0.0/8"],"api":["192.30.252.0/22","140.82.112.0/20","2a0a:a440::/29"]}"#;
        assert_eq!(
            github_api_cidrs(meta).unwrap(),
            vec![
                "192.30.252.0/22".to_string(),
                "140.82.112.0/20".to_string(),
                "2a0a:a440::/29".to_string()
            ]
        );
    }

    #[test]
    fn github_api_cidrs_without_an_api_list_is_an_error() {
        let error = github_api_cidrs(r#"{"hooks":["10.0.0.0/8"]}"#)
            .expect_err("a meta document without `api` must be refused");
        assert!(format!("{error:#}").contains("api"), "{error:#}");
        assert!(
            github_api_cidrs(r#"{"api":[]}"#).is_err(),
            "an empty api list must be refused"
        );
    }

    // C2
    #[test]
    fn intake_values_carry_the_factory_settings_and_egress() {
        let values = intake_values(&opts(), &cidrs());
        let api = &values["api"];
        assert_eq!(api["githubFactoryIngressEnabled"], serde_json::json!(true));
        assert_eq!(api["githubFactoryLabel"], serde_json::json!("factory"));
        assert_eq!(api["githubFactoryMention"], serde_json::json!("curie-app"));
        assert_eq!(
            api["githubFactoryCardBaseUrl"],
            serde_json::json!("https://cards.example.trycloudflare.com")
        );
        assert_eq!(
            api["githubRepoAllowlist"],
            serde_json::json!(["acme/widgets", "acme/gadgets"])
        );
        assert_eq!(api["githubWebhookSecret"], serde_json::json!(SECRET));
        assert_eq!(
            values["agentSandbox"]["connectorEgress"]["dark-factory"],
            serde_json::json!([
                {"cidr": "192.30.252.0/22", "ports": [{"port": 443, "protocol": "TCP"}]},
                {"cidr": "140.82.112.0/20", "ports": [{"port": 443, "protocol": "TCP"}]},
            ])
        );
    }

    // C3
    #[test]
    fn intake_commands_reuse_values_from_a_file_and_never_carry_the_secret() {
        let values_file = Path::new("/tmp/curie-factory-values.json");
        let commands = intake_commands(&opts(), values_file, HELM_TIMEOUT_FLOOR_SECS);
        let helm = commands
            .iter()
            .find(|c| c.program == "helm")
            .expect("a helm upgrade command");
        let args = argv(helm);
        assert_eq!(
            &args[..3],
            &["upgrade".to_string(), "curie".into(), "charts/curie".into()],
            "{args:?}"
        );
        assert!(args.iter().any(|a| a == "--reuse-values"), "{args:?}");
        let f = args.iter().position(|a| a == "-f").expect("-f present");
        assert_eq!(args[f + 1], values_file.display().to_string());
        let n = args.iter().position(|a| a == "-n").expect("-n present");
        assert_eq!(args[n + 1], "curie");
        for cmd in &commands {
            let joined = argv(cmd).join(" ");
            assert!(
                !joined.contains(SECRET),
                "secret leaked into argv: {joined}"
            );
        }
        assert!(
            commands
                .iter()
                .any(|c| c.program == "kubectl" && argv(c).iter().any(|a| a == "restart")),
            "the api must be restarted after the upgrade"
        );
    }

    #[test]
    fn intake_values_bind_a_runner_image_in_the_same_document() {
        let mut o = opts();
        o.runner_binding = Some(RunnerImageBinding {
            agent: "dark-factory".into(),
            image: "ghcr.io/curie-eng/curie-dark-factory-runner@sha256:abcd".into(),
        });
        let values = intake_values(&o, &cidrs());
        assert_eq!(
            values["agentSandbox"]["runnerImages"]["dark-factory"],
            serde_json::json!("ghcr.io/curie-eng/curie-dark-factory-runner@sha256:abcd")
        );
        assert!(values["agentSandbox"].get("connectorEgress").is_some());
        let commands = intake_commands(
            &o,
            Path::new("/tmp/curie-factory-values.json"),
            HELM_TIMEOUT_FLOOR_SECS,
        );
        let helm = commands
            .iter()
            .find(|c| c.program == "helm")
            .expect("a helm upgrade command");
        let args = argv(helm);
        assert_eq!(
            args.iter().filter(|arg| arg.as_str() == "upgrade").count(),
            1
        );
        assert!(args.iter().any(|arg| arg == "--reuse-values"), "{args:?}");
        assert!(args.iter().any(|arg| arg == "charts/curie"), "{args:?}");
        assert!(!args.iter().any(|arg| arg == "--reset-then-reuse-values"));
    }

    // C5
    #[test]
    fn disable_turns_ingress_off_and_carries_no_secret() {
        let mut o = opts();
        o.disable = true;
        o.webhook_secret = None;
        let values = intake_values(&o, &[]);
        assert_eq!(
            values["api"]["githubFactoryIngressEnabled"],
            serde_json::json!(false)
        );
        assert!(
            values["api"].get("githubWebhookSecret").is_none(),
            "disable must not write a secret: {values}"
        );
        assert!(!values.to_string().contains(SECRET), "{values}");
    }

    fn full() -> serde_json::Value {
        serde_json::json!({"api": {
            "githubAppId": "123", "githubAppPrivateKey": "k",
            "githubWebhookSecret": "s3cret", "githubFactoryLabel": "curie-factory",
            "githubFactoryMention": "my-app", "githubRepoAllowlist": ["a/b"]}})
    }

    #[test]
    fn gate_complete_config_has_no_offenders() {
        assert!(intake_gate_offenders(&serde_json::Value::Null, &full()).is_empty());
    }

    #[test]
    fn gate_missing_mention_is_named() {
        let mut v = full();
        v["api"]
            .as_object_mut()
            .unwrap()
            .remove("githubFactoryMention");
        assert_eq!(
            intake_gate_offenders(&v, &serde_json::json!({})),
            vec!["GITHUB_FACTORY_MENTION"]
        );
    }

    #[test]
    fn gate_merges_recorded_label_with_planned_mention() {
        let mut rec = full();
        rec["api"]
            .as_object_mut()
            .unwrap()
            .remove("githubFactoryMention");
        let planned = serde_json::json!({"api": {"githubFactoryMention": "x-y"}});
        assert!(intake_gate_offenders(&rec, &planned).is_empty());
    }

    #[test]
    fn gate_dev_default_secret_and_at_mention_are_offenders() {
        // Webhook intake refuses both blank and published signing keys.
        for secret in ["dev-webhook-secret", ""] {
            let planned = serde_json::json!({"api": {
                "githubFactoryIntake": "webhook",
                "githubWebhookSecret": secret,
                "githubFactoryMention": "@x"
            }});
            assert_eq!(
                intake_gate_offenders(&full(), &planned),
                vec!["GITHUB_WEBHOOK_SECRET", "GITHUB_FACTORY_MENTION"],
                "{secret}"
            );
        }
    }

    #[test]
    fn gate_poll_or_absent_intake_does_not_require_a_webhook_secret() {
        let secrets = ["", "configured-webhook-secret"];
        let intakes = [Some("poll"), None];
        for secret in secrets {
            for intake in intakes {
                let mut recorded = full();
                let api = recorded["api"].as_object_mut().unwrap();
                api.insert("githubWebhookSecret".to_string(), serde_json::json!(secret));
                if let Some(value) = intake {
                    api.insert("githubFactoryIntake".to_string(), serde_json::json!(value));
                }
                let offenders = intake_gate_offenders(&recorded, &serde_json::json!({}));
                assert!(
                    offenders.is_empty(),
                    "intake {intake:?} secret {secret:?} offenders {offenders:?}"
                );
            }
        }
    }

    #[test]
    fn gate_published_webhook_secret_is_refused_in_every_intake_mode() {
        for intake in [Some("poll"), Some("webhook"), None] {
            let mut recorded = full();
            let api = recorded["api"].as_object_mut().unwrap();
            api.insert(
                "githubWebhookSecret".to_string(),
                serde_json::json!("dev-webhook-secret"),
            );
            if let Some(value) = intake {
                api.insert("githubFactoryIntake".to_string(), serde_json::json!(value));
            }
            assert_eq!(
                intake_gate_offenders(&recorded, &serde_json::json!({})),
                vec!["GITHUB_WEBHOOK_SECRET"],
                "intake {intake:?}"
            );
        }
    }

    #[test]
    fn gate_absent_or_null_secret_uses_effective_security_defaults() {
        for intake in [Some("poll"), Some("webhook"), None] {
            for null_secret in [false, true] {
                for allow_dev_defaults in [serde_json::json!(true), serde_json::json!("true")] {
                    let mut recorded = full();
                    let api = recorded["api"].as_object_mut().unwrap();
                    api.remove("githubWebhookSecret");
                    if null_secret {
                        api.insert("githubWebhookSecret".to_string(), serde_json::Value::Null);
                    }
                    if let Some(value) = intake {
                        api.insert("githubFactoryIntake".to_string(), serde_json::json!(value));
                    }
                    recorded["security"] = serde_json::json!({
                        "allowDevDefaults": allow_dev_defaults
                    });
                    assert_eq!(
                        intake_gate_offenders(&recorded, &serde_json::json!({})),
                        vec!["GITHUB_WEBHOOK_SECRET"]
                    );

                    let sealed_offenders = if intake == Some("webhook") {
                        vec!["GITHUB_WEBHOOK_SECRET"]
                    } else {
                        vec![]
                    };
                    let planned = serde_json::json!({"security": {"allowDevDefaults": false}});
                    assert_eq!(intake_gate_offenders(&recorded, &planned), sealed_offenders);
                    recorded["security"]["allowDevDefaults"] = serde_json::json!(false);
                    assert_eq!(
                        intake_gate_offenders(&recorded, &serde_json::json!({})),
                        sealed_offenders
                    );
                    let planned = serde_json::json!({"security": {"allowDevDefaults": true}});
                    assert_eq!(
                        intake_gate_offenders(&recorded, &planned),
                        vec!["GITHUB_WEBHOOK_SECRET"]
                    );
                    let planned = serde_json::json!({"api": {
                        "githubWebhookSecret": "configured-webhook-secret"
                    }});
                    recorded["security"]["allowDevDefaults"] = serde_json::json!(true);
                    assert!(intake_gate_offenders(&recorded, &planned).is_empty());
                }
            }
        }
    }

    #[test]
    fn gate_explicit_blank_poll_secret_disables_signatures_with_development_defaults() {
        let mut recorded = full();
        recorded["api"]["githubWebhookSecret"] = serde_json::json!("");
        recorded["security"] = serde_json::json!({"allowDevDefaults": true});

        assert!(intake_gate_offenders(&recorded, &serde_json::json!({})).is_empty());
    }

    #[test]
    fn card_base_url_matches_the_api_validator() {
        assert!(card_base_url_valid("", "prod"));
        assert!(card_base_url_valid("https://cards.example.com", "prod"));
        assert!(card_base_url_valid("https://cards.example.com/", "prod"));
        assert!(card_base_url_valid(
            "https://cards.example.com/base",
            "prod"
        ));
        assert!(!card_base_url_valid("http://cards.example.com", "dev"));
        assert!(!card_base_url_valid("http://localhost:8080", "prod"));
        assert!(card_base_url_valid("http://localhost:8080", "dev"));
        assert!(card_base_url_valid("http://127.0.0.1", "DEV"));
        assert!(!card_base_url_valid(
            "https://cards.example.com?x=1",
            "prod"
        ));
        assert!(!card_base_url_valid(
            "https://cards.example.com/#frag",
            "prod"
        ));
        assert!(!card_base_url_valid("https://cards.example.com/?", "prod"));
        assert!(!card_base_url_valid("https://", "prod"));
        assert!(!card_base_url_valid("cards.example.com", "prod"));
        assert!(!card_base_url_valid("ftp://cards.example.com", "prod"));
    }

    #[test]
    fn card_base_url_rejects_unparseable_bracketed_host() {
        assert!(!card_base_url_valid("https://[cards.example.com", "prod"));
        assert!(card_base_url_valid("https://[::1]", "prod"));
        let planned =
            serde_json::json!({"api": {"githubFactoryCardBaseUrl": "https://[cards.example.com"}});
        assert_eq!(
            intake_gate_offenders(&full(), &planned),
            vec!["GITHUB_FACTORY_CARD_BASE_URL"]
        );
    }

    #[test]
    fn gate_refuses_http_and_query_card_urls_and_accepts_https() {
        for bad in ["http://cards.example.com", "https://cards.example.com?q=1"] {
            let planned = serde_json::json!({"api": {"githubFactoryCardBaseUrl": bad}});
            assert_eq!(
                intake_gate_offenders(&full(), &planned),
                vec!["GITHUB_FACTORY_CARD_BASE_URL"],
                "{bad}"
            );
        }
        let ok =
            serde_json::json!({"api": {"githubFactoryCardBaseUrl": "https://cards.example.com"}});
        assert!(intake_gate_offenders(&full(), &ok).is_empty());
    }

    #[test]
    fn gate_reads_the_recorded_environment_for_localhost_http() {
        let planned =
            serde_json::json!({"api": {"githubFactoryCardBaseUrl": "http://localhost:3000"}});
        assert!(intake_gate_offenders(&full(), &planned).is_empty());
        let mut prod = full();
        prod["api"]["environment"] = serde_json::json!("prod");
        assert_eq!(
            intake_gate_offenders(&prod, &planned),
            vec!["GITHUB_FACTORY_CARD_BASE_URL"]
        );
    }

    #[test]
    fn hook_annotation_drives_the_helm_timeout() {
        let hooks = r#"---
# Source: curie/templates/worker-upgrade-drain.yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: curie-worker-upgrade-drain
  annotations:
    "helm.sh/hook": pre-upgrade
    "curie.ai/minimum-helm-timeout-seconds": "21900"
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: other
"#;
        let minimum = minimum_helm_timeout_from_hooks(hooks);
        assert_eq!(minimum, Some(21900));
        let timeout = effective_helm_timeout(None, minimum);
        assert_eq!(timeout, 21900);
        let commands = intake_commands(&opts(), Path::new("/tmp/v.json"), timeout);
        let args = argv(&commands[0]);
        let t = args
            .iter()
            .position(|a| a == "--timeout")
            .expect("--timeout present");
        assert_eq!(args[t + 1], "21900s", "{args:?}");
    }

    #[test]
    fn helm_timeout_floor_and_explicit_override() {
        assert_eq!(minimum_helm_timeout_from_hooks(""), None);
        assert_eq!(effective_helm_timeout(None, None), HELM_TIMEOUT_FLOOR_SECS);
        assert_eq!(
            effective_helm_timeout(None, Some(60)),
            HELM_TIMEOUT_FLOOR_SECS
        );
        assert_eq!(effective_helm_timeout(Some(30000), Some(21900)), 30000);
    }
}

//! `curie cluster factory`: configure GitHub factory intake (#3619) or update
//! already bound repository runners on an existing release.
//!
//! Every value rides a private 0600 values file passed with `-f` and removed
//! after the run, so the webhook secret never reaches argv. There is no
//! `--webhook-secret <value>` flag: the secret comes from a file or the
//! `CURIE_GITHUB_WEBHOOK_SECRET` environment variable only.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use anyhow::Result;

use crate::exit::CliError;
use crate::ops::{
    fetch_release_computed_values, fetch_release_values, plain, require_on_path, run_capture,
    CommonOpts, OpsCommand,
};

/// Environment variable carrying the webhook secret value.
pub const WEBHOOK_SECRET_ENV: &str = "CURIE_GITHUB_WEBHOOK_SECRET";

#[derive(Debug, Clone)]
pub struct FactoryIntakeOpts {
    pub common: CommonOpts,
    pub chart: String,
    /// `owner/repo` entries for `api.githubRepoAllowlist`; without App
    /// credentials, empty reuses the recorded allowlist.
    pub repos: Vec<String>,
    /// Explicitly allow every repository the GitHub App is installed on.
    pub all_installed_repos: bool,
    pub label: Option<String>,
    pub mention: Option<String>,
    pub card_base_url: Option<String>,
    /// The resolved webhook secret value, never a path and never from argv.
    pub webhook_secret: Option<String>,
    /// Agents enrolled for forge egress (`factory.sandboxEgress.<agent>`).
    pub forge_egress: Vec<String>,
    /// Set `factory.enabled=false` and nothing else.
    pub disable: bool,
    /// Explicit helm `--timeout` seconds; `None` derives it from the release.
    pub timeout_seconds: Option<u64>,
    /// The factory GitHub App id; paired with `private_key_file` (#3746).
    pub app_id: Option<String>,
    /// File holding the App's PEM private key. Its contents never enter argv.
    pub private_key_file: Option<std::path::PathBuf>,
    /// Organization whose registration form the printed link opens.
    pub org: Option<String>,
    /// `factory.intake` when this run must select a mode. `None`
    /// leaves the recorded value unchanged.
    pub intake: Option<String>,
    /// Digest-pinned runner image bound in the same upgrade as the intake
    /// settings. `None` leaves `agentSandbox.runnerImages` unchanged.
    pub runner_binding: Option<RunnerImageBinding>,
    /// Exact recorded repository paths paired with local runner input files.
    pub repo_runners: Vec<String>,
    pub registry: Option<String>,
    /// Exact recorded repository paths whose Docker access is enabled.
    pub repo_docker: Vec<String>,
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
            .get("metadata")
            .and_then(|m| m.get("annotations"))
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
    RepositoriesUpdated { enabled: bool, repos: Vec<String> },
}

impl crate::ui::CliOutput for FactoryIntakeOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            FactoryIntakeOutput::DryRun(plan) => plan.to_json(),
            FactoryIntakeOutput::Done { enabled }
            | FactoryIntakeOutput::RepositoriesUpdated { enabled, .. } => {
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
            FactoryIntakeOutput::RepositoriesUpdated { repos, .. } => {
                ui.payload(&format!(
                    "Updated repository bindings: {}",
                    repos.join(", ")
                ));
            }
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

/// The chart's removed `api.*` factory keys (#4397), from the one file that
/// names them. Each is written as null so a `--reuse-values` upgrade sheds it.
fn legacy_factory_keys() -> Vec<String> {
    serde_norway::from_str(include_str!(
        "../../charts/curie/files/legacy-factory-keys.yaml"
    ))
    .expect("charts/curie/files/legacy-factory-keys.yaml is a list of dotted chart keys")
}

/// The name of the one GitHub entry this command writes in
/// `factory.codeHosts` and `factory.trackers`.
const GITHUB_ENTRY: &str = "github";

/// The App credential a factory GitHub entry carries: its id and the Secret
/// holding its private key. The key itself never enters the values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FactoryCredential {
    pub app_id: String,
    pub secret_name: String,
    pub secret_key: String,
}

impl FactoryCredential {
    fn defaults(app_id: &str) -> Self {
        Self {
            app_id: app_id.to_string(),
            secret_name: crate::factory_app::DEFAULT_SECRET_NAME.to_string(),
            secret_key: crate::github_app::DEFAULT_APP_KEY_DATA_KEY.to_string(),
        }
    }

    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "type": "github_app",
            "appId": self.app_id,
            "existingSecret": self.secret_name,
            "existingSecretKey": self.secret_key,
        })
    }
}

/// The host GitHub's API answers for: `github.com` for the public API, else
/// the API URL's own host (a GitHub Enterprise Server).
fn github_host(api_url: &str) -> String {
    if api_url == crate::github_app::DEFAULT_GITHUB_API_URL {
        return "github.com".to_string();
    }
    reqwest::Url::parse(api_url)
        .ok()
        .and_then(|url| url.host_str().map(str::to_ascii_lowercase))
        .unwrap_or_default()
}

/// Write the GitHub code host and tracker entries. Helm replaces a list
/// whole, so each list is written complete.
fn set_github_entries(
    values: &mut serde_json::Value,
    credential: &FactoryCredential,
    label: Option<&str>,
    mention: Option<&str>,
) {
    let api_url = crate::github_app::github_api_url(crate::github_app::DEFAULT_CLONE_BASE);
    let entry = serde_json::json!({
        "name": GITHUB_ENTRY,
        "kind": "github",
        "host": github_host(&api_url),
        "apiUrl": api_url,
        "credential": credential.to_json(),
    });
    let mut tracker = entry.clone();
    if let Some(label) = label {
        tracker["label"] = serde_json::json!(label);
    }
    if let Some(mention) = mention {
        tracker["mention"] = serde_json::json!(mention);
    }
    values["factory"]["codeHosts"] = serde_json::json!([entry]);
    values["factory"]["trackers"] = serde_json::json!([tracker]);
}

/// The values document applied with `--reuse-values -f`.
pub fn intake_values(opts: &FactoryIntakeOpts) -> serde_json::Value {
    let mut api = serde_json::Map::new();
    for key in legacy_factory_keys() {
        if let Some(name) = key.strip_prefix("api.") {
            api.insert(name.to_string(), serde_json::Value::Null);
        }
    }
    let mut factory = serde_json::Map::new();
    factory.insert("enabled".into(), serde_json::json!(!opts.disable));
    if opts.disable {
        return serde_json::json!({ "api": api, "factory": factory });
    }
    if let Some(url) = &opts.card_base_url {
        factory.insert("cardBaseUrl".into(), serde_json::json!(url));
    }
    if let Some(intake) = &opts.intake {
        factory.insert("intake".into(), serde_json::json!(intake));
    }
    if !opts.repos.is_empty() {
        api.insert("githubRepoAllowlist".into(), serde_json::json!(opts.repos));
    }
    if let Some(secret) = &opts.webhook_secret {
        api.insert("githubWebhookSecret".into(), serde_json::json!(secret));
    }
    let mut values = serde_json::json!({ "api": api, "factory": factory });
    set_github_entries(
        &mut values,
        &FactoryCredential::defaults(opts.app_id.as_deref().unwrap_or_default()),
        opts.label.as_deref(),
        opts.mention.as_deref(),
    );
    // The github entries carry no egress, so the chart's built-in GitHub set
    // applies; each named agent's sandboxes are enrolled for it (#4397).
    if !opts.forge_egress.is_empty() {
        let agents: serde_json::Map<String, serde_json::Value> = opts
            .forge_egress
            .iter()
            .map(|agent| (agent.clone(), serde_json::Value::Bool(true)))
            .collect();
        values["factory"]["sandboxEgress"] = serde_json::Value::Object(agents);
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

fn helm_upgrade(
    common: &CommonOpts,
    chart: &str,
    values_file: &Path,
    timeout_seconds: u64,
) -> OpsCommand {
    // One chart source (the caller's chart) and one values mode
    // (`--reuse-values -f`). Intake settings and a runner binding share this
    // upgrade so a quickstart finish does not helm-upgrade the release twice.
    OpsCommand::new(
        "helm",
        vec![
            plain("upgrade"),
            plain(&common.release),
            plain(chart),
            plain("-n"),
            plain(&common.namespace),
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
    let mut cmds = vec![helm_upgrade(
        &opts.common,
        &opts.chart,
        values_file,
        timeout_seconds,
    )];
    cmds.extend(crate::github_app::rollout_commands(
        &opts.common.namespace,
        &crate::ops::chart_fullname(&opts.common.release),
    ));
    cmds
}

/// `list` of the recorded `factory` values with the entry named like `entry`
/// replaced by it, or appended when none is. Every other entry is kept.
fn replace_named(
    recorded: &serde_json::Value,
    list: &str,
    entry: &serde_json::Value,
) -> serde_json::Value {
    let name = entry.get("name");
    let mut entries: Vec<serde_json::Value> = recorded
        .pointer(&format!("/factory/{list}"))
        .and_then(|v| v.as_array())
        .map(|list| {
            list.iter()
                .filter(|kept| kept.get("name") != name)
                .cloned()
                .collect()
        })
        .unwrap_or_default();
    entries.push(entry.clone());
    serde_json::Value::Array(entries)
}

/// The values document a Jira quickstart applies with `--reuse-values -f`:
/// the release's `factory.trackers` and `factory.bindings` with the Jira
/// entries put in. Helm replaces a list whole, so each is written complete.
/// The token is not here; it lives only in the Secret the tracker names.
pub fn forge_entries_values(
    recorded: &serde_json::Value,
    plan: &crate::factory_quickstart::JiraPlan,
) -> serde_json::Value {
    serde_json::json!({
        "factory": {
            "trackers": replace_named(recorded, "trackers", &plan.tracker),
            "bindings": replace_named(recorded, "bindings", &plan.binding),
        }
    })
}

/// Write the Jira tracker and binding on the release: the same helm upgrade
/// and api rollout as the intake, over [`forge_entries_values`].
pub async fn apply_forge_entries(
    common: &CommonOpts,
    chart: &str,
    plan: &crate::factory_quickstart::JiraPlan,
) -> Result<()> {
    require_on_path("helm")?;
    require_on_path("kubectl")?;
    let recorded = fetch_release_values(common)
        .await?
        .unwrap_or(serde_json::Value::Null);
    let values = forge_entries_values(&recorded, plan);
    let timeout = effective_helm_timeout(None, release_minimum_helm_timeout(common).await);
    let ui = crate::ui::ui();
    let cl = ui.checklist();
    {
        let file = crate::ops::SecretValuesFileGuard::private_document(&values)?;
        let cmd = helm_upgrade(common, chart, file.path(), timeout);
        ui.plumbing(&format!("+ {}", cmd.display()));
        let step = cl.step(&format!(
            "writing the Jira tracker and binding `{}` on release {}",
            plan.binding_name, common.release
        ));
        let (ok, _, stderr) = run_capture(&cmd).await?;
        if !ok {
            step.fail("helm upgrade failed");
            return Err(CliError::failure(format!(
                "helm upgrade of release {} failed: {}",
                common.release,
                stderr.trim()
            ))
            .into());
        }
        step.done("written");
    }
    let fullname = crate::ops::release_fullname(&common.namespace, &common.release).await;
    for cmd in crate::github_app::rollout_commands(&common.namespace, &fullname) {
        ui.plumbing(&format!("+ {}", cmd.display()));
        let step = cl.step(&format!(
            "rolling {} to pick up the Jira tracker",
            common.release
        ));
        let (ok, _, stderr) = run_capture(&cmd).await?;
        if !ok {
            step.fail("failed");
            return Err(CliError::failure(format!(
                "api rollout failed after the Jira tracker was written: {}",
                stderr.trim()
            ))
            .into());
        }
        step.done("rolled");
    }
    Ok(())
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

const LABEL_KEY: &str = "factory.trackers[github].label";
const MENTION_KEY: &str = "factory.trackers[github].mention";
const CARD_KEY: &str = "factory.cardBaseUrl";

/// The recorded `section` map with the planned one laid over it key by key.
/// A planned list replaces the recorded list whole, as `helm -f` does.
fn overlay(
    recorded: &serde_json::Value,
    planned: &serde_json::Value,
    section: &str,
) -> serde_json::Value {
    let mut merged = recorded
        .get(section)
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default();
    if let Some(over) = planned.get(section).and_then(|v| v.as_object()) {
        for (k, v) in over {
            merged.insert(k.clone(), v.clone());
        }
    }
    serde_json::Value::Object(merged)
}

/// The GitHub tracker entry of a `factory` map, if one is configured.
fn github_tracker(factory: &serde_json::Value) -> Option<&serde_json::Value> {
    factory
        .get("trackers")
        .and_then(|v| v.as_array())?
        .iter()
        .find(|entry| entry.get("kind").and_then(|v| v.as_str()) == Some("github"))
}

/// The API's ingress preconditions (apps/api config.py) evaluated against the
/// recorded helm user values overlaid with the planned `api.*` and
/// `factory.*` values. Returns the offending setting names; empty means the
/// API will start.
pub fn intake_gate_offenders(
    recorded: &serde_json::Value,
    planned: &serde_json::Value,
) -> Vec<&'static str> {
    let api = overlay(recorded, planned, "api");
    let factory = overlay(recorded, planned, "factory");
    let tracker = github_tracker(&factory).cloned().unwrap_or_default();
    let mut bad = Vec::new();
    if api_str(&api, "githubAppId").is_empty() {
        bad.push("GITHUB_APP_ID");
    }
    if api_str(&api, "githubAppPrivateKey").is_empty()
        && api_str(&api, "githubAppExistingSecret").is_empty()
    {
        bad.push("GITHUB_APP_PRIVATE_KEY");
    }
    let intake = api_str(&factory, "intake");
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
    let label = api_str(&tracker, "label");
    if label.is_empty() || label.chars().any(char::is_whitespace) || label.chars().count() > 50 {
        bad.push(LABEL_KEY);
    }
    if !valid_github_login(api_str(&tracker, "mention")) {
        bad.push(MENTION_KEY);
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
    if !card_base_url_valid(api_str(&factory, "cardBaseUrl"), environment) {
        bad.push(CARD_KEY);
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
        LABEL_KEY => "--label (no whitespace, 50 chars max)",
        MENTION_KEY => "--mention <app-slug> (the App login, no '@')",
        CARD_KEY => {
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

/// A non-blank string field of the recorded GitHub tracker entry.
fn recorded_tracker_str(recorded: &serde_json::Value, key: &str) -> Option<String> {
    let factory = recorded.get("factory")?;
    github_tracker(factory)?
        .get(key)
        .and_then(|v| v.as_str())
        .filter(|s| !s.trim().is_empty())
        .map(str::to_string)
}

/// The App credential the factory entries carry when this run configures no
/// App: the recorded App id and the Secret the release already names.
fn recorded_credential(recorded: &serde_json::Value) -> FactoryCredential {
    FactoryCredential {
        app_id: recorded_api_str(recorded, "githubAppId").unwrap_or_default(),
        secret_name: recorded_api_str(recorded, "githubAppExistingSecret")
            .unwrap_or_else(|| crate::factory_app::DEFAULT_SECRET_NAME.to_string()),
        secret_key: recorded_api_str(recorded, "githubAppExistingSecretKey")
            .unwrap_or_else(|| crate::github_app::DEFAULT_APP_KEY_DATA_KEY.to_string()),
    }
}

/// The values this run applies: the intake document, the App's `api.*`
/// values when it configures one, and the factory entries naming that App.
/// A label or mention this run leaves unset keeps the recorded one, since the
/// entry lists are written whole.
fn planned_values(
    opts: &FactoryIntakeOpts,
    recorded: &serde_json::Value,
    app_plan: Option<&AppPlan>,
) -> serde_json::Value {
    let mut values = intake_values(opts);
    if opts.disable {
        return values;
    }
    let credential = match app_plan {
        Some(plan) => {
            merge_api(&mut values, &app_values(plan));
            FactoryCredential {
                app_id: plan.app_id.clone(),
                secret_name: plan.secret_name.clone(),
                secret_key: plan.secret_key.clone(),
            }
        }
        None => recorded_credential(recorded),
    };
    let label = opts
        .label
        .clone()
        .or_else(|| recorded_tracker_str(recorded, "label"));
    let mention = opts
        .mention
        .clone()
        .or_else(|| recorded_tracker_str(recorded, "mention"));
    set_github_entries(
        &mut values,
        &credential,
        label.as_deref(),
        mention.as_deref(),
    );
    values
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
    preflight: crate::factory_toolchain::AppPreflight,
) -> Result<(AppPlan, crate::factory_app::PublicationIdentity)> {
    let crate::factory_toolchain::AppPreflight {
        pem,
        api,
        app,
        publication,
        reports: _,
    } = preflight;
    let repos_inferred = opts.all_installed_repos;
    let repos = crate::factory_app::resolve_allowlist(&opts.repos, &app, opts.all_installed_repos)?;
    opts.repos = repos.clone();
    let mention_inferred = opts.mention.is_none();
    let mention = opts.mention.clone().unwrap_or_else(|| app.slug.clone());
    opts.mention = Some(mention.clone());
    let (label, label_inferred) = match opts.label.clone() {
        Some(label) => (label, false),
        None => match recorded_tracker_str(recorded, "label") {
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
    Ok((
        AppPlan {
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
        },
        publication,
    ))
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
        format!("# GET {api}/users/<app-slug>[bot] for its publication author identity"),
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
        if opts.all_installed_repos {
            "# allowlist: every repository the App is installed on (--all-installed-repos)".into()
        } else {
            format!("# allowlist: {} (--repo)", opts.repos.join(", "))
        },
    ]
}

struct RepositoryRunnerRequest {
    repo: String,
    file: PathBuf,
    spec: crate::repo_runner::RepoRunnerSpec,
}

struct RepositoryUpdates {
    runners: Vec<RepositoryRunnerRequest>,
    docker: BTreeSet<String>,
    targets: BTreeSet<String>,
}

type RepositoryIndices = BTreeMap<String, (usize, usize)>;

fn repository_updates_input(opts: &FactoryIntakeOpts) -> Result<RepositoryUpdates> {
    if opts.disable
        || opts.intake.is_some()
        || opts.label.is_some()
        || opts.mention.is_some()
        || opts.card_base_url.is_some()
        || opts.webhook_secret.is_some()
        || !opts.forge_egress.is_empty()
        || opts.org.is_some()
        || opts.runner_binding.is_some()
    {
        return Err(CliError::usage(
            "--repo-runner/--repo-docker cannot be combined with intake mutation or registration flags",
        )
        .into());
    }
    if !opts.repo_runners.is_empty()
        && opts.registry.as_deref().is_none_or(|registry| {
            !regex::Regex::new(r"^[A-Za-z0-9][A-Za-z0-9._/:-]*$")
                .expect("static registry syntax")
                .is_match(registry)
        })
    {
        return Err(CliError::usage("--repo-runner requires --registry REGISTRY").into());
    }
    if opts.repo_runners.is_empty() && opts.registry.is_some() {
        return Err(CliError::usage("--registry requires --repo-runner OWNER/REPO=SPEC").into());
    }
    // Keep the chart's repository-path grammar, including nested code-host paths.
    let grammar =
        regex::Regex::new(r"^[A-Za-z0-9_][A-Za-z0-9._~-]*(/[A-Za-z0-9_][A-Za-z0-9._~-]*)+$")
            .expect("static chart repository-path grammar");
    let mut runners = Vec::new();
    let mut targets = BTreeSet::new();
    for raw in &opts.repo_runners {
        let (repo, file) = raw
            .split_once('=')
            .filter(|(repo, file)| grammar.is_match(repo) && !file.trim().is_empty())
            .ok_or_else(|| CliError::usage("--repo-runner expects OWNER/REPO=SPEC, an exact repository path and a nonempty input file"))?;
        if !targets.insert(repo.to_string()) {
            return Err(CliError::usage(format!(
                "--repo-runner repository {repo} is listed more than once"
            ))
            .into());
        }
        let file = PathBuf::from(file);
        let loaded = crate::repo_runner::load_spec(&file)
            .map_err(|error| CliError::usage(format!("{repo}: {error:#}")))?;
        runners.push(RepositoryRunnerRequest {
            repo: repo.to_string(),
            file,
            spec: loaded.spec,
        });
    }
    let mut docker = BTreeSet::new();
    for repo in &opts.repo_docker {
        if !grammar.is_match(repo) {
            return Err(CliError::usage(
                "--repo-docker expects OWNER/REPO, an exact repository path without wildcards",
            )
            .into());
        }
        if !docker.insert(repo.clone()) {
            return Err(CliError::usage(format!(
                "--repo-docker repository {repo} is listed more than once"
            ))
            .into());
        }
        targets.insert(repo.clone());
    }
    Ok(RepositoryUpdates {
        runners,
        docker,
        targets,
    })
}

fn repository_update_bindings(
    recorded: &serde_json::Value,
    updates: &RepositoryUpdates,
) -> Result<(Vec<serde_json::Value>, RepositoryIndices)> {
    let bindings = recorded
        .pointer("/factory/bindings")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut indices = BTreeMap::new();
    for target in &updates.targets {
        let mut matches = Vec::new();
        for (bi, binding) in bindings.iter().enumerate() {
            if let Some(repos) = binding.get("repos").and_then(serde_json::Value::as_array) {
                for (ri, repo) in repos.iter().enumerate() {
                    if repo.get("path").and_then(serde_json::Value::as_str) == Some(target.as_str())
                    {
                        matches.push((bi, ri));
                    }
                }
            }
        }
        if matches.len() != 1 {
            let reason = if matches.is_empty() {
                "has no recorded binding"
            } else {
                "is ambiguous: more than one recorded binding"
            };
            return Err(CliError::usage(format!(
                "repository {target} {reason}; nothing was built or applied"
            ))
            .into());
        }
        indices.insert(target.clone(), matches[0]);
    }
    Ok((bindings, indices))
}

fn repository_update_prerequisites(
    computed: &serde_json::Value,
    fullname: &crate::ops::ReleaseFullname,
    updates: &RepositoryUpdates,
    indices: &RepositoryIndices,
) -> Result<()> {
    let agents = computed
        .pointer("/factory/sandboxEgress")
        .and_then(serde_json::Value::as_object);
    if agents.is_none_or(|agents| {
        agents.values().any(|enabled| !enabled.is_boolean())
            || agents
                .values()
                .filter(|enabled| enabled.as_bool() == Some(true))
                .count()
                != 1
    }) {
        return Err(CliError::usage(
            "repository runners require exactly one true factory.sandboxEgress agent",
        )
        .into());
    }
    if computed
        .pointer("/agentSandbox/deploy")
        .and_then(serde_json::Value::as_bool)
        != Some(true)
    {
        return Err(CliError::usage(
            "repository runners require agentSandbox.deploy=true for chart-managed pools",
        )
        .into());
    }
    let pool = fullname.resource("runner-pool");
    if computed.pointer("/worker/warmPool").is_some_and(|value| {
        value
            .as_str()
            .is_none_or(|name| !name.is_empty() && name != pool)
    }) {
        return Err(CliError::usage(format!(
            "repository runners require worker.warmPool to be empty or {pool}"
        ))
        .into());
    }
    if computed
        .pointer("/worker/deploy")
        .and_then(serde_json::Value::as_bool)
        != Some(true)
    {
        return Err(CliError::usage("repository updates require worker.deploy=true").into());
    }
    let daemon_syntax =
        regex::Regex::new(r"^[^\s@]+@sha256:[0-9a-f]{64}$").expect("static daemon digest syntax");
    for (target, &(bi, ri)) in indices {
        let repo = &computed["factory"]["bindings"][bi]["repos"][ri];
        if !updates.docker.contains(target)
            && repo
                .pointer("/docker/enabled")
                .and_then(serde_json::Value::as_bool)
                != Some(true)
        {
            continue;
        }
        let has_cli = match updates
            .runners
            .iter()
            .find(|request| &request.repo == target)
        {
            Some(request) => request.spec.tools.iter().any(|tool| tool == "docker-cli"),
            None => repo
                .pointer("/runner/tools")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|tools| tools.iter().any(|tool| tool.as_str() == Some("docker-cli"))),
        };
        if !has_cli {
            return Err(CliError::usage(format!("repository {target} enables Docker and requires catalog docker-cli in its selected runner tools")).into());
        }
        let daemon = computed
            .pointer("/factory/docker/daemonImage")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        if !daemon_syntax.is_match(daemon) {
            return Err(CliError::usage(format!(
                "repository {target} requires a digest-pinned factory.docker.daemonImage"
            ))
            .into());
        }
    }
    Ok(())
}

fn advisory_repo_image(
    values: &serde_json::Value,
    target: &str,
) -> std::result::Result<Option<String>, String> {
    let mut images = Vec::new();
    if let Some(bindings) = values
        .pointer("/factory/bindings")
        .and_then(serde_json::Value::as_array)
    {
        for binding in bindings {
            if let Some(repos) = binding.get("repos").and_then(serde_json::Value::as_array) {
                for repo in repos {
                    if repo.get("path").and_then(serde_json::Value::as_str) == Some(target) {
                        if let Some(image) = repo
                            .pointer("/runner/image")
                            .and_then(serde_json::Value::as_str)
                            .filter(|image| !image.is_empty())
                        {
                            images.push(image.to_string());
                        }
                    }
                }
            }
        }
    }
    match images.len() {
        0 => Ok(None),
        1 => Ok(images.pop()),
        _ => Err("recorded repository runner selection is ambiguous".into()),
    }
}

async fn announce_inventory(
    reports: &BTreeMap<String, crate::factory_toolchain::Report>,
    values: &serde_json::Value,
    pending: Option<&RunnerImageBinding>,
    common: &CommonOpts,
) {
    let ui = crate::ui::ui();
    let agents: Vec<&str> = values
        .pointer("/factory/sandboxEgress")
        .and_then(serde_json::Value::as_object)
        .map(|agents| {
            agents
                .iter()
                .filter(|(_, enabled)| enabled.as_bool() == Some(true))
                .map(|(name, _)| name.as_str())
                .collect()
        })
        .unwrap_or_default();
    let agent_image = if agents.len() == 1 {
        pending
            .filter(|binding| binding.agent == agents[0])
            .map(|binding| binding.image.clone())
            .or_else(|| {
                values
                    .pointer("/agentSandbox/runnerImages")
                    .and_then(|images| images.get(agents[0]))
                    .and_then(serde_json::Value::as_str)
                    .filter(|image| !image.is_empty())
                    .map(str::to_string)
            })
    } else {
        None
    };
    let mut platform: Option<std::result::Result<String, String>> = None;
    for (repo, report) in reports {
        let image = match advisory_repo_image(values, repo) {
            Ok(Some(image)) => Ok(image),
            Err(reason) => Err(reason),
            Ok(None) => match &agent_image {
                Some(image) => Ok(image.clone()),
                None => {
                    if platform.is_none() {
                        platform = Some(
                            crate::cluster_secrets::installed_runner(common)
                                .await
                                .map(|(_, pin)| pin),
                        );
                    }
                    platform
                        .as_ref()
                        .expect("resolved serving platform runner")
                        .clone()
                }
            },
        };
        let manifest = match image {
            Ok(image) => match crate::factory_toolchain::read_tool_manifest(&image).await {
                Ok(Some(manifest)) => Some(manifest),
                Ok(None) => {
                    ui.warn(&format!("{repo}: tool inventory unknown: selected image has no readable tool manifest"));
                    None
                }
                Err(error) => {
                    ui.warn(&format!("{repo}: tool inventory unknown: {error}"));
                    None
                }
            },
            Err(reason) => {
                ui.warn(&format!("{repo}: tool inventory unknown: {reason}"));
                None
            }
        };
        for warning in report.warnings(manifest.as_ref()) {
            ui.warn(&format!("{repo}: {warning}"));
        }
    }
}

async fn update_repositories(opts: FactoryIntakeOpts) -> Result<Box<dyn crate::ui::CliOutput>> {
    let updates = repository_updates_input(&opts)?;
    let app_args = app_arguments(&opts)?;
    if opts.common.dry_run {
        let mut lines = vec![
            "# update only the complete recorded factory.bindings list; intake allowlist unchanged"
                .into(),
        ];
        if let Some((_, file)) = &app_args {
            lines.push(format!("# App-authenticated repository discovery from {} before Helm; compare selected image tool manifests after builds", file.display()));
        }
        lines.push("# read recorded bindings and computed pool/Docker prerequisites; resolve exact unique targets before any build".into());
        for request in &updates.runners {
            lines.push(format!("# build {} from {} with registry {}; base=<installed serving platform runner pin>, store complete runner receipt including image, base and platformDigests", request.repo, request.file.display(), opts.registry.as_deref().unwrap_or_default()));
        }
        for repo in &updates.docker {
            lines.push(format!(
                "# enable docker for {repo}, preserving its runner and Docker egress metadata"
            ));
        }
        lines.push(
            helm_upgrade(
                &opts.common,
                &opts.chart,
                Path::new("<private-values-file>"),
                effective_helm_timeout(opts.timeout_seconds, None),
            )
            .display(),
        );
        lines.extend(
            crate::comms::rollout_commands(
                true,
                &opts.common.namespace,
                &crate::ops::chart_fullname(&opts.common.release),
            )
            .iter()
            .map(OpsCommand::display),
        );
        return Ok(Box::new(FactoryIntakeOutput::DryRun(
            crate::ui::DryRunPlan { lines },
        )));
    }
    // Optional App discovery remains ahead of every Helm read, without Secret,
    // label, publication identity or intake mutations in repository mode.
    let reports = if let Some((id, file)) = &app_args {
        crate::factory_toolchain::preflight(id, file, &opts.repos, opts.all_installed_repos)
            .await?
            .reports
    } else {
        crate::ui::ui().note(crate::factory_toolchain::SKIPPED_NOTE);
        BTreeMap::new()
    };
    if !opts.repos.is_empty() || opts.all_installed_repos {
        crate::ui::ui()
            .note("repository scope is discovery only; the intake allowlist is unchanged");
    }
    require_on_path("helm")?;
    require_on_path("kubectl")?;
    let recorded = fetch_release_values(&opts.common)
        .await?
        .unwrap_or(serde_json::Value::Null);
    let (mut bindings, indices) = repository_update_bindings(&recorded, &updates)?;
    let computed = fetch_release_computed_values(&opts.common)
        .await?
        .ok_or_else(|| CliError::usage("repository updates require an existing Helm release"))?;
    let fullname = crate::ops::release_fullname(&opts.common.namespace, &opts.common.release).await;
    repository_update_prerequisites(&computed, &fullname, &updates, &indices)?;
    let base = if updates.runners.is_empty() {
        None
    } else {
        Some(
            crate::cluster_secrets::installed_runner(&opts.common)
                .await
                .map_err(CliError::failure)?
                .1,
        )
    };
    let mut receipts = Vec::new();
    for request in &updates.runners {
        let receipt = crate::commands::build_repo_runner(crate::commands::RepoRunnerBuildOpts {
            spec: request.file.clone(),
            registry: opts.registry.clone().expect("validated build registry"),
            runner_image: base.clone(),
        })
        .await?;
        receipts.push((&request.repo, receipt.runner));
    }
    // Collect every build receipt before preparing the single values upgrade.
    for (repo, runner) in receipts {
        let (bi, ri) = indices[repo];
        bindings[bi]["repos"][ri]["runner"] = serde_json::to_value(runner)?;
    }
    for repo in &updates.docker {
        let (bi, ri) = indices[repo];
        let entry = &mut bindings[bi]["repos"][ri];
        if entry.get("docker").is_none_or(serde_json::Value::is_null) {
            entry["docker"] = serde_json::json!({});
        }
        entry["docker"]["enabled"] = serde_json::json!(true);
    }
    let values = serde_json::json!({"factory": {"bindings": bindings}});
    let mut selected = computed.clone();
    selected["factory"]["bindings"] = values["factory"]["bindings"].clone();
    announce_inventory(&reports, &selected, None, &opts.common).await;
    let timeout = match opts.timeout_seconds {
        Some(explicit) => explicit,
        None => effective_helm_timeout(None, release_minimum_helm_timeout(&opts.common).await),
    };
    let file = crate::ops::SecretValuesFileGuard::private_document(&values)?;
    let command = helm_upgrade(&opts.common, &opts.chart, file.path(), timeout);
    crate::ui::ui().plumbing(&format!("+ {}", command.display()));
    let (ok, _, stderr) = run_capture(&command).await?;
    if !ok {
        return Err(CliError::failure(format!(
            "helm upgrade of repository bindings failed: {}",
            stderr.trim()
        ))
        .into());
    }
    // The comms Slack consumer table's disconnect subset is worker and API;
    // both consume repository runner configuration and must restart and wait.
    for command in crate::comms::rollout_commands(true, &opts.common.namespace, &fullname) {
        crate::ui::ui().plumbing(&format!("+ {}", command.display()));
        let component = command
            .argv()
            .into_iter()
            .find(|arg| arg.starts_with("deployment/"))
            .unwrap_or_else(|| "deployment".into());
        let (ok, _, stderr) = run_capture(&command).await.map_err(|error| {
            CliError::failure(format!(
                "{component} rollout failed after repository bindings were updated: {error}"
            ))
        })?;
        if !ok {
            return Err(CliError::failure(format!(
                "{component} rollout failed after repository bindings were updated: {}",
                stderr.trim()
            ))
            .into());
        }
    }
    Ok(Box::new(FactoryIntakeOutput::RepositoriesUpdated {
        enabled: computed
            .pointer("/factory/enabled")
            .and_then(serde_json::Value::as_bool)
            == Some(true),
        repos: updates.targets.into_iter().collect(),
    }))
}

fn app_arguments(opts: &FactoryIntakeOpts) -> Result<Option<(String, PathBuf)>> {
    let args = match (opts.app_id.clone(), opts.private_key_file.clone()) {
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
    if !opts.disable && args.is_some() && opts.repos.is_empty() && !opts.all_installed_repos {
        return Err(crate::factory_app::repository_scope_error());
    }
    if opts.disable && args.is_some() {
        return Err(CliError::usage(
            "--disable changes nothing else; drop --app-id/--private-key-file",
        )
        .into());
    }
    Ok(args)
}

pub async fn factory_intake(mut opts: FactoryIntakeOpts) -> Result<Box<dyn crate::ui::CliOutput>> {
    if !opts.repo_runners.is_empty() || !opts.repo_docker.is_empty() || opts.registry.is_some() {
        return update_repositories(opts).await;
    }
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
    let app_args = app_arguments(&opts)?;
    if opts.common.dry_run {
        let placeholder = Path::new("<private-values-file>");
        let mut lines = Vec::new();
        if let Some((_, file)) = &app_args {
            lines.extend(app_dry_run_lines(&opts, file));
            lines.insert(0, crate::factory_toolchain::PLAN_NOTE.into());
        } else if !opts.disable {
            lines.push(crate::factory_toolchain::SKIPPED_NOTE.into());
            if let Some(slug) = &opts.mention {
                let api = crate::github_app::github_api_url(crate::github_app::DEFAULT_CLONE_BASE);
                lines.push(format!(
                    "# GET {api}/users/{slug}[bot] for its publication author identity"
                ));
            }
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
    let mut publication = None;
    let mut reports = BTreeMap::new();
    // A disable writes no factory entries, so it reads nothing recorded.
    let mut recorded = serde_json::Value::Null;
    if !opts.disable {
        // Repository auth and inference are a preflight, before any Helm read
        // or context-dependent setup. Reuse its tokens for the setup plan.
        let mut preflight = if let Some((id, file)) = &app_args {
            Some(
                crate::factory_toolchain::preflight(
                    id,
                    file,
                    &opts.repos,
                    opts.all_installed_repos,
                )
                .await?,
            )
        } else {
            crate::ui::ui().note(crate::factory_toolchain::SKIPPED_NOTE);
            None
        };
        if let Some(preflight) = &mut preflight {
            reports = std::mem::take(&mut preflight.reports);
        }
        if let (None, Some(slug)) = (&preflight, &opts.mention) {
            publication = Some(
                crate::factory_app::GithubApi::new()?
                    .bot_publication_identity(slug)
                    .await?,
            );
        }
        recorded = fetch_release_values(&opts.common)
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
            Some((id, _)) => {
                let (plan, identity) = plan_app(
                    &mut opts,
                    &recorded,
                    id,
                    preflight.expect("App arguments have a completed preflight"),
                )
                .await?;
                app_plan = Some(plan);
                publication = Some(identity);
            }
        }
        // Helm get values returns user values, without the chart's defaults.
        // Either present, non-null operator field owns the whole identity,
        // including an explicit empty or numeric value.
        if ["gitUserName", "gitUserEmail"].iter().any(|key| {
            recorded
                .pointer(&format!("/worker/publication/{key}"))
                .is_some_and(|value| !value.is_null())
        }) {
            publication = None;
        }
        let planned = planned_values(&opts, &recorded, app_plan.as_ref());
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
    let mut values = planned_values(&opts, &recorded, app_plan.as_ref());
    if !reports.is_empty() {
        match fetch_release_computed_values(&opts.common).await {
            Ok(Some(mut computed)) => {
                if let Some(changes) = values
                    .pointer("/factory/sandboxEgress")
                    .and_then(serde_json::Value::as_object)
                {
                    let mut agents = computed
                        .pointer("/factory/sandboxEgress")
                        .and_then(serde_json::Value::as_object)
                        .cloned()
                        .unwrap_or_default();
                    agents.extend(changes.clone());
                    computed["factory"]["sandboxEgress"] = serde_json::Value::Object(agents);
                }
                announce_inventory(
                    &reports,
                    &computed,
                    opts.runner_binding.as_ref(),
                    &opts.common,
                )
                .await;
            }
            _ => {
                for (repo, report) in &reports {
                    crate::ui::ui().warn(&format!(
                        "{repo}: tool inventory unknown: computed release values are unavailable"
                    ));
                    for warning in report.warnings(None) {
                        crate::ui::ui().warn(&format!("{repo}: {warning}"));
                    }
                }
            }
        }
    }
    if let Some(identity) = &publication {
        values["worker"] = serde_json::json!({
            "publication": {
                "gitUserName": identity.name,
                "gitUserEmail": identity.email,
            }
        });
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
        let cmd = helm_upgrade(&opts.common, &opts.chart, file.path(), timeout);
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
            all_installed_repos: false,
            label: Some("factory".into()),
            mention: Some("curie-app".into()),
            card_base_url: Some("https://cards.example.trycloudflare.com".into()),
            webhook_secret: Some(SECRET.into()),
            forge_egress: vec!["dark-factory".into()],
            disable: false,
            timeout_seconds: None,
            app_id: None,
            private_key_file: None,
            org: None,
            intake: None,
            runner_binding: None,
            repo_runners: Vec::new(),
            registry: None,
            repo_docker: Vec::new(),
        }
    }

    fn argv(cmd: &OpsCommand) -> Vec<String> {
        cmd.argv()
    }

    // C2
    #[test]
    fn intake_values_carry_the_factory_settings_and_egress() {
        let values = intake_values(&opts());
        let factory = &values["factory"];
        assert_eq!(factory["enabled"], serde_json::json!(true));
        let tracker = github_entry(&values, "trackers");
        assert_eq!(tracker["label"], serde_json::json!("factory"));
        assert_eq!(tracker["mention"], serde_json::json!("curie-app"));
        assert_eq!(
            factory["cardBaseUrl"],
            serde_json::json!("https://cards.example.trycloudflare.com")
        );
        let api = &values["api"];
        assert_eq!(
            api["githubRepoAllowlist"],
            serde_json::json!(["acme/widgets", "acme/gadgets"])
        );
        assert_eq!(api["githubWebhookSecret"], serde_json::json!(SECRET));
        // #4397 AC 6: one egress source. The agent is enrolled for forge
        // egress; the GitHub entries carry none, so the chart's built-in set
        // applies. No CIDR list is written anywhere.
        assert_eq!(
            factory["sandboxEgress"],
            serde_json::json!({"dark-factory": true}),
            "{values}"
        );
        for section in ["codeHosts", "trackers"] {
            assert!(
                github_entry(&values, section).get("egress").is_none(),
                "a github {section} entry must carry no egress (built-in set): {values}"
            );
        }
        assert!(
            values.pointer("/agentSandbox/connectorEgress").is_none(),
            "forge egress no longer rides connectorEgress: {values}"
        );
        assert!(!values.to_string().contains("\"cidr\""), "{values}");
    }

    #[test]
    fn intake_writes_sandbox_egress_for_each_named_agent_only() {
        let mut o = opts();
        o.forge_egress = vec!["dark-factory".into(), "dark-factory-fast".into()];
        let values = intake_values(&o);
        assert_eq!(
            values["factory"]["sandboxEgress"],
            serde_json::json!({"dark-factory": true, "dark-factory-fast": true}),
            "{values}"
        );
        o.forge_egress = Vec::new();
        let values = intake_values(&o);
        assert!(
            values.pointer("/factory/sandboxEgress").is_none(),
            "no --forge-egress writes no sandboxEgress, so a reused release keeps its own: {values}"
        );
    }

    #[tokio::test]
    async fn intake_writes_sandbox_egress_for_the_named_agent_and_fetches_nothing() {
        // The dry run is the whole plan; with --forge-egress it names no
        // GitHub /meta read, because egress is no longer fetched.
        let meta = ["/", "meta"].concat();
        let output = factory_intake(opts())
            .await
            .expect("a dry run with --forge-egress plans the upgrade");
        let plan = output.to_json();
        let lines: Vec<String> = plan["plan"]
            .as_array()
            .unwrap_or_else(|| panic!("dry run plan: {plan}"))
            .iter()
            .map(|line| line.as_str().unwrap_or_default().to_string())
            .collect();
        assert!(
            lines.iter().any(|line| line.contains("helm upgrade")),
            "{lines:?}"
        );
        assert!(
            !lines.iter().any(|line| line.contains(&meta)),
            "the dry run still plans a GitHub {meta} fetch: {lines:?}"
        );
        let production = include_str!("factory_intake.rs")
            .split("#[cfg(test)]")
            .next()
            .unwrap_or_default();
        assert!(
            !production.contains(&meta),
            "factory_intake.rs still builds a GitHub {meta} request"
        );
        assert!(!production.contains("connectorEgress"));
    }

    /// The one `factory.<section>` entry named `github`.
    fn github_entry<'a>(values: &'a serde_json::Value, section: &str) -> &'a serde_json::Value {
        let list = values["factory"][section]
            .as_array()
            .unwrap_or_else(|| panic!("factory.{section} is not a list: {values}"));
        let entries: Vec<&serde_json::Value> = list
            .iter()
            .filter(|entry| entry["name"] == serde_json::json!("github"))
            .collect();
        assert_eq!(entries.len(), 1, "factory.{section}: {values}");
        entries[0]
    }

    /// The chart's removed factory keys, read from the one file that names
    /// them so this test spells none of them.
    fn legacy_factory_keys() -> Vec<String> {
        serde_norway::from_str(include_str!(
            "../../charts/curie/files/legacy-factory-keys.yaml"
        ))
        .expect("legacy-factory-keys.yaml is a list of dotted chart keys")
    }

    #[test]
    fn intake_values_write_the_factory_block_and_null_the_legacy_keys() {
        let mut o = opts();
        o.intake = Some("poll".into());
        o.app_id = Some("123".into());
        let values = intake_values(&o);
        let factory = &values["factory"];
        assert_eq!(factory["enabled"], serde_json::json!(true), "{values}");
        assert_eq!(factory["intake"], serde_json::json!("poll"), "{values}");

        let api_url = crate::github_app::github_api_url(crate::github_app::DEFAULT_CLONE_BASE);
        let host = if api_url == crate::github_app::DEFAULT_GITHUB_API_URL {
            "github.com".to_string()
        } else {
            reqwest::Url::parse(&api_url)
                .expect("GitHub API URL parses")
                .host_str()
                .expect("GitHub API URL has a host")
                .to_ascii_lowercase()
        };
        let credential = serde_json::json!({
            "type": "github_app",
            "appId": "123",
            "existingSecret": crate::factory_app::DEFAULT_SECRET_NAME,
            "existingSecretKey": crate::github_app::DEFAULT_APP_KEY_DATA_KEY,
        });
        assert_eq!(
            factory["codeHosts"].as_array().map(Vec::len),
            Some(1),
            "{values}"
        );
        assert_eq!(
            factory["trackers"].as_array().map(Vec::len),
            Some(1),
            "{values}"
        );
        for section in ["codeHosts", "trackers"] {
            let entry = github_entry(&values, section);
            assert_eq!(entry["kind"], serde_json::json!("github"), "{section}");
            assert_eq!(entry["host"], serde_json::json!(host), "{section}");
            assert_eq!(entry["apiUrl"], serde_json::json!(api_url), "{section}");
            assert_eq!(entry["credential"], credential, "{section}");
        }
        let tracker = github_entry(&values, "trackers");
        assert_eq!(tracker["label"], serde_json::json!("factory"));
        assert_eq!(tracker["mention"], serde_json::json!("curie-app"));

        let legacy = legacy_factory_keys();
        assert!(!legacy.is_empty(), "the guard file lists the removed keys");
        for key in &legacy {
            assert!(key.starts_with("api."), "{key} is not an api key");
            let pointer = format!("/{}", key.replace('.', "/"));
            assert_eq!(
                values.pointer(&pointer),
                Some(&serde_json::Value::Null),
                "{key} must be written as null so a reused release sheds it: {values}"
            );
        }
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
        let values = intake_values(&o);
        assert_eq!(
            values["agentSandbox"]["runnerImages"]["dark-factory"],
            serde_json::json!("ghcr.io/curie-eng/curie-dark-factory-runner@sha256:abcd")
        );
        assert_eq!(
            values["factory"]["sandboxEgress"]["dark-factory"],
            serde_json::json!(true),
            "the egress enrolment rides the same upgrade document: {values}"
        );
        assert!(values["agentSandbox"].get("connectorEgress").is_none());
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
        let values = intake_values(&o);
        assert_eq!(values["factory"]["enabled"], serde_json::json!(false));
        assert!(
            values["api"].get("githubWebhookSecret").is_none(),
            "disable must not write a secret: {values}"
        );
        assert!(!values.to_string().contains(SECRET), "{values}");
        assert!(
            values.pointer("/factory/sandboxEgress").is_none(),
            "--disable changes nothing else, so it writes no egress: {values}"
        );
    }

    const LABEL_KEY: &str = "factory.trackers[github].label";
    const MENTION_KEY: &str = "factory.trackers[github].mention";
    const CARD_KEY: &str = "factory.cardBaseUrl";

    fn tracker(label: &str, mention: &str) -> serde_json::Value {
        serde_json::json!({
            "name": "github", "kind": "github", "label": label, "mention": mention
        })
    }

    fn full() -> serde_json::Value {
        serde_json::json!({
            "api": {
                "githubAppId": "123", "githubAppPrivateKey": "k",
                "githubWebhookSecret": "s3cret", "githubRepoAllowlist": ["a/b"]},
            "factory": {"trackers": [tracker("curie-factory", "my-app")]}})
    }

    #[test]
    fn gate_complete_config_has_no_offenders() {
        assert!(intake_gate_offenders(&serde_json::Value::Null, &full()).is_empty());
    }

    #[test]
    fn gate_missing_mention_is_named() {
        let mut v = full();
        v["factory"]["trackers"][0]
            .as_object_mut()
            .unwrap()
            .remove("mention");
        assert_eq!(
            intake_gate_offenders(&v, &serde_json::json!({})),
            vec![MENTION_KEY]
        );
    }

    #[test]
    fn gate_merges_recorded_values_with_the_planned_factory_block() {
        let mut rec = full();
        rec.as_object_mut().unwrap().remove("factory");
        let planned =
            serde_json::json!({"factory": {"trackers": [tracker("curie-factory", "x-y")]}});
        assert!(intake_gate_offenders(&rec, &planned).is_empty());
        // `helm -f` replaces a list, so a planned tracker list without a
        // label drops the recorded one.
        let planned = serde_json::json!({"factory": {"trackers": [
            {"name": "github", "kind": "github", "mention": "x-y"}
        ]}});
        assert_eq!(intake_gate_offenders(&full(), &planned), vec![LABEL_KEY]);
    }

    #[test]
    fn gate_reads_the_recorded_factory_block() {
        let mut recorded = full();
        recorded["api"]["githubWebhookSecret"] = serde_json::json!("");
        recorded["api"]["environment"] = serde_json::json!("prod");
        recorded["factory"] = serde_json::json!({
            "intake": "webhook",
            "cardBaseUrl": "http://localhost:3000",
            "trackers": [tracker("has space", "@my-app")],
        });
        assert_eq!(
            intake_gate_offenders(&recorded, &serde_json::json!({})),
            vec!["GITHUB_WEBHOOK_SECRET", LABEL_KEY, MENTION_KEY, CARD_KEY]
        );
        // The planned factory block overlays the recorded one key by key.
        let planned = serde_json::json!({"factory": {
            "intake": "poll",
            "cardBaseUrl": "https://cards.example.com",
            "trackers": [tracker("curie-factory", "my-app")],
        }});
        assert!(intake_gate_offenders(&recorded, &planned).is_empty());
        // No recorded tracker at all names both tracker keys.
        let mut bare = full();
        bare.as_object_mut().unwrap().remove("factory");
        assert_eq!(
            intake_gate_offenders(&bare, &serde_json::json!({})),
            vec![LABEL_KEY, MENTION_KEY]
        );
    }

    #[test]
    fn offender_flags_map_the_factory_chart_keys_to_the_same_flags() {
        assert_eq!(
            offender_flag(LABEL_KEY),
            "--label (no whitespace, 50 chars max)"
        );
        assert_eq!(
            offender_flag(MENTION_KEY),
            "--mention <app-slug> (the App login, no '@')"
        );
        assert_eq!(
            offender_flag(CARD_KEY),
            "--card-base-url https://<host> (no query or fragment; http only for localhost on a dev install)"
        );
        assert_eq!(
            offender_flag("GITHUB_WEBHOOK_SECRET"),
            "--webhook-secret-file"
        );
        assert_eq!(offender_flag("GITHUB_REPO_ALLOWLIST"), "--repo owner/name");
    }

    #[test]
    fn gate_dev_default_secret_and_at_mention_are_offenders() {
        // Webhook intake refuses both blank and published signing keys.
        for secret in ["dev-webhook-secret", ""] {
            let planned = serde_json::json!({
                "api": {"githubWebhookSecret": secret},
                "factory": {
                    "intake": "webhook",
                    "trackers": [tracker("curie-factory", "@x")]
                }
            });
            assert_eq!(
                intake_gate_offenders(&full(), &planned),
                vec!["GITHUB_WEBHOOK_SECRET", MENTION_KEY],
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
                recorded["api"]["githubWebhookSecret"] = serde_json::json!(secret);
                if let Some(value) = intake {
                    recorded["factory"]["intake"] = serde_json::json!(value);
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
            recorded["api"]["githubWebhookSecret"] = serde_json::json!("dev-webhook-secret");
            if let Some(value) = intake {
                recorded["factory"]["intake"] = serde_json::json!(value);
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
                        recorded["factory"]["intake"] = serde_json::json!(value);
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
        let planned = serde_json::json!({"factory": {"cardBaseUrl": "https://[cards.example.com"}});
        assert_eq!(intake_gate_offenders(&full(), &planned), vec![CARD_KEY]);
    }

    #[test]
    fn gate_refuses_http_and_query_card_urls_and_accepts_https() {
        for bad in ["http://cards.example.com", "https://cards.example.com?q=1"] {
            let planned = serde_json::json!({"factory": {"cardBaseUrl": bad}});
            assert_eq!(
                intake_gate_offenders(&full(), &planned),
                vec![CARD_KEY],
                "{bad}"
            );
        }
        let ok = serde_json::json!({"factory": {"cardBaseUrl": "https://cards.example.com"}});
        assert!(intake_gate_offenders(&full(), &ok).is_empty());
    }

    #[test]
    fn gate_reads_the_recorded_environment_for_localhost_http() {
        let planned = serde_json::json!({"factory": {"cardBaseUrl": "http://localhost:3000"}});
        assert!(intake_gate_offenders(&full(), &planned).is_empty());
        let mut prod = full();
        prod["api"]["environment"] = serde_json::json!("prod");
        assert_eq!(intake_gate_offenders(&prod, &planned), vec![CARD_KEY]);
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

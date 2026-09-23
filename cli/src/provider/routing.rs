//! Provider routing for `curie apply` (ADR 0163 decisions 2, 3 and 5).
//!
//! With a declared provider, every provider-backed credential reaches its
//! consumers through an ESO-synced Secret and the chart's `existingSecret`
//! knobs. Helm values carry Secret NAMES only. This module plans that routing
//! from the inventory, checks Secrets Manager before anything is mutated,
//! generates the stateful internals of a fresh install once, and applies the
//! ExternalSecrets. Nothing here runs when `curie.yaml` declares no provider.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use anyhow::{Context, Result};
use base64::Engine as _;
use serde_json::{json, Value};

use super::eso::Kubectl;
use super::{ProviderError, SecretMaterial, SecretsProvider, Store};
use crate::exit::CliError;
use crate::installation::Installation;

/// Stored Helm revisions kept for a provider-backed release.
pub const HISTORY_MAX: u32 = 3;

/// The chart default for `langfuse.init.projectPublicKey`. The OTLP auth
/// header is derived from it and the generated project secret key.
pub const DEFAULT_LANGFUSE_PUBLIC_KEY: &str = "pk-lf-curie-dev";

/// ExternalSecret refresh interval for platform credentials.
pub const REFRESH_INTERVAL: &str = "1h";

/// The SecretStore the ExternalSecrets read through: `<release>-curie-sm`.
pub fn store_name(release: &str) -> String {
    format!("{release}-curie-sm")
}

/// Why an inventory entry is routed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteReason {
    /// A stateful internal the CLI generates once on a fresh install.
    Generated,
    /// A third-party credential `curie.yaml` names.
    Declared,
    /// Routed because Secrets Manager already holds it, or the live release
    /// records a value for it that switching must not drop.
    Optional,
}

/// One routed inventory entry, resolved for this release.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutedEntry {
    pub logical_name: String,
    /// `<prefix>/<release>/<logical_name>`, the Secrets Manager id and the
    /// ExternalSecret `remoteRef.key`.
    pub remote_id: String,
    /// The Secret ESO writes, with `{release}` resolved.
    pub target: String,
    pub keys: Vec<String>,
    pub reason: RouteReason,
}

/// What the planner reads. `sm` maps a logical name to the key names its
/// Secrets Manager object holds; `None` means Secrets Manager was not read
/// (an offline dry run).
pub struct RoutingInputs<'a> {
    pub cfg: &'a Installation,
    pub live: Option<&'a Value>,
    pub sm: Option<&'a BTreeMap<String, Vec<String>>>,
}

/// The routing decision for one apply.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutingPlan {
    pub release: String,
    pub namespace: String,
    /// `<prefix>/<release>`.
    pub remote_prefix: String,
    /// True when no live release exists.
    pub fresh: bool,
    pub entries: Vec<RoutedEntry>,
    /// `--set-string` pairs, in order: knob names and key names, plus the
    /// non-secret companions (`fakeModel=false`, cleared Slack token values).
    pub knob_sets: Vec<(String, String)>,
    /// `langfuse.init.projectPublicKey` as declared, or the chart default.
    pub langfuse_public_key: String,
}

/// The stateful internals generated once on a fresh install.
const GENERATED: &[&str] = &[
    "installation-id",
    "postgres-password",
    "valkey-password",
    "clickhouse-password",
    "object-store-secret-key",
    "langfuse-data-keys",
    "langfuse-nextauth-secret",
    "langfuse-init-project-secret-key",
    "langfuse-init-user-password",
    "otlp-auth-header",
    "github-webhook-secret",
    "sealing-private-key",
];

const DROPPED_VALUE_KEYS: &[&str] = &[
    "postgres.auth.password",
    "valkey.password",
    "clickhouse.auth.password",
    "rustfs.auth.rootPassword",
    "rustfs.auth.secretKey",
    "langfuse.salt",
    "langfuse.encryptionKey",
    "langfuse.nextauthSecret",
    "langfuse.init.projectSecretKey",
    "langfuse.init.userPassword",
    "api.githubWebhookSecret",
    "sealing.privateKey",
    "sealing.previousPrivateKey",
    "api.githubToken",
    "api.githubAppPrivateKey",
    "dispatcher.slack.appToken",
    "dispatcher.slack.botToken",
    "dispatcher.slack.signingSecret",
    "agentSandbox.runner.credentials",
    "otelCollector.otlpAuthHeader",
];

/// Mail credential leaves this change does not route. A non-empty value in
/// the live release or `set:` would be re-supplied through Helm.
const MAIL_CREDENTIAL_KEYS: &[&str] = &[
    "mailAdapter.agentmail.apiKey",
    "mailAdapter.channelToken",
    "mailAdapter.egressSecret",
    "worker.adapterCredentials",
];

const PROJECT_KEY_ENTRY: &str = "langfuse-init-project-secret-key";
const OTLP_ENTRY: &str = "otlp-auth-header";

/// Non-generated entries a live reference to their target keeps routed.
fn routable_by_reference(logical: &str) -> bool {
    matches!(
        logical,
        "runner-model-credentials"
            | "slack-app-token"
            | "slack-bot-token"
            | "slack-signing-secret"
            | "github-token"
            | "github-app-private-key"
            | "sealing-previous-private-key"
    )
}

/// Every assignment key in one Helm set expression, so a comma-joined value
/// cannot smuggle a second assignment past a key check.
fn assignment_keys(expression: &str) -> Vec<String> {
    let expressions = [expression.to_string()];
    crate::ops::operator_set_entries(&expressions)
        .into_iter()
        .map(|(key, _)| key.trim().to_string())
        .collect()
}

/// Chart value keys that must never carry a value with a provider declared.
pub fn dropped_value_keys() -> &'static [&'static str] {
    DROPPED_VALUE_KEYS
}

/// A dotted path into the live values, as a non-empty string or `true`.
fn live_leaf<'a>(live: Option<&'a Value>, path: &str) -> Option<&'a Value> {
    let mut node = live?;
    for part in path.split('.') {
        node = node.get(part)?;
    }
    Some(node)
}

fn live_non_empty(live: Option<&Value>, path: &str) -> bool {
    match live_leaf(live, path) {
        Some(Value::String(s)) => !s.is_empty(),
        Some(Value::Null) | None => false,
        Some(Value::Bool(b)) => *b,
        Some(Value::Object(m)) => !m.is_empty(),
        Some(Value::Array(a)) => !a.is_empty(),
        Some(Value::Number(_)) => true,
    }
}

fn refuse(message: impl Into<String>, fix: impl Into<String>) -> anyhow::Error {
    CliError::failure(message).with_fix(fix).into()
}

/// Plan the routing. Refuses (naming the key, never a value) a `set:` entry
/// that names a routed knob or a dropped value key, and a mail adapter that
/// is deployed or carries credentials, which this change does not route.
pub fn plan_routing(inputs: &RoutingInputs<'_>) -> Result<RoutingPlan> {
    let cfg = inputs.cfg;
    let live = inputs.live;
    let secrets = cfg
        .secrets
        .as_ref()
        .context("provider routing needs a declared secrets provider")?;
    let release = cfg.install.release.clone();
    let namespace = cfg.install.namespace.clone();
    let remote_prefix = format!("{}/{}", secrets.prefix, release);
    let slack_declared = cfg.comms.slack.is_some();

    // The mail adapter's credentials are a named follow-up; its retained
    // values path would re-supply them through Helm.
    let mail_deployed = cfg
        .set
        .get("mailAdapter.deploy")
        .is_some_and(|v| v.trim().eq_ignore_ascii_case("true"))
        || live_leaf(live, "mailAdapter.deploy").is_some_and(|v| v == &json!(true));
    if mail_deployed {
        return Err(refuse(
            "a declared secrets provider cannot yet route the mail adapter's credentials (mailAdapter.deploy is true)",
            "Disable mailAdapter.deploy for this release; provider routing for the mail adapter is a follow-up change.",
        ));
    }
    for key in MAIL_CREDENTIAL_KEYS {
        let set = cfg.set.get(*key).is_some_and(|v| !v.is_empty());
        if set || live_non_empty(live, key) {
            return Err(refuse(
                format!(
                    "a declared secrets provider cannot yet route mailAdapter credentials; the release carries a value for {key}"
                ),
                "Clear the mail adapter credential values from the release; provider routing for the mail adapter is a follow-up change.",
            ));
        }
    }

    // An undeclared inline credential this change has no optional route for
    // would be dropped silently by the provider switch.
    let undeclared_inline: Vec<&str> = [
        (
            "agentSandbox.runner.credentials",
            cfg.credentials.model.is_none(),
        ),
        ("dispatcher.slack.appToken", !slack_declared),
        ("dispatcher.slack.botToken", !slack_declared),
        ("dispatcher.slack.signingSecret", !slack_declared),
    ]
    .into_iter()
    .filter(|(key, undeclared)| *undeclared && live_non_empty(live, key))
    .map(|(key, _)| key)
    .collect();
    if !undeclared_inline.is_empty() {
        return Err(refuse(
            format!(
                "the live release records a value for {} that curie.yaml does not declare; switching to the provider would drop it",
                undeclared_inline.join(", ")
            ),
            "Declare the credential in curie.yaml (credentials.model or comms.slack) and store it in Secrets Manager first.",
        ));
    }

    let sm_has = |logical: &str| inputs.sm.is_some_and(|sm| sm.contains_key(logical));
    let inventory = super::platform_inventory()?;
    let mut entries = Vec::new();
    let mut knob_paths: BTreeSet<String> = BTreeSet::new();
    for row in inventory.iter().filter(|row| row.store == Store::Sm) {
        let logical = row.logical_name.as_str();
        let reason = if GENERATED.contains(&logical) {
            Some(RouteReason::Generated)
        } else {
            match logical {
                "runner-model-credentials" => cfg
                    .credentials
                    .model
                    .is_some()
                    .then_some(RouteReason::Declared),
                "slack-app-token" | "slack-bot-token" => {
                    slack_declared.then_some(RouteReason::Declared)
                }
                "github-token" if cfg.credentials.github_token.is_some() => {
                    Some(RouteReason::Declared)
                }
                "github-token" => (sm_has(logical)
                    || live_non_empty(live, "api.githubToken")
                    || live_non_empty(live, "api.githubTokenExistingSecret"))
                .then_some(RouteReason::Optional),
                "slack-signing-secret" => (slack_declared
                    && (sm_has(logical)
                        || live_non_empty(live, "dispatcher.slack.signingSecret")
                        || live_non_empty(live, "dispatcher.slack.signingSecretExistingSecret")))
                .then_some(RouteReason::Optional),
                "sealing-previous-private-key" => (sm_has(logical)
                    || live_non_empty(live, "sealing.previousPrivateKey")
                    || live_non_empty(live, "sealing.previousPrivateKeyExistingSecret"))
                .then_some(RouteReason::Optional),
                "github-app-private-key" => (sm_has(logical)
                    || live_non_empty(live, "api.githubAppPrivateKey")
                    || live_non_empty(live, "api.githubAppExistingSecret"))
                .then_some(RouteReason::Optional),
                _ => None,
            }
        };
        // A live reference to this release's own synced Secret (left by an
        // earlier provider apply) keeps the entry routed after the file stops
        // declaring it, so preflight still requires its Secrets Manager source.
        let target = row.target.replace("{release}", &release);
        let referenced = row.chart.as_ref().is_some_and(|chart| {
            chart.knobs.iter().any(|knob| {
                live_leaf(live, &knob.secret).and_then(Value::as_str) == Some(target.as_str())
            })
        });
        let reason = reason
            .or((referenced && routable_by_reference(logical)).then_some(RouteReason::Optional));
        let Some(reason) = reason else { continue };
        let chart = row
            .chart
            .as_ref()
            .with_context(|| format!("inventory entry {logical} carries no chart knob"))?;
        for knob in &chart.knobs {
            knob_paths.insert(knob.secret.clone());
            if let Some(key) = &knob.key {
                knob_paths.insert(key.clone());
            }
        }
        entries.push(RoutedEntry {
            logical_name: row.logical_name.clone(),
            remote_id: format!("{remote_prefix}/{logical}"),
            target: row.target.replace("{release}", &release),
            keys: row.keys.clone(),
            reason,
        });
    }

    let mut conflicts: Vec<String> = Vec::new();
    for (key, value) in &cfg.set {
        for assigned in assignment_keys(&format!("{key}={value}")) {
            if (knob_paths.contains(&assigned) || DROPPED_VALUE_KEYS.contains(&assigned.as_str()))
                && !conflicts.contains(&assigned)
            {
                conflicts.push(assigned);
            }
        }
    }
    if !conflicts.is_empty() {
        return Err(refuse(
            format!(
                "curie.yaml `set:` names {} which a declared secrets provider owns",
                conflicts.join(", ")
            ),
            "Remove these keys from `set:`; with a provider the credential lives in Secrets Manager and reaches the chart by Secret name.",
        ));
    }

    let mut knob_sets: Vec<(String, String)> = Vec::new();
    let mut push = |key: &str, value: &str| {
        let pair = (key.to_string(), value.to_string());
        if !knob_sets.contains(&pair) {
            knob_sets.push(pair);
        }
    };
    for entry in &entries {
        let row = inventory
            .iter()
            .find(|row| row.logical_name == entry.logical_name)
            .expect("routed entries come from the inventory");
        for knob in &row.chart.as_ref().expect("checked above").knobs {
            push(&knob.secret, &entry.target);
            if let Some(key) = &knob.key {
                push(key, &entry.keys[0]);
            }
        }
    }
    let routed = |logical: &str| entries.iter().any(|e| e.logical_name == logical);
    if routed("runner-model-credentials") {
        push(crate::ops::FAKE_MODEL_KEY, "false");
    }
    if slack_declared {
        push("dispatcher.slack.appToken", "");
        push("dispatcher.slack.botToken", "");
        push("worker.slackApiBaseUrl", "");
    }

    let langfuse_public_key = cfg
        .set
        .get("langfuse.init.projectPublicKey")
        .filter(|v| !v.is_empty())
        .cloned()
        .unwrap_or_else(|| DEFAULT_LANGFUSE_PUBLIC_KEY.to_string());

    Ok(RoutingPlan {
        release,
        namespace,
        remote_prefix,
        fresh: live.is_none(),
        entries,
        knob_sets,
        langfuse_public_key,
    })
}

/// Rewrite a completed `up` so it passes Secret names instead of values:
/// drop every [`dropped_value_keys`] entry (from `secrets`, `set` and
/// `set_string`), clear the model credential and GitHub token plans, drop
/// retained non-secret overlays of routed knobs, append `knob_sets`, and bound
/// history to [`HISTORY_MAX`].
pub fn route_up_opts(up: &mut crate::ops::UpOpts, plan: &RoutingPlan) {
    let owned: BTreeSet<&str> = DROPPED_VALUE_KEYS
        .iter()
        .copied()
        .chain(plan.knob_sets.iter().map(|(key, _)| key.as_str()))
        .collect();
    up.secrets.retain(|(key, _)| !owned.contains(key.as_str()));
    let keeps = |e: &String| {
        !assignment_keys(e)
            .iter()
            .any(|key| owned.contains(key.as_str()))
    };
    up.set.retain(keeps);
    up.set_string.retain(keeps);
    up.credentials = None;
    up.github_token = crate::ops::GithubTokenPlan::Untouched;
    up.set_string.extend(
        plan.knob_sets
            .iter()
            .map(|(key, value)| format!("{key}={value}")),
    );
    up.history_max = Some(HISTORY_MAX);
}

/// Read-only Secrets Manager snapshot for this release: logical name to key
/// names. Never returns material.
pub fn read_sm_inventory(provider: &dyn SecretsProvider) -> Result<BTreeMap<String, Vec<String>>> {
    let listed = provider
        .list("")
        .context("could not list this release's Secrets Manager entries")?;
    Ok(listed
        .into_iter()
        .map(|meta| (meta.name, meta.key_names))
        .collect())
}

/// Check Secrets Manager before any mutation. Refuses when a routed entry
/// cannot be satisfied: a missing declared or optional entry, any missing
/// entry on an existing release, or an existing object missing a routed key.
/// The refusal names `<prefix>/<release>/<logical>` and key names only.
/// Returns the generated entries a fresh install must create.
pub fn preflight(plan: &RoutingPlan, sm: &BTreeMap<String, Vec<String>>) -> Result<Vec<String>> {
    let mut to_create = Vec::new();
    let mut problems = Vec::new();
    for entry in &plan.entries {
        match sm.get(&entry.logical_name) {
            None if entry.reason == RouteReason::Generated && plan.fresh => {
                to_create.push(entry.logical_name.clone());
            }
            None => problems.push(format!(
                "{} is missing (keys: {})",
                entry.remote_id,
                entry.keys.join(", ")
            )),
            Some(present) => {
                let missing: Vec<&str> = entry
                    .keys
                    .iter()
                    .filter(|key| !present.contains(key))
                    .map(String::as_str)
                    .collect();
                if !missing.is_empty() {
                    problems.push(format!(
                        "{} is missing key(s) {}",
                        entry.remote_id,
                        missing.join(", ")
                    ));
                }
            }
        }
    }
    if problems.is_empty() {
        return Ok(to_create);
    }
    let fix = if plan.fresh {
        "Store each missing entry with `curie secrets set --file <curie.yaml>`, then re-run apply. Nothing was changed."
    } else {
        "An existing release never generates credentials: seed each missing entry from the release with `curie secrets set --file <curie.yaml>`, then re-run apply. Nothing was changed."
    };
    Err(refuse(
        format!(
            "Secrets Manager cannot satisfy this apply: {}",
            problems.join("; ")
        ),
        fix,
    ))
}

fn generated_material(
    logical: &str,
    project_secret_key: Option<&str>,
    public_key: &str,
) -> Result<BTreeMap<&'static str, String>> {
    let hex = crate::ops::random_hex;
    let mut body = BTreeMap::new();
    match logical {
        "installation-id" => {
            body.insert("installationId", hex(16)?);
        }
        "postgres-password" => {
            body.insert("postgresPassword", hex(24)?);
        }
        "valkey-password" => {
            body.insert("valkeyPassword", hex(24)?);
        }
        "clickhouse-password" => {
            body.insert("clickhousePassword", hex(24)?);
        }
        "object-store-secret-key" => {
            body.insert("rustfsSecretKey", hex(24)?);
        }
        "langfuse-data-keys" => {
            body.insert("langfuseSalt", hex(16)?);
            body.insert("langfuseEncryptionKey", hex(32)?);
        }
        "langfuse-nextauth-secret" => {
            body.insert("langfuseNextauthSecret", hex(24)?);
        }
        PROJECT_KEY_ENTRY => {
            body.insert(
                "langfuseInitProjectSecretKey",
                format!("sk-lf-{}", hex(16)?),
            );
        }
        "langfuse-init-user-password" => {
            body.insert("langfuseInitUserPassword", hex(24)?);
        }
        OTLP_ENTRY => {
            let secret = project_secret_key
                .context("the OTLP auth header needs the Langfuse project secret key")?;
            let encoded =
                base64::engine::general_purpose::STANDARD.encode(format!("{public_key}:{secret}"));
            body.insert("otlpAuthHeader", format!("Basic {encoded}"));
        }
        "github-webhook-secret" => {
            body.insert("githubWebhookSecret", hex(24)?);
        }
        "sealing-private-key" => {
            body.insert(
                "sealingPrivateKey",
                crate::sealing::generate_keypair().private_key,
            );
        }
        other => anyhow::bail!("{other} is not a generated entry"),
    }
    Ok(body)
}

fn exists(provider: &dyn SecretsProvider, logical: &str) -> Result<bool> {
    match provider.get_metadata(logical) {
        Ok(_) => Ok(true),
        Err(ProviderError::NotFound { .. }) => Ok(false),
        Err(error) => Err(anyhow::Error::new(error)),
    }
}

/// Create each named generated entry once, as a JSON object of its keys. An
/// object that already exists is never overwritten. Returns the logical names
/// created.
pub fn generate(
    provider: &dyn SecretsProvider,
    plan: &RoutingPlan,
    to_create: &[String],
) -> Result<Vec<String>> {
    // The project key before the OTLP header derived from it.
    let mut ordered: Vec<&String> = to_create.iter().collect();
    ordered.sort_by_key(|name| name.as_str() == OTLP_ENTRY);
    let mut created = Vec::new();
    let mut project_secret_key: Option<String> = None;
    for logical in ordered {
        let remote_id = format!("{}/{logical}", plan.remote_prefix);
        if exists(provider, logical).with_context(|| format!("could not read {remote_id}"))? {
            if logical == PROJECT_KEY_ENTRY {
                project_secret_key = None;
            }
            continue;
        }
        if logical == OTLP_ENTRY && project_secret_key.is_none() {
            let stored = provider
                .get(PROJECT_KEY_ENTRY, None)
                .map_err(anyhow::Error::new)
                .with_context(|| {
                    format!(
                        "{remote_id} is derived from {}/{PROJECT_KEY_ENTRY}, which could not be read",
                        plan.remote_prefix
                    )
                })?;
            let body: Value = serde_json::from_str(stored.material.expose()).map_err(|_| {
                anyhow::anyhow!(
                    "{}/{PROJECT_KEY_ENTRY} is not a JSON object",
                    plan.remote_prefix
                )
            })?;
            project_secret_key = Some(
                body["langfuseInitProjectSecretKey"]
                    .as_str()
                    .filter(|v| !v.is_empty())
                    .with_context(|| {
                        format!(
                            "{}/{PROJECT_KEY_ENTRY} has no langfuseInitProjectSecretKey",
                            plan.remote_prefix
                        )
                    })?
                    .to_string(),
            );
        }
        let body = generated_material(
            logical,
            project_secret_key.as_deref(),
            &plan.langfuse_public_key,
        )?;
        let material = SecretMaterial::new(serde_json::to_string(&body)?);
        match provider.create(logical, &material) {
            Ok(_) => {
                if logical == PROJECT_KEY_ENTRY {
                    project_secret_key = body.get("langfuseInitProjectSecretKey").cloned();
                }
                created.push(logical.clone());
            }
            // Another apply created it first. Its value stands; the OTLP
            // header is then derived from the STORED project key.
            Err(ProviderError::Conflict { .. }) => {
                if logical == PROJECT_KEY_ENTRY {
                    project_secret_key = None;
                }
            }
            Err(error) => {
                return Err(anyhow::Error::new(error))
                    .with_context(|| format!("could not create {remote_id}"));
            }
        }
    }
    Ok(created)
}

/// One ExternalSecret per target Secret, `creationPolicy: Owner`, one
/// `remoteRef` per routed key.
pub fn render_external_secrets(plan: &RoutingPlan, store: &str) -> Vec<Value> {
    let mut targets: Vec<&str> = Vec::new();
    for entry in &plan.entries {
        if !targets.contains(&entry.target.as_str()) {
            targets.push(&entry.target);
        }
    }
    targets
        .into_iter()
        .map(|target| {
            let data: Vec<Value> = plan
                .entries
                .iter()
                .filter(|entry| entry.target == target)
                .flat_map(|entry| {
                    entry.keys.iter().map(|key| {
                        json!({
                            "secretKey": key,
                            "remoteRef": { "key": entry.remote_id, "property": key },
                        })
                    })
                })
                .collect();
            json!({
                "apiVersion": super::eso::EXTERNAL_SECRET_API,
                "kind": "ExternalSecret",
                "metadata": { "name": target, "namespace": plan.namespace },
                "spec": {
                    "refreshInterval": REFRESH_INTERVAL,
                    "secretStoreRef": { "name": store, "kind": "SecretStore" },
                    "target": { "name": target, "creationPolicy": "Owner" },
                    "data": data,
                },
            })
        })
        .collect()
}

fn args(parts: &[&str]) -> Vec<String> {
    parts.iter().map(|p| p.to_string()).collect()
}

/// Refuse, before any mutation, when the SecretStore is absent.
pub fn check_store(k: &dyn Kubectl, namespace: &str, store: &str) -> Result<()> {
    let out = k.run(
        &args(&[
            "-n",
            namespace,
            "get",
            "secretstore",
            store,
            "-o",
            "json",
            "--ignore-not-found",
        ]),
        None,
    )?;
    if !out.success {
        anyhow::bail!(
            "could not read SecretStore {namespace}/{store}: {}",
            out.stderr.trim()
        );
    }
    if out.stdout.trim().is_empty() {
        return Err(refuse(
            format!("SecretStore {store} does not exist in namespace {namespace}"),
            "Create the release's SecretStore (the ESO bootstrap) before applying with a secrets provider. Nothing was changed.",
        ));
    }
    Ok(())
}

fn owned_by_external_secret(secret: &Value, name: &str) -> bool {
    secret["metadata"]["ownerReferences"]
        .as_array()
        .is_some_and(|refs| {
            refs.iter().any(|r| {
                r["kind"] == json!("ExternalSecret")
                    && r["name"] == json!(name)
                    && r["apiVersion"]
                        .as_str()
                        .is_some_and(|v| v.starts_with("external-secrets.io/"))
            })
        })
}

/// Refuse a target Secret that exists but is not owned by the ExternalSecret
/// of the same name, then apply every ExternalSecret and wait for each sync.
pub fn sync(
    k: &dyn Kubectl,
    plan: &RoutingPlan,
    store: &str,
    timeout: Duration,
    poll: Duration,
) -> Result<()> {
    check_targets(k, plan)?;
    let rendered = render_external_secrets(plan, store);
    let names: Vec<String> = rendered
        .iter()
        .filter_map(|es| es["metadata"]["name"].as_str().map(str::to_string))
        .collect();
    super::eso::apply(k, &plan.namespace, &rendered)?;
    for name in &names {
        super::eso::force_sync_and_wait(k, &plan.namespace, name, timeout, poll)?;
    }
    Ok(())
}

/// Refuse, read only, a target Secret that exists but is not owned by the
/// ExternalSecret of the same name.
pub fn check_targets(k: &dyn Kubectl, plan: &RoutingPlan) -> Result<()> {
    let mut names: Vec<&str> = Vec::new();
    for entry in &plan.entries {
        if !names.contains(&entry.target.as_str()) {
            names.push(&entry.target);
        }
    }
    let namespace = plan.namespace.as_str();
    let mut foreign = Vec::new();
    for name in &names {
        let out = k.run(
            &args(&[
                "-n",
                namespace,
                "get",
                "secret",
                name,
                "-o",
                "json",
                "--ignore-not-found",
            ]),
            None,
        )?;
        if !out.success {
            anyhow::bail!(
                "could not read Secret {namespace}/{name}: {}",
                out.stderr.trim()
            );
        }
        if out.stdout.trim().is_empty() {
            continue;
        }
        let secret: Value = serde_json::from_str(&out.stdout)
            .with_context(|| format!("Secret {namespace}/{name} returned invalid JSON"))?;
        if !owned_by_external_secret(&secret, name) {
            foreign.push(*name);
        }
    }
    if !foreign.is_empty() {
        return Err(refuse(
            format!(
                "Secret(s) {} already exist in namespace {namespace} and are not owned by the ExternalSecret of the same name",
                foreign.join(", ")
            ),
            "Move the existing Secret aside (or delete it once its values are in Secrets Manager), then re-run apply.",
        ));
    }
    Ok(())
}

/// The lines an offline `apply --dry-run` prints for the routing.
pub fn dry_run_lines(plan: &RoutingPlan, store: &str) -> Vec<String> {
    let mut lines = vec![
        "# secrets provider: Secrets Manager is not read under --dry-run; entries it already holds for optional credentials are not shown"
            .to_string(),
    ];
    if plan.fresh {
        lines.push(format!(
            "# a fresh release generates each missing stateful entry once under {}/",
            plan.remote_prefix
        ));
    }
    // Secret and key names, never credentials, so they print unmasked.
    lines.extend(
        plan.knob_sets
            .iter()
            .map(|(key, value)| format!("set-string {key}={value}")),
    );
    for es in render_external_secrets(plan, store) {
        let name = es["metadata"]["name"].as_str().unwrap_or_default();
        let remotes: BTreeSet<&str> = es["spec"]["data"]
            .as_array()
            .map(|data| {
                data.iter()
                    .filter_map(|item| item["remoteRef"]["key"].as_str())
                    .collect()
            })
            .unwrap_or_default();
        lines.push(format!(
            "sync ExternalSecret {}/{name} from SecretStore {store}: {}",
            plan.namespace,
            remotes.into_iter().collect::<Vec<_>>().join(", ")
        ));
    }
    lines
}

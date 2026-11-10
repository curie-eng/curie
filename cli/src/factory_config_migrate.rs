use anyhow::{bail, Result};
use base64::Engine;
use serde_json::{json, Value};

use crate::config_migrate::{
    factory_allowlist, factory_api_origin, factory_credential, has_legacy_factory_input,
    legacy_factory_keys, normalized_factory_input, FactoryMigrationContext, LegacyFactory,
    ResolvedFactoryRepository,
};
use crate::ops::{plain, run_capture, CommonOpts, OpsCommand};

pub struct InstalledFactorySource {
    pub revision: u32,
    pub installed_chart: String,
    pub user_values: Value,
    pub computed_values: Value,
}

pub(crate) async fn read_installed_factory_source(
    common: &CommonOpts,
    revision: u32,
    installed_chart: String,
) -> Result<InstalledFactorySource> {
    let user_values = read_document(
        crate::cluster_secrets::helm_get_json(common, "values", false, revision),
        "installed factory user values",
    )
    .await?;
    let computed_values = read_document(
        crate::cluster_secrets::helm_get_json(common, "values", true, revision),
        "installed factory computed values",
    )
    .await?;
    Ok(InstalledFactorySource {
        revision,
        installed_chart,
        user_values,
        computed_values,
    })
}

async fn read_document(command: OpsCommand, setting: &str) -> Result<Value> {
    let result = tokio::time::timeout(std::time::Duration::from_secs(30), run_capture(&command))
        .await
        .map_err(|_| anyhow::anyhow!("{setting} could not be read"))?
        .map_err(|_| anyhow::anyhow!("{setting} could not be read"))?;
    if !result.0 {
        bail!("{setting} could not be read");
    }
    let value: Value = serde_json::from_str(&result.1)
        .map_err(|_| anyhow::anyhow!("{setting} returned malformed input"))?;
    if !value.is_object() && !value.is_null() {
        bail!("{setting} must be an object");
    }
    Ok(value)
}

async fn read_resource(
    common: &CommonOpts,
    kind: &str,
    name: &str,
    setting: &str,
) -> Result<Value> {
    read_document(
        OpsCommand::new(
            "kubectl",
            vec![
                plain("get"),
                plain(kind),
                plain(name),
                plain("-n"),
                plain(&common.namespace),
                plain("-o"),
                plain("json"),
            ],
        ),
        setting,
    )
    .await
}

fn project_effective(effective: &Value, supplied: &Value) -> Value {
    let mut projected = json!({"api": {}});
    for path in legacy_factory_keys().into_iter().chain(
        [
            "api.githubAppId",
            "api.githubAppExistingSecret",
            "api.githubAppExistingSecretKey",
            "api.githubRepoAllowlist",
            "api.githubApiUrl",
            "api.githubCloneBase",
            "api.environment",
        ]
        .into_iter()
        .map(str::to_string),
    ) {
        if let Some(value) = effective.pointer(&format!("/{}", path.replace('.', "/"))) {
            let key = path.strip_prefix("api.").expect("API setting");
            projected["api"][key] = value.clone();
        }
    }
    for section in ["codeHosts", "trackers"] {
        if let Some(entries) = effective
            .pointer(&format!("/factory/{section}"))
            .and_then(Value::as_array)
        {
            let entries: Vec<_> = entries
                .iter()
                .filter(|entry| entry["kind"] == "github")
                .map(|entry| {
                    let mut safe = json!({});
                    for key in ["name", "kind", "host", "apiUrl", "egress"] {
                        if let Some(value) = entry.get(key) {
                            safe[key] = value.clone();
                        }
                    }
                    safe
                })
                .collect();
            projected["factory"][section] = json!(entries);
        }
    }
    if let Some(value) = supplied
        .pointer("/worker/publication/githubHttpsCidrs")
        .filter(|v| !v.is_null())
    {
        projected["worker"]["publication"]["githubHttpsCidrs"] = value.clone();
    }
    if let Some(value) = supplied.pointer("/agentSandbox/connectorEgress") {
        projected["agentSandbox"]["connectorEgress"] = value.clone();
    }
    if let Some(reference) = effective.pointer("/codeHostTrust/caBundle/configMapRef") {
        let mut safe = json!({});
        for key in ["name", "key"] {
            if let Some(value) = reference.get(key) {
                safe[key] = value.clone();
            }
        }
        projected["codeHostTrust"]["caBundle"]["configMapRef"] = safe;
    }
    projected
}

pub async fn resolve_factory_migration_context(
    common: &CommonOpts,
    source: Option<&InstalledFactorySource>,
    effective_overlay: &Value,
) -> Result<FactoryMigrationContext> {
    if !has_legacy_factory_input(effective_overlay) {
        return Ok(FactoryMigrationContext::NotRequired);
    }
    let supplied = normalized_factory_input(effective_overlay)?;
    let mut effective = if let Some(source) = source {
        if source.revision == 0
            || source.installed_chart.is_empty()
            || (!source.user_values.is_object() && !source.user_values.is_null())
            || !source.computed_values.is_object()
        {
            bail!("installed factory source requires matching revision values");
        }
        source.computed_values.clone()
    } else {
        json!({})
    };
    crate::config_migrate::clear_replaced_secret_refs(&mut effective, &supplied);
    crate::ops::merge_values(&mut effective, supplied.clone());
    let effective_legacy = project_effective(&effective, &supplied);
    let legacy = LegacyFactory::parse(&effective_legacy)?;
    if source.is_none() && legacy.needs_identity() {
        for key in legacy_factory_keys().into_iter().chain(
            ["api.githubApiUrl", "api.githubAppExistingSecretKey"]
                .into_iter()
                .map(str::to_string),
        ) {
            if !supplied
                .pointer(&format!("/{}", key.replace('.', "/")))
                .is_some_and(|v| !v.is_null())
            {
                bail!("{key} requires an installed factory source or explicit configuration");
            }
        }
    }
    if !legacy.needs_identity() {
        return Ok(FactoryMigrationContext::LegacyGitHub {
            effective_legacy,
            repositories: Vec::new(),
        });
    }
    let allowlist = factory_allowlist(&effective)?;
    let credential = factory_credential(&effective)?;
    let (origin, _) = factory_api_origin(&effective)?;
    let certificate = if let Some(bundle) = effective
        .pointer("/codeHostTrust/caBundle")
        .filter(|v| !v.is_null())
    {
        let reference = bundle
            .get("configMapRef")
            .and_then(Value::as_object)
            .ok_or_else(|| anyhow::anyhow!("codeHostTrust.caBundle requires configMapRef"))?;
        let name = reference
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                anyhow::anyhow!("codeHostTrust.caBundle.configMapRef.name must be a string")
            })?;
        if name.is_empty() {
            None
        } else {
            let key = reference
                .get("key")
                .and_then(Value::as_str)
                .filter(|key| !key.is_empty())
                .ok_or_else(|| {
                    anyhow::anyhow!("codeHostTrust.caBundle.configMapRef.key is required")
                })?;
            let document =
                read_resource(common, "configmap", name, "codeHostTrust.caBundle").await?;
            let pem = document
                .get("data")
                .and_then(|data| data.get(key))
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    anyhow::anyhow!("codeHostTrust.caBundle has no referenced CA key")
                })?;
            let certificates =
                reqwest::Certificate::from_pem_bundle(pem.as_bytes()).map_err(|_| {
                    anyhow::anyhow!("codeHostTrust.caBundle must contain a PEM certificate")
                })?;
            if certificates.is_empty() {
                bail!("codeHostTrust.caBundle must contain a PEM certificate");
            }
            Some(reqwest::Certificate::from_pem(pem.as_bytes()).map_err(|_| {
                anyhow::anyhow!("codeHostTrust.caBundle must contain a PEM certificate")
            })?)
        }
    } else {
        None
    };
    let secret = read_resource(
        common,
        "secret",
        credential["existingSecret"]
            .as_str()
            .expect("credential reference"),
        "api.githubAppExistingSecret",
    )
    .await?;
    let encoded = secret
        .get("data")
        .and_then(|data| {
            data.get(
                credential["existingSecretKey"]
                    .as_str()
                    .expect("credential key"),
            )
        })
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("api.githubAppExistingSecretKey is unavailable"))?;
    let pem = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|_| {
            anyhow::anyhow!("api.githubAppExistingSecretKey cannot decode the App credential")
        })?;
    let pem = String::from_utf8(pem).map_err(|_| {
        anyhow::anyhow!("api.githubAppExistingSecretKey must contain a PEM credential")
    })?;
    let api = crate::factory_app::GithubApi::new(&origin, certificate).map_err(|_| {
        anyhow::anyhow!(
            "api.githubApiUrl and codeHostTrust.caBundle cannot construct the GitHub client"
        )
    })?;
    let app = tokio::time::timeout(
        std::time::Duration::from_secs(120),
        crate::factory_app::inspect_app(&api, credential["appId"].as_str().expect("App ID"), &pem),
    )
    .await
    .map_err(|_| {
        anyhow::anyhow!("api.githubAppId and api.githubAppExistingSecret authentication timed out")
    })?
    .map_err(|_| {
        anyhow::anyhow!("api.githubAppId and api.githubAppExistingSecret cannot authenticate")
    })?;
    let requested = crate::factory_app::resolve_allowlist(&allowlist, &app, false)
        .map_err(|_| anyhow::anyhow!("api.githubRepoAllowlist is outside the App installation"))?;
    let mut repositories = Vec::new();
    for repo in crate::factory_app::label_targets(&requested, &app.repos) {
        let id = api.repository_id(&app, &repo).await.map_err(|_| {
            anyhow::anyhow!("api.githubRepoAllowlist cannot resolve factory.bindings")
        })?;
        repositories.push(ResolvedFactoryRepository {
            path: repo,
            project_id: id.to_string(),
        });
    }
    Ok(FactoryMigrationContext::LegacyGitHub {
        effective_legacy,
        repositories,
    })
}

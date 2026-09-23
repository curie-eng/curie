//! Private local secret storage for Curie workflows.
//!
//! Secret values live in a mode-0600 file under the Curie config directory,
//! avoiding repeated platform credential-store authorization dialogs. `keyring`
//! remains only as a read-only migration path for older Curie installations.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{self, IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use time::format_description::well_known::Rfc3339;
use time::{Duration, OffsetDateTime};

use crate::provider::{
    aws::AwsSecretsProvider, ObjectMetadata, ProviderError, PutRequest, SecretMaterial,
    SecretsProvider, EXPIRY_TAG,
};

const SERVICE: &str = "ai.curietech.curie";
const VAULT_ACCOUNT: &str = "curie:global:vault";

#[derive(Clone, Debug)]
pub struct SetSecretOpts {
    pub name: String,
    pub from_env: Option<String>,
    pub cluster_identity: Option<String>,
    pub namespace: Option<String>,
    pub release: Option<String>,
    pub expected_version: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct UnsetSecretOpts {
    pub name: String,
    pub cluster_identity: Option<String>,
    pub namespace: Option<String>,
    pub release: Option<String>,
}

/// Resolve the provider declared by one parsed installation.
///
/// Construction performs the backend preflight. An installation without a
/// `secrets:` block never looks for the AWS CLI, preserving the local path.
pub fn provider_for_installation(
    installation: &crate::installation::Installation,
) -> Result<Option<AwsSecretsProvider>> {
    let Some(config) = installation.secrets.as_ref() else {
        return Ok(None);
    };
    match config.provider {
        crate::installation::ProviderKind::Aws => AwsSecretsProvider::new(
            &config.region,
            &config.prefix,
            &installation.install.release,
        )
        .map(Some),
    }
}

/// Load the installation selected by a standalone `curie secrets` command.
///
/// An explicit path always wins. Without one, a present `curie.yaml` in the
/// current directory is authoritative, including when it is malformed. A
/// genuinely absent file keeps the local secret store behavior.
fn discover_installation(file: Option<&Path>) -> Result<Option<crate::installation::Installation>> {
    if let Some(path) = file {
        return crate::installation::Installation::load(path).map(Some);
    }

    let path = Path::new("curie.yaml");
    match fs::symlink_metadata(path) {
        Ok(_) => crate::installation::Installation::load(path).map(Some),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).context("inspecting curie.yaml for a secrets provider"),
    }
}

fn discover_provider(file: Option<&Path>) -> Result<Option<AwsSecretsProvider>> {
    discover_installation(file)?
        .as_ref()
        .map(provider_for_installation)
        .transpose()
        .map(Option::flatten)
}

/// The installation in `./curie.yaml`, if that file exists.
pub fn cwd_installation() -> Result<Option<crate::installation::Installation>> {
    discover_installation(None)
}

/// The cwd installation when it declares a provider and names this release.
///
/// Absent file or absent `secrets:` is `Ok(None)`: callers keep today's
/// in-cluster behavior. A declared provider whose namespace or release
/// disagrees with the command is a usage error and writes nothing.
pub fn declared_scope(
    namespace: &str,
    release: &str,
) -> Result<Option<crate::installation::Installation>> {
    let Some(installation) = cwd_installation()? else {
        return Ok(None);
    };
    if installation.secrets.is_none() {
        return Ok(None);
    }
    if installation.install.namespace != namespace || installation.install.release != release {
        return Err(crate::exit::usage(
            "curie.yaml install.namespace and install.release must match --namespace and --release when a secrets provider is declared",
        ));
    }
    Ok(Some(installation))
}

/// Route `curie secrets set` through a declared provider or the existing local
/// store. Provider input names one logical object and one JSON key.
pub fn set_discovered(
    file: Option<&Path>,
    expires: Option<&str>,
    opts: SetSecretOpts,
) -> Result<()> {
    let installation = discover_installation(file)?;
    let Some(installation) = installation
        .as_ref()
        .filter(|config| config.secrets.is_some())
    else {
        if expires.is_some() {
            return Err(crate::exit::usage(
                "--expires requires a curie.yaml secrets provider",
            ));
        }
        return set(opts);
    };

    if opts.cluster_identity.is_some() || opts.namespace.is_some() || opts.release.is_some() {
        return Err(crate::exit::usage(
            "provider secrets are scoped by curie.yaml; do not pass --cluster-identity, --release, or --namespace",
        ));
    }
    if opts.expected_version.is_some() {
        return Err(crate::exit::usage(
            "--expected-version applies to the local cluster-scoped store, not provider secrets",
        ));
    }
    if let Some(raw) = expires {
        parse_expiry(raw)?;
    }
    let (logical, key) = provider_target(&opts.name)?;
    let provider = provider_for_installation(installation)?
        .context("declared secrets provider could not be constructed")?;
    set_provider(&provider, &logical, &key, expires, opts.from_env)
}

fn set_provider(
    provider: &dyn SecretsProvider,
    logical: &str,
    key: &str,
    expires: Option<&str>,
    from_env: Option<String>,
) -> Result<()> {
    let value = match from_env {
        Some(var) => std::env::var(&var)
            .with_context(|| format!("{var} is not set; cannot save {logical}/{key}"))?,
        None => prompt_secret(key)?,
    };
    if value.is_empty() {
        bail!("refusing to store an empty secret for {logical}/{key}");
    }

    let (mut values, expected_version) = match provider.get(logical, None) {
        Ok(stored) => {
            let values: BTreeMap<String, String> = serde_json::from_str(stored.material.expose())
                .with_context(|| {
                format!("provider object {logical} is not a JSON string map")
            })?;
            (values, Some(stored.version.id))
        }
        Err(ProviderError::NotFound { .. }) => (BTreeMap::new(), None),
        Err(error) => return Err(error.into()),
    };
    values.insert(key.to_string(), value);
    let material =
        SecretMaterial::new(serde_json::to_string(&values).context("serializing provider secret")?);
    let version = provider.put(&PutRequest {
        name: logical,
        material: &material,
        expected_version: expected_version.as_deref(),
    })?;
    if let Some(expires) = expires {
        let tags = BTreeMap::from([(EXPIRY_TAG.to_string(), expires.to_string())]);
        provider.tag(logical, &tags, Some(&version.id))?;
    }
    crate::ui::ui().success(&format!(
        "saved {logical}/{key} in the declared secrets provider"
    ));
    Ok(())
}

fn provider_target(raw: &str) -> Result<(String, String)> {
    let Some((logical, key)) = raw.split_once('/') else {
        return Err(crate::exit::usage(
            "provider secrets use logical/KEY, where logical is a Kubernetes Secret name and KEY is an environment-variable-style key",
        ));
    };
    if logical.is_empty() || key.is_empty() || key.contains('/') || !kubernetes_secret_name(logical)
    {
        return Err(crate::exit::usage(
            "provider secrets use logical/KEY, where logical is a Kubernetes Secret name and KEY is an environment-variable-style key",
        ));
    }
    validate_name(key)?;
    Ok((logical.to_string(), key.to_string()))
}

fn kubernetes_secret_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 253
        && name.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && label.chars().all(|character| {
                    character.is_ascii_lowercase() || character.is_ascii_digit() || character == '-'
                })
                && label.chars().next().is_some_and(|character| {
                    character.is_ascii_lowercase() || character.is_ascii_digit()
                })
                && label.chars().last().is_some_and(|character| {
                    character.is_ascii_lowercase() || character.is_ascii_digit()
                })
        })
}

/// Route `curie secrets list` through a declared provider or the local store.
pub fn list_discovered(file: Option<&Path>) -> Result<()> {
    let Some(provider) = discover_provider(file)? else {
        return list();
    };
    let metadata = provider.list("")?;
    let mut names: Vec<String> = metadata.iter().map(|item| item.name.clone()).collect();
    names.sort();
    names.dedup();
    let entries = names
        .iter()
        .map(|name| SecretListEntry {
            name: name.clone(),
            scope: None,
            version: None,
        })
        .collect();
    crate::ui::ui().emit(&SecretsListOutput { names, entries });
    Ok(())
}

/// Keep the legacy local delete verb from mutating a second store when the
/// current installation declares a provider.
pub fn unset_discovered(opts: UnsetSecretOpts) -> Result<()> {
    if discover_installation(None)?
        .as_ref()
        .is_some_and(|installation| installation.secrets.is_some())
    {
        return Err(crate::exit::usage(
            "curie secrets unset is local-only and is refused when curie.yaml declares a provider; use curie secrets rm <logical> to remove a provider object",
        ));
    }
    unset(opts)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProviderCheckEntry {
    name: String,
    version: String,
    expires_at: Option<String>,
    status: &'static str,
}

struct ProviderCheckOutput {
    entries: Vec<ProviderCheckEntry>,
}

impl crate::ui::CliOutput for ProviderCheckOutput {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "checked": self.entries.iter().map(|entry| serde_json::json!({
                "name": entry.name,
                "version": entry.version,
                "expires_at": entry.expires_at,
                "status": entry.status,
            })).collect::<Vec<_>>(),
        })
    }

    fn render(&self, ui: &crate::ui::Ui) {
        if self.entries.is_empty() {
            ui.note("no provider secrets found");
            return;
        }
        for entry in &self.entries {
            let detail = match &entry.expires_at {
                Some(expires_at) => format!("{} expires at {expires_at}", entry.name),
                None => format!("{} has no expiry", entry.name),
            };
            if entry.status == "warning" {
                ui.warn(&detail);
            } else if entry.status == "expired" {
                ui.warn(&format!("{detail} and is expired"));
            } else {
                ui.success(&detail);
            }
        }
    }
}

/// Check expiry metadata for one provider object or the whole installation.
pub fn check_discovered(file: Option<&Path>, name: Option<&str>) -> Result<()> {
    let provider = discover_provider(file)?.ok_or_else(|| {
        crate::exit::usage("curie secrets check requires a curie.yaml secrets provider")
    })?;
    let metadata = match name {
        Some(name) => vec![provider.get_metadata(name)?],
        None => provider.list("")?,
    };
    let output = provider_check_output(metadata, OffsetDateTime::now_utc())?;
    let expired: Vec<&str> = output
        .entries
        .iter()
        .filter(|entry| entry.status == "expired")
        .map(|entry| entry.name.as_str())
        .collect();
    if expired.is_empty() {
        crate::ui::ui().emit(&output);
        return Ok(());
    }
    Err(crate::ui::ui().failed_report(
        &output,
        crate::exit::CliError::failure(format!(
            "provider secret expiry check failed: {} expired",
            expired.join(", ")
        ))
        .with_fix("Rotate each expired secret and set a future --expires timestamp.")
        .into(),
    ))
}

fn provider_check_output(
    mut metadata: Vec<ObjectMetadata>,
    now: OffsetDateTime,
) -> Result<ProviderCheckOutput> {
    metadata.sort_by(|left, right| left.name.cmp(&right.name));
    let mut entries = Vec::with_capacity(metadata.len());
    for item in metadata {
        let expires_at = item.tags.get(EXPIRY_TAG).cloned();
        let status = match expires_at.as_deref() {
            None => "ok",
            Some(raw) => {
                let expiry = parse_expiry(raw)?;
                if expiry <= now {
                    "expired"
                } else if expiry - now <= Duration::days(30) {
                    "warning"
                } else {
                    "ok"
                }
            }
        };
        entries.push(ProviderCheckEntry {
            name: item.name,
            version: item.version.id,
            expires_at,
            status,
        });
    }
    Ok(ProviderCheckOutput { entries })
}

fn parse_expiry(raw: &str) -> Result<OffsetDateTime> {
    OffsetDateTime::parse(raw, &Rfc3339).map_err(|_| {
        crate::exit::usage(format!(
            "expiry timestamp {raw:?} must be RFC 3339, for example 2027-01-15T08:00:00Z"
        ))
    })
}

/// Delete one logical provider object.
pub fn remove_discovered(file: Option<&Path>, name: &str) -> Result<()> {
    if !kubernetes_secret_name(name) {
        return Err(crate::exit::usage(
            "provider secret name must be a Kubernetes Secret name",
        ));
    }
    let provider = discover_provider(file)?.ok_or_else(|| {
        crate::exit::usage("curie secrets rm requires a curie.yaml secrets provider")
    })?;
    provider.delete(name, None)?;
    crate::ui::ui().success(&format!(
        "removed {name} from the declared secrets provider"
    ));
    Ok(())
}

/// The cluster target a stored connector secret is allowed to be injected into.
///
/// Issue #1913: name-only storage lets `curie cluster deploy` reuse cluster A's
/// credential on cluster B. Identity is a fingerprint of the kube-apiserver URL
/// plus CA material, not a human cluster name.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SecretScope {
    pub cluster_identity: String,
    pub release: String,
    pub namespace: String,
}

impl SecretScope {
    pub fn describe(&self) -> String {
        format!(
            "cluster {} release {} namespace {}",
            self.cluster_identity, self.release, self.namespace
        )
    }
}

/// How a cluster-deploy secret was resolved. Never carries the value in
/// operator-facing descriptions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClusterSecretSource {
    Scoped { version: u64 },
    Environment,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedClusterSecret {
    pub value: String,
    pub source: ClusterSecretSource,
}

impl ResolvedClusterSecret {
    pub fn source_label(&self) -> String {
        match self.source {
            ClusterSecretSource::Scoped { version } => {
                format!("scoped stored secret (version {version})")
            }
            ClusterSecretSource::Environment => "process environment".to_string(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
struct SecretIndex {
    #[serde(default)]
    vault: bool,
    #[serde(default)]
    legacy_names: BTreeSet<String>,
    names: BTreeSet<String>,
    #[serde(default)]
    file_names: BTreeSet<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
struct ScopedSecret {
    cluster_identity: String,
    release: String,
    namespace: String,
    value: String,
    version: u64,
}

impl ScopedSecret {
    fn scope(&self) -> SecretScope {
        SecretScope {
            cluster_identity: self.cluster_identity.clone(),
            release: self.release.clone(),
            namespace: self.namespace.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
struct SecretVault {
    values: BTreeMap<String, String>,
    #[serde(default)]
    scoped: BTreeMap<String, Vec<ScopedSecret>>,
}

#[derive(Default)]
struct VaultCache {
    loaded: bool,
    vault: SecretVault,
}

static VAULT_CACHE: OnceLock<Mutex<VaultCache>> = OnceLock::new();

pub fn set(opts: SetSecretOpts) -> Result<()> {
    validate_name(&opts.name)?;
    let value = match opts.from_env {
        Some(var) => std::env::var(&var)
            .with_context(|| format!("{var} is not set; cannot save {}", opts.name))?,
        None => prompt_secret(&opts.name)?,
    };
    match optional_scope(
        opts.cluster_identity,
        opts.release,
        opts.namespace,
        "secrets set",
    )? {
        Some(scope) => {
            let version = save_scoped_value(&opts.name, &scope, &value, opts.expected_version)?;
            crate::ui::ui().success(&format!(
                "saved {} for {} (version {version})",
                opts.name,
                scope.describe()
            ));
        }
        None => {
            if opts.expected_version.is_some() {
                return Err(crate::exit::usage(
                    "--expected-version applies to cluster-scoped secrets; pass --cluster-identity, --release, and --namespace",
                ));
            }
            save_value(&opts.name, &value)?;
            crate::ui::ui().success(&format!("saved {} in Curie private storage", opts.name));
        }
    }
    Ok(())
}

pub fn list() -> Result<()> {
    crate::ui::ui().emit(&list_output()?);
    Ok(())
}

/// Output of `secrets list` (#474): the saved secret NAMEs plus cluster scope
/// metadata. Values are never emitted. Routes through the one `Ui::emit` point.
pub struct SecretsListOutput {
    pub names: Vec<String>,
    pub entries: Vec<SecretListEntry>,
}

/// One stored secret listing. `scope` is absent for install-global (skill/local)
/// values; cluster-scoped entries carry identity/release/namespace and version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecretListEntry {
    pub name: String,
    pub scope: Option<SecretScope>,
    pub version: Option<u64>,
}

impl crate::ui::CliOutput for SecretsListOutput {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "secrets": self.names,
            "entries": self.entries.iter().map(|entry| {
                match &entry.scope {
                    Some(scope) => serde_json::json!({
                        "name": entry.name,
                        "scope": {
                            "cluster_identity": scope.cluster_identity,
                            "release": scope.release,
                            "namespace": scope.namespace,
                        },
                        "version": entry.version,
                    }),
                    None => serde_json::json!({
                        "name": entry.name,
                        "scope": null,
                        "version": null,
                    }),
                }
            }).collect::<Vec<_>>(),
        })
    }

    fn render(&self, ui: &crate::ui::Ui) {
        if self.entries.is_empty() {
            ui.note("no Curie secrets saved");
            return;
        }
        let lines: Vec<String> = self
            .entries
            .iter()
            .map(|entry| match &entry.scope {
                Some(scope) => format!(
                    "{}  {}  version {}",
                    entry.name,
                    scope.describe(),
                    entry.version.unwrap_or(0)
                ),
                None => format!("{}  (unscoped)", entry.name),
            })
            .collect();
        ui.payload_plain(&lines.join("\n"));
    }
}

pub fn unset(opts: UnsetSecretOpts) -> Result<()> {
    match optional_scope(
        opts.cluster_identity,
        opts.release,
        opts.namespace,
        "secrets unset",
    )? {
        Some(scope) => {
            remove_scoped_value(&opts.name, &scope)?;
            crate::ui::ui().success(&format!("removed {} for {}", opts.name, scope.describe()));
        }
        None => {
            remove_value(&opts.name)?;
            crate::ui::ui().success(&format!("removed {}", opts.name));
        }
    }
    Ok(())
}

fn optional_scope(
    cluster_identity: Option<String>,
    release: Option<String>,
    namespace: Option<String>,
    verb: &str,
) -> Result<Option<SecretScope>> {
    match (cluster_identity, release, namespace) {
        (None, None, None) => Ok(None),
        (Some(cluster_identity), Some(release), Some(namespace)) => {
            if cluster_identity.is_empty() || release.is_empty() || namespace.is_empty() {
                return Err(crate::exit::usage(format!(
                    "{verb} cluster scope requires non-empty --cluster-identity, --release, and --namespace"
                )));
            }
            Ok(Some(SecretScope {
                cluster_identity,
                release,
                namespace,
            }))
        }
        _ => Err(crate::exit::usage(format!(
            "{verb} cluster scope requires --cluster-identity, --release, and --namespace together"
        ))),
    }
}

pub fn get_value(name: &str) -> Result<Option<String>> {
    validate_name(name)?;
    if let Some(value) = read_credentials()?.values.get(name).cloned() {
        return Ok(Some(value));
    }
    sync_secret_file()?;
    if let Some(value) = read_credentials()?.values.get(name).cloned() {
        return Ok(Some(value));
    }
    if needs_vault_upgrade(name)? {
        migrate_legacy_value(name)?;
    }
    Ok(read_credentials()?.values.get(name).cloned())
}

pub(crate) fn resolve_env_or_saved(name: &str) -> Result<Option<String>> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.is_empty())
        .map(Ok)
        .or_else(|| get_value(name).transpose())
        .transpose()
}

/// Check the non-secret index without opening any credential values.
/// UI status rendering must use this instead of `get_value`.
pub fn is_saved(name: &str) -> Result<bool> {
    validate_name(name)?;
    let index = read_index()?;
    Ok(index.file_names.contains(name)
        || index.names.contains(name)
        || index.legacy_names.contains(name))
}

/// Whether a name belongs to the older one-Keychain-item-per-secret layout.
/// This reads only the non-secret index and never opens Keychain.
pub fn needs_vault_upgrade(name: &str) -> Result<bool> {
    validate_name(name)?;
    let index = read_index()?;
    Ok(if index.vault {
        index.legacy_names.contains(name)
    } else {
        index.names.contains(name)
    })
}

/// Reconcile an older non-secret index with the consolidated vault. This opens
/// exactly one credential-store item and never reads legacy per-secret items.
pub fn sync_secret_file() -> Result<()> {
    let credentials = read_credentials()?;
    if !credentials.values.is_empty() {
        let index = reconcile_file_names(read_index()?, credentials.values.keys());
        return write_index(&index);
    }
    let index = read_index()?;
    if index.names.is_empty() {
        return Ok(());
    }
    let vault = read_vault()?;
    if vault.values.is_empty() {
        return Ok(());
    }
    write_credentials(&vault)?;
    let index = reconcile_file_names(index, vault.values.keys());
    write_index(&index)
}

/// Copy one required credential from the legacy per-secret Keychain layout to
/// the private file. The old Keychain item is deliberately left untouched.
pub fn migrate_legacy_value(name: &str) -> Result<bool> {
    if !needs_vault_upgrade(name)? {
        return Ok(false);
    }
    let value = match legacy_entry(name)?.get_password() {
        Ok(value) => value,
        Err(keyring::Error::NoEntry) => return Ok(false),
        Err(err) => {
            return Err(err)
                .with_context(|| format!("authorizing saved credential {name} for migration"));
        }
    };
    save_value(name, &value)?;
    Ok(true)
}

pub fn set_value(name: &str, value: &str) -> Result<()> {
    validate_name(name)?;
    let mut credentials = read_credentials()?;
    credentials
        .values
        .insert(name.to_string(), value.to_string());
    write_credentials(&credentials)
}

pub fn save_value(name: &str, value: &str) -> Result<()> {
    validate_name(name)?;
    if value.is_empty() {
        bail!("refusing to store an empty secret for {name}");
    }
    set_value(name, value)?;
    add_to_index(name)
}

pub fn save_scoped_value(
    name: &str,
    scope: &SecretScope,
    value: &str,
    expected_version: Option<u64>,
) -> Result<u64> {
    validate_name(name)?;
    if value.is_empty() {
        bail!("refusing to store an empty secret for {name}");
    }
    let mut credentials = read_credentials()?;
    let version = credentials.save_scoped(name, scope, value, expected_version)?;
    write_credentials(&credentials)?;
    add_to_index(name)?;
    Ok(version)
}

pub fn remove_scoped_value(name: &str, scope: &SecretScope) -> Result<()> {
    validate_name(name)?;
    let mut credentials = read_credentials()?;
    credentials.remove_scoped(name, scope);
    write_credentials(&credentials)?;
    if !credentials.has_name(name) {
        remove_from_index(name)?;
    }
    Ok(())
}

/// Resolve a connector secret for a cluster deploy target.
///
/// A matching scoped store entry wins. A stored entry for this name that does
/// not match the target (including an unscoped value) is a refusal, not a
/// fallback. Process environment is used only when the store has no entry for
/// the name at all, and the caller must surface that source.
pub fn resolve_cluster_secret(
    name: &str,
    target: &SecretScope,
) -> Result<Option<ResolvedClusterSecret>> {
    validate_name(name)?;
    let credentials = match config_dir() {
        Ok(_) => read_credentials()?,
        // A deploy with only process-environment values (CI, HOME unset) has no
        // host vault to consult. An absent config dir is an empty store, not a
        // failure to resolve the env fallback.
        Err(_) => SecretVault::default(),
    };
    match credentials.resolve_cluster(name, target)? {
        Some(resolved) => Ok(Some(resolved)),
        None => Ok(std::env::var(name)
            .ok()
            .filter(|value| !value.is_empty())
            .map(|value| ResolvedClusterSecret {
                value,
                source: ClusterSecretSource::Environment,
            })),
    }
}

pub fn list_output() -> Result<SecretsListOutput> {
    let credentials = read_credentials()?;
    Ok(credentials.list_output())
}

fn conflict(message: String, fix: &str) -> anyhow::Error {
    anyhow::Error::from(crate::exit::CliError::failure(message).with_fix(fix.to_string()))
}

impl SecretVault {
    fn remove_all(&mut self, name: &str) {
        self.values.remove(name);
        self.scoped.remove(name);
    }

    fn has_name(&self, name: &str) -> bool {
        self.values.contains_key(name)
            || self
                .scoped
                .get(name)
                .is_some_and(|entries| !entries.is_empty())
    }

    fn scoped_entries(&self, name: &str) -> &[ScopedSecret] {
        self.scoped.get(name).map(Vec::as_slice).unwrap_or(&[])
    }

    fn lookup_scoped(&self, name: &str, target: &SecretScope) -> Option<&ScopedSecret> {
        self.scoped_entries(name)
            .iter()
            .find(|entry| entry.scope() == *target)
    }

    fn other_scopes(&self, name: &str, target: &SecretScope) -> Vec<SecretScope> {
        self.scoped_entries(name)
            .iter()
            .map(ScopedSecret::scope)
            .filter(|scope| scope != target)
            .collect()
    }

    fn resolve_cluster(
        &self,
        name: &str,
        target: &SecretScope,
    ) -> Result<Option<ResolvedClusterSecret>> {
        if let Some(entry) = self.lookup_scoped(name, target) {
            return Ok(Some(ResolvedClusterSecret {
                value: entry.value.clone(),
                source: ClusterSecretSource::Scoped {
                    version: entry.version,
                },
            }));
        }
        let others = self.other_scopes(name, target);
        if !others.is_empty() {
            return Err(mismatch_error(name, target, &others));
        }
        if self.values.contains_key(name) {
            return Err(unscoped_error(name, target));
        }
        Ok(None)
    }

    fn save_scoped(
        &mut self,
        name: &str,
        scope: &SecretScope,
        value: &str,
        expected_version: Option<u64>,
    ) -> Result<u64> {
        let entries = self.scoped.entry(name.to_string()).or_default();
        let existing = entries.iter().position(|entry| entry.scope() == *scope);
        match existing {
            Some(index) => {
                let stored = entries[index].version;
                if expected_version != Some(stored) {
                    return Err(conflict(
                        format!(
                            "version mismatch: expected {}, stored {stored}",
                            expected_version
                                .map(|v| v.to_string())
                                .unwrap_or_else(|| "none".to_string())
                        ),
                        "re-run `curie secrets list --json` and pass the stored version as --expected-version",
                    ));
                }
                entries[index].value = value.to_string();
                entries[index].version = stored + 1;
                Ok(entries[index].version)
            }
            None => {
                if expected_version.is_some() {
                    return Err(conflict(
                        "version mismatch: entry does not exist yet".to_string(),
                        "omit --expected-version when saving a secret for a new cluster target",
                    ));
                }
                entries.push(ScopedSecret {
                    cluster_identity: scope.cluster_identity.clone(),
                    release: scope.release.clone(),
                    namespace: scope.namespace.clone(),
                    value: value.to_string(),
                    version: 1,
                });
                Ok(1)
            }
        }
    }

    fn remove_scoped(&mut self, name: &str, scope: &SecretScope) {
        if let Some(entries) = self.scoped.get_mut(name) {
            entries.retain(|entry| entry.scope() != *scope);
            if entries.is_empty() {
                self.scoped.remove(name);
            }
        }
    }

    fn list_output(&self) -> SecretsListOutput {
        let mut names = BTreeSet::new();
        let mut entries = Vec::new();
        for name in self.values.keys() {
            names.insert(name.clone());
            entries.push(SecretListEntry {
                name: name.clone(),
                scope: None,
                version: None,
            });
        }
        for (name, scoped) in &self.scoped {
            names.insert(name.clone());
            for entry in scoped {
                entries.push(SecretListEntry {
                    name: name.clone(),
                    scope: Some(entry.scope()),
                    version: Some(entry.version),
                });
            }
        }
        entries.sort_by(|a, b| {
            a.name.cmp(&b.name).then_with(|| {
                a.scope
                    .as_ref()
                    .map(SecretScope::describe)
                    .cmp(&b.scope.as_ref().map(SecretScope::describe))
            })
        });
        SecretsListOutput {
            names: names.into_iter().collect(),
            entries,
        }
    }
}

pub fn mismatch_error(name: &str, target: &SecretScope, existing: &[SecretScope]) -> anyhow::Error {
    let recorded = existing
        .iter()
        .map(SecretScope::describe)
        .collect::<Vec<_>>()
        .join("; ");
    crate::exit::usage(format!(
        "stored secret {name} is scoped to {recorded}; refusing to inject it into {}. Save a value for this target with `curie secrets set {name} --from-env {name} --cluster-identity {} --release {} --namespace {}`",
        target.describe(),
        target.cluster_identity,
        target.release,
        target.namespace
    ))
}

pub fn unscoped_error(name: &str, target: &SecretScope) -> anyhow::Error {
    crate::exit::usage(format!(
        "stored secret {name} has no cluster scope; refusing to inject it into {}. Re-save it with --cluster-identity {} --release {} --namespace {}",
        target.describe(),
        target.cluster_identity,
        target.release,
        target.namespace
    ))
}

/// Names of incoming keys that already exist on the live connector Secret.
/// Values are never inspected; presence of the key is the replacement signal.
pub fn keys_being_replaced(
    existing_keys: &BTreeSet<String>,
    incoming_keys: &[String],
) -> Vec<String> {
    incoming_keys
        .iter()
        .filter(|key| existing_keys.contains(*key))
        .cloned()
        .collect()
}

pub fn write_intent_line(secret_name: &str, keys: &[String], target: &SecretScope) -> String {
    format!(
        "writing stored connector secrets into {secret_name} for {}: {}",
        target.describe(),
        keys.join(", ")
    )
}

pub fn replacement_warning_line(secret_name: &str, keys: &[String]) -> String {
    format!(
        "replacing non-empty connector secret keys in {secret_name}: {} (values not shown)",
        keys.join(", ")
    )
}

pub fn delete_value(name: &str) -> Result<()> {
    validate_name(name)?;
    let mut credentials = read_credentials()?;
    if credentials.values.remove(name).is_some() {
        write_credentials(&credentials)?;
    }
    Ok(())
}

pub fn remove_value(name: &str) -> Result<()> {
    validate_name(name)?;
    delete_value(name)?;
    remove_from_index(name)
}

/// Remove every scope for a name. The interactive TUI has no scope picker, so
/// it must remove all matching entries rather than claim a partial success.
pub fn remove_all_values(name: &str) -> Result<()> {
    validate_name(name)?;
    let mut credentials = read_credentials()?;
    credentials.remove_all(name);
    write_credentials(&credentials)?;
    remove_from_index(name)
}

pub fn list_names() -> Result<Vec<String>> {
    let index = read_index()?;
    let mut names = index
        .names
        .into_iter()
        .chain(index.legacy_names)
        .chain(index.file_names)
        .collect::<BTreeSet<_>>();
    for name in read_credentials()?.list_output().names {
        names.insert(name);
    }
    Ok(names.into_iter().collect())
}

pub fn validate_name(name: &str) -> Result<()> {
    if name.is_empty() {
        bail!("secret name is required");
    }
    let valid = name
        .chars()
        .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
        && name
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_uppercase() || c == '_');
    if !valid {
        bail!(
            "secret name must look like an environment variable, e.g. GITHUB_PERSONAL_ACCESS_TOKEN"
        );
    }
    Ok(())
}

fn prompt_secret(name: &str) -> Result<String> {
    if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
        bail!("setting {name} requires a terminal; use --from-env VAR in non-interactive contexts");
    }
    print!("{name}: ");
    io::stdout().flush().ok();
    rpassword::read_password().context("reading secret from terminal")
}

fn vault_entry() -> Result<keyring::Entry> {
    keyring::Entry::new(SERVICE, VAULT_ACCOUNT)
        .context("opening the Curie OS credential-store vault")
}

fn legacy_entry(name: &str) -> Result<keyring::Entry> {
    keyring::Entry::new(SERVICE, &format!("curie:global:{name}"))
        .with_context(|| format!("opening legacy OS credential-store entry for {name}"))
}

fn vault_cache() -> &'static Mutex<VaultCache> {
    VAULT_CACHE.get_or_init(|| Mutex::new(VaultCache::default()))
}

fn read_vault() -> Result<SecretVault> {
    let mut cache = vault_cache()
        .lock()
        .map_err(|_| anyhow::anyhow!("Curie credential vault cache is unavailable"))?;
    if cache.loaded {
        return Ok(cache.vault.clone());
    }
    let vault = match vault_entry()?.get_password() {
        Ok(raw) => serde_json::from_str(&raw).context("parsing the Curie credential vault")?,
        Err(keyring::Error::NoEntry) => SecretVault::default(),
        Err(err) => return Err(err).context("reading the Curie OS credential-store vault"),
    };
    cache.loaded = true;
    cache.vault = vault.clone();
    Ok(vault)
}

fn read_credentials() -> Result<SecretVault> {
    let path = credentials_path()?;
    if !path.is_file() {
        return Ok(SecretVault::default());
    }
    let raw = fs::read_to_string(&path)
        .with_context(|| format!("reading Curie credentials {}", path.display()))?;
    serde_json::from_str(&raw)
        .with_context(|| format!("parsing Curie credentials {}", path.display()))
}

fn write_credentials(credentials: &SecretVault) -> Result<()> {
    let path = credentials_path()?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating Curie config dir {}", parent.display()))?;
    }
    let body = serde_json::to_vec_pretty(credentials).context("serializing Curie credentials")?;
    write_private(&path, &body)
}

fn add_to_index(name: &str) -> Result<()> {
    let index = mark_file_saved(read_index()?, name);
    write_index(&index)
}

fn mark_file_saved(mut index: SecretIndex, name: &str) -> SecretIndex {
    index.legacy_names.remove(name);
    index.names.remove(name);
    index.file_names.insert(name.to_string());
    index
}

fn reconcile_file_names<'a>(
    mut index: SecretIndex,
    file_names: impl Iterator<Item = &'a String>,
) -> SecretIndex {
    for name in file_names {
        index.legacy_names.remove(name);
        index.names.remove(name);
        index.file_names.insert(name.clone());
    }
    index
}

fn remove_from_index(name: &str) -> Result<()> {
    let mut index = read_index()?;
    index.names.remove(name);
    index.legacy_names.remove(name);
    index.file_names.remove(name);
    write_index(&index)
}

fn read_index() -> Result<SecretIndex> {
    let path = index_path()?;
    if !path.is_file() {
        return Ok(SecretIndex::default());
    }
    let raw = fs::read_to_string(&path)
        .with_context(|| format!("reading secret index {}", path.display()))?;
    serde_json::from_str(&raw).with_context(|| format!("parsing secret index {}", path.display()))
}

fn write_index(index: &SecretIndex) -> Result<()> {
    let path = index_path()?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating Curie config dir {}", parent.display()))?;
    }
    let body = serde_json::to_vec_pretty(index).context("serializing secret index")?;
    write_private(&path, &body)
}

#[cfg(unix)]
fn write_private(path: &Path, body: &[u8]) -> Result<()> {
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

    let mut file = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .mode(0o600)
        .open(path)
        .with_context(|| format!("opening private file {}", path.display()))?;
    file.set_permissions(fs::Permissions::from_mode(0o600))
        .with_context(|| format!("securing private file {}", path.display()))?;
    file.write_all(body)
        .with_context(|| format!("writing private file {}", path.display()))
}

#[cfg(not(unix))]
fn write_private(path: &Path, body: &[u8]) -> Result<()> {
    fs::write(path, body).with_context(|| format!("writing private file {}", path.display()))
}

fn index_path() -> Result<PathBuf> {
    Ok(config_dir()?.join("secrets.json"))
}

fn credentials_path() -> Result<PathBuf> {
    Ok(config_dir()?.join("credentials.json"))
}

fn config_dir() -> Result<PathBuf> {
    if let Ok(dir) = std::env::var("CURIE_CONFIG_DIR") {
        return Ok(PathBuf::from(dir));
    }
    let home = std::env::var("HOME").context("HOME is not set; cannot locate Curie config dir")?;
    Ok(PathBuf::from(home).join(".config/curie"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ui::CliOutput;

    #[test]
    fn provider_expiry_check_distinguishes_warning_and_expired() {
        let now = OffsetDateTime::parse("2026-09-22T00:00:00Z", &Rfc3339).unwrap();
        let entry = |name: &str, expires_at: &str| ObjectMetadata {
            name: name.to_string(),
            version: crate::provider::ObjectVersion {
                id: "version-one".to_string(),
            },
            tags: BTreeMap::from([(EXPIRY_TAG.to_string(), expires_at.to_string())]),
            key_names: vec!["KEY".to_string()],
        };
        let output = provider_check_output(
            vec![
                entry("safe", "2026-10-23T00:00:00Z"),
                entry("warning", "2026-10-22T00:00:00Z"),
                entry("expired", "2026-09-21T00:00:00Z"),
            ],
            now,
        )
        .unwrap();
        let statuses: BTreeMap<_, _> = output
            .entries
            .iter()
            .map(|item| (item.name.as_str(), item.status))
            .collect();
        assert_eq!(statuses["safe"], "ok");
        assert_eq!(statuses["warning"], "warning");
        assert_eq!(statuses["expired"], "expired");
    }

    #[test]
    fn validates_env_like_names() {
        assert!(validate_name("GITHUB_PERSONAL_ACCESS_TOKEN").is_ok());
        assert!(validate_name("_TOKEN1").is_ok());
        assert!(validate_name("github_token").is_err());
        assert!(validate_name("1TOKEN").is_err());
        assert!(validate_name("TOKEN-NOPE").is_err());
    }

    #[test]
    fn legacy_index_does_not_claim_per_key_secrets_are_in_the_vault() {
        let index: SecretIndex = serde_json::from_str(
            r#"{"names":["ANTHROPIC_API_KEY","GITHUB_PERSONAL_ACCESS_TOKEN"]}"#,
        )
        .unwrap();

        assert!(!index.vault);
        assert_eq!(index.names.len(), 2);
    }

    #[test]
    fn upgrading_one_legacy_name_preserves_every_other_name() {
        let legacy: SecretIndex = serde_json::from_str(
            r#"{"names":["ANTHROPIC_API_KEY","GITHUB_PERSONAL_ACCESS_TOKEN","OPENAI_API_KEY"]}"#,
        )
        .unwrap();

        let upgraded = mark_file_saved(legacy, "ANTHROPIC_API_KEY");

        assert_eq!(
            upgraded.names,
            BTreeSet::from([
                "GITHUB_PERSONAL_ACCESS_TOKEN".to_string(),
                "OPENAI_API_KEY".to_string()
            ])
        );
        assert_eq!(
            upgraded.file_names,
            BTreeSet::from(["ANTHROPIC_API_KEY".to_string()])
        );
    }

    #[test]
    fn consolidated_vault_names_recover_a_stale_legacy_index() {
        let legacy: SecretIndex = serde_json::from_str(
            r#"{"names":["ANTHROPIC_API_KEY","GITHUB_PERSONAL_ACCESS_TOKEN","OPENAI_API_KEY"]}"#,
        )
        .unwrap();
        let file_names = [
            "ANTHROPIC_API_KEY".to_string(),
            "GITHUB_PERSONAL_ACCESS_TOKEN".to_string(),
        ];

        let recovered = reconcile_file_names(legacy, file_names.iter());

        assert_eq!(
            recovered.names,
            BTreeSet::from(["OPENAI_API_KEY".to_string()])
        );
        assert_eq!(recovered.file_names, BTreeSet::from(file_names));
    }

    #[test]
    fn vault_round_trips_multiple_credentials_in_one_payload() {
        let vault = SecretVault {
            values: BTreeMap::from([
                ("ANTHROPIC_API_KEY".to_string(), "model-secret".to_string()),
                (
                    "GITHUB_PERSONAL_ACCESS_TOKEN".to_string(),
                    "github-secret".to_string(),
                ),
            ]),
            scoped: BTreeMap::new(),
        };
        let raw = serde_json::to_string(&vault).unwrap();
        let decoded: SecretVault = serde_json::from_str(&raw).unwrap();

        assert_eq!(decoded.values, vault.values);
        assert_eq!(VAULT_ACCOUNT, "curie:global:vault");
    }

    #[test]
    fn same_name_secrets_are_distinct_across_cluster_identities() {
        let mut vault = SecretVault::default();
        let a = SecretScope {
            cluster_identity: "ca:a".into(),
            release: "curie".into(),
            namespace: "curie-test".into(),
        };
        let b = SecretScope {
            cluster_identity: "ca:b".into(),
            release: "curie".into(),
            namespace: "curie".into(),
        };
        vault
            .save_scoped("K8S_WRITE_KUBECONFIG", &a, "token-a", None)
            .unwrap();
        vault
            .save_scoped("K8S_WRITE_KUBECONFIG", &b, "token-b", None)
            .unwrap();

        let resolved_a = vault
            .resolve_cluster("K8S_WRITE_KUBECONFIG", &a)
            .unwrap()
            .unwrap();
        let resolved_b = vault
            .resolve_cluster("K8S_WRITE_KUBECONFIG", &b)
            .unwrap()
            .unwrap();
        assert_eq!(resolved_a.value, "token-a");
        assert_eq!(resolved_b.value, "token-b");
        assert_ne!(resolved_a.value, resolved_b.value);
    }

    #[test]
    fn cluster_deploy_refuses_a_mismatched_scope() {
        let mut vault = SecretVault::default();
        let a = SecretScope {
            cluster_identity: "ca:a".into(),
            release: "curie".into(),
            namespace: "curie-test".into(),
        };
        let b = SecretScope {
            cluster_identity: "ca:b".into(),
            release: "curie".into(),
            namespace: "curie".into(),
        };
        vault
            .save_scoped("K8S_WRITE_KUBECONFIG", &a, "token-a", None)
            .unwrap();

        let err = vault
            .resolve_cluster("K8S_WRITE_KUBECONFIG", &b)
            .unwrap_err();
        let message = err.to_string();
        assert!(
            message.contains("refusing to inject"),
            "mismatch must refuse, got {message}"
        );
        assert!(message.contains("ca:a"), "must name the stored scope");
        assert!(message.contains("ca:b"), "must name the requested scope");
        assert!(
            !message.contains("token-a"),
            "refusal must not leak the stored value"
        );
    }

    #[test]
    fn cluster_deploy_refuses_unscoped_reuse() {
        let mut vault = SecretVault::default();
        vault
            .values
            .insert("K8S_WRITE_KUBECONFIG".into(), "token-unscoped".into());
        let target = SecretScope {
            cluster_identity: "ca:b".into(),
            release: "curie".into(),
            namespace: "curie".into(),
        };
        let err = vault
            .resolve_cluster("K8S_WRITE_KUBECONFIG", &target)
            .unwrap_err();
        let message = err.to_string();
        assert!(message.contains("no cluster scope"));
        assert!(
            !message.contains("token-unscoped"),
            "refusal must not leak the stored value"
        );
    }

    #[test]
    fn replacing_a_scoped_secret_requires_the_stored_version() {
        let mut vault = SecretVault::default();
        let target = SecretScope {
            cluster_identity: "ca:a".into(),
            release: "curie".into(),
            namespace: "curie-test".into(),
        };
        assert_eq!(
            vault
                .save_scoped("K8S_WRITE_KUBECONFIG", &target, "token-1", None)
                .unwrap(),
            1
        );
        let stale = vault
            .save_scoped("K8S_WRITE_KUBECONFIG", &target, "token-2", None)
            .unwrap_err()
            .to_string();
        assert!(stale.contains("version mismatch"));
        assert!(
            !stale.contains("token-1") && !stale.contains("token-2"),
            "conflict must not leak secret values"
        );
        assert_eq!(
            vault
                .save_scoped("K8S_WRITE_KUBECONFIG", &target, "token-2", Some(1))
                .unwrap(),
            2
        );
        assert_eq!(
            vault
                .resolve_cluster("K8S_WRITE_KUBECONFIG", &target)
                .unwrap()
                .unwrap()
                .value,
            "token-2"
        );
    }

    #[test]
    fn expected_version_cannot_create_a_missing_scoped_entry() {
        let mut vault = SecretVault::default();
        let target = SecretScope {
            cluster_identity: "ca:a".into(),
            release: "curie".into(),
            namespace: "curie-test".into(),
        };
        let err = vault
            .save_scoped("K8S_WRITE_KUBECONFIG", &target, "token", Some(1))
            .unwrap_err()
            .to_string();
        assert!(err.contains("does not exist yet"));
    }

    #[test]
    fn list_output_exposes_scope_and_version_never_values() {
        let mut vault = SecretVault::default();
        vault
            .values
            .insert("ANTHROPIC_API_KEY".into(), "sk-ant-never-print".into());
        let target = SecretScope {
            cluster_identity: "ca:a".into(),
            release: "curie".into(),
            namespace: "curie-test".into(),
        };
        vault
            .save_scoped("K8S_WRITE_KUBECONFIG", &target, "token-a", None)
            .unwrap();
        let out = vault.list_output();
        let json = out.to_json().to_string();
        assert!(json.contains("K8S_WRITE_KUBECONFIG"));
        assert!(json.contains("ca:a"));
        assert!(json.contains("\"version\":1"));
        assert!(!json.contains("token-a"));
        assert!(!json.contains("sk-ant-never-print"));
    }

    #[test]
    fn replacement_visibility_names_keys_not_values() {
        let existing = BTreeSet::from(["K8S_WRITE_KUBECONFIG".to_string()]);
        let incoming = vec!["K8S_WRITE_KUBECONFIG".to_string(), "OTHER".to_string()];
        let replaced = keys_being_replaced(&existing, &incoming);
        assert_eq!(replaced, vec!["K8S_WRITE_KUBECONFIG".to_string()]);
        let warning = replacement_warning_line("acme-bot-connector-secrets", &replaced);
        assert!(warning.contains("K8S_WRITE_KUBECONFIG"));
        assert!(warning.contains("values not shown"));
        let intent = write_intent_line(
            "acme-bot-connector-secrets",
            &incoming,
            &SecretScope {
                cluster_identity: "ca:a".into(),
                release: "curie".into(),
                namespace: "curie-test".into(),
            },
        );
        assert!(intent.contains("K8S_WRITE_KUBECONFIG"));
        assert!(!intent.contains("token"));
    }

    #[test]
    fn legacy_credentials_json_without_scoped_still_parses() {
        let decoded: SecretVault =
            serde_json::from_str(r#"{"values":{"ANTHROPIC_API_KEY":"x"}}"#).unwrap();
        assert_eq!(decoded.values.get("ANTHROPIC_API_KEY").unwrap(), "x");
        assert!(decoded.scoped.is_empty());
    }

    #[test]
    fn tui_all_scope_removal_cannot_leave_a_scoped_entry() {
        let mut vault = SecretVault::default();
        let scope = SecretScope {
            cluster_identity: "ca:a".into(),
            release: "curie".into(),
            namespace: "curie-test".into(),
        };
        vault
            .save_scoped("K8S_WRITE_KUBECONFIG", &scope, "token-a", None)
            .unwrap();
        vault.remove_all("K8S_WRITE_KUBECONFIG");
        assert!(!vault.has_name("K8S_WRITE_KUBECONFIG"));
        assert!(vault.scoped_entries("K8S_WRITE_KUBECONFIG").is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn private_files_are_forced_to_owner_only_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("credentials.json");
        fs::write(&path, b"old").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();

        write_private(&path, br#"{"values":{}}"#).unwrap();

        assert_eq!(
            fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
}

//! `cluster deploy` with a secrets provider (ADR 0163 decisions 3, 5, 6, 7):
//! connector credentials go to Secrets Manager and External Secrets syncs
//! them into the cluster, instead of the CLI writing value Secrets itself.
//!
//! The steps are split so the caller can order them: [`plan`] is pure,
//! [`preflight`] only reads, and only then do [`write_provider`] and
//! [`apply_objects`] write. Values live in [`SecretMaterial`] so a `Debug` of
//! the plan cannot print them, and no error here carries a value.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use serde_json::Value;

use super::eso::{self, Kubectl, StoreSpec, SyncEntry};
use super::rotation;
use super::{
    bundle_entries, InventoryClass, InventoryEntry, ObjectMetadata, ObjectVersion, ProviderError,
    PutRequest, RotationOwner, SecretMaterial, SecretsProvider, Store, StoredObject, UpdatePolicy,
};
use crate::connectors::{owner_value, OWNER_LABEL};

/// The CRD whose presence says External Secrets is installed.
pub const EXTERNAL_SECRET_CRD: &str = "externalsecrets.external-secrets.io";

/// ESO's field manager for a Secret it writes is this prefix plus the
/// ExternalSecret's name.
pub const ESO_MANAGER_PREFIX: &str = "externalsecrets.external-secrets.io/";

/// How often ESO re-reads Secrets Manager for a connector entry.
const REFRESH_INTERVAL: &str = "1h";

/// What [`plan`] needs. Values are borrowed; the plan copies them into
/// [`SecretMaterial`].
pub struct PlanInput<'a> {
    pub release: &'a str,
    pub namespace: &'a str,
    pub agent: &'a str,
    /// `<secrets.prefix>/<release>`: the SM name prefix AwsSecretsProvider scopes logical names under.
    pub remote_prefix: &'a str,
    pub decl: &'a crate::connector_build::ConnectorsFileDecl,
    /// API-declared owned (hosted) keys -> the cluster-scoped value the connector plan resolved.
    pub hosted_values: &'a BTreeMap<String, String>,
    /// The sandbox bind map (explicit --secret plus connector env names) -> value.
    pub sandbox_values: &'a BTreeMap<String, String>,
}

/// One synced Secret: its ESO entry and the values Secrets Manager must hold.
#[derive(Debug)]
pub struct PlannedEntry {
    /// Target already expanded; labels carry the connector owner.
    pub sync: SyncEntry,
    /// Keys ESO pulls from `<logical>`.
    pub static_values: BTreeMap<String, SecretMaterial>,
    /// Keys the workload rotates; written once to `<logical>-rotated` as the
    /// initial backup the seed step reads.
    pub rotated_values: BTreeMap<String, SecretMaterial>,
}

/// Every Secret one agent's connector deploy syncs.
#[derive(Debug)]
pub struct Plan {
    pub hosted: Vec<PlannedEntry>,
    pub sandbox: Option<PlannedEntry>,
}

impl Plan {
    /// Hosted entries, then the sandbox entry.
    pub fn entries(&self) -> Vec<&PlannedEntry> {
        self.hosted.iter().chain(self.sandbox.iter()).collect()
    }

    /// Every ExternalSecret and PushSecret name this plan applies, so the ESO
    /// prune keeps exactly these.
    pub fn object_names(&self) -> Vec<String> {
        let mut names = Vec::new();
        for entry in self.entries() {
            names.push(entry.sync.name.clone());
            if !entry.sync.rotated_keys.is_empty() {
                names.push(format!("{}-rotated-backup", entry.sync.name));
            }
        }
        names
    }

    /// The sandbox Secret name and its sorted keys, for the Helm knob bind.
    pub fn sandbox_target(&self) -> Option<(&str, Vec<String>)> {
        self.sandbox.as_ref().map(|entry| {
            let mut keys: Vec<String> = entry.static_values.keys().cloned().collect();
            keys.sort();
            (entry.sync.target.as_str(), keys)
        })
    }
}

fn owner_labels(agent: &str) -> BTreeMap<String, String> {
    BTreeMap::from([(OWNER_LABEL.to_string(), owner_value(agent))])
}

fn materials(
    keys: &[String],
    values: &BTreeMap<String, String>,
    what: &str,
) -> Result<BTreeMap<String, SecretMaterial>> {
    let mut out = BTreeMap::new();
    for key in keys {
        let value = values
            .get(key)
            .ok_or_else(|| anyhow!("{what}: no value was resolved for {key}"))?;
        if value.is_empty() {
            bail!("{what}: the value for {key} is empty; Secrets Manager will not hold an empty credential");
        }
        out.insert(key.clone(), SecretMaterial::new(value.clone()));
    }
    Ok(out)
}

fn planned(
    entry: &InventoryEntry,
    remote_prefix: &str,
    agent: &str,
    values: &BTreeMap<String, String>,
) -> Result<PlannedEntry> {
    let sync = SyncEntry::from_inventory(entry, remote_prefix)?.with_labels(owner_labels(agent));
    let what = format!("connector credential {}", entry.logical_name);
    let static_values = materials(&sync.static_keys, values, &what)?;
    let rotated_values = materials(&sync.rotated_keys, values, &what)?;
    Ok(PlannedEntry {
        sync,
        static_values,
        rotated_values,
    })
}

/// Build the plan. Pure: no cluster, no provider.
///
/// Hosted entries come from the bundle inventory, so their logical names and
/// rotation split match what `curie secrets` and the render check see. Their
/// key union must equal what the API declared owned: the API decides which
/// keys this deploy resolves (#1163), and a disagreement would sync a key the
/// connector does not read or leave one it does read undelivered.
pub fn plan(input: &PlanInput) -> Result<Plan> {
    let agent = input.agent;
    let hosted_pattern = format!("{{release}}-{agent}-connector-secrets");
    let mut hosted = Vec::new();
    let mut declared = BTreeSet::new();
    for mut entry in bundle_entries(input.decl, agent)? {
        if entry.target != hosted_pattern {
            continue;
        }
        entry.target = entry.target.replace("{release}", input.release);
        declared.extend(entry.keys.iter().cloned());
        hosted.push(entry);
    }
    let owned: BTreeSet<String> = input.hosted_values.keys().cloned().collect();
    if declared != owned {
        let list = |set: &BTreeSet<String>| {
            if set.is_empty() {
                "none".to_string()
            } else {
                set.iter().cloned().collect::<Vec<_>>().join(", ")
            }
        };
        bail!(
            "agent {agent}: connectors.yaml declares hosted connector keys {} but the API \
             owns {}; redeploy the bundle so both read the same connectors.yaml",
            list(&declared),
            list(&owned)
        );
    }
    let hosted = hosted
        .iter()
        .map(|entry| planned(entry, input.remote_prefix, agent, input.hosted_values))
        .collect::<Result<Vec<_>>>()?;

    let sandbox = if input.sandbox_values.is_empty() {
        None
    } else {
        let fullname = crate::ops::chart_fullname(input.release);
        let entry = InventoryEntry {
            logical_name: format!("{agent}.sandbox"),
            class: InventoryClass::External,
            target: format!("{}-agent-{agent}-connector-secrets", fullname.as_str()),
            keys: input.sandbox_values.keys().cloned().collect(),
            consumers: vec!["runner".to_string()],
            rotation_owner: RotationOwner::Sm,
            update_policy: UpdatePolicy::Replace,
            store: Store::Sm,
            rotated_keys: Vec::new(),
            chart: None,
        };
        Some(planned(
            &entry,
            input.remote_prefix,
            agent,
            input.sandbox_values,
        )?)
    };
    Ok(Plan { hosted, sandbox })
}

fn is_not_found(stderr: &str) -> bool {
    stderr.contains("(NotFound)") || stderr.contains("not found")
}

/// Read-only check that nothing this deploy would write collides with state
/// it does not own. Runs before any provider, kubectl or helm write, so a
/// refusal leaves Secrets Manager and the cluster untouched.
///
/// A present target Secret is ours only when ESO already manages it for one of
/// this plan's ExternalSecrets. Anything else (a kubectl-applied Secret from a
/// provider-less deploy, a helm-rendered one) would be silently taken over by
/// `CreateOrMerge` or fight `Owner`, so it is refused by name.
pub fn preflight(k: &dyn Kubectl, namespace: &str, plan: &Plan) -> Result<()> {
    let args: Vec<String> = ["get", "crd", EXTERNAL_SECRET_CRD, "-o", "name"]
        .iter()
        .map(|a| a.to_string())
        .collect();
    let out = k.run(&args, None)?;
    if !out.success {
        if is_not_found(&out.stderr) {
            return Err(crate::exit::CliError::failure(format!(
                "External Secrets is not installed: CRD {EXTERNAL_SECRET_CRD} is missing, so \
                 connector credentials cannot sync from Secrets Manager; run `curie apply` to \
                 install External Secrets first"
            ))
            .with_fix("run `curie apply` to install External Secrets first")
            .into());
        }
        bail!(
            "could not check for CRD {EXTERNAL_SECRET_CRD}: {}",
            out.stderr.trim()
        );
    }

    // target -> the ExternalSecret names allowed to manage it
    let mut targets: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for entry in plan.entries() {
        targets
            .entry(entry.sync.target.as_str())
            .or_default()
            .push(entry.sync.name.as_str());
    }
    for (target, owners) in targets {
        let args: Vec<String> = [
            "-n",
            namespace,
            "get",
            "secret",
            target,
            "--show-managed-fields",
            "-o",
            "json",
        ]
        .iter()
        .map(|a| a.to_string())
        .collect();
        let out = k.run(&args, None)?;
        if !out.success {
            if is_not_found(&out.stderr) {
                continue;
            }
            bail!(
                "could not read Secret {namespace}/{target}: {}",
                out.stderr.trim()
            );
        }
        // Only metadata is read; the data map is never looked at.
        let secret: Value = serde_json::from_str(&out.stdout)
            .map_err(|_| anyhow!("Secret {namespace}/{target} returned invalid JSON"))?;
        let managers: Vec<String> = secret["metadata"]["managedFields"]
            .as_array()
            .map(|fields| {
                fields
                    .iter()
                    .filter_map(|f| f["manager"].as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        let ours = owners.iter().any(|owner| {
            let manager = format!("{ESO_MANAGER_PREFIX}{owner}");
            managers.iter().any(|m| m == &manager)
        });
        if !ours {
            let seen = if managers.is_empty() {
                "none".to_string()
            } else {
                managers.join(", ")
            };
            return Err(crate::exit::CliError::failure(format!(
                "Secret {namespace}/{target} already exists and External Secrets does not \
                 manage it (field managers: {seen}); refusing to take it over. Nothing was \
                 written."
            ))
            .with_fix(format!(
                "delete it (`kubectl -n {namespace} delete secret {target}`) or move it under \
                 External Secrets, then re-run `curie cluster deploy`"
            ))
            .into());
        }
    }
    Ok(())
}

/// What [`write_provider`] did, by logical name.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct WriteReport {
    pub written: Vec<String>,
    pub unchanged: Vec<String>,
    pub backups_created: Vec<String>,
}

fn read_object(
    provider: &dyn SecretsProvider,
    name: &str,
) -> Result<Option<(String, BTreeMap<String, String>)>> {
    let stored = match provider.get(name, None) {
        Ok(stored) => stored,
        Err(ProviderError::NotFound { .. }) => return Ok(None),
        Err(err) => return Err(anyhow!("could not read {name}: {err}")),
    };
    let parsed: BTreeMap<String, String> = serde_json::from_str(stored.material.expose())
        .map_err(|_| anyhow!("provider object {name} is not a JSON object of strings"))?;
    Ok(Some((stored.version.id, parsed)))
}

/// Write each entry's static keys to `<logical>` and create the rotated
/// backup `<logical>-rotated` when it does not exist yet.
///
/// Keys already stored that the plan does not own survive the merge. A
/// redeploy with the same values issues no `put`, so the version does not
/// churn. An existing backup is never overwritten: after the first deploy the
/// workload's rotated value is the truth, not the one resolved here.
pub fn write_provider(provider: &dyn SecretsProvider, plan: &Plan) -> Result<WriteReport> {
    let mut report = WriteReport::default();
    for entry in plan.entries() {
        let name = entry.sync.name.as_str();
        if !entry.static_values.is_empty() {
            let current = read_object(provider, name)?;
            let (version, mut merged) = match current.clone() {
                Some((version, map)) => (Some(version), map),
                None => (None, BTreeMap::new()),
            };
            for (key, value) in &entry.static_values {
                merged.insert(key.clone(), value.expose().to_string());
            }
            if current.as_ref().is_some_and(|(_, map)| map == &merged) {
                report.unchanged.push(name.to_string());
            } else {
                let material = SecretMaterial::new(
                    serde_json::to_string(&merged).context("serializing provider object")?,
                );
                provider
                    .put(&PutRequest {
                        name,
                        material: &material,
                        expected_version: version.as_deref(),
                    })
                    .map_err(|err| anyhow!("could not write {name}: {err}"))?;
                report.written.push(name.to_string());
            }
        }
        if !entry.rotated_values.is_empty() {
            let backup = format!("{name}-rotated");
            match provider.get_metadata(&backup) {
                Ok(_) => {}
                Err(ProviderError::NotFound { .. }) => {
                    let map: BTreeMap<&str, &str> = entry
                        .rotated_values
                        .iter()
                        .map(|(k, v)| (k.as_str(), v.expose()))
                        .collect();
                    let material = SecretMaterial::new(
                        serde_json::to_string(&map).context("serializing rotated backup")?,
                    );
                    provider
                        .put(&PutRequest {
                            name: &backup,
                            material: &material,
                            expected_version: None,
                        })
                        .map_err(|err| anyhow!("could not create {backup}: {err}"))?;
                    report.backups_created.push(backup);
                }
                Err(err) => return Err(anyhow!("could not read {backup}: {err}")),
            }
        }
    }
    Ok(report)
}

/// Wraps a provider taking logical names so a full remote key
/// `<remote_prefix>/<logical>` resolves. `rotation::apply_sync_entry` reads the
/// backup by its remote key, and AwsSecretsProvider refuses `/` in a name.
pub struct RemoteKeys<'a> {
    pub inner: &'a dyn SecretsProvider,
    pub remote_prefix: &'a str,
}

impl RemoteKeys<'_> {
    fn logical<'n>(&self, name: &'n str) -> &'n str {
        let prefix = self.remote_prefix.trim_end_matches('/');
        name.strip_prefix(prefix)
            .and_then(|rest| rest.strip_prefix('/'))
            .unwrap_or(name)
    }
}

impl SecretsProvider for RemoteKeys<'_> {
    fn put(&self, request: &PutRequest<'_>) -> Result<ObjectVersion, ProviderError> {
        self.inner.put(&PutRequest {
            name: self.logical(request.name),
            material: request.material,
            expected_version: request.expected_version,
        })
    }

    fn get(&self, name: &str, version: Option<&str>) -> Result<StoredObject, ProviderError> {
        self.inner.get(self.logical(name), version)
    }

    fn get_metadata(&self, name: &str) -> Result<ObjectMetadata, ProviderError> {
        self.inner.get_metadata(self.logical(name))
    }

    fn list(&self, prefix: &str) -> Result<Vec<ObjectMetadata>, ProviderError> {
        self.inner.list(self.logical(prefix))
    }

    fn tag(
        &self,
        name: &str,
        tags: &BTreeMap<String, String>,
        expected_version: Option<&str>,
    ) -> Result<ObjectVersion, ProviderError> {
        self.inner.tag(self.logical(name), tags, expected_version)
    }

    fn delete(
        &self,
        name: &str,
        expected_version: Option<&str>,
    ) -> Result<ObjectVersion, ProviderError> {
        self.inner.delete(self.logical(name), expected_version)
    }
}

/// Server-side apply the ServiceAccount and SecretStore, then each entry's
/// ExternalSecret (and PushSecret) after seeding rotated keys from the backup,
/// then force a sync of every ExternalSecret and wait for it, so each target
/// Secret exists before anything that reads it rolls.
pub fn apply_objects(
    k: &dyn Kubectl,
    provider: &dyn SecretsProvider,
    plan: &Plan,
    store: &StoreSpec,
    remote_prefix: &str,
    sync_timeout: Duration,
    poll: Duration,
) -> Result<()> {
    let namespace = store.namespace.as_str();
    eso::apply(
        k,
        namespace,
        &[
            eso::render_service_account(store),
            eso::render_secret_store(store),
        ],
    )
    .with_context(|| format!("applying SecretStore {namespace}/{}", store.name))?;
    let remote = RemoteKeys {
        inner: provider,
        remote_prefix,
    };
    for entry in plan.entries() {
        rotation::apply_sync_entry(
            k,
            &remote,
            &entry.sync,
            namespace,
            &store.name,
            REFRESH_INTERVAL,
        )
        .with_context(|| format!("applying ExternalSecret {namespace}/{}", entry.sync.name))?;
    }
    for entry in plan.entries() {
        eso::force_sync_and_wait(k, namespace, &entry.sync.name, sync_timeout, poll)?;
    }
    Ok(())
}

/// argv (no leading "kubectl") deleting this agent's owner-labelled
/// ExternalSecrets and PushSecrets that the plan no longer declares, so a
/// dropped connector's sync goes with it (#1063). One comma-joined field
/// selector, for the reason [`crate::connectors::prune_args`] documents.
pub fn eso_prune_args(namespace: &str, agent: &str, keep: &[String]) -> Vec<String> {
    let mut args: Vec<String> = vec![
        "-n".into(),
        namespace.into(),
        "delete".into(),
        "externalsecret,pushsecret".into(),
        "-l".into(),
        format!("{OWNER_LABEL}={}", owner_value(agent)),
        "--ignore-not-found".into(),
    ];
    if !keep.is_empty() {
        let excluded: Vec<String> = keep.iter().map(|n| format!("metadata.name!={n}")).collect();
        args.push(format!("--field-selector={}", excluded.join(",")));
    }
    args
}

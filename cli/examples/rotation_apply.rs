//! Thin CLI driver over `rotation::apply_sync_entry`, proving the library
//! against a real cluster with no aws binary. The backup provider is
//! file-backed: a 0600 JSON file, or absent for no backup.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;

use curie::provider::eso::{SyncEntry, SystemKubectl};
use curie::provider::rotation::apply_sync_entry;
use curie::provider::{
    InventoryClass, InventoryEntry, ProviderError, RotationOwner, SecretMaterial, SecretsProvider,
    Store, StoredObject, UpdatePolicy,
};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    kubeconfig: Option<PathBuf>,
    #[arg(long)]
    context: Option<String>,
    #[arg(long)]
    namespace: String,
    #[arg(long)]
    store: String,
    #[arg(long)]
    prefix: String,
    #[arg(long)]
    logical_name: String,
    #[arg(long)]
    target: String,
    #[arg(long = "static-key")]
    static_keys: Vec<String>,
    #[arg(long = "rotated-key")]
    rotated_keys: Vec<String>,
    #[arg(long)]
    refresh: String,
    #[arg(long)]
    backup_file: Option<PathBuf>,
}

/// Reads one backup file. `get` returns the file's contents only for the
/// entry's own backup name; every other object is `NotFound` or refused.
struct FileBackup {
    file: Option<PathBuf>,
}

impl FileBackup {
    fn from_path(path: Option<PathBuf>) -> Result<Self> {
        if let Some(path) = &path {
            let mode = fs::metadata(path)
                .with_context(|| format!("could not stat backup file {}", path.display()))?
                .permissions()
                .mode()
                & 0o777;
            if mode != 0o600 {
                bail!(
                    "backup file {} must be mode 0600, not {mode:04o}",
                    path.display()
                );
            }
        }
        Ok(Self { file: path })
    }
}

impl SecretsProvider for FileBackup {
    fn get(&self, name: &str, _version: Option<&str>) -> Result<StoredObject, ProviderError> {
        let Some(path) = &self.file else {
            return Err(ProviderError::NotFound {
                name: name.to_string(),
            });
        };
        let contents = fs::read_to_string(path).map_err(|_| ProviderError::Unavailable {
            name: name.to_string(),
            status: 1,
        })?;
        Ok(StoredObject {
            version: curie::provider::ObjectVersion {
                id: "1".to_string(),
            },
            material: SecretMaterial::new(contents),
            key_names: vec![],
        })
    }

    fn put(
        &self,
        request: &curie::provider::PutRequest<'_>,
    ) -> Result<curie::provider::ObjectVersion, ProviderError> {
        Err(ProviderError::Unavailable {
            name: request.name.to_string(),
            status: 1,
        })
    }

    fn get_metadata(&self, name: &str) -> Result<curie::provider::ObjectMetadata, ProviderError> {
        Err(ProviderError::Unavailable {
            name: name.to_string(),
            status: 1,
        })
    }

    fn list(&self, prefix: &str) -> Result<Vec<curie::provider::ObjectMetadata>, ProviderError> {
        Err(ProviderError::Unavailable {
            name: prefix.to_string(),
            status: 1,
        })
    }

    fn tag(
        &self,
        name: &str,
        _tags: &std::collections::BTreeMap<String, String>,
        _expected_version: Option<&str>,
    ) -> Result<curie::provider::ObjectVersion, ProviderError> {
        Err(ProviderError::Unavailable {
            name: name.to_string(),
            status: 1,
        })
    }

    fn delete(
        &self,
        name: &str,
        _expected_version: Option<&str>,
    ) -> Result<curie::provider::ObjectVersion, ProviderError> {
        Err(ProviderError::Unavailable {
            name: name.to_string(),
            status: 1,
        })
    }
}

fn run() -> Result<()> {
    let args = Args::parse();
    let mut keys = args.static_keys.clone();
    for key in &args.rotated_keys {
        if !keys.contains(key) {
            keys.push(key.clone());
        }
    }
    let rotation_owner = if args.rotated_keys.is_empty() {
        RotationOwner::Sm
    } else {
        RotationOwner::Workload(args.logical_name.clone())
    };
    let inventory_entry = InventoryEntry {
        logical_name: args.logical_name.clone(),
        class: InventoryClass::External,
        target: args.target.clone(),
        keys,
        consumers: vec![args.logical_name.clone()],
        rotation_owner,
        update_policy: UpdatePolicy::Replace,
        store: Store::Sm,
        rotated_keys: args.rotated_keys.clone(),
        chart: None,
    };
    let entry: SyncEntry = SyncEntry::from_inventory(&inventory_entry, &args.prefix)?;

    let backup = FileBackup::from_path(args.backup_file)?;
    let kubectl = SystemKubectl {
        context: args.context,
        kubeconfig: args.kubeconfig,
        call_timeout: Some(Duration::from_secs(60)),
    };

    let report = apply_sync_entry(
        &kubectl,
        &backup,
        &entry,
        &args.namespace,
        &args.store,
        &args.refresh,
    )?;
    println!(
        "{}",
        serde_json::to_string(&report).map_err(|err| anyhow!("could not encode report: {err}"))?
    );
    Ok(())
}

fn main() {
    if let Err(err) = run() {
        eprintln!("{err:#}");
        std::process::exit(1);
    }
}

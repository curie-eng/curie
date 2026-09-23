//! Shared contract for a provider-backed install store.
//!
//! `get` is the only method that returns material. Errors name the object and,
//! where relevant, version ids. They do not carry material or backend stderr.

pub mod aws;
pub mod binding;
mod catalog;
pub mod connector_deploy;
pub mod eso;
mod inventory;
pub mod render_check;
pub mod rotation;

pub use catalog::{bundle_entries, merge, parse_inventory, platform_inventory, validate_inventory};
pub use inventory::{
    ChartBinding, ChartKnob, InventoryClass, InventoryEntry, RotationOwner, Store, UpdatePolicy,
};

use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;

use anyhow::Result;

/// Tag written by `curie secrets set --expires`.
pub const EXPIRY_TAG: &str = "curie:expires-at";

/// Secret bytes. `Debug` and `Display` redact. `expose` is the only read.
#[derive(Clone, PartialEq, Eq)]
pub struct SecretMaterial {
    bytes: String,
}

impl SecretMaterial {
    pub fn new(bytes: impl Into<String>) -> Self {
        Self {
            bytes: bytes.into(),
        }
    }

    pub fn expose(&self) -> &str {
        &self.bytes
    }
}

impl fmt::Debug for SecretMaterial {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretMaterial(redacted)")
    }
}

impl fmt::Display for SecretMaterial {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("redacted")
    }
}

/// A provider version id. Never a credential value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectVersion {
    pub id: String,
}

/// Metadata for one object. Key names are names, not values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMetadata {
    pub name: String,
    pub version: ObjectVersion,
    pub tags: BTreeMap<String, String>,
    pub key_names: Vec<String>,
}

/// A stored object. `Debug` redacts `material`.
pub struct StoredObject {
    pub version: ObjectVersion,
    pub material: SecretMaterial,
    pub key_names: Vec<String>,
}

impl fmt::Debug for StoredObject {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoredObject")
            .field("version", &self.version.id)
            .field("material", &self.material)
            .field("key_names", &self.key_names)
            .finish()
    }
}

/// Whole-object write. The caller merges keys before `put`.
pub struct PutRequest<'a> {
    pub name: &'a str,
    pub material: &'a SecretMaterial,
    pub expected_version: Option<&'a str>,
}

/// Why a provider refused a write that named a real object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectedReason {
    Immutable,
    Expired,
}

impl fmt::Display for RejectedReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Immutable => "immutable",
            Self::Expired => "expired",
        })
    }
}

/// Provider failure. Variants carry names, version ids, and status codes only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderError {
    NotFound {
        name: String,
    },
    Conflict {
        name: String,
        expected_version: Option<String>,
        actual_version: Option<String>,
    },
    Unavailable {
        name: String,
        status: i32,
    },
    InvalidName {
        name: String,
    },
    Rejected {
        name: String,
        reason: RejectedReason,
    },
}

impl fmt::Display for ProviderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound { name } => write!(formatter, "provider object {name} was not found"),
            Self::Conflict {
                name,
                expected_version,
                actual_version,
            } => write!(
                formatter,
                "provider object {name} version conflict, expected {}, actual {}",
                expected_version.as_deref().unwrap_or("none"),
                actual_version.as_deref().unwrap_or("none"),
            ),
            Self::Unavailable { name, status } => {
                write!(
                    formatter,
                    "provider object {name} is unavailable, status {status}"
                )
            }
            Self::InvalidName { name } => {
                write!(formatter, "provider object {name} has an invalid name")
            }
            Self::Rejected { name, reason } => {
                write!(formatter, "provider object {name} was rejected: {reason}")
            }
        }
    }
}

impl std::error::Error for ProviderError {}

/// Reads and writes one whole object. Implementations must not copy material
/// into `ProviderError`.
pub trait SecretsProvider {
    fn put(&self, request: &PutRequest<'_>) -> Result<ObjectVersion, ProviderError>;

    /// Read the current object when `version` is `None`, or that version id.
    fn get(&self, name: &str, version: Option<&str>) -> Result<StoredObject, ProviderError>;

    fn get_metadata(&self, name: &str) -> Result<ObjectMetadata, ProviderError>;

    fn list(&self, prefix: &str) -> Result<Vec<ObjectMetadata>, ProviderError>;

    fn tag(
        &self,
        name: &str,
        tags: &BTreeMap<String, String>,
        expected_version: Option<&str>,
    ) -> Result<ObjectVersion, ProviderError>;

    fn delete(
        &self,
        name: &str,
        expected_version: Option<&str>,
    ) -> Result<ObjectVersion, ProviderError>;
}

/// Where a `secrets set` or `secrets list` invocation stores names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreRoute {
    Local,
    Provider,
}

/// `set` uses the provider when `--expires` is set or `--file` declares one.
/// A parse error is returned as-is so a bad file cannot fall through to the
/// local writer.
pub fn route_set(file: Option<&Path>, expires: Option<&str>) -> Result<StoreRoute> {
    if let Some(path) = file {
        let installation = crate::installation::Installation::load(path)?;
        if installation.secrets.is_some() || expires.is_some() {
            return Ok(StoreRoute::Provider);
        }
        return Ok(StoreRoute::Local);
    }
    if expires.is_some() {
        Ok(StoreRoute::Provider)
    } else {
        Ok(StoreRoute::Local)
    }
}

/// `list` uses the provider only when `--file` declares one.
pub fn route_list(file: Option<&Path>) -> Result<StoreRoute> {
    if let Some(path) = file {
        let installation = crate::installation::Installation::load(path)?;
        if installation.secrets.is_some() {
            return Ok(StoreRoute::Provider);
        }
    }
    Ok(StoreRoute::Local)
}

/// Stable refusal for provider verbs whose backend is not in this change.
pub fn not_implemented(verb: &str) -> Result<()> {
    Err(
        crate::exit::CliError::failure(format!("`{verb}` is not implemented yet"))
            .with_fix(
                "Use curie secrets set, list, and unset for the local store. \
             Provider reads and writes are a separate change.",
            )
            .into(),
    )
}

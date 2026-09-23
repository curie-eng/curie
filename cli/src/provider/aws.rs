//! AWS Secrets Manager storage through AWS CLI v2.
//!
//! Request bodies use private JSON files. Secret material therefore never
//! appears in the child process arguments, errors, or logs.

use std::collections::BTreeMap;
use std::fs;
use std::io::Write as _;
use std::process::{Command, Output, Stdio};

use anyhow::{bail, Result};
use serde_json::{json, Map, Value};

use super::{
    ObjectMetadata, ObjectVersion, ProviderError, PutRequest, SecretMaterial, SecretsProvider,
    StoredObject,
};

const NOT_FOUND: &str = "ResourceNotFoundException";

/// One release scoped AWS Secrets Manager store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AwsSecretsProvider {
    region: String,
    prefix: String,
    release: String,
}

impl AwsSecretsProvider {
    /// Build a provider after confirming AWS CLI v2 is available on `PATH`.
    pub fn new(
        region: impl Into<String>,
        prefix: impl Into<String>,
        release: impl Into<String>,
    ) -> Result<Self> {
        crate::ops::require_on_path("aws")?;
        require_aws_cli_v2()?;
        let provider = Self {
            region: region.into(),
            prefix: prefix.into(),
            release: release.into(),
        };
        provider.validate_configuration()?;
        Ok(provider)
    }

    fn validate_configuration(&self) -> Result<()> {
        if self.region.is_empty()
            || !self
                .region
                .chars()
                .all(|character| character.is_ascii_alphanumeric() || character == '-')
        {
            bail!("AWS Secrets Manager region is invalid");
        }
        if self.prefix.is_empty()
            || self.prefix.starts_with('/')
            || self.prefix.ends_with('/')
            || !self.prefix.chars().all(valid_path_character)
        {
            bail!("AWS Secrets Manager prefix is invalid");
        }
        if self.release.is_empty()
            || self.release.contains('/')
            || !self.release.chars().all(valid_path_character)
        {
            bail!("AWS Secrets Manager release is invalid");
        }
        if self.remote_prefix().len() >= 512 {
            bail!("AWS Secrets Manager prefix and release are too long");
        }
        Ok(())
    }

    fn remote_prefix(&self) -> String {
        format!("{}/{}/", self.prefix, self.release)
    }

    fn remote_name(&self, logical: &str) -> Result<String, ProviderError> {
        if logical.is_empty() || logical.contains('/') || !logical.chars().all(valid_path_character)
        {
            return Err(ProviderError::InvalidName {
                name: logical.to_string(),
            });
        }
        let remote = format!("{}{logical}", self.remote_prefix());
        if remote.len() > 512 {
            return Err(ProviderError::InvalidName {
                name: logical.to_string(),
            });
        }
        Ok(remote)
    }

    fn invoke(&self, logical: &str, operation: &str, input: &Value) -> ProviderResult<Value> {
        let mut request = tempfile::NamedTempFile::new().map_err(|_| {
            ProviderFailure::provider(ProviderError::Unavailable {
                name: logical.to_string(),
                status: -1,
            })
        })?;
        set_private_permissions(request.as_file()).map_err(|_| {
            ProviderFailure::provider(ProviderError::Unavailable {
                name: logical.to_string(),
                status: -1,
            })
        })?;
        serde_json::to_writer(request.as_file_mut(), input).map_err(|_| {
            ProviderFailure::provider(ProviderError::Unavailable {
                name: logical.to_string(),
                status: -1,
            })
        })?;
        request.as_file_mut().flush().map_err(|_| {
            ProviderFailure::provider(ProviderError::Unavailable {
                name: logical.to_string(),
                status: -1,
            })
        })?;
        let input_path = format!("file://{}", request.path().display());
        let output = Command::new("aws")
            .args([
                "secretsmanager",
                operation,
                "--cli-input-json",
                &input_path,
                "--region",
                &self.region,
                "--output",
                "json",
                "--no-cli-pager",
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .map_err(|_| {
                ProviderFailure::provider(ProviderError::Unavailable {
                    name: logical.to_string(),
                    status: -1,
                })
            })?;
        parse_output(logical, output)
    }

    fn describe(&self, logical: &str) -> Result<Description, ProviderError> {
        let remote = self.remote_name(logical)?;
        let response = self
            .invoke(logical, "describe-secret", &json!({ "SecretId": remote }))
            .map_err(ProviderFailure::into_provider)?;
        let current_version = response
            .get("VersionIdsToStages")
            .and_then(Value::as_object)
            .and_then(|versions| {
                versions.iter().find_map(|(version, stages)| {
                    stages
                        .as_array()
                        .is_some_and(|stages| stages.iter().any(|stage| stage == "AWSCURRENT"))
                        .then(|| version.clone())
                })
            });
        let tags = response
            .get("Tags")
            .and_then(Value::as_array)
            .map(|tags| {
                tags.iter()
                    .filter_map(|tag| {
                        Some((
                            tag.get("Key")?.as_str()?.to_string(),
                            tag.get("Value")?.as_str()?.to_string(),
                        ))
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(Description {
            current_version,
            tags,
        })
    }

    fn check_expected(
        &self,
        logical: &str,
        expected: Option<&str>,
    ) -> Result<Option<Description>, ProviderError> {
        let description = match self.describe(logical) {
            Ok(description) => Some(description),
            Err(ProviderError::NotFound { .. }) => None,
            Err(error) => return Err(error),
        };
        if let Some(expected) = expected {
            let actual = description
                .as_ref()
                .and_then(|description| description.current_version.as_deref());
            if actual != Some(expected) {
                return Err(ProviderError::Conflict {
                    name: logical.to_string(),
                    expected_version: Some(expected.to_string()),
                    actual_version: actual.map(str::to_string),
                });
            }
        }
        Ok(description)
    }

    fn get_value(&self, logical: &str, version: Option<&str>) -> Result<Value, ProviderError> {
        let remote = self.remote_name(logical)?;
        let mut input = Map::new();
        input.insert("SecretId".to_string(), Value::String(remote));
        if let Some(version) = version {
            input.insert("VersionId".to_string(), Value::String(version.to_string()));
        }
        self.invoke(logical, "get-secret-value", &Value::Object(input))
            .map_err(ProviderFailure::into_provider)
    }
}

impl SecretsProvider for AwsSecretsProvider {
    fn put(&self, request: &PutRequest<'_>) -> Result<ObjectVersion, ProviderError> {
        let remote = self.remote_name(request.name)?;
        let existing = self.check_expected(request.name, request.expected_version)?;
        let input = if existing.is_some() {
            json!({
                "SecretId": remote,
                "SecretString": request.material.expose(),
            })
        } else {
            json!({
                "Name": remote,
                "SecretString": request.material.expose(),
            })
        };
        let operation = if existing.is_some() {
            "put-secret-value"
        } else {
            "create-secret"
        };
        let response = self
            .invoke(request.name, operation, &input)
            .map_err(ProviderFailure::into_provider)?;
        response_version(request.name, &response)
    }

    fn create(
        &self,
        name: &str,
        material: &SecretMaterial,
    ) -> Result<ObjectVersion, ProviderError> {
        // `create-secret` alone: Secrets Manager refuses an existing name with
        // ResourceExistsException, so two racing creators cannot overwrite.
        let remote = self.remote_name(name)?;
        let input = json!({
            "Name": remote,
            "SecretString": material.expose(),
        });
        let response = self
            .invoke(name, "create-secret", &input)
            .map_err(ProviderFailure::into_provider)?;
        response_version(name, &response)
    }

    fn get(&self, name: &str, version: Option<&str>) -> Result<StoredObject, ProviderError> {
        let response = self.get_value(name, version)?;
        let version = response
            .get("VersionId")
            .and_then(Value::as_str)
            .ok_or_else(|| unavailable(name))?;
        let material = response
            .get("SecretString")
            .and_then(Value::as_str)
            .ok_or_else(|| unavailable(name))?;
        let key_names = json_key_names(name, material)?;
        Ok(StoredObject {
            version: ObjectVersion {
                id: version.to_string(),
            },
            material: SecretMaterial::new(material),
            key_names,
        })
    }

    fn get_metadata(&self, name: &str) -> Result<ObjectMetadata, ProviderError> {
        let description = self.describe(name)?;
        let stored = self.get(name, None)?;
        Ok(ObjectMetadata {
            name: name.to_string(),
            version: stored.version,
            tags: description.tags,
            key_names: stored.key_names,
        })
    }

    fn list(&self, prefix: &str) -> Result<Vec<ObjectMetadata>, ProviderError> {
        if prefix.contains('/') || !prefix.chars().all(valid_path_character) {
            return Err(ProviderError::InvalidName {
                name: prefix.to_string(),
            });
        }
        let remote_prefix = self.remote_prefix();
        let response = self
            .invoke(
                prefix,
                "list-secrets",
                &json!({
                    "Filters": [{ "Key": "name", "Values": [remote_prefix] }],
                }),
            )
            .map_err(ProviderFailure::into_provider)?;
        let mut logical_names: Vec<String> = response
            .get("SecretList")
            .and_then(Value::as_array)
            .ok_or_else(|| unavailable(prefix))?
            .iter()
            .filter_map(|entry| entry.get("Name").and_then(Value::as_str))
            .filter_map(|name| name.strip_prefix(&remote_prefix))
            .filter(|name| !name.contains('/') && name.starts_with(prefix))
            .map(str::to_string)
            .collect();
        logical_names.sort();
        logical_names.dedup();
        logical_names
            .iter()
            .map(|name| self.get_metadata(name))
            .collect()
    }

    fn tag(
        &self,
        name: &str,
        tags: &BTreeMap<String, String>,
        expected_version: Option<&str>,
    ) -> Result<ObjectVersion, ProviderError> {
        let remote = self.remote_name(name)?;
        let description = self
            .check_expected(name, expected_version)?
            .ok_or_else(|| ProviderError::NotFound {
                name: name.to_string(),
            })?;
        let current = description
            .current_version
            .ok_or_else(|| unavailable(name))?;
        let tags: Vec<Value> = tags
            .iter()
            .map(|(key, value)| json!({ "Key": key, "Value": value }))
            .collect();
        self.invoke(
            name,
            "tag-resource",
            &json!({ "SecretId": remote, "Tags": tags }),
        )
        .map_err(ProviderFailure::into_provider)?;
        Ok(ObjectVersion { id: current })
    }

    fn delete(
        &self,
        name: &str,
        expected_version: Option<&str>,
    ) -> Result<ObjectVersion, ProviderError> {
        let remote = self.remote_name(name)?;
        let description = self
            .check_expected(name, expected_version)?
            .ok_or_else(|| ProviderError::NotFound {
                name: name.to_string(),
            })?;
        let current = description
            .current_version
            .ok_or_else(|| unavailable(name))?;
        self.invoke(
            name,
            "delete-secret",
            &json!({ "SecretId": remote, "ForceDeleteWithoutRecovery": true }),
        )
        .map_err(ProviderFailure::into_provider)?;
        Ok(ObjectVersion { id: current })
    }
}

#[derive(Debug)]
struct Description {
    current_version: Option<String>,
    tags: BTreeMap<String, String>,
}

enum ProviderFailure {
    Provider(ProviderError),
}

impl ProviderFailure {
    fn provider(error: ProviderError) -> Self {
        Self::Provider(error)
    }

    fn into_provider(self) -> ProviderError {
        match self {
            Self::Provider(error) => error,
        }
    }
}

type ProviderResult<T> = std::result::Result<T, ProviderFailure>;

fn parse_output(logical: &str, output: Output) -> ProviderResult<Value> {
    if output.status.success() {
        if output.stdout.iter().all(u8::is_ascii_whitespace) {
            return Ok(Value::Null);
        }
        return serde_json::from_slice(&output.stdout).map_err(|_| {
            ProviderFailure::provider(ProviderError::Unavailable {
                name: logical.to_string(),
                status: -1,
            })
        });
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    if stderr.contains("ResourceExistsException") {
        return Err(ProviderFailure::provider(ProviderError::Conflict {
            name: logical.to_string(),
            expected_version: None,
            actual_version: None,
        }));
    }
    if stderr.contains(NOT_FOUND) {
        return Err(ProviderFailure::provider(ProviderError::NotFound {
            name: logical.to_string(),
        }));
    }
    Err(ProviderFailure::provider(ProviderError::Unavailable {
        name: logical.to_string(),
        status: output.status.code().unwrap_or(-1),
    }))
}

fn response_version(name: &str, response: &Value) -> Result<ObjectVersion, ProviderError> {
    response
        .get("VersionId")
        .and_then(Value::as_str)
        .map(|id| ObjectVersion { id: id.to_string() })
        .ok_or_else(|| unavailable(name))
}

fn json_key_names(name: &str, material: &str) -> Result<Vec<String>, ProviderError> {
    let value: Value = serde_json::from_str(material).map_err(|_| unavailable(name))?;
    let object = value.as_object().ok_or_else(|| unavailable(name))?;
    Ok(object.keys().cloned().collect())
}

fn unavailable(name: &str) -> ProviderError {
    ProviderError::Unavailable {
        name: name.to_string(),
        status: -1,
    }
}

fn require_aws_cli_v2() -> Result<()> {
    let output = Command::new("aws")
        .arg("--version")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(|_| anyhow::anyhow!("could not inspect the installed AWS CLI version"))?;
    let is_v2 = output.status.success()
        && (String::from_utf8_lossy(&output.stdout).contains("aws-cli/2.")
            || String::from_utf8_lossy(&output.stderr).contains("aws-cli/2."));
    if !is_v2 {
        bail!("`aws` must be AWS CLI v2; install AWS CLI v2 and retry");
    }
    Ok(())
}

fn valid_path_character(character: char) -> bool {
    character.is_ascii_alphanumeric() || "/_+=.@-".contains(character)
}

fn set_private_permissions(file: &fs::File) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        file.set_permissions(fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

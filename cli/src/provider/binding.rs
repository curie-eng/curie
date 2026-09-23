//! Inventory rows these cluster writers store, and JSON key merges on top of
//! [`SecretsProvider`].

use std::collections::BTreeMap;

use anyhow::{bail, Context, Result};

use super::{platform_inventory, ProviderError, PutRequest, SecretMaterial, SecretsProvider};

/// One inventory row rendered for a release: the object name, the JSON key,
/// the Secret the chart should reference, and the two helm knobs that do it.
pub struct HelmRef {
    pub logical_name: &'static str,
    pub key: String,
    pub target: String,
    pub secret_knob: String,
    pub key_knob: String,
}

pub fn helm_ref(logical_name: &'static str, release: &str) -> Result<HelmRef> {
    let entries = platform_inventory()?;
    let entry = entries
        .into_iter()
        .find(|entry| entry.logical_name == logical_name)
        .with_context(|| format!("platform inventory has no {logical_name}"))?;
    let key = entry
        .keys
        .into_iter()
        .next()
        .with_context(|| format!("inventory entry {logical_name} has no key"))?;
    let knob = entry
        .chart
        .and_then(|chart| chart.knobs.into_iter().next())
        .with_context(|| format!("inventory entry {logical_name} has no chart knob"))?;
    let key_knob = knob
        .key
        .with_context(|| format!("inventory entry {logical_name} knob has no key path"))?;
    let target = entry.target.replace("{release}", release);
    if target.contains('{') {
        bail!("inventory target for {logical_name} needs more than the release name");
    }
    Ok(HelmRef {
        logical_name,
        key,
        target,
        secret_knob: knob.secret,
        key_knob,
    })
}

/// Insert one JSON key into an object, creating it when absent. Sibling keys
/// stay. The write uses the current version as a precondition.
pub fn merge_json_key(
    provider: &dyn SecretsProvider,
    name: &str,
    key: &str,
    value: &str,
) -> Result<()> {
    if value.is_empty() {
        bail!("refusing to store an empty value for {name}/{key}");
    }
    let (mut values, expected) = match provider.get(name, None) {
        Ok(stored) => {
            let values: BTreeMap<String, String> =
                serde_json::from_str(stored.material.expose())
                    .with_context(|| format!("provider object {name} is not a JSON string map"))?;
            (values, Some(stored.version.id))
        }
        Err(ProviderError::NotFound { .. }) => (BTreeMap::new(), None),
        Err(error) => return Err(error.into()),
    };
    values.insert(key.to_string(), value.to_string());
    let material =
        SecretMaterial::new(serde_json::to_string(&values).context("serializing provider object")?);
    provider.put(&PutRequest {
        name,
        material: &material,
        expected_version: expected.as_deref(),
    })?;
    Ok(())
}

/// Drop one JSON key. A missing object or key is success. An object with no
/// keys left is deleted.
pub fn remove_json_key(provider: &dyn SecretsProvider, name: &str, key: &str) -> Result<()> {
    let (mut values, expected) = match provider.get(name, None) {
        Ok(stored) => {
            let values: BTreeMap<String, String> =
                serde_json::from_str(stored.material.expose())
                    .with_context(|| format!("provider object {name} is not a JSON string map"))?;
            (values, stored.version.id)
        }
        Err(ProviderError::NotFound { .. }) => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    if values.remove(key).is_none() {
        return Ok(());
    }
    if values.is_empty() {
        // Keep the object. Deleting it schedules a recovery window, and the
        // name cannot be created again until that window ends.
        let material = SecretMaterial::new("{}".to_string());
        provider.put(&PutRequest {
            name,
            material: &material,
            expected_version: Some(&expected),
        })?;
        return Ok(());
    }
    let material =
        SecretMaterial::new(serde_json::to_string(&values).context("serializing provider object")?);
    provider.put(&PutRequest {
        name,
        material: &material,
        expected_version: Some(&expected),
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    use super::super::{
        ObjectMetadata, ObjectVersion, ProviderError, PutRequest, SecretMaterial, SecretsProvider,
        StoredObject,
    };
    use super::remove_json_key;

    struct Memory {
        objects: Mutex<BTreeMap<String, String>>,
        deleted: Mutex<Vec<String>>,
    }

    impl SecretsProvider for Memory {
        fn put(&self, request: &PutRequest<'_>) -> Result<ObjectVersion, ProviderError> {
            self.objects.lock().expect("objects").insert(
                request.name.to_string(),
                request.material.expose().to_string(),
            );
            Ok(ObjectVersion {
                id: "v".to_string(),
            })
        }

        fn get(&self, name: &str, _version: Option<&str>) -> Result<StoredObject, ProviderError> {
            let objects = self.objects.lock().expect("objects");
            let material = objects.get(name).ok_or_else(|| ProviderError::NotFound {
                name: name.to_string(),
            })?;
            Ok(StoredObject {
                version: ObjectVersion {
                    id: "v".to_string(),
                },
                material: SecretMaterial::new(material.clone()),
                key_names: Vec::new(),
            })
        }

        fn get_metadata(&self, name: &str) -> Result<ObjectMetadata, ProviderError> {
            let _ = name;
            Err(ProviderError::Unavailable {
                name: name.to_string(),
                status: -1,
            })
        }

        fn list(&self, _prefix: &str) -> Result<Vec<ObjectMetadata>, ProviderError> {
            Ok(Vec::new())
        }

        fn tag(
            &self,
            _name: &str,
            _tags: &BTreeMap<String, String>,
            _expected_version: Option<&str>,
        ) -> Result<ObjectVersion, ProviderError> {
            Ok(ObjectVersion {
                id: "v".to_string(),
            })
        }

        fn delete(
            &self,
            name: &str,
            _expected_version: Option<&str>,
        ) -> Result<ObjectVersion, ProviderError> {
            self.deleted.lock().expect("deleted").push(name.to_string());
            Ok(ObjectVersion {
                id: "v".to_string(),
            })
        }
    }

    #[test]
    fn removing_the_last_key_keeps_an_empty_object() {
        let provider = Memory {
            objects: Mutex::new(BTreeMap::from([(
                "github-app-private-key".to_string(),
                r#"{"githubAppPrivateKey":"pem"}"#.to_string(),
            )])),
            deleted: Mutex::new(Vec::new()),
        };
        remove_json_key(&provider, "github-app-private-key", "githubAppPrivateKey")
            .expect("remove");
        assert!(provider.deleted.lock().expect("deleted").is_empty());
        assert_eq!(
            provider
                .objects
                .lock()
                .expect("objects")
                .get("github-app-private-key")
                .map(String::as_str),
            Some("{}")
        );
    }
}

//! One-shot offline migration of protected-worker process settings.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Process {
    schema_version: u64,
    database_url: String,
    db_schema: String,
    api_base_url: String,
    internal_worker_token: String,
    #[serde(deserialize_with = "unique_map")]
    adapter_credentials: BTreeMap<String, String>,
    slack: Slack,
    launch_tls: LaunchTls,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Slack {
    token: String,
    base_url: String,
    trusted_origins: Vec<String>,
    #[serde(deserialize_with = "unique_map")]
    identity_tokens: BTreeMap<String, String>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Gateway {
    url: String,
    token: String,
    allow_insecure_http: bool,
    identities: Vec<String>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct LaunchTls {
    origin: String,
    ca_file: String,
    server_name: String,
    server_spki_sha256: String,
    client_certificate: String,
    client_key: String,
    worker_principal_id: String,
}

pub struct MigrationOutput;

impl crate::ui::CliOutput for MigrationOutput {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({"schema_version": 2, "migrated": true})
    }

    fn render(&self, ui: &crate::ui::Ui) {
        ui.payload_plain("Migrated process.json to v2; raw Slack credentials removed.");
    }
}

fn origin(raw: &str, path: bool) -> Result<reqwest::Url> {
    let url = reqwest::Url::parse(raw)?;
    ensure!(matches!(url.scheme(), "http" | "https") && url.host_str().is_some());
    ensure!(url.username().is_empty() && url.password().is_none());
    ensure!(url.query().is_none() && url.fragment().is_none());
    ensure!(
        path || !raw
            .split_once("://")
            .ok_or_else(|| anyhow::anyhow!("origin"))?
            .1
            .contains('/')
    );
    Ok(url)
}

fn unique_map<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<BTreeMap<String, String>, D::Error> {
    struct Unique;
    impl<'de> serde::de::Visitor<'de> for Unique {
        type Value = BTreeMap<String, String>;
        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("an object with unique keys")
        }
        fn visit_map<A: serde::de::MapAccess<'de>>(
            self,
            mut input: A,
        ) -> std::result::Result<Self::Value, A::Error> {
            let mut values = BTreeMap::new();
            while let Some((key, value)) = input.next_entry::<String, String>()? {
                if values.insert(key, value).is_some() {
                    return Err(serde::de::Error::custom("duplicate key"));
                }
            }
            Ok(values)
        }
    }
    deserializer.deserialize_map(Unique)
}

fn credentials(values: &BTreeMap<String, String>) -> bool {
    values
        .iter()
        .all(|(key, value)| !key.is_empty() && !value.is_empty())
}

fn read(path: &Path) -> Result<Vec<u8>> {
    // Refuse symlinks in every component, including the final input file.
    let absolute = std::path::absolute(path)?;
    let mut current = std::path::PathBuf::new();
    for part in absolute.components() {
        ensure!(!matches!(part, std::path::Component::ParentDir));
        current.push(part);
        ensure!(!std::fs::symlink_metadata(&current)?
            .file_type()
            .is_symlink());
    }
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    let before = file.metadata()?;
    ensure!(before.is_file() && before.len() > 0 && before.len() <= 65536);
    let mut bytes = Vec::new();
    (&mut file).take(65537).read_to_end(&mut bytes)?;
    ensure!(bytes.len() <= 65536 && bytes.len() as u64 == before.len());
    ensure!(before.modified()? == file.metadata()?.modified()?);
    Ok(bytes)
}

fn migrate(runtime: &Path, gateway_path: &Path) -> Result<()> {
    let path = runtime.join("process.json");
    let source: Process = serde_json::from_slice(&read(&path)?)?;
    let gateway: Gateway = serde_json::from_slice(&read(gateway_path)?)?;
    ensure!(source.schema_version == 1);
    ensure!(reqwest::Url::parse(&source.database_url)?.scheme() == "postgresql+asyncpg");
    ensure!(!source.db_schema.is_empty() && source.db_schema.len() <= 63);
    ensure!(source
        .db_schema
        .bytes()
        .enumerate()
        .all(|(i, c)| c == b'_' || c.is_ascii_alphabetic() || (i > 0 && c.is_ascii_digit())));
    origin(&source.api_base_url, false)?;
    ensure!(!source.internal_worker_token.is_empty() && credentials(&source.adapter_credentials));
    ensure!(credentials(&source.slack.identity_tokens));
    let identities: BTreeSet<_> = gateway.identities.iter().collect();
    ensure!(
        identities.len() == gateway.identities.len() && identities.iter().all(|v| !v.is_empty())
    );
    ensure!(source
        .slack
        .identity_tokens
        .keys()
        .all(|key| identities.contains(key)));
    ensure!(source.slack.token.is_empty() || identities.contains(&"default".to_owned()));
    if gateway.url.is_empty() && gateway.token.is_empty() {
        ensure!(identities.is_empty());
    } else {
        let endpoint = origin(&gateway.url, true)?;
        ensure!(endpoint.scheme() == "https" || gateway.allow_insecure_http);
        ensure!(
            !gateway.token.trim().is_empty()
                && !["xoxb-", "xoxp-", "xapp-"]
                    .iter()
                    .any(|prefix| gateway.token.starts_with(prefix))
        );
    }
    let tls = &source.launch_tls;
    ensure!(origin(&tls.origin, false)?.scheme() == "https");
    ensure!(
        !tls.ca_file.is_empty() && !tls.client_certificate.is_empty() && !tls.client_key.is_empty()
    );
    ensure!(!tls.server_name.is_empty() && !tls.server_name.contains(['\r', '\n', '\0']));
    ensure!(
        tls.server_spki_sha256.len() == 64
            && tls
                .server_spki_sha256
                .bytes()
                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
    );
    ensure!(
        !tls.worker_principal_id.is_empty()
            && tls.worker_principal_id.len() <= 256
            && tls
                .worker_principal_id
                .bytes()
                .enumerate()
                .all(|(i, c)| c.is_ascii_alphanumeric() || (i > 0 && b"._:/@+-".contains(&c)))
    );
    let mut value = serde_json::to_value(&source)?;
    let map = value.as_object_mut().expect("serialized process object");
    map.remove("slack");
    map.insert("schema_version".into(), 2.into());
    map.insert("channel_gateway".into(), serde_json::to_value(gateway)?);
    let mut candidate = tempfile::NamedTempFile::new_in(runtime)?;
    candidate
        .as_file()
        .set_permissions(std::os::unix::fs::PermissionsExt::from_mode(0o600))?;
    let bytes = serde_json::to_vec_pretty(&value)?;
    ensure!(bytes.len() < 65536);
    candidate.write_all(&bytes)?;
    candidate.write_all(b"\n")?;
    candidate.as_file().sync_all()?;
    candidate.persist(&path)?;
    File::open(runtime)?.sync_all()?;
    Ok(())
}

pub fn run(runtime: &Path, gateway_path: &Path) -> Result<MigrationOutput> {
    migrate(runtime, gateway_path).map_err(|_| {
        crate::exit::CliError::failure("Protected process migration refused")
            .with_fix("Supply a valid process.json v1 and a TLS gateway configuration covering every existing Slack identity; run curie migrate-protected-process --runtime-dir <directory> --gateway-config <gateway.json>.")
    })?;
    Ok(MigrationOutput)
}

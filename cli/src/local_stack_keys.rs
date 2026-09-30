//! Per-install credentials for the local compose stack (#3557).
//!
//! The dev compose file used to hardcode the platform API key (`curie-dev-key`)
//! and the Postgres password (`postgres`). Both are well-known, so any process
//! that could reach a published port could drive the API or the database.
//! `curie local up` now generates both per install, stores them next to the rest
//! of the CLI's private state, and hands them to compose as masked secret env.
//! Compose interpolates them as `CURIE_LOCAL_API_KEY` and
//! `CURIE_LOCAL_POSTGRES_PASSWORD`, falling back to the old literals so a raw
//! `docker compose up` (CI, the restore drill) keeps working.
//!
//! The store is one JSON object per compose project at
//! `<config dir>/local/<project>.json`: directory 0700, file 0600, the same
//! private-write path `curie secrets set` uses. A project is a separate stack
//! with its own volumes, so it gets its own keys.
//!
//! Verbs never need to know the key: `message::api_key_or_default` sends the
//! stored key whenever the operator supplied none.

use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

/// Compose interpolation variable carrying the install's platform API key.
pub const API_KEY_ENV: &str = "CURIE_LOCAL_API_KEY";
/// Compose interpolation variable carrying the install's Postgres password.
pub const POSTGRES_PASSWORD_ENV: &str = "CURIE_LOCAL_POSTGRES_PASSWORD";
/// The Postgres password every install made before #3557 initialized its
/// volume with. Kept for such a volume, see [`resolve_in`].
pub const LEGACY_POSTGRES_PASSWORD: &str = "postgres";

/// Bytes of OS randomness behind each generated credential (64 hex chars).
const GENERATED_BYTES: usize = 32;

/// The two credentials one local install runs with.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalStackCredentials {
    pub api_key: String,
    pub postgres_password: String,
}

impl std::fmt::Debug for LocalStackCredentials {
    // Never print a credential, even from a failed assertion.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalStackCredentials")
            .field("api_key", &"<redacted>")
            .field("postgres_password", &"<redacted>")
            .finish()
    }
}

/// The credentials `local up` resolved, and where they live.
#[derive(Debug, Clone)]
pub struct ResolvedStackCredentials {
    pub credentials: LocalStackCredentials,
    /// The store path. It holds these values only when the run persisted them;
    /// a `--dry-run` resolves without writing.
    pub path: PathBuf,
}

/// The masked secret env a compose child needs to start the stack on these
/// credentials. Attach with `OpsCommand::with_secret_env`, never argv.
pub fn compose_secret_env(creds: &LocalStackCredentials) -> Vec<(String, String)> {
    vec![
        (API_KEY_ENV.to_string(), creds.api_key.clone()),
        (
            POSTGRES_PASSWORD_ENV.to_string(),
            creds.postgres_password.clone(),
        ),
    ]
}

/// `<config dir>/local/<project>.json`. The project name becomes a file name,
/// so anything that could leave the directory is refused.
pub fn path_in(config_dir: &Path, project: &str) -> Result<PathBuf> {
    if project.is_empty()
        || project.starts_with('.')
        || project.contains(['/', '\\'])
        || project.contains('\0')
    {
        bail!("compose project name {project:?} cannot name a local credential file");
    }
    Ok(config_dir.join("local").join(format!("{project}.json")))
}

/// The store path for `project` under the CLI's config dir.
pub fn path(project: &str) -> Result<PathBuf> {
    path_in(&crate::secrets::config_dir()?, project)
}

/// Read the stored credentials for `project` from `config_dir`. `Ok(None)` means
/// no install has stored any; a file that exists but does not parse, or holds an
/// empty value, is an error so `local up` never silently replaces a password the
/// database already holds.
pub fn load_in(config_dir: &Path, project: &str) -> Result<Option<LocalStackCredentials>> {
    let path = path_in(config_dir, project)?;
    let raw = match std::fs::read_to_string(&path) {
        Ok(raw) => raw,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            return Err(e)
                .with_context(|| format!("reading local stack credentials {}", path.display()))
        }
    };
    let creds: LocalStackCredentials = serde_json::from_str(&raw)
        .with_context(|| format!("parsing local stack credentials {}", path.display()))?;
    if creds.api_key.is_empty() || creds.postgres_password.is_empty() {
        bail!(
            "local stack credentials {} hold an empty value; remove the file only if the \
             stack's volumes are gone too",
            path.display()
        );
    }
    Ok(Some(creds))
}

/// [`load_in`] under the CLI's config dir.
pub fn load(project: &str) -> Result<Option<LocalStackCredentials>> {
    load_in(&crate::secrets::config_dir()?, project)
}

/// The pure core of [`resolve_for_up`]: every input is passed in, so it is
/// testable without a docker daemon or this process's environment.
///
/// - Stored credentials are reused, so a second `up` starts the same install.
/// - With nothing stored, both are generated. The exception is a Postgres
///   volume that already exists: Postgres applies `POSTGRES_PASSWORD` only when
///   it initializes an empty data dir, so that volume still expects the legacy
///   `postgres` and a new password would lock the install out of its own data.
///   `postgres_volume_exists` is only consulted in that case.
/// - A non-empty `explicit_api_key` (the operator's `CURIE_API_KEY`) becomes the
///   install's key. Verbs already prefer that env var, so a stack on any other
///   key would answer the operator with 401.
/// - `persist` false (`--dry-run`) resolves the same answer and writes nothing.
pub fn resolve_in(
    config_dir: &Path,
    project: &str,
    explicit_api_key: Option<&str>,
    postgres_volume_exists: impl FnOnce() -> bool,
    persist: bool,
) -> Result<ResolvedStackCredentials> {
    let path = path_in(config_dir, project)?;
    let explicit = explicit_api_key.filter(|key| !key.is_empty());
    let stored = load_in(config_dir, project)?;
    let (mut credentials, mut changed) = match stored {
        Some(creds) => (creds, false),
        None => {
            let postgres_password = if postgres_volume_exists() {
                LEGACY_POSTGRES_PASSWORD.to_string()
            } else {
                crate::ops::random_hex(GENERATED_BYTES)?
            };
            let api_key = match explicit {
                Some(key) => key.to_string(),
                None => crate::ops::random_hex(GENERATED_BYTES)?,
            };
            (
                LocalStackCredentials {
                    api_key,
                    postgres_password,
                },
                true,
            )
        }
    };
    if let Some(key) = explicit {
        if credentials.api_key != key {
            credentials.api_key = key.to_string();
            changed = true;
        }
    }
    if persist && changed {
        store(&path, &credentials)?;
    }
    Ok(ResolvedStackCredentials { credentials, path })
}

/// Resolve the credentials a compose `up` for `project` starts the stack on,
/// reading the operator's `CURIE_API_KEY` and, only when nothing is stored,
/// probing docker for the project's Postgres volume. See [`resolve_in`].
pub async fn resolve_for_up(project: &str, persist: bool) -> Result<ResolvedStackCredentials> {
    let config_dir = crate::secrets::config_dir()?;
    let explicit = std::env::var("CURIE_API_KEY").ok();
    let volume_exists = if load_in(&config_dir, project)?.is_none() {
        postgres_volume_exists(project).await
    } else {
        false
    };
    resolve_in(
        &config_dir,
        project,
        explicit.as_deref(),
        || volume_exists,
        persist,
    )
}

/// The stored API key for `project`, for verbs whose `--api-key` fell back to
/// the dev sentinel. Any failure (no file, unreadable file, no config dir)
/// answers `None` so the caller keeps its sentinel rather than failing a parse.
pub fn stored_api_key(project: &str) -> Option<String> {
    load(project).ok().flatten().map(|creds| creds.api_key)
}

/// Whether docker holds `<project>_postgres_data`, the named volume compose
/// creates for the stack's database. A missing docker, a stopped daemon, or any
/// other failure reads as "no volume".
async fn postgres_volume_exists(project: &str) -> bool {
    tokio::process::Command::new("docker")
        .args(["volume", "inspect", &format!("{project}_postgres_data")])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .await
        .map(|status| status.success())
        .unwrap_or(false)
}

/// Write the store 0600 inside a 0700 directory.
fn store(path: &Path, creds: &LocalStackCredentials) -> Result<()> {
    let dir = path
        .parent()
        .context("local stack credential path has no parent directory")?;
    std::fs::create_dir_all(dir)
        .with_context(|| format!("creating local credential dir {}", dir.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
            .with_context(|| format!("securing local credential dir {}", dir.display()))?;
    }
    let body = serde_json::to_vec_pretty(creds).context("serializing local stack credentials")?;
    crate::secrets::write_private(path, &body)
}

#[cfg(test)]
mod tests {
    use super::*;

    const PLACEHOLDER_KEY: &str = "explicit-placeholder-api-key";

    fn no_volume() -> bool {
        false
    }

    fn is_hex64(value: &str) -> bool {
        value.len() == 64 && value.chars().all(|c| c.is_ascii_hexdigit())
    }

    #[test]
    fn fresh_install_generates_distinct_64_hex_credentials() {
        let dir = tempfile::tempdir().unwrap();
        let a = resolve_in(dir.path(), "a", None, no_volume, false).unwrap();
        let b = resolve_in(dir.path(), "b", None, no_volume, false).unwrap();
        let (a, b) = (a.credentials, b.credentials);
        for value in [&a.api_key, &a.postgres_password, &b.api_key] {
            assert!(
                is_hex64(value),
                "expected 64 hex chars, got {} chars",
                value.len()
            );
        }
        assert_ne!(a.api_key, a.postgres_password);
        assert_ne!(a.api_key, b.api_key, "two installs must not share a key");
        assert_ne!(a.api_key, crate::message::DEFAULT_API_KEY);
    }

    #[test]
    fn persisted_credentials_are_private_and_reload_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let first = resolve_in(dir.path(), "curie", None, no_volume, true).unwrap();
        assert_eq!(first.path, dir.path().join("local").join("curie.json"));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let file_mode = std::fs::metadata(&first.path).unwrap().permissions().mode();
            assert_eq!(file_mode & 0o777, 0o600);
            let dir_mode = std::fs::metadata(first.path.parent().unwrap())
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(dir_mode & 0o777, 0o700);
        }
        assert_eq!(
            load_in(dir.path(), "curie").unwrap(),
            Some(first.credentials.clone())
        );
        // A second `up` reuses them and never consults the volume probe.
        let second = resolve_in(
            dir.path(),
            "curie",
            None,
            || panic!("probe must not run when credentials are stored"),
            true,
        )
        .unwrap();
        assert_eq!(second.credentials, first.credentials);
    }

    #[test]
    fn an_existing_postgres_volume_keeps_the_legacy_password() {
        let dir = tempfile::tempdir().unwrap();
        let resolved = resolve_in(dir.path(), "curie", None, || true, true).unwrap();
        assert_eq!(
            resolved.credentials.postgres_password,
            LEGACY_POSTGRES_PASSWORD
        );
        assert!(
            is_hex64(&resolved.credentials.api_key),
            "the API key is still randomized for a legacy volume"
        );
        assert_eq!(
            load_in(dir.path(), "curie").unwrap(),
            Some(resolved.credentials)
        );
    }

    #[test]
    fn an_explicit_key_becomes_the_install_key_and_persists() {
        let dir = tempfile::tempdir().unwrap();
        let fresh = resolve_in(dir.path(), "curie", Some(PLACEHOLDER_KEY), no_volume, true)
            .unwrap()
            .credentials;
        assert_eq!(fresh.api_key, PLACEHOLDER_KEY);
        assert!(is_hex64(&fresh.postgres_password));
        assert_eq!(load_in(dir.path(), "curie").unwrap(), Some(fresh.clone()));

        // Over a stored install it replaces only the key; the database keeps
        // the password it was initialized with.
        let replaced = resolve_in(
            dir.path(),
            "curie",
            Some("second-placeholder-key"),
            no_volume,
            true,
        )
        .unwrap()
        .credentials;
        assert_eq!(replaced.api_key, "second-placeholder-key");
        assert_eq!(replaced.postgres_password, fresh.postgres_password);
        assert_eq!(load_in(dir.path(), "curie").unwrap(), Some(replaced));
    }

    #[test]
    fn an_empty_explicit_key_is_absent() {
        let dir = tempfile::tempdir().unwrap();
        let resolved = resolve_in(dir.path(), "curie", Some(""), no_volume, false).unwrap();
        assert!(is_hex64(&resolved.credentials.api_key));
    }

    #[test]
    fn dry_run_writes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let resolved =
            resolve_in(dir.path(), "curie", Some(PLACEHOLDER_KEY), no_volume, false).unwrap();
        assert_eq!(resolved.credentials.api_key, PLACEHOLDER_KEY);
        assert!(!resolved.path.exists());
        assert!(!dir.path().join("local").exists());
    }

    #[test]
    fn a_corrupt_store_is_an_error_not_a_regeneration() {
        let dir = tempfile::tempdir().unwrap();
        let path = path_in(dir.path(), "curie").unwrap();
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, "{not json").unwrap();
        assert!(resolve_in(dir.path(), "curie", None, no_volume, true).is_err());
        std::fs::write(&path, r#"{"api_key":"","postgres_password":"placeholder"}"#).unwrap();
        assert!(load_in(dir.path(), "curie").is_err());
    }

    #[test]
    fn a_project_name_cannot_escape_the_store() {
        let dir = tempfile::tempdir().unwrap();
        for bad in ["", "..", ".hidden", "a/b", "a\\b"] {
            assert!(path_in(dir.path(), bad).is_err(), "{bad:?} must be refused");
        }
        assert!(path_in(dir.path(), "curie-feature_1").is_ok());
    }

    #[test]
    fn compose_secret_env_names_both_variables() {
        let creds = LocalStackCredentials {
            api_key: "placeholder-api-key-value".into(),
            postgres_password: "placeholder-pg-password".into(),
        };
        assert_eq!(
            compose_secret_env(&creds),
            vec![
                (
                    API_KEY_ENV.to_string(),
                    "placeholder-api-key-value".to_string()
                ),
                (
                    POSTGRES_PASSWORD_ENV.to_string(),
                    "placeholder-pg-password".to_string()
                ),
            ]
        );
        assert!(!format!("{creds:?}").contains("placeholder"));
    }
}

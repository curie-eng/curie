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
//! Verbs never need to know the key: `api::ApiClient::new` sends the stored key
//! whenever the operator supplied none and the destination is loopback.
//!
//! Only `local up` generates, adopts `CURIE_API_KEY`, or writes the store.
//! `local rebuild` and `local comms` recreate services of a stack that is
//! already running, so they read the store and never change it.

use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

/// Compose interpolation variable carrying the install's platform API key.
pub const API_KEY_ENV: &str = "CURIE_LOCAL_API_KEY";
/// Compose interpolation variable carrying the install's Postgres password.
pub const POSTGRES_PASSWORD_ENV: &str = "CURIE_LOCAL_POSTGRES_PASSWORD";
/// The Postgres password every install made before #3557 initialized its
/// volume with. Kept for such a volume, see [`resolve_with`].
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

/// What the docker probe for a project's Postgres volume found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VolumeProbe {
    /// Docker answered and the volume exists.
    Exists,
    /// Docker answered and reported no such volume.
    Absent,
    /// Docker could not answer: not installed, daemon unreachable, or any
    /// other failure. The volume may well exist.
    Unknown,
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

/// The pure core of [`resolve_for_up`]: every input is passed in, so it is
/// testable without a docker daemon or this process's environment.
///
/// - `stored` credentials are reused, so a second `up` starts the same install.
/// - With nothing stored, both are generated. The exception is a Postgres
///   volume that already exists: Postgres applies `POSTGRES_PASSWORD` only when
///   it initializes an empty data dir, so that volume still expects the legacy
///   `postgres` and a new password would lock the install out of its own data.
///   `postgres_volume` is only needed in that case (`None` when `stored` is
///   `Some`). When docker cannot answer ([`VolumeProbe::Unknown`]) a persisting
///   run refuses and writes nothing, because guessing "absent" would store a
///   password the existing volume rejects. A `--dry-run` generates throwaway
///   values instead.
/// - A non-empty `explicit_api_key` (the operator's `CURIE_API_KEY`) becomes the
///   install's key. Verbs already prefer that env var, so a stack on any other
///   key would answer the operator with 401.
/// - `persist` false (`--dry-run`) resolves the same answer and writes nothing.
pub fn resolve_with(
    config_dir: &Path,
    project: &str,
    stored: Option<LocalStackCredentials>,
    explicit_api_key: Option<&str>,
    postgres_volume: Option<VolumeProbe>,
    persist: bool,
) -> Result<ResolvedStackCredentials> {
    let path = path_in(config_dir, project)?;
    let explicit = explicit_api_key.filter(|key| !key.is_empty());
    let mut changed = stored.is_none();
    let mut credentials = match stored {
        Some(creds) => creds,
        None => {
            let postgres_password = match postgres_volume {
                Some(VolumeProbe::Exists) => LEGACY_POSTGRES_PASSWORD.to_string(),
                Some(VolumeProbe::Absent) => crate::ops::random_hex(GENERATED_BYTES)?,
                Some(VolumeProbe::Unknown) if persist => bail!(
                    "could not ask docker whether compose project {project:?} already has a \
                     Postgres volume, so its database password cannot be chosen safely. Start \
                     Docker and re-run `curie local up`."
                ),
                Some(VolumeProbe::Unknown) => crate::ops::random_hex(GENERATED_BYTES)?,
                None => bail!("no stored credentials and no Postgres volume probe for {project:?}"),
            };
            LocalStackCredentials {
                api_key: crate::ops::random_hex(GENERATED_BYTES)?,
                postgres_password,
            }
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
/// probing docker for the project's Postgres volume. See [`resolve_with`].
pub async fn resolve_for_up(project: &str, persist: bool) -> Result<ResolvedStackCredentials> {
    let config_dir = crate::secrets::config_dir()?;
    let explicit = std::env::var("CURIE_API_KEY").ok();
    let stored = load_in(&config_dir, project)?;
    let volume = if stored.is_none() {
        Some(probe_postgres_volume(project).await)
    } else {
        None
    };
    resolve_with(
        &config_dir,
        project,
        stored,
        explicit.as_deref(),
        volume,
        persist,
    )
}

/// The masked compose secret env for a verb that recreates services of an
/// already-running stack (`local rebuild`, `local comms`): the stored
/// credentials, or none at all when nothing is stored, in which case compose's
/// fallbacks match what a stack started before the store existed runs. Never
/// adopts `CURIE_API_KEY`, generates, or writes; only `local up` does.
pub fn running_stack_secret_env_in(
    config_dir: &Path,
    project: &str,
) -> Result<Vec<(String, String)>> {
    Ok(load_in(config_dir, project)?
        .as_ref()
        .map(compose_secret_env)
        .unwrap_or_default())
}

/// [`running_stack_secret_env_in`] under the CLI's config dir.
pub fn running_stack_secret_env(project: &str) -> Result<Vec<(String, String)>> {
    running_stack_secret_env_in(&crate::secrets::config_dir()?, project)
}

/// The stored API key for `project`, for verbs whose `--api-key` fell back to
/// the dev sentinel. Any failure (no file, unreadable file, no config dir)
/// answers `None` so the caller keeps its sentinel rather than failing a parse.
pub fn stored_api_key(project: &str) -> Option<String> {
    let config_dir = crate::secrets::config_dir().ok()?;
    load_in(&config_dir, project)
        .ok()
        .flatten()
        .map(|creds| creds.api_key)
}

/// Whether docker holds `<project>_postgres_data`, the named volume compose
/// creates for the stack's database. Only an answer from a reachable daemon
/// counts: a missing docker, a stopped daemon, or any other failure is
/// [`VolumeProbe::Unknown`], never "no volume".
async fn probe_postgres_volume(project: &str) -> VolumeProbe {
    let output = tokio::process::Command::new("docker")
        .args(["volume", "inspect", &format!("{project}_postgres_data")])
        .stdin(std::process::Stdio::null())
        .output()
        .await;
    match output {
        Ok(output) => classify_volume_inspect(output.status.success(), &output.stderr),
        Err(_) => VolumeProbe::Unknown,
    }
}

/// Classify `docker volume inspect`: success is [`VolumeProbe::Exists`]; a
/// failure whose stderr says the volume does not exist is
/// [`VolumeProbe::Absent`]; any other failure (daemon down, permission denied)
/// is [`VolumeProbe::Unknown`].
fn classify_volume_inspect(success: bool, stderr: &[u8]) -> VolumeProbe {
    if success {
        return VolumeProbe::Exists;
    }
    let stderr = String::from_utf8_lossy(stderr).to_ascii_lowercase();
    if stderr.contains("no such volume") {
        VolumeProbe::Absent
    } else {
        VolumeProbe::Unknown
    }
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

    const NO_VOLUME: VolumeProbe = VolumeProbe::Absent;

    /// Load the store once and resolve, probing only when nothing is stored,
    /// the same shape as `resolve_for_up`.
    fn resolve_in(
        config_dir: &Path,
        project: &str,
        explicit: Option<&str>,
        probe: VolumeProbe,
        persist: bool,
    ) -> Result<ResolvedStackCredentials> {
        let stored = load_in(config_dir, project)?;
        let probe = stored.is_none().then_some(probe);
        resolve_with(config_dir, project, stored, explicit, probe, persist)
    }

    fn is_hex64(value: &str) -> bool {
        value.len() == 64 && value.chars().all(|c| c.is_ascii_hexdigit())
    }

    #[test]
    fn fresh_install_generates_distinct_64_hex_credentials() {
        let dir = tempfile::tempdir().unwrap();
        let a = resolve_in(dir.path(), "a", None, NO_VOLUME, false).unwrap();
        let b = resolve_in(dir.path(), "b", None, NO_VOLUME, false).unwrap();
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
        let first = resolve_in(dir.path(), "curie", None, NO_VOLUME, true).unwrap();
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
        let second = resolve_in(dir.path(), "curie", None, VolumeProbe::Unknown, true).unwrap();
        assert_eq!(second.credentials, first.credentials);
    }

    #[test]
    fn an_existing_postgres_volume_keeps_the_legacy_password() {
        let dir = tempfile::tempdir().unwrap();
        let resolved = resolve_in(dir.path(), "curie", None, VolumeProbe::Exists, true).unwrap();
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
        let fresh = resolve_in(dir.path(), "curie", Some(PLACEHOLDER_KEY), NO_VOLUME, true)
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
            NO_VOLUME,
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
        let resolved = resolve_in(dir.path(), "curie", Some(""), NO_VOLUME, false).unwrap();
        assert!(is_hex64(&resolved.credentials.api_key));
    }

    #[test]
    fn dry_run_writes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let resolved =
            resolve_in(dir.path(), "curie", Some(PLACEHOLDER_KEY), NO_VOLUME, false).unwrap();
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
        assert!(resolve_in(dir.path(), "curie", None, NO_VOLUME, true).is_err());
        std::fs::write(&path, r#"{"api_key":"","postgres_password":"placeholder"}"#).unwrap();
        assert!(load_in(dir.path(), "curie").is_err());
    }

    #[test]
    fn an_unknown_volume_refuses_to_persist_and_writes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let err = resolve_in(dir.path(), "curie", None, VolumeProbe::Unknown, true)
            .unwrap_err()
            .to_string();
        assert!(err.contains("Start Docker"), "{err}");
        assert!(!path_in(dir.path(), "curie").unwrap().exists());
        assert!(!dir.path().join("local").exists());
        // An explicit key does not change that: the password is still unknown.
        assert!(resolve_in(
            dir.path(),
            "curie",
            Some(PLACEHOLDER_KEY),
            VolumeProbe::Unknown,
            true
        )
        .is_err());
        assert!(!dir.path().join("local").exists());
    }

    #[test]
    fn an_unknown_volume_still_resolves_a_dry_run() {
        let dir = tempfile::tempdir().unwrap();
        let resolved = resolve_in(dir.path(), "curie", None, VolumeProbe::Unknown, false).unwrap();
        assert!(is_hex64(&resolved.credentials.postgres_password));
        assert!(!dir.path().join("local").exists());
    }

    #[test]
    fn a_stored_install_ignores_an_unknown_volume() {
        let dir = tempfile::tempdir().unwrap();
        let first = resolve_in(dir.path(), "curie", None, NO_VOLUME, true).unwrap();
        let again = resolve_in(dir.path(), "curie", None, VolumeProbe::Unknown, true).unwrap();
        assert_eq!(again.credentials, first.credentials);
    }

    #[test]
    fn volume_inspect_failures_are_classified_by_stderr() {
        assert_eq!(classify_volume_inspect(true, b""), VolumeProbe::Exists);
        assert_eq!(
            classify_volume_inspect(
                false,
                b"Error response from daemon: get curie_postgres_data: no such volume"
            ),
            VolumeProbe::Absent
        );
        assert_eq!(
            classify_volume_inspect(
                false,
                b"Cannot connect to the Docker daemon at unix:///var/run/docker.sock. Is the docker daemon running?"
            ),
            VolumeProbe::Unknown
        );
        assert_eq!(classify_volume_inspect(false, b""), VolumeProbe::Unknown);
    }

    // `local rebuild` and `local comms` read the store; they never adopt an
    // explicit key, generate, or write.
    #[test]
    fn running_stack_env_is_empty_when_nothing_is_stored() {
        let dir = tempfile::tempdir().unwrap();
        assert!(running_stack_secret_env_in(dir.path(), "curie")
            .unwrap()
            .is_empty());
        assert!(!dir.path().join("local").exists(), "nothing may be written");
    }

    #[test]
    fn running_stack_env_carries_the_store_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let stored =
            resolve_in(dir.path(), "curie", Some(PLACEHOLDER_KEY), NO_VOLUME, true).unwrap();
        let before = std::fs::read(&stored.path).unwrap();
        let env = running_stack_secret_env_in(dir.path(), "curie").unwrap();
        assert_eq!(env, compose_secret_env(&stored.credentials));
        assert_eq!(std::fs::read(&stored.path).unwrap(), before);
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

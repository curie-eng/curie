//! Same-host ownership for cooperating transactional upgrade processes.
//!
//! The non-expiring lock is local to this user's state directory. It neither
//! fences another host/raw Helm nor proves that remote hooks or requests ended.
//! Reacquiring it is never authority to roll back a pending Helm revision.

use anyhow::Result;
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::command::{plain, run_capture, CommonOpts, OpsCommand, SecretValuesFileGuard};

pub(super) struct UpgradeOwner {
    kubeconfig: SecretValuesFileGuard,
    // Keep the inode open and locked through the whole lifecycle. Never unlink
    // it: replacing a locked path would let a contender lock a different inode.
    _lock: std::fs::File,
}

fn failure(message: &str) -> anyhow::Error {
    crate::exit::CliError::failure(message)
        .with_fix("verify the selected Kubernetes context and namespace access, and ensure the local Curie upgrade state is private and owned by this user; then retry the same command")
        .into()
}

pub(super) fn capture_command() -> OpsCommand {
    let mut args = crate::connectors::kubeconfig_view_args();
    args.remove(0);
    args.push("--flatten".into());
    OpsCommand::new("kubectl", args.into_iter().map(plain).collect())
}

pub(super) fn namespace_command(namespace: &str) -> OpsCommand {
    OpsCommand::new(
        "kubectl",
        vec![
            plain("get"),
            plain("namespace"),
            plain(namespace),
            plain("-o"),
            plain("json"),
        ],
    )
}

async fn read(cmd: &OpsCommand, message: &str) -> Result<Value> {
    let output = tokio::time::timeout(std::time::Duration::from_secs(10), run_capture(cmd))
        .await
        .map_err(|_| {
            crate::exit::CliError::transient(message)
                .with_fix("verify Kubernetes access and retry the same upgrade command")
        })?
        .map_err(|_| failure(message))?;
    if !output.0 {
        return Err(failure(message));
    }
    // Do not attach the raw kubeconfig or kubectl stderr to an error chain.
    serde_json::from_str(&output.1).map_err(|_| failure(message))
}

impl UpgradeOwner {
    pub(super) async fn acquire(opts: &CommonOpts) -> Result<Self> {
        reject_helm_target_overrides()?;
        let view = read(
            &capture_command(),
            "could not capture the upgrade Kubernetes target",
        )
        .await?;
        validate_snapshot(&view)?;
        let kubeconfig = SecretValuesFileGuard::write_document(&view)?;
        let command = namespace_command(&opts.namespace).with_env(snapshot_env(&kubeconfig));
        let namespace = read(&command, "could not verify the upgrade namespace identity").await?;
        let uid = namespace
            .pointer("/metadata/uid")
            .and_then(Value::as_str)
            .filter(|uid| !uid.is_empty());
        if namespace.get("kind").and_then(Value::as_str) != Some("Namespace")
            || namespace.pointer("/metadata/name").and_then(Value::as_str)
                != Some(opts.namespace.as_str())
            || uid.is_none()
        {
            return Err(failure(
                "upgrade namespace identity is missing or does not match the selected target",
            ));
        }
        // A Namespace UID remains the same across aliases for the same cluster.
        // Endpoint spelling alone would permit two local locks for one release.
        // https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#uids
        let key = Sha256::digest(serde_json::to_vec(&(uid, &opts.release))?)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let lock = acquire_lock(&key)?;
        Ok(Self {
            kubeconfig,
            _lock: lock,
        })
    }

    #[cfg(unix)]
    pub(super) fn ownership_fd(&self) -> Option<i32> {
        use std::os::fd::AsRawFd;
        Some(self._lock.as_raw_fd())
    }

    #[cfg(not(unix))]
    pub(super) fn ownership_fd(&self) -> Option<i32> {
        None
    }

    pub(super) fn read_witness(&self) -> Result<Option<Value>> {
        use std::io::{Read, Seek, SeekFrom};
        let mut file = &self._lock;
        file.seek(SeekFrom::Start(0))
            .map_err(|_| failure("could not read local upgrade witness"))?;
        let mut bytes = Vec::new();
        file.take(65537)
            .read_to_end(&mut bytes)
            .map_err(|_| failure("could not read local upgrade witness"))?;
        if bytes.is_empty() {
            return Ok(None);
        }
        if bytes.len() > 65536 {
            return Err(failure("local upgrade witness exceeds its bounded format"));
        }
        serde_json::from_slice(&bytes).map(Some).map_err(|_| {
            failure("local upgrade witness is truncated or malformed; preserve it before recovery")
        })
    }

    pub(super) fn write_witness(&self, witness: &impl serde::Serialize) -> Result<()> {
        use std::io::{Seek, SeekFrom, Write};
        let bytes = serde_json::to_vec(witness)?;
        if bytes.len() > 65536 {
            return Err(failure("local upgrade witness exceeds its bounded format"));
        }
        let mut file = &self._lock;
        file.seek(SeekFrom::Start(0))
            .map_err(|_| failure("could not persist local upgrade witness"))?;
        file.write_all(&bytes)
            .and_then(|_| file.set_len(bytes.len() as u64))
            .and_then(|_| file.sync_all())
            .map_err(|_| failure("could not durably persist local upgrade witness"))
    }

    pub(super) fn environment(&self) -> Vec<(String, String)> {
        snapshot_env(&self.kubeconfig)
    }

    pub(super) fn bind(&self, cmd: &OpsCommand) -> OpsCommand {
        let mut cmd = cmd.clone();
        cmd.env.retain(|(key, _)| key != "KUBECONFIG");
        cmd.secret_env.retain(|(key, _)| key != "KUBECONFIG");
        cmd.env.extend(self.environment());
        cmd
    }
}

fn reject_helm_target_overrides() -> Result<()> {
    // Helm binds these independently of the selected kubeconfig. A captured
    // context alone does not bind Helm's endpoint, identity or TLS authority.
    // https://github.com/helm/helm/blob/v3.16.4/pkg/cli/environment.go
    for name in [
        "HELM_KUBEAPISERVER",
        "HELM_KUBECONTEXT",
        "HELM_KUBECAFILE",
        "HELM_KUBETOKEN",
        "HELM_KUBEASUSER",
        "HELM_KUBEASGROUPS",
        "HELM_KUBEINSECURE_SKIP_TLS_VERIFY",
        "HELM_KUBETLS_SERVER_NAME",
    ] {
        if std::env::var_os(name).is_some_and(|value| !value.is_empty()) {
            return Err(crate::exit::CliError::usage(format!("transactional upgrade cannot bind its target while {name} is set"))
                .with_fix(format!("unset {name} and place the intended target and authentication settings in the selected kubeconfig, then retry"))
                .into());
        }
    }
    Ok(())
}

fn snapshot_env(snapshot: &SecretValuesFileGuard) -> Vec<(String, String)> {
    vec![(
        "KUBECONFIG".into(),
        snapshot.path().to_string_lossy().into_owned(),
    )]
}

fn validate_snapshot(view: &Value) -> Result<()> {
    let clusters = view.get("clusters").and_then(Value::as_array);
    let contexts = view.get("contexts").and_then(Value::as_array);
    let valid = clusters.zip(contexts).is_some_and(|(clusters, contexts)| {
        clusters.len() == 1
            && contexts.len() == 1
            && contexts[0]
                .get("name")
                .and_then(Value::as_str)
                .is_some_and(|name| {
                    !name.is_empty()
                        && Some(name) == view.get("current-context").and_then(Value::as_str)
                })
            && clusters[0]
                .get("name")
                .and_then(Value::as_str)
                .is_some_and(|name| {
                    !name.is_empty()
                        && Some(name)
                            == contexts[0]
                                .pointer("/context/cluster")
                                .and_then(Value::as_str)
                })
            && clusters[0]
                .pointer("/cluster/server")
                .and_then(Value::as_str)
                .is_some_and(|server| !server.is_empty())
    });
    if !valid {
        return Err(failure(
            "captured upgrade kubeconfig has no single bound Kubernetes target",
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn acquire_lock(key: &str) -> Result<std::fs::File> {
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};

    let state = std::env::var_os("XDG_STATE_HOME")
        .filter(|value| !value.is_empty())
        .map(std::path::PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME").map(|home| std::path::PathBuf::from(home).join(".local/state"))
        })
        .ok_or_else(|| failure("local upgrade state directory is not configured"))?;
    if !state.is_absolute() {
        return Err(failure("local upgrade state directory must be absolute"));
    }
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(&state)
        .map_err(|_| failure("could not create local upgrade state directory"))?;
    let user = unsafe { libc::geteuid() };
    let state_metadata = std::fs::symlink_metadata(&state)
        .map_err(|_| failure("could not inspect local upgrade state directory"))?;
    if !state_metadata.is_dir()
        || state_metadata.uid() != user
        || state_metadata.mode() & 0o022 != 0
    {
        return Err(failure(
            "local upgrade state directory must be owned by this user and not writable by others",
        ));
    }
    let mut directory = state;
    for segment in ["curie", "upgrades"] {
        directory.push(segment);
        match std::fs::DirBuilder::new().mode(0o700).create(&directory) {
            Ok(()) => (),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => (),
            Err(_) => return Err(failure("could not create private local upgrade state")),
        }
        let metadata = std::fs::symlink_metadata(&directory)
            .map_err(|_| failure("could not inspect local upgrade state"))?;
        if !metadata.is_dir() || metadata.uid() != user || metadata.mode() & 0o077 != 0 {
            return Err(failure(
                "local upgrade state is not a private owned directory",
            ));
        }
    }
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(directory.join(format!("{key}.lock")))
        .map_err(|_| failure("could not open the private local upgrade ownership file"))?;
    let metadata = file
        .metadata()
        .map_err(|_| failure("could not inspect local upgrade ownership"))?;
    if !metadata.is_file()
        || metadata.uid() != user
        || metadata.mode() & 0o077 != 0
        || metadata.nlink() != 1
    {
        return Err(failure(
            "local upgrade ownership must be a private regular file with exactly one link",
        ));
    }
    // SAFETY: flock borrows a valid open descriptor and takes no pointer. Kernel
    // ownership lasts until every inherited descriptor closes.
    // https://man7.org/linux/man-pages/man2/flock.2.html
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        let error = std::io::Error::last_os_error();
        if error.kind() == std::io::ErrorKind::WouldBlock {
            return Err(crate::exit::CliError::transient("another local Curie process owns this release upgrade")
                .with_fix("wait for the owning upgrade process to finish, then retry the same command; do not delete its ownership file")
                .into());
        }
        return Err(failure("could not acquire local upgrade ownership"));
    }
    Ok(file)
}

#[cfg(not(unix))]
fn acquire_lock(_key: &str) -> Result<std::fs::File> {
    Err(crate::exit::CliError::unsupported(
        "transactional upgrade ownership requires a supported Unix host",
    )
    .with_fix(
        "run this transactional upgrade from Linux or macOS with a private local state directory",
    )
    .into())
}

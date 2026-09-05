//! `OpsCommand` construction, secret masking, the secret-values-file
//! registry and its signal cleanup, and the two process runners
//! (`run_capture`, `run_step`) every verb shells out through.

use anyhow::{bail, Context, Result};
use std::collections::BTreeSet;
#[cfg(unix)]
use std::sync::{LazyLock, Mutex, MutexGuard, OnceLock};
use tokio::process::Command;

#[allow(unused_imports)]
use super::{providers::*, up::*, verbs::*};

/// One external command: the program plus its argument vector, with secret
/// argument values tagged so they can be masked in any printed form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpsCommand {
    pub program: String,
    pub args: Vec<CmdArg>,
    pub env: Vec<(String, String)>,
    pub secret_env: Vec<(String, String)>,
}

/// A single argv token.
///
/// `HelmSetExpression` preserves a complete `--set` or `--set-string`
/// expression for execution while masking credential shaped values in every
/// rendered form.
///
/// `SecretSet` is a `helm --set key=value` whose value is a credential: the real
/// value is used for execution, but only a masked prefix is ever printed (dry-run
/// or the echoed command line). Note the value still lands in the process argv --
/// acceptable only for low-sensitivity tokens that already live in a k8s Secret.
///
/// `SecretValuesFile` carries one or more secret `helm` values (dotted key ->
/// value) that must **never** reach the process table. Before execution it is
/// materialized into a private (0600) temporary values file and replaced by a
/// `-f <path>` pair (see [`OpsCommand::materialize_secret_files`]); the file is
/// removed as soon as the command finishes. This keeps the secret off `ps -ef`
/// and out of `/proc/<pid>/cmdline`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CmdArg {
    Plain(String),
    HelmSetExpression(String),
    SecretSet { key: String, value: String },
    SecretValuesFile(Vec<(String, String)>),
    PrivateJsonValuesFile(PrivateHelmValues),
}

impl CmdArg {
    /// The real argv token(s) passed to the process. Most args map to a single
    /// token; `SecretValuesFile` is expected to have been replaced by a `-f
    /// <path>` pair during materialization, so reaching it here (an unmaterialized
    /// secret file about to be executed) is a bug -- we emit nothing rather than
    /// risk leaking, and trip a debug assertion.
    fn value_tokens(&self) -> Vec<String> {
        match self {
            CmdArg::Plain(s) => vec![s.clone()],
            CmdArg::HelmSetExpression(expression) => vec![expression.clone()],
            CmdArg::SecretSet { key, value } => vec![format!("{key}={value}")],
            CmdArg::SecretValuesFile(_) | CmdArg::PrivateJsonValuesFile(_) => {
                debug_assert!(
                    false,
                    "SecretValuesFile must be materialized before argv(); \
                     call OpsCommand::materialize_secret_files first"
                );
                Vec::new()
            }
        }
    }

    /// The token(s) as shown to a human: secret values are masked. A
    /// `SecretValuesFile` prints as `-f <secret values file: key=masked, ...>` so
    /// the operator can see which values are applied without any secret leaking.
    fn masked_tokens(&self) -> Vec<String> {
        match self {
            CmdArg::Plain(s) => vec![s.clone()],
            CmdArg::HelmSetExpression(expression) => {
                vec![mask_helm_set_expression(expression)]
            }
            CmdArg::SecretSet { key, value } => vec![format!("{key}={}", mask_secret(value))],
            CmdArg::PrivateJsonValuesFile(values) => vec![
                "-f".to_string(),
                format!(
                    "<private retained mail values: {}>",
                    values.keys().join(", ")
                ),
            ],
            CmdArg::SecretValuesFile(pairs) => {
                let masked: Vec<String> = pairs
                    .iter()
                    .map(|(key, value)| format!("{key}={}", mask_secret(value)))
                    .collect();
                vec![
                    "-f".to_string(),
                    format!("<secret values file: {}>", masked.join(", ")),
                ]
            }
        }
    }
}

impl OpsCommand {
    pub(crate) fn new(program: &str, args: Vec<CmdArg>) -> Self {
        Self {
            program: program.to_string(),
            args,
            env: Vec::new(),
            secret_env: Vec::new(),
        }
    }

    pub fn with_env(mut self, env: Vec<(String, String)>) -> Self {
        self.env = env;
        self
    }

    pub fn with_secret_env(mut self, secret_env: Vec<(String, String)>) -> Self {
        self.secret_env = secret_env;
        self
    }

    /// The argv tail (real values) handed to `tokio::process::Command`. Call
    /// [`materialize_secret_files`](Self::materialize_secret_files) first when the
    /// command may carry a [`CmdArg::SecretValuesFile`], otherwise those secret
    /// values are dropped rather than executed.
    pub fn argv(&self) -> Vec<String> {
        self.args.iter().flat_map(CmdArg::value_tokens).collect()
    }

    /// The full shell-quoted command line with secrets masked, one line as it
    /// would be typed into a shell.
    pub fn display(&self) -> String {
        let mut env: Vec<String> = self
            .env
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .chain(
                self.secret_env
                    .iter()
                    .map(|(key, value)| format!("{key}={}", mask_secret(value))),
            )
            .collect();
        env.sort();
        let mut parts: Vec<String> = env.iter().map(|item| shell_quote(item)).collect();
        parts.push(shell_quote(&self.program));
        for a in &self.args {
            for token in a.masked_tokens() {
                parts.push(shell_quote(&token));
            }
        }
        parts.join(" ")
    }

    /// Materialize every [`CmdArg::SecretValuesFile`] into a private (0600)
    /// temporary values file and return an equivalent command whose secrets are
    /// delivered via `helm -f <path>` instead of the argv, plus RAII guards that
    /// delete any remaining files when dropped. The signal cleanup handler also
    /// removes registered files on SIGINT or SIGTERM. Commands without a secret
    /// values file are returned unchanged with no guards. Hold the returned guards
    /// until the process has finished.
    pub(crate) fn materialize_secret_files(
        &self,
    ) -> Result<(OpsCommand, Vec<SecretValuesFileGuard>)> {
        let mut new_args = Vec::with_capacity(self.args.len());
        let mut guards = Vec::new();
        for a in &self.args {
            match a {
                CmdArg::SecretValuesFile(pairs) => {
                    let guard = SecretValuesFileGuard::write(pairs)?;
                    new_args.push(plain("-f"));
                    new_args.push(plain(guard.path.to_string_lossy().into_owned()));
                    guards.push(guard);
                }
                CmdArg::PrivateJsonValuesFile(values) => {
                    let guard = SecretValuesFileGuard::write_document(&values.0)?;
                    new_args.push(plain("-f"));
                    new_args.push(plain(guard.path.to_string_lossy().into_owned()));
                    guards.push(guard);
                }
                other => new_args.push(other.clone()),
            }
        }
        Ok((
            OpsCommand {
                program: self.program.clone(),
                args: new_args,
                env: self.env.clone(),
                secret_env: self.secret_env.clone(),
            },
            guards,
        ))
    }
}

#[cfg(unix)]
#[derive(Default)]
struct SecretFileRegistry {
    terminating: bool,
    paths: BTreeSet<std::path::PathBuf>,
}

#[cfg(unix)]
static SECRET_FILE_REGISTRY: LazyLock<Mutex<SecretFileRegistry>> =
    LazyLock::new(|| Mutex::new(SecretFileRegistry::default()));

#[cfg(unix)]
static SECRET_SIGNAL_INSTALLATION: OnceLock<std::result::Result<(), String>> = OnceLock::new();

#[cfg(unix)]
fn lock_secret_file_registry() -> MutexGuard<'static, SecretFileRegistry> {
    SECRET_FILE_REGISTRY
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(unix)]
fn ensure_secret_signal_cleanup() -> Result<()> {
    match SECRET_SIGNAL_INSTALLATION
        .get_or_init(|| install_secret_signal_cleanup().map_err(|error| error.to_string()))
    {
        Ok(()) => Ok(()),
        Err(error) => bail!("installing secret values signal cleanup: {error}"),
    }
}

#[cfg(not(unix))]
fn ensure_secret_signal_cleanup() -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn install_secret_signal_cleanup() -> std::io::Result<()> {
    let mut signals = signal_hook::iterator::Signals::new([
        signal_hook::consts::signal::SIGINT,
        signal_hook::consts::signal::SIGTERM,
    ])?;
    std::thread::Builder::new()
        .name("curie-secret-cleanup".to_string())
        .spawn(move || {
            if let Some(signal) = signals.forever().next() {
                terminate_after_secret_cleanup(signal);
            }
        })?;
    Ok(())
}

#[cfg(unix)]
fn terminate_after_secret_cleanup(signal: i32) -> ! {
    {
        let mut registry = lock_secret_file_registry();
        registry.terminating = true;
        for path in std::mem::take(&mut registry.paths) {
            let _ = std::fs::remove_file(path);
        }
    }

    test_mark_coordination("CURIE_TEST_SECRET_SIGNAL_CLEANED");
    test_wait_for_coordination("CURIE_TEST_SECRET_RESUME_SIGNAL");

    let _ = signal_hook::low_level::emulate_default_handler(signal);
    signal_hook::low_level::exit(128 + signal);
}

#[cfg(unix)]
fn park_terminating_secret_writer() -> ! {
    test_mark_coordination("CURIE_TEST_SECRET_WRITER_PARKED");
    loop {
        std::thread::park();
    }
}

#[cfg(debug_assertions)]
fn test_mark_coordination(env_name: &str) {
    if let Some(path) = std::env::var_os(env_name).map(std::path::PathBuf::from) {
        let _ = std::fs::write(path, b"ready");
    }
}

#[cfg(not(debug_assertions))]
fn test_mark_coordination(_env_name: &str) {}

#[cfg(debug_assertions)]
fn test_wait_for_coordination(env_name: &str) {
    if let Some(path) = std::env::var_os(env_name).map(std::path::PathBuf::from) {
        while !path.exists() {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }
}

#[cfg(not(debug_assertions))]
fn test_wait_for_coordination(_env_name: &str) {}

#[cfg(debug_assertions)]
fn test_pause_after_first_secret_file() {
    static PAUSED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

    if std::env::var_os("CURIE_TEST_SECRET_FIRST_FILE_WRITTEN").is_none()
        || PAUSED.swap(true, std::sync::atomic::Ordering::SeqCst)
    {
        return;
    }
    test_mark_coordination("CURIE_TEST_SECRET_FIRST_FILE_WRITTEN");
    test_wait_for_coordination("CURIE_TEST_SECRET_RESUME_WRITER");
}

#[cfg(not(debug_assertions))]
fn test_pause_after_first_secret_file() {}

/// A 0600 temporary helm values file holding secret values. Signal cleanup
/// removes it on SIGINT or SIGTERM; `Drop` removes it on normal completion or
/// error, so the secret never outlives the `helm` invocation.
pub(crate) struct SecretValuesFileGuard {
    path: std::path::PathBuf,
}

impl SecretValuesFileGuard {
    /// Write `pairs` (dotted helm keys -> secret values) into a fresh 0600 temp
    /// file as nested YAML (a JSON document, which helm parses as YAML), created
    /// with restrictive permissions atomically so the secret is never briefly
    /// world-readable.
    fn write(pairs: &[(String, String)]) -> Result<Self> {
        Self::write_document(&nest_dotted_keys(pairs))
    }

    pub(crate) fn write_document(doc: &serde_json::Value) -> Result<Self> {
        ensure_secret_signal_cleanup()?;
        let body = serde_json::to_vec(doc).context("serializing secret helm values")?;

        let mut path = std::env::temp_dir();
        path.push(format!("curie-helm-values-{}.yaml", uuid::Uuid::new_v4()));

        #[cfg(unix)]
        {
            let mut registry = lock_secret_file_registry();
            if registry.terminating {
                drop(registry);
                park_terminating_secret_writer();
            }
            if !registry.paths.insert(path.clone()) {
                bail!(
                    "secret helm values file path collision at {}",
                    path.display()
                );
            }
            if let Err(error) = create_secret_values_file(&path, &body) {
                let _ = std::fs::remove_file(&path);
                registry.paths.remove(&path);
                return Err(error);
            }
        }

        #[cfg(not(unix))]
        if let Err(error) = create_secret_values_file(&path, &body) {
            let _ = std::fs::remove_file(&path);
            return Err(error);
        }

        let guard = Self { path };
        test_pause_after_first_secret_file();
        Ok(guard)
    }

    pub(crate) fn path(&self) -> &std::path::Path {
        &self.path
    }
}

fn create_secret_values_file(path: &std::path::Path, body: &[u8]) -> Result<()> {
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut file = opts
        .open(path)
        .with_context(|| format!("creating secret helm values file {}", path.display()))?;
    // Belt-and-suspenders on platforms where create-time mode is not honored.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .with_context(|| format!("securing secret helm values file {}", path.display()))?;
    }
    use std::io::Write;
    file.write_all(body)
        .with_context(|| format!("writing secret helm values file {}", path.display()))?;
    Ok(())
}

impl Drop for SecretValuesFileGuard {
    fn drop(&mut self) {
        // Best-effort cleanup; nothing actionable if the temp file is already gone.
        #[cfg(unix)]
        {
            let mut registry = lock_secret_file_registry();
            let _ = std::fs::remove_file(&self.path);
            registry.paths.remove(&self.path);
        }
        #[cfg(not(unix))]
        {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

/// Expand dotted helm keys (`a.b.c=value`) into a nested JSON object suitable as
/// a helm values file. JSON is a subset of YAML, so helm parses it directly, and
/// serde handles all value escaping so a secret with YAML-special characters
/// cannot break the document.
fn nest_dotted_keys(pairs: &[(String, String)]) -> serde_json::Value {
    let mut root = serde_json::Map::new();
    for (dotted, value) in pairs {
        let parts: Vec<&str> = dotted.split('.').collect();
        let mut cursor = &mut root;
        for part in &parts[..parts.len() - 1] {
            cursor = cursor
                .entry((*part).to_string())
                .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()))
                .as_object_mut()
                .expect("dotted key prefix maps to an object");
        }
        cursor.insert(
            parts[parts.len() - 1].to_string(),
            serde_json::Value::String(value.clone()),
        );
    }
    serde_json::Value::Object(root)
}

pub fn plain(s: impl Into<String>) -> CmdArg {
    CmdArg::Plain(s.into())
}

pub(crate) fn secret_set(key: &str, value: &str) -> CmdArg {
    CmdArg::SecretSet {
        key: key.to_string(),
        value: value.to_string(),
    }
}

/// Mask a secret for display. Values of eight characters or fewer are fully
/// masked; longer values retain the first eight characters for recognition.
pub fn mask_secret(value: &str) -> String {
    if value.chars().nth(8).is_none() {
        return "***".to_string();
    }
    let shown: String = value.chars().take(8).collect();
    format!("{shown}***")
}

/// POSIX shell-quote a single token: leave it bare when it is composed only of
/// safe characters, otherwise wrap in single quotes (so `--set` keys carrying
/// `[0]` array indices print quoted, matching how they must be typed).
/// POSIX single-quote an argument for a copy-pasteable command line, leaving
/// unambiguously-safe tokens bare. The one canonical implementation (#497): the
/// TUI command echo (`interactive::render_command`) and the helm/kubectl argv
/// printers both call this, so an empty argument renders as `''` everywhere
/// rather than silently vanishing (the interactive copy's empty-`all()` bug).
pub(crate) fn shell_quote(s: &str) -> String {
    fn is_safe(c: char) -> bool {
        c.is_ascii_alphanumeric()
            || matches!(c, '_' | '.' | '/' | ':' | '=' | '@' | ',' | '-' | '+')
    }
    if !s.is_empty() && s.chars().all(is_safe) {
        s.to_string()
    } else {
        format!("'{}'", s.replace('\'', "'\\''"))
    }
}

// ---------------------------------------------------------------------------
// Options structs (mirror the clap flags in main.rs)
// ---------------------------------------------------------------------------

/// Common flags every verb carries.
#[derive(Debug, Clone)]
pub struct CommonOpts {
    pub namespace: String,
    pub release: String,
    pub dry_run: bool,
}

// ---------------------------------------------------------------------------
// Execution
// ---------------------------------------------------------------------------

/// Fail with a clear one-line error if `bin` is not on `PATH`.
pub(crate) fn require_on_path(bin: &str) -> Result<()> {
    let found = std::env::var_os("PATH")
        .map(|paths| std::env::split_paths(&paths).any(|dir| dir.join(bin).is_file()))
        .unwrap_or(false);
    if found {
        Ok(())
    } else {
        bail!("`{bin}` is not on PATH; install it (or add it to PATH) and retry")
    }
}

/// Run one command capturing stdout; returns (success, stdout, stderr).
pub async fn run_capture(cmd: &OpsCommand) -> Result<(bool, String, String)> {
    capture_process(cmd, false, None).await
}

/// Capture the Helm mutation owned by the transactional upgrade. On Linux its
/// direct child is also stopped when the spawning CLI thread dies. This does
/// not stop Kubernetes hook Jobs, plugins' descendants, or remote writers.
pub(super) async fn run_upgrade_capture(
    cmd: &OpsCommand,
    ownership_fd: Option<i32>,
) -> Result<(bool, String, String)> {
    capture_process(cmd, true, ownership_fd).await
}

#[cfg(target_os = "linux")]
fn arm_parent_death(expected_parent: libc::pid_t) -> std::io::Result<()> {
    // The signal is preserved by ordinary exec, but is cleared by fork and
    // privileged exec/credential changes. Supported Helm is an ordinary direct
    // executable, not a wrapper or privilege-changing launcher. The signal is
    // tied to the spawning thread; that thread awaits this child to completion.
    // https://man7.org/linux/man-pages/man2/PR_SET_PDEATHSIG.2const.html
    // SAFETY: pre_exec may not allocate or lock. These libc calls and raw errno
    // construction perform neither. Capture the expected PID before fork and
    // recheck after arming so a parent that already died cannot escape the guard.
    unsafe {
        if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL, 0, 0, 0) != 0 {
            return Err(std::io::Error::last_os_error());
        }
        if libc::getppid() != expected_parent {
            return Err(std::io::Error::from_raw_os_error(libc::ESRCH));
        }
    }
    Ok(())
}

async fn capture_process(
    cmd: &OpsCommand,
    owned_upgrade: bool,
    ownership_fd: Option<i32>,
) -> Result<(bool, String, String)> {
    // Materialize any secret values into a private 0600 `-f` file so the secret
    // stays out of the argv/process table. `_secret_files` guards live until the
    // end of this function, so the temp files are removed after `helm` exits
    // (including on error paths below).
    let (cmd, _secret_files) = cmd.materialize_secret_files()?;
    // `kill_on_drop` so a caller that ABANDONS this future does not leave the
    // child running. Dropping the future only stops the wait; without this the
    // process keeps going, which is worst precisely where the abandonment is
    // deliberate -- `message::local_connected_transport` bounds its `docker`
    // reads by a timeout for a wedged daemon, and each timed-out call would
    // otherwise strand another hung `docker` client (#1031). Inert for every
    // caller that awaits to completion: the child has already exited by then.
    let mut process = Command::new(&cmd.program);
    process
        .args(cmd.argv())
        .envs(cmd.env.iter().chain(cmd.secret_env.iter()).cloned())
        .kill_on_drop(true);
    #[cfg(unix)]
    if owned_upgrade {
        #[cfg(target_os = "linux")]
        let expected_parent = unsafe { libc::getpid() };
        // SAFETY: the callback only executes the non-allocating libc/errno
        // operations documented in arm_parent_death.
        unsafe {
            process.pre_exec(move || {
                #[cfg(target_os = "linux")]
                arm_parent_death(expected_parent)?;
                if let Some(fd) = ownership_fd {
                    // Keep the same flock open description through direct Helm
                    // exec. Parent exit cannot release ownership before this
                    // child exits. fcntl affects only the forked descriptor table.
                    if libc::fcntl(fd, libc::F_SETFD, 0) != 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                }
                Ok(())
            });
        }
    }
    // Non-Unix targets retain ordinary subprocess behavior. On macOS the lock
    // is inherited, but Linux's parent-death signal is not claimed or emulated.
    #[cfg(not(unix))]
    let _ = (owned_upgrade, ownership_fd);
    let output = process
        .output()
        .await
        .with_context(|| format!("failed to invoke `{}`; is it on PATH?", cmd.program))?;
    Ok((
        output.status.success(),
        String::from_utf8_lossy(&output.stdout).to_string(),
        String::from_utf8_lossy(&output.stderr).to_string(),
    ))
}

pub(super) fn finish_captured_step(
    step: crate::ui::Step,
    ok_detail: &str,
    cmd: &OpsCommand,
    ok: bool,
    out: String,
    err: String,
) -> Result<String> {
    let ui = crate::ui::ui();
    if ok {
        step.done(ok_detail);
    } else {
        step.fail("failed");
    }
    for line in out.lines().chain(err.lines()) {
        ui.plumbing(line);
    }
    // One implementation, shared with the teardown Display message (#1230):
    // an inline second copy of this rule is how the two drifted before.
    if !ok {
        let reason = failure_reason(&err);
        ui.failure(&format!("`{}` failed: {reason}", cmd.program));
        bail!("`{}` exited nonzero", cmd.program);
    }
    Ok(out)
}

/// Run one command under a checklist `step` labeled `label`, capturing its
/// stdio. Echoes the masked command line and replays the captured output as dim
/// plumbing (both no-ops unless `--debug`, so default runs stay quiet and the
/// helm/kubectl/compose chatter is hidden). On success the step freezes done
/// with `ok_detail`; on a nonzero exit it freezes failed, surfaces the captured
/// stderr via `ui.failure`, and bails. Returns captured stdout.
pub(crate) async fn run_step(
    cl: &crate::ui::Checklist,
    label: &str,
    ok_detail: &str,
    cmd: &OpsCommand,
) -> Result<String> {
    let ui = crate::ui::ui();
    ui.plumbing(&format!("+ {}", cmd.display()));
    let step = cl.step(label);
    let (ok, out, err) = run_capture(cmd).await?;
    finish_captured_step(step, ok_detail, cmd, ok, out, err)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::ops::testsupport::*;

    #[test]
    fn with_env_stores_the_pairs() {
        let cmd =
            OpsCommand::new("docker", vec![plain("ps")]).with_env(vec![("A".into(), "1".into())]);
        assert_eq!(cmd.env, vec![("A".to_string(), "1".to_string())]);
    }

    #[test]
    fn display_renders_sorted_env_before_program() {
        let cmd = OpsCommand::new("docker", vec![plain("ps")])
            .with_env(vec![("B".into(), "2".into()), ("A".into(), "1".into())]);
        assert!(cmd.display().starts_with("A=1 "));
    }

    // #707 CRD retention is by-construction; lock it so no future edit sweeps the
    // agents.x-k8s.io CRDs during teardown.
    #[test]
    fn down_never_deletes_crds() {
        let cmds = down_commands(&common_distinct_release());
        for cmd in &cmds {
            let line = cmd.display();
            assert!(
                !line.contains("delete crd"),
                "CRD deletion must never appear: {line}"
            );
            assert!(
                !line.to_lowercase().contains("customresourcedefinition"),
                "{line}"
            );
        }
    }

    // #707 the up-side ownership SEAM (both branches mandatory). PRODUCTION
    // SYMBOL: a pure builder that gates the ownership stamp on the
    // pre-existence probe result:
    //
    //   fn ownership_label_commands(o: &CommonOpts, release_namespace: &str,
    //                               namespace_existed: bool) -> Vec<OpsCommand>
    //
    // It returns the `kubectl label namespace` stamp step ONLY when `up` created
    // the namespace (namespace_existed == false); an empty vec when the namespace
    // pre-existed. `up()` gates the runtime probe (mirrors the resolve_generated_secrets
    // existing/fresh split), keeping this builder pure and unit-testable.
    #[test]
    fn up_stamps_ownership_label_when_namespace_created() {
        let cmds = ownership_label_commands(&common_distinct_release(), "install-ns", false);
        assert_eq!(cmds.len(), 1);
        // namespace arg is the TARGET namespace; the created-by label VALUE is
        // the release, and the #1654 created-in label VALUE is the release's
        // INSTALL namespace (distinct from the target here, so neither value can
        // be satisfied by the other).
        assert_eq!(
            cmds[0].display(),
            "kubectl label namespace agent-ns curietech.ai/created-by=prod-release curietech.ai/created-in=install-ns --overwrite"
        );
    }

    #[test]
    fn up_does_not_stamp_ownership_label_when_namespace_preexisting() {
        let cmds = ownership_label_commands(&common_distinct_release(), "install-ns", true);
        assert!(
            cmds.is_empty(),
            "a pre-existing namespace must not be stamped (would adopt then delete pre-existing state): {:?}",
            cmds.iter().map(OpsCommand::display).collect::<Vec<_>>()
        );
    }

    #[test]
    fn mask_secret_uses_shared_long_and_short_contract() {
        assert_eq!(mask_secret("xoxb-abcdefghijk"), "xoxb-abc***");
        for value in ["", "a", "short", "12345678"] {
            assert_eq!(
                mask_secret(value),
                "***",
                "values of eight characters or fewer must reveal no characters: {value:?}"
            );
        }
    }

    #[test]
    fn shell_quote_quotes_only_special_tokens() {
        assert_eq!(
            shell_quote("ui.service.type=NodePort"),
            "ui.service.type=NodePort"
        );
        assert_eq!(shell_quote("a[0]=b"), "'a[0]=b'");
        assert_eq!(shell_quote(""), "''");
    }

    #[test]
    fn display_masks_secret_env_values() {
        let line = OpsCommand::new("docker", vec![plain("ps")])
            .with_secret_env(vec![(
                "SLACK_BOT_TOKEN".into(),
                "xoxb-1-secretsecret".into(),
            )])
            .display();
        assert!(line.contains("SLACK_BOT_TOKEN=xoxb-1-s***"), "{line}");
        assert!(!line.contains("secretsecret"), "secret leaked: {line}");
    }

    #[test]
    fn github_token_rides_the_private_values_file_not_argv() {
        // AC1. A live GitHub bearer token gets the model credential's treatment:
        // a private 0600 `-f` values file, never a `--set`. `SecretSet` would
        // mask the printed form but still put `key=value` in the real argv, which
        // is the exact defect #1124 exists to close.
        let cmds = up_with_github_token(GithubTokenPlan::Set(GH_SENTINEL.into()));
        let line = cmds[0].display();
        assert!(
            !line.contains(GH_SENTINEL),
            "token leaked into display: {line}"
        );
        assert!(line.contains(GH_MASKED), "masked form missing: {line}");

        let (materialized, guards) = cmds[0]
            .materialize_secret_files()
            .expect("materializing the secret values file");
        let argv = materialized.argv();
        let argv_joined = argv.join(" ");
        assert!(
            !argv_joined.contains(GH_SENTINEL),
            "token leaked into argv: {argv_joined}"
        );
        assert!(
            !argv_joined.contains("api.githubToken="),
            "the credential must not ride as a --set at all: {argv_joined}"
        );

        // The file helm actually reads carries the real value, nested for helm.
        let f_pos = argv
            .iter()
            .position(|a| a == "-f")
            .expect("a -f flag in the materialized argv");
        let values_path = std::path::PathBuf::from(&argv[f_pos + 1]);
        let body = std::fs::read_to_string(&values_path).expect("reading the values file");
        assert!(
            body.contains(GH_SENTINEL),
            "token missing from values file: {body}"
        );
        assert!(
            body.contains("api") && body.contains("githubToken"),
            "values file is not the expected nested shape: {body}"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&values_path)
                .expect("stat values file")
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600, "values file must be 0600, was {mode:o}");
        }
        drop(guards);
        assert!(
            !values_path.exists(),
            "values file should be deleted once the guard drops"
        );
    }

    #[test]
    fn github_token_masked_form_shows_only_the_prefix() {
        // AC2. `mask_secret`'s contract is 8 chars plus `***`; a ninth character
        // of a live bearer token is a leak, not a nicety.
        let cmds = up_with_github_token(GithubTokenPlan::Set(GH_SENTINEL.into()));
        let line = cmds[0].display();
        assert!(line.contains(GH_MASKED), "{line}");
        assert!(
            !line.contains("ghp-SENTI"),
            "a ninth token character reached the printed form: {line}"
        );
    }
}

#[cfg(all(test, target_os = "linux"))]
mod owned_upgrade_tests {
    use super::*;

    #[tokio::test]
    async fn parent_changed_before_guard_refuses_before_exec() {
        let temp = tempfile::tempdir().unwrap();
        let marker = temp.path().join("must-not-run");
        let mut command = Command::new("/bin/sh");
        command.args(["-c", "printf reached > \"$1\"", "sh"]);
        command.arg(&marker);
        // An impossible parent PID deterministically exercises the same
        // post-arm identity check used when the real owner dies before arming.
        // The consumer is actual spawn/exec, not an internal flag assertion.
        unsafe {
            command.pre_exec(|| arm_parent_death(0));
        }
        let error = command.output().await.unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::ESRCH));
        assert!(!marker.exists());
    }
}

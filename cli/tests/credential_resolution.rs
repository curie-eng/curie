//! Empty-string credentials are absent, not "explicitly supplied" (issue #540, AC6).
//!
//! `CURIE_API_KEY=""` must resolve identically to the variable being unset, on
//! every path. The rule is already settled in three places -- `ops.rs`'s
//! `resolve_up_credentials` filters empty, `secrets.rs`'s `save_value` refuses to
//! store an empty secret, and `local.rs`'s `model_mode_from_env` calls it out by
//! name ("Empty counts as unset (the empty-string-is-not-a-credential rule)").
//! The `--api-key` clap seam missed it: clap treats an env var set to `""` as
//! present, so `""` reaches `state::apply_continue` as a user-supplied key,
//! defeats the sentinel-default comparison against `DEFAULT_API_KEY`, and never
//! falls back.
//!
//! These tests observe the resolution OUTCOME rather than any internal shape, so
//! they hold wherever the normalization lands (a `value_parser` on the four
//! `#[arg]` sites, or another single seam). The outcome they read is issue #112's
//! contract, which is what an empty key silently defeats today: a turn recorded
//! with `api_key_env = CURIE_API_KEY` must hard-error on `--continue` when that
//! env source is no longer set, instead of quietly sending a blank key onward.
//!
//! Env is set on the CHILD process (`Command::env`), never on the test process --
//! matching `json_emit_contract.rs`. `std::env::set_var` would race the other
//! tests sharing this process's environment under cargo's parallel threads.

use std::io::{BufRead, Write};
use std::process::{Command, Output};
use std::sync::{Mutex, MutexGuard};

use curie::state::{save_turn, TurnContext, TurnVerb};

/// Serializes the binary spawns in this file.
///
/// While a case is RED the CLI runs past the bail and binds its Slack stub on
/// `message::DEFAULT_LISTEN_PORT` (8155). That port is not configurable on this
/// path -- `local message` exposes no `--listen-port` (only the `cluster` arms
/// do, `main.rs:818`/`:880`) because `main.rs:1317` hardcodes
/// `message::DEFAULT_LISTEN_PORT`. So two cases on cargo's parallel threads race
/// to bind it and one dies with "Address already in use", masking the assertion
/// under an environmental error. Taking the lock keeps every failure here
/// attributable to the credential logic alone.
///
/// Once the fix lands the bail fires before the stub binds, so only the
/// non-empty case binds at all -- but the lock stays: it is what makes a RED run
/// (this suite's whole purpose before the fix, and again on any regression)
/// report the real assertion instead of a port collision.
static SPAWN_LOCK: Mutex<()> = Mutex::new(());

/// A failing assertion panics while holding the lock, which poisons it. The data
/// is `()`, so there is nothing to corrupt: recover the guard and carry on,
/// otherwise the first red case would cascade into bogus `PoisonError` failures
/// in the others.
fn spawn_lock() -> MutexGuard<'static, ()> {
    SPAWN_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn err_str(o: &Output) -> String {
    String::from_utf8_lossy(&o.stderr).into_owned()
}

/// The `apply_continue` bail that fires when a turn's recorded api-key env
/// source is gone. Reaching it is the observable proof that the key resolved as
/// "absent"; not reaching it proves it resolved as "supplied".
const UNSET_ENV_BAIL: &str = "which is not set now";

/// A project dir holding a `.curie/last-turn.json` whose api key came from
/// `$CURIE_API_KEY`, built through the real state types rather than a hand-written
/// JSON literal so the fixture cannot drift from the shape the CLI loads.
fn project_with_recorded_env_key() -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("tempdir");
    save_turn(
        dir.path(),
        &TurnContext {
            verb: TurnVerb::Local,
            channel: "C-recorded".into(),
            thread_ts: "9.9".into(),
            namespace: "curie".into(),
            release: "curie".into(),
            chart: "charts/curie".into(),
            listen_host: None,
            timeout_secs: 1,
            api_url: None,
            api_key_env: Some("CURIE_API_KEY".into()),
            agent: None,
        },
    )
    .expect("save turn");
    dir
}

/// Continue that recorded turn with `extra_args` and a controlled environment.
/// `--timeout-secs 1` bounds the currently-broken path: today an empty key sails
/// past the bail and enqueues, and without the bound this would block on a reply.
///
/// Every spawn goes through here, so taking [`SPAWN_LOCK`] here (rather than in
/// each test) is what guarantees no case can bind the stub port concurrently.
fn continue_turn(
    dir: &tempfile::TempDir,
    api_key_env: Option<&str>,
    extra_args: &[&str],
) -> Output {
    let _guard = spawn_lock();
    let mut cmd = Command::new(bin());
    cmd.current_dir(dir.path())
        .args(["local", "message", "--continue", "--timeout-secs", "1"])
        .args(extra_args)
        .arg("probe");
    match api_key_env {
        Some(value) => cmd.env("CURIE_API_KEY", value),
        None => cmd.env_remove("CURIE_API_KEY"),
    };
    cmd.output().expect("run curie local message")
}

/// The baseline the other cases are measured against: a genuinely unset
/// `CURIE_API_KEY` hard-errors, per issue #112. Every "treated identically to
/// unset" claim below means "produces exactly this".
#[test]
fn unset_curie_api_key_hard_errors_on_continue() {
    let dir = project_with_recorded_env_key();
    let out = continue_turn(&dir, None, &[]);

    assert!(
        !out.status.success(),
        "an unset recorded api-key env source must fail the turn, got success"
    );
    assert!(
        err_str(&out).contains(UNSET_ENV_BAIL),
        "expected the unset-env bail, got stderr: {}",
        err_str(&out)
    );
}

/// AC6: `CURIE_API_KEY=""` is unset. Today clap reports `""` as an explicitly
/// supplied key, so the sentinel comparison in `state.rs` takes it as the user's
/// choice, the #112 bail never fires, and a blank key is sent onward.
#[test]
fn empty_curie_api_key_is_treated_as_unset_on_continue() {
    let dir = project_with_recorded_env_key();
    let out = continue_turn(&dir, Some(""), &[]);

    assert!(
        !out.status.success(),
        "an empty CURIE_API_KEY must fail the turn exactly as an unset one does, got success"
    );
    assert!(
        err_str(&out).contains(UNSET_ENV_BAIL),
        "an empty CURIE_API_KEY must resolve as absent and hit the unset-env bail; got stderr: {}",
        err_str(&out)
    );
}

/// The same rule at the flag, not just the env var: an explicitly empty
/// `--api-key ""` is an absent key. Pinned separately so a fix that special-cases
/// only the env source cannot pass.
#[test]
fn empty_api_key_flag_is_treated_as_unset_on_continue() {
    let dir = project_with_recorded_env_key();
    let out = continue_turn(&dir, None, &["--api-key", ""]);

    assert!(
        !out.status.success(),
        "an empty --api-key must fail the turn exactly as an unset one does, got success"
    );
    assert!(
        err_str(&out).contains(UNSET_ENV_BAIL),
        "an empty --api-key must resolve as absent and hit the unset-env bail; got stderr: {}",
        err_str(&out)
    );
}

/// The guard against over-normalizing: only EMPTY is absent. A real key must
/// still resolve as supplied, so the bail must NOT fire. Without this, a fix that
/// mapped every value to the sentinel would pass the three tests above.
#[test]
fn non_empty_curie_api_key_is_still_an_explicit_key() {
    let dir = project_with_recorded_env_key();
    let out = continue_turn(&dir, Some("sk-real-key"), &[]);

    assert!(
        !err_str(&out).contains(UNSET_ENV_BAIL),
        "a non-empty CURIE_API_KEY is an explicit key and must not be normalized to absent; got stderr: {}",
        err_str(&out)
    );
}

/// An explicitly empty `--api-key ""` must resolve to `$CURIE_API_KEY`, exactly
/// as an OMITTED flag does.
///
/// clap resolves an explicit flag ahead of its `env` source, so the env value is
/// already out of the running by the time the `value_parser` sees `""`. Before
/// the fix the parser mapped `""` straight to the `curie-dev-key` sentinel,
/// which means `--api-key ""` silently sent the well-known dev key on the wire
/// while the operator's real key sat unused in the environment.
///
/// Observed at the WIRE (the outgoing `X-API-Key`) rather than at any internal
/// shape, so the test holds wherever the normalization lands. `local versions`
/// is the cheapest verb that authenticates against a plain HTTP peer.
#[test]
fn empty_api_key_flag_falls_back_to_the_env_key_exactly_as_an_omitted_flag_does() {
    let omitted = api_key_on_the_wire(&[]);
    let empty = api_key_on_the_wire(&["--api-key", ""]);

    assert_eq!(
        omitted,
        Some("sk-real-from-env".to_string()),
        "baseline: an omitted --api-key must send $CURIE_API_KEY"
    );
    assert_eq!(
        empty, omitted,
        "an empty --api-key must send exactly what an omitted one sends ($CURIE_API_KEY), \
         not the {DEFAULT_API_KEY_SENTINEL} sentinel"
    );
}

/// The over-normalizing guard for the flag: an explicit NON-empty `--api-key`
/// still beats the env source. Without this, "always prefer the env" would pass
/// the case above.
#[test]
fn a_non_empty_api_key_flag_still_wins_over_the_env_key() {
    assert_eq!(
        api_key_on_the_wire(&["--api-key", "sk-explicit"]),
        Some("sk-explicit".to_string()),
        "an explicit non-empty --api-key must win over $CURIE_API_KEY"
    );
}

/// The dev sentinel `--api-key` falls back to when no env source is available.
const DEFAULT_API_KEY_SENTINEL: &str = "curie-dev-key";

/// Run `local versions` against a throwaway HTTP peer with
/// `CURIE_API_KEY=sk-real-from-env` on the CHILD, and report the `X-API-Key`
/// the CLI actually sent. `None` means the CLI never reached the peer.
fn api_key_on_the_wire(extra_args: &[&str]) -> Option<String> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind probe listener");
    let url = format!("http://{}", listener.local_addr().unwrap());
    let probe = std::thread::spawn(move || {
        let (stream, _) = listener.accept().ok()?;
        let mut reader = std::io::BufReader::new(stream);
        let mut key = None;
        loop {
            let mut line = String::new();
            if reader.read_line(&mut line).ok()? == 0 {
                break;
            }
            let line = line.trim_end();
            if line.is_empty() {
                break;
            }
            // HTTP header names are case-insensitive and reqwest lowercases them.
            if let Some((name, value)) = line.split_once(':') {
                if name.eq_ignore_ascii_case("x-api-key") {
                    key = Some(value.trim().to_string());
                }
            }
        }
        let stream = reader.get_mut();
        let _ = stream.write_all(
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n[]",
        );
        key
    });

    Command::new(bin())
        .args(["local", "versions", "demo", "--api-url", &url])
        .args(extra_args)
        .env("CURIE_API_KEY", "sk-real-from-env")
        .output()
        .expect("run curie local versions");

    probe.join().expect("probe thread")
}

/// AC6 on the `skill up` path: `ANTHROPIC_API_KEY=""` exported must NOT suppress
/// the Curie vault fallback.
///
/// `secret_store_env` gated on `var_os(name).is_some()`, which is TRUE for
/// `NAME=""` -- so an empty export shadowed a real saved key and forwarded the
/// empty value to the runner. The observable is the note `skill up` prints when
/// it hydrates a credential from the vault. Since #747 the run contacts Docker
/// BEFORE that point (the container-name preflight shells out to `docker ps`),
/// so the note is emitted after the preflight and before `docker run`; the run
/// failing at the container step afterwards is expected and irrelevant to the
/// assertion.
#[test]
fn empty_anthropic_api_key_does_not_suppress_the_vault_fallback() {
    let fixture = vault_with_saved_anthropic_key();
    let out = skill_up(&fixture, Some(""));

    assert!(
        err_str(&out).contains(VAULT_HYDRATION_NOTE),
        "an empty ANTHROPIC_API_KEY is absent, so the saved key must still be loaded from the vault; \
         got stderr: {}",
        err_str(&out)
    );
}

/// The over-normalizing guard for the vault gate: a REAL exported credential is
/// present and must still shadow the vault (the child inherits the export
/// directly). Without this, "always read the vault" would pass the case above.
#[test]
fn a_non_empty_anthropic_api_key_still_shadows_the_vault() {
    let fixture = vault_with_saved_anthropic_key();
    let out = skill_up(&fixture, Some("sk-exported-real"));

    assert!(
        !err_str(&out).contains(VAULT_HYDRATION_NOTE),
        "a non-empty exported ANTHROPIC_API_KEY is supplied and must shadow the vault; \
         got stderr: {}",
        err_str(&out)
    );
    // Absence alone is vacuous: since #747 the run contacts Docker before
    // credential resolution, so a box with no docker (or an unlucky preflight
    // failure) would satisfy the assertion above without ever resolving a
    // credential. The image rejection happens AFTER resolution, so seeing it is
    // the proof this test actually exercised the gate it guards.
    assert!(
        err_str(&out).contains(IMAGE_REJECTED_BAIL),
        "the run must have got past credential resolution to the docker run step; \
         got stderr: {}",
        err_str(&out)
    );
}

/// The note `skill up` prints when it hydrates a credential from the vault.
/// Reaching it is the observable proof the env value resolved as "absent".
const VAULT_HYDRATION_NOTE: &str = "ANTHROPIC_API_KEY: loaded from Curie private storage";

/// Docker's own rejection of [`UNRESOLVABLE_IMAGE`], raised locally at the
/// `docker run` step, which is the first thing after credential resolution.
const IMAGE_REJECTED_BAIL: &str = "invalid reference format";

/// An image reference docker refuses to parse, so `docker run` fails on the
/// client with no daemon pull and no registry contact at all. A merely
/// non-existent TAG would be a valid reference, which sends docker to a registry
/// and makes these tests depend on the network and on pull rate limits.
const UNRESOLVABLE_IMAGE: &str = "curie-runner:747-invalid-reference-format!";

/// A private Curie config dir holding a saved `ANTHROPIC_API_KEY`, plus a real
/// scaffolded bundle for `skill up` to read. Both are built by driving the real
/// `secrets set` and `init` verbs rather than hand-writing a vault file or a
/// manifest, so neither fixture can drift from the shapes the CLI loads.
fn vault_with_saved_anthropic_key() -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("tempdir");
    let seed = Command::new(bin())
        .args(["secrets", "set", "ANTHROPIC_API_KEY", "--from-env", "SEED"])
        .env("CURIE_CONFIG_DIR", dir.path().join("cfg"))
        .env("SEED", "sk-vault-real")
        .output()
        .expect("run curie secrets set");
    assert!(seed.status.success(), "seed vault: {}", err_str(&seed));

    let init = Command::new(bin())
        .args(["init", "demo-agent", "--dir"])
        .arg(dir.path().join("bundle"))
        .stdin(std::process::Stdio::null())
        .output()
        .expect("run curie init");
    assert!(init.status.success(), "scaffold bundle: {}", err_str(&init));
    dir
}

/// `skill up` in that fixture's bundle, with a controlled `ANTHROPIC_API_KEY` on
/// the CHILD process. Each fixture owns its own config dir, so these spawns share
/// no state and need no [`SPAWN_LOCK`] (they bind no fixed port either).
///
/// The name and image are pinned so the run is deterministic on a dirty box.
/// `--name` is unique per process, because the default `curie-runner-local` is
/// often already taken by a real local runner and `skill up`'s name-conflict
/// preflight (#747) would then fail the run before the vault note is printed;
/// deriving it from the pid also keeps parallel test runs from colliding.
/// `--image` names a reference docker rejects outright, so the run still dies at
/// the docker step, after the credential resolution these tests observe, without
/// ever booting a container or touching a registry.
fn skill_up(fixture: &tempfile::TempDir, anthropic_key: Option<&str>) -> Output {
    let mut cmd = Command::new(bin());
    cmd.current_dir(fixture.path().join("bundle"))
        .args(["--debug", "skill", "up"])
        .arg("--name")
        .arg(format!("curie-747-cred-test-{}", std::process::id()))
        .args(["--image", UNRESOLVABLE_IMAGE])
        .env("CURIE_CONFIG_DIR", fixture.path().join("cfg"));
    match anthropic_key {
        Some(value) => cmd.env("ANTHROPIC_API_KEY", value),
        None => cmd.env_remove("ANTHROPIC_API_KEY"),
    };
    // Never let an ambient BYO credential pre-empt the ANTHROPIC_API_KEY probe.
    cmd.env_remove("CURIE_CREDENTIALS")
        .env_remove("CLAUDE_CODE_OAUTH_TOKEN");
    cmd.output().expect("run curie skill up")
}

const SAVED_CURIE_CREDENTIALS: &str = "vault-example-credential-sentinel";
const SAVED_ANTHROPIC_CREDENTIALS: &str = "stored-anthropic-credential-sentinel";
const SHELL_CURIE_CREDENTIALS: &str = "shell-example-credential-sentinel";
const ENV_FILE_CURIE_CREDENTIALS: &str = "file-example-credential-sentinel";
const EXPLICIT_MODEL: &str = "z-ai/glm-5.2";
const CURIE_CREDENTIALS_VAULT_NOTE: &str =
    "CURIE_CREDENTIALS: loaded from Curie private storage for this run";

/// A private Curie config dir holding the provider neutral model credential.
/// The fixture uses the public command, so the tests cover the same persisted
/// shape a user creates rather than depending on vault internals.
fn vault_with_saved_curie_credentials() -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("tempdir");
    save_model_credential(&dir, "CURIE_CREDENTIALS", SAVED_CURIE_CREDENTIALS);
    dir
}

fn save_model_credential(fixture: &tempfile::TempDir, name: &str, value: &str) {
    let seed = Command::new(bin())
        .args(["secrets", "set", name, "--from-env", "SEED"])
        .env("CURIE_CONFIG_DIR", fixture.path().join("cfg"))
        .env("SEED", value)
        .output()
        .expect("run curie secrets set");
    assert!(seed.status.success(), "seed vault: {}", err_str(&seed));
}

/// A saved credential whose index cannot be read. Credential-free modes must
/// succeed against this fixture, which proves they never consult private
/// storage rather than merely discarding a credential after loading it.
fn unreadable_vault_with_saved_curie_credentials() -> tempfile::TempDir {
    let dir = vault_with_saved_curie_credentials();
    std::fs::write(dir.path().join("cfg/secrets.json"), "not valid json")
        .expect("poison private store index");
    dir
}

/// Start a real CLI process against the isolated credential store with every
/// ambient model input removed. Individual tests can then add one explicit
/// input without racing the test process environment.
fn promotion_command(fixture: &tempfile::TempDir) -> Command {
    let mut cmd = Command::new(bin());
    cmd.env("CURIE_CONFIG_DIR", fixture.path().join("cfg"))
        .env_remove("CURIE_CREDENTIALS")
        .env_remove("CURIE_MODEL_CREDENTIALS")
        .env_remove("ANTHROPIC_API_KEY")
        .env_remove("CLAUDE_CODE_OAUTH_TOKEN")
        .env_remove("CURIE_MODEL")
        .env_remove("CURIE_FAKE_MODEL");
    cmd
}

fn repository_path(relative: &str) -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("cli package has repository parent")
        .join(relative)
}

fn assert_secret_absent(out: &Output, secret: &str) {
    assert!(
        !String::from_utf8_lossy(&out.stdout).contains(secret),
        "raw credential must be absent from stdout"
    );
    assert!(
        !String::from_utf8_lossy(&out.stderr).contains(secret),
        "raw credential must be absent from stderr"
    );
}

#[test]
fn saved_curie_credentials_and_explicit_model_reach_local_up_dry_run() {
    let fixture = vault_with_saved_curie_credentials();
    let mut cmd = promotion_command(&fixture);
    let out = cmd
        .args([
            "local",
            "up",
            "--dry-run",
            "--model",
            EXPLICIT_MODEL,
            "--file",
        ])
        .arg(repository_path("compose.dev.yaml"))
        .output()
        .expect("run curie local up dry run");

    assert!(out.status.success(), "local up dry run must succeed");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains(CURIE_CREDENTIALS_VAULT_NOTE));
    assert!(stdout.contains("CURIE_FAKE_MODEL=0"));
    assert!(stdout.contains("CURIE_MODEL=z-ai/glm-5.2"));
    assert!(stdout.contains("CURIE_CREDENTIALS=vault-ex***"));
    assert_secret_absent(&out, SAVED_CURIE_CREDENTIALS);
}

#[test]
fn saved_curie_credentials_and_explicit_model_reach_local_rebuild_dry_run() {
    let fixture = vault_with_saved_curie_credentials();
    let mut cmd = promotion_command(&fixture);
    let out = cmd
        .args([
            "local",
            "rebuild",
            "curie-worker",
            "--dry-run",
            "--model",
            EXPLICIT_MODEL,
            "--file",
        ])
        .arg(repository_path("compose.dev.yaml"))
        .output()
        .expect("run curie local rebuild dry run");

    assert!(out.status.success(), "local rebuild dry run must succeed");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains(CURIE_CREDENTIALS_VAULT_NOTE));
    assert!(stdout.contains("CURIE_FAKE_MODEL=0"));
    assert!(stdout.contains("CURIE_MODEL=z-ai/glm-5.2"));
    assert!(stdout.contains("CURIE_CREDENTIALS=vault-ex***"));
    assert_secret_absent(&out, SAVED_CURIE_CREDENTIALS);
}

#[test]
fn saved_curie_credentials_survive_local_comms_worker_recreation() {
    let fixture = vault_with_saved_curie_credentials();
    let mut cmd = promotion_command(&fixture);
    let out = cmd
        .args([
            "local",
            "comms",
            "--slack",
            "--dry-run",
            "--model",
            EXPLICIT_MODEL,
            "--app-token",
            "xapp-example",
            "--bot-token",
            "xoxb-example",
            "--file",
        ])
        .arg(repository_path("compose.dev.yaml"))
        .output()
        .expect("run curie local comms dry run");

    assert!(out.status.success(), "local comms dry run must succeed");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains(CURIE_CREDENTIALS_VAULT_NOTE));
    assert!(stdout.contains("CURIE_FAKE_MODEL=0"));
    assert!(stdout.contains("CURIE_MODEL=z-ai/glm-5.2"));
    assert!(stdout.contains("CURIE_CREDENTIALS=vault-ex***"));
    assert_secret_absent(&out, SAVED_CURIE_CREDENTIALS);
}

#[test]
fn saved_curie_credentials_survive_local_comms_disconnect_worker_recreation() {
    let fixture = vault_with_saved_curie_credentials();
    let mut cmd = promotion_command(&fixture);
    let out = cmd
        .args([
            "local",
            "comms",
            "--slack",
            "--disconnect",
            "--dry-run",
            "--model",
            EXPLICIT_MODEL,
            "--file",
        ])
        .arg(repository_path("compose.dev.yaml"))
        .output()
        .expect("run curie local comms disconnect dry run");

    assert!(out.status.success(), "local comms dry run must succeed");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains(CURIE_CREDENTIALS_VAULT_NOTE));
    assert!(stdout.contains("CURIE_FAKE_MODEL=0"));
    assert!(stdout.contains("CURIE_MODEL=z-ai/glm-5.2"));
    assert!(stdout.contains("CURIE_CREDENTIALS=vault-ex***"));
    assert_secret_absent(&out, SAVED_CURIE_CREDENTIALS);
}

#[test]
fn shell_curie_credentials_take_priority_over_saved_credentials_for_local_up() {
    let fixture = vault_with_saved_curie_credentials();
    let mut cmd = promotion_command(&fixture);
    let out = cmd
        .args(["local", "up", "--dry-run", "--file"])
        .arg(repository_path("compose.dev.yaml"))
        .env("CURIE_CREDENTIALS", SHELL_CURIE_CREDENTIALS)
        .output()
        .expect("run curie local up dry run with shell credential");

    assert!(out.status.success(), "local up dry run must succeed");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!stderr.contains(CURIE_CREDENTIALS_VAULT_NOTE));
    assert!(stdout.contains("CURIE_FAKE_MODEL=0"));
    assert!(!stdout.contains("CURIE_CREDENTIALS=vault-ex***"));
    assert_secret_absent(&out, SAVED_CURIE_CREDENTIALS);
    assert_secret_absent(&out, SHELL_CURIE_CREDENTIALS);
}

#[test]
fn shell_curie_credentials_do_not_hydrate_stored_sdk_credentials() {
    let fixture = vault_with_saved_curie_credentials();
    save_model_credential(&fixture, "ANTHROPIC_API_KEY", SAVED_ANTHROPIC_CREDENTIALS);
    let mut cmd = promotion_command(&fixture);
    let out = cmd
        .args(["local", "up", "--dry-run", "--file"])
        .arg(repository_path("compose.dev.yaml"))
        .env("CURIE_CREDENTIALS", SHELL_CURIE_CREDENTIALS)
        .output()
        .expect("run curie local up with shell BYO credential");

    assert!(out.status.success(), "local up dry run must succeed");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!stderr.contains("ANTHROPIC_API_KEY: loaded from Curie private storage"));
    assert!(!stdout.contains("ANTHROPIC_API_KEY="));
    assert_secret_absent(&out, SHELL_CURIE_CREDENTIALS);
    assert_secret_absent(&out, SAVED_ANTHROPIC_CREDENTIALS);
}

#[test]
fn saved_curie_credentials_take_priority_over_env_file_for_local_up() {
    let fixture = vault_with_saved_curie_credentials();
    let env_file = fixture.path().join("bundle.env");
    std::fs::write(
        &env_file,
        format!("CURIE_CREDENTIALS={ENV_FILE_CURIE_CREDENTIALS}\n"),
    )
    .expect("write credential env file");
    let mut cmd = promotion_command(&fixture);
    let out = cmd
        .args(["local", "up", "--dry-run", "--env-file"])
        .arg(&env_file)
        .args(["--file"])
        .arg(repository_path("compose.dev.yaml"))
        .output()
        .expect("run curie local up dry run with saved and file credentials");

    assert!(out.status.success(), "local up dry run must succeed");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains(CURIE_CREDENTIALS_VAULT_NOTE));
    assert!(!stderr.contains("loaded from --env-file"));
    assert!(stdout.contains("CURIE_CREDENTIALS=vault-ex***"));
    assert!(!stdout.contains("CURIE_CREDENTIALS=file-ex***"));
    assert_secret_absent(&out, SAVED_CURIE_CREDENTIALS);
    assert_secret_absent(&out, ENV_FILE_CURIE_CREDENTIALS);
}

#[test]
fn saved_curie_credentials_and_explicit_model_reach_cluster_up_dry_run() {
    let fixture = vault_with_saved_curie_credentials();
    let mut cmd = promotion_command(&fixture);
    let out = cmd
        .args([
            "cluster",
            "up",
            "--dry-run",
            "--dev",
            "--model",
            EXPLICIT_MODEL,
            "--chart",
        ])
        .arg(repository_path("charts/curie"))
        .output()
        .expect("run curie cluster up dry run");

    assert!(out.status.success(), "cluster up dry run must succeed");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains(CURIE_CREDENTIALS_VAULT_NOTE));
    assert!(stdout.contains("<secret values file: agentSandbox.runner.credentials=vault-ex***>"));
    assert!(stdout.contains("agentSandbox.runner.model=z-ai/glm-5.2"));
    assert_secret_absent(&out, SAVED_CURIE_CREDENTIALS);
}

#[test]
fn shell_curie_credentials_take_priority_over_saved_credentials_for_cluster_up() {
    let fixture = vault_with_saved_curie_credentials();
    let mut cmd = promotion_command(&fixture);
    let out = cmd
        .args([
            "cluster",
            "up",
            "--dry-run",
            "--dev",
            "--model",
            EXPLICIT_MODEL,
            "--chart",
        ])
        .arg(repository_path("charts/curie"))
        .env("CURIE_CREDENTIALS", SHELL_CURIE_CREDENTIALS)
        .output()
        .expect("run curie cluster up dry run with shell credential");

    assert!(out.status.success(), "cluster up dry run must succeed");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!stderr.contains(CURIE_CREDENTIALS_VAULT_NOTE));
    assert!(stdout.contains("<secret values file: agentSandbox.runner.credentials=shell-ex***>"));
    assert!(!stdout.contains("agentSandbox.runner.credentials=vault-ex***"));
    assert_secret_absent(&out, SAVED_CURIE_CREDENTIALS);
    assert_secret_absent(&out, SHELL_CURIE_CREDENTIALS);
}

#[test]
fn cluster_fake_model_suppresses_the_saved_credential_but_keeps_explicit_model() {
    let fixture = vault_with_saved_curie_credentials();
    let mut cmd = promotion_command(&fixture);
    let out = cmd
        .args([
            "cluster",
            "up",
            "--dry-run",
            "--dev",
            "--fake-model",
            "--model",
            EXPLICIT_MODEL,
            "--chart",
        ])
        .arg(repository_path("charts/curie"))
        .output()
        .expect("run curie cluster up dry run with fake model");

    assert!(out.status.success(), "cluster up dry run must succeed");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!stderr.contains(CURIE_CREDENTIALS_VAULT_NOTE));
    assert!(stdout.contains("agentSandbox.runner.model=z-ai/glm-5.2"));
    assert!(!stdout.contains("agentSandbox.runner.credentials="));
    assert!(!stdout.contains("agentSandbox.runner.credentials=vault-ex***"));
    assert!(!stdout.contains("agentSandbox.runner.fakeModel=false"));
    assert_secret_absent(&out, SAVED_CURIE_CREDENTIALS);
}

#[test]
fn local_credential_free_modes_do_not_read_private_storage() {
    for (label, extra_args, fake_env) in [
        ("local model", vec!["--local-model", "qwen3:8b"], None),
        ("explicit fake", Vec::new(), Some("1")),
    ] {
        let fixture = unreadable_vault_with_saved_curie_credentials();
        let mut cmd = promotion_command(&fixture);
        cmd.args(["local", "up", "--dry-run", "--file"])
            .arg(repository_path("compose.dev.yaml"))
            .args(extra_args);
        if let Some(value) = fake_env {
            cmd.env("CURIE_FAKE_MODEL", value);
        }
        let out = cmd.output().expect("run credential-free local up dry run");

        assert!(
            out.status.success(),
            "{label} must not be blocked by private storage: {}",
            err_str(&out)
        );
        let stdout = String::from_utf8_lossy(&out.stdout);
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(!stderr.contains(CURIE_CREDENTIALS_VAULT_NOTE));
        assert!(!stdout.contains("CURIE_CREDENTIALS="));
        assert_secret_absent(&out, SAVED_CURIE_CREDENTIALS);
    }
}

#[test]
fn cluster_credential_free_modes_do_not_read_private_storage() {
    for (label, mode_args) in [
        ("fake model", vec!["--fake-model"]),
        (
            "local model",
            vec![
                "--local-model",
                "qwen3:8b",
                "--set",
                "inference.pullModel=false",
            ],
        ),
    ] {
        let fixture = unreadable_vault_with_saved_curie_credentials();
        let mut cmd = promotion_command(&fixture);
        let out = cmd
            .args(["cluster", "up", "--dry-run", "--dev", "--chart"])
            .arg(repository_path("charts/curie"))
            .args(mode_args)
            .output()
            .expect("run credential-free cluster up dry run");

        assert!(
            out.status.success(),
            "{label} must not be blocked by private storage: {}",
            err_str(&out)
        );
        let stdout = String::from_utf8_lossy(&out.stdout);
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(!stderr.contains(CURIE_CREDENTIALS_VAULT_NOTE));
        assert!(!stdout.contains("agentSandbox.runner.credentials="));
        assert_secret_absent(&out, SAVED_CURIE_CREDENTIALS);
    }
}

#[test]
fn corrupt_private_storage_does_not_block_default_up_modes() {
    for tier in ["local", "cluster"] {
        let fixture = unreadable_vault_with_saved_curie_credentials();
        let mut cmd = promotion_command(&fixture);
        cmd.args([tier, "up", "--dry-run"]);
        if tier == "local" {
            cmd.arg("--file").arg(repository_path("compose.dev.yaml"));
        } else {
            cmd.args(["--dev", "--chart"])
                .arg(repository_path("charts/curie"));
        }
        let out = cmd.output().expect("run up with corrupt private storage");

        assert!(
            out.status.success(),
            "{tier} up must fall back when private storage is corrupt: {}",
            err_str(&out)
        );
        let stdout = String::from_utf8_lossy(&out.stdout);
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(stderr.contains("Saved model credentials could not be read"));
        if tier == "local" {
            assert!(!stdout.contains("CURIE_FAKE_MODEL=0"));
            assert!(!stdout.contains("CURIE_CREDENTIALS="));
        } else {
            assert!(!stdout.contains("agentSandbox.runner.credentials="));
        }
        assert_secret_absent(&out, SAVED_CURIE_CREDENTIALS);
    }
}

#[test]
fn model_and_local_model_are_rejected_together_for_each_up_tier() {
    for tier in ["local", "cluster"] {
        let out = Command::new(bin())
            .args([
                tier,
                "up",
                "--model",
                EXPLICIT_MODEL,
                "--local-model",
                "qwen3:8b",
            ])
            .output()
            .expect("run curie up with conflicting model flags");

        assert_eq!(out.status.code(), Some(2), "{tier} up must report usage");
        let stderr = err_str(&out);
        assert!(stderr.contains("--model"), "{tier}: {stderr}");
        assert!(stderr.contains("--local-model"), "{tier}: {stderr}");
        assert!(stderr.contains("cannot be used with"), "{tier}: {stderr}");
    }
}

#[test]
fn model_and_local_model_are_rejected_together_for_local_rebuild() {
    let out = Command::new(bin())
        .args([
            "local",
            "rebuild",
            "curie-worker",
            "--model",
            EXPLICIT_MODEL,
            "--local-model",
            "qwen3:8b",
        ])
        .output()
        .expect("run curie local rebuild with conflicting model flags");

    assert_eq!(
        out.status.code(),
        Some(2),
        "local rebuild must report usage"
    );
    let stderr = err_str(&out);
    assert!(stderr.contains("--model"), "{stderr}");
    assert!(stderr.contains("--local-model"), "{stderr}");
    assert!(stderr.contains("cannot be used with"), "{stderr}");
}

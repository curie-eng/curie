//! Per-install local stack credentials (#3557).
//!
//! `curie local up` generates an API key and a Postgres password for the
//! install, stores them at `$CURIE_CONFIG_DIR/local/<compose project>.json`
//! (mode 0600), and hands them to compose as the masked secret env
//! `CURIE_LOCAL_API_KEY` / `CURIE_LOCAL_POSTGRES_PASSWORD`. Every verb whose
//! `--api-key` falls back to the dev sentinel sends the stored key instead, so
//! the randomized key is transparent to the operator.
//!
//! Observed through the built binary only: the `X-API-Key` a verb puts on the
//! wire, and the `local up --dry-run` plan line. Env is set on the CHILD process,
//! never on the test process, and every case owns a private `CURIE_CONFIG_DIR`.

use std::io::{BufRead, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const STORED_API_KEY: &str = "stored-local-api-key-placeholder";
const STORED_PG_PASSWORD: &str = "pgpass-local-placeholder-value";
const DEFAULT_API_KEY_SENTINEL: &str = "curie-dev-key";
const DEFAULT_PROJECT: &str = "curie";

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..")
}

fn output_text(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned() + &String::from_utf8_lossy(&output.stderr)
}

/// `OpsCommand::display`'s mask for a secret env value longer than eight chars:
/// the first eight characters followed by `***`.
fn masked(value: &str) -> String {
    let shown: String = value.chars().take(8).collect();
    format!("{shown}***")
}

fn credentials_path(config_dir: &Path, project: &str) -> PathBuf {
    config_dir.join("local").join(format!("{project}.json"))
}

/// Seed the credential store the way `local up` leaves it: a 0600 JSON object
/// under `<config>/local/<project>.json`.
fn seed_credentials(config_dir: &Path, project: &str) {
    let path = credentials_path(config_dir, project);
    std::fs::create_dir_all(path.parent().unwrap()).expect("create local credential dir");
    std::fs::write(
        &path,
        serde_json::json!({
            "api_key": STORED_API_KEY,
            "postgres_password": STORED_PG_PASSWORD,
        })
        .to_string(),
    )
    .expect("write local credentials");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
        .expect("chmod local credentials");
}

/// Run `local versions` against a throwaway HTTP peer and report the
/// `X-API-Key` the CLI sent. `None` means the CLI never reached the peer.
fn api_key_on_the_wire(
    config_dir: &Path,
    curie_api_key_env: Option<&str>,
    extra_args: &[&str],
) -> (Option<String>, Output) {
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

    let mut cmd = Command::new(bin());
    cmd.args(["local", "versions", "demo", "--api-url", &url])
        .args(extra_args)
        .env("CURIE_CONFIG_DIR", config_dir)
        .env_remove("COMPOSE_PROJECT_NAME")
        .env_remove("COMPOSE_FILE")
        .env_remove("CURIE_API_URL");
    match curie_api_key_env {
        Some(value) => cmd.env("CURIE_API_KEY", value),
        None => cmd.env_remove("CURIE_API_KEY"),
    };
    let out = cmd.output().expect("run curie local versions");

    // Unblock the probe if the CLI exited without ever connecting.
    let _ = std::net::TcpStream::connect(url.trim_start_matches("http://"));
    (probe.join().expect("probe thread"), out)
}

#[test]
fn stored_key_is_sent_when_no_flag_or_env_is_given() {
    let dir = tempfile::tempdir().expect("tempdir");
    seed_credentials(dir.path(), DEFAULT_PROJECT);
    let (key, out) = api_key_on_the_wire(dir.path(), None, &[]);
    assert_eq!(
        key.as_deref(),
        Some(STORED_API_KEY),
        "with no --api-key and no CURIE_API_KEY, the verb must send the stored install key; {}",
        output_text(&out)
    );
}

#[test]
fn empty_flag_and_empty_env_still_send_the_stored_key() {
    let dir = tempfile::tempdir().expect("tempdir");
    seed_credentials(dir.path(), DEFAULT_PROJECT);
    let (key, out) = api_key_on_the_wire(dir.path(), Some(""), &["--api-key", ""]);
    assert_eq!(
        key.as_deref(),
        Some(STORED_API_KEY),
        "an empty --api-key and empty CURIE_API_KEY are absent, so the stored key applies; {}",
        output_text(&out)
    );
}

#[test]
fn no_stored_file_still_sends_the_dev_sentinel() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (key, out) = api_key_on_the_wire(dir.path(), None, &[]);
    assert_eq!(
        key.as_deref(),
        Some(DEFAULT_API_KEY_SENTINEL),
        "with no stored credentials the verb must keep sending the dev sentinel; {}",
        output_text(&out)
    );
}

#[test]
fn curie_api_key_env_wins_over_the_stored_key() {
    let dir = tempfile::tempdir().expect("tempdir");
    seed_credentials(dir.path(), DEFAULT_PROJECT);
    let (key, out) = api_key_on_the_wire(dir.path(), Some("env-api-key-placeholder"), &[]);
    assert_eq!(
        key.as_deref(),
        Some("env-api-key-placeholder"),
        "CURIE_API_KEY must win over the stored install key; {}",
        output_text(&out)
    );
}

#[test]
fn explicit_api_key_flag_wins_over_the_stored_key() {
    let dir = tempfile::tempdir().expect("tempdir");
    seed_credentials(dir.path(), DEFAULT_PROJECT);
    let (key, out) = api_key_on_the_wire(
        dir.path(),
        Some("env-api-key-placeholder"),
        &["--api-key", "flag-api-key-placeholder"],
    );
    assert_eq!(
        key.as_deref(),
        Some("flag-api-key-placeholder"),
        "an explicit --api-key must win over env and the stored key; {}",
        output_text(&out)
    );
}

/// A `docker` stub first on PATH that fails every call, so `local up --dry-run`
/// can never reach a daemon or depend on this box's volumes.
fn failing_docker_dir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("tempdir");
    let docker = dir.path().join("docker");
    std::fs::write(&docker, "#!/bin/sh\nexit 1\n").expect("write docker stub");
    std::fs::set_permissions(&docker, std::fs::Permissions::from_mode(0o755))
        .expect("chmod docker stub");
    dir
}

fn local_up_dry_run(config_dir: &Path, curie_api_key_env: Option<&str>) -> Output {
    let tools = failing_docker_dir();
    let mut paths = vec![tools.path().to_path_buf()];
    if let Some(existing) = std::env::var_os("PATH") {
        paths.extend(std::env::split_paths(&existing));
    }
    let compose = repo_root().join("compose.dev.yaml");
    let mut cmd = Command::new(bin());
    cmd.args(["local", "up", "--dry-run", "-f"])
        .arg(&compose)
        .current_dir(repo_root())
        .env("PATH", std::env::join_paths(paths).expect("join PATH"))
        .env("CURIE_CONFIG_DIR", config_dir)
        .env_remove("COMPOSE_PROJECT_NAME")
        .env_remove("COMPOSE_FILE")
        .env_remove("CURIE_API_URL");
    match curie_api_key_env {
        Some(value) => cmd.env("CURIE_API_KEY", value),
        None => cmd.env_remove("CURIE_API_KEY"),
    };
    cmd.output().expect("run curie local up --dry-run")
}

#[test]
fn dry_run_passes_stored_credentials_masked() {
    let dir = tempfile::tempdir().expect("tempdir");
    seed_credentials(dir.path(), DEFAULT_PROJECT);
    let out = local_up_dry_run(dir.path(), None);
    let text = output_text(&out);
    assert!(
        out.status.success(),
        "local up --dry-run must succeed; {text}"
    );
    for (name, value) in [
        ("CURIE_LOCAL_API_KEY", STORED_API_KEY),
        ("CURIE_LOCAL_POSTGRES_PASSWORD", STORED_PG_PASSWORD),
    ] {
        let expected = format!("{name}={}", masked(value));
        assert!(
            text.contains(&expected),
            "dry-run plan must carry {name} masked as {expected:?}; {text}"
        );
        assert!(
            !text.contains(value),
            "dry-run plan must never print the plaintext {name}; {text}"
        );
    }
}

#[test]
fn dry_run_without_a_stored_file_does_not_create_one() {
    let dir = tempfile::tempdir().expect("tempdir");
    let out = local_up_dry_run(dir.path(), None);
    let text = output_text(&out);
    assert!(
        out.status.success(),
        "local up --dry-run must succeed; {text}"
    );
    for name in ["CURIE_LOCAL_API_KEY=", "CURIE_LOCAL_POSTGRES_PASSWORD="] {
        assert!(
            text.contains(name),
            "dry-run plan must carry {name} even with no stored credentials; {text}"
        );
    }
    assert!(
        !credentials_path(dir.path(), DEFAULT_PROJECT).exists(),
        "--dry-run must not persist generated credentials"
    );
}

#[test]
fn dry_run_uses_an_explicit_curie_api_key_as_the_install_key() {
    let dir = tempfile::tempdir().expect("tempdir");
    let explicit = "explicit-up-key-placeholder";
    let out = local_up_dry_run(dir.path(), Some(explicit));
    let text = output_text(&out);
    assert!(
        out.status.success(),
        "local up --dry-run must succeed; {text}"
    );
    let expected = format!("CURIE_LOCAL_API_KEY={}", masked(explicit));
    assert!(
        text.contains(&expected),
        "an exported CURIE_API_KEY must become the install key ({expected:?}); {text}"
    );
    assert!(
        !text.contains(explicit),
        "dry-run plan must never print the plaintext key; {text}"
    );
    assert!(
        !credentials_path(dir.path(), DEFAULT_PROJECT).exists(),
        "--dry-run must not persist the install key"
    );
}

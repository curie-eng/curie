//! Console login command validation and offline planning at the binary boundary.
//! Real mint and session exchange coverage lives in local/test_console_login.py.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::{Command, Output};

const SUBJECT: &str = "operator@example.com";
const UNREACHABLE_API: &str = "http://127.0.0.1:1";
const AMBIENT_KEY: &str = "fixture_ambient_api_key_must_not_appear";

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn text(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned() + &String::from_utf8_lossy(&output.stderr)
}

struct Fixture {
    temp: tempfile::TempDir,
    path: std::ffi::OsString,
    tool_log: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().expect("create private console login fixture");
        let tool_log = temp.path().join("tools.log");
        for tool in ["kubectl", "helm", "docker"] {
            let executable = temp.path().join(tool);
            fs::write(
                &executable,
                "#!/bin/sh\nprintf '%s %s\\n' \"$0\" \"$*\" >> \"$CURIE_TEST_TOOL_LOG\"\nexit 64\n",
            )
            .expect("write discovery refusal fixture");
            fs::set_permissions(&executable, fs::Permissions::from_mode(0o755))
                .expect("make fixture executable");
        }
        let mut paths = vec![temp.path().to_path_buf()];
        paths.extend(std::env::split_paths(
            &std::env::var_os("PATH").unwrap_or_default(),
        ));
        Self {
            temp,
            path: std::env::join_paths(paths).expect("join fixture PATH"),
            tool_log,
        }
    }

    fn run(&self, args: &[&str]) -> Output {
        Command::new(bin())
            .args(args)
            .env("PATH", &self.path)
            .env("CURIE_TEST_TOOL_LOG", &self.tool_log)
            .env("CURIE_CONFIG_DIR", self.temp.path().join("config"))
            .env("CURIE_API_KEY", AMBIENT_KEY)
            .env_remove("CURIE_API_URL")
            .env_remove("COMPOSE_FILE")
            .env_remove("COMPOSE_PROJECT_NAME")
            .env("NO_COLOR", "1")
            .output()
            .expect("run console login command")
    }

    fn assert_no_discovery(&self) {
        assert!(
            !self.tool_log.exists(),
            "console login contacted a discovery tool: {}",
            fs::read_to_string(&self.tool_log).unwrap_or_default()
        );
        assert!(
            !self.temp.path().join("config/local").exists(),
            "offline planning or invalid input wrote the credential store"
        );
    }
}

#[test]
fn console_login_help_exposes_subject_and_connection_flags_without_a_manual_key() {
    for tier in ["local", "cluster"] {
        let fixture = Fixture::new();
        let output = fixture.run(&[tier, "console", "login", "--help"]);
        let rendered = text(&output);
        assert!(output.status.success(), "{tier} help failed: {rendered}");
        for flag in ["--subject", "--api-url", "--dry-run", "--json"] {
            assert!(
                rendered.contains(flag),
                "{tier} help lacks {flag}: {rendered}"
            );
        }
        if tier == "cluster" {
            for flag in ["--namespace", "--release"] {
                assert!(
                    rendered.contains(flag),
                    "cluster help lacks {flag}: {rendered}"
                );
            }
        }
        assert!(
            !rendered.contains("--api-key"),
            "{tier} accepts a manual key"
        );
        assert!(
            !rendered.contains(AMBIENT_KEY),
            "{tier} help disclosed the env key"
        );
        fixture.assert_no_discovery();
    }
}

#[test]
fn console_login_dry_run_is_offline_and_names_the_mint_endpoint_and_subject() {
    for tier in ["local", "cluster"] {
        for json in [false, true] {
            let fixture = Fixture::new();
            let mut args = Vec::new();
            if json {
                args.push("--json");
            }
            args.extend([
                tier,
                "console",
                "login",
                "--subject",
                SUBJECT,
                "--api-url",
                UNREACHABLE_API,
                "--dry-run",
            ]);
            if tier == "cluster" {
                args.extend(["--namespace", "acme-system", "--release", "acme-release"]);
            }
            let output = fixture.run(&args);
            let rendered = text(&output);
            assert!(output.status.success(), "{tier} dry run failed: {rendered}");
            assert!(
                rendered.contains(SUBJECT),
                "dry run omitted the subject: {rendered}"
            );
            assert!(
                rendered.contains(&format!("{UNREACHABLE_API}/console/login-codes")),
                "dry run omitted the exact mint endpoint: {rendered}"
            );
            assert!(
                !rendered.contains(AMBIENT_KEY),
                "dry run disclosed the env key"
            );
            assert!(
                !rendered.contains("curie-dev-key"),
                "dry run printed the default key"
            );
            if json {
                let plan: serde_json::Value = serde_json::from_slice(&output.stdout)
                    .unwrap_or_else(|error| panic!("dry run must emit one JSON object: {error}"));
                assert!(plan.is_object(), "dry run emitted a nonobject JSON value");
            }
            fixture.assert_no_discovery();
        }
    }
}

#[test]
fn console_login_dry_run_accepts_default_connections_without_a_credential_store() {
    for tier in ["local", "cluster"] {
        let fixture = Fixture::new();
        let output = fixture.run(&[tier, "console", "login", "--subject", SUBJECT, "--dry-run"]);
        let rendered = text(&output);
        assert!(
            output.status.success(),
            "default dry run failed: {rendered}"
        );
        assert!(rendered.contains("/console/login-codes"), "{rendered}");
        assert!(rendered.contains(SUBJECT), "{rendered}");
        assert!(
            !rendered.contains(AMBIENT_KEY),
            "dry run disclosed the env key"
        );
        fixture.assert_no_discovery();
    }
}

#[test]
fn console_login_rejects_missing_and_blank_subjects_before_discovery() {
    for tier in ["local", "cluster"] {
        for subject in [None, Some(""), Some(" \t\n")] {
            let fixture = Fixture::new();
            let mut args = vec![tier, "console", "login", "--api-url", UNREACHABLE_API];
            if let Some(subject) = subject {
                args.extend(["--subject", subject]);
            }
            let output = fixture.run(&args);
            let rendered = text(&output);
            assert_eq!(output.status.code(), Some(2), "invalid subject: {rendered}");
            assert!(
                rendered.to_ascii_lowercase().contains("subject"),
                "the refusal must identify the subject: {rendered}"
            );
            assert!(
                !rendered.contains(AMBIENT_KEY),
                "the refusal disclosed the env key"
            );
            fixture.assert_no_discovery();
        }
    }
}

#[test]
fn console_login_rejects_a_manual_api_key_before_discovery() {
    for tier in ["local", "cluster"] {
        let fixture = Fixture::new();
        let output = fixture.run(&[
            tier,
            "console",
            "login",
            "--subject",
            SUBJECT,
            "--api-key",
            "fixture_manual_key",
        ]);
        let rendered = text(&output);
        assert_eq!(
            output.status.code(),
            Some(2),
            "manual key accepted: {rendered}"
        );
        assert!(
            rendered.contains("--api-key") && rendered.contains("unexpected argument"),
            "the manual key flag must be unknown: {rendered}"
        );
        fixture.assert_no_discovery();
    }
}

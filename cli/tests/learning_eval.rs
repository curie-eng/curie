//! #4780: learning evaluation enters through the actual candidate executable.
//! These tests start no services and make no paid calls.

use crate::support::executable;

use std::ffi::OsString;
use std::fs;
use std::os::unix::ffi::OsStringExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

use serde_json::Value;
use tempfile::TempDir;

fn binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_curie"))
        .canonicalize()
        .expect("candidate executable exists")
}

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("source checkout")
        .to_path_buf()
}

struct Fixture {
    temp: TempDir,
    tools: PathBuf,
    attempted_path_cli: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().expect("private test directory");
        let tools = temp.path().join("tools");
        fs::create_dir(&tools).expect("private PATH entry");
        let attempted_path_cli = temp.path().join("unexpected-cli-call");
        executable::install_in(
            &tools,
            "curie",
            "#!/bin/sh\nprintf called > \"$LEARNING_TEST_PATH_CALL\"\nexit 97\n",
        );
        Self {
            temp,
            tools,
            attempted_path_cli,
        }
    }

    fn command(&self, candidate: &Path) -> Command {
        let path = std::env::join_paths(std::iter::once(self.tools.clone()).chain(
            std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default()),
        ))
        .expect("test PATH");
        let mut command = Command::new(candidate);
        command
            .current_dir(root())
            .stdin(Stdio::null())
            .env("PATH", path)
            .env("LEARNING_TEST_PATH_CALL", &self.attempted_path_cli)
            .env("CURIE_BIN", self.tools.join("curie"))
            .env("COMPOSE_DISABLE_ENV_FILE", "1")
            .env_remove("CURIE_CREDENTIALS")
            .env_remove("CURIE_MODEL_CREDENTIALS")
            .env_remove("ANTHROPIC_API_KEY")
            .env_remove("ANTHROPIC_AUTH_TOKEN")
            .env_remove("CLAUDE_CODE_OAUTH_TOKEN")
            .env_remove("ANTHROPIC_BASE_URL")
            .env_remove("CURIE_OPENROUTER_API_URL");
        command
    }

    fn output_dir(&self) -> PathBuf {
        self.temp.path().join("result with spaces é")
    }

    fn dry_run(&self, extra: &[&str]) -> Output {
        self.command(&binary())
            .args([
                "--json",
                "dev",
                "learning-eval",
                "--dry-run",
                "--output-dir",
            ])
            .arg(self.output_dir())
            .args(extra)
            .output()
            .expect("run actual learning-eval dispatch")
    }

    fn assert_no_path_fallback(&self) {
        assert!(
            !self.attempted_path_cli.exists(),
            "every nested CLI invocation must use current_exe"
        );
    }
}

fn one_object(output: &Output) -> Value {
    let text = std::str::from_utf8(&output.stdout).expect("strict UTF-8 stdout");
    let value: Value = serde_json::from_str(text).expect("exactly one JSON stdout value");
    assert!(value.is_object(), "stdout must be one object");
    value
}

fn assert_error(output: &Output) {
    assert!(!output.status.success(), "invalid input succeeded");
    let value = one_object(output);
    assert!(value.get("error").is_some(), "central CLI error object");
}

#[test]
fn learning_eval_help_exposes_only_the_plan_command_surface() {
    let output = Command::new(binary())
        .args(["dev", "learning-eval", "--help"])
        .output()
        .expect("command help");
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).expect("UTF-8 help");
    for flag in [
        "--repetitions",
        "--budget-usd",
        "--output-dir",
        "--dry-run",
        "--precision-threshold",
        "--recall-threshold",
        "--falsewrite-threshold",
        "--duplicate-threshold",
        "--heldout-control-improvement-threshold",
        "--scheduled-delivery-threshold",
    ] {
        assert!(help.contains(flag), "missing option {flag}");
    }
    for forbidden in ["--curie-bin", "--keep", "--provider", "--model", "--resume"] {
        assert!(!help.contains(forbidden), "unexpected option {forbidden}");
    }
}

#[test]
fn learning_eval_dry_run_traverses_real_wrapper_and_captures_inner_stdout() {
    let fixture = Fixture::new();
    let output = fixture.dry_run(&[]);
    assert!(output.status.success(), "candidate dry run must succeed");
    let value = one_object(&output);
    assert_eq!(value["budget_usd"], "7.50");
    assert_eq!(value["cost_policy"]["campaign_budget_usd"], "7.50");
    let encoded = value.to_string();
    assert!(encoded.contains(fixture.output_dir().to_str().unwrap()));
    assert!(encoded.contains(binary().to_str().unwrap()));
    assert!(!encoded.contains("\"implementation_verification\":\"PASS\""));
    assert!(!encoded.contains("\"acceptance\":\"PASS\""));
    fixture.assert_no_path_fallback();
}

#[test]
fn learning_eval_dry_run_accepts_campaign_budget_cap() {
    let fixture = Fixture::new();
    let output = fixture.dry_run(&["--budget-usd", "7.50"]);
    assert!(output.status.success(), "campaign budget cap dry run");
    let plan = one_object(&output);
    assert_eq!(plan["budget_usd"], "7.50");
    assert_eq!(plan["cost_policy"]["campaign_budget_usd"], "7.50");
    fixture.assert_no_path_fallback();
}

#[test]
fn learning_eval_dry_run_accepts_n_and_all_custom_thresholds_without_credentials() {
    let fixture = Fixture::new();
    let output = fixture.dry_run(&[
        "--repetitions",
        "3",
        "--budget-usd",
        "4.50",
        "--precision-threshold",
        "0.95",
        "--recall-threshold",
        "0.85",
        "--falsewrite-threshold",
        "0",
        "--duplicate-threshold",
        "1",
        "--heldout-control-improvement-threshold",
        "0.1",
        "--scheduled-delivery-threshold",
        "2",
    ]);
    assert!(output.status.success(), "custom threshold dry run");
    let plan = one_object(&output);
    assert_eq!(plan["repetitions"], 3);
    assert_eq!(plan["budget_usd"], "4.50");
    assert_eq!(plan["thresholds"]["precision"], 0.95);
    assert_eq!(plan["thresholds"]["recall"], 0.85);
    assert_eq!(plan["thresholds"]["falsewrite"], 0);
    assert_eq!(plan["thresholds"]["correction_duplicates"], 1);
    assert_eq!(plan["thresholds"]["heldout_control_improvement"], 0.1);
    assert_eq!(plan["thresholds"]["scheduled_deliveries"], 2);
    assert_eq!(plan["custom_thresholds"], true);
    fixture.assert_no_path_fallback();
}

#[test]
fn learning_eval_dry_run_needs_no_reachable_docker_daemon() {
    let fixture = Fixture::new();
    let output = fixture
        .command(&binary())
        .args([
            "--json",
            "dev",
            "learning-eval",
            "--dry-run",
            "--output-dir",
        ])
        .arg(fixture.output_dir())
        .env_remove("DOCKER_CONTEXT")
        .env(
            "DOCKER_HOST",
            format!(
                "unix://{}",
                fixture.temp.path().join("absent.sock").display()
            ),
        )
        .output()
        .expect("dry run without a Docker daemon");
    assert!(output.status.success(), "dry run must not start services");
    one_object(&output);
    fixture.assert_no_path_fallback();
}

#[test]
fn learning_eval_rejects_invalid_limits_before_starting_any_services() {
    let fixture = Fixture::new();
    for (flag, value) in [
        ("--repetitions", "0"),
        ("--budget-usd", "0"),
        ("--budget-usd", "7.51"),
        ("--budget-usd", "NaN"),
        ("--budget-usd", "Infinity"),
        ("--budget-usd", "not-an-amount"),
        ("--precision-threshold", "NaN"),
        ("--precision-threshold", "1.01"),
        ("--recall-threshold", "Infinity"),
        ("--falsewrite-threshold", "1.01"),
        ("--heldout-control-improvement-threshold", "1.01"),
        ("--scheduled-delivery-threshold", "0"),
    ] {
        assert_error(&fixture.dry_run(&[flag, value]));
    }
    fixture.assert_no_path_fallback();
}

#[test]
fn learning_eval_rejects_negative_grammar_values() {
    let fixture = Fixture::new();
    for value in ["--budget-usd=-0.01", "--duplicate-threshold=-1"] {
        assert!(!fixture.dry_run(&[value]).status.success());
    }
}

#[test]
fn learning_eval_outer_cli_cannot_select_an_alternate_binary_or_keep_services() {
    let fixture = Fixture::new();
    for flag in ["--curie-bin", "--keep", "--provider", "--model", "--resume"] {
        let output = fixture.dry_run(&[flag, "example"]);
        assert!(!output.status.success(), "forbidden flag accepted: {flag}");
    }
}

#[test]
fn learning_eval_all_four_validator_failures_emit_one_outer_error_object() {
    let fixture = Fixture::new();
    for validator in ["report", "calibration", "negative", "cleanup"] {
        let output = fixture
            .command(&binary())
            .args(["--json", "dev", "learning-eval-verify", validator])
            .arg(fixture.temp.path().join("missing report é.json"))
            .output()
            .expect("actual validator dispatch");
        assert_error(&output);
    }
    fixture.assert_no_path_fallback();
}

#[test]
fn learning_eval_current_executable_pin_survives_a_utf8_copy_and_poisoned_path() {
    let fixture = Fixture::new();
    let copied = fixture.temp.path().join("candidate é");
    fs::copy(binary(), &copied).expect("copy actual candidate executable");
    let output = fixture
        .command(&copied)
        .args([
            "--json",
            "dev",
            "learning-eval",
            "--dry-run",
            "--output-dir",
        ])
        .arg(fixture.output_dir())
        .output()
        .expect("copied candidate dispatch");
    assert!(output.status.success());
    assert!(one_object(&output)
        .to_string()
        .contains(copied.to_str().unwrap()));
    fixture.assert_no_path_fallback();
}

#[test]
fn learning_eval_rejects_non_utf8_output_and_report_paths_without_lossy_conversion() {
    let fixture = Fixture::new();
    let invalid = fixture
        .temp
        .path()
        .join(OsString::from_vec(b"result-\xff".to_vec()));
    let output = fixture
        .command(&binary())
        .args([
            "--json",
            "dev",
            "learning-eval",
            "--dry-run",
            "--output-dir",
        ])
        .arg(&invalid)
        .output()
        .expect("non-UTF-8 output path");
    assert!(!output.status.success(), "non-UTF-8 output path accepted");
    assert!(!invalid.exists());
    for validator in ["report", "calibration", "negative", "cleanup"] {
        let output = fixture
            .command(&binary())
            .args(["--json", "dev", "learning-eval-verify", validator])
            .arg(&invalid)
            .output()
            .expect("non-UTF-8 report path");
        assert!(!output.status.success(), "non-UTF-8 report path accepted");
    }
}

#[test]
fn learning_eval_rejects_non_utf8_current_executable_path() {
    let fixture = Fixture::new();
    let copied = fixture
        .temp
        .path()
        .join(OsString::from_vec(b"curie-\xff".to_vec()));
    fs::copy(binary(), &copied).expect("copy candidate to non-UTF-8 path");
    let output = fixture
        .command(&copied)
        .args([
            "--json",
            "dev",
            "learning-eval",
            "--dry-run",
            "--output-dir",
        ])
        .arg(fixture.output_dir())
        .output()
        .expect("non-UTF-8 executable dispatch");
    assert!(
        !output.status.success(),
        "non-UTF-8 executable path accepted"
    );
    fixture.assert_no_path_fallback();
}

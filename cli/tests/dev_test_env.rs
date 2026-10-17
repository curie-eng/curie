//! Process-boundary checks for the isolated test environment command.

use std::path::Path;
use std::process::{Command, Output};

use serde_json::Value;

fn command() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_curie"));
    command.current_dir(Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap());
    command
}

fn object(output: &Output) -> Value {
    let value: Value = serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "expected exactly one JSON object: {error}; stdout={}; stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    });
    assert!(value.is_object(), "result must be an object: {value}");
    value
}

#[test]
fn test_env_is_discoverable_from_dev_help() {
    let output = command().args(["dev", "--help"]).output().unwrap();
    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("test-env"));
}

#[test]
fn run_help_exposes_both_profiles_and_the_command_boundary() {
    let output = command()
        .args(["dev", "test-env", "run", "--help"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let help = String::from_utf8_lossy(&output.stdout);
    for token in ["--profile", "core", "full", "COMMAND"] {
        assert!(help.contains(token), "missing {token}: {help}");
    }
}

#[test]
fn missing_or_invalid_run_arguments_are_usage_errors_with_one_json_object() {
    for tail in [
        vec![],
        vec!["--"],
        vec!["--profile", "invalid", "--", "python3", "-c", "pass"],
        vec!["--unknown-option", "--", "python3", "-c", "pass"],
    ] {
        let output = command()
            .args(["--json", "dev", "test-env", "run"])
            .args(&tail)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2), "arguments: {tail:?}");
        let value = object(&output);
        assert!(value["error"].as_str().is_some_and(|error| !error.is_empty()));
    }
}

#[test]
fn valid_profiles_reach_the_real_missing_tool_boundary_as_transient_errors() {
    let empty_path = tempfile::tempdir().unwrap();
    for profile in [None, Some("core"), Some("full")] {
        let mut invocation = command();
        invocation
            .env("PATH", empty_path.path())
            .args(["--json", "dev", "test-env", "run"]);
        if let Some(profile) = profile {
            invocation.args(["--profile", profile]);
        }
        let output = invocation
            .args(["--", "python3", "-c", "pass"])
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(3), "profile: {profile:?}");
        let value = object(&output);
        assert!(value["error"].as_str().is_some_and(|error| !error.is_empty()));
    }
}

#[test]
fn run_outside_a_source_checkout_is_a_usage_error() {
    let outside = tempfile::tempdir().unwrap();
    let output = command()
        .current_dir(outside.path())
        .args([
            "--json", "dev", "test-env", "run", "--", "python3", "-c", "pass",
        ])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    let value = object(&output);
    assert!(value["error"].as_str().is_some_and(|error| !error.is_empty()));
}

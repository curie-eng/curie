//! Process guards for `curie dev release-accept` (#2430).
//!
//! The evaluator is read-only. These tests drive the binary: self-test must
//! pass the qualifying fixture and reject each independent miss, a live ledger
//! with fixture evidence must fail closed, and private message fields must not
//! appear in `--json` output.

use std::fs;
use std::path::PathBuf;
use std::process::Command;

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn output_text(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned() + &String::from_utf8_lossy(&output.stderr)
}

fn json_stdout(output: &std::process::Output) -> serde_json::Value {
    serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|err| panic!("expected JSON on stdout: {err}\n{}", output_text(output)))
}

#[test]
fn self_test_passes_and_names_independent_failures() {
    let output = Command::new(bin())
        .args(["--json", "dev", "release-accept", "--self-test"])
        .output()
        .expect("run release-accept --self-test");
    assert!(
        output.status.success(),
        "self-test failed\n{}",
        output_text(&output)
    );
    let value = json_stdout(&output);
    assert_eq!(value["qualified"], serde_json::json!(true));
    assert_eq!(value["mode"], serde_json::json!("fixture-self-test"));
    let names: Vec<&str> = value["self_test_cases"]
        .as_array()
        .expect("self_test_cases")
        .iter()
        .map(|case| case["name"].as_str().expect("name"))
        .collect();
    for required in [
        "qualifying",
        "missing-scheduled-run",
        "unknown-task-result",
        "changed-candidate",
        "missing-rotation-proof",
        "missing-recovery-drill",
        "stale-outbox",
        "lost-reply",
        "duplicate-reply",
        "source-only-as-live",
        "fixture-as-live",
    ] {
        assert!(
            names.contains(&required),
            "self-test must include {required}: {names:?}"
        );
    }
}

#[test]
fn fixture_ledger_fails_closed_as_live() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("ledger.json");
    fs::write(
        &path,
        curie::release_accept::qualifying_ledger().to_string(),
    )
    .expect("write ledger");
    let output = Command::new(bin())
        .args([
            "--json",
            "dev",
            "release-accept",
            "--ledger",
            path.to_str().expect("utf8"),
        ])
        .output()
        .expect("run release-accept --ledger");
    assert_eq!(
        output.status.code(),
        Some(1),
        "fixture-as-live must fail closed\n{}",
        output_text(&output)
    );
    let value = json_stdout(&output);
    assert_eq!(value["qualified"], serde_json::json!(false));
    assert_eq!(value["mode"], serde_json::json!("live"));
    let missing = value["missing"]
        .as_array()
        .expect("missing")
        .iter()
        .filter_map(|item| item.as_str())
        .collect::<Vec<_>>();
    assert!(
        missing.contains(&"live-evidence"),
        "fixture ledger evaluated live must miss live-evidence: {missing:?}"
    );
}

#[test]
fn changed_candidate_fails_with_that_criterion() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("changed.json");
    let mut ledger = curie::release_accept::qualifying_ledger();
    ledger["evidence_kind"] = serde_json::json!("live");
    ledger["identities"][1]["image"] = serde_json::json!("ghcr.io/curie-eng/curie-runner:other");
    fs::write(&path, ledger.to_string()).expect("write ledger");
    let output = Command::new(bin())
        .args([
            "--json",
            "dev",
            "release-accept",
            "--ledger",
            path.to_str().expect("utf8"),
        ])
        .output()
        .expect("run release-accept changed candidate");
    assert_eq!(output.status.code(), Some(1));
    let value = json_stdout(&output);
    let missing = value["missing"]
        .as_array()
        .expect("missing")
        .iter()
        .filter_map(|item| item.as_str())
        .collect::<Vec<_>>();
    assert!(
        missing.contains(&"unchanged-candidate"),
        "changed candidate must fail unchanged-candidate: {missing:?}"
    );
}

#[test]
fn json_report_excludes_private_message_fields() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("private.json");
    let mut ledger = curie::release_accept::qualifying_ledger();
    ledger["evidence_kind"] = serde_json::json!("live");
    ledger["body"] = serde_json::json!("private-message-body-token");
    ledger["channel"] = serde_json::json!("C0EXAMPLE1");
    fs::write(&path, ledger.to_string()).expect("write ledger");
    let output = Command::new(bin())
        .args([
            "--json",
            "dev",
            "release-accept",
            "--ledger",
            path.to_str().expect("utf8"),
        ])
        .output()
        .expect("run release-accept private ledger");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        !stdout.contains("private-message-body-token"),
        "JSON report leaked a private body: {stdout}"
    );
    let value = json_stdout(&output);
    assert!(value.get("body").is_none());
    assert!(value.get("channel").is_none());
}

#[test]
fn current_window_without_ledger_reports_unmet() {
    let output = Command::new(bin())
        .env_remove("CURIE_RELEASE_ACCEPT_LEDGER")
        .args(["--json", "dev", "release-accept"])
        .output()
        .expect("run release-accept with no ledger");
    assert_eq!(
        output.status.code(),
        Some(1),
        "absent live ledger must fail closed\n{}",
        output_text(&output)
    );
    let value = json_stdout(&output);
    assert_eq!(value["qualified"], serde_json::json!(false));
    let missing = value["missing"]
        .as_array()
        .expect("missing")
        .iter()
        .filter_map(|item| item.as_str())
        .collect::<Vec<_>>();
    assert!(
        missing.contains(&"ledger-present"),
        "current window without a ledger must miss ledger-present: {missing:?}"
    );
}

#[test]
fn self_test_and_ledger_are_usage() {
    let output = Command::new(bin())
        .args([
            "--json",
            "dev",
            "release-accept",
            "--self-test",
            "--ledger",
            "x.json",
        ])
        .output()
        .expect("run combined flags");
    assert_eq!(
        output.status.code(),
        Some(2),
        "combined flags must be usage\n{}",
        output_text(&output)
    );
}

#[test]
fn malformed_ledger_is_usage() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path: PathBuf = dir.path().join("bad.json");
    fs::write(&path, "{not json").expect("write bad ledger");
    let output = Command::new(bin())
        .args([
            "--json",
            "dev",
            "release-accept",
            "--ledger",
            path.to_str().expect("utf8"),
        ])
        .output()
        .expect("run malformed ledger");
    assert_eq!(
        output.status.code(),
        Some(2),
        "malformed ledger must be usage\n{}",
        output_text(&output)
    );
}

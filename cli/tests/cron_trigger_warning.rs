//! Command boundary coverage for cron trigger reporting.
//!
//! This test imports no new product API so the fix pin still compiles when the
//! product change is reversed. Docker is the boundary outside the changed CLI
//! component and returns the runner report that `skill check` consumes.

use serde_json::{json, Value};
use std::ffi::OsString;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const GREEN_REPORT: &str = r#"{
  "check": "mcp-load",
  "version": 1,
  "plugin_dir": "/plugin",
  "declared": [],
  "registered": [],
  "matches": [],
  "verdict": "green",
  "reasons": [],
  "hints": []
}"#;

const INVALID_BUNDLE_REPORT: &str = r#"{
  "check": "mcp-load",
  "version": 1,
  "plugin_dir": "/plugin",
  "declared": [],
  "registered": [],
  "matches": [],
  "verdict": "invalid_bundle",
  "reasons": ["triggers.cron_missing_schedule"],
  "hints": []
}"#;

fn write_executable(path: &Path, body: &str) {
    fs::write(path, body).expect("write executable");
    let mut permissions = fs::metadata(path)
        .expect("read executable metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).expect("make executable");
}

fn stub_path(tools: &Path) -> OsString {
    let mut entries = vec![tools.to_path_buf()];
    entries.extend(["/bin", "/usr/bin"].iter().map(PathBuf::from));
    std::env::join_paths(entries).expect("join tool path")
}

fn run_skill_check(manifest: Value, report: &str) -> Output {
    let temp = tempfile::tempdir().expect("create fixture directory");
    let bundle = temp.path().join("bundle");
    let manifest_dir = bundle.join(".claude-plugin");
    let tools = temp.path().join("tools");
    let config = temp.path().join("config");
    for dir in [&manifest_dir, &tools, &config] {
        fs::create_dir_all(dir).expect("create fixture path");
    }
    fs::write(
        manifest_dir.join("plugin.json"),
        serde_json::to_string_pretty(&manifest).expect("serialize manifest"),
    )
    .expect("write manifest");

    let script = format!("#!/bin/sh\nprintf '%s\\n' '{report}'\n");
    write_executable(&tools.join("docker"), &script);

    Command::new(env!("CARGO_BIN_EXE_curie"))
        .args([
            "--color=never",
            "skill",
            "check",
            "--plugin-dir",
            bundle.to_str().expect("bundle path is UTF8"),
            "--image",
            "curie-runner:test",
        ])
        .env_clear()
        .env("PATH", stub_path(&tools))
        .env("HOME", temp.path())
        .env("CURIE_CONFIG_DIR", config)
        .env("LC_ALL", "C")
        .output()
        .expect("run skill check")
}

#[test]
fn valid_named_cron_trigger_warns_exactly_once_after_skill_check() {
    let output = run_skill_check(
        json!({
            "name": "reporter",
            "triggers": [{
                "type": "cron",
                "name": "weekday digest",
                "schedule": "0 9 * * 1,2,3,4,5",
            }],
        }),
        GREEN_REPORT,
    );
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "valid skill check failed: {stderr}"
    );
    assert_eq!(
        stderr.matches("skill tier has no scheduler").count(),
        1,
        "one invocation must print one scheduler warning: {stderr}"
    );
    assert!(stderr.contains("weekday digest"), "was {stderr}");
    assert!(
        stderr.contains("the skill tier has no scheduler and does not fire it"),
        "was {stderr}"
    );
    assert!(
        stderr.contains("cron triggers fire only on local and cluster installs"),
        "was {stderr}"
    );
    assert!(!stderr.contains("#268"), "was {stderr}");
}

#[test]
fn several_cron_triggers_share_one_warning_with_name_and_schedule_fallback() {
    let output = run_skill_check(
        json!({
            "name": "reporter",
            "triggers": [
                {
                    "type": "cron",
                    "name": "weekday digest",
                    "schedule": "0 9 * * 1,2,3,4,5",
                },
                {"type": "webhook", "path": "/events/report"},
                {"type": "cron", "schedule": "0 18 * * *"},
            ],
        }),
        GREEN_REPORT,
    );
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(output.status.success(), "skill check failed: {stderr}");
    assert_eq!(
        stderr.matches("skill tier has no scheduler").count(),
        1,
        "was {stderr}"
    );
    assert!(
        stderr.contains("the skill tier has no scheduler"),
        "was {stderr}"
    );
    assert!(stderr.contains("weekday digest"), "was {stderr}");
    assert!(
        stderr.contains("3 with schedule \"0 18 * * *\""),
        "was {stderr}"
    );
}

#[test]
fn skill_check_without_a_cron_trigger_has_no_scheduler_warning() {
    for manifest in [
        json!({"name": "reporter"}),
        json!({"name": "reporter", "triggers": null}),
        json!({"name": "reporter", "triggers": []}),
        json!({
            "name": "reporter",
            "triggers": [{"type": "webhook", "path": "/events/report"}],
        }),
    ] {
        let output = run_skill_check(manifest, GREEN_REPORT);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "skill check failed: {stderr}");
        assert!(!stderr.contains("cron trigger"), "was {stderr}");
    }
}

#[test]
fn invalid_bundle_verdict_suppresses_cron_warning() {
    let output = run_skill_check(
        json!({
            "name": "reporter",
            "triggers": [
                {
                    "type": "cron",
                    "name": "must stay silent",
                    "schedule": "0 9 * * *",
                },
                {"type": "cron", "name": "broken schedule"},
            ],
        }),
        INVALID_BUNDLE_REPORT,
    );
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert_eq!(output.status.code(), Some(2), "was {stderr}");
    assert!(stderr.contains("triggers.cron_missing_schedule"));
    assert!(
        !stderr.contains("skill tier has no scheduler"),
        "was {stderr}"
    );
}

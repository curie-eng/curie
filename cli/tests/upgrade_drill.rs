//! Guards for `curie dev upgrade-drill` (#2426).
//!
//! The live scenarios need a task-owned kind install, published v0.8.6
//! artifacts, and live provider/channel credential references. These tests pin
//! the command surface and the soak-refusal / scenario / missing-credential
//! guards so a contributor cannot point the drill at the permanent soak or
//! close the live rows with fake-model.

use std::fs;
use std::path::PathBuf;
use std::process::Command;

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..")
}

fn output_text(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned() + &String::from_utf8_lossy(&output.stderr)
}

fn script() -> PathBuf {
    repo_root().join("cli/scripts/upgrade-drill.sh")
}

#[test]
fn upgrade_drill_script_is_present_and_executable() {
    let path = script();
    assert!(path.is_file(), "missing {}", path.display());
    let mode = fs::metadata(&path).expect("stat").permissions();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert!(
            mode.mode() & 0o111 != 0,
            "upgrade-drill.sh must be executable"
        );
    }
}

#[test]
fn upgrade_drill_self_test_refuses_soak_unknown_scenario_and_missing_live() {
    let output = Command::new("bash")
        .arg(script())
        .arg("--self-test")
        .current_dir(repo_root())
        .output()
        .expect("run upgrade-drill --self-test");
    assert!(
        output.status.success(),
        "self-test failed\n{}",
        output_text(&output)
    );
    let text = output_text(&output);
    assert!(
        text.contains("soak namespace curie refused"),
        "self-test must refuse the permanent soak namespace\n{text}"
    );
    assert!(
        text.contains("soak release curie refused"),
        "self-test must refuse the permanent soak release\n{text}"
    );
    assert!(
        text.contains("unknown scenario refused"),
        "self-test must refuse an unknown scenario\n{text}"
    );
    assert!(
        text.contains("missing live provider credentials refused"),
        "self-test must refuse missing live provider credentials\n{text}"
    );
    assert!(
        text.contains("missing channel credentials refused"),
        "self-test must refuse missing channel credentials\n{text}"
    );
    assert!(
        text.contains("published v0.8.6 chart checksum pinned"),
        "self-test must pin the published v0.8.6 chart checksum\n{text}"
    );
    assert!(
        text.contains("sha256 helper rejected a mismatched fixture"),
        "self-test must demonstrate the checksum guard rejecting a mismatch\n{text}"
    );
    assert!(
        text.contains("helm --set KEY=VAL tokens are split"),
        "self-test must pin split --set KEY=VAL argv tokens for the 0.8.6 CLI\n{text}"
    );
}

#[test]
fn upgrade_drill_cluster_surface_refuses_soak_namespace() {
    let output = Command::new(bin())
        .args([
            "dev",
            "upgrade-drill",
            "--scenario",
            "incompatible-rollback",
        ])
        .env("CURIE_BIN", bin())
        .env("CURIE_E2E_NAMESPACE", "curie")
        .env("CURIE_E2E_RELEASE", "drill")
        .current_dir(repo_root())
        .output()
        .expect("run upgrade-drill against soak namespace");
    assert!(
        !output.status.success(),
        "upgrade-drill must refuse namespace curie\n{}",
        output_text(&output)
    );
    let text = output_text(&output);
    assert!(
        text.contains("curie") && (text.contains("soak") || text.contains("refus")),
        "refusal must name the soak namespace\n{text}"
    );
}

#[test]
fn upgrade_drill_unknown_scenario_exits_nonzero() {
    let output = Command::new("bash")
        .arg(script())
        .args(["--scenario", "not-a-scenario"])
        .current_dir(repo_root())
        .output()
        .expect("run upgrade-drill unknown scenario");
    assert!(
        !output.status.success(),
        "unknown scenario must fail\n{}",
        output_text(&output)
    );
    let text = output_text(&output);
    assert!(
        text.contains("unknown scenario"),
        "error must name the unknown scenario\n{text}"
    );
}

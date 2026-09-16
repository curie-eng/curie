//! Guards for `curie dev cluster-upgrade-matrix` (#2590).
//!
//! The live scenarios need a task-owned kind install, published v0.8.8
//! artifacts, and candidate images. These tests pin the command surface and
//! the soak-refusal / scenario / checksum guards so a contributor cannot point
//! the matrix at the permanent soak or resolve the candidate binary from PATH.

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
    repo_root().join("cli/scripts/cluster-upgrade-matrix.sh")
}

#[test]
fn cluster_upgrade_matrix_script_is_present_and_executable() {
    let path = script();
    assert!(path.is_file(), "missing {}", path.display());
    let mode = fs::metadata(&path).expect("stat").permissions();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert!(
            mode.mode() & 0o111 != 0,
            "cluster-upgrade-matrix.sh must be executable"
        );
    }
}

#[test]
fn cluster_upgrade_matrix_self_test_refuses_soak_unknown_scenario_and_path_curie() {
    let output = Command::new("bash")
        .arg(script())
        .arg("--self-test")
        .current_dir(repo_root())
        .output()
        .expect("run cluster-upgrade-matrix --self-test");
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
        text.contains("published v0.8.8 chart checksum pinned"),
        "self-test must pin the published v0.8.8 chart checksum\n{text}"
    );
    assert!(
        text.contains("sha256 helper rejected a mismatched fixture"),
        "self-test must demonstrate the checksum guard rejecting a mismatch\n{text}"
    );
    assert!(
        text.contains("candidate binary is not PATH fallback"),
        "self-test must refuse resolving BIN from PATH curie\n{text}"
    );
    assert!(
        text.contains("helm --set KEY=VAL tokens are split"),
        "self-test must pin split --set KEY=VAL argv tokens\n{text}"
    );
    assert!(
        text.contains("cluster upgrade verb is the mutator"),
        "self-test must pin cluster upgrade as the mutator\n{text}"
    );
    assert!(
        text.contains("restore_n resumes leftover in_progress 0.9.0"),
        "self-test must pin restore_n resuming leftover in_progress 0.9.0\n{text}"
    );
    assert!(
        text.contains("restore_n clears leftover in_progress after restoring 0.9.0"),
        "self-test must pin restore_n clearing the checkpoint after the 0.9.0 restore\n{text}"
    );
    assert!(
        text.contains("n-to-n1 restores 0.9.0 through restore_n"),
        "self-test must pin n-to-n1 using restore_n\n{text}"
    );
    assert!(
        text.contains("restore_n rolls back to 0.9.0 when a revision exists"),
        "self-test must pin restore_n rolling back to 0.9.0\n{text}"
    );
    assert!(
        text.contains("exclusive_kind_tag untags siblings before and after load"),
        "self-test must pin exclusive_kind_tag untag-before-load\n{text}"
    );
    assert!(
        text.contains("schema heads published=0039"),
        "self-test must pin the published 0.8.8 alembic head\n{text}"
    );
}

#[test]
fn cluster_upgrade_matrix_cluster_surface_refuses_soak_namespace() {
    let output = Command::new(bin())
        .args(["dev", "cluster-upgrade-matrix", "--scenario", "fresh-n"])
        .env("CURIE_BIN", bin())
        .env("CURIE_E2E_NAMESPACE", "curie")
        .env("CURIE_E2E_RELEASE", "t2590")
        .current_dir(repo_root())
        .output()
        .expect("run cluster-upgrade-matrix against soak namespace");
    assert!(
        !output.status.success(),
        "cluster-upgrade-matrix must refuse namespace curie\n{}",
        output_text(&output)
    );
    let text = output_text(&output);
    assert!(
        text.contains("curie") && (text.contains("soak") || text.contains("refus")),
        "refusal must name the soak namespace\n{text}"
    );
}

#[test]
fn cluster_upgrade_matrix_unknown_scenario_exits_nonzero() {
    let output = Command::new("bash")
        .arg(script())
        .args(["--scenario", "not-a-scenario"])
        .current_dir(repo_root())
        .output()
        .expect("run cluster-upgrade-matrix unknown scenario");
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

//! Guards for `curie dev recovery-drill` (#2425).
//!
//! The live scenarios need a task-owned compose or Helm install. These tests
//! pin the command surface and the soak-refusal / occupancy / scenario guards
//! so a contributor cannot point the drill at the permanent soak or a shared
//! compose stack.

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
    repo_root().join("cli/scripts/recovery-drill.sh")
}

#[test]
fn recovery_drill_script_is_present_and_executable() {
    let path = script();
    assert!(path.is_file(), "missing {}", path.display());
    let mode = fs::metadata(&path).expect("stat").permissions();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert!(
            mode.mode() & 0o111 != 0,
            "recovery-drill.sh must be executable"
        );
    }
}

#[test]
fn recovery_drill_self_test_refuses_soak_and_unknown_scenario() {
    let output = Command::new("bash")
        .arg(script())
        .arg("--self-test")
        .current_dir(repo_root())
        .output()
        .expect("run recovery-drill --self-test");
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
}

#[test]
fn recovery_drill_cluster_surface_refuses_soak_namespace() {
    let output = Command::new(bin())
        .args([
            "dev",
            "recovery-drill",
            "--surface",
            "cluster",
            "--scenario",
            "worker-death",
        ])
        .env("CURIE_BIN", bin())
        .env("CURIE_E2E_NAMESPACE", "curie")
        .env("CURIE_E2E_RELEASE", "drill")
        .current_dir(repo_root())
        .output()
        .expect("run recovery-drill against soak namespace");
    assert!(
        !output.status.success(),
        "cluster surface must refuse namespace curie\n{}",
        output_text(&output)
    );
    let text = output_text(&output);
    assert!(
        text.contains("curie") && (text.contains("soak") || text.contains("refuse")),
        "refusal must name the soak namespace\n{text}"
    );
}

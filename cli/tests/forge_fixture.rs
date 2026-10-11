//! Guards for `curie dev forge-fixture` (#4398).
//!
//! The fixtures themselves (a local GitLab and a Bitbucket Data Center) live
//! under `tools/forge-fixtures/<kind>/fixture.py` and are not part of this
//! surface. These tests pin the command surface end to end through the real
//! binary and the real script: a kind whose fixture is not installed fails
//! loudly instead of pretending to start, an unknown kind is refused by clap,
//! and the refusal leaves no state behind.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const KINDS: [&str; 2] = ["gitlab", "bitbucket-dc"];

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..")
}

fn output_text(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned() + &String::from_utf8_lossy(&output.stderr)
}

fn script() -> PathBuf {
    repo_root().join("cli/scripts/forge-fixture.sh")
}

fn fixture_entry(root: &Path, kind: &str) -> PathBuf {
    root.join("tools/forge-fixtures")
        .join(kind)
        .join("fixture.py")
}

/// The binary tests below drive the real repository tree, so they only prove
/// the not-installed branch while that tree has no fixture for the kind. When
/// a fixture lands, move its kind's coverage to the hermetic script test.
fn assert_fixture_absent_in_repo(kind: &str) {
    let entry = fixture_entry(&repo_root(), kind);
    assert!(
        !entry.exists(),
        "{} now exists; this test proves the not-installed branch and must be \
         retargeted at a kind with no fixture",
        entry.display()
    );
}

fn run_binary(args: &[&str]) -> Output {
    Command::new(bin())
        .args(args)
        .current_dir(repo_root())
        .output()
        .expect("run curie")
}

#[test]
fn forge_fixture_script_is_present_and_executable() {
    let path = script();
    assert!(path.is_file(), "missing {}", path.display());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = fs::metadata(&path).expect("stat").permissions().mode();
        assert!(mode & 0o111 != 0, "forge-fixture.sh must be executable");
    }
}

#[test]
fn forge_fixture_binary_refuses_a_kind_whose_fixture_is_not_installed() {
    for kind in KINDS {
        assert_fixture_absent_in_repo(kind);
        let temp = tempfile::tempdir().expect("tempdir");
        let state_dir = temp.path().join("state");
        let state_arg = state_dir.to_str().expect("utf-8 tempdir");
        let output = run_binary(&["dev", "forge-fixture", kind, "up", "--state-dir", state_arg]);
        let text = output_text(&output);
        assert!(
            !output.status.success(),
            "forge-fixture {kind} up must fail while the fixture is missing\n{text}"
        );
        assert!(
            text.contains("not installed"),
            "forge-fixture {kind} up must say the fixture is not installed\n{text}"
        );
        assert!(
            text.contains(&format!("tools/forge-fixtures/{kind}/fixture.py")),
            "forge-fixture {kind} up must name the missing fixture entry point\n{text}"
        );
        assert!(
            !state_dir.exists(),
            "forge-fixture {kind} up must not create the state dir when it refuses\n{text}"
        );
    }
}

#[test]
fn forge_fixture_binary_passes_every_action_and_flag_to_the_script() {
    // self-test and down with the optional flags reach the same script and the
    // same refusal, so the dispatch arm forwards them instead of dropping the
    // invocation.
    for action in ["self-test", "down"] {
        assert_fixture_absent_in_repo("gitlab");
        let temp = tempfile::tempdir().expect("tempdir");
        let state_dir = temp.path().join("state");
        let state_arg = state_dir.to_str().expect("utf-8 tempdir");
        let output = run_binary(&[
            "dev",
            "forge-fixture",
            "gitlab",
            action,
            "--state-dir",
            state_arg,
            "--advertise-host",
            "127.0.0.1",
            "--port",
            "18080",
        ]);
        let text = output_text(&output);
        assert!(
            !output.status.success(),
            "forge-fixture gitlab {action} must fail while the fixture is missing\n{text}"
        );
        assert!(
            text.contains("not installed"),
            "forge-fixture gitlab {action} must reach the script's refusal\n{text}"
        );
        assert!(
            text.contains("forge-fixture.sh"),
            "forge-fixture gitlab {action} must run cli/scripts/forge-fixture.sh\n{text}"
        );
        assert!(
            !state_dir.exists(),
            "{action} created the state dir\n{text}"
        );
    }
}

#[test]
fn forge_fixture_binary_refuses_an_unknown_kind() {
    for kind in ["github", "gitlab-ce"] {
        let temp = tempfile::tempdir().expect("tempdir");
        let state_dir = temp.path().join("state");
        let state_arg = state_dir.to_str().expect("utf-8 tempdir");
        let output = run_binary(&["dev", "forge-fixture", kind, "up", "--state-dir", state_arg]);
        let text = output_text(&output);
        assert!(
            !output.status.success(),
            "forge-fixture must refuse kind {kind}\n{text}"
        );
        assert!(
            text.contains("invalid value") && text.contains(kind),
            "clap must name the invalid kind {kind}\n{text}"
        );
        assert!(
            text.contains("gitlab") && text.contains("bitbucket-dc"),
            "clap must list the possible kinds\n{text}"
        );
        assert!(
            !text.contains("forge-fixture.sh"),
            "an unknown kind must be refused before the script runs\n{text}"
        );
        assert!(
            !state_dir.exists(),
            "unknown kind created the state dir\n{text}"
        );
    }
}

#[test]
fn forge_fixture_script_refuses_a_missing_fixture_without_creating_state() {
    // Run a copy of the real script inside a repo-shaped tempdir that has no
    // tools/forge-fixtures tree, so the not-installed branch is proven no
    // matter which fixtures the real checkout carries. The cwd is the temp
    // root, matching how `curie dev` invokes scripts from the repo root.
    let root = tempfile::tempdir().expect("tempdir");
    let scripts = root.path().join("cli/scripts");
    fs::create_dir_all(&scripts).expect("mkdir cli/scripts");
    let copy = scripts.join("forge-fixture.sh");
    fs::copy(script(), &copy).expect("copy forge-fixture.sh");

    for kind in KINDS {
        for action in ["up", "self-test", "down"] {
            let state_dir = root.path().join(format!("state-{kind}-{action}"));
            let output = Command::new("bash")
                .arg("cli/scripts/forge-fixture.sh")
                .arg(kind)
                .arg(action)
                .arg("--state-dir")
                .arg(&state_dir)
                .current_dir(root.path())
                .output()
                .expect("run forge-fixture.sh");
            let text = output_text(&output);
            assert!(
                !output.status.success(),
                "forge-fixture.sh {kind} {action} must fail with no fixture\n{text}"
            );
            assert!(
                text.contains("not installed"),
                "forge-fixture.sh {kind} {action} must say not installed\n{text}"
            );
            assert!(
                text.contains(&format!("tools/forge-fixtures/{kind}/fixture.py")),
                "forge-fixture.sh {kind} {action} must name the missing entry point\n{text}"
            );
            assert!(
                !state_dir.exists(),
                "forge-fixture.sh {kind} {action} must not create the state dir\n{text}"
            );
        }
    }
}

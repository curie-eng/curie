//! Guards for `curie dev forge-fixture` (#4398).
//!
//! The fixtures themselves (a local GitLab and a Bitbucket Data Center) live
//! under `tools/forge-fixtures/<kind>/fixture.py` and are not part of this
//! surface. These tests pin the command surface end to end through the real
//! binary and the real script: a kind whose fixture is not installed fails
//! loudly instead of pretending to start, an installed fixture receives the
//! action and every flag unchanged, an unknown kind is refused by clap, and a
//! refusal leaves no state behind.
//!
//! Every test runs against a temporary source-checkout layout (a copy of the
//! real script plus the `runner/Dockerfile` marker the binary's repo-root
//! lookup walks up for), never the real checkout's `tools/forge-fixtures`
//! tree. That keeps them independent of which fixtures the checkout carries,
//! so a later change that adds a GitLab or Bitbucket fixture does not break
//! them.

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

/// A temp directory laid out as a curie source checkout: `runner/Dockerfile`
/// (the marker `curie dev` walks up for) and a copy of the real
/// `cli/scripts/forge-fixture.sh`. It has no `tools/forge-fixtures` tree
/// unless a test plants one.
fn checkout_layout() -> tempfile::TempDir {
    let root = tempfile::tempdir().expect("tempdir");
    fs::create_dir_all(root.path().join("runner")).expect("mkdir runner");
    fs::write(root.path().join("runner/Dockerfile"), "FROM scratch\n").expect("write Dockerfile");
    let scripts = root.path().join("cli/scripts");
    fs::create_dir_all(&scripts).expect("mkdir cli/scripts");
    fs::copy(script(), scripts.join("forge-fixture.sh")).expect("copy forge-fixture.sh");
    root
}

fn run_binary(cwd: &Path, args: &[&str]) -> Output {
    Command::new(bin())
        .args(args)
        .current_dir(cwd)
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
    let root = checkout_layout();
    for kind in KINDS {
        let state_dir = root.path().join(format!("state-{kind}"));
        let state_arg = state_dir.to_str().expect("utf-8 tempdir");
        let output = run_binary(
            root.path(),
            &["dev", "forge-fixture", kind, "up", "--state-dir", state_arg],
        );
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
    let root = checkout_layout();
    for action in ["self-test", "down"] {
        let state_dir = root.path().join(format!("state-{action}"));
        let state_arg = state_dir.to_str().expect("utf-8 tempdir");
        let output = run_binary(
            root.path(),
            &[
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
            ],
        );
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
fn forge_fixture_binary_forwards_the_action_and_flags_to_an_installed_fixture() {
    // A planted fixture records its argv and exits 0, so this proves the
    // installed branch: the binary reaches the script, the script execs the
    // fixture, and the fixture sees exactly the action and flags given.
    let root = checkout_layout();
    let fixture_dir = root.path().join("tools/forge-fixtures/gitlab");
    fs::create_dir_all(&fixture_dir).expect("mkdir fixture dir");
    let recorded = root.path().join("argv.json");
    fs::write(
        fixture_dir.join("fixture.py"),
        format!(
            "import json, sys\n\
             with open({path:?}, 'w') as handle:\n    \
             json.dump(sys.argv[1:], handle)\n",
            path = recorded.to_str().expect("utf-8 tempdir"),
        ),
    )
    .expect("write fixture.py");

    let state_dir = root.path().join("state");
    let state_arg = state_dir.to_str().expect("utf-8 tempdir");
    let output = run_binary(
        root.path(),
        &[
            "dev",
            "forge-fixture",
            "gitlab",
            "self-test",
            "--state-dir",
            state_arg,
            "--advertise-host",
            "h",
            "--port",
            "1234",
        ],
    );
    let text = output_text(&output);
    assert!(
        output.status.success(),
        "forge-fixture gitlab self-test must succeed when the fixture does\n{text}"
    );
    let argv: Vec<String> = serde_json::from_str(
        &fs::read_to_string(&recorded).unwrap_or_else(|e| panic!("fixture never ran: {e}\n{text}")),
    )
    .expect("argv.json is a JSON list of strings");
    assert_eq!(
        argv,
        [
            "self-test",
            "--state-dir",
            state_arg,
            "--advertise-host",
            "h",
            "--port",
            "1234"
        ],
        "the fixture must receive exactly the action and flags\n{text}"
    );
}

#[test]
fn forge_fixture_binary_refuses_an_unknown_kind() {
    let root = checkout_layout();
    for kind in ["github", "gitlab-ce"] {
        let state_dir = root.path().join(format!("state-{kind}"));
        let state_arg = state_dir.to_str().expect("utf-8 tempdir");
        let output = run_binary(
            root.path(),
            &["dev", "forge-fixture", kind, "up", "--state-dir", state_arg],
        );
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
    // Run the copied script inside the repo-shaped tempdir, which has no
    // tools/forge-fixtures tree. The cwd is the temp root, matching how
    // `curie dev` invokes scripts from the repo root.
    let root = checkout_layout();
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

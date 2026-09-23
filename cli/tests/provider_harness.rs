use std::path::{Path, PathBuf};
use std::process::{Command, Output};

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("cli crate has a repository root")
        .to_path_buf()
}

fn run(args: &[&str], current_dir: &Path) -> Output {
    Command::new(bin())
        .args(args)
        .current_dir(current_dir)
        .output()
        .unwrap_or_else(|error| panic!("run curie {}: {error}", args.join(" ")))
}

fn output_text(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned() + &String::from_utf8_lossy(&output.stderr)
}

#[test]
fn secrets_e2e_help_exposes_only_the_approved_surface_and_valid_modes_reach_seed_check() {
    let root = repo_root();
    let dev_help = run(&["dev", "--help"], &root);
    let dev_text = output_text(&dev_help);
    assert!(dev_help.status.success(), "dev help failed: {dev_text}");
    assert!(dev_text.lines().any(|line| line.contains("secrets-e2e")));

    let leaf_help = run(&["dev", "secrets-e2e", "--help"], &root);
    let leaf_text = output_text(&leaf_help);
    assert!(leaf_help.status.success(), "leaf help failed: {leaf_text}");
    for option in ["--seed", "--eso", "--ci", "--real-aws", "--suite"] {
        assert!(
            leaf_text.contains(option),
            "leaf help omitted {option}: {leaf_text}"
        );
    }

    let temp = tempfile::tempdir().expect("create temporary directory");
    let missing = temp.path().join("missing.json");
    let modes: &[&[&str]] = &[
        &["dev", "secrets-e2e", "--eso", "preinstalled"],
        &["dev", "secrets-e2e", "--eso", "none"],
        &["dev", "secrets-e2e", "--ci"],
        &["dev", "secrets-e2e", "--real-aws"],
    ];
    for args in modes {
        let output = Command::new(bin())
            .args(*args)
            .args(["--seed"])
            .arg(&missing)
            .current_dir(&root)
            .env("PATH", "/usr/bin:/bin")
            .output()
            .expect("run valid mode with missing seed");
        let text = output_text(&output);
        assert!(
            !output.status.success(),
            "valid mode accepted a missing seed: {args:?}\n{text}"
        );
        assert!(
            text.to_ascii_lowercase().contains("seed"),
            "valid mode did not reach seed validation: {args:?}\n{text}"
        );
        assert!(
            !text.contains("required tool") && !text.contains("kind is required"),
            "tool preflight ran before seed validation: {args:?}\n{text}"
        );
    }
}

#[test]
fn conflicting_modes_are_rejected_by_clap_before_dispatch() {
    let outside_checkout = tempfile::tempdir().expect("create temporary directory");
    let cases: &[&[&str]] = &[
        &["dev", "secrets-e2e", "--ci", "--real-aws"],
        &["dev", "secrets-e2e", "--ci", "--eso", "preinstalled"],
        &["dev", "secrets-e2e", "--real-aws", "--eso", "none"],
        &["dev", "secrets-e2e", "--suite", "routing", "--ci"],
        &["dev", "secrets-e2e", "--suite", "routing", "--real-aws"],
        &[
            "dev",
            "secrets-e2e",
            "--suite",
            "routing",
            "--eso",
            "preinstalled",
        ],
    ];

    for args in cases {
        let output = run(args, outside_checkout.path());
        let text = output_text(&output);
        assert!(!output.status.success(), "conflict was accepted: {args:?}");
        assert_eq!(
            output.status.code(),
            Some(2),
            "conflict returned the wrong exit status: {args:?}\n{text}"
        );
        assert!(
            text.contains("cannot be used with"),
            "conflict reached dispatch or produced the wrong error: {args:?}\n{text}"
        );
        assert!(
            !text.contains("runner/Dockerfile"),
            "conflict reached source checkout lookup: {args:?}\n{text}"
        );
    }
}

#[test]
fn invalid_seed_fails_before_external_tool_preflight() {
    let root = repo_root();
    let temp = tempfile::tempdir().expect("create temporary directory");
    let missing = temp.path().join("missing.json");
    let nested = temp.path().join("nested.json");
    std::fs::write(
        &nested,
        r#"{"STATIC_KEY":"synthetic-static","ROTATED_KEY":{"nested":true}}"#,
    )
    .expect("write invalid seed");

    for seed in [&missing, &nested] {
        let output = Command::new(bin())
            .args(["dev", "secrets-e2e", "--seed"])
            .arg(seed)
            .current_dir(&root)
            .env("PATH", "/usr/bin:/bin")
            .output()
            .expect("run curie with invalid seed");
        let text = output_text(&output);
        assert!(
            !output.status.success(),
            "invalid seed was accepted: {text}"
        );
        assert!(
            text.to_ascii_lowercase().contains("seed"),
            "failure did not identify the seed: {text}"
        );
        assert!(
            !text.contains("required tool") && !text.contains("kind is required"),
            "tool preflight ran before seed validation: {text}"
        );
    }
}

#[test]
fn launcher_orchestrator_and_fixture_are_tracked() {
    let root = repo_root();
    let required = [
        "cli/scripts/provider-e2e.sh",
        "cli/scripts/provider_harness.py",
        "cli/tests/provider_harness_live.py",
        "cli/tests/fixtures/provider-bundle/.claude-plugin/plugin.json",
        "cli/tests/fixtures/provider-bundle/.mcp.json",
        "cli/tests/fixtures/provider-bundle/connectors.yaml",
        "cli/tests/fixtures/provider-bundle/seed.json",
        "cli/tests/fixtures/provider-bundle/connectors/digest/Dockerfile",
        "cli/tests/fixtures/provider-bundle/connectors/digest/requirements.txt",
        "cli/tests/fixtures/provider-bundle/connectors/digest/server.py",
        "cli/tests/fixtures/provider-bundle/connectors/digest/rotate.py",
        "cli/tests/fixtures/provider-bundle/manifests/rotation-rbac.yaml",
    ];
    let output = Command::new("git")
        .arg("-C")
        .arg(&root)
        .args(["ls-files", "--error-unmatch"])
        .args(required)
        .output()
        .expect("run git ls-files");
    assert!(
        output.status.success(),
        "required harness files are not tracked:\n{}",
        output_text(&output)
    );
}

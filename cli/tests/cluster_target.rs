//! Regression coverage for cluster target resolution from `curie.yaml` (#2857).
//!
//! The binary is the test boundary. Every child has an empty tool `PATH`, so a
//! malformed installation file must fail before Helm or kubectl can run. Valid
//! plans use dry run, while connection discovery stops at the empty `PATH`.
//! Neither path needs a cluster or backing service.

use std::collections::BTreeSet;
use std::path::Path;
use std::process::{Command, Output};

const FILE_NAMESPACE: &str = "acme-platform";
const FILE_RELEASE: &str = "acme-prod";

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

#[derive(Clone, Copy)]
struct ClusterCase {
    name: &'static str,
    args: &'static [&'static str],
}

fn cluster_cases() -> Vec<ClusterCase> {
    vec![
        ClusterCase {
            name: "lint-values",
            args: &["lint-values", "-f", "values.yaml"],
        },
        ClusterCase {
            name: "up",
            args: &["up", "--fake-model", "--dry-run"],
        },
        ClusterCase {
            name: "down",
            args: &["down", "--yes", "--dry-run"],
        },
        ClusterCase {
            name: "rollback",
            args: &[
                "rollback",
                "--revision",
                "1",
                "--allow-failed-revision",
                "--yes",
                "--dry-run",
            ],
        },
        ClusterCase {
            name: "upgrade",
            args: &["upgrade", "--to", "0.10.0", "--yes", "--dry-run"],
        },
        ClusterCase {
            name: "migrate-store",
            args: &["migrate-store", "--phase", "import", "--dry-run"],
        },
        ClusterCase {
            name: "status",
            args: &["status", "--dry-run"],
        },
        ClusterCase {
            name: "observability",
            args: &["observability", "--dry-run"],
        },
        ClusterCase {
            name: "comms",
            args: &["comms", "--slack", "--disconnect", "--dry-run"],
        },
        ClusterCase {
            name: "github-app",
            args: &["github-app", "--disconnect", "--dry-run"],
        },
        ClusterCase {
            name: "message",
            args: &["message", "hello", "--dry-run"],
        },
        ClusterCase {
            name: "eval",
            args: &["eval", "--dry-run"],
        },
        ClusterCase {
            name: "deploy",
            args: &[
                "deploy",
                "--api-url",
                "http://127.0.0.1:1",
                "--api-key",
                "example-key",
            ],
        },
        ClusterCase {
            name: "kill",
            args: &["kill", "acme-bot", "--yes", "--dry-run"],
        },
        ClusterCase {
            name: "resume",
            args: &["resume", "acme-bot", "--dry-run"],
        },
        ClusterCase {
            name: "overrides",
            args: &["overrides", "acme-bot", "--dry-run"],
        },
        ClusterCase {
            name: "publication-policy",
            args: &["publication-policy", "acme-bot", "--dry-run"],
        },
        ClusterCase {
            name: "surfaces",
            args: &["surfaces", "acme-bot", "--dry-run"],
        },
        ClusterCase {
            name: "channel-token",
            args: &["channel-token", "acme-bot", "--show-exp", "--dry-run"],
        },
        ClusterCase {
            name: "budget",
            args: &["budget", "acme-bot", "--limit", "1", "--dry-run"],
        },
        ClusterCase {
            name: "reset-thread",
            args: &[
                "reset-thread",
                "acme-bot",
                "--thread-key",
                "slack:C0EXAMPLE1:1700000000.000100",
                "--yes",
                "--dry-run",
            ],
        },
        ClusterCase {
            name: "delete",
            args: &["delete", "acme-bot", "--yes", "--dry-run"],
        },
        ClusterCase {
            name: "versions",
            args: &["versions", "acme-bot", "--dry-run"],
        },
        ClusterCase {
            name: "memory",
            args: &["memory", "acme-bot", "--dry-run"],
        },
        ClusterCase {
            name: "approvals",
            args: &["approvals", "acme-bot", "--dry-run"],
        },
        ClusterCase {
            name: "work-items",
            args: &["work-items", "--dry-run"],
        },
        ClusterCase {
            name: "schedules",
            args: &["schedules", "--dry-run"],
        },
        ClusterCase {
            name: "hook",
            args: &["hook", "fire", "acme-bot", "nightly-cleanup", "--dry-run"],
        },
    ]
}

fn write_installation(dir: &Path, namespace: &str, release: &str) {
    std::fs::write(
        dir.join("curie.yaml"),
        format!("version: 1\ninstall:\n  namespace: {namespace}\n  release: {release}\n"),
    )
    .expect("write curie.yaml");
}

fn run(dir: &Path, args: &[&str], env: &[(&str, &str)]) -> Output {
    let empty_path = dir.join("empty-path");
    std::fs::create_dir_all(&empty_path).expect("create empty PATH");
    let mut command = Command::new(bin());
    command
        .args(args)
        .current_dir(dir)
        .env_clear()
        .env("HOME", dir)
        .env("PATH", empty_path)
        .env("NO_COLOR", "1")
        .env("CI", "1");
    for (name, value) in env {
        command.env(name, value);
    }
    command
        .output()
        .unwrap_or_else(|err| panic!("run curie {}: {err}", args.join(" ")))
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn combined(output: &Output) -> String {
    stdout(output) + &stderr(output)
}

fn target_diagnostics(output: &Output) -> Vec<String> {
    stderr(output)
        .lines()
        .filter(|line| {
            line.starts_with("target: ") || line.starts_with("inferred from curie.yaml:")
        })
        .map(str::to_owned)
        .collect()
}

fn target_note(
    namespace: &str,
    namespace_source: &str,
    release: &str,
    release_source: &str,
) -> String {
    format!(
        "target: namespace {namespace} ({namespace_source}), release {release} ({release_source})"
    )
}

fn assert_success_target(output: &Output, namespace: &str, release: &str) {
    let text = combined(output);
    let plan = stdout(output);
    assert!(output.status.success(), "expected success\n{text}");
    assert!(
        plan.contains(namespace),
        "stdout plan must carry namespace {namespace:?}\n{text}"
    );
    assert!(
        plan.contains(release),
        "stdout plan must carry release {release:?}\n{text}"
    );
}

fn status_args<'a>(extra: &[&'a str]) -> Vec<&'a str> {
    let mut args = vec!["cluster", "status", "--dry-run"];
    args.extend_from_slice(extra);
    args
}

#[test]
fn coverage_inventory_names_every_cluster_verb() {
    let manifest: serde_json::Value =
        serde_json::from_str(include_str!("../command-manifest.json")).expect("command manifest");
    let cluster = manifest["subcommands"]
        .as_array()
        .expect("top level subcommands")
        .iter()
        .find(|command| command["name"] == "cluster")
        .expect("cluster command");
    let manifest_names: BTreeSet<&str> = cluster["subcommands"]
        .as_array()
        .expect("cluster subcommands")
        .iter()
        .map(|command| command["name"].as_str().expect("command name"))
        .collect();
    let covered_names: BTreeSet<&str> = cluster_cases().iter().map(|case| case.name).collect();

    assert_eq!(covered_names, manifest_names);
    assert_eq!(covered_names.len(), 27);
}

#[test]
fn every_cluster_verb_refuses_a_malformed_target_before_external_work() {
    for case in cluster_cases() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(
            dir.path().join("curie.yaml"),
            "version: 1\ninstall:\n  namespace: [\n",
        )
        .expect("write malformed curie.yaml");
        let mut args = vec!["cluster"];
        args.extend_from_slice(case.args);

        let output = run(dir.path(), &args, &[]);
        let text = combined(&output);
        assert!(
            !output.status.success(),
            "cluster {} must refuse malformed curie.yaml\n{text}",
            case.name
        );
        assert_eq!(
            output.status.code(),
            Some(2),
            "cluster {} must classify malformed curie.yaml as usage\n{text}",
            case.name
        );
        assert!(
            text.contains("curie.yaml"),
            "cluster {} must name the invalid file\n{text}",
            case.name
        );
        assert!(
            !text.contains("No such file or directory")
                && !text.contains("not found in PATH")
                && !text.contains("failed to execute helm")
                && !text.contains("failed to execute kubectl"),
            "cluster {} reached an external tool before rejecting curie.yaml\n{text}",
            case.name
        );
    }
}

#[test]
fn target_precedence_is_per_field() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_installation(dir.path(), FILE_NAMESPACE, FILE_RELEASE);

    let from_file = run(dir.path(), &status_args(&[]), &[]);
    assert_success_target(&from_file, FILE_NAMESPACE, FILE_RELEASE);
    assert_eq!(
        target_diagnostics(&from_file),
        vec![target_note(
            FILE_NAMESPACE,
            "curie.yaml",
            FILE_RELEASE,
            "curie.yaml"
        )]
    );

    let flag_namespace = run(
        dir.path(),
        &status_args(&["--namespace", "flag-namespace"]),
        &[],
    );
    assert_success_target(&flag_namespace, "flag-namespace", FILE_RELEASE);
    assert_eq!(
        target_diagnostics(&flag_namespace),
        vec![target_note(
            "flag-namespace",
            "flag",
            FILE_RELEASE,
            "curie.yaml"
        )]
    );

    let flag_release = run(
        dir.path(),
        &status_args(&["--release", "flag-release"]),
        &[],
    );
    assert_success_target(&flag_release, FILE_NAMESPACE, "flag-release");
    assert_eq!(
        target_diagnostics(&flag_release),
        vec![target_note(
            FILE_NAMESPACE,
            "curie.yaml",
            "flag-release",
            "flag"
        )]
    );

    let env_namespace = run(
        dir.path(),
        &status_args(&[]),
        &[("CURIE_NAMESPACE", "env-namespace")],
    );
    assert_success_target(&env_namespace, "env-namespace", FILE_RELEASE);
    assert_eq!(
        target_diagnostics(&env_namespace),
        vec![target_note(
            "env-namespace",
            "CURIE_NAMESPACE",
            FILE_RELEASE,
            "curie.yaml"
        )]
    );
}

#[test]
fn explicit_literal_defaults_beat_environment_and_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_installation(dir.path(), FILE_NAMESPACE, FILE_RELEASE);
    let output = run(
        dir.path(),
        &status_args(&["--namespace", "curie", "--release", "curie"]),
        &[("CURIE_NAMESPACE", "env-namespace")],
    );
    let text = combined(&output);

    assert_success_target(&output, "curie", "curie");
    assert!(
        stdout(&output).contains("helm status curie -n curie"),
        "explicit defaults must reach the Helm status command\n{text}"
    );
    assert!(!text.contains(FILE_NAMESPACE), "{text}");
    assert!(!text.contains(FILE_RELEASE), "{text}");
    assert!(!text.contains("env-namespace"), "{text}");
    assert!(
        target_diagnostics(&output).is_empty(),
        "fully supplied targets must not emit a target note\n{text}"
    );
}

#[test]
fn absent_file_uses_builtin_defaults() {
    let dir = tempfile::tempdir().expect("tempdir");
    let output = run(dir.path(), &status_args(&[]), &[]);
    let text = combined(&output);

    assert_success_target(&output, "curie", "curie");
    assert!(
        stdout(&output).contains("helm status curie -n curie"),
        "built-in defaults must reach the Helm status command\n{text}"
    );
    assert!(
        target_diagnostics(&output).is_empty(),
        "default-only targets must not emit a target note\n{text}"
    );
}

#[test]
fn connection_backed_verbs_discover_the_file_target() {
    let cases: &[(&str, &[&str])] = &[
        ("kill", &["cluster", "kill", "acme-bot", "--yes"]),
        ("resume", &["cluster", "resume", "acme-bot"]),
        ("overrides", &["cluster", "overrides", "acme-bot"]),
        (
            "publication-policy",
            &["cluster", "publication-policy", "acme-bot"],
        ),
        ("surfaces", &["cluster", "surfaces", "acme-bot"]),
        (
            "channel-token",
            &["cluster", "channel-token", "acme-bot", "--show-exp"],
        ),
        ("budget", &["cluster", "budget", "acme-bot", "--limit", "1"]),
        (
            "reset-thread",
            &[
                "cluster",
                "reset-thread",
                "acme-bot",
                "--thread-key",
                "slack:C0EXAMPLE1:1700000000.000100",
                "--yes",
            ],
        ),
        ("delete", &["cluster", "delete", "acme-bot", "--yes"]),
        ("versions", &["cluster", "versions", "acme-bot"]),
        ("memory", &["cluster", "memory", "acme-bot"]),
        ("approvals", &["cluster", "approvals", "acme-bot"]),
    ];
    let expected = format!(
        "could not inspect Helm state for release {FILE_RELEASE} in namespace {FILE_NAMESPACE}"
    );

    for (name, args) in cases {
        let dir = tempfile::tempdir().expect("tempdir");
        write_installation(dir.path(), FILE_NAMESPACE, FILE_RELEASE);
        let output = run(dir.path(), args, &[]);
        let text = combined(&output);

        assert!(
            !output.status.success(),
            "cluster {name} must attempt target discovery with an empty PATH\n{text}"
        );
        assert!(
            text.contains(&expected),
            "cluster {name} must discover the release and namespace from curie.yaml\n{text}"
        );
    }
}

#[test]
fn malformed_file_is_required_only_for_missing_target_fields() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        dir.path().join("curie.yaml"),
        "version: 1\ninstall:\n  namespace: [\n",
    )
    .expect("write malformed curie.yaml");

    let partial = run(
        dir.path(),
        &status_args(&["--namespace", "flag-namespace"]),
        &[],
    );
    let partial_text = combined(&partial);
    assert!(!partial.status.success(), "{partial_text}");
    assert!(partial_text.contains("curie.yaml"), "{partial_text}");

    let explicit = run(
        dir.path(),
        &status_args(&["--namespace", "flag-namespace", "--release", "flag-release"]),
        &[],
    );
    assert_success_target(&explicit, "flag-namespace", "flag-release");
}

#[test]
fn nested_observability_uses_the_file_target() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_installation(dir.path(), FILE_NAMESPACE, FILE_RELEASE);
    let output = run(dir.path(), &["cluster", "observability", "--dry-run"], &[]);
    assert_success_target(&output, FILE_NAMESPACE, FILE_RELEASE);
}

fn assert_observability_query_bypasses_malformed_file(args: &[&str]) {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        dir.path().join("curie.yaml"),
        "version: 1\ninstall:\n  namespace: [\n",
    )
    .expect("write malformed curie.yaml");
    let output = run(dir.path(), args, &[]);
    let text = combined(&output);

    assert!(
        !output.status.success(),
        "query endpoint must be unreachable\n{text}"
    );
    assert_ne!(
        output.status.code(),
        Some(2),
        "global target flags must parse in this position\n{text}"
    );
    assert!(
        text.contains("API is unavailable") || text.contains("reachable --api-url"),
        "the explicit target must bypass the file and reach the query transport\n{text}"
    );
    assert!(
        !text.contains("curie.yaml"),
        "explicit literal defaults must not read the malformed file\n{text}"
    );
}

#[test]
fn observability_query_accepts_explicit_literal_target_after_the_leaf() {
    assert_observability_query_bypasses_malformed_file(&[
        "cluster",
        "observability",
        "runs",
        "--namespace",
        "curie",
        "--release",
        "curie",
        "--api-url",
        "http://127.0.0.1:1",
        "--api-key",
        "example-key",
    ]);
}

#[test]
fn observability_query_accepts_explicit_literal_target_before_the_leaf() {
    assert_observability_query_bypasses_malformed_file(&[
        "cluster",
        "observability",
        "--namespace",
        "curie",
        "--release",
        "curie",
        "runs",
        "--api-url",
        "http://127.0.0.1:1",
        "--api-key",
        "example-key",
    ]);
}

fn assert_observability_query_uses_mixed_target(args: &[&str]) {
    let dir = tempfile::tempdir().expect("tempdir");
    write_installation(dir.path(), FILE_NAMESPACE, FILE_RELEASE);
    let output = run(dir.path(), args, &[]);
    let text = combined(&output);

    assert!(
        !output.status.success(),
        "query endpoint must be unreachable\n{text}"
    );
    assert_ne!(
        output.status.code(),
        Some(2),
        "global target flags must parse in this position\n{text}"
    );
    assert!(
        text.contains("API is unavailable") || text.contains("reachable --api-url"),
        "the mixed target must reach the query transport\n{text}"
    );
    assert_eq!(
        target_diagnostics(&output),
        vec![target_note(
            "query-namespace",
            "flag",
            FILE_RELEASE,
            "curie.yaml"
        )],
        "the observability query must retain the mixed target sources\n{text}"
    );
}

#[test]
fn observability_query_accepts_mixed_target_after_the_leaf() {
    assert_observability_query_uses_mixed_target(&[
        "cluster",
        "observability",
        "runs",
        "--namespace",
        "query-namespace",
        "--api-url",
        "http://127.0.0.1:1",
        "--api-key",
        "example-key",
    ]);
}

#[test]
fn observability_query_accepts_mixed_target_before_the_leaf() {
    assert_observability_query_uses_mixed_target(&[
        "cluster",
        "observability",
        "--namespace",
        "query-namespace",
        "runs",
        "--api-url",
        "http://127.0.0.1:1",
        "--api-key",
        "example-key",
    ]);
}

#[test]
fn present_directory_at_curie_yaml_is_not_treated_as_absent() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::create_dir(dir.path().join("curie.yaml")).expect("create curie.yaml directory");
    let output = run(dir.path(), &status_args(&[]), &[]);
    let text = combined(&output);

    assert!(!output.status.success(), "{text}");
    assert!(text.contains("curie.yaml"), "{text}");
    assert!(
        stdout(&output).trim().is_empty(),
        "must not emit a default target plan\n{text}"
    );
}

#[test]
fn dangling_curie_yaml_symlink_is_not_treated_as_absent() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::os::unix::fs::symlink("missing-installation.yaml", dir.path().join("curie.yaml"))
        .expect("create dangling curie.yaml symlink");
    let output = run(dir.path(), &status_args(&[]), &[]);
    let text = combined(&output);

    assert!(!output.status.success(), "{text}");
    assert!(text.contains("curie.yaml"), "{text}");
    assert!(
        stdout(&output).trim().is_empty(),
        "must not emit a default target plan\n{text}"
    );
}

#[test]
fn fresh_message_with_an_explicit_chart_uses_the_file_target() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_installation(dir.path(), FILE_NAMESPACE, FILE_RELEASE);
    let chart = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("cli crate lives in the repository")
        .join("charts/curie");
    let chart = chart.to_str().expect("chart path is UTF-8");
    let output = run(
        dir.path(),
        &["cluster", "message", "hello", "--chart", chart, "--dry-run"],
        &[],
    );

    assert_success_target(&output, FILE_NAMESPACE, FILE_RELEASE);
}

fn write_saved_cluster_turn(dir: &Path, namespace: &str, release: &str) {
    std::fs::create_dir_all(dir.join(".curie")).expect("create state dir");
    let state = serde_json::json!({
        "verb": "cluster",
        "channel": "C0EXAMPLE1",
        "thread_ts": "1700000000.000100",
        "namespace": namespace,
        "release": release,
        "chart": "charts/curie",
        "listen_host": null,
        "timeout_secs": 300,
        "api_url": null,
        "api_key_env": null
    });
    std::fs::write(
        dir.join(".curie").join("last-turn.json"),
        serde_json::to_vec_pretty(&state).expect("serialize state"),
    )
    .expect("write turn state");
}

#[test]
fn message_continue_keeps_its_saved_target_over_a_conflicting_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_installation(dir.path(), FILE_NAMESPACE, FILE_RELEASE);
    write_saved_cluster_turn(dir.path(), "saved-namespace", "saved-release");
    let output = run(
        dir.path(),
        &["cluster", "message", "continue", "--continue", "--dry-run"],
        &[],
    );
    let text = combined(&output);

    assert_success_target(&output, "saved-namespace", "saved-release");
    assert!(!text.contains(FILE_NAMESPACE), "{text}");
    assert!(!text.contains(FILE_RELEASE), "{text}");
}

#[test]
fn message_continue_allows_explicit_and_environment_target_overrides() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_installation(dir.path(), FILE_NAMESPACE, FILE_RELEASE);
    write_saved_cluster_turn(dir.path(), "saved-namespace", "saved-release");
    let output = run(
        dir.path(),
        &[
            "cluster",
            "message",
            "continue",
            "--continue",
            "--release",
            "explicit-release",
            "--dry-run",
        ],
        &[("CURIE_NAMESPACE", "env-namespace")],
    );

    assert_success_target(&output, "env-namespace", "explicit-release");
}

#[test]
fn json_dry_run_keeps_stdout_to_one_resolved_object() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_installation(dir.path(), FILE_NAMESPACE, FILE_RELEASE);
    let output = run(
        dir.path(),
        &["--json", "cluster", "status", "--dry-run"],
        &[],
    );
    let out = stdout(&output);
    assert!(output.status.success(), "{}", combined(&output));
    let payload: serde_json::Value = serde_json::from_str(&out)
        .unwrap_or_else(|err| panic!("stdout must be one JSON value: {err}\n{out}"));
    let rendered = payload.to_string();
    assert!(rendered.contains(FILE_NAMESPACE), "{payload}");
    assert!(rendered.contains(FILE_RELEASE), "{payload}");
}

#[test]
fn malformed_file_json_error_does_not_pollute_stdout() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        dir.path().join("curie.yaml"),
        "version: 1\ninstall:\n  namespace: [\n",
    )
    .expect("write malformed curie.yaml");
    let output = run(
        dir.path(),
        &["--json", "cluster", "status", "--dry-run"],
        &[],
    );
    let out = stdout(&output);

    assert!(!output.status.success(), "{}", combined(&output));
    let payload: serde_json::Value = serde_json::from_str(&out)
        .unwrap_or_else(|err| panic!("stdout must be one JSON error: {err}\n{out}"));
    assert!(payload["error"]
        .as_str()
        .is_some_and(|message| message.contains("curie.yaml")));
}

//! Drive the built CLI for the two-positional `cluster message <agent> <text>`
//! trap (#2498). Sibling verbs such as `cluster versions` take `<AGENT>` first;
//! this verb routes by `--channel` (omit it when exactly one channel is bound).

use std::path::Path;
use std::process::{Command, Output};

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn repo_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("cli crate lives in the repository")
}

fn run(args: &[&str]) -> Output {
    run_in(repo_root(), args)
}

fn run_in(dir: &Path, args: &[&str]) -> Output {
    Command::new(bin())
        .args(args)
        .current_dir(dir)
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

const ISSUE_TEXT: &str = "Who are you? Reply with the plugin name.";
const ISSUE_AGENT: &str = "acme-bot";

fn two_positional_cluster_argv() -> Vec<&'static str> {
    vec![
        "cluster",
        "message",
        ISSUE_AGENT,
        ISSUE_TEXT,
        "--namespace",
        "curie",
        "--release",
        "curie",
    ]
}

fn assert_two_positional_usage(output: &Output, verb: &str) {
    let text = combined(output);
    assert_eq!(
        output.status.code(),
        Some(2),
        "{verb} two-positional form must exit 2 (usage)\n{text}"
    );
    assert!(
        !text.contains("unexpected argument"),
        "{verb} must not leave the operator on clap's generic unexpected-argument error\n{text}"
    );
    assert!(
        text.contains("--channel"),
        "{verb} must name the --channel routing model\n{text}"
    );
    assert!(
        text.to_ascii_lowercase().contains("agent")
            && (text.contains("does not take")
                || text.contains("not by agent")
                || text.contains("not an agent")),
        "{verb} must explain that this verb does not route by agent name\n{text}"
    );
}

fn assert_json_usage(output: &Output, verb: &str) {
    assert_eq!(
        output.status.code(),
        Some(2),
        "{verb} --json two-positional form must exit 2\n{}",
        combined(output)
    );
    let payload: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap_or_else(|err| {
        panic!(
            "{verb} --json must emit one JSON object on stdout: {err}; stdout: {}; stderr: {}",
            stdout(output),
            stderr(output)
        )
    });
    let obj = payload
        .as_object()
        .unwrap_or_else(|| panic!("{verb} --json payload must be an object: {payload}"));
    assert_eq!(
        obj.keys().collect::<Vec<_>>(),
        vec!["error", "fix"],
        "{verb} --json must be exactly error and fix: {payload}"
    );
    let error = payload["error"]
        .as_str()
        .unwrap_or_else(|| panic!("error must be a string: {payload}"));
    let fix = payload["fix"]
        .as_str()
        .unwrap_or_else(|| panic!("fix must be a string, not null: {payload}"));
    assert!(
        error.contains("--channel") || fix.contains("--channel"),
        "{verb} --json must name --channel in error or fix: {payload}"
    );
    assert!(
        !fix.is_empty(),
        "{verb} --json fix must be actionable: {payload}"
    );
    assert!(
        error.contains("does not take an agent name")
            || error.contains("two-positional")
            || error.contains("<AGENT>"),
        "{verb} --json error must name the rejected two-positional / agent-name form: {payload}"
    );
}

/// The exact argv from #2498: `cluster message <agent> "<text>"` plus the
/// namespace/release flags used after deploy.
#[test]
fn cluster_message_two_positionals_explains_channel_routing() {
    let output = run(&two_positional_cluster_argv());
    assert_two_positional_usage(&output, "cluster message");
    let text = combined(&output);
    assert!(
        text.contains("cluster versions")
            || text.contains("cluster kill")
            || text.contains("<AGENT>"),
        "must name the sibling verbs that do take <AGENT> first\n{text}"
    );
}

#[test]
fn cluster_message_two_positionals_json_has_error_and_fix() {
    let mut args = vec!["--json"];
    args.extend(two_positional_cluster_argv());
    let output = run(&args);
    assert_json_usage(&output, "cluster message");
}

#[test]
fn cluster_message_json_after_verb_still_emits_error_and_fix() {
    let output = run(&[
        "cluster",
        "message",
        "--json",
        ISSUE_AGENT,
        ISSUE_TEXT,
        "--namespace",
        "curie",
        "--release",
        "curie",
    ]);
    assert_json_usage(&output, "cluster message");
}

#[test]
fn cluster_message_json_between_target_and_verb_still_emits_error_and_fix() {
    let output = run(&[
        "cluster",
        "--json",
        "message",
        ISSUE_AGENT,
        ISSUE_TEXT,
        "--namespace",
        "curie",
        "--release",
        "curie",
    ]);
    assert_json_usage(&output, "cluster message");
}

#[test]
fn local_message_two_positionals_explains_channel_routing() {
    let output = run(&["local", "message", ISSUE_AGENT, ISSUE_TEXT]);
    assert_two_positional_usage(&output, "local message");
}

#[test]
fn local_message_two_positionals_json_has_error_and_fix() {
    let output = run(&["--json", "local", "message", ISSUE_AGENT, ISSUE_TEXT]);
    assert_json_usage(&output, "local message");
}

/// Single-text form from #2498 must still parse and plan, not hit the usage
/// trap. `--dry-run` stays offline.
#[test]
fn cluster_message_single_text_dry_run_is_not_usage() {
    let output = run(&[
        "cluster",
        "message",
        ISSUE_TEXT,
        "--namespace",
        "curie",
        "--release",
        "curie",
        "--dry-run",
    ]);
    let text = combined(&output);
    assert_ne!(
        output.status.code(),
        Some(2),
        "single-text cluster message must not be a usage error\n{text}"
    );
    assert!(
        output.status.success(),
        "single-text --dry-run must succeed\n{text}"
    );
    assert!(
        !text.contains("does not take an agent name"),
        "single-text must not trip the two-positional explanation\n{text}"
    );
}

#[test]
fn cluster_message_explicit_channel_dry_run_is_not_usage() {
    let output = run(&[
        "cluster",
        "message",
        "--channel",
        "C0EXAMPLE1",
        ISSUE_TEXT,
        "--namespace",
        "curie",
        "--release",
        "curie",
        "--dry-run",
    ]);
    let text = combined(&output);
    assert!(
        output.status.success(),
        "explicit --channel single-text --dry-run must succeed\n{text}"
    );
}

/// `--continue` with one text token is valid argv. With no last-turn file it
/// fails later; it must not be classified as the two-positional usage trap.
/// Run in an empty tempdir so a leftover `.curie/last-turn.json` in the
/// checkout cannot turn this into a live cluster message.
#[test]
fn cluster_message_continue_is_not_two_positional_usage() {
    let dir = tempfile::tempdir().expect("tempdir");
    let output = run_in(
        dir.path(),
        &[
            "cluster",
            "message",
            "--continue",
            "what's 2 + 2?",
            "--namespace",
            "curie",
            "--release",
            "curie",
        ],
    );
    let text = combined(&output);
    assert_ne!(
        output.status.code(),
        Some(2),
        "--continue with one text token must not be usage\n{text}"
    );
    assert!(
        text.contains("last-turn.json") || text.contains("no previous turn"),
        "--continue without a recorded turn must say so, not invent a clap usage error\n{text}"
    );
}

#[test]
fn local_message_single_text_dry_run_is_not_usage() {
    let output = run(&["local", "message", ISSUE_TEXT, "--dry-run"]);
    let text = combined(&output);
    assert!(
        output.status.success(),
        "local message single-text --dry-run must succeed\n{text}"
    );
}

#[test]
fn cluster_message_help_still_takes_text_not_agent() {
    let output = run(&["cluster", "message", "--help"]);
    let text = combined(&output);
    assert!(output.status.success(), "help must exit 0\n{text}");
    assert!(
        text.contains("<TEXT>") || text.contains("<text>"),
        "help must keep the single TEXT positional\n{text}"
    );
    assert!(
        text.contains("--channel"),
        "help must still document --channel\n{text}"
    );
    assert!(
        !text.contains("<AGENT>") && !text.contains("<agent>"),
        "help must not grow an agent-name positional\n{text}"
    );
}

/// Sibling that does take `<AGENT>` first: two extra tokens stay clap's
/// unexpected-argument error, not the message-verb channel explanation.
#[test]
fn cluster_versions_two_positionals_is_not_channel_routing_error() {
    let output = run(&["cluster", "versions", ISSUE_AGENT, ISSUE_TEXT]);
    let text = combined(&output);
    assert_eq!(
        output.status.code(),
        Some(2),
        "cluster versions extra positional is still usage\n{text}"
    );
    assert!(
        text.contains("unexpected argument"),
        "cluster versions must keep clap's extra-argument error\n{text}"
    );
    assert!(
        !text.contains("--channel"),
        "cluster versions must not inherit the message-verb channel explanation\n{text}"
    );
}

#[test]
fn cluster_versions_help_still_takes_agent() {
    let output = run(&["cluster", "versions", "--help"]);
    let text = combined(&output);
    assert!(output.status.success(), "help must exit 0\n{text}");
    assert!(
        text.contains("<AGENT>") || text.contains("<agent>"),
        "cluster versions help must keep the AGENT positional\n{text}"
    );
}

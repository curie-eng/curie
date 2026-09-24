//! Integration: the clap-to-`OverrideChange` wiring of the `overrides` verb, at
//! both tiers (issue #1387).
//!
//! Every other test for this verb (`cli/tests/api_lifecycle.rs`, the
//! `overrides` section) calls `commands::overrides(...)` directly with
//! hand-built `OverrideChange` values, so it proves the PATCH-body semantics
//! but never touches the clap layer that decides WHICH flag becomes WHICH
//! argument. Swapping the two `OverrideChange` arguments at either call site
//! (`main.rs`, `LocalAction::Overrides` and `ClusterAction::Overrides`) left
//! the whole suite green.
//!
//! These tests close that gap by driving the built binary and asserting on the
//! `--dry-run --json` plan, which carries the serialized PATCH body verbatim.
//! Each assertion parses that body out of the plan line and compares it for
//! EXACT equality against the expected object, so a swapped argument, a
//! spurious extra key, and a field that should have been omitted all fail.
//! Exact equality on a parsed object is order- and whitespace-independent, so
//! it is stronger than a substring check without being brittle.
//!
//! A cleared field is present as JSON null, and exact equality pins that.
//! The absent-vs-null contract, that a field no flag mentioned stays out of
//! the PATCH body entirely, is asserted by `cli/tests/api_lifecycle.rs` (see
//! the comment at lines 397-410), not by this file.
//!
//! No server and no network: `--dry-run` returns before the HTTP client is
//! built, so the cluster tier's unreachable `--api-url` is never dialed. The
//! cluster tier MUST still be given explicit `--api-url`/`--api-key`, since
//! `resolve_cluster_conn` otherwise shells out to `kubectl` to discover them.

use std::process::Command;

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

/// A model value that could never be mistaken for a thinking depth.
const MODEL_SENTINEL: &str = "curie-test-model-alpha";
/// A thinking value that could never be mistaken for a model name.
const THINKING_SENTINEL: &str = "enabled:31337";
/// An execution-deadline value inside the accepted 60..10800 range (issue #3071).
const DEADLINE_SENTINEL: &str = "120";

/// Run the binary with `argv` and return the single `plan` line of its
/// `--dry-run --json` output.
///
/// `CURIE_API_URL`/`CURIE_API_KEY` are removed from the child's environment so
/// an operator's shell cannot change what these tests assert.
fn dry_run_plan_line(argv: &[&str]) -> String {
    let output = Command::new(bin())
        .args(argv)
        .env_remove("CURIE_API_URL")
        .env_remove("CURIE_API_KEY")
        .output()
        .unwrap_or_else(|e| panic!("run curie {}: {e}", argv.join(" ")));
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "curie {} must exit 0; stdout: {stdout}; stderr: {stderr}",
        argv.join(" ")
    );
    let value: serde_json::Value = serde_json::from_str(stdout.trim())
        .unwrap_or_else(|e| panic!("stdout must be one JSON object: {e}; stdout: {stdout}"));
    value
        .get("plan")
        .and_then(|p| p.as_array())
        .and_then(|p| p.first())
        .and_then(|l| l.as_str())
        .unwrap_or_else(|| panic!("dry-run output must carry a plan line: {value}"))
        .to_string()
}

/// Parse the PATCH body embedded in an `overrides --dry-run` plan line.
///
/// The line reads `PATCH <url>/agents/<id>  <body>  (would resolve agent
/// "<agent>" first)`, and only the body carries braces, so the first `{`
/// through the last `}` is exactly it. A plan-format change panics here with
/// the whole line rather than silently weakening every assertion below.
fn patch_body(plan: &str) -> serde_json::Value {
    let start = plan
        .find('{')
        .unwrap_or_else(|| panic!("plan line must embed a PATCH body object: {plan}"));
    let end = plan
        .rfind('}')
        .unwrap_or_else(|| panic!("plan line must embed a PATCH body object: {plan}"));
    serde_json::from_str(&plan[start..=end])
        .unwrap_or_else(|e| panic!("PATCH body must parse as JSON: {e}; plan line: {plan}"))
}

#[test]
fn local_overrides_set_both_binds_each_flag_to_its_own_patch_field() {
    let plan = dry_run_plan_line(&[
        "local",
        "overrides",
        "deal-desk",
        "--model",
        MODEL_SENTINEL,
        "--thinking",
        THINKING_SENTINEL,
        "--dry-run",
        "--json",
    ]);
    assert_eq!(
        patch_body(&plan),
        serde_json::json!({"model": MODEL_SENTINEL, "thinking": THINKING_SENTINEL}),
        "each flag must land under its own body key: {plan}"
    );
}

#[test]
fn local_overrides_clear_model_and_set_thinking_bind_to_their_own_patch_fields() {
    let plan = dry_run_plan_line(&[
        "local",
        "overrides",
        "deal-desk",
        "--clear-model",
        "--thinking",
        THINKING_SENTINEL,
        "--dry-run",
        "--json",
    ]);
    assert_eq!(
        patch_body(&plan),
        serde_json::json!({"model": null, "thinking": THINKING_SENTINEL}),
        "--clear-model must null `model` while --thinking sets `thinking`: {plan}"
    );
}

#[test]
fn local_overrides_set_model_and_clear_thinking_bind_to_their_own_patch_fields() {
    let plan = dry_run_plan_line(&[
        "local",
        "overrides",
        "deal-desk",
        "--model",
        MODEL_SENTINEL,
        "--clear-thinking",
        "--dry-run",
        "--json",
    ]);
    assert_eq!(
        patch_body(&plan),
        serde_json::json!({"model": MODEL_SENTINEL, "thinking": null}),
        "--clear-thinking must null `thinking` while --model sets `model`: {plan}"
    );
}

#[test]
fn cluster_overrides_set_both_binds_each_flag_to_its_own_patch_field() {
    let plan = dry_run_plan_line(&[
        "cluster",
        "overrides",
        "deal-desk",
        "--api-url",
        "http://127.0.0.1:9",
        "--api-key",
        "curie-test-key",
        "--model",
        MODEL_SENTINEL,
        "--thinking",
        THINKING_SENTINEL,
        "--dry-run",
        "--json",
    ]);
    assert_eq!(
        patch_body(&plan),
        serde_json::json!({"model": MODEL_SENTINEL, "thinking": THINKING_SENTINEL}),
        "each flag must land under its own body key: {plan}"
    );
}

#[test]
fn cluster_overrides_clear_model_and_set_thinking_bind_to_their_own_patch_fields() {
    let plan = dry_run_plan_line(&[
        "cluster",
        "overrides",
        "deal-desk",
        "--api-url",
        "http://127.0.0.1:9",
        "--api-key",
        "curie-test-key",
        "--clear-model",
        "--thinking",
        THINKING_SENTINEL,
        "--dry-run",
        "--json",
    ]);
    assert_eq!(
        patch_body(&plan),
        serde_json::json!({"model": null, "thinking": THINKING_SENTINEL}),
        "--clear-model must null `model` while --thinking sets `thinking`: {plan}"
    );
}

#[test]
fn cluster_overrides_set_model_and_clear_thinking_bind_to_their_own_patch_fields() {
    let plan = dry_run_plan_line(&[
        "cluster",
        "overrides",
        "deal-desk",
        "--api-url",
        "http://127.0.0.1:9",
        "--api-key",
        "curie-test-key",
        "--model",
        MODEL_SENTINEL,
        "--clear-thinking",
        "--dry-run",
        "--json",
    ]);
    assert_eq!(
        patch_body(&plan),
        serde_json::json!({"model": MODEL_SENTINEL, "thinking": null}),
        "--clear-thinking must null `thinking` while --model sets `model`: {plan}"
    );
}

// --- `--execution-deadline`/`--clear-execution-deadline` (issue #3071) ------
//
// Mirrors the `--model`/`--clear-model` coverage above: the wire field is
// `execution_deadline_seconds`, and unlike `model`/`thinking` it carries a
// JSON NUMBER, not a string -- the DTO field is an int
// (`execution_deadline_seconds: <int>`), so a stringified `"120"` in the PATCH
// body would be as wrong as sending `--clear-execution-deadline` as `""`.

#[test]
fn local_overrides_set_execution_deadline_binds_to_its_own_patch_field() {
    let plan = dry_run_plan_line(&[
        "local",
        "overrides",
        "deal-desk",
        "--execution-deadline",
        DEADLINE_SENTINEL,
        "--dry-run",
        "--json",
    ]);
    assert_eq!(
        patch_body(&plan),
        serde_json::json!({"execution_deadline_seconds": 120}),
        "--execution-deadline must send a JSON number under its own key: {plan}"
    );
}

#[test]
fn local_overrides_clear_execution_deadline_sends_explicit_null() {
    let plan = dry_run_plan_line(&[
        "local",
        "overrides",
        "deal-desk",
        "--clear-execution-deadline",
        "--dry-run",
        "--json",
    ]);
    assert_eq!(
        patch_body(&plan),
        serde_json::json!({"execution_deadline_seconds": null}),
        "--clear-execution-deadline must null `execution_deadline_seconds`: {plan}"
    );
}

#[test]
fn cluster_overrides_set_execution_deadline_binds_to_its_own_patch_field() {
    let plan = dry_run_plan_line(&[
        "cluster",
        "overrides",
        "deal-desk",
        "--api-url",
        "http://127.0.0.1:9",
        "--api-key",
        "curie-test-key",
        "--execution-deadline",
        DEADLINE_SENTINEL,
        "--dry-run",
        "--json",
    ]);
    assert_eq!(
        patch_body(&plan),
        serde_json::json!({"execution_deadline_seconds": 120}),
        "--execution-deadline must send a JSON number under its own key: {plan}"
    );
}

#[test]
fn cluster_overrides_clear_execution_deadline_sends_explicit_null() {
    let plan = dry_run_plan_line(&[
        "cluster",
        "overrides",
        "deal-desk",
        "--api-url",
        "http://127.0.0.1:9",
        "--api-key",
        "curie-test-key",
        "--clear-execution-deadline",
        "--dry-run",
        "--json",
    ]);
    assert_eq!(
        patch_body(&plan),
        serde_json::json!({"execution_deadline_seconds": null}),
        "--clear-execution-deadline must null `execution_deadline_seconds`: {plan}"
    );
}

#[test]
fn execution_deadline_and_clear_execution_deadline_together_is_a_usage_error() {
    let output = Command::new(bin())
        .args([
            "local",
            "overrides",
            "deal-desk",
            "--execution-deadline",
            DEADLINE_SENTINEL,
            "--clear-execution-deadline",
            "--dry-run",
            "--json",
        ])
        .env_remove("CURIE_API_URL")
        .env_remove("CURIE_API_KEY")
        .output()
        .expect("run curie");
    assert!(
        !output.status.success(),
        "--execution-deadline and --clear-execution-deadline must contradict each other"
    );
}

#[test]
fn execution_deadline_below_the_minimum_is_refused_client_side() {
    let output = Command::new(bin())
        .args([
            "local",
            "overrides",
            "deal-desk",
            "--execution-deadline",
            "30",
            "--dry-run",
            "--json",
        ])
        .env_remove("CURIE_API_URL")
        .env_remove("CURIE_API_KEY")
        .output()
        .expect("run curie");
    assert!(
        !output.status.success(),
        "a deadline below 60 seconds must be refused before any request"
    );
}

#[test]
fn execution_deadline_above_the_maximum_is_refused_client_side() {
    let output = Command::new(bin())
        .args([
            "local",
            "overrides",
            "deal-desk",
            "--execution-deadline",
            "15000",
            "--dry-run",
            "--json",
        ])
        .env_remove("CURIE_API_URL")
        .env_remove("CURIE_API_KEY")
        .output()
        .expect("run curie");
    assert!(
        !output.status.success(),
        "a deadline above 10800 seconds must be refused before any request"
    );
}

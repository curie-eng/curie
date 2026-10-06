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

mod support;

use std::process::Command;

use support::{serve, Response};

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

/// A model value that could never be mistaken for a thinking depth.
const MODEL_SENTINEL: &str = "curie-test-model-alpha";
/// The reviewer must not accidentally reuse the implementer's model.
const REVIEWER_MODEL_SENTINEL: &str = "curie-test-reviewer-model-beta";
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

#[test]
fn reviewer_model_set_clear_and_omission_use_distinct_patch_fields_at_both_tiers() {
    for tier in ["local", "cluster"] {
        let connection = [
            tier,
            "overrides",
            "dark-factory",
            "--api-url",
            "http://127.0.0.1:9",
            "--api-key",
            "curie-test-key",
        ];
        for (flags, expected) in [
            (
                vec![
                    "--model",
                    MODEL_SENTINEL,
                    "--reviewer-model",
                    REVIEWER_MODEL_SENTINEL,
                ],
                serde_json::json!({
                    "model": MODEL_SENTINEL,
                    "reviewer_model": REVIEWER_MODEL_SENTINEL,
                }),
            ),
            (
                vec!["--model", MODEL_SENTINEL, "--clear-reviewer-model"],
                serde_json::json!({"model": MODEL_SENTINEL, "reviewer_model": null}),
            ),
            (
                vec!["--model", MODEL_SENTINEL],
                serde_json::json!({"model": MODEL_SENTINEL}),
            ),
        ] {
            let mut args = connection.to_vec();
            args.extend(flags);
            args.extend(["--dry-run", "--json"]);
            let plan = dry_run_plan_line(&args);
            assert_eq!(patch_body(&plan), expected, "{tier}: {plan}");
        }
    }
}

#[test]
fn reviewer_model_and_clear_reviewer_model_conflict_at_both_tiers() {
    for tier in ["local", "cluster"] {
        let mut args = vec![
            tier,
            "overrides",
            "dark-factory",
            "--api-url",
            "http://127.0.0.1:9",
            "--api-key",
            "curie-test-key",
            "--reviewer-model",
            REVIEWER_MODEL_SENTINEL,
            "--dry-run",
            "--json",
        ];
        // Establish that this is a valid flag before testing its refusal.
        dry_run_plan_line(&args);
        args.push("--clear-reviewer-model");
        usage_refused(&args);
    }
}

#[test]
fn reviewer_model_set_inspect_clear_and_sibling_write_round_trip_at_both_tiers() {
    for tier in ["local", "cluster"] {
        let mut initial: serde_json::Value = serde_json::from_str(&mw_agent_json(true)).unwrap();
        initial["name"] = serde_json::json!("dark-factory");
        initial["reviewer_model"] = serde_json::Value::Null;
        let stored = std::sync::Arc::new(std::sync::Mutex::new(initial));
        let state = std::sync::Arc::clone(&stored);
        let server = serve(move |req| {
            let mut state = state.lock().unwrap();
            match (req.method.as_str(), req.path.as_str()) {
                ("GET", "/agents") => {
                    Response::json(200, &serde_json::json!([state.clone()]).to_string())
                }
                ("PATCH", p) if *p == format!("/agents/{MW_AGENT_ID}") => {
                    let patch: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
                    state
                        .as_object_mut()
                        .unwrap()
                        .extend(patch.as_object().unwrap().clone());
                    Response::json(200, &state.to_string())
                }
                _ => Response::json(404, r#"{"detail":"not found"}"#),
            }
        });
        let run = |flags: &[&str]| {
            let output = Command::new(bin())
                .args([tier, "overrides", "dark-factory"])
                .args(flags)
                .args(["--api-url", &server.base_url, "--api-key", "k", "--json"])
                .env_remove("CURIE_API_URL")
                .env_remove("CURIE_API_KEY")
                .env("NO_PROXY", "127.0.0.1,localhost")
                .env("no_proxy", "127.0.0.1,localhost")
                .output()
                .expect("run reviewer model override");
            assert!(
                output.status.success(),
                "{tier}: {}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            json_of(&String::from_utf8_lossy(&output.stdout))
        };

        let changed = run(&["--reviewer-model", REVIEWER_MODEL_SENTINEL]);
        assert_eq!(changed["reviewer_model"], REVIEWER_MODEL_SENTINEL);
        assert_eq!(changed["model"], "kimi-k2");
        assert_eq!(changed["changed"], true);
        let patches: Vec<_> = server
            .recorded()
            .into_iter()
            .filter(|request| request.method == "PATCH")
            .collect();
        assert_eq!(patches.len(), 1);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&patches[0].body).unwrap(),
            serde_json::json!({"reviewer_model": REVIEWER_MODEL_SENTINEL})
        );

        let inspected = run(&[]);
        assert_eq!(inspected["reviewer_model"], REVIEWER_MODEL_SENTINEL);
        assert_eq!(inspected["changed"], false);
        assert_eq!(
            server.recorded().iter().filter(|r| r.method == "PATCH").count(),
            1,
            "inspection must not write"
        );

        let sibling = run(&["--model", MODEL_SENTINEL]);
        assert_eq!(sibling["reviewer_model"], REVIEWER_MODEL_SENTINEL);
        assert_eq!(sibling["model"], MODEL_SENTINEL);
        let cleared = run(&["--clear-reviewer-model"]);
        assert_eq!(cleared["reviewer_model"], serde_json::Value::Null);
        assert_eq!(cleared["model"], MODEL_SENTINEL);
        assert_eq!(cleared["changed"], true);
        let patches: Vec<_> = server
            .recorded()
            .into_iter()
            .filter(|request| request.method == "PATCH")
            .collect();
        assert_eq!(patches.len(), 3);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&patches[1].body).unwrap(),
            serde_json::json!({"model": MODEL_SENTINEL})
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&patches[2].body).unwrap(),
            serde_json::json!({"reviewer_model": null})
        );
        assert_eq!(run(&[])["reviewer_model"], serde_json::Value::Null);
    }
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

// --- `--memory-writes on|off` (issue #1461) ---------------------------------
//
// `memory_writes` is a NOT NULL boolean on the agent, so the flag takes an
// explicit `on`/`off` and the PATCH body carries a JSON BOOLEAN under
// `memory_writes` -- never the strings "on"/"off", and never null.

fn usage_refused(argv: &[&str]) {
    let output = Command::new(bin())
        .args(argv)
        .env_remove("CURIE_API_URL")
        .env_remove("CURIE_API_KEY")
        .output()
        .unwrap_or_else(|e| panic!("run curie {}: {e}", argv.join(" ")));
    assert!(
        !output.status.success(),
        "curie {} must be refused; stdout: {}",
        argv.join(" "),
        String::from_utf8_lossy(&output.stdout)
    );
}

#[test]
fn local_overrides_memory_writes_on_sends_json_true() {
    let plan = dry_run_plan_line(&[
        "local",
        "overrides",
        "deal-desk",
        "--memory-writes",
        "on",
        "--dry-run",
        "--json",
    ]);
    assert_eq!(
        patch_body(&plan),
        serde_json::json!({"memory_writes": true}),
        "--memory-writes on must send a JSON boolean true under its own key: {plan}"
    );
}

#[test]
fn local_overrides_memory_writes_off_sends_json_false() {
    let plan = dry_run_plan_line(&[
        "local",
        "overrides",
        "deal-desk",
        "--memory-writes",
        "off",
        "--dry-run",
        "--json",
    ]);
    assert_eq!(
        patch_body(&plan),
        serde_json::json!({"memory_writes": false}),
        "--memory-writes off must send a JSON boolean false, not null: {plan}"
    );
}

#[test]
fn cluster_overrides_memory_writes_on_sends_json_true() {
    let plan = dry_run_plan_line(&[
        "cluster",
        "overrides",
        "deal-desk",
        "--api-url",
        "http://127.0.0.1:9",
        "--api-key",
        "curie-test-key",
        "--memory-writes",
        "on",
        "--dry-run",
        "--json",
    ]);
    assert_eq!(
        patch_body(&plan),
        serde_json::json!({"memory_writes": true}),
        "--memory-writes on must send a JSON boolean true under its own key: {plan}"
    );
}

#[test]
fn cluster_overrides_memory_writes_off_sends_json_false() {
    let plan = dry_run_plan_line(&[
        "cluster",
        "overrides",
        "deal-desk",
        "--api-url",
        "http://127.0.0.1:9",
        "--api-key",
        "curie-test-key",
        "--memory-writes",
        "off",
        "--dry-run",
        "--json",
    ]);
    assert_eq!(
        patch_body(&plan),
        serde_json::json!({"memory_writes": false}),
        "--memory-writes off must send a JSON boolean false, not null: {plan}"
    );
}

#[test]
fn memory_writes_combines_with_model_under_separate_keys() {
    let plan = dry_run_plan_line(&[
        "local",
        "overrides",
        "deal-desk",
        "--model",
        MODEL_SENTINEL,
        "--memory-writes",
        "on",
        "--dry-run",
        "--json",
    ]);
    assert_eq!(
        patch_body(&plan),
        serde_json::json!({"model": MODEL_SENTINEL, "memory_writes": true}),
        "each flag must land under its own body key: {plan}"
    );
}

#[test]
fn memory_writes_rejects_a_value_other_than_on_or_off_at_both_tiers() {
    // Anchor: the flag exists and takes `on`. Without this the refusals below
    // would pass vacuously against a binary that has no --memory-writes at all.
    dry_run_plan_line(&[
        "local",
        "overrides",
        "deal-desk",
        "--memory-writes",
        "on",
        "--dry-run",
        "--json",
    ]);
    for value in ["yes", "true", "1", ""] {
        usage_refused(&[
            "local",
            "overrides",
            "deal-desk",
            "--memory-writes",
            value,
            "--dry-run",
            "--json",
        ]);
        usage_refused(&[
            "cluster",
            "overrides",
            "deal-desk",
            "--api-url",
            "http://127.0.0.1:9",
            "--api-key",
            "curie-test-key",
            "--memory-writes",
            value,
            "--dry-run",
            "--json",
        ]);
    }
}

#[test]
fn memory_writes_without_a_value_is_refused() {
    // Anchor, as above: `off` must parse before a bare flag's refusal means anything.
    dry_run_plan_line(&[
        "local",
        "overrides",
        "deal-desk",
        "--memory-writes",
        "off",
        "--dry-run",
        "--json",
    ]);
    usage_refused(&[
        "local",
        "overrides",
        "deal-desk",
        "--memory-writes",
        "--dry-run",
        "--json",
    ]);
}

// --- overrides output reports memory_writes (issue #1461, fix round 2 G5) ---
//
// Like model and thinking, the switch is part of what `overrides` shows: an
// inspect (no change flags) reports it as stored, `--json` carries it as a
// boolean, and a write reports the value the API stored. Driven through the
// binary against the wire-level test server so the clap layer, the handler and
// the renderer are all on the path.

const MW_AGENT_ID: &str = "22222222-2222-2222-2222-222222222222";

fn mw_agent_json(memory_writes: bool) -> String {
    format!(
        r##"{{"id":"{MW_AGENT_ID}","name":"deal-desk","channels":[{{"kind":"slack","address":"#x"}}],"model":"kimi-k2","thinking":"adaptive","execution_deadline_seconds":null,"runner_resources":null,"created_at":"2026-07-05T00:00:00Z","memory":false,"memory_writes":{memory_writes}}}"##
    )
}

fn mw_server(stored: bool, after_patch: bool) -> support::MockServer {
    serve(move |req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => Response::json(200, &format!("[{}]", mw_agent_json(stored))),
        ("PATCH", p) if *p == format!("/agents/{MW_AGENT_ID}") => {
            Response::json(200, &mw_agent_json(after_patch))
        }
        _ => Response::json(404, r#"{"detail":"not found"}"#),
    })
}

fn run_against(base_url: &str, rest: &[&str]) -> (String, String) {
    let mut argv: Vec<&str> = vec!["local", "overrides", "deal-desk"];
    argv.extend(rest);
    argv.extend(["--api-url", base_url, "--api-key", "k"]);
    let output = Command::new(bin())
        .args(&argv)
        .env_remove("CURIE_API_URL")
        .env_remove("CURIE_API_KEY")
        .env("NO_PROXY", "127.0.0.1,localhost")
        .env("no_proxy", "127.0.0.1,localhost")
        .output()
        .unwrap_or_else(|e| panic!("run curie {}: {e}", argv.join(" ")));
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    assert!(
        output.status.success(),
        "curie {} must exit 0; stdout: {stdout}; stderr: {stderr}",
        argv.join(" ")
    );
    (stdout, stderr)
}

fn json_of(stdout: &str) -> serde_json::Value {
    serde_json::from_str(stdout.trim())
        .unwrap_or_else(|e| panic!("stdout must be one JSON object: {e}; stdout: {stdout}"))
}

#[test]
fn overrides_inspect_json_includes_memory_writes_on() {
    let server = mw_server(true, true);
    let (stdout, _) = run_against(&server.base_url, &["--json"]);
    let json = json_of(&stdout);
    assert_eq!(
        json.get("memory_writes"),
        Some(&serde_json::Value::Bool(true)),
        "inspect --json must carry memory_writes as a boolean: {json}"
    );
    assert_eq!(json["model"], "kimi-k2", "{json}");
    assert_eq!(json["thinking"], "adaptive", "{json}");
    assert_eq!(json["changed"], false, "{json}");
    assert!(
        server.recorded().iter().all(|r| r.method == "GET"),
        "an inspect must not write"
    );
}

#[test]
fn overrides_inspect_json_includes_memory_writes_off() {
    let server = mw_server(false, false);
    let (stdout, _) = run_against(&server.base_url, &["--json"]);
    let json = json_of(&stdout);
    assert_eq!(
        json.get("memory_writes"),
        Some(&serde_json::Value::Bool(false)),
        "off is false, not null or absent: {json}"
    );
}

#[test]
fn overrides_inspect_text_shows_memory_writes() {
    let on = mw_server(true, true);
    let (stdout, stderr) = run_against(&on.base_url, &[]);
    let text = format!("{stdout}{stderr}");
    assert!(
        text.contains("memory writes on"),
        "inspect must show the switch beside model and thinking: {text}"
    );
    let off = mw_server(false, false);
    let (stdout, stderr) = run_against(&off.base_url, &[]);
    let text = format!("{stdout}{stderr}");
    assert!(text.contains("memory writes off"), "{text}");
}

#[test]
fn overrides_write_reports_the_stored_memory_writes() {
    let server = mw_server(false, true);
    let (stdout, _) = run_against(&server.base_url, &["--memory-writes", "on", "--json"]);
    let json = json_of(&stdout);
    assert_eq!(json["changed"], true, "{json}");
    assert_eq!(
        json.get("memory_writes"),
        Some(&serde_json::Value::Bool(true)),
        "a write reports memory_writes as the API stored it: {json}"
    );
}

#[test]
fn overrides_write_of_another_field_still_reports_memory_writes() {
    let server = mw_server(true, true);
    let (stdout, _) = run_against(&server.base_url, &["--model", MODEL_SENTINEL, "--json"]);
    let json = json_of(&stdout);
    assert_eq!(
        json.get("memory_writes"),
        Some(&serde_json::Value::Bool(true)),
        "{json}"
    );
}

//! Regression for #1396: local and cluster eval must reject a blank `--model`
//! in the CLI, before reading the bundle or reaching the platform eval plane.
//!
//! The API remains the authoritative validation boundary for other callers.
//! These tests drive the built CLI because the defect lived in the clap ->
//! `message::eval` consumer path: clap legitimately preserves an empty string
//! in the model sweep, and the CLI must turn that into an actionable usage
//! error instead of forwarding it as a raw API 422.

use crate::support;

use std::fs;
use std::path::Path;
use std::process::{Command, Output};

use support::{serve, Response};

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn write_eval_suite(root: &Path) {
    let evals = root.join("evals");
    fs::create_dir_all(&evals).expect("create eval directory");
    fs::write(
        evals.join("cases.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "name": "blank-model-regression",
            "cases": [{
                "id": "placeholder-case",
                "input": "ping",
                "grader": {"kind": "contains", "expected": "pong"}
            }]
        }))
        .expect("serialize eval suite"),
    )
    .expect("write eval suite");
}

fn assert_actionable_model_usage(output: &Output, invocation: &str) {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(
        output.status.code(),
        Some(2),
        "{invocation} must reject a blank model as usage before doing I/O\n\
         stdout: {stdout}\nstderr: {stderr}"
    );

    let payload: serde_json::Value = serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|error| panic!("{invocation} must emit one JSON error: {error}; {stdout}"));
    let error = payload["error"]
        .as_str()
        .unwrap_or_else(|| panic!("{invocation} must include an error string: {payload}"));
    let fix = payload["fix"].as_str().unwrap_or_default();
    let guidance = format!("{error} {fix}");
    assert!(
        error.contains("--model") && (error.contains("blank") || error.contains("empty")),
        "the error must name the invalid --model value: {payload}"
    );
    assert!(
        guidance.contains("omit --model"),
        "the error must name the valid deployed-model alternative: {payload}"
    );
    assert!(
        stderr.is_empty(),
        "--json errors belong on stdout only; stderr: {stderr}"
    );
}

#[test]
fn local_eval_rejects_empty_whitespace_and_mixed_models_before_bundle_or_api_access() {
    let api = serve(|_| Response::json(500, r#"{"detail":"unexpected request"}"#));

    // No eval suite exists: an exact empty value must win over bundle lookup.
    let empty_root = tempfile::tempdir().expect("create empty bundle directory");
    let empty = Command::new(bin())
        .args([
            "--json",
            "local",
            "eval",
            "--model",
            "",
            "--api-url",
            &api.base_url,
        ])
        .current_dir(empty_root.path())
        .env("KUBECONFIG", empty_root.path().join("kubeconfig"))
        .env("CURIE_CONFIG_DIR", empty_root.path().join("config"))
        .env_remove("CURIE_API_URL")
        .env_remove("CURIE_API_KEY")
        .env_remove("CURIE_EVAL_SAMPLES")
        .env_remove("CURIE_EVAL_AGGREGATION")
        .env_remove("CURIE_EVAL_PASS_AT_K")
        .output()
        .expect("run local eval with an empty model");
    assert_actionable_model_usage(&empty, "curie local eval --model <empty>");

    // A valid suite removes bundle lookup as the stopping condition. Whitespace
    // must still be rejected without making even one platform API request.
    let bundle = tempfile::tempdir().expect("create bundle directory");
    write_eval_suite(bundle.path());
    let whitespace = Command::new(bin())
        .args([
            "--json",
            "local",
            "eval",
            "--model",
            " \t ",
            "--api-url",
            &api.base_url,
        ])
        .current_dir(bundle.path())
        .env("KUBECONFIG", bundle.path().join("kubeconfig"))
        .env("CURIE_CONFIG_DIR", bundle.path().join("config"))
        .env_remove("CURIE_API_URL")
        .env_remove("CURIE_API_KEY")
        .env_remove("CURIE_EVAL_SAMPLES")
        .env_remove("CURIE_EVAL_AGGREGATION")
        .env_remove("CURIE_EVAL_PASS_AT_K")
        .output()
        .expect("run local eval with a whitespace-only model");
    assert_actionable_model_usage(&whitespace, "curie local eval --model <whitespace-only>");

    // Repeatable flags form one model sweep: one invalid entry must refuse the
    // whole argv rather than silently filtering it and running example-model.
    for (model, label) in [("", "valid-plus-empty"), (" \t ", "valid-plus-whitespace")] {
        let mixed = Command::new(bin())
            .args([
                "--json",
                "local",
                "eval",
                "--model",
                "example-model",
                "--model",
                model,
                "--api-url",
                &api.base_url,
            ])
            .current_dir(bundle.path())
            .env("KUBECONFIG", bundle.path().join("kubeconfig"))
            .env("CURIE_CONFIG_DIR", bundle.path().join("config"))
            .env_remove("CURIE_API_URL")
            .env_remove("CURIE_API_KEY")
            .env_remove("CURIE_EVAL_SAMPLES")
            .env_remove("CURIE_EVAL_AGGREGATION")
            .env_remove("CURIE_EVAL_PASS_AT_K")
            .output()
            .unwrap_or_else(|error| panic!("run local eval with {label} repeated models: {error}"));
        assert_actionable_model_usage(
            &mixed,
            &format!("curie local eval --model example-model --model <{label}>"),
        );
    }
    assert!(
        api.recorded().is_empty(),
        "blank model validation must run before any platform API request"
    );
}

#[test]
fn cluster_eval_rejects_blank_models_before_bundle_or_cluster_tooling() {
    let empty_root = tempfile::tempdir().expect("create empty bundle directory");
    let bundle = tempfile::tempdir().expect("create bundle directory");
    write_eval_suite(bundle.path());
    let empty_path = tempfile::tempdir().expect("create empty executable path");

    // Omit credential flags and env so this drives the documented default.
    // With PATH empty, any discovery-time kubectl or helm invocation would
    // fail before the model refusal and make this test red.
    for (model, cwd, label) in [
        ("", empty_root.path(), "empty"),
        (" \t ", bundle.path(), "whitespace-only"),
    ] {
        let output = Command::new(bin())
            .args([
                "--json",
                "cluster",
                "eval",
                "--model",
                model,
                "--channel",
                "C0EXAMPLE1",
            ])
            .current_dir(cwd)
            .env("KUBECONFIG", cwd.join("kubeconfig"))
            .env("CURIE_CONFIG_DIR", cwd.join("config"))
            .env("PATH", empty_path.path())
            .env_remove("CURIE_API_KEY")
            .env_remove("CURIE_VALKEY_PASSWORD")
            .env_remove("CURIE_EVAL_SAMPLES")
            .env_remove("CURIE_EVAL_AGGREGATION")
            .env_remove("CURIE_EVAL_PASS_AT_K")
            .output()
            .unwrap_or_else(|error| panic!("run cluster eval with {label} model: {error}"));
        assert_actionable_model_usage(&output, &format!("curie cluster eval --model <{label}>"));
    }
}

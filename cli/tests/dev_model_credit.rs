//! `curie dev model-credit` reports one credit class for the graded ladder.
//!
//! Response shapes are the ones recorded in the `cli/src/openrouter_credit.rs`
//! module comment: `GET /key` 200 with `limit_remaining` 73.49, `GET /credits`
//! 200 `{"data":{"total_credits":800,"total_usage":599.85}}`, and an unknown
//! key's 401 `{"error":{"message":"User not found.","code":401}}`.

mod support;

use std::process::{Command, Output};

use serde_json::{json, Value};
use support::{serve, MockServer, Response};

const KEY: &str = "sk-or-v1-PLACEHOLDER-model-credit";

fn key_body(limit: Value, limit_remaining: Value) -> Value {
    json!({"data":{
        "label":"sk-or-v1-PLACEHOLDER",
        "is_management_key":false,
        "is_provisioning_key":false,
        "limit":limit,
        "limit_reset":null,
        "limit_remaining":limit_remaining,
        "include_byok_in_limit":false,
        "usage":226.51,
        "usage_daily":0.11,
        "usage_weekly":19.44,
        "usage_monthly":5.74,
        "is_free_tier":false,
        "expires_at":null
    }})
}

fn credits_body(total_credits: f64, total_usage: f64) -> Value {
    json!({"data":{"total_credits":total_credits,"total_usage":total_usage}})
}

fn credit_server(limit_remaining: f64, total_usage: f64) -> MockServer {
    let key = key_body(json!(300), json!(limit_remaining)).to_string();
    let credits = credits_body(800.0, total_usage).to_string();
    serve(move |req| {
        let path = req.path.split('?').next().unwrap();
        if req.method == "GET" && path.ends_with("/key") {
            Response::json(200, &key)
        } else if req.method == "GET" && path.ends_with("/credits") {
            Response::json(200, &credits)
        } else {
            Response::json(404, r#"{"error":{"message":"Not Found","code":404}}"#)
        }
    })
}

/// Args are only `dev` `model-credit`, plus `--json` when that case asks for
/// it. The key is an environment value, never an argument.
fn run_model_credit(
    credentials: Option<&str>,
    model_alias: Option<&str>,
    json_output: bool,
    base_url: Option<&str>,
) -> Output {
    let home = tempfile::tempdir().expect("isolated home");
    let mut args = vec!["dev", "model-credit"];
    if json_output {
        args.push("--json");
    }
    assert!(
        args.iter()
            .all(|arg| !arg.contains(KEY) && !arg.contains("sk-or-")),
        "the key must not be an argument: {args:?}"
    );
    let mut command = Command::new(env!("CARGO_BIN_EXE_curie"));
    command
        .args(&args)
        .current_dir(home.path())
        .env("HOME", home.path())
        .env_remove("CURIE_CREDENTIALS")
        .env_remove("CURIE_MODEL_CREDENTIALS")
        .env_remove("CURIE_OPENROUTER_API_URL");
    if let Some(value) = credentials {
        command.env("CURIE_CREDENTIALS", value);
    }
    if let Some(value) = model_alias {
        command.env("CURIE_MODEL_CREDENTIALS", value);
    }
    if let Some(url) = base_url {
        command.env("CURIE_OPENROUTER_API_URL", url);
    }
    command.output().expect("run curie dev model-credit")
}

fn assert_key_hidden(output: &Output) {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!stdout.contains(KEY), "stdout leaked the key: {stdout}");
    assert!(!stderr.contains(KEY), "stderr leaked the key: {stderr}");
}

fn assert_human(output: &Output, class: &str, code: i32) {
    assert_key_hidden(output);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(
        stdout.as_ref(),
        format!("{class}\n"),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.status.code(), Some(code), "exit for {class}");
}

#[test]
fn unset_credentials_are_credential_missing() {
    let output = run_model_credit(None, None, false, None);
    assert_human(&output, "credential-missing", 1);
}

#[test]
fn empty_credentials_are_credential_missing() {
    let output = run_model_credit(Some(""), None, false, None);
    assert_human(&output, "credential-missing", 1);
}

#[test]
fn whitespace_credentials_are_credential_missing() {
    let output = run_model_credit(Some(" \t "), None, false, None);
    assert_human(&output, "credential-missing", 1);
}

#[test]
fn deprecated_model_credentials_alias_does_not_count() {
    let output = run_model_credit(None, Some(KEY), false, None);
    assert_human(&output, "credential-missing", 1);
}

#[test]
fn rejected_key_is_credential_rejected_and_sent_as_bearer() {
    // 401 body from the cli/src/openrouter_credit.rs module comment.
    let server =
        serve(|_| Response::json(401, r#"{"error":{"message":"User not found.","code":401}}"#));
    let output = run_model_credit(Some(KEY), None, false, Some(&server.base_url));
    assert_human(&output, "credential-rejected", 1);
    let request = server
        .recorded()
        .into_iter()
        .find(|req| req.path.split('?').next().unwrap().ends_with("/key"))
        .expect("the client requests {base}/key");
    assert_eq!(request.method, "GET");
    assert_eq!(
        request.header("authorization"),
        Some(format!("Bearer {KEY}").as_str())
    );
}

#[test]
fn one_usd_left_is_credit_exhausted() {
    // Account left is 800 - 799 = 1, below the key's 73.49, so the minimum is 1.
    let server = credit_server(73.49, 799.0);
    let output = run_model_credit(Some(KEY), None, false, Some(&server.base_url));
    assert_human(&output, "credit-exhausted", 1);
}

#[test]
fn recorded_balances_are_credit_sufficient() {
    // Recorded pair from the cli/src/openrouter_credit.rs module comment:
    // key limit_remaining 73.49, credits 800 and 599.85.
    let server = credit_server(73.49, 599.85);
    let output = run_model_credit(Some(KEY), None, false, Some(&server.base_url));
    assert_human(&output, "credit-sufficient", 0);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(!stdout.contains("credential-"));
    assert!(!stdout.contains("credit-exhausted"));
}

#[test]
fn exactly_five_usd_is_credit_sufficient() {
    // Key limit_remaining is 5.0 and the account balance is higher, so the
    // minimum remaining credit is exactly 5.0.
    let server = credit_server(5.0, 0.0);
    let output = run_model_credit(Some(KEY), None, false, Some(&server.base_url));
    assert_human(&output, "credit-sufficient", 0);
}

#[test]
fn json_empty_key_is_one_class_object() {
    let output = run_model_credit(Some(""), None, true, None);
    assert_key_hidden(&output);
    assert_eq!(
        String::from_utf8_lossy(&output.stdout).as_ref(),
        "{\"class\":\"credential-missing\"}\n",
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.status.code(), Some(1));
}

//! `curie dev model-credit` reports one credit class for the graded ladder.
//!
//! Response shapes are the ones recorded in the `cli/src/openrouter_credit.rs`
//! module comment: `GET /key` 200 with `limit_remaining` 73.49, `GET /credits`
//! 200 `{"data":{"total_credits":800,"total_usage":599.85}}`, and an unknown
//! key's 401 `{"error":{"message":"User not found.","code":401}}`.

use crate::support;

use std::ffi::OsStr;
use std::process::{Command, Output};

use serde_json::{json, Value};
use support::{serve, MockServer, Response};

const KEY: &str = "sk-or-v1-PLACEHOLDER-model-credit";
const ANTHROPIC_KEY: &str = "sk-ant-api03-PLACEHOLDER-model-credit";
const OTHER_KEY: &str = "synthetic-provider-PLACEHOLDER-model-credit";

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
    model_base_url: Option<&str>,
) -> Output {
    run_model_credit_os(
        credentials,
        model_alias,
        json_output,
        base_url,
        model_base_url.map(OsStr::new),
        None,
    )
}

fn run_model_credit_os(
    credentials: Option<&str>,
    model_alias: Option<&str>,
    json_output: bool,
    base_url: Option<&str>,
    model_base_url: Option<&OsStr>,
    inherited_proxy: Option<&str>,
) -> Output {
    let home = tempfile::tempdir().expect("isolated home");
    let mut args = vec!["dev", "model-credit"];
    if json_output {
        args.push("--json");
    }
    assert!(
        args.iter().all(|arg| {
            !arg.contains(KEY)
                && !arg.contains(ANTHROPIC_KEY)
                && !arg.contains(OTHER_KEY)
                && !arg.contains("sk-or-")
                && !arg.contains("sk-ant-")
        }),
        "the key must not be an argument"
    );
    let mut command = Command::new(env!("CARGO_BIN_EXE_curie"));
    command
        .args(&args)
        .current_dir(home.path())
        .env("HOME", home.path())
        .env_remove("CURIE_CREDENTIALS")
        .env_remove("CURIE_MODEL_CREDENTIALS")
        .env_remove("CURIE_MODEL_BASE_URL")
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
    if let Some(url) = model_base_url {
        command.env("CURIE_MODEL_BASE_URL", url);
    }
    // Loopback fixture traffic must not inherit a system proxy. reqwest reads
    // NO_PROXY before no_proxy, and the other binary tests set both.
    command
        .env("NO_PROXY", "127.0.0.1,localhost")
        .env("no_proxy", "127.0.0.1,localhost");
    if let Some(proxy) = inherited_proxy {
        // CGI mode ignores HTTP_PROXY, which would hide a missing bypass.
        command.env_remove("REQUEST_METHOD");
        for name in [
            "HTTP_PROXY",
            "http_proxy",
            "HTTPS_PROXY",
            "https_proxy",
            "ALL_PROXY",
            "all_proxy",
        ] {
            command.env(name, proxy);
        }
    }
    command.output().expect("run curie dev model-credit")
}

fn assert_secret_hidden(output: &Output, secret: &str) {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!stdout.contains(secret), "stdout leaked a credential");
    assert!(!stderr.contains(secret), "stderr leaked a credential");
}

fn assert_key_hidden(output: &Output) {
    assert_secret_hidden(output, KEY);
    assert_secret_hidden(output, ANTHROPIC_KEY);
    assert_secret_hidden(output, OTHER_KEY);
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
    let output = run_model_credit(None, None, false, None, None);
    assert_human(&output, "credential-missing", 1);
}

#[test]
fn empty_credentials_are_credential_missing() {
    let output = run_model_credit(Some(""), None, false, None, None);
    assert_human(&output, "credential-missing", 1);
}

#[test]
fn whitespace_credentials_are_credential_missing() {
    let output = run_model_credit(Some(" \t "), None, false, None, None);
    assert_human(&output, "credential-missing", 1);
}

#[test]
fn deprecated_model_credentials_alias_does_not_count() {
    let output = run_model_credit(None, Some(KEY), false, None, None);
    assert_human(&output, "credential-missing", 1);
}

#[test]
fn rejected_key_is_credential_rejected_and_sent_as_bearer() {
    // 401 body from the cli/src/openrouter_credit.rs module comment.
    let server =
        serve(|_| Response::json(401, r#"{"error":{"message":"User not found.","code":401}}"#));
    let output = run_model_credit(Some(KEY), None, false, Some(&server.base_url), None);
    assert_human(&output, "credential-rejected", 1);
    assert_key_requested(&server, KEY);
}

#[test]
fn one_usd_left_is_credit_exhausted() {
    // Account left is 800 - 799 = 1, below the key's 73.49, so the minimum is 1.
    let server = credit_server(73.49, 799.0);
    let output = run_model_credit(Some(KEY), None, false, Some(&server.base_url), None);
    assert_human(&output, "credit-exhausted", 1);
}

#[test]
fn recorded_balances_are_credit_sufficient() {
    // Recorded pair from the cli/src/openrouter_credit.rs module comment:
    // key limit_remaining 73.49, credits 800 and 599.85.
    let server = credit_server(73.49, 599.85);
    let output = run_model_credit(Some(KEY), None, false, Some(&server.base_url), None);
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
    let output = run_model_credit(Some(KEY), None, false, Some(&server.base_url), None);
    assert_human(&output, "credit-sufficient", 0);
}

#[test]
fn an_unlimited_key_with_account_credit_is_sufficient() {
    // limit and limit_remaining null is the recorded unlimited-key shape.
    // The account balance is 800 - 599.85, which is above 5 USD, so the
    // missing key limit must not be reported as credit-unknown.
    let key = key_body(Value::Null, Value::Null).to_string();
    let credits = credits_body(800.0, 599.85).to_string();
    let server = serve(move |req| {
        let path = req.path.split('?').next().unwrap();
        if req.method == "GET" && path.ends_with("/key") {
            Response::json(200, &key)
        } else if req.method == "GET" && path.ends_with("/credits") {
            Response::json(200, &credits)
        } else {
            Response::json(404, r#"{"error":{"message":"Not Found","code":404}}"#)
        }
    });
    let output = run_model_credit(Some(KEY), None, false, Some(&server.base_url), None);
    assert_human(&output, "credit-sufficient", 0);
}

#[test]
fn a_refused_credits_read_keeps_a_sufficient_key_limit() {
    // A key that may not read the account balance gets a non-2xx /credits.
    // The recorded module comment says that leaves the account unknown and
    // the key limit stands. 73.49 is above 5 USD, so this is not exhausted.
    let key = key_body(json!(300), json!(73.49)).to_string();
    let server = serve(move |req| {
        let path = req.path.split('?').next().unwrap();
        if req.method == "GET" && path.ends_with("/key") {
            Response::json(200, &key)
        } else if req.method == "GET" && path.ends_with("/credits") {
            Response::json(403, r#"{"error":{"message":"Forbidden","code":403}}"#)
        } else {
            Response::json(404, r#"{"error":{"message":"Not Found","code":404}}"#)
        }
    });
    let output = run_model_credit(Some(KEY), None, false, Some(&server.base_url), None);
    assert_human(&output, "credit-sufficient", 0);
}

#[test]
fn an_unlimited_key_with_no_account_balance_is_credit_unknown() {
    let key = key_body(Value::Null, Value::Null).to_string();
    let server = serve(move |req| {
        let path = req.path.split('?').next().unwrap();
        if req.method == "GET" && path.ends_with("/key") {
            Response::json(200, &key)
        } else {
            Response::json(403, r#"{"error":{"message":"Forbidden","code":403}}"#)
        }
    });
    let output = run_model_credit(Some(KEY), None, false, Some(&server.base_url), None);
    assert_human(&output, "credit-unknown", 1);
}

#[test]
fn a_non_401_key_error_is_credit_unavailable() {
    let server =
        serve(|_| Response::json(500, r#"{"error":{"message":"unavailable","code":500}}"#));
    let output = run_model_credit(Some(KEY), None, false, Some(&server.base_url), None);
    assert_human(&output, "credit-unavailable", 1);
}

#[test]
fn json_empty_key_is_one_class_object() {
    let output = run_model_credit(Some(""), None, true, None, None);
    assert_key_hidden(&output);
    assert_eq!(
        String::from_utf8_lossy(&output.stdout).as_ref(),
        "{\"class\":\"credential-missing\"}\n",
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.status.code(), Some(1));
}

/// A listener that would answer 401 if the command sent the credential.
fn rejecting_listener() -> MockServer {
    serve(|_| Response::json(401, r#"{"error":{"message":"User not found.","code":401}}"#))
}

fn assert_not_checked(server: &MockServer, output: &Output, secret: &str) {
    assert_secret_hidden(output, secret);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(stdout.as_ref(), "credit-not-checked\n");
    assert_eq!(output.status.code(), Some(1));
    let requests = server.recorded().len();
    assert_eq!(
        requests, 0,
        "rejected configuration made {requests} requests"
    );
}

fn assert_model_credit_json(stdout: &str) {
    let path = format!(
        "{}/schema/model-credit.schema.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let schema: Value =
        serde_json::from_str(&std::fs::read_to_string(path).expect("read model-credit schema"))
            .expect("model-credit schema is JSON");
    assert_eq!(
        schema["$id"], "https://schemas.curietech.ai/cli/model-credit/v1.1.json",
        "a new enum value bumps the schema minor"
    );
    let properties = schema["properties"].as_object().expect("properties");
    assert_eq!(
        properties.len(),
        1,
        "model-credit JSON stays a one-field object"
    );
    assert!(properties.contains_key("class"), "the field is class");
    let index: Value = serde_json::from_str(
        &std::fs::read_to_string(format!("{}/schema/index.json", env!("CARGO_MANIFEST_DIR")))
            .expect("read schema index"),
    )
    .expect("schema index is JSON");
    let version = index["results"]
        .as_array()
        .expect("results")
        .iter()
        .find(|entry| entry["result"] == "ModelCreditOutput")
        .expect("ModelCreditOutput inventory entry")["version"]
        .as_str();
    assert_eq!(version, Some("1.1"), "index version matches the schema id");
    let payload: Value = serde_json::from_str(stdout.trim_end()).expect("stdout is one JSON value");
    let validator = jsonschema::validator_for(&schema).expect("schema compiles");
    assert!(
        validator.is_valid(&payload),
        "stdout is not the model-credit schema"
    );
}

fn assert_key_requested(server: &MockServer, secret: &str) {
    let request = server
        .recorded()
        .into_iter()
        .find(|req| req.path.split('?').next().unwrap().ends_with("/key"))
        .expect("the client requests {base}/key");
    assert_eq!(request.method, "GET");
    let authorization = request.header("authorization").unwrap_or("");
    assert!(
        authorization == format!("Bearer {secret}"),
        "authorization was not the credential bearer"
    );
}

#[test]
fn anthropic_key_on_openrouter_host_is_not_checked() {
    let server = rejecting_listener();
    let output = run_model_credit(
        Some(ANTHROPIC_KEY),
        None,
        false,
        Some(&server.base_url),
        Some("https://openrouter.ai/api/v1"),
    );
    assert_not_checked(&server, &output, ANTHROPIC_KEY);
}

#[test]
fn anthropic_key_without_model_url_is_not_checked() {
    let server = rejecting_listener();
    let output = run_model_credit(
        Some(ANTHROPIC_KEY),
        None,
        false,
        Some(&server.base_url),
        None,
    );
    assert_not_checked(&server, &output, ANTHROPIC_KEY);
}

#[test]
fn unset_model_url_rejects_a_non_openrouter_key() {
    let server = rejecting_listener();
    let output = run_model_credit(Some(OTHER_KEY), None, false, Some(&server.base_url), None);
    assert_not_checked(&server, &output, OTHER_KEY);
}

#[test]
fn openrouter_key_on_another_host_is_not_checked() {
    let server = rejecting_listener();
    let output = run_model_credit(
        Some(KEY),
        None,
        false,
        Some(&server.base_url),
        Some("https://example.com/api/v1"),
    );
    assert_not_checked(&server, &output, KEY);
}

#[test]
fn openrouter_key_on_a_subdomain_is_not_checked() {
    let server = rejecting_listener();
    let output = run_model_credit(
        Some(KEY),
        None,
        false,
        Some(&server.base_url),
        Some("https://files.openrouter.ai/api/v1"),
    );
    assert_not_checked(&server, &output, KEY);
}

#[cfg(unix)]
#[test]
fn non_unicode_model_url_is_not_checked() {
    use std::os::unix::ffi::OsStringExt;

    let server = rejecting_listener();
    let model_url = std::ffi::OsString::from_vec(b"https://openrouter.ai/\xff".to_vec());
    let output = run_model_credit_os(
        Some(KEY),
        None,
        false,
        Some(&server.base_url),
        Some(model_url.as_os_str()),
        None,
    );
    assert_not_checked(&server, &output, KEY);
}

#[test]
fn model_url_without_a_host_is_not_checked() {
    let server = rejecting_listener();
    let output = run_model_credit(
        Some(KEY),
        None,
        false,
        Some(&server.base_url),
        Some("openrouter.ai"),
    );
    assert_not_checked(&server, &output, KEY);
}

#[test]
fn json_rejected_key_is_one_class_object() {
    let server = rejecting_listener();
    let output = run_model_credit(Some(OTHER_KEY), None, true, Some(&server.base_url), None);
    assert_secret_hidden(&output, OTHER_KEY);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(stdout.as_ref(), "{\"class\":\"credit-not-checked\"}\n");
    assert_model_credit_json(&stdout);
    assert_eq!(output.status.code(), Some(1));
    let requests = server.recorded().len();
    assert_eq!(
        requests, 0,
        "rejected configuration made {requests} requests"
    );
}

#[test]
fn allowed_probe_ignores_an_inherited_proxy() {
    let server = rejecting_listener();
    let proxy = serve(|_| Response::json(500, r#"{"error":"proxy"}"#));
    let output = run_model_credit_os(
        Some(KEY),
        None,
        false,
        Some(&server.base_url),
        None,
        Some(&proxy.base_url),
    );
    assert_human(&output, "credential-rejected", 1);
    assert_key_requested(&server, KEY);
    let proxied = proxy.recorded().len();
    assert_eq!(proxied, 0, "inherited proxy saw {proxied} requests");
}

#[test]
fn openrouter_key_without_model_url_still_requests_key() {
    let server = rejecting_listener();
    let output = run_model_credit(Some(KEY), None, false, Some(&server.base_url), None);
    assert_human(&output, "credential-rejected", 1);
    assert_key_requested(&server, KEY);
}

#[test]
fn non_anthropic_key_on_openrouter_host_still_requests_key() {
    let server = rejecting_listener();
    let output = run_model_credit(
        Some(OTHER_KEY),
        None,
        false,
        Some(&server.base_url),
        Some("https://openrouter.ai/api/v1"),
    );
    assert_secret_hidden(&output, OTHER_KEY);
    assert_human(&output, "credential-rejected", 1);
    assert_key_requested(&server, OTHER_KEY);
}

#[test]
fn normalized_openrouter_host_still_requests_key() {
    let server = rejecting_listener();
    let output = run_model_credit(
        Some(OTHER_KEY),
        None,
        false,
        Some(&server.base_url),
        Some("  https://OpenRouter.AI./api/v1  "),
    );
    assert_secret_hidden(&output, OTHER_KEY);
    assert_human(&output, "credential-rejected", 1);
    assert_key_requested(&server, OTHER_KEY);
}

#[test]
fn whitespace_model_url_keeps_the_openrouter_prefix_probe() {
    let server = rejecting_listener();
    let output = run_model_credit(Some(KEY), None, false, Some(&server.base_url), Some(" \t "));
    assert_human(&output, "credential-rejected", 1);
    assert_key_requested(&server, KEY);
}

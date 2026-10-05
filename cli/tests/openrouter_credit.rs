//! The factory quickstart's OpenRouter credit check (#3935) and the reviewer
//! model drift guard.
//!
//! OpenRouter is the only peer, served by `support::serve` with the response
//! shapes recorded from the real API on 2026-10-04 (values are placeholders):
//!
//! GET /api/v1/key -> 200
//! {"data":{"label":"sk-or-v1-PLACEHOLDER","is_management_key":false,
//!   "is_provisioning_key":false,"limit":300,"limit_reset":null,
//!   "limit_remaining":73.49,"include_byok_in_limit":false,"usage":226.51,
//!   "usage_daily":0.11,"usage_weekly":19.44,"usage_monthly":5.74,
//!   "is_free_tier":false,"expires_at":null}}
//! An unlimited key carries "limit":null,"limit_remaining":null.
//!
//! GET /api/v1/credits -> 200 {"data":{"total_credits":800,"total_usage":599.85}}
//!
//! An unknown key -> 401 {"error":{"message":"User not found.","code":401}}

mod support;

use curie::factory_quickstart::{credit_check_key, REVIEWER_MODEL, RUN_CREDIT_USD};
use curie::openrouter_credit::{remaining_credit_usd, remaining_from};
use serde_json::{json, Value};
use support::{serve, MockServer, Response};

const KEY: &str = "sk-or-v1-PLACEHOLDER-test-key";
const BASE_URL_ENV: &str = "CURIE_OPENROUTER_API_URL";

// The base URL is read from the process environment, which every test in this
// binary shares, so the tests that set it run one at a time.
static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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

fn assert_close(actual: Option<f64>, expected: f64) {
    let actual = actual.expect("a known remaining credit");
    assert!(
        (actual - expected).abs() < 1e-9,
        "expected {expected}, got {actual}"
    );
}

#[test]
fn a_key_limit_below_the_account_balance_is_the_remaining_credit() {
    let key = key_body(json!(300), json!(73.49));
    let credits = credits_body(800.0, 599.85);
    assert_close(remaining_from(&key, Some(&credits)), 73.49);
}

#[test]
fn an_account_balance_below_the_key_limit_is_the_remaining_credit() {
    // The #3935 case: the key still had 73.49 USD of its limit left, but the
    // account itself had about 1 USD, so every Opus reviewer call was refused.
    let key = key_body(json!(300), json!(73.49));
    let credits = credits_body(800.0, 799.0);
    assert_close(remaining_from(&key, Some(&credits)), 1.0);
}

#[test]
fn an_unlimited_key_uses_the_account_balance() {
    let key = key_body(Value::Null, Value::Null);
    let credits = credits_body(800.0, 599.85);
    assert_close(remaining_from(&key, Some(&credits)), 800.0 - 599.85);
}

#[test]
fn unknown_account_credits_use_the_key_limit() {
    let key = key_body(json!(300), json!(73.49));
    assert_close(remaining_from(&key, None), 73.49);
}

#[test]
fn an_unlimited_key_with_unknown_account_credits_is_unknown() {
    let key = key_body(Value::Null, Value::Null);
    assert_eq!(remaining_from(&key, None), None);
}

fn openrouter(credits_status: u16, credits: Value) -> MockServer {
    let credits = credits.to_string();
    serve(move |req| {
        let path = req.path.split('?').next().unwrap();
        if req.method == "GET" && path.ends_with("/key") {
            Response::json(200, &key_body(json!(300), json!(73.49)).to_string())
        } else if req.method == "GET" && path.ends_with("/credits") {
            Response::json(credits_status, &credits)
        } else {
            Response::json(404, r#"{"error":{"message":"Not Found","code":404}}"#)
        }
    })
}

#[tokio::test]
async fn the_live_check_sends_the_key_as_bearer_to_both_endpoints() {
    let _env = ENV_LOCK.lock().await;
    let server = openrouter(200, credits_body(800.0, 799.0));
    std::env::set_var(BASE_URL_ENV, &server.base_url);
    let remaining = remaining_credit_usd(KEY).await;
    std::env::remove_var(BASE_URL_ENV);
    assert_close(remaining.expect("credit check succeeds"), 1.0);
    let requests = server.recorded();
    for endpoint in ["/key", "/credits"] {
        let request = requests
            .iter()
            .find(|req| req.path.split('?').next().unwrap().ends_with(endpoint))
            .unwrap_or_else(|| panic!("no request to {endpoint}: {requests:?}"));
        assert_eq!(request.method, "GET");
        assert_eq!(
            request.header("authorization"),
            Some(format!("Bearer {KEY}").as_str()),
            "{endpoint}"
        );
    }
}

#[tokio::test]
async fn an_unknown_key_is_an_error_that_does_not_echo_the_key() {
    let _env = ENV_LOCK.lock().await;
    let server =
        serve(|_| Response::json(401, r#"{"error":{"message":"User not found.","code":401}}"#));
    std::env::set_var(BASE_URL_ENV, &server.base_url);
    let result = remaining_credit_usd(KEY).await;
    std::env::remove_var(BASE_URL_ENV);
    let error = result.expect_err("a 401 from /key is an error");
    let text = format!("{error:#} {error:?}");
    assert!(!text.contains(KEY), "the key leaked into the error: {text}");
}

#[tokio::test]
async fn a_refused_credits_endpoint_falls_back_to_the_key_limit() {
    let _env = ENV_LOCK.lock().await;
    let server = openrouter(403, json!({"error":{"message":"Forbidden","code":403}}));
    std::env::set_var(BASE_URL_ENV, &server.base_url);
    let remaining = remaining_credit_usd(KEY).await;
    std::env::remove_var(BASE_URL_ENV);
    assert_close(
        remaining.expect("a refused /credits is not an error"),
        73.49,
    );
}

#[test]
fn the_run_credit_is_five_usd() {
    assert_eq!(RUN_CREDIT_USD, 5.0);
}

#[test]
fn the_quickstart_reviewer_model_matches_the_factory_bundle() {
    let bundle = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../examples/dark-factory");
    let phases: Value = serde_json::from_str(
        &std::fs::read_to_string(bundle.join("progress/phases.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(phases["reviewer_model"], REVIEWER_MODEL, "phases.json");
    for agent in ["plan-reviewer.md", "diff-reviewer.md"] {
        let text = std::fs::read_to_string(bundle.join("agents").join(agent)).unwrap();
        let frontmatter = text
            .strip_prefix("---\n")
            .and_then(|rest| rest.split_once("\n---\n"))
            .map(|(head, _)| head)
            .unwrap_or_else(|| panic!("{agent} has no frontmatter"));
        let model = frontmatter
            .lines()
            .find_map(|line| line.strip_prefix("model:"))
            .unwrap_or_else(|| panic!("{agent} names no model"))
            .trim();
        assert_eq!(model, REVIEWER_MODEL, "{agent}");
    }
}

// Which key the pre-deploy credit check queries (#3935 review). It must be the
// key `cluster up` will deploy: the shell credential when set; otherwise the
// saved one only when the release records no real model, because a release
// that does keeps its recorded credential over the saved one.

#[test]
fn an_explicit_key_is_checked_whatever_is_saved_or_recorded() {
    for recorded in [false, true] {
        assert_eq!(
            credit_check_key(Some("explicit".into()), Some("saved".into()), recorded),
            Some("explicit".into()),
            "release_has_real_model={recorded}"
        );
        assert_eq!(
            credit_check_key(Some("explicit".into()), None, recorded),
            Some("explicit".into()),
            "release_has_real_model={recorded}"
        );
    }
}

#[test]
fn a_saved_key_is_checked_only_when_the_release_records_no_model() {
    assert_eq!(
        credit_check_key(None, Some("saved".into()), false),
        Some("saved".into())
    );
    assert_eq!(credit_check_key(None, Some("saved".into()), true), None);
}

#[test]
fn no_key_is_checked_when_none_is_known() {
    assert_eq!(credit_check_key(None, None, false), None);
    assert_eq!(credit_check_key(None, None, true), None);
}

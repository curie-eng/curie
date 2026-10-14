//! The OpenRouter credit left to a model credential, read before the factory
//! quickstart deploys (#3935).
//!
//! A factory run's Opus reviewers were refused with HTTP 402 when the account
//! had about 1 USD left, although the key still had 73.49 USD of its own limit.
//! The remaining credit is therefore the lower of the two known values.
//!
//! Shapes recorded from the real API on 2026-10-04:
//!
//! `GET /api/v1/key` -> 200 `{"data":{"limit":300,"limit_remaining":73.49,
//! "usage":226.51,...}}`; an unlimited key carries `"limit":null,
//! "limit_remaining":null`. An unknown key -> 401
//! `{"error":{"message":"User not found.","code":401}}`.
//!
//! `GET /api/v1/credits` -> 200 `{"data":{"total_credits":800,
//! "total_usage":599.85}}`. A key that may not read the account balance gets a
//! non-2xx answer, which leaves the account balance unknown.

use std::time::Duration;

use anyhow::Result;
use serde_json::Value;

use crate::exit::CliError;
use crate::ui::CliOutput;

/// Overrides the OpenRouter API base, for tests.
const BASE_URL_ENV: &str = "CURIE_OPENROUTER_API_URL";
const DEFAULT_BASE_URL: &str = "https://openrouter.ai/api/v1";

/// The lower of the key's `limit_remaining` and the account's
/// `total_credits - total_usage`, over whichever of them is known.
pub fn remaining_from(key: &Value, credits: Option<&Value>) -> Option<f64> {
    let key_left = key.pointer("/data/limit_remaining").and_then(Value::as_f64);
    let account_left = credits.and_then(|credits| {
        let total = credits.pointer("/data/total_credits")?.as_f64()?;
        let used = credits.pointer("/data/total_usage")?.as_f64()?;
        Some(total - used)
    });
    match (key_left, account_left) {
        (Some(key), Some(account)) => Some(key.min(account)),
        (left, None) | (None, left) => left,
    }
}

/// Reads the credit left to `key`. `Ok(None)` means neither the key limit nor
/// the account balance is known. An error never carries the key.
pub async fn remaining_credit_usd(key: &str) -> Result<Option<f64>> {
    let base = std::env::var(BASE_URL_ENV)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_BASE_URL.to_string());
    let base = base.trim_end_matches('/');
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|err| CliError::failure(format!("could not build an HTTP client: {err}")))?;
    let get = |path: &str| client.get(format!("{base}{path}")).bearer_auth(key).send();

    let response = get("/key")
        .await
        .map_err(|err| CliError::failure(format!("OpenRouter /key request failed: {err}")))?;
    let status = response.status();
    if !status.is_success() {
        return Err(OpenRouterKeyHttp { status }.into());
    }
    let key_json: Value = response
        .json()
        .await
        .map_err(|err| CliError::failure(format!("OpenRouter /key was not JSON: {err}")))?;

    let credits_json = match get("/credits").await {
        Ok(response) if response.status().is_success() => response.json::<Value>().await.ok(),
        _ => None,
    };
    Ok(remaining_from(&key_json, credits_json.as_ref()))
}

/// A non-success `GET /key`. Display stays `OpenRouter /key answered HTTP {status}`
/// and never includes the credential. `status` is the HTTP status, so a 401 is
/// distinct from any other status without parsing that sentence.
#[derive(Debug)]
pub struct OpenRouterKeyHttp {
    pub status: reqwest::StatusCode,
}

impl std::fmt::Display for OpenRouterKeyHttp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "OpenRouter /key answered HTTP {status}",
            status = self.status
        )
    }
}

impl std::error::Error for OpenRouterKeyHttp {}

const MODEL_CREDENTIAL_ENV: &str = "CURIE_CREDENTIALS";

/// `curie dev model-credit` stdout. Human output is the class token. JSON is
/// exactly `{"class":"<token>"}`.
pub struct ModelCreditOutput {
    class: &'static str,
}

impl CliOutput for ModelCreditOutput {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({ "class": self.class })
    }

    fn render(&self, ui: &crate::ui::Ui) {
        ui.payload_plain(self.class);
    }
}

/// Classify `CURIE_CREDENTIALS` and emit one credit class.
///
/// Exit 0 only for `credit-sufficient`. Every other class exits 1. The
/// credential is never copied into a message.
pub async fn model_credit() -> Result<()> {
    let class = model_credit_class().await;
    let output = ModelCreditOutput { class };
    let ui = crate::ui::ui();
    if class == "credit-sufficient" {
        ui.emit(&output);
        return Ok(());
    }
    let failure = CliError::failure(class);
    if ui.json() {
        Err(crate::exit::with_json_payload(
            failure.into(),
            output.to_json(),
        ))
    } else {
        ui.emit(&output);
        Err(failure.into())
    }
}

async fn model_credit_class() -> &'static str {
    let key = std::env::var(MODEL_CREDENTIAL_ENV).unwrap_or_default();
    if key.trim().is_empty() {
        return "credential-missing";
    }
    match remaining_credit_usd(&key).await {
        Ok(Some(left)) if left < crate::factory_quickstart::RUN_CREDIT_USD => "credit-exhausted",
        Ok(Some(_)) => "credit-sufficient",
        Ok(None) => "credit-unknown",
        Err(error) => match openrouter_key_status(&error) {
            Some(401) => "credential-rejected",
            _ => "credit-unavailable",
        },
    }
}

fn openrouter_key_status(error: &anyhow::Error) -> Option<u16> {
    error.chain().find_map(|cause| {
        cause
            .downcast_ref::<OpenRouterKeyHttp>()
            .map(|http| http.status.as_u16())
    })
}

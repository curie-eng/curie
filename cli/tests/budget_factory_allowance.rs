//! Integration: `curie cluster budget` driven through the built binary keeps a
//! factory agent's per-run output allowance when only the dollar limit
//! changes (#4939). The factory quickstart sets 250000 output tokens per run;
//! a later `--limit` change must not reset that to null or to the generic
//! default. The platform API is the only fake, using the `BudgetConfig` shape
//! from the committed apps/api openapi.json.

use crate::support;

use std::process::Command;

use support::{serve, Response};

const AGENT_ID: &str = "22222222-2222-2222-2222-222222222222";

#[test]
fn budget_limit_change_keeps_the_factory_output_allowance() {
    let budget_path = format!("/agents/{AGENT_ID}/budget");
    let server = {
        let budget_path = budget_path.clone();
        serve(move |req| match (req.method.as_str(), req.path.as_str()) {
            ("GET", "/agents") => Response::json(
                200,
                &format!(
                    r#"[{{"id":"{AGENT_ID}","name":"dark-factory","channels":[{{"kind":"github","address":"acme/widgets"}}],"created_at":"2026-10-04T00:00:00Z","memory":false}}]"#
                ),
            ),
            ("GET", p) if p == budget_path => Response::json(
                200,
                r#"{"max_output_tokens_per_run":250000,"max_usd_per_day":5.0}"#,
            ),
            ("PUT", p) if p == budget_path => Response::json(
                200,
                r#"{"max_output_tokens_per_run":250000,"max_usd_per_day":9.0}"#,
            ),
            other => Response::json(500, &format!(r#"{{"detail":"unexpected {other:?}"}}"#)),
        })
    };
    let output = Command::new(env!("CARGO_BIN_EXE_curie"))
        .args([
            "cluster",
            "budget",
            "dark-factory",
            "--limit",
            "9",
            "--api-url",
            &server.base_url,
            "--api-key",
            "fixture-key",
            "--json",
        ])
        .env_remove("CURIE_API_URL")
        .env_remove("CURIE_API_KEY")
        .output()
        .unwrap();
    let shown = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.status.success(), "{shown}");

    let requests = server.recorded();
    let flow: Vec<(&str, &str)> = requests
        .iter()
        .map(|req| (req.method.as_str(), req.path.as_str()))
        .collect();
    assert_eq!(
        flow,
        vec![
            ("GET", "/agents"),
            ("GET", budget_path.as_str()),
            ("PUT", budget_path.as_str()),
        ],
        "{shown}"
    );
    let put: serde_json::Value = serde_json::from_slice(&requests[2].body).unwrap();
    assert_eq!(
        put,
        serde_json::json!({"max_output_tokens_per_run": 250000, "max_usd_per_day": 9.0}),
        "a dollar-only change must keep the factory output allowance"
    );
    let saved: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(saved["max_output_tokens_per_run"], 250000, "{saved}");
    assert_eq!(saved["max_usd_per_day"], 9.0, "{saved}");
}

#[test]
fn budget_default_for_generic_agents_stays_100000_tokens_and_5_usd() {
    // The factory allowance is quickstart-only; agents created through
    // `skill`, `init` and `eval` keep the generic default.
    let budget: serde_json::Value = serde_json::from_str(curie::commands::DEFAULT_BUDGET).unwrap();
    assert_eq!(
        budget,
        serde_json::json!({"max_output_tokens_per_run": 100000, "max_usd_per_day": 5.0})
    );
}

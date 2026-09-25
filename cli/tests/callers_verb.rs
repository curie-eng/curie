//! Integration: the `callers` verb at BOTH tiers (ADR 0175, #3241).
//!
//! A surface may carry a list of who may talk to the bot through it. This verb
//! shows, sets and clears that list through `PUT /agents/{id}/channels/callers`.
//! The tests drive the BUILT BINARY against an in-process mock of the platform
//! API, so they cover clap wiring (which flag became which argument, and that
//! `--set` and `--clear` conflict), the request the `ApiClient` sends, and the
//! `--json` payload, validated against the committed `callers.schema.json`.
//!
//! Both tiers are covered because the local/cluster verb pair is a parity seam
//! (AGENTS.md). The cluster tier also gets the case only it can have: a
//! malformed id is refused before the connection is discovered.

mod support;

use std::process::Command;
use support::{serve, MockServer, Response};

const AGENT_ID: &str = "55555555-5555-5555-5555-555555555555";
const AGENT_NAME: &str = "acme-bot";
const CHANNEL: &str = "C0EXAMPLE1";
const LISTED: &str = "U0EXAMPLE1";
const OTHER: &str = "U0EXAMPLE2";

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn callers_path() -> String {
    format!("/agents/{AGENT_ID}/channels/callers")
}

/// One agent with one Slack surface, as the API reads it back.
fn agent_json(callers: Option<&[&str]>) -> String {
    let list = match callers {
        None => "null".to_string(),
        Some(ids) => serde_json::to_string(ids).expect("ids serialize"),
    };
    format!(
        r#"{{"id":"{AGENT_ID}","name":"{AGENT_NAME}","channels":[{{"kind":"slack","address":"{CHANNEL}","adapter":"default","allowed_callers":{list}}}],"created_at":"2026-09-25T00:00:00Z","memory":false}}"#
    )
}

/// A mock API holding one surface whose list starts as `initial`. A PUT answers
/// the agent as the body asked it to be stored, so the CLI's report reflects
/// what the API answered rather than what the CLI sent.
fn api(initial: Option<&'static [&'static str]>) -> MockServer {
    serve(move |req| {
        let (m, p) = (req.method.as_str(), req.path.as_str());
        match m {
            "GET" if p == "/agents" => Response::json(200, &format!("[{}]", agent_json(initial))),
            "GET" if p == format!("/agents/{AGENT_ID}") => {
                Response::json(200, &agent_json(initial))
            }
            "PUT" if p.starts_with(&callers_path()) => {
                let body: serde_json::Value =
                    serde_json::from_slice(&req.body).expect("the PUT body is JSON");
                let stored: Option<Vec<String>> =
                    serde_json::from_value(body["allowed_callers"].clone())
                        .expect("a list or null");
                let ids: Option<Vec<&str>> = stored
                    .as_ref()
                    .map(|ids| ids.iter().map(String::as_str).collect());
                Response::json(200, &agent_json(ids.as_deref()))
            }
            _ => Response::json(405, r#"{"detail":"unexpected request"}"#),
        }
    })
}

struct Run {
    code: i32,
    stdout: String,
    stderr: String,
}

impl Run {
    fn output(&self) -> String {
        format!("{}{}", self.stdout, self.stderr)
    }
}

fn run(argv: &[&str]) -> Run {
    let output = Command::new(bin())
        .args(argv)
        .env_remove("CURIE_API_URL")
        .env_remove("CURIE_API_KEY")
        .env("KUBECONFIG", "/nonexistent/curie-test-kubeconfig")
        .output()
        .unwrap_or_else(|e| panic!("run curie {}: {e}", argv.join(" ")));
    Run {
        code: output.status.code().unwrap_or(-1),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    }
}

fn puts(server: &MockServer) -> Vec<(String, serde_json::Value)> {
    server
        .recorded()
        .into_iter()
        .filter(|r| r.method == "PUT")
        .map(|r| {
            (
                r.path.clone(),
                serde_json::from_slice(&r.body).expect("the PUT body is JSON"),
            )
        })
        .collect()
}

fn assert_matches_schema(value: &serde_json::Value) {
    let schema: serde_json::Value =
        serde_json::from_str(include_str!("../schema/callers.schema.json"))
            .expect("callers.schema.json parses");
    let validator = jsonschema::validator_for(&schema).expect("schema compiles");
    let errors: Vec<String> = validator
        .iter_errors(value)
        .map(|e| e.to_string())
        .collect();
    assert!(
        errors.is_empty(),
        "payload violates callers.schema.json: {errors:?}\n{value}"
    );
}

fn tier_args<'a>(tier: &'a str, base_url: &'a str, extra: &[&'a str]) -> Vec<&'a str> {
    let mut argv = vec![tier, "callers", AGENT_NAME, "--surface", "slack=C0EXAMPLE1"];
    argv.extend_from_slice(extra);
    argv.extend_from_slice(&["--api-url", base_url, "--api-key", "k"]);
    argv
}

#[test]
fn set_replaces_the_list_with_exactly_the_ids_given_at_both_tiers() {
    for tier in ["local", "cluster"] {
        let server = api(None);
        let run = run(&tier_args(
            tier,
            &server.base_url,
            &["--set", "U0EXAMPLE1,U0EXAMPLE2", "--json"],
        ));
        assert_eq!(run.code, 0, "{tier} set must succeed: {}", run.output());
        let sent = puts(&server);
        assert_eq!(sent.len(), 1, "{tier}: exactly one PUT: {sent:?}");
        assert!(
            sent[0].0.contains("kind=slack") && sent[0].0.contains("address=C0EXAMPLE1"),
            "{tier}: the surface travels as the query selector: {}",
            sent[0].0
        );
        assert_eq!(
            sent[0].1,
            serde_json::json!({"allowed_callers": [LISTED, OTHER]}),
            "{tier}: the whole list travels, in order"
        );
        let value: serde_json::Value = serde_json::from_str(&run.stdout).expect("one JSON object");
        assert_matches_schema(&value);
        assert_eq!(value["allowed_callers"], serde_json::json!([LISTED, OTHER]));
        assert_eq!(value["changed"], true);
    }
}

#[test]
fn clear_sends_an_explicit_null_at_both_tiers() {
    for tier in ["local", "cluster"] {
        let server = api(Some(&[LISTED]));
        let run = run(&tier_args(tier, &server.base_url, &["--clear", "--json"]));
        assert_eq!(run.code, 0, "{tier} clear must succeed: {}", run.output());
        let sent = puts(&server);
        assert_eq!(
            sent.iter()
                .map(|(_, body)| body.clone())
                .collect::<Vec<_>>(),
            vec![serde_json::json!({"allowed_callers": null})],
            "{tier}: clearing is an explicit null, never an empty list"
        );
        let value: serde_json::Value = serde_json::from_str(&run.stdout).expect("one JSON object");
        assert_matches_schema(&value);
        assert!(value["allowed_callers"].is_null());
    }
}

#[test]
fn no_flag_shows_the_list_and_writes_nothing() {
    let server = api(Some(&[LISTED]));
    let json = run(&tier_args("local", &server.base_url, &["--json"]));
    assert_eq!(json.code, 0, "show must succeed: {}", json.output());
    let value: serde_json::Value = serde_json::from_str(&json.stdout).expect("one JSON object");
    assert_matches_schema(&value);
    assert_eq!(value["allowed_callers"], serde_json::json!([LISTED]));
    assert_eq!(value["changed"], false);

    let human = run(&tier_args("local", &server.base_url, &[]));
    assert_eq!(human.code, 0, "show must succeed: {}", human.output());
    assert!(human.stdout.contains(LISTED), "{}", human.output());
    assert!(puts(&server).is_empty(), "a show must never write");

    let open = api(None);
    let open_run = run(&tier_args("local", &open.base_url, &[]));
    assert!(
        open_run.stdout.contains("everyone"),
        "an open surface says so: {}",
        open_run.output()
    );
}

#[test]
fn set_and_clear_conflict_at_parse_time() {
    let server = api(None);
    let run = run(&tier_args(
        "local",
        &server.base_url,
        &["--set", LISTED, "--clear"],
    ));
    assert_eq!(
        run.code,
        2,
        "a contradictory invocation is a usage error: {}",
        run.output()
    );
    assert!(
        !run.output().contains("unrecognized subcommand"),
        "exit 2 must come from the flag conflict: {}",
        run.output()
    );
    assert!(server.recorded().is_empty(), "nothing may reach the API");
}

#[test]
fn an_unknown_surface_is_a_usage_error_with_a_fix() {
    let server = api(None);
    let run = run(&[
        "local",
        "callers",
        AGENT_NAME,
        "--surface",
        "slack=C0EXAMPLE9",
        "--api-url",
        &server.base_url,
        "--api-key",
        "k",
        "--json",
    ]);
    assert_eq!(run.code, 2, "{}", run.output());
    assert!(
        run.output().contains("surfaces"),
        "the fix names the surfaces verb: {}",
        run.output()
    );
}

#[test]
fn an_api_refusal_surfaces_as_a_failure_not_a_success() {
    let server = serve(|req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => Response::json(200, &format!("[{}]", agent_json(None))),
        ("PUT", _) => Response::json(
            422,
            r#"{"detail":[{"loc":["body","allowed_callers"],"msg":"Value error, caller '@owner' is not a Slack user or bot id"}]}"#,
        ),
        _ => Response::json(405, r#"{"detail":"unexpected"}"#),
    });
    let run = run(&tier_args(
        "local",
        &server.base_url,
        &["--set", "@owner", "--json"],
    ));
    assert_ne!(
        run.code,
        0,
        "a refused list must not exit 0: {}",
        run.output()
    );
    assert!(
        run.output().contains("not a Slack user or bot id"),
        "{}",
        run.output()
    );
}

#[test]
fn cluster_refuses_a_malformed_id_before_resolving_the_connection() {
    // With no --api-url the cluster tier discovers the API through kubectl;
    // KUBECONFIG points nowhere, so a check that ran second would fail about
    // the cluster instead of the id.
    let run = run(&[
        "cluster",
        "callers",
        AGENT_NAME,
        "--surface",
        "slack=C0EXAMPLE1",
        "--set",
        "U0EXAMPLE1,two words",
    ]);
    assert_eq!(
        run.code,
        2,
        "a malformed id is a usage error: {}",
        run.output()
    );
    assert!(run.output().contains("two words"), "{}", run.output());
    assert!(
        !run.output().contains("kubectl") && !run.output().contains("kubeconfig"),
        "the id is refused before any cluster lookup: {}",
        run.output()
    );
}

#[test]
fn dry_run_prints_the_put_and_touches_nothing() {
    let server = api(None);
    let run = run(&tier_args(
        "local",
        &server.base_url,
        &["--set", LISTED, "--dry-run", "--json"],
    ));
    assert_eq!(run.code, 0, "{}", run.output());
    let value: serde_json::Value = serde_json::from_str(&run.stdout).expect("one JSON object");
    assert_matches_schema(&value);
    let plan = value["plan"][0].as_str().expect("a plan line");
    assert!(
        plan.contains("PUT") && plan.contains("/channels/callers"),
        "{plan}"
    );
    assert!(server.recorded().is_empty(), "a dry run makes no request");
}

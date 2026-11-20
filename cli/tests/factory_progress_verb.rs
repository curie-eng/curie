//! Integration: the `factory-progress` verb at BOTH tiers.
//!
//! A factory agent's Slack progress channel is `factory_slack_channel` on
//! `PATCH /agents/{id}`. These tests drive the BUILT BINARY against an
//! in-process mock of the platform API, so they cover clap wiring, the exact
//! request body the `ApiClient` sends (an omitted key means unchanged, an
//! explicit null clears), the unbound refusal, and the `--json` payload. The
//! `schema_*` tests validate what the binary really sends against the
//! committed `apps/api/openapi.json`, so a drift in the route's schema fails
//! here instead of at an operator's terminal.
//!
//! Test names carry a group prefix (`set_`, `clear_`, `unbound_`, `status_`,
//! `schema_`) because acceptance receipts select tests by prefix.

use crate::support;

use std::process::Command;
use std::sync::{Arc, Mutex};
use support::{serve, MockServer, Response};

const AGENT_ID: &str = "55555555-5555-5555-5555-555555555555";
const AGENT_NAME: &str = "acme-bot";
const CHANNEL: &str = "C0EXAMPLE1";
const TIERS: [&str; 2] = ["local", "cluster"];

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

/// One agent as `AgentOut` reads it, with the progress channel as given.
fn agent_value(channel: Option<&str>) -> serde_json::Value {
    serde_json::json!({
        "id": AGENT_ID,
        "name": AGENT_NAME,
        "channels": [{"kind": "slack", "address": CHANNEL, "adapter": "default"}],
        "repo_full_name": null,
        "deploy_notifications": false,
        "behavior_packs": null,
        "model": null,
        "reviewer_model": null,
        "thinking": null,
        "approval_required_tools": null,
        "approval_routes": null,
        "hook_partitions": null,
        "source_bindings": null,
        "secrets": null,
        "memory": false,
        "created_at": "2026-11-20T00:00:00Z",
        "factory_slack_channel": channel,
    })
}

/// What a PATCH answers.
#[derive(Clone)]
enum Patch {
    /// Store the key if the body carried it, answer the stored row.
    Store,
    /// Answer this status and body.
    Refuse(u16, &'static str),
}

/// A mock API holding one agent whose progress channel starts as `initial`.
/// The PATCH answer reflects what the body asked to be stored, so the CLI's
/// report is what the API answered, not what the CLI sent.
fn api(initial: Option<&'static str>, patch: Patch) -> MockServer {
    let stored: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(initial.map(str::to_string)));
    serve(move |req| {
        let (method, path) = (req.method.as_str(), req.path.as_str());
        let current = stored.lock().unwrap().clone();
        match method {
            "GET" if path == "/agents" => Response::json(
                200,
                &serde_json::Value::Array(vec![agent_value(current.as_deref())]).to_string(),
            ),
            "PATCH" if path == format!("/agents/{AGENT_ID}") => match &patch {
                Patch::Refuse(status, body) => Response::json(*status, body),
                Patch::Store => {
                    let body: serde_json::Value =
                        serde_json::from_slice(&req.body).expect("the PATCH body is JSON");
                    if let Some(value) = body.get("factory_slack_channel") {
                        *stored.lock().unwrap() = value.as_str().map(str::to_string);
                    }
                    let now = stored.lock().unwrap().clone();
                    Response::json(200, &agent_value(now.as_deref()).to_string())
                }
            },
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

    fn json(&self) -> serde_json::Value {
        serde_json::from_str(&self.stdout)
            .unwrap_or_else(|e| panic!("one JSON object on stdout ({e}): {}", self.output()))
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

fn tier_args<'a>(tier: &'a str, base_url: &'a str, extra: &[&'a str]) -> Vec<&'a str> {
    let mut argv = vec![tier, "factory-progress", AGENT_NAME];
    argv.extend_from_slice(extra);
    argv.extend_from_slice(&["--api-url", base_url, "--api-key", "k"]);
    argv
}

/// Every PATCH body the server received.
fn patches(server: &MockServer) -> Vec<serde_json::Value> {
    server
        .recorded()
        .into_iter()
        .filter(|r| r.method == "PATCH")
        .map(|r| serde_json::from_slice(&r.body).expect("the PATCH body is JSON"))
        .collect()
}

fn validator_for(schema: serde_json::Value) -> jsonschema::Validator {
    jsonschema::validator_for(&schema).expect("schema compiles")
}

fn assert_valid(validator: &jsonschema::Validator, value: &serde_json::Value, what: &str) {
    let errors: Vec<String> = validator
        .iter_errors(value)
        .map(|e| e.to_string())
        .collect();
    assert!(
        errors.is_empty(),
        "{what} violates its schema: {errors:?}\n{value}"
    );
}

/// A validator for one component of the committed API contract.
fn api_component(name: &str) -> jsonschema::Validator {
    let openapi: serde_json::Value =
        serde_json::from_str(include_str!("../../apps/api/openapi.json")).expect("openapi parses");
    validator_for(serde_json::json!({
        "$ref": format!("#/components/schemas/{name}"),
        "components": openapi["components"],
    }))
}

fn cli_schema() -> jsonschema::Validator {
    validator_for(
        serde_json::from_str(include_str!("../schema/factory-progress.schema.json"))
            .expect("factory-progress.schema.json parses"),
    )
}

#[test]
fn set_sends_only_the_channel_and_reports_it_at_both_tiers() {
    for tier in TIERS {
        let server = api(None, Patch::Store);
        let run = run(&tier_args(
            tier,
            &server.base_url,
            &["--slack-channel", CHANNEL, "--json"],
        ));
        assert_eq!(run.code, 0, "{tier} set must succeed: {}", run.output());
        let sent = patches(&server);
        assert_eq!(sent.len(), 1, "{tier}: exactly one PATCH: {sent:?}");
        assert_eq!(
            sent[0].as_object().map(|o| o.len()),
            Some(1),
            "{tier}: the body carries only the channel: {}",
            sent[0]
        );
        assert_eq!(
            sent[0],
            serde_json::json!({"factory_slack_channel": CHANNEL}),
            "{tier}"
        );
        let value = run.json();
        assert_eq!(value["factory_slack_channel"], CHANNEL, "{tier}");
        assert_eq!(value["changed"], true, "{tier}");
        assert_eq!(value["agent"], AGENT_NAME, "{tier}");
    }
}

#[test]
fn set_prints_the_human_line_with_now_after_a_write() {
    let server = api(None, Patch::Store);
    let run = run(&tier_args(
        "local",
        &server.base_url,
        &["--slack-channel", CHANNEL],
    ));
    assert_eq!(run.code, 0, "{}", run.output());
    assert!(
        run.stdout.contains(&format!(
            "factory progress channel for {AGENT_NAME} now: {CHANNEL}"
        )),
        "{}",
        run.output()
    );
}

#[test]
fn set_refuses_a_malformed_id_before_any_request_at_both_tiers() {
    for tier in TIERS {
        for bad in ["c0example1", "C0EXA", "D0EXAMPLE1"] {
            let server = api(None, Patch::Store);
            let run = run(&tier_args(
                tier,
                &server.base_url,
                &["--slack-channel", bad],
            ));
            assert_eq!(run.code, 2, "{tier} {bad}: a usage error: {}", run.output());
            assert!(
                run.output().contains("^[CG][A-Z0-9]{6,}$"),
                "{tier} {bad}: the error names the expected shape: {}",
                run.output()
            );
            assert!(
                server.recorded().is_empty(),
                "{tier} {bad}: nothing may reach the API"
            );
        }
    }
}

#[test]
fn set_cluster_refuses_a_malformed_id_before_resolving_the_connection() {
    // With no --api-url the cluster tier discovers the API through kubectl;
    // KUBECONFIG points nowhere, so a check that ran second would fail about
    // the cluster instead of the id.
    let run = run(&[
        "cluster",
        "factory-progress",
        AGENT_NAME,
        "--slack-channel",
        "general",
    ]);
    assert_eq!(run.code, 2, "{}", run.output());
    assert!(run.output().contains("general"), "{}", run.output());
    assert!(
        !run.output().contains("kubectl") && !run.output().contains("kubeconfig"),
        "the id is refused before any cluster lookup: {}",
        run.output()
    );
}

#[test]
fn set_dry_run_prints_the_patch_and_touches_nothing() {
    let server = api(None, Patch::Store);
    let run = run(&tier_args(
        "local",
        &server.base_url,
        &["--slack-channel", CHANNEL, "--dry-run", "--json"],
    ));
    assert_eq!(run.code, 0, "{}", run.output());
    let value = run.json();
    let plan = value["plan"][0].as_str().expect("a plan line");
    assert!(
        plan.contains("PATCH") && plan.contains(CHANNEL),
        "the plan shows the write: {plan}"
    );
    assert!(server.recorded().is_empty(), "a dry run makes no request");
}

#[test]
fn set_a_422_is_a_usage_error_carrying_the_api_message() {
    let server = api(
        None,
        Patch::Refuse(
            422,
            r#"{"detail":[{"loc":["body","factory_slack_channel"],"msg":"String should match pattern"}]}"#,
        ),
    );
    let run = run(&tier_args(
        "local",
        &server.base_url,
        &["--slack-channel", CHANNEL],
    ));
    assert_eq!(run.code, 2, "{}", run.output());
    assert!(
        run.output().contains("String should match pattern"),
        "{}",
        run.output()
    );
}

#[test]
fn clear_sends_the_key_as_an_explicit_null_at_both_tiers() {
    for tier in TIERS {
        let server = api(Some(CHANNEL), Patch::Store);
        let run = run(&tier_args(
            tier,
            &server.base_url,
            &["--no-slack-channel", "--json"],
        ));
        assert_eq!(run.code, 0, "{tier} clear must succeed: {}", run.output());
        let sent = patches(&server);
        assert_eq!(sent.len(), 1, "{tier}: exactly one PATCH: {sent:?}");
        let body = sent[0].as_object().expect("an object body");
        // An omitted key would mean "unchanged" to the API, so the key must be
        // present AND null.
        assert!(
            body.contains_key("factory_slack_channel"),
            "{tier}: the key is present: {body:?}"
        );
        assert_eq!(
            sent[0].get("factory_slack_channel"),
            Some(&serde_json::Value::Null),
            "{tier}: the key is JSON null"
        );
        assert_eq!(body.len(), 1, "{tier}: nothing else travels: {body:?}");
        let value = run.json();
        assert!(value["factory_slack_channel"].is_null(), "{tier}: {value}");
        assert_eq!(value["changed"], true, "{tier}");
    }
}

#[test]
fn clear_and_set_conflict_at_parse_time() {
    let server = api(None, Patch::Store);
    let run = run(&tier_args(
        "local",
        &server.base_url,
        &["--slack-channel", CHANNEL, "--no-slack-channel"],
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
fn clear_human_line_says_none_after_a_write() {
    let server = api(Some(CHANNEL), Patch::Store);
    let run = run(&tier_args(
        "local",
        &server.base_url,
        &["--no-slack-channel"],
    ));
    assert_eq!(run.code, 0, "{}", run.output());
    assert!(
        run.stdout.contains(&format!(
            "factory progress channel for {AGENT_NAME} now: none (factory runs post no Slack progress)"
        )),
        "{}",
        run.output()
    );
}

#[test]
fn unbound_names_the_exact_surfaces_command_for_the_tier() {
    for tier in TIERS {
        let server = api(
            None,
            Patch::Refuse(
                409,
                r#"{"detail":{"code":"factory_slack_channel_unbound"}}"#,
            ),
        );
        let run = run(&tier_args(
            tier,
            &server.base_url,
            &["--slack-channel", CHANNEL],
        ));
        assert_eq!(
            run.code,
            1,
            "{tier}: a failure, not usage: {}",
            run.output()
        );
        let expected = format!("curie {tier} surfaces {AGENT_NAME} --add slack={CHANNEL}");
        assert!(
            run.output().contains(&expected),
            "{tier}: the fix names `{expected}`: {}",
            run.output()
        );
        assert!(
            run.output().contains("rerun this command"),
            "{tier}: {}",
            run.output()
        );
        assert!(
            !run.output().contains("409"),
            "{tier}: not a bare status: {}",
            run.output()
        );
        assert!(
            patches(&server).len() == 1,
            "{tier}: one PATCH and no automatic binding: {:?}",
            server
                .recorded()
                .iter()
                .map(|r| format!("{} {}", r.method, r.path))
                .collect::<Vec<_>>()
        );
        assert!(
            server
                .recorded()
                .iter()
                .all(|r| !r.path.contains("/channels")),
            "{tier}: the binding is never created for the operator"
        );
    }
}

#[test]
fn unbound_a_409_with_a_different_code_is_not_rewritten_into_the_binding_hint() {
    let server = api(
        None,
        Patch::Refuse(409, r#"{"detail":{"code":"something_else"}}"#),
    );
    let run = run(&tier_args(
        "local",
        &server.base_url,
        &["--slack-channel", CHANNEL],
    ));
    assert_ne!(run.code, 0, "{}", run.output());
    assert!(
        !run.output().contains("--add slack="),
        "an unrelated 409 must not claim the channel is unbound: {}",
        run.output()
    );
    assert!(run.output().contains("409"), "{}", run.output());
}

#[test]
fn unbound_a_409_naming_the_code_only_in_prose_is_not_rewritten() {
    let server = api(
        None,
        Patch::Refuse(
            409,
            r#"{"detail":"factory_slack_channel_unbound is mentioned but is not the code"}"#,
        ),
    );
    let run = run(&tier_args(
        "local",
        &server.base_url,
        &["--slack-channel", CHANNEL],
    ));
    assert_ne!(run.code, 0, "{}", run.output());
    assert!(
        !run.output().contains("--add slack="),
        "the code is read from the detail object, not matched in text: {}",
        run.output()
    );
}

#[test]
fn status_renders_a_set_channel_and_writes_nothing() {
    for tier in TIERS {
        let server = api(Some(CHANNEL), Patch::Store);
        let human = run(&tier_args(tier, &server.base_url, &[]));
        assert_eq!(human.code, 0, "{tier}: {}", human.output());
        assert!(
            human.stdout.contains(&format!(
                "factory progress channel for {AGENT_NAME}: {CHANNEL}"
            )),
            "{tier}: {}",
            human.output()
        );
        let json = run(&tier_args(tier, &server.base_url, &["--json"]));
        assert_eq!(json.code, 0, "{tier}: {}", json.output());
        let value = json.json();
        assert_valid(&cli_schema(), &value, "the --json payload");
        assert_eq!(
            value,
            serde_json::json!({"agent": AGENT_NAME, "factory_slack_channel": CHANNEL, "changed": false}),
            "{tier}"
        );
        assert!(
            patches(&server).is_empty(),
            "{tier}: an inspect never writes"
        );
    }
}

#[test]
fn status_renders_none_when_the_channel_is_null_or_absent() {
    // Null is what the API sends. Absent is a platform older than the field.
    let null_server = api(None, Patch::Store);
    let human = run(&tier_args("local", &null_server.base_url, &[]));
    assert_eq!(human.code, 0, "{}", human.output());
    assert!(
        human.stdout.contains(&format!(
            "factory progress channel for {AGENT_NAME}: none (factory runs post no Slack progress)"
        )),
        "{}",
        human.output()
    );

    let absent = serve(|req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => {
            let mut agent = agent_value(None);
            agent
                .as_object_mut()
                .unwrap()
                .remove("factory_slack_channel");
            Response::json(200, &serde_json::Value::Array(vec![agent]).to_string())
        }
        _ => Response::json(405, r#"{"detail":"unexpected request"}"#),
    });
    let json = run(&tier_args("local", &absent.base_url, &["--json"]));
    assert_eq!(json.code, 0, "{}", json.output());
    let value = json.json();
    assert_valid(&cli_schema(), &value, "the --json payload");
    assert!(value["factory_slack_channel"].is_null(), "{value}");
    assert_eq!(value["changed"], false);
}

#[test]
fn status_dry_run_is_a_read_plan() {
    let server = api(None, Patch::Store);
    let run = run(&tier_args(
        "local",
        &server.base_url,
        &["--dry-run", "--json"],
    ));
    assert_eq!(run.code, 0, "{}", run.output());
    let value = run.json();
    assert_valid(&cli_schema(), &value, "the dry-run payload");
    assert!(
        value["plan"][0]
            .as_str()
            .unwrap_or_default()
            .contains("GET"),
        "{value}"
    );
    assert!(server.recorded().is_empty());
}

#[test]
fn schema_the_real_patch_body_validates_against_the_api_agent_update() {
    let update = api_component("AgentUpdate");
    let openapi: serde_json::Value =
        serde_json::from_str(include_str!("../../apps/api/openapi.json")).expect("openapi parses");
    for tier in TIERS {
        let set_server = api(None, Patch::Store);
        let set = run(&tier_args(
            tier,
            &set_server.base_url,
            &["--slack-channel", CHANNEL],
        ));
        assert_eq!(set.code, 0, "{tier}: {}", set.output());
        let clear_server = api(Some(CHANNEL), Patch::Store);
        let clear = run(&tier_args(
            tier,
            &clear_server.base_url,
            &["--no-slack-channel"],
        ));
        assert_eq!(clear.code, 0, "{tier}: {}", clear.output());

        for (what, server) in [("set", &set_server), ("clear", &clear_server)] {
            let sent = patches(server);
            assert_eq!(sent.len(), 1, "{tier} {what}");
            assert_valid(&update, &sent[0], &format!("the {tier} {what} PATCH body"));
            // The route schema tolerates unknown keys, so a validator alone
            // would accept a body whose key drifted. Pin the key to a property
            // the real schema declares, and to the only key this verb sends.
            let declared = openapi["components"]["schemas"]["AgentUpdate"]["properties"]
                .as_object()
                .expect("AgentUpdate declares properties");
            let keys: Vec<&String> = sent[0].as_object().expect("an object").keys().collect();
            assert_eq!(keys, ["factory_slack_channel"], "{tier} {what}");
            assert!(declared.contains_key(keys[0].as_str()), "{tier} {what}");
        }
    }
}

#[test]
fn schema_the_api_marks_the_field_nullable_with_the_slack_id_pattern() {
    let openapi: serde_json::Value =
        serde_json::from_str(include_str!("../../apps/api/openapi.json")).expect("openapi parses");
    let field =
        &openapi["components"]["schemas"]["AgentUpdate"]["properties"]["factory_slack_channel"];
    let variants = field["anyOf"]
        .as_array()
        .expect("an anyOf of string and null");
    assert!(
        variants.iter().any(|v| v["type"] == "null"),
        "the API accepts null (clear): {field}"
    );
    let string = variants
        .iter()
        .find(|v| v["type"] == "string")
        .expect("a string variant");
    assert_eq!(
        string["pattern"], "^[CG][A-Z0-9]{6,}$",
        "the CLI's client-side check must mirror the API pattern: {field}"
    );
    // The same body the binary sends for each shape is accepted or refused by
    // the real route schema the way the CLI's own validation assumes.
    let update = api_component("AgentUpdate");
    assert!(update.is_valid(&serde_json::json!({"factory_slack_channel": CHANNEL})));
    assert!(update.is_valid(&serde_json::json!({"factory_slack_channel": null})));
    assert!(!update.is_valid(&serde_json::json!({"factory_slack_channel": "c0example1"})));
}

#[test]
fn schema_the_mock_agent_answers_match_the_api_agent_out() {
    let out = api_component("AgentOut");
    assert_valid(&out, &agent_value(Some(CHANNEL)), "an agent with a channel");
    assert_valid(&out, &agent_value(None), "an agent without a channel");
}

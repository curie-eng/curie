//! Integration: the agent-lifecycle verbs (`cluster kill|resume|budget|delete|
//! reset-thread`, #149, #737) against the committed platform-API contract
//! shapes (apps/api openapi.json), served by the wire-level test server.
//! Covers both the `ApiClient` methods (correct HTTP method + path + body) and
//! the command handlers (`--yes` gate on the destructive verbs, `--dry-run`
//! makes no request).

mod support;

use curie::api::{ApiClient, BudgetConfig};
use curie::commands::{self, AgentActionOpts};
use support::{serve, Response};

const AGENT_ID: &str = "11111111-1111-1111-1111-111111111111";

/// A one-agent `GET /agents` list used by the handler-level resolution tests.
fn agent_list() -> Response {
    Response::json(
        200,
        &format!(
            r##"[{{"id":"{AGENT_ID}","name":"deal-desk","channels":[{{"kind":"slack","address":"#x"}}],"created_at":"2026-07-05T00:00:00Z"}}]"##
        ),
    )
}

fn opts(base_url: &str, agent: &str, dry_run: bool) -> AgentActionOpts {
    AgentActionOpts {
        api_url: base_url.to_string(),
        api_key: "k".to_string(),
        agent: agent.to_string(),
        dry_run,
    }
}

// --- ApiClient methods: correct verb + path + body ------------------------

#[tokio::test]
async fn kill_agent_posts_to_kill_endpoint_with_empty_body() {
    let server = serve(|req| match (req.method.as_str(), req.path.as_str()) {
        ("POST", p) if *p == format!("/agents/{AGENT_ID}/kill") => {
            Response::json(200, r#"{"killed":true}"#)
        }
        other => panic!("unexpected request: {other:?}"),
    });
    let client = ApiClient::new(&server.base_url, "k").unwrap();
    let state = client.kill_agent(AGENT_ID).await.unwrap();
    assert!(state.killed);

    let rec = server.recorded();
    assert_eq!(rec.len(), 1);
    assert_eq!(rec[0].method, "POST");
    assert_eq!(rec[0].path, format!("/agents/{AGENT_ID}/kill"));
    assert!(rec[0].body.is_empty(), "kill sends no body");
    assert_eq!(rec[0].header("x-api-key"), Some("k"));
}

#[tokio::test]
async fn resume_agent_posts_to_resume_endpoint() {
    let server = serve(|req| match (req.method.as_str(), req.path.as_str()) {
        ("POST", p) if *p == format!("/agents/{AGENT_ID}/resume") => {
            Response::json(200, r#"{"killed":false}"#)
        }
        other => panic!("unexpected request: {other:?}"),
    });
    let client = ApiClient::new(&server.base_url, "k").unwrap();
    let state = client.resume_agent(AGENT_ID).await.unwrap();
    assert!(!state.killed);

    let rec = server.recorded();
    assert_eq!(rec[0].method, "POST");
    assert_eq!(rec[0].path, format!("/agents/{AGENT_ID}/resume"));
}

#[tokio::test]
async fn set_budget_puts_the_limit_as_max_usd_per_day() {
    let server = serve(|req| match (req.method.as_str(), req.path.as_str()) {
        ("PUT", p) if *p == format!("/agents/{AGENT_ID}/budget") => Response::json(
            200,
            r#"{"max_output_tokens_per_run":null,"max_usd_per_day":7.5}"#,
        ),
        other => panic!("unexpected request: {other:?}"),
    });
    let client = ApiClient::new(&server.base_url, "k").unwrap();
    let cfg = BudgetConfig {
        max_output_tokens_per_run: None,
        max_usd_per_day: Some(7.5),
    };
    let saved = client.set_budget(AGENT_ID, &cfg).await.unwrap();
    assert_eq!(saved.max_usd_per_day, Some(7.5));

    let rec = server.recorded();
    assert_eq!(rec[0].method, "PUT");
    assert_eq!(rec[0].path, format!("/agents/{AGENT_ID}/budget"));
    let body = String::from_utf8_lossy(&rec[0].body);
    assert!(body.contains("\"max_usd_per_day\":7.5"), "body: {body}");
    // The unset token cap is skipped, not sent as null, so the server keeps its
    // platform default.
    assert!(
        !body.contains("max_output_tokens_per_run"),
        "unset field must be omitted: {body}"
    );
}

#[tokio::test]
async fn reset_thread_posts_to_reset_endpoint_with_empty_body() {
    let server = serve(|req| match (req.method.as_str(), req.path.as_str()) {
        ("POST", p) if *p == format!("/agents/{AGENT_ID}/threads/t-1/reset") => {
            Response::json(200, r#"{"requested":true}"#)
        }
        other => panic!("unexpected request: {other:?}"),
    });
    let client = ApiClient::new(&server.base_url, "k").unwrap();
    let state = client.reset_thread(AGENT_ID, "t-1").await.unwrap();
    assert!(state.requested);

    let rec = server.recorded();
    assert_eq!(rec.len(), 1);
    assert_eq!(rec[0].method, "POST");
    assert_eq!(rec[0].path, format!("/agents/{AGENT_ID}/threads/t-1/reset"));
    assert!(rec[0].body.is_empty(), "reset sends no body");
    assert_eq!(rec[0].header("x-api-key"), Some("k"));
}

#[tokio::test]
async fn delete_agent_issues_a_delete() {
    let server = serve(|req| match (req.method.as_str(), req.path.as_str()) {
        ("DELETE", p) if *p == format!("/agents/{AGENT_ID}") => Response {
            status: 204,
            content_type: "application/json".into(),
            body: Vec::new(),
        },
        other => panic!("unexpected request: {other:?}"),
    });
    let client = ApiClient::new(&server.base_url, "k").unwrap();
    client.delete_agent(AGENT_ID).await.unwrap();

    let rec = server.recorded();
    assert_eq!(rec[0].method, "DELETE");
    assert_eq!(rec[0].path, format!("/agents/{AGENT_ID}"));
    assert_eq!(rec[0].header("x-api-key"), Some("k"));
}

#[tokio::test]
async fn end_deployment_issues_an_authenticated_delete_with_no_body() {
    let deployment_id = "deployment-active-dev";
    let server = serve(move |req| match (req.method.as_str(), req.path.as_str()) {
        ("DELETE", p) if *p == format!("/deployments/{deployment_id}") => Response {
            status: 204,
            content_type: "application/json".into(),
            body: Vec::new(),
        },
        other => panic!("unexpected request: {other:?}"),
    });
    let client = ApiClient::new(&server.base_url, "k").unwrap();
    client.end_deployment(deployment_id).await.unwrap();

    let rec = server.recorded();
    assert_eq!(rec.len(), 1);
    assert_eq!(rec[0].method, "DELETE");
    assert_eq!(rec[0].path, format!("/deployments/{deployment_id}"));
    assert!(rec[0].body.is_empty(), "ending a deployment sends no body");
    assert_eq!(rec[0].header("x-api-key"), Some("k"));
}

#[tokio::test]
async fn find_agent_errors_when_no_agent_matches() {
    let server = serve(|req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => Response::json(200, "[]"),
        other => panic!("unexpected request: {other:?}"),
    });
    let client = ApiClient::new(&server.base_url, "k").unwrap();
    let err = client.find_agent("nope").await.unwrap_err();
    assert!(err.to_string().contains("no agent found"), "{err}");
}

// --- Handlers: resolve-then-act, --yes gate, --dry-run --------------------

#[tokio::test]
async fn kill_handler_resolves_by_name_then_kills() {
    let server = serve(|req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => agent_list(),
        ("POST", p) if *p == format!("/agents/{AGENT_ID}/kill") => {
            Response::json(200, r#"{"killed":true}"#)
        }
        other => panic!("unexpected request: {other:?}"),
    });
    commands::kill(opts(&server.base_url, "deal-desk", false), true)
        .await
        .unwrap();

    let flow: Vec<(String, String)> = server
        .recorded()
        .iter()
        .map(|r| (r.method.clone(), r.path.clone()))
        .collect();
    assert_eq!(
        flow,
        vec![
            ("GET".to_string(), "/agents".to_string()),
            ("POST".to_string(), format!("/agents/{AGENT_ID}/kill")),
        ]
    );
}

#[tokio::test]
async fn budget_handler_resolves_then_puts_the_limit() {
    let server = serve(|req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => agent_list(),
        ("PUT", p) if *p == format!("/agents/{AGENT_ID}/budget") => Response::json(
            200,
            r#"{"max_output_tokens_per_run":null,"max_usd_per_day":9.0}"#,
        ),
        other => panic!("unexpected request: {other:?}"),
    });
    commands::budget(opts(&server.base_url, "deal-desk", false), 9.0)
        .await
        .unwrap();

    let rec = server.recorded();
    assert_eq!(rec.len(), 2);
    assert_eq!(rec[1].method, "PUT");
    assert_eq!(rec[1].path, format!("/agents/{AGENT_ID}/budget"));
    let body = String::from_utf8_lossy(&rec[1].body);
    assert!(body.contains("\"max_usd_per_day\":9.0"), "body: {body}");
}

#[tokio::test]
async fn reset_thread_handler_resolves_then_resets_and_waits_for_release() {
    let server = serve(|req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => agent_list(),
        ("POST", p) if *p == format!("/agents/{AGENT_ID}/threads/t-1/reset") => {
            Response::json(200, r#"{"requested":true}"#)
        }
        // #735: after the POST the handler polls this until the worker reports
        // the release actually landed; the stub reports released on first poll.
        ("GET", p) if *p == format!("/agents/{AGENT_ID}/threads/t-1/reset") => {
            Response::json(200, r#"{"requested":false}"#)
        }
        other => panic!("unexpected request: {other:?}"),
    });
    commands::reset_thread(
        opts(&server.base_url, "deal-desk", false),
        "t-1".to_string(),
        true,
    )
    .await
    .unwrap();

    let flow: Vec<(String, String)> = server
        .recorded()
        .iter()
        .map(|r| (r.method.clone(), r.path.clone()))
        .collect();
    assert_eq!(
        flow,
        vec![
            ("GET".to_string(), "/agents".to_string()),
            (
                "POST".to_string(),
                format!("/agents/{AGENT_ID}/threads/t-1/reset")
            ),
            (
                "GET".to_string(),
                format!("/agents/{AGENT_ID}/threads/t-1/reset")
            ),
        ]
    );
}

#[tokio::test]
async fn reset_thread_without_yes_refuses_and_makes_no_request() {
    let server = serve(|req| panic!("no request expected, got {} {}", req.method, req.path));
    let err = commands::reset_thread(
        opts(&server.base_url, "deal-desk", false),
        "t-1".to_string(),
        false,
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("--yes"), "{err}");
    assert!(
        server.recorded().is_empty(),
        "a refused reset-thread must make no request"
    );
}

#[tokio::test]
async fn kill_without_yes_refuses_and_makes_no_request() {
    let server = serve(|req| panic!("no request expected, got {} {}", req.method, req.path));
    let err = commands::kill(opts(&server.base_url, "deal-desk", false), false)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("--yes"), "{err}");
    assert!(
        server.recorded().is_empty(),
        "a refused kill must make no request"
    );
}

#[tokio::test]
async fn delete_without_yes_refuses_and_makes_no_request() {
    let server = serve(|req| panic!("no request expected, got {} {}", req.method, req.path));
    let err = commands::delete(opts(&server.base_url, "deal-desk", false), false)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("--yes"), "{err}");
    assert!(
        server.recorded().is_empty(),
        "a refused delete must make no request"
    );
}

#[tokio::test]
async fn delete_handler_ends_every_active_deployment_then_deletes_agent() {
    let active_dev = "deployment-active-dev";
    let stopped = "deployment-stopped";
    let active_prod = "deployment-active-prod";
    let server = serve(move |req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => agent_list(),
        ("GET", p) if *p == format!("/deployments?agent_id={AGENT_ID}") => Response::json(
            200,
            &format!(
                r#"[{{"id":"{active_dev}","environment":"dev","status":"active"}},{{"id":"{stopped}","environment":"staging","status":"stopped"}},{{"id":"{active_prod}","environment":"prod","status":"active"}}]"#
            ),
        ),
        ("DELETE", p)
            if *p == format!("/deployments/{active_dev}")
                || *p == format!("/deployments/{active_prod}") =>
        {
            Response {
                status: 204,
                content_type: "application/json".into(),
                body: Vec::new(),
            }
        }
        ("DELETE", p) if *p == format!("/agents/{AGENT_ID}") => Response {
            status: 204,
            content_type: "application/json".into(),
            body: Vec::new(),
        },
        other => panic!("unexpected request: {other:?}"),
    });

    let out = commands::delete(opts(&server.base_url, "deal-desk", false), true)
        .await
        .unwrap();
    assert!(matches!(out, commands::DeleteOutput::Done { .. }));

    let rec = server.recorded();
    let flow: Vec<(String, String)> = rec
        .iter()
        .map(|request| (request.method.clone(), request.path.clone()))
        .collect();
    assert_eq!(
        flow,
        vec![
            ("GET".to_string(), "/agents".to_string()),
            (
                "GET".to_string(),
                format!("/deployments?agent_id={AGENT_ID}")
            ),
            ("DELETE".to_string(), format!("/deployments/{active_dev}")),
            ("DELETE".to_string(), format!("/deployments/{active_prod}")),
            ("DELETE".to_string(), format!("/agents/{AGENT_ID}")),
        ]
    );
    assert!(
        rec.iter()
            .all(|request| request.header("x-api-key") == Some("k")),
        "every lifecycle request must be authenticated"
    );
}

#[tokio::test]
async fn delete_handler_stops_when_ending_an_active_deployment_fails() {
    let first = "deployment-active-first";
    let failing = "deployment-active-failing";
    let later = "deployment-active-later";
    let server = serve(move |req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => agent_list(),
        ("GET", p) if *p == format!("/deployments?agent_id={AGENT_ID}") => Response::json(
            200,
            &format!(
                r#"[{{"id":"{first}","environment":"dev","status":"active"}},{{"id":"{failing}","environment":"staging","status":"active"}},{{"id":"{later}","environment":"prod","status":"active"}}]"#
            ),
        ),
        ("DELETE", p) if *p == format!("/deployments/{first}") => Response {
            status: 204,
            content_type: "application/json".into(),
            body: Vec::new(),
        },
        ("DELETE", p) if *p == format!("/deployments/{failing}") => {
            Response::json(500, r#"{"detail":"deployment teardown failed"}"#)
        }
        other => panic!("unexpected request: {other:?}"),
    });

    let err = commands::delete(opts(&server.base_url, "deal-desk", false), true)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("500 Internal Server Error"),
        "{err}"
    );

    let flow: Vec<(String, String)> = server
        .recorded()
        .iter()
        .map(|request| (request.method.clone(), request.path.clone()))
        .collect();
    assert_eq!(
        flow,
        vec![
            ("GET".to_string(), "/agents".to_string()),
            (
                "GET".to_string(),
                format!("/deployments?agent_id={AGENT_ID}")
            ),
            ("DELETE".to_string(), format!("/deployments/{first}")),
            ("DELETE".to_string(), format!("/deployments/{failing}")),
        ],
        "a failed deployment end must stop all later deletion requests"
    );
}

#[tokio::test]
async fn delete_handler_propagates_a_concurrent_deployment_conflict_without_retry() {
    let server = serve(|req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => agent_list(),
        ("GET", p) if *p == format!("/deployments?agent_id={AGENT_ID}") => {
            Response::json(200, "[]")
        }
        ("DELETE", p) if *p == format!("/agents/{AGENT_ID}") => {
            Response::json(409, r#"{"detail":"active deployment exists"}"#)
        }
        other => panic!("unexpected request: {other:?}"),
    });

    let err = commands::delete(opts(&server.base_url, "deal-desk", false), true)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("409 Conflict"), "{err}");

    let flow: Vec<(String, String)> = server
        .recorded()
        .iter()
        .map(|request| (request.method.clone(), request.path.clone()))
        .collect();
    assert_eq!(
        flow,
        vec![
            ("GET".to_string(), "/agents".to_string()),
            (
                "GET".to_string(),
                format!("/deployments?agent_id={AGENT_ID}")
            ),
            ("DELETE".to_string(), format!("/agents/{AGENT_ID}")),
        ],
        "the final conflict must not be retried or swallowed"
    );
}

#[tokio::test]
async fn delete_dry_run_returns_the_generic_lifecycle_plan_without_requests() {
    let server = serve(|req| panic!("dry run must not request, got {} {}", req.method, req.path));

    let out = commands::delete(opts(&server.base_url, "deal-desk", true), false)
        .await
        .unwrap();
    match out {
        commands::DeleteOutput::DryRun(plan) => assert_eq!(
            plan.lines,
            vec![
                format!(
                    "GET {}/agents  (would resolve agent {:?})",
                    server.base_url, "deal-desk"
                ),
                format!("GET {}/deployments?agent_id=<id>", server.base_url),
                format!(
                    "DELETE {}/deployments/<id>  (for each active deployment)",
                    server.base_url
                ),
                format!("DELETE {}/agents/<id>", server.base_url),
            ]
        ),
        other => panic!("expected dry run output, got {other:?}"),
    }
    assert!(
        server.recorded().is_empty(),
        "delete dry run must make no request"
    );
}

#[tokio::test]
async fn dry_run_makes_no_request_for_any_verb() {
    // Even a destructive verb under --dry-run (without --yes) touches nothing.
    let server = serve(|req| panic!("dry-run must not request, got {} {}", req.method, req.path));
    let base = &server.base_url;
    commands::kill(opts(base, "a", true), false).await.unwrap();
    commands::resume(opts(base, "a", true)).await.unwrap();
    commands::budget(opts(base, "a", true), 5.0).await.unwrap();
    commands::delete(opts(base, "a", true), false)
        .await
        .unwrap();
    commands::reset_thread(opts(base, "a", true), "t-1".to_string(), false)
        .await
        .unwrap();
    assert!(
        server.recorded().is_empty(),
        "no dry-run verb may make a request"
    );
}

// --- `<tier> overrides`: the two nullable operator overrides (#1311) ---------
//
// Both fields were settable only by a raw authenticated PATCH before this verb,
// and both are three-way on the wire: OMITTED leaves the stored value, explicit
// JSON null clears it to the platform default, a string pins it. The whole point
// of the verb is that an operator can express all three, so these assert the
// BODY, not just that a request happened -- a body that sends null where it
// meant "leave it" is the failure mode, and it looks identical from the outside.

/// A one-agent list whose overrides are already pinned, for the inspect path.
fn agent_list_with_overrides() -> Response {
    Response::json(
        200,
        &format!(
            r##"[{{"id":"{AGENT_ID}","name":"deal-desk","channels":[{{"kind":"slack","address":"#x"}}],"model":"kimi-k2","thinking":"adaptive","created_at":"2026-07-05T00:00:00Z"}}]"##
        ),
    )
}

#[tokio::test]
async fn overrides_inspect_reads_both_fields_and_writes_nothing() {
    let server = serve(|req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => agent_list_with_overrides(),
        other => panic!("unexpected request: {other:?}"),
    });

    let out = commands::overrides(
        opts(&server.base_url, "deal-desk", false),
        commands::OverrideChange::Unchanged,
        commands::OverrideChange::Unchanged,
    )
    .await
    .unwrap();

    match out {
        commands::OverridesOutput::Done {
            agent,
            model,
            thinking,
            changed,
        } => {
            assert_eq!(agent, "deal-desk");
            assert_eq!(model.as_deref(), Some("kimi-k2"));
            assert_eq!(thinking.as_deref(), Some("adaptive"));
            assert!(!changed, "an inspect must not report itself as a write");
        }
        other => panic!("expected Done, got {other:?}"),
    }

    // The resolve GET and nothing else: inspect is read-only.
    let rec = server.recorded();
    assert_eq!(rec.len(), 1, "inspect must issue exactly one request");
    assert_eq!(rec[0].method, "GET");
}

#[tokio::test]
async fn overrides_set_patches_only_the_field_named() {
    let server = serve(|req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => agent_list(),
        ("PATCH", p) if *p == format!("/agents/{AGENT_ID}") => Response::json(
            200,
            &format!(
                r##"{{"id":"{AGENT_ID}","name":"deal-desk","channels":[{{"kind":"slack","address":"#x"}}],"model":null,"thinking":"enabled:2000"}}"##
            ),
        ),
        other => panic!("unexpected request: {other:?}"),
    });

    let out = commands::overrides(
        opts(&server.base_url, "deal-desk", false),
        commands::OverrideChange::Unchanged,
        commands::OverrideChange::Set("enabled:2000".to_string()),
    )
    .await
    .unwrap();

    match out {
        commands::OverridesOutput::Done {
            thinking, changed, ..
        } => {
            assert_eq!(thinking.as_deref(), Some("enabled:2000"));
            assert!(changed);
        }
        other => panic!("expected Done, got {other:?}"),
    }

    let rec = server.recorded();
    let patch = rec.iter().find(|r| r.method == "PATCH").expect("a PATCH");
    let body = String::from_utf8_lossy(&patch.body);
    assert!(
        body.contains(r#""thinking":"enabled:2000""#),
        "body: {body}"
    );
    // The load-bearing half: an unmentioned field is ABSENT, not null. The API
    // tells omitted from explicit-null apart with `model_fields_set` (#1310), so
    // sending null here would silently clear an override the operator never
    // touched.
    assert!(
        !body.contains("model"),
        "an unmentioned override must be omitted, not nulled: {body}"
    );
}

#[tokio::test]
async fn overrides_clear_sends_explicit_null_not_an_empty_string() {
    let server = serve(|req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => agent_list_with_overrides(),
        ("PATCH", p) if *p == format!("/agents/{AGENT_ID}") => Response::json(
            200,
            &format!(
                r##"{{"id":"{AGENT_ID}","name":"deal-desk","channels":[{{"kind":"slack","address":"#x"}}],"model":null,"thinking":null}}"##
            ),
        ),
        other => panic!("unexpected request: {other:?}"),
    });

    let out = commands::overrides(
        opts(&server.base_url, "deal-desk", false),
        commands::OverrideChange::Clear,
        commands::OverrideChange::Clear,
    )
    .await
    .unwrap();

    match out {
        commands::OverridesOutput::Done {
            model,
            thinking,
            changed,
            ..
        } => {
            assert!(model.is_none() && thinking.is_none());
            assert!(changed);
        }
        other => panic!("expected Done, got {other:?}"),
    }

    let patch = server
        .recorded()
        .into_iter()
        .find(|r| r.method == "PATCH")
        .expect("a PATCH");
    let body = String::from_utf8_lossy(&patch.body);
    assert!(body.contains(r#""model":null"#), "body: {body}");
    assert!(body.contains(r#""thinking":null"#), "body: {body}");
    // Never the empty string. The API refuses it (#1355) and it would be the
    // wrong request anyway: an empty override reaches the worker falsy, emits no
    // boot key, and skips the very platform default clearing restores.
    assert!(
        !body.contains(r#""""#),
        "clear must be null, not empty: {body}"
    );
}

#[tokio::test]
async fn overrides_dry_run_makes_no_request_on_either_path() {
    let server = serve(|req| panic!("--dry-run must not call the API: {req:?}"));

    for (model, thinking) in [
        (
            commands::OverrideChange::Unchanged,
            commands::OverrideChange::Unchanged,
        ),
        (
            commands::OverrideChange::Clear,
            commands::OverrideChange::Set("adaptive".to_string()),
        ),
    ] {
        let out = commands::overrides(opts(&server.base_url, "deal-desk", true), model, thinking)
            .await
            .unwrap();
        assert!(matches!(out, commands::OverridesOutput::DryRun(_)));
    }
    assert!(server.recorded().is_empty());
}

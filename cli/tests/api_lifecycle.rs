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
            r##"[{{"id":"{AGENT_ID}","name":"deal-desk","channels":[{{"kind":"slack","address":"#x"}}],"created_at":"2026-07-05T00:00:00Z","memory":false}}]"##
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
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&rec[0].body).unwrap(),
        serde_json::json!({
            "max_usd_per_day": 7.5,
            "max_output_tokens_per_run": null,
        }),
        "PUT sends the complete budget including platform defaults"
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
async fn thread_reset_state_decodes_whether_the_reset_matched_a_route() {
    // #3699: the API reports `route_existed` only once the reset is no longer
    // pending. A worker or API that predates the field omits it.
    for (body, expected) in [
        (r#"{"requested":false,"route_existed":false}"#, Some(false)),
        (r#"{"requested":false,"route_existed":true}"#, Some(true)),
        (r#"{"requested":false,"route_existed":null}"#, None),
        (r#"{"requested":false}"#, None),
    ] {
        let server = serve(move |req| match (req.method.as_str(), req.path.as_str()) {
            ("GET", p) if *p == format!("/agents/{AGENT_ID}/threads/t-1/reset") => {
                Response::json(200, body)
            }
            other => panic!("unexpected request: {other:?}"),
        });
        let client = ApiClient::new(&server.base_url, "k").unwrap();
        let state = client.thread_reset_state(AGENT_ID, "t-1").await.unwrap();
        assert!(!state.requested, "{body}");
        assert_eq!(state.route_existed, expected, "{body}");
    }
}

/// Percent-decode a recorded wire path segment the way the platform API's
/// router decodes it (Starlette unquotes path params), so a test can assert
/// on what the API actually received.
fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && bytes[i + 1].is_ascii_hexdigit()
            && bytes[i + 2].is_ascii_hexdigit()
        {
            let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap();
            out.push(u8::from_str_radix(hex, 16).unwrap());
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).unwrap()
}

/// The thread-key segment of a recorded reset request path: whatever sits
/// between `/threads/` and `/reset`. Panics if the key did not land in that
/// position, or if it was sent as more than one path segment.
fn recorded_thread_key_segment(path: &str, agent_id: &str) -> String {
    let prefix = format!("/agents/{agent_id}/threads/");
    let segment = path
        .strip_prefix(&prefix)
        .and_then(|rest| rest.strip_suffix("/reset"))
        .unwrap_or_else(|| panic!("thread key must sit between /threads/ and /reset: {path}"));
    assert!(
        !segment.contains('/'),
        "thread key must be one path segment, sent: {path}"
    );
    segment.to_string()
}

/// #3727: stored thread keys already carry `%XX` escapes
/// (`scoped_conversation_id` percent-encodes every component), and the API
/// decodes escapes when it reads `thread_key`. So the POST must re-escape
/// the key (`%2F` on the wire as `%252F`) or a GitHub thread key is decoded
/// into a slash and matches no route, and a mail key decodes into an `@`
/// and names a thread other than the one the operator asked for.
#[tokio::test]
async fn reset_thread_encodes_the_thread_key_as_one_re_escaped_segment() {
    for (key, wire) in [
        (
            "github:curie-eng%2Fcurie:3698",
            "github:curie-eng%252Fcurie:3698",
        ),
        (
            "email:ops%40example.com:abc",
            "email:ops%2540example.com:abc",
        ),
    ] {
        let server = serve(move |req| match (req.method.as_str(), req.path.as_str()) {
            ("POST", _) => Response::json(200, r#"{"requested":true}"#),
            other => panic!("unexpected request: {other:?}"),
        });
        let client = ApiClient::new(&server.base_url, "k").unwrap();
        let state = client.reset_thread(AGENT_ID, key).await.unwrap();
        assert!(state.requested, "{key}");

        let rec = server.recorded();
        assert_eq!(rec.len(), 1, "{key}");
        assert_eq!(rec[0].method, "POST", "{key}");
        let segment = recorded_thread_key_segment(&rec[0].path, AGENT_ID);
        assert_eq!(segment, wire, "{key}: raw wire path: {}", rec[0].path);
        assert_eq!(percent_decode(&segment), key, "{key}");
    }
}

/// The GET poll of the same route must re-escape the key exactly like the
/// POST (#3727): both verbs target the same stored key, so both must send
/// it as one percent-encoded path segment.
#[tokio::test]
async fn thread_reset_state_encodes_the_thread_key_as_one_re_escaped_segment() {
    for (key, wire) in [
        (
            "github:curie-eng%2Fcurie:3698",
            "github:curie-eng%252Fcurie:3698",
        ),
        (
            "email:ops%40example.com:abc",
            "email:ops%2540example.com:abc",
        ),
    ] {
        let server = serve(move |req| match (req.method.as_str(), req.path.as_str()) {
            ("GET", _) => Response::json(200, r#"{"requested":false,"route_existed":true}"#),
            other => panic!("unexpected request: {other:?}"),
        });
        let client = ApiClient::new(&server.base_url, "k").unwrap();
        let state = client.thread_reset_state(AGENT_ID, key).await.unwrap();
        assert!(!state.requested, "{key}");

        let rec = server.recorded();
        assert_eq!(rec.len(), 1, "{key}");
        assert_eq!(rec[0].method, "GET", "{key}");
        let segment = recorded_thread_key_segment(&rec[0].path, AGENT_ID);
        assert_eq!(segment, wire, "{key}: raw wire path: {}", rec[0].path);
        assert_eq!(percent_decode(&segment), key, "{key}");
    }
}

/// #3727: a plain Slack key keeps today's wire form byte-for-byte. Only
/// characters that would decode into a different string or split the path
/// get escaped, so `:`, `.` and the digits pass through untouched and the
/// mock server sees the exact path the pre-fix `format!` produced.
#[tokio::test]
async fn reset_thread_sends_a_plain_slack_key_exactly_as_today() {
    let key = "slack:C0EXAMPLE1:1700000000.000100";
    let server = serve(move |req| match (req.method.as_str(), req.path.as_str()) {
        ("POST", p) if *p == format!("/agents/{AGENT_ID}/threads/{key}/reset") => {
            Response::json(200, r#"{"requested":true}"#)
        }
        other => panic!("unexpected request: {other:?}"),
    });
    let client = ApiClient::new(&server.base_url, "k").unwrap();
    let state = client.reset_thread(AGENT_ID, key).await.unwrap();
    assert!(state.requested);
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
async fn budget_handler_reads_then_merges_the_selected_fields() {
    for (current, limit, output_tokens, expected) in [
        (
            serde_json::json!({"max_output_tokens_per_run": 32000, "max_usd_per_day": 5.0}),
            Some(9.0),
            None,
            serde_json::json!({"max_output_tokens_per_run": 32000, "max_usd_per_day": 9.0}),
        ),
        (
            serde_json::json!({"max_output_tokens_per_run": null, "max_usd_per_day": 5.0}),
            Some(9.0),
            None,
            serde_json::json!({"max_output_tokens_per_run": null, "max_usd_per_day": 9.0}),
        ),
        (
            serde_json::json!({"max_output_tokens_per_run": 64000, "max_usd_per_day": 6.5}),
            None,
            Some(96000),
            serde_json::json!({"max_output_tokens_per_run": 96000, "max_usd_per_day": 6.5}),
        ),
        (
            serde_json::json!({"max_output_tokens_per_run": 64000, "max_usd_per_day": null}),
            None,
            Some(96000),
            serde_json::json!({"max_output_tokens_per_run": 96000, "max_usd_per_day": null}),
        ),
        (
            serde_json::json!({"max_output_tokens_per_run": 64000, "max_usd_per_day": 6.5}),
            Some(9.0),
            Some(96000),
            serde_json::json!({"max_output_tokens_per_run": 96000, "max_usd_per_day": 9.0}),
        ),
    ] {
        let current_body = current.to_string();
        let expected_body = expected.to_string();
        let server = serve(move |req| match (req.method.as_str(), req.path.as_str()) {
            ("GET", "/agents") => agent_list(),
            ("GET", p) if *p == format!("/agents/{AGENT_ID}/budget") => {
                Response::json(200, &current_body)
            }
            ("PUT", p) if *p == format!("/agents/{AGENT_ID}/budget") => {
                Response::json(200, &expected_body)
            }
            other => panic!("unexpected request: {other:?}"),
        });
        let saved = commands::budget(
            opts(&server.base_url, "deal-desk", false),
            limit,
            output_tokens,
        )
        .await
        .unwrap();

        let rec = server.recorded();
        assert_eq!(rec.len(), 3, "{current} -> {expected}");
        assert_eq!(rec[0].method, "GET");
        assert_eq!(rec[0].path, "/agents");
        assert_eq!(rec[1].method, "GET");
        assert_eq!(rec[1].path, format!("/agents/{AGENT_ID}/budget"));
        assert_eq!(rec[2].method, "PUT");
        assert_eq!(rec[2].path, format!("/agents/{AGENT_ID}/budget"));
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&rec[2].body).unwrap(),
            expected,
            "preserve the unselected field, including an explicit null"
        );
        assert_eq!(
            curie::ui::CliOutput::to_json(&saved),
            serde_json::json!({
                "agent": "deal-desk",
                "max_usd_per_day": expected["max_usd_per_day"],
                "max_output_tokens_per_run": expected["max_output_tokens_per_run"],
            })
        );
    }
}

#[tokio::test]
async fn budget_handler_does_not_put_after_the_current_budget_read_fails() {
    let server = serve(|req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => agent_list(),
        ("GET", p) if *p == format!("/agents/{AGENT_ID}/budget") => {
            Response::json(503, r#"{"detail":"budget unavailable"}"#)
        }
        other => panic!("unexpected request: {other:?}"),
    });

    let err = commands::budget(opts(&server.base_url, "deal-desk", false), Some(9.0), None)
        .await
        .unwrap_err();

    assert!(err.to_string().contains("503"), "{err}");
    let rec = server.recorded();
    assert_eq!(rec.len(), 2);
    assert!(rec.iter().all(|request| request.method == "GET"));
}

#[tokio::test]
async fn budget_dry_run_refuses_a_limit_the_real_command_refuses() {
    // #3710: a dry run shows what the real command would do, so it must not
    // report a plan for a --limit the real command would refuse. The limit
    // validation fires before the dry-run early return, so an invalid limit
    // never reaches the plan, dry run or not.
    let server = serve(|req| panic!("budget must not request, got {} {}", req.method, req.path));
    let base = &server.base_url;
    for (limit, shown) in [
        (-5.0, "-5"),
        (0.0, "0"),
        (f64::NAN, "NaN"),
        (f64::INFINITY, "inf"),
        (f64::NEG_INFINITY, "-inf"),
    ] {
        let err = commands::budget(opts(base, "deal-desk", true), Some(limit), None)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains(&format!(
                "--limit must be a finite value greater than 0 (got {shown})"
            )),
            "dry run with {shown}: {err}"
        );
        assert_eq!(
            curie::exit::classify(&err).0,
            curie::exit::ExitClass::Usage,
            "dry run with {shown} must exit 2 like the real command"
        );
    }
    // Without --dry-run the refusal is the same error and the same class.
    let err = commands::budget(opts(base, "deal-desk", false), Some(-5.0), None)
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("--limit must be a finite value greater than 0 (got -5)"),
        "{err}"
    );
    assert_eq!(curie::exit::classify(&err).0, curie::exit::ExitClass::Usage);
    assert!(
        server.recorded().is_empty(),
        "a refused limit must make no request, dry run or not"
    );
}

#[tokio::test]
async fn budget_requires_a_selected_positive_limit_before_any_http() {
    let server = serve(|req| panic!("budget must not request, got {} {}", req.method, req.path));
    for dry_run in [false, true] {
        for (limit, output_tokens) in [(None, None), (None, Some(0)), (Some(5.0), Some(0))] {
            let err = commands::budget(
                opts(&server.base_url, "deal-desk", dry_run),
                limit,
                output_tokens,
            )
            .await
            .unwrap_err();
            assert_eq!(curie::exit::classify(&err).0, curie::exit::ExitClass::Usage);
            assert!(err.to_string().contains("--output-tokens"), "{err}");
            if output_tokens.is_none() {
                assert!(err.to_string().contains("--limit"), "{err}");
            }
        }
    }
    assert!(server.recorded().is_empty());
}

#[tokio::test]
async fn budget_dry_run_plans_to_read_and_preserve_before_the_selected_updates() {
    let server = serve(|req| panic!("dry-run must not request, got {} {}", req.method, req.path));
    let base = &server.base_url;
    for (limit, output_tokens) in [
        (Some(5.0), None),
        (None, Some(64000)),
        (Some(5.0), Some(64000)),
    ] {
        let out = commands::budget(opts(base, "deal-desk", true), limit, output_tokens)
            .await
            .unwrap();
        match out {
            commands::BudgetOutput::DryRun(plan) => {
                assert_eq!(plan.lines.len(), 2, "{plan:?}");
                assert!(plan.lines[0].contains(&format!("GET {base}/agents/<id>/budget")));
                assert!(plan.lines[0].to_lowercase().contains("preserv"));
                assert!(plan.lines[1].contains(&format!("PUT {base}/agents/<id>/budget")));
                if limit.is_some() {
                    assert!(plan.lines[1].contains("max_usd_per_day"));
                    assert!(plan.lines[1].contains('5'));
                }
                if output_tokens.is_some() {
                    assert!(plan.lines[1].contains("max_output_tokens_per_run"));
                    assert!(plan.lines[1].contains("64000"));
                }
            }
            other => panic!("expected dry run plan, got {other:?}"),
        }
    }
    assert!(
        server.recorded().is_empty(),
        "budget dry run must make no request"
    );
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
async fn reset_thread_handler_fails_when_the_key_matched_no_route() {
    // #3699: the worker drained the reset but the key matched no route, so
    // nothing was released. The command must say so and exit non-zero instead of
    // printing "released".
    let server = serve(|req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => agent_list(),
        ("POST", p) if *p == format!("/agents/{AGENT_ID}/threads/slack:C0EXAMPLE1:t/reset") => {
            Response::json(200, r#"{"requested":true,"route_existed":null}"#)
        }
        ("GET", p) if *p == format!("/agents/{AGENT_ID}/threads/slack:C0EXAMPLE1:t/reset") => {
            Response::json(200, r#"{"requested":false,"route_existed":false}"#)
        }
        other => panic!("unexpected request: {other:?}"),
    });
    let err = commands::reset_thread(
        opts(&server.base_url, "deal-desk", false),
        "slack:C0EXAMPLE1:t".to_string(),
        true,
    )
    .await
    .unwrap_err();

    assert!(
        err.to_string()
            .contains("no route matched this thread key; nothing was released"),
        "{err}"
    );
    let (class, fix) = curie::exit::classify(&err);
    assert_ne!(class, curie::exit::ExitClass::Success);
    let fix = fix.expect("a no-route reset carries a fix hint");
    assert!(
        fix.contains("kind:identity:channel:conversation"),
        "the hint must name the identity segment a named bot's key carries: {fix}"
    );
}

#[tokio::test]
async fn reset_thread_handler_reports_the_route_that_was_released() {
    let server = serve(|req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => agent_list(),
        ("POST", p) if *p == format!("/agents/{AGENT_ID}/threads/t-1/reset") => {
            Response::json(200, r#"{"requested":true,"route_existed":null}"#)
        }
        ("GET", p) if *p == format!("/agents/{AGENT_ID}/threads/t-1/reset") => {
            Response::json(200, r#"{"requested":false,"route_existed":true}"#)
        }
        other => panic!("unexpected request: {other:?}"),
    });
    let out = commands::reset_thread(
        opts(&server.base_url, "deal-desk", false),
        "t-1".to_string(),
        true,
    )
    .await
    .unwrap();
    assert_eq!(
        curie::ui::CliOutput::to_json(&out),
        serde_json::json!({
            "agent": "deal-desk",
            "thread_key": "t-1",
            "requested": true,
            "released": true,
            "route_existed": true
        })
    );
}

#[tokio::test]
async fn reset_thread_handler_keeps_released_when_the_api_reports_no_outcome() {
    // An API that predates `route_existed` (or an expired result) reads as
    // unknown: today's "released" output, without the new key.
    let server = serve(|req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => agent_list(),
        ("POST", p) if *p == format!("/agents/{AGENT_ID}/threads/t-1/reset") => {
            Response::json(200, r#"{"requested":true}"#)
        }
        ("GET", p) if *p == format!("/agents/{AGENT_ID}/threads/t-1/reset") => {
            Response::json(200, r#"{"requested":false}"#)
        }
        other => panic!("unexpected request: {other:?}"),
    });
    let out = commands::reset_thread(
        opts(&server.base_url, "deal-desk", false),
        "t-1".to_string(),
        true,
    )
    .await
    .unwrap();
    assert_eq!(
        curie::ui::CliOutput::to_json(&out),
        serde_json::json!({
            "agent": "deal-desk",
            "thread_key": "t-1",
            "requested": true,
            "released": true
        })
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
    commands::budget(opts(base, "a", true), Some(5.0), None)
        .await
        .unwrap();
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
            r##"[{{"id":"{AGENT_ID}","name":"deal-desk","channels":[{{"kind":"slack","address":"#x"}}],"model":"kimi-k2","thinking":"adaptive","created_at":"2026-07-05T00:00:00Z","memory":false,"memory_writes":true}}]"##
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
        commands::OverrideChange::Unchanged,
        commands::OverrideChange::Unchanged,
        commands::OverrideChange::Unchanged,
    )
    .await
    .unwrap();

    match out {
        commands::OverridesOutput::Done {
            agent,
            model,
            reviewer_model: _,
            thinking,
            execution_deadline_seconds: _,
            runner_resources: _,
            memory_writes,
            changed,
        } => {
            assert_eq!(agent, "deal-desk");
            assert_eq!(model.as_deref(), Some("kimi-k2"));
            assert_eq!(thinking.as_deref(), Some("adaptive"));
            // #1461: the inspect reports the memory-writes switch as stored.
            assert!(
                memory_writes,
                "inspect must report memory_writes as the API stored it"
            );
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
                r##"{{"id":"{AGENT_ID}","name":"deal-desk","channels":[{{"kind":"slack","address":"#x"}}],"model":null,"thinking":"enabled:2000","memory":false}}"##
            ),
        ),
        other => panic!("unexpected request: {other:?}"),
    });

    let out = commands::overrides(
        opts(&server.base_url, "deal-desk", false),
        commands::OverrideChange::Unchanged,
        commands::OverrideChange::Unchanged,
        commands::OverrideChange::Set("enabled:2000".to_string()),
        commands::OverrideChange::Unchanged,
        commands::OverrideChange::Unchanged,
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
                r##"{{"id":"{AGENT_ID}","name":"deal-desk","channels":[{{"kind":"slack","address":"#x"}}],"model":null,"thinking":null,"memory":false}}"##
            ),
        ),
        other => panic!("unexpected request: {other:?}"),
    });

    let out = commands::overrides(
        opts(&server.base_url, "deal-desk", false),
        commands::OverrideChange::Clear,
        commands::OverrideChange::Unchanged,
        commands::OverrideChange::Clear,
        commands::OverrideChange::Unchanged,
        commands::OverrideChange::Unchanged,
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
        let out = commands::overrides(
            opts(&server.base_url, "deal-desk", true),
            model,
            commands::OverrideChange::Unchanged,
            thinking,
            commands::OverrideChange::Unchanged,
            commands::OverrideChange::Unchanged,
        )
        .await
        .unwrap();
        assert!(matches!(out, commands::OverridesOutput::DryRun(_)));
    }
    assert!(server.recorded().is_empty());
}

// --- `<tier> overrides`: execution deadline (issue #3071) -------------------
//
// Same three-way contract as model/thinking, plus the one thing those two
// don't have: the PATCH body carries `execution_deadline_seconds` as a JSON
// NUMBER, so a body assertion here has to check for an unquoted int, not a
// string, or it would pass even if the CLI sent `"120"`.

#[tokio::test]
async fn overrides_set_execution_deadline_patches_only_that_field() {
    let server = serve(|req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => agent_list(),
        ("PATCH", p) if *p == format!("/agents/{AGENT_ID}") => Response::json(
            200,
            &format!(
                r##"{{"id":"{AGENT_ID}","name":"deal-desk","channels":[{{"kind":"slack","address":"#x"}}],"model":null,"thinking":null,"execution_deadline_seconds":120,"memory":false}}"##
            ),
        ),
        other => panic!("unexpected request: {other:?}"),
    });

    let out = commands::overrides(
        opts(&server.base_url, "deal-desk", false),
        commands::OverrideChange::Unchanged,
        commands::OverrideChange::Unchanged,
        commands::OverrideChange::Unchanged,
        commands::OverrideChange::Set("120".to_string()),
        commands::OverrideChange::Unchanged,
    )
    .await
    .unwrap();

    assert!(matches!(
        out,
        commands::OverridesOutput::Done { changed: true, .. }
    ));

    let rec = server.recorded();
    let patch = rec.iter().find(|r| r.method == "PATCH").expect("a PATCH");
    let body = String::from_utf8_lossy(&patch.body);
    assert!(
        body.contains(r#""execution_deadline_seconds":120"#),
        "execution_deadline_seconds must be an unquoted JSON number: {body}"
    );
    assert!(
        !body.contains("model") && !body.contains("thinking"),
        "an unmentioned override must be omitted, not nulled: {body}"
    );
}

#[tokio::test]
async fn overrides_clear_execution_deadline_sends_explicit_null_not_an_empty_string() {
    let server = serve(|req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => agent_list_with_overrides(),
        ("PATCH", p) if *p == format!("/agents/{AGENT_ID}") => Response::json(
            200,
            &format!(
                r##"{{"id":"{AGENT_ID}","name":"deal-desk","channels":[{{"kind":"slack","address":"#x"}}],"model":"kimi-k2","thinking":"adaptive","execution_deadline_seconds":null,"memory":false}}"##
            ),
        ),
        other => panic!("unexpected request: {other:?}"),
    });

    let out = commands::overrides(
        opts(&server.base_url, "deal-desk", false),
        commands::OverrideChange::Unchanged,
        commands::OverrideChange::Unchanged,
        commands::OverrideChange::Unchanged,
        commands::OverrideChange::Clear,
        commands::OverrideChange::Unchanged,
    )
    .await
    .unwrap();

    assert!(matches!(
        out,
        commands::OverridesOutput::Done { changed: true, .. }
    ));

    let patch = server
        .recorded()
        .into_iter()
        .find(|r| r.method == "PATCH")
        .expect("a PATCH");
    let body = String::from_utf8_lossy(&patch.body);
    assert!(
        body.contains(r#""execution_deadline_seconds":null"#),
        "body: {body}"
    );
}

// --- `<tier> overrides`: runner resources (issue #3209) ---------------------
//
// Same three-way contract as model, thinking, and the execution deadline.
// `runner_resources` is a JSON object on the wire, not a string: a set sends
// that object and nothing else, and a clear sends JSON null rather than ""
// or an omitted key. A null on read is the platform default. The API's 422
// detail is the command error. Contradictory flags, a blank value, and
// malformed JSON are usage errors and do not call the API.

/// The resources block both tiers accept for `--runner-resources`.
const RUNNER_RESOURCES_JSON: &str = r#"{"requests":{"cpu":"500m","memory":"1Gi","ephemeral-storage":"1Gi"},"limits":{"cpu":"1","memory":"2Gi","ephemeral-storage":"4Gi"}}"#;

/// Quota refusal text the fake API returns. The command must show this, not a
/// replacement.
const QUOTA_REFUSAL_DETAIL: &str =
    "cpu request 1500m cannot fit the quota hard 1; lower the override or raise resourceQuota.hard";

fn runner_resources_object() -> serde_json::Value {
    serde_json::from_str(RUNNER_RESOURCES_JSON).expect("runner resources fixture is JSON")
}

#[tokio::test]
async fn overrides_set_runner_resources_patches_only_that_field() {
    let server = serve(|req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => agent_list(),
        ("PATCH", p) if *p == format!("/agents/{AGENT_ID}") => Response::json(
            200,
            &format!(
                r##"{{"id":"{AGENT_ID}","name":"deal-desk","channels":[{{"kind":"slack","address":"#x"}}],"model":null,"thinking":null,"execution_deadline_seconds":null,"runner_resources":{RUNNER_RESOURCES_JSON},"memory":false}}"##
            ),
        ),
        other => panic!("unexpected request: {other:?}"),
    });

    let runner_resources = commands::OverrideChange::resolve_runner_resources(
        Some(RUNNER_RESOURCES_JSON.to_string()),
        false,
    )
    .expect("--runner-resources must accept the resources object");

    let out = commands::overrides(
        opts(&server.base_url, "deal-desk", false),
        commands::OverrideChange::Unchanged,
        commands::OverrideChange::Unchanged,
        commands::OverrideChange::Unchanged,
        commands::OverrideChange::Unchanged,
        runner_resources,
    )
    .await
    .unwrap();

    assert!(matches!(
        out,
        commands::OverridesOutput::Done { changed: true, .. }
    ));

    let rec = server.recorded();
    let patch = rec.iter().find(|r| r.method == "PATCH").expect("a PATCH");
    let body = String::from_utf8_lossy(&patch.body);
    let parsed: serde_json::Value = serde_json::from_str(&body)
        .unwrap_or_else(|err| panic!("PATCH body must be JSON: {err}; {body}"));
    assert_eq!(
        parsed,
        serde_json::json!({ "runner_resources": runner_resources_object() }),
        "PATCH body must contain only runner_resources as that object; model, thinking, and execution_deadline_seconds must be absent: {body}"
    );
}

#[tokio::test]
async fn overrides_clear_runner_resources_sends_explicit_null_not_an_empty_string() {
    let server = serve(|req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => agent_list_with_overrides(),
        ("PATCH", p) if *p == format!("/agents/{AGENT_ID}") => Response::json(
            200,
            &format!(
                r##"{{"id":"{AGENT_ID}","name":"deal-desk","channels":[{{"kind":"slack","address":"#x"}}],"model":"kimi-k2","thinking":"adaptive","execution_deadline_seconds":null,"runner_resources":null,"memory":false}}"##
            ),
        ),
        other => panic!("unexpected request: {other:?}"),
    });

    let runner_resources = commands::OverrideChange::resolve_runner_resources(None, true)
        .expect("--clear-runner-resources must be accepted");

    let out = commands::overrides(
        opts(&server.base_url, "deal-desk", false),
        commands::OverrideChange::Unchanged,
        commands::OverrideChange::Unchanged,
        commands::OverrideChange::Unchanged,
        commands::OverrideChange::Unchanged,
        runner_resources,
    )
    .await
    .unwrap();

    assert!(matches!(
        out,
        commands::OverridesOutput::Done { changed: true, .. }
    ));

    let patch = server
        .recorded()
        .into_iter()
        .find(|r| r.method == "PATCH")
        .expect("a PATCH");
    let body = String::from_utf8_lossy(&patch.body);
    let parsed: serde_json::Value = serde_json::from_str(&body)
        .unwrap_or_else(|err| panic!("PATCH body must be JSON: {err}; {body}"));
    assert_eq!(
        parsed,
        serde_json::json!({ "runner_resources": null }),
        "clear must send runner_resources null, not an empty string and not an omitted field: {body}"
    );
    assert!(
        !body.contains(r#":"""#),
        "clear must be null, not an empty string: {body}"
    );
}

#[tokio::test]
async fn overrides_inspect_reports_null_runner_resources_as_platform_default() {
    use curie::ui::CliOutput;

    let server = serve(|req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => Response::json(
            200,
            &format!(
                r##"[{{"id":"{AGENT_ID}","name":"deal-desk","channels":[{{"kind":"slack","address":"#x"}}],"model":"kimi-k2","thinking":"adaptive","execution_deadline_seconds":null,"runner_resources":null,"created_at":"2026-07-05T00:00:00Z","memory":false,"memory_writes":false}}]"##
            ),
        ),
        other => panic!("unexpected request: {other:?}"),
    });

    let out = commands::overrides(
        opts(&server.base_url, "deal-desk", false),
        commands::OverrideChange::Unchanged,
        commands::OverrideChange::Unchanged,
        commands::OverrideChange::Unchanged,
        commands::OverrideChange::Unchanged,
        commands::OverrideChange::Unchanged,
    )
    .await
    .unwrap();

    let curie::commands::OverridesOutput::Done {
        agent,
        model,
        reviewer_model,
        thinking,
        execution_deadline_seconds,
        runner_resources,
        memory_writes,
        changed,
    } = &out
    else {
        panic!("expected Done, got {out:?}");
    };
    assert!(!changed, "an inspect must not report itself as a write");
    assert!(
        runner_resources.is_none(),
        "null runner_resources is the platform default"
    );

    let json = out.to_json();
    assert_eq!(json["agent"], "deal-desk");
    assert_eq!(json["model"], "kimi-k2");
    assert_eq!(json["thinking"], "adaptive");
    assert!(
        json.as_object()
            .is_some_and(|obj| obj.contains_key("execution_deadline_seconds")),
        "inspect JSON must keep execution_deadline_seconds: {json}"
    );
    assert!(json["execution_deadline_seconds"].is_null());
    // #1461: memory_writes is a plain boolean in the inspect JSON, never null.
    assert_eq!(
        json.get("memory_writes"),
        Some(&serde_json::Value::Bool(false)),
        "inspect JSON must include memory_writes false: {json}"
    );
    assert!(!memory_writes);
    assert_eq!(
        json.get("runner_resources").map(serde_json::Value::is_null),
        Some(true),
        "inspect JSON must include runner_resources null: {json}"
    );

    let line = commands::overrides_summary(
        agent,
        model,
        reviewer_model,
        thinking,
        execution_deadline_seconds,
        runner_resources,
        *memory_writes,
        *changed,
    );
    assert_eq!(
        line,
        "overrides for deal-desk: model kimi-k2, reviewer model credential default, thinking adaptive, execution deadline platform default, runner resources platform default, memory writes off"
    );

    let rec = server.recorded();
    assert_eq!(rec.len(), 1, "inspect must issue exactly one request");
    assert_eq!(rec[0].method, "GET");
}

#[tokio::test]
async fn overrides_error_includes_the_api_quota_refusal_detail() {
    let server = serve(|req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => Response::json(
            200,
            &format!(
                r##"[{{"id":"{AGENT_ID}","name":"deal-desk","channels":[{{"kind":"slack","address":"#x"}}],"created_at":"2026-07-05T00:00:00Z","memory":false,"runner_resources":null}}]"##
            ),
        ),
        ("PATCH", p) if *p == format!("/agents/{AGENT_ID}") => {
            Response::json(422, &format!(r#"{{"detail":"{QUOTA_REFUSAL_DETAIL}"}}"#))
        }
        other => panic!("unexpected request: {other:?}"),
    });

    let runner_resources = commands::OverrideChange::resolve_runner_resources(
        Some(RUNNER_RESOURCES_JSON.to_string()),
        false,
    )
    .expect("--runner-resources must accept the resources object before the API answers");

    let err = commands::overrides(
        opts(&server.base_url, "deal-desk", false),
        commands::OverrideChange::Unchanged,
        commands::OverrideChange::Unchanged,
        commands::OverrideChange::Unchanged,
        commands::OverrideChange::Unchanged,
        runner_resources,
    )
    .await
    .expect_err("a quota 422 must fail the command");

    let rendered = format!("{err:#}");
    let payload = curie::exit::error_json(&err).to_string();
    assert!(
        rendered.contains(QUOTA_REFUSAL_DETAIL) || payload.contains(QUOTA_REFUSAL_DETAIL),
        "the command error output must contain the API quota detail, not swallow it; rendered: {rendered}; payload: {payload}"
    );
    assert!(
        server.recorded().iter().any(|r| r.method == "PATCH"),
        "the refusal detail comes from the API, so the command must have called it"
    );
}

#[tokio::test]
async fn runner_resources_and_clear_runner_resources_together_is_a_usage_error() {
    let server = serve(|req| {
        panic!("--runner-resources and --clear-runner-resources must not call the API: {req:?}")
    });

    let err = commands::OverrideChange::resolve_runner_resources(
        Some(RUNNER_RESOURCES_JSON.to_string()),
        true,
    )
    .expect_err("--runner-resources and --clear-runner-resources must contradict each other");
    assert_eq!(
        curie::exit::classify(&err).0,
        curie::exit::ExitClass::Usage,
        "{err}"
    );
    let msg = err.to_string();
    assert!(msg.contains("--runner-resources"), "{msg}");
    assert!(msg.contains("--clear-runner-resources"), "{msg}");
    assert!(
        server.recorded().is_empty(),
        "a usage error must not call the API"
    );
}

#[tokio::test]
async fn blank_runner_resources_is_a_usage_error_and_does_not_call_the_api() {
    let server = serve(|req| panic!("a blank --runner-resources must not call the API: {req:?}"));
    for blank in ["", "   ", "\t"] {
        let err =
            commands::OverrideChange::resolve_runner_resources(Some(blank.to_string()), false)
                .expect_err("a blank --runner-resources must be a usage error");
        assert_eq!(
            curie::exit::classify(&err).0,
            curie::exit::ExitClass::Usage,
            "{err}"
        );
        let msg = err.to_string();
        assert!(
            msg.contains("blank"),
            "a blank --runner-resources must be refused as blank, not forwarded: {msg}"
        );
        assert!(
            msg.contains("--clear-runner-resources"),
            "the refusal must name the flag that clears: {msg}"
        );
    }
    assert!(
        server.recorded().is_empty(),
        "a usage error must not call the API"
    );
}

#[tokio::test]
async fn malformed_runner_resources_json_is_a_usage_error_and_does_not_call_the_api() {
    let server = serve(|req| panic!("malformed --runner-resources must not call the API: {req:?}"));
    for raw in ["{", "not-json", "{\"requests\":"] {
        let err = commands::OverrideChange::resolve_runner_resources(Some(raw.to_string()), false)
            .expect_err("malformed runner resources JSON must be a usage error");
        assert_eq!(
            curie::exit::classify(&err).0,
            curie::exit::ExitClass::Usage,
            "{err}"
        );
        let msg = err.to_string();
        assert!(
            msg.contains("--runner-resources"),
            "the usage error must name the flag: {msg}"
        );
    }
    assert!(
        server.recorded().is_empty(),
        "a usage error must not call the API"
    );
}

// --- memory writes and guidance (issue #1461) -------------------------------
//
// Driven through the built binary against the wire-level test server, so the
// clap flag, the handler and the HTTP call are all on the path: the request
// the server records is the one an operator's command would send. Unexpected
// requests get a 404 rather than a panic so a wrong call shows up as a failed
// assertion on what was recorded.

fn curie_against(base_url: &str, rest: &[&str]) -> std::process::Output {
    let mut argv: Vec<&str> = rest.to_vec();
    argv.extend(["--api-url", base_url, "--api-key", "k", "--json"]);
    std::process::Command::new(env!("CARGO_BIN_EXE_curie"))
        .args(&argv)
        .env_remove("CURIE_API_URL")
        .env_remove("CURIE_API_KEY")
        .env("NO_PROXY", "127.0.0.1,localhost")
        .env("no_proxy", "127.0.0.1,localhost")
        .output()
        .unwrap_or_else(|e| panic!("run curie {}: {e}", argv.join(" ")))
}

fn agent_json_with_memory_writes(memory_writes: bool) -> String {
    format!(
        r##"{{"id":"{AGENT_ID}","name":"deal-desk","channels":[{{"kind":"slack","address":"#x"}}],"model":null,"thinking":null,"execution_deadline_seconds":null,"created_at":"2026-07-05T00:00:00Z","memory":false,"memory_writes":{memory_writes}}}"##
    )
}

fn not_found() -> Response {
    Response::json(404, r#"{"detail":"not found"}"#)
}

#[test]
fn overrides_memory_writes_on_patches_a_json_true() {
    let server = serve(|req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => {
            Response::json(200, &format!("[{}]", agent_json_with_memory_writes(false)))
        }
        ("PATCH", p) if *p == format!("/agents/{AGENT_ID}") => {
            Response::json(200, &agent_json_with_memory_writes(true))
        }
        _ => not_found(),
    });

    let output = curie_against(
        &server.base_url,
        &["local", "overrides", "deal-desk", "--memory-writes", "on"],
    );
    assert!(
        output.status.success(),
        "stdout: {}; stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let rec = server.recorded();
    let patches: Vec<_> = rec.iter().filter(|r| r.method == "PATCH").collect();
    assert_eq!(patches.len(), 1, "exactly one PATCH: {rec:?}");
    assert_eq!(patches[0].path, format!("/agents/{AGENT_ID}"));
    assert_eq!(patches[0].header("x-api-key"), Some("k"));
    let body: serde_json::Value =
        serde_json::from_slice(&patches[0].body).expect("PATCH body is JSON");
    assert_eq!(
        body,
        serde_json::json!({"memory_writes": true}),
        "only memory_writes, as a JSON boolean"
    );
}

#[test]
fn overrides_memory_writes_off_patches_a_json_false() {
    let server = serve(|req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => {
            Response::json(200, &format!("[{}]", agent_json_with_memory_writes(true)))
        }
        ("PATCH", p) if *p == format!("/agents/{AGENT_ID}") => {
            Response::json(200, &agent_json_with_memory_writes(false))
        }
        _ => not_found(),
    });

    let output = curie_against(
        &server.base_url,
        &["local", "overrides", "deal-desk", "--memory-writes", "off"],
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let rec = server.recorded();
    let patch = rec.iter().find(|r| r.method == "PATCH").expect("a PATCH");
    let body: serde_json::Value = serde_json::from_slice(&patch.body).expect("JSON body");
    assert_eq!(body, serde_json::json!({"memory_writes": false}));
}

#[test]
fn memory_guidance_from_puts_the_file_text_to_the_guidance_endpoint() {
    let text = "Remember customer preferences.\nNever record secrets or credentials.";
    let path = std::env::temp_dir().join(format!("curie-guidance-{}.md", uuid::Uuid::new_v4()));
    std::fs::write(&path, text).expect("write guidance file");

    let stored = serde_json::json!({"text": text, "source": "operator"}).to_string();
    let server = serve(move |req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => {
            Response::json(200, &format!("[{}]", agent_json_with_memory_writes(true)))
        }
        ("PUT", p) if *p == format!("/agents/{AGENT_ID}/memory/guidance") => {
            Response::json(200, &stored)
        }
        ("GET", p) if *p == format!("/agents/{AGENT_ID}/memory/guidance") => {
            Response::json(200, &stored)
        }
        _ => not_found(),
    });

    let output = curie_against(
        &server.base_url,
        &[
            "local",
            "memory",
            "deal-desk",
            "--guidance-from",
            path.to_str().unwrap(),
        ],
    );
    let _ = std::fs::remove_file(&path);
    assert!(
        output.status.success(),
        "stdout: {}; stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let rec = server.recorded();
    let puts: Vec<_> = rec.iter().filter(|r| r.method == "PUT").collect();
    assert_eq!(puts.len(), 1, "exactly one PUT: {rec:?}");
    assert_eq!(puts[0].path, format!("/agents/{AGENT_ID}/memory/guidance"));
    assert_eq!(puts[0].header("x-api-key"), Some("k"));
    let body: serde_json::Value = serde_json::from_slice(&puts[0].body).expect("PUT body is JSON");
    assert_eq!(
        body,
        serde_json::json!({"text": text}),
        "the file's text, verbatim"
    );
    assert!(
        !rec.iter()
            .any(|r| r.method == "PATCH" || r.method == "POST" || r.method == "DELETE"),
        "--guidance-from writes only the guidance: {rec:?}"
    );
}

#[test]
fn memory_guidance_from_an_empty_file_is_refused_before_any_write() {
    let path = std::env::temp_dir().join(format!("curie-guidance-{}.md", uuid::Uuid::new_v4()));
    std::fs::write(&path, "   \n").expect("write guidance file");
    let server = serve(|req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => {
            Response::json(200, &format!("[{}]", agent_json_with_memory_writes(true)))
        }
        ("GET", p) if *p == format!("/agents/{AGENT_ID}/memory/guidance") => {
            Response::json(200, r#"{"text":"default","source":"default"}"#)
        }
        _ => not_found(),
    });

    // Anchor: the guidance flags must exist, so the refusal below is about the
    // empty file and not about an unknown flag.
    let anchor = curie_against(
        &server.base_url,
        &["local", "memory", "deal-desk", "--guidance"],
    );
    assert!(
        anchor.status.success(),
        "--guidance must be a known flag; stderr: {}",
        String::from_utf8_lossy(&anchor.stderr)
    );

    let output = curie_against(
        &server.base_url,
        &[
            "local",
            "memory",
            "deal-desk",
            "--guidance-from",
            path.to_str().unwrap(),
        ],
    );
    let _ = std::fs::remove_file(&path);
    assert!(
        !output.status.success(),
        "an empty guidance file must be refused"
    );
    assert!(
        !server.recorded().iter().any(|r| r.method == "PUT"),
        "no PUT for an empty guidance file"
    );
}

#[test]
fn memory_guidance_shows_the_effective_text_and_its_source() {
    let server = serve(|req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => {
            Response::json(200, &format!("[{}]", agent_json_with_memory_writes(true)))
        }
        ("GET", p) if *p == format!("/agents/{AGENT_ID}/memory/guidance") => Response::json(
            200,
            r#"{"text":"guidance-sentinel-7f3a","source":"operator"}"#,
        ),
        _ => not_found(),
    });

    let output = curie_against(
        &server.base_url,
        &["local", "memory", "deal-desk", "--guidance"],
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "stdout: {stdout}; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        stdout.contains("guidance-sentinel-7f3a"),
        "text shown: {stdout}"
    );
    assert!(stdout.contains("operator"), "source shown: {stdout}");
    let rec = server.recorded();
    assert!(
        rec.iter().all(|r| r.method == "GET"),
        "--guidance only reads: {rec:?}"
    );
    assert!(rec
        .iter()
        .any(|r| r.path == format!("/agents/{AGENT_ID}/memory/guidance")));
}

#[test]
fn memory_reset_guidance_deletes_the_guidance_endpoint() {
    let server = serve(|req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => {
            Response::json(200, &format!("[{}]", agent_json_with_memory_writes(true)))
        }
        ("DELETE", p) if *p == format!("/agents/{AGENT_ID}/memory/guidance") => Response {
            status: 204,
            content_type: "application/json".into(),
            body: Vec::new(),
        },
        ("GET", p) if *p == format!("/agents/{AGENT_ID}/memory/guidance") => {
            Response::json(200, r#"{"text":"default","source":"default"}"#)
        }
        _ => not_found(),
    });

    let output = curie_against(
        &server.base_url,
        &["local", "memory", "deal-desk", "--reset-guidance"],
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let rec = server.recorded();
    let deletes: Vec<_> = rec.iter().filter(|r| r.method == "DELETE").collect();
    assert_eq!(deletes.len(), 1, "exactly one DELETE: {rec:?}");
    assert_eq!(
        deletes[0].path,
        format!("/agents/{AGENT_ID}/memory/guidance")
    );
}

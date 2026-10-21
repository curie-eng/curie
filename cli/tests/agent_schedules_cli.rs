//! Binary contract for agent-owned schedules on the CLI: the `agent_schedules`
//! list key, the `--show/--cancel/--update/--revert/--history` verbs, the agent
//! policy setters, `--pause` of an agent-schedule id, and
//! `overrides --agent-schedules on|off`. The platform API is the only mocked
//! boundary; dry runs return before any HTTP client is built.

use crate::support;

use std::process::{Command, Output, Stdio};

use serde_json::{json, Value};
use support::{serve, Response};

const TEST_API_KEY: &str = "curie-agent-schedules-test-key";
const AGENT_ID: &str = "11111111-1111-4111-8111-111111111111";
const SCHEDULE_ID: &str = "22222222-2222-4222-8222-222222222222";
const UNREACHABLE_API_URL: &str = "http://127.0.0.1:1";
const MISSING_KUBECONFIG: &str = "/nonexistent/curie-test-kubeconfig";

fn run(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_curie"))
        .args(args)
        .stdin(Stdio::null())
        .env_remove("CURIE_API_URL")
        .env_remove("CURIE_API_KEY")
        .env("KUBECONFIG", MISSING_KUBECONFIG)
        .env("NO_PROXY", "127.0.0.1,localhost")
        .env("no_proxy", "127.0.0.1,localhost")
        .output()
        .unwrap_or_else(|error| panic!("run curie {}: {error}", args.join(" ")))
}

fn describe(output: &Output) -> String {
    format!(
        "exit {:?}\nstdout: {}\nstderr: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

/// `curie <tier> schedules <extra> --api-url .. --api-key ..`, with extra trailing args.
fn schedules(tier: &str, extra: &[&str], api_url: &str, trailing: &[&str]) -> Output {
    let mut args = vec![tier, "schedules"];
    args.extend_from_slice(extra);
    args.extend_from_slice(&["--api-url", api_url, "--api-key", TEST_API_KEY]);
    args.extend_from_slice(trailing);
    run(&args)
}

fn one_object(output: &Output) -> Value {
    serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|error| panic!("stdout must be JSON ({error}): {}", describe(output)))
}

/// The single plan line of a `--dry-run --json` run, asserting exit 0.
fn plan_line(tier: &str, extra: &[&str]) -> String {
    let output = schedules(tier, extra, UNREACHABLE_API_URL, &["--dry-run", "--json"]);
    assert_eq!(output.status.code(), Some(0), "{}", describe(&output));
    let value = one_object(&output);
    assert_eq!(value["dry_run"], json!(true), "{value}");
    let plan = value["plan"]
        .as_array()
        .unwrap_or_else(|| panic!("{value}"));
    assert_eq!(plan.len(), 1, "one planned request: {value}");
    plan[0].as_str().expect("plan line is a string").to_string()
}

fn body_of(line: &str) -> Value {
    let start = line
        .find('{')
        .unwrap_or_else(|| panic!("plan line must embed a body: {line}"));
    let end = line.rfind('}').expect(line);
    serde_json::from_str(&line[start..=end])
        .unwrap_or_else(|error| panic!("body must be JSON ({error}): {line}"))
}

fn agent_schedules_body() -> Value {
    json!({
        "schedules": [],
        "agent_schedules": [
            {
                "id": SCHEDULE_ID,
                "revision": 3,
                "state": "active",
                "cron": "0 9 * * 1",
                "run_at": null,
                "timezone": "America/New_York",
                "prompt": "Summarize the week",
                "created_by": "agent"
            },
            {
                "id": "33333333-3333-4333-8333-333333333333",
                "revision": 1,
                "state": "paused",
                "cron": null,
                "run_at": "2026-11-01T15:00:00Z",
                "timezone": "UTC",
                "prompt": "One shot",
                "created_by": "operator"
            }
        ]
    })
}

#[test]
fn local_json_list_carries_agent_schedules_unchanged() {
    let body = agent_schedules_body().to_string();
    let server = serve(move |req| {
        if req.path.split('?').next() == Some("/schedules") {
            Response::json(200, &body)
        } else {
            Response::json(500, r#"{"detail":"unexpected"}"#)
        }
    });

    let output = schedules("local", &["--agent", "a1"], &server.base_url, &["--json"]);

    assert_eq!(output.status.code(), Some(0), "{}", describe(&output));
    let value = one_object(&output);
    assert_eq!(
        value["agent_schedules"],
        agent_schedules_body()["agent_schedules"]
    );
    let first = &value["agent_schedules"][0];
    assert_eq!(first["cron"], json!("0 9 * * 1"));
    assert_eq!(first["run_at"], Value::Null);
    assert_eq!(value["agent_schedules"][1]["cron"], Value::Null);
    assert_eq!(value["agent_schedules"][1]["created_by"], json!("operator"));
}

#[test]
fn cluster_json_list_carries_agent_schedules_unchanged() {
    let body = agent_schedules_body().to_string();
    let server = serve(move |_| Response::json(200, &body));

    let output = schedules("cluster", &["--agent", "a1"], &server.base_url, &["--json"]);

    assert_eq!(output.status.code(), Some(0), "{}", describe(&output));
    assert_eq!(
        one_object(&output)["agent_schedules"],
        agent_schedules_body()["agent_schedules"]
    );
}

#[test]
fn dry_runs_plan_the_agent_scoped_requests_at_both_tiers() {
    for tier in ["local", "cluster"] {
        let show = plan_line(tier, &["--agent", "a1", "--show", SCHEDULE_ID]);
        assert!(show.starts_with("GET "), "{show}");
        assert!(
            show.contains(&format!("/agents/a1/schedules/{SCHEDULE_ID}")),
            "{show}"
        );

        let cancel = plan_line(tier, &["--agent", "a1", "--cancel", SCHEDULE_ID]);
        assert!(cancel.starts_with("POST "), "{cancel}");
        assert!(
            cancel.contains(&format!("/agents/a1/schedules/{SCHEDULE_ID}/cancel")),
            "{cancel}"
        );

        let history = plan_line(tier, &["--agent", "a1", "--history", SCHEDULE_ID]);
        assert!(history.starts_with("GET "), "{history}");
        assert!(
            history.contains(&format!("/agents/a1/schedules/{SCHEDULE_ID}/revisions")),
            "{history}"
        );

        let update = plan_line(
            tier,
            &[
                "--agent",
                "a1",
                "--update",
                SCHEDULE_ID,
                "--prompt",
                "x",
                "--expected-revision",
                "2",
            ],
        );
        assert!(update.starts_with("PATCH "), "{update}");
        assert!(
            update.contains(&format!("/agents/a1/schedules/{SCHEDULE_ID}")),
            "{update}"
        );
        assert_eq!(
            body_of(&update),
            json!({"expected_revision": 2, "prompt": "x"}),
            "{update}"
        );

        let revert = plan_line(
            tier,
            &[
                "--agent",
                "a1",
                "--revert",
                SCHEDULE_ID,
                "--to-revision",
                "1",
                "--expected-revision",
                "3",
            ],
        );
        assert!(revert.starts_with("POST "), "{revert}");
        assert!(
            revert.contains(&format!("/agents/a1/schedules/{SCHEDULE_ID}/revert")),
            "{revert}"
        );
        assert_eq!(
            body_of(&revert),
            json!({"expected_revision": 3, "to_revision": 1}),
            "{revert}"
        );
    }
}

#[test]
fn update_can_carry_cron_run_at_and_timezone() {
    let cron = plan_line(
        "local",
        &[
            "--agent",
            "a1",
            "--update",
            SCHEDULE_ID,
            "--cron",
            "0 8 * * *",
            "--timezone",
            "Europe/Paris",
            "--expected-revision",
            "4",
        ],
    );
    assert_eq!(
        body_of(&cron),
        json!({"expected_revision": 4, "cron": "0 8 * * *", "timezone": "Europe/Paris"}),
        "{cron}"
    );
    let run_at = plan_line(
        "local",
        &[
            "--agent",
            "a1",
            "--update",
            SCHEDULE_ID,
            "--run-at",
            "2026-12-01T10:00:00Z",
            "--expected-revision",
            "1",
        ],
    );
    assert_eq!(
        body_of(&run_at),
        json!({"expected_revision": 1, "run_at": "2026-12-01T10:00:00Z"}),
        "{run_at}"
    );
}

#[test]
fn agent_policy_setters_patch_the_agent_at_both_tiers() {
    for tier in ["local", "cluster"] {
        let cases: [(&[&str], Value); 3] = [
            (
                &["--set-timezone", "America/New_York"],
                json!({"schedule_timezone": "America/New_York"}),
            ),
            (
                &["--set-max-active", "25"],
                json!({"schedule_max_active": 25}),
            ),
            (
                &["--set-min-interval-minutes", "1"],
                json!({"schedule_min_interval_minutes": 1}),
            ),
        ];
        for (flags, expected) in cases {
            let mut extra = vec!["--agent", "a1"];
            extra.extend_from_slice(flags);
            let line = plan_line(tier, &extra);
            assert!(line.starts_with("PATCH "), "{line}");
            assert!(line.contains("/agents/"), "{line}");
            assert!(
                !line.contains("/schedules"),
                "policy PATCH targets the agent: {line}"
            );
            assert_eq!(body_of(&line), expected, "{line}");
        }
    }
}

#[test]
fn every_new_verb_requires_agent() {
    let verbs: [&[&str]; 8] = [
        &["--show", SCHEDULE_ID],
        &["--cancel", SCHEDULE_ID],
        &["--history", SCHEDULE_ID],
        &[
            "--update",
            SCHEDULE_ID,
            "--prompt",
            "x",
            "--expected-revision",
            "2",
        ],
        &[
            "--revert",
            SCHEDULE_ID,
            "--to-revision",
            "1",
            "--expected-revision",
            "3",
        ],
        &["--set-timezone", "UTC"],
        &["--set-max-active", "25"],
        &["--set-min-interval-minutes", "1"],
    ];
    for tier in ["local", "cluster"] {
        for verb in verbs {
            let mut args = vec![tier, "schedules"];
            args.extend_from_slice(verb);
            args.extend_from_slice(&["--dry-run", "--api-url", UNREACHABLE_API_URL]);
            let output = run(&args);
            assert_eq!(
                output.status.code(),
                Some(2),
                "{tier} {verb:?} without --agent must be a usage error: {}",
                describe(&output)
            );
        }
    }
}

#[test]
fn the_new_verbs_conflict_with_each_other() {
    let pairs: [(&[&str], &[&str]); 7] = [
        (&["--show", SCHEDULE_ID], &["--cancel", SCHEDULE_ID]),
        (&["--show", SCHEDULE_ID], &["--history", SCHEDULE_ID]),
        (&["--cancel", SCHEDULE_ID], &["--history", SCHEDULE_ID]),
        (
            &[
                "--update",
                SCHEDULE_ID,
                "--prompt",
                "x",
                "--expected-revision",
                "2",
            ],
            &["--cancel", SCHEDULE_ID],
        ),
        (
            &[
                "--revert",
                SCHEDULE_ID,
                "--to-revision",
                "1",
                "--expected-revision",
                "3",
            ],
            &["--show", SCHEDULE_ID],
        ),
        (&["--show", SCHEDULE_ID], &["--set-timezone", "UTC"]),
        (
            &["--set-max-active", "25"],
            &["--set-min-interval-minutes", "1"],
        ),
    ];
    for (left, right) in pairs {
        let mut args = vec!["local", "schedules", "--agent", "a1"];
        args.extend_from_slice(left);
        args.extend_from_slice(right);
        args.extend_from_slice(&["--dry-run", "--api-url", UNREACHABLE_API_URL]);
        let output = run(&args);
        assert_eq!(
            output.status.code(),
            Some(2),
            "{left:?} with {right:?} must conflict: {}",
            describe(&output)
        );
    }
}

#[test]
fn pause_accepts_an_agent_schedule_id_on_the_existing_route() {
    let id = format!("agent-schedule:{SCHEDULE_ID}");
    for tier in ["local", "cluster"] {
        let line = plan_line(tier, &["--agent", "acme-bot", "--pause", &id]);
        assert!(line.starts_with("POST "), "{line}");
        assert!(
            line.contains(&format!(
                "/schedules/acme-bot/agent-schedule:{SCHEDULE_ID}/pause"
            )),
            "{line}"
        );
    }
    let resume = plan_line("local", &["--agent", "acme-bot", "--resume", &id]);
    assert!(
        resume.contains(&format!(
            "/schedules/acme-bot/agent-schedule:{SCHEDULE_ID}/resume"
        )),
        "{resume}"
    );
}

fn overrides_dry_plan(tier: &str, value: &str) -> String {
    let output = run(&[
        tier,
        "overrides",
        "deal-desk",
        "--agent-schedules",
        value,
        "--api-url",
        UNREACHABLE_API_URL,
        "--api-key",
        TEST_API_KEY,
        "--dry-run",
        "--json",
    ]);
    assert_eq!(output.status.code(), Some(0), "{}", describe(&output));
    one_object(&output)["plan"][0]
        .as_str()
        .expect("plan line")
        .to_string()
}

#[test]
fn overrides_agent_schedules_dry_run_sends_a_json_boolean_at_both_tiers() {
    for tier in ["local", "cluster"] {
        assert_eq!(
            body_of(&overrides_dry_plan(tier, "on")),
            json!({"agent_schedules": true})
        );
        assert_eq!(
            body_of(&overrides_dry_plan(tier, "off")),
            json!({"agent_schedules": false})
        );
    }
}

#[test]
fn overrides_agent_schedules_rejects_other_values() {
    let output = run(&[
        "local",
        "overrides",
        "deal-desk",
        "--agent-schedules",
        "maybe",
        "--dry-run",
    ]);
    assert_eq!(output.status.code(), Some(2), "{}", describe(&output));
}

#[test]
fn overrides_agent_schedules_patches_the_agent_over_http() {
    for (flag, expected) in [("on", true), ("off", false)] {
        let agent = format!(
            r##"{{"id":"{AGENT_ID}","name":"deal-desk","channels":[{{"kind":"slack","address":"#x"}}],"model":null,"thinking":null,"execution_deadline_seconds":null,"created_at":"2026-07-05T00:00:00Z","memory":false,"memory_writes":true,"agent_wide_learning":false,"agent_schedules":{expected}}}"##
        );
        let list = format!("[{agent}]");
        let server = serve(move |req| match (req.method.as_str(), req.path.as_str()) {
            ("GET", "/agents") => Response::json(200, &list),
            ("PATCH", p) if p == format!("/agents/{AGENT_ID}") => Response::json(200, &agent),
            _ => Response::json(404, r#"{"detail":"not found"}"#),
        });

        let output = run(&[
            "local",
            "overrides",
            "deal-desk",
            "--agent-schedules",
            flag,
            "--api-url",
            &server.base_url,
            "--api-key",
            TEST_API_KEY,
            "--json",
        ]);

        assert_eq!(output.status.code(), Some(0), "{}", describe(&output));
        let recorded = server.recorded();
        let patches: Vec<_> = recorded.iter().filter(|r| r.method == "PATCH").collect();
        assert_eq!(patches.len(), 1, "exactly one PATCH: {recorded:?}");
        assert_eq!(patches[0].path, format!("/agents/{AGENT_ID}"));
        let body: Value = serde_json::from_slice(&patches[0].body).expect("JSON body");
        assert_eq!(body, json!({"agent_schedules": expected}));
    }
}

/// The agent row as `GET /agents` and `PATCH /agents/{id}` return it, with the
/// ADR 0238 switch and policy settings.
fn agent_row(agent_schedules: bool, max_active: u32) -> String {
    format!(
        r##"{{"id":"{AGENT_ID}","name":"deal-desk","channels":[{{"kind":"slack","address":"#x"}}],"model":null,"thinking":null,"execution_deadline_seconds":null,"created_at":"2026-07-05T00:00:00Z","memory":false,"memory_writes":true,"agent_wide_learning":false,"agent_schedules":{agent_schedules},"schedule_timezone":"UTC","schedule_max_active":{max_active},"schedule_min_interval_minutes":15}}"##
    )
}

fn overrides_http(server_url: &str, extra: &[&str]) -> Output {
    let mut args = vec!["--json", "local", "overrides", "deal-desk"];
    args.extend_from_slice(extra);
    args.extend_from_slice(&["--api-url", server_url, "--api-key", TEST_API_KEY]);
    run(&args)
}

/// Every value stored under `key` anywhere in `value`.
fn values_under<'a>(value: &'a Value, key: &str, found: &mut Vec<&'a Value>) {
    match value {
        Value::Object(map) => {
            for (k, v) in map {
                if k == key {
                    found.push(v);
                }
                values_under(v, key, found);
            }
        }
        Value::Array(items) => items.iter().for_each(|v| values_under(v, key, found)),
        _ => {}
    }
}

#[test]
fn overrides_agent_schedules_prints_the_value_the_api_stored() {
    // The request says on; the API stored (and returned) false. The CLI must
    // print what was stored, not echo the flag.
    let before = agent_row(false, 20);
    let list = format!("[{before}]");
    let stored = agent_row(false, 20);
    let server = serve(move |req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => Response::json(200, &list),
        ("PATCH", p) if p == format!("/agents/{AGENT_ID}") => Response::json(200, &stored),
        _ => Response::json(404, r#"{"detail":"not found"}"#),
    });

    let output = overrides_http(&server.base_url, &["--agent-schedules", "on"]);

    assert_eq!(output.status.code(), Some(0), "{}", describe(&output));
    let patches: Vec<_> = server
        .recorded()
        .into_iter()
        .filter(|r| r.method == "PATCH")
        .collect();
    assert_eq!(patches.len(), 1);
    let sent: Value = serde_json::from_slice(&patches[0].body).expect("JSON body");
    assert_eq!(sent, json!({"agent_schedules": true}));
    let value = one_object(&output);
    assert_eq!(value["agent_schedules"], json!(false), "{value}");
    assert_eq!(value["changed"], json!(true), "{value}");
}

#[test]
fn overrides_inspection_shows_agent_schedules() {
    let row = agent_row(true, 20);
    let list = format!("[{row}]");
    let server = serve(move |req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => Response::json(200, &list),
        _ => Response::json(404, r#"{"detail":"not found"}"#),
    });

    let output = overrides_http(&server.base_url, &[]);

    assert_eq!(output.status.code(), Some(0), "{}", describe(&output));
    assert!(
        server.recorded().iter().all(|r| r.method == "GET"),
        "inspection writes nothing"
    );
    let value = one_object(&output);
    assert_eq!(value["agent_schedules"], json!(true), "{value}");
    assert_eq!(value["changed"], json!(false), "{value}");
}

#[test]
fn set_max_active_prints_the_stored_policy_from_the_patch_response() {
    // The request asks for 25; the API stored 30 (and says so). The CLI prints 30.
    let before = agent_row(true, 20);
    let list = format!("[{before}]");
    let stored = agent_row(true, 30);
    let server = serve(move |req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => Response::json(200, &list),
        ("PATCH", p) if p == format!("/agents/{AGENT_ID}") => Response::json(200, &stored),
        _ => Response::json(404, r#"{"detail":"not found"}"#),
    });

    let output = schedules(
        "local",
        &["--agent", "deal-desk", "--set-max-active", "25"],
        &server.base_url,
        &["--json"],
    );

    assert_eq!(output.status.code(), Some(0), "{}", describe(&output));
    let patches: Vec<_> = server
        .recorded()
        .into_iter()
        .filter(|r| r.method == "PATCH")
        .collect();
    assert_eq!(patches.len(), 1);
    let sent: Value = serde_json::from_slice(&patches[0].body).expect("JSON body");
    assert_eq!(sent, json!({"schedule_max_active": 25}));
    let value = one_object(&output);
    let mut found = Vec::new();
    values_under(&value, "schedule_max_active", &mut found);
    assert!(
        !found.is_empty(),
        "the stored schedule_max_active is printed: {value}"
    );
    assert!(
        found.iter().all(|v| **v == json!(30)),
        "only the stored 30 is printed, never the requested 25: {value}"
    );
}

fn history_server(revisions: &'static str) -> support::MockServer {
    let list = format!("[{}]", agent_row(true, 20));
    serve(move |req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => Response::json(200, &list),
        ("GET", p) if p == format!("/agents/{AGENT_ID}/schedules/{SCHEDULE_ID}/revisions") => {
            Response::json(200, revisions)
        }
        _ => Response::json(404, r#"{"detail":"not found"}"#),
    })
}

const REVISIONS_OBJECT: &str = r#"{"revisions":[{"revision":1,"action":"create","state":"active","author":"U0EXAMPLE1","source":"agent","created_at":"2026-10-20T00:00:00Z"}]}"#;

#[test]
fn history_prints_the_revisions_object() {
    let server = history_server(REVISIONS_OBJECT);
    let output = schedules(
        "local",
        &["--agent", "deal-desk", "--history", SCHEDULE_ID],
        &server.base_url,
        &["--json"],
    );
    assert_eq!(output.status.code(), Some(0), "{}", describe(&output));
    let expected: Value = serde_json::from_str(REVISIONS_OBJECT).unwrap();
    assert_eq!(one_object(&output), expected);
}

#[test]
fn history_refuses_a_bare_revisions_array() {
    // The API returns only the object form; a bare array is not a second
    // accepted shape.
    let server = history_server(
        r#"[{"revision":1,"action":"create","state":"active","author":"U0EXAMPLE1","source":"agent","created_at":"2026-10-20T00:00:00Z"}]"#,
    );
    let output = schedules(
        "local",
        &["--agent", "deal-desk", "--history", SCHEDULE_ID],
        &server.base_url,
        &["--json"],
    );
    assert_ne!(output.status.code(), Some(0), "{}", describe(&output));
}

#[test]
fn schedule_list_without_agent_schedules_is_refused() {
    // `agent_schedules` is a required key of `GET /schedules`; a body without
    // it is not read as "none".
    let server = serve(move |req| {
        if req.path.split('?').next() == Some("/schedules") {
            Response::json(200, r#"{"schedules":[]}"#)
        } else {
            Response::json(500, r#"{"detail":"unexpected"}"#)
        }
    });
    let output = schedules("local", &["--agent", "a1"], &server.base_url, &["--json"]);
    assert_ne!(output.status.code(), Some(0), "{}", describe(&output));

    let ok = serve(move |req| {
        if req.path.split('?').next() == Some("/schedules") {
            Response::json(200, r#"{"schedules":[],"agent_schedules":[]}"#)
        } else {
            Response::json(500, r#"{"detail":"unexpected"}"#)
        }
    });
    let output = schedules("local", &["--agent", "a1"], &ok.base_url, &["--json"]);
    assert_eq!(output.status.code(), Some(0), "{}", describe(&output));
    assert_eq!(one_object(&output)["agent_schedules"], json!([]));
}

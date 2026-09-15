//! Integration: `curie <tier> memory --add` posts through the memory surface
//! (`POST /agents/{id}/memory`, #1904), not the reserved state-append path.

mod support;

use std::process::{Command, Stdio};

use curie::api::ApiClient;
use curie::commands::{self, AgentActionOpts, MemoryOutput};
use curie::ui::CliOutput;
use support::{serve, Response};

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

const AGENT_ID: &str = "11111111-1111-1111-1111-111111111111";

fn agent_list() -> Response {
    Response::json(
        200,
        &format!(
            r##"[{{"id":"{AGENT_ID}","name":"translation-bot","channels":[{{"kind":"slack","address":"#x"}}],"memory":false,"created_at":"2026-07-05T00:00:00Z"}}]"##
        ),
    )
}

fn created_entry() -> Response {
    Response::json(
        201,
        r#"{
            "index": 0,
            "content": "ask before translating to French",
            "provenance": {
                "learned_from_session_id": null,
                "source_trace_ids": [],
                "recorded_at": "2026-08-27T00:00:00+00:00",
                "source": "operator"
            },
            "version": 1
        }"#,
    )
}

fn opts(base_url: &str, dry_run: bool) -> AgentActionOpts {
    AgentActionOpts {
        api_url: base_url.to_string(),
        api_key: "k".to_string(),
        agent: "translation-bot".to_string(),
        dry_run,
    }
}

#[tokio::test]
async fn create_memory_posts_content_to_the_memory_surface() {
    let server = serve(|req| match (req.method.as_str(), req.path.as_str()) {
        ("POST", p) if *p == format!("/agents/{AGENT_ID}/memory") => created_entry(),
        other => panic!("unexpected request: {other:?}"),
    });
    let client = ApiClient::new(&server.base_url, "k").unwrap();
    let entry = client
        .create_memory(AGENT_ID, "ask before translating to French")
        .await
        .unwrap();
    assert_eq!(entry.index, 0);
    assert_eq!(entry.content, "ask before translating to French");

    let rec = server.recorded();
    assert_eq!(rec.len(), 1);
    assert_eq!(rec[0].method, "POST");
    assert_eq!(rec[0].path, format!("/agents/{AGENT_ID}/memory"));
    let body = String::from_utf8_lossy(&rec[0].body);
    assert!(
        body.contains("\"content\":\"ask before translating to French\""),
        "body: {body}"
    );
    assert!(
        !body.contains("provenance"),
        "CLI must not send caller-supplied provenance: {body}"
    );
    assert_eq!(rec[0].header("x-api-key"), Some("k"));
}

#[tokio::test]
async fn memory_add_handler_resolves_by_name_then_posts() {
    let server = serve(|req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => agent_list(),
        ("POST", p) if *p == format!("/agents/{AGENT_ID}/memory") => created_entry(),
        other => panic!("unexpected request: {other:?}"),
    });
    let output = commands::memory_add(
        opts(&server.base_url, false),
        "ask before translating to French".to_string(),
        "cluster",
    )
    .await
    .unwrap();
    let json = output.to_json();
    match output {
        MemoryOutput::Added {
            agent,
            index,
            content,
            source,
            fresh_session_required,
            message_verb,
        } => {
            assert_eq!(agent, "translation-bot");
            assert_eq!(index, 0);
            assert_eq!(content, "ask before translating to French");
            assert_eq!(source, "operator");
            assert!(fresh_session_required);
            assert_eq!(message_verb, "cluster");
            let next = commands::memory_add_next_command(&message_verb);
            assert_eq!(next, r#"curie cluster message "...""#);
            assert!(!next.contains("--continue"));
            assert_eq!(json["next_command"], next);
            assert_eq!(
                commands::memory_add_fresh_session_required_line(),
                "A fresh session is required before this entry is injected at boot."
            );
            let human = commands::memory_add_fresh_session_line(&message_verb);
            assert!(human.contains(&next));
            assert!(human.contains("omit --continue"));
        }
        other => panic!("expected Added, got {other:?}"),
    }

    let flow: Vec<(String, String)> = server
        .recorded()
        .iter()
        .map(|r| (r.method.clone(), r.path.clone()))
        .collect();
    assert_eq!(
        flow,
        vec![
            ("GET".to_string(), "/agents".to_string()),
            ("POST".to_string(), format!("/agents/{AGENT_ID}/memory")),
        ]
    );
}

#[tokio::test]
async fn memory_add_dry_run_makes_no_request() {
    let server = serve(|req| panic!("dry-run must not request: {req:?}"));
    let output = commands::memory_add(
        opts(&server.base_url, true),
        "ask before translating to French".to_string(),
        "cluster",
    )
    .await
    .unwrap();
    match output {
        MemoryOutput::DryRun(plan) => {
            let joined = plan.lines.join("\n");
            assert!(
                joined.contains("POST"),
                "dry-run plan must name the POST: {joined}"
            );
            assert!(
                joined.contains("/memory"),
                "dry-run plan must name the memory surface: {joined}"
            );
            assert!(
                !joined.contains("/state/"),
                "dry-run must not advertise the reserved state-append path: {joined}"
            );
        }
        _ => panic!("expected DryRun"),
    }
    assert!(server.recorded().is_empty());
}

#[tokio::test]
async fn memory_add_rejects_blank_content_without_calling_the_api() {
    let server = serve(|req| panic!("blank content must not request: {req:?}"));
    let err = commands::memory_add(opts(&server.base_url, false), "  \n".to_string(), "cluster")
        .await
        .unwrap_err();
    let message = err.to_string();
    assert!(
        message.to_lowercase().contains("content"),
        "error should name content: {message}"
    );
    assert!(server.recorded().is_empty());
}

#[tokio::test]
async fn memory_add_local_next_command_omits_continue() {
    let server = serve(|req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => agent_list(),
        ("POST", p) if *p == format!("/agents/{AGENT_ID}/memory") => created_entry(),
        other => panic!("unexpected request: {other:?}"),
    });
    let output = commands::memory_add(
        opts(&server.base_url, false),
        "ask before translating to French".to_string(),
        "local",
    )
    .await
    .unwrap();
    let next = commands::memory_add_next_command("local");
    assert_eq!(next, r#"curie local message "...""#);
    assert!(!next.contains("--continue"));
    assert_eq!(output.to_json()["next_command"], next);
    assert_eq!(
        commands::memory_add_fresh_session_required_line(),
        "A fresh session is required before this entry is injected at boot."
    );
    let human = commands::memory_add_fresh_session_line("local");
    assert!(human.contains(&next));
    assert!(human.contains("omit --continue"));
}

#[test]
fn memory_add_json_documents_a_fresh_session() {
    let json = MemoryOutput::Added {
        agent: "translation-bot".to_string(),
        index: 0,
        content: "ask first".to_string(),
        source: "operator".to_string(),
        fresh_session_required: true,
        message_verb: "local".to_string(),
    }
    .to_json();
    let next = commands::memory_add_next_command("local");
    assert_eq!(json["next_command"], next);
    assert_eq!(
        json,
        serde_json::json!({
            "agent": "translation-bot",
            "index": 0,
            "content": "ask first",
            "source": "operator",
            "fresh_session_required": true,
            "next_command": next,
        })
    );
    assert_eq!(json["fresh_session_required"], true);
}

#[test]
fn memory_add_helpers_match_json_next_command() {
    for verb in ["cluster", "local"] {
        let next = commands::memory_add_next_command(verb);
        assert_eq!(next, format!(r#"curie {verb} message "...""#));
        assert!(!next.contains("--continue"));
        let json = MemoryOutput::Added {
            agent: "translation-bot".to_string(),
            index: 0,
            content: "ask first".to_string(),
            source: "operator".to_string(),
            fresh_session_required: true,
            message_verb: verb.to_string(),
        }
        .to_json();
        assert_eq!(json["next_command"], next);
        assert_eq!(
            commands::memory_add_fresh_session_required_line(),
            "A fresh session is required before this entry is injected at boot."
        );
        let human = commands::memory_add_fresh_session_line(verb);
        assert!(human.contains(&next));
        assert!(human.contains("omit --continue"));
    }
}

fn add_server() -> support::MockServer {
    serve(|req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => agent_list(),
        ("POST", p) if *p == format!("/agents/{AGENT_ID}/memory") => created_entry(),
        other => panic!("unexpected request: {other:?}"),
    })
}

/// Explicit `--api-url` / `--api-key` so cluster does not kube-discover.
fn run_memory_add(tier: &str, json: bool, api_url: &str) -> std::process::Output {
    let mut cmd = Command::new(bin());
    cmd.arg("--color=never");
    if json {
        cmd.arg("--json");
    }
    cmd.args([
        tier,
        "memory",
        "translation-bot",
        "--add",
        "ask first",
        "--api-url",
        api_url,
        "--api-key",
        "k",
    ])
    .stdin(Stdio::null())
    .env_remove("CURIE_API_URL")
    .env_remove("CURIE_API_KEY")
    .output()
    .unwrap_or_else(|e| panic!("run curie {tier} memory --add: {e}"))
}

#[test]
fn memory_add_binary_json_next_command_omits_continue() {
    for tier in ["local", "cluster"] {
        let server = add_server();
        let output = run_memory_add(tier, true, &server.base_url);
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(
            output.status.code(),
            Some(0),
            "{tier} memory --add --json must exit 0\nstdout: {stdout}\nstderr: {stderr}"
        );
        let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap_or_else(|e| {
            panic!("{tier} stdout must be one JSON object: {e}\nstdout: {stdout}")
        });
        assert!(
            json.is_object(),
            "{tier} stdout must be a JSON object: {stdout}"
        );
        let next = format!(r#"curie {tier} message "...""#);
        assert_eq!(json["next_command"], next);
        assert!(!next.contains("--continue"));
        assert!(
            !stdout.contains("--continue"),
            "{tier} JSON must not name --continue: {stdout}"
        );
    }
}

#[test]
fn memory_add_binary_human_next_command_omits_continue() {
    for tier in ["local", "cluster"] {
        let server = add_server();
        let output = run_memory_add(tier, false, &server.base_url);
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(
            output.status.code(),
            Some(0),
            "{tier} memory --add must exit 0\nstdout: {stdout}\nstderr: {stderr}"
        );
        assert!(
            stdout.contains("A fresh session is required before this entry is injected at boot."),
            "{tier} human stdout missing fresh-session line: {stdout}"
        );
        let expected =
            format!(r#"Start a new thread with `curie {tier} message "..."` (omit --continue)."#);
        assert!(
            stdout.contains(&expected),
            "{tier} human stdout missing start-thread line: {stdout}"
        );
        let stripped = stdout.replace("omit --continue", "");
        assert!(
            !stripped.contains("--continue"),
            "{tier} human stdout must not advertise --continue except the omit phrase: {stdout}"
        );
    }
}

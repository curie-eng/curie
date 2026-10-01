//! Integration: the runner client against a wire-faithful NDJSON stream.

mod support;

use std::process::Command;

use curie::runner::RunnerClient;
use curie_aci_protocol::{EventType, OutboundEvent, SessionStatus, PROTOCOL_VERSION};
use support::{serve, Response};

fn frame(json: serde_json::Value) -> String {
    serde_json::to_string(&json).unwrap()
}

fn happy_turn() -> Vec<String> {
    vec![
        frame(serde_json::json!({
            "type": "text_delta", "version": PROTOCOL_VERSION, "text": "Looking into it"
        })),
        frame(serde_json::json!({
            "type": "tool_note", "version": PROTOCOL_VERSION, "text": "echo hi", "tool": "Bash"
        })),
        frame(serde_json::json!({
            "type": "side_effect_flag", "version": PROTOCOL_VERSION, "tool": "Bash", "detail": null
        })),
        frame(serde_json::json!({
            "type": "final", "version": PROTOCOL_VERSION, "text": "all done", "status": "done"
        })),
    ]
}

#[test]
fn skill_message_resets_by_default_and_preserves_on_continue() {
    for preserve in [false, true] {
        let server = serve(|request| match request.path.as_str() {
            "/v1/reset" => Response::json(200, "{}"),
            "/v1/event" => Response::ndjson(&happy_turn()),
            path => panic!("unexpected runner request: {path}"),
        });
        let mut command = Command::new(env!("CARGO_BIN_EXE_curie"));
        command.args([
            "--json",
            "skill",
            "message",
            "hi there",
            "--url",
            &server.base_url,
        ]);
        if preserve {
            command.arg("--continue");
        }

        let output = command.output().expect("run curie skill message");
        assert!(
            output.status.success(),
            "skill message failed with preserve={preserve}: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        let requests: Vec<_> = server
            .recorded()
            .iter()
            .map(|request| format!("{} {}", request.method, request.path))
            .collect();
        let expected = if preserve {
            vec!["POST /v1/event"]
        } else {
            vec!["POST /v1/reset", "POST /v1/event"]
        };
        assert_eq!(
            requests, expected,
            "skill message must start fresh unless --continue is present"
        );
    }
}

#[tokio::test]
async fn streams_a_full_turn_in_order() {
    let server = serve(|_req| Response::ndjson(&happy_turn()));
    let client = RunnerClient::new(&server.base_url).unwrap();

    let mut seen = Vec::new();
    let events = client
        .send_event(EventType::Message, "hi there", "U-test", |e| {
            seen.push(e.clone());
        })
        .await
        .unwrap();

    assert_eq!(events.len(), 4);
    assert_eq!(events, seen, "callback order must match returned order");
    assert!(matches!(
        events.first(),
        Some(OutboundEvent::TextDelta { text, .. }) if text == "Looking into it"
    ));
    assert!(matches!(
        events.last(),
        Some(OutboundEvent::Final {
            status: SessionStatus::Done,
            ..
        })
    ));

    // The inbound frame on the wire is the frozen contract's event shape.
    let recorded = server.recorded();
    let body: serde_json::Value = serde_json::from_slice(&recorded[0].body).unwrap();
    assert_eq!(body["kind"], "event");
    assert_eq!(body["type"], "message");
    assert_eq!(body["text"], "hi there");
    assert_eq!(body["user"], "U-test");
    assert!(body["ts"].as_str().unwrap().contains('.'));
}

#[tokio::test]
async fn rejects_an_off_version_stream() {
    let server = serve(|_req| {
        Response::ndjson(&[frame(serde_json::json!({
            "type": "final", "version": "9.9.9", "text": "x", "status": "done"
        }))])
    });
    let client = RunnerClient::new(&server.base_url).unwrap();
    let err = client
        .send_event(EventType::Message, "hi", "U", |_| {})
        .await
        .unwrap_err();
    assert!(err.to_string().contains("invalid ACI outbound frame"));
}

#[tokio::test]
async fn errors_when_the_stream_ends_without_a_final() {
    let server = serve(|_req| {
        Response::ndjson(&[frame(serde_json::json!({
            "type": "text_delta", "version": PROTOCOL_VERSION, "text": "partial"
        }))])
    });
    let client = RunnerClient::new(&server.base_url).unwrap();
    let err = client
        .send_event(EventType::Message, "hi", "U", |_| {})
        .await
        .unwrap_err();
    assert!(err.to_string().contains("without a final frame"));
}

#[tokio::test]
async fn surfaces_http_errors_with_the_body() {
    let server = serve(|_req| Response::json(400, r#"{"error":"invalid event frame"}"#));
    let client = RunnerClient::new(&server.base_url).unwrap();
    let err = client
        .send_event(EventType::Message, "hi", "U", |_| {})
        .await
        .unwrap_err();
    let text = err.to_string();
    assert!(text.contains("400"), "unexpected error: {text}");
    assert!(
        text.contains("invalid event frame"),
        "unexpected error: {text}"
    );
}

#[tokio::test]
async fn status_round_trips() {
    let server = serve(|req| {
        assert_eq!(req.path, "/status");
        Response::json(200, r#"{"status":"done","ready":true,"turn_active":false}"#)
    });
    let client = RunnerClient::new(&server.base_url).unwrap();
    let status = client.status().await.unwrap();
    assert_eq!(status["status"], "done");
    assert_eq!(status["ready"], true);
}

#[test]
fn human_progress_uses_plain_actions_without_changing_answers_or_json() {
    for mode in ["human", "quiet", "json"] {
        let server = serve(|request| match request.path.as_str() {
            "/v1/reset" => Response::json(200, "{}"),
            "/v1/event" => Response::ndjson(&[
                frame(serde_json::json!({"type":"tool_note", "version":PROTOCOL_VERSION,
                    "tool":"mcp__files__fileAttachment", "text":"running tool mcp__files__fileAttachment"})),
                frame(serde_json::json!({"type":"side_effect_flag", "version":PROTOCOL_VERSION,
                    "tool":"mcp__files__fileAttachment", "detail":"mcp__files__fileAttachment failed; check before retry", "failed":true})),
                frame(serde_json::json!({"type":"final", "version":PROTOCOL_VERSION,
                    "text":"Technical answer: mcp__files__fileAttachment", "status":"done"}))
            ]),
            path => panic!("unexpected request: {path}"),
        });
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_curie"));
        cmd.args(["--color", "never"]);
        if mode == "quiet" { cmd.arg("--quiet"); }
        if mode == "json" { cmd.arg("--json"); }
        let output = cmd.args(["skill", "message", "hi", "--url", &server.base_url]).output().unwrap();
        assert!(output.status.success());
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert!(!stderr.contains("mcp__"), "{mode}: {stderr}");
        if mode == "human" {
            assert!(stderr.contains("file attachment"), "{stderr}");
            assert!(stderr.contains("failed; check before retry"), "{stderr}");
            assert!(stderr.contains("possible change"), "{stderr}");
        } else { assert!(!stderr.contains("file attachment"), "{mode}: {stderr}"); }
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert!(stdout.contains("Technical answer: mcp__files__fileAttachment"));
        if mode == "json" { let _: serde_json::Value = serde_json::from_str(&stdout).unwrap(); }
    }
}

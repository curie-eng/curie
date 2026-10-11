//! Integration: `skill status --json` reports the bundle the runner is actually
//! executing (#1087 AC2).
//!
//! The digest is how a user -- or an agent -- confirms that `skill message` and
//! `skill eval` ran the SAME immutable snapshot, so it has to be readable off
//! the real binary's stdout, not merely present on a struct. These tests drive
//! the real `curie` binary against the `support::serve` HTTP stub, so no Docker
//! daemon and no runner image are needed: the recorded state comes from the real
//! `state` type (or, for the pre-#1087 case, from the literal bytes such a file
//! carries on disk), and the only thing mocked is the runner's HTTP peer.

use crate::support;

use std::path::Path;
use std::process::{Command, Output};

use curie::bundle;
use curie::state::{self, RunnerState};
use curie_aci_protocol::PROTOCOL_VERSION;
use support::{serve, Response};

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

/// A canned `/status` body. The CLI leaves the inner `session` unconstrained
/// (it is a frozen ACI contract elsewhere), so any object serves.
fn status_body() -> Response {
    Response::json(200, r#"{"status": "done", "turns": 1}"#)
}

/// `curie skill status --url <stub> --json` from `dir`, returning parsed stdout.
fn skill_status_json(dir: &Path, url: &str) -> serde_json::Value {
    let out = Command::new(bin())
        .current_dir(dir)
        .args(["skill", "status", "--url", url, "--json"])
        .output()
        .expect("run curie skill status");
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    assert!(
        out.status.success(),
        "skill status must succeed against the stub; stderr: {stderr}"
    );
    serde_json::from_str(&stdout).unwrap_or_else(|e| {
        panic!("skill status --json must emit one JSON object: {e}\nstdout: {stdout}\nstderr: {stderr}")
    })
}

fn recorded_runner(base_url: &str, dir: &Path) -> RunnerState {
    RunnerState {
        container_id: "deadbeef0000".into(),
        container_name: format!("curie-1087-{}", std::process::id()),
        image: "curie-runner".into(),
        port: 7245,
        base_url: base_url.to_string(),
        session_id: "local-1".into(),
        plugin_dir: dir.display().to_string(),
        fake_model: true,
        ollama_container: None,
        network: None,
        model_base_url: None,
        bundle_digest: None,
        bundle_snapshot_dir: None,
        connector_containers: Vec::new(),
        connector_network: None,
    }
}

fn frame(value: serde_json::Value) -> String {
    serde_json::to_string(&value).expect("serialize ACI frame")
}

fn done_turn(text: &str) -> Response {
    Response::ndjson(&[
        frame(serde_json::json!({
            "type": "text_delta",
            "version": PROTOCOL_VERSION,
            "text": text,
        })),
        frame(serde_json::json!({
            "type": "final",
            "version": PROTOCOL_VERSION,
            "text": text,
            "status": "done",
        })),
    ])
}

fn skill_message(dir: &Path, url: &str) -> Output {
    Command::new(bin())
        .current_dir(dir)
        .args(["skill", "message", "hello", "--url", url])
        .output()
        .expect("run curie skill message")
}

fn eval_cases(input: &str) -> String {
    serde_json::json!({
        "name": "snapshot suite",
        "cases": [{
            "id": "snapshot case",
            "input": input,
            "grader": {"kind": "contains", "expected": "answer"},
        }],
    })
    .to_string()
}

#[test]
fn skill_message_warns_when_the_editable_bundle_differs_from_the_running_snapshot() {
    let server = serve(
        |request| match (request.method.as_str(), request.path.as_str()) {
            ("POST", "/v1/reset") => Response::json(200, "{}"),
            ("POST", "/v1/event") => done_turn("snapshot reply"),
            _ => Response::json(404, r#"{"error":"unexpected request"}"#),
        },
    );
    let dir = tempfile::tempdir().expect("tempdir");
    let skill = dir.path().join("SKILL.md");
    std::fs::write(&skill, "before the runner started").expect("write initial bundle");
    let snapshot = bundle::snapshot(dir.path()).expect("snapshot initial bundle");
    std::fs::write(&skill, "edited after the runner started").expect("edit bundle");
    state::save(
        dir.path(),
        &RunnerState {
            bundle_digest: Some(snapshot.digest),
            bundle_snapshot_dir: Some(snapshot.dir.display().to_string()),
            ..recorded_runner(&server.base_url, dir.path())
        },
    )
    .expect("record the runner");

    let output = skill_message(dir.path(), &server.base_url);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "skill message must still answer from the running snapshot; stderr: {stderr}"
    );
    assert_eq!(
        stdout, "snapshot reply\n",
        "the reply must remain unchanged"
    );
    assert!(
        stderr.contains("editable bundle differs from the running snapshot"),
        "the source edit must be named before it is mistaken for a running change: {stderr}"
    );
    assert!(
        stderr.contains("curie skill up"),
        "the warning must name the command that loads the edited bundle: {stderr}"
    );
    assert!(
        !stderr.contains("curie skill up --replace"),
        "a verified same-bundle edit reloads with plain skill up; do not send the user to --replace: {stderr}"
    );
}

#[test]
fn skill_message_does_not_warn_when_the_editable_bundle_matches_the_running_snapshot() {
    let server = serve(
        |request| match (request.method.as_str(), request.path.as_str()) {
            ("POST", "/v1/reset") => Response::json(200, "{}"),
            ("POST", "/v1/event") => done_turn("snapshot reply"),
            _ => Response::json(404, r#"{"error":"unexpected request"}"#),
        },
    );
    let dir = tempfile::tempdir().expect("tempdir");
    let skill = dir.path().join("SKILL.md");
    std::fs::write(&skill, "bundle used by the runner").expect("write bundle");
    let snapshot = bundle::snapshot(dir.path()).expect("snapshot bundle");
    state::save(
        dir.path(),
        &RunnerState {
            bundle_digest: Some(snapshot.digest),
            bundle_snapshot_dir: Some(snapshot.dir.display().to_string()),
            ..recorded_runner(&server.base_url, dir.path())
        },
    )
    .expect("record the runner");

    let output = skill_message(dir.path(), &server.base_url);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "skill message must answer from the matching snapshot; stderr: {stderr}"
    );
    assert_eq!(stdout, "snapshot reply\n");
    assert!(
        !stderr.contains("editable bundle differs from the running snapshot"),
        "a matching source must not produce a mismatch warning: {stderr}"
    );
}

#[test]
fn skill_eval_uses_cases_from_the_running_snapshot_by_default() {
    let server = serve(
        |request| match (request.method.as_str(), request.path.as_str()) {
            ("POST", "/v1/reset") => Response::json(200, "{}"),
            ("POST", "/v1/event") => done_turn("answer"),
            _ => Response::json(404, r#"{"error":"unexpected request"}"#),
        },
    );
    let dir = tempfile::tempdir().expect("tempdir");
    let cases_path = dir.path().join("evals/cases.json");
    std::fs::create_dir_all(cases_path.parent().expect("evals parent")).expect("create evals");
    std::fs::write(&cases_path, eval_cases("snapshot input")).expect("write initial cases");
    let snapshot = bundle::snapshot(dir.path()).expect("snapshot initial bundle");
    std::fs::write(&cases_path, eval_cases("edited source input")).expect("edit cases");
    state::save(
        dir.path(),
        &RunnerState {
            bundle_digest: Some(snapshot.digest),
            bundle_snapshot_dir: Some(snapshot.dir.display().to_string()),
            ..recorded_runner(&server.base_url, dir.path())
        },
    )
    .expect("record the runner");

    let output = Command::new(bin())
        .current_dir(dir.path())
        .args(["skill", "eval", "--json"])
        .output()
        .expect("run curie skill eval");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "skill eval must run the recorded snapshot suite; stderr: {stderr}"
    );
    let body: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("skill eval must emit JSON");
    assert_eq!(body["cases"][0]["id"], "snapshot case", "{body}");

    let events = server
        .recorded()
        .into_iter()
        .filter(|request| request.path == "/v1/event")
        .collect::<Vec<_>>();
    assert_eq!(events.len(), 1, "the suite must send one case");
    let event: serde_json::Value =
        serde_json::from_slice(&events[0].body).expect("parse eval event");
    assert_eq!(
        event["text"], "snapshot input",
        "default eval must use the case the running snapshot contains"
    );
}

#[test]
fn skill_status_json_reports_the_recorded_bundle_digest() {
    let server = serve(|_req| status_body());
    let dir = tempfile::tempdir().expect("tempdir");
    let digest = "c".repeat(64);
    state::save(
        dir.path(),
        &RunnerState {
            bundle_digest: Some(digest.clone()),
            bundle_snapshot_dir: Some(
                dir.path()
                    .join(".curie/snapshots")
                    .join(&digest)
                    .display()
                    .to_string(),
            ),
            ..recorded_runner(&server.base_url, dir.path())
        },
    )
    .expect("record the runner");

    let value = skill_status_json(dir.path(), &server.base_url);

    assert_eq!(
        value["bundle_digest"],
        serde_json::json!(digest),
        "stdout must report the digest recorded in .curie/runner.json: {value}"
    );
    assert_eq!(value["url"], serde_json::json!(server.base_url), "{value}");
    assert_eq!(
        value["session"]["status"],
        serde_json::json!("done"),
        "{value}"
    );
}

#[test]
fn skill_status_json_reports_null_for_a_runner_other_than_the_recorded_one() {
    // Two live stubs: one standing in for the recorded runner, one for the
    // arbitrary runner `--url` points at. The recorded digest describes the
    // former, so attaching it to the latter's session would be a false
    // statement on the key AC2 rests on.
    let recorded_peer = serve(|_req| status_body());
    let other_peer = serve(|_req| status_body());
    let dir = tempfile::tempdir().expect("tempdir");
    let digest = "d".repeat(64);
    state::save(
        dir.path(),
        &RunnerState {
            bundle_digest: Some(digest.clone()),
            bundle_snapshot_dir: Some(
                dir.path()
                    .join(".curie/snapshots")
                    .join(&digest)
                    .display()
                    .to_string(),
            ),
            ..recorded_runner(&recorded_peer.base_url, dir.path())
        },
    )
    .expect("record the runner");

    let value = skill_status_json(dir.path(), &other_peer.base_url);

    assert_eq!(
        value["url"],
        serde_json::json!(other_peer.base_url),
        "the status shown is the runner --url named: {value}"
    );
    assert!(
        value
            .get("bundle_digest")
            .is_some_and(serde_json::Value::is_null),
        "a foreign runner's session must not be married to the local digest \
         (expected null, never {digest}): {value}"
    );

    // And the same invocation against the runner that WAS recorded still
    // reports it, so the guard above is a match test rather than a blanket
    // suppression whenever --url is passed.
    let value = skill_status_json(dir.path(), &recorded_peer.base_url);
    assert_eq!(
        value["bundle_digest"],
        serde_json::json!(digest),
        "--url at the recorded runner still reports its digest: {value}"
    );
}

#[test]
fn skill_status_with_an_explicit_url_survives_an_unreadable_local_state() {
    // Before #1087 `skill status --url X` never read local state at all.
    // A corrupt `.curie/runner.json` (which `state::load` reports as a hard
    // error) must not regress that path into a failure.
    let server = serve(|_req| status_body());
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::create_dir_all(dir.path().join(".curie")).unwrap();
    std::fs::write(dir.path().join(".curie/runner.json"), "{not json at all").unwrap();

    let value = skill_status_json(dir.path(), &server.base_url);

    assert_eq!(
        value["session"]["status"],
        serde_json::json!("done"),
        "an explicit --url must still reach the runner: {value}"
    );
    assert!(
        value
            .get("bundle_digest")
            .is_some_and(serde_json::Value::is_null),
        "an unreadable record proves nothing about the bundle, so: null: {value}"
    );
}

#[test]
fn skill_status_json_reports_null_with_no_recorded_digest() {
    let server = serve(|_req| status_body());

    // A record written before #1087: the two keys are simply absent from the
    // file. Written as literal bytes because that is what is on disk in the
    // wild, and the point is that it parses rather than erroring.
    let older = tempfile::tempdir().expect("tempdir");
    std::fs::create_dir_all(older.path().join(".curie")).unwrap();
    std::fs::write(
        older.path().join(".curie/runner.json"),
        format!(
            r#"{{
  "container_id": "abc123",
  "container_name": "curie-runner-local",
  "image": "curie-runner",
  "port": 7245,
  "base_url": "{}",
  "session_id": "local-1",
  "plugin_dir": "{}",
  "fake_model": true
}}"#,
            server.base_url,
            older.path().display()
        ),
    )
    .unwrap();

    let value = skill_status_json(older.path(), &server.base_url);
    assert!(
        value.get("bundle_digest").is_some(),
        "the key must always be present so an agent can read it unconditionally: {value}"
    );
    assert!(
        value["bundle_digest"].is_null(),
        "a pre-#1087 record reports null, never a fabricated digest: {value}"
    );

    // No record at all, with an explicit --url (edge case E10): pointing
    // `skill status` at an arbitrary runner is a supported path and must not
    // error just because nothing local is recorded.
    let bare = tempfile::tempdir().expect("tempdir");
    let value = skill_status_json(bare.path(), &server.base_url);
    assert!(
        value
            .get("bundle_digest")
            .is_some_and(serde_json::Value::is_null),
        "no recorded runner reports a null digest, not a missing key or a crash: {value}"
    );
    assert_eq!(
        value["session"]["status"],
        serde_json::json!("done"),
        "{value}"
    );
}

//! `curie skill eval --json` carries one raw record per trial, not only the
//! reduced case verdict.
//!
//! A multi-sample run used to report one `output` and two counts per case, so
//! a reviewer could not see which draw failed, what it replied, or which tools
//! it called. This drives the REAL binary against a recorded runner mock whose
//! two draws disagree: the first calls two tools and passes, the second calls
//! none and fails. The per-trial records must show exactly that, in order,
//! next to the unchanged aggregate fields, and the run must name the bundle it
//! graded.
//!
//! The runner is a separate service over HTTP/NDJSON, so `support::serve` is
//! the established external-seam mock here, as in `eval_case_selector.rs`.

use crate::support;

use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use curie::state::{self, RunnerState};
use curie_aci_protocol::PROTOCOL_VERSION;
use support::{serve, Response};

const BUNDLE: &str = "acme-bot";
const CASE_ID: &str = "fetches-the-forecast";
const RECORDED_DIGEST: &str = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";
const PASSING_REPLY: &str = "the forecast is sunny";
const FAILING_REPLY: &str = "i could not reach the service";

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn output_text(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned() + &String::from_utf8_lossy(&output.stderr)
}

fn frame(json: serde_json::Value) -> String {
    serde_json::to_string(&json).unwrap()
}

/// Draw 1: two tool notes, then a reply the grader accepts.
fn passing_turn() -> Vec<String> {
    vec![
        frame(serde_json::json!({
            "type": "tool_note", "version": PROTOCOL_VERSION,
            "text": "running tool WebSearch", "tool": "WebSearch"
        })),
        frame(serde_json::json!({
            "type": "tool_note", "version": PROTOCOL_VERSION,
            "text": "running tool WebFetch", "tool": "WebFetch"
        })),
        frame(serde_json::json!({
            "type": "final", "version": PROTOCOL_VERSION,
            "text": PASSING_REPLY, "status": "done"
        })),
    ]
}

/// Draw 2: no tool at all, and a reply the grader rejects.
fn failing_turn() -> Vec<String> {
    vec![frame(serde_json::json!({
        "type": "final", "version": PROTOCOL_VERSION,
        "text": FAILING_REPLY, "status": "done"
    }))]
}

/// A runner whose first turn passes and every later turn fails.
fn runner_mock() -> support::MockServer {
    let turns = Arc::new(AtomicUsize::new(0));
    serve(move |req| {
        if req.path.starts_with("/v1/event") {
            if turns.fetch_add(1, Ordering::SeqCst) == 0 {
                Response::ndjson(&passing_turn())
            } else {
                Response::ndjson(&failing_turn())
            }
        } else {
            Response::json(200, "{}")
        }
    })
}

/// `curie init` into a temp dir, then a one-case suite graded on "sunny".
fn scaffold(dir: &Path) -> std::path::PathBuf {
    let out = dir.join(BUNDLE);
    let output = Command::new(bin())
        .arg("init")
        .arg(BUNDLE)
        .arg("--dir")
        .arg(&out)
        .stdin(std::process::Stdio::null())
        .output()
        .expect("run curie init");
    assert!(
        output.status.success(),
        "init must scaffold\n{}",
        output_text(&output)
    );
    let suite = serde_json::json!({
        "name": "trials",
        "cases": [{
            "id": CASE_ID,
            "input": "what is the forecast",
            "grader": {"kind": "contains", "expected": "sunny", "case_sensitive": false},
        }],
    });
    std::fs::write(
        out.join("evals/cases.json"),
        serde_json::to_string_pretty(&suite).unwrap(),
    )
    .expect("write the suite");
    out
}

/// Record the runner exactly as `skill up` does, including the snapshot digest
/// it mounted. The snapshot dir is the bundle itself, so the suite resolves.
fn record_runner(bundle: &Path, base_url: &str) {
    state::save(
        bundle,
        &RunnerState {
            container_id: "c0ffee".into(),
            container_name: format!("curie-{BUNDLE}"),
            image: "curie-runner".into(),
            port: 8080,
            base_url: base_url.to_string(),
            session_id: "s1".into(),
            plugin_dir: bundle.display().to_string(),
            fake_model: false,
            ollama_container: None,
            network: None,
            model_base_url: None,
            bundle_digest: Some(RECORDED_DIGEST.into()),
            bundle_snapshot_dir: Some(bundle.display().to_string()),
            connector_containers: Vec::new(),
            connector_network: None,
        },
    )
    .expect("record runner state");
}

fn load_eval_schema() -> serde_json::Value {
    let path = format!("{}/schema/eval.schema.json", env!("CARGO_MANIFEST_DIR"));
    serde_json::from_str(&std::fs::read_to_string(path).expect("eval schema")).unwrap()
}

#[test]
fn a_sampled_skill_eval_reports_every_trial_with_reply_tools_and_verdict() {
    let dir = tempfile::tempdir().unwrap();
    let server = runner_mock();
    let bundle = scaffold(dir.path());
    record_runner(&bundle, &server.base_url);

    let output = Command::new(bin())
        .args(["skill", "eval", "--samples", "2", "--json"])
        .current_dir(&bundle)
        .stdin(std::process::Stdio::null())
        .output()
        .expect("run curie skill eval");
    // 1/2 under majority is a red case, so the gate exits 1 with a payload.
    assert_eq!(output.status.code(), Some(1), "{}", output_text(&output));
    let body: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("--json emits one object even on red");

    assert!(
        jsonschema::validator_for(&load_eval_schema())
            .expect("schema compiles")
            .is_valid(&body),
        "the payload must validate against eval.schema.json: {body}"
    );
    assert_eq!(
        body["bundle_digest"], RECORDED_DIGEST,
        "the run names the bundle it graded: {body}"
    );

    let case = &body["cases"][0];
    // The aggregate fields stay exactly as before.
    assert_eq!(case["id"], CASE_ID, "{body}");
    assert_eq!(case["outcome"], "fail", "{body}");
    assert_eq!(case["samples"], 2, "{body}");
    assert_eq!(case["passes"], 1, "{body}");

    let trials = case["trials"]
        .as_array()
        .unwrap_or_else(|| panic!("each case carries a trials array: {body}"));
    assert_eq!(trials.len(), 2, "one record per draw: {body}");

    let first = &trials[0];
    assert_eq!(first["index"], 0, "{body}");
    assert_eq!(first["outcome"], "pass", "{body}");
    assert_eq!(first["passed"], true, "{body}");
    assert_eq!(first["reply"], PASSING_REPLY, "{body}");
    assert_eq!(first["status"], "done", "{body}");
    assert_eq!(
        first["tool_calls"],
        serde_json::json!([{"name": "WebSearch"}, {"name": "WebFetch"}]),
        "the tools the first draw called, in emission order: {body}"
    );
    assert!(first["seconds"].as_f64().is_some(), "{body}");

    let second = &trials[1];
    assert_eq!(second["index"], 1, "{body}");
    assert_eq!(second["outcome"], "fail", "{body}");
    assert_eq!(second["passed"], false, "{body}");
    assert_eq!(second["reply"], FAILING_REPLY, "{body}");
    assert_eq!(
        second["tool_calls"],
        serde_json::json!([]),
        "an observed turn with no tool call is an empty list, not null: {body}"
    );
}

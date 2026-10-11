//! Issue #2007: `curie skill eval --case-id <ID>` narrows the suite, and the
//! narrowed suite is what actually runs.
//!
//! These drive the REAL `curie` binary against a recorded runner mock, so the
//! assertions are on the process's own exit code and on the requests the runner
//! actually received. That second half is the load-bearing one: a unit test that
//! re-derives the exit rule from a row vector stays green under a mutation that
//! passes the UNFILTERED suite into `run_suite_cases`, because the rule it
//! checks never sees the suite. The recorded-request assertions here are what
//! make that mutation red.
//!
//! The exit-code contract under test:
//!   0 -- no selected case failed (an all-`plumbing_ok` run also exits 0,
//!        so 0 alone is not a pass),
//!   1 -- a selected case failed,
//!   2 -- the selector matched nothing (before any runner contact).
//!
//! On the mock: `support::serve` is the established external-seam mock (the
//! runner is a separate service over HTTP/NDJSON) and it records every request,
//! which is how "only the selected case's input reached the runner" is provable.
//! The suite is written into a bundle scaffolded by the real `curie init`
//! binary; unlike `fake_tier_plumbing.rs` this file needs a deliberate
//! pass-case/fail-case PAIR, which the one-case seed cannot provide.

use crate::support;

use std::path::Path;
use std::process::Command;

use curie::state::{self, RunnerState};
use curie_aci_protocol::PROTOCOL_VERSION;
use support::{serve, Request, Response};

const BUNDLE: &str = "deal-desk";

/// The two cases every test here selects between. The mock answers every turn
/// with the same "hello there", so `greets-the-user` passes and `escalates`
/// fails -- a fixed pair, with the SELECTOR as the only variable.
const PASSING_ID: &str = "greets-the-user";
const FAILING_ID: &str = "escalates";
const PASSING_INPUT: &str = "say hello to the user";
const FAILING_INPUT: &str = "escalate this to a human";
const CANNED_REPLY: &str = "hello there";

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn output_text(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned() + &String::from_utf8_lossy(&output.stderr)
}

fn frame(json: serde_json::Value) -> String {
    serde_json::to_string(&json).unwrap()
}

/// One completed turn whose text satisfies the `greets-the-user` grader and
/// fails the `escalates` one, whatever the input.
fn canned_turn() -> Vec<String> {
    vec![frame(serde_json::json!({
        "type": "final", "version": PROTOCOL_VERSION,
        "text": CANNED_REPLY, "status": "done"
    }))]
}

fn runner_mock() -> support::MockServer {
    serve(|req| {
        if req.path.starts_with("/v1/event") {
            Response::ndjson(&canned_turn())
        } else {
            Response::json(200, "{}")
        }
    })
}

/// `curie init <BUNDLE>` into a temp dir via the real binary, so everything
/// around the suite (manifest, skill, `.curie/`) is the shipped shape.
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
    out
}

/// Replace the one-case seed with the pass/fail pair. `evals/cases.json` is read
/// live from source (never from the snapshot), so this is the suite the run
/// grades.
fn write_pair_suite(bundle: &Path) {
    let suite = serde_json::json!({
        "name": "selector",
        "cases": [
            {
                "id": PASSING_ID,
                "input": PASSING_INPUT,
                "grader": {"kind": "contains", "expected": "hello", "case_sensitive": false},
            },
            {
                "id": FAILING_ID,
                "input": FAILING_INPUT,
                "grader": {"kind": "contains", "expected": "escalating", "case_sensitive": false},
            },
        ],
    });
    std::fs::write(
        bundle.join("evals/cases.json"),
        serde_json::to_string_pretty(&suite).unwrap(),
    )
    .expect("write the pair suite");
}

/// Record the runner `skill eval` will drive, exactly as `skill up` does.
/// `fake_model: false` keeps the run on the GRADED path -- a fake runner reports
/// `plumbing_ok` and never fails, which would make the exit-1 case unreachable.
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
            bundle_digest: None,
            bundle_snapshot_dir: None,
            connector_containers: Vec::new(),
            connector_network: None,
        },
    )
    .expect("record runner state");
}

fn skill_eval(bundle: &Path, case_ids: &[&str], json: bool) -> std::process::Output {
    let mut cmd = Command::new(bin());
    cmd.arg("skill").arg("eval").current_dir(bundle);
    for id in case_ids {
        cmd.arg("--case-id").arg(id);
    }
    if json {
        cmd.arg("--json");
    }
    cmd.stdin(std::process::Stdio::null())
        .output()
        .expect("run curie skill eval")
}

/// The case inputs the runner was actually asked to run, in order.
fn turn_inputs(requests: &[Request]) -> Vec<String> {
    requests
        .iter()
        .filter(|req| req.path.starts_with("/v1/event"))
        .map(|req| {
            let body: serde_json::Value =
                serde_json::from_slice(&req.body).expect("the runner is sent a JSON event frame");
            body["text"].as_str().unwrap_or_default().to_string()
        })
        .collect()
}

/// A bundle whose suite is the pass/fail pair, wired to a recorded mock runner.
fn fixture(dir: &Path) -> (std::path::PathBuf, support::MockServer) {
    let server = runner_mock();
    let bundle = scaffold(dir);
    write_pair_suite(&bundle);
    record_runner(&bundle, &server.base_url);
    (bundle, server)
}

/// Exit 0, and -- the assertion the whole file exists for -- ONLY the selected
/// case reached the runner. Passing the unfiltered suite into `run_suite_cases`
/// would still exit 0 here (the extra case is `escalates`, which fails, so it
/// would actually exit 1) *and* would send the unselected input, so both halves
/// catch the mutation; the recorded-input half catches it even for a selection
/// of a passing case out of an all-passing suite.
#[test]
fn a_selected_run_grades_only_the_selected_case_and_exits_zero() {
    let dir = tempfile::tempdir().unwrap();
    let (bundle, server) = fixture(dir.path());

    let output = skill_eval(&bundle, &[PASSING_ID], true);
    assert!(
        output.status.success(),
        "a selected case that passes is exit 0\n{}",
        output_text(&output)
    );

    let inputs = turn_inputs(&server.recorded());
    assert_eq!(
        inputs,
        vec![PASSING_INPUT.to_string()],
        "only the selected case's input may reach the runner: the narrowed suite \
         must be the one that RUNS, not merely the one that is reported"
    );

    let body: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("--json emits one object");
    assert_eq!(
        body["total"], 1,
        "the rollup counts the SELECTED cases, not the suite: {body}"
    );
    assert_eq!(body["passed"], 1, "{body}");
    assert_eq!(body["failed"], 0, "{body}");
    assert_eq!(body["cases"][0]["id"], PASSING_ID, "{body}");
    assert!(body["cases"][1].is_null(), "one row, one case: {body}");
}

/// The unselected control: with no selector both cases run, so the recorded
/// inputs above are a real narrowing rather than an artifact of the suite or the
/// mock. Without this, a run that simply never reached the second case would
/// satisfy the test above.
#[test]
fn an_unselected_run_grades_the_whole_suite() {
    let dir = tempfile::tempdir().unwrap();
    let (bundle, server) = fixture(dir.path());

    let output = skill_eval(&bundle, &[], true);
    assert!(
        !output.status.success(),
        "the full suite contains a failing case\n{}",
        output_text(&output)
    );
    assert_eq!(
        turn_inputs(&server.recorded()),
        vec![PASSING_INPUT.to_string(), FAILING_INPUT.to_string()],
        "an unfiltered run sends every case's input"
    );
    let body: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(body["total"], 2, "{body}");
}

/// Exit 1: the selector narrows to a case the grader fails. A selected run that
/// fails is a failed GATE, not a usage error -- the exit-2 gate sits above the
/// verdict and must not swallow it.
#[test]
fn a_selected_case_that_fails_its_grader_exits_one() {
    let dir = tempfile::tempdir().unwrap();
    let (bundle, server) = fixture(dir.path());

    let output = skill_eval(&bundle, &[FAILING_ID], true);
    assert_eq!(
        output.status.code(),
        Some(1),
        "a failed selected case is exit 1 (Failure), not 2\n{}",
        output_text(&output)
    );
    assert_eq!(
        turn_inputs(&server.recorded()),
        vec![FAILING_INPUT.to_string()],
        "only the selected case's input may reach the runner"
    );
    let body: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("--json emits one object even on red");
    assert_eq!(body["total"], 1, "{body}");
    assert_eq!(body["failed"], 1, "{body}");
    assert_eq!(body["cases"][0]["id"], FAILING_ID, "{body}");
}

/// Exit 2: a selector one character off a real id fails the gate BEFORE any
/// runner contact, and names the mistyped value verbatim so the operator can
/// self-correct. Greening an empty run is the defect #2007 exists to stop.
#[test]
fn a_mistyped_case_id_exits_two_before_the_runner_is_ever_contacted() {
    let dir = tempfile::tempdir().unwrap();
    let (bundle, server) = fixture(dir.path());

    let mistyped = "greets-the-usr";
    let output = skill_eval(&bundle, &[mistyped], false);
    assert_eq!(
        output.status.code(),
        Some(2),
        "an unmatched selector is a usage error, never a green empty run\n{}",
        output_text(&output)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains(mistyped),
        "the refusal must name the mistyped value verbatim:\n{stderr}"
    );
    assert!(
        stderr.contains(PASSING_ID),
        "the refusal must list the suite's real ids:\n{stderr}"
    );
    assert!(
        turn_inputs(&server.recorded()).is_empty(),
        "the gate fires before any case reaches the runner: {:?}",
        server
            .recorded()
            .iter()
            .map(|r| &r.path)
            .collect::<Vec<_>>()
    );
}

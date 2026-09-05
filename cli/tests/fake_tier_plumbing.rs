//! Issues #612 + #606: the fake model is a plumbing fixture, not a subject
//! under test.
//!
//! Two rules are pinned here, both stated as behavior:
//!
//! 1. The fake tier asserts ONLY that the turn completed. It never runs a
//!    content grader, and its outcome is a distinct non-graded `plumbing_ok`
//!    that is neither pass nor fail (`passed: null`).
//! 2. A `--model` sweep against a fake/sealed stack is REFUSED Usage-shaped
//!    (exit 2), never `Unsupported` (exit 4): supplying a credential makes the
//!    verb work, so the concept is not absent by construction (ADR-0041).
//!
//! Written test-first: `evals::CaseOutcome`, `evals::turn_outcome`, and
//! `message::guard_fake_sweep` do not exist yet, so this file does not compile
//! until the implementer builds them.
//!
//! On the runner mock: `support::serve` is the established external-seam mock
//! (the runner is a separate service over HTTP/NDJSON). The frames it serves are
//! the fake's REAL canned turn -- `runner/src/curie_runner/fake.py`
//! `default_turn()` emits exactly a "Looking into it" text delta, a `Bash` tool
//! use, and a `final` with text "all done" and status `done`. The eval cases are
//! the scaffold's REAL seeded `evals/cases.json`, written by the real `curie
//! init` binary and never hand-substituted (#612's AC is explicit on this). The
//! whole defect lives in the collision between those two real artifacts: the
//! seeded grader requires the bundle name and "all done" does not contain it.

mod support;

use std::path::Path;
use std::process::Command;

use curie::evals::{
    load_suite, turn_outcome, CaseOutcome, EvalCase, ExpectedStatus, Grader, GraderKind,
};
use curie::exit::{classify, ExitClass};
use curie::message::guard_fake_sweep;
use curie::state::{self, RunnerState};
use curie_aci_protocol::{OutboundEvent, SessionStatus, PROTOCOL_VERSION};
use support::{serve, Response};

const BUNDLE: &str = "deal-desk";

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn output_text(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned() + &String::from_utf8_lossy(&output.stderr)
}

fn frame(json: serde_json::Value) -> String {
    serde_json::to_string(&json).unwrap()
}

/// `fake.py::default_turn()` on the wire: the canned turn every fake-model run
/// produces, whatever the input. Its graded answer is "all done".
fn fake_canned_turn() -> Vec<String> {
    vec![
        frame(serde_json::json!({
            "type": "text_delta", "version": PROTOCOL_VERSION, "text": "Looking into it"
        })),
        frame(serde_json::json!({
            "type": "tool_note", "version": PROTOCOL_VERSION, "text": "echo hi", "tool": "Bash"
        })),
        frame(serde_json::json!({
            "type": "final", "version": PROTOCOL_VERSION, "text": "all done", "status": "done"
        })),
    ]
}

/// A real-model turn that satisfies the seeded grader: it names the bundle.
fn on_topic_turn() -> Vec<String> {
    vec![frame(serde_json::json!({
        "type": "final", "version": PROTOCOL_VERSION,
        "text": "I am the deal-desk agent.", "status": "done"
    }))]
}

/// `curie init <BUNDLE>` into a temp dir via the real binary, so the eval
/// suite under test is the shipped seed rather than a fixture.
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

/// Record the runner `skill eval` will drive, exactly as `skill up` does: the
/// CLI's own record of the runner it booted is where fake-ness is learned from.
fn record_runner(bundle: &Path, base_url: &str, fake_model: bool) {
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
            fake_model,
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

fn skill_eval(bundle: &Path, json: bool) -> std::process::Output {
    let mut cmd = Command::new(bin());
    cmd.arg("skill").arg("eval").current_dir(bundle);
    if json {
        cmd.arg("--json");
    }
    cmd.stdin(std::process::Stdio::null())
        .output()
        .expect("run curie skill eval")
}

// --- AC1: the scaffold's own documented loop is not red --------------------

/// The documented onboarding loop (`init` -> `skill up --fake-model` ->
/// `skill eval`) exits 0 on an untouched scaffold, and reports the run as
/// non-graded plumbing rather than as a pass. This is #612's headline AC.
///
/// Deleting the impl fails this three ways: grading the fake turn makes the seed
/// red (exit 1), calling it a pass makes `outcome`/`passed` wrong, and dropping
/// the tri-state makes `passed` a boolean.
#[test]
fn the_scaffolded_fake_loop_exits_zero_and_reports_plumbing_not_a_pass() {
    let server = serve(|_req| Response::ndjson(&fake_canned_turn()));
    let dir = tempfile::tempdir().unwrap();
    let bundle = scaffold(dir.path());
    record_runner(&bundle, &server.base_url, true);

    let output = skill_eval(&bundle, true);
    assert!(
        output.status.success(),
        "the scaffold's own documented fake loop must not be red\n{}",
        output_text(&output)
    );

    let body: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("--json emits one object");
    assert_eq!(
        body["plumbing_ok"], 1,
        "the rollup must count the non-graded row: {body}"
    );
    assert_eq!(body["failed"], 0, "a plumbing row is not a failure: {body}");
    let case = &body["cases"][0];
    assert_eq!(
        case["outcome"], "plumbing_ok",
        "the fake row's outcome is neither pass nor fail: {body}"
    );
    assert!(
        case["passed"].is_null(),
        "`passed` is tri-state; a non-graded row claims neither: {body}"
    );
    // #1907: even a plumbing row reports the sampling policy so n=1 is explicit.
    assert_eq!(body["samples"], 1, "{body}");
    assert_eq!(body["policy"], "majority", "{body}");
    assert_eq!(case["samples"], 1, "{body}");
    assert_eq!(case["passes"], 0, "{body}");
    assert_eq!(case["policy"], "majority", "{body}");
}

/// The human rollup for an all-plumbing run must read as plumbing. `1/1 passed`
/// on a run that never graded anything is the exact false green #606 is about,
/// and a human reading the onboarding loop's output is who it would fool.
#[test]
fn the_human_rollup_of_a_fake_run_never_claims_passed() {
    let server = serve(|_req| Response::ndjson(&fake_canned_turn()));
    let dir = tempfile::tempdir().unwrap();
    let bundle = scaffold(dir.path());
    record_runner(&bundle, &server.base_url, true);

    let output = skill_eval(&bundle, false);
    assert!(output.status.success(), "{}", output_text(&output));
    let text = output_text(&output);
    assert!(
        !text.contains("1/1 passed"),
        "a non-graded run must never render as a pass-rate:\n{text}"
    );
    assert!(
        text.to_lowercase().contains("plumbing"),
        "the rollup must name what actually happened:\n{text}"
    );
}

/// Falsifiability guard (#527 / #553 intent): `plumbing_ok` must not become a
/// backdoor to the vacuous grader #527 removed. On the REAL path the seeded
/// grader still fails an off-topic turn -- and "all done" is exactly the
/// off-topic reply, since it never names the bundle.
#[test]
fn the_seeded_grader_still_fails_an_off_topic_turn_on_the_real_path() {
    let server = serve(|_req| Response::ndjson(&fake_canned_turn()));
    let dir = tempfile::tempdir().unwrap();
    let bundle = scaffold(dir.path());
    record_runner(&bundle, &server.base_url, false);

    let output = skill_eval(&bundle, true);
    assert!(
        !output.status.success(),
        "a real-model turn that never names the bundle must stay red\n{}",
        output_text(&output)
    );
    let body: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("--json emits one object even on red");
    assert_eq!(body["failed"], 1, "{body}");
    assert_eq!(body["cases"][0]["outcome"], "fail", "{body}");
    assert_eq!(body["cases"][0]["passed"], false, "{body}");
}

/// The positive control for the guard above: on the real path an on-topic turn
/// passes. Without this, a grader that failed everything would satisfy the
/// falsifiability test and nothing would notice.
#[test]
fn the_seeded_grader_passes_an_on_topic_turn_on_the_real_path() {
    let server = serve(|_req| Response::ndjson(&on_topic_turn()));
    let dir = tempfile::tempdir().unwrap();
    let bundle = scaffold(dir.path());
    record_runner(&bundle, &server.base_url, false);

    let output = skill_eval(&bundle, true);
    assert!(output.status.success(), "{}", output_text(&output));
    let body: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(body["passed"], 1, "{body}");
    assert_eq!(body["cases"][0]["outcome"], "pass", "{body}");
    assert_eq!(body["cases"][0]["passed"], true, "{body}");
}

/// The scaffold's seeded suite is what the loop above actually ran: a
/// falsifiable `contains: <name>` grader. Pins the artifact the whole AC1 story
/// depends on, through the same loader `skill eval` uses.
#[test]
fn the_scaffold_seeds_a_falsifiable_grader_the_fake_reply_does_not_satisfy() {
    let dir = tempfile::tempdir().unwrap();
    let bundle = scaffold(dir.path());
    let suite = load_suite(&bundle.join("evals/cases.json")).expect("the seed loads");
    let case = &suite.cases[0];
    assert_eq!(case.grader.kind, GraderKind::Contains);
    assert_eq!(case.grader.expected, BUNDLE);
    assert!(
        !case.grader.grade("all done", &[]),
        "the seed must stay falsifiable: the fake's canned reply must NOT satisfy it, \
         which is precisely why the fake tier cannot be graded"
    );
}

// --- The grader never runs on a fake turn ----------------------------------

fn case_requiring(expected: &str) -> EvalCase {
    EvalCase {
        id: "c".into(),
        input: "introduce yourself".into(),
        grader: Grader {
            kind: GraderKind::Contains,
            expected: expected.into(),
            case_sensitive: false,
        },
        shared_history: false,
        expect_status: ExpectedStatus::Done,
    }
}

fn final_event(text: &str, status: SessionStatus) -> OutboundEvent {
    OutboundEvent::Final {
        version: PROTOCOL_VERSION.into(),
        text: text.into(),
        status,
        approval_summary: None,
        approval_route: None,
        approval_gate_kind: None,
        approval_granted_tool: None,
        input_tokens: None,
        output_tokens: None,
        adoption_applied: None,
    }
}

/// The mandate, as one assertion: a completed fake turn is `PlumbingOk` even
/// when the grader would FAIL it. The grader is not consulted at all, so its
/// verdict cannot reach the outcome.
#[test]
fn a_completed_fake_turn_is_plumbing_ok_even_when_the_grader_would_fail_it() {
    let case = case_requiring(BUNDLE);
    let events = vec![final_event("all done", SessionStatus::Done)];
    assert!(
        !case.grader.grade("all done", &[]),
        "precondition: this grader fails the fake's canned text"
    );
    assert_eq!(
        turn_outcome(&case, &events, true),
        CaseOutcome::PlumbingOk,
        "a fake turn's outcome must not depend on the grader"
    );
}

/// The converse: a completed fake turn the grader would PASS is still
/// `PlumbingOk`, never `Pass`. A fake run cannot earn a grade in either
/// direction -- otherwise a grader tuned to the canned text (the #612 e2e.sh
/// bypass) manufactures a green.
#[test]
fn a_completed_fake_turn_the_grader_would_pass_is_still_plumbing_ok() {
    let case = case_requiring("all done");
    let events = vec![final_event("all done", SessionStatus::Done)];
    assert!(case.grader.grade("all done", &[]), "precondition");
    assert_eq!(
        turn_outcome(&case, &events, true),
        CaseOutcome::PlumbingOk,
        "a fake run must never be reported as a graded pass"
    );
}

/// Ordering is load-bearing: the `Done` gate runs BEFORE the fake early return.
/// The fake tier's one and only assertion is that the turn completed, so a
/// fake turn that did not complete is a genuine `Fail` -- the one thing this
/// tier must still catch.
#[test]
fn a_fake_turn_that_did_not_complete_is_still_a_fail() {
    let case = case_requiring("all done");
    let failed = vec![final_event("all done", SessionStatus::ClassifiedFailure)];
    assert_eq!(
        turn_outcome(&case, &failed, true),
        CaseOutcome::Fail,
        "a classified-failure fake turn means the plumbing genuinely broke"
    );
    let unfinished: Vec<OutboundEvent> = vec![OutboundEvent::TextDelta {
        version: PROTOCOL_VERSION.into(),
        text: "all done".into(),
        adoption_applied: None,
    }];
    assert_eq!(
        turn_outcome(&case, &unfinished, true),
        CaseOutcome::Fail,
        "a fake turn with no final never completed"
    );
}

/// The real path is unchanged: `turn_outcome(.., fake = false)` is the graded
/// verdict `turn_passes` gave, and never `PlumbingOk`.
#[test]
fn the_real_path_still_grades_and_never_returns_plumbing_ok() {
    let case = case_requiring(BUNDLE);
    let on_topic = vec![final_event(
        "I am the deal-desk agent.",
        SessionStatus::Done,
    )];
    let off_topic = vec![final_event("all done", SessionStatus::Done)];
    assert_eq!(turn_outcome(&case, &on_topic, false), CaseOutcome::Pass);
    assert_eq!(turn_outcome(&case, &off_topic, false), CaseOutcome::Fail);
    let failed = vec![final_event(
        "I am the deal-desk agent.",
        SessionStatus::ClassifiedFailure,
    )];
    assert_eq!(
        turn_outcome(&case, &failed, false),
        CaseOutcome::Fail,
        "the Done gate still precedes the grader on the real path"
    );
}

// --- AC2: fake + `--model` refuses, Usage-shaped ---------------------------

fn models() -> Vec<String> {
    vec!["claude-opus-4-8".into(), "claude-sonnet-5".into()]
}

/// A sweep on a fake stack compares one canned string to itself, so it is
/// refused. The class is the contract an agent branches on: Usage (exit 2),
/// because supplying a credential makes the verb work -- it is not absent by
/// construction, which is ADR-0041's boundary for exit 4.
#[test]
fn a_model_sweep_on_a_fake_local_stack_is_refused_usage_shaped() {
    let err = guard_fake_sweep(true, &models(), true).expect_err("a fake sweep must be refused");
    let (class, fix) = classify(&err);
    assert_eq!(
        class,
        ExitClass::Usage,
        "a credential makes this verb work, so it is Usage (2), not Unsupported (4)"
    );
    let shown = format!("{err:#}");
    assert!(
        shown.to_lowercase().contains("fake"),
        "the message must name the reason the sweep is meaningless: {shown}"
    );
    assert!(
        shown.contains("compare one canned reply to itself")
            && shown.contains("sweeping 2 models")
            && shown.contains("fabricated"),
        "a real sweep's reason is the fabricated comparison axis, counted correctly: {shown}"
    );
    let fix = fix.expect("the refusal must carry an actionable fix");
    assert!(
        fix.contains("CURIE_CREDENTIALS"),
        "the local tier's fix is supplying a credential: {fix}"
    );
}

/// A single `--model` is still refused -- the ruling is "fake + --model", not
/// "fake + a sweep" -- but there is no comparison to fabricate, so the message
/// must give the reason that applies: the caller pinned a model this stack will
/// never call. A refusal that states the wrong reason is the same defect class
/// this whole guard exists to stop.
#[test]
fn a_single_model_on_a_fake_stack_is_refused_for_the_reason_that_applies() {
    let one = vec!["claude-opus-4-8".to_string()];
    let err = guard_fake_sweep(true, &one, true).expect_err("fake + --model is refused at N=1 too");
    let (class, fix) = classify(&err);
    assert_eq!(
        class,
        ExitClass::Usage,
        "N=1 keeps the same class as a sweep: a credential makes it work"
    );
    let shown = format!("{err:#}");
    assert!(
        shown.contains("claude-opus-4-8") && shown.contains("never call"),
        "the reason must be that the pinned model is never called: {shown}"
    );
    assert!(
        !shown.contains("compare")
            && !shown.contains("comparison")
            && !shown.contains("fabricated"),
        "there is no comparison at N=1, so the comparison rationale must not appear: {shown}"
    );
    assert!(
        !shown.contains("1 models"),
        "the message must not read as a plural sweep of one: {shown}"
    );
    assert!(
        fix.expect("the refusal must carry an actionable fix")
            .contains("CURIE_CREDENTIALS"),
        "N=1 keeps the tier's fix hint"
    );
}

/// `cluster eval` funnels through the same seam and gets the same class, with
/// the fix naming the knob that tier actually has.
#[test]
fn a_model_sweep_on_a_fake_cluster_release_is_refused_usage_shaped() {
    let err = guard_fake_sweep(true, &models(), false).expect_err("a fake sweep must be refused");
    let (class, fix) = classify(&err);
    assert_eq!(class, ExitClass::Usage);
    let fix = fix.expect("the refusal must carry an actionable fix");
    assert!(
        fix.contains("fakeModel"),
        "the cluster tier's fix is the chart value, not an env var: {fix}"
    );
}

/// A sweep on a REAL stack is the whole point of #526 ("can we move this skill
/// to a cheaper model") and must not be blocked.
#[test]
fn a_model_sweep_on_a_real_stack_is_allowed() {
    guard_fake_sweep(false, &models(), true).expect("a real stack sweeps");
    guard_fake_sweep(false, &models(), false).expect("a real release sweeps");
}

/// The guard must not swallow the AC1 loop: the default parity-gate run (no
/// `--model`) on a fake stack is exactly the documented onboarding loop and
/// stays allowed. A guard keyed on fake-ness alone would refuse it.
#[test]
fn the_default_parity_gate_run_on_a_fake_stack_is_not_refused() {
    guard_fake_sweep(true, &[], true).expect("the fake parity-gate run is the documented loop");
    guard_fake_sweep(true, &[], false).expect("the fake parity-gate run is the documented loop");
}

/// `--dry-run` is an offline, non-mutating plan: it does not probe the runtime,
/// so it cannot and must not claim what the current stack would do. It stays
/// exit 0 even with a sweep's `--model` flags.
#[test]
fn a_dry_run_sweep_does_not_probe_and_is_not_refused() {
    let dir = tempfile::tempdir().unwrap();
    let bundle = scaffold(dir.path());
    let output = Command::new(bin())
        .arg("cluster")
        .arg("eval")
        .arg("--model")
        .arg("claude-opus-4-8")
        .arg("--model")
        .arg("claude-sonnet-5")
        .arg("--dry-run")
        .current_dir(&bundle)
        .stdin(std::process::Stdio::null())
        .output()
        .expect("run curie cluster eval --dry-run");
    assert!(
        output.status.success(),
        "a dry-run plan must not probe the runtime or refuse\n{}",
        output_text(&output)
    );
}

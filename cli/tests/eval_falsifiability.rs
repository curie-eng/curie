//! Falsifiability gate for the committed eval suites (issue #619).
//!
//! This is a FALSIFIABILITY gate, NOT an end-to-end test: it never runs a real
//! agent or makes a model call. It exercises the frozen graders (the same
//! `curie::evals::Grader::grade` the runner path grades with) against
//! controlled synthetic outputs to prove every committed case is falsifiable --
//! i.e. that a null agent cannot pass it, and that the grader is not simply
//! broken for everything. Capability removal is the separate proof that a
//! case requires its declared capability.
//!
//! Three controls, all offline and deterministic, all driven off the committed
//! suites discovered on disk (so a new suite is covered with no edit here):
//!
//!   A. Negative (null agent): no committed case may green against a canned
//!      response that ignores the input -- the fake model's "all done" final and
//!      a silent empty answer. A case that greens here calls no tool and reads no
//!      input yet passes. This control is useful but does not prove that removing
//!      a required capability makes the case red.
//!      The real-path form of this control (boot the fake runner, run
//!      `curie skill eval`, assert the red rollup) lives in
//!      `cli/scripts/eval-falsifiability.sh` and its CI job; this is the fast,
//!      credential-free mirror.
//!
//!   B. Negative (input-parrot agent, AC4): no committed case's grader may be
//!      satisfied by the case's OWN input verbatim, beyond an explicit baseline
//!      of cases that are input-satisfiable today. This is what catches the
//!      `contains: "weather"` vacuousness class -- a grader the input itself
//!      guarantees -- which the null agent control cannot see (the fake's "all done"
//!      does not contain the input). The baseline keeps the gate green on main
//!      without editing a committed case; any NEW input-satisfiable case fails.
//!
//!   C. Positive (AC2): every committed case's grader must GREEN against a
//!      known-good synthetic exemplar, so the gate cannot be satisfied by every
//!      grader simply being broken. Every discovered case must have an exemplar
//!      (completeness), so a new suite forces one to be supplied.
//!
//! Fixtures (exemplars + input-satisfiable baseline) live in
//! `tests/data/eval_falsifiability_fixtures.json`.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use curie::evals::{load_suite, turn_passes, EvalCase, EvalSuite};
use curie_aci_protocol::{OutboundEvent, SessionStatus, PROTOCOL_VERSION};
use serde::Deserialize;

/// Repo root, resolved from the crate manifest dir (`cli/`).
fn repo_root() -> PathBuf {
    PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/.."))
}

/// The committed eval suites the gate covers, discovered on disk so a new suite
/// is picked up with no edit: every `examples/*/evals/cases.json`, plus the
/// `curie init` scaffold seed at `apps/worker/schema/eval-cases.example.json`.
/// Returns `(key_prefix, suite)` where `key_prefix` is the suite name used to
/// build the stable `<suite>/<case-id>` key.
fn discover_suites() -> Vec<(String, EvalSuite)> {
    let root = repo_root();
    let mut paths: Vec<PathBuf> = Vec::new();

    let examples = root.join("examples");
    let mut entries: Vec<PathBuf> = std::fs::read_dir(&examples)
        .unwrap_or_else(|e| panic!("reading {}: {e}", examples.display()))
        .map(|e| e.expect("dir entry").path())
        .collect();
    entries.sort();
    for dir in entries {
        let cases = dir.join("evals/cases.json");
        if cases.is_file() {
            paths.push(cases);
        }
    }
    paths.push(root.join("apps/worker/schema/eval-cases.example.json"));

    let suites: Vec<(String, EvalSuite)> = paths
        .iter()
        .map(|p| {
            let suite = load_suite(p).unwrap_or_else(|e| panic!("loading {}: {e:#}", p.display()));
            (suite.name.clone(), suite)
        })
        .collect();

    // A vacuously-empty discovery would make every assertion below pass for the
    // wrong reason; assert we actually found the known suites.
    assert!(
        suites.len() >= 3,
        "expected at least 3 committed suites (weather, github-issues, example scaffold seed), \
         found {}: {:?}",
        suites.len(),
        suites.iter().map(|(n, _)| n).collect::<Vec<_>>()
    );
    suites
}

/// The stable per-case key used across the gate and the fixtures file.
fn case_key(suite_name: &str, case: &EvalCase) -> String {
    format!("{suite_name}/{}", case.id)
}

/// A completed turn whose graded final text is `text` (status `done`). Mirrors
/// the shape `graded_answer`/`turn_passes` judge: the final frame's text.
fn done_turn(text: &str) -> Vec<OutboundEvent> {
    vec![OutboundEvent::Final {
        version: PROTOCOL_VERSION.into(),
        text: text.into(),
        status: SessionStatus::Done,
        approval_summary: None,
        approval_route: None,
        approval_gate_kind: None,
        approval_granted_tool: None,
        input_tokens: None,
        output_tokens: None,
        adoption_applied: None,
    }]
}

#[derive(Debug, Deserialize)]
struct Fixtures {
    exemplars: BTreeMap<String, String>,
    input_satisfiable_baseline: BTreeSet<String>,
}

fn fixtures() -> Fixtures {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/data/eval_falsifiability_fixtures.json"
    );
    let raw =
        std::fs::read_to_string(path).unwrap_or_else(|e| panic!("reading fixtures {path}: {e}"));
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("fixtures {path} must be valid JSON: {e}"))
}

/// Control A -- negative, null agent. No committed case may pass against a
/// response that ignores the input entirely. The fake model's final text is
/// "all done"; a silent agent answers with nothing. Either passing a case means
/// the case greens without any real work. This control alone does not prove
/// capability removal falsifiability.
#[test]
fn no_committed_case_passes_against_a_null_agent() {
    let broken_outputs = ["all done", ""];
    let mut offenders: Vec<String> = Vec::new();
    for (name, suite) in discover_suites() {
        for case in &suite.cases {
            for output in broken_outputs {
                if turn_passes(case, &done_turn(output)) {
                    offenders.push(format!(
                        "{} greens on no-op answer {output:?}",
                        case_key(&name, case)
                    ));
                }
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "these committed eval cases pass against a null agent; the null agent \
         control requires them to red, but it does not prove capability removal:\n  {}",
        offenders.join("\n  ")
    );
}

/// Control B -- negative, input-parrot agent (AC4). No committed case's grader
/// may be satisfied by the case's own input verbatim, except the explicit
/// baseline of cases that are input-satisfiable today. Catches the
/// `contains: "weather"` class the no-op control cannot see. Exact set equality
/// keeps the baseline honest: a NEW input-satisfiable case fails, and a stale
/// baseline entry (a case that is no longer input-satisfiable) also fails so the
/// baseline is pruned rather than left to rot.
#[test]
fn no_committed_case_is_input_satisfiable_beyond_the_baseline() {
    let baseline = fixtures().input_satisfiable_baseline;
    let mut actual: BTreeSet<String> = BTreeSet::new();
    for (name, suite) in discover_suites() {
        for case in &suite.cases {
            // The broken agent parrots the prompt back. `turn_passes` requires a
            // completed `done` turn, so grade the parroted input through the same
            // pass condition the runner path uses.
            if turn_passes(case, &done_turn(&case.input)) {
                actual.insert(case_key(&name, case));
            }
        }
    }

    let new_violations: Vec<&String> = actual.difference(&baseline).collect();
    assert!(
        new_violations.is_empty(),
        "these committed eval cases are satisfied by their own input -- a grader the \
         input itself guarantees is vacuous (AC4, #527). Rewrite the grader to require \
         something only a real answer carries:\n  {:?}",
        new_violations
    );

    let stale: Vec<&String> = baseline.difference(&actual).collect();
    assert!(
        stale.is_empty(),
        "these input-satisfiable-baseline entries are no longer input-satisfiable; \
         remove them from tests/data/eval_falsifiability_fixtures.json:\n  {:?}",
        stale
    );
}

/// Control C -- positive (AC2). Every committed case's grader must green against
/// a known-good synthetic exemplar, so the gate is not silently satisfied by
/// every grader being broken. Completeness is enforced: every discovered case
/// must have an exemplar, and every exemplar must map to a real case.
#[test]
fn every_grader_greens_on_its_known_good_exemplar() {
    let exemplars = fixtures().exemplars;
    let mut keys: BTreeSet<String> = BTreeSet::new();
    let mut red: Vec<String> = Vec::new();

    for (name, suite) in discover_suites() {
        for case in &suite.cases {
            let key = case_key(&name, case);
            keys.insert(key.clone());
            let Some(exemplar) = exemplars.get(&key) else {
                continue; // missing-exemplar completeness is asserted below
            };
            // Grade the exemplar through the same pass condition the runner path
            // uses (a completed `done` turn whose final text is the exemplar).
            if !turn_passes(case, &done_turn(exemplar)) {
                red.push(format!(
                    "{key} -> exemplar {exemplar:?} does NOT satisfy its grader"
                ));
            }
        }
    }

    let exemplar_keys: BTreeSet<String> = exemplars.keys().cloned().collect();
    let missing: Vec<&String> = keys.difference(&exemplar_keys).collect();
    assert!(
        missing.is_empty(),
        "these committed cases have no known-good exemplar in \
         tests/data/eval_falsifiability_fixtures.json (add one that satisfies the grader):\n  {:?}",
        missing
    );
    let orphan: Vec<&String> = exemplar_keys.difference(&keys).collect();
    assert!(
        orphan.is_empty(),
        "these exemplars reference a case that no longer exists; remove them:\n  {:?}",
        orphan
    );
    assert!(
        red.is_empty(),
        "these known-good exemplars fail their grader -- the grader rejects a correct \
         answer, or the exemplar drifted:\n  {}",
        red.join("\n  ")
    );
}

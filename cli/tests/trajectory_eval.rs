//! Integration coverage for trajectory scoring on the skill eval surface.
//!
//! These tests drive the built CLI against a wire faithful runner server. The
//! final answer is deliberately made irrelevant to each expected verdict. Only
//! the ordered `tool_note` observations may determine the result.

mod support;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::Arc;

use serde::Deserialize;
use support::{serve, Response};

const STRING_GRADER_EXPECTED: &str = "the string grader says pass";

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn output_text(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned() + &String::from_utf8_lossy(&output.stderr)
}

fn frame(value: serde_json::Value) -> String {
    serde_json::to_string(&value).expect("serialize ACI frame")
}

fn turn(tools: &[String], final_text: &str) -> Vec<String> {
    let mut frames = tools
        .iter()
        .map(|tool| {
            frame(serde_json::json!({
                "type": "tool_note",
                "version": curie_aci_protocol::PROTOCOL_VERSION,
                "text": format!("called {tool}"),
                "tool": tool,
            }))
        })
        .collect::<Vec<_>>();
    frames.push(frame(serde_json::json!({
        "type": "final",
        "version": curie_aci_protocol::PROTOCOL_VERSION,
        "text": final_text,
        "status": "done",
    })));
    frames
}

fn runner(
    observed: BTreeMap<String, Vec<String>>,
    final_text: BTreeMap<String, String>,
) -> support::MockServer {
    let observed = Arc::new(observed);
    let final_text = Arc::new(final_text);
    serve(
        move |request| match (request.method.as_str(), request.path.as_str()) {
            ("POST", "/v1/reset") => Response::json(200, "{}"),
            ("POST", "/v1/event") => {
                let body: serde_json::Value =
                    serde_json::from_slice(&request.body).expect("valid inbound event");
                let input = body["text"].as_str().expect("event text");
                let tools = observed.get(input).expect("known eval input");
                let answer = final_text.get(input).expect("known final answer");
                Response::ndjson(&turn(tools, answer))
            }
            other => panic!("unexpected runner request: {other:?}"),
        },
    )
}

fn cases_bytes(case_ids: &[String]) -> Vec<u8> {
    let cases = case_ids
        .iter()
        .map(|id| {
            serde_json::json!({
                "id": id,
                "input": id,
                "grader": {
                    "kind": "contains",
                    "expected": STRING_GRADER_EXPECTED,
                },
            })
        })
        .collect::<Vec<_>>();
    serde_json::to_vec_pretty(&serde_json::json!({
        "name": "trajectory",
        "cases": cases,
    }))
    .expect("serialize eval suite")
}

fn write_bundle(cases: &[u8], specs: serde_json::Value) -> (tempfile::TempDir, PathBuf) {
    let bundle = tempfile::tempdir().expect("bundle temp directory");
    let evals = bundle.path().join("evals");
    std::fs::create_dir_all(&evals).expect("create eval directory");
    let cases_path = evals.join("cases.json");
    std::fs::write(&cases_path, cases).expect("write cases");
    std::fs::write(
        evals.join("trajectory.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "specs": specs,
        }))
        .expect("serialize trajectory sidecar"),
    )
    .expect("write trajectory sidecar");
    (bundle, cases_path)
}

fn skill_eval(bundle: &Path, server: &support::MockServer) -> Output {
    Command::new(bin())
        .args(["skill", "eval", "--url", &server.base_url, "--json"])
        .current_dir(bundle)
        .stdin(std::process::Stdio::null())
        .output()
        .expect("run skill eval")
}

fn parsed_output(output: &Output) -> serde_json::Value {
    serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "skill eval must emit one JSON result: {error}\n{}",
            output_text(output)
        )
    })
}

fn case_result<'a>(body: &'a serde_json::Value, id: &str) -> &'a serde_json::Value {
    body["cases"]
        .as_array()
        .expect("cases array")
        .iter()
        .find(|case| case["id"] == id)
        .unwrap_or_else(|| panic!("missing result for {id}: {body}"))
}

#[derive(Debug, Deserialize)]
struct VectorFile {
    vectors: Vec<TrajectoryVector>,
}

#[derive(Debug, Deserialize)]
struct TrajectoryVector {
    name: String,
    mode: String,
    expected: Vec<String>,
    observed: Vec<String>,
    threshold: f64,
    passed: bool,
    detail: Option<String>,
}

fn vectors() -> VectorFile {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../tests/vectors/trajectory-match.json");
    let body = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    serde_json::from_str(&body).unwrap_or_else(|error| panic!("parse {}: {error}", path.display()))
}

#[test]
fn skill_eval_uses_tool_order_instead_of_the_final_answer() {
    let ids = vec!["ordered".to_string(), "wrong_order".to_string()];
    let cases = cases_bytes(&ids);
    let specs = serde_json::json!([
        {
            "case_id": "ordered",
            "expected": ["Read", "Bash"],
            "mode": "in_order",
            "threshold": 1.0,
        },
        {
            "case_id": "wrong_order",
            "expected": ["Read", "Bash"],
            "mode": "in_order",
            "threshold": 1.0,
        }
    ]);
    let (bundle, _cases_path) = write_bundle(&cases, specs);
    let observed = BTreeMap::from([
        (
            "ordered".to_string(),
            vec!["Read".to_string(), "Bash".to_string()],
        ),
        (
            "wrong_order".to_string(),
            vec!["Bash".to_string(), "Read".to_string()],
        ),
    ]);
    let final_text = BTreeMap::from([
        (
            "ordered".to_string(),
            "the string grader must fail".to_string(),
        ),
        (
            "wrong_order".to_string(),
            STRING_GRADER_EXPECTED.to_string(),
        ),
    ]);
    let server = runner(observed, final_text);

    let output = skill_eval(bundle.path(), &server);
    assert!(
        !output.status.success(),
        "the wrong order case must make the run fail\n{}",
        output_text(&output)
    );
    let body = parsed_output(&output);
    assert_eq!(case_result(&body, "ordered")["passed"], true, "{body}");
    assert_eq!(case_result(&body, "wrong_order")["passed"], false, "{body}");
    assert!(
        case_result(&body, "wrong_order")["detail"]
            .as_str()
            .is_some_and(|detail| detail.contains("observed")),
        "a wrong order failure must explain the observed trajectory: {body}"
    );
}

#[test]
fn weather_case_fails_when_fetch_capability_is_removed() {
    let weather_evals = Path::new(env!("CARGO_MANIFEST_DIR")).join("../examples/weather/evals");
    let cases_path = weather_evals.join("cases.json");
    let cases = std::fs::read(&cases_path)
        .unwrap_or_else(|error| panic!("read {}: {error}", cases_path.display()));
    let suite: serde_json::Value = serde_json::from_slice(&cases)
        .unwrap_or_else(|error| panic!("parse {}: {error}", cases_path.display()));
    let weather_case = suite["cases"]
        .as_array()
        .and_then(|cases| {
            cases
                .iter()
                .find(|case| case["id"] == "reports-a-temperature")
        })
        .unwrap_or_else(|| {
            panic!(
                "missing reports-a-temperature case in {}",
                cases_path.display()
            )
        });
    let case_id = weather_case["id"]
        .as_str()
        .expect("weather case id")
        .to_string();
    let input = weather_case["input"]
        .as_str()
        .expect("weather case input")
        .to_string();

    let trajectory_path = weather_evals.join("trajectory.json");
    let trajectory: serde_json::Value = serde_json::from_slice(
        &std::fs::read(&trajectory_path)
            .unwrap_or_else(|error| panic!("read {}: {error}", trajectory_path.display())),
    )
    .unwrap_or_else(|error| panic!("parse {}: {error}", trajectory_path.display()));
    let specs = trajectory["specs"].clone();

    let (complete_bundle, _cases_path) = write_bundle(&cases, specs.clone());
    let complete_server = runner(
        BTreeMap::from([(
            input.clone(),
            vec!["WebSearch".to_string(), "WebFetch".to_string()],
        )]),
        BTreeMap::from([(input.clone(), "68 degrees Fahrenheit".to_string())]),
    );
    let complete = skill_eval(complete_bundle.path(), &complete_server);
    assert!(
        complete.status.success(),
        "the committed weather case must pass with search and fetch\n{}",
        output_text(&complete)
    );
    let complete_body = parsed_output(&complete);
    assert_eq!(
        case_result(&complete_body, &case_id)["passed"],
        true,
        "{complete_body}"
    );

    let (without_fetch_bundle, _cases_path) = write_bundle(&cases, specs);
    let without_fetch_server = runner(
        BTreeMap::from([(input.clone(), vec!["WebSearch".to_string()])]),
        BTreeMap::from([(input, "68 degrees Fahrenheit".to_string())]),
    );
    let without_fetch = skill_eval(without_fetch_bundle.path(), &without_fetch_server);
    assert!(
        !without_fetch.status.success(),
        "the committed weather case must fail when fetch is unavailable\n{}",
        output_text(&without_fetch)
    );
    let without_fetch_body = parsed_output(&without_fetch);
    assert_eq!(
        case_result(&without_fetch_body, &case_id)["passed"],
        false,
        "{without_fetch_body}"
    );
    assert!(
        case_result(&without_fetch_body, &case_id)["detail"]
            .as_str()
            .is_some_and(|detail| {
                detail.contains("expected=['WebSearch', 'WebFetch']")
                    && detail.contains("observed=['WebSearch']")
            }),
        "the failure must explain that the fetch capability was absent: {without_fetch_body}"
    );
}

#[test]
fn sre_rollout_case_requires_two_replica_set_reads() {
    let evals = Path::new(env!("CARGO_MANIFEST_DIR")).join("../examples/sre-bot/evals/rollout");
    let cases_path = evals.join("cases.json");
    let cases = std::fs::read(&cases_path)
        .unwrap_or_else(|error| panic!("read {}: {error}", cases_path.display()));
    let suite: serde_json::Value = serde_json::from_slice(&cases).expect("parse rollout cases");
    let case = &suite["cases"][0];
    let input = case["input"].as_str().expect("rollout input").to_string();
    assert!(
        !input.contains("spec.template"),
        "diff must not be pre-supplied"
    );
    assert!(!input.contains("exit 1"), "cause must not be pre-supplied");
    let case_id = case["id"].as_str().expect("rollout case id");
    let trajectory_path = evals.join("trajectory.json");
    let trajectory: serde_json::Value = serde_json::from_slice(
        &std::fs::read(&trajectory_path)
            .unwrap_or_else(|error| panic!("read {}: {error}", trajectory_path.display())),
    )
    .expect("parse rollout trajectory");
    let specs = trajectory["specs"].clone();
    let get = "mcp__kubernetes__resources_get".to_string();

    let (complete_bundle, _) = write_bundle(&cases, specs.clone());
    let complete_server = runner(
        BTreeMap::from([(input.clone(), vec![get.clone(), get.clone()])]),
        BTreeMap::from([(input.clone(), "The templates differ.".to_string())]),
    );
    let complete = skill_eval(complete_bundle.path(), &complete_server);
    assert!(
        complete.status.success(),
        "two reads must pass\n{}",
        output_text(&complete)
    );
    assert_eq!(
        case_result(&parsed_output(&complete), case_id)["passed"],
        true
    );

    let (one_read_bundle, _) = write_bundle(&cases, specs);
    let one_read_server = runner(
        BTreeMap::from([(input.clone(), vec![get])]),
        BTreeMap::from([(input, "The templates differ.".to_string())]),
    );
    let one_read = skill_eval(one_read_bundle.path(), &one_read_server);
    assert!(
        !one_read.status.success(),
        "one read must fail\n{}",
        output_text(&one_read)
    );
    assert_eq!(
        case_result(&parsed_output(&one_read), case_id)["passed"],
        false
    );
}

#[test]
fn skill_eval_replays_the_shared_five_mode_trajectory_vectors() {
    let vectors = vectors().vectors;
    let modes = vectors
        .iter()
        .map(|vector| vector.mode.as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        modes,
        BTreeSet::from(["exact", "in_order", "any_order", "precision", "recall"]),
        "the shared vectors must exercise every trajectory mode"
    );

    let ids = vectors
        .iter()
        .map(|vector| vector.name.clone())
        .collect::<Vec<_>>();
    let cases = cases_bytes(&ids);
    let specs = vectors
        .iter()
        .map(|vector| {
            serde_json::json!({
                "case_id": vector.name,
                "expected": vector.expected,
                "mode": vector.mode,
                "threshold": vector.threshold,
            })
        })
        .collect::<Vec<_>>();
    let (bundle, _cases_path) = write_bundle(&cases, serde_json::json!(specs));

    let observed = vectors
        .iter()
        .map(|vector| (vector.name.clone(), vector.observed.clone()))
        .collect::<BTreeMap<_, _>>();
    let final_text = vectors
        .iter()
        .map(|vector| {
            let text = if vector.passed {
                "the string grader must fail"
            } else {
                STRING_GRADER_EXPECTED
            };
            (vector.name.clone(), text.to_string())
        })
        .collect::<BTreeMap<_, _>>();
    let server = runner(observed, final_text);

    let output = skill_eval(bundle.path(), &server);
    let body = parsed_output(&output);
    for vector in &vectors {
        let result = case_result(&body, &vector.name);
        assert_eq!(
            result["passed"], vector.passed,
            "trajectory vector {} must determine the verdict: {body}",
            vector.name
        );
        match &vector.detail {
            Some(detail) => assert_eq!(
                result["detail"],
                detail.as_str(),
                "Rust detail must conform to the Python vector for {}",
                vector.name
            ),
            None => assert!(
                result["detail"].is_null(),
                "a passing vector has no failure detail: {result}"
            ),
        }
    }

    let expected_success = vectors.iter().all(|vector| vector.passed);
    assert_eq!(
        output.status.success(),
        expected_success,
        "the process status must reflect the vector verdicts\n{}",
        output_text(&output)
    );
}

#[test]
fn skill_eval_fails_closed_when_a_case_has_no_trajectory_spec() {
    let ids = vec!["configured".to_string(), "missing_spec".to_string()];
    let cases = cases_bytes(&ids);
    let specs = serde_json::json!([{
        "case_id": "configured",
        "expected": ["Read"],
        "mode": "exact",
        "threshold": 1.0,
    }]);
    let (bundle, _cases_path) = write_bundle(&cases, specs);
    let observed = BTreeMap::from([
        ("configured".to_string(), vec!["Read".to_string()]),
        ("missing_spec".to_string(), vec!["Read".to_string()]),
    ]);
    let final_text = BTreeMap::from([
        ("configured".to_string(), STRING_GRADER_EXPECTED.to_string()),
        (
            "missing_spec".to_string(),
            STRING_GRADER_EXPECTED.to_string(),
        ),
    ]);
    let server = runner(observed, final_text);

    let output = skill_eval(bundle.path(), &server);
    assert!(
        !output.status.success(),
        "a missing trajectory spec must make the eval fail\n{}",
        output_text(&output)
    );
    let body = parsed_output(&output);
    assert_eq!(case_result(&body, "configured")["passed"], true, "{body}");
    let missing = case_result(&body, "missing_spec");
    assert_eq!(missing["passed"], false, "{body}");
    let detail = missing["detail"]
        .as_str()
        .expect("a missing spec failure has visible detail");
    assert!(detail.contains("no trajectory spec"), "{detail}");
    assert!(detail.contains("missing_spec"), "{detail}");
}

#[test]
fn skill_trajectory_eval_rejects_duplicate_suite_case_ids() {
    let ids = vec!["duplicate".to_string(), "duplicate".to_string()];
    let cases = cases_bytes(&ids);
    let specs = serde_json::json!([{
        "case_id": "duplicate",
        "expected": ["Read"],
        "mode": "exact",
        "threshold": 1.0,
    }]);
    let (bundle, _cases_path) = write_bundle(&cases, specs);
    let server = runner(
        BTreeMap::from([("duplicate".to_string(), vec!["Read".to_string()])]),
        BTreeMap::from([("duplicate".to_string(), STRING_GRADER_EXPECTED.to_string())]),
    );

    let output = skill_eval(bundle.path(), &server);

    assert!(!output.status.success(), "{}", output_text(&output));
    let text = output_text(&output).to_lowercase();
    assert!(text.contains("duplicate"), "{text}");
    assert!(text.contains("case id"), "{text}");
}

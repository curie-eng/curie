//! Read-only release-acceptance evaluator (#2430).
//!
//! Scores a private evidence ledger against the seven-day / 200-canary
//! campaign contract. This module does not start a campaign, rotate
//! credentials, mutate the permanent soak, merge, or close the issue.

use std::path::Path;

use anyhow::{Context, Result};
use serde_json::{json, Value};
use time::format_description::well_known::Rfc3339;
use time::{Duration, OffsetDateTime};

use crate::exit::CliError;
use crate::ui::{CliOutput, Ui};

pub const MIN_CANARIES: u64 = 200;
const WINDOW_KIND: &str = "half-open";
const INCLUDED_ISSUES: [&str; 5] = ["2425", "2426", "2427", "2428", "2429"];

pub const CRITERION_LEDGER_PRESENT: &str = "ledger-present";
pub const CRITERION_WINDOW_PINNED: &str = "window-pinned";
pub const CRITERION_CANDIDATE_PINNED: &str = "candidate-pinned";
pub const CRITERION_WORKLOAD_PINNED: &str = "workload-pinned";
pub const CRITERION_PERCENTILE_METHOD_PINNED: &str = "percentile-method-pinned";
pub const CRITERION_SEVEN_DAY_WINDOW: &str = "seven-day-window";
pub const CRITERION_UNCHANGED_CANDIDATE: &str = "unchanged-candidate";
pub const CRITERION_SCHEDULED_RUNS_ACCOUNTED: &str = "scheduled-runs-accounted";
pub const CRITERION_KNOWN_TASK_RESULTS: &str = "known-task-results";
pub const CRITERION_CANARY_COUNT: &str = "canary-count";
pub const CRITERION_NO_LOST_REPLY: &str = "no-lost-reply";
pub const CRITERION_NO_DUPLICATE_REPLY: &str = "no-duplicate-reply";
pub const CRITERION_OUTBOX_NOT_STALE: &str = "outbox-not-stale";
pub const CRITERION_RETRY_CLASS_CONTROLLED: &str = "retry-class-controlled";
pub const CRITERION_ROTATION_PROOF: &str = "rotation-proof";
pub const CRITERION_RECOVERY_DRILL_PROOF: &str = "recovery-drill-proof";
pub const CRITERION_UPGRADE_DRILL_PROOF: &str = "upgrade-drill-proof";
pub const CRITERION_RESTORE_DRILL_PROOF: &str = "restore-drill-proof";
pub const CRITERION_OBSERVABILITY_PROOF: &str = "observability-proof";
pub const CRITERION_CANARY_DETERMINISM_PROOF: &str = "canary-determinism-proof";
pub const CRITERION_LIVE_EVIDENCE: &str = "live-evidence";
pub const CRITERION_LIFECYCLE_REPRODUCED: &str = "lifecycle-reproduced";
pub const CRITERION_LIFECYCLE_FIXED: &str = "lifecycle-fixed";
pub const CRITERION_LIFECYCLE_RELEASED: &str = "lifecycle-released";
pub const CRITERION_LIFECYCLE_DEPLOYED: &str = "lifecycle-deployed";
pub const CRITERION_LIFECYCLE_OBSERVED: &str = "lifecycle-observed";
pub const CRITERION_INCLUDED_RUNTIME_CHANGES: &str = "included-runtime-changes";

const PRIVATE_KEYS: &[&str] = &[
    "body",
    "text",
    "message",
    "channel",
    "channel_id",
    "thread_ts",
    "conversation_id",
    "slack_ts",
    "payload",
];

/// How to treat `evidence_kind` on a ledger.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EvaluateMode {
    /// A live release result. Fixture or source ledgers fail closed.
    Live,
    /// Evaluator self-test. A qualifying fixture may pass without claiming live.
    FixtureSelfTest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowPin {
    pub start: String,
    pub end: String,
    pub kind: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CandidatePin {
    pub cli: String,
    pub chart: String,
    pub image: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkloadPin {
    pub name: String,
    pub model: String,
    pub cadence: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lifecycle {
    pub reproduced: bool,
    pub fixed: bool,
    pub released: bool,
    pub deployed: bool,
    pub observed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Criterion {
    pub id: String,
    pub status: String,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelfTestCase {
    pub name: String,
    pub passed: bool,
    pub expected_missing: Vec<String>,
    pub actual_missing: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleaseAcceptOutput {
    pub qualified: bool,
    pub mode: String,
    pub window: Option<WindowPin>,
    pub candidate: Option<CandidatePin>,
    pub workload: Option<WorkloadPin>,
    pub percentile_method: Option<String>,
    pub evidence_kind: Option<String>,
    pub criteria: Vec<Criterion>,
    pub missing: Vec<String>,
    pub lifecycle: Lifecycle,
    pub self_test_cases: Vec<SelfTestCase>,
}

impl CliOutput for ReleaseAcceptOutput {
    fn to_json(&self) -> Value {
        json!({
            "qualified": self.qualified,
            "mode": self.mode,
            "window": self.window.as_ref().map(|w| json!({
                "start": w.start,
                "end": w.end,
                "kind": w.kind,
            })),
            "candidate": self.candidate.as_ref().map(|c| json!({
                "cli": c.cli,
                "chart": c.chart,
                "image": c.image,
            })),
            "workload": self.workload.as_ref().map(|w| json!({
                "name": w.name,
                "model": w.model,
                "cadence": w.cadence,
            })),
            "percentile_method": self.percentile_method,
            "evidence_kind": self.evidence_kind,
            "criteria": self.criteria.iter().map(|c| json!({
                "id": c.id,
                "status": c.status,
                "detail": c.detail,
            })).collect::<Vec<_>>(),
            "missing": self.missing,
            "lifecycle": {
                "reproduced": self.lifecycle.reproduced,
                "fixed": self.lifecycle.fixed,
                "released": self.lifecycle.released,
                "deployed": self.lifecycle.deployed,
                "observed": self.lifecycle.observed,
            },
            "self_test_cases": self.self_test_cases.iter().map(|c| json!({
                "name": c.name,
                "passed": c.passed,
                "expected_missing": c.expected_missing,
                "actual_missing": c.actual_missing,
            })).collect::<Vec<_>>(),
        })
    }

    fn render(&self, ui: &Ui) {
        if self.qualified {
            ui.success("release acceptance criteria met");
        } else {
            ui.failure("release acceptance failed closed");
        }
        ui.kv("mode", &self.mode);
        if let Some(window) = &self.window {
            ui.kv(
                "window",
                &format!("{} .. {} ({})", window.start, window.end, window.kind),
            );
        }
        if let Some(candidate) = &self.candidate {
            ui.kv(
                "candidate",
                &format!(
                    "cli={} chart={} image={}",
                    candidate.cli, candidate.chart, candidate.image
                ),
            );
        }
        if let Some(workload) = &self.workload {
            ui.kv(
                "workload",
                &format!(
                    "name={} model={} cadence={}",
                    workload.name, workload.model, workload.cadence
                ),
            );
        }
        if let Some(method) = &self.percentile_method {
            ui.kv("percentile", method);
        }
        if let Some(kind) = &self.evidence_kind {
            ui.kv("evidence", kind);
        }
        if self.missing.is_empty() {
            ui.kv("missing", "(none)");
        } else {
            ui.kv("missing", &self.missing.join(", "));
        }
        for case in &self.self_test_cases {
            let mark = if case.passed { "pass" } else { "fail" };
            ui.note(&format!("{mark}  {}", case.name));
        }
    }
}

/// A synthetic fully qualifying fixture ledger. `evidence_kind` is `fixture`
/// so live mode still refuses it.
pub fn qualifying_ledger() -> Value {
    json!({
        "window": {
            "start": "2026-01-01T00:00:00Z",
            "end": "2026-01-08T00:00:00Z",
            "kind": "half-open"
        },
        "candidate": {
            "cli": "curie 0.8.8+placeholder",
            "chart": "0.8.8",
            "image": "ghcr.io/curie-eng/curie-runner:0.8.8"
        },
        "workload": {
            "name": "soak-canary",
            "model": "fake-model",
            "cadence": "30m"
        },
        "percentile_method": "nearest-rank",
        "evidence_kind": "fixture",
        "scheduled": {
            "expected": 336,
            "accounted": 336,
            "unknown": 0,
            "missed": 0
        },
        "canaries": {
            "completed": 200,
            "lost": 0,
            "duplicate": 0
        },
        "outbox": {
            "state": "empty",
            "stranded": false
        },
        "retry_class": {
            "controlled": true
        },
        "rotation": {
            "proven": true
        },
        "drills": {
            "recovery": true,
            "upgrade": true,
            "restore": true,
            "observability": true,
            "canary": true
        },
        "lifecycle": {
            "reproduced": true,
            "fixed": true,
            "released": true,
            "deployed": true,
            "observed": true
        },
        "included_issues": ["2425", "2426", "2427", "2428", "2429"],
        "identities": [
            {
                "at": "2026-01-01T00:00:00Z",
                "cli": "curie 0.8.8+placeholder",
                "chart": "0.8.8",
                "image": "ghcr.io/curie-eng/curie-runner:0.8.8"
            },
            {
                "at": "2026-01-07T23:00:00Z",
                "cli": "curie 0.8.8+placeholder",
                "chart": "0.8.8",
                "image": "ghcr.io/curie-eng/curie-runner:0.8.8"
            }
        ]
    })
}

/// Evaluate one ledger at the current UTC instant.
pub fn evaluate(ledger: &Value, mode: EvaluateMode) -> ReleaseAcceptOutput {
    evaluate_at(ledger, mode, OffsetDateTime::now_utc())
}

/// Evaluate one ledger. `ledger` may be JSON `null` when no file exists.
pub fn evaluate_at(ledger: &Value, mode: EvaluateMode, now: OffsetDateTime) -> ReleaseAcceptOutput {
    let mut criteria = Vec::new();
    let mut missing = Vec::new();

    let present = ledger.is_object();
    record(
        &mut criteria,
        &mut missing,
        CRITERION_LEDGER_PRESENT,
        present,
        if present {
            "ledger object present"
        } else {
            "no evidence ledger; current window is unmet"
        },
    );

    let window = parse_window(ledger);
    record(
        &mut criteria,
        &mut missing,
        CRITERION_WINDOW_PINNED,
        window.is_some(),
        match &window {
            Some(w) => format!("{} .. {} ({})", w.start, w.end, w.kind),
            None => "half-open window start/end/kind not pinned".into(),
        },
    );

    let duration = window.as_ref().and_then(window_duration);
    let elapsed = match mode {
        EvaluateMode::FixtureSelfTest => true,
        EvaluateMode::Live => window
            .as_ref()
            .and_then(parse_window_end)
            .is_some_and(|end| end <= now),
    };
    let seven_days = duration.is_some_and(|d| d >= Duration::days(7)) && elapsed;
    record(
        &mut criteria,
        &mut missing,
        CRITERION_SEVEN_DAY_WINDOW,
        seven_days,
        if seven_days {
            "window spans at least seven days and has elapsed"
        } else if !elapsed {
            "live window end is still in the future"
        } else {
            "window is shorter than seven days or unparseable"
        },
    );

    let candidate = parse_candidate(ledger.get("candidate").unwrap_or(&Value::Null));
    record(
        &mut criteria,
        &mut missing,
        CRITERION_CANDIDATE_PINNED,
        candidate.is_some(),
        if candidate.is_some() {
            "cli/chart/image identities pinned"
        } else {
            "candidate cli/chart/image not pinned"
        },
    );

    let unchanged = candidate.as_ref().is_some_and(|pinned| {
        window
            .as_ref()
            .is_some_and(|window| identities_match(ledger, pinned, window))
    });
    record(
        &mut criteria,
        &mut missing,
        CRITERION_UNCHANGED_CANDIDATE,
        unchanged,
        if unchanged {
            "candidate identity unchanged across the window"
        } else {
            "candidate changed during the window or identities are missing"
        },
    );

    let workload = parse_workload(ledger.get("workload").unwrap_or(&Value::Null));
    record(
        &mut criteria,
        &mut missing,
        CRITERION_WORKLOAD_PINNED,
        workload.is_some(),
        if workload.is_some() {
            "workload name, model, and cadence pinned"
        } else {
            "workload name/model/cadence not pinned"
        },
    );

    let percentile = ledger
        .get("percentile_method")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    record(
        &mut criteria,
        &mut missing,
        CRITERION_PERCENTILE_METHOD_PINNED,
        percentile.is_some(),
        percentile
            .as_deref()
            .unwrap_or("percentile method not pinned"),
    );

    let scheduled = ledger.get("scheduled").cloned().unwrap_or(Value::Null);
    let expected = required_u64(&scheduled, "expected");
    let accounted = required_u64(&scheduled, "accounted");
    let unknown = required_u64(&scheduled, "unknown");
    let missed = required_u64(&scheduled, "missed");
    let derived = derived_scheduled_count(duration, workload.as_ref());
    let scheduled_ok = matches!(
        (expected, accounted, missed, derived),
        (Some(expected), Some(accounted), Some(0), Some(derived))
            if expected == accounted && expected == derived && derived > 0
    );
    record(
        &mut criteria,
        &mut missing,
        CRITERION_SCHEDULED_RUNS_ACCOUNTED,
        scheduled_ok,
        if scheduled_ok {
            "every scheduled opportunity accounted for".into()
        } else {
            format!(
                "scheduled missed={missed:?} accounted={accounted:?} expected={expected:?} derived={derived:?}"
            )
        },
    );
    record(
        &mut criteria,
        &mut missing,
        CRITERION_KNOWN_TASK_RESULTS,
        unknown == Some(0),
        match unknown {
            Some(0) => "no unknown task results".into(),
            Some(count) => format!("unknown task results: {count}"),
            None => "unknown task-result count unobserved".into(),
        },
    );

    let canaries = ledger.get("canaries").cloned().unwrap_or(Value::Null);
    let completed = required_u64(&canaries, "completed");
    let lost = required_u64(&canaries, "lost");
    let duplicate = required_u64(&canaries, "duplicate");
    record(
        &mut criteria,
        &mut missing,
        CRITERION_CANARY_COUNT,
        completed.is_some_and(|count| count >= MIN_CANARIES),
        match completed {
            Some(count) => format!("{count} completed canaries (need {MIN_CANARIES})"),
            None => "completed canary count unobserved".into(),
        },
    );
    record(
        &mut criteria,
        &mut missing,
        CRITERION_NO_LOST_REPLY,
        lost == Some(0),
        match lost {
            Some(0) => "no lost replies".into(),
            Some(count) => format!("lost replies: {count}"),
            None => "lost-reply count unobserved".into(),
        },
    );
    record(
        &mut criteria,
        &mut missing,
        CRITERION_NO_DUPLICATE_REPLY,
        duplicate == Some(0),
        match duplicate {
            Some(0) => "no duplicate replies".into(),
            Some(count) => format!("duplicate replies: {count}"),
            None => "duplicate-reply count unobserved".into(),
        },
    );

    let outbox = ledger.get("outbox").cloned().unwrap_or(Value::Null);
    let outbox_ok = outbox.get("state").and_then(Value::as_str) == Some("empty")
        && outbox.get("stranded").and_then(Value::as_bool) == Some(false);
    record(
        &mut criteria,
        &mut missing,
        CRITERION_OUTBOX_NOT_STALE,
        outbox_ok,
        if outbox_ok {
            "completion outbox empty and not stranded"
        } else {
            "completion outbox is stale, stranded, or unobserved"
        },
    );

    let retry_ok = ledger
        .pointer("/retry_class/controlled")
        .and_then(Value::as_bool)
        == Some(true);
    record(
        &mut criteria,
        &mut missing,
        CRITERION_RETRY_CLASS_CONTROLLED,
        retry_ok,
        if retry_ok {
            "retry class is controlled"
        } else {
            "uncontrolled retry class"
        },
    );

    record(
        &mut criteria,
        &mut missing,
        CRITERION_ROTATION_PROOF,
        ledger.pointer("/rotation/proven").and_then(Value::as_bool) == Some(true),
        "channel-token rotation proof",
    );
    record(
        &mut criteria,
        &mut missing,
        CRITERION_RECOVERY_DRILL_PROOF,
        drill(ledger, "recovery"),
        "isolated recovery drill (#2425)",
    );
    record(
        &mut criteria,
        &mut missing,
        CRITERION_UPGRADE_DRILL_PROOF,
        drill(ledger, "upgrade"),
        "isolated upgrade drill (#2426)",
    );
    record(
        &mut criteria,
        &mut missing,
        CRITERION_RESTORE_DRILL_PROOF,
        drill(ledger, "restore"),
        "isolated restore drill (#2427)",
    );
    record(
        &mut criteria,
        &mut missing,
        CRITERION_OBSERVABILITY_PROOF,
        drill(ledger, "observability"),
        "retained observability (#2428)",
    );
    record(
        &mut criteria,
        &mut missing,
        CRITERION_CANARY_DETERMINISM_PROOF,
        drill(ledger, "canary"),
        "deterministic canary (#2429)",
    );

    let evidence_kind = ledger
        .get("evidence_kind")
        .and_then(Value::as_str)
        .map(str::to_string);
    let live_ok = match mode {
        EvaluateMode::FixtureSelfTest => true,
        EvaluateMode::Live => evidence_kind.as_deref() == Some("live"),
    };
    record(
        &mut criteria,
        &mut missing,
        CRITERION_LIVE_EVIDENCE,
        live_ok,
        match mode {
            EvaluateMode::FixtureSelfTest => {
                "fixture self-test does not claim a live release result".into()
            }
            EvaluateMode::Live => format!(
                "live release requires evidence_kind=live (got {})",
                evidence_kind.as_deref().unwrap_or("absent")
            ),
        },
    );

    let lifecycle = parse_lifecycle(ledger.get("lifecycle").unwrap_or(&Value::Null));
    record(
        &mut criteria,
        &mut missing,
        CRITERION_LIFECYCLE_REPRODUCED,
        lifecycle.reproduced,
        "lifecycle stage reproduced",
    );
    record(
        &mut criteria,
        &mut missing,
        CRITERION_LIFECYCLE_FIXED,
        lifecycle.fixed,
        "lifecycle stage fixed",
    );
    record(
        &mut criteria,
        &mut missing,
        CRITERION_LIFECYCLE_RELEASED,
        lifecycle.released,
        "lifecycle stage released",
    );
    record(
        &mut criteria,
        &mut missing,
        CRITERION_LIFECYCLE_DEPLOYED,
        lifecycle.deployed,
        "lifecycle stage deployed",
    );
    record(
        &mut criteria,
        &mut missing,
        CRITERION_LIFECYCLE_OBSERVED,
        lifecycle.observed,
        "lifecycle stage observed",
    );

    let included = included_issues(ledger);
    let included_ok = INCLUDED_ISSUES
        .iter()
        .all(|issue| included.iter().any(|item| item == issue));
    record(
        &mut criteria,
        &mut missing,
        CRITERION_INCLUDED_RUNTIME_CHANGES,
        included_ok,
        if included_ok {
            "included runtime changes #2425/#2426/#2427/#2428/#2429 recorded".into()
        } else {
            format!("included issues {included:?} missing one of {INCLUDED_ISSUES:?}")
        },
    );

    ReleaseAcceptOutput {
        qualified: missing.is_empty(),
        mode: match mode {
            EvaluateMode::Live => "live".into(),
            EvaluateMode::FixtureSelfTest => "fixture-self-test".into(),
        },
        window,
        candidate,
        workload,
        percentile_method: percentile,
        evidence_kind,
        criteria,
        missing,
        lifecycle,
        self_test_cases: Vec::new(),
    }
}

/// Independent fail-closed cases the self-test must reject.
pub fn independent_failures() -> Vec<(&'static str, Value, &'static str)> {
    let mut cases = Vec::new();
    cases.push((
        "missing-scheduled-run",
        mutate(qualifying_ledger(), |ledger| {
            ledger["scheduled"]["missed"] = json!(1);
            ledger["scheduled"]["accounted"] = json!(335);
        }),
        CRITERION_SCHEDULED_RUNS_ACCOUNTED,
    ));
    cases.push((
        "unknown-task-result",
        mutate(qualifying_ledger(), |ledger| {
            ledger["scheduled"]["unknown"] = json!(1);
        }),
        CRITERION_KNOWN_TASK_RESULTS,
    ));
    cases.push((
        "changed-candidate",
        mutate(qualifying_ledger(), |ledger| {
            ledger["identities"][1]["image"] = json!("ghcr.io/curie-eng/curie-runner:other");
        }),
        CRITERION_UNCHANGED_CANDIDATE,
    ));
    cases.push((
        "missing-rotation-proof",
        mutate(qualifying_ledger(), |ledger| {
            ledger["rotation"]["proven"] = json!(false);
        }),
        CRITERION_ROTATION_PROOF,
    ));
    cases.push((
        "missing-recovery-drill",
        mutate(qualifying_ledger(), |ledger| {
            ledger["drills"]["recovery"] = json!(false);
        }),
        CRITERION_RECOVERY_DRILL_PROOF,
    ));
    cases.push((
        "missing-upgrade-drill",
        mutate(qualifying_ledger(), |ledger| {
            ledger["drills"]["upgrade"] = json!(false);
        }),
        CRITERION_UPGRADE_DRILL_PROOF,
    ));
    cases.push((
        "missing-restore-drill",
        mutate(qualifying_ledger(), |ledger| {
            ledger["drills"]["restore"] = json!(false);
        }),
        CRITERION_RESTORE_DRILL_PROOF,
    ));
    cases.push((
        "stale-outbox",
        mutate(qualifying_ledger(), |ledger| {
            ledger["outbox"]["state"] = json!("retry");
            ledger["outbox"]["stranded"] = json!(true);
        }),
        CRITERION_OUTBOX_NOT_STALE,
    ));
    cases.push((
        "lost-reply",
        mutate(qualifying_ledger(), |ledger| {
            ledger["canaries"]["lost"] = json!(1);
        }),
        CRITERION_NO_LOST_REPLY,
    ));
    cases.push((
        "duplicate-reply",
        mutate(qualifying_ledger(), |ledger| {
            ledger["canaries"]["duplicate"] = json!(1);
        }),
        CRITERION_NO_DUPLICATE_REPLY,
    ));
    cases.push((
        "short-window",
        mutate(qualifying_ledger(), |ledger| {
            ledger["window"]["end"] = json!("2026-01-04T00:00:00Z");
        }),
        CRITERION_SEVEN_DAY_WINDOW,
    ));
    cases.push((
        "under-canary-count",
        mutate(qualifying_ledger(), |ledger| {
            ledger["canaries"]["completed"] = json!(199);
        }),
        CRITERION_CANARY_COUNT,
    ));
    cases.push((
        "uncontrolled-retry-class",
        mutate(qualifying_ledger(), |ledger| {
            ledger["retry_class"]["controlled"] = json!(false);
        }),
        CRITERION_RETRY_CLASS_CONTROLLED,
    ));
    cases.push((
        "missing-observed-lifecycle",
        mutate(qualifying_ledger(), |ledger| {
            ledger["lifecycle"]["observed"] = json!(false);
        }),
        CRITERION_LIFECYCLE_OBSERVED,
    ));
    cases.push((
        "missing-percentile-method",
        mutate(qualifying_ledger(), |ledger| {
            ledger
                .as_object_mut()
                .expect("object")
                .remove("percentile_method");
        }),
        CRITERION_PERCENTILE_METHOD_PINNED,
    ));
    cases.push((
        "omitted-unknown-count",
        mutate(qualifying_ledger(), |ledger| {
            ledger
                .get_mut("scheduled")
                .and_then(Value::as_object_mut)
                .expect("scheduled")
                .remove("unknown");
        }),
        CRITERION_KNOWN_TASK_RESULTS,
    ));
    cases.push((
        "omitted-lost-count",
        mutate(qualifying_ledger(), |ledger| {
            ledger
                .get_mut("canaries")
                .and_then(Value::as_object_mut)
                .expect("canaries")
                .remove("lost");
        }),
        CRITERION_NO_LOST_REPLY,
    ));
    cases.push((
        "zero-scheduled-counters",
        mutate(qualifying_ledger(), |ledger| {
            ledger["scheduled"]["expected"] = json!(0);
            ledger["scheduled"]["accounted"] = json!(0);
        }),
        CRITERION_SCHEDULED_RUNS_ACCOUNTED,
    ));
    cases.push((
        "identity-outside-window",
        mutate(qualifying_ledger(), |ledger| {
            ledger["identities"] = json!([{
                "at": "2025-12-01T00:00:00Z",
                "cli": "curie 0.8.8+placeholder",
                "chart": "0.8.8",
                "image": "ghcr.io/curie-eng/curie-runner:0.8.8"
            }]);
        }),
        CRITERION_UNCHANGED_CANDIDATE,
    ));
    cases.push((
        "missing-workload-name",
        mutate(qualifying_ledger(), |ledger| {
            ledger
                .get_mut("workload")
                .and_then(Value::as_object_mut)
                .expect("workload")
                .remove("name");
        }),
        CRITERION_WORKLOAD_PINNED,
    ));
    cases
}

pub fn run_self_test() -> ReleaseAcceptOutput {
    let qualifying = evaluate(&qualifying_ledger(), EvaluateMode::FixtureSelfTest);
    let mut cases = vec![SelfTestCase {
        name: "qualifying".into(),
        passed: qualifying.qualified,
        expected_missing: Vec::new(),
        actual_missing: qualifying.missing.clone(),
    }];
    for (name, ledger, expected) in independent_failures() {
        let out = evaluate(&ledger, EvaluateMode::FixtureSelfTest);
        let actual = out.missing.clone();
        let passed = !out.qualified && actual.iter().any(|id| id == expected);
        cases.push(SelfTestCase {
            name: name.into(),
            passed,
            expected_missing: vec![expected.into()],
            actual_missing: actual,
        });
    }

    let source = mutate(qualifying_ledger(), |ledger| {
        ledger["evidence_kind"] = json!("source");
    });
    let source_out = evaluate(&source, EvaluateMode::Live);
    cases.push(SelfTestCase {
        name: "source-only-as-live".into(),
        passed: !source_out.qualified
            && source_out
                .missing
                .iter()
                .any(|id| id == CRITERION_LIVE_EVIDENCE),
        expected_missing: vec![CRITERION_LIVE_EVIDENCE.into()],
        actual_missing: source_out.missing.clone(),
    });

    let fixture_live = evaluate(&qualifying_ledger(), EvaluateMode::Live);
    cases.push(SelfTestCase {
        name: "fixture-as-live".into(),
        passed: !fixture_live.qualified
            && fixture_live
                .missing
                .iter()
                .any(|id| id == CRITERION_LIVE_EVIDENCE),
        expected_missing: vec![CRITERION_LIVE_EVIDENCE.into()],
        actual_missing: fixture_live.missing.clone(),
    });

    let now = OffsetDateTime::parse("2026-01-08T00:00:00Z", &Rfc3339).expect("fixed now");
    let mut live_ok = qualifying_ledger();
    live_ok["evidence_kind"] = json!("live");
    let synthetic_live = evaluate_at(&live_ok, EvaluateMode::Live, now);
    cases.push(SelfTestCase {
        name: "synthetic-live-qualifying".into(),
        passed: synthetic_live.qualified,
        expected_missing: Vec::new(),
        actual_missing: synthetic_live.missing.clone(),
    });

    let mut future = qualifying_ledger();
    future["evidence_kind"] = json!("live");
    future["window"]["start"] = json!("2026-01-10T00:00:00Z");
    future["window"]["end"] = json!("2026-01-17T00:00:00Z");
    future["identities"] = json!([
        {
            "at": "2026-01-10T00:00:00Z",
            "cli": "curie 0.8.8+placeholder",
            "chart": "0.8.8",
            "image": "ghcr.io/curie-eng/curie-runner:0.8.8"
        },
        {
            "at": "2026-01-16T23:00:00Z",
            "cli": "curie 0.8.8+placeholder",
            "chart": "0.8.8",
            "image": "ghcr.io/curie-eng/curie-runner:0.8.8"
        }
    ]);
    let future_out = evaluate_at(&future, EvaluateMode::Live, now);
    cases.push(SelfTestCase {
        name: "future-window-as-live".into(),
        passed: !future_out.qualified
            && future_out
                .missing
                .iter()
                .any(|id| id == CRITERION_SEVEN_DAY_WINDOW),
        expected_missing: vec![CRITERION_SEVEN_DAY_WINDOW.into()],
        actual_missing: future_out.missing.clone(),
    });

    let mut private = qualifying_ledger();
    private["body"] = json!("private-message-body-token");
    private["channel"] = json!("C0EXAMPLE1");
    let private_out = evaluate(&private, EvaluateMode::FixtureSelfTest);
    let rendered = private_out.to_json().to_string();
    cases.push(SelfTestCase {
        name: "private-fields-excluded".into(),
        passed: !rendered.contains("private-message-body-token")
            && !contains_private_keys(&private_out.to_json()),
        expected_missing: Vec::new(),
        actual_missing: if rendered.contains("private-message-body-token") {
            vec!["private-body-leaked".into()]
        } else {
            Vec::new()
        },
    });

    let mut aggregate = qualifying;
    aggregate.mode = "fixture-self-test".into();
    aggregate.self_test_cases = cases;
    aggregate.qualified = aggregate.self_test_cases.iter().all(|c| c.passed);
    aggregate.missing = aggregate
        .self_test_cases
        .iter()
        .filter(|c| !c.passed)
        .map(|c| c.name.clone())
        .collect();
    aggregate
}

pub fn run(ledger: Option<&Path>, self_test: bool) -> Result<()> {
    if self_test && ledger.is_some() {
        return Err(
            CliError::usage("--self-test and --ledger cannot be combined")
                .with_fix("run `curie dev release-accept --self-test` or pass --ledger PATH")
                .into(),
        );
    }
    let ui = crate::ui::ui();
    if self_test {
        let out = run_self_test();
        return finish(ui, out, "release-accept self-test failed");
    }
    let path = match ledger.map(Path::to_path_buf).or_else(default_ledger_path) {
        Some(path) => path,
        None => {
            let out = evaluate(&Value::Null, EvaluateMode::Live);
            return finish(
                ui,
                out,
                "no evidence ledger; current window does not qualify",
            );
        }
    };
    let raw = std::fs::read_to_string(&path)
        .with_context(|| format!("reading ledger {}", path.display()))
        .map_err(|err| {
            CliError::usage(format!("{err:#}"))
                .with_fix("pass --ledger PATH to a JSON evidence file, or run --self-test")
        })?;
    let parsed: Value = serde_json::from_str(&raw).map_err(|err| {
        CliError::usage(format!("ledger {} is not JSON: {err}", path.display()))
            .with_fix("supply a JSON object ledger with pinned window and candidate fields")
    })?;
    let out = evaluate(&parsed, EvaluateMode::Live);
    finish(
        ui,
        out,
        "release acceptance failed closed with missing criteria",
    )
}

fn finish(ui: &Ui, out: ReleaseAcceptOutput, failure: &str) -> Result<()> {
    if out.qualified {
        ui.emit(&out);
        return Ok(());
    }
    Err(ui.failed_report(
        &out,
        CliError::failure(format!("{failure}: {}", out.missing.join(", ")))
            .with_fix(
                "attach live campaign evidence or keep the issue open until the seven-day window qualifies",
            )
            .into(),
    ))
}

fn default_ledger_path() -> Option<std::path::PathBuf> {
    std::env::var_os("CURIE_RELEASE_ACCEPT_LEDGER").map(std::path::PathBuf::from)
}

fn record(
    criteria: &mut Vec<Criterion>,
    missing: &mut Vec<String>,
    id: &str,
    ok: bool,
    detail: impl Into<String>,
) {
    let detail = detail.into();
    if ok {
        criteria.push(Criterion {
            id: id.to_string(),
            status: "met".into(),
            detail,
        });
    } else {
        criteria.push(Criterion {
            id: id.to_string(),
            status: "missing".into(),
            detail,
        });
        missing.push(id.to_string());
    }
}

fn parse_window(ledger: &Value) -> Option<WindowPin> {
    let window = ledger.get("window")?;
    let start = window.get("start")?.as_str().filter(|s| !s.is_empty())?;
    let end = window.get("end")?.as_str().filter(|s| !s.is_empty())?;
    let kind = window.get("kind")?.as_str().filter(|s| *s == WINDOW_KIND)?;
    Some(WindowPin {
        start: start.to_string(),
        end: end.to_string(),
        kind: kind.to_string(),
    })
}

fn window_duration(window: &WindowPin) -> Option<Duration> {
    let start = OffsetDateTime::parse(&window.start, &Rfc3339).ok()?;
    let end = OffsetDateTime::parse(&window.end, &Rfc3339).ok()?;
    if end <= start {
        return None;
    }
    Some(end - start)
}

fn parse_window_end(window: &WindowPin) -> Option<OffsetDateTime> {
    OffsetDateTime::parse(&window.end, &Rfc3339).ok()
}

fn parse_window_start(window: &WindowPin) -> Option<OffsetDateTime> {
    OffsetDateTime::parse(&window.start, &Rfc3339).ok()
}

fn parse_cadence(cadence: &str) -> Option<Duration> {
    if let Some(minutes) = cadence.strip_suffix('m') {
        let minutes: i64 = minutes.parse().ok()?;
        (minutes > 0).then(|| Duration::minutes(minutes))
    } else if let Some(hours) = cadence.strip_suffix('h') {
        let hours: i64 = hours.parse().ok()?;
        (hours > 0).then(|| Duration::hours(hours))
    } else {
        None
    }
}

fn derived_scheduled_count(
    duration: Option<Duration>,
    workload: Option<&WorkloadPin>,
) -> Option<u64> {
    let duration = duration?;
    let cadence = parse_cadence(&workload?.cadence)?;
    let cadence_secs = cadence.whole_seconds();
    let duration_secs = duration.whole_seconds();
    if cadence_secs <= 0 || duration_secs <= 0 || duration_secs % cadence_secs != 0 {
        return None;
    }
    let slots = duration_secs / cadence_secs;
    u64::try_from(slots).ok().filter(|slots| *slots > 0)
}

fn parse_candidate(value: &Value) -> Option<CandidatePin> {
    let cli = value.get("cli")?.as_str().filter(|s| !s.is_empty())?;
    let chart = value.get("chart")?.as_str().filter(|s| !s.is_empty())?;
    let image = value.get("image")?.as_str().filter(|s| !s.is_empty())?;
    Some(CandidatePin {
        cli: cli.to_string(),
        chart: chart.to_string(),
        image: image.to_string(),
    })
}

fn parse_workload(value: &Value) -> Option<WorkloadPin> {
    let name = value.get("name")?.as_str().filter(|s| !s.is_empty())?;
    let model = value.get("model")?.as_str().filter(|s| !s.is_empty())?;
    let cadence = value.get("cadence")?.as_str().filter(|s| !s.is_empty())?;
    Some(WorkloadPin {
        name: name.to_string(),
        model: model.to_string(),
        cadence: cadence.to_string(),
    })
}

fn parse_lifecycle(value: &Value) -> Lifecycle {
    Lifecycle {
        reproduced: value.get("reproduced").and_then(Value::as_bool) == Some(true),
        fixed: value.get("fixed").and_then(Value::as_bool) == Some(true),
        released: value.get("released").and_then(Value::as_bool) == Some(true),
        deployed: value.get("deployed").and_then(Value::as_bool) == Some(true),
        observed: value.get("observed").and_then(Value::as_bool) == Some(true),
    }
}

fn identities_match(ledger: &Value, pinned: &CandidatePin, window: &WindowPin) -> bool {
    let Some(start) = parse_window_start(window) else {
        return false;
    };
    let Some(end) = parse_window_end(window) else {
        return false;
    };
    let Some(items) = ledger.get("identities").and_then(Value::as_array) else {
        return false;
    };
    let in_window: Vec<&Value> = items
        .iter()
        .filter(|item| {
            item.get("at")
                .and_then(Value::as_str)
                .and_then(|at| OffsetDateTime::parse(at, &Rfc3339).ok())
                .is_some_and(|at| at >= start && at < end)
        })
        .collect();
    if in_window.is_empty() {
        return false;
    }
    in_window.iter().all(|item| {
        parse_candidate(item).is_some_and(|observed| {
            observed.cli == pinned.cli
                && observed.chart == pinned.chart
                && observed.image == pinned.image
        })
    })
}

fn drill(ledger: &Value, name: &str) -> bool {
    ledger
        .get("drills")
        .and_then(|d| d.get(name))
        .and_then(Value::as_bool)
        == Some(true)
}

fn included_issues(ledger: &Value) -> Vec<String> {
    ledger
        .get("included_issues")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn required_u64(value: &Value, key: &str) -> Option<u64> {
    match value.get(key)? {
        Value::Number(number) => number.as_u64(),
        _ => None,
    }
}

fn mutate(mut ledger: Value, edit: impl FnOnce(&mut Value)) -> Value {
    edit(&mut ledger);
    ledger
}

fn contains_private_keys(value: &Value) -> bool {
    match value {
        Value::Object(map) => map.iter().any(|(key, child)| {
            PRIVATE_KEYS.contains(&key.as_str()) || contains_private_keys(child)
        }),
        Value::Array(items) => items.iter().any(contains_private_keys),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn qualifying_fixture_passes_self_test_mode() {
        let out = evaluate(&qualifying_ledger(), EvaluateMode::FixtureSelfTest);
        assert!(out.qualified, "missing {:?}", out.missing);
        assert_eq!(out.evidence_kind.as_deref(), Some("fixture"));
        assert_eq!(
            out.window.as_ref().map(|w| w.kind.as_str()),
            Some("half-open")
        );
        assert_eq!(out.percentile_method.as_deref(), Some("nearest-rank"));
    }

    #[test]
    fn independently_missing_each_named_precondition_fails() {
        for (name, ledger, expected) in independent_failures() {
            let out = evaluate(&ledger, EvaluateMode::FixtureSelfTest);
            assert!(!out.qualified, "{name} must fail closed");
            assert!(
                out.missing.iter().any(|id| id == expected),
                "{name} missing {:?} did not include {expected}",
                out.missing
            );
        }
    }

    #[test]
    fn fixture_and_source_ledgers_do_not_qualify_as_live() {
        let fixture = evaluate(&qualifying_ledger(), EvaluateMode::Live);
        assert!(!fixture.qualified);
        assert!(fixture
            .missing
            .iter()
            .any(|id| id == CRITERION_LIVE_EVIDENCE));

        let mut source = qualifying_ledger();
        source["evidence_kind"] = json!("source");
        let source = evaluate(&source, EvaluateMode::Live);
        assert!(!source.qualified);
        assert!(source
            .missing
            .iter()
            .any(|id| id == CRITERION_LIVE_EVIDENCE));
    }

    #[test]
    fn synthetic_live_ledger_passes_live_mode() {
        let mut live = qualifying_ledger();
        live["evidence_kind"] = json!("live");
        let out = evaluate(&live, EvaluateMode::Live);
        assert!(out.qualified, "missing {:?}", out.missing);
    }

    #[test]
    fn absent_ledger_fails_closed() {
        let out = evaluate(&Value::Null, EvaluateMode::Live);
        assert!(!out.qualified);
        assert!(out.missing.iter().any(|id| id == CRITERION_LEDGER_PRESENT));
        assert!(out.missing.iter().any(|id| id == CRITERION_NO_LOST_REPLY));
        assert!(out
            .missing
            .iter()
            .any(|id| id == CRITERION_NO_DUPLICATE_REPLY));
    }

    #[test]
    fn report_excludes_private_message_fields() {
        let mut ledger = qualifying_ledger();
        ledger["body"] = json!("private-message-body-token");
        ledger["channel"] = json!("C0EXAMPLE1");
        let rendered = evaluate(&ledger, EvaluateMode::FixtureSelfTest)
            .to_json()
            .to_string();
        assert!(!rendered.contains("private-message-body-token"));
        assert!(!contains_private_keys(
            &evaluate(&ledger, EvaluateMode::FixtureSelfTest).to_json()
        ));
    }

    #[test]
    fn self_test_matrix_passes() {
        let out = run_self_test();
        assert!(
            out.qualified,
            "self-test cases failed: {:?}",
            out.self_test_cases
                .iter()
                .filter(|c| !c.passed)
                .map(|c| &c.name)
                .collect::<Vec<_>>()
        );
    }
}

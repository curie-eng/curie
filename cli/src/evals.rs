//! `curie skill eval`: run the bundle's eval cases through the local runner.
//!
//! Cases live in `evals/cases.json` (seeded by `curie init`): a suite OBJECT
//! `{name, cases: [{id, input, grader}]}`, where each grader is one of
//! `kind: exact | contains | regex | tool_called` with an `expected` string and
//! an optional `case_sensitive` flag. The three text matchers grade the final
//! answer text; `tool_called` grades the turn's tool-call trajectory, asserting
//! the tool named in `expected` was actually invoked (ADR-0022 Phase 1). This
//! shape hand-mirrors the frozen canonical eval-case
//! format owned by the worker (`apps/worker/schema/eval-cases.schema.json`, the
//! Pydantic `EvalSuite`); a shape change lands in the same reviewed change as the
//! Python models. Grading semantics mirror the platform's `Grader.grade`. This is
//! the CLI-local seed of the K1 eval machinery, not a replacement for it.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use anyhow::{anyhow, bail, Context, Result};
use curie_aci_protocol::{OutboundEvent, SessionStatus};
use regex::RegexBuilder;
use serde::{Deserialize, Serialize};

/// How a case's expected value is compared against the agent's answer.
// `Serialize` is derived alongside `Deserialize` so the spec scaffold path
// (`spec.rs`) can re-emit an assembled suite into `evals/cases.json`; the
// `rename_all = "lowercase"` round-trips both ways so the written kind is the
// same lowercase token `load_suite` reads back.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum GraderKind {
    Exact,
    Contains,
    Regex,
    /// Assert a named tool was invoked during the turn (graded against the
    /// tool-call trajectory, not the answer text). The wire token is snake_case
    /// `tool_called`, so it is renamed explicitly rather than by the container's
    /// `lowercase` rule (which would emit `toolcalled`).
    #[serde(rename = "tool_called")]
    ToolCalled,
}

/// The terminal session status an eval case asserts. Mirrors the frozen
/// `ExpectedStatus` (apps/worker/.../models.py): `done` = the turn completed and
/// answered, `awaiting-approval` = an approval gate correctly held (ADR-0010).
/// A deliberate subset of `SessionStatus`; classified-failure is never an
/// expectable success. Default `done` keeps every pre-existing case unchanged.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ExpectedStatus {
    #[default]
    Done,
    AwaitingApproval,
}

impl ExpectedStatus {
    /// True if an observed final `status` satisfies this expectation.
    pub fn matches(self, status: &SessionStatus) -> bool {
        matches!(
            (self, status),
            (ExpectedStatus::Done, SessionStatus::Done)
                | (
                    ExpectedStatus::AwaitingApproval,
                    SessionStatus::AwaitingApproval
                )
        )
    }
}

/// A single deterministic grader mirroring the worker's `Grader`. For the text
/// matchers `expected` is the string to match against the answer; for
/// `tool_called` it is instead the tool NAME that must have been invoked.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Grader {
    pub kind: GraderKind,
    pub expected: String,
    #[serde(default)]
    pub case_sensitive: bool,
}

impl Grader {
    /// True if the turn satisfies this grader. `output` is the graded answer
    /// text; `trajectory` is the ordered tool names the turn invoked (each
    /// `tool_note` frame's `tool`, in emission order). Mirrors the platform's
    /// `Grader.grade`: the text matchers judge `output` (exact compares
    /// whitespace-trimmed values, contains is a substring test, regex is a
    /// search; all case-fold both sides unless `case_sensitive`) and ignore the
    /// trajectory; `tool_called` judges the trajectory and ignores `output`.
    /// Tool names are exact identifiers, so `tool_called` compares them exactly
    /// and does not fold case (the `case_sensitive` flag is a text-matcher
    /// concern here).
    pub fn grade(&self, output: &str, trajectory: &[&str]) -> bool {
        if self.kind == GraderKind::ToolCalled {
            return trajectory.contains(&self.expected.as_str());
        }
        if self.kind == GraderKind::Regex {
            return match RegexBuilder::new(&self.expected)
                .case_insensitive(!self.case_sensitive)
                .build()
            {
                Ok(re) => re.is_match(output),
                Err(_) => false,
            };
        }
        // Exact and Contains fold both sides unless case_sensitive, then differ
        // only in the comparison; fold once here rather than per arm.
        let (actual, expected) = if self.case_sensitive {
            (output.to_string(), self.expected.clone())
        } else {
            (output.to_lowercase(), self.expected.to_lowercase())
        };
        if self.kind == GraderKind::Exact {
            actual.trim() == expected.trim()
        } else {
            actual.contains(&expected)
        }
    }
}

/// One eval: an input prompt and the grader that judges the answer.
/// `expect_status` asserts the turn's terminal status: default `done`, or
/// `awaiting-approval` to assert an approval gate blocked the action.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EvalCase {
    pub id: String,
    pub input: String,
    pub grader: Grader,
    /// Per-case isolation opt-out (#550). Each case runs in a *fresh
    /// conversation* by default (`false`): `curie skill eval` resets the runner
    /// before the case so it cannot answer from an earlier case's history instead
    /// of actually invoking its tools -- a false green for a side-effecting agent,
    /// and a silent order-dependence in the suite. Set `true` to deliberately
    /// chain a case onto the prior case's conversation (a multi-turn scenario as
    /// ordered cases); the driver then skips the reset. On the first case it is a
    /// no-op-with-caveat (no prior case to chain onto -- it only means "do not
    /// reset first", inheriting any state the runner already held). Optional with
    /// a `false`
    /// default so it stays byte-compatible with the frozen eval-case schema
    /// (`shared_history: false` is omitted on serialize, mirroring an authored
    /// suite that never wrote the field).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub shared_history: bool,
    #[serde(default)]
    pub expect_status: ExpectedStatus,
}

/// A named set of eval cases run together against one plugin version.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EvalSuite {
    pub name: String,
    pub cases: Vec<EvalCase>,
}

/// One supported comparison for an observed tool sequence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrajectoryMode {
    Exact,
    InOrder,
    AnyOrder,
    Precision,
    Recall,
}

impl TrajectoryMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Exact => "exact",
            Self::InOrder => "in_order",
            Self::AnyOrder => "any_order",
            Self::Precision => "precision",
            Self::Recall => "recall",
        }
    }
}

fn default_trajectory_threshold() -> f64 {
    1.0
}

/// The trajectory expectation supplied above the frozen eval case port.
#[derive(Debug, Clone, Deserialize)]
pub struct TrajectorySpec {
    pub case_id: String,
    pub expected: Vec<String>,
    pub mode: TrajectoryMode,
    #[serde(default = "default_trajectory_threshold")]
    pub threshold: f64,
}

/// The optional sibling configuration for trajectory scoring.
#[derive(Debug, Clone, Deserialize)]
struct TrajectorySidecar {
    specs: Vec<TrajectorySpec>,
}

/// A trajectory verdict and its explanation when red.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrajectoryScore {
    pub passed: bool,
    pub detail: Option<String>,
}

/// The case keyed scorer assembled from a validated trajectory sidecar.
#[derive(Debug, Clone)]
pub struct TrajectoryScorer {
    specs: BTreeMap<String, TrajectorySpec>,
}

impl TrajectoryScorer {
    pub fn score(&self, case_id: &str, observed: &[&str]) -> TrajectoryScore {
        let Some(spec) = self.specs.get(case_id) else {
            return TrajectoryScore {
                passed: false,
                detail: Some(format!(
                    "no trajectory spec for case {}",
                    python_string(case_id)
                )),
            };
        };
        match_trajectory(spec, observed)
    }
}

/// The immutable cases payload plus its optional run layer scorer.
#[derive(Debug, Clone)]
pub struct LoadedEval {
    pub suite: EvalSuite,
    pub trajectory: Option<TrajectoryScorer>,
}

fn python_string(value: &str) -> String {
    let escaped = value
        .replace('\\', "\\\\")
        .replace('\'', "\\'")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t");
    format!("'{escaped}'")
}

fn python_list(values: &[impl AsRef<str>]) -> String {
    let items = values
        .iter()
        .map(|value| python_string(value.as_ref()))
        .collect::<Vec<_>>()
        .join(", ");
    format!("[{items}]")
}

fn python_float(value: f64) -> String {
    if value.fract() == 0.0 {
        format!("{value:.1}")
    } else {
        value.to_string()
    }
}

/// Rust mirror of the worker's authoritative deterministic matcher.
pub fn match_trajectory(spec: &TrajectorySpec, observed: &[&str]) -> TrajectoryScore {
    let expected = spec.expected.iter().map(String::as_str).collect::<Vec<_>>();
    let mut detail = format!(
        "mode={} expected={} observed={}",
        spec.mode.as_str(),
        python_list(&expected),
        python_list(observed),
    );

    let passed = match spec.mode {
        TrajectoryMode::Exact => observed == expected,
        TrajectoryMode::InOrder => {
            let mut expected_index = 0usize;
            for tool in observed {
                if expected
                    .get(expected_index)
                    .is_some_and(|want| want == tool)
                {
                    expected_index += 1;
                }
            }
            expected_index == expected.len()
        }
        TrajectoryMode::AnyOrder => {
            let mut remaining = BTreeMap::<&str, usize>::new();
            for tool in &expected {
                *remaining.entry(tool).or_default() += 1;
            }
            for tool in observed {
                if let Some(count) = remaining.get_mut(tool) {
                    *count = count.saturating_sub(1);
                }
            }
            remaining.values().all(|count| *count == 0)
        }
        TrajectoryMode::Precision => {
            let expected_tools = expected.iter().copied().collect::<BTreeSet<_>>();
            let ratio = if observed.is_empty() {
                1.0
            } else {
                observed
                    .iter()
                    .filter(|tool| expected_tools.contains(**tool))
                    .count() as f64
                    / observed.len() as f64
            };
            detail.push_str(&format!(
                " precision={ratio:.3} threshold={}",
                python_float(spec.threshold)
            ));
            ratio >= spec.threshold
        }
        TrajectoryMode::Recall => {
            let expected_tools = expected.iter().copied().collect::<BTreeSet<_>>();
            let observed_tools = observed.iter().copied().collect::<BTreeSet<_>>();
            let ratio = if expected_tools.is_empty() {
                1.0
            } else {
                expected_tools
                    .iter()
                    .filter(|tool| observed_tools.contains(**tool))
                    .count() as f64
                    / expected_tools.len() as f64
            };
            detail.push_str(&format!(
                " recall={ratio:.3} threshold={}",
                python_float(spec.threshold)
            ));
            ratio >= spec.threshold
        }
    };

    TrajectoryScore {
        passed,
        detail: (!passed).then_some(detail),
    }
}

/// Validate an assembled suite: reject an empty case list and eagerly compile
/// every regex grader so a bad pattern fails now, not mid-run. Factored out of
/// `load_suite` so the spec scaffold path (`spec.rs`) enforces the identical
/// eval-case discipline against a suite it built in memory rather than read from
/// disk -- one rule, two entry points, no drift.
pub fn validate_suite(name: &str, cases: &[EvalCase]) -> Result<()> {
    if cases.is_empty() {
        bail!("suite {:?} contains no eval cases", name);
    }
    for case in cases {
        if case.grader.kind == GraderKind::Regex {
            RegexBuilder::new(&case.grader.expected)
                .build()
                .map_err(|err| {
                    anyhow!(
                        "case {:?} has an invalid regex grader {:?}: {err}. The local CLI compiles \
                         patterns with the Rust `regex` crate, a portable subset with no lookaround \
                         or backreferences; the pattern may still be valid on the platform.",
                        case.id,
                        case.grader.expected
                    )
                })?;
        }
        // A tool_called grader's `expected` is a tool name; an empty one matches
        // no tool and can never be satisfied, so reject it at load exactly as the
        // platform's Grader validator does.
        if case.grader.kind == GraderKind::ToolCalled && case.grader.expected.trim().is_empty() {
            bail!(
                "case {:?} has a tool_called grader with an empty tool name in `expected`",
                case.id
            );
        }
    }
    Ok(())
}

/// Narrow `suite` to the cases named by `selector` (`--case-id`, repeatable).
///
/// An empty `selector` is the unfiltered run and returns the suite untouched, so
/// every pre-existing invocation is byte-identical. Otherwise the suite is
/// filtered in SUITE order (not selector order), so a filtered run reports its
/// cases in the same order a full run would.
///
/// A selector value that matches no case in the suite is a `Usage` error (exit
/// 2, ADR-0021): a mistyped `--case-id` must FAIL the gate rather than green it
/// on an empty run. This is checked per value, not only when the whole selection
/// is empty -- `--case-id good --case-id typo` silently dropping the typo is the
/// same silent-gate failure, one case quieter. The message names the unmatched
/// value(s) verbatim and lists the suite's available ids so the operator can
/// self-correct without opening the file.
pub fn select_cases(suite: EvalSuite, selector: &[String]) -> Result<EvalSuite> {
    if selector.is_empty() {
        return Ok(suite);
    }
    let available = suite
        .cases
        .iter()
        .map(|case| case.id.clone())
        .collect::<Vec<_>>();
    let mut unmatched: Vec<&str> = Vec::new();
    for want in selector {
        if !available.iter().any(|id| id == want) && !unmatched.contains(&want.as_str()) {
            unmatched.push(want.as_str());
        }
    }
    if !unmatched.is_empty() {
        let missing = unmatched.join(", ");
        let known = available.join(", ");
        return Err(anyhow::Error::from(
            crate::exit::CliError::usage(format!(
                "--case-id matched no case in suite {:?}: {missing}. A selector that matches \
                 nothing fails the eval gate instead of greening an empty run; the suite defines: \
                 {known}",
                suite.name,
            ))
            .with_fix(format!(
                "correct the --case-id value(s) ({missing}); the suite defines: {known}"
            )),
        ));
    }
    let mut suite = suite;
    suite
        .cases
        .retain(|case| selector.iter().any(|want| want == &case.id));
    Ok(suite)
}

/// The one-line human note for an active selector, or `None` for an unfiltered
/// run. A filtered run must not be mistakable for a full one when someone reads
/// the terminal, so the note names the ids and the narrowed count. Pure and
/// separate from [`select_cases`] so the wording is testable without a suite,
/// and so the `--json` payload is untouched (notes go to stderr).
pub fn selection_note(selector: &[String], selected: usize, total: usize) -> Option<String> {
    if selector.is_empty() {
        return None;
    }
    Some(format!(
        "selector: --case-id {} ({selected} of {total} cases)",
        selector.join(", ")
    ))
}

fn parse_suite(path: &Path, body: &[u8]) -> Result<EvalSuite> {
    let value: serde_json::Value = serde_json::from_slice(body)
        .with_context(|| format!("{} is not valid JSON", path.display()))?;
    if value.is_array() {
        bail!(
            "{} is in the retired eval-case format (a top-level array of \
             [{{name, input, expect_contains}}]). The eval-case format is now a suite \
             object: {{\"name\": \"...\", \"cases\": [{{\"id\": \"...\", \"input\": \"...\", \
             \"grader\": {{\"kind\": \"contains\", \"expected\": \"...\", \"case_sensitive\": false}}}}]}}. \
             Rewrite the file to the object form.",
            path.display()
        );
    }
    let suite: EvalSuite = serde_json::from_value(value)
        .with_context(|| format!("{} is not a valid eval suite", path.display()))?;
    validate_suite(&suite.name, &suite.cases)?;
    Ok(suite)
}

/// Parse the suite object at `path`. Rejects an empty `cases` list, eagerly
/// compiles every regex grader (so a bad pattern fails at load, not mid-run),
/// and turns the retired top-level-array format into a targeted migration hint.
pub fn load_suite(path: &Path) -> Result<EvalSuite> {
    let body = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    parse_suite(path, &body)
}

/// Load the immutable cases bytes and validate an optional trajectory sidecar.
///
/// Specs may omit a case on purpose; the scorer then fails that case closed.
pub fn load_eval(path: &Path) -> Result<LoadedEval> {
    let body = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    let suite = parse_suite(path, &body)?;
    let sidecar_path = path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("trajectory.json");
    if !sidecar_path.is_file() {
        return Ok(LoadedEval {
            suite,
            trajectory: None,
        });
    }

    let sidecar_body = std::fs::read(&sidecar_path)
        .with_context(|| format!("reading {}", sidecar_path.display()))?;
    let sidecar: TrajectorySidecar = serde_json::from_slice(&sidecar_body).with_context(|| {
        format!(
            "{} is not a valid trajectory sidecar",
            sidecar_path.display()
        )
    })?;

    let mut case_ids = BTreeSet::new();
    for case in &suite.cases {
        if !case_ids.insert(case.id.as_str()) {
            bail!(
                "suite {:?} contains duplicate case id {:?}; trajectory specs require unique case ids",
                suite.name,
                case.id,
            );
        }
    }
    let mut specs = BTreeMap::new();
    for spec in sidecar.specs {
        if !(0.0..=1.0).contains(&spec.threshold) {
            bail!(
                "{} gives case {:?} threshold {}; expected a value from 0 through 1",
                sidecar_path.display(),
                spec.case_id,
                spec.threshold,
            );
        }
        let case_id = spec.case_id.clone();
        if specs.insert(case_id.clone(), spec).is_some() {
            bail!(
                "{} contains duplicate trajectory spec for case {:?}",
                sidecar_path.display(),
                case_id,
            );
        }
    }
    Ok(LoadedEval {
        suite,
        trajectory: Some(TrajectoryScorer { specs }),
    })
}

/// The graded answer for a turn: the `final` frame's text when a final arrived,
/// else the concatenation of the streamed text deltas. Mirrors the platform
/// runner: streamed interim text is not graded once a final exists.
pub fn graded_answer(events: &[OutboundEvent]) -> String {
    let mut final_text: Option<&str> = None;
    let mut deltas = String::new();
    for event in events {
        match event {
            OutboundEvent::Final { text, .. } => final_text = Some(text),
            OutboundEvent::TextDelta { text, .. } => deltas.push_str(text),
            _ => {}
        }
    }
    match final_text {
        Some(text) => text.to_string(),
        None => deltas,
    }
}

/// The ordered tool-call trajectory of a turn: the `tool` field of every
/// `tool_note` frame, in emission order. This is the observed record a
/// `tool_called` grader asserts against -- read from the trajectory the runner
/// emitted, never inferred from the answer text (ADR-0022). Mirrors the platform
/// runner, which accumulates the same list off the `ToolNote` frames.
pub fn trajectory(events: &[OutboundEvent]) -> Vec<&str> {
    events
        .iter()
        .filter_map(|event| match event {
            OutboundEvent::ToolNote { tool: Some(t), .. } => Some(t.as_str()),
            _ => None,
        })
        .collect()
}

/// Full pass condition for a turn: it must end in a `final` whose status equals
/// the case's `expect_status` (default `done`, or `awaiting-approval` for a
/// gate-blocked case) AND the grader must match the turn. A text grader matches
/// the graded answer; a `tool_called` grader matches the observed tool-call
/// trajectory. A classified-failure or interrupted turn still never passes,
/// because those statuses match neither `done` nor `awaiting-approval`.
pub fn turn_passes(case: &EvalCase, events: &[OutboundEvent]) -> bool {
    turn_completed(case, events)
        && case
            .grader
            .grade(&graded_answer(events), &trajectory(events))
}

/// True when the turn ended in a `final` frame whose status matches the case's
/// `expect_status` (default `done`, or `awaiting-approval` for a gate-blocked
/// case). The one assertion the fake tier makes, and the gate the real path
/// applies before the grader is ever consulted; a classified-failure or
/// interrupted turn matches neither expected status, so it never completes.
///
/// `pub` so a `--model` sweep can tally *completion* per case, independent of
/// `CaseOutcome`: `turn_outcome` collapses "never completed" and "completed but
/// graded wrong" into the same `Fail`, which is exactly the ambiguity a sweep
/// row must not have (issue #622, #526 AC4) -- a model that produced zero
/// completed turns across the whole suite is a categorically different outcome
/// from one that completed turns and lost on the grader.
pub fn turn_completed(case: &EvalCase, events: &[OutboundEvent]) -> bool {
    let Some(OutboundEvent::Final { status, .. }) = events.last() else {
        return false;
    };
    case.expect_status.matches(status)
}

/// What a run of one eval case concluded. `PlumbingOk` is a THIRD state, not a
/// shade of pass or fail: the fake model is a plumbing fixture, not a subject
/// under test (ADR-0055), so a fake turn is never graded and can claim neither
/// verdict. Mirrors the worker's `EvalOutcome`; the two vocabularies stay
/// aligned or the skill/local/cluster parity this file exists to hold breaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CaseOutcome {
    Pass,
    Fail,
    PlumbingOk,
}

impl CaseOutcome {
    /// The tri-state `passed` an agent consumer reads: a non-graded row claims
    /// neither verdict, so it is `null` rather than a fabricated `false`.
    pub fn passed(self) -> Option<bool> {
        match self {
            CaseOutcome::Pass => Some(true),
            CaseOutcome::Fail => Some(false),
            CaseOutcome::PlumbingOk => None,
        }
    }
}

/// The outcome of one case's turn. `fake` says the runner that produced `events`
/// was booted with the fake model.
///
/// The completion gate runs FIRST and applies to both tiers: a turn that did not
/// reach its `expect_status` is a genuine `Fail` whatever produced it, and on the
/// fake tier that is the only thing left to catch. Past the gate the two tiers
/// diverge: a fake turn returns `PlumbingOk` WITHOUT consulting the grader at all,
/// so no grader verdict -- in either direction -- can reach a fake run's outcome.
pub fn turn_outcome(case: &EvalCase, events: &[OutboundEvent], fake: bool) -> CaseOutcome {
    if !turn_completed(case, events) {
        return CaseOutcome::Fail;
    }
    if fake {
        return CaseOutcome::PlumbingOk;
    }
    if case
        .grader
        .grade(&graded_answer(events), &trajectory(events))
    {
        CaseOutcome::Pass
    } else {
        CaseOutcome::Fail
    }
}

/// One run layer verdict with optional scorer evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScoredCaseOutcome {
    pub outcome: CaseOutcome,
    pub detail: Option<String>,
}

/// Grade a turn through the selected run layer scorer.
///
/// Completion and fake model truthfulness remain ahead of scoring. This means a
/// fake turn is plumbing only even when a trajectory sidecar exists, while an
/// incomplete turn remains a real failure without pretending a matcher ran.
pub fn score_turn(
    case: &EvalCase,
    events: &[OutboundEvent],
    fake: bool,
    trajectory_scorer: Option<&TrajectoryScorer>,
) -> ScoredCaseOutcome {
    if !turn_completed(case, events) {
        return ScoredCaseOutcome {
            outcome: CaseOutcome::Fail,
            detail: None,
        };
    }
    if fake {
        return ScoredCaseOutcome {
            outcome: CaseOutcome::PlumbingOk,
            detail: None,
        };
    }
    if let Some(scorer) = trajectory_scorer {
        let observed = trajectory(events);
        let score = scorer.score(&case.id, &observed);
        return ScoredCaseOutcome {
            outcome: if score.passed {
                CaseOutcome::Pass
            } else {
                CaseOutcome::Fail
            },
            detail: score.detail,
        };
    }
    ScoredCaseOutcome {
        outcome: turn_outcome(case, events, false),
        detail: None,
    }
}

/// One rendered result line: check-or-cross, name, duration (design canon).
pub fn case_line(name: &str, passed: bool, seconds: f64) -> String {
    let mark = if passed { '\u{2713}' } else { '\u{2717}' };
    format!("{mark} {name}  {seconds:.1}s")
}

pub fn summary_line(passed: usize, total: usize) -> String {
    format!("{passed}/{total} passed")
}

/// The mark and word for one case's outcome in the results table. A plumbing row
/// gets neither the check nor the cross: it is not a verdict.
pub fn outcome_label(outcome: CaseOutcome) -> String {
    match outcome {
        CaseOutcome::Pass => format!("{} pass", '\u{2713}'),
        CaseOutcome::Fail => format!("{} fail", '\u{2717}'),
        CaseOutcome::PlumbingOk => format!("{} plumbing OK", '\u{2022}'),
    }
}

/// The run's one-line verdict. A run with no plumbing rows reads exactly as it
/// always did (`summary_line`); once any row is non-graded the line says so in
/// words, because `N/N passed` on a run that graded nothing is the false green
/// this whole outcome exists to kill (#606). Pure so the wording is testable.
pub fn rollup_line(passed: usize, failed: usize, plumbing_ok: usize) -> String {
    if plumbing_ok == 0 {
        return summary_line(passed, passed + failed);
    }
    let plumbing = format!("{plumbing_ok} plumbing OK (not graded)");
    let graded = passed + failed;
    if graded == 0 {
        return plumbing;
    }
    format!("{}, {plumbing}", summary_line(passed, graded))
}

#[cfg(test)]
mod tests {
    use super::*;
    use curie_aci_protocol::PROTOCOL_VERSION;

    fn grader(kind: GraderKind, expected: &str, case_sensitive: bool) -> Grader {
        Grader {
            kind,
            expected: expected.into(),
            case_sensitive,
        }
    }

    fn case(g: Grader) -> EvalCase {
        case_with_status(g, ExpectedStatus::Done)
    }

    fn case_with_status(g: Grader, expect_status: ExpectedStatus) -> EvalCase {
        EvalCase {
            id: "c".into(),
            input: "hi".into(),
            grader: g,
            shared_history: false,
            expect_status,
        }
    }

    fn delta(text: &str) -> OutboundEvent {
        OutboundEvent::TextDelta {
            version: PROTOCOL_VERSION.into(),
            text: text.into(),
            adoption_applied: None,
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

    fn tool_note(tool: &str) -> OutboundEvent {
        OutboundEvent::ToolNote {
            version: PROTOCOL_VERSION.into(),
            text: format!("calling {tool}"),
            tool: Some(tool.into()),
            adoption_applied: None,
        }
    }

    fn write(body: &str) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cases.json");
        std::fs::write(&path, body).unwrap();
        (dir, path)
    }

    #[test]
    fn loads_the_object_suite_form() {
        let (_dir, path) = write(
            r#"{"name":"s","cases":[{"id":"a","input":"b","grader":{"kind":"contains","expected":"x"}}]}"#,
        );
        let suite = load_suite(&path).unwrap();
        assert_eq!(suite.name, "s");
        assert_eq!(suite.cases.len(), 1);
        assert_eq!(suite.cases[0].id, "a");
        assert_eq!(suite.cases[0].grader.kind, GraderKind::Contains);
        assert!(!suite.cases[0].grader.case_sensitive);
        // An absent expect_status defaults to Done, keeping pre-existing cases
        // byte-identical in behavior.
        assert_eq!(suite.cases[0].expect_status, ExpectedStatus::Done);
    }

    #[test]
    fn loads_expect_status_awaiting_approval() {
        let (_dir, path) = write(
            r#"{"name":"s","cases":[{"id":"a","input":"b","grader":{"kind":"contains","expected":"x"},"expect_status":"awaiting-approval"}]}"#,
        );
        let suite = load_suite(&path).unwrap();
        assert_eq!(
            suite.cases[0].expect_status,
            ExpectedStatus::AwaitingApproval
        );
    }

    #[test]
    fn shared_history_defaults_to_false_and_reads_true_when_set() {
        // Omitted -> false (backward compatible with every authored suite).
        let (_dir, path) = write(
            r#"{"name":"s","cases":[{"id":"a","input":"b","grader":{"kind":"contains","expected":"x"}}]}"#,
        );
        assert!(!load_suite(&path).unwrap().cases[0].shared_history);
        // Present and true -> the case opts into the prior case's conversation.
        let (_dir2, path2) = write(
            r#"{"name":"s","cases":[{"id":"a","input":"b","grader":{"kind":"contains","expected":"x"},"shared_history":true}]}"#,
        );
        assert!(load_suite(&path2).unwrap().cases[0].shared_history);
    }

    #[test]
    fn a_false_shared_history_is_omitted_on_serialize() {
        // Byte-compat with the frozen schema: a fresh-conversation case (the
        // default) serializes exactly as a suite that never wrote the field, so
        // the scaffold and spec-authored cases.json stay unchanged.
        let json = serde_json::to_string(&case(grader(GraderKind::Contains, "x", false))).unwrap();
        assert!(!json.contains("shared_history"), "got: {json}");
    }

    #[test]
    fn rejects_the_retired_array_form_with_a_migration_hint() {
        let (_dir, path) = write(r#"[{"name":"a","input":"b","expect_contains":["c"]}]"#);
        let err = load_suite(&path).unwrap_err().to_string();
        assert!(err.contains("retired eval-case format"), "{err}");
        assert!(err.contains("expect_contains"), "{err}");
        assert!(err.contains("\"cases\""), "{err}");
    }

    #[test]
    fn rejects_an_empty_cases_list() {
        let (_dir, path) = write(r#"{"name":"s","cases":[]}"#);
        let err = load_suite(&path).unwrap_err().to_string();
        assert!(err.contains("no eval cases"), "{err}");
    }

    #[test]
    fn rejects_an_unknown_grader_kind() {
        let (_dir, path) = write(
            r#"{"name":"s","cases":[{"id":"a","input":"b","grader":{"kind":"llm_judge","expected":"x"}}]}"#,
        );
        assert!(load_suite(&path).is_err());
    }

    #[test]
    fn rejects_an_invalid_regex_grader_at_load() {
        let (_dir, path) = write(
            r#"{"name":"s","cases":[{"id":"a","input":"b","grader":{"kind":"regex","expected":"(unclosed"}}]}"#,
        );
        let err = load_suite(&path).unwrap_err().to_string();
        assert!(err.contains("invalid regex grader"), "{err}");
        assert!(err.contains("may still be valid on the platform"), "{err}");
    }

    #[test]
    fn exact_grader_trims_and_case_folds() {
        assert!(grader(GraderKind::Exact, "  Done  ", false).grade("done", &[]));
        assert!(!grader(GraderKind::Exact, "done", true).grade("Done", &[]));
        assert!(grader(GraderKind::Exact, "done", true).grade("  done  ", &[]));
        assert!(!grader(GraderKind::Exact, "done", false).grade("all done", &[]));
    }

    #[test]
    fn contains_grader_case_folds_unless_flagged() {
        assert!(grader(GraderKind::Contains, "WEATHER", false).grade("the weather today", &[]));
        assert!(!grader(GraderKind::Contains, "WEATHER", true).grade("the weather today", &[]));
        assert!(grader(GraderKind::Contains, "weather", true).grade("the weather today", &[]));
    }

    #[test]
    fn regex_grader_searches_with_optional_case_flag() {
        assert!(grader(GraderKind::Regex, "wea.her", false).grade("The WEATHER", &[]));
        assert!(!grader(GraderKind::Regex, "WEA.HER", true).grade("the weather", &[]));
        assert!(grader(GraderKind::Regex, "^done$", false).grade("DONE", &[]));
    }

    #[test]
    fn tool_called_grader_reads_the_trajectory_not_the_text() {
        let g = grader(GraderKind::ToolCalled, "DeterministicEngine", false);
        // GREEN: the tool appears in the observed trajectory.
        assert!(g.grade("", &["DeterministicEngine"]));
        assert!(g.grade("", &["Bash", "DeterministicEngine", "Read"]));
        // RED: the tool was never called, no matter what the answer text says --
        // grading must read the trajectory, not grep the final text (ADR-0022).
        assert!(!g.grade("I ran the DeterministicEngine tool", &[]));
        assert!(!g.grade("DeterministicEngine", &["Bash", "Read"]));
        // The case_sensitive flag is a text-matcher concern; tool names match
        // exactly regardless of it.
        assert!(!grader(GraderKind::ToolCalled, "Bash", false).grade("", &["bash"]));
    }

    #[test]
    fn tool_called_case_greens_when_the_tool_note_is_present_and_reds_when_absent() {
        // The #621 acceptance, at the turn level: a case asserting a tool was
        // called GREENs when the trajectory carries that tool_note and REDs when
        // it does not -- and a do-nothing turn that calls no tool fails it, so the
        // grader is falsifiable.
        let c = case(grader(GraderKind::ToolCalled, "DeterministicEngine", false));
        let called = vec![
            tool_note("DeterministicEngine"),
            final_event("done", SessionStatus::Done),
        ];
        let not_called = vec![
            tool_note("Bash"),
            final_event("I used the DeterministicEngine", SessionStatus::Done),
        ];
        let did_nothing = vec![final_event("all done", SessionStatus::Done)];
        assert!(turn_passes(&c, &called));
        assert!(!turn_passes(&c, &not_called));
        assert!(!turn_passes(&c, &did_nothing));
    }

    #[test]
    fn trajectory_collects_tool_note_names_in_order() {
        let events = vec![
            tool_note("Bash"),
            delta("thinking"),
            tool_note("Read"),
            final_event("done", SessionStatus::Done),
        ];
        assert_eq!(trajectory(&events), vec!["Bash", "Read"]);
    }

    #[test]
    fn loads_a_tool_called_grader_and_rejects_an_empty_tool_name() {
        let (_dir, path) = write(
            r#"{"name":"s","cases":[{"id":"a","input":"b","grader":{"kind":"tool_called","expected":"DeterministicEngine"}}]}"#,
        );
        let suite = load_suite(&path).unwrap();
        assert_eq!(suite.cases[0].grader.kind, GraderKind::ToolCalled);
        assert_eq!(suite.cases[0].grader.expected, "DeterministicEngine");
        // An empty tool name can never be satisfied, so the loader rejects it.
        let (_dir2, path2) = write(
            r#"{"name":"s","cases":[{"id":"a","input":"b","grader":{"kind":"tool_called","expected":"  "}}]}"#,
        );
        let err = load_suite(&path2).unwrap_err().to_string();
        assert!(err.contains("empty tool name"), "{err}");
    }

    #[test]
    fn tool_called_round_trips_through_serialize() {
        // The scaffold/spec re-emit path serializes a Grader; the tool_called kind
        // must write the snake_case wire token load_suite reads back.
        let json = serde_json::to_string(&grader(GraderKind::ToolCalled, "Bash", false)).unwrap();
        assert!(json.contains(r#""kind":"tool_called""#), "got: {json}");
    }

    #[test]
    fn graded_answer_is_final_text_when_a_final_exists() {
        let events = vec![
            delta("Looking into it"),
            final_event("all done", SessionStatus::Done),
        ];
        assert_eq!(graded_answer(&events), "all done");
    }

    #[test]
    fn graded_answer_joins_deltas_when_no_final() {
        let events = vec![delta("Looking "), delta("into it")];
        assert_eq!(graded_answer(&events), "Looking into it");
    }

    #[test]
    fn a_classified_failure_never_passes_even_when_text_matches() {
        let done = vec![
            delta("Looking into it"),
            final_event("all done", SessionStatus::Done),
        ];
        let failed = vec![
            delta("Looking into it"),
            final_event("all done", SessionStatus::ClassifiedFailure),
        ];
        let c = case(grader(GraderKind::Contains, "all done", false));
        assert!(turn_passes(&c, &done));
        assert!(!turn_passes(&c, &failed));
    }

    #[test]
    fn gate_blocked_turn_is_green_and_narrate_only_is_red() {
        // The run-7 anti-correlation, encoded: an approval-gated case that asserts
        // `awaiting-approval` with a match-anything grader is GREEN when the gate
        // holds (turn ends awaiting-approval) and RED when the agent merely
        // narrated and the turn completed (done). Before this change the pass
        // condition hardcoded Done, so "the gate correctly blocked" was RED and
        // "the agent narrated" was GREEN -- scoring anti-correlated with safety.
        let gated = case_with_status(
            grader(GraderKind::Contains, "", false),
            ExpectedStatus::AwaitingApproval,
        );
        let held = vec![final_event(
            "blocked the close",
            SessionStatus::AwaitingApproval,
        )];
        let narrated = vec![final_event("I asked for approval", SessionStatus::Done)];
        assert!(turn_passes(&gated, &held)); // the gate held -> GREEN
        assert!(!turn_passes(&gated, &narrated)); // agent just narrated -> RED

        // Inverse guard: a default (Done) case never passes on an awaiting-approval
        // final, so widening the enum did not loosen the default gate.
        let default_case = case(grader(GraderKind::Contains, "", false));
        assert!(!turn_passes(&default_case, &held));
    }

    #[test]
    fn every_schema_expected_status_deserializes() {
        // The frozen eval-case schema owns the expected-status vocabulary (#262).
        // Every value it enumerates must round-trip through the Rust loader, so a
        // value added to the schema but not to this crate's ExpectedStatus enum
        // fails here rather than silently rejecting a valid platform-authored case.
        let schema: serde_json::Value = serde_json::from_str(include_str!(
            "../../apps/worker/schema/eval-cases.schema.json"
        ))
        .expect("committed eval-cases schema is valid JSON");
        let statuses = schema["$defs"]["ExpectedStatus"]["enum"]
            .as_array()
            .expect("ExpectedStatus enum is an array");
        assert!(!statuses.is_empty(), "schema declares no expected statuses");
        for status in statuses {
            let status = status.as_str().expect("expected status is a string");
            let body = format!(
                r#"{{"name":"s","cases":[{{"id":"c","input":"i","grader":{{"kind":"contains","expected":"x"}},"expect_status":"{status}"}}]}}"#
            );
            let (_dir, path) = write(&body);
            let suite = load_suite(&path).unwrap_or_else(|e| {
                panic!("schema expected status {status:?} was rejected by the Rust loader: {e}")
            });
            assert_eq!(suite.cases.len(), 1);
        }
    }

    #[test]
    fn loads_the_committed_weather_example() {
        // The exact bytes `curie skill eval` reads on `examples/weather`.
        let body = include_str!("../../examples/weather/evals/cases.json");
        let (_dir, path) = write(body);
        let suite = load_suite(&path).unwrap();
        assert_eq!(suite.cases.len(), 1);
        let case = &suite.cases[0];
        // Answer matcher: a passing final answer must carry a temperature figure.
        // The trajectory sidecar supplies the separate fetch capability proof.
        // The loader ignores the documentation-only `note` key on the case.
        assert_eq!(case.id, "reports-a-temperature");
        assert_eq!(case.grader.kind, GraderKind::Regex);
        // #620: the pattern accepts the degree glyph AND the spelled-out unit, so
        // a correct plain-English forecast is no longer graded red. The alternation
        // stays inside the Python-re / Rust-regex intersection (no lookaround, no
        // backreferences), so the CLI compiles it identically to the platform.
        assert_eq!(case.grader.expected, "\\d+\\s*(°|deg)");
    }

    #[test]
    fn weather_answer_matcher_accepts_glyph_and_spelled_unit_but_not_a_figureless_refusal() {
        // #620: prove the committed pattern's behavior by EXECUTING the grader
        // (not by inspecting the string) against the acceptance strings. The glyph
        // and both spellings pass; a refusal that carries no figure fails.
        let body = include_str!("../../examples/weather/evals/cases.json");
        let (_dir, path) = write(body);
        let grader = &load_suite(&path).unwrap().cases[0].grader;
        assert!(grader.grade("68°", &[]), "glyph form must pass");
        assert!(grader.grade("68 deg F", &[]), "abbreviated unit must pass");
        assert!(
            grader.grade(
                "The high in San Francisco today is 68 degrees Fahrenheit",
                &[]
            ),
            "spelled-out unit must pass"
        );
        assert!(
            !grader.grade("I could not confirm a current forecast", &[]),
            "a refusal with no temperature figure must still fail"
        );
    }

    #[test]
    fn every_schema_grader_kind_deserializes() {
        // The frozen eval-case schema owns the grader-kind vocabulary (#500).
        // Every kind it enumerates must round-trip through the Rust loader, so a
        // kind added to the schema but not to this crate's GraderKind enum fails
        // here rather than silently rejecting a valid platform-authored case.
        let schema: serde_json::Value = serde_json::from_str(include_str!(
            "../../apps/worker/schema/eval-cases.schema.json"
        ))
        .expect("committed eval-cases schema is valid JSON");
        let kinds = schema["$defs"]["GraderKind"]["enum"]
            .as_array()
            .expect("GraderKind enum is an array");
        assert!(!kinds.is_empty(), "schema declares no grader kinds");
        for kind in kinds {
            let kind = kind.as_str().expect("grader kind is a string");
            let body = format!(
                r#"{{"name":"s","cases":[{{"id":"c","input":"i","grader":{{"kind":"{kind}","expected":"x"}}}}]}}"#
            );
            let (_dir, path) = write(&body);
            let suite = load_suite(&path).unwrap_or_else(|e| {
                panic!("schema grader kind {kind:?} was rejected by the Rust loader: {e}")
            });
            assert_eq!(suite.cases.len(), 1);
        }
    }

    #[test]
    fn renders_design_canon_lines() {
        assert_eq!(case_line("approver", true, 1.24), "\u{2713} approver  1.2s");
        assert_eq!(case_line("crm", false, 0.9), "\u{2717} crm  0.9s");
        assert_eq!(summary_line(34, 36), "34/36 passed");
    }

    // --- case selector (#2007) ---------------------------------------------

    fn selector_suite() -> EvalSuite {
        let ids = ["greets-the-user", "looks-up-the-order", "escalates"];
        EvalSuite {
            name: "smoke".into(),
            cases: ids
                .iter()
                .map(|id| EvalCase {
                    id: (*id).into(),
                    input: "hi".into(),
                    grader: grader(GraderKind::Contains, "hi", false),
                    shared_history: false,
                    expect_status: ExpectedStatus::Done,
                })
                .collect(),
        }
    }

    fn ids(selector: &[&str]) -> Vec<String> {
        selector.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn an_empty_selector_returns_the_suite_untouched() {
        // The unfiltered run must be byte-identical to today's behaviour.
        let selected = select_cases(selector_suite(), &[]).expect("no selector is not an error");
        assert_eq!(selected.name, "smoke");
        assert_eq!(
            selected
                .cases
                .iter()
                .map(|c| c.id.as_str())
                .collect::<Vec<_>>(),
            vec!["greets-the-user", "looks-up-the-order", "escalates"],
        );
        assert_eq!(selection_note(&[], 3, 3), None, "no note on a full run");
    }

    #[test]
    fn a_mistyped_case_id_selector_is_a_usage_error_naming_the_typo() {
        // THE headline of #2007: a selector that matches nothing must FAIL the
        // gate (exit 2), never green an empty run. `greets-the-usr` is one
        // dropped character from a real case id -- exactly the CI typo this
        // guards against.
        let err = select_cases(selector_suite(), &ids(&["greets-the-usr"]))
            .expect_err("a selector matching nothing must fail");
        let (class, fix) = crate::exit::classify(&err);
        assert_eq!(
            class,
            crate::exit::ExitClass::Usage,
            "an unmatched selector exits 2, not 0: {err:#}"
        );
        let shown = format!("{err:#}");
        assert!(
            shown.contains("greets-the-usr"),
            "the message names the mistyped value verbatim: {shown}"
        );
        assert!(
            shown.contains("greets-the-user"),
            "the message lists the suite's real ids so the operator can self-correct: {shown}"
        );
        let fix = fix.expect("an unmatched selector carries a fix");
        assert!(
            fix.contains("greets-the-usr") && fix.contains("greets-the-user"),
            "the fix names the typo and the available ids: {fix}"
        );
    }

    #[test]
    fn a_partially_mistyped_selector_fails_and_names_only_the_typo() {
        // One good id plus one typo must not quietly run the good one: that is
        // the same silent-gate failure, one case quieter.
        let err = select_cases(selector_suite(), &ids(&["escalates", "escalatez"]))
            .expect_err("a partially unmatched selector must fail");
        assert_eq!(
            crate::exit::classify(&err).0,
            crate::exit::ExitClass::Usage,
            "{err:#}"
        );
        let shown = format!("{err:#}");
        assert!(shown.contains("escalatez"), "{shown}");
        assert!(
            !shown.contains("matched no case in suite \"smoke\": escalates,"),
            "only the unmatched value is reported as unmatched: {shown}"
        );
    }

    #[test]
    fn a_matching_selector_narrows_the_suite_in_suite_order() {
        // Selector order is deliberately NOT the run order: a filtered run
        // reports its cases in the same order a full run would.
        let selected = select_cases(selector_suite(), &ids(&["escalates", "greets-the-user"]))
            .expect("both ids exist");
        assert_eq!(
            selected
                .cases
                .iter()
                .map(|c| c.id.as_str())
                .collect::<Vec<_>>(),
            vec!["greets-the-user", "escalates"],
        );
        assert_eq!(selected.name, "smoke", "the suite name is preserved");
    }

    #[test]
    fn an_active_selector_notes_the_ids_and_the_narrowed_count() {
        assert_eq!(
            selection_note(&ids(&["a", "b"]), 2, 7).as_deref(),
            Some("selector: --case-id a, b (2 of 7 cases)"),
        );
    }

    #[test]
    fn a_selected_suite_still_passes_validate_suite() {
        // The narrowed suite is a real suite: it never trips the empty-cases
        // rule, because every selector value matched at least one case.
        let selected = select_cases(selector_suite(), &ids(&["escalates"])).expect("the id exists");
        validate_suite(&selected.name, &selected.cases).expect("a narrowed suite is valid");
        assert_eq!(selected.cases.len(), 1);
    }
}

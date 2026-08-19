//! `curie scenario`: run a ticket's scenario manifest and emit criterion-bound
//! structured evidence.
//!
//! The five stages, each a real production code path:
//!
//! 1. **Package once.** [`crate::bundle::snapshot_ephemeral`] materializes a
//!    run-unique snapshot, exactly one pack per run (#1087). Run-unique rather
//!    than the canonical content-addressed path so a concurrent `skill up` of
//!    the same bundle cannot pack over the artifact this run booted on.
//! 2. **Boot on that exact artifact.** [`crate::commands::start`], the function
//!    `skill up` calls, taking the snapshot rather than packing its own.
//! 3. **Verify runtime identity against the DAEMON.** The `/plugin` mount source
//!    Docker reports must be the snapshot this run packed, and the container's
//!    `CURIE_FAKE_MODEL` must agree with the manifest's `model_mode`. Both are
//!    reads of a genuine second party, never an echo of what this process wrote.
//! 4. **Probe.** Each probe is a real graded turn over the runner's ACI surface,
//!    judged by the frozen [`crate::evals`] grader `skill eval` uses, with a
//!    `/v1/reset` between probes for per-case isolation (#550).
//! 5. **Tear down unconditionally, and verify it.** Bundle-scoped
//!    [`crate::commands::stop`] followed by three asserted postconditions; any
//!    unmet one turns the run red rather than reporting a clean teardown.
//!
//! Only the `skill` tier is supported. `local` and `cluster` are modelled so the
//! refusal is reachable, and are refused with exit 4 (ADR-0041) naming the
//! upstream platform-API blocker: there is no route that runs a probe against a
//! CLI-created version, and none that ends a deployment, so a scenario there
//! could neither grade a turn nor tear down what it created. Refusing is the
//! contract; degrading to a probe-less run is not.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use curie_aci_protocol::{EventType, OutboundEvent};
use serde::{Deserialize, Serialize};

use crate::commands::{StartOpts, DEFAULT_BUDGET, DEFAULT_PORT};
use crate::docker;
use crate::evals::{self, CaseOutcome, EvalCase, ExpectedStatus, Grader, GraderKind};
use crate::exit::{self, CliError};
use crate::runner::RunnerClient;
use crate::state;
use crate::ui::{CliOutput, Ui};

/// The mount destination inside the runner container that holds the bundle.
const PLUGIN_MOUNT: &str = "/plugin";

/// The container env var that says the runner was booted on the fake model.
const FAKE_MODEL_ENV: &str = "CURIE_FAKE_MODEL";

/// The reply recorded for a turn that produced no gradeable text, so a red probe
/// is diagnosable without a re-run (#548).
const NO_REPLY: &str = "<no reply text>";

/// How long a probe waits for `/v1/reset`, a runner control call answered
/// immediately. A peer that accepts the connection and then never answers would
/// otherwise wedge the run forever, and a run that hangs never tears down: the
/// container, the record and the snapshot all outlive it.
const RESET_DEADLINE: Duration = Duration::from_secs(30);

/// How long a probe waits for one complete graded turn. Generous, because a live
/// model turn legitimately takes minutes, but bounded for the same reason the
/// reset is.
const PROBE_DEADLINE: Duration = Duration::from_secs(300);

// ---------------------------------------------------------------------------
// The manifest contract
// ---------------------------------------------------------------------------

/// A tier a scenario manifest can select. All three known tiers are modelled so
/// the refusal below is reachable; only [`TierName::Skill`] reaches the
/// pipeline, and an unmodelled string is a parse (usage) error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum TierName {
    Skill,
    Local,
    Cluster,
}

impl TierName {
    fn as_str(self) -> &'static str {
        match self {
            TierName::Skill => "skill",
            TierName::Local => "local",
            TierName::Cluster => "cluster",
        }
    }
}

/// Which model the scenario asserts the runner was booted on. Asserted against
/// the live container, never reinterpreted after the fact.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ModelMode {
    Fake,
    Live,
}

/// A positive probe asserts the required behavior is present; a negative one is
/// the falsifiability control, and passes only when its grader does NOT match.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ProbeKind {
    Positive,
    Negative,
}

/// The roll-up verdict of a run or of one tier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    Passed,
    Failed,
    PlumbingOk,
}

/// A scenario manifest: what a ticket's acceptance criteria are, and the probes
/// that answer them.
///
/// `deny_unknown_fields` on this and EVERY nested struct: a typo'd key would
/// otherwise mean a probe that silently never ran under a green report.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScenarioManifest {
    pub schema_version: u32,
    pub ticket: String,
    pub acceptance_criteria: Vec<String>,
    pub bundle_path: PathBuf,
    pub tiers: Vec<TierName>,
    pub model_mode: ModelMode,
    pub probes: Vec<Probe>,
    pub teardown: Teardown,
}

/// One probe: a prompt, the criteria it answers, and the concrete grader that
/// judges the reply.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Probe {
    pub id: String,
    pub kind: ProbeKind,
    pub acceptance_criteria: Vec<String>,
    pub prompt: String,
    pub expect: Expect,
}

/// The expectation a probe is judged by, mirroring the frozen
/// [`crate::evals::Grader`] so a probe cannot drift from how this repo already
/// judges a turn.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Expect {
    pub grader: GraderKind,
    pub value: String,
}

/// The teardown the manifest acknowledges. `remove_runner` is constrained to
/// `true`: teardown is unconditional, so the author acknowledges in the
/// checked-in artifact that running the scenario destroys the runner and gets no
/// degradation path.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Teardown {
    pub remove_runner: bool,
}

/// The manifest schema version this build understands.
const SUPPORTED_SCHEMA_VERSION: u32 = 1;

/// A usage refusal (exit 2) whose recovery instruction rides in the `fix` FIELD
/// rather than inside the diagnosis, the ADR-0021 shape an agent consumer reads.
fn refusal(error: impl Into<String>, fix: impl Into<String>) -> anyhow::Error {
    anyhow::Error::from(CliError::usage(error).with_fix(fix))
}

/// Parse and validate a scenario manifest.
///
/// Every rule here is also expressed in `cli/schema/scenario-manifest.schema.json`,
/// because a hand-written manifest bypasses the schema; the cross-field coverage
/// rule is the one JSON Schema cannot express, so this is its only enforcement.
pub fn parse_manifest(raw: &str, source: &Path) -> Result<ScenarioManifest> {
    let manifest: ScenarioManifest = serde_json::from_str(raw).map_err(|err| {
        refusal(
            format!(
                "{} is not a valid scenario manifest: {err}",
                source.display()
            ),
            "correct the manifest against cli/schema/scenario-manifest.schema.json, which \
             declares every field and value this build reads",
        )
    })?;

    if manifest.schema_version != SUPPORTED_SCHEMA_VERSION {
        return Err(refusal(
            format!(
                "scenario manifest schema_version {} is not supported; this build reads version {SUPPORTED_SCHEMA_VERSION}",
                manifest.schema_version
            ),
            format!(
                "set \"schema_version\": {SUPPORTED_SCHEMA_VERSION}, or run a curie build that \
                 reads the version this manifest declares"
            ),
        ));
    }
    if manifest.tiers.is_empty() {
        return Err(refusal(
            "the scenario selects no tier, so it would run nothing and report nothing",
            "list at least one tier in `tiers`, e.g. \"tiers\": [\"skill\"]",
        ));
    }
    if manifest.acceptance_criteria.is_empty() {
        return Err(refusal(
            "the scenario names no acceptance criteria, so nothing it reports is bound to \
             the ticket",
            "list the ticket's criteria in `acceptance_criteria`",
        ));
    }
    if !manifest.teardown.remove_runner {
        return Err(refusal(
            "teardown.remove_runner is false, but a scenario run always destroys the runner \
             it booted and there is no mode that keeps it",
            "set \"teardown\": { \"remove_runner\": true } in the manifest",
        ));
    }
    if !manifest
        .probes
        .iter()
        .any(|probe| probe.kind == ProbeKind::Positive)
    {
        return Err(refusal(
            "the scenario declares no positive probe, so nothing shows the required behavior \
             is present",
            "add a probe with \"kind\": \"positive\" whose grader matches the required behavior",
        ));
    }
    if !manifest
        .probes
        .iter()
        .any(|probe| probe.kind == ProbeKind::Negative)
    {
        return Err(refusal(
            "the scenario declares no negative probe, so it cannot be falsified",
            "add a control with \"kind\": \"negative\" whose grader must NOT match",
        ));
    }
    let mut seen = BTreeSet::new();
    for probe in &manifest.probes {
        if !seen.insert(probe.id.as_str()) {
            return Err(refusal(
                format!("probe id {:?} appears twice", probe.id),
                "give every probe a unique `id`; the id is what binds a graded row to the \
                 probe that produced it",
            ));
        }
    }

    let uncovered = uncovered_criteria(&manifest);
    if !uncovered.is_empty() {
        return Err(refusal(
            format!(
                "these acceptance criteria have no positive probe: {}. A criterion whose only \
                 evidence is that the pre-fix behavior is absent has no positive evidence that \
                 the required behavior is present",
                uncovered.join(", ")
            ),
            "add a positive probe naming each of those criteria in its `acceptance_criteria`",
        ));
    }

    // The graders, before ANY side effect: a manifest that reaches Docker and
    // only then discovers an unsatisfiable expectation has already packed and
    // booted. `minLength: 1` is a schema rule, and a hand-written manifest never
    // passes through the schema, so this is its only enforcement.
    for probe in &manifest.probes {
        if probe.expect.value.trim().is_empty() {
            return Err(refusal(
                format!(
                    "probe {:?} has an empty expect.value: a grader with no value matches every \
                     completed reply, so a positive probe can never fail and a control can never \
                     be satisfied",
                    probe.id
                ),
                "give the probe a non-empty `expect.value` that the reply must (or, for a \
                 control, must not) satisfy",
            ));
        }
    }
    // The frozen validation `skill eval` applies to a suite, applied to the
    // probes verbatim: a probe is judged by that code, so it is validated by it
    // too. An uncompilable regex grades false, and on a NEGATIVE probe that false
    // is inverted into a pass, so a typo'd control could never fail.
    let cases: Vec<EvalCase> = manifest.probes.iter().map(eval_case).collect();
    evals::validate_suite(&manifest.ticket, &cases).map_err(|err| {
        refusal(
            format!("{err:#}"),
            "correct that probe's `expect` in the manifest: every grader is compiled before \
             anything is packed or booted",
        )
    })?;

    Ok(manifest)
}

/// The top-level acceptance criteria no POSITIVE probe answers, in manifest
/// order. Pure, and it names the uncovered ones rather than reporting a count:
/// an author cannot act on "2 uncovered".
fn uncovered_criteria(manifest: &ScenarioManifest) -> Vec<String> {
    let covered: BTreeSet<&str> = manifest
        .probes
        .iter()
        .filter(|probe| probe.kind == ProbeKind::Positive)
        .flat_map(|probe| probe.acceptance_criteria.iter().map(String::as_str))
        .collect();
    manifest
        .acceptance_criteria
        .iter()
        .filter(|criterion| !covered.contains(criterion.as_str()))
        .cloned()
        .collect()
}

/// The ADR-0041 refusal for a tier this command knows and cannot run.
fn tier_refusal(tier: TierName) -> anyhow::Error {
    anyhow::Error::from(
        CliError::unsupported(format!(
            "curie scenario cannot run the {} tier: the platform API exposes no route that runs \
             a probe against a CLI-created version (its eval route resolves a commit_sha that \
             version creation never sets) and no route that ends a deployment, so a scenario \
             there could neither grade a turn nor tear down the agent it created",
            tier.as_str()
        ))
        .with_fix(
            "run this scenario at the skill tier: set \"tiers\": [\"skill\"] in the manifest. \
             The two upstream gaps are tracked against apps/api: commit_sha on version creation, \
             and a deployment teardown route",
        ),
    )
}

// ---------------------------------------------------------------------------
// The result contract
// ---------------------------------------------------------------------------

/// The `curie scenario --json` payload: one object per run, on both the green
/// and the red path.
#[derive(Debug, Serialize)]
pub struct ScenarioOutput {
    pub ticket: String,
    pub bundle_path: String,
    /// The bundle tree's commit, or `null` outside a git tree. Never fabricated.
    pub source_commit: Option<String>,
    /// sha256 of the ONE snapshot this run packed.
    pub artifact_digest: String,
    pub model_mode: ModelMode,
    pub verdict: Verdict,
    pub error: Option<String>,
    pub fix: Option<String>,
    pub tiers: Vec<TierReport>,
}

/// What happened at one tier. `mounted_snapshot_dir` and `container_fake_model`
/// are the DAEMON's readings, named so a reader can tell they are not our own
/// record.
#[derive(Debug, Serialize)]
pub struct TierReport {
    pub tier: TierName,
    pub verdict: Verdict,
    pub identity_verified: bool,
    pub mounted_snapshot_dir: Option<String>,
    pub container_fake_model: Option<bool>,
    pub runner_url: String,
    pub probes: Vec<ProbeReport>,
    pub teardown: TeardownReport,
    pub error: Option<String>,
    pub fix: Option<String>,
}

/// One probe's graded turn.
#[derive(Debug, Serialize)]
pub struct ProbeReport {
    pub id: String,
    pub kind: ProbeKind,
    pub acceptance_criteria: Vec<String>,
    pub outcome: CaseOutcome,
    /// Tri-state view of `outcome`, copying `eval.schema.json`: a non-graded row
    /// claims neither verdict, so it is null rather than a fabricated false.
    pub passed: Option<bool>,
    /// The RAW grader result, before the positive/negative inversion, so a red
    /// control is diagnosable without a re-run.
    pub grader_matched: Option<bool>,
    pub reply: String,
}

/// The teardown's asserted postconditions, not a self-report of "we called
/// stop".
#[derive(Debug, Serialize)]
pub struct TeardownReport {
    pub attempted: bool,
    pub container_removed: bool,
    pub state_cleared: bool,
    pub snapshot_released: bool,
    pub error: Option<String>,
}

/// The single JSON object, built in one place so the green and the red path
/// cannot emit two different shapes.
pub fn scenario_json(out: &ScenarioOutput) -> serde_json::Value {
    serde_json::to_value(out).expect("the scenario result is plain data and always serializes")
}

impl CliOutput for ScenarioOutput {
    fn to_json(&self) -> serde_json::Value {
        scenario_json(self)
    }

    fn render(&self, ui: &Ui) {
        for tier in &self.tiers {
            let rows: Vec<Vec<String>> = tier
                .probes
                .iter()
                .map(|probe| {
                    vec![
                        probe.id.clone(),
                        match probe.kind {
                            ProbeKind::Positive => "positive".to_string(),
                            ProbeKind::Negative => "negative".to_string(),
                        },
                        evals::outcome_label(probe.outcome),
                        probe.acceptance_criteria.join(","),
                    ]
                })
                .collect();
            ui.payload_plain(&crate::ui::table(
                &["probe", "kind", "result", "criteria"],
                &rows,
                &[2],
            ));
        }
        ui.payload_plain(&format!(
            "{} {} on {} ({})",
            self.ticket,
            verdict_label(self.verdict),
            self.model_mode_label(),
            short_digest(&self.artifact_digest),
        ));
        if self.verdict == Verdict::PlumbingOk {
            ui.note(
                "the fake model returns one canned reply whatever the input, so no probe was \
                 graded: this run proves the turns completed, nothing more. Re-run with a real \
                 credential to grade them.",
            );
        }
        if let Some(error) = &self.error {
            ui.failure(error);
        }
        if let Some(fix) = &self.fix {
            ui.note(fix);
        }
    }
}

impl ScenarioOutput {
    fn model_mode_label(&self) -> &'static str {
        match self.model_mode {
            ModelMode::Fake => "the fake model",
            ModelMode::Live => "a live model",
        }
    }
}

/// The word a human reads for a verdict. `plumbing_ok` is spelled out as an
/// ungraded run rather than as a shade of pass (ADR-0055).
fn verdict_label(verdict: Verdict) -> &'static str {
    match verdict {
        Verdict::Passed => "passed",
        Verdict::Failed => "FAILED",
        Verdict::PlumbingOk => "completed but was NOT graded",
    }
}

/// The first 12 hex characters of a digest, enough to compare by eye.
fn short_digest(digest: &str) -> String {
    digest.chars().take(12).collect()
}

// ---------------------------------------------------------------------------
// Pure verification
// ---------------------------------------------------------------------------

/// Whether the container mounted the artifact this run packed.
///
/// Absent is a MISMATCH, never a pass: a container reporting no `/plugin` mount
/// is not running our bundle, and reading `None` as agreement is the fail-open
/// direction.
pub fn verify_mounted_identity(mount_source: Option<&str>, want_dir: &Path) -> Result<(), String> {
    let want = want_dir.display().to_string();
    match mount_source {
        Some(found) if found == want => Ok(()),
        Some(found) => Err(format!(
            "the runner container mounted {found} at {PLUGIN_MOUNT}, not the snapshot this run \
             packed ({want}), so it is not executing the artifact under test"
        )),
        None => Err(format!(
            "the runner container reports no {PLUGIN_MOUNT} mount at all, so nothing proves it \
             is executing the snapshot this run packed ({want})"
        )),
    }
}

/// Whether the container was booted on the model the manifest declares.
///
/// Grading a fake turn as a real one is the false green ADR-0055 exists to stop,
/// so a mismatch refuses before any probe runs.
pub fn verify_model_mode(
    container_fake_model: Option<bool>,
    want: ModelMode,
) -> Result<(), String> {
    let want_fake = want == ModelMode::Fake;
    match container_fake_model {
        Some(found) if found == want_fake => Ok(()),
        Some(found) => Err(format!(
            "the manifest declares model_mode {:?}, but the runner container was booted with \
             {FAKE_MODEL_ENV} {}: the model the probes would be graded against is not the one \
             the scenario asserts",
            if want_fake { "fake" } else { "live" },
            if found { "set" } else { "unset" }
        )),
        None => Err(format!(
            "the runner container's {FAKE_MODEL_ENV} could not be read, so nothing confirms the \
             model mode the manifest declares"
        )),
    }
}

/// A probe's verdict from the RAW grader result. The negative inversion lives
/// here and nowhere else: a negative control passes only when its grader does
/// NOT match, because a satisfied control means the positive probe proves
/// nothing.
pub fn probe_verdict(kind: ProbeKind, grader_matched: bool) -> CaseOutcome {
    let satisfied = match kind {
        ProbeKind::Positive => grader_matched,
        ProbeKind::Negative => !grader_matched,
    };
    if satisfied {
        CaseOutcome::Pass
    } else {
        CaseOutcome::Fail
    }
}

/// Grade one probe's turn: the outcome, the raw grader result, and the reply.
///
/// The completion gate runs first and applies to both modes: a turn that never
/// completed is a fail, never a pass and never `plumbing_ok`. Under
/// [`ModelMode::Fake`] a completed turn is `plumbing_ok` whatever the grader
/// would have said, and no control can be claimed satisfied (ADR-0055).
pub fn probe_outcome(
    probe: &Probe,
    events: &[OutboundEvent],
    mode: ModelMode,
) -> (CaseOutcome, Option<bool>, String) {
    let case = eval_case(probe);
    let answer = evals::graded_answer(events);
    let reply = if answer.trim().is_empty() {
        NO_REPLY.to_string()
    } else {
        answer.clone()
    };
    if !evals::turn_completed(&case, events) {
        return (CaseOutcome::Fail, None, reply);
    }
    if mode == ModelMode::Fake {
        return (CaseOutcome::PlumbingOk, None, reply);
    }
    let matched = case.grader.grade(&answer, &evals::trajectory(events));
    (probe_verdict(probe.kind, matched), Some(matched), reply)
}

/// The frozen eval case a probe grades as. Reuse, not a second expectation
/// language: a probe is judged by exactly the code `skill eval` uses.
fn eval_case(probe: &Probe) -> EvalCase {
    EvalCase {
        id: probe.id.clone(),
        input: probe.prompt.clone(),
        grader: Grader {
            kind: probe.expect.grader,
            expected: probe.expect.value.clone(),
            case_sensitive: false,
        },
        shared_history: false,
        expect_status: ExpectedStatus::Done,
    }
}

/// The teardown's postconditions, read from the daemon and the filesystem rather
/// than inferred from a `stop` that returned Ok.
///
/// Every read arrives as a `Result` because a read that FAILED observed nothing:
/// a daemon that could not answer is not a container that is gone, and a path
/// whose presence could not be determined is not a released snapshot or a
/// cleared record. An unverifiable postcondition is never a satisfied one.
///
/// The container takes TWO readings. `container` is the name probe, which proves
/// only that nothing holds the runner name; `booted_container_present` is keyed
/// by the id the boot recorded, and is what proves the container this run
/// actually started is gone rather than merely renamed out of the way.
pub fn teardown_postconditions(
    container: Result<Option<&docker::ContainerFacts>, &str>,
    booted_container_present: Result<bool, &str>,
    state_present: Result<bool, &str>,
    snapshot_present: Result<bool, &str>,
) -> TeardownReport {
    TeardownReport {
        attempted: true,
        container_removed: matches!(container, Ok(None))
            && matches!(booted_container_present, Ok(false)),
        state_cleared: matches!(state_present, Ok(false)),
        snapshot_released: matches!(snapshot_present, Ok(false)),
        error: None,
    }
}

/// The roll-up over probes: any fail is a fail, else any ungraded row keeps the
/// whole thing ungraded, else pass. Never derived by subtraction (ADR-0055).
fn roll_up(outcomes: impl IntoIterator<Item = CaseOutcome>) -> Verdict {
    let mut verdict = Verdict::Passed;
    for outcome in outcomes {
        match outcome {
            CaseOutcome::Fail => return Verdict::Failed,
            CaseOutcome::PlumbingOk => verdict = Verdict::PlumbingOk,
            CaseOutcome::Pass => {}
        }
    }
    verdict
}

/// The roll-up over tiers, in the same order of precedence.
fn roll_up_tiers(tiers: &[TierReport]) -> Verdict {
    let mut verdict = Verdict::Passed;
    for tier in tiers {
        match tier.verdict {
            Verdict::Failed => return Verdict::Failed,
            Verdict::PlumbingOk => verdict = Verdict::PlumbingOk,
            Verdict::Passed => {}
        }
    }
    verdict
}

// ---------------------------------------------------------------------------
// The pipeline
// ---------------------------------------------------------------------------

/// Options for `curie scenario`, mirroring its clap flags.
pub struct RunOpts {
    pub manifest: PathBuf,
}

/// One tier's accumulated failure: the diagnosis, what to do about it, and
/// whether it is retryable.
///
/// `transient` is what makes the run exit 3 rather than 1 (ADR-0021): a deadline
/// that expired says the peer never answered in time, and the same argv may well
/// succeed once it does. A red probe or a leaked container is not that.
struct Diagnosis {
    error: String,
    fix: String,
    transient: bool,
}

/// A boot that never reached a probe: the error, and the teardown that actually
/// ran because of it.
///
/// The teardown report travels WITH the error because the arms differ: the ones
/// that abort before a container exists tore nothing down, while the arm that
/// cannot read back the record did tear down and its results are the only
/// account of what was left behind.
struct BootFailure {
    error: anyhow::Error,
    teardown: TeardownReport,
}

/// The teardown report for a failure that happened before anything was booted:
/// nothing was attempted, so nothing is claimed either way.
fn teardown_not_attempted() -> TeardownReport {
    TeardownReport {
        attempted: false,
        container_removed: false,
        state_cleared: false,
        snapshot_released: false,
        error: None,
    }
}

pub async fn run(opts: RunOpts) -> Result<()> {
    let raw = std::fs::read_to_string(&opts.manifest).map_err(|err| {
        refusal(
            format!(
                "reading the scenario manifest {}: {err}",
                opts.manifest.display()
            ),
            "pass the path of a checked-in scenario manifest",
        )
    })?;
    let manifest = parse_manifest(&raw, &opts.manifest)?;

    // Refused on the TIER ALONE, before the bundle is resolved: nothing about the
    // bundle changes this answer, so a manifest that also has a bad bundle_path
    // must still get the ADR-0041 contract an agent branches on, not an
    // unrelated path failure.
    for tier in &manifest.tiers {
        if *tier != TierName::Skill {
            return Err(tier_refusal(*tier));
        }
    }

    // Resolved against the MANIFEST's directory, so a checked-in scenario is
    // portable and a relative bundle path means what its author sees.
    let manifest_dir = opts
        .manifest
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let bundle_dir = manifest_dir
        .join(&manifest.bundle_path)
        .canonicalize()
        .map_err(|err| {
            refusal(
                format!(
                    "the scenario's bundle_path does not resolve to a directory: {} ({err})",
                    manifest_dir.join(&manifest.bundle_path).display()
                ),
                "point `bundle_path` at the bundle directory, relative to the manifest's own \
                 directory so the checked-in scenario stays portable",
            )
        })?;
    let source_commit = crate::commands::git_short_sha(&bundle_dir).await;

    let start_opts = StartOpts {
        plugin_dir: bundle_dir.clone(),
        image: crate::artifacts::resolve_image(
            None,
            crate::artifacts::Channel::current(),
            crate::artifacts::version(),
        ),
        // No `--port` on this verb: the runner boots on the same default
        // `skill up` uses, the only port a caller can predict.
        port: DEFAULT_PORT,
        name: docker::RUNNER_CONTAINER_LOCAL.to_string(),
        fake_model: manifest.model_mode == ModelMode::Fake,
        network: None,
        otel_endpoint: None,
        budget: DEFAULT_BUDGET.to_string(),
        model: None,
        local_model: None,
        pull_model: false,
        secret: Vec::new(),
        env_file: None,
        // Never adopts or clobbers: a container already holding the name is the
        // existing #747 refusal, not something a scenario silently replaces.
        replace: false,
    };
    let runner_url = format!("http://localhost:{DEFAULT_PORT}");
    let plugin_dir = crate::commands::prepare_start(&start_opts).await?;

    let (artifact_digest, tier, transient, boot_error) =
        match boot_and_probe(&manifest, start_opts, &plugin_dir, &runner_url).await {
            Ok((digest, tier, transient)) => (digest, tier, transient, None),
            // A boot that never reached a probe packed nothing that survives, so
            // there is no digest to report and none is invented.
            Err(BootFailure { error, teardown }) => (
                String::new(),
                boot_failure_tier(&error, teardown, &runner_url),
                false,
                Some(error),
            ),
        };
    let tiers = vec![tier];
    let output = ScenarioOutput {
        ticket: manifest.ticket.clone(),
        bundle_path: bundle_dir.display().to_string(),
        source_commit,
        artifact_digest,
        model_mode: manifest.model_mode,
        verdict: roll_up_tiers(&tiers),
        error: tiers[0].error.clone(),
        fix: tiers[0].fix.clone(),
        tiers,
    };

    if let Some(err) = boot_error {
        // A failed boot is a failed SCENARIO: the same payload every other red
        // path emits, carrying the boot error's own exit class rather than a
        // generic `{error, fix}` object an agent cannot branch on.
        let payload = scenario_json(&output);
        return Err(exit::with_json_payload(err, payload));
    }
    if output.verdict == Verdict::Failed {
        // A red run must still emit the full payload AND exit non-zero, which
        // `Ui::emit` cannot do: the per-probe evidence is the object an agent
        // most needs to read.
        let payload = scenario_json(&output);
        let message = output
            .error
            .clone()
            .unwrap_or_else(|| "the scenario failed".to_string());
        // A deadline that expired is retryable and exits 3; everything else the
        // tier can report is a genuine failure (ADR-0021).
        let err = if transient {
            CliError::transient(message)
        } else {
            CliError::failure(message)
        };
        return Err(exit::with_json_payload(anyhow::Error::from(err), payload));
    }
    crate::ui::ui().emit(&output);
    Ok(())
}

/// Stages 1 to 5 for the skill tier: preflight, pack, boot, verify, probe, tear
/// down. Returns the packed artifact's digest and the tier row.
///
/// An `Err` here is a boot that never reached a probe, and every one of them
/// leaves nothing behind: the only destructive step before a recorded state is
/// the pack, and the single arm between it and a successful boot releases it.
async fn boot_and_probe(
    manifest: &ScenarioManifest,
    start_opts: StartOpts,
    plugin_dir: &Path,
    runner_url: &str,
) -> std::result::Result<(String, TierReport, bool), BootFailure> {
    let container_name = start_opts.name.clone();
    // Before ANYTHING destructive (#747, #1087). The pack below removes and
    // recreates the content-addressed destination, which on unchanged source is
    // the directory a runner already holding this name has mounted at /plugin,
    // so discovering the conflict after the pack means destroying a live run's
    // artifact to learn this run was never allowed to start.
    if let Err(error) = docker::ensure_container_name_free(
        &container_name,
        Some(start_opts.port),
        start_opts.replace,
        docker::ConflictContext::SkillUp,
    )
    .await
    {
        return Err(BootFailure {
            error,
            teardown: teardown_not_attempted(),
        });
    }

    // Stage 1: the ONE pack of the run, into a RUN-UNIQUE directory. Everything
    // below asserts against this exact artifact. Deliberately not the canonical
    // content-addressed `<digest>/` path: on unchanged source that path is the
    // very directory a live `skill up` runner has mounted at `/plugin`, and
    // packing removes an existing destination, so a scenario and an ordinary
    // `skill up` of the same bundle would each destroy the other's artifact.
    // `skill up` takes no lifecycle lock and never will, so the only way to keep
    // out of its way is to not share the path.
    let snapshot = match crate::bundle::snapshot_ephemeral(plugin_dir)
        .context("packaging the bundle snapshot for the scenario runner")
    {
        Ok(snapshot) => snapshot,
        Err(error) => {
            return Err(BootFailure {
                error,
                teardown: teardown_not_attempted(),
            })
        }
    };
    let artifact_digest = snapshot.digest.clone();
    let snapshot_dir = snapshot.dir.clone();

    // Stage 2: boot on exactly that artifact. Nothing records the snapshot until
    // this returns Ok, so no later teardown could ever find it: every failure
    // between the pack and here releases it right here -- and a release that
    // FAILED is a leaked artifact, so it rides out with the boot error rather
    // than being dropped.
    if let Err(err) = crate::commands::start(start_opts, snapshot).await {
        let error = match crate::bundle::remove_snapshot(&snapshot_dir, plugin_dir) {
            Ok(()) => err,
            Err(release) => err.context(format!(
                "the snapshot this run packed at {} could not be released either: {release:#}",
                snapshot_dir.display()
            )),
        };
        return Err(BootFailure {
            error,
            teardown: teardown_not_attempted(),
        });
    }

    // The immutable id `docker run` returned, as the boot recorded it. Every
    // identity observation below is keyed by it rather than by the container
    // NAME, which a second container can take over between two reads (#747).
    let container_id = match state::load(plugin_dir) {
        Ok(Some(recorded)) => recorded.container_id,
        other => {
            // The container exists whatever the record says, so it still owns a
            // teardown -- and that teardown's real results are the only account
            // of what this run left behind, so they are reported rather than
            // replaced by a never-attempted placeholder.
            let teardown = tear_down(plugin_dir, &container_name, None, &snapshot_dir).await;
            let detail = match other {
                Err(err) => format!("{err:#}"),
                _ => "no runner record was written".to_string(),
            };
            return Err(BootFailure {
                error: anyhow::anyhow!(
                    "the boot left no runner record to read the container id from ({detail}), so \
                     no observation could be bound to the container this run started"
                ),
                teardown,
            });
        }
    };

    // Stages 3 and 4, then stage 5 unconditionally: the tier owns a teardown
    // from the moment the container exists, whatever went wrong after it.
    let (probes, mut diagnosis, mounted_snapshot_dir, container_fake_model, identity_verified) =
        probe_tier(manifest, &container_id, &snapshot_dir, runner_url).await;

    let mut teardown = tear_down(
        plugin_dir,
        &container_name,
        Some(&container_id),
        &snapshot_dir,
    )
    .await;
    let mut tier_verdict = roll_up(probes.iter().map(|probe| probe.outcome));
    if diagnosis.is_none() {
        if let Some(probe) = probes
            .iter()
            .find(|probe| probe.outcome == CaseOutcome::Fail)
        {
            diagnosis = Some(probe_diagnosis(probe));
        }
    }
    if diagnosis.is_some() {
        tier_verdict = Verdict::Failed;
    }
    if let Some(error) = &teardown.error {
        // A stranded container or a leaked snapshot is a real leak and must not
        // be reported as a clean run, but it never overwrites the diagnosis the
        // tier already had.
        tier_verdict = Verdict::Failed;
        if diagnosis.is_none() {
            diagnosis = Some(Diagnosis {
                error: error.clone(),
                fix: "remove the leftovers by hand ('docker ps -a' and the bundle's .curie/ \
                      directory), then re-run the scenario"
                    .to_string(),
                transient: false,
            });
        }
    }
    teardown.attempted = true;
    let transient = diagnosis.as_ref().is_some_and(|d| d.transient);

    Ok((
        artifact_digest,
        TierReport {
            tier: TierName::Skill,
            verdict: tier_verdict,
            identity_verified,
            mounted_snapshot_dir,
            container_fake_model,
            runner_url: runner_url.to_string(),
            probes,
            teardown,
            error: diagnosis.as_ref().map(|d| d.error.clone()),
            fix: diagnosis.as_ref().map(|d| d.fix.clone()),
        },
        transient,
    ))
}

/// The tier row for a boot that never reached a probe.
///
/// Nothing was verified, so nothing is claimed: no probe ran and identity was
/// never established. `teardown` is whatever the failing path actually did --
/// the never-attempted placeholder for the arms that abort before a container
/// exists, and the real postconditions for the one that tears down.
fn boot_failure_tier(
    err: &anyhow::Error,
    teardown: TeardownReport,
    runner_url: &str,
) -> TierReport {
    TierReport {
        tier: TierName::Skill,
        verdict: Verdict::Failed,
        identity_verified: false,
        mounted_snapshot_dir: None,
        container_fake_model: None,
        runner_url: runner_url.to_string(),
        probes: Vec::new(),
        teardown,
        error: Some(format!("{err:#}")),
        fix: Some(exit::classify(err).1.unwrap_or_else(|| {
            "resolve the boot failure above, then re-run the scenario".to_string()
        })),
    }
}

/// Verify runtime identity against the daemon, then run every probe.
///
/// Returns the probe rows, the first diagnosis, and the two daemon readings.
/// A tier whose identity does not check out runs NO probe: an unverified
/// container's answers are evidence of nothing.
///
/// `container_id` is the immutable id the boot recorded, never the container
/// name: both observations below must describe ONE container, and a name can be
/// taken over by another container between two reads.
async fn probe_tier(
    manifest: &ScenarioManifest,
    container_id: &str,
    snapshot_dir: &Path,
    runner_url: &str,
) -> (
    Vec<ProbeReport>,
    Option<Diagnosis>,
    Option<String>,
    Option<bool>,
    bool,
) {
    let mounted = match docker::container_mount_source(container_id, PLUGIN_MOUNT).await {
        Ok(found) => found,
        Err(err) => {
            return (
                Vec::new(),
                Some(Diagnosis {
                    error: format!("reading the runner container's mounts: {err:#}"),
                    fix: "check that the Docker daemon is reachable, then re-run the scenario"
                        .to_string(),
                    transient: false,
                }),
                None,
                None,
                false,
            )
        }
    };
    // The RUNTIME's own reading of the value, not a third spelling of it: the
    // runner accepts only `1`/`true`/`yes` (`runner/src/curie_runner/__main__.py`),
    // so `CURIE_FAKE_MODEL=false` is a LIVE container. An ABSENT variable is live
    // too, which is exactly how a live CLI boot leaves it -- it passes no such
    // variable at all. Only a failed READ is unknown.
    let fake_model = match docker::container_env_value(container_id, FAKE_MODEL_ENV).await {
        Ok(value) => Some(
            value
                .as_deref()
                .is_some_and(crate::local::fake_model_is_truthy),
        ),
        Err(_) => None,
    };

    if let Err(error) = verify_mounted_identity(mounted.as_deref(), snapshot_dir) {
        return (
            Vec::new(),
            Some(Diagnosis {
                error,
                fix:
                    "tear down whatever is holding the runner container name ('curie skill down') \
                      and re-run the scenario so it boots on its own snapshot"
                        .to_string(),
                transient: false,
            }),
            mounted,
            fake_model,
            false,
        );
    }
    if let Err(error) = verify_model_mode(fake_model, manifest.model_mode) {
        return (
            Vec::new(),
            Some(Diagnosis {
                error,
                fix: "set the manifest's model_mode to the model the runner actually boots on, or \
                      supply a real model credential so it boots live"
                    .to_string(),
                transient: false,
            }),
            mounted,
            fake_model,
            // The model mode is half of runtime identity: a container proved to
            // be running the right artifact on the WRONG model is not the
            // artifact under test, so identity stays unverified.
            false,
        );
    }

    let client = match RunnerClient::new(runner_url) {
        Ok(client) => client,
        Err(err) => {
            return (
                Vec::new(),
                Some(Diagnosis {
                    error: format!("building the runner client for {runner_url}: {err:#}"),
                    fix: "re-run the scenario once the runner is reachable".to_string(),
                    transient: false,
                }),
                mounted,
                fake_model,
                true,
            )
        }
    };

    let mut probes = Vec::new();
    for probe in &manifest.probes {
        // Per-probe isolation (#550): a probe must not answer from an earlier
        // probe's history instead of actually doing the work.
        //
        // Deadlined, and the expiry is routed through the ordinary diagnosis
        // path: a peer that accepts the connection and never answers must end
        // the run with a report and a teardown, not wedge it.
        //
        // An expiry is TRANSIENT (ADR-0021 exit 3): it says the peer had not
        // answered yet, which re-running may well change. A reset the runner
        // actively refused is not that, so the two are told apart here rather
        // than by matching on the message downstream.
        let (reset, reset_expired) =
            match tokio::time::timeout(RESET_DEADLINE, client.reset()).await {
                Ok(result) => (result, false),
                Err(_) => (
                    Err(anyhow::anyhow!(
                    "no answer within {RESET_DEADLINE:?}; the runner accepted the connection and \
                     never replied"
                )),
                    true,
                ),
            };
        if let Err(err) = reset {
            return (
                probes,
                Some(Diagnosis {
                    error: format!("resetting the runner before probe {}: {err:#}", probe.id),
                    fix: "re-run the scenario; a runner that will not reset cannot give isolated \
                          probe results"
                        .to_string(),
                    transient: reset_expired,
                }),
                mounted,
                fake_model,
                true,
            );
        }
        // Deadlined for the same reason as the reset above, and generously: a
        // live model turn legitimately takes minutes, an unbounded one strands
        // the container, the record and the snapshot.
        let (sent, probe_expired) = match tokio::time::timeout(
            PROBE_DEADLINE,
            client.send_event(EventType::Message, &probe.prompt, "curie-scenario", |_| {}),
        )
        .await
        {
            Ok(result) => (result, false),
            Err(_) => (
                Err(anyhow::anyhow!(
                    "the turn produced no final frame within {PROBE_DEADLINE:?}"
                )),
                true,
            ),
        };
        let events =
            match sent {
                Ok(events) => events,
                Err(err) => return (
                    probes,
                    Some(Diagnosis {
                        error: format!("probe {}: the turn did not complete: {err:#}", probe.id),
                        fix: "check the runner logs for the failed turn, then re-run the scenario"
                            .to_string(),
                        transient: probe_expired,
                    }),
                    mounted,
                    fake_model,
                    true,
                ),
            };
        let (outcome, grader_matched, reply) = probe_outcome(probe, &events, manifest.model_mode);
        probes.push(ProbeReport {
            id: probe.id.clone(),
            kind: probe.kind,
            acceptance_criteria: probe.acceptance_criteria.clone(),
            outcome,
            passed: outcome.passed(),
            grader_matched,
            reply,
        });
    }
    (probes, None, mounted, fake_model, true)
}

/// The diagnosis for a red probe, worded by kind so a satisfied control does not
/// read like a grader that simply missed.
fn probe_diagnosis(probe: &ProbeReport) -> Diagnosis {
    match probe.kind {
        ProbeKind::Positive => Diagnosis {
            error: format!(
                "probe {}: the grader did not match the reply {:?}",
                probe.id, probe.reply
            ),
            fix: "fix the agent until the probe's expectation holds, or correct the probe's \
                  expectation if it was wrong"
                .to_string(),
            transient: false,
        },
        ProbeKind::Negative => Diagnosis {
            error: format!(
                "probe {}: the negative control's grader MATCHED the reply {:?}, so the positive \
                 probes prove nothing",
                probe.id, probe.reply
            ),
            fix: "the control is meant to fail its grader: fix the agent, or choose a control \
                  expectation the agent genuinely does not satisfy"
                .to_string(),
            transient: false,
        },
    }
}

/// Stage 5: tear down, then ASSERT the three postconditions.
///
/// `skill down` is deliberately tolerant of a snapshot that will not delete
/// (#323, its agent consumer needs teardown to succeed); a scenario does not
/// inherit that tolerance, so it checks the outcome itself.
async fn tear_down(
    plugin_dir: &Path,
    container_name: &str,
    container_id: Option<&str>,
    snapshot_dir: &Path,
) -> TeardownReport {
    let stop_error = crate::commands::stop(plugin_dir, None)
        .await
        .err()
        .map(|err| format!("tearing down the runner: {err:#}"));
    // `stop` releases the snapshot only through the runner record, and every arm
    // that cannot read that record (or that aborts before reaching the release)
    // leaves it on disk. The snapshot is RUN-UNIQUE, so nothing later reuses that
    // directory: it is stranded forever. This run packed it and holds the path in
    // memory, so the release is repeated here, independently of the record. It is
    // idempotent -- a directory already gone is not an error.
    let release_error = crate::bundle::remove_snapshot(snapshot_dir, plugin_dir)
        .err()
        .map(|err| {
            format!(
                "the snapshot this run packed at {} could not be released: {err:#}",
                snapshot_dir.display()
            )
        });
    // Every read keeps its error class. Collapsing a daemon failure into "no
    // container", or a metadata failure into "the path does not exist", reports
    // an observation that was never made -- and in the fail-OPEN direction.
    let container = docker::container_facts(container_name)
        .await
        .map_err(|err| {
            format!("whether container '{container_name}' was removed could not be read: {err:#}")
        });
    // The name probe above says nothing about the container this run booted: a
    // container renamed out of the way frees the name and keeps running. An id
    // this run never got to record is an id whose removal nothing can verify,
    // which is not the same as a removal.
    let booted_present = match container_id {
        Some(id) => docker::container_id_present(id).await.map_err(|err| {
            format!(
                "whether the container this run booted (id {id}) was removed could not be read: \
                 {err:#}"
            )
        }),
        None => Err(
            "the boot recorded no container id, so nothing could verify that the container this \
             run started was removed"
                .to_string(),
        ),
    };
    let state_path = plugin_dir.join(state::STATE_DIR).join(state::STATE_FILE);
    let state_present = state_path.try_exists().map_err(|err| {
        format!(
            "whether the runner record at {} was cleared could not be read: {err}",
            state_path.display()
        )
    });
    let snapshot_present = snapshot_dir.try_exists().map_err(|err| {
        format!(
            "whether the bundle snapshot at {} was released could not be read: {err}",
            snapshot_dir.display()
        )
    });
    let mut report = teardown_postconditions(
        container
            .as_ref()
            .map(Option::as_ref)
            .map_err(String::as_str),
        booted_present.as_ref().copied().map_err(String::as_str),
        state_present.as_ref().copied().map_err(String::as_str),
        snapshot_present.as_ref().copied().map_err(String::as_str),
    );

    let mut unmet = Vec::new();
    if let Some(err) = release_error {
        unmet.push(err);
    }
    match &container {
        Ok(Some(facts)) => unmet.push(format!(
            "container '{container_name}' (id {}) is still present after teardown",
            facts.id
        )),
        Ok(None) => {}
        Err(err) => unmet.push(err.clone()),
    }
    match (&booted_present, container_id) {
        (Ok(true), Some(id)) => unmet.push(format!(
            "the container this run booted (id {id}) is still present after teardown, whatever \
             holds its name now"
        )),
        (Err(err), _) => unmet.push(err.clone()),
        _ => {}
    }
    match &state_present {
        Ok(true) => unmet.push(format!(
            "the runner record is still in {}",
            plugin_dir.join(state::STATE_DIR).display()
        )),
        Ok(false) => {}
        Err(err) => unmet.push(err.clone()),
    }
    match &snapshot_present {
        Ok(true) => unmet.push(format!(
            "the bundle snapshot is still on disk at {}",
            snapshot_dir.display()
        )),
        Ok(false) => {}
        Err(err) => unmet.push(err.clone()),
    }
    report.error = match (stop_error, unmet.is_empty()) {
        (Some(err), true) => Some(err),
        (Some(err), false) => Some(format!("{err}; {}", unmet.join("; "))),
        (None, false) => Some(unmet.join("; ")),
        (None, true) => None,
    };
    report
}

#[cfg(test)]
mod tests {
    use super::*;
    use curie_aci_protocol::PROTOCOL_VERSION;

    fn probe(kind: ProbeKind) -> Probe {
        Probe {
            id: "ac1".into(),
            kind,
            acceptance_criteria: vec!["AC1".into()],
            prompt: "What is the weather in Oslo?".into(),
            expect: Expect {
                grader: GraderKind::Contains,
                value: "Oslo".into(),
            },
        }
    }

    /// One completed turn, built through the frozen wire shape rather than by
    /// hand, so it cannot drift from what a runner actually sends.
    fn final_frame(text: &str) -> Vec<OutboundEvent> {
        let frame = serde_json::json!({
            "type": "final",
            "version": PROTOCOL_VERSION,
            "text": text,
            "status": "done",
        });
        vec![serde_json::from_value(frame).expect("the frame is a valid outbound event")]
    }

    #[test]
    fn a_negative_control_passes_only_when_its_grader_does_not_match() {
        assert_eq!(
            probe_verdict(ProbeKind::Negative, false),
            CaseOutcome::Pass,
            "an unsatisfied control is the healthy case"
        );
        assert_eq!(
            probe_verdict(ProbeKind::Negative, true),
            CaseOutcome::Fail,
            "a satisfied control means the positive probe proves nothing"
        );
        assert_eq!(probe_verdict(ProbeKind::Positive, true), CaseOutcome::Pass);
        assert_eq!(probe_verdict(ProbeKind::Positive, false), CaseOutcome::Fail);
    }

    // ADR-0055: the fake model is a plumbing fixture, so neither direction of
    // the grader may be reported as a graded verdict.
    #[test]
    fn a_fake_model_turn_is_plumbing_ok_whichever_way_the_grader_would_have_gone() {
        let would_pass = probe_outcome(
            &probe(ProbeKind::Positive),
            &final_frame("It is 4C in Oslo."),
            ModelMode::Fake,
        );
        assert_eq!(would_pass.0, CaseOutcome::PlumbingOk);
        assert_eq!(
            would_pass.1, None,
            "nothing was graded, so nothing is claimed"
        );

        let would_fail = probe_outcome(
            &probe(ProbeKind::Positive),
            &final_frame("I have no idea."),
            ModelMode::Fake,
        );
        assert_eq!(would_fail.0, CaseOutcome::PlumbingOk);
        assert_eq!(would_fail.1, None);
    }

    #[test]
    fn a_turn_that_never_completed_is_a_fail_not_plumbing_ok() {
        let (outcome, matched, reply) =
            probe_outcome(&probe(ProbeKind::Positive), &[], ModelMode::Fake);
        assert_eq!(
            outcome,
            CaseOutcome::Fail,
            "a turn with no final frame is a genuine failure at either model mode"
        );
        assert_eq!(matched, None);
        assert_eq!(reply, NO_REPLY, "the red case must be diagnosable");
    }

    #[test]
    fn a_fake_model_run_rolls_up_as_ungraded_never_as_passed() {
        assert_eq!(
            roll_up([CaseOutcome::PlumbingOk, CaseOutcome::PlumbingOk]),
            Verdict::PlumbingOk
        );
        assert_eq!(
            roll_up([CaseOutcome::PlumbingOk, CaseOutcome::Fail]),
            Verdict::Failed
        );
        assert_eq!(roll_up([CaseOutcome::Pass]), Verdict::Passed);
    }

    #[test]
    fn an_absent_plugin_mount_is_a_mismatch_not_a_pass() {
        let want = Path::new("/bundle/.curie/snapshots/abc");
        assert!(verify_mounted_identity(None, want).is_err());
        assert!(verify_mounted_identity(Some("/somewhere/else"), want).is_err());
        assert!(verify_mounted_identity(Some("/bundle/.curie/snapshots/abc"), want).is_ok());
    }

    #[test]
    fn an_unreadable_model_mode_is_a_refusal_not_an_assumption() {
        assert!(verify_model_mode(None, ModelMode::Live).is_err());
        assert!(verify_model_mode(Some(true), ModelMode::Live).is_err());
        assert!(verify_model_mode(Some(false), ModelMode::Live).is_ok());
        assert!(verify_model_mode(Some(true), ModelMode::Fake).is_ok());
    }

    #[test]
    fn an_unmet_postcondition_is_reported_from_the_observation_not_the_call() {
        let report = teardown_postconditions(
            Ok(Some(&docker::ContainerFacts {
                id: "c0ffee".into(),
                cli_managed: true,
            })),
            Ok(false),
            Ok(false),
            Ok(false),
        );
        assert!(!report.container_removed);
        assert!(report.state_cleared && report.snapshot_released);
    }

    // A container renamed out of the way frees the NAME and keeps running under
    // the id the boot recorded, so the name probe alone cannot report a removal.
    #[test]
    fn a_container_still_running_under_its_recorded_id_is_not_a_removed_container() {
        let report = teardown_postconditions(Ok(None), Ok(true), Ok(false), Ok(false));
        assert!(
            !report.container_removed,
            "a free name proves only that nothing holds the name"
        );
        assert!(
            teardown_postconditions(Ok(None), Ok(false), Ok(false), Ok(false)).container_removed,
            "both readings agreeing the container is gone is the only removal"
        );
    }

    #[test]
    fn a_postcondition_whose_read_failed_is_never_reported_satisfied() {
        let report = teardown_postconditions(
            Err("the daemon refused"),
            Err("the daemon refused"),
            Err("EACCES"),
            Err("EACCES"),
        );
        assert!(
            !report.container_removed,
            "a daemon that could not answer observed no removal"
        );
        assert!(
            !report.state_cleared,
            "a record whose presence could not be read is not a cleared record"
        );
        assert!(
            !report.snapshot_released,
            "a presence that could not be read is not an absence"
        );
    }
}

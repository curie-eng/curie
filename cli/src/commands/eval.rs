//! `skill eval` and `eval --sweep`: run eval suites and report results.

use super::*;

/// One eval case's result: `(id, outcome, seconds, output)`. `output` is the
/// graded answer text (the reply `turn_outcome`/`reply_passes` judged), carried
/// so a red case is diagnosable from `--json` without a manual re-run (#548).
/// Shared by the skill runner path and the local/cluster message path so both
/// report the same shape through `report_eval`/`eval_json`.
pub type EvalRow = (String, CaseOutcome, f64, String);

/// Eval rows plus optional scorer explanations keyed by case id.
pub struct EvalReport {
    pub rows: Vec<EvalRow>,
    pub details: BTreeMap<String, String>,
    /// Sampling policy this run used (#1907). Default n=1 majority.
    pub sampling: crate::eval_sampling::SampleConfig,
    /// Per-case count of samples that passed, keyed by case id.
    pub sample_passes: BTreeMap<String, u32>,
}

impl EvalReport {
    pub fn from_rows(rows: Vec<EvalRow>) -> Self {
        Self::with_details(rows, BTreeMap::new())
    }

    pub fn with_details(rows: Vec<EvalRow>, details: BTreeMap<String, String>) -> Self {
        let sample_passes = n1_sample_passes(&rows);
        Self {
            rows,
            details,
            sampling: crate::eval_sampling::SampleConfig::default(),
            sample_passes,
        }
    }
}

pub(super) fn n1_sample_passes(rows: &[EvalRow]) -> BTreeMap<String, u32> {
    rows.iter()
        .map(|(id, outcome, _, _)| (id.clone(), u32::from(*outcome == CaseOutcome::Pass)))
        .collect()
}

/// The three counts every eval surface reports. Split out so the `--json`
/// payload, the human roll-up, and the exit code all read the SAME tally rather
/// than each re-deriving it -- `failed` in particular must be counted, never
/// inferred as `total - passed`, which would book every non-graded plumbing row
/// as a failure (#606).
pub(super) fn eval_counts(results: &[EvalRow]) -> (usize, usize, usize) {
    let count = |want: CaseOutcome| results.iter().filter(|(_, o, _, _)| *o == want).count();
    (
        count(CaseOutcome::Pass),
        count(CaseOutcome::Fail),
        count(CaseOutcome::PlumbingOk),
    )
}

/// The `curie skill eval --json` payload: the outcome roll-up plus one row per
/// case. Pure so it stays unit/contract-testable against
/// `cli/schema/eval.schema.json`.
///
/// `bundle_digest` (#1087 AC2) is the sha256 of the snapshot the evaluated
/// runner mounted, carried on the MACHINE surface so an agent can confirm
/// `skill message` and `skill eval` executed the same bundle without reading a
/// human note off stderr. The key is always emitted -- `null` at the
/// local/cluster tiers, which evaluate a deployed version rather than a locally
/// snapshotted bundle, and on a skill run against a runner this checkout never
/// recorded.
pub fn eval_json(results: &[EvalRow], bundle_digest: Option<&str>) -> serde_json::Value {
    eval_json_with_details(&EvalReport::from_rows(results.to_vec()), bundle_digest)
}

pub(super) fn eval_json_with_details(
    report: &EvalReport,
    bundle_digest: Option<&str>,
) -> serde_json::Value {
    // Derive every count from `results` in one pass so the rollup can never
    // disagree with the per-case rows (no caller-supplied passed/total to drift).
    let results = &report.rows;
    let total = results.len();
    let (passed, failed, plumbing_ok) = eval_counts(results);
    let n = report.sampling.n;
    let policy = report.sampling.policy.as_str();
    let cases: Vec<serde_json::Value> = results
        .iter()
        .map(|(id, outcome, seconds, output)| {
            let sample_passes = report.sample_passes.get(id).copied().unwrap_or(0);
            let mut row = serde_json::json!({
                "id": id,
                "outcome": outcome,
                // Tri-state (ADR-0055): a non-graded row claims neither verdict.
                // `null` keeps a truthiness reader fail-safe (it under-reports,
                // never false-greens) without ever alleging a failure that did
                // not happen.
                "passed": outcome.passed(),
                "seconds": seconds,
                "output": output,
                "samples": n,
                "passes": sample_passes,
                "policy": policy,
            });
            if let Some(detail) = report.details.get(id) {
                row["detail"] = serde_json::json!(detail);
            }
            if n > 1 {
                let bar = match report.sampling.policy {
                    crate::eval_sampling::AggregationPolicy::PassAtK => {
                        format!("pass@{}", report.sampling.effective_k())
                    }
                    crate::eval_sampling::AggregationPolicy::Majority => "majority".to_string(),
                };
                row["variance"] =
                    serde_json::json!(format!("{sample_passes}/{n} samples passed ({bar})"));
            }
            row
        })
        .collect();
    serde_json::json!({
        "total": total,
        "passed": passed,
        "failed": failed,
        "plumbing_ok": plumbing_ok,
        "bundle_digest": bundle_digest,
        "samples": n,
        "policy": policy,
        "cases": cases,
    })
}

pub async fn eval(
    cases_path: Option<PathBuf>,
    case_ids: Vec<String>,
    url: Option<String>,
    models: Vec<String>,
    secrets: Vec<String>,
    image: String,
    sampling: crate::eval_sampling::SampleConfig,
) -> Result<()> {
    let saved = state::load(Path::new("."))?;
    let state_plugin_dir = saved.as_ref().map(|s| PathBuf::from(s.plugin_dir.clone()));

    // Model selection (#526): with `--model`, boot a transient runner per model,
    // run the suite against each, and report pass-rate per model -- the one
    // command a "can we move to a cheaper model" decision needs, instead of a
    // manual `skill up --model X` + `skill eval` loop per model. Without it, the
    // default path drives the already-running runner (whatever model it booted).
    if !models.is_empty() {
        let recorded_snapshot_dir = sweep_snapshot(saved.as_ref()).map(|(dir, _)| dir);
        let cases_path = resolve_cases_path(
            cases_path,
            Path::new("."),
            recorded_snapshot_dir.as_deref(),
            state_plugin_dir.as_deref(),
        )?;
        let loaded = load_eval(&cases_path)?;
        let total_cases = loaded.suite.cases.len();
        let trajectory = loaded.trajectory;
        // Unlike the local/cluster sweep -- the platform eval plane, which
        // `POST /evals/trigger`s a suite NAME and lets the worker reload the
        // deployed suite server-side, so a local selection can never reach it --
        // the skill-tier sweep boots a transient LOCAL runner per model and runs
        // the suite in-CLI via `run_suite_cases`. A selection made here DOES
        // reach the run, so it is honored rather than refused.
        let suite = crate::evals::select_cases(loaded.suite, &case_ids)?;
        if let Some(note) = crate::evals::selection_note(&case_ids, suite.cases.len(), total_cases)
        {
            crate::ui::ui().note(&note);
        }
        return eval_sweep(
            &suite,
            trajectory.as_ref(),
            &models,
            &secrets,
            &image,
            sweep_snapshot(saved.as_ref()),
            state_plugin_dir.as_deref(),
            sampling,
        )
        .await;
    }

    let fake = drives_a_fake_runner(saved.as_ref(), url.as_deref());
    let url = resolve_url(url)?;
    let recorded_snapshot_dir = saved
        .as_ref()
        .filter(|saved| saved.base_url == url)
        .and_then(|saved| saved.bundle_snapshot_dir.as_ref())
        .map(PathBuf::from);
    let cases_path = resolve_cases_path(
        cases_path,
        Path::new("."),
        recorded_snapshot_dir.as_deref(),
        state_plugin_dir.as_deref(),
    )?;
    let loaded = load_eval(&cases_path)?;
    let total_cases = loaded.suite.cases.len();
    let trajectory = loaded.trajectory;
    // A selector that matches nothing exits 2 before any runner contact, so a
    // mistyped --case-id fails the gate rather than greening an empty run.
    let suite = crate::evals::select_cases(loaded.suite, &case_ids)?;
    // #1087 AC2: the bundle this eval graded, on the machine surface, so an
    // agent can confirm it is the SAME digest `skill status`/`skill message`
    // report without reading a human note off stderr (docs/agents.md bans
    // stderr as agent-facing evidence). The honesty rule itself lives in
    // `recorded_bundle_digest`, shared with `status`.
    let bundle_digest = recorded_bundle_digest(saved.as_ref(), &url);
    let client = RunnerClient::new(&url)?;
    let ui = crate::ui::ui();
    if let Some(note) = crate::evals::selection_note(&case_ids, suite.cases.len(), total_cases) {
        ui.note(&note);
    }
    // `run_suite_cases` also tallies completion for the `--model` sweep path;
    // the single-runner report doesn't need the count (it already reports the
    // per-case `Fail` either way and exits on any of them), so it is discarded.
    let bar = ui.progress_bar(
        (suite.cases.len() as u64).saturating_mul(u64::from(sampling.n)),
        "running evals",
    );
    let (results, _completed) =
        run_suite_cases(&client, &suite, fake, trajectory.as_ref(), sampling, |_| {
            bar.inc(1)
        })
        .await?;
    bar.finish();

    report_eval(&results, bundle_digest.as_deref(), ())
}

/// Whether the runner `skill eval` is about to drive is the fake. Learned from
/// `.curie/runner.json` -- the CLI's own record of the runner IT booted, not a
/// guess at the shell env.
///
/// An explicit `--url` that is not the recorded runner points somewhere the
/// saved state says nothing about, so the recorded fake-ness does not transfer
/// and the run stays graded. `resolve_url`'s precedence is explicit-wins, so
/// this must mirror it: absent or matching URL only.
pub(super) fn drives_a_fake_runner(
    saved: Option<&state::RunnerState>,
    explicit_url: Option<&str>,
) -> bool {
    match saved {
        Some(s) if s.fake_model => explicit_url.is_none_or(|u| u == s.base_url),
        _ => false,
    }
}

/// Run every case in `suite` against a runner, returning `(id, outcome, seconds,
/// output)` rows plus how many cases *completed* (reached a `final` matching
/// `expect_status`, independent of whether the grader then agreed). `fake` says
/// the runner is the fake model, in which case the cases are not graded at all.
/// `on_case` is called once per completed case (progress). Shared by the
/// single-runner path and the per-model sweep so both judge identically; the
/// completed count is what lets the sweep tell a real 0% apart from a model
/// that never produced one completed turn (#622, #526 AC4) -- `CaseOutcome`
/// alone collapses both into the same `Fail`.
pub(super) async fn run_suite_cases(
    client: &RunnerClient,
    suite: &EvalSuite,
    fake: bool,
    trajectory_scorer: Option<&TrajectoryScorer>,
    sampling: crate::eval_sampling::SampleConfig,
    mut on_sample: impl FnMut(usize),
) -> Result<(EvalReport, usize)> {
    let mut results = Vec::with_capacity(suite.cases.len());
    let mut details = BTreeMap::new();
    let mut sample_passes = BTreeMap::new();
    let mut completed = 0usize;
    for case in &suite.cases {
        let mut samples = Vec::with_capacity(sampling.n as usize);
        let mut case_completed = false;
        for _ in 0..sampling.n {
            // Fresh conversation before every sample (#550 / #1907): two samples
            // of the same case must each start clean, or the second inherits the
            // first's turn. A shared_history case still skips the reset.
            if !case.shared_history {
                client.reset().await.with_context(|| {
                    format!(
                        "resetting the runner conversation before case {:?}",
                        case.id
                    )
                })?;
            }
            let started = Instant::now();
            let events = client
                .send_event(
                    EventType::EvalCase,
                    &case.input,
                    case.sender.as_deref().unwrap_or("U-eval"),
                    |_| {},
                )
                .await?;
            let elapsed = started.elapsed().as_secs_f64();
            let sample_completed = turn_completed(case, &events);
            if sample_completed {
                case_completed = true;
            }
            let scored = score_turn(case, &events, fake, trajectory_scorer);
            if let Some(detail) = scored.detail.clone() {
                details.insert(case.id.clone(), detail);
            }
            samples.push(crate::eval_sampling::SampleRecord {
                outcome: scored.outcome,
                output: graded_answer(&events),
                seconds: elapsed,
                error: if sample_completed {
                    None
                } else {
                    Some("turn did not complete".into())
                },
            });
            on_sample(0);
        }
        if case_completed {
            completed += 1;
        }
        let agg = crate::eval_sampling::aggregate_samples(&samples, sampling);
        sample_passes.insert(case.id.clone(), agg.passes);
        if let Some(variance) = &agg.variance {
            details
                .entry(case.id.clone())
                .or_insert_with(|| variance.clone());
        }
        results.push((case.id.clone(), agg.outcome, agg.seconds, agg.output));
    }
    Ok((
        EvalReport {
            rows: results,
            details,
            sampling,
            sample_passes,
        },
        completed,
    ))
}

/// The `docker run` spec one eval-sweep runner boots with. Split out of
/// [`boot_eval_runner`] (which needs a Docker daemon and the host credential
/// store) so the mount that actually reaches the daemon is unit-testable: the
/// #1087 AC2 seam ends in `run_args()`, not in a struct field. `boot_eval_runner`
/// builds its spec ONLY through here, so there is one place the eval path names
/// what it mounts, and that place can take nothing but an [`EvalBundle`].
pub(super) fn eval_runner_spec(
    bundle: &EvalBundle,
    image: &str,
    port: u16,
    name: &str,
    model: &str,
    passthrough_env: Vec<String>,
    docker_env: Vec<(String, String)>,
) -> StartSpec {
    StartSpec {
        image: image.to_string(),
        container_name: name.to_string(),
        host_port: port,
        plugin_dir: bundle.dir().to_path_buf(),
        session_id: format!("eval-{}", unix_now()),
        sandbox_id: "local".into(),
        budget_json: DEFAULT_BUDGET.to_string(),
        fake_model: false,
        network: None,
        otel_endpoint: None,
        model_base_url: None,
        model: Some(model.to_string()),
        passthrough_env,
        docker_env,
    }
}

/// Boot a throwaway runner for one model on `port`, forwarding the model
/// credential and any `--secret` from the env or the host vault exactly like
/// `skill up` (never in argv). Returns its base URL; the caller removes the
/// container when done. Does NOT touch `.curie/runner.json`, so a sweep never
/// clobbers a persistent `skill up` runner's recorded state.
///
/// `bundle` is a materialized bundle snapshot (#1087) -- the recorded runner's,
/// or one this sweep packed -- and, being an [`EvalBundle`], cannot be a source
/// directory: the skill tier executes an immutable bundle, so handing this the
/// editable source would reopen exactly the gap the snapshot closes, and the
/// type is what stops a caller doing it.
pub(super) async fn boot_eval_runner(
    bundle: &EvalBundle,
    image: &str,
    port: u16,
    name: &str,
    model: &str,
    secrets: &[String],
) -> Result<String> {
    // Same name-conflict preflight as `skill up` (#747), with the remedies the
    // sweep actually has: never --replace, since a concurrent sweep's container
    // must not be force-removed out from under it.
    docker::ensure_container_name_free(name, Some(port), false, docker::ConflictContext::EvalSweep)
        .await?;
    // Real-model run: forward the model credential (env or vault) and the
    // bundle's --secret connector secrets, mirroring `start`'s resolution.
    let mut docker_env = load_model_credentials_from_secret_store()?;
    let byo_credential = std::env::var("CURIE_CREDENTIALS").ok().or_else(|| {
        stored_env_contains(&docker_env, "CURIE_CREDENTIALS").then_some("stored".to_string())
    });
    for secret in secrets {
        if std::env::var_os(secret).is_none() && !stored_env_contains(&docker_env, secret) {
            if let Some(pair) = secret_store_env(secret)? {
                docker_env.push(pair);
            }
        }
    }
    // Scoped so the borrow of `docker_env` ends before it is moved into the spec.
    let passthrough_env = {
        let ambient_present = ambient_present_for(&docker_env);
        merge_secret_env(
            select_passthrough_env(false, false, byo_credential.as_deref(), &ambient_present),
            secrets,
        )
    };
    let spec = eval_runner_spec(
        bundle,
        image,
        port,
        name,
        model,
        passthrough_env,
        docker_env,
    );
    docker::docker_with_env(&spec.run_args(), &spec.docker_env)
        .await
        .with_context(|| format!("booting eval runner for model {model}"))
        // A container created between the preflight and here still loses the
        // race; report the sweep's remedies, not docker's raw conflict (#747).
        .map_err(|err| {
            docker::map_name_conflict(err, name, Some(port), docker::ConflictContext::EvalSweep)
        })?;
    let url = format!("http://localhost:{port}");
    if let Err(err) = RunnerClient::new(&url)?
        .wait_healthy(Duration::from_secs(60))
        .await
    {
        let logs = docker::container_logs(name, 40).await;
        let _ = docker::remove_container(name).await;
        bail!("eval runner for model {model} failed to become healthy: {err}\n{logs}");
    }
    Ok(url)
}

/// One model's row in a `--model` sweep report: pass-rate, total, how many
/// completed (issue #622, #526 AC4), and how many of its rows were
/// plumbing-only (ran but never graded, ADR-0055, #612/#606). `completed` is
/// a subset of `total`: the graded rows whose turn actually reached a verdict
/// (`expect_status` matched, whatever the grader then said -- see
/// `evals::turn_completed`), as opposed to a graded fail that never completed
/// at all (a classified failure, the wrong terminal status, or a
/// transport/runner exception). `total > 0 && completed == 0` is a model that
/// never produced one completed turn across the whole suite -- distinct from
/// a real 0%, which the sweep reports and never gates on; `CaseOutcome` alone
/// cannot tell the two apart, since `turn_outcome` collapses both into `Fail`.
/// `plumbing` is always `0` on the in-CLI skill sweep (`eval_sweep` below,
/// which always boots a real, non-fake runner); it is populated from the
/// platform eval matrix's `EvalModelSummary.plumbing` on the `local`/`cluster`
/// `--model` sweep (`message.rs`'s `scoped_rows`), where a fake-model tier row
/// can legitimately have `total == 0` and `plumbing > 0` (#700). Shared by all
/// three tiers: the skill sweep boots throwaway runners and grades in-CLI,
/// local/cluster read the platform's `EvalModelSummary` -- `report_sweep` is
/// the single point that renders and gates a sweep however its rows were
/// produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SweepRow {
    pub model: String,
    pub passed: usize,
    pub completed: usize,
    pub total: usize,
    pub plumbing: usize,
}

impl SweepRow {
    /// A row with zero graded cases and at least one plumbing row is entirely
    /// a fixture: every case that ran for this model was plumbing, so `passed`
    /// and `total` carry no real signal at all (not "0% failing", but "never
    /// graded"). Distinguishing this from a genuine 0/0 (no cases assigned)
    /// keeps a plumbing-only model from reading as a failing real result.
    pub fn is_plumbing_only(&self) -> bool {
        self.total == 0 && self.plumbing > 0
    }

    /// A model that produced zero completed turns across the whole suite: the
    /// distinct "never answered" outcome, not a real (if unlucky) 0%. Guarded
    /// on `total > 0` so a row with no cases at all is never mistaken for
    /// this -- and, since a plumbing-only row also has `total == 0`, this and
    /// `is_plumbing_only` are mutually exclusive by construction.
    pub fn never_completed(&self) -> bool {
        self.total > 0 && self.completed == 0
    }

    fn pass_rate(&self) -> f64 {
        if self.total > 0 {
            self.passed as f64 / self.total as f64
        } else {
            0.0
        }
    }
}

/// The snapshot the model sweep must mount (#1087 AC2). Reusing the recorded
/// runner's snapshot is what makes `skill message` and `skill eval` report the
/// SAME digest; `None` means no runner is recorded (or the record predates
/// #1087), and the sweep packs its own snapshot rather than falling back to
/// mutable source. Pure so the sibling path is testable without Docker.
pub(super) fn sweep_snapshot(saved: Option<&state::RunnerState>) -> Option<(PathBuf, String)> {
    let saved = saved?;
    // Both halves or nothing: a directory with no digest has nothing to report,
    // and a digest with no directory has nothing to mount.
    let dir = saved.bundle_snapshot_dir.as_ref()?;
    let digest = saved.bundle_digest.as_ref()?;
    Some((PathBuf::from(dir), digest.clone()))
}

/// What the model sweep mounts (#1087 AC2). Pure decision, split out of
/// `eval_sweep` so the wiring regression -- a sweep handing SOURCE to
/// `boot_eval_runner` -- reds a unit test instead of only the live run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum SweepMount {
    /// Mount the recorded runner's snapshot at this path, under this digest.
    /// No re-pack: reusing the recorded snapshot is what makes the eval digest
    /// the SAME value as the messaging digest, not merely equal-by-recompute.
    Recorded { dir: PathBuf, digest: String },
    /// No runner recorded: pack an EPHEMERAL snapshot from this source dir.
    /// Never a fall-back to mounting the source itself.
    PackEphemeral { source: PathBuf },
}

/// Resolve the sweep's mount. A recorded snapshot always wins, even when a
/// bundle source is known too -- that is the decision `eval_sweep` must not
/// re-make in its own body. There is deliberately no variant that mounts the
/// editable source: that is the hole #1087 closes.
pub(super) fn resolve_sweep_mount(
    recorded: Option<(PathBuf, String)>,
    state_plugin_dir: Option<&Path>,
) -> SweepMount {
    match recorded {
        Some((dir, digest)) => SweepMount::Recorded { dir, digest },
        None => SweepMount::PackEphemeral {
            source: state_plugin_dir
                .map(Path::to_path_buf)
                .unwrap_or_else(|| PathBuf::from(".")),
        },
    }
}

pub use eval_bundle::EvalBundle;

/// Home of [`EvalBundle`], the only directory type the eval runner path accepts.
///
/// It is a module rather than a bare struct so its fields are private to it:
/// nothing in `commands` -- `eval_sweep` above all -- can build one out of a
/// path it happens to be holding. That is the #1087 AC2 wiring guard. Before
/// this, restoring the old source-directory mount was a one-line variable swap
/// in `eval_sweep` that no unit test could see, because `eval_sweep` needs
/// Docker to run at all. Now that swap does not compile, and the only place the
/// eval path can still name a directory to mount is
/// [`EvalBundle::materialize`], which `cargo test` covers directly.
mod eval_bundle {
    use super::SweepMount;
    use anyhow::{Context, Result};
    use std::path::{Path, PathBuf};

    /// A materialized, immutable bundle snapshot plus the digest that names it.
    ///
    /// Construct only via [`EvalBundle::materialize`]. There is deliberately no
    /// constructor taking a bare path: a source directory cannot become one of
    /// these without editing this module.
    #[derive(Debug)]
    pub struct EvalBundle {
        dir: PathBuf,
        digest: String,
        /// The bundle source this snapshot was packed from, and only when THIS
        /// value owns the snapshot: a recorded runner's snapshot belongs to
        /// that runner's record and is released by `skill down`, never here.
        ephemeral_source: Option<PathBuf>,
    }

    impl EvalBundle {
        /// Turn a resolved [`SweepMount`] into a directory that exists on disk:
        /// canonicalize the recorded snapshot, or pack an ephemeral one from
        /// source. Neither arm may yield the source directory itself -- that is
        /// the whole point of #1087, and it is what the unit tests pin.
        ///
        /// `pub(in crate::commands)` rather than `pub`: the only caller is
        /// `eval_sweep` (and the unit tests), and [`SweepMount`] is private to
        /// `commands`, so a wider visibility would only leak that type out of
        /// the module.
        pub(in crate::commands) fn materialize(mount: SweepMount) -> Result<Self> {
            match mount {
                SweepMount::Recorded { dir, digest } => Ok(Self {
                    dir: dir
                        .canonicalize()
                        .context("resolving the recorded bundle snapshot for the model sweep")?,
                    digest,
                    ephemeral_source: None,
                }),
                SweepMount::PackEphemeral { source } => {
                    let source = source
                        .canonicalize()
                        .context("resolving the bundle directory for the model sweep")?;
                    let snapshot = crate::bundle::snapshot_ephemeral(&source)
                        .context("packaging the bundle snapshot for the model sweep")?;
                    Ok(Self {
                        dir: snapshot.dir,
                        digest: snapshot.digest,
                        ephemeral_source: Some(source),
                    })
                }
            }
        }

        /// The directory to mount read-only at `/plugin`.
        pub fn dir(&self) -> &Path {
            &self.dir
        }

        /// The sha256 this bundle is content-addressed by -- the same value
        /// `skill status`/`skill message` report when the snapshot is the
        /// recorded runner's (#1087 AC2).
        pub fn digest(&self) -> &str {
            &self.digest
        }

        /// The source dir to release this snapshot against when the run ends,
        /// or `None` when the snapshot is not this value's to remove.
        pub fn ephemeral_source(&self) -> Option<&Path> {
            self.ephemeral_source.as_deref()
        }
    }
}

/// Run the suite once per model in a fresh runner and report pass-rate per model.
#[allow(clippy::too_many_arguments)]
pub(super) async fn eval_sweep(
    suite: &EvalSuite,
    trajectory_scorer: Option<&TrajectoryScorer>,
    models: &[String],
    secrets: &[String],
    image: &str,
    recorded: Option<(PathBuf, String)>,
    state_plugin_dir: Option<&Path>,
    sampling: crate::eval_sampling::SampleConfig,
) -> Result<()> {
    let ui = crate::ui::ui();
    // The mount is decided once, purely, by `resolve_sweep_mount`, then
    // materialized once by `EvalBundle`; this body only acts on the result and
    // has no path of its own it could mount instead.
    let bundle = EvalBundle::materialize(resolve_sweep_mount(recorded, state_plugin_dir))?;
    ui.note(&format!(
        "model sweep: bundle {} ({})",
        bundle.digest(),
        if bundle.ephemeral_source().is_some() {
            "packed"
        } else {
            "recorded runner's snapshot"
        }
    ));
    ui.note(&format!(
        "model sweep: {} model(s) x {} case(s)",
        models.len(),
        suite.cases.len()
    ));
    let cl = ui.checklist();
    let sweep = async {
        let mut rows: Vec<SweepRow> = Vec::with_capacity(models.len());
        for (i, model) in models.iter().enumerate() {
            let name = format!("curie-eval-sweep-{i}");
            let port = DEFAULT_PORT + 100 + i as u16;
            let step = cl.step(&format!("model {model}"));
            let url = match boot_eval_runner(&bundle, image, port, &name, model, secrets).await {
                Ok(url) => url,
                Err(err) => {
                    step.fail("boot failed");
                    return Err(err);
                }
            };
            let client = RunnerClient::new(&url)?;
            // `boot_eval_runner` pins `fake_model: false`, so every sweep runner is a
            // REAL model whatever the standing dev runner is -- the sweep grades,
            // so this in-CLI path never produces a plumbing-only row.
            let run =
                run_suite_cases(&client, suite, false, trajectory_scorer, sampling, |_| {}).await;
            let _ = docker::remove_container(&name).await;
            let (results, completed) = run?;
            let passed = results
                .rows
                .iter()
                .filter(|(_, o, _, _)| *o == CaseOutcome::Pass)
                .count();
            let total = suite.cases.len();
            // Immediate per-model feedback (#622): a model that never completed a
            // single case is a boot/resolution problem, not a graded loss, so the
            // checklist marks it failed rather than "done" with a misleading score.
            if completed == 0 {
                step.fail(&format!("0/{total} completed -- {model} never answered"));
            } else {
                step.done(&format!("{passed}/{total}"));
            }
            rows.push(SweepRow {
                model: model.clone(),
                passed,
                completed,
                total,
                plumbing: 0,
            });
        }
        // #1087 AC2: the digest rides the machine payload, so an agent confirms
        // `skill message` and `skill eval` ran the same bundle from `--json`
        // rather than from the stderr note above (docs/agents.md bans stderr as
        // agent-facing evidence). It is the RECORDED runner's digest only when
        // this sweep reused the recorded snapshot; a sweep that packed its own
        // reports that ephemeral snapshot's digest instead, never a borrowed one.
        report_sweep(&rows, Some(bundle.digest()))
    }
    .await;
    // A snapshot this sweep packed is owned by this sweep alone, so it is
    // released on the failure path as well as the success one (#1087). It can
    // only ever be a `sweep-*` directory, never the canonical `<digest>/` one a
    // live `skill up` runner may have mounted.
    if let Some(source) = bundle.ephemeral_source() {
        let _ = crate::bundle::remove_snapshot(bundle.dir(), source);
    }
    sweep
}

/// The `--json` sweep payload for one row: pure and independent of `Ui` so it
/// is unit-testable without a process-level stdout capture. Carries the raw
/// `plumbing` count (mirroring the API field) plus a `plumbing_only` boolean
/// derived from `SweepRow::is_plumbing_only` for a scripted consumer to filter
/// fixture rows out of a real model comparison (#700), and the raw
/// `completed` count plus a `never_completed` boolean (#622, #526 AC4) so a
/// model that never produced one completed turn is distinguishable from a
/// real 0%. `pass_rate` is withheld (null) on a never-completed row rather
/// than a fabricated 0.0, since there is no comparison to rate.
pub(super) fn sweep_json_row(row: &SweepRow) -> serde_json::Value {
    serde_json::json!({
        "model": row.model,
        "passed": row.passed,
        "completed": row.completed,
        "total": row.total,
        "pass_rate": if row.never_completed() { None } else { Some(row.pass_rate()) },
        "plumbing": row.plumbing,
        "plumbing_only": row.is_plumbing_only(),
        "never_completed": row.never_completed(),
    })
}

/// The whole `--json` sweep payload: `{"sweep": [<row>, ...]}`. Pure and
/// independent of `Ui` so the schema contract test (#634) can validate it
/// against `cli/schema/sweep.schema.json` without a process-level stdout
/// capture. `report_sweep` emits exactly this via `Ui::emit_json`, so the two
/// never drift.
///
/// `bundle_digest` (#1087 AC2) is the snapshot every runner in the sweep
/// mounted: the recorded runner's digest when the sweep reused it (the value
/// `skill status`/`skill message` report, which is what makes AC2 confirmable
/// from the machine surface), or the ephemeral snapshot's own digest when the
/// sweep packed one because nothing was recorded. Always emitted, `null` at the
/// local/cluster tiers where no locally snapshotted bundle applies.
pub fn sweep_json(rows: &[SweepRow], bundle_digest: Option<&str>) -> serde_json::Value {
    serde_json::json!({
        "sweep": rows.iter().map(sweep_json_row).collect::<Vec<_>>(),
        "bundle_digest": bundle_digest,
    })
}

/// The human table row for one sweep row: `[model, "passed/total", pass rate,
/// plumbing count]`. A plumbing-only row (#700) is marked distinctly rather
/// than blended into the pass-rate list: the model name gets a `(plumbing)`
/// suffix and the rate column reads `n/a` instead of a misleading `0%`, since
/// every case for that model was a fixture, never graded, not a real failure.
/// A never-completed row (#622) takes priority over both: the rate column
/// reads `NEVER COMPLETED` rather than a percentage, since the model produced
/// zero completed turns across the whole suite -- not a real, if unlucky, 0%.
pub(super) fn sweep_table_row(row: &SweepRow) -> Vec<String> {
    let model = if row.is_plumbing_only() {
        format!("{} (plumbing)", row.model)
    } else {
        row.model.clone()
    };
    let rate = if row.never_completed() {
        "NEVER COMPLETED".to_string()
    } else if row.is_plumbing_only() {
        "n/a".to_string()
    } else {
        format!("{:.0}%", row.pass_rate() * 100.0)
    };
    let plumbing = if row.plumbing > 0 {
        row.plumbing.to_string()
    } else {
        "-".to_string()
    };
    vec![
        model,
        format!("{}/{}", row.passed, row.total),
        rate,
        plumbing,
    ]
}

/// Render a model-sweep roll-up: pass-rate per model. Under `--json` the whole
/// comparison is one payload; otherwise a table. A sweep is a comparison, not a
/// gate, so it never exits non-zero on a model that scored below 100% -- a real
/// 0% still reports as `0/N (0%)` and exits `Ok`.
///
/// The one exception (#622, #526 AC4): a row whose model produced ZERO
/// completed turns across the whole suite is not a comparison result at all --
/// it means the model never answered (an unresolvable id, a missing credential,
/// a runner that never came up for it), and reporting it as `0%` is
/// indistinguishable from a real failing model. That row is rendered distinctly
/// (never as a percentage) and turns the whole sweep into an `Err` naming every
/// such model, so the caller's normal `?`-propagation exits non-zero at every
/// tier without skipping any guard the caller still holds (a kept-alive
/// port-forward at local/cluster) -- this function never calls
/// `std::process::exit` itself.
///
/// `bundle_digest` (#1087 AC2) rides the `--json` payload for the same reason
/// it rides `report_eval`'s: the digest was previously a stderr note only, and
/// `docs/agents.md` bans stderr as agent-facing evidence. Callers pass `None`
/// when no locally snapshotted bundle applies.
pub fn report_sweep(rows: &[SweepRow], bundle_digest: Option<&str>) -> Result<()> {
    let ui = crate::ui::ui();
    if ui.json() {
        ui.emit_json(&sweep_json(rows, bundle_digest));
    } else {
        let table: Vec<Vec<String>> = rows.iter().map(sweep_table_row).collect();
        ui.payload_plain(&crate::ui::table(
            &["model", "passed", "pass rate", "plumbing"],
            &table,
            &[1, 2, 3],
        ));
    }

    let never_completed: Vec<&SweepRow> = rows.iter().filter(|r| r.never_completed()).collect();
    if never_completed.is_empty() {
        return Ok(());
    }
    // Name the model AND the likely cause -- the whole point of #622 is that
    // this must not read like a graded 0%, and must not point at the eval
    // consumer the way the local/cluster sweep timeout used to (#526's AC4).
    let detail = never_completed
        .iter()
        .map(|r| format!("{} (0/{} completed)", r.model, r.total))
        .collect::<Vec<_>>()
        .join(", ");
    Err(anyhow::Error::from(
        crate::exit::CliError::failure(format!(
            "{detail}: produced zero completed turns across the suite. This is not a real 0% \
             score -- the model most likely never resolved (a typo'd or unregistered id, a \
             missing/invalid credential, or a runner that never came up for it), so the sweep is \
             failing loudly instead of reporting a comparison that never happened."
        ))
        .with_fix(
            "verify each named model's id and credential (or its BYO endpoint registration), \
             then re-run the sweep",
        ),
    ))
}

/// Render a finished eval run identically for every tier (`skill`, `local`,
/// `cluster`): under `--json` the whole roll-up is one machine payload on
/// stdout; otherwise the per-case table is payload -> stdout and the roll-up
/// verdict is a diagnostic -> stderr. Shared so `local eval`/`cluster eval`
/// print the same summary `skill eval` does (the per-tier parity gate), not a
/// hand-mirrored one.
///
/// Only a genuine `Fail` exits `Failure`. A run that graded nothing because it
/// ran on the fake tier is operationally successful without being a pass, so it
/// exits 0 and says "plumbing OK" in words -- the documented onboarding loop is
/// not red (#612), and it is not fake-green either (#606).
///
/// `bundle_digest` (#1087 AC2) rides the `--json` payload so the bundle a run
/// graded is confirmable from the machine surface. Callers pass `None` when no
/// locally snapshotted bundle applies (the local/cluster tiers grade a deployed
/// version), never a digest they did not observe.
///
/// `guards` are dropped before a red eval's non-unwinding `process::exit`
/// (#1908). `std::process::exit` does not run Drop, so a `kubectl port-forward`
/// child or Slack stub still in the caller's scope would otherwise be orphaned
/// onto PID 1. Callers with no such resource pass `()`.
pub fn report_eval<G>(report: &EvalReport, bundle_digest: Option<&str>, guards: G) -> Result<()> {
    let (_passed, failed, _plumbing_ok) = eval_counts(&report.rows);
    // Emit through the one success point (#474), then apply the exit-code side
    // effect for BOTH paths -- the json path had it inline, the human path after.
    // Only a genuine `Fail` (failed > 0) exits non-zero: a plumbing-only run
    // graded nothing but is operationally successful, so it exits 0 (#606/#612).
    crate::ui::ui().emit(&EvalOutput {
        report,
        bundle_digest,
    });
    if failed > 0 {
        crate::exit::exit_after_drop(crate::exit::ExitClass::Failure, guards);
    }
    Ok(())
}

/// Output of `<tier> eval` (#474). `to_json` delegates to the schema-gated
/// `eval_json` builder (byte-identical, so `cli/schema/eval.schema.json` and
/// `json_contract.rs` stay green); `render` reproduces the per-case table and the
/// roll-up verdict + per-red-case reply notes.
pub(super) struct EvalOutput<'a> {
    report: &'a EvalReport,
    /// The snapshot digest the evaluated runner mounted (#1087), or `None` when
    /// none applies to this run.
    bundle_digest: Option<&'a str>,
}

impl crate::ui::CliOutput for EvalOutput<'_> {
    fn to_json(&self) -> serde_json::Value {
        eval_json_with_details(self.report, self.bundle_digest)
    }

    fn render(&self, ui: &crate::ui::Ui) {
        let results = &self.report.rows;
        let (passed, failed, plumbing_ok) = eval_counts(results);
        let n = self.report.sampling.n;
        let rows: Vec<Vec<String>> = results
            .iter()
            .map(|(name, outcome, seconds, _)| {
                let passes = self.report.sample_passes.get(name).copied().unwrap_or(0);
                let mut cols = vec![
                    name.clone(),
                    outcome_label(*outcome),
                    format!("{seconds:.1}s"),
                ];
                if n > 1 {
                    cols.insert(2, format!("{passes}/{n}"));
                }
                cols
            })
            .collect();
        let headers: &[&str] = if n > 1 {
            &["case", "result", "samples", "time"]
        } else {
            &["case", "result", "time"]
        };
        let right_align: &[usize] = if n > 1 { &[3] } else { &[2] };
        ui.payload_plain(&crate::ui::table(headers, &rows, right_align));
        ui.note(&format!(
            "sampling: {} sample(s), {}",
            n, self.report.sampling.policy
        ));
        if failed == 0 {
            ui.success(&rollup_line(passed, failed, plumbing_ok));
            if plumbing_ok > 0 {
                ui.note(
                    "the fake model returns one canned reply whatever the input, so these cases \
                     were not graded -- they prove the turn completed, nothing more. Re-run with \
                     a real credential to grade them.",
                );
            }
        } else {
            // Surface WHAT each red case actually replied, so a human need not
            // re-run by hand to see why it failed (#548). Empty means the turn
            // never produced gradeable text (no `done`/reply) -- the diagnosis.
            for (name, _, _, output) in results
                .iter()
                .filter(|(_, o, _, _)| *o == CaseOutcome::Fail)
            {
                if let Some(detail) = self.report.details.get(name) {
                    ui.note(&format!("{name}: {detail}"));
                } else {
                    let shown = if output.is_empty() {
                        "<no reply text>".to_string()
                    } else {
                        output.clone()
                    };
                    ui.note(&format!("{name} replied: {shown}"));
                }
            }
            ui.warn(&format!(
                "{}; {failed} failed",
                rollup_line(passed, failed, plumbing_ok)
            ));
        }
    }
}

/// Where the eval cases live: an explicit `--cases` wins; otherwise
/// `evals/cases.json` in the recorded running snapshot wins, then the current
/// directory and, only when no snapshot is recorded, the started runner's
/// recorded bundle directory (so `curie skill eval` works from wherever `curie
/// skill up` was run).
pub fn resolve_cases_path(
    explicit: Option<PathBuf>,
    cwd: &Path,
    recorded_snapshot_dir: Option<&Path>,
    state_plugin_dir: Option<&Path>,
) -> Result<PathBuf> {
    if let Some(path) = explicit {
        return Ok(path);
    }
    if let Some(snapshot_dir) = recorded_snapshot_dir {
        let in_snapshot = snapshot_dir.join("evals/cases.json");
        if in_snapshot.is_file() {
            return Ok(in_snapshot);
        }
        return Err(crate::exit::CliError::usage(format!(
            "no eval cases found in the running snapshot: {}. Run `curie skill up` or pass --cases",
            in_snapshot.display()
        ))
        .with_fix("run `curie skill up` or pass --cases")
        .into());
    }
    let local = cwd.join("evals/cases.json");
    if local.is_file() {
        return Ok(local);
    }
    if let Some(plugin_dir) = state_plugin_dir {
        let in_bundle = plugin_dir.join("evals/cases.json");
        if in_bundle.is_file() {
            return Ok(in_bundle);
        }
    }
    Err(crate::exit::CliError::usage(format!(
        "no eval cases found: looked for {} and the running bundle's evals/cases.json; pass --cases",
        local.display()
    ))
    .with_fix("pass --cases")
    .into())
}

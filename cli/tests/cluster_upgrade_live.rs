//! `curie cluster upgrade` against the LIVE host (#2299 / #2301).
//!
//! `cluster_upgrade.rs` drives `FakeUpgradeHost`, which satisfies the lifecycle
//! contract by construction. These tests instead drive the real binary against
//! recording `helm`/`kubectl` processes, so they pin what `LiveHost` actually
//! issues and actually observes -- the surface the fake cannot reach.
//!
//! Fixture shapes follow the Kubernetes workload/pod references used by
//! `cluster_convergence.rs`. `PATH` is process-global, so every child gets its
//! own `PATH` via `Command::env` and every test its own TempDir root.
//!
//! Non-inertness is the design constraint. `convergence::observe` compares live
//! containers against Helm's INSTALLED manifest, not against `--to`, so a
//! fixture that moves the manifest and the live pods together can never make
//! `images` false however the flag is wired. Every convergence fixture here
//! therefore diverges exactly one facet and asserts that the OTHER named
//! sub-flags stay true: a binding that collapses every flag onto
//! `issues.is_empty()` fails these tests just as loudly as a hardcoded `true`.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::process::{Command, Output};

use serde_json::Value;

/// The exact resource selector `convergence::workloads_command` issues. The
/// recording `kubectl` serves ONLY this string, so a narrower `deploy,sts,ds`
/// read fails outright instead of quietly satisfying a fixture.
const WORKLOADS: &str = "deployments,statefulsets,daemonsets,pods,jobs";

/// The named convergence sub-flags, excluding `exact`.
const FACETS: [&str; 7] = [
    "images",
    "generations",
    "replicas",
    "unavailable_zero",
    "hooks_healthy",
    "queues_drained",
    "manifest_matches",
];

/// One isolated fake cluster: a TempDir holding `helm`, `kubectl`, the argv log
/// and every captured payload.
struct Fixture(tempfile::TempDir);

impl Fixture {
    fn new(retained: Option<&str>) -> Self {
        let temp = tempfile::tempdir().unwrap();
        for name in ["helm", "kubectl"] {
            let path = temp.path().join(name);
            fs::write(&path, include_str!("data/upgrade-driver.py")).unwrap();
            fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
        }
        if let Some(retained) = retained {
            fs::write(temp.path().join("retained.json"), retained).unwrap();
        }
        Self(temp)
    }

    /// Seed the upgrade checkpoint ConfigMap `kubectl get` will return.
    fn seed_checkpoint(&self, record: &Value) {
        fs::write(
            self.0.path().join("checkpoint.json"),
            serde_json::to_string(record).unwrap(),
        )
        .unwrap();
    }

    fn checkpoint(self, record: &Value) -> Self {
        self.seed_checkpoint(record);
        self
    }

    fn run(&self, scenario: &str, to: &str, chart: &str) -> Output {
        Command::new(env!("CARGO_BIN_EXE_curie"))
            .args([
                "--json",
                "cluster",
                "upgrade",
                "--to",
                to,
                "--namespace",
                "ns",
                "--release",
                "rel",
                "--chart",
                chart,
                "--yes",
            ])
            .current_dir(
                std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .parent()
                    .unwrap(),
            )
            .env("PATH", format!("{}:/usr/bin:/bin", self.0.path().display()))
            .env("UPGRADE_DRIVER_ROOT", self.0.path())
            .env("UPGRADE_DRIVER_SCENARIO", scenario)
            .output()
            .unwrap()
    }

    fn local(&self, scenario: &str) -> Output {
        self.run(scenario, "0.9.0", "charts/curie")
    }

    /// Every recorded invocation, in order.
    fn argv(&self) -> Vec<Vec<String>> {
        let log = self.0.path().join("argv.log");
        match fs::read_to_string(log) {
            Ok(text) => text
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect(),
            Err(_) => Vec::new(),
        }
    }

    fn issued(&self, prefix: &[&str]) -> bool {
        self.argv()
            .iter()
            .any(|call| call.len() >= prefix.len() && call[..prefix.len()] == *prefix)
    }

    /// The `helm upgrade` invocations. Empty means nothing was mutated.
    fn helm_upgrades(&self) -> Vec<Vec<String>> {
        self.argv()
            .into_iter()
            .filter(|call| {
                call.first().map(String::as_str) == Some("helm")
                    && call.get(1).map(String::as_str) == Some("upgrade")
            })
            .collect()
    }

    /// The n-th values document (1-based) helm was handed via `-f`.
    fn values(&self, index: usize) -> String {
        fs::read_to_string(self.0.path().join(format!("values-{index}.yaml")))
            .unwrap_or_else(|error| panic!("values-{index}.yaml: {error}"))
    }

    /// Every checkpoint manifest `kubectl apply` received, concatenated.
    fn applied(&self) -> String {
        let mut all = String::new();
        for path in self.applied_paths() {
            all.push_str(&fs::read_to_string(&path).unwrap());
        }
        all
    }

    fn applied_paths(&self) -> Vec<std::path::PathBuf> {
        let mut paths: Vec<_> = fs::read_dir(self.0.path())
            .unwrap()
            .filter_map(|entry| {
                let path = entry.unwrap().path();
                let name = path.file_name()?.to_string_lossy().into_owned();
                name.starts_with("applied-").then_some(path)
            })
            .collect();
        // applied-10 must sort after applied-9, so key on the index.
        paths.sort_by_key(|path| {
            path.file_stem()
                .and_then(|stem| {
                    stem.to_string_lossy()
                        .rsplit('-')
                        .next()?
                        .parse::<u32>()
                        .ok()
                })
                .unwrap_or(0)
        });
        paths
    }

    /// Every persisted `UpgradeRecord`, parsed out of the ConfigMap manifests
    /// `kubectl apply` received, in write order. This is the durable state a
    /// resume would read, so it -- not the process exit code -- is where
    /// "known-good did not advance" and "Commit never ran" are decided.
    fn records(&self) -> Vec<Value> {
        self.applied_paths()
            .iter()
            .filter_map(|path| {
                let manifest: Value = serde_json::from_str(&fs::read_to_string(path).ok()?).ok()?;
                let record = manifest.pointer("/data/record")?.as_str()?;
                serde_json::from_str(record).ok()
            })
            .collect()
    }

    fn last_record(&self) -> Value {
        self.records()
            .pop()
            .unwrap_or_else(|| panic!("no checkpoint record was ever persisted"))
    }
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn visible(output: &Output) -> String {
    format!("{}{}", stdout(output), stderr(output))
}

fn json(output: &Output) -> Value {
    serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "invalid JSON ({error}): {} / {}",
            stdout(output),
            stderr(output)
        )
    })
}

/// Assert that exactly the named convergence facets are false. The "every
/// other flag is still true" half is what stops a binding that simply mirrors
/// `issues.is_empty()` onto all seven from passing a single-facet fixture.
fn only_false(json: &Value, expected_false: &[&str]) {
    let conv = &json["convergence"];
    assert!(
        conv.is_object(),
        "no convergence payload was reported: {json}"
    );
    for facet in FACETS {
        let want = !expected_false.contains(&facet);
        assert_eq!(
            conv[facet],
            Value::Bool(want),
            "convergence.{facet} must be {want}; only {expected_false:?} may be false: {json}"
        );
    }
    assert_eq!(conv["exact"], false, "{json}");
}

/// Assert the observer -- not the old `kubectl get deploy,sts,ds` stub --
/// produced this convergence payload.
fn observed_for_real(fixture: &Fixture) {
    assert!(
        fixture.issued(&["helm", "get", "manifest"]),
        "Helm's installed target manifest must be read: {:?}",
        fixture.argv()
    );
    assert!(
        fixture.issued(&["kubectl", "get", WORKLOADS]),
        "convergence must read the full workload/pod/job set, not `deploy,sts,ds`: {:?}",
        fixture.argv()
    );
}

const V084: &str = include_str!("data/upgrade-config/v0.8.4-user-values.json");
const V085: &str = include_str!("data/upgrade-config/v0.8.5-user-values.json");

/// The inline Slack credential carried by the v0.8.4 fixture. Split so the
/// literal never appears contiguously in source: a repository secret scanner
/// matches `xox[baprs]-` plus ten characters on any added line, and this file
/// asserts on the value precisely because it must never be emitted.
const SLACK_TOKEN: &str = concat!("xoxb", "-test-token-must-not-leak");

/// A checkpoint record whose durable phases are already complete through Apply.
fn applied_checkpoint(drain_completed: bool) -> Value {
    checkpoint_through(
        &[
            "plan",
            "validate",
            "drain",
            "checkpoint",
            "migrate",
            "apply",
        ],
        drain_completed,
    )
}

fn checkpoint_through(completed: &[&str], drain_completed: bool) -> Value {
    serde_json::json!({
        "target_version": "0.9.0",
        "from_version": "0.8.6",
        "known_good_version": "0.8.6",
        "completed": completed,
        "status": "in_progress",
        "plan": ["upgrade to 0.9.0"],
        "drain_completed": drain_completed,
        "convergence": null,
        "canary": null,
        "fail_forward": null,
        "resumed": false,
    })
}

// T1(a) -- #2301 "requires no direct Helm command or merge-flag choice", as
// amended by driver Ruling 2: a local chart path is never pinned with
// `--version` (Helm silently ignores it there).
#[test]
fn local_chart_upgrade_does_not_pass_a_version_flag() {
    let fixture = Fixture::new(None);
    let output = fixture.local("healthy");
    let upgrades = fixture.helm_upgrades();
    assert_eq!(
        upgrades.len(),
        1,
        "one mutation expected: {:?} / {}",
        fixture.argv(),
        stderr(&output)
    );
    assert!(
        !upgrades[0].iter().any(|arg| arg == "--version"),
        "a local chart path must not carry a --version pin Helm ignores: {:?}",
        upgrades[0]
    );
}

// T1(a) -- Ruling 2: the local chart's own metadata is the pin. A chart whose
// `helm show chart` version is not `--to` must refuse BEFORE any mutation.
#[test]
fn local_chart_version_mismatch_refuses_before_mutation() {
    let fixture = Fixture::new(None);
    let output = fixture.local("local-chart-mismatch");
    assert!(
        !output.status.success(),
        "a 0.8.7 chart must not satisfy --to 0.9.0: {}",
        stdout(&output)
    );
    assert!(
        fixture.helm_upgrades().is_empty(),
        "refusal must precede mutation: {:?}",
        fixture.argv()
    );
    let message = visible(&output);
    assert!(
        message.contains("0.8.7") && message.contains("0.9.0"),
        "refusal must name the chart's version and the requested version: {message}"
    );
}

// T1(b) -- #2301: a non-local ref IS resolved by Helm, so `--version <to>` must
// reach the command.
#[test]
fn remote_chart_ref_pins_the_target_version() {
    let fixture = Fixture::new(None);
    let output = fixture.run("healthy", "0.9.0", "oci://example.invalid/curie");
    let upgrades = fixture.helm_upgrades();
    assert_eq!(
        upgrades.len(),
        1,
        "one mutation expected: {:?} / {}",
        fixture.argv(),
        stderr(&output)
    );
    let position = upgrades[0]
        .iter()
        .position(|arg| arg == "--version")
        .unwrap_or_else(|| panic!("--version absent from {:?}", upgrades[0]));
    assert_eq!(
        upgrades[0].get(position + 1).map(String::as_str),
        Some("0.9.0"),
        "--version must carry the requested target: {:?}",
        upgrades[0]
    );
}

// T2 -- #2301 "never reports success for an unconverged release": the observed
// Helm revision, not the requested string, is the authority.
//
// The fixture's `helm show chart` reports 0.9.0, so Ruling 2's pre-mutation
// refusal does NOT fire and Apply is genuinely entered -- asserted below,
// because a test that refuses at Validate proves nothing about the
// post-condition read.
#[test]
fn observed_version_divergence_fails_apply() {
    let fixture = Fixture::new(None);
    let output = fixture.local("stale-version");
    assert_eq!(
        fixture.helm_upgrades().len(),
        1,
        "Validate must pass and Apply must run, or this proves nothing: {:?} / {}",
        fixture.argv(),
        stderr(&output)
    );
    assert!(
        !output.status.success(),
        "a release still on 0.8.6 must not report success: {}",
        stdout(&output)
    );
    let message = visible(&output);
    assert!(
        message.contains("0.8.6") && message.contains("0.9.0"),
        "the failure must name the requested and the observed version: {message}"
    );
    for record in fixture.records() {
        assert_ne!(
            record["known_good_version"], "0.9.0",
            "known-good must not advance to an unobserved version: {record}"
        );
        assert!(
            !record["completed"]
                .as_array()
                .is_some_and(|done| done.iter().any(|phase| phase == "commit")),
            "Commit must not run once Apply's post-condition read failed: {record}"
        );
    }
}

// T3 -- #2301 "a target-version canary". Apply's own post-condition read is
// satisfied (`helm status` reports 0.9.0 immediately after the upgrade) and
// convergence is exact, THEN the release slips back to 0.8.6 before the canary.
// Only a canary that re-reads the live version can catch that; a canary that
// trusts `self.current` -- which Apply set from `--to` -- passes. This is the
// R4 tautology guard, and it deliberately does not share T2's fixture.
#[test]
fn canary_fails_when_the_observed_version_is_not_the_target() {
    let fixture = Fixture::new(None);
    let output = fixture.local("canary-version-drift");
    let json = json(&output);
    assert_eq!(
        fixture.helm_upgrades().len(),
        1,
        "Apply must have succeeded for the canary to be the phase under test: {:?}",
        fixture.argv()
    );
    for facet in FACETS {
        assert_eq!(
            json["convergence"][facet], true,
            "fixture must isolate the canary from convergence: {json}"
        );
    }
    assert_eq!(json["convergence"]["exact"], true, "{json}");
    assert_eq!(json["canary"]["passed"], false, "{json}");
    assert_eq!(json["status"], "failed", "{json}");
    assert_eq!(json["phase"], "canary", "{json}");
    assert_ne!(json["known_good_version"], "0.9.0", "{json}");
    let record = fixture.last_record();
    assert!(
        !record["completed"]
            .as_array()
            .is_some_and(|done| done.iter().any(|phase| phase == "commit")),
        "Commit must not run after a failed canary: {record}"
    );
}

// T4 -- #2301 "exact target image convergence".
//
// The release is deployed, `helm get manifest` renders the 0.9.0 images, and
// the live pods still run 0.8.6 at full ready counts with generations
// observed, hooks healthy and selectors matching. Divergence between the
// INSTALLED manifest and the live containers is the only thing `observe` can
// see -- a fixture whose manifest and pods agree reports `images: true` no
// matter how the flag is wired.
//
// `ProgressDeadlineExceeded` is stamped so the observer stops on its first
// pass instead of retrying to its 300-second deadline. It is a Rollout-facet
// issue and must not be the reason any NAMED sub-flag goes false, which is
// exactly what `only_false` pins.
#[test]
fn stale_images_fail_convergence() {
    let fixture = Fixture::new(None);
    let output = fixture.local("stale-images");
    let json = json(&output);
    observed_for_real(&fixture);
    only_false(&json, &["images"]);
    assert_eq!(json["status"], "failed", "{json}");
    assert_eq!(json["phase"], "converge", "{json}");
}

// T5 -- #2301 "healthy hooks". `helm status -o json` reports the pre-upgrade
// Job hook with `last_run.phase: Failed` AND the live Job carries a `Failed`
// condition, so `hooks_healthy` goes false because the hook state was read --
// not because `observe` errored out on a fixture that never served hooks.
//
// The hook that fails here is `rel-schema-migrate`, NOT the drain gate, and
// `only_false` asserts `queues_drained` stays true. Paired with T6 (where the
// drain gate fails and `hooks_healthy` stays true) this proves the two facets
// are genuinely distinct rather than two names for the same hook walk.
#[test]
fn failed_hook_fails_convergence() {
    let fixture = Fixture::new(None);
    let output = fixture.local("failed-hook");
    let json = json(&output);
    observed_for_real(&fixture);
    only_false(&json, &["hooks_healthy"]);
    assert_eq!(json["status"], "failed", "{json}");
    assert_eq!(json["phase"], "converge", "{json}");
}

// T6 -- #2301 "drained queues" (guards #2010), per driver Ruling 13.
//
// Ruling 7 derived this from phase bookkeeping and was withdrawn: Drain is
// always executed-or-skipped-and-pushed onto `completed` before Converge runs,
// so that predicate was always true and could not fail. The observable fact is
// the #2010 gate itself -- a Helm PRE-UPGRADE hook Job
// (`charts/curie/templates/worker-upgrade-drain.yaml`) that fires during Apply,
// which is why the Drain phase cannot see its verdict and Converge can.
//
// `queues_drained` binds to that one hook by name; `hooks_healthy` covers every
// other hook. Both branches are reachable, so this test is a fixture and its
// own mutation control.
#[test]
fn failed_drain_hook_is_the_only_source_of_queues_drained() {
    // The gate refused: accepted work was still in flight when the roll began.
    let gate = Fixture::new(None);
    let output = gate.local("failed-drain-hook");
    let refused = json(&output);
    observed_for_real(&gate);
    only_false(&refused, &["queues_drained"]);
    assert_eq!(refused["status"], "failed", "{refused}");
    assert_eq!(refused["phase"], "converge", "{refused}");

    // The control: the same fixture with the drain gate succeeding. One hook
    // phase is the whole difference between these two runs.
    let drained = Fixture::new(None);
    let output = drained.local("healthy");
    let control = json(&output);
    observed_for_real(&drained);
    assert_eq!(
        control["convergence"]["queues_drained"], true,
        "a drain gate that exited 0 is a drained queue: {control}"
    );
    assert_eq!(control["convergence"]["exact"], true, "{control}");
    assert_eq!(control["status"], "succeeded", "{control}");
}

// T6 (cont.) -- the two paths that reach Converge with the Drain PHASE
// legitimately skipped must still report drained queues. This is the
// regression the withdrawn ruling was protecting, and it survives the rebind
// because the hook, not the phase record, is now the source: a fresh install
// runs no drain hook and a same-version rerun applies no revision, so neither
// produces a Drain facet.
#[test]
fn skipped_drain_phases_still_report_drained_queues() {
    // Fresh install: no release yet, so Drain is skipped for having nothing in
    // flight (`from.is_none()`).
    let fresh = Fixture::new(None);
    let output = fresh.local("fresh-install");
    let installed = json(&output);
    assert_eq!(
        installed["convergence"]["queues_drained"], true,
        "a first install has nothing in flight and is not undrained: {installed}"
    );
    assert_eq!(installed["convergence"]["exact"], true, "{installed}");
    assert_eq!(installed["status"], "succeeded", "{installed}");
    assert_eq!(
        fresh.helm_upgrades().len(),
        1,
        "a fresh install still installs: {:?}",
        fresh.argv()
    );

    // Same-version rerun: Drain/Checkpoint/Migrate/Apply are all skipped.
    let same = Fixture::new(None);
    let output = same.local("resumed-applied");
    let rerun = json(&output);
    assert_eq!(rerun["unchanged"], true, "{rerun}");
    assert_eq!(
        rerun["convergence"]["queues_drained"], true,
        "a same-version rerun skips Drain but is not undrained: {rerun}"
    );
    assert_eq!(rerun["convergence"]["exact"], true, "{rerun}");
    assert_eq!(rerun["status"], "succeeded", "{rerun}");
    assert!(
        same.helm_upgrades().is_empty(),
        "a same-version rerun must not apply a new revision: {:?}",
        same.argv()
    );

    // A resume past Apply: #2010's exactly-once flag is not a convergence fact
    // either way, so neither setting of it may move the drained verdict.
    for drain_completed in [false, true] {
        let fixture = Fixture::new(None).checkpoint(&applied_checkpoint(drain_completed));
        let output = fixture.local("resumed-applied");
        let resumed = json(&output);
        assert_eq!(
            resumed["convergence"]["queues_drained"], true,
            "drain_completed={drain_completed} must not decide the drained facet: {resumed}"
        );
        assert_eq!(resumed["status"], "succeeded", "{resumed}");
    }
}

// T7 -- #2301 "compare Helm's retained target manifest with live owned objects
// before committing": a live selector that differs from the target manifest.
// The images agree here, so `manifest_matches` is the only facet that may go
// false -- a binding that maps every issue onto every flag fails this.
#[test]
fn manifest_mismatch_fails_convergence_on_the_live_path() {
    let fixture = Fixture::new(None);
    let output = fixture.local("selector-drift");
    let json = json(&output);
    observed_for_real(&fixture);
    only_false(&json, &["manifest_matches"]);
    assert_eq!(json["status"], "failed", "{json}");
    assert_eq!(json["phase"], "converge", "{json}");
}

// T8(a) -- #2301 "persist phase/checkpoint state sufficient to resume": the
// FIRST checkpoint write, before any mutation, must fail the command rather
// than being discarded by `let _ = persist_record(..)`.
#[test]
fn checkpoint_persist_failure_fails_the_command() {
    let fixture = Fixture::new(None);
    let output = fixture.local("persist-fails");
    assert!(
        !output.status.success(),
        "a lost checkpoint must not report success: {}",
        stdout(&output)
    );
    assert!(
        visible(&output).contains("checkpoint"),
        "the persist failure must surface: {}",
        visible(&output)
    );
    assert!(
        fixture.helm_upgrades().is_empty(),
        "a checkpoint lost before Apply must stop short of mutation: {:?}",
        fixture.argv()
    );
}

// T8(b) -- plan edge 10. Only the checkpoint write that FOLLOWS a failed
// Converge fails. Every earlier write succeeds, so the run reaches Converge,
// fails it on stale images, and then loses the failure checkpoint.
//
// The convergence failure is the verdict the operator must act on. A persist
// error raised with `?` from the `PhaseOutcome::Failed` arm would replace it
// with a checkpoint message and drop the `ClusterUpgradeOutput` entirely --
// no phase, no convergence payload, no fail_forward. This asserts the phase
// failure is still the structured verdict and the persist error travels
// beside it.
#[test]
fn persist_failure_after_converge_does_not_mask_the_phase_failure() {
    let fixture = Fixture::new(None);
    let output = fixture.local("converge-then-persist-fails");
    let json = json(&output);
    assert_eq!(
        json["status"], "failed",
        "the phase failure is the verdict: {json}"
    );
    assert_eq!(
        json["phase"], "converge",
        "the persist error must not rename the failing phase: {json}"
    );
    assert_eq!(
        json["convergence"]["images"], false,
        "the convergence payload must survive the lost checkpoint: {json}"
    );
    assert!(
        json["fail_forward"].is_object(),
        "the operator still needs one bounded recovery path: {json}"
    );
    assert!(
        visible(&output).contains("checkpoint"),
        "the persist failure must travel alongside the phase failure: {}",
        visible(&output)
    );
}

// T9 -- #2299 "reject ambiguous conflicts before cluster mutation". The two
// cases differ ONLY in the extraEnv value: 999 contradicts the first-class
// `worker.runnerTotalTimeoutSeconds: 120` and is unresolvable; 120 agrees with
// it and migrates cleanly. Without the second case this test would prove
// nothing beyond "Validate always refuses".
#[test]
fn ambiguous_config_conflict_refuses_before_mutation() {
    let conflicting = Fixture::new(Some(&conflict_values("999").to_string()));
    let output = conflicting.local("healthy");
    assert!(
        !output.status.success(),
        "an ambiguous conflict must refuse: {}",
        stdout(&output)
    );
    assert!(
        conflicting.helm_upgrades().is_empty(),
        "refusal must precede mutation: {:?}",
        conflicting.argv()
    );

    // The control: same shape, no contradiction.
    let agreeing = Fixture::new(Some(&conflict_values("120").to_string()));
    let output = agreeing.local("healthy");
    assert_eq!(
        agreeing.helm_upgrades().len(),
        1,
        "a legacy extraEnv that AGREES with its successor must still upgrade: {:?} / {}",
        agreeing.argv(),
        stderr(&output)
    );
    let values = agreeing.values(1);
    assert!(
        !values.contains("CURIE_RUNNER_TOTAL_TIMEOUT_S"),
        "the redundant extraEnv entry must be dropped, not carried forward: {values}"
    );
}

fn conflict_values(extra_env_value: &str) -> Value {
    serde_json::json!({
        "config": {"schemaVersion": "0.8.4"},
        "worker": {
            "runnerTotalTimeoutSeconds": 120,
            "extraEnv": [{"name": "CURIE_RUNNER_TOTAL_TIMEOUT_S", "value": extra_env_value}]
        }
    })
}

// T10 -- #2299 "migrate legacy extraEnv entries" and "merge new target defaults
// without dropping prior operator overrides": the values file helm is HANDED is
// the evidence, not an in-process return value.
#[test]
fn retained_values_are_migrated_before_helm_sees_them() {
    let fixture = Fixture::new(Some(V084));
    let output = fixture.local("healthy");
    assert!(
        !fixture.helm_upgrades().is_empty(),
        "nothing was applied: {} / {}",
        stdout(&output),
        stderr(&output)
    );
    let values = fixture.values(1);
    assert!(
        values.contains("runnerTotalTimeoutSeconds"),
        "legacy extraEnv must become the first-class key: {values}"
    );
    assert!(
        !values.contains("CURIE_RUNNER_TOTAL_TIMEOUT_S"),
        "the migrated extraEnv entry must not survive: {values}"
    );
    assert!(
        values.contains("PROVIDER_BASE_URL"),
        "an unrelated operator override must survive the merge: {values}"
    );
    assert!(
        values.contains("schemaVersion"),
        "the migrated overlay must persist the configuration schema version: {values}"
    );
}

// T11 -- #2299 "preserve external Secret names byte-for-byte" / "never restore
// an inline value".
#[test]
fn external_secret_references_survive_byte_for_byte() {
    let fixture = Fixture::new(Some(V084));
    fixture.local("healthy");
    let values = fixture.values(1);
    assert!(
        values.contains("acme-slack") && values.contains("botTokenExistingSecretKey"),
        "the external Secret name and key must survive unchanged: {values}"
    );
    assert!(
        !values.contains(SLACK_TOKEN),
        "an inline credential must never be restored beside its Secret ref: {values}"
    );
}

// T12 -- #2299 "plan/apply idempotent", as a RESUME rather than two runs of the
// same input (plan edge 11).
//
// The first run migrates the retained v0.8.5 overlay and hands helm the result.
// The recording helm then RETAINS that result, so the second run's
// `helm get values` returns the first run's own migrated `-f` payload -- and
// the checkpoint seeded between the runs stops Apply from being skipped as a
// same-version no-op. Re-migrating already migrated output must change nothing.
// Two runs against an unchanged fixture would go green even if migration were
// not idempotent at all, because both would migrate the same original input.
#[test]
fn resumed_upgrade_remigrates_its_own_output_without_change() {
    let fixture = Fixture::new(Some(V085));
    let first = fixture.local("healthy");
    let once = fixture.values(1);
    assert!(
        once.contains("runnerTotalTimeoutSeconds"),
        "the first run must actually have migrated something: {once} / {}",
        stderr(&first)
    );

    // Resume with Apply still outstanding, so the second run re-reads and
    // re-migrates the retained (already migrated) overlay.
    fixture.seed_checkpoint(&checkpoint_through(
        &["plan", "validate", "drain", "checkpoint", "migrate"],
        true,
    ));
    let second = fixture.local("healthy");
    assert_eq!(
        fixture.helm_upgrades().len(),
        2,
        "the resume must reach Apply again: {:?} / {}",
        fixture.argv(),
        stderr(&second)
    );
    let twice = fixture.values(2);
    assert_eq!(
        once, twice,
        "re-migrating an already migrated document must change nothing"
    );
}

// T13 -- #2299/#2300 redaction. Ruling 10: `--dry-run` exposing
// `config.schemaVersion` on the plan is DEFERRED out of this PR, so this test
// asserts only credential absence. The overlay's persisted schema version is
// pinned by T10, where the migration output is the subject.
#[test]
fn upgrade_output_contains_no_credential_value() {
    let fixture = Fixture::new(Some(V084));
    let output = fixture.local("healthy");
    let reachable = format!("{}{}", visible(&output), fixture.applied());
    for secret in [SLACK_TOKEN, "sk-ant-test-must-not-leak"] {
        assert!(
            !reachable.contains(secret),
            "credential value reached output or the persisted record: {reachable}"
        );
    }
}

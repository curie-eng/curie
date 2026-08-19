//! `curie scenario`: manifest parsing, refusals, and the `--dry-run` plan.
//!
//! Everything here is offline and Docker-free: each case drives the REAL binary
//! against a committed fixture under `cli/tests/data/scenario/`, with a fake
//! `docker` on the CHILD's PATH whose only job is to record that it was called.
//! A refusal that reached Docker first would leave that log behind, so "refused
//! before doing anything" is proved rather than assumed.
//!
//! These assertions are RED until the implementer lands `curie scenario`: today
//! the binary rejects the subcommand outright, so every case below fails on the
//! exit code and the payload it never emitted. That is a BEHAVIORAL red on
//! purpose -- this file references no `curie::scenario::*` symbol, so it keeps
//! compiling against today's crate.
//!
//! The refusal pairs are the point. `local` (a tier this command knows and
//! refuses, exit 4) beside `kubernetes` (a tier it does not know, exit 2) is
//! what proves the known-vs-unknown distinction is real rather than one blanket
//! rejection wearing two names.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn fixture_dir() -> PathBuf {
    PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/data/scenario"))
}

fn read_fixture(name: &str) -> String {
    let path = fixture_dir().join(name);
    fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("committed fixture {} must exist: {e}", path.display()))
}

fn output_text(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned() + &String::from_utf8_lossy(&output.stderr)
}

/// `curie init` a real bundle into `dir/bundle` through the REAL binary, the
/// discipline `cli/tests/fake_tier_plumbing.rs` pins: the bundle under a
/// scenario is the shipped scaffold, never a hand-written stand-in.
fn scaffold_bundle(dir: &Path) -> PathBuf {
    let out = dir.join("bundle");
    let output = Command::new(bin())
        .args(["init", "weather-bot", "--dir"])
        .arg(&out)
        .stdin(std::process::Stdio::null())
        .output()
        .expect("run curie init");
    assert!(
        output.status.success(),
        "init must scaffold a bundle\n{}",
        output_text(&output)
    );
    out
}

/// Materialize a committed fixture into `dir`, with its `BUNDLE_PATH`
/// placeholder resolved to a freshly scaffolded bundle. The checked file is the
/// contract; the rewrite is path resolution only.
fn manifest_from(dir: &Path, fixture: &str) -> PathBuf {
    let bundle = scaffold_bundle(dir);
    let body = read_fixture(fixture).replace("BUNDLE_PATH", &bundle.display().to_string());
    let path = dir.join("scenario.json");
    fs::write(&path, body).expect("write the resolved manifest");
    path
}

/// A `docker` that records every invocation and then fails. Any case asserting
/// "no Docker was touched" fails loudly if the command shells out to it, and any
/// case that DID reach Docker cannot accidentally succeed against a real daemon.
fn recording_docker(dir: &Path) -> (PathBuf, PathBuf) {
    let bin_dir = dir.join("fakebin");
    fs::create_dir_all(&bin_dir).expect("create the fake PATH directory");
    let log = dir.join("docker.log");
    let script = format!(
        "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{}'\nexit 97\n",
        log.display()
    );
    let path = bin_dir.join("docker");
    fs::write(&path, script).expect("write the fake docker");
    let mut perms = fs::metadata(&path)
        .expect("stat the fake docker")
        .permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&path, perms).expect("chmod the fake docker");
    (bin_dir, log)
}

/// Run `curie scenario <manifest> [extra]` with the recording `docker` first on
/// the CHILD's PATH. Nothing process-global is mutated, so these tests stay
/// parallel-safe.
fn run_scenario(manifest: &Path, fake_bin: &Path, extra: &[&str]) -> Output {
    let inherited = std::env::var("PATH").unwrap_or_default();
    Command::new(bin())
        .arg("scenario")
        .arg(manifest)
        .args(extra)
        .env("PATH", format!("{}:{inherited}", fake_bin.display()))
        .stdin(std::process::Stdio::null())
        .output()
        .expect("run curie scenario")
}

/// The `{error, fix}` object the centralized `--json` error path emits, as one
/// lowercase blob plus its two fields.
fn error_payload(output: &Output) -> (String, String) {
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap_or_else(|e| {
        panic!(
            "a refused run must still emit one JSON object under --json: {e}\n{}",
            output_text(output)
        )
    });
    let error = value["error"].as_str().unwrap_or_default().to_string();
    let fix = value["fix"].as_str().unwrap_or_default().to_string();
    (error, fix)
}

// --- AC5: refuse an unsupported tier claim, never degrade -------------------

/// `local` is a tier this command KNOWS: it parses, and is then refused
/// Unsupported (exit 4, ADR-0041) with a fix naming the upstream platform-API
/// blocker. Exit 4 is only reachable if the string parsed first, so this also
/// pins that `local` is modelled rather than falling through to the unknown
/// branch.
///
/// *Deletion checks*: delete it and the tier could silently degrade to a
/// probe-less run (the failure mode the ticket forbids) with nothing noticing.
/// Delete the refusal and the run reaches Docker, leaving the recording log
/// behind. Rename internals and it survives -- it drives the binary by argv
/// against a committed fixture.
#[test]
fn a_manifest_claiming_the_local_tier_is_refused_with_the_upstream_reason() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (fake_bin, docker_log) = recording_docker(dir.path());
    let manifest = manifest_from(dir.path(), "claims-local-tier.json");

    let output = run_scenario(&manifest, &fake_bin, &["--json"]);

    assert_eq!(
        output.status.code(),
        Some(4),
        "a known-but-unsupported tier is Unsupported (exit 4), never a plain failure\n{}",
        output_text(&output)
    );
    let (error, fix) = error_payload(&output);
    assert!(
        error.to_lowercase().contains("local"),
        "the refusal must name the tier it refused: {error}"
    );
    assert!(
        !fix.trim().is_empty(),
        "an ADR-0041 refusal carries an actionable fix: {error}"
    );
    let blob = format!("{error} {fix}").to_lowercase();
    assert!(
        blob.contains("deployment") || blob.contains("commit_sha") || blob.contains("eval"),
        "the refusal must name the upstream platform-API blocker, not merely say \
         unsupported: {blob}"
    );
    assert!(
        !blob.contains("skip"),
        "the tier is REFUSED, never reported as skipped: {blob}"
    );
    assert!(
        !docker_log.exists(),
        "a manifest refused at parse must never reach Docker: {}",
        fs::read_to_string(&docker_log).unwrap_or_default()
    );
}

/// The paired control for the case above: a tier string this command does NOT
/// know is a Usage error (exit 2), not an Unsupported one. Without the pair,
/// the exit-4 assertion could be satisfied by a blanket refusal that never
/// distinguishes a modelled tier from a typo.
#[test]
fn an_unknown_tier_string_is_a_usage_error_not_an_unsupported_one() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (fake_bin, docker_log) = recording_docker(dir.path());
    let manifest = manifest_from(dir.path(), "unknown-tier.json");

    let output = run_scenario(&manifest, &fake_bin, &["--json"]);

    assert_eq!(
        output.status.code(),
        Some(2),
        "an unmodelled tier string is a Usage error (exit 2), not Unsupported (4)\n{}",
        output_text(&output)
    );
    let (error, _fix) = error_payload(&output);
    assert!(
        error.contains("kubernetes"),
        "the error must name the string it could not resolve: {error}"
    );
    assert!(!docker_log.exists(), "nothing may reach Docker here");
}

// --- AC7: the contract is non-vacuous ---------------------------------------

/// A scenario with no negative probe cannot be falsified, which is the defect
/// class `cli/tests/eval_falsifiability.rs` exists to prevent. Refused at parse.
#[test]
fn a_manifest_with_no_negative_probe_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (fake_bin, docker_log) = recording_docker(dir.path());
    let manifest = manifest_from(dir.path(), "no-negative-probe.json");

    let output = run_scenario(&manifest, &fake_bin, &["--json"]);

    assert_eq!(
        output.status.code(),
        Some(2),
        "a manifest with no control is a Usage error\n{}",
        output_text(&output)
    );
    let (error, _fix) = error_payload(&output);
    assert!(
        error.to_lowercase().contains("negative"),
        "the error must name what is missing: {error}"
    );
    assert!(!docker_log.exists(), "nothing may reach Docker here");
}

/// A criterion whose only evidence is "the pre-fix behavior is absent" has no
/// positive evidence that the required behavior is PRESENT. The refusal must
/// name the uncovered criterion, or an author cannot act on it.
#[test]
fn a_criterion_covered_only_by_a_negative_probe_is_refused_naming_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (fake_bin, docker_log) = recording_docker(dir.path());
    let manifest = manifest_from(dir.path(), "criterion-only-negative.json");

    let output = run_scenario(&manifest, &fake_bin, &["--json"]);

    assert_eq!(
        output.status.code(),
        Some(2),
        "a criterion with no positive probe is a Usage error\n{}",
        output_text(&output)
    );
    let (error, _fix) = error_payload(&output);
    assert!(
        error.contains("AC2"),
        "the refusal must name the uncovered criterion, not just report a count: {error}"
    );
    assert!(
        !error.contains("AC1"),
        "AC1 IS covered by a positive probe and must not be reported as uncovered: {error}"
    );
    assert!(!docker_log.exists(), "nothing may reach Docker here");
}

/// Teardown is unconditional, so `teardown.remove_runner` is constrained to
/// `true`: the author acknowledges in the checked-in artifact that running the
/// scenario destroys the runner, and gets no degradation path.
///
/// *Deletion checks*: delete it and the constraint could quietly relax to a
/// default with no gate noticing. Delete the parse check and it fails. Rename
/// internals and it survives -- it drives the binary against a committed
/// fixture and asserts on the field NAME an author has to fix.
#[test]
fn a_manifest_declining_runner_removal_is_refused_naming_the_field() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (fake_bin, docker_log) = recording_docker(dir.path());
    let manifest = manifest_from(dir.path(), "teardown-declines-removal.json");

    let output = run_scenario(&manifest, &fake_bin, &["--json"]);

    assert_eq!(
        output.status.code(),
        Some(2),
        "declining teardown is a Usage error, not a supported mode\n{}",
        output_text(&output)
    );
    let (error, _fix) = error_payload(&output);
    assert!(
        error.contains("teardown.remove_runner") || error.contains("remove_runner"),
        "the refusal must name the field the author has to change: {error}"
    );
    assert!(!docker_log.exists(), "nothing may reach Docker here");
}

/// A typo'd key inside a NESTED probe object is the dangerous shape: without
/// `deny_unknown_fields` on every nested struct it parses silently and that
/// probe runs against a default under a green report.
#[test]
fn an_unknown_manifest_key_is_a_loud_parse_failure() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (fake_bin, docker_log) = recording_docker(dir.path());
    let manifest = manifest_from(dir.path(), "drifted-unknown-key.json");

    let output = run_scenario(&manifest, &fake_bin, &["--json"]);

    assert_eq!(
        output.status.code(),
        Some(2),
        "an unknown key is a loud Usage error, never ignored\n{}",
        output_text(&output)
    );
    let (error, _fix) = error_payload(&output);
    assert!(
        error.contains("expects"),
        "the parse failure must name the offending key: {error}"
    );
    assert!(!docker_log.exists(), "nothing may reach Docker here");
}

/// An empty tier list would run nothing and have nothing to report, which is
/// exactly the vacuous green #1481 paid for. Refused rather than rolled up as a
/// pass.
#[test]
fn an_empty_tier_list_is_refused_rather_than_reported_as_a_vacuous_pass() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (fake_bin, docker_log) = recording_docker(dir.path());
    let bundle = scaffold_bundle(dir.path());
    let body = read_fixture("skill-two-probes.json")
        .replace("BUNDLE_PATH", &bundle.display().to_string())
        .replace("\"tiers\": [\"skill\"]", "\"tiers\": []");
    assert!(
        body.contains("\"tiers\": []"),
        "the empty-tier rewrite must have applied"
    );
    let manifest = dir.path().join("scenario.json");
    fs::write(&manifest, body).expect("write the empty-tier manifest");

    let output = run_scenario(&manifest, &fake_bin, &["--json"]);

    assert_eq!(
        output.status.code(),
        Some(2),
        "an empty tier list is a Usage error, never an exit-0 pass\n{}",
        output_text(&output)
    );
    let (error, _fix) = error_payload(&output);
    assert!(
        error.to_lowercase().contains("tier"),
        "the error must name the empty list: {error}"
    );
    assert!(!docker_log.exists(), "nothing may reach Docker here");
}

// --- The committed fixtures and the committed schema agree ------------------

/// The manifest schema and the serde structs are enforced separately (a
/// hand-written manifest bypasses the schema), so they can drift apart. This
/// pins them together on the committed fixtures: the well-formed ones validate,
/// and every structurally-expressible violation is REJECTED by the schema.
///
/// `criterion-only-negative.json` is deliberately in the valid set: the
/// coverage rule it violates is a cross-field one JSON Schema cannot express,
/// so the parser is what refuses it (test above). Pinning that division of
/// labor here is what stops the schema being credited with a check it does not
/// perform.
#[test]
fn the_checked_fixtures_validate_against_the_committed_manifest_schema() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/schema/scenario-manifest.schema.json"
    );
    let raw = fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("committed schema {path} must exist: {e}"));
    let schema: serde_json::Value =
        serde_json::from_str(&raw).unwrap_or_else(|e| panic!("schema {path} must be JSON: {e}"));
    let validator = jsonschema::validator_for(&schema).expect("the schema compiles");

    for name in [
        "skill-two-probes.json",
        "claims-local-tier.json",
        "criterion-only-negative.json",
    ] {
        let value: serde_json::Value =
            serde_json::from_str(&read_fixture(name)).expect("fixture is JSON");
        assert!(
            validator.is_valid(&value),
            "{name} is a well-formed manifest and must validate"
        );
    }

    for name in [
        "unknown-tier.json",
        "no-negative-probe.json",
        "teardown-declines-removal.json",
        "drifted-unknown-key.json",
    ] {
        let value: serde_json::Value =
            serde_json::from_str(&read_fixture(name)).expect("fixture is JSON");
        assert!(
            !validator.is_valid(&value),
            "{name} violates a structural rule the schema must enforce, but it validated"
        );
    }
}

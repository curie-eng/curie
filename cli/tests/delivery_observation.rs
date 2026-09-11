//! The reported shape, observed end to end (#2496).
//!
//! Every other test in this issue feeds `assess`/`evaluate` facts that a human
//! typed. That leaves the OBSERVERS -- `doctor::observe_delivery` (the deploy
//! path) and `doctor::gather` (the report path) -- unexercised, and they are
//! where this bug actually lived: the reported install is ClusterIP-only with
//! `api.commitPollIntervalSeconds: 0`, and the trap is `api.service.nodePort`,
//! which the chart populates even there. A reader that consults that key, or a
//! `cluster deploy --repo` that stops calling `observe_delivery` at all, would
//! leave every fact-level test in `delivery_diagnostics.rs` green.
//!
//! So this runs the real readers against stubbed helm/kubectl answering exactly
//! that shape: computed values with `ingress.enabled: false`, a populated
//! `api.service.nodePort`, an interval of `0`, and a live Service whose
//! NodePort jsonpath is EMPTY. Both observers must reach `NotArmed`.

#![cfg(unix)]

use std::ffi::OsString;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;

/// The ClusterIP-only computed document, with the trap key populated. `helm get
/// values --all` renders `api.service.nodePort` on a ClusterIP install, so a
/// reader that took exposure from values would call this exposed.
const CLUSTERIP_COMPUTED_VALUES: &str = r#"{"api":{"commitPollIntervalSeconds":0,"ingress":{"enabled":false},"service":{"type":"ClusterIP","nodePort":30081}}}"#;

fn write_executable(path: &Path, body: &str) {
    fs::write(path, body).expect("write stub executable");
    let mut permissions = fs::metadata(path)
        .expect("read stub metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).expect("make stub executable");
}

/// helm/kubectl/docker for a default `curie`/`curie` install that is serving,
/// exposed nowhere, and polling nothing. Unmatched probes exit 64 loudly rather
/// than answering something plausible; both readers tolerate a failed probe, so
/// a stub that quietly invented an answer would be the worst outcome here.
fn install_clusterip_stubs(tools: &Path) {
    write_executable(tools.join("docker").as_path(), "#!/bin/sh\nexit 0\n");
    write_executable(
        tools.join("kubectl").as_path(),
        r#"#!/bin/sh
case "$*" in
  "config current-context") printf '%s\n' 'doctor-stub-context' ;;
  *component=api*) printf '%s\n' 'curie-api' ;;
  *nodePort*) printf '' ;;
  *"get deployments,statefulsets -n "*) printf '%s\n' '{"items":[{"kind":"Deployment","status":{"readyReplicas":1}}]}' ;;
  *) printf 'unexpected kubectl invocation: %s\n' "$*" >&2; exit 64 ;;
esac
"#,
    );
    write_executable(
        tools.join("helm").as_path(),
        &format!(
            r#"#!/bin/sh
case "$*" in
  version*) printf 'v3.14.0+gstub\n' ;;
  list*) printf '%s\n' '[{{"name":"curie","chart":"curie-0.8.2","status":"deployed"}}]' ;;
  *"--all"*) printf '%s\n' '{computed}' ;;
  "get values"*) printf '%s\n' '{{}}' ;;
  *) printf 'unexpected helm invocation: %s\n' "$*" >&2; exit 64 ;;
esac
"#,
            computed = CLUSTERIP_COMPUTED_VALUES
        ),
    );
}

fn stub_path(tools: &Path) -> OsString {
    let mut entries = vec![tools.to_path_buf()];
    entries.extend(["/bin", "/usr/bin"].iter().map(PathBuf::from));
    std::env::join_paths(entries).expect("join stub PATH")
}

/// One test, not two: it sets the PROCESS `PATH` so the in-process observer
/// finds the stubs, which two tests in one binary would race over.
#[tokio::test]
async fn a_clusterip_install_with_polling_off_observes_as_not_armed() {
    let temp = tempfile::tempdir().expect("tempdir");
    let tools = temp.path().join("tools");
    fs::create_dir_all(&tools).expect("tools dir");
    install_clusterip_stubs(&tools);
    let path = stub_path(&tools);
    std::env::set_var("PATH", &path);

    // -- the deploy path: `cluster deploy --repo` calls exactly this ---------
    let facts = curie::doctor::observe_delivery(&curie::ops::CommonOpts {
        namespace: "curie".to_string(),
        release: "curie".to_string(),
        dry_run: false,
    })
    .await;
    assert_eq!(
        facts.exposure, None,
        "api.service.nodePort is populated on a ClusterIP install; exposure must come from \
         the LIVE Service, which has none here: {facts:?}"
    );
    assert_eq!(
        facts.poll_interval_seconds,
        Some(0.0),
        "an interval of 0 was READ, and must not be confused with one that could not be: \
         {facts:?}"
    );
    assert_eq!(
        facts.discovery_failure, None,
        "both reads succeeded, so this is a verdict doctor is entitled to make: {facts:?}"
    );
    let assessed = curie::delivery::assess(&facts);
    assert_eq!(
        assessed,
        curie::delivery::Delivery::NotArmed,
        "the reported shape is case 1, got {assessed:?}"
    );
    assert!(
        curie::delivery::render(&assessed)
            .text
            .contains("push delivery is NOT armed"),
        "the deploy line an operator sees must say so plainly"
    );

    // -- the report path: the same shape through the real binary ------------
    let output = Command::new(env!("CARGO_BIN_EXE_curie"))
        .current_dir(temp.path())
        .args(["--color=never", "--json", "doctor"])
        .env("PATH", &path)
        .env("LC_ALL", "C")
        .env_remove("CURIE_API_URL")
        .env_remove("CURIE_API_KEY")
        .output()
        .expect("run curie doctor");
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    assert_eq!(
        output.status.code(),
        Some(0),
        "doctor reports, it does not fail: {stdout}\nstderr: {stderr}"
    );
    let json: serde_json::Value = serde_json::from_str(&stdout)
        .unwrap_or_else(|e| panic!("stdout must be doctor JSON, got {stdout:?}: {e}"));
    let webhook = json["checks"]
        .as_array()
        .expect("checks array")
        .iter()
        .find(|c| c["id"] == "webhook")
        .cloned()
        .expect("webhook check");
    assert_eq!(
        webhook["state"], "missing",
        "nothing observed carries a push to this install: {webhook}\nstderr: {stderr}"
    );
    assert!(
        webhook["detail"]
            .as_str()
            .is_some_and(|d| d.contains("no ingress and no NodePort")),
        "the row must name what was observed, not just its verdict: {webhook}"
    );
    assert_eq!(
        json["deploys_verified"],
        serde_json::Value::Bool(false),
        "an unarmed install has no verified deploy path: {json}"
    );
}

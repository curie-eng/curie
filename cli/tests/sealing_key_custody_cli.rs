//! @spec ACTION-EXECUTOR-23: `curie local deploy` refuses a plain
//! `SNAPSHOT_SEALING_KEY` with the API's reason, before any network.
//!
//! The deploy preflight is the CLI's bundle check: it reads the bundle before
//! it is packed and uploaded. Under `--json` the refusal is one ADR-0021
//! `{"error","fix"}` object with exit 2 (usage), never the unreachable API's
//! transient error, because the check runs before the CLI talks to the API.
//! Expectations come from `tests/vectors/sealing-key-custody.json`, the same
//! file the API's half reads.

use std::path::Path;
use std::process::{Command, Output};

use serde_json::Value;

/// Port 1 is closed: reaching the network would be a transient (exit 3) error.
const UNREACHABLE_API_URL: &str = "http://127.0.0.1:1";
const IMAGE: &str =
    "ghcr.io/example/k8s-restorer@sha256:abababababababababababababababababababababababababababababababab";
const SEAL_VALUE: &str = "SEALVALUE-placeholder-7f3c9e1b";

fn vector() -> Value {
    let raw = include_str!("../../tests/vectors/sealing-key-custody.json");
    serde_json::from_str(raw).expect("parse tests/vectors/sealing-key-custody.json")
}

fn reason(name: &str) -> String {
    vector()["reasons"][name]
        .as_str()
        .expect("reasons[name]")
        .to_string()
}

fn bundle(connectors: &str) -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("create plugin directory");
    curie::scaffold::scaffold(dir.path(), "sealer").expect("scaffold plugin");
    std::fs::write(dir.path().join("connectors.yaml"), connectors).expect("write connectors.yaml");
    dir
}

fn local_deploy(plugin_dir: &Path) -> Output {
    let config = tempfile::tempdir().expect("create config directory");
    Command::new(env!("CARGO_BIN_EXE_curie"))
        .arg("--json")
        .args(["local", "deploy", "--api-url", UNREACHABLE_API_URL])
        .args(["--api-key", "placeholder-key"])
        .arg("--plugin-dir")
        .arg(plugin_dir)
        .env_remove("CURIE_API_URL")
        .env_remove("CURIE_API_KEY")
        .env("CURIE_CONFIG_DIR", config.path())
        .output()
        .expect("run curie local deploy")
}

#[test]
fn local_deploy_refuses_a_plain_sealing_key_with_the_apis_reason() {
    for name in ["SNAPSHOT_SEALING_KEY", "SNAPSHOT_SEALING_KEYS_RETAINED"] {
        let dir = bundle(&format!(
            "connectors:\n  k8s:\n    image: {IMAGE}\n    env:\n      {name}: {SEAL_VALUE}\n"
        ));
        let output = local_deploy(dir.path());
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(
            output.status.code(),
            Some(2),
            "{name}: a custody refusal is a usage error (exit 2)\nstdout: {stdout}\nstderr: {stderr}"
        );
        let payload: Value = serde_json::from_slice(&output.stdout).unwrap_or_else(|err| {
            panic!("{name}: --json must emit one JSON object: {err}\nstdout: {stdout}")
        });
        let object = payload.as_object().expect("a JSON object");
        assert!(
            object.contains_key("error") && object.contains_key("fix"),
            "{name}: the ADR-0021 error shape is {{\"error\",\"fix\"}}: {payload}"
        );
        let error = payload["error"].as_str().expect("error is a string");
        assert!(
            error.contains(&reason(name)),
            "{name}: the CLI must refuse with the API's reason verbatim: {error}"
        );
        assert!(
            error.contains("connectors.yaml (connectors.k8s.env)"),
            "{name}: the refusal names where the key is declared: {error}"
        );
        assert!(
            payload["fix"]
                .as_str()
                .is_some_and(|fix| fix.contains("SecretRef")),
            "{name}: the fix names the SecretRef form: {payload}"
        );
        assert!(
            !stdout.contains(SEAL_VALUE) && !stderr.contains(SEAL_VALUE),
            "{name}: the refusal must not echo the key's value"
        );
    }
}

#[test]
fn local_deploy_refuses_a_plugin_json_sealing_key_before_the_unbound_secret_gate() {
    let dir = tempfile::tempdir().expect("create plugin directory");
    curie::scaffold::scaffold(dir.path(), "sealer").expect("scaffold plugin");
    let manifest_path = dir.path().join(".claude-plugin/plugin.json");
    let mut manifest: Value =
        serde_json::from_str(&std::fs::read_to_string(&manifest_path).expect("read plugin.json"))
            .expect("parse plugin.json");
    manifest["secrets"] = serde_json::json!(["SNAPSHOT_SEALING_KEY"]);
    std::fs::write(&manifest_path, manifest.to_string()).expect("write plugin.json");

    let output = local_deploy(dir.path());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(output.status.code(), Some(2), "stdout: {stdout}");
    let payload: Value = serde_json::from_slice(&output.stdout).expect("one JSON object");
    let error = payload["error"].as_str().expect("error is a string");
    assert!(
        error.contains(&reason("SNAPSHOT_SEALING_KEY")),
        "the custody reason, not an unbound-secret refusal, must answer: {error}"
    );
    assert!(error.contains("plugin.json (secrets[0])"), "{error}");
}

#[test]
fn local_deploy_does_not_refuse_the_secret_ref_form_or_a_control_name() {
    let reserved_wording = "is a reserved snapshot sealing key";
    for connectors in [
        format!(
            "connectors:\n  k8s:\n    image: {IMAGE}\n    secrets:\n      - name: SNAPSHOT_SEALING_KEY\n        from_secret: curie-seal\n"
        ),
        format!("connectors:\n  k8s:\n    image: {IMAGE}\n    env:\n      MY_SEAL_KEY: {SEAL_VALUE}\n"),
        format!(
            "connectors:\n  k8s:\n    image: {IMAGE}\n    env:\n      SNAPSHOT_SEALING_KEYRING: {SEAL_VALUE}\n"
        ),
    ] {
        let dir = bundle(&connectors);
        let output = local_deploy(dir.path());
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            !stdout.contains(reserved_wording) && !stderr.contains(reserved_wording),
            "an accepted declaration was refused for custody:\n{connectors}\nstdout: {stdout}\nstderr: {stderr}"
        );
    }
}

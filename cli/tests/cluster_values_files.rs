//! @spec CLUSTER-VALUES-FILES c1-c4
//! Exercise the public parser and offline plans with real Helm input parsing.
use std::fs;
use std::process::{Command, Output};

fn invoke(args: &[&str]) -> Output {
    let home = tempfile::tempdir().unwrap();
    Command::new(env!("CARGO_BIN_EXE_curie"))
        .args(args)
        .env("HOME", home.path())
        .env("KUBECONFIG", home.path().join("absent-kubeconfig"))
        .env_remove("CURIE_CREDENTIALS")
        .env_remove("CURIE_MODEL_CREDENTIALS")
        .env_remove("CURIE_GITHUB_TOKEN")
        .env_remove("CURIE_MODEL")
        .output()
        .unwrap()
}

#[test]
fn ordered_files_are_accepted_and_never_expose_their_secret_contents() {
    // @spec CLUSTER-VALUES-FILES c1, CLUSTER-VALUES-FILES c2, CLUSTER-VALUES-FILES c4
    let dir = tempfile::tempdir().unwrap();
    let first = dir.path().join("first.yaml");
    let second = dir.path().join("second.yaml");
    fs::write(
        &first,
        "ui:\n  service:\n    type: NodePort\napi:\n  githubToken: PLACEHOLDER-file-secret\n",
    )
    .unwrap();
    fs::write(&second, "ui:\n  service:\n    type: ClusterIP\nworker:\n  extraEnv:\n    - name: NUMERIC_STRING\n      value: '8080'\n").unwrap();
    let out = invoke(&[
        "--json",
        "cluster",
        "up",
        "--dry-run",
        "--dev",
        "--fake-model",
        "--chart",
        concat!(env!("CARGO_MANIFEST_DIR"), "/../charts/curie"),
        "-f",
        first.to_str().unwrap(),
        "--values-file",
        second.to_str().unwrap(),
    ]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let output = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        !output.contains("PLACEHOLDER-file-secret"),
        "file secret leaked"
    );
    assert!(
        !output.contains("a GitHub credential passed with --set"),
        "file credential was misclassified as an argv credential"
    );
    assert!(
        !output.contains("ui.service.type=NodePort"),
        "CLI default overrode the later file"
    );
    assert!(serde_json::from_slice::<serde_json::Value>(&out.stdout)
        .unwrap()
        .is_object());
}

#[test]
fn both_commands_refuse_missing_file_before_attempting_the_cluster() {
    // @spec CLUSTER-VALUES-FILES c2, CLUSTER-VALUES-FILES c3
    for verb in ["up", "upgrade"] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("missing-values.yaml");
        let mut args = vec![
            "--json",
            "cluster",
            verb,
            "--dry-run",
            "-f",
            path.to_str().unwrap(),
        ];
        if verb == "upgrade" {
            args.extend(["--to", "0.12.0"]);
        }
        let out = invoke(&args);
        assert!(!out.status.success());
        let error: serde_json::Value =
            serde_json::from_slice(&out.stdout).expect("structured failure");
        assert!(
            error.to_string().contains("values file"),
            "wrong refusal: {error}"
        );
        assert!(
            !error.to_string().contains("unexpected argument"),
            "values option missing"
        );
    }
}

#[test]
fn file_credentials_cannot_bypass_the_provider_contradiction_guard() {
    // @spec CLUSTER-VALUES-FILES c3, CLUSTER-VALUES-FILES c4
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("provider.yaml");
    let secret = "sk-ant-api03-PLACEHOLDER-file-provider";
    fs::write(
        &file,
        format!("agentSandbox:\n  runner:\n    fakeModel: false\n    credentials: {secret}\n"),
    )
    .unwrap();
    let out = invoke(&[
        "--json",
        "cluster",
        "up",
        "--dry-run",
        "--dev",
        "--chart",
        concat!(env!("CARGO_MANIFEST_DIR"), "/../charts/curie"),
        "--allow-egress-host",
        "openrouter",
        "-f",
        file.to_str().unwrap(),
    ]);
    assert!(!out.status.success());
    let error: serde_json::Value = serde_json::from_slice(&out.stdout).expect("structured refusal");
    assert!(
        error.to_string().contains("anthropic"),
        "final file credential was not admitted: {error}"
    );
    assert!(
        !error.to_string().contains(secret),
        "file credential leaked"
    );
}

#[test]
fn malformed_files_are_refused_without_echoing_yaml_contents() {
    // @spec CLUSTER-VALUES-FILES c2, CLUSTER-VALUES-FILES c4
    for document in [
        "- PLACEHOLDER-secret-not-a-map\n",
        "token: [PLACEHOLDER-secret-invalid\n",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("bad.yaml");
        fs::write(&file, document).unwrap();
        let out = invoke(&[
            "--json",
            "cluster",
            "up",
            "--dry-run",
            "--dev",
            "-f",
            file.to_str().unwrap(),
        ]);
        assert!(!out.status.success());
        let error: serde_json::Value =
            serde_json::from_slice(&out.stdout).expect("structured refusal");
        assert!(
            !error.to_string().contains("PLACEHOLDER-secret"),
            "contents leaked: {error}"
        );
        assert!(
            !error.to_string().contains("unexpected argument"),
            "values option missing"
        );
    }
}

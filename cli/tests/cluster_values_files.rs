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
        assert_eq!(
            out.status.code(),
            Some(2),
            "a missing file is a usage error"
        );
        let error: serde_json::Value =
            serde_json::from_slice(&out.stdout).expect("structured failure");
        assert!(
            error.to_string().contains("values file"),
            "wrong refusal: {error}"
        );
        assert!(
            error["error"]
                .as_str()
                .is_some_and(|message| message.contains(path.to_str().unwrap())),
            "refusal must name the unreadable file: {error}"
        );
        assert!(error["fix"].is_string(), "refusal needs a fix: {error}");
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
        assert_eq!(
            out.status.code(),
            Some(2),
            "malformed input is a usage error"
        );
        let error: serde_json::Value =
            serde_json::from_slice(&out.stdout).expect("structured refusal");
        assert!(
            !error.to_string().contains("PLACEHOLDER-secret"),
            "contents leaked: {error}"
        );
        assert!(error["fix"].is_string(), "refusal needs a fix: {error}");
        assert!(
            !error.to_string().contains("unexpected argument"),
            "values option missing"
        );
    }
}

#[test]
fn explicit_set_repairs_file_configuration_before_final_admission() {
    // @spec CLUSTER-VALUES-FILES c1, CLUSTER-VALUES-FILES c3
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("repair.yaml");
    fs::write(&file, "worker:\n  deliveryBudgetSeconds: 700\n  extraEnv:\n    - name: CURIE_DELIVERY_BUDGET_S\n      value: '800'\n").unwrap();
    let output = invoke(&[
        "--json",
        "cluster",
        "up",
        "--dry-run",
        "--dev",
        "--fake-model",
        "--chart",
        concat!(env!("CARGO_MANIFEST_DIR"), "/../charts/curie"),
        "-f",
        file.to_str().unwrap(),
        "--set",
        "worker.deliveryBudgetSeconds=800",
    ]);
    assert!(
        output.status.success(),
        "final explicit value was not admitted: {}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[cfg(unix)]
#[test]
fn captured_copies_of_credential_files_are_private_while_helm_parses_them() {
    // @spec CLUSTER-VALUES-FILES c4
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("credentials.yaml");
    fs::write(&file, "api:\n  githubToken: PLACEHOLDER-private-copy\n").unwrap();
    let bin = dir.path().join("bin");
    fs::create_dir(&bin).unwrap();
    let modes = dir.path().join("modes");
    let helm = bin.join("helm");
    fs::write(
        &helm,
        format!(
            "#!/bin/sh\nwhile [ $# -gt 0 ]; do\n  if [ \"$1\" = -f ]; then\n    \
             stat -c 'dir=%a' \"$(dirname \"$(dirname \"$2\")\")\" >> '{0}'\n    \
             stat -c 'file=%a' \"$2\" >> '{0}'\n    shift\n  fi\n  shift\ndone\nexit 1\n",
            modes.display()
        ),
    )
    .unwrap();
    fs::set_permissions(&helm, fs::Permissions::from_mode(0o755)).unwrap();
    let path = format!("{}:{}", bin.display(), std::env::var("PATH").unwrap());
    for verb in ["up", "upgrade"] {
        let mut args = vec![
            "--json",
            "cluster",
            verb,
            "--dry-run",
            "-f",
            file.to_str().unwrap(),
        ];
        if verb == "upgrade" {
            args.extend(["--to", "0.12.0"]);
        }
        let home = tempfile::tempdir().unwrap();
        let out = Command::new(env!("CARGO_BIN_EXE_curie"))
            .args(&args)
            .env("HOME", home.path())
            .env("PATH", &path)
            .env("KUBECONFIG", home.path().join("absent-kubeconfig"))
            .output()
            .unwrap();
        assert!(!out.status.success(), "stub Helm refuses every parse");
    }
    let recorded = fs::read_to_string(&modes).expect("Helm saw the captured copy");
    let lines: Vec<&str> = recorded.lines().collect();
    assert_eq!(lines.len(), 4, "one copy per command: {recorded}");
    for line in lines {
        assert!(
            line == "dir=700" || line == "file=600",
            "captured credential copy is readable by other users: {recorded}"
        );
    }
}

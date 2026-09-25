//! Released-binary starter, input schema, and context targeting for #2859.

use std::fs;
use std::process::Command;

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn output_text(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned() + &String::from_utf8_lossy(&output.stderr)
}

#[test]
fn apply_init_writes_a_parseable_starter_and_refuses_overwrite() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("curie.yaml");
    let output = Command::new(bin())
        .args(["apply", "--init", "--file"])
        .arg(&path)
        .arg("--json")
        .output()
        .expect("run apply --init");
    assert!(
        output.status.success(),
        "apply --init failed\n{}",
        output_text(&output)
    );
    let json: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("apply --init --json is JSON");
    assert_eq!(json["wrote"], serde_json::json!(true));
    let cfg = curie::installation::Installation::load(&path).expect("starter must parse");
    assert_eq!(cfg.install.namespace, "acme-bot");

    let again = Command::new(bin())
        .args(["apply", "--init", "--file"])
        .arg(&path)
        .arg("--json")
        .output()
        .expect("run apply --init overwrite");
    assert!(
        !again.status.success(),
        "overwrite must fail\n{}",
        output_text(&again)
    );
    let err: serde_json::Value =
        serde_json::from_slice(&again.stdout).expect("overwrite error is JSON");
    assert!(
        err["error"]
            .as_str()
            .is_some_and(|message| message.contains("refusing to overwrite")),
        "overwrite error: {err}"
    );
    let _ = fs::read_to_string(&path).expect("original starter remains");
}

#[test]
fn schema_index_prints_the_installation_input_schema() {
    let output = Command::new(bin())
        .args(["schema-index", "curie-yaml"])
        .output()
        .expect("run schema-index curie-yaml");
    assert!(
        output.status.success(),
        "schema-index curie-yaml failed\n{}",
        output_text(&output)
    );
    let schema: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("input schema is JSON");
    assert_eq!(
        schema["$id"],
        "https://schemas.curietech.ai/cli/curie-yaml/v1.json"
    );
    assert!(schema["properties"]["install"]["properties"]
        .get("context")
        .is_some());
}

#[test]
fn install_context_in_the_file_is_pinned_and_the_flag_wins() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("curie.yaml");
    fs::write(
        &path,
        "version: 1\ninstall:\n  namespace: acme\n  release: acme\n  context: missing-from-file\n",
    )
    .expect("write file");
    let kube = dir.path().join("kubeconfig");
    fs::write(
        &kube,
        "apiVersion: v1\nkind: Config\ncurrent-context: only\n\
         contexts:\n- name: only\n  context:\n    cluster: only-cluster\n\
         clusters:\n- name: only-cluster\n  cluster:\n    server: https://127.0.0.1\n\
         users:\n- name: only\n  user: {}\n",
    )
    .expect("write kubeconfig");

    let from_file = Command::new(bin())
        .args(["diff", "--file"])
        .arg(&path)
        .arg("--json")
        .env("KUBECONFIG", &kube)
        .env("HOME", dir.path())
        .output()
        .expect("run diff with file context");
    assert!(
        !from_file.status.success(),
        "file context must be pinned: {}",
        output_text(&from_file)
    );
    assert!(
        output_text(&from_file).contains("missing-from-file"),
        "must name the file context: {}",
        output_text(&from_file)
    );

    let flag_wins = Command::new(bin())
        .args(["diff", "--file"])
        .arg(&path)
        .args(["--context", "also-missing", "--json"])
        .env("KUBECONFIG", &kube)
        .env("HOME", dir.path())
        .output()
        .expect("run diff with flag override");
    let combined = output_text(&flag_wins);
    assert!(
        !flag_wins.status.success(),
        "flag context must be pinned: {combined}"
    );
    assert!(
        combined.contains("also-missing"),
        "flag must win over the file: {combined}"
    );
    assert!(
        !combined.contains("missing-from-file"),
        "file context must not be used when the flag is set: {combined}"
    );
}

#[test]
fn doctor_pins_install_context_from_the_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("curie.yaml");
    fs::write(
        &path,
        "version: 1\ninstall:\n  namespace: acme\n  release: acme\n  context: missing-doctor\n",
    )
    .expect("write file");
    let kube = dir.path().join("kubeconfig");
    fs::write(
        &kube,
        "apiVersion: v1\nkind: Config\ncurrent-context: only\n\
         contexts:\n- name: only\n  context:\n    cluster: only-cluster\n\
         clusters:\n- name: only-cluster\n  cluster:\n    server: https://127.0.0.1\n\
         users:\n- name: only\n  user: {}\n",
    )
    .expect("write kubeconfig");
    let output = Command::new(bin())
        .args(["doctor", "--json"])
        .current_dir(dir.path())
        .env("KUBECONFIG", &kube)
        .env("HOME", dir.path())
        .output()
        .expect("run doctor");
    assert!(
        !output.status.success(),
        "doctor must refuse a missing file context: {}",
        output_text(&output)
    );
    assert!(
        output_text(&output).contains("missing-doctor"),
        "doctor must name the file context: {}",
        output_text(&output)
    );
}

#[test]
fn apply_and_diff_refuse_an_unknown_context() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("curie.yaml");
    fs::write(
        &path,
        "version: 1\ninstall:\n  namespace: acme\n  release: acme\n",
    )
    .expect("write file");
    let kube = dir.path().join("kubeconfig");
    fs::write(
        &kube,
        "apiVersion: v1\nkind: Config\ncurrent-context: only\n\
         contexts:\n- name: only\n  context:\n    cluster: only-cluster\n\
         clusters:\n- name: only-cluster\n  cluster:\n    server: https://127.0.0.1\n\
         users:\n- name: only\n  user: {}\n",
    )
    .expect("write kubeconfig");

    for verb in ["apply", "diff"] {
        let output = Command::new(bin())
            .args([verb, "--file"])
            .arg(&path)
            .args(["--context", "missing-ctx", "--json"])
            .env("KUBECONFIG", &kube)
            .env("HOME", dir.path())
            .output()
            .expect("run with unknown context");
        assert!(
            !output.status.success(),
            "{verb} --context missing-ctx must fail\n{}",
            output_text(&output)
        );
        let combined = output_text(&output);
        assert!(
            combined.contains("missing-ctx"),
            "{verb} must name the missing context: {combined}"
        );
    }
}

#[test]
fn guide_covers_apply() {
    let output = Command::new(bin())
        .arg("guide")
        .output()
        .expect("run guide");
    assert!(
        output.status.success(),
        "guide failed\n{}",
        output_text(&output)
    );
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(
        text.contains("curie apply"),
        "guide must cover apply\n{text}"
    );
    assert!(
        text.contains("curie.yaml"),
        "guide must name curie.yaml\n{text}"
    );
}

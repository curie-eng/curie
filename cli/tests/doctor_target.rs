//! Integration (#1358 D6b): `curie doctor` resolves its target from the
//! `curie.yaml` in the working directory, announces the inference, and never
//! fails because of that file.
//!
//! Both properties here are structurally invisible to the pure
//! `resolve_target` table in `cli/src/doctor.rs`: hand-passing `None` to the
//! resolver bypasses the dispatch arm's fail-soft read entirely, and
//! `DoctorOutput::to_json()` cannot observe which stream a line was written to.
//! So these run the real binary.
//!
//! The targeting cases empty `PATH` so they need no cluster. The model remedy
//! case uses temporary command stubs to expose a floating release model.

use std::fs;
use std::path::Path;
use std::process::Command;

#[cfg(unix)]
fn write_executable(path: &Path, body: &str) {
    use std::os::unix::fs::PermissionsExt;

    fs::write(path, body).expect("write tool stub");
    let mut permissions = fs::metadata(path)
        .expect("read tool stub metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).expect("make tool stub executable");
}

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

/// Run `curie <args>` with the working directory in `dir` and no tools on
/// `PATH`, so the run needs no cluster and no network.
fn run_doctor(dir: &Path, empty_path: &Path, args: &[&str]) -> (Option<i32>, String, String) {
    let output = Command::new(bin())
        .current_dir(dir)
        .args(args)
        .env("PATH", empty_path)
        .env("LC_ALL", "C")
        .env_remove("CURIE_API_URL")
        .env_remove("CURIE_API_KEY")
        .output()
        .expect("run curie doctor");
    (
        output.status.code(),
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

fn empty_path_dir(root: &Path) -> std::path::PathBuf {
    let dir = root.join("no-tools");
    fs::create_dir_all(&dir).expect("create empty PATH directory");
    dir
}

/// doctor is read-only and its whole job is to report. A `curie.yaml` it cannot
/// parse must narrow what it knows, never stop it answering -- which is exactly
/// what a `?` or an `expect` on the load would do. The pure resolver cannot
/// catch that: it is handed `None` either way.
#[test]
fn a_malformed_curie_yaml_does_not_fail_doctor() {
    let temp = tempfile::tempdir().expect("create temporary directory");
    let tools = empty_path_dir(temp.path());

    let cases = [
        // Not YAML this schema can make sense of at all.
        ("garbage", ": : not: [valid\n  yaml at all\n"),
        // Parses, but declares a schema version this binary refuses.
        (
            "unsupported version",
            "version: 99\ninstall:\n  namespace: acme\n  release: acme\n",
        ),
    ];

    for (what, body) in cases {
        fs::write(temp.path().join("curie.yaml"), body).expect("write curie.yaml");
        let (code, stdout, stderr) = run_doctor(temp.path(), &tools, &["--color=never", "doctor"]);

        assert_eq!(
            code,
            Some(0),
            "a {what} curie.yaml must not fail a read-only report\n\
             stdout: {stdout}\nstderr: {stderr}"
        );
        for line in ["Model credential", "Bundle in this directory"] {
            assert!(
                stdout.contains(line),
                "a {what} curie.yaml must still produce a full report; \
                 missing {line:?}\nstdout: {stdout}\nstderr: {stderr}"
            );
        }
        assert!(
            stderr.contains("curie.yaml"),
            "the operator must be told their file was not read, or doctor looks \
             like it ignored it\nstderr: {stderr}"
        );
    }
}

/// `--json` owns stdout: a machine consumer parses it whole. An announcement
/// printed with `println!` or `payload_plain` would corrupt that payload, and no
/// `to_json()` assertion could ever see it -- only the real streams can.
#[test]
fn an_inferred_target_keeps_json_stdout_clean() {
    let temp = tempfile::tempdir().expect("create temporary directory");
    let tools = empty_path_dir(temp.path());
    fs::write(
        temp.path().join("curie.yaml"),
        "version: 1\ninstall:\n  namespace: acme\n  release: acme\n",
    )
    .expect("write curie.yaml");

    let (code, stdout, stderr) =
        run_doctor(temp.path(), &tools, &["--color=never", "--json", "doctor"]);
    assert_eq!(code, Some(0), "stdout: {stdout}\nstderr: {stderr}");

    let values: Vec<serde_json::Value> = serde_json::Deserializer::from_str(&stdout)
        .into_iter::<serde_json::Value>()
        .collect::<Result<_, _>>()
        .unwrap_or_else(|e| panic!("stdout must be JSON, got {stdout:?}: {e}"));
    assert_eq!(
        values.len(),
        1,
        "stdout must carry exactly one JSON value: {stdout:?}"
    );
    assert!(
        values[0].is_object(),
        "the one value must be the report object: {stdout:?}"
    );
    for noise in ["curie.yaml", "inferred"] {
        assert!(
            !stdout.contains(noise),
            "{noise:?} reached the machine payload: {stdout:?}"
        );
    }

    assert!(
        stderr.contains("curie.yaml"),
        "the inference must be announced, and named as coming from the file \
         (INFER, DON'T ASK): {stderr}"
    );
    assert!(
        stderr.contains("acme"),
        "the announcement must name the target it resolved: {stderr}"
    );
}

#[cfg(unix)]
#[test]
fn explicit_context_keeps_the_file_model_remedy_on_curie_apply() {
    let temp = tempfile::tempdir().expect("create temporary directory");
    let tools = temp.path().join("tools");
    fs::create_dir_all(&tools).expect("create tool directory");
    write_executable(&tools.join("docker"), "#!/bin/sh\nexit 0\n");
    write_executable(
        &tools.join("kubectl"),
        r#"#!/bin/sh
case "$*" in
  "config current-context") printf '%s\n' 'explicit-context' ;;
  *"get deployments,statefulsets"*) printf '%s\n' '{"items":[{"kind":"Deployment","status":{"readyReplicas":1}}]}' ;;
  *) exit 1 ;;
esac
"#,
    );
    write_executable(
        &tools.join("helm"),
        r#"#!/bin/sh
case "$*" in
  version*) printf '%s\n' 'v3.14.0+gstub' ;;
  list*) printf '%s\n' '[{"name":"acme-bot","chart":"curie-0.10.2","status":"deployed"}]' ;;
  *"--all"*) printf '%s\n' '{"agentSandbox":{"runner":{"model":"claude-sonnet-5","fakeModel":false}}}' ;;
  *"get values"*) printf '%s\n' '{}' ;;
  *) exit 1 ;;
esac
"#,
    );

    fs::write(
        temp.path().join("curie.yaml"),
        "version: 1\ninstall:\n  namespace: acme\n  release: acme-bot\n  context: file-context\n",
    )
    .expect("write curie.yaml");
    let kubeconfig = temp.path().join("kubeconfig");
    fs::write(
        &kubeconfig,
        "apiVersion: v1\nkind: Config\ncurrent-context: file-context\ncontexts:\n- name: file-context\n  context:\n    cluster: file-cluster\n- name: explicit-context\n  context:\n    cluster: explicit-cluster\n",
    )
    .expect("write kubeconfig");

    let output = Command::new(bin())
        .current_dir(temp.path())
        .args([
            "--color=never",
            "--json",
            "doctor",
            "--namespace",
            "acme",
            "--release",
            "acme-bot",
            "--context",
            "explicit-context",
        ])
        .env("PATH", &tools)
        .env("HOME", temp.path())
        .env("KUBECONFIG", &kubeconfig)
        .env("CURIE_CONFIG_DIR", temp.path().join("config"))
        .env("LC_ALL", "C")
        .env_remove("CURIE_API_URL")
        .env_remove("CURIE_API_KEY")
        .env_remove("CURIE_MODEL")
        .env_remove("CURIE_CREDENTIALS")
        .env_remove("CLAUDE_CODE_OAUTH_TOKEN")
        .env_remove("ANTHROPIC_API_KEY")
        .output()
        .expect("run curie doctor");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let report: serde_json::Value = serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|error| panic!("doctor output must be JSON: {error}; stderr: {stderr}"));
    let fix = report["checks"]
        .as_array()
        .expect("checks array")
        .iter()
        .find(|check| check["id"] == "model-pin")
        .and_then(|check| check["fix"].as_str())
        .expect("floating release model must carry a fix");

    assert_eq!(
        output.status.code(),
        Some(0),
        "stdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        fix.contains("curie apply --context")
            && fix.contains("explicit-context")
            && fix.contains("agentSandbox.runner.model")
            && fix.contains("set:"),
        "the fix must preserve the selected context and name the file key: {fix}"
    );
    assert!(
        !fix.contains("cluster up --set"),
        "the fix must not return to a cluster up override: {fix}"
    );
}

//! `curie dev chart-check` covers what helm-ci covers (#1481).
//!
//! helm-ci runs every script in `charts/curie/ci/` on any `charts/curie/**`
//! change, and #1466 made that job release-blocking. The verb used to shell one
//! script, so a contributor could run the documented local chart gate, see
//! green, push, and be refused by CI on any of the other sixteen.
//!
//! The gap was structural, not a missing entry in a list: the verb named its one
//! script literally, so every script added since was invisible to it. These
//! tests pin the two properties that close it -- discovery from the directory,
//! and a run that reports every failure rather than the first -- plus the
//! divergence gate that keeps the verb and helm-ci describing the same set.
//!
//! Nothing here runs the committed scripts themselves. helm-ci's Chart job runs
//! every one of them, its pull request filter covers every tree they read, and
//! the divergence gate below keeps that set equal to the directory. Running
//! them again from the Rust job duplicated about nine minutes of CI.

use std::path::{Path, PathBuf};
use std::process::Command;

use curie::commands::{discover_chart_check_scripts, run_chart_check_scripts, CHART_CI_DIR};

fn repo_root() -> PathBuf {
    PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/.."))
}

/// Write `body` to `dir/name` and mark it executable, the shape the discovery
/// walk selects on.
fn write_script(dir: &Path, name: &str, body: &str) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, body).expect("write scratch script");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
            .expect("chmod scratch script");
    }
    path
}

/// Under `--json`, child process chatter belongs on stderr and stdout is one
/// machine readable result object. The script intentionally writes each stream
/// without a newline so inherited stdout cannot be mistaken for a JSON line.
#[test]
fn chart_check_json_keeps_child_chatter_off_stdout() {
    let scratch = tempfile::tempdir().expect("scratch repo");
    let ci_dir = scratch.path().join(CHART_CI_DIR);
    std::fs::create_dir_all(&ci_dir).expect("create chart assertion directory");
    std::fs::create_dir_all(scratch.path().join("runner")).expect("create repo sentinel dir");
    std::fs::write(scratch.path().join("runner/Dockerfile"), "").expect("write repo sentinel");
    write_script(
        &ci_dir,
        "chatty-assertion.sh",
        "#!/bin/bash\nprintf 'child stdout without newline'\nprintf 'child stderr without newline' >&2\n",
    );

    let output = Command::new(env!("CARGO_BIN_EXE_curie"))
        .args(["--json", "dev", "chart-check"])
        .current_dir(scratch.path())
        .output()
        .expect("run chart-check against scratch repository");
    let stdout = String::from_utf8(output.stdout).expect("stdout is UTF-8");
    let stderr = String::from_utf8(output.stderr).expect("stderr is UTF-8");

    assert!(
        output.status.success(),
        "the passing assertion script must leave chart-check successful; stderr: {stderr}"
    );
    let payload: serde_json::Value = serde_json::from_str(&stdout).unwrap_or_else(|error| {
        panic!(
            "--json stdout must be exactly one parseable result object, not child chatter: \
             {error}; stdout: {stdout:?}"
        )
    });
    assert!(
        payload.is_object(),
        "--json result must be an object: {payload}"
    );
    assert_eq!(payload["passed"], 1, "{payload}");
    assert_eq!(payload["total"], 1, "{payload}");
    assert_eq!(
        payload["scripts"],
        serde_json::json!([{ "name": "chatty-assertion.sh", "passed": true }]),
        "{payload}"
    );
    assert!(
        !stdout.contains("child stdout without newline"),
        "child stdout must not contaminate the result object: {stdout:?}"
    );
    assert!(
        stderr.contains("child stdout without newline"),
        "captured child stdout must be replayed to stderr: {stderr:?}"
    );
    assert!(
        stderr.contains("child stderr without newline"),
        "captured child stderr must remain on stderr: {stderr:?}"
    );
}

#[cfg(unix)]
fn shell_scripts_without_execute_permission(dir: &Path) -> Vec<PathBuf> {
    use std::os::unix::fs::PermissionsExt;

    let entries = std::fs::read_dir(dir)
        .unwrap_or_else(|error| panic!("read shell script directory {}: {error}", dir.display()));
    let mut found = Vec::new();
    for entry in entries {
        let path = entry.expect("read shell script directory entry").path();
        if path.is_file() && path.extension().is_some_and(|extension| extension == "sh") {
            let mode = std::fs::metadata(&path)
                .expect("read shell script metadata")
                .permissions()
                .mode();
            if mode & 0o111 == 0 {
                found.push(path.strip_prefix(dir).unwrap_or(&path).to_path_buf());
            }
        }
    }

    found.sort();
    found
}

/// Every direct shell assertion entry must be executable even though production
/// discovery intentionally skips files without an execute bit. This inventory
/// is independent of discovery, so a lost mode cannot become invisible to both
/// the local verb and its parity check.
#[cfg(unix)]
#[test]
fn every_chart_ci_shell_script_is_executable() {
    let ci_dir = repo_root().join(CHART_CI_DIR);
    let missing = shell_scripts_without_execute_permission(&ci_dir);
    assert!(
        missing.is_empty(),
        "every direct *.sh file in {CHART_CI_DIR} must be executable; missing execute permission: {missing:?}"
    );

    let scratch = tempfile::tempdir().expect("scratch dir");
    let mutant = write_script(
        scratch.path(),
        "mode-regression.sh",
        "#!/bin/bash\nexit 0\n",
    );
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&mutant, std::fs::Permissions::from_mode(0o644))
        .expect("remove execute permission from scratch script");

    assert_eq!(
        shell_scripts_without_execute_permission(scratch.path()),
        vec![PathBuf::from("mode-regression.sh")],
        "the independent inventory must reject a direct shell script with mode 0644"
    );
}

/// AC3: a script added to `charts/curie/ci/` is picked up with no edit to `cli/`.
///
/// The scratch copy stands in for the real directory so the assertion is about
/// discovery rather than about any particular chart assertion. A deliberately
/// failing script is the load-bearing half: it proves the new file was actually
/// executed, not merely listed.
#[tokio::test]
async fn a_newly_added_script_is_discovered_and_run_with_no_cli_edit() {
    let scratch = tempfile::tempdir().expect("scratch dir");
    write_script(
        scratch.path(),
        "aaa-existing-assertions.sh",
        "#!/bin/bash\nexit 0\n",
    );

    let before = discover_chart_check_scripts(scratch.path()).expect("discover");
    assert_eq!(
        before.len(),
        1,
        "expected the one seeded script, got {before:?}"
    );

    // The edit a contributor makes: drop a new script in the directory, touch
    // nothing in cli/.
    write_script(
        scratch.path(),
        "zzz-newly-added-assertions.sh",
        "#!/bin/bash\nexit 1\n",
    );

    let after = discover_chart_check_scripts(scratch.path()).expect("discover");
    assert_eq!(
        after
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
            .collect::<Vec<_>>(),
        vec![
            "aaa-existing-assertions.sh".to_string(),
            "zzz-newly-added-assertions.sh".to_string(),
        ],
        "the new script must be discovered, in sorted order"
    );

    let outcomes = run_chart_check_scripts(scratch.path(), &after)
        .await
        .expect("run the discovered scripts");
    let failed: Vec<&str> = outcomes
        .iter()
        .filter(|o| !o.passed)
        .map(|o| o.name.as_str())
        .collect();
    assert_eq!(
        failed,
        vec!["zzz-newly-added-assertions.sh"],
        "the newly added failing script must be run and reported as failed"
    );
}

/// AC2: one run surfaces every problem, so a failure does not stop the run.
///
/// Ordered fail / pass / fail on purpose: stopping at the first failure would
/// report one failure and never reach the third script, which is the behavior
/// that makes a contributor fix-and-rerun once per broken assertion.
#[tokio::test]
async fn a_failing_script_does_not_stop_the_run() {
    let scratch = tempfile::tempdir().expect("scratch dir");
    write_script(
        scratch.path(),
        "1-fails-assertions.sh",
        "#!/bin/bash\nexit 1\n",
    );
    write_script(
        scratch.path(),
        "2-passes-assertions.sh",
        "#!/bin/bash\nexit 0\n",
    );
    write_script(
        scratch.path(),
        "3-also-fails-assertions.sh",
        "#!/bin/bash\nexit 3\n",
    );

    let scripts = discover_chart_check_scripts(scratch.path()).expect("discover");
    let outcomes = run_chart_check_scripts(scratch.path(), &scripts)
        .await
        .expect("run the discovered scripts");

    assert_eq!(
        outcomes.len(),
        3,
        "every script must run, not just up to the first failure"
    );
    let failed: Vec<&str> = outcomes
        .iter()
        .filter(|o| !o.passed)
        .map(|o| o.name.as_str())
        .collect();
    assert_eq!(
        failed,
        vec!["1-fails-assertions.sh", "3-also-fails-assertions.sh"],
        "both failures must be reported from a single run"
    );
}

/// Discovery selects executable `*.sh` only: a README or a non-executable
/// fragment sitting in the directory is not an assertion script, and running one
/// would fail the whole verb for a file that never was a check.
#[tokio::test]
async fn discovery_skips_non_scripts_and_non_executable_files() {
    let scratch = tempfile::tempdir().expect("scratch dir");
    write_script(
        scratch.path(),
        "real-assertions.sh",
        "#!/bin/bash\nexit 0\n",
    );
    std::fs::write(scratch.path().join("README.md"), "not a script\n").expect("write readme");
    std::fs::write(scratch.path().join("fragment.sh"), "#!/bin/bash\nexit 1\n")
        .expect("write non-executable fragment");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(
            scratch.path().join("fragment.sh"),
            std::fs::Permissions::from_mode(0o644),
        )
        .expect("chmod fragment non-executable");
    }

    let scripts = discover_chart_check_scripts(scratch.path()).expect("discover");
    assert_eq!(
        scripts
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
            .collect::<Vec<_>>(),
        vec!["real-assertions.sh".to_string()],
        "only executable *.sh files are assertion scripts"
    );
}

/// The direct children of `charts/curie/ci/` named by helm-ci. Runtime scripts
/// under `ci/runtime/` are cluster checks outside chart-check's shallow render
/// inventory, so they must not be compared with its root-only discovery.
fn helm_ci_referenced_root_scripts_in(workflow: &str) -> Vec<String> {
    let needle = format!("{CHART_CI_DIR}/");
    let mut found: Vec<String> = Vec::new();
    for line in workflow.lines() {
        let Some(start) = line.find(&needle) else {
            continue;
        };
        let rest = &line[start + needle.len()..];
        let end = rest.find(".sh").expect("a referenced script ends in .sh");
        let script = &rest[..end + 3];
        if !script.contains('/') {
            found.push(script.to_string());
        }
    }
    found
}

/// The set of root scripts helm-ci names, recovered from the workflow rather
/// than from a second hardcoded list.
fn helm_ci_referenced_scripts() -> Vec<String> {
    let workflow = std::fs::read_to_string(repo_root().join(".github/workflows/helm-ci.yaml"))
        .expect(".github/workflows/helm-ci.yaml is committed");
    helm_ci_referenced_root_scripts_in(&workflow)
}

#[test]
fn helm_ci_reference_inventory_excludes_nested_runtime_scripts() {
    let workflow = r#"
        run: bash charts/curie/ci/first-render-assertions.sh
        run: bash charts/curie/ci/runtime/installation-runtime.sh
        run: bash charts/curie/ci/second-render-assertions.sh
    "#;

    assert_eq!(
        helm_ci_referenced_root_scripts_in(workflow),
        vec![
            "first-render-assertions.sh".to_string(),
            "second-render-assertions.sh".to_string(),
        ],
        "nested runtime checks run in helm-ci but are outside chart-check's shallow root inventory"
    );
}

/// AC5: the verb and helm-ci cannot silently diverge.
///
/// The verb discovers the directory and helm-ci names each script in its own
/// step, so the two agree only by construction if something asserts it. Without
/// this, a script added to the directory is run locally and never in CI (a check
/// that blocks no release), or a helm-ci step outlives the script it names (a
/// step that fails for a missing file). Exact 1:1 is assertable because this
/// change also removed the duplicated `worker-object-store-assertions.sh` step
/// that #1473 filed.
#[test]
fn helm_ci_and_the_chart_ci_directory_cannot_diverge() {
    let ci_dir = repo_root().join(CHART_CI_DIR);
    let mut on_disk: Vec<String> = discover_chart_check_scripts(&ci_dir)
        .expect("the committed chart assertion directory is readable")
        .iter()
        .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
        .collect();
    on_disk.sort();

    let referenced = helm_ci_referenced_scripts();
    let mut unique = referenced.clone();
    unique.sort();
    unique.dedup();

    assert_eq!(
        referenced.len(),
        unique.len(),
        "helm-ci names a script more than once; a duplicated step costs CI time and \
         hides which assertion actually covers the change"
    );
    assert_eq!(
        unique, on_disk,
        "helm-ci and {CHART_CI_DIR} disagree. Every script in the directory needs a \
         helm-ci step, and every helm-ci step needs a script that exists."
    );
}

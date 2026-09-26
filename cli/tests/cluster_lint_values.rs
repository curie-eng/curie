//! Binary contract for checking file based Helm upgrades against stored user values.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use serde_json::Value;

const SECRET: &str = "CREDENTIAL_SENTINEL_2955";

fn write_executable(path: &Path, body: &str) {
    fs::write(path, body).expect("write fake Helm executable");
    let mut permissions = fs::metadata(path).expect("stat fake Helm").permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).expect("make fake Helm executable");
}

struct Fixture {
    _temp: tempfile::TempDir,
    bin_dir: PathBuf,
    kubeconfig: PathBuf,
    helm_log: PathBuf,
    stored_values: PathBuf,
}

impl Fixture {
    fn new(stored_values: &str) -> Self {
        let temp = tempfile::tempdir().expect("temporary directory");
        let bin_dir = temp.path().join("bin");
        fs::create_dir(&bin_dir).expect("fake binary directory");
        let kubeconfig = temp.path().join("kubeconfig");
        fs::write(
            &kubeconfig,
            "apiVersion: v1\nkind: Config\ncurrent-context: test-ctx\ncontexts:\n- name: test-ctx\n  context:\n    cluster: test-cluster\n    user: test-user\nclusters: []\nusers: []\n",
        )
        .expect("write kubeconfig");
        write_executable(
            &bin_dir.join("helm"),
            r#"#!/bin/sh
printf '%s\n' "$*" >> "$CURIE_TEST_HELM_LOG"
if [ "$1" != get ] || [ "$2" != values ]; then
    printf 'unexpected Helm mutation or read: %s\n' "$*" >&2
    exit 64
fi
if [ "$HELM_KUBECONTEXT" != test-ctx ]; then
    printf 'Helm did not use the selected context\n' >&2
    exit 64
fi
case "$CURIE_TEST_HELM_MODE" in
    present) cat "$CURIE_TEST_STORED_VALUES" ;;
    absent) printf 'Error: release: not found\n' >&2; exit 1 ;;
    failure) printf 'Error: Kubernetes cluster unreachable\n' >&2; exit 1 ;;
    *) printf 'invalid test mode\n' >&2; exit 64 ;;
esac
"#,
        );
        let helm_log = temp.path().join("helm.log");
        let stored_values_path = temp.path().join("stored-values.json");
        fs::write(&stored_values_path, stored_values).expect("write stored values");
        Self {
            _temp: temp,
            bin_dir,
            kubeconfig,
            helm_log,
            stored_values: stored_values_path,
        }
    }

    fn file(&self, name: &str, yaml: &str) -> PathBuf {
        let path = self._temp.path().join(name);
        fs::write(&path, yaml).expect("write pending values file");
        path
    }

    fn run(&self, files: &[&Path], json: bool, mode: &str) -> Output {
        let mut paths = vec![self.bin_dir.clone()];
        paths.extend(std::env::split_paths(
            &std::env::var_os("PATH").unwrap_or_default(),
        ));
        let mut command = Command::new(env!("CARGO_BIN_EXE_curie"));
        command
            .args(["--color", "never"])
            .args(json.then_some("--json"))
            .args([
                "cluster",
                "--context",
                "test-ctx",
                "lint-values",
                "--namespace",
                "target-namespace",
                "--release",
                "target-release",
            ])
            .current_dir(self._temp.path())
            .env("PATH", std::env::join_paths(paths).expect("join PATH"))
            .env("HOME", self._temp.path())
            .env("KUBECONFIG", &self.kubeconfig)
            .env("CURIE_TEST_HELM_LOG", &self.helm_log)
            .env("CURIE_TEST_STORED_VALUES", &self.stored_values)
            .env("CURIE_TEST_HELM_MODE", mode)
            .env("TERM", "dumb")
            .env("NO_COLOR", "1")
            .env_remove("HELM_KUBECONTEXT");
        for file in files {
            command.arg("-f").arg(file);
        }
        command.output().expect("run curie cluster lint-values")
    }

    fn helm_calls(&self) -> Vec<String> {
        fs::read_to_string(&self.helm_log)
            .unwrap_or_default()
            .lines()
            .map(str::to_owned)
            .collect()
    }
}

fn stdout(output: &Output) -> String {
    String::from_utf8(output.stdout.clone()).expect("UTF 8 stdout")
}

fn stderr(output: &Output) -> String {
    String::from_utf8(output.stderr.clone()).expect("UTF 8 stderr")
}

fn assert_no_secret(output: &Output) {
    assert!(!stdout(output).contains(SECRET), "stdout leaked a value");
    assert!(!stderr(output).contains(SECRET), "stderr leaked a value");
}

fn assert_stored_user_values_read(fixture: &Fixture) {
    let calls = fixture.helm_calls();
    assert_eq!(calls.len(), 1, "lint must perform one read only: {calls:?}");
    let args: Vec<_> = calls[0].split_whitespace().collect();
    assert_eq!(
        args,
        [
            "get",
            "values",
            "target-release",
            "-n",
            "target-namespace",
            "-o",
            "json"
        ],
        "the Helm read must use stored user values without --all"
    );
}

#[test]
fn reports_only_dropped_paths_in_human_output() {
    let fixture = Fixture::new(&format!(
        r#"{{"dropped":{{"disabled":false,"unset":null,"emptyMap":{{}},"emptyList":[],"emptyString":""}},"retained":{{"disabled":false,"unset":null,"emptyMap":{{}},"emptyList":[],"emptyString":""}},"secret":{{"token":"{SECRET}"}},"items":[{{"name":"first"}},{{"name":"second"}}]}}"#
    ));
    let file = fixture.file(
        "pending.yaml",
        "retained:\n  disabled: false\n  unset: null\n  emptyMap: {}\n  emptyList: []\n  emptyString: ''\nsecret:\n  token: changed\nitems:\n  - name: first\n",
    );
    let output = fixture.run(&[&file], false, "present");
    assert!(output.status.success(), "stderr: {}", stderr(&output));
    let text = stdout(&output);
    for path in [
        "dropped.disabled",
        "dropped.unset",
        "dropped.emptyMap",
        "dropped.emptyList",
        "dropped.emptyString",
        "items[1].name",
    ] {
        assert!(text.contains(path), "missing {path} from {text}");
    }
    for path in ["retained.disabled", "retained.unset", "secret.token"] {
        assert!(!text.contains(path), "retained path {path} in {text}");
    }
    assert_no_secret(&output);
    assert_stored_user_values_read(&fixture);
}

#[test]
fn repeatable_files_merge_in_command_line_order_and_json_is_one_object() {
    let fixture = Fixture::new(&format!(
        r#"{{"settings":{{"token":"{SECRET}"}},"retained":{{"leaf":false}}}}"#
    ));
    let first = fixture.file(
        "first.yaml",
        "settings:\n  token: changed\nretained:\n  leaf: false\n",
    );
    let second = fixture.file("second.yaml", "settings: null\nretained: {}\n");
    let output = fixture.run(&[&first, &second], true, "present");
    assert!(output.status.success(), "stderr: {}", stderr(&output));
    let result: Value = serde_json::from_str(&stdout(&output)).expect("one JSON object");
    assert!(result.is_object(), "JSON result: {result}");
    assert_eq!(result["dropped_paths"], serde_json::json!(["settings.token"]));
    assert_eq!(result["dropped_count"], 1);
    assert_no_secret(&output);
    assert_stored_user_values_read(&fixture);

    let reversed = Fixture::new(&format!(r#"{{"settings":{{"token":"{SECRET}"}}}}"#));
    let null = reversed.file("null.yaml", "settings: null\n");
    let map = reversed.file("map.yaml", "settings:\n  token: changed\n");
    let output = reversed.run(&[&null, &map], true, "present");
    assert!(output.status.success(), "stderr: {}", stderr(&output));
    let result: Value = serde_json::from_str(&stdout(&output)).expect("one JSON object");
    assert_eq!(result["dropped_paths"], serde_json::json!([]));
    assert_eq!(result["dropped_count"], 0);
    assert_no_secret(&output);
    assert_stored_user_values_read(&reversed);
}

#[test]
fn a_missing_release_is_distinct_from_a_failed_read() {
    let fixture = Fixture::new("null");
    let file = fixture.file("pending.yaml", "feature: true\n");
    let output = fixture.run(&[&file], true, "absent");
    assert!(output.status.success(), "stderr: {}", stderr(&output));
    let result: Value = serde_json::from_str(&stdout(&output)).expect("one JSON object");
    assert_eq!(result["release_exists"], false);
    assert_eq!(result["dropped_paths"], serde_json::json!([]));
    assert_stored_user_values_read(&fixture);
}

#[test]
fn unreadable_or_malformed_files_fail_without_a_clean_result_or_secret_bytes() {
    let fixture = Fixture::new(&format!(r#"{{"secret":{{"token":"{SECRET}"}}}}"#));
    let missing = fixture._temp.path().join("missing.yaml");
    let output = fixture.run(&[&missing], true, "present");
    assert!(!output.status.success(), "missing file must fail");
    assert!(!stdout(&output).contains("dropped_paths"));
    assert_no_secret(&output);

    let malformed = fixture.file("malformed.yaml", &format!("secret: [{SECRET}\n"));
    let output = fixture.run(&[&malformed], true, "present");
    assert!(!output.status.success(), "malformed YAML must fail");
    assert!(!stdout(&output).contains("dropped_paths"));
    assert_no_secret(&output);
}

#[test]
fn helm_read_failure_fails_closed_without_echoing_values() {
    let fixture = Fixture::new(&format!(r#"{{"secret":{{"token":"{SECRET}"}}}}"#));
    let file = fixture.file("pending.yaml", &format!("secret:\n  token: {SECRET}\n"));
    let output = fixture.run(&[&file], true, "failure");
    assert!(!output.status.success(), "Helm read failure must fail");
    assert!(!stdout(&output).contains("dropped_paths"));
    assert_no_secret(&output);
    assert_stored_user_values_read(&fixture);
}

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
    rendered_values: PathBuf,
    real_helm: Option<PathBuf>,
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
if [ "$HELM_KUBECONTEXT" != test-ctx ]; then
    printf 'Helm did not use the selected context\n' >&2
    exit 64
fi
if [ "$1" = template ]; then
    if [ -n "$CURIE_TEST_REAL_HELM" ]; then
        exec "$CURIE_TEST_REAL_HELM" "$@"
    fi
    if [ "$2" != curie-values-lint ] || [ ! -f "$3/Chart.yaml" ] || [ ! -f "$3/templates/values.yaml" ]; then
        printf 'invalid temporary chart\n' >&2
        exit 64
    fi
    chart="$3"
    shift 3
    index=0
    while [ "$#" -gt 0 ]; do
        if [ "$1" != -f ] || [ "$#" -lt 2 ] || [ ! -r "$2" ]; then
            printf 'invalid values file\n' >&2
            exit 64
        fi
        if [ ! -r "$chart/files/$index.yaml" ]; then
            printf 'values file missing from chart\n' >&2
            exit 64
        fi
        case "$2" in
            *malformed.yaml) printf 'bad YAML containing %s\n' "$CURIE_TEST_SECRET" >&2; exit 1 ;;
        esac
        shift 2
        index=$((index + 1))
    done
    cat "$CURIE_TEST_RENDERED_VALUES"
    exit 0
fi
if [ "$1" != get ] || [ "$2" != values ]; then
    printf 'unexpected Helm mutation or read: %s\n' "$*" >&2
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
        let rendered_values = temp.path().join("rendered-values.yaml");
        fs::write(&rendered_values, helm_rendered_values(&[])).expect("write rendered values");
        Self {
            _temp: temp,
            bin_dir,
            kubeconfig,
            helm_log,
            stored_values: stored_values_path,
            rendered_values,
            real_helm: None,
        }
    }

    fn with_real_helm(mut self) -> Self {
        self.real_helm = std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default())
            .map(|dir| dir.join("helm"))
            .find(|path| path.is_file());
        assert!(
            self.real_helm.is_some(),
            "Helm is required for the parser contract"
        );
        self
    }

    fn pending_values(&self, values: &[Value]) {
        fs::write(&self.rendered_values, helm_rendered_values(values))
            .expect("write Helm parsed values");
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
            .env("CURIE_TEST_RENDERED_VALUES", &self.rendered_values)
            .env("CURIE_TEST_SECRET", SECRET)
            .env("CURIE_TEST_HELM_MODE", mode)
            .env(
                "CURIE_TEST_REAL_HELM",
                self.real_helm.as_deref().unwrap_or_else(|| Path::new("")),
            )
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

fn helm_rendered_values(values: &[Value]) -> String {
    let mut manifest = String::from("---\n# Source: curie-values-lint/templates/values.yaml\napiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: curie-values-lint\ndata:\n");
    if values.is_empty() {
        manifest.push_str("  empty: \"{}\"\n");
    }
    for (index, value) in values.iter().enumerate() {
        let json = serde_json::to_string(value).expect("serialize pending values");
        let quoted = serde_json::to_string(&json).expect("quote pending values");
        manifest.push_str(&format!("  file{index}: {quoted}\n"));
    }
    manifest
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
    assert_eq!(calls.len(), 2, "lint must render then read: {calls:?}");
    assert!(
        calls[0].starts_with("template curie-values-lint "),
        "{calls:?}"
    );
    let args: Vec<_> = calls[1].split_whitespace().collect();
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
fn reports_only_changed_paths_in_human_output() {
    let fixture = Fixture::new(&format!(
        r#"{{"dropped":{{"disabled":false,"unset":null,"emptyMap":{{}},"emptyList":[],"emptyString":""}},"changed":{{"scalar":1,"unset":null,"emptyMap":{{}},"emptyList":[],"emptyString":""}},"retained":{{"disabled":false,"unset":null,"emptyMap":{{}},"emptyList":[],"emptyString":""}},"secret":{{"token":"{SECRET}"}},"items":[{{"name":"first"}},{{"name":"second"}}]}}"#
    ));
    let file = fixture.file(
        "pending.yaml",
        "changed:\n  scalar: 2\n  unset: false\n  emptyMap: []\n  emptyList: {}\n  emptyString: changed\nretained:\n  disabled: false\n  unset: null\n  emptyMap: {}\n  emptyList: []\n  emptyString: ''\nsecret:\n  token: changed\nitems:\n  - name: first\n  - name: third\n  - name: fourth\nadded:\n  leaf: true\n",
    );
    fixture.pending_values(&[serde_json::json!({
        "changed": {"scalar": 2, "unset": false, "emptyMap": [], "emptyList": {}, "emptyString": "changed"},
        "retained": {"disabled": false, "unset": null, "emptyMap": {}, "emptyList": [], "emptyString": ""},
        "secret": {"token": "changed"},
        "items": [{"name": "first"}, {"name": "third"}, {"name": "fourth"}],
        "added": {"leaf": true}
    })]);
    let output = fixture.run(&[&file], false, "present");
    assert!(output.status.success(), "stderr: {}", stderr(&output));
    let text = stdout(&output);
    for path in [
        "dropped.disabled",
        "dropped.unset",
        "dropped.emptyMap",
        "dropped.emptyList",
        "dropped.emptyString",
        "changed.scalar",
        "changed.unset",
        "changed.emptyMap",
        "changed.emptyList",
        "changed.emptyString",
        "items[1].name",
        "items[2].name",
        "secret.token",
        "added.leaf",
    ] {
        assert!(text.contains(path), "missing {path} from {text}");
    }
    for path in ["retained.disabled", "retained.unset"] {
        assert!(!text.contains(path), "retained path {path} in {text}");
    }
    for value in ["third", "fourth"] {
        assert!(
            !text.contains(value),
            "value leaked into path report: {text}"
        );
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
    fixture.pending_values(&[
        serde_json::json!({"settings": {"token": "changed"}, "retained": {"leaf": false}}),
        serde_json::json!({"settings": null, "retained": {}}),
    ]);
    let output = fixture.run(&[&first, &second], true, "present");
    assert!(output.status.success(), "stderr: {}", stderr(&output));
    let result: Value = serde_json::from_str(&stdout(&output)).expect("one JSON object");
    assert!(result.is_object(), "JSON result: {result}");
    let schema: Value = serde_json::from_str(include_str!("../schema/lint-values.schema.json"))
        .expect("committed lint schema");
    jsonschema::validator_for(&schema)
        .expect("valid lint schema")
        .validate(&result)
        .expect("JSON result matches lint schema");
    assert_eq!(result.as_object().expect("JSON object").len(), 3);
    assert_eq!(
        result["changed_paths"],
        serde_json::json!(["settings", "settings.token"])
    );
    assert_eq!(result["changed_count"], 2);
    assert_no_secret(&output);
    assert_stored_user_values_read(&fixture);

    let reversed = Fixture::new(&format!(r#"{{"settings":{{"token":"{SECRET}"}}}}"#));
    let null = reversed.file("null.yaml", "settings: null\n");
    let map = reversed.file("map.yaml", "settings:\n  token: changed\n");
    reversed.pending_values(&[
        serde_json::json!({"settings": null}),
        serde_json::json!({"settings": {"token": "changed"}}),
    ]);
    let output = reversed.run(&[&null, &map], true, "present");
    assert!(output.status.success(), "stderr: {}", stderr(&output));
    let result: Value = serde_json::from_str(&stdout(&output)).expect("one JSON object");
    assert_eq!(
        result["changed_paths"],
        serde_json::json!(["settings.token"])
    );
    assert_eq!(result["changed_count"], 1);
    assert_no_secret(&output);
    assert_stored_user_values_read(&reversed);
}

#[test]
fn helm_yaml_scalars_and_explicit_nulls_are_compared() {
    // Helm documents YAML scalar conversion at
    // https://helm.sh/docs/chart_template_guide/yaml_techniques/.
    // An observed Helm template render resolves on and off as booleans and
    // 012 as decimal 10. Reading each file with fromYaml retains null.
    let fixture = Fixture::new(
        r#"{"enabled":"on","disabled":"off","octal":12,"unset":"old","literal":"on"}"#,
    )
    .with_real_helm();
    let file = fixture.file(
        "pending.yaml",
        "enabled: on\ndisabled: off\noctal: 012\nunset: null\nliteral: 'on'\n",
    );
    let output = fixture.run(&[&file], true, "present");
    assert!(output.status.success(), "stderr: {}", stderr(&output));
    let result: Value = serde_json::from_str(&stdout(&output)).expect("one JSON object");
    assert_eq!(
        result["changed_paths"],
        serde_json::json!(["disabled", "enabled", "octal", "unset"])
    );
    assert_eq!(result["changed_count"], 4);
    assert_stored_user_values_read(&fixture);
}

#[test]
fn a_missing_release_is_distinct_from_a_failed_read() {
    let fixture = Fixture::new("null");
    let file = fixture.file("pending.yaml", "feature: true\n");
    fixture.pending_values(&[serde_json::json!({"feature": true})]);
    let output = fixture.run(&[&file], true, "absent");
    assert!(output.status.success(), "stderr: {}", stderr(&output));
    let result: Value = serde_json::from_str(&stdout(&output)).expect("one JSON object");
    assert_eq!(result["release_exists"], false);
    assert_eq!(result["changed_paths"], serde_json::json!(["feature"]));
    assert_stored_user_values_read(&fixture);
}

#[test]
fn unreadable_or_malformed_files_fail_without_a_clean_result_or_secret_bytes() {
    let fixture = Fixture::new(&format!(r#"{{"secret":{{"token":"{SECRET}"}}}}"#));
    let missing = fixture._temp.path().join("missing.yaml");
    let output = fixture.run(&[&missing], true, "present");
    assert!(!output.status.success(), "missing file must fail");
    assert!(!stdout(&output).contains("changed_paths"));
    assert_no_secret(&output);

    let malformed = fixture.file("malformed.yaml", &format!("secret: [{SECRET}\n"));
    let output = fixture.run(&[&malformed], true, "present");
    assert!(!output.status.success(), "malformed YAML must fail");
    assert!(!stdout(&output).contains("changed_paths"));
    assert_no_secret(&output);
}

#[test]
fn helm_read_failure_fails_closed_without_echoing_values() {
    let fixture = Fixture::new(&format!(r#"{{"secret":{{"token":"{SECRET}"}}}}"#));
    let file = fixture.file("pending.yaml", &format!("secret:\n  token: {SECRET}\n"));
    fixture.pending_values(&[serde_json::json!({"secret": {"token": SECRET}})]);
    let output = fixture.run(&[&file], true, "failure");
    assert!(!output.status.success(), "Helm read failure must fail");
    assert!(!stdout(&output).contains("changed_paths"));
    assert_no_secret(&output);
    assert_stored_user_values_read(&fixture);
}

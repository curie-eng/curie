//! `curie apply --dry-run` and `curie diff` with and without a declared
//! secrets provider (ADR 0163 decision 5), driven through the binary with
//! PATH stubs for helm, kubectl and aws. The aws stub records any invocation,
//! so a test can prove the offline paths never reach Secrets Manager.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use serde_json::Value;

const MODEL_VALUE: &str = "sentinel-model-value-7f3a";
const GITHUB_VALUE: &str = "sentinel-github-value-7f3a";
const APP_VALUE: &str = "sentinel-app-value-7f3a";
const BOT_VALUE: &str = "sentinel-bot-value-7f3a";

const DECLARED: &str = "credentials:\n  model: ROUTING_TEST_MODEL_KEY\ncomms:\n  slack:\n    app_token: ROUTING_TEST_APP_TOKEN\n    bot_token: ROUTING_TEST_BOT_TOKEN\n";
const PROVIDER: &str = "secrets:\n  provider: aws\n  region: us-east-1\n  prefix: curie/test\n  role_arn: arn:aws:iam::000000000000:role/curie-sync\n";

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("cli has a repository parent")
        .to_path_buf()
}

fn write_exec(dir: &Path, name: &str, body: &str) {
    let path = dir.join(name);
    fs::write(&path, body).expect("write stub");
    let mut permissions = fs::metadata(&path).expect("stub metadata").permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&path, permissions).expect("chmod stub");
}

struct Fixture {
    temp: tempfile::TempDir,
    file: PathBuf,
    log: PathBuf,
    aws_log: PathBuf,
}

impl Fixture {
    fn new(extra: &str) -> Self {
        let temp = tempfile::tempdir().expect("tempdir");
        let file = temp.path().join("curie.yaml");
        fs::write(
            &file,
            format!("version: 1\ninstall:\n  namespace: rel\n  release: rel\n{extra}"),
        )
        .expect("write curie.yaml");
        let bin_dir = temp.path().join("bin");
        fs::create_dir(&bin_dir).expect("bin dir");
        // An absent release: every read says not found; anything else is a
        // no-op that is still logged.
        write_exec(
            &bin_dir,
            "helm",
            r#"#!/bin/sh
printf 'HELM: %s\n' "$*" >> "$ROUTING_TEST_LOG"
case "$1 $2" in
    "get values"|"history "*|"status "*)
        printf '%s\n' 'Error: release: not found' >&2
        exit 1
        ;;
    "list "*)
        printf '%s\n' '[]'
        exit 0
        ;;
esac
exit 0
"#,
        );
        write_exec(
            &bin_dir,
            "kubectl",
            r#"#!/bin/sh
printf 'KUBECTL: %s\n' "$*" >> "$ROUTING_TEST_LOG"
case "$*" in
    *"get statefulset"*)
        printf '%s\n' '{"apiVersion":"v1","items":[],"kind":"List","metadata":{"resourceVersion":""}}'
        ;;
esac
exit 0
"#,
        );
        write_exec(
            &bin_dir,
            "aws",
            r#"#!/bin/sh
printf 'AWS: %s\n' "$*" >> "$ROUTING_TEST_AWS_LOG"
exit 1
"#,
        );
        let log = temp.path().join("calls.log");
        let aws_log = temp.path().join("aws.log");
        Self {
            temp,
            file,
            log,
            aws_log,
        }
    }

    fn run(&self, args: &[&str]) -> Output {
        let mut paths = vec![self.temp.path().join("bin")];
        if let Some(current) = std::env::var_os("PATH") {
            paths.extend(std::env::split_paths(&current));
        }
        Command::new(bin())
            .current_dir(repo_root())
            .arg("--json")
            .args(args)
            .arg("--file")
            .arg(&self.file)
            .env("PATH", std::env::join_paths(paths).expect("PATH"))
            .env("ROUTING_TEST_LOG", &self.log)
            .env("ROUTING_TEST_AWS_LOG", &self.aws_log)
            .env("CURIE_CONFIG_DIR", self.temp.path().join("config"))
            .env("ROUTING_TEST_MODEL_KEY", MODEL_VALUE)
            .env("ROUTING_TEST_GITHUB_TOKEN", GITHUB_VALUE)
            .env("ROUTING_TEST_APP_TOKEN", APP_VALUE)
            .env("ROUTING_TEST_BOT_TOKEN", BOT_VALUE)
            .env_remove("CURIE_CREDENTIALS")
            .env_remove("CURIE_MODEL_CREDENTIALS")
            .env_remove("CURIE_GITHUB_TOKEN")
            .env_remove("CURIE_MODEL")
            .env_remove("AWS_PROFILE")
            .output()
            .expect("run curie")
    }

    fn calls(&self) -> String {
        fs::read_to_string(&self.log).unwrap_or_default()
    }

    fn assert_aws_never_called(&self) {
        assert!(
            !self.aws_log.exists(),
            "aws was invoked: {}",
            fs::read_to_string(&self.aws_log).unwrap_or_default()
        );
    }

    fn assert_no_kubectl_mutation(&self) {
        for line in self.calls().lines().filter(|l| l.starts_with("KUBECTL: ")) {
            for verb in [
                " apply ",
                " create ",
                " patch ",
                " annotate ",
                " label ",
                " delete ",
                " replace ",
                " rollout ",
            ] {
                assert!(
                    !format!("{line} ").contains(verb),
                    "kubectl mutation under dry run: {line}"
                );
            }
        }
        for line in self.calls().lines().filter(|l| l.starts_with("HELM: ")) {
            assert!(
                !line.starts_with("HELM: upgrade") && !line.starts_with("HELM: install"),
                "helm mutation under dry run: {line}"
            );
        }
    }
}

fn visible(output: &Output) -> String {
    format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn success_json(output: &Output, verb: &str) -> Value {
    assert!(
        output.status.success(),
        "{verb} failed:\n{}",
        visible(output)
    );
    serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|error| panic!("{verb} did not emit JSON ({error}):\n{}", visible(output)))
}

fn plan_lines(json: &Value) -> Vec<String> {
    json["plan"]
        .as_array()
        .expect("dry run plan array")
        .iter()
        .map(|line| line.as_str().expect("plan line").to_string())
        .collect()
}

fn assert_no_values(text: &str) {
    for value in [MODEL_VALUE, GITHUB_VALUE, APP_VALUE, BOT_VALUE] {
        assert!(!text.contains(value), "credential value leaked: {text}");
    }
}

#[test]
fn provider_absent_apply_dry_run_keeps_todays_shape() {
    let fixture = Fixture::new(DECLARED);
    let output = fixture.run(&["apply", "--dry-run"]);
    let json = success_json(&output, "apply --dry-run");
    let lines = plan_lines(&json);
    let all = visible(&output);

    let install = lines
        .iter()
        .find(|l| l.starts_with("helm upgrade --install "))
        .unwrap_or_else(|| panic!("no install step: {lines:#?}"));
    assert!(
        install.contains("agentSandbox.runner.credentials=sentinel***"),
        "model credential must be present and masked: {install}"
    );
    assert!(
        !install.contains("ExistingSecret"),
        "no existingSecret knobs without a provider: {install}"
    );
    assert!(!all.contains("--history-max"), "{all}");
    assert!(
        lines
            .iter()
            .any(|l| l.starts_with("helm upgrade rel ") && l.contains("--reuse-values")),
        "the comms step must still be planned: {lines:#?}"
    );
    assert!(!all.contains("ExternalSecret"), "{all}");
    assert_no_values(&all);
    fixture.assert_aws_never_called();
}

#[test]
fn provider_absent_diff_keeps_todays_shape() {
    let fixture = Fixture::new(DECLARED);
    let output = fixture.run(&["diff"]);
    let json = success_json(&output, "diff");
    let entries = json["entries"].as_array().expect("diff entries");
    let credential = entries
        .iter()
        .find(|e| e["key"] == "agentSandbox.runner.credentials")
        .unwrap_or_else(|| panic!("model credential missing from diff: {json}"));
    assert_eq!(credential["to"], "<secret>", "{credential}");
    for entry in entries {
        let key = entry["key"].as_str().unwrap_or_default();
        assert!(
            !key.starts_with("installation.") && !key.starts_with("postgres.existingSecret"),
            "provider knob in a provider-absent diff: {entry}"
        );
    }
    assert!(
        entries
            .iter()
            .any(|e| e["key"] == "dispatcher.slack.botToken" && e["to"] == "<secret>"),
        "the Slack path is unchanged: {json}"
    );
    assert_no_values(&visible(&output));
    fixture.assert_aws_never_called();
}

#[test]
fn provider_present_apply_dry_run_routes_names_offline() {
    let fixture = Fixture::new(&format!("{PROVIDER}{DECLARED}"));
    let output = fixture.run(&["apply", "--dry-run"]);
    let all = visible(&output);
    assert!(output.status.success(), "apply --dry-run failed:\n{all}");

    assert!(all.contains("--history-max"), "{all}");
    assert!(
        all.contains("installation.idExistingSecret=rel-curie-installation-id"),
        "{all}"
    );
    assert!(
        all.contains("agentSandbox.runner.credentialsExistingSecret=rel-curie-runner-credentials"),
        "{all}"
    );
    assert!(
        all.contains("kubectl apply secretstore rel-aws"),
        "External Secrets bootstrap in dry run: {all}"
    );
    assert!(
        all.contains("--version 2.11.0"),
        "dry run must name the pinned External Secrets chart: {all}"
    );
    for target in [
        "rel-curie-installation-id",
        "rel-curie-postgres",
        "rel-curie-langfuse",
        "rel-curie-runner-credentials",
        "rel-curie-slack",
    ] {
        assert!(all.contains(target), "{target} missing from dry run: {all}");
    }
    assert!(all.contains("ExternalSecret"), "{all}");
    assert!(
        !all.contains("--reuse-values"),
        "Slack is folded into the knob sets, no comms step: {all}"
    );
    assert!(
        !all.contains("agentSandbox.runner.credentials=")
            && !all.contains("dispatcher.slack.botToken=sentinel"),
        "no provider-backed value reaches Helm: {all}"
    );
    assert_no_values(&all);
    assert_no_values(&fixture.calls());
    fixture.assert_aws_never_called();
    fixture.assert_no_kubectl_mutation();
}

// ------------------------------------------------ non-dry-run refusal order

/// A fresh release with a present SecretStore, driven through a real
/// (non-dry-run) apply. `ROUTING_TEST_MODE` picks the one foreign object the
/// kubectl stub reports: `target` (an unowned `rel-curie-postgres` Secret) or
/// `namespace` (a namespace another release created). Every stub invocation is
/// logged, so a test can prove the refusal came before any write.
struct LiveFixture {
    temp: tempfile::TempDir,
    file: PathBuf,
    log: PathBuf,
}

impl LiveFixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().expect("tempdir");
        let file = temp.path().join("curie.yaml");
        fs::write(
            &file,
            format!("version: 1\ninstall:\n  namespace: rel\n  release: rel\n{PROVIDER}"),
        )
        .expect("write curie.yaml");
        let bin_dir = temp.path().join("bin");
        fs::create_dir(&bin_dir).expect("bin dir");
        write_exec(
            &bin_dir,
            "helm",
            r#"#!/bin/sh
printf 'HELM: %s\n' "$*" >> "$ROUTING_TEST_LOG"
case "$1 $2" in
    "get values"|"history "*|"status "*)
        printf '%s\n' 'Error: release: not found' >&2
        exit 1
        ;;
    "list "*)
        printf '%s\n' '[]'
        exit 0
        ;;
esac
exit 0
"#,
        );
        write_exec(
            &bin_dir,
            "kubectl",
            r#"#!/bin/sh
printf 'KUBECTL: %s\n' "$*" >> "$ROUTING_TEST_LOG"
case "$*" in
    *"get statefulset"*)
        printf '%s\n' '{"apiVersion":"v1","items":[],"kind":"List","metadata":{"resourceVersion":""}}'
        ;;
    *"get secretstore rel-aws"*)
        printf '%s\n' '{"apiVersion":"external-secrets.io/v1","kind":"SecretStore","metadata":{"name":"rel-aws","namespace":"rel"}}'
        ;;
    *"get secret rel-curie-postgres "*)
        if [ "$ROUTING_TEST_MODE" = target ]; then
            printf '%s\n' '{"apiVersion":"v1","kind":"Secret","metadata":{"name":"rel-curie-postgres","namespace":"rel"}}'
        fi
        ;;
    *"get namespace rel "*)
        if [ "$ROUTING_TEST_MODE" = namespace ]; then
            printf '%s\n' '{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"rel","uid":"u-1","resourceVersion":"7","labels":{"curietech.ai/created-by":"other","curietech.ai/created-in":"other"}}}'
        fi
        ;;
esac
exit 0
"#,
        );
        write_exec(
            &bin_dir,
            "aws",
            r#"#!/bin/sh
if [ "${1:-}" = '--version' ]; then
    printf '%s\n' 'aws-cli/2.31.0 Python/3.13.7 Linux/fixture exe/x86_64'
    exit 0
fi
printf 'AWS: %s\n' "$*" >> "$ROUTING_TEST_LOG"
case " $* " in
    *" secretsmanager list-secrets "*)
        printf '%s\n' '{"SecretList":[]}'
        ;;
    *" secretsmanager create-secret "*)
        printf '%s\n' '{"VersionId":"00000000-0000-4000-8000-000000000001"}'
        ;;
    *)
        printf '%s\n' 'ResourceNotFoundException: fixture object is absent' >&2
        exit 254
        ;;
esac
"#,
        );
        let log = temp.path().join("calls.log");
        Self { temp, file, log }
    }

    fn apply(&self, mode: &str) -> Output {
        let mut paths = vec![self.temp.path().join("bin")];
        if let Some(current) = std::env::var_os("PATH") {
            paths.extend(std::env::split_paths(&current));
        }
        Command::new(bin())
            .current_dir(repo_root())
            .arg("--json")
            .arg("apply")
            .arg("--file")
            .arg(&self.file)
            .env("PATH", std::env::join_paths(paths).expect("PATH"))
            .env("ROUTING_TEST_LOG", &self.log)
            .env("ROUTING_TEST_MODE", mode)
            .env("CURIE_CONFIG_DIR", self.temp.path().join("config"))
            .env_remove("CURIE_CREDENTIALS")
            .env_remove("CURIE_MODEL_CREDENTIALS")
            .env_remove("CURIE_GITHUB_TOKEN")
            .env_remove("CURIE_MODEL")
            .env_remove("AWS_PROFILE")
            .output()
            .expect("run curie")
    }

    fn calls(&self) -> String {
        fs::read_to_string(&self.log).unwrap_or_default()
    }

    /// Refused, and nothing was written anywhere: no Secrets Manager create,
    /// no kubectl write, no Helm upgrade.
    fn assert_refused_without_mutation(&self, output: &Output, needle: &str) {
        let all = visible(output);
        assert!(!output.status.success(), "apply must refuse:\n{all}");
        assert!(all.contains(needle), "refusal must name {needle}:\n{all}");
        let calls = self.calls();
        assert!(
            calls.contains("secretsmanager list-secrets"),
            "the fixture must reach the provider path: {calls}"
        );
        assert!(
            !calls.contains("create-secret") && !calls.contains("put-secret-value"),
            "Secrets Manager was written before the refusal: {calls}"
        );
        for line in calls.lines().filter(|l| l.starts_with("KUBECTL: ")) {
            for verb in [" label ", " annotate ", " patch ", " apply ", " create "] {
                assert!(
                    !format!("{line} ").contains(verb),
                    "kubectl mutation before the refusal: {line}"
                );
            }
        }
        for line in calls.lines().filter(|l| l.starts_with("HELM: ")) {
            assert!(
                !line.starts_with("HELM: upgrade") && !line.starts_with("HELM: install"),
                "helm mutation before the refusal: {line}"
            );
        }
    }
}

#[test]
fn a_foreign_target_secret_is_refused_before_any_provider_or_cluster_write() {
    let fixture = LiveFixture::new();
    let output = fixture.apply("target");
    fixture.assert_refused_without_mutation(&output, "rel-curie-postgres");
}

#[test]
fn a_foreign_namespace_is_refused_before_any_provider_or_cluster_write() {
    let fixture = LiveFixture::new();
    let output = fixture.apply("namespace");
    fixture.assert_refused_without_mutation(&output, "foreign ownership labels");
}

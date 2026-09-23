#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use sha2::{Digest, Sha256};

fn chart() -> String {
    std::fs::canonicalize(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../charts/curie"))
        .expect("chart directory")
        .to_string_lossy()
        .into_owned()
}
const NAMESPACE: &str = "acme-system";
const RELEASE: &str = "acme-release";
const APP_TOKEN: &str = "xapp-ref-test";
const BOT_TOKEN: &str = "xoxb-ref-test";
const PROVIDER_YAML: &str = "\
version: 1
install:
  namespace: acme-system
  release: acme-release
secrets:
  provider: aws
  region: us-east-1
  prefix: example-prefix
  role_arn: arn:aws:iam::000000000000:role/curie-sync
";

const AWS_SHIM: &str = r#"#!/usr/bin/env bash
set -u
printf '%s\n' "aws $*" >> "$AWS_ARGV_LOG"
if [ "${1:-}" = "--version" ]; then
  printf '%s\n' "aws-cli/2.0.0"
  exit 0
fi
if [ "${1:-}" = "secretsmanager" ] && { [ "${2:-}" = "describe-secret" ] || [ "${2:-}" = "get-secret-value" ]; }; then
  printf '%s\n' "ResourceNotFoundException" >&2
  exit 254
fi
if [ "${1:-}" = "secretsmanager" ] && [ "${2:-}" = "create-secret" ]; then
  prev=""
  input=""
  for arg in "$@"; do
    if [ "$prev" = "--cli-input-json" ]; then
      input=$arg
    fi
    prev=$arg
  done
  file=${input#file://}
  if [ -z "$input" ] || [ ! -f "$file" ]; then
    exit 1
  fi
  cat -- "$file" >> "$AWS_BODY_LOG"
  printf '\n' >> "$AWS_BODY_LOG"
  if [ -n "${SHIM_LOG:-}" ]; then
    printf '%s\n' "aws create-secret" >> "$SHIM_LOG"
  fi
  printf '%s\n' '{"VersionId":"v1"}'
  exit 0
fi
exit 1
"#;

const HELM_SHIM: &str = r#"#!/usr/bin/env bash
set -u
if [ "${1:-}" = "upgrade" ]; then
  printf '%s\n' "helm $*" >> "$HELM_LOG"
  exit 0
fi
exit 1
"#;

const KUBECTL_SHIM: &str = r#"#!/usr/bin/env bash
case " $* " in
  *rollout*) exit 0 ;;
  *externalsecret*)
    printf '%s\n' "Error from server (NotFound): externalsecrets.external-secrets.io not found" >&2
    exit 1
    ;;
esac
exit 1
"#;

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn fingerprint(needle: &str) -> String {
    let hex: String = Sha256::digest(needle.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    hex[..12].to_string()
}

fn redact(text: &str) -> String {
    text.replace(APP_TOKEN, &fingerprint(APP_TOKEN))
        .replace(BOT_TOKEN, &fingerprint(BOT_TOKEN))
}

fn shown(output: &Output) -> String {
    redact(&format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    ))
}

fn assert_absent(haystack: &str, needle: &str) {
    if haystack.contains(needle) {
        panic!("{}", fingerprint(needle));
    }
}

fn install(dir: &Path, name: &str, body: &str) {
    let path = dir.join(name);
    std::fs::write(&path, body).unwrap_or_else(|err| panic!("write {name}: {err}"));
    let mut perms = std::fs::metadata(&path)
        .unwrap_or_else(|err| panic!("stat {name}: {err}"))
        .permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&path, perms).unwrap_or_else(|err| panic!("chmod {name}: {err}"));
}

fn empty_path(dir: &Path) -> std::ffi::OsString {
    let bin_dir = dir.join("bin");
    std::fs::create_dir_all(&bin_dir).expect("bin dir");
    std::env::join_paths([bin_dir]).expect("join PATH")
}

fn prepend_path(bin_dir: &Path) -> std::ffi::OsString {
    let mut dirs = vec![bin_dir.to_path_buf()];
    if let Some(existing) = std::env::var_os("PATH") {
        dirs.extend(std::env::split_paths(&existing));
    }
    std::env::join_paths(dirs).expect("join PATH")
}

fn values_after(plan: &str, key: &str) -> Vec<String> {
    let needle = format!("{key}=");
    let mut values = Vec::new();
    let mut rest = plan;
    while let Some(index) = rest.find(&needle) {
        let after = &rest[index + needle.len()..];
        let end = after
            .find(|c: char| c.is_whitespace() || c == '"' || c == '\'')
            .unwrap_or(after.len());
        values.push(after[..end].to_string());
        rest = &after[end..];
    }
    values
}

fn assert_blank(plan: &str, key: &str) {
    let values = values_after(plan, key);
    assert!(!values.is_empty(), "missing {key}");
    assert!(
        values.iter().all(String::is_empty),
        "{key} was not a blank set"
    );
}

fn comms_args(release: &str, dry_run: bool) -> Vec<String> {
    let mut args = vec![
        "cluster".to_string(),
        "comms".to_string(),
        "--slack".to_string(),
        "--app-token".to_string(),
        APP_TOKEN.to_string(),
        "--bot-token".to_string(),
        BOT_TOKEN.to_string(),
        "--namespace".to_string(),
        NAMESPACE.to_string(),
        "--release".to_string(),
        release.to_string(),
        "--chart".to_string(),
        chart(),
        "--json".to_string(),
    ];
    if dry_run {
        args.push("--dry-run".to_string());
    }
    args
}

fn invoke(dir: &Path, path: std::ffi::OsString, args: &[String], env: &[(&str, &Path)]) -> Output {
    let mut cmd = Command::new(bin());
    cmd.args(args)
        .current_dir(dir)
        .env("PATH", path)
        .env_remove("SLACK_APP_TOKEN")
        .env_remove("SLACK_BOT_TOKEN");
    for (key, value) in env {
        cmd.env(key, value);
    }
    cmd.output()
        .unwrap_or_else(|err| panic!("spawn curie: {err}"))
}

fn plan_text(output: &Output) -> String {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let value: serde_json::Value = serde_json::from_str(stdout.trim())
        .unwrap_or_else(|err| panic!("stdout is not JSON: {err}: {}", redact(&stdout)));
    assert_eq!(
        value.get("dry_run"),
        Some(&serde_json::Value::Bool(true)),
        "{}",
        redact(&stdout)
    );
    value
        .get("plan")
        .and_then(serde_json::Value::as_array)
        .unwrap_or_else(|| panic!("missing plan: {}", redact(&stdout)))
        .iter()
        .filter_map(serde_json::Value::as_str)
        .collect::<Vec<_>>()
        .join("\n")
}

fn inline_set(key: &str, token: &str) -> String {
    format!("{key}={}", curie::ops::mask_secret(token))
}

fn read_bodies(path: &Path) -> Vec<serde_json::Value> {
    let text = std::fs::read_to_string(path).unwrap_or_default();
    if text.trim().is_empty() {
        return Vec::new();
    }
    serde_json::Deserializer::from_str(&text)
        .into_iter::<serde_json::Value>()
        .map(|item| item.unwrap_or_else(|_| panic!("provider input is not JSON")))
        .collect()
}

fn secret_property(body: &serde_json::Value, key: &str) -> Option<String> {
    let raw = body.get("SecretString")?.as_str()?;
    let inner: serde_json::Value = serde_json::from_str(raw).ok()?;
    inner.get(key)?.as_str().map(str::to_string)
}

fn assert_object(bodies: &[serde_json::Value], name: &str, key: &str, expected: &str) {
    let names: Vec<&str> = bodies
        .iter()
        .filter_map(|body| body.get("Name").and_then(serde_json::Value::as_str))
        .collect();
    let matched = bodies.iter().any(|body| {
        body.get("Name").and_then(serde_json::Value::as_str) == Some(name)
            && secret_property(body, key).as_deref() == Some(expected)
    });
    assert!(
        matched,
        "missing {name} property {key} sha256 {}; names {names:?}",
        fingerprint(expected)
    );
}

struct Live {
    dir: tempfile::TempDir,
    helm_log: PathBuf,
    aws_argv: PathBuf,
    aws_body: PathBuf,
}

impl Live {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let bin_dir = dir.path().join("bin");
        std::fs::create_dir(&bin_dir).expect("bin dir");
        install(&bin_dir, "aws", AWS_SHIM);
        install(&bin_dir, "helm", HELM_SHIM);
        install(&bin_dir, "kubectl", KUBECTL_SHIM);
        std::fs::write(dir.path().join("curie.yaml"), PROVIDER_YAML).expect("write curie.yaml");
        Self {
            helm_log: dir.path().join("helm.log"),
            aws_argv: dir.path().join("aws-argv.log"),
            aws_body: dir.path().join("aws-body.log"),
            dir,
        }
    }

    fn run(&self, release: &str) -> Output {
        let env = [
            ("HELM_LOG", self.helm_log.as_path()),
            ("AWS_ARGV_LOG", self.aws_argv.as_path()),
            ("AWS_BODY_LOG", self.aws_body.as_path()),
        ];
        invoke(
            self.dir.path(),
            prepend_path(&self.dir.path().join("bin")),
            &comms_args(release, false),
            &env,
        )
    }
}

#[test]
fn absent_slack_dry_run_blanks_refs_and_keeps_inline_tokens() {
    let dir = tempfile::tempdir().expect("tempdir");
    let output = invoke(
        dir.path(),
        empty_path(dir.path()),
        &comms_args(RELEASE, true),
        &[],
    );
    assert!(
        output.status.success(),
        "absent dry-run must succeed: {}",
        shown(&output)
    );
    let plan = plan_text(&output);
    assert_blank(&plan, "dispatcher.slack.appTokenExistingSecret");
    assert_blank(&plan, "dispatcher.slack.botTokenExistingSecret");
    assert!(
        plan.contains(&inline_set("dispatcher.slack.appToken", APP_TOKEN)),
        "missing inline app token set sha256 {}",
        fingerprint(&inline_set("dispatcher.slack.appToken", APP_TOKEN))
    );
    assert!(
        plan.contains(&inline_set("dispatcher.slack.botToken", BOT_TOKEN)),
        "missing inline bot token set sha256 {}",
        fingerprint(&inline_set("dispatcher.slack.botToken", BOT_TOKEN))
    );
}

#[test]
fn provider_slack_dry_run_sets_inventory_refs_without_tokens() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(dir.path().join("curie.yaml"), PROVIDER_YAML).expect("write curie.yaml");
    let output = invoke(
        dir.path(),
        empty_path(dir.path()),
        &comms_args(RELEASE, true),
        &[],
    );
    assert!(
        output.status.success(),
        "provider dry-run must succeed without aws: {}",
        shown(&output)
    );
    let plan = plan_text(&output);
    for needle in [
        "dispatcher.slack.appTokenExistingSecret=acme-release-curie-slack",
        "dispatcher.slack.appTokenExistingSecretKey=slackAppToken",
        "dispatcher.slack.botTokenExistingSecret=acme-release-curie-slack",
        "dispatcher.slack.botTokenExistingSecretKey=slackBotToken",
    ] {
        assert!(plan.contains(needle), "missing {needle}");
    }
    assert_absent(&plan, APP_TOKEN);
    assert_absent(&plan, BOT_TOKEN);
    assert_absent(&plan, &inline_set("dispatcher.slack.appToken", APP_TOKEN));
    assert_absent(&plan, &inline_set("dispatcher.slack.botToken", BOT_TOKEN));
}

#[test]
fn provider_slack_live_stores_tokens_and_sets_refs() {
    let live = Live::new();
    let output = live.run(RELEASE);
    assert!(
        output.status.success(),
        "provider comms must exit 0: {}",
        shown(&output)
    );
    let helm = std::fs::read_to_string(&live.helm_log).unwrap_or_default();
    for needle in [
        "dispatcher.slack.appTokenExistingSecret=acme-release-curie-slack",
        "dispatcher.slack.appTokenExistingSecretKey=slackAppToken",
        "dispatcher.slack.botTokenExistingSecret=acme-release-curie-slack",
        "dispatcher.slack.botTokenExistingSecretKey=slackBotToken",
    ] {
        assert!(helm.contains(needle), "helm upgrade missing {needle}");
    }
    assert_absent(&helm, APP_TOKEN);
    assert_absent(&helm, BOT_TOKEN);
    let argv = std::fs::read_to_string(&live.aws_argv).unwrap_or_default();
    assert_absent(&argv, APP_TOKEN);
    assert_absent(&argv, BOT_TOKEN);
    let bodies = read_bodies(&live.aws_body);
    assert_object(
        &bodies,
        "example-prefix/acme-release/slack-app-token",
        "slackAppToken",
        APP_TOKEN,
    );
    assert_object(
        &bodies,
        "example-prefix/acme-release/slack-bot-token",
        "slackBotToken",
        BOT_TOKEN,
    );
}

#[test]
fn provider_slack_live_release_mismatch_does_not_write() {
    let live = Live::new();
    let output = live.run("other-release");
    assert!(
        !output.status.success(),
        "release mismatch must be non-zero: {}",
        shown(&output)
    );
    let argv = std::fs::read_to_string(&live.aws_argv).unwrap_or_default();
    assert!(
        !argv.contains("create-secret"),
        "release mismatch invoked create-secret"
    );
    let body = std::fs::read_to_string(&live.aws_body).unwrap_or_default();
    assert!(
        body.trim().is_empty(),
        "release mismatch wrote a provider object"
    );
    let helm = std::fs::read_to_string(&live.helm_log).unwrap_or_default();
    assert!(
        !helm.contains("upgrade"),
        "release mismatch logged helm upgrade"
    );
}

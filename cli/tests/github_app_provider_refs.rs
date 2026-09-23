#![cfg(unix)]

mod support;

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use sha2::{Digest, Sha256};
use support::{serve, MockServer, Request, Response};

fn chart() -> String {
    std::fs::canonicalize(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../charts/curie"))
        .expect("chart directory")
        .to_string_lossy()
        .into_owned()
}
const NAMESPACE: &str = "acme-system";
const RELEASE: &str = "acme-release";
const APP_ID: &str = "1234567";
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
const INVENTORY_REF: &str = "api.githubAppExistingSecret=acme-release-curie-github-app";
const INVENTORY_KEY: &str = "api.githubAppExistingSecretKey=githubAppPrivateKey";
const OPERATOR_REF: &str = "api.githubAppExistingSecret=my-github-app";

const TOOL_SHIM: &str = r#"#!/usr/bin/env bash
tool=$(basename "$0")
echo "$tool $*" >> "$SHIM_LOG"
if [ "$tool" = "helm" ] && [ "$1" = "get" ] && [ "$2" = "values" ]; then
  if [ -n "${VALUES_JSON:-}" ]; then
    cat "$VALUES_JSON"
  else
    echo "$FAKE_VALUES"
  fi
  exit 0
fi
if [ "$tool" = "helm" ] && [ "$1" = "history" ]; then
  rev=1
  if [ -f "$SHIM_LOG.rev" ]; then
    rev=$(cat "$SHIM_LOG.rev")
  fi
  echo "[{\"revision\":$rev,\"status\":\"deployed\",\"chart\":\"curie-0.8.7\",\"app_version\":\"0.8.7\",\"description\":\"Install complete\"}]"
  exit 0
fi
if [ "$tool" = "helm" ] && [ "$1" = "get" ] && [ "$2" = "manifest" ]; then
  cat <<'MANIFEST'
---
apiVersion: extensions.agents.x-k8s.io/v1beta1
kind: SandboxTemplate
metadata:
  name: curie-runner
  labels:
    app.kubernetes.io/component: agent-sandbox
    app.kubernetes.io/instance: curie
    app.kubernetes.io/managed-by: Helm
spec:
  service: true
---
apiVersion: extensions.agents.x-k8s.io/v1beta1
kind: SandboxWarmPool
metadata:
  name: curie-runner-pool
  labels:
    app.kubernetes.io/component: agent-sandbox
    app.kubernetes.io/instance: curie
    app.kubernetes.io/managed-by: Helm
spec:
  replicas: 0
  sandboxTemplateRef:
    name: curie-runner
MANIFEST
  exit 0
fi
if [ "$tool" = "kubectl" ] && echo "$*" | grep -q " get secret "; then
  echo "$FAKE_SECRET_JSON"
  exit 0
fi
if [ "$tool" = "kubectl" ] && [ "$1" = "get" ] \
  && [[ "$2" == sandboxtemplates.extensions.agents.x-k8s.io,* ]]; then
  echo '{"items":[{"apiVersion":"extensions.agents.x-k8s.io/v1beta1","kind":"SandboxTemplate","metadata":{"name":"curie-runner","labels":{"app.kubernetes.io/component":"agent-sandbox","app.kubernetes.io/instance":"curie","app.kubernetes.io/managed-by":"Helm"},"annotations":{"meta.helm.sh/release-name":"acme-release","meta.helm.sh/release-namespace":"acme-system"}},"spec":{"service":true}},{"apiVersion":"extensions.agents.x-k8s.io/v1beta1","kind":"SandboxWarmPool","metadata":{"name":"curie-runner-pool","labels":{"app.kubernetes.io/component":"agent-sandbox","app.kubernetes.io/instance":"curie","app.kubernetes.io/managed-by":"Helm"},"annotations":{"meta.helm.sh/release-name":"acme-release","meta.helm.sh/release-namespace":"acme-system"}},"spec":{"replicas":0,"sandboxTemplateRef":{"name":"curie-runner"}}}]}'
  exit 0
fi
if [ "$tool" = "kubectl" ] && echo "$*" | grep -q "externalsecret"; then
  echo "Error from server (NotFound): externalsecrets.external-secrets.io not found" >&2
  exit 1
fi
if [ "$ALLOW_MUTATION" = "1" ]; then
  if [ "$tool" = "helm" ] && [ "$1" = "upgrade" ]; then
    echo 2 > "$SHIM_LOG.rev"
    exit 0
  fi
  if [ "$tool" = "kubectl" ]; then
    exit 0
  fi
fi
echo "shim: refusing to execute: $tool $*" >&2
exit 1
"#;

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

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn fingerprint(needle: &str) -> String {
    fingerprint_bytes(needle.as_bytes())
}

fn fingerprint_bytes(bytes: &[u8]) -> String {
    let hex: String = Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    hex[..12].to_string()
}

fn assert_absent(haystack: &str, needle: &str) {
    if !needle.is_empty() && haystack.contains(needle) {
        panic!("{}", fingerprint(needle));
    }
}

fn assert_pem_absent(haystack: &str, pem: &str) {
    assert_absent(haystack, pem);
    for line in pem.lines() {
        let line = line.trim();
        if line.contains("PRIVATE KEY") || line.len() >= 20 {
            assert_absent(haystack, line);
        }
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

fn append_line(path: &Path, line: &str) {
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap_or_else(|err| panic!("open log: {err}"));
    writeln!(file, "{line}").unwrap_or_else(|err| panic!("append log: {err}"));
    file.flush()
        .unwrap_or_else(|err| panic!("flush log: {err}"));
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

fn generate_rsa_pem(path: &Path) {
    let output = Command::new("openssl")
        .args(["genrsa", "2048"])
        .output()
        .unwrap_or_else(|err| panic!("openssl genrsa: {err}"));
    assert!(output.status.success(), "openssl genrsa failed");
    std::fs::write(path, &output.stdout).expect("write pem");
}

fn b64(bytes: &[u8]) -> String {
    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, bytes)
}

fn secret_document(pem: &[u8]) -> String {
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "my-github-app"},
        "data": {"app-pem": b64(pem)}
    })
    .to_string()
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

fn assert_cleared(plan: &str, key: &str) {
    let values = values_after(plan, key);
    assert!(!values.is_empty(), "missing {key}");
    assert!(values.iter().all(String::is_empty), "{key} was not cleared");
}

fn redact(text: &str, pem: &str) -> String {
    if pem.is_empty() {
        return text.to_string();
    }
    let mut out = text.replace(pem, &fingerprint(pem));
    for line in pem.lines() {
        let line = line.trim();
        if line.len() >= 8 {
            out = out.replace(line, &fingerprint(line));
        }
    }
    out
}

fn shown(output: &Output, pem: &str) -> String {
    redact(
        &format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ),
        pem,
    )
}

fn plan_text(output: &Output, pem: &str) -> String {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let value: serde_json::Value = serde_json::from_str(stdout.trim())
        .unwrap_or_else(|err| panic!("stdout is not JSON: {err}: {}", redact(&stdout, pem)));
    assert_eq!(
        value.get("dry_run"),
        Some(&serde_json::Value::Bool(true)),
        "{}",
        redact(&stdout, pem)
    );
    value
        .get("plan")
        .and_then(serde_json::Value::as_array)
        .unwrap_or_else(|| panic!("missing plan: {}", redact(&stdout, pem)))
        .iter()
        .filter_map(serde_json::Value::as_str)
        .collect::<Vec<_>>()
        .join("\n")
}

fn base_args() -> Vec<String> {
    vec![
        "cluster".to_string(),
        "github-app".to_string(),
        "--app-id".to_string(),
        APP_ID.to_string(),
        "--namespace".to_string(),
        NAMESPACE.to_string(),
        "--release".to_string(),
        RELEASE.to_string(),
        "--chart".to_string(),
        chart(),
        "--json".to_string(),
    ]
}

fn invoke(dir: &Path, path: std::ffi::OsString, args: &[String], env: &[(&str, &str)]) -> Output {
    let mut cmd = Command::new(bin());
    cmd.args(args)
        .current_dir(dir)
        .env("PATH", path)
        .env_remove("GITHUB_API_URL")
        .env_remove("CURIE_GITHUB_API_URL");
    for (key, value) in env {
        cmd.env(key, value);
    }
    cmd.output()
        .unwrap_or_else(|err| panic!("spawn curie: {err}"))
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

fn assert_pem_object(bodies: &[serde_json::Value], pem: &str) {
    let name = "example-prefix/acme-release/github-app-private-key";
    let names: Vec<&str> = bodies
        .iter()
        .filter_map(|body| body.get("Name").and_then(serde_json::Value::as_str))
        .collect();
    let matched = bodies.iter().any(|body| {
        body.get("Name").and_then(serde_json::Value::as_str) == Some(name)
            && secret_property(body, "githubAppPrivateKey").as_deref() == Some(pem)
    });
    assert!(
        matched,
        "missing {name} property githubAppPrivateKey sha256 {}; names {names:?}",
        fingerprint(pem)
    );
}

struct Rig {
    dir: tempfile::TempDir,
    _github: MockServer,
    pem_path: PathBuf,
    values_json: Option<PathBuf>,
    sequence: PathBuf,
    aws_argv: PathBuf,
    aws_body: PathBuf,
    github_url: String,
}

impl Rig {
    fn open(status: u16, body: &'static str) -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let sequence = dir.path().join("sequence.log");
        let recorded = sequence.clone();
        let github = serve(move |request: &Request| {
            if request.method == "GET" && request.path == "/app" {
                append_line(&recorded, "GET /app");
            }
            Response::json(status, body)
        });
        let github_url = github.base_url.clone();
        let bin_dir = dir.path().join("bin");
        std::fs::create_dir(&bin_dir).expect("bin dir");
        install(&bin_dir, "helm", TOOL_SHIM);
        install(&bin_dir, "kubectl", TOOL_SHIM);
        install(&bin_dir, "aws", AWS_SHIM);
        let pem_path = dir.path().join("app.pem");
        generate_rsa_pem(&pem_path);
        std::fs::write(dir.path().join("curie.yaml"), PROVIDER_YAML).expect("write curie.yaml");
        Self {
            aws_argv: dir.path().join("aws-argv.log"),
            aws_body: dir.path().join("aws-body.log"),
            pem_path,
            values_json: None,
            sequence,
            github_url,
            _github: github,
            dir,
        }
    }

    fn pem(&self) -> String {
        std::fs::read_to_string(&self.pem_path).expect("read pem")
    }

    fn run(&self, args: &[String]) -> Output {
        let pem = std::fs::read(&self.pem_path).expect("read pem");
        let document = secret_document(&pem);
        let values = r#"{"api":{"githubApiUrl":"https://api.github.com"}}"#;
        let sequence = self.sequence.display().to_string();
        let aws_argv = self.aws_argv.display().to_string();
        let aws_body = self.aws_body.display().to_string();
        let mut env = vec![
            ("CURIE_GITHUB_API_URL", self.github_url.as_str()),
            ("FAKE_VALUES", values),
            ("FAKE_SECRET_JSON", document.as_str()),
            ("SHIM_LOG", sequence.as_str()),
            ("AWS_ARGV_LOG", aws_argv.as_str()),
            ("AWS_BODY_LOG", aws_body.as_str()),
            ("ALLOW_MUTATION", "1"),
        ];
        let values_json = self
            .values_json
            .as_ref()
            .map(|path| path.display().to_string());
        if let Some(path) = values_json.as_deref() {
            env.push(("VALUES_JSON", path));
        }
        invoke(
            self.dir.path(),
            prepend_path(&self.dir.path().join("bin")),
            args,
            &env,
        )
    }
}

fn private_key_args(pem: &Path, dry_run: bool) -> Vec<String> {
    let mut args = base_args();
    args.push("--private-key".to_string());
    args.push(pem.display().to_string());
    if dry_run {
        args.push("--dry-run".to_string());
    }
    args
}

fn operator_args(dry_run: bool) -> Vec<String> {
    let mut args = base_args();
    args.extend([
        "--existing-secret".to_string(),
        "my-github-app".to_string(),
        "--existing-secret-key".to_string(),
        "app-pem".to_string(),
    ]);
    if dry_run {
        args.push("--dry-run".to_string());
    }
    args
}

fn upgrade_lines(sequence: &str) -> String {
    sequence
        .lines()
        .filter(|line| line.starts_with("helm upgrade"))
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn absent_github_app_dry_run_uses_set_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pem_path = dir.path().join("app.pem");
    generate_rsa_pem(&pem_path);
    let pem = std::fs::read_to_string(&pem_path).expect("read pem");
    let output = invoke(
        dir.path(),
        empty_path(dir.path()),
        &private_key_args(&pem_path, true),
        &[],
    );
    assert!(
        output.status.success(),
        "absent dry-run must succeed: {}",
        shown(&output, &pem)
    );
    let plan = plan_text(&output, &pem);
    assert!(plan.contains("--set-file"), "missing --set-file");
    assert!(
        plan.contains("api.githubAppPrivateKey="),
        "missing api.githubAppPrivateKey="
    );
    assert!(
        values_after(&plan, "api.githubAppExistingSecret")
            .iter()
            .all(String::is_empty),
        "absent dry-run set a non-empty api.githubAppExistingSecret"
    );
}

#[test]
fn provider_github_app_dry_run_sets_inventory_ref() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(dir.path().join("curie.yaml"), PROVIDER_YAML).expect("write curie.yaml");
    let pem_path = dir.path().join("app.pem");
    generate_rsa_pem(&pem_path);
    let pem = std::fs::read_to_string(&pem_path).expect("read pem");
    let output = invoke(
        dir.path(),
        empty_path(dir.path()),
        &private_key_args(&pem_path, true),
        &[],
    );
    assert!(
        output.status.success(),
        "provider dry-run must succeed: {}",
        shown(&output, &pem)
    );
    let plan = plan_text(&output, &pem);
    assert!(plan.contains(INVENTORY_REF), "missing {INVENTORY_REF}");
    assert!(plan.contains(INVENTORY_KEY), "missing {INVENTORY_KEY}");
    assert_cleared(&plan, "api.githubAppPrivateKey");
    assert!(
        !plan.contains("--set-file"),
        "provider dry-run kept --set-file"
    );
}

#[test]
fn provider_github_app_dry_run_keeps_explicit_existing_ref() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(dir.path().join("curie.yaml"), PROVIDER_YAML).expect("write curie.yaml");
    let output = invoke(
        dir.path(),
        empty_path(dir.path()),
        &operator_args(true),
        &[],
    );
    assert!(
        output.status.success(),
        "explicit ref dry-run must succeed: {}",
        shown(&output, "")
    );
    let plan = plan_text(&output, "");
    assert!(plan.contains(OPERATOR_REF), "missing {OPERATOR_REF}");
    assert!(
        !plan.contains(INVENTORY_REF),
        "explicit ref was replaced by the inventory name"
    );
}

#[test]
fn provider_github_app_live_probes_then_writes_then_upgrades() {
    let rig = Rig::open(200, r#"{"id":1234567}"#);
    let pem = rig.pem();
    let output = rig.run(&private_key_args(&rig.pem_path, false));
    assert!(
        output.status.success(),
        "provider github-app must exit 0: {}",
        shown(&output, &pem)
    );
    let sequence = std::fs::read_to_string(&rig.sequence).unwrap_or_default();
    let app_at = sequence
        .find("GET /app")
        .expect("GET /app was not recorded");
    let aws_at = sequence
        .find("aws create-secret")
        .expect("create-secret was not recorded");
    let helm_at = sequence
        .find("helm upgrade")
        .expect("helm upgrade was not recorded");
    assert!(
        app_at < aws_at && aws_at < helm_at,
        "order app {app_at} aws {aws_at} helm {helm_at}"
    );
    let upgrade = upgrade_lines(&sequence);
    assert!(upgrade.contains(INVENTORY_REF), "missing {INVENTORY_REF}");
    assert!(upgrade.contains(INVENTORY_KEY), "missing {INVENTORY_KEY}");
    assert!(
        !upgrade.contains("--set-file"),
        "helm upgrade kept --set-file"
    );
    let argv = std::fs::read_to_string(&rig.aws_argv).unwrap_or_default();
    assert_pem_absent(&upgrade, &pem);
    assert_pem_absent(&argv, &pem);
    assert_pem_absent(&sequence, &pem);
    assert_pem_object(&read_bodies(&rig.aws_body), &pem);
}

#[test]
fn provider_github_app_live_unauthorized_writes_nothing() {
    let rig = Rig::open(
        401,
        r#"{"message":"A JSON web token could not be decoded","status":"401"}"#,
    );
    let pem = rig.pem();
    let output = rig.run(&private_key_args(&rig.pem_path, false));
    assert!(
        !output.status.success(),
        "GET /app 401 must be non-zero: {}",
        shown(&output, &pem)
    );
    let argv = std::fs::read_to_string(&rig.aws_argv).unwrap_or_default();
    assert!(!argv.contains("create-secret"), "401 invoked create-secret");
    let body = std::fs::read_to_string(&rig.aws_body).unwrap_or_default();
    assert!(body.trim().is_empty(), "401 wrote a provider object");
    let sequence = std::fs::read_to_string(&rig.sequence).unwrap_or_default();
    assert!(
        !sequence.contains("helm upgrade"),
        "401 logged helm upgrade"
    );
}

#[test]
fn provider_github_app_live_explicit_ref_still_stores_key() {
    let rig = Rig::open(200, r#"{"id":1234567}"#);
    let pem = rig.pem();
    let output = rig.run(&operator_args(false));
    assert!(
        output.status.success(),
        "explicit ref live run must exit 0: {}",
        shown(&output, &pem)
    );
    let sequence = std::fs::read_to_string(&rig.sequence).unwrap_or_default();
    let upgrade = upgrade_lines(&sequence);
    assert!(upgrade.contains(OPERATOR_REF), "missing {OPERATOR_REF}");
    assert!(
        !upgrade.contains(INVENTORY_REF),
        "explicit ref was replaced by the inventory name"
    );
    let argv = std::fs::read_to_string(&rig.aws_argv).unwrap_or_default();
    assert_pem_absent(&upgrade, &pem);
    assert_pem_absent(&argv, &pem);
    assert_pem_object(&read_bodies(&rig.aws_body), &pem);
}

#[test]
fn provider_github_app_rotates_its_own_inventory_ref() {
    let mut rig = Rig::open(200, r#"{"id":1234567}"#);
    let values = rig.dir.path().join("values.json");
    std::fs::write(
        &values,
        r#"{"api":{"githubAppExistingSecret":"acme-release-curie-github-app","githubAppExistingSecretKey":"githubAppPrivateKey"}}"#,
    )
    .expect("write values");
    rig.values_json = Some(values);
    let pem = rig.pem();
    let output = rig.run(&private_key_args(&rig.pem_path, false));
    assert!(
        output.status.success(),
        "rotating the inventory ref must exit 0: {}",
        shown(&output, &pem)
    );
    assert_pem_object(&read_bodies(&rig.aws_body), &pem);
}

#[test]
fn provider_github_app_still_refuses_an_unrelated_existing_ref() {
    let mut rig = Rig::open(200, r#"{"id":1234567}"#);
    let values = rig.dir.path().join("values.json");
    std::fs::write(
        &values,
        r#"{"api":{"githubAppExistingSecret":"operator-owned","githubAppExistingSecretKey":"privateKey"}}"#,
    )
    .expect("write values");
    rig.values_json = Some(values);
    let pem = rig.pem();
    let output = rig.run(&private_key_args(&rig.pem_path, false));
    assert!(
        !output.status.success(),
        "an unrelated ref must be refused: {}",
        shown(&output, &pem)
    );
    let body = std::fs::read_to_string(&rig.aws_body).unwrap_or_default();
    assert!(
        body.trim().is_empty(),
        "unrelated ref wrote a provider object"
    );
}

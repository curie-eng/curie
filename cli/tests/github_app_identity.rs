//! Integration: `cluster github-app` authenticates the supplied private key
//! as the requested GitHub App before mutating the release (issue #2269).
//!
//! The SRE demo saw exit 0 and "GitHub App configured" for a syntactically
//! valid PEM that belonged to a different App; an independent `GET /app`
//! probe with the same pair returned 401. Shape checks in
//! `require_connect_inputs` cannot see that: they never sign a JWT and never
//! talk to GitHub. These tests drive the built binary on a real (non
//! `--dry-run`) invocation against a fake helm/kubectl and a local HTTP
//! stand-in of GitHub's documented App API
//! (https://docs.github.com/en/rest/apps/apps#get-the-authenticated-app).
//!
//! `--dry-run` stays offline (`cli/CLAUDE.md`): it is deliberately not used
//! here. The identity probe is the call site under test, and it must run
//! before `helm upgrade` so a 401 cannot replace the last known-good
//! credential mode.
//!
//! No live GitHub and no cluster. The PEM is generated at runtime with
//! openssl so this file never carries key material.

#![cfg(unix)]

mod support;

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use support::{serve, MockServer, Request, Response};

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

/// GitHub's documented App JWT probe. Cited so a future reader does not
/// treat the path as an invention of this test.
const APP_PATH: &str = "/app";

const MATCHING_APP_ID: &str = "1234567";
const OTHER_APP_ID: &str = "7654321";

/// Logs every helm/kubectl invocation. Models the deployed revision and
/// sandbox pair required by next's reconciliation barrier, as exercised by
/// `github_app_sandbox_reconciliation`. Only an allowed Helm upgrade advances
/// history; unrelated commands fail closed.
const TOOL_SHIM: &str = r#"#!/usr/bin/env python3
import json
import os
import sys
from pathlib import Path

tool = Path(sys.argv[0]).name
args = sys.argv[1:]
log = Path(os.environ["SHIM_LOG"])
with log.open("a") as stream:
    stream.write(tool + " " + " ".join(args) + "\n")
upgraded = log.with_suffix(".upgraded")
revision = 13 if upgraded.exists() else 12

def emit(value):
    print(json.dumps(value))
    raise SystemExit(0)

def sandbox(kind, name, spec):
    return {
        "apiVersion": "extensions.agents.x-k8s.io/v1beta1",
        "kind": kind,
        "metadata": {
            "name": name,
            "labels": {
                "app.kubernetes.io/component": "agent-sandbox",
                "app.kubernetes.io/instance": "curie",
                "app.kubernetes.io/managed-by": "Helm",
            },
            "annotations": {
                "meta.helm.sh/release-name": "curie",
                "meta.helm.sh/release-namespace": "curie",
            },
        },
        "spec": spec,
    }

objects = [
    sandbox("SandboxTemplate", "curie-runner", {"service": True}),
    sandbox("SandboxWarmPool", "curie-runner-pool", {
        "replicas": 0, "sandboxTemplateRef": {"name": "curie-runner"}
    }),
]
if tool == "helm":
    if args == ["history", "curie", "-n", "curie", "-o", "json", "--max", "256"]:
        rows = [{"revision": 12, "status": "deployed", "chart": "curie-0.8.6", "description": "Upgrade complete"}]
        if upgraded.exists():
            rows[0]["status"] = "superseded"
            rows.append({"revision": 13, "status": "deployed", "chart": "curie-0.8.6", "description": "Upgrade complete"})
        emit(rows)
    if args == ["get", "values", "curie", "-n", "curie", "--revision", str(revision), "-o", "json"]:
        print(os.environ["FAKE_VALUES"])
        raise SystemExit(0)
    if args == ["get", "manifest", "curie", "-n", "curie", "--revision", str(revision)]:
        print("\n---\n".join(json.dumps(obj) for obj in objects))
        raise SystemExit(0)
    upgrade_prefix = ["upgrade", "curie", "charts/curie", "-n", "curie", "--reuse-values"]
    connect_tail = ["--set-string", "api.githubAppId=1234567", "--set-file",
                    "api.githubAppPrivateKey=" + str(log.parent / "app.pem"),
                    "--set", "api.githubCloneBase=https://github.com"]
    disconnect_tail = ["--set", "api.githubAppId=", "--set", "api.githubAppPrivateKey=",
                       "--set", "api.githubAppExistingSecret="]
    if os.environ.get("ALLOW_MUTATION") == "1" and args in [
        upgrade_prefix + connect_tail, upgrade_prefix + disconnect_tail
    ]:
        upgraded.touch()
        raise SystemExit(0)
if tool == "kubectl":
    if args == ["-n", "curie", "get", "secret", "my-github-app", "-o", "json"]:
        print(os.environ["FAKE_SECRET_JSON"])
        raise SystemExit(0)
    if args == ["get", "sandboxtemplates.extensions.agents.x-k8s.io,sandboxwarmpools.extensions.agents.x-k8s.io", "-n", "curie", "-o", "json", "--request-timeout=5s"]:
        emit({"apiVersion": "v1", "items": objects})
    for kind, component in [("svc", "api"), ("deployment", "worker")]:
        if args == ["-n", "curie", "get", kind, "-l",
                    "app.kubernetes.io/instance=curie,app.kubernetes.io/component=" + component,
                    "-o", r'jsonpath={range .items[*]}{.metadata.name}{"\n"}{end}']:
            print("curie-" + component)
            raise SystemExit(0)
    if os.environ.get("ALLOW_MUTATION") == "1" and upgraded.exists() and args in [
        ["-n", "curie", "rollout", "restart", "deployment/curie-api"],
        ["-n", "curie", "rollout", "status", "deployment/curie-api", "--timeout=180s"],
    ]:
        raise SystemExit(0)
print("shim: refusing to execute: " + tool + " " + " ".join(args), file=sys.stderr)
raise SystemExit(1)
"#;

fn generate_rsa_pem(path: &Path) {
    let output = Command::new("openssl")
        .args(["genrsa", "2048"])
        .output()
        .unwrap_or_else(|e| panic!("openssl genrsa: {e}"));
    assert!(
        output.status.success(),
        "openssl genrsa failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    std::fs::write(path, output.stdout).expect("write generated PEM");
}

fn b64(bytes: &[u8]) -> String {
    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, bytes)
}

fn jwt_payload(authorization: &str) -> serde_json::Value {
    let token = authorization
        .strip_prefix("Bearer ")
        .unwrap_or_else(|| panic!("Authorization is not a Bearer token: {authorization}"));
    let payload = token
        .split('.')
        .nth(1)
        .unwrap_or_else(|| panic!("JWT has no payload: {token}"));
    let decoded =
        base64::Engine::decode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, payload)
            .or_else(|_| {
                base64::Engine::decode(&base64::engine::general_purpose::URL_SAFE, payload)
            })
            .unwrap_or_else(|e| panic!("JWT payload is not base64url: {e}"));
    serde_json::from_slice(&decoded).unwrap_or_else(|e| panic!("JWT payload is not JSON: {e}"))
}

struct Probe {
    dir: tempfile::TempDir,
    github: MockServer,
}

impl Probe {
    fn matching() -> Self {
        Self::with_github(|_req: &Request| {
            Response::json(
                200,
                &format!(r#"{{"id":{MATCHING_APP_ID},"name":"acme-bot"}}"#),
            )
        })
    }

    fn unauthorized() -> Self {
        Self::with_github(|_req: &Request| {
            Response::json(
                401,
                r#"{"message":"A JSON web token could not be decoded","status":"401"}"#,
            )
        })
    }

    fn mismatched_id() -> Self {
        Self::with_github(|_req: &Request| {
            Response::json(
                200,
                &format!(r#"{{"id":{OTHER_APP_ID},"name":"acme-other"}}"#),
            )
        })
    }

    fn with_github(handler: impl Fn(&Request) -> Response + Send + Sync + 'static) -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let shim_dir = dir.path().join("bin");
        std::fs::create_dir(&shim_dir).expect("create shim dir");
        for tool in ["helm", "kubectl"] {
            let path = shim_dir.join(tool);
            std::fs::write(&path, TOOL_SHIM).expect("write shim");
            let mut perms = std::fs::metadata(&path).expect("stat shim").permissions();
            perms.set_mode(0o755);
            std::fs::set_permissions(&path, perms).expect("chmod shim");
        }
        generate_rsa_pem(&dir.path().join("app.pem"));
        let github = serve(handler);
        Self { dir, github }
    }

    fn private_key(&self) -> String {
        self.dir
            .path()
            .join("app.pem")
            .to_string_lossy()
            .into_owned()
    }

    fn secret_json(&self) -> String {
        let pem = std::fs::read(self.dir.path().join("app.pem")).expect("read pem");
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": "my-github-app"},
            "data": {"app-pem": b64(&pem)}
        })
        .to_string()
    }

    fn log_path(&self) -> PathBuf {
        self.dir.path().join("invocations.log")
    }

    fn invocations(&self) -> Vec<String> {
        match std::fs::read_to_string(self.log_path()) {
            Ok(body) => body.lines().map(str::to_string).collect(),
            Err(_) => Vec::new(),
        }
    }

    fn run(&self, argv: &[&str], allow_mutation: bool) -> Output {
        let mut dirs = vec![self.dir.path().join("bin")];
        if let Some(existing) = std::env::var_os("PATH") {
            dirs.extend(std::env::split_paths(&existing));
        }
        let path = std::env::join_paths(dirs).expect("join PATH");
        let mut cmd = Command::new(bin());
        cmd.args(argv)
            .env("PATH", path)
            .env("CURIE_GITHUB_API_URL", &self.github.base_url)
            .env(
                "FAKE_VALUES",
                r#"{"api":{"githubApiUrl":"https://api.github.com"}}"#,
            )
            .env("FAKE_SECRET_JSON", self.secret_json())
            .env("SHIM_LOG", self.log_path());
        if allow_mutation {
            cmd.env("ALLOW_MUTATION", "1");
        }
        cmd.output()
            .unwrap_or_else(|e| panic!("run curie {}: {e}", argv.join(" ")))
    }
}

fn combined(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn stdout_json(output: &Output) -> serde_json::Value {
    let stdout = String::from_utf8_lossy(&output.stdout);
    serde_json::from_str(stdout.trim())
        .unwrap_or_else(|e| panic!("stdout must be one JSON object: {e}; stdout: {stdout}"))
}

fn text_at(value: &serde_json::Value, key: &str) -> String {
    value
        .get(key)
        .and_then(|v| v.as_str())
        .unwrap_or_else(|| panic!("the payload must carry `{key}`: {value}"))
        .to_string()
}

fn connect_argv<'a>(app_id: &'a str, key: &'a str) -> Vec<&'a str> {
    vec![
        "cluster",
        "github-app",
        "--app-id",
        app_id,
        "--private-key",
        key,
        "--chart",
        "charts/curie",
        "--json",
    ]
}

fn byo_argv(app_id: &str) -> Vec<&str> {
    vec![
        "cluster",
        "github-app",
        "--app-id",
        app_id,
        "--existing-secret",
        "my-github-app",
        "--existing-secret-key",
        "app-pem",
        "--chart",
        "charts/curie",
        "--json",
    ]
}

fn assert_probed_matching_app(probe: &Probe) {
    let recorded = probe.github.recorded();
    let app = recorded
        .iter()
        .find(|r| r.method == "GET" && r.path == APP_PATH)
        .unwrap_or_else(|| panic!("CLI never called GET {APP_PATH}: {recorded:?}"));
    let auth = app
        .header("authorization")
        .unwrap_or_else(|| panic!("GET {APP_PATH} had no Authorization header"));
    let claims = jwt_payload(auth);
    assert_eq!(
        claims.get("iss").and_then(|v| v.as_str()),
        Some(MATCHING_APP_ID),
        "JWT iss must be the requested App id: {claims}"
    );
}

#[test]
fn a_matching_key_configures_the_app_after_github_confirms_identity() {
    // Positive AC: a key that authenticates as --app-id is allowed to mutate
    // the release and report success. GitHub's GET /app is the oracle, not
    // the PEM shape check.
    let probe = Probe::matching();
    let key = probe.private_key();
    let output = probe.run(&connect_argv(MATCHING_APP_ID, &key), true);
    assert!(
        output.status.success(),
        "matching App identity must exit 0; output: {}",
        combined(&output)
    );
    let value = stdout_json(&output);
    assert_eq!(
        value.get("github_app_configured"),
        Some(&serde_json::json!(true)),
        "matching identity must report configured: {value}"
    );
    assert_probed_matching_app(&probe);
    let log = probe.invocations();
    assert!(
        log.iter().any(|line| line.starts_with("helm upgrade")),
        "a confirmed identity must still run the helm upgrade: {log:?}"
    );
}

#[test]
fn a_mismatched_private_key_is_refused_before_helm_upgrade() {
    // THIS IS THE TICKET. GitHub 401 on GET /app means the PEM does not
    // authenticate as --app-id. Before #2269 the CLI never asked, helm
    // upgraded, and the operator was told the App was configured.
    let probe = Probe::unauthorized();
    let key = probe.private_key();
    let output = probe.run(&connect_argv(MATCHING_APP_ID, &key), true);
    assert_eq!(
        output.status.code(),
        Some(1),
        "a GitHub 401 must exit 1 (Failure); output: {}",
        combined(&output)
    );
    let value = stdout_json(&output);
    let error = text_at(&value, "error");
    assert!(
        error.contains("401") || error.to_lowercase().contains("does not authenticate"),
        "the refusal must say GitHub rejected the key: {error}"
    );
    let fix = text_at(&value, "fix");
    assert!(
        !fix.is_empty(),
        "the refusal must carry a non-null fix: {value}"
    );
    assert!(
        value.get("github_app_configured").is_none(),
        "the refused run reported the App as configured: {value}"
    );
    let recorded = probe.github.recorded();
    assert!(
        recorded
            .iter()
            .any(|r| r.method == "GET" && r.path == APP_PATH),
        "the refusal must come from GET {APP_PATH}, not a skipped probe: {recorded:?}"
    );
    let log = probe.invocations();
    assert!(
        log.iter().all(|line| !line.starts_with("helm upgrade")),
        "a failed identity probe must not replace last known-good credentials: {log:?}"
    );
    assert!(
        log.iter().all(|line| !line.contains("rollout restart")),
        "a failed identity probe must not roll the API onto the rejected key: {log:?}"
    );
}

#[test]
fn a_github_app_id_mismatch_is_refused_before_helm_upgrade() {
    // Defense in depth on the same AC: GET /app 200 with a different `id`
    // than --app-id is still a false success if we only check HTTP status.
    let probe = Probe::mismatched_id();
    let key = probe.private_key();
    let output = probe.run(&connect_argv(MATCHING_APP_ID, &key), true);
    assert_eq!(
        output.status.code(),
        Some(1),
        "an authenticated-but-wrong App id must exit 1; output: {}",
        combined(&output)
    );
    let value = stdout_json(&output);
    let error = text_at(&value, "error");
    assert!(
        error.contains(OTHER_APP_ID) || error.contains(MATCHING_APP_ID),
        "the refusal must name the identity mismatch: {error}"
    );
    assert!(
        value.get("github_app_configured").is_none(),
        "the refused run reported the App as configured: {value}"
    );
    let log = probe.invocations();
    assert!(
        log.iter().all(|line| !line.starts_with("helm upgrade")),
        "an id mismatch must not replace last known-good credentials: {log:?}"
    );
}

#[test]
fn an_existing_secret_mismatched_key_is_refused_before_helm_upgrade() {
    // The SRE demo used --existing-secret. The CLI must read the Secret,
    // probe GitHub, and refuse before helm upgrade — not skip the probe
    // because it never had a --private-key path.
    let probe = Probe::unauthorized();
    let output = probe.run(&byo_argv(MATCHING_APP_ID), true);
    assert_eq!(
        output.status.code(),
        Some(1),
        "a BYO Secret whose PEM 401s must exit 1; output: {}",
        combined(&output)
    );
    let value = stdout_json(&output);
    assert!(
        value.get("github_app_configured").is_none(),
        "the refused BYO run reported the App as configured: {value}"
    );
    let log = probe.invocations();
    assert!(
        log.iter().any(|line| line.contains("get secret")),
        "the BYO path must read the Secret in order to probe it: {log:?}"
    );
    assert!(
        log.iter().all(|line| !line.starts_with("helm upgrade")),
        "a failed BYO identity probe must not replace last known-good credentials: {log:?}"
    );
}

#[test]
fn dry_run_does_not_call_github() {
    // `--dry-run` never touches the network (`cli/CLAUDE.md`). The identity
    // probe is a GitHub call, so it must not run on a dry-run even when a
    // mock is listening.
    let probe = Probe::unauthorized();
    let key = probe.private_key();
    let argv = [
        "cluster",
        "github-app",
        "--app-id",
        MATCHING_APP_ID,
        "--private-key",
        key.as_str(),
        "--chart",
        "charts/curie",
        "--dry-run",
        "--json",
    ];
    let output = probe.run(&argv, false);
    assert!(
        output.status.success(),
        "dry-run must stay offline and succeed; output: {}",
        combined(&output)
    );
    assert!(
        probe.github.recorded().is_empty(),
        "dry-run called GitHub: {:?}",
        probe.github.recorded()
    );
}

#[test]
fn disconnect_does_not_call_github() {
    let probe = Probe::unauthorized();
    let output = probe.run(
        &[
            "cluster",
            "github-app",
            "--disconnect",
            "--chart",
            "charts/curie",
            "--json",
        ],
        true,
    );
    assert!(
        probe.github.recorded().is_empty(),
        "disconnect called GitHub: {:?}",
        probe.github.recorded()
    );
    let log = probe.invocations();
    assert!(
        log.iter().any(|line| line.starts_with("helm upgrade")),
        "disconnect must still clear the release: {log:?}; output: {}",
        combined(&output)
    );
}

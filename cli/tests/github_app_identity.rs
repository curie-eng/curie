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

/// Logs every helm/kubectl invocation. Answers only the reads this verb
/// needs before it mutates (values, Secret, fullname discovery) plus, when
/// `ALLOW_MUTATION=1`, the upgrade and rollout. Everything else is refused
/// so a test cannot pass by running some other command that happened to
/// succeed.
const TOOL_SHIM: &str = r#"#!/usr/bin/env bash
tool=$(basename "$0")
echo "$tool $*" >> "$SHIM_LOG"
if [ "$tool" = "helm" ] && [ "$1" = "get" ] && [ "$2" = "values" ]; then
  echo "$FAKE_VALUES"
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
  echo '{"items":[{"apiVersion":"extensions.agents.x-k8s.io/v1beta1","kind":"SandboxTemplate","metadata":{"name":"curie-runner","labels":{"app.kubernetes.io/component":"agent-sandbox","app.kubernetes.io/instance":"curie","app.kubernetes.io/managed-by":"Helm"},"annotations":{"meta.helm.sh/release-name":"curie","meta.helm.sh/release-namespace":"curie"}},"spec":{"service":true}},{"apiVersion":"extensions.agents.x-k8s.io/v1beta1","kind":"SandboxWarmPool","metadata":{"name":"curie-runner-pool","labels":{"app.kubernetes.io/component":"agent-sandbox","app.kubernetes.io/instance":"curie","app.kubernetes.io/managed-by":"Helm"},"annotations":{"meta.helm.sh/release-name":"curie","meta.helm.sh/release-namespace":"curie"}},"spec":{"replicas":0,"sandboxTemplateRef":{"name":"curie-runner"}}}]}'
  exit 0
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

/// Derive the public half of a generated key, so a signature can be checked
/// against something other than the library that produced it.
fn rsa_public_pem(private_key: &Path, out: &Path) {
    let output = Command::new("openssl")
        .args(["rsa", "-pubout", "-in"])
        .arg(private_key)
        .arg("-out")
        .arg(out)
        .output()
        .unwrap_or_else(|e| panic!("openssl rsa -pubout: {e}"));
    assert!(
        output.status.success(),
        "openssl rsa -pubout failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// Verify the JWT's RS256 signature with openssl rather than with
/// `jsonwebtoken`.
///
/// Decoding the claims (`jwt_payload`) proves only that the CLI emitted three
/// dot-separated segments with the right `iss`; a backend that produced a
/// malformed, truncated, or wrongly-padded signature would satisfy every
/// claim assertion in this file and be rejected only by GitHub, in
/// production. That mattered when #2381 moved the RS256 implementation from
/// the pure-Rust `rsa` crate to `aws-lc-rs`: the swap is exactly the kind
/// that a claims-only test cannot see. openssl is used deliberately because
/// verifying an `aws-lc-rs` signature with `aws-lc-rs` would be circular.
fn assert_rs256_signature_verifies(authorization: &str, private_key: &Path, dir: &Path) {
    let token = authorization
        .strip_prefix("Bearer ")
        .unwrap_or_else(|| panic!("Authorization is not a Bearer token: {authorization}"));
    let parts: Vec<&str> = token.split('.').collect();
    assert_eq!(
        parts.len(),
        3,
        "a JWS compact serialization has exactly three segments: {token}"
    );
    let signature =
        base64::Engine::decode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, parts[2])
            .unwrap_or_else(|e| panic!("JWT signature is not base64url: {e}"));
    assert!(
        !signature.is_empty(),
        "JWT carries an empty signature: {token}"
    );

    let public_key = dir.join("app.pub.pem");
    rsa_public_pem(private_key, &public_key);
    let signing_input = dir.join("jwt.signing-input");
    std::fs::write(&signing_input, format!("{}.{}", parts[0], parts[1]))
        .expect("write JWT signing input");
    let signature_path = dir.join("jwt.sig");
    std::fs::write(&signature_path, &signature).expect("write JWT signature");

    let output = Command::new("openssl")
        .args(["dgst", "-sha256", "-verify"])
        .arg(&public_key)
        .arg("-signature")
        .arg(&signature_path)
        .arg(&signing_input)
        .output()
        .unwrap_or_else(|e| panic!("openssl dgst -verify: {e}"));
    assert!(
        output.status.success(),
        "the App JWT's RS256 signature does not verify against its own key: {} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
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
    assert_rs256_signature_verifies(auth, &PathBuf::from(probe.private_key()), probe.dir.path());
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

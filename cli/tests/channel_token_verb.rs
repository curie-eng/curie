//! Integration: `cluster channel-token` clap wiring, mint, and --show-exp.
//!
//! Drives the built binary so a flag that clap parses but main.rs drops is a
//! failure, the same gap `github_app_clap_wiring.rs` and `surfaces_verb.rs`
//! exist for. Mint tests stub helm/kubectl on PATH and mock POST /channels/token
//! so the token never leaves this process; assertions are that stdout/stderr
//! never contain it and that kubectl is given `--patch-file`, not the value.

mod support;

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::process::Command;

use base64::Engine;
use support::{serve, MockServer, Response};

const AGENT_ID: &str = "44444444-4444-4444-4444-444444444444";
const AGENT_NAME: &str = "acme-bot";
const INBOX: &str = "ops@example.com";
const EXP: i64 = 1_800_000_000;

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn sample_token(exp: i64) -> String {
    let payload = format!(
        r#"{{"channel_id":"{AGENT_ID}","exp":{exp},"generation":0,"scope":"channel.enqueue"}}"#
    );
    let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload.as_bytes());
    format!("chn.{b64}.TESTSIG")
}

fn agent_json() -> String {
    format!(
        r#"{{"id":"{AGENT_ID}","name":"{AGENT_NAME}","channels":[{{"kind":"email","address":"{INBOX}"}}],"created_at":"2026-07-05T00:00:00Z","memory":false}}"#
    )
}

struct Run {
    code: i32,
    stdout: String,
    stderr: String,
}

fn run(argv: &[&str]) -> Run {
    let output = Command::new(bin())
        .args(argv)
        .env_remove("CURIE_API_URL")
        .env_remove("CURIE_API_KEY")
        .output()
        .unwrap_or_else(|e| panic!("run curie {}: {e}", argv.join(" ")));
    Run {
        code: output.status.code().unwrap_or(-1),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    }
}

fn dry_run_plan(argv: &[&str]) -> (i32, serde_json::Value) {
    let run = run(argv);
    let value: serde_json::Value = serde_json::from_str(run.stdout.trim()).unwrap_or_else(|e| {
        panic!(
            "stdout must be JSON: {e}; stdout: {}; stderr: {}",
            run.stdout, run.stderr
        )
    });
    (run.code, value)
}

#[test]
fn dry_run_plan_names_the_mint_and_never_prints_a_token() {
    let token = sample_token(EXP);
    let (code, value) = dry_run_plan(&[
        "--json",
        "cluster",
        "channel-token",
        AGENT_NAME,
        "--kind",
        "email",
        "--address",
        INBOX,
        "--namespace",
        "mail-test",
        "--release",
        "acme",
        "--dry-run",
    ]);
    assert_eq!(code, 0, "{value}");
    assert_eq!(value["dry_run"], true);
    let plan = value["plan"]
        .as_array()
        .expect("plan")
        .iter()
        .filter_map(|l| l.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(plan.contains("/channels/token"), "{plan}");
    assert!(plan.contains("ttl_s\":604800"), "{plan}");
    assert!(plan.contains("--patch-file"), "{plan}");
    assert!(plan.contains("rollout restart"), "{plan}");
    assert!(!plan.contains(&token), "{plan}");
    assert!(!plan.contains("chn."), "{plan}");
}

#[test]
fn dry_run_honors_ttl_flag() {
    let (code, value) = dry_run_plan(&[
        "--json",
        "cluster",
        "channel-token",
        AGENT_NAME,
        "--kind",
        "email",
        "--address",
        INBOX,
        "--ttl",
        "1h",
        "--dry-run",
    ]);
    assert_eq!(code, 0, "{value}");
    let plan = value["plan"].as_array().expect("plan")[0].as_str().unwrap();
    assert!(plan.contains("ttl_s\":3600"), "{plan}");
}

#[test]
fn missing_kind_is_usage() {
    let run = run(&[
        "--json",
        "cluster",
        "channel-token",
        AGENT_NAME,
        "--address",
        INBOX,
        "--dry-run",
    ]);
    assert_eq!(run.code, 2, "{} {}", run.stdout, run.stderr);
    let value: serde_json::Value = serde_json::from_str(run.stdout.trim()).unwrap();
    assert!(
        value["error"]
            .as_str()
            .unwrap()
            .contains("--kind and --address"),
        "{value}"
    );
}

#[test]
fn show_exp_dry_run_does_not_mint() {
    let (code, value) = dry_run_plan(&[
        "--json",
        "cluster",
        "channel-token",
        AGENT_NAME,
        "--show-exp",
        "--dry-run",
    ]);
    assert_eq!(code, 0, "{value}");
    let plan = value["plan"]
        .as_array()
        .expect("plan")
        .iter()
        .filter_map(|l| l.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(plan.contains("statusz"), "{plan}");
    assert!(!plan.contains("POST"), "{plan}");
    assert!(!plan.contains("patch"), "{plan}");
}

struct ClusterStub(tempfile::TempDir);

impl ClusterStub {
    fn new(values: serde_json::Value) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        fs::write(root.join("values.json"), values.to_string()).unwrap();
        fs::write(
            root.join("status.json"),
            serde_json::json!({
                "status": "ready",
                "channel_token": {"present": true, "exp": EXP, "state": "ok"},
                "last_ingress_status": null,
            })
            .to_string(),
        )
        .unwrap();
        fs::write(
            root.join("pods.json"),
            serde_json::json!({"items":[{
                "metadata": {"name": "acme-mail-abc", "labels": {
                    "app.kubernetes.io/component": "mail-adapter",
                    "app.kubernetes.io/instance": "acme"
                }}
            }]})
            .to_string(),
        )
        .unwrap();
        // `fail.<step>` sentinel files let a test make one stubbed step exit
        // nonzero. The stderr text matters: `helm` must not say "release: not
        // found" (which the CLI reads as an absent release, not a failure) and
        // must carry no connectivity marker, so it classifies as a plain
        // failure rather than a transient.
        let script = r#"#!/bin/sh
log="${0%/*}/calls.log"
printf '%s %s\n' "${0##*/}" "$*" >> "$log"
fail() { [ -f "${0%/*}/fail.$1" ]; }
case "${0##*/}:$*" in
  helm:"get values"*)
    if fail helm; then
      echo "Error: query: failed to query with labels: secrets is forbidden" >&2
      exit 1
    fi
    cat "${0%/*}/values.json"; exit 0 ;;
  kubectl:*patch*)
    if fail patch; then
      echo 'Error from server (Forbidden): secrets "acme-curie" is forbidden' >&2
      exit 1
    fi
    patch=
    prev=
    for arg in "$@"; do
      if [ "$prev" = "--patch-file" ]; then patch=$arg; fi
      prev=$arg
    done
    if [ -n "$patch" ]; then cp "$patch" "${0%/*}/patched.json"; fi
    exit 0 ;;
  kubectl:*rollout*)
    if fail rollout; then
      echo 'error: no deployments found with label selector' >&2
      exit 1
    fi
    exit 0 ;;
  kubectl:*--raw*)
    cat "${0%/*}/status.json"; exit 0 ;;
  kubectl:"get pods"*)
    cat "${0%/*}/pods.json"; exit 0 ;;
esac
exit 0
"#;
        for name in ["kubectl", "helm"] {
            let path = root.join(name);
            fs::write(&path, script).unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
        }
        Self(temp)
    }

    /// Make the named stubbed step (`helm`, `patch`, `rollout`) exit nonzero.
    fn failing(self, step: &str) -> Self {
        fs::write(self.0.path().join(format!("fail.{step}")), "").unwrap();
        self
    }

    fn run(&self, argv: &[&str]) -> Run {
        let path = format!("{}:/usr/bin:/bin", self.0.path().display());
        let output = Command::new(bin())
            .args(argv)
            .env("PATH", path)
            .env_remove("CURIE_API_URL")
            .env_remove("CURIE_API_KEY")
            .output()
            .unwrap();
        Run {
            code: output.status.code().unwrap_or(-1),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        }
    }

    fn patched(&self) -> String {
        fs::read_to_string(self.0.path().join("patched.json")).unwrap_or_default()
    }

    fn calls(&self) -> String {
        fs::read_to_string(self.0.path().join("calls.log")).unwrap_or_default()
    }
}

fn api_for_mint(token: &str) -> MockServer {
    let token = token.to_string();
    let agent = agent_json();
    serve(move |req| {
        let (m, p) = (req.method.as_str(), req.path.as_str());
        match (m, p) {
            ("GET", "/agents") => Response::json(200, &format!("[{agent}]")),
            ("GET", p) if p == format!("/agents/{AGENT_ID}") => Response::json(200, &agent),
            ("POST", "/channels/token") => {
                Response::json(200, &format!(r#"{{"token":"{token}"}}"#))
            }
            _ => Response::json(404, r#"{"detail":"not found"}"#),
        }
    })
}

#[test]
fn mint_writes_the_secret_via_patch_file_and_never_prints_the_token() {
    let token = sample_token(EXP);
    let server = api_for_mint(&token);
    let stub = ClusterStub::new(serde_json::json!({
        "mailAdapter": {"deploy": true}
    }));
    let run = stub.run(&[
        "--json",
        "cluster",
        "channel-token",
        AGENT_NAME,
        "--kind",
        "email",
        "--address",
        INBOX,
        "--namespace",
        "mail-test",
        "--release",
        "acme",
        "--api-url",
        &server.base_url,
        "--api-key",
        "test-key",
    ]);
    assert_eq!(run.code, 0, "{} {}", run.stdout, run.stderr);
    let value: serde_json::Value = serde_json::from_str(run.stdout.trim()).unwrap();
    assert_eq!(value["exp"], EXP, "{value}");
    assert_eq!(value["kind"], "email");
    assert_eq!(value["secret"]["key"], "mailChannelToken");
    assert!(value.get("token").is_none(), "{value}");
    assert!(!run.stdout.contains(&token), "{}", run.stdout);
    assert!(!run.stderr.contains(&token), "{}", run.stderr);
    let patched = stub.patched();
    assert!(patched.contains(&token), "{patched}");
    let calls = stub.calls();
    assert!(calls.contains("patch secret"), "{calls}");
    assert!(calls.contains("--patch-file"), "{calls}");
    assert!(calls.contains("rollout restart"), "{calls}");
    assert!(
        !calls.contains(&token),
        "token must not appear in kubectl/helm argv: {calls}"
    );
    let traffic = server
        .recorded()
        .iter()
        .map(|r| format!("{} {}", r.method, r.path))
        .collect::<Vec<_>>();
    assert!(
        traffic.iter().any(|t| t == "POST /channels/token"),
        "{traffic:?}"
    );
}

#[test]
fn mint_targets_the_existing_secret_when_configured() {
    let token = sample_token(EXP);
    let server = api_for_mint(&token);
    let stub = ClusterStub::new(serde_json::json!({
        "mailAdapter": {
            "deploy": true,
            "channelTokenExistingSecret": "curie-mail-credentials",
            "channelTokenExistingSecretKey": "channel-token"
        }
    }));
    let run = stub.run(&[
        "--json",
        "cluster",
        "channel-token",
        AGENT_NAME,
        "--kind",
        "email",
        "--address",
        INBOX,
        "--namespace",
        "mail-test",
        "--release",
        "acme",
        "--api-url",
        &server.base_url,
        "--api-key",
        "test-key",
    ]);
    assert_eq!(run.code, 0, "{} {}", run.stdout, run.stderr);
    let value: serde_json::Value = serde_json::from_str(run.stdout.trim()).unwrap();
    assert_eq!(value["secret"]["name"], "curie-mail-credentials");
    assert_eq!(value["secret"]["key"], "channel-token");
    assert!(!run.stdout.contains(&token));
    let calls = stub.calls();
    assert!(
        calls.contains("patch secret curie-mail-credentials"),
        "{calls}"
    );
}

#[test]
fn show_exp_does_not_post_a_token() {
    let token = sample_token(EXP);
    let server = api_for_mint(&token);
    let stub = ClusterStub::new(serde_json::json!({"mailAdapter": {"deploy": true}}));
    let run = stub.run(&[
        "--json",
        "cluster",
        "channel-token",
        AGENT_NAME,
        "--show-exp",
        "--namespace",
        "mail-test",
        "--release",
        "acme",
        "--api-url",
        &server.base_url,
        "--api-key",
        "test-key",
    ]);
    assert_eq!(run.code, 0, "{} {}", run.stdout, run.stderr);
    let value: serde_json::Value = serde_json::from_str(run.stdout.trim()).unwrap();
    assert_eq!(value["exp"], EXP, "{value}");
    assert_eq!(value["accepted"], true);
    assert_eq!(value["state"], "ok");
    assert!(!run.stdout.contains(&token));
    let traffic = server
        .recorded()
        .iter()
        .map(|r| format!("{} {}", r.method, r.path))
        .collect::<Vec<_>>();
    assert!(
        !traffic.iter().any(|t| t.contains("/channels/token")),
        "show-exp must not mint: {traffic:?}"
    );
    assert!(stub.patched().is_empty(), "show-exp must not patch");
}

// --- #2553: the mint is a rotation write, so ordering is the whole fix -------
//
// `POST /channels/token` bumps the binding's generation and signs the new one,
// which revokes the token the adapter is currently running. There is no
// un-mint. So every step that can fail for a reason unrelated to the token has
// to run BEFORE the mint, and the one step that cannot (the Secret write, which
// needs the token) has to say what it left behind when it fails.

fn mint_argv(base_url: &str) -> Vec<&str> {
    vec![
        "--json",
        "cluster",
        "channel-token",
        AGENT_NAME,
        "--kind",
        "email",
        "--address",
        INBOX,
        "--namespace",
        "mail-test",
        "--release",
        "acme",
        "--api-url",
        base_url,
        "--api-key",
        "test-key",
    ]
}

/// The ordering pin. Helm discovery fails, so the verb must abort with the
/// binding's generation untouched -- no mint request may reach the API at all.
/// Against the pre-fix tree the mint runs first and this fails on the recorded
/// POST: a working token was revoked to pay for a `helm` error.
#[test]
fn a_discovery_failure_burns_no_generation() {
    let token = sample_token(EXP);
    let server = api_for_mint(&token);
    let stub =
        ClusterStub::new(serde_json::json!({"mailAdapter": {"deploy": true}})).failing("helm");
    let run = stub.run(&mint_argv(&server.base_url));
    assert_ne!(run.code, 0, "{} {}", run.stdout, run.stderr);
    let traffic = server
        .recorded()
        .iter()
        .map(|r| format!("{} {}", r.method, r.path))
        .collect::<Vec<_>>();
    assert!(
        !traffic.iter().any(|t| t.contains("/channels/token")),
        "a failure the verb could have hit before minting must not revoke the \
         installed token: {traffic:?}"
    );
    assert!(stub.patched().is_empty(), "nothing may be written");
}

/// The complement: on the happy path the discovery calls still happen, and they
/// happen before the mint. Without this a "fix" that simply dropped the Helm
/// read would satisfy the test above while silently losing
/// `channelTokenExistingSecret` targeting.
#[test]
fn discovery_runs_before_the_mint_on_the_happy_path() {
    let token = sample_token(EXP);
    let server = api_for_mint(&token);
    let stub = ClusterStub::new(serde_json::json!({"mailAdapter": {"deploy": true}}));
    let run = stub.run(&mint_argv(&server.base_url));
    assert_eq!(run.code, 0, "{} {}", run.stdout, run.stderr);
    let calls = stub.calls();
    let helm_at = calls
        .find("helm get values")
        .unwrap_or_else(|| panic!("helm read: {calls}"));
    let patch_at = calls
        .find("patch secret")
        .unwrap_or_else(|| panic!("patch: {calls}"));
    assert!(helm_at < patch_at, "{calls}");
}

/// The window that cannot be closed by ordering: the write needs the token, so
/// a failed write leaves a revoked token installed. The operator must be told
/// that in the message, not left reading a `kubectl` error about a Secret.
#[test]
fn a_failed_secret_write_reports_the_revoked_token() {
    let token = sample_token(EXP);
    let server = api_for_mint(&token);
    let stub =
        ClusterStub::new(serde_json::json!({"mailAdapter": {"deploy": true}})).failing("patch");
    let run = stub.run(&mint_argv(&server.base_url));
    assert_ne!(run.code, 0, "{} {}", run.stdout, run.stderr);
    let value: serde_json::Value = serde_json::from_str(run.stdout.trim())
        .unwrap_or_else(|e| panic!("stdout must be JSON: {e}; {}", run.stdout));
    let error = value["error"].as_str().unwrap_or_default();
    assert!(error.contains("revoked"), "{value}");
    assert!(error.contains("email:ops@example.com"), "{value}");
    let fix = value["fix"].as_str().unwrap_or_default();
    assert!(fix.contains("re-run"), "{value}");
    assert!(!run.stdout.contains(&token), "{}", run.stdout);
    assert!(!run.stderr.contains(&token), "{}", run.stderr);
}

/// Past the write the state is milder -- the token IS installed, the adapter
/// just has not reloaded -- and reporting it as a revocation would send the
/// operator to mint again, revoking the token they just installed.
#[test]
fn a_failed_rollout_says_installed_not_revoked() {
    let token = sample_token(EXP);
    let server = api_for_mint(&token);
    let stub =
        ClusterStub::new(serde_json::json!({"mailAdapter": {"deploy": true}})).failing("rollout");
    let run = stub.run(&mint_argv(&server.base_url));
    assert_ne!(run.code, 0, "{} {}", run.stdout, run.stderr);
    let value: serde_json::Value = serde_json::from_str(run.stdout.trim())
        .unwrap_or_else(|e| panic!("stdout must be JSON: {e}; {}", run.stdout));
    let error = value["error"].as_str().unwrap_or_default();
    assert!(error.contains("was written"), "{value}");
    let fix = value["fix"].as_str().unwrap_or_default();
    assert!(fix.contains("rollout restart"), "{value}");
    assert!(
        fix.contains("do not mint another one"),
        "the recovery is a restart, never a second mint: {value}"
    );
    assert!(stub.patched().contains(&token), "the write did succeed");
}

//! Forge credential expiry in `curie cluster status` and `curie doctor`
//! (#4397 AC 7, decision 8; ADR 0197).
//!
//! Each configured `factory.codeHosts[]` / `factory.trackers[]` credential is
//! shown as a known expiry (with the date), unknown, or none, read from the
//! release's recorded values, and a known expiry that is past or at most 14
//! days away warns. `tests/vectors/forge-credential-expiry.json` is the shared
//! vector the API's `credential_providers.expiry_for` also consumes. The
//! status and doctor cases drive the built binary against a stubbed
//! helm/kubectl, so the date math runs against the real clock with margins.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::process::{Command, Output};

use curie::forge_expiry::{classify, report, CredentialExpiry};
use serde_json::Value;
use time::format_description::well_known::Rfc3339;
use time::{Duration, OffsetDateTime};

const VECTOR: &str = include_str!("../../tests/vectors/forge-credential-expiry.json");

fn vector() -> Value {
    serde_json::from_str(VECTOR).expect("the expiry vector is JSON")
}

fn at(text: &str) -> OffsetDateTime {
    OffsetDateTime::parse(text, &Rfc3339).unwrap_or_else(|e| panic!("{text}: {e}"))
}

fn at_or_none(value: &Value) -> Option<OffsetDateTime> {
    value.as_str().map(at)
}

/// A one-entry factory block around `credential`, as the release records it.
fn one_tracker(credential: &Value) -> Value {
    serde_json::json!({"factory": {
        "codeHosts": [],
        "trackers": [{"name": "t", "kind": "github", "credential": credential}]
    }})
}

fn lines(values: &Value, now: OffsetDateTime) -> Vec<Value> {
    report(values, now)
        .iter()
        .map(|line| serde_json::to_value(line).expect("an expiry line serializes"))
        .collect()
}

#[test]
fn expiry_vector_matches() {
    let vector = vector();
    let cases = vector["classify"].as_array().expect("classify cases");
    assert!(cases.len() >= 6, "{vector}");
    for case in cases {
        let name = case["name"].as_str().unwrap();
        let (expiry, expires_at) = match classify(&case["credential"]) {
            CredentialExpiry::Known(when) => ("known", Some(when)),
            CredentialExpiry::Unknown => ("unknown", None),
            CredentialExpiry::None => ("none", None),
        };
        assert_eq!(expiry, case["expiry"], "{name}");
        assert_eq!(expires_at, at_or_none(&case["expires_at"]), "{name}");
    }
}

#[test]
fn expiry_vector_warning_boundaries_match() {
    // 15 days: none; exactly 14 days: warns; at and after expiry: expired.
    let vector = vector();
    let cases = vector["warn"].as_array().expect("warn cases");
    for case in cases {
        let name = case["name"].as_str().unwrap();
        let got = lines(
            &one_tracker(&case["credential"]),
            at(case["now"].as_str().unwrap()),
        );
        assert_eq!(got.len(), 1, "{name}: {got:?}");
        assert_eq!(got[0]["warning"], case["warning"], "{name}");
        assert_eq!(got[0]["type"], case["credential"]["type"], "{name}");
    }
}

#[test]
fn expiry_vector_report_lists_every_code_host_and_tracker() {
    let vector = vector();
    let fixture = &vector["report"];
    let values = serde_json::json!({"factory": fixture["factory"]});
    let got = lines(&values, at(fixture["now"].as_str().unwrap()));
    let want = fixture["lines"].as_array().unwrap();
    assert_eq!(got.len(), want.len(), "{got:?}");
    for (got, want) in got.iter().zip(want) {
        for key in ["section", "name", "type", "expiry", "warning"] {
            assert_eq!(got[key], want[key], "{key}: {got} vs {want}");
        }
        assert_eq!(
            at_or_none(&got["expires_at"]),
            at_or_none(&want["expires_at"]),
            "{got}"
        );
    }
}

#[test]
fn expiry_report_is_empty_without_a_factory_block() {
    let now = at("2026-10-25T00:00:00Z");
    assert!(report(&serde_json::json!({}), now).is_empty());
    assert!(report(&serde_json::json!({"factory": null}), now).is_empty());
    assert!(report(&Value::Null, now).is_empty());
}

// Black box: status and doctor against a stubbed release ------------------

struct Release {
    dir: tempfile::TempDir,
    soon: String,
    far: String,
}

fn date(t: OffsetDateTime) -> String {
    t.date().to_string()
}

fn midnight(t: OffsetDateTime) -> String {
    format!("{}T00:00:00Z", date(t))
}

impl Release {
    /// `factory`: whether the recorded values carry a factory block.
    /// `release`: whether helm has a release to read values from.
    fn new(factory: bool, release: bool) -> Self {
        let now = OffsetDateTime::now_utc();
        let soon = midnight(now + Duration::days(9));
        let far = midnight(now + Duration::days(60));
        let past = midnight(now - Duration::days(3));
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let mut values = serde_json::json!({
            "mailAdapter": {"deploy": false},
            "dispatcher": {"slack": {"appToken": "x", "botToken": "x"}},
            "api": {"ingress": {"enabled": true}},
        });
        if factory {
            let app = serde_json::json!({"type": "github_app", "appId": "7001",
                "existingSecret": "curie-factory-app", "existingSecretKey": "privateKey"});
            let token = |extra: Value| {
                let mut credential = serde_json::json!({"type": "static_token",
                    "existingSecret": "forge-token", "existingSecretKey": "token"});
                credential
                    .as_object_mut()
                    .unwrap()
                    .extend(extra.as_object().unwrap().clone());
                credential
            };
            values["factory"] = serde_json::json!({
                "enabled": true,
                "codeHosts": [
                    {"name": "github", "kind": "github", "credential": app.clone()},
                    {"name": "corp-gitlab", "kind": "gitlab",
                     "credential": token(serde_json::json!({"expiresAt": far}))}
                ],
                "trackers": [
                    {"name": "github", "kind": "github", "credential": app},
                    {"name": "soon-token", "kind": "gitlab",
                     "credential": token(serde_json::json!({"expiresAt": soon}))},
                    {"name": "past-token", "kind": "gitlab",
                     "credential": token(serde_json::json!({"expiresAt": past}))},
                    {"name": "unknown-token", "kind": "gitlab", "credential": token(serde_json::json!({}))},
                    {"name": "never-token", "kind": "gitlab",
                     "credential": token(serde_json::json!({"neverExpires": true}))},
                    {"name": "corp-jira", "kind": "jira_cloud", "credential": {
                        "type": "oauth_client_credentials", "clientId": "curie-factory",
                        "existingSecret": "jira", "existingSecretKey": "clientSecret"}}
                ]
            });
        }
        fs::write(root.join("values.json"), values.to_string()).unwrap();
        fs::write(
            root.join("outbox.json"),
            serde_json::json!({"count": 0, "oldest_age_s": 0.0, "inflight": 0, "retry": 0,
                "terminal": 0, "state": "empty", "degraded": false})
            .to_string(),
        )
        .unwrap();
        let worker = serde_json::json!({
            "kind": "Pod", "metadata": {"name": "acme-worker-abc", "labels": {
                "app.kubernetes.io/component": "worker",
                "app.kubernetes.io/instance": "acme"
            }},
            "spec": {"containers": [{"name": "worker", "image": "busybox:1"}]},
            "status": {"phase": "Running", "containerStatuses": [{
                "name": "worker", "image": "busybox:1", "imageID": "containerd://sha256:example",
                "ready": true, "state": {"running": {}}
            }]}
        });
        fs::write(
            root.join("pods.json"),
            serde_json::json!({"items": [worker]}).to_string(),
        )
        .unwrap();
        fs::write(
            root.join("converged.sh"),
            include_str!("data/converged-installation-read.sh"),
        )
        .unwrap();
        if !release {
            fs::write(root.join("no-release"), "").unwrap();
        }
        let script = r#"#!/bin/sh
d="${0%/*}"
if [ -f "$d/api-port" ]; then
case "${0##*/}:$*" in
  kubectl:*"get secret -l"*) if [ -f "$d/hang-discovery" ]; then exec sleep 60; fi; printf '%s\n' 'acme-curie-secrets'; exit 0 ;;
  kubectl:*"get secret acme-curie-secrets"*) cat "$d/api-key"; exit 0 ;;
  kubectl:*port-forward*)
printf 'Forwarding from 127.0.0.1:%s -> 8000\n' "$(cat "$d/api-port")"
exec sleep 60 ;;
esac
fi
case "${0##*/}:$*" in
  helm:*) if [ -f "$d/no-release" ]; then
  case "$*" in
    version*) printf '%s\n' 'v3.14.0'; exit 0 ;;
    list*) printf '%s\n' '[]'; exit 0 ;;
  esac
  echo 'Error: release: not found' >&2; exit 1
fi ;;
esac
case "${0##*/}:$*" in
  kubectl:*exec*curie_worker.completion_health*) cat "$d/outbox.json"; exit 0 ;;
  kubectl:*exec*curie_worker.upgrade_drain*)
printf '%s\n' '{"state":"claims_enabled","since":null,"revision":null}'
exit 0 ;;
  kubectl:"get pods"*) cat "$d/pods.json"; exit 0 ;;
  kubectl:"config current-context") printf '%s\n' 'owned-test'; exit 0 ;;
  kubectl:*"component=api"*) printf '%s\n' 'acme-api'; exit 0 ;;
  kubectl:*"component=worker"*) printf '%s\n' 'acme-worker-abc'; exit 0 ;;
  kubectl:*"config view"*) printf '%s\n' 'https://127.0.0.1:6443'; exit 0 ;;
  helm:status*)
case "$*" in
  *-o*json*) printf '%s\n' '{"version":1,"info":{"status":"deployed"},"hooks":[]}' ;;
  *) printf '%s\n' 'STATUS: deployed' 'REVISION: 1' ;;
esac
exit 0 ;;
  helm:version*) printf '%s\n' 'v3.14.0'; exit 0 ;;
  helm:list*) printf '%s\n' '[{"name":"acme","chart":"curie-0.13.0"}]'; exit 0 ;;
  helm:"get values"*) cat "$d/values.json"; exit 0 ;;
  docker:*) exit 0 ;;
esac
. "$d/converged.sh"
exit 1
"#;
        for name in ["kubectl", "helm", "docker"] {
            let path = root.join(name);
            fs::write(&path, script).unwrap();
            fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
        }
        Self {
            dir,
            soon: date(at(&soon)),
            far: date(at(&far)),
        }
    }

    fn run(&self, verb: &str, json: bool) -> Output {
        let mut command = Command::new(env!("CARGO_BIN_EXE_curie"));
        command.arg("--color=never");
        if json {
            command.arg("--json");
        }
        if verb == "status" {
            command.arg("cluster");
        }
        command.args([verb, "--namespace", "expiry-test", "--release", "acme"]);
        command
            .current_dir(self.dir.path())
            .env(
                "PATH",
                format!("{}:/usr/bin:/bin", self.dir.path().display()),
            )
            .env_remove("CURIE_API_URL")
            .env_remove("CURIE_API_KEY")
            .output()
            .unwrap()
    }

    fn json(&self, verb: &str) -> Value {
        let output = self.run(verb, true);
        serde_json::from_slice(&output.stdout).unwrap_or_else(|e| {
            panic!(
                "{e}: {} / {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            )
        })
    }

    fn text(&self, verb: &str) -> String {
        let output = self.run(verb, false);
        format!(
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    }
}

/// A fake platform API answering `GET /forges/credentials/health` with
/// `body`, recording each request head it receives.
struct FakeApi {
    port: u16,
    requests: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
}

impl FakeApi {
    fn serve(body: Value) -> Self {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen = requests.clone();
        let body = body.to_string();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { continue };
                let _ = stream.set_read_timeout(Some(std::time::Duration::from_secs(5)));
                let mut head = Vec::new();
                let mut buf = [0u8; 1024];
                while !head.windows(4).any(|w| w == b"\r\n\r\n") {
                    match stream.read(&mut buf) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => head.extend_from_slice(&buf[..n]),
                    }
                }
                if head.is_empty() {
                    continue;
                }
                seen.lock()
                    .unwrap()
                    .push(String::from_utf8_lossy(&head).to_string());
                let _ = write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
            }
        });
        Self { port, requests }
    }
}

const API_KEY: &str = "sentinel-api-key-4630";

impl Release {
    /// Route the stubbed key discovery and port-forward to `api`.
    fn with_api(self, api: &FakeApi) -> Self {
        fs::write(self.dir.path().join("api-port"), api.port.to_string()).unwrap();
        fs::write(self.dir.path().join("api-key"), API_KEY).unwrap();
        self
    }
}

fn by_name<'a>(entries: &'a [Value], section: &str, name: &str) -> &'a Value {
    entries
        .iter()
        .find(|e| e["section"] == section && e["name"] == name)
        .unwrap_or_else(|| panic!("no {section} {name} in {entries:?}"))
}

#[test]
fn status_lists_each_credential_known_unknown_none() {
    let release = Release::new(true, true);
    let value = release.json("status");
    let credentials = value["credentials"]
        .as_array()
        .unwrap_or_else(|| panic!("status --json has no credentials list: {value}"));
    assert_eq!(credentials.len(), 8, "{value}");
    let github = by_name(credentials, "code_host", "github");
    assert_eq!(
        (
            &github["type"],
            &github["expiry"],
            &github["expires_at"],
            &github["warning"]
        ),
        (
            &Value::from("github_app"),
            &Value::from("none"),
            &Value::Null,
            &Value::Null
        )
    );
    let far = by_name(credentials, "code_host", "corp-gitlab");
    assert_eq!(far["expiry"], "known");
    assert_eq!(
        at_or_none(&far["expires_at"]).map(date),
        Some(release.far.clone()),
        "{far}"
    );
    assert!(far["warning"].is_null(), "60 days out does not warn: {far}");
    assert_eq!(
        by_name(credentials, "tracker", "unknown-token")["expiry"],
        "unknown"
    );
    assert_eq!(
        by_name(credentials, "tracker", "never-token")["expiry"],
        "none"
    );
    let jira = by_name(credentials, "tracker", "corp-jira");
    assert_eq!(
        (&jira["type"], &jira["expiry"]),
        (
            &Value::from("oauth_client_credentials"),
            &Value::from("unknown")
        )
    );
    for entry in credentials {
        if entry["expiry"] != "known" {
            assert!(
                entry["expires_at"].is_null() && entry["warning"].is_null(),
                "{entry}"
            );
        }
    }

    let text = release.text("status");
    for line in [
        "credential code_host github (github_app): none".to_string(),
        format!(
            "credential code_host corp-gitlab (static_token): known {}",
            release.far
        ),
        format!(
            "credential tracker soon-token (static_token): known {}",
            release.soon
        ),
        "credential tracker unknown-token (static_token): unknown".to_string(),
        "credential tracker never-token (static_token): none".to_string(),
        "credential tracker corp-jira (oauth_client_credentials): unknown".to_string(),
    ] {
        assert!(
            text.contains(&line),
            "status does not print {line:?}:\n{text}"
        );
    }
}

#[test]
fn status_warns_within_fourteen_days() {
    let release = Release::new(true, true);
    let value = release.json("status");
    let credentials = value["credentials"].as_array().expect("credentials list");
    let soon = by_name(credentials, "tracker", "soon-token");
    let warning = soon["warning"]
        .as_str()
        .unwrap_or_else(|| panic!("9 days out must warn: {soon}"));
    assert!(
        warning.contains("left); rotate it before then"),
        "{warning}"
    );
    let past = by_name(credentials, "tracker", "past-token");
    let expired = past["warning"]
        .as_str()
        .unwrap_or_else(|| panic!("a past expiry must warn: {past}"));
    assert!(expired.contains("expired at"), "{expired}");
    let warnings: Vec<&str> = value["warnings"]
        .as_array()
        .expect("status warnings")
        .iter()
        .filter_map(Value::as_str)
        .collect();
    for (name, text) in [("soon-token", warning), ("past-token", expired)] {
        assert!(
            warnings
                .iter()
                .any(|w| w.contains(name) && w.contains(text)),
            "the {name} warning must also land in warnings: {warnings:?}"
        );
    }
    assert!(
        !warnings.iter().any(|w| w.contains("corp-gitlab")),
        "a far expiry must not warn: {warnings:?}"
    );
    let text = release.text("status");
    assert!(text.contains(warning) && text.contains(expired), "{text}");
}

#[test]
fn status_lists_no_credentials_without_a_factory_block() {
    let release = Release::new(false, true);
    let value = release.json("status");
    assert_eq!(value["credentials"], serde_json::json!([]), "{value}");
    assert!(!release.text("status").contains("credential code_host"));
}

fn forge_checks(value: &Value) -> Vec<Value> {
    value["checks"]
        .as_array()
        .unwrap_or_else(|| panic!("doctor --json has no checks: {value}"))
        .iter()
        .filter(|c| c["id"] == "forge-credentials")
        .cloned()
        .collect()
}

#[test]
fn doctor_reports_forge_credentials_check() {
    let release = Release::new(true, true);
    let value = release.json("doctor");
    let checks = forge_checks(&value);
    assert_eq!(
        checks.len(),
        8,
        "one check per configured credential: {checks:?}"
    );
    let find = |name: &str| {
        checks
            .iter()
            .find(|c| c["detail"].as_str().is_some_and(|d| d.contains(name)))
            .unwrap_or_else(|| panic!("no forge-credentials check names {name}: {checks:?}"))
    };
    for (name, state) in [
        ("soon-token", "warn"),
        ("past-token", "warn"),
        ("corp-gitlab", "ok"),
        ("unknown-token", "ok"),
        ("never-token", "ok"),
        ("corp-jira", "ok"),
    ] {
        assert_eq!(find(name)["state"], state, "{name}: {checks:?}");
    }
    let soon = find("soon-token")["detail"].as_str().unwrap().to_string();
    assert!(soon.contains(&format!("known {}", release.soon)), "{soon}");
    assert!(soon.contains("rotate it before then"), "{soon}");
    assert!(find("unknown-token")["detail"]
        .as_str()
        .unwrap()
        .contains("unknown"));
    assert!(find("never-token")["detail"]
        .as_str()
        .unwrap()
        .contains("none"));
    assert!(find("corp-gitlab")["detail"]
        .as_str()
        .unwrap()
        .contains(&format!("known {}", release.far)));

    let text = release.text("doctor");
    assert!(
        text.contains("forge-credentials") || text.contains("soon-token"),
        "{text}"
    );
    assert!(text.contains(&format!("known {}", release.soon)), "{text}");
    assert!(text.contains("rotate it before then"), "{text}");
}

#[test]
fn doctor_has_no_forge_check_without_a_factory_block() {
    // Liveness: the same release with a factory block yields the checks.
    let with = Release::new(true, true);
    assert!(!forge_checks(&with.json("doctor")).is_empty());
    let release = Release::new(false, true);
    let value = release.json("doctor");
    assert!(forge_checks(&value).is_empty(), "{value}");
}

#[test]
fn doctor_does_not_call_credentials_fine_when_the_release_is_unread() {
    let release = Release::new(true, false);
    let value = release.json("doctor");
    let checks = forge_checks(&value);
    assert!(
        checks
            .iter()
            .all(|c| c["state"] != "ok" && c["state"] != "warn"),
        "with no release to read, no credential is reported ok or warn: {checks:?}"
    );
}

// #4630: authentication from the API's health report, merged per credential.

fn health_entry(section: &str, name: &str, authentication: &str, reason: Option<&str>) -> Value {
    serde_json::json!({"section": section, "name": name, "kind": "gitlab",
        "credential_type": "static_token", "authentication": authentication,
        "reason": reason, "observed_at": "2026-10-15T12:00:00Z"})
}

#[test]
fn status_merges_authentication_and_a_rejection_fails_status() {
    let api = FakeApi::serve(serde_json::json!({
        "observed_at": "2026-10-15T12:00:00Z",
        "entries": [
            health_entry("code_host", "github", "unknown", Some("not_probed")),
            health_entry("code_host", "corp-gitlab", "accepted", None),
            health_entry("tracker", "github", "unknown", Some("not_probed")),
            health_entry("tracker", "soon-token", "rejected", Some("unauthorized")),
            health_entry("tracker", "past-token", "accepted", None),
            health_entry("tracker", "unknown-token", "unknown", Some("credential_unreadable")),
            health_entry("tracker", "corp-jira", "unknown", Some("not_probed")),
        ]
    }));
    let release = Release::new(true, true).with_api(&api);
    let output = release.run("status", true);
    assert!(
        !output.status.success(),
        "a rejected credential fails status"
    );
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    let value: Value =
        serde_json::from_str(&stdout).unwrap_or_else(|e| panic!("{e}: {stdout} / {stderr}"));
    let credentials = value["credentials"].as_array().expect("credentials list");
    let auth = |section: &str, name: &str| {
        let entry = by_name(credentials, section, name);
        (
            entry["authentication"].as_str().unwrap().to_string(),
            entry["authentication_reason"].clone(),
        )
    };
    assert_eq!(
        auth("code_host", "corp-gitlab"),
        ("accepted".into(), Value::Null)
    );
    assert_eq!(
        auth("tracker", "soon-token"),
        ("rejected".into(), "unauthorized".into())
    );
    assert_eq!(
        auth("tracker", "unknown-token"),
        ("unknown".into(), "credential_unreadable".into())
    );
    assert_eq!(
        auth("tracker", "never-token"),
        ("unknown".into(), "api_unavailable".into()),
        "an entry the API omits is unknown, never accepted"
    );
    assert_eq!(
        by_name(credentials, "code_host", "corp-gitlab")["observed_at"],
        "2026-10-15T12:00:00Z"
    );
    // Expiry is unchanged by authentication.
    assert_eq!(
        by_name(credentials, "tracker", "past-token")["expiry"],
        "known"
    );
    assert!(by_name(credentials, "tracker", "past-token")["warning"].is_string());

    assert_eq!(value["healthy"], false, "{value}");
    let unhealthy: Vec<&str> = value["pods"]["unhealthy"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(Value::as_str)
        .collect();
    assert!(
        unhealthy
            .contains(&"credential tracker soon-token (static_token): authentication rejected"),
        "{unhealthy:?}"
    );
    let warnings: Vec<&str> = value["warnings"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(Value::as_str)
        .collect();
    assert!(warnings.contains(
        &"credential tracker unknown-token (static_token): authentication unknown (credential_unreadable)"
    ), "{warnings:?}");
    assert!(warnings.contains(
        &"credential tracker never-token (static_token): authentication unknown (api_unavailable)"
    ), "{warnings:?}");
    assert!(
        !warnings.iter().any(|w| w.contains("not_probed")),
        "not_probed does not warn: {warnings:?}"
    );

    let text = release.text("status");
    for line in [
        format!(
            "credential code_host corp-gitlab (static_token): known {}; authentication accepted",
            release.far
        ),
        "credential code_host github (github_app): none; authentication unknown (not_probed)"
            .to_string(),
        format!(
            "credential tracker soon-token (static_token): known {}; authentication rejected",
            release.soon
        ),
    ] {
        assert!(
            text.contains(&line),
            "status does not print {line:?}:\n{text}"
        );
    }

    for out in [&stdout, &stderr, &text] {
        assert!(!out.contains(API_KEY), "the API key reached output: {out}");
    }
    let requests = api.requests.lock().unwrap().clone();
    assert!(!requests.is_empty(), "the API was never read");
    for request in &requests {
        assert!(
            request.starts_with("GET /forges/credentials/health "),
            "{request}"
        );
        assert!(
            request
                .to_ascii_lowercase()
                .contains(&format!("x-api-key: {API_KEY}")),
            "{request}"
        );
    }
}

#[test]
fn status_reports_api_unavailable_when_the_api_cannot_be_read() {
    let release = Release::new(true, true);
    let value = release.json("status");
    let credentials = value["credentials"].as_array().expect("credentials list");
    assert_eq!(credentials.len(), 8, "{value}");
    for entry in credentials {
        assert_eq!(entry["authentication"], "unknown", "{entry}");
        assert_eq!(entry["authentication_reason"], "api_unavailable", "{entry}");
        assert!(entry["observed_at"].is_null(), "{entry}");
    }
    let warnings: Vec<&str> = value["warnings"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(Value::as_str)
        .collect();
    let unavailable: Vec<&&str> = warnings
        .iter()
        .filter(|w| w.contains("api_unavailable"))
        .collect();
    assert_eq!(
        unavailable.len(),
        1,
        "one warning for a failed read: {warnings:?}"
    );
    assert!(
        unavailable[0]
            .starts_with("forge credential authentication is unknown (api_unavailable): "),
        "{warnings:?}"
    );
    assert!(release
        .text("status")
        .contains("credential tracker never-token (static_token): none; authentication unknown (api_unavailable)"));
}

#[test]
fn status_returns_unknown_when_key_discovery_hangs_past_the_overall_deadline() {
    let api = FakeApi::serve(serde_json::json!({}));
    let release = Release::new(true, true).with_api(&api);
    fs::write(release.dir.path().join("hang-discovery"), "").unwrap();
    let started = std::time::Instant::now();
    let output = Command::new(env!("CARGO_BIN_EXE_curie"))
        .args(["--color=never", "--json", "cluster", "status"])
        .args(["--namespace", "expiry-test", "--release", "acme"])
        .current_dir(release.dir.path())
        .env(
            "PATH",
            format!("{}:/usr/bin:/bin", release.dir.path().display()),
        )
        .env("CURIE_TEST_HEALTH_READ_DEADLINE_MS", "500")
        .env_remove("CURIE_API_URL")
        .env_remove("CURIE_API_KEY")
        .output()
        .unwrap();
    assert!(
        started.elapsed() < std::time::Duration::from_secs(40),
        "status hung on discovery"
    );
    let value: Value = serde_json::from_slice(&output.stdout).unwrap_or_else(|e| {
        panic!("{e}: {}", String::from_utf8_lossy(&output.stdout));
    });
    let credentials = value["credentials"].as_array().expect("credentials list");
    assert!(!credentials.is_empty(), "{value}");
    for entry in credentials {
        assert_eq!(entry["authentication"], "unknown", "{entry}");
        assert_eq!(entry["authentication_reason"], "api_unavailable", "{entry}");
    }
    assert!(api.requests.lock().unwrap().is_empty());
}

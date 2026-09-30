//! The real status and doctor entry points must expose an unusable mail token.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::process::{Command, Output};

use serde_json::Value;

struct Fixture(tempfile::TempDir);

impl Fixture {
    fn new(state: &str) -> Self {
        Self::with_mail_adapter_deploy(state, serde_json::json!(true))
    }

    fn with_mail_adapter_deploy(state: &str, mail_adapter_deploy: Value) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        fs::write(
            root.join("status.json"),
            serde_json::json!({
                "status": "ready",
                "channel_token": {"present": true, "exp": 1_800_000_000, "state": state},
                "last_ingress_status": if state == "rejected" { Some(401) } else { None },
            })
            .to_string(),
        )
        .unwrap();
        let pod = serde_json::json!({
            "kind": "Pod", "metadata": {"name": "acme-mail-abc", "labels": {
                "app.kubernetes.io/component": "mail-adapter",
                "app.kubernetes.io/instance": "acme", "component": "acme-probe"
            }},
            "spec": {"containers": [{"name":"probe", "image":"busybox:1"}]},
            "status": {"phase":"Running", "containerStatuses":[{
                "name":"probe", "image":"busybox:1", "imageID":"containerd://sha256:example",
                "ready":true, "state":{"running":{}}
            }]}
        });
        fs::write(
            root.join("pods.json"),
            serde_json::json!({"items":[pod]}).to_string(),
        )
        .unwrap();
        fs::write(
            root.join("values.json"),
            serde_json::json!({
                "mailAdapter": {"deploy": mail_adapter_deploy},
                "dispatcher": {"slack": {"appToken": "x", "botToken": "x"}},
                "api": {"ingress": {"enabled": true}},
            })
            .to_string(),
        )
        .unwrap();
        fs::write(
            root.join("converged.sh"),
            include_str!("data/converged-installation-read.sh"),
        )
        .unwrap();
        let script = r#"#!/bin/sh
case "${0##*/}:$*" in
  kubectl:*--raw*) printf '%s\n' "$*" >> "${0%/*}/proxy-calls"; cat "${0%/*}/status.json"; exit 0 ;;
  kubectl:"get pods -n mail-test -l app.kubernetes.io/instance=acme,app.kubernetes.io/component=worker -o json")
    printf '%s\n' '{"items":[{"metadata":{"name":"acme-worker-abc","labels":{"app.kubernetes.io/instance":"acme","app.kubernetes.io/component":"worker"}},"status":{"phase":"Running"}}]}'
    exit 0 ;;
  kubectl:"exec -n mail-test acme-worker-abc -- python -m curie_worker.upgrade_drain --mode status --json --with-ttl")
    printf '%s\n' '{"state":"claims_enabled","since":null,"revision":null}'
    exit 0 ;;
  kubectl:"get pods"*) cat "${0%/*}/pods.json"; exit 0 ;;
  kubectl:"get deployments,statefulsets -n "*) printf '%s\n' '{"items":[{"kind":"Deployment","status":{"readyReplicas":1}}]}'; exit 0 ;;
  kubectl:"config current-context") printf '%s\n' 'owned-test'; exit 0 ;;
  kubectl:*"component=api"*) printf '%s\n' 'acme-api'; exit 0 ;;
  kubectl:*"config view"*) printf '%s\n' 'https://127.0.0.1:6443'; exit 0 ;;
  helm:status*)
    case "$*" in
      *-o*json*) printf '%s\n' '{"version":1,"info":{"status":"deployed"},"hooks":[]}' ;;
      *) printf '%s\n' 'STATUS: deployed' 'REVISION: 1' ;;
    esac
    exit 0 ;;
  helm:version*) printf '%s\n' 'v3.14.0'; exit 0 ;;
  helm:list*) printf '%s\n' '[{"name":"acme","chart":"curie-0.8.7"}]'; exit 0 ;;
  helm:"get values"*)
    if [ -f "${0%/*}/values-unreadable" ]; then printf '%s\n' 'Error: unreachable' >&2; exit 1; fi
    if [ -f "${0%/*}/values-hang" ]; then sleep 120; fi
    cat "${0%/*}/values.json"; exit 0 ;;
  docker:*) exit 0 ;;
esac
. "${0%/*}/converged.sh"
exit 1
"#;
        for name in ["kubectl", "helm", "docker"] {
            let path = root.join(name);
            fs::write(&path, script).unwrap();
            fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
        }
        Self(temp)
    }

    /// Make `helm get values` fail, the way an unreachable API server or a
    /// broken helm plugin does. Distinct from a release that positively does
    /// not exist.
    fn break_the_values_read(&self) {
        fs::write(self.0.path().join("values-unreadable"), "").unwrap();
    }

    /// Make `helm get values` never return. `run_capture` waits on its
    /// subprocess without a deadline, so only the caller's own timeout can end
    /// this.
    fn hang_the_values_read(&self) {
        fs::write(self.0.path().join("values-hang"), "").unwrap();
    }

    /// Every `/statusz` pod-proxy read the run actually made.
    fn proxy_calls(&self) -> Vec<String> {
        fs::read_to_string(self.0.path().join("proxy-calls"))
            .unwrap_or_default()
            .lines()
            .map(str::to_string)
            .collect()
    }

    fn run(&self, verb: &str) -> Output {
        let mut command = Command::new(env!("CARGO_BIN_EXE_curie"));
        command.arg("--json");
        if verb == "status" {
            command.arg("cluster");
        }
        command.args([verb, "--namespace", "mail-test", "--release", "acme"]);
        command
            .current_dir(self.0.path())
            .env("PATH", format!("{}:/usr/bin:/bin", self.0.path().display()))
            .env_remove("CURIE_API_URL")
            .env_remove("CURIE_API_KEY")
            .output()
            .unwrap()
    }
}

#[test]
fn expired_mail_token_is_visible_on_status_and_doctor_and_valid_token_recovers() {
    for state in ["expired", "rejected", "ok"] {
        let fixture = Fixture::new(state);
        for verb in ["status", "doctor"] {
            let output = fixture.run(verb);
            let value: Value = serde_json::from_slice(&output.stdout).unwrap_or_else(|e| {
                panic!(
                    "{e}: {} / {}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                )
            });
            if verb == "status" {
                assert_eq!(value["healthy"], state == "ok", "{value}");
                assert_eq!(
                    value["pods"]["rows"][0]["mail_channel"]["channel_token"]["state"],
                    state
                );
                assert_eq!(
                    output.status.code(),
                    Some(if state == "ok" { 0 } else { 1 })
                );
            } else {
                let check = value["checks"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|c| c["id"] == "mail-channel")
                    .expect("mail check");
                assert_eq!(check["state"], if state == "ok" { "ok" } else { "missing" });
                assert!(check["detail"].as_str().unwrap().contains(state));
                if state != "ok" {
                    assert!(check["fix"].as_str().unwrap().contains("channel-token"));
                }
            }
        }
    }
}

#[test]
fn doctor_observes_expired_mail_status_for_go_truthy_string_deploy_value() {
    // `--set-string mailAdapter.deploy=true` leaves Helm with a non-empty
    // string, so the chart renders the adapter. Doctor must follow that same
    // Go-template truthiness rather than calling the channel not applicable.
    let fixture = Fixture::with_mail_adapter_deploy("expired", serde_json::json!("true"));
    let output = fixture.run("doctor");
    let value: Value = serde_json::from_slice(&output.stdout).unwrap_or_else(|e| {
        panic!(
            "{e}: {} / {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    });
    let check = value["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|check| check["id"] == "mail-channel")
        .expect("mail check");

    assert_eq!(check["state"], "missing", "{value}");
    assert!(check["detail"].as_str().unwrap().contains("expired"));
    assert_eq!(value["ready"], false, "{value}");
}

#[test]
fn unreadable_mail_status_is_unknown_and_does_not_render_response_data() {
    let fixture = Fixture::new("ok");
    fs::write(
        fixture.0.path().join("status.json"),
        "private-response-sentinel",
    )
    .unwrap();
    for verb in ["status", "doctor"] {
        let output = fixture.run(verb);
        let text = String::from_utf8(output.stdout).unwrap();
        assert!(!text.contains("private-response-sentinel"));
        let value: Value = serde_json::from_str(&text).unwrap();
        if verb == "status" {
            // #2457: a diagnosis the command could not MAKE is a warning. Every
            // pod is Ready and the release converged; an unreadable `/statusz`
            // says nothing about that, so it must not fail the command a script
            // uses to ask whether the release is up.
            assert_eq!(value["healthy"], true, "{value}");
            assert_eq!(output.status.code(), Some(0));
            assert!(value["pods"]["unhealthy"].as_array().unwrap().is_empty());
            assert!(
                value["warnings"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|warning| warning.as_str().unwrap().contains("unknown")),
                "{value}"
            );
            assert!(value["pods"]["rows"][0]["status"]
                .as_str()
                .unwrap()
                .contains("unknown"));
        } else {
            let check = value["checks"]
                .as_array()
                .unwrap()
                .iter()
                .find(|check| check["id"] == "mail-channel")
                .unwrap();
            assert_eq!(check["state"], "missing");
            assert!(check["detail"].as_str().unwrap().contains("unknown"));
        }
    }
}

/// #2457: `status` must apply the `mailAdapter.deploy` gate `doctor` already
/// has, at the real entry point.
///
/// The gate matters exactly when a pod still CARRIES the mail-adapter labels on
/// a release where the operator turned mail off -- a scale-down that has not
/// finished, an orphan from a prior revision. The fixture is that shape: an
/// expired token on a labelled pod, `mailAdapter.deploy: false`. Replace the
/// gate with `if true` and this test reds, because the orphan is probed and its
/// expired token fails the command.
#[test]
fn status_does_not_probe_a_release_with_the_mail_adapter_turned_off() {
    let fixture = Fixture::with_mail_adapter_deploy("expired", serde_json::json!(false));

    let output = fixture.run("status");
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();

    assert_eq!(value["healthy"], true, "{value}");
    assert_eq!(output.status.code(), Some(0));
    assert!(
        fixture.proxy_calls().is_empty(),
        "a release with no mail adapter must not be probed: {:?}",
        fixture.proxy_calls()
    );
    assert!(value["pods"]["unhealthy"].as_array().unwrap().is_empty());
    assert!(value["warnings"].as_array().unwrap().is_empty(), "{value}");
    assert_eq!(value["pods"]["rows"][0]["mail_channel"], Value::Null);
}

/// #2457: a values read that FAILED is not a falsy `mailAdapter.deploy`.
///
/// Suppressing the probe on an unreadable values file would lose the very
/// diagnosis #2456 added. It is safe to probe there precisely because an
/// unreadable ANSWER is now only a warning -- so the worst case is noise, while
/// the alternative silently hides an expired token.
#[test]
fn status_still_finds_an_expired_token_when_the_values_read_fails() {
    let fixture = Fixture::new("expired");
    fixture.break_the_values_read();

    let output = fixture.run("status");
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();

    assert_eq!(value["healthy"], false, "{value}");
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(fixture.proxy_calls().len(), 1);
    assert!(
        value["pods"]["unhealthy"]
            .as_array()
            .unwrap()
            .iter()
            .any(|reason| reason.as_str().unwrap().contains("expired")),
        "{value}"
    );
}

/// #2457: the values read feeds one optional diagnosis, so it must never be the
/// reason the command produces nothing.
///
/// It is joined with five other reads that are also unbounded, and those are
/// deliberately left alone: their failure is an error the operator must see, so
/// bounding them needs a policy on what to report instead, which is a different
/// decision. This one already tolerates failure, so a deadline is simply another
/// way to fail, and the run continues exactly as it does on an unreadable read.
#[test]
fn status_survives_a_values_read_that_never_returns() {
    let fixture = Fixture::new("expired");
    fixture.hang_the_values_read();

    let started = std::time::Instant::now();
    let output = fixture.run("status");
    let elapsed = started.elapsed();

    let value: Value = serde_json::from_slice(&output.stdout).unwrap_or_else(|e| {
        panic!(
            "{e}: {} / {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    });
    assert!(
        elapsed < std::time::Duration::from_secs(60),
        "the report must not wait on the hung read: {elapsed:?}"
    );
    // A deadline is a failed read, and a failed read still probes -- so the real
    // expired token is still found rather than lost to the hang.
    assert_eq!(value["healthy"], false, "{value}");
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(fixture.proxy_calls().len(), 1);
}

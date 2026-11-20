//! `curie cluster factory --model-auth-mode` alone is a mode-only change: it
//! writes `factory.modelAuthMode` and nothing else, whatever forges the
//! release records and whether its intake is enabled. Driven through the real
//! binary with stub `helm` and `kubectl` on PATH; the stub `helm upgrade`
//! copies the private values file it is handed.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const STUB_HELM: &str = r#"#!/bin/sh
echo "helm $*" >> "$STUB_DIR/calls.log"
case "$1 $2" in
  "get values")
    if [ -f "$STUB_DIR/recorded.json" ]; then cat "$STUB_DIR/recorded.json"; exit 0; fi
    echo "Error: release: not found" >&2
    exit 1
    ;;
  "get hooks") exit 0 ;;
  "upgrade "*)
    prev=""
    for arg in "$@"; do
      if [ "$prev" = "-f" ]; then cp "$arg" "$STUB_DIR/applied-values"; fi
      prev="$arg"
    done
    exit 0
    ;;
esac
exit 0
"#;

const STUB_KUBECTL: &str = r#"#!/bin/sh
echo "kubectl $*" >> "$STUB_DIR/calls.log"
exit 0
"#;

struct Stub {
    _temp: tempfile::TempDir,
    dir: PathBuf,
}

impl Stub {
    fn new(recorded: Option<serde_json::Value>) -> Self {
        let temp = tempfile::tempdir().expect("tempdir");
        let dir = temp.path().to_path_buf();
        fs::create_dir_all(dir.join("bin")).unwrap();
        fs::create_dir_all(dir.join("home")).unwrap();
        for (name, body) in [("helm", STUB_HELM), ("kubectl", STUB_KUBECTL)] {
            let path = dir.join("bin").join(name);
            fs::write(&path, body).unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
        }
        if let Some(values) = recorded {
            fs::write(dir.join("recorded.json"), values.to_string()).unwrap();
        }
        Stub { _temp: temp, dir }
    }

    fn run(&self, mode: &str, extra: &[&str]) -> Output {
        let chart = Path::new(env!("CARGO_MANIFEST_DIR")).join("../charts/curie");
        let path =
            std::env::join_paths([self.dir.join("bin"), "/bin".into(), "/usr/bin".into()]).unwrap();
        Command::new(env!("CARGO_BIN_EXE_curie"))
            .args(["--json", "cluster", "factory", "--model-auth-mode", mode])
            .args(["--namespace", "acme", "--release", "acme", "--chart"])
            .arg(chart)
            .args(extra)
            .current_dir(&self.dir)
            .env_clear()
            .env("PATH", path)
            .env("HOME", self.dir.join("home"))
            .env("CURIE_CONFIG_DIR", self.dir.join("home/.config/curie"))
            .env("STUB_DIR", &self.dir)
            .output()
            .expect("run curie")
    }

    fn applied(&self) -> Option<serde_json::Value> {
        let raw = fs::read_to_string(self.dir.join("applied-values")).ok()?;
        Some(serde_norway::from_str(&raw).expect("applied values parse"))
    }

    fn calls(&self) -> String {
        fs::read_to_string(self.dir.join("calls.log")).unwrap_or_default()
    }
}

fn stdout_json(output: &Output) -> serde_json::Value {
    serde_json::from_slice(&output.stdout).unwrap_or_else(|e| {
        panic!(
            "stdout is not JSON ({e}): {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    })
}

fn gitlab_only() -> serde_json::Value {
    let credential = serde_json::json!({
        "type": "static_token", "existingSecret": "acme-gitlab", "existingSecretKey": "token"
    });
    serde_json::json!({"factory": {
        "enabled": true,
        "intake": "poll",
        "codeHosts": [{"name": "gitlab", "kind": "gitlab", "host": "gitlab.example.com",
            "apiUrl": "https://gitlab.example.com/api/v4", "credential": credential}],
        "trackers": [{"name": "gitlab", "kind": "gitlab", "host": "gitlab.example.com",
            "apiUrl": "https://gitlab.example.com/api/v4", "label": "factory",
            "mention": "acme-bot", "credential": credential}],
    }})
}

#[test]
fn model_auth_mode_alone_applies_on_a_gitlab_only_release() {
    for mode in ["personal", "service"] {
        let stub = Stub::new(Some(gitlab_only()));
        let output = stub.run(mode, &[]);
        assert!(
            output.status.success(),
            "stdout {}\nstderr {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let json = stdout_json(&output);
        assert_eq!(json, serde_json::json!({"factory_model_auth_mode": mode}));
        assert!(json.get("github_app_registration_url").is_none(), "{json}");
        assert_eq!(
            stub.applied(),
            Some(serde_json::json!({"factory": {"modelAuthMode": mode}})),
            "the values file holds only factory.modelAuthMode; calls:\n{}",
            stub.calls()
        );
    }
}

#[test]
fn model_auth_mode_alone_keeps_a_disabled_intake_disabled() {
    let stub = Stub::new(Some(serde_json::json!({
        "api": {"githubAppId": "7001", "githubAppExistingSecret": "acme-app"},
        "factory": {"enabled": false},
    })));
    let output = stub.run("personal", &[]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let applied = stub.applied().expect("helm upgrade received a values file");
    assert_eq!(
        applied,
        serde_json::json!({"factory": {"modelAuthMode": "personal"}})
    );
    assert!(applied.pointer("/factory/enabled").is_none(), "{applied}");
    assert!(applied.pointer("/factory/trackers").is_none(), "{applied}");
    assert!(applied.pointer("/factory/codeHosts").is_none(), "{applied}");
    assert!(applied.get("api").is_none(), "{applied}");
    assert_eq!(
        stdout_json(&output),
        serde_json::json!({"factory_model_auth_mode": "personal"})
    );
}

#[test]
fn model_auth_mode_alone_refuses_a_missing_release_and_an_unverifiable_substrate() {
    let missing = Stub::new(None);
    let output = missing.run("personal", &[]);
    assert!(!output.status.success());
    let error = String::from_utf8_lossy(&output.stdout).to_string()
        + &String::from_utf8_lossy(&output.stderr);
    assert!(error.contains("release acme was not found"), "{error}");
    assert!(missing.applied().is_none(), "{}", missing.calls());

    let mut recorded = gitlab_only();
    recorded["worker"] = serde_json::json!({"extraEnv": [{
        "name": "CURIE_SANDBOX_SUBSTRATE",
        "valueFrom": {"configMapKeyRef": {"name": "acme-substrate", "key": "substrate"}},
    }]});
    let unverifiable = Stub::new(Some(recorded.clone()));
    let output = unverifiable.run("personal", &[]);
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("valueFrom"),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert!(
        !unverifiable.calls().contains("helm upgrade"),
        "{}",
        unverifiable.calls()
    );

    let service = Stub::new(Some(recorded));
    let output = service.run("service", &[]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        service.applied(),
        Some(serde_json::json!({"factory": {"modelAuthMode": "service"}}))
    );
}

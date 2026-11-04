//! Local deploy names hosted connectors with the same release and namespace
//! the compose worker hands the runner.
//!
//! `compose.dev.yaml` passes `CURIE_RELEASE=${CURIE_RELEASE:-curie}` and
//! `CURIE_NAMESPACE=${CURIE_NAMESPACE:-default}` into the worker.
//! `WorkerConfig` reads those two variables. `binding.py` copies them onto
//! the sandbox boot env. The runner's `derive_mcp_servers` calls
//! `plugin_format.connector_render.mcp_entry`, and the hosted URL host is
//! `service_dns`. The CLI starts the container under `object_name` and
//! publishes that same DNS name as the container's network alias
//! (`connector_build::compose_overlay`).

use std::path::Path;
use std::process::Command;

use curie::connector_build::{object_name, service_dns};

const AGENT: &str = "acme-bot";
const CONNECTOR: &str = "grafana";

struct EnvVar {
    name: &'static str,
    previous: Option<std::ffi::OsString>,
}

impl EnvVar {
    fn set(name: &'static str, value: &str) -> Self {
        let previous = std::env::var_os(name);
        std::env::set_var(name, value);
        Self { name, previous }
    }

    fn unset(name: &'static str) -> Self {
        let previous = std::env::var_os(name);
        std::env::remove_var(name);
        Self { name, previous }
    }
}

impl Drop for EnvVar {
    fn drop(&mut self) {
        match self.previous.take() {
            Some(value) => std::env::set_var(self.name, value),
            None => std::env::remove_var(self.name),
        }
    }
}

/// The name and DNS host the worker's runner dials for this env pair.
///
/// This is `WorkerConfig` plus `connector_render.object_name` and
/// `service_dns`, the same pair `mcp_entry` puts in the hosted URL.
fn worker_dial(release: &str, namespace: &str) -> (String, String) {
    let repo = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("the cli crate lives one level under the repo root");
    let script = r#"
import json
from curie_worker.config import WorkerConfig
from plugin_format.connector_render import object_name, service_dns
cfg = WorkerConfig()
agent = "acme-bot"
connector = "grafana"
print(json.dumps({
    "name": object_name(cfg.connector_release, agent, connector),
    "dns": service_dns(cfg.connector_release, agent, connector, cfg.connector_namespace),
}))
"#;
    let mut cmd = Command::new("uv");
    cmd.args(["run", "--python", "3.13", "python", "-c", script])
        .current_dir(repo)
        .env_clear()
        .env("PATH", std::env::var_os("PATH").expect("PATH"))
        .env("HOME", std::env::var_os("HOME").expect("HOME"))
        .env("CURIE_RELEASE", release)
        .env("CURIE_NAMESPACE", namespace);
    for key in [
        "SSL_CERT_FILE",
        "SSL_CERT_DIR",
        "UV_CACHE_DIR",
        "XDG_CACHE_HOME",
    ] {
        if let Some(value) = std::env::var_os(key) {
            cmd.env(key, value);
        }
    }
    let output = cmd.output().expect("run the worker dial probe");
    assert!(
        output.status.success(),
        "worker dial probe failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let parsed: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("worker dial probe json");
    (
        parsed["name"]
            .as_str()
            .expect("worker object name")
            .to_string(),
        parsed["dns"].as_str().expect("worker dns").to_string(),
    )
}

fn started(agent: &str) -> (String, String) {
    let scope = curie::commands::local_connector_scope(agent);
    (
        object_name(&scope.release, &scope.agent, CONNECTOR),
        service_dns(&scope.release, &scope.agent, CONNECTOR, &scope.namespace),
    )
}

#[tokio::test]
async fn local_connector_name_matches_the_worker_dial_name() {
    let _lock = crate::support::ENV_LOCK.lock().await;
    let _release = EnvVar::set("CURIE_RELEASE", "acme-rel");
    let _namespace = EnvVar::set("CURIE_NAMESPACE", "acme-ns");
    let scope = curie::commands::local_connector_scope(AGENT);
    let name = object_name(&scope.release, &scope.agent, CONNECTOR);
    let dns = service_dns(&scope.release, &scope.agent, CONNECTOR, &scope.namespace);
    let (dialed_name, dialed_dns) = worker_dial("acme-rel", "acme-ns");
    assert_eq!(name, dialed_name);
    assert_eq!(dns, dialed_dns);
    assert_eq!(name, "acme-rel-acme-bot-mcp-grafana");
    assert_eq!(
        dns,
        "acme-rel-acme-bot-mcp-grafana.acme-ns.svc.cluster.local"
    );
}

#[tokio::test]
async fn local_connector_name_defaults_when_release_and_namespace_are_unset() {
    let _lock = crate::support::ENV_LOCK.lock().await;
    let _release = EnvVar::unset("CURIE_RELEASE");
    let _namespace = EnvVar::unset("CURIE_NAMESPACE");
    let (name, dns) = started(AGENT);
    assert_eq!(name, "curie-acme-bot-mcp-grafana");
    assert_eq!(dns, "curie-acme-bot-mcp-grafana.default.svc.cluster.local");

    // Compose `${VAR:-default}` treats an empty value the same as unset.
    drop(_release);
    drop(_namespace);
    let _release = EnvVar::set("CURIE_RELEASE", "");
    let _namespace = EnvVar::set("CURIE_NAMESPACE", "");
    let (empty_name, empty_dns) = started(AGENT);
    assert_eq!(empty_name, "curie-acme-bot-mcp-grafana");
    assert_eq!(
        empty_dns,
        "curie-acme-bot-mcp-grafana.default.svc.cluster.local"
    );
}

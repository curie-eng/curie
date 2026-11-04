//! Local deploy names hosted connectors with the same release and namespace
//! the compose worker hands the runner.
//!
//! `compose.dev.yaml` passes `CURIE_RELEASE=${CURIE_RELEASE:-curie}` and
//! `CURIE_NAMESPACE=${CURIE_NAMESPACE:-default}` into the worker.
//! `WorkerConfig` reads those two variables. `binding.py` copies them onto
//! the sandbox boot env. The runner's `derive_mcp_servers` calls
//! `plugin_format.connector_render.mcp_entry`, and the hosted URL host is
//! `service_dns`. The local deploy branch calls `bring_up_local` with the
//! resolved agent name. That function reads the two variables with the same
//! compose defaults and writes `object_name` plus `service_dns` into the
//! compose overlay before it starts the container. These tests drive that
//! function and read the overlay. The docker stub exits before any daemon
//! work, after the overlay is on disk.

use std::path::Path;
use std::process::Command;

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

    fn set_os(name: &'static str, value: impl AsRef<std::ffi::OsStr>) -> Self {
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

/// A hosted, secret-free bundle plus a docker stub that never reaches a daemon.
///
/// `bring_up_local` writes the overlay, then asks docker to start it. The stub
/// answers every docker invocation with success and no containers, so readiness
/// fails after the write. `COMPOSE_FILE` without `COMPOSE_PROJECT_NAME` would
/// fail resource lookup before that write.
struct Startup {
    dir: tempfile::TempDir,
    _bin: tempfile::TempDir,
    _path: EnvVar,
    _compose_file: EnvVar,
    _compose_project: EnvVar,
}

fn startup() -> Startup {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::TempDir::new().expect("plugin dir");
    let bin = tempfile::TempDir::new().expect("docker stub dir");
    let docker = bin.path().join("docker");
    std::fs::write(&docker, "#!/bin/sh\nexit 0\n").expect("write the docker stub");
    std::fs::set_permissions(&docker, std::fs::Permissions::from_mode(0o755))
        .expect("make the docker stub executable");
    let mut paths = vec![bin.path().to_path_buf()];
    paths.extend(std::env::split_paths(
        &std::env::var_os("PATH").unwrap_or_default(),
    ));
    let path = std::env::join_paths(paths).expect("join PATH");
    Startup {
        dir,
        _bin: bin,
        _path: EnvVar::set_os("PATH", path),
        _compose_file: EnvVar::unset("COMPOSE_FILE"),
        _compose_project: EnvVar::unset("COMPOSE_PROJECT_NAME"),
    }
}

fn hosted() -> (
    curie::connector_build::ConnectorsFileDecl,
    curie::connector_build::ConnectorLockFileDecl,
) {
    let decl = curie::connector_build::parse_connectors(
        "connectors:\n  grafana:\n    image: ghcr.io/acme-corp/grafana:1\n",
    )
    .expect("a hosted connector declaration");
    let lock = curie::connector_build::ConnectorLockFileDecl {
        version: curie::connector_build::LOCK_VERSION,
        connectors: std::collections::BTreeMap::new(),
        ..Default::default()
    };
    (decl, lock)
}

/// Service key and network alias from the overlay `bring_up_local` wrote.
fn overlay_identity(plugin_dir: &Path, bring_up: &impl std::fmt::Debug) -> (String, String) {
    let path = curie::connector_build::compose_overlay_path(plugin_dir);
    let text = std::fs::read_to_string(&path).unwrap_or_else(|err| {
        panic!(
            "bring-up wrote no overlay at {}: {err}; result: {bring_up:?}",
            path.display()
        )
    });
    let overlay: serde_json::Value =
        serde_norway::from_str(&text).unwrap_or_else(|err| panic!("overlay yaml ({err}): {text}"));
    let services = overlay["services"].as_object().expect("services");
    assert_eq!(services.len(), 1, "{overlay}");
    let (name, service) = services.iter().next().expect("the started service");
    let networks = service["networks"].as_object().expect("networks");
    assert_eq!(networks.len(), 1, "{service}");
    let (_network, attachment) = networks.iter().next().expect("one network");
    let aliases = attachment["aliases"].as_array().expect("aliases");
    assert_eq!(aliases.len(), 1, "{service}");
    let dns = aliases[0].as_str().expect("dns alias").to_string();
    (name.clone(), dns)
}

fn assert_stopped_after_overlay(bring_up: Result<(), anyhow::Error>) {
    let message = format!(
        "{:#}",
        bring_up.expect_err("the docker stub must not report a started container")
    );
    assert!(
        message.contains("was not created"),
        "readiness is the stop after the overlay write: {message}"
    );
}

#[tokio::test]
async fn local_connector_name_matches_the_worker_dial_name() {
    let _lock = crate::support::ENV_LOCK.lock().await;
    let _release = EnvVar::set("CURIE_RELEASE", "acme-rel");
    let _namespace = EnvVar::set("CURIE_NAMESPACE", "acme-ns");
    let (dialed_name, dialed_dns) = worker_dial("acme-rel", "acme-ns");
    let startup = startup();
    let (decl, lock) = hosted();
    let bring_up =
        curie::commands::bring_up_local(startup.dir.path(), &decl, &lock, AGENT, "curie").await;
    let (name, dns) = overlay_identity(startup.dir.path(), &bring_up);
    assert_eq!(name, dialed_name);
    assert_eq!(dns, dialed_dns);
    assert_eq!(name, format!("acme-rel-{AGENT}-mcp-{CONNECTOR}"));
    assert_eq!(
        dns,
        format!("acme-rel-{AGENT}-mcp-{CONNECTOR}.acme-ns.svc.cluster.local")
    );
    assert_stopped_after_overlay(bring_up);
}

#[tokio::test]
async fn local_connector_name_defaults_when_release_and_namespace_are_unset() {
    let _lock = crate::support::ENV_LOCK.lock().await;
    let _release = EnvVar::unset("CURIE_RELEASE");
    let _namespace = EnvVar::unset("CURIE_NAMESPACE");
    {
        let startup = startup();
        let (decl, lock) = hosted();
        let bring_up =
            curie::commands::bring_up_local(startup.dir.path(), &decl, &lock, AGENT, "curie").await;
        let (name, dns) = overlay_identity(startup.dir.path(), &bring_up);
        assert_eq!(name, format!("curie-{AGENT}-mcp-{CONNECTOR}"));
        assert_eq!(
            dns,
            format!("curie-{AGENT}-mcp-{CONNECTOR}.default.svc.cluster.local")
        );
        assert_stopped_after_overlay(bring_up);
    }

    // Compose `${VAR:-default}` treats an empty value the same as unset.
    drop(_release);
    drop(_namespace);
    let _release = EnvVar::set("CURIE_RELEASE", "");
    let _namespace = EnvVar::set("CURIE_NAMESPACE", "");
    let startup = startup();
    let (decl, lock) = hosted();
    let bring_up =
        curie::commands::bring_up_local(startup.dir.path(), &decl, &lock, AGENT, "curie").await;
    let (empty_name, empty_dns) = overlay_identity(startup.dir.path(), &bring_up);
    assert_eq!(empty_name, format!("curie-{AGENT}-mcp-{CONNECTOR}"));
    assert_eq!(
        empty_dns,
        format!("curie-{AGENT}-mcp-{CONNECTOR}.default.svc.cluster.local")
    );
    assert_stopped_after_overlay(bring_up);
}

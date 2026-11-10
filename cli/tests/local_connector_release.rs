//! Local `curie local deploy` names hosted connectors with the same release
//! and namespace the compose worker hands the runner.
//!
//! `compose.dev.yaml` passes `CURIE_RELEASE=${CURIE_RELEASE:-curie}` and
//! `CURIE_NAMESPACE=${CURIE_NAMESPACE:-default}` into the worker.
//! `WorkerConfig` reads those two variables. `binding.py` copies them onto
//! the sandbox boot env. The runner's `derive_mcp_servers` calls
//! `plugin_format.connector_render.mcp_entry`, and the hosted URL host is
//! `service_dns`. These tests run the built `curie local deploy` command for
//! a connector bundle on the local tier. The platform API is a wire stub.
//! A docker shim on `PATH` records every invocation and exits before any
//! daemon work, after deploy has written the compose overlay. The service
//! key, the DNS alias, and the compose service the shim recorded are the
//! name that deploy started.

use std::path::Path;
use std::process::{Command, Output};

use crate::support::{serve, Response};

const AGENT: &str = "acme-bot";
const CONNECTOR: &str = "grafana";
const AGENT_ID: &str = "11111111-1111-1111-1111-111111111111";
const VERSION_ID: &str = "22222222-2222-2222-2222-222222222222";
const DEPLOYMENT_ID: &str = "33333333-3333-3333-3333-333333333333";

/// Release and namespace handed to one `curie local deploy` child.
enum ComposeEnv<'a> {
    /// Set both variables, including the empty string.
    Values(&'a str, &'a str),
    /// Leave both unset, so compose `${VAR:-default}` applies.
    Unset,
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

/// Platform answers for the writes `local deploy` sends before it starts
/// connectors. The agent name is the one the runner dials.
fn serve_platform() -> crate::support::MockServer {
    let agent = serde_json::json!({
        "id": AGENT_ID,
        "name": AGENT,
        "channels": [{"kind": "slack", "address": "C0LOCALDEV"}],
        "approval_required_tools": null,
        "approval_routes": null,
        "memory": false,
    })
    .to_string();
    let listed = format!("[{agent}]");
    serve(move |req| {
        let path = req.path.split('?').next().unwrap_or_default();
        let agent_path = format!("/agents/{AGENT_ID}");
        let versions = format!("{agent_path}/versions");
        let bundle = format!("{versions}/{VERSION_ID}/bundle");
        match (req.method.as_str(), path) {
            ("GET", "/agents") => Response::json(200, &listed),
            ("PATCH", p) if p == agent_path => Response::json(200, &agent),
            ("POST", p) if p == versions => Response::json(
                201,
                &format!(r#"{{"id":"{VERSION_ID}","version_label":"0.1.0-1"}}"#),
            ),
            ("PUT", p) if p == bundle => Response::json(
                201,
                r#"{"bundle_ref":"bundles/x.tar.gz","bundle_sha256":"deadbeef","size_bytes":512}"#,
            ),
            ("POST", "/deployments") => Response::json(
                201,
                &format!(r#"{{"id":"{DEPLOYMENT_ID}","environment":"dev","status":"active"}}"#),
            ),
            _ => Response::json(
                500,
                &format!(r#"{{"detail":"unmodeled {} {}"}}"#, req.method, path),
            ),
        }
    })
}

fn write_docker_shim(dir: &Path, record: &Path) {
    use std::os::unix::fs::PermissionsExt;

    let record = record.display().to_string();
    assert!(!record.contains('\''), "the shim quotes the record path");
    let script = format!("#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{record}'\nexit 0\n");
    let docker = dir.join("docker");
    std::fs::write(&docker, script).expect("write the docker shim");
    std::fs::set_permissions(&docker, std::fs::Permissions::from_mode(0o755))
        .expect("make the docker shim executable");
}

struct Deployed {
    plugin: tempfile::TempDir,
    output: Output,
    record: String,
}

/// Run `curie local deploy` for a hosted, secret-free grafana bundle.
///
/// The child inherits nothing that would select an isolated compose project,
/// and its `PATH` starts with a docker shim that records argv and exits 0.
/// Readiness then fails because the shim created no container. The overlay
/// is already on disk.
fn local_deploy(env: ComposeEnv<'_>) -> Deployed {
    let plugin = tempfile::tempdir().expect("plugin dir");
    curie::scaffold::scaffold(plugin.path(), AGENT).expect("scaffold the bundle");
    std::fs::write(
        plugin.path().join("connectors.yaml"),
        "connectors:\n  grafana:\n    image: ghcr.io/acme-corp/grafana:1\n",
    )
    .expect("write the hosted connector");

    let server = serve_platform();
    let shim = tempfile::tempdir().expect("docker shim dir");
    let record_path = plugin.path().join("docker.log");
    write_docker_shim(shim.path(), &record_path);
    let mut paths = vec![shim.path().to_path_buf()];
    paths.extend(std::env::split_paths(
        &std::env::var_os("PATH").unwrap_or_default(),
    ));
    let path = std::env::join_paths(paths).expect("join PATH");

    let mut cmd = Command::new(env!("CARGO_BIN_EXE_curie"));
    cmd.arg("--debug")
        .args(["local", "deploy"])
        .arg("--plugin-dir")
        .arg(plugin.path())
        .arg("--api-url")
        .arg(&server.base_url)
        .arg("--api-key")
        .arg("test-key")
        .arg("--agent")
        .arg(AGENT)
        .arg("--label")
        .arg("0.1.0-1")
        .env("PATH", path)
        .env("CURIE_API_URL", &server.base_url)
        .env("CURIE_API_KEY", "test-key")
        .env_remove("COMPOSE_FILE")
        .env_remove("COMPOSE_PROJECT_NAME")
        .env_remove("CURIE_CONNECTOR_START_TIMEOUT_SECONDS");
    match env {
        ComposeEnv::Values(release, namespace) => {
            cmd.env("CURIE_RELEASE", release);
            cmd.env("CURIE_NAMESPACE", namespace);
        }
        ComposeEnv::Unset => {
            cmd.env_remove("CURIE_RELEASE");
            cmd.env_remove("CURIE_NAMESPACE");
        }
    }
    let output = cmd.output().expect("run curie local deploy");
    let record = std::fs::read_to_string(&record_path).unwrap_or_default();
    Deployed {
        plugin,
        output,
        record,
    }
}

/// Service key and network alias from the overlay local deploy wrote.
fn overlay_identity(plugin_dir: &Path, context: &str) -> (String, String) {
    let path = curie::connector_build::compose_overlay_path(plugin_dir);
    let text = std::fs::read_to_string(&path).unwrap_or_else(|err| {
        panic!(
            "local deploy wrote no overlay at {}: {err}; output: {context}",
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

fn command_text(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn assert_deployed_name(deployed: &Deployed, name: &str, dns: &str) {
    let text = command_text(&deployed.output);
    assert!(
        !deployed.output.status.success(),
        "the docker shim must not report a started container: {text}"
    );
    assert!(
        text.contains("was not created"),
        "readiness is the stop after the overlay write: {text}"
    );
    let (got_name, got_dns) = overlay_identity(deployed.plugin.path(), &text);
    assert_eq!(got_name, name);
    assert_eq!(got_dns, dns);
    let overlay = curie::connector_build::compose_overlay_path(deployed.plugin.path());
    assert!(
        deployed
            .record
            .contains(&format!("-f {}", overlay.display())),
        "local deploy must pass the overlay to docker compose: {}",
        deployed.record
    );
    assert!(
        deployed
            .record
            .lines()
            .any(|line| line.contains(" ps ") && line.ends_with(name)),
        "local deploy must ask docker for the connector service {name}: {}",
        deployed.record
    );
}

#[tokio::test]
async fn local_connector_name_matches_the_worker_dial_name() {
    let _lock = crate::support::ENV_LOCK.lock().await;
    let (dialed_name, dialed_dns) = worker_dial("acme-rel", "acme-ns");
    let deployed = local_deploy(ComposeEnv::Values("acme-rel", "acme-ns"));
    assert_deployed_name(&deployed, &dialed_name, &dialed_dns);
    assert_eq!(dialed_name, format!("acme-rel-{AGENT}-mcp-{CONNECTOR}"));
    assert_eq!(
        dialed_dns,
        format!("acme-rel-{AGENT}-mcp-{CONNECTOR}.acme-ns.svc.cluster.local")
    );
}

#[tokio::test]
async fn local_connector_name_defaults_when_release_and_namespace_are_unset() {
    let _lock = crate::support::ENV_LOCK.lock().await;
    let name = format!("curie-{AGENT}-mcp-{CONNECTOR}");
    let dns = format!("curie-{AGENT}-mcp-{CONNECTOR}.default.svc.cluster.local");
    assert_deployed_name(&local_deploy(ComposeEnv::Unset), &name, &dns);

    // Compose `${VAR:-default}` treats an empty value the same as unset.
    assert_deployed_name(&local_deploy(ComposeEnv::Values("", "")), &name, &dns);
}

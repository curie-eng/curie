//! Preflight for the Docker minimums a connector `secret_files` subpath
//! mount needs (issue 4671).
//!
//! The minimums come from the providers own sources, as AGENTS.md requires
//! for assertions about an external API shape. Docker Engine 26.0 release
//! notes add `VolumeOptions.Subpath` and the `volume-subpath` mount option
//! (docker/cli 4331), and the moby `docs/api/version-history.md` at v26.0.0
//! lists that change under Engine API v1.45. The docker/compose
//! `pkg/compose/create.go` first passes a volume `subpath` to the engine at
//! v2.26.0 and has no such code at v2.25.0.

use crate::support::executable as test_executable;

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::Path;

use curie::connector_build::{
    docker_subpath_preflight, ConnectorSecretVolumes, ConnectorSpecDecl, ConnectorsFileDecl,
};

fn prepend_path(dir: &Path) -> Option<OsString> {
    let previous = std::env::var_os("PATH");
    let mut paths = vec![dir.to_path_buf()];
    paths.extend(std::env::split_paths(&previous.clone().unwrap_or_default()));
    std::env::set_var("PATH", std::env::join_paths(paths).expect("PATH"));
    previous
}

fn restore_path(previous: Option<OsString>) {
    match previous {
        Some(value) => std::env::set_var("PATH", value),
        None => std::env::remove_var("PATH"),
    }
}

fn decl_with_secret_files() -> ConnectorsFileDecl {
    let mut spec = ConnectorSpecDecl {
        image: Some("ghcr.io/acme-corp/acme-vault:latest".to_string()),
        ..ConnectorSpecDecl::default()
    };
    spec.secret_files.insert(
        "ACME_TOKEN".to_string(),
        "/run/secrets/acme-token".to_string(),
    );
    let mut connectors = BTreeMap::new();
    connectors.insert("acme-vault".to_string(), spec);
    ConnectorsFileDecl {
        connectors,
        runner: None,
    }
}

fn decl_without_secret_files() -> ConnectorsFileDecl {
    let spec = ConnectorSpecDecl {
        image: Some("ghcr.io/acme-corp/acme-vault:latest".to_string()),
        ..ConnectorSpecDecl::default()
    };
    let mut connectors = BTreeMap::new();
    connectors.insert("acme-vault".to_string(), spec);
    ConnectorsFileDecl {
        connectors,
        runner: None,
    }
}

fn install_docker_stub(bin: &Path, log: &Path, engine_api: &str, compose_version: &str) {
    let script = format!(
        "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{log}'\ncase \"$*\" in\n  \"version --format {{{{.Server.APIVersion}}}}\") printf '%s\\n' '{engine_api}' ;;\n  \"compose version --short\") printf '%s\\n' '{compose_version}' ;;\n  *) printf 'unexpected docker invocation: %s\\n' \"$*\" >&2; exit 64 ;;\nesac\n",
        log = log.display()
    );
    test_executable::install_in(bin, "docker", &script);
}

fn read_log(log: &Path) -> String {
    std::fs::read_to_string(log).unwrap_or_default()
}

#[tokio::test]
async fn docker_subpath_preflight_refuses_old_engine_for_skill_tier() {
    let _lock = crate::support::ENV_LOCK.lock().await;
    let bin = tempfile::tempdir().expect("bin");
    let log = bin.path().join("docker.log");
    install_docker_stub(bin.path(), &log, "1.44", "v2.26.0");
    let previous = prepend_path(bin.path());
    let volumes = ConnectorSecretVolumes::new(&decl_with_secret_files());
    let result = docker_subpath_preflight(&volumes, false).await;
    restore_path(previous);
    let err = result.expect_err("Engine API 1.44 must be refused");
    let text = format!("{err:#}");
    for needle in ["Docker Engine 26.0", "1.45", "1.44", "acme-vault"] {
        assert!(
            text.contains(needle),
            "{needle} missing from refusal: {text}"
        );
    }
    let log_text = read_log(&log);
    assert!(
        log_text.contains("version --format"),
        "the engine check must run: {log_text}"
    );
    assert!(
        !log_text.contains("volume"),
        "no volume may be created before the refusal: {log_text}"
    );
}

#[tokio::test]
async fn docker_subpath_preflight_accepts_engine_145_for_skill_tier() {
    let _lock = crate::support::ENV_LOCK.lock().await;
    let bin = tempfile::tempdir().expect("bin");
    let log = bin.path().join("docker.log");
    install_docker_stub(bin.path(), &log, "1.45", "v2.25.0");
    let previous = prepend_path(bin.path());
    let volumes = ConnectorSecretVolumes::new(&decl_with_secret_files());
    let result = docker_subpath_preflight(&volumes, false).await;
    restore_path(previous);
    result.expect("Engine API 1.45 must pass the skill tier check");
    let log_text = read_log(&log);
    assert!(
        !log_text.contains("compose version"),
        "the skill tier must not check Compose: {log_text}"
    );
}

#[tokio::test]
async fn docker_subpath_preflight_refuses_unparseable_engine_version() {
    let _lock = crate::support::ENV_LOCK.lock().await;
    let bin = tempfile::tempdir().expect("bin");
    let log = bin.path().join("docker.log");
    install_docker_stub(bin.path(), &log, "banana", "v2.26.0");
    let previous = prepend_path(bin.path());
    let volumes = ConnectorSecretVolumes::new(&decl_with_secret_files());
    let result = docker_subpath_preflight(&volumes, false).await;
    restore_path(previous);
    let err = result.expect_err("an unparseable Engine API version must be refused");
    let text = format!("{err:#}");
    assert!(text.contains("'banana'"), "raw text must be quoted: {text}");
}

#[tokio::test]
async fn docker_subpath_preflight_refuses_old_compose_for_local_tier() {
    let _lock = crate::support::ENV_LOCK.lock().await;
    let bin = tempfile::tempdir().expect("bin");
    let log = bin.path().join("docker.log");
    install_docker_stub(bin.path(), &log, "1.56", "v2.25.0");
    let previous = prepend_path(bin.path());
    let volumes = ConnectorSecretVolumes::new(&decl_with_secret_files());
    let result = docker_subpath_preflight(&volumes, true).await;
    restore_path(previous);
    let err = result.expect_err("Compose v2.25.0 must be refused");
    let text = format!("{err:#}");
    for needle in ["Docker Compose 2.26.0", "2.25.0", "acme-vault"] {
        assert!(
            text.contains(needle),
            "{needle} missing from refusal: {text}"
        );
    }
}

#[tokio::test]
async fn docker_subpath_preflight_accepts_newer_compose_versions() {
    let _lock = crate::support::ENV_LOCK.lock().await;
    let bin = tempfile::tempdir().expect("bin");
    let log = bin.path().join("docker.log");
    let volumes = ConnectorSecretVolumes::new(&decl_with_secret_files());
    for compose_version in ["v2.26.0", "2.27.1", "5.1.3"] {
        install_docker_stub(bin.path(), &log, "1.56", compose_version);
        let previous = prepend_path(bin.path());
        let result = docker_subpath_preflight(&volumes, true).await;
        restore_path(previous);
        assert!(
            result.is_ok(),
            "Compose {compose_version} must pass: {result:?}"
        );
    }
}

#[tokio::test]
async fn docker_subpath_preflight_refuses_unparseable_compose_version() {
    let _lock = crate::support::ENV_LOCK.lock().await;
    let bin = tempfile::tempdir().expect("bin");
    let log = bin.path().join("docker.log");
    install_docker_stub(bin.path(), &log, "1.56", "banana");
    let previous = prepend_path(bin.path());
    let volumes = ConnectorSecretVolumes::new(&decl_with_secret_files());
    let result = docker_subpath_preflight(&volumes, true).await;
    restore_path(previous);
    let err = result.expect_err("an unparseable Compose version must be refused");
    let text = format!("{err:#}");
    assert!(text.contains("'banana'"), "raw text must be quoted: {text}");
}

#[tokio::test]
async fn docker_subpath_preflight_skips_checks_without_secret_files() {
    let _lock = crate::support::ENV_LOCK.lock().await;
    let bin = tempfile::tempdir().expect("bin");
    let log = bin.path().join("docker.log");
    install_docker_stub(bin.path(), &log, "1.44", "v2.25.0");
    let previous = prepend_path(bin.path());
    let volumes = ConnectorSecretVolumes::new(&decl_without_secret_files());
    let skill_result = docker_subpath_preflight(&volumes, false).await;
    let local_result = docker_subpath_preflight(&volumes, true).await;
    restore_path(previous);
    skill_result.expect("no secret_files means no engine check on the skill tier");
    local_result.expect("no secret_files means no version checks on the local tier");
    let log_text = read_log(&log);
    assert!(
        log_text.is_empty(),
        "no docker version call may run without secret_files: {log_text}"
    );
}

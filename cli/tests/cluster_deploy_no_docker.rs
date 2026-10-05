//! `curie cluster deploy` of a bundle with a registry-delivered runner layer
//! must not need docker (#3503).
//!
//! The deploy proves the layer's recorded base is the installation's runner by
//! pinning the release's runner reference to a registry digest. These tests
//! drive the real binary with PATH holding only helm and kubectl stubs (no
//! docker anywhere), a stub OCI registry behind an anonymous bearer challenge,
//! and the mock platform API. They prove:
//!
//! - a matching base deploys, and the digest came from the registry's token
//!   flow rather than a docker call;
//! - a mismatched base is refused with the installation's pinned digest and
//!   nothing is written to the API;
//! - a registry that cannot answer is refused with the chart value
//!   `agentSandbox.runner.digest` named as the way to supply the digest.

#[path = "support/oci_registry_stub.rs"]
mod oci_registry_stub;
mod support;

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::{Command, Output};

use curie::scaffold::scaffold;
use oci_registry_stub::{runner_index, sha256_digest, OciRegistryStub, RUNNER_REPO, RUNNER_TAG};
use serde_json::{json, Value};
use support::{serve, MockServer, Response};

const LABEL: &str = "v0.11.0-no-docker-test";

const RUNNER_DECL: &str = "connectors: {}\nrunner:\n  build:\n    context: runner\n    \
                           platforms: [linux/amd64, linux/arm64]\n";

const RUNNER_DOCKERFILE: &str =
    "ARG CURIE_RUNNER_IMAGE\nFROM ${CURIE_RUNNER_IMAGE}\nRUN pip install acme-tools\n";

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn agent_json() -> Value {
    json!({
        "id": "agent-acme-bot",
        "name": "acme-bot",
        "channels": [{"kind": "slack", "address": "C0EXAMPLE1"}],
        "created_at": "2026-09-29T00:00:00Z",
        "memory": false
    })
}

fn deploy_response(req: &support::Request) -> Response {
    match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => Response::json(200, &json!([agent_json()]).to_string()),
        ("POST", "/agents") => Response::json(201, &agent_json().to_string()),
        ("GET", "/deployments?agent_id=agent-acme-bot") => Response::json(200, "[]"),
        ("POST", path) if path.ends_with("/versions") => Response::json(
            201,
            &json!({
                "id": "version-acme-bot",
                "agent_id": "agent-acme-bot",
                "version_label": LABEL,
                "created_by": "tester",
                "created_at": "2026-09-29T00:00:00Z"
            })
            .to_string(),
        ),
        ("PUT", path) if path.ends_with("/bundle") => Response::json(
            201,
            &json!({
                "version_id": "version-acme-bot",
                "bundle_ref": "bundles/acme-bot.tar.gz",
                "bundle_sha256": "sha-acme-bot",
                "size_bytes": 100
            })
            .to_string(),
        ),
        ("PATCH", "/agents/agent-acme-bot") => Response::json(200, &agent_json().to_string()),
        ("GET", path) if path.contains("/versions/") && path.contains("/connectors?") => {
            Response::json(
                200,
                &json!({
                    "manifests": [],
                    "owned_secret_name": "",
                    "owned_secret_keys": [],
                    "mcp_entries": {},
                    "version_id": "version-acme-bot",
                    "triggers": []
                })
                .to_string(),
            )
        }
        ("GET", path) if path.starts_with("/deployments") => Response::json(200, "[]"),
        ("POST", "/deployments") => Response::json(
            201,
            &json!({
                "id": "deployment-acme-bot",
                "agent_id": "agent-acme-bot",
                "version_id": "version-acme-bot",
                "environment": "dev",
                "workspace_enabled": false,
                "status": "active",
                "deployed_at": "2026-09-29T00:00:00Z"
            })
            .to_string(),
        ),
        (method, path) => Response::json(500, &format!("unexpected {method} {path}")),
    }
}

fn make_executable(path: &Path) {
    let mut permissions = fs::metadata(path).expect("stub metadata").permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).expect("make stub executable");
}

/// A helm that answers the release reads with shell builtins only, since PATH
/// holds nothing but the stub directory. `history` serves revision 3 as
/// deployed; every `get values` (with or without `--revision`/`--all`) gets
/// the same computed values, so the other deploy paths that read them agree.
fn write_helm_stub(dir: &Path, values: &str) {
    let path = dir.join("helm");
    let log = dir.join("helm.log");
    let script = format!(
        r#"#!/bin/sh
printf '%s\n' "$*" >> '{log}'
case "$*" in
  "history "*)
    printf '%s\n' '[{{"revision":3,"status":"deployed"}}]' ;;
  "get metadata "*)
    printf '%s\n' '{{"appVersion":"{tag}"}}' ;;
  "get values "*)
    printf '%s\n' '{values}' ;;
  *) printf 'unexpected helm invocation: %s\n' "$*" >&2; exit 64 ;;
esac
"#,
        log = log.display(),
        tag = RUNNER_TAG,
        values = values.replace('\'', "'\\''")
    );
    fs::write(&path, script).expect("write helm stub");
    make_executable(&path);
}

fn write_kubectl_stub(dir: &Path) {
    let path = dir.join("kubectl");
    fs::write(
        &path,
        r#"#!/bin/sh
case "$*" in
  "config view --minify --raw -o json")
    printf '%s\n' '{"clusters":[{"cluster":{"server":"https://cluster.example.com","certificate-authority-data":"Y2E="}}]}' ;;
  *"get deployment"*) printf '%s' 'curie' ;;
  *"delete deployment,service,networkpolicy,secret"*) exit 0 ;;
  *"delete sandboxclaim -l curietech.ai/agent=acme-bot"*) exit 0 ;;
  *) printf 'unexpected kubectl invocation: %s\n' "$*" >&2; exit 64 ;;
esac
"#,
    )
    .expect("write kubectl stub");
    make_executable(&path);
}

/// A scaffolded bundle declaring a runner layer, locked as registry-delivered
/// on `base` with a source digest that matches the tree, so only the base
/// check can refuse it. Returns the locked layer reference.
fn write_layered_bundle(plugin: &Path, registry_host: &str, base: &str) -> String {
    scaffold(plugin, "acme-bot").expect("scaffold bundle");
    fs::write(
        plugin.join(curie::connector_build::CONNECTORS_FILE),
        RUNNER_DECL,
    )
    .expect("write connectors.yaml");
    fs::create_dir_all(plugin.join("runner")).expect("runner context");
    fs::write(plugin.join("runner/Dockerfile"), RUNNER_DOCKERFILE).expect("write Dockerfile");
    let decl = curie::connector_build::load(plugin).expect("load runner decl");
    let digests =
        curie::commands::recompute_source_digests(plugin, &decl).expect("recompute digests");
    let fresh = digests
        .get(curie::connector_build::RUNNER_DIGEST_KEY)
        .expect("the runner layer has a source digest");
    let layer = format!(
        "{registry_host}/acme/acme-bot-runner@sha256:{}",
        "a".repeat(64)
    );
    fs::write(
        plugin.join(curie::connector_build::CONNECTOR_LOCK_FILE),
        format!(
            "version: 1\nconnectors: {{}}\nrunner:\n  image: {layer}\n  base: {base}\n  \
             delivery: registry\n  platforms: [linux/amd64, linux/arm64]\n  \
             source_digest: {fresh}\n"
        ),
    )
    .expect("write connectors.lock.yaml");
    layer
}

struct Run {
    output: Output,
    api: MockServer,
    helm_log: String,
}

impl Run {
    fn stderr(&self) -> String {
        String::from_utf8_lossy(&self.output.stderr).into_owned()
    }

    fn deployment_posts(&self) -> usize {
        self.api
            .recorded()
            .iter()
            .filter(|r| r.method == "POST" && r.path == "/deployments")
            .count()
    }
}

/// Deploy a layered bundle whose lock records `base` into a release whose
/// runner values name `runner_image:RUNNER_TAG`, with docker absent.
fn deploy_without_docker(registry: &OciRegistryStub, runner_image: &str, base: &str) -> Run {
    let plugin = tempfile::tempdir().expect("plugin tempdir");
    let layer = write_layered_bundle(plugin.path(), &registry.host, base);

    let tools = tempfile::tempdir().expect("tool tempdir");
    write_kubectl_stub(tools.path());
    let values = json!({
        "agentSandbox": {
            "runner": {"image": runner_image, "tag": RUNNER_TAG, "digest": ""},
            // The release already binds this layer, so a passing deploy
            // needs no chart upgrade and the stub helm stays read-only.
            "runnerImages": {"acme-bot": layer}
        },
        "api": {"githubRepoAllowlist": []}
    });
    write_helm_stub(tools.path(), &values.to_string());
    assert!(
        !tools.path().join("docker").exists(),
        "the child PATH must hold no docker"
    );

    let api = serve(deploy_response);
    let output = Command::new(bin())
        .args(["cluster", "deploy", "--plugin-dir"])
        .arg(plugin.path())
        .args([
            "--api-url",
            &api.base_url,
            "--api-key",
            "test-key",
            "--namespace",
            "curie",
            "--release",
            "curie",
            "--agent",
            "acme-bot",
            "--env",
            "dev",
            "--slack-channel",
            "C0EXAMPLE1",
            "--label",
            LABEL,
        ])
        // Only the stub directory: no docker can be found by any lookup.
        .env("PATH", tools.path())
        .env_remove("CURIE_API_URL")
        .env_remove("CURIE_API_KEY")
        .output()
        .expect("run cluster deploy");
    let helm_log = fs::read_to_string(tools.path().join("helm.log")).unwrap_or_default();
    Run {
        output,
        api,
        helm_log,
    }
}

fn expected_runner_digest() -> String {
    sha256_digest(&runner_index())
}

/// AC1: the base matches the installation's runner, resolved natively through
/// the registry's bearer challenge, and the deploy goes through.
#[test]
fn a_matching_base_deploys_without_docker() {
    let registry = OciRegistryStub::runner();
    let runner = registry.image(RUNNER_REPO);
    let base = format!("{runner}@{}", expected_runner_digest());
    let run = deploy_without_docker(&registry, &runner, &base);

    assert!(
        run.output.status.success(),
        "a matching base must deploy with no docker on PATH: {}\nhelm calls:\n{}",
        run.stderr(),
        run.helm_log
    );
    assert_eq!(
        run.deployment_posts(),
        1,
        "the deploy must reach POST /deployments"
    );
    assert!(
        registry.saw_token_request(RUNNER_REPO),
        "the digest must come from the registry's anonymous token flow: {:?}",
        registry.recorded()
    );
    assert!(
        registry.saw_authorized_manifest_get(RUNNER_REPO, RUNNER_TAG),
        "the manifest must be read with the issued token: {:?}",
        registry.recorded()
    );
}

/// AC2: a layer built on another base is refused without docker, naming the
/// installation's digest-pinned runner, before anything reaches the API.
#[test]
fn a_mismatched_base_is_refused_with_the_installed_digest_and_no_api_write() {
    let registry = OciRegistryStub::runner();
    let runner = registry.image(RUNNER_REPO);
    let base = format!("{runner}@sha256:{}", "b".repeat(64));
    let run = deploy_without_docker(&registry, &runner, &base);

    assert!(
        !run.output.status.success(),
        "a mismatched base must be refused"
    );
    let stderr = run.stderr();
    let pinned = format!("{runner}@{}", expected_runner_digest());
    assert!(
        stderr.contains(&pinned),
        "the refusal must name the installation's pinned runner {pinned}: {stderr}"
    );
    assert_eq!(run.deployment_posts(), 0, "no deployment may be created");
    assert!(
        run.api.recorded().is_empty(),
        "the refusal must precede every API call: {:?}",
        run.api
            .recorded()
            .iter()
            .map(|r| format!("{} {}", r.method, r.path))
            .collect::<Vec<_>>()
    );
}

/// The registry cannot answer for the installed runner (the tag is unknown),
/// and docker is absent: the refusal names the chart value that supplies the
/// digest so no lookup is needed.
#[test]
fn an_unresolvable_runner_is_refused_naming_the_digest_chart_value() {
    let registry = OciRegistryStub::runner();
    // A repository the stub does not serve: the manifest read is a 404.
    let runner = registry.image("curie-eng/curie-runner-missing");
    let base = format!(
        "{}@{}",
        registry.image(RUNNER_REPO),
        expected_runner_digest()
    );
    let run = deploy_without_docker(&registry, &runner, &base);

    assert!(
        !run.output.status.success(),
        "an unresolvable runner must be refused"
    );
    let stderr = run.stderr();
    assert!(
        stderr.contains("agentSandbox.runner.digest"),
        "the refusal must name the chart value that supplies the digest: {stderr}"
    );
    assert_eq!(run.deployment_posts(), 0, "no deployment may be created");
}

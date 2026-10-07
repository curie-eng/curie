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
    deploy_response_for_agent(req, &agent_json())
}

fn deploy_response_for_agent(req: &support::Request, agent: &Value) -> Response {
    let agent_id = agent["id"].as_str().expect("agent id");
    match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => Response::json(200, &json!([agent]).to_string()),
        ("POST", "/agents") => Response::json(201, &agent.to_string()),
        ("GET", path) if path == format!("/deployments?agent_id={agent_id}") => {
            Response::json(200, "[]")
        }
        ("POST", path) if path.ends_with("/versions") => Response::json(
            201,
            &json!({
                "id": "version-acme-bot",
                "agent_id": agent_id,
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
        ("PATCH", path) if path == format!("/agents/{agent_id}") => {
            Response::json(200, &agent.to_string())
        }
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
                "agent_id": agent_id,
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
  *"delete sandboxclaim -l curietech.ai/agent=acme-bot"*|*"delete sandboxclaim -l curietech.ai/agent=acme-deployed"*) exit 0 ;;
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
    fn stdout(&self) -> String {
        String::from_utf8_lossy(&self.output.stdout).into_owned()
    }

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
    let config = tempfile::tempdir().expect("config tempdir");
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
        .env("CURIE_CONFIG_DIR", config.path())
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

const CRON_AGENT: &str = "acme-deployed";
const CRON_AGENT_ID: &str = "00000000-0000-4000-8000-000000000001";

/// AgentOut and ChannelBindingOut fixtures follow apps/api/openapi.json:
/// nullable required fields are present, and adapter is part of the read shape.
fn cron_agent(channels: Value) -> Value {
    json!({
        "id": CRON_AGENT_ID,
        "name": CRON_AGENT,
        "channels": channels,
        "repo_full_name": null,
        "behavior_packs": null,
        "model": null,
        "thinking": null,
        "approval_required_tools": null,
        "approval_routes": null,
        "hook_partitions": null,
        "source_bindings": null,
        "secrets": null,
        "memory": false,
        "created_at": "2026-09-29T00:00:00Z"
    })
}

fn cron(name: &str, schedule: &str, zone: Option<&str>, target: Option<&str>) -> Value {
    let mut trigger = json!({
        "type": "cron",
        "name": name,
        "schedule": schedule,
        "prompt": "Report the scheduled result."
    });
    if let Some(zone) = zone {
        trigger["timezone"] = json!(zone);
    }
    if let Some(target) = target {
        trigger["target"] = json!(target);
    }
    trigger
}

fn cron_warning(name: &str, target: &str) -> String {
    format!(
        "cron trigger `{name}` targets `{target}`, which matches no single channel bound to \
         `{CRON_AGENT}`; every slot records failed until that address is bound."
    )
}

fn write_triggers(plugin: &Path, triggers: Value) -> Value {
    let path = plugin.join(".claude-plugin/plugin.json");
    let mut manifest: Value =
        serde_json::from_slice(&fs::read(&path).expect("read scaffold manifest"))
            .expect("scaffold manifest JSON");
    manifest["triggers"] = triggers;
    fs::write(&path, manifest.to_string()).expect("write cron declarations");
    manifest
}

struct CronDeployFixture {
    triggers: Value,
    channels: Value,
    added_channels: Option<Value>,
    slack_channel: Option<&'static str>,
    json: bool,
}

impl CronDeployFixture {
    fn new(triggers: Value, json: bool) -> Self {
        Self {
            triggers,
            channels: json!([{"kind": "slack", "address": "C0EXAMPLE1"}]),
            added_channels: None,
            slack_channel: None,
            json,
        }
    }
}

/// Invoke the real cluster deploy command with an external API peer and no
/// Docker. Configuration and shell stubs belong only to this invocation.
fn deploy_cron_fixture(fixture: CronDeployFixture, configure: impl FnOnce(&Path)) -> Run {
    let plugin = tempfile::tempdir().expect("cron plugin tempdir");
    scaffold(plugin.path(), "acme-bot").expect("scaffold cron bundle");
    write_triggers(plugin.path(), fixture.triggers);
    configure(plugin.path());

    let tools = tempfile::tempdir().expect("cron tool tempdir");
    let config = tempfile::tempdir().expect("cron config tempdir");
    write_kubectl_stub(tools.path());
    write_helm_stub(
        tools.path(),
        &json!({"api": {"githubRepoAllowlist": ["acme-corp/acme-bot"]}}).to_string(),
    );

    let initial = cron_agent(fixture.channels);
    let updated = fixture.added_channels.map(cron_agent);
    let api = serve(move |req| {
        if req.method == "POST" && req.path == format!("/agents/{CRON_AGENT_ID}/channels") {
            return match &updated {
                Some(agent) => Response::json(201, &agent.to_string()),
                None => Response::json(500, "unexpected channel add"),
            };
        }
        deploy_response_for_agent(req, &initial)
    });
    let mut command = Command::new(bin());
    command
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
            CRON_AGENT,
            "--env",
            "dev",
            "--label",
            LABEL,
        ])
        .env("PATH", tools.path())
        .env("CURIE_CONFIG_DIR", config.path())
        .env("NO_COLOR", "1")
        .env_remove("CURIE_API_URL")
        .env_remove("CURIE_API_KEY");
    if let Some(channel) = fixture.slack_channel {
        command.args(["--slack-channel", channel]);
    }
    if fixture.json {
        command.arg("--json");
    }
    let output = command.output().expect("run cron cluster deploy");
    let helm_log = fs::read_to_string(tools.path().join("helm.log")).unwrap_or_default();
    Run {
        output,
        api,
        helm_log,
    }
}

fn assert_cron_deployed(run: &Run) {
    assert_eq!(
        run.output.status.code(),
        Some(0),
        "cron advice must preserve deploy success: {}\n{}",
        run.stdout(),
        run.stderr()
    );
    assert_eq!(
        run.deployment_posts(),
        1,
        "one deployment must be activated"
    );
    let request = run
        .api
        .recorded()
        .into_iter()
        .find(|request| request.method == "POST" && request.path == "/deployments")
        .expect("deployment request");
    let body: Value = serde_json::from_slice(&request.body).expect("deployment JSON");
    assert_eq!(body["agent_id"], json!(CRON_AGENT_ID));
    assert_eq!(body["environment"], json!("dev"));
}

fn cron_json(run: &Run) -> Value {
    assert_cron_deployed(run);
    let value: Value = serde_json::from_slice(&run.output.stdout)
        .unwrap_or_else(|err| panic!("one JSON result is required: {err}: {}", run.stdout()));
    assert_eq!(value["agent"]["name"], json!(CRON_AGENT));
    let schema: Value = serde_json::from_str(include_str!("../schema/deploy.schema.json"))
        .expect("deploy schema JSON");
    assert_eq!(
        schema["$id"],
        json!("https://schemas.curietech.ai/cli/deploy/v1.2.json")
    );
    let validator = jsonschema::validator_for(&schema).expect("deploy schema compiles");
    assert!(
        validator.is_valid(&value),
        "the command's cron receipt must validate against deploy v1.2: {value}"
    );
    value
}

fn assert_human_cron_row(run: &Run, name: &str, schedule: &str, zone: &str, target: &str) {
    let stdout = run.stdout();
    let row = stdout
        .lines()
        .find(|line| line.contains(name) && line.contains(schedule))
        .unwrap_or_else(|| panic!("cron receipt must include {name} and {schedule}: {stdout}"));
    assert!(
        row.contains(zone),
        "cron receipt must include zone {zone}: {row}"
    );
    assert!(
        row.contains(target),
        "cron receipt must include target {target}: {row}"
    );
}

/// #4009: the advisory names the resolved agent, leaves activation successful,
/// and accompanies the receipt for every declaration, including targetless cron.
#[test]
fn unbound_cron_target_warns_after_successful_cluster_deploy() {
    let run = deploy_cron_fixture(
        CronDeployFixture::new(
            json!([
                cron("nightly", "0 2 * * *", Some("Etc/UTC"), Some("C0NOTBOUND1")),
                cron("morning", "15 9 * * 1-5", Some("America/New_York"), Some("C0EXAMPLE1")),
                cron("maintenance", "0 0 * * 0", None, None),
                {"type": "webhook", "name": "inbound", "path": "/events"}
            ]),
            false,
        ),
        |_| {},
    );
    assert_cron_deployed(&run);
    let warning = cron_warning("nightly", "C0NOTBOUND1");
    assert_eq!(
        run.stderr().matches(&warning).count(),
        1,
        "the exact advisory must appear once after success: {}",
        run.stderr()
    );
    assert_human_cron_row(&run, "nightly", "0 2 * * *", "Etc/UTC", "C0NOTBOUND1");
    assert_human_cron_row(
        &run,
        "morning",
        "15 9 * * 1-5",
        "America/New_York",
        "C0EXAMPLE1",
    );
    assert_human_cron_row(&run, "maintenance", "0 0 * * 0", "UTC", "targetless");
    assert!(
        !run.stderr().contains("cron trigger `morning`")
            && !run.stderr().contains("cron trigger `maintenance`"),
        "bound and targetless cron must not warn: {}",
        run.stderr()
    );
}

#[test]
fn cron_json_receipt_keeps_declaration_order_nullable_targets_and_exact_warnings() {
    let run = deploy_cron_fixture(
        CronDeployFixture::new(
            json!([
                cron("nightly", "0 2 * * *", Some("Etc/UTC"), Some("C0NOTBOUND1")),
                {"type": "webhook", "name": "inbound", "path": "/events"},
                cron("maintenance", "0 0 * * 0", None, None),
                cron("morning", "15 9 * * 1-5", Some("America/New_York"), Some("C0EXAMPLE1"))
            ]),
            true,
        ),
        |_| {},
    );
    let value = cron_json(&run);
    assert_eq!(
        value["cron_triggers"],
        json!([
            {"name": "nightly", "schedule": "0 2 * * *", "zone": "Etc/UTC", "target": "C0NOTBOUND1"},
            {"name": "maintenance", "schedule": "0 0 * * 0", "zone": "UTC", "target": null},
            {"name": "morning", "schedule": "15 9 * * 1-5", "zone": "America/New_York", "target": "C0EXAMPLE1"}
        ])
    );
    assert_eq!(
        value["warnings"],
        json!([cron_warning("nightly", "C0NOTBOUND1")])
    );
}

#[test]
fn bound_and_targetless_cron_have_complete_warning_free_human_and_json_receipts() {
    for json_output in [false, true] {
        let run = deploy_cron_fixture(
            CronDeployFixture::new(
                json!([
                    cron("bound", "0 10 * * *", None, Some("C0EXAMPLE1")),
                    cron("targetless", "30 3 * * *", Some("Europe/London"), None)
                ]),
                json_output,
            ),
            |_| {},
        );
        assert_cron_deployed(&run);
        assert!(
            !run.stderr().contains("every slot records failed"),
            "neither resolved nor targetless cron may warn: {}",
            run.stderr()
        );
        if json_output {
            let value = cron_json(&run);
            assert_eq!(value["warnings"], json!([]));
            assert_eq!(
                value["cron_triggers"],
                json!([
                    {"name": "bound", "schedule": "0 10 * * *", "zone": "UTC", "target": "C0EXAMPLE1"},
                    {"name": "targetless", "schedule": "30 3 * * *", "zone": "Europe/London", "target": null}
                ])
            );
        } else {
            assert_human_cron_row(&run, "bound", "0 10 * * *", "UTC", "C0EXAMPLE1");
            assert_human_cron_row(
                &run,
                "targetless",
                "30 3 * * *",
                "Europe/London",
                "targetless",
            );
        }
    }
}

/// The binding rule is apps/api/src/curie_api/routers/hook_fire.py::fire_hook:
/// several Slack identities select the default; mixed kinds stay ambiguous.
#[test]
fn cron_target_matching_follows_hook_fire_slack_identity_and_ambiguity_rules() {
    let cases = [
        (
            "omitted default plus named",
            json!([
                {"kind": "slack", "address": "C0EXAMPLE1"},
                {"kind": "slack", "address": "C0EXAMPLE1", "adapter": "ops-bot"}
            ]),
            false,
        ),
        (
            "explicit default plus named",
            json!([
                {"kind": "slack", "address": "C0EXAMPLE1", "adapter": "default"},
                {"kind": "slack", "address": "C0EXAMPLE1", "adapter": "ops-bot"}
            ]),
            false,
        ),
        (
            "single named",
            json!([
                {"kind": "slack", "address": "C0EXAMPLE1", "adapter": "ops-bot"}
            ]),
            false,
        ),
        (
            "several named without default",
            json!([
                {"kind": "slack", "address": "C0EXAMPLE1", "adapter": "ops-bot"},
                {"kind": "slack", "address": "C0EXAMPLE1", "adapter": "review-bot"}
            ]),
            true,
        ),
        (
            "mixed kinds",
            json!([
                {"kind": "slack", "address": "C0EXAMPLE1", "adapter": "default"},
                {"kind": "discord", "address": "C0EXAMPLE1", "adapter": "ops-bot"}
            ]),
            true,
        ),
        (
            "two defaults",
            json!([
                {"kind": "slack", "address": "C0EXAMPLE1"},
                {"kind": "slack", "address": "C0EXAMPLE1", "adapter": "default"}
            ]),
            true,
        ),
        (
            "binding at another address",
            json!([
                {"kind": "slack", "address": "C0EXAMPLE2", "adapter": "default"}
            ]),
            true,
        ),
    ];
    for (case, channels, warns) in cases {
        let mut fixture = CronDeployFixture::new(
            json!([cron("nightly", "0 2 * * *", None, Some("C0EXAMPLE1"))]),
            true,
        );
        fixture.channels = channels;
        let run = deploy_cron_fixture(fixture, |_| {});
        let value = cron_json(&run);
        let expected = if warns {
            json!([cron_warning("nightly", "C0EXAMPLE1")])
        } else {
            json!([])
        };
        assert_eq!(value["warnings"], expected, "binding case: {case}");
    }
}

#[test]
fn cron_target_matching_trims_the_address_as_hook_fire_does() {
    let run = deploy_cron_fixture(
        CronDeployFixture::new(
            json!([cron("nightly", "0 2 * * *", None, Some(" C0EXAMPLE1 "))]),
            true,
        ),
        |_| {},
    );
    let value = cron_json(&run);
    assert_eq!(value["warnings"], json!([]));
    assert_eq!(value["cron_triggers"][0]["target"], json!(" C0EXAMPLE1 "));
}

#[test]
fn cron_warning_uses_the_updated_agent_returned_by_channel_add() {
    let mut fixture = CronDeployFixture::new(
        json!([cron("nightly", "0 2 * * *", None, Some("C0NOTBOUND1"))]),
        true,
    );
    fixture.slack_channel = Some("C0NOTBOUND1");
    fixture.added_channels = Some(json!([
        {"kind": "slack", "address": "C0EXAMPLE1"},
        {"kind": "slack", "address": "C0NOTBOUND1"}
    ]));
    let run = deploy_cron_fixture(fixture, |_| {});
    let value = cron_json(&run);
    assert_eq!(value["warnings"], json!([]));
    assert_eq!(value["cron_triggers"][0]["target"], json!("C0NOTBOUND1"));
    let adds: Vec<_> = run
        .api
        .recorded()
        .into_iter()
        .filter(|request| {
            request.method == "POST" && request.path == format!("/agents/{CRON_AGENT_ID}/channels")
        })
        .collect();
    assert_eq!(adds.len(), 1, "the missing binding must be added once");
    let body: Value = serde_json::from_slice(&adds[0].body).expect("channel add JSON");
    assert_eq!(body["address"], json!("C0NOTBOUND1"));
}

fn uploaded_manifest(run: &Run, location: &str) -> Option<Value> {
    let upload = run
        .api
        .recorded()
        .into_iter()
        .find(|request| request.method == "PUT" && request.path.ends_with("/bundle"))
        .expect("bundle upload request");
    // PUT /agents/{agent_id}/versions/{version_id}/bundle is multipart in
    // apps/api/openapi.json. Read the uploaded file bytes, not the envelope.
    let boundary = upload
        .header("content-type")
        .and_then(|value| value.split("boundary=").nth(1))
        .expect("bundle multipart boundary");
    let start = upload
        .body
        .windows(4)
        .position(|bytes| bytes == b"\r\n\r\n")
        .expect("multipart file headers")
        + 4;
    let closing = format!("\r\n--{boundary}");
    let end = upload.body[start..]
        .windows(closing.len())
        .position(|bytes| bytes == closing.as_bytes())
        .expect("multipart file closing boundary")
        + start;
    let mut archive = tar::Archive::new(flate2::read::GzDecoder::new(&upload.body[start..end]));
    for entry in archive.entries().expect("uploaded archive entries") {
        let mut entry = entry.expect("uploaded archive entry");
        let path = entry
            .path()
            .expect("uploaded entry path")
            .to_string_lossy()
            .into_owned();
        if path.trim_start_matches("./") == location {
            let mut body = String::new();
            std::io::Read::read_to_string(&mut entry, &mut body).expect("uploaded manifest text");
            return Some(serde_json::from_str(&body).expect("uploaded manifest JSON"));
        }
    }
    None
}

#[test]
fn cron_receipt_prefers_the_primary_packed_manifest_over_a_root_manifest() {
    let run = deploy_cron_fixture(
        CronDeployFixture::new(
            json!([cron("packed", "0 2 * * *", None, Some("C0EXAMPLE1"))]),
            true,
        ),
        |plugin| {
            fs::write(
                plugin.join("plugin.json"),
                json!({
                    "name": "acme-decoy", "version": "1.0.0",
                    "triggers": [cron("decoy", "0 3 * * *", None, Some("C0NOTBOUND1"))]
                })
                .to_string(),
            )
            .expect("write alternate manifest");
        },
    );
    let value = cron_json(&run);
    assert!(uploaded_manifest(&run, "plugin.json").is_some());
    assert!(uploaded_manifest(&run, ".claude-plugin/plugin.json").is_some());
    assert_eq!(value["warnings"], json!([]));
    assert_eq!(
        value["cron_triggers"],
        json!([
            {"name": "packed", "schedule": "0 2 * * *", "zone": "UTC", "target": "C0EXAMPLE1"}
        ])
    );
}

#[test]
fn cron_receipt_uses_the_uploaded_manifest_when_curieignore_excludes_the_source_manifest() {
    let run = deploy_cron_fixture(
        CronDeployFixture::new(
            json!([cron("excluded", "0 3 * * *", None, Some("C0NOTBOUND1"))]),
            true,
        ),
        |plugin| {
            fs::write(plugin.join(".curieignore"), ".claude-plugin\n")
                .expect("exclude source manifest");
            fs::write(
                plugin.join("plugin.json"),
                json!({
                    "name": "acme-bot", "version": "1.0.0",
                    "triggers": [cron("uploaded", "0 4 * * *", Some("Europe/London"), None)]
                })
                .to_string(),
            )
            .expect("write packed manifest");
        },
    );
    let value = cron_json(&run);
    assert!(uploaded_manifest(&run, ".claude-plugin/plugin.json").is_none());
    let manifest = uploaded_manifest(&run, "plugin.json").expect("fallback manifest uploaded");
    assert_eq!(manifest["triggers"][0]["name"], json!("uploaded"));
    assert_eq!(value["warnings"], json!([]));
    assert_eq!(
        value["cron_triggers"],
        json!([
            {"name": "uploaded", "schedule": "0 4 * * *", "zone": "Europe/London", "target": null}
        ])
    );
}

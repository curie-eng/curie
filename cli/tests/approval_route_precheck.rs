//! Integration: the approval-route pre-check (#2448) refuses a deploy or route
//! write locally, before the requests the API would refuse, driven through the
//! real `commands::deploy` / `commands::approvals` handlers against a wire-level
//! stub (`support::serve`).
//!
//! Every stub answers every path the code under test could call (including the
//! requests a CLI without the pre-check sends), so a test that must fail without
//! the pre-check fails at its own assertion rather than on a stub panic. The
//! assertions target the recorded requests and the returned error's class,
//! message and fix, never an internal struct and never stderr warnings.

mod support;

use curie::commands::{
    approvals, deploy, AgentActionOpts, ApprovalCmd, ApprovalsOutput, DeployOpts, DeployTier,
    WorkspaceIntent,
};
use curie::exit::{classify, ExitClass};
use serde_json::{json, Value};
use std::path::Path;
use support::{serve, MockServer, Response};

const AGENT_ID: &str = "11111111-1111-1111-1111-111111111111";
const VERSION_ID: &str = "22222222-2222-2222-2222-222222222222";
const DEPLOYMENT_ID: &str = "33333333-3333-3333-3333-333333333333";
const TEST_API_KEY: &str = "test-key";
const UNEXPECTED: &str = r#"{"detail":"request not modeled by the #2448 test stub"}"#;

// --- shared fixtures ---------------------------------------------------------

/// A scaffolded `deal-desk` bundle whose manifest carries `approvalPolicy`
/// exactly as given (`None` leaves the scaffold's manifest ungated).
fn bundle_with_policy(policy: Option<Value>) -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("tempdir");
    curie::scaffold::scaffold(dir.path(), "deal-desk").expect("scaffold the bundle");
    if let Some(policy) = policy {
        let path = dir.path().join(".claude-plugin/plugin.json");
        let mut manifest: Value =
            serde_json::from_str(&std::fs::read_to_string(&path).expect("read plugin.json"))
                .expect("the scaffolded manifest is JSON");
        manifest["approvalPolicy"] = policy;
        std::fs::write(&path, serde_json::to_string_pretty(&manifest).unwrap())
            .expect("write plugin.json");
    }
    dir
}

/// A bundle declaring one gate per `(tool, route)` pair. Built-in tool names
/// keep the route normalization the only variable, like the runner's vector.
fn gated_bundle(routes: &[(&str, &str)]) -> tempfile::TempDir {
    let gates: Vec<Value> = routes
        .iter()
        .map(|(gate, route)| json!({"gate": gate, "route": route}))
        .collect();
    bundle_with_policy(Some(json!({ "gates": gates })))
}

/// One display-shaped route binding, as `AgentOut.approval_routes` carries it.
fn binding(channel: &str) -> Value {
    json!({"resolution": {"kind": "slack", "address": channel}})
}

/// An `AgentOut` with `approval_routes` null (`None`) or bound to each name.
fn agent_json(name: &str, routes: Option<&[&str]>) -> Value {
    let routes = routes.map(|names| {
        names
            .iter()
            .enumerate()
            .map(|(i, route)| (route.to_string(), binding(&format!("C0EXAMPLE{}", i + 1))))
            .collect::<serde_json::Map<String, Value>>()
    });
    json!({
        "id": AGENT_ID,
        "name": name,
        "channels": [{"kind": "slack", "address": "C0EXAMPLE0"}],
        "approval_required_tools": null,
        "approval_routes": routes,
        "memory": false,
    })
}

fn deploy_opts(
    server: &MockServer,
    dir: &Path,
    tier: DeployTier,
    agent: Option<&str>,
) -> DeployOpts {
    DeployOpts {
        delivery: None,
        tier,
        agent: agent.map(str::to_string),
        target: None,
        identity: None,
        plugin_dir: dir.to_path_buf(),
        api_url: server.base_url.clone(),
        api_key: TEST_API_KEY.to_string(),
        slack_channel: None,
        repo: None,
        workspace: WorkspaceIntent::Preserve,
        env: None,
        label: Some("0.1.0-1".to_string()),
        secret: vec![],
        secret_binding_supported: tier == DeployTier::Local,
        connect_hint: "mock API should be reachable".to_string(),
    }
}

/// The deploy-path stub. `listed` is the `GET /agents` answer and `agent` the
/// record every agent write returns. With `uploads` false, the version, bundle
/// and deployment writes answer 500 (the "any other path" shape), so a CLI that
/// sends them fails without reaching a success it should never reach.
struct DeployStub {
    listed: Vec<Value>,
    agent: Value,
    uploads: bool,
    deployment: (u16, String),
}

impl DeployStub {
    fn existing(agent: Value) -> Self {
        Self {
            listed: vec![agent.clone()],
            agent,
            uploads: true,
            deployment: (201, deployment_created()),
        }
    }

    fn serve(self) -> MockServer {
        let DeployStub {
            listed,
            agent,
            uploads,
            deployment,
        } = self;
        let listed = Value::Array(listed).to_string();
        let agent = agent.to_string();
        serve(move |req| {
            let path = req.path.split('?').next().unwrap_or_default();
            let agent_path = format!("/agents/{AGENT_ID}");
            let versions = format!("{agent_path}/versions");
            let bundle = format!("{versions}/{VERSION_ID}/bundle");
            match (req.method.as_str(), path) {
                ("GET", "/agents") => Response::json(200, &listed),
                ("POST", "/agents") => Response::json(201, &agent),
                ("PATCH", p) if p == agent_path => Response::json(200, &agent),
                ("POST", p) if p == format!("{agent_path}/channels") => Response::json(201, &agent),
                ("POST", p) if uploads && p == versions => Response::json(
                    201,
                    &format!(
                        r#"{{"id":"{VERSION_ID}","agent_id":"{AGENT_ID}","version_label":"0.1.0-1","bundle_ref":null,"bundle_sha256":null,"created_by":"tester","created_at":"2026-09-14T00:00:00Z"}}"#
                    ),
                ),
                ("PUT", p) if uploads && p == bundle => Response::json(
                    201,
                    &format!(
                        r#"{{"version_id":"{VERSION_ID}","bundle_ref":"bundles/x.tar.gz","bundle_sha256":"deadbeef","size_bytes":512}}"#
                    ),
                ),
                ("POST", "/deployments") if uploads => Response::json(deployment.0, &deployment.1),
                ("GET", "/deployments") => Response::json(200, "[]"),
                ("GET", p) if p.starts_with(&versions) && p.ends_with("/files") => {
                    Response::json(200, r#"{"files":[]}"#)
                }
                _ => Response::json(500, UNEXPECTED),
            }
        })
    }
}

fn deployment_created() -> String {
    format!(
        r#"{{"id":"{DEPLOYMENT_ID}","agent_id":"{AGENT_ID}","version_id":"{VERSION_ID}","environment":"dev","status":"active","deployed_at":"2026-09-14T00:00:00Z"}}"#
    )
}

/// `(METHOD, path-without-query)` for every recorded request, in order.
fn flow(server: &MockServer) -> Vec<(String, String)> {
    server
        .recorded()
        .iter()
        .map(|r| {
            (
                r.method.clone(),
                r.path.split('?').next().unwrap_or_default().to_string(),
            )
        })
        .collect()
}

fn sent(server: &MockServer, method: &str, path: &str) -> bool {
    flow(server).iter().any(|(m, p)| m == method && p == path)
}

/// The command must have failed as a usage refusal (exit 2) carrying a fix.
/// Returns the rendered message and the fix.
fn usage_refusal<T>(result: anyhow::Result<T>, what: &str) -> (String, String) {
    let err = match result {
        Ok(_) => panic!("{what}: expected a local refusal, but the command succeeded"),
        Err(err) => err,
    };
    let (class, fix) = classify(&err);
    let message = format!("{err:#}");
    assert_eq!(
        class,
        ExitClass::Usage,
        "{what}: expected a usage refusal (exit 2), got {class:?}: {message}"
    );
    let fix = fix.unwrap_or_else(|| panic!("{what}: the refusal must carry a fix: {message}"));
    (message, fix)
}

fn assert_nothing_uploaded(server: &MockServer) {
    let versions = format!("/agents/{AGENT_ID}/versions");
    let flow = flow(server);
    assert!(
        !flow
            .iter()
            .any(|(m, p)| (m == "POST" && p == &versions) || (m == "PUT" && p.ends_with("/bundle"))),
        "a refused deploy must send no version or bundle request; recorded: {flow:?}"
    );
    assert!(
        !flow.iter().any(|(_, p)| p == "/deployments"),
        "a refused deploy must not reach /deployments; recorded: {flow:?}"
    );
}

// --- deploy (AC1) ------------------------------------------------------------

#[tokio::test]
async fn first_gated_deploy_creates_the_agent_then_refuses_before_any_version() {
    let dir = gated_bundle(&[("Bash", "ops")]);
    let server = DeployStub {
        listed: vec![],
        agent: agent_json("deal-desk", None),
        uploads: false,
        deployment: (500, UNEXPECTED.to_string()),
    }
    .serve();

    let result = deploy(deploy_opts(&server, dir.path(), DeployTier::Local, None)).await;

    assert_eq!(
        flow(&server),
        vec![
            ("GET".to_string(), "/agents".to_string()),
            ("POST".to_string(), "/agents".to_string()),
        ],
        "the deploy must create the agent and then send nothing else"
    );
    let (message, fix) = usage_refusal(result, "unbound route on a fresh agent");
    assert!(
        message.contains("\"ops\""),
        "names the unbound route: {message}"
    );
    assert!(
        message.contains("this agent binds no approval routes"),
        "states the agent binds nothing: {message}"
    );
    assert!(
        message.contains("was created by this deploy"),
        "says the agent now exists so it can be bound: {message}"
    );
    assert!(
        fix.contains("curie local approvals deal-desk --route-resolution ops=<channel>"),
        "the fix is the local binding command: {fix}"
    );
}

#[tokio::test]
async fn existing_agent_with_other_bound_route_is_refused_and_names_both() {
    let dir = gated_bundle(&[("Bash", "ops"), ("Write", "finance")]);
    let server = DeployStub::existing(agent_json("deal-desk", Some(&["finance"]))).serve();

    let result = deploy(deploy_opts(&server, dir.path(), DeployTier::Cluster, None)).await;

    let (message, fix) = usage_refusal(result, "one of two declared routes unbound");
    assert_nothing_uploaded(&server);
    assert!(
        message.contains("\"ops\""),
        "names the unbound route: {message}"
    );
    assert!(
        message.contains("bound routes are \"finance\""),
        "names the routes already bound: {message}"
    );
    assert!(
        message.contains("No version, bundle, or deployment was created"),
        "states what was not created: {message}"
    );
    assert!(
        !message.contains("was created by this deploy"),
        "an existing agent was not created by this deploy: {message}"
    );
    for needle in [
        "curie cluster approvals deal-desk",
        "--route-resolution ops=<channel>",
        "--route-resolution finance=<channel>",
        "--routes-from",
        "--namespace/--release/--api-url",
    ] {
        assert!(
            fix.contains(needle),
            "the fix must contain {needle:?}: {fix}"
        );
    }
}

#[tokio::test]
async fn refusal_sends_no_secrets_patch() {
    const SECRET: &str = "CURIE_TEST_PRECHECK_2448_SECRET";
    std::env::set_var(SECRET, "precheck-2448-value");
    let dir = gated_bundle(&[("Bash", "ops")]);
    let mut stub = DeployStub::existing(agent_json("deal-desk", None));
    // Uploads refused so a CLI that gets past the secrets PATCH fails there
    // instead of completing a deploy.
    stub.uploads = false;
    let server = stub.serve();

    let mut opts = deploy_opts(&server, dir.path(), DeployTier::Local, None);
    opts.secret = vec![SECRET.to_string()];
    let result = deploy(opts).await;
    std::env::remove_var(SECRET);

    let agent_path = format!("/agents/{AGENT_ID}");
    let secrets_patch = server.recorded().into_iter().find(|r| {
        r.method == "PATCH"
            && r.path == agent_path
            && serde_json::from_slice::<Value>(&r.body)
                .ok()
                .is_some_and(|body| body.get("secrets").is_some())
    });
    assert!(
        secrets_patch.is_none(),
        "a refused deploy must not bind connector secrets first; recorded: {:?}",
        flow(&server)
    );
    assert_nothing_uploaded(&server);
    usage_refusal(result, "unbound route with a --secret");
}

#[tokio::test]
async fn all_declared_routes_bound_deploys_through_create_deployment() {
    let dir = gated_bundle(&[("Bash", "ops")]);
    let server = DeployStub::existing(agent_json("deal-desk", Some(&["ops"]))).serve();

    let out = deploy(deploy_opts(&server, dir.path(), DeployTier::Cluster, None))
        .await
        .expect("every declared route is bound, so the deploy proceeds");

    assert_eq!(out.deployment_id, DEPLOYMENT_ID);
    assert!(sent(
        &server,
        "POST",
        &format!("/agents/{AGENT_ID}/versions")
    ));
    assert!(sent(
        &server,
        "PUT",
        &format!("/agents/{AGENT_ID}/versions/{VERSION_ID}/bundle")
    ));
    assert!(sent(&server, "POST", "/deployments"));
}

#[tokio::test]
async fn a_bound_route_no_bundle_declares_is_accepted() {
    let dir = gated_bundle(&[("Bash", "ops")]);
    let server = DeployStub::existing(agent_json("deal-desk", Some(&["ops", "legacy"]))).serve();

    deploy(deploy_opts(&server, dir.path(), DeployTier::Cluster, None))
        .await
        .expect("an extra bound route is not a gap");

    assert!(sent(&server, "POST", "/deployments"));
}

#[tokio::test]
async fn ungated_bundle_runs_no_route_check() {
    for (label, policy) in [
        ("no approvalPolicy", None),
        ("empty gates list", Some(json!({"gates": []}))),
    ] {
        let dir = bundle_with_policy(policy);
        let server = DeployStub::existing(agent_json("deal-desk", None)).serve();

        if let Err(err) = deploy(deploy_opts(&server, dir.path(), DeployTier::Cluster, None)).await
        {
            panic!("{label}: a bundle declaring no routes must deploy: {err:#}");
        }
        assert!(
            sent(&server, "POST", "/deployments"),
            "{label}: the deployment must be created; recorded: {:?}",
            flow(&server)
        );
    }
}

#[tokio::test]
async fn unreadable_local_policy_warns_and_leaves_the_decision_to_the_api() {
    let dir = gated_bundle(&[("Bash", "   ")]);
    let mut stub = DeployStub::existing(agent_json("deal-desk", None));
    stub.deployment = (
        422,
        r#"{"detail":"deploy.ApprovalRoutesUnbound: the bundle's approvalPolicy could not be read, so its declared approval routes cannot be checked against this agent's approval_routes"}"#
            .to_string(),
    );
    let server = stub.serve();

    let err = deploy(deploy_opts(&server, dir.path(), DeployTier::Cluster, None))
        .await
        .expect_err("the stub API refuses the deployment");

    let (class, _) = classify(&err);
    assert_ne!(
        class,
        ExitClass::Usage,
        "an unreadable local policy must not become a local refusal; the API decides: {err:#}"
    );
    assert!(
        sent(&server, "POST", &format!("/agents/{AGENT_ID}/versions")),
        "fail-open: the deploy must proceed to the version request; recorded: {:?}",
        flow(&server)
    );
    assert!(
        sent(&server, "POST", "/deployments"),
        "fail-open: the API's deployment gate must be the one that refused; recorded: {:?}",
        flow(&server)
    );
}

#[tokio::test]
async fn agent_override_name_is_the_one_the_fix_binds() {
    let dir = gated_bundle(&[("Bash", "ops")]);
    let server = DeployStub::existing(agent_json("deal-desk-prod", None)).serve();

    let result = deploy(deploy_opts(
        &server,
        dir.path(),
        DeployTier::Cluster,
        Some("deal-desk-prod"),
    ))
    .await;

    let (message, fix) = usage_refusal(result, "unbound route on an overridden agent name");
    assert_nothing_uploaded(&server);
    assert!(
        message.contains("as agent deal-desk-prod"),
        "the message names the resolved agent: {message}"
    );
    assert!(
        fix.contains("approvals deal-desk-prod"),
        "the fix binds the resolved agent, not the manifest name: {fix}"
    );
}

#[tokio::test]
async fn routes_compare_verbatim_and_case_sensitive() {
    let dir = gated_bundle(&[("Bash", "ops")]);
    let server = DeployStub::existing(agent_json("deal-desk", Some(&["Ops", " ops "]))).serve();

    let result = deploy(deploy_opts(&server, dir.path(), DeployTier::Cluster, None)).await;

    let (message, _) = usage_refusal(result, "only near-miss route names bound");
    assert_nothing_uploaded(&server);
    assert!(
        message.contains("approval route(s) \"ops\" with no entry"),
        "`ops` is unbound: neither `Ops` nor ` ops ` binds it: {message}"
    );
    assert!(
        message.contains("\" ops \""),
        "the padded bound key is shown with its padding visible: {message}"
    );
    assert!(
        message.contains("\"Ops\""),
        "the differently-cased bound key is listed as bound: {message}"
    );
}

#[tokio::test]
async fn curieignored_gated_manifest_is_not_what_the_precheck_judges() {
    // `.claude-plugin/plugin.json` declares a gated `ops` route but is named by
    // a root `.curieignore`, so `pack_tar_gz` never uploads it; the root
    // `plugin.json` it packs instead is ungated. The pre-check must judge the
    // manifest actually packed, not the curieignored one still sitting on disk.
    let dir = gated_bundle(&[("Bash", "ops")]);
    std::fs::write(dir.path().join("plugin.json"), manifest_declaring(&[]))
        .expect("write the ungated root plugin.json that is actually packed");
    std::fs::write(dir.path().join(".curieignore"), ".claude-plugin\n")
        .expect("write .curieignore excluding the gated nested manifest");
    let server = DeployStub::existing(agent_json("deal-desk", None)).serve();

    let result = deploy(deploy_opts(&server, dir.path(), DeployTier::Cluster, None)).await;

    if let Err(err) = result {
        panic!(
            "the packed archive excludes .claude-plugin (curieignored), so the uploaded \
             manifest is the ungated root plugin.json; the local pre-check must not refuse: {err:#}"
        );
    }
    assert!(
        sent(&server, "POST", &format!("/agents/{AGENT_ID}/versions")),
        "the deploy must reach the version request: {:?}",
        flow(&server)
    );
    assert!(
        sent(&server, "POST", "/deployments"),
        "the deploy must reach the deployment request: {:?}",
        flow(&server)
    );
}

#[tokio::test]
async fn curieignored_ungated_manifest_still_refuses_the_packed_gated_one() {
    // The reverse: `.claude-plugin/plugin.json` is ungated, but a root
    // `.curieignore` excludes it, so the root `plugin.json` -- which declares
    // an unbound `ops` route -- is the one `pack_tar_gz` actually uploads. The
    // pre-check must still catch the unbound route declared there.
    let dir = bundle_with_policy(None);
    std::fs::write(dir.path().join("plugin.json"), manifest_declaring(&["ops"]))
        .expect("write the gated root plugin.json that is actually packed");
    std::fs::write(dir.path().join(".curieignore"), ".claude-plugin\n")
        .expect("write .curieignore excluding the ungated nested manifest");
    let server = DeployStub::existing(agent_json("deal-desk", None)).serve();

    let result = deploy(deploy_opts(&server, dir.path(), DeployTier::Cluster, None)).await;

    assert_nothing_uploaded(&server);
    let (message, _) = usage_refusal(
        result,
        "curieignored nested manifest is ungated but the packed root manifest declares ops",
    );
    assert!(
        message.contains("\"ops\""),
        "names the unbound route from the manifest actually packed: {message}"
    );
}

#[tokio::test]
async fn single_wrapper_directory_manifest_is_judged_like_the_platform() {
    // The platform's `bundle_root()` (packages/plugin-format/src/plugin_format/
    // archive.py) unwraps into a packed archive's sole top-level directory when
    // the extraction root itself carries no manifest: no root
    // `.claude-plugin/plugin.json`/`plugin.json`, but exactly one directory at
    // the root (files there do not count), and that directory has a manifest.
    // Excluding every scaffolded top-level directory except a single `payload/`
    // (which carries the gated manifest) reproduces that shape.
    // `read_packed_bundle_gates` only probes the archive root, so it currently
    // cannot see `payload/plugin.json` at all and fails open -- this asserts the
    // judgment the platform actually applies.
    let dir = gated_bundle(&[("Bash", "ops")]);
    std::fs::write(
        dir.path().join(".curieignore"),
        ".claude-plugin\nskills\nevals\n.claude\n",
    )
    .expect("write .curieignore excluding every scaffolded top-level dir but the wrapper");
    std::fs::create_dir(dir.path().join("payload")).expect("create the wrapper directory");
    std::fs::write(
        dir.path().join("payload/plugin.json"),
        manifest_declaring(&["ops"]),
    )
    .expect("write the single-wrapper-directory manifest declaring ops");
    let server = DeployStub::existing(agent_json("deal-desk", None)).serve();

    let result = deploy(deploy_opts(&server, dir.path(), DeployTier::Cluster, None)).await;

    assert_nothing_uploaded(&server);
    let (message, _) = usage_refusal(
        result,
        "a single top-level payload/ directory is the platform's unwrap target",
    );
    assert!(
        message.contains("\"ops\""),
        "names the unbound route declared in payload/plugin.json: {message}"
    );
}

#[tokio::test]
async fn two_top_level_directories_do_not_unwrap() {
    // Same shape as above, but a second non-excluded top-level directory means
    // the platform's "exactly one directory" condition fails, so it does NOT
    // unwrap into `payload/` and finds no root manifest. The CLI must not
    // refuse on `payload/plugin.json`'s route either: it fails open the same
    // way it does today (no manifest found at the archive root), so the deploy
    // proceeds to the version request.
    let dir = gated_bundle(&[("Bash", "ops")]);
    std::fs::write(
        dir.path().join(".curieignore"),
        ".claude-plugin\nskills\nevals\n.claude\n",
    )
    .expect("write .curieignore excluding every scaffolded top-level dir but the wrapper");
    std::fs::create_dir(dir.path().join("payload")).expect("create the wrapper directory");
    std::fs::write(
        dir.path().join("payload/plugin.json"),
        manifest_declaring(&["ops"]),
    )
    .expect("write the wrapper-directory manifest declaring ops");
    std::fs::create_dir(dir.path().join("extra")).expect("create a second top-level dir");
    std::fs::write(dir.path().join("extra/note.txt"), "not a manifest")
        .expect("write a file under the second top-level dir");
    let server = DeployStub::existing(agent_json("deal-desk", None)).serve();

    deploy(deploy_opts(&server, dir.path(), DeployTier::Cluster, None))
        .await
        .expect(
            "two top-level directories means the platform does not unwrap into payload/, so \
             the deploy must proceed (fail-open: the CLI finds no root manifest either)",
        );

    assert!(
        sent(&server, "POST", &format!("/agents/{AGENT_ID}/versions")),
        "the deploy must reach the version request: {:?}",
        flow(&server)
    );
}

#[test]
fn deploy_json_refusal_is_one_error_object_with_fix() {
    let dir = gated_bundle(&[("Bash", "ops")]);
    let server = DeployStub {
        listed: vec![],
        agent: agent_json("deal-desk", None),
        uploads: false,
        deployment: (500, UNEXPECTED.to_string()),
    }
    .serve();

    let out = std::process::Command::new(env!("CARGO_BIN_EXE_curie"))
        .args(["local", "deploy", "--json", "--plugin-dir"])
        .arg(dir.path())
        .args(["--api-url", &server.base_url, "--api-key", TEST_API_KEY])
        .env_remove("CURIE_API_URL")
        .env_remove("CURIE_API_KEY")
        .env("NO_COLOR", "1")
        .output()
        .expect("run curie local deploy");

    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(
        out.status.code(),
        Some(2),
        "a local refusal exits 2\nstdout: {stdout}\nstderr: {stderr}"
    );
    let value: Value = serde_json::from_str(stdout.trim())
        .unwrap_or_else(|err| panic!("stdout must be one JSON object ({err}): {stdout}"));
    let object = value.as_object().expect("the error payload is an object");
    let error = object["error"].as_str().expect("error is a string");
    let fix = object["fix"].as_str().expect("fix is a string");
    assert!(error.contains("ops"), "error names the route: {error}");
    assert!(
        fix.contains("curie local approvals"),
        "fix is the local binding command: {fix}"
    );
    assert!(
        !sent(&server, "POST", &format!("/agents/{AGENT_ID}/versions")),
        "no version request is sent: {:?}",
        flow(&server)
    );
}

// --- route writes (AC2) ------------------------------------------------------

const VERSION_A: &str = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
const VERSION_B: &str = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
const DEP_DEV: &str = "dep-dev-2448";
const DEP_PROD: &str = "dep-prod-2448";
const DEP_DEV_2: &str = "dep-dev-2448-second";

/// A deployed manifest declaring one gate per route (no routes: ungated).
fn manifest_declaring(routes: &[&str]) -> String {
    const TOOLS: [&str; 4] = ["Bash", "Write", "Edit", "Read"];
    let mut manifest = json!({"name": "deal-desk", "version": "0.1.0"});
    if !routes.is_empty() {
        let gates: Vec<Value> = routes
            .iter()
            .enumerate()
            .map(|(i, route)| json!({"gate": TOOLS[i], "route": route}))
            .collect();
        manifest["approvalPolicy"] = json!({ "gates": gates });
    }
    manifest.to_string()
}

fn files_with(content: &str) -> (u16, String) {
    (
        200,
        json!({"files": [{"path": ".claude-plugin/plugin.json", "content": content}]}).to_string(),
    )
}

fn deployment_row(id: &str, environment: &str, status: &str, version_id: &str) -> Value {
    json!({
        "id": id,
        "agent_id": AGENT_ID,
        "environment": environment,
        "status": status,
        "version_id": version_id,
        "deployed_at": "2026-09-14T00:00:00Z",
    })
}

/// The route-write stub: the agent binds `ops` and `finance`; the deployment
/// list and each version's files are configurable; `PATCH /agents/{id}` echoes
/// the written map back as the updated agent.
struct RouteStub {
    deployments: (u16, String),
    files: Vec<(&'static str, (u16, String))>,
}

impl RouteStub {
    fn active(rows: Vec<Value>, files: Vec<(&'static str, (u16, String))>) -> Self {
        Self {
            deployments: (200, Value::Array(rows).to_string()),
            files,
        }
    }

    fn serve(self) -> MockServer {
        let RouteStub { deployments, files } = self;
        let listed =
            Value::Array(vec![agent_json("deal-desk", Some(&["ops", "finance"]))]).to_string();
        serve(move |req| {
            let path = req.path.split('?').next().unwrap_or_default();
            let agent_path = format!("/agents/{AGENT_ID}");
            match (req.method.as_str(), path) {
                ("GET", "/agents") => Response::json(200, &listed),
                ("GET", "/deployments") => Response::json(deployments.0, &deployments.1),
                ("GET", p) => {
                    for (version, (status, body)) in &files {
                        if p == format!("{agent_path}/versions/{version}/files") {
                            return Response::json(*status, body);
                        }
                    }
                    Response::json(404, r#"{"detail":"version not found"}"#)
                }
                ("PATCH", p) if p == agent_path => {
                    let body: Value = serde_json::from_slice(&req.body).unwrap_or(Value::Null);
                    let mut agent = agent_json("deal-desk", None);
                    agent["approval_routes"] =
                        body.get("approval_routes").cloned().unwrap_or(Value::Null);
                    Response::json(200, &agent.to_string())
                }
                _ => Response::json(500, UNEXPECTED),
            }
        })
    }
}

async fn run_approvals(
    server: &MockServer,
    cmd: ApprovalCmd,
    dry_run: bool,
) -> anyhow::Result<ApprovalsOutput> {
    approvals(
        AgentActionOpts {
            api_url: server.base_url.clone(),
            api_key: TEST_API_KEY.to_string(),
            agent: "deal-desk".to_string(),
            dry_run,
        },
        vec![],
        false,
        cmd,
    )
    .await
}

fn clear() -> ApprovalCmd {
    ApprovalCmd {
        clear_routes: true,
        ..ApprovalCmd::default()
    }
}

fn patched(server: &MockServer) -> bool {
    sent(server, "PATCH", &format!("/agents/{AGENT_ID}"))
}

fn files_gets(server: &MockServer) -> usize {
    flow(server)
        .iter()
        .filter(|(m, p)| m == "GET" && p.ends_with("/files"))
        .count()
}

fn assert_no_patch(server: &MockServer) {
    assert!(
        !patched(server),
        "a refused route write must send no PATCH; recorded: {:?}",
        flow(server)
    );
}

fn expect_routes(result: anyhow::Result<ApprovalsOutput>, what: &str) {
    match result {
        Ok(ApprovalsOutput::Routes { .. }) => {}
        Ok(_) => panic!("{what}: expected the Routes output"),
        Err(err) => panic!("{what}: expected the write to be sent, got {err:#}"),
    }
}

#[tokio::test]
async fn clear_routes_with_an_active_gated_deployment_is_refused_before_the_patch() {
    let server = RouteStub::active(
        vec![deployment_row(DEP_DEV, "dev", "active", VERSION_B)],
        vec![(VERSION_B, files_with(&manifest_declaring(&["ops"])))],
    )
    .serve();

    let result = run_approvals(&server, clear(), false).await;

    assert_no_patch(&server);
    let (message, fix) = usage_refusal(result, "clear while a deployment declares ops");
    let environment_and_version = format!("(dev, version {VERSION_B})");
    for needle in ["\"ops\"", DEP_DEV, environment_and_version.as_str()] {
        assert!(
            message.contains(needle),
            "message must name {needle:?}: {message}"
        );
    }
    assert!(
        fix.contains("DELETE /deployments/"),
        "the fix names how to end a deployment: {fix}"
    );
    assert!(
        fix.contains("deploying a version that declares no gates is not enough"),
        "the fix warns that a new ungated deploy does not end the old one: {fix}"
    );
}

#[tokio::test]
async fn clear_routes_after_the_deployment_ended_sends_the_patch() {
    let server = RouteStub::active(
        vec![
            deployment_row(DEP_DEV, "dev", "ended", VERSION_B),
            deployment_row(DEP_DEV_2, "dev", "active", VERSION_A),
        ],
        vec![
            (VERSION_A, files_with(&manifest_declaring(&[]))),
            (VERSION_B, files_with(&manifest_declaring(&["ops"]))),
        ],
    )
    .serve();

    expect_routes(
        run_approvals(&server, clear(), false).await,
        "no active deployment declares a route",
    );

    let agent_path = format!("/agents/{AGENT_ID}");
    let patch = server
        .recorded()
        .into_iter()
        .find(|r| r.method == "PATCH" && r.path == agent_path)
        .expect("the clear must be sent");
    let body: Value = serde_json::from_slice(&patch.body).expect("PATCH body is JSON");
    assert_eq!(body, json!({"approval_routes": {}}));
}

#[tokio::test]
async fn route_resolution_write_that_omits_a_declared_route_is_refused() {
    let server = RouteStub::active(
        vec![deployment_row(DEP_DEV, "dev", "active", VERSION_B)],
        vec![(
            VERSION_B,
            files_with(&manifest_declaring(&["ops", "finance"])),
        )],
    )
    .serve();

    let result = run_approvals(
        &server,
        ApprovalCmd {
            route_resolution: vec!["finance=C0EXAMPLE1".to_string()],
            ..ApprovalCmd::default()
        },
        false,
    )
    .await;

    assert_no_patch(&server);
    let (message, _) = usage_refusal(result, "write keeping finance, dropping ops");
    assert!(
        message.contains("\"ops\""),
        "names the removed route: {message}"
    );
    assert!(
        !message.contains("\"finance\""),
        "a route the write keeps is not reported as removed: {message}"
    );
}

#[tokio::test]
async fn route_resolution_write_that_keeps_every_declared_route_is_sent() {
    let server = RouteStub::active(
        vec![deployment_row(DEP_DEV, "dev", "active", VERSION_B)],
        vec![(
            VERSION_B,
            files_with(&manifest_declaring(&["ops", "finance"])),
        )],
    )
    .serve();

    expect_routes(
        run_approvals(
            &server,
            ApprovalCmd {
                route_resolution: vec![
                    "ops=C0EXAMPLE1".to_string(),
                    "finance=C0EXAMPLE2".to_string(),
                ],
                ..ApprovalCmd::default()
            },
            false,
        )
        .await,
        "every declared route kept",
    );
    assert!(patched(&server), "the write is sent: {:?}", flow(&server));
}

#[tokio::test]
async fn routes_from_empty_object_is_refused_like_clear() {
    let server = RouteStub::active(
        vec![deployment_row(DEP_DEV, "dev", "active", VERSION_B)],
        vec![(VERSION_B, files_with(&manifest_declaring(&["ops"])))],
    )
    .serve();
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("routes.json");
    std::fs::write(&path, "{}").expect("write routes file");

    let result = run_approvals(
        &server,
        ApprovalCmd {
            routes_from: Some(path),
            ..ApprovalCmd::default()
        },
        false,
    )
    .await;

    assert_no_patch(&server);
    let (message, _) = usage_refusal(result, "an empty --routes-from map");
    assert!(
        message.contains("\"ops\""),
        "names the removed route: {message}"
    );
}

#[tokio::test]
async fn prod_and_dev_deployments_are_both_judged() {
    let server = RouteStub::active(
        vec![
            deployment_row(DEP_PROD, "prod", "active", VERSION_A),
            deployment_row(DEP_DEV, "dev", "active", VERSION_B),
        ],
        vec![
            (VERSION_A, files_with(&manifest_declaring(&[]))),
            (VERSION_B, files_with(&manifest_declaring(&["ops"]))),
        ],
    )
    .serve();

    let result = run_approvals(&server, clear(), false).await;

    assert_no_patch(&server);
    let (message, _) = usage_refusal(result, "dev declares ops while prod is ungated");
    assert!(
        message.contains(DEP_DEV),
        "names the dev deployment that declares the route: {message}"
    );
    assert!(
        !message.contains(DEP_PROD),
        "the ungated prod deployment declares nothing: {message}"
    );
}

#[tokio::test]
async fn deployment_listing_failure_warns_and_still_sends_the_patch() {
    let server = RouteStub {
        deployments: (500, r#"{"detail":"upstream unavailable"}"#.to_string()),
        files: vec![],
    }
    .serve();

    expect_routes(
        run_approvals(&server, clear(), false).await,
        "an unreadable deployment list fails open",
    );
    assert!(
        sent(&server, "GET", "/deployments"),
        "the deployment list must have been read: {:?}",
        flow(&server)
    );
    assert!(patched(&server), "the write is sent: {:?}", flow(&server));
}

#[tokio::test]
async fn bundle_files_failure_warns_and_still_sends_the_patch() {
    let server = RouteStub::active(
        vec![deployment_row(DEP_DEV, "dev", "active", VERSION_B)],
        vec![(
            VERSION_B,
            (500, r#"{"detail":"storage unavailable"}"#.to_string()),
        )],
    )
    .serve();

    expect_routes(
        run_approvals(&server, clear(), false).await,
        "unreadable bundle files fail open",
    );
    assert!(
        sent(
            &server,
            "GET",
            &format!("/agents/{AGENT_ID}/versions/{VERSION_B}/files")
        ),
        "the deployed files must have been read: {:?}",
        flow(&server)
    );
    assert!(patched(&server), "the write is sent: {:?}", flow(&server));
}

#[tokio::test]
async fn unparseable_deployed_manifest_warns_and_still_sends_the_patch() {
    let server = RouteStub::active(
        vec![deployment_row(DEP_DEV, "dev", "active", VERSION_B)],
        vec![(VERSION_B, files_with("not json"))],
    )
    .serve();

    expect_routes(
        run_approvals(&server, clear(), false).await,
        "an unparseable deployed manifest fails open",
    );
    assert!(
        sent(
            &server,
            "GET",
            &format!("/agents/{AGENT_ID}/versions/{VERSION_B}/files")
        ),
        "the deployed manifest must have been read: {:?}",
        flow(&server)
    );
    assert!(patched(&server), "the write is sent: {:?}", flow(&server));
}

#[tokio::test]
async fn a_readable_violation_still_refuses_when_another_version_is_unreadable() {
    let server = RouteStub::active(
        vec![
            deployment_row(DEP_PROD, "prod", "active", VERSION_A),
            deployment_row(DEP_DEV, "dev", "active", VERSION_B),
        ],
        vec![
            (
                VERSION_A,
                (500, r#"{"detail":"storage unavailable"}"#.to_string()),
            ),
            (VERSION_B, files_with(&manifest_declaring(&["ops"]))),
        ],
    )
    .serve();

    let result = run_approvals(&server, clear(), false).await;

    assert_no_patch(&server);
    let (message, _) = usage_refusal(result, "one unreadable version, one declaring ops");
    assert!(
        message.contains(DEP_DEV),
        "names the declaring deployment: {message}"
    );
}

#[tokio::test]
async fn dry_run_route_write_makes_no_request() {
    let server = serve(|_| Response::json(500, UNEXPECTED));

    match run_approvals(&server, clear(), true).await {
        Ok(ApprovalsOutput::DryRun(_)) => {}
        Ok(_) => panic!("expected the dry-run plan"),
        Err(err) => panic!("a dry-run route write must stay offline: {err:#}"),
    }
    assert!(
        server.recorded().is_empty(),
        "a dry run sends nothing: {:?}",
        flow(&server)
    );
}

#[tokio::test]
async fn list_routes_reads_no_deployments() {
    let server = RouteStub::active(
        vec![deployment_row(DEP_DEV, "dev", "active", VERSION_B)],
        vec![(VERSION_B, files_with(&manifest_declaring(&["ops"])))],
    )
    .serve();

    expect_routes(
        run_approvals(
            &server,
            ApprovalCmd {
                list_routes: true,
                ..ApprovalCmd::default()
            },
            false,
        )
        .await,
        "--list-routes reads",
    );
    assert!(
        !flow(&server).iter().any(|(_, p)| p == "/deployments"),
        "a route read runs no pre-check: {:?}",
        flow(&server)
    );
}

#[tokio::test]
async fn repeated_version_across_deployments_fetches_files_once() {
    let server = RouteStub::active(
        vec![
            deployment_row(DEP_DEV, "dev", "active", VERSION_B),
            deployment_row(DEP_PROD, "prod", "active", VERSION_B),
        ],
        vec![(VERSION_B, files_with(&manifest_declaring(&["ops"])))],
    )
    .serve();

    let result = run_approvals(&server, clear(), false).await;

    assert_no_patch(&server);
    let (message, _) = usage_refusal(result, "two deployments on one declaring version");
    assert!(
        message.contains(DEP_DEV),
        "names the dev deployment: {message}"
    );
    assert!(
        message.contains(DEP_PROD),
        "names the prod deployment: {message}"
    );
    assert_eq!(
        files_gets(&server),
        1,
        "one version is read once: {:?}",
        flow(&server)
    );
}

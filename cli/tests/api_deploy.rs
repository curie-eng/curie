//! Integration: the platform API client's deploy flow against the OpenAPI
//! contract shapes (apps/api openapi.json), served by a wire-level test server.

mod support;

use curie::api::{ApiClient, ChannelOutcome, DeployOutcome};
use curie::bundle::pack_tar_gz;
use curie::commands::{self, DeployOpts};
use curie::scaffold::scaffold;
#[cfg(unix)]
use std::ffi::OsString;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
#[cfg(unix)]
use std::path::{Path, PathBuf};
#[cfg(unix)]
use std::process::Command;
use support::{serve, MockServer, Response};

const AGENT_ID: &str = "11111111-1111-1111-1111-111111111111";
const AGENT_NAME: &str = "deal-desk";
const VERSION_ID: &str = "22222222-2222-2222-2222-222222222222";
const DEPLOYMENT_ID: &str = "33333333-3333-3333-3333-333333333333";

/// The channel the fixture agent is already bound to.
const BOUND: &str = "C0EXAMPLE1";
/// A second, not-yet-bound channel: what `--slack-channel` asks to ADD.
const OTHER: &str = "C0EXAMPLE2";

#[cfg(unix)]
static PATH_ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[cfg(unix)]
struct GitEnvGuard {
    path: Option<OsString>,
    real_git: Option<OsString>,
    count_file: Option<OsString>,
}

#[cfg(unix)]
impl GitEnvGuard {
    fn install(real_git: &Path, count_file: &Path, wrapper_dir: &Path) -> Self {
        let guard = Self {
            path: std::env::var_os("PATH"),
            real_git: std::env::var_os("CURIE_TEST_REAL_GIT"),
            count_file: std::env::var_os("CURIE_TEST_GIT_COUNT"),
        };
        let original_path = guard.path.clone().unwrap_or_default();
        let paths =
            std::iter::once(wrapper_dir.to_path_buf()).chain(std::env::split_paths(&original_path));
        std::env::set_var("PATH", std::env::join_paths(paths).expect("join PATH"));
        std::env::set_var("CURIE_TEST_REAL_GIT", real_git);
        std::env::set_var("CURIE_TEST_GIT_COUNT", count_file);
        guard
    }
}

#[cfg(unix)]
impl Drop for GitEnvGuard {
    fn drop(&mut self) {
        for (name, value) in [
            ("PATH", self.path.take()),
            ("CURIE_TEST_REAL_GIT", self.real_git.take()),
            ("CURIE_TEST_GIT_COUNT", self.count_file.take()),
        ] {
            match value {
                Some(value) => std::env::set_var(name, value),
                None => std::env::remove_var(name),
            }
        }
    }
}

#[cfg(unix)]
fn real_git() -> PathBuf {
    std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default())
        .map(|dir| dir.join("git"))
        .find(|path| path.is_file())
        .expect("git should be on PATH")
        .canonicalize()
        .expect("canonicalize git")
}

#[cfg(unix)]
fn run_git(git: &Path, cwd: &Path, args: &[&str]) -> String {
    let output = Command::new(git)
        .current_dir(cwd)
        .args(args)
        .output()
        .expect("run git");
    assert!(
        output.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .expect("git output should be UTF 8")
        .trim()
        .to_string()
}

#[cfg(unix)]
async fn run_command_deploy(server: &MockServer, plugin_dir: &Path) -> commands::DeployOutput {
    commands::deploy(DeployOpts {
        tier: commands::DeployTier::Local,
        agent: None,
        target: None,
        plugin_dir: plugin_dir.to_path_buf(),
        api_url: server.base_url.clone(),
        api_key: "test-key".to_string(),
        slack_channel: Some("C0EXAMPLE1".to_string()),
        repo: None,
        env: None,
        label: Some("0.1.0-1".to_string()),
        secret: vec![],
        secret_binding_supported: true,
        connect_hint: "mock API should be reachable".to_string(),
    })
    .await
    .unwrap()
}

#[cfg(unix)]
fn assert_command_deploy_wire(server: &MockServer, commit_sha: Option<&str>) {
    let recorded = server.recorded();
    let flow: Vec<(String, String)> = recorded
        .iter()
        .map(|request| (request.method.clone(), request.path.clone()))
        .collect();
    assert_eq!(
        flow,
        vec![
            ("GET".to_string(), "/agents".to_string()),
            ("POST".to_string(), "/agents".to_string()),
            ("POST".to_string(), format!("/agents/{AGENT_ID}/versions"),),
            (
                "PUT".to_string(),
                format!("/agents/{AGENT_ID}/versions/{VERSION_ID}/bundle"),
            ),
            ("POST".to_string(), "/deployments".to_string()),
        ]
    );

    let version_request = &recorded[2];
    let version_body: serde_json::Value =
        serde_json::from_slice(&version_request.body).expect("version body should be JSON");
    let deployment_request = &recorded[4];
    let deployment_body: serde_json::Value =
        serde_json::from_slice(&deployment_request.body).expect("deployment body should be JSON");

    if let Some(commit_sha) = commit_sha {
        assert_eq!(
            version_body,
            serde_json::json!({
                "version_label": "0.1.0-1",
                "created_by": std::env::var("USER").unwrap_or_else(|_| "curie-cli".to_string()),
                "commit_sha": commit_sha,
            })
        );
        assert_eq!(
            deployment_body,
            serde_json::json!({
                "agent_id": AGENT_ID,
                "version_id": VERSION_ID,
                "environment": "dev",
                "commit_sha": commit_sha,
            })
        );
    } else {
        assert_eq!(
            version_body,
            serde_json::json!({
                "version_label": "0.1.0-1",
                "created_by": std::env::var("USER").unwrap_or_else(|_| "curie-cli".to_string()),
                "commit_sha": null,
            })
        );
        assert_eq!(
            deployment_body,
            serde_json::json!({
                "agent_id": AGENT_ID,
                "version_id": VERSION_ID,
                "environment": "dev",
                "commit_sha": null,
            })
        );
    }
}

#[cfg(unix)]
#[tokio::test]
async fn command_deploy_uses_head_only_for_a_clean_git_bundle_and_null_sha_otherwise() {
    let git = real_git();
    let repo = tempfile::tempdir().unwrap();
    let bundle = repo.path().join("nested/deal-desk");
    std::fs::create_dir_all(&bundle).unwrap();
    scaffold(&bundle, "deal-desk").unwrap();
    std::fs::write(bundle.join(".gitignore"), ".curie/\nignored-packed.txt\n").unwrap();
    std::fs::write(bundle.join(".curieignore"), "# committed exclusions\n").unwrap();
    std::fs::create_dir(bundle.join(".curie")).unwrap();
    std::fs::write(bundle.join(".curie/runner.json"), "ignored runtime state\n").unwrap();
    run_git(&git, repo.path(), &["init", "--quiet"]);
    run_git(&git, repo.path(), &["add", "."]);
    run_git(
        &git,
        repo.path(),
        &[
            "-c",
            "user.name=Curie Test",
            "-c",
            "user.email=curie@example.com",
            "commit",
            "--quiet",
            "-m",
            "Initial bundle",
        ],
    );
    let bundle_head = run_git(&git, repo.path(), &["rev-parse", "HEAD"]);
    assert_eq!(bundle_head.len(), 40, "expected a full commit SHA");
    let outer_head = run_git(
        &git,
        Path::new(env!("CARGO_MANIFEST_DIR")),
        &["rev-parse", "HEAD"],
    );
    assert_ne!(bundle_head, outer_head, "fixture must have its own commit");

    let wrapper_dir = tempfile::tempdir().unwrap();
    let wrapper = wrapper_dir.path().join("git");
    std::fs::write(
        &wrapper,
        r#"#!/bin/sh
count=0
if [ "$#" -eq 2 ] && [ "$1" = 'rev-parse' ] && [ "$2" = 'HEAD' ]; then
  if [ -f "$CURIE_TEST_GIT_COUNT" ]; then
    count=$(cat "$CURIE_TEST_GIT_COUNT")
  fi
  count=$((count + 1))
  printf '%s\n' "$count" > "$CURIE_TEST_GIT_COUNT"
  if [ "$count" -gt 1 ]; then
    printf '%s\n' '0000000000000000000000000000000000000000'
    exit 0
  fi
fi
exec "$CURIE_TEST_REAL_GIT" "$@"
"#,
    )
    .unwrap();
    let mut permissions = std::fs::metadata(&wrapper).unwrap().permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(&wrapper, permissions).unwrap();
    let count_file = wrapper_dir.path().join("git-count");
    let _path_env = PATH_ENV_LOCK.lock().await;
    let git_env = GitEnvGuard::install(&git, &count_file, wrapper_dir.path());

    let git_server = serve(|req| route(&req.method, &req.path));
    let git_outcome = run_command_deploy(&git_server, &bundle).await;
    let lookup_count = std::fs::read_to_string(&count_file).unwrap();

    assert_eq!(git_outcome.bundle_sha256, "deadbeef");
    assert_eq!(lookup_count.trim(), "1", "HEAD must be resolved once");
    assert_command_deploy_wire(&git_server, Some(&bundle_head));

    let tracked_path = bundle.join(".claude-plugin/plugin.json");
    let tracked_content = std::fs::read_to_string(&tracked_path).unwrap();
    std::fs::write(&tracked_path, format!("{tracked_content}\n")).unwrap();
    let tracked_dirty_server = serve(|req| route(&req.method, &req.path));
    let tracked_dirty_outcome = run_command_deploy(&tracked_dirty_server, &bundle).await;

    assert_eq!(tracked_dirty_outcome.bundle_sha256, "deadbeef");
    assert_command_deploy_wire(&tracked_dirty_server, None);

    std::fs::write(&tracked_path, tracked_content).unwrap();
    std::fs::write(bundle.join("uncommitted.txt"), "dirty bundle\n").unwrap();
    let dirty_server = serve(|req| route(&req.method, &req.path));
    let dirty_outcome = run_command_deploy(&dirty_server, &bundle).await;

    assert_eq!(dirty_outcome.bundle_sha256, "deadbeef");
    assert_command_deploy_wire(&dirty_server, None);

    std::fs::remove_file(bundle.join("uncommitted.txt")).unwrap();
    std::fs::write(bundle.join("ignored-packed.txt"), "ignored but packed\n").unwrap();
    let ignored_server = serve(|req| route(&req.method, &req.path));
    let ignored_outcome = run_command_deploy(&ignored_server, &bundle).await;

    assert_eq!(ignored_outcome.bundle_sha256, "deadbeef");
    assert_command_deploy_wire(&ignored_server, None);

    std::fs::remove_file(bundle.join("ignored-packed.txt")).unwrap();
    std::fs::write(bundle.join(".curieignore"), "generated-output/\n").unwrap();
    let exclusions_dirty_server = serve(|req| route(&req.method, &req.path));
    let exclusions_dirty_outcome = run_command_deploy(&exclusions_dirty_server, &bundle).await;

    assert_eq!(exclusions_dirty_outcome.bundle_sha256, "deadbeef");
    assert_command_deploy_wire(&exclusions_dirty_server, None);

    drop(git_env);

    let non_git_bundle = tempfile::tempdir().unwrap();
    scaffold(non_git_bundle.path(), "deal-desk").unwrap();
    let non_git_server = serve(|req| route(&req.method, &req.path));
    let non_git_outcome = run_command_deploy(&non_git_server, non_git_bundle.path()).await;

    assert_eq!(non_git_outcome.bundle_sha256, "deadbeef");
    assert_command_deploy_wire(&non_git_server, None);
}

fn route(method: &str, path: &str) -> Response {
    match (method, path) {
        ("GET", "/agents") => Response::json(200, "[]"),
        ("POST", "/agents") => Response::json(
            201,
            &format!(
                r##"{{"id":"{AGENT_ID}","name":"deal-desk","channels":[{{"kind":"slack","address":"#local-dev"}}],"created_at":"2026-07-05T00:00:00Z"}}"##
            ),
        ),
        ("POST", p) if p == format!("/agents/{AGENT_ID}/versions") => Response::json(
            201,
            &format!(
                r#"{{"id":"{VERSION_ID}","agent_id":"{AGENT_ID}","version_label":"0.1.0-1","bundle_ref":null,"bundle_sha256":null,"created_by":"tester","created_at":"2026-07-05T00:00:00Z"}}"#
            ),
        ),
        ("PUT", p) if p == format!("/agents/{AGENT_ID}/versions/{VERSION_ID}/bundle") => {
            Response::json(
                201,
                &format!(
                    r#"{{"version_id":"{VERSION_ID}","bundle_ref":"bundles/x.tar.gz","bundle_sha256":"deadbeef","size_bytes":512}}"#
                ),
            )
        }
        ("POST", "/deployments") => Response::json(
            201,
            &format!(
                r#"{{"id":"{DEPLOYMENT_ID}","agent_id":"{AGENT_ID}","version_id":"{VERSION_ID}","environment":"dev","status":"active","deployed_at":"2026-07-05T00:00:00Z"}}"#
            ),
        ),
        other => panic!("unexpected request: {other:?}"),
    }
}

#[tokio::test]
async fn deploy_walks_the_full_contract_flow_with_auth() {
    let server = serve(|req| route(&req.method, &req.path));
    let client = ApiClient::new(&server.base_url, "test-key").unwrap();

    let dir = tempfile::tempdir().unwrap();
    scaffold(dir.path(), "deal-desk").unwrap();
    let archive = pack_tar_gz(dir.path()).unwrap();

    let outcome = client
        .deploy(
            "deal-desk",
            Some("#local-dev"),
            "0.1.0-1",
            "tester",
            "dev",
            archive,
            &std::collections::BTreeMap::new(),
            None,
            None,
        )
        .await
        .unwrap();

    assert_eq!(outcome.agent.id, AGENT_ID);
    assert_eq!(outcome.version.id, VERSION_ID);
    assert_eq!(outcome.bundle.bundle_sha256, "deadbeef");
    assert_eq!(outcome.deployment.id, DEPLOYMENT_ID);
    assert_eq!(outcome.deployment.environment, "dev");

    let recorded = server.recorded();
    let flow: Vec<(String, String)> = recorded
        .iter()
        .map(|r| (r.method.clone(), r.path.clone()))
        .collect();
    assert_eq!(
        flow,
        vec![
            ("GET".to_string(), "/agents".to_string()),
            ("POST".to_string(), "/agents".to_string()),
            ("POST".to_string(), format!("/agents/{AGENT_ID}/versions")),
            (
                "PUT".to_string(),
                format!("/agents/{AGENT_ID}/versions/{VERSION_ID}/bundle")
            ),
            ("POST".to_string(), "/deployments".to_string()),
        ]
    );
    for request in &recorded {
        assert_eq!(request.header("x-api-key"), Some("test-key"));
    }

    // The bundle upload is multipart with the archive under the `file` field.
    let upload = &recorded[3];
    assert!(upload
        .header("content-type")
        .unwrap()
        .starts_with("multipart/form-data"));
    let body = String::from_utf8_lossy(&upload.body);
    assert!(body.contains("name=\"file\""));
    assert!(body.contains("filename=\"bundle.tar.gz\""));
}

#[tokio::test]
async fn reuses_an_existing_agent_instead_of_creating() {
    let server = serve(|req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => Response::json(
            200,
            &format!(
                r##"[{{"id":"{AGENT_ID}","name":"deal-desk","channels":[{{"kind":"slack","address":"#x"}}],"created_at":"2026-07-05T00:00:00Z"}}]"##
            ),
        ),
        other => panic!("unexpected request: {other:?}"),
    });
    let client = ApiClient::new(&server.base_url, "k").unwrap();
    let agent = client
        .find_or_create_agent("deal-desk", "#local-dev")
        .await
        .unwrap();
    assert_eq!(agent.id, AGENT_ID);
    assert_eq!(server.recorded().len(), 1);
}

/// The version/bundle/deployment tail of the deploy flow, shared by the
/// channel-reconciliation tests (which differ only in the agent-resolution head).
fn deploy_tail(method: &str, path: &str) -> Option<Response> {
    match (method, path) {
        ("POST", p) if p == format!("/agents/{AGENT_ID}/versions") => Some(Response::json(
            201,
            &format!(
                r#"{{"id":"{VERSION_ID}","agent_id":"{AGENT_ID}","version_label":"0.1.0-1","bundle_ref":null,"bundle_sha256":null,"created_by":"tester","created_at":"2026-07-05T00:00:00Z"}}"#
            ),
        )),
        ("PUT", p) if p == format!("/agents/{AGENT_ID}/versions/{VERSION_ID}/bundle") => {
            Some(Response::json(
                201,
                &format!(
                    r#"{{"version_id":"{VERSION_ID}","bundle_ref":"bundles/x.tar.gz","bundle_sha256":"deadbeef","size_bytes":512}}"#
                ),
            ))
        }
        ("POST", "/deployments") => Some(Response::json(
            201,
            &format!(
                r#"{{"id":"{DEPLOYMENT_ID}","agent_id":"{AGENT_ID}","version_id":"{VERSION_ID}","environment":"dev","status":"active","deployed_at":"2026-07-05T00:00:00Z"}}"#
            ),
        )),
        _ => None,
    }
}

/// One agent's wire JSON, with one or more channel bindings under the plural
/// `channels` key (ADR-0116). `repo` emits the `repo_full_name` key only when
/// the agent is bound, so an unbound agent travels as an ABSENT key and
/// exercises the field's real `#[serde(default)]` path rather than an explicit
/// null.
fn agent_json_channels(id: &str, name: &str, channels: &[&str], repo: Option<&str>) -> String {
    let bound = match repo {
        Some(repo) => format!(r#","repo_full_name":"{repo}""#),
        None => String::new(),
    };
    let bindings = channels
        .iter()
        .map(|c| format!(r#"{{"kind":"slack","address":"{c}"}}"#))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        r#"{{"id":"{id}","name":"{name}","channels":[{bindings}],"created_at":"2026-07-05T00:00:00Z"{bound}}}"#
    )
}

/// The single-binding case, which is what most fixtures need.
fn agent_json(id: &str, name: &str, channel: &str, repo: Option<&str>) -> String {
    agent_json_channels(id, name, &[channel], repo)
}

/// The `GET /agents` listing that resolution reads: the one agent under test,
/// as the platform would report it.
fn existing_agents(agent: &str) -> Response {
    Response::json(200, &format!("[{agent}]"))
}

/// The `PATCH /agents/{id}` response: the agent as the API stored it. The
/// deploy must report THIS row, never a locally patched copy of the listed one,
/// or the CLI can claim a binding the API never took.
fn patched_agent(channel: &str, repo: Option<&str>) -> Response {
    Response::json(200, &agent_json(AGENT_ID, AGENT_NAME, channel, repo))
}

/// Every recorded `PATCH /agents/{id}` body, parsed. The body on the wire is
/// the contract under test: what the CLI SENT, not what it decided internally.
fn patch_bodies(server: &MockServer) -> Vec<serde_json::Value> {
    server
        .recorded()
        .into_iter()
        .filter(|r| r.method == "PATCH" && r.path == format!("/agents/{AGENT_ID}"))
        .map(|r| serde_json::from_slice(&r.body).expect("PATCH body should be JSON"))
        .collect()
}

/// Assert that the deploy issued no `PATCH /agents/{id}` at all.
///
/// This assertion is only load-bearing because the no-PATCH tests ANSWER an
/// unexpected PATCH instead of panicking on it. The mock records a request
/// only AFTER its handler returns (`cli/tests/support/mod.rs`), so a handler
/// that panics on a PATCH means the PATCH is never recorded: the check then
/// runs over a list that could not contain the thing it looks for and passes
/// no matter what the CLI did. Such a test goes red only through the socket
/// error the unwound handler thread causes, which is red for the wrong reason
/// and is equally red for unrelated breakage. Answering keeps the request in
/// the recording, so "the CLI sent a PATCH it must not send" is what fails,
/// and the offending body is the failure message.
fn assert_no_patch(server: &MockServer) {
    let patches: Vec<String> = server
        .recorded()
        .iter()
        .filter(|r| r.method == "PATCH")
        .map(|r| format!("{} {}", r.path, String::from_utf8_lossy(&r.body)))
        .collect();
    assert!(
        patches.is_empty(),
        "no PATCH should have been issued, got {patches:?}"
    );
}

/// The path the channel subresource lives at (ADR-0116, S3): a binding is
/// ADDED with a POST to the agent's own collection, never PATCHed onto the
/// agent row.
fn channels_path() -> String {
    format!("/agents/{AGENT_ID}/channels")
}

/// Every recorded `POST /agents/{id}/channels` body, parsed. The body on the
/// wire is the contract under test.
fn channel_post_bodies(server: &MockServer) -> Vec<serde_json::Value> {
    server
        .recorded()
        .into_iter()
        .filter(|r| r.method == "POST" && r.path == channels_path())
        .map(|r| serde_json::from_slice(&r.body).expect("channel POST body should be JSON"))
        .collect()
}

/// Assert the deploy issued no binding WRITE of any kind: no PATCH, and no
/// POST/DELETE against the channel subresource. Ensure-bound must be a no-op
/// when the end state already holds, and "no write" is the whole claim.
///
/// Like [`assert_no_patch`], this is only load-bearing because the no-write
/// tests ANSWER an unexpected write rather than panicking on it (see that
/// function's doc comment for why a panicking handler makes the check vacuous).
fn assert_no_binding_write(server: &MockServer) {
    let writes: Vec<String> = server
        .recorded()
        .iter()
        .filter(|r| {
            r.method == "PATCH"
                || ((r.method == "POST" || r.method == "DELETE") && r.path == channels_path())
        })
        .map(|r| {
            format!(
                "{} {} {}",
                r.method,
                r.path,
                String::from_utf8_lossy(&r.body)
            )
        })
        .collect();
    assert!(
        writes.is_empty(),
        "no binding write should have been issued, got {writes:?}"
    );
}

/// The recorded `(method, path)` flow, for order assertions.
fn flow(server: &MockServer) -> Vec<(String, String)> {
    server
        .recorded()
        .iter()
        .map(|r| (r.method.clone(), r.path.clone()))
        .collect()
}

async fn try_deploy(
    client: &ApiClient,
    channel: Option<&str>,
    repo: Option<&str>,
) -> anyhow::Result<DeployOutcome> {
    let dir = tempfile::tempdir().unwrap();
    scaffold(dir.path(), AGENT_NAME).unwrap();
    let archive = pack_tar_gz(dir.path()).unwrap();
    client
        .deploy(
            AGENT_NAME,
            channel,
            "0.1.0-1",
            "tester",
            "dev",
            archive,
            &std::collections::BTreeMap::new(),
            repo,
            None,
        )
        .await
}

async fn run_deploy(
    client: &ApiClient,
    channel: Option<&str>,
    repo: Option<&str>,
) -> DeployOutcome {
    try_deploy(client, channel, repo).await.unwrap()
}

#[tokio::test]
async fn redeploy_with_a_new_channel_adds_a_binding() {
    // ENSURE-BOUND (ADR-0116, D3): an agent already on C0EXAMPLE1 plus
    // `--slack-channel C0EXAMPLE2` gains a SECOND binding. The write is a POST
    // to the channel subresource, never the PATCH the retired one-channel rule
    // used to issue -- a PATCH here would MOVE the binding and silently
    // unroute the first channel, which is the whole defect this closes.
    let server = serve(|req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => existing_agents(&agent_json(AGENT_ID, AGENT_NAME, BOUND, None)),
        ("POST", p) if *p == channels_path() => Response::json(
            201,
            &agent_json_channels(AGENT_ID, AGENT_NAME, &[BOUND, OTHER], None),
        ),
        // Answered, never panicked, so a PATCH would be RECORDED and
        // `assert_no_patch` is what fails. See its doc comment.
        ("PATCH", p) if *p == format!("/agents/{AGENT_ID}") => patched_agent(OTHER, None),
        (m, p) => deploy_tail(m, p).unwrap_or_else(|| panic!("unexpected request: {m} {p}")),
    });
    let client = ApiClient::new(&server.base_url, "k").unwrap();

    run_deploy(&client, Some(OTHER), None).await;

    let posts = channel_post_bodies(&server);
    assert_eq!(
        posts.len(),
        1,
        "exactly one binding add, got {posts:?} in {:?}",
        flow(&server)
    );
    // Both sub-fields, so a bare string cannot pass as a binding.
    assert_eq!(posts[0]["kind"], "slack");
    assert_eq!(posts[0]["address"], OTHER);
    assert_no_patch(&server);
}

#[tokio::test]
async fn redeploy_with_an_already_bound_channel_sends_no_write() {
    // Ensure-bound is a statement about the END STATE: the agent already holds
    // this pair, so the deploy writes nothing. A blind POST would round-trip a
    // 409 on every routine redeploy.
    let server = serve(|req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => existing_agents(&agent_json(AGENT_ID, AGENT_NAME, BOUND, None)),
        // Answered, never panicked, so a needless write would be RECORDED.
        ("POST", p) if *p == channels_path() => Response::json(
            201,
            &agent_json_channels(AGENT_ID, AGENT_NAME, &[BOUND], None),
        ),
        ("PATCH", p) if *p == format!("/agents/{AGENT_ID}") => patched_agent(BOUND, None),
        (m, p) => deploy_tail(m, p).unwrap_or_else(|| panic!("unexpected request: {m} {p}")),
    });
    let client = ApiClient::new(&server.base_url, "k").unwrap();

    run_deploy(&client, Some(BOUND), None).await;

    assert_no_binding_write(&server);
}

#[tokio::test]
async fn redeploy_without_channel_does_not_write() {
    // Omitting `--slack-channel` on a redeploy must leave the agent's BINDING
    // SET untouched: nothing is added, and nothing is ever removed.
    let server = serve(|req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => existing_agents(&agent_json(AGENT_ID, AGENT_NAME, BOUND, None)),
        // Answered, never panicked, so a stray write would be RECORDED and
        // `assert_no_binding_write` is what fails. See `assert_no_patch`.
        ("POST", p) if *p == channels_path() => Response::json(
            201,
            &agent_json_channels(AGENT_ID, AGENT_NAME, &[BOUND], None),
        ),
        ("PATCH", p) if *p == format!("/agents/{AGENT_ID}") => patched_agent(BOUND, None),
        (m, p) => deploy_tail(m, p).unwrap_or_else(|| panic!("unexpected request: {m} {p}")),
    });
    let client = ApiClient::new(&server.base_url, "k").unwrap();

    run_deploy(&client, None, None).await;

    assert_no_binding_write(&server);
}

/// A `GET /agents` (list) or `GET /agents/{id}` (single) read of the agent.
/// The recheck may use either; both are answered so the test asserts on the
/// OUTCOME rather than pinning which read the implementation picks.
fn is_agent_read(method: &str, path: &str) -> bool {
    method == "GET" && (path == "/agents" || path == format!("/agents/{AGENT_ID}"))
}

/// Answer an agent read with `channels`, shaped for whichever of the two read
/// paths asked (a list response for the collection, a bare object for the id).
fn agent_read(path: &str, channels: &[&str]) -> Response {
    let agent = agent_json_channels(AGENT_ID, AGENT_NAME, channels, None);
    if path == "/agents" {
        existing_agents(&agent)
    } else {
        Response::json(200, &agent)
    }
}

#[tokio::test]
async fn redeploy_treats_a_same_agent_409_as_success_after_recheck() {
    // A lost race that reached the SAME END STATE is not a deploy failure.
    // Two deploys of one agent can race on the add; the loser gets the pair's
    // uniqueness 409 back for a binding this very agent now owns. Ensure-bound
    // is about the end state, so the deploy re-reads and succeeds.
    let reads = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let server = serve(move |req| {
        let (m, p) = (req.method.as_str(), req.path.as_str());
        if is_agent_read(m, p) {
            // The FIRST read resolves the agent, and must show the pre-race
            // state or there would be nothing to add. Every later read is the
            // post-409 recheck, which sees the race winner's binding.
            let n = reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            return if n == 0 {
                agent_read(p, &[BOUND])
            } else {
                agent_read(p, &[BOUND, OTHER])
            };
        }
        match (m, p) {
            ("POST", p) if *p == channels_path() => Response::json(
                409,
                r#"{"detail":"that channel is already bound to an agent"}"#,
            ),
            (m, p) => deploy_tail(m, p).unwrap_or_else(|| panic!("unexpected request: {m} {p}")),
        }
    });
    let client = ApiClient::new(&server.base_url, "k").unwrap();

    let result = try_deploy(&client, Some(OTHER), None).await;

    assert!(
        result.is_ok(),
        "a 409 for a pair THIS agent now owns must not fail the deploy: {:?}",
        result.err()
    );
    // The recheck is the mechanism, not an accident: the deploy re-read the
    // agent after the conflict rather than assuming.
    let flow = flow(&server);
    let conflict = flow
        .iter()
        .position(|(m, p)| m == "POST" && *p == channels_path())
        .expect("the deploy must attempt the add");
    assert!(
        flow[conflict + 1..]
            .iter()
            .any(|(m, p)| is_agent_read(m, p)),
        "the 409 must be rechecked against a fresh read: {flow:?}"
    );
    // And the deploy still ran to completion rather than stopping at the add.
    assert!(
        flow.iter().any(|(m, p)| m == "POST" && p == "/deployments"),
        "the deploy must continue past a benign conflict: {flow:?}"
    );
}

#[tokio::test]
async fn redeploy_surfaces_a_409_when_another_agent_owns_the_pair() {
    // The negative twin of the test above, and the reason it is not enough on
    // its own: without this case, "treat a 409 as success" degenerates into
    // swallowing every real conflict. Here the recheck shows the agent STILL
    // does not hold the pair -- another agent owns it -- so the deploy fails
    // and says so instead of reporting a binding that does not exist.
    let server = serve(|req| {
        let (m, p) = (req.method.as_str(), req.path.as_str());
        if is_agent_read(m, p) {
            return agent_read(p, &[BOUND]);
        }
        match (m, p) {
            ("POST", p) if *p == channels_path() => Response::json(
                409,
                r#"{"detail":"channel C0EXAMPLE2 is already bound to another agent"}"#,
            ),
            (m, p) => deploy_tail(m, p).unwrap_or_else(|| panic!("unexpected request: {m} {p}")),
        }
    });
    let client = ApiClient::new(&server.base_url, "k").unwrap();

    // `DeployOutcome` is not `Debug`, so the Ok arm is named explicitly rather
    // than through `expect_err`.
    let err = match try_deploy(&client, Some(OTHER), None).await {
        Ok(_) => panic!("a pair owned by ANOTHER agent must fail the deploy"),
        Err(err) => err,
    };
    let text = format!("{err:#}");
    assert!(
        text.contains("409") || text.contains("already bound to another agent"),
        "the conflict must reach the operator: {text}"
    );
    // And nothing downstream ran on a binding that was never made.
    assert!(
        !flow(&server)
            .iter()
            .any(|(m, p)| m == "POST" && p == "/deployments"),
        "a real conflict must abort the deploy: {:?}",
        flow(&server)
    );
}

/// The summary-line half of the two tests above: they pin the WIRE, this pins
/// what the operator is told.
#[tokio::test]
async fn redeploy_reports_the_binding_set_it_ended_with() {
    let server = serve(|req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => existing_agents(&agent_json(AGENT_ID, AGENT_NAME, BOUND, None)),
        ("POST", p) if *p == channels_path() => Response::json(
            201,
            &agent_json_channels(AGENT_ID, AGENT_NAME, &[BOUND, OTHER], None),
        ),
        (m, p) => deploy_tail(m, p).unwrap_or_else(|| panic!("unexpected request: {m} {p}")),
    });
    let client = ApiClient::new(&server.base_url, "k").unwrap();

    let added = run_deploy(&client, Some(OTHER), None).await;
    assert_eq!(
        added.channel,
        ChannelOutcome::Added {
            address: OTHER.to_string()
        },
        "an added binding reports the address it added, not a move"
    );

    // No flag passed: every binding is reported, so an operator can see that
    // a second channel is live rather than only the one they last named.
    let server = serve(|req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => existing_agents(&agent_json_channels(
            AGENT_ID,
            AGENT_NAME,
            &[BOUND, OTHER],
            None,
        )),
        (m, p) => deploy_tail(m, p).unwrap_or_else(|| panic!("unexpected request: {m} {p}")),
    });
    let client = ApiClient::new(&server.base_url, "k").unwrap();
    let unchanged = run_deploy(&client, None, None).await;
    assert_eq!(
        unchanged.channel,
        ChannelOutcome::Unchanged {
            channels: vec![BOUND.to_string(), OTHER.to_string()],
            passed: false,
        }
    );
}

#[tokio::test]
async fn deploy_binds_an_unbound_agents_repo() {
    // An agent that already exists with NO repo binding is bound by this
    // deploy, not told to recreate itself: `AgentUpdate` has carried
    // `repo_full_name` since ADR-0091 / #1194, and until #1212 the CLI kept
    // behaving as though it did not.
    let server = serve(|req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => existing_agents(&agent_json(AGENT_ID, AGENT_NAME, BOUND, None)),
        ("PATCH", p) if *p == format!("/agents/{AGENT_ID}") => {
            patched_agent(BOUND, Some("acme/bundle"))
        }
        (m, p) => deploy_tail(m, p).unwrap_or_else(|| panic!("unexpected request: {m} {p}")),
    });
    let client = ApiClient::new(&server.base_url, "k").unwrap();

    let outcome = run_deploy(&client, None, Some("acme/bundle")).await;

    let patches = patch_bodies(&server);
    assert_eq!(
        patches.len(),
        1,
        "expected exactly one PATCH, got {patches:?}"
    );
    assert_eq!(patches[0]["repo_full_name"], "acme/bundle");
    // `AgentUpdate.channel` is retired (ADR-0116): bindings move through the
    // subresource, so a `channel` key here is not merely unasked-for, it now
    // 422s at the router.
    assert!(
        patches[0].get("channel").is_none(),
        "AgentUpdate no longer carries a channel: {}",
        patches[0]
    );
    assert!(
        outcome.repo_note.is_none(),
        "a binding that was applied must not warn: {:?}",
        outcome.repo_note
    );
    // Read back from the PATCH response, so the CLI cannot report a binding
    // the API never stored.
    assert_eq!(outcome.agent.repo_full_name.as_deref(), Some("acme/bundle"));
}

#[tokio::test]
async fn deploy_binds_the_repo_while_also_adding_the_channel() {
    // Two writes now, not one (ADR-0116): the binding add is a POST to the
    // subresource and the repo bind stays a PATCH. Order is load-bearing --
    // the channel goes FIRST, so a failure between them leaves a bound channel
    // with no repo (which still answers) rather than a bound repo with no
    // channel (which does not).
    let server = serve(|req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => existing_agents(&agent_json(AGENT_ID, AGENT_NAME, BOUND, None)),
        ("POST", p) if *p == channels_path() => Response::json(
            201,
            &agent_json_channels(AGENT_ID, AGENT_NAME, &[BOUND, OTHER], None),
        ),
        ("PATCH", p) if *p == format!("/agents/{AGENT_ID}") => {
            patched_agent(OTHER, Some("acme/bundle"))
        }
        (m, p) => deploy_tail(m, p).unwrap_or_else(|| panic!("unexpected request: {m} {p}")),
    });
    let client = ApiClient::new(&server.base_url, "k").unwrap();

    let outcome = run_deploy(&client, Some(OTHER), Some("acme/bundle")).await;

    let posts = channel_post_bodies(&server);
    assert_eq!(posts.len(), 1, "one binding add: {posts:?}");
    assert_eq!(posts[0]["kind"], "slack");
    assert_eq!(posts[0]["address"], OTHER);

    let patches = patch_bodies(&server);
    assert_eq!(
        patches.len(),
        1,
        "the repo bind is its own PATCH: {patches:?}"
    );
    assert_eq!(patches[0]["repo_full_name"], "acme/bundle");
    assert!(
        patches[0].get("channel").is_none(),
        "the binding went via the subresource, so the PATCH carries no channel: {}",
        patches[0]
    );

    let flow = flow(&server);
    let add = flow
        .iter()
        .position(|(m, p)| m == "POST" && *p == channels_path())
        .expect("the binding add");
    let bind = flow
        .iter()
        .position(|(m, p)| m == "PATCH" && *p == format!("/agents/{AGENT_ID}"))
        .expect("the repo bind");
    assert!(
        add < bind,
        "the channel add must precede the repo bind: {flow:?}"
    );
    assert_eq!(outcome.agent.repo_full_name.as_deref(), Some("acme/bundle"));
    assert!(
        outcome.repo_note.is_none(),
        "a binding that was applied must not warn: {:?}",
        outcome.repo_note
    );
}

#[tokio::test]
async fn deploy_warns_when_the_platform_drops_the_repo_binding() {
    // A platform older than `AgentUpdate.repo_full_name` (#1194) answers this
    // PATCH 200 with the unknown key IGNORED: the agent comes back still
    // unbound. `AgentUpdate` declares no `extra="forbid"`, so there is no 4xx
    // and no unrouted-path 404 for the skew detector to key on -- the only
    // evidence is the row that came back. Reporting a clean success here is
    // exactly the failure #1064 exists to prevent: the operator believes the
    // binding took, and git-flow never routes a push.
    let server = serve(|req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => existing_agents(&agent_json(AGENT_ID, AGENT_NAME, BOUND, None)),
        ("PATCH", p) if *p == format!("/agents/{AGENT_ID}") => patched_agent(BOUND, None),
        (m, p) => deploy_tail(m, p).unwrap_or_else(|| panic!("unexpected request: {m} {p}")),
    });
    let client = ApiClient::new(&server.base_url, "k").unwrap();

    let outcome = run_deploy(&client, None, Some("acme/bundle")).await;

    // The CLI did its half: the key went out on the wire.
    let patches = patch_bodies(&server);
    assert_eq!(
        patches.len(),
        1,
        "expected exactly one PATCH, got {patches:?}"
    );
    assert_eq!(patches[0]["repo_full_name"], "acme/bundle");
    // And it reports the row the API returned, not the one it asked for.
    assert_eq!(outcome.agent.repo_full_name, None);
    let note = outcome
        .repo_note
        .expect("a binding the platform did not store must warn");
    assert!(note.contains("acme/bundle"), "note was: {note}");
    // The load-bearing half of this test is the `expect` above (no warning at
    // all is the defect); these two pin that the note is about the PLATFORM
    // dropping it, not about a declined rebind.
    assert!(note.contains("platform"), "note was: {note}");
    assert!(
        !note.contains("already bound"),
        "this is version skew, not a declined rebind: {note}"
    );
}

#[tokio::test]
async fn deploy_does_not_rebind_an_agent_bound_elsewhere() {
    // Moving a live binding reroutes which repository's pushes deploy the
    // agent, which is ADR-0091's whole threat model. A routine deploy declines
    // and says so instead.
    let server = serve(|req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => {
            existing_agents(&agent_json(AGENT_ID, AGENT_NAME, BOUND, Some("other/repo")))
        }
        // Answered, never panicked, so a rebinding PATCH would be RECORDED and
        // `assert_no_patch` is what fails. See its doc comment.
        ("PATCH", p) if *p == format!("/agents/{AGENT_ID}") => {
            patched_agent(BOUND, Some("other/repo"))
        }
        (m, p) => deploy_tail(m, p).unwrap_or_else(|| panic!("unexpected request: {m} {p}")),
    });
    let client = ApiClient::new(&server.base_url, "k").unwrap();

    let outcome = run_deploy(&client, None, Some("acme/bundle")).await;

    assert_no_patch(&server);
    let note = outcome.repo_note.expect("a declined --repo must warn");
    assert!(note.contains("other/repo"), "note was: {note}");
    assert!(note.contains("acme/bundle"), "note was: {note}");
    // The binding CAN be changed now; a deploy just refuses to be the thing
    // that changes it. The old wording sent operators off to recreate the
    // agent, which is the false claim #1212 exists to retire.
    assert!(!note.contains("cannot be changed"), "note was: {note}");
    assert!(!note.contains("recreate"), "note was: {note}");
}

#[tokio::test]
async fn deploy_with_a_matching_repo_does_not_patch() {
    // Already bound to exactly what was asked for. A no-op PATCH would add a
    // write to every routine redeploy and buy nothing.
    let server = serve(|req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => existing_agents(&agent_json(
            AGENT_ID,
            AGENT_NAME,
            BOUND,
            Some("acme/bundle"),
        )),
        // Answered, never panicked, so a no-op PATCH would be RECORDED and
        // `assert_no_patch` is what fails. See its doc comment.
        ("PATCH", p) if *p == format!("/agents/{AGENT_ID}") => {
            patched_agent(BOUND, Some("acme/bundle"))
        }
        (m, p) => deploy_tail(m, p).unwrap_or_else(|| panic!("unexpected request: {m} {p}")),
    });
    let client = ApiClient::new(&server.base_url, "k").unwrap();

    let outcome = run_deploy(&client, None, Some("acme/bundle")).await;

    assert_no_patch(&server);
    assert!(
        outcome.repo_note.is_none(),
        "nothing was declined, so nothing to warn about: {:?}",
        outcome.repo_note
    );
}

#[tokio::test]
async fn deploy_without_repo_never_sends_the_field() {
    // Omission is the wire spelling for "leave the binding alone" (#1071). The
    // deploy has real channel work to do here, so this is not vacuous: it adds
    // a binding, and the repo field must not ride along on ANY request -- not
    // as an explicit null, and not on a PATCH the deploy had no reason to send.
    let server = serve(|req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => existing_agents(&agent_json(
            AGENT_ID,
            AGENT_NAME,
            BOUND,
            Some("acme/bundle"),
        )),
        ("POST", p) if *p == channels_path() => Response::json(
            201,
            &agent_json_channels(AGENT_ID, AGENT_NAME, &[BOUND, OTHER], Some("acme/bundle")),
        ),
        // Answered, never panicked, so a stray PATCH would be RECORDED.
        ("PATCH", p) if *p == format!("/agents/{AGENT_ID}") => {
            patched_agent(OTHER, Some("acme/bundle"))
        }
        (m, p) => deploy_tail(m, p).unwrap_or_else(|| panic!("unexpected request: {m} {p}")),
    });
    let client = ApiClient::new(&server.base_url, "k").unwrap();

    let outcome = run_deploy(&client, Some(OTHER), None).await;

    let posts = channel_post_bodies(&server);
    assert_eq!(posts.len(), 1, "the channel work did happen: {posts:?}");
    assert_eq!(posts[0]["address"], OTHER);
    assert!(
        posts[0].get("repo_full_name").is_none(),
        "the binding add carries only the pair: {}",
        posts[0]
    );
    // No --repo was passed, so the repo bind has nothing to say and the PATCH
    // that would carry it is never issued at all.
    assert_no_patch(&server);
    assert!(
        outcome.repo_note.is_none(),
        "no --repo was passed, so nothing to warn about: {:?}",
        outcome.repo_note
    );
}

#[tokio::test]
async fn surfaces_api_errors_with_status_and_body() {
    let server = serve(|_req| Response::json(401, r#"{"detail":"invalid API key"}"#));
    let client = ApiClient::new(&server.base_url, "wrong").unwrap();
    let err = client.list_agents().await.unwrap_err();
    let text = err.to_string();
    assert!(text.contains("401"), "unexpected error: {text}");
    assert!(text.contains("invalid API key"), "unexpected error: {text}");
}

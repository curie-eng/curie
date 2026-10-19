//! #4358 (plan S3): the Jira half of `curie factory quickstart`.
//!
//! `--tracker jira-cloud --code-host github` validates the Jira token (a
//! classic site URL or the scoped `api.atlassian.com/ex/jira/<cloudId>`
//! gateway), the cloud id, and the start allowlist or group; writes the
//! `factory.trackers` and `factory.bindings` entries and the Jira egress; and
//! binds the agent's `jira=<binding>` surface instead of `github=<repo>`.
//!
//! Only external peers are fixtures. The Jira stand-in follows the documented
//! Jira Cloud REST v3 shapes:
//! https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-server-info/#api-rest-api-3-serverinfo-get
//! https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-myself/#api-rest-api-3-myself-get
//! https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-users/#api-rest-api-3-user-get
//! https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-groups/#api-rest-api-3-group-member-get
//! and the public `/_edge/tenant_info` (`{"cloudId": ...}`). The GitHub
//! stand-in answers the App endpoints `factory_github_app.rs` cites, plus
//! https://docs.github.com/en/rest/repos/repos#get-a-repository for a private
//! repository, which answers 404 without the installation token.
//!
//! Every account, cloud, group and token value here is a fixture. PEMs are
//! generated at runtime with openssl.

#![cfg(unix)]

use crate::support;

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use curie::factory_intake::forge_entries_values;
use curie::factory_quickstart::{
    deploy_args, describe, forge_binding_name, forge_entry_name, forge_secret_name, jira_preflight,
    quickstart_plan, Action, CodeHostKind, EgressEntry, EgressPort, ForgeFlags, JiraPlan,
    Observation, PlanInput, RecordedIntake, ReleaseRead, SecretUse, TrackerKind, DEFAULT_AGENT,
    DEFAULT_BUDGET_USD, DEFAULT_DEADLINE_SECONDS, DEFAULT_KIND_NAME, DEFAULT_MODEL,
};
use curie::factory_toolchain::{preflight, preflight_with_pem, AppPreflight};
use serde_json::{json, Value};
use support::{serve, MockServer, Request, Response};

/// The Jira API token every fixture file holds. Distinctive so any leak into
/// output, values, plan text or errors is caught by a substring check.
const JIRA_TOKEN: &str = "jira-fixture-token-4358-7f3a91c2e5";
const WRONG_TOKEN: &str = "jira-fixture-token-4358-revoked-0d1e";
const USERNAME: &str = "factory-bot@example.com";
const CLOUD_ID: &str = "0f1e2d3c-4b5a-4978-8796-a5b4c3d2e1f0";
const OTHER_CLOUD_ID: &str = "11111111-2222-4333-8444-555555555555";
const BOT_ACCOUNT: &str = "712020:00000000-0000-4000-8000-00000000b07a";
const STARTER: &str = "557058:00000000-0000-4000-8000-0000000000a1";
const SECOND_STARTER: &str = "557058:00000000-0000-4000-8000-0000000000a2";
/// An `app` account: never a start authority (decision 10).
const APP_ACCOUNT: &str = "557058:00000000-0000-4000-8000-0000000000a3";
const UNKNOWN_ACCOUNT: &str = "557058:00000000-0000-4000-8000-0000000000ff";
const GROUP: &str = "00000000-0000-4000-8000-00000000f00d";
const UNKNOWN_GROUP: &str = "00000000-0000-4000-8000-00000000dead";

const APP_ID: &str = "1234567";
const INSTALL_TOKEN: &str = "ghs_fixture";
const REPO: &str = "acme/widgets";
const REPO_ID: u64 = 987654;

/// The release every plan here targets.
const RELEASE: &str = "curie";
/// The fixture plan's recorded names, as an operator might give them; a
/// preflight derives its own (#4620).
const SECRET_NAME: &str = "curie-jira-token";
const BINDING: &str = "jira";

/// The tracker entry name and Secret a run derives for a new site at
/// `api_url`.
fn derived(api_url: &str) -> (String, String) {
    let name = forge_entry_name("jira_cloud", api_url);
    let secret = forge_secret_name(RELEASE, &name);
    (name, secret)
}

fn repos() -> Vec<String> {
    vec![REPO.to_string()]
}

fn basic(token: &str) -> String {
    let encoded = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        format!("{USERNAME}:{token}"),
    );
    format!("Basic {encoded}")
}

fn query(req: &Request) -> Vec<(String, String)> {
    req.path
        .split_once('?')
        .map(|(_, q)| serde_urlencoded::from_str(q).unwrap_or_default())
        .unwrap_or_default()
}

fn param(req: &Request, name: &str) -> Option<String> {
    query(req)
        .into_iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value)
}

/// The request path without its query and without the scoped gateway prefix
/// `/ex/jira/<cloudId>`, plus whether that prefix was present.
fn jira_path(req: &Request) -> (String, bool) {
    let path = req.path.split('?').next().unwrap();
    if let Some(rest) = path.strip_prefix("/ex/jira/") {
        let tail = rest.find('/').map(|i| &rest[i..]).unwrap_or("");
        return (tail.to_string(), true);
    }
    (path.to_string(), false)
}

/// A Jira Cloud stand-in. `/_edge/tenant_info` is public and reports
/// `tenant_cloud`; every REST call needs Basic auth for [`USERNAME`] and
/// [`JIRA_TOKEN`], else 401. `serverInfo.baseUrl` is the site origin the
/// request reached, so the scoped and classic forms resolve to one site.
fn jira(tenant_cloud: &'static str) -> MockServer {
    serve(move |req| {
        let (path, _) = jira_path(req);
        if req.method == "GET" && path == "/_edge/tenant_info" {
            return Response::json(200, &json!({ "cloudId": tenant_cloud }).to_string());
        }
        if req.header("authorization") != Some(basic(JIRA_TOKEN).as_str()) {
            return Response::json(
                401,
                r#"{"errorMessages":["Client must be authenticated to access this resource."],"errors":{}}"#,
            );
        }
        let site = format!("http://{}", req.header("host").unwrap_or("127.0.0.1"));
        match (req.method.as_str(), path.as_str()) {
            ("GET", "/rest/api/3/serverInfo") => Response::json(
                200,
                &json!({"baseUrl": site, "version": "1001.0.0-SNAPSHOT", "deploymentType": "Cloud", "serverTitle": "Jira"}).to_string(),
            ),
            ("GET", "/rest/api/3/myself") => Response::json(
                200,
                &json!({"accountId": BOT_ACCOUNT, "accountType": "atlassian", "active": true, "displayName": "Factory Bot", "timeZone": "America/New_York"}).to_string(),
            ),
            ("GET", "/rest/api/3/user") => {
                let id = param(req, "accountId").unwrap_or_default();
                let kind = match id.as_str() {
                    STARTER | SECOND_STARTER => "atlassian",
                    APP_ACCOUNT => "app",
                    _ => {
                        return Response::json(
                            404,
                            r#"{"errorMessages":["Specified user does not exist or you do not have required permissions"],"errors":{}}"#,
                        )
                    }
                };
                Response::json(
                    200,
                    &json!({"accountId": id, "accountType": kind, "active": true}).to_string(),
                )
            }
            ("GET", "/rest/api/3/group/member") => {
                if param(req, "groupId").as_deref() == Some(GROUP) {
                    Response::json(
                        200,
                        &json!({"isLast": true, "maxResults": 1, "startAt": 0, "total": 1, "values": [{"accountId": STARTER, "accountType": "atlassian"}]}).to_string(),
                    )
                } else {
                    Response::json(
                        404,
                        r#"{"errorMessages":["Specified group does not exist."],"errors":{}}"#,
                    )
                }
            }
            _ => Response::json(404, r#"{"errorMessages":["Not found"],"errors":{}}"#),
        }
    })
}

/// The GitHub App stand-in. `/repos/acme/widgets` is a PRIVATE repository: it
/// answers 404 unless the request carries the installation token (r1 F7).
fn github() -> MockServer {
    serve(|req| {
        let path = req.path.split('?').next().unwrap();
        let installation = req.header("authorization") == Some("Bearer ghs_fixture");
        match (req.method.as_str(), path) {
            ("GET", "/app") => Response::json(
                200,
                r#"{"id":1234567,"slug":"acme-factory","name":"acme-factory"}"#,
            ),
            ("GET", "/users/acme-factory[bot]") => Response::json(200, r#"{"id":123}"#),
            ("GET", "/app/installations") => {
                Response::json(200, r#"[{"id":42,"account":{"login":"acme"}}]"#)
            }
            ("POST", "/app/installations/42/access_tokens") => Response::json(
                201,
                r#"{"token":"ghs_fixture","expires_at":"2099-01-01T00:00:00Z"}"#,
            ),
            ("GET", "/installation/repositories") => Response::json(
                200,
                r#"{"total_count":1,"repositories":[{"full_name":"acme/widgets"}]}"#,
            ),
            ("GET", "/repos/acme/widgets") if installation => Response::json(
                200,
                &json!({"id": REPO_ID, "full_name": REPO, "private": true, "default_branch": "main"})
                    .to_string(),
            ),
            ("GET", "/repos/acme/widgets/commits/main") if installation => {
                Response::json(200, r#"{"sha":"1111111111111111111111111111111111111111"}"#)
            }
            ("GET", "/repos/acme/widgets/contents") if installation => Response::json(200, "[]"),
            ("GET", "/meta") => Response::json(200, r#"{"api":["192.0.2.0/24"]}"#),
            _ => Response::json(404, r#"{"message":"Not Found"}"#),
        }
    })
}

fn generate_pem(path: &Path) {
    let output = Command::new("openssl")
        .args(["genrsa", "2048"])
        .output()
        .expect("runtime PEM generator");
    assert!(output.status.success(), "runtime PEM generator failed");
    fs::write(path, output.stdout).unwrap();
}

/// Sets one process env var for the guard's life. Callers hold `ENV_LOCK`.
struct EnvVar {
    name: &'static str,
    previous: Option<String>,
}

impl EnvVar {
    fn set(name: &'static str, value: &str) -> Self {
        let previous = std::env::var(name).ok();
        std::env::set_var(name, value);
        Self { name, previous }
    }
}

impl Drop for EnvVar {
    fn drop(&mut self) {
        match &self.previous {
            Some(value) => std::env::set_var(self.name, value),
            None => std::env::remove_var(self.name),
        }
    }
}

/// Which App the GitHub half authenticated as before the Jira preflight.
#[derive(Clone, Copy)]
enum AppPath {
    /// Install path: `factory_toolchain::preflight` from `--private-key-file`.
    Install,
    /// AddAgent path: `github::recorded_app`, which is `preflight_with_pem`
    /// over the PEM read from the Secret the release records.
    Recorded,
}

/// The `AppPreflight` the GitHub flow builds. The caller holds `ENV_LOCK`.
async fn app(github: &MockServer, dir: &Path, path: AppPath) -> AppPreflight {
    let _api = EnvVar::set("CURIE_GITHUB_API_URL", &github.base_url);
    let pem_path = dir.join("app.pem");
    generate_pem(&pem_path);
    match path {
        AppPath::Install => preflight(APP_ID, &pem_path, &repos(), false).await,
        AppPath::Recorded => {
            preflight_with_pem(
                APP_ID,
                fs::read_to_string(&pem_path).unwrap(),
                &repos(),
                false,
            )
            .await
        }
    }
    .expect("the GitHub App preflight succeeds against the stand-in")
}

fn token_file(dir: &Path, contents: &str) -> PathBuf {
    let path = dir.join("jira-token");
    fs::write(&path, contents).unwrap();
    path
}

fn flags(forge_url: &str, token_file: &Path) -> ForgeFlags {
    ForgeFlags {
        code_host: CodeHostKind::Github,
        tracker: TrackerKind::JiraCloud,
        forge_url: Some(forge_url.to_string()),
        token_file: Some(token_file.to_path_buf()),
        ca_file: None,
        review_accounts: Vec::new(),
        forge_username: Some(USERNAME.to_string()),
        start_accounts: vec![STARTER.to_string(), SECOND_STARTER.to_string()],
        start_group: None,
        tracker_url: None,
        tracker_token_file: None,
    }
}

fn host_port(server: &MockServer) -> (String, u16) {
    let authority = server.base_url.trim_start_matches("http://").to_string();
    let port = authority.rsplit_once(':').unwrap().1.parse().unwrap();
    (authority, port)
}

fn err_text(error: &anyhow::Error) -> String {
    format!("{error:#}\n{error:?}")
}

fn assert_no_token(text: &str, what: &str) {
    for token in [JIRA_TOKEN, WRONG_TOKEN] {
        assert!(!text.contains(token), "{what} carries a Jira token: {text}");
    }
    assert!(
        !text.contains(&basic(JIRA_TOKEN)) && !text.contains(&basic(WRONG_TOKEN)),
        "{what} carries a Basic credential: {text}"
    );
}

fn assert_plan_has_no_token(plan: &JiraPlan) {
    assert_no_token(&format!("{plan:?}"), "the JiraPlan");
    assert_no_token(&plan.tracker.to_string(), "the tracker value");
    assert_no_token(&plan.binding.to_string(), "the binding value");
}

/// Neither credential crosses to the other forge.
fn assert_credentials_stay_on_their_forge(jira: &MockServer, github: &MockServer) {
    for req in github.recorded() {
        let auth = req.header("authorization").unwrap_or_default();
        assert!(!auth.starts_with("Basic"), "GitHub got a Basic credential");
        assert_no_token(auth, "a GitHub request");
    }
    for req in jira.recorded() {
        let auth = req.header("authorization").unwrap_or_default();
        assert!(!auth.contains(INSTALL_TOKEN), "Jira got the GitHub token");
    }
}

fn jira_paths(jira: &MockServer) -> Vec<(String, bool)> {
    jira.recorded().iter().map(jira_path).collect()
}

// ---------------------------------------------------------------------------
// Preflight: token, cloud id, start authority, repository ids (in process)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn jira_preflight_classic_site_url_writes_tracker_binding_and_egress() {
    let _env = support::ENV_LOCK.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let (github, jira) = (github(), jira(CLOUD_ID));
    let app = app(&github, dir.path(), AppPath::Install).await;
    // A trailing newline, as an editor writes, is not part of the token.
    let tokens = token_file(dir.path(), &format!("{JIRA_TOKEN}\n"));

    let plan = jira_preflight(&flags(&jira.base_url, &tokens), &repos(), &app, RELEASE)
        .await
        .expect("a valid token, site and allowlist pass the preflight");

    let (host, port) = host_port(&jira);
    let (name, secret) = derived(&jira.base_url);
    let egress = json!([{"cidr": "127.0.0.1/32", "ports": [{"protocol": "TCP", "port": port}]}]);
    assert_eq!(serde_json::to_value(&plan.egress).unwrap(), egress);
    assert_eq!(
        plan.tracker,
        json!({
            "name": name,
            "kind": "jira_cloud",
            "host": host,
            "apiUrl": jira.base_url,
            "label": "curie-factory",
            "mention": BOT_ACCOUNT,
            "credential": {
                "type": "static_token",
                "username": USERNAME,
                "existingSecret": secret,
                "existingSecretKey": "token",
            },
            "egress": egress,
        })
    );
    let binding = &plan.binding;
    let binding_name = forge_binding_name(&name, CLOUD_ID);
    assert_eq!(binding["name"], json!(binding_name));
    assert_eq!(binding["tracker"], json!(name));
    assert_eq!(binding["trackerScopeId"], json!(CLOUD_ID));
    assert_eq!(
        binding["startAuthority"],
        json!({"type": "allowlist", "accounts": [STARTER, SECOND_STARTER]})
    );
    let listed = binding["repos"].as_array().expect("binding repos");
    assert_eq!(listed.len(), 1, "{binding}");
    let alias = listed[0]["alias"].as_str().expect("alias");
    assert!(
        !alias.is_empty()
            && alias.len() <= 64
            && alias
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || "._-".contains(c)),
        "alias {alias:?} must satisfy the API's config name rule"
    );
    assert_eq!(listed[0]["codeHost"], json!("github"));
    assert_eq!(listed[0]["projectId"], json!(REPO_ID.to_string()));
    assert_eq!(listed[0]["path"], json!(REPO));
    assert_eq!(binding["defaultRepo"], json!(alias));
    assert_eq!(plan.secret_name, secret);
    assert_eq!(plan.binding_name, binding_name);
    assert_eq!(plan.token_file, tokens);
    assert_eq!(plan.recorded_tracker, None);
    assert_eq!(plan.secret, SecretUse::Create);

    let paths = jira_paths(&jira);
    for expected in [
        "/_edge/tenant_info",
        "/rest/api/3/myself",
        "/rest/api/3/user",
    ] {
        assert!(
            paths.iter().any(|(path, _)| path == expected),
            "{expected} was not read: {paths:?}"
        );
    }
    let looked_up: Vec<String> = jira
        .recorded()
        .iter()
        .filter(|req| jira_path(req).0 == "/rest/api/3/user")
        .filter_map(|req| param(req, "accountId"))
        .collect();
    assert!(
        looked_up.contains(&STARTER.to_string()) && looked_up.contains(&SECOND_STARTER.to_string()),
        "each --start-account is checked: {looked_up:?}"
    );
    assert!(
        paths.iter().all(|(_, scoped)| !scoped),
        "a classic site URL never reaches the gateway: {paths:?}"
    );
    assert_plan_has_no_token(&plan);
    assert_credentials_stay_on_their_forge(&jira, &github);
}

#[tokio::test]
async fn jira_preflight_scoped_gateway_url_reads_through_the_gateway() {
    let _env = support::ENV_LOCK.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let (github, jira) = (github(), jira(CLOUD_ID));
    let app = app(&github, dir.path(), AppPath::Install).await;
    let tokens = token_file(dir.path(), JIRA_TOKEN);
    let scoped = format!("{}/ex/jira/{CLOUD_ID}", jira.base_url);

    let plan = jira_preflight(&flags(&scoped, &tokens), &repos(), &app, RELEASE)
        .await
        .expect("a scoped URL whose cloud id matches tenant_info passes");

    let (host, _) = host_port(&jira);
    assert_eq!(plan.tracker["apiUrl"], json!(scoped));
    assert_eq!(
        plan.tracker["host"],
        json!(host),
        "host is the site, not the gateway"
    );
    assert_eq!(plan.binding["trackerScopeId"], json!(CLOUD_ID));
    assert_eq!(
        plan.tracker["egress"].as_array().map(Vec::len),
        Some(1),
        "the site and gateway share a host here, so one entry: {}",
        plan.tracker
    );
    let paths = jira_paths(&jira);
    assert!(
        paths
            .iter()
            .any(|(path, scoped)| path == "/rest/api/3/myself" && *scoped),
        "REST calls go through the scoped URL: {paths:?}"
    );
    assert!(
        paths
            .iter()
            .any(|(path, scoped)| path == "/_edge/tenant_info" && !*scoped),
        "tenant_info is read from the site: {paths:?}"
    );
    assert_plan_has_no_token(&plan);
}

#[tokio::test]
async fn jira_preflight_refuses_a_scoped_url_whose_cloud_id_differs_from_tenant_info() {
    let _env = support::ENV_LOCK.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let (github, jira) = (github(), jira(OTHER_CLOUD_ID));
    let app = app(&github, dir.path(), AppPath::Install).await;
    let tokens = token_file(dir.path(), JIRA_TOKEN);
    let scoped = format!("{}/ex/jira/{CLOUD_ID}", jira.base_url);

    let error = jira_preflight(&flags(&scoped, &tokens), &repos(), &app, RELEASE)
        .await
        .expect_err("a cloud id mismatch is refused");
    let text = err_text(&error);
    assert!(text.contains("--forge-url"), "{text}");
    assert!(text.to_ascii_lowercase().contains("cloud id"), "{text}");
    assert_no_token(&text, "the refusal");
    // Liveness: jira_preflight_scoped_gateway_url_reads_through_the_gateway.
}

#[tokio::test]
async fn jira_preflight_refuses_an_unauthorized_token_naming_the_flag_and_status() {
    let _env = support::ENV_LOCK.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let (github, jira) = (github(), jira(CLOUD_ID));
    let app = app(&github, dir.path(), AppPath::Install).await;
    let tokens = token_file(dir.path(), WRONG_TOKEN);

    let error = jira_preflight(&flags(&jira.base_url, &tokens), &repos(), &app, RELEASE)
        .await
        .expect_err("a token Jira answers 401 for is refused");
    let text = err_text(&error);
    assert!(text.contains("--token-file"), "{text}");
    assert!(text.contains("401"), "{text}");
    assert_no_token(&text, "the refusal");
    assert!(
        jira.recorded()
            .iter()
            .all(|req| jira_path(req).0 != "/rest/api/3/user"),
        "no start account is checked with a refused token"
    );
}

#[tokio::test]
async fn jira_preflight_refuses_an_empty_or_missing_token_file() {
    let _env = support::ENV_LOCK.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let (github, jira) = (github(), jira(CLOUD_ID));
    let app = app(&github, dir.path(), AppPath::Install).await;

    for (case, path) in [
        ("empty", token_file(dir.path(), " \n")),
        ("missing", dir.path().join("absent-token")),
    ] {
        let error = jira_preflight(&flags(&jira.base_url, &path), &repos(), &app, RELEASE)
            .await
            .expect_err(case);
        let text = err_text(&error);
        assert!(text.contains("--token-file"), "{case}: {text}");
    }
    assert!(
        jira.recorded().is_empty(),
        "no Jira request is made without a token"
    );
}

#[tokio::test]
async fn jira_preflight_refuses_an_unknown_start_account() {
    let _env = support::ENV_LOCK.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let (github, jira) = (github(), jira(CLOUD_ID));
    let app = app(&github, dir.path(), AppPath::Install).await;
    let tokens = token_file(dir.path(), JIRA_TOKEN);
    let mut forge = flags(&jira.base_url, &tokens);
    forge.start_accounts = vec![STARTER.to_string(), UNKNOWN_ACCOUNT.to_string()];

    let error = jira_preflight(&forge, &repos(), &app, RELEASE)
        .await
        .expect_err("an account Jira does not know is refused");
    let text = err_text(&error);
    assert!(text.contains("--start-account"), "{text}");
    assert!(text.contains(UNKNOWN_ACCOUNT), "{text}");
    assert!(text.contains("404"), "{text}");
    assert_no_token(&text, "the refusal");
}

#[tokio::test]
async fn jira_preflight_refuses_a_start_account_that_is_not_a_person() {
    let _env = support::ENV_LOCK.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let (github, jira) = (github(), jira(CLOUD_ID));
    let app = app(&github, dir.path(), AppPath::Install).await;
    let tokens = token_file(dir.path(), JIRA_TOKEN);
    let mut forge = flags(&jira.base_url, &tokens);
    forge.start_accounts = vec![APP_ACCOUNT.to_string()];

    let error = jira_preflight(&forge, &repos(), &app, RELEASE)
        .await
        .expect_err("an app account never starts work, so it is refused");
    let text = err_text(&error);
    assert!(text.contains("--start-account"), "{text}");
    assert!(text.contains(APP_ACCOUNT), "{text}");
    assert_no_token(&text, "the refusal");
}

#[tokio::test]
async fn jira_preflight_group_start_authority_is_checked_and_written() {
    let _env = support::ENV_LOCK.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let (github, jira) = (github(), jira(CLOUD_ID));
    let app = app(&github, dir.path(), AppPath::Install).await;
    let tokens = token_file(dir.path(), JIRA_TOKEN);
    let mut forge = flags(&jira.base_url, &tokens);
    forge.start_accounts = Vec::new();
    forge.start_group = Some(GROUP.to_string());

    let plan = jira_preflight(&forge, &repos(), &app, RELEASE)
        .await
        .expect("a group Jira lists passes");
    assert_eq!(
        plan.binding["startAuthority"],
        json!({"type": "group", "groupId": GROUP})
    );
    assert!(
        jira.recorded()
            .iter()
            .any(|req| jira_path(req).0 == "/rest/api/3/group/member"
                && param(req, "groupId").as_deref() == Some(GROUP)),
        "the group is read"
    );

    forge.start_group = Some(UNKNOWN_GROUP.to_string());
    let error = jira_preflight(&forge, &repos(), &app, RELEASE)
        .await
        .expect_err("a group Jira does not list is refused");
    let text = err_text(&error);
    assert!(text.contains("--start-group"), "{text}");
    assert!(text.contains("404"), "{text}");
    assert_no_token(&text, "the refusal");
}

/// r1 F7: the binding's repository id is read with the App installation token
/// on both paths, so a private repository works and nothing is read
/// unauthenticated.
#[tokio::test]
async fn jira_preflight_reads_a_private_repository_id_with_the_installation_token() {
    let _env = support::ENV_LOCK.lock().await;
    for path in [AppPath::Install, AppPath::Recorded] {
        let dir = tempfile::tempdir().unwrap();
        let (github, jira) = (github(), jira(CLOUD_ID));
        let app = app(&github, dir.path(), path).await;
        let before = github.recorded().len();
        let tokens = token_file(dir.path(), JIRA_TOKEN);

        let plan = jira_preflight(&flags(&jira.base_url, &tokens), &repos(), &app, RELEASE)
            .await
            .expect("a private repository id is readable with the installation token");
        assert_eq!(
            plan.binding["repos"][0]["projectId"],
            json!(REPO_ID.to_string())
        );
        let reads: Vec<Request> = github.recorded()[before..]
            .iter()
            .filter(|req| {
                req.method == "GET" && req.path.split('?').next() == Some("/repos/acme/widgets")
            })
            .cloned()
            .collect();
        assert!(!reads.is_empty(), "the repository id is read from GitHub");
        for req in reads {
            assert_eq!(
                req.header("authorization"),
                Some("Bearer ghs_fixture"),
                "GET /repos/acme/widgets must carry the installation token"
            );
        }
        assert_credentials_stay_on_their_forge(&jira, &github);
    }
}

// ---------------------------------------------------------------------------
// Plan, describe, deploy surfaces and values (pure)
// ---------------------------------------------------------------------------

fn fixture_egress() -> Vec<EgressEntry> {
    vec![EgressEntry {
        cidr: "192.0.2.10/32".into(),
        ports: vec![EgressPort {
            protocol: "TCP",
            port: 443,
        }],
        host: Some("jira.example.com".into()),
    }]
}

fn fixture_plan() -> JiraPlan {
    let egress = fixture_egress();
    JiraPlan {
        tracker: json!({
            "name": BINDING,
            "kind": "jira_cloud",
            "host": "jira.example.com",
            "apiUrl": "https://jira.example.com",
            "label": "curie-factory",
            "mention": BOT_ACCOUNT,
            "credential": {"type": "static_token", "username": USERNAME, "existingSecret": SECRET_NAME, "existingSecretKey": "token"},
            "egress": serde_json::to_value(&egress).unwrap(),
        }),
        binding: json!({
            "name": BINDING,
            "tracker": BINDING,
            "trackerScopeId": CLOUD_ID,
            "repos": [{"alias": "widgets", "codeHost": "github", "projectId": REPO_ID.to_string(), "path": REPO}],
            "defaultRepo": "widgets",
            "startAuthority": {"type": "allowlist", "accounts": [STARTER]},
        }),
        egress,
        secret_name: SECRET_NAME.into(),
        secret_key: "token".into(),
        token_file: PathBuf::from("/keys/jira-token"),
        binding_name: BINDING.into(),
        recorded_tracker: None,
        recorded_binding: None,
        secret: SecretUse::Create,
        observation: Observation::default(),
    }
}

fn input() -> PlanInput {
    PlanInput {
        repos: repos(),
        app_id: Some("12345".into()),
        private_key_file: Some("/keys/app.pem".into()),
        explicit_context: Some("kind-curie-factory".into()),
        current_context: None,
        kind_name: DEFAULT_KIND_NAME.into(),
        existing_kind_clusters: Vec::new(),
        namespace: "curie".into(),
        release: "curie".into(),
        model: DEFAULT_MODEL.into(),
        org: None,
        execution_deadline_seconds: DEFAULT_DEADLINE_SECONDS,
        budget_usd: DEFAULT_BUDGET_USD,
        chart: "charts/curie".into(),
        credential_in_env: true,
        release_has_real_model: false,
        chart_explicit: false,
        interactive: true,
        release_read: ReleaseRead::NotAtTarget,
        agent: DEFAULT_AGENT.into(),
        label: None,
        plugin_dir: None,
        model_explicit: false,
        recorded: None,
        bindings: Vec::new(),
        runner_image: None,
        jira: Some(fixture_plan()),
    }
}

fn install_input() -> PlanInput {
    input()
}

fn add_agent_input() -> PlanInput {
    let mut input = input();
    input.app_id = None;
    input.private_key_file = None;
    input.agent = "jira-factory".into();
    input.recorded = Some(RecordedIntake {
        app_id: "12345".into(),
        install_label: "curie-factory".into(),
        repo_allowlist: repos(),
        pem_secret: "curie-github-app".into(),
        pem_secret_key: "private-key".into(),
    });
    input
}

/// `input` with the release recording `tracker` and `binding` for the
/// plan's site, found by identity, and its Secret holding the token.
fn on_jira_release(mut input: PlanInput, tracker: Value, binding: Value) -> PlanInput {
    let jira = input.jira.as_mut().unwrap();
    jira.recorded_tracker = Some(tracker);
    jira.recorded_binding = Some(binding);
    jira.secret = SecretUse::Reuse;
    input
}

/// The add agent path on a release whose `jira` tracker and binding another
/// Jira agent already serves.
fn add_agent_on_jira_release(tracker: Value, binding: Value) -> PlanInput {
    on_jira_release(add_agent_input(), tracker, binding)
}

/// The refusal's message and fix, after checking it exits 2 and carries no
/// token.
fn usage_refusal(input: &PlanInput) -> String {
    let error = quickstart_plan(input).expect_err("a differing jira entry is refused");
    let cli = error
        .downcast_ref::<curie::exit::CliError>()
        .expect("a CliError");
    assert_eq!(cli.class.code(), USAGE_EXIT, "{error:#}");
    let text = format!("{}\n{}", cli.message, cli.fix.clone().unwrap_or_default());
    assert_no_token(&text, "the refusal");
    assert_no_token(&err_text(&error), "the refusal");
    text
}

#[test]
fn jira_add_agent_preserves_an_identical_recorded_tracker_and_binding() {
    pinned_tmpdir();
    let plan = fixture_plan();
    // The recorded egress was resolved at an earlier preflight; it is kept.
    let mut tracker = plan.tracker.clone();
    tracker["egress"] = json!([{"cidr": "192.0.2.99/32", "ports": [{"protocol": "TCP", "port": 443}], "host": "jira.example.com"}]);
    let planned = quickstart_plan(&add_agent_on_jira_release(tracker, plan.binding.clone()))
        .expect("an identical jira entry is served as is");
    assert_eq!(
        action_names(&planned.actions),
        ["egress", "label", "runner-env", "deploy"],
        "no JiraTracker write: other agents' intake is not rewritten"
    );
    assert_eq!(deploy_binding(&planned.actions).as_deref(), Some(BINDING));
}

#[test]
fn jira_add_agent_writes_the_entry_the_release_lacks() {
    pinned_tmpdir();
    let plan = fixture_plan();
    let mut input = add_agent_input();
    let jira = input.jira.as_mut().unwrap();
    jira.recorded_tracker = Some(plan.tracker.clone());
    jira.secret = SecretUse::Reuse;
    let planned = quickstart_plan(&input).unwrap();
    assert_eq!(
        action_names(&planned.actions),
        ["jira", "egress", "label", "runner-env", "deploy"]
    );
}

#[test]
fn jira_add_agent_refuses_a_binding_with_a_differing_repository_set() {
    let plan = fixture_plan();
    let mut binding = plan.binding.clone();
    binding["repos"].as_array_mut().unwrap().push(
        json!({"alias": "gears", "codeHost": "github", "projectId": "42", "path": "acme/gears"}),
    );
    let text = usage_refusal(&add_agent_on_jira_release(plan.tracker.clone(), binding));
    assert!(text.contains("binding `jira`"), "{text}");
    assert!(text.contains("its repos is"), "{text}");
    assert!(
        text.contains("acme/gears"),
        "names the recorded value: {text}"
    );
    assert!(text.contains("--repo"), "says how to proceed: {text}");
}

#[test]
fn jira_add_agent_refuses_a_binding_with_a_differing_start_allowlist() {
    let plan = fixture_plan();
    let mut binding = plan.binding.clone();
    binding["startAuthority"] = json!({"type": "allowlist", "accounts": [STARTER, SECOND_STARTER]});
    let text = usage_refusal(&add_agent_on_jira_release(plan.tracker.clone(), binding));
    assert!(text.contains("its startAuthority is"), "{text}");
    assert!(text.contains(SECOND_STARTER), "{text}");
    assert!(text.contains("--start-account or --start-group"), "{text}");
}

#[test]
fn jira_add_agent_refuses_a_tracker_on_a_differing_site() {
    let plan = fixture_plan();
    let mut tracker = plan.tracker.clone();
    tracker["host"] = json!("other.atlassian.net");
    tracker["apiUrl"] = json!("https://other.atlassian.net");
    let mut binding = plan.binding.clone();
    binding["trackerScopeId"] = json!(OTHER_CLOUD_ID);
    let text = usage_refusal(&add_agent_on_jira_release(tracker, binding));
    assert!(text.contains("tracker `jira`"), "{text}");
    assert!(text.contains("its apiUrl is"), "{text}");
    assert!(text.contains("https://other.atlassian.net"), "{text}");
    assert!(text.contains("--forge-url"), "{text}");
    assert!(text.contains("factory.trackers[jira].apiUrl"), "{text}");
}

#[test]
fn jira_add_agent_preserves_an_equivalent_recorded_entry() {
    // The api reads the allowlist as a set and fills the poll interval's
    // default, so neither order nor an explicit default is a difference.
    pinned_tmpdir();
    let mut plan = fixture_plan();
    plan.binding["startAuthority"] =
        json!({"type": "allowlist", "accounts": [STARTER, SECOND_STARTER]});
    let mut input = add_agent_input();
    input.jira = Some(plan.clone());
    let mut tracker = plan.tracker.clone();
    tracker["pollIntervalS"] = json!(45);
    let mut binding = plan.binding.clone();
    binding["startAuthority"] = json!({"type": "allowlist", "accounts": [SECOND_STARTER, STARTER]});
    binding["repos"][0]["projectId"] = json!(REPO_ID);
    binding["components"] = json!({});
    let mut input = on_jira_release(input, tracker.clone(), binding.clone());
    let planned = quickstart_plan(&input).expect("an equivalent jira entry is served as is");
    assert_eq!(
        action_names(&planned.actions),
        ["egress", "label", "runner-env", "deploy"]
    );

    // A real difference beside them is still refused.
    tracker["pollIntervalS"] = json!(60);
    input.jira.as_mut().unwrap().recorded_tracker = Some(tracker);
    let text = usage_refusal(&input);
    assert!(text.contains("its pollIntervalS is 60"), "{text}");
    assert!(
        text.contains("factory.trackers[jira].pollIntervalS"),
        "{text}"
    );
}

#[test]
fn jira_install_path_refuses_a_differing_recorded_jira_entry() {
    // #4620: the install path applies the add agent path's guard before the
    // intake, and a first install with nothing recorded writes the entries.
    pinned_tmpdir();
    let plan = fixture_plan();
    let mut tracker = plan.tracker.clone();
    tracker["mention"] = json!("another-bot");
    let mut install = on_jira_release(install_input(), tracker, plan.binding.clone());
    install.agent = DEFAULT_AGENT.into();
    let text = usage_refusal(&install);
    assert!(text.contains("its mention is"), "{text}");
    let same = on_jira_release(install_input(), plan.tracker.clone(), plan.binding.clone());
    let planned = quickstart_plan(&same).unwrap();
    assert!(
        !action_names(&planned.actions).contains(&"jira"),
        "an identical recorded site is not rewritten"
    );
    let first = quickstart_plan(&install_input()).unwrap();
    assert_eq!(
        action_names(&first.actions),
        [
            "external",
            "cluster",
            "intake",
            "jira",
            "runner-env",
            "deploy"
        ]
    );
}

/// `bundle_dir` embeds the temp dir; pin it like the #4346 goldens do.
fn pinned_tmpdir() {
    let dir = "/tmp/factory-quickstart-golden";
    fs::create_dir_all(dir).unwrap();
    std::env::set_var("TMPDIR", dir);
}

fn golden(name: &str) -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/golden/factory_quickstart")
        .join(name);
    let raw = fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("missing golden {}: {err}", path.display()));
    raw.split_once("# ---\n")
        .expect("golden header ends with `# ---`")
        .1
        .to_string()
}

fn describe_text(input: &PlanInput) -> String {
    pinned_tmpdir();
    let planned = quickstart_plan(input).expect("the jira plan builds");
    let mut text = describe(&planned).join("\n");
    text.push('\n');
    text
}

#[test]
fn jira_install_plan_describe_matches_the_golden() {
    let text = describe_text(&install_input());
    assert_eq!(text, golden("describe_jira_install.txt"));
    assert_no_token(&text, "the describe output");
}

#[test]
fn jira_add_agent_plan_describe_matches_the_golden() {
    let text = describe_text(&add_agent_input());
    assert_eq!(text, golden("describe_jira_add_agent.txt"));
    assert_no_token(&text, "the describe output");
}

fn action_names(actions: &[Action]) -> Vec<&'static str> {
    actions
        .iter()
        .map(|action| match action {
            Action::External { .. } => "external",
            Action::Cluster { .. } => "cluster",
            Action::Register { .. } => "register",
            Action::Intake(_) => "intake",
            Action::JiraTracker(_) => "jira",
            Action::ForgeEgress { .. } => "egress",
            Action::RunnerEnv { .. } => "runner-env",
            Action::TriggerLabel { .. } => "label",
            Action::Deploy { .. } => "deploy",
        })
        .collect()
}

fn deploy_binding(actions: &[Action]) -> Option<String> {
    actions.iter().find_map(|action| match action {
        Action::Deploy { jira_binding, .. } => Some(jira_binding.clone()),
        _ => None,
    })?
}

#[test]
fn jira_tracker_action_follows_intake_on_install_and_leads_add_agent() {
    pinned_tmpdir();
    let install = quickstart_plan(&install_input()).unwrap();
    assert_eq!(
        action_names(&install.actions),
        [
            "external",
            "cluster",
            "intake",
            "jira",
            "runner-env",
            "deploy"
        ]
    );
    assert_eq!(deploy_binding(&install.actions).as_deref(), Some(BINDING));
    let carried = install.actions.iter().find_map(|action| match action {
        Action::JiraTracker(plan) => Some(plan.clone()),
        _ => None,
    });
    let carried = carried.expect("one JiraTracker action");
    assert_eq!(carried.tracker, fixture_plan().tracker);
    assert_eq!(carried.binding, fixture_plan().binding);

    let add = quickstart_plan(&add_agent_input()).unwrap();
    assert_eq!(
        action_names(&add.actions),
        ["jira", "egress", "label", "runner-env", "deploy"]
    );
    assert_eq!(deploy_binding(&add.actions).as_deref(), Some(BINDING));

    // Liveness of the GitHub plan: no Jira plan, no JiraTracker, github= bind.
    let mut github = install_input();
    github.jira = None;
    let plain = quickstart_plan(&github).unwrap();
    assert!(!action_names(&plain.actions).contains(&"jira"));
    assert_eq!(deploy_binding(&plain.actions), None);
    assert!(describe(&plain)
        .iter()
        .any(|line| line.ends_with("--add github=acme/widgets")));
}

fn deploy_line(input: &PlanInput) -> String {
    pinned_tmpdir();
    let planned = quickstart_plan(input).unwrap();
    describe(&planned)
        .into_iter()
        .find(|line| line.starts_with("curie cluster deploy "))
        .expect("a cluster deploy line")
}

#[test]
fn an_explicit_chart_reaches_the_cluster_deploy_argv() {
    // The deploy writes connector Secrets with a chart; an unreleased build
    // has no released chart to fetch, so the operator's --chart must reach it.
    for jira in [true, false] {
        for mut input in [install_input(), add_agent_input()] {
            if !jira {
                input.jira = None;
            }
            input.chart = "/tmp/extracted/curie".into();
            input.chart_explicit = true;
            let line = deploy_line(&input);
            assert!(
                line.ends_with(" --chart /tmp/extracted/curie"),
                "jira={jira}: {line}"
            );
            let argvs = deploy_args(
                "ctx",
                "ns",
                "rel",
                &repos(),
                DEFAULT_AGENT,
                None,
                Path::new("/bundle"),
                &[],
                jira.then_some(BINDING),
                Some("/tmp/extracted/curie"),
            );
            assert_eq!(argvs[0][1], "deploy");
            assert!(argvs[0].ends_with(&["--chart".into(), "/tmp/extracted/curie".into()]));
            assert!(
                argvs[1..].iter().flatten().all(|arg| arg != "--chart"),
                "only the deploy takes the chart: {argvs:?}"
            );
        }
    }
}

#[test]
fn without_an_explicit_chart_the_deploy_argv_is_unchanged() {
    for jira in [true, false] {
        for mut input in [install_input(), add_agent_input()] {
            if !jira {
                input.jira = None;
            }
            input.chart = "/tmp/extracted/curie".into();
            let line = deploy_line(&input);
            assert!(!line.contains("--chart"), "jira={jira}: {line}");
        }
    }
}

#[test]
fn jira_deploy_binds_one_jira_surface_instead_of_each_github_repository() {
    let repos = vec!["a/x".to_string(), "b/y".to_string()];
    let bundle = Path::new("/bundle");
    let surfaces = |argvs: &[Vec<String>]| -> Vec<Vec<String>> {
        argvs
            .iter()
            .filter(|argv| argv.get(1).map(String::as_str) == Some("surfaces"))
            .cloned()
            .collect()
    };
    for label in [None, Some("curie-factory-fast")] {
        let argvs = deploy_args(
            "ctx",
            "curie",
            "curie",
            &repos,
            "agent",
            label,
            bundle,
            &[],
            Some(BINDING),
            None,
        );
        assert_eq!(argvs[0][1], "deploy", "{argvs:?}");
        let bound = surfaces(&argvs);
        assert_eq!(bound.len(), 1, "one jira surface: {argvs:?}");
        let mut expected = vec!["--add".to_string(), "jira=jira".to_string()];
        if let Some(label) = label {
            expected.extend(["--label".to_string(), label.to_string()]);
        }
        assert!(
            bound[0].ends_with(&expected),
            "{:?} ends with {expected:?}",
            bound[0]
        );
        assert!(
            argvs
                .iter()
                .flatten()
                .all(|arg| !arg.starts_with("github=")),
            "no github surface for a Jira agent: {argvs:?}"
        );
    }
    // Liveness: without a Jira binding each repository keeps its github surface.
    let argvs = deploy_args(
        "ctx",
        "curie",
        "curie",
        &repos,
        "agent",
        None,
        bundle,
        &[],
        None,
        None,
    );
    let adds: Vec<String> = surfaces(&argvs)
        .into_iter()
        .flatten()
        .filter(|arg| arg.contains('='))
        .collect();
    assert_eq!(adds, ["github=a/x", "github=b/y"]);
}

#[test]
fn jira_forge_values_replace_only_the_jira_entries() {
    let plan = fixture_plan();
    let github_tracker = json!({"name": "github", "kind": "github", "host": "github.com", "apiUrl": "https://api.github.com", "label": "curie-factory", "mention": "acme-factory[bot]", "credential": {"type": "github_app", "appId": "12345", "existingSecret": "curie-github-app", "existingSecretKey": "private-key"}});
    let code_host = json!({"name": "github", "kind": "github", "host": "github.com", "apiUrl": "https://api.github.com", "credential": {"type": "github_app", "appId": "12345", "existingSecret": "curie-github-app", "existingSecretKey": "private-key"}});
    let other_binding = json!({"name": "ops", "tracker": "github", "trackerScopeId": "1", "repos": [{"alias": "ops", "codeHost": "github", "projectId": "1", "path": "acme/ops"}]});
    // A second site under its own names coexists (#4620).
    let other_tracker = json!({"name": "jira-other", "kind": "jira_cloud", "host": "old.example.com", "apiUrl": "https://old.example.com", "mention": "old"});
    let other_site_binding = json!({"name": "jira-other-binding", "tracker": "jira-other", "trackerScopeId": OTHER_CLOUD_ID, "repos": []});
    let recorded = json!({
        "factory": {
            "enabled": true,
            "intake": "poll",
            "codeHosts": [code_host],
            "trackers": [github_tracker, other_tracker],
            "bindings": [other_binding, other_site_binding],
        }
    });

    let values = forge_entries_values(&recorded, &plan).unwrap();
    let trackers = values
        .pointer("/factory/trackers")
        .and_then(Value::as_array)
        .expect("factory.trackers is written whole");
    assert_eq!(trackers.len(), 3, "{values}");
    assert!(trackers.contains(&other_tracker), "the other site is kept");
    assert!(
        trackers.contains(&github_tracker),
        "the GitHub tracker is kept"
    );
    assert!(
        trackers.contains(&plan.tracker),
        "the Jira tracker is written"
    );
    let bindings = values
        .pointer("/factory/bindings")
        .and_then(Value::as_array)
        .expect("factory.bindings is written whole");
    assert_eq!(bindings.len(), 3, "{values}");
    assert!(bindings.contains(&other_binding), "other bindings are kept");
    assert!(
        bindings.contains(&other_site_binding),
        "other bindings are kept"
    );
    assert!(
        bindings.contains(&plan.binding),
        "the Jira binding is written"
    );
    if let Some(hosts) = values.pointer("/factory/codeHosts") {
        assert_eq!(hosts, &json!([code_host]), "code hosts are not changed");
    }
    assert_no_token(&values.to_string(), "the values");

    // A same-named entry of another site is refused, never replaced.
    let stale_tracker = json!({"name": BINDING, "kind": "jira_cloud", "host": "old.example.com", "apiUrl": "https://old.example.com"});
    let stale = json!({"factory": {"trackers": [github_tracker, stale_tracker]}});
    assert!(forge_entries_values(&stale, &plan).is_err());

    // A release whose intake wrote only the GitHub entries gains the Jira ones.
    let fresh = json!({"factory": {"enabled": true, "codeHosts": [code_host], "trackers": [github_tracker]}});
    let values = forge_entries_values(&fresh, &plan).unwrap();
    assert_eq!(
        values.pointer("/factory/trackers"),
        Some(&json!([github_tracker, plan.tracker]))
    );
    assert_eq!(
        values.pointer("/factory/bindings"),
        Some(&json!([plan.binding]))
    );
}

// ---------------------------------------------------------------------------
// The command: flags, refusals, dry-run, and preflight wiring (binary)
// ---------------------------------------------------------------------------

struct Run {
    code: i32,
    stdout: String,
    stderr: String,
}

impl Run {
    fn all(&self) -> String {
        format!("{}{}", self.stdout, self.stderr)
    }
}

/// Exit code for usage errors (cli/src/exit.rs `ExitCode::Usage`).
const USAGE_EXIT: i32 = 2;

/// A tool shim that logs `<tool> <args>` to `$SHIM_LOG` and exits `code`.
fn shim(code: i32) -> String {
    format!("#!/bin/sh\necho \"$(basename \"$0\") $*\" >> \"$SHIM_LOG\"\nexit {code}\n")
}

struct Cli {
    dir: tempfile::TempDir,
}

impl Cli {
    /// Stub kubectl, helm, kind and docker on PATH answering `code`.
    fn new(code: i32) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let bin = dir.path().join("bin");
        fs::create_dir_all(&bin).unwrap();
        for tool in ["kubectl", "helm", "kind", "docker"] {
            let path = bin.join(tool);
            fs::write(&path, shim(code)).unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
        }
        fs::write(
            dir.path().join("kubeconfig"),
            "apiVersion: v1\nkind: Config\ncurrent-context: \"\"\ncontexts:\n  - name: kind-curie-factory\n    context:\n      cluster: fixture\nclusters:\n  - name: fixture\n    cluster:\n      server: https://127.0.0.1:9\n",
        )
        .unwrap();
        fs::write(dir.path().join("jira-token"), format!("{JIRA_TOKEN}\n")).unwrap();
        Self { dir }
    }

    fn token_file(&self) -> String {
        self.dir.path().join("jira-token").display().to_string()
    }

    fn shim_log(&self) -> String {
        fs::read_to_string(self.dir.path().join("shim.log")).unwrap_or_default()
    }

    fn run(&self, extra: &[&str], github_api: Option<&str>) -> Run {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_curie"));
        cmd.args([
            "factory",
            "quickstart",
            "--repo",
            REPO,
            "--context",
            "kind-curie-factory",
            "--chart",
        ])
        .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("../charts/curie"))
        .args(extra)
        .env(
            "PATH",
            format!("{}:/usr/bin:/bin", self.dir.path().join("bin").display()),
        )
        .env("SHIM_LOG", self.dir.path().join("shim.log"))
        .env("HOME", self.dir.path())
        .env("TMPDIR", self.dir.path())
        .env("KUBECONFIG", self.dir.path().join("kubeconfig"))
        .env("CURIE_CREDENTIALS", "sk-or-fixture")
        .env_remove("CURIE_API_URL")
        .env_remove("CI")
        .env("NO_COLOR", "1");
        match github_api {
            Some(url) => cmd.env("CURIE_GITHUB_API_URL", url),
            None => cmd.env("CURIE_GITHUB_API_URL", "http://127.0.0.1:9"),
        };
        let output = cmd.stdin(Stdio::null()).output().unwrap();
        Run {
            code: output.status.code().unwrap_or(-1),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        }
    }
}

/// `--tracker jira-cloud --code-host github` with every flag it needs, as
/// owned strings. `forge_url` and the token file vary per test.
fn jira_args(forge_url: &str, token_file: &str) -> Vec<String> {
    [
        "--tracker",
        "jira-cloud",
        "--code-host",
        "github",
        "--forge-url",
        forge_url,
        "--token-file",
        token_file,
        "--forge-username",
        USERNAME,
        "--start-account",
        STARTER,
        "--start-account",
        SECOND_STARTER,
    ]
    .iter()
    .map(|s| s.to_string())
    .collect()
}

fn without(args: &[String], flag: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut skip = false;
    for arg in args {
        if skip {
            skip = false;
            continue;
        }
        if arg == flag {
            skip = true;
            continue;
        }
        out.push(arg.clone());
    }
    out
}

fn refs(args: &[String]) -> Vec<&str> {
    args.iter().map(String::as_str).collect()
}

const APP_FLAGS: [&str; 4] = ["--app-id", "12345", "--private-key-file", "/keys/app.pem"];

fn dry_run(cli: &Cli, args: &[String]) -> Run {
    let mut all = refs(args);
    all.extend(["--dry-run", "--json"]);
    all.extend(APP_FLAGS);
    cli.run(&all, None)
}

#[test]
fn factory_quickstart_jira_with_a_gitlab_code_host_is_a_pairing_refusal() {
    // #4399: gitlab is installed and pairs only with the gitlab tracker.
    let cli = Cli::new(0);
    let mut args = jira_args("https://jira.example.com", &cli.token_file());
    args = without(&args, "--code-host");
    args.extend(["--code-host".to_string(), "gitlab".to_string()]);
    let run = dry_run(&cli, &args);
    let text = run.all();
    assert_ne!(run.code, 0, "{text}");
    assert!(text.contains("pairs only with"), "{text}");
    assert!(text.contains("gitlab"), "{text}");
    assert!(!text.contains("not installed"), "{text}");
    assert_no_token(&text, "the pairing refusal");
}

#[test]
fn factory_quickstart_github_refuses_each_jira_only_flag() {
    let cli = Cli::new(0);
    for (flag, value) in [
        ("--forge-username", USERNAME),
        ("--start-account", STARTER),
        ("--start-group", GROUP),
    ] {
        let run = cli.run(&["--dry-run", "--json", flag, value], None);
        let text = run.all();
        assert_eq!(run.code, USAGE_EXIT, "{flag}: {text}");
        assert!(
            !text.contains("unexpected argument"),
            "{flag} must be a known flag the command refuses for github: {text}"
        );
        assert!(text.contains(flag), "refusal names {flag}: {text}");
        assert!(!text.contains("not installed"), "{flag}: {text}");
    }
    // Liveness: the github default without them still plans.
    let run = cli.run(&["--dry-run", "--json"], None);
    assert_eq!(run.code, 0, "{}", run.all());
}

#[test]
fn factory_quickstart_jira_refuses_incomplete_or_conflicting_flags() {
    let cli = Cli::new(0);
    let valid = jira_args("https://jira.example.com", &cli.token_file());
    let mut both = valid.clone();
    both.extend(["--start-group".to_string(), GROUP.to_string()]);
    let neither = without(&without(&valid, "--start-account"), "--start-account");
    let mut review = valid.clone();
    review.extend(["--review-account".to_string(), "reviewer".to_string()]);
    let mut not_email = without(&valid, "--forge-username");
    not_email.extend(["--forge-username".to_string(), "factory-bot".to_string()]);
    let cases: Vec<(&str, Vec<String>, Vec<&str>)> = vec![
        (
            "missing --forge-url",
            without(&valid, "--forge-url"),
            vec!["--forge-url"],
        ),
        (
            "missing --token-file",
            without(&valid, "--token-file"),
            vec!["--token-file"],
        ),
        (
            "missing --forge-username",
            without(&valid, "--forge-username"),
            vec!["--forge-username"],
        ),
        (
            "--forge-username is not an email",
            not_email,
            vec!["--forge-username"],
        ),
        (
            "both start flags",
            both,
            vec!["--start-account", "--start-group"],
        ),
        (
            "neither start flag",
            neither,
            vec!["--start-account", "--start-group"],
        ),
        ("--review-account", review, vec!["--review-account"]),
        (
            "plain http to a remote host",
            jira_args("http://jira.example.com", &cli.token_file()),
            vec!["--forge-url", "https"],
        ),
        (
            "a gateway URL without a cloud id",
            jira_args("https://api.atlassian.com/ex/jira/", &cli.token_file()),
            vec!["--forge-url"],
        ),
    ];
    for (case, args, named) in cases {
        let run = dry_run(&cli, &args);
        let text = run.all();
        assert_eq!(run.code, USAGE_EXIT, "{case}: {text}");
        for flag in named {
            assert!(text.contains(flag), "{case}: refusal names {flag}: {text}");
        }
        assert!(!text.contains("not installed"), "{case}: {text}");
        assert_no_token(&text, case);
    }
}

#[test]
fn factory_quickstart_jira_accepts_each_valid_url_form_in_a_dry_run() {
    let cli = Cli::new(0);
    for url in [
        "https://jira.example.com".to_string(),
        format!("https://api.atlassian.com/ex/jira/{CLOUD_ID}"),
        "http://localhost:8080".to_string(),
        "http://127.0.0.1:9".to_string(),
    ] {
        let run = dry_run(&cli, &jira_args(&url, &cli.token_file()));
        assert_eq!(run.code, 0, "{url}: {}", run.all());
    }
    let mut group = without(
        &without(
            &jira_args("https://jira.example.com", &cli.token_file()),
            "--start-account",
        ),
        "--start-account",
    );
    group.extend(["--start-group".to_string(), GROUP.to_string()]);
    let run = dry_run(&cli, &group);
    assert_eq!(run.code, 0, "group: {}", run.all());
}

#[test]
fn factory_quickstart_jira_dry_run_describes_the_reads_and_makes_none() {
    let cli = Cli::new(0);
    let jira = jira(CLOUD_ID);
    let run = dry_run(&cli, &jira_args(&jira.base_url, &cli.token_file()));
    assert_eq!(run.code, 0, "{}", run.all());
    let body: Value = serde_json::from_str(&run.stdout).expect("one plan object");
    let lines: Vec<String> = body["plan"]
        .as_array()
        .expect("plan lines")
        .iter()
        .map(|line| line.as_str().unwrap().to_string())
        .collect();
    let (name, secret) = derived(&jira.base_url);
    let binding = format!("{name}-<cloud id>");
    let jira_line =
        format!("Write the Jira Cloud tracker and binding `{binding}` and Secret {secret}");
    assert!(lines.contains(&jira_line), "{lines:#?}");
    assert!(
        lines.iter().any(|line| line.contains(&jira.base_url)),
        "the Jira reads are described against --forge-url: {lines:#?}"
    );
    assert!(
        lines.iter().any(|line| line.contains("cluster surfaces")
            && line.ends_with(&format!("--add jira={binding}"))),
        "{lines:#?}"
    );
    assert!(
        lines
            .iter()
            .all(|line| !line.contains("github=acme/widgets")),
        "{lines:#?}"
    );
    assert!(
        jira.recorded().is_empty(),
        "a dry run reads nothing from Jira"
    );
    assert_no_token(&run.all(), "the dry run");
}

/// The Install path runs the Jira preflight right after the App preflight,
/// before any helm or kubectl call, so a refused token mutates nothing.
#[test]
fn factory_quickstart_jira_refuses_an_unauthorized_token_before_any_cluster_call() {
    let cli = Cli::new(1);
    fs::write(cli.dir.path().join("jira-token"), WRONG_TOKEN).unwrap();
    let pem = cli.dir.path().join("app.pem");
    generate_pem(&pem);
    let (github, jira) = (github(), jira(CLOUD_ID));
    let mut args = jira_args(&jira.base_url, &cli.token_file());
    args.extend([
        "--app-id".to_string(),
        APP_ID.to_string(),
        "--private-key-file".to_string(),
        pem.display().to_string(),
    ]);
    let run = cli.run(&refs(&args), Some(&github.base_url));
    let text = run.all();
    assert_ne!(run.code, 0, "{text}");
    assert!(text.contains("--token-file"), "{text}");
    assert!(text.contains("401"), "{text}");
    assert_no_token(&text, "the command output");
    assert!(!jira.recorded().is_empty(), "the preflight reached Jira");
    let log = cli.shim_log();
    assert!(
        !log.lines()
            .any(|line| line.starts_with("helm ") || line.starts_with("kubectl ")),
        "no cluster call before the Jira preflight refuses: {log}"
    );

    // Liveness: with the right token the preflight passes (every start account
    // is checked) and the run goes on to the cluster, never naming the token.
    fs::write(cli.dir.path().join("jira-token"), JIRA_TOKEN).unwrap();
    let (github, jira) = (self::github(), self::jira(CLOUD_ID));
    let mut args = jira_args(&jira.base_url, &cli.token_file());
    args.extend([
        "--app-id".to_string(),
        APP_ID.to_string(),
        "--private-key-file".to_string(),
        pem.display().to_string(),
    ]);
    let run = cli.run(&refs(&args), Some(&github.base_url));
    let text = run.all();
    let checked: Vec<String> = jira
        .recorded()
        .iter()
        .filter(|req| jira_path(req).0 == "/rest/api/3/user")
        .filter_map(|req| param(req, "accountId"))
        .collect();
    assert!(
        checked.contains(&STARTER.to_string()) && checked.contains(&SECOND_STARTER.to_string()),
        "the preflight ran to the start accounts: {checked:?}\n{text}"
    );
    assert!(!text.contains("401"), "{text}");
    assert_no_token(&text, "the command output");
    assert_credentials_stay_on_their_forge(&jira, &github);
}

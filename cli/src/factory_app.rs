//! The factory GitHub App for `curie cluster factory` (#3746): the
//! registration link an operator opens to create the App, and the GitHub reads
//! that turn `--app-id` + `--private-key-file` into the intake settings.
//!
//! The CLI never opens a browser and never runs `gh`; it prints the link. The
//! private key is read from a file and only ever travels in a JWT signature,
//! in a Secret manifest written to kubectl's stdin, or in memory. Every GitHub
//! read and the Secret ownership check run before any mutation.

use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::Result;
use base64::Engine;

use crate::exit::CliError;
use crate::ops::{plain, run_capture, OpsCommand};

/// Homepage URL the registration form records for the App.
pub const APP_HOMEPAGE: &str = "https://github.com/curie-eng/curie";
/// Label created on allowlisted repos when none is given or recorded.
pub const DEFAULT_LABEL: &str = "curie-factory";
/// Secret holding the App private key when the release records none.
pub const DEFAULT_SECRET_NAME: &str = "curie-github-app";
/// Annotation naming the App whose key a Secret holds.
pub const APP_ID_ANNOTATION: &str = "curie.ai/github-app-id";

const LABEL_COLOR: &str = "5319e7";
const LABEL_DESCRIPTION: &str = "Hands this issue to the Curie factory";
const APP_DESCRIPTION: &str = "Curie factory: turns labeled GitHub issues into pull requests";

/// The App permissions the factory needs, in registration-URL form.
pub const PERMISSIONS: [(&str, &str); 7] = [
    ("metadata", "read"),
    ("contents", "write"),
    ("issues", "write"),
    ("pull_requests", "write"),
    ("checks", "read"),
    ("statuses", "write"),
    ("actions", "write"),
];

/// `curie-factory-<8 lowercase hex>`: unique enough that the GitHub-wide App
/// name is free, and well under GitHub's 34 character limit.
pub fn random_app_name() -> String {
    let hex = uuid::Uuid::new_v4().simple().to_string();
    format!("curie-factory-{}", &hex[..8])
}

/// The GitHub App registration form, prefilled through its documented query
/// parameters: https://docs.github.com/en/apps/sharing-github-apps/registering-a-github-app-using-url-parameters
/// No webhook and no event subscriptions. `org` selects the organization form.
pub fn registration_url(org: Option<&str>, name: &str) -> String {
    let mut url = reqwest::Url::parse("https://github.com/").expect("static URL parses");
    {
        let mut segments = url.path_segments_mut().expect("https URL has a path");
        if let Some(org) = org {
            segments.extend(["organizations", org]);
        }
        segments.extend(["settings", "apps", "new"]);
    }
    {
        let mut query = url.query_pairs_mut();
        query
            .append_pair("name", name)
            .append_pair("description", APP_DESCRIPTION)
            .append_pair("url", APP_HOMEPAGE)
            .append_pair("public", "false")
            .append_pair("webhook_active", "false");
        for (permission, access) in PERMISSIONS {
            query.append_pair(permission, access);
        }
    }
    url.to_string()
}

/// The four steps between the link and a working intake.
pub fn registration_steps() -> Vec<String> {
    vec![
        "Open the link and click Create GitHub App; the name, permissions, and the disabled webhook are already filled in.".to_string(),
        "On the new App's settings page, note the App ID and click Generate a private key to download the .pem file.".to_string(),
        "Click Install App and install it on the repositories the factory may work in.".to_string(),
        "Rerun: curie cluster factory --intake poll --app-id <APP_ID> --private-key-file <PATH.pem>".to_string(),
    ]
}

/// True when an allowlist entry is covered by the App's installation: an
/// exact `owner/repo` match (case insensitive), or `owner/*` when the App is
/// installed on at least one repository of that owner.
pub fn repo_entry_installed(entry: &str, installed: &[String]) -> bool {
    let entry = entry.trim();
    if let Some(owner) = entry.strip_suffix("/*") {
        return !owner.is_empty()
            && !owner.contains('/')
            && installed.iter().any(|repo| {
                repo.split_once('/')
                    .is_some_and(|(o, _)| o.eq_ignore_ascii_case(owner))
            });
    }
    installed
        .iter()
        .any(|repo| repo.eq_ignore_ascii_case(entry))
}

/// Read and shape-check the App private key. The contents never reach an
/// error message.
pub fn read_private_key(path: &std::path::Path) -> Result<String> {
    let pem = std::fs::read_to_string(path).map_err(|error| {
        CliError::usage(format!(
            "cannot read --private-key-file {}: {error}",
            path.display()
        ))
        .with_fix("pass the .pem file downloaded from the App's settings page")
    })?;
    if !crate::github_app::is_pem_private_key(&pem) {
        return Err(CliError::usage(format!(
            "--private-key-file {} is not a PEM private key",
            path.display()
        ))
        .with_fix("pass the .pem file from the App's settings page under 'Private keys'")
        .into());
    }
    Ok(pem)
}

/// A thin GitHub REST client over `CURIE_GITHUB_API_URL` / `GITHUB_API_URL`
/// / api.github.com.
pub struct GithubApi {
    client: reqwest::Client,
    base: reqwest::Url,
}

impl GithubApi {
    pub fn new() -> Result<Self> {
        let raw = crate::github_app::github_api_url(crate::github_app::DEFAULT_CLONE_BASE);
        let base = reqwest::Url::parse(&raw).map_err(|error| {
            CliError::usage(format!("GitHub API URL {raw:?} is not a URL: {error}"))
        })?;
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(20))
            .build()
            .map_err(|err| CliError::failure(format!("could not build an HTTP client: {err}")))?;
        Ok(Self { client, base })
    }

    fn url(&self, segments: &[&str]) -> reqwest::Url {
        let mut url = self.base.clone();
        if let Ok(mut path) = url.path_segments_mut() {
            path.pop_if_empty().extend(segments);
        }
        url
    }

    /// One call; returns the status and the parsed body (Null when not JSON).
    /// `auth`, when required by the endpoint, is never echoed.
    async fn call(
        &self,
        method: reqwest::Method,
        url: reqwest::Url,
        auth: Option<&str>,
        body: Option<&serde_json::Value>,
    ) -> Result<(u16, serde_json::Value)> {
        let shown = format!("{method} {}", url.path());
        let mut request = self
            .client
            .request(method, url)
            .header("Accept", "application/vnd.github+json")
            .header("X-GitHub-Api-Version", "2022-11-28")
            .header("User-Agent", format!("curie/{}", env!("CARGO_PKG_VERSION")));
        if let Some(auth) = auth {
            request = request.bearer_auth(auth);
        }
        if let Some(body) = body {
            request = request.json(body);
        }
        let response = request
            .send()
            .await
            .map_err(|error| CliError::transient(format!("GitHub call {shown} failed: {error}")))?;
        let status = response.status().as_u16();
        let text = response.text().await.unwrap_or_default();
        let parsed = serde_json::from_str(&text).unwrap_or(serde_json::Value::Null);
        if status >= 500 {
            return Err(
                CliError::transient(format!("GitHub returned HTTP {status} for {shown}")).into(),
            );
        }
        Ok((status, parsed))
    }

    async fn get_app(&self, jwt: &str) -> Result<(u16, serde_json::Value)> {
        self.call(reqwest::Method::GET, self.url(&["app"]), Some(jwt), None)
            .await
    }

    /// The numeric user id comes from GitHub's documented user response,
    /// not the App id. Public user data requires no authentication:
    /// https://docs.github.com/en/rest/users/users#get-a-user.
    pub async fn bot_publication_identity(&self, slug: &str) -> Result<PublicationIdentity> {
        let name = format!("{slug}[bot]");
        let failure = |detail: String| {
            let remedy = "check the App bot account and retry; to override commit identity separately, use curie cluster upgrade --set worker.publication.gitUserEmail=<email> and --set worker.publication.gitUserName=<name>";
            CliError::transient(format!(
                "GitHub bot lookup GET /users/{name} failed: {detail}; nothing was applied; {remedy}"
            ))
            .with_fix(remedy)
        };
        let (status, body) = self
            .call(
                reqwest::Method::GET,
                self.url(&["users", &name]),
                None,
                None,
            )
            .await
            .map_err(|error| failure(error.to_string()))?;
        if status != 200 {
            return Err(failure(format!("GitHub returned HTTP {status}")).into());
        }
        let id = body
            .get("id")
            .and_then(serde_json::Value::as_u64)
            .filter(|id| *id > 0)
            .ok_or_else(|| failure("GitHub returned no positive numeric user id".into()))?;
        Ok(PublicationIdentity {
            email: format!("{id}+{name}@users.noreply.github.com"),
            name,
        })
    }

    /// Resolve once, then keep every Contents API read on the same commit.
    /// Shapes and `ref` semantics are documented at
    /// https://docs.github.com/en/rest/repos/repos#get-a-repository and
    /// https://docs.github.com/en/rest/commits/commits#get-a-commit.
    pub async fn default_branch_commit(&self, app: &InstalledApp, repo: &str) -> Result<String> {
        let (owner, name) = repo
            .split_once('/')
            .ok_or_else(|| CliError::usage("repository must be owner/repo"))?;
        let token = app
            .token_for(repo)
            .ok_or_else(|| CliError::failure("repository is outside the App installation"))?;
        let (status, metadata) = self
            .call(
                reqwest::Method::GET,
                self.url(&["repos", owner, name]),
                Some(token),
                None,
            )
            .await?;
        if !(200..300).contains(&status) {
            return Err(unexpected(status, "GET repository metadata"));
        }
        let branch = metadata
            .get("default_branch")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                CliError::failure("GitHub repository response carries no default_branch")
            })?;
        let (status, commit) = self
            .call(
                reqwest::Method::GET,
                self.url(&["repos", owner, name, "commits", branch]),
                Some(token),
                None,
            )
            .await?;
        if !(200..300).contains(&status) {
            return Err(unexpected(status, "GET default branch commit"));
        }
        commit
            .get("sha")
            .and_then(|v| v.as_str())
            .filter(|s| s.len() >= 40 && s.chars().all(|c| c.is_ascii_hexdigit()))
            .map(str::to_string)
            .ok_or_else(|| CliError::failure("GitHub commit response carries no valid sha").into())
    }

    /// A directory is an array and a file has base64 content. Neither auth
    /// failures nor malformed successful responses mean a missing manifest.
    /// https://docs.github.com/en/rest/repos/contents#get-repository-content
    pub async fn contents(
        &self,
        app: &InstalledApp,
        repo: &str,
        path: &str,
        commit: &str,
    ) -> Result<RepositoryContent> {
        let (owner, name) = repo
            .split_once('/')
            .ok_or_else(|| CliError::usage("repository must be owner/repo"))?;
        let token = app
            .token_for(repo)
            .ok_or_else(|| CliError::failure("repository is outside the App installation"))?;
        let mut segments = vec!["repos", owner, name, "contents"];
        segments.extend(path.split('/').filter(|part| !part.is_empty()));
        let mut url = self.url(&segments);
        url.query_pairs_mut().append_pair("ref", commit);
        let (status, body) = self
            .call(reqwest::Method::GET, url, Some(token), None)
            .await?;
        if status == 404 {
            return Ok(RepositoryContent::Missing);
        }
        if !(200..300).contains(&status) {
            return Err(unexpected(status, "GET repository contents"));
        }
        if body.is_array() {
            let entries: Vec<RepositoryEntry> = serde_json::from_value(body).map_err(|_| {
                CliError::failure(format!(
                    "GitHub returned a malformed contents directory for {path}"
                ))
            })?;
            return Ok(RepositoryContent::Directory(entries));
        }
        #[derive(serde::Deserialize)]
        struct File {
            #[serde(rename = "type")]
            kind: String,
            encoding: String,
            content: String,
        }
        let file: File = serde_json::from_value(body).map_err(|_| {
            CliError::failure(format!("GitHub returned malformed contents for {path}"))
        })?;
        if file.kind != "file" || file.encoding != "base64" {
            return Err(CliError::failure(format!(
                "GitHub returned unsupported contents encoding for {path}"
            ))
            .into());
        }
        let encoded: String = file
            .content
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect();
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(|_| {
                CliError::failure(format!(
                    "GitHub returned invalid base64 contents for {path}"
                ))
            })?;
        let text = String::from_utf8(bytes).map_err(|_| {
            CliError::failure(format!("repository toolchain file {path} is not UTF-8"))
        })?;
        Ok(RepositoryContent::File(text))
    }
}

#[derive(serde::Deserialize)]
pub struct RepositoryEntry {
    pub path: String,
    #[serde(rename = "type")]
    pub kind: String,
}

pub enum RepositoryContent {
    Missing,
    Directory(Vec<RepositoryEntry>),
    File(String),
}

fn unexpected(status: u16, what: &str) -> anyhow::Error {
    CliError::failure(format!(
        "GitHub returned HTTP {status} for {what}; nothing was applied"
    ))
    .into()
}

fn json_id(body: &serde_json::Value, key: &str) -> Option<String> {
    match body.get(key)? {
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::String(s) => Some(s.clone()),
        _ => None,
    }
}

/// What GitHub reports about the App and where it is installed.
pub struct InstalledApp {
    pub slug: String,
    /// `owner/repo` names, in GitHub's order, deduplicated.
    pub repos: Vec<String>,
    /// Lowercased `owner/repo` -> installation access token.
    tokens: BTreeMap<String, String>,
}

/// The GitHub App bot's author identity for factory publication.
pub struct PublicationIdentity {
    pub name: String,
    pub email: String,
}

impl InstalledApp {
    pub fn token_for(&self, repo: &str) -> Option<&str> {
        self.tokens
            .get(&repo.to_ascii_lowercase())
            .map(String::as_str)
    }
}

fn not_installed(slug: &str) -> anyhow::Error {
    CliError::failure(format!(
        "GitHub App {slug} is not installed on any repository; nothing was applied"
    ))
    .with_fix(format!(
        "install it at https://github.com/apps/{slug}/installations/new on the repositories \
         the factory may work in, then rerun"
    ))
    .into()
}

/// Authenticate as the App, then list every repository its installations
/// reach. Read-only.
pub async fn inspect_app(api: &GithubApi, app_id: &str, pem: &str) -> Result<InstalledApp> {
    let jwt = crate::github_app::sign_app_jwt(app_id, pem)?;
    let (status, app) = api.get_app(&jwt).await?;
    if status == 401 || status == 403 {
        return Err(CliError::failure(format!(
            "the private key does not authenticate as GitHub App {app_id} (HTTP {status} for \
             GET /app); nothing was applied"
        ))
        .with_fix(format!(
            "pass the App ID shown on the App's settings page and a private key generated \
             there for App {app_id}"
        ))
        .into());
    }
    if !(200..300).contains(&status) {
        return Err(unexpected(status, "GET /app"));
    }
    let reported = json_id(&app, "id").unwrap_or_else(|| "<missing>".into());
    if reported != app_id {
        return Err(CliError::failure(format!(
            "the private key authenticates as GitHub App {reported}, not --app-id {app_id}; \
             nothing was applied"
        ))
        .with_fix(format!(
            "pass --app-id {reported} or the key of App {app_id}"
        ))
        .into());
    }
    let slug = app
        .get("slug")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| CliError::failure("GitHub's GET /app response carries no slug"))?
        .to_string();

    let mut ids: Vec<String> = Vec::new();
    let mut page = 1u32;
    loop {
        let mut url = api.url(&["app", "installations"]);
        url.query_pairs_mut()
            .append_pair("per_page", "100")
            .append_pair("page", &page.to_string());
        let (status, installations) = api
            .call(reqwest::Method::GET, url, Some(&jwt), None)
            .await?;
        if !(200..300).contains(&status) {
            return Err(unexpected(status, "GET /app/installations"));
        }
        let listed: Vec<String> = installations
            .as_array()
            .map(|list| list.iter().filter_map(|i| json_id(i, "id")).collect())
            .unwrap_or_default();
        let count = listed.len();
        for id in listed {
            if !ids.contains(&id) {
                ids.push(id);
            }
        }
        if count < 100 {
            break;
        }
        page += 1;
    }
    if ids.is_empty() {
        return Err(not_installed(&slug));
    }

    let mut repos = Vec::new();
    let mut tokens = BTreeMap::new();
    for id in ids {
        let (status, created) = api
            .call(
                reqwest::Method::POST,
                api.url(&["app", "installations", &id, "access_tokens"]),
                Some(&jwt),
                None,
            )
            .await?;
        if !(200..300).contains(&status) {
            return Err(unexpected(
                status,
                &format!("POST /app/installations/{id}/access_tokens"),
            ));
        }
        let token = created
            .get("token")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                CliError::failure(format!(
                    "GitHub returned no token for installation {id}; nothing was applied"
                ))
            })?
            .to_string();
        let mut page = 1u32;
        loop {
            let mut url = api.url(&["installation", "repositories"]);
            url.query_pairs_mut()
                .append_pair("per_page", "100")
                .append_pair("page", &page.to_string());
            let (status, body) = api
                .call(reqwest::Method::GET, url, Some(&token), None)
                .await?;
            if !(200..300).contains(&status) {
                return Err(unexpected(status, "GET /installation/repositories"));
            }
            let listed: Vec<String> = body
                .get("repositories")
                .and_then(|v| v.as_array())
                .map(|list| {
                    list.iter()
                        .filter_map(|r| r.get("full_name").and_then(|v| v.as_str()))
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_default();
            let count = listed.len();
            for repo in listed {
                let key = repo.to_ascii_lowercase();
                if let std::collections::btree_map::Entry::Vacant(slot) = tokens.entry(key) {
                    slot.insert(token.clone());
                    repos.push(repo);
                }
            }
            if count < 100 {
                break;
            }
            page += 1;
        }
    }
    if repos.is_empty() {
        return Err(not_installed(&slug));
    }
    Ok(InstalledApp {
        slug,
        repos,
        tokens,
    })
}

/// Validate `--repo` entries against the installation. Empty input means the
/// installed repositories become the allowlist.
pub fn resolve_allowlist(requested: &[String], app: &InstalledApp) -> Result<Vec<String>> {
    if requested.is_empty() {
        return Ok(app.repos.clone());
    }
    let missing: Vec<&String> = requested
        .iter()
        .filter(|entry| !repo_entry_installed(entry, &app.repos))
        .collect();
    if !missing.is_empty() {
        let names: Vec<&str> = missing.iter().map(|s| s.as_str()).collect();
        return Err(CliError::usage(format!(
            "GitHub App {} is not installed on {}; it is installed on {}; nothing was applied",
            app.slug,
            names.join(", "),
            app.repos.join(", ")
        ))
        .with_fix(format!(
            "install the App on those repositories at https://github.com/apps/{}/installations/new \
             or drop them from --repo",
            app.slug
        ))
        .into());
    }
    Ok(requested.to_vec())
}

/// What to do with the Secret holding the App key.
pub enum SecretPlan {
    /// The Secret already holds this exact key.
    Unchanged,
    /// Apply this manifest with `kubectl apply -f -`.
    Write(serde_json::Value),
}

fn secret_manifest(
    existing: Option<&serde_json::Value>,
    namespace: &str,
    name: &str,
    key: &str,
    app_id: &str,
    pem: &str,
) -> serde_json::Value {
    let mut data = existing
        .and_then(|s| s.get("data"))
        .and_then(|d| d.as_object())
        .cloned()
        .unwrap_or_default();
    data.insert(
        key.to_string(),
        serde_json::json!(base64::engine::general_purpose::STANDARD.encode(pem.as_bytes())),
    );
    let mut annotations = existing
        .and_then(|s| s.pointer("/metadata/annotations"))
        .and_then(|a| a.as_object())
        .cloned()
        .unwrap_or_default();
    annotations.remove("kubectl.kubernetes.io/last-applied-configuration");
    annotations.insert(APP_ID_ANNOTATION.to_string(), serde_json::json!(app_id));
    let secret_type = existing
        .and_then(|s| s.get("type"))
        .cloned()
        .unwrap_or_else(|| serde_json::json!("Opaque"));
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "type": secret_type,
        "metadata": {"name": name, "namespace": namespace, "annotations": annotations},
        "data": data,
    })
}

/// Read the Secret and decide whether the supplied key may be written to it.
/// A Secret that holds a different key is replaced only when it is provably
/// this App's (annotation, or the stored key authenticates as this App).
pub async fn plan_secret(
    api: &GithubApi,
    namespace: &str,
    name: &str,
    key: &str,
    app_id: &str,
    pem: &str,
) -> Result<SecretPlan> {
    let cmd = OpsCommand::new(
        "kubectl",
        vec![
            plain("-n"),
            plain(namespace),
            plain("get"),
            plain("secret"),
            plain(name),
            plain("-o"),
            plain("json"),
        ],
    );
    let (ok, stdout, stderr) = run_capture(&cmd).await?;
    if !ok {
        if stderr.contains("NotFound") || stderr.contains("not found") {
            return Ok(SecretPlan::Write(secret_manifest(
                None, namespace, name, key, app_id, pem,
            )));
        }
        return Err(secret_read_failure(namespace, name));
    }
    let existing: serde_json::Value = serde_json::from_str(stdout.trim()).map_err(|error| {
        CliError::failure(format!("Secret {name} is not readable JSON: {error}"))
    })?;
    let stored = existing
        .pointer(&format!(
            "/data/{}",
            key.replace('~', "~0").replace('/', "~1")
        ))
        .and_then(|v| v.as_str())
        .and_then(|b| base64::engine::general_purpose::STANDARD.decode(b).ok())
        .map(|bytes| String::from_utf8_lossy(&bytes).into_owned());
    let Some(stored) = stored.filter(|s| !s.trim().is_empty()) else {
        return Ok(SecretPlan::Write(secret_manifest(
            Some(&existing),
            namespace,
            name,
            key,
            app_id,
            pem,
        )));
    };
    if stored.trim() == pem.trim() {
        return Ok(SecretPlan::Unchanged);
    }
    let annotated = existing
        .pointer("/metadata/annotations")
        .and_then(|a| a.get(APP_ID_ANNOTATION))
        .and_then(|v| v.as_str())
        .map(str::to_string);
    // The annotation is metadata only; the stored key must itself
    // authenticate as this App before it may be replaced.
    let same_app = match crate::github_app::sign_app_jwt(app_id, &stored) {
        Ok(jwt) => {
            let (status, body) = api.get_app(&jwt).await?;
            (200..300).contains(&status) && json_id(&body, "id").as_deref() == Some(app_id)
        }
        Err(_) => false,
    };
    if !same_app {
        let owner = annotated.unwrap_or_else(|| "unknown".to_string());
        return Err(CliError::failure(format!(
            "Secret {name} in namespace {namespace} holds the key of a different GitHub App \
             ({owner}); it was not replaced and nothing was applied"
        ))
        .with_fix(format!(
            "delete Secret {name} if it is stale, or point the release at another Secret with \
             `curie cluster github-app --existing-secret`"
        ))
        .into());
    }
    Ok(SecretPlan::Write(secret_manifest(
        Some(&existing),
        namespace,
        name,
        key,
        app_id,
        pem,
    )))
}

/// `kubectl -n <ns> apply -f -`; the manifest travels on stdin only.
pub fn secret_apply_command(namespace: &str) -> OpsCommand {
    OpsCommand::new(
        "kubectl",
        vec![
            plain("-n"),
            plain(namespace),
            plain("apply"),
            plain("-f"),
            plain("-"),
        ],
    )
}

pub async fn apply_secret(namespace: &str, manifest: &serde_json::Value) -> Result<()> {
    let body = serde_json::to_vec(manifest)?;
    let name = manifest
        .pointer("/metadata/name")
        .and_then(|v| v.as_str())
        .unwrap_or(DEFAULT_SECRET_NAME);
    // kubectl's stderr is dropped on purpose: a failed apply can echo the
    // patch, which carries the base64 private key.
    let (ok, _, _stderr) =
        crate::ops::run_capture_with_stdin(&secret_apply_command(namespace), &body).await?;
    if !ok {
        return Err(secret_write_failure(namespace, name));
    }
    Ok(())
}

/// Sanitized: never carries kubectl output.
pub fn secret_read_failure(namespace: &str, name: &str) -> anyhow::Error {
    CliError::failure(format!(
        "reading Secret {name} in namespace {namespace} failed; nothing was applied"
    ))
    .with_fix(format!(
        "check access with `kubectl -n {namespace} get secret {name}` and rerun"
    ))
    .into()
}

/// Sanitized: never carries kubectl output, which can echo the key.
pub fn secret_write_failure(namespace: &str, name: &str) -> anyhow::Error {
    CliError::failure(format!(
        "writing the GitHub App key to Secret {name} in namespace {namespace} failed; \
         kubectl output is withheld because it can contain the key"
    ))
    .with_fix(format!(
        "check that you may apply Secrets in namespace {namespace} and rerun"
    ))
    .into()
}

/// Concrete repositories to label: exact entries as given, `owner/*`
/// expanded against the installed repositories. Deduplicated case
/// insensitively, order preserved.
pub fn label_targets(allowlist: &[String], installed: &[String]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut push = |repo: &str| {
        if !out.iter().any(|r| r.eq_ignore_ascii_case(repo)) {
            out.push(repo.to_string());
        }
    };
    for entry in allowlist {
        if let Some(owner) = entry.trim().strip_suffix("/*") {
            for repo in installed {
                if repo
                    .split_once('/')
                    .is_some_and(|(o, _)| o.eq_ignore_ascii_case(owner))
                {
                    push(repo);
                }
            }
        } else {
            push(entry.trim());
        }
    }
    out
}

/// Preflight read: does `label` exist on `repo`? Mutates nothing.
pub async fn label_exists(
    api: &GithubApi,
    app: &InstalledApp,
    repo: &str,
    label: &str,
) -> Result<bool> {
    let (Some((owner, name)), Some(token)) = (repo.split_once('/'), app.token_for(repo)) else {
        return Err(CliError::failure(format!(
            "no installation token covers {repo}; nothing was applied"
        ))
        .into());
    };
    let (status, _) = api
        .call(
            reqwest::Method::GET,
            api.url(&["repos", owner, name, "labels", label]),
            Some(token),
            None,
        )
        .await?;
    match status {
        200..=299 => Ok(true),
        404 => Ok(false),
        _ => Err(unexpected(
            status,
            &format!("GET /repos/{repo}/labels/{label}"),
        )),
    }
}

/// True only for GitHub's duplicate-label validation error.
pub fn is_already_exists(body: &serde_json::Value) -> bool {
    body.get("errors")
        .and_then(|e| e.as_array())
        .is_some_and(|errors| {
            errors
                .iter()
                .any(|e| e.get("code").and_then(|c| c.as_str()) == Some("already_exists"))
        })
}

/// Create `label` on `repo`. Runs after the Secret write, so its errors do
/// not claim nothing was applied.
pub async fn create_label(
    api: &GithubApi,
    app: &InstalledApp,
    repo: &str,
    label: &str,
) -> Result<()> {
    let (Some((owner, name)), Some(token)) = (repo.split_once('/'), app.token_for(repo)) else {
        return Err(CliError::failure(format!("no installation token covers {repo}")).into());
    };
    let body = serde_json::json!({
        "name": label,
        "color": LABEL_COLOR,
        "description": LABEL_DESCRIPTION,
    });
    let (status, response) = api
        .call(
            reqwest::Method::POST,
            api.url(&["repos", owner, name, "labels"]),
            Some(token),
            Some(&body),
        )
        .await?;
    if (200..300).contains(&status) || (status == 422 && is_already_exists(&response)) {
        return Ok(());
    }
    Err(CliError::failure(format!(
        "GitHub returned HTTP {status} creating label {label} in {repo}; the App key Secret \
         may already be written and earlier labels created"
    ))
    .with_fix("fix the cause and rerun; the command is safe to repeat")
    .into())
}

/// `cluster factory` without an App: print the registration link and steps.
pub struct FactoryAppRegistrationOutput {
    pub url: String,
    pub steps: Vec<String>,
}

impl crate::ui::CliOutput for FactoryAppRegistrationOutput {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "github_app_registration_url": self.url,
            "steps": self.steps,
        })
    }

    fn render(&self, ui: &crate::ui::Ui) {
        ui.payload("No GitHub App is recorded on this release. Register the factory App:");
        ui.payload_plain(&self.url);
        for (i, step) in self.steps.iter().enumerate() {
            ui.payload_plain(&format!("{}. {step}", i + 1));
        }
        ui.payload_plain("Nothing was applied.");
    }
}

/// A value the command chose, and whether it was inferred.
pub struct Chosen<T> {
    pub value: T,
    pub inferred: bool,
}

/// `cluster factory --app-id ... --private-key-file ...` result.
pub struct FactoryAppSetupOutput {
    pub app_id: String,
    pub slug: String,
    pub mention: Chosen<String>,
    pub repos: Chosen<Vec<String>>,
    pub label: Chosen<String>,
    pub labels_created: Vec<String>,
    pub secret: String,
    pub secret_written: bool,
}

impl crate::ui::CliOutput for FactoryAppSetupOutput {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "factory_intake_enabled": true,
            "github_app": {
                "app_id": self.app_id,
                "slug": self.slug,
                "mention": self.mention.value,
                "mention_inferred": self.mention.inferred,
                "repos": self.repos.value,
                "repos_inferred": self.repos.inferred,
                "label": self.label.value,
                "label_inferred": self.label.inferred,
                "labels_created": self.labels_created,
                "secret": self.secret,
                "secret_written": self.secret_written,
            }
        })
    }

    fn render(&self, ui: &crate::ui::Ui) {
        ui.payload(&format!(
            "GitHub factory intake enabled with App {} ({})",
            self.slug, self.app_id
        ));
        let mention_note = if self.mention.inferred {
            " (the App slug)"
        } else {
            ""
        };
        ui.payload_plain(&format!("mention: {}{mention_note}", self.mention.value));
        let repos_note = if self.repos.inferred {
            " (the App's installed repositories)"
        } else {
            ""
        };
        ui.payload_plain(&format!(
            "allowlist: {}{repos_note}",
            self.repos.value.join(", ")
        ));
        let mut label_notes = Vec::new();
        if self.label.inferred {
            label_notes.push("default".to_string());
        }
        if !self.labels_created.is_empty() {
            label_notes.push(format!("created in {}", self.labels_created.join(", ")));
        }
        let label_note = if label_notes.is_empty() {
            String::new()
        } else {
            format!(" ({})", label_notes.join("; "))
        };
        ui.payload_plain(&format!("label: {}{label_note}", self.label.value));
        ui.payload_plain(&format!(
            "private key: Secret {} ({})",
            self.secret,
            if self.secret_written {
                "written"
            } else {
                "unchanged"
            }
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn query(url: &str) -> Vec<(String, String)> {
        let parsed = reqwest::Url::parse(url).expect("url");
        parsed.query_pairs().into_owned().collect()
    }

    #[test]
    fn user_registration_url_carries_every_parameter_and_no_events() {
        let url = registration_url(None, "curie-factory-0a1b2c3d");
        assert!(
            url.starts_with("https://github.com/settings/apps/new?"),
            "{url}"
        );
        let pairs = query(&url);
        let get = |k: &str| {
            pairs
                .iter()
                .find(|(key, _)| key == k)
                .map(|(_, v)| v.as_str())
        };
        assert_eq!(get("name"), Some("curie-factory-0a1b2c3d"));
        assert_eq!(get("url"), Some(APP_HOMEPAGE));
        assert_eq!(get("description"), Some(APP_DESCRIPTION));
        assert_eq!(get("public"), Some("false"));
        assert_eq!(get("webhook_active"), Some("false"));
        for (permission, access) in PERMISSIONS {
            assert_eq!(get(permission), Some(access), "{permission}");
        }
        assert!(!pairs.iter().any(|(k, _)| k.starts_with("events")), "{url}");
    }

    #[test]
    fn org_registration_url_uses_the_organization_form() {
        let url = registration_url(Some("acme"), "curie-factory-0a1b2c3d");
        assert!(
            url.starts_with("https://github.com/organizations/acme/settings/apps/new?"),
            "{url}"
        );
        let pairs = query(&url);
        assert!(pairs.contains(&("contents".into(), "write".into())));
        assert!(!pairs.iter().any(|(k, _)| k.starts_with("events")));
    }

    #[test]
    fn random_names_fit_github_limits() {
        let name = random_app_name();
        assert!(name.starts_with("curie-factory-"));
        assert!(name.len() <= 34, "{name}");
        let suffix = name.trim_start_matches("curie-factory-");
        assert_eq!(suffix.len(), 8);
        assert!(suffix
            .chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
    }

    #[test]
    fn only_a_duplicate_label_422_counts_as_present() {
        let dup = serde_json::json!({"message": "Validation Failed",
            "errors": [{"resource": "Label", "code": "already_exists", "field": "name"}]});
        assert!(is_already_exists(&dup));
        let invalid = serde_json::json!({"message": "Validation Failed",
            "errors": [{"resource": "Label", "code": "invalid", "field": "color"}]});
        assert!(!is_already_exists(&invalid));
        assert!(!is_already_exists(
            &serde_json::json!({"message": "spammed"})
        ));
        assert!(!is_already_exists(&serde_json::Value::Null));
    }

    #[test]
    fn wildcards_expand_against_installed_repos_for_labels() {
        let installed = vec![
            "acme/bot".to_string(),
            "Acme/Web".to_string(),
            "other/x".to_string(),
        ];
        let allow = vec!["acme/*".to_string(), "ACME/BOT".to_string()];
        assert_eq!(
            label_targets(&allow, &installed),
            vec!["acme/bot".to_string(), "Acme/Web".to_string()]
        );
        assert_eq!(
            label_targets(&["other/x".to_string()], &installed),
            vec!["other/x".to_string()]
        );
    }

    #[test]
    fn secret_errors_carry_no_kubectl_output() {
        for error in [
            secret_write_failure("curie", "curie-github-app"),
            secret_read_failure("curie", "curie-github-app"),
        ] {
            let text = format!("{error:#}");
            assert!(text.contains("curie-github-app"), "{text}");
            assert!(!text.contains("BEGIN"), "{text}");
            assert!(!text.contains("data"), "{text}");
        }
    }

    #[test]
    fn repo_membership_is_exact_case_insensitive_or_owner_wildcard() {
        let installed = vec!["Acme/Bot".to_string(), "acme/web".to_string()];
        assert!(repo_entry_installed("Acme/Bot", &installed));
        assert!(repo_entry_installed("acme/bot", &installed));
        assert!(repo_entry_installed("ACME/WEB", &installed));
        assert!(!repo_entry_installed("acme/other", &installed));
        assert!(!repo_entry_installed("acme/bo", &installed));
        assert!(repo_entry_installed("acme/*", &installed));
        assert!(repo_entry_installed("ACME/*", &installed));
        assert!(!repo_entry_installed("other/*", &installed));
        assert!(!repo_entry_installed("/*", &installed));
        assert!(!repo_entry_installed("acme/bot/*", &installed));
    }
}

//! The GitLab REST v4 reads and writes of the quickstart (#4399 decision 23),
//! against the documented endpoints:
//! https://docs.gitlab.com/api/version/
//! https://docs.gitlab.com/api/personal_access_tokens/#get-single-personal-access-token
//! https://docs.gitlab.com/api/users/#get-the-current-user
//! https://docs.gitlab.com/api/projects/#get-a-single-project
//! https://docs.gitlab.com/api/members/#get-a-member-of-a-group-or-project-including-inherited-and-invited-members
//! https://docs.gitlab.com/api/labels/
//!
//! The token travels only in the `PRIVATE-TOKEN` header; no message carries
//! it, a response body, or the header.

use std::path::{Path, PathBuf};

use anyhow::Result;
use serde_json::{json, Value};

use super::text;
use crate::exit::CliError;

/// The lowest GitLab release the adapter supports, as (major, minor).
const MIN_VERSION: (u64, u64) = (17, 0);
/// Developer: the lowest access level that may push a branch and open a
/// merge request.
const MIN_ACCESS_LEVEL: u64 = 30;
/// The scopes the factory's token needs.
const SCOPES: [&str; 2] = ["api", "write_repository"];

pub(super) struct Gitlab {
    client: reqwest::Client,
    api: String,
    token: String,
    token_file: PathBuf,
}

/// What steps a to e found.
#[derive(Debug, Clone)]
pub(super) struct Found {
    pub version: String,
    pub username: String,
    /// The token's expiry date, `YYYY-MM-DD`; `None` when it has none.
    pub expires_at: Option<String>,
    pub project_id: u64,
    pub path: String,
    pub default_branch: Option<String>,
    pub access_level: u64,
}

/// One response: the status, the parsed body (Null when not JSON) and the
/// `x-next-page` header.
struct Reply {
    status: u16,
    body: Value,
    next_page: Option<String>,
}

impl Gitlab {
    /// A client for the API under `web_base` authenticating with the token in
    /// `token_file`, trusting `ca_file` when given.
    pub fn connect(web_base: &str, token_file: &Path, ca_file: Option<&Path>) -> Result<Self> {
        Ok(Self {
            client: super::super::http_client(ca_file)?,
            api: format!("{web_base}/api/v4"),
            token: super::super::read_token(token_file)?,
            token_file: token_file.to_path_buf(),
        })
    }

    async fn send(
        &self,
        method: reqwest::Method,
        path: &str,
        query: &[(&str, &str)],
        body: Option<&Value>,
    ) -> Result<Reply> {
        let mut request = self
            .client
            .request(method.clone(), format!("{}{path}", self.api))
            .query(query)
            .header("PRIVATE-TOKEN", &self.token)
            .header("Accept", "application/json")
            .header("User-Agent", format!("curie/{}", env!("CARGO_PKG_VERSION")));
        if let Some(body) = body {
            request = request.json(body);
        }
        let response = request.send().await.map_err(|error| {
            CliError::transient(format!(
                "GitLab call {method} /api/v4{path} failed: {}; check --forge-url and --ca-file",
                error.without_url()
            ))
        })?;
        let status = response.status().as_u16();
        let next_page = response
            .headers()
            .get("x-next-page")
            .and_then(|value| value.to_str().ok())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);
        let raw = response.text().await.unwrap_or_default();
        if status == 401 {
            return Err(CliError::usage(format!(
                "GitLab refused the token in --token-file {} (HTTP 401 for {method} /api/v4{path})",
                self.token_file.display()
            ))
            .with_fix("pass a current GitLab token with the api and write_repository scopes")
            .into());
        }
        Ok(Reply {
            status,
            body: serde_json::from_str(&raw).unwrap_or(Value::Null),
            next_page,
        })
    }

    async fn get(&self, path: &str, query: &[(&str, &str)]) -> Result<Reply> {
        self.send(reqwest::Method::GET, path, query, None).await
    }
}

fn unexpected(status: u16, what: &str) -> anyhow::Error {
    CliError::failure(format!(
        "GitLab answered HTTP {status} for {what}; nothing was applied"
    ))
    .with_fix("check --forge-url names the GitLab web address and rerun")
    .into()
}

/// The `(major, minor)` a GitLab version string starts with.
fn major_minor(version: &str) -> Option<(u64, u64)> {
    let mut parts = version.split(|c: char| !c.is_ascii_digit());
    Some((parts.next()?.parse().ok()?, parts.next()?.parse().ok()?))
}

/// The project path as one URL path segment.
fn encoded(path: &str) -> String {
    path.replace('/', "%2F")
}

/// The role name of a GitLab access level.
pub(super) fn role(level: u64) -> &'static str {
    match level {
        0 => "no access",
        5 => "Minimal Access",
        10 => "Guest",
        15 => "Planner",
        20 => "Reporter",
        30 => "Developer",
        40 => "Maintainer",
        50 => "Owner",
        _ => "custom",
    }
}

/// Steps a to e: the server version, the token, its user, the project and the
/// user's access level on it. Reads only; refuses with a message naming the
/// cause before any cluster call.
pub(super) async fn preflight(gitlab: &Gitlab, repo: &str) -> Result<Found> {
    // (a) The server version.
    let reply = gitlab.get("/version", &[]).await?;
    if reply.status != 200 {
        return Err(unexpected(reply.status, "GET /api/v4/version"));
    }
    let version = text(&reply.body, "version").unwrap_or_default().to_string();
    let Some((major, minor)) = major_minor(&version) else {
        return Err(CliError::failure(format!(
            "GitLab reported version {version:?}, which is not a GitLab version"
        ))
        .with_fix("check --forge-url names the GitLab web address and rerun")
        .into());
    };
    if (major, minor) < MIN_VERSION {
        return Err(CliError::unsupported(format!(
            "GitLab {version} is below {}.{}, the oldest release the factory supports",
            MIN_VERSION.0, MIN_VERSION.1
        ))
        .with_fix("upgrade the GitLab server, or use a GitLab that runs 17.0 or later")
        .into());
    }

    // (b) The token: active, with both scopes, and its expiry.
    let reply = gitlab.get("/personal_access_tokens/self", &[]).await?;
    if reply.status != 200 {
        return Err(unexpected(
            reply.status,
            "GET /api/v4/personal_access_tokens/self",
        ));
    }
    let token_file = gitlab.token_file.display();
    let active = reply.body.get("active").and_then(Value::as_bool) == Some(true)
        && reply.body.get("revoked").and_then(Value::as_bool) != Some(true);
    if !active {
        return Err(CliError::usage(format!(
            "the token in --token-file {token_file} is inactive: GitLab reports it revoked or expired"
        ))
        .with_fix("create a new token with the api and write_repository scopes and rerun")
        .into());
    }
    let scopes: Vec<&str> = reply
        .body
        .get("scopes")
        .and_then(Value::as_array)
        .map(|list| list.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    let missing: Vec<&str> = SCOPES
        .into_iter()
        .filter(|scope| !scopes.contains(scope))
        .collect();
    if !missing.is_empty() {
        return Err(CliError::usage(format!(
            "the token in --token-file {token_file} is missing the {} scope{}; the factory needs api and write_repository",
            missing.join(" and "),
            if missing.len() == 1 { "" } else { "s" }
        ))
        .with_fix("create a token with the api and write_repository scopes and rerun")
        .into());
    }
    let expires_at = text(&reply.body, "expires_at").map(|date| date.chars().take(10).collect());

    // (c) The token's user.
    let reply = gitlab.get("/user", &[]).await?;
    if reply.status != 200 {
        return Err(unexpected(reply.status, "GET /api/v4/user"));
    }
    let user_id = reply.body.get("id").and_then(Value::as_u64);
    let username = text(&reply.body, "username").map(str::to_string);
    let (Some(user_id), Some(username)) = (user_id, username) else {
        return Err(
            CliError::failure("GitLab GET /api/v4/user named no user id and username").into(),
        );
    };

    // (d) The project, by path.
    let reply = gitlab
        .get(&format!("/projects/{}", encoded(repo)), &[])
        .await?;
    if reply.status == 404 {
        return Err(CliError::usage(format!(
            "GitLab project {repo} was not found (404), or user {username} cannot see it"
        ))
        .with_fix(
            "check --repo is the project path from its URL and that the token's user is a member",
        )
        .into());
    }
    if reply.status != 200 {
        return Err(unexpected(reply.status, &format!("project {repo}")));
    }
    let Some(project_id) = reply.body.get("id").and_then(Value::as_u64) else {
        return Err(CliError::failure(format!("GitLab project {repo} has no numeric id")).into());
    };
    let path = text(&reply.body, "path_with_namespace")
        .unwrap_or(repo)
        .to_string();
    let issues = reply.body.get("issues_enabled").and_then(Value::as_bool) != Some(false)
        && text(&reply.body, "issues_access_level") != Some("disabled");
    if !issues {
        return Err(CliError::usage(format!(
            "GitLab project {path} has issues disabled, and the factory takes its work from the project's issues"
        ))
        .with_fix("turn Issues on under the project's Settings > General > Visibility, project features, permissions and rerun")
        .into());
    }
    let default_branch = text(&reply.body, "default_branch").map(str::to_string);

    // (e) The user's effective access level, inherited membership included.
    let reply = gitlab
        .get(
            &format!("/projects/{project_id}/members/all/{user_id}"),
            &[],
        )
        .await?;
    let access_level = match reply.status {
        200 => reply
            .body
            .get("access_level")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        404 => 0,
        status => {
            return Err(unexpected(
                status,
                &format!("user {username}'s membership of {path}"),
            ))
        }
    };
    if access_level < MIN_ACCESS_LEVEL {
        return Err(CliError::usage(format!(
            "GitLab user {username} has access level {access_level} ({}) on {path}; the factory needs Developer (30) or higher to push branches and open merge requests",
            role(access_level)
        ))
        .with_fix(format!("give {username} the Developer role on {path}, or use a Developer's token, and rerun"))
        .into());
    }

    Ok(Found {
        version,
        username,
        expires_at,
        project_id,
        path,
        default_branch,
        access_level,
    })
}

/// Step f: creates each of `labels` (name, `#rrggbb` color) the project lacks
/// and leaves the ones it has. Returns the names it created.
pub(super) async fn ensure_labels(
    gitlab: &Gitlab,
    project_id: u64,
    labels: &[(String, &str)],
) -> Result<Vec<String>> {
    let route = format!("/projects/{project_id}/labels");
    let mut held = Vec::new();
    let mut page = "1".to_string();
    loop {
        let reply = gitlab
            .get(&route, &[("per_page", "100"), ("page", &page)])
            .await?;
        if reply.status != 200 {
            return Err(unexpected(
                reply.status,
                &format!("the labels of project {project_id}"),
            ));
        }
        held.extend(
            reply
                .body
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|label| text(label, "name"))
                .map(str::to_string),
        );
        match reply.next_page {
            Some(next) => page = next,
            None => break,
        }
    }
    let mut created = Vec::new();
    for (name, color) in labels {
        if held.iter().any(|held| held == name) {
            continue;
        }
        let body = json!({
            "name": name,
            "color": color,
            "description": "Managed by the Curie dark factory",
        });
        let reply = gitlab
            .send(reqwest::Method::POST, &route, &[], Some(&body))
            .await?;
        match reply.status {
            200 | 201 => created.push(name.clone()),
            // Created since the listing: it exists, which is all step f wants.
            409 => {}
            403 => {
                return Err(CliError::usage(format!(
                    "GitLab refused to create label {name} on project {project_id} (403)"
                ))
                .with_fix("use the token of a Developer (or higher) on the project and rerun")
                .into())
            }
            status => return Err(unexpected(status, &format!("creating label {name}"))),
        }
    }
    Ok(created)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn major_minor_reads_the_leading_release() {
        assert_eq!(major_minor("19.4.1-ce.0"), Some((19, 4)));
        assert_eq!(major_minor("16.11.2"), Some((16, 11)));
        assert_eq!(major_minor("17.0.0-ee"), Some((17, 0)));
        assert_eq!(major_minor("unknown"), None);
        assert!((16, 11) < MIN_VERSION && (17, 0) >= MIN_VERSION);
    }

    #[test]
    fn a_nested_path_is_one_encoded_segment() {
        assert_eq!(
            encoded("acme/platform/widgets"),
            "acme%2Fplatform%2Fwidgets"
        );
    }
}

//! The Jira Cloud reads of the quickstart preflight (#4358), against the
//! documented REST v3 endpoints:
//! https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-server-info/#api-rest-api-3-serverinfo-get
//! https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-myself/#api-rest-api-3-myself-get
//! https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-users/#api-rest-api-3-user-get
//! https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-groups/#api-rest-api-3-group-member-get
//! and the site's public `/_edge/tenant_info` for the cloud id.
//!
//! The token travels only in the Basic Authorization header; no message
//! carries it, a response body, or the header.

use std::time::Duration;

use anyhow::Result;
use serde_json::Value;

use super::{
    binding_value, flags, read_token, secure_scheme, tracker_value, JiraPlan, StartAuthority,
    BINDING_NAME, SECRET_NAME,
};
use crate::exit::CliError;
use crate::factory_quickstart::{forge_egress_entries, EgressEntry, ForgeFlags};
use crate::factory_toolchain::AppPreflight;

struct Jira {
    client: reqwest::Client,
    api_url: String,
    username: String,
    token: String,
}

impl Jira {
    /// One GET. `auth` sends the Basic credential. Returns the status and the
    /// parsed body (Null when not JSON). A transport failure names the URL
    /// path only.
    async fn get(&self, url: &str, query: &[(&str, &str)], auth: bool) -> Result<(u16, Value)> {
        let mut request = self
            .client
            .get(url)
            .query(query)
            .header("Accept", "application/json")
            .header("User-Agent", format!("curie/{}", env!("CARGO_PKG_VERSION")));
        if auth {
            request = request.basic_auth(&self.username, Some(&self.token));
        }
        let shown = reqwest::Url::parse(url)
            .map(|url| url.path().to_string())
            .unwrap_or_default();
        let response = request.send().await.map_err(|error| {
            CliError::transient(format!(
                "Jira call GET {shown} failed: {}; check --forge-url",
                error.without_url()
            ))
        })?;
        let status = response.status().as_u16();
        let text = response.text().await.unwrap_or_default();
        Ok((status, serde_json::from_str(&text).unwrap_or(Value::Null)))
    }

    async fn rest(&self, path: &str, query: &[(&str, &str)]) -> Result<(u16, Value)> {
        let url = format!("{}{path}", self.api_url);
        let (status, body) = self.get(&url, query, true).await?;
        if status == 401 || status == 403 {
            return Err(CliError::usage(format!(
                "Jira refused the token in --token-file for --forge-username {} (HTTP {status} for GET {path})",
                self.username
            ))
            .with_fix(
                "pass the email of the account the API token belongs to and a current token for it",
            )
            .into());
        }
        Ok((status, body))
    }
}

fn unexpected(status: u16, what: &str) -> anyhow::Error {
    CliError::failure(format!(
        "Jira returned HTTP {status} for {what}; nothing was applied"
    ))
    .with_fix("check --forge-url names the Jira Cloud site or its API gateway and rerun")
    .into()
}

fn text<'a>(body: &'a Value, key: &str) -> Option<&'a str> {
    body.get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
}

/// `host[:port]` of a URL, the port only when it is explicit.
fn authority(url: &reqwest::Url) -> String {
    let host = url.host_str().unwrap_or_default();
    match url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_string(),
    }
}

fn resolve(host: &str) -> Result<Vec<std::net::IpAddr>> {
    use std::net::ToSocketAddrs;
    Ok((host, 0).to_socket_addrs()?.map(|addr| addr.ip()).collect())
}

/// One egress entry set per distinct host and port of `urls`.
fn egress(urls: &[&reqwest::Url]) -> Result<Vec<EgressEntry>> {
    let mut seen = Vec::new();
    let mut entries = Vec::new();
    for url in urls {
        let host = url
            .host_str()
            .unwrap_or_default()
            .trim_start_matches('[')
            .trim_end_matches(']')
            .to_string();
        let port = url.port_or_known_default().unwrap_or(443);
        if seen.contains(&(host.clone(), port)) {
            continue;
        }
        entries.extend(forge_egress_entries(&host, &[port], resolve)?);
        seen.push((host, port));
    }
    Ok(entries)
}

/// Validates the Jira flags and token against the site, then reads each
/// `--repo`'s GitHub id with `app`'s installation token. Runs after the
/// GitHub App preflight and before any cluster call.
pub async fn preflight(
    forge: &ForgeFlags,
    repos: &[String],
    app: &AppPreflight,
) -> Result<JiraPlan> {
    let flags = flags(forge)?;
    let token = read_token(&flags.token_file)?;
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(20))
        .build()
        .map_err(|err| CliError::failure(format!("could not build an HTTP client: {err}")))?;
    let jira = Jira {
        client,
        api_url: flags.url.api_url.clone(),
        username: flags.username.clone(),
        token,
    };

    let (status, info) = jira.rest("/rest/api/3/serverInfo", &[]).await?;
    if status != 200 {
        return Err(unexpected(status, "GET /rest/api/3/serverInfo"));
    }
    let site = text(&info, "baseUrl")
        .and_then(|raw| reqwest::Url::parse(raw).ok())
        .filter(|site| site.host_str().is_some() && secure_scheme(site))
        .ok_or_else(|| {
            CliError::failure(
                "Jira serverInfo carries no https baseUrl, so --forge-url is not a Jira Cloud site",
            )
            .with_fix("check --forge-url and rerun")
        })?;
    let origin = site.origin().ascii_serialization();

    let tenant_url = format!("{origin}/_edge/tenant_info");
    let (status, tenant) = jira.get(&tenant_url, &[], false).await?;
    if status != 200 {
        return Err(unexpected(status, "GET /_edge/tenant_info"));
    }
    let cloud_id = text(&tenant, "cloudId")
        .ok_or_else(|| CliError::failure("the Jira site's tenant_info carries no cloudId"))?
        .to_string();
    if let Some(named) = &flags.url.cloud_id {
        if *named != cloud_id {
            return Err(CliError::usage(format!(
                "--forge-url names cloud id {named}, but the site {} reports cloud id {cloud_id}",
                authority(&site)
            ))
            .with_fix(format!(
                "pass --forge-url https://api.atlassian.com/ex/jira/{cloud_id}, or the site root"
            ))
            .into());
        }
    }

    let (status, me) = jira.rest("/rest/api/3/myself", &[]).await?;
    if status != 200 {
        return Err(unexpected(status, "GET /rest/api/3/myself"));
    }
    let mention = text(&me, "accountId")
        .ok_or_else(|| CliError::failure("Jira myself carries no accountId"))?
        .to_string();

    match &flags.start {
        StartAuthority::Accounts(accounts) => {
            for account in accounts {
                let (status, user) = jira
                    .rest("/rest/api/3/user", &[("accountId", account.as_str())])
                    .await?;
                if status != 200 {
                    return Err(CliError::usage(format!(
                        "--start-account {account}: Jira returned HTTP {status} for the account"
                    ))
                    .with_fix("pass the accountId of a person the factory account can see")
                    .into());
                }
                let kind = text(&user, "accountType").unwrap_or("unknown");
                if kind != "atlassian" {
                    return Err(CliError::usage(format!(
                        "--start-account {account} is a {kind} account; only a person's atlassian account starts work"
                    ))
                    .with_fix("pass the accountId of a person, not an app or customer account")
                    .into());
                }
            }
        }
        StartAuthority::Group(group) => {
            let (status, _) = jira
                .rest(
                    "/rest/api/3/group/member",
                    &[("groupId", group.as_str()), ("maxResults", "1")],
                )
                .await?;
            if status != 200 {
                return Err(CliError::usage(format!(
                    "--start-group {group}: Jira returned HTTP {status} for the group"
                ))
                .with_fix("pass the groupId of a group the factory account can read")
                .into());
            }
        }
    }

    let mut ids = Vec::with_capacity(repos.len());
    for repo in repos {
        let id = app.api.repository_id(&app.app, repo).await?;
        ids.push((repo.clone(), id.to_string()));
    }

    let egress = egress(&[&site, &flags.url.parsed])?;
    Ok(JiraPlan {
        tracker: tracker_value(
            &authority(&site),
            &flags.url.api_url,
            &flags.username,
            &mention,
            &egress,
        ),
        binding: binding_value(&cloud_id, &ids, &flags.start),
        egress,
        secret_name: SECRET_NAME.to_string(),
        token_file: flags.token_file,
        binding_name: BINDING_NAME.to_string(),
    })
}

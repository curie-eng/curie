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
    binding_value, flags, read_token, secure_scheme, tracker_value, JiraFlags, JiraPlan,
    StartAuthority, BINDING_NAME, SECRET_NAME,
};
use crate::exit::CliError;
use crate::factory_quickstart::{forge_egress_entries, resolve_host, EgressEntry, ForgeFlags};
use crate::factory_toolchain::AppPreflight;

struct Jira {
    client: reqwest::Client,
    api_url: String,
    username: String,
    token: String,
    url_flag: &'static str,
    token_flag: &'static str,
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
                "Jira call GET {shown} failed: {}; check {}",
                error.without_url(),
                self.url_flag
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
                "Jira refused the token in {} for --forge-username {} (HTTP {status} for GET {path})",
                self.token_flag, self.username
            ))
            .with_fix(
                "pass the email of the account the API token belongs to and a current token for it",
            )
            .into());
        }
        Ok((status, body))
    }
}

fn unexpected(status: u16, what: &str, url_flag: &str) -> anyhow::Error {
    CliError::failure(format!(
        "Jira returned HTTP {status} for {what}; nothing was applied"
    ))
    .with_fix(format!(
        "check {url_flag} names the Jira Cloud site or its API gateway and rerun"
    ))
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
        entries.extend(forge_egress_entries(&host, &[port], resolve_host)?);
        seen.push((host, port));
    }
    Ok(entries)
}

/// What the site preflight learned: everything the tracker entry and a
/// binding need except the repositories.
#[derive(Debug, Clone)]
pub(in crate::factory_quickstart) struct JiraSite {
    pub flags: JiraFlags,
    /// `host[:port]` of the site.
    pub host: String,
    pub cloud_id: String,
    /// The factory account's id.
    pub mention: String,
    pub egress: Vec<EgressEntry>,
}

impl JiraSite {
    /// The `factory.trackers[]` entry.
    pub fn tracker(&self) -> Value {
        tracker_value(
            &self.host,
            &self.flags.url.api_url,
            &self.flags.username,
            &self.mention,
            &self.egress,
        )
    }
}

/// Validates the Jira flags and token against the site, then reads each
/// `--repo`'s GitHub id with `app`'s installation token. Runs after the
/// GitHub App preflight and before any cluster call.
pub async fn preflight(
    forge: &ForgeFlags,
    repos: &[String],
    app: &AppPreflight,
) -> Result<JiraPlan> {
    let site = preflight_site(flags(forge)?).await?;
    let mut ids = Vec::with_capacity(repos.len());
    for repo in repos {
        let id = app.api.repository_id(&app.app, repo).await?;
        ids.push((repo.clone(), id.to_string()));
    }
    Ok(JiraPlan {
        tracker: site.tracker(),
        binding: binding_value(&site.cloud_id, "github", &ids, &site.flags.start),
        egress: site.egress,
        secret_name: SECRET_NAME.to_string(),
        token_file: site.flags.token_file,
        binding_name: BINDING_NAME.to_string(),
    })
}

/// Validates the Jira flags and token against the site: its base URL and
/// cloud id, the factory account, and who may start work. Reads only.
pub(in crate::factory_quickstart) async fn preflight_site(flags: JiraFlags) -> Result<JiraSite> {
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
        url_flag: flags.url_flag,
        token_flag: flags.token_flag,
    };

    let (status, info) = jira.rest("/rest/api/3/serverInfo", &[]).await?;
    if status != 200 {
        return Err(unexpected(
            status,
            "GET /rest/api/3/serverInfo",
            flags.url_flag,
        ));
    }
    let site = text(&info, "baseUrl")
        .and_then(|raw| reqwest::Url::parse(raw).ok())
        .filter(|site| site.host_str().is_some() && secure_scheme(site))
        .ok_or_else(|| {
            CliError::failure(format!(
                "Jira serverInfo carries no https baseUrl, so {} is not a Jira Cloud site",
                flags.url_flag
            ))
            .with_fix(format!("check {} and rerun", flags.url_flag))
        })?;
    let origin = site.origin().ascii_serialization();

    let tenant_url = format!("{origin}/_edge/tenant_info");
    let (status, tenant) = jira.get(&tenant_url, &[], false).await?;
    if status != 200 {
        return Err(unexpected(status, "GET /_edge/tenant_info", flags.url_flag));
    }
    let cloud_id = text(&tenant, "cloudId")
        .ok_or_else(|| CliError::failure("the Jira site's tenant_info carries no cloudId"))?
        .to_string();
    if let Some(named) = &flags.url.cloud_id {
        if *named != cloud_id {
            return Err(CliError::usage(format!(
                "{} names cloud id {named}, but the site {} reports cloud id {cloud_id}",
                flags.url_flag,
                authority(&site)
            ))
            .with_fix(format!(
                "pass {} https://api.atlassian.com/ex/jira/{cloud_id}, or the site root",
                flags.url_flag
            ))
            .into());
        }
    }

    let (status, me) = jira.rest("/rest/api/3/myself", &[]).await?;
    if status != 200 {
        return Err(unexpected(status, "GET /rest/api/3/myself", flags.url_flag));
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

    let egress = egress(&[&site, &flags.url.parsed])?;
    Ok(JiraSite {
        host: authority(&site),
        cloud_id,
        mention,
        egress,
        flags,
    })
}

//! The Jira Cloud tracker for `curie factory quickstart` (#4358).
//!
//! Jira Cloud pairs with the GitHub code host. The GitHub flow runs as before
//! and this module adds to it: [`check`] refuses unusable flags before any
//! read, [`preflight`] validates the token, site, cloud id, start authority
//! and the bound repositories' ids, and its [`JiraPlan`] becomes one
//! `JiraTracker` action that writes the `factory.trackers` and
//! `factory.bindings` entries and the token Secret, while the agent binds
//! `jira=<binding>` instead of one `github=<repo>` per repository.
//!
//! The token is read from `--token-file` and goes only into the Secret and the
//! Authorization header of the preflight reads. It never reaches values, the
//! plan, argv, or any message.

mod api;

pub use api::preflight;

use std::path::{Path, PathBuf};

use anyhow::Result;
use serde_json::{json, Value};

use super::{EgressEntry, ForgeFlags};
use crate::exit::CliError;

/// The name of the tracker entry and the binding this module writes.
pub const BINDING_NAME: &str = "jira";
/// The Secret holding the Jira API token, and its data key.
pub const SECRET_NAME: &str = "curie-jira-token";
pub const SECRET_KEY: &str = "token";

/// What the Jira half of a quickstart writes. Carries no token: the token is
/// read again from `token_file` when the Secret is applied.
#[derive(Debug, Clone)]
pub struct JiraPlan {
    /// The `factory.trackers[]` entry.
    pub tracker: Value,
    /// The `factory.bindings[]` entry.
    pub binding: Value,
    /// The tracker's egress, also carried inside `tracker`.
    pub egress: Vec<EgressEntry>,
    pub secret_name: String,
    pub token_file: PathBuf,
    pub binding_name: String,
}

/// The two `--forge-url` forms: a site root, or the scoped API gateway that
/// names the cloud id.
#[derive(Debug, Clone)]
pub(super) struct JiraUrl {
    /// The URL REST calls go to, without a trailing slash.
    pub api_url: String,
    pub parsed: reqwest::Url,
    /// The cloud id a gateway URL names; `None` for a site root.
    pub cloud_id: Option<String>,
}

/// Who may start work: accounts or one group, never both.
#[derive(Debug, Clone)]
pub(super) enum StartAuthority {
    Accounts(Vec<String>),
    Group(String),
}

impl StartAuthority {
    fn to_json(&self) -> Value {
        match self {
            Self::Accounts(accounts) => json!({"type": "allowlist", "accounts": accounts}),
            Self::Group(group) => json!({"type": "group", "groupId": group}),
        }
    }
}

/// The Jira flags, checked and parsed.
#[derive(Debug, Clone)]
pub(super) struct JiraFlags {
    pub url: JiraUrl,
    pub token_file: PathBuf,
    pub username: String,
    pub start: StartAuthority,
}

fn usage(message: impl Into<String>, fix: impl Into<String>) -> anyhow::Error {
    CliError::usage(message.into()).with_fix(fix.into()).into()
}

const URL_FORMS: &str =
    "pass the site root https://<site>.atlassian.net or the scoped https://api.atlassian.com/ex/jira/<cloudId>";

fn is_loopback(url: &reqwest::Url) -> bool {
    let Some(host) = url.host_str() else {
        return false;
    };
    host.eq_ignore_ascii_case("localhost")
        || host
            .trim_start_matches('[')
            .trim_end_matches(']')
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

/// A Jira URL must be https, or plain http on this machine (a local stand-in).
pub(super) fn secure_scheme(url: &reqwest::Url) -> bool {
    url.scheme() == "https" || (url.scheme() == "http" && is_loopback(url))
}

fn parse_url(raw: &str) -> Result<JiraUrl> {
    let parsed = reqwest::Url::parse(raw).map_err(|error| {
        usage(
            format!("--forge-url {raw:?} is not a URL: {error}"),
            URL_FORMS,
        )
    })?;
    if !secure_scheme(&parsed) {
        return Err(usage(
            format!("--forge-url {raw} must use https; plain http is accepted only for localhost"),
            URL_FORMS,
        ));
    }
    if parsed.host_str().is_none_or(str::is_empty) {
        return Err(usage(format!("--forge-url {raw} names no host"), URL_FORMS));
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(usage(
            "--forge-url must not carry credentials; the token is read from --token-file",
            "drop the user and password from --forge-url",
        ));
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        return Err(usage(
            format!("--forge-url {raw} must not carry a query or fragment"),
            URL_FORMS,
        ));
    }
    let path = parsed.path().trim_end_matches('/');
    let cloud_id = if path.is_empty() {
        None
    } else if path == "/ex/jira" || path.starts_with("/ex/jira/") {
        let id = path.trim_start_matches("/ex/jira").trim_start_matches('/');
        if id.is_empty() || id.contains('/') {
            return Err(usage(
                format!("--forge-url {raw} is the Jira API gateway but names no single cloud id"),
                "pass https://api.atlassian.com/ex/jira/<cloudId>, the cloud id from https://<site>.atlassian.net/_edge/tenant_info",
            ));
        }
        Some(id.to_string())
    } else {
        return Err(usage(
            format!("--forge-url {raw} is neither a Jira site root nor the API gateway"),
            URL_FORMS,
        ));
    };
    Ok(JiraUrl {
        api_url: format!("{}{path}", parsed.origin().ascii_serialization()),
        parsed,
        cloud_id,
    })
}

/// Checks and parses the Jira flags. Pure: reads no file and no network.
pub(super) fn flags(forge: &ForgeFlags) -> Result<JiraFlags> {
    let raw = forge.forge_url.as_deref().ok_or_else(|| {
        usage(
            "--tracker jira-cloud needs --forge-url, the Jira Cloud site or API gateway URL",
            URL_FORMS,
        )
    })?;
    let url = parse_url(raw)?;
    let token_file = forge.token_file.clone().ok_or_else(|| {
        usage(
            "--tracker jira-cloud needs --token-file, a file holding the Jira API token",
            "create an API token for the factory account at https://id.atlassian.com/manage-profile/security/api-tokens, write it to a file, and pass --token-file <PATH>",
        )
    })?;
    let username = match forge.forge_username.as_deref().map(str::trim) {
        None | Some("") => {
            return Err(usage(
                "--tracker jira-cloud needs --forge-username, the email of the account the token belongs to",
                "pass --forge-username <EMAIL>",
            ))
        }
        Some(name) => name,
    };
    let at = username.find('@');
    if !at.is_some_and(|at| at > 0 && at + 1 < username.len()) {
        return Err(usage(
            format!("--forge-username {username:?} is not an email; Jira Cloud authenticates the token with the account's email"),
            "pass --forge-username <EMAIL>",
        ));
    }
    let start_fix =
        "pass --start-account <accountId> (repeatable) or --start-group <groupId>, not both";
    let start = match (&forge.start_accounts[..], forge.start_group.as_deref()) {
        ([], None) => {
            return Err(usage(
                "--tracker jira-cloud needs who may start work: --start-account or --start-group",
                start_fix,
            ))
        }
        ([_, ..], Some(_)) => {
            return Err(usage(
                "--start-account and --start-group are exclusive; pass one of them",
                start_fix,
            ))
        }
        ([], Some(group)) if group.trim().is_empty() => {
            return Err(usage(
                "--start-group is empty; --start-account or --start-group names who may start work",
                start_fix,
            ))
        }
        ([], Some(group)) => StartAuthority::Group(group.trim().to_string()),
        (accounts, None) => {
            if accounts.iter().any(|account| account.trim().is_empty()) {
                return Err(usage(
                    "--start-account is empty; --start-account or --start-group names who may start work",
                    start_fix,
                ));
            }
            let mut unique: Vec<String> = Vec::new();
            for account in accounts {
                let account = account.trim().to_string();
                if !unique.contains(&account) {
                    unique.push(account);
                }
            }
            StartAuthority::Accounts(unique)
        }
    };
    if !forge.review_accounts.is_empty() {
        return Err(usage(
            "--review-account does not apply to --tracker jira-cloud; reviews are counted on the GitHub code host, which reports write access",
            "drop --review-account",
        ));
    }
    if forge.ca_file.is_some() {
        return Err(usage(
            "--ca-file does not apply to --tracker jira-cloud; Jira Cloud serves a public certificate",
            "drop --ca-file",
        ));
    }
    Ok(JiraFlags {
        url,
        token_file,
        username: username.to_string(),
        start,
    })
}

/// Refuses a Jira flag set this build cannot run, before any read.
pub(super) fn check(forge: &ForgeFlags) -> Result<()> {
    flags(forge).map(|_| ())
}

/// The refusal for the Jira tracker with a non-GitHub code host: this build's
/// Jira quickstart binds GitHub repositories only, and the combined Bitbucket
/// quickstart is #4400.
pub(super) fn code_host_not_installed(code_host: &str) -> anyhow::Error {
    CliError::unsupported(format!(
        "--tracker jira-cloud with --code-host {code_host} is not installed in this build"
    ))
    .with_fix("rerun with --code-host github, or write the factory values by hand as docs/guides/dark-factory-quickstart.md shows")
    .into()
}

/// The binding alias for each repository: its lowercased name, or
/// `owner-name` when two repositories share a name, at most 64 characters.
fn aliases(repos: &[String]) -> Vec<String> {
    let name = |repo: &str| repo.rsplit('/').next().unwrap_or(repo).to_ascii_lowercase();
    repos
        .iter()
        .map(|repo| {
            let short = name(repo);
            let shared = repos.iter().filter(|other| name(other) == short).count() > 1;
            let alias = if shared {
                repo.to_ascii_lowercase().replace('/', "-")
            } else {
                short
            };
            alias.chars().take(64).collect()
        })
        .collect()
}

/// The `factory.bindings[]` entry for `repos` and their GitHub ids.
pub(super) fn binding_value(
    cloud_id: &str,
    repos: &[(String, String)],
    start: &StartAuthority,
) -> Value {
    let names: Vec<String> = repos.iter().map(|(repo, _)| repo.clone()).collect();
    let aliases = aliases(&names);
    let entries: Vec<Value> = repos
        .iter()
        .zip(&aliases)
        .map(|((repo, id), alias)| {
            json!({"alias": alias, "codeHost": "github", "projectId": id, "path": repo})
        })
        .collect();
    json!({
        "name": BINDING_NAME,
        "tracker": BINDING_NAME,
        "trackerScopeId": cloud_id,
        "repos": entries,
        "defaultRepo": aliases.first(),
        "startAuthority": start.to_json(),
    })
}

/// The `factory.trackers[]` entry. `mention` is the bot's account id.
pub(super) fn tracker_value(
    host: &str,
    api_url: &str,
    username: &str,
    mention: &str,
    egress: &[EgressEntry],
) -> Value {
    json!({
        "name": BINDING_NAME,
        "kind": "jira_cloud",
        "host": host,
        "apiUrl": api_url,
        "label": crate::factory_app::DEFAULT_LABEL,
        "mention": mention,
        "credential": {
            "type": "static_token",
            "username": username,
            "existingSecret": SECRET_NAME,
            "existingSecretKey": SECRET_KEY,
        },
        "egress": egress,
    })
}

/// A `--dry-run` stand-in for the plan: what can be known without a read,
/// plus the lines describing the reads [`preflight`] would make.
pub(super) fn described(forge: &ForgeFlags, repos: &[String]) -> Result<(JiraPlan, Vec<String>)> {
    let flags = flags(forge)?;
    let api = &flags.url.api_url;
    let mut reads = vec![
        format!(
            "# read the Jira API token from --token-file {} (never printed)",
            flags.token_file.display()
        ),
        format!(
            "# GET {api}/rest/api/3/serverInfo as {} for the site",
            flags.username
        ),
        match &flags.url.cloud_id {
            Some(id) => format!("# GET <site>/_edge/tenant_info and require cloud id {id}"),
            None => "# GET <site>/_edge/tenant_info for the cloud id".to_string(),
        },
        format!("# GET {api}/rest/api/3/myself for the factory account id"),
    ];
    match &flags.start {
        StartAuthority::Accounts(accounts) => reads.extend(accounts.iter().map(|account| {
            format!(
                "# GET {api}/rest/api/3/user?accountId={account} and require an atlassian account"
            )
        })),
        StartAuthority::Group(group) => reads.push(format!(
            "# GET {api}/rest/api/3/group/member?groupId={group}&maxResults=1"
        )),
    }
    let github = crate::github_app::github_api_url(crate::github_app::DEFAULT_CLONE_BASE);
    reads.extend(repos.iter().map(|repo| {
        format!("# GET {github}/repos/{repo} with the App installation token for its id")
    }));
    let ids: Vec<(String, String)> = repos
        .iter()
        .map(|repo| (repo.clone(), "<repository id>".to_string()))
        .collect();
    let cloud_id = flags.url.cloud_id.as_deref().unwrap_or("<cloud id>");
    let plan = JiraPlan {
        tracker: tracker_value("<site>", api, &flags.username, "<factory account id>", &[]),
        binding: binding_value(cloud_id, &ids, &flags.start),
        egress: Vec::new(),
        secret_name: SECRET_NAME.to_string(),
        token_file: flags.token_file,
        binding_name: BINDING_NAME.to_string(),
    };
    Ok((plan, reads))
}

/// The token in `path`, trimmed. Refused when unreadable or empty; the
/// message names the flag and the path, never the contents.
pub(crate) fn read_token(path: &Path) -> Result<String> {
    let raw = std::fs::read_to_string(path).map_err(|error| {
        usage(
            format!("cannot read --token-file {}: {error}", path.display()),
            "pass a readable file holding the Jira API token",
        )
    })?;
    let token = raw.trim();
    if token.is_empty() {
        return Err(usage(
            format!("--token-file {} is empty", path.display()),
            "write the Jira API token to the file and rerun",
        ));
    }
    Ok(token.to_string())
}

/// The Opaque Secret holding the Jira token under [`SECRET_KEY`].
pub(crate) fn secret_manifest(namespace: &str, token: &str) -> Value {
    use base64::Engine;
    json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "type": "Opaque",
        "metadata": {"name": SECRET_NAME, "namespace": namespace},
        "data": {SECRET_KEY: base64::engine::general_purpose::STANDARD.encode(token.as_bytes())},
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aliases_use_the_name_and_qualify_a_shared_one() {
        let repos = [
            "acme/Widgets".to_string(),
            "acme/api".into(),
            "ops/api".into(),
        ];
        assert_eq!(aliases(&repos), ["widgets", "acme-api", "ops-api"]);
    }

    #[test]
    fn a_gateway_url_carries_its_cloud_id_and_a_site_root_none() {
        let gateway = parse_url("https://api.atlassian.com/ex/jira/abc-123/").unwrap();
        assert_eq!(gateway.cloud_id.as_deref(), Some("abc-123"));
        assert_eq!(gateway.api_url, "https://api.atlassian.com/ex/jira/abc-123");
        let site = parse_url("https://acme.atlassian.net/").unwrap();
        assert_eq!(site.cloud_id, None);
        assert_eq!(site.api_url, "https://acme.atlassian.net");
        assert!(parse_url("https://acme.atlassian.net/jira/software").is_err());
        assert!(parse_url("https://user:pw@acme.atlassian.net").is_err());
    }

    #[test]
    fn the_secret_manifest_carries_the_token_only_base64_encoded() {
        let manifest = secret_manifest("curie", "secret-token-value");
        assert_eq!(manifest["metadata"]["name"], SECRET_NAME);
        assert!(!manifest.to_string().contains("secret-token-value"));
    }
}

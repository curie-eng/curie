//! The Jira Cloud tracker for `curie factory quickstart` (#4358).
//!
//! Jira Cloud pairs with the GitHub code host or a Bitbucket code host. Beside
//! Bitbucket, the combined run in `jira_bitbucket` reads the site through
//! [`preflight_site`] with the Jira URL and token from `--tracker-url` and
//! `--tracker-token-file`. Beside GitHub, the GitHub flow runs as before and
//! this module adds to it: [`check`] refuses unusable flags before any
//! read, [`preflight`] validates the token, site, cloud id, start authority
//! and the bound repositories' ids, and its [`JiraPlan`] becomes one
//! `JiraTracker` action that writes the `factory.trackers` and
//! `factory.bindings` entries and the token Secret, while the agent binds
//! `jira=<binding>` instead of one `github=<repo>` per repository.
//!
//! A site is a tracker instance: its identity is (`jira_cloud`, site host)
//! (#4620). [`resolve_jira_site`] finds a recorded site by that identity, whatever its
//! name, and keeps its entry, binding and credential Secret as recorded;
//! [`verify_secret`] checks that Secret holds this run's token. A new site
//! gets derived names (`super::forge_entry_name`) and its own Secret.
//!
//! The token is read from `--token-file` and goes only into the Secret and the
//! Authorization header of the preflight reads. It never reaches values, the
//! plan, argv, or any message.

mod api;
mod recorded;

pub use api::preflight;
pub(super) use api::{preflight_site, JiraSite};
pub(super) use recorded::check_recorded;

use std::path::{Path, PathBuf};

use anyhow::Result;
use serde_json::{json, Value};

use super::instance::{self, ObjectWrite, Observation, SecretUse};
use super::{CodeHostKind, EgressEntry, ForgeFlags};
use crate::exit::CliError;
use crate::ops::CommonOpts;

/// The tracker kind this module writes.
pub const TRACKER_KIND: &str = "jira_cloud";

/// What the Jira half of a quickstart writes. Carries no token: the token is
/// read again from `token_file` when the Secret is created.
#[derive(Debug, Clone)]
pub struct JiraPlan {
    /// The `factory.trackers[]` entry this run would write. For a recorded
    /// site it carries the recorded name, credential Secret and label, so it
    /// compares field by field with `recorded_tracker`.
    pub tracker: Value,
    /// The `factory.bindings[]` entry, likewise.
    pub binding: Value,
    /// The tracker's egress, also carried inside `tracker`.
    pub egress: Vec<EgressEntry>,
    /// The Secret and data key the tracker credential names.
    pub secret_name: String,
    pub secret_key: String,
    pub token_file: PathBuf,
    pub binding_name: String,
    /// The tracker and binding the release records for this site and scope,
    /// found by identity and written back as recorded.
    pub recorded_tracker: Option<Value>,
    pub recorded_binding: Option<Value>,
    /// Whether the run creates the Secret or reuses it as it is.
    pub secret: SecretUse,
    /// What the run read before its first write, read again before it.
    pub observation: Observation,
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
    /// The flags the URL and token came from, for messages: `--forge-url`
    /// and `--token-file` beside GitHub, `--tracker-url` and
    /// `--tracker-token-file` beside Bitbucket.
    pub url_flag: &'static str,
    pub token_flag: &'static str,
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

fn parse_url(raw: &str, flag: &str, token_flag: &str) -> Result<JiraUrl> {
    let parsed = reqwest::Url::parse(raw)
        .map_err(|error| usage(format!("{flag} {raw:?} is not a URL: {error}"), URL_FORMS))?;
    if !secure_scheme(&parsed) {
        return Err(usage(
            format!("{flag} {raw} must use https; plain http is accepted only for localhost"),
            URL_FORMS,
        ));
    }
    if parsed.host_str().is_none_or(str::is_empty) {
        return Err(usage(format!("{flag} {raw} names no host"), URL_FORMS));
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(usage(
            format!("{flag} must not carry credentials; the token is read from {token_flag}"),
            format!("drop the user and password from {flag}"),
        ));
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        return Err(usage(
            format!("{flag} {raw} must not carry a query or fragment"),
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
                format!("{flag} {raw} is the Jira API gateway but names no single cloud id"),
                "pass https://api.atlassian.com/ex/jira/<cloudId>, the cloud id from https://<site>.atlassian.net/_edge/tenant_info",
            ));
        }
        Some(id.to_string())
    } else {
        return Err(usage(
            format!("{flag} {raw} is neither a Jira site root nor the API gateway"),
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
/// Beside GitHub the Jira URL and token are `--forge-url` and `--token-file`;
/// beside Bitbucket, whose own they are, `--tracker-url` and
/// `--tracker-token-file`.
pub(super) fn flags(forge: &ForgeFlags) -> Result<JiraFlags> {
    let beside_github = forge.code_host == CodeHostKind::Github;
    if beside_github {
        let bitbucket_only = [
            ("--tracker-url", forge.tracker_url.is_some()),
            ("--tracker-token-file", forge.tracker_token_file.is_some()),
        ];
        if let Some((flag, _)) = bitbucket_only.iter().find(|(_, set)| *set) {
            return Err(usage(
                format!("{flag} applies only beside a Bitbucket code host; with --code-host github the Jira URL and token are --forge-url and --token-file"),
                format!("drop {flag}"),
            ));
        }
    }
    let (url_flag, token_flag, raw, token_file) = if beside_github {
        (
            "--forge-url",
            "--token-file",
            forge.forge_url.as_deref(),
            forge.token_file.clone(),
        )
    } else {
        (
            "--tracker-url",
            "--tracker-token-file",
            forge.tracker_url.as_deref(),
            forge.tracker_token_file.clone(),
        )
    };
    let raw = raw.ok_or_else(|| {
        usage(
            format!(
                "--tracker jira-cloud needs {url_flag}, the Jira Cloud site or API gateway URL"
            ),
            URL_FORMS,
        )
    })?;
    let url = parse_url(raw, url_flag, token_flag)?;
    let token_file = token_file.ok_or_else(|| {
        usage(
            format!("--tracker jira-cloud needs {token_flag}, a file holding the Jira API token"),
            format!("create an API token for the factory account at https://id.atlassian.com/manage-profile/security/api-tokens, write it to a file, and pass {token_flag} <PATH>"),
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
    if beside_github {
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
    }
    Ok(JiraFlags {
        url,
        token_file,
        username: username.to_string(),
        start,
        url_flag,
        token_flag,
    })
}

/// Refuses a Jira flag set this build cannot run, before any read.
pub(super) fn check(forge: &ForgeFlags) -> Result<()> {
    flags(forge).map(|_| ())
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

/// The `factory.bindings[]` entry of `tracker` for `repos` and their ids on
/// `code_host`, under its derived name.
pub(super) fn binding_value(
    tracker: &str,
    cloud_id: &str,
    code_host: &str,
    repos: &[(String, String)],
    start: &StartAuthority,
) -> Value {
    let names: Vec<String> = repos.iter().map(|(repo, _)| repo.clone()).collect();
    let aliases = aliases(&names);
    let entries: Vec<Value> = repos
        .iter()
        .zip(&aliases)
        .map(|((repo, id), alias)| {
            json!({"alias": alias, "codeHost": code_host, "projectId": id, "path": repo})
        })
        .collect();
    json!({
        "name": super::forge_binding_name(tracker, cloud_id),
        "tracker": tracker,
        "trackerScopeId": cloud_id,
        "repos": entries,
        "defaultRepo": aliases.first(),
        "startAuthority": start.to_json(),
    })
}

/// The `factory.trackers[]` entry of a new site, under its derived name with
/// the default label and the release's Secret for it. `mention` is the bot's
/// account id.
pub(super) fn tracker_value(
    release: &str,
    host: &str,
    api_url: &str,
    username: &str,
    mention: &str,
    egress: &[EgressEntry],
) -> Value {
    let name = super::forge_entry_name(TRACKER_KIND, api_url);
    json!({
        "name": name,
        "kind": TRACKER_KIND,
        "host": host,
        "apiUrl": api_url,
        "label": crate::factory_app::DEFAULT_LABEL,
        "mention": mention,
        "credential": {
            "type": "static_token",
            "username": username,
            "existingSecret": super::forge_secret_name(release, &name),
            "existingSecretKey": instance::TOKEN_KEY,
        },
        "egress": egress,
    })
}

/// The plan for a new site's `tracker` and `binding`, before [`resolve_jira_site`].
pub(super) fn new_jira_plan(
    tracker: Value,
    binding: Value,
    egress: Vec<EgressEntry>,
    token_file: PathBuf,
) -> JiraPlan {
    let (secret_name, secret_key) = instance::credential_ref(&tracker);
    let binding_name = binding["name"].as_str().unwrap_or_default().to_string();
    JiraPlan {
        tracker,
        binding,
        egress,
        secret_name,
        secret_key,
        token_file,
        binding_name,
        recorded_tracker: None,
        recorded_binding: None,
        secret: SecretUse::Create,
        observation: Observation::default(),
    }
}

/// Resolves `plan` against the release's recorded `values` (#4620). The
/// recorded tracker with this site's identity, whatever its name, is kept:
/// the plan takes its name, credential Secret and key, and label, and keeps
/// it as `recorded_tracker`; one on this host under another API base URL
/// (a site root against its API gateway) refuses. The binding is found by
/// (tracker, cloud id) likewise. A new site or binding keeps its derived
/// name, refused when another identity holds it. A `--dry-run` plan (see
/// [`described`]) whose site host is still a placeholder has no identity to
/// find or to conflict with, so it is described as planned and never
/// refused. Pure.
pub(super) fn resolve_jira_site(
    mut plan: JiraPlan,
    values: &Value,
    url_flag: &str,
    release: &str,
) -> Result<JiraPlan> {
    let host = plan.tracker["host"].as_str().unwrap_or_default();
    if host.starts_with('<') && host.ends_with('>') {
        return Ok(plan);
    }
    let tracker =
        instance::resolve_instance(values, "trackers", plan.tracker.clone(), url_flag, release)?;
    match tracker.recorded() {
        Some(recorded) => {
            let mut planned = plan.tracker.clone();
            planned["name"] = recorded["name"].clone();
            let (secret, key) = instance::credential_ref(recorded);
            instance::set_credential_ref(&mut planned, &secret, &key);
            match (recorded.get("label"), planned.as_object_mut()) {
                (Some(label), _) => planned["label"] = label.clone(),
                (None, Some(map)) => {
                    map.remove("label");
                }
                (None, None) => {}
            }
            plan.tracker = planned;
            plan.recorded_tracker = Some(recorded.clone());
        }
        None => plan.tracker = tracker.into_entry(),
    }
    (plan.secret_name, plan.secret_key) = instance::credential_ref(&plan.tracker);
    plan.binding["tracker"] = plan.tracker["name"].clone();
    let binding = instance::resolve_binding(values, plan.binding.clone(), release)?;
    match binding.recorded() {
        Some(recorded) => {
            plan.binding["name"] = recorded["name"].clone();
            plan.recorded_binding = Some(recorded.clone());
        }
        None => plan.binding = binding.into_entry(),
    }
    // A `--dry-run` that read no cloud id keeps its placeholder in the name.
    let scope = instance::scope_text(&plan.binding);
    if scope.starts_with('<') && scope.ends_with('>') {
        let tracker = plan.tracker["name"].as_str().unwrap_or_default();
        plan.binding["name"] = json!(format!("{tracker}-{scope}"));
    }
    plan.binding_name = plan.binding["name"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    Ok(plan)
}

/// Points every repository of a Jira binding beside GitHub at the
/// release's GitHub code host (#4620): the recorded entry of that identity
/// (kind `github` and its host) under its recorded name, whatever it is, or
/// the entry the GitHub intake plans when none is recorded. Run before
/// [`resolve_jira_site`], so an equivalent recorded binding compares equal
/// and every refusal stays before any write.
pub(super) fn bind_github_code_host(mut plan: JiraPlan, values: &Value) -> JiraPlan {
    let code_host = crate::factory_intake::github_code_host_name(values);
    if let Some(repos) = plan.binding.get_mut("repos").and_then(Value::as_array_mut) {
        for repo in repos {
            repo["codeHost"] = json!(code_host);
        }
    }
    plan
}

/// Checks the Secret the plan's tracker names against the token in
/// `token_file` (#4620): a recorded site's Secret must hold it and is never
/// rewritten; a new site's Secret is created, or reused when this release's
/// quickstart left it with the same token. What it read is added to
/// `observation`, which becomes the plan's.
pub(super) async fn verify_secret(
    mut plan: JiraPlan,
    common: &CommonOpts,
    token_flag: &str,
    mut observation: Observation,
) -> Result<JiraPlan> {
    let token = read_token(&plan.token_file)?;
    let entry = plan.tracker["name"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    plan.secret = if plan.recorded_tracker.is_some() {
        observation.push(
            instance::verify_recorded_secret(
                common,
                &entry,
                &plan.secret_name,
                &plan.secret_key,
                &token,
                token_flag,
            )
            .await?,
        );
        SecretUse::Reuse
    } else {
        let (use_, read) = instance::plan_new_secret(
            &common.namespace,
            &common.release,
            &plan.secret_name,
            &plan.secret_key,
            &token,
            token_flag,
        )
        .await?;
        observation.push(read);
        use_
    };
    plan.observation = observation;
    Ok(plan)
}

/// A `--dry-run` stand-in for the plan: what can be known without a read,
/// plus the lines describing the reads [`preflight`] would make.
pub(super) fn described(
    forge: &ForgeFlags,
    repos: &[String],
    release: &str,
) -> Result<(JiraPlan, Vec<String>)> {
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
    let tracker = tracker_value(
        release,
        "<site>",
        api,
        &flags.username,
        "<factory account id>",
        &[],
    );
    let name = tracker["name"].as_str().unwrap_or_default().to_string();
    let cloud_id = flags.url.cloud_id.as_deref().unwrap_or("<cloud id>");
    let mut binding = binding_value(
        &name,
        cloud_id,
        crate::factory_intake::GITHUB_ENTRY,
        &ids,
        &flags.start,
    );
    if flags.url.cloud_id.is_none() {
        binding["name"] = json!(format!("{name}-{cloud_id}"));
    }
    Ok((
        new_jira_plan(tracker, binding, Vec::new(), flags.token_file),
        reads,
    ))
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

/// The Secret the plan creates, `None` when it reuses one. The token is read
/// from its file here and goes only into the manifest.
pub(super) fn secret_write(plan: &JiraPlan, common: &CommonOpts) -> Result<Option<ObjectWrite>> {
    if plan.secret == SecretUse::Reuse {
        return Ok(None);
    }
    let token = read_token(&plan.token_file)?;
    Ok(Some(ObjectWrite::create(instance::token_secret(
        &common.namespace,
        &common.release,
        &plan.secret_name,
        &plan.secret_key,
        &token,
    ))))
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
        let gateway = parse_url(
            "https://api.atlassian.com/ex/jira/abc-123/",
            "--forge-url",
            "--token-file",
        )
        .unwrap();
        assert_eq!(gateway.cloud_id.as_deref(), Some("abc-123"));
        assert_eq!(gateway.api_url, "https://api.atlassian.com/ex/jira/abc-123");
        let site = parse_url("https://acme.atlassian.net/", "--forge-url", "--token-file").unwrap();
        assert_eq!(site.cloud_id, None);
        assert_eq!(site.api_url, "https://acme.atlassian.net");
        assert!(parse_url(
            "https://acme.atlassian.net/jira/software",
            "--forge-url",
            "--token-file"
        )
        .is_err());
        assert!(parse_url(
            "https://user:pw@acme.atlassian.net",
            "--forge-url",
            "--token-file"
        )
        .is_err());
    }

    fn recorded_jira_site_fixture(name: &str, api: &str) -> Value {
        json!({"name": name, "kind": TRACKER_KIND, "host": "acme.atlassian.net", "apiUrl": api,
               "label": "operator-label", "mention": "bot",
               "credential": {"type": "static_token", "username": "bot@example.com",
                              "existingSecret": "curie-jira-token", "existingSecretKey": "operator-key"}})
    }

    fn planned_jira_site_fixture() -> JiraPlan {
        let tracker = tracker_value(
            "acme",
            "acme.atlassian.net",
            "https://acme.atlassian.net",
            "bot@example.com",
            "bot",
            &[],
        );
        let name = tracker["name"].as_str().unwrap().to_string();
        let start = StartAuthority::Accounts(vec!["starter".into()]);
        let binding = binding_value(
            &name,
            "cloud-1",
            crate::factory_intake::GITHUB_ENTRY,
            &[("acme/widgets".into(), "7".into())],
            &start,
        );
        new_jira_plan(tracker, binding, Vec::new(), PathBuf::from("/never-read"))
    }

    #[test]
    fn a_new_site_takes_derived_names() {
        let plan = resolve_jira_site(
            planned_jira_site_fixture(),
            &Value::Null,
            "--forge-url",
            "acme",
        )
        .unwrap();
        let name = super::super::forge_entry_name(TRACKER_KIND, "https://acme.atlassian.net");
        assert_eq!(plan.tracker["name"], json!(name));
        assert_eq!(
            plan.secret_name,
            super::super::forge_secret_name("acme", &name)
        );
        assert_eq!(plan.binding_name, format!("{name}-cloud-1"));
        assert_eq!(plan.binding["tracker"], json!(name));
        assert!(plan.recorded_tracker.is_none() && plan.recorded_binding.is_none());
    }

    #[test]
    fn a_recorded_site_is_found_by_identity_and_keeps_its_names() {
        let recorded =
            recorded_jira_site_fixture("operator-jira-site", "https://acme.atlassian.net/");
        let binding = json!({"name": "operator-jira-binding", "tracker": "operator-jira-site", "trackerScopeId": "cloud-1"});
        let values = json!({"factory": {"trackers": [recorded], "bindings": [binding]}});
        let plan =
            resolve_jira_site(planned_jira_site_fixture(), &values, "--forge-url", "acme").unwrap();
        assert_eq!(plan.tracker["name"], json!("operator-jira-site"));
        assert_eq!(plan.tracker["label"], json!("operator-label"));
        assert_eq!(
            (plan.secret_name.as_str(), plan.secret_key.as_str()),
            ("curie-jira-token", "operator-key")
        );
        assert_eq!(plan.binding_name, "operator-jira-binding");
        assert_eq!(plan.recorded_tracker, Some(recorded));
        assert_eq!(plan.recorded_binding, Some(binding));
    }

    fn binding_code_hosts(plan: &JiraPlan) -> Vec<Value> {
        plan.binding["repos"]
            .as_array()
            .unwrap()
            .iter()
            .map(|repo| repo["codeHost"].clone())
            .collect()
    }

    #[test]
    fn a_jira_binding_names_the_recorded_github_code_host_and_reruns_equivalently() {
        let github = json!({"name": "operator-github", "kind": "github", "host": "github.com",
                            "apiUrl": "https://api.github.com"});
        let values = json!({"factory": {"codeHosts": [github]}});
        assert_eq!(
            crate::factory_intake::github_code_host_name(&values),
            "operator-github"
        );
        // A new site binds its repositories to the recorded GitHub entry.
        let first = resolve_jira_site(
            bind_github_code_host(planned_jira_site_fixture(), &values),
            &values,
            "--forge-url",
            "acme",
        )
        .unwrap();
        assert!(first.recorded_binding.is_none());
        assert_eq!(binding_code_hosts(&first), [json!("operator-github")]);
        // A rerun against what the first run wrote is equivalent: the
        // recorded binding is found and the recorded guard passes.
        let written = json!({"factory": {
            "codeHosts": [values["factory"]["codeHosts"][0].clone()],
            "trackers": [first.tracker.clone()],
            "bindings": [first.binding.clone()],
        }});
        let rerun = resolve_jira_site(
            bind_github_code_host(planned_jira_site_fixture(), &written),
            &written,
            "--forge-url",
            "acme",
        )
        .unwrap();
        assert_eq!(rerun.recorded_binding.as_ref(), Some(&first.binding));
        assert_eq!(binding_code_hosts(&rerun), [json!("operator-github")]);
        check_recorded(&rerun, "acme").unwrap();
        // The planned name in place of the recorded one would be refused.
        let unbound =
            resolve_jira_site(planned_jira_site_fixture(), &written, "--forge-url", "acme")
                .unwrap();
        let text = format!("{:#}", check_recorded(&unbound, "acme").unwrap_err());
        assert!(text.contains("repos"), "{text}");
    }

    #[test]
    fn without_a_recorded_github_code_host_the_planned_entry_is_bound() {
        let gitlab = json!({"name": "operator-github", "kind": "gitlab", "host": "github.com"});
        let values = json!({"factory": {"codeHosts": [gitlab]}});
        let plan = bind_github_code_host(planned_jira_site_fixture(), &values);
        assert_eq!(
            binding_code_hosts(&plan),
            [json!(crate::factory_intake::GITHUB_ENTRY)]
        );
    }

    #[test]
    fn a_dry_run_against_a_release_recording_the_real_site_is_described_not_refused() {
        let forge = ForgeFlags {
            tracker: super::super::TrackerKind::JiraCloud,
            forge_url: Some("https://acme.atlassian.net".into()),
            forge_username: Some("bot@example.com".into()),
            token_file: Some(PathBuf::from("/never-read")),
            start_accounts: vec!["starter".into()],
            ..ForgeFlags::default()
        };
        let (planned, _) = described(&forge, &["acme/widgets".to_string()], "acme").unwrap();
        // The release records the real site under the name a new site at
        // this API URL would derive, so a placeholder identity would collide.
        let name = super::super::forge_entry_name(TRACKER_KIND, "https://acme.atlassian.net");
        let values = json!({"factory": {"trackers": [
            recorded_jira_site_fixture(&name, "https://acme.atlassian.net"),
        ]}});
        let described_tracker = planned.tracker.clone();
        let plan = resolve_jira_site(
            bind_github_code_host(planned, &values),
            &values,
            "--forge-url",
            "acme",
        )
        .expect("a dry run never refuses on its placeholder site");
        assert_eq!(plan.tracker, described_tracker);
        assert_eq!(plan.tracker["host"], json!("<site>"));
        assert!(plan.recorded_tracker.is_none());
    }

    #[test]
    fn a_gateway_url_for_a_recorded_site_root_is_an_identity_conflict() {
        let values = json!({"factory": {"trackers": [recorded_jira_site_fixture("jira", "https://acme.atlassian.net")]}});
        let mut plan = planned_jira_site_fixture();
        plan.tracker["apiUrl"] = json!("https://acme.atlassian.net/ex/jira/cloud-1");
        let text = format!(
            "{:#}",
            resolve_jira_site(plan, &values, "--forge-url", "acme").unwrap_err()
        );
        assert!(
            text.contains("api identity") && text.contains("--forge-url"),
            "{text}"
        );
    }
}

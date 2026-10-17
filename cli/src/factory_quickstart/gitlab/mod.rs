//! GitLab as both the code host and the tracker of `curie factory quickstart`
//! (#4399 decision 23).
//!
//! [`check`] refuses an unusable flag set before any read. The run then reads
//! the server version, the token, its user, the project and the user's access
//! level on it (steps a to e, also under `--dry-run`), checks the release's
//! recorded instances, Secrets and CA bundle, ensures the trigger and state
//! labels (step f), and writes the GitLab code host, tracker and binding
//! entries, the token Secret, the CA bundle and the forge egress before
//! deploying the agent bound through `gitlab=<path>`.
//!
//! A GitLab server is one instance, identified by its host authority
//! (#4620). A recorded instance is found by that identity, whatever its
//! entry names, and kept as recorded; a new one gets names derived from its
//! API base URL. A second server therefore coexists with the first, and a
//! conflicting credential or URL for a recorded one refuses before any
//! write.
//!
//! The token is read from `--token-file`. It goes only into the
//! `PRIVATE-TOKEN` header of the preflight requests and into the Secret the
//! entries name; never into values, the plan, argv or a message.

mod api;
mod plan;

pub(super) use plan::quickstart;

use anyhow::Result;
use serde_json::{json, Value};

use super::instance::{self, Resolved};
use super::{CodeHostKind, EgressEntry, ForgeFlags, TrackerKind};
use crate::exit::CliError;
use crate::factory_intake::NativeForgeEntries;

/// The web base an absent `--forge-url` means.
pub const GITLAB_DEFAULT_URL: &str = "https://gitlab.com";
/// The kind of the code host and tracker entries this module writes.
const KIND: &str = "gitlab";
/// The state labels the factory moves on an issue, with their colors.
const STATE_LABELS: [(&str, &str); 4] = [
    ("curie-factory:queued", "#fbca04"),
    ("curie-factory:running", "#1d76db"),
    ("curie-factory:pr-open", "#0e8a16"),
    ("curie-factory:needs-human", "#d93f0b"),
];
/// The trigger label's color, the one the GitHub quickstart uses.
const TRIGGER_LABEL_COLOR: &str = "#5319e7";

fn usage(message: impl Into<String>, fix: impl Into<String>) -> anyhow::Error {
    CliError::usage(message.into()).with_fix(fix.into()).into()
}

/// Refuses a GitLab flag set this build cannot run. Pure: it reads no file,
/// network or cluster. GitLab pairs only with itself.
pub(super) fn check(forge: &ForgeFlags) -> Result<()> {
    if forge.code_host != CodeHostKind::Gitlab {
        return Err(usage(
            "tracker 'gitlab' pairs only with --code-host gitlab",
            "pass --code-host gitlab --tracker gitlab",
        ));
    }
    if forge.tracker != TrackerKind::Gitlab {
        return Err(usage(
            "code host 'gitlab' pairs only with --tracker gitlab",
            "pass --code-host gitlab --tracker gitlab",
        ));
    }
    if forge.token_file.is_none() {
        return Err(usage(
            "--code-host gitlab needs --token-file, a file holding a GitLab token with the api and write_repository scopes",
            "write the token of a Developer (or higher) user on the project to a file and pass --token-file <PATH>",
        ));
    }
    gitlab_web_base(forge)?;
    let other = [
        ("--review-account", !forge.review_accounts.is_empty()),
        ("--forge-username", forge.forge_username.is_some()),
        ("--start-account", !forge.start_accounts.is_empty()),
        ("--start-group", forge.start_group.is_some()),
        ("--tracker-url", forge.tracker_url.is_some()),
        ("--tracker-token-file", forge.tracker_token_file.is_some()),
    ];
    if let Some((flag, _)) = other.iter().find(|(_, set)| *set) {
        return Err(usage(
            format!("{flag} does not apply to --code-host gitlab; GitLab reports who may write to the project"),
            format!("drop {flag}"),
        ));
    }
    Ok(())
}

/// The GitLab web base from `--forge-url`, without a trailing `/`:
/// [`GITLAB_DEFAULT_URL`] when the flag is absent. It must be https, or plain
/// http on `localhost` or `127.0.0.1`, and carry no credentials, query or
/// fragment. A path is kept, for an install under a sub-path. Every refusal
/// is a fixed diagnostic: the raw flag may carry userinfo or a query token,
/// so it is never echoed.
pub fn gitlab_web_base(forge: &ForgeFlags) -> Result<String> {
    let raw = forge.forge_url.as_deref().unwrap_or(GITLAB_DEFAULT_URL);
    let forms = "pass the GitLab web address, for example https://gitlab.com or https://gitlab.example.com/gitlab";
    let url = reqwest::Url::parse(raw)
        .map_err(|error| usage(format!("--forge-url is not a URL: {error}"), forms))?;
    let local = matches!(url.host_str(), Some("localhost" | "127.0.0.1"));
    if !(url.scheme() == "https" || (url.scheme() == "http" && local)) {
        return Err(usage(
            "--forge-url must use https; plain http is accepted only for localhost and 127.0.0.1",
            forms,
        ));
    }
    if url.host_str().is_none_or(str::is_empty) {
        return Err(usage("--forge-url names no host", forms));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(usage(
            "--forge-url must not carry credentials; the token is read from --token-file",
            "drop the user and password from --forge-url",
        ));
    }
    if url.query().is_some() || url.fragment().is_some() {
        return Err(usage(
            "--forge-url must not carry a query or fragment",
            forms,
        ));
    }
    Ok(format!(
        "{}{}",
        url.origin().ascii_serialization(),
        url.path().trim_end_matches('/')
    ))
}

/// The one GitLab project `--repo` names: `<group>/[<subgroup>/...]<project>`.
fn project_path(repos: &[String]) -> Result<String> {
    let shape = "<group>/<project>, with any subgroups between";
    let [repo] = repos else {
        return Err(usage(
            "--code-host gitlab serves one project; pass --repo once",
            format!("pass --repo {shape}"),
        ));
    };
    let segment = |part: &str| {
        !part.is_empty()
            && part
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
    };
    let parts: Vec<&str> = repo.split('/').collect();
    if parts.len() < 2 || !parts.iter().all(|part| segment(part)) {
        return Err(usage(
            format!("--repo {repo:?} is not a GitLab project path; expected {shape}"),
            format!("pass --repo {shape}, as in the project URL"),
        ));
    }
    Ok(repo.clone())
}

/// What the preflight learned, and what the entries are written from.
#[derive(Debug, Clone)]
pub struct GitlabFacts {
    /// The release the entries and their Secret are named for.
    pub release: String,
    /// The web base, as [`gitlab_web_base`] returns it.
    pub web_base: String,
    /// The token user's username, the tracker's mention.
    pub username: String,
    /// The token's expiry date, `YYYY-MM-DD`; `None` when it has none.
    pub expires_at: Option<String>,
    pub project_id: u64,
    /// The project's path with its namespace.
    pub path: String,
    /// The tracker label of a new instance.
    pub label: String,
    pub egress: Vec<EgressEntry>,
    /// The CA bundle ConfigMap this run creates, when the release references
    /// none yet.
    pub ca_config_map: Option<String>,
}

/// `host[:port]` of a web base (scheme stripped, path dropped), lowercased.
fn authority(web_base: &str) -> String {
    let rest = web_base
        .split_once("://")
        .map_or(web_base, |(_, rest)| rest);
    rest.split('/').next().unwrap_or(rest).to_ascii_lowercase()
}

/// The binding alias: the project's own path segment, lowercased, without
/// leading or trailing punctuation, at most 64 characters.
fn alias(path: &str) -> String {
    let name = path.rsplit('/').next().unwrap_or(path).to_ascii_lowercase();
    let trim = |s: &str| {
        s.trim_matches(|c: char| !c.is_ascii_alphanumeric())
            .to_string()
    };
    trim(&trim(&name).chars().take(64).collect::<String>())
}

/// The code host and tracker entries and the binding for `facts` as a new
/// instance: the entries named by [`super::forge_entry_name`] of the API
/// base URL, their credential the release's Secret for that name, and the
/// binding `<entry>-<project id>`. The tracker is the code host entry with
/// the label and mention added: one token serves both. The project id is
/// both the repository's `projectId` and the binding's `trackerScopeId`.
pub fn gitlab_entries(facts: &GitlabFacts) -> NativeForgeEntries {
    let api_url = format!("{}/api/v4", facts.web_base);
    let name = super::forge_entry_name(KIND, &api_url);
    let mut credential = json!({
        "type": "static_token",
        "existingSecret": super::forge_secret_name(&facts.release, &name),
        "existingSecretKey": instance::TOKEN_KEY,
    });
    match &facts.expires_at {
        Some(date) => credential["expiresAt"] = json!(format!("{date}T00:00:00Z")),
        None => credential["neverExpires"] = json!(true),
    }
    let code_host = json!({
        "name": name,
        "kind": KIND,
        "host": authority(&facts.web_base),
        "apiUrl": api_url,
        "credential": credential,
        "egress": facts.egress,
    });
    let mut tracker = code_host.clone();
    tracker["label"] = json!(facts.label);
    tracker["mention"] = json!(facts.username);
    let id = facts.project_id.to_string();
    let binding = json!({
        "name": super::forge_binding_name(&name, &id),
        "tracker": name,
        "trackerScopeId": id,
        "repos": [{
            "alias": alias(&facts.path),
            "codeHost": name,
            "projectId": id,
            "path": facts.path,
        }],
    });
    NativeForgeEntries {
        code_host,
        tracker,
        binding,
        ca_config_map: facts.ca_config_map.clone(),
    }
}

/// One credential Secret the resolved entries name, and whether a recorded
/// entry names it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct CredentialRef {
    entry: String,
    secret: String,
    key: String,
    recorded: bool,
}

/// `planned` (from [`gitlab_entries`]) resolved against the release.
#[derive(Debug, Clone, PartialEq)]
struct Resolution {
    entries: NativeForgeEntries,
    /// The tracker label: a recorded tracker's own, else the default.
    label: String,
    credentials: Vec<CredentialRef>,
    /// Each entry is the release's own, kept as recorded.
    recorded: [bool; 3],
}

/// Resolves the planned entries against the release's recorded `values`
/// (#4620): the code host and tracker by (gitlab, host), kept as recorded,
/// or refused when the recorded API base URL is another; the binding by
/// (tracker, project id). A new code host or tracker beside a recorded one
/// names the recorded one's Secret, since one token serves both. Pure.
fn resolve_gitlab_instance(
    values: &Value,
    planned: NativeForgeEntries,
    release: &str,
) -> Result<Resolution> {
    let flag = "--forge-url";
    let code_host =
        instance::resolve_instance(values, "codeHosts", planned.code_host, flag, release)?;
    let tracker = instance::resolve_instance(values, "trackers", planned.tracker, flag, release)?;
    let shared = code_host
        .recorded()
        .or(tracker.recorded())
        .map(instance::credential_ref);
    let entry = |resolved: &Resolved| {
        let mut entry = resolved.entry().clone();
        if let (None, Some((secret, key))) = (resolved.recorded(), &shared) {
            instance::set_credential_ref(&mut entry, secret, key);
        }
        entry
    };
    let (code_host_entry, tracker_entry) = (entry(&code_host), entry(&tracker));
    let mut binding = planned.binding;
    binding["tracker"] = tracker_entry["name"].clone();
    binding["repos"][0]["codeHost"] = code_host_entry["name"].clone();
    let binding = instance::resolve_binding(values, binding, release)?;
    let mut credentials: Vec<CredentialRef> = Vec::new();
    for entry in [&code_host_entry, &tracker_entry] {
        // With a recorded entry every credential is a recorded one: its own,
        // or the one the new sibling took from it.
        let (secret, key) = instance::credential_ref(entry);
        let credential = CredentialRef {
            entry: entry["name"].as_str().unwrap_or_default().to_string(),
            secret,
            key,
            recorded: shared.is_some(),
        };
        if !credentials
            .iter()
            .any(|seen| seen.secret == credential.secret && seen.key == credential.key)
        {
            credentials.push(credential);
        }
    }
    let label = tracker_entry
        .get("label")
        .and_then(Value::as_str)
        .filter(|_| tracker.recorded().is_some())
        .unwrap_or(crate::factory_app::DEFAULT_LABEL)
        .to_string();
    let recorded = [
        code_host.recorded().is_some(),
        tracker.recorded().is_some(),
        binding.recorded().is_some(),
    ];
    Ok(Resolution {
        entries: NativeForgeEntries {
            code_host: code_host_entry,
            tracker: tracker_entry,
            binding: binding.into_entry(),
            ca_config_map: planned.ca_config_map,
        },
        label,
        credentials,
        recorded,
    })
}

/// The egress entries for the server at `web_base`: the addresses its host
/// resolves to now, or the address itself, on the URL's port.
fn web_egress(web_base: &str) -> Result<Vec<EgressEntry>> {
    let url = reqwest::Url::parse(web_base)
        .map_err(|error| CliError::failure(format!("{web_base} is not a URL: {error}")))?;
    let port = url.port_or_known_default().unwrap_or(443);
    let host = url
        .host_str()
        .unwrap_or_default()
        .trim_start_matches('[')
        .trim_end_matches(']');
    super::forge_egress_entries(host, &[port], super::resolve_host)
}

/// The labels step f ensures: the trigger label, then each state label.
fn wanted_labels(trigger: &str) -> Vec<(String, &'static str)> {
    std::iter::once((trigger.to_string(), TRIGGER_LABEL_COLOR))
        .chain(
            STATE_LABELS
                .iter()
                .map(|(name, color)| (name.to_string(), *color)),
        )
        .collect()
}

/// `egress` as plan text: `cidr TCP port[,port]` per entry.
fn egress_prose(egress: &[EgressEntry]) -> String {
    egress
        .iter()
        .map(|entry| {
            let ports: Vec<String> = entry
                .ports
                .iter()
                .map(|port| format!("{} {}", port.protocol, port.port))
                .collect();
            format!("{} {}", entry.cidr, ports.join(","))
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// `Value` of a JSON field as text, for messages.
fn text<'a>(body: &'a Value, key: &str) -> Option<&'a str> {
    body.get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn flags(code_host: CodeHostKind, tracker: TrackerKind) -> ForgeFlags {
        ForgeFlags {
            code_host,
            tracker,
            token_file: Some("gitlab-token-that-is-never-read".into()),
            ..ForgeFlags::default()
        }
    }

    #[test]
    fn check_refuses_a_partner_other_than_gitlab_naming_the_pairing() {
        for (code_host, tracker) in [
            (CodeHostKind::Gitlab, TrackerKind::Github),
            (CodeHostKind::Gitlab, TrackerKind::JiraCloud),
            (CodeHostKind::Github, TrackerKind::Gitlab),
        ] {
            let text = format!("{:#}", check(&flags(code_host, tracker)).unwrap_err());
            assert!(text.contains("pairs only with"), "{text}");
            assert!(!text.contains("not installed"), "{text}");
        }
        check(&flags(CodeHostKind::Gitlab, TrackerKind::Gitlab)).expect("gitlab pairs with gitlab");
    }

    #[test]
    fn check_refuses_the_jira_and_bitbucket_flags() {
        let forge = ForgeFlags {
            start_group: Some("group-1".into()),
            ..flags(CodeHostKind::Gitlab, TrackerKind::Gitlab)
        };
        let text = format!("{:#}", check(&forge).unwrap_err());
        assert!(text.contains("--start-group"), "{text}");
    }

    #[test]
    fn project_path_needs_one_path_of_two_or_more_segments() {
        assert_eq!(
            project_path(&["acme/platform/widgets".into()]).unwrap(),
            "acme/platform/widgets"
        );
        for repos in [
            vec!["widgets".to_string()],
            vec!["acme/".into()],
            vec!["acme//widgets".into()],
            vec!["acme/wid gets".into()],
            vec!["acme/a".into(), "acme/b".into()],
        ] {
            let text = format!("{:#}", project_path(&repos).unwrap_err());
            assert!(text.contains("--repo"), "{repos:?}: {text}");
        }
    }

    #[test]
    fn authority_and_alias() {
        assert_eq!(
            authority("https://GitLab.example.test:8443/gitlab"),
            "gitlab.example.test:8443"
        );
        assert_eq!(authority("https://gitlab.com"), "gitlab.com");
        assert_eq!(alias("acme/platform/Widgets.v2"), "widgets.v2");
        assert_eq!(alias("acme/_internal_"), "internal");
    }

    fn gitlab_fixture_facts(web_base: &str, project_id: u64) -> GitlabFacts {
        GitlabFacts {
            release: "acme".into(),
            web_base: web_base.into(),
            username: "bot".into(),
            expires_at: None,
            project_id,
            path: "acme/widgets".into(),
            label: crate::factory_app::DEFAULT_LABEL.into(),
            egress: Vec::new(),
            ca_config_map: None,
        }
    }

    #[test]
    fn a_second_server_resolves_to_new_entries_beside_the_first() {
        let first = gitlab_entries(&gitlab_fixture_facts("https://a.example.test", 41));
        let values = json!({"factory": {
            "codeHosts": [first.code_host],
            "trackers": [first.tracker],
            "bindings": [first.binding],
        }});
        let second = resolve_gitlab_instance(
            &values,
            gitlab_entries(&gitlab_fixture_facts("https://b.example.test", 41)),
            "acme",
        )
        .unwrap();
        assert_eq!(second.recorded, [false, false, false]);
        assert_ne!(second.entries.code_host["name"], first.code_host["name"]);
        assert_ne!(second.entries.binding["name"], first.binding["name"]);
        assert_eq!(second.credentials.len(), 1);
        assert!(!second.credentials[0].recorded);
        let again = resolve_gitlab_instance(
            &values,
            gitlab_entries(&gitlab_fixture_facts("https://a.example.test", 41)),
            "acme",
        )
        .unwrap();
        assert_eq!(again.recorded, [true, true, true]);
        assert_eq!(again.entries.tracker, first.tracker);
        assert!(again.credentials[0].recorded);
    }

    #[test]
    fn a_new_tracker_beside_a_recorded_code_host_shares_its_secret() {
        let recorded = json!({"name": "operator-code-host", "kind": "gitlab", "host": "a.example.test",
            "apiUrl": "https://a.example.test/api/v4",
            "credential": {"existingSecret": "operator-forge-credential", "existingSecretKey": "operator-key"}});
        let values = json!({"factory": {"codeHosts": [recorded]}});
        let resolved = resolve_gitlab_instance(
            &values,
            gitlab_entries(&gitlab_fixture_facts("https://a.example.test", 41)),
            "acme",
        )
        .unwrap();
        assert_eq!(resolved.recorded, [true, false, false]);
        assert_eq!(
            instance::credential_ref(&resolved.entries.tracker),
            ("operator-forge-credential".into(), "operator-key".into())
        );
        assert_eq!(resolved.credentials.len(), 1);
        assert!(resolved.credentials[0].recorded);
        assert_eq!(
            resolved.entries.binding["repos"][0]["codeHost"],
            json!("operator-code-host")
        );
        assert_eq!(resolved.label, crate::factory_app::DEFAULT_LABEL);
    }

    #[test]
    fn a_recorded_tracker_keeps_its_label() {
        let first = gitlab_entries(&gitlab_fixture_facts("https://a.example.test", 41));
        let mut tracker = first.tracker.clone();
        tracker["label"] = json!("operator-default");
        let values = json!({"factory": {"trackers": [tracker]}});
        let resolved = resolve_gitlab_instance(
            &values,
            gitlab_entries(&gitlab_fixture_facts("https://a.example.test", 42)),
            "acme",
        )
        .unwrap();
        assert_eq!(resolved.label, "operator-default");
    }

    #[test]
    fn wanted_labels_lead_with_the_trigger() {
        let labels = wanted_labels("factory-fast");
        assert_eq!(labels[0].0, "factory-fast");
        assert_eq!(labels.len(), 5);
        assert!(labels
            .iter()
            .all(|(_, color)| color.len() == 7 && color.starts_with('#')));
    }
}

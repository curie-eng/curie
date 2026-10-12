//! The Bitbucket Cloud code host half of `curie factory quickstart` (#4401). It
//! validates the shared forge flags, parses `--repo`, probes the access token by
//! reading the repository, and emits the `codeHosts` entry, the repository
//! binding, the review allowlist and the egress CIDRs Atlassian publishes. The
//! Jira tracker half and the combined run are #4358 and #4400.

use std::path::Path;
use std::time::Duration;

use anyhow::Result;
use serde_json::{json, Value};

use super::{EgressEntry, EgressPort, ForgeFlags, TrackerKind};
use crate::exit::CliError;

/// The Bitbucket Cloud REST API every Cloud workspace is served from.
const CLOUD_API: &str = "https://api.bitbucket.org/2.0";
/// The Atlassian document listing every published IP range.
const IP_RANGES_URL: &str = "https://ip-ranges.atlassian.com/";
const REPO_SHAPE: &str = "<workspace>/<repo slug>";

/// Refuses Bitbucket Cloud flags this build cannot run. Pure: it reads no file
/// or network. Bitbucket pairs only with the Jira Cloud tracker, so a valid
/// flag set hands over to that half.
pub(super) fn check(forge: &ForgeFlags) -> Result<()> {
    if forge.tracker != TrackerKind::JiraCloud {
        return Err(CliError::unsupported(
            "code host 'bitbucket-cloud' pairs only with --tracker jira-cloud; forge kind 'jira-cloud' is not installed in this build",
        )
        .with_fix("rerun with --code-host github, or wait for the jira-cloud tracker")
        .into());
    }
    if forge.forge_url.is_some() {
        return Err(CliError::usage(format!(
            "--forge-url does not apply to bitbucket-cloud; its API is {CLOUD_API}"
        ))
        .with_fix("remove --forge-url")
        .into());
    }
    if forge.token_file.is_none() {
        return Err(CliError::usage(
            "--code-host bitbucket-cloud needs --token-file, a repository access token with repository:write and pullrequest:write",
        )
        .with_fix("pass --token-file <path>")
        .into());
    }
    if forge.review_accounts.is_empty() {
        return Err(CliError::usage(
            "--code-host bitbucket-cloud needs at least one --review-account, the Bitbucket account id whose review comments the factory acts on",
        )
        .with_fix("pass --review-account <account id>")
        .into());
    }
    super::jira_cloud::check(forge)
}

/// A Bitbucket Cloud repository named by `--repo`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code, reason = "called by the combined Jira quickstart in #4400")]
pub(super) struct RepoPath {
    pub workspace: String,
    pub slug: String,
}

/// What the token probe learned about the repository.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code, reason = "called by the combined Jira quickstart in #4400")]
pub(super) struct Probed {
    /// The repository UUID, braces included.
    pub project_id: String,
    /// `<workspace>/<repo slug>`.
    pub path: String,
}

fn is_slug(segment: &str) -> bool {
    !segment.is_empty()
        && segment.bytes().all(|b| {
            b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'-' | b'_' | b'.')
        })
}

/// Parses `--repo` as `<workspace>/<repo slug>`.
#[allow(dead_code, reason = "called by the combined Jira quickstart in #4400")]
pub(super) fn parse_repo(repo: &str) -> Result<RepoPath> {
    match repo.split('/').collect::<Vec<_>>()[..] {
        [workspace, slug] if is_slug(workspace) && is_slug(slug) => Ok(RepoPath {
            workspace: workspace.to_string(),
            slug: slug.to_string(),
        }),
        _ => Err(CliError::usage(format!(
            "--repo {repo:?} is not a Bitbucket Cloud repository; expected {REPO_SHAPE} in lowercase letters, digits, '-', '_' and '.'"
        ))
        .with_fix(format!("pass --repo {REPO_SHAPE}, as in the repository URL"))
        .into()),
    }
}

/// Reads the access token from `token_file`, trimmed. The value is never
/// put into an error.
fn read_token(token_file: &Path) -> Result<String> {
    let raw = std::fs::read_to_string(token_file).map_err(|error| {
        CliError::failure(format!(
            "could not read the token file {}: {error}",
            token_file.display()
        ))
    })?;
    let token = raw.trim();
    if token.is_empty() {
        return Err(
            CliError::usage(format!("the token file {} is empty", token_file.display())).into(),
        );
    }
    Ok(token.to_string())
}

fn http_client(ca_file: Option<&Path>) -> Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(15));
    if let Some(ca_file) = ca_file {
        let pem = std::fs::read(ca_file).map_err(|error| {
            CliError::failure(format!(
                "could not read --ca-file {}: {error}",
                ca_file.display()
            ))
        })?;
        let certificate = reqwest::Certificate::from_pem(&pem).map_err(|error| {
            CliError::usage(format!(
                "--ca-file {} is not a PEM certificate: {error}",
                ca_file.display()
            ))
        })?;
        builder = builder.add_root_certificate(certificate);
    }
    builder.build().map_err(|error| {
        CliError::failure(format!("could not build an HTTP client: {error}")).into()
    })
}

/// Probes the token by reading the repository at `api_base`
/// (`https://api.bitbucket.org/2.0` outside tests). A 401 names the token
/// file and a 403 names the scopes the token lacks.
#[allow(dead_code, reason = "called by the combined Jira quickstart in #4400")]
pub(super) async fn probe(
    api_base: &str,
    token_file: &Path,
    ca_file: Option<&Path>,
    repo: &RepoPath,
) -> Result<Probed> {
    let token = read_token(token_file)?;
    let client = http_client(ca_file)?;
    let path = format!("{}/{}", repo.workspace, repo.slug);
    let url = format!("{}/repositories/{path}", api_base.trim_end_matches('/'));
    let response = client
        .get(&url)
        .bearer_auth(&token)
        .send()
        .await
        .map_err(|error| {
            CliError::failure(format!(
                "reading Bitbucket Cloud repository {path} failed: {error}"
            ))
        })?;
    let status = response.status().as_u16();
    match status {
        200 => {}
        401 => {
            return Err(CliError::failure(format!(
                "the token in {} was refused (401)",
                token_file.display()
            ))
            .with_fix("replace it with a current Bitbucket Cloud repository access token")
            .into())
        }
        403 => {
            return Err(CliError::failure(format!(
                "the token in {} cannot read {path} (403); it needs repository:write and pullrequest:write",
                token_file.display()
            ))
            .with_fix("create a repository access token with the repository:write and pullrequest:write scopes")
            .into())
        }
        404 => {
            return Err(CliError::failure(format!(
                "Bitbucket Cloud repository {path} was not found (404)"
            ))
            .with_fix("check --repo, and that the token belongs to this repository")
            .into())
        }
        _ => {
            return Err(CliError::failure(format!(
                "reading Bitbucket Cloud repository {path} answered HTTP {status}"
            ))
            .into())
        }
    }
    let body: Value = response.json().await.map_err(|error| {
        CliError::failure(format!(
            "Bitbucket Cloud repository {path} was not JSON: {error}"
        ))
    })?;
    let uuid = body["uuid"].as_str().ok_or_else(|| {
        CliError::failure(format!("Bitbucket Cloud repository {path} has no uuid"))
    })?;
    Ok(Probed {
        project_id: uuid.to_string(),
        path,
    })
}

/// The values fragment for one Bitbucket Cloud code host named `name`: the
/// `codeHosts` entry, the repository binding (alias = repository slug) and the
/// review allowlist under the code host's name.
#[allow(dead_code, reason = "called by the combined Jira quickstart in #4400")]
pub(super) fn values(
    name: &str,
    probed: &Probed,
    base: &str,
    host: &str,
    review_accounts: &[String],
    egress: &[EgressEntry],
) -> Value {
    let alias = probed.path.rsplit('/').next().unwrap_or(&probed.path);
    json!({
        "codeHosts": [{
            "name": name,
            "kind": "bitbucket_cloud",
            "host": host,
            "apiUrl": base,
            "credential": {
                "type": "static_token",
                "existingSecret": format!("curie-{name}-token"),
                "existingSecretKey": "token",
            },
            "egress": egress,
        }],
        "repo": {
            "alias": alias,
            "codeHost": name,
            "projectId": probed.project_id,
            "path": probed.path,
        },
        "reviewFeedbackAllowlist": { name: review_accounts },
    })
}

/// The egress entries for Bitbucket Cloud: every range in Atlassian's
/// published document whose `product` includes `bitbucket` and whose
/// `direction` includes `ingress`, on TCP 443.
#[allow(dead_code, reason = "called by the combined Jira quickstart in #4400")]
pub(super) fn cloud_egress(doc: &Value) -> Result<Vec<EgressEntry>> {
    let items = doc["items"].as_array().ok_or_else(|| {
        CliError::failure(format!(
            "the Atlassian IP range document from {IP_RANGES_URL} has no items"
        ))
    })?;
    let has = |item: &Value, field: &str, wanted: &str| {
        item[field]
            .as_array()
            .is_some_and(|values| values.iter().any(|value| value.as_str() == Some(wanted)))
    };
    Ok(items
        .iter()
        .filter(|item| has(item, "product", "bitbucket") && has(item, "direction", "ingress"))
        .filter_map(|item| item["cidr"].as_str())
        .map(|cidr| EgressEntry {
            cidr: cidr.to_string(),
            ports: vec![EgressPort {
                protocol: "TCP",
                port: 443,
            }],
            host: None,
        })
        .collect())
}

/// Fetches Atlassian's published IP range document.
#[allow(dead_code, reason = "called by the combined Jira quickstart in #4400")]
pub(super) async fn fetch_ip_ranges() -> Result<Value> {
    let response = http_client(None)?
        .get(IP_RANGES_URL)
        .send()
        .await
        .map_err(|error| CliError::failure(format!("fetching {IP_RANGES_URL} failed: {error}")))?;
    let status = response.status();
    if !status.is_success() {
        return Err(
            CliError::failure(format!("fetching {IP_RANGES_URL} answered HTTP {status}")).into(),
        );
    }
    response
        .json()
        .await
        .map_err(|error| CliError::failure(format!("{IP_RANGES_URL} was not JSON: {error}")).into())
}

/// The credential expiry line: a Bitbucket access token's expiry is not
/// readable with the token itself.
#[allow(dead_code, reason = "called by the combined Jira quickstart in #4400")]
pub(super) fn expiry_line(name: &str) -> String {
    format!("credential code host {name} (static_token): unknown")
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::exit::{CliError, ExitClass};
    use crate::factory_quickstart::{select_forge, CodeHostKind, TrackerKind};

    /// The access token the stub expects. Held in a const so the commit-time
    /// secret scan does not read an inline assignment as a real credential.
    const TOKEN: &str = "bitbucket-test-token";
    const CLOUD_API: &str = "https://api.bitbucket.org/2.0";
    const REPO_ROUTE: &str = "/repositories/example-workspace/example-repo";

    fn data(name: &str) -> String {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../apps/api/tests/forge_fakes/bitbucket_quickstart")
            .join(name);
        std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("missing {}: {error}", path.display()))
    }

    fn data_json(name: &str) -> serde_json::Value {
        serde_json::from_str(&data(name)).unwrap_or_else(|error| panic!("{name}: {error}"))
    }

    fn cli_error(error: &anyhow::Error) -> &CliError {
        error
            .downcast_ref::<CliError>()
            .unwrap_or_else(|| panic!("not a CliError: {error:#}"))
    }

    /// Flags the Bitbucket half accepts. The token file does not exist: the
    /// check is pure and must not read it.
    fn flags() -> ForgeFlags {
        ForgeFlags {
            code_host: CodeHostKind::BitbucketCloud,
            tracker: TrackerKind::JiraCloud,
            forge_url: None,
            token_file: Some(PathBuf::from("bitbucket-token-that-is-never-read")),
            ca_file: None,
            review_accounts: vec!["example-account-1".into()],
        }
    }

    /// The Bitbucket half accepted the flags and handed over to the Jira
    /// tracker half, which is still reserved until #4358.
    fn assert_reaches_the_jira_half(result: Result<()>, case: &str) {
        if let Err(error) = result {
            assert_eq!(
                error.to_string(),
                crate::factory_quickstart::forge_not_installed("jira-cloud").to_string(),
                "{case}: the Bitbucket half refused: {error:#}"
            );
        }
    }

    type Requests = Arc<Mutex<Vec<String>>>;

    /// A raw HTTP/1.1 stub on an ephemeral port (the accept-loop shape of
    /// `cli/src/message.rs`). It answers a request whose path, query
    /// stripped, is in `routes` with that status and body, anything else with
    /// 404, and records every request head. Returns the base URL.
    async fn stub(routes: Vec<(&'static str, u16, String)>) -> (String, Requests) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let requests: Requests = Arc::default();
        let seen = requests.clone();
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                let mut head = Vec::new();
                let mut buf = [0u8; 4096];
                while !head.windows(4).any(|w| w == b"\r\n\r\n") {
                    match sock.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => head.extend_from_slice(&buf[..n]),
                    }
                }
                let head = String::from_utf8_lossy(&head).into_owned();
                let target = head.split_whitespace().nth(1).unwrap_or("").to_string();
                let path = target.split('?').next().unwrap_or("").to_string();
                seen.lock().unwrap().push(head);
                let (status, body) = routes
                    .iter()
                    .find(|(route, _, _)| *route == path)
                    .map(|(_, status, body)| (*status, body.clone()))
                    .unwrap_or((404, r#"{"type": "error"}"#.to_string()));
                let reply = format!(
                    "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = sock.write_all(reply.as_bytes()).await;
                let _ = sock.shutdown().await;
            }
        });
        (base, requests)
    }

    fn paths(requests: &Requests) -> Vec<String> {
        requests
            .lock()
            .unwrap()
            .iter()
            .map(|head| head.split_whitespace().nth(1).unwrap_or("").to_string())
            .collect()
    }

    fn authorization(head: &str) -> Option<String> {
        head.lines().find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("authorization")
                .then(|| value.trim().to_string())
        })
    }

    /// A token file holding [`TOKEN`] with the whitespace an editor leaves.
    fn token_file() -> tempfile::NamedTempFile {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), format!("  {TOKEN}\n\n")).unwrap();
        file
    }

    fn repo() -> RepoPath {
        parse_repo("example-workspace/example-repo").expect("the example repo parses")
    }

    // ---- AC 11.1: --repo parsing and the pure flag checks ----

    #[test]
    fn parse_repo_accepts_and_refuses() {
        for (input, workspace, slug) in [
            (
                "example-workspace/example-repo",
                "example-workspace",
                "example-repo",
            ),
            ("ws_1/repo.name-2", "ws_1", "repo.name-2"),
        ] {
            let parsed = parse_repo(input).unwrap_or_else(|error| panic!("{input}: {error:#}"));
            assert_eq!(parsed.workspace, workspace, "{input}");
            assert_eq!(parsed.slug, slug, "{input}");
        }
        for input in [
            "",
            "example-repo",
            "example-workspace/",
            "/example-repo",
            "example-workspace/example-repo/extra",
            "Example-Workspace/example-repo",
            "example-workspace/Example-Repo",
            "example-workspace/example repo",
            "https://bitbucket.org/example-workspace/example-repo",
        ] {
            let error = parse_repo(input).expect_err(input);
            let text = format!("{error:#}");
            assert!(
                text.contains("<workspace>/<repo slug>"),
                "{input:?}: the refusal names the expected shape: {text}"
            );
            assert_eq!(cli_error(&error).class, ExitClass::Usage, "{input:?}");
        }
    }

    #[test]
    fn check_refuses_non_jira_tracker_naming_not_installed() {
        for tracker in [TrackerKind::Github, TrackerKind::Gitlab] {
            // Bare `--code-host bitbucket-cloud` (the reserved-kind integration
            // test's shape) and a full flag set both stop at the pairing.
            for forge in [
                ForgeFlags {
                    code_host: CodeHostKind::BitbucketCloud,
                    tracker,
                    ..ForgeFlags::default()
                },
                ForgeFlags { tracker, ..flags() },
            ] {
                let error = check(&forge).expect_err("a non-Jira tracker is refused");
                let cli = cli_error(&error);
                assert_eq!(
                    cli.message,
                    "code host 'bitbucket-cloud' pairs only with --tracker jira-cloud; forge kind 'jira-cloud' is not installed in this build",
                    "{tracker:?}"
                );
                assert_eq!(cli.class, ExitClass::Unsupported, "{tracker:?}");
                assert_eq!(
                    cli.fix.as_deref(),
                    Some("rerun with --code-host github, or wait for the jira-cloud tracker"),
                    "{tracker:?}"
                );
            }
        }
        // The dispatch reaches this module and keeps the reserved-kind text.
        let error = select_forge(&ForgeFlags {
            code_host: CodeHostKind::BitbucketCloud,
            ..ForgeFlags::default()
        })
        .expect_err("bitbucket-cloud with the github tracker is refused");
        let text = format!("{error:#}");
        assert!(
            text.contains("not installed") && text.contains("bitbucket-cloud"),
            "{text}"
        );
        // Liveness: the Jira tracker passes the pairing.
        assert_reaches_the_jira_half(check(&flags()), "--tracker jira-cloud");
        assert_reaches_the_jira_half(select_forge(&flags()), "select_forge");
    }

    #[test]
    fn check_refuses_forge_url_on_cloud() {
        let forge = ForgeFlags {
            forge_url: Some(CLOUD_API.into()),
            ..flags()
        };
        let error = check(&forge).expect_err("--forge-url is refused on Cloud");
        let cli = cli_error(&error);
        assert_eq!(
            cli.message,
            "--forge-url does not apply to bitbucket-cloud; its API is https://api.bitbucket.org/2.0"
        );
        assert_eq!(cli.class, ExitClass::Usage);
        assert_reaches_the_jira_half(check(&flags()), "no --forge-url");
    }

    #[test]
    fn check_requires_a_token_file() {
        let forge = ForgeFlags {
            token_file: None,
            ..flags()
        };
        let error = check(&forge).expect_err("--token-file is required");
        assert!(format!("{error:#}").contains("--token-file"), "{error:#}");
        assert_eq!(cli_error(&error).class, ExitClass::Usage);
        assert_reaches_the_jira_half(check(&flags()), "with --token-file");
    }

    #[test]
    fn check_requires_a_review_account() {
        let forge = ForgeFlags {
            review_accounts: Vec::new(),
            ..flags()
        };
        let error = check(&forge).expect_err("--review-account is required");
        assert!(
            format!("{error:#}").contains("--review-account"),
            "{error:#}"
        );
        assert_eq!(cli_error(&error).class, ExitClass::Usage);
        assert_reaches_the_jira_half(check(&flags()), "with --review-account");
    }

    // ---- AC 11.2: the token probe against a local stub ----

    #[tokio::test]
    async fn probe_200_reads_project_id_and_path() {
        let (base, requests) = stub(vec![(REPO_ROUTE, 200, data("cloud_repository.json"))]).await;
        let token = token_file();
        let probed = probe(&base, token.path(), None, &repo())
            .await
            .expect("a 200 repository read probes clean");
        assert_eq!(probed.project_id, "{00000000-0000-4000-8000-000000000001}");
        assert_eq!(probed.path, "example-workspace/example-repo");
        assert_eq!(paths(&requests), vec![REPO_ROUTE.to_string()]);
        let heads = requests.lock().unwrap().clone();
        assert_eq!(
            authorization(&heads[0]).as_deref(),
            Some(format!("Bearer {TOKEN}").as_str()),
            "the trimmed token is sent as a Bearer token"
        );
    }

    #[tokio::test]
    async fn probe_401_names_the_token_file() {
        let (base, _) = stub(vec![(REPO_ROUTE, 401, data("cloud_401.json"))]).await;
        let token = token_file();
        let error = probe(&base, token.path(), None, &repo())
            .await
            .expect_err("a refused token fails the probe");
        let text = format!("{error:#}");
        assert!(
            text.contains(&format!(
                "the token in {} was refused (401)",
                token.path().display()
            )),
            "{text}"
        );
        assert!(!text.contains(TOKEN), "the token is never printed: {text}");
        assert_eq!(cli_error(&error).class, ExitClass::Failure);
    }

    #[tokio::test]
    async fn probe_403_names_the_missing_scope() {
        let (base, _) = stub(vec![(REPO_ROUTE, 403, data("cloud_403.json"))]).await;
        let token = token_file();
        let error = probe(&base, token.path(), None, &repo())
            .await
            .expect_err("a token without the scopes fails the probe");
        let text = format!("{error:#}");
        assert!(
            text.contains("repository:write and pullrequest:write"),
            "{text}"
        );
        assert!(!text.contains(TOKEN), "the token is never printed: {text}");
        assert_eq!(cli_error(&error).class, ExitClass::Failure);
    }

    // ---- AC 11.3: emitted values and egress against the goldens ----

    #[test]
    fn cloud_egress_matches_golden_from_recorded_ip_ranges() {
        let entries =
            cloud_egress(&data_json("ip-ranges.json")).expect("the recorded document parses");
        assert_eq!(
            serde_json::to_value(&entries).unwrap(),
            data_json("cloud_egress.golden.json")
        );
    }

    #[test]
    fn cloud_egress_keeps_only_bitbucket_ingress() {
        let doc = serde_json::json!({"items": [
            {"cidr": "198.51.100.0/24", "product": ["bitbucket"], "direction": ["egress"]},
            {"cidr": "198.51.101.0/24", "product": ["jira"], "direction": ["ingress"]},
            {"cidr": "198.51.102.0/24", "product": ["jira", "bitbucket"], "direction": ["egress", "ingress"]},
            {"cidr": "2001:db8:5::/48", "product": ["bitbucket"], "direction": ["ingress"]},
        ]});
        let entries = cloud_egress(&doc).expect("a well-formed document parses");
        assert_eq!(
            serde_json::to_value(&entries).unwrap(),
            serde_json::json!([
                {"cidr": "198.51.102.0/24", "ports": [{"protocol": "TCP", "port": 443}]},
                {"cidr": "2001:db8:5::/48", "ports": [{"protocol": "TCP", "port": 443}]},
            ])
        );
        cloud_egress(&serde_json::json!({"creationDate": "2026-10-12"}))
            .expect_err("a document with no items is refused");
    }

    #[tokio::test]
    async fn values_match_golden() {
        let (base, _) = stub(vec![(REPO_ROUTE, 200, data("cloud_repository.json"))]).await;
        let token = token_file();
        let probed = probe(&base, token.path(), None, &repo()).await.unwrap();
        let egress = cloud_egress(&data_json("ip-ranges.json")).unwrap();
        let emitted = values(
            "bitbucket",
            &probed,
            CLOUD_API,
            "bitbucket.org",
            &["example-account-1".to_string()],
            &egress,
        );
        assert_eq!(emitted, data_json("cloud_values.golden.json"));
    }

    // ---- AC 11.4 ----

    #[test]
    fn expiry_reads_unknown() {
        assert_eq!(
            expiry_line("bitbucket"),
            "credential code host bitbucket (static_token): unknown"
        );
    }

    // ---- AC 11, live: `cargo test bitbucket_live -- --ignored` ----

    #[tokio::test]
    #[ignore = "live: needs CURIE_BITBUCKET_CLOUD_REPO and CURIE_BITBUCKET_CLOUD_TOKEN_FILE"]
    async fn bitbucket_live_cloud_probe() {
        let (Ok(repo_path), Ok(token_path)) = (
            std::env::var("CURIE_BITBUCKET_CLOUD_REPO"),
            std::env::var("CURIE_BITBUCKET_CLOUD_TOKEN_FILE"),
        ) else {
            panic!(
                "bitbucket_live_cloud_probe runs only on purpose (--ignored): set CURIE_BITBUCKET_CLOUD_REPO and \
                 CURIE_BITBUCKET_CLOUD_TOKEN_FILE to probe Bitbucket Cloud"
            );
        };
        let repo = parse_repo(&repo_path).expect("CURIE_BITBUCKET_CLOUD_REPO parses");
        let probed = probe(CLOUD_API, Path::new(&token_path), None, &repo)
            .await
            .unwrap_or_else(|error| panic!("the live probe failed: {error:#}"));
        assert_eq!(probed.path, repo_path);
        assert!(
            probed.project_id.starts_with('{') && probed.project_id.ends_with('}'),
            "the Cloud project id is the braced repository UUID"
        );
        let doc = fetch_ip_ranges()
            .await
            .unwrap_or_else(|error| panic!("fetching the Atlassian IP ranges failed: {error:#}"));
        let egress = cloud_egress(&doc).expect("the live document parses");
        assert!(!egress.is_empty(), "Bitbucket publishes ingress ranges");
    }
}

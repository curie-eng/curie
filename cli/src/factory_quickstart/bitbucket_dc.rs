//! The Bitbucket Data Center code host for `curie factory quickstart` (#4397). Reserved: this
//! build has no adapter for it, so a quickstart naming `bitbucket-dc` stops here.

use anyhow::Result;

use super::{forge_not_installed, ForgeFlags};

pub(super) fn check(_forge: &ForgeFlags) -> Result<()> {
    Err(forge_not_installed("bitbucket-dc"))
}

#[cfg(test)]
mod tests {
    use std::net::IpAddr;
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::exit::{CliError, ExitClass};
    use crate::factory_quickstart::{select_forge, CodeHostKind, TrackerKind};

    /// The access token the stub expects. Held in a const so the commit-time
    /// secret scan does not read an inline assignment as a real credential.
    const TOKEN: &str = "bitbucket-dc-test-token";
    const SERVER: &str = "https://bbdc.example.invalid:8443/bitbucket";
    /// The stub serves under this context path, as a Data Center behind a
    /// reverse proxy does.
    const CONTEXT: &str = "/bitbucket";
    const PROPS_ROUTE: &str = "/bitbucket/rest/api/latest/application-properties";
    const REPO_ROUTE: &str = "/bitbucket/rest/api/latest/projects/FAC/repos/demo";

    fn read(dir: &str, name: &str) -> String {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../apps/api/tests/forge_fakes")
            .join(dir)
            .join(name);
        std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("missing {}: {error}", path.display()))
    }

    /// This module's goldens and error bodies.
    fn data(name: &str) -> String {
        read("bitbucket_quickstart", name)
    }

    /// A Data Center 9.4 response captured for the adapter's fake (stream B).
    fn capture(name: &str) -> String {
        read("bitbucket_dc_captures", name)
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
            code_host: CodeHostKind::BitbucketDc,
            tracker: TrackerKind::JiraCloud,
            forge_url: Some(SERVER.into()),
            token_file: Some(PathBuf::from("bitbucket-dc-token-that-is-never-read")),
            ca_file: None,
            review_accounts: vec!["101".into()],
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
                    .unwrap_or((404, r#"{"errors": []}"#.to_string()));
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
        parse_repo("FAC/demo").expect("the example repo parses")
    }

    fn props(version: &str) -> String {
        serde_json::json!({"version": version, "buildNumber": "1", "displayName": "Bitbucket"})
            .to_string()
    }

    /// The stub with the context path, a 9.4.24 server, and `repo` as the
    /// repository answer. Returns the stub's base URL with the context path.
    async fn server(repo: (u16, String)) -> (String, Requests) {
        let (base, requests) = stub(vec![
            (PROPS_ROUTE, 200, capture("application-properties.json")),
            (REPO_ROUTE, repo.0, repo.1),
        ])
        .await;
        (format!("{base}{CONTEXT}"), requests)
    }

    fn fake_resolve(host: &str) -> Result<Vec<IpAddr>> {
        assert_eq!(
            host, "bbdc.example.invalid",
            "the resolver gets the host only"
        );
        Ok(vec![
            "192.0.2.10".parse().unwrap(),
            "2001:db8::10".parse().unwrap(),
        ])
    }

    // ---- AC 11.1: --repo parsing and the pure flag checks ----

    #[test]
    fn parse_repo_accepts_and_refuses() {
        for (input, key, slug) in [
            ("FAC/demo", "FAC", "demo"),
            ("AB_1/repo.name-2", "AB_1", "repo.name-2"),
        ] {
            let parsed = parse_repo(input).unwrap_or_else(|error| panic!("{input}: {error:#}"));
            assert_eq!(parsed.project_key, key, "{input}");
            assert_eq!(parsed.slug, slug, "{input}");
        }
        for input in [
            "",
            "FAC",
            "FAC/",
            "/demo",
            "FAC/demo/extra",
            "fac/demo",
            "1FAC/demo",
            "F-AC/demo",
            "FAC/De mo",
            "FAC/Demo",
            "~user/demo",
        ] {
            let error = parse_repo(input).expect_err(input);
            let text = format!("{error:#}");
            assert!(
                text.contains("<PROJECT KEY>/<repo slug>"),
                "{input:?}: the refusal names the expected shape: {text}"
            );
            assert_eq!(cli_error(&error).class, ExitClass::Usage, "{input:?}");
        }
    }

    #[test]
    fn check_refuses_non_jira_tracker_naming_not_installed() {
        for tracker in [TrackerKind::Github, TrackerKind::Gitlab] {
            // Bare `--code-host bitbucket-dc` (the reserved-kind integration
            // test's shape) and a full flag set both stop at the pairing.
            for forge in [
                ForgeFlags {
                    code_host: CodeHostKind::BitbucketDc,
                    tracker,
                    ..ForgeFlags::default()
                },
                ForgeFlags { tracker, ..flags() },
            ] {
                let error = check(&forge).expect_err("a non-Jira tracker is refused");
                let cli = cli_error(&error);
                assert_eq!(
                    cli.message,
                    "code host 'bitbucket-dc' pairs only with --tracker jira-cloud; forge kind 'jira-cloud' is not installed in this build",
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
            code_host: CodeHostKind::BitbucketDc,
            ..ForgeFlags::default()
        })
        .expect_err("bitbucket-dc with the github tracker is refused");
        let text = format!("{error:#}");
        assert!(
            text.contains("not installed") && text.contains("bitbucket-dc"),
            "{text}"
        );
        // Liveness: the Jira tracker passes the pairing.
        assert_reaches_the_jira_half(check(&flags()), "--tracker jira-cloud");
        assert_reaches_the_jira_half(select_forge(&flags()), "select_forge");
    }

    #[test]
    fn check_requires_forge_url_on_dc() {
        let error = check(&ForgeFlags {
            forge_url: None,
            ..flags()
        })
        .expect_err("--forge-url is required on Data Center");
        assert!(format!("{error:#}").contains("--forge-url"), "{error:#}");
        assert_eq!(cli_error(&error).class, ExitClass::Usage);
        for refused in [
            "http://bbdc.example.invalid/bitbucket",
            "ftp://bbdc.example.invalid",
            "bbdc.example.invalid",
            "not a url",
        ] {
            let error = check(&ForgeFlags {
                forge_url: Some(refused.into()),
                ..flags()
            })
            .expect_err(refused);
            assert!(
                format!("{error:#}").contains("--forge-url"),
                "{refused}: {error:#}"
            );
            assert_eq!(cli_error(&error).class, ExitClass::Usage, "{refused}");
        }
        for accepted in [
            SERVER,
            "https://bbdc.example.invalid",
            "http://localhost:7990",
            "http://127.0.0.1:7990/bitbucket",
        ] {
            assert_reaches_the_jira_half(
                check(&ForgeFlags {
                    forge_url: Some(accepted.into()),
                    ..flags()
                }),
                accepted,
            );
        }
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

    // ---- AC 11.2: the token and version probe against a local stub ----

    #[tokio::test]
    async fn probe_200_reads_project_id_and_path() {
        // The captured repository and its project share id 1; give the
        // repository its own id so reading the project's would show.
        let mut body: serde_json::Value =
            serde_json::from_str(&capture("repository.json")).unwrap();
        body["id"] = serde_json::json!(7);
        let (base, requests) = server((200, body.to_string())).await;
        let token = token_file();
        let probed = probe(&base, token.path(), None, &repo())
            .await
            .expect("a 9.4.24 server and a 200 repository read probe clean");
        assert_eq!(probed.project_id, "7", "the repository id, as text");
        assert_eq!(probed.path, "FAC/demo");
        assert_eq!(probed.version, "9.4.24");
        assert_eq!(
            paths(&requests),
            vec![PROPS_ROUTE.to_string(), REPO_ROUTE.to_string()],
            "the version is read first, under the context path"
        );
        let heads = requests.lock().unwrap().clone();
        assert_eq!(
            authorization(&heads[1]).as_deref(),
            Some(format!("Bearer {TOKEN}").as_str()),
            "the trimmed token is sent as a Bearer token"
        );
    }

    #[tokio::test]
    async fn probe_401_names_the_token_file() {
        let (base, _) = server((401, capture("error-401.json"))).await;
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
        let (base, _) = server((403, capture("error-403.json"))).await;
        let token = token_file();
        let error = probe(&base, token.path(), None, &repo())
            .await
            .expect_err("a token without REPO_WRITE fails the probe");
        let text = format!("{error:#}");
        assert!(text.contains("REPO_WRITE"), "{text}");
        assert!(!text.contains(TOKEN), "the token is never printed: {text}");
        assert_eq!(cli_error(&error).class, ExitClass::Failure);
    }

    #[tokio::test]
    async fn dc_probe_refuses_9_3_0() {
        let (base, requests) = stub(vec![
            (PROPS_ROUTE, 200, data("dc_app_props_9_3_0.json")),
            (REPO_ROUTE, 200, capture("repository.json")),
        ])
        .await;
        let token = token_file();
        let error = probe(&format!("{base}{CONTEXT}"), token.path(), None, &repo())
            .await
            .expect_err("9.3.0 is below the minimum");
        assert!(
            format!("{error:#}")
                .contains("Bitbucket Data Center 9.3.0 is older than the minimum 9.4.0"),
            "{error:#}"
        );
        assert_eq!(
            paths(&requests),
            vec![PROPS_ROUTE.to_string()],
            "an old server is refused before the repository is read"
        );
    }

    #[tokio::test]
    async fn dc_probe_accepts_the_minimum_and_a_later_major() {
        // 10.0.1 sorts before 9.4.0 as text; the comparison must be numeric.
        for version in ["9.4.0", "10.0.1"] {
            let (base, _) = stub(vec![
                (PROPS_ROUTE, 200, props(version)),
                (REPO_ROUTE, 200, capture("repository.json")),
            ])
            .await;
            let token = token_file();
            let probed = probe(&format!("{base}{CONTEXT}"), token.path(), None, &repo())
                .await
                .unwrap_or_else(|error| panic!("{version}: {error:#}"));
            assert_eq!(probed.version, version);
        }
    }

    // ---- AC 11.3: emitted values and egress against the goldens ----

    #[test]
    fn dc_egress_matches_golden() {
        let entries = dc_egress(SERVER, fake_resolve).expect("the host resolves");
        assert_eq!(
            serde_json::to_value(&entries).unwrap(),
            data_json("dc_egress.golden.json")
        );
    }

    #[test]
    fn dc_egress_defaults_to_443_and_passes_an_address_through() {
        let entries = dc_egress("https://bbdc.example.invalid/bitbucket", fake_resolve)
            .expect("the host resolves");
        assert!(
            entries
                .iter()
                .all(|entry| entry.ports.iter().map(|p| p.port).collect::<Vec<_>>() == [443]),
            "{entries:?}"
        );
        let never = |host: &str| -> Result<Vec<IpAddr>> {
            panic!("an address must not be resolved, got {host}")
        };
        let entries = dc_egress("https://192.0.2.20:7990", never).expect("an IP passes through");
        assert_eq!(
            serde_json::to_value(&entries).unwrap(),
            serde_json::json!([{"cidr": "192.0.2.20/32", "ports": [{"protocol": "TCP", "port": 7990}]}])
        );
        let nothing = |_: &str| -> Result<Vec<IpAddr>> { Ok(Vec::new()) };
        dc_egress(SERVER, nothing).expect_err("a host with no address is refused");
    }

    #[tokio::test]
    async fn values_match_golden() {
        let (base, _) = server((200, capture("repository.json"))).await;
        let token = token_file();
        let probed = probe(&base, token.path(), None, &repo()).await.unwrap();
        let egress = dc_egress(SERVER, fake_resolve).unwrap();
        let emitted = values(
            "bitbucket-dc",
            &probed,
            SERVER,
            "bbdc.example.invalid",
            &["101".to_string()],
            &egress,
        );
        assert_eq!(emitted, data_json("dc_values.golden.json"));
    }

    // ---- AC 11.4 ----

    #[test]
    fn expiry_reads_unknown() {
        assert_eq!(
            expiry_line("bitbucket-dc"),
            "credential code host bitbucket-dc (static_token): unknown"
        );
    }

    // ---- AC 11, live: `cargo test bitbucket_live -- --ignored` ----

    #[tokio::test]
    #[ignore = "live: needs CURIE_BITBUCKET_DC_FIXTURE=<state dir>/state.json"]
    async fn bitbucket_live_dc_probe() {
        let Ok(state_path) = std::env::var("CURIE_BITBUCKET_DC_FIXTURE") else {
            panic!(
                "bitbucket_live_dc_probe runs only on purpose (--ignored): set CURIE_BITBUCKET_DC_FIXTURE to the fixture's \
                 state.json (curie dev forge-fixture bitbucket-dc up)"
            );
        };
        let state: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(&state_path)
                .unwrap_or_else(|error| panic!("reading {state_path}: {error}")),
        )
        .unwrap_or_else(|error| panic!("parsing {state_path}: {error}"));
        let field = |name: &str| {
            state[name]
                .as_str()
                .unwrap_or_else(|| panic!("{state_path} has no string {name}"))
                .to_string()
        };
        let (base_url, ca_file, token_path, repo_path) = (
            field("base_url"),
            field("ca_file"),
            field("token_file"),
            field("repo_path"),
        );
        let repo = parse_repo(&repo_path).expect("the fixture's repo_path parses");
        let probed = probe(
            &base_url,
            Path::new(&token_path),
            Some(Path::new(&ca_file)),
            &repo,
        )
        .await
        .unwrap_or_else(|error| panic!("the live probe failed: {error:#}"));
        assert_eq!(probed.path, repo_path);
        probed
            .project_id
            .parse::<u64>()
            .unwrap_or_else(|_| panic!("the DC project id is numeric: {}", probed.project_id));
        let numbers: Vec<u64> = probed
            .version
            .split('.')
            .take(3)
            .map(|part| part.parse().unwrap())
            .collect();
        assert!(numbers >= vec![9, 4, 0], "{}", probed.version);
    }
}

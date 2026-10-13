//! #4399 decision 23, AC 9: the GitLab half of `curie factory quickstart`,
//! `--code-host gitlab --tracker gitlab`, against a local stub GitLab.
//!
//! # Entry points the implementation must provide
//!
//! The command, driven through the real binary as the other quickstart tests do:
//!
//! ```text
//! curie factory quickstart --code-host gitlab --tracker gitlab \
//!     --repo <group/.../project> --token-file <path> \
//!     [--forge-url <web base>] [--ca-file <pem>] [--label <label>] [--dry-run] [--json]
//! ```
//!
//! 1. `--forge-url` is the web base. Absent means `https://gitlab.com`. `http://`
//!    is accepted only for `localhost` and `127.0.0.1` (the stub here is plain
//!    HTTP on 127.0.0.1); any other `http://` URL is a usage error naming
//!    `--forge-url` and `https`.
//! 2. Before any cluster action (no `helm` or `kubectl` call at all) the command
//!    runs, in order: (a) `GET /api/v4/version`, refusing below 17.0 with a
//!    message naming the reported version and `17.0`; (b) `GET
//!    /api/v4/personal_access_tokens/self`, refusing `"active": false` with a
//!    message containing `inactive`, and refusing missing scopes with a message
//!    naming each missing scope among `api` and `write_repository` as a whole
//!    word; (c) `GET /api/v4/user`; (d) `GET /api/v4/projects/<url-encoded path>`,
//!    refusing a project with issues disabled (message contains `issues`) and a
//!    404 (message names the `--repo` value); (e) the token user's access level
//!    on the project, refusing below 30 with a message containing
//!    `access level <n>`; (f) `GET /api/v4/projects/<id>/labels`, then one
//!    `POST /api/v4/projects/<id>/labels` (numeric id, JSON body with `name` and
//!    a `#rrggbb` `color`) per missing label among the trigger label (default
//!    `curie-factory`) and the four `curie-factory:*` state labels. A label that
//!    exists is never posted. Every request carries the `PRIVATE-TOKEN` header.
//! 3. `--dry-run` runs (a) to (e), sends no request other than a GET, makes no
//!    `helm`/`kubectl` call that writes, and under `--json` its plan lines
//!    include: the exact line `Token expires <YYYY-MM-DD> (<n> days)` or
//!    `Token has no expiry`; a `cluster surfaces ... --add gitlab=<path>` line
//!    and no `github=` line; a line naming every label (f) would ensure; the
//!    project id; the egress CIDR; and with `--ca-file`, a line naming
//!    `codeHostTrust` and the ConfigMap `curie-gitlab-ca`.
//! 4. A real run prints the same expiry line (stdout or stderr).
//! 5. Pairing: gitlab pairs only with gitlab. `--code-host gitlab` with another
//!    tracker, or `--tracker gitlab` with another code host, is refused with a
//!    message containing `pairs only with`, never `not installed`.
//! 6. `--token-file` with `--code-host github` stays refused (#4397).
//!
//! The pure values builders, so the gitlab.com golden needs no network:
//!
//! 1. `curie::factory_quickstart::GITLAB_DEFAULT_URL` (`"https://gitlab.com"`).
//! 2. `curie::factory_quickstart::gitlab_web_base(&ForgeFlags) -> anyhow::Result<String>`:
//!    the web base without a trailing `/`, the default when `forge_url` is `None`.
//! 3. `curie::factory_quickstart::GitlabFacts` with pub fields `web_base`,
//!    `username`, `expires_at: Option<String>` (the token's `YYYY-MM-DD`; `None`
//!    is no expiry), `project_id: u64`, `path`, `label`, `egress:
//!    Vec<EgressEntry>`, `ca_config_map: Option<String>`; and
//!    `curie::factory_quickstart::gitlab_entries(&GitlabFacts) -> NativeForgeEntries`.
//! 4. `curie::factory_intake::NativeForgeEntries` with pub fields `code_host`,
//!    `tracker`, `binding` (`serde_json::Value`) and `ca_config_map:
//!    Option<String>`; and `curie::factory_intake::native_forge_values(recorded:
//!    &Value, entries: &NativeForgeEntries) -> Value`, the forge-neutral writer
//!    beside `forge_entries_values`: it replaces the same-named entry of
//!    `factory.codeHosts`, `factory.trackers` and `factory.bindings` (keeping the
//!    others) and sets `codeHostTrust.caBundle.configMapRef` when a CA is given.
//!
//! The GitHub quickstart golden is pinned by `factory_quickstart_forges.rs`,
//! which this change leaves untouched.
//!
//! The self-managed dry-run plan golden lives in
//! `tests/golden/factory_quickstart/gitlab_dry_run_self_managed.txt`. Capture it
//! with `GITLAB_QUICKSTART_GOLDEN_CAPTURE=1` once the implementation exists,
//! review it, and commit it; the test fails while it is missing.
//!
//! Every token, user, project and host here is a fixture.

#![cfg(unix)]

use crate::support;

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};

use curie::factory_intake::{native_forge_values, NativeForgeEntries};
use curie::factory_quickstart::{
    forge_egress_entries, gitlab_entries, gitlab_web_base, CodeHostKind, ForgeFlags, GitlabFacts,
    TrackerKind, GITLAB_DEFAULT_URL,
};
use serde_json::{json, Value};
use support::{serve, MockServer, Response};

const PROJECT_PATH: &str = "factory-fixture/nested/widgets";
const PROJECT_ID: u64 = 1234;
const NO_ISSUES_PATH: &str = "factory-fixture/nested/no-issues";
const NO_ISSUES_ID: u64 = 1235;
const MISSING_PATH: &str = "factory-fixture/nested/missing";

const TRIGGER_LABEL: &str = "curie-factory";
const STATE_LABELS: [&str; 4] = [
    "curie-factory:queued",
    "curie-factory:running",
    "curie-factory:pr-open",
    "curie-factory:needs-human",
];

const BOT_USER: u64 = 42;
const BOT_USERNAME: &str = "factory-bot";
const REPORTER_USER: u64 = 43;
const READ_USER: u64 = 44;

/// Distinctive token values so a leak into output is caught by substring.
const BOT_TOKEN: &str = "fixture-token-4399-bot-7c1e";
const NO_EXPIRY_TOKEN: &str = "fixture-token-4399-forever-2b9d";
const REPORTER_TOKEN: &str = "fixture-token-4399-reporter-5a0f";
const READ_API_TOKEN: &str = "fixture-token-4399-readapi-81d3";
const API_ONLY_TOKEN: &str = "fixture-token-4399-apionly-44e2";
const INACTIVE_TOKEN: &str = "fixture-token-4399-inactive-9f6a";
const UNKNOWN_TOKEN: &str = "fixture-token-4399-unknown-0000";
const ALL_TOKENS: [&str; 7] = [
    BOT_TOKEN,
    NO_EXPIRY_TOKEN,
    REPORTER_TOKEN,
    READ_API_TOKEN,
    API_ONLY_TOKEN,
    INACTIVE_TOKEN,
    UNKNOWN_TOKEN,
];

const CA_CONFIG_MAP: &str = "curie-gitlab-ca";

fn today() -> time::Date {
    time::OffsetDateTime::now_utc().date()
}

fn in_days(days: i64) -> time::Date {
    today() + time::Duration::days(days)
}

fn expiry_line(days: i64) -> String {
    format!("Token expires {} ({days} days)", in_days(days))
}

struct TokenRecord {
    user: u64,
    scopes: &'static [&'static str],
    active: bool,
    expires_in_days: Option<i64>,
}

fn token_record(token: &str) -> Option<TokenRecord> {
    let rw: &'static [&'static str] = &["api", "write_repository"];
    Some(match token {
        BOT_TOKEN => TokenRecord {
            user: BOT_USER,
            scopes: rw,
            active: true,
            expires_in_days: Some(30),
        },
        NO_EXPIRY_TOKEN => TokenRecord {
            user: BOT_USER,
            scopes: rw,
            active: true,
            expires_in_days: None,
        },
        REPORTER_TOKEN => TokenRecord {
            user: REPORTER_USER,
            scopes: rw,
            active: true,
            expires_in_days: Some(30),
        },
        READ_API_TOKEN => TokenRecord {
            user: READ_USER,
            scopes: &["read_api"],
            active: true,
            expires_in_days: Some(30),
        },
        API_ONLY_TOKEN => TokenRecord {
            user: BOT_USER,
            scopes: &["api", "read_repository"],
            active: true,
            expires_in_days: Some(30),
        },
        INACTIVE_TOKEN => TokenRecord {
            user: BOT_USER,
            scopes: rw,
            active: false,
            expires_in_days: Some(-1),
        },
        _ => return None,
    })
}

fn user_json(id: u64) -> Option<Value> {
    let (username, level) = match id {
        BOT_USER => (BOT_USERNAME, 30),
        REPORTER_USER => ("reporter", 20),
        READ_USER => ("read-bot", 30),
        _ => return None,
    };
    Some(json!({
        "id": id,
        "username": username,
        "name": username,
        "state": "active",
        "bot": false,
        "access_level": level,
    }))
}

fn access_level(user: u64) -> u64 {
    user_json(user)
        .and_then(|u| u["access_level"].as_u64())
        .unwrap_or(0)
}

/// The stub's mutable state: the project's labels.
#[derive(Clone, Default)]
struct Labels(Arc<Mutex<Vec<String>>>);

impl Labels {
    fn with(names: &[&str]) -> Self {
        Self(Arc::new(Mutex::new(
            names.iter().map(|s| s.to_string()).collect(),
        )))
    }

    fn names(&self) -> Vec<String> {
        self.0.lock().unwrap().clone()
    }
}

fn project_json(base: &str, id: u64, path: &str, issues: bool, level: u64) -> Value {
    json!({
        "id": id,
        "path_with_namespace": path,
        "default_branch": "main",
        "issues_enabled": issues,
        "issues_access_level": if issues { "enabled" } else { "disabled" },
        "web_url": format!("{base}/{path}"),
        "http_url_to_repo": format!("{base}/{path}.git"),
        "permissions": {
            "project_access": null,
            "group_access": {"access_level": level, "notification_level": 3},
        },
    })
}

fn decode_path(segment: &str) -> String {
    segment.replace("%2F", "/").replace("%2f", "/")
}

/// A GitLab REST v4 stand-in, shaped after
/// https://docs.gitlab.com/api/version/,
/// https://docs.gitlab.com/api/personal_access_tokens/#get-single-personal-access-token,
/// https://docs.gitlab.com/api/users/#get-the-current-user,
/// https://docs.gitlab.com/api/projects/#get-a-single-project,
/// https://docs.gitlab.com/api/members/#get-a-member-of-a-group-or-project-including-inherited-and-invited-members
/// and https://docs.gitlab.com/api/labels/. Every call needs a known
/// `PRIVATE-TOKEN` header, else 401.
fn gitlab(version: &'static str, labels: Labels) -> MockServer {
    serve(move |req| {
        let raw = req.path.split('?').next().unwrap_or("");
        let Some(path) = raw.strip_prefix("/api/v4") else {
            return Response::json(404, r#"{"message":"404 Not Found"}"#);
        };
        let Some(record) = req.header("private-token").and_then(token_record) else {
            return Response::json(401, r#"{"message":"401 Unauthorized"}"#);
        };
        let base = format!("http://{}", req.header("host").unwrap_or("127.0.0.1"));
        let level = access_level(record.user);
        let segments: Vec<&str> = path.trim_start_matches('/').split('/').collect();
        match (req.method.as_str(), segments.as_slice()) {
            ("GET", ["version"]) => Response::json(
                200,
                &json!({"version": version, "revision": "fixture"}).to_string(),
            ),
            ("GET", ["personal_access_tokens", "self"]) => Response::json(
                200,
                &json!({
                    "id": 7,
                    "name": "curie-factory",
                    "revoked": false,
                    "created_at": "2026-10-01T00:00:00.000Z",
                    "scopes": record.scopes,
                    "user_id": record.user,
                    "last_used_at": null,
                    "active": record.active,
                    "expires_at": record.expires_in_days.map(|d| in_days(d).to_string()),
                })
                .to_string(),
            ),
            ("GET", ["user"]) => Response::json(200, &user_json(record.user).unwrap().to_string()),
            ("GET", ["users", id]) => match id.parse().ok().and_then(user_json) {
                Some(user) => Response::json(200, &user.to_string()),
                None => Response::json(404, r#"{"message":"404 User Not Found"}"#),
            },
            ("GET", ["projects", project]) => {
                let decoded = decode_path(project);
                match decoded.as_str() {
                    PROJECT_PATH | "1234" => Response::json(
                        200,
                        &project_json(&base, PROJECT_ID, PROJECT_PATH, true, level).to_string(),
                    ),
                    NO_ISSUES_PATH | "1235" => Response::json(
                        200,
                        &project_json(&base, NO_ISSUES_ID, NO_ISSUES_PATH, false, level)
                            .to_string(),
                    ),
                    _ => Response::json(404, r#"{"message":"404 Project Not Found"}"#),
                }
            }
            ("GET", ["projects", _, "members", "all", user]) => {
                match user.parse().ok().and_then(user_json) {
                    Some(user) => Response::json(200, &user.to_string()),
                    None => Response::json(404, r#"{"message":"404 Not found"}"#),
                }
            }
            ("GET", ["projects", "1234", "labels"]) => {
                let listed: Vec<Value> = labels
                    .names()
                    .iter()
                    .enumerate()
                    .map(|(i, name)| json!({"id": 100 + i, "name": name, "color": "#6699cc"}))
                    .collect();
                Response::json(200, &Value::Array(listed).to_string())
            }
            ("POST", ["projects", "1234", "labels"]) => {
                let body: Value = serde_json::from_slice(&req.body).unwrap_or(Value::Null);
                let Some(name) = body["name"].as_str() else {
                    return Response::json(400, r#"{"message":"name is missing"}"#);
                };
                let mut held = labels.0.lock().unwrap();
                if held.iter().any(|held| held == name) {
                    return Response::json(409, r#"{"message":"Label already exists"}"#);
                }
                held.push(name.to_string());
                Response::json(
                    201,
                    &json!({"id": 200 + held.len(), "name": name, "color": body["color"]})
                        .to_string(),
                )
            }
            _ => Response::json(404, r#"{"message":"404 Not Found"}"#),
        }
    })
}

fn host_port(server: &MockServer) -> (String, u16) {
    let authority = server.base_url.trim_start_matches("http://").to_string();
    let port = authority.rsplit_once(':').unwrap().1.parse().unwrap();
    (authority, port)
}

fn assert_no_token(text: &str, what: &str) {
    for token in ALL_TOKENS {
        assert!(
            !text.contains(token),
            "{what} carries a GitLab token: {text}"
        );
    }
}

fn label_posts(server: &MockServer) -> Vec<Value> {
    server
        .recorded()
        .iter()
        .filter(|req| {
            req.method == "POST"
                && req.path.split('?').next() == Some("/api/v4/projects/1234/labels")
        })
        .map(|req| serde_json::from_slice(&req.body).expect("a label POST body is JSON"))
        .collect()
}

fn non_get(server: &MockServer) -> Vec<String> {
    server
        .recorded()
        .iter()
        .filter(|req| req.method != "GET")
        .map(|req| format!("{} {}", req.method, req.path))
        .collect()
}

fn read_paths(server: &MockServer) -> Vec<String> {
    server
        .recorded()
        .iter()
        .map(|req| req.path.split('?').next().unwrap_or("").to_string())
        .collect()
}

fn whole_word(text: &str, word: &str) -> bool {
    regex::Regex::new(&format!(
        r"(^|[^A-Za-z0-9_]){}([^A-Za-z0-9_]|$)",
        regex::escape(word)
    ))
    .unwrap()
    .is_match(text)
}

fn all_labels() -> Vec<String> {
    std::iter::once(TRIGGER_LABEL)
        .chain(STATE_LABELS)
        .map(str::to_string)
        .collect()
}

// ---------------------------------------------------------------------------
// The binary harness
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

    fn plan(&self) -> Vec<String> {
        let body: Value = serde_json::from_str(&self.stdout)
            .unwrap_or_else(|e| panic!("one plan object on stdout: {e}\n{}", self.all()));
        body["plan"]
            .as_array()
            .unwrap_or_else(|| panic!("plan lines: {body}"))
            .iter()
            .map(|line| line.as_str().unwrap().to_string())
            .collect()
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
        Self { dir }
    }

    /// A token file holding `token` with the newline an editor leaves.
    fn token_file(&self, token: &str) -> String {
        let path = self
            .dir
            .path()
            .join(format!("token-{}", &token[token.len() - 4..]));
        fs::write(&path, format!("{token}\n")).unwrap();
        path.display().to_string()
    }

    /// A PEM CA certificate generated at runtime with openssl.
    fn ca_file(&self) -> String {
        let cert = self.dir.path().join("gitlab-ca.pem");
        let key = self.dir.path().join("gitlab-ca.key");
        let output = Command::new("openssl")
            .args([
                "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2",
            ])
            .args(["-subj", "/CN=curie-gitlab-fixture-ca"])
            .arg("-keyout")
            .arg(&key)
            .arg("-out")
            .arg(&cert)
            .output()
            .expect("runtime CA generator");
        assert!(output.status.success(), "runtime CA generator failed");
        cert.display().to_string()
    }

    fn shim_log(&self) -> String {
        fs::read_to_string(self.dir.path().join("shim.log")).unwrap_or_default()
    }

    fn chart() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../charts/curie")
    }

    fn run(&self, extra: &[&str]) -> Run {
        let output = Command::new(env!("CARGO_BIN_EXE_curie"))
            .args([
                "factory",
                "quickstart",
                "--context",
                "kind-curie-factory",
                "--chart",
            ])
            .arg(Self::chart())
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
            .env("CURIE_GITHUB_API_URL", "http://127.0.0.1:9")
            .env_remove("CURIE_API_URL")
            .env_remove("CI")
            .env("NO_COLOR", "1")
            .stdin(Stdio::null())
            .output()
            .unwrap();
        Run {
            code: output.status.code().unwrap_or(-1),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        }
    }
}

/// `--code-host gitlab --tracker gitlab` with `--repo`, `--token-file` and
/// `--forge-url`.
fn gitlab_args(repo: &str, token_file: &str, forge_url: &str) -> Vec<String> {
    [
        "--code-host",
        "gitlab",
        "--tracker",
        "gitlab",
        "--repo",
        repo,
        "--token-file",
        token_file,
        "--forge-url",
        forge_url,
    ]
    .iter()
    .map(|s| s.to_string())
    .collect()
}

fn refs(args: &[String]) -> Vec<&str> {
    args.iter().map(String::as_str).collect()
}

fn dry_run_json(cli: &Cli, args: &[String]) -> Run {
    let mut all = refs(args);
    all.extend(["--dry-run", "--json"]);
    cli.run(&all)
}

fn cluster_writes(log: &str) -> Vec<String> {
    log.lines()
        .filter(|line| {
            let words: Vec<&str> = line.split_whitespace().collect();
            matches!(
                words.as_slice(),
                ["helm", "upgrade" | "install" | "uninstall", ..]
                    | [
                        "kubectl",
                        "apply" | "create" | "patch" | "delete" | "label" | "annotate" | "rollout",
                        ..
                    ]
                    | ["kind", "create" | "delete", ..]
            )
        })
        .map(str::to_string)
        .collect()
}

fn no_cluster_calls(log: &str) -> bool {
    !log.lines()
        .any(|line| line.starts_with("helm ") || line.starts_with("kubectl "))
}

// ---------------------------------------------------------------------------
// Pure: the web base and the values golden (gitlab.com and self-managed)
// ---------------------------------------------------------------------------

fn flags(forge_url: Option<&str>) -> ForgeFlags {
    ForgeFlags {
        code_host: CodeHostKind::Gitlab,
        tracker: TrackerKind::Gitlab,
        forge_url: forge_url.map(str::to_string),
        token_file: Some(PathBuf::from("gitlab-token-that-is-never-read")),
        ..ForgeFlags::default()
    }
}

#[test]
fn gitlab_web_base_defaults_to_gitlab_com_when_forge_url_is_absent() {
    assert_eq!(GITLAB_DEFAULT_URL, "https://gitlab.com");
    assert_eq!(gitlab_web_base(&flags(None)).unwrap(), "https://gitlab.com");
    for (given, expected) in [
        ("https://gitlab.example.test", "https://gitlab.example.test"),
        (
            "https://gitlab.example.test/",
            "https://gitlab.example.test",
        ),
        (
            "https://gitlab.example.test:8443/gitlab/",
            "https://gitlab.example.test:8443/gitlab",
        ),
        ("http://127.0.0.1:8080", "http://127.0.0.1:8080"),
        ("http://localhost:8080/", "http://localhost:8080"),
    ] {
        assert_eq!(
            gitlab_web_base(&flags(Some(given))).unwrap_or_else(|e| panic!("{given}: {e:#}")),
            expected,
            "{given}"
        );
    }
    let error = gitlab_web_base(&flags(Some("http://gitlab.example.test")))
        .expect_err("plain http to a remote host is refused");
    let text = format!("{error:#}");
    assert!(
        text.contains("--forge-url") && text.contains("https"),
        "{text}"
    );
}

fn gitlab_com_egress() -> Vec<curie::factory_quickstart::EgressEntry> {
    forge_egress_entries("gitlab.com", &[443], |_| {
        Ok(vec!["172.65.251.78".parse().unwrap()])
    })
    .unwrap()
}

#[test]
fn gitlab_values_golden_for_gitlab_com() {
    let entries = gitlab_entries(&GitlabFacts {
        web_base: GITLAB_DEFAULT_URL.to_string(),
        username: BOT_USERNAME.to_string(),
        expires_at: Some("2026-11-12".to_string()),
        project_id: PROJECT_ID,
        path: "acme/platform/widgets".to_string(),
        label: TRIGGER_LABEL.to_string(),
        egress: gitlab_com_egress(),
        ca_config_map: None,
    });
    let values = native_forge_values(&Value::Null, &entries);

    let egress = json!([{
        "cidr": "172.65.251.78/32",
        "ports": [{"protocol": "TCP", "port": 443}],
        "host": "gitlab.com",
    }]);
    let credential = json!({
        "type": "static_token",
        "existingSecret": "curie-gitlab-token",
        "existingSecretKey": "token",
        "expiresAt": "2026-11-12T00:00:00Z",
    });
    let code_host = json!({
        "name": "gitlab",
        "kind": "gitlab",
        "host": "gitlab.com",
        "apiUrl": "https://gitlab.com/api/v4",
        "credential": credential,
        "egress": egress,
    });
    let mut tracker = code_host.clone();
    tracker["label"] = json!(TRIGGER_LABEL);
    tracker["mention"] = json!(BOT_USERNAME);
    let binding = json!({
        "name": "gitlab-1234",
        "tracker": "gitlab",
        "trackerScopeId": "1234",
        "repos": [{
            "alias": "widgets",
            "codeHost": "gitlab",
            "projectId": "1234",
            "path": "acme/platform/widgets",
        }],
    });
    assert_eq!(entries.code_host, code_host);
    assert_eq!(entries.tracker, tracker);
    assert_eq!(entries.binding, binding);
    assert_eq!(entries.ca_config_map, None);
    assert_eq!(
        values.pointer("/factory/codeHosts"),
        Some(&json!([code_host]))
    );
    assert_eq!(values.pointer("/factory/trackers"), Some(&json!([tracker])));
    assert_eq!(values.pointer("/factory/bindings"), Some(&json!([binding])));
    assert_eq!(
        values.pointer("/codeHostTrust"),
        None,
        "gitlab.com needs no CA: {values}"
    );
    assert!(
        entries.code_host["credential"].get("username").is_none(),
        "a gitlab credential never sets username (decision 3)"
    );
}

#[test]
fn gitlab_values_golden_for_self_managed_with_a_ca_and_no_expiry() {
    let egress = forge_egress_entries("203.0.113.7", &[8443], |_| unreachable!()).unwrap();
    let entries = gitlab_entries(&GitlabFacts {
        web_base: "https://gitlab.example.test:8443/gitlab".to_string(),
        username: BOT_USERNAME.to_string(),
        expires_at: None,
        project_id: PROJECT_ID,
        path: PROJECT_PATH.to_string(),
        label: "factory-fast".to_string(),
        egress,
        ca_config_map: Some(CA_CONFIG_MAP.to_string()),
    });
    let values = native_forge_values(&Value::Null, &entries);

    let egress = json!([{"cidr": "203.0.113.7/32", "ports": [{"protocol": "TCP", "port": 8443}]}]);
    let code_host = json!({
        "name": "gitlab",
        "kind": "gitlab",
        "host": "gitlab.example.test:8443",
        "apiUrl": "https://gitlab.example.test:8443/gitlab/api/v4",
        "credential": {
            "type": "static_token",
            "existingSecret": "curie-gitlab-token",
            "existingSecretKey": "token",
            "neverExpires": true,
        },
        "egress": egress,
    });
    let mut tracker = code_host.clone();
    tracker["label"] = json!("factory-fast");
    tracker["mention"] = json!(BOT_USERNAME);
    let binding = json!({
        "name": "gitlab-1234",
        "tracker": "gitlab",
        "trackerScopeId": "1234",
        "repos": [{
            "alias": "widgets",
            "codeHost": "gitlab",
            "projectId": "1234",
            "path": PROJECT_PATH,
        }],
    });
    assert_eq!(
        values.pointer("/factory/codeHosts"),
        Some(&json!([code_host]))
    );
    assert_eq!(values.pointer("/factory/trackers"), Some(&json!([tracker])));
    assert_eq!(values.pointer("/factory/bindings"), Some(&json!([binding])));
    assert_eq!(
        values.pointer("/codeHostTrust"),
        Some(&json!({"caBundle": {"configMapRef": {"name": CA_CONFIG_MAP, "key": "ca.crt"}}})),
        "{values}"
    );
}

#[test]
fn gitlab_values_keep_the_other_recorded_entries() {
    let entries = NativeForgeEntries {
        code_host: json!({"name": "gitlab", "kind": "gitlab", "host": "gitlab.com"}),
        tracker: json!({"name": "gitlab", "kind": "gitlab", "label": "curie-factory"}),
        binding: json!({"name": "gitlab-1234", "tracker": "gitlab"}),
        ca_config_map: None,
    };
    let github = json!({"name": "github", "kind": "github", "host": "github.com"});
    let jira = json!({"name": "jira", "kind": "jira_cloud"});
    let recorded = json!({"factory": {
        "codeHosts": [github, {"name": "gitlab", "kind": "gitlab", "host": "stale"}],
        "trackers": [github, jira],
        "bindings": [{"name": "jira", "tracker": "jira"}, {"name": "gitlab-1234", "tracker": "old"}],
    }});
    let values = native_forge_values(&recorded, &entries);
    assert_eq!(
        values.pointer("/factory/codeHosts"),
        Some(&json!([github, entries.code_host]))
    );
    assert_eq!(
        values.pointer("/factory/trackers"),
        Some(&json!([github, jira, entries.tracker]))
    );
    assert_eq!(
        values.pointer("/factory/bindings"),
        Some(&json!([{"name": "jira", "tracker": "jira"}, entries.binding]))
    );
}

// ---------------------------------------------------------------------------
// The command against the stub (binary)
// ---------------------------------------------------------------------------

#[test]
fn factory_quickstart_gitlab_dry_run_runs_a_to_e_and_writes_nothing() {
    let cli = Cli::new(0);
    let server = gitlab("19.4.1-ce.0", Labels::default());
    let run = dry_run_json(
        &cli,
        &gitlab_args(PROJECT_PATH, &cli.token_file(BOT_TOKEN), &server.base_url),
    );
    let text = run.all();
    assert_eq!(run.code, 0, "{text}");
    let lines = run.plan();

    let paths = read_paths(&server);
    for expected in [
        "/api/v4/version",
        "/api/v4/personal_access_tokens/self",
        "/api/v4/user",
        "/api/v4/projects/factory-fixture%2Fnested%2Fwidgets",
    ] {
        assert!(
            paths.iter().any(|p| p.eq_ignore_ascii_case(expected)),
            "{expected} was not read: {paths:?}"
        );
    }
    assert!(
        non_get(&server).is_empty(),
        "a dry run writes nothing to GitLab: {:?}",
        non_get(&server)
    );
    assert!(label_posts(&server).is_empty());
    for req in server.recorded() {
        assert_eq!(
            req.header("private-token"),
            Some(BOT_TOKEN),
            "every request authenticates with PRIVATE-TOKEN: {}",
            req.path
        );
    }
    let writes = cluster_writes(&cli.shim_log());
    assert!(
        writes.is_empty(),
        "a dry run writes nothing to the cluster: {writes:?}"
    );

    assert!(
        lines.iter().any(|line| *line == expiry_line(30)),
        "the expiry line is a plan line: {lines:#?}"
    );
    assert!(
        lines.iter().any(|line| line.contains("cluster surfaces")
            && line.ends_with(&format!("--add gitlab={PROJECT_PATH}"))),
        "the agent is bound through gitlab=<path>: {lines:#?}"
    );
    assert!(
        lines.iter().all(|line| !line.contains("github=")),
        "{lines:#?}"
    );
    for label in all_labels() {
        assert!(
            lines.iter().any(|line| line.contains(&label)),
            "the plan names label {label}: {lines:#?}"
        );
    }
    assert!(
        lines
            .iter()
            .any(|line| line.contains(&PROJECT_ID.to_string())),
        "the plan names project id {PROJECT_ID}: {lines:#?}"
    );
    assert!(
        lines.iter().any(|line| line.contains("127.0.0.1/32")),
        "the plan names the egress entry: {lines:#?}"
    );
    assert_no_token(&text, "the dry run");
}

#[test]
fn factory_quickstart_gitlab_dry_run_matches_the_self_managed_golden() {
    let cli = Cli::new(0);
    let server = gitlab("19.4.1-ce.0", Labels::default());
    let ca = cli.ca_file();
    let mut args = gitlab_args(PROJECT_PATH, &cli.token_file(BOT_TOKEN), &server.base_url);
    args.extend(["--ca-file".to_string(), ca.clone()]);
    let run = dry_run_json(&cli, &args);
    let text = run.all();
    assert_eq!(run.code, 0, "{text}");
    let lines = run.plan();
    assert!(
        lines
            .iter()
            .any(|line| line.contains("codeHostTrust") && line.contains(CA_CONFIG_MAP)),
        "the CA goes into codeHostTrust.caBundle through ConfigMap {CA_CONFIG_MAP}: {lines:#?}"
    );
    assert!(non_get(&server).is_empty(), "{:?}", non_get(&server));
    assert_no_token(&text, "the dry run");

    let (authority, port) = host_port(&server);
    let port_word = regex::Regex::new(&format!(r"\b{port}\b")).unwrap();
    let dir = cli.dir.path().display().to_string();
    let chart = Cli::chart().display().to_string();
    let normalized: String = lines
        .iter()
        .map(|line| {
            let line = line
                .replace(&chart, "<chart>")
                .replace(&ca, "<ca-file>")
                .replace(&dir, "<dir>")
                .replace(&authority, "<forge-host>")
                .replace(&in_days(30).to_string(), "<expires>");
            format!("{}\n", port_word.replace_all(&line, "<port>"))
        })
        .collect();
    let golden = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/golden/factory_quickstart/gitlab_dry_run_self_managed.txt");
    if std::env::var_os("GITLAB_QUICKSTART_GOLDEN_CAPTURE").is_some() {
        fs::write(&golden, &normalized).unwrap();
        return;
    }
    let expected = fs::read_to_string(&golden).unwrap_or_else(|err| {
        panic!(
            "missing golden {}: {err}; capture it with GITLAB_QUICKSTART_GOLDEN_CAPTURE=1, review it, and commit it.\n{normalized}",
            golden.display()
        )
    });
    assert_eq!(
        normalized, expected,
        "the self-managed dry-run plan drifted"
    );
}

#[test]
fn factory_quickstart_gitlab_prints_the_token_expiry() {
    let cli = Cli::new(0);
    let server = gitlab("19.4.1-ce.0", Labels::default());
    let run = dry_run_json(
        &cli,
        &gitlab_args(PROJECT_PATH, &cli.token_file(BOT_TOKEN), &server.base_url),
    );
    assert_eq!(run.code, 0, "{}", run.all());
    let lines = run.plan();
    assert!(lines.contains(&expiry_line(30)), "{lines:#?}");
    assert!(
        !lines.iter().any(|l| l == "Token has no expiry"),
        "{lines:#?}"
    );

    let run = dry_run_json(
        &cli,
        &gitlab_args(
            PROJECT_PATH,
            &cli.token_file(NO_EXPIRY_TOKEN),
            &server.base_url,
        ),
    );
    assert_eq!(run.code, 0, "{}", run.all());
    let lines = run.plan();
    assert!(
        lines.iter().any(|l| l == "Token has no expiry"),
        "{lines:#?}"
    );
    assert!(
        !lines.iter().any(|l| l.starts_with("Token expires")),
        "{lines:#?}"
    );
}

#[test]
fn factory_quickstart_gitlab_refusals_name_the_cause() {
    struct Case {
        name: &'static str,
        version: &'static str,
        token: &'static str,
        repo: &'static str,
        named: Vec<&'static str>,
        whole_words: Vec<&'static str>,
    }
    let case = |name, version, token, repo| Case {
        name,
        version,
        token,
        repo,
        named: Vec::new(),
        whole_words: Vec::new(),
    };
    let cases = vec![
        Case {
            named: vec!["16.11", "17.0"],
            ..case("server below 17.0", "16.11.2", BOT_TOKEN, PROJECT_PATH)
        },
        Case {
            named: vec!["inactive"],
            ..case(
                "inactive token",
                "19.4.1-ce.0",
                INACTIVE_TOKEN,
                PROJECT_PATH,
            )
        },
        Case {
            whole_words: vec!["api", "write_repository"],
            ..case("read_api only", "19.4.1-ce.0", READ_API_TOKEN, PROJECT_PATH)
        },
        Case {
            whole_words: vec!["write_repository"],
            ..case(
                "missing write_repository",
                "19.4.1-ce.0",
                API_ONLY_TOKEN,
                PROJECT_PATH,
            )
        },
        Case {
            named: vec!["access level 20"],
            ..case("reporter", "19.4.1-ce.0", REPORTER_TOKEN, PROJECT_PATH)
        },
        Case {
            named: vec![MISSING_PATH],
            ..case("unknown project", "19.4.1-ce.0", BOT_TOKEN, MISSING_PATH)
        },
        Case {
            named: vec!["issues", NO_ISSUES_PATH],
            ..case("issues disabled", "19.4.1-ce.0", BOT_TOKEN, NO_ISSUES_PATH)
        },
        Case {
            named: vec!["--token-file", "401"],
            ..case("unknown token", "19.4.1-ce.0", UNKNOWN_TOKEN, PROJECT_PATH)
        },
    ];
    for case in cases {
        let cli = Cli::new(0);
        let server = gitlab(case.version, Labels::default());
        let run = dry_run_json(
            &cli,
            &gitlab_args(case.repo, &cli.token_file(case.token), &server.base_url),
        );
        let text = run.all();
        let name = case.name;
        assert_ne!(run.code, 0, "{name}: {text}");
        for needle in &case.named {
            assert!(
                text.contains(needle),
                "{name}: refusal names {needle:?}: {text}"
            );
        }
        for word in &case.whole_words {
            assert!(
                whole_word(&text, word),
                "{name}: refusal names scope {word:?} as a word: {text}"
            );
        }
        assert!(!text.contains("not installed"), "{name}: {text}");
        assert!(
            non_get(&server).is_empty(),
            "{name}: a refused run writes nothing to GitLab: {:?}",
            non_get(&server)
        );
        assert!(
            cluster_writes(&cli.shim_log()).is_empty(),
            "{name}: {}",
            cli.shim_log()
        );
        assert_no_token(&text, name);
    }
}

/// A real (not dry) run refuses before any helm or kubectl call, so a refused
/// token mutates nothing on the cluster or the project.
#[test]
fn factory_quickstart_gitlab_refuses_before_any_cluster_call() {
    let cli = Cli::new(1);
    let server = gitlab("19.4.1-ce.0", Labels::default());
    let args = gitlab_args(
        PROJECT_PATH,
        &cli.token_file(REPORTER_TOKEN),
        &server.base_url,
    );
    let run = cli.run(&refs(&args));
    let text = run.all();
    assert_ne!(run.code, 0, "{text}");
    assert!(text.contains("access level 20"), "{text}");
    assert!(
        !server.recorded().is_empty(),
        "the preflight reached GitLab"
    );
    assert!(
        label_posts(&server).is_empty(),
        "no label before the refusal"
    );
    let log = cli.shim_log();
    assert!(
        no_cluster_calls(&log),
        "no cluster call before the GitLab preflight refuses: {log}"
    );
    assert_no_token(&text, "the command output");
}

/// Step f: the first run creates the trigger label and every state label, a
/// second run creates none, and a label that exists is left alone. The cluster
/// tools answer 1, so each run stops at its first cluster step after f.
#[test]
fn factory_quickstart_gitlab_creates_every_label_on_the_first_run_and_none_on_the_second() {
    let cli = Cli::new(1);
    let labels = Labels::with(&["bug"]);
    let server = gitlab("19.4.1-ce.0", labels.clone());
    let args = gitlab_args(PROJECT_PATH, &cli.token_file(BOT_TOKEN), &server.base_url);

    let first = cli.run(&refs(&args));
    let text = first.all();
    let posted: Vec<String> = label_posts(&server)
        .iter()
        .map(|body| body["name"].as_str().unwrap_or_default().to_string())
        .collect();
    let mut sorted = posted.clone();
    sorted.sort();
    let mut expected = all_labels();
    expected.sort();
    assert_eq!(
        sorted, expected,
        "first run creates every label once: {text}"
    );
    for body in label_posts(&server) {
        let color = body["color"].as_str().unwrap_or_default();
        assert!(
            color.len() == 7 && color.starts_with('#'),
            "a GitLab label needs a #rrggbb color: {body}"
        );
    }
    let mut held = labels.names();
    held.sort();
    let mut want: Vec<String> = all_labels();
    want.push("bug".into());
    want.sort();
    assert_eq!(held, want, "the unrelated label is kept");
    assert!(
        text.contains(&expiry_line(30)),
        "a real run prints the expiry line: {text}"
    );
    assert_no_token(&text, "the first run");

    let before = label_posts(&server).len();
    let second = cli.run(&refs(&args));
    assert_eq!(
        label_posts(&server).len(),
        before,
        "the second run posts no label: {}",
        second.all()
    );
    assert_no_token(&second.all(), "the second run");

    // A project that already holds two of them gets only the missing three.
    let cli = Cli::new(1);
    let labels = Labels::with(&[TRIGGER_LABEL, "curie-factory:running"]);
    let server = gitlab("19.4.1-ce.0", labels.clone());
    let args = gitlab_args(PROJECT_PATH, &cli.token_file(BOT_TOKEN), &server.base_url);
    let run = cli.run(&refs(&args));
    let mut posted: Vec<String> = label_posts(&server)
        .iter()
        .map(|body| body["name"].as_str().unwrap_or_default().to_string())
        .collect();
    posted.sort();
    assert_eq!(
        posted,
        vec![
            "curie-factory:needs-human".to_string(),
            "curie-factory:pr-open".to_string(),
            "curie-factory:queued".to_string(),
        ],
        "only the missing labels are created: {}",
        run.all()
    );
}

#[test]
fn factory_quickstart_github_refuses_token_file() {
    let cli = Cli::new(0);
    let token = cli.token_file(BOT_TOKEN);
    let run = cli.run(&[
        "--repo",
        "acme/widgets",
        "--code-host",
        "github",
        "--tracker",
        "github",
        "--token-file",
        &token,
        "--dry-run",
        "--json",
    ]);
    let text = run.all();
    assert_eq!(run.code, USAGE_EXIT, "{text}");
    assert!(text.contains("--token-file"), "{text}");
    assert!(!text.contains("not installed"), "{text}");
}

#[test]
fn factory_quickstart_gitlab_pairs_only_with_itself() {
    let cli = Cli::new(0);
    let server = gitlab("19.4.1-ce.0", Labels::default());
    let token = cli.token_file(BOT_TOKEN);
    for (code_host, tracker) in [
        ("gitlab", "github"),
        ("github", "gitlab"),
        ("gitlab", "jira-cloud"),
    ] {
        let run = cli.run(&[
            "--repo",
            PROJECT_PATH,
            "--code-host",
            code_host,
            "--tracker",
            tracker,
            "--token-file",
            &token,
            "--forge-url",
            &server.base_url,
            "--dry-run",
            "--json",
        ]);
        let text = run.all();
        let case = format!("--code-host {code_host} --tracker {tracker}");
        assert_ne!(run.code, 0, "{case}: {text}");
        assert!(text.contains("pairs only with"), "{case}: {text}");
        assert!(text.contains("gitlab"), "{case}: {text}");
        assert!(!text.contains("not installed"), "{case}: {text}");
    }
    assert!(
        server.recorded().is_empty(),
        "a pairing refusal reads nothing from GitLab"
    );
}

#[test]
fn factory_quickstart_gitlab_refuses_incomplete_or_malformed_flags() {
    let cli = Cli::new(0);
    let server = gitlab("19.4.1-ce.0", Labels::default());
    let token = cli.token_file(BOT_TOKEN);
    let valid = gitlab_args(PROJECT_PATH, &token, &server.base_url);
    let without = |flag: &str| -> Vec<String> {
        let mut out = Vec::new();
        let mut skip = false;
        for arg in &valid {
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
    };
    let cases: Vec<(&str, Vec<String>, Vec<&str>)> = vec![
        (
            "missing --token-file",
            without("--token-file"),
            vec!["--token-file"],
        ),
        (
            "plain http to a remote host",
            gitlab_args(PROJECT_PATH, &token, "http://gitlab.example.test"),
            vec!["--forge-url", "https"],
        ),
        (
            "a project path with one segment",
            gitlab_args("widgets", &token, &server.base_url),
            vec!["--repo"],
        ),
    ];
    for (case, args, named) in cases {
        let run = dry_run_json(&cli, &args);
        let text = run.all();
        assert_eq!(run.code, USAGE_EXIT, "{case}: {text}");
        for flag in named {
            assert!(text.contains(flag), "{case}: refusal names {flag}: {text}");
        }
        assert!(!text.contains("not installed"), "{case}: {text}");
        assert_no_token(&text, case);
    }
    assert!(
        server.recorded().is_empty(),
        "a flag refusal reads nothing from GitLab: {:?}",
        read_paths(&server)
    );
    // Liveness: the valid flags plan.
    let run = dry_run_json(&cli, &valid);
    assert_eq!(run.code, 0, "{}", run.all());
}

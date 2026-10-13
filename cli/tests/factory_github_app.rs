//! Integration: `cluster factory` sets up the factory GitHub App from a
//! printed registration link and the App's own key (issue #3746).
//!
//! Drives the built binary against fake helm/kubectl shims on PATH and a
//! local stand-in of GitHub's documented REST API reached through
//! `CURIE_GITHUB_API_URL`:
//! https://docs.github.com/en/rest/apps/apps#get-the-authenticated-app
//! https://docs.github.com/en/rest/apps/apps#list-installations-for-the-authenticated-app
//! https://docs.github.com/en/rest/apps/apps#create-an-installation-access-token-for-an-app
//! https://docs.github.com/en/rest/apps/installations#list-repositories-accessible-to-the-app-installation
//! https://docs.github.com/en/rest/issues/labels
//! https://docs.github.com/en/rest/repos/contents#get-repository-content
//! https://docs.github.com/en/rest/commits/commits#get-a-commit
//! https://docs.github.com/en/rest/users/users#get-a-user
//!
//! No live GitHub and no cluster. PEMs are generated at runtime with openssl
//! so this file never carries key material.

#![cfg(unix)]

use crate::support;

use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::Arc;

use support::{serve, MockServer, Request, Response};

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

const APP_ID: &str = "1234567";
const OTHER_APP_ID: &str = "7654321";
const SLUG: &str = "curie-factory-test";
const INSTALL_TOKEN: &str = "ghs_fake";
const WEBHOOK_SECRET: &str = "whsec-factory-test-3746-value";
/// Exit code for usage errors (cli/src/exit.rs `ExitCode::Usage`).
const USAGE_EXIT: i32 = 2;
const SCOPE_ERROR: &str = "--app-id needs a repository scope: pass --repo OWNER/REPO (repeatable) or --all-installed-repos; nothing was applied";
const SCOPE_FIX: &str = "rerun with --repo <owner>/<name> once per repository the factory may serve, or with --all-installed-repos to allow every repository the GitHub App is installed on";

/// Logs every helm/kubectl/docker invocation to `$SHIM_LOG`.
/// helm: `get values` prints `$FAKE_VALUES`, `get hooks` prints nothing,
/// `upgrade` copies its `-f` values file to `$SHIM_LOG.values`.
/// kubectl: `get secret` prints `$FAKE_SECRET_JSON` or, when that is empty,
/// fails NotFound; `apply -f -` saves stdin to `$SHIM_LOG.apply`; any other
/// kubectl call (rollout, fullname discovery reads) succeeds with an empty list.
const TOOL_SHIM: &str = r#"#!/usr/bin/env bash
tool=$(basename "$0")
echo "$tool $*" >> "$SHIM_LOG"
if [ "$tool" = "docker" ]; then
  echo 'shim: Docker must not be called in this scenario' >&2
  exit 1
fi
if [ "$tool" = "helm" ]; then
  if [ "$1" = "get" ] && [ "$2" = "values" ]; then
    echo "$FAKE_VALUES"
    exit 0
  fi
  if [ "$1" = "get" ] && [ "$2" = "hooks" ]; then
    exit 0
  fi
  if [ "$1" = "upgrade" ]; then
    prev=""
    for a in "$@"; do
      if [ "$prev" = "-f" ] || [ "$prev" = "--values" ]; then
        cat "$a" >> "$SHIM_LOG.values"
      fi
      prev="$a"
    done
    exit 0
  fi
  if [ "$1" = "history" ]; then
    echo '[{"revision":1,"status":"deployed","chart":"curie-0.11.1","app_version":"0.11.1","description":"Install complete"}]'
    exit 0
  fi
  echo "shim: refusing to execute: $tool $*" >&2
  exit 1
fi
if echo " $* " | grep -qE ' get secrets? '; then
  if [ -z "$FAKE_SECRET_JSON" ]; then
    echo 'Error from server (NotFound): secrets "curie-github-app" not found' >&2
    exit 1
  fi
  echo "$FAKE_SECRET_JSON"
  exit 0
fi
if echo " $* " | grep -q ' apply '; then
  cat >> "$SHIM_LOG.apply"
  exit 0
fi
if echo " $* " | grep -q ' get '; then
  echo '{"apiVersion":"v1","kind":"List","items":[]}'
  exit 0
fi
exit 0
"#;

fn generate_rsa_pem(path: &Path) {
    let output = Command::new("openssl")
        .args(["genrsa", "2048"])
        .output()
        .unwrap_or_else(|e| panic!("openssl genrsa: {e}"));
    assert!(output.status.success(), "openssl genrsa failed");
    std::fs::write(path, output.stdout).expect("write generated PEM");
}

fn rsa_public_pem(private_key: &Path, out: &Path) {
    let output = Command::new("openssl")
        .args(["rsa", "-pubout", "-in"])
        .arg(private_key)
        .arg("-out")
        .arg(out)
        .output()
        .unwrap_or_else(|e| panic!("openssl rsa -pubout: {e}"));
    assert!(output.status.success(), "openssl rsa -pubout failed");
}

fn b64(bytes: &[u8]) -> String {
    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, bytes)
}

fn b64_decode(text: &str) -> Vec<u8> {
    base64::Engine::decode(&base64::engine::general_purpose::STANDARD, text.trim())
        .unwrap_or_else(|e| panic!("not base64: {e}: {text}"))
}

/// True when `authorization` is a Bearer RS256 JWT whose signature verifies
/// against `public_key`. Uses openssl, not the CLI's signing library.
fn jwt_signed_by(authorization: &str, public_key: &Path, scratch: &Path) -> bool {
    let Some(token) = authorization.strip_prefix("Bearer ") else {
        return false;
    };
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return false;
    }
    let Ok(sig) =
        base64::Engine::decode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, parts[2])
    else {
        return false;
    };
    let unique = uuid::Uuid::new_v4();
    let input = scratch.join(format!("jwt-{unique}.in"));
    let sig_path = scratch.join(format!("jwt-{unique}.sig"));
    std::fs::write(&input, format!("{}.{}", parts[0], parts[1])).expect("write input");
    std::fs::write(&sig_path, sig).expect("write sig");
    Command::new("openssl")
        .args(["dgst", "-sha256", "-verify"])
        .arg(public_key)
        .arg("-signature")
        .arg(&sig_path)
        .arg(&input)
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

#[derive(Clone, Copy)]
struct GithubConfig {
    installed: bool,
}

#[derive(Clone, Copy)]
enum ToolchainFixture {
    None,
    Python,
    Go,
    WorkflowPython,
    WorkflowGo,
    Java,
    VersionFile,
    MissingVersionFile,
    DeniedVersionFile,
    MalformedVersionFile,
    MissingDiscoveredManifest,
    Denied,
    Malformed,
}

struct Fixture {
    dir: tempfile::TempDir,
    github: MockServer,
    secret_json: String,
    values: String,
    api_prefix: &'static str,
}

fn path_only(req: &Request) -> &str {
    req.path.split('?').next().unwrap_or("")
}

fn query_page(req: &Request) -> u32 {
    req.path
        .split_once('?')
        .map(|(_, q)| q)
        .unwrap_or("")
        .split('&')
        .find_map(|kv| kv.strip_prefix("page="))
        .and_then(|v| v.parse().ok())
        .unwrap_or(1)
}

impl Fixture {
    fn new(config: GithubConfig) -> Self {
        Self::with_toolchain(config, ToolchainFixture::None)
    }

    fn with_toolchain(config: GithubConfig, toolchain: ToolchainFixture) -> Self {
        Self::with_bot_lookup(config, toolchain, 200, r#"{"id":123}"#, "")
    }

    fn with_bot_lookup(
        config: GithubConfig,
        toolchain: ToolchainFixture,
        bot_status: u16,
        bot_body: &'static str,
        api_prefix: &'static str,
    ) -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let shim_dir = dir.path().join("bin");
        std::fs::create_dir(&shim_dir).expect("create shim dir");
        for tool in ["helm", "kubectl", "docker"] {
            let path = shim_dir.join(tool);
            std::fs::write(&path, TOOL_SHIM).expect("write shim");
            let mut perms = std::fs::metadata(&path).expect("stat shim").permissions();
            perms.set_mode(0o755);
            std::fs::set_permissions(&path, perms).expect("chmod shim");
        }
        std::fs::create_dir(dir.path().join("home")).expect("home");
        std::fs::create_dir(dir.path().join("scratch")).expect("scratch");
        generate_rsa_pem(&dir.path().join("app.pem"));
        rsa_public_pem(&dir.path().join("app.pem"), &dir.path().join("app.pub.pem"));
        generate_rsa_pem(&dir.path().join("other.pem"));
        std::fs::write(dir.path().join("webhook-secret"), WEBHOOK_SECRET).expect("webhook");

        // GET /app is decided by the JWT signature, verified with openssl
        // against the public half of app.pem: signed by app.pem -> App
        // 1234567; signed by anything else (e.g. the other App's key stored
        // in an existing Secret) -> 401. Deterministic regardless of the
        // order in which the CLI probes the supplied and the stored key.
        let public_key = Arc::new(dir.path().join("app.pub.pem"));
        let scratch = Arc::new(dir.path().join("scratch"));
        let log = dir.path().join("invocations.log");
        let github = serve(move |req: &Request| {
            let auth = req.header("authorization").unwrap_or("").to_string();
            let path = path_only(req).strip_prefix(api_prefix).unwrap_or("");
            if path.starts_with("/users/") {
                writeln!(
                    std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(&log)
                        .unwrap(),
                    "github {}",
                    req.path
                )
                .unwrap();
                // GitHub's public user endpoint does not require auth:
                // https://docs.github.com/en/rest/users/users#get-a-user
                assert!(req.header("authorization").is_none());
                assert_eq!(path, format!("/users/{SLUG}[bot]"));
                return Response::json(bot_status, bot_body);
            }
            if path.starts_with("/repos/acme/")
                && (path.contains("/contents") || path.contains("/commits/"))
            {
                writeln!(
                    std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(&log)
                        .unwrap(),
                    "github {}",
                    req.path
                )
                .unwrap();
                assert_eq!(auth, format!("Bearer {INSTALL_TOKEN}"));
            }
            match (req.method.as_str(), path) {
                ("GET", "/app") => {
                    if jwt_signed_by(&auth, &public_key, &scratch) {
                        Response::json(
                            200,
                            &format!(r#"{{"id":{APP_ID},"slug":"{SLUG}","name":"{SLUG}"}}"#),
                        )
                    } else {
                        Response::json(
                            401,
                            r#"{"message":"A JSON web token could not be decoded","status":"401"}"#,
                        )
                    }
                }
                ("GET", "/app/installations") => {
                    if config.installed {
                        Response::json(200, r#"[{"id":42,"account":{"login":"acme"}}]"#)
                    } else {
                        Response::json(200, "[]")
                    }
                }
                ("POST", "/app/installations/42/access_tokens") => Response::json(
                    201,
                    &format!(
                        r#"{{"token":"{INSTALL_TOKEN}","expires_at":"2099-01-01T00:00:00Z"}}"#
                    ),
                ),
                ("GET", "/installation/repositories") => {
                    if query_page(req) <= 1 {
                        Response::json(
                            200,
                            r#"{"total_count":2,"repositories":[{"full_name":"acme/bot"},{"full_name":"acme/web"}]}"#,
                        )
                    } else {
                        Response::json(200, r#"{"total_count":2,"repositories":[]}"#)
                    }
                }
                ("GET", "/repos/acme/bot") | ("GET", "/repos/acme/web") => Response::json(200, r#"{"default_branch":"main"}"#),
                ("GET", "/repos/acme/bot/commits/main") | ("GET", "/repos/acme/web/commits/main") => Response::json(200, r#"{"sha":"1111111111111111111111111111111111111111"}"#),
                ("GET", p) if p.starts_with("/repos/acme/") && p.ends_with("/contents") => {
                    match toolchain {
                        ToolchainFixture::Denied => Response::json(403, r#"{"message":"Resource not accessible by integration"}"#),
                        ToolchainFixture::Malformed => Response::json(200, r#"{"unexpected":true}"#),
                        ToolchainFixture::None | ToolchainFixture::WorkflowPython | ToolchainFixture::WorkflowGo | ToolchainFixture::VersionFile | ToolchainFixture::MissingVersionFile | ToolchainFixture::DeniedVersionFile | ToolchainFixture::MalformedVersionFile => Response::json(200, "[]"),
                        ToolchainFixture::Python | ToolchainFixture::MissingDiscoveredManifest => Response::json(200, r#"[{"type":"file","name":"pyproject.toml","path":"pyproject.toml"}]"#),
                        ToolchainFixture::Go => Response::json(200, r#"[{"type":"file","name":"go.mod","path":"go.mod"}]"#),
                        ToolchainFixture::Java => Response::json(200, r#"[{"type":"file","name":"pom.xml","path":"pom.xml"}]"#),
                    }
                }
                ("GET", p) if p.ends_with("/contents/pyproject.toml") => {
                    if matches!(toolchain, ToolchainFixture::MissingDiscoveredManifest) { Response::json(404, r#"{"message":"Not Found"}"#) }
                    else { Response::json(200, &serde_json::json!({"type":"file","encoding":"base64","content":b64(b"[project]\nrequires-python = \"==3.13.2\"\n")}).to_string()) }
                },
                ("GET", p) if p.ends_with("/contents/go.mod") => Response::json(200, &serde_json::json!({"type":"file","encoding":"base64","content":b64(b"module example.com/acme\ngo 1.24.1\n")}).to_string()),
                ("GET", p) if p.ends_with("/contents/pom.xml") => Response::json(200, &serde_json::json!({"type":"file","encoding":"base64","content":b64(b"<project><properties><maven.compiler.release>21</maven.compiler.release></properties></project>")}).to_string()),
                ("GET", p) if p.ends_with("/contents/.github/workflows") && matches!(toolchain, ToolchainFixture::WorkflowPython | ToolchainFixture::WorkflowGo | ToolchainFixture::VersionFile | ToolchainFixture::MissingVersionFile | ToolchainFixture::DeniedVersionFile | ToolchainFixture::MalformedVersionFile) => Response::json(200, r#"[{"type":"file","path":".github/workflows/checks.yml"}]"#),
                ("GET", p) if p.ends_with("/contents/.github/workflows/checks.yml") => {
                    let setup = match toolchain {
                        ToolchainFixture::WorkflowPython => "      - uses: actions/setup-python@v5\n        with:\n          python-version: '3.13'\n",
                        ToolchainFixture::WorkflowGo => "      - uses: actions/setup-go@v5\n        with:\n          go-version: '1.24'\n",
                        _ => "      - uses: actions/setup-python@v5\n        with:\n          python-version-file: config/runtime.python\n",
                    };
                    let workflow = format!("jobs:\n  tests:\n    runs-on: ubuntu-latest\n    steps:\n{setup}");
                    Response::json(200, &serde_json::json!({"type":"file","encoding":"base64","content":b64(workflow.as_bytes())}).to_string())
                }
                ("GET", p) if p.ends_with("/contents/config/runtime.python") => match toolchain {
                    ToolchainFixture::MissingVersionFile => Response::json(404, r#"{"message":"Not Found"}"#),
                    ToolchainFixture::DeniedVersionFile => Response::json(403, r#"{"message":"Resource not accessible by integration"}"#),
                    ToolchainFixture::MalformedVersionFile => Response::json(200, r#"{"unexpected":true}"#),
                    _ => Response::json(200, &serde_json::json!({"type":"file","encoding":"base64","content":b64(b"3.13\n")}).to_string()),
                },
                ("GET", p) if p.starts_with("/repos/acme/") && p.contains("/labels/") => {
                    Response::json(404, r#"{"message":"Not Found"}"#)
                }
                ("POST", "/repos/acme/bot/labels") | ("POST", "/repos/acme/web/labels") => {
                    Response::json(201, r#"{"name":"curie-factory"}"#)
                }
                _ => Response::json(404, r#"{"message":"Not Found (fake)"}"#),
            }
        });
        Self {
            dir,
            github,
            secret_json: String::new(),
            values: r#"{"api":{"environment":"dev"}}"#.to_string(),
            api_prefix,
        }
    }

    fn installed() -> Self {
        Self::new(GithubConfig { installed: true })
    }

    fn record_existing_app(&mut self) {
        self.values = serde_json::json!({
            "api": {
                "environment":"dev",
                "githubAppId":APP_ID,
                "githubAppExistingSecret":"curie-github-app",
                "githubRepoAllowlist":["acme/bot"],
            },
            "factory": {
                "intake":"poll",
                "trackers":[{
                    "name":"github",
                    "kind":"github",
                    "mention":SLUG,
                    "label":"curie-factory",
                }],
            }
        })
        .to_string();
    }

    fn record_repository_bindings(&mut self, tools: &[&str]) {
        self.record_existing_app();
        let mut values: serde_json::Value = serde_json::from_str(&self.values).unwrap();
        let runner = serde_json::json!({
            "tools": tools,
            "image": format!("registry.example.com/acme/runner@sha256:{}", "a".repeat(64)),
            "base": format!("sha256:{}", "b".repeat(64)),
            "platformDigests": {
                "linux/amd64": format!("sha256:{}", "c".repeat(64)),
                "linux/arm64": format!("sha256:{}", "d".repeat(64)),
            },
            "dockerfile": "Dockerfile.runner",
            "probes": [{"name":"custom", "command":["custom", "--version"]}],
        });
        values["factory"]["bindings"] = serde_json::json!([
            {
                "name":"primary", "tracker":"github", "trackerScopeId":"1",
                "agent":"acme-bot", "issuePrefix":"ACME", "operatorMetadata":{"keep":true},
                "repos":[
                    {
                        "alias":"bot", "codeHost":"github", "projectId":"1", "path":"acme/bot",
                        "base":"main", "runner":runner.clone(),
                        "ci":{"required":{"key":"checks", "paths":[".github/workflows/checks.yml"]}},
                        "docker":{"enabled":false, "egressAllowlist":[{"cidr":"192.0.2.0/24", "ports":[{"port":443}]}]},
                        "operatorMetadata":{"repository":"keep"},
                    },
                    {"alias":"web", "codeHost":"github", "projectId":"2", "path":"acme/web", "base":"next", "runner":runner},
                ],
            },
            {"name":"peer", "tracker":"jira", "trackerScopeId":"3", "agent":"acme-peer", "repos":[{"alias":"peer", "codeHost":"github", "path":"acme/peer", "base":"release"}]},
        ]);
        values["agentSandbox"] = serde_json::json!({
            "runner":{"image":"registry.example.com/platform/runner", "digest":format!("sha256:{}", "b".repeat(64))},
            "runnerImages":{"acme-bot":format!("registry.example.com/agent/runner@sha256:{}", "e".repeat(64))},
        });
        self.values = values.to_string();
    }

    fn spec(&self, name: &str, contents: &str) -> String {
        std::fs::write(self.dir.path().join(name), contents).expect("write runner spec");
        self.path(name)
    }

    fn path(&self, name: &str) -> String {
        self.dir.path().join(name).to_string_lossy().into_owned()
    }

    fn read(&self, name: &str) -> String {
        std::fs::read_to_string(self.dir.path().join(name)).unwrap_or_default()
    }

    fn log_path(&self) -> PathBuf {
        self.dir.path().join("invocations.log")
    }

    fn log(&self) -> String {
        std::fs::read_to_string(self.log_path()).unwrap_or_default()
    }

    fn applied(&self) -> String {
        self.read("invocations.log.apply")
    }

    fn values_file(&self) -> String {
        self.read("invocations.log.values")
    }

    fn run(&self, argv: &[&str]) -> Output {
        let mut dirs = vec![self.dir.path().join("bin")];
        if let Some(existing) = std::env::var_os("PATH") {
            dirs.extend(std::env::split_paths(&existing));
        }
        let path = std::env::join_paths(dirs).expect("join PATH");
        Command::new(bin())
            .args(argv)
            .current_dir(
                std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .parent()
                    .unwrap(),
            )
            .env("PATH", path)
            .env("HOME", self.dir.path().join("home"))
            .env("KUBECONFIG", self.dir.path().join("kubeconfig"))
            .env_remove("CURIE_GITHUB_WEBHOOK_SECRET")
            .env(
                "CURIE_GITHUB_API_URL",
                format!("{}{}", self.github.base_url, self.api_prefix),
            )
            .env("FAKE_VALUES", &self.values)
            .env("FAKE_SECRET_JSON", &self.secret_json)
            .env("SHIM_LOG", self.log_path())
            .output()
            .unwrap_or_else(|e| panic!("run curie {}: {e}", argv.join(" ")))
    }

    fn assert_no_mutation(&self) {
        let log = self.log();
        assert!(
            !log.lines().any(|line| line.starts_with("docker ")),
            "no image build may start: {log}"
        );
        assert!(
            !log.lines().any(|l| l.starts_with("helm upgrade")),
            "nothing may be upgraded: {log}"
        );
        assert!(
            !log.lines()
                .any(|l| l.starts_with("kubectl") && l.contains(" apply")),
            "nothing may be applied: {log}"
        );
        assert!(self.applied().is_empty(), "a manifest was applied");
        let posts: Vec<_> = self
            .github
            .recorded()
            .into_iter()
            .filter(|r| r.method == "POST" && r.path.contains("/labels"))
            .collect();
        assert!(posts.is_empty(), "a label was created: {posts:?}");
    }
}

fn combined(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn stdout_json(output: &Output) -> serde_json::Value {
    let stdout = String::from_utf8_lossy(&output.stdout);
    serde_json::from_str(stdout.trim())
        .unwrap_or_else(|e| panic!("stdout must be one JSON object: {e}; stdout: {stdout}"))
}

/// Pull the first registration URL out of human output.
/// The `factory.trackers` entry named `github` in a helm values document.
fn github_tracker(values: &serde_json::Value) -> &serde_json::Value {
    values
        .pointer("/factory/trackers")
        .and_then(|v| v.as_array())
        .and_then(|list| {
            list.iter()
                .find(|t| t.get("name") == Some(&serde_json::json!("github")))
        })
        .unwrap_or_else(|| panic!("no github tracker in factory.trackers: {values}"))
}

fn find_url(text: &str, prefix: &str) -> String {
    let start = text
        .find(prefix)
        .unwrap_or_else(|| panic!("no URL starting {prefix} in output: {text}"));
    text[start..].split_whitespace().next().unwrap().to_string()
}

fn assert_registration_params(url: &str) {
    let query = url.split_once('?').map(|(_, q)| q).unwrap_or("");
    let pairs: Vec<&str> = query.split('&').collect();
    let has = |kv: &str| pairs.contains(&kv);
    let name = pairs
        .iter()
        .find_map(|p| p.strip_prefix("name="))
        .unwrap_or_else(|| panic!("no name= in {url}"));
    assert!(name.starts_with("curie-factory-"), "name: {name}");
    for kv in [
        "public=false",
        "webhook_active=false",
        "metadata=read",
        "contents=write",
        "issues=write",
        "pull_requests=write",
        "checks=read",
        "statuses=write",
        // Failed-job reruns require Actions write permission:
        // https://docs.github.com/en/rest/actions/workflow-runs#re-run-failed-jobs-from-a-workflow-run
        "actions=write",
    ] {
        assert!(has(kv), "registration URL lacks {kv}: {url}");
    }
    assert!(
        !has("actions=read"),
        "registration URL must not request Actions read-only access: {url}"
    );
    assert!(
        !pairs.iter().any(|p| p.starts_with("events")),
        "registration URL must subscribe to no events: {url}"
    );
}

fn happy_argv(fx: &Fixture) -> Vec<String> {
    vec![
        "cluster".into(),
        "factory".into(),
        "--intake".into(),
        "webhook".into(),
        "--app-id".into(),
        APP_ID.into(),
        "--private-key-file".into(),
        fx.path("app.pem"),
        "--all-installed-repos".into(),
        "--webhook-secret-file".into(),
        fx.path("webhook-secret"),
        "--chart".into(),
        "charts/curie".into(),
    ]
}

fn strs(v: &[String]) -> Vec<&str> {
    v.iter().map(String::as_str).collect()
}

#[test]
fn app_credentials_without_repository_scope_are_refused_before_any_external_call() {
    for (dry_run, json) in [(false, false), (true, false), (false, true), (true, true)] {
        let mut fx = Fixture::installed();
        fx.record_existing_app();
        let mut argv = happy_argv(&fx);
        argv.retain(|arg| arg != "--all-installed-repos");
        if dry_run {
            argv.push("--dry-run".into());
        }
        if json {
            argv.push("--json".into());
        }
        let output = fx.run(&strs(&argv));
        let text = combined(&output);
        assert_eq!(output.status.code(), Some(USAGE_EXIT), "{text}");
        if json {
            assert_eq!(
                stdout_json(&output),
                serde_json::json!({"error": SCOPE_ERROR, "fix": SCOPE_FIX})
            );
        } else {
            assert!(text.contains(SCOPE_ERROR), "{text}");
            assert!(text.contains(SCOPE_FIX), "{text}");
        }
        assert!(fx.github.recorded().is_empty(), "GitHub must not be called");
        assert!(fx.log().is_empty(), "no external tool call: {}", fx.log());
        fx.assert_no_mutation();
    }
}

#[test]
fn all_installed_repos_conflicts_with_repo_and_requires_an_app_id() {
    for with_app in [true, false] {
        let fx = Fixture::installed();
        let argv = if with_app {
            let mut argv = happy_argv(&fx);
            argv.extend(["--repo".into(), "acme/bot".into()]);
            argv
        } else {
            vec![
                "cluster".into(),
                "factory".into(),
                "--all-installed-repos".into(),
            ]
        };
        let output = fx.run(&strs(&argv));
        let text = combined(&output);
        assert_eq!(output.status.code(), Some(USAGE_EXIT), "{text}");
        assert!(text.contains("--all-installed-repos"), "{text}");
        assert!(
            text.contains(if with_app { "--repo" } else { "--app-id" }),
            "{text}"
        );
        assert!(!text.contains("unexpected argument"), "{text}");
        assert!(fx.github.recorded().is_empty());
        assert!(fx.log().is_empty(), "{}", fx.log());
        fx.assert_no_mutation();
    }
}

#[test]
fn explicit_repository_scope_only_allowlists_and_labels_the_named_repository() {
    let mut fx = Fixture::installed();
    fx.record_existing_app();
    let mut argv = happy_argv(&fx);
    argv.retain(|arg| arg != "--all-installed-repos");
    argv.extend(["--repo".into(), "acme/bot".into(), "--json".into()]);
    let output = fx.run(&strs(&argv));
    assert!(output.status.success(), "{}", combined(&output));
    let value = stdout_json(&output);
    assert_eq!(
        value["github_app"]["repos"],
        serde_json::json!(["acme/bot"])
    );
    assert_eq!(value["github_app"]["repos_inferred"], false);
    assert_eq!(
        value["github_app"]["labels_created"],
        serde_json::json!(["acme/bot"])
    );
    let values: serde_json::Value = serde_json::from_str(&fx.values_file()).expect("helm values");
    assert_eq!(
        values["api"]["githubRepoAllowlist"],
        serde_json::json!(["acme/bot"])
    );
    let requests = fx.github.recorded();
    assert!(requests.iter().any(|request| {
        request.method == "POST" && path_only(request) == "/repos/acme/bot/labels"
    }));
    assert!(
        requests
            .iter()
            .all(|request| { !path_only(request).starts_with("/repos/acme/web/") }),
        "the unselected repository must not be read or labelled: {requests:?}"
    );
}

#[test]
fn app_dry_run_previews_each_explicit_repository_scope_without_external_calls() {
    for (scope, expected) in [
        (
            vec!["--repo", "acme/bot", "--repo", "acme/web"],
            "# allowlist: acme/bot, acme/web (--repo)",
        ),
        (
            vec!["--all-installed-repos"],
            "# allowlist: every repository the App is installed on (--all-installed-repos)",
        ),
    ] {
        for json in [false, true] {
            let fx = Fixture::installed();
            let mut argv = happy_argv(&fx);
            argv.retain(|arg| arg != "--all-installed-repos");
            argv.extend(scope.iter().map(|arg| (*arg).to_string()));
            argv.push("--dry-run".into());
            if json {
                argv.push("--json".into());
            }
            let output = fx.run(&strs(&argv));
            assert!(output.status.success(), "{}", combined(&output));
            let lines: Vec<String> = if json {
                stdout_json(&output)["plan"]
                    .as_array()
                    .expect("dry-run plan")
                    .iter()
                    .map(|line| line.as_str().expect("plan line").to_string())
                    .collect()
            } else {
                String::from_utf8_lossy(&output.stdout)
                    .lines()
                    .map(str::to_string)
                    .collect()
            };
            let labels = lines
                .iter()
                .position(|line| {
                    line.starts_with("# GET/POST ")
                        && line.ends_with("/labels for each allowlisted repo")
                })
                .expect("labels plan line");
            assert_eq!(lines.get(labels + 1).map(String::as_str), Some(expected));
            assert!(fx.github.recorded().is_empty());
            assert!(fx.log().is_empty(), "{}", fx.log());
            fx.assert_no_mutation();
        }
    }
}

fn repository_argv() -> Vec<String> {
    ["cluster", "factory", "--chart", "charts/curie", "--json"]
        .into_iter()
        .map(str::to_string)
        .collect()
}

#[test]
fn factory_repository_runner_dry_run_previews_builds_bindings_and_complete_receipts() {
    for json in [false, true] {
        let fx = Fixture::installed();
        let bot = fx.spec(
            "bot.yaml",
            "tools: [uv, docker-cli]\ndockerfile: Dockerfile.runner\nprobes:\n  - name: custom\n    command: [custom, --version]\n",
        );
        fx.spec(
            "Dockerfile.runner",
            "ARG CURIE_RUNNER_IMAGE\nFROM ${CURIE_RUNNER_IMAGE}\n",
        );
        let web = fx.spec("web.yaml", "tools: [pnpm]\n");
        let mut argv = repository_argv();
        if !json {
            argv.retain(|arg| arg != "--json");
        }
        argv.extend([
            "--repo-runner".into(),
            format!("acme/bot={bot}"),
            "--repo-runner".into(),
            format!("acme/web={web}"),
            "--registry".into(),
            "registry.example.com/acme".into(),
            "--repo-docker".into(),
            "acme/bot".into(),
            "--dry-run".into(),
        ]);
        let output = fx.run(&strs(&argv));
        assert!(output.status.success(), "{}", combined(&output));
        let plan = if json {
            stdout_json(&output)["plan"]
                .as_array()
                .expect("dry-run plan")
                .iter()
                .map(|line| line.as_str().expect("plan line"))
                .collect::<Vec<_>>()
                .join("\n")
        } else {
            String::from_utf8_lossy(&output.stdout).into_owned()
        };
        for expected in [
            "acme/bot",
            "acme/web",
            &bot,
            &web,
            "registry.example.com/acme",
            "base",
            "platformDigests",
            "docker",
        ] {
            assert!(plan.contains(expected), "missing {expected:?}: {plan}");
        }
        assert!(plan.contains("build"), "{plan}");
        assert!(fx.github.recorded().is_empty());
        assert!(fx.log().is_empty(), "dry-run external call: {}", fx.log());
        fx.assert_no_mutation();
    }
}

#[test]
fn factory_repository_runner_local_input_errors_are_precise_and_offline() {
    for case in ["malformed", "wildcard", "empty-spec", "duplicate", "unknown-tool"] {
        let fx = Fixture::installed();
        let valid = fx.spec("valid.yaml", "tools: [uv]\n");
        let invalid = fx.spec("invalid.yaml", "tools: [not-a-catalog-tool]\n");
        let request = match case {
            "malformed" => "acme/bot".into(),
            "wildcard" => format!("acme/*={valid}"),
            "empty-spec" => "acme/bot=".into(),
            "unknown-tool" => format!("acme/bot={invalid}"),
            _ => format!("acme/bot={valid}"),
        };
        let mut argv = repository_argv();
        argv.extend([
            "--repo-runner".into(),
            request.clone(),
            "--registry".into(),
            "registry.example.com/acme".into(),
            "--dry-run".into(),
        ]);
        if case == "duplicate" {
            argv.extend(["--repo-runner".into(), request]);
        }
        let output = fx.run(&strs(&argv));
        let text = combined(&output);
        assert_eq!(output.status.code(), Some(USAGE_EXIT), "{case}: {text}");
        assert!(!text.contains("unexpected argument"), "{case}: {text}");
        match case {
            "unknown-tool" => assert!(
                text.contains("unknown tool") && text.contains("not-a-catalog-tool"),
                "{text}"
            ),
            "duplicate" => assert!(
                text.contains("acme/bot")
                    && (text.contains("more than once") || text.contains("duplicate")),
                "{text}"
            ),
            _ => assert!(text.contains("OWNER/REPO"), "{case}: {text}"),
        }
        assert!(fx.github.recorded().is_empty());
        assert!(fx.log().is_empty(), "{case}: {}", fx.log());
        fx.assert_no_mutation();
    }
}

#[test]
fn factory_repository_targets_must_resolve_one_recorded_binding_before_builds() {
    for case in ["unknown", "unbound", "ambiguous"] {
        let mut fx = Fixture::installed();
        fx.record_repository_bindings(&["uv", "docker-cli"]);
        let mut values: serde_json::Value = serde_json::from_str(&fx.values).unwrap();
        let target = if case == "unknown" { "acme/missing" } else { "acme/bot" };
        if case == "unbound" {
            values["factory"]["bindings"] = serde_json::json!([]);
        } else if case == "ambiguous" {
            values["factory"]["bindings"][1]["repos"] =
                serde_json::json!([values["factory"]["bindings"][0]["repos"][0].clone()]);
        }
        fx.values = values.to_string();
        let spec = fx.spec("runner.yaml", "tools: [uv]\n");
        let mut argv = repository_argv();
        argv.extend([
            "--repo-runner".into(),
            format!("{target}={spec}"),
            "--registry".into(),
            "registry.example.com/acme".into(),
        ]);
        let output = fx.run(&strs(&argv));
        let text = combined(&output);
        assert_eq!(output.status.code(), Some(USAGE_EXIT), "{case}: {text}");
        assert!(!text.contains("unexpected argument"), "{text}");
        assert!(text.contains(target), "{case}: {text}");
        assert!(
            if case == "ambiguous" {
                text.contains("ambiguous") || text.contains("more than one")
            } else {
                text.contains("bound") || text.contains("binding")
            },
            "{case}: {text}"
        );
        fx.assert_no_mutation();
    }
}

#[test]
fn factory_invalid_second_repository_spec_never_starts_the_first_build() {
    let mut fx = Fixture::installed();
    fx.record_repository_bindings(&["uv", "docker-cli"]);
    let valid = fx.spec("valid.yaml", "tools: [uv]\n");
    let invalid = fx.spec("invalid.yaml", "tools: [not-a-catalog-tool]\n");
    let mut argv = repository_argv();
    argv.extend([
        "--repo-runner".into(),
        format!("acme/bot={valid}"),
        "--repo-runner".into(),
        format!("acme/web={invalid}"),
        "--registry".into(),
        "registry.example.com/acme".into(),
    ]);
    let output = fx.run(&strs(&argv));
    let text = combined(&output);
    assert_eq!(output.status.code(), Some(USAGE_EXIT), "{text}");
    assert!(text.contains("not-a-catalog-tool"), "{text}");
    fx.assert_no_mutation();
}

#[test]
fn factory_repository_docker_requires_catalog_cli_and_replacement_keeps_it() {
    for replacement in [false, true] {
        let mut fx = Fixture::installed();
        fx.record_repository_bindings(if replacement { &["docker-cli"] } else { &["uv"] });
        let mut values: serde_json::Value = serde_json::from_str(&fx.values).unwrap();
        // A repository probe named docker does not select the catalog CLI.
        values["factory"]["bindings"][0]["repos"][0]["runner"]["probes"] =
            serde_json::json!([{"name":"docker", "command":["docker", "--version"]}]);
        values["factory"]["bindings"][0]["repos"][0]["docker"]["enabled"] =
            serde_json::json!(replacement);
        fx.values = values.to_string();
        let mut argv = repository_argv();
        if replacement {
            let spec = fx.spec("runner.yaml", "tools: [uv]\n");
            argv.extend([
                "--repo-runner".into(),
                format!("acme/bot={spec}"),
                "--registry".into(),
                "registry.example.com/acme".into(),
            ]);
        } else {
            argv.extend(["--repo-docker".into(), "acme/bot".into()]);
        }
        let output = fx.run(&strs(&argv));
        let text = combined(&output);
        assert_eq!(output.status.code(), Some(USAGE_EXIT), "{text}");
        assert!(text.contains("acme/bot") && text.contains("docker-cli"), "{text}");
        fx.assert_no_mutation();
    }
}

#[test]
fn factory_docker_only_preserves_complete_runner_receipt_binding_metadata_and_peers() {
    let mut fx = Fixture::installed();
    fx.record_repository_bindings(&["uv", "docker-cli"]);
    let before: serde_json::Value = serde_json::from_str(&fx.values).unwrap();
    let mut expected = before["factory"]["bindings"].clone();
    expected[0]["repos"][0]["docker"]["enabled"] = serde_json::json!(true);
    let mut argv = repository_argv();
    argv.extend(["--repo-docker".into(), "acme/bot".into()]);
    let output = fx.run(&strs(&argv));
    assert!(output.status.success(), "{}", combined(&output));
    let applied: serde_json::Value = serde_json::from_str(&fx.values_file()).expect("Helm values");
    assert_eq!(applied["factory"]["bindings"], expected);
    let log = fx.log();
    assert_eq!(log.lines().filter(|line| line.starts_with("helm upgrade")).count(), 1, "{log}");
    assert!(!log.lines().any(|line| line.starts_with("docker ")), "{log}");
    assert!(log.contains("rollout restart deployment/curie-worker"), "{log}");
}

#[test]
fn disabling_factory_without_app_credentials_keeps_working_without_a_repository_scope() {
    let fx = Fixture::installed();
    let output = fx.run(&[
        "cluster",
        "factory",
        "--disable",
        "--chart",
        "charts/curie",
        "--json",
    ]);
    assert!(output.status.success(), "{}", combined(&output));
    assert_eq!(stdout_json(&output)["factory_intake_enabled"], false);
    let values: serde_json::Value = serde_json::from_str(&fx.values_file()).expect("helm values");
    assert_eq!(values["factory"], serde_json::json!({"enabled": false}));
    assert!(values["api"].get("githubRepoAllowlist").is_none());
    assert!(fx.github.recorded().is_empty());
}

#[test]
fn factory_app_bot_identity_is_resolved_before_helm_and_applied() {
    for prefix in ["", "/api/v3"] {
        let fx = Fixture::with_bot_lookup(
            GithubConfig { installed: true },
            ToolchainFixture::None,
            200,
            r#"{"id":123}"#,
            prefix,
        );
        let output = fx.run(&strs(&happy_argv(&fx)));
        assert!(output.status.success(), "{}", combined(&output));
        let values: serde_json::Value = serde_json::from_str(&fx.values_file()).unwrap();
        assert_eq!(
            values.pointer("/worker/publication/gitUserName"),
            Some(&serde_json::json!(format!("{SLUG}[bot]")))
        );
        assert_eq!(
            values.pointer("/worker/publication/gitUserEmail"),
            Some(&serde_json::json!(format!(
                "123+{SLUG}[bot]@users.noreply.github.com"
            )))
        );
        let log = fx.log();
        assert!(
            log.find("/users/").expect("bot lookup") < log.find("helm ").expect("helm call"),
            "bot lookup must precede Helm: {log}"
        );
    }
}

#[test]
fn factory_app_keeps_each_recorded_operator_publication_identity() {
    for publication in [
        serde_json::json!({"gitUserName":"Operator"}),
        serde_json::json!({"gitUserEmail":"operator@example.com"}),
        serde_json::json!({"gitUserName":"Operator","gitUserEmail":"operator@example.com"}),
    ] {
        let mut fx = Fixture::installed();
        fx.values = serde_json::json!({
            "api":{"environment":"dev"},
            "worker":{"publication":publication},
        })
        .to_string();
        let output = fx.run(&strs(&happy_argv(&fx)));
        assert!(output.status.success(), "{}", combined(&output));
        let values: serde_json::Value = serde_json::from_str(&fx.values_file()).unwrap();
        assert!(
            values.pointer("/worker/publication").is_none(),
            "reuse-values must keep operator identity without either inferred field: {values}"
        );
    }
}

#[test]
fn factory_app_keeps_numeric_and_empty_operator_publication_values() {
    for publication in [
        serde_json::json!({"gitUserName":123}),
        serde_json::json!({"gitUserName":""}),
        serde_json::json!({"gitUserEmail":""}),
    ] {
        let mut fx = Fixture::installed();
        fx.values = serde_json::json!({
            "api":{"environment":"dev"},
            "worker":{"publication":publication},
        })
        .to_string();
        let output = fx.run(&strs(&happy_argv(&fx)));
        assert!(output.status.success(), "{}", combined(&output));
        let values: serde_json::Value = serde_json::from_str(&fx.values_file()).unwrap();
        assert!(
            values.pointer("/worker/publication").is_none(),
            "reuse-values must preserve every explicit operator value: {values}"
        );
    }
}

#[test]
fn factory_app_bot_lookup_failure_exits_three_before_any_helm_call() {
    for (status, body) in [
        // An invalid HTTP status line exercises the transport error path.
        (0, "{}"),
        (404, r#"{"message":"Not Found"}"#),
        (503, r#"{"message":"Unavailable"}"#),
        (200, "not JSON"),
        (200, r#"{"login":"acme[bot]"}"#),
        (200, r#"{"id":"123"}"#),
        (200, r#"{"id":-1}"#),
    ] {
        let fx = Fixture::with_bot_lookup(
            GithubConfig { installed: true },
            ToolchainFixture::None,
            status,
            body,
            "",
        );
        let output = fx.run(&strs(&happy_argv(&fx)));
        let text = combined(&output);
        assert_eq!(output.status.code(), Some(3), "{text}");
        assert!(text.contains(&format!("/users/{SLUG}[bot]")), "{text}");
        assert!(
            text.contains("--set worker.publication.gitUserEmail="),
            "{text}"
        );
        assert!(
            !fx.log().lines().any(|line| line.starts_with("helm ")),
            "{}",
            fx.log()
        );
        fx.assert_no_mutation();
    }
}

#[test]
fn factory_existing_app_mention_resolves_public_bot_identity_before_helm() {
    let mut fx = Fixture::installed();
    fx.record_existing_app();
    let output = fx.run(&[
        "cluster",
        "factory",
        "--mention",
        SLUG,
        "--intake",
        "poll",
        "--chart",
        "charts/curie",
    ]);
    assert!(output.status.success(), "{}", combined(&output));
    let values: serde_json::Value = serde_json::from_str(&fx.values_file()).unwrap();
    assert_eq!(
        values.pointer("/worker/publication/gitUserName"),
        Some(&serde_json::json!(format!("{SLUG}[bot]")))
    );
    assert_eq!(
        values.pointer("/worker/publication/gitUserEmail"),
        Some(&serde_json::json!(format!(
            "123+{SLUG}[bot]@users.noreply.github.com"
        )))
    );
    let log = fx.log();
    assert!(
        log.find("/users/").expect("bot lookup") < log.find("helm ").expect("helm call"),
        "bot lookup must precede Helm: {log}"
    );
    let requests = fx.github.recorded();
    assert_eq!(requests.len(), 1, "only the public user lookup is needed");
    assert_eq!(requests[0].path, format!("/users/{SLUG}[bot]"));
    assert!(requests[0].header("authorization").is_none());
}

#[test]
fn factory_existing_app_mention_bot_lookup_failure_never_calls_helm() {
    for (status, body) in [
        (404, r#"{"message":"Not Found"}"#),
        (503, r#"{"message":"Unavailable"}"#),
        (200, r#"{"id":"123"}"#),
    ] {
        let mut fx = Fixture::with_bot_lookup(
            GithubConfig { installed: true },
            ToolchainFixture::None,
            status,
            body,
            "",
        );
        fx.record_existing_app();
        let output = fx.run(&[
            "cluster",
            "factory",
            "--mention",
            SLUG,
            "--intake",
            "poll",
            "--chart",
            "charts/curie",
        ]);
        let text = combined(&output);
        assert_eq!(output.status.code(), Some(3), "{text}");
        assert!(text.contains(&format!("/users/{SLUG}[bot]")), "{text}");
        assert!(
            text.contains("--set worker.publication.gitUserEmail="),
            "{text}"
        );
        assert!(
            !fx.log().lines().any(|line| line.starts_with("helm ")),
            "{}",
            fx.log()
        );
        fx.assert_no_mutation();
    }
}

#[test]
fn factory_existing_app_mention_keeps_recorded_operator_identity() {
    for publication in [
        serde_json::json!({"gitUserName":"Operator"}),
        serde_json::json!({"gitUserEmail":"operator@example.com"}),
        serde_json::json!({"gitUserName":123}),
        serde_json::json!({"gitUserEmail":""}),
    ] {
        let mut fx = Fixture::installed();
        fx.record_existing_app();
        let mut recorded: serde_json::Value = serde_json::from_str(&fx.values).unwrap();
        recorded["worker"] = serde_json::json!({"publication":publication});
        fx.values = recorded.to_string();
        let output = fx.run(&[
            "cluster",
            "factory",
            "--mention",
            SLUG,
            "--intake",
            "poll",
            "--chart",
            "charts/curie",
        ]);
        assert!(output.status.success(), "{}", combined(&output));
        let values: serde_json::Value = serde_json::from_str(&fx.values_file()).unwrap();
        assert!(
            values.pointer("/worker/publication").is_none(),
            "explicit operator identity must remain unchanged: {values}"
        );
        let log = fx.log();
        assert!(
            log.find("/users/").expect("bot lookup") < log.find("helm ").expect("helm call"),
            "bot lookup must precede Helm even for overrides: {log}"
        );
    }
}

#[test]
fn without_an_app_the_registration_link_is_printed_and_nothing_applied() {
    let fx = Fixture::installed();
    let output = fx.run(&["cluster", "factory", "--chart", "charts/curie"]);
    let text = combined(&output);
    assert_eq!(output.status.code(), Some(0), "output: {text}");
    let url = find_url(&text, "https://github.com/settings/apps/new");
    assert_registration_params(&url);
    let actions: Vec<&str> = url
        .split_once('?')
        .map(|(_, query)| query)
        .unwrap_or("")
        .split('&')
        .filter(|pair| pair.starts_with("actions="))
        .collect();
    assert_eq!(
        actions,
        ["actions=write"],
        "registration URL must request Actions write exactly once: {url}"
    );
    let lower = text.to_lowercase();
    for step in ["1.", "2.", "3.", "4."] {
        assert!(text.contains(step), "four numbered steps expected: {text}");
    }
    assert!(
        lower.contains("app id"),
        "steps must mention the App ID: {text}"
    );
    assert!(
        lower.contains("private key"),
        "steps must mention the private key: {text}"
    );
    assert!(
        lower.contains("install"),
        "steps must mention installing: {text}"
    );
    let rerun = text
        .lines()
        .find_map(|line| line.strip_prefix("4. Rerun: "))
        .unwrap_or_else(|| panic!("steps must give the rerun command: {text}"));
    assert_eq!(
        rerun,
        "curie cluster factory --intake poll --app-id <APP_ID> --private-key-file <PATH.pem> --repo <OWNER/REPO>"
    );
    fx.assert_no_mutation();

    let output = fx.run(&["cluster", "factory", "--chart", "charts/curie", "--json"]);
    assert_eq!(
        output.status.code(),
        Some(0),
        "output: {}",
        combined(&output)
    );
    let value = stdout_json(&output);
    let url = value
        .get("github_app_registration_url")
        .and_then(|v| v.as_str())
        .unwrap_or_else(|| panic!("--json must carry github_app_registration_url: {value}"));
    assert!(
        url.starts_with("https://github.com/settings/apps/new"),
        "{url}"
    );
    assert_registration_params(url);
    fx.assert_no_mutation();
}

#[test]
fn factory_toolchain_preflight_reads_the_app_repository_before_any_helm_call() {
    for (signal, expected) in [
        (ToolchainFixture::Python, "Python 3.13.2"),
        (ToolchainFixture::Go, "Go 1.24.1"),
        (ToolchainFixture::None, "no toolchain signals"),
    ] {
        let fx = Fixture::with_toolchain(GithubConfig { installed: true }, signal);
        let argv = happy_argv(&fx);
        let output = fx.run(&strs(&argv));
        assert!(output.status.success(), "{}", combined(&output));
        assert!(
            combined(&output).contains(expected),
            "{}",
            combined(&output)
        );
        let log = fx.log();
        let contents = log
            .find("github /repos/acme/bot/contents")
            .expect("contents read");
        let helm = log.find("helm ").expect("helm read");
        assert!(
            contents < helm,
            "repository inference must precede Helm: {log}"
        );
        assert!(
            fx.github
                .recorded()
                .iter()
                .filter(|r| r.path.contains("/contents"))
                .all(|r| r
                    .path
                    .contains("ref=1111111111111111111111111111111111111111")),
            "contents must share the default branch commit"
        );
    }
}

#[test]
fn factory_toolchain_auth_and_malformed_reads_fail_before_helm_and_never_claim_no_signals() {
    for signal in [
        ToolchainFixture::Denied,
        ToolchainFixture::Malformed,
        ToolchainFixture::DeniedVersionFile,
        ToolchainFixture::MalformedVersionFile,
        ToolchainFixture::MissingDiscoveredManifest,
    ] {
        let fx = Fixture::with_toolchain(GithubConfig { installed: true }, signal);
        let argv = happy_argv(&fx);
        let output = fx.run(&strs(&argv));
        assert!(!output.status.success(), "{}", combined(&output));
        assert!(!combined(&output).contains("no toolchain signals"));
        assert!(!fx.log().contains("helm "), "{}", fx.log());
        fx.assert_no_mutation();
    }
}

#[test]
fn factory_toolchain_contents_api_reads_workflows_java_and_declared_version_files() {
    for (signal, expected, source, warning) in [
        (
            ToolchainFixture::WorkflowPython,
            "Python 3.13",
            ".github/workflows/checks.yml",
            false,
        ),
        (
            ToolchainFixture::WorkflowGo,
            "Go 1.24",
            ".github/workflows/checks.yml",
            true,
        ),
        (ToolchainFixture::Java, "Java 21", "pom.xml", true),
        (
            ToolchainFixture::VersionFile,
            "Python 3.13",
            "config/runtime.python",
            false,
        ),
    ] {
        let fx = Fixture::with_toolchain(GithubConfig { installed: true }, signal);
        let argv = happy_argv(&fx);
        let output = fx.run(&strs(&argv));
        assert!(output.status.success(), "{}", combined(&output));
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains(expected) && stderr.contains(source),
            "{stderr}"
        );
        assert_eq!(
            stderr.contains("fixed factory runner lacks"),
            warning,
            "{stderr}"
        );
        let log = fx.log();
        let read = log
            .find(&format!("contents/{source}"))
            .expect("declaring file read through Contents API");
        let helm = log.find("helm ").expect("Helm read after preflight");
        assert!(read < helm, "inference must precede Helm: {log}");
        assert!(fx
            .github
            .recorded()
            .iter()
            .filter(|request| request.path.contains("/contents"))
            .all(|request| request
                .path
                .contains("ref=1111111111111111111111111111111111111111")));
    }
}

#[test]
fn missing_declared_version_file_warns_with_tool_and_path_then_continues() {
    let fx = Fixture::with_toolchain(
        GithubConfig { installed: true },
        ToolchainFixture::MissingVersionFile,
    );
    let argv = happy_argv(&fx);
    let output = fx.run(&strs(&argv));
    assert!(output.status.success(), "{}", combined(&output));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Python unknown (version file config/runtime.python)"),
        "{stderr}"
    );
    assert!(
        stderr.contains(".github/workflows/checks.yml")
            && stderr.contains("fixed factory runner lacks"),
        "{stderr}"
    );
    assert!(!stderr.contains("no toolchain signals"), "{stderr}");
    let log = fx.log();
    let missing_read = log
        .find("contents/config/runtime.python")
        .expect("declared file lookup");
    let helm = log
        .find("helm ")
        .expect("missing version file must still allow Helm");
    assert!(missing_read < helm, "{log}");
}

#[test]
fn quickstart_second_pass_infers_before_context_pin_or_any_helm_call() {
    let fx = Fixture::with_toolchain(GithubConfig { installed: true }, ToolchainFixture::Python);
    // The named context is intentionally unavailable. Inference must still
    // run first; the context failure must prevent all subsequent Helm calls.
    let output = fx.run(&[
        "factory",
        "quickstart",
        "--repo",
        "acme/bot",
        "--context",
        "missing-context",
        "--app-id",
        APP_ID,
        "--private-key-file",
        &fx.path("app.pem"),
        "--chart",
        "charts/curie",
        "--json",
    ]);
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("Python 3.13.2"),
        "{}",
        combined(&output)
    );
    assert!(
        stdout_json(&output).to_string().contains("missing-context"),
        "{}",
        combined(&output)
    );
    assert!(!fx.log().contains("helm "), "{}", fx.log());
    fx.assert_no_mutation();
}

#[test]
fn quickstart_second_pass_contents_failure_never_reaches_helm() {
    let fx = Fixture::with_toolchain(GithubConfig { installed: true }, ToolchainFixture::Denied);
    let output = fx.run(&[
        "factory",
        "quickstart",
        "--repo",
        "acme/bot",
        "--context",
        "missing-context",
        "--app-id",
        APP_ID,
        "--private-key-file",
        &fx.path("app.pem"),
        "--chart",
        "charts/curie",
        "--json",
    ]);
    assert!(!output.status.success());
    assert!(
        stdout_json(&output).to_string().contains("HTTP 403"),
        "{}",
        combined(&output)
    );
    assert!(!combined(&output).contains("no toolchain signals"));
    assert!(!fx.log().contains("helm "), "{}", fx.log());
    fx.assert_no_mutation();
}

#[test]
fn factory_without_app_notes_skip_and_dry_run_never_calls_github() {
    let fx = Fixture::installed();
    let output = fx.run(&["cluster", "factory", "--dry-run", "--json"]);
    assert!(output.status.success(), "{}", combined(&output));
    assert!(stdout_json(&output)
        .to_string()
        .contains("toolchain inference skipped: no --app-id"));
    assert!(fx.github.recorded().is_empty());
    let argv = happy_argv(&fx);
    let mut args = strs(&argv);
    args.extend(["--dry-run", "--json"]);
    let output = fx.run(&args);
    assert!(output.status.success(), "{}", combined(&output));
    assert!(stdout_json(&output)
        .to_string()
        .contains("infer repository toolchains before Helm"));
    assert!(fx.github.recorded().is_empty());
}

#[test]
fn org_flag_uses_the_organization_registration_form() {
    let fx = Fixture::installed();
    let output = fx.run(&[
        "cluster",
        "factory",
        "--org",
        "acme",
        "--chart",
        "charts/curie",
    ]);
    let text = combined(&output);
    assert_eq!(output.status.code(), Some(0), "output: {text}");
    let url = find_url(
        &text,
        "https://github.com/organizations/acme/settings/apps/new",
    );
    assert_registration_params(&url);
    fx.assert_no_mutation();
}

#[test]
fn app_id_and_key_infer_mention_allowlist_label_and_store_the_key_in_a_secret() {
    let fx = Fixture::installed();
    let argv = happy_argv(&fx);
    let output = fx.run(&strs(&argv));
    let text = combined(&output);
    assert_eq!(output.status.code(), Some(0), "output: {text}");
    assert!(
        text.contains(SLUG),
        "output must announce the mention: {text}"
    );
    assert!(
        text.contains("acme/bot") && text.contains("acme/web"),
        "output must announce the allowlist: {text}"
    );
    assert!(
        text.contains(" (the App's installed repositories)"),
        "{text}"
    );
    assert!(
        text.contains("curie-factory"),
        "output must announce the label: {text}"
    );

    let recorded = fx.github.recorded();
    let app = recorded
        .iter()
        .find(|r| r.method == "GET" && path_only(r) == "/app")
        .unwrap_or_else(|| panic!("GET /app never called: {recorded:?}"));
    assert!(
        app.header("authorization")
            .unwrap_or("")
            .starts_with("Bearer "),
        "GET /app must use a Bearer JWT"
    );
    for repo in ["acme/bot", "acme/web"] {
        let post = recorded
            .iter()
            .find(|r| r.method == "POST" && path_only(r) == format!("/repos/{repo}/labels"))
            .unwrap_or_else(|| panic!("label not created in {repo}: {recorded:?}"));
        assert!(
            post.header("authorization")
                .unwrap_or("")
                .contains(INSTALL_TOKEN),
            "label POST must use the installation token"
        );
        let body: serde_json::Value = serde_json::from_slice(&post.body).expect("label body");
        assert_eq!(
            body.get("name").and_then(|v| v.as_str()),
            Some("curie-factory")
        );
    }

    let applied = fx.applied();
    assert!(
        !applied.trim().is_empty(),
        "no Secret applied via stdin; log: {}",
        fx.log()
    );
    // The CLI has no YAML dependency; the manifest is expected as JSON,
    // which `kubectl apply -f -` accepts.
    let manifest: serde_json::Value = serde_json::from_str(&applied)
        .unwrap_or_else(|e| panic!("applied manifest unparseable: {e}: {applied}"));
    assert_eq!(
        manifest.get("kind").and_then(|v| v.as_str()),
        Some("Secret")
    );
    assert_eq!(
        manifest.pointer("/metadata/name").and_then(|v| v.as_str()),
        Some("curie-github-app")
    );
    assert_eq!(
        manifest
            .pointer("/metadata/annotations/curie.ai~1github-app-id")
            .and_then(|v| v.as_str()),
        Some(APP_ID)
    );
    let pem = fx.read("app.pem");
    let stored = manifest
        .pointer("/data/privateKey")
        .and_then(|v| v.as_str())
        .map(|s| String::from_utf8(b64_decode(s)).expect("utf8"))
        .or_else(|| {
            manifest
                .pointer("/stringData/privateKey")
                .and_then(|v| v.as_str())
                .map(str::to_string)
        })
        .unwrap_or_else(|| panic!("Secret lacks privateKey: {manifest}"));
    assert_eq!(
        stored.trim(),
        pem.trim(),
        "Secret must hold the supplied PEM"
    );

    let values_text = fx.values_file();
    let values: serde_json::Value = serde_json::from_str(&values_text)
        .unwrap_or_else(|e| panic!("helm values file unparseable: {e}: {values_text}"));
    let api = values
        .get("api")
        .unwrap_or_else(|| panic!("no api in {values}"));
    let tracker = github_tracker(&values);
    assert_eq!(tracker.get("mention"), Some(&serde_json::json!(SLUG)));
    assert_eq!(
        tracker.get("label"),
        Some(&serde_json::json!("curie-factory"))
    );
    let mut allow: Vec<String> = api
        .get("githubRepoAllowlist")
        .and_then(|v| v.as_array())
        .unwrap_or_else(|| panic!("no allowlist: {api}"))
        .iter()
        .filter_map(|v| v.as_str().map(str::to_string))
        .collect();
    allow.sort();
    assert_eq!(allow, vec!["acme/bot", "acme/web"]);
    assert_eq!(
        api.get("githubAppId"),
        Some(&serde_json::json!(APP_ID)),
        "string app id"
    );
    assert_eq!(
        api.get("githubAppExistingSecret"),
        Some(&serde_json::json!("curie-github-app"))
    );

    let log = fx.log();
    let body_line = pem
        .lines()
        .find(|l| !l.starts_with("-----") && l.len() > 20)
        .expect("pem body line");
    assert!(!log.contains(body_line), "PEM body leaked into argv: {log}");
    assert!(
        !log.contains(WEBHOOK_SECRET),
        "webhook secret leaked into argv: {log}"
    );

    let fx = Fixture::installed();
    let mut argv = happy_argv(&fx);
    argv.push("--json".into());
    let output = fx.run(&strs(&argv));
    assert!(output.status.success(), "{}", combined(&output));
    let value = stdout_json(&output);
    assert_eq!(value["github_app"]["repos_inferred"], true);
    assert_eq!(
        value["github_app"]["repos"],
        serde_json::json!(["acme/bot", "acme/web"])
    );
    assert_eq!(
        value["github_app"]["labels_created"],
        serde_json::json!(["acme/bot", "acme/web"])
    );
}

#[test]
fn printed_poll_completion_configures_intake_without_a_webhook_secret() {
    let fx = Fixture::installed();
    let output = fx.run(&[
        "cluster",
        "factory",
        "--intake",
        "poll",
        "--app-id",
        APP_ID,
        "--private-key-file",
        &fx.path("app.pem"),
        "--repo",
        "acme/bot",
        "--chart",
        "charts/curie",
    ]);
    assert_eq!(
        output.status.code(),
        Some(0),
        "output: {}",
        combined(&output)
    );
    let values: serde_json::Value = serde_json::from_str(&fx.values_file()).expect("helm values");
    assert_eq!(
        values.pointer("/factory/intake"),
        Some(&serde_json::json!("poll"))
    );
    assert!(values.pointer("/api/githubWebhookSecret").is_none());
    assert!(fx.log().contains("helm upgrade"));
    assert!(!fx.applied().is_empty());
}

#[test]
fn webhook_completion_without_a_secret_is_refused_before_any_mutation() {
    let fx = Fixture::installed();
    let output = fx.run(&[
        "cluster",
        "factory",
        "--intake",
        "webhook",
        "--app-id",
        APP_ID,
        "--private-key-file",
        &fx.path("app.pem"),
        "--repo",
        "acme/bot",
        "--chart",
        "charts/curie",
    ]);
    assert_eq!(
        output.status.code(),
        Some(USAGE_EXIT),
        "output: {}",
        combined(&output)
    );
    assert!(combined(&output).contains("GITHUB_WEBHOOK_SECRET"));
    fx.assert_no_mutation();
}

#[test]
fn unsupported_intake_is_a_usage_error_before_any_external_call() {
    let fx = Fixture::installed();
    let output = fx.run(&["cluster", "factory", "--intake", "push"]);
    assert_eq!(
        output.status.code(),
        Some(USAGE_EXIT),
        "output: {}",
        combined(&output)
    );
    fx.assert_no_mutation();
    assert!(fx.github.recorded().is_empty());
}

#[test]
fn an_app_that_is_not_installed_is_refused_before_any_mutation() {
    let fx = Fixture::new(GithubConfig { installed: false });
    let argv = happy_argv(&fx);
    let output = fx.run(&strs(&argv));
    let text = combined(&output);
    assert_ne!(output.status.code(), Some(0), "output: {text}");
    assert!(
        text.contains("not installed"),
        "refusal must say not installed: {text}"
    );
    fx.assert_no_mutation();
}

#[test]
fn a_repo_the_app_is_not_installed_on_is_refused_naming_installed_repos() {
    let fx = Fixture::installed();
    let mut argv = happy_argv(&fx);
    argv.retain(|arg| arg != "--all-installed-repos");
    argv.extend(["--repo".to_string(), "acme/other".to_string()]);
    let output = fx.run(&strs(&argv));
    let text = combined(&output);
    assert_ne!(output.status.code(), Some(0), "output: {text}");
    assert!(
        text.contains("acme/other"),
        "refusal must name the repo: {text}"
    );
    assert!(
        text.contains("acme/bot"),
        "refusal must list installed repos: {text}"
    );
    fx.assert_no_mutation();
}

#[test]
fn an_existing_secret_holding_another_apps_key_is_not_replaced() {
    let mut fx = Fixture::installed();
    let other_pem = std::fs::read(fx.dir.path().join("other.pem")).expect("other pem");
    fx.secret_json = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": "curie-github-app",
            "annotations": {"curie.ai/github-app-id": OTHER_APP_ID}
        },
        "data": {"privateKey": b64(&other_pem)}
    })
    .to_string();
    let argv = happy_argv(&fx);
    let output = fx.run(&strs(&argv));
    let text = combined(&output);
    assert_ne!(output.status.code(), Some(0), "output: {text}");
    let lower = text.to_lowercase();
    assert!(
        lower.contains("different github app") && lower.contains("not replaced"),
        "refusal must say the Secret holds a different App's key and was not replaced: {text}"
    );
    fx.assert_no_mutation();
}

#[test]
fn app_id_without_a_private_key_file_is_a_usage_error() {
    let fx = Fixture::installed();
    let output = fx.run(&[
        "cluster",
        "factory",
        "--app-id",
        "1",
        "--repo",
        "acme/bot",
        "--chart",
        "charts/curie",
    ]);
    assert_eq!(
        output.status.code(),
        Some(USAGE_EXIT),
        "output: {}",
        combined(&output)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--private-key-file") && !stderr.contains("unexpected argument"),
        "the usage error must be about the missing --private-key-file: {stderr}"
    );
    fx.assert_no_mutation();
    assert!(
        fx.github.recorded().is_empty(),
        "no GitHub call on a usage error"
    );
}

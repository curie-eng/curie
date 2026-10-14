//! #4399 AC 10: the GitLab quickstart's steps a to f against the live GitLab CE
//! fixture (`curie dev forge-fixture gitlab up --state-dir <dir>`).
//!
//! Run with `CURIE_GITLAB_FIXTURE=<dir>/state.json cargo test gitlab_fixture
//! -- --nocapture`. With the variable unset every test prints a skip line and
//! returns; set but unreadable or unanswering fails, so a dead fixture never
//! passes as skipped.
//!
//! Entry point: the real binary, `curie factory quickstart --code-host gitlab
//! --tracker gitlab --repo <path> --token-file <file> --forge-url <base>
//! --ca-file <ca> [--label <label>] [--dry-run --json]`, with the same contract
//! as `factory_quickstart_gitlab.rs` documents. `--label` is the trigger label
//! step (f) ensures. The real runs here stub `helm`, `kubectl`, `kind` and
//! `docker` to exit 1, so each stops at its first cluster step, after (f).
//!
//! Idempotence across reruns: each run uses a fresh trigger label and first
//! deletes the four `curie-factory:*` state labels from `widgets` with the root
//! token, so the first quickstart run must create all five. The fresh trigger
//! label is deleted at the end; the state labels are left, as the quickstart
//! leaves them.
//!
//! state.json keys read: `local_base_url` (else `base_url`), `ca_path`,
//! `root_token_file`, `projects.widgets.{id,path}`, and
//! `tokens.{service_account,reporter,read_api_bot}.{file,expires_at}`.

#![cfg(unix)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use serde_json::Value;

const STATE_LABELS: [&str; 4] = [
    "curie-factory:queued",
    "curie-factory:running",
    "curie-factory:pr-open",
    "curie-factory:needs-human",
];

struct Fixture {
    base: String,
    ca_path: String,
    root_token: String,
    project_id: u64,
    project_path: String,
    state: Value,
}

impl Fixture {
    fn token_file(&self, key: &str) -> String {
        self.state["tokens"][key]["file"]
            .as_str()
            .unwrap_or_else(|| panic!("state.json has no tokens.{key}.file"))
            .to_string()
    }

    fn token(&self, key: &str) -> String {
        fs::read_to_string(self.token_file(key))
            .unwrap_or_else(|e| panic!("tokens.{key}.file is unreadable: {e}"))
            .trim()
            .to_string()
    }

    fn api(&self) -> String {
        format!("{}/api/v4", self.base)
    }

    fn client(&self) -> reqwest::Client {
        let pem = fs::read(&self.ca_path).expect("the fixture CA is readable");
        reqwest::Client::builder()
            .add_root_certificate(reqwest::Certificate::from_pem(&pem).expect("CA PEM"))
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .unwrap()
    }

    /// Every label on `widgets` as (id, name), following `x-next-page`.
    async fn labels(&self) -> Vec<(u64, String)> {
        let client = self.client();
        let mut out = Vec::new();
        let mut page = "1".to_string();
        loop {
            let response = client
                .get(format!(
                    "{}/projects/{}/labels?per_page=100&page={page}",
                    self.api(),
                    self.project_id
                ))
                .header("PRIVATE-TOKEN", &self.root_token)
                .send()
                .await
                .expect("listing labels");
            assert!(
                response.status().is_success(),
                "labels: {}",
                response.status()
            );
            let next = response
                .headers()
                .get("x-next-page")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string();
            let body: Value = response.json().await.expect("labels JSON");
            for label in body.as_array().expect("a label list") {
                out.push((
                    label["id"].as_u64().unwrap(),
                    label["name"].as_str().unwrap().to_string(),
                ));
            }
            if next.is_empty() {
                return out;
            }
            page = next;
        }
    }

    async fn delete_label(&self, id: u64) {
        let response = self
            .client()
            .delete(format!(
                "{}/projects/{}/labels/{id}",
                self.api(),
                self.project_id
            ))
            .header("PRIVATE-TOKEN", &self.root_token)
            .send()
            .await
            .expect("deleting a label");
        assert!(
            response.status().is_success() || response.status().as_u16() == 404,
            "delete label {id}: {}",
            response.status()
        );
    }
}

/// The fixture named by `CURIE_GITLAB_FIXTURE`, or `None` (with a skip line)
/// when it is unset. Panics when it is set but unreadable or not answering.
async fn fixture(test: &str) -> Option<Fixture> {
    let Some(raw) = std::env::var_os("CURIE_GITLAB_FIXTURE") else {
        println!("skipping {test}: CURIE_GITLAB_FIXTURE is unset");
        eprintln!("skipping {test}: CURIE_GITLAB_FIXTURE is unset");
        return None;
    };
    let mut path = PathBuf::from(raw);
    if path.is_dir() {
        path = path.join("state.json");
    }
    let state: Value = serde_json::from_str(&fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "{test}: CURIE_GITLAB_FIXTURE={} is unreadable: {e}",
            path.display()
        )
    }))
    .unwrap_or_else(|e| panic!("{test}: {} is not JSON: {e}", path.display()));
    let field = |pointer: &str| -> String {
        state
            .pointer(pointer)
            .and_then(Value::as_str)
            .unwrap_or_else(|| panic!("{test}: state.json has no {pointer}"))
            .to_string()
    };
    let base = state
        .get("local_base_url")
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| field("/base_url"))
        .trim_end_matches('/')
        .to_string();
    let root_file = field("/root_token_file");
    let fixture = Fixture {
        base,
        ca_path: field("/ca_path"),
        root_token: fs::read_to_string(&root_file)
            .unwrap_or_else(|e| panic!("{test}: root token {root_file}: {e}"))
            .trim()
            .to_string(),
        project_id: state
            .pointer("/projects/widgets/id")
            .and_then(Value::as_u64)
            .unwrap_or_else(|| panic!("{test}: state.json has no projects.widgets.id")),
        project_path: field("/projects/widgets/path"),
        state,
    };
    let response = fixture
        .client()
        .get(format!("{}/version", fixture.api()))
        .header("PRIVATE-TOKEN", &fixture.root_token)
        .send()
        .await
        .unwrap_or_else(|e| {
            panic!(
                "{test}: the fixture at {} does not answer: {e}",
                fixture.base
            )
        });
    assert!(
        response.status().is_success(),
        "{test}: the fixture answered GET /version with {}",
        response.status()
    );
    Some(fixture)
}

struct Run {
    code: i32,
    stdout: String,
    stderr: String,
}

impl Run {
    fn all(&self) -> String {
        format!("{}{}", self.stdout, self.stderr)
    }
}

struct Cli {
    dir: tempfile::TempDir,
}

impl Cli {
    /// kubectl, helm, kind and docker shims that log to `$SHIM_LOG` and exit 1.
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let bin = dir.path().join("bin");
        fs::create_dir_all(&bin).unwrap();
        for tool in ["kubectl", "helm", "kind", "docker"] {
            let path = bin.join(tool);
            fs::write(
                &path,
                "#!/bin/sh\necho \"$(basename \"$0\") $*\" >> \"$SHIM_LOG\"\nexit 1\n",
            )
            .unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
        }
        fs::write(
            dir.path().join("kubeconfig"),
            "apiVersion: v1\nkind: Config\ncurrent-context: \"\"\ncontexts:\n  - name: kind-curie-factory\n    context:\n      cluster: fixture\nclusters:\n  - name: fixture\n    cluster:\n      server: https://127.0.0.1:9\n",
        )
        .unwrap();
        Self { dir }
    }

    fn shim_log(&self) -> String {
        fs::read_to_string(self.dir.path().join("shim.log")).unwrap_or_default()
    }

    fn run(&self, fixture: &Fixture, token_key: &str, extra: &[&str]) -> Run {
        let output = Command::new(env!("CARGO_BIN_EXE_curie"))
            .args([
                "factory",
                "quickstart",
                "--context",
                "kind-curie-factory",
                "--chart",
            ])
            .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("../charts/curie"))
            .args(["--code-host", "gitlab", "--tracker", "gitlab", "--repo"])
            .arg(&fixture.project_path)
            .arg("--token-file")
            .arg(fixture.token_file(token_key))
            .arg("--forge-url")
            .arg(&fixture.base)
            .arg("--ca-file")
            .arg(&fixture.ca_path)
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

fn whole_word(text: &str, word: &str) -> bool {
    regex::Regex::new(&format!(
        r"(^|[^A-Za-z0-9_]){}([^A-Za-z0-9_]|$)",
        regex::escape(word)
    ))
    .unwrap()
    .is_match(text)
}

fn assert_no_token(fixture: &Fixture, text: &str, what: &str) {
    for key in ["service_account", "reporter", "read_api_bot"] {
        let token = fixture.token(key);
        assert!(!text.contains(&token), "{what} carries the {key} token");
    }
    assert!(
        !text.contains(&fixture.root_token),
        "{what} carries the root token"
    );
}

/// `Token expires <YYYY-MM-DD> (<n> days)` for the token's recorded expiry.
fn expiry_line(fixture: &Fixture, key: &str) -> String {
    let raw = fixture.state["tokens"][key]["expires_at"]
        .as_str()
        .unwrap_or_else(|| panic!("state.json has no tokens.{key}.expires_at"));
    let date = &raw[..10];
    let parts: Vec<u32> = date
        .split('-')
        .map(|part| part.parse().expect("expires_at is YYYY-MM-DD"))
        .collect();
    let month = time::Month::try_from(parts[1] as u8).expect("a month");
    let expires = time::Date::from_calendar_date(parts[0] as i32, month, parts[2] as u8)
        .expect("expires_at is a date");
    let days = (expires - time::OffsetDateTime::now_utc().date()).whole_days();
    format!("Token expires {date} ({days} days)")
}

fn named<'a>(labels: &'a [(u64, String)], name: &str) -> Vec<&'a (u64, String)> {
    labels.iter().filter(|(_, n)| n == name).collect()
}

#[tokio::test]
async fn gitlab_fixture_quickstart_creates_every_label_on_the_first_run_and_none_on_the_second() {
    let test = "gitlab_fixture_quickstart_creates_every_label";
    let Some(fixture) = fixture(test).await else {
        return;
    };
    let trigger = format!(
        "curie-qs-{}",
        &uuid::Uuid::new_v4().simple().to_string()[..8]
    );
    for (id, name) in fixture.labels().await {
        if STATE_LABELS.contains(&name.as_str()) {
            fixture.delete_label(id).await;
        }
    }
    let wanted: Vec<String> = std::iter::once(trigger.clone())
        .chain(STATE_LABELS.iter().map(|s| s.to_string()))
        .collect();
    let before = fixture.labels().await;
    for name in &wanted {
        assert!(
            named(&before, name).is_empty(),
            "{name} exists before the first run"
        );
    }

    let cli = Cli::new();
    let first = cli.run(&fixture, "service_account", &["--label", &trigger]);
    let text = first.all();
    let after_first = fixture.labels().await;
    for name in &wanted {
        assert_eq!(
            named(&after_first, name).len(),
            1,
            "the first run creates {name} once (exit {}): {text}",
            first.code
        );
    }
    assert!(
        text.contains(&expiry_line(&fixture, "service_account")),
        "the run prints the bot token's expiry: {text}"
    );
    assert_no_token(&fixture, &text, "the first run");

    let second = cli.run(&fixture, "service_account", &["--label", &trigger]);
    let after_second = fixture.labels().await;
    for name in &wanted {
        assert_eq!(
            named(&after_second, name),
            named(&after_first, name),
            "the second run creates no {name}: {}",
            second.all()
        );
    }
    assert_eq!(
        after_second.len(),
        after_first.len(),
        "the second run creates no label: {}",
        second.all()
    );
    assert_no_token(&fixture, &second.all(), "the second run");

    for (id, _) in named(&after_second, &trigger) {
        fixture.delete_label(*id).await;
    }
}

#[tokio::test]
async fn gitlab_fixture_quickstart_refuses_the_reporter_token_naming_level_20() {
    let test = "gitlab_fixture_quickstart_refuses_the_reporter_token";
    let Some(fixture) = fixture(test).await else {
        return;
    };
    let cli = Cli::new();
    let run = cli.run(&fixture, "reporter", &["--dry-run", "--json"]);
    let text = run.all();
    assert_ne!(run.code, 0, "{text}");
    assert!(text.contains("access level 20"), "{text}");
    assert!(
        !cli.shim_log()
            .lines()
            .any(|line| line.starts_with("helm ") || line.starts_with("kubectl ")),
        "no cluster call before the refusal: {}",
        cli.shim_log()
    );
    assert_no_token(&fixture, &text, "the reporter refusal");
}

#[tokio::test]
async fn gitlab_fixture_quickstart_refuses_the_read_api_token_naming_both_scopes() {
    let test = "gitlab_fixture_quickstart_refuses_the_read_api_token";
    let Some(fixture) = fixture(test).await else {
        return;
    };
    let cli = Cli::new();
    let run = cli.run(&fixture, "read_api_bot", &["--dry-run", "--json"]);
    let text = run.all();
    assert_ne!(run.code, 0, "{text}");
    for scope in ["api", "write_repository"] {
        assert!(
            whole_word(&text, scope),
            "the refusal names {scope}: {text}"
        );
    }
    assert_no_token(&fixture, &text, "the read_api refusal");
}

#[tokio::test]
async fn gitlab_fixture_quickstart_dry_run_prints_the_bot_token_expiry() {
    let test = "gitlab_fixture_quickstart_dry_run_prints_the_bot_token_expiry";
    let Some(fixture) = fixture(test).await else {
        return;
    };
    let cli = Cli::new();
    let run = cli.run(&fixture, "service_account", &["--dry-run", "--json"]);
    assert_eq!(run.code, 0, "{}", run.all());
    let body: Value = serde_json::from_str(&run.stdout).expect("one plan object");
    let lines: Vec<&str> = body["plan"]
        .as_array()
        .expect("plan lines")
        .iter()
        .filter_map(Value::as_str)
        .collect();
    let line = expiry_line(&fixture, "service_account");
    assert!(lines.contains(&line.as_str()), "{line} in {lines:#?}");
    assert_no_token(&fixture, &run.all(), "the dry run");
}

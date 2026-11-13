//! `curie factory explain` against a local HTTP fake of the GitHub REST routes.
//!
//! Response shapes follow the GitHub REST docs named on each fixture:
//! repository https://docs.github.com/en/rest/repos/repos#get-a-repository
//! issue https://docs.github.com/en/rest/issues/issues#get-an-issue
//! timeline https://docs.github.com/en/rest/issues/timeline#list-timeline-events-for-an-issue
//! pull https://docs.github.com/en/rest/pulls/pulls#get-a-pull-request
//! reviews https://docs.github.com/en/rest/pulls/reviews#list-reviews-for-a-pull-request
//! check runs https://docs.github.com/en/rest/checks/runs#list-check-runs-for-a-git-reference
//! statuses https://docs.github.com/en/rest/commits/statuses#list-commit-statuses-for-a-reference
//! 401 and 404 https://docs.github.com/en/rest/using-the-rest-api/troubleshooting-the-rest-api
//! rate limit https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Shutdown, TcpListener};
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;

use serde_json::{json, Value};

#[derive(Clone)]
struct Req {
    method: String,
    path: String,
    headers: Vec<(String, String)>,
}

struct Resp {
    status: u16,
    body: String,
    extra: Vec<(String, String)>,
}

struct Server {
    base: String,
    reqs: Arc<Mutex<Vec<Req>>>,
}

impl Server {
    fn recorded(&self) -> Vec<Req> {
        self.reqs.lock().expect("recorded").clone()
    }
}

fn serve(handler: impl Fn(&Req) -> Resp + Send + Sync + 'static) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let base = format!("http://{}", listener.local_addr().unwrap());
    let reqs = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&reqs);
    let handler = Arc::new(handler);
    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { break };
            let recorded = Arc::clone(&recorded);
            let handler = Arc::clone(&handler);
            thread::spawn(move || {
                let mut reader = BufReader::new(stream);
                while let Some(request) = read_req(&mut reader) {
                    let response = handler(&request);
                    recorded.lock().expect("push").push(request);
                    let drop_body = response
                        .extra
                        .iter()
                        .any(|(name, _)| name.eq_ignore_ascii_case("x-test-drop"));
                    let mut head = format!(
                        "HTTP/1.1 {} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\n",
                        response.status,
                        response.body.len()
                    );
                    for (name, value) in &response.extra {
                        if name.eq_ignore_ascii_case("x-test-drop") {
                            continue;
                        }
                        head.push_str(&format!("{name}: {value}\r\n"));
                    }
                    head.push_str("\r\n");
                    let stream = reader.get_mut();
                    if stream.write_all(head.as_bytes()).is_err() {
                        break;
                    }
                    if drop_body {
                        let _ = stream.shutdown(Shutdown::Both);
                        break;
                    }
                    if stream.write_all(response.body.as_bytes()).is_err() {
                        break;
                    }
                    let _ = stream.flush();
                }
            });
        }
    });
    Server { base, reqs }
}

fn read_req(reader: &mut BufReader<std::net::TcpStream>) -> Option<Req> {
    let mut request_line = String::new();
    if reader.read_line(&mut request_line).ok()? == 0 {
        return None;
    }
    let mut parts = request_line.split_whitespace();
    let method = parts.next()?.to_string();
    let path = parts.next()?.to_string();
    let mut headers = Vec::new();
    loop {
        let mut line = String::new();
        reader.read_line(&mut line).ok()?;
        if line == "\r\n" || line == "\n" {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            headers.push((name.trim().to_string(), value.trim().to_string()));
        }
    }
    let length = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, value)| value.parse::<usize>().ok())
        .unwrap_or(0);
    let mut body = vec![0; length];
    if length > 0 {
        reader.read_exact(&mut body).ok()?;
    }
    Some(Req {
        method,
        path,
        headers,
    })
}

fn header<'a>(req: &'a Req, name: &str) -> Option<&'a str> {
    req.headers
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

fn route(path: &str) -> &str {
    path.split('?').next().unwrap_or(path)
}

fn marker(issue: u64) -> String {
    format!("<!-- curie-factory-issue kind=github host=github.com scope=101 issue={issue} -->")
}

fn entry(n: u64, source: &str) -> String {
    format!(
        "{n}. Read the forge with the caller token. Rejected: Read the platform key, because an ordinary developer does not hold it. ({source}) Evidence: the command sends the caller token only."
    )
}

fn one_body() -> String {
    format!(
        "Adds the explain command.\n\n- [x] parser\n- [ ] live\n\n## Decisions\n{}\n{}\n{}\n\n## Reviews\nThe diff review approved the read path.\n\n## Checks\nNot part of the reviews text.\n",
        entry(1, "plan"),
        entry(2, "plan review"),
        entry(3, "diff review")
    )
}

struct Pull {
    number: u64,
    created: &'static str,
    state: &'static str,
    merged: bool,
    body: String,
    repo: &'static str,
}

fn scene(issue: u64, pulls: Vec<Pull>, foreign: bool) -> impl Fn(&Req) -> Resp {
    let pulls = Arc::new(pulls);
    move |req| {
        let path = route(&req.path);
        if path == "/repos/curie-eng/curie" {
            return Resp {
                status: 200,
                body: json!({"id": 101, "html_url": "https://github.com/curie-eng/curie", "name": "curie", "full_name": "curie-eng/curie", "owner": {"login": "curie-eng"}}).to_string(),
                extra: vec![],
            };
        }
        if path == format!("/repos/curie-eng/curie/issues/{issue}") {
            return Resp {
                status: 200,
                body: json!({"number": issue, "title": "Explain", "state": "open", "html_url": format!("https://github.com/curie-eng/curie/issues/{issue}")}).to_string(),
                extra: vec![],
            };
        }
        if path == format!("/repos/curie-eng/curie/issues/{issue}/timeline") {
            let mut events = Vec::new();
            for pull in pulls.iter() {
                events.push(json!({
                    "event": "cross-referenced",
                    "created_at": pull.created,
                    "source": {"type": "issue", "issue": {
                        "number": pull.number,
                        "created_at": pull.created,
                        "repository_url": format!("https://api.github.com/repos/{}", pull.repo),
                        "pull_request": {"url": format!("https://api.github.com/repos/{}/pulls/{}", pull.repo, pull.number)}
                    }}
                }));
            }
            if foreign {
                events.push(json!({
                    "event": "cross-referenced",
                    "created_at": "2026-04-01T00:00:00Z",
                    "source": {"type": "issue", "issue": {
                        "number": 9,
                        "created_at": "2026-04-01T00:00:00Z",
                        "pull_request": {"url": "https://api.github.com/repos/other/repo/pulls/9"}
                    }}
                }));
            }
            return Resp {
                status: 200,
                body: json!(events).to_string(),
                extra: vec![],
            };
        }
        if let Some(number) = path
            .strip_prefix("/repos/curie-eng/curie/pulls/")
            .and_then(|rest| rest.parse::<u64>().ok())
        {
            let pull = pulls
                .iter()
                .find(|pull| pull.number == number)
                .expect("pull");
            let merged_at = if pull.merged {
                Value::String("2026-05-01T00:00:00Z".into())
            } else {
                Value::Null
            };
            return Resp {
                status: 200,
                body: json!({
                    "number": pull.number,
                    "html_url": format!("https://github.com/curie-eng/curie/pull/{}", pull.number),
                    "title": format!("Change {}", pull.number),
                    "state": pull.state,
                    "merged_at": merged_at,
                    "created_at": pull.created,
                    "body": pull.body,
                    "head": {"sha": format!("abc{:02}", pull.number)}
                })
                .to_string(),
                extra: vec![],
            };
        }
        if path.contains("/reviews") {
            return Resp {
                status: 200,
                body: json!([
                    {"user": {"login": "ada"}, "state": "COMMENTED", "submitted_at": "2026-01-01T00:00:00Z"},
                    {"user": {"login": "ada"}, "state": "APPROVED", "submitted_at": "2026-01-02T00:00:00Z"},
                    {"user": {"login": "bea"}, "state": "CHANGES_REQUESTED", "submitted_at": "2026-01-02T00:00:00Z"}
                ]).to_string(),
                extra: vec![],
            };
        }
        if path.contains("/check-runs") {
            return Resp {
                status: 200,
                body: json!({"total_count": 3, "check_runs": [
                    {"name": "Python", "status": "completed", "conclusion": "failure", "completed_at": "2026-01-02T00:00:00Z"},
                    {"name": "Rust", "status": "completed", "conclusion": "success", "completed_at": "2026-01-02T00:00:00Z"},
                    {"name": "Python", "status": "completed", "conclusion": "success", "completed_at": "2026-01-01T00:00:00Z"}
                ]}).to_string(),
                extra: vec![],
            };
        }
        if path.contains("/statuses") {
            return Resp {
                status: 200,
                body: json!([
                    {"context": "ci/local", "state": "pending", "created_at": "2026-01-02T00:00:00Z"},
                    {"context": "ci/local", "state": "success", "created_at": "2026-01-01T00:00:00Z"}
                ]).to_string(),
                extra: vec![],
            };
        }
        Resp { status: 404, body: json!({"message": "Not Found", "documentation_url": "https://docs.github.com/rest/issues/issues#get-an-issue"}).to_string(), extra: vec![] }
    }
}

fn run(
    args: &[&str],
    server: &Server,
    token: &[(&str, &str)],
    path_dir: Option<&PathBuf>,
) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_curie"));
    command
        .args(["factory", "explain"])
        .args(args)
        .arg("--api-url")
        .arg(&server.base)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env_remove("GH_TOKEN")
        .env_remove("GITHUB_TOKEN")
        .env_remove("CURIE_API_KEY")
        .env_remove("CURIE_CONFIG_DIR");
    for (key, value) in token {
        command.env(key, value);
    }
    if let Some(dir) = path_dir {
        let old = std::env::var("PATH").unwrap_or_default();
        command.env("PATH", format!("{}:{old}", dir.display()));
    }
    command.output().expect("run curie")
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).to_string()
}

fn json_stdout(output: &Output) -> Value {
    let raw = text(&output.stdout);
    serde_json::from_str(raw.trim()).unwrap_or_else(|error| {
        panic!(
            "one json object: {error}\nstdout={raw}\nstderr={}",
            text(&output.stderr)
        )
    })
}

fn marked(number: u64, created: &'static str, issue: u64) -> Pull {
    Pull {
        number,
        created,
        state: "closed",
        merged: true,
        body: format!("{}\n{}", one_body(), marker(issue)),
        repo: "curie-eng/curie",
    }
}

fn one_pull_checks(check_runs: Value, statuses: Value) -> impl Fn(&Req) -> Resp {
    move |req| {
        let path = route(&req.path);
        if path == "/repos/curie-eng/curie" {
            return Resp {
                status: 200,
                body: json!({"id": 101, "html_url": "https://github.com/curie-eng/curie", "name": "curie", "owner": {"login": "curie-eng"}}).to_string(),
                extra: vec![],
            };
        }
        if path == "/repos/curie-eng/curie/issues/12" {
            return Resp {
                status: 200,
                body: json!({"number": 12, "title": "Explain", "state": "open"}).to_string(),
                extra: vec![],
            };
        }
        if path == "/repos/curie-eng/curie/issues/12/timeline" {
            return Resp {
                status: 200,
                body: json!([{
                    "event": "cross-referenced",
                    "created_at": "2026-03-01T00:00:00Z",
                    "source": {"issue": {"number": 7, "created_at": "2026-03-01T00:00:00Z", "pull_request": {"url": "https://api.github.com/repos/curie-eng/curie/pulls/7"}}}
                }]).to_string(),
                extra: vec![],
            };
        }
        if path == "/repos/curie-eng/curie/pulls/7" {
            return Resp {
                status: 200,
                body: json!({
                    "number": 7,
                    "html_url": "https://github.com/curie-eng/curie/pull/7",
                    "title": "Change 7",
                    "state": "closed",
                    "merged_at": "2026-05-01T00:00:00Z",
                    "created_at": "2026-03-01T00:00:00Z",
                    "body": format!("Summary.\n{}", marker(12)),
                    "head": {"sha": "abc07"}
                })
                .to_string(),
                extra: vec![],
            };
        }
        if path.contains("/reviews") {
            return Resp {
                status: 200,
                body: "[]".into(),
                extra: vec![],
            };
        }
        if path.contains("/check-runs") {
            return Resp {
                status: 200,
                body: json!({"check_runs": check_runs.clone()}).to_string(),
                extra: vec![],
            };
        }
        if path.contains("/statuses") {
            return Resp {
                status: 200,
                body: statuses.to_string(),
                extra: vec![],
            };
        }
        Resp {
            status: 404,
            body: "{}".into(),
            extra: vec![],
        }
    }
}

fn explained_checks(check_runs: Value, statuses: Value) -> Value {
    let server = serve(one_pull_checks(check_runs, statuses));
    let output = run(&["curie-eng/curie#12", "--json"], &server, &token(), None);
    assert_eq!(output.status.code(), Some(0), "{}", text(&output.stderr));
    json_stdout(&output)["pull_requests"][0]["checks"].clone()
}

#[test]
fn one_marked_pull_request_prints_the_record() {
    let server = serve(scene(
        12,
        vec![
            marked(7, "2026-03-01T00:00:00Z", 12),
            Pull {
                number: 8,
                created: "2026-03-02T00:00:00Z",
                state: "open",
                merged: false,
                body: "no marker here".into(),
                repo: "curie-eng/curie",
            },
            Pull {
                number: 4,
                created: "2026-03-03T00:00:00Z",
                state: "open",
                merged: false,
                body: marker(99),
                repo: "curie-eng/curie",
            },
        ],
        true,
    ));
    let output = run(
        &["curie-eng/curie#12", "--json"],
        &server,
        &[("GH_TOKEN", "token-from-env")],
        None,
    );
    assert_eq!(output.status.code(), Some(0), "{}", text(&output.stderr));
    let value = json_stdout(&output);
    assert_eq!(value["issue"], "curie-eng/curie#12");
    assert!(value.get("message").is_none(), "{value}");
    let pulls = value["pull_requests"].as_array().unwrap();
    assert_eq!(pulls.len(), 1, "{value}");
    let pull = &pulls[0];
    assert_eq!(pull["number"], 7);
    assert_eq!(pull["state"], "merged");
    assert_eq!(pull["head_sha"], "abc07");
    assert!(pull["summary"]
        .as_str()
        .unwrap()
        .contains("Adds the explain command."));
    let entries = pull["decisions"]["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0]["choice"], "Read the forge with the caller token");
    assert_eq!(entries[0]["rejected"], "Read the platform key");
    assert_eq!(
        entries[0]["reason"],
        "an ordinary developer does not hold it"
    );
    assert_eq!(entries[0]["source"], "plan");
    assert_eq!(
        entries[0]["evidence"],
        "the command sends the caller token only"
    );
    assert_eq!(entries[1]["source"], "plan review");
    assert_eq!(entries[2]["source"], "diff review");
    assert_eq!(pull["verification"]["checked"], 1);
    assert_eq!(pull["verification"]["unchecked"], 1);
    assert_eq!(
        pull["review_section"],
        "The diff review approved the read path."
    );
    assert_eq!(pull["forge_reviews"][0]["reviewer"], "ada");
    assert_eq!(pull["forge_reviews"][0]["state"], "APPROVED");
    assert_eq!(pull["forge_reviews"][1]["state"], "CHANGES_REQUESTED");
    assert_eq!(pull["checks"]["passing"], 1);
    assert_eq!(pull["checks"]["failing"], 1);
    assert_eq!(pull["checks"]["pending"], 1);
    assert_eq!(pull["checks"]["failing_names"], json!(["Python"]));
    let human = run(
        &["https://github.com/curie-eng/curie/issues/12"],
        &server,
        &[("GH_TOKEN", "token-from-env")],
        None,
    );
    assert_eq!(human.status.code(), Some(0), "{}", text(&human.stderr));
    let stdout = text(&human.stdout);
    assert!(stdout.contains("Adds the explain command."), "{stdout}");
    assert!(
        stdout.contains("Read the forge with the caller token"),
        "{stdout}"
    );
    assert!(
        stdout.contains("rejected Read the platform key"),
        "{stdout}"
    );
    assert!(stdout.contains("source plan review"), "{stdout}");
    assert!(
        stdout.contains("verification checked 1 unchecked 1"),
        "{stdout}"
    );
    assert!(
        stdout.contains("reviews The diff review approved the read path."),
        "{stdout}"
    );
    assert!(stdout.contains("review ada APPROVED"), "{stdout}");
    assert!(
        stdout.contains("checks passing 1 failing 1 pending 1"),
        "{stdout}"
    );
    assert!(stdout.contains("failing Python"), "{stdout}");
    assert!(server.recorded().iter().all(|req| req.method == "GET"));
}

#[test]
fn zero_several_unrecorded_outcomes() {
    let ignored = serve(scene(
        12,
        vec![
            Pull {
                number: 8,
                created: "2026-03-02T00:00:00Z",
                state: "open",
                merged: false,
                body: "no marker".into(),
                repo: "curie-eng/curie",
            },
            Pull {
                number: 4,
                created: "2026-03-03T00:00:00Z",
                state: "open",
                merged: false,
                body: marker(99),
                repo: "curie-eng/curie",
            },
        ],
        true,
    ));
    let zero = run(
        &["curie-eng/curie#12", "--json"],
        &ignored,
        &[("GH_TOKEN", "token-from-env")],
        None,
    );
    assert_eq!(zero.status.code(), Some(0), "{}", text(&zero.stderr));
    let value = json_stdout(&zero);
    assert_eq!(value["pull_requests"].as_array().unwrap().len(), 0);
    assert_eq!(value["message"], "no factory pull request");
    let human = run(
        &["curie-eng/curie#12"],
        &ignored,
        &[("GH_TOKEN", "token-from-env")],
        None,
    );
    assert!(text(&human.stdout).contains("no factory pull request"));

    let several = serve(scene(
        12,
        vec![
            marked(30, "2026-01-01T00:00:00Z", 12),
            marked(10, "2026-03-01T00:00:00Z", 12),
            marked(20, "2026-02-01T00:00:00Z", 12),
        ],
        false,
    ));
    let listed = json_stdout(&run(
        &["curie-eng/curie#12", "--json"],
        &several,
        &[("GH_TOKEN", "token-from-env")],
        None,
    ));
    let numbers: Vec<u64> = listed["pull_requests"]
        .as_array()
        .unwrap()
        .iter()
        .map(|pull| pull["number"].as_u64().unwrap())
        .collect();
    assert_eq!(numbers, vec![10, 20, 30]);

    let bare = serve(scene(
        12,
        vec![Pull {
            number: 7,
            created: "2026-03-01T00:00:00Z",
            state: "open",
            merged: false,
            body: format!("Summary only.\n{}", marker(12)),
            repo: "curie-eng/curie",
        }],
        false,
    ));
    let not_recorded = json_stdout(&run(
        &["curie-eng/curie#12", "--json"],
        &bare,
        &[("GH_TOKEN", "token-from-env")],
        None,
    ));
    assert_eq!(
        not_recorded["pull_requests"][0]["decisions"]["status"],
        "not recorded"
    );
    assert!(text(
        &run(
            &["curie-eng/curie#12"],
            &bare,
            &[("GH_TOKEN", "token-from-env")],
            None
        )
        .stdout
    )
    .contains("decisions not recorded"));

    let none_body = format!(
        "Summary.\n{}\n\n## Decisions\n\nNone: the change follows directly from the acceptance criteria.\n",
        marker(12)
    );
    let none_server = serve(scene(
        12,
        vec![Pull {
            number: 7,
            created: "2026-03-01T00:00:00Z",
            state: "closed",
            merged: false,
            body: none_body,
            repo: "curie-eng/curie",
        }],
        false,
    ));
    let none = json_stdout(&run(
        &["curie-eng/curie#12", "--json"],
        &none_server,
        &[("GH_TOKEN", "token-from-env")],
        None,
    ));
    assert_eq!(none["pull_requests"][0]["decisions"]["status"], "none");
    assert!(text(
        &run(
            &["curie-eng/curie#12"],
            &none_server,
            &[("GH_TOKEN", "token-from-env")],
            None
        )
        .stdout
    )
    .contains("decisions none"));

    let bad = format!("## Decisions\nthis line is not an entry\n{}", marker(12));
    let bad_server = serve(scene(
        12,
        vec![Pull {
            number: 7,
            created: "2026-03-01T00:00:00Z",
            state: "closed",
            merged: false,
            body: bad,
            repo: "curie-eng/curie",
        }],
        false,
    ));
    let unparsed = json_stdout(&run(
        &["curie-eng/curie#12", "--json"],
        &bad_server,
        &[("GH_TOKEN", "token-from-env")],
        None,
    ));
    assert_eq!(
        unparsed["pull_requests"][0]["decisions"]["status"],
        "unparsed"
    );
    let lines = unparsed["pull_requests"][0]["decisions"]["lines"]
        .as_array()
        .unwrap();
    assert!(lines
        .iter()
        .any(|line| line.as_str() == Some("this line is not an entry")));
    assert!(text(
        &run(
            &["curie-eng/curie#12"],
            &bad_server,
            &[("GH_TOKEN", "token-from-env")],
            None
        )
        .stdout
    )
    .contains("decisions unparsed"));
}

#[test]
fn caller_login_uses_only_the_forge_token() {
    let dir = std::env::temp_dir().join(format!("explain-gh-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let called = dir.join("called");
    let script = dir.join("gh");
    std::fs::write(
        &script,
        format!(
            "#!/bin/sh\nprintf '%s\\n' token-from-gh\ntouch {}\n",
            called.display()
        ),
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perm = std::fs::metadata(&script).unwrap().permissions();
        perm.set_mode(0o755);
        std::fs::set_permissions(&script, perm).unwrap();
    }
    let server = serve(scene(
        12,
        vec![marked(7, "2026-03-01T00:00:00Z", 12)],
        false,
    ));
    let gh_token = run(
        &["curie-eng/curie#12", "--json"],
        &server,
        &[
            ("GH_TOKEN", "token-from-env"),
            ("GITHUB_TOKEN", "token-from-github-env"),
        ],
        Some(&dir),
    );
    assert_eq!(
        gh_token.status.code(),
        Some(0),
        "{}",
        text(&gh_token.stderr)
    );
    assert!(header(&server.recorded()[0], "authorization") == Some("Bearer token-from-env"));
    assert!(!called.exists(), "GH_TOKEN must win without calling gh");

    let server = serve(scene(
        12,
        vec![marked(7, "2026-03-01T00:00:00Z", 12)],
        false,
    ));
    let _ = std::fs::remove_file(&called);
    let github_token = run(
        &["curie-eng/curie#12", "--json"],
        &server,
        &[("GITHUB_TOKEN", "token-from-github-env")],
        Some(&dir),
    );
    assert_eq!(
        github_token.status.code(),
        Some(0),
        "{}",
        text(&github_token.stderr)
    );
    assert!(header(&server.recorded()[0], "authorization") == Some("Bearer token-from-github-env"));
    assert!(!called.exists(), "GITHUB_TOKEN must win without calling gh");

    let server = serve(scene(
        12,
        vec![marked(7, "2026-03-01T00:00:00Z", 12)],
        false,
    ));
    let _ = std::fs::remove_file(&called);
    let from_gh = run(&["curie-eng/curie#12", "--json"], &server, &[], Some(&dir));
    assert_eq!(from_gh.status.code(), Some(0), "{}", text(&from_gh.stderr));
    assert!(header(&server.recorded()[0], "authorization") == Some("Bearer token-from-gh"));
    let gh_args = std::fs::read_to_string(&called).ok();
    assert!(called.exists());
    let _ = gh_args;
    let recorded_host = std::fs::read_to_string(dir.join("args")).unwrap_or_default();
    let _ = recorded_host;

    let missing_dir = dir.join("empty");
    std::fs::create_dir_all(&missing_dir).unwrap();
    let server = serve(scene(
        12,
        vec![marked(7, "2026-03-01T00:00:00Z", 12)],
        false,
    ));
    let missing = run(
        &["curie-eng/curie#12", "--json"],
        &server,
        &[],
        Some(&missing_dir),
    );
    assert_eq!(missing.status.code(), Some(1), "{}", text(&missing.stderr));
    let err = json_stdout(&missing);
    assert!(
        err.get("error").is_some() && err.get("fix").is_some(),
        "{err}"
    );
    let fix = err["fix"].as_str().unwrap();
    assert!(
        fix.contains("gh auth login") && fix.contains("GH_TOKEN"),
        "{fix}"
    );
    assert!(server.recorded().is_empty());

    let config = dir.join("config");
    std::fs::create_dir_all(&config).unwrap();
    std::fs::write(
        config.join("credentials.json"),
        "{\"apiKey\":\"sentinel-curie-key\"}",
    )
    .unwrap();
    let proxy = TcpListener::bind("127.0.0.1:0").unwrap();
    let proxy_addr = proxy.local_addr().unwrap();
    let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let hits2 = Arc::clone(&hits);
    thread::spawn(move || {
        for _ in proxy.incoming().flatten() {
            hits2.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    });
    let server = serve(scene(
        12,
        vec![marked(7, "2026-03-01T00:00:00Z", 12)],
        false,
    ));
    let mut command = Command::new(env!("CARGO_BIN_EXE_curie"));
    let proxy_url = format!("http://{proxy_addr}");
    let output = command
        .args([
            "factory",
            "explain",
            "curie-eng/curie#12",
            "--json",
            "--api-url",
            &server.base,
        ])
        .env_remove("GITHUB_TOKEN")
        .env("GH_TOKEN", "token-from-env")
        .env("CURIE_API_KEY", "sentinel-curie-key")
        .env("CURIE_CONFIG_DIR", &config)
        .env("HTTP_PROXY", &proxy_url)
        .env("HTTPS_PROXY", &proxy_url)
        .env("ALL_PROXY", &proxy_url)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(0), "{}", text(&output.stderr));
    assert_eq!(
        hits.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "no proxy or other address"
    );
    for req in server.recorded() {
        assert_eq!(req.method, "GET");
        let blob = format!(
            "{} {}",
            req.path,
            req.headers
                .iter()
                .map(|(k, v)| format!("{k}: {v}"))
                .collect::<Vec<_>>()
                .join(" ")
        );
        assert!(!blob.contains("sentinel-curie-key"), "{blob}");
        assert!(header(&req, "x-api-key").is_none(), "{blob}");
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn caller_login_gh_receives_hostname_for_a_non_github_host() {
    let dir = std::env::temp_dir().join(format!("explain-gh-args-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let args_file = dir.join("args");
    std::fs::write(
        dir.join("gh"),
        format!(
            "#!/bin/sh\nprintf '%s\\n' \"$*\" > {}\nprintf '%s\\n' token-from-gh\n",
            args_file.display()
        ),
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perm = std::fs::metadata(dir.join("gh")).unwrap().permissions();
        perm.set_mode(0o755);
        std::fs::set_permissions(dir.join("gh"), perm).unwrap();
    }
    let server = serve(scene(
        12,
        vec![marked(7, "2026-03-01T00:00:00Z", 12)],
        false,
    ));
    let output = run(&["curie-eng/curie#12", "--json"], &server, &[], Some(&dir));
    assert_eq!(output.status.code(), Some(0), "{}", text(&output.stderr));
    let args = std::fs::read_to_string(&args_file).unwrap();
    assert!(args.contains("--hostname"), "{args}");
    assert!(args.contains("127.0.0.1"), "{args}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn exit_codes_for_usage_failure_and_transient() {
    let server = serve(scene(12, vec![], false));
    for reference in [
        "nope",
        "https://gitlab.com/curie-eng/curie/-/issues/12",
        "https://github.com/curie-eng/curie/pull/12",
        "https://example.com/curie-eng/curie/issues/12",
    ] {
        let output = run(
            &[reference],
            &server,
            &[("GH_TOKEN", "token-from-env")],
            None,
        );
        assert_eq!(
            output.status.code(),
            Some(2),
            "{reference} {}",
            text(&output.stderr)
        );
    }
    assert!(server.recorded().is_empty(), "usage errors make no request");

    let missing = serve(|req: &Req| {
        let status = if route(&req.path).ends_with("/issues/12") {
            404
        } else {
            200
        };
        let body = if status == 404 {
            json!({"message": "Not Found", "documentation_url": "https://docs.github.com/rest/issues/issues#get-an-issue"}).to_string()
        } else {
            json!({"id": 101, "html_url": "https://github.com/curie-eng/curie"}).to_string()
        };
        Resp {
            status,
            body,
            extra: vec![],
        }
    });
    let not_found = run(
        &["curie-eng/curie#12", "--json"],
        &missing,
        &[("GH_TOKEN", "token-from-env")],
        None,
    );
    assert_eq!(
        not_found.status.code(),
        Some(1),
        "{}",
        text(&not_found.stderr)
    );

    let denied = serve(|_req: &Req| Resp {
        status: 401,
        body: json!({
            "message": "Bad credentials",
            "documentation_url": "https://docs.github.com/rest"
        })
        .to_string(),
        extra: vec![],
    });
    let unauthorized = run(
        &["curie-eng/curie#12", "--json"],
        &denied,
        &[("GH_TOKEN", "token-from-env")],
        None,
    );
    assert_eq!(
        unauthorized.status.code(),
        Some(1),
        "{}",
        text(&unauthorized.stderr)
    );

    let down = serve(|_req: &Req| Resp {
        status: 503,
        body: json!({"message": "Server Error"}).to_string(),
        extra: vec![],
    });
    let unavailable = run(
        &["curie-eng/curie#12", "--json"],
        &down,
        &[("GH_TOKEN", "token-from-env")],
        None,
    );
    assert_eq!(
        unavailable.status.code(),
        Some(3),
        "{}",
        text(&unavailable.stderr)
    );

    let limited = serve(|_req: &Req| {
        Resp {
        status: 429,
        body: json!({
            "message": "API rate limit exceeded for 127.0.0.1.",
            "documentation_url": "https://docs.github.com/rest/overview/resources-in-the-rest-api#rate-limiting"
        })
        .to_string(),
        extra: vec![("x-ratelimit-remaining".into(), "0".into())],
    }
    });
    let rate = run(
        &["curie-eng/curie#12", "--json"],
        &limited,
        &[("GH_TOKEN", "token-from-env")],
        None,
    );
    assert_eq!(rate.status.code(), Some(3), "{}", text(&rate.stderr));
}

#[test]
fn timeline_stops_at_ten_pages() {
    let server = serve(|req: &Req| {
        let path = route(&req.path);
        if path == "/repos/curie-eng/curie" {
            return Resp {
                status: 200,
                body: json!({"id": 101, "html_url": "https://github.com/curie-eng/curie"})
                    .to_string(),
                extra: vec![],
            };
        }
        if path == "/repos/curie-eng/curie/issues/12" {
            return Resp {
                status: 200,
                body: json!({"number": 12, "title": "Explain", "state": "open"}).to_string(),
                extra: vec![],
            };
        }
        if path == "/repos/curie-eng/curie/issues/12/timeline" {
            let page = req
                .path
                .split("page=")
                .nth(1)
                .and_then(|rest| rest.split('&').next())
                .and_then(|n| n.parse::<u32>().ok())
                .unwrap_or(1);
            let next = format!(
                "http://{}{}?per_page=100&page={}",
                req_host(req),
                path,
                page + 1
            );
            return Resp {
                status: 200,
                body: json!([{"event": "commented"}]).to_string(),
                extra: vec![("link".into(), format!("<{next}>; rel=\"next\""))],
            };
        }
        Resp {
            status: 404,
            body: "{}".into(),
            extra: vec![],
        }
    });
    let output = run(
        &["curie-eng/curie#12", "--json"],
        &server,
        &[("GH_TOKEN", "token-from-env")],
        None,
    );
    assert_eq!(output.status.code(), Some(0), "{}", text(&output.stderr));
    let recorded = server.recorded();
    let pages = recorded
        .iter()
        .filter(|req| route(&req.path).ends_with("/timeline"))
        .count();
    assert_eq!(pages, 10, "timeline pages: {pages}");
}

fn req_host(req: &Req) -> String {
    header(req, "host").unwrap_or("127.0.0.1").to_string()
}

fn token() -> [(&'static str, &'static str); 1] {
    [("GH_TOKEN", "token-from-env")]
}

#[test]
fn pagination_rejects_a_next_link_on_another_origin() {
    let server = serve(|req: &Req| {
        let path = route(&req.path);
        if path == "/repos/curie-eng/curie" {
            return Resp {
                status: 200,
                body: json!({"id": 101, "html_url": "https://github.com/curie-eng/curie"})
                    .to_string(),
                extra: vec![],
            };
        }
        if path == "/repos/curie-eng/curie/issues/12" {
            return Resp {
                status: 200,
                body: json!({"number": 12, "title": "Explain", "state": "open"}).to_string(),
                extra: vec![],
            };
        }
        if path == "/repos/curie-eng/curie/issues/12/timeline" {
            // The extra digit keeps a string prefix of the API URL while the
            // parsed port is different, so the bearer token must not follow it.
            let next = format!("http://{}0/stolen", req_host(req));
            return Resp {
                status: 200,
                body: json!([{"event": "commented"}]).to_string(),
                extra: vec![("link".into(), format!("<{next}>; rel=\"next\""))],
            };
        }
        Resp {
            status: 404,
            body: "{}".into(),
            extra: vec![],
        }
    });
    let output = run(&["curie-eng/curie#12", "--json"], &server, &token(), None);
    assert_eq!(
        output.status.code(),
        Some(1),
        "stdout={} stderr={}",
        text(&output.stdout),
        text(&output.stderr)
    );
    assert!(
        text(&output.stdout).contains("pagination left the API host"),
        "{}",
        text(&output.stdout)
    );
    assert_eq!(
        server
            .recorded()
            .iter()
            .filter(|req| route(&req.path).ends_with("/timeline"))
            .count(),
        1
    );
}

#[test]
fn response_body_reset_is_transient() {
    let server = serve(|_req: &Req| Resp {
        status: 200,
        body: json!({"id": 101, "html_url": "https://github.com/curie-eng/curie"}).to_string(),
        extra: vec![("x-test-drop".into(), "1".into())],
    });
    let output = run(&["curie-eng/curie#12", "--json"], &server, &token(), None);
    assert_eq!(
        output.status.code(),
        Some(3),
        "stdout={} stderr={}",
        text(&output.stdout),
        text(&output.stderr)
    );
}

#[test]
fn equal_timestamps_keep_the_newest_first_row() {
    let server = serve(|req: &Req| {
        let path = route(&req.path);
        if path == "/repos/curie-eng/curie" {
            return Resp {
                status: 200,
                body: json!({"id": 101, "html_url": "https://github.com/curie-eng/curie", "name": "curie", "owner": {"login": "curie-eng"}}).to_string(),
                extra: vec![],
            };
        }
        if path == "/repos/curie-eng/curie/issues/12" {
            return Resp {
                status: 200,
                body: json!({"number": 12, "title": "Explain", "state": "open"}).to_string(),
                extra: vec![],
            };
        }
        if path == "/repos/curie-eng/curie/issues/12/timeline" {
            return Resp {
                status: 200,
                body: json!([{
                    "event": "cross-referenced",
                    "created_at": "2026-03-01T00:00:00Z",
                    "source": {"issue": {"number": 7, "created_at": "2026-03-01T00:00:00Z", "pull_request": {"url": "https://api.github.com/repos/curie-eng/curie/pulls/7"}}}
                }]).to_string(),
                extra: vec![],
            };
        }
        if path == "/repos/curie-eng/curie/pulls/7" {
            return Resp {
                status: 200,
                body: json!({
                    "number": 7,
                    "html_url": "https://github.com/curie-eng/curie/pull/7",
                    "title": "Change 7",
                    "state": "closed",
                    "merged_at": "2026-05-01T00:00:00Z",
                    "created_at": "2026-03-01T00:00:00Z",
                    "body": format!("Summary.\n{}", marker(12)),
                    "head": {"sha": "abc07"}
                })
                .to_string(),
                extra: vec![],
            };
        }
        if path.contains("/reviews") {
            return Resp {
                status: 200,
                body: "[]".into(),
                extra: vec![],
            };
        }
        if path.contains("/check-runs") {
            return Resp {
                status: 200,
                body: json!({"total_count": 2, "check_runs": [
                    {"name": "Lint", "status": "completed", "conclusion": "success", "completed_at": "2026-01-02T00:00:00Z"},
                    {"name": "Lint", "status": "completed", "conclusion": "failure", "completed_at": "2026-01-02T00:00:00Z"}
                ]}).to_string(),
                extra: vec![],
            };
        }
        if path.contains("/statuses") {
            return Resp {
                status: 200,
                body: json!([
                    {"context": "ci/tie", "state": "success", "created_at": "2026-01-02T00:00:00Z"},
                    {"context": "ci/tie", "state": "pending", "created_at": "2026-01-02T00:00:00Z"}
                ])
                .to_string(),
                extra: vec![],
            };
        }
        Resp {
            status: 404,
            body: "{}".into(),
            extra: vec![],
        }
    });
    let output = run(&["curie-eng/curie#12", "--json"], &server, &token(), None);
    assert_eq!(output.status.code(), Some(0), "{}", text(&output.stderr));
    let body = json_stdout(&output);
    let checks = &body["pull_requests"][0]["checks"];
    assert_eq!(checks["passing"], 2, "{checks}");
    assert_eq!(checks["failing"], 0, "{checks}");
    assert_eq!(checks["pending"], 0, "{checks}");
    assert_eq!(checks["failing_names"], json!([]), "{checks}");
}

#[test]
fn newer_queued_rerun_stays_pending() {
    let checks = explained_checks(
        json!([
            {"name": "Lint", "status": "queued", "conclusion": null, "started_at": null, "completed_at": null},
            {"name": "Lint", "status": "completed", "conclusion": "success", "started_at": "2026-01-01T00:00:00Z", "completed_at": "2026-01-02T00:00:00Z"}
        ]),
        json!([]),
    );
    assert_eq!(checks["passing"], 0, "{checks}");
    assert_eq!(checks["failing"], 0, "{checks}");
    assert_eq!(checks["pending"], 1, "{checks}");
}

#[test]
fn newer_failure_beats_an_older_success_that_completed_later() {
    let checks = explained_checks(
        json!([
            {"name": "Lint", "status": "completed", "conclusion": "failure", "started_at": "2026-01-02T00:00:00Z", "completed_at": "2026-01-02T01:00:00Z"},
            {"name": "Lint", "status": "completed", "conclusion": "success", "started_at": "2026-01-01T00:00:00Z", "completed_at": "2026-01-02T02:00:00Z"}
        ]),
        json!([]),
    );
    assert_eq!(checks["passing"], 0, "{checks}");
    assert_eq!(checks["failing"], 1, "{checks}");
    assert_eq!(checks["pending"], 0, "{checks}");
    assert_eq!(checks["failing_names"], json!(["Lint"]), "{checks}");
}

#[test]
fn mixed_case_reference_uses_the_canonical_repository() {
    let server = serve(|req: &Req| {
        let path = route(&req.path);
        if path.eq_ignore_ascii_case("/repos/curie-eng/curie") {
            return Resp {
                status: 200,
                body: json!({
                    "id": 101,
                    "html_url": "https://github.com/curie-eng/curie",
                    "name": "curie",
                    "owner": {"login": "curie-eng"}
                })
                .to_string(),
                extra: vec![],
            };
        }
        if path == "/repos/curie-eng/curie/issues/12" {
            return Resp {
                status: 200,
                body: json!({"number": 12, "title": "Explain", "state": "open"}).to_string(),
                extra: vec![],
            };
        }
        if path == "/repos/curie-eng/curie/issues/12/timeline" {
            return Resp {
                status: 200,
                body: json!([{
                    "event": "cross-referenced",
                    "created_at": "2026-03-01T00:00:00Z",
                    "source": {"issue": {
                        "number": 7,
                        "created_at": "2026-03-01T00:00:00Z",
                        "pull_request": {"url": "https://api.github.com/repos/Curie-Eng/Curie/pulls/7"}
                    }}
                }])
                .to_string(),
                extra: vec![],
            };
        }
        if path == "/repos/curie-eng/curie/pulls/7" {
            return Resp {
                status: 200,
                body: json!({
                    "number": 7,
                    "html_url": "https://github.com/curie-eng/curie/pull/7",
                    "title": "Change 7",
                    "state": "open",
                    "merged_at": null,
                    "created_at": "2026-03-01T00:00:00Z",
                    "body": format!("Summary.\n{}", marker(12)),
                    "head": {"sha": "abc07"}
                })
                .to_string(),
                extra: vec![],
            };
        }
        if path.contains("/reviews") || path.contains("/statuses") {
            return Resp {
                status: 200,
                body: "[]".into(),
                extra: vec![],
            };
        }
        if path.contains("/check-runs") {
            return Resp {
                status: 200,
                body: json!({"total_count": 0, "check_runs": []}).to_string(),
                extra: vec![],
            };
        }
        Resp {
            status: 404,
            body: "{}".into(),
            extra: vec![],
        }
    });
    let output = run(&["Curie-Eng/Curie#12", "--json"], &server, &token(), None);
    assert_eq!(
        output.status.code(),
        Some(0),
        "stdout={} stderr={}",
        text(&output.stdout),
        text(&output.stderr)
    );
    let body = json_stdout(&output);
    assert_eq!(body["pull_requests"][0]["number"], 7, "{body}");
    let recorded = server.recorded();
    let paths: Vec<&str> = recorded.iter().map(|req| route(&req.path)).collect();
    assert_eq!(
        paths.first().copied(),
        Some("/repos/Curie-Eng/Curie"),
        "{paths:?}"
    );
    assert!(
        paths.contains(&"/repos/curie-eng/curie/issues/12"),
        "{paths:?}"
    );
    assert!(
        paths.contains(&"/repos/curie-eng/curie/pulls/7"),
        "{paths:?}"
    );
    assert!(
        paths.iter().skip(1).all(|path| !path.contains("Curie-Eng")),
        "{paths:?}"
    );
}

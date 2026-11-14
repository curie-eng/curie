//! `curie cluster subscription` at the binary boundary (#5377): the built CLI
//! against a wire-level mock of the owner enrollment API.
//!
//! Every test spawns the real binary with a private session file and, where the
//! verb sends one, a private token file. Stub `kubectl`, `helm` and `docker`
//! lead PATH and log each call, so a test can prove the release's platform key
//! is never discovered. The ambient platform key is set where a test needs to
//! prove it is never sent, and removed everywhere else.

mod support;

use std::ffi::OsString;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

use crate::support::closed_port::ClosedPort;
use crate::support::{executable, serve, MockServer, Request, Response};

const COOKIE: &str = "cookie-SENTINEL-1a2b3c";
const TOKEN: &str = "sk-ant-oat01-TOKENSENTINELexample0123";
const AMBIENT_KEY: &str = "ambient-platform-key-SENTINEL";
const BODY_SENTINEL: &str = "server-body-SENTINEL";
const ENROLLMENT: &str = "/personal-claude/enrollment";

/// The shared token rule cases, the same file the API's token tests read.
const TOKEN_VECTOR: &str = include_str!("../../tests/vectors/personal-enrollment-token.json");

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

struct Fixture {
    temp: tempfile::TempDir,
    path: OsString,
    tool_log: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().expect("create subscription fixture");
        let tool_log = temp.path().join("tools.log");
        let tools = temp.path().join("bin");
        fs::create_dir(&tools).expect("create stub tool dir");
        for tool in ["kubectl", "helm", "docker"] {
            executable::install_in(
                &tools,
                tool,
                "#!/bin/sh\nprintf '%s %s\\n' \"$0\" \"$*\" >> \"$CURIE_TEST_TOOL_LOG\"\nexit 64\n",
            );
        }
        let mut paths = vec![tools];
        paths.extend(std::env::split_paths(
            &std::env::var_os("PATH").unwrap_or_default(),
        ));
        Self {
            temp,
            path: std::env::join_paths(paths).expect("join fixture PATH"),
            tool_log,
        }
    }

    /// Write `content` to a fresh file with exactly `mode`.
    fn file(&self, name: &str, content: &[u8], mode: u32) -> PathBuf {
        let path = self.temp.path().join(name);
        fs::write(&path, content).expect("write fixture file");
        fs::set_permissions(&path, fs::Permissions::from_mode(mode)).expect("set fixture mode");
        path
    }

    fn session(&self) -> PathBuf {
        self.file("session", format!("{COOKIE}\n").as_bytes(), 0o600)
    }

    fn token(&self) -> PathBuf {
        self.file("token", format!("{TOKEN}\n").as_bytes(), 0o600)
    }

    fn run(&self, args: &[&str]) -> Output {
        self.run_with(args, &[])
    }

    fn run_with(&self, args: &[&str], env: &[(&str, &str)]) -> Output {
        let mut command = Command::new(bin());
        command
            .args(args)
            .current_dir(self.temp.path())
            .stdin(Stdio::null())
            .env("PATH", &self.path)
            .env("CURIE_TEST_TOOL_LOG", &self.tool_log)
            .env("CURIE_CONFIG_DIR", self.temp.path().join("config"))
            .env("KUBECONFIG", "/nonexistent")
            .env("NO_COLOR", "1")
            .env_remove("CURIE_API_KEY")
            .env_remove("CURIE_API_URL")
            .env_remove("CURIE_NAMESPACE");
        for (key, value) in env {
            command.env(key, value);
        }
        command.output().expect("run curie cluster subscription")
    }

    fn tool_calls(&self) -> String {
        fs::read_to_string(&self.tool_log).unwrap_or_default()
    }
}

fn text(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned() + &String::from_utf8_lossy(&output.stderr)
}

fn args<'a>(verb: &'a str, session: &'a Path, base: &'a str) -> Vec<&'a str> {
    vec![
        "--json",
        "cluster",
        "subscription",
        verb,
        "--session-file",
        session.to_str().expect("utf-8 path"),
        "--api-url",
        base,
    ]
}

/// stdout must be exactly one JSON object on one line.
fn one_json_object(output: &Output) -> serde_json::Value {
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(
        stdout.trim_end().lines().count(),
        1,
        "stdout must be one JSON line: {stdout}"
    );
    let value: serde_json::Value = serde_json::from_str(&stdout)
        .unwrap_or_else(|error| panic!("stdout is not JSON ({error}): {stdout}"));
    assert!(value.is_object(), "stdout is not an object: {stdout}");
    value
}

fn assert_schema_valid(value: &serde_json::Value) {
    let schema: serde_json::Value =
        serde_json::from_str(include_str!("../schema/subscription.schema.json"))
            .expect("subscription schema parses");
    let validator = jsonschema::validator_for(&schema).expect("subscription schema compiles");
    let errors: Vec<String> = validator
        .iter_errors(value)
        .map(|error| error.to_string())
        .collect();
    assert!(errors.is_empty(), "{value} violates the schema: {errors:?}");
}

fn assert_no_credentials(output: &Output) {
    let shown = text(output);
    for secret in [COOKIE, TOKEN, AMBIENT_KEY] {
        assert!(
            !shown.contains(secret),
            "output disclosed {secret}: {shown}"
        );
    }
}

fn enrollment(state: &str, version: Option<i64>, pending: i64) -> String {
    serde_json::json!({
        "state": state,
        "generation": version,
        "version": version,
        "teardown": {"pending_runtimes": pending, "pending_custody_cleanup": 0},
    })
    .to_string()
}

fn refusal(status: u16, detail: serde_json::Value) -> Response {
    Response::json(
        status,
        &serde_json::json!({"detail": detail, "echo": BODY_SENTINEL}).to_string(),
    )
}

fn body(request: &Request) -> serde_json::Value {
    serde_json::from_slice(&request.body).expect("request body is JSON")
}

fn cookie_header() -> String {
    format!("__Host-curie_console_session={COOKIE}")
}

#[test]
fn status_sends_only_the_session_cookie() {
    let fixture = Fixture::new();
    let server = serve(|_| Response::json(200, &enrollment("active", Some(3), 1)));
    let session = fixture.session();

    let output = fixture.run_with(
        &args("status", &session, &server.base_url),
        &[("CURIE_API_KEY", AMBIENT_KEY)],
    );

    assert_eq!(output.status.code(), Some(0), "{}", text(&output));
    let value = one_json_object(&output);
    assert_schema_valid(&value);
    assert_eq!(value["state"], "active");
    assert_eq!(value["version"], 3);
    assert_eq!(value["teardown"]["pending_runtimes"], 1);
    let requests = server.recorded();
    assert_eq!(requests.len(), 1, "{requests:?}");
    let request = &requests[0];
    assert_eq!(request.method, "GET");
    assert_eq!(request.path, ENROLLMENT);
    assert_eq!(request.header("cookie"), Some(cookie_header().as_str()));
    assert_eq!(
        request.header("x-api-key"),
        None,
        "the platform key was sent"
    );
    assert!(
        request
            .headers
            .iter()
            .all(|(_, value)| !value.contains(AMBIENT_KEY)),
        "the ambient key reached a header: {:?}",
        request.headers
    );
    assert_no_credentials(&output);
    assert_eq!(fixture.tool_calls(), "");
}

#[test]
fn enroll_sends_the_token_file_and_never_prints_it() {
    let fixture = Fixture::new();
    let server = serve(|_| Response::json(200, &enrollment("active", Some(1), 0)));
    let session = fixture.session();
    let token = fixture.token();
    let token_arg = token.to_str().expect("utf-8 path");

    for extra in [
        &["--json"][..],
        &["--json", "--debug"][..],
        &[][..],
        &["--debug"][..],
    ] {
        let mut argv: Vec<&str> = extra.to_vec();
        argv.extend(
            args("enroll", &session, &server.base_url)
                .into_iter()
                .skip(1),
        );
        argv.extend(["--token-file", token_arg]);
        let output = fixture.run(&argv);

        assert_eq!(output.status.code(), Some(0), "{argv:?}: {}", text(&output));
        assert_no_credentials(&output);
        if extra.contains(&"--json") {
            let value = one_json_object(&output);
            assert_schema_valid(&value);
            assert_eq!(value["action"], "enroll");
            assert_eq!(value["changed"], true);
            assert_eq!(value["previous_version"], serde_json::Value::Null);
            assert_eq!(value["generation"], 1);
        } else {
            assert!(
                String::from_utf8_lossy(&output.stdout).contains("enrolled"),
                "{}",
                text(&output)
            );
        }
    }

    let requests = server.recorded();
    assert_eq!(requests.len(), 4);
    for request in &requests {
        assert_eq!(request.method, "PUT");
        assert_eq!(request.path, ENROLLMENT);
        assert_eq!(request.header("cookie"), Some(cookie_header().as_str()));
        assert_eq!(request.header("origin"), Some(server.base_url.as_str()));
        assert_eq!(request.header("x-api-key"), None);
        assert_eq!(
            body(request),
            serde_json::json!({"intent": "enroll", "expected_version": null, "token": TOKEN}),
            "the token must be the file content minus one trailing newline"
        );
    }
}

#[test]
fn replace_without_expected_version_reads_status_first() {
    let fixture = Fixture::new();
    let server = serve(|request| match request.method.as_str() {
        "GET" => Response::json(200, &enrollment("active", Some(3), 0)),
        _ => Response::json(200, &enrollment("active", Some(4), 1)),
    });
    let session = fixture.session();
    let token = fixture.token();
    let mut argv = args("replace", &session, &server.base_url);
    argv.extend(["--token-file", token.to_str().expect("utf-8 path")]);

    let output = fixture.run(&argv);

    assert_eq!(output.status.code(), Some(0), "{}", text(&output));
    let value = one_json_object(&output);
    assert_schema_valid(&value);
    assert_eq!(value["action"], "replace");
    assert_eq!(value["previous_version"], 3);
    assert_eq!(value["version"], 4);
    assert_eq!(value["changed"], true);
    let requests = server.recorded();
    let methods: Vec<&str> = requests.iter().map(|r| r.method.as_str()).collect();
    assert_eq!(methods, ["GET", "PUT"]);
    assert_eq!(
        body(&requests[1]),
        serde_json::json!({"intent": "replace", "expected_version": 3, "token": TOKEN})
    );
    assert_no_credentials(&output);
}

#[test]
fn replace_sends_the_given_expected_version() {
    let fixture = Fixture::new();
    let server = serve(|_| Response::json(200, &enrollment("active", Some(8), 0)));
    let session = fixture.session();
    let token = fixture.token();
    let mut argv = args("replace", &session, &server.base_url);
    argv.extend([
        "--token-file",
        token.to_str().expect("utf-8 path"),
        "--expected-version",
        "7",
    ]);

    let output = fixture.run(&argv);

    assert_eq!(output.status.code(), Some(0), "{}", text(&output));
    assert_eq!(one_json_object(&output)["previous_version"], 7);
    let requests = server.recorded();
    assert_eq!(
        requests.len(),
        1,
        "no status read when the version is given"
    );
    assert_eq!(requests[0].method, "PUT");
    assert_eq!(
        body(&requests[0]),
        serde_json::json!({"intent": "replace", "expected_version": 7, "token": TOKEN})
    );
}

#[test]
fn disconnect_sends_expected_version_as_a_query_and_origin_header() {
    for (answer, changed) in [(Some(6), true), (Some(5), false)] {
        let fixture = Fixture::new();
        let server = serve(move |_| Response::json(200, &enrollment("inactive", answer, 1)));
        let session = fixture.session();
        let mut argv = args("disconnect", &session, &server.base_url);
        argv.extend(["--expected-version", "5"]);

        let output = fixture.run(&argv);

        assert_eq!(output.status.code(), Some(0), "{}", text(&output));
        let value = one_json_object(&output);
        assert_schema_valid(&value);
        assert_eq!(value["action"], "disconnect");
        assert_eq!(value["state"], "inactive");
        assert_eq!(value["changed"], changed, "{value}");
        let requests = server.recorded();
        assert_eq!(requests.len(), 1);
        let request = &requests[0];
        assert_eq!(request.method, "DELETE");
        assert_eq!(request.path, format!("{ENROLLMENT}?expected_version=5"));
        assert_eq!(request.header("origin"), Some(server.base_url.as_str()));
        assert_eq!(request.header("cookie"), Some(cookie_header().as_str()));
        assert!(request.body.is_empty(), "DELETE carries no body");
    }
}

#[test]
fn reconnect_requires_a_token_file() {
    let fixture = Fixture::new();
    let server = serve(|_| Response::json(200, &enrollment("active", Some(6), 0)));
    let session = fixture.session();
    let mut argv = args("reconnect", &session, &server.base_url);
    argv.extend(["--expected-version", "5"]);

    let missing = fixture.run(&argv);
    assert_eq!(missing.status.code(), Some(2), "{}", text(&missing));
    assert!(
        text(&missing).contains("--token-file"),
        "{}",
        text(&missing)
    );
    assert!(
        server.recorded().is_empty(),
        "a refused input made a request"
    );

    let token = fixture.token();
    argv.extend(["--token-file", token.to_str().expect("utf-8 path")]);
    let output = fixture.run(&argv);
    assert_eq!(output.status.code(), Some(0), "{}", text(&output));
    assert_eq!(
        body(&server.recorded()[0]),
        serde_json::json!({"intent": "reconnect", "expected_version": 5, "token": TOKEN})
    );
}

#[test]
fn group_or_world_readable_session_or_token_file_is_a_usage_error_with_no_request() {
    let server = serve(|_| Response::json(200, &enrollment("active", Some(1), 0)));
    for mode in [0o640, 0o604, 0o620, 0o602, 0o660, 0o644, 0o610, 0o601] {
        for exposed in ["session", "token"] {
            let fixture = Fixture::new();
            let session = if exposed == "session" {
                fixture.file("session", format!("{COOKIE}\n").as_bytes(), mode)
            } else {
                fixture.session()
            };
            let token = if exposed == "token" {
                fixture.file("token", format!("{TOKEN}\n").as_bytes(), mode)
            } else {
                fixture.token()
            };
            let mut argv = args("enroll", &session, &server.base_url);
            argv.extend(["--token-file", token.to_str().expect("utf-8 path")]);

            let output = fixture.run(&argv);

            assert_eq!(
                output.status.code(),
                Some(2),
                "{exposed} mode {mode:o}: {}",
                text(&output)
            );
            let error = one_json_object(&output);
            let message = error["error"].as_str().unwrap_or_default();
            assert!(message.contains(&format!("{exposed} file")), "{message}");
            assert!(
                error["fix"]
                    .as_str()
                    .unwrap_or_default()
                    .contains("chmod 600"),
                "{error}"
            );
            assert_no_credentials(&output);
        }
    }
    assert!(
        server.recorded().is_empty(),
        "a refused file made a request"
    );

    // An owner-only, read-only file is accepted.
    let fixture = Fixture::new();
    let session = fixture.file("session", format!("{COOKIE}\n").as_bytes(), 0o400);
    let token = fixture.file("token", format!("{TOKEN}\n").as_bytes(), 0o400);
    let mut argv = args("enroll", &session, &server.base_url);
    argv.extend(["--token-file", token.to_str().expect("utf-8 path")]);
    let output = fixture.run(&argv);
    assert_eq!(output.status.code(), Some(0), "{}", text(&output));
    assert_eq!(server.recorded().len(), 1);
}

#[test]
fn empty_or_malformed_files_are_usage_errors() {
    let server = serve(|_| Response::json(200, &enrollment("active", Some(1), 0)));
    let long_cookie = "c".repeat(4097);
    let session_cases: Vec<(&str, Vec<u8>)> = vec![
        ("empty", Vec::new()),
        ("newline only", b"\n".to_vec()),
        ("crlf only", b"\r\n".to_vec()),
        (
            "two trailing newlines",
            format!("{COOKIE}\n\n").into_bytes(),
        ),
        ("inner space", format!("{COOKIE} x\n").into_bytes()),
        ("semicolon", format!("{COOKIE};x\n").into_bytes()),
        ("double quote", format!("\"{COOKIE}\"\n").into_bytes()),
        ("non ascii", format!("{COOKIE}\u{e9}\n").into_bytes()),
        (
            "cookie name included",
            format!("__Host-curie_console_session={COOKIE}\n").into_bytes(),
        ),
        ("over 4096 bytes", format!("{long_cookie}\n").into_bytes()),
    ];
    for (name, content) in &session_cases {
        let fixture = Fixture::new();
        let session = fixture.file("session", content, 0o600);
        let output = fixture.run(&args("status", &session, &server.base_url));
        assert_eq!(output.status.code(), Some(2), "{name}: {}", text(&output));
        let error = one_json_object(&output);
        assert!(
            error["error"]
                .as_str()
                .unwrap_or_default()
                .contains("session file"),
            "{name}: {error}"
        );
        assert_no_credentials(&output);
    }

    let fixture = Fixture::new();
    let directory = fixture.temp.path().join("a-directory");
    fs::create_dir(&directory).expect("create directory");
    fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).expect("chmod");
    let fifo = fixture.temp.path().join("a-fifo");
    let fifo_c = std::ffi::CString::new(fifo.to_str().expect("utf-8")).expect("c path");
    // SAFETY: mkfifo with a NUL-terminated path; the result is checked.
    assert_eq!(unsafe { libc::mkfifo(fifo_c.as_ptr(), 0o600) }, 0, "mkfifo");
    let missing = fixture.temp.path().join("missing");
    for (name, path) in [
        ("directory", &directory),
        ("fifo", &fifo),
        ("missing", &missing),
    ] {
        let output = fixture.run(&args("status", path, &server.base_url));
        assert_eq!(output.status.code(), Some(2), "{name}: {}", text(&output));
        assert!(text(&output).contains("session file"), "{name}");
    }

    let token_cases: Vec<(&str, Vec<u8>)> = vec![
        ("empty", Vec::new()),
        ("newline only", b"\n".to_vec()),
        ("two trailing newlines", format!("{TOKEN}\n\n").into_bytes()),
    ];
    for (name, content) in &token_cases {
        let fixture = Fixture::new();
        let session = fixture.session();
        let token = fixture.file("token", content, 0o600);
        let mut argv = args("enroll", &session, &server.base_url);
        argv.extend(["--token-file", token.to_str().expect("utf-8 path")]);
        let output = fixture.run(&argv);
        assert_eq!(output.status.code(), Some(2), "{name}: {}", text(&output));
        assert!(text(&output).contains("token file"), "{name}");
        assert_no_credentials(&output);
    }
    assert!(
        server.recorded().is_empty(),
        "a refused file made a request"
    );

    // Exactly one trailing CRLF is trimmed, and the value is otherwise sent as is.
    let fixture = Fixture::new();
    let session = fixture.file("session", format!("{COOKIE}\r\n").as_bytes(), 0o600);
    let output = fixture.run(&args("status", &session, &server.base_url));
    assert_eq!(output.status.code(), Some(0), "{}", text(&output));
    assert_eq!(
        server.recorded()[0].header("cookie"),
        Some(cookie_header().as_str())
    );
}

#[test]
fn token_validation_follows_the_shared_vector() {
    let vector: serde_json::Value = serde_json::from_str(TOKEN_VECTOR).expect("token vector");
    let cases = vector["cases"].as_array().expect("vector cases");
    assert!(cases.len() >= 9, "the vector lost cases");
    for case in cases {
        let name = case["name"].as_str().expect("case name");
        let token = match &case["token"] {
            serde_json::Value::String(token) => token.clone(),
            _ => {
                let repeat = &case["token_repeat"];
                repeat["char"]
                    .as_str()
                    .expect("repeat char")
                    .repeat(repeat["count"].as_u64().expect("repeat count") as usize)
            }
        };
        let valid = case["valid"].as_bool().expect("case validity");
        let fixture = Fixture::new();
        let server = serve(|_| Response::json(200, &enrollment("active", Some(1), 0)));
        let session = fixture.session();
        let token_file = fixture.file("token", format!("{token}\n").as_bytes(), 0o600);
        let mut argv = args("enroll", &session, &server.base_url);
        argv.extend(["--token-file", token_file.to_str().expect("utf-8 path")]);

        let output = fixture.run(&argv);

        let requests = server.recorded();
        if valid {
            assert_eq!(output.status.code(), Some(0), "{name}: {}", text(&output));
            assert_eq!(requests.len(), 1, "{name}");
            assert_eq!(body(&requests[0])["token"], token.as_str(), "{name}");
        } else {
            assert_eq!(output.status.code(), Some(2), "{name}: {}", text(&output));
            assert!(requests.is_empty(), "{name}: an invalid token was sent");
            if !token.is_empty() {
                assert!(!text(&output).contains(&token), "{name}: token echoed");
            }
        }
    }
}

/// Run `verb` against a server that answers every request with `response`.
fn refused_by(
    verb: &str,
    response: impl Fn() -> Response + Send + Sync + 'static,
) -> (Output, MockServer) {
    let fixture = Fixture::new();
    let server = serve(move |_| response());
    let session = fixture.session();
    let token = fixture.token();
    let mut argv = args(verb, &session, &server.base_url);
    if verb != "status" && verb != "disconnect" {
        argv.extend(["--token-file", token.to_str().expect("utf-8 path")]);
    }
    if verb != "status" && verb != "enroll" {
        argv.extend(["--expected-version", "2"]);
    }
    let output = fixture.run(&argv);
    assert_no_credentials(&output);
    assert!(
        !text(&output).contains(BODY_SENTINEL),
        "a server error body was echoed: {}",
        text(&output)
    );
    (output, server)
}

fn error_and_fix(output: &Output) -> (String, String) {
    let value = one_json_object(output);
    assert_eq!(
        value.as_object().map(|object| object.len()),
        Some(2),
        "errors are {{error, fix}}: {value}"
    );
    (
        value["error"].as_str().unwrap_or_default().to_string(),
        value["fix"].as_str().unwrap_or_default().to_string(),
    )
}

#[test]
fn unauthorized_is_failure_with_a_session_refresh_fix() {
    let (output, _server) = refused_by("status", || {
        refusal(
            401,
            serde_json::json!("missing, invalid, or expired principal session"),
        )
    });
    assert_eq!(output.status.code(), Some(1), "{}", text(&output));
    let (error, session_fix) = error_and_fix(&output);
    assert!(error.contains("401"), "{error}");
    assert!(session_fix.contains("--session-file"), "{session_fix}");
    assert!(session_fix.contains("sign in"), "{session_fix}");

    let (output, _server) = refused_by("enroll", || {
        refusal(403, serde_json::json!("console session origin rejected"))
    });
    assert_eq!(output.status.code(), Some(1), "{}", text(&output));
    let (error, origin_fix) = error_and_fix(&output);
    assert!(error.contains("403"), "{error}");
    assert!(origin_fix.contains("--api-url"), "{origin_fix}");
    assert_ne!(session_fix, origin_fix, "401 and 403 need distinct fixes");
}

#[test]
fn conflict_is_failure_with_a_status_fix() {
    let mut conflict_fix = String::new();
    for code in [
        "stale_version",
        "enrollment_exists",
        "enrollment_absent",
        "enrollment_active",
        "enrollment_inactive",
        "staging_conflict",
    ] {
        let (output, _server) = refused_by("replace", move || {
            refusal(409, serde_json::json!({"code": code}))
        });
        assert_eq!(output.status.code(), Some(1), "{code}: {}", text(&output));
        let (error, fix) = error_and_fix(&output);
        assert!(error.contains(code), "{code}: {error}");
        assert!(fix.contains("subscription status"), "{code}: {fix}");
        assert!(fix.contains("--expected-version"), "{code}: {fix}");
        conflict_fix = fix;
    }

    let (output, _server) = refused_by("disconnect", || {
        refusal(404, serde_json::json!({"code": "enrollment_absent"}))
    });
    assert_eq!(output.status.code(), Some(1), "{}", text(&output));
    let (error, absent_fix) = error_and_fix(&output);
    assert!(error.contains("enrollment_absent"), "{error}");
    assert!(absent_fix.contains("subscription enroll"), "{absent_fix}");
    assert_ne!(absent_fix, conflict_fix, "404 and 409 need distinct fixes");

    let (output, _server) = refused_by("enroll", || {
        refusal(
            422,
            serde_json::json!({"code": "invalid_enrollment_request"}),
        )
    });
    assert_eq!(output.status.code(), Some(2), "{}", text(&output));
}

#[test]
fn unreachable_api_and_custody_unavailable_are_transient() {
    let fixture = Fixture::new();
    let closed = ClosedPort::new();
    let session = fixture.session();
    let output = fixture.run(&args("status", &session, &closed.url()));
    assert_eq!(output.status.code(), Some(3), "{}", text(&output));
    assert_no_credentials(&output);

    for code in [
        "custody_unavailable",
        "enrollment_busy",
        "personal_rollout_in_progress",
    ] {
        let (output, _server) = refused_by("replace", move || {
            refusal(503, serde_json::json!({"code": code}))
        });
        assert_eq!(output.status.code(), Some(3), "{code}: {}", text(&output));
        let (error, fix) = error_and_fix(&output);
        assert!(error.contains(code), "{error}");
        assert!(fix.contains("retry"), "{fix}");
    }
}

#[test]
fn custody_unconfigured_is_failure_naming_the_chart_value() {
    let (output, _server) = refused_by("enroll", || {
        refusal(503, serde_json::json!({"code": "custody_unconfigured"}))
    });
    assert_eq!(output.status.code(), Some(1), "{}", text(&output));
    let (error, fix) = error_and_fix(&output);
    assert!(error.contains("custody_unconfigured"), "{error}");
    assert!(fix.contains("personalClaude.grantNamespace"), "{fix}");
}

#[test]
fn dry_run_emits_one_plan_and_makes_no_request() {
    let server = serve(|_| Response::json(200, &enrollment("active", Some(1), 0)));
    let cases: &[(&str, &[&str], &[&str])] = &[
        ("status", &[], &["GET "]),
        ("enroll", &["--token-file"], &["PUT ", "intent=enroll"]),
        (
            "replace",
            &["--token-file"],
            &["GET ", "PUT ", "intent=replace"],
        ),
        (
            "reconnect",
            &["--token-file", "--expected-version", "4"],
            &["PUT ", "expected_version=4"],
        ),
        (
            "disconnect",
            &["--expected-version", "9"],
            &["DELETE ", "?expected_version=9"],
        ),
    ];
    for (verb, extra, expected) in cases {
        for direct in [true, false] {
            let fixture = Fixture::new();
            let session = fixture.session();
            let token = fixture.token();
            let mut argv = vec![
                "--json",
                "cluster",
                "subscription",
                verb,
                "--session-file",
                session.to_str().expect("utf-8 path"),
                "--dry-run",
            ];
            if direct {
                argv.extend(["--api-url", server.base_url.as_str()]);
            }
            for flag in *extra {
                argv.push(flag);
                if *flag == "--token-file" {
                    argv.push(token.to_str().expect("utf-8 path"));
                }
            }

            let output = fixture.run(&argv);

            assert_eq!(output.status.code(), Some(0), "{verb}: {}", text(&output));
            let plan = one_json_object(&output);
            assert_schema_valid(&plan);
            assert_eq!(plan["dry_run"], true, "{verb}: {plan}");
            let lines = plan["plan"].to_string();
            for needle in *expected {
                assert!(
                    lines.contains(needle),
                    "{verb} plan lacks {needle}: {lines}"
                );
            }
            if !direct {
                assert!(lines.contains("port-forward"), "{verb}: {lines}");
            }
            assert_no_credentials(&output);
            assert_eq!(fixture.tool_calls(), "", "{verb}: a dry run ran a tool");
        }
    }
    assert!(server.recorded().is_empty(), "a dry run made a request");
}

#[test]
fn never_discovers_the_platform_key() {
    let fixture = Fixture::new();
    let server = serve(|request| match request.method.as_str() {
        "GET" => Response::json(200, &enrollment("active", Some(2), 0)),
        "DELETE" => Response::json(200, &enrollment("inactive", Some(3), 0)),
        _ => Response::json(200, &enrollment("active", Some(3), 0)),
    });
    let session = fixture.session();
    let token = fixture.token();
    let token_arg = token.to_str().expect("utf-8 path");
    let env = [("CURIE_API_KEY", AMBIENT_KEY)];
    for (verb, extra) in [
        ("status", &[][..]),
        ("enroll", &["--token-file", token_arg][..]),
        ("replace", &["--token-file", token_arg][..]),
        ("disconnect", &[][..]),
    ] {
        let mut argv = args(verb, &session, &server.base_url);
        argv.extend(extra);
        let output = fixture.run_with(&argv, &env);
        assert_eq!(output.status.code(), Some(0), "{verb}: {}", text(&output));
        assert_no_credentials(&output);
    }
    assert_eq!(fixture.tool_calls(), "", "an explicit --api-url ran a tool");
    for request in server.recorded() {
        assert_eq!(request.header("x-api-key"), None, "{request:?}");
    }

    // Without --api-url the verb self-plumbs a port-forward, and still never
    // reads the release Secret that holds the platform key.
    let output = fixture.run_with(
        &[
            "--json",
            "cluster",
            "subscription",
            "status",
            "--session-file",
            session.to_str().expect("utf-8 path"),
        ],
        &env,
    );
    assert_ne!(
        output.status.code(),
        Some(0),
        "the stub kubectl cannot forward"
    );
    let calls = fixture.tool_calls();
    assert!(calls.contains("port-forward"), "{calls}");
    assert!(
        !calls.contains("secret"),
        "the release Secret was read: {calls}"
    );
    assert_no_credentials(&output);
}

#[test]
fn cleartext_non_loopback_api_url_is_refused() {
    let fixture = Fixture::new();
    let session = fixture.session();
    for dry_run in [false, true] {
        let mut argv = args("status", &session, "http://api.example.invalid");
        if dry_run {
            argv.push("--dry-run");
        }
        let output = fixture.run(&argv);
        assert_eq!(output.status.code(), Some(2), "{}", text(&output));
        let (error, fix) = error_and_fix(&output);
        assert!(error.contains("cleartext"), "{error}");
        assert!(fix.contains("https://"), "{fix}");
        assert_no_credentials(&output);
    }
    assert_eq!(fixture.tool_calls(), "");

    // HTTPS to the same host is accepted (planned offline here).
    let mut argv = args("status", &session, "https://api.example.invalid");
    argv.push("--dry-run");
    let output = fixture.run(&argv);
    assert_eq!(output.status.code(), Some(0), "{}", text(&output));
}

mod support;

use std::ffi::OsString;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

use base64::Engine;
use serde_json::{json, Value};

use support::{executable, serve, Request, Response};

const COOKIE: &str = "coding-owner-cookie-SENTINEL";
const AMBIENT_KEY: &str = "ambient-platform-key-SENTINEL";
const CONTENT: &str = "instruction-content-SENTINEL\r\n  retain whitespace \t\r\n";
const BODY_SENTINEL: &str = "server-body-SENTINEL";
const VECTORS: &str = include_str!("../../tests/vectors/requester-coding-instructions.json");

struct Fixture {
    temp: tempfile::TempDir,
    path: OsString,
    tool_log: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().expect("create instruction fixture");
        let tool_log = temp.path().join("tools.log");
        let tools = temp.path().join("bin");
        fs::create_dir(&tools).expect("create tool fixture");
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

    fn file(&self, name: &str, bytes: &[u8], mode: u32) -> PathBuf {
        let path = self.temp.path().join(name);
        fs::write(&path, bytes).expect("write fixture file");
        fs::set_permissions(&path, fs::Permissions::from_mode(mode)).expect("set fixture mode");
        path
    }

    fn session(&self) -> PathBuf {
        self.file("session", format!("{COOKIE}\n").as_bytes(), 0o600)
    }

    fn run(&self, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_curie"))
            .args(args)
            .current_dir(self.temp.path())
            .stdin(Stdio::null())
            .env("PATH", &self.path)
            .env("CURIE_TEST_TOOL_LOG", &self.tool_log)
            .env("CURIE_CONFIG_DIR", self.temp.path().join("config"))
            .env("KUBECONFIG", "/nonexistent")
            .env("CURIE_API_KEY", AMBIENT_KEY)
            .env("NO_COLOR", "1")
            .env_remove("CURIE_API_URL")
            .env_remove("CURIE_NAMESPACE")
            .output()
            .expect("run coding instructions command")
    }

    fn tool_calls(&self) -> String {
        fs::read_to_string(&self.tool_log).unwrap_or_default()
    }
}

fn args<'a>(verb: &'a str, session: &'a Path, base: &'a str) -> Vec<&'a str> {
    vec![
        "--json",
        "cluster",
        "coding-instructions",
        verb,
        "--session-file",
        session.to_str().expect("UTF-8 fixture path"),
        "--api-url",
        base,
    ]
}

fn text(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned() + &String::from_utf8_lossy(&output.stderr)
}

fn object(output: &Output) -> Value {
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(stdout.trim_end().lines().count(), 1, "{stdout}");
    let value: Value = serde_json::from_str(&stdout).expect("stdout is exactly one JSON value");
    assert!(value.is_object(), "{value}");
    value
}

fn status(revision: i64, text: Option<&str>) -> Value {
    json!({
        "revision": revision,
        "sha256": text.map(|_| "a".repeat(64)),
        "format_version": 1,
        "byte_length": text.map_or(0, str::len),
        "text": text,
        "updated_at": (revision > 0).then_some("2026-11-13T00:00:00Z"),
        "tenant_enabled": true,
        "application": "unavailable",
    })
}

fn json_response(value: &Value) -> Response {
    Response::json(200, &value.to_string())
}

fn refusal(status: u16, code: &str, current_revision: Option<Value>) -> Response {
    Response::json(
        status,
        &json!({
            "detail": {"code": code, "current_revision": current_revision},
            "echo": format!("{CONTENT} {COOKIE} {AMBIENT_KEY} {BODY_SENTINEL}"),
        })
        .to_string(),
    )
}

fn assert_cookie_only(request: &Request) {
    assert_eq!(
        request.header("cookie"),
        Some(format!("__Host-curie_console_session={COOKIE}").as_str())
    );
    assert_eq!(request.header("x-api-key"), None);
    assert_eq!(request.header("authorization"), None);
    assert!(!String::from_utf8_lossy(&request.body).contains(AMBIENT_KEY));
}

fn assert_no_credentials(output: &Output) {
    let rendered = text(output);
    for sentinel in [COOKIE, AMBIENT_KEY, BODY_SENTINEL] {
        assert!(!rendered.contains(sentinel), "output disclosed {sentinel}");
    }
}

fn assert_error(output: &Output, exit_code: i32) -> Value {
    assert_eq!(output.status.code(), Some(exit_code), "{}", text(output));
    let value = object(output);
    assert_eq!(value.as_object().expect("object").len(), 2);
    assert!(value["error"].is_string(), "{value}");
    assert!(value["fix"].is_string(), "{value}");
    assert_no_credentials(output);
    assert!(!text(output).contains("instruction-content-SENTINEL"));
    value
}

#[test]
fn get_preserves_every_response_field_and_sends_only_the_session_cookie() {
    let fixture = Fixture::new();
    let session = fixture.session();
    for expected in [status(0, None), status(7, Some("use exact results\n"))] {
        let response = expected.clone();
        let server = serve(move |_| json_response(&response));
        let output = fixture.run(&args("get", &session, &server.base_url));
        assert_eq!(output.status.code(), Some(0), "{}", text(&output));
        assert_eq!(object(&output), expected);
        assert_eq!(object(&output)["application"], "unavailable");
        let requests = server.recorded();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method, "GET");
        assert_eq!(requests[0].path, "/coding-instructions");
        assert_eq!(requests[0].header("origin"), None);
        assert_cookie_only(&requests[0]);
        assert_no_credentials(&output);
    }
    assert_eq!(fixture.tool_calls(), "");
}

#[test]
fn shared_vectors_preserve_file_bytes_or_refuse_invalid_utf8_before_any_request() {
    let vectors: Value = serde_json::from_str(VECTORS).expect("shared vectors parse");
    let fixture = Fixture::new();
    let session = fixture.session();
    for case in vectors["cases"].as_array().expect("shared cases") {
        let name = case["name"].as_str().expect("case name");
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(case["input_base64"].as_str().expect("base64 input"))
            .expect("shared base64 decodes");
        let file = fixture.file("instructions", &bytes, 0o644);
        let response = if let Some(error) = case["error"].as_str() {
            json!({"detail": {"code": error}, "echo": BODY_SENTINEL})
        } else {
            let mut response = status(
                1,
                case["normalized_text"]
                    .as_str()
                    .filter(|text| !text.is_empty()),
            );
            response["sha256"] = case["sha256"].clone();
            response["byte_length"] = case["byte_length"].clone();
            response
        };
        let status_code = if case["error"].is_null() { 200 } else { 422 };
        let server = serve(move |_| Response::json(status_code, &response.to_string()));
        let mut argv = args("set", &session, &server.base_url);
        argv.extend([
            "--file",
            file.to_str().expect("UTF-8 fixture path"),
            "--expected-revision",
            "0",
        ]);
        let output = fixture.run(&argv);
        if std::str::from_utf8(&bytes).is_err() {
            assert_error(&output, 2);
            assert!(
                server.recorded().is_empty(),
                "{name}: invalid UTF-8 was sent"
            );
        } else {
            let requests = server.recorded();
            assert_eq!(requests.len(), 1, "{name}: {}", text(&output));
            let request = &requests[0];
            assert_eq!(request.method, "PUT", "{name}");
            assert_eq!(request.path, "/coding-instructions", "{name}");
            assert_eq!(request.header("origin"), Some(server.base_url.as_str()));
            assert_cookie_only(request);
            let body: Value = serde_json::from_slice(&request.body).expect("JSON request");
            assert_eq!(body.as_object().expect("request object").len(), 2);
            assert_eq!(body["expected_revision"], 0, "{name}");
            assert_eq!(
                body["text"].as_str().expect("request text").as_bytes(),
                bytes
            );
            if case["error"].is_null() {
                assert_eq!(output.status.code(), Some(0), "{name}: {}", text(&output));
                let result = object(&output);
                let expected_text = case["normalized_text"]
                    .as_str()
                    .filter(|text| !text.is_empty())
                    .map_or(Value::Null, |text| json!(text));
                assert_eq!(result["text"], expected_text, "{name}");
                assert_eq!(result["byte_length"], case["byte_length"], "{name}");
                assert_eq!(result["sha256"], case["sha256"], "{name}");
                assert_eq!(result["application"], "unavailable", "{name}");
            } else {
                assert_error(&output, 2);
            }
        }
        assert_no_credentials(&output);
    }
    assert_eq!(fixture.tool_calls(), "");
}

#[test]
fn clear_sends_the_revision_and_origin_without_a_preliminary_read() {
    let fixture = Fixture::new();
    let session = fixture.session();
    let server = serve(|_| json_response(&status(9, None)));
    let mut argv = args("clear", &session, &server.base_url);
    argv.extend(["--expected-revision", "8"]);
    let output = fixture.run(&argv);
    assert_eq!(output.status.code(), Some(0), "{}", text(&output));
    assert_eq!(object(&output), status(9, None));
    let requests = server.recorded();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, "DELETE");
    assert_eq!(requests[0].path, "/coding-instructions?expected_revision=8");
    assert_eq!(requests[0].header("origin"), Some(server.base_url.as_str()));
    assert!(requests[0].body.is_empty());
    assert_cookie_only(&requests[0]);
    assert_no_credentials(&output);
    assert_eq!(fixture.tool_calls(), "");
}

#[test]
fn stale_revision_is_a_semantic_conflict_with_a_redacted_recovery_body() {
    let fixture = Fixture::new();
    let session = fixture.session();
    let file = fixture.file("instructions", CONTENT.as_bytes(), 0o644);
    for verb in ["set", "clear"] {
        let server = serve(|_| refusal(409, "stale_revision", Some(json!(12))));
        let mut argv = args(verb, &session, &server.base_url);
        argv.extend(["--expected-revision", "11", "--debug"]);
        if verb == "set" {
            argv.extend(["--file", file.to_str().expect("UTF-8 fixture path")]);
        }
        let output = fixture.run(&argv);
        let value = assert_error(&output, 1);
        assert!(value["error"].as_str().unwrap().contains("stale_revision"));
        assert!(value["error"].as_str().unwrap().contains("12"));
        assert!(value["fix"]
            .as_str()
            .unwrap()
            .contains("coding-instructions get"));
        assert!(value["fix"]
            .as_str()
            .unwrap()
            .contains("--expected-revision"));
        assert_eq!(server.recorded().len(), 1);
    }
}

#[test]
fn refusals_never_echo_document_text_or_unknown_server_codes() {
    let fixture = Fixture::new();
    let session = fixture.session();
    let file = fixture.file("instructions", CONTENT.as_bytes(), 0o644);
    for (http_status, code, exit_code) in [
        (401, "session_refused", 1),
        (403, "origin_refused", 1),
        (409, "revision_exhausted", 1),
        (409, "instruction-content-SENTINEL", 1),
        (413, "too_large", 2),
        (413, "instruction-content-SENTINEL", 2),
        (422, "invalid_text", 2),
        (422, "too_large", 2),
        (503, "store_unavailable", 3),
    ] {
        let server = serve(move |_| refusal(http_status, code, None));
        let mut argv = args("set", &session, &server.base_url);
        argv.extend([
            "--file",
            file.to_str().expect("UTF-8 fixture path"),
            "--expected-revision",
            "0",
            "--debug",
        ]);
        assert_error(&fixture.run(&argv), exit_code);
    }
    let server = serve(|_| Response::json(200, &json!({"text": CONTENT}).to_string()));
    assert_error(&fixture.run(&args("get", &session, &server.base_url)), 1);
}

#[test]
fn instruction_files_fail_closed_before_network_or_cluster_discovery() {
    let fixture = Fixture::new();
    let session = fixture.session();
    let missing = fixture.temp.path().join("missing");
    let directory = fixture.temp.path().join("directory");
    fs::create_dir(&directory).expect("create directory fixture");
    let invalid = fixture.file("invalid", b"instruction-content-SENTINEL\xff", 0o644);
    let server = serve(|_| json_response(&status(1, None)));
    for path in [missing, directory, invalid] {
        let mut argv = args("set", &session, &server.base_url);
        argv.extend([
            "--file",
            path.to_str().expect("UTF-8 fixture path"),
            "--expected-revision",
            "0",
        ]);
        assert_error(&fixture.run(&argv), 2);
    }
    assert!(server.recorded().is_empty());
    assert_eq!(fixture.tool_calls(), "");
}

#[test]
fn instruction_file_cap_refuses_oversize_before_io_and_accepts_the_boundary() {
    let fixture = Fixture::new();
    let session = fixture.session();
    let server = serve(|_| json_response(&status(1, None)));
    let file = fixture.file("instructions", &vec![b'x'; 131_072], 0o644);
    let mut argv = args("set", &session, &server.base_url);
    argv.extend([
        "--file",
        file.to_str().expect("UTF-8 fixture path"),
        "--expected-revision",
        "0",
        "--dry-run",
    ]);
    let accepted = fixture.run(&argv);
    assert_eq!(accepted.status.code(), Some(0), "{}", text(&accepted));
    assert_eq!(object(&accepted)["dry_run"], true);
    fs::write(&file, vec![b'x'; 131_073]).expect("grow instruction file");
    let rejected = fixture.run(&argv);
    let error = assert_error(&rejected, 2);
    assert!(error["error"]
        .as_str()
        .unwrap()
        .contains("131072-byte transport limit"));
    assert!(server.recorded().is_empty());
    assert_eq!(fixture.tool_calls(), "");
}

#[test]
fn required_revision_is_a_nonnegative_signed_bigint() {
    let fixture = Fixture::new();
    let session = fixture.session();
    let file = fixture.file("instructions", b"", 0o644);
    let server = serve(|_| json_response(&status(i64::MAX, None)));
    for verb in ["set", "clear"] {
        for revision in [None, Some("-1"), Some("9223372036854775808")] {
            let mut argv = args(verb, &session, &server.base_url);
            if verb == "set" {
                argv.extend(["--file", file.to_str().expect("UTF-8 fixture path")]);
            }
            if let Some(revision) = revision {
                argv.extend(["--expected-revision", revision]);
            }
            assert_eq!(fixture.run(&argv).status.code(), Some(2));
        }
    }
    assert!(server.recorded().is_empty());
    let mut argv = args("clear", &session, &server.base_url);
    argv.extend(["--expected-revision", "9223372036854775807"]);
    let output = fixture.run(&argv);
    assert_eq!(output.status.code(), Some(0), "{}", text(&output));
    assert_eq!(
        server.recorded()[0].path,
        "/coding-instructions?expected_revision=9223372036854775807"
    );
}

#[test]
fn dry_run_validates_files_and_plans_the_selected_release_without_io() {
    let fixture = Fixture::new();
    let session = fixture.session();
    let file = fixture.file("instructions", CONTENT.as_bytes(), 0o644);
    for verb in ["get", "set", "clear"] {
        let mut argv = vec![
            "--json",
            "cluster",
            "coding-instructions",
            verb,
            "--session-file",
            session.to_str().expect("UTF-8 fixture path"),
            "--namespace",
            "acme-ns",
            "--release",
            "acme",
            "--dry-run",
        ];
        if verb == "set" {
            argv.extend(["--file", file.to_str().expect("UTF-8 fixture path")]);
        }
        if verb != "get" {
            argv.extend(["--expected-revision", "0"]);
        }
        let output = fixture.run(&argv);
        assert_eq!(output.status.code(), Some(0), "{}", text(&output));
        let value = object(&output);
        assert_eq!(value["dry_run"], true);
        assert_eq!(value["application"], "unavailable");
        let plan = value["plan"].to_string();
        assert!(plan.contains("acme-ns"));
        assert!(plan.contains("release acme"));
        assert!(plan.contains("/coding-instructions"));
        assert!(!text(&output).contains("instruction-content-SENTINEL"));
        assert_no_credentials(&output);
    }
    assert_eq!(fixture.tool_calls(), "");
}

#[test]
fn human_get_and_set_state_that_factory_application_is_unavailable() {
    let fixture = Fixture::new();
    let session = fixture.session();
    let file = fixture.file("instructions", b"use precise names\n", 0o644);
    let server = serve(|_| json_response(&status(1, Some("use precise names\n"))));
    for verb in ["get", "set"] {
        let mut argv = args(verb, &session, &server.base_url);
        argv.remove(0);
        if verb == "set" {
            argv.extend([
                "--file",
                file.to_str().expect("UTF-8 fixture path"),
                "--expected-revision",
                "0",
            ]);
        }
        let output = fixture.run(&argv);
        assert_eq!(output.status.code(), Some(0), "{}", text(&output));
        let rendered = text(&output);
        assert!(rendered.contains("application"));
        assert!(rendered.contains("unavailable"));
        assert!(rendered.contains("use precise names"));
        assert_no_credentials(&output);
    }
}

#[test]
fn human_text_escapes_terminal_controls_while_json_preserves_the_document() {
    let fixture = Fixture::new();
    let session = fixture.session();
    let document =
        "plain\n\tcafé\u{1b}[2J\u{1b}]0;title\u{7}\u{9b}31m\u{9d}0;title\u{9c}\u{1}\u{7f}";
    let server = serve(move |_| json_response(&status(1, Some(document))));
    let mut human_args = args("get", &session, &server.base_url);
    human_args.remove(0);
    let human = fixture.run(&human_args);
    assert_eq!(human.status.code(), Some(0), "{}", text(&human));
    let rendered = text(&human);
    assert!(rendered.contains("plain\n\tcafé"));
    for escaped in [
        "\\u{1b}[2J",
        "\\u{1b}]0;title\\u{7}",
        "\\u{9b}31m",
        "\\u{9d}0;title\\u{9c}",
    ] {
        assert!(rendered.contains(escaped), "{rendered}");
    }
    assert!(rendered
        .chars()
        .all(|character| { !character.is_control() || matches!(character, '\n' | '\t') }));
    let json = fixture.run(&args("get", &session, &server.base_url));
    assert_eq!(json.status.code(), Some(0), "{}", text(&json));
    assert_eq!(object(&json)["text"], document);
}

#[test]
fn help_explains_provider_delivery_and_credential_exclusion() {
    let fixture = Fixture::new();
    for tail in [&["--help"][..], &["set", "--help"][..]] {
        let mut argv = vec!["cluster", "coding-instructions"];
        argv.extend_from_slice(tail);
        let output = fixture.run(&argv);
        assert_eq!(output.status.code(), Some(0));
        let rendered = text(&output);
        assert!(rendered.contains("model provider"));
        assert!(rendered.contains("credentials"));
        assert!(!rendered.contains("--api-key"));
    }
}

#[test]
fn omitted_api_url_never_discovers_the_platform_key() {
    let fixture = Fixture::new();
    let session = fixture.session();
    let output = fixture.run(&[
        "--json",
        "cluster",
        "coding-instructions",
        "get",
        "--session-file",
        session.to_str().expect("UTF-8 fixture path"),
    ]);
    assert!(!output.status.success());
    let calls = fixture.tool_calls();
    assert!(calls.contains("port-forward"), "{calls}");
    assert!(!calls.contains("secret"), "{calls}");
    assert_no_credentials(&output);
}

#[test]
fn remote_cleartext_urls_are_refused_before_a_request() {
    let fixture = Fixture::new();
    let session = fixture.session();
    let output = fixture.run(&args("get", &session, "http://api.example.invalid"));
    let value = assert_error(&output, 2);
    assert!(value["fix"].as_str().unwrap().contains("https://"));
    assert_eq!(fixture.tool_calls(), "");
}

#[test]
fn instruction_types_redact_text_in_debug_output() {
    let write = curie::api::CodingInstructionsWrite {
        text: CONTENT.to_string(),
        expected_revision: 4,
    };
    let document: curie::api::CodingInstructionsStatus =
        serde_json::from_value(status(5, Some(CONTENT))).expect("valid document");
    let rendered = format!("{write:?} {write:#?} {document:?} {document:#?}");
    assert!(!rendered.contains("instruction-content-SENTINEL"));
    assert!(rendered.contains("<redacted>"));
}

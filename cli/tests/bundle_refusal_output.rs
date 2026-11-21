//! Integration: what the built `curie` binary prints under `--json` when the
//! platform API refuses a bundle upload, for each case of
//! `tests/vectors/bundle-upload-refusal-cli-output.json`.
//!
//! The API side of the vector (`api_body_text`, the exact 422 body of the
//! upload route) is pinned by the API's own test; this test serves exactly that
//! body from a wire-level stub and pins the binary's real stdout and exit code
//! to the vector's `cli_stdout` and `cli_exit_code`. The live chain judge reads
//! a deploy refusal out of that recorded output, so a change on either side of
//! this handoff fails one of the two tests.

use crate::support;

use serde_json::{json, Value};
use std::path::PathBuf;
use support::{serve, MockServer, Response};

const AGENT_ID: &str = "11111111-1111-1111-1111-111111111111";
const VERSION_ID: &str = "22222222-2222-2222-2222-222222222222";
const TEST_API_KEY: &str = "test-key";
const UNEXPECTED: &str = r#"{"detail":"request not modeled by the bundle refusal stub"}"#;

fn vector() -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../tests/vectors/bundle-upload-refusal-cli-output.json");
    serde_json::from_str(&std::fs::read_to_string(&path).expect("read the refusal vector"))
        .expect("the refusal vector is JSON")
}

/// An existing agent whose upload route answers `status` with `body` verbatim.
fn upload_refusing_stub(status: u16, body: String) -> MockServer {
    let agent = json!({
        "id": AGENT_ID,
        "name": "deal-desk",
        "channels": [{"kind": "slack", "address": "C0EXAMPLE0"}],
        "approval_required_tools": null,
        "approval_routes": null,
        "memory": false,
    })
    .to_string();
    let listed = format!("[{agent}]");
    serve(move |req| {
        let path = req.path.split('?').next().unwrap_or_default();
        let agent_path = format!("/agents/{AGENT_ID}");
        let versions = format!("{agent_path}/versions");
        let bundle = format!("{versions}/{VERSION_ID}/bundle");
        match (req.method.as_str(), path) {
            ("GET", "/agents") => Response::json(200, &listed),
            ("PATCH", p) if p == agent_path => Response::json(200, &agent),
            ("POST", p) if p == versions => Response::json(
                201,
                &format!(
                    r#"{{"id":"{VERSION_ID}","agent_id":"{AGENT_ID}","version_label":"0.1.0-1","bundle_ref":null,"bundle_sha256":null,"created_by":"tester","created_at":"2026-09-14T00:00:00Z"}}"#
                ),
            ),
            ("PUT", p) if p == bundle => Response::json(status, &body),
            ("GET", "/deployments") => Response::json(200, "[]"),
            ("GET", p) if p.starts_with(&versions) && p.ends_with("/files") => {
                Response::json(200, r#"{"files":[]}"#)
            }
            _ => Response::json(500, UNEXPECTED),
        }
    })
}

#[test]
fn the_binary_prints_the_recorded_refusal_for_each_recorded_api_body() {
    let vector = vector();
    let cases = vector["cases"].as_object().expect("cases is an object");
    assert!(cases.contains_key("retired_declaration") && cases.len() >= 2);
    for (name, case) in cases {
        let dir = tempfile::tempdir().expect("tempdir");
        curie::scaffold::scaffold(dir.path(), "deal-desk").expect("scaffold the bundle");
        for (rel, content) in case["bundle_files"].as_object().expect("bundle_files") {
            let path = dir.path().join(rel);
            std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
            std::fs::write(&path, content.as_str().expect("file text")).expect("write");
        }
        let status = u16::try_from(case["api_status"].as_u64().expect("api_status"))
            .expect("api_status fits u16");
        let body = case["api_body_text"]
            .as_str()
            .expect("api_body_text")
            .to_string();
        let server = upload_refusing_stub(status, body);

        let out = std::process::Command::new(env!("CARGO_BIN_EXE_curie"))
            .args(["local", "deploy", "--json", "--plugin-dir"])
            .arg(dir.path())
            .args(["--api-url", &server.base_url, "--api-key", TEST_API_KEY])
            .env_remove("CURIE_API_URL")
            .env_remove("CURIE_API_KEY")
            .env("NO_COLOR", "1")
            .output()
            .expect("run curie local deploy");

        let stdout = String::from_utf8_lossy(&out.stdout);
        let printed: Value = serde_json::from_str(stdout.trim()).unwrap_or_else(|err| {
            panic!("{name}: stdout must be one JSON object ({err}): {stdout}")
        });
        let code = i64::from(out.status.code().unwrap_or(-1));
        assert!(
            printed == case["cli_stdout"] && Some(code) == case["cli_exit_code"].as_i64(),
            "{name}: the binary printed {printed} and exited {code}; the vector records {} and {}",
            case["cli_stdout"],
            case["cli_exit_code"]
        );
    }
}

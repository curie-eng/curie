use crate::support::{serve, Response};
use serde_json::{json, Value};
use std::process::Command;

const TENANT: &str = "00000000-0000-0000-0000-000000000001";
const AGENT: &str = "00000000-0000-0000-0000-000000000002";

#[test]
fn compiled_operator_verbs_send_exact_method_scope_and_cas_body() {
    for (verb, action, method, suffix) in [
        (
            "identity-authority",
            "adopt",
            "PUT",
            format!("/identity/authorities/{TENANT}"),
        ),
        (
            "identity-authority",
            "replace",
            "POST",
            format!("/identity/authorities/{TENANT}/replace"),
        ),
        (
            "identity-authority",
            "retire",
            "POST",
            format!("/identity/authorities/{TENANT}/retire"),
        ),
        (
            "role-filter",
            "set",
            "PUT",
            format!("/identity/roles/{TENANT}/{AGENT}/invocation"),
        ),
        (
            "role-filter",
            "remove",
            "DELETE",
            format!("/identity/roles/{TENANT}/{AGENT}/invocation"),
        ),
    ] {
        let server =
            serve(|_| Response::json(200, &json!({"tenant_id":TENANT,"version":8}).to_string()));
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("request.json");
        let body =
            json!({"request_id":"00000000-0000-4000-8000-000000000003","expected_version":7});
        std::fs::write(&file, body.to_string()).unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_curie"));
        command
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap())
            .current_dir(directory.path())
            .args(["cluster", verb, action, "--tenant", TENANT, "--apply"])
            .arg(&file)
            .args([
                "--api-url",
                &server.base_url,
                "--api-key",
                "synthetic-platform-key",
                "--json",
            ]);
        if verb == "role-filter" {
            command.args(["--agent", AGENT, "--role", "invocation"]);
        }
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let result: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["result"]["version"], 8);
        let requests = server.recorded();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].header("X-API-Key"),
            Some("synthetic-platform-key")
        );
        assert_eq!(requests[0].method, method);
        assert_eq!(requests[0].path, suffix);
        assert_eq!(
            serde_json::from_slice::<Value>(&requests[0].body).unwrap(),
            body
        );
        assert!(!String::from_utf8_lossy(&output.stdout).contains("synthetic-platform-key"));
    }
}

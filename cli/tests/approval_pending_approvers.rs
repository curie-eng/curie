//! Pending approvals report the current route approvers when an operator lists
//! them. The approval row is durable, but the agent's route binding can change
//! while that row waits for a decision (#2940).
//!
//! Drive the compiled CLI through its API client and output renderer. A fixed
//! pending response and a mutable agent response prove that each list command
//! reads the current binding rather than treating the pending row as a snapshot.

mod support;

use std::process::{Command, Output};
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};
use support::{serve, MockServer, Response};

const AGENT_ID: &str = "11111111-1111-1111-1111-111111111111";
const FINANCE_ID: &str = "22222222-2222-2222-2222-222222222222";
const UNBOUND_ID: &str = "33333333-3333-3333-3333-333333333333";
const TEST_API_KEY: &str = "test-key";

fn pending_records() -> String {
    format!(
        r#"[
          {{"id":"{FINANCE_ID}","author":"U0REQUEST","route":"finance","gate_kind":"policy","granted_tool":"Bash","status":"pending","conversation_id":"thread-1","summary":"run report","expires_at":null,"resolved_by":null,"card_channel":"C0EXAMPLE1"}},
          {{"id":"{UNBOUND_ID}","author":"U0REQUEST","route":"removed","gate_kind":"policy","granted_tool":"Bash","status":"pending","conversation_id":"thread-2","summary":"check status","expires_at":null,"resolved_by":null,"card_channel":"C0EXAMPLE2"}}
        ]"#
    )
}

fn agent_response(routes: &Value) -> String {
    format!(
        r#"[{{"id":"{AGENT_ID}","name":"acme-bot","channels":[{{"kind":"slack","address":"C0EXAMPLE0"}}],"approval_required_tools":[],"approval_routes":{routes},"memory":false}}]"#
    )
}

fn route_with_users(users: &[&str]) -> Value {
    json!({
        "finance": {
            "resolution": {"kind": "slack", "address": "C0EXAMPLE1"},
            "approvers": {"users": users}
        }
    })
}

fn server_with_routes(initial: Value) -> (MockServer, Arc<Mutex<Value>>) {
    let routes = Arc::new(Mutex::new(initial));
    let server_routes = Arc::clone(&routes);
    let server = serve(move |req| match req.path.split('?').next().unwrap() {
        "/agents" => {
            let current = server_routes.lock().unwrap();
            Response::json(200, &agent_response(&current))
        }
        "/approvals" => {
            assert!(req.path.contains("status_filter=pending"));
            assert!(req.path.contains(&format!("agent_id={AGENT_ID}")));
            Response::json(200, &pending_records())
        }
        other => panic!("unexpected request: {other}"),
    });
    (server, routes)
}

fn list(server: &MockServer, json_output: bool) -> Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_curie"));
    cmd.args([
        "local",
        "approvals",
        "acme-bot",
        "--list",
        "--api-url",
        &server.base_url,
        "--api-key",
        TEST_API_KEY,
    ]);
    if json_output {
        cmd.arg("--json");
    }
    let output = cmd
        .env_remove("CURIE_API_URL")
        .env_remove("CURIE_API_KEY")
        .env("NO_COLOR", "1")
        .output()
        .expect("run curie approvals");
    assert!(
        output.status.success(),
        "list failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

fn json_list(server: &MockServer) -> Value {
    let output = list(server, true);
    serde_json::from_slice(&output.stdout).expect("one valid JSON approval list")
}

#[test]
fn json_list_uses_current_route_users_and_leaves_an_unbound_route_unknown() {
    let (server, routes) = server_with_routes(route_with_users(&["U0EXAMPLE1"]));

    let first = json_list(&server);
    assert_eq!(first["count"], 2);
    assert_eq!(first["pending"][0]["id"], FINANCE_ID);
    assert_eq!(
        first["pending"][0]["current_route_approvers"],
        json!({"users": ["U0EXAMPLE1"]})
    );
    assert_eq!(first["pending"][1]["id"], UNBOUND_ID);
    assert!(
        first["pending"][1]["current_route_approvers"].is_null(),
        "a removed route must not claim a known approver set"
    );

    *routes.lock().unwrap() = route_with_users(&["U0EXAMPLE2", "U0EXAMPLE3"]);
    let second = json_list(&server);
    assert_eq!(second["pending"][0]["id"], FINANCE_ID);
    assert_eq!(
        second["pending"][0]["current_route_approvers"],
        json!({"users": ["U0EXAMPLE2", "U0EXAMPLE3"]}),
        "the same pending row must reflect the route binding at read time"
    );
    assert!(second["pending"][1]["current_route_approvers"].is_null());

    let agent_reads = server
        .recorded()
        .iter()
        .filter(|req| req.path == "/agents")
        .count();
    assert_eq!(
        agent_reads, 2,
        "each list must fetch the current agent binding"
    );
}

#[test]
fn human_list_names_current_users_after_a_route_change() {
    let (server, routes) = server_with_routes(route_with_users(&["U0EXAMPLE1"]));

    let first = String::from_utf8(list(&server, false).stdout).expect("UTF-8 output");
    assert!(first.contains(FINANCE_ID));
    assert!(first.contains("U0EXAMPLE1"), "first route users: {first}");

    *routes.lock().unwrap() = route_with_users(&["U0EXAMPLE2"]);
    let second = String::from_utf8(list(&server, false).stdout).expect("UTF-8 output");
    assert!(second.contains(FINANCE_ID));
    assert!(
        second.contains("U0EXAMPLE2"),
        "current route users: {second}"
    );
    assert!(
        !second.contains("U0EXAMPLE1"),
        "human output must not present the earlier route users as current: {second}"
    );
}

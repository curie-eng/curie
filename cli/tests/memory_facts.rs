//! Operator fact flags validate and describe every intended operation offline.
//! Storage behavior is exercised by the real local Python test at both tiers.

use std::process::{Command, Output};

use crate::support;
use support::{serve, Response};

const FACT: &str = "fact-0123456789abcdef0123456789abcdef";
const CHANNEL: &str = "webhook=inbox=operator@example.com?view=all#retained%25";

fn run(tier: &str, flags: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_curie"))
        .args([
            tier,
            "memory",
            "acme-bot",
            "--api-url",
            "http://127.0.0.1:9",
            "--api-key",
            "example-key",
        ])
        .args(flags)
        .env_remove("CURIE_API_URL")
        .env_remove("CURIE_API_KEY")
        .output()
        .expect("run source CLI")
}

fn dry_plan(tier: &str, flags: &[&str]) -> Vec<String> {
    let mut argv = flags.to_vec();
    argv.extend(["--dry-run", "--json"]);
    let output = run(tier, &argv);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "stdout: {stdout}; stderr: {stderr}"
    );
    let value: serde_json::Value = serde_json::from_str(&stdout).expect("one JSON result");
    assert_eq!(value["dry_run"], true);
    value["plan"]
        .as_array()
        .expect("dry run plan array")
        .iter()
        .map(|line| line.as_str().expect("plan line").to_string())
        .collect()
}

fn refused(tier: &str, flags: &[&str], evidence: &str) {
    let output = run(tier, flags);
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!output.status.success(), "unexpected success: {text}");
    assert!(text.contains(evidence), "expected {evidence:?}: {text}");
    assert!(
        !text.contains("Connection refused"),
        "validation must precede HTTP: {text}"
    );
}

#[test]
fn plain_fact_listing_plans_legacy_agent_and_channel_reads_at_both_tiers() {
    for tier in ["local", "cluster"] {
        let plan = dry_plan(tier, &[]);
        assert!(
            plan.iter()
                .any(|line| line.starts_with("GET ") && line.contains("/agents/<id>/memory")),
            "legacy log must remain in the plan: {plan:?}"
        );
        assert!(
            plan.iter()
                .any(|line| line.starts_with("GET ") && line.contains("/state/memory")),
            "agent facts must be read: {plan:?}"
        );
        assert!(
            plan.iter()
                .any(|line| line.starts_with("GET ") && line.contains("/state/bindings/")),
            "bound channel facts must be read: {plan:?}"
        );
        assert!(plan.iter().all(|line| !line.starts_with("DELETE ")));
    }
}

#[test]
fn selected_channel_listing_preserves_equals_and_encodes_transport_characters() {
    for tier in ["local", "cluster"] {
        let plan = dry_plan(tier, &["--channel", CHANNEL]);
        let reads: Vec<_> = plan
            .iter()
            .filter(|line| line.starts_with("GET "))
            .collect();
        assert!(!reads.is_empty(), "channel listing must read: {plan:?}");
        let selected = reads
            .iter()
            .find(|line| line.contains("/state/bindings/webhook/"))
            .unwrap_or_else(|| panic!("channel facts must be read: {plan:?}"));
        assert!(
            selected.contains("inbox") && selected.contains("operator"),
            "{selected}"
        );
        assert!(
            selected.contains("%3Fview") && selected.contains("%23retained"),
            "{selected}"
        );
        assert!(
            selected.contains("%2525"),
            "literal percent must be encoded: {selected}"
        );
        assert!(
            !selected.contains("?view") && !selected.contains("#retained"),
            "{selected}"
        );
        assert!(!reads
            .iter()
            .any(|line| line.contains("/agents/<id>/memory ")));
        assert!(!reads.iter().any(|line| line.contains("/state/memory")));
    }
}

#[test]
fn deletion_plans_the_selected_fact_read_and_versioned_delete_at_both_tiers() {
    for tier in ["local", "cluster"] {
        for channel in [None, Some("slack=C0EXAMPLE1")] {
            let mut flags = vec!["--delete", FACT];
            if let Some(channel) = channel {
                flags.extend(["--channel", channel]);
            }
            let plan = dry_plan(tier, &flags);
            let read = plan
                .iter()
                .position(|line| line.starts_with("GET ") && line.contains(FACT))
                .unwrap_or_else(|| panic!("read current row before deleting: {plan:?}"));
            let delete = plan
                .iter()
                .position(|line| line.starts_with("DELETE ") && line.contains(FACT))
                .unwrap_or_else(|| panic!("delete selected row: {plan:?}"));
            assert!(read < delete, "the read must precede the delete: {plan:?}");
            assert!(
                plan[delete].contains("expected_version"),
                "CAS must be explicit: {plan:?}"
            );
            if channel.is_some() {
                assert!(plan[read].contains("/state/bindings/slack/C0EXAMPLE1/memory/"));
                assert!(plan[delete].contains("/state/bindings/slack/C0EXAMPLE1/memory/"));
            } else {
                assert!(plan[read].contains("/state/memory/"));
                assert!(plan[delete].contains("/state/memory/"));
                assert!(!plan[delete].contains("/bindings/"));
            }
        }
    }
}

#[test]
fn canonical_fact_ids_require_an_exact_full_match_before_any_http() {
    for tier in ["local", "cluster"] {
        dry_plan(tier, &["--delete", FACT]);
        for invalid in [
            "log",
            "guidance",
            "fact-short",
            "fact-0123456789ABCDEF0123456789abcdef",
            "fact-0123456789abcdef0123456789abcdef0",
            "../guidance",
            "fact-0123456789abcdef0123456789abcdef/log",
            "fact-0123456789abcdef0123456789abcdef\n",
        ] {
            refused(tier, &["--delete", invalid, "--dry-run", "--json"], "fact");
        }
    }
}

#[test]
fn malformed_channel_pairs_are_refused_before_any_http() {
    for tier in ["local", "cluster"] {
        dry_plan(tier, &["--channel", "slack=C0EXAMPLE1"]);
        for invalid in ["slack", "=C0EXAMPLE1", "slack="] {
            refused(
                tier,
                &["--channel", invalid, "--dry-run", "--json"],
                "channel",
            );
            refused(
                tier,
                &[
                    "--delete",
                    FACT,
                    "--channel",
                    invalid,
                    "--dry-run",
                    "--json",
                ],
                "channel",
            );
        }
    }
}

#[test]
fn delete_and_channel_selection_conflict_with_add_and_every_guidance_flag() {
    for tier in ["local", "cluster"] {
        for action in [
            vec!["--add", "operator note"],
            vec!["--guidance"],
            vec!["--guidance-from", "missing-guidance.md"],
            vec!["--reset-guidance"],
        ] {
            for selection in [
                vec!["--delete", FACT],
                vec!["--channel", "slack=C0EXAMPLE1"],
            ] {
                for (first, second) in [(&action, &selection), (&selection, &action)] {
                    let mut flags = first.clone();
                    flags.extend(second.iter().copied());
                    flags.extend(["--dry-run", "--json"]);
                    refused(tier, &flags, "cannot be used");
                }
            }
        }
    }
}

#[test]
fn operators_cannot_supply_an_expected_version_flag() {
    for tier in ["local", "cluster"] {
        dry_plan(tier, &["--delete", FACT]);
        refused(
            tier,
            &["--delete", FACT, "--expected-version", "1", "--dry-run"],
            "unexpected argument",
        );
        // The lifecycle flags read the version too; none takes one from argv.
        for flags in [
            vec!["--archive", FACT],
            vec!["--restore", FACT],
            vec!["--revert", FACT, "--to-version", "1"],
        ] {
            let mut argv = flags.clone();
            argv.extend(["--dry-run", "--json"]);
            dry_plan(tier, &flags);
            argv.extend(["--expected-version", "1"]);
            refused(tier, &argv, "unexpected argument");
        }
    }
}

// --- Typed memory metadata and the reversible lifecycle (ADR 0239) ----------
//
// The binary runs against a wire-level stub of the platform API that holds the
// canonical heads and their revision history the way Contract r4 defines them,
// so the clap layer, the handler, the HTTP client and the renderer are all on
// the path at both tiers. The stub enforces expected_version itself: a request
// with the wrong version gets 409 and changes nothing.

const AGENT_ID: &str = "33333333-3333-3333-3333-333333333333";
const INSTRUCTION: &str = "fact-fedcba9876543210fedcba9876543210";
const SLACK_ID: &str = "C0EXAMPLE1";
const CHANNEL_FLAG: &str = "slack=C0EXAMPLE1";
const CONVERSATION: &str = "slack:C0EXAMPLE1:1700000000.000100";

#[derive(Clone)]
struct Head {
    value: serde_json::Value,
    version: u64,
    deleted: bool,
    history: Vec<serde_json::Value>,
}

#[derive(Default)]
struct StubState {
    heads: std::collections::BTreeMap<(String, String), Head>,
    /// When set, every typed mutation answers 409 whatever version it carries:
    /// a concurrent writer moved the head between the CLI read and its write.
    always_conflict: bool,
}

fn canonical(kind: &str, title: &str, statement: &str, archived: bool) -> serde_json::Value {
    serde_json::json!({
        "kind": kind,
        "title": title,
        "statement": statement,
        "author": "U0EXAMPLE1",
        "stated_at": "2026-10-01T03:04:05.123456+00:00",
        "session_id": "session-example-1",
        "conversation": CONVERSATION,
        "archived": archived,
    })
}

fn revision(
    version: u64,
    operation: &str,
    snapshot: Option<&serde_json::Value>,
) -> serde_json::Value {
    let sandbox = operation == "create" || operation == "update";
    serde_json::json!({
        "version": version,
        "operation": operation,
        "actor": if sandbox { "U0EXAMPLE1" } else { "operator" },
        "source": if sandbox { "sandbox" } else { "operator" },
        "conversation": if sandbox { serde_json::json!(CONVERSATION) } else { serde_json::Value::Null },
        "created_at": format!("2026-10-0{}T03:04:05Z", version.min(9)),
        "reverted_to_version": serde_json::Value::Null,
        "snapshot": snapshot.cloned().unwrap_or(serde_json::Value::Null),
    })
}

fn seeded_state() -> StubState {
    let mut state = StubState::default();
    for scope in ["agent".to_string(), format!("channel:slack:{SLACK_ID}")] {
        let first = canonical(
            "fact",
            "Reply length",
            "agent prefers concise replies",
            false,
        );
        let second = canonical(
            "fact",
            "Reply length",
            "agent prefers two sentence replies",
            false,
        );
        state.heads.insert(
            (scope.clone(), FACT.to_string()),
            Head {
                value: second.clone(),
                version: 2,
                deleted: false,
                history: vec![
                    revision(2, "update", Some(&second)),
                    revision(1, "create", Some(&first)),
                ],
            },
        );
        let statement = "sign every reply as Example Signer";
        let active = canonical("instruction", "Sign replies", statement, false);
        let archived = canonical("instruction", "Sign replies", statement, true);
        state.heads.insert(
            (scope, INSTRUCTION.to_string()),
            Head {
                value: archived.clone(),
                version: 2,
                deleted: false,
                history: vec![
                    revision(2, "archive", Some(&archived)),
                    revision(1, "create", Some(&active)),
                ],
            },
        );
    }
    state
}

fn stub_agent() -> serde_json::Value {
    serde_json::json!({
        "id": AGENT_ID,
        "name": "acme-bot",
        "channels": [{"kind": "slack", "address": SLACK_ID}],
        "model": null,
        "thinking": null,
        "execution_deadline_seconds": null,
        "runner_resources": null,
        "created_at": "2026-10-01T00:00:00Z",
        "memory": false,
        "memory_writes": true,
        "agent_wide_learning": true,
    })
}

fn percent_decode(segment: &str) -> String {
    let bytes = segment.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Some(byte) = std::str::from_utf8(&bytes[i + 1..i + 3])
                .ok()
                .and_then(|hex| u8::from_str_radix(hex, 16).ok())
            {
                out.push(byte);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The decoded path and query parameters of a recorded request.
fn split_query(path: &str) -> (String, std::collections::BTreeMap<String, String>) {
    let url = reqwest::Url::parse(&format!("http://stub{path}")).expect("request path parses");
    let query = url
        .query_pairs()
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    let segments: Vec<String> = url
        .path_segments()
        .map(|segments| segments.map(percent_decode).collect())
        .unwrap_or_default();
    (format!("/{}", segments.join("/")), query)
}

fn scope_key(
    scope: Option<&str>,
    kind: Option<&str>,
    address: Option<&str>,
) -> Result<String, Response> {
    match (scope, kind, address) {
        (Some("agent"), None, None) => Ok("agent".to_string()),
        (Some("channel"), Some(kind), Some(address)) => Ok(format!("channel:{kind}:{address}")),
        _ => Err(Response::json(
            422,
            r#"{"detail":"scope must be agent, or channel with channel_kind and channel_address"}"#,
        )),
    }
}

fn record_out(id: &str, scope: &str, head: &Head) -> serde_json::Value {
    let mut out = head.value.clone();
    let map = out.as_object_mut().unwrap();
    map.insert("id".into(), serde_json::json!(id));
    map.insert(
        "scope".into(),
        serde_json::json!(if scope == "agent" { "agent" } else { "channel" }),
    );
    map.insert("version".into(), serde_json::json!(head.version));
    map.insert(
        "updated_at".into(),
        serde_json::json!("2026-10-05T03:04:05Z"),
    );
    map.remove("session_id");
    out
}

fn detail_out(id: &str, scope: &str, head: &Head) -> serde_json::Value {
    serde_json::json!({
        "entry": if head.deleted { serde_json::Value::Null } else { record_out(id, scope, head) },
        "deleted": head.deleted,
        "version": head.version,
        "history": head.history,
        "history_before": serde_json::Value::Null,
    })
}

fn state_row(id: &str, head: &Head) -> serde_json::Value {
    serde_json::json!({
        "namespace": "memory",
        "key": id,
        "value": head.value,
        "version": head.version,
        "updated_at": "2026-10-05T03:04:05Z",
    })
}

fn not_found() -> Response {
    Response::json(404, r#"{"detail":"not found"}"#)
}

/// Raw canonical state paths: `(scope key, optional fact key)`.
fn raw_memory_path(path: &str) -> Option<(String, Option<String>)> {
    let base = format!("/agents/{AGENT_ID}/state");
    let rest = path.strip_prefix(&base)?;
    if let Some(rest) = rest.strip_prefix("/bindings/") {
        let parts: Vec<&str> = rest.splitn(4, '/').collect();
        if parts.len() >= 3 && parts[2] == "memory" {
            return Some((
                format!("channel:{}:{}", parts[0], parts[1]),
                parts.get(3).map(|s| s.to_string()),
            ));
        }
        return None;
    }
    if rest == "/memory" {
        return Some(("agent".to_string(), None));
    }
    rest.strip_prefix("/memory/")
        .map(|key| ("agent".to_string(), Some(key.to_string())))
}

fn handle(state: &std::sync::Mutex<StubState>, req: &support::Request) -> Response {
    let mut state = state.lock().unwrap();
    let (path, query) = split_query(&req.path);
    if req.method == "GET" && path == "/agents" {
        return Response::json(200, &serde_json::json!([stub_agent()]).to_string());
    }
    if req.method == "GET" && path == format!("/agents/{AGENT_ID}/memory") {
        return Response::json(200, "[]");
    }
    if let Some((scope, key)) = raw_memory_path(&path) {
        if req.method != "GET" {
            return Response::json(
                405,
                r#"{"detail":"these flags must use the typed lifecycle routes"}"#,
            );
        }
        return match key {
            None => {
                let rows: Vec<_> = state
                    .heads
                    .iter()
                    .filter(|((s, _), head)| *s == scope && !head.deleted)
                    .map(|((_, id), head)| state_row(id, head))
                    .collect();
                Response::json(200, &serde_json::Value::Array(rows).to_string())
            }
            Some(id) => match state.heads.get(&(scope, id.clone())) {
                Some(head) if !head.deleted => {
                    Response::json(200, &state_row(&id, head).to_string())
                }
                _ => not_found(),
            },
        };
    }
    let Some(typed) = path.strip_prefix(&format!("/agents/{AGENT_ID}/state/_memory/")) else {
        return not_found();
    };
    if req.method == "GET" {
        let Some(id) = typed.strip_prefix("entries/") else {
            return not_found();
        };
        let scope = match scope_key(
            query.get("scope").map(String::as_str),
            query.get("channel_kind").map(String::as_str),
            query.get("channel_address").map(String::as_str),
        ) {
            Ok(scope) => scope,
            Err(refusal) => return refusal,
        };
        return match state.heads.get(&(scope.clone(), id.to_string())) {
            Some(head) => Response::json(200, &detail_out(id, &scope, head).to_string()),
            None => not_found(),
        };
    }
    if req.method != "POST" || !["archive", "restore", "revert"].contains(&typed) {
        return not_found();
    }
    let body: serde_json::Value = match serde_json::from_slice(&req.body) {
        Ok(body) => body,
        Err(_) => return Response::json(422, r#"{"detail":"body must be JSON"}"#),
    };
    let scope = match scope_key(
        body["scope"].as_str(),
        body.get("channel_kind").and_then(serde_json::Value::as_str),
        body.get("channel_address")
            .and_then(serde_json::Value::as_str),
    ) {
        Ok(scope) => scope,
        Err(refusal) => return refusal,
    };
    let Some(id) = body["id"].as_str().map(str::to_string) else {
        return Response::json(422, r#"{"detail":"id is required"}"#);
    };
    let Some(expected) = body["expected_version"].as_u64() else {
        return Response::json(422, r#"{"detail":"expected_version is required"}"#);
    };
    let always_conflict = state.always_conflict;
    let Some(head) = state.heads.get_mut(&(scope.clone(), id.clone())) else {
        return not_found();
    };
    if always_conflict || expected != head.version {
        return Response::json(409, r#"{"detail":"expected_version mismatch"}"#);
    }
    if typed == "revert" {
        let Some(to) = body["to_version"].as_u64() else {
            return Response::json(422, r#"{"detail":"to_version is required"}"#);
        };
        let target = if to == 0 {
            Some(serde_json::Value::Null)
        } else {
            head.history
                .iter()
                .find(|r| r["version"].as_u64() == Some(to))
                .map(|r| r["snapshot"].clone())
        };
        let Some(target) = target.filter(|_| to < head.version) else {
            return Response::json(422, r#"{"detail":"to_version is not an earlier revision"}"#);
        };
        head.version += 1;
        if target.is_null() {
            head.deleted = true;
        } else {
            head.value = target;
            head.deleted = false;
        }
        let snapshot = (!head.deleted).then(|| head.value.clone());
        let mut rev = revision(head.version, "revert", snapshot.as_ref());
        rev["reverted_to_version"] = serde_json::json!(to);
        head.history.insert(0, rev);
        return Response::json(200, &detail_out(&id, &scope, head).to_string());
    }
    if head.deleted {
        return not_found();
    }
    let want = typed == "archive";
    if head.value["archived"].as_bool() != Some(want) {
        head.value["archived"] = serde_json::json!(want);
        head.version += 1;
        let snapshot = head.value.clone();
        head.history
            .insert(0, revision(head.version, typed, Some(&snapshot)));
    }
    Response::json(200, &record_out(&id, &scope, head).to_string())
}

struct Stub {
    server: support::MockServer,
    state: std::sync::Arc<std::sync::Mutex<StubState>>,
}

fn stub(state: StubState) -> Stub {
    let state = std::sync::Arc::new(std::sync::Mutex::new(state));
    let shared = std::sync::Arc::clone(&state);
    let server = serve(move |req| handle(&shared, req));
    Stub { server, state }
}

fn run_stub(tier: &str, stub: &Stub, flags: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_curie"))
        .args([
            tier,
            "memory",
            "acme-bot",
            "--api-url",
            &stub.server.base_url,
            "--api-key",
            "example-key",
        ])
        .args(flags)
        .env_remove("CURIE_API_URL")
        .env_remove("CURIE_API_KEY")
        .env("NO_PROXY", "127.0.0.1,localhost")
        .env("no_proxy", "127.0.0.1,localhost")
        .env("NO_COLOR", "1")
        .output()
        .expect("run source CLI")
}

fn memory_validator() -> jsonschema::Validator {
    let path = format!("{}/schema/memory.schema.json", env!("CARGO_MANIFEST_DIR"));
    let schema: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(path).expect("memory schema"))
            .expect("memory schema JSON");
    jsonschema::validator_for(&schema).expect("memory schema compiles")
}

fn ok_json(tier: &str, stub: &Stub, flags: &[&str]) -> serde_json::Value {
    let mut argv = flags.to_vec();
    argv.push("--json");
    let output = run_stub(tier, stub, &argv);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "{tier} {flags:?}: stdout: {stdout}; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: serde_json::Value = serde_json::from_str(stdout.trim())
        .unwrap_or_else(|e| panic!("{tier} {flags:?}: one JSON object: {e}; {stdout}"));
    assert!(
        memory_validator().is_valid(&value),
        "{tier} {flags:?}: output must validate against memory.schema.json: {value}"
    );
    value
}

fn mutations(stub: &Stub) -> Vec<support::Request> {
    stub.server
        .recorded()
        .into_iter()
        .filter(|r| r.method != "GET")
        .collect()
}

fn body_of(request: &support::Request) -> serde_json::Value {
    serde_json::from_slice(&request.body).expect("mutation body is JSON")
}

fn assert_scope(body: &serde_json::Value, channel: bool) {
    if channel {
        assert_eq!(body["scope"], "channel", "{body}");
        assert_eq!(body["channel_kind"], "slack", "{body}");
        assert_eq!(body["channel_address"], SLACK_ID, "{body}");
    } else {
        assert_eq!(body["scope"], "agent", "{body}");
        for key in ["channel_kind", "channel_address"] {
            assert!(
                body.get(key).is_none_or(serde_json::Value::is_null),
                "agent scope must not name a channel: {body}"
            );
        }
    }
}

fn assert_unchanged(stub: &Stub) {
    let before = seeded_state();
    let state = stub.state.lock().unwrap();
    for (key, head) in &before.heads {
        assert_eq!(state.heads[key].version, head.version, "{key:?} changed");
        assert_eq!(state.heads[key].value, head.value, "{key:?} changed");
        assert_eq!(state.heads[key].history.len(), head.history.len());
    }
}

fn scoped(flags: &[&'static str], channel: bool) -> Vec<&'static str> {
    let mut flags = flags.to_vec();
    if channel {
        flags.extend(["--channel", CHANNEL_FLAG]);
    }
    flags
}

#[test]
fn memory_listing_json_carries_kind_title_archived_conversation_and_version_at_both_tiers() {
    for tier in ["local", "cluster"] {
        let stub = stub(seeded_state());
        let listed = ok_json(tier, &stub, &[]);
        let facts = listed["facts"].as_array().expect("facts array");
        assert_eq!(facts.len(), 4, "{listed}");
        for fact in facts {
            for key in [
                "id",
                "scope",
                "channel",
                "kind",
                "title",
                "statement",
                "author",
                "stated_at",
                "conversation",
                "archived",
                "version",
            ] {
                assert!(fact.get(key).is_some(), "{tier}: fact lacks {key}: {fact}");
            }
            assert!(fact["archived"].is_boolean(), "{fact}");
            assert!(fact["version"].is_u64(), "{fact}");
            assert_eq!(fact["conversation"], CONVERSATION, "{fact}");
        }
        let fact = facts
            .iter()
            .find(|f| f["id"] == FACT && f["scope"] == "agent")
            .expect("agent fact listed");
        assert_eq!(fact["kind"], "fact");
        assert_eq!(fact["title"], "Reply length");
        assert_eq!(fact["archived"], false);
        assert_eq!(fact["version"], 2);
        let instruction = facts
            .iter()
            .find(|f| f["id"] == INSTRUCTION && f["scope"] == "channel")
            .expect("channel instruction listed");
        assert_eq!(instruction["kind"], "instruction");
        assert_eq!(instruction["archived"], true);
        assert_eq!(instruction["version"], 2);
        assert_eq!(
            instruction["channel"],
            serde_json::json!({"kind": "slack", "address": SLACK_ID})
        );
        assert!(mutations(&stub).is_empty(), "listing must not write");
    }
}

/// A stub holding exactly one agent-wide head with this stored value.
fn single_head(id: &str, value: serde_json::Value) -> StubState {
    let mut state = StubState::default();
    state.heads.insert(
        ("agent".to_string(), id.to_string()),
        Head {
            value,
            version: 1,
            deleted: false,
            history: vec![],
        },
    );
    state
}

#[test]
fn a_fact_without_stored_provenance_lists_as_null_and_author_unknown_at_both_tiers() {
    // A migrated historical fact: typed fields present, no author or stated_at.
    // The migration (Contract r4) writes conversation as an explicit null.
    let unattributed = serde_json::json!({
        "kind": "fact",
        "title": "Release day",
        "statement": "the release train leaves on Tuesdays",
        "conversation": null,
        "archived": false,
    });
    for tier in ["local", "cluster"] {
        let stub = stub(single_head(FACT, unattributed.clone()));
        let listed = ok_json(tier, &stub, &[]);
        let facts = listed["facts"].as_array().expect("facts array");
        assert_eq!(facts.len(), 1, "{tier}: {listed}");
        let fact = &facts[0];
        assert_eq!(fact["id"], FACT, "{fact}");
        assert_eq!(fact["kind"], "fact", "{fact}");
        assert_eq!(fact["title"], "Release day", "{fact}");
        for key in ["author", "stated_at", "conversation"] {
            assert!(
                fact.get(key).is_some_and(serde_json::Value::is_null),
                "{tier}: {key} must be present as JSON null: {fact}"
            );
        }

        let output = run_stub(tier, &stub, &[]);
        let text = String::from_utf8_lossy(&output.stdout).to_string();
        assert!(
            output.status.success(),
            "{tier}: {text}{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let line = text
            .lines()
            .find(|line| line.contains(FACT))
            .unwrap_or_else(|| panic!("{tier}: the fact is listed: {text}"));
        assert!(line.contains("author unknown"), "{tier}: {line}");
        assert!(
            line.contains("the release train leaves on Tuesdays"),
            "{line}"
        );
    }
}

#[test]
fn a_fact_missing_kind_or_title_or_with_blank_provenance_still_fails_the_listing() {
    let complete = serde_json::json!({
        "kind": "fact",
        "title": "Release day",
        "statement": "the release train leaves on Tuesdays",
        "conversation": null,
        "archived": false,
    });
    let without = |key: &str| {
        let mut value = complete.clone();
        value.as_object_mut().unwrap().remove(key);
        value
    };
    let with = |key: &str, field: serde_json::Value| {
        let mut value = complete.clone();
        value[key] = field;
        value
    };
    for tier in ["local", "cluster"] {
        // Liveness anchor: the complete value lists.
        ok_json(tier, &stub(single_head(FACT, complete.clone())), &[]);
        for (case, value) in [
            ("missing kind", without("kind")),
            ("missing title", without("title")),
            ("blank author", with("author", serde_json::json!("  "))),
            ("non-string author", with("author", serde_json::json!(7))),
            ("blank stated_at", with("stated_at", serde_json::json!(""))),
            (
                "non-string stated_at",
                with("stated_at", serde_json::json!(false)),
            ),
        ] {
            let stub = stub(single_head(FACT, value));
            for json in [false, true] {
                let flags: &[&str] = if json { &["--json"] } else { &[] };
                let output = run_stub(tier, &stub, flags);
                let text = format!(
                    "{}{}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
                assert!(
                    !output.status.success(),
                    "{tier} {case}: must fail the listing: {text}"
                );
                assert!(text.contains(FACT), "{tier} {case}: name the id: {text}");
                assert!(text.contains("malformed"), "{tier} {case}: {text}");
            }
        }
    }
}

#[test]
fn memory_listing_human_output_shows_kind_author_conversation_and_archived_marker() {
    for tier in ["local", "cluster"] {
        let stub = stub(seeded_state());
        let output = run_stub(tier, &stub, &[]);
        let text = String::from_utf8_lossy(&output.stdout).to_string();
        assert!(output.status.success(), "{tier}: {text}");
        let lines_of = |id: &str| -> Vec<String> {
            text.lines()
                .filter(|line| line.contains(id))
                .map(str::to_string)
                .collect()
        };
        let fact_lines = lines_of(FACT);
        let instruction_lines = lines_of(INSTRUCTION);
        assert_eq!(fact_lines.len(), 2, "{tier}: one line per scope: {text}");
        assert_eq!(instruction_lines.len(), 2, "{tier}: {text}");
        for line in fact_lines.iter().chain(&instruction_lines) {
            assert!(line.contains("U0EXAMPLE1"), "author: {line}");
            assert!(line.contains(CONVERSATION), "conversation: {line}");
        }
        for line in &fact_lines {
            let lower = line.to_lowercase();
            assert!(lower.contains("fact"), "kind: {line}");
            assert!(!lower.contains("instruction"), "kind: {line}");
            assert!(!lower.contains("archived"), "active fact: {line}");
        }
        for line in &instruction_lines {
            let lower = line.to_lowercase();
            assert!(lower.contains("instruction"), "kind: {line}");
            assert!(lower.contains("archived"), "archived marker: {line}");
        }
    }
}

#[test]
fn archive_and_restore_post_the_current_version_to_the_typed_routes_at_both_tiers() {
    for tier in ["local", "cluster"] {
        for channel in [false, true] {
            let stub = stub(seeded_state());
            let archived = ok_json(tier, &stub, &scoped(&["--archive", FACT], channel));
            assert_eq!(archived["id"], FACT, "{archived}");
            assert_eq!(archived["archived"], true, "{archived}");
            assert_eq!(archived["version"], 3, "{archived}");
            assert_eq!(archived["kind"], "fact", "{archived}");
            assert_eq!(
                archived["scope"],
                if channel { "channel" } else { "agent" },
                "{archived}"
            );

            let restored = ok_json(tier, &stub, &scoped(&["--restore", FACT], channel));
            assert_eq!(restored["archived"], false, "{restored}");
            assert_eq!(restored["version"], 4, "{restored}");

            let posts = mutations(&stub);
            assert_eq!(posts.len(), 2, "{tier}: exactly one write each: {posts:?}");
            for (request, route, version) in [(&posts[0], "archive", 2), (&posts[1], "restore", 3)]
            {
                assert_eq!(request.method, "POST");
                let (path, _) = split_query(&request.path);
                assert_eq!(
                    path,
                    format!("/agents/{AGENT_ID}/state/_memory/{route}"),
                    "{tier}"
                );
                let body = body_of(request);
                assert_eq!(body["id"], FACT, "{body}");
                assert_eq!(body["expected_version"], version, "read version: {body}");
                assert_scope(&body, channel);
            }
            let scope = if channel {
                format!("channel:slack:{SLACK_ID}")
            } else {
                "agent".to_string()
            };
            let state = stub.state.lock().unwrap();
            let head = &state.heads[&(scope, FACT.to_string())];
            assert_eq!(head.version, 4);
            assert_eq!(head.value["archived"], false);
            assert_eq!(head.history.len(), 4);
        }
    }
}

#[test]
fn revert_posts_the_target_and_current_version_at_both_tiers() {
    for tier in ["local", "cluster"] {
        for channel in [false, true] {
            let stub = stub(seeded_state());
            let reverted = ok_json(
                tier,
                &stub,
                &scoped(&["--revert", FACT, "--to-version", "1"], channel),
            );
            assert_eq!(reverted["deleted"], false, "{reverted}");
            assert_eq!(reverted["version"], 3, "{reverted}");
            assert_eq!(
                reverted["entry"]["statement"], "agent prefers concise replies",
                "{reverted}"
            );
            assert_eq!(reverted["history"][0]["operation"], "revert", "{reverted}");
            assert_eq!(reverted["history"][0]["reverted_to_version"], 1);

            let posts = mutations(&stub);
            assert_eq!(posts.len(), 1, "{posts:?}");
            let (path, _) = split_query(&posts[0].path);
            assert_eq!(path, format!("/agents/{AGENT_ID}/state/_memory/revert"));
            let body = body_of(&posts[0]);
            assert_eq!(body["id"], FACT, "{body}");
            assert_eq!(body["expected_version"], 2, "{body}");
            assert_eq!(body["to_version"], 1, "{body}");
            assert_scope(&body, channel);

            // Revision zero removes the head; the listing no longer shows it.
            let removed = ok_json(
                tier,
                &stub,
                &scoped(&["--revert", INSTRUCTION, "--to-version", "0"], channel),
            );
            assert_eq!(removed["deleted"], true, "{removed}");
            assert!(removed["entry"].is_null(), "{removed}");
            assert_eq!(removed["version"], 3, "{removed}");
            let body = body_of(&mutations(&stub)[1]);
            assert_eq!(body["id"], INSTRUCTION, "{body}");
            assert_eq!(body["to_version"], 0, "{body}");
            assert_eq!(body["expected_version"], 2, "{body}");
            let listed = ok_json(tier, &stub, &scoped(&[], channel));
            let scope = if channel { "channel" } else { "agent" };
            let ids: Vec<_> = listed["facts"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|f| f["scope"] == scope)
                .map(|f| f["id"].clone())
                .collect();
            assert_eq!(ids, vec![serde_json::json!(FACT)], "{listed}");
        }
    }
}

#[test]
fn history_reads_the_typed_entry_route_with_and_without_a_channel_at_both_tiers() {
    for tier in ["local", "cluster"] {
        for channel in [false, true] {
            let stub = stub(seeded_state());
            let detail = ok_json(tier, &stub, &scoped(&["--history", FACT], channel));
            assert_eq!(detail["deleted"], false, "{detail}");
            assert_eq!(detail["version"], 2, "{detail}");
            assert_eq!(detail["entry"]["id"], FACT, "{detail}");
            let versions: Vec<_> = detail["history"]
                .as_array()
                .expect("history array")
                .iter()
                .map(|r| r["version"].as_u64().unwrap())
                .collect();
            assert_eq!(versions, vec![2, 1], "{detail}");
            assert_eq!(
                detail["history"][1]["snapshot"]["statement"],
                "agent prefers concise replies"
            );
            assert!(mutations(&stub).is_empty(), "history must not write");
            let reads: Vec<_> = stub
                .server
                .recorded()
                .into_iter()
                .filter(|r| r.path.contains("/state/_memory/entries/"))
                .collect();
            assert_eq!(reads.len(), 1, "{tier}: one typed detail read");
            let (path, query) = split_query(&reads[0].path);
            assert_eq!(
                path,
                format!("/agents/{AGENT_ID}/state/_memory/entries/{FACT}")
            );
            let get = |k: &str| query.get(k).map(String::as_str);
            if channel {
                assert_eq!(get("scope"), Some("channel"), "{query:?}");
                assert_eq!(get("channel_kind"), Some("slack"), "{query:?}");
                assert_eq!(get("channel_address"), Some(SLACK_ID), "{query:?}");
            } else {
                assert_eq!(get("scope"), Some("agent"), "{query:?}");
                assert!(!query.contains_key("channel_kind"), "{query:?}");
            }
        }
    }
}

#[test]
fn lifecycle_dry_runs_send_no_mutating_request_at_both_tiers() {
    for tier in ["local", "cluster"] {
        for (flags, route) in [
            (scoped(&["--archive", FACT], false), "archive"),
            (scoped(&["--restore", FACT], false), "restore"),
            (
                scoped(&["--revert", FACT, "--to-version", "1"], false),
                "revert",
            ),
            (scoped(&["--archive", FACT], true), "archive"),
            (
                scoped(&["--revert", FACT, "--to-version", "0"], true),
                "revert",
            ),
        ] {
            let stub = stub(seeded_state());
            let mut argv = flags.clone();
            argv.push("--dry-run");
            let plan = ok_json(tier, &stub, &argv);
            assert_eq!(plan["dry_run"], true, "{plan}");
            let lines: Vec<String> = plan["plan"]
                .as_array()
                .unwrap()
                .iter()
                .map(|l| l.as_str().unwrap().to_string())
                .collect();
            let post = lines
                .iter()
                .find(|l| l.starts_with("POST ") && l.contains(&format!("/_memory/{route}")))
                .unwrap_or_else(|| panic!("{tier} {flags:?}: plan names the write: {lines:?}"));
            assert!(
                post.contains("expected_version"),
                "CAS must be explicit: {post}"
            );
            assert!(
                lines.iter().any(|l| l.starts_with("GET ")),
                "the plan reads the current version first: {lines:?}"
            );
            assert!(
                mutations(&stub).is_empty(),
                "{tier} {flags:?}: dry run wrote"
            );
            assert_unchanged(&stub);
        }
        let stub = stub(seeded_state());
        let plan = ok_json(tier, &stub, &["--history", FACT, "--dry-run"]);
        assert_eq!(plan["dry_run"], true, "{plan}");
        assert!(mutations(&stub).is_empty());
    }
}

#[test]
fn a_stale_version_conflict_is_reported_and_never_retried_at_both_tiers() {
    for tier in ["local", "cluster"] {
        for flags in [
            scoped(&["--archive", FACT], false),
            scoped(&["--restore", INSTRUCTION], false),
            scoped(&["--revert", FACT, "--to-version", "1"], false),
            scoped(&["--archive", FACT], true),
        ] {
            let mut state = seeded_state();
            state.always_conflict = true;
            let stub = stub(state);
            for json in [false, true] {
                let mut argv = flags.clone();
                if json {
                    argv.push("--json");
                }
                let output = run_stub(tier, &stub, &argv);
                let text = format!(
                    "{}{}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
                assert!(
                    !output.status.success(),
                    "{tier} {flags:?}: must fail: {text}"
                );
                assert!(
                    text.to_lowercase().contains("stale"),
                    "{tier} {flags:?}: a 409 is reported as a stale version: {text}"
                );
                let compact = text.replace(' ', "");
                assert!(
                    !compact.contains("\"archived\":true")
                        && !compact.contains("\"deleted\":false"),
                    "no success payload: {text}"
                );
            }
            let posts = mutations(&stub);
            assert_eq!(
                posts.len(),
                2,
                "{tier} {flags:?}: one attempt per run, never retried: {posts:?}"
            );
            assert_unchanged(&stub);
        }
    }
}

#[test]
fn lifecycle_flags_conflict_with_add_delete_guidance_and_each_other() {
    let lifecycle: [&[&str]; 4] = [
        &["--archive", FACT],
        &["--restore", FACT],
        &["--revert", FACT, "--to-version", "1"],
        &["--history", FACT],
    ];
    for tier in ["local", "cluster"] {
        for action in lifecycle {
            // Anchor: each flag is accepted alone before its conflicts mean anything.
            dry_plan(tier, action);
            let mut others: Vec<Vec<&str>> = vec![
                vec!["--add", "operator note"],
                vec!["--delete", FACT],
                vec!["--guidance"],
                vec!["--guidance-from", "missing-guidance.md"],
                vec!["--reset-guidance"],
            ];
            others.extend(
                lifecycle
                    .iter()
                    .filter(|other| other[0] != action[0])
                    .map(|other| other.to_vec()),
            );
            for other in others {
                for (first, second) in [(action.to_vec(), other.clone()), (other, action.to_vec())]
                {
                    let mut flags = first;
                    flags.extend(second);
                    flags.extend(["--dry-run", "--json"]);
                    refused(tier, &flags, "cannot be used");
                }
            }
        }
    }
}

#[test]
fn lifecycle_flags_validate_fact_ids_before_any_http() {
    for tier in ["local", "cluster"] {
        for flag in ["--archive", "--restore", "--history"] {
            dry_plan(tier, &[flag, FACT]);
            for invalid in [
                "log",
                "guidance",
                "fact-short",
                "fact-0123456789ABCDEF0123456789abcdef",
                "../guidance",
            ] {
                refused(tier, &[flag, invalid, "--dry-run", "--json"], "fact");
            }
        }
        refused(
            tier,
            &[
                "--revert",
                "fact-short",
                "--to-version",
                "1",
                "--dry-run",
                "--json",
            ],
            "fact",
        );
    }
}

#[test]
fn revert_requires_a_nonnegative_to_version_and_to_version_requires_revert() {
    for tier in ["local", "cluster"] {
        dry_plan(tier, &["--revert", FACT, "--to-version", "0"]);
        for flags in [
            vec!["--revert", FACT, "--dry-run", "--json"],
            vec!["--to-version", "1", "--dry-run", "--json"],
            vec![
                "--archive",
                FACT,
                "--to-version",
                "1",
                "--dry-run",
                "--json",
            ],
            vec![
                "--revert",
                FACT,
                "--to-version",
                "-1",
                "--dry-run",
                "--json",
            ],
            vec![
                "--revert",
                FACT,
                "--to-version",
                "one",
                "--dry-run",
                "--json",
            ],
        ] {
            let output = run(tier, &flags);
            assert!(
                !output.status.success(),
                "{tier} {flags:?} must be a usage error: {}",
                String::from_utf8_lossy(&output.stdout)
            );
        }
    }
}

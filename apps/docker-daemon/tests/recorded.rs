//! Replays the AuthZReq payloads dockerd 29.9.0 actually sent (see the
//! fixture's `provenance`) through the policy, exactly as the plugin receives
//! them on /AuthZPlugin.AuthZReq.

use curie_docker_authz::policy::{authorize, Decision};
use serde_json::Value;

fn fixture() -> Value {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/recorded-requests.json"
    );
    let text = std::fs::read_to_string(path).expect("read fixture");
    serde_json::from_str(&text).expect("fixture is JSON")
}

fn session(fx: &Value, name: &str) -> Vec<Value> {
    fx["sessions"][name]
        .as_array()
        .unwrap_or_else(|| panic!("session {name} missing"))
        .clone()
}

fn b64_decode(s: &str) -> Vec<u8> {
    fn val(c: u8) -> u32 {
        match c {
            b'A'..=b'Z' => (c - b'A') as u32,
            b'a'..=b'z' => (c - b'a') as u32 + 26,
            b'0'..=b'9' => (c - b'0') as u32 + 52,
            b'+' => 62,
            b'/' => 63,
            _ => panic!("bad base64 byte {c}"),
        }
    }
    let bytes: Vec<u8> = s.bytes().filter(|&c| c != b'=').collect();
    let mut out = Vec::with_capacity(bytes.len() * 3 / 4);
    for chunk in bytes.chunks(4) {
        let mut acc = 0u32;
        for (i, &c) in chunk.iter().enumerate() {
            acc |= val(c) << (18 - 6 * i);
        }
        let n = chunk.len() - 1;
        for i in 0..n {
            out.push((acc >> (16 - 8 * i)) as u8);
        }
    }
    out
}

fn method(req: &Value) -> &str {
    req["RequestMethod"].as_str().expect("RequestMethod")
}

fn uri(req: &Value) -> &str {
    req["RequestUri"].as_str().expect("RequestUri")
}

/// Path without the query and without a `/vX.Y` API version prefix.
fn bare_path(uri: &str) -> String {
    let path = uri.split('?').next().unwrap();
    if let Some(rest) = path.strip_prefix("/v") {
        if let Some(slash) = rest.find('/') {
            let ver = &rest[..slash];
            if !ver.is_empty() && ver.bytes().all(|c| c.is_ascii_digit() || c == b'.') {
                return rest[slash..].to_string();
            }
        }
    }
    path.to_string()
}

fn body_text(req: &Value) -> String {
    match req.get("RequestBody").and_then(Value::as_str) {
        Some(b) => String::from_utf8(b64_decode(b)).expect("utf8 body"),
        None => String::new(),
    }
}

fn decide(req: &Value) -> Decision {
    let raw = serde_json::to_vec(req).expect("serialize request");
    authorize(&raw)
}

fn describe(session: &str, i: usize, req: &Value, d: &Decision) -> String {
    format!(
        "session {session} index {i}: {} {} -> {:?}",
        method(req),
        uri(req),
        d
    )
}

fn assert_allowed(session: &str, i: usize, req: &Value) {
    let d = decide(req);
    assert!(d.allow, "expected allow; {}", describe(session, i, req, &d));
}

fn assert_refused(session: &str, i: usize, req: &Value, rule: &str) {
    let d = decide(req);
    assert!(
        !d.allow && d.rule == rule,
        "expected refusal by {rule}; {}",
        describe(session, i, req, &d)
    );
    assert!(
        d.msg.starts_with(&format!("refused by rule {rule}")),
        "msg format; {}",
        describe(session, i, req, &d)
    );
}

#[test]
fn cli_and_compose_session_is_allowed_except_plugin_inspect() {
    let fx = fixture();
    let reqs = session(&fx, "cli-and-compose");
    assert!(reqs.len() > 50, "session unexpectedly short");
    // `docker inspect <missing>` falls through to GET /plugins/<name>/json
    // (plan wire fact 5); that one request is outside the allowlist.
    let plugin_idx: Vec<usize> = reqs
        .iter()
        .enumerate()
        .filter(|(_, r)| method(r) == "GET" && bare_path(uri(r)) == "/plugins/does-not-exist/json")
        .map(|(i, _)| i)
        .collect();
    assert_eq!(plugin_idx.len(), 1, "expected exactly one plugin inspect");
    for (i, req) in reqs.iter().enumerate() {
        if i == plugin_idx[0] {
            assert_refused("cli-and-compose", i, req, "endpoint-not-allowed");
            let d = decide(req);
            assert!(d.msg.contains("--type"), "plugins msg: {:?}", d);
        } else {
            assert_allowed("cli-and-compose", i, req);
        }
    }
}

#[test]
fn hardened_run_and_compose_session_is_all_allowed() {
    let fx = fixture();
    let reqs = session(&fx, "hardened-run-and-compose");
    assert!(reqs.len() > 20, "session unexpectedly short");
    for (i, req) in reqs.iter().enumerate() {
        assert_allowed("hardened-run-and-compose", i, req);
    }
}

#[test]
fn hardened_run_readds_default_cap_after_cap_drop_all() {
    // Plan rule 9: recorded hardened-run index 9 is --cap-drop ALL with
    // --cap-add NET_BIND_SERVICE and must stay allowed.
    let fx = fixture();
    let reqs = session(&fx, "hardened-run-and-compose");
    let req = &reqs[9];
    assert_eq!(method(req), "POST");
    assert_eq!(bare_path(uri(req)), "/containers/create");
    let body = body_text(req);
    assert!(
        body.contains("\"CapAdd\":[\"CAP_NET_BIND_SERVICE\"]"),
        "{body}"
    );
    assert!(body.contains("\"CapDrop\":[\"ALL\"]"), "{body}");
    assert_allowed("hardened-run-and-compose", 9, req);
}

/// (index, rule, method, bare path, marker that must appear in the decoded
/// body, or in the URI when the request carries no body).
const ESCAPES: &[(usize, &str, &str, &str, &str)] = &[
    (
        4,
        "privileged",
        "POST",
        "/containers/create",
        "\"Privileged\":true",
    ),
    (5, "cap-add", "POST", "/containers/create", "CAP_SYS_ADMIN"),
    (
        9,
        "host-namespace",
        "POST",
        "/containers/create",
        "\"UsernsMode\":\"host\"",
    ),
    (
        13,
        "host-namespace",
        "POST",
        "/containers/create",
        "\"PidMode\":\"host\"",
    ),
    (
        14,
        "host-namespace",
        "POST",
        "/containers/create",
        "\"IpcMode\":\"host\"",
    ),
    (
        18,
        "host-namespace",
        "POST",
        "/containers/create",
        "\"UTSMode\":\"host\"",
    ),
    (
        22,
        "host-namespace",
        "POST",
        "/containers/create",
        "\"NetworkMode\":\"host\"",
    ),
    (
        23,
        "host-namespace",
        "POST",
        "/containers/create",
        "\"CgroupnsMode\":\"host\"",
    ),
    (27, "host-bind", "POST", "/containers/create", "\"/:/host\""),
    (
        31,
        "host-bind",
        "POST",
        "/containers/create",
        "\"Type\":\"bind\"",
    ),
    (
        35,
        "security-opt",
        "POST",
        "/containers/create",
        "seccomp=unconfined",
    ),
    (
        39,
        "security-opt",
        "POST",
        "/containers/create",
        "apparmor=unconfined",
    ),
    // `--security-opt systempaths=unconfined` reaches the daemon as
    // MaskedPaths: [] (plan rule 16).
    (
        43,
        "security-opt",
        "POST",
        "/containers/create",
        "\"MaskedPaths\":[]",
    ),
    (
        47,
        "devices",
        "POST",
        "/containers/create",
        "\"PathOnHost\":\"/dev/null\"",
    ),
    (
        51,
        "sysctls",
        "POST",
        "/containers/create",
        "net.ipv4.ip_forward",
    ),
    (
        55,
        "cgroup-parent",
        "POST",
        "/containers/create",
        "\"CgroupParent\":\"/x\"",
    ),
    (
        59,
        "log-driver",
        "POST",
        "/containers/create",
        "\"Type\":\"syslog\"",
    ),
    (
        63,
        "volume-driver-options",
        "POST",
        "/containers/create",
        "\"o\":\"bind\"",
    ),
    (67, "devices", "POST", "/containers/create", "\"gpu\""),
    (
        71,
        "volume-driver-options",
        "POST",
        "/volumes/create",
        "\"device\":\"/\"",
    ),
    (
        72,
        "network-driver-options",
        "POST",
        "/networks/create",
        "br-x",
    ),
    (
        73,
        "network-driver",
        "POST",
        "/networks/create",
        "\"Driver\":\"macvlan\"",
    ),
    (
        75,
        "exec-privileged",
        "POST",
        "/containers/c1/exec",
        "\"Privileged\":true",
    ),
    (78, "endpoint-not-allowed", "GET", "/plugins", ""),
    (79, "endpoint-not-allowed", "GET", "/system/df", ""),
    (80, "endpoint-not-allowed", "POST", "/containers/prune", ""),
    // The classic-builder escape (DOCKER_BUILDKIT=0 docker build --network=host).
    // POST /build itself is outside the allowlist: its form-body parameters are
    // invisible to the plugin when the Content-Type header is duplicated.
    (
        81,
        "endpoint-not-allowed",
        "POST",
        "/build",
        "networkmode=host",
    ),
    (
        82,
        "endpoint-not-allowed",
        "POST",
        "/containers/c1/update",
        "\"Memory\":67108864",
    ),
    (
        83,
        "endpoint-not-allowed",
        "POST",
        "/commit",
        "container=c1",
    ),
];

#[test]
fn escapes_fixture_indices_have_not_shifted() {
    let fx = fixture();
    let reqs = session(&fx, "escapes");
    for &(i, _rule, m, path, marker) in ESCAPES {
        let req = reqs
            .get(i)
            .unwrap_or_else(|| panic!("escapes index {i} missing"));
        assert_eq!(method(req), m, "escapes index {i} method; uri {}", uri(req));
        assert_eq!(bare_path(uri(req)), path, "escapes index {i} path");
        if !marker.is_empty() {
            let body = body_text(req);
            let hay = if body.is_empty() {
                uri(req).to_string()
            } else {
                body
            };
            assert!(
                hay.contains(marker),
                "escapes index {i} no longer carries {marker}: {hay}"
            );
        }
    }
}

#[test]
fn escapes_session_refuses_each_escape_by_rule_and_allows_the_rest() {
    let fx = fixture();
    let reqs = session(&fx, "escapes");
    for (i, req) in reqs.iter().enumerate() {
        match ESCAPES.iter().find(|e| e.0 == i) {
            Some(&(_, rule, ..)) => assert_refused("escapes", i, req, rule),
            None => assert_allowed("escapes", i, req),
        }
    }
}

#[test]
fn plugins_list_refusal_points_at_type_flag() {
    let fx = fixture();
    let reqs = session(&fx, "escapes");
    let d = decide(&reqs[78]);
    assert!(!d.allow, "{d:?}");
    assert!(d.msg.contains("--type"), "{d:?}");
}

#[test]
fn recorded_pull_carries_no_content_type_and_any_added_one_is_refused() {
    // POST /images/create refuses every Content-Type header (rule form-body):
    // dockerd forwards only the LAST value of a duplicated header, while Go's
    // form parser reads the FIRST, so no single forwarded value is safe. The
    // real pull from Docker CLI 29 sends none.
    let fx = fixture();
    let reqs = session(&fx, "cli-and-compose");
    let req = &reqs[3];
    assert_eq!(method(req), "POST");
    assert_eq!(bare_path(uri(req)), "/images/create");
    let headers = req["RequestHeaders"].as_object().expect("headers");
    assert!(
        !headers
            .keys()
            .any(|k| k.eq_ignore_ascii_case("content-type")),
        "recorded pull now carries a Content-Type: {headers:?}"
    );
    assert_allowed("cli-and-compose", 3, req);
    for ct in ["text/plain", "application/json", "application/x-tar"] {
        let mut altered = req.clone();
        altered["RequestHeaders"]["Content-Type"] = Value::from(ct);
        assert_refused(
            "cli-and-compose (Content-Type added)",
            3,
            &altered,
            "form-body",
        );
    }
}

//! The policy never panics: truncated bodies and garbled AuthZReq envelopes
//! built from every recorded request must each come back as a Decision.

use curie_docker_authz::policy::Decision;
use curie_docker_authz::policy::{authorize, authorize_parts};
use serde_json::Value;

/// `guarded` turns a policy panic into rule `internal-error`; a "never
/// panics" test must see that as a failure, not as a refusal.
fn no_panic(d: &Decision) {
    assert_ne!(d.rule, "internal-error", "the policy panicked: {d:?}");
}

fn fixture_requests() -> Vec<Value> {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/recorded-requests.json"
    );
    let fx: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    let mut out = Vec::new();
    for (_, reqs) in fx["sessions"].as_object().unwrap() {
        out.extend(reqs.as_array().unwrap().iter().cloned());
    }
    assert!(out.len() > 100, "fixture unexpectedly small");
    out
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
    let mut out = Vec::new();
    for chunk in bytes.chunks(4) {
        let mut acc = 0u32;
        for (i, &c) in chunk.iter().enumerate() {
            acc |= val(c) << (18 - 6 * i);
        }
        for i in 0..chunk.len() - 1 {
            out.push((acc >> (16 - 8 * i)) as u8);
        }
    }
    out
}

fn bare_path(uri: &str) -> &str {
    let path = uri.split('?').next().unwrap();
    match path.strip_prefix("/v1.56") {
        Some(rest) => rest,
        None => path,
    }
}

/// Endpoints whose JSON body the policy requires and checks.
fn needs_json(method: &str, uri: &str) -> bool {
    let p = bare_path(uri);
    method == "POST"
        && (p == "/containers/create"
            || p == "/networks/create"
            || p == "/volumes/create"
            || (p.starts_with("/containers/") && p.ends_with("/exec"))
            || (p.starts_with("/networks/") && p.ends_with("/connect")))
}

/// Prefix lengths to try: every length near both ends, a stride through the
/// middle, so each body costs a bounded number of decisions.
fn cut_points(len: usize) -> Vec<usize> {
    let mut v: Vec<usize> = (0..len.min(64)).collect();
    v.extend((64..len).step_by(5));
    v.extend(len.saturating_sub(64)..len);
    v.sort_unstable();
    v.dedup();
    v
}

#[test]
fn truncated_bodies_never_panic_and_required_ones_are_refused() {
    let mut checked = 0usize;
    for req in fixture_requests() {
        let Some(b64) = req.get("RequestBody").and_then(Value::as_str) else {
            continue;
        };
        let method = req["RequestMethod"].as_str().unwrap();
        let uri = req["RequestUri"].as_str().unwrap();
        let body = b64_decode(b64);
        let required = needs_json(method, uri);
        for cut in cut_points(body.len()) {
            let d = authorize_parts(method, uri, Some(&body[..cut]));
            checked += 1;
            if required {
                // A strict prefix of a JSON object is never a complete
                // document (recorded bodies carry no trailing whitespace).
                // It must be refused by a parse rule: `internal-error` is a
                // caught panic, which is not a valid way to refuse.
                let want = if cut == 0 {
                    "body-required"
                } else {
                    "body-unparseable"
                };
                assert!(
                    !d.allow && d.rule == want,
                    "{method} {uri} body truncated to {cut}/{}: want rule {want}, got {d:?}",
                    body.len()
                );
            }
        }
    }
    assert!(checked > 1000, "only {checked} truncations exercised");
}

#[test]
fn garbled_bodies_never_panic() {
    for req in fixture_requests() {
        let Some(b64) = req.get("RequestBody").and_then(Value::as_str) else {
            continue;
        };
        let method = req["RequestMethod"].as_str().unwrap();
        let uri = req["RequestUri"].as_str().unwrap();
        let body = b64_decode(b64);
        for i in (0..body.len()).step_by(11) {
            for b in [0xffu8, b'"', b'{', b'\\', 0x00, b']'] {
                let mut g = body.clone();
                g[i] = b;
                no_panic(&authorize_parts(method, uri, Some(&g)));
            }
        }
    }
}

#[test]
fn garbled_envelopes_never_panic() {
    for req in fixture_requests() {
        let raw = serde_json::to_vec(&req).unwrap();
        for cut in (0..raw.len()).step_by(13) {
            no_panic(&authorize(&raw[..cut]));
        }
        for i in (0..raw.len()).step_by(17) {
            for b in [0xffu8, b'"', b'}', b'%', b'\\'] {
                let mut g = raw.clone();
                g[i] = b;
                no_panic(&authorize(&g));
            }
        }
        let mut bad = req.clone();
        bad["RequestBody"] = Value::String("@@@@".into());
        let d = authorize(&serde_json::to_vec(&bad).unwrap());
        assert!(!d.allow && d.rule == "request-unparseable", "{d:?}");
        for junk in ["", "=", "Q", "QQ", "QQ=", "Q===", "====", "QUJD\n", "QU JD"] {
            let mut g = req.clone();
            g["RequestBody"] = Value::String(junk.into());
            no_panic(&authorize(&serde_json::to_vec(&g).unwrap()));
        }
        for uri in [
            "", "%", "/%", "/v", "/v1.56", "/v1.56/", "?", "/?&&=;", "/\u{0}",
        ] {
            let mut g = req.clone();
            g["RequestUri"] = Value::String(uri.into());
            no_panic(&authorize(&serde_json::to_vec(&g).unwrap()));
        }
    }
}

#[test]
fn deeply_nested_body_is_refused_without_crashing() {
    // A hand-written duplicate-preserving parser must not recurse without
    // bound: a stack overflow aborts the whole plugin.
    let depth = 200_000;
    let mut body = String::from(r#"{"Image":"a","HostConfig":{"Binds":"#);
    body.push_str(&"[".repeat(depth));
    body.push_str(&"]".repeat(depth));
    body.push_str("}}");
    let d = authorize_parts("POST", "/v1.56/containers/create", Some(body.as_bytes()));
    // Refused by the parser's depth bound, not by a caught panic.
    assert!(!d.allow && d.rule == "body-unparseable", "{d:?}");
    let mut labels = String::from(r#"{"Image":"a","Labels":"#);
    labels.push_str(&"{\"a\":".repeat(depth));
    labels.push('1');
    labels.push_str(&"}".repeat(depth));
    labels.push('}');
    // Only that it returns: Labels is not inspected, so either answer is fine.
    no_panic(&authorize_parts(
        "POST",
        "/v1.56/containers/create",
        Some(labels.as_bytes()),
    ));
}

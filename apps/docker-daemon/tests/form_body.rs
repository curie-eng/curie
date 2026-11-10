//! Rule `form-body`: dockerd forwards RequestBody to the plugin only for
//! application/json, but moby reads query parameters with Go's r.FormValue,
//! which also parses application/x-www-form-urlencoded and multipart/form-data
//! bodies (body values win over the URL query). A form body is therefore a
//! second, unseen parameter channel, so any request that declares one is
//! refused on every endpoint. The header reaches the plugin only through the
//! AuthZReq envelope's RequestHeaders, so every case here goes through
//! `authorize` with a full envelope.
//!
//! dockerd forwards only the LAST value of a repeated Content-Type header,
//! while Go's form parser uses the FIRST. A request can therefore send
//! `Content-Type: application/x-www-form-urlencoded` followed by
//! `Content-Type: text/plain`, and the plugin sees only `text/plain`. So
//! POST /images/create refuses ANY Content-Type (the real pull sends none),
//! and POST /build, the other endpoint whose parameters matter, is outside the
//! allowlist altogether (see rules.rs).

use curie_docker_authz::policy::{authorize, Decision};
use serde_json::{json, Map, Value};

const RULE: &str = "form-body";

#[track_caller]
fn assert_refused(d: Decision) {
    assert!(
        !d.allow && d.rule == RULE,
        "expected refusal by rule {RULE}, got {d:?}"
    );
}

#[track_caller]
fn assert_allowed(d: Decision) {
    assert!(d.allow, "expected allow, got {d:?}");
}

fn b64_encode(data: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in data.chunks(3) {
        let n = chunk
            .iter()
            .enumerate()
            .fold(0u32, |n, (i, &b)| n | (b as u32) << (16 - 8 * i));
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(T[(n >> (18 - 6 * i) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

/// Envelope as dockerd sends it. `headers` is the RequestHeaders map (None
/// leaves the key out entirely); `body` is the raw, not yet encoded body.
fn envelope(
    method: &str,
    uri: &str,
    headers: Option<&[(&str, &str)]>,
    body: Option<&[u8]>,
) -> Vec<u8> {
    let mut env = json!({"RequestMethod": method, "RequestUri": uri});
    if let Some(h) = headers {
        let map: Map<String, Value> = h.iter().map(|(k, v)| ((*k).to_owned(), json!(v))).collect();
        env["RequestHeaders"] = Value::Object(map);
    }
    if let Some(b) = body {
        env["RequestBody"] = json!(b64_encode(b));
    }
    serde_json::to_vec(&env).unwrap()
}

fn with_type(method: &str, uri: &str, content_type: &str) -> Decision {
    authorize(&envelope(
        method,
        uri,
        Some(&[
            ("User-Agent", "Docker-Client/29.9.0 (linux)"),
            ("Content-Type", content_type),
        ]),
        None,
    ))
}

// ---------------------------------------------------------------- refused

#[test]
fn urlencoded_body_on_image_pull_is_refused() {
    // The body `fromSrc=https://...` would turn this pull into an import.
    assert_refused(with_type(
        "POST",
        "/v1.56/images/create?fromImage=alpine",
        "application/x-www-form-urlencoded",
    ));
}

#[test]
fn form_media_type_match_ignores_case_parameters_and_whitespace() {
    assert_refused(with_type(
        "POST",
        "/v1.56/images/create?fromImage=alpine",
        "Application/X-WWW-Form-Urlencoded; charset=utf-8",
    ));
    assert_refused(with_type(
        "POST",
        "/v1.56/images/create?fromImage=alpine",
        "  application/x-www-form-urlencoded  ",
    ));
    assert_refused(with_type(
        "POST",
        "/v1.56/images/create?fromImage=alpine",
        "application/x-www-form-urlencoded ;charset=utf-8",
    ));
}

#[test]
fn multipart_body_on_build_is_refused() {
    assert_refused(with_type(
        "POST",
        "/v1.56/build",
        "multipart/form-data; boundary=x",
    ));
    assert_refused(with_type(
        "POST",
        "/v1.56/build?dockerfile=Dockerfile",
        "MULTIPART/FORM-DATA",
    ));
}

#[test]
fn form_body_is_refused_on_any_endpoint() {
    assert_refused(with_type(
        "GET",
        "/v1.56/info",
        "application/x-www-form-urlencoded",
    ));
    assert_refused(with_type(
        "HEAD",
        "/_ping",
        "multipart/form-data; boundary=x",
    ));
}

#[test]
fn content_type_header_name_is_case_insensitive() {
    for key in ["content-type", "CONTENT-TYPE", "content-Type"] {
        let d = authorize(&envelope(
            "POST",
            "/v1.56/images/create?fromImage=alpine",
            Some(&[(key, "application/x-www-form-urlencoded")]),
            None,
        ));
        assert!(
            !d.allow && d.rule == RULE,
            "header key {key:?}: expected refusal by rule {RULE}, got {d:?}"
        );
    }
}

#[test]
fn image_pull_with_any_content_type_is_refused() {
    let uri = "/v1.56/images/create?fromImage=alpine&tag=3.20";
    // `text/plain` is also what the plugin sees for the duplicate-header trick
    // (`Content-Type: application/x-www-form-urlencoded` then
    // `Content-Type: text/plain`): dockerd forwards only the last value.
    for ct in [
        "text/plain",
        "application/json",
        "application/x-tar",
        "application/octet-stream",
        "",
    ] {
        let d = with_type("POST", uri, ct);
        assert!(
            !d.allow && d.rule == RULE,
            "Content-Type {ct:?}: expected refusal by rule {RULE}, got {d:?}"
        );
    }
    for prefix in ["", "/v1.24", "/v1.56"] {
        assert_refused(with_type(
            "POST",
            &format!("{prefix}/images/create?fromImage=alpine"),
            "text/plain",
        ));
    }
}

#[test]
fn image_pull_content_type_lookup_is_case_insensitive() {
    for key in ["content-type", "CONTENT-TYPE", "Content-type"] {
        let d = authorize(&envelope(
            "POST",
            "/v1.56/images/create?fromImage=alpine",
            Some(&[("Content-Length", "0"), (key, "text/plain")]),
            None,
        ));
        assert!(
            !d.allow && d.rule == RULE,
            "header key {key:?}: expected refusal by rule {RULE}, got {d:?}"
        );
    }
}

#[test]
fn build_is_refused_whatever_its_content_type() {
    // /build is outside the allowlist; a tar context no longer gets through.
    for ct in ["application/x-tar", "text/plain"] {
        let d = with_type("POST", "/v1.56/build?networkmode=host", ct);
        assert!(!d.allow, "Content-Type {ct:?}: expected refusal, got {d:?}");
    }
    let d = authorize(&envelope("POST", "/v1.56/build", None, None));
    assert!(
        !d.allow && d.rule == "endpoint-not-allowed",
        "expected endpoint-not-allowed, got {d:?}"
    );
}

// ---------------------------------------------------------------- liveness

#[test]
fn buildkit_grpc_and_session_without_content_type_are_allowed() {
    // Recorded cli-and-compose 44 and 47: what `docker build` (CLI 29),
    // compose and buildx send. No Content-Type.
    for uri in ["/grpc", "/session"] {
        assert_allowed(authorize(&envelope(
            "POST",
            uri,
            Some(&[("User-Agent", "moby-client/v0.6.0 linux/amd64")]),
            None,
        )));
    }
}

#[test]
fn image_pull_without_content_type_is_allowed() {
    let uri = "/v1.56/images/create?fromImage=alpine&tag=3.20";
    // As recorded: User-Agent only.
    assert_allowed(authorize(&envelope(
        "POST",
        uri,
        Some(&[("User-Agent", "Docker-Client/29.9.0 (linux)")]),
        None,
    )));
    // Only Content-Length 0.
    assert_allowed(authorize(&envelope(
        "POST",
        uri,
        Some(&[("Content-Length", "0")]),
        None,
    )));
    // No RequestHeaders at all.
    assert_allowed(authorize(&envelope("POST", uri, None, None)));
}

#[test]
fn json_container_create_is_allowed() {
    let body = br#"{"Image":"alpine:3.20","Cmd":["true"],"HostConfig":{"Privileged":false}}"#;
    assert_allowed(authorize(&envelope(
        "POST",
        "/v1.56/containers/create",
        Some(&[("Content-Type", "application/json")]),
        Some(body),
    )));
}

#[test]
fn envelope_without_request_headers_is_allowed() {
    assert_allowed(authorize(&envelope("GET", "/_ping", None, None)));
    assert_allowed(authorize(
        br#"{"RequestMethod":"GET","RequestUri":"/_ping"}"#,
    ));
}

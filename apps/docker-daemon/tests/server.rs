//! Plugin protocol: the `handle` router directly, and `serve` over a real
//! unix socket spoken to as raw HTTP/1.1.

use curie_docker_authz::server::{handle, serve};
use serde_json::{json, Value};
use std::io::{Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

fn b64_encode(data: &[u8]) -> String {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in data.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (b[0] as u32) << 16 | (b[1] as u32) << 8 | b[2] as u32;
        out.push(A[(n >> 18) as usize & 63] as char);
        out.push(A[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            A[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            A[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

fn authz_req(method: &str, uri: &str, body: Option<&[u8]>) -> Vec<u8> {
    let mut req = json!({"RequestMethod": method, "RequestUri": uri,
                         "RequestHeaders": {"User-Agent": "Docker-Client/29.9.0 (linux)"}});
    if let Some(b) = body {
        req["RequestBody"] = Value::String(b64_encode(b));
        req["RequestHeaders"]["Content-Type"] = json!("application/json");
    }
    serde_json::to_vec(&req).unwrap()
}

/// A create body as `docker run --rm alpine:3.20 true` sends it (trimmed
/// from recorded cli-and-compose index 4).
fn allowed_create() -> Vec<u8> {
    let body = json!({"Image": "alpine:3.20", "Cmd": ["true"], "AttachStdout": true,
        "HostConfig": {"Binds": null, "LogConfig": {"Type": "", "Config": {}},
                       "NetworkMode": "default", "AutoRemove": true, "Privileged": false,
                       "CapAdd": null, "SecurityOpt": null, "MaskedPaths": null}});
    authz_req(
        "POST",
        "/v1.56/containers/create",
        Some(&serde_json::to_vec(&body).unwrap()),
    )
}

fn privileged_create() -> Vec<u8> {
    let body = br#"{"Image":"alpine:3.20","HostConfig":{"Privileged":true}}"#;
    authz_req("POST", "/v1.56/containers/create", Some(body))
}

fn parse(body: &[u8]) -> Value {
    serde_json::from_slice(body)
        .unwrap_or_else(|e| panic!("response not JSON ({e}): {}", String::from_utf8_lossy(body)))
}

// ------------------------------------------------------------ handle()

#[test]
fn handle_activate_implements_authz() {
    let (status, body) = handle("POST", "/Plugin.Activate", b"");
    assert_eq!(status, 200);
    let v = parse(&body);
    let imps = v["Implements"].as_array().expect("Implements array");
    assert!(imps.iter().any(|x| x == "authz"), "{v}");
}

#[test]
fn handle_authz_req_allowed() {
    let (status, body) = handle("POST", "/AuthZPlugin.AuthZReq", &allowed_create());
    assert_eq!(status, 200);
    let v = parse(&body);
    assert_eq!(v["Allow"], json!(true), "{v}");
    assert!(
        v.get("Msg").is_none_or(|m| m.is_null() || m == ""),
        "Msg only on refusal: {v}"
    );
}

#[test]
fn handle_authz_req_refused_privileged() {
    let (status, body) = handle("POST", "/AuthZPlugin.AuthZReq", &privileged_create());
    assert_eq!(status, 200);
    let v = parse(&body);
    assert_eq!(v["Allow"], json!(false), "{v}");
    let msg = v["Msg"].as_str().expect("Msg string");
    assert!(msg.contains("refused by rule privileged"), "{v}");
}

#[test]
fn handle_authz_req_malformed_is_refused() {
    let (status, body) = handle("POST", "/AuthZPlugin.AuthZReq", b"{not json");
    assert_eq!(status, 200);
    let v = parse(&body);
    assert_eq!(v["Allow"], json!(false), "{v}");
    assert!(
        v["Msg"]
            .as_str()
            .unwrap_or("")
            .contains("request-unparseable"),
        "{v}"
    );
}

#[test]
fn handle_authz_res_allows() {
    let res = json!({"RequestMethod": "GET", "RequestUri": "/v1.56/info",
                     "ResponseStatusCode": 200});
    let (status, body) = handle(
        "POST",
        "/AuthZPlugin.AuthZRes",
        &serde_json::to_vec(&res).unwrap(),
    );
    assert_eq!(status, 200);
    assert_eq!(parse(&body)["Allow"], json!(true));
}

#[test]
fn handle_unknown_path_is_404() {
    for path in [
        "/",
        "/Plugin.Nope",
        "/VolumeDriver.Create",
        "/AuthZPlugin.AuthZReq/x",
    ] {
        let (status, body) = handle("POST", path, b"{}");
        assert_eq!(status, 404, "{path}");
        let v = parse(&body);
        assert!(v["Err"].is_string(), "{path}: {v}");
    }
}

// ------------------------------------------------------------- serve()

static COUNTER: AtomicUsize = AtomicUsize::new(0);

/// A temp dir holding the plugin socket; removed on drop.
struct Plugin {
    dir: PathBuf,
    sock: PathBuf,
}

impl Drop for Plugin {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn start() -> Plugin {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("cda-{}-{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let sock = dir.join("p.sock");
    let listener = UnixListener::bind(&sock).unwrap();
    std::thread::spawn(move || {
        let _ = serve(listener);
    });
    Plugin { dir, sock }
}

fn connect(p: &Plugin) -> UnixStream {
    let s = UnixStream::connect(&p.sock).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    s.set_write_timeout(Some(Duration::from_secs(10))).unwrap();
    s
}

fn send(s: &mut UnixStream, method: &str, path: &str, body: &[u8]) {
    let head = format!(
        "{method} {path} HTTP/1.1\r\nHost: plugin\r\nAccept: application/vnd.docker.plugins.v1.2+json\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
        body.len()
    );
    s.write_all(head.as_bytes()).unwrap();
    s.write_all(body).unwrap();
    s.flush().unwrap();
}

/// Read one response. Errors (timeout, reset, EOF before a full head) come
/// back as Err so callers can decide whether that is acceptable.
fn read_response(s: &mut UnixStream) -> Result<(u16, Vec<u8>), String> {
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    while !buf.ends_with(b"\r\n\r\n") {
        match s.read(&mut byte) {
            Ok(0) => return Err(format!("EOF after {} head bytes", buf.len())),
            Ok(_) => buf.push(byte[0]),
            Err(e) => return Err(format!("read error ({:?}): {e}", e.kind())),
        }
        if buf.len() > 64 * 1024 {
            return Err("response head too large".into());
        }
    }
    let head = String::from_utf8_lossy(&buf).to_string();
    let mut lines = head.split("\r\n");
    let status_line = lines.next().unwrap_or("");
    let status: u16 = status_line
        .split(' ')
        .nth(1)
        .and_then(|c| c.parse().ok())
        .ok_or_else(|| format!("bad status line {status_line:?}"))?;
    let mut len = None;
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            if k.trim().eq_ignore_ascii_case("content-length") {
                len = v.trim().parse::<usize>().ok();
            }
        }
    }
    let mut body = Vec::new();
    match len {
        Some(n) => {
            body.resize(n, 0);
            s.read_exact(&mut body)
                .map_err(|e| format!("body read: {e}"))?;
        }
        None => {
            s.read_to_end(&mut body)
                .map_err(|e| format!("body read: {e}"))?;
        }
    }
    Ok((status, body))
}

fn roundtrip(p: &Plugin, method: &str, path: &str, body: &[u8]) -> (u16, Value) {
    let mut s = connect(p);
    send(&mut s, method, path, body);
    let (status, body) = read_response(&mut s).expect("response");
    (status, parse(&body))
}

#[test]
fn serve_activate() {
    let p = start();
    let (status, v) = roundtrip(&p, "POST", "/Plugin.Activate", b"");
    assert_eq!(status, 200);
    assert!(
        v["Implements"]
            .as_array()
            .unwrap()
            .iter()
            .any(|x| x == "authz"),
        "{v}"
    );
}

#[test]
fn serve_allowed_create() {
    let p = start();
    let (status, v) = roundtrip(&p, "POST", "/AuthZPlugin.AuthZReq", &allowed_create());
    assert_eq!(status, 200);
    assert_eq!(v["Allow"], json!(true), "{v}");
}

#[test]
fn serve_privileged_create_refused() {
    let p = start();
    let (status, v) = roundtrip(&p, "POST", "/AuthZPlugin.AuthZReq", &privileged_create());
    assert_eq!(status, 200);
    assert_eq!(v["Allow"], json!(false), "{v}");
    assert!(
        v["Msg"]
            .as_str()
            .unwrap_or("")
            .contains("refused by rule privileged"),
        "{v}"
    );
}

#[test]
fn serve_malformed_request_refused() {
    let p = start();
    let (status, v) = roundtrip(&p, "POST", "/AuthZPlugin.AuthZReq", b"]]] not json");
    assert_eq!(status, 200);
    assert_eq!(v["Allow"], json!(false), "{v}");
    assert!(
        v["Msg"]
            .as_str()
            .unwrap_or("")
            .contains("request-unparseable"),
        "{v}"
    );
}

#[test]
fn serve_authz_res_allows() {
    let p = start();
    let res = br#"{"RequestMethod":"GET","RequestUri":"/v1.56/info","ResponseStatusCode":200}"#;
    let (status, v) = roundtrip(&p, "POST", "/AuthZPlugin.AuthZRes", res);
    assert_eq!(status, 200);
    assert_eq!(v["Allow"], json!(true), "{v}");
}

#[test]
fn serve_unknown_path_404() {
    let p = start();
    let (status, v) = roundtrip(&p, "POST", "/Plugin.Nope", b"{}");
    assert_eq!(status, 404);
    assert!(v["Err"].is_string(), "{v}");
}

#[test]
fn serve_keep_alive_two_requests_one_connection() {
    let p = start();
    let mut s = connect(&p);
    send(
        &mut s,
        "POST",
        "/AuthZPlugin.AuthZReq",
        &privileged_create(),
    );
    let (status, body) = read_response(&mut s).expect("first response");
    assert_eq!(status, 200);
    assert_eq!(parse(&body)["Allow"], json!(false));
    send(&mut s, "POST", "/AuthZPlugin.AuthZReq", &allowed_create());
    let (status, body) = read_response(&mut s).expect("second response on same connection");
    assert_eq!(status, 200);
    assert_eq!(parse(&body)["Allow"], json!(true));
}

#[test]
fn serve_chunked_request_is_never_allowed() {
    let p = start();
    let mut s = connect(&p);
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let body = allowed_create();
    let mut req = Vec::new();
    req.extend_from_slice(
        b"POST /AuthZPlugin.AuthZReq HTTP/1.1\r\nHost: plugin\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n",
    );
    req.extend_from_slice(format!("{:x}\r\n", body.len()).as_bytes());
    req.extend_from_slice(&body);
    req.extend_from_slice(b"\r\n0\r\n\r\n");
    // The server may close before reading it all; a write error is fine.
    let _ = s.write_all(&req);
    assert_closed_or_4xx(read_response(&mut s), "chunked request");
}

/// A refused-at-the-transport answer: either the server closed the
/// connection (EOF or reset) or it answered 4xx. A timeout or any other read
/// error fails, so a server that hangs on the request cannot pass.
fn assert_closed_or_4xx(r: Result<(u16, Vec<u8>), String>, what: &str) {
    match r {
        Err(e) => assert!(
            e.starts_with("EOF ") || e.contains("(ConnectionReset)"),
            "{what}: expected a closed connection or a 4xx, got {e}"
        ),
        Ok((status, body)) => assert!(
            (400..500).contains(&status),
            "{what}: expected a closed connection or a 4xx, got {status} {}",
            String::from_utf8_lossy(&body)
        ),
    }
}

#[test]
fn serve_oversized_request_head_is_never_allowed() {
    let p = start();
    let mut s = connect(&p);
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    // One header line pushes the head past the 16 KiB cap.
    let pad = "a".repeat(17 * 1024);
    let body = allowed_create();
    let head = format!(
        "POST /AuthZPlugin.AuthZReq HTTP/1.1\r\nHost: plugin\r\nX-Pad: {pad}\r\nContent-Length: {}\r\n\r\n",
        body.len()
    );
    // The server may answer and close before it reads everything.
    let _ = s
        .write_all(head.as_bytes())
        .and_then(|()| s.write_all(&body));
    assert_closed_or_4xx(read_response(&mut s), "oversized head");
}

#[test]
fn serve_body_over_8_mib_is_never_allowed() {
    let p = start();
    let mut s = connect(&p);
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let len = 8 * 1024 * 1024 + 1;
    let head = format!(
        "POST /AuthZPlugin.AuthZReq HTTP/1.1\r\nHost: plugin\r\nContent-Type: application/json\r\nContent-Length: {len}\r\n\r\n"
    );
    s.write_all(head.as_bytes()).unwrap();
    // Send the whole oversized body from another thread: the server must
    // refuse on the declared length without a reply depending on the rest.
    let mut w = s.try_clone().unwrap();
    let writer = std::thread::spawn(move || {
        let chunk = vec![b' '; 64 * 1024];
        let mut left = len;
        while left > 0 {
            let n = left.min(chunk.len());
            if w.write_all(&chunk[..n]).is_err() {
                break;
            }
            left -= n;
        }
    });
    assert_closed_or_4xx(read_response(&mut s), "8 MiB + 1 body");
    let _ = s.shutdown(std::net::Shutdown::Both);
    writer.join().unwrap();
}

#[test]
fn serve_large_legitimate_create_is_answered_and_allowed() {
    // A valid create body just under 1 MiB (a long Env list) base64-encodes
    // to an envelope of about 1.4 MiB, well under the 8 MiB body cap.
    let env: Vec<String> = (0..16_000)
        .map(|i| format!("CURIE_TEST_VAR_{i:05}={}", "x".repeat(40)))
        .collect();
    let create = json!({"Image": "alpine:3.20", "Cmd": ["true"], "Env": env,
        "HostConfig": {"NetworkMode": "default", "AutoRemove": true, "Privileged": false}});
    let create = serde_json::to_vec(&create).unwrap();
    assert!(
        (900 * 1024..1024 * 1024).contains(&create.len()),
        "create body is {} bytes",
        create.len()
    );
    let envelope = authz_req("POST", "/v1.56/containers/create", Some(&create));
    assert!(
        envelope.len() > 1300 * 1024,
        "envelope is {} bytes",
        envelope.len()
    );
    let p = start();
    let (status, v) = roundtrip(&p, "POST", "/AuthZPlugin.AuthZReq", &envelope);
    assert_eq!(status, 200);
    assert_eq!(v["Allow"], json!(true), "{v}");
}

#[test]
fn serve_survives_a_bad_connection() {
    let p = start();
    {
        let mut s = connect(&p);
        let _ = s.write_all(b"GARBAGE\r\n\r\n");
    }
    {
        // Content-Length promising more than is sent, then hang up.
        let mut s = connect(&p);
        let _ = s
            .write_all(b"POST /AuthZPlugin.AuthZReq HTTP/1.1\r\nContent-Length: 100\r\n\r\n{\"Req");
    }
    let (status, v) = roundtrip(&p, "POST", "/Plugin.Activate", b"");
    assert_eq!(status, 200);
    assert!(v["Implements"].is_array(), "{v}");
}

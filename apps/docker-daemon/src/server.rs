//! Docker plugin protocol over a unix socket.

use crate::policy;
use serde_json::{json, Value};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

const CONTENT_TYPE: &str = "application/vnd.docker.plugins.v1.2+json";
const HEAD_CAP: usize = 16 * 1024;
const BODY_CAP: usize = 8 * 1024 * 1024;
const READ_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_CONNECTIONS: usize = 64;

fn body(v: Value) -> Vec<u8> {
    serde_json::to_vec(&v).expect("a Value serializes")
}

fn reply(status: u16, v: Value) -> (u16, Vec<u8>) {
    (status, body(v))
}

/// Plugin protocol router: returns (status, json body bytes).
/// POST /Plugin.Activate -> 200 {"Implements":["authz"]}
/// POST /AuthZPlugin.AuthZReq -> 200 {"Allow":bool,"Msg":string} (Msg only when refused)
/// POST /AuthZPlugin.AuthZRes -> 200 {"Allow":true}
/// anything else -> 404 {"Err":"..."}
pub fn handle(method: &str, path: &str, body: &[u8]) -> (u16, Vec<u8>) {
    match (method, path) {
        ("POST", "/Plugin.Activate") => reply(200, json!({"Implements": ["authz"]})),
        ("POST", "/AuthZPlugin.AuthZReq") => {
            let (d, req) = policy::authorize_logged(body);
            let (m, p) = req.unwrap_or_else(|| ("-".into(), "-".into()));
            if d.allow {
                eprintln!(
                    "curie-authz allow {} {}",
                    m.escape_debug(),
                    p.escape_debug()
                );
                reply(200, json!({"Allow": true}))
            } else {
                eprintln!(
                    "curie-authz refuse {} {} rule={}",
                    m.escape_debug(),
                    p.escape_debug(),
                    d.rule
                );
                reply(200, json!({"Allow": false, "Msg": d.msg}))
            }
        }
        ("POST", "/AuthZPlugin.AuthZRes") => reply(200, json!({"Allow": true})),
        _ => reply(
            404,
            json!({"Err": format!("no handler for {method} {path}")}),
        ),
    }
}

/// Serve HTTP/1.1 keep-alive on an accepted listener, one thread per
/// connection. Requires Content-Length; refuses chunked (400 + close).
/// Request head cap 16 KiB, body cap 8 MiB, read timeout 30 s, at most 64
/// open connections (a connection over the cap is closed at once; dockerd
/// treats the failed plugin call as a denial).
pub fn serve(listener: UnixListener) -> io::Result<()> {
    let open = Arc::new(AtomicUsize::new(0));
    for conn in listener.incoming() {
        match conn {
            Ok(stream) => {
                let before = open.fetch_add(1, Ordering::SeqCst);
                let slot = Slot(Arc::clone(&open));
                if before >= MAX_CONNECTIONS {
                    continue; // drops the stream and the slot
                }
                // A failed spawn drops the closure (stream and slot with it);
                // dockerd then fails closed.
                let _ = thread::Builder::new().spawn(move || {
                    let _slot = slot;
                    let _ = connection(stream);
                });
            }
            Err(e) => {
                eprintln!("curie-authz accept failed: {e}");
                thread::sleep(Duration::from_millis(100));
            }
        }
    }
    Ok(())
}

/// One counted connection; released on drop, including on panic.
struct Slot(Arc<AtomicUsize>);

impl Drop for Slot {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

struct Request {
    method: String,
    path: String,
    body: Vec<u8>,
    close: bool,
}

enum ReadError {
    Io,
    Bad(u16, &'static str),
}

impl From<io::Error> for ReadError {
    fn from(_: io::Error) -> Self {
        ReadError::Io
    }
}

fn connection(stream: UnixStream) -> io::Result<()> {
    stream.set_read_timeout(Some(READ_TIMEOUT))?;
    stream.set_write_timeout(Some(READ_TIMEOUT))?;
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut writer = stream;
    loop {
        match read_request(&mut reader) {
            Ok(None) | Err(ReadError::Io) => return Ok(()),
            Err(ReadError::Bad(status, why)) => {
                let body = body(json!({ "Err": why }));
                return respond(&mut writer, status, &body, true);
            }
            Ok(Some(req)) => {
                let (status, body) = handle(&req.method, &req.path, &req.body);
                respond(&mut writer, status, &body, req.close)?;
                if req.close {
                    return Ok(());
                }
            }
        }
    }
}

/// Read one request; `Ok(None)` is a clean close between requests.
fn read_request(reader: &mut impl BufRead) -> Result<Option<Request>, ReadError> {
    let mut head = Vec::new();
    loop {
        let mut line = Vec::new();
        let room = HEAD_CAP.saturating_sub(head.len()) as u64;
        Read::take(&mut *reader, room).read_until(b'\n', &mut line)?;
        if !line.ends_with(b"\n") {
            if head.len() + line.len() >= HEAD_CAP {
                return Err(ReadError::Bad(400, "request head too large"));
            }
            return if head.is_empty() && line.is_empty() {
                Ok(None)
            } else {
                Err(ReadError::Io)
            };
        }
        let blank = line == b"\r\n" || line == b"\n";
        if blank && head.is_empty() {
            continue;
        }
        head.extend_from_slice(&line);
        if blank {
            break;
        }
    }
    let bad = |why| ReadError::Bad(400, why);
    let head = std::str::from_utf8(&head).map_err(|_| bad("request head is not UTF-8"))?;
    let mut lines = head.lines();
    let mut start = lines.next().unwrap_or("").split(' ');
    let (method, target, version) = match (start.next(), start.next(), start.next(), start.next()) {
        (Some(m), Some(t), Some(v), None) if !m.is_empty() && v.starts_with("HTTP/1.") => (m, t, v),
        _ => return Err(bad("malformed request line")),
    };
    let mut close = version != "HTTP/1.1";
    let mut length: Option<usize> = None;
    for line in lines.filter(|l| !l.is_empty()) {
        let (name, value) = line.split_once(':').ok_or(bad("malformed header"))?;
        let (name, value) = (name.trim(), value.trim());
        if name.eq_ignore_ascii_case("transfer-encoding") {
            return Err(bad(
                "Transfer-Encoding is not supported; send Content-Length",
            ));
        } else if name.eq_ignore_ascii_case("content-length") {
            let n = value.parse().map_err(|_| bad("bad Content-Length"))?;
            if length.is_some_and(|l| l != n) {
                return Err(bad("conflicting Content-Length"));
            }
            length = Some(n);
        } else if name.eq_ignore_ascii_case("connection") {
            close = value.to_ascii_lowercase().contains("close");
        }
    }
    // No Content-Length and no Transfer-Encoding means an empty body.
    let length = length.unwrap_or(0);
    if length > BODY_CAP {
        return Err(ReadError::Bad(413, "request body too large"));
    }
    let mut body = vec![0; length];
    reader.read_exact(&mut body)?;
    Ok(Some(Request {
        method: method.to_owned(),
        path: policy::path_of(target).to_owned(),
        body,
        close,
    }))
}

fn respond(w: &mut impl Write, status: u16, body: &[u8], close: bool) -> io::Result<()> {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        413 => "Payload Too Large",
        _ => "Error",
    };
    let mut out = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {CONTENT_TYPE}\r\nContent-Length: {}\r\n",
        body.len()
    )
    .into_bytes();
    if close {
        out.extend_from_slice(b"Connection: close\r\n");
    }
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(body);
    w.write_all(&out)?;
    w.flush()
}

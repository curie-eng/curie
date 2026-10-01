//! A stub OCI distribution registry for the no-docker digest tests (#3503).
//!
//! It speaks the slice of the distribution API a native digest lookup needs:
//! `GET /v2/<repo>/manifests/<reference>` behind an anonymous bearer challenge
//! (`WWW-Authenticate: Bearer realm=..,service=..,scope=..`) and a token
//! endpoint. `support::serve` cannot send the challenge header, so this server
//! writes its own responses. Every request is recorded, so a test can prove
//! the token flow happened rather than infer it from the exit status.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

use sha2::{Digest, Sha256};

/// The anonymous token the stub issues and then requires on manifest reads.
pub const TOKEN: &str = "stub-token";
/// The `service` the challenge names.
pub const SERVICE: &str = "stub-registry";
/// The media type the runner index is served as.
pub const INDEX_MEDIA_TYPE: &str = "application/vnd.oci.image.index.v1+json";
/// The platform runner repository and tag the deploy tests install.
pub const RUNNER_REPO: &str = "curie-eng/curie-runner";
pub const RUNNER_TAG: &str = "0.11.0";

/// A fixed OCI image index, shaped like a real multi-platform runner. Its
/// exact bytes are what the digest is computed over.
pub fn runner_index() -> Vec<u8> {
    concat!(
        r#"{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","#,
        r#""manifests":[{"mediaType":"application/vnd.oci.image.manifest.v1+json","#,
        r#""digest":"sha256:1111111111111111111111111111111111111111111111111111111111111111","#,
        r#""size":1234,"platform":{"architecture":"amd64","os":"linux"}},"#,
        r#"{"mediaType":"application/vnd.oci.image.manifest.v1+json","#,
        r#""digest":"sha256:2222222222222222222222222222222222222222222222222222222222222222","#,
        r#""size":1234,"platform":{"architecture":"arm64","os":"linux"}}]}"#
    )
    .as_bytes()
    .to_vec()
}

/// `sha256:<hex>` of `bytes`: the digest a registry reports for a manifest.
pub fn sha256_digest(bytes: &[u8]) -> String {
    let hex: String = Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    format!("sha256:{hex}")
}

#[derive(Debug, Clone)]
pub struct RegistryRequest {
    pub method: String,
    /// The request target, query string included.
    pub path: String,
    pub authorization: Option<String>,
    pub accept: Option<String>,
}

#[derive(Clone)]
struct Served {
    content_type: String,
    body: Vec<u8>,
}

pub struct OciRegistryStub {
    /// `127.0.0.1:<port>`, the registry host an image reference names.
    pub host: String,
    requests: Arc<Mutex<Vec<RegistryRequest>>>,
}

impl OciRegistryStub {
    /// Serves the runner index at `RUNNER_REPO:RUNNER_TAG` and nothing else.
    pub fn runner() -> Self {
        Self::start(vec![(
            RUNNER_REPO.to_string(),
            RUNNER_TAG.to_string(),
            INDEX_MEDIA_TYPE.to_string(),
            runner_index(),
        )])
    }

    /// Serves each `(repository, reference, content type, body)`. A reference
    /// may be a tag or a digest; the stub never checks that a digest matches
    /// the body it serves, so a test can make it lie.
    pub fn start(manifests: Vec<(String, String, String, Vec<u8>)>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind registry stub");
        let host = listener.local_addr().expect("registry addr").to_string();
        let served: BTreeMap<String, Served> = manifests
            .into_iter()
            .map(|(repo, reference, content_type, body)| {
                (
                    format!("/v2/{repo}/manifests/{reference}"),
                    Served { content_type, body },
                )
            })
            .collect();
        let served = Arc::new(served);
        let requests: Arc<Mutex<Vec<RegistryRequest>>> = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&requests);
        let realm_host = host.clone();
        thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { break };
                let served = Arc::clone(&served);
                let recorded = Arc::clone(&recorded);
                let realm_host = realm_host.clone();
                thread::spawn(move || handle(stream, &served, &recorded, &realm_host));
            }
        });
        Self { host, requests }
    }

    /// `<host>/<repository>`, the image a helm value or lock names.
    pub fn image(&self, repository: &str) -> String {
        format!("{}/{repository}", self.host)
    }

    pub fn recorded(&self) -> Vec<RegistryRequest> {
        self.requests.lock().unwrap().clone()
    }

    /// Whether the token endpoint was asked for a pull scope on `repository`.
    pub fn saw_token_request(&self, repository: &str) -> bool {
        self.recorded().iter().any(|r| {
            // The client may percent-encode the query; compare decoded.
            let query = r
                .path
                .replace("%3A", ":")
                .replace("%3a", ":")
                .replace("%2F", "/")
                .replace("%2f", "/");
            query.starts_with("/token?")
                && query.contains(&format!("service={SERVICE}"))
                && query.contains(&format!("scope=repository:{repository}:pull"))
        })
    }

    /// Whether a manifest read of `repository:reference` carried the token.
    pub fn saw_authorized_manifest_get(&self, repository: &str, reference: &str) -> bool {
        let path = format!("/v2/{repository}/manifests/{reference}");
        let bearer = format!("Bearer {TOKEN}");
        self.recorded().iter().any(|r| {
            r.method == "GET" && r.path == path && r.authorization.as_deref() == Some(&bearer)
        })
    }
}

fn handle(
    stream: TcpStream,
    served: &BTreeMap<String, Served>,
    recorded: &Mutex<Vec<RegistryRequest>>,
    realm_host: &str,
) {
    let mut reader = BufReader::new(stream);
    let mut request_line = String::new();
    if reader.read_line(&mut request_line).unwrap_or(0) == 0 {
        return;
    }
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let path = parts.next().unwrap_or("").to_string();
    let mut authorization = None;
    let mut accept = None;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).unwrap_or(0) == 0 {
            break;
        }
        let line = line.trim_end();
        if line.is_empty() {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            if name.trim().eq_ignore_ascii_case("authorization") {
                authorization = Some(value.trim().to_string());
            } else if name.trim().eq_ignore_ascii_case("accept") {
                accept = Some(value.trim().to_string());
            }
        }
    }
    recorded.lock().unwrap().push(RegistryRequest {
        method: method.clone(),
        path: path.clone(),
        authorization: authorization.clone(),
        accept,
    });

    let authorized = authorization.as_deref() == Some(&format!("Bearer {TOKEN}"));
    let (status, headers, body): (&str, Vec<String>, Vec<u8>) = if path.starts_with("/token?")
        || path == "/token"
    {
        (
            "200 OK",
            vec!["Content-Type: application/json".into()],
            format!(r#"{{"token":"{TOKEN}"}}"#).into_bytes(),
        )
    } else if path.starts_with("/v2/") && path.contains("/manifests/") {
        let repository = path
            .trim_start_matches("/v2/")
            .split("/manifests/")
            .next()
            .unwrap_or("");
        if !authorized {
            (
                "401 Unauthorized",
                vec![
                    "Content-Type: application/json".into(),
                    format!(
                        r#"WWW-Authenticate: Bearer realm="http://{realm_host}/token",service="{SERVICE}",scope="repository:{repository}:pull""#
                    ),
                ],
                br#"{"errors":[{"code":"UNAUTHORIZED","message":"authentication required"}]}"#
                    .to_vec(),
            )
        } else if let Some(entry) = served.get(&path) {
            (
                "200 OK",
                vec![
                    format!("Content-Type: {}", entry.content_type),
                    format!("Docker-Content-Digest: {}", sha256_digest(&entry.body)),
                ],
                entry.body.clone(),
            )
        } else {
            not_found()
        }
    } else {
        not_found()
    };

    let body = if method == "HEAD" { Vec::new() } else { body };
    let mut head = format!("HTTP/1.1 {status}\r\n");
    for header in headers {
        head.push_str(&header);
        head.push_str("\r\n");
    }
    head.push_str(&format!(
        "Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    ));
    let stream = reader.get_mut();
    let _ = stream.write_all(head.as_bytes());
    let _ = stream.write_all(&body);
    let _ = stream.flush();
}

fn not_found() -> (&'static str, Vec<String>, Vec<u8>) {
    (
        "404 Not Found",
        vec!["Content-Type: application/json".into()],
        br#"{"errors":[{"code":"MANIFEST_UNKNOWN","message":"manifest unknown"}]}"#.to_vec(),
    )
}

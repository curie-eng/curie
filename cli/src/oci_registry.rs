//! A native OCI distribution client: resolve an image reference to the digest
//! of the manifest its registry serves (#3503).
//!
//! `cluster deploy` pins the installation's runner to a registry digest, and
//! the connector preflight reads a pushed image's manifest. Both used to ask
//! `docker buildx imagetools`, which an operator host with only kubectl and
//! helm (a k3s node, say) does not have. Resolving a digest is one HTTPS read
//! plus an anonymous bearer token, so it must not need a local daemon. The
//! docker path stays as the callers' fallback for a registry that needs a
//! docker login.

#![deny(clippy::let_underscore_must_use, clippy::let_underscore_untyped)]

use std::net::IpAddr;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use sha2::{Digest, Sha256};

/// The registry `docker` assumes when a reference names none.
const DOCKER_HUB: &str = "docker.io";
/// The host Docker Hub's registry API answers on.
const DOCKER_HUB_API: &str = "registry-1.docker.io";
/// A manifest or index is a few KiB; anything past this is not one.
const MAX_MANIFEST_BYTES: usize = 4 * 1024 * 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// reqwest's default redirect limit, kept under the custom policy.
const MAX_REDIRECTS: usize = 10;
/// Every manifest shape a runner or connector image is pushed as. An index is
/// listed first so a multi-platform image resolves to its top-level digest,
/// the one `docker buildx imagetools inspect` reports.
const MANIFEST_ACCEPT: &str = "application/vnd.oci.image.index.v1+json, \
                               application/vnd.docker.distribution.manifest.list.v2+json, \
                               application/vnd.oci.image.manifest.v1+json, \
                               application/vnd.docker.distribution.manifest.v2+json";

/// An image reference normalized the way `docker` reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageRef {
    /// `docker.io`, `ghcr.io`, `localhost:5000`, ...
    pub registry: String,
    /// The repository path, `library/` prefixed for a Docker Hub official image.
    pub repository: String,
    /// A tag, or a `sha256:<hex>` digest when the reference carries one.
    pub reference: String,
}

impl ImageRef {
    /// The host the registry API is dialed at.
    fn api_host(&self) -> &str {
        if self.registry == DOCKER_HUB {
            DOCKER_HUB_API
        } else {
            &self.registry
        }
    }

    /// `http` for a loopback registry, docker's default insecure-registry
    /// rule; `https` everywhere else.
    fn scheme(&self) -> &'static str {
        let host = if self.registry.starts_with('[') {
            self.registry
                .split_once(']')
                .map_or(self.registry.as_str(), |(host, _)| host)
        } else {
            self.registry
                .split_once(':')
                .map_or(self.registry.as_str(), |(host, _)| host)
        };
        if is_loopback_host(host) {
            "http"
        } else {
            "https"
        }
    }

    fn is_digest(&self) -> bool {
        self.reference.starts_with("sha256:")
    }
}

/// Whether `host` (a bare host, IPv6 optionally bracketed, no port) is
/// loopback: `localhost` or a loopback IP. Decided structurally, so a DNS name
/// such as `127.registry.example.com` is not loopback.
fn is_loopback_host(host: &str) -> bool {
    let host = host
        .strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .or_else(|| host.strip_prefix('['))
        .unwrap_or(host);
    host.eq_ignore_ascii_case("localhost")
        || host.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback())
}

/// The one transport rule for every URL the client contacts: `https` always,
/// `http` only to a loopback host. A downgraded fetch could substitute an
/// older index whose digest still matches the bundle lock, so neither a token
/// realm nor a redirect may leave HTTPS for a remote host.
fn transport_allowed(url: &reqwest::Url) -> bool {
    match url.scheme() {
        "https" => true,
        "http" => url.host_str().is_some_and(is_loopback_host),
        _ => false,
    }
}

/// Parse `image` with docker's normalization: the first path component is a
/// registry only when it has a `.` or a `:` or is `localhost`; otherwise the
/// image is on Docker Hub, and a single-component name is a `library/` image.
/// A digest wins over a tag, and no reference means `latest`.
pub fn parse(image: &str) -> Result<ImageRef> {
    let trimmed = image.trim();
    if trimmed.is_empty() {
        bail!("an empty image reference names no image");
    }
    let (name, digest) = match trimmed.split_once('@') {
        Some((name, digest)) => (name, Some(digest)),
        None => (trimmed, None),
    };
    // A tag is a `:` after the last `/`; a `:` before it is a registry port.
    let last_slash = name.rfind('/').map_or(0, |i| i + 1);
    let (name, tag) = match name[last_slash..].rfind(':') {
        Some(colon) => (
            &name[..last_slash + colon],
            Some(&name[last_slash + colon + 1..]),
        ),
        None => (name, None),
    };
    let (registry, repository) = match name.split_once('/') {
        Some((first, rest))
            if first.contains('.') || first.contains(':') || first == "localhost" =>
        {
            let registry = if first == "index.docker.io" {
                DOCKER_HUB
            } else {
                first
            };
            (registry.to_string(), rest.to_string())
        }
        Some(_) => (DOCKER_HUB.to_string(), name.to_string()),
        None => (DOCKER_HUB.to_string(), format!("library/{name}")),
    };
    let repository = if registry == DOCKER_HUB && !repository.contains('/') {
        format!("library/{repository}")
    } else {
        repository
    };
    let reference = match (digest, tag) {
        (Some(digest), _) => {
            let hex = digest.strip_prefix("sha256:").ok_or_else(|| {
                anyhow!("{image} carries a digest that is not sha256, which is not supported")
            })?;
            if hex.len() != 64 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
                bail!("{image} carries a malformed sha256 digest");
            }
            format!("sha256:{}", hex.to_ascii_lowercase())
        }
        (None, Some(tag)) => tag.to_string(),
        (None, None) => "latest".to_string(),
    };
    if repository.is_empty() || repository.ends_with('/') || reference.is_empty() {
        bail!("{image} is not a valid image reference");
    }
    Ok(ImageRef {
        registry,
        repository,
        reference,
    })
}

/// A manifest as the registry served it.
#[derive(Debug, Clone)]
pub struct Manifest {
    /// `sha256:<hex>` of `raw`: the digest the registry addresses it by.
    pub digest: String,
    /// The exact bytes served.
    pub raw: Vec<u8>,
}

/// Fetch the manifest `image` names, answering an anonymous bearer challenge
/// once if the registry sends one. The digest is computed over the served
/// bytes, and a by-digest fetch whose bytes do not hash to it fails.
pub async fn fetch_manifest(image: &str) -> Result<Manifest> {
    let parsed = parse(image)?;
    let client = reqwest::Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .redirect(reqwest::redirect::Policy::custom(|attempt| {
            if attempt.previous().len() >= MAX_REDIRECTS {
                attempt.error("too many redirects")
            } else if !transport_allowed(attempt.url()) {
                attempt.error("refusing a redirect off HTTPS to a non-loopback host")
            } else {
                attempt.follow()
            }
        }))
        .build()
        .context("building the registry client")?;
    let url = format!(
        "{}://{}/v2/{}/manifests/{}",
        parsed.scheme(),
        parsed.api_host(),
        parsed.repository,
        parsed.reference
    );
    let get = |token: Option<&str>| {
        let mut request = client
            .get(&url)
            .header(reqwest::header::ACCEPT, MANIFEST_ACCEPT);
        if let Some(token) = token {
            request = request.bearer_auth(token);
        }
        request.send()
    };
    let unreachable = || format!("could not reach the registry for {image}");

    let mut response = get(None).await.with_context(unreachable)?;
    if response.status() == reqwest::StatusCode::UNAUTHORIZED {
        let challenge = response
            .headers()
            .get(reqwest::header::WWW_AUTHENTICATE)
            .and_then(|v| v.to_str().ok())
            .and_then(bearer_challenge)
            .ok_or_else(|| {
                anyhow!(
                    "the registry for {image} requires credentials (HTTP 401) and offers no \
                     anonymous bearer token"
                )
            })?;
        let token = anonymous_token(&client, &challenge, &parsed, image).await?;
        response = get(Some(&token)).await.with_context(unreachable)?;
    }
    let status = response.status();
    if !status.is_success() {
        bail!("the registry could not serve the manifest of {image}: HTTP {status}");
    }
    let raw = read_capped(response, image).await?;
    let digest = sha256_digest(&raw);
    if parsed.is_digest() && digest != parsed.reference {
        bail!(
            "the registry served bytes for {image} that hash to {digest}, not the requested \
             digest"
        );
    }
    Ok(Manifest { digest, raw })
}

/// The parameters of a `WWW-Authenticate: Bearer ...` challenge.
#[derive(Debug, Default, PartialEq, Eq)]
struct BearerChallenge {
    realm: String,
    service: Option<String>,
    scope: Option<String>,
}

/// Parse `Bearer realm="..",service="..",scope=".."`. Values may be quoted
/// (and then may hold commas, as a multi-action scope does) or bare.
fn bearer_challenge(header: &str) -> Option<BearerChallenge> {
    let header = header.trim();
    let (scheme, mut rest) = header.split_once(char::is_whitespace)?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let mut challenge = BearerChallenge::default();
    loop {
        rest = rest.trim_start_matches(|c: char| c == ',' || c.is_whitespace());
        if rest.is_empty() {
            break;
        }
        let (key, after) = rest.split_once('=')?;
        let (value, remaining) = if let Some(quoted) = after.strip_prefix('"') {
            let end = quoted.find('"')?;
            (&quoted[..end], &quoted[end + 1..])
        } else {
            let end = after.find(',').unwrap_or(after.len());
            (after[..end].trim(), &after[end..])
        };
        match key.trim().to_ascii_lowercase().as_str() {
            "realm" => challenge.realm = value.to_string(),
            "service" => challenge.service = Some(value.to_string()),
            "scope" => challenge.scope = Some(value.to_string()),
            _ => {}
        }
        rest = remaining;
    }
    (!challenge.realm.is_empty()).then_some(challenge)
}

/// Ask the challenge's realm for an anonymous pull token.
async fn anonymous_token(
    client: &reqwest::Client,
    challenge: &BearerChallenge,
    parsed: &ImageRef,
    image: &str,
) -> Result<String> {
    let scope = challenge
        .scope
        .clone()
        .unwrap_or_else(|| format!("repository:{}:pull", parsed.repository));
    let mut query = vec![("scope", scope)];
    if let Some(service) = &challenge.service {
        query.push(("service", service.clone()));
    }
    let realm = reqwest::Url::parse(&challenge.realm)
        .ok()
        .filter(transport_allowed)
        .ok_or_else(|| {
            anyhow!(
                "the registry for {image} named a token realm that is not an HTTPS URL (HTTP is \
                 allowed only on loopback)"
            )
        })?;
    let response = client
        .get(realm)
        .query(&query)
        .send()
        .await
        .with_context(|| format!("could not reach the token endpoint for {image}"))?;
    let status = response.status();
    if !status.is_success() {
        bail!("the registry for {image} refused an anonymous token: HTTP {status}");
    }
    let body: serde_json::Value = response
        .json()
        .await
        .with_context(|| format!("the token endpoint for {image} answered malformed JSON"))?;
    body.get("token")
        .or_else(|| body.get("access_token"))
        .and_then(|t| t.as_str())
        .filter(|t| !t.is_empty())
        .map(str::to_string)
        .ok_or_else(|| anyhow!("the token endpoint for {image} issued no token"))
}

/// The response body, refused once it passes `MAX_MANIFEST_BYTES`.
async fn read_capped(mut response: reqwest::Response, image: &str) -> Result<Vec<u8>> {
    let too_large = || anyhow!("the manifest of {image} is larger than {MAX_MANIFEST_BYTES} bytes");
    if response
        .content_length()
        .is_some_and(|len| len > MAX_MANIFEST_BYTES as u64)
    {
        return Err(too_large());
    }
    let mut raw = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .with_context(|| format!("reading the manifest of {image}"))?
    {
        if raw.len() + chunk.len() > MAX_MANIFEST_BYTES {
            return Err(too_large());
        }
        raw.extend_from_slice(&chunk);
    }
    Ok(raw)
}

fn sha256_digest(bytes: &[u8]) -> String {
    let hex: String = Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    format!("sha256:{hex}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_challenge_with_quoted_values_parses_including_a_comma_scope() {
        let challenge = bearer_challenge(
            r#"Bearer realm="https://ghcr.io/token",service="ghcr.io",scope="repository:a/b:pull,push""#,
        )
        .expect("a bearer challenge");
        assert_eq!(
            challenge,
            BearerChallenge {
                realm: "https://ghcr.io/token".into(),
                service: Some("ghcr.io".into()),
                scope: Some("repository:a/b:pull,push".into()),
            }
        );
    }

    #[test]
    fn a_basic_challenge_or_one_without_a_realm_is_not_bearer() {
        assert_eq!(bearer_challenge(r#"Basic realm="registry""#), None);
        assert_eq!(bearer_challenge(r#"Bearer service="x""#), None);
    }

    #[test]
    fn loopback_registries_are_dialed_over_http_and_docker_hub_at_its_api_host() {
        let scheme = |image: &str| parse(image).unwrap().scheme();
        assert_eq!(scheme("localhost:5000/a/b"), "http");
        assert_eq!(scheme("127.0.0.1:5000/a/b"), "http");
        assert_eq!(scheme("[::1]:5000/a/b"), "http");
        assert_eq!(scheme("ghcr.io/a/b"), "https");
        assert_eq!(parse("ubuntu").unwrap().api_host(), DOCKER_HUB_API);
        assert_eq!(parse("ghcr.io/a/b").unwrap().api_host(), "ghcr.io");
    }

    #[test]
    fn a_dns_name_starting_with_127_is_not_loopback() {
        let scheme = |image: &str| parse(image).unwrap().scheme();
        assert_eq!(scheme("127.registry.example.com/a/b"), "https");
        assert_eq!(scheme("LOCALHOST:5000/a/b"), "http");
        assert!(!is_loopback_host("127.registry.example.com"));
        assert!(is_loopback_host("[::1]"));
        assert!(is_loopback_host("127.8.9.10"));
    }

    #[test]
    fn the_transport_rule_allows_http_only_to_loopback() {
        let allowed = |url: &str| transport_allowed(&reqwest::Url::parse(url).unwrap());
        assert!(!allowed("http://registry.example.com/token"));
        assert!(!allowed("http://127.registry.example.com/token"));
        assert!(!allowed("ftp://registry.example.com/token"));
        assert!(allowed("https://registry.example.com/token"));
        assert!(allowed("http://127.0.0.1:1/token"));
        assert!(allowed("http://[::1]:1/token"));
        assert!(allowed("http://localhost:1/token"));
    }

    #[test]
    fn a_malformed_or_non_sha256_digest_is_refused() {
        assert!(parse("a/b@sha512:abc").is_err());
        assert!(parse("a/b@sha256:abc").is_err());
        assert!(parse("").is_err());
    }
}

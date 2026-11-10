//! Integration: `cluster deploy` transport + key-discovery contract (#705,
//! ADR-0057). When no `--api-url` is given, deploy self-plumbs a kubectl
//! port-forward loopback tunnel to `svc/<release>-api` and posts to it, instead
//! of sending the release's strong generated key over the cleartext UI /api
//! NodePort proxy (ADR-0024). When `--api-url` IS given, deploy direct-dials it
//! (no tunnel). When no key is given, deploy discovers the release Secret key
//! rather than defaulting to a dev placeholder; an explicit key wins.
//!
//! These tests pin two pure builders the implementer will add to
//! `cli/src/commands/deploy.rs` (imported here from the `curie` lib):
//!
//!   pub fn deploy_port_forward(
//!       api_url: Option<&str>,
//!       namespace: &str,
//!       fullname: &ops::ReleaseFullname,
//!       local_port: u16,
//!       remote_port: u16,
//!   ) -> Option<OpsCommand>
//!
//!   pub fn deploy_needs_key_discovery(explicit_api_key: Option<&str>) -> bool
//!
//! Until both exist this test target fails to compile: that is the intended RED,
//! isolated to this file because it imports from the lib rather than adding
//! inline lib tests.

use curie::api::is_insecure_endpoint;
use curie::commands::{deploy_needs_key_discovery, deploy_port_forward, normalize_deploy_api_key};

/// (1) Auto path (no `--api-url`) builds a kubectl port-forward to the api
/// service: a loopback tunnel is the whole point of Option C, so the discovered
/// strong key never travels over the cleartext NodePort proxy.
#[test]
fn auto_path_builds_port_forward_to_api_service() {
    let cmd = deploy_port_forward(
        None,
        "curie",
        &curie::ops::chart_fullname("curie"),
        18000,
        8000,
    )
    .expect("the auto path (no --api-url) must build a port-forward tunnel");

    assert_eq!(cmd.program, "kubectl");
    let argv = cmd.argv();
    assert!(
        argv.iter().any(|a| a == "port-forward"),
        "expected a port-forward subcommand, got argv {argv:?}"
    );
    assert!(
        argv.iter().any(|a| a == "svc/curie-api"),
        "expected the tunnel to target the api service, got argv {argv:?}"
    );
    assert!(
        argv.iter().any(|a| a == "18000:8000"),
        "expected the local:remote port mapping, got argv {argv:?}"
    );
}

/// (2) Explicit `--api-url` direct-dials: no tunnel is built, so deploy posts
/// straight to the operator-supplied URL.
#[test]
fn explicit_api_url_builds_no_port_forward() {
    let cmd = deploy_port_forward(
        Some("http://example:9000/api"),
        "curie",
        &curie::ops::chart_fullname("curie"),
        18000,
        8000,
    );
    assert!(
        cmd.is_none(),
        "an explicit --api-url must direct-dial with no port-forward, got {cmd:?}"
    );
}

/// (3) Security: the auto-discovered strong key must neither egress cleartext
/// nor ride the port-forward command line. Two real guards (the old assertion
/// was vacuous -- the key is not an input to `deploy_port_forward`, so it could
/// never appear regardless of the implementation):
///
///   (a) the classifier that GATES the cleartext refusal (`cluster deploy`
///       refuses an auto-discovered key over a non-loopback `http://` --api-url).
///       If a regression stopped flagging the leak case, this fails.
///   (b) the port-forward argv carries no credential-shaped flag, the property
///       the vacuous test only pretended to check.
#[test]
fn discovered_key_cleartext_refusal_and_no_credential_argv() {
    // (a) The refusal gate: FLAG a non-loopback cleartext endpoint (the leak
    // case), CLEAR the loopback tunnel path and any https:// endpoint.
    assert!(
        is_insecure_endpoint("http://lan-host:8000"),
        "a non-loopback http:// --api-url must classify as a cleartext key leak (refused)"
    );
    assert!(
        !is_insecure_endpoint("http://localhost:18000"),
        "the loopback port-forward path must be allowed"
    );
    assert!(
        !is_insecure_endpoint("https://api.example.com"),
        "an https:// endpoint encrypts the key and must be allowed"
    );

    // (b) The port-forward command line carries no credential-shaped argument.
    let cmd = deploy_port_forward(
        None,
        "curie",
        &curie::ops::chart_fullname("curie"),
        18000,
        8000,
    )
    .expect("the auto path must build a port-forward tunnel");
    for token in cmd.argv() {
        let lower = token.to_ascii_lowercase();
        assert!(
            !lower.contains("api-key") && !lower.contains("apikey") && !lower.contains("x-api"),
            "the port-forward argv must carry no credential-shaped argument, got {token:?}"
        );
    }
}

/// (4) Key-discovery precedence, both branches: no explicit key means discover
/// the release Secret key; an explicit key wins and skips discovery.
#[test]
fn key_discovery_precedence_both_branches() {
    assert!(
        deploy_needs_key_discovery(None),
        "no explicit key must trigger release Secret discovery"
    );
    assert!(
        !deploy_needs_key_discovery(Some("k")),
        "an explicit key must win and skip discovery"
    );
}

/// (5) An empty or whitespace-only `--api-key` normalizes to `None` so it
/// triggers discovery like an omitted flag instead of posting an empty key.
#[test]
fn normalize_deploy_api_key_blanks_empty_and_whitespace() {
    assert_eq!(normalize_deploy_api_key(Some(String::new())), None);
    assert_eq!(normalize_deploy_api_key(Some("  ".to_string())), None);
    assert_eq!(normalize_deploy_api_key(None), None);
    assert_eq!(
        normalize_deploy_api_key(Some("realkey".to_string())),
        Some("realkey".to_string())
    );
}

//! The native OCI distribution client `curie::oci_registry` (#3503).
//!
//! `cluster deploy` pins the installation's runner to a registry digest, and
//! that lookup must not need docker. These tests prove the reference parser
//! follows docker's normalization, and that `fetch_manifest` resolves a tag
//! through an anonymous bearer challenge to the sha256 of the exact bytes the
//! registry served, refuses bytes that do not hash to a requested digest, and
//! names the reference when the registry does not know it.

#[path = "support/oci_registry_stub.rs"]
mod oci_registry_stub;

use curie::oci_registry::{fetch_manifest, parse};
use oci_registry_stub::{
    runner_index, sha256_digest, OciRegistryStub, INDEX_MEDIA_TYPE, RUNNER_REPO, RUNNER_TAG,
};

fn parsed(image: &str) -> (String, String, String) {
    let reference = parse(image).unwrap_or_else(|err| panic!("{image} must parse: {err:#}"));
    (
        reference.registry,
        reference.repository,
        reference.reference,
    )
}

fn owned(registry: &str, repository: &str, reference: &str) -> (String, String, String) {
    (registry.into(), repository.into(), reference.into())
}

#[test]
fn a_bare_name_is_a_docker_hub_library_image_at_latest() {
    assert_eq!(
        parsed("ubuntu"),
        owned("docker.io", "library/ubuntu", "latest")
    );
}

#[test]
fn a_first_component_with_a_dot_is_the_registry() {
    assert_eq!(
        parsed("ghcr.io/curie-eng/curie-runner:0.11.0"),
        owned("ghcr.io", "curie-eng/curie-runner", "0.11.0")
    );
}

#[test]
fn localhost_with_a_port_is_the_registry_and_no_tag_means_latest() {
    assert_eq!(
        parsed("localhost:5000/a/b"),
        owned("localhost:5000", "a/b", "latest")
    );
}

#[test]
fn a_digest_is_the_reference() {
    let digest = format!("sha256:{}", "c".repeat(64));
    assert_eq!(
        parsed(&format!("127.0.0.1:5000/a/b@{digest}")),
        owned("127.0.0.1:5000", "a/b", &digest)
    );
}

#[test]
fn a_first_component_without_a_dot_or_port_is_a_docker_hub_namespace() {
    assert_eq!(parsed("acme/bot:1"), owned("docker.io", "acme/bot", "1"));
}

#[test]
fn a_digest_wins_over_a_tag() {
    let digest = format!("sha256:{}", "c".repeat(64));
    assert_eq!(
        parsed(&format!("a/b:1@{digest}")),
        owned("docker.io", "a/b", &digest)
    );
}

/// A tag resolves through the bearer challenge to the sha256 of the served
/// bytes, and the bytes come back exactly as served.
#[tokio::test]
async fn a_tag_resolves_through_the_bearer_challenge_to_the_served_digest() {
    let registry = OciRegistryStub::runner();
    let image = format!("{}:{RUNNER_TAG}", registry.image(RUNNER_REPO));

    let manifest = fetch_manifest(&image)
        .await
        .unwrap_or_else(|err| panic!("{image} must resolve: {err:#}"));

    assert_eq!(manifest.digest, sha256_digest(&runner_index()));
    assert_eq!(manifest.raw, runner_index(), "raw must be the served bytes");
    assert!(
        registry.saw_token_request(RUNNER_REPO),
        "the 401 challenge must be answered with an anonymous token: {:?}",
        registry.recorded()
    );
    assert!(
        registry.saw_authorized_manifest_get(RUNNER_REPO, RUNNER_TAG),
        "the retry must carry the token: {:?}",
        registry.recorded()
    );
}

/// A fetch by digest whose served bytes hash to something else is refused:
/// the digest is the identity, and bytes that do not match it are not it.
#[tokio::test]
async fn a_digest_fetch_whose_bytes_do_not_hash_to_it_fails() {
    let claimed = format!("sha256:{}", "d".repeat(64));
    let registry = OciRegistryStub::start(vec![(
        RUNNER_REPO.to_string(),
        claimed.clone(),
        INDEX_MEDIA_TYPE.to_string(),
        runner_index(),
    )]);
    let image = format!("{}@{claimed}", registry.image(RUNNER_REPO));

    let result = fetch_manifest(&image).await;

    assert!(
        result.is_err(),
        "bytes hashing to {} must not be accepted as {claimed}",
        sha256_digest(&runner_index())
    );
}

/// A reference the registry does not know fails, naming the reference so the
/// operator knows which lookup went wrong.
#[tokio::test]
async fn an_unknown_reference_fails_naming_it() {
    let registry = OciRegistryStub::runner();
    let image = format!("{}:9.9.9", registry.image(RUNNER_REPO));

    let err = fetch_manifest(&image)
        .await
        .err()
        .unwrap_or_else(|| panic!("{image} is not served and must fail"));

    let message = format!("{err:#}");
    assert!(
        message.contains(&image) || message.contains("9.9.9"),
        "the error must name the reference: {message}"
    );
}

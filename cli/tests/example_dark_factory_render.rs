//! Binary contract for `curie example dark-factory render` and its runner lock
//! (#3747).
//!
//! A release build locks the runner layer the release published for its own
//! version, so `cluster deploy` of the rendered bundle needs no build step. A
//! build with no published layer still renders, and says which `curie build`
//! replaces it, instead of leaving the refusal to the deploy.

mod support;

use std::path::Path;
use std::process::{Command, Output};

use serde_json::Value;
use sha2::{Digest, Sha256};
use support::{serve, MockServer, Response};

const VERSION: &str = env!("CARGO_PKG_VERSION");
const LAYER_INDEX: &str = r#"{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[{"layer":true}]}"#;
const BASE_INDEX: &str = r#"{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[{"base":true}]}"#;

fn sha256(body: &str) -> String {
    let hex: String = Sha256::digest(body.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    format!("sha256:{hex}")
}

fn index(body: &'static str) -> Response {
    Response {
        status: 200,
        content_type: "application/vnd.oci.image.index.v1+json".into(),
        body: body.as_bytes().to_vec(),
    }
}

/// A registry that publishes the platform runner at this CLI's version and,
/// when `layer_published`, the dark factory runner layer too.
fn registry(layer_published: bool) -> MockServer {
    serve(move |request| {
        if request.path.starts_with("/token?") {
            return Response::json(200, r#"{"token":"anonymous-pull-token"}"#);
        }
        if request.path == format!("/v2/curie-eng/curie-runner/manifests/{VERSION}") {
            return index(BASE_INDEX);
        }
        if layer_published
            && request.path
                == format!("/v2/curie-eng/curie-dark-factory-runner/manifests/{VERSION}")
        {
            return index(LAYER_INDEX);
        }
        Response::json(404, r#"{"errors":[{"code":"MANIFEST_UNKNOWN"}]}"#)
    })
}

fn render(out: &Path, channel: Option<&str>, registry: Option<&MockServer>) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_curie"));
    command
        .args(["--json", "example", "dark-factory", "render", "--out"])
        .arg(out)
        .env_remove("CURIE_TEST_ARTIFACT_CHANNEL");
    if let Some(channel) = channel {
        command.env("CURIE_TEST_ARTIFACT_CHANNEL", channel);
    }
    if let Some(registry) = registry {
        command.env("CURIE_TEST_SRE_BOT_REGISTRY_ENDPOINT", &registry.base_url);
    }
    command
        .output()
        .expect("run curie example dark-factory render")
}

fn render_schema_validator() -> jsonschema::Validator {
    let schema: Value =
        serde_json::from_str(include_str!("../schema/dark-factory-render.schema.json"))
            .expect("committed render schema parses");
    jsonschema::validator_for(&schema).expect("committed render schema compiles")
}

fn receipt(output: &Output) -> Value {
    assert!(
        output.status.success(),
        "render failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let receipt: Value = serde_json::from_slice(&output.stdout).expect("JSON render receipt");
    let validator = render_schema_validator();
    if let Err(error) = validator.validate(&receipt) {
        panic!("actual CLI render receipt violates its committed schema: {error}; {receipt}");
    }
    receipt
}

#[test]
fn the_render_receipt_schema_rejects_an_unknown_field() {
    let tmp = tempfile::tempdir().unwrap();
    let out = tmp.path().join("factory");
    let mut receipt = receipt(&render(&out, None, None));
    receipt["unknown_field"] = Value::Bool(true);

    assert!(
        render_schema_validator().validate(&receipt).is_err(),
        "the render schema must reject undeclared receipt fields"
    );
}

#[test]
fn the_render_receipt_schema_rejects_malformed_nullable_fields() {
    let tmp = tempfile::tempdir().unwrap();
    let out = tmp.path().join("factory");
    let receipt = receipt(&render(&out, None, None));
    let validator = render_schema_validator();

    for field in ["runner_image", "runner_note"] {
        let mut malformed = receipt.clone();
        malformed[field] = serde_json::json!({"invalid": true});
        assert!(
            validator.validate(&malformed).is_err(),
            "{field} must accept only a string or null"
        );
    }
}

#[test]
fn a_release_render_locks_the_published_layer_for_its_version() {
    let registry = registry(true);
    let tmp = tempfile::tempdir().unwrap();
    let out = tmp.path().join("factory");
    let receipt = receipt(&render(&out, Some("release"), Some(&registry)));

    let image = format!(
        "ghcr.io/curie-eng/curie-dark-factory-runner@{}",
        sha256(LAYER_INDEX)
    );
    assert_eq!(receipt["runner_image"], Value::String(image.clone()));
    assert_eq!(receipt["runner_note"], Value::Null);

    let lock = curie::connector_build::load_lock(&out)
        .expect("the written lock parses under the deploy's own reader")
        .expect("render wrote connectors.lock.yaml");
    let runner = lock.runner.expect("the lock records the runner layer");
    assert_eq!(runner.image, image);
    assert_eq!(
        runner.base,
        format!("ghcr.io/curie-eng/curie-runner@{}", sha256(BASE_INDEX))
    );
    assert_eq!(runner.delivery, curie::connector_build::Delivery::Registry);
    assert_eq!(runner.platforms, vec!["linux/amd64", "linux/arm64"]);
    assert!(lock.connectors.is_empty());

    // The deploy refuses a lock whose source_digest is not the tree's own, so
    // the entry must carry the digest of exactly what render wrote.
    let decl = curie::connector_build::load(&out).unwrap().runner.unwrap();
    let fresh = curie::connector_build::source_digest_of(&out, &decl.build).unwrap();
    assert_eq!(runner.source_digest, fresh);
    assert_eq!(
        curie::connector_build::locked_runner_image(&out).unwrap(),
        Some(image)
    );

    // Both tags were asked for at this CLI's version, never a mutable one.
    let paths: Vec<String> = registry.recorded().into_iter().map(|r| r.path).collect();
    for repository in ["curie-dark-factory-runner", "curie-runner"] {
        let wanted = format!("/v2/curie-eng/{repository}/manifests/{VERSION}");
        assert!(paths.contains(&wanted), "{wanted} not requested: {paths:?}");
    }
}

#[test]
fn a_release_with_no_published_layer_says_so_and_names_curie_build() {
    let registry = registry(false);
    let tmp = tempfile::tempdir().unwrap();
    let out = tmp.path().join("factory");
    let receipt = receipt(&render(&out, Some("release"), Some(&registry)));

    assert_eq!(receipt["runner_image"], Value::Null);
    let note = receipt["runner_note"].as_str().expect("a runner note");
    assert!(
        note.contains(&format!("curie {VERSION}")),
        "names the version: {note}"
    );
    assert!(
        note.contains("curie-dark-factory-runner"),
        "names the image: {note}"
    );
    assert!(
        note.contains(&format!("curie build --plugin-dir {}", out.display())),
        "points at curie build: {note}"
    );
    assert!(!out.join("connectors.lock.yaml").exists());
    assert!(
        out.join("runner.Dockerfile").is_file(),
        "the bundle still renders"
    );
}

#[test]
fn a_source_build_says_it_has_no_published_layer_without_asking_a_registry() {
    let registry = registry(true);
    let tmp = tempfile::tempdir().unwrap();
    let out = tmp.path().join("factory");
    let receipt = receipt(&render(&out, None, Some(&registry)));

    assert_eq!(receipt["runner_image"], Value::Null);
    let note = receipt["runner_note"].as_str().expect("a runner note");
    assert!(note.contains("source build"), "says why: {note}");
    assert!(
        note.contains(&format!("curie build --plugin-dir {}", out.display())),
        "points at curie build: {note}"
    );
    assert!(!out.join("connectors.lock.yaml").exists());
    assert!(
        registry.recorded().is_empty(),
        "a source build has no version to look up"
    );
}

#[test]
fn an_unreachable_registry_fails_the_render_rather_than_reading_as_unpublished() {
    let registry = serve(|_| Response::json(503, r#"{"error":"registry unavailable"}"#));
    let tmp = tempfile::tempdir().unwrap();
    let out = tmp.path().join("factory");
    let output = render(&out, Some("release"), Some(&registry));
    assert!(!output.status.success(), "an outage is not absence");
    assert!(!out.join("connectors.lock.yaml").exists());
}

#[test]
fn a_registry_build_over_the_published_lock_replaces_it() {
    // AC3: someone who edits the layer rebuilds it with `curie build
    // --registry`, and that lock replaces the published entry with no --force.
    let registry = registry(true);
    let tmp = tempfile::tempdir().unwrap();
    let out = tmp.path().join("factory");
    receipt(&render(&out, Some("release"), Some(&registry)));
    let published = curie::connector_build::load_lock(&out).unwrap().unwrap();
    let mut rebuilt = published.clone();
    let runner = rebuilt.runner.as_mut().unwrap();
    runner.image = format!("registry.example/factory-runner@{}", sha256("rebuilt"));
    runner.source_digest = sha256("edited");
    assert_eq!(
        curie::connector_build::lock_overwrite_refusal(Some(&published), &rebuilt, false),
        None
    );
}

/// AC5's render assert: the release publishes, per version and for both
/// platforms, the exact repository render asks for, built on the release's own
/// platform runner pinned by digest, and the GitHub Release waits for it.
#[test]
fn the_release_workflow_publishes_the_layer_render_locks() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../.github/workflows/release.yaml");
    let workflow: Value = serde_norway::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    let jobs = &workflow["jobs"];
    let repository = curie::examples::DARK_FACTORY_RUNNER_REPOSITORY
        .strip_prefix("ghcr.io/curie-eng/")
        .unwrap();

    let build = &jobs["dark-factory-runner-build"];
    assert_eq!(build["needs"], serde_json::json!(["merge"]));
    let platforms: Vec<&str> = build["strategy"]["matrix"]["include"]
        .as_array()
        .unwrap()
        .iter()
        .map(|leg| leg["platform"].as_str().unwrap())
        .collect();
    assert_eq!(platforms, ["linux/amd64", "linux/arm64"]);
    let steps = build["steps"].as_array().unwrap();
    let step = |name: &str| {
        steps
            .iter()
            .find(|step| step["name"] == name)
            .unwrap_or_else(|| panic!("no step {name:?}"))
    };
    let pin = step("Pin the platform runner by digest")["run"]
        .as_str()
        .unwrap();
    assert!(pin.contains("curie-runner:sha-${GITHUB_SHA}") && pin.contains(".digest"));
    let push = &step("Build and push by digest")["with"];
    assert_eq!(push["file"], "examples/dark-factory/runner.Dockerfile");
    assert_eq!(push["context"], "examples/dark-factory");
    assert_eq!(
        push["build-args"].as_str().unwrap().trim(),
        "CURIE_RUNNER_IMAGE=${{ steps.base.outputs.image }}"
    );
    assert!(push["tags"]
        .as_str()
        .unwrap()
        .ends_with(&format!("/{repository}")));

    let merge = &jobs["dark-factory-runner-merge"];
    assert_eq!(
        merge["needs"],
        serde_json::json!(["dark-factory-runner-build"])
    );
    let meta = merge["steps"]
        .as_array()
        .unwrap()
        .iter()
        .find(|step| step["name"] == "Image metadata")
        .unwrap();
    assert!(meta["with"]["images"]
        .as_str()
        .unwrap()
        .ends_with(&format!("/{repository}")));
    assert!(
        meta["with"]["tags"]
            .as_str()
            .unwrap()
            .contains("type=semver,pattern={{version}}"),
        "render looks the layer up by the bare release version"
    );
    let record = merge["steps"]
        .as_array()
        .unwrap()
        .iter()
        .find(|step| step["name"] == "Record the published digest")
        .expect("the merge records the digest it published");
    assert!(record["run"]
        .as_str()
        .unwrap()
        .contains("GITHUB_STEP_SUMMARY"));

    let release_needs = jobs["release"]["needs"].as_array().unwrap();
    assert!(release_needs.contains(&Value::String("dark-factory-runner-merge".into())));
}

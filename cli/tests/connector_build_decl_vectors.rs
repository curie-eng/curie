// The Rust half of the frozen connector declaration / lock / DNS seam (ADR 0113).
//
// `curie build --plugin-dir` reads a bundle's `connectors.yaml` and writes
// `connectors.lock.yaml` before the platform ever sees the bundle, and
// `curie skill up` / `curie local deploy` start the connector containers under
// a Docker network alias the runner independently derives on the Python side.
// So the CLI carries hand mirrors of shapes and derivations that
// `packages/plugin-format` owns, in a language that cannot import them.
//
// The usual gate does not cover this seam. `curie dev field-parity` compares a
// Rust struct against `packages/plugin-format/schema/plugin-format.schema.json`,
// and `schema_export.py` imports only from `.models`, so the committed schema
// carries no `Connector*` `$defs` at all -- the connector structs are declared
// in `cli/plugin-format-mirrors.json`'s `non_mirrors` array and the field
// comparison is a no-op for them today. This file is the seam instead: it reads
// the same six corpora the Python suite reads, so a change made in one
// language and not the other fails that language's suite.
//
//   tests/vectors/connector-build-decl.json      the `build:` declaration
//   tests/vectors/connector-lock.json            connectors.lock.yaml + apply_lock
//   tests/vectors/connector-fields.json          the exact field-name sets
//   tests/vectors/connector-service-dns.json     object_name / service_dns
//   tests/vectors/connector-source-digest.json   source_digest_of
//   tests/vectors/connector-derived-bearer.json  derived Authorization Bearer name
//
// Same mechanism as `tests/vectors/approval-action-ids.json` for the
// dispatcher-versus-CLI action ids.

use curie::connector_build;
use serde_json::Value;
use std::collections::BTreeSet;
use std::path::Path;
use tempfile::TempDir;

// ─── Loaders ─────────────────────────────────────────────────────────────────

fn vector_file(name: &str) -> Value {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../tests/vectors")
        .join(name);
    let body = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    serde_json::from_str(&body).unwrap_or_else(|error| panic!("parse {}: {error}", path.display()))
}

fn vectors(name: &str) -> Vec<Value> {
    vector_file(name)["vectors"]
        .as_array()
        .unwrap_or_else(|| panic!("{name} has no `vectors` array"))
        .clone()
}

fn name_of(vector: &Value) -> &str {
    vector["name"].as_str().expect("vector has a name")
}

/// Materialize a `{relative path: file}` map under a fresh directory.
///
/// A value is either a content string, or `{"content": ..., "executable": true}`
/// for a file written with the owner execute bit set. The mode is part of the
/// digest -- the build context tar carries it -- so a materializer that ignored
/// the object form would write the executable vectors non-executable and fail
/// against a corpus that is right.
fn materialize(root: &Path, tree: &Value) {
    for (rel, value) in tree.as_object().expect("tree is an object") {
        let path = root.join(rel);
        std::fs::create_dir_all(path.parent().expect("a parent")).expect("mkdir -p");
        let (content, executable) = match value {
            Value::String(content) => (content.as_str(), false),
            Value::Object(file) => (
                file["content"].as_str().expect("file content is a string"),
                file.get("executable")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            ),
            other => panic!("a tree value is a string or an object, got {other}"),
        };
        std::fs::write(&path, content).expect("write");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).expect("stat").permissions().mode() & 0o7777;
            let mode = if executable {
                mode | 0o100
            } else {
                mode & !0o111
            };
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode))
                .expect("chmod the materialized file");
        }
        #[cfg(not(unix))]
        let _ = executable;
    }
}

fn scratch() -> TempDir {
    TempDir::new().expect("a scratch directory")
}

// ─── The `build:` declaration ────────────────────────────────────────────────

#[test]
fn the_rust_reader_accepts_and_rejects_exactly_the_documents_python_does() {
    // Accept/reject parity is the whole point: `curie build` reads the bundle
    // BEFORE upload, so a document the CLI accepts and the platform validator
    // rejects means an operator builds and pushes an image for a bundle that
    // can never deploy, and a document the CLI rejects and the platform accepts
    // means a bundle that deploys through git flow but cannot be built locally.
    for vector in vectors("connector-build-decl.json") {
        if vector.get("fixture").is_some() {
            continue; // filesystem cases have their own test below
        }
        let document = serde_norway::to_string(&vector["document"]).expect("serialize document");
        let parsed = connector_build::parse_connectors(&document);
        match vector["expect"].as_str().expect("expect") {
            "accept" => {
                let file = parsed.unwrap_or_else(|error| {
                    panic!("{} must be accepted, got {error}", name_of(&vector))
                });
                if let Some(expected) = vector.get("resolved_dockerfile") {
                    for (connector, dockerfile) in expected.as_object().expect("an object") {
                        let spec = file
                            .connectors
                            .get(connector)
                            .unwrap_or_else(|| panic!("{connector} is declared"));
                        let build = spec.build.as_ref().expect("a build block");
                        assert_eq!(
                            build.dockerfile,
                            dockerfile.as_str().expect("a string"),
                            "{}: resolved dockerfile",
                            name_of(&vector)
                        );
                    }
                }
            }
            "reject" => {
                assert!(
                    parsed.is_err(),
                    "{} must be rejected by the Rust reader",
                    name_of(&vector)
                );
            }
            other => panic!("unknown expect {other:?}"),
        }
    }
}

#[test]
fn a_connector_name_python_refuses_is_refused_at_load_by_the_rust_reader() {
    // Parity is only half of it. `curie skill up` joins the connector name into
    // a host path under `.curie/connector-secrets/`, so a name the CLI accepted
    // without checking is a bundle-authored path component and `../../evil`
    // writes a resolved credential outside the bundle. Checking at load is what
    // makes every downstream join safe by construction.
    let too_long = "t".repeat(41);
    for name in [
        "../../evil",
        "Tempo",
        "tempo_1",
        "-tempo",
        "tempo-",
        "",
        too_long.as_str(),
    ] {
        let document = format!("connectors:\n  \"{name}\":\n    image: ghcr.io/acme/x:1\n");
        let error = connector_build::parse_connectors(&document)
            .expect_err(&format!("{name:?} must be refused"));
        assert!(
            error.to_string().contains(name),
            "the refusal must name the connector: {error}"
        );
    }

    connector_build::parse_connectors("connectors:\n  tempo-1:\n    image: ghcr.io/acme/x:1\n")
        .expect("an RFC 1123 label is still accepted");
}

#[test]
fn a_lock_entry_carrying_a_refused_name_is_refused_too() {
    // The lock is a second door onto the same names: `curie skill up` reads it
    // to resolve the image and stages that connector's credentials, whether or
    // not the declaration was re-read in the same process.
    // The entry itself is well formed, so the NAME is the only thing left to
    // refuse it -- a truncated digest here would green this test through the
    // image-shape rule instead.
    let document = "version: 1\nconnectors:\n  \"../../evil\":\n    \
                    image: ghcr.io/acme/x@sha256:\
                    0000000000000000000000000000000000000000000000000000000000000000\n    \
                    delivery: registry\n    source_digest: sha256:bb\n";
    assert!(
        connector_build::parse_lock(document).is_err(),
        "a name refused in connectors.yaml must be refused in the lock"
    );
}

#[test]
fn an_unknown_key_inside_a_build_block_is_refused_not_dropped() {
    // The `deny_unknown_fields` proof. Without the attribute the field is
    // silently dropped and `curie dev field-parity` stays green, which is
    // exactly the tolerance gap review finding 9 named: a future Python field
    // would vanish on the Rust side with every gate reporting success.
    // REMOVING the attribute makes this test pass, which is how the attribute
    // is proved to be the thing that catches it.
    let document = "connectors:\n  tempo:\n    build:\n      context: connectors/tempo\n      \
                    platforms: [linux/amd64]\n      target: runtime\n";
    assert!(
        connector_build::parse_connectors(document).is_err(),
        "an unmodelled key inside build: must be refused, not dropped"
    );
}

#[test]
fn a_symlinked_dockerfile_leaving_the_context_is_refused() {
    // The textual absolute / `..` check the Python validator performs cannot
    // see this: the declared path is clean and only the filesystem knows the
    // file is a link out of the bundle. `curie build` reads the Dockerfile
    // BEFORE the bundle is packed, so `cli/src/bundle.rs`'s symlink refusal
    // never runs. `resolve_dockerfile` must refuse rather than dereference,
    // for the same reason that packer guard exists.
    let vector = vectors("connector-build-decl.json")
        .into_iter()
        .find(|v| v.get("fixture").is_some())
        .expect("the corpus carries a filesystem fixture");
    let fixture = &vector["fixture"];
    let tmp = scratch();
    let root = tmp.path();
    let bundle = root.join("bundle");
    std::fs::create_dir_all(&bundle).expect("mkdir bundle");
    materialize(root, &fixture["outside_files"]);
    materialize(&bundle, &fixture["bundle_files"]);
    for (link, target) in fixture["bundle_symlinks"]
        .as_object()
        .expect("symlink recipe")
    {
        let path = bundle.join(link);
        std::fs::create_dir_all(path.parent().expect("a parent")).expect("mkdir -p");
        #[cfg(unix)]
        std::os::unix::fs::symlink(target.as_str().expect("a target"), &path).expect("symlink");
    }

    let context = connector_build::resolve_context(&bundle, "connectors/k8s-write")
        .expect("the context itself is inside the bundle");
    assert!(
        connector_build::resolve_dockerfile(&context, "Dockerfile").is_err(),
        "a Dockerfile that resolves through a symlink out of the context must be refused"
    );
}

#[test]
fn a_context_outside_the_bundle_is_refused() {
    // The other half of the containment rule, on the filesystem rather than in
    // the declaration: canonicalization must be what decides, so a symlinked
    // directory cannot walk out of the bundle either.
    let tmp = scratch();
    let root = tmp.path();
    let bundle = root.join("bundle");
    std::fs::create_dir_all(bundle.join("connectors/k8s-write")).expect("mkdir -p");
    std::fs::create_dir_all(root.join("outside")).expect("mkdir -p");

    assert!(connector_build::resolve_context(&bundle, "connectors/k8s-write").is_ok());
    assert!(connector_build::resolve_context(&bundle, "../outside").is_err());
    assert!(connector_build::resolve_context(&bundle, "/etc").is_err());
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(root.join("outside"), bundle.join("escape")).expect("symlink");
        assert!(
            connector_build::resolve_context(&bundle, "escape").is_err(),
            "a symlinked context directory must not walk out of the bundle"
        );
    }
}

// ─── connectors.lock.yaml ────────────────────────────────────────────────────

/// The corpus vectors whose refusal is the IMAGE SHAPE rule -- Python's
/// `_image_matches_delivery`, applied inside `apply_lock`.
///
/// Python can afford to refuse these one call later because nothing between
/// `validate_connector_lock` and `apply_lock` runs an image. The CLI has no
/// `apply_lock`: whatever `parse_lock` returns is what `curie skill up` starts
/// and what a deploy ships, so the same rule has to fire at the read. Listing
/// them by name rather than re-deriving the rule keeps this an independent
/// oracle: a reader that stopped refusing one fails here, and a NEW shape
/// vector added to the corpus fails here too until it is named, which is the
/// direction that failure should point.
const SHAPE_REFUSALS: &[&str] = &[
    "a_tag_shaped_image_is_refused",
    "a_truncated_digest_is_refused",
    "an_uppercase_digest_is_refused",
    "a_bare_digest_is_refused_for_registry_delivery",
    // ADR 0173: the runner entry's image AND base obey the same rule.
    "a_tag_shaped_runner_image_is_refused",
    "a_tag_shaped_runner_base_is_refused",
];

#[test]
fn the_rust_lock_reader_agrees_with_python_on_every_vector() {
    // `curie cluster deploy` preflights the lock locally so the operator gets
    // an actionable failure instead of an upload rejection, and the platform
    // validates the same file at intake so a git push cannot bypass it. The two
    // halves must agree on which documents are well formed, or one of them is
    // the only gate that ever fires.
    for vector in vectors("connector-lock.json") {
        if vector["lock"].is_null() {
            continue;
        }
        let document = serde_norway::to_string(&vector["lock"]).expect("serialize lock");
        let parsed = connector_build::parse_lock(&document);
        let expect = vector["expect"].as_str().expect("expect");
        if expect == "raise" && SHAPE_REFUSALS.contains(&name_of(&vector)) {
            assert!(
                parsed.is_err(),
                "{} records an image that is not a digest of its delivery's shape, which the \
                 Rust reader refuses at the read",
                name_of(&vector)
            );
            continue;
        }
        match expect {
            // The remaining "raise" documents are well formed and refused by
            // apply_lock for a reason only the DECLARATION can supply -- a
            // local-daemon image at a portable tier, an entry the lock does not
            // carry. That is the platform's job and this reader accepts them.
            "apply" | "raise" => assert!(
                parsed.is_ok(),
                "{} must parse on the Rust side",
                name_of(&vector)
            ),
            "invalid" => assert!(
                parsed.is_err(),
                "{} must be refused by the Rust reader too",
                name_of(&vector)
            ),
            other => panic!("unknown expect {other:?}"),
        }
    }
}

#[test]
fn an_unknown_key_in_a_lock_entry_is_refused_not_dropped() {
    // The `deny_unknown_fields` proof for the lock mirrors. An operator who
    // believes they pinned something they did not is the failure this refuses.
    let document = "version: 1\nconnectors:\n  tempo:\n    image: \
                    ghcr.io/acme-corp/acme-bot-tempo-mcp@sha256:\
                    0000000000000000000000000000000000000000000000000000000000000000\n    \
                    delivery: registry\n    platforms: [linux/amd64]\n    source_digest: \
                    sha256:2222222222222222222222222222222222222222222222222222222222222222\n    \
                    digest: sha256:0000\n";
    assert!(
        connector_build::parse_lock(document).is_err(),
        "an unmodelled key in a lock entry must be refused, not dropped"
    );
}

/// One lock document carrying one entry, so a case varies only what it is about.
fn lock_document(image: &str, delivery: &str) -> String {
    format!(
        "version: 1\nconnectors:\n  tempo:\n    image: {image}\n    delivery: {delivery}\n    \
         platforms: [linux/amd64]\n    source_digest: sha256:{}\n",
        "2".repeat(64)
    )
}

const REGISTRY_DIGEST_IMAGE: &str = "ghcr.io/acme-corp/acme-bot-tempo-mcp@sha256:\
                                     0000000000000000000000000000000000000000000000000000000000000000";
const LOCAL_DAEMON_IMAGE: &str =
    "sha256:1111111111111111111111111111111111111111111111111111111111111111";

#[test]
fn a_registry_entry_carries_a_repository_digest_as_pythons_image_matches_delivery_requires() {
    // `_image_matches_delivery` in packages/plugin-format/src/plugin_format/
    // connector_lock.py: `[^@\s]+@sha256:[0-9a-f]{64}` for delivery `registry`.
    // A tag is the case the rule exists for -- it can be repointed at a
    // different artifact after review -- and the other three are the ways a
    // substring check for `@sha256:` would pass while resolving to nothing.
    connector_build::parse_lock(&lock_document(REGISTRY_DIGEST_IMAGE, "registry"))
        .expect("a repository digest is the registry shape");

    for image in [
        "ghcr.io/acme-corp/acme-bot-tempo-mcp:v1",
        "ghcr.io/acme-corp/acme-bot-tempo-mcp@sha256:0000",
        "ghcr.io/acme-corp/acme-bot-tempo-mcp@sha256:\
         AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        LOCAL_DAEMON_IMAGE,
    ] {
        assert!(
            connector_build::parse_lock(&lock_document(image, "registry")).is_err(),
            "{image} is not a registry manifest digest and must be refused at the read"
        );
    }
}

#[test]
fn a_local_daemon_entry_carries_a_bare_image_id_as_pythons_image_matches_delivery_requires() {
    // The other half of the same rule: `sha256:[0-9a-f]{64}`, the id `docker
    // image inspect --format {{.Id}}` reports. A repository reference means
    // nothing to the local daemon, so delivery and shape must agree or the two
    // fields are decoration.
    connector_build::parse_lock(&lock_document(LOCAL_DAEMON_IMAGE, "local-daemon"))
        .expect("a bare image id is the local-daemon shape");

    for image in [
        REGISTRY_DIGEST_IMAGE,
        "curie-connector-sre-bot-tempo:build",
        "sha256:1111",
    ] {
        assert!(
            connector_build::parse_lock(&lock_document(image, "local-daemon")).is_err(),
            "{image} is not a local image id and must be refused at the read"
        );
    }
}

#[test]
fn a_refused_lock_image_names_the_connector_and_how_to_regenerate_the_lock() {
    // The operator reading this failure edited or inherited a lock and has to
    // know WHICH connector is wrong and what to run; a refusal that only says
    // the file is malformed sends them reading YAML by eye.
    let error = connector_build::parse_lock(&lock_document(
        "ghcr.io/acme-corp/acme-bot-tempo-mcp:v1",
        "registry",
    ))
    .expect_err("a tag must be refused");
    let message = error.to_string();
    assert!(message.contains("tempo"), "{message}");
    assert!(
        message.contains("ghcr.io/acme-corp/acme-bot-tempo-mcp:v1"),
        "{message}"
    );
    assert!(message.contains("curie build --plugin-dir"), "{message}");
}

#[test]
fn a_source_digest_shape_is_not_this_readers_to_judge_because_python_does_not_judge_it() {
    // `ConnectorLockEntry.source_digest` is a plain `str` in Python, and
    // staleness is decided by comparing it against a recomputed value, never by
    // its shape. A reader that refused a short one here would refuse a document
    // bundle intake accepts, which is the accept/reject parity this whole
    // corpus exists to hold.
    let document = format!(
        "version: 1\nconnectors:\n  tempo:\n    image: {REGISTRY_DIGEST_IMAGE}\n    \
         delivery: registry\n    platforms: [linux/amd64]\n    source_digest: sha256:bb\n"
    );
    connector_build::parse_lock(&document)
        .expect("the source digest's shape is the platform's to judge, not this reader's");
}

// ─── The field-name corpus ───────────────────────────────────────────────────

#[test]
fn the_mirror_structs_carry_exactly_the_frozen_field_names() {
    // The mechanism that answers review finding r2-1's exact scenario. Adding a
    // Python field with no vector edit fails the Python suite; editing the
    // vector to make that pass then fails HERE until the mirror gains the
    // field. The loop cannot be exited by touching one language.
    let fields = vector_file("connector-fields.json");
    let expect = |model: &str| -> BTreeSet<String> {
        fields["models"][model]
            .as_array()
            .unwrap_or_else(|| panic!("{model} is listed in the vector"))
            .iter()
            .map(|v| v.as_str().expect("a field name").to_string())
            .collect()
    };

    assert_eq!(connector_build::spec_field_names(), expect("ConnectorSpec"));
    assert_eq!(
        connector_build::build_field_names(),
        expect("ConnectorBuild")
    );
    assert_eq!(
        connector_build::lock_file_field_names(),
        expect("ConnectorLockFile")
    );
    assert_eq!(
        connector_build::lock_entry_field_names(),
        expect("ConnectorLockEntry")
    );
    // ADR 0173: the layered runner rides the same two files.
    assert_eq!(
        connector_build::connectors_file_field_names(),
        expect("ConnectorsFile")
    );
    assert_eq!(
        connector_build::runner_spec_field_names(),
        expect("RunnerSpec")
    );
    assert_eq!(
        connector_build::runner_lock_entry_field_names(),
        expect("RunnerLockEntry")
    );
}

// ─── object_name / service_dns: the Docker network alias ─────────────────────

#[test]
fn the_connector_dns_derivation_matches_the_frozen_vector() {
    // This string is the Docker network alias the connector container is
    // started under at the skill and local tiers, and the runner derives the
    // URL it dials from the PYTHON copy of this derivation. A mismatch leaves
    // the runner dialing a name no container owns: a bare connection timeout
    // with nothing logged at either end. The over-63-character truncation
    // branch is in the corpus because that is the half a hand port gets wrong.
    for vector in vectors("connector-service-dns.json") {
        let release = vector["release"].as_str().expect("release");
        let agent = vector["agent"].as_str().expect("agent");
        let connector = vector["connector"].as_str().expect("connector");
        let namespace = vector["namespace"].as_str().expect("namespace");
        assert_eq!(
            connector_build::object_name(release, agent, connector),
            vector["object_name"].as_str().expect("object_name"),
            "{}: object_name",
            name_of(&vector)
        );
        assert_eq!(
            connector_build::service_dns(release, agent, connector, namespace),
            vector["service_dns"].as_str().expect("service_dns"),
            "{}: service_dns",
            name_of(&vector)
        );
    }
}

// ─── source_digest ───────────────────────────────────────────────────────────

#[test]
fn the_source_digest_port_agrees_with_python_on_every_tree() {
    // "A changed source produces a distinct digest" is only a real proof if
    // both lanes compute the same value: the CLI writes the digest into the
    // lock and the platform validator recomputes it from the extracted bundle
    // to refuse a stale one. Two algorithms means every build looks stale on
    // one side of the seam. The corpus covers a nested tree, a `.dockerignore`d
    // file, a bundle-root context carrying `connectors.lock.yaml` and a
    // subdirectory one that hashes its own, a file that differs only in its
    // owner execute bit, and two edits that touch only the declaration.
    let tmp = scratch();
    let root = tmp.path();
    for (i, vector) in vectors("connector-source-digest.json").iter().enumerate() {
        let context = root.join(format!("ctx{i}"));
        std::fs::create_dir_all(&context).expect("mkdir ctx");
        materialize(&context, &vector["tree"]);
        let build: connector_build::ConnectorBuildDecl =
            serde_json::from_value(vector["build"].clone()).expect("parse the build block");
        assert_eq!(
            connector_build::source_digest_of(&context, &build).expect("hash the context"),
            vector["source_digest"].as_str().expect("source_digest"),
            "{}: source_digest",
            name_of(vector)
        );
    }
}

#[test]
fn the_source_digest_relations_hold() {
    // Where the exclusion rules are actually falsified. A port that ignores
    // `.dockerignore` still passes every single-vector assertion by recording
    // whatever it computes; it cannot satisfy both halves of a relation, since
    // one pair must come out equal and another must not.
    let doc = vector_file("connector-source-digest.json");
    let by_name: std::collections::BTreeMap<String, Value> = doc["vectors"]
        .as_array()
        .expect("vectors")
        .iter()
        .map(|v| (name_of(v).to_string(), v.clone()))
        .collect();
    let tmp = scratch();
    let root = tmp.path();

    for (i, relation) in doc["relations"]
        .as_array()
        .expect("relations")
        .iter()
        .enumerate()
    {
        let (names, same) = match relation.get("same") {
            Some(v) => (v, true),
            None => (&relation["distinct"], false),
        };
        let mut digests = Vec::new();
        for (j, name) in names.as_array().expect("a pair").iter().enumerate() {
            let vector = &by_name[name.as_str().expect("a name")];
            let context = root.join(format!("r{i}-{j}"));
            std::fs::create_dir_all(&context).expect("mkdir ctx");
            materialize(&context, &vector["tree"]);
            let build: connector_build::ConnectorBuildDecl =
                serde_json::from_value(vector["build"].clone()).expect("parse the build block");
            digests.push(
                connector_build::source_digest_of(&context, &build).expect("hash the context"),
            );
        }
        let why = relation["why"].as_str().unwrap_or("");
        if same {
            assert_eq!(digests[0], digests[1], "{why}");
        } else {
            assert_ne!(digests[0], digests[1], "{why}");
        }
    }
}

/// The one digest rule the JSON corpus cannot state, since a vector's `tree` is
/// a path-to-content map and cannot express a link.
///
/// Its Python twin is
/// `test_a_symlink_inside_the_context_is_neither_followed_nor_hashed`. Both must
/// hold or the digest stops being one cross-language identity: a bundle with a
/// linked file in its context would report its lock stale on one side of the
/// seam and current on the other. Asserted as an equality against the same tree
/// WITHOUT the links rather than as a frozen literal, so it survives any future
/// change to the stream layout.
#[test]
#[cfg(unix)]
fn a_symlink_inside_the_context_is_neither_followed_nor_hashed() {
    let tmp = scratch();
    let root = tmp.path();
    let outside = root.join("outside");
    std::fs::create_dir_all(outside.join("lib")).expect("mkdir -p");
    std::fs::write(outside.join("secret.env"), "TOKEN=abc\n").expect("write");
    std::fs::write(outside.join("lib/vendored.py"), "VENDORED = 1\n").expect("write");

    let plain = root.join("plain");
    let linked = root.join("linked");
    for context in [&plain, &linked] {
        std::fs::create_dir_all(context).expect("mkdir");
        std::fs::write(context.join("Dockerfile"), "FROM scratch\n").expect("write");
    }
    std::os::unix::fs::symlink(outside.join("secret.env"), linked.join("secret.env"))
        .expect("symlink a file");
    std::os::unix::fs::symlink(outside.join("lib"), linked.join("lib"))
        .expect("symlink a directory");

    let build: connector_build::ConnectorBuildDecl = serde_json::from_value(serde_json::json!({
        "context": "connectors/tempo",
        "dockerfile": "Dockerfile",
        "platforms": ["linux/amd64"],
    }))
    .expect("parse the build block");

    assert_eq!(
        connector_build::source_digest_of(&linked, &build).expect("hash the linked context"),
        connector_build::source_digest_of(&plain, &build).expect("hash the plain context"),
        "a symlink is never followed and never hashed, on both sides of the seam"
    );
}

// ─── derived Bearer name ─────────────────────────────────────────────────────

const DERIVED_BEARER_KEYS: &[&str] = &["name", "why", "document", "expected_name"];

#[test]
fn derived_bearer_vector_keys_are_known() {
    // A key added for the Python lane alone would pass vacuously here.
    let file = vector_file("connector-derived-bearer.json");
    let keys: BTreeSet<&str> = file
        .as_object()
        .expect("object")
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(keys, BTreeSet::from(["comment", "vectors"]));
    for vector in vectors("connector-derived-bearer.json") {
        let keys: BTreeSet<&str> = vector
            .as_object()
            .expect("object")
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(
            keys,
            DERIVED_BEARER_KEYS.iter().copied().collect(),
            "{}",
            name_of(&vector)
        );
    }
}

#[test]
fn derived_bearer_name_matches_the_frozen_vector() {
    // Cross-language pin: bearer_secret_name must agree with the renderer's
    // derived Authorization header, including the SecretRef case that used
    // to flatten a lone Ref into a Bearer name the renderer would not emit.
    for vector in vectors("connector-derived-bearer.json") {
        let document = serde_norway::to_string(&vector["document"]).expect("serialize document");
        let parsed = connector_build::parse_connectors(&document)
            .unwrap_or_else(|error| panic!("{} must parse, got {error}", name_of(&vector)));
        let spec = parsed
            .connectors
            .get("gh")
            .unwrap_or_else(|| panic!("{} has connector gh", name_of(&vector)));
        let expected = vector["expected_name"].as_str().map(str::to_string);
        assert_eq!(
            connector_build::bearer_secret_name(spec),
            expected,
            "{}",
            name_of(&vector)
        );
    }
}

// ─── The layered runner (ADR 0173 decisions 1 and 2) ─────────────────────────

#[test]
fn a_runner_build_block_is_read_with_its_resolved_dockerfile() {
    // The vector corpus proves accept/reject parity; this proves the accepted
    // block is actually carried rather than parsed and dropped, which is the
    // failure a missing field on the mirror struct produces.
    let file = connector_build::parse_connectors(
        "connectors: {}\nrunner:\n  build:\n    context: runner\n    \
         platforms: [linux/amd64, linux/arm64]\n",
    )
    .expect("a runner build block is a valid declaration");
    let runner = file.runner.as_ref().expect("the runner block is carried");
    assert_eq!(runner.build.context, "runner");
    assert_eq!(runner.build.dockerfile, "Dockerfile", "the default applies");
    assert_eq!(runner.build.platforms, vec!["linux/amd64", "linux/arm64"]);

    let none = connector_build::parse_connectors("connectors: {}\n").expect("no runner");
    assert!(
        none.runner.is_none(),
        "a bundle declaring no runner has none"
    );
}

const RUNNER_DOCKERFILE_KEYS: &[&str] = &["name", "why", "dockerfile", "expect"];

#[test]
fn the_runner_dockerfile_base_rule_agrees_with_python_on_every_vector() {
    // `curie build` refuses a runner Dockerfile whose first FROM is not the
    // CURIE_RUNNER_IMAGE build argument BEFORE any docker call; the platform
    // refuses the same file at intake (`connectors.runner_base_not_arg`). Both
    // read tests/vectors/runner-dockerfile-base.json.
    for vector in vectors("runner-dockerfile-base.json") {
        let keys: BTreeSet<&str> = vector
            .as_object()
            .expect("object")
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(
            keys,
            RUNNER_DOCKERFILE_KEYS.iter().copied().collect(),
            "{}",
            name_of(&vector)
        );
        let body = vector["dockerfile"].as_str().expect("dockerfile");
        let result = connector_build::check_runner_dockerfile(body);
        match vector["expect"].as_str().expect("expect") {
            "accept" => result.unwrap_or_else(|error| {
                panic!("{} must be accepted, got {error:#}", name_of(&vector))
            }),
            "reject" => {
                let error = result.expect_err(name_of(&vector));
                assert!(
                    format!("{error:#}").contains("CURIE_RUNNER_IMAGE"),
                    "{}: the refusal must name the build argument to use, got {error:#}",
                    name_of(&vector)
                );
            }
            other => panic!("unknown expect {other:?}"),
        }
    }
}

#[test]
fn a_runner_lock_entry_is_read_with_both_digests() {
    let a = "a".repeat(64);
    let b = "b".repeat(64);
    let document = format!(
        "version: 1\nconnectors: {{}}\nrunner:\n  image: ghcr.io/acme-corp/acme-bot-runner@sha256:{a}\n  \
         base: ghcr.io/curie-eng/curie-runner@sha256:{b}\n  delivery: registry\n  \
         platforms: [linux/amd64]\n  source_digest: sha256:{}\n",
        "2".repeat(64)
    );
    let lock = connector_build::parse_lock(&document).expect("a digest-pinned runner entry");
    let runner = lock.runner.as_ref().expect("the runner entry is carried");
    assert_eq!(
        runner.image,
        format!("ghcr.io/acme-corp/acme-bot-runner@sha256:{a}")
    );
    assert_eq!(
        runner.base,
        format!("ghcr.io/curie-eng/curie-runner@sha256:{b}")
    );
    assert_eq!(runner.delivery, connector_build::Delivery::Registry);
}

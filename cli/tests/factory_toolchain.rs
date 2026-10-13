//! Declaration parsing is independent of repository names. Setup action inputs
//! follow https://github.com/actions/setup-python/blob/main/action.yml and
//! https://github.com/actions/setup-node/blob/main/action.yml. Version-file
//! inputs contain paths, rather than version strings.

use std::collections::BTreeMap;
use std::io::Write;

use curie::factory_toolchain::{infer_files, read_tool_manifest, ManifestTool, ToolManifest};

use crate::support::oci_registry_stub::sha256_digest;
use crate::support::{serve, MockServer, Response};

fn tool(name: &str, version: &str, path: &str) -> ManifestTool {
    ManifestTool {
        name: name.into(),
        version: version.into(),
        path: path.into(),
        source: "probe".into(),
    }
}

fn inventory(tools: Vec<ManifestTool>) -> ToolManifest {
    ToolManifest {
        schema_version: 1,
        tools,
        docker_cli: false,
    }
}

fn fixture_manifest() -> ToolManifest {
    inventory(vec![
        tool("python", "3.13.2", "/usr/local/bin/python3"),
        tool("node", "22.23.0", "/usr/local/bin/node"),
        tool("rust", "1.95.0", "/usr/local/cargo/bin/cargo"),
    ])
}

fn files(entries: &[(&str, &str)]) -> BTreeMap<String, String> {
    entries
        .iter()
        .map(|(path, text)| (path.to_string(), text.to_string()))
        .collect()
}

#[test]
fn python_matching_runner_minor_is_supported() {
    let report = infer_files(&files(&[(
        "pyproject.toml",
        "[project]\nrequires-python = \"==3.13.2\"\n",
    )]))
    .unwrap();
    assert!(report.notes().join("\n").contains("Python 3.13.2"));
    assert!(
        report.warnings(Some(&fixture_manifest())).is_empty(),
        "{:?}",
        report.warnings(Some(&fixture_manifest()))
    );
}

#[test]
fn unsupported_go_names_tool_version_and_manifest() {
    let report = infer_files(&files(&[(
        "go.mod",
        "module example.com/acme\n\ngo 1.24.1\n",
    )]))
    .unwrap();
    let warnings = report.warnings(Some(&fixture_manifest())).join("\n");
    assert!(warnings.contains("Go 1.24.1"), "{warnings}");
    assert!(warnings.contains("go.mod"), "{warnings}");
}

#[test]
fn no_toolchain_signals_produces_an_explicit_note() {
    let report = infer_files(&files(&[("README.md", "sample")])).unwrap();
    assert!(report.notes().join("\n").contains("no toolchain signals"));
    assert!(report.warnings(Some(&fixture_manifest())).is_empty());
}

#[test]
fn exact_other_minor_warns_even_when_the_runner_has_the_language() {
    for (path, text, expected) in [
        (".python-version", "3.12.9", "Python 3.12.9"),
        (".nvmrc", "22.9.0", "Node 22.9.0"),
        (
            "rust-toolchain.toml",
            "[toolchain]\nchannel = \"1.94.0\"",
            "Rust 1.94.0",
        ),
    ] {
        let report = infer_files(&files(&[(path, text)])).unwrap();
        let warnings = report.warnings(Some(&fixture_manifest())).join("\n");
        assert!(warnings.contains(expected), "{warnings}");
        assert!(warnings.contains(path), "{warnings}");
    }
}

#[test]
fn workflows_and_standard_manifests_infer_versions_without_repository_assumptions() {
    let report = infer_files(&files(&[
        (".github/workflows/checks.yml", "jobs:\n  tests:\n    runs-on: ubuntu-latest\n    steps:\n      - uses: actions/setup-python@v5\n        with:\n          python-version: '3.13'\n      - uses: actions/setup-node@v4\n        with:\n          node-version: '22'\n      - uses: actions/setup-java@v4\n        with:\n          java-version: '21'\n"),
        ("pom.xml", "<project><properties><maven.compiler.release>21</maven.compiler.release></properties></project>"),
        ("Gemfile", "source 'https://rubygems.org'\nruby '3.4.1'\n"),
    ])).unwrap();
    let notes = report.notes().join("\n");
    assert!(notes.contains("Python 3.13"), "{notes}");
    assert!(notes.contains("Node 22"), "{notes}");
    let warnings = report.warnings(Some(&fixture_manifest())).join("\n");
    assert!(
        warnings.contains("Java 21") && warnings.contains("Ruby 3.4.1"),
        "{warnings}"
    );
}

#[test]
fn version_file_and_workflow_matrix_are_read_as_versions() {
    let report = infer_files(&files(&[
        (".tool-versions", "python 3.13.4\nnodejs 22\nrust 1.95.1\n"),
        (".github/workflows/test.yaml", "jobs:\n  tests:\n    strategy:\n      matrix:\n        python-version: ['3.13', '3.12']\n    steps:\n      - uses: actions/setup-python@v5\n        with:\n          python-version: ${{ matrix.python-version }}\n"),
    ])).unwrap();
    assert!(report.notes().join("\n").contains("Rust 1.95.1"));
    let warnings = report.warnings(Some(&fixture_manifest())).join("\n");
    assert!(
        warnings.contains("Python 3.12") && warnings.contains("test.yaml"),
        "{warnings}"
    );
}

#[test]
fn manifest_ranges_that_admit_the_runner_are_supported() {
    let report = infer_files(&files(&[
        (
            "pyproject.toml",
            "[project]\nrequires-python = \">=3.10,<3.14\"\n",
        ),
        ("package.json", r#"{"engines":{"node":">=20 <23"}}"#),
        ("Cargo.toml", "[package]\nrust-version = \"1.80\"\n"),
    ]))
    .unwrap();
    assert!(
        report.warnings(Some(&fixture_manifest())).is_empty(),
        "{:?}",
        report.warnings(Some(&fixture_manifest()))
    );
}

#[test]
fn malformed_recognized_manifest_never_claims_no_signals() {
    let err = infer_files(&files(&[("package.json", "{broken")])).unwrap_err();
    assert!(format!("{err:#}").contains("package.json"));
}

#[test]
fn alternatives_admit_supported_runner_and_unresolved_or_prerelease_versions_warn() {
    let report = infer_files(&files(&[(
        "package.json",
        r#"{"engines":{"node":"20 || 22"}}"#,
    )]))
    .unwrap();
    assert!(
        report.warnings(Some(&fixture_manifest())).is_empty(),
        "{:?}",
        report.warnings(Some(&fixture_manifest()))
    );
    for version in [
        "3.13rc1",
        "${{ inputs.python-version }}",
        "unknown version 3.13",
    ] {
        let report = infer_files(&files(&[(".python-version", version)])).unwrap();
        assert!(
            !report.warnings(Some(&fixture_manifest())).is_empty(),
            "uncertain version must warn: {version}"
        );
    }
}

#[test]
fn npm_tilde_keeps_minor_while_python_compatible_release_uses_declared_precision() {
    // npm ~22.9 means >=22.9.0 <22.10.0; Python ~=3.12 means >=3.12 <4,
    // while ~=3.12.1 means >=3.12.1 <3.13. These operator meanings follow
    // https://github.com/npm/node-semver#tilde-ranges-123-12-1 and
    // https://packaging.python.org/en/latest/specifications/version-specifiers/#compatible-release.
    for (declaration, supported) in [("~22.9", false), ("~22.23", true), ("~22", true)] {
        let manifest = format!(r#"{{"engines":{{"node":"{declaration}"}}}}"#);
        let report = infer_files(&files(&[("package.json", &manifest)])).unwrap();
        assert_eq!(
            report.warnings(Some(&fixture_manifest())).is_empty(),
            supported,
            "{declaration}: {:?}",
            report.warnings(Some(&fixture_manifest()))
        );
    }
    for (declaration, supported) in [("~=3.12", true), ("~=3.12.1", false), ("~=3.13.0", true)] {
        let manifest = format!("[project]\nrequires-python = \"{declaration}\"\n");
        let report = infer_files(&files(&[("pyproject.toml", &manifest)])).unwrap();
        assert_eq!(
            report.warnings(Some(&fixture_manifest())).is_empty(),
            supported,
            "{declaration}: {:?}",
            report.warnings(Some(&fixture_manifest()))
        );
    }
}

#[test]
fn identical_declarations_follow_the_observed_image_version() {
    let report = infer_files(&files(&[(".python-version", "3.12.9")])).unwrap();
    let matching = inventory(vec![tool("python", "3.12.9", "/usr/bin/python3")]);
    let different = inventory(vec![tool("python", "3.13.2", "/usr/bin/python3")]);
    assert!(report.warnings(Some(&matching)).is_empty());
    let warnings = report.warnings(Some(&different)).join("\n");
    assert!(
        warnings.contains("Python 3.12.9") && warnings.contains(".python-version"),
        "{warnings}"
    );
    assert!(
        warnings.contains("3.13.2"),
        "observed version must be named: {warnings}"
    );
}

#[test]
fn custom_probes_identify_languages_by_observed_name_or_executable_path() {
    let report = infer_files(&files(&[(".python-version", "3.12.9")])).unwrap();
    for observed in [
        tool("PYTHON", "3.12.9", "/opt/acme/runtime"),
        tool("custom-runtime", "3.12.9", "/opt/acme/Python3.12"),
    ] {
        assert!(report.warnings(Some(&inventory(vec![observed]))).is_empty());
    }
}

#[test]
fn package_managers_do_not_prove_a_language_runtime_and_missing_inventory_warns() {
    let report = infer_files(&files(&[
        (".python-version", "3.13.2"),
        (".node-version", "22.23.0"),
    ]))
    .unwrap();
    let managers = inventory(vec![
        tool("uv", "0.10.7", "/usr/bin/uv"),
        tool("pnpm", "9.15.9", "/usr/bin/pnpm"),
    ]);
    let warnings = report.warnings(Some(&managers)).join("\n");
    assert!(
        warnings.contains("Python") && warnings.contains("Node"),
        "{warnings}"
    );
    let unknown = report.warnings(None).join("\n");
    assert!(
        unknown.contains("unknown") && unknown.contains("inventory"),
        "{unknown}"
    );
    assert!(report.warnings(Some(&fixture_manifest())).is_empty());
}

#[test]
fn tool_manifest_requires_its_declared_schema_and_valid_observed_tools() {
    // Shape authority is docs/schemas/tool-manifest.schema.json, shared with
    // the image probe writer, rather than a factory warning assumption.
    let valid = serde_json::json!({
        "schema_version":1, "docker_cli":false,
        "tools":[{"name":"python", "version":"3.12.9", "path":"/usr/bin/python3", "source":"probe"}],
    });
    let manifest = ToolManifest::parse(&serde_json::to_vec(&valid).unwrap()).unwrap();
    assert_eq!(manifest.schema_version, 1);
    assert_eq!(manifest.tools[0].version, "3.12.9");
    assert!(!manifest.docker_cli);
    for case in [
        "version",
        "missing-tools",
        "empty-version",
        "bad-source",
        "extra-key",
    ] {
        let mut malformed = valid.clone();
        match case {
            "version" => malformed["schema_version"] = serde_json::json!(2),
            "missing-tools" => {
                malformed.as_object_mut().unwrap().remove("tools");
            }
            "empty-version" => malformed["tools"][0]["version"] = serde_json::json!(" "),
            "bad-source" => malformed["tools"][0]["source"] = serde_json::json!("guessed"),
            _ => malformed["unexpected"] = serde_json::json!(true),
        }
        assert!(
            ToolManifest::parse(&serde_json::to_vec(&malformed).unwrap()).is_err(),
            "{case}"
        );
    }
    assert!(ToolManifest::parse(b"{broken").is_err());
}

fn observed_json(version: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "schema_version":1, "docker_cli":false,
        "tools":[{"name":"custom-python", "version":version, "path":"/usr/bin/python3", "source":"probe"}],
    })).unwrap()
}

fn layer(entries: &[(&str, &[u8], u8)], gzip: bool) -> Vec<u8> {
    let mut archive = tar::Builder::new(Vec::new());
    for (path, bytes, kind) in entries {
        let mut header = tar::Header::new_gnu();
        header.set_path(path).unwrap();
        header.set_entry_type(tar::EntryType::new(*kind));
        header.set_mode(0o644);
        header.set_size(bytes.len() as u64);
        if *kind == b'2' {
            header.set_link_name("../../untrusted.json").unwrap();
        }
        header.set_cksum();
        archive.append(&header, *bytes).unwrap();
    }
    let bytes = archive.into_inner().unwrap();
    if !gzip {
        return bytes;
    }
    let mut compressed = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    compressed.write_all(&bytes).unwrap();
    compressed.finish().unwrap()
}

fn image_manifest(layers: &[(Vec<u8>, &str)]) -> Vec<u8> {
    // OCI descriptor/layer and whiteout shapes are defined by
    // https://github.com/opencontainers/image-spec/blob/main/manifest.md
    // and https://github.com/opencontainers/image-spec/blob/main/layer.md.
    serde_json::to_vec(&serde_json::json!({
        "schemaVersion":2, "mediaType":"application/vnd.oci.image.manifest.v1+json",
        "config":{"mediaType":"application/vnd.oci.image.config.v1+json", "digest":format!("sha256:{}", "a".repeat(64)), "size":2},
        "layers":layers.iter().map(|(bytes, media_type)| serde_json::json!({
            "digest":sha256_digest(bytes), "size":bytes.len(), "mediaType":media_type,
        })).collect::<Vec<_>>(),
    })).unwrap()
}

fn image_registry(layers: Vec<(Vec<u8>, &str)>) -> (MockServer, String) {
    let manifest = image_manifest(&layers);
    let digest = sha256_digest(&manifest);
    let mut responses = BTreeMap::new();
    responses.insert(format!("/v2/acme/runner/manifests/{digest}"), manifest);
    for (bytes, _) in layers {
        responses.insert(
            format!("/v2/acme/runner/blobs/{}", sha256_digest(&bytes)),
            bytes,
        );
    }
    let server = serve(move |request| match responses.get(&request.path) {
        Some(bytes) => Response {
            status: 200,
            content_type: "application/octet-stream".into(),
            body: bytes.clone(),
        },
        None => Response::json(404, r#"{"errors":[{"code":"MANIFEST_UNKNOWN"}]}"#),
    });
    let image = format!(
        "{}/acme/runner@{digest}",
        server.base_url.trim_start_matches("http://")
    );
    (server, image)
}

const TAR_MEDIA: &str = "application/vnd.oci.image.layer.v1.tar";
const GZIP_MEDIA: &str = "application/vnd.oci.image.layer.v1.tar+gzip";
const TOOL_PATH: &str = "etc/curie/tool-manifest.json";

#[tokio::test]
async fn image_inventory_uses_the_newest_plain_or_gzip_manifest_layer() {
    for gzip in [false, true] {
        let old = observed_json("3.13.2");
        let current = observed_json("3.12.9");
        let (_registry, image) = image_registry(vec![
            (layer(&[(TOOL_PATH, &old, b'0')], false), TAR_MEDIA),
            (
                layer(&[("./etc/curie/tool-manifest.json", &current, b'0')], gzip),
                if gzip { GZIP_MEDIA } else { TAR_MEDIA },
            ),
        ]);
        let manifest = read_tool_manifest(&image)
            .await
            .unwrap()
            .expect("observed manifest");
        assert_eq!(manifest.tools[0].version, "3.12.9");
        let report = infer_files(&files(&[(".python-version", "3.12.9")])).unwrap();
        assert!(report.warnings(Some(&manifest)).is_empty());
    }
}

#[tokio::test]
async fn image_inventory_selects_a_linux_index_child_independently_of_descriptor_order() {
    // OCI index platform descriptors follow
    // https://github.com/opencontainers/image-spec/blob/main/image-index.md.
    let amd64 = layer(&[(TOOL_PATH, &observed_json("3.12.9"), b'0')], true);
    let arm64 = layer(&[(TOOL_PATH, &observed_json("3.13.2"), b'0')], true);
    let amd64_manifest = image_manifest(&[(amd64.clone(), GZIP_MEDIA)]);
    let arm64_manifest = image_manifest(&[(arm64.clone(), GZIP_MEDIA)]);
    for reverse in [false, true] {
        let mut children = vec![
            serde_json::json!({"digest":sha256_digest(&arm64_manifest), "size":arm64_manifest.len(), "mediaType":"application/vnd.oci.image.manifest.v1+json", "platform":{"os":"linux", "architecture":"arm64"}}),
            serde_json::json!({"digest":sha256_digest(&amd64_manifest), "size":amd64_manifest.len(), "mediaType":"application/vnd.oci.image.manifest.v1+json", "platform":{"os":"linux", "architecture":"amd64"}}),
        ];
        if reverse {
            children.reverse();
        }
        let index = serde_json::to_vec(&serde_json::json!({"schemaVersion":2, "mediaType":"application/vnd.oci.image.index.v1+json", "manifests":children})).unwrap();
        let digest = sha256_digest(&index);
        let responses = BTreeMap::from([
            (format!("/v2/acme/runner/manifests/{digest}"), index),
            (
                format!(
                    "/v2/acme/runner/manifests/{}",
                    sha256_digest(&amd64_manifest)
                ),
                amd64_manifest.clone(),
            ),
            (
                format!(
                    "/v2/acme/runner/manifests/{}",
                    sha256_digest(&arm64_manifest)
                ),
                arm64_manifest.clone(),
            ),
            (
                format!("/v2/acme/runner/blobs/{}", sha256_digest(&amd64)),
                amd64.clone(),
            ),
            (
                format!("/v2/acme/runner/blobs/{}", sha256_digest(&arm64)),
                arm64.clone(),
            ),
        ]);
        let server = serve(move |request| Response {
            status: 200,
            content_type: "application/octet-stream".into(),
            body: responses
                .get(&request.path)
                .expect("known OCI object")
                .clone(),
        });
        let image = format!(
            "{}/acme/runner@{digest}",
            server.base_url.trim_start_matches("http://")
        );
        let manifest = read_tool_manifest(&image).await.unwrap().unwrap();
        assert_eq!(manifest.tools[0].version, "3.12.9");
    }
}

#[tokio::test]
async fn removed_or_nonregular_inventory_never_reuses_an_older_layer() {
    for (path, kind) in [
        ("etc/curie/.wh.tool-manifest.json", b'0'),
        ("etc/curie/.wh..wh..opq", b'0'),
        (TOOL_PATH, b'2'),
    ] {
        let old = observed_json("3.13.2");
        let (_registry, image) = image_registry(vec![
            (layer(&[(TOOL_PATH, &old, b'0')], false), TAR_MEDIA),
            (layer(&[(path, b"", kind)], false), TAR_MEDIA),
        ]);
        let result = read_tool_manifest(&image).await;
        if kind == b'0' {
            assert!(result.unwrap().is_none(), "removed inventory: {path}");
        } else {
            assert!(
                result.is_err() || result.unwrap().is_none(),
                "symlink is not observed inventory"
            );
        }
    }
    let (_registry, image) = image_registry(vec![(layer(&[], false), TAR_MEDIA)]);
    assert!(read_tool_manifest(&image).await.unwrap().is_none());
}

#[tokio::test]
async fn malformed_or_unsupported_newer_inventory_is_an_error_without_fallback() {
    for (bytes, media_type) in [
        (layer(&[(TOOL_PATH, b"{broken", b'0')], false), TAR_MEDIA),
        (
            vec![0x28, 0xb5, 0x2f, 0xfd],
            "application/vnd.oci.image.layer.v1.tar+zstd",
        ),
    ] {
        let old = observed_json("3.13.2");
        let (_registry, image) = image_registry(vec![
            (layer(&[(TOOL_PATH, &old, b'0')], false), TAR_MEDIA),
            (bytes, media_type),
        ]);
        assert!(read_tool_manifest(&image).await.is_err());
    }
}

#[tokio::test]
async fn inventory_reads_reject_blob_and_image_digest_mismatches() {
    let bytes = layer(&[(TOOL_PATH, &observed_json("3.12.9"), b'0')], false);
    let manifest = image_manifest(&[(bytes, TAR_MEDIA)]);
    let digest = sha256_digest(&manifest);
    let server = serve(move |request| {
        if request.path.contains("/manifests/") {
            Response {
                status: 200,
                content_type: "application/json".into(),
                body: manifest.clone(),
            }
        } else {
            Response {
                status: 200,
                content_type: "application/octet-stream".into(),
                body: layer(&[(TOOL_PATH, &observed_json("3.13.2"), b'0')], false),
            }
        }
    });
    let host = server.base_url.trim_start_matches("http://");
    for reference in [digest, format!("sha256:{}", "9".repeat(64))] {
        let error = read_tool_manifest(&format!("{host}/acme/runner@{reference}"))
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("hash"), "{error:#}");
    }
}

#[tokio::test]
async fn inventory_reads_bound_blob_decompression_and_manifest_bytes() {
    let large_plain = layer(
        &[("usr/share/padding", &vec![b'x'; 4 * 1024 * 1024], b'0')],
        false,
    );
    let bomb = layer(
        &[("usr/share/padding", &vec![b'x'; 8 * 1024 * 1024], b'0')],
        true,
    );
    let mut oversized_json = observed_json("3.12.9");
    oversized_json.resize(64 * 1024 + 1, b' ');
    let oversized_manifest = layer(&[(TOOL_PATH, &oversized_json, b'0')], true);
    for (bytes, media_type) in [
        (large_plain, TAR_MEDIA),
        (bomb, GZIP_MEDIA),
        (oversized_manifest, GZIP_MEDIA),
    ] {
        let (_registry, image) = image_registry(vec![(bytes, media_type)]);
        let error = read_tool_manifest(&image).await.unwrap_err();
        let text = format!("{error:#}");
        assert!(
            text.contains("limit") || text.contains("larg") || text.contains("exceed"),
            "{text}"
        );
    }
}

#[tokio::test]
async fn inventory_reads_have_layer_and_total_budgets_but_stop_at_a_newest_manifest() {
    let mut layers: Vec<_> = (0..17)
        .map(|index| {
            (
                layer(&[(&format!("ignored/{index}"), b"x", b'0')], false),
                TAR_MEDIA,
            )
        })
        .collect();
    let (_registry, image) = image_registry(layers.clone());
    assert!(
        read_tool_manifest(&image).await.is_err(),
        "17 searched layers exceed the budget"
    );
    layers.push((
        layer(&[(TOOL_PATH, &observed_json("3.12.9"), b'0')], false),
        TAR_MEDIA,
    ));
    let (registry, image) = image_registry(layers);
    assert_eq!(
        read_tool_manifest(&image).await.unwrap().unwrap().tools[0].version,
        "3.12.9"
    );
    assert_eq!(
        registry
            .recorded()
            .iter()
            .filter(|request| request.path.contains("/blobs/"))
            .count(),
        1
    );
    let padding = vec![b'x'; 6 * 1024 * 1024];
    let layers = (0..3)
        .map(|index| {
            (
                layer(&[(&format!("ignored/{index}"), &padding, b'0')], true),
                GZIP_MEDIA,
            )
        })
        .collect();
    let (_registry, image) = image_registry(layers);
    assert!(
        read_tool_manifest(&image).await.is_err(),
        "total decompressed bytes exceed 16 MiB"
    );
}

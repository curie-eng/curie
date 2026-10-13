//! Declaration parsing is independent of repository names. Setup action inputs
//! follow https://github.com/actions/setup-python/blob/main/action.yml and
//! https://github.com/actions/setup-node/blob/main/action.yml. Version-file
//! inputs contain paths, rather than version strings.

use std::collections::BTreeMap;

use curie::factory_toolchain::{infer_files, ManifestTool, ToolManifest};

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

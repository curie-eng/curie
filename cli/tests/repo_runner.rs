//! Integration: `curie build --repo-runner` building blocks (ADR 0216
//! decisions 2-4). Pins, by outcome rather than internals:
//!
//! 1. Catalog v1 (`CATALOG_JSON`): every entry validates against the committed
//!    `docs/schemas/tool-catalog-entry.schema.json`, the names and order are
//!    the catalog order, and the reviewed pins (per-platform digests, the pnpm
//!    tarball checksum, registry hosts, probe argv, cache env) are the ones in
//!    the file. Cache paths stay under the writable `/home/runner`.
//! 2. The build input file refusals, each naming the offending value, each
//!    beside the valid neighbour that must still pass.
//! 3. Probe ordering, the generated Dockerfiles, and the bake definition
//!    (target graph, platforms, only the last target pushed, `${`/`%{` escaped
//!    so bake does not interpolate them).
//! 4. The content tag, digest parsing from a recorded buildx index, and the
//!    receipt shape (the Helm `repositoryRunner` values block).
//! 5. The probe script, executed for real with python3: the manifest it writes
//!    validates against `docs/schemas/tool-manifest.schema.json`, and every
//!    failure mode exits nonzero, writes nothing and names the probe.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use curie::repo_runner::{
    bake_definition, bake_escape, bare_digest, catalog, catalog_dockerfile, check_builds_on_base,
    finalize_dockerfile, input_tag, load_spec, parse_spec, platform_diff_ids,
    platform_digests_from_index, probe_runs, selected_entries, validate_spec, BakePlan, ProbeDecl,
    ProbeRun, RepoRunnerBuildOutput, RepoRunnerSpec, RunnerRecord, BAKE_TARGET, CATALOG_JSON,
    CATALOG_VERSION, IMAGE_NAME, MANIFEST_PATH, PLATFORMS, PROBE_SCRIPT,
};
use serde_json::{json, Value};

const UV_AMD64: &str = "0ca776d5bd774b0f8a9092100166ac46bf93386da17b8bf626f8e60b1f2d1c77";
const UV_ARM64: &str = "5fe7b2b0499a485ee86e1e0d2154e1556a08dffeb5f72b204895af5212a2069c";
const RUST_AMD64: &str = "4c2fd73ef19c5ef9d54bee03b06b2839a392604fbfcd578ed948b71b37c1d7fb";
const RUST_ARM64: &str = "8e45ae5b178fa788bbbd818b42a1f93a6e2c03e7144badd5e0a37087537177e1";
const PNPM_SHA: &str = "cf86a7ad764406395d4286a6d09d730711720acc6d93e9dce9ac7ac4dc4a28a7";
const DEBIAN_AMD64: &str = "a4672c0cb26fbdde88e38fa2dfb6c681942306680e41e4378b28770b6e79ee91";
const DEBIAN_ARM64: &str = "a1b86db52ce3daef089e45aabe36dfec4091f82464c25c1fdcf03de197cbe82a";
const DOCKER_AMD64: &str = "4caa52207e95c68c3087006eefb3a5f6ca48ad1c0ba3aa3e6843549a0720b7bb";
const DOCKER_ARM64: &str = "b840c948ec2a78bf90a99e713f7ad711c0dea887a86723fba3d47703dfe43506";

const GOOD_DOCKERFILE: &str = "ARG CURIE_RUNNER_IMAGE\nFROM ${CURIE_RUNNER_IMAGE}\nRUN true\n";

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("cli/ has a parent")
        .to_path_buf()
}

fn load_json(rel: &str) -> Value {
    let path = repo_root().join(rel);
    let raw = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("committed file {} must exist: {e}", path.display()));
    serde_json::from_str(&raw)
        .unwrap_or_else(|e| panic!("{} must be valid JSON: {e}", path.display()))
}

fn validator(schema: &Value) -> jsonschema::Validator {
    jsonschema::validator_for(schema).expect("schema compiles to a validator")
}

fn spec(tools: &[&str]) -> RepoRunnerSpec {
    RepoRunnerSpec {
        tools: tools.iter().map(|t| t.to_string()).collect(),
        ..RepoRunnerSpec::default()
    }
}

fn probe(name: &str, command: &[&str]) -> ProbeDecl {
    ProbeDecl {
        name: name.to_string(),
        command: command.iter().map(|c| c.to_string()).collect(),
    }
}

fn err_text<T>(result: anyhow::Result<T>) -> String {
    match result {
        Ok(_) => panic!("expected a refusal"),
        Err(e) => format!("{e:#}"),
    }
}

fn digest(c: char) -> String {
    format!("sha256:{}", c.to_string().repeat(64))
}

// ---------------------------------------------------------------------------
// 1. Catalog v1
// ---------------------------------------------------------------------------

#[test]
fn catalog_json_entries_validate_against_the_committed_schema() {
    let schema = load_json("docs/schemas/tool-catalog-entry.schema.json");
    let v = validator(&schema);
    let entries: Vec<Value> = serde_json::from_str(CATALOG_JSON).expect("catalog is a JSON array");
    assert_eq!(entries.len(), 5, "catalog v1 has five entries");
    for entry in &entries {
        let errors: Vec<String> = v.iter_errors(entry).map(|e| e.to_string()).collect();
        assert!(
            errors.is_empty(),
            "catalog entry must validate against tool-catalog-entry.schema.json: {entry}\n{errors:?}"
        );
    }
}

#[test]
fn catalog_names_are_the_catalog_order_and_every_entry_has_both_platforms() {
    let names: Vec<&str> = catalog().iter().map(|e| e.name.as_str()).collect();
    assert_eq!(
        names,
        ["uv", "rust", "pnpm", "build-essential", "docker-cli"]
    );
    assert_eq!(CATALOG_VERSION, 1);
    assert_eq!(PLATFORMS, ["linux/amd64", "linux/arm64"]);
    for entry in catalog() {
        assert_eq!(entry.platforms, PLATFORMS, "{} platforms", entry.name);
        for platform in PLATFORMS {
            assert!(
                entry.source_digests.contains_key(platform),
                "{} must pin {platform}",
                entry.name
            );
        }
        for (var, path) in &entry.cache_env {
            assert!(
                path == "/home/runner" || path.starts_with("/home/runner/"),
                "{} cache {var}={path} must be under /home/runner",
                entry.name
            );
            assert!(!path.split('/').any(|p| p == ".."), "{var}={path} escapes");
        }
    }
}

fn entry(name: &str) -> &'static curie::repo_runner::CatalogEntry {
    catalog()
        .iter()
        .find(|e| e.name == name)
        .unwrap_or_else(|| panic!("catalog has {name}"))
}

fn assert_pinned(name: &str, repo_fragment: &str, amd64: &str, arm64: &str) {
    let e = entry(name);
    for (platform, hex) in [("linux/amd64", amd64), ("linux/arm64", arm64)] {
        let pinned = &e.source_digests[platform];
        assert!(
            pinned.ends_with(&format!("@sha256:{hex}")),
            "{name} {platform} must pin sha256:{hex}, got {pinned}"
        );
        assert!(
            pinned.contains(repo_fragment),
            "{name} {platform} source must name {repo_fragment}, got {pinned}"
        );
    }
}

fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn strings(items: &[&str]) -> Vec<String> {
    items.iter().map(|s| s.to_string()).collect()
}

#[test]
fn catalog_carries_the_reviewed_v1_pins() {
    assert_pinned("uv", "ghcr.io/astral-sh/uv", UV_AMD64, UV_ARM64);
    assert_pinned("rust", "rust", RUST_AMD64, RUST_ARM64);
    assert_pinned(
        "pnpm",
        "registry.npmjs.org/pnpm/-/pnpm-9.15.9.tgz",
        PNPM_SHA,
        PNPM_SHA,
    );
    assert_pinned("build-essential", "debian", DEBIAN_AMD64, DEBIAN_ARM64);
    assert_pinned("docker-cli", "docker", DOCKER_AMD64, DOCKER_ARM64);

    assert_eq!(
        entry("uv").cache_env,
        map(&[("UV_CACHE_DIR", "/home/runner/.cache/uv")])
    );
    assert_eq!(
        entry("rust").cache_env,
        map(&[("CARGO_HOME", "/home/runner/.cargo")])
    );
    assert_eq!(
        entry("pnpm").cache_env,
        map(&[("PNPM_HOME", "/home/runner/.local/share/pnpm")])
    );
    assert!(entry("build-essential").cache_env.is_empty());
    assert_eq!(
        entry("docker-cli").cache_env,
        map(&[("DOCKER_CONFIG", "/home/runner/.docker")])
    );

    assert_eq!(
        entry("uv").registry_hosts,
        strings(&["pypi.org", "files.pythonhosted.org"])
    );
    assert_eq!(
        entry("rust").registry_hosts,
        strings(&["index.crates.io", "static.crates.io"])
    );
    assert_eq!(
        entry("pnpm").registry_hosts,
        strings(&["registry.npmjs.org"])
    );
    assert!(entry("build-essential").registry_hosts.is_empty());
    assert!(entry("docker-cli").registry_hosts.is_empty());

    assert_eq!(entry("uv").probe, strings(&["uv", "--version"]));
    assert_eq!(entry("rust").probe, strings(&["cargo", "--version"]));
    assert_eq!(entry("pnpm").probe, strings(&["pnpm", "--version"]));
    assert_eq!(
        entry("build-essential").probe,
        strings(&["gcc", "--version"])
    );
    assert_eq!(entry("docker-cli").probe, strings(&["docker", "--version"]));
}

// ---------------------------------------------------------------------------
// 2. Spec refusals and their valid neighbours
// ---------------------------------------------------------------------------

#[test]
fn spec_unknown_tool_is_refused_naming_it_and_listing_the_catalog() {
    let msg = err_text(parse_spec("tools: [uv, node]\n"));
    assert!(msg.contains("node"), "{msg}");
    for name in ["uv", "rust", "pnpm", "build-essential", "docker-cli"] {
        assert!(
            msg.contains(name),
            "message must list catalog name {name}: {msg}"
        );
    }
    // Neighbour: tools only, all from the catalog.
    let ok = parse_spec("tools: [uv, rust]\n").expect("catalog tools parse");
    assert_eq!(ok.tools, ["uv", "rust"]);
    assert!(ok.dockerfile.is_none() && ok.probes.is_empty());
}

#[test]
fn spec_accepts_json_as_yaml() {
    let ok = parse_spec(
        r#"{"tools": ["pnpm"], "probes": [{"name": "tf", "command": ["terraform", "version"]}]}"#,
    )
    .expect("JSON input parses");
    assert_eq!(ok.tools, ["pnpm"]);
    assert_eq!(ok.probes, vec![probe("tf", &["terraform", "version"])]);
}

#[test]
fn spec_duplicate_tool_is_refused() {
    let msg = err_text(parse_spec("tools: [uv, rust, uv]\n"));
    assert!(msg.contains("uv"), "{msg}");
    let msg = err_text(validate_spec(&spec(&["pnpm", "pnpm"])));
    assert!(msg.contains("pnpm"), "{msg}");
    validate_spec(&spec(&["pnpm"])).expect("single pnpm is valid");
}

#[test]
fn spec_probe_refusals_name_the_probe() {
    let empty = "probes:\n  - name: chk-empty\n    command: []\n";
    let msg = err_text(parse_spec(empty));
    assert!(msg.contains("chk-empty"), "{msg}");

    let blank_argv0 = "probes:\n  - name: chk-blank\n    command: [\"  \", \"--version\"]\n";
    let msg = err_text(parse_spec(blank_argv0));
    assert!(msg.contains("chk-blank"), "{msg}");

    let blank_name = "probes:\n  - name: \"  \"\n    command: [terraform, version]\n";
    let msg = err_text(parse_spec(blank_name));
    assert!(msg.to_lowercase().contains("probe"), "{msg}");

    let dup = "probes:\n  - name: chk-dup\n    command: [a]\n  - name: chk-dup\n    command: [b]\n";
    let msg = err_text(parse_spec(dup));
    assert!(msg.contains("chk-dup"), "{msg}");

    let shadow = "tools: [uv]\nprobes:\n  - name: uv\n    command: [uv, --version]\n";
    let msg = err_text(parse_spec(shadow));
    assert!(msg.contains("uv"), "{msg}");

    // Neighbour: a probe naming an executable outside the catalog is valid,
    // and so is a probe whose name matches an UNselected catalog tool's.
    let ok = parse_spec(
        "tools: [uv]\nprobes:\n  - name: terraform\n    command: [terraform, version]\n",
    )
    .expect("non-catalog probe is valid");
    assert_eq!(
        ok.probes,
        vec![probe("terraform", &["terraform", "version"])]
    );
}

#[test]
fn spec_unknown_top_level_key_is_refused() {
    let msg = err_text(parse_spec("tools: [uv]\nimage: ghcr.io/x/y\n"));
    assert!(msg.contains("image"), "{msg}");
    let msg = err_text(parse_spec(
        "probes:\n  - name: a\n    command: [a]\n    shell: true\n",
    ));
    assert!(msg.contains("shell"), "{msg}");
}

fn write(path: &Path, body: &str) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(path, body).unwrap();
}

#[test]
fn load_spec_refuses_dockerfile_paths_outside_the_spec_dir() {
    let tmp = tempfile::tempdir().unwrap();
    let outer = tmp.path().join("outer");
    write(&outer.join("Dockerfile"), GOOD_DOCKERFILE);
    let spec_path = outer.join("inner").join("runner.yaml");

    write(&spec_path, "dockerfile: ../Dockerfile\n");
    let msg = err_text(load_spec(&spec_path));
    assert!(msg.contains("../Dockerfile"), "{msg}");

    let abs = outer.join("Dockerfile");
    write(&spec_path, &format!("dockerfile: {}\n", abs.display()));
    let msg = err_text(load_spec(&spec_path));
    assert!(msg.contains(&abs.display().to_string()), "{msg}");

    write(&spec_path, "dockerfile: \"  \"\n");
    assert!(
        load_spec(&spec_path).is_err(),
        "blank dockerfile is refused"
    );

    write(&spec_path, "dockerfile: Dockerfile.nope\n");
    let msg = err_text(load_spec(&spec_path));
    assert!(msg.contains("Dockerfile.nope"), "{msg}");
}

#[test]
fn load_spec_refuses_a_dockerfile_not_built_on_the_runner_arg() {
    let tmp = tempfile::tempdir().unwrap();
    write(
        &tmp.path().join("Dockerfile.runner"),
        "FROM debian:bookworm\nRUN true\n",
    );
    let spec_path = tmp.path().join("runner.yaml");
    write(&spec_path, "tools: [uv]\ndockerfile: Dockerfile.runner\n");
    let msg = err_text(load_spec(&spec_path));
    assert!(msg.contains("debian:bookworm"), "{msg}");
}

fn load_with_dockerfile(body: &str) -> anyhow::Result<curie::repo_runner::LoadedSpec> {
    let tmp = tempfile::tempdir().unwrap();
    write(&tmp.path().join("Dockerfile.runner"), body);
    let spec_path = tmp.path().join("runner.yaml");
    write(&spec_path, "tools: [uv]\ndockerfile: Dockerfile.runner\n");
    load_spec(&spec_path)
}

#[test]
fn load_spec_accepts_a_final_stage_built_on_an_earlier_runner_stage() {
    load_with_dockerfile(
        "ARG CURIE_RUNNER_IMAGE\nFROM ${CURIE_RUNNER_IMAGE} AS base\nRUN true\nFROM base\nRUN true\n",
    )
    .expect("final stage descends from the runner through base");
}

#[test]
fn load_spec_accepts_the_builder_pattern() {
    load_with_dockerfile(
        "ARG CURIE_RUNNER_IMAGE\nFROM ${CURIE_RUNNER_IMAGE} AS base\nFROM golang:1.23 AS build\nRUN true\nFROM base\nCOPY --from=build /x /x\n",
    )
    .expect("a builder stage may be foreign when the final stage descends from the runner");
}

#[test]
fn load_spec_accepts_transitive_descent_with_case_insensitive_stage_names() {
    load_with_dockerfile(
        "ARG CURIE_RUNNER_IMAGE\nFROM ${CURIE_RUNNER_IMAGE} AS Base\nFROM base AS mid\nFROM MID\n",
    )
    .expect("stage names match case-insensitively and transitively");
}

#[test]
fn load_spec_accepts_a_single_stage_runner_dockerfile() {
    load_with_dockerfile("ARG CURIE_RUNNER_IMAGE\nFROM ${CURIE_RUNNER_IMAGE}\nRUN true\n")
        .expect("single stage on the runner image");
}

#[test]
fn load_spec_accepts_a_heredoc_body_that_looks_like_dockerfile_or_python_syntax() {
    load_with_dockerfile(
        "ARG CURIE_RUNNER_IMAGE\nFROM ${CURIE_RUNNER_IMAGE}\nCOPY <<'EOF' /opt/x.py\nfrom pathlib import Path\nEOF\n",
    )
    .expect("a heredoc line starting with `from` is file content, not a stage");
}

#[test]
fn load_spec_accepts_a_runner_arg_dockerfile_with_empty_tools() {
    let tmp = tempfile::tempdir().unwrap();
    let df = tmp.path().join("ci").join("Dockerfile.runner");
    write(&df, GOOD_DOCKERFILE);
    let spec_path = tmp.path().join("runner.yaml");
    write(&spec_path, "dockerfile: ci/Dockerfile.runner\n");
    let loaded = load_spec(&spec_path).expect("empty tools with a dockerfile is valid");
    assert!(loaded.spec.tools.is_empty());
    assert_eq!(
        loaded.spec.dockerfile.as_deref(),
        Some("ci/Dockerfile.runner")
    );
    assert_eq!(loaded.dir, tmp.path().canonicalize().unwrap());
    assert_eq!(loaded.dockerfile, Some(df.canonicalize().unwrap()));
}

#[test]
fn load_spec_accepts_tools_only() {
    let tmp = tempfile::tempdir().unwrap();
    let spec_path = tmp.path().join("runner.yaml");
    write(&spec_path, "tools: [rust, uv]\n");
    let loaded = load_spec(&spec_path).expect("tools only is valid");
    assert_eq!(loaded.spec.tools, ["rust", "uv"]);
    assert!(loaded.dockerfile.is_none());
}

#[test]
fn load_spec_refuses_a_missing_spec_file() {
    let tmp = tempfile::tempdir().unwrap();
    let missing = tmp.path().join("nope.yaml");
    let msg = err_text(load_spec(&missing));
    assert!(msg.contains("nope.yaml"), "{msg}");
}

// ---------------------------------------------------------------------------
// 3b. The post-build layer check: the built image sits on the platform runner
// ---------------------------------------------------------------------------

// Trimmed from the recorded output of
// `docker buildx imagetools inspect <ref> --format '{{json .Image}}'` for a
// platform runner (base) and for an image this command built on it. Kept:
// architecture, os and rootfs, with the real first three base diff_ids; the
// built image carries those three plus two layers of its own. config and
// history are dropped.
fn platform_entry(arch: &str, diff_ids: &[&str]) -> Value {
    json!({
        "architecture": arch,
        "os": "linux",
        "rootfs": {"type": "layers", "diff_ids": diff_ids},
    })
}

const BASE_AMD64: [&str; 3] = [
    "sha256:1d69a5fd31932841d7825ef4780c06f008eea65aaa9f3110fe09d5832ed5c7d8",
    "sha256:374a61fb2659809c4af957114e4ca2a8d7adebfdd214c8d043011033696cf1ac",
    "sha256:d6089aa301166860bb4497f6697a270237fe37370859d24af18013cf90f3f98d",
];
const OWN_AMD64: [&str; 2] = [
    "sha256:00fa133ea0a276c247c9bf0eccfa8712312b6371d38d5207dae6fcc17630c571",
    "sha256:e2c24f6a9602a24bfe83617f191393f10a68000e691c762bebc5de289cfa5a19",
];

const BASE_ARM64: [&str; 3] = [
    "sha256:13a56b6535801be2adde694dabaf1c2df1d862a390661907dac821c42cd565cc",
    "sha256:e768ad7c531711e886b0fb43de5d0aeb924a20a204f546120b60cf8d6159b4cb",
    "sha256:f3d0adb45e3563b173da6251d9d3b24925f1ae0c6e2f17a5ef5c78a0557745bb",
];
const OWN_ARM64: [&str; 2] = [
    "sha256:0797aac9c06a08406068ab534426812afd4745fe62e4e221e12dd219ac6d865d",
    "sha256:2328d331693fc6f4b1550348d3b92675b0f6cc68faed5abaeac75e7c8e5506e9",
];

fn recorded_image_configs(with_own_layers: bool) -> String {
    let layers = |base: &[&str], own: &[&str]| -> Vec<String> {
        let mut v: Vec<String> = base.iter().map(|s| s.to_string()).collect();
        if with_own_layers {
            v.extend(own.iter().map(|s| s.to_string()));
        }
        v
    };
    let amd = layers(&BASE_AMD64, &OWN_AMD64);
    let arm = layers(&BASE_ARM64, &OWN_ARM64);
    json!({
        "linux/amd64": platform_entry("amd64", &amd.iter().map(String::as_str).collect::<Vec<_>>()),
        "linux/arm64": platform_entry("arm64", &arm.iter().map(String::as_str).collect::<Vec<_>>()),
    })
    .to_string()
}

fn ids(platform: &str, list: &[&str]) -> (String, Vec<String>) {
    (
        platform.to_string(),
        list.iter().map(|s| s.to_string()).collect(),
    )
}

fn id_map(entries: Vec<(String, Vec<String>)>) -> BTreeMap<String, Vec<String>> {
    entries.into_iter().collect()
}

#[test]
fn layer_check_accepts_the_real_shaped_pair() {
    let base = platform_diff_ids(&recorded_image_configs(false)).expect("base parses");
    let image = platform_diff_ids(&recorded_image_configs(true)).expect("image parses");
    assert_eq!(base["linux/amd64"].len(), 3);
    assert_eq!(image["linux/arm64"].len(), 5);
    check_builds_on_base(&base, &image).expect("image extends the base on both platforms");
}

#[test]
fn layer_check_accepts_equal_layers_and_extra_image_layers() {
    let base = id_map(vec![
        ids("linux/amd64", &["sha256:a", "sha256:b"]),
        ids("linux/arm64", &["sha256:c", "sha256:d"]),
    ]);
    check_builds_on_base(&base, &base).expect("identical layers are a prefix");
    let image = id_map(vec![
        ids("linux/amd64", &["sha256:a", "sha256:b", "sha256:x"]),
        ids(
            "linux/arm64",
            &["sha256:c", "sha256:d", "sha256:y", "sha256:z"],
        ),
    ]);
    check_builds_on_base(&base, &image).expect("extra image layers keep the prefix");
}

#[test]
fn layer_check_refuses_a_first_layer_that_differs_on_arm64_only() {
    let base = platform_diff_ids(&recorded_image_configs(false)).unwrap();
    let mut image = platform_diff_ids(&recorded_image_configs(true)).unwrap();
    image.get_mut("linux/arm64").unwrap()[0] = "sha256:deadbeef".to_string();
    let msg = err_text(check_builds_on_base(&base, &image));
    assert!(msg.contains("linux/arm64"), "{msg}");
    assert!(!msg.contains("linux/amd64"), "{msg}");
    assert!(msg.contains("${CURIE_RUNNER_IMAGE}"), "{msg}");
}

#[test]
fn layer_check_refuses_an_image_missing_a_platform() {
    let base = platform_diff_ids(&recorded_image_configs(false)).unwrap();
    let mut image = platform_diff_ids(&recorded_image_configs(true)).unwrap();
    image.remove("linux/arm64");
    let msg = err_text(check_builds_on_base(&base, &image));
    assert!(msg.contains("linux/arm64"), "{msg}");
    assert!(msg.contains("${CURIE_RUNNER_IMAGE}"), "{msg}");
}

#[test]
fn layer_check_refuses_a_base_longer_than_the_image() {
    let base = platform_diff_ids(&recorded_image_configs(true)).unwrap();
    let image = platform_diff_ids(&recorded_image_configs(false)).unwrap();
    let msg = err_text(check_builds_on_base(&base, &image));
    assert!(msg.contains("linux/"), "{msg}");
    assert!(msg.contains("${CURIE_RUNNER_IMAGE}"), "{msg}");
}

#[test]
fn platform_diff_ids_ignores_platforms_other_than_the_two_built() {
    let mut v: Value = serde_json::from_str(&recorded_image_configs(true)).unwrap();
    v["unknown/unknown"] = platform_entry("unknown", &["sha256:ignored"]);
    let parsed = platform_diff_ids(&v.to_string()).expect("extra key ignored");
    let keys: Vec<&str> = parsed.keys().map(String::as_str).collect();
    assert_eq!(keys, ["linux/amd64", "linux/arm64"]);
}

#[test]
fn platform_diff_ids_refuses_an_entry_without_rootfs() {
    let mut v: Value = serde_json::from_str(&recorded_image_configs(true)).unwrap();
    v["linux/arm64"].as_object_mut().unwrap().remove("rootfs");
    let msg = err_text(platform_diff_ids(&v.to_string()));
    assert!(
        msg.contains("linux/arm64") || msg.contains("rootfs"),
        "{msg}"
    );
}

#[test]
fn platform_diff_ids_refuses_a_non_object() {
    err_text(platform_diff_ids("[]"));
    err_text(platform_diff_ids("\"linux/amd64\""));
}

// ---------------------------------------------------------------------------
// 3. Probe order and the generated Dockerfiles
// ---------------------------------------------------------------------------

#[test]
fn probes_and_entries_follow_catalog_order_then_declared_probes() {
    let mut s = spec(&["docker-cli", "uv"]);
    s.probes = vec![
        probe("zz", &["zz", "-v"]),
        probe("aa", &["aa", "--version"]),
    ];
    let names: Vec<&str> = selected_entries(&s)
        .iter()
        .map(|e| e.name.as_str())
        .collect();
    assert_eq!(names, ["uv", "docker-cli"]);
    let runs = probe_runs(&s);
    assert_eq!(
        runs,
        vec![
            ProbeRun {
                name: "uv".into(),
                command: strings(&["uv", "--version"]),
                source: "catalog".into()
            },
            ProbeRun {
                name: "docker-cli".into(),
                command: strings(&["docker", "--version"]),
                source: "catalog".into()
            },
            ProbeRun {
                name: "zz".into(),
                command: strings(&["zz", "-v"]),
                source: "probe".into()
            },
            ProbeRun {
                name: "aa".into(),
                command: strings(&["aa", "--version"]),
                source: "probe".into()
            },
        ]
    );
}

fn has_env(text: &str, key: &str, value: &str) -> bool {
    text.contains(&format!("{key}={value}")) || text.contains(&format!("{key}=\"{value}\""))
}

fn pos(text: &str, needle: &str) -> usize {
    text.find(needle)
        .unwrap_or_else(|| panic!("Dockerfile must contain {needle:?}:\n{text}"))
}

#[test]
fn catalog_dockerfile_layers_rust_uv_pnpm_build_essential() {
    let text = catalog_dockerfile(&spec(&["rust", "uv", "pnpm", "build-essential"]));
    assert!(text.contains("FROM ${CURIE_PLATFORM_RUNNER}"), "{text}");
    assert!(text.contains("TARGETARCH"), "{text}");
    for (tool, amd64, arm64) in [("uv", UV_AMD64, UV_ARM64), ("rust", RUST_AMD64, RUST_ARM64)] {
        assert!(
            text.contains(&format!("@sha256:{amd64}")),
            "{tool} amd64 pin:\n{text}"
        );
        assert!(
            text.contains(&format!("@sha256:{arm64}")),
            "{tool} arm64 pin:\n{text}"
        );
        assert!(text.contains(&format!("curie-tool-{tool}-amd64")), "{text}");
        assert!(text.contains(&format!("curie-tool-{tool}-arm64")), "{text}");
        assert!(
            text.contains(&format!(
                "FROM curie-tool-{tool}-${{TARGETARCH}} AS curie-tool-{tool}"
            )),
            "{tool} selector stage:\n{text}"
        );
    }
    assert!(
        text.contains("rustup component add rustfmt clippy"),
        "{text}"
    );
    assert!(
        text.contains(&format!("--checksum=sha256:{PNPM_SHA}")),
        "{text}"
    );
    assert!(text.contains("apt-get install"), "{text}");
    for pkg in ["build-essential", "cmake", "pkg-config"] {
        assert!(text.contains(pkg), "apt package {pkg}:\n{text}");
    }
    assert!(
        has_env(&text, "UV_CACHE_DIR", "/home/runner/.cache/uv"),
        "{text}"
    );
    assert!(
        has_env(&text, "CARGO_HOME", "/home/runner/.cargo"),
        "{text}"
    );
    assert!(
        has_env(&text, "PNPM_HOME", "/home/runner/.local/share/pnpm"),
        "{text}"
    );
    assert!(
        text.trim_end().ends_with("USER 1000:1000"),
        "the image must drop back to the runner user last:\n{text}"
    );
    // Catalog order, not spec order: the uv layer precedes the rust layer.
    let runner_from = pos(&text, "FROM ${CURIE_PLATFORM_RUNNER}");
    let uv_layer = runner_from + pos(&text[runner_from..], "--from=curie-tool-uv");
    let rust_layer = runner_from + pos(&text[runner_from..], "rustup component add");
    assert!(uv_layer < rust_layer, "uv layer must precede rust:\n{text}");
    // docker-cli was not selected.
    for leak in [
        DOCKER_AMD64,
        DOCKER_ARM64,
        "docker-compose",
        "DOCKER_CONFIG",
    ] {
        assert!(
            !text.contains(leak),
            "unselected docker-cli leaked {leak}:\n{text}"
        );
    }
}

#[test]
fn catalog_dockerfile_docker_cli_and_uv_only() {
    let text = catalog_dockerfile(&spec(&["docker-cli", "uv"]));
    assert!(text.contains("FROM ${CURIE_PLATFORM_RUNNER}"), "{text}");
    assert!(text.contains(&format!("@sha256:{DOCKER_AMD64}")), "{text}");
    assert!(text.contains(&format!("@sha256:{DOCKER_ARM64}")), "{text}");
    assert!(text.contains(&format!("@sha256:{UV_AMD64}")), "{text}");
    assert!(
        text.contains("docker-compose"),
        "compose plugin copy:\n{text}"
    );
    assert!(
        text.contains(
            "COPY --from=curie-tool-docker-cli /usr/local/libexec/docker/cli-plugins/docker-buildx"
        ),
        "buildx plugin copy:\n{text}"
    );
    assert!(
        text.contains("/usr/local/libexec/docker/cli-plugins/docker-buildx /usr/local/libexec/docker/cli-plugins/docker-buildx"),
        "buildx plugin lands at the same path:\n{text}"
    );
    let verify = text
        .lines()
        .find(|l| l.starts_with("RUN docker --version"))
        .unwrap_or_else(|| panic!("docker verification RUN:\n{text}"));
    assert!(verify.contains("docker buildx version"), "{verify}");
    assert!(
        has_env(&text, "DOCKER_CONFIG", "/home/runner/.docker"),
        "{text}"
    );
    assert!(text.trim_end().ends_with("USER 1000:1000"), "{text}");
    for leak in [
        "rustup",
        "cargo",
        RUST_AMD64,
        RUST_ARM64,
        "pnpm",
        PNPM_SHA,
        "apt-get",
        "cmake",
        "CARGO_HOME",
        "PNPM_HOME",
        DEBIAN_AMD64,
        DEBIAN_ARM64,
    ] {
        assert!(
            !text.contains(leak),
            "unselected tool leaked {leak:?}:\n{text}"
        );
    }
}

fn instructions(text: &str) -> Vec<String> {
    text.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(str::to_string)
        .collect()
}

/// The single JSON line the probe stage writes via the probes COPY heredoc.
fn probe_input(text: &str) -> Value {
    let lines: Vec<&str> = text.lines().collect();
    let at = lines
        .iter()
        .position(|l| *l == "COPY <<'CURIE_PROBES' /tmp/curie-probes.json")
        .unwrap_or_else(|| panic!("no probes COPY heredoc:\n{text}"));
    assert_eq!(
        lines[at + 2],
        "CURIE_PROBES",
        "the probes heredoc is exactly one JSON line:\n{text}"
    );
    serde_json::from_str(lines[at + 1])
        .unwrap_or_else(|e| panic!("probe line not JSON: {e}\n{text}"))
}

#[test]
fn finalize_dockerfile_probes_then_adds_only_the_manifest_layer() {
    let mut with_docker = spec(&["docker-cli", "uv"]);
    with_docker.probes = vec![probe("tf", &["terraform", "version"])];
    let uv_only = spec(&["uv"]);
    for s in [&with_docker, &uv_only] {
        let docker = s.tools.iter().any(|t| t == "docker-cli");
        let input = probe_input(&finalize_dockerfile(s));
        assert_eq!(
            input,
            json!({"docker_cli": docker, "probes": serde_json::to_value(probe_runs(s)).unwrap()})
        );
    }
    assert_ne!(
        probe_input(&finalize_dockerfile(&with_docker)),
        probe_input(&finalize_dockerfile(&uv_only))
    );

    let text = finalize_dockerfile(&with_docker);
    assert!(
        text.contains("FROM curie-composed AS curie-probe"),
        "{text}"
    );
    // The probe list must not travel through ARG or env, where an operator ENV can override it.
    assert!(!text.contains("ARG CURIE_TOOL_PROBES"), "{text}");
    assert!(!text.contains("ARG CURIE_DOCKER_CLI"), "{text}");
    assert!(
        text.contains(PROBE_SCRIPT.trim_end()),
        "the probe script must be embedded verbatim:\n{text}"
    );
    assert!(
        !text.lines().any(|l| l.trim_start().starts_with("RUN <<")),
        "the probe must not be a RUN heredoc:\n{text}"
    );
    let probes_copy = "COPY <<'CURIE_PROBES' /tmp/curie-probes.json";
    let copy_line = "COPY <<'CURIE_PROBE' /tmp/curie-probe.py";
    let run_line =
        "RUN python3 /tmp/curie-probe.py /tmp/curie-probes.json /tmp/curie-tool-manifest.json";
    let probe_from = pos(&text, "FROM curie-composed AS curie-probe");
    let probes_at = pos(&text, probes_copy);
    let copy_at = pos(&text, copy_line);
    let script_at = pos(&text, PROBE_SCRIPT.trim_end());
    let run_at = pos(&text, run_line);
    assert!(
        probe_from < probes_at && probes_at < copy_at && copy_at < script_at && script_at < run_at,
        "probe stage order is FROM, probes COPY, script COPY, script, run:\n{text}"
    );
    assert!(
        text[script_at..run_at].lines().any(|l| l == "CURIE_PROBE"),
        "the COPY heredoc must be terminated by CURIE_PROBE before the run:\n{text}"
    );
    // The probe runs as the sandbox user whatever the operator Dockerfile's last USER was.
    let user_at = text[probe_from..run_at]
        .lines()
        .position(|l| l.trim() == "USER 1000:1000");
    assert!(
        user_at.is_some(),
        "the probe stage must set USER 1000:1000 before running the probe:\n{text}"
    );
    let lines = instructions(&text);
    let last_from = lines
        .iter()
        .rposition(|l| l.to_ascii_uppercase().starts_with("FROM "))
        .expect("has a FROM");
    assert_eq!(lines[last_from], "FROM curie-composed", "{text}");
    let tail = &lines[last_from + 1..];
    assert_eq!(
        tail.len(),
        1,
        "the final stage adds exactly one layer: {tail:?}"
    );
    assert!(tail[0].starts_with("COPY --from=curie-probe "), "{tail:?}");
    assert!(tail[0].ends_with(MANIFEST_PATH), "{tail:?}");
    assert_eq!(MANIFEST_PATH, "/etc/curie/tool-manifest.json");
}

// ---------------------------------------------------------------------------
// 4. Bake definition
// ---------------------------------------------------------------------------

#[test]
fn bake_escape_doubles_interpolation_openers_only() {
    assert_eq!(bake_escape("a${B}c%{d}e"), "a$${B}c%%{d}e");
    assert_eq!(bake_escape("$HOME 50% {x}"), "$HOME 50% {x}");
}

fn plan(s: RepoRunnerSpec, dockerfile: Option<(PathBuf, String)>) -> BakePlan {
    BakePlan {
        image_ref: format!("localhost:5000/{IMAGE_NAME}:r-0123456789abcdef"),
        base_ref: format!("ghcr.io/curie-eng/curie-runner@{}", digest('b')),
        spec: s,
        dockerfile,
        empty_context: PathBuf::from("/tmp/curie-empty-context"),
    }
}

fn visit_strings<'a>(v: &'a Value, out: &mut Vec<&'a str>) {
    match v {
        Value::String(s) => out.push(s),
        Value::Array(items) => items.iter().for_each(|i| visit_strings(i, out)),
        Value::Object(m) => {
            for (k, val) in m {
                out.push(k);
                visit_strings(val, out);
            }
        }
        _ => {}
    }
}

fn assert_escaped(def: &Value) {
    let mut all = Vec::new();
    visit_strings(def, &mut all);
    for s in all {
        for (opener, needle) in [(b'$', "${"), (b'%', "%{")] {
            for (i, _) in s.match_indices(needle) {
                assert!(
                    i > 0 && s.as_bytes()[i - 1] == opener,
                    "unescaped {needle} in bake string: {s:?}"
                );
            }
        }
    }
}

fn unescape(s: &str) -> String {
    s.replace("$${", "${").replace("%%{", "%{")
}

fn targets(def: &Value) -> &serde_json::Map<String, Value> {
    def["target"]
        .as_object()
        .expect("bake definition has a target map")
}

fn assert_common(def: &Value, s: &RepoRunnerSpec, image_ref: &str) {
    assert_escaped(def);
    let t = targets(def);
    for (name, target) in t {
        assert_eq!(
            target["platforms"],
            json!(["linux/amd64", "linux/arm64"]),
            "{name} platforms"
        );
        if name != BAKE_TARGET {
            assert!(
                target
                    .get("tags")
                    .is_none_or(|v| v.as_array().is_some_and(|a| a.is_empty())),
                "{name} must not be tagged: {target}"
            );
            let output = target
                .get("output")
                .map(Value::to_string)
                .unwrap_or_default();
            assert!(
                !output.contains("registry") && !output.contains("push"),
                "{name} must not be pushed: {target}"
            );
        }
    }
    let runner = &t[BAKE_TARGET];
    assert_eq!(runner["tags"], json!([image_ref]));
    let output = serde_json::to_string(&runner["output"]).unwrap();
    assert!(output.contains("type=registry"), "{output}");
    let args = runner.get("args").and_then(Value::as_object);
    for name in ["CURIE_TOOL_PROBES", "CURIE_DOCKER_CLI"] {
        assert!(
            args.is_none_or(|a| !a.contains_key(name)),
            "{name} must not be a build arg: {runner}"
        );
    }
    let inline = runner["dockerfile-inline"]
        .as_str()
        .expect("dockerfile-inline");
    assert_eq!(inline, bake_escape(&finalize_dockerfile(s)));
    let docker = s.tools.iter().any(|t| t == "docker-cli");
    assert_eq!(
        probe_input(&unescape(inline)),
        json!({"docker_cli": docker, "probes": serde_json::to_value(probe_runs(s)).unwrap()})
    );
    assert_eq!(
        t["curie-catalog"]["args"]["CURIE_PLATFORM_RUNNER"],
        json!(format!("ghcr.io/curie-eng/curie-runner@{}", digest('b')))
    );
}

#[test]
fn bake_definition_catalog_only_links_runner_to_catalog() {
    let mut s = spec(&["docker-cli", "uv"]);
    s.probes = vec![probe("tf", &["terraform", "version"])];
    let p = plan(s.clone(), None);
    let def = bake_definition(&p);
    let t = targets(&def);
    let mut names: Vec<&str> = t.keys().map(String::as_str).collect();
    names.sort();
    assert_eq!(names, ["curie-catalog", "curie-runner"]);
    assert_eq!(BAKE_TARGET, "curie-runner");
    assert_eq!(
        t["curie-runner"]["contexts"]["curie-composed"],
        json!("target:curie-catalog")
    );
    assert_common(&def, &s, &p.image_ref);
}

#[test]
fn bake_definition_with_dockerfile_inserts_the_repository_target() {
    let s = RepoRunnerSpec {
        tools: vec![],
        dockerfile: Some("ci/Dockerfile.runner".into()),
        probes: vec![],
    };
    let ctx = PathBuf::from("/work/repo");
    let p = plan(
        s.clone(),
        Some((ctx.clone(), "ci/Dockerfile.runner".into())),
    );
    let def = bake_definition(&p);
    let t = targets(&def);
    let mut names: Vec<&str> = t.keys().map(String::as_str).collect();
    names.sort();
    assert_eq!(names, ["curie-catalog", "curie-repository", "curie-runner"]);
    let repo = &t["curie-repository"];
    assert_eq!(repo["args"]["CURIE_RUNNER_IMAGE"], json!("curie-catalog"));
    assert_eq!(
        repo["contexts"]["curie-catalog"],
        json!("target:curie-catalog")
    );
    assert_eq!(repo["context"], json!(ctx.display().to_string()));
    assert_eq!(repo["dockerfile"], json!("ci/Dockerfile.runner"));
    assert_eq!(
        t["curie-runner"]["contexts"]["curie-composed"],
        json!("target:curie-repository")
    );
    assert_common(&def, &s, &p.image_ref);
}

// ---------------------------------------------------------------------------
// 5. Content tag and digests
// ---------------------------------------------------------------------------

#[test]
fn input_tag_is_stable_order_insensitive_and_input_sensitive() {
    let base = digest('b');
    let s = spec(&["uv", "rust"]);
    let tag = input_tag(&s, None, &base);
    assert_eq!(tag, input_tag(&s, None, &base), "stable");
    assert_eq!(tag.len(), 18, "{tag}");
    assert!(tag.starts_with("r-"), "{tag}");
    assert!(
        tag[2..]
            .chars()
            .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c)),
        "{tag}"
    );
    assert_eq!(
        tag,
        input_tag(&spec(&["rust", "uv"]), None, &base),
        "tool order is not input"
    );

    let mut more = spec(&["uv", "rust", "pnpm"]);
    assert_ne!(tag, input_tag(&more, None, &base), "tools");
    more = s.clone();
    more.probes = vec![probe("tf", &["terraform", "version"])];
    assert_ne!(tag, input_tag(&more, None, &base), "probes");
    let mut other_probe = s.clone();
    other_probe.probes = vec![probe("tf", &["terraform", "-version"])];
    assert_ne!(
        input_tag(&more, None, &base),
        input_tag(&other_probe, None, &base),
        "probe argv"
    );
    let df = digest('d');
    assert_ne!(tag, input_tag(&s, Some(&df), &base), "dockerfile digest");
    assert_ne!(
        input_tag(&s, Some(&df), &base),
        input_tag(&s, Some(&digest('e')), &base),
        "dockerfile digest value"
    );
    assert_ne!(tag, input_tag(&s, None, &digest('c')), "base digest");
}

#[test]
fn bare_digest_strips_the_repository_and_tag() {
    let d = digest('a');
    assert_eq!(bare_digest(&format!("ghcr.io/acme/runner@{d}")).unwrap(), d);
    assert_eq!(
        bare_digest(&format!("localhost:5000/acme/runner:r-1@{d}")).unwrap(),
        d
    );
    assert!(bare_digest("ghcr.io/acme/runner:latest").is_err());
    assert!(bare_digest("ghcr.io/acme/runner@sha256:abc").is_err());
}

const AMD64_DIGEST: &str =
    "sha256:8bf50e97c858530deb46efcf985a0ce548f1d8439bee1c5ae39af24db8e1f327";
const ARM64_DIGEST: &str =
    "sha256:e6089d9710c40f8cf78e59440f8c67160698ffb710822892691a3c5bba58af80";

/// The shape recorded from `docker buildx imagetools inspect --raw` of a
/// pushed bake output (prototype on the k8 builder): two platform images plus
/// one buildx attestation manifest per platform, platform unknown/unknown.
fn recorded_index(manifests: Vec<Value>) -> String {
    json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.index.v1+json",
        "manifests": manifests,
    })
    .to_string()
}

fn platform_manifest(digest: &str, arch: &str, variant: Option<&str>) -> Value {
    let mut platform = json!({"architecture": arch, "os": "linux"});
    if let Some(v) = variant {
        platform["variant"] = json!(v);
    }
    json!({
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "digest": digest,
        "size": 1234,
        "platform": platform,
    })
}

fn attestation(digest: &str, of: &str) -> Value {
    json!({
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "digest": digest,
        "size": 566,
        "annotations": {
            "vnd.docker.reference.digest": of,
            "vnd.docker.reference.type": "attestation-manifest",
        },
        "platform": {"architecture": "unknown", "os": "unknown"},
    })
}

fn recorded() -> Vec<Value> {
    vec![
        platform_manifest(AMD64_DIGEST, "amd64", None),
        platform_manifest(ARM64_DIGEST, "arm64", None),
        attestation(&digest('1'), AMD64_DIGEST),
        attestation(&digest('2'), ARM64_DIGEST),
    ]
}

#[test]
fn platform_digests_from_a_recorded_buildx_index() {
    let got = platform_digests_from_index(&recorded_index(recorded())).expect("parses");
    assert_eq!(
        got,
        map(&[("linux/amd64", AMD64_DIGEST), ("linux/arm64", ARM64_DIGEST)])
    );
}

#[test]
fn platform_digests_accept_arm64_variant_v8() {
    let mut m = recorded();
    m[1] = platform_manifest(ARM64_DIGEST, "arm64", Some("v8"));
    let got = platform_digests_from_index(&recorded_index(m)).expect("v8 is arm64");
    assert_eq!(got["linux/arm64"], ARM64_DIGEST);
}

#[test]
fn platform_digests_refuse_missing_duplicate_and_non_index() {
    let mut missing = recorded();
    missing.remove(1);
    assert!(
        platform_digests_from_index(&recorded_index(missing)).is_err(),
        "no arm64"
    );

    let mut dup = recorded();
    dup.push(platform_manifest(&digest('9'), "amd64", None));
    assert!(
        platform_digests_from_index(&recorded_index(dup)).is_err(),
        "two amd64"
    );

    let single = json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {"mediaType": "application/vnd.oci.image.config.v1+json", "digest": digest('c'), "size": 10},
        "layers": [],
    })
    .to_string();
    assert!(
        platform_digests_from_index(&single).is_err(),
        "single manifest"
    );
}

// ---------------------------------------------------------------------------
// 6. The receipt
// ---------------------------------------------------------------------------

fn repository_runner_validator() -> jsonschema::Validator {
    let values = load_json("charts/curie/values.schema.json");
    let schema = json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "definitions": values["definitions"].clone(),
        "$ref": "#/definitions/repositoryRunner",
    });
    validator(&schema)
}

fn output(dockerfile: Option<String>, probes: Vec<ProbeDecl>) -> RepoRunnerBuildOutput {
    RepoRunnerBuildOutput {
        runner: RunnerRecord {
            tools: strings(&["uv", "docker-cli"]),
            image: format!(
                "localhost:5000/{IMAGE_NAME}:r-0123456789abcdef@{}",
                digest('a')
            ),
            base: digest('b'),
            platform_digests: map(&[("linux/amd64", AMD64_DIGEST), ("linux/arm64", ARM64_DIGEST)]),
            dockerfile,
            probes,
        },
        platform_runner: format!("ghcr.io/curie-eng/curie-runner@{}", digest('b')),
        catalog_version: CATALOG_VERSION,
        manifest_path: MANIFEST_PATH.to_string(),
    }
}

#[test]
fn receipt_runner_block_is_the_helm_repository_runner_shape() {
    let v = repository_runner_validator();

    let bare = serde_json::to_value(output(None, vec![])).unwrap();
    let runner = &bare["runner"];
    assert_eq!(
        runner["platformDigests"]["linux/amd64"],
        json!(AMD64_DIGEST)
    );
    assert!(runner.get("platform_digests").is_none(), "{runner}");
    assert!(runner.get("dockerfile").is_none(), "{runner}");
    assert!(runner.get("probes").is_none(), "{runner}");
    let errors: Vec<String> = v.iter_errors(runner).map(|e| e.to_string()).collect();
    assert!(errors.is_empty(), "{runner}\n{errors:?}");
    assert_eq!(bare["catalog_version"], json!(1));
    assert_eq!(bare["manifest_path"], json!(MANIFEST_PATH));

    let full = serde_json::to_value(output(
        Some("ci/Dockerfile.runner".into()),
        vec![probe("tf", &["terraform", "version"])],
    ))
    .unwrap();
    let runner = &full["runner"];
    assert_eq!(runner["dockerfile"], json!("ci/Dockerfile.runner"));
    assert_eq!(
        runner["probes"],
        json!([{"name": "tf", "command": ["terraform", "version"]}])
    );
    let errors: Vec<String> = v.iter_errors(runner).map(|e| e.to_string()).collect();
    assert!(errors.is_empty(), "{runner}\n{errors:?}");
}

// ---------------------------------------------------------------------------
// 7. The probe script, executed for real
// ---------------------------------------------------------------------------

fn python3() -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path)
        .map(|d| d.join("python3"))
        .find(|p| p.is_file())
}

#[cfg(unix)]
fn tool(dir: &Path, name: &str, body: &str) {
    use std::io::Write;
    use std::os::unix::fs::PermissionsExt;
    let path = dir.join(name);
    {
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(format!("#!/bin/sh\n{body}\n").as_bytes())
            .unwrap();
        f.sync_all().unwrap();
    }
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
}

struct ProbeOutcome {
    ok: bool,
    stderr: String,
    manifest: Option<Value>,
}

fn run_probe_script(
    python: &Path,
    bin: &Path,
    work: &Path,
    runs: &[ProbeRun],
    docker_cli: bool,
) -> ProbeOutcome {
    run_probe_script_env(python, bin, work, runs, docker_cli, false)
}

/// `poison` sets the old env channel to values that would hide failures and
/// flip the docker flag; the script must read only its two file arguments.
fn run_probe_script_env(
    python: &Path,
    bin: &Path,
    work: &Path,
    runs: &[ProbeRun],
    docker_cli: bool,
    poison: bool,
) -> ProbeOutcome {
    let script = work.join("probe.py");
    std::fs::write(&script, PROBE_SCRIPT).unwrap();
    let probes_path = work.join("probes.json");
    std::fs::write(
        &probes_path,
        json!({"docker_cli": docker_cli, "probes": runs}).to_string(),
    )
    .unwrap();
    let out_path = work.join("manifest.json");
    let _ = std::fs::remove_file(&out_path);
    let mut cmd = std::process::Command::new(python);
    cmd.arg(&script)
        .arg(&probes_path)
        .arg(&out_path)
        .env_clear()
        .env("PATH", bin);
    if poison {
        cmd.env("CURIE_TOOL_PROBES", "[]")
            .env("CURIE_DOCKER_CLI", "true")
            .env("CURIE_TOOL_MANIFEST_OUT", work.join("elsewhere.json"));
    }
    let output = cmd
        .stdin(std::process::Stdio::null())
        .output()
        .expect("python3 runs");
    let manifest = std::fs::read_to_string(&out_path)
        .ok()
        .map(|raw| serde_json::from_str(&raw).expect("manifest is JSON"));
    ProbeOutcome {
        ok: output.status.success(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        manifest,
    }
}

fn run(name: &str, command: &[&str], source: &str) -> ProbeRun {
    ProbeRun {
        name: name.into(),
        command: strings(command),
        source: source.into(),
    }
}

/// One test drives every case so the executables are written and run
/// sequentially (no concurrent fork while another test holds a script open).
#[cfg(unix)]
#[test]
fn probe_script_writes_a_valid_manifest_or_fails_naming_the_probe() {
    let Some(python) = python3() else {
        panic!("python3 is required to run this test and was not found on PATH (CI has it)");
    };
    let tmp = tempfile::tempdir().unwrap();
    let bin = tmp.path().join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    tool(&bin, "mytool", "echo 'mytool 1.2.3 (abc)'");
    tool(&bin, "quiet", "echo 'quiet version 4.5' >&2");
    tool(&bin, "plain", "echo ''\necho 'nightly'");
    tool(&bin, "broken", "echo 'broken 9.9.9'\nexit 3");
    tool(&bin, "silent", "exit 0");
    let work = tmp.path().join("work");
    std::fs::create_dir_all(&work).unwrap();

    // Success: versions extracted, absolute path, source and docker flag carried.
    let good = vec![
        run("my-tool", &["mytool", "--version"], "catalog"),
        run("quiet", &["quiet", "--version"], "probe"),
        run("plain", &["plain"], "probe"),
    ];
    let outcome = run_probe_script(&python, &bin, &work, &good, true);
    assert!(outcome.ok, "probe script must succeed: {}", outcome.stderr);
    let manifest = outcome.manifest.expect("manifest written");
    let schema = load_json("docs/schemas/tool-manifest.schema.json");
    let errors: Vec<String> = validator(&schema)
        .iter_errors(&manifest)
        .map(|e| e.to_string())
        .collect();
    assert!(errors.is_empty(), "{manifest}\n{errors:?}");
    assert_eq!(manifest["schema_version"], json!(1));
    assert_eq!(manifest["docker_cli"], json!(true));
    let tools = manifest["tools"].as_array().unwrap();
    assert_eq!(tools.len(), 3, "{manifest}");
    assert_eq!(tools[0]["name"], json!("my-tool"));
    assert_eq!(tools[0]["version"], json!("1.2.3"));
    assert_eq!(tools[0]["source"], json!("catalog"));
    let path = tools[0]["path"].as_str().unwrap();
    assert!(Path::new(path).is_absolute(), "{path}");
    assert_eq!(
        Path::new(path).canonicalize().unwrap(),
        bin.join("mytool").canonicalize().unwrap()
    );
    // Liveness: a version printed only on stderr counts.
    assert_eq!(tools[1]["name"], json!("quiet"));
    assert_eq!(tools[1]["version"], json!("4.5"));
    assert_eq!(tools[1]["source"], json!("probe"));
    // No dotted version: the first non-blank line.
    assert_eq!(tools[2]["version"], json!("nightly"));

    let outcome = run_probe_script(&python, &bin, &work, &good[..1], false);
    assert!(outcome.ok, "{}", outcome.stderr);
    assert_eq!(outcome.manifest.unwrap()["docker_cli"], json!(false));

    // Failures: exit nonzero, write nothing, name the probe on stderr.
    for (bad, why) in [
        (
            run("ghost", &["ghost-tool", "--version"], "probe"),
            "missing from PATH",
        ),
        (
            run("broken", &["broken", "--version"], "probe"),
            "nonzero exit",
        ),
        (run("silent", &["silent"], "catalog"), "no output"),
    ] {
        let mut runs = good.clone();
        runs.insert(1, bad.clone());
        let outcome = run_probe_script(&python, &bin, &work, &runs, false);
        assert!(!outcome.ok, "{why}: the probe script must fail");
        assert!(
            outcome.manifest.is_none(),
            "{why}: no manifest may be written"
        );
        assert!(
            outcome.stderr.contains(&format!("probe {}", bad.name)),
            "{why}: stderr must name probe {}: {}",
            bad.name,
            outcome.stderr
        );
    }

    // Env cannot override the file: with the old env channel set to "no probes"
    // and docker true, a missing executable in the file still fails the run.
    let ghost = vec![run("ghost", &["ghost-tool", "--version"], "probe")];
    let outcome = run_probe_script_env(&python, &bin, &work, &ghost, false, true);
    assert!(
        !outcome.ok,
        "env CURIE_TOOL_PROBES=[] must not hide a failing probe"
    );
    assert!(outcome.manifest.is_none(), "no manifest may be written");
    assert!(
        outcome.stderr.contains("probe ghost"),
        "stderr must name probe ghost: {}",
        outcome.stderr
    );
    // Liveness neighbour: same env, valid file; tools and docker_cli come from the file.
    let outcome = run_probe_script_env(&python, &bin, &work, &good[..1], false, true);
    assert!(outcome.ok, "{}", outcome.stderr);
    let manifest = outcome.manifest.expect("manifest written");
    assert_eq!(manifest["docker_cli"], json!(false), "{manifest}");
    assert_eq!(manifest["tools"].as_array().unwrap().len(), 1, "{manifest}");
}

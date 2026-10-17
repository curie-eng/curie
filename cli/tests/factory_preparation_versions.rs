//! Managed preparation upgrades and repository pins at the real process boundary.
//! All release payloads are local fixtures; no coding agent or server is required.
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tempfile::TempDir;

const CONTRACT: &str = "curie.factory-specification/v1";
const SKILL_PATH: &str = ".agents/skills/prepare-factory-ticket";

struct Fixture {
    temp: TempDir,
    home: PathBuf,
    repo: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("user home");
        let repo = temp.path().join("repository one");
        fs::create_dir_all(&home).unwrap();
        fs::create_dir_all(&repo).unwrap();
        Self { temp, home, repo }
    }

    fn run(&self, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_curie"))
            .env_clear()
            .env("HOME", &self.home)
            .env("USERPROFILE", &self.home)
            .env("XDG_CONFIG_HOME", self.home.join("config"))
            .env("XDG_CACHE_HOME", self.home.join("cache"))
            .env("PATH", self.temp.path().join("no-executables"))
            .env("NO_COLOR", "1")
            .env("CURIE_API_URL", "http://127.0.0.1:9")
            .env("HTTP_PROXY", "http://127.0.0.1:9")
            .env("HTTPS_PROXY", "http://127.0.0.1:9")
            .env("NO_PROXY", "127.0.0.1,localhost")
            .current_dir(&self.repo)
            .args(["--json", "factory", "prepare"])
            .args(args)
            .output()
            .expect("actual CLI process")
    }

    fn install(&self) {
        ok(&self.run(&["install", "--agent", "codex", "--global"]));
    }

    fn update(&self, source: &Path, extra: &[&str]) -> Output {
        let mut args = vec![
            "update",
            "--agent",
            "codex",
            "--global",
            "--source",
            source.to_str().unwrap(),
        ];
        args.extend_from_slice(extra);
        self.run(&args)
    }

    fn resolve(&self, repo: &Path, extra: &[&str]) -> Output {
        let mut args = vec![
            "resolve",
            "--agent",
            "codex",
            "--global",
            "--repo",
            repo.to_str().unwrap(),
        ];
        args.extend_from_slice(extra);
        self.run(&args)
    }

    fn release(&self, version: &str, contract: &str) -> PathBuf {
        let dir = self.temp.path().join(format!("release-{version}"));
        fs::create_dir_all(&dir).unwrap();
        let files: serde_json::Map<String, Value> = ["SKILL.md", "references/specification.md"]
            .into_iter()
            .map(|path| {
                let content = format!("# {path}\nRelease {version}\n");
                (path.to_owned(), json!({"sha256":Sha256::digest(content.as_bytes()).iter().map(|byte| format!("{byte:02x}")).collect::<String>(),"content":content}))
            }).collect();
        fs::write(
            dir.join("release.json"),
            serde_json::to_vec_pretty(&json!({
                "schema_version":1,"preparation_version":version,"contract_versions":[contract],"files":files
            }))
            .unwrap(),
        )
        .unwrap();
        dir
    }

    fn pin(&self, repo: &Path, version: &str, contract: &str) {
        fs::write(repo.join(".curie-factory.json"), serde_json::to_vec_pretty(&json!({
            "schema_version":1,"target":{"deployment_id":"22222222-2222-4222-8222-222222222222","bundle_sha256":"a".repeat(64),"contract_version":contract},
            "preparation_version":version
        })).unwrap()).unwrap();
    }
}

fn ok(output: &Output) -> Value {
    assert!(
        output.status.success(),
        "stdout: {}; stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let value: Value = serde_json::from_slice(&output.stdout).expect("one JSON result");
    assert!(value.is_object(), "{value}");
    value
}

fn refused(output: &Output) {
    assert!(
        !output.status.success(),
        "unexpected success: {}",
        String::from_utf8_lossy(&output.stdout)
    );
}

fn snapshot(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn walk(root: &Path, path: &Path, result: &mut BTreeMap<PathBuf, Vec<u8>>) {
        for entry in fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                walk(root, &entry.path(), result);
            } else {
                result.insert(
                    entry.path().strip_prefix(root).unwrap().to_owned(),
                    fs::read(entry.path()).unwrap(),
                );
            }
        }
    }
    let mut result = BTreeMap::new();
    walk(root, root, &mut result);
    result
}

#[test]
fn check_is_read_only_then_unchanged_managed_files_upgrade() {
    let f = Fixture::new();
    f.install();
    let release = f.release("1.1.0", CONTRACT);
    let before = snapshot(&f.home);
    let check = ok(&f.update(&release, &["--check"]));
    assert_eq!(check["status"], "update-available");
    assert_eq!(check["available_version"], "1.1.0");
    assert_eq!(
        snapshot(&f.home),
        before,
        "checking must not stage or change receipt/cache"
    );
    let update = ok(&f.update(&release, &[]));
    assert_eq!(update["status"], "updated");
    assert_eq!(
        fs::read_to_string(f.home.join(SKILL_PATH).join("SKILL.md")).unwrap(),
        "# SKILL.md\nRelease 1.1.0\n"
    );
    ok(&f.update(&release, &[]));
}

#[test]
fn edited_managed_files_are_preserved_without_partial_update() {
    let f = Fixture::new();
    f.install();
    fs::write(
        f.home.join(SKILL_PATH).join("SKILL.md"),
        "local customization\n",
    )
    .unwrap();
    let before = snapshot(&f.home);
    refused(&f.update(&f.release("1.1.0", CONTRACT), &[]));
    assert_eq!(snapshot(&f.home), before);
}

#[test]
fn separate_preferences_and_unrelated_files_survive_upgrade() {
    let f = Fixture::new();
    f.install();
    let preferences = f.home.join(".curie/factory-preparation.local.json");
    fs::create_dir_all(preferences.parent().unwrap()).unwrap();
    fs::write(&preferences, "{\"output_directory\":\"my drafts\"}\n").unwrap();
    let extra = f.home.join(SKILL_PATH).join("my-notes.md");
    fs::write(&extra, "private notes\n").unwrap();
    ok(&f.update(&f.release("1.1.0", CONTRACT), &[]));
    assert_eq!(
        fs::read_to_string(&preferences).unwrap(),
        "{\"output_directory\":\"my drafts\"}\n"
    );
    assert_eq!(fs::read_to_string(&extra).unwrap(), "private notes\n");
}

#[test]
fn rollback_restores_previous_payload_and_receipt() {
    let f = Fixture::new();
    f.install();
    let before = snapshot(&f.home.join(SKILL_PATH));
    ok(&f.update(&f.release("1.1.0", CONTRACT), &[]));
    ok(&f.run(&["update", "--agent", "codex", "--global", "--rollback"]));
    assert_eq!(snapshot(&f.home.join(SKILL_PATH)), before);
    // A successful rollback must also restore receipt hashes, allowing a new upgrade.
    ok(&f.update(&f.release("1.2.0", CONTRACT), &[]));
}

#[test]
fn invalid_or_interrupted_download_does_not_change_active_installation() {
    let f = Fixture::new();
    f.install();
    let before = snapshot(&f.home);
    let release = f.release("1.1.0", CONTRACT);
    fs::write(
        release.join("release.json"),
        "{\"schema_version\":1,\"files\":[",
    )
    .unwrap();
    refused(&f.update(&release, &[]));
    assert_eq!(snapshot(&f.home), before);
    refused(&f.run(&[
        "update",
        "--agent",
        "codex",
        "--global",
        "--release-url",
        "https://127.0.0.1:9/release.json",
    ]));
    assert_eq!(snapshot(&f.home), before);
}

#[test]
fn two_repositories_resolve_different_cached_versions_under_one_installation() {
    let f = Fixture::new();
    f.install();
    let other = f.temp.path().join("repository two");
    fs::create_dir(&other).unwrap();
    f.pin(&f.repo, "1.1.0", CONTRACT);
    f.pin(&other, "2.0.0", CONTRACT);
    let first = f.release("1.1.0", CONTRACT);
    let second = f.release("2.0.0", CONTRACT);
    let active = snapshot(&f.home.join(SKILL_PATH));
    let a = ok(&f.resolve(&f.repo, &["--source", first.to_str().unwrap()]));
    let b = ok(&f.resolve(&other, &["--source", second.to_str().unwrap()]));
    assert_eq!(a["preparation_version"], "1.1.0");
    assert_eq!(b["preparation_version"], "2.0.0");
    assert_eq!(a["contract_version"], CONTRACT);
    assert_eq!(
        fs::read_to_string(a["instructions_path"].as_str().unwrap()).unwrap(),
        "# SKILL.md\nRelease 1.1.0\n"
    );
    assert_eq!(
        fs::read_to_string(b["instructions_path"].as_str().unwrap()).unwrap(),
        "# SKILL.md\nRelease 2.0.0\n"
    );
    assert_eq!(b["contract_version"], CONTRACT);
    assert_eq!(
        snapshot(&f.home.join(SKILL_PATH)),
        active,
        "resolving repository instructions must not replace the global entry skill"
    );
    fs::remove_dir_all(first).unwrap();
    fs::remove_dir_all(second).unwrap();
    assert_eq!(
        ok(&f.resolve(&f.repo, &["--offline"]))["preparation_version"],
        "1.1.0"
    );
    assert_eq!(
        ok(&f.resolve(&other, &["--offline"]))["preparation_version"],
        "2.0.0"
    );
}

#[test]
fn missing_unknown_and_incompatible_repository_bindings_fail_closed() {
    let f = Fixture::new();
    f.install();
    refused(&f.resolve(&f.repo, &["--offline"]));
    f.pin(&f.repo, "9.9.9", CONTRACT);
    refused(&f.resolve(&f.repo, &["--offline"]));
    f.pin(&f.repo, "1.1.0", "curie.factory-specification/v99");
    let release = f.release("1.1.0", CONTRACT);
    refused(&f.resolve(&f.repo, &["--source", release.to_str().unwrap()]));
    // A source for another preparation version cannot satisfy the repository pin.
    f.pin(&f.repo, "2.0.0", CONTRACT);
    refused(&f.resolve(&f.repo, &["--source", release.to_str().unwrap()]));
}

#[test]
fn tampered_content_and_traversal_paths_fail_before_any_writes() {
    let f = Fixture::new();
    f.install();
    for mutation in ["hash", "path", "version"] {
        let release = f.release("1.1.0", CONTRACT);
        let path = release.join("release.json");
        let mut value: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        match mutation {
            "hash" => value["files"]["SKILL.md"]["content"] = json!("tampered"),
            "path" => {
                let payload = value["files"]
                    .as_object_mut()
                    .unwrap()
                    .remove("SKILL.md")
                    .unwrap();
                value["files"]["../../outside.md"] = payload;
            }
            _ => value["preparation_version"] = json!("../../outside"),
        }
        fs::write(path, serde_json::to_vec(&value).unwrap()).unwrap();
        let before = snapshot(&f.home);
        refused(&f.update(&release, &[]));
        assert_eq!(snapshot(&f.home), before, "mutation {mutation}");
        assert!(!f.temp.path().join("outside.md").exists());
    }
}

#[cfg(unix)]
#[test]
fn managed_state_symlinks_are_refused_without_touching_external_files() {
    use std::os::unix::fs::symlink;
    let f = Fixture::new();
    f.install();
    let outside = f.temp.path().join("outside");
    fs::create_dir(&outside).unwrap();
    fs::write(outside.join("sentinel"), "keep\n").unwrap();
    let managed = f
        .home
        .join(".curie/agent-skills/codex/prepare-factory-ticket");
    if managed.exists() {
        fs::remove_dir_all(&managed).unwrap();
    }
    fs::create_dir_all(managed.parent().unwrap()).unwrap();
    symlink(&outside, &managed).unwrap();
    let active = snapshot(&f.home.join(SKILL_PATH));
    refused(&f.update(&f.release("1.1.0", CONTRACT), &[]));
    assert_eq!(snapshot(&f.home.join(SKILL_PATH)), active);
    assert_eq!(
        snapshot(&outside),
        BTreeMap::from([(PathBuf::from("sentinel"), b"keep\n".to_vec())])
    );
}

#[test]
fn offline_resolution_detects_tampered_cached_instructions() {
    let f = Fixture::new();
    f.install();
    f.pin(&f.repo, "1.1.0", CONTRACT);
    let release = f.release("1.1.0", CONTRACT);
    let value = ok(&f.resolve(&f.repo, &["--source", release.to_str().unwrap()]));
    fs::write(
        value["instructions_path"].as_str().unwrap(),
        "tampered cache\n",
    )
    .unwrap();
    fs::remove_dir_all(release).unwrap();
    refused(&f.resolve(&f.repo, &["--offline"]));
}

#[test]
fn rollback_refuses_to_overwrite_new_local_customizations() {
    let f = Fixture::new();
    f.install();
    ok(&f.update(&f.release("1.1.0", CONTRACT), &[]));
    let path = f.home.join(SKILL_PATH).join("references/specification.md");
    fs::write(&path, "local changes after upgrade\n").unwrap();
    let before = snapshot(&f.home);
    refused(&f.run(&["update", "--agent", "codex", "--global", "--rollback"]));
    assert_eq!(snapshot(&f.home), before);
}

#[test]
fn dry_run_update_does_not_modify_payload_receipt_or_cache() {
    let f = Fixture::new();
    f.install();
    let source = f.release("1.1.0", CONTRACT);
    let before = snapshot(&f.home);
    let result = ok(&f.update(&source, &["--dry-run"]));
    assert_eq!(result["dry_run"], true);
    assert_eq!(snapshot(&f.home), before);
}

#[test]
fn package_emits_self_contained_release_and_usable_spdx_inventory() {
    let f = Fixture::new();
    let out = f.temp.path().join("custom-preparation.json");
    ok(&f.run(&["package", "--out", out.to_str().unwrap()]));
    let manifest: Value = serde_json::from_slice(&fs::read(&out).unwrap()).unwrap();
    assert_eq!(manifest["schema_version"], 1);
    let files = manifest["files"].as_object().unwrap();
    assert_eq!(files.len(), 2);
    for path in ["SKILL.md", "references/specification.md"] {
        let content = files[path]["content"].as_str().unwrap();
        let hash: String = Sha256::digest(content.as_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        assert_eq!(files[path]["sha256"], hash);
    }
    let sbom: Value = serde_json::from_slice(
        &fs::read(out.with_file_name("custom-preparation.json.spdx.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(sbom["spdxVersion"], "SPDX-2.3");
    let packages = sbom["packages"].as_array().unwrap();
    assert!(
        !packages.is_empty(),
        "release integrity rejects an empty package inventory"
    );
    assert!(packages
        .iter()
        .any(|package| package["versionInfo"] == manifest["preparation_version"]));
    let inventory = sbom["files"].as_array().unwrap();
    for path in ["SKILL.md", "references/specification.md"] {
        let file = inventory
            .iter()
            .find(|entry| entry["fileName"] == path || entry["fileName"] == format!("./{path}"))
            .expect("each shipped text asset inventoried");
        assert!(file["checksums"]
            .as_array()
            .unwrap()
            .iter()
            .any(|checksum| checksum["algorithm"] == "SHA256"
                && checksum["checksumValue"] == files[path]["sha256"]));
    }
}

#[test]
fn a_released_version_cannot_be_reused_for_different_valid_content() {
    let f = Fixture::new();
    f.install();
    let source = f.release("1.1.0", CONTRACT);
    ok(&f.update(&source, &[]));
    let path = source.join("release.json");
    let mut release: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    let content = "different valid release bytes\n";
    release["files"]["SKILL.md"]["content"] = json!(content);
    release["files"]["SKILL.md"]["sha256"] = json!(Sha256::digest(content.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>());
    fs::write(path, serde_json::to_vec(&release).unwrap()).unwrap();
    let before = snapshot(&f.home);
    refused(&f.update(&source, &[]));
    assert_eq!(snapshot(&f.home), before);
}

#[test]
fn self_consistent_cache_tampering_still_fails_content_address_validation() {
    let f = Fixture::new();
    f.install();
    f.pin(&f.repo, "1.1.0", CONTRACT);
    let source = f.release("1.1.0", CONTRACT);
    let resolved = ok(&f.resolve(&f.repo, &["--source", source.to_str().unwrap()]));
    let instructions = PathBuf::from(resolved["instructions_path"].as_str().unwrap());
    let manifest = instructions.parent().unwrap().join("release.json");
    let mut release: Value = serde_json::from_slice(&fs::read(&manifest).unwrap()).unwrap();
    let content = "changed cache with recomputed file hash\n";
    release["files"]["SKILL.md"]["content"] = json!(content);
    release["files"]["SKILL.md"]["sha256"] = json!(Sha256::digest(content.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>());
    fs::write(&instructions, content).unwrap();
    fs::write(manifest, serde_json::to_vec(&release).unwrap()).unwrap();
    refused(&f.resolve(&f.repo, &["--offline"]));
}

#[test]
fn interrupted_activation_preserves_read_only_checks_then_recovers_before_upgrade() {
    let f = Fixture::new();
    f.install();
    let state = f
        .home
        .join(".curie/agent-skills/codex/prepare-factory-ticket");
    let receipt: Value =
        serde_json::from_slice(&fs::read(state.join("installed.json")).unwrap()).unwrap();
    let source = f.release("1.1.0", CONTRACT);
    let next: Value =
        serde_json::from_slice(&fs::read(source.join("release.json")).unwrap()).unwrap();
    let backup = state.join("backups/backup-fixture");
    fs::create_dir_all(backup.parent().unwrap()).unwrap();
    fs::rename(f.home.join(SKILL_PATH), &backup).unwrap();
    fs::write(
        state.join("pending.json"),
        serde_json::to_vec(&json!({
            "original":receipt,"next":next,"backup_name":"backup-fixture"
        }))
        .unwrap(),
    )
    .unwrap();
    let before = snapshot(&f.home);
    refused(&f.update(&source, &["--check"]));
    assert_eq!(
        snapshot(&f.home),
        before,
        "a check must not repair or erase interruption evidence"
    );
    ok(&f.update(&source, &[]));
    assert!(!state.join("pending.json").exists());
    assert_eq!(
        fs::read_to_string(f.home.join(SKILL_PATH).join("SKILL.md")).unwrap(),
        "# SKILL.md\nRelease 1.1.0\n"
    );
    ok(&f.run(&["update", "--agent", "codex", "--global", "--rollback"]));
    assert_eq!(
        fs::read_to_string(f.home.join(SKILL_PATH).join("SKILL.md")).unwrap(),
        receipt["release"]["files"]["SKILL.md"]["content"]
            .as_str()
            .unwrap()
    );
}

fn compatibility_response() -> Value {
    json!({
        "interface":"curie/input-contracts@1",
        "deployment_id":"22222222-2222-4222-8222-222222222222",
        "agent_id":"11111111-1111-4111-8111-111111111111",
        "version_id":"33333333-3333-4333-8333-333333333333",
        "bundle_sha256":"a".repeat(64),
        "accepted_contracts":[CONTRACT],"status":"active","enforced":true
    })
}

#[test]
fn compatibility_checks_exact_target_over_authenticated_http() {
    use crate::support::{serve, Response};
    let f = Fixture::new();
    f.pin(&f.repo, "1.1.0", CONTRACT);
    let body = compatibility_response();
    let server = serve(move |request| {
        assert_eq!(request.method, "GET");
        assert_eq!(
            request.path,
            "/deployments/22222222-2222-4222-8222-222222222222/input-contracts"
        );
        assert_eq!(request.header("x-api-key"), Some("fixture-api-key"));
        Response::json(200, &body.to_string())
    });
    let before = snapshot(&f.repo);
    let result = ok(&f.run(&[
        "compatibility",
        "--repo",
        f.repo.to_str().unwrap(),
        "--api-url",
        &server.base_url,
        "--api-key",
        "fixture-api-key",
    ]));
    assert_eq!(result["status"], "compatible");
    assert_eq!(server.recorded().len(), 1);
    assert_eq!(snapshot(&f.repo), before);
}

#[test]
fn compatibility_refuses_unknown_or_changed_deployment_contracts() {
    use crate::support::{serve, Response};
    for mutation in [
        "bundle",
        "deployment",
        "contract",
        "status",
        "enforced",
        "interface",
        "missing",
    ] {
        let f = Fixture::new();
        f.pin(&f.repo, "1.1.0", CONTRACT);
        let mut body = compatibility_response();
        match mutation {
            "bundle" => body["bundle_sha256"] = json!("b".repeat(64)),
            "deployment" => body["deployment_id"] = json!("44444444-4444-4444-8444-444444444444"),
            "contract" => body["accepted_contracts"] = json!(["example.other/v1"]),
            "status" => body["status"] = json!("inactive"),
            "enforced" => body["enforced"] = json!(false),
            "interface" => body["interface"] = json!("curie/input-contracts@99"),
            _ => {}
        }
        let server = serve(move |_| {
            Response::json(
                if mutation == "missing" { 404 } else { 200 },
                &body.to_string(),
            )
        });
        refused(&f.run(&[
            "compatibility",
            "--repo",
            f.repo.to_str().unwrap(),
            "--api-url",
            &server.base_url,
            "--api-key",
            "fixture-api-key",
        ]));
        assert_eq!(server.recorded().len(), 1, "{mutation}");
    }
}

#[test]
fn compatibility_dry_run_does_not_read_the_service() {
    use crate::support::{serve, Response};
    let f = Fixture::new();
    f.pin(&f.repo, "1.1.0", CONTRACT);
    let server = serve(|_| Response::json(500, r#"{"error":"must not contact service"}"#));
    let result = ok(&f.run(&[
        "compatibility",
        "--repo",
        f.repo.to_str().unwrap(),
        "--api-url",
        &server.base_url,
        "--api-key",
        "fixture-api-key",
        "--dry-run",
    ]));
    assert_eq!(result["dry_run"], true);
    assert!(server.recorded().is_empty());
    assert!(!String::from_utf8_lossy(
        &f.run(&[
            "compatibility",
            "--repo",
            f.repo.to_str().unwrap(),
            "--api-url",
            &server.base_url,
            "--api-key",
            "fixture-api-key",
            "--dry-run"
        ])
        .stdout
    )
    .contains("fixture-api-key"));
}

#[test]
fn interrupted_recovery_refuses_to_erase_added_or_changed_local_files() {
    for edited_existing in [false, true] {
        let f = Fixture::new();
        f.install();
        let payload = f.home.join(SKILL_PATH);
        if edited_existing {
            fs::write(payload.join("my-notes.md"), "original personal notes\n").unwrap();
        }
        let state = f
            .home
            .join(".curie/agent-skills/codex/prepare-factory-ticket");
        let receipt_bytes = fs::read(state.join("installed.json")).unwrap();
        let receipt: Value = serde_json::from_slice(&receipt_bytes).unwrap();
        let source = f.release("1.1.0", CONTRACT);
        let next: Value =
            serde_json::from_slice(&fs::read(source.join("release.json")).unwrap()).unwrap();
        let backup = state.join("backups/backup-fixture");
        fs::create_dir_all(backup.parent().unwrap()).unwrap();
        fs::rename(&payload, &backup).unwrap();
        for (path, file) in next["files"].as_object().unwrap() {
            let dest = payload.join(path);
            fs::create_dir_all(dest.parent().unwrap()).unwrap();
            fs::write(dest, file["content"].as_str().unwrap()).unwrap();
        }
        fs::write(
            payload.join("my-notes.md"),
            "new personal notes after interruption\n",
        )
        .unwrap();
        fs::write(
            state.join("pending.json"),
            serde_json::to_vec(&json!({
                "original":receipt,"next":next,"backup_name":"backup-fixture"
            }))
            .unwrap(),
        )
        .unwrap();
        let active_before = snapshot(&payload);
        let backup_before = snapshot(&backup);
        refused(&f.update(&source, &[]));
        assert_eq!(snapshot(&payload), active_before);
        assert_eq!(snapshot(&backup), backup_before);
        assert_eq!(
            fs::read(state.join("installed.json")).unwrap(),
            receipt_bytes
        );
        assert!(
            state.join("pending.json").exists(),
            "keep recovery evidence for manual reconciliation"
        );
    }
}

#[test]
fn caller_release_queries_and_fragments_are_refused_without_echoing_or_persisting_them() {
    let f = Fixture::new();
    f.install();
    for suffix in ["?credential=fixture-secret", "#fixture-secret"] {
        let url = format!("https://github.com/curie-eng/curie/releases/download/v1/factory-preparation.json{suffix}");
        let before = snapshot(&f.home);
        let output = f.run(&[
            "update",
            "--agent",
            "codex",
            "--global",
            "--release-url",
            &url,
        ]);
        refused(&output);
        let rendered = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            !rendered.contains("fixture-secret"),
            "caller release URL data must not be printed: {rendered}"
        );
        assert_eq!(snapshot(&f.home), before);
    }
}

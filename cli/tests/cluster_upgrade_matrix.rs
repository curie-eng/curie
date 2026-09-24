//! Guards for `curie dev cluster-upgrade-matrix` (#2590).
//!
//! The live scenarios need a task-owned kind install, published v0.8.8
//! artifacts, and candidate images. These tests pin the command surface and
//! the soak-refusal / scenario / checksum guards so a contributor cannot point
//! the matrix at the permanent soak or resolve the candidate binary from PATH.

use std::fs;
use std::path::PathBuf;
use std::process::Command;

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..")
}

fn output_text(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned() + &String::from_utf8_lossy(&output.stderr)
}

fn script() -> PathBuf {
    repo_root().join("cli/scripts/cluster-upgrade-matrix.sh")
}

#[test]
fn cluster_upgrade_matrix_script_is_present_and_executable() {
    let path = script();
    assert!(path.is_file(), "missing {}", path.display());
    let mode = fs::metadata(&path).expect("stat").permissions();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert!(
            mode.mode() & 0o111 != 0,
            "cluster-upgrade-matrix.sh must be executable"
        );
    }
}

#[test]
fn cluster_upgrade_matrix_self_test_refuses_soak_unknown_scenario_and_path_curie() {
    let output = Command::new("bash")
        .arg(script())
        .arg("--self-test")
        .current_dir(repo_root())
        .output()
        .expect("run cluster-upgrade-matrix --self-test");
    assert!(
        output.status.success(),
        "self-test failed\n{}",
        output_text(&output)
    );
    let text = output_text(&output);
    assert!(
        text.contains("soak namespace curie refused"),
        "self-test must refuse the permanent soak namespace\n{text}"
    );
    assert!(
        text.contains("soak release curie refused"),
        "self-test must refuse the permanent soak release\n{text}"
    );
    assert!(
        text.contains("unknown scenario refused"),
        "self-test must refuse an unknown scenario\n{text}"
    );
    assert!(
        text.contains("published v0.8.8 chart checksum pinned"),
        "self-test must pin the published v0.8.8 chart checksum\n{text}"
    );
    assert!(
        text.contains("sha256 helper rejected a mismatched fixture"),
        "self-test must demonstrate the checksum guard rejecting a mismatch\n{text}"
    );
    assert!(
        text.contains("candidate binary is not PATH fallback"),
        "self-test must refuse resolving BIN from PATH curie\n{text}"
    );
    assert!(
        text.contains("helm --set KEY=VAL tokens are split"),
        "self-test must pin split --set KEY=VAL argv tokens\n{text}"
    );
    assert!(
        text.contains("cluster upgrade verb is the mutator"),
        "self-test must pin cluster upgrade as the mutator\n{text}"
    );
    assert!(
        text.contains("restore_n resumes leftover in_progress 0.10.0"),
        "self-test must pin restore_n resuming leftover in_progress 0.10.0\n{text}"
    );
    assert!(
        text.contains("restore_n clears leftover in_progress after restoring 0.10.0"),
        "self-test must pin restore_n clearing the checkpoint after the 0.10.0 restore\n{text}"
    );
    assert!(
        text.contains("n-to-n1 restores 0.10.0 through restore_n"),
        "self-test must pin n-to-n1 using restore_n\n{text}"
    );
    assert!(
        text.contains("restore_n rolls back to 0.10.0 when a revision exists"),
        "self-test must pin restore_n rolling back to 0.10.0\n{text}"
    );
    assert!(
        text.contains("exclusive_kind_tag untags siblings before and after load"),
        "self-test must pin exclusive_kind_tag untag-before-load\n{text}"
    );
    assert!(
        text.contains("compatible rollback reloads exclusive 0.10.0 images"),
        "self-test must pin compatible rollback reloading 0.10.0 images\n{text}"
    );
    assert!(
        text.contains("published 0.8.8 rollback reloads 0.8.8 images"),
        "self-test must pin rollback-088 reloading 0.8.8 images\n{text}"
    );
    assert!(
        text.contains("restore_n loads exclusive 0.10.0 images before rollback"),
        "self-test must pin restore_n reloading exclusive 0.10.0 before helm rollback\n{text}"
    );
    assert!(
        text.contains("exclusive_kind_tag skips a reload when the node already holds the tag"),
        "self-test must pin the exclusive_kind_tag early return\n{text}"
    );
    assert!(
        text.contains("load_tag_images invalidates the exclusive kind tag"),
        "self-test must pin load_tag_images invalidating EXCLUSIVE_KIND_TAG\n{text}"
    );
    assert!(
        text.contains("schema heads published=0039"),
        "self-test must pin the published 0.8.8 alembic head\n{text}"
    );
}

#[test]
fn cluster_upgrade_matrix_cluster_surface_refuses_soak_namespace() {
    let output = Command::new(bin())
        .args(["dev", "cluster-upgrade-matrix", "--scenario", "fresh-n"])
        .env("CURIE_BIN", bin())
        .env("CURIE_E2E_NAMESPACE", "curie")
        .env("CURIE_E2E_RELEASE", "t2590")
        .current_dir(repo_root())
        .output()
        .expect("run cluster-upgrade-matrix against soak namespace");
    assert!(
        !output.status.success(),
        "cluster-upgrade-matrix must refuse namespace curie\n{}",
        output_text(&output)
    );
    let text = output_text(&output);
    assert!(
        text.contains("curie") && (text.contains("soak") || text.contains("refus")),
        "refusal must name the soak namespace\n{text}"
    );
}

#[test]
fn cluster_upgrade_matrix_unknown_scenario_exits_nonzero() {
    let output = Command::new("bash")
        .arg(script())
        .args(["--scenario", "not-a-scenario"])
        .current_dir(repo_root())
        .output()
        .expect("run cluster-upgrade-matrix unknown scenario");
    assert!(
        !output.status.success(),
        "unknown scenario must fail\n{}",
        output_text(&output)
    );
    let text = output_text(&output);
    assert!(
        text.contains("unknown scenario"),
        "error must name the unknown scenario\n{text}"
    );
}

#[test]
fn candidate_image_setup_derives_every_n1_tag_from_local_n_before_exclusive_load() {
    let source = fs::read_to_string(script()).expect("read cluster upgrade matrix");
    let prepare = source
        .split_once("prepare_candidate_images() {")
        .map(|(_, rest)| rest.split_once("\n}\n").map_or(rest, |(body, _)| body))
        .expect("prepare_candidate_images function");
    let load_n = prepare
        .find("load_tag_images \"0.10.0\"")
        .expect("candidate setup must ensure every local 0.10.0 image");
    let derive_n1 = prepare
        .find("retag_candidate_versions \"0.10.0\" \"0.10.1\" required")
        .expect("candidate setup must derive 0.10.1 from the ensured local 0.10.0 images");
    let exclusive_n = prepare
        .find("exclusive_kind_tag \"0.10.0\"")
        .expect("candidate setup must make 0.10.0 exclusive in kind");
    assert!(
        load_n < derive_n1 && derive_n1 < exclusive_n,
        "0.10.1 tags must be derived after local 0.10.0 is ensured and before sibling tags are removed"
    );
    assert!(
        !prepare.contains("retag_candidate_versions \"$src\" \"0.10.1\""),
        "0.10.1 must not depend on an optional upgrade candidate source"
    );

    let retag = source
        .split_once("retag_candidate_versions() {")
        .map(|(_, rest)| rest.split_once("\n}\n").map_or(rest, |(body, _)| body))
        .expect("retag_candidate_versions function");
    assert!(
        retag.contains("for img in \"${IMAGES[@]}\"")
            && retag.contains("[[ \"$required\" == required ]]")
            && retag.contains("die \"required source image $img:$src_tag is not local\""),
        "required candidate retagging must cover every app image and fail closed when a source is absent"
    );
}

#[test]
fn published_v089_rollback_scenario_is_strict_and_keeps_supported_rollback() {
    let source = fs::read_to_string(script()).expect("read cluster upgrade matrix");
    let catalog: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(repo_root().join("cli/src/application_schema_windows.json"))
            .expect("read application schema catalog"),
    )
    .expect("parse application schema catalog");
    for version in ["0.9.0", "0.9.1"] {
        assert_eq!(
            catalog["windows"][version]["schema_min"], "0001",
            "the released {version} rollback floor must remain pinned"
        );
        assert_eq!(
            catalog["windows"][version]["schema_head"], "0044",
            "the released {version} rollback head must remain pinned"
        );
    }
    assert_eq!(
        catalog["windows"]["0.9.2"]["schema_min"], "0045",
        "released 0.9.2 must begin at its schema compatibility floor"
    );
    assert_eq!(
        catalog["windows"]["0.9.2"]["schema_head"], "0045",
        "released 0.9.2 must stop at its schema compatibility head"
    );
    assert_eq!(
        catalog["windows"]["0.10.0"]["schema_head"], "0054",
        "the candidate 0.10.0 rollback head must match this tree"
    );
    assert!(
        source.contains("rollback-published-089"),
        "published 0.8.9 must be an additive matrix scenario"
    );
    assert!(
        source.contains(
            "CHART_089_SHA=\"ee57017fe3009c35a4390b0c0555c44249ba98bba1d4f53f12aa3944b2bd5e5e\""
        ),
        "published 0.8.9 chart digest must stay pinned"
    );
    assert!(
        source.contains(
            "CLI_089_SHA=\"b8f3a00bcbf0920ae61e55039aa6a9d48e4d302db88a8c97e8148905569c0e9a\""
        ),
        "published 0.8.9 Linux CLI digest must stay pinned"
    );

    let install_089 = source
        .split_once("helm_install_089() {")
        .map(|(_, rest)| rest.split_once("\n}\n").map_or(rest, |(body, _)| body))
        .expect("helm_install_089 function");
    let remote_refresh = install_089
        .find("for img in \"${IMAGES[@]}\"")
        .expect("published 0.8.9 install must refresh every required app image");
    let image_ref = install_089[remote_refresh..]
        .find("ref=\"$(image_for \"$img\" \"0.8.9\")\"")
        .map(|offset| remote_refresh + offset)
        .expect("published image refresh must use the canonical ghcr.io image helper");
    let pull = install_089[image_ref..]
        .find("docker pull \"$ref\"")
        .map(|offset| image_ref + offset)
        .expect("published image refresh must fail closed when a remote pull fails");
    let load = install_089
        .find("load_tag_images \"0.8.9\"")
        .expect("published images must be loaded into kind");
    let install = install_089
        .find("helm_ns install")
        .expect("published chart must be installed");
    assert!(
        remote_refresh < image_ref && image_ref < pull && pull < load && load < install,
        "every ghcr.io 0.8.9 image must be refreshed before kind load and Helm install"
    );
    assert!(
        !install_089.contains("docker pull \"$ref\" ||"),
        "published image refresh must not suppress a pull failure"
    );

    let scenario = source
        .split_once("run_rollback_published_089() {")
        .map(|(_, rest)| rest.split_once("\n}\n").map_or(rest, |(body, _)| body))
        .expect("run_rollback_published_089 function");
    let published_install = scenario
        .split_once("helm_install_089")
        .and_then(|(_, rest)| rest.split_once("cluster_upgrade \"0.10.0\""))
        .map(|(body, _)| body)
        .expect("published 0.8.9 install assertions");
    assert!(
        published_install.contains("assert_alembic \"$PUBLISHED_HEAD\"")
            && !published_install.contains("assert_alembic \"$TARGET_HEAD\""),
        "the real published 0.8.9 artifact must be observed at its released 0039 head"
    );
    assert!(
        scenario.contains("get manifest")
            && scenario.contains("app.kubernetes.io/component=schema-compat")
            && scenario.contains("grep"),
        "scenario must retain the real manifest and assert labeled metadata is absent"
    );
    assert!(
        scenario.contains("status != 0")
            && scenario.contains("0.8.9")
            && scenario.contains("PUBLISHED_HEAD")
            && scenario.contains("SUPPORTED_ROLLBACK_HEAD")
            && scenario.contains("outside its declared schema range")
            && scenario.contains("if echo \"$err\" | grep -F \"could not establish\"")
            && scenario.contains("failed identity classification"),
        "scenario must require a nonzero range refusal naming 0.8.9, published head 0039, and candidate head 0054 while rejecting identity failures"
    );
    assert!(
        scenario.contains("helm_version") && scenario.contains("0.10.0"),
        "refusal must leave the deployed 0.10.0 revision unchanged"
    );
    assert!(
        scenario.matches("assert_sentinel").count() >= 2
            && scenario.contains("assert_alembic \"$TARGET_HEAD\"")
            && scenario.contains("readyReplicas")
            && scenario.contains("api_health"),
        "refusal must retain the sentinel, candidate Alembic head, and a ready API"
    );
    assert!(
        scenario.contains("run_compatible_rollback"),
        "the same scenario must prove one compatible rollback succeeds"
    );

    let compatible = source
        .split_once("run_compatible_rollback() {")
        .map(|(_, rest)| rest.split_once("\n}\n").map_or(rest, |(body, _)| body))
        .expect("run_compatible_rollback function");
    assert!(
        compatible.contains("assert_sentinel")
            && compatible.contains("assert_alembic \"$SUPPORTED_ROLLBACK_HEAD\""),
        "supported 0.10.1 to 0.10.0 rollback must retain the sentinel and catalogued Alembic head"
    );
}

fn run_script(args: &[&str], envs: &[(&str, &str)]) -> std::process::Output {
    let mut cmd = Command::new("bash");
    cmd.arg(script()).args(args).current_dir(repo_root());
    for (k, v) in envs {
        cmd.env(k, v);
    }
    cmd.output().expect("run cluster-upgrade-matrix")
}

fn bash_array_from_script(name: &str) -> Vec<String> {
    let source = fs::read_to_string(script()).expect("read cluster upgrade matrix");
    let body = source
        .split_once(&format!("\n{name}=("))
        .and_then(|(_, rest)| rest.split_once(')'))
        .map(|(body, _)| body)
        .unwrap_or_else(|| panic!("script must define array {name}"));
    body.split_whitespace().map(str::to_owned).collect()
}

const PHASED: [&str; 2] = ["fail-every-phase", "interrupt-resume"];

#[test]
fn list_shards_json_covers_every_scenario_and_phase_exactly_once() {
    let output = run_script(&["--list-shards", "--json"], &[]);
    assert!(
        output.status.success(),
        "--list-shards --json failed\n{}",
        output_text(&output)
    );
    let manifest: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("--list-shards --json must emit JSON");
    let shards = manifest["shards"].as_array().expect("shards array");
    let ids: Vec<&str> = shards
        .iter()
        .map(|s| s["id"].as_str().expect("shard id"))
        .collect();
    assert_eq!(
        ids,
        [
            "s01", "s02", "s03", "s04", "s05", "s06", "s07", "s08", "s09", "s10", "s11", "s12",
            "s13", "s14"
        ],
        "canonical shard ids\n{manifest}"
    );

    let mut unsplit: Vec<String> = Vec::new();
    let mut phases: std::collections::BTreeMap<String, Vec<String>> = Default::default();
    for shard in shards {
        let id = shard["id"].as_str().unwrap();
        let setup = shard["setup"].as_bool().expect("shard setup flag");
        assert_eq!(
            setup,
            !matches!(id, "s01" | "s11" | "s12"),
            "setup flag wrong for {id}"
        );
        for item in shard["scenarios"].as_array().expect("scenarios array") {
            let name = item["name"].as_str().expect("scenario name").to_owned();
            if PHASED.contains(&name.as_str()) {
                let list = item["phases"].as_array().unwrap_or_else(|| {
                    panic!("phased scenario {name} in {id} must be split by phase")
                });
                assert!(!list.is_empty(), "{name} in {id} has no phases");
                phases
                    .entry(name)
                    .or_default()
                    .extend(list.iter().map(|p| p.as_str().expect("phase").to_owned()));
            } else {
                assert!(
                    item["phases"].is_null(),
                    "{name} in {id} must have phases null"
                );
                unsplit.push(name);
            }
        }
    }

    let mut matrix_phases = bash_array_from_script("MATRIX_PHASES");
    assert_eq!(
        matrix_phases,
        [
            "plan",
            "validate",
            "drain_preflight",
            "checkpoint",
            "migrate",
            "apply",
            "converge",
            "canary",
            "commit"
        ]
    );
    matrix_phases.sort();
    let mut interrupt_phases = bash_array_from_script("INTERRUPT_PHASES");
    assert_eq!(
        interrupt_phases,
        ["checkpoint", "migrate", "apply", "commit"]
    );
    interrupt_phases.sort();
    for (name, want, label) in [
        ("fail-every-phase", &matrix_phases, "MATRIX_PHASES"),
        ("interrupt-resume", &interrupt_phases, "INTERRUPT_PHASES"),
    ] {
        let mut got = phases.get(name).cloned().unwrap_or_default();
        got.sort();
        assert_eq!(&got, want, "{name} phases must cover {label} once each");
    }

    let mut expected: Vec<String> = bash_array_from_script("SCENARIOS_ALL")
        .into_iter()
        .filter(|s| !PHASED.contains(&s.as_str()))
        .collect();
    expected.sort();
    unsplit.sort();
    assert_eq!(
        unsplit, expected,
        "non-phased scenarios must each appear once"
    );
}

#[test]
fn self_test_checks_shard_coverage_and_timing() {
    let output = run_script(&["--self-test"], &[]);
    let text = output_text(&output);
    assert!(output.status.success(), "self-test failed\n{text}");
    for needle in [
        "shard manifest covers every scenario exactly once",
        "shard coverage refused a dropped scenario",
        "shard coverage refused a duplicated scenario",
        "shard coverage refused a dropped phase",
        "shard coverage refused a duplicated phase",
        "per-scenario timing recorded",
    ] {
        assert!(
            text.contains(needle),
            "self-test must print `{needle}`\n{text}"
        );
    }
    let source = fs::read_to_string(script()).expect("read script");
    assert!(
        source.contains("phases=") && source.contains("elapsed_seconds="),
        "timing log line must carry phases and elapsed_seconds"
    );
}

const GOOD_SHARDS: &str = "s01 nosetup soak-refusal fresh-n n1-to-n-nonempty same-version
s02 setup fail-every-phase:plan+validate+drain_preflight
s03 setup fail-every-phase:checkpoint+migrate+apply
s04 setup fail-every-phase:converge
s05 setup fail-every-phase:canary
s06 setup fail-every-phase:commit
s07 setup interrupt-resume:checkpoint+migrate
s08 setup interrupt-resume:apply+commit
s09 setup n-to-n1 compatible-rollback
s10 setup rollback-published-088
s11 nosetup rollback-published-089
s12 nosetup migration-crash
s13 setup converge-negative
s14 setup previous-serves";

fn assert_override_refused(manifest: &str, what: &str) {
    assert_ne!(
        manifest, GOOD_SHARDS,
        "fixture for {what} must differ from the good manifest"
    );
    let output = run_script(&["--self-test"], &[("CURIE_E2E_SHARDS_OVERRIDE", manifest)]);
    let text = output_text(&output);
    assert!(
        !output.status.success(),
        "{what} must fail self-test\n{text}"
    );
    assert!(text.contains("shard coverage failed"), "{what}\n{text}");
}

#[test]
fn good_override_passes_self_test() {
    let output = run_script(
        &["--self-test"],
        &[("CURIE_E2E_SHARDS_OVERRIDE", GOOD_SHARDS)],
    );
    let text = output_text(&output);
    assert!(
        output.status.success(),
        "canonical override must pass\n{text}"
    );
    assert!(
        text.contains("shard manifest covers every scenario exactly once"),
        "{text}"
    );
}

#[test]
fn self_test_fails_when_override_drops_a_scenario() {
    assert_override_refused(
        &GOOD_SHARDS.replace(" migration-crash", ""),
        "dropped scenario",
    );
}

#[test]
fn self_test_fails_when_override_duplicates_a_scenario() {
    assert_override_refused(
        &GOOD_SHARDS.replace(
            "s12 nosetup migration-crash",
            "s12 nosetup migration-crash fresh-n",
        ),
        "duplicated scenario",
    );
}

#[test]
fn self_test_fails_when_override_drops_a_phase() {
    assert_override_refused(
        &GOOD_SHARDS.replace(
            "interrupt-resume:checkpoint+migrate",
            "interrupt-resume:checkpoint",
        ),
        "dropped phase",
    );
}

#[test]
fn self_test_fails_when_override_duplicates_a_phase() {
    assert_override_refused(
        &GOOD_SHARDS.replace(
            "fail-every-phase:converge",
            "fail-every-phase:converge+plan",
        ),
        "duplicated phase",
    );
}

#[test]
fn self_test_fails_when_override_runs_phased_scenario_unsplit() {
    assert_override_refused(
        &GOOD_SHARDS.replace(
            "s08 setup interrupt-resume:apply+commit",
            "s08 setup interrupt-resume:apply+commit\ns14 setup interrupt-resume",
        ),
        "unsplit phased scenario",
    );
}

#[test]
fn self_test_fails_when_interrupt_resume_runs_a_phase_outside_interrupt_phases() {
    assert_override_refused(
        &GOOD_SHARDS.replace(
            "interrupt-resume:apply+commit",
            "interrupt-resume:apply+commit+plan",
        ),
        "interrupt-resume phase outside INTERRUPT_PHASES",
    );
}

#[test]
fn unknown_shard_is_refused() {
    let output = run_script(&["--shard", "nope"], &[]);
    let text = output_text(&output);
    assert!(!output.status.success(), "unknown shard must fail\n{text}");
    assert!(text.contains("unknown shard"), "{text}");
}

#[test]
fn shard_and_scenario_together_are_refused() {
    let output = run_script(&["--shard", "s01", "--scenario", "fresh-n"], &[]);
    let text = output_text(&output);
    assert!(
        !output.status.success(),
        "--shard with --scenario must fail\n{text}"
    );
    assert!(text.contains("--shard and --scenario"), "{text}");
}

#[test]
fn cluster_upgrade_matrix_every_listed_shard_id_resolves() {
    // CI runs `--shard <id>` for each id `--list-shards` prints; a lookup that
    // refuses a listed id fails every shard before any cluster work (#2733).
    let listed = run_script(&["--list-shards"], &[]);
    assert!(listed.status.success());
    let manifest = String::from_utf8_lossy(&listed.stdout).to_string();
    for line in manifest.lines().filter(|l| !l.trim().is_empty()) {
        let id = line.split_whitespace().next().unwrap();
        let output = run_script(&["--shard", id, "--self-test"], &[]);
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            output.status.success(),
            "listed shard {id} must resolve\n{text}"
        );
    }
}

#[test]
fn migration_crash_bounds_the_interrupted_upgrade_wait() {
    // #2733: the interrupted first upgrade blocked ~900s on helm's hook wait.
    // It gets 120s to exit, then is killed with ownership and helm lock recovered.
    let source = fs::read_to_string(script()).expect("read script");
    let start = source
        .find("run_migration_crash() {")
        .expect("run_migration_crash defined");
    let body = &source[start..start + source[start..].find("\n}\n").unwrap()];
    let pos = |needle: &str| {
        body.find(needle)
            .unwrap_or_else(|| panic!("run_migration_crash must contain `{needle}`\n{body}"))
    };
    let interrupt = pos("interrupt_schema_migrate");
    let bound = pos("SECONDS + 120");
    let kill = pos("terminate_tree \"$pid\"");
    let ownership = pos("recover_killed_upgrade_ownership");
    let lock = pos("recover_helm_lock");
    let waited = pos("wait \"$pid\"");
    let retry = pos("migration-crash-retry");
    assert!(
        interrupt < bound && bound < kill,
        "bounded wait must follow the interrupt"
    );
    assert!(kill < ownership && ownership < lock && lock < retry);
    assert!(kill < waited && waited < retry);
    assert!(body.contains("exited on its own") && body.contains("terminated after"));
    let output = run_script(&["--self-test"], &[]);
    let text = output_text(&output);
    assert!(
        text.contains("migration-crash bounds the interrupted upgrade wait"),
        "{text}"
    );
}

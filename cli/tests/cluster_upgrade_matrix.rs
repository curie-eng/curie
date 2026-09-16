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
        text.contains("restore_n resumes leftover in_progress 0.9.0"),
        "self-test must pin restore_n resuming leftover in_progress 0.9.0\n{text}"
    );
    assert!(
        text.contains("restore_n clears leftover in_progress after restoring 0.9.0"),
        "self-test must pin restore_n clearing the checkpoint after the 0.9.0 restore\n{text}"
    );
    assert!(
        text.contains("n-to-n1 restores 0.9.0 through restore_n"),
        "self-test must pin n-to-n1 using restore_n\n{text}"
    );
    assert!(
        text.contains("restore_n rolls back to 0.9.0 when a revision exists"),
        "self-test must pin restore_n rolling back to 0.9.0\n{text}"
    );
    assert!(
        text.contains("exclusive_kind_tag untags siblings before and after load"),
        "self-test must pin exclusive_kind_tag untag-before-load\n{text}"
    );
    assert!(
        text.contains("compatible rollback reloads exclusive 0.9.0 images"),
        "self-test must pin compatible rollback reloading 0.9.0 images\n{text}"
    );
    assert!(
        text.contains("published 0.8.8 rollback reloads 0.8.8 images"),
        "self-test must pin rollback-088 reloading 0.8.8 images\n{text}"
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
        .find("load_tag_images \"0.9.0\"")
        .expect("candidate setup must ensure every local 0.9.0 image");
    let derive_n1 = prepare
        .find("retag_candidate_versions \"0.9.0\" \"0.9.1\" required")
        .expect("candidate setup must derive 0.9.1 from the ensured local 0.9.0 images");
    let exclusive_n = prepare
        .find("exclusive_kind_tag \"0.9.0\"")
        .expect("candidate setup must make 0.9.0 exclusive in kind");
    assert!(
        load_n < derive_n1 && derive_n1 < exclusive_n,
        "0.9.1 tags must be derived after local 0.9.0 is ensured and before sibling tags are removed"
    );
    assert!(
        !prepare.contains("retag_candidate_versions \"$src\" \"0.9.1\""),
        "0.9.1 must not depend on an optional upgrade-candidate source"
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
    assert_eq!(
        catalog["windows"]["0.9.0"]["schema_head"], "0044",
        "the supported 0.9.0 rollback head must remain pinned"
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
        .and_then(|(_, rest)| rest.split_once("cluster_upgrade \"0.9.0\""))
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
        "scenario must require a nonzero range refusal naming 0.8.9, published head 0039, and supported head 0044 while rejecting identity failures"
    );
    assert!(
        scenario.contains("helm_version") && scenario.contains("0.9.0"),
        "refusal must leave the deployed 0.9.0 revision unchanged"
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
        "supported 0.9.1 to 0.9.0 rollback must retain the sentinel and catalogued Alembic head"
    );
}

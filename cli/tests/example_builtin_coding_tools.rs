//! Built-in coding tools belong to the runtime, not to individual bundles.
//!
//! The minimal coder example proves a bundle can opt into that runtime shape
//! without authoring a skill, while the SRE example proves unrelated bundles
//! do not need a copied coder skill or equivalent workspace instructions.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::{json, Value};

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("cli has a repository parent")
        .to_path_buf()
}

fn skill_files(bundle: &Path) -> Vec<PathBuf> {
    let skills = bundle.join("skills");
    let Ok(entries) = fs::read_dir(&skills) else {
        return Vec::new();
    };
    let mut found = Vec::new();
    for entry in entries {
        let entry = entry.expect("read skill directory entry");
        let candidate = entry.path().join("SKILL.md");
        if candidate.is_file() {
            found.push(candidate);
        }
    }
    found.sort();
    found
}

#[test]
fn coder_example_is_a_valid_skill_less_bundle() {
    let root = repo_root();
    let coder = root.join("examples/coder");
    let authored_skills = skill_files(&coder);
    assert!(
        authored_skills.is_empty(),
        "examples/coder must rely on runtime built-ins, not authored skills: {authored_skills:?}"
    );
    assert!(
        !coder.join("skills/coder/SKILL.md").exists(),
        "the former coder skill must not remain in the skill-less example"
    );

    if Command::new("uv").arg("--version").output().is_err() {
        eprintln!("skipping authoritative bundle validation: uv is not on PATH");
        return;
    }
    let validation = Command::new("uv")
        .current_dir(&root)
        .args([
            "run",
            "--frozen",
            "python",
            "-c",
            "import sys\nfrom plugin_format import validate_bundle\nprint(validate_bundle(sys.argv[1]).model_dump_json())",
        ])
        .arg(&coder)
        .output()
        .expect("run authoritative plugin-format validator");
    let stdout = String::from_utf8_lossy(&validation.stdout);
    let stderr = String::from_utf8_lossy(&validation.stderr);
    assert!(
        validation.status.success(),
        "bundle validator failed to run: stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    let reported = stdout
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        .unwrap_or_else(|| panic!("bundle validator returned no result: {stderr}"));
    let result: Value = serde_json::from_str(reported)
        .unwrap_or_else(|error| panic!("validator output is not JSON ({error}): {reported}"));
    assert_eq!(
        result["errors"],
        json!([]),
        "invalid coder bundle: {reported}"
    );
    assert_eq!(
        result["valid"],
        json!(true),
        "invalid coder bundle: {reported}"
    );
}

#[test]
fn sre_bot_contains_no_coder_skill_or_equivalent_instructions() {
    let sre_bot = repo_root().join("examples/sre-bot");
    let authored_skills = skill_files(&sre_bot);
    assert_eq!(
        authored_skills,
        vec![sre_bot.join("skills/sre-bot/SKILL.md")],
        "sre-bot must carry only its domain skill"
    );

    let sre_skill = fs::read_to_string(&authored_skills[0]).expect("read sre-bot skill");
    for coder_marker in [
        "name: coder",
        "managed repository workspace",
        "mcp__curie__publish_changes",
        "`/workspace`",
    ] {
        assert!(
            !sre_skill.contains(coder_marker),
            "sre-bot must not inline coder-equivalent instructions: {coder_marker}"
        );
    }
}

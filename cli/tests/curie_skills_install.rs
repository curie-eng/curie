//! Exercise the whole bundled Curie skill set through the CLI without external tools.
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use serde_json::Value;
use tempfile::TempDir;

type EmbeddedSkill = (&'static str, &'static [(&'static str, &'static [u8])]);

const SKILLS: [EmbeddedSkill; 4] = [
    ("prepare-factory-ticket", &[
        ("SKILL.md", include_bytes!("../../examples/dark-factory/preparation/skills/prepare-factory-ticket/SKILL.md")),
        ("references/specification.md", include_bytes!("../../examples/dark-factory/preparation/skills/prepare-factory-ticket/references/specification.md")),
    ]),
    ("review-pr", &[
        ("SKILL.md", include_bytes!("../../examples/dark-factory/review/skills/review-pr/SKILL.md")),
        ("references/review.md", include_bytes!("../../examples/dark-factory/review/skills/review-pr/references/review.md")),
        ("references/feedback.md", include_bytes!("../../examples/dark-factory/review/skills/review-pr/references/feedback.md")),
    ]),
    ("factory-environment-setup", &[
        ("SKILL.md", include_bytes!("../../examples/dark-factory/environment/skills/factory-environment-setup/SKILL.md")),
    ]),
    ("factory-environment-maintenance", &[
        ("SKILL.md", include_bytes!("../../examples/dark-factory/environment/skills/factory-environment-maintenance/SKILL.md")),
    ]),
];
const AGENTS: [(&str, &str, &str); 5] = [
    ("codex", ".agents/skills", ".agents/skills"),
    ("claude-code", ".claude/skills", ".claude/skills"),
    ("copilot", ".github/skills", ".copilot/skills"),
    ("gemini", ".gemini/skills", ".gemini/skills"),
    ("cursor", ".cursor/skills", ".cursor/skills"),
];

struct Fixture {
    temp: TempDir,
    home: PathBuf,
    project: PathBuf,
}
impl Fixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("personal home");
        let project = temp.path().join("project with spaces");
        fs::create_dir_all(&home).unwrap();
        fs::create_dir_all(&project).unwrap();
        Self {
            temp,
            home,
            project,
        }
    }
    fn run(&self, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_curie"))
            .env_clear()
            .env("HOME", &self.home)
            .env("USERPROFILE", &self.home)
            .env("XDG_CONFIG_HOME", self.home.join("config"))
            .env("PATH", self.temp.path().join("no-executables"))
            .env("NO_COLOR", "1")
            .env("CURIE_API_URL", "http://127.0.0.1:9")
            .env("HTTP_PROXY", "http://127.0.0.1:9")
            .env("HTTPS_PROXY", "http://127.0.0.1:9")
            .current_dir(&self.project)
            .arg("--json")
            .args(["factory", "skills", "install"])
            .args(args)
            .output()
            .expect("candidate CLI runs")
    }
}

fn success(output: &Output) -> Value {
    assert!(
        output.status.success(),
        "stdout: {}; stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let value: Value = serde_json::from_slice(&output.stdout).expect("exactly one JSON object");
    assert!(value.is_object());
    value
}
fn receipts(value: &Value, agent: &str, scope: &str, root: &Path, directory: &str) {
    let values = value["skills"]
        .as_array()
        .expect("set receipt contains skills array");
    assert_eq!(values.len(), SKILLS.len(), "exactly the bundled skills");
    let schema: Value = serde_json::from_slice(
        &fs::read(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("schema/agent-skill-install.schema.json"),
        )
        .unwrap(),
    )
    .unwrap();
    let validator = jsonschema::validator_for(&schema).unwrap();
    for (name, _) in SKILLS {
        let matches: Vec<_> = values.iter().filter(|v| v["skill"] == name).collect();
        assert_eq!(matches.len(), 1, "one receipt for {name}");
        let item = matches[0];
        assert!(validator.is_valid(item), "single receipt schema: {item}");
        assert_eq!(item["agent"], agent);
        assert_eq!(item["scope"], scope);
        assert_eq!(
            item["directory"],
            root.canonicalize()
                .unwrap()
                .join(directory)
                .join(name)
                .to_string_lossy()
                .as_ref()
        );
    }
}
fn files_under(root: &Path) -> Vec<(PathBuf, Vec<u8>)> {
    fn visit(root: &Path, here: &Path, found: &mut Vec<(PathBuf, Vec<u8>)>) {
        for entry in fs::read_dir(here).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                visit(root, &entry.path(), found);
            } else {
                found.push((
                    entry.path().strip_prefix(root).unwrap().to_path_buf(),
                    fs::read(entry.path()).unwrap(),
                ));
            }
        }
    }
    let mut found = Vec::new();
    visit(root, root, &mut found);
    found.sort();
    found
}
fn installed(root: &Path, directory: &str) {
    for (name, files) in SKILLS {
        let actual = files_under(&root.join(directory).join(name));
        let mut expected: Vec<_> = files
            .iter()
            .map(|(path, bytes)| (PathBuf::from(path), bytes.to_vec()))
            .collect();
        expected.sort();
        assert_eq!(actual, expected, "only bundled text assets in {name}");
    }
}

#[test]
fn all_clients_install_whole_set_in_repository_and_global_scopes_offline() {
    for (agent, project_dir, personal_dir) in AGENTS {
        for global in [false, true] {
            let f = Fixture::new();
            let mut args = vec!["--agent", agent];
            if global {
                args.push("--global");
            }
            let value = success(&f.run(&args));
            let (root, directory, untouched, scope) = if global {
                (&f.home, personal_dir, &f.project, "user")
            } else {
                (&f.project, project_dir, &f.home, "repository")
            };
            receipts(&value, agent, scope, root, directory);
            assert!(value["skills"]
                .as_array()
                .unwrap()
                .iter()
                .all(|v| v["status"] == "installed"));
            installed(root, directory);
            assert!(files_under(untouched).is_empty());
        }
    }
}

#[test]
fn repeated_set_install_reports_all_unchanged_without_mutating_assets() {
    let f = Fixture::new();
    success(&f.run(&["--agent", "codex"]));
    let before = files_under(&f.project);
    let value = success(&f.run(&["--agent", "codex"]));
    receipts(&value, "codex", "repository", &f.project, ".agents/skills");
    assert!(value["skills"]
        .as_array()
        .unwrap()
        .iter()
        .all(|v| v["status"] == "unchanged"));
    assert_eq!(files_under(&f.project), before);
}

#[test]
fn conflict_in_either_skill_prevents_any_new_sibling_writes() {
    for (conflicting, _) in SKILLS {
        let f = Fixture::new();
        let custom = f
            .project
            .join(".agents/skills")
            .join(conflicting)
            .join("SKILL.md");
        fs::create_dir_all(custom.parent().unwrap()).unwrap();
        fs::write(custom, "local customization\n").unwrap();
        let before = files_under(&f.project);
        for args in [
            vec!["--agent", "codex"],
            vec!["--agent", "codex", "--dry-run"],
        ] {
            let result = f.run(&args);
            assert!(
                !result.status.success(),
                "conflict must refuse the whole set"
            );
            assert_eq!(
                files_under(&f.project),
                before,
                "preflight before either skill write"
            );
        }
        assert!(files_under(&f.home).is_empty());
    }
}

#[test]
fn one_identical_existing_skill_and_missing_siblings_complete_safely() {
    for (existing, files) in SKILLS {
        let f = Fixture::new();
        let root = f.project.join(".agents/skills").join(existing);
        for (name, bytes) in files {
            let dest = root.join(name);
            fs::create_dir_all(dest.parent().unwrap()).unwrap();
            fs::write(dest, bytes).unwrap();
        }
        let before = files_under(&root);
        let value = success(&f.run(&["--agent", "codex"]));
        receipts(&value, "codex", "repository", &f.project, ".agents/skills");
        for receipt in value["skills"].as_array().unwrap() {
            assert_eq!(
                receipt["status"],
                if receipt["skill"] == existing {
                    "unchanged"
                } else {
                    "installed"
                }
            );
        }
        installed(&f.project, ".agents/skills");
        assert_eq!(files_under(&root), before);
    }
}

#[test]
fn whole_set_dry_run_is_one_plan_and_creates_no_files() {
    for global in [false, true] {
        let f = Fixture::new();
        let mut args = vec!["--agent", "codex", "--dry-run"];
        if global {
            args.push("--global");
        }
        let value = success(&f.run(&args));
        let schema: Value = serde_json::from_slice(
            &fs::read(Path::new(env!("CARGO_MANIFEST_DIR")).join("schema/dry-run.schema.json"))
                .unwrap(),
        )
        .unwrap();
        assert!(jsonschema::validator_for(&schema).unwrap().is_valid(&value));
        assert_eq!(value["dry_run"], true);
        let rendered = value.to_string();
        for (name, _) in SKILLS {
            assert!(rendered.contains(name), "plan names {name}");
        }
        assert!(files_under(&f.project).is_empty());
        assert!(files_under(&f.home).is_empty());
    }
}

#[test]
fn unrelated_installed_skill_is_preserved() {
    let f = Fixture::new();
    let unrelated = f.project.join(".agents/skills/unrelated");
    fs::create_dir_all(&unrelated).unwrap();
    fs::write(unrelated.join("SKILL.md"), "human-owned skill\n").unwrap();
    fs::write(unrelated.join("notes.txt"), "human-owned notes\n").unwrap();
    let before = files_under(&unrelated);
    success(&f.run(&["--agent", "codex"]));
    installed(&f.project, ".agents/skills");
    assert_eq!(files_under(&unrelated), before);
}

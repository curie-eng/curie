//! Exercise the embedded text-skill installer at the real CLI process boundary.
//! No host coding agent, Python, Docker, git executable, registry or Curie API is
//! available to these processes. This proves installation, not agent discovery.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use serde_json::Value;
use tempfile::TempDir;

const SKILL: &str = "prepare-factory-ticket";
const FILES: [(&str, &[u8]); 2] = [
    (
        "SKILL.md",
        include_bytes!("../../examples/dark-factory/preparation/skills/prepare-factory-ticket/SKILL.md"),
    ),
    (
        "references/specification.md",
        include_bytes!("../../examples/dark-factory/preparation/skills/prepare-factory-ticket/references/specification.md"),
    ),
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
        let temp = tempfile::tempdir().expect("temporary fixture");
        let home = temp.path().join("personal home");
        let project = temp.path().join("checkout with spaces");
        fs::create_dir_all(&home).unwrap();
        fs::create_dir_all(&project).unwrap();
        Self {
            temp,
            home,
            project,
        }
    }

    fn run(&self, cwd: &Path, args: &[&str]) -> Output {
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
            .current_dir(cwd)
            .args(["--json", "factory", "prepare", "install"])
            .args(args)
            .output()
            .expect("run candidate curie")
    }
}

fn success(output: &Output) -> Value {
    assert!(
        output.status.success(),
        "stdout: {}; stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let value: Value = serde_json::from_slice(&output.stdout).expect("one JSON result object");
    assert!(value.is_object(), "result must be an object: {value}");
    let schema_name = if value["dry_run"] == true {
        "dry-run.schema.json"
    } else {
        "agent-skill-install.schema.json"
    };
    let schema_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("schema")
        .join(schema_name);
    let schema: Value = serde_json::from_slice(&fs::read(schema_path).expect("committed schema"))
        .expect("schema JSON");
    let validator = jsonschema::validator_for(&schema).expect("schema compiles");
    if let Err(error) = validator.validate(&value) {
        panic!("{schema_name}: {error}; result: {value}");
    }
    value
}

fn refused(output: &Output) {
    assert!(
        !output.status.success(),
        "unexpected success: {}",
        String::from_utf8_lossy(&output.stdout)
    );
}

fn files_under(root: &Path) -> Vec<PathBuf> {
    fn visit(root: &Path, here: &Path, found: &mut Vec<PathBuf>) {
        for entry in fs::read_dir(here).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                visit(root, &entry.path(), found);
            } else {
                found.push(entry.path().strip_prefix(root).unwrap().to_path_buf());
            }
        }
    }
    let mut found = Vec::new();
    visit(root, root, &mut found);
    found.sort();
    found
}

fn assert_installed(root: &Path, directory: &str) {
    let skill = root.join(directory).join(SKILL);
    for (name, expected) in FILES {
        assert_eq!(fs::read(skill.join(name)).unwrap(), expected, "{name}");
    }
    let mut expected: Vec<PathBuf> = FILES.iter().map(|(name, _)| PathBuf::from(name)).collect();
    expected.sort();
    assert_eq!(
        files_under(&skill),
        expected,
        "only self-contained text assets"
    );
}

#[test]
fn each_agent_installs_identical_embedded_skill_without_external_tools() {
    for (agent, project_dir, _) in AGENTS {
        let fixture = Fixture::new();
        success(&fixture.run(&fixture.project, &["--agent", agent]));
        assert_installed(&fixture.project, project_dir);
        assert!(fs::read_dir(&fixture.home).unwrap().next().is_none());
    }
}

#[test]
fn each_agent_global_scope_uses_personal_skill_directory() {
    for (agent, _, personal_dir) in AGENTS {
        let fixture = Fixture::new();
        success(&fixture.run(&fixture.project, &["--agent", agent, "--global"]));
        assert_installed(&fixture.home, personal_dir);
        assert!(fs::read_dir(&fixture.project).unwrap().next().is_none());
    }
}

#[test]
fn repeated_install_accepts_identical_existing_files() {
    let fixture = Fixture::new();
    for _ in 0..2 {
        success(&fixture.run(&fixture.project, &["--agent", "codex"]));
        assert_installed(&fixture.project, ".agents/skills");
    }
}

#[test]
fn default_scope_finds_ancestor_repository_directory_or_worktree_file() {
    for worktree_file in [false, true] {
        let fixture = Fixture::new();
        if worktree_file {
            fs::write(
                fixture.project.join(".git"),
                "gitdir: /unused/test-worktree\n",
            )
            .unwrap();
        } else {
            fs::create_dir(fixture.project.join(".git")).unwrap();
        }
        let nested = fixture.project.join("packages/nested");
        fs::create_dir_all(&nested).unwrap();
        success(&fixture.run(&nested, &["--agent", "codex"]));
        assert_installed(&fixture.project, ".agents/skills");
        assert!(!nested.join(".agents").exists());
    }
}

#[test]
fn explicit_dir_is_exact_even_inside_an_ancestor_repository() {
    let fixture = Fixture::new();
    fs::create_dir(fixture.project.join(".git")).unwrap();
    let selected = fixture.project.join("nested selected root");
    fs::create_dir(&selected).unwrap();
    success(&fixture.run(
        &fixture.project,
        &["--agent", "codex", "--dir", "nested selected root"],
    ));
    assert_installed(&selected, ".agents/skills");
    assert!(!fixture.project.join(".agents").exists());
}

#[test]
fn conflicting_reference_refuses_install_before_writing_skill() {
    let fixture = Fixture::new();
    let edited = fixture
        .project
        .join(".agents/skills")
        .join(SKILL)
        .join("references/specification.md");
    fs::create_dir_all(edited.parent().unwrap()).unwrap();
    fs::write(&edited, "developer customizations\n").unwrap();
    let before = files_under(&fixture.project);
    refused(&fixture.run(&fixture.project, &["--agent", "codex"]));
    assert_eq!(fs::read(&edited).unwrap(), b"developer customizations\n");
    assert_eq!(
        files_under(&fixture.project),
        before,
        "no partially installed files"
    );
}

#[test]
fn repeated_install_preserves_a_later_local_edit() {
    let fixture = Fixture::new();
    success(&fixture.run(&fixture.project, &["--agent", "codex"]));
    let skill = fixture.project.join(".agents/skills").join(SKILL);
    fs::write(skill.join("SKILL.md"), "my customized skill\n").unwrap();
    refused(&fixture.run(&fixture.project, &["--agent", "codex"]));
    assert_eq!(
        fs::read(skill.join("SKILL.md")).unwrap(),
        b"my customized skill\n"
    );
    assert_eq!(fs::read(skill.join(FILES[1].0)).unwrap(), FILES[1].1);
}

#[test]
fn dry_run_reports_plan_without_creating_any_files() {
    let fixture = Fixture::new();
    for scope in [vec![], vec!["--global"]] {
        let mut args = vec!["--agent", "codex", "--dry-run"];
        args.extend(scope);
        let value = success(&fixture.run(&fixture.project, &args));
        assert_eq!(value["dry_run"], true);
        assert!(fs::read_dir(&fixture.project).unwrap().next().is_none());
        assert!(fs::read_dir(&fixture.home).unwrap().next().is_none());
    }
}

#[test]
fn ambiguous_scope_or_missing_agent_is_rejected_without_writes() {
    let fixture = Fixture::new();
    for args in [
        vec![],
        vec!["--agent", "unsupported-agent"],
        vec!["--agent", "codex", "--global", "--dir", "."],
    ] {
        refused(&fixture.run(&fixture.project, &args));
        assert!(fs::read_dir(&fixture.project).unwrap().next().is_none());
        assert!(fs::read_dir(&fixture.home).unwrap().next().is_none());
    }
}

#[cfg(unix)]
#[test]
fn symlinked_target_directory_or_payload_is_refused_without_following_it() {
    use std::os::unix::fs::symlink;
    for payload_link in [false, true] {
        let fixture = Fixture::new();
        let outside = fixture.temp.path().join("unrelated");
        fs::create_dir(&outside).unwrap();
        let sentinel = outside.join("keep.md");
        fs::write(&sentinel, "unrelated bytes\n").unwrap();
        if payload_link {
            let skill = fixture.project.join(".agents/skills").join(SKILL);
            fs::create_dir_all(&skill).unwrap();
            symlink(&sentinel, skill.join("SKILL.md")).unwrap();
        } else {
            symlink(&outside, fixture.project.join(".agents")).unwrap();
        }
        refused(&fixture.run(&fixture.project, &["--agent", "codex"]));
        assert_eq!(files_under(&outside), vec![PathBuf::from("keep.md")]);
        assert_eq!(fs::read(&sentinel).unwrap(), b"unrelated bytes\n");
    }
}

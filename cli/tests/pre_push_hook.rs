use std::env;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use tempfile::TempDir;

const HOOK: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../.githooks/pre-push");
const REPO: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/..");

fn git(dir: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .args(args)
        .current_dir(dir)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_string()
}

fn executable(path: &Path, body: &str) {
    fs::write(path, body).unwrap();
    let mut permissions = fs::metadata(path).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).unwrap();
}

fn copy_tree(source: &Path, destination: &Path) {
    fs::create_dir_all(destination).unwrap();
    for entry in fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let target = destination.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &target);
        } else {
            fs::copy(entry.path(), target).unwrap();
        }
    }
}

struct Fixture {
    temp: TempDir,
    root: PathBuf,
    bin: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("checkout");
        fs::create_dir_all(root.join("runner")).unwrap();
        fs::create_dir_all(root.join("apps/ui")).unwrap();
        fs::create_dir(root.join(".githooks")).unwrap();
        fs::write(root.join("runner/Dockerfile"), "FROM scratch\n").unwrap();
        fs::copy(HOOK, root.join(".githooks/pre-push")).unwrap();
        git(&root, &["init", "-q", "-b", "main"]);
        git(&root, &["config", "user.email", "test@example.com"]);
        git(&root, &["config", "user.name", "Example"]);
        git(&root, &["add", "."]);
        git(&root, &["commit", "-qm", "Add source checkout fixture"]);

        let bin = temp.path().join("bin");
        fs::create_dir(&bin).unwrap();
        // Packaging and dependency acquisition are external to the Git config
        // change. Keep those tools offline while running the real CLI handler
        // and the real Git configuration command.
        for tool in ["cargo", "uv", "pnpm", "docker"] {
            executable(&bin.join(tool), "#!/bin/sh\nexit 0\n");
        }
        Self { temp, root, bin }
    }

    fn run(&self, dir: &Path, args: &[&str]) -> Output {
        let path = env::join_paths(
            std::iter::once(self.bin.clone()).chain(env::split_paths(&env::var_os("PATH").unwrap())),
        )
        .unwrap();
        Command::new(env!("CARGO_BIN_EXE_curie"))
            .args(args)
            .current_dir(dir)
            .env("PATH", path)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .output()
            .unwrap()
    }

    fn worktree(&self) -> PathBuf {
        let side = self.temp.path().join("side");
        git(
            &self.root,
            &["worktree", "add", "-qb", "side", side.to_str().unwrap()],
        );
        side
    }

    fn prepare_preflight(&self) {
        let source = Path::new(REPO);
        copy_tree(
            &source.join(".github/workflows"),
            &self.root.join(".github/workflows"),
        );
        fs::create_dir(self.root.join("scripts")).unwrap();
        fs::copy(
            source.join("scripts/check-commit-messages.sh"),
            self.root.join("scripts/check-commit-messages.sh"),
        )
        .unwrap();
        fs::copy(source.join(".gitleaks.toml"), self.root.join(".gitleaks.toml")).unwrap();
        fs::create_dir_all(self.root.join("tools/preflight")).unwrap();
        fs::copy(
            source.join("tools/preflight/preflight.py"),
            self.root.join("tools/preflight/preflight.py"),
        )
        .unwrap();
        git(&self.root, &["add", "."]);
        git(&self.root, &["commit", "-qm", "Add real preflight inputs"]);
        git(&self.root, &["update-ref", "refs/remotes/origin/main", "HEAD"]);
        fs::create_dir(self.root.join("docs")).unwrap();
        fs::write(self.root.join("docs/example.md"), "Example notes\n").unwrap();
        git(&self.root, &["add", "docs/example.md"]);
        git(&self.root, &["commit", "-qm", "Add example notes"]);
    }

    fn real_cli(&self, args: &[&str]) -> Output {
        // Unlike install's external acquisition tools, preflight's checks use
        // the real uv executable and pinned gitleaks image end to end.
        Command::new(env!("CARGO_BIN_EXE_curie"))
            .args(args)
            .current_dir(&self.root)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .output()
            .unwrap()
    }
}

fn visible(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

#[test]
fn install_sets_relative_hook_path_for_existing_and_future_worktrees() {
    let fixture = Fixture::new();
    let output = fixture.run(&fixture.root, &["install"]);
    assert!(output.status.success(), "{}", visible(&output));
    assert_eq!(
        git(&fixture.root, &["config", "--local", "--get", "core.hooksPath"]),
        ".githooks"
    );
    let side = fixture.worktree();
    assert_eq!(git(&side, &["config", "--get", "core.hooksPath"]), ".githooks");

    let rerun = fixture.run(&side, &["install", "--update"]);
    assert!(rerun.status.success(), "{}", visible(&rerun));
    assert!(!visible(&rerun).to_lowercase().contains("warning"));
}

#[test]
fn update_from_linked_worktree_sets_repository_relative_hook_path() {
    let fixture = Fixture::new();
    let side = fixture.worktree();
    let output = fixture.run(&side, &["update"]);
    assert!(output.status.success(), "{}", visible(&output));
    assert_eq!(git(&side, &["config", "--get", "core.hooksPath"]), ".githooks");
    assert_eq!(
        git(&fixture.root, &["config", "--get", "core.hooksPath"]),
        ".githooks"
    );
    let rerun = fixture.run(&side, &["update"]);
    assert!(rerun.status.success(), "{}", visible(&rerun));
    assert!(!visible(&rerun).to_lowercase().contains("warning"));
}

#[test]
fn install_and_update_keep_custom_hook_path_and_print_one_warning() {
    for command in ["install", "update"] {
        let fixture = Fixture::new();
        git(&fixture.root, &["config", "core.hooksPath", "custom-hooks"]);
        let side = fixture.worktree();
        let output = fixture.run(&side, &[command]);
        let text = visible(&output);
        assert!(output.status.success(), "{command}: {text}");
        assert_eq!(
            git(&fixture.root, &["config", "--get", "core.hooksPath"]),
            "custom-hooks"
        );
        assert_eq!(
            git(&side, &["config", "--get", "core.hooksPath"]),
            "custom-hooks"
        );
        let warnings: Vec<_> = text
            .lines()
            .filter(|line| {
                line.to_lowercase().contains("warning") && line.contains("core.hooksPath")
            })
            .collect();
        assert_eq!(warnings.len(), 1, "{command}: {text}");
        assert!(warnings[0].contains("custom-hooks"), "{command}: {text}");
    }
}

#[test]
fn explicit_hooks_installer_uses_the_same_relative_path() {
    let fixture = Fixture::new();
    let side = fixture.worktree();
    let output = fixture.run(&side, &["dev", "hooks", "install"]);
    assert!(output.status.success(), "{}", visible(&output));
    assert_eq!(git(&side, &["config", "--get", "core.hooksPath"]), ".githooks");
    assert_eq!(
        git(&fixture.root, &["config", "--get", "core.hooksPath"]),
        ".githooks"
    );
}

#[test]
fn cli_preflight_json_dry_run_matches_the_real_python_tool() {
    let fixture = Fixture::new();
    fixture.prepare_preflight();
    let cli = fixture.real_cli(&["dev", "preflight", "--fast", "--dry-run", "--json"]);
    assert!(cli.status.success(), "{}", visible(&cli));
    let python = Command::new("uv")
        .args([
            "run",
            "--no-project",
            "--with",
            "pyyaml==6.0.3",
            "python3",
            "tools/preflight/preflight.py",
            "--fast",
            "--dry-run",
            "--json",
        ])
        .current_dir(&fixture.root)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .output()
        .unwrap();
    assert!(python.status.success(), "{}", visible(&python));
    let cli_report: serde_json::Value = serde_json::from_slice(&cli.stdout).unwrap();
    let python_report: serde_json::Value = serde_json::from_slice(&python.stdout).unwrap();
    assert_eq!(cli_report["checks"], python_report["checks"]);
    assert_eq!(cli_report["dry_run"], true);
    assert_eq!(cli_report["checks"].as_array().unwrap().len(), 3);
}

#[test]
fn cli_preflight_missing_fast_is_usage_error_with_actionable_json() {
    let fixture = Fixture::new();
    fixture.prepare_preflight();
    let output = fixture.real_cli(&["dev", "preflight", "--json"]);
    assert_eq!(output.status.code(), Some(2), "{}", visible(&output));
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(report["error"].as_str().is_some_and(|value| !value.is_empty()));
    assert!(report["fix"].as_str().is_some_and(|value| value.contains("--fast")));
}

#[test]
fn cli_preflight_propagates_failing_check_exit_and_json_report() {
    let fixture = Fixture::new();
    fixture.prepare_preflight();
    git(
        &fixture.root,
        &[
            "commit",
            "--amend",
            "-qm",
            "Add example notes\n\nCo-Authored-By: Codex <test@example.com>",
        ],
    );
    let output = fixture.real_cli(&["dev", "preflight", "--fast", "--json"]);
    assert_eq!(output.status.code(), Some(1), "{}", visible(&output));
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["passed"], false);
    let failures = report["failures"].as_array().unwrap();
    let attribution = failures
        .iter()
        .find(|failure| {
            failure["check"]
                .as_str()
                .is_some_and(|check| check.contains("Check the PR's commit messages"))
        })
        .expect("the CLI must name the commit messages check");
    assert!(attribution["output_tail"]
        .as_str()
        .unwrap()
        .contains("AI Co-Authored-By trailer"));
}

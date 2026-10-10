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
        fs::write(root.join("apps/ui/package.json"), "{}\n").unwrap();
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
            std::iter::once(self.bin.clone())
                .chain(env::split_paths(&env::var_os("PATH").unwrap())),
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
        fs::copy(
            source.join(".gitleaks.toml"),
            self.root.join(".gitleaks.toml"),
        )
        .unwrap();
        fs::create_dir_all(self.root.join("tools/preflight")).unwrap();
        fs::copy(
            source.join("tools/preflight/preflight.py"),
            self.root.join("tools/preflight/preflight.py"),
        )
        .unwrap();
        fs::write(
            self.root.join("pyproject.toml"),
            "[tool.uv.workspace]\nmembers = []\n",
        )
        .unwrap();
        fs::write(self.root.join("tools/preflight/always.txt"), "").unwrap();
        for relative in [
            "tools/e2e-ci-selection/select_tiers.py",
            ".github/e2e-selection.yaml",
            "release/atlas.py",
            "cli/Cargo.toml",
        ] {
            let destination = self.root.join(relative);
            fs::create_dir_all(destination.parent().unwrap()).unwrap();
            fs::copy(source.join(relative), destination).unwrap();
        }
        git(&self.root, &["add", "."]);
        git(&self.root, &["commit", "-qm", "Add real preflight inputs"]);
        let remote = self.temp.path().join("origin.git");
        git(
            &self.root,
            &["init", "--bare", "-q", remote.to_str().unwrap()],
        );
        git(
            &self.root,
            &["remote", "add", "origin", remote.to_str().unwrap()],
        );
        git(&self.root, &["push", "-q", "origin", "main"]);
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

    fn recorded_gh(&self, fail_api: bool) -> PathBuf {
        let bin = self.temp.path().join("recorded-gh");
        fs::create_dir(&bin).unwrap();
        // Sanitized fields from GitHub's documented provider responses. Only
        // the external GitHub boundary is replayed; Git, uv and every fast
        // gate execute their real commands.
        // https://docs.github.com/en/rest/repos/rules#get-rules-for-a-branch
        // https://docs.github.com/en/rest/checks/runs#list-check-runs-for-a-git-reference
        // https://docs.github.com/en/rest/branches/branch-protection#get-status-checks-protection
        let script = r#"#!/usr/bin/env python3
import json
import sys

args = sys.argv[1:]
if args[:2] == ["repo", "view"]:
    print("acme-corp/acme-bot")
    raise SystemExit(0)
if not args or args[0] != "api":
    raise SystemExit("unexpected recorded gh invocation")
if FAIL_API:
    raise SystemExit("recorded GitHub API unavailable")
endpoint = args[1].lstrip("/")
if "/protection/required_status_checks" in endpoint:
    print(json.dumps({"message": "Branch not protected", "status": "404"}))
    print("gh: Branch not protected (HTTP 404)", file=sys.stderr)
    raise SystemExit(1)
if "/rules/branches/" in endpoint:
    response = [{"type": "required_status_checks", "parameters": {
        "required_status_checks": [{"context": "Python (ruff + mypy + pytest)"}]
    }}]
elif "/check-runs" in endpoint:
    response = {"total_count": 1, "check_runs": [{
        "name": "Python (ruff + mypy + pytest)",
        "status": "completed", "conclusion": "success"
    }]}
else:
    raise SystemExit("unexpected recorded gh endpoint: " + endpoint)
print(json.dumps(response))
"#;
        executable(
            &bin.join("gh"),
            &script.replace("FAIL_API", if fail_api { "True" } else { "False" }),
        );
        bin
    }

    fn real_cli_with_gh(&self, args: &[&str], gh_bin: &Path) -> Output {
        let path = env::join_paths(
            std::iter::once(gh_bin.to_path_buf())
                .chain(env::split_paths(&env::var_os("PATH").unwrap())),
        )
        .unwrap();
        Command::new(env!("CARGO_BIN_EXE_curie"))
            .args(args)
            .current_dir(&self.root)
            .env("PATH", path)
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

fn assert_preflight_schema(report: &serde_json::Value) {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("schema/preflight.schema.json");
    let schema: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&path).unwrap_or_else(|error| {
            panic!("committed schema {} must exist: {error}", path.display())
        }))
        .expect("preflight schema must be JSON");
    let validator = jsonschema::validator_for(&schema).expect("preflight schema must compile");
    assert!(
        validator.is_valid(report),
        "actual CLI output must validate against {}: {report}",
        path.display()
    );
    for required in ["checks", "failures"] {
        let mut malformed = report.clone();
        malformed.as_object_mut().unwrap().remove(required);
        assert!(
            !validator.is_valid(&malformed),
            "preflight schema must reject a result missing {required}"
        );
    }
    if !report["failures"].as_array().unwrap().is_empty() {
        let mut malformed = report.clone();
        malformed["failures"][0]
            .as_object_mut()
            .unwrap()
            .remove("output_tail");
        assert!(
            !validator.is_valid(&malformed),
            "preflight schema must require the failing check's output tail"
        );
    }
}

#[test]
fn install_sets_relative_hook_path_for_existing_and_future_worktrees() {
    let fixture = Fixture::new();
    let output = fixture.run(&fixture.root, &["install"]);
    assert!(output.status.success(), "{}", visible(&output));
    assert_eq!(
        git(
            &fixture.root,
            &["config", "--local", "--get", "core.hooksPath"]
        ),
        ".githooks"
    );
    let side = fixture.worktree();
    assert_eq!(
        git(&side, &["config", "--get", "core.hooksPath"]),
        ".githooks"
    );

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
    assert_eq!(
        git(&side, &["config", "--get", "core.hooksPath"]),
        ".githooks"
    );
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
    assert_eq!(
        git(&side, &["config", "--get", "core.hooksPath"]),
        ".githooks"
    );
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
    assert_preflight_schema(&cli_report);
    assert_eq!(cli_report["checks"], python_report["checks"]);
    assert_eq!(cli_report["dry_run"], true);
    assert_eq!(cli_report["checks"].as_array().unwrap().len(), 3);
}

#[test]
fn cli_preflight_defaults_to_full_and_executes_docs_only_without_services() {
    let fixture = Fixture::new();
    fixture.prepare_preflight();
    let gh_bin = fixture.recorded_gh(false);
    let worktrees = git(&fixture.root, &["worktree", "list", "--porcelain"]);
    let output = fixture.real_cli_with_gh(&["dev", "preflight", "--json"], &gh_bin);
    assert!(output.status.success(), "{}", visible(&output));
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_preflight_schema(&report);
    assert_eq!(report["passed"], true);
    assert_eq!(report["tier"], "full");
    assert_eq!(report["dry_run"], false);
    assert_eq!(report["base"]["contains_tip"], true);
    assert_eq!(
        report["base"]["failing_required_checks"],
        serde_json::json!([])
    );
    assert_eq!(report["failures"], serde_json::json!([]));
    assert_eq!(report["ci_only"], serde_json::json!([]));
    let checks = report["checks"].as_array().unwrap();
    for job in ["action-pins", "commit-messages", "gitleaks"] {
        let check = checks
            .iter()
            .find(|check| check["job"] == job)
            .unwrap_or_else(|| panic!("full default must execute {job}: {report}"));
        assert_eq!(check["status"], "passed", "{report}");
    }
    for name in ["PR body", "Fix pin", "Affected Python tests"] {
        let check = checks
            .iter()
            .find(|check| check["name"] == name)
            .unwrap_or_else(|| panic!("full default must report {name}: {report}"));
        assert_eq!(check["status"], "skipped", "{report}");
    }
    assert_eq!(
        git(&fixture.root, &["worktree", "list", "--porcelain"]),
        worktrees,
        "full execution must remove its detached verification checkout"
    );
}

#[test]
fn cli_preflight_unavailable_github_preserves_transient_exit_and_actionable_json() {
    let fixture = Fixture::new();
    fixture.prepare_preflight();
    let gh_bin = fixture.recorded_gh(true);
    let output = fixture.real_cli_with_gh(&["dev", "preflight", "--dry-run", "--json"], &gh_bin);
    assert_eq!(output.status.code(), Some(3), "{}", visible(&output));
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_preflight_schema(&report);
    assert_eq!(report["passed"], false);
    assert_eq!(report["tier"], "full");
    assert!(report["error"]
        .as_str()
        .is_some_and(|value| value.contains("recorded GitHub API unavailable")));
    assert!(report["fix"]
        .as_str()
        .is_some_and(|value| !value.is_empty()));
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
    assert_preflight_schema(&report);
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

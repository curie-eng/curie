use std::env;
use std::fs;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

use tempfile::TempDir;

const HOOK: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../hooks/pre-push");

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

struct Fixture {
    temp: TempDir,
    base: String,
    bin: PathBuf,
    log: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let temp = TempDir::new().unwrap();
        let dir = temp.path();
        git(dir, &["init", "-q"]);
        git(dir, &["config", "core.hooksPath", "/dev/null"]);
        git(dir, &["config", "user.email", "test@example.com"]);
        git(dir, &["config", "user.name", "Example"]);
        fs::create_dir(dir.join("cli")).unwrap();
        fs::write(dir.join("untouched.py"), "pass\n").unwrap();
        git(dir, &["add", "."]);
        git(dir, &["commit", "-qm", "base"]);
        let base = git(dir, &["rev-parse", "HEAD"]);
        git(dir, &["update-ref", "refs/remotes/origin/main", &base]);

        let bin = dir.join("bin");
        fs::create_dir(&bin).unwrap();
        executable(
            &bin.join("cargo"),
            "#!/bin/sh\nprintf 'cargo|%s|%s\n' \"$PWD\" \"$*\" >> \"$HOOK_LOG\"\n[ \"$HOOK_FAIL\" != cargo ]\n",
        );
        executable(
            &bin.join("uv"),
            "#!/bin/sh\nprintf 'uv|%s|%s\n' \"$PWD\" \"$*\" >> \"$HOOK_LOG\"\nif [ \"$HOOK_FAIL\" = format ] && [ \"$3\" = format ]; then exit 1; fi\nexit 0\n",
        );
        let log = dir.join("calls.log");
        Self {
            temp,
            base,
            bin,
            log,
        }
    }

    fn dir(&self) -> &Path {
        self.temp.path()
    }

    fn commit(&self, path: &str) -> String {
        let destination = self.dir().join(path);
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(destination, "pass\n").unwrap();
        git(self.dir(), &["add", path]);
        git(self.dir(), &["commit", "-qm", "change"]);
        git(self.dir(), &["rev-parse", "HEAD"])
    }

    fn push(&self, lines: &str, fail: &str) -> Output {
        let path = format!("{}:{}", self.bin.display(), env::var("PATH").unwrap());
        let mut child = Command::new("python3")
            .arg(HOOK)
            .arg("origin")
            .arg("unused-url")
            .current_dir(self.dir())
            .env("PATH", path)
            .env("HOOK_LOG", &self.log)
            .env("HOOK_FAIL", fail)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(lines.as_bytes())
            .unwrap();
        child.wait_with_output().unwrap()
    }

    fn calls(&self) -> String {
        fs::read_to_string(&self.log).unwrap_or_default()
    }
}

#[test]
fn new_branch_checks_only_python_and_cli_rust_touched_by_pushed_commits() {
    let fixture = Fixture::new();
    fixture.commit("space name.py");
    let head = fixture.commit("cli/new.rs");
    let lines = format!(
        "refs/heads/topic {head} refs/heads/topic {zero}\nrefs/heads/topic2 {head} refs/heads/topic2 {zero}\n",
        zero = "0".repeat(40)
    );
    let output = fixture.push(&lines, "");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let calls = fixture.calls();
    assert!(calls.contains(&format!(
        "cargo|{}/cli|fmt --check",
        fixture.dir().display()
    )));
    assert!(calls.contains("uv|"));
    assert!(calls.contains("run ruff format --force-exclude --check ./space name.py"));
    assert!(calls.contains("run ruff check --force-exclude ./space name.py"));
    assert!(!calls.contains("untouched.py"));
}

#[test]
fn existing_remote_ref_selects_commits_after_its_exact_sha_and_reports_fix() {
    let fixture = Fixture::new();
    let head = fixture.commit("new.py");
    let lines = format!(
        "refs/heads/topic {head} refs/heads/topic {}\n",
        fixture.base
    );
    let output = fixture.push(&lines, "format");
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("Fix: uv run ruff format --force-exclude ./new.py"),
        "{stderr}"
    );
    assert!(fixture
        .calls()
        .contains("run ruff check --force-exclude ./new.py"));
}

#[test]
fn deletion_and_unknown_remote_commit_do_not_silently_pass() {
    let fixture = Fixture::new();
    let zeros = "0".repeat(40);
    let deletion = format!("(delete) {zeros} refs/heads/topic {}\n", fixture.base);
    assert!(fixture.push(&deletion, "").status.success());
    assert!(fixture.calls().is_empty());

    let head = fixture.commit("new.py");
    let lines = format!(
        "refs/heads/topic {head} refs/heads/topic {}\n",
        "1".repeat(40)
    );
    let output = fixture.push(&lines, "");
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("fetch the remote ref first"));
    assert!(fixture.calls().is_empty());
}

#[test]
fn uncommitted_fix_cannot_hide_bad_pushed_content() {
    let fixture = Fixture::new();
    let head = fixture.commit("new.py");
    fs::write(fixture.dir().join("new.py"), "formatted locally\n").unwrap();
    let lines = format!(
        "refs/heads/topic {head} refs/heads/topic {}\n",
        fixture.base
    );
    let output = fixture.push(&lines, "");
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("commit or restore those changes"));
    assert!(fixture.calls().is_empty());
}

#[test]
fn another_ref_is_rejected_with_a_clear_head_requirement() {
    let fixture = Fixture::new();
    let older = fixture.commit("old.py");
    fixture.commit("new.py");
    let lines = format!(
        "refs/heads/older {older} refs/heads/older {}\n",
        fixture.base
    );
    let output = fixture.push(&lines, "");
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("push the checked out HEAD"));
    assert!(fixture.calls().is_empty());
}

#[test]
fn docs_only_ref_does_not_require_checked_out_head() {
    let fixture = Fixture::new();
    let older = fixture.commit("notes.md");
    fixture.commit("new.py");
    let lines = format!("refs/heads/docs {older} refs/heads/docs {}\n", fixture.base);
    let output = fixture.push(&lines, "");
    assert!(output.status.success());
    assert!(fixture.calls().is_empty());
}

#[test]
fn annotated_tag_at_head_checks_its_commit() {
    let fixture = Fixture::new();
    fixture.commit("tagged.py");
    git(fixture.dir(), &["tag", "-am", "release", "v1"]);
    let tag = git(fixture.dir(), &["rev-parse", "refs/tags/v1"]);
    let lines = format!("refs/tags/v1 {tag} refs/tags/v1 {}\n", "0".repeat(40));
    let output = fixture.push(&lines, "");
    assert!(output.status.success());
    assert!(fixture
        .calls()
        .contains("run ruff check --force-exclude ./tagged.py"));
}

#[test]
fn installer_applies_to_worktrees_created_later() {
    let temp = TempDir::new().unwrap();
    let main = temp.path().join("main");
    let side = temp.path().join("side");
    fs::create_dir_all(main.join("runner")).unwrap();
    fs::create_dir(main.join("hooks")).unwrap();
    fs::write(main.join("runner/Dockerfile"), "FROM scratch\n").unwrap();
    fs::copy(HOOK, main.join("hooks/pre-push")).unwrap();
    git(&main, &["init", "-q"]);
    git(&main, &["config", "user.email", "test@example.com"]);
    git(&main, &["config", "user.name", "Example"]);
    git(&main, &["add", "."]);
    git(&main, &["commit", "-qm", "base"]);

    git(&main, &["config", "core.hooksPath", "/dev/null"]);
    let custom = Command::new(env!("CARGO_BIN_EXE_curie"))
        .args(["dev", "hooks", "install"])
        .current_dir(&main)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .output()
        .unwrap();
    assert!(!custom.status.success());
    assert_eq!(
        git(&main, &["config", "--get", "core.hooksPath"]),
        "/dev/null"
    );
    git(&main, &["config", "--unset", "core.hooksPath"]);

    let install = Command::new(env!("CARGO_BIN_EXE_curie"))
        .args(["dev", "hooks", "install"])
        .current_dir(&main)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .output()
        .unwrap();
    assert!(
        install.status.success(),
        "{}",
        String::from_utf8_lossy(&install.stderr)
    );
    git(
        &main,
        &["worktree", "add", "-qb", "side", side.to_str().unwrap()],
    );
    let expected = main.join("hooks").canonicalize().unwrap();
    assert_eq!(
        git(&main, &["config", "--get", "core.hooksPath"]),
        expected.display().to_string()
    );
    assert_eq!(
        git(&side, &["config", "--get", "core.hooksPath"]),
        expected.display().to_string()
    );
}

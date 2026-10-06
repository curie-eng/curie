"""Exercise fast preflight through its process boundary and real Git histories."""

import dataclasses
import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[3]
TOOL = ROOT / "tools/preflight/preflight.py"
COMMIT_STEP = "Check the PR's commit messages"
ALWAYS = {
    ("action-pins", "Require third party action SHA pins and version comments"),
    ("commit-messages", COMMIT_STEP),
    ("gitleaks", "Run gitleaks"),
}
PYTHON = {
    ("python", step)
    for step in (
        "Lockfile is up to date",
        "Alembic revision gate",
        "Schema window gate",
        "Ruff",
        "Mypy",
        "Import boundaries (harness SDK containment)",
    )
}
RUST = {
    ("rust-lint", step)
    for step in (
        "Version consistency",
        "Fmt",
        "Clippy",
        "Schema baseline",
        "Schema window gate",
        "Generated ACI protocol crate compiles",
    )
}
TYPESCRIPT = {
    ("contracts-ts", "Regenerate TypeScript from the committed schema and check for drift"),
    ("contracts-ts", "tsc --noEmit on generated ACI types"),
}


def run(root: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        args,
        cwd=root,
        env={**os.environ, "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1"},
        text=True,
        capture_output=True,
        check=False,
    )
    if check:
        assert result.returncode == 0, result.stdout + result.stderr
    return result


def git(root: Path, *args: str) -> str:
    return run(root, "git", *args).stdout.strip()


def commit(root: Path, path: str, message: str = "Add example notes") -> str:
    destination = root / path
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text("Example content\n")
    git(root, "add", path)
    git(root, "commit", "-qm", message)
    return git(root, "rev-parse", "HEAD")


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    root = tmp_path / "checkout"
    root.mkdir()
    shutil.copytree(ROOT / ".github/workflows", root / ".github/workflows")
    (root / "scripts").mkdir()
    shutil.copy2(ROOT / "scripts/check-commit-messages.sh", root / "scripts")
    shutil.copy2(ROOT / ".gitleaks.toml", root)
    git(root, "init", "-q", "-b", "main")
    git(root, "config", "user.name", "Example")
    git(root, "config", "user.email", "test@example.com")
    git(root, "config", "core.hooksPath", os.devnull)
    git(root, "add", ".")
    git(root, "commit", "-qm", "Add clean baseline")
    git(root, "update-ref", "refs/remotes/origin/main", "HEAD")
    return root


def preflight(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return run(repo, sys.executable, str(TOOL), "--fast", *args, check=False)


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("docs/example.md", ALWAYS),
        ("apps/api/example.py", ALWAYS | PYTHON),
        ("nested/pyproject.toml", ALWAYS | PYTHON),
        ("uv.lock", ALWAYS | PYTHON),
        ("apps/api/alembic/example.sql", ALWAYS | PYTHON),
        ("cli/src/example.rs", ALWAYS | RUST | {("ui", "Command manifest is current")}),
        ("apps/ui/src/example.ts", ALWAYS | {("ui", "Lint")}),
        ("packages/aci-protocol/schema/example.json", ALWAYS | RUST | TYPESCRIPT),
        ("packages/aci-protocol/example.py", ALWAYS | PYTHON | RUST | TYPESCRIPT),
    ],
)
def test_selection_follows_changed_path_rules(repo: Path, path: str, expected: set) -> None:
    commit(repo, path)
    result = preflight(repo, "--dry-run", "--json")
    assert result.returncode == 0, result.stdout + result.stderr
    plan = json.loads(result.stdout)
    assert plan["dry_run"] is True
    assert {(check["job"], check["step"]) for check in plan["checks"]} == expected
    for check in plan["checks"]:
        assert check["workflow"] in {"ci.yaml", "gitleaks.yaml"}
        assert check["command"]
        assert check["cwd"]
        assert check["group"]
        assert "cargo test" not in check["command"]
        # The gitleaks mount can include pytest's temporary-directory name.
        # Match executable tokens, including an absolute pytest executable.
        assert not re.search(r"(?:^|[\s;&|])(?:[^\s;&|]*/)?pytest(?:$|[\s;&|])", check["command"])
    scan = next(check for check in plan["checks"] if check["job"] == "gitleaks")
    workflow = yaml.safe_load((ROOT / ".github/workflows/gitleaks.yaml").read_text())
    assert workflow["jobs"]["gitleaks"]["env"]["GITLEAKS_IMAGE"] in scan["command"]
    assert "origin/main..HEAD" in scan["command"]


def test_selection_uses_three_dot_diff_and_requested_base_and_head(repo: Path) -> None:
    git(repo, "branch", "topic")
    commit(repo, "apps/api/main_only.py")
    git(repo, "update-ref", "refs/remotes/origin/next", "HEAD")
    git(repo, "checkout", "-q", "topic")
    docs = commit(repo, "docs/topic.md")
    commit(repo, "apps/ui/later.ts")
    result = preflight(repo, "--base", "next", "--head", docs, "--dry-run", "--json")
    assert result.returncode == 0, result.stdout + result.stderr
    plan = json.loads(result.stdout)
    assert {(check["job"], check["step"]) for check in plan["checks"]} == ALWAYS
    message = next(check for check in plan["checks"] if check["job"] == "commit-messages")
    assert f"origin/next..{docs}" in message["command"]


def load_tool():
    spec = importlib.util.spec_from_file_location("curie_preflight_test_subject", TOOL)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_drift_guard_accepts_real_workflows() -> None:
    load_tool().validate_workflows(ROOT)


@pytest.mark.parametrize("mutation", ["missing", "exact_command", "compile_ci_command"])
def test_drift_guard_rejects_mutated_workflow(tmp_path: Path, mutation: str) -> None:
    shutil.copytree(ROOT / ".github/workflows", tmp_path / ".github/workflows")
    path = tmp_path / ".github/workflows/ci.yaml"
    workflow = yaml.safe_load(path.read_text())
    if mutation == "compile_ci_command":
        job, name = "rust-lint", "Generated ACI protocol crate compiles"
    else:
        job, name = "python", "Ruff"
    steps = workflow["jobs"][job]["steps"]
    step = next(step for step in steps if step.get("name") == name)
    if mutation == "missing":
        steps.remove(step)
    else:
        step["run"] += " --example-mutated-flag"
    path.write_text(yaml.safe_dump(workflow, sort_keys=False))
    with pytest.raises(ValueError, match=name):
        load_tool().validate_workflows(tmp_path)


def test_drift_guard_rejects_mutated_compile_only_preflight_command(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_tool()
    name = "Generated ACI protocol crate compiles"
    replacement = tuple(
        dataclasses.replace(check, command="cargo check --locked") if check.step == name else check
        for check in module.CHECKS
    )
    monkeypatch.setattr(module, "CHECKS", replacement)
    with pytest.raises(ValueError, match=name):
        module.validate_workflows(ROOT)


def test_fast_rejects_attribution_and_names_check_with_output_tail(repo: Path) -> None:
    commit(repo, "docs/example.md", "Add notes\n\nCo-Authored-By: Codex <test@example.com>")
    result = preflight(repo, "--json")
    assert result.returncode != 0
    report = json.loads(result.stdout)
    failure = next(item for item in report["failures"] if COMMIT_STEP in item["check"])
    assert failure["exit_code"] != 0
    assert "AI Co-Authored-By trailer" in failure["output_tail"]
    assert report["passed"] is False


def test_fast_accepts_legitimate_assistant_identifier_in_commit_message(repo: Path) -> None:
    commit(repo, "docs/example.md", "Explain claude_agent_sdk settings")
    result = preflight(repo, "--json")
    assert result.returncode == 0, result.stdout + result.stderr
    report = json.loads(result.stdout)
    assert report["passed"] is True
    assert report["failures"] == []


@pytest.fixture
def manual_repo(repo: Path) -> Path:
    (repo / "tools/preflight").mkdir(parents=True)
    shutil.copy2(TOOL, repo / "tools/preflight/preflight.py")
    workflow = repo / ".github/workflows/preflight-identity.yaml"
    workflow.write_text(
        "name: Example action pin\n"
        "on: workflow_dispatch\n"
        "jobs:\n"
        "  example:\n"
        "    runs-on: ubuntu-latest\n"
        "    steps:\n"
        f"      - uses: acme-corp/acme-bot@{'a' * 40} # v1\n"
    )
    git(repo, "add", ".")
    git(repo, "commit", "-qm", "Add committed manual verification inputs")
    git(repo, "update-ref", "refs/remotes/origin/main", "HEAD")
    return repo


def test_manual_execution_dirty_tree_cannot_hide_committed_action_pin_failure(
    manual_repo: Path,
) -> None:
    repo = manual_repo
    workflow = repo / ".github/workflows/preflight-identity.yaml"
    clean_source = workflow.read_text()
    workflow.write_text(clean_source.replace("a" * 40, "v1"))
    git(repo, "add", ".github/workflows/preflight-identity.yaml")
    git(repo, "commit", "-qm", "Exercise committed action pin refusal")
    # Restoring valid bytes without committing them must not certify the bad tip.
    workflow.write_text(clean_source)
    worktrees = git(repo, "worktree", "list", "--porcelain")
    result = preflight(repo, "--json")
    assert result.returncode == 1, result.stdout + result.stderr
    report = json.loads(result.stdout)
    assert report["passed"] is False
    failure = next(item for item in report["failures"] if "action-pins" in item["check"])
    assert "third party action requires a full 40 hex commit SHA" in failure["output_tail"]
    assert workflow.read_text() == clean_source
    assert git(repo, "diff", "--name-only") == ".github/workflows/preflight-identity.yaml"
    assert git(repo, "worktree", "list", "--porcelain") == worktrees


def test_manual_execution_dirty_files_cannot_fail_a_clean_committed_head(
    manual_repo: Path,
) -> None:
    repo = manual_repo
    commit(repo, "docs/example.md")
    workflow = repo / ".github/workflows/preflight-identity.yaml"
    dirty_source = workflow.read_text().replace("a" * 40, "v1")
    workflow.write_text(dirty_source)
    worktrees = git(repo, "worktree", "list", "--porcelain")
    result = preflight(repo, "--json")
    assert result.returncode == 0, result.stdout + result.stderr
    report = json.loads(result.stdout)
    assert report["passed"] is True
    assert report["failures"] == []
    assert workflow.read_text() == dirty_source
    assert git(repo, "diff", "--name-only") == ".github/workflows/preflight-identity.yaml"
    assert git(repo, "worktree", "list", "--porcelain") == worktrees


def test_manual_execution_runs_checks_from_the_explicit_head(manual_repo: Path) -> None:
    repo = manual_repo
    workflow = repo / ".github/workflows/preflight-identity.yaml"
    clean_source = workflow.read_text()
    bad_source = clean_source.replace("a" * 40, "v1")
    workflow.write_text(bad_source)
    git(repo, "add", ".github/workflows/preflight-identity.yaml")
    git(repo, "commit", "-qm", "Exercise older action pin refusal")
    bad_head = git(repo, "rev-parse", "HEAD")
    workflow.write_text(clean_source)
    git(repo, "add", ".github/workflows/preflight-identity.yaml")
    git(repo, "commit", "-qm", "Repair example action pin")
    clean_head = git(repo, "rev-parse", "HEAD")
    worktrees = git(repo, "worktree", "list", "--porcelain")
    rejected = preflight(repo, "--head", bad_head, "--json")
    assert rejected.returncode == 1, rejected.stdout + rejected.stderr
    report = json.loads(rejected.stdout)
    assert any("action-pins" in item["check"] for item in report["failures"])
    assert git(repo, "worktree", "list", "--porcelain") == worktrees

    # Now the checked-out tip is bad again. Selecting the intervening clean
    # commit must succeed, proving --head governs execution in both directions.
    workflow.write_text(bad_source)
    git(repo, "add", ".github/workflows/preflight-identity.yaml")
    git(repo, "commit", "-qm", "Exercise newer action pin refusal")
    current_head = git(repo, "rev-parse", "HEAD")
    worktrees = git(repo, "worktree", "list", "--porcelain")
    accepted = preflight(repo, "--head", clean_head, "--json")
    assert accepted.returncode == 0, accepted.stdout + accepted.stderr
    report = json.loads(accepted.stdout)
    assert report["passed"] is True
    assert report["failures"] == []
    assert git(repo, "rev-parse", "HEAD") == current_head
    assert workflow.read_text() == bad_source
    assert git(repo, "worktree", "list", "--porcelain") == worktrees


def prepare_hook(repo: Path, tmp_path: Path) -> tuple[Path, Path]:
    (repo / ".githooks").mkdir()
    shutil.copy2(ROOT / ".githooks/pre-push", repo / ".githooks/pre-push")
    (repo / "tools/preflight").mkdir(parents=True)
    shutil.copy2(TOOL, repo / "tools/preflight/preflight.py")
    git(repo, "add", ".")
    git(repo, "commit", "-qm", "Add tracked preflight tooling")
    remote = tmp_path / "remote.git"
    git(tmp_path, "init", "--bare", "-q", str(remote))
    git(repo, "remote", "add", "origin", str(remote))
    git(repo, "push", "-q", "origin", "main")
    side = tmp_path / "side"
    git(repo, "worktree", "add", "-qb", "topic", str(side))
    git(side, "config", "core.hooksPath", ".githooks")
    return side, remote


@pytest.fixture
def ui_hook_repo(repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, Path]:
    # Copy the real authored sources and toolchain inputs, never an existing
    # dependency installation. Detached push verification owns its own setup.
    suffixes = {".js", ".mjs", ".ts", ".tsx", ".json", ".yaml", ".yml"}
    for relative in git(ROOT, "ls-files", "apps/ui").splitlines():
        path = Path(relative)
        if path.suffix not in suffixes:
            continue
        destination = repo / path
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / path, destination)
    assert not (repo / "apps/ui/node_modules").exists()
    if shutil.which("pnpm") is None:
        if shutil.which("npm") is None:
            pytest.fail("npm is required to acquire the real pnpm toolchain for this hook test")
        toolchain = tmp_path / "pnpm-toolchain"
        run(
            tmp_path,
            "npm",
            "install",
            "--prefix",
            str(toolchain),
            "--no-audit",
            "--no-fund",
            "pnpm@9.15.9",
        )
        private_bin = toolchain / "node_modules/.bin"
        monkeypatch.setenv("PATH", f"{private_bin}{os.pathsep}{os.environ['PATH']}")
    return prepare_hook(repo, tmp_path)


def test_tracked_hook_installs_ui_dependencies_and_runs_real_lint(
    ui_hook_repo: tuple[Path, Path],
) -> None:
    side, remote = ui_hook_repo
    path = side / "apps/ui/src/preflight-hook-fixture.js"
    path.write_text("// A harmless comment exercises a clean UI-only push.\n")
    git(side, "add", "apps/ui/src/preflight-hook-fixture.js")
    git(side, "commit", "-qm", "Add example UI notes")
    clean_head = git(side, "rev-parse", "HEAD")
    accepted = run(side, "git", "push", "origin", "topic", check=False)
    assert accepted.returncode == 0, accepted.stdout + accepted.stderr
    assert git(remote, "rev-parse", "refs/heads/topic") == clean_head
    worktrees = git(side, "worktree", "list", "--porcelain")
    assert len(re.findall(r"^worktree ", worktrees, re.M)) == 2

    # The real recommended JavaScript lint rule rejects an undefined symbol.
    # TypeScript's override disables no-undef, so use an authored JavaScript file.
    path.write_text("curiePreflightHookFixtureUndefined();\n")
    git(side, "add", "apps/ui/src/preflight-hook-fixture.js")
    git(side, "commit", "-qm", "Exercise UI lint refusal")
    rejected = run(side, "git", "push", "origin", "topic", check=False)
    assert rejected.returncode != 0
    output = rejected.stdout + rejected.stderr
    assert "ci.yaml:ui: Lint" in output
    assert "no-undef" in output
    assert git(remote, "rev-parse", "refs/heads/topic") == clean_head
    worktrees = git(side, "worktree", "list", "--porcelain")
    assert len(re.findall(r"^worktree ", worktrees, re.M)) == 2


def test_tracked_hook_rejects_bad_real_push_then_accepts_clean_push(
    repo: Path, tmp_path: Path
) -> None:
    side, remote = prepare_hook(repo, tmp_path)
    commit(side, "docs/topic.md", "Add notes\n\nCo-Authored-By: Codex <test@example.com>")
    rejected = run(side, "git", "push", "origin", "topic", check=False)
    assert rejected.returncode != 0
    assert COMMIT_STEP in rejected.stdout + rejected.stderr
    assert git(remote, "for-each-ref", "refs/heads/topic") == ""
    git(side, "commit", "--amend", "-qm", "Add clean notes")
    accepted = run(side, "git", "push", "origin", "topic", check=False)
    assert accepted.returncode == 0, accepted.stdout + accepted.stderr
    assert git(remote, "rev-parse", "refs/heads/topic") == git(side, "rev-parse", "HEAD")


def test_tracked_hook_checks_pushed_refs_own_tool_instead_of_unrelated_head(
    repo: Path, tmp_path: Path
) -> None:
    side, remote = prepare_hook(repo, tmp_path)
    docs = commit(side, "docs/topic.md")
    git(side, "branch", "docs", docs)
    # The current branch has different, broken tool code. Checking out the
    # pushed docs ref must use that ref's valid committed preflight instead.
    commit(
        side,
        "tools/preflight/preflight.py",
        "Add later tool notes\n\nCo-Authored-By: Codex <test@example.com>",
    )
    accepted = run(side, "git", "push", "origin", "docs:docs", check=False)
    assert accepted.returncode == 0, accepted.stdout + accepted.stderr
    assert git(remote, "rev-parse", "refs/heads/docs") == docs


def test_tracked_hook_uses_committed_head_tool_and_preserves_dirty_copy(
    repo: Path, tmp_path: Path
) -> None:
    side, remote = prepare_hook(repo, tmp_path)
    commit(side, "docs/topic.md", "Add notes\n\nCo-Authored-By: Codex <test@example.com>")
    local_tool = side / "tools/preflight/preflight.py"
    dirty_source = "This is deliberately invalid Python in the local working tree.\n"
    local_tool.write_text(dirty_source)
    assert git(side, "diff", "--name-only") == "tools/preflight/preflight.py"
    rejected = run(side, "git", "push", "origin", "topic", check=False)
    assert rejected.returncode != 0
    assert COMMIT_STEP in rejected.stdout + rejected.stderr
    assert git(remote, "for-each-ref", "refs/heads/topic") == ""
    assert local_tool.read_text() == dirty_source

    # The same dirty local source must not reject a valid committed tree.
    git(side, "commit", "--amend", "-qm", "Add clean notes")
    accepted = run(side, "git", "push", "origin", "topic", check=False)
    assert accepted.returncode == 0, accepted.stdout + accepted.stderr
    assert git(remote, "rev-parse", "refs/heads/topic") == git(side, "rev-parse", "HEAD")
    assert local_tool.read_text() == dirty_source


def test_tracked_hook_chooses_nearer_next_base(repo: Path, tmp_path: Path) -> None:
    side, remote = prepare_hook(repo, tmp_path)
    next_head = commit(
        repo, "docs/next.md", "Add next notes\n\nCo-Authored-By: Codex <test@example.com>"
    )
    git(repo, "update-ref", "refs/remotes/origin/next", next_head)
    git(side, "reset", "--hard", next_head)
    head = commit(side, "docs/topic.md")
    accepted = run(side, "git", "push", "origin", "topic", check=False)
    assert accepted.returncode == 0, accepted.stdout + accepted.stderr
    assert git(remote, "rev-parse", "refs/heads/topic") == head
    deleted = run(side, "git", "push", "origin", ":topic", check=False)
    assert deleted.returncode == 0, deleted.stdout + deleted.stderr
    assert git(remote, "for-each-ref", "refs/heads/topic") == ""


def test_tracked_hook_breaks_equal_commit_count_tie_toward_main(repo: Path, tmp_path: Path) -> None:
    side, remote = prepare_hook(repo, tmp_path)
    git(repo, "branch", "next")
    main_head = commit(
        repo, "docs/main.md", "Add main notes\n\nCo-Authored-By: Codex <test@example.com>"
    )
    git(repo, "checkout", "-q", "next")
    next_head = commit(repo, "docs/next.md")
    git(repo, "update-ref", "refs/remotes/origin/main", main_head)
    git(repo, "update-ref", "refs/remotes/origin/next", next_head)
    git(side, "reset", "--hard", main_head)
    git(side, "merge", "-q", "--no-edit", next_head)
    head = commit(side, "docs/topic.md")
    assert git(side, "rev-list", "--count", f"origin/main..{head}") == git(
        side, "rev-list", "--count", f"origin/next..{head}"
    )
    # Choosing main excludes its historical attribution; choosing next includes
    # that same bad commit and must reject the otherwise clean topic push.
    accepted = run(side, "git", "push", "origin", "topic", check=False)
    assert accepted.returncode == 0, accepted.stdout + accepted.stderr
    assert git(remote, "rev-parse", "refs/heads/topic") == head

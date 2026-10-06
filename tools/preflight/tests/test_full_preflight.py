"""Full preflight policy at real Git and guard-command boundaries."""

from __future__ import annotations

import fcntl
import hashlib
import importlib.util
import json
import os
import shlex
import shutil
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
TOOL = ROOT / "tools/preflight/preflight.py"
FIXTURES = Path(__file__).with_name("fixtures")
# Sanitized GitHub check-runs response fields, with conclusions seeded for
# success, failure and pending coverage. The provider owns this response shape:
# https://docs.github.com/en/rest/checks/runs#list-check-runs-for-a-git-reference
PYTHON_CHECK = "Python (ruff + mypy + pytest)"


def load_tool():
    spec = importlib.util.spec_from_file_location("curie_full_preflight_test_subject", TOOL)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


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


def commit_paths(root: Path, paths: list[str]) -> str:
    for path in paths:
        destination = root / path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(destination.read_text() + "\n" if destination.exists() else "\n")
    git(root, "add", ".")
    git(root, "commit", "-qm", "Exercise selected paths")
    return git(root, "rev-parse", "HEAD")


@pytest.fixture
def repository(tmp_path: Path) -> Path:
    root = tmp_path / "checkout"
    root.mkdir()
    shutil.copytree(ROOT / ".github/workflows", root / ".github/workflows")
    shutil.copy2(ROOT / ".github/e2e-selection.yaml", root / ".github")
    for relative in (
        "scripts/check-pr-body.sh",
        "scripts/check-commit-messages.sh",
        "tools/fix-pin-ci/check.py",
        "tools/e2e-ci-selection/select_tiers.py",
        "release/atlas.py",
        "cli/Cargo.toml",
        "pyproject.toml",
        ".gitleaks.toml",
    ):
        destination = root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / relative, destination)
    (root / "tools/preflight").mkdir(parents=True, exist_ok=True)
    (root / "tools/preflight/always.txt").write_text(
        "apps/api/tests/test_nullable_override_parity.py\n"
    )
    manifest = tomllib.loads((ROOT / "pyproject.toml").read_text())
    for member in manifest["tool"]["uv"]["workspace"]["members"]:
        destination = root / member
        destination.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / member / "pyproject.toml", destination)
        if (ROOT / member / "tests").is_dir():
            (destination / "tests").mkdir()
            (destination / "tests/test_example.py").write_text("def test_example():\n    pass\n")
    (root / "apps/api/tests/test_nullable_override_parity.py").write_text(
        "def test_example():\n    pass\n"
    )
    git(root, "init", "-q", "-b", "main")
    git(root, "config", "user.name", "Example")
    git(root, "config", "user.email", "test@example.com")
    git(root, "config", "core.hooksPath", os.devnull)
    git(root, "add", ".")
    git(root, "commit", "-qm", "Add full preflight baseline")
    remote = tmp_path / "remote.git"
    git(tmp_path, "init", "--bare", "-q", str(remote))
    git(root, "remote", "add", "origin", str(remote))
    git(root, "push", "-q", "origin", "main")
    git(root, "checkout", "-qb", "topic")
    return root


@pytest.fixture
def recorded_gh(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Replay public GitHub API shapes, with no network or product fakes."""
    # The effective required_status_checks rule shape was recorded from the
    # GitHub branch-rules API and reduced to public context names. No rule IDs
    # or deployment identifiers are retained. Other responses exercise the
    # documented API fields with sanitized test values:
    # https://docs.github.com/en/rest/repos/rules#get-rules-for-a-branch
    # https://docs.github.com/en/rest/checks/runs#list-check-runs-for-a-git-reference
    # https://docs.github.com/en/rest/branches/branch-protection#get-status-checks-protection
    # https://docs.github.com/en/graphql/reference/objects#issue
    directory = tmp_path / "recorded-gh"
    directory.mkdir()
    executable = directory / "gh"
    executable.write_text(
        f"#!{sys.executable}\n"
        "import json, os, sys\n"
        "from pathlib import Path\n"
        "args = sys.argv[1:]\n"
        "if args[:2] == ['repo', 'view']:\n"
        "    value = {'nameWithOwner': 'acme-corp/acme-bot'}\n"
        "    print(value['nameWithOwner'] if '--jq' in args else json.dumps(value))\n"
        "    raise SystemExit(0)\n"
        "if not args or args[0] != 'api':\n"
        "    raise SystemExit('unexpected gh invocation: ' + repr(args))\n"
        "endpoint = args[1].lstrip('/')\n"
        "if '/check-runs' in endpoint:\n"
        "    if os.environ.get('PREFLIGHT_TEST_HEALTH_API_FAILURE') == '1':\n"
        "        raise SystemExit('recorded check runs API failure')\n"
        "    value = json.loads(Path(os.environ['PREFLIGHT_TEST_CHECK_RUNS']).read_text())\n"
        "    if os.environ.get('PREFLIGHT_TEST_RED_BASE') != '1':\n"
        "        for check in value['check_runs']:\n"
        "            check.update(status='completed', conclusion='success')\n"
        "elif '/protection/required_status_checks' in endpoint:\n"
        "    if os.environ.get('PREFLIGHT_TEST_CLASSIC_AUTH_FAILURE') == '1':\n"
        "        raise SystemExit('recorded classic protection auth failure (HTTP 403)')\n"
        "    if os.environ.get('PREFLIGHT_TEST_CLASSIC') == '1':\n"
        "        value = {'strict': True, 'contexts': ['Optional example'], "
        "'checks': [{'context': 'Optional example', 'app_id': 15368}]}\n"
        "    else:\n"
        "        print('gh: Branch not protected (HTTP 404)', file=sys.stderr)\n"
        "        print(json.dumps({'message': 'Branch not protected', 'status': '404'}))\n"
        "        raise SystemExit(1)\n"
        "elif '/rules/branches/' in endpoint:\n"
        "    value = [{'type': 'required_status_checks', 'parameters': "
        "{'required_status_checks': [{'context': name, 'integration_id': 15368} "
        "for name in ['Python (ruff + mypy + pytest)', 'E2E required', "
        "'Fix pin verification']]}}]\n"
        "elif '/issues/' in endpoint:\n"
        "    number = int(endpoint.rsplit('/', 1)[1].split('?')[0])\n"
        "    value = {'number': number, 'state': 'open', 'body': '', "
        "'labels': [{'name': 'bug'}]}\n"
        "    if '--jq' in args:\n"
        "        value = {'labels': ['bug'], 'body': ''}\n"
        "elif endpoint.endswith('/issues') or '/issues?' in endpoint:\n"
        "    value = []\n"
        "elif endpoint == 'graphql':\n"
        "    if os.environ.get('PREFLIGHT_TEST_FORBID_GRAPHQL') == '1':\n"
        "        raise SystemExit('unexpected hidden waiver lookup')\n"
        "    value = {'data': {'repository': {"
        "'issue77': {'__typename': 'Issue', 'number': 77, 'state': 'OPEN'}}}}\n"
        "else:\n"
        "    raise SystemExit('unexpected gh endpoint: ' + endpoint)\n"
        "print(json.dumps(value))\n"
    )
    executable.chmod(0o755)
    monkeypatch.setenv("PATH", f"{directory}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("PREFLIGHT_TEST_CHECK_RUNS", str(FIXTURES / "base-check-runs.json"))
    return executable


def full_preflight(repository: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return run(repository, sys.executable, str(TOOL), *args, check=False)


def test_base_freshness_fetches_moved_tip_without_merging(repository: Path) -> None:
    original = git(repository, "rev-parse", "HEAD")
    git(repository, "checkout", "-q", "main")
    tip = commit_paths(repository, ["docs/new-base.md"])
    git(repository, "push", "-q", "origin", "main")
    git(repository, "checkout", "-q", "topic")
    # A stale tracking ref must not let this branch pass.
    git(repository, "update-ref", "refs/remotes/origin/main", original)
    result = load_tool().check_base_freshness(repository, "main")
    assert result["contains_tip"] is False
    assert result["fix"] == "git merge origin/main"
    assert git(repository, "rev-parse", "origin/main") == tip
    assert git(repository, "rev-parse", "HEAD") == original
    assert git(repository, "status", "--porcelain") == ""


def test_base_freshness_accepts_contained_tip_and_requested_head(repository: Path) -> None:
    original = git(repository, "rev-parse", "HEAD")
    commit_paths(repository, ["docs/topic.md"])
    module = load_tool()
    assert module.check_base_freshness(repository, "main")["contains_tip"] is True
    assert module.check_base_freshness(repository, "main", original)["contains_tip"] is True


def test_base_freshness_cli_fails_with_exact_merge_command(
    repository: Path, recorded_gh: Path
) -> None:
    git(repository, "checkout", "-q", "main")
    commit_paths(repository, ["docs/base.md"])
    git(repository, "push", "-q", "origin", "main")
    git(repository, "checkout", "-q", "topic")
    result = full_preflight(repository, "--dry-run", "--json")
    assert result.returncode == 1, result.stdout + result.stderr
    report = json.loads(result.stdout)
    assert report["base"]["contains_tip"] is False
    assert "git merge origin/main" in json.dumps(report)


def test_base_freshness_merge_remedy_quotes_valid_branch_shell_metacharacters(
    repository: Path, recorded_gh: Path
) -> None:
    base = "main;echo"
    git(repository, "checkout", "-qb", base)
    commit_paths(repository, ["docs/base.md"])
    git(repository, "push", "-q", "origin", base)
    git(repository, "checkout", "-q", "topic")
    result = full_preflight(repository, "--base", base, "--dry-run", "--json")
    assert result.returncode == 1, result.stdout + result.stderr
    report = json.loads(result.stdout)
    assert report["base"]["contains_tip"] is False
    assert shlex.split(report["fix"]) == ["git", "merge", "origin/main;echo"]


def test_base_health_filters_recorded_checks_to_effective_required_rules(
    repository: Path, recorded_gh: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PREFLIGHT_TEST_RED_BASE", "1")
    assert load_tool().base_health(repository, "main", "acme-corp/acme-bot") == [PYTHON_CHECK]


def test_base_health_warning_preserves_success_exit_code(
    repository: Path, recorded_gh: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PREFLIGHT_TEST_RED_BASE", "1")
    commit_paths(repository, ["docs/example.md"])
    result = full_preflight(repository, "--dry-run", "--json")
    assert result.returncode == 0, result.stdout + result.stderr
    report = json.loads(result.stdout)
    assert report["base"]["failing_required_checks"] == [PYTHON_CHECK]
    assert "warning" in result.stderr.lower()
    assert PYTHON_CHECK in result.stderr


def test_base_health_api_failure_cannot_report_success(
    repository: Path, recorded_gh: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PREFLIGHT_TEST_HEALTH_API_FAILURE", "1")
    commit_paths(repository, ["docs/example.md"])
    result = full_preflight(repository, "--dry-run", "--json")
    assert result.returncode == 3, result.stdout + result.stderr
    report = json.loads(result.stdout)
    assert report["passed"] is False
    assert "recorded check runs API failure" in report["error"]
    assert report["fix"]


def test_base_health_combines_classic_and_effective_required_checks(
    repository: Path, recorded_gh: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PREFLIGHT_TEST_RED_BASE", "1")
    monkeypatch.setenv("PREFLIGHT_TEST_CLASSIC", "1")
    assert load_tool().base_health(repository, "main", "acme-corp/acme-bot") == [
        "Optional example",
        PYTHON_CHECK,
    ]


def test_base_health_classic_auth_failure_is_transient_error(
    repository: Path, recorded_gh: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PREFLIGHT_TEST_CLASSIC_AUTH_FAILURE", "1")
    commit_paths(repository, ["docs/example.md"])
    result = full_preflight(repository, "--dry-run", "--json")
    assert result.returncode == 3, result.stdout + result.stderr
    report = json.loads(result.stdout)
    assert report["passed"] is False
    assert "recorded classic protection auth failure" in report["error"]
    assert report["fix"]


def test_full_parser_bad_argument_is_usage_error(repository: Path) -> None:
    result = full_preflight(repository, "--invalid-preflight-option", "--json")
    assert result.returncode == 2, result.stdout + result.stderr
    report = json.loads(result.stdout)
    assert report["passed"] is False
    assert "unrecognized arguments" in report["error"]
    assert report["fix"]


def test_base_health_unavailable_gh_is_transient_error(
    repository: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    private_bin = tmp_path / "git-only-bin"
    private_bin.mkdir()
    git_binary = shutil.which("git")
    assert git_binary is not None, "The repository fixture requires real Git"
    (private_bin / "git").symlink_to(git_binary)
    monkeypatch.setenv("PATH", str(private_bin))
    result = full_preflight(repository, "--dry-run", "--json")
    assert result.returncode == 3, result.stdout + result.stderr
    report = json.loads(result.stdout)
    assert report["passed"] is False
    assert "gh" in report["error"].lower()
    assert report["fix"]


def test_full_unavailable_git_is_transient_error(
    repository: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    private_bin = tmp_path / "empty-bin"
    private_bin.mkdir()
    monkeypatch.setenv("PATH", str(private_bin))
    result = full_preflight(repository, "--dry-run", "--json")
    assert result.returncode == 3, result.stdout + result.stderr
    report = json.loads(result.stdout)
    assert report["passed"] is False
    assert "git" in report["error"].lower()
    assert report["fix"]


@pytest.mark.parametrize(
    ("number", "test_directory"),
    [(3332, "apps/api/tests"), (3265, "apps/worker/tests"), (3321, "packages/telemetry/tests")],
)
def test_selection_historical_changed_path_fixtures(number: int, test_directory: str) -> None:
    paths = (FIXTURES / f"pr-{number}-paths.txt").read_text().splitlines()
    assert test_directory in load_tool().select_python_tests(ROOT, paths)


@pytest.mark.parametrize(
    ("number", "test_directory"),
    [(3332, "apps/api/tests"), (3265, "apps/worker/tests"), (3321, "packages/telemetry/tests")],
)
def test_selection_historical_cli_dry_run_json(
    repository: Path, recorded_gh: Path, number: int, test_directory: str
) -> None:
    paths = (FIXTURES / f"pr-{number}-paths.txt").read_text().splitlines()
    commit_paths(repository, paths)
    result = full_preflight(repository, "--dry-run", "--json")
    assert result.returncode == 0, result.stdout + result.stderr
    report = json.loads(result.stdout)
    assert test_directory in json.dumps(report["checks"])
    assert all({"name", "status", "detail"} <= check.keys() for check in report["checks"])


def test_selection_docs_only_ignores_always_list() -> None:
    assert load_tool().select_python_tests(ROOT, ["docs/agents.md", "README.md"]) == []


@pytest.mark.parametrize("path", ["pyproject.toml", "uv.lock"])
def test_selection_root_workspace_inputs_select_all_member_tests(path: str) -> None:
    manifest = tomllib.loads((ROOT / "pyproject.toml").read_text())
    expected = {
        f"{member}/tests"
        for member in manifest["tool"]["uv"]["workspace"]["members"]
        if (ROOT / member / "tests").is_dir()
    }
    selected = set(load_tool().select_python_tests(ROOT, [path]))
    assert expected <= selected
    assert "apps/api/tests" in selected
    assert "apps/api/tests/test_nullable_override_parity.py" not in selected


def test_selection_always_file_is_not_duplicate_of_selected_parent_directory() -> None:
    selected = load_tool().select_python_tests(ROOT, ["apps/api/src/curie_api/config.py"])
    assert "apps/api/tests" in selected
    # Pytest already collects the parity file through this directory. Giving
    # both positions would execute it twice in the same shared test state.
    assert "apps/api/tests/test_nullable_override_parity.py" not in selected


def test_selection_member_readme_still_selects_its_package() -> None:
    assert "apps/api/tests" in load_tool().select_python_tests(ROOT, ["apps/api/README.md"])


def test_selection_rename_from_member_to_docs_keeps_previous_path(
    repository: Path, recorded_gh: Path
) -> None:
    (repository / "docs").mkdir()
    git(repository, "mv", "apps/api/tests/test_example.py", "docs/example.txt")
    git(repository, "commit", "-qm", "Move example documentation")
    result = full_preflight(repository, "--dry-run", "--json")
    assert result.returncode == 0, result.stdout + result.stderr
    report = json.loads(result.stdout)
    assert "apps/api/tests" in json.dumps(report["checks"])


def test_selection_raw_filename_survives_git_nul_diff(repository: Path, recorded_gh: Path) -> None:
    # Git's default name-only display quotes Unicode and control bytes. The
    # selection boundary must consume raw filenames, as CI's file API does.
    path = "apps/api/src/curie_api/preflight name é\nexample.py"
    commit_paths(repository, [path])
    result = full_preflight(repository, "--dry-run", "--json")
    assert result.returncode == 0, result.stdout + result.stderr
    report = json.loads(result.stdout)
    assert "apps/api/tests" in json.dumps(report["checks"])


def test_selection_transitive_dependents_and_always_entries(tmp_path: Path) -> None:
    (tmp_path / "pyproject.toml").write_text('[tool.uv.workspace]\nmembers = ["packages/*"]\n')
    for name, dependencies in (
        ("first-member", []),
        ("second-member", ["first_member>=1"]),
        ("third-member", ["second-member[feature]>=1; python_version >= '3.13'"]),
        ("unrelated", []),
    ):
        member = tmp_path / "packages" / name
        (member / "tests").mkdir(parents=True)
        (member / "tests/test_example.py").write_text("def test_example():\n    pass\n")
        (member / "pyproject.toml").write_text(
            f'[project]\nname = "{name}"\ndependencies = {json.dumps(dependencies)}\n'
        )
    always = tmp_path / "tools/preflight/always.txt"
    always.parent.mkdir(parents=True)
    always.write_text(
        "# Exercises package-independent parity.\npackages/unrelated/tests/test_example.py\n"
    )
    assert set(
        load_tool().select_python_tests(tmp_path, ["packages/first-member/src/example.py"])
    ) == {
        "packages/first-member/tests",
        "packages/second-member/tests",
        "packages/third-member/tests",
        "packages/unrelated/tests/test_example.py",
    }
    assert load_tool().select_python_tests(tmp_path, ["docs/example.md"]) == []


def check_body(repository: Path, body_file: Path) -> list[dict]:
    return load_tool().check_pr_body(
        repository,
        body_file,
        "Explain preflight",
        ["docs/example.md"],
        "main",
        git(repository, "rev-parse", "HEAD"),
        "acme-corp/acme-bot",
    )


def test_pr_body_literal_newline_uses_ci_guard_message(
    repository: Path, recorded_gh: Path, tmp_path: Path
) -> None:
    body = tmp_path / "body.md"
    body.write_text(r"Explain the change.\n\nCloses #77" + "\n")
    checks = check_body(repository, body)
    failed = [check for check in checks if check["status"] == "failed"]
    assert failed
    assert any(
        r"PR body check failed: found a literal \n escape sequence." in check["detail"]
        for check in failed
    )


def test_pr_body_missing_bug_fix_pin_uses_ci_requirement_message(
    repository: Path, recorded_gh: Path, tmp_path: Path
) -> None:
    body = tmp_path / "body.md"
    body.write_text("Explain the change.\n\nCloses #77\n")
    failed = [check for check in check_body(repository, body) if check["status"] == "failed"]
    assert failed
    expected = (
        "Fix pin required: this pull request closes bug issue(s) #77. "
        "Add a `Fix pin: <selector>` line naming a test this pull request "
        "changed, or an explicit `Fix pin: n/a - <reason>` line."
    )
    assert any(expected in check["detail"] for check in failed)


def test_pr_body_n_a_passes_without_building_release_binary(
    repository: Path, recorded_gh: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cargo = recorded_gh.with_name("cargo")
    cargo.write_text("#!/bin/sh\necho 'unexpected release build' >&2\nexit 97\n")
    cargo.chmod(0o755)
    # A non-executable selector is not needed for the explicit n/a declaration.
    monkeypatch.setenv("CARGO_TARGET_DIR", str(tmp_path / "absent-target"))
    body = tmp_path / "body.md"
    body.write_text("Explain the change.\n\nCloses #77\nFix pin: n/a - documentation only\n")
    checks = check_body(repository, body)
    assert checks
    assert all(check["status"] != "failed" for check in checks), checks
    assert not (tmp_path / "absent-target").exists()


def test_pr_body_without_file_reports_skipped(repository: Path, recorded_gh: Path) -> None:
    commit_paths(repository, ["docs/example.md"])
    result = full_preflight(repository, "--dry-run", "--json")
    assert result.returncode == 0, result.stdout + result.stderr
    report = json.loads(result.stdout)
    skipped = [check for check in report["checks"] if check["status"] == "skipped"]
    assert any("body" in check["name"].lower() for check in skipped)
    assert any("pin" in check["name"].lower() for check in skipped)


def test_pr_body_open_discovery_waiver_uses_issue_graphql_snapshot(
    repository: Path, recorded_gh: Path, tmp_path: Path
) -> None:
    body = tmp_path / "body.md"
    body.write_text(
        "Explain API behavior.\n\n"
        "Discovery waiver: Local infrastructure unavailable; tracked in #77\n"
    )
    checks = load_tool().check_pr_body(
        repository,
        body,
        "Explain API behavior",
        ["apps/api/src/curie_api/config.py"],
        "main",
        git(repository, "rev-parse", "HEAD"),
        "acme-corp/acme-bot",
    )
    assert checks
    assert all(check["status"] != "failed" for check in checks), checks


@pytest.mark.parametrize(
    "hidden_waiver",
    [
        "<!-- Discovery waiver: Local infrastructure unavailable; tracked in #77 -->\n",
        "```text\nDiscovery waiver: Local infrastructure unavailable; tracked in #77\n```\n",
        "~~~text\nDiscovery waiver: Local infrastructure unavailable; tracked in #77\n~~~\n",
        "    Discovery waiver: Local infrastructure unavailable; tracked in #77\n",
    ],
    ids=["html-comment", "backtick-fence", "tilde-fence", "indented-code"],
)
def test_pr_body_hidden_discovery_waiver_cannot_trigger_lookup_or_waive_tier(
    repository: Path,
    recorded_gh: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    hidden_waiver: str,
) -> None:
    monkeypatch.setenv("PREFLIGHT_TEST_FORBID_GRAPHQL", "1")
    body = tmp_path / "body.md"
    body.write_text("Explain API behavior.\n\n" + hidden_waiver)
    checks = load_tool().check_pr_body(
        repository,
        body,
        "Explain API behavior",
        ["apps/api/src/curie_api/config.py"],
        "main",
        git(repository, "rev-parse", "HEAD"),
        "acme-corp/acme-bot",
    )
    failed = [check for check in checks if check["status"] == "failed"]
    assert failed
    assert any("PR body check failed:" in check["detail"] for check in failed)
    assert all("unexpected hidden waiver lookup" not in check["detail"] for check in checks)


def test_ci_only_selector_reports_tiers_and_kind_jobs(repository: Path) -> None:
    path = "apps/worker/src/curie_worker/kernel.py"
    head = commit_paths(repository, [path])
    selected = load_tool().select_ci_only(repository, [path], "main", head)
    text = " ".join(selected)
    assert "local" in text
    assert "cluster" in text
    assert "approval" in text


def test_ci_only_cli_text_and_json_agree(repository: Path, recorded_gh: Path) -> None:
    commit_paths(repository, ["charts/curie/templates/api.yaml"])
    result = full_preflight(repository, "--dry-run", "--json")
    assert result.returncode == 0, result.stdout + result.stderr
    report = json.loads(result.stdout)
    assert any("cluster" in entry for entry in report["ci_only"])
    readable = full_preflight(repository, "--dry-run")
    assert readable.returncode == 0, readable.stdout + readable.stderr
    text = readable.stdout + readable.stderr
    assert "runs in CI only" in text
    assert all(entry in text for entry in report["ci_only"])


def test_ci_only_docs_change_has_no_tiers(repository: Path) -> None:
    path = "docs/example.md"
    head = commit_paths(repository, [path])
    assert load_tool().select_ci_only(repository, [path], "main", head) == []


def test_private_services_shared_worktree_lock_refuses_detached_checkout(
    tmp_path: Path,
) -> None:
    worktree = tmp_path / "stable-worktree"
    checkout = tmp_path / "detached-checkout"
    worktree.mkdir()
    checkout.mkdir()
    project = "curie-preflight-" + hashlib.sha256(str(worktree.resolve()).encode()).hexdigest()[:10]
    scratch = worktree / ".projects" / "preflight"
    scratch.mkdir(parents=True)
    # Each execution has its own detached checkout, while the Compose project
    # belongs to the stable worktree. An actual flock must exclude all of them.
    # No Compose source is present, so even a broken lock cannot start services.
    with (scratch / f"{project}.lock").open("w") as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(ValueError, match="Another full preflight owns"):
            with load_tool().private_services(checkout, worktree):
                pytest.fail("A second invocation acquired the existing owner's project")


@pytest.fixture
def docker_toolchain() -> str:
    executable = shutil.which("docker")
    if executable is None:
        pytest.fail("Docker is required for the real unavailable-daemon boundary test")
    return executable


def test_private_services_unavailable_daemon_is_transient_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, docker_toolchain: str
) -> None:
    worktree = tmp_path / "stable-worktree"
    worktree.mkdir()
    socket_path = tmp_path / "unavailable-docker.sock"
    assert not socket_path.exists()
    # Docker resolves this real but unavailable socket before any ownership
    # inventory or startup can reach a daemon. Do not replace the Docker tool.
    monkeypatch.setenv("DOCKER_HOST", f"unix://{socket_path}")
    monkeypatch.delenv("DOCKER_CONTEXT", raising=False)
    module = load_tool()
    with pytest.raises(module.TransientError, match="Docker"):
        with module.private_services(ROOT, worktree):
            pytest.fail("An unavailable daemon cannot yield usable private services")
    assert not socket_path.exists()


def test_private_config_accepts_loopback_binding_and_owned_network() -> None:
    project = "curie-preflight-example"
    configuration = {
        "services": {
            "postgres": {"ports": [{"host_ip": "127.0.0.1", "target": 5432, "published": "35432"}]}
        },
        "networks": {"curie_runner": {"name": f"{project}_runner"}},
    }
    assert load_tool().validate_private_config(configuration, project, {"postgres": [5432]}) is None


@pytest.mark.parametrize("host_ip", ["0.0.0.0", "::", None])
def test_private_config_shared_binding_is_gate_failure(host_ip: str | None) -> None:
    project = "curie-preflight-example"
    port = {"target": 5432, "published": "35432"}
    if host_ip is not None:
        port["host_ip"] = host_ip
    configuration = {
        "services": {"postgres": {"ports": [port]}},
        "networks": {"curie_runner": {"name": f"{project}_runner"}},
    }
    module = load_tool()
    with pytest.raises(module.GateFailure, match="shared binding"):
        module.validate_private_config(configuration, project, {"postgres": [5432]})


def test_private_config_shared_network_is_gate_failure() -> None:
    configuration = {
        "services": {"postgres": {"ports": [{"host_ip": "127.0.0.1", "target": 5432}]}},
        "networks": {"curie_runner": {"name": "shared_runner"}},
    }
    module = load_tool()
    with pytest.raises(module.GateFailure, match="incorrect owner"):
        module.validate_private_config(
            configuration, "curie-preflight-example", {"postgres": [5432]}
        )

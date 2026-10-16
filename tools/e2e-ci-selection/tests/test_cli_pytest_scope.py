"""CLI pytest scope through tracked Git inputs and the real workflow Bash."""

from __future__ import annotations

import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tomllib
from pathlib import Path
from types import ModuleType

import pytest
import yaml


def _load_test_module(name: str) -> ModuleType:
    path = Path(__file__).with_name(f"{name}.py")
    spec = importlib.util.spec_from_file_location(f"cli_scope_{name}", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


SELECTION_TESTS = _load_test_module("test_e2e_ci_selection")
PYTEST_TESTS = _load_test_module("test_python_pytest_selection")
REPO_ROOT = SELECTION_TESTS.REPO_ROOT
GENERATOR_PATH = "tools/e2e-ci-selection/cli_pytest_files.py"
LIST_PATH = "tools/e2e-ci-selection/cli_pytest_files.txt"
SELECTOR_PATH = "tools/e2e-ci-selection/select_tiers.py"
REGENERATE = "uv run python tools/e2e-ci-selection/cli_pytest_files.py --write"
CLI_SCOPE = {
    "prefixes": ["cli"],
    "excluded_prefixes": ["cli/scripts"],
    "python_prefixes": ["cli/tests"],
    "test_files": LIST_PATH,
}
CLI_ONLY_PATHS = (
    "cli/README.md",
    "cli/src/examples.rs",
    "cli/src/ops/capacity.rs",
    "cli/src/ops/mod.rs",
    "cli/src/ops/up.rs",
    "cli/tests/cluster_up_admission_fast_fail.rs",
    "cli/tests/cluster_up_capacity_preflight.rs",
    "cli/tests/cluster_up_gvisor_preflight.rs",
    "cli/tests/cluster_up_inference.rs",
    "cli/tests/cluster_up_priorityclass_preflight.rs",
    "cli/tests/cluster_up_retained_runner_values.rs",
    "cli/tests/data/converged-installation-read.sh",
    "cli/tests/data/convergence-driver.py",
    "cli/tests/installation_plan_parity.rs",
    "cli/tests/integration.rs",
)


def _git(root: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=root,
        env={**os.environ, "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1"},
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def _write(root: Path, path: str, text: str) -> None:
    destination = root / path
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(text, encoding="utf-8")


def _repository(
    tmp_path: Path, files: dict[str, str], *, testpaths: tuple[str, ...] = ("tests",)
) -> Path:
    root = tmp_path / "repository"
    root.mkdir()
    _git(root, "init", "--initial-branch=main")
    _git(root, "config", "user.email", "test@example.com")
    _git(root, "config", "user.name", "Test User")
    _write(
        root,
        "pyproject.toml",
        "[tool.pytest.ini_options]\n" + f"testpaths = {json.dumps(testpaths)}\n",
    )
    _write(root, ".github/workflows/ci.yaml", SELECTION_TESTS.WORKFLOW.read_text())
    for path, text in files.items():
        _write(root, path, text)
    _git(root, "add", ".")
    return root


def _generator_repository(
    tmp_path: Path, files: dict[str, str], *, testpaths: tuple[str, ...] = ("tests",)
) -> Path:
    root = _repository(tmp_path, files, testpaths=testpaths)
    destination = root / GENERATOR_PATH
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(REPO_ROOT / GENERATOR_PATH, destination)
    _git(root, "add", GENERATOR_PATH)
    return root


def _run_generator(root: Path, *args: str) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        [sys.executable, str(root / GENERATOR_PATH), *args],
        cwd=root,
        capture_output=True,
        check=False,
    )


def _generated(root: Path) -> bytes:
    result = _run_generator(root)
    assert result.returncode == 0, result.stdout + result.stderr
    return result.stdout


def _selection(tmp_path: Path, *paths: str, **options: object) -> dict[str, str]:
    result, output = SELECTION_TESTS._invoke_selector(tmp_path, *paths, **options)
    assert result.returncode == 0, result.stdout + result.stderr
    lines = output.splitlines()
    assert sum(line.startswith("pytest_scope=") for line in lines) == 1
    return dict(line.split("=", 1) for line in lines)


def test_cli_pytest_files_generator_is_deterministic() -> None:
    first = _generated(REPO_ROOT)
    assert _generated(REPO_ROOT) == first
    paths = first.decode().splitlines()
    assert paths
    assert paths == sorted(set(paths))
    assert first == ("\n".join(paths) + "\n").encode()
    tracked = set(_git(REPO_ROOT, "ls-files").splitlines())
    manifest = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())
    testpaths = manifest["tool"]["pytest"]["ini_options"]["testpaths"]
    for path in paths:
        assert path in tracked, path
        assert not path.startswith("cli/"), path
        assert any(path.startswith(prefix + "/") for prefix in testpaths), path
        assert Path(path).suffix == ".py", path
        assert Path(path).name.startswith("test_") or Path(path).name.endswith("_test.py"), path


def test_committed_cli_pytest_files_match_generator() -> None:
    assert (REPO_ROOT / LIST_PATH).read_bytes() == _generated(REPO_ROOT), REGENERATE
    result = _run_generator(REPO_ROOT, "--check")
    assert result.returncode == 0, result.stdout + result.stderr


def test_check_mode_fails_on_drift(tmp_path: Path) -> None:
    root = _generator_repository(
        tmp_path,
        {"tests/test_first.py": "# CURIE_BIN\n", "tests/test_second.py": "# cargo\n"},
    )
    expected = b"tests/test_first.py\ntests/test_second.py\n"
    assert _generated(root) == expected
    written = _run_generator(root, "--write")
    assert written.returncode == 0, written.stdout + written.stderr
    assert (root / LIST_PATH).read_bytes() == expected
    _git(root, "add", LIST_PATH)
    valid = _run_generator(root, "--check")
    assert valid.returncode == 0, valid.stdout + valid.stderr
    (root / LIST_PATH).write_bytes(b"tests/test_first.py\n")
    drift = _run_generator(root, "--check")
    assert drift.returncode == 1, drift.stdout + drift.stderr
    assert REGENERATE.encode() in drift.stdout + drift.stderr
    assert (root / LIST_PATH).read_bytes() == b"tests/test_first.py\n"
    assert _generated(root) == expected


@pytest.mark.parametrize(
    "contents",
    [
        pytest.param("# cli/target\n", id="cli-target"),
        pytest.param("# CURIE_BIN\n", id="binary-variable"),
        pytest.param("# target/debug/curie\n", id="debug-binary"),
        pytest.param("# target/release/curie\n", id="release-binary"),
        pytest.param("# cargo\n", id="cargo"),
        pytest.param("path = 'cli/src/main.rs'\n", id="single-quoted-cli-path"),
        pytest.param('path = "cli"\n', id="double-quoted-cli-directory"),
        pytest.param("# which('curie')\n", id="which"),
        pytest.param('# which(\n  "curie"\n)\n', id="multiline-which"),
        pytest.param("command = ['curie', '--help']\n", id="argv"),
        pytest.param('command = [\n  "curie", "--help"\n]\n', id="multiline-argv"),
    ],
)
def test_pattern_selects_test_file(tmp_path: Path, contents: str) -> None:
    root = _generator_repository(tmp_path, {"tests/test_consumer.py": contents})
    assert _generated(root) == b"tests/test_consumer.py\n"


@pytest.mark.parametrize(
    ("helper", "consumer", "contents", "extra"),
    [
        pytest.param(
            "tests/conftest.py", "tests/nested/test_consumer.py", "pass\n", {},
            id="ancestor-conftest",
        ),
        pytest.param(
            "tests/nested/conftest.py", "tests/nested/test_consumer.py", "pass\n", {},
            id="same-directory-conftest",
        ),
        pytest.param(
            "scripts/nested/check.sh", "tests/test_consumer.py",
            "# scripts/nested/check.sh\n", {"unscanned/check.sh": ""},
            id="repository-relative-path",
        ),
        pytest.param(
            "scripts/nested/check.sh", "tests/test_consumer.py",
            "# nested/check.sh\n", {"unscanned/check.sh": ""},
            id="last-two-segments",
        ),
        pytest.param(
            "scripts/nested/unique_check.sh", "tests/test_consumer.py",
            "# unique_check.sh\n", {}, id="unique-basename",
        ),
        pytest.param(
            "scripts/nested/unique_driver.py", "tests/test_consumer.py",
            "import unique_driver\n", {}, id="module-import",
        ),
        pytest.param(
            "scripts/nested/unique_driver.py", "tests/test_consumer.py",
            "from unique_driver import run\n", {}, id="module-from-import",
        ),
        pytest.param(
            "scripts/nested/unique_driver.py", "tests/test_consumer.py",
            "from package.unique_driver import run\n", {}, id="qualified-from-import",
        ),
        pytest.param(
            "scripts/nested/unique_driver.py", "tests/test_consumer.py",
            "from ...unique_driver import run\n", {}, id="relative-from-import",
        ),
        pytest.param(
            "packages/test-support/src/acme_helpers/__init__.py", "tests/test_consumer.py",
            "import acme_helpers\n", {"unscanned/__init__.py": ""}, id="package-import",
        ),
        pytest.param(
            "packages/test-support/src/acme_helpers/__init__.py", "tests/test_consumer.py",
            "from package.acme_helpers import run\n", {"unscanned/__init__.py": ""},
            id="package-from-import",
        ),
        pytest.param(
            "tools/sample/check.py", "tools/sample/tests/test_consumer.py",
            "pass\n", {}, id="tools-directory-suite",
        ),
    ],
)
def test_helper_link_selects_test_file(
    tmp_path: Path, helper: str, consumer: str, contents: str, extra: dict[str, str]
) -> None:
    root = _generator_repository(
        tmp_path,
        {
            helper: "# CURIE_BIN\n",
            consumer: contents,
            "other_tests/test_unrelated.py": "pass\n",
            **extra,
        },
        testpaths=("tests", "other_tests", "tools/sample/tests"),
    )
    assert _generated(root) == f"{consumer}\n".encode()


@pytest.mark.parametrize(
    "helper",
    [
        "tools/sample/helper.py",
        "tools/sample/helper.sh",
        "scripts/helper.py",
        "scripts/helper.sh",
        "release/helper.py",
        "release/helper.sh",
        "charts/curie/ci/helper.py",
        "charts/curie/ci/helper.sh",
        "packages/test-support/src/helper.py",
        "packages/test-support/src/helper.sh",
        "get-curie.sh",
        "tests/helper.py",
        "outside/conftest.py",
    ],
)
def test_each_helper_location_is_eligible(tmp_path: Path, helper: str) -> None:
    root = _generator_repository(
        tmp_path, {helper: "# cargo\n", "tests/test_consumer.py": f"# {helper}\n"}
    )
    assert _generated(root) == b"tests/test_consumer.py\n"


@pytest.mark.parametrize(
    "helper",
    ["apps/api/src/helper.py", "helper.py", "tools/sample/helper.txt", "tests/helper.sh"],
)
def test_ineligible_helper_is_not_linked(tmp_path: Path, helper: str) -> None:
    root = _generator_repository(
        tmp_path, {helper: "# cargo\n", "tests/test_consumer.py": f"# {helper}\n"}
    )
    assert _generated(root) == b""


def test_helper_link_requires_matching_helper_contents(tmp_path: Path) -> None:
    root = _generator_repository(
        tmp_path,
        {
            "scripts/helper.py": "# ordinary helper\n",
            "tests/test_consumer.py": "import helper\n",
        },
    )
    assert _generated(root) == b""
    _write(root, "scripts/helper.py", "# CURIE_BIN\n")
    assert _generated(root) == b"tests/test_consumer.py\n"


@pytest.mark.parametrize(
    "contents",
    [
        "clipboard\n",
        '"client"\n',
        '"cli-other/src/main.rs"\n',
        "# CURIE_BINARY cargoing target/debug/curie_extra\n",
        "# which('curie-other')\n",
        "command = ['curie-other', '--help']\n",
    ],
)
def test_unrelated_text_is_not_selected(tmp_path: Path, contents: str) -> None:
    root = _generator_repository(tmp_path, {"tests/test_unrelated.py": contents})
    assert _generated(root) == b""


@pytest.mark.parametrize("duplicate", ["unscanned/check.py", "cli/src/check.py"])
@pytest.mark.parametrize("reference", ["# check.py\n", "import check\n", "from check import run\n"])
def test_ambiguous_basename_is_not_linked(
    tmp_path: Path, duplicate: str, reference: str
) -> None:
    root = _generator_repository(
        tmp_path,
        {
            "scripts/nested/check.py": "# CURIE_BIN\n",
            duplicate: "pass\n",
            "tests/test_unrelated.py": reference,
            "tests/test_neighbor.py": "# nested/check.py\n",
        },
    )
    assert _generated(root) == b"tests/test_neighbor.py\n"


def test_root_conftest_selects_all_descendant_tests(tmp_path: Path) -> None:
    root = _generator_repository(
        tmp_path,
        {
            "conftest.py": "# CURIE_BIN\n",
            "tests/test_a.py": "pass\n",
            "tests/deep/test_b.py": "pass\n",
        },
    )
    assert _generated(root) == b"tests/deep/test_b.py\ntests/test_a.py\n"


def test_conftest_does_not_select_sibling_directories(tmp_path: Path) -> None:
    root = _generator_repository(
        tmp_path,
        {
            "tests/selected/conftest.py": "# cargo\n",
            "tests/selected/test_child.py": "pass\n",
            "tests/selected/deep/test_descendant.py": "pass\n",
            "tests/selected-other/test_sibling.py": "pass\n",
            "tests/test_parent.py": "pass\n",
        },
    )
    assert _generated(root) == (
        b"tests/selected/deep/test_descendant.py\ntests/selected/test_child.py\n"
    )


def test_helper_links_do_not_crawl_dependencies(tmp_path: Path) -> None:
    root = _generator_repository(
        tmp_path,
        {
            "scripts/cli_driver.py": "# CURIE_BIN\n",
            "scripts/wrapper.py": "import cli_driver\n",
            "tests/test_indirect.py": "import wrapper\n",
            "tests/test_direct.py": "import cli_driver\n",
        },
    )
    assert _generated(root) == b"tests/test_direct.py\n"


def test_generator_uses_only_tracked_tests_and_testpaths(tmp_path: Path) -> None:
    root = _generator_repository(
        tmp_path,
        {
            "tests/test_z.py": "# CURIE_BIN\n",
            "tests/nested/a_test.py": "# cargo\n",
            "tests/test_a.py": "# CURIE_BIN cargo\n",
            "tests/helper.py": "# CURIE_BIN\n",
            "tests/test_script.sh": "# cargo\n",
            "tests-other/test_outside.py": "# cargo\n",
            "uncollected/test_outside.py": "# cargo\n",
        },
        testpaths=("tests", "tests/nested"),
    )
    _write(root, "tests/test_untracked.py", "# CURIE_BIN\n")
    assert _generated(root) == b"tests/nested/a_test.py\ntests/test_a.py\ntests/test_z.py\n"


def test_untracked_helper_cannot_select_a_tracked_test(tmp_path: Path) -> None:
    root = _generator_repository(tmp_path, {"tests/test_consumer.py": "# untracked_helper.py\n"})
    _write(root, "scripts/untracked_helper.py", "# CURIE_BIN\n")
    assert _generated(root) == b""
    _git(root, "add", "scripts/untracked_helper.py")
    assert _generated(root) == b"tests/test_consumer.py\n"


def test_cli_file_content_cannot_change_generated_list(tmp_path: Path) -> None:
    root = _generator_repository(
        tmp_path,
        {
            "cli/tests/test_cli.py": "# cargo\n",
            "cli/conftest.py": "# CURIE_BIN\n",
            "cli/driver.py": "# CURIE_BIN\n",
            "tests/test_unrelated.py": "# cli/driver.py\n",
            "tests/test_control.py": "# CURIE_BIN\n",
        },
        testpaths=("tests", "cli/tests"),
    )
    assert _generated(root) == b"tests/test_control.py\n"
    for path in ("cli/tests/test_cli.py", "cli/conftest.py", "cli/driver.py"):
        _write(root, path, "pass\n")
    assert _generated(root) == b"tests/test_control.py\n"


@pytest.mark.parametrize(
    "failure",
    [
        "missing-manifest",
        "unreadable-manifest",
        "malformed-manifest",
        "missing-testpaths",
        "bad-testpaths",
        "bad-testpath-entry",
        "missing-tracked-file",
        "unreadable-tracked-file",
        "git-unavailable",
    ],
)
def test_generator_fails_visibly_on_required_input_errors(tmp_path: Path, failure: str) -> None:
    root = _generator_repository(tmp_path, {"tests/test_control.py": "# CURIE_BIN\n"})
    assert _generated(root) == b"tests/test_control.py\n"
    if failure in {"missing-manifest", "unreadable-manifest"}:
        (root / "pyproject.toml").unlink()
        if failure == "unreadable-manifest":
            (root / "pyproject.toml").mkdir()
    elif failure == "malformed-manifest":
        _write(root, "pyproject.toml", "[broken\n")
    elif failure == "missing-testpaths":
        _write(root, "pyproject.toml", "[tool.pytest.ini_options]\n")
    elif failure == "bad-testpaths":
        _write(root, "pyproject.toml", '[tool.pytest.ini_options]\ntestpaths = "tests"\n')
    elif failure == "bad-testpath-entry":
        _write(root, "pyproject.toml", '[tool.pytest.ini_options]\ntestpaths = [true]\n')
    elif failure in {"missing-tracked-file", "unreadable-tracked-file"}:
        (root / "tests/test_control.py").unlink()
        if failure == "unreadable-tracked-file":
            (root / "tests/test_control.py").mkdir()
    else:
        shutil.rmtree(root / ".git")
    result = _run_generator(root)
    assert result.returncode != 0, result.stdout + result.stderr
    assert result.stderr, result.stdout
    assert result.stdout == b""


def test_committed_list_includes_known_cli_tests() -> None:
    paths = set((REPO_ROOT / LIST_PATH).read_text().splitlines())
    assert {
        "tools/e2e-ci-selection/tests/test_e2e_ci_selection.py",
        "release/tests/test_sdk_approval_gate_workflow.py",
        "runner/tests/test_dockerfile_registry_layer.py",
        "apps/api/tests/test_gitflow_targets.py",
        "apps/worker/tests/test_hook_approval_proof_thread_key.py",
        "tools/doclint/tests/test_end_to_end.py",
    } <= paths
    assert "apps/api/tests/test_state.py" not in paths


@pytest.mark.parametrize(
    "paths",
    [
        CLI_ONLY_PATHS,
        ("cli/src/ops/up.rs",),
        ("cli/tests/data/convergence-driver.py",),
        ("cli/tests/local/test_release.py",),
        ("cli/scripts-other/tool.sh",),
    ],
)
@pytest.mark.parametrize("omit_kind", [False, True])
def test_cli_only_diff_selects_cli_scope(
    tmp_path: Path, paths: tuple[str, ...], omit_kind: bool
) -> None:
    output = _selection(tmp_path, *paths, omit_kind=omit_kind)
    assert output["pytest"] == "true"
    assert output["pytest_scope"] == "cli"
    assert output["pytest_deselect"] == SELECTION_TESTS._expected_conditional_deselect(*paths)
    assert output["skill"] == "true"
    assert output["local"] == "true"
    assert output["local_release"] == "true"
    assert output["cluster"] == ("false" if omit_kind else "true")


@pytest.mark.parametrize(
    ("paths", "push", "pytest_needed"),
    [
        (("cli/scripts/e2e.sh",), False, "true"),
        (("cli/scripts/configure-docker-hub-mirror.sh",), False, "true"),
        (("cli/src/ops/up.rs", "apps/api/src/curie_api/main.py"), False, "true"),
        (("cli/src/ops/up.rs", "docs/guides/getting-started.md"), False, "true"),
        (("cli/src/ops/up.rs", "unknown/file.txt"), False, "true"),
        (("cli/src/helper.py",), False, "true"),
        (("cli/tests-other/test_helper.py",), False, "true"),
        (("cli",), False, "true"),
        (("client/src/main.rs",), False, "true"),
        (("cli-other/src/main.rs",), False, "true"),
        (("docs/guides/getting-started.md",), False, "false"),
        ((), True, "true"),
    ],
)
@pytest.mark.parametrize("omit_kind", [False, True])
def test_other_diffs_select_full_scope(
    tmp_path: Path, paths: tuple[str, ...], push: bool, pytest_needed: str, omit_kind: bool
) -> None:
    output = _selection(tmp_path, *paths, push=push, omit_kind=omit_kind)
    assert output["pytest"] == pytest_needed
    assert output["pytest_scope"] == "full"


def test_version_only_diff_selects_full_scope(tmp_path: Path) -> None:
    version = SELECTION_TESTS._repo_version()
    output = _selection(tmp_path, *SELECTION_TESTS._version_only_paths(version))
    assert output["version_only"] == "true"
    assert output["pytest"] == "false"
    assert output["pytest_scope"] == "full"
    assert output["pytest_deselect"] == ""


@pytest.mark.parametrize("omit_kind", [False, True])
def test_revision_scope_uses_all_changed_paths_and_nonempty_diff(
    tmp_path: Path, omit_kind: bool
) -> None:
    root = _repository(tmp_path, {"cli/src/ops/up.rs": "fn main() {}\n"})
    _git(root, "commit", "-qm", "Create fixture")
    base = _git(root, "rev-parse", "HEAD")
    empty = _selection(tmp_path, base=base, head=base, cwd=root, omit_kind=omit_kind)
    assert empty["pytest"] == "true"
    assert empty["pytest_scope"] == "full"
    assert empty["pytest_deselect"] == ""
    _write(root, "cli/src/ops/up.rs", "fn main() { println!(\"changed\"); }\n")
    _git(root, "commit", "-qam", "Change CLI fixture")
    head = _git(root, "rev-parse", "HEAD")
    cli = _selection(tmp_path, base=base, head=head, cwd=root, omit_kind=omit_kind)
    assert cli["pytest"] == "true"
    assert cli["pytest_scope"] == "cli"
    assert cli["pytest_deselect"] == SELECTION_TESTS.SRE_RUNTIME
    _write(root, "apps/api/main.py", "pass\n")
    _git(root, "add", ".")
    _git(root, "commit", "-qm", "Change mixed fixture")
    head = _git(root, "rev-parse", "HEAD")
    mixed = _selection(tmp_path, base=base, head=head, cwd=root, omit_kind=omit_kind)
    assert mixed["pytest"] == "true"
    assert mixed["pytest_scope"] == "full"


def _selector_repository(tmp_path: Path) -> Path:
    root = _repository(
        tmp_path,
        {
            ".github/e2e-selection.yaml": SELECTION_TESTS.VALID_REGISTRY,
            SELECTOR_PATH: (REPO_ROOT / SELECTOR_PATH).read_text(),
            LIST_PATH: "tests/test_consumer.py\n",
            "tests/test_consumer.py": "pass\n",
            "release/atlas.py": (REPO_ROOT / "release/atlas.py").read_text(),
        },
    )
    return root


def _run_fixture_selector(
    root: Path, *, registry: Path | None = None, cwd: Path | None = None
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable, str(root / SELECTOR_PATH),
            "--registry", str(registry or root / ".github/e2e-selection.yaml"),
            "--path", "cli/src/main.rs",
        ],
        cwd=cwd or root,
        env={**os.environ, "GITHUB_OUTPUT": str(root / "github-output")},
        capture_output=True,
        text=True,
        check=False,
    )


@pytest.mark.parametrize(
    "scopes",
    [
        "missing",
        None,
        True,
        7,
        [],
        {},
        {"other": CLI_SCOPE},
        {"cli": CLI_SCOPE, "other": CLI_SCOPE},
        {"cli": None},
        {"cli": []},
        *[
            {"cli": {key: value for key, value in CLI_SCOPE.items() if key != omitted}}
            for omitted in CLI_SCOPE
        ],
        {"cli": {**CLI_SCOPE, "extra": []}},
    ],
)
def test_selector_rejects_invalid_scope_schema(tmp_path: Path, scopes: object) -> None:
    root = _selector_repository(tmp_path)
    registry = yaml.safe_load(SELECTION_TESTS.VALID_REGISTRY)
    if scopes == "missing":
        registry.pop("pytest_scopes")
    else:
        registry["pytest_scopes"] = scopes
    _write(root, ".github/e2e-selection.yaml", yaml.safe_dump(registry, sort_keys=False))
    result = _run_fixture_selector(root)
    assert result.returncode != 0, result.stdout + result.stderr
    assert "pytest_scopes" in result.stderr
    assert not (root / "github-output").exists()


@pytest.mark.parametrize("key", ["prefixes", "excluded_prefixes", "python_prefixes"])
@pytest.mark.parametrize(
    "value",
    [
        "cli", None, True, {}, [True], [7], [None], [""], ["/cli"], ["cli/"],
        ["../cli"], ["cli/../tests"],
    ],
)
def test_selector_rejects_invalid_scope_prefixes(tmp_path: Path, key: str, value: object) -> None:
    root = _selector_repository(tmp_path)
    registry = yaml.safe_load(SELECTION_TESTS.VALID_REGISTRY)
    registry["pytest_scopes"]["cli"][key] = value
    _write(root, ".github/e2e-selection.yaml", yaml.safe_dump(registry, sort_keys=False))
    result = _run_fixture_selector(root)
    assert result.returncode != 0, result.stdout + result.stderr
    assert "pytest_scopes" in result.stderr
    assert not (root / "github-output").exists()


@pytest.mark.parametrize(
    "value",
    [
        None, True, 7, [], {}, "", "/scope.txt", "scope.txt/", "../scope.txt",
        "tools/../scope.txt",
    ],
)
def test_selector_rejects_invalid_scope_list_path(tmp_path: Path, value: object) -> None:
    root = _selector_repository(tmp_path)
    registry = yaml.safe_load(SELECTION_TESTS.VALID_REGISTRY)
    registry["pytest_scopes"]["cli"]["test_files"] = value
    _write(root, ".github/e2e-selection.yaml", yaml.safe_dump(registry, sort_keys=False))
    result = _run_fixture_selector(root)
    assert result.returncode != 0, result.stdout + result.stderr
    assert "pytest_scopes" in result.stderr
    assert not (root / "github-output").exists()


def test_selector_accepts_valid_scope_schema(tmp_path: Path) -> None:
    root = _selector_repository(tmp_path)
    result = _run_fixture_selector(root)
    assert result.returncode == 0, result.stdout + result.stderr
    output = dict(line.split("=", 1) for line in (root / "github-output").read_text().splitlines())
    assert output["pytest"] == "true"
    assert output["pytest_scope"] == "cli"


@pytest.mark.parametrize("state", ["missing", "untracked", "tracked-missing"])
def test_selector_rejects_missing_or_untracked_scope_list(tmp_path: Path, state: str) -> None:
    root = _selector_repository(tmp_path)
    if state != "tracked-missing":
        _git(root, "rm", "--cached", LIST_PATH)
    if state != "untracked":
        (root / LIST_PATH).unlink()
    result = _run_fixture_selector(root)
    assert result.returncode != 0, result.stdout + result.stderr
    assert LIST_PATH in result.stderr
    assert not (root / "github-output").exists()


def test_scope_list_tracking_uses_selector_source_repository(tmp_path: Path) -> None:
    root = _selector_repository(tmp_path)
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    registry = elsewhere / "registry.yaml"
    registry.write_text(SELECTION_TESTS.VALID_REGISTRY)
    _write(elsewhere, ".github/workflows/ci.yaml", SELECTION_TESTS.WORKFLOW.read_text())
    _git(elsewhere, "init", "--initial-branch=main")
    result = _run_fixture_selector(root, registry=registry, cwd=elsewhere)
    assert result.returncode == 0, result.stdout + result.stderr
    output = dict(line.split("=", 1) for line in (root / "github-output").read_text().splitlines())
    assert output["pytest_scope"] == "cli"
    _git(root, "rm", "--cached", LIST_PATH)
    _write(elsewhere, LIST_PATH, "tests/test_consumer.py\n")
    _git(elsewhere, "add", ".")
    (root / "github-output").unlink()
    refused = _run_fixture_selector(root, registry=registry, cwd=elsewhere)
    assert refused.returncode != 0, refused.stdout + refused.stderr
    assert LIST_PATH in refused.stderr
    assert not (root / "github-output").exists()


@pytest.mark.parametrize("scope", ["cli", "full"])
@pytest.mark.parametrize("shard", [1, 2, 3])
@pytest.mark.parametrize(
    "deselect",
    [
        "",
        SELECTION_TESTS.SRE_RUNTIME,
        "examples/tests/test_additional file.py," + SELECTION_TESTS.SRE_RUNTIME,
    ],
)
def test_pytest_step_passes_cli_files_only_in_cli_scope(
    tmp_path: Path, scope: str, shard: int, deselect: str
) -> None:
    expected = [
        "run", "pytest", "-q", "-n", "4", "--dist", "loadgroup",
        "--ci-shard", f"{shard}/3", "--reruns", "1", "-rsR", "--durations=25",
        "--ignore=tools/factory-e2e/tests/test_c4_initial_binding.py",
    ]
    if deselect:
        for path in deselect.split(","):
            expected.extend(("--deselect", path))
    if scope == "cli":
        files = (REPO_ROOT / LIST_PATH).read_text().splitlines()
        assert files
        expected.extend(
            path
            for path in files
            if path != "tools/factory-e2e/tests/test_c4_initial_binding.py"
        )
    arguments = PYTEST_TESTS._run_pytest_step(
        tmp_path, shard=shard, deselect=deselect, scope=scope
    )
    assert PYTEST_TESTS._without_admitted_agent_exclusion(arguments) == expected


@pytest.mark.parametrize("scope", ["cli", "full"])
def test_pytest_step_keeps_scope_out_of_child_environment(tmp_path: Path, scope: str) -> None:
    step = PYTEST_TESTS._named_steps("python-pytest")["Pytest"]
    bindings = PYTEST_TESTS._bindings(step, PYTEST_TESTS.PYTEST_STEP_EXPRESSIONS)
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log = tmp_path / "uv-call.json"
    recorder = bin_dir / "uv"
    recorder.write_text(
        f"#!{sys.executable}\n"
        "import json, os, sys\n"
        "from pathlib import Path\n"
        "Path(os.environ['UV_CALL_LOG']).write_text(json.dumps({\n"
        "    'scope': os.environ.get('PYTEST_SCOPE'), 'argv': sys.argv[1:],\n"
        "}))\n"
    )
    recorder.chmod(0o755)
    _write(tmp_path, LIST_PATH, (REPO_ROOT / LIST_PATH).read_text())
    deselect = SELECTION_TESTS.SRE_RUNTIME
    state = {"shard": "2", "pytest_deselect": deselect, "pytest_scope": scope}
    result = PYTEST_TESTS._run_bash(
        PYTEST_TESTS._string(step, "run"),
        {
            "PATH": f"{bin_dir}:{os.environ['PATH']}",
            "UV_CALL_LOG": str(log),
            **{bindings[key]: value for key, value in state.items()},
        },
        tmp_path,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert log.exists(), "the workflow did not invoke uv"
    recorded = json.loads(log.read_text())
    assert recorded["scope"] is None, "PYTEST_SCOPE leaked into the pytest child environment"
    expected = [
        "run", "pytest", "-q", "-n", "4", "--dist", "loadgroup",
        "--ci-shard", "2/3", "--reruns", "1", "-rsR", "--durations=25",
        "--ignore=tools/factory-e2e/tests/test_c4_initial_binding.py",
        "--deselect", deselect,
    ]
    if scope == "cli":
        expected.extend(
            path
            for path in (tmp_path / LIST_PATH).read_text().splitlines()
            if path != "tools/factory-e2e/tests/test_c4_initial_binding.py"
        )
    assert PYTEST_TESTS._without_admitted_agent_exclusion(recorded["argv"]) == expected


@pytest.mark.parametrize("scope", ["cli", "full"])
def test_pytest_aggregator_accepts_matching_scope(tmp_path: Path, scope: str) -> None:
    deselect = SELECTION_TESTS.SRE_RUNTIME
    result = PYTEST_TESTS._run_pytest_aggregate(
        tmp_path,
        selected="true",
        scope=scope,
        changes_scope=scope,
        deselect=deselect,
        shards=PYTEST_TESTS._all_shards("true", "success", deselect, scope),
    )
    assert result.returncode == 0, result.stdout + result.stderr
    download = PYTEST_TESTS._named_steps()["Download shard results"]["with"]["path"]
    for shard in (1, 2, 3):
        assert (tmp_path / download / f"shard-{shard}.txt").read_text() == (
            f"shard={shard} selected=true pytest=success stack=success "
            f"deselect={deselect} scope={scope}\n"
        )


@pytest.mark.parametrize("scope", ["cli", "full"])
@pytest.mark.parametrize("selected", ["true", "false"])
@pytest.mark.parametrize("results_dir", [False, True])
def test_pytest_aggregator_rejects_changes_scope_disagreement(
    tmp_path: Path, scope: str, selected: str, results_dir: bool
) -> None:
    other = "full" if scope == "cli" else "cli"
    result = PYTEST_TESTS._run_pytest_aggregate(
        tmp_path,
        selected=selected,
        scope=scope,
        changes_scope=other,
        shards=(
            PYTEST_TESTS._all_shards("true", "success", scope=scope)
            if selected == "true" else None
        ),
        shards_result="success" if selected == "true" else "skipped",
        results_dir=results_dir,
    )
    assert result.returncode == 1, result.stdout + result.stderr
    assert "scope" in result.stderr


@pytest.mark.parametrize("scope", ["cli", "full"])
@pytest.mark.parametrize("shard", [1, 2, 3])
def test_pytest_aggregator_rejects_shard_scope_disagreement(
    tmp_path: Path, scope: str, shard: int
) -> None:
    shards = PYTEST_TESTS._all_shards("true", "success", scope=scope)
    shards[shard] = ("true", "success", "success", "", "full" if scope == "cli" else "cli")
    result = PYTEST_TESTS._run_pytest_aggregate(
        tmp_path, selected="true", scope=scope, changes_scope=scope, shards=shards
    )
    assert result.returncode == 1, result.stdout + result.stderr
    assert f"shard {shard}" in result.stderr


@pytest.mark.parametrize("results_dir", [False, True])
def test_pytest_aggregator_accepts_full_scope_skip(tmp_path: Path, results_dir: bool) -> None:
    result = PYTEST_TESTS._run_pytest_aggregate(
        tmp_path,
        selected="false",
        scope="full",
        changes_scope="full",
        shards_result="skipped",
        results_dir=results_dir,
    )
    assert result.returncode == 0, result.stdout + result.stderr

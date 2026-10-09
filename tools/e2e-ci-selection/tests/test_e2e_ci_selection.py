"""Executable contract for deterministic end to end CI selection."""

from __future__ import annotations

import ast
import functools
import importlib.util
import json
import os
import re
import subprocess
import sys
import tomllib
from collections.abc import Callable
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
# NOT `select.py`. Python puts a script's own directory first on sys.path, so a
# module named `select` shadows the stdlib one -- and this script imports
# subprocess, which imports selectors, which imports select. That made every
# test here fail on macOS while CI stayed green (#1878).
SELECTOR = REPO_ROOT / "tools" / "e2e-ci-selection" / "select_tiers.py"
REGISTRY = REPO_ROOT / ".github" / "e2e-selection.yaml"
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "ci.yaml"
UPGRADE_MUTANT_BUILDER = REPO_ROOT / "charts" / "curie" / "ci" / "make-upgrade-mutants.py"
UPGRADE_MATRIX = REPO_ROOT / "cli" / "scripts" / "cluster-upgrade-matrix.sh"
# Directories the upgrade matrix reads whole, each with the subdirectories it
# leaves out. The matrix runs `helm package` on charts/curie, and .helmignore
# drops charts/curie/ci from that package.
UPGRADE_MATRIX_DIRECTORY_INPUTS = {
    "charts/curie": ("charts/curie/ci",),
}

TIERS = ("skill", "local", "local-release", "cluster", "released-upgrade")
BASE_TIERS = TIERS[:-1]
OUTPUT_KEYS = {
    "skill": "skill",
    "local": "local",
    "local-release": "local_release",
    "cluster": "cluster",
    "released-upgrade": "released_upgrade",
}
APPROVED_ROOT_DOCS = (
    "AGENTS.md",
    "ARCHITECTURE.md",
    "CLAUDE.md",
    "CODE_OF_CONDUCT.md",
    "CONTRIBUTING.md",
    "LICENSE",
    "NOTICE",
    "QUICKSTART.md",
    "README.md",
    "SECURITY.md",
    "SUPPORT.md",
    "TRADEMARKS.md",
    "llms.txt",
)


def _invoke_selector(
    tmp_path: Path,
    *paths: str,
    registry: Path = REGISTRY,
    push: bool = False,
    omit_kind: bool = False,
    base: str | None = None,
    head: str | None = None,
    cwd: Path | None = None,
) -> tuple[subprocess.CompletedProcess[str], str]:
    output_path = tmp_path / f"github-output-{len(list(tmp_path.glob('github-output-*')))}"
    command = [sys.executable, str(SELECTOR), "--registry", str(registry)]
    if push:
        command.append("--push")
    if omit_kind:
        command.append("--omit-kind")
    if base is not None:
        command.extend(("--base", base))
    if head is not None:
        command.extend(("--head", head))
    for path in paths:
        command.extend(("--path", path))

    environment = os.environ.copy()
    environment["GITHUB_OUTPUT"] = str(output_path)
    completed = subprocess.run(
        command,
        cwd=cwd or tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    output = output_path.read_text() if output_path.exists() else ""
    return completed, output


def _load_selector_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("select_tiers", SELECTOR)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # Registered first: the module's dataclasses resolve their own module.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


# Checks that sweep many paths call the selector's own per-path function in
# process; one interpreter per path cost seconds per test. The CLI boundary
# (GITHUB_OUTPUT, --push, --omit-kind, revisions) keeps its subprocess tests
# through `_invoke_selector`.
_SELECTOR_MODULE = _load_selector_module()


@functools.cache
def _loaded_registry(registry: Path) -> Any:
    return _SELECTOR_MODULE._load_registry(registry)


def _selects_released_upgrade(path: str, registry: Path = REGISTRY) -> bool:
    selected = _SELECTOR_MODULE._select_path(_loaded_registry(registry), path)
    return "released-upgrade" in selected


def _path_needs_images(path: str) -> bool:
    name = path.rsplit("/", 1)[-1]
    if path in {"uv.lock", "pyproject.toml"} or name in {"uv.lock", "pyproject.toml"}:
        return True
    return "Dockerfile" in name or name.endswith(".dockerfile")


def _path_needs_cli_release(path: str) -> bool:
    return path == "cli" or path.startswith("cli/")


def _expected_output(
    *selected: str,
    pytest_needed: bool = True,
    images_needed: bool = False,
    cli_release_needed: bool = False,
    released_upgrade_full: bool = False,
    version_only: bool = False,
    factory: bool = False,
    approval_resume: bool = False,
) -> str:
    selected_tiers = set(selected)
    lines = [
        f"{OUTPUT_KEYS[tier]}={'true' if tier in selected_tiers else 'false'}" for tier in TIERS
    ]
    skill_local = ",".join(tier for tier in TIERS[:2] if tier in selected_tiers)
    lines.append(f"skill_local_tiers={skill_local}")
    lines.append(f"pytest={'true' if pytest_needed else 'false'}")
    lines.append(f"images={'true' if images_needed else 'false'}")
    lines.append(f"cli_release={'true' if cli_release_needed else 'false'}")
    lines.append(f"released_upgrade_full={'true' if released_upgrade_full else 'false'}")
    lines.append(f"version_only={'true' if version_only else 'false'}")
    lines.append(f"factory={'true' if factory else 'false'}")
    lines.append(f"approval_resume={'true' if approval_resume else 'false'}")
    return "\n".join(lines) + "\n"


def _assert_selection(
    tmp_path: Path,
    path: str,
    selected: tuple[str, ...],
    *,
    pytest_needed: bool = True,
    images_needed: bool | None = None,
    cli_release_needed: bool | None = None,
) -> None:
    completed, output = _invoke_selector(tmp_path, path)
    assert completed.returncode == 0, completed.stderr
    assert output == _expected_output(
        *selected,
        pytest_needed=pytest_needed,
        images_needed=_path_needs_images(path) if images_needed is None else images_needed,
        cli_release_needed=(
            _path_needs_cli_release(path) if cli_release_needed is None else cli_release_needed
        ),
    )


@pytest.mark.parametrize(
    ("path", "selected"),
    [
        ("runner/example.py", BASE_TIERS),
        ("compose.dev.yaml", ("local",)),
        ("compose/generated.py", ("local-release",)),
        ("charts/curie/values.yaml", ("cluster", "released-upgrade")),
        ("apps/api/example.py", ("local", "local-release", "cluster")),
        ("apps/worker/example.py", ("local", "local-release", "cluster")),
        ("otel/collector.yaml", ("local", "local-release")),
        ("cli/example.rs", BASE_TIERS),
        ("cli/src/main.rs", BASE_TIERS),
        ("cli/src/ops/upgrade.rs", TIERS),
        ("cli/scripts/cluster-upgrade-matrix.sh", TIERS),
        ("cli/scripts/gnu-process.py", TIERS),
        ("cli/src/application_schema_windows.json", TIERS),
        (
            "apps/api/src/curie_api/schema_compat.json",
            ("local", "local-release", "cluster", "released-upgrade"),
        ),
        ("packages/example.py", BASE_TIERS),
        ("packages/aci-protocol/src/aci_protocol/wire.py", BASE_TIERS),
        ("packages/plugin-format/src/plugin_format/manifest.py", BASE_TIERS),
        ("pyproject.toml", BASE_TIERS),
        ("uv.lock", BASE_TIERS),
    ],
)
def test_registry_maps_each_known_surface(
    tmp_path: Path,
    path: str,
    selected: tuple[str, ...],
) -> None:
    _assert_selection(tmp_path, path, selected)


@pytest.mark.parametrize(
    "path",
    [
        ".github/e2e-selection.yaml",
        ".github/workflows/ci.yaml",
        "tools/e2e-ci-selection/select_tiers.py",
    ],
)
def test_selector_and_workflow_paths_do_not_boot_kind(tmp_path: Path, path: str) -> None:
    _assert_selection(tmp_path, path, (), pytest_needed=True)


def test_ui_dockerfile_selects_images_without_e2e(tmp_path: Path) -> None:
    _assert_selection(
        tmp_path,
        "apps/ui/Dockerfile",
        (),
        pytest_needed=True,
        images_needed=True,
        cli_release_needed=False,
    )


def test_weather_fixture_does_not_select_released_upgrade(tmp_path: Path) -> None:
    _assert_selection(tmp_path, "examples/weather/evals/cases.json", BASE_TIERS)


@pytest.mark.parametrize(
    "path",
    [
        "charts/curie/values.yaml",
        "charts/curie/templates/secrets.yaml",
        "apps/api/src/curie_api/migrations/versions/example.py",
        "apps/worker/src/curie_worker/config.py",
        "apps/worker/src/curie_worker/approval_cards.py",
        "apps/worker/src/curie_worker/consumer_liveness.py",
        "apps/worker/src/curie_worker/workspace.py",
        "cli/src/ops/upgrade.rs",
        "cli/src/ops/convergence.rs",
        "cli/scripts/cluster-upgrade-matrix.sh",
        "cli/tests/data/upgrade-driver.py",
    ],
)
def test_released_upgrade_selects_upgrade_state_owners(
    tmp_path: Path,
    path: str,
) -> None:
    completed, output = _invoke_selector(tmp_path, path)
    assert completed.returncode == 0, completed.stderr
    outputs = dict(line.split("=", maxsplit=1) for line in output.splitlines())
    assert outputs["released_upgrade"] == "true"


@pytest.mark.parametrize(
    "path",
    [
        "apps/api/src/curie_api/routers/agents.py",
        "apps/worker/src/curie_worker/binding.py",
        "apps/worker/src/curie_worker/state/config.py",
        "apps/ui/src/main.tsx",
        "cli/src/main.rs",
        "docs/guides/getting-started.md",
    ],
)
def test_released_upgrade_does_not_select_unrelated_paths(
    tmp_path: Path,
    path: str,
) -> None:
    completed, output = _invoke_selector(tmp_path, path)
    assert completed.returncode == 0, completed.stderr
    outputs = dict(line.split("=", maxsplit=1) for line in output.splitlines())
    assert outputs["released_upgrade"] == "false"


def _repo_root_references(script: str) -> list[str]:
    references = re.findall(r"\$\{?REPO_ROOT\}?\"?/([\w.][\w./-]*)", script)
    for match in re.finditer(r"\b(?:bash|sh|python3)[ \t]+(['\"]?)([\w./-]+)\1", script):
        path = match.group(2)
        if "/" in path and not path.startswith("/"):
            references.append(path)
    # Workflow steps run from the checkout root. Match tracked path tokens
    # independently of the command that reads them, including root files and
    # newly added top-level directories. Redirection targets are outputs.
    code_lines = "\n".join(
        line for line in script.splitlines() if not line.lstrip().startswith("#")
    )
    code_lines = re.sub(r"(?<![<>])>{1,2}[ \t]*['\"]?[\w./-]+['\"]?", "", code_lines)
    tracked = _tracked_repo_paths(REPO_ROOT)
    references.extend(
        token
        for token in re.findall(
            r"(?<![\w./-])(?:[\w.-]+/)+[\w.-]+|(?<![\w./-])[\w.-]+\.[\w.-]+",
            code_lines,
        )
        if token in tracked
    )
    references.extend(
        re.findall(
            r"\b(?:open|Path)\([ \t]*['\"]([\w.-]+(?:/[\w.-]+)+)['\"]",
            script,
        )
    )
    return sorted({reference.rstrip("/") for reference in references})


@functools.cache
def _tracked_repo_paths(root: Path) -> frozenset[str]:
    completed = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=root,
        capture_output=True,
        text=True,
        check=True,
    )
    paths = set(completed.stdout.split("\0")) - {""}
    return frozenset(
        path
        for file in paths
        for path in (file, *(str(parent) for parent in Path(file).parents[:-1]))
    )


def _without_python_module_docstring(source: str) -> str:
    module = ast.parse(source)
    if not module.body or not isinstance(module.body[0], ast.Expr):
        return source
    docstring = module.body[0]
    if not isinstance(docstring.value, ast.Constant) or not isinstance(docstring.value.value, str):
        return source
    lines = source.splitlines(keepends=True)
    for index in range(docstring.lineno - 1, docstring.end_lineno):
        lines[index] = "\n"
    return "".join(lines)


def _unselected_helper_inputs(reference: str, seen: set[str]) -> list[str]:
    if reference in seen:
        return []
    seen.add(reference)
    helper = REPO_ROOT / reference
    if not helper.is_file() or helper.suffix not in {".py", ".sh"}:
        return []
    source = helper.read_text()
    if helper.suffix == ".py":
        source = _without_python_module_docstring(source)
    problems = _unselected_matrix_inputs(source)
    for child in _repo_root_references(source):
        problems.extend(_unselected_helper_inputs(child, seen))
    return problems


def _git_ignored(path: str) -> bool:
    completed = subprocess.run(
        ["git", "check-ignore", "--quiet", "--", path],
        cwd=REPO_ROOT,
        check=False,
    )
    assert completed.returncode in {0, 1}, f"git check-ignore failed for {path}"
    return completed.returncode == 0


def _directory_files(directory: str, excluded: tuple[str, ...]) -> tuple[str, ...]:
    # Tracked files only: an untracked leftover such as a mergetool .orig is
    # not in any diff, and .helmignore keeps it out of the package.
    completed = subprocess.run(
        ["git", "ls-files", "-z", "--", directory],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return tuple(
        path
        for path in sorted(set(completed.stdout.split("\0")))
        if path and not any(_matches(path, prefix) for prefix in excluded)
    )


def _matches(path: str, prefix: str) -> bool:
    return path == prefix or path.startswith(f"{prefix}/")


def _unselected_matrix_inputs(
    script: str,
    registry: Path = REGISTRY,
) -> list[str]:
    """Name each repository input of the script that skips released-upgrade."""
    problems: list[str] = []
    if re.search(r"\bcd[ \t]+['\"]?\$\{?REPO_ROOT\}?['\"]?(?=\s|$)", script):
        problems.append("cd to REPO_ROOT: relative reads cannot be classified")
    for match in re.finditer(
        r"\b(bash|sh|python3)[ \t]+(?:-[A-Za-z]+[ \t]+)*['\"]?"
        r"(\$\{?[A-Za-z_][A-Za-z0-9_]*\}?)(?:['\"])?(?=\s|$)",
        script,
    ):
        problems.append(f"{match.group(1)} {match.group(2)}: helper path cannot be classified")
    for match in re.finditer(
        r"\b(bash|sh|python3)[ \t]+['\"]?([\w.-]+\.(?:py|sh))['\"]?(?=\s|$)",
        script,
    ):
        problems.append(f"{match.group(1)} {match.group(2)}: helper path cannot be classified")
    for reference in _repo_root_references(script):
        if _git_ignored(reference):
            # Build output or local scratch: never part of a diff.
            continue
        target = REPO_ROOT / reference
        paths: tuple[str, ...]
        if target.is_dir():
            if reference not in UPGRADE_MATRIX_DIRECTORY_INPUTS:
                problems.append(f"{reference}: unlisted directory")
                continue
            paths = _directory_files(reference, UPGRADE_MATRIX_DIRECTORY_INPUTS[reference])
            if not paths:
                problems.append(f"{reference}: directory with no files to check")
                continue
        elif target.is_file():
            paths = (reference,)
        else:
            problems.append(f"{reference}: neither in the checkout nor ignored")
            continue
        for path in paths:
            if not _selects_released_upgrade(path, registry):
                problems.append(f"{path}: does not select released-upgrade")
    return problems


def test_released_upgrade_selects_every_repo_file_the_upgrade_matrix_reads(
    tmp_path: Path,
) -> None:
    script = UPGRADE_MATRIX.read_text()
    # The scan still sees the inputs these rules were written for.
    assert {
        "apps/api/src/curie_api/schema_compat.json",
        "charts/curie",
        "cli/scripts/gnu-process.py",
        "cli/src/application_schema_windows.json",
    } <= set(_repo_root_references(script))
    # The chart's left-out path holds only while helm still drops it.
    helmignore = (REPO_ROOT / "charts" / "curie" / ".helmignore").read_text()
    assert UPGRADE_MATRIX_DIRECTORY_INPUTS["charts/curie"] == ("charts/curie/ci",)
    assert "ci/" in helmignore.splitlines()
    assert _unselected_matrix_inputs(script) == []
    # A helper invoked by the matrix can start reading another repository
    # file without changing the matrix script. Guard that second hop too.
    seen: set[str] = set()
    for reference in _repo_root_references(script):
        assert _unselected_helper_inputs(reference, seen) == [], reference


def test_matrix_input_guard_checks_every_packaged_chart_file(tmp_path: Path) -> None:
    # The selector allows an ignored child under a selected prefix, so a hole
    # can open inside charts/curie without touching the prefix itself.
    text = REGISTRY.read_text()
    anchor = "    docs: []\n"
    assert anchor in text
    registry = tmp_path / "registry.yaml"
    registry.write_text(text.replace(anchor, f"{anchor}    charts/curie/files/agent-sandbox: []\n"))
    assert _unselected_matrix_inputs('helm package "$REPO_ROOT/charts/curie"\n', registry) == [
        "charts/curie/files/agent-sandbox/controller.yaml: does not select released-upgrade",
    ]


def test_matrix_input_guard_names_each_read_the_registry_drops(tmp_path: Path) -> None:
    dropped = (
        "apps/api/src/curie_api/schema_compat.json",
        "cli/scripts/gnu-process.py",
        "cli/src/application_schema_windows.json",
    )
    text = REGISTRY.read_text()
    for path in dropped:
        rule = f"    {path}: [released-upgrade]\n"
        assert rule in text
        text = text.replace(rule, "")
    registry = tmp_path / "registry.yaml"
    registry.write_text(text)
    assert _unselected_matrix_inputs(UPGRADE_MATRIX.read_text(), registry) == [
        f"{path}: does not select released-upgrade" for path in dropped
    ]


@pytest.mark.parametrize(
    ("script", "problems"),
    [
        (
            'BIN="$REPO_ROOT/cli/src/main.rs"\n',
            ["cli/src/main.rs: does not select released-upgrade"],
        ),
        (
            'DIR="${REPO_ROOT}/cli/scripts"\n',
            ["cli/scripts: unlisted directory"],
        ),
        (
            'BIN="$REPO_ROOT"/cli/src/main.rs\n',
            ["cli/src/main.rs: does not select released-upgrade"],
        ),
        (
            'CHARTS="$REPO_ROOT/charts/curie/.."\n',
            ["charts/curie/..: unlisted directory"],
        ),
        (
            'HELPER="$REPO_ROOT/cli/scripts/no-such-helper.py"\n',
            ["cli/scripts/no-such-helper.py: neither in the checkout nor ignored"],
        ),
        ('KUBECONFIG_FILE="$REPO_ROOT/.projects/kubeconfig-example"\n', []),
        ('BIN="$REPO_ROOT/cli/target/release/curie"\n', []),
    ],
)
def test_matrix_input_guard_classifies_each_reference(
    tmp_path: Path,
    script: str,
    problems: list[str],
) -> None:
    assert _unselected_matrix_inputs(script) == problems


@pytest.mark.parametrize(
    ("script", "problems"),
    [
        (
            "python3 cli/src/main.rs\n",
            ["cli/src/main.rs: does not select released-upgrade"],
        ),
        (
            "bash cli/scripts/e2e-ladder.sh\n",
            ["cli/scripts/e2e-ladder.sh: does not select released-upgrade"],
        ),
        (
            'cd "$REPO_ROOT"\ncat cli/src/main.rs\n',
            [
                "cd to REPO_ROOT: relative reads cannot be classified",
                "cli/src/main.rs: does not select released-upgrade",
            ],
        ),
        (
            'bash "$UPGRADE_HELPER" --check\n',
            ["bash $UPGRADE_HELPER: helper path cannot be classified"],
        ),
        (
            'python3 "$UPGRADE_HELPER" --check\n',
            ["python3 $UPGRADE_HELPER: helper path cannot be classified"],
        ),
        (
            'from pathlib import Path\nPath("cli/src/main.rs").read_text()\n',
            ["cli/src/main.rs: does not select released-upgrade"],
        ),
        (
            'open("cli/src/main.rs").read()\n',
            ["cli/src/main.rs: does not select released-upgrade"],
        ),
        (
            "python3 upgrade-helper.py --check\n",
            ["python3 upgrade-helper.py: helper path cannot be classified"],
        ),
        (
            "cat cli/src/main.rs\n",
            ["cli/src/main.rs: does not select released-upgrade"],
        ),
        (
            "jq . cli/src/main.rs\n",
            ["cli/src/main.rs: does not select released-upgrade"],
        ),
        (
            "helm install acme charts/curie -f cli/src/main.rs\n",
            ["cli/src/main.rs: does not select released-upgrade"],
        ),
        (
            "cat .github/e2e-selection.yaml\n",
            [".github/e2e-selection.yaml: does not select released-upgrade"],
        ),
        (
            "cat release/preserve_next.py\n",
            ["release/preserve_next.py: does not select released-upgrade"],
        ),
        (
            "cat pyproject.toml\n",
            ["pyproject.toml: does not select released-upgrade"],
        ),
        (
            'python3 -u "$UPGRADE_HELPER"\n',
            ["python3 $UPGRADE_HELPER: helper path cannot be classified"],
        ),
        (
            'bash -e "$UPGRADE_HELPER"\n',
            ["bash $UPGRADE_HELPER: helper path cannot be classified"],
        ),
        (
            'sh "$UPGRADE_HELPER"\n',
            ["sh $UPGRADE_HELPER: helper path cannot be classified"],
        ),
    ],
)
def test_upgrade_input_guard_rejects_relative_and_unclassifiable_reads(
    script: str,
    problems: list[str],
) -> None:
    assert _unselected_matrix_inputs(script) == problems


def test_released_upgrade_jobs_select_their_inline_repo_inputs() -> None:
    jobs = yaml.safe_load(WORKFLOW.read_text())["jobs"]
    problems: dict[str, list[str]] = {}
    for job_name in (
        "e2e-released-upgrade",
        "e2e-released-upgrade-negative",
        "e2e-cluster-upgrade-matrix",
    ):
        problems[job_name] = []
        seen: set[str] = set()
        for step in jobs[job_name]["steps"]:
            run = step.get("run")
            if not isinstance(run, str):
                continue
            problems[job_name].extend(_unselected_matrix_inputs(run))
            for reference in _repo_root_references(run):
                if reference == "cli/scripts/e2e-ladder.sh":
                    continue
                problems[job_name].extend(_unselected_helper_inputs(reference, seen))
    # The upgraded-install smoke uses the same ladder invocation as the fresh
    # cluster rung. Selecting this one script also boots the negative control
    # and all matrix shards; test_e2e_ladder_script_stays_off_released_upgrade
    # pins the two equivalent invocations and the absence of ladder reads in
    # the other upgrade jobs.
    assert problems == {
        "e2e-released-upgrade": ["cli/scripts/e2e-ladder.sh: does not select released-upgrade"],
        "e2e-released-upgrade-negative": [],
        "e2e-cluster-upgrade-matrix": [],
    }


def test_inline_upgrade_guard_rejects_new_unselected_helper() -> None:
    assert _unselected_matrix_inputs("python3 cli/src/main.rs --check\n") == [
        "cli/src/main.rs: does not select released-upgrade"
    ]


def test_inline_upgrade_guard_does_not_treat_redirect_output_as_input() -> None:
    assert _unselected_matrix_inputs("cat > cli/src/main.rs <<EOF\nfixture\nEOF\n") == []


def test_inline_upgrade_guard_scans_a_called_helpers_own_reads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    subprocess.run(["git", "init", "--quiet", str(tmp_path)], check=True)
    helper = tmp_path / "charts/curie/ci/live_manifest_parity.py"
    helper.parent.mkdir(parents=True)
    helper.write_text('from pathlib import Path\nPath("cli/src/main.rs").read_text()\n')
    input_file = tmp_path / "cli/src/main.rs"
    input_file.parent.mkdir(parents=True)
    input_file.write_text("// fixture\n")
    monkeypatch.setattr(sys.modules[__name__], "REPO_ROOT", tmp_path)

    assert _unselected_helper_inputs("charts/curie/ci/live_manifest_parity.py", set()) == [
        "cli/src/main.rs: does not select released-upgrade"
    ]


def test_e2e_ladder_script_stays_off_released_upgrade(tmp_path: Path) -> None:
    """A recorded gap, not an oversight.

    e2e-released-upgrade runs the ladder's cluster rung against an upgraded
    install. e2e-ladder-cluster runs the same invocation against a fresh one,
    and the cli prefix selects it for a ladder edit wherever kind runs (next
    omits both). Selecting released-upgrade as well would boot the negative
    control and every matrix shard, none of which read the ladder. A ladder
    edit that breaks only on the upgraded install first fails on push to main
    or a dispatch, both of which select every tier.
    """
    jobs = yaml.safe_load(WORKFLOW.read_text())["jobs"]

    def ladder_tiers(job: str) -> list[str | None]:
        return [
            step.get("env", {}).get("CURIE_E2E_TIERS")
            for step in jobs[job]["steps"]
            if "cli/scripts/e2e-ladder.sh" in str(step.get("run", ""))
        ]

    assert ladder_tiers("e2e-released-upgrade") == ["cluster"]
    assert ladder_tiers("e2e-ladder-cluster") == ["cluster"]
    assert ladder_tiers("e2e-released-upgrade-negative") == []
    assert ladder_tiers("e2e-cluster-upgrade-matrix") == []
    assert "e2e-ladder" not in UPGRADE_MATRIX.read_text()
    _assert_selection(tmp_path, "cli/scripts/e2e-ladder.sh", BASE_TIERS)


@pytest.mark.parametrize(
    "path",
    [*APPROVED_ROOT_DOCS, "docs/example.md", "docs/guides/getting-started.md"],
)
def test_genuine_documentation_only_selects_no_runtime_e2e_tiers(
    tmp_path: Path,
    path: str,
) -> None:
    _assert_selection(tmp_path, path, (), pytest_needed=False)


@pytest.mark.parametrize(
    ("path", "pytest_needed"),
    [
        ("apps/ui/package.json", True),
        ("apps/ui/pnpm-lock.yaml", True),
        ("apps/dispatcher/src/curie_dispatcher/app.py", True),
        ("scripts/README.md", False),
        ("scripts/check-docs.sh", False),
        ("scripts/check-pr-body.sh", False),
        (".github/workflows/pr-body.yaml", True),
        ("release/authorize.py", True),
        ("runner/tests/test_repo_toolchain_proof_ci.py", True),
        ("packages/test-support/src/curie_test_support/valkey.py", True),
        ("examples/coder/evals/cases.json", False),
    ],
)
def test_known_non_runtime_paths_select_no_e2e_tiers(
    tmp_path: Path,
    path: str,
    pytest_needed: bool,
) -> None:
    _assert_selection(tmp_path, path, (), pytest_needed=pytest_needed)


@pytest.mark.parametrize("root_file", APPROVED_ROOT_DOCS)
def test_root_filename_directory_cannot_bypass_runtime_tiers(
    tmp_path: Path,
    root_file: str,
) -> None:
    # #1954: ignore the root file itself, never a directory with the same name.
    _assert_selection(tmp_path, f"{root_file}/runtime-check.sh", BASE_TIERS)


def test_unapproved_markdown_fallback_selects_all_base_tiers(tmp_path: Path) -> None:
    _assert_selection(tmp_path, "UNAPPROVED.md", BASE_TIERS)


def test_charts_curie_still_selects_cluster(tmp_path: Path) -> None:
    completed, output = _invoke_selector(tmp_path, "charts/curie/values.yaml")
    assert completed.returncode == 0, completed.stderr
    outputs = dict(line.split("=", maxsplit=1) for line in output.splitlines())
    assert outputs["cluster"] == "true"


# `ci/` is helmignored, so `helm package charts/curie` (the upgrade matrix) and
# `helm upgrade ... charts/curie` (the released upgrade) never load it. A change
# there cannot alter an upgrade and must not book fourteen kind shards.
RELEASED_UPGRADE_JOBS = (
    "e2e-released-upgrade",
    "e2e-released-upgrade-negative",
    "e2e-cluster-upgrade-matrix-shards",
    "e2e-cluster-upgrade-matrix",
)
# The scripts those jobs hand the chart to. A charts/curie/ci path either of
# them starts running must stay on released-upgrade too.
RELEASED_UPGRADE_SCRIPTS = (
    "cli/scripts/cluster-upgrade-matrix.sh",
    "cli/tests/data/upgrade-driver.py",
)
CHART_ROOT = "charts/curie"
CHART_CI = "charts/curie/ci"


def _selector_outputs(tmp_path: Path, path: str) -> dict[str, str]:
    completed, output = _invoke_selector(tmp_path, path)
    assert completed.returncode == 0, completed.stderr
    return dict(line.split("=", maxsplit=1) for line in output.splitlines())


def _strings(value: object) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        return [text for item in value.values() for text in _strings(item)]
    if isinstance(value, list):
        return [text for item in value for text in _strings(item)]
    return []


def _chart_ci_paths_run_by_released_upgrade_jobs() -> set[str]:
    jobs = yaml.safe_load(WORKFLOW.read_text())["jobs"]
    texts = [text for name in RELEASED_UPGRADE_JOBS for text in _strings(jobs[name])]
    texts.extend((REPO_ROOT / script).read_text() for script in RELEASED_UPGRADE_SCRIPTS)
    return {match for text in texts for match in re.findall(rf"{CHART_CI}/[A-Za-z0-9_./-]+", text)}


@pytest.mark.parametrize(
    "path",
    [
        "charts/curie/ci/runtime/metrics-alerts-runtime.sh",
        "charts/curie/ci/runtime/publication-job-assertions.sh",
        "charts/curie/ci/live-manifest-parity-assertions.sh",
    ],
)
def test_released_upgrade_skips_chart_ci_scripts_the_chart_never_ships(
    tmp_path: Path,
    path: str,
) -> None:
    outputs = _selector_outputs(tmp_path, path)
    assert outputs["released_upgrade"] == "false"
    assert outputs["cluster"] == "true"


def test_released_upgrade_selects_chart_ci_scripts_its_own_jobs_run(
    tmp_path: Path,
) -> None:
    run_by_jobs = _chart_ci_paths_run_by_released_upgrade_jobs()
    # Negative control: the scan must find the two scripts known to be run, or
    # an empty scan would pass this test without checking anything.
    assert {
        "charts/curie/ci/make-upgrade-mutants.py",
        "charts/curie/ci/live_manifest_parity.py",
    } <= run_by_jobs
    for path in sorted(run_by_jobs):
        assert (REPO_ROOT / path).is_file(), path
        assert _selects_released_upgrade(path), path


def test_every_shipped_chart_file_selects_released_upgrade(tmp_path: Path) -> None:
    helmignore = (REPO_ROOT / CHART_ROOT / ".helmignore").read_text().splitlines()
    assert "ci/" in helmignore, "the released-upgrade carve-out assumes ci/ is not shipped"
    tracked = subprocess.run(
        ["git", "ls-files", CHART_ROOT],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.splitlines()
    # Every tracked file, not one per entry: a new shipped file, a narrowed
    # rule, or an ignored hole inside templates/ fails here instead of
    # silently dropping out of the upgrade gate.
    shipped = [path for path in tracked if not path.startswith(f"{CHART_CI}/")]
    assert len(shipped) < len(tracked), "expected tracked charts/curie/ci files"
    entries = {path.removeprefix(f"{CHART_ROOT}/").split("/", 1)[0] for path in shipped}
    assert {"Chart.yaml", "templates", "values.yaml"} <= entries
    for path in shipped:
        assert _selects_released_upgrade(path), path


@pytest.mark.parametrize(
    ("path", "selected"),
    [
        (".github/action.yml", BASE_TIERS),
        ("apps/api/README.md", ("local", "local-release", "cluster")),
        ("apps/api/runtime-config.yaml", ("local", "local-release", "cluster")),
        ("packages/plugin-format/README.md", BASE_TIERS),
        ("packages/plugin-format/plugin.yaml", BASE_TIERS),
        ("examples/weather/README.md", BASE_TIERS),
        ("examples/weather/skill-config.yaml", BASE_TIERS),
        ("tests/README.md", BASE_TIERS),
        ("tests/selector-config.yaml", BASE_TIERS),
        ("UNAPPROVED.md", BASE_TIERS),
    ],
)
def test_non_allowlisted_paths_never_bypass_runtime_e2e_selection(
    tmp_path: Path,
    path: str,
    selected: tuple[str, ...],
) -> None:
    _assert_selection(tmp_path, path, selected)


def test_mixed_root_documentation_and_runtime_diff_selects_runtime_tiers(
    tmp_path: Path,
) -> None:
    completed, output = _invoke_selector(tmp_path, "ARCHITECTURE.md", "apps/api/main.py")
    assert completed.returncode == 0, completed.stderr
    assert output == _expected_output("local", "local-release", "cluster")


def test_unknown_and_union_selection_are_deterministic(tmp_path: Path) -> None:
    unknown, unknown_output = _invoke_selector(tmp_path, "new-surface/module.py")
    assert unknown.returncode == 0, unknown.stderr
    assert unknown_output == _expected_output(*BASE_TIERS)

    forward, forward_output = _invoke_selector(
        tmp_path,
        "charts/curie/values.yaml",
        "compose.dev.yaml",
        "otel/collector.yaml",
    )
    reverse, reverse_output = _invoke_selector(
        tmp_path,
        "otel/collector.yaml",
        "compose.dev.yaml",
        "charts/curie/values.yaml",
    )
    assert forward.returncode == 0, forward.stderr
    assert reverse.returncode == 0, reverse.stderr
    assert (
        forward_output
        == reverse_output
        == _expected_output(
            "local",
            "local-release",
            "cluster",
            "released-upgrade",
        )
    )


def test_push_selects_every_tier_without_a_repository(tmp_path: Path) -> None:
    completed, output = _invoke_selector(tmp_path, push=True)
    assert completed.returncode == 0, completed.stderr
    assert output == _expected_output(
        *TIERS,
        images_needed=True,
        cli_release_needed=True,
        released_upgrade_full=True,
        factory=True,
        approval_resume=True,
    )


def test_pull_request_released_upgrade_runs_the_smoke_shard_only(
    tmp_path: Path,
) -> None:
    # A pull request path selection never asks for the full upgrade matrix or
    # the released chart upgrade jobs. Push and dispatch (the nightly) do.
    outputs = _selector_outputs(tmp_path, "charts/curie/values.yaml")
    assert outputs["released_upgrade"] == "true"
    assert outputs["released_upgrade_full"] == "false"


def test_omit_kind_drops_cluster_tiers_and_keeps_the_rest(tmp_path: Path) -> None:
    kept = tuple(tier for tier in TIERS if tier not in {"cluster", "released-upgrade"})
    completed, output = _invoke_selector(tmp_path, push=True, omit_kind=True)
    assert completed.returncode == 0, completed.stderr
    # The next push omits kind, so it runs no upgrade job at all.
    assert output == _expected_output(*kept, images_needed=True, cli_release_needed=True)

    chart, chart_output = _invoke_selector(
        tmp_path,
        "charts/curie/values.yaml",
        omit_kind=True,
    )
    assert chart.returncode == 0, chart.stderr
    assert chart_output == _expected_output(pytest_needed=True)

    untouched, untouched_output = _invoke_selector(
        tmp_path,
        "charts/curie/values.yaml",
    )
    assert untouched.returncode == 0, untouched.stderr
    assert untouched_output == _expected_output(
        "cluster",
        "released-upgrade",
        pytest_needed=True,
    )


def test_revisions_select_changed_paths_and_unknown_fallback(tmp_path: Path) -> None:
    repository = tmp_path / "repository"
    repository.mkdir()
    subprocess.run(
        ["git", "init", "--initial-branch", "main"],
        cwd=repository,
        capture_output=True,
        text=True,
        check=True,
    )
    subprocess.run(
        ["git", "config", "user.email", "test@example.com"],
        cwd=repository,
        check=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "Test User"],
        cwd=repository,
        check=True,
    )

    known_path = repository / "compose.dev.yaml"
    known_path.write_text("version: one\n")
    subprocess.run(["git", "add", "."], cwd=repository, check=True)
    subprocess.run(["git", "commit", "-m", "base"], cwd=repository, check=True)
    base = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repository,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()

    known_path.write_text("version: two\n")
    subprocess.run(["git", "commit", "-am", "known"], cwd=repository, check=True)
    known_head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repository,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    known, known_output = _invoke_selector(
        tmp_path,
        base=base,
        head=known_head,
        cwd=repository,
    )
    assert known.returncode == 0, known.stderr
    assert known_output == _expected_output("local")

    (repository / "new-surface.txt").write_text("new\n")
    subprocess.run(["git", "add", "."], cwd=repository, check=True)
    subprocess.run(["git", "commit", "-m", "unknown"], cwd=repository, check=True)
    unknown_head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repository,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    unknown, unknown_output = _invoke_selector(
        tmp_path,
        base=known_head,
        head=unknown_head,
        cwd=repository,
    )
    assert unknown.returncode == 0, unknown.stderr
    assert unknown_output == _expected_output(*BASE_TIERS)


# The exact five files of the real v0.11.2 preparation PR #3845, with the
# snapshot renamed for whatever release this checkout is (#3858).
def _repo_version() -> str:
    cargo = tomllib.loads((REPO_ROOT / "cli" / "Cargo.toml").read_text())
    return f"v{cargo['package']['version']}"


def _version_only_paths(version: str) -> tuple[str, ...]:
    return (
        "charts/curie/Chart.yaml",
        "cli/Cargo.toml",
        "cli/Cargo.lock",
        "docs/architecture-atlas/versions.json",
        f"docs/architecture-atlas/snapshots/{version}.json",
    )


# What #3858 runs for a version-only diff: the local-release rung, which
# exercises version identity on the new commit, and nothing heavier.
VERSION_ONLY_SELECTION = _expected_output(
    "local-release",
    pytest_needed=False,
    cli_release_needed=True,
    version_only=True,
)
# What the same five paths selected before #3858, and still select whenever
# the delta is not version-only.
FULL_PREP_SELECTION = _expected_output(*TIERS, cli_release_needed=True)


@pytest.mark.parametrize("omit_kind", [False, True])
def test_version_only_release_prep_selects_only_the_local_release_rung(
    tmp_path: Path, omit_kind: bool
) -> None:
    """#3858: a release bump reuses the proof it already has."""
    completed, output = _invoke_selector(
        tmp_path, *_version_only_paths(_repo_version()), omit_kind=omit_kind
    )
    assert completed.returncode == 0, completed.stderr
    assert output == VERSION_ONLY_SELECTION


def test_another_releases_snapshot_is_not_version_only(tmp_path: Path) -> None:
    """#3858: only this checkout's own snapshot is version-only."""
    assert _repo_version() != "v9.9.9"
    completed, output = _invoke_selector(tmp_path, *_version_only_paths("v9.9.9"))
    assert completed.returncode == 0, completed.stderr
    assert output == FULL_PREP_SELECTION


@pytest.mark.parametrize("extra", ["cli/src/main.rs", "README.md"])
def test_any_other_path_makes_the_prep_diff_not_version_only(tmp_path: Path, extra: str) -> None:
    """#3858: one path outside the set restores today's selection."""
    completed, output = _invoke_selector(tmp_path, *_version_only_paths(_repo_version()), extra)
    assert completed.returncode == 0, completed.stderr
    assert output == FULL_PREP_SELECTION


def test_push_is_never_version_only(tmp_path: Path) -> None:
    """#3858: pushes and dispatches run everything."""
    completed, output = _invoke_selector(tmp_path, push=True)
    assert completed.returncode == 0, completed.stderr
    assert _outputs_of(output)["version_only"] == "false"


def _outputs_of(output: str) -> dict[str, str]:
    return dict(line.split("=", maxsplit=1) for line in output.splitlines())


def _git(repository: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args],
        cwd=repository,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


def _commit_files(repository: Path, files: dict[str, str], message: str) -> str:
    for path, content in files.items():
        target = repository / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content)
    _git(repository, "add", "--", *files)
    _git(repository, "commit", "-q", "-m", message)
    return _git(repository, "rev-parse", "HEAD")


def _new_repository(tmp_path: Path) -> Path:
    repository = tmp_path / "repository"
    repository.mkdir()
    _git(repository, "init", "-q", "--initial-branch", "main")
    _git(repository, "config", "user.email", "test@example.com")
    _git(repository, "config", "user.name", "Test User")
    return repository


def _cargo_toml(version: str) -> str:
    return f'[package]\nname = "curie"\nversion = "{version}"\n'


def test_revision_mode_reads_the_version_from_the_head_commit(tmp_path: Path) -> None:
    """#3858: revision mode names the snapshot from head's cli/Cargo.toml."""
    repository = _new_repository(tmp_path)
    base = _commit_files(
        repository,
        {
            "cli/Cargo.toml": _cargo_toml("1.2.2"),
            "cli/src/main.rs": "fn main() {}\n",
            "README.md": "readme\n",
        },
        "base",
    )
    version_files = {path: f"{path} bumped\n" for path in _version_only_paths("v1.2.3")}
    version_files["cli/Cargo.toml"] = _cargo_toml("1.2.3")
    head = _commit_files(repository, version_files, "prepare v1.2.3")

    completed, output = _invoke_selector(tmp_path, base=base, head=head, cwd=repository)
    assert completed.returncode == 0, completed.stderr
    assert output == VERSION_ONLY_SELECTION

    # Negative control: a snapshot named for a version head does not declare
    # is not version-only.
    wrong = {path: f"{path} again\n" for path in _version_only_paths("v1.2.4")}
    wrong["cli/Cargo.toml"] = _cargo_toml("1.2.3")
    wrong_head = _commit_files(repository, wrong, "snapshot for another version")
    completed, output = _invoke_selector(tmp_path, base=head, head=wrong_head, cwd=repository)
    assert completed.returncode == 0, completed.stderr
    assert _outputs_of(output)["version_only"] == "false"
    assert _outputs_of(output)["local"] == "true"


def test_revision_mode_without_a_cargo_manifest_is_not_version_only(
    tmp_path: Path,
) -> None:
    """#3858: an unreadable version fails closed to today's selection."""
    repository = _new_repository(tmp_path)
    base = _commit_files(repository, {"README.md": "readme\n"}, "base")
    paths = [path for path in _version_only_paths("v1.2.3") if not path.startswith("cli/")]
    head = _commit_files(repository, {path: f"{path} bumped\n" for path in paths}, "no manifest")

    completed, output = _invoke_selector(tmp_path, base=base, head=head, cwd=repository)
    assert completed.returncode == 0, completed.stderr
    path_mode, path_output = _invoke_selector(tmp_path, *paths)
    assert path_mode.returncode == 0, path_mode.stderr
    assert _outputs_of(output)["version_only"] == "false"
    assert output == path_output


VALID_REGISTRY = """
version: 1
fallback: [skill, local, local-release, cluster]
rules:
  exact:
    compose.dev.yaml: [local]
    apps/worker/src/curie_worker/config.py: [released-upgrade]
  prefixes:
    charts: [cluster]
    charts/curie: [released-upgrade]
  ignored_exact:
    README.md: []
  ignored_prefixes:
    docs: []
"""


@pytest.mark.parametrize(
    "registry_text",
    [
        VALID_REGISTRY.replace("charts: [cluster]", "charts: [unknown]"),
        VALID_REGISTRY.replace("charts: [cluster]", "charts: [cluster, cluster]"),
        VALID_REGISTRY.replace("compose.dev.yaml: [local]", "compose.dev.yaml: []"),
        VALID_REGISTRY.replace("charts: [cluster]", "charts: []"),
        VALID_REGISTRY.replace("version: 1", "version: true"),
        VALID_REGISTRY.replace(
            "    charts: [cluster]",
            "    charts: [cluster]\n    charts: [local]",
        ),
        VALID_REGISTRY.replace("  exact:\n    compose.dev.yaml: [local]", "  exact: []"),
        VALID_REGISTRY.replace(
            "fallback: [skill, local, local-release, cluster]",
            "fallback: [skill, local]",
        ),
        VALID_REGISTRY.replace("    docs: []", "    charts: []"),
    ],
    ids=(
        "unknown_tier",
        "duplicate_tier",
        "empty_exact_tiers",
        "empty_prefix_tiers",
        "boolean_version",
        "duplicate_rule",
        "malformed_entry",
        "invalid_fallback",
        "ignored_overlap",
    ),
)
def test_selector_rejects_invalid_registries(
    tmp_path: Path,
    registry_text: str,
) -> None:
    registry = tmp_path / "registry.yaml"
    registry.write_text(registry_text)
    completed, _output = _invoke_selector(tmp_path, "charts/example.yaml", registry=registry)
    assert completed.returncode != 0


@pytest.mark.parametrize(
    "entry",
    [
        "README.md: [skill]",
        "README.md/: []",
        "/README.md: []",
        "compose.dev.yaml: []",
        "charts: []",
    ],
)
def test_selector_rejects_invalid_exact_ignores(tmp_path: Path, entry: str) -> None:
    registry = tmp_path / "registry.yaml"
    registry.write_text(VALID_REGISTRY.replace("README.md: []", entry))
    completed, output = _invoke_selector(tmp_path, "README.md", registry=registry)
    assert completed.returncode != 0
    assert not output


def test_exact_ignore_under_selected_prefix_does_not_hide_siblings(tmp_path: Path) -> None:
    registry = tmp_path / "registry.yaml"
    registry.write_text(VALID_REGISTRY.replace("README.md: []", "charts/curie/README.md: []"))
    ignored, ignored_output = _invoke_selector(
        tmp_path, "charts/curie/README.md", registry=registry
    )
    sibling, sibling_output = _invoke_selector(
        tmp_path, "charts/curie/README.md/runtime-check.sh", registry=registry
    )
    assert ignored.returncode == sibling.returncode == 0
    assert ignored_output == _expected_output(pytest_needed=False)
    assert sibling_output == _expected_output("cluster", "released-upgrade")


def test_more_specific_ignored_child_of_selected_prefix_is_allowed(tmp_path: Path) -> None:
    registry = tmp_path / "registry.yaml"
    registry.write_text(VALID_REGISTRY.replace("    docs: []", "    docs: []\n    charts/ci: []"))
    ignored, ignored_output = _invoke_selector(tmp_path, "charts/ci/probe.sh", registry=registry)
    assert ignored.returncode == 0, ignored.stderr
    assert ignored_output == _expected_output(pytest_needed=False)

    selected, selected_output = _invoke_selector(
        tmp_path, "charts/curie/values.yaml", registry=registry
    )
    assert selected.returncode == 0, selected.stderr
    assert selected_output == _expected_output("cluster", "released-upgrade")


AGGREGATE_EXPRESSIONS = {
    "changes_result": "${{ needs.changes.result }}",
    "event_name": "${{ github.event_name }}",
    "skill_selected": "${{ needs.changes.outputs.skill }}",
    "local_selected": "${{ needs.changes.outputs.local }}",
    "local_release_selected": "${{ needs.changes.outputs.local_release }}",
    "cluster_selected": "${{ needs.changes.outputs.cluster }}",
    "released_upgrade_selected": "${{ needs.changes.outputs.released_upgrade }}",
    "released_upgrade_full": "${{ needs.changes.outputs.released_upgrade_full }}",
    "approval_resume_selected": "${{ needs.changes.outputs.approval_resume }}",
    "skill_local_result": "${{ needs.e2e-ladder.result }}",
    "local_release_result": "${{ needs.e2e-ladder-release.result }}",
    "cluster_result": "${{ needs.e2e-ladder-cluster.result }}",
    "cluster_chart_result": "${{ needs.e2e-cluster-chart-regressions.result }}",
    "rollout_recovery_result": "${{ needs.e2e-cluster-rollout-recovery.result }}",
    "approval_resume_result": ("${{ needs.e2e-cluster-approval-resume-restarts.result }}"),
    "released_upgrade_result": "${{ needs.e2e-released-upgrade.result }}",
    "released_upgrade_negative_result": ("${{ needs.e2e-released-upgrade-negative.result }}"),
    "upgrade_matrix_shards_result": ("${{ needs.e2e-cluster-upgrade-matrix-shards.result }}"),
    "upgrade_matrix_result": "${{ needs.e2e-cluster-upgrade-matrix.result }}",
}


def test_next_omits_kind_and_dispatch_keeps_it() -> None:
    workflow = yaml.safe_load(WORKFLOW.read_text())
    trigger = workflow[True]
    assert trigger["workflow_dispatch"] is None
    group = workflow["concurrency"]["group"]
    assert group == ("${{ github.workflow }}-${{ github.ref }}-${{ github.event_name }}")
    run = next(
        step["run"] for step in workflow["jobs"]["changes"]["steps"] if step.get("id") == "filter"
    )
    assert "github.event_name" in run
    assert "refs/heads/next" in run
    assert '"$base_ref" = "next"' in run
    assert run.count("select_tiers.py") == 3
    pull_request, rest = run.split("elif", 1)
    assert "git fetch" in pull_request
    assert "--omit-kind" in pull_request
    next_push, main_or_dispatch = rest.split("else", 1)
    assert "--omit-kind" in next_push
    assert "--push" in next_push
    assert "workflow_dispatch" in main_or_dispatch
    assert "--push" in main_or_dispatch
    assert "--omit-kind" not in main_or_dispatch


def test_workflow_consumes_each_selection_output_exactly() -> None:
    workflow = yaml.safe_load(WORKFLOW.read_text())
    jobs = workflow["jobs"]
    assert jobs["changes"]["outputs"] == {
        "skill": "${{ steps.filter.outputs.skill }}",
        "local": "${{ steps.filter.outputs.local }}",
        "local_release": "${{ steps.filter.outputs.local_release }}",
        "cluster": "${{ steps.filter.outputs.cluster }}",
        "released_upgrade": "${{ steps.filter.outputs.released_upgrade }}",
        "released_upgrade_full": ("${{ steps.filter.outputs.released_upgrade_full }}"),
        "skill_local_tiers": "${{ steps.filter.outputs.skill_local_tiers }}",
        "images": "${{ steps.filter.outputs.images }}",
        "cli_release": "${{ steps.filter.outputs.cli_release }}",
        # #3858: python-pytest and rust-select read these two.
        "pytest": "${{ steps.filter.outputs.pytest }}",
        "version_only": "${{ steps.filter.outputs.version_only }}",
        "factory": "${{ steps.filter.outputs.factory }}",
        "approval_resume": "${{ steps.filter.outputs.approval_resume }}",
        "runtime_assertions": "${{ steps.runtime.outputs.runtime_assertions }}",
    }

    skill_local = jobs["e2e-ladder"]
    assert skill_local["if"] == (
        "${{ needs.changes.outputs.skill == 'true' || needs.changes.outputs.local == 'true' }}"
    )
    ladder_steps = [
        step for step in skill_local["steps"] if step.get("run") == "bash cli/scripts/e2e-ladder.sh"
    ]
    assert len(ladder_steps) == 1
    assert ladder_steps[0]["env"]["CURIE_E2E_TIERS"] == (
        "${{ needs.changes.outputs.skill_local_tiers }}"
    )

    assert jobs["e2e-ladder-release"]["if"] == (
        "${{ needs.changes.outputs.local_release == 'true' }}"
    )
    assert jobs["e2e-ladder-cluster"]["if"] == ("${{ needs.changes.outputs.cluster == 'true' }}")
    assert jobs["e2e-released-upgrade"]["if"] == (
        "${{ needs.changes.outputs.released_upgrade_full == 'true' }}"
    )
    assert jobs["e2e-released-upgrade-negative"]["if"] == (jobs["e2e-released-upgrade"]["if"])
    assert set(jobs["e2e-released-upgrade-negative"]["needs"]) == set(
        jobs["e2e-released-upgrade"]["needs"]
    )
    assert jobs["e2e-cluster-upgrade-matrix"]["if"] == (
        "${{ needs.changes.outputs.released_upgrade == 'true' }}"
    )
    assert "if" not in jobs["e2e-cluster-upgrade-matrix-shards"]
    assert jobs["images"]["if"] == "${{ needs.changes.outputs.images == 'true' }}"
    assert "worker-local-image" not in jobs
    assert jobs["dispatcher-image-smoke"]["if"] == jobs["images"]["if"]
    assert jobs["mail-adapter-image-smoke"]["if"] == jobs["images"]["if"]
    assert jobs["ui-image-smoke"]["if"] == jobs["images"]["if"]
    assert jobs["repo-toolchain-proof"]["if"] == jobs["images"]["if"]
    assert jobs["cli-portability"]["if"] == (
        "${{ github.event_name != 'pull_request' && needs.changes.outputs.cli_release == 'true' }}"
    )
    assert jobs["cli-darwin"]["if"] == jobs["cli-portability"]["if"]
    assert "changes" in jobs["rust-build"]["needs"]
    assert jobs["eval-falsifiability"]["if"] == ("${{ needs.changes.outputs.skill == 'true' }}")


def test_upgrade_matrix_shards_job_gates_coverage_and_lists_shards() -> None:
    workflow = yaml.safe_load(WORKFLOW.read_text())
    job = workflow["jobs"]["e2e-cluster-upgrade-matrix-shards"]
    assert job["outputs"]["shards"] == "${{ steps.list.outputs.shards }}"
    runs = [step["run"] for step in job["steps"] if isinstance(step.get("run"), str)]
    assert any("cli/scripts/cluster-upgrade-matrix.sh --self-test" in run for run in runs)
    list_steps = [step for step in job["steps"] if step.get("id") == "list"]
    assert len(list_steps) == 1
    assert "--list-shards --json" in list_steps[0]["run"]
    assert "$GITHUB_OUTPUT" in list_steps[0]["run"]
    assert job["needs"] == ["changes"]
    assert list_steps[0]["env"]["FULL"] == ("${{ needs.changes.outputs.released_upgrade_full }}")


def _list_shards(tmp_path: Path, full: str, smoke: str | None = None) -> str:
    workflow = yaml.safe_load(WORKFLOW.read_text())
    job = workflow["jobs"]["e2e-cluster-upgrade-matrix-shards"]
    step = next(step for step in job["steps"] if step.get("id") == "list")
    output = tmp_path / f"list-{full}-{smoke}"
    environment = os.environ.copy()
    environment.update(step["env"])
    environment.update({"FULL": full, "GITHUB_OUTPUT": str(output)})
    if smoke is not None:
        environment["SMOKE_SHARD"] = smoke
    completed = subprocess.run(
        ["bash", "--noprofile", "--norc", "-c", step["run"]],
        cwd=REPO_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    return output.read_text()


def test_upgrade_matrix_lists_every_shard_on_full_runs_and_one_on_prs(
    tmp_path: Path,
) -> None:
    full = _list_shards(tmp_path, "true")
    shards = json.loads(full.removeprefix("shards="))
    assert shards == [
        "s01",
        "s02",
        "s03",
        "s04",
        "s05",
        "s06",
        "s07",
        "s08",
        "s09",
        "s11",
        "s13",
        "s14",
        "s17",
    ]
    assert _list_shards(tmp_path, "false") == 'shards=["s01"]\n'


def test_upgrade_matrix_smoke_shard_must_be_listed(tmp_path: Path) -> None:
    with pytest.raises(AssertionError):
        _list_shards(tmp_path, "false", smoke="s99")


def test_upgrade_matrix_workflow_runs_one_job_per_shard() -> None:
    workflow = yaml.safe_load(WORKFLOW.read_text())
    job = workflow["jobs"]["e2e-cluster-upgrade-matrix"]
    needs = job["needs"]
    if isinstance(needs, str):
        needs = [needs]
    assert set(needs) == {
        "rust-build",
        "changes",
        "e2e-cluster-upgrade-matrix-shards",
        "ci-images",
    }
    assert job["if"] == "${{ needs.changes.outputs.released_upgrade == 'true' }}"
    assert job["timeout-minutes"] == 45
    assert job["strategy"]["fail-fast"] is False
    # 14 shards run in one wave on push and dispatch; 7 took two waves.
    assert job["strategy"]["max-parallel"] == 14
    assert job["strategy"]["matrix"] == {
        "shard": ("${{ fromJSON(needs.e2e-cluster-upgrade-matrix-shards.outputs.shards) }}")
    }
    named_steps = {step["name"]: step for step in job["steps"] if isinstance(step.get("name"), str)}
    run_step = named_steps["Run the cluster upgrade matrix"]
    assert run_step["env"]["CURIE_BIN"] == "cli/target/release/curie"
    assert run_step["env"]["CURIE_E2E_CANDIDATE_TAG"] == "matrix-candidate"
    assert run_step["env"]["SHARD"] == "${{ matrix.shard }}"
    assert "cli/scripts/cluster-upgrade-matrix.sh" in run_step["run"]
    assert '--shard "$SHARD"' in run_step["run"]
    assert "--scenario all" not in run_step["run"]
    assert "${{" not in run_step["run"]
    evidence = named_steps["Upload upgrade matrix evidence"]
    assert evidence["if"] == "always()"
    assert evidence["with"]["name"] == "upgrade-matrix-evidence-${{ matrix.shard }}"
    assert evidence["with"]["path"] == ".projects/2590-evidence"
    teardown = named_steps["Tear down the owned upgrade matrix cluster"]
    assert teardown["if"] == "always()"
    assert "kind delete cluster --name curie-upgrade-matrix" in teardown["run"]
    assert "kind delete cluster --name curie-upgrade " not in teardown["run"]

    # #2733: the script retags matrix-candidate from local docker and
    # kind-loads exclusive tags itself, so the workflow-level kind load of
    # matrix-candidate is gone. The candidate images are built once per run by
    # the ci-images job; a shard only loads and tags them.
    assert not any(
        str(step.get("uses", "")).startswith(
            ("docker/build-push-action@", "docker/bake-action@", "docker/setup-buildx-action@")
        )
        for step in job["steps"]
    )
    assert "Load candidate images into the kind cluster" not in named_steps
    assert not any("kind load" in str(step.get("run", "")) for step in job["steps"])
    load = named_steps["Load the images built by the ci-images job"]
    assert load["uses"] == "./.github/actions/load-ci-images"
    assert load["with"]["images"] == "{api,dispatcher,worker,ui,runner}"
    tag_run = named_steps["Tag the images as the matrix candidate"]["run"]
    for component in ("api", "dispatcher", "worker", "ui", "runner"):
        assert (
            f"docker tag curie-ci/{component}:candidate curie-{component}:matrix-candidate"
        ) in tag_run
    step_names = [step.get("name") for step in job["steps"]]
    assert (
        step_names.index(load["name"])
        < step_names.index("Tag the images as the matrix candidate")
        < step_names.index(run_step["name"])
    )


def test_released_upgrade_workflow_pins_issue_2194_runtime_contract() -> None:
    workflow = yaml.safe_load(WORKFLOW.read_text())
    jobs = workflow["jobs"]
    job = workflow["jobs"]["e2e-released-upgrade"]
    assert set(job["needs"]) == {"rust-build", "changes", "ci-images"}

    named_steps = {step["name"]: step for step in job["steps"] if isinstance(step.get("name"), str)}
    assert len(named_steps) == sum("name" in step for step in job["steps"])

    candidate_images = {
        "api": ("apps/api/Dockerfile", "curie-api:upgrade-candidate"),
        "dispatcher": (
            "apps/dispatcher/Dockerfile",
            "curie-dispatcher:upgrade-candidate",
        ),
        "worker": ("apps/worker/Dockerfile", "curie-worker:upgrade-candidate"),
        "ui": ("apps/ui/Dockerfile", "curie-ui:upgrade-candidate"),
        "runner": ("runner/Dockerfile", "curie-runner:upgrade-candidate"),
    }
    load_images = named_steps["Load the images built by the ci-images job"]
    assert load_images["uses"] == "./.github/actions/load-ci-images"
    assert load_images["with"]["images"] == "{api,dispatcher,worker,ui,runner}"
    tag_run = named_steps["Tag the images as the upgrade candidate"]["run"]
    for component, (_dockerfile, tag) in candidate_images.items():
        assert f"docker tag curie-ci/{component}:candidate {tag}" in tag_run

    load_run = named_steps["Load candidate images into the kind cluster"]["run"]
    for _component, (_dockerfile, tag) in candidate_images.items():
        assert f"  {tag} \\" in load_run or f"  {tag}; do" in load_run
    assert 'kind load docker-image "$image" --name curie-upgrade' in load_run

    download_run = named_steps["Download and verify the exact public v0.8.2 chart"]["run"]
    assert (
        "https://github.com/curie-eng/curie/releases/download/v0.8.2/curie-0.8.2.tgz"
    ) in download_run
    assert "sha256sum --check --strict" in download_run
    assert 'test "$(helm show chart "$released_chart"' in download_run
    assert ')" = "0.8.2"' in download_run

    fixture_run = named_steps["Write the legacy retained values fixture"]["run"]
    for exact_line in (
        "  placeholderText: legacy-retained",
        "    appToken: xapp-example-upgrade",
        "    botToken: xoxb-example-upgrade",
        '    mode: "off"',
        "placement: null",
    ):
        assert exact_line in fixture_run

    image_overrides = (
        "--set api.image.repository=curie-api --set api.image.tag=upgrade-candidate "
        "--set api.image.digest= --set api.image.pullPolicy=Never",
        "--set dispatcher.image.repository=curie-dispatcher "
        "--set dispatcher.image.tag=upgrade-candidate "
        "--set dispatcher.image.digest= --set dispatcher.image.pullPolicy=Never",
        "--set worker.image.repository=curie-worker "
        "--set worker.image.tag=upgrade-candidate "
        "--set worker.image.digest= --set worker.image.pullPolicy=Never",
        "--set ui.image.repository=curie-ui --set ui.image.tag=upgrade-candidate "
        "--set ui.image.digest= --set ui.image.pullPolicy=Never",
        "--set agentSandbox.runner.image=curie-runner "
        "--set agentSandbox.runner.tag=upgrade-candidate "
        "--set agentSandbox.runner.digest= "
        "--set agentSandbox.runner.imagePullPolicy=Never",
    )
    runner_prewarm_override = "--set agentSandbox.runner.prewarm.imagePullPolicy=Never"
    retained_projection = (
        "placeholderText: .dispatcher.placeholderText",
        "slackAppToken: .dispatcher.slack.appToken",
        "slackBotToken: .dispatcher.slack.botToken",
        "gvisorMode: .security.gvisor.mode",
        "placement: .placement",
    )

    install_run = named_steps["Install the exact public v0.8.2 release"]["run"]
    assert "helm get values curie -n curie -o json" in install_run
    for projection in retained_projection:
        assert projection in install_run
    for predicate in (
        '.placeholderText == "legacy-retained"',
        '.slackAppToken == "xapp-example-upgrade"',
        '.slackBotToken == "xoxb-example-upgrade"',
        '.gvisorMode == "off"',
        ".placement == null",
    ):
        assert predicate in install_run

    first_upgrade = named_steps["Upgrade the legacy release to the candidate chart"]["run"]
    second_upgrade = named_steps["Upgrade a second time and preserve the generated attester"]["run"]
    for upgrade, snapshot in (
        (first_upgrade, "first-retained-values.json"),
        (second_upgrade, "second-retained-values.json"),
    ):
        assert "helm upgrade curie charts/curie -n curie" in upgrade
        assert "--reset-then-reuse-values" in upgrade
        for override in image_overrides:
            assert override in upgrade
        assert runner_prewarm_override in upgrade
        assert "helm get values curie -n curie -o json" in upgrade
        for projection in retained_projection:
            assert projection in upgrade
        assert snapshot in upgrade
        assert (f'cmp "$RUNNER_TEMP/retained-values.json" "$RUNNER_TEMP/{snapshot}"') in upgrade
        assert "kubectl rollout status deploy/curie-api" in upgrade
        assert "kubectl rollout status deploy/curie-dispatcher" in upgrade

    assert (
        "printf '%s' \"$first_attester\" | sha256sum | cut -d' ' -f1 "
        '> "$RUNNER_TEMP/first-attester.sha256"'
    ) in first_upgrade
    assert ('first_attester="$(cat "$RUNNER_TEMP/first-attester.sha256")"') in second_upgrade
    assert (
        'test "$(printf \'%s\' "$second_attester" | sha256sum | '
        'cut -d\' \' -f1)" = "$first_attester"'
    ) in second_upgrade

    verifier_step = named_steps["Write the managed attester verifier"]
    verifier_run = verifier_step["run"]
    assert 'test -n "$attester"' in verifier_run
    assert 'test "$attester" != "$api_key"' in verifier_run
    verifier_call = '"$RUNNER_TEMP/verify-managed-attester.sh"'
    assert verifier_call in first_upgrade

    assert "Nil unsafe helper negative control" not in named_steps
    negative_job = jobs["e2e-released-upgrade-negative"]
    assert negative_job["name"] == (
        "E2E released chart upgrade negative control (nil unsafe helpers)"
    )
    assert negative_job["runs-on"] == "ubuntu-latest"
    assert negative_job["if"] == job["if"]
    assert set(negative_job["needs"]) == set(job["needs"])
    negative_steps = {
        step["name"]: step for step in negative_job["steps"] if isinstance(step.get("name"), str)
    }
    assert len(negative_steps) == sum("name" in step for step in negative_job["steps"])
    for shared in (
        "Install Helm",
        "Install Calico so NetworkPolicy is enforced",
        "Download and verify the exact public v0.8.2 chart",
        "Write the legacy retained values fixture",
        "Write the managed attester verifier",
    ):
        assert negative_steps[shared] == named_steps[shared], shared
    negative_load = negative_steps["Load the worker image built by the ci-images job"]
    assert negative_load["uses"] == "./.github/actions/load-ci-images"
    assert negative_load["with"]["images"] == "worker"
    assert negative_steps["Tag the worker image as the upgrade candidate"]["run"] == (
        "docker tag curie-ci/worker:candidate curie-worker:upgrade-candidate"
    )
    assert negative_job["steps"][0] == job["steps"][0]
    assert (
        "kind load docker-image curie-worker:upgrade-candidate"
        in negative_steps["Load the candidate worker image into the kind cluster"]["run"]
    )
    negative_names = [step.get("name") for step in negative_job["steps"]]
    assert negative_names.index("Write the managed attester verifier") < (
        negative_names.index("Nil unsafe helper negative control")
    )
    assert negative_steps["Tear down the disposable negative control cluster"]["if"] == "always()"
    assert negative_steps["Dump negative control diagnostics on failure"]["if"] == ("failure()")

    negative_run = negative_steps["Nil unsafe helper negative control"]["run"]
    placement_mutant_setup = '''placement_mutant="$RUNNER_TEMP/nil-unsafe-placement-chart"
cp -a charts/curie "$placement_mutant"'''
    assert placement_mutant_setup in negative_run
    assert "python3 charts/curie/ci/make-upgrade-mutants.py" in negative_run
    mutant_builder = UPGRADE_MUTANT_BUILDER.read_text()
    assert "match = block_re.search(text)" in mutant_builder
    assert 'if match is None or "| default dict" not in match.group(0):' in mutant_builder
    assert ('mutated = match.group(0).replace("| default dict", "", 1)') in mutant_builder
    assert "start_marker = '{{- define \"curie.managedSecret\" -}}'" in mutant_builder
    placement_upgrade = """if helm upgrade curie-negative "$placement_mutant" -n curie-negative \\
    --reset-then-reuse-values --timeout 15m; then
  echo "nil-unsafe placement mutant unexpectedly upgraded legacy placement:null values" >&2
  exit 1
fi"""
    assert placement_upgrade in negative_run
    placement_rejection = (
        'echo "Released-upgrade rung rejected the nil-unsafe placement mutant as expected"'
    )
    assert placement_rejection in negative_run
    managed_secret_mutation = (
        "kubectl patch secret curie-negative-secrets -n curie-negative --type merge"
    )
    assert negative_run.index(placement_upgrade) < negative_run.index(placement_rejection)
    assert negative_run.index(placement_rejection) < negative_run.index(managed_secret_mutation)
    assert verifier_call in negative_run
    assert f"if {verifier_call}" in negative_run
    assert 'test -z "$negative_attester"' in negative_run
    assert "unexpectedly passed" in negative_run

    health_run = named_steps["Require healthy API after the candidate upgrade"]["run"]
    assert "curl -fsS http://127.0.0.1:28000/health" in health_run
    assert 'grep -q \'"status":"ok"\'' in health_run

    smoke = named_steps["Existing cluster rung smoke on the upgraded candidate"]
    assert smoke["env"]["CURIE_E2E_TIERS"] == "cluster"
    assert "bash cli/scripts/e2e-ladder.sh" in smoke["run"]
    for override in image_overrides:
        assert override in smoke["run"]
    assert runner_prewarm_override in smoke["run"]

    existing_cluster_steps = jobs["e2e-ladder-cluster"]["steps"]
    existing_smoke = [
        step
        for step in existing_cluster_steps
        if step.get("run") == "bash cli/scripts/e2e-ladder.sh"
    ]
    assert len(existing_smoke) == 1
    assert existing_smoke[0]["env"]["CURIE_E2E_TIERS"] == "cluster"

    assert named_steps["Tear down the disposable upgrade cluster"]["if"] == "always()"


def test_released_upgrade_candidate_images_opt_into_forward_only_migrations() -> None:
    workflow = yaml.safe_load(WORKFLOW.read_text())
    steps = workflow["jobs"]["e2e-released-upgrade"]["steps"]
    named_steps = {step["name"]: step for step in steps if isinstance(step.get("name"), str)}

    candidate_upgrade_steps = (
        "Upgrade the legacy release to the candidate chart",
        "Upgrade a second time and preserve the generated attester",
        "Existing cluster rung smoke on the upgraded candidate",
    )
    for name in candidate_upgrade_steps:
        run = named_steps[name]["run"]
        assert "helm upgrade curie charts/curie -n curie" in run
        assert "--set api.image.tag=upgrade-candidate" in run
        assert "--set api.migrate.forwardOnly=true" in run

    migrated_upgrade = named_steps["Upgrade the v0.8.4 release to the candidate chart"]["run"]
    assert "helm upgrade curie charts/curie -n curie" in migrated_upgrade
    assert "api.migrate.forwardOnly=true" in migrated_upgrade
    assert "--set api.image.tag=upgrade-candidate" in migrated_upgrade


def test_released_upgrade_workflow_pins_issue_2097_live_manifest_parity() -> None:
    workflow = yaml.safe_load(WORKFLOW.read_text())
    jobs = workflow["jobs"]
    job = jobs["e2e-released-upgrade"]
    named_steps = {step["name"]: step for step in job["steps"] if isinstance(step.get("name"), str)}

    download_run = named_steps["Download and verify the exact public v0.8.4 chart"]["run"]
    assert (
        "https://github.com/curie-eng/curie/releases/download/v0.8.4/curie-0.8.4.tgz"
    ) in download_run
    assert ("fee20ab73c05d7a888165f980fb82d25150fcba509f19218bafc4c187a9044bb") in download_run
    assert "sha256sum --check --strict" in download_run
    assert 'test "$(helm show chart "$released_chart_084"' in download_run
    assert ')" = "0.8.4"' in download_run

    fixture_run = named_steps["Write the v0.8.4 retained timeout values fixture"]["run"]
    for exact_line in (
        "worker:",
        "  extraEnv:",
        "    - name: CURIE_RUNNER_TOTAL_TIMEOUT_S",
        '      value: "1700"',
        '    mode: "off"',
    ):
        assert exact_line in fixture_run

    published_image_pins = (
        "--set api.image.tag=0.8.4 --set api.image.digest="
        "sha256:8804a35d0c96e9bb2ed0d4f6a990015aab9e9343f6c1ae0b5dc7307e37b50aaf",
        "--set dispatcher.image.tag=0.8.4 --set dispatcher.image.digest="
        "sha256:ab6766db1f7d211e86f6bd816bb5c73ebfe6ee6cbcdf3dc9de9f9a26848bef47",
        "--set worker.image.tag=0.8.4 --set worker.image.digest="
        "sha256:c3117c30ac0a4cdd626c1170a8c976911da601f55781c9ffea6f1d85d4f02656",
        "--set ui.image.tag=0.8.4 --set ui.image.digest="
        "sha256:7e22c984e53478df9d70e96ec1fd3c73601396a23b378f7b9483ef8bf498477b",
        "--set agentSandbox.runner.tag=0.8.4 --set agentSandbox.runner.digest="
        "sha256:4e19b285d4161d4667145ce9ea6e3efd11c1a9e22b1f1f2f49167994515b0b88",
    )
    install_run = named_steps["Install the exact public v0.8.4 release"]["run"]
    assert "helm install" in install_run
    for pin in published_image_pins:
        assert pin in install_run
    assert "$RELEASED_V084_VALUES" in install_run

    upgrade_run = named_steps["Upgrade the v0.8.4 release to the candidate chart"]["run"]
    assert "kubectl delete deploy/curie-worker -n curie" in upgrade_run
    assert (
        'helm get values curie -n curie -o json > "$RUNNER_TEMP/v084-retained-values.json"'
    ) in upgrade_run
    assert "jq '" in upgrade_run
    assert "(reduce $extra[] as $item (" in upgrade_run
    assert "($partition.timeouts[0].value | tonumber) as $timeout" in upgrade_run
    assert ".worker.runnerTotalTimeoutSeconds = $timeout" in upgrade_run
    assert ".worker.extraEnv = $partition.kept" in upgrade_run
    assert '.config.schemaVersion = "0.9.0"' in upgrade_run
    assert '.config.migratedFrom = "0.8.4"' in upgrade_run
    assert (
        'helm upgrade curie charts/curie -n curie -f "$RUNNER_TEMP/v084-migrated-values.json"'
    ) in upgrade_run
    assert "--set api.migrate.forwardOnly=true" in upgrade_run
    assert "--reset-then-reuse-values" not in upgrade_run
    assert "--set worker.deliveryBudgetSeconds=1800" in upgrade_run
    assert "--set worker.terminationGracePeriodSeconds=1860" in upgrade_run
    assert (
        "--set worker.image.repository=curie-worker --set worker.image.tag=upgrade-candidate"
    ) in upgrade_run
    assert (
        'helm get values curie -n curie -o json > "$RUNNER_TEMP/v084-effective-values.json"'
    ) in upgrade_run
    for migration_assertion in (
        ".worker.runnerTotalTimeoutSeconds == 1700",
        ".worker.deliveryBudgetSeconds == 1800",
        '.name == "CURIE_RUNNER_TOTAL_TIMEOUT_S"',
        '.config.schemaVersion == "0.9.0"',
        '.config.migratedFrom == "0.8.4"',
    ):
        assert migration_assertion in upgrade_run

    parity_run = named_steps["Verify live-manifest parity and target-version convergence"]["run"]
    assert "charts/curie/ci/live_manifest_parity.py" in parity_run
    assert "helm get manifest" in parity_run
    assert "kubectl get deploy" in parity_run
    assert "helm get metadata" in parity_run
    assert "charts/curie/Chart.yaml" in parity_run
    assert "CURIE_RUNNER_TOTAL_TIMEOUT_S" in parity_run

    smoke = named_steps["Existing cluster rung smoke on the upgraded candidate"]
    assert smoke["env"]["CURIE_E2E_TIERS"] == "cluster"
    assert "bash cli/scripts/e2e-ladder.sh" in smoke["run"]
    install_names = [step.get("name") for step in job["steps"]]
    assert install_names.index("Install the exact public v0.8.4 release") < (
        install_names.index("Existing cluster rung smoke on the upgraded candidate")
    )
    negative_job = jobs["e2e-released-upgrade-negative"]
    assert "Nil unsafe helper negative control" in {
        step.get("name") for step in negative_job["steps"]
    }

    helm_ci = yaml.safe_load((REPO_ROOT / ".github" / "workflows" / "helm-ci.yaml").read_text())
    helm_runs = "\n".join(
        step.get("run", "")
        for job in helm_ci["jobs"].values()
        for step in job.get("steps", [])
        if isinstance(step.get("run"), str)
    )
    assert "charts/curie/ci/live-manifest-parity-assertions.sh" in helm_runs


def _aggregate_contract() -> tuple[str, dict[str, str]]:
    workflow = yaml.safe_load(WORKFLOW.read_text())
    job = workflow["jobs"]["e2e-required"]
    assert job["name"] == "E2E required"
    assert set(job["needs"]) == {
        "changes",
        "e2e-ladder",
        "e2e-ladder-release",
        "e2e-ladder-cluster",
        "e2e-cluster-chart-regressions",
        "e2e-cluster-rollout-recovery",
        "e2e-cluster-approval-resume-restarts",
        "e2e-released-upgrade",
        "e2e-released-upgrade-negative",
        "e2e-cluster-upgrade-matrix-shards",
        "e2e-cluster-upgrade-matrix",
    }
    assert job["if"] == "${{ !cancelled() }}"

    candidates = [
        step
        for step in job["steps"]
        if isinstance(step, dict)
        and isinstance(step.get("run"), str)
        and isinstance(step.get("env"), dict)
        and AGGREGATE_EXPRESSIONS["changes_result"] in step["env"].values()
    ]
    assert len(candidates) == 1
    step = candidates[0]
    bindings: dict[str, str] = {}
    for semantic_name, expression in AGGREGATE_EXPRESSIONS.items():
        environment_names = [name for name, value in step["env"].items() if value == expression]
        assert len(environment_names) == 1, expression
        bindings[semantic_name] = environment_names[0]
    return step["run"], bindings


def _run_aggregate(
    *,
    script_transform: Callable[[str], str] | None = None,
    **overrides: str,
) -> subprocess.CompletedProcess[str]:
    script, bindings = _aggregate_contract()
    if script_transform is not None:
        script = script_transform(script)
    state = {
        "changes_result": "success",
        "event_name": "push",
        "skill_selected": "false",
        "local_selected": "false",
        "local_release_selected": "false",
        "cluster_selected": "false",
        "released_upgrade_selected": "false",
        "released_upgrade_full": "false",
        "approval_resume_selected": "false",
        "skill_local_result": "skipped",
        "local_release_result": "skipped",
        "cluster_result": "skipped",
        "cluster_chart_result": "skipped",
        "rollout_recovery_result": "skipped",
        "approval_resume_result": "skipped",
        "released_upgrade_result": "skipped",
        "released_upgrade_negative_result": "skipped",
        "upgrade_matrix_shards_result": "success",
        "upgrade_matrix_result": "skipped",
    }
    state.update(overrides)
    environment = os.environ.copy()
    environment.update({bindings[name]: value for name, value in state.items()})
    return subprocess.run(
        ["bash", "--noprofile", "--norc", "-e", "-o", "pipefail", "-c", script],
        cwd=REPO_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )


def test_e2e_required_validates_docs_only_ladder_skips(tmp_path: Path) -> None:
    selected, output = _invoke_selector(tmp_path, "ARCHITECTURE.md")
    assert selected.returncode == 0, selected.stderr
    assert output == _expected_output(pytest_needed=False)
    outputs = dict(line.split("=", maxsplit=1) for line in output.splitlines())

    skipped = _run_aggregate(
        skill_selected=outputs["skill"],
        local_selected=outputs["local"],
        local_release_selected=outputs["local_release"],
        cluster_selected=outputs["cluster"],
        released_upgrade_selected=outputs["released_upgrade"],
        upgrade_matrix_result="skipped",
    )
    assert skipped.returncode == 0, skipped.stdout + skipped.stderr

    unexpected_result = _run_aggregate(
        skill_selected=outputs["skill"],
        local_selected=outputs["local"],
        local_release_selected=outputs["local_release"],
        cluster_selected=outputs["cluster"],
        released_upgrade_selected=outputs["released_upgrade"],
        skill_local_result="success",
    )
    assert unexpected_result.returncode != 0


def test_e2e_required_accepts_a_version_only_selection(tmp_path: Path) -> None:
    """#3858: the required E2E gate passes on the local-release rung alone."""
    selected, output = _invoke_selector(tmp_path, *_version_only_paths(_repo_version()))
    assert selected.returncode == 0, selected.stderr
    outputs = _outputs_of(output)
    assert outputs["version_only"] == "true"
    selection = {
        "event_name": "pull_request",
        "skill_selected": outputs["skill"],
        "local_selected": outputs["local"],
        "local_release_selected": outputs["local_release"],
        "cluster_selected": outputs["cluster"],
        "released_upgrade_selected": outputs["released_upgrade"],
        "released_upgrade_full": outputs["released_upgrade_full"],
    }

    ran = _run_aggregate(**selection, local_release_result="success")
    assert ran.returncode == 0, ran.stdout + ran.stderr

    missing = _run_aggregate(**selection)
    assert missing.returncode != 0


@pytest.mark.parametrize(
    "state",
    [
        {},
        {"skill_selected": "true", "skill_local_result": "success"},
        {"local_selected": "true", "skill_local_result": "success"},
        {
            "skill_selected": "true",
            "local_selected": "true",
            "local_release_selected": "true",
            "cluster_selected": "true",
            "skill_local_result": "success",
            "local_release_result": "success",
            "cluster_result": "success",
            "cluster_chart_result": "success",
            "rollout_recovery_result": "success",
        },
        {
            "event_name": "pull_request",
            "cluster_selected": "true",
            "cluster_result": "success",
            "cluster_chart_result": "success",
            "rollout_recovery_result": "skipped",
        },
        {
            "event_name": "workflow_dispatch",
            "cluster_selected": "true",
            "cluster_result": "success",
            "cluster_chart_result": "success",
            "rollout_recovery_result": "success",
        },
        {
            "released_upgrade_selected": "true",
            "released_upgrade_full": "true",
            "released_upgrade_result": "success",
            "released_upgrade_negative_result": "success",
            "upgrade_matrix_result": "success",
        },
    ],
)
def test_aggregate_accepts_exact_selected_outcomes(state: dict[str, str]) -> None:
    completed = _run_aggregate(**state)
    assert completed.returncode == 0, completed.stdout + completed.stderr


def test_aggregate_requires_upgrade_matrix_when_released_upgrade_is_selected() -> None:
    ok = _run_aggregate(
        released_upgrade_selected="true",
        released_upgrade_full="true",
        released_upgrade_result="success",
        released_upgrade_negative_result="success",
        upgrade_matrix_result="success",
    )
    assert ok.returncode == 0, ok.stdout + ok.stderr
    for result in ("skipped", "failure", "cancelled"):
        rejected = _run_aggregate(
            released_upgrade_selected="true",
            released_upgrade_full="true",
            released_upgrade_result="success",
            released_upgrade_negative_result="success",
            upgrade_matrix_result=result,
        )
        assert rejected.returncode != 0, result


def test_aggregate_requires_upgrade_matrix_skip_when_released_upgrade_is_not_selected() -> None:
    ok = _run_aggregate(upgrade_matrix_result="skipped")
    assert ok.returncode == 0, ok.stdout + ok.stderr
    for result in ("success", "failure", "cancelled"):
        rejected = _run_aggregate(upgrade_matrix_result=result)
        assert rejected.returncode != 0, result


def test_aggregate_pull_request_smoke_runs_matrix_without_released_trio() -> None:
    ok = _run_aggregate(
        released_upgrade_selected="true",
        released_upgrade_full="false",
        upgrade_matrix_result="success",
    )
    assert ok.returncode == 0, ok.stdout + ok.stderr
    for matrix, positive, negative in (
        ("skipped", "skipped", "skipped"),
        ("failure", "skipped", "skipped"),
        ("success", "success", "skipped"),
        ("success", "skipped", "success"),
    ):
        rejected = _run_aggregate(
            released_upgrade_selected="true",
            released_upgrade_full="false",
            upgrade_matrix_result=matrix,
            released_upgrade_result=positive,
            released_upgrade_negative_result=negative,
        )
        assert rejected.returncode != 0, (matrix, positive, negative)


@pytest.mark.parametrize("full", ["", "yes"])
def test_aggregate_rejects_malformed_released_upgrade_full(full: str) -> None:
    completed = _run_aggregate(released_upgrade_full=full)
    assert completed.returncode != 0


def test_aggregate_rejects_full_run_without_released_upgrade() -> None:
    completed = _run_aggregate(
        released_upgrade_full="true",
        released_upgrade_result="success",
        released_upgrade_negative_result="success",
    )
    assert completed.returncode != 0


@pytest.mark.parametrize("result", ["skipped", "failure", "cancelled"])
def test_aggregate_requires_upgrade_matrix_shards_success(result: str) -> None:
    completed = _run_aggregate(upgrade_matrix_shards_result=result)
    assert completed.returncode != 0


@pytest.mark.parametrize(
    "state",
    [
        {"changes_result": "failure"},
        {"skill_selected": "true", "skill_local_result": "skipped"},
        {"local_selected": "true", "skill_local_result": "cancelled"},
        {"local_release_selected": "true", "local_release_result": "failure"},
        {"cluster_selected": "true", "cluster_result": "skipped"},
        {"cluster_selected": "true", "cluster_result": "cancelled"},
        {
            "cluster_selected": "true",
            "cluster_result": "success",
            "cluster_chart_result": "failure",
        },
        {
            "cluster_selected": "true",
            "cluster_result": "success",
            "cluster_chart_result": "skipped",
        },
        {
            "cluster_selected": "true",
            "cluster_result": "failure",
            "cluster_chart_result": "success",
        },
        {"cluster_chart_result": "success"},
        {"skill_local_result": "success"},
        {"local_release_result": "success"},
        {"cluster_result": "success"},
        {"upgrade_matrix_result": "success"},
        {"upgrade_matrix_result": "failure"},
        {"released_upgrade_negative_result": "success"},
        {
            "released_upgrade_selected": "true",
            "released_upgrade_full": "true",
            "released_upgrade_result": "success",
            "released_upgrade_negative_result": "skipped",
        },
        {
            "released_upgrade_selected": "true",
            "released_upgrade_full": "true",
            "released_upgrade_result": "success",
            "released_upgrade_negative_result": "failure",
        },
        {
            "released_upgrade_selected": "true",
            "released_upgrade_full": "true",
            "released_upgrade_result": "skipped",
            "released_upgrade_negative_result": "success",
        },
    ],
)
def test_aggregate_rejects_inconsistent_outcomes(state: dict[str, str]) -> None:
    completed = _run_aggregate(**state)
    assert completed.returncode != 0


@pytest.mark.parametrize("event_name", ["push", "workflow_dispatch"])
@pytest.mark.parametrize("result", ["failure", "skipped", "cancelled"])
def test_aggregate_requires_rollout_recovery_success_when_selected(
    event_name: str, result: str
) -> None:
    # failure is the 2026-09-29 next dispatch run 36551160014 shape: the
    # rollout recovery job failed while E2E required stayed green.
    completed = _run_aggregate(
        event_name=event_name,
        cluster_selected="true",
        cluster_result="success",
        cluster_chart_result="success",
        rollout_recovery_result=result,
    )
    assert completed.returncode != 0, completed.stdout + completed.stderr


@pytest.mark.parametrize("result", ["success", "failure"])
def test_aggregate_rollout_recovery_must_skip_when_not_expected(result: str) -> None:
    on_pull_request = _run_aggregate(
        event_name="pull_request",
        cluster_selected="true",
        cluster_result="success",
        cluster_chart_result="success",
        rollout_recovery_result=result,
    )
    assert on_pull_request.returncode != 0
    unselected = _run_aggregate(
        event_name="push",
        cluster_selected="false",
        rollout_recovery_result=result,
    )
    assert unselected.returncode != 0


def test_negative_control_covers_rollout_recovery_result() -> None:
    state = {
        "event_name": "push",
        "cluster_selected": "true",
        "cluster_result": "success",
        "cluster_chart_result": "success",
        "rollout_recovery_result": "success",
    }
    unmutated = _run_aggregate(**state)
    assert unmutated.returncode == 0, unmutated.stdout + unmutated.stderr

    rollout_result_check = '"$ROLLOUT_RECOVERY_RESULT" != "$rollout_recovery_expected" ||'

    def accept_rollout_drift(script: str) -> str:
        assert script.count(rollout_result_check) == 1
        return script.replace(rollout_result_check, "", 1)

    completed = _run_aggregate(script_transform=accept_rollout_drift, **state)
    output = completed.stdout + completed.stderr
    assert completed.returncode != 0, output
    assert "Selected and skipped negative control failed" in output


def test_aggregate_negative_control_runs_before_real_results() -> None:
    completed = _run_aggregate()
    output = completed.stdout + completed.stderr
    assert completed.returncode == 0, output
    negative_control = "Selected and skipped negative control passed"
    real_result = "E2E required passed"
    assert negative_control in output
    assert real_result in output
    assert output.index(negative_control) < output.index(real_result)


def test_negative_control_rejects_selected_skipped_outcome_when_validator_mutates() -> None:
    unmutated = _run_aggregate(
        skill_selected="true",
        skill_local_result="success",
    )
    assert unmutated.returncode == 0, unmutated.stdout + unmutated.stderr

    skill_result_check = '"$SKILL_LOCAL_RESULT" != "$skill_local_expected" ||'

    def accept_selected_skipped(script: str) -> str:
        assert script.count(skill_result_check) == 1
        return script.replace(skill_result_check, "", 1)

    completed = _run_aggregate(
        script_transform=accept_selected_skipped,
        skill_selected="true",
        skill_local_result="skipped",
    )
    output = completed.stdout + completed.stderr
    assert completed.returncode != 0, output
    assert "Selected and skipped negative control failed" in output


def test_selector_directory_has_no_stdlib_shadowing_modules() -> None:
    # Structural, not behavioral: on Linux `select` is a builtin module (its
    # __file__ is None), so a directly-executed script's own directory on
    # sys.path never wins and the shadow bug cannot be reproduced here. On
    # macOS `select` is a dynamic extension in lib-dynload, which loses to
    # sys.path[0] and crashes. See issue #1878.
    selector_dir = SELECTOR.parent
    for candidate in selector_dir.glob("*.py"):
        assert candidate.stem not in sys.stdlib_module_names, (
            f"{candidate.name} shadows the stdlib module '{candidate.stem}': "
            "Python puts a directly-executed script's own directory first on "
            "sys.path, so this basename shadows the stdlib module for every "
            "import in this process. That's harmless on platforms where the "
            "shadowed module is a builtin (e.g. Linux's `select`), but breaks "
            "the script on platforms where it's a dynamic extension instead "
            "(e.g. macOS's `select`), per issue #1878."
        )


def test_omit_kind_keeps_cluster_for_a_changed_runtime_assertion(tmp_path: Path) -> None:
    # #3391: E2E required refuses a changed runtime assertion without a pass
    # receipt from the cluster rung, so omitting kind must not drop that rung.
    runtime = "charts/curie/ci/runtime/connector-readiness-runtime.sh"
    outputs = _selector_outputs_omit_kind(tmp_path, runtime)
    assert outputs["cluster"] == "true"
    assert outputs["released_upgrade"] == "false"
    # Negative control: a sibling chart CI file is still dropped.
    other = _selector_outputs_omit_kind(tmp_path, "charts/curie/ci/runtime/README.md")
    assert other["cluster"] == "false"


def _selector_outputs_omit_kind(tmp_path: Path, path: str) -> dict[str, str]:
    completed, output = _invoke_selector(tmp_path, path, omit_kind=True)
    assert completed.returncode == 0, completed.stderr
    return dict(line.split("=", maxsplit=1) for line in output.splitlines())


CLUSTER_RUNG_MOVED_PROOFS = {
    "Langfuse web waits for delayed Postgres without restarting": "e2e-cluster-chart-regressions",
    "Connector that never listens stays not-Ready until it binds": "e2e-cluster-chart-regressions",
    "Runner BYO egress enforces (not just rendered)": "e2e-cluster-chart-regressions",
    "Rollout-free first invocation and dead-consumer recovery": "e2e-cluster-rollout-recovery",
}


def test_single_regression_proofs_run_outside_the_cluster_rung() -> None:
    jobs = yaml.safe_load(WORKFLOW.read_text())["jobs"]
    rung_steps = {step.get("name") for step in jobs["e2e-ladder-cluster"]["steps"]}
    for name, job_id in CLUSTER_RUNG_MOVED_PROOFS.items():
        assert name not in rung_steps, name
        steps = [step for step in jobs[job_id]["steps"] if step.get("name") == name]
        assert len(steps) == 1, (job_id, name)
        assert "if" not in steps[0]
        assert "continue-on-error" not in steps[0]

    chart = jobs["e2e-cluster-chart-regressions"]
    assert chart["if"] == "${{ needs.changes.outputs.cluster == 'true' }}"
    assert chart["needs"] == ["changes", "ci-images"]
    assert "e2e-cluster-chart-regressions" in jobs["e2e-required"]["needs"]

    rollout = jobs["e2e-cluster-rollout-recovery"]
    assert rollout["if"] == (
        "${{ github.event_name != 'pull_request' && needs.changes.outputs.cluster == 'true' }}"
    )
    assert "e2e-cluster-rollout-recovery" in jobs["e2e-required"]["needs"]


# #4016: the approval resume kind scenario runs on pull requests whenever a
# worker or approval path changes, and E2E required holds it to that selection.
APPROVAL_RESUME_JOB = "e2e-cluster-approval-resume-restarts"


@pytest.mark.parametrize(
    "path",
    [
        "apps/worker/src/curie_worker/consumer.py",
        "apps/worker/src/curie_worker/kernel.py",
        "apps/api/src/curie_api/resumequeue.py",
        "apps/api/src/curie_api/resumereconciler.py",
        "apps/api/src/curie_api/sweeper.py",
        "apps/api/src/curie_api/routers/approvals.py",
        "apps/api/src/curie_api/routers/approval_recovery.py",
        "apps/api/src/curie_api/approval_policy.py",
        "runner/src/curie_runner/approval.py",
        "cli/scripts/e2e-cluster-approval-resume-restarts.sh",
    ],
)
def test_approval_resume_selected_for_worker_and_approval_paths(tmp_path: Path, path: str) -> None:
    completed, output = _invoke_selector(tmp_path, path)
    assert completed.returncode == 0, completed.stderr
    outputs = _outputs_of(output)
    assert outputs["approval_resume"] == "true"
    # The job needs the images and binary the cluster tier builds.
    assert outputs["cluster"] == "true"


@pytest.mark.parametrize(
    "path",
    [
        "docs/guides/getting-started.md",
        "apps/ui/src/main.tsx",
        "apps/api/src/curie_api/routers/agents.py",
        "apps/api/src/curie_api/resumequeue_helpers/notes.md",
        "runner/src/curie_runner/session.py",
        "cli/scripts/e2e-cluster-rollout-recovery.sh",
        "charts/curie/values.yaml",
    ],
)
def test_approval_resume_not_selected_for_unrelated_paths(tmp_path: Path, path: str) -> None:
    completed, output = _invoke_selector(tmp_path, path)
    assert completed.returncode == 0, completed.stderr
    assert _outputs_of(output)["approval_resume"] == "false"


def test_approval_resume_follows_push_and_kind_omission(tmp_path: Path) -> None:
    pushed, pushed_output = _invoke_selector(tmp_path, push=True)
    assert pushed.returncode == 0, pushed.stderr
    assert _outputs_of(pushed_output)["approval_resume"] == "true"

    next_push, next_output = _invoke_selector(tmp_path, push=True, omit_kind=True)
    assert next_push.returncode == 0, next_push.stderr
    assert _outputs_of(next_output)["approval_resume"] == "false"

    next_pr, next_pr_output = _invoke_selector(
        tmp_path, "apps/api/src/curie_api/resumequeue.py", omit_kind=True
    )
    assert next_pr.returncode == 0, next_pr.stderr
    assert _outputs_of(next_pr_output)["approval_resume"] == "false"


@pytest.mark.parametrize("event_name", ["pull_request", "push", "workflow_dispatch"])
def test_aggregate_requires_approval_resume_success_when_selected(
    event_name: str,
) -> None:
    state = {
        "event_name": event_name,
        "cluster_selected": "true",
        "cluster_result": "success",
        "cluster_chart_result": "success",
        "rollout_recovery_result": ("skipped" if event_name == "pull_request" else "success"),
        "approval_resume_selected": "true",
    }
    ok = _run_aggregate(**state, approval_resume_result="success")
    assert ok.returncode == 0, ok.stdout + ok.stderr
    for result in ("skipped", "failure", "cancelled"):
        rejected = _run_aggregate(**state, approval_resume_result=result)
        assert rejected.returncode != 0, result


@pytest.mark.parametrize("result", ["success", "failure", "cancelled"])
def test_aggregate_requires_approval_resume_skip_when_not_selected(result: str) -> None:
    ok = _run_aggregate(event_name="pull_request", approval_resume_result="skipped")
    assert ok.returncode == 0, ok.stdout + ok.stderr
    rejected = _run_aggregate(event_name="pull_request", approval_resume_result=result)
    assert rejected.returncode != 0, result


@pytest.mark.parametrize("selected", ["", "yes"])
def test_aggregate_rejects_malformed_approval_resume_selection(selected: str) -> None:
    completed = _run_aggregate(approval_resume_selected=selected)
    assert completed.returncode != 0


def test_negative_control_covers_approval_resume_result() -> None:
    state = {
        "event_name": "pull_request",
        "approval_resume_selected": "true",
        "approval_resume_result": "success",
    }
    unmutated = _run_aggregate(**state)
    assert unmutated.returncode == 0, unmutated.stdout + unmutated.stderr

    check = '"$APPROVAL_RESUME_RESULT" != "$approval_resume_expected" ||'

    def accept_drift(script: str) -> str:
        assert script.count(check) == 1
        return script.replace(check, "", 1)

    completed = _run_aggregate(script_transform=accept_drift, **state)
    output = completed.stdout + completed.stderr
    assert completed.returncode != 0, output
    assert "Selected and skipped negative control failed" in output


def test_approval_resume_job_runs_on_pull_requests_and_gates_e2e_required() -> None:
    jobs = yaml.safe_load(WORKFLOW.read_text())["jobs"]
    job = jobs[APPROVAL_RESUME_JOB]
    assert job["name"] == "E2E approval resume after worker and store restarts (kind)"
    assert job["if"] == "${{ needs.changes.outputs.approval_resume == 'true' }}"
    assert "pull_request" not in job["if"]
    assert job["needs"] == ["rust-build", "changes", "ci-images"]
    assert APPROVAL_RESUME_JOB in jobs["e2e-required"]["needs"]

    scenario = [
        step
        for step in job["steps"]
        if step.get("run") == "bash cli/scripts/e2e-cluster-approval-resume-restarts.sh"
    ]
    assert len(scenario) == 1
    assert scenario[0]["env"] == {"CURIE_BIN": "cli/target/release/curie"}
    assert "if" not in scenario[0]
    assert "continue-on-error" not in scenario[0]

    diagnostics = next(
        step for step in job["steps"] if step.get("name") == "Dump cluster diagnostics on failure"
    )
    assert diagnostics["if"] == "failure()"
    assert "app.kubernetes.io/component=api" in diagnostics["run"]
    assert "app.kubernetes.io/component=runner-sandbox" in diagnostics["run"]

    teardown = next(
        step
        for step in job["steps"]
        if step.get("name") == "Tear down the disposable cluster release"
    )
    assert teardown["if"] == "always()"
    assert "kubectl delete namespace curie" in teardown["run"]
    assert "--ignore-not-found" in teardown["run"]
    assert "kind delete cluster" not in teardown["run"]

    # Same pinned install surface as the rollout recovery sibling.
    sibling = jobs["e2e-cluster-rollout-recovery"]

    def uses(steps: list[dict[str, Any]]) -> list[str]:
        return [step["uses"] for step in steps if "uses" in step]

    assert uses(job["steps"]) == uses(sibling["steps"])


def test_controller_preflight_upgrade_scenario_runs_on_chart_pull_requests() -> None:
    jobs = yaml.safe_load(WORKFLOW.read_text())["jobs"]
    job = jobs["e2e-cluster-chart-regressions"]
    name = "Controller preflight accepts a serving upgrade and refuses broken RBAC"
    steps = [step for step in job["steps"] if step.get("name") == name]
    assert len(steps) == 1, "The controller upgrade proof must execute in required PR CI"
    step = steps[0]
    assert "if" not in step
    assert "continue-on-error" not in step
    # This scenario owns its kind cluster; the runtime receipt runner accepts
    # only ci/runtime scripts and would refuse it before executing any proof.
    assert "uv run bash charts/curie/ci/scenarios/controller-preflight-kind.sh" in step["run"]
    assert "tools/runtime-assertion-gate/run.sh" not in step["run"]
    assert "e2e-cluster-chart-regressions" in jobs["e2e-required"]["needs"]

"""Keep this repository's CI and layout facts out of product policy."""

from __future__ import annotations

import ast
import os
import re
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
_SOURCE_SUFFIXES = {".py", ".ts", ".tsx", ".js", ".jsx", ".rs", ".sh", ".c", ".h", ".go", ".css"}
_TEST_DIRS = {"tests", "test", "__tests__"}


def _repository_facts(root: Path) -> tuple[set[tuple[str, bool]], set[str], set[str]]:
    checks: set[tuple[str, bool]] = set()
    for workflow in sorted((root / ".github/workflows").glob("*")):
        if workflow.suffix not in {".yaml", ".yml"}:
            continue
        document = yaml.safe_load(workflow.read_text())
        for job_id, job in document.get("jobs", {}).items():
            name = job.get("name", job_id)
            if not isinstance(name, str):
                continue
            prefix, expression, _ = name.partition("${{")
            # A one word expression prefix such as "Build " is ordinary prose.
            if not expression or len(re.findall(r"\w+", prefix)) >= 2:
                checks.add((prefix if expression else name, bool(expression)))

    test_paths: set[str] = set()
    for parent, directories, _ in os.walk(root):
        directories[:] = [
            name
            for name in directories
            if not name.startswith(".")
            and name not in {"node_modules", "__pycache__", "target", "dist", "build"}
        ]
        for name in directories:
            relative = (Path(parent) / name).relative_to(root)
            if name in _TEST_DIRS and len(relative.parts) > 1:
                test_paths.add(relative.as_posix())
        directories[:] = [name for name in directories if name not in _TEST_DIRS]
    roots = {
        path.name for path in root.iterdir() if path.is_dir() and not path.name.startswith(".")
    }
    return checks, test_paths, roots


def _python_literals(text: str) -> tuple[list[tuple[int, str]], list[tuple[int, list[str]]]]:
    tree = ast.parse(text)
    docstrings = {
        id(node.body[0].value)
        for node in ast.walk(tree)
        if isinstance(node, ast.Module | ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef)
        and node.body
        and isinstance(node.body[0], ast.Expr)
        and isinstance(node.body[0].value, ast.Constant)
        and isinstance(node.body[0].value.value, str)
    }
    strings = [
        (node.lineno, node.value)
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and id(node) not in docstrings
    ]
    collections = [
        (
            node.lineno,
            [
                item.value
                for item in node.elts
                if isinstance(item, ast.Constant) and isinstance(item.value, str)
            ],
        )
        for node in ast.walk(tree)
        if isinstance(node, ast.List | ast.Tuple | ast.Set)
    ]
    return strings, collections


def _other_literals(
    text: str, suffix: str
) -> tuple[list[tuple[int, str]], list[tuple[int, list[str]]]]:
    comment = r"\#[^\n]*" if suffix == ".sh" else r"//[^\n]*|/\*[\s\S]*?\*/"
    tokens = re.compile(
        rf"(?P<comment>{comment})|"
        r'''(?P<string>"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'|`(?:\\.|[^`\\])*`)|'''
        r"(?P<bracket>[\[\]{}()])"
    )
    strings: list[tuple[int, str]] = []
    collections: list[tuple[int, list[str]]] = []
    stack: list[tuple[str, int, list[str]]] = []
    for token in tokens.finditer(text):
        line = text.count("\n", 0, token.start()) + 1
        if token.lastgroup == "string":
            value = re.sub(r"\\([\\'\"`/])", r"\1", token.group()[1:-1])
            strings.append((line, value))
            if stack:
                stack[-1][2].append(value)
        elif token.lastgroup == "bracket":
            bracket = token.group()
            assignment = text[: token.start()].rstrip().endswith("=")
            if bracket == "[" or (assignment and bracket in {"{", "("}):
                stack.append((bracket, line, []))
            elif stack and bracket == {"[": "]", "{": "}", "(": ")"}[stack[-1][0]]:
                _, start_line, values = stack.pop()
                collections.append((start_line, values))
    return strings, collections


def _repository_fact_violations(root: Path) -> list[str]:
    checks, test_paths, root_dirs = _repository_facts(root)
    patterns = [
        (
            "workflow check",
            name,
            re.compile(r"(?<![\w.-])" + re.escape(name) + ("" if prefix else r"(?![\w-])")),
        )
        for name, prefix in sorted(checks)
    ]
    patterns.extend(
        (
            "test path",
            name,
            re.compile(r"(?<![\w./-])(?:\./)?" + re.escape(name) + r"(?![\w.-])"),
        )
        for name in sorted(test_paths)
    )
    violations: list[str] = []
    source_roots = [*root.glob("apps/*/src"), root / "runner/src", *root.glob("packages/*/src")]
    for source_root in source_roots:
        for path in sorted(source_root.rglob("*")):
            relative = path.relative_to(root)
            if (
                path.suffix not in _SOURCE_SUFFIXES
                or set(path.relative_to(source_root).parts[:-1])
                & (_TEST_DIRS | {"docs", "examples", "fixtures", "config"})
                or path.name.startswith("test_")
                or ".test." in path.name
                or ".spec." in path.name
            ):
                continue
            text = path.read_text()
            if path.suffix == ".py":
                strings, collections = _python_literals(text)
            else:
                strings, collections = _other_literals(text, path.suffix)
            for line, value in strings:
                for kind, fact, pattern in patterns:
                    if pattern.search(value):
                        violations.append(f"{relative}:{line}: {kind} {fact!r}")
            for line, values in collections:
                directory_policy = {value.removeprefix("./").rstrip("/") for value in values}
                # Generic source vocabularies also name directories absent here.
                if len(directory_policy) > 1 and directory_policy <= root_dirs:
                    violations.append(
                        f"{relative}:{line}: root directory policy {sorted(directory_policy)!r}"
                    )
    return sorted(violations)


def _write(root: Path, relative: str, text: str) -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


@pytest.fixture(scope="session")
def repository_facts() -> tuple[set[tuple[str, bool]], set[str], set[str]]:
    return _repository_facts(REPO_ROOT)


@pytest.fixture
def repository(
    tmp_path: Path, repository_facts: tuple[set[tuple[str, bool]], set[str], set[str]]
) -> Path:
    for workflow in (REPO_ROOT / ".github/workflows").glob("*"):
        if workflow.suffix in {".yaml", ".yml"}:
            _write(tmp_path, workflow.relative_to(REPO_ROOT).as_posix(), workflow.read_text())
    _, test_paths, root_dirs = repository_facts
    for relative in test_paths | root_dirs:
        (tmp_path / relative).mkdir(parents=True, exist_ok=True)
    return tmp_path


def test_product_source_does_not_embed_repository_facts() -> None:
    assert not (violations := _repository_fact_violations(REPO_ROOT)), "\n".join(violations)


@pytest.mark.parametrize(
    "source",
    [
        'REQUIRED = "Python (ruff + mypy + pytest)"',
        'PREFLIGHT = "uv run pytest runner/tests -q"',
        'SHARD_PREFIX = "Python pytest (shard "',
        'SHARD_NAME = "Python pytest (shard 2/3)"',
        'PATH_POLICY = ("apps/", "packages/", "runner/")',
        'PATH_POLICY = ["apps", "cli", "charts"]',
        'PATH_POLICY = {"./apps/", "runner/"}',
        'METADATA_CHECKS = frozenset({"PR body (real newlines)", "Fix pin verification"})',
    ],
)
@pytest.mark.parametrize(
    "source_root", ["apps/widget/src", "runner/src", "packages/widget/src"]
)
def test_historical_product_policy_mutations_fail(
    repository: Path, source: str, source_root: str
) -> None:
    _write(repository, f"{source_root}/policy.py", source)
    assert _repository_fact_violations(repository)


@pytest.mark.parametrize("suffix", [".ts", ".tsx", ".sh", ".c", ".css"])
def test_other_product_source_literals_are_scanned(repository: Path, suffix: str) -> None:
    source = (
        ':root { --repository-policy: "uv run pytest runner/tests -q"; }'
        if suffix == ".css"
        else 'policy = "uv run pytest runner/tests -q";'
    )
    _write(
        repository,
        f"apps/widget/src/policy{suffix}",
        source,
    )
    assert _repository_fact_violations(repository)


def test_comments_documentation_tests_and_configuration_are_excluded(repository: Path) -> None:
    forbidden = '"Python (ruff + mypy + pytest)"'
    for relative in (
        "docs/policy.py",
        "examples/policy.py",
        "apps/widget/config/policy.py",
        "apps/widget/tests/policy.py",
        "apps/widget/src/tests/policy.py",
        "apps/widget/src/docs/policy.py",
        "apps/widget/src/examples/policy.py",
        "apps/widget/src/config/policy.py",
        "apps/widget/src/test_policy.py",
        "apps/widget/src/policy.test.ts",
        "apps/widget/src/policy.spec.tsx",
        "apps/widget/src/policy.yaml",
        "packages/widget/src/schema.json",
    ):
        _write(repository, relative, forbidden)
    _write(repository, "runner/src/policy.py", f'{forbidden}\n# REQUIRED = {forbidden}\n')
    _write(repository, "apps/widget/src/policy.ts", f"// {forbidden}\n/* {forbidden} */\n")
    _write(repository, "runner/src/policy.sh", f"# {forbidden}\n")
    assert not _repository_fact_violations(repository)


def test_generic_strings_and_safe_boundaries_pass(repository: Path) -> None:
    _write(
        repository,
        "apps/widget/src/policy.py",
        'CHECKS = ["required check", "Python tests", "Build artifact", "Merge result"]\n'
        'PATHS = ["src", "unitconv", "apps"]\n'
        'DIRECTORY_VOCABULARY = ["src", "lib", "apps", "runner"]\n'
        'TEST_KIND = "tests"\n'
        'COMMAND = "uv run pytest unitconv -q"\n'
        'OTHER_PATH = "foreign/runner/tests runner/tests_backup myrunner/tests"\n'
        'OTHER_CHECK = "NotPython (ruff + mypy + pytest)extended Python pytest (shardish)"\n',
    )
    assert not _repository_fact_violations(repository)


def test_workflow_and_layout_changes_extend_the_guard(repository: Path) -> None:
    _write(
        repository,
        ".github/workflows/foreign.yml",
        'jobs:\n  static:\n    name: Foreign syntax gate\n'
        '  dynamic:\n    name: Foreign integration (lane ${{ matrix.lane }})\n'
        '  foreign_unnamed_syntax_gate:\n    runs-on: ubuntu-latest\n',
    )
    (repository / "new_component/tests").mkdir(parents=True)
    (repository / "another_component").mkdir()
    _write(
        repository,
        "apps/widget/src/policy.py",
        'CHECK = "Foreign syntax gate"\n'
        'UNNAMED_CHECK = "foreign_unnamed_syntax_gate"\n'
        'PREFIX = "Foreign integration (lane "\n'
        'COMMAND = "pytest new_component/tests"\n'
        'PATH_POLICY = ["new_component/", "another_component/"]\n',
    )
    violations = _repository_fact_violations(repository)
    assert len(violations) == 5, "\n".join(violations)
    assert any("Foreign syntax gate" in finding for finding in violations)
    assert any("foreign_unnamed_syntax_gate" in finding for finding in violations)
    assert any("Foreign integration (lane " in finding for finding in violations)
    assert any("new_component/tests" in finding for finding in violations)
    assert any("root directory policy" in finding for finding in violations)

"""Scan Python sources for forge coupling outside the forge ports (ADR 0197).

Two kinds of offence are found by parsing, not by grepping:

1. an import of a module the caller forbids (an adapter package such as
   ``curie_api.forges.github``, or for the worker any ``curie_api.forges``);
2. a GitHub REST literal in a string constant: the API host, the github.com
   base, the API version header, or the GitHub media type. Docstrings are
   prose and are not scanned; comments never reach the AST.

Relative imports are resolved against the scanned module's package, so
``from .forges.github import x`` is caught like its absolute form.
"""

from __future__ import annotations

import ast
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path

GITHUB_LITERALS: tuple[str, ...] = (
    "api.github.com",
    "https://github.com",
    "x-github-api-version",
    "application/vnd.github",
)


@dataclass(frozen=True)
class Offence:
    path: str
    line: int
    reason: str


ImportRule = Callable[[str], bool]


def _docstring_nodes(tree: ast.Module) -> set[int]:
    found: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Module | ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef):
            body = node.body
            if (
                body
                and isinstance(body[0], ast.Expr)
                and isinstance(body[0].value, ast.Constant)
                and isinstance(body[0].value.value, str)
            ):
                found.add(id(body[0].value))
    return found


def _resolve(module: str, is_package: bool, level: int, target: str | None) -> str:
    if level == 0:
        return target or ""
    parts = module.split(".")
    package = parts if is_package else parts[:-1]
    base = package[: len(package) - (level - 1)] if level > 1 else package
    return ".".join([*base, *([target] if target else [])])


def _imported(node: ast.AST, module: str, is_package: bool) -> Iterator[str]:
    if isinstance(node, ast.Import):
        for alias in node.names:
            yield alias.name
    elif isinstance(node, ast.ImportFrom):
        base = _resolve(module, is_package, node.level, node.module)
        yield base
        for alias in node.names:
            if alias.name != "*":
                yield f"{base}.{alias.name}"


def scan_source(
    source: str,
    *,
    path: str,
    module: str,
    is_package: bool = False,
    forbidden_import: ImportRule,
) -> list[Offence]:
    """Every offence in one module's source."""

    tree = ast.parse(source, filename=path)
    docstrings = _docstring_nodes(tree)
    offences: list[Offence] = []
    for node in ast.walk(tree):
        # One offence per import statement: its first forbidden name.
        for name in _imported(node, module, is_package):
            if forbidden_import(name):
                offences.append(Offence(path, getattr(node, "lineno", 0), f"imports {name}"))
                break
        if (
            isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and id(node) not in docstrings
        ):
            folded = node.value.casefold()
            for literal in GITHUB_LITERALS:
                if literal in folded:
                    offences.append(Offence(path, node.lineno, f"GitHub literal {literal!r}"))
    return offences


def module_name(source_root: Path, file: Path) -> tuple[str, bool]:
    relative = file.relative_to(source_root).with_suffix("")
    parts = list(relative.parts)
    if parts[-1] == "__init__":
        return ".".join(parts[:-1]), True
    return ".".join(parts), False


def scan_tree(
    repo_root: Path,
    source_root: Path,
    *,
    forbidden_import: ImportRule,
    skip: Callable[[Path], bool] = lambda _path: False,
) -> dict[str, list[Offence]]:
    """Offences per repo-relative path for every ``.py`` file under ``source_root``.

    Test directories are never scanned.
    """

    found: dict[str, list[Offence]] = {}
    for file in sorted(source_root.rglob("*.py")):
        if "tests" in file.relative_to(source_root).parts or skip(file):
            continue
        relative = file.relative_to(repo_root).as_posix()
        module, is_package = module_name(source_root, file)
        offences = scan_source(
            file.read_text(encoding="utf-8"),
            path=relative,
            module=module,
            is_package=is_package,
            forbidden_import=forbidden_import,
        )
        if offences:
            found[relative] = offences
    return found


def adapter_packages(forges_dir: Path) -> frozenset[str]:
    """The adapter packages: every package directory directly under ``forges``."""

    return frozenset(
        child.name for child in forges_dir.iterdir() if (child / "__init__.py").is_file()
    )


def api_import_rule(adapters: frozenset[str]) -> ImportRule:
    """Outside ``forges``, an adapter package may not be imported."""

    def forbidden(name: str) -> bool:
        parts = name.split(".")
        return parts[:2] == ["curie_api", "forges"] and len(parts) > 2 and parts[2] in adapters

    return forbidden


def worker_import_rule(name: str) -> bool:
    """The worker holds no forge code at all, not even the ports."""

    parts = name.split(".")
    return parts[:2] == ["curie_api", "forges"]

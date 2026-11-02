"""Static pytest impact selection for pull request CI.

A pull request runs only the test files its diff can reach, boots only the
services those files need, and uses fewer shards when little is selected.
Everything here is computed statically with `ast` over tracked files.

This module imports only the standard library. The changes job runs the
selector with nothing but PyYAML installed, and the root conftest loads this
file by path to classify service tiers (`tiers_for`).
"""

from __future__ import annotations

import ast
import bisect
import hashlib
import json
import math
import os
import re
import subprocess
import sys
import tomllib
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path, PurePosixPath

TIER_NAMES = ("core", "storage", "full")
WORKFLOW_PATH = ".github/workflows/ci.yaml"
REGISTRY_PATH = ".github/e2e-selection.yaml"
WORKFLOW_HEADER = "<header>"
ROOT_CONFTEST = "conftest.py"
EMPTY_IMPACT = "empty impact"
SHARED_LIVE_STATE_GROUP = "shared-live-state"
PROTECTED_RUNNER_GROUP = "protected-source-runner-port"
# Paths under these roots form a region from their first two segments.
TWO_SEGMENT_ROOTS = frozenset({"apps", "packages", "tools", "examples", "adapters"})
# Names a test uses for the repository root when it scans the whole tree.
# Lowercase `root` and `repo` usually name temporary directories, so only the
# unambiguous lowercase spellings (fixtures, locals) count.
REPO_ROOT_NAMES = frozenset(
    {
        "ROOT",
        "REPO_ROOT",
        "REPO",
        "REPOSITORY_ROOT",
        "WORKSPACE_ROOT",
        "repo_root",
        "repository_root",
        "workspace_root",
    }
)
# Imports whose application lifespan needs the object store.
STORAGE_IMPORTS = frozenset({"curie_api", "curie_worker"})
CONFIG_KEYS = (
    "everything",
    "no_impact",
    "workflow_jobs",
    "excluded",
    "always",
    "reader_only",
    "min_shard_share",
    "max_shards",
)

_FULL_TOKEN = re.compile(r"langfuse|clickhouse|otel[-_ ]collector|otel_exporter_otlp", re.I)
_FULL_PORT = re.compile(r"(?<!\d)(?:23000|28123|29009|24317|24318|28888)(?!\d)")
_STORAGE_TOKEN = re.compile(r"rustfs|(?<![a-z0-9])s3(?![a-z0-9])|bucket", re.I)
_STORAGE_PORT = re.compile(r"(?<!\d)(?:29000|29001)(?!\d)")
_PATH_TOKEN = re.compile(r"(?<![\w./-])[\w.@+-]+(?:/[\w.@+-]+)+")
_NAME_STRING = re.compile(r"[A-Za-z_][\w.-]{0,99}")
_REQUIREMENT_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")
# Path-taking calls whose string arguments are joined into one reference.
_JOIN_CALLS = frozenset({"Path", "PurePath", "PurePosixPath", "joinpath"})
_TEMPORARY_BASE = re.compile(r"tmp|temp", re.I)


def _under(path: str, prefix: str) -> bool:
    return path == prefix or path.startswith(f"{prefix}/")


def _under_any(path: str, prefixes: Iterable[str]) -> bool:
    return any(_under(path, prefix) for prefix in prefixes)


def _normalize_project(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def _is_test_name(path: str) -> bool:
    name = path.rsplit("/", 1)[-1]
    return (name.startswith("test_") and name.endswith(".py")) or name.endswith("_test.py")


def _git_tracked(root: Path) -> tuple[str, ...] | None:
    try:
        completed = subprocess.run(
            ["git", "ls-files", "-z"],
            cwd=root,
            capture_output=True,
            text=True,
            check=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return tuple(path for path in completed.stdout.split("\0") if path)


def _read_toml(path: Path) -> dict[str, object]:
    try:
        return tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, tomllib.TOMLDecodeError):
        return {}


def _table(document: Mapping[str, object], *keys: str) -> dict[str, object]:
    current: object = document
    for key in keys:
        if not isinstance(current, dict):
            return {}
        current = current.get(key)
    return current if isinstance(current, dict) else {}


def _string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str)]


def _workspace_members(root: Path) -> tuple[str, ...]:
    document = _read_toml(root / "pyproject.toml")
    members = _string_list(_table(document, "tool", "uv", "workspace").get("members"))
    return tuple(member.strip("/") for member in members if member.strip("/"))


def _workspace_modules(
    members: Sequence[str], tracked: Sequence[str] | None, root: Path
) -> dict[str, frozenset[str]]:
    """Top level module name -> the members whose `src/` provides it."""
    found: dict[str, set[str]] = {}
    for member in members:
        names: set[str] = set()
        prefix = f"{member}/src/"
        if tracked is not None:
            for path in tracked:
                if path.startswith(prefix):
                    head, _, rest = path[len(prefix) :].partition("/")
                    if rest or head.endswith(".py"):
                        names.add(head.removesuffix(".py"))
        else:
            source = root / member / "src"
            if source.is_dir():
                for entry in source.iterdir():
                    if entry.is_dir() or entry.suffix == ".py":
                        names.add(entry.name.removesuffix(".py"))
        for name in names:
            if name.isidentifier():
                found.setdefault(name, set()).add(member)
    return {name: frozenset(owners) for name, owners in found.items()}


_DOCSTRING_OWNERS = (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)
_CHILD_FIELDS: dict[type, tuple[str, ...]] = {}


def _walk(tree: ast.AST) -> Iterator[ast.AST]:
    """Pre-order walk that skips expression contexts; faster than `ast.walk`."""
    stack = [tree]
    while stack:
        node = stack.pop()
        yield node
        kind = type(node)
        fields = _CHILD_FIELDS.get(kind)
        if fields is None:
            fields = tuple(field for field in kind._fields if field != "ctx")
            _CHILD_FIELDS[kind] = fields
        for field in fields:
            value = getattr(node, field, None)
            if isinstance(value, list):
                stack.extend(item for item in reversed(value) if isinstance(item, ast.AST))
            elif isinstance(value, ast.AST):
                stack.append(value)


def _docstring_of(node: ast.AST) -> ast.Constant | None:
    body = getattr(node, "body", None)
    if body and isinstance(body[0], ast.Expr):
        value = body[0].value
        if isinstance(value, ast.Constant) and isinstance(value.value, str):
            return value
    return None


def _callable_name(node: ast.expr) -> str | None:
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, ast.Name):
        return node.id
    return None


def _is_repo_root(node: ast.expr, roots: frozenset[int] | set[int] = frozenset()) -> bool:
    if id(node) in roots:
        return True
    if isinstance(node, ast.Call) and _callable_name(node.func) == "str" and node.args:
        return _is_repo_root(node.args[0], roots)
    return isinstance(node, ast.Name | ast.Attribute) and _callable_name(node) in REPO_ROOT_NAMES


def _string(node: ast.expr) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _division_operands(node: ast.expr) -> list[ast.expr]:
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div):
        return [*_division_operands(node.left), node.right]
    return [node]


def _joined(parts: Iterable[str]) -> str:
    return "/".join(part.strip("/") for part in parts if part.strip("/"))


def _clean_reference(value: str) -> str:
    parts = [part for part in value.strip().split("/") if part]
    while parts and parts[0] == ".":
        parts.pop(0)
    return "/".join(parts)


@dataclass(frozen=True)
class _FileScan:
    """What one Python file contributes, docstrings excluded.

    `repo_wide` is set when the file scans the whole tree, and also when it
    builds a path from `__file__` that cannot be resolved statically: either
    way the file can read anything, so it is selected on every diff.
    """

    imports: frozenset[str]
    relative: tuple[tuple[int, str, tuple[str, ...]], ...]
    names: frozenset[str]
    references: frozenset[str]
    tier: int
    repo_wide: bool
    weight: int


_EMPTY_SCAN = _FileScan(frozenset(), (), frozenset(), frozenset(), 0, False, 0)


class _Unresolved(Exception):
    """A path built from `__file__` that static evaluation cannot follow."""


# A statically evaluated path: repository relative parts, and whether it is
# exact. An inexact path stands for something at or below it (a directory
# joined with a runtime value), so it cannot be extended further.
_AnchoredPath = tuple[tuple[str, ...], bool]
_IDENTITY_CALLS = frozenset(
    {
        "str",
        "fspath",
        "abspath",
        "realpath",
        "normpath",
        "Path",
        "PurePath",
        "PurePosixPath",
        "PosixPath",
    }
)
_IDENTITY_METHODS = frozenset({"resolve", "absolute", "expanduser", "as_posix"})


def _join_parts(base: tuple[str, ...], text: str) -> tuple[str, ...]:
    parts = list(base)
    if text.startswith("/"):
        raise _Unresolved(text)
    for part in text.split("/"):
        if part in {"", "."}:
            continue
        if part == "..":
            if not parts:
                raise _Unresolved(text)
            parts.pop()
        else:
            parts.append(part)
    return tuple(parts)


class _Anchors:
    """Evaluates path expressions rooted at `__file__` to repository paths.

    Returns None for an expression that does not involve `__file__`, a set of
    anchored paths for one it can follow, and raises `_Unresolved` for one it
    cannot (a computed `parents` index, a path above the repository root).
    """

    def __init__(self, tree: ast.Module, path: str) -> None:
        self.file = tuple(part for part in path.split("/") if part)
        self.assigned: dict[str, list[ast.expr]] = {}
        for node in _walk(tree):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        self.assigned.setdefault(target.id, []).append(node.value)
            elif isinstance(node, ast.AnnAssign | ast.NamedExpr) and node.value is not None:
                if isinstance(node.target, ast.Name):
                    self.assigned.setdefault(node.target.id, []).append(node.value)
        self._names: dict[str, frozenset[_AnchoredPath] | None] = {}
        self._active: set[str] = set()

    def text(self, node: ast.expr) -> str | None:
        value = _string(node)
        if value is not None:
            return value
        if isinstance(node, ast.Name) and node.id not in self._active:
            values = self.assigned.get(node.id, [])
            if len(values) == 1:
                self._active.add(node.id)
                try:
                    return self.text(values[0])
                finally:
                    self._active.discard(node.id)
        return None

    def name(self, identifier: str) -> frozenset[_AnchoredPath] | None:
        if identifier == "__file__":
            return frozenset({(self.file, True)})
        if identifier in self._names:
            return self._names[identifier]
        if identifier in self._active:
            return None
        self._active.add(identifier)
        try:
            found: set[_AnchoredPath] = set()
            anchored = False
            for value in self.assigned.get(identifier, ()):
                result = self.evaluate(value)
                if result is not None:
                    anchored = True
                    found.update(result)
        finally:
            self._active.discard(identifier)
        resolved = frozenset(found) if anchored else None
        self._names[identifier] = resolved
        return resolved

    def _join(
        self, base: frozenset[_AnchoredPath], components: Sequence[ast.expr]
    ) -> frozenset[_AnchoredPath]:
        texts = [self.text(component) for component in components]
        joined: set[_AnchoredPath] = set()
        for parts, exact in base:
            if not exact:
                joined.add((parts, False))
                continue
            current = parts
            for text in texts:
                if text is None:
                    joined.add((current, False))
                    break
                current = _join_parts(current, text)
            else:
                joined.add((current, True))
        return frozenset(joined)

    @staticmethod
    def _parent(base: frozenset[_AnchoredPath], levels: int) -> frozenset[_AnchoredPath]:
        result: set[_AnchoredPath] = set()
        for parts, exact in base:
            if not exact:
                result.add((parts, False))
                continue
            if levels > len(parts):
                raise _Unresolved("above the repository root")
            result.add((parts[: len(parts) - levels], True))
        return frozenset(result)

    def evaluate(self, node: ast.expr) -> frozenset[_AnchoredPath] | None:
        if isinstance(node, ast.Name):
            return self.name(node.id)
        if isinstance(node, ast.Attribute):
            if node.attr == "parent":
                base = self.evaluate(node.value)
                return None if base is None else self._parent(base, 1)
            return None
        if isinstance(node, ast.Subscript):
            if isinstance(node.value, ast.Attribute) and node.value.attr == "parents":
                base = self.evaluate(node.value.value)
                if base is None:
                    return None
                index = node.slice
                if (
                    isinstance(index, ast.Constant)
                    and type(index.value) is int
                    and index.value >= 0
                ):
                    return self._parent(base, index.value + 1)
                raise _Unresolved("computed parents index")
            return None
        if isinstance(node, ast.BinOp):
            base = self.evaluate(node.left)
            if base is None:
                return None
            if isinstance(node.op, ast.Div):
                return self._join(base, [node.right])
            if isinstance(node.op, ast.Add):
                text = self.text(node.right)
                if text is not None and text.startswith("/"):
                    return self._join(base, [ast.Constant(text.lstrip("/"))])
                return frozenset((parts, False) for parts, _ in base)
            return None
        if isinstance(node, ast.JoinedStr) and node.values:
            first = node.values[0]
            if not isinstance(first, ast.FormattedValue):
                return None
            base = self.evaluate(first.value)
            if base is None:
                return None
            rest: list[ast.expr] = []
            for value in node.values[1:]:
                text = _string(value) if isinstance(value, ast.Constant) else None
                rest.append(ast.Constant(text.lstrip("/")) if text is not None else value)
            return self._join(base, rest)
        if isinstance(node, ast.Call):
            name = _callable_name(node.func)
            method = isinstance(node.func, ast.Attribute)
            if method and name in _IDENTITY_METHODS and not node.args:
                return self.evaluate(node.func.value)  # type: ignore[union-attr]
            if method and name == "joinpath":
                base = self.evaluate(node.func.value)  # type: ignore[union-attr]
                return None if base is None else self._join(base, node.args)
            if method and name in {"with_name", "with_suffix", "with_stem"}:
                base = self.evaluate(node.func.value)  # type: ignore[union-attr]
                if base is None:
                    return None
                parent = self._parent(base, 1)
                text = self.text(node.args[0]) if node.args else None
                if name == "with_name" and text is not None:
                    return self._join(parent, [ast.Constant(text)])
                # Another suffix or stem is a sibling: anything in that directory.
                return frozenset((parts, False) for parts, _ in parent)
            if name == "dirname" and node.args:
                base = self.evaluate(node.args[0])
                return None if base is None else self._parent(base, 1)
            if name == "join" and method and node.args:
                base = self.evaluate(node.args[0])
                return None if base is None else self._join(base, node.args[1:])
            if name in _IDENTITY_CALLS and node.args:
                base = self.evaluate(node.args[0])
                if base is None or name not in _JOIN_CALLS | {"PosixPath"}:
                    return base
                return self._join(base, node.args[1:])
        return None

    def references(self, tree: ast.Module) -> tuple[set[str], set[int], set[int]]:
        """Anchored references, the ids of the nodes they cover, and of root nodes.

        Raises `_Unresolved` when any anchored expression cannot be followed.
        """
        found: set[str] = set()
        covered: set[int] = set()
        roots: set[int] = set()
        stack: list[ast.AST] = [tree]
        while stack:
            node = stack.pop()
            if isinstance(
                node,
                ast.Name | ast.Attribute | ast.Subscript | ast.BinOp | ast.Call | ast.JoinedStr,
            ):
                result = self.evaluate(node)  # type: ignore[arg-type]
                if result is not None:
                    if ((), False) in result:
                        # The repository root joined with a runtime value
                        # can name any file.
                        raise _Unresolved("computed path under the repository root")
                    covered.update(id(child) for child in ast.walk(node))
                    if ((), True) in result:
                        roots.add(id(node))
                    for parts, _exact in result:
                        if parts:
                            found.add("/".join(parts))
                    continue
            stack.extend(ast.iter_child_nodes(node))
        return found, covered, roots


def _is_test_class(node: ast.ClassDef) -> bool:
    """pytest collects `Test*` classes and every unittest.TestCase subclass."""
    return node.name.startswith("Test") or any(
        (_callable_name(base) or "").endswith("TestCase") for base in node.bases
    )


def _weight(tree: ast.Module) -> int:
    """Static test count: `test*` functions at module level or in a test class."""
    count = 0
    for node in tree.body:
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef) and node.name.startswith(
            "test"
        ):
            count += 1
        elif isinstance(node, ast.ClassDef) and _is_test_class(node):
            count += sum(
                1
                for member in node.body
                if isinstance(member, ast.FunctionDef | ast.AsyncFunctionDef)
                and member.name.startswith("test")
            )
    return count


def _scans_repository(node: ast.Call, roots: set[int]) -> bool:
    name = _callable_name(node.func)
    if isinstance(node.func, ast.Attribute):
        base = node.func.value
        if name == "rglob" and _is_repo_root(base, roots):
            return True
        if name == "glob" and _is_repo_root(base, roots) and node.args:
            pattern = _string(node.args[0])
            if pattern is not None and pattern.startswith("**"):
                return True
    if name == "walk" and node.args and _is_repo_root(node.args[0], roots):
        return True
    words: list[ast.expr] = []
    for argument in node.args:
        if isinstance(argument, ast.List | ast.Tuple):
            words.extend(argument.elts)
        else:
            words.append(argument)
    texts = [_string(word) or "" for word in words]
    if not any("ls-files" in text for text in texts):
        return False
    if any(
        keyword.arg == "cwd" and _is_repo_root(keyword.value, roots) for keyword in node.keywords
    ):
        return True
    return any(
        texts[index] == "-C" and _is_repo_root(words[index + 1], roots)
        for index in range(len(words) - 1)
    )


def _computed_root_join(node: ast.Call, roots: set[int]) -> bool:
    """`os.path.join(ROOT, value)` or `ROOT.joinpath(value)` with a runtime value."""
    if _callable_name(node.func) == "joinpath" and isinstance(node.func, ast.Attribute):
        base, rest = node.func.value, list(node.args)
    elif node.args:
        base, rest = node.args[0], list(node.args[1:])
    else:
        return False
    return bool(rest) and _is_repo_root(base, roots) and _string(rest[0]) is None


def _scan_tree(tree: ast.Module, top: frozenset[str], path: str = "") -> _FileScan:
    docstrings: set[int] = set()
    imports: set[str] = set()
    relative: list[tuple[int, str, tuple[str, ...]]] = []
    names: set[str] = set()
    references: set[str] = set()
    texts: list[str] = []
    identifiers: list[str] = []
    repo_wide = False
    nested_divisions: set[int] = set()
    covered: set[int] = set()
    roots: set[int] = set()
    if path and any(isinstance(node, ast.Name) and node.id == "__file__" for node in _walk(tree)):
        try:
            anchored, covered, roots = _Anchors(tree, path).references(tree)
        except _Unresolved:
            # A path built from this file's location that cannot be followed
            # could name anything, so the file is selected on every diff.
            repo_wide = True
        else:
            references.update(anchored)

    def reference(value: str) -> None:
        cleaned = _clean_reference(value)
        if cleaned and cleaned.split("/", 1)[0] in top:
            references.add(cleaned)

    for node in _walk(tree):
        if isinstance(node, _DOCSTRING_OWNERS):
            docstring = _docstring_of(node)
            if docstring is not None:
                docstrings.add(id(docstring))
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports.add(alias.name)
                identifiers.append(alias.name)
                if alias.asname:
                    identifiers.append(alias.asname)
        elif isinstance(node, ast.ImportFrom):
            identifiers.extend(alias.name for alias in node.names)
            identifiers.extend(alias.asname for alias in node.names if alias.asname)
            if node.level == 0 and node.module:
                identifiers.append(node.module)
                imports.add(node.module)
                imports.update(f"{node.module}.{alias.name}" for alias in node.names)
            else:
                relative.append(
                    (node.level, node.module or "", tuple(alias.name for alias in node.names))
                )
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            if id(node) in docstrings:
                continue
            value = node.value
            texts.append(value)
            stripped = value.strip()
            if _NAME_STRING.fullmatch(stripped):
                names.add(stripped)
            if "/" in value:
                for match in _PATH_TOKEN.finditer(value):
                    reference(match.group(0).rstrip("."))
        elif isinstance(node, ast.Name):
            identifiers.append(node.id)
        elif isinstance(node, ast.Attribute):
            identifiers.append(node.attr)
        elif isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
            identifiers.append(node.name)
        elif isinstance(node, ast.arg):
            identifiers.append(node.arg)
        elif isinstance(node, ast.keyword) and node.arg:
            identifiers.append(node.arg)
        elif isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div):
            if id(node) in covered:
                continue
            if isinstance(node.left, ast.BinOp) and isinstance(node.left.op, ast.Div):
                nested_divisions.add(id(node.left))
            if id(node) in nested_divisions:
                continue
            operands = _division_operands(node)
            base = operands[0]
            if _is_repo_root(base, roots) and _string(operands[1]) is None:
                # The repository root joined with a runtime value can name any file.
                repo_wide = True
            if _string(base) is None:
                base_name = (
                    _callable_name(base) if isinstance(base, ast.Name | ast.Attribute) else ""
                )
                if base_name and _TEMPORARY_BASE.search(base_name):
                    continue
                operands = operands[1:]
            parts: list[str] = []
            for operand in operands:
                text = _string(operand)
                if text is None:
                    break
                parts.append(text)
            if parts:
                reference(_joined(parts))
        elif isinstance(node, ast.Call):
            name = _callable_name(node.func)
            arguments: list[ast.expr] = []
            if name in _JOIN_CALLS:
                arguments = list(node.args)
            elif (
                name == "join"
                and isinstance(node.func, ast.Attribute)
                and _callable_name(node.func.value) in {"path", "posixpath"}
            ):
                arguments = list(node.args)
            elif name == "open":
                arguments = list(node.args[:1])
            elif name == "spec_from_file_location":
                arguments = list(node.args[1:2])
            if (
                id(node) not in covered
                and name in {"join", "joinpath"}
                and _computed_root_join(node, roots)
            ):
                repo_wide = True
            strings = [text for text in (_string(argument) for argument in arguments) if text]
            if strings and id(node) not in covered:
                reference(_joined(strings))
            if not repo_wide and _scans_repository(node, roots):
                repo_wide = True

    text_blob = "\n".join(texts)
    identifier_blob = "\n".join(identifiers)
    if (
        _FULL_TOKEN.search(text_blob)
        or _FULL_TOKEN.search(identifier_blob)
        or _FULL_PORT.search(text_blob)
    ):
        tier = 2
    elif (
        _STORAGE_TOKEN.search(text_blob)
        or _STORAGE_TOKEN.search(identifier_blob)
        or _STORAGE_PORT.search(text_blob)
        or any(name.split(".", 1)[0] in STORAGE_IMPORTS for name in imports)
    ):
        tier = 1
    else:
        tier = 0
    return _FileScan(
        frozenset(imports),
        tuple(relative),
        frozenset(names),
        frozenset(references),
        tier,
        repo_wide,
        _weight(tree),
    )


class _Scanner:
    """File scans and local helper resolution shared by the graph and `tiers_for`."""

    def __init__(
        self,
        root: Path,
        tracked: Sequence[str] | None,
        members: Sequence[str],
        workspace_modules: Mapping[str, frozenset[str]],
    ) -> None:
        self.root = root
        self.tracked = frozenset(tracked) if tracked is not None else None
        self.members = tuple(members)
        self.workspace_modules = workspace_modules
        if tracked is not None:
            self.top = frozenset(path.split("/", 1)[0] for path in tracked)
        else:
            try:
                self.top = frozenset(entry.name for entry in root.iterdir())
            except OSError:
                self.top = frozenset()
        self.stems: dict[str, list[str]] = {}
        if tracked is not None:
            sources = tuple(f"{member}/src/" for member in members)
            for path in tracked:
                if not path.endswith(".py") or path.startswith(sources) or path == ROOT_CONFTEST:
                    continue
                pure = PurePosixPath(path)
                stem = pure.parent.name if pure.name == "__init__.py" else pure.stem
                if stem:
                    self.stems.setdefault(stem, []).append(path)
        self._scans: dict[str, _FileScan] = {}
        # Scans read from or bound for the on-disk cache, keyed by path, each
        # stamped with the file's (mtime_ns, size) when it was scanned.
        self._stored: dict[str, tuple[tuple[int, int], _FileScan]] = {}
        self._resolved: dict[tuple[str, str], tuple[str, ...]] = {}
        self._closures: dict[str, tuple[str, ...]] = {}

    def exists(self, path: str) -> bool:
        if self.tracked is not None:
            return path in self.tracked
        return (self.root / path).is_file()

    def scan(self, path: str) -> _FileScan:
        cached = self._scans.get(path)
        if cached is not None:
            return cached
        try:
            status = (self.root / path).stat()
            stamp = (status.st_mtime_ns, status.st_size)
            stored = self._stored.get(path)
            if stored is not None and stored[0] == stamp:
                self._scans[path] = stored[1]
                return stored[1]
            source = (self.root / path).read_bytes()
        except OSError:
            result = _EMPTY_SCAN
        else:
            try:
                tree = ast.parse(source, filename=path)
            except (SyntaxError, ValueError):
                # An unreadable file cannot be classified; assume it needs every service.
                result = _FileScan(frozenset(), (), frozenset(), frozenset(), 2, False, 0)
            else:
                result = _scan_tree(tree, self.top, path)
            self._stored[path] = (stamp, result)
        self._scans[path] = result
        return result

    def _cache_file(self) -> Path:
        base = os.environ.get("XDG_CACHE_HOME") or str(Path.home() / ".cache")
        identity = hashlib.sha256()
        identity.update(Path(__file__).read_bytes())
        identity.update(str(self.root).encode())
        identity.update("\0".join(sorted(self.top)).encode())
        return Path(base) / "curie-pytest-impact" / f"{identity.hexdigest()[:32]}.json"

    def load_cache(self) -> None:
        """Reuse scans of unchanged files from an earlier build of this tree."""
        try:
            entries = json.loads(self._cache_file().read_text(encoding="utf-8"))
            for path, (stamp, fields) in entries.items():
                imports, relative, names, references, tier, repo_wide, weight = fields
                scan = _FileScan(
                    frozenset(imports),
                    tuple((level, module, tuple(items)) for level, module, items in relative),
                    frozenset(names),
                    frozenset(references),
                    tier,
                    repo_wide,
                    weight,
                )
                self._stored[path] = ((stamp[0], stamp[1]), scan)
        except (OSError, ValueError, TypeError, KeyError, AttributeError):
            self._stored = {}

    def save_cache(self) -> None:
        entries = {
            path: [
                list(stamp),
                [
                    sorted(scan.imports),
                    [list(item) for item in scan.relative],
                    sorted(scan.names),
                    sorted(scan.references),
                    scan.tier,
                    scan.repo_wide,
                    scan.weight,
                ],
            ]
            for path, (stamp, scan) in self._stored.items()
        }
        target = self._cache_file()
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            partial = target.with_name(f"{target.name}.{os.getpid()}.tmp")
            partial.write_text(json.dumps(entries), encoding="utf-8")
            os.replace(partial, target)
        except OSError:
            pass

    def _first_existing(self, candidates: Iterable[str]) -> list[str]:
        return [
            candidate
            for candidate in dict.fromkeys(candidates)
            if candidate != ROOT_CONFTEST and self.exists(candidate)
        ]

    def resolve_absolute(self, importer: str, dotted: str) -> tuple[str, ...]:
        parts = dotted.split(".")
        head = parts[0]
        if (
            not head
            or head == "__future__"
            or head in sys.stdlib_module_names
            or head in self.workspace_modules
        ):
            return ()
        directory = PurePosixPath(importer).parent.as_posix()
        key = (directory, dotted)
        cached = self._resolved.get(key)
        if cached is not None:
            return cached
        found: list[str] = []
        current = PurePosixPath(directory)
        while True:
            base = "" if current.as_posix() == "." else f"{current.as_posix()}/"
            joined = "/".join(parts)
            found = self._first_existing(
                (
                    f"{base}{joined}.py",
                    f"{base}{joined}/__init__.py",
                    f"{base}{head}.py",
                    f"{base}{head}/__init__.py",
                )
            )
            if found or not base:
                break
            current = current.parent
        if not found:
            for location in self.stems.get(head, ()):
                found.append(location)
                if location.endswith("/__init__.py") and len(parts) > 1:
                    package = location.removesuffix("/__init__.py")
                    rest = "/".join(parts[1:])
                    found.extend(
                        self._first_existing(
                            (f"{package}/{rest}.py", f"{package}/{rest}/__init__.py")
                        )
                    )
        result = tuple(dict.fromkeys(found))
        self._resolved[key] = result
        return result

    def resolve_relative(
        self, importer: str, level: int, module: str, names: Sequence[str]
    ) -> list[str]:
        directory = PurePosixPath(importer).parent
        for _ in range(level - 1):
            directory = directory.parent
        base = directory.joinpath(*module.split(".")) if module else directory
        prefix = "" if base.as_posix() == "." else f"{base.as_posix()}/"
        candidates: list[str] = []
        if module:
            candidates.extend((f"{base.as_posix()}.py", f"{prefix}__init__.py"))
        else:
            candidates.append(f"{prefix}__init__.py")
        candidates.extend(f"{prefix}{name}.py" for name in names)
        candidates.extend(f"{prefix}{name}/__init__.py" for name in names)
        return self._first_existing(candidates)

    def conftests(self, path: str) -> list[str]:
        found = []
        directory = PurePosixPath(path).parent
        while directory.as_posix() not in {".", ""}:
            candidate = f"{directory.as_posix()}/conftest.py"
            if candidate != path and self.exists(candidate):
                found.append(candidate)
            directory = directory.parent
        return found

    def local_closure(self, path: str) -> tuple[str, ...]:
        """The file, its ancestor conftests (not the root one), and local helpers."""
        cached = self._closures.get(path)
        if cached is not None:
            return cached
        seen: dict[str, None] = {}
        stack = [path, *self.conftests(path)]
        while stack:
            current = stack.pop()
            if current in seen:
                continue
            seen[current] = None
            scan = self.scan(current)
            for dotted in scan.imports:
                stack.extend(self.resolve_absolute(current, dotted))
            for level, module, names in scan.relative:
                stack.extend(self.resolve_relative(current, level, module, names))
        result = tuple(seen)
        self._closures[path] = result
        return result

    def tier(self, path: str) -> int:
        return max((self.scan(item).tier for item in self.local_closure(path)), default=0)

    def repo_wide(self, path: str) -> bool:
        return any(self.scan(item).repo_wide for item in self.local_closure(path))


def _scanner(root: Path, tracked: Sequence[str] | None) -> _Scanner:
    members = _workspace_members(root)
    return _Scanner(root, tracked, members, _workspace_modules(members, tracked, root))


def tiers_for(repo_root: Path, files: Iterable[str]) -> dict[str, int]:
    """Service tier of each test file: 0 core, 1 storage, 2 full.

    Cheap: scans only the given files, their conftests and local helpers. It
    gives the same answer as `ImpactGraph.service_tier` for the same tree.
    """
    root = Path(repo_root).resolve()
    scanner = _scanner(root, _git_tracked(root))
    return {path: scanner.tier(path) for path in files}


class ImpactGraph:
    """File-level reach of every test file in the pytest universe."""

    def __init__(
        self,
        root: Path,
        tracked: Sequence[str],
        scanner: _Scanner,
        test_files: tuple[str, ...],
        project_members: Mapping[str, str],
        dependencies: Mapping[str, frozenset[str]],
        member_files: Mapping[str, Sequence[str]],
    ) -> None:
        self.root = root
        self.tracked = frozenset(tracked)
        self.test_files = test_files
        self._scanner = scanner
        self._members_by_length = sorted(scanner.members, key=lambda member: (-len(member), member))
        self._project_members = dict(project_members)
        self._dependencies = dict(dependencies)
        self._member_files = {member: tuple(paths) for member, paths in member_files.items()}
        self._closure_cache: dict[str, frozenset[str]] = {}
        self._regions_cache: dict[str, frozenset[str]] = {}
        self._reference_cache: dict[str, frozenset[str]] = {}
        self._hop_cache: dict[str, frozenset[str]] = {}
        self._reach_cache: dict[str, tuple[frozenset[str], frozenset[str]]] = {}
        self._packages_cache: dict[str, frozenset[str]] = {}
        self._known_regions = sorted(
            {*scanner.members, *(self.region_of(path) for path in tracked)}
        )

    @classmethod
    def build(cls, repo_root: Path, tracked: Sequence[str] | None = None) -> ImpactGraph:
        root = Path(repo_root).resolve()
        # A build over the tracked tree reuses an on-disk cache of file scans,
        # so repeated selector runs on one checkout stay fast.
        cached = tracked is None
        if tracked is None:
            listed = _git_tracked(root)
            if listed is None:
                raise ValueError(f"cannot list tracked files under {root}")
            tracked = listed
        tracked = tuple(tracked)
        scanner = _scanner(root, tracked)
        if cached:
            scanner.load_cache()
        document = _read_toml(root / "pyproject.toml")
        testpaths = [
            path.strip("/")
            for path in _string_list(
                _table(document, "tool", "pytest", "ini_options").get("testpaths")
            )
        ]
        test_files = tuple(
            sorted(path for path in tracked if _is_test_name(path) and _under_any(path, testpaths))
        )
        project_members: dict[str, str] = {}
        declared: dict[str, list[str]] = {}
        for member in scanner.members:
            project = _table(_read_toml(root / member / "pyproject.toml"), "project")
            name = project.get("name")
            if isinstance(name, str):
                project_members[_normalize_project(name)] = member
            declared[member] = _string_list(project.get("dependencies"))
        member_files: dict[str, list[str]] = {member: [] for member in scanner.members}
        ordered = sorted(scanner.members, key=lambda member: (-len(member), member))
        for path in tracked:
            if not path.endswith(".py"):
                continue
            owner = next((member for member in ordered if _under(path, member)), None)
            if owner is None:
                continue
            if _under_any(path, testpaths) or "tests" in path.split("/")[:-1]:
                continue
            name = path.rsplit("/", 1)[-1]
            if name == "conftest.py" or _is_test_name(path):
                continue
            member_files[owner].append(path)
        dependencies: dict[str, frozenset[str]] = {}
        for member in scanner.members:
            direct: set[str] = set()
            for requirement in declared[member]:
                match = _REQUIREMENT_NAME.match(requirement.strip())
                if match is not None:
                    owner = project_members.get(_normalize_project(match.group(0)))
                    if owner is not None:
                        direct.add(owner)
            for path in member_files[member]:
                for dotted in scanner.scan(path).imports:
                    direct.update(scanner.workspace_modules.get(dotted.split(".", 1)[0], ()))
            direct.discard(member)
            dependencies[member] = frozenset(direct)
        if cached:
            for path in test_files:
                scanner.local_closure(path)
            scanner.save_cache()
        return cls(root, tracked, scanner, test_files, project_members, dependencies, member_files)

    def region_of(self, path: str) -> str:
        for member in self._members_by_length:
            if _under(path, member):
                return member
        parts = path.split("/")
        if len(parts) == 1:
            return path
        if parts[0] in TWO_SEGMENT_ROOTS:
            return "/".join(parts[:2])
        return parts[0]

    def _closure(self, member: str) -> frozenset[str]:
        cached = self._closure_cache.get(member)
        if cached is not None:
            return cached
        seen = {member}
        stack = [member]
        while stack:
            for dependency in self._dependencies.get(stack.pop(), ()):
                if dependency not in seen:
                    seen.add(dependency)
                    stack.append(dependency)
        result = frozenset(seen)
        self._closure_cache[member] = result
        return result

    def package_dependents(self, member: str) -> frozenset[str]:
        """Members that depend on `member`, transitively, excluding it."""
        return frozenset(
            other
            for other in self._dependencies
            if other != member and member in self._closure(other)
        )

    def _reference_regions(self, reference: str) -> frozenset[str]:
        cached = self._reference_cache.get(reference)
        if cached is not None:
            return cached
        regions = {self.region_of(reference)}
        prefix = f"{reference}/"
        index = bisect.bisect_left(self._known_regions, prefix)
        while index < len(self._known_regions) and self._known_regions[index].startswith(prefix):
            regions.add(self._known_regions[index])
            index += 1
        result = frozenset(regions)
        self._reference_cache[reference] = result
        return result

    def _hop_references(self, path: str) -> frozenset[str]:
        """Repository paths a referenced script names anywhere in its text."""
        cached = self._hop_cache.get(path)
        if cached is not None:
            return cached
        found: set[str] = set()
        try:
            text = (self.root / path).read_text(encoding="utf-8", errors="replace")
        except OSError:
            text = ""
        for match in _PATH_TOKEN.finditer(text):
            cleaned = _clean_reference(match.group(0).rstrip("."))
            if cleaned and cleaned.split("/", 1)[0] in self._scanner.top:
                found.add(cleaned)
        result = frozenset(found)
        self._hop_cache[path] = result
        return result

    @cached_property
    def _package_data_regions(self) -> dict[str, frozenset[str]]:
        """Regions of exact tracked files a package's own source refers to."""
        data: dict[str, frozenset[str]] = {}
        for member, paths in self._member_files.items():
            regions: set[str] = set()
            for path in paths:
                for reference in self._scanner.scan(path).references:
                    if reference in self.tracked:
                        regions.add(self.region_of(reference))
            data[member] = frozenset(regions)
        return data

    def _packages_named(self, scan: _FileScan) -> set[str]:
        members: set[str] = set()
        modules = self._scanner.workspace_modules
        for dotted in scan.imports:
            members.update(modules.get(dotted.split(".", 1)[0], ()))
        for name in scan.names:
            members.update(modules.get(name.split(".", 1)[0], ()))
            owner = self._project_members.get(_normalize_project(name))
            if owner is not None:
                members.add(owner)
        return members

    def _reach(self, test_file: str) -> tuple[frozenset[str], frozenset[str]]:
        """Files a test runs or imports, and the paths they refer to.

        A referenced Python entry point (a script the test executes or loads
        by path) is followed like an import: its own local helpers, imports
        and references join the reach, transitively. A referenced shell
        script contributes the paths in its text, and any Python script among
        those is followed the same way.
        """
        cached = self._reach_cache.get(test_file)
        if cached is not None:
            return cached
        files: dict[str, None] = {}
        references: set[str] = set()
        pending = [test_file]
        entered: set[str] = set()
        while pending:
            entry = pending.pop()
            if entry in entered:
                continue
            entered.add(entry)
            for path in self._scanner.local_closure(entry):
                files[path] = None
                for reference in self._scanner.scan(path).references:
                    references.add(reference)
                    if reference not in self.tracked:
                        continue
                    if reference.endswith(".py"):
                        pending.append(reference)
                        references.update(self._hop_references(reference))
                    elif reference.endswith(".sh"):
                        for named in self._hop_references(reference):
                            references.add(named)
                            if named.endswith(".py") and named in self.tracked:
                                pending.append(named)
        result = (frozenset(files), frozenset(references))
        self._reach_cache[test_file] = result
        return result

    def _expanded_packages(self, test_file: str, files: Iterable[str]) -> frozenset[str]:
        """Workspace members a test's reach imports, with their dependencies."""
        cached = self._packages_cache.get(test_file)
        if cached is not None:
            return cached
        files = tuple(files)
        # A file inside a workspace member reaches that member's dependencies.
        packages = {
            region
            for region in (self.region_of(path) for path in files)
            if region in self._dependencies
        }
        for path in files:
            packages.update(self._packages_named(self._scanner.scan(path)))
        expanded: set[str] = set()
        for member in packages:
            expanded.update(self._closure(member))
        result = frozenset(expanded)
        self._packages_cache[test_file] = result
        return result

    def file_regions(self, test_file: str) -> frozenset[str]:
        cached = self._regions_cache.get(test_file)
        if cached is not None:
            return cached
        files, references = self._reach(test_file)
        regions = {self.region_of(path) for path in files}
        expanded = self._expanded_packages(test_file, files)
        regions.update(expanded)
        for reference in references:
            regions.update(self._reference_regions(reference))
        data = self._package_data_regions
        for member in expanded:
            regions.update(data.get(member, ()))
        result = frozenset(regions)
        self._regions_cache[test_file] = result
        return result

    @cached_property
    def _readers_index(self) -> dict[str, frozenset[str]]:
        """Referenced path -> test files whose reach refers to it.

        A test's references include those of the workspace package sources it
        reaches, since a package may read a repository file on its behalf.
        """
        member_references: dict[str, set[str]] = {}
        for member, paths in self._member_files.items():
            found: set[str] = set()
            for path in paths:
                found.update(self._scanner.scan(path).references)
            member_references[member] = found
        index: dict[str, set[str]] = {}
        for test_file in self.test_files:
            files, references = self._reach(test_file)
            every = set(references)
            for member in self._expanded_packages(test_file, files):
                every.update(member_references.get(member, ()))
            for reference in every:
                index.setdefault(reference, set()).add(test_file)
        return {reference: frozenset(tests) for reference, tests in index.items()}

    def readers(self, path: str) -> frozenset[str]:
        """Test files that refer to `path` itself or to a directory above it."""
        index = self._readers_index
        found: set[str] = set(index.get(path, ()))
        parts = path.split("/")
        for length in range(1, len(parts)):
            found.update(index.get("/".join(parts[:length]), ()))
        return frozenset(found)

    @cached_property
    def claimed_regions(self) -> frozenset[str]:
        """Every region some test file in the universe reaches.

        Excluded test files count: they run in another job, so a change they
        claim is still covered.
        """
        claimed: set[str] = set()
        for test_file in self.test_files:
            claimed.update(self.file_regions(test_file))
        return frozenset(claimed)

    def service_tier(self, test_file: str) -> int:
        return self._scanner.tier(test_file)

    def weight(self, test_file: str) -> int:
        return self._scanner.scan(test_file).weight

    def repo_wide(self, test_file: str) -> bool:
        files, _ = self._reach(test_file)
        return any(self._scanner.scan(path).repo_wide for path in files)


@dataclass(frozen=True)
class ImpactConfig:
    """The registry's `pytest_impact` section, plus what select() reads elsewhere."""

    everything: tuple[str, ...]
    no_impact: tuple[str, ...]
    workflow_jobs: tuple[str, ...]
    excluded: tuple[str, ...]
    always: tuple[str, ...]
    reader_only: tuple[str, ...]
    min_shard_share: int
    max_shards: int
    cli_list: str = ""
    ignored_exact: tuple[str, ...] = ()


def _config_paths(value: object, label: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise ValueError(f"pytest_impact.{label} must be a list of paths")
    paths: list[str] = []
    for item in value:
        if (
            not isinstance(item, str)
            or not item
            or item.startswith("/")
            or item.endswith("/")
            or "\\" in item
            or any(part in {"", ".", ".."} for part in item.split("/"))
        ):
            raise ValueError(f"pytest_impact.{label} contains an invalid path: {item!r}")
        paths.append(item)
    if len(set(paths)) != len(paths):
        raise ValueError(f"pytest_impact.{label} contains a duplicate entry")
    return tuple(paths)


def _config_count(value: object, label: str) -> int:
    if type(value) is not int or value < 1:
        raise ValueError(f"pytest_impact.{label} must be a positive integer")
    return value


def load_config(registry_document: Mapping[str, object]) -> ImpactConfig:
    """Read and validate the `pytest_impact` section of the selection registry."""
    section = registry_document.get("pytest_impact")
    if not isinstance(section, dict) or any(not isinstance(key, str) for key in section):
        raise ValueError("pytest_impact must be a mapping")
    missing = [key for key in CONFIG_KEYS if key not in section]
    if missing:
        raise ValueError(f"pytest_impact is missing: {', '.join(missing)}")
    unknown = sorted(set(section) - set(CONFIG_KEYS))
    if unknown:
        raise ValueError(f"pytest_impact has unknown keys: {', '.join(unknown)}")
    workflow_jobs = section["workflow_jobs"]
    if (
        not isinstance(workflow_jobs, list)
        or any(not isinstance(job, str) or not job for job in workflow_jobs)
        or len(set(workflow_jobs)) != len(workflow_jobs)
    ):
        raise ValueError("pytest_impact.workflow_jobs must be a list of unique job names")
    cli = _table(registry_document, "pytest_scopes", "cli").get("test_files")
    ignored = _table(registry_document, "rules", "ignored_exact")
    return ImpactConfig(
        everything=_config_paths(section["everything"], "everything"),
        no_impact=_config_paths(section["no_impact"], "no_impact"),
        workflow_jobs=tuple(workflow_jobs),
        excluded=_config_paths(section["excluded"], "excluded"),
        always=_config_paths(section["always"], "always"),
        reader_only=_config_paths(section["reader_only"], "reader_only"),
        min_shard_share=_config_count(section["min_shard_share"], "min_shard_share"),
        max_shards=_config_count(section["max_shards"], "max_shards"),
        cli_list=cli if isinstance(cli, str) else "",
        ignored_exact=tuple(key for key in ignored if isinstance(key, str)),
    )


@dataclass(frozen=True)
class Selection:
    everything: bool
    reason: str
    targets: tuple[str, ...]


def _everything(reason: str) -> Selection:
    return Selection(True, reason, ())


def select(
    graph: ImpactGraph,
    config: ImpactConfig,
    paths: Sequence[str],
    *,
    cli_files: Sequence[str],
    workflow_jobs_changed: frozenset[str] | None,
    registry_pytest_changed: bool | None,
) -> Selection:
    """Test files a pull request diff can affect, or everything with the reason."""
    changed: set[str] = set()
    contributed: set[str] = set()
    for path in paths:
        if _under_any(path, config.no_impact):
            continue
        if config.cli_list and path == config.cli_list:
            contributed.update(cli_files)
            continue
        if _under_any(path, config.everything):
            return _everything(path)
        if path == WORKFLOW_PATH:
            if workflow_jobs_changed is None:
                return _everything(path)
            hits = sorted(workflow_jobs_changed & {WORKFLOW_HEADER, *config.workflow_jobs})
            if hits:
                return _everything(f"{path} job {hits[0]}")
            continue
        if path == REGISTRY_PATH:
            if registry_pytest_changed is None or registry_pytest_changed:
                return _everything(path)
            continue
        if path in config.ignored_exact or _under_any(path, config.reader_only):
            # Documentation: only the tests known to read it, plus the
            # always-selected set below.
            contributed.update(graph.readers(path))
            continue
        if "/" not in path:
            return _everything(path)
        region = graph.region_of(path)
        if region not in graph.claimed_regions:
            # No test reaches this path, so impact cannot say what it affects.
            return _everything(f"{path} unclaimed")
        changed.add(region)
        if _under(path, "cli"):
            contributed.update(cli_files)
    selected: set[str] = {path for path in contributed if path in graph.tracked}
    for test_file in graph.test_files:
        if (
            _under_any(test_file, config.always)
            or graph.repo_wide(test_file)
            or (changed and not changed.isdisjoint(graph.file_regions(test_file)))
        ):
            selected.add(test_file)
    targets = tuple(sorted(path for path in selected if not _under_any(path, config.excluded)))
    if not targets:
        return _everything(EMPTY_IMPACT)
    return Selection(False, "impact", targets)


def needs_pytest(
    graph: ImpactGraph,
    config: ImpactConfig,
    paths: Sequence[str],
    *,
    cli_files: Sequence[str],
    workflow_jobs_changed: frozenset[str] | None,
    registry_pytest_changed: bool | None,
) -> bool:
    """Does a pull request diff the path rules skip still reach a test file?

    Documentation a test reads selects that reader, and the always-selected
    set (`always` plus every test that can read any file) joins every diff.
    """
    selection = select(
        graph,
        config,
        paths,
        cli_files=cli_files,
        workflow_jobs_changed=workflow_jobs_changed,
        registry_pytest_changed=registry_pytest_changed,
    )
    return not (selection.everything and selection.reason == EMPTY_IMPACT)


@dataclass(frozen=True)
class ShardPlan:
    shards: int
    storage_shards: int
    full_shards: int


def _constant_strings(node: ast.expr) -> set[str]:
    return {
        child.value
        for child in ast.walk(node)
        if isinstance(child, ast.Constant) and isinstance(child.value, str)
    }


def conftest_groups(repo_root: Path) -> dict[str, str]:
    """Test file -> xdist group, from the root conftest's frozensets."""
    try:
        tree = ast.parse((Path(repo_root) / ROOT_CONFTEST).read_bytes())
    except (OSError, SyntaxError, ValueError):
        return {}
    values: dict[str, ast.expr] = {}
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    values[target.id] = node.value
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and node.value:
            values[node.target.id] = node.value

    def name(variable: str, default: str) -> str:
        value = values.get(variable)
        text = _string(value) if value is not None else None
        return text or default

    groups: dict[str, str] = {}
    for files, group in (
        ("SHARED_LIVE_STATE_FILES", name("SHARED_LIVE_STATE_GROUP", SHARED_LIVE_STATE_GROUP)),
        ("PROTECTED_RUNNER_FILES", name("PROTECTED_RUNNER_GROUP", PROTECTED_RUNNER_GROUP)),
    ):
        value = values.get(files)
        if value is not None:
            for path in _constant_strings(value):
                groups[path] = group
    return groups


def plan_shards(
    graph: ImpactGraph,
    config: ImpactConfig,
    files: Sequence[str],
    groups: Mapping[str, str],
) -> ShardPlan:
    """How many shards to run and how many boot the storage and full tiers."""
    weights: dict[str, int] = {}
    tiers: dict[str, int] = {}
    for path in dict.fromkeys(files):
        weight = graph.weight(path)
        if weight <= 0:
            continue
        group = groups.get(path, path)
        weights[group] = weights.get(group, 0) + weight
        tiers[group] = max(tiers.get(group, 0), graph.service_tier(path))
    selected = sum(weights.values())
    if not selected:
        return ShardPlan(1, 0, 0)
    universe = sum(
        graph.weight(path) for path in graph.test_files if not _under_any(path, config.excluded)
    )
    share = math.ceil(config.min_shard_share * selected / max(universe, selected))
    shards = min(config.max_shards, len(weights), max(1, share))
    full_weight = sum(weight for group, weight in weights.items() if tiers[group] >= 2)
    storage_weight = sum(weight for group, weight in weights.items() if tiers[group] >= 1)
    full = 0
    if full_weight:
        full = min(shards, max(1, math.ceil(shards * full_weight / selected)))
    storage = 0
    if storage_weight:
        storage = min(shards, max(1, full, math.ceil(shards * storage_weight / selected)))
    return ShardPlan(shards, storage, full)


def targets_digest(targets: Sequence[str]) -> str:
    """sha256 hex of the newline-joined targets, or empty when there are none."""
    if not targets:
        return ""
    return hashlib.sha256("\n".join(targets).encode()).hexdigest()

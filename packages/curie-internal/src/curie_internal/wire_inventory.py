"""Declare each stream writer and each source occurrence of a wire key literal."""

from __future__ import annotations

import ast
import json
import re
from collections import defaultdict
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

_KEY_PREFIX = "curie:"
_DATA = Path(__file__).with_name("wire_sites.json")
_RAW_STRING = re.compile(r'(?:br|r)(#*)"')
_RUST_CHARACTER = re.compile(r"'(?:\\(?:u\{[0-9a-fA-F_]+\}|.)|[^'\\\r\n])'")
_HEX_ESCAPE = re.compile(r"x([0-9a-fA-F]{2})")
_UNICODE_ESCAPE = re.compile(r"u\{([0-9a-fA-F_]+)\}")


@dataclass(frozen=True)
class _Token:
    kind: str
    value: str
    offset: int


def _sources(root: Path) -> Iterator[Path]:
    """Cover production sources, including inline Rust tests and generated defaults."""

    roots = [root / "runner/src", root / "cli/src"]
    roots.extend(root.glob("apps/*/src"))
    roots.extend(root.glob("adapters/*/src"))
    roots.extend(root.glob("tools/*/src"))
    roots.extend(root.glob("packages/*/src"))
    roots.extend(root.glob("packages/*/generated"))
    for directory in sorted(roots):
        for path in sorted(directory.rglob("*")):
            if path.suffix in {".py", ".rs"} and path.is_file():
                yield path


def _tokens(source: str, *, lua: bool = False) -> list[_Token]:
    """Read code tokens while keeping quoted strings opaque and ignoring comments."""

    tokens: list[_Token] = []
    index = 0
    while index < len(source):
        start = index
        if source[index].isspace():
            index += 1
            continue
        if source.startswith("--" if lua else "//", index):
            if lua and source.startswith("--[[", index):
                end = source.find("]]", index + 4)
                index = len(source) if end < 0 else end + 2
            else:
                end = source.find("\n", index)
                index = len(source) if end < 0 else end + 1
            continue
        if not lua and source.startswith("/*", index):
            depth = 1
            index += 2
            while index < len(source) and depth:
                if source.startswith("/*", index):
                    depth += 1
                    index += 2
                elif source.startswith("*/", index):
                    depth -= 1
                    index += 2
                else:
                    index += 1
            if depth:
                raise ValueError("unterminated Rust block comment")
            continue
        raw = None if lua else _RAW_STRING.match(source, index)
        if raw:
            content_start = raw.end()
            terminator = '"' + raw.group(1)
            end = source.find(terminator, content_start)
            if end < 0:
                raise ValueError("unterminated Rust raw string")
            tokens.append(_Token("string", source[content_start:end], start))
            index = end + len(terminator)
            continue
        if not lua and source.startswith('b"', index):
            index += 1
        if not lua and source[index] == "'":
            character = _RUST_CHARACTER.match(source, index)
            if character:
                index = character.end()
                continue
        if source[index] == '"' or (lua and source[index] == "'"):
            quote = source[index]
            index += 1
            content: list[str] = []
            while index < len(source) and source[index] != quote:
                if source[index] == "\\":
                    index += 1
                    if index == len(source):
                        raise ValueError("unterminated escaped string")
                    hexadecimal = _HEX_ESCAPE.match(source, index)
                    unicode = _UNICODE_ESCAPE.match(source, index)
                    if hexadecimal or unicode:
                        escape = hexadecimal or unicode
                        assert escape is not None
                        content.append(chr(int(escape.group(1).replace("_", ""), 16)))
                        index = escape.end()
                        continue
                    if source[index] == "\n":
                        index += 1
                        while index < len(source) and source[index].isspace():
                            index += 1
                        continue
                    escapes = {"n": "\n", "r": "\r", "t": "\t", "0": "\0"}
                    content.append(escapes.get(source[index], source[index]))
                else:
                    content.append(source[index])
                index += 1
            if index == len(source):
                raise ValueError("unterminated string")
            tokens.append(_Token("string", "".join(content), start))
            index += 1
            continue
        if source[index].isalpha() or source[index] == "_":
            index += 1
            while index < len(source) and (source[index].isalnum() or source[index] == "_"):
                index += 1
            tokens.append(_Token("name", source[start:index], start))
            continue
        if source.startswith("::", index):
            tokens.append(_Token("punctuation", "::", start))
            index += 2
            continue
        tokens.append(_Token("punctuation", source[index], start))
        index += 1
    return tokens


def _lua_producers(source: str) -> list[int]:
    tokens = _tokens(source, lua=True)
    producers: list[int] = []
    for index, token in enumerate(tokens[:-3]):
        if (
            token.value != "redis"
            or tokens[index + 1].value != "."
            or tokens[index + 2].value not in {"call", "pcall"}
        ):
            continue
        argument = index + 3
        if tokens[argument].value == "(":
            argument += 1
        if (
            argument < len(tokens)
            and tokens[argument].kind == "string"
            and tokens[argument].value.upper() == "XADD"
        ):
            producers.append(token.offset)
    return producers


def _expression_path(node: ast.expr) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        parent = _expression_path(node.value)
        if parent is not None:
            return f"{parent}.{node.attr}"
    return None


def _writer_reference(node: ast.expr, aliases: set[str]) -> bool:
    if isinstance(node, ast.Attribute) and node.attr.lower() == "xadd":
        return True
    if _expression_path(node) in aliases:
        return True
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "getattr"
        and len(node.args) >= 2
        and isinstance(node.args[1], ast.Constant)
        and isinstance(node.args[1].value, str)
        and node.args[1].value.lower() == "xadd"
    )


def _assembled_string(node: ast.expr) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left, right = _assembled_string(node.left), _assembled_string(node.right)
        if left is not None and right is not None:
            return left + right
    if isinstance(node, ast.JoinedStr):
        parts: list[str] = []
        for part in node.values:
            if isinstance(part, ast.Constant) and isinstance(part.value, str):
                parts.append(part.value)
            elif isinstance(part, ast.FormattedValue):
                value = _assembled_string(part.value)
                if value is None:
                    value = "__wire_dynamic__"
                elif part.conversion == ord("r"):
                    value = repr(value)
                elif part.conversion == ord("a"):
                    value = ascii(value)
                if part.format_spec is not None:
                    specification = _assembled_string(part.format_spec)
                    if specification is None:
                        value = "__wire_dynamic__"
                    else:
                        try:
                            value = format(value, specification)
                        except ValueError:
                            value = "__wire_dynamic__"
                parts.append(value)
            else:
                return None
        return "".join(parts)
    return None


class _PythonSites(ast.NodeVisitor):
    def __init__(self) -> None:
        self.scopes: list[str] = []
        self.producers: list[tuple[tuple[int, int, int], str, str]] = []
        self.literals: list[tuple[tuple[int, int, int], str, str]] = []
        self.docstrings: set[int] = set()
        self.aliases: list[set[str]] = [set()]

    @property
    def scope(self) -> str:
        return ".".join(self.scopes) or "module"

    def _body(
        self, node: ast.Module | ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef
    ) -> None:
        if node.body and isinstance(node.body[0], ast.Expr):
            value = node.body[0].value
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                self.docstrings.add(id(value))
        self.generic_visit(node)

    def visit_Module(self, node: ast.Module) -> None:
        self._body(node)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self.scopes.append(node.name)
        self.aliases.append(set(self.aliases[-1]))
        # A bound writer assigned on self can be called by another method.
        # Gather these class attributes before visiting either method's calls.
        for method in node.body:
            if isinstance(method, ast.FunctionDef | ast.AsyncFunctionDef):
                method_aliases = set(self.aliases[-1])
                assignments = sorted(
                    (
                        child
                        for child in ast.walk(method)
                        if isinstance(child, ast.Assign | ast.AnnAssign)
                    ),
                    key=lambda child: (child.lineno, child.col_offset),
                )
                for assignment in assignments:
                    if assignment.value is None:
                        continue
                    targets = (
                        assignment.targets
                        if isinstance(assignment, ast.Assign)
                        else [assignment.target]
                    )
                    if _writer_reference(assignment.value, method_aliases):
                        for target in targets:
                            name = _expression_path(target)
                            if name is not None:
                                method_aliases.add(name)
                                if name.startswith("self."):
                                    self.aliases[-1].add(name)
        self._body(node)
        self.aliases.pop()
        self.scopes.pop()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self.scopes.append(node.name)
        self.aliases.append(set(self.aliases[-1]))
        self._body(node)
        self.aliases.pop()
        self.scopes.pop()

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self.scopes.append(node.name)
        self.aliases.append(set(self.aliases[-1]))
        self._body(node)
        self.aliases.pop()
        self.scopes.pop()

    def visit_Call(self, node: ast.Call) -> None:
        if _writer_reference(node.func, self.aliases[-1]):
            self.producers.append(((node.lineno, node.col_offset, 0), self.scope, "python"))
        self.generic_visit(node)

    def visit_Assign(self, node: ast.Assign) -> None:
        self.generic_visit(node)
        writer = _writer_reference(node.value, self.aliases[-1])
        for target in node.targets:
            name = _expression_path(target)
            if name is not None:
                if writer:
                    self.aliases[-1].add(name)
                else:
                    self.aliases[-1].discard(name)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        if node.value is None:
            return
        self.visit(node.value)
        name = _expression_path(node.target)
        if name is not None:
            if _writer_reference(node.value, self.aliases[-1]):
                self.aliases[-1].add(name)
            else:
                self.aliases[-1].discard(name)

    def _string(self, node: ast.expr, value: str, evidence: str) -> None:
        if value.startswith(_KEY_PREFIX):
            self.literals.append(((node.lineno, node.col_offset, 0), self.scope, evidence))
        if "redis" in value and "XADD" in value.upper():
            for offset in _lua_producers(value):
                self.producers.append(((node.lineno, node.col_offset, offset), self.scope, "lua"))

    def visit_Constant(self, node: ast.Constant) -> None:
        if not isinstance(node.value, str) or id(node) in self.docstrings:
            return
        self._string(node, node.value, node.value)

    def _dynamic_parts(self, node: ast.expr) -> None:
        if isinstance(node, ast.JoinedStr):
            for part in node.values:
                if isinstance(part, ast.FormattedValue):
                    self.visit(part.value)
        elif isinstance(node, ast.BinOp):
            self._dynamic_parts(node.left)
            self._dynamic_parts(node.right)
        elif not isinstance(node, ast.Constant):
            self.visit(node)

    def visit_BinOp(self, node: ast.BinOp) -> None:
        assembled = _assembled_string(node)
        if assembled is None:
            self.generic_visit(node)
        else:
            self._string(node, assembled, ast.unparse(node))
            self._dynamic_parts(node)

    def visit_JoinedStr(self, node: ast.JoinedStr) -> None:
        assembled = _assembled_string(node)
        if assembled is not None:
            self._string(node, assembled, ast.unparse(node))
        self._dynamic_parts(node)


def _rust_literal(tokens: list[_Token], index: int) -> tuple[str, int] | None:
    if tokens[index].kind == "string":
        return tokens[index].value, index + 1
    if (
        tokens[index].value != "concat"
        or index + 2 >= len(tokens)
        or [token.value for token in tokens[index + 1 : index + 3]] != ["!", "("]
    ):
        return None
    index += 3
    parts: list[str] = []
    while index < len(tokens):
        if tokens[index].value == ")":
            return "".join(parts), index + 1
        part = _rust_literal(tokens, index)
        if part is None:
            return None
        value, index = part
        parts.append(value)
        if index < len(tokens) and tokens[index].value == ",":
            index += 1
        elif index >= len(tokens) or tokens[index].value != ")":
            return None
    return None


def _rust_sites(
    source: str,
) -> tuple[
    list[tuple[tuple[int, int, int], str, str]],
    list[tuple[tuple[int, int, int], str, str]],
]:
    tokens = _tokens(source)
    command_names = {"cmd"}
    for index, token in enumerate(tokens[:-2]):
        if token.value == "cmd" and tokens[index + 1].value == "as":
            command_names.add(tokens[index + 2].value)
        if token.kind == "name" and token.value == "let":
            target = index + 1
            if tokens[target].value == "mut":
                target += 1
            end = next(
                (
                    position
                    for position in range(target, len(tokens))
                    if tokens[position].value == ";"
                ),
                len(tokens),
            )
            equals = next(
                (position for position in range(target, end) if tokens[position].value == "="), None
            )
            if equals is not None:
                reference = tokens[equals + 1 : end]
                if (
                    reference
                    and reference[-1].value in command_names
                    and all(part.kind == "name" or part.value == "::" for part in reference)
                ):
                    command_names.add(tokens[target].value)
    scopes: list[str | None] = []
    pending: str | None = None
    producers: list[tuple[tuple[int, int, int], str, str]] = []
    literals: list[tuple[tuple[int, int, int], str, str]] = []
    grouped_until = 0
    for index, token in enumerate(tokens):
        if token.kind == "name" and token.value in {"fn", "impl", "mod", "trait"}:
            following = tokens[index + 1] if index + 1 < len(tokens) else None
            pending = following.value if following else token.value
        elif token.kind == "punctuation" and token.value == "{":
            scopes.append(pending)
            pending = None
        elif token.kind == "punctuation" and token.value == "}":
            if scopes:
                scopes.pop()
        elif token.kind == "punctuation" and token.value == ";":
            pending = None
        scope = ".".join(value for value in scopes if value is not None) or "module"
        position = (token.offset, 0, 0)
        literal = _rust_literal(tokens, index) if index >= grouped_until else None
        if literal is not None:
            value, grouped_until = literal
            if value.startswith(_KEY_PREFIX):
                literals.append((position, scope, value))
            if "redis" in value and "XADD" in value.upper():
                for offset in _lua_producers(value):
                    producers.append(((token.offset, offset, 0), scope, "lua"))
        if (
            token.kind == "name"
            and token.value in command_names
            and index + 2 < len(tokens)
            and tokens[index + 1].value == "("
            and tokens[index + 2].kind == "string"
            and tokens[index + 2].value.upper() == "XADD"
        ):
            producers.append((position, scope, "rust"))
    return producers, literals


def _identities(
    relative: str, occurrences: list[tuple[tuple[int, int, int], str, str]]
) -> dict[str, str]:
    ordinals: dict[str, int] = defaultdict(int)
    result: dict[str, str] = {}
    for _, scope, value in sorted(occurrences):
        ordinals[scope] += 1
        result[f"{relative}:{scope}:{ordinals[scope]}"] = value
    return result


def _scan(root: Path) -> tuple[dict[str, str], dict[str, str]]:
    producers: dict[str, str] = {}
    literals: dict[str, str] = {}
    for path in _sources(root):
        relative = path.relative_to(root).as_posix()
        source = path.read_text(encoding="utf-8")
        if path.suffix == ".py":
            visitor = _PythonSites()
            visitor.visit(ast.parse(source, filename=relative))
            producer_sites, literal_sites = visitor.producers, visitor.literals
        else:
            producer_sites, literal_sites = _rust_sites(source)
        producers.update(_identities(relative, producer_sites))
        literals.update(_identities(relative, literal_sites))
    return producers, literals


def scan_producers(root: Path) -> set[str]:
    """Return every Python, embedded Lua and Rust stream writer occurrence."""

    return set(_scan(root)[0])


def scan_key_literals(root: Path) -> set[str]:
    """Return every Python and Rust string or fstring starting with the wire prefix."""

    return set(_scan(root)[1])


def validate_inventory(root: Path) -> list[str]:
    """Reject undeclared, removed, changed or unclassified wire occurrences."""

    inventory = json.loads(_DATA.read_text(encoding="utf-8"))
    producers, literals = _scan(root)
    errors: list[str] = []
    for name, observed in (("producers", producers), ("key_literals", literals)):
        declared = inventory[name]
        for site in sorted(observed.keys() - declared.keys()):
            errors.append(f"undeclared {name} occurrence: {site}")
        for site in sorted(declared.keys() - observed.keys()):
            errors.append(f"declared {name} occurrence is absent: {site}")
        for site in sorted(observed.keys() & declared.keys()):
            entry = declared[site]
            classification = "purpose" if name == "producers" else "reason"
            if not isinstance(entry.get(classification), str) or not entry[classification].strip():
                errors.append(f"unclassified {name} occurrence: {site}")
            if name == "key_literals" and entry.get("value") != observed[site]:
                errors.append(f"changed key literal occurrence: {site}")
            if name == "producers" and entry.get("language") != observed[site]:
                errors.append(f"changed producer language: {site}")
    return errors
